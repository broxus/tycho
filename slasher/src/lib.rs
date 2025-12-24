use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use arc_swap::ArcSwapOption;
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::instrument;
use tycho_core::block_strider::{StateSubscriber, StateSubscriberContext};
use tycho_core::blockchain_rpc::BlockchainRpcClient;
use tycho_crypto::ed25519;
use tycho_slasher_traits::{ValidationSessionId, ValidatorEventsListener};
use tycho_types::boc::Boc;
use tycho_util::futures::JoinTask;
use tycho_util::serde_helpers;

use self::bc::MessageDeliveryStatus;
pub use self::bc::{
    BlocksBatch, ContractSubscription, EncodeBlocksBatchMessage, SignatureHistory, SignedMessage,
    SlasherContract,
};
use self::collector::{ValidatorEventsCollector, ValidatorSessionInfo};

pub mod collector {
    pub use self::validator_events::*;

    mod validator_events;
    // TODO: mod mempool_events;
}

mod bc;
mod util;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlasherConfig {
    /// TTL of messages to the slasher contract.
    ///
    /// Default: `30s`
    #[serde(with = "serde_helpers::humantime")]
    pub message_ttl: Duration,
    /// Interval between message delivery attempts.
    ///
    /// Default: `1s`
    #[serde(with = "serde_helpers::humantime")]
    pub message_retry_interval: Duration,

    /// Additional time to wait for the previous batch delivery.
    ///
    /// Default: `5s`
    #[serde(with = "serde_helpers::humantime")]
    pub prev_delivery_timeout: Option<Duration>,
}

impl Default for SlasherConfig {
    fn default() -> Self {
        Self {
            message_ttl: Duration::from_secs(30),
            message_retry_interval: Duration::from_secs(1),
            prev_delivery_timeout: Some(Duration::from_secs(5)),
        }
    }
}

pub struct Slasher {
    validator_events_collector: Arc<ValidatorEventsCollector>,
    shared: Arc<SlasherSharedState>,
    cancellation_token: CancellationToken,
}

impl Slasher {
    pub fn new<C: SlasherContract>(
        node_keys: Arc<ed25519::KeyPair>,
        contract: C,
        blockchain_rpc_client: BlockchainRpcClient,
        config: SlasherConfig,
    ) -> Self {
        let collector = Arc::new(ValidatorEventsCollector::new(contract.default_batch_size()));

        Self {
            validator_events_collector: collector,
            shared: Arc::new(SlasherSharedState {
                config,
                node_keys,
                contract: Box::new(contract),
                subscription: ArcSwapOption::empty(),
                blockchain_rpc_client,
            }),
            cancellation_token: Default::default(),
        }
    }

    pub fn validator_events_listener(&self) -> Arc<dyn ValidatorEventsListener> {
        self.validator_events_collector.clone()
    }

    async fn handle_state_impl(&self, cx: &StateSubscriberContext) -> Result<()> {
        if !cx.block.id().is_masterchain() {
            return Ok(());
        }
        let mc_seqno = cx.block.id().seqno;

        let this = self.shared.as_ref();

        // Check config updates
        let config_params = cx.state.config_params()?;
        let Some(slasher_address) = this
            .contract
            .find_account_address(&config_params)
            .context("failed to find contract address")?
            .filter(|addr| addr.is_masterchain())
        else {
            return Ok(());
        };

        let subscription = match this.subscription.load_full() {
            Some(s) if s.address() == &slasher_address => s,
            // TODO: Use `ArcSwap::compare_and_swap`?
            _ => {
                let s = Arc::new(ContractSubscription::new(&slasher_address));
                this.subscription.store(Some(s.clone()));
                s
            }
        };

        let extra = cx.block.load_extra()?.account_blocks.load()?;
        if let Some((_, account_block)) = extra.get(&slasher_address.address)? {
            subscription.handle_account_transactions(&account_block)?;
        }

        // TODO: Get or update batch size from the contract
        let batch_size = NonZeroU32::new(100).unwrap();

        while let Some(session_info) = self
            .validator_events_collector
            .pop_session_to_init(mc_seqno)
        {
            let session_id = session_info.session_id;
            if !session_info.can_participate(&this.node_keys.public_key) {
                tracing::info!(?session_id, "skipping session");
                continue;
            }

            let (tx, rx) = mpsc::unbounded_channel::<BlocksBatch>();
            if !self
                .validator_events_collector
                .init_session(session_id, batch_size, tx)
            {
                tracing::warn!(?session_id, "session removed before init");
                continue;
            }

            let token = self.cancellation_token.clone();
            let shared = self.shared.clone();
            tokio::task::spawn(
                token.run_until_cancelled_owned(shared.send_batches_to_contract(session_info, rx)),
            );
        }

        Ok(())
    }
}

impl Drop for Slasher {
    fn drop(&mut self) {
        self.cancellation_token.cancel();
    }
}

impl StateSubscriber for Slasher {
    type HandleStateFut<'a> = BoxFuture<'a, Result<()>>;

    #[inline]
    fn handle_state<'a>(&'a self, cx: &'a StateSubscriberContext) -> Self::HandleStateFut<'a> {
        Box::pin(self.handle_state_impl(cx))
    }
}

struct SlasherSharedState {
    config: SlasherConfig,
    node_keys: Arc<ed25519::KeyPair>,
    contract: Box<dyn SlasherContract>,
    subscription: ArcSwapOption<ContractSubscription>,
    blockchain_rpc_client: BlockchainRpcClient,
}

impl SlasherSharedState {
    #[instrument(skip_all, fields(session_id = ?info.session_id))]
    async fn send_batches_to_contract(
        self: Arc<Self>,
        info: ValidatorSessionInfo,
        mut rx: collector::BlocksBatchRx,
    ) {
        tracing::info!("started");
        scopeguard::defer!(tracing::info!("finished"));

        let mut send_task = None;

        while let Some(batch) = rx.recv().await {
            if let Some(send_task) = send_task.take()
                && let Some(timeout) = self.config.prev_delivery_timeout
                && tokio::time::timeout(timeout, send_task).await.is_err()
            {
                tracing::warn!("timeout on waiting for the previous batch to be delivered");
            }

            send_task = Some(JoinTask::new(self.clone().deliver_batch_message(
                info.session_id,
                info.own_validator_idx,
                batch,
            )));
        }
    }

    async fn deliver_batch_message(
        self: Arc<Self>,
        session_id: ValidationSessionId,
        validator_idx: u16,
        batch: BlocksBatch,
    ) {
        let params = EncodeBlocksBatchMessage {
            session_id,
            batch: &batch,
            validator_idx,
            keypair: &self.node_keys,
            ttl: self.config.message_ttl,
        };

        loop {
            let Some(subscription) = self.subscription.load_full() else {
                tracing::warn!("no slasher contract subscription");
                break;
            };

            let signed = match self.contract.encode_blocks_batch_message(&params) {
                Ok(signed) => signed,
                Err(e) => {
                    tracing::error!("failed to encode batch message: {e:?}");
                    return;
                }
            };
            let message_hash = *signed.message.repr_hash();
            let boc = Boc::encode(signed.message.into_inner());

            match subscription.track_message(&message_hash, signed.expire_at) {
                Ok(res) => {
                    self.blockchain_rpc_client
                        .broadcast_external_message(&boc)
                        .await;
                    drop(boc);

                    match res.await {
                        Ok(MessageDeliveryStatus::Sent { tx_hash }) => {
                            tracing::info!(%tx_hash, "batch message delivered");
                            return;
                        }
                        Ok(MessageDeliveryStatus::Expired) => {
                            // TODO: Execute transaction locally to guess the reason.
                            tracing::warn!("batch message expired");
                        }
                        Err(_) => return,
                    }
                }
                Err(e) => tracing::warn!("failed to track message: {e:?}"),
            }

            tokio::time::sleep(self.config.message_retry_interval).await;
        }
    }
}
