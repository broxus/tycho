use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use arc_swap::{ArcSwap, ArcSwapOption};
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::instrument;
use tycho_core::block_strider::{StateSubscriber, StateSubscriberContext};
use tycho_core::blockchain_rpc::BlockchainRpcClient;
use tycho_crypto::ed25519;
use tycho_slasher_traits::{ValidationSessionId, ValidatorEventsListener};
use tycho_storage::StorageContext;
use tycho_types::boc::Boc;
use tycho_types::models::{AutoSignatureContext, StdAddr};
use tycho_util::config::PartialConfig;
use tycho_util::futures::JoinTask;
use tycho_util::serde_helpers;

pub use self::bc::{
    BlocksBatch, ContractSubscription, EncodeBlocksBatchMessage, MessageDeliveryStatus,
    SignatureHistory, SignedMessage, SlasherContract, StubSlasherContract,
};
use self::collector::{ValidatorEventsCollector, ValidatorSessionInfo};
use self::storage::SlasherStorage;
use self::util::AtomicValidationSessionId;

pub mod collector {
    pub use self::validator_events::*;

    mod validator_events;
    // TODO: mod mempool_events;
}

mod bc;
#[expect(unused)]
mod storage;
mod util;

#[derive(Debug, Clone, Serialize, Deserialize, PartialConfig)]
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
        storage_context: &StorageContext,
        config: SlasherConfig,
    ) -> Result<Self> {
        let storage =
            SlasherStorage::open(storage_context).context("failed to open slasher storage")?;

        let collector = Arc::new(ValidatorEventsCollector::new(contract.default_batch_size()));

        Ok(Self {
            validator_events_collector: collector,
            shared: Arc::new(SlasherSharedState {
                config,
                node_keys,
                contract: Box::new(contract),
                subscription: ArcSwapOption::empty(),
                blockchain_rpc_client,
                storage,
                known_session_id: AtomicValidationSessionId::new(ValidationSessionId {
                    seqno: 0,
                    vset_switch_round: 0,
                    catchain_seqno: 0,
                }),
                signature_context: ArcSwap::new(Arc::new(AutoSignatureContext {
                    global_id: 0,
                    capabilities: Default::default(),
                })),
            }),
            cancellation_token: Default::default(),
        })
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
        let state_extra = cx.state.state_extra()?;

        // Sync signature context (TODO: do it only when config changes)
        let global = state_extra.config.get_global_version()?;
        self.shared
            .signature_context
            .store(Arc::new(AutoSignatureContext {
                global_id: cx.block.as_ref().global_id,
                capabilities: global.capabilities,
            }));

        // Check config updates (TODO: do it only when config changes)
        let Some(slasher_params) = this
            .contract
            .find_params(&state_extra.config)
            .context("failed to find slasher params")?
        else {
            return Ok(());
        };
        self.validator_events_collector
            .set_default_batch_size(slasher_params.blocks_batch_size);
        let slasher_address = StdAddr::new_masterchain(slasher_params.address);

        let catchain_seqno = state_extra.validator_info.catchain_seqno;
        let vset_switch_round = state_extra.consensus_info.vset_switch_round;

        let known_session_id = this.known_session_id.load();
        let session_id_from_block = if known_session_id.vset_switch_round == vset_switch_round
            && known_session_id.catchain_seqno == catchain_seqno
        {
            known_session_id
        } else {
            ValidationSessionId {
                seqno: known_session_id.seqno.saturating_add(1),
                vset_switch_round,
                catchain_seqno,
            }
        };
        tracing::trace!(?slasher_params, ?session_id_from_block);

        // Clear old sessions if needed
        // TODO: Add metrics.
        if session_id_from_block != known_session_id {
            let span = tracing::Span::current();
            let storage = this.storage.clone();
            tokio::task::spawn_blocking(move || {
                let _span = span.enter();
                storage.remove_outdated_batches(session_id_from_block)
            })
            .await??;

            this.known_session_id.set(session_id_from_block);
        }

        // Handle subscription
        let subscription = match this.subscription.load_full() {
            Some(s) if s.address() == &slasher_address => s,
            _ => {
                tracing::info!(%slasher_address, "slasher address changed");
                let s = Arc::new(ContractSubscription::new(&slasher_address));
                this.subscription.store(Some(s.clone()));
                s
            }
        };

        let extra = cx.block.load_extra()?.account_blocks.load()?;
        if let Some((_, account_block)) = extra.get(slasher_address.address)? {
            for entry in account_block.transactions.iter() {
                let (_, _, tx) = entry?;
                let tx_hash = tx.repr_hash();
                let tx = tx.load()?;

                tracing::debug!(
                    %tx_hash,
                    msg_hash = ?tx.in_msg.as_ref().map(|msg| msg.repr_hash()),
                    "found slasher transaction",
                );

                subscription.handle_account_transaction(tx_hash, &tx)?;

                match self.shared.contract.decode_event(&tx) {
                    Ok(Some(event)) => match event {
                        bc::SlasherContractEvent::SubmitBlocksBatch(submitted) => {
                            // TODO: Move into blocking.
                            this.storage.store_blocks_batch(
                                session_id_from_block,
                                submitted.validator_idx,
                                &submitted.blocks_batch,
                            )?;
                            tokio::task::yield_now().await;
                        }
                    },
                    Ok(None) => {}
                    Err(e) => tracing::warn!(%tx_hash, "failed to parse slasher event: {e:?}"),
                }
            }
        }

        while let Some(session_info) = self
            .validator_events_collector
            .pop_session_to_init(mc_seqno)
        {
            let session_id = session_info.session_id;
            tracing::info!(?session_id, "found session to init");
            if !session_info.can_participate(&this.node_keys.public_key) {
                tracing::info!(?session_id, "skipping session");
                continue;
            }

            let (tx, rx) = mpsc::unbounded_channel::<BlocksBatch>();
            if !self.validator_events_collector.init_session(
                session_id,
                slasher_params.blocks_batch_size,
                tx,
            ) {
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
    storage: SlasherStorage,
    known_session_id: AtomicValidationSessionId,
    signature_context: ArcSwap<AutoSignatureContext>,
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
        loop {
            let Some(subscription) = self.subscription.load_full() else {
                tracing::warn!("no slasher contract subscription");
                break;
            };

            let params = EncodeBlocksBatchMessage {
                address: subscription.address(),
                session_id,
                batch: &batch,
                validator_idx,
                signature_context: **self.signature_context.load(),
                keypair: &self.node_keys,
                ttl: self.config.message_ttl,
            };

            let signed = match self.contract.encode_blocks_batch_message(&params) {
                Ok(signed) => signed,
                Err(e) => {
                    tracing::error!("failed to encode batch message: {e:?}");
                    return;
                }
            };
            let msg_hash = *signed.message.repr_hash();
            let boc = Boc::encode(signed.message.into_inner());

            match subscription.track_message(&msg_hash, signed.expire_at) {
                Ok(res) => {
                    tracing::info!(
                        %msg_hash,
                        address = %params.address,
                        session_id = ?params.session_id,
                        validator_idx = params.validator_idx,
                        batch_seqno = batch.start_seqno,
                        block_count = batch.committed_blocks.len(),
                        "sending blocks batch"
                    );
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
