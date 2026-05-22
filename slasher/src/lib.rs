use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use arc_swap::{ArcSwap, ArcSwapOption};
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::instrument;
use tycho_block_util::config::BlockchainConfigExt;
use tycho_core::block_strider::{StateSubscriber, StateSubscriberContext};
use tycho_core::blockchain_rpc::BlockchainRpcClient;
use tycho_crypto::ed25519;
use tycho_slasher_traits::{ValidationSessionId, ValidatorEventsListener};
use tycho_storage::StorageContext;
use tycho_types::models::{BlockchainConfig, SignatureContext, StdAddr, ValidatorSet};
use tycho_types::prelude::*;
use tycho_util::config::PartialConfig;
use tycho_util::futures::JoinTask;
use tycho_util::serde_helpers;

use self::bc::SlasherParams;
pub use self::bc::{
    BlocksBatch, ContractSubscription, EncodeBlocksBatchMessage, MessageDelivered,
    SignatureHistory, SignedMessage, SlasherContract, StubSlasherContract,
};
use self::collector::{ValidatorEventsCollector, ValidatorSessionInfo};
use self::storage::SlasherStorage;
use self::util::AtomicValidationSessionId;

mod analyzer;
pub mod collector {
    pub use self::validator_events::*;

    mod validator_events;
    // TODO: mod mempool_events;
}

mod bc;
mod storage;
mod util;

#[derive(Debug, Clone, Serialize, Deserialize, PartialConfig)]
#[serde(default)]
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

    /// Absolute threshold of bad-session weight after which validator is bad in a vset epoch.
    ///
    /// Default: `100`
    pub bad_sessions_weight_threshold: u64,
}

impl Default for SlasherConfig {
    fn default() -> Self {
        Self {
            message_ttl: Duration::from_secs(30),
            message_retry_interval: Duration::from_secs(1),
            prev_delivery_timeout: Some(Duration::from_secs(5)),
            bad_sessions_weight_threshold: 100,
        }
    }
}

pub struct Slasher {
    validator_events_collector: Arc<ValidatorEventsCollector>,
    shared: Arc<SlasherSharedState>,
    cancellation_token: CancellationToken,
}

impl Slasher {
    #[allow(clippy::too_many_arguments)]
    pub fn new<C: SlasherContract>(
        node_keys: Arc<ed25519::KeyPair>,
        contract: C,
        blockchain_rpc_client: BlockchainRpcClient,
        storage_context: &StorageContext,
        config: SlasherConfig,
        global_id: i32,
        blockchain_config: &BlockchainConfig,
        known_session_id: ValidationSessionId,
    ) -> Result<Self> {
        let storage =
            SlasherStorage::open(storage_context).context("failed to open slasher storage")?;

        let current_vset = Arc::new(ParsedVset::from_raw(
            blockchain_config.get_current_validator_set_raw()?,
        )?);

        let slasher_params = contract
            .find_params(blockchain_config)
            .context("failed to find slasher params")?;

        let subscription = match &slasher_params {
            Some(slasher_params) => {
                let slasher_address = StdAddr::new_masterchain(slasher_params.address);
                tracing::info!(%slasher_address, ?slasher_params, "slasher initialized");
                Some(Arc::new(ContractSubscription::new(&slasher_address)))
            }
            None => None,
        };

        let collector = Arc::new(ValidatorEventsCollector::new(
            slasher_params
                .as_ref()
                .map_or(contract.default_batch_size(), |p| p.blocks_batch_size),
        ));
        let global = blockchain_config.get_global_version()?;

        // TODO: Spawn previous unsubmitted reports.

        Ok(Self {
            validator_events_collector: collector,
            shared: Arc::new(SlasherSharedState {
                config,
                node_keys,
                contract: Box::new(contract),
                subscription: ArcSwapOption::new(subscription),
                blockchain_rpc_client,
                storage,
                known_session_id: AtomicValidationSessionId::new(known_session_id),
                parsed_config: ArcSwap::new(Arc::new(ParsedConfig {
                    current_vset,
                    signature_context: SignatureContext {
                        global_id,
                        capabilities: global.capabilities,
                    },
                    slasher_params,
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

        let current_vset_raw = state_extra.config.get_current_validator_set_raw()?;
        let current_vset_hash = *current_vset_raw.repr_hash();

        // Apply config changes when needed.
        let mut vset_to_complete = None;
        if state_extra.after_key_block {
            let known_vset = self.shared.parsed_config.load().current_vset.clone();
            let current_vset = if current_vset_hash == known_vset.hash {
                known_vset
            } else {
                vset_to_complete = Some(known_vset);
                Arc::new(ParsedVset::from_raw(current_vset_raw)?)
            };

            let global = state_extra.config.get_global_version()?;
            let slasher_params = this
                .contract
                .find_params(&state_extra.config)
                .context("failed to find slasher params")?;

            if let Some(slasher_params) = &slasher_params {
                self.validator_events_collector
                    .set_default_batch_size(slasher_params.blocks_batch_size);
            }

            let slasher_address = slasher_params
                .as_ref()
                .map(|p| StdAddr::new_masterchain(p.address));

            // Update parsed config.
            self.shared.parsed_config.store(Arc::new(ParsedConfig {
                current_vset,
                signature_context: SignatureContext {
                    global_id: cx.block.as_ref().global_id,
                    capabilities: global.capabilities,
                },
                slasher_params,
            }));

            // Update subscription if changed.
            match (this.subscription.load_full(), &slasher_address) {
                // Slasher has been disabled.
                (_subscription, None) => {
                    // TODO: Notify subscription that it is no longer needed.
                    this.subscription.store(None);
                }
                // Slasher address unchanged.
                (Some(s), Some(slasher_address)) if s.address() == slasher_address => {}
                // Slasher address has changed.
                (_, Some(slasher_address)) => {
                    tracing::info!(%slasher_address, "slasher address changed");
                    this.subscription
                        .store(Some(Arc::new(ContractSubscription::new(slasher_address))));
                }
            }
        }

        // Sync session id.
        let current_session_id = ValidationSessionId::from(state_extra);
        let session_changed = current_session_id != this.known_session_id.load();
        if session_changed {
            // TODO: Add metrics.
            tracing::info!(
                old_session_id = ?this.known_session_id.load(),
                ?current_session_id,
                "slasher observed validation session change",
            );
            this.known_session_id.set(current_session_id);
            this.storage
                .store_vset_session(current_session_id, &current_vset_hash, mc_seqno)?;
        }

        // TODO: Cleanup stored vset sessions.

        // Prepare slasher handler context.
        let Some(slasher_params) = this.parsed_config.load().slasher_params.clone() else {
            // Slasher disabled.
            return Ok(());
        };
        let Some(subscription) = this.subscription.load_full() else {
            return Ok(());
        };

        tracing::trace!(
            ?slasher_params,
            ?current_session_id,
            %current_vset_hash,
        );

        // TODO: Move into blocking.
        let extra = cx.block.load_extra()?.account_blocks.load()?;
        if let Some((_, account_block)) = extra.get(slasher_params.address)? {
            for entry in account_block.transactions.iter() {
                let (_, _, tx) = entry?;
                let tx_hash = tx.repr_hash();
                let tx = tx.load()?;

                tracing::debug!(
                    %tx_hash,
                    msg_hash = ?tx.in_msg.as_ref().map(|msg| msg.repr_hash()),
                    "found slasher transaction",
                );

                let own_message = subscription.handle_account_transaction(tx_hash, &tx)?;

                match self.shared.contract.decode_event(&tx) {
                    Ok(Some(event)) => match event {
                        bc::SlasherContractEvent::SubmitBlocksBatch(submitted) => {
                            let batch = &submitted.blocks_batch;
                            tracing::info!(
                                %tx_hash,
                                %current_vset_hash,
                                validator_idx = submitted.validator_idx,
                                batch_start_seqno = batch.start_seqno(),
                                batch_seqno_after = batch.seqno_after(),
                                batch_slots = batch.committed_blocks.len(),
                                committed_blocks = batch.committed_block_count(),
                                validators = batch.validator_count(),
                                is_own = own_message,
                                "blocks batch submitted",
                            );

                            this.storage.store_blocks_batch(
                                &current_vset_hash,
                                submitted.validator_idx,
                                &submitted.blocks_batch,
                            )?;
                            tokio::task::yield_now().await;
                        }
                    },
                    Ok(None) => {}
                    Err(e) => tracing::warn!(
                        %tx_hash,
                        "failed to parse slasher event: {e:?}"
                    ),
                }
            }
        }

        // Update subscription state.
        subscription.cleanup_expired_messages(cx.block.load_info()?.gen_utime);

        // Trigger reporting.
        if let Some(vset) = vset_to_complete
            && let Some(last_seqno) = mc_seqno.checked_sub(1)
        {
            self.shared
                .complete_vset(&vset, last_seqno, &slasher_params)?;
        }

        // Start session handlers.
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
    parsed_config: ArcSwap<ParsedConfig>,
}

impl SlasherSharedState {
    fn complete_vset(
        &self,
        vset: &ParsedVset,
        last_seqno: u32,
        params: &SlasherParams,
    ) -> Result<()> {
        let Some(own_validator_idx) =
            vset.vset.list.iter().position(|item| {
                item.public_key.as_array() == self.node_keys.public_key.as_bytes()
            })
        else {
            tracing::warn!(vset_hash = %vset.hash, "not in a validator set");
            return Ok(());
        };

        let accusations = analyzer::analyze_vset(
            self.storage.snapshot(),
            vset,
            last_seqno,
            own_validator_idx,
            params,
        )?;
        tracing::warn!("slasher accusations: {accusations:?}");

        Ok(())
    }

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

            let signature_context = self.parsed_config.load().signature_context;
            let params = EncodeBlocksBatchMessage {
                address: subscription.address(),
                session_id,
                batch: &batch,
                validator_idx,
                signature_context,
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
                        batch_start_seqno = batch.start_seqno(),
                        batch_seqno_after = batch.seqno_after(),
                        batch_slots = batch.committed_blocks.len(),
                        committed_blocks = batch.committed_block_count(),
                        validators = batch.validator_count(),
                        "sending own blocks batch to slasher"
                    );
                    self.blockchain_rpc_client
                        .broadcast_external_message(&boc)
                        .await;
                    drop(boc);

                    match res.await {
                        Ok(MessageDelivered { tx_hash }) => {
                            tracing::info!(
                                %tx_hash,
                                session_id = ?params.session_id,
                                validator_idx = params.validator_idx,
                                batch_start_seqno = batch.start_seqno(),
                                batch_seqno_after = batch.seqno_after(),
                                batch_slots = batch.committed_blocks.len(),
                                committed_blocks = batch.committed_block_count(),
                                validators = batch.validator_count(),
                                "own blocks batch delivered"
                            );
                            return;
                        }
                        Err(_) => {
                            // TODO: Execute transaction locally to guess the reason.
                            tracing::warn!("batch message expired");
                        }
                    }
                }
                Err(e) => tracing::warn!("failed to track message: {e:?}"),
            }

            tokio::time::sleep(self.config.message_retry_interval).await;
        }
    }
}

struct ParsedConfig {
    current_vset: Arc<ParsedVset>,
    signature_context: SignatureContext,
    slasher_params: Option<SlasherParams>,
}

struct ParsedVset {
    hash: HashBytes,
    vset: ValidatorSet,
}

impl ParsedVset {
    fn from_raw(raw: Cell) -> Result<Self> {
        let vset = raw.parse::<ValidatorSet>()?;
        Ok(Self {
            hash: *raw.repr_hash(),
            vset,
        })
    }
}
