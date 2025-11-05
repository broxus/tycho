use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use anyhow::{Context, Result};
use async_trait::async_trait;
use error::CollatorError;
use futures_util::future::Future;
use messages_reader::{
    FinalizedMessagesReader, MessagesReader, MessagesReaderContext, ReaderState,
};
use tokio::sync::{Notify, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use tycho_block_util::block::calc_next_block_id_short;
use tycho_block_util::state::{RefMcStateHandle, ShardStateStuff};
use tycho_core::global_config::MempoolGlobalConfig;
use tycho_network::PeerId;
use tycho_types::cell::{Cell, HashBytes};
use tycho_types::merkle::MerkleUpdate;
use tycho_types::models::*;
use tycho_util::futures::JoinTask;
use tycho_util::mem::Reclaimer;
use tycho_util::metrics::{HistogramGuard, HistogramGuardWithLabels};
use tycho_util::sync::rayon_run;
use tycho_util::time::now_millis;
use types::{AnchorInfo, AnchorsCache, MsgsExecutionParamsStuff};
use self::types::{BlockSerializerCache, CollatorStats, PrevData, WorkingState};
use crate::internal_queue::types::EnqueuedMessage;
use crate::mempool::{GetAnchorResult, MempoolAdapter, MempoolAnchorId};
use crate::queue_adapter::MessageQueueAdapter;
use crate::state_node::StateNodeAdapter;
use crate::types::processed_upto::ProcessedUptoInfoExtension;
use crate::types::{
    BlockCollationResult, CollationSessionId, CollationSessionInfo, CollatorConfig,
    DebugDisplay, DisplayBlockIdsIntoIter, McData, TopBlockDescription,
};
use crate::utils::async_queued_dispatcher::{
    AsyncQueuedDispatcher, STANDARD_QUEUED_DISPATCHER_BUFFER_SIZE,
};
use crate::{method_to_queued_async_closure, tracing_targets};
mod debug_info;
mod do_collate;
mod error;
mod execution_manager;
mod messages_buffer;
mod messages_reader;
mod types;
#[cfg(any(test, feature = "bench-helpers"))]
mod test_utils;
#[cfg(feature = "bench-helpers")]
pub mod bench_export {
    pub use super::messages_buffer::{IncludeAllMessages, MessageGroup, MessagesBuffer};
    pub use super::test_utils::make_stub_internal_parsed_message;
    pub use super::types::ParsedMessage;
}
pub use do_collate::{is_first_block_after_prev_master, work_units};
pub use error::CollationCancelReason;
pub use types::{ForceMasterCollation, ShardDescriptionExt};
#[cfg(test)]
#[path = "tests/collator_tests.rs"]
pub(super) mod tests;
#[cfg(test)]
pub(crate) use messages_reader::tests::{TestInternalMessage, TestMessageFactory};
pub struct CollatorContext {
    pub mq_adapter: Arc<dyn MessageQueueAdapter<EnqueuedMessage>>,
    pub mpool_adapter: Arc<dyn MempoolAdapter>,
    pub state_node_adapter: Arc<dyn StateNodeAdapter>,
    pub config: Arc<CollatorConfig>,
    pub collation_session: Arc<CollationSessionInfo>,
    pub listener: Arc<dyn CollatorEventListener>,
    pub shard_id: ShardIdent,
    pub prev_blocks_ids: Vec<BlockId>,
    pub mc_data: Arc<McData>,
    /// Mempool config override for a new genesis
    pub mempool_config_override: Option<MempoolGlobalConfig>,
    /// For graceful collation cancellation
    pub cancel_collation: Arc<Notify>,
}
#[async_trait]
pub trait CollatorFactory: Send + Sync + 'static {
    type Collator: Collator;
    async fn start(&self, cx: CollatorContext) -> Result<Self::Collator>;
}
#[async_trait]
impl<F, FT, R> CollatorFactory for F
where
    F: Fn(CollatorContext) -> FT + Send + Sync + 'static,
    FT: Future<Output = Result<R>> + Send + 'static,
    R: Collator,
{
    type Collator = R;
    async fn start(&self, cx: CollatorContext) -> Result<Self::Collator> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(start)),
            file!(),
            109u32,
        );
        let cx = cx;
        {
            __guard.end_section(110u32);
            let __result = self(cx).await;
            __guard.start_section(110u32);
            __result
        }
    }
}
#[async_trait]
pub trait CollatorEventListener: Send + Sync {
    /// Process block collation skip by any reason
    async fn on_skipped(
        &self,
        prev_mc_block_id: BlockId,
        next_block_id_short: BlockIdShort,
        anchor_chain_time: u64,
        force_mc_block: ForceMasterCollation,
        collation_config: Arc<CollationConfig>,
    ) -> Result<()>;
    /// Handle when collator action was cancelled
    async fn on_cancelled(
        &self,
        prev_mc_block_id: BlockId,
        next_block_id_short: BlockIdShort,
        cancel_reason: CollationCancelReason,
    ) -> Result<()>;
    /// Process new collated shard or master block
    async fn on_block_candidate(
        &self,
        collation_result: BlockCollationResult,
    ) -> Result<()>;
    /// Process collator stopped event
    async fn on_collator_stopped(
        &self,
        collation_session_id: CollationSessionId,
    ) -> Result<()>;
}
#[async_trait]
pub trait Collator: Send + Sync + 'static {
    /// Enqueue collator stop task
    async fn enqueue_stop(&self) -> Result<()>;
    /// Enqueue update `McData` if newer, reset `PrevData` and run next collation attempt
    async fn enqueue_resume_collation(
        &self,
        mc_data: Arc<McData>,
        reset: bool,
        collation_session: Arc<CollationSessionInfo>,
        prev_blocks_ids: Vec<BlockId>,
    ) -> Result<()>;
    /// Enqueue next attemt to collate block
    /// (with check if there are internals or externals for collation).
    /// Check implementation for master and shards for details
    async fn enqueue_try_collate(&self) -> Result<()>;
    /// Enqueue new block collation (without check of internals and externals)
    async fn enqueue_do_collate(
        &self,
        top_shard_blocks_info: Vec<TopBlockDescription>,
        next_chain_time: u64,
    ) -> Result<()>;
}
pub struct CollatorStdImplFactory {
    pub wu_tuner_event_sender: Option<tokio::sync::mpsc::Sender<work_units::WuEvent>>,
}
#[async_trait]
impl CollatorFactory for CollatorStdImplFactory {
    type Collator = AsyncQueuedDispatcher<CollatorStdImpl>;
    async fn start(&self, cx: CollatorContext) -> Result<Self::Collator> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(start)),
            file!(),
            174u32,
        );
        let cx = cx;
        {
            __guard.end_section(189u32);
            let __result = CollatorStdImpl::start(
                    cx.mq_adapter,
                    cx.mpool_adapter,
                    cx.state_node_adapter,
                    cx.config,
                    cx.collation_session,
                    cx.listener,
                    cx.shard_id,
                    cx.prev_blocks_ids,
                    cx.mc_data,
                    cx.mempool_config_override,
                    cx.cancel_collation,
                    self.wu_tuner_event_sender.clone(),
                )
                .await;
            __guard.start_section(189u32);
            __result
        }
    }
}
#[async_trait]
impl Collator for AsyncQueuedDispatcher<CollatorStdImpl> {
    async fn enqueue_stop(&self) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(enqueue_stop)),
            file!(),
            195u32,
        );
        let cancel_token = self.cancel_token().clone();
        {
            __guard.end_section(198u32);
            let __result = self
                .enqueue_task(
                    method_to_queued_async_closure!(stop_collator, cancel_token),
                )
                .await;
            __guard.start_section(198u32);
            __result
        }
    }
    /// Enqueue update `McData` if newer, reset `PrevData` if required and run next collation attempt
    async fn enqueue_resume_collation(
        &self,
        mc_data: Arc<McData>,
        reset: bool,
        collation_session: Arc<CollationSessionInfo>,
        prev_blocks_ids: Vec<BlockId>,
    ) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(enqueue_resume_collation)),
            file!(),
            208u32,
        );
        let mc_data = mc_data;
        let reset = reset;
        let collation_session = collation_session;
        let prev_blocks_ids = prev_blocks_ids;
        {
            __guard.end_section(216u32);
            let __result = self
                .enqueue_task(
                    method_to_queued_async_closure!(
                        resume_collation_wrapper, mc_data, reset, collation_session,
                        prev_blocks_ids
                    ),
                )
                .await;
            __guard.start_section(216u32);
            __result
        }
    }
    async fn enqueue_try_collate(&self) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(enqueue_try_collate)),
            file!(),
            219u32,
        );
        {
            __guard.end_section(223u32);
            let __result = self
                .enqueue_task(
                    method_to_queued_async_closure!(wait_state_and_try_collate_wrapper,),
                )
                .await;
            __guard.start_section(223u32);
            __result
        }
    }
    async fn enqueue_do_collate(
        &self,
        top_shard_blocks_info: Vec<TopBlockDescription>,
        next_chain_time: u64,
    ) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(enqueue_do_collate)),
            file!(),
            230u32,
        );
        let top_shard_blocks_info = top_shard_blocks_info;
        let next_chain_time = next_chain_time;
        {
            __guard.end_section(236u32);
            let __result = self
                .enqueue_task(
                    method_to_queued_async_closure!(
                        wait_state_and_do_collate_wrapper, top_shard_blocks_info,
                        next_chain_time
                    ),
                )
                .await;
            __guard.start_section(236u32);
            __result
        }
    }
}
pub struct CollatorStdImpl {
    next_block_info: BlockIdShort,
    config: Arc<CollatorConfig>,
    collation_session: Arc<CollationSessionInfo>,
    listener: Arc<dyn CollatorEventListener>,
    mq_adapter: Arc<dyn MessageQueueAdapter<EnqueuedMessage>>,
    mpool_adapter: Arc<dyn MempoolAdapter>,
    state_node_adapter: Arc<dyn StateNodeAdapter>,
    shard_id: ShardIdent,
    delayed_working_state: DelayedWorkingState,
    store_new_state_tasks: Vec<StateUpdateContext>,
    /// Refs on states to keep them alive until a Merkle chain is applied
    store_state_refs: VecDeque<Cell>,
    background_store_new_state_tx: tokio::sync::mpsc::UnboundedSender<
        StateUpdateContext,
    >,
    anchors_cache: AnchorsCache,
    block_serializer_cache: BlockSerializerCache,
    stats: CollatorStats,
    timer: std::time::Instant,
    anchor_timer: std::time::Instant,
    shard_blocks_count_from_last_anchor: u16,
    /// Mempool config override for a new genesis
    mempool_config_override: Option<MempoolGlobalConfig>,
    /// For graceful collation cancellation
    cancel_collation: Arc<Notify>,
    /// Events sender for Work Units tuner service
    wu_tuner_event_sender: Option<tokio::sync::mpsc::Sender<work_units::WuEvent>>,
}
impl CollatorStdImpl {
    #[allow(clippy::too_many_arguments)]
    pub async fn start(
        mq_adapter: Arc<dyn MessageQueueAdapter<EnqueuedMessage>>,
        mpool_adapter: Arc<dyn MempoolAdapter>,
        state_node_adapter: Arc<dyn StateNodeAdapter>,
        config: Arc<CollatorConfig>,
        collation_session: Arc<CollationSessionInfo>,
        listener: Arc<dyn CollatorEventListener>,
        shard_id: ShardIdent,
        prev_blocks_ids: Vec<BlockId>,
        mc_data: Arc<McData>,
        mempool_config_override: Option<MempoolGlobalConfig>,
        cancel_collation: Arc<Notify>,
        wu_tuner_event_sender: Option<tokio::sync::mpsc::Sender<work_units::WuEvent>>,
    ) -> Result<AsyncQueuedDispatcher<Self>> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(start)),
            file!(),
            292u32,
        );
        let mq_adapter = mq_adapter;
        let mpool_adapter = mpool_adapter;
        let state_node_adapter = state_node_adapter;
        let config = config;
        let collation_session = collation_session;
        let listener = listener;
        let shard_id = shard_id;
        let prev_blocks_ids = prev_blocks_ids;
        let mc_data = mc_data;
        let mempool_config_override = mempool_config_override;
        let cancel_collation = cancel_collation;
        let wu_tuner_event_sender = wu_tuner_event_sender;
        const BLOCK_CELL_COUNT_BASELINE: usize = 100_000;
        let next_block_info = calc_next_block_id_short(&prev_blocks_ids);
        tracing::info!(
            target : tracing_targets::COLLATOR,
            "(next_block_id={}): collator starting...", next_block_info,
        );
        let (working_state_tx, working_state_rx) = oneshot::channel::<
            Result<Box<WorkingState>>,
        >();
        let (background_store_new_state_tx, mut background_store_new_state_rx) = tokio::sync::mpsc::unbounded_channel::<
            StateUpdateContext,
        >();
        let store_state_refs = VecDeque::with_capacity(config.merkle_chain_limit);
        let processor = Self {
            next_block_info,
            config,
            collation_session,
            listener,
            mq_adapter,
            mpool_adapter,
            state_node_adapter,
            shard_id,
            delayed_working_state: DelayedWorkingState::new(
                shard_id,
                async move {
                    let mut __guard = crate::__async_profile_guard__::Guard::new(
                        concat!(module_path!(), "::async_block"),
                        file!(),
                        317u32,
                    );
                    match {
                        __guard.end_section(318u32);
                        let __result = working_state_rx.await;
                        __guard.start_section(318u32);
                        __result
                    } {
                        Ok(state) => state,
                        Err(_) => anyhow::bail!("collator init cancelled"),
                    }
                },
            ),
            store_new_state_tasks: Default::default(),
            store_state_refs,
            background_store_new_state_tx,
            anchors_cache: Default::default(),
            block_serializer_cache: BlockSerializerCache::with_capacity(
                BLOCK_CELL_COUNT_BASELINE,
            ),
            stats: Default::default(),
            timer: std::time::Instant::now(),
            anchor_timer: std::time::Instant::now(),
            shard_blocks_count_from_last_anchor: 0,
            mempool_config_override,
            cancel_collation,
            wu_tuner_event_sender,
        };
        tokio::spawn(async move {
            let mut __guard = crate::__async_profile_guard__::Guard::new(
                concat!(module_path!(), "::async_block"),
                file!(),
                338u32,
            );
            while let Some(cx) = {
                __guard.end_section(339u32);
                let __result = background_store_new_state_rx.recv().await;
                __guard.start_section(339u32);
                __result
            } {
                __guard.checkpoint(339u32);
                if let Err(err) = {
                    __guard.end_section(340u32);
                    let __result = cx.store_new_state_task.await;
                    __guard.start_section(340u32);
                    __result
                } {
                    tracing::error!(
                        target : tracing_targets::COLLATOR,
                        "Error when store new state: {:?}", err,
                    );
                }
                Reclaimer::instance().drop(cx.state_update);
            }
        });
        let dispatcher = AsyncQueuedDispatcher::create(
            processor,
            STANDARD_QUEUED_DISPATCHER_BUFFER_SIZE,
        );
        tracing::trace!(
            target : tracing_targets::COLLATOR,
            "(next_block_id={}): collator tasks queue dispatcher started",
            next_block_info,
        );
        {
            __guard.end_section(367u32);
            let __result = dispatcher
                .enqueue_task(
                    method_to_queued_async_closure!(
                        init_collator_wrapper, prev_blocks_ids, mc_data, working_state_tx
                    ),
                )
                .await;
            __guard.start_section(367u32);
            __result
        }
            .context("task receiver had to be not closed or dropped here")?;
        tracing::info!(
            target : tracing_targets::COLLATOR,
            "(next_block_id={}): collator initialization task enqueued", next_block_info,
        );
        tracing::info!(
            target : tracing_targets::COLLATOR, "(next_block_id={}): collator started",
            next_block_info,
        );
        Ok(dispatcher)
    }
    async fn stop_collator(
        &mut self,
        dispatcher_cancel_token: CancellationToken,
    ) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(stop_collator)),
            file!(),
            380u32,
        );
        let dispatcher_cancel_token = dispatcher_cancel_token;
        {
            __guard.end_section(383u32);
            let __result = self
                .listener
                .on_collator_stopped(self.collation_session.id())
                .await;
            __guard.start_section(383u32);
            __result
        }?;
        dispatcher_cancel_token.cancel();
        Ok(())
    }
    async fn init_collator_wrapper(
        &mut self,
        prev_blocks_ids: Vec<BlockId>,
        mc_data: Arc<McData>,
        working_state_tx: oneshot::Sender<Result<Box<WorkingState>>>,
    ) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(init_collator_wrapper)),
            file!(),
            393u32,
        );
        let prev_blocks_ids = prev_blocks_ids;
        let mc_data = mc_data;
        let working_state_tx = working_state_tx;
        {
            __guard.end_section(395u32);
            let __result = self
                .init_collator(prev_blocks_ids, mc_data, working_state_tx)
                .await;
            __guard.start_section(395u32);
            __result
        }
            .with_context(|| format!("next_block_id: {}", self.next_block_info))
    }
    #[tracing::instrument(skip_all, fields(next_block_id = %self.next_block_info))]
    async fn init_collator(
        &mut self,
        prev_blocks_ids: Vec<BlockId>,
        mc_data: Arc<McData>,
        working_state_tx: oneshot::Sender<Result<Box<WorkingState>>>,
    ) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(init_collator)),
            file!(),
            406u32,
        );
        let prev_blocks_ids = prev_blocks_ids;
        let mc_data = mc_data;
        let working_state_tx = working_state_tx;
        let labels = [("workchain", self.shard_id.workchain().to_string())];
        tracing::info!(target : tracing_targets::COLLATOR, "initializing...");
        let mut working_state = {
            __guard.end_section(418u32);
            let __result = Self::init_working_state(
                    &self.next_block_info,
                    self.state_node_adapter.clone(),
                    mc_data,
                    prev_blocks_ids,
                )
                .await;
            __guard.start_section(418u32);
            __result
        }?;
        let prev_shard_data = working_state.prev_shard_data_ref();
        let prev_block_id = prev_shard_data.blocks_ids()[0];
        let anchors_processing_info_opt = Self::get_anchors_processing_info(
            &working_state.next_block_id_short.shard,
            &working_state.mc_data,
            &prev_block_id,
            prev_shard_data.gen_chain_time(),
            prev_shard_data.processed_upto().get_min_externals_processed_to()?,
        );
        if let Some(anchors_processing_info) = anchors_processing_info_opt {
            let timer = std::time::Instant::now();
            tracing::info!(
                target : tracing_targets::COLLATOR, % anchors_processing_info,
                "will check and import init anchors on init",
            );
            let cancel_collation = self.cancel_collation.clone();
            let collation_cancelled = cancel_collation.notified();
            let import_fut = self
                .check_and_import_init_anchors(&working_state, anchors_processing_info);
            let import_init_anchors_res = {
                __guard.end_section(449u32);
                let __result = tokio::select! {
                    res = import_fut => res, _ = collation_cancelled => {
                    tracing::info!(target : tracing_targets::COLLATOR,
                    "collation was cancelled by manager on init",);
                    metrics::counter!("tycho_collator_anchor_import_cancelled_count", &
                    labels) .increment(1); self.listener.on_cancelled(working_state
                    .mc_data.block_id, working_state.next_block_id_short,
                    CollationCancelReason::ExternalCancel,). await ?; return Ok(()); }
                };
                __guard.start_section(449u32);
                __result
            };
            let ImportInitAnchorsResult {
                anchors_info,
                mut anchors_count_above_last_imported_in_current_shard,
            } = match import_init_anchors_res {
                Err(CollatorError::Cancelled(reason)) => {
                    {
                        __guard.end_section(478u32);
                        let __result = self
                            .listener
                            .on_cancelled(
                                working_state.mc_data.block_id,
                                working_state.next_block_id_short,
                                reason,
                            )
                            .await;
                        __guard.start_section(478u32);
                        __result
                    }?;
                    {
                        __guard.end_section(479u32);
                        return Ok(());
                    };
                }
                res => res?,
            };
            if !anchors_info.is_empty() {
                tracing::info!(
                    target : tracing_targets::COLLATOR, elapsed = timer.elapsed()
                    .as_millis(), "imported anchors on init: {:?}", anchors_info
                    .as_slice()
                );
                let wu_used = &mut working_state.wu_used_from_last_anchor;
                let wu_step = working_state
                    .collation_config
                    .wu_used_to_import_next_anchor;
                while anchors_count_above_last_imported_in_current_shard > 0 {
                    __guard.checkpoint(496u32);
                    anchors_count_above_last_imported_in_current_shard -= 1;
                    if let Some(new_wu_used) = wu_used.checked_sub(wu_step) {
                        *wu_used = new_wu_used;
                    } else {
                        {
                            __guard.end_section(501u32);
                            __guard.start_section(501u32);
                            break;
                        };
                    }
                }
            }
        }
        working_state_tx.send(Ok(working_state)).ok();
        self.timer = std::time::Instant::now();
        self.anchor_timer = std::time::Instant::now();
        tracing::info!(target : tracing_targets::COLLATOR, "init finished");
        tracing::info!(
            target : tracing_targets::COLLATOR,
            "trying to collate next block after init..."
        );
        {
            __guard.end_section(517u32);
            let __result = self.wait_state_and_try_collate().await;
            __guard.start_section(517u32);
            __result
        }?;
        Ok(())
    }
    async fn check_and_import_init_anchors(
        &mut self,
        working_state: &WorkingState,
        anchors_proc_info: AnchorsProcessingInfo,
    ) -> Result<ImportInitAnchorsResult, CollatorError> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(check_and_import_init_anchors)),
            file!(),
            526u32,
        );
        let working_state = working_state;
        let anchors_proc_info = anchors_proc_info;
        let genesis_info = self
            .mempool_config_override
            .as_ref()
            .map_or(
                working_state.mc_data.consensus_info.genesis_info,
                |c| { c.genesis_info },
            );
        tracing::debug!(
            target : tracing_targets::COLLATOR, ? genesis_info, ? anchors_proc_info,
            "check_and_import_init_anchors",
        );
        let import_init_anchors = if genesis_info.start_round > 0 {
            if anchors_proc_info.processed_to_anchor_id <= genesis_info.start_round {
                if anchors_proc_info.processed_to_anchor_id == genesis_info.start_round {
                    false
                } else if self.shard_id.is_masterchain() {
                    {
                        __guard.end_section(553u32);
                        return Err(
                            CollatorError::Cancelled(
                                CollationCancelReason::AnchorNotFound(
                                    anchors_proc_info.processed_to_anchor_id,
                                ),
                            ),
                        );
                    };
                } else {
                    false
                }
            } else {
                true
            }
        } else {
            true
        };
        if import_init_anchors {
            tracing::debug!(
                target : tracing_targets::COLLATOR,
                "importing anchors from processed to anchor ({}) with offset ({}) to chain_time {} \
                (current_shard_last_imported_chain_time = {})",
                anchors_proc_info.processed_to_anchor_id, anchors_proc_info
                .processed_to_msgs_offset, anchors_proc_info.last_imported_chain_time,
                anchors_proc_info.current_shard_last_imported_chain_time,
            );
            {
                __guard.end_section(592u32);
                let __result = Self::import_init_anchors(
                        anchors_proc_info.processed_to_anchor_id,
                        anchors_proc_info.processed_to_msgs_offset,
                        anchors_proc_info.last_imported_chain_time,
                        anchors_proc_info.current_shard_last_imported_chain_time,
                        self.shard_id,
                        &mut self.anchors_cache,
                        self.mpool_adapter.clone(),
                    )
                    .await;
                __guard.start_section(592u32);
                __result
            }
        } else {
            let block_stuff = {
                __guard.end_section(600u32);
                let __result = self
                    .state_node_adapter
                    .load_block(&anchors_proc_info.last_imported_in_block_id)
                    .await;
                __guard.start_section(600u32);
                __result
            }?
                .unwrap();
            let created_by = block_stuff
                .load_extra()
                .map_err(|e| CollatorError::Anyhow(e.into()))?
                .created_by;
            let anchor_info = AnchorInfo {
                id: genesis_info.start_round,
                ct: anchors_proc_info.last_imported_chain_time,
                all_exts_count: 0,
                our_exts_count: 0,
                author: PeerId(created_by.0),
            };
            self.anchors_cache.add_imported_anchor_info(anchor_info);
            Ok(Default::default())
        }
    }
    async fn resume_collation_wrapper(
        &mut self,
        mc_data: Arc<McData>,
        reset: bool,
        collation_session: Arc<CollationSessionInfo>,
        new_prev_blocks_ids: Vec<BlockId>,
    ) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(resume_collation_wrapper)),
            file!(),
            625u32,
        );
        let mc_data = mc_data;
        let reset = reset;
        let collation_session = collation_session;
        let new_prev_blocks_ids = new_prev_blocks_ids;
        {
            __guard.end_section(627u32);
            let __result = self
                .resume_collation(mc_data, reset, collation_session, new_prev_blocks_ids)
                .await;
            __guard.start_section(627u32);
            __result
        }
            .with_context(|| format!("next_block_id: {}", self.next_block_info))
    }
    #[tracing::instrument(skip_all, fields(next_block_id = %self.next_block_info))]
    async fn resume_collation(
        &mut self,
        mc_data: Arc<McData>,
        reset: bool,
        collation_session: Arc<CollationSessionInfo>,
        new_prev_blocks_ids: Vec<BlockId>,
    ) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(resume_collation)),
            file!(),
            638u32,
        );
        let mc_data = mc_data;
        let reset = reset;
        let collation_session = collation_session;
        let new_prev_blocks_ids = new_prev_blocks_ids;
        let labels = [("workchain", self.shard_id.workchain().to_string())];
        let histogram = HistogramGuard::begin_with_labels(
            "tycho_collator_resume_collation_time_high",
            &labels,
        );
        self.collation_session = collation_session;
        let mut working_state = if !reset {
            let mut working_state = {
                __guard.end_section(646u32);
                let __result = self.delayed_working_state.wait().await;
                __guard.start_section(646u32);
                __result
            }?;
            tracing::info!(
                target : tracing_targets::COLLATOR, mc_data_block_id = % working_state
                .mc_data.block_id.as_short_id(), "resume collation without reset",
            );
            let prev_mc_seqno = working_state.mc_data.block_id.seqno;
            if prev_mc_seqno < mc_data.block_id.seqno {
                working_state.collation_config = Arc::new(
                    mc_data.config.get_collation_config()?,
                );
                working_state.mc_data = mc_data;
                if working_state.has_unprocessed_messages == Some(false) {
                    working_state.has_unprocessed_messages = None;
                }
                if !self.shard_id.is_masterchain()
                    && !self.store_new_state_tasks.is_empty()
                {
                    let last_task = self
                        .store_new_state_tasks
                        .pop()
                        .expect("shouldn't happen");
                    if last_task.store_new_state_task.is_finished() {
                        {
                            __guard.end_section(671u32);
                            let __result = last_task.store_new_state_task.await;
                            __guard.start_section(671u32);
                            __result
                        }?;
                        {
                            __guard.end_section(679u32);
                            let __result = Self::reload_prev_data(
                                    prev_mc_seqno,
                                    &mut working_state,
                                    self.state_node_adapter.clone(),
                                )
                                .await;
                            __guard.start_section(679u32);
                            __result
                        }?;
                    } else {
                        let mut unfinished_tasks = vec![last_task];
                        while let Some(task) = self.store_new_state_tasks.pop() {
                            __guard.checkpoint(684u32);
                            let is_last = self.store_new_state_tasks.is_empty();
                            if !task.store_new_state_task.is_finished()
                                && unfinished_tasks.len() < self.config.merkle_chain_limit
                                && !is_last
                            {
                                unfinished_tasks.push(task);
                                {
                                    __guard.end_section(693u32);
                                    __guard.start_section(693u32);
                                    continue;
                                };
                            }
                            {
                                __guard.end_section(696u32);
                                let __result = task.store_new_state_task.await;
                                __guard.start_section(696u32);
                                __result
                            }?;
                            let _histogram_apply_merkles = HistogramGuard::begin_with_labels(
                                "tycho_collator_resume_collation_apply_merkles_time_high",
                                &labels,
                            );
                            let mut prev_state = {
                                __guard.end_section(707u32);
                                let __result = self
                                    .state_node_adapter
                                    .load_state(prev_mc_seqno, &task.block_id)
                                    .await;
                                __guard.start_section(707u32);
                                __result
                            }
                                .context("failed to load prev shard state")?;
                            while let Some(task) = unfinished_tasks.pop() {
                                __guard.checkpoint(710u32);
                                let prev_block_id = *prev_state.block_id();
                                prev_state = {
                                    __guard.end_section(723u32);
                                    let __result = rayon_run({
                                            let block_id = task.block_id;
                                            let merkle_update = task.state_update.clone();
                                            move || {
                                                prev_state
                                                    .par_make_next_state(&block_id, &merkle_update, Some(5))
                                            }
                                        })
                                        .await;
                                    __guard.start_section(723u32);
                                    __result
                                }
                                    .with_context(|| {
                                        format!(
                                            "failed to apply merkle of block {} to {prev_block_id}",
                                            task.block_id
                                        )
                                    })?;
                                self.background_store_new_state_tx.send(task)?;
                            }
                            {
                                __guard.end_section(736u32);
                                let __result = Self::update_prev_data(
                                        &mut working_state,
                                        prev_state,
                                    )
                                    .await;
                                __guard.start_section(736u32);
                                __result
                            }?;
                            {
                                __guard.end_section(738u32);
                                __guard.start_section(738u32);
                                break;
                            };
                        }
                    }
                }
            }
            while let Some(cell) = self.store_state_refs.pop_front() {
                __guard.checkpoint(745u32);
                Reclaimer::instance().drop(cell);
            }
            for cx in self.store_new_state_tasks.drain(..) {
                __guard.checkpoint(750u32);
                self.background_store_new_state_tx.send(cx)?;
            }
            working_state
        } else {
            while let Some(cell) = self.store_state_refs.pop_front() {
                __guard.checkpoint(757u32);
                Reclaimer::instance().drop(cell);
            }
            for cx in self.store_new_state_tasks.drain(..) {
                __guard.checkpoint(762u32);
                self.background_store_new_state_tx.send(cx)?;
            }
            self.delayed_working_state.reset();
            self.next_block_info = calc_next_block_id_short(&new_prev_blocks_ids);
            tracing::info!(
                target : tracing_targets::COLLATOR, mc_data_block_id = % mc_data.block_id
                .as_short_id(), new_prev_blocks_ids = % DisplayBlockIdsIntoIter(&
                new_prev_blocks_ids), new_next_block_id = % self.next_block_info,
                "resume collation with reset",
            );
            tracing::debug!(
                target : tracing_targets::COLLATOR, new_next_block_id = % self
                .next_block_info, "reset working state and msgs buffer",
            );
            let mut working_state = {
                __guard.end_section(789u32);
                let __result = Self::init_working_state(
                        &self.next_block_info,
                        self.state_node_adapter.clone(),
                        mc_data,
                        new_prev_blocks_ids,
                    )
                    .await;
                __guard.start_section(789u32);
                __result
            }?;
            let prev_shard_data = working_state.prev_shard_data_ref();
            let prev_block_id = prev_shard_data.blocks_ids()[0];
            let prev_shard_data_gen_chain_time = prev_shard_data.gen_chain_time();
            let anchors_processing_info_opt = Self::get_anchors_processing_info(
                &working_state.next_block_id_short.shard,
                &working_state.mc_data,
                &prev_block_id,
                prev_shard_data_gen_chain_time,
                prev_shard_data.processed_upto().get_min_externals_processed_to()?,
            );
            if let Some(anchors_processing_info) = anchors_processing_info_opt {
                let timer = std::time::Instant::now();
                tracing::debug!(
                    target : tracing_targets::COLLATOR, % anchors_processing_info,
                    "will check and import init anchors on resume",
                );
                let cancel_collation = self.cancel_collation.clone();
                let collation_cancelled = cancel_collation.notified();
                let import_fut = self
                    .check_and_import_init_anchors(
                        &working_state,
                        anchors_processing_info,
                    );
                let import_init_anchors_res = {
                    __guard.end_section(821u32);
                    let __result = tokio::select! {
                        res = import_fut => res, _ = collation_cancelled => {
                        tracing::info!(target : tracing_targets::COLLATOR,
                        "collation was cancelled by manager on resume",);
                        metrics::counter!("tycho_collator_anchor_import_cancelled_count",
                        & labels) .increment(1); self.listener.on_cancelled(working_state
                        .mc_data.block_id, working_state.next_block_id_short,
                        CollationCancelReason::ExternalCancel,). await ?; return Ok(());
                        }
                    };
                    __guard.start_section(821u32);
                    __result
                };
                let ImportInitAnchorsResult {
                    anchors_info,
                    mut anchors_count_above_last_imported_in_current_shard,
                } = match import_init_anchors_res {
                    Err(CollatorError::Cancelled(reason)) => {
                        {
                            __guard.end_section(850u32);
                            let __result = self
                                .listener
                                .on_cancelled(
                                    working_state.mc_data.block_id,
                                    working_state.next_block_id_short,
                                    reason,
                                )
                                .await;
                            __guard.start_section(850u32);
                            __result
                        }?;
                        {
                            __guard.end_section(851u32);
                            return Ok(());
                        };
                    }
                    res => res?,
                };
                if !anchors_info.is_empty() {
                    tracing::debug!(
                        target : tracing_targets::COLLATOR, elapsed = timer.elapsed()
                        .as_millis(), new_next_block_id = % self.next_block_info,
                        "imported anchors on resume: {:?}", anchors_info.as_slice(),
                    );
                    let wu_used = &mut working_state.wu_used_from_last_anchor;
                    let wu_step = working_state
                        .collation_config
                        .wu_used_to_import_next_anchor;
                    while anchors_count_above_last_imported_in_current_shard > 0 {
                        __guard.checkpoint(869u32);
                        anchors_count_above_last_imported_in_current_shard -= 1;
                        if let Some(new_wu_used) = wu_used.checked_sub(wu_step) {
                            *wu_used = new_wu_used;
                        } else {
                            {
                                __guard.end_section(874u32);
                                __guard.start_section(874u32);
                                break;
                            };
                        }
                    }
                }
            }
            working_state
        };
        working_state.resume_collation_elapsed = histogram.finish();
        if self.shard_id.is_masterchain() {
            {
                __guard.end_section(887u32);
                let __result = self
                    .try_collate_next_master_block_impl(working_state)
                    .await;
                __guard.start_section(887u32);
                __result
            }
        } else {
            {
                __guard.end_section(889u32);
                let __result = self
                    .try_collate_next_shard_block_impl(working_state)
                    .await;
                __guard.start_section(889u32);
                __result
            }
        }
    }
    #[tracing::instrument(skip_all, fields(next_block_id = %next_block_id_short))]
    async fn init_working_state(
        next_block_id_short: &BlockIdShort,
        state_node_adapter: Arc<dyn StateNodeAdapter>,
        mc_data: Arc<McData>,
        prev_blocks_ids: Vec<BlockId>,
    ) -> Result<Box<WorkingState>> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(init_working_state)),
            file!(),
            899u32,
        );
        let next_block_id_short = next_block_id_short;
        let state_node_adapter = state_node_adapter;
        let mc_data = mc_data;
        let prev_blocks_ids = prev_blocks_ids;
        tracing::debug!(
            target : tracing_targets::COLLATOR, prev_blocks_ids = %
            DisplayBlockIdsIntoIter(& prev_blocks_ids),
            "loading prev states and queue diffs...",
        );
        let (prev_states, prev_queue_diff_hashes) = {
            __guard.end_section(910u32);
            let __result = Self::load_states_and_diffs(
                    mc_data.block_id.seqno,
                    state_node_adapter,
                    prev_blocks_ids,
                )
                .await;
            __guard.start_section(910u32);
            __result
        }?;
        tracing::debug!(target : tracing_targets::COLLATOR, "building working state...");
        Self::build_and_validate_init_working_state(
            mc_data,
            prev_states,
            prev_queue_diff_hashes,
        )
    }
    async fn update_prev_data(
        working_state: &mut WorkingState,
        prev_state: ShardStateStuff,
    ) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(update_prev_data)),
            file!(),
            921u32,
        );
        let working_state = working_state;
        let prev_state = prev_state;
        working_state.usage_tree.take();
        let prev_shard_data = working_state
            .prev_shard_data
            .as_ref()
            .expect("should exist here");
        let prev_queue_diff_hashes = prev_shard_data.prev_queue_diff_hashes().clone();
        tracing::debug!(
            target : tracing_targets::COLLATOR,
            "updating prev data in working state from built pure state root..."
        );
        let (prev_shard_data, usage_tree) = PrevData::build(
            vec![prev_state],
            prev_queue_diff_hashes,
        )?;
        working_state.prev_shard_data = Some(prev_shard_data);
        working_state.usage_tree = Some(usage_tree);
        Ok(())
    }
    async fn reload_prev_data(
        prev_mc_seqno: u32,
        working_state: &mut WorkingState,
        state_node_adapter: Arc<dyn StateNodeAdapter>,
    ) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(reload_prev_data)),
            file!(),
            949u32,
        );
        let prev_mc_seqno = prev_mc_seqno;
        let working_state = working_state;
        let state_node_adapter = state_node_adapter;
        working_state.usage_tree.take();
        let prev_shard_data = working_state
            .prev_shard_data
            .as_ref()
            .expect("should exist here");
        let prev_queue_diff_hashes = prev_shard_data.prev_queue_diff_hashes().clone();
        let prev_blocks_ids = prev_shard_data.blocks_ids();
        tracing::debug!(
            target : tracing_targets::COLLATOR, prev_blocks_ids = %
            DisplayBlockIdsIntoIter(prev_blocks_ids), "loading prev states...",
        );
        let prev_states = {
            __guard.end_section(968u32);
            let __result = Self::load_prev_states(
                    prev_mc_seqno,
                    state_node_adapter.as_ref(),
                    prev_blocks_ids,
                )
                .await;
            __guard.start_section(968u32);
            __result
        }?;
        tracing::debug!(
            target : tracing_targets::COLLATOR,
            "updating prev data in working state from reloaded state root..."
        );
        let (prev_shard_data, usage_tree) = PrevData::build(
            prev_states,
            prev_queue_diff_hashes,
        )?;
        working_state.prev_shard_data = Some(prev_shard_data);
        working_state.usage_tree = Some(usage_tree);
        Ok(())
    }
    /// Build working state update that would be applied before next collation
    #[allow(clippy::too_many_arguments)]
    fn prepare_working_state_update(
        &mut self,
        block_id: BlockId,
        new_observable_state: Box<ShardStateUnsplit>,
        new_observable_state_root: Cell,
        state_update: MerkleUpdate,
        store_new_state_task: JoinTask<Result<bool>>,
        new_queue_diff_hash: HashBytes,
        new_mc_data: Arc<McData>,
        collation_config: Arc<CollationConfig>,
        has_unprocessed_messages: bool,
        reader_state: ReaderState,
        ref_mc_state_handle: RefMcStateHandle,
        resume_collation_elapsed: Duration,
    ) -> Result<()> {
        enum GetNewShardStateStuff {
            ReloadFromStorage(JoinTask<Result<bool>>),
            BuildFromNewObservable {
                block_id: BlockId,
                new_observable_state: Box<ShardStateUnsplit>,
                new_observable_state_root: Cell,
            },
        }
        let get_new_state_stuff = if block_id.is_masterchain() {
            GetNewShardStateStuff::ReloadFromStorage(store_new_state_task)
        } else {
            self.store_new_state_tasks
                .push(StateUpdateContext {
                    block_id,
                    store_new_state_task,
                    state_update,
                });
            if self.store_state_refs.len() == self.config.merkle_chain_limit
                && let Some(old_ref) = self.store_state_refs.pop_front()
            {
                Reclaimer::instance().drop(old_ref);
            }
            self.store_state_refs.push_back(new_observable_state_root.clone());
            GetNewShardStateStuff::BuildFromNewObservable {
                block_id,
                new_observable_state,
                new_observable_state_root,
            }
        };
        let state_node_adapter = self.state_node_adapter.clone();
        let labels = [("workchain", self.shard_id.workchain().to_string())];
        self.delayed_working_state.future = Some(
            Box::pin(async move {
                let mut __guard = crate::__async_profile_guard__::Guard::new(
                    concat!(module_path!(), "::async_block"),
                    file!(),
                    1038u32,
                );
                let _histogram = HistogramGuard::begin_with_labels(
                    "tycho_collator_build_new_state_time_high",
                    &labels,
                );
                let new_state_stuff = match get_new_state_stuff {
                    GetNewShardStateStuff::BuildFromNewObservable {
                        block_id,
                        new_observable_state,
                        new_observable_state_root,
                    } => {
                        ShardStateStuff::from_state_and_root(
                            &block_id,
                            new_observable_state,
                            new_observable_state_root,
                            ref_mc_state_handle,
                        )?
                    }
                    GetNewShardStateStuff::ReloadFromStorage(store_new_state_task) => {
                        {
                            __guard.end_section(1056u32);
                            let __result = store_new_state_task.await;
                            __guard.start_section(1056u32);
                            __result
                        }?;
                        let load_task = JoinTask::new({
                            async move {
                                let mut __guard = crate::__async_profile_guard__::Guard::new(
                                    concat!(module_path!(), "::async_block"),
                                    file!(),
                                    1059u32,
                                );
                                {
                                    __guard.end_section(1062u32);
                                    let __result = state_node_adapter
                                        .load_state(block_id.seqno, &block_id)
                                        .await;
                                    __guard.start_section(1062u32);
                                    __result
                                }
                            }
                        });
                        {
                            __guard.end_section(1065u32);
                            let __result = load_task.await;
                            __guard.start_section(1065u32);
                            __result
                        }?
                    }
                };
                let prev_states = vec![new_state_stuff];
                let prev_queue_diff_hashes = vec![new_queue_diff_hash];
                let (prev_shard_data, usage_tree) = PrevData::build(
                    prev_states,
                    prev_queue_diff_hashes,
                )?;
                let next_block_id_short = calc_next_block_id_short(
                    prev_shard_data.blocks_ids(),
                );
                Ok(
                    Box::new(WorkingState {
                        next_block_id_short,
                        mc_data: new_mc_data,
                        collation_config,
                        wu_used_from_last_anchor: prev_shard_data
                            .wu_used_from_last_anchor(),
                        resume_collation_elapsed,
                        prev_shard_data: Some(prev_shard_data),
                        usage_tree: Some(usage_tree),
                        has_unprocessed_messages: Some(has_unprocessed_messages),
                        reader_state,
                    }),
                )
            }),
        );
        Ok(())
    }
    /// Load required prev states and prev queue diff hashes
    async fn load_states_and_diffs(
        ref_by_mc_seqno: u32,
        state_node_adapter: Arc<dyn StateNodeAdapter>,
        prev_blocks_ids: Vec<BlockId>,
    ) -> Result<(Vec<ShardStateStuff>, Vec<HashBytes>)> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(load_states_and_diffs)),
            file!(),
            1097u32,
        );
        let ref_by_mc_seqno = ref_by_mc_seqno;
        let state_node_adapter = state_node_adapter;
        let prev_blocks_ids = prev_blocks_ids;
        let load_state_fut: JoinTask<Result<Vec<ShardStateStuff>>> = JoinTask::new({
            let state_node_adapter = state_node_adapter.clone();
            let prev_blocks_ids = prev_blocks_ids.clone();
            let span = tracing::Span::current();
            async move {
                let mut __guard = crate::__async_profile_guard__::Guard::new(
                    concat!(module_path!(), "::async_block"),
                    file!(),
                    1103u32,
                );
                {
                    __guard.end_section(1109u32);
                    let __result = Self::load_prev_states(
                            ref_by_mc_seqno,
                            state_node_adapter.as_ref(),
                            &prev_blocks_ids,
                        )
                        .await;
                    __guard.start_section(1109u32);
                    __result
                }
            }
                .instrument(span)
        });
        let prev_hashes_fut: JoinTask<Result<Vec<HashBytes>>> = JoinTask::new({
            let span = tracing::Span::current();
            async move {
                let mut __guard = crate::__async_profile_guard__::Guard::new(
                    concat!(module_path!(), "::async_block"),
                    file!(),
                    1116u32,
                );
                let mut prev_hashes = vec![];
                for prev_block_id in prev_blocks_ids {
                    __guard.checkpoint(1118u32);
                    if let Some(diff) = {
                        __guard.end_section(1120u32);
                        let __result = state_node_adapter
                            .load_diff(&prev_block_id)
                            .await;
                        __guard.start_section(1120u32);
                        __result
                    }? {
                        tracing::debug!(
                            target : tracing_targets::COLLATOR,
                            "loaded queue diff for prev_block_id {}", prev_block_id
                            .as_short_id(),
                        );
                        prev_hashes.push(*diff.diff_hash());
                    }
                }
                Ok(prev_hashes)
            }
                .instrument(span)
        });
        let (prev_states, prev_hash) = {
            __guard.end_section(1134u32);
            let __result = futures_util::future::join(load_state_fut, prev_hashes_fut)
                .await;
            __guard.start_section(1134u32);
            __result
        };
        Ok((prev_states?, prev_hash?))
    }
    async fn load_prev_states(
        prev_mc_seqno: u32,
        state_node_adapter: &dyn StateNodeAdapter,
        prev_blocks_ids: &[BlockId],
    ) -> Result<Vec<ShardStateStuff>> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(load_prev_states)),
            file!(),
            1143u32,
        );
        let prev_mc_seqno = prev_mc_seqno;
        let state_node_adapter = state_node_adapter;
        let prev_blocks_ids = prev_blocks_ids;
        let mut prev_states = vec![];
        for prev_block_id in prev_blocks_ids {
            __guard.checkpoint(1145u32);
            let state = {
                __guard.end_section(1148u32);
                let __result = state_node_adapter
                    .load_state(prev_mc_seqno, prev_block_id)
                    .await;
                __guard.start_section(1148u32);
                __result
            }?;
            tracing::debug!(
                target : tracing_targets::COLLATOR,
                "loaded prev shard state for prev_block_id {}", prev_block_id
                .as_short_id(),
            );
            prev_states.push(state);
        }
        Ok(prev_states)
    }
    /// Build working state structure:
    /// * master state
    /// * observable previous state
    /// * usage tree that tracks data access to state cells
    ///
    /// Perform some validations on state
    fn build_and_validate_init_working_state(
        mc_data: Arc<McData>,
        prev_states: Vec<ShardStateStuff>,
        prev_queue_diff_hashes: Vec<HashBytes>,
    ) -> Result<Box<WorkingState>> {
        let (prev_shard_data, usage_tree) = PrevData::build(
            prev_states,
            prev_queue_diff_hashes,
        )?;
        let next_block_id_short = calc_next_block_id_short(prev_shard_data.blocks_ids());
        let collation_config = Arc::new(mc_data.config.get_collation_config()?);
        Ok(
            Box::new(WorkingState {
                next_block_id_short,
                mc_data,
                wu_used_from_last_anchor: prev_shard_data.wu_used_from_last_anchor(),
                resume_collation_elapsed: Duration::ZERO,
                reader_state: ReaderState::new(prev_shard_data.processed_upto()),
                prev_shard_data: Some(prev_shard_data),
                usage_tree: Some(usage_tree),
                has_unprocessed_messages: None,
                collation_config,
            }),
        )
    }
    fn get_anchors_processing_info(
        shard_id: &ShardIdent,
        mc_data: &McData,
        prev_block_id: &BlockId,
        prev_gen_chain_time: u64,
        prev_externals_processed_to: (MempoolAnchorId, u64),
    ) -> Option<AnchorsProcessingInfo> {
        let mut from_mc_info_opt = None;
        if !shard_id.is_masterchain() {
            let (mc_processed_to_anchor_id, mc_processed_to_msgs_offset) = mc_data
                .processed_upto
                .get_min_externals_processed_to()
                .unwrap_or_default();
            if mc_processed_to_anchor_id > 0 {
                for (top_shard_id, top_shard_descr) in mc_data.shards.iter() {
                    if shard_id == top_shard_id {
                        if prev_block_id.seqno == top_shard_descr.seqno
                            && !top_shard_descr.top_sc_block_updated
                        {
                            from_mc_info_opt = Some(AnchorsProcessingInfo {
                                processed_to_anchor_id: mc_processed_to_anchor_id,
                                processed_to_msgs_offset: mc_processed_to_msgs_offset,
                                last_imported_chain_time: mc_data.gen_chain_time,
                                last_imported_in_block_id: mc_data.block_id,
                                current_shard_last_imported_chain_time: prev_gen_chain_time,
                            });
                        }
                        break;
                    }
                }
            }
        }
        let from_prev_info_opt = match prev_externals_processed_to {
            (
                processed_to_anchor_id,
                processed_to_msgs_offset,
            ) if processed_to_anchor_id > 0 => {
                Some(AnchorsProcessingInfo {
                    processed_to_anchor_id,
                    processed_to_msgs_offset,
                    last_imported_chain_time: prev_gen_chain_time,
                    last_imported_in_block_id: *prev_block_id,
                    current_shard_last_imported_chain_time: prev_gen_chain_time,
                })
            }
            _ => None,
        };
        match (from_mc_info_opt, from_prev_info_opt) {
            (Some(from_mc_info), Some(from_prev_info)) => {
                if from_mc_info.processed_to_anchor_id
                    > from_prev_info.processed_to_anchor_id
                    || (from_mc_info.processed_to_anchor_id
                        == from_prev_info.processed_to_anchor_id
                        && from_mc_info.processed_to_msgs_offset
                            > from_prev_info.processed_to_msgs_offset)
                {
                    Some(from_mc_info)
                } else {
                    Some(from_prev_info)
                }
            }
            (from_mc_info_opt, None) => from_mc_info_opt,
            (None, from_prev_info_opt) => from_prev_info_opt,
        }
    }
    /// 1. Get last imported anchor id from cache
    /// 2. Await next anchor via mempool adapter
    /// 3. Store anchor in cache and return it
    async fn import_next_anchor(
        shard_id: ShardIdent,
        anchors_cache: &mut AnchorsCache,
        mpool_adapter: Arc<dyn MempoolAdapter>,
        top_processed_to_anchor: MempoolAnchorId,
        max_consensus_lag_rounds: u32,
    ) -> Result<ImportNextAnchor> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(import_next_anchor)),
            file!(),
            1274u32,
        );
        let shard_id = shard_id;
        let anchors_cache = anchors_cache;
        let mpool_adapter = mpool_adapter;
        let top_processed_to_anchor = top_processed_to_anchor;
        let max_consensus_lag_rounds = max_consensus_lag_rounds;
        let labels = [("workchain", shard_id.workchain().to_string())];
        let _histogram = HistogramGuardWithLabels::begin(
            "tycho_collator_import_next_anchor_time_high",
            &labels,
        );
        let timer = std::time::Instant::now();
        let (prev_anchor_id, ct) = anchors_cache
            .get_last_imported_anchor_id_and_ct()
            .unwrap_or_default();
        if prev_anchor_id.saturating_sub(top_processed_to_anchor)
            > max_consensus_lag_rounds.saturating_mul(2).saturating_div(3)
        {
            metrics::counter!("tycho_collator_anchor_import_skipped_count", & labels)
                .increment(1);
            {
                __guard.end_section(1292u32);
                return Ok(ImportNextAnchor::Skipped);
            };
        }
        let requested_at = now_millis();
        let get_anchor_result = {
            __guard.end_section(1297u32);
            let __result = mpool_adapter.get_next_anchor(prev_anchor_id).await;
            __guard.start_section(1297u32);
            __result
        }?;
        let has_our_externals = match &get_anchor_result {
            GetAnchorResult::Exist(next_anchor) => {
                let our_exts_count = next_anchor.count_externals_for(&shard_id, 0);
                anchors_cache.add(next_anchor.clone(), our_exts_count);
                let has_externals = our_exts_count > 0;
                metrics::counter!("tycho_collator_ext_msgs_imported_count", & labels)
                    .increment(our_exts_count as _);
                metrics::gauge!("tycho_collator_ext_msgs_imported_queue_size", & labels)
                    .increment(our_exts_count as f64);
                let chain_time_elapsed = next_anchor.chain_time.saturating_sub(ct);
                tracing::info!(
                    target : tracing_targets::COLLATOR, elapsed = timer.elapsed()
                    .as_millis(), chain_time_elapsed,
                    "imported next anchor (id: {}, chain_time: {}, all_exts: {}, our_exts: {})",
                    next_anchor.id, next_anchor.chain_time, next_anchor.externals.len(),
                    our_exts_count,
                );
                has_externals
            }
            GetAnchorResult::NotExist => false,
        };
        Ok(ImportNextAnchor::Result {
            prev_anchor_id,
            get_anchor_result,
            has_our_externals,
            requested_at,
        })
    }
    /// 1. Get `processed_to` anchor
    /// 2. Get next anchors until `last_block_chain_time`
    /// 3. Store anchors in cache
    pub(self) async fn import_init_anchors(
        processed_to_anchor_id: MempoolAnchorId,
        processed_to_msgs_offset: u64,
        last_block_chain_time: u64,
        current_shard_last_imported_chain_time: u64,
        shard_id: ShardIdent,
        anchors_cache: &mut AnchorsCache,
        mpool_adapter: Arc<dyn MempoolAdapter>,
    ) -> Result<ImportInitAnchorsResult, CollatorError> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(import_init_anchors)),
            file!(),
            1347u32,
        );
        let processed_to_anchor_id = processed_to_anchor_id;
        let processed_to_msgs_offset = processed_to_msgs_offset;
        let last_block_chain_time = last_block_chain_time;
        let current_shard_last_imported_chain_time = current_shard_last_imported_chain_time;
        let shard_id = shard_id;
        let anchors_cache = anchors_cache;
        let mpool_adapter = mpool_adapter;
        let labels = [("workchain", shard_id.workchain().to_string())];
        let mut res = ImportInitAnchorsResult::default();
        let mut last_anchor = None;
        let mut all_anchors_are_taken_from_cache = false;
        let mut processed_to_anchor_exists_in_cache = false;
        anchors_cache.remove_last_imported_above(last_block_chain_time);
        for anchor in anchors_cache.iter().map(|(_, ca)| &ca.anchor) {
            __guard.checkpoint(1363u32);
            if anchor.id >= processed_to_anchor_id {
                if anchor.id == processed_to_anchor_id {
                    processed_to_anchor_exists_in_cache = true;
                    if anchor.externals.len() == processed_to_msgs_offset as usize {
                        {
                            __guard.end_section(1371u32);
                            __guard.start_section(1371u32);
                            continue;
                        };
                    }
                }
                if !processed_to_anchor_exists_in_cache {
                    {
                        __guard.end_section(1384u32);
                        __guard.start_section(1384u32);
                        break;
                    };
                }
                if anchor.chain_time == last_block_chain_time {
                    all_anchors_are_taken_from_cache = true;
                }
                let our_exts_count = anchor.count_externals_for(&shard_id, 0);
                res.anchors_info
                    .push(
                        InitAnchorSource::FromCache(
                            AnchorInfo::from_anchor(anchor, our_exts_count),
                        ),
                    );
                last_anchor = Some(anchor.clone());
            }
        }
        if all_anchors_are_taken_from_cache {
            let Some(InitAnchorSource::FromCache(_)) = res.anchors_info.last() else {
                {
                    __guard.end_section(1408u32);
                    return Err(
                        CollatorError::Anyhow(
                            anyhow::anyhow!(
                                "`anchors_info` should contain almost one `FromCache` here"
                            ),
                        ),
                    );
                };
            };
            {
                __guard.end_section(1413u32);
                return Ok(res);
            };
        }
        if !processed_to_anchor_exists_in_cache {
            anchors_cache.clear();
            metrics::gauge!("tycho_collator_ext_msgs_imported_queue_size", & labels)
                .set(0);
        }
        let mut prev_anchor_id;
        let mut last_imported_anchor_ct;
        let mut next_anchor = match last_anchor {
            Some(anchor) => {
                prev_anchor_id = anchor.id;
                last_imported_anchor_ct = anchor.chain_time;
                None
            }
            None => {
                let GetAnchorResult::Exist(anchor) = {
                    __guard.end_section(1433u32);
                    let __result = mpool_adapter
                        .get_anchor_by_id(processed_to_anchor_id)
                        .await;
                    __guard.start_section(1433u32);
                    __result
                }? else {
                    {
                        __guard.end_section(1435u32);
                        return Err(
                            CollatorError::Cancelled(
                                CollationCancelReason::AnchorNotFound(
                                    processed_to_anchor_id,
                                ),
                            ),
                        );
                    };
                };
                prev_anchor_id = anchor.id;
                last_imported_anchor_ct = anchor.chain_time;
                Some(anchor)
            }
        };
        loop {
            __guard.checkpoint(1445u32);
            if let Some(anchor) = next_anchor {
                let our_exts_count = anchor.count_externals_for(&shard_id, 0);
                if anchor.id == processed_to_anchor_id
                    && anchor.externals.len() == processed_to_msgs_offset as usize
                {
                    anchors_cache
                        .add_imported_anchor_info(
                            AnchorInfo::from_anchor(&anchor, our_exts_count),
                        );
                } else {
                    anchors_cache.add(anchor.clone(), our_exts_count);
                    metrics::counter!("tycho_collator_ext_msgs_imported_count", & labels)
                        .increment(our_exts_count as u64);
                    metrics::gauge!(
                        "tycho_collator_ext_msgs_imported_queue_size", & labels
                    )
                        .increment(our_exts_count as f64);
                }
                res.anchors_info
                    .push(
                        InitAnchorSource::Imported(
                            AnchorInfo::from_anchor(&anchor, our_exts_count),
                        ),
                    );
            }
            if last_imported_anchor_ct > current_shard_last_imported_chain_time {
                res.anchors_count_above_last_imported_in_current_shard += 1;
            }
            if last_imported_anchor_ct >= last_block_chain_time {
                {
                    __guard.end_section(1482u32);
                    __guard.start_section(1482u32);
                    break;
                };
            }
            let GetAnchorResult::Exist(anchor) = {
                __guard.end_section(1486u32);
                let __result = mpool_adapter.get_next_anchor(prev_anchor_id).await;
                __guard.start_section(1486u32);
                __result
            }? else {
                {
                    __guard.end_section(1488u32);
                    return Err(
                        CollatorError::Cancelled(
                            CollationCancelReason::NextAnchorNotFound(prev_anchor_id),
                        ),
                    );
                };
            };
            prev_anchor_id = anchor.id;
            last_imported_anchor_ct = anchor.chain_time;
            next_anchor = Some(anchor);
        }
        Ok(res)
    }
    fn check_has_unprocessed_messages(
        &mut self,
        working_state: &mut WorkingState,
    ) -> Result<bool> {
        if let Some(has_messages) = working_state.has_unprocessed_messages {
            return Ok(has_messages);
        }
        if working_state.reader_state.check_has_non_zero_processed_offset() {
            working_state.has_unprocessed_messages = Some(true);
            return Ok(true);
        }
        if working_state.reader_state.has_messages_in_buffers() {
            working_state.has_unprocessed_messages = Some(true);
            return Ok(true);
        }
        let msgs_exec_params = MsgsExecutionParamsStuff::create(
            working_state
                .prev_shard_data_ref()
                .processed_upto()
                .msgs_exec_params
                .clone(),
            working_state.collation_config.msgs_exec_params.clone(),
        );
        let mut messages_reader = MessagesReader::new(
            MessagesReaderContext {
                for_shard_id: working_state.next_block_id_short.shard,
                block_seqno: working_state.next_block_id_short.seqno,
                next_chain_time: 0,
                msgs_exec_params,
                mc_state_gen_lt: working_state.mc_data.gen_lt,
                prev_state_gen_lt: working_state.prev_shard_data_ref().gen_lt(),
                mc_top_shards_end_lts: working_state
                    .mc_data
                    .shards
                    .iter()
                    .map(|(k, v)| (*k, v.end_lt))
                    .collect(),
                cumulative_stats_calc_params: None,
                reader_state: std::mem::take(&mut working_state.reader_state),
                anchors_cache: Default::default(),
                is_first_block_after_prev_master: is_first_block_after_prev_master(
                    working_state.prev_shard_data_ref().blocks_ids()[0],
                    &working_state.mc_data.shards,
                ),
                part_stat_ranges: None,
            },
            self.mq_adapter.clone(),
        )?;
        let has_pending_internals = messages_reader
            .check_has_pending_internals_in_iterators()?;
        if working_state.next_block_id_short.is_masterchain() {
            messages_reader.drop_internals_next_range_readers();
        } else if !has_pending_internals {
            messages_reader.drop_internals_next_range_readers();
        }
        let FinalizedMessagesReader { mut reader_state, .. } = messages_reader
            .finalize(0, &Default::default())?;
        std::mem::swap(&mut working_state.reader_state, &mut reader_state);
        working_state.has_unprocessed_messages = Some(has_pending_internals);
        Ok(has_pending_internals)
    }
    async fn wait_state_and_try_collate_wrapper(&mut self) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(
                module_path!(), "::", stringify!(wait_state_and_try_collate_wrapper)
            ),
            file!(),
            1593u32,
        );
        {
            __guard.end_section(1595u32);
            let __result = self.wait_state_and_try_collate().await;
            __guard.start_section(1595u32);
            __result
        }
            .with_context(|| format!("next_block_id: {}", self.next_block_info))
    }
    async fn wait_state_and_try_collate(&mut self) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(wait_state_and_try_collate)),
            file!(),
            1599u32,
        );
        let working_state = {
            __guard.end_section(1600u32);
            let __result = self.delayed_working_state.wait().await;
            __guard.start_section(1600u32);
            __result
        }?;
        if self.shard_id.is_masterchain() {
            {
                __guard.end_section(1603u32);
                let __result = self
                    .try_collate_next_master_block_impl(working_state)
                    .await;
                __guard.start_section(1603u32);
                __result
            }
        } else {
            {
                __guard.end_section(1605u32);
                let __result = self
                    .try_collate_next_shard_block_impl(working_state)
                    .await;
                __guard.start_section(1605u32);
                __result
            }
        }
    }
    async fn wait_state_and_do_collate_wrapper(
        &mut self,
        top_shard_blocks_info: Vec<TopBlockDescription>,
        next_chain_time: u64,
    ) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(wait_state_and_do_collate_wrapper)),
            file!(),
            1613u32,
        );
        let top_shard_blocks_info = top_shard_blocks_info;
        let next_chain_time = next_chain_time;
        {
            __guard.end_section(1615u32);
            let __result = self
                .wait_state_and_do_collate(top_shard_blocks_info, next_chain_time)
                .await;
            __guard.start_section(1615u32);
            __result
        }
            .with_context(|| format!("next_block_id: {}", self.next_block_info))
    }
    async fn wait_state_and_do_collate(
        &mut self,
        top_shard_blocks_info: Vec<TopBlockDescription>,
        next_chain_time: u64,
    ) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(wait_state_and_do_collate)),
            file!(),
            1623u32,
        );
        let top_shard_blocks_info = top_shard_blocks_info;
        let next_chain_time = next_chain_time;
        let mut working_state = {
            __guard.end_section(1624u32);
            let __result = self.delayed_working_state.wait().await;
            __guard.start_section(1624u32);
            __result
        }?;
        let (mut last_imported_anchor_id, mut last_imported_chain_time) = self
            .anchors_cache
            .get_last_imported_anchor_id_and_ct()
            .unwrap();
        if last_imported_chain_time < next_chain_time {
            let labels = [("workchain", self.shard_id.workchain().to_string())];
            let top_processed_to_anchor = working_state.mc_data.top_processed_to_anchor;
            let max_consensus_lag_rounds = working_state
                .mc_data
                .config
                .get_consensus_config()?
                .max_consensus_lag_rounds
                .get() as u32;
            while last_imported_chain_time < next_chain_time {
                __guard.checkpoint(1641u32);
                let collation_cancelled = self.cancel_collation.notified();
                let import_fut = Self::import_next_anchor(
                    self.shard_id,
                    &mut self.anchors_cache,
                    self.mpool_adapter.clone(),
                    top_processed_to_anchor,
                    max_consensus_lag_rounds,
                );
                let import_anchor_result = {
                    __guard.end_section(1653u32);
                    let __result = tokio::select! {
                        res = import_fut => res ?, _ = collation_cancelled => {
                        tracing::info!(target : tracing_targets::COLLATOR,
                        top_processed_to_anchor, last_imported_anchor_id,
                        last_imported_chain_time,
                        "collation was cancelled by manager on wait_state_and_do_collate",);
                        metrics::counter!("tycho_collator_anchor_import_cancelled_count",
                        & labels) .increment(1); self.listener.on_cancelled(working_state
                        .mc_data.block_id, working_state.next_block_id_short,
                        CollationCancelReason::ExternalCancel,). await ?; self
                        .delayed_working_state.delay(working_state); return Ok(()); }
                    };
                    __guard.start_section(1653u32);
                    __result
                };
                match import_anchor_result {
                    ImportNextAnchor::Skipped => {
                        anyhow::bail!(
                            "anchor import cannot be skipped here because anchor \
                            with next_chain_time {} should exit",
                            next_chain_time,
                        )
                    }
                    ImportNextAnchor::Result {
                        get_anchor_result,
                        has_our_externals,
                        ..
                    } => {
                        match get_anchor_result {
                            GetAnchorResult::NotExist => {
                                anyhow::bail!(
                                    "anchor with next_chain_time {} should exit",
                                    next_chain_time
                                )
                            }
                            GetAnchorResult::Exist(next_anchor) => {
                                let elapsed_from_prev_anchor = self.anchor_timer.elapsed();
                                self.anchor_timer = std::time::Instant::now();
                                metrics::histogram!(
                                    "tycho_collator_from_prev_anchor_time", & labels
                                )
                                    .record(elapsed_from_prev_anchor);
                                working_state.wu_used_from_last_anchor = 0;
                                last_imported_anchor_id = next_anchor.id;
                                last_imported_chain_time = next_anchor.chain_time;
                                tracing::debug!(
                                    target : tracing_targets::COLLATOR, top_processed_to_anchor,
                                    last_imported_anchor_id, last_imported_chain_time,
                                    has_externals = has_our_externals,
                                    "imported next anchor to reach next_chain_time",
                                );
                            }
                        }
                    }
                }
            }
        }
        {
            __guard.end_section(1725u32);
            let __result = self
                .do_collate(
                    working_state,
                    Some(top_shard_blocks_info),
                    next_chain_time,
                    ForceMasterCollation::No,
                )
                .await;
            __guard.start_section(1725u32);
            __result
        }
    }
    /// Run collation if there are internals,
    /// otherwise import next anchor and notify it to manager
    /// that will route next collation steps
    #[tracing::instrument(
        parent = None,
        name = "try_collate_next_master_block",
        skip_all,
        fields(
            next_block_id = %self.next_block_info,
            mc_data_block_id = %working_state.mc_data.block_id.as_short_id(),
        )
    )]
    async fn try_collate_next_master_block_impl(
        &mut self,
        mut working_state: Box<WorkingState>,
    ) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(
                module_path!(), "::", stringify!(try_collate_next_master_block_impl)
            ),
            file!(),
            1741u32,
        );
        let mut working_state = working_state;
        tracing::debug!(
            target : tracing_targets::COLLATOR, "Check if can collate next master block",
        );
        let labels = [("workchain", self.shard_id.workchain().to_string())];
        let _histogram = HistogramGuardWithLabels::begin(
            "tycho_collator_try_collate_next_master_block_time",
            &labels,
        );
        let top_processed_to_anchor = working_state.mc_data.top_processed_to_anchor;
        let (last_imported_anchor_id, last_imported_chain_time) = self
            .anchors_cache
            .get_last_imported_anchor_id_and_ct()
            .unwrap_or_default();
        let has_unprocessed_messages = self
            .check_has_unprocessed_messages(&mut working_state)?;
        if has_unprocessed_messages {
            tracing::info!(
                target : tracing_targets::COLLATOR, top_processed_to_anchor,
                last_imported_anchor_id, last_imported_chain_time,
                "there are unprocessed messages from previous block, will collate next block",
            );
            {
                __guard.end_section(1779u32);
                let __result = self
                    .listener
                    .on_skipped(
                        working_state.mc_data.block_id,
                        working_state.next_block_id_short,
                        working_state.prev_shard_data_ref().gen_chain_time(),
                        ForceMasterCollation::ByUnprocessedMessages,
                        working_state.collation_config.clone(),
                    )
                    .await;
                __guard.start_section(1779u32);
                __result
            }?;
            self.delayed_working_state.delay(working_state);
            {
                __guard.end_section(1781u32);
                return Ok(());
            };
        }
        tracing::debug!(
            target : tracing_targets::COLLATOR, top_processed_to_anchor,
            last_imported_anchor_id, last_imported_chain_time,
            "there are no unprocessed messages, will import next anchor",
        );
        let collation_cancelled = self.cancel_collation.notified();
        let import_fut = Self::import_next_anchor(
            self.shard_id,
            &mut self.anchors_cache,
            self.mpool_adapter.clone(),
            working_state.mc_data.top_processed_to_anchor,
            working_state
                .mc_data
                .config
                .get_consensus_config()?
                .max_consensus_lag_rounds
                .get() as u32,
        );
        let import_anchor_result = {
            __guard.end_section(1808u32);
            let __result = tokio::select! {
                res = import_fut => res ?, _ = collation_cancelled => {
                tracing::info!(target : tracing_targets::COLLATOR,
                top_processed_to_anchor, last_imported_anchor_id,
                last_imported_chain_time,
                "collation was cancelled by manager on try_collate_next_master_block",);
                metrics::counter!("tycho_collator_anchor_import_cancelled_count", &
                labels) .increment(1); self.listener.on_cancelled(working_state.mc_data
                .block_id, working_state.next_block_id_short,
                CollationCancelReason::ExternalCancel,). await ?; self
                .delayed_working_state.delay(working_state); return Ok(()); }
            };
            __guard.start_section(1808u32);
            __result
        };
        match import_anchor_result {
            ImportNextAnchor::Result {
                prev_anchor_id,
                get_anchor_result,
                has_our_externals,
                ..
            } => {
                match get_anchor_result {
                    GetAnchorResult::NotExist => {
                        tracing::warn!(
                            target : tracing_targets::COLLATOR, top_processed_to_anchor,
                            last_imported_anchor_id, last_imported_chain_time,
                            "next anchor not exist, cancel collation attempts",
                        );
                        {
                            __guard.end_section(1850u32);
                            let __result = self
                                .listener
                                .on_cancelled(
                                    working_state.mc_data.block_id,
                                    working_state.next_block_id_short,
                                    CollationCancelReason::NextAnchorNotFound(prev_anchor_id),
                                )
                                .await;
                            __guard.start_section(1850u32);
                            __result
                        }?;
                        self.delayed_working_state.delay(working_state);
                        {
                            __guard.end_section(1852u32);
                            return Ok(());
                        };
                    }
                    GetAnchorResult::Exist(next_anchor) => {
                        let elapsed_from_prev_anchor = self.anchor_timer.elapsed();
                        self.anchor_timer = std::time::Instant::now();
                        metrics::histogram!(
                            "tycho_collator_from_prev_anchor_time", & labels
                        )
                            .record(elapsed_from_prev_anchor);
                        working_state.wu_used_from_last_anchor = 0;
                        tracing::debug!(
                            target : tracing_targets::COLLATOR, force_mc_block = false,
                            top_processed_to_anchor, last_imported_anchor_id =
                            next_anchor.id, last_imported_chain_time = next_anchor
                            .chain_time, has_externals = has_our_externals,
                            "imported next anchor, will notify collation manager",
                        );
                        {
                            __guard.end_section(1881u32);
                            let __result = self
                                .listener
                                .on_skipped(
                                    working_state.mc_data.block_id,
                                    working_state.next_block_id_short,
                                    next_anchor.chain_time,
                                    ForceMasterCollation::No,
                                    working_state.collation_config.clone(),
                                )
                                .await;
                            __guard.start_section(1881u32);
                            __result
                        }?;
                        self.delayed_working_state.delay(working_state);
                    }
                }
            }
            ImportNextAnchor::Skipped => {
                tracing::debug!(
                    target : tracing_targets::COLLATOR, force_mc_block = true,
                    top_processed_to_anchor, last_imported_anchor_id,
                    last_imported_chain_time,
                    "mempool paused, will notify collation manager",
                );
                {
                    __guard.end_section(1904u32);
                    let __result = self
                        .listener
                        .on_skipped(
                            working_state.mc_data.block_id,
                            working_state.next_block_id_short,
                            last_imported_chain_time,
                            ForceMasterCollation::ByAnchorImportSkipped,
                            working_state.collation_config.clone(),
                        )
                        .await;
                    __guard.start_section(1904u32);
                    __result
                }?;
                self.delayed_working_state.delay(working_state);
            }
        }
        Ok(())
    }
    /// Run collation if there are internals or externals,
    /// otherwise import next anchor.
    /// Import next anchor if wu used exceeded limit.
    /// If it has externals then run collation,
    /// otherwise skip collation and notify collation manager.
    /// Skip collation and notify collation manager
    /// if max uncommitted chain length reached.
    #[tracing::instrument(
        parent = None,
        name = "try_collate_next_shard_block",
        skip_all,
        fields(
            next_block_id = %self.next_block_info,
            mc_data_block_id = %working_state.mc_data.block_id.as_short_id(),
        )
    )]
    async fn try_collate_next_shard_block_impl(
        &mut self,
        mut working_state: Box<WorkingState>,
    ) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(try_collate_next_shard_block_impl)),
            file!(),
            1930u32,
        );
        let mut working_state = working_state;
        tracing::debug!(
            target : tracing_targets::COLLATOR, "Check if can collate next shard block",
        );
        let labels = [("workchain", self.shard_id.workchain().to_string())];
        let histogram = HistogramGuardWithLabels::begin(
            "tycho_collator_try_collate_next_shard_block_time",
            &labels,
        );
        let top_processed_to_anchor = working_state.mc_data.top_processed_to_anchor;
        let mut last_committed_seqno = 0;
        for (shard_id, shard_descr) in working_state.mc_data.shards.iter() {
            __guard.checkpoint(1945u32);
            if *shard_id == self.shard_id {
                last_committed_seqno = shard_descr.seqno;
            }
        }
        let uncommitted_chain_length = working_state.next_block_id_short.seqno - 1
            - last_committed_seqno;
        let max_uncommitted_chain_length = working_state
            .collation_config
            .max_uncommitted_chain_length as u32;
        let force_mc_block_by_uncommitted_chain = uncommitted_chain_length
            >= max_uncommitted_chain_length;
        #[derive(Debug)]
        enum TryCollateCheck {
            ForceMcBlockByUncommittedChainLength,
            HasUnprocessedMessages,
            HasExternals,
            NoPendingMessages,
            ForceEmptyShardBlock,
        }
        let mut anchor_import_skipped = false;
        let mut try_collate_check = 'check: {
            {
                let has_uprocessed_messages = self
                    .check_has_unprocessed_messages(&mut working_state)?;
                let has_externals = self.anchors_cache.has_pending_externals();
                let wu_used_from_last_anchor = working_state.wu_used_from_last_anchor;
                let wu_used_to_import_next_anchor = working_state
                    .collation_config
                    .wu_used_to_import_next_anchor;
                let force_import_anchor_by_used_wu = wu_used_from_last_anchor
                    > wu_used_to_import_next_anchor;
                metrics::gauge!(
                    "tycho_collator_wu_used_from_last_anchor", & [("type",
                    "wu_one_anchor"),]
                )
                    .set(wu_used_to_import_next_anchor as f64);
                metrics::gauge!(
                    "tycho_collator_wu_used_from_last_anchor", & [("type",
                    "wu_used_sum"),]
                )
                    .set(wu_used_from_last_anchor as f64);
                let can_import_anchors_count = wu_used_from_last_anchor
                    .saturating_div(wu_used_to_import_next_anchor);
                metrics::gauge!("tycho_collator_can_import_anchors_count")
                    .set(can_import_anchors_count as f64);
                let (last_imported_anchor_id, last_imported_chain_time) = self
                    .anchors_cache
                    .get_last_imported_anchor_id_and_ct()
                    .unwrap_or_default();
                match (
                    has_uprocessed_messages,
                    has_externals,
                    force_import_anchor_by_used_wu,
                ) {
                    _ if force_mc_block_by_uncommitted_chain => {
                        tracing::debug!(
                            target : tracing_targets::COLLATOR, top_processed_to_anchor,
                            last_imported_anchor_id, last_imported_chain_time,
                            uncommitted_chain_length, max_uncommitted_chain_length,
                            "uncommitted chain limit reached, will import next anchor and then force master collation",
                        );
                    }
                    (true, _, false) => {
                        __guard.end_section(2022u32);
                        __guard.start_section(2022u32);
                        break 'check TryCollateCheck::HasUnprocessedMessages;
                    }
                    (_, true, false) => {
                        __guard.end_section(2023u32);
                        __guard.start_section(2023u32);
                        break 'check TryCollateCheck::HasExternals;
                    }
                    (false, false, false) => {
                        tracing::debug!(
                            target : tracing_targets::COLLATOR, top_processed_to_anchor,
                            last_imported_anchor_id, last_imported_chain_time,
                            "there are no pending internals or externals, will import next anchor",
                        );
                        working_state.wu_used_from_last_anchor = 0;
                    }
                    (_, _, true) => {
                        tracing::info!(
                            target : tracing_targets::COLLATOR, top_processed_to_anchor,
                            last_imported_anchor_id, last_imported_chain_time,
                            "wu used from last anchor {} reached limit {} on length {}, will import next anchor",
                            wu_used_from_last_anchor, wu_used_to_import_next_anchor,
                            uncommitted_chain_length,
                        );
                    }
                }
                let max_consensus_lag_rounds = working_state
                    .mc_data
                    .config
                    .get_consensus_config()?
                    .max_consensus_lag_rounds
                    .get() as u32;
                let mut imported_anchors_count = 0;
                let mut imported_anchors_has_externals = false;
                loop {
                    __guard.checkpoint(2057u32);
                    let collation_cancelled = self.cancel_collation.notified();
                    let import_fut = Self::import_next_anchor(
                        self.shard_id,
                        &mut self.anchors_cache,
                        self.mpool_adapter.clone(),
                        working_state.mc_data.top_processed_to_anchor,
                        max_consensus_lag_rounds,
                    );
                    let import_anchor_result = {
                        __guard.end_section(2069u32);
                        let __result = tokio::select! {
                            res = import_fut => res ?, _ = collation_cancelled => {
                            tracing::info!(target : tracing_targets::COLLATOR,
                            top_processed_to_anchor, last_imported_anchor_id,
                            last_imported_chain_time,
                            "collation was cancelled by manager on try_collate_next_shard_block",);
                            metrics::counter!("tycho_collator_anchor_import_cancelled_count",
                            & labels) .increment(1); self.listener
                            .on_cancelled(working_state.mc_data.block_id, working_state
                            .next_block_id_short, CollationCancelReason::ExternalCancel,)
                            . await ?; self.delayed_working_state.delay(working_state);
                            return Ok(()); }
                        };
                        __guard.start_section(2069u32);
                        __result
                    };
                    match import_anchor_result {
                        ImportNextAnchor::Result {
                            prev_anchor_id,
                            get_anchor_result,
                            has_our_externals,
                            requested_at,
                        } => {
                            match get_anchor_result {
                                GetAnchorResult::NotExist => {
                                    tracing::warn!(
                                        target : tracing_targets::COLLATOR, top_processed_to_anchor,
                                        last_imported_anchor_id, last_imported_chain_time,
                                        "next anchor not exist, cancel collation attempts",
                                    );
                                    {
                                        __guard.end_section(2112u32);
                                        let __result = self
                                            .listener
                                            .on_cancelled(
                                                working_state.mc_data.block_id,
                                                working_state.next_block_id_short,
                                                CollationCancelReason::NextAnchorNotFound(prev_anchor_id),
                                            )
                                            .await;
                                        __guard.start_section(2112u32);
                                        __result
                                    }?;
                                    self.delayed_working_state.delay(working_state);
                                    {
                                        __guard.end_section(2114u32);
                                        return Ok(());
                                    };
                                }
                                GetAnchorResult::Exist(anchor) => {
                                    imported_anchors_count += 1;
                                    let elapsed_from_prev_anchor = self.anchor_timer.elapsed();
                                    self.anchor_timer = std::time::Instant::now();
                                    metrics::histogram!(
                                        "tycho_collator_from_prev_anchor_time", & labels
                                    )
                                        .record(elapsed_from_prev_anchor);
                                    metrics::gauge!(
                                        "tycho_collator_shard_blocks_count_btw_anchors"
                                    )
                                        .set(self.shard_blocks_count_from_last_anchor);
                                    self.shard_blocks_count_from_last_anchor = 0;
                                    let lag = work_units::MempoolAnchorLag {
                                        requested_at,
                                        chain_time: anchor.chain_time,
                                    };
                                    let prev_block_seqno = working_state
                                        .next_block_id_short
                                        .seqno - 1;
                                    if let Some(sender) = &self.wu_tuner_event_sender {
                                        if let Err(err) = {
                                            __guard.end_section(2145u32);
                                            let __result = sender
                                                .send(work_units::WuEvent {
                                                    shard: self.shard_id,
                                                    seqno: prev_block_seqno,
                                                    data: work_units::WuEventData::AnchorLag(lag),
                                                })
                                                .await;
                                            __guard.start_section(2145u32);
                                            __result
                                        } {
                                            tracing::warn!(
                                                target : tracing_targets::COLLATOR, ? err,
                                                "error sending anchor lag to the tuner service",
                                            );
                                        }
                                    } else {
                                        work_units::report_anchor_lag_to_metrics(
                                            &self.shard_id,
                                            lag.lag(),
                                        );
                                    }
                                    imported_anchors_has_externals |= has_our_externals;
                                    working_state.wu_used_from_last_anchor = working_state
                                        .wu_used_from_last_anchor
                                        .saturating_sub(wu_used_to_import_next_anchor);
                                    tracing::debug!(
                                        target : tracing_targets::COLLATOR, wu_used_from_last_anchor
                                        = working_state.wu_used_from_last_anchor,
                                        force_import_anchor_by_used_wu, "used wu dropped to",
                                    );
                                    if working_state.wu_used_from_last_anchor
                                        < wu_used_to_import_next_anchor
                                    {
                                        {
                                            __guard.end_section(2175u32);
                                            __guard.start_section(2175u32);
                                            break;
                                        };
                                    }
                                }
                            }
                        }
                        ImportNextAnchor::Skipped => {
                            anchor_import_skipped = true;
                            {
                                __guard.end_section(2181u32);
                                __guard.start_section(2181u32);
                                break;
                            };
                        }
                    }
                }
                metrics::gauge!("tycho_collator_import_next_anchor_count")
                    .set(imported_anchors_count);
                if imported_anchors_has_externals {
                    tracing::debug!(
                        target : tracing_targets::COLLATOR,
                        "just imported anchors has externals",
                    );
                }
                let has_externals = has_externals || imported_anchors_has_externals;
                match (has_uprocessed_messages, has_externals) {
                    _ if force_mc_block_by_uncommitted_chain => {
                        TryCollateCheck::ForceMcBlockByUncommittedChainLength
                    }
                    (true, _) => TryCollateCheck::HasUnprocessedMessages,
                    (false, true) => TryCollateCheck::HasExternals,
                    (false, false) => TryCollateCheck::NoPendingMessages,
                }
            }
        };
        let (last_imported_anchor_id, last_imported_chain_time) = self
            .anchors_cache
            .get_last_imported_anchor_id_and_ct()
            .unwrap();
        let prev_block_chain_time = working_state.prev_shard_data_ref().gen_chain_time();
        if matches!(try_collate_check, TryCollateCheck::NoPendingMessages) {
            let empty_sc_block_interval_ms = working_state
                .collation_config
                .empty_sc_block_interval_ms as u64;
            let ct_elapsed_from_prev_block = last_imported_chain_time
                .saturating_sub(prev_block_chain_time);
            if empty_sc_block_interval_ms > 0
                && ct_elapsed_from_prev_block >= empty_sc_block_interval_ms
            {
                try_collate_check = TryCollateCheck::ForceEmptyShardBlock;
            }
        }
        match try_collate_check {
            TryCollateCheck::HasUnprocessedMessages
            | TryCollateCheck::HasExternals
            | TryCollateCheck::ForceEmptyShardBlock => {
                tracing::debug!(
                    target : tracing_targets::COLLATOR, reason = ? try_collate_check,
                    anchor_import_skipped, top_processed_to_anchor,
                    last_imported_anchor_id, last_imported_chain_time,
                    prev_block_chain_time, "will collate next shard block",
                );
                let force_next_mc_block = if anchor_import_skipped
                    && uncommitted_chain_length > 4
                {
                    ForceMasterCollation::ByAnchorImportSkipped
                } else {
                    ForceMasterCollation::No
                };
                drop(histogram);
                {
                    __guard.end_section(2260u32);
                    let __result = self
                        .do_collate(
                            working_state,
                            None,
                            last_imported_chain_time,
                            force_next_mc_block,
                        )
                        .await;
                    __guard.start_section(2260u32);
                    __result
                }?;
            }
            TryCollateCheck::NoPendingMessages
            | TryCollateCheck::ForceMcBlockByUncommittedChainLength => {
                tracing::debug!(
                    target : tracing_targets::COLLATOR, reason = ? try_collate_check,
                    anchor_import_skipped, force_mc_block_by_uncommitted_chain,
                    top_processed_to_anchor, last_imported_anchor_id,
                    last_imported_chain_time, prev_block_chain_time,
                    uncommitted_chain_length, max_uncommitted_chain_length,
                    "will NOT collate next shard block, will notify collation manager",
                );
                let force_mc_block = match try_collate_check {
                    TryCollateCheck::ForceMcBlockByUncommittedChainLength => {
                        ForceMasterCollation::ByUncommittedChain
                    }
                    TryCollateCheck::NoPendingMessages if anchor_import_skipped => {
                        ForceMasterCollation::ByAnchorImportSkipped
                    }
                    TryCollateCheck::NoPendingMessages if uncommitted_chain_length
                        >= 1 => ForceMasterCollation::NoPendingMessagesAfterShardBlocks,
                    _ => ForceMasterCollation::No,
                };
                {
                    __guard.end_section(2301u32);
                    let __result = self
                        .listener
                        .on_skipped(
                            working_state.mc_data.block_id,
                            working_state.next_block_id_short,
                            last_imported_chain_time,
                            force_mc_block,
                            working_state.collation_config.clone(),
                        )
                        .await;
                    __guard.start_section(2301u32);
                    __result
                }?;
                self.delayed_working_state.delay(working_state);
            }
        }
        Ok(())
    }
}
struct DelayedWorkingState {
    shard_id: ShardIdent,
    future: Option<DelayedWorkingStateFut>,
    unused: Option<Box<WorkingState>>,
}
type DelayedWorkingStateFut = Pin<
    Box<dyn Future<Output = Result<Box<WorkingState>>> + Send + Sync + 'static>,
>;
impl DelayedWorkingState {
    fn new<F>(shard_id: ShardIdent, f: F) -> Self
    where
        F: Future<Output = Result<Box<WorkingState>>> + Send + Sync + 'static,
    {
        Self {
            shard_id,
            future: Some(Box::pin(f)),
            unused: None,
        }
    }
    async fn wait(&mut self) -> Result<Box<WorkingState>> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(wait)),
            file!(),
            2332u32,
        );
        let labels = [("workchain", self.shard_id.workchain().to_string())];
        let _histogram = HistogramGuardWithLabels::begin(
            "tycho_collator_wait_for_working_state_time_high",
            &labels,
        );
        if let Some(state) = self.unused.take() {
            {
                __guard.end_section(2340u32);
                return Ok(state);
            };
        }
        if let Some(fut) = self.future.take() {
            {
                __guard.end_section(2344u32);
                return {
                    __guard.end_section(2344u32);
                    let __result = fut.await;
                    __guard.start_section(2344u32);
                    __result
                };
            };
        }
        anyhow::bail!("No pending working state found");
    }
    fn delay(&mut self, state: Box<WorkingState>) {
        self.unused = Some(state);
    }
    fn reset(&mut self) {
        self.unused = None;
        self.future = None;
    }
}
struct StateUpdateContext {
    block_id: BlockId,
    store_new_state_task: JoinTask<Result<bool>>,
    state_update: MerkleUpdate,
}
struct AnchorsProcessingInfo {
    pub processed_to_anchor_id: MempoolAnchorId,
    pub processed_to_msgs_offset: u64,
    pub last_imported_chain_time: u64,
    pub last_imported_in_block_id: BlockId,
    /// The chain time of last imported anchor in current shard.
    /// For master block will always be equal to `last_imported_chain_time`.
    /// For shard block may be below `last_imported_chain_time` if there
    /// were no shard blocks between last and previous master (shard was not
    /// updated in last master block)
    pub current_shard_last_imported_chain_time: u64,
}
impl std::fmt::Debug for AnchorsProcessingInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self, f)
    }
}
impl std::fmt::Display for AnchorsProcessingInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AnchorsProcessingInfo")
            .field("processed_to_anchor_id", &self.processed_to_anchor_id)
            .field("processed_to_msgs_offset", &self.processed_to_msgs_offset)
            .field("last_imported_chain_time", &self.last_imported_chain_time)
            .field(
                "last_imported_in_block_id",
                &DebugDisplay(&self.last_imported_in_block_id),
            )
            .field(
                "current_shard_last_imported_chain_time",
                &self.current_shard_last_imported_chain_time,
            )
            .finish()
    }
}
enum ImportNextAnchor {
    Result {
        prev_anchor_id: MempoolAnchorId,
        get_anchor_result: GetAnchorResult,
        has_our_externals: bool,
        requested_at: u64,
    },
    Skipped,
}
#[derive(Debug)]
#[allow(dead_code)]
enum InitAnchorSource {
    FromCache(AnchorInfo),
    Imported(AnchorInfo),
}
#[derive(Default)]
struct ImportInitAnchorsResult {
    anchors_info: Vec<InitAnchorSource>,
    /// Number of anchors that were imported
    /// after "last imported in current shard"
    /// up to "top last imported anchor"
    anchors_count_above_last_imported_in_current_shard: usize,
}
