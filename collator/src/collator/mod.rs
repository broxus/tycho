use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use do_collate::is_first_block_after_prev_master;
use error::CollatorError;
use futures_util::future::Future;
use messages_reader::{
    FinalizedMessagesReader, MessagesReader, MessagesReaderContext, ReaderState,
};
use tokio::sync::{Notify, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use tycho_block_util::block::calc_next_block_id_short;
use tycho_block_util::state::{MinRefMcStateTracker, ShardStateStuff};
use tycho_core::global_config::MempoolGlobalConfig;
use tycho_network::PeerId;
use tycho_types::cell::{Cell, HashBytes};
use tycho_types::models::*;
use tycho_util::futures::JoinTask;
use tycho_util::metrics::{HistogramGuard, HistogramGuardWithLabels};
use tycho_util::time::now_millis;
use types::{AnchorInfo, AnchorsCache, MsgsExecutionParamsStuff};

use self::types::{BlockSerializerCache, CollatorStats, PrevData, WorkingState};
use crate::internal_queue::types::EnqueuedMessage;
use crate::mempool::{GetAnchorResult, MempoolAdapter, MempoolAnchorId};
use crate::queue_adapter::MessageQueueAdapter;
use crate::state_node::StateNodeAdapter;
use crate::types::processed_upto::ProcessedUptoInfoExtension;
use crate::types::{
    BlockCollationResult, CollationSessionId, CollationSessionInfo, CollatorConfig, DebugDisplay,
    DisplayBlockIdsIntoIter, McData, TopBlockDescription,
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

pub use do_collate::work_units;
pub use error::CollationCancelReason;
pub use types::{ForceMasterCollation, ShardDescriptionExt};

#[cfg(test)]
#[path = "tests/collator_tests.rs"]
pub(super) mod tests;

#[cfg(test)]
pub(crate) use messages_reader::tests::{TestInternalMessage, TestMessageFactory};

// FACTORY

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
        self(cx).await
    }
}

// EVENTS LISTENER

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
    async fn on_block_candidate(&self, collation_result: BlockCollationResult) -> Result<()>;
    /// Process collator stopped event
    async fn on_collator_stopped(&self, collation_session_id: CollationSessionId) -> Result<()>;
}

// COLLATOR

#[async_trait]
pub trait Collator: Send + Sync + 'static {
    /// Enqueue collator stop task
    async fn enqueue_stop(&self) -> Result<()>;
    /// Enqueue update McData if newer, reset PrevData and run next collation attempt
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

pub struct CollatorStdImplFactory;

#[async_trait]
impl CollatorFactory for CollatorStdImplFactory {
    type Collator = AsyncQueuedDispatcher<CollatorStdImpl>;

    async fn start(&self, cx: CollatorContext) -> Result<Self::Collator> {
        CollatorStdImpl::start(
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
        )
        .await
    }
}

#[async_trait]
impl Collator for AsyncQueuedDispatcher<CollatorStdImpl> {
    async fn enqueue_stop(&self) -> Result<()> {
        let cancel_token = self.cancel_token().clone();
        self.enqueue_task(method_to_queued_async_closure!(stop_collator, cancel_token))
            .await
    }

    /// Enqueue update McData if newer, reset PrevData if required and run next collation attempt
    async fn enqueue_resume_collation(
        &self,
        mc_data: Arc<McData>,
        reset: bool,
        collation_session: Arc<CollationSessionInfo>,
        prev_blocks_ids: Vec<BlockId>,
    ) -> Result<()> {
        self.enqueue_task(method_to_queued_async_closure!(
            resume_collation_wrapper,
            mc_data,
            reset,
            collation_session,
            prev_blocks_ids
        ))
        .await
    }

    async fn enqueue_try_collate(&self) -> Result<()> {
        self.enqueue_task(method_to_queued_async_closure!(
            wait_state_and_try_collate_wrapper,
        ))
        .await
    }

    async fn enqueue_do_collate(
        &self,
        top_shard_blocks_info: Vec<TopBlockDescription>,
        next_chain_time: u64,
    ) -> Result<()> {
        self.enqueue_task(method_to_queued_async_closure!(
            wait_state_and_do_collate_wrapper,
            top_shard_blocks_info,
            next_chain_time
        ))
        .await
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
    store_new_state_tasks: Vec<JoinTask<Result<bool>>>,
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
    ) -> Result<AsyncQueuedDispatcher<Self>> {
        const BLOCK_CELL_COUNT_BASELINE: usize = 100_000;

        let next_block_info = calc_next_block_id_short(&prev_blocks_ids);

        tracing::info!(target: tracing_targets::COLLATOR,
            "(next_block_id={}): collator starting...", next_block_info,
        );

        let (working_state_tx, working_state_rx) = oneshot::channel::<Result<Box<WorkingState>>>();

        let processor = Self {
            next_block_info,
            config,
            collation_session,
            listener,
            mq_adapter,
            mpool_adapter,
            state_node_adapter,
            shard_id,
            delayed_working_state: DelayedWorkingState::new(shard_id, async move {
                match working_state_rx.await {
                    Ok(state) => state,
                    Err(_) => anyhow::bail!("collator init cancelled"),
                }
            }),
            store_new_state_tasks: Default::default(),
            anchors_cache: Default::default(),
            block_serializer_cache: BlockSerializerCache::with_capacity(BLOCK_CELL_COUNT_BASELINE),
            stats: Default::default(),
            timer: std::time::Instant::now(),
            anchor_timer: std::time::Instant::now(),
            shard_blocks_count_from_last_anchor: 0,
            mempool_config_override,
            cancel_collation,
        };

        // create dispatcher for own async tasks queue
        let dispatcher =
            AsyncQueuedDispatcher::create(processor, STANDARD_QUEUED_DISPATCHER_BUFFER_SIZE);
        tracing::trace!(target: tracing_targets::COLLATOR,
            "(next_block_id={}): collator tasks queue dispatcher started", next_block_info,
        );

        // equeue first initialization task
        // sending to the receiver here cannot return Error because it is guaranteed not closed or dropped
        dispatcher
            .enqueue_task(method_to_queued_async_closure!(
                init_collator_wrapper,
                prev_blocks_ids,
                mc_data,
                working_state_tx
            ))
            .await
            .context("task receiver had to be not closed or dropped here")?;
        tracing::info!(target: tracing_targets::COLLATOR,
            "(next_block_id={}): collator initialization task enqueued", next_block_info,
        );

        tracing::info!(target: tracing_targets::COLLATOR,
            "(next_block_id={}): collator started", next_block_info,
        );

        Ok(dispatcher)
    }

    async fn stop_collator(&mut self, dispatcher_cancel_token: CancellationToken) -> Result<()> {
        self.listener
            .on_collator_stopped(self.collation_session.id())
            .await?;
        dispatcher_cancel_token.cancel();
        Ok(())
    }

    async fn init_collator_wrapper(
        &mut self,
        prev_blocks_ids: Vec<BlockId>,
        mc_data: Arc<McData>,
        working_state_tx: oneshot::Sender<Result<Box<WorkingState>>>,
    ) -> Result<()> {
        self.init_collator(prev_blocks_ids, mc_data, working_state_tx)
            .await
            .with_context(|| format!("next_block_id: {}", self.next_block_info))
    }

    // Initialize collator working state then run collation
    #[tracing::instrument(skip_all, fields(next_block_id = %self.next_block_info))]
    async fn init_collator(
        &mut self,
        prev_blocks_ids: Vec<BlockId>,
        mc_data: Arc<McData>,
        working_state_tx: oneshot::Sender<Result<Box<WorkingState>>>,
    ) -> Result<()> {
        let labels = [("workchain", self.shard_id.workchain().to_string())];

        tracing::info!(target: tracing_targets::COLLATOR, "initializing...");

        // init working state
        let mut working_state = Self::init_working_state(
            &self.next_block_info,
            self.state_node_adapter.clone(),
            mc_data,
            prev_blocks_ids,
        )
        .await?;

        // get last processed and last imported anchor info (will be None on zerostate)
        let prev_shard_data = working_state.prev_shard_data_ref();
        let prev_block_id = prev_shard_data.blocks_ids()[0]; // TODO: consider split/merge
        let anchors_processing_info_opt = Self::get_anchors_processing_info(
            &working_state.next_block_id_short.shard,
            &working_state.mc_data,
            &prev_block_id,
            prev_shard_data.gen_chain_time(),
            prev_shard_data
                .processed_upto()
                .get_min_externals_processed_to()?,
        );

        // import anchors
        if let Some(anchors_processing_info) = anchors_processing_info_opt {
            let timer = std::time::Instant::now();

            tracing::info!(target: tracing_targets::COLLATOR,
                %anchors_processing_info,
                "will check and import init anchors on init",
            );

            // anchors importing can stuck if mempool paused
            // so allow to cancel collation here
            let cancel_collation = self.cancel_collation.clone();
            let collation_cancelled = cancel_collation.notified();
            let import_fut =
                self.check_and_import_init_anchors(&working_state, anchors_processing_info);

            let import_init_anchors_res = tokio::select! {
                res = import_fut => res,
                _ = collation_cancelled => {
                    tracing::info!(target: tracing_targets::COLLATOR,
                        "collation was cancelled by manager on init",
                    );
                    metrics::counter!("tycho_collator_anchor_import_cancelled_count", &labels).increment(1);
                    self.listener
                        .on_cancelled(
                            working_state.mc_data.block_id,
                            working_state.next_block_id_short,
                            CollationCancelReason::ExternalCancel,
                        )
                        .await?;
                    return Ok(());
                }
            };

            let ImportInitAnchorsResult {
                anchors_info,
                mut anchors_count_above_last_imported_in_current_shard,
            } = match import_init_anchors_res {
                Err(CollatorError::Cancelled(reason)) => {
                    self.listener
                        .on_cancelled(
                            working_state.mc_data.block_id,
                            working_state.next_block_id_short,
                            reason,
                        )
                        .await?;
                    return Ok(());
                }
                res => res?,
            };

            if !anchors_info.is_empty() {
                tracing::info!(target: tracing_targets::COLLATOR,
                    elapsed = timer.elapsed().as_millis(),
                    "imported anchors on init: {:?}",
                    anchors_info.as_slice()
                );

                // reduce accumulated wu used from last anchor on
                // the number of anchors imported after last in current shard
                // if shard was not updated in last master block
                let wu_used = &mut working_state.wu_used_from_last_anchor;
                let wu_step = working_state.collation_config.wu_used_to_import_next_anchor;
                while anchors_count_above_last_imported_in_current_shard > 0 {
                    anchors_count_above_last_imported_in_current_shard -= 1;
                    if let Some(new_wu_used) = wu_used.checked_sub(wu_step) {
                        *wu_used = new_wu_used;
                    } else {
                        break;
                    }
                }
            }
        }

        working_state_tx.send(Ok(working_state)).ok();

        self.timer = std::time::Instant::now();

        self.anchor_timer = std::time::Instant::now();

        tracing::info!(target: tracing_targets::COLLATOR, "init finished");

        // trying to collate next block
        tracing::info!(target: tracing_targets::COLLATOR, "trying to collate next block after init...");
        self.wait_state_and_try_collate().await?;

        Ok(())
    }

    async fn check_and_import_init_anchors(
        &mut self,
        working_state: &WorkingState,
        anchors_proc_info: AnchorsProcessingInfo,
    ) -> Result<ImportInitAnchorsResult, CollatorError> {
        // get overrided genesis info if present or last know from state
        let genesis_info = self
            .mempool_config_override
            .as_ref()
            .map_or(working_state.mc_data.consensus_info.genesis_info, |c| {
                c.genesis_info
            });

        tracing::debug!(target: tracing_targets::COLLATOR,
            ?genesis_info,
            ?anchors_proc_info,
            "check_and_import_init_anchors",
        );

        // There may be cases when processed to anchor in shard is before anchor in master.
        // We can produce incorrect shard block, then ignore it, take correct from bc and try to collate the next one.
        let import_init_anchors = if genesis_info.start_round > 0 {
            if anchors_proc_info.processed_to_anchor_id <= genesis_info.start_round {
                if anchors_proc_info.processed_to_anchor_id == genesis_info.start_round {
                    // when we start from new genesis we unable to import anchor on the start round
                    // because mempool actually is starting from the next round
                    // so we should not try to import init anchors
                    false
                } else if self.shard_id.is_masterchain() {
                    // if last processed_to anchor is before the start round for master,
                    // then cancel collation because we need to receive more blocks from bc
                    return Err(CollatorError::Cancelled(
                        CollationCancelReason::AnchorNotFound(
                            anchors_proc_info.processed_to_anchor_id,
                        ),
                    ));
                } else {
                    // last processed_to anchor in shard can be before last processed in master
                    // it is normal, so we should not cancel collation but we unable to import init anchors
                    // possibly we will produce incorrect shard block then take correct from bc and try to collate next one
                    false
                }
            } else {
                // when last processed_to anchor is after genesis start round then we can import init anchors
                true
            }
        } else {
            // required to import init anchors when genesis is a zerostate
            true
        };

        if import_init_anchors {
            tracing::debug!(target: tracing_targets::COLLATOR,
                "importing anchors from processed to anchor ({}) with offset ({}) to chain_time {} \
                (current_shard_last_imported_chain_time = {})",
                anchors_proc_info.processed_to_anchor_id,
                anchors_proc_info.processed_to_msgs_offset,
                anchors_proc_info.last_imported_chain_time,
                anchors_proc_info.current_shard_last_imported_chain_time,
            );

            Self::import_init_anchors(
                anchors_proc_info.processed_to_anchor_id,
                anchors_proc_info.processed_to_msgs_offset,
                anchors_proc_info.last_imported_chain_time,
                anchors_proc_info.current_shard_last_imported_chain_time,
                self.shard_id,
                &mut self.anchors_cache,
                self.mpool_adapter.clone(),
            )
            .await
        } else {
            // needs to set last imported anchor info into cache
            // because nothing will be imported on init
            // but we should be able to import next anchor for collation
            let block_stuff = self
                .state_node_adapter
                .load_block(&anchors_proc_info.last_imported_in_block_id)
                .await?
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
            self.anchors_cache
                .set_last_imported_anchor_info(anchor_info);

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
        self.resume_collation(mc_data, reset, collation_session, new_prev_blocks_ids)
            .await
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
        let labels = [("workchain", self.shard_id.workchain().to_string())];
        let histogram =
            HistogramGuard::begin_with_labels("tycho_collator_resume_collation_time_high", &labels);

        // update collation session info to refer to a correct subset in collated block
        self.collation_session = collation_session;

        // previously wait when all state store tasks finished
        if !self.store_new_state_tasks.is_empty() {
            tracing::debug!(target: tracing_targets::COLLATOR,
                "awaiting when all state store tasks finished...",
            );
            for task in self.store_new_state_tasks.drain(..) {
                task.await?;
            }
        }

        let mut working_state = if !reset {
            let mut working_state = self.delayed_working_state.wait().await?;

            // update mc_data if newer
            if working_state.mc_data.block_id.seqno < mc_data.block_id.seqno {
                working_state.collation_config = Arc::new(mc_data.config.get_collation_config()?);
                working_state.mc_data = mc_data;

                if working_state.has_unprocessed_messages == Some(false) {
                    working_state.has_unprocessed_messages = None;
                }

                // and only for shard collator
                // reload prev states from storage to drop usage tree
                if !self.shard_id.is_masterchain() && working_state.next_block_id_short.seqno != 0 {
                    Self::reload_prev_data(&mut working_state, self.state_node_adapter.clone())
                        .await?;
                }
            }

            tracing::info!(target: tracing_targets::COLLATOR,
                mc_data_block_id = %working_state.mc_data.block_id.as_short_id(),
                "resume collation without reset",
            );

            working_state
        } else {
            // reset any delayed working state because we will init a new one
            self.delayed_working_state.reset();

            self.next_block_info = calc_next_block_id_short(&new_prev_blocks_ids);

            tracing::info!(target: tracing_targets::COLLATOR,
                mc_data_block_id = %mc_data.block_id.as_short_id(),
                new_prev_blocks_ids = %DisplayBlockIdsIntoIter(&new_prev_blocks_ids),
                new_next_block_id = %self.next_block_info,
                "resume collation with reset",
            );

            // reload prev data, reinit working state, drop msgs buffer
            tracing::debug!(target: tracing_targets::COLLATOR,
                new_next_block_id = %self.next_block_info,
                "reset working state and msgs buffer",
            );
            let mut working_state = Self::init_working_state(
                &self.next_block_info,
                self.state_node_adapter.clone(),
                mc_data,
                new_prev_blocks_ids,
            )
            .await?;

            // get last processed and last imported anchor info (will be None on zerostate)
            let prev_shard_data = working_state.prev_shard_data_ref();
            let prev_block_id = prev_shard_data.blocks_ids()[0]; // TODO: consider split/merge
            let prev_shard_data_gen_chain_time = prev_shard_data.gen_chain_time();
            let anchors_processing_info_opt = Self::get_anchors_processing_info(
                &working_state.next_block_id_short.shard,
                &working_state.mc_data,
                &prev_block_id,
                prev_shard_data_gen_chain_time,
                prev_shard_data
                    .processed_upto()
                    .get_min_externals_processed_to()?,
            );

            // import anchors
            if let Some(anchors_processing_info) = anchors_processing_info_opt {
                let timer = std::time::Instant::now();

                tracing::debug!(target: tracing_targets::COLLATOR,
                    %anchors_processing_info,
                    "will check and import init anchors on resume",
                );

                // anchors importing can stuck if mempool paused
                // so allow to cancel collation here
                let cancel_collation = self.cancel_collation.clone();
                let collation_cancelled = cancel_collation.notified();
                let import_fut =
                    self.check_and_import_init_anchors(&working_state, anchors_processing_info);

                let import_init_anchors_res = tokio::select! {
                    res = import_fut => res,
                    _ = collation_cancelled => {
                        tracing::info!(target: tracing_targets::COLLATOR,
                            "collation was cancelled by manager on resume",
                        );
                        metrics::counter!("tycho_collator_anchor_import_cancelled_count", &labels).increment(1);
                        self.listener
                            .on_cancelled(
                                working_state.mc_data.block_id,
                                working_state.next_block_id_short,
                                CollationCancelReason::ExternalCancel,
                            )
                            .await?;
                        return Ok(());
                    }
                };

                let ImportInitAnchorsResult {
                    anchors_info,
                    mut anchors_count_above_last_imported_in_current_shard,
                } = match import_init_anchors_res {
                    Err(CollatorError::Cancelled(reason)) => {
                        self.listener
                            .on_cancelled(
                                working_state.mc_data.block_id,
                                working_state.next_block_id_short,
                                reason,
                            )
                            .await?;
                        return Ok(());
                    }
                    res => res?,
                };

                if !anchors_info.is_empty() {
                    tracing::debug!(target: tracing_targets::COLLATOR,
                        elapsed = timer.elapsed().as_millis(),
                        new_next_block_id = %self.next_block_info,
                        "imported anchors on resume: {:?}",
                        anchors_info.as_slice(),
                    );

                    // reduce accumulated wu used from last anchor on
                    // the number of anchors imported after last in current shard
                    // if shard was not updated in last master block
                    let wu_used = &mut working_state.wu_used_from_last_anchor;
                    let wu_step = working_state.collation_config.wu_used_to_import_next_anchor;
                    while anchors_count_above_last_imported_in_current_shard > 0 {
                        anchors_count_above_last_imported_in_current_shard -= 1;
                        if let Some(new_wu_used) = wu_used.checked_sub(wu_step) {
                            *wu_used = new_wu_used;
                        } else {
                            break;
                        }
                    }
                }
            }

            working_state
        };

        // will use time elapsed to resume collation to calculate wu price
        working_state.resume_collation_elapsed = histogram.finish();

        if self.shard_id.is_masterchain() {
            self.try_collate_next_master_block_impl(working_state).await
        } else {
            self.try_collate_next_shard_block_impl(working_state).await
        }
    }

    #[tracing::instrument(skip_all, fields(next_block_id = %next_block_id_short))]
    async fn init_working_state(
        next_block_id_short: &BlockIdShort,
        state_node_adapter: Arc<dyn StateNodeAdapter>,
        mc_data: Arc<McData>,
        prev_blocks_ids: Vec<BlockId>,
    ) -> Result<Box<WorkingState>> {
        // load prev states and queue diff hashes
        tracing::debug!(target: tracing_targets::COLLATOR,
            prev_blocks_ids = %DisplayBlockIdsIntoIter(&prev_blocks_ids),
            "loading prev states and queue diffs...",
        );
        let (prev_states, prev_queue_diff_hashes) =
            Self::load_states_and_diffs(state_node_adapter, prev_blocks_ids).await?;

        // build and validate working state
        tracing::debug!(target: tracing_targets::COLLATOR, "building working state...");

        Self::build_and_validate_init_working_state(mc_data, prev_states, prev_queue_diff_hashes)
    }

    async fn reload_prev_data(
        working_state: &mut WorkingState,
        state_node_adapter: Arc<dyn StateNodeAdapter>,
    ) -> Result<()> {
        // drop prev shard data and usage tree
        let prev_queue_diff_hashes;
        let prev_blocks_ids;
        {
            working_state.usage_tree.take();

            let prev_shard_data = working_state.prev_shard_data.take().unwrap();
            prev_queue_diff_hashes = prev_shard_data.prev_queue_diff_hashes().clone();
            prev_blocks_ids = prev_shard_data.blocks_ids().clone();
        }

        tracing::debug!(target: tracing_targets::COLLATOR,
            prev_blocks_ids = %DisplayBlockIdsIntoIter(&prev_blocks_ids),
            "loading prev states...",
        );
        let prev_states =
            Self::load_prev_states(state_node_adapter.as_ref(), &prev_blocks_ids).await?;

        // update working state
        tracing::debug!(target: tracing_targets::COLLATOR, "updating working state...");

        let (prev_shard_data, usage_tree) = PrevData::build(prev_states, prev_queue_diff_hashes)?;

        // set new prev shard data and usage tree
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
        new_state_root: Cell,
        store_new_state_task: JoinTask<Result<bool>>,
        new_queue_diff_hash: HashBytes,
        new_mc_data: Arc<McData>,
        collation_config: Arc<CollationConfig>,
        has_unprocessed_messages: bool,
        reader_state: ReaderState,
        tracker: MinRefMcStateTracker,
        resume_collation_elapsed: Duration,
    ) -> Result<()> {
        let labels = [("workchain", self.shard_id.workchain().to_string())];
        let _histogram = HistogramGuard::begin_with_labels(
            "tycho_collator_prepare_working_state_update_time_high",
            &labels,
        );

        enum GetNewShardStateStuff {
            ReloadFromStorage(JoinTask<Result<bool>>),
            BuildFromNewObservable {
                block_id: BlockId,
                shard_state: Box<ShardStateUnsplit>,
                root: Cell,
                tracker: MinRefMcStateTracker,
            },
        }

        let get_new_state_stuff = {
            if block_id.is_masterchain() {
                GetNewShardStateStuff::ReloadFromStorage(store_new_state_task)
            } else {
                // append new store task
                self.store_new_state_tasks.push(store_new_state_task);

                // build state stuff from new observable state after collation
                GetNewShardStateStuff::BuildFromNewObservable {
                    block_id,
                    shard_state: new_observable_state,
                    root: new_state_root,
                    tracker,
                }
            }
        };

        let state_node_adapter = self.state_node_adapter.clone();

        self.delayed_working_state.future = Some(Box::pin(async move {
            let new_state_stuff = match get_new_state_stuff {
                GetNewShardStateStuff::BuildFromNewObservable {
                    block_id,
                    shard_state,
                    root,
                    tracker,
                } => ShardStateStuff::from_state_and_root(&block_id, shard_state, root, &tracker)?,
                GetNewShardStateStuff::ReloadFromStorage(store_new_state_task) => {
                    store_new_state_task.await?;
                    let load_task = JoinTask::new({
                        async move { state_node_adapter.load_state(&block_id).await }
                    });
                    load_task.await?
                }
            };

            let prev_states = vec![new_state_stuff];
            let prev_queue_diff_hashes = vec![new_queue_diff_hash];
            let (prev_shard_data, usage_tree) =
                PrevData::build(prev_states, prev_queue_diff_hashes)?;

            let next_block_id_short = calc_next_block_id_short(prev_shard_data.blocks_ids());

            Ok(Box::new(WorkingState {
                next_block_id_short,
                mc_data: new_mc_data,
                collation_config,
                wu_used_from_last_anchor: prev_shard_data.wu_used_from_last_anchor(),
                resume_collation_elapsed,
                prev_shard_data: Some(prev_shard_data),
                usage_tree: Some(usage_tree),
                has_unprocessed_messages: Some(has_unprocessed_messages),
                reader_state,
            }))
        }));

        Ok(())
    }

    /// Load required prev states and prev queue diff hashes
    async fn load_states_and_diffs(
        state_node_adapter: Arc<dyn StateNodeAdapter>,
        prev_blocks_ids: Vec<BlockId>,
    ) -> Result<(Vec<ShardStateStuff>, Vec<HashBytes>)> {
        // otherwise await prev states by prev block ids
        let load_state_fut: JoinTask<Result<Vec<ShardStateStuff>>> = JoinTask::new({
            let state_node_adapter = state_node_adapter.clone();
            let prev_blocks_ids = prev_blocks_ids.clone();
            let span = tracing::Span::current();
            async move { Self::load_prev_states(state_node_adapter.as_ref(), &prev_blocks_ids).await }
                .instrument(span)
        });

        let prev_hashes_fut: JoinTask<Result<Vec<HashBytes>>> = JoinTask::new({
            let span = tracing::Span::current();
            async move {
                let mut prev_hashes = vec![];
                for prev_block_id in prev_blocks_ids {
                    // request state for prev block and wait for response
                    if let Some(diff) = state_node_adapter.load_diff(&prev_block_id).await? {
                        tracing::debug!(target: tracing_targets::COLLATOR,
                            "loaded queue diff for prev_block_id {}",
                            prev_block_id.as_short_id(),
                        );
                        prev_hashes.push(*diff.diff_hash());
                    }
                }
                Ok(prev_hashes)
            }
            .instrument(span)
        });

        let (prev_states, prev_hash) =
            futures_util::future::join(load_state_fut, prev_hashes_fut).await;

        Ok((prev_states?, prev_hash?))
    }

    async fn load_prev_states(
        state_node_adapter: &dyn StateNodeAdapter,
        prev_blocks_ids: &[BlockId],
    ) -> Result<Vec<ShardStateStuff>> {
        let mut prev_states = vec![];
        for prev_block_id in prev_blocks_ids {
            let state = state_node_adapter.load_state(prev_block_id).await?;
            tracing::debug!(target: tracing_targets::COLLATOR,
                "loaded prev shard state for prev_block_id {}",
                prev_block_id.as_short_id(),
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
        // TODO: consider split/merge

        let (prev_shard_data, usage_tree) = PrevData::build(prev_states, prev_queue_diff_hashes)?;

        let next_block_id_short = calc_next_block_id_short(prev_shard_data.blocks_ids());

        let collation_config = Arc::new(mc_data.config.get_collation_config()?);

        Ok(Box::new(WorkingState {
            next_block_id_short,
            mc_data,
            wu_used_from_last_anchor: prev_shard_data.wu_used_from_last_anchor(),
            resume_collation_elapsed: Duration::ZERO,
            reader_state: ReaderState::new(prev_shard_data.processed_upto()),
            prev_shard_data: Some(prev_shard_data),
            usage_tree: Some(usage_tree),
            has_unprocessed_messages: None,
            collation_config,
        }))
    }

    fn get_anchors_processing_info(
        shard_id: &ShardIdent,
        mc_data: &McData,
        prev_block_id: &BlockId,
        prev_gen_chain_time: u64,
        prev_externals_processed_to: (MempoolAnchorId, u64),
    ) -> Option<AnchorsProcessingInfo> {
        // try get from mc data
        let mut from_mc_info_opt = None;
        if !shard_id.is_masterchain() {
            let (mc_processed_to_anchor_id, mc_processed_to_msgs_offset) = mc_data
                .processed_upto
                .get_min_externals_processed_to()
                .unwrap_or_default();
            if mc_processed_to_anchor_id > 0 {
                // TODO: consider split/merge

                // get from mc data if prev shard block is equal to the top shard
                // and top shard was not updated in master
                // it means that no shard blocks were collated between masters
                // because there were no messages for processing
                // and we can omit top processed anchor from shard
                // if it is lower then top processed from master
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

        // try get from prev data
        let from_prev_info_opt = match prev_externals_processed_to {
            (processed_to_anchor_id, processed_to_msgs_offset) if processed_to_anchor_id > 0 => {
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

        // choose the higher one
        match (from_mc_info_opt, from_prev_info_opt) {
            (Some(from_mc_info), Some(from_prev_info)) => {
                if from_mc_info.processed_to_anchor_id > from_prev_info.processed_to_anchor_id
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
        let labels = [("workchain", shard_id.workchain().to_string())];

        let _histogram =
            HistogramGuardWithLabels::begin("tycho_collator_import_next_anchor_time_high", &labels);

        let timer = std::time::Instant::now();

        let (prev_anchor_id, ct) = anchors_cache
            .get_last_imported_anchor_id_and_ct()
            .unwrap_or_default();

        // do not import anchor if mempool may be paused
        // needs to process more anchors in collator first
        if prev_anchor_id.saturating_sub(top_processed_to_anchor)
            > max_consensus_lag_rounds.saturating_mul(2).saturating_div(3)
        {
            metrics::counter!("tycho_collator_anchor_import_skipped_count", &labels).increment(1);
            return Ok(ImportNextAnchor::Skipped);
        }

        let anchor_requested_at = now_millis();

        let get_anchor_result = mpool_adapter.get_next_anchor(prev_anchor_id).await?;

        let has_our_externals = match &get_anchor_result {
            GetAnchorResult::Exist(next_anchor) => {
                // report anchor importing lag to metrics
                metrics::gauge!("tycho_collator_anchor_importing_lag_ms", &labels)
                    .set(anchor_requested_at as f64 - next_anchor.chain_time as f64);

                let our_exts_count = next_anchor.count_externals_for(&shard_id, 0);
                anchors_cache.insert(next_anchor.clone(), our_exts_count);

                let has_externals = our_exts_count > 0;

                metrics::counter!("tycho_collator_ext_msgs_imported_count", &labels)
                    .increment(our_exts_count as _);
                metrics::gauge!("tycho_collator_ext_msgs_imported_queue_size", &labels)
                    .increment(our_exts_count as f64);

                let chain_time_elapsed = next_anchor.chain_time.saturating_sub(ct);

                tracing::info!(target: tracing_targets::COLLATOR,
                    elapsed = timer.elapsed().as_millis(),
                    chain_time_elapsed,
                    "imported next anchor (id: {}, chain_time: {}, all_exts: {}, our_exts: {})",
                    next_anchor.id,
                    next_anchor.chain_time,
                    next_anchor.externals.len(),
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
        let labels = [("workchain", shard_id.workchain().to_string())];

        let mut res = ImportInitAnchorsResult::default();

        let mut last_anchor = None;
        let mut all_anchors_are_taken_from_cache = false;
        let mut processed_to_anchor_exists_in_cache = false;
        let mut total_our_exts_count: usize = 0;

        // we count our messages for processed_to with existing offset
        let mut offset = processed_to_msgs_offset as usize;

        // first look for required anchors in cache
        let mut idx = 0;
        while let Some((_, anchor)) = anchors_cache.get(idx) {
            if anchor.id >= processed_to_anchor_id {
                // check if processed_to anchor exists in cache
                if anchor.id == processed_to_anchor_id {
                    processed_to_anchor_exists_in_cache = true;
                }

                // NOTE: in case when we collated block 1 and now collating block 2
                //      and block 1 mismatched with received from bc,
                //      then we will collate block 2 again based on received block 1,
                //      but we may already have removed processed_to anchor
                //      from cache during the previous collation attempt of block 2,
                //      so we cannot use cache and should fully refill it

                // use anchors from cache only when processed_to anchor exists in cache
                if !processed_to_anchor_exists_in_cache {
                    break;
                }

                // when we found anchor with last_block_chain_time
                // it means that we have all required anchors in the cache
                if anchor.chain_time == last_block_chain_time {
                    all_anchors_are_taken_from_cache = true;
                }

                // remove all next anchors after last_block_chain_time
                if anchor.chain_time > last_block_chain_time {
                    let our_exts_count = anchor.count_externals_for(&shard_id, 0);
                    anchors_cache.remove(idx);
                    metrics::gauge!("tycho_collator_ext_msgs_imported_queue_size", &labels)
                        .decrement(our_exts_count as f64);
                    continue;
                }

                // and keep others
                let our_exts_count = anchor.count_externals_for(&shard_id, offset);
                total_our_exts_count = total_our_exts_count.saturating_add(our_exts_count);
                res.anchors_info
                    .push(InitAnchorSource::FromCache(AnchorInfo::from_anchor(
                        anchor.clone(),
                        our_exts_count,
                    )));

                // in next anchors after processed_to will count from 0
                offset = 0;

                last_anchor = Some(anchor);
            }
            idx += 1;
        }

        // we have all required anchors in cache
        if all_anchors_are_taken_from_cache {
            // update last imported anchor info
            let Some(InitAnchorSource::FromCache(anchor_info)) = res.anchors_info.last() else {
                return Err(CollatorError::Anyhow(anyhow::anyhow!(
                    "`anchors_info` should contain almost one `FromCache` here"
                )));
            };
            anchors_cache.set_last_imported_anchor_info(anchor_info.clone());
            anchors_cache.set_has_pending_externals(total_our_exts_count > 0);

            return Ok(res);
        }

        // clear cache if processed_to_anchor_id does not exist in cache
        if !processed_to_anchor_exists_in_cache {
            anchors_cache.clear();
            metrics::gauge!("tycho_collator_ext_msgs_imported_queue_size", &labels).set(0);
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
                let GetAnchorResult::Exist(anchor) = mpool_adapter
                    .get_anchor_by_id(processed_to_anchor_id)
                    .await?
                else {
                    return Err(CollatorError::Cancelled(
                        CollationCancelReason::AnchorNotFound(processed_to_anchor_id),
                    ));
                };
                prev_anchor_id = anchor.id;
                last_imported_anchor_ct = anchor.chain_time;
                Some(anchor)
            }
        };

        loop {
            // add loaded anchor to cache
            if let Some(anchor) = next_anchor {
                let our_exts_count = anchor.count_externals_for(&shard_id, offset);
                total_our_exts_count = total_our_exts_count.saturating_add(our_exts_count);
                anchors_cache.insert(anchor.clone(), our_exts_count);
                res.anchors_info
                    .push(InitAnchorSource::Imported(AnchorInfo::from_anchor(
                        anchor,
                        our_exts_count,
                    )));

                metrics::counter!("tycho_collator_ext_msgs_imported_count", &labels)
                    .increment(our_exts_count as u64);
                metrics::gauge!("tycho_collator_ext_msgs_imported_queue_size", &labels)
                    .increment(our_exts_count as f64);
            }

            // in next anchors after processed_to will count from 0
            offset = 0;

            // count number of anchors imported above last chain time for current shard
            if last_imported_anchor_ct > current_shard_last_imported_chain_time {
                res.anchors_count_above_last_imported_in_current_shard += 1;
            }

            // import next anchor if last block chain time not reached
            if last_imported_anchor_ct >= last_block_chain_time {
                break;
            }

            let GetAnchorResult::Exist(anchor) =
                mpool_adapter.get_next_anchor(prev_anchor_id).await?
            else {
                return Err(CollatorError::Cancelled(
                    CollationCancelReason::NextAnchorNotFound(prev_anchor_id),
                ));
            };
            prev_anchor_id = anchor.id;
            last_imported_anchor_ct = anchor.chain_time;

            next_anchor = Some(anchor);
        }

        anchors_cache.set_has_pending_externals(total_our_exts_count > 0);

        Ok(res)
    }

    fn check_has_unprocessed_messages(&mut self, working_state: &mut WorkingState) -> Result<bool> {
        if let Some(has_messages) = working_state.has_unprocessed_messages {
            return Ok(has_messages);
        }

        // when we have processed offset > 0 in externals or externals
        // then we should have uprocessed messages in buffer
        if working_state
            .reader_state
            .check_has_non_zero_processed_offset()
        {
            working_state.has_unprocessed_messages = Some(true);
            return Ok(true);
        }

        // if all internals and externals read
        // then check if any buffer contain messages
        if working_state.reader_state.has_messages_in_buffers() {
            working_state.has_unprocessed_messages = Some(true);
            return Ok(true);
        }

        // finally check if has pending messages in iterators

        let msgs_exec_params = MsgsExecutionParamsStuff::create(
            working_state
                .prev_shard_data_ref()
                .processed_upto()
                .msgs_exec_params
                .clone(),
            working_state.collation_config.msgs_exec_params.clone(),
        );

        // create reader
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
                // extract reader state to use in the reader
                reader_state: std::mem::take(&mut working_state.reader_state),
                // do not use anchors cache because we need to check
                // only for pending internals in iterators
                anchors_cache: Default::default(),
                is_first_block_after_prev_master: is_first_block_after_prev_master(
                    working_state.prev_shard_data_ref().blocks_ids()[0], /* TODO: consider split/merge */
                    &working_state.mc_data.shards,
                ),
                part_stat_ranges: None,
            },
            self.mq_adapter.clone(),
        )?;

        // check if has pending internals
        let has_pending_internals = messages_reader.check_has_pending_internals_in_iterators()?;

        // drop created next internals range readers for master
        // because shard end_lt does not include updated top shard block info
        if working_state.next_block_id_short.is_masterchain() {
            messages_reader.drop_internals_next_range_readers();
        } else if !has_pending_internals {
            // drop created next internals range readers for shard
            // when we do not have pending internals
            // because master can be changed on the next check and
            // range to boudary should be updated
            messages_reader.drop_internals_next_range_readers();
        }

        // return reader state to working state
        let FinalizedMessagesReader {
            mut reader_state, ..
        } = messages_reader.finalize(
            0, // can pass 0 because new messages reader was not initialized in this case
            &Default::default(),
        )?;
        std::mem::swap(&mut working_state.reader_state, &mut reader_state);

        working_state.has_unprocessed_messages = Some(has_pending_internals);

        Ok(has_pending_internals)
    }

    async fn wait_state_and_try_collate_wrapper(&mut self) -> Result<()> {
        self.wait_state_and_try_collate()
            .await
            .with_context(|| format!("next_block_id: {}", self.next_block_info))
    }

    async fn wait_state_and_try_collate(&mut self) -> Result<()> {
        let working_state = self.delayed_working_state.wait().await?;

        if self.shard_id.is_masterchain() {
            self.try_collate_next_master_block_impl(working_state).await
        } else {
            self.try_collate_next_shard_block_impl(working_state).await
        }
    }

    async fn wait_state_and_do_collate_wrapper(
        &mut self,
        top_shard_blocks_info: Vec<TopBlockDescription>,
        next_chain_time: u64,
    ) -> Result<()> {
        self.wait_state_and_do_collate(top_shard_blocks_info, next_chain_time)
            .await
            .with_context(|| format!("next_block_id: {}", self.next_block_info))
    }

    async fn wait_state_and_do_collate(
        &mut self,
        top_shard_blocks_info: Vec<TopBlockDescription>,
        next_chain_time: u64,
    ) -> Result<()> {
        let mut working_state = self.delayed_working_state.wait().await?;

        // if last imported anchor chain time is less then next required
        // then we should import next anchors until we reach required chain time
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
                .max_consensus_lag_rounds as u32;
            while last_imported_chain_time < next_chain_time {
                // next anchor importing can stuck if mempool paused
                // so allow to cancel collation here
                let collation_cancelled = self.cancel_collation.notified();
                let import_fut = Self::import_next_anchor(
                    self.shard_id,
                    &mut self.anchors_cache,
                    self.mpool_adapter.clone(),
                    top_processed_to_anchor,
                    max_consensus_lag_rounds,
                );

                let import_anchor_result = tokio::select! {
                    res = import_fut => res?,
                    _ = collation_cancelled => {
                        tracing::info!(target: tracing_targets::COLLATOR,
                            top_processed_to_anchor,
                            last_imported_anchor_id,
                            last_imported_chain_time,
                            "collation was cancelled by manager on wait_state_and_do_collate",
                        );
                        metrics::counter!("tycho_collator_anchor_import_cancelled_count", &labels).increment(1);
                        self.listener
                            .on_cancelled(
                                working_state.mc_data.block_id,
                                working_state.next_block_id_short,
                                CollationCancelReason::ExternalCancel,
                            )
                            .await?;
                        self.delayed_working_state.delay(working_state);
                        return Ok(());
                    }
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
                    } => match get_anchor_result {
                        GetAnchorResult::NotExist => {
                            anyhow::bail!(
                                "anchor with next_chain_time {} should exit",
                                next_chain_time
                            )
                        }
                        GetAnchorResult::Exist(next_anchor) => {
                            // time elapsed from prev anchor
                            let elapsed_from_prev_anchor = self.anchor_timer.elapsed();
                            self.anchor_timer = std::time::Instant::now();
                            metrics::histogram!("tycho_collator_from_prev_anchor_time", &labels)
                                .record(elapsed_from_prev_anchor);

                            working_state.wu_used_from_last_anchor = 0;

                            last_imported_anchor_id = next_anchor.id;
                            last_imported_chain_time = next_anchor.chain_time;

                            tracing::debug!(target: tracing_targets::COLLATOR,
                                top_processed_to_anchor,
                                last_imported_anchor_id,
                                last_imported_chain_time,
                                has_externals = has_our_externals,
                                "imported next anchor to reach next_chain_time",
                            );
                        }
                    },
                }
            }
        }

        self.do_collate(
            working_state,
            Some(top_shard_blocks_info),
            ForceMasterCollation::No,
        )
        .await
    }

    /// Run collation if there are internals,
    /// otherwise import next anchor and notify it to manager
    /// that will route next collation steps
    #[tracing::instrument(
        parent = None, name = "try_collate_next_master_block",
        skip_all, fields(
            next_block_id = %self.next_block_info,
            mc_data_block_id = %working_state.mc_data.block_id.as_short_id(),
        )
    )]
    async fn try_collate_next_master_block_impl(
        &mut self,
        mut working_state: Box<WorkingState>,
    ) -> Result<()> {
        tracing::debug!(target: tracing_targets::COLLATOR,
            "Check if can collate next master block",
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

        // check if has unprocessed messages in buffer or queue
        let has_unprocessed_messages = self.check_has_unprocessed_messages(&mut working_state)?;

        if has_unprocessed_messages {
            // do not import next anchor and force collation
            tracing::info!(target: tracing_targets::COLLATOR,
                top_processed_to_anchor,
                last_imported_anchor_id,
                last_imported_chain_time,
                "there are unprocessed messages from previous block, will collate next block",
            );

            self.listener
                .on_skipped(
                    working_state.mc_data.block_id,
                    working_state.next_block_id_short,
                    working_state.prev_shard_data_ref().gen_chain_time(),
                    ForceMasterCollation::ByUprocessedMessages,
                    working_state.collation_config.clone(),
                )
                .await?;
            self.delayed_working_state.delay(working_state);
            return Ok(());
        }

        // otherwise import next anchor and check if can collate in manager
        tracing::debug!(target: tracing_targets::COLLATOR,
            top_processed_to_anchor,
            last_imported_anchor_id,
            last_imported_chain_time,
            "there are no unprocessed messages, will import next anchor",
        );

        // next anchor importing can stuck if mempool paused
        // so allow to cancel collation here
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
                .max_consensus_lag_rounds as u32,
        );

        let import_anchor_result = tokio::select! {
            res = import_fut => res?,
            _ = collation_cancelled => {
                tracing::info!(target: tracing_targets::COLLATOR,
                    top_processed_to_anchor,
                    last_imported_anchor_id,
                    last_imported_chain_time,
                    "collation was cancelled by manager on try_collate_next_master_block",
                );
                metrics::counter!("tycho_collator_anchor_import_cancelled_count", &labels).increment(1);
                self.listener
                    .on_cancelled(
                        working_state.mc_data.block_id,
                        working_state.next_block_id_short,
                        CollationCancelReason::ExternalCancel,
                    )
                    .await?;
                self.delayed_working_state.delay(working_state);
                return Ok(());
            }
        };

        match import_anchor_result {
            ImportNextAnchor::Result {
                prev_anchor_id,
                get_anchor_result,
                has_our_externals,
            } => match get_anchor_result {
                GetAnchorResult::NotExist => {
                    tracing::warn!(target: tracing_targets::COLLATOR,
                        top_processed_to_anchor,
                        last_imported_anchor_id,
                        last_imported_chain_time,
                        "next anchor not exist, cancel collation attempts",
                    );
                    self.listener
                        .on_cancelled(
                            working_state.mc_data.block_id,
                            working_state.next_block_id_short,
                            CollationCancelReason::NextAnchorNotFound(prev_anchor_id),
                        )
                        .await?;
                    self.delayed_working_state.delay(working_state);
                    return Ok(());
                }
                GetAnchorResult::Exist(next_anchor) => {
                    // time elapsed from prev anchor
                    let elapsed_from_prev_anchor = self.anchor_timer.elapsed();
                    self.anchor_timer = std::time::Instant::now();
                    metrics::histogram!("tycho_collator_from_prev_anchor_time", &labels)
                        .record(elapsed_from_prev_anchor);

                    working_state.wu_used_from_last_anchor = 0;

                    tracing::debug!(target: tracing_targets::COLLATOR,
                        force_mc_block = false,
                        top_processed_to_anchor,
                        last_imported_anchor_id = next_anchor.id,
                        last_imported_chain_time = next_anchor.chain_time,
                        has_externals = has_our_externals,
                        "imported next anchor, will notify collation manager",
                    );

                    // this may start master block collation or cause next anchor import
                    self.listener
                        .on_skipped(
                            working_state.mc_data.block_id,
                            working_state.next_block_id_short,
                            next_anchor.chain_time,
                            ForceMasterCollation::No,
                            working_state.collation_config.clone(),
                        )
                        .await?;

                    self.delayed_working_state.delay(working_state);
                }
            },
            ImportNextAnchor::Skipped => {
                tracing::debug!(target: tracing_targets::COLLATOR,
                    force_mc_block = true,
                    top_processed_to_anchor,
                    last_imported_anchor_id,
                    last_imported_chain_time,
                    "mempool paused, will notify collation manager",
                );

                // this may start master block collation or cause next anchor import
                self.listener
                    .on_skipped(
                        working_state.mc_data.block_id,
                        working_state.next_block_id_short,
                        last_imported_chain_time,
                        ForceMasterCollation::ByAnchorImportSkipped,
                        working_state.collation_config.clone(),
                    )
                    .await?;

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
        parent = None, name = "try_collate_next_shard_block",
        skip_all, fields(
            next_block_id = %self.next_block_info,
            mc_data_block_id = %working_state.mc_data.block_id.as_short_id(),
        )
    )]
    async fn try_collate_next_shard_block_impl(
        &mut self,
        mut working_state: Box<WorkingState>,
    ) -> Result<()> {
        tracing::debug!(target: tracing_targets::COLLATOR,
            "Check if can collate next shard block",
        );

        let labels = [("workchain", self.shard_id.workchain().to_string())];
        let histogram = HistogramGuardWithLabels::begin(
            "tycho_collator_try_collate_next_shard_block_time",
            &labels,
        );

        let top_processed_to_anchor = working_state.mc_data.top_processed_to_anchor;

        // check if should force master block collation by uncommitted chain length
        let mut last_committed_seqno = 0;
        for (shard_id, shard_descr) in working_state.mc_data.shards.iter() {
            if *shard_id == self.shard_id {
                last_committed_seqno = shard_descr.seqno;
            }
        }
        let uncommitted_chain_length =
            working_state.next_block_id_short.seqno - 1 - last_committed_seqno;
        let max_uncommitted_chain_length =
            working_state.collation_config.max_uncommitted_chain_length as u32;

        let force_mc_block_by_uncommitted_chain =
            uncommitted_chain_length >= max_uncommitted_chain_length;

        // check if should collate next shard block, import anchors if required
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
            if force_mc_block_by_uncommitted_chain {
                TryCollateCheck::ForceMcBlockByUncommittedChainLength
            } else {
                // check has unprocessed messages in buffer or queue
                let has_uprocessed_messages =
                    self.check_has_unprocessed_messages(&mut working_state)?;

                // check pending externals
                let has_externals = self.anchors_cache.has_pending_externals();

                // check if should import anchor after fixed wu used by blocks collation
                let wu_used_from_last_anchor = working_state.wu_used_from_last_anchor;
                let wu_used_to_import_next_anchor =
                    working_state.collation_config.wu_used_to_import_next_anchor;
                let force_import_anchor_by_used_wu =
                    wu_used_from_last_anchor > wu_used_to_import_next_anchor;

                // report wu used related metrics
                metrics::gauge!("tycho_collator_wu_used_from_last_anchor", &[(
                    "type",
                    "wu_one_anchor"
                ),])
                .set(wu_used_to_import_next_anchor as f64);
                metrics::gauge!("tycho_collator_wu_used_from_last_anchor", &[(
                    "type",
                    "wu_used_sum"
                ),])
                .set(wu_used_from_last_anchor as f64);
                let can_import_anchors_count =
                    wu_used_from_last_anchor.saturating_div(wu_used_to_import_next_anchor);
                metrics::gauge!("tycho_collator_can_import_anchors_count")
                    .set(can_import_anchors_count as f64);

                // decide if should import anchors
                let (last_imported_anchor_id, last_imported_chain_time) = self
                    .anchors_cache
                    .get_last_imported_anchor_id_and_ct()
                    .unwrap_or_default();
                match (
                    has_uprocessed_messages,
                    has_externals,
                    force_import_anchor_by_used_wu,
                ) {
                    (true, _, false) => break 'check TryCollateCheck::HasUnprocessedMessages,
                    (_, true, false) => break 'check TryCollateCheck::HasExternals,
                    (false, false, false) => {
                        tracing::debug!(target: tracing_targets::COLLATOR,
                            top_processed_to_anchor,
                            last_imported_anchor_id,
                            last_imported_chain_time,
                            "there are no pending internals or externals, will import next anchor",
                        );

                        // drop used wu when anchor was not imported by used wu
                        working_state.wu_used_from_last_anchor = 0;
                    }
                    (_, _, true) => {
                        tracing::info!(target: tracing_targets::COLLATOR,
                            top_processed_to_anchor,
                            last_imported_anchor_id,
                            last_imported_chain_time,
                            "wu used from last anchor {} reached limit {} on length {}, will import next anchor",
                            wu_used_from_last_anchor, wu_used_to_import_next_anchor,  uncommitted_chain_length,
                        );
                    }
                }

                let max_consensus_lag_rounds = working_state
                    .mc_data
                    .config
                    .get_consensus_config()?
                    .max_consensus_lag_rounds as u32;

                let mut imported_anchors_count = 0;
                let mut imported_anchors_has_externals = false;

                // import anchors until wu_used_from_last_anchor > wu_used_to_import_next_anchor
                loop {
                    // next anchor importing can stuck if mempool paused
                    // so allow to cancel collation here
                    let collation_cancelled = self.cancel_collation.notified();
                    let import_fut = Self::import_next_anchor(
                        self.shard_id,
                        &mut self.anchors_cache,
                        self.mpool_adapter.clone(),
                        working_state.mc_data.top_processed_to_anchor,
                        max_consensus_lag_rounds,
                    );

                    let import_anchor_result = tokio::select! {
                        res = import_fut => res?,
                        _ = collation_cancelled => {
                            tracing::info!(target: tracing_targets::COLLATOR,
                                top_processed_to_anchor,
                                last_imported_anchor_id,
                                last_imported_chain_time,
                                "collation was cancelled by manager on try_collate_next_shard_block",
                            );
                            metrics::counter!("tycho_collator_anchor_import_cancelled_count", &labels).increment(1);
                            self.listener
                                .on_cancelled(
                                    working_state.mc_data.block_id,
                                    working_state.next_block_id_short,
                                    CollationCancelReason::ExternalCancel,
                                )
                                .await?;
                            self.delayed_working_state.delay(working_state);
                            return Ok(());
                        }
                    };

                    match import_anchor_result {
                        ImportNextAnchor::Result {
                            prev_anchor_id,
                            get_anchor_result,
                            has_our_externals,
                        } => match get_anchor_result {
                            GetAnchorResult::NotExist => {
                                // cancel collation attempts if mempool cannot return required anchor
                                tracing::warn!(target: tracing_targets::COLLATOR,
                                    top_processed_to_anchor,
                                    last_imported_anchor_id,
                                    last_imported_chain_time,
                                    "next anchor not exist, cancel collation attempts",
                                );
                                self.listener
                                    .on_cancelled(
                                        working_state.mc_data.block_id,
                                        working_state.next_block_id_short,
                                        CollationCancelReason::NextAnchorNotFound(prev_anchor_id),
                                    )
                                    .await?;
                                self.delayed_working_state.delay(working_state);
                                return Ok(());
                            }
                            GetAnchorResult::Exist(_) => {
                                imported_anchors_count += 1;

                                // time elapsed from prev anchor
                                let elapsed_from_prev_anchor = self.anchor_timer.elapsed();
                                self.anchor_timer = std::time::Instant::now();
                                metrics::histogram!(
                                    "tycho_collator_from_prev_anchor_time",
                                    &labels
                                )
                                .record(elapsed_from_prev_anchor);

                                metrics::gauge!("tycho_collator_shard_blocks_count_btw_anchors")
                                    .set(self.shard_blocks_count_from_last_anchor);
                                self.shard_blocks_count_from_last_anchor = 0;

                                imported_anchors_has_externals |= has_our_externals;

                                // reduce used wu by one imported anchor
                                working_state.wu_used_from_last_anchor = working_state
                                    .wu_used_from_last_anchor
                                    .saturating_sub(wu_used_to_import_next_anchor);

                                tracing::debug!(target: tracing_targets::COLLATOR,
                                    wu_used_from_last_anchor = working_state.wu_used_from_last_anchor,
                                    force_import_anchor_by_used_wu,
                                    "used wu dropped to",
                                );

                                if working_state.wu_used_from_last_anchor
                                    < wu_used_to_import_next_anchor
                                {
                                    break;
                                }
                            }
                        },
                        ImportNextAnchor::Skipped => {
                            anchor_import_skipped = true;
                            break;
                        }
                    }
                }

                metrics::gauge!("tycho_collator_import_next_anchor_count")
                    .set(imported_anchors_count);

                if imported_anchors_has_externals {
                    tracing::debug!(target: tracing_targets::COLLATOR,
                        "just imported anchors has externals",
                    );
                }

                let has_externals = has_externals || imported_anchors_has_externals;

                match (has_uprocessed_messages, has_externals) {
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

        // when no messages for processing
        // then force empty shard block collation if related timeout elapsed
        if matches!(try_collate_check, TryCollateCheck::NoPendingMessages) {
            let empty_sc_block_interval_ms =
                working_state.collation_config.empty_sc_block_interval_ms as u64;
            let ct_elapsed_from_prev_block =
                last_imported_chain_time.saturating_sub(prev_block_chain_time);
            if empty_sc_block_interval_ms > 0
                && ct_elapsed_from_prev_block >= empty_sc_block_interval_ms
            {
                try_collate_check = TryCollateCheck::ForceEmptyShardBlock;
            }
        }

        // collate next shard block or skip
        match try_collate_check {
            TryCollateCheck::HasUnprocessedMessages
            | TryCollateCheck::HasExternals
            | TryCollateCheck::ForceEmptyShardBlock => {
                tracing::debug!(target: tracing_targets::COLLATOR,
                    reason = ?try_collate_check,
                    anchor_import_skipped,
                    top_processed_to_anchor,
                    last_imported_anchor_id,
                    last_imported_chain_time,
                    prev_block_chain_time,
                    "will collate next shard block",
                );

                // should force next master block collation after this shard block
                // when anchor import was skipped
                let force_next_mc_block = if anchor_import_skipped && uncommitted_chain_length > 4 {
                    ForceMasterCollation::ByAnchorImportSkipped
                } else {
                    ForceMasterCollation::No
                };

                drop(histogram);
                self.do_collate(working_state, None, force_next_mc_block)
                    .await?;
            }
            TryCollateCheck::NoPendingMessages
            | TryCollateCheck::ForceMcBlockByUncommittedChainLength => {
                tracing::debug!(target: tracing_targets::COLLATOR,
                    reason = ?try_collate_check,
                    anchor_import_skipped,
                    force_mc_block_by_uncommitted_chain,
                    top_processed_to_anchor,
                    last_imported_anchor_id,
                    last_imported_chain_time,
                    prev_block_chain_time,
                    uncommitted_chain_length,
                    max_uncommitted_chain_length,
                    "will NOT collate next shard block, will notify collation manager",
                );

                // we should force master block collation when anchor import was skipped
                // because without importing of next anchor chain time will not update
                // and master block interval will never be elapsed
                let force_mc_block = match try_collate_check {
                    TryCollateCheck::ForceMcBlockByUncommittedChainLength => {
                        ForceMasterCollation::ByUncommittedChain
                    }
                    _ if anchor_import_skipped => ForceMasterCollation::ByAnchorImportSkipped,
                    _ => ForceMasterCollation::No,
                };

                self.listener
                    .on_skipped(
                        working_state.mc_data.block_id,
                        working_state.next_block_id_short,
                        last_imported_chain_time,
                        force_mc_block,
                        working_state.collation_config.clone(),
                    )
                    .await?;

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

type DelayedWorkingStateFut =
    Pin<Box<dyn Future<Output = Result<Box<WorkingState>>> + Send + Sync + 'static>>;

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
        let labels = [("workchain", self.shard_id.workchain().to_string())];
        let _histogram = HistogramGuardWithLabels::begin(
            "tycho_collator_wait_for_working_state_time_high",
            &labels,
        );

        if let Some(state) = self.unused.take() {
            return Ok(state);
        }

        if let Some(fut) = self.future.take() {
            return fut.await;
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
    },
    Skipped,
}

#[derive(Debug)]
enum InitAnchorSource {
    FromCache(AnchorInfo),
    #[allow(dead_code)]
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
