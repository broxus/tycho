use std::pin::Pin;
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use error::CollatorError;
use everscale_types::cell::{Cell, HashBytes};
use everscale_types::models::*;
use futures_util::future::Future;
use mq_iterator_adapter::{InitIteratorMode, QueueIteratorAdapter};
use tokio::sync::{oneshot, Notify};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use tycho_block_util::block::calc_next_block_id_short;
use tycho_block_util::state::{MinRefMcStateTracker, ShardStateStuff};
use tycho_core::global_config::MempoolGlobalConfig;
use tycho_network::PeerId;
use tycho_util::futures::JoinTask;
use tycho_util::metrics::{HistogramGuard, HistogramGuardWithLabels};
use types::{AnchorInfo, AnchorsCache, MessagesBuffer};

use self::types::{CollatorStats, PrevData, WorkingState};
use crate::internal_queue::types::EnqueuedMessage;
use crate::mempool::{GetAnchorResult, MempoolAdapter, MempoolAnchorId};
use crate::queue_adapter::MessageQueueAdapter;
use crate::state_node::StateNodeAdapter;
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
mod mq_iterator_adapter;
mod types;

pub use error::CollationCancelReason;
pub use types::ForceMasterCollation;

#[cfg(test)]
#[path = "tests/collator_tests.rs"]
pub(super) mod tests;

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

    async fn start(&self, cx: CollatorContext) -> Self::Collator;
}

#[async_trait]
impl<F, FT, R> CollatorFactory for F
where
    F: Fn(CollatorContext) -> FT + Send + Sync + 'static,
    FT: Future<Output = R> + Send + 'static,
    R: Collator,
{
    type Collator = R;

    async fn start(&self, cx: CollatorContext) -> Self::Collator {
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

    async fn start(&self, cx: CollatorContext) -> Self::Collator {
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
    ) -> AsyncQueuedDispatcher<Self> {
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
            .expect("task receiver had to be not closed or dropped here");
        tracing::info!(target: tracing_targets::COLLATOR,
            "(next_block_id={}): collator initialization task enqueued", next_block_info,
        );

        tracing::info!(target: tracing_targets::COLLATOR,
            "(next_block_id={}): collator started", next_block_info,
        );

        dispatcher
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
        tracing::info!(target: tracing_targets::COLLATOR, "initializing...");

        // init working state
        let working_state = Self::init_working_state(
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
            prev_shard_data.processed_upto().externals.as_ref(),
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

            let anchors_info = match import_init_anchors_res {
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
    ) -> Result<Vec<AnchorInfo>, CollatorError> {
        let import_init_anchors = match &self.mempool_config_override {
            // There may be cases when processed to anchor in shard is before anchor in master.
            // We can produce incorrect shard block, then ignore it, take correct from bc and try to collate next one.
            Some(mempool_config) if mempool_config.start_round > 0 => {
                let import_init_anchors =
                    if anchors_proc_info.processed_to_anchor_id <= mempool_config.start_round {
                        if anchors_proc_info.processed_to_anchor_id == mempool_config.start_round {
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
                            // last processed_to anchor in shard can be before last procssed in master
                            // it is normal, so we should not cancel collation buy we unable to import init anchors
                            false
                        }
                    } else {
                        // when last processed_to anchor is after genesis start round then we can import init anchors
                        true
                    };

                if !import_init_anchors {
                    // needs to generate last imported anchor info
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
                        id: mempool_config.start_round,
                        ct: anchors_proc_info.last_imported_chain_time,
                        all_exts_count: 0,
                        our_exts_count: 0,
                        author: PeerId(created_by.0),
                    };
                    self.anchors_cache
                        .set_last_imported_anchor_info(anchor_info);
                }

                import_init_anchors
            }
            _ => true,
        };

        if import_init_anchors {
            tracing::debug!(target: tracing_targets::COLLATOR,
                next_block_id = %self.next_block_info,
                "importing anchors from processed to anchor ({}) with offset ({}) to chain_time {}",
                anchors_proc_info.processed_to_anchor_id,
                anchors_proc_info.processed_to_msgs_offset,
                anchors_proc_info.last_imported_chain_time,
            );

            Self::import_init_anchors(
                anchors_proc_info.processed_to_anchor_id,
                anchors_proc_info.processed_to_msgs_offset,
                anchors_proc_info.last_imported_chain_time,
                self.shard_id,
                &mut self.anchors_cache,
                self.mpool_adapter.clone(),
                working_state.mc_data.top_processed_to_anchor,
            )
            .await
        } else {
            Ok(vec![])
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
            HistogramGuard::begin_with_labels("tycho_collator_resume_collation_time", &labels);

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

        let working_state = if !reset {
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
            let working_state = Self::init_working_state(
                &self.next_block_info,
                self.state_node_adapter.clone(),
                mc_data,
                new_prev_blocks_ids,
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
                prev_shard_data.processed_upto().externals.as_ref(),
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

                let anchors_info = match import_init_anchors_res {
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
                }
            }

            working_state
        };
        drop(histogram);

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
        msgs_buffer: MessagesBuffer,
        tracker: MinRefMcStateTracker,
    ) -> Result<()> {
        let labels = [("workchain", self.shard_id.workchain().to_string())];
        let _histogram = HistogramGuard::begin_with_labels(
            "tycho_collator_prepare_working_state_update_time",
            &labels,
        );

        enum GetNewShardStateStuff {
            ReloadFromStorage(JoinTask<Result<bool>>),
            BuildFromNewObservable(ShardStateStuff),
        }

        let get_new_state_stuff = {
            if block_id.is_masterchain() {
                GetNewShardStateStuff::ReloadFromStorage(store_new_state_task)
            } else {
                // append new store task
                self.store_new_state_tasks.push(store_new_state_task);

                // build state stuff from new observable state after collation
                GetNewShardStateStuff::BuildFromNewObservable(ShardStateStuff::from_state_and_root(
                    &block_id,
                    new_observable_state,
                    new_state_root,
                    &tracker,
                )?)
            }
        };

        let state_node_adapter = self.state_node_adapter.clone();

        self.delayed_working_state.future = Some(Box::pin(async move {
            let new_state_stuff = match get_new_state_stuff {
                GetNewShardStateStuff::BuildFromNewObservable(state_stuff) => state_stuff,
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
                prev_shard_data: Some(prev_shard_data),
                usage_tree: Some(usage_tree),
                has_unprocessed_messages: Some(has_unprocessed_messages),
                msgs_buffer,
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
            prev_shard_data: Some(prev_shard_data),
            usage_tree: Some(usage_tree),
            has_unprocessed_messages: None,
            msgs_buffer: MessagesBuffer::new(
                next_block_id_short.shard,
                collation_config.msgs_exec_params.group_limit as _,
                collation_config.msgs_exec_params.group_vert_size as _,
            ),
            collation_config,
        }))
    }

    fn get_anchors_processing_info(
        shard_id: &ShardIdent,
        mc_data: &McData,
        prev_block_id: &BlockId,
        prev_gen_chain_time: u64,
        prev_processed_upto_externals: Option<&ExternalsProcessedUpto>,
    ) -> Option<AnchorsProcessingInfo> {
        // try get from mc data
        let mut info_opt = None;
        if !shard_id.is_masterchain() {
            if let Some((mc_processed_to_anchor_id, mc_processed_to_msgs_offset)) = mc_data
                .processed_upto
                .externals
                .as_ref()
                .map(|upto| upto.processed_to)
            {
                // TODO: consider split/merge

                // get from mc data if prev shard block is equal to the top shard
                // and top shard was not updated in master
                // it means that no shard blocks were collated between masters
                // because there were no messages for processing
                // and we can omit anchors rpocessing info from shard
                for (top_shard_id, top_shard_descr) in mc_data.shards.iter() {
                    if shard_id == top_shard_id
                        && prev_block_id.seqno == top_shard_descr.seqno
                        && !top_shard_descr.top_sc_block_updated
                    {
                        info_opt = Some(AnchorsProcessingInfo {
                            processed_to_anchor_id: mc_processed_to_anchor_id,
                            processed_to_msgs_offset: mc_processed_to_msgs_offset,
                            last_imported_chain_time: mc_data.gen_chain_time,
                            last_imported_in_block_id: mc_data.block_id,
                        });
                        break;
                    }
                }
            }
        }

        // try get from prev data
        if info_opt.is_none() {
            info_opt = prev_processed_upto_externals
                .as_ref()
                .map(|upto| AnchorsProcessingInfo {
                    processed_to_anchor_id: upto.processed_to.0,
                    processed_to_msgs_offset: upto.processed_to.1,
                    last_imported_chain_time: prev_gen_chain_time,
                    last_imported_in_block_id: *prev_block_id,
                });
        }

        info_opt
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
            HistogramGuardWithLabels::begin("tycho_collator_import_next_anchor_time", &labels);

        let timer = std::time::Instant::now();

        let (prev_anchor_id, ct) = anchors_cache
            .get_last_imported_anchor_id_and_ct()
            .unwrap_or_default();

        // do not import anchor if mempool may be paused
        // needs to process more anchors in collator first
        if prev_anchor_id.saturating_sub(top_processed_to_anchor) > max_consensus_lag_rounds / 2 {
            return Ok(ImportNextAnchor::Skipped);
        }

        let get_anchor_result = mpool_adapter
            .get_next_anchor(top_processed_to_anchor, prev_anchor_id)
            .await?;

        let has_our_externals = match &get_anchor_result {
            GetAnchorResult::Exist(next_anchor) => {
                let our_exts_count = next_anchor.count_externals_for(&shard_id, 0);

                let has_externals = our_exts_count > 0;

                anchors_cache.insert(next_anchor.clone(), our_exts_count);

                metrics::counter!("tycho_collator_ext_msgs_imported_count", &labels)
                    .increment(our_exts_count as _);
                metrics::gauge!("tycho_collator_ext_msgs_imported_queue_size", &labels)
                    .increment(our_exts_count as f64);

                let chain_time_elapsed = next_anchor.chain_time.saturating_sub(ct);

                tracing::info!(target: tracing_targets::COLLATOR,
                    elapsed = timer.elapsed().as_millis(),
                    chain_time_elapsed,
                    "imported next anchor (id: {}, chain_time: {}, total_exts: {}, our_exts: {})",
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
        shard_id: ShardIdent,
        anchors_cache: &mut AnchorsCache,
        mpool_adapter: Arc<dyn MempoolAdapter>,
        top_processed_to_anchor: MempoolAnchorId,
    ) -> Result<Vec<AnchorInfo>, CollatorError> {
        let labels = [("workchain", shard_id.workchain().to_string())];

        let mut anchors_info = vec![];
        let mut last_anchor = None;
        let mut all_anchors_are_taken_from_cache = false;
        let mut processed_to_anchor_exists_in_cache = false;

        let mut removed_our_exts_count_total: usize = 0;

        // first look for required anchors in cache
        let mut idx = 0;
        while let Some((_, anchor)) = anchors_cache.get(idx) {
            // remove all other anchors after last_block_chain_time
            if all_anchors_are_taken_from_cache {
                let our_exts_count = anchor.count_externals_for(&shard_id, 0);
                removed_our_exts_count_total =
                    removed_our_exts_count_total.saturating_add(our_exts_count);
                anchors_cache.remove(idx);
                continue;
            }
            // check if processed_to_anchor_id exists in cache
            if anchor.id == processed_to_anchor_id {
                processed_to_anchor_exists_in_cache = true;
            }
            // use anchors from cache only when processed_to_anchor_id exists in cache
            // otherwise we should clear the cache
            if processed_to_anchor_exists_in_cache && anchor.id >= processed_to_anchor_id {
                // when we found anchor with last_block_chain_time
                // it means that we have all required anchors in the cache
                if anchor.chain_time == last_block_chain_time {
                    all_anchors_are_taken_from_cache = true;
                }

                last_anchor = Some(anchor);
            }
            idx += 1;
        }

        // we have all required anchors in cache
        if all_anchors_are_taken_from_cache {
            // update last imported anchor info
            let last_anchor = last_anchor.unwrap();
            let our_exts_count = last_anchor.count_externals_for(&shard_id, 0);
            anchors_cache.set_last_imported_anchor_info(AnchorInfo::from_anchor(
                last_anchor,
                our_exts_count,
            ));

            // decrease queue size
            metrics::gauge!("tycho_collator_ext_msgs_imported_queue_size", &labels)
                .decrement(removed_our_exts_count_total as f64);

            return Ok(anchors_info);
        }

        // clear cache if processed_to_anchor_id does not exist in cache
        if !processed_to_anchor_exists_in_cache {
            anchors_cache.clear();
            metrics::gauge!("tycho_collator_ext_msgs_imported_queue_size", &labels).set(0);
        }

        let mut our_exts_count_total: usize = 0;

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
                    .get_anchor_by_id(top_processed_to_anchor, processed_to_anchor_id)
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

        // we count our messages for the first anchor from processed_to offset
        let mut offset = processed_to_msgs_offset as usize;

        loop {
            // add loaded anchor to cache
            if let Some(anchor) = next_anchor {
                let our_exts_count = anchor.count_externals_for(&shard_id, offset);
                our_exts_count_total = our_exts_count_total.saturating_add(our_exts_count);
                anchors_cache.insert(anchor.clone(), our_exts_count);
                anchors_info.push(AnchorInfo::from_anchor(anchor, our_exts_count));
            }

            // in other anchors will count from 0
            offset = 0;

            // import next anchor if last block chain time not reached
            if last_imported_anchor_ct >= last_block_chain_time {
                break;
            }

            let GetAnchorResult::Exist(anchor) = mpool_adapter
                .get_next_anchor(top_processed_to_anchor, prev_anchor_id)
                .await?
            else {
                return Err(CollatorError::Cancelled(
                    CollationCancelReason::NextAnchorNotFound(prev_anchor_id),
                ));
            };
            prev_anchor_id = anchor.id;
            last_imported_anchor_ct = anchor.chain_time;

            next_anchor = Some(anchor);
        }

        metrics::counter!("tycho_collator_ext_msgs_imported_count", &labels)
            .increment(our_exts_count_total as u64);
        metrics::gauge!("tycho_collator_ext_msgs_imported_queue_size", &labels)
            .increment(our_exts_count_total as f64);

        Ok(anchors_info)
    }

    fn check_has_unprocessed_messages(&mut self, working_state: &mut WorkingState) -> Result<bool> {
        if let Some(has_messages) = working_state.has_unprocessed_messages {
            return Ok(has_messages);
        }

        let prev_shard_data = working_state.prev_shard_data_ref();

        // when processing offset is > 0 we have uprocessed messages in buffer
        if prev_shard_data.processed_upto().processed_offset > 0 {
            working_state.has_unprocessed_messages = Some(true);
            return Ok(true);
        }

        // check messages buffer directly
        let msgs_buffer = &working_state.msgs_buffer;

        let has_pending_messages_in_buffer = msgs_buffer.has_pending_messages();
        if has_pending_messages_in_buffer {
            working_state.has_unprocessed_messages = Some(true);
            return Ok(true);
        }

        // check in queue using iterator
        let mut mq_iterator_adapter = QueueIteratorAdapter::new(
            self.shard_id,
            self.mq_adapter.clone(),
            msgs_buffer.current_iterator_positions.clone().unwrap(),
            working_state.mc_data.gen_lt,
            prev_shard_data.gen_lt(),
        );

        let mut current_processed_upto = prev_shard_data.processed_upto().clone();
        mq_iterator_adapter.try_init_next_range_iterator(
            &mut current_processed_upto,
            working_state
                .mc_data
                .shards
                .iter()
                .map(|(k, v)| (*k, v.end_lt)),
            InitIteratorMode::UseNextRange,
        )?;

        let has_internals = if !mq_iterator_adapter.no_pending_existing_internals() {
            mq_iterator_adapter.iterator().next(true)?.is_some()
        } else {
            false
        };

        working_state.has_unprocessed_messages = Some(has_internals);

        Ok(has_internals)
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
        let working_state = self.delayed_working_state.wait().await?;
        self.do_collate(working_state, Some(top_shard_blocks_info), next_chain_time)
            .await
    }

    /// Run collation if there are internals,
    /// otherwise import next anchor and notify it to manager
    /// that will route next collation steps
    #[tracing::instrument(
        parent = None, name = "try_collate_next_master_block",
        skip_all, fields(next_block_id = %self.next_block_info)
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
                    metrics::histogram!("tycho_do_collate_from_prev_anchor_time", &labels)
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
        skip_all, fields(next_block_id = %self.next_block_info)
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
                                    "tycho_do_collate_from_prev_anchor_time",
                                    &labels
                                )
                                .record(elapsed_from_prev_anchor);

                                metrics::gauge!("tycho_do_collate_shard_blocks_count_btw_anchors")
                                    .set(self.shard_blocks_count_from_last_anchor);
                                self.shard_blocks_count_from_last_anchor = 0;

                                imported_anchors_has_externals |= has_our_externals;

                                working_state.wu_used_from_last_anchor = working_state
                                    .wu_used_from_last_anchor
                                    .saturating_sub(wu_used_to_import_next_anchor);

                                tracing::debug!(target: tracing_targets::COLLATOR,
                                    wu_used_from_last_anchor = working_state.wu_used_from_last_anchor,
                                    force_import_anchor_by_used_wu,
                                    "wu_used_from_last_anchor dropped to",
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

                metrics::gauge!("tycho_do_collate_import_next_anchor_count")
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

                drop(histogram);
                self.do_collate(working_state, None, last_imported_chain_time)
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
        let _histogram =
            HistogramGuardWithLabels::begin("tycho_collator_wait_for_working_state_time", &labels);

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
