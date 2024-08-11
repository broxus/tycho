use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use everscale_types::models::*;
use execution_manager::ExecutionManager;
use futures_util::future::{BoxFuture, Future};
use mq_iterator_adapter::QueueIteratorAdapter;
use tokio::sync::oneshot;
use tycho_block_util::state::ShardStateStuff;
use tycho_util::futures::JoinTask;
use tycho_util::metrics::HistogramGuardWithLabels;
use types::AnchorInfo;

use self::types::{CachedMempoolAnchor, CollatorStats, PrevData, WorkingState};
use crate::mempool::{MempoolAdapter, MempoolAnchor, MempoolAnchorId};
use crate::queue_adapter::MessageQueueAdapter;
use crate::state_node::StateNodeAdapter;
use crate::types::{
    BlockCollationResult, CollationConfig, CollationSessionId, CollationSessionInfo, McData,
    TopBlockDescription,
};
use crate::utils::async_queued_dispatcher::{
    AsyncQueuedDispatcher, STANDARD_QUEUED_DISPATCHER_BUFFER_SIZE,
};
use crate::{method_to_queued_async_closure, tracing_targets};

mod build_block;
mod do_collate;
mod execution_manager;
mod mq_iterator_adapter;
mod types;

// FACTORY

pub struct CollatorContext {
    pub mq_adapter: Arc<dyn MessageQueueAdapter>,
    pub mpool_adapter: Arc<dyn MempoolAdapter>,
    pub state_node_adapter: Arc<dyn StateNodeAdapter>,
    pub config: Arc<CollationConfig>,
    pub collation_session: Arc<CollationSessionInfo>,
    pub listener: Arc<dyn CollatorEventListener>,
    pub shard_id: ShardIdent,
    pub prev_blocks_ids: Vec<BlockId>,
    pub mc_data: Arc<McData>,
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
    /// Process anchor that was skipped without block collation
    /// or due to master block force condition
    async fn on_skipped_anchor(
        &self,
        next_block_id_short: BlockIdShort,
        anchor: Arc<MempoolAnchor>,
        force_mc_block: bool,
    ) -> Result<()>;
    /// Process new collated shard or master block
    async fn on_block_candidate(&self, collation_result: BlockCollationResult) -> Result<()>;
    /// Process collator stopped event
    async fn on_collator_stopped(&self, stop_key: CollationSessionId) -> Result<()>;
}

// COLLATOR

#[async_trait]
pub trait Collator: Send + Sync + 'static {
    /// Enqueue collator stop task
    async fn equeue_stop(&self, stop_key: CollationSessionId) -> Result<()>;
    /// Enqueue update of McData in working state and run attempt to collate next shard block
    async fn equeue_update_mc_data_and_try_collate(&self, mc_data: Arc<McData>) -> Result<()>;
    /// Enqueue next attemt to collate block
    /// (with check if there are internals or externals for collation).
    /// Check implementation for master and shards for details
    async fn equeue_try_collate(&self) -> Result<()>;
    /// Enqueue new block collation (without check of internals and externals)
    async fn equeue_do_collate(
        &self,
        top_shard_blocks_info: Vec<TopBlockDescription>,
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
        )
        .await
    }
}

#[async_trait]
impl Collator for AsyncQueuedDispatcher<CollatorStdImpl> {
    async fn equeue_stop(&self, _stop_key: CollationSessionId) -> Result<()> {
        todo!()
    }

    async fn equeue_update_mc_data_and_try_collate(&self, mc_data: Arc<McData>) -> Result<()> {
        self.enqueue_task(method_to_queued_async_closure!(
            update_mc_data_and_try_collate,
            mc_data
        ))
        .await
    }

    async fn equeue_try_collate(&self) -> Result<()> {
        self.enqueue_task(method_to_queued_async_closure!(try_collate,))
            .await
    }

    async fn equeue_do_collate(
        &self,
        top_shard_blocks_info: Vec<TopBlockDescription>,
    ) -> Result<()> {
        self.enqueue_task(method_to_queued_async_closure!(
            wait_state_and_do_collate,
            top_shard_blocks_info
        ))
        .await
    }
}

pub struct CollatorStdImpl {
    next_block_id_short: BlockIdShort,

    config: Arc<CollationConfig>,
    collation_session: Arc<CollationSessionInfo>,

    dispatcher: AsyncQueuedDispatcher<Self>,
    listener: Arc<dyn CollatorEventListener>,
    mq_adapter: Arc<dyn MessageQueueAdapter>,
    exec_manager: Option<ExecutionManager>,
    mpool_adapter: Arc<dyn MempoolAdapter>,
    state_node_adapter: Arc<dyn StateNodeAdapter>,
    shard_id: ShardIdent,
    working_state: DelayedWorkingState,

    /// The cache of imported from mempool anchors that were not processed yet.
    /// Anchor is removed from the cache when all its externals are processed.
    anchors_cache: VecDeque<(MempoolAnchorId, CachedMempoolAnchor)>,

    last_imported_anchor: Option<AnchorInfo>,

    /// TRUE - when exist imported anchors in cache,
    /// when they have externals for current shard of collator,
    /// and not all these externals were read.
    ///
    /// Updated in the `import_next_anchor()` and `read_next_externals()`
    has_pending_externals: bool,

    stats: CollatorStats,
    timer: std::time::Instant,
}

impl CollatorStdImpl {
    #[allow(clippy::too_many_arguments)]
    pub async fn start(
        mq_adapter: Arc<dyn MessageQueueAdapter>,
        mpool_adapter: Arc<dyn MempoolAdapter>,
        state_node_adapter: Arc<dyn StateNodeAdapter>,
        config: Arc<CollationConfig>,
        collation_session: Arc<CollationSessionInfo>,
        listener: Arc<dyn CollatorEventListener>,
        shard_id: ShardIdent,
        prev_blocks_ids: Vec<BlockId>,
        mc_data: Arc<McData>,
    ) -> AsyncQueuedDispatcher<Self> {
        let max_prev_seqno = prev_blocks_ids.iter().map(|id| id.seqno).max().unwrap();
        let next_block_id_short = BlockIdShort {
            shard: shard_id,
            seqno: max_prev_seqno + 1,
        };
        tracing::info!(target: tracing_targets::COLLATOR,
            "Collator (block_id={}): starting...", next_block_id_short,
        );

        // create dispatcher for own async tasks queue
        let (dispatcher, receiver) =
            AsyncQueuedDispatcher::new(STANDARD_QUEUED_DISPATCHER_BUFFER_SIZE);

        let exec_manager = ExecutionManager::new(
            shard_id,
            mq_adapter.clone(),
            config.msgs_exec_params.buffer_limit as _,
            config.msgs_exec_params.group_limit as _,
            config.msgs_exec_params.group_vert_size as _,
        );

        let (working_state_tx, working_state_rx) = oneshot::channel::<Result<WorkingState>>();

        let processor = Self {
            next_block_id_short,
            config,
            collation_session,
            dispatcher: dispatcher.clone(),
            listener,
            mq_adapter,
            exec_manager: Some(exec_manager),
            mpool_adapter,
            state_node_adapter,
            shard_id,
            working_state: DelayedWorkingState::new(shard_id, async move {
                match working_state_rx.await {
                    Ok(state) => state,
                    Err(_) => anyhow::bail!("collator init cancelled"),
                }
            }),

            anchors_cache: VecDeque::new(),
            last_imported_anchor: None,

            has_pending_externals: false,

            stats: Default::default(),
            timer: std::time::Instant::now(),
        };

        AsyncQueuedDispatcher::run(processor, receiver);
        tracing::trace!(target: tracing_targets::COLLATOR,
            "Collator (block_id={}): tasks queue dispatcher started", next_block_id_short,
        );

        // equeue first initialization task
        // sending to the receiver here cannot return Error because it is guaranteed not closed or dropped
        dispatcher
            .enqueue_task(method_to_queued_async_closure!(
                init,
                prev_blocks_ids,
                mc_data,
                working_state_tx
            ))
            .await
            .expect("task receiver had to be not closed or dropped here");
        tracing::info!(target: tracing_targets::COLLATOR,
            "Collator (block_id={}): initialization task enqueued", next_block_id_short,
        );

        tracing::info!(target: tracing_targets::COLLATOR,
            "Collator (block_id={}): started", next_block_id_short,
        );

        dispatcher
    }

    // Initialize collator working state then run collation
    async fn init(
        &mut self,
        prev_blocks_ids: Vec<BlockId>,
        mc_data: Arc<McData>,
        working_state_tx: oneshot::Sender<Result<WorkingState>>,
    ) -> Result<()> {
        tracing::info!(target: tracing_targets::COLLATOR,
            "Collator (block_id={}): initializing...", self.next_block_id_short,
        );

        // init working state

        // load states
        tracing::info!(target: tracing_targets::COLLATOR,
            "Collator (block_id={}): init: loading initial shard state...", self.next_block_id_short,
        );
        let prev_states =
            Self::load_init_states(self.state_node_adapter.as_ref(), prev_blocks_ids).await?;

        // build, validate and set working state
        tracing::info!(target: tracing_targets::COLLATOR,
            "Collator (block_id={}): init: building working state...", self.next_block_id_short,
        );

        let working_state = Self::build_and_validate_working_state(mc_data, prev_states)?;

        if let Some(processed_upto_anchor_id) = working_state
            .prev_shard_data
            .processed_upto()
            .externals
            .as_ref()
            .map(|upto| upto.processed_to)
        {
            tracing::info!(target: tracing_targets::COLLATOR,
                "Collator (block_id={}): init: import anchors from processed upto anchor id {} offset {} ...",
                self.next_block_id_short,
                processed_upto_anchor_id.0,
                processed_upto_anchor_id.1,
            );
            self.import_anchors_on_init(
                processed_upto_anchor_id.0,
                processed_upto_anchor_id.1 as usize,
                working_state.prev_shard_data.gen_chain_time(),
            )
            .await?;
        }

        working_state_tx.send(Ok(working_state)).ok();

        self.timer = std::time::Instant::now();

        // TODO: collate right now instead of queuing

        // enqueue collation attempt of next block
        self.dispatcher
            .enqueue_task(method_to_queued_async_closure!(try_collate,))
            .await?;
        tracing::info!(target: tracing_targets::COLLATOR,
            "Collator (block_id={}): init: collation attempt enqueued", self.next_block_id_short,
        );

        tracing::info!(target: tracing_targets::COLLATOR,
            "Collator (block_id={}): init: finished", self.next_block_id_short,
        );

        Ok(())
    }

    /// Update working state from new block and state after block collation
    fn update_working_state(
        &mut self,
        block_id: &BlockId,
        new_state_stuff: JoinTask<Result<ShardStateStuff>>,
        mc_data: Option<Arc<McData>>,
        has_pending_internals: bool,
        prev_working_state: WorkingState,
    ) -> Result<()> {
        self.next_block_id_short = BlockIdShort {
            shard: block_id.shard,
            seqno: block_id.seqno + 1,
        };

        // TODO: Check if the result mc_data is properly updated on masterchain block
        let mc_data = mc_data.unwrap_or_else(|| prev_working_state.mc_data);

        self.working_state.future = Some(Box::pin(async move {
            let new_state_stuff = new_state_stuff.await?;

            let prev_states = vec![new_state_stuff];
            Self::check_prev_states_and_master(&mc_data, &prev_states)?;
            let (prev_shard_data, usage_tree) = PrevData::build(prev_states)?;

            Ok(WorkingState {
                mc_data: mc_data.into(),
                prev_shard_data,
                usage_tree,
                has_pending_internals: Some(has_pending_internals),
            })
        }));

        tracing::debug!(target: tracing_targets::COLLATOR,
            "working state updated from just collated block",
        );

        Ok(())
    }

    /// Load required initial states:
    /// master state + list of previous shard states
    async fn load_init_states(
        state_node_adapter: &dyn StateNodeAdapter,
        prev_blocks_ids: Vec<BlockId>,
    ) -> Result<Vec<ShardStateStuff>> {
        // otherwise await prev states by prev block ids
        let mut prev_states = vec![];
        for prev_block_id in prev_blocks_ids {
            // request state for prev block and wait for response
            let state = state_node_adapter.load_state(&prev_block_id).await?;
            tracing::info!(
                target: tracing_targets::COLLATOR,
                "To init working state loaded prev shard state for prev_block_id {}",
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
    fn build_and_validate_working_state(
        mc_data: Arc<McData>,
        prev_states: Vec<ShardStateStuff>,
    ) -> Result<WorkingState> {
        // TODO: make real implementation

        Self::check_prev_states_and_master(&mc_data, &prev_states)?;
        let (prev_shard_data, usage_tree) = PrevData::build(prev_states)?;

        Ok(WorkingState {
            mc_data: mc_data.into(),
            prev_shard_data,
            usage_tree,
            has_pending_internals: None,
        })
    }

    /// (TODO) Perform some checks on master state and prev states
    fn check_prev_states_and_master(
        _mc_data: &McData,
        _prev_states: &[ShardStateStuff],
    ) -> Result<()> {
        // TODO: make real implementation
        // refer to the old node impl:
        //  Collator::unpack_last_state()
        Ok(())
    }

    /// 1. Get last imported anchor from cache or last processed from `externals_processed_upto`
    /// 2. Await next anchor via mempool adapter
    /// 3. Store anchor in cache and return it
    ///
    /// Returns: (`next_anchor`, `has_externals`)
    async fn import_next_anchor(&mut self) -> Result<(Arc<MempoolAnchor>, bool)> {
        let labels = [("workchain", self.shard_id.workchain().to_string())];

        let _histogram =
            HistogramGuardWithLabels::begin("tycho_collator_import_next_anchor_time", &labels);

        let timer = std::time::Instant::now();

        let next_anchor = self
            .mpool_adapter
            .get_next_anchor(
                self.last_imported_anchor
                    .as_ref()
                    .map(|a| a.id)
                    .unwrap_or_default(),
            )
            .await?;

        let our_exts_count = next_anchor.count_externals_for(&self.shard_id, 0);
        let has_externals = our_exts_count > 0;
        if has_externals {
            self.has_pending_externals = true;

            self.anchors_cache
                .push_back((next_anchor.id, CachedMempoolAnchor {
                    anchor: next_anchor.clone(),
                    has_externals,
                }));
        }

        metrics::counter!("tycho_collator_ext_msgs_imported_count", &labels)
            .increment(our_exts_count as _);
        metrics::gauge!("tycho_collator_ext_msgs_imported_queue_size", &labels)
            .increment(our_exts_count as f64);

        let chain_time_elapsed = next_anchor.chain_time
            - self
                .last_imported_anchor
                .as_ref()
                .map(|a| a.ct)
                .unwrap_or_default();

        self.last_imported_anchor =
            Some(AnchorInfo::from_anchor(next_anchor.clone(), our_exts_count));

        tracing::debug!(target: tracing_targets::COLLATOR,
            elapsed = timer.elapsed().as_millis(),
            chain_time_elapsed,
            "imported next anchor (id: {}, chain_time: {}, externals: {})",
            next_anchor.id,
            next_anchor.chain_time,
            next_anchor.externals.len(),
        );

        Ok((next_anchor, has_externals))
    }

    /// 1. Get anchor from `externals_processed_upto`
    /// 2. Get next anchors until `last_block_chain_time`
    /// 3. Store anchors in cache
    async fn import_anchors_on_init(
        &mut self,
        processed_upto_anchor_id: u32,
        processed_upto_offset: usize,
        last_block_chain_time: u64,
    ) -> Result<()> {
        let labels = [("workchain", self.shard_id.workchain().to_string())];

        let timer = std::time::Instant::now();

        let mut next_anchor = self
            .mpool_adapter
            .get_anchor_by_id(processed_upto_anchor_id)
            .await?
            .unwrap();

        let mut anchors_info: Vec<AnchorInfo> = vec![];

        let mut our_exts_count =
            next_anchor.count_externals_for(&self.shard_id, processed_upto_offset);
        let has_externals = our_exts_count > 0;
        if has_externals {
            self.has_pending_externals = true;

            self.anchors_cache
                .push_back((next_anchor.id, CachedMempoolAnchor {
                    anchor: next_anchor.clone(),
                    has_externals,
                }));
        }

        let mut last_imported_anchor = AnchorInfo::from_anchor(next_anchor, our_exts_count);

        anchors_info.push(last_imported_anchor.clone());

        while last_block_chain_time > last_imported_anchor.ct {
            next_anchor = self
                .mpool_adapter
                .get_next_anchor(last_imported_anchor.id)
                .await?;

            our_exts_count = next_anchor.count_externals_for(&self.shard_id, 0);
            let has_externals = our_exts_count > 0;
            if has_externals {
                self.has_pending_externals = true;

                self.anchors_cache
                    .push_back((next_anchor.id, CachedMempoolAnchor {
                        anchor: next_anchor.clone(),
                        has_externals,
                    }));
            }

            last_imported_anchor = AnchorInfo::from_anchor(next_anchor, our_exts_count);

            anchors_info.push(last_imported_anchor.clone());
        }

        let imported_count: usize = anchors_info.iter().map(|a| a.our_exts_count).sum();
        metrics::counter!("tycho_collator_ext_msgs_imported_count", &labels)
            .increment(imported_count as u64);
        metrics::gauge!("tycho_collator_ext_msgs_imported_queue_size", &labels)
            .increment(imported_count as f64);

        self.last_imported_anchor = Some(last_imported_anchor);

        tracing::debug!(target: tracing_targets::COLLATOR,
            elapsed = timer.elapsed().as_millis(),
            "Collator (block_id={}): init: imported anchors on init: {:?}",
            self.next_block_id_short,
            anchors_info.as_slice()
        );

        Ok(())
    }

    async fn check_has_pending_internals(
        &mut self,
        working_state: &mut WorkingState,
    ) -> Result<bool> {
        if let Some(has_internals) = working_state.has_pending_internals {
            return Ok(has_internals);
        }

        let has_pending_messages_in_buffer = self
            .exec_manager
            .as_ref()
            .unwrap()
            .has_pending_messages_in_buffer();
        if has_pending_messages_in_buffer {
            working_state.has_pending_internals = Some(true);
            return Ok(true);
        }

        let mut mq_iterator_adapter = QueueIteratorAdapter::new(
            self.shard_id,
            self.mq_adapter.clone(),
            Some(
                self.exec_manager
                    .as_ref()
                    .unwrap()
                    .get_current_iterator_positions(),
            ),
        );

        let mut current_processed_upto = working_state.prev_shard_data.processed_upto().clone();
        mq_iterator_adapter
            .try_init_next_range_iterator(&mut current_processed_upto, working_state)
            .await?;

        let has_internals = if !mq_iterator_adapter.no_pending_existing_internals() {
            mq_iterator_adapter.iterator().next(true)?.is_some()
        } else {
            false
        };
        working_state.has_pending_internals = Some(has_internals);

        Ok(has_internals)
    }

    async fn update_mc_data_and_try_collate(&mut self, mc_data: Arc<McData>) -> Result<()> {
        let mut working_state = self.working_state.wait().await?;

        working_state.mc_data = mc_data;
        if working_state.has_pending_internals == Some(false) {
            working_state.has_pending_internals = None;
        }

        self.try_collate_next_shard_block_impl(working_state).await
    }

    fn try_collate(&mut self) -> BoxFuture<'_, Result<()>> {
        // NOTE: Prevents recursive future creation
        Box::pin(async move { self.try_collate_impl().await })
    }

    async fn try_collate_impl(&mut self) -> Result<()> {
        let working_state = self.working_state.wait().await?;

        if self.shard_id.is_masterchain() {
            self.try_collate_next_master_block_impl(working_state).await
        } else {
            self.try_collate_next_shard_block_impl(working_state).await
        }
    }

    async fn wait_state_and_do_collate(
        &mut self,
        top_shard_blocks_info: Vec<TopBlockDescription>,
    ) -> Result<()> {
        let working_state = self.working_state.wait().await?;
        self.do_collate(working_state, Some(top_shard_blocks_info))
            .await
    }

    /// Run collation if there are internals,
    /// otherwise import next anchor and notify it to manager
    /// that will route next collation steps
    #[tracing::instrument(name = "try_collate_next_master_block", skip_all, fields(block_id = %self.next_block_id_short))]
    async fn try_collate_next_master_block_impl(
        &mut self,
        mut working_state: WorkingState,
    ) -> Result<()> {
        tracing::debug!(target: tracing_targets::COLLATOR,
            "Check if can collate next master block",
        );

        let labels = [("workchain", self.shard_id.workchain().to_string())];

        let _histogram = HistogramGuardWithLabels::begin(
            "tycho_collator_try_collate_next_master_block_time",
            &labels,
        );

        // check has pending internals
        let has_internals = self.check_has_pending_internals(&mut working_state).await?;
        if has_internals {
            // collate if has internals
            tracing::info!(target: tracing_targets::COLLATOR,
                "there are unprocessed internals from previous block, will collate next block",
            );
            self.do_collate(working_state, None).await?;
        } else {
            // otherwise import next anchor and return it notify to manager
            tracing::debug!(target: tracing_targets::COLLATOR,
                "there are no internals, will import next anchor",
            );
            let (next_anchor, has_externals) = self.import_next_anchor().await?;
            if has_externals {
                tracing::debug!(target: tracing_targets::COLLATOR,
                    "just imported anchor has externals for master",
                );
            }
            // this may start master block collation or cause next anchor import
            self.listener
                .on_skipped_anchor(self.next_block_id_short, next_anchor, false)
                .await?;

            self.working_state.delay(working_state)
        }

        Ok(())
    }

    /// Run collation if there are internals or externals,
    /// otherwise import next anchor.
    /// If it has externals then run collation,
    /// otherwise return empty anchor to manager
    /// that will route next collation steps
    #[tracing::instrument(name = "try_collate_next_shard_block", skip_all, fields(block_id = %self.next_block_id_short))]
    async fn try_collate_next_shard_block_impl(
        &mut self,
        mut working_state: WorkingState,
    ) -> Result<()> {
        tracing::debug!(target: tracing_targets::COLLATOR,
            "Check if can collate next shard block",
        );

        let labels = [("workchain", self.shard_id.workchain().to_string())];

        let collation_prepare_histogram = HistogramGuardWithLabels::begin(
            "tycho_collator_try_collate_next_shard_block_without_do_collate_time",
            &labels,
        );

        // check has pending internals
        let has_internals = self.check_has_pending_internals(&mut working_state).await?;
        if has_internals {
            tracing::info!(target: tracing_targets::COLLATOR,
                "there are unprocessed internals from previous block, will collate next block",
            );
        }

        // check pending externals
        let mut has_externals = self.has_pending_externals;
        if has_externals {
            tracing::info!(target: tracing_targets::COLLATOR,
                "there are pending externals from previously imported anchors, will collate next block",
            );
        }

        // calc uncommitted chain length
        let mut last_committed_seqno = 0;
        for shard in working_state.mc_data.shards.iter() {
            let (shard_id, shard_descr) = shard?;
            if shard_id == self.shard_id {
                last_committed_seqno = shard_descr.seqno;
            }
        }
        let uncommitted_chain_length = self.next_block_id_short.seqno - 1 - last_committed_seqno;

        // check if should force master block collation
        let force_mc_block_by_uncommitted_chain =
            uncommitted_chain_length >= self.config.max_uncommitted_chain_length;

        // should import anchor after fixed gas used by shard blocks in uncommitted blocks chain
        let gas_used_from_last_anchor = working_state.prev_shard_data.gas_used_from_last_anchor();
        let force_import_anchor_by_used_gas = uncommitted_chain_length > 0
            && gas_used_from_last_anchor > self.config.gas_used_to_import_next_anchor;

        // check if has pending internals or externals
        let no_pending_msgs = !has_internals && !has_externals;

        // import next anchor if meet one of above conditions
        let next_anchor_info_opt = if no_pending_msgs
            || force_import_anchor_by_used_gas
            || force_mc_block_by_uncommitted_chain
        {
            if no_pending_msgs {
                tracing::debug!(target: tracing_targets::COLLATOR,
                    "there are no internals or pending externals, will import next anchor",
                );
            } else if force_mc_block_by_uncommitted_chain {
                tracing::info!(target: tracing_targets::COLLATOR,
                    "max_uncommitted_chain_length {} reached, will import next anchor",
                    self.config.max_uncommitted_chain_length,
                );
            } else if force_import_anchor_by_used_gas {
                tracing::info!(target: tracing_targets::COLLATOR,
                    "gas used from last anchor {} reached limit {} on length {}, will import next anchor",
                    gas_used_from_last_anchor, self.config.gas_used_to_import_next_anchor,  uncommitted_chain_length,
                );
            }
            let (next_anchor, next_anchor_has_externals) = self.import_next_anchor().await?;
            has_externals = next_anchor_has_externals;
            if has_externals && !force_mc_block_by_uncommitted_chain {
                tracing::info!(target: tracing_targets::COLLATOR,
                    "just imported anchor has externals, will collate next block",
                );
            }
            working_state.prev_shard_data.clear_gas_used();
            Some((next_anchor, next_anchor_has_externals))
        } else {
            None
        };

        drop(collation_prepare_histogram);

        // collate block if has internals or externals
        if (has_internals || has_externals) && !force_mc_block_by_uncommitted_chain {
            self.do_collate(working_state, None).await?;
        } else {
            // otherwise we have definitely imported the next anchor
            let (next_anchor, next_anchor_has_externals) =
                next_anchor_info_opt.expect("should be Some here");
            // notify manager when next anchor was imported but id does not contain externals
            if !next_anchor_has_externals || force_mc_block_by_uncommitted_chain {
                // this may start master block collation or next shard block collation attempt
                if force_mc_block_by_uncommitted_chain {
                    tracing::info!(target: tracing_targets::COLLATOR,
                        "force_mc_block_by_uncommitted_chain={}, will notify collation manager",
                        force_mc_block_by_uncommitted_chain,
                    );
                } else {
                    tracing::debug!(target: tracing_targets::COLLATOR,
                        "just imported anchor has no externals for current shard, will notify collation manager",
                    );
                }
                self.listener
                    .on_skipped_anchor(
                        self.next_block_id_short,
                        next_anchor,
                        force_mc_block_by_uncommitted_chain,
                    )
                    .await?;
            }

            self.working_state.delay(working_state);
        }

        Ok(())
    }
}

struct DelayedWorkingState {
    shard_id: ShardIdent,
    future: Option<Pin<Box<dyn Future<Output = Result<WorkingState>> + Send + Sync + 'static>>>,
    unused: Option<WorkingState>,
}

impl DelayedWorkingState {
    fn new<F>(shard_id: ShardIdent, f: F) -> Self
    where
        F: Future<Output = Result<WorkingState>> + Send + Sync + 'static,
    {
        Self {
            shard_id,
            future: Some(Box::pin(f)),
            unused: None,
        }
    }

    #[must_use]
    async fn wait(&mut self) -> Result<WorkingState> {
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

    fn delay(&mut self, state: WorkingState) {
        self.unused = Some(state);
    }
}
