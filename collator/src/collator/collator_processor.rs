use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use everscale_types::models::{
    BlockId, BlockIdShort, BlockInfo, OwnedMessage, ShardIdent, ValueFlow,
};

use tycho_block_util::state::{MinRefMcStateTracker, ShardStateStuff};

use crate::tracing_targets;
use crate::{
    mempool::{MempoolAdapter, MempoolAnchor, MempoolAnchorId},
    method_to_async_task_closure,
    msg_queue::{MessageQueueAdapter, QueueIterator},
    state_node::StateNodeAdapter,
    types::{CollationConfig, CollationSessionInfo},
    utils::async_queued_dispatcher::AsyncQueuedDispatcher,
};

use super::{
    types::{McData, PrevData, WorkingState},
    CollatorEventListener,
};

#[path = "./build_block.rs"]
mod build_block;
#[path = "./do_collate.rs"]
mod do_collate;
#[path = "./execution_manager.rs"]
mod execution_manager;

// COLLATOR PROCESSOR

#[async_trait]
pub(super) trait CollatorProcessor<MQ, MP, ST>: Sized + Send + 'static {
    fn new(
        collator_descr: Arc<String>,
        config: Arc<CollationConfig>,
        collation_session: Arc<CollationSessionInfo>,
        dispatcher: Arc<AsyncQueuedDispatcher<Self, ()>>,
        listener: Arc<dyn CollatorEventListener>,
        mq_adapter: Arc<MQ>,
        mpool_adapter: Arc<MP>,
        state_node_adapter: Arc<ST>,
        shard_id: ShardIdent,
        state_tracker: Arc<MinRefMcStateTracker>,
    ) -> Self;

    // Initialize collator working state then run collation
    async fn init(
        &mut self,
        prev_blocks_ids: Vec<BlockId>,
        mc_state: Arc<ShardStateStuff>,
    ) -> Result<()>;

    /// Update McData in working state
    /// and equeue next attempt to collate block
    async fn update_mc_data_and_resume_collation(
        &mut self,
        mc_state: Arc<ShardStateStuff>,
    ) -> Result<()>;

    /// Attempt to collate next shard block
    /// 1. Run collation if there are internals or pending externals from previously imported anchors
    /// 2. Otherwise request next anchor with externals
    /// 3. If no internals or externals then notify manager about skipped empty anchor
    async fn try_collate_next_shard_block(&mut self) -> Result<()>;

    /// Collate one block
    async fn do_collate(
        &mut self,
        next_chain_time: u64,
        top_shard_blocks_info: Vec<(BlockId, BlockInfo, ValueFlow)>,
    ) -> Result<()>;
}

#[async_trait]
impl<MQ, MP, ST> CollatorProcessor<MQ, MP, ST> for CollatorProcessorStdImpl<MQ, MP, ST>
where
    MQ: MessageQueueAdapter,
    MP: MempoolAdapter,
    ST: StateNodeAdapter,
{
    fn new(
        collator_descr: Arc<String>,
        config: Arc<CollationConfig>,
        collation_session: Arc<CollationSessionInfo>,
        dispatcher: Arc<AsyncQueuedDispatcher<Self, ()>>,
        listener: Arc<dyn CollatorEventListener>,
        mq_adapter: Arc<MQ>,
        mpool_adapter: Arc<MP>,
        state_node_adapter: Arc<ST>,
        shard_id: ShardIdent,
        state_tracker: Arc<MinRefMcStateTracker>,
    ) -> Self {
        Self {
            collator_descr,
            config,
            collation_session,
            dispatcher,
            listener,
            mq_adapter,
            mpool_adapter,
            state_node_adapter,
            shard_id,
            working_state: None,

            anchors_cache: BTreeMap::new(),
            last_imported_anchor_id: None,
            last_imported_anchor_chain_time: None,

            externals_read_upto: BTreeMap::new(),
            has_pending_externals: false,

            state_tracker,
        }
    }

    // Initialize collator working state then run collation
    async fn init(
        &mut self,
        prev_blocks_ids: Vec<BlockId>,
        mc_state: Arc<ShardStateStuff>,
    ) -> Result<()> {
        tracing::info!(target: tracing_targets::COLLATOR, "Collator init ({}): processing...", self.collator_descr());

        // init working state

        // load states
        tracing::info!(target: tracing_targets::COLLATOR, "Collator init ({}): loading initial shard state...", self.collator_descr());
        let (mc_state, prev_states) = Self::load_init_states(
            self.state_node_adapter.clone(),
            self.shard_id,
            prev_blocks_ids.clone(),
            mc_state,
        )
        .await?;

        // build, validate and set working state
        tracing::info!(target: tracing_targets::COLLATOR, "Collator init ({}): building working state...", self.collator_descr());
        let working_state =
            Self::build_and_validate_working_state(mc_state, prev_states, prev_blocks_ids.clone())?;
        self.set_working_state(working_state);

        // master block collations will be called by the collation manager directly

        // enqueue collation attempt of next shard block
        if !self.shard_id.is_masterchain() {
            self.dispatcher
                .enqueue_task(method_to_async_task_closure!(try_collate_next_shard_block,))
                .await?;
            tracing::info!(target: tracing_targets::COLLATOR, "Collator init ({}): collation attempt enqueued", self.collator_descr());
        }

        tracing::info!(target: tracing_targets::COLLATOR, "Collator init ({}): finished", self.collator_descr());

        Ok(())
    }

    async fn update_mc_data_and_resume_collation(
        &mut self,
        mc_state: Arc<ShardStateStuff>,
    ) -> Result<()> {
        self.update_mc_data(mc_state)?;

        self.dispatcher
            .enqueue_task(method_to_async_task_closure!(try_collate_next_shard_block,))
            .await
    }

    async fn try_collate_next_shard_block(&mut self) -> Result<()> {
        tracing::trace!(
            target: tracing_targets::COLLATOR,
            "Collator ({}): checking if can collate next block",
            self.collator_descr(),
        );

        //TODO: fix the work with internals

        // check internals
        let has_internals = self.has_internals()?;
        if has_internals {
            tracing::debug!(
                target: tracing_targets::COLLATOR,
                "Collator ({}): there are unprocessed internals from previous block, will collate next block",
                self.collator_descr(),
            );
        }

        // check pending externals
        let mut has_externals = true;
        if !has_internals {
            has_externals = self.has_pending_externals;
            if has_externals {
                tracing::debug!(
                    target: tracing_targets::COLLATOR,
                    "Collator ({}): there are pending externals from previously imported anchors, will collate next block",
                    self.collator_descr(),
                );
            }
        };

        // import next anchor if no internals and no pending externals for collation
        // otherwise it will be imported during collation when the parallel slot is free
        // or may be imported at the end of collation to update chain time
        let next_anchor = if !has_internals && !has_externals {
            tracing::debug!(
                target: tracing_targets::COLLATOR,
                "Collator ({}): there are no internals or pending externals, will import next anchor",
                self.collator_descr(),
            );
            let next_anchor = self.import_next_anchor().await?;
            has_externals = next_anchor.has_externals();
            if has_externals {
                tracing::debug!(
                    target: tracing_targets::COLLATOR,
                    "Collator ({}): just imported anchor has externals, will collate next block",
                    self.collator_descr(),
                );
            }
            Some(next_anchor)
        } else {
            None
        };

        // queue collation if has internals or externals
        if has_internals || has_externals {
            let next_chain_time = self.get_last_imported_anchor_chain_time();
            self.dispatcher
                .enqueue_task(method_to_async_task_closure!(
                    do_collate,
                    next_chain_time,
                    vec![]
                ))
                .await?;
            tracing::debug!(
                target: tracing_targets::COLLATOR,
                "Collator ({}): block collation task enqueued",
                self.collator_descr(),
            );
        } else {
            // notify manager when next anchor was imported but id does not contain externals
            if let Some(anchor) = next_anchor {
                // this will initiate master block collation or next shard block collation attempt
                tracing::debug!(
                    target: tracing_targets::COLLATOR,
                    "Collator ({}): just imported anchor has no externals, will notify collation manager",
                    self.collator_descr(),
                );
                self.listener
                    .on_skipped_empty_anchor(self.shard_id, anchor)
                    .await?;
            } else {
                // otherwise enqueue next shard block collation attempt right now
                self.dispatcher
                    .enqueue_task(method_to_async_task_closure!(try_collate_next_shard_block,))
                    .await?;
            }
        }

        Ok(())
    }

    async fn do_collate(
        &mut self,
        next_chain_time: u64,
        top_shard_blocks_info: Vec<(BlockId, BlockInfo, ValueFlow)>,
    ) -> Result<()> {
        self.do_collate_impl(next_chain_time, top_shard_blocks_info)
            .await
    }
}

pub(crate) struct CollatorProcessorStdImpl<MQ, MP, ST> {
    collator_descr: Arc<String>,

    config: Arc<CollationConfig>,
    collation_session: Arc<CollationSessionInfo>,

    dispatcher: Arc<AsyncQueuedDispatcher<Self, ()>>,
    listener: Arc<dyn CollatorEventListener>,
    mq_adapter: Arc<MQ>,
    mpool_adapter: Arc<MP>,
    state_node_adapter: Arc<ST>,
    shard_id: ShardIdent,
    working_state: Option<WorkingState>,

    /// The cache of imported from mempool anchors that were not processed yet.
    /// Anchor is removed from the cache when all its externals are processed.
    anchors_cache: BTreeMap<MempoolAnchorId, Arc<MempoolAnchor>>,

    last_imported_anchor_id: Option<MempoolAnchorId>,
    last_imported_anchor_chain_time: Option<u64>,

    /// Pointers that show what externals were read from anchors in the cache before
    /// committingthe `externals_processed_upto` on block candidate finalization.
    ///
    /// Updated in the `get_next_external()` method
    externals_read_upto: BTreeMap<MempoolAnchorId, usize>,
    /// TRUE - when exist imported anchors in cache and not all their externals were processed.
    ///
    /// Updated in the `get_next_external()` method
    has_pending_externals: bool,

    /// State tracker for creating ShardStateStuff locally
    state_tracker: Arc<MinRefMcStateTracker>,
}

impl<MQ, MP, ST> CollatorProcessorStdImpl<MQ, MP, ST>
where
    MQ: MessageQueueAdapter,
    MP: MempoolAdapter,
    ST: StateNodeAdapter,
{
    fn collator_descr(&self) -> &str {
        &self.collator_descr
    }

    fn working_state(&self) -> &WorkingState {
        self.working_state
            .as_ref()
            .expect("should `init` collator before calling `working_state`")
    }
    fn set_working_state(&mut self, working_state: WorkingState) {
        self.working_state = Some(working_state);
    }

    /// Update working state from new block and state after block collation
    fn update_working_state(&mut self, new_state_stuff: Arc<ShardStateStuff>) -> Result<()> {
        let new_next_block_id_short = BlockIdShort {
            shard: new_state_stuff.block_id().shard,
            seqno: new_state_stuff.block_id().seqno + 1,
        };
        let new_collator_descr = format!("next block: {}", new_next_block_id_short);

        let working_state_mut = self
            .working_state
            .as_mut()
            .expect("should `init` collator before calling `update_working_state`");

        if new_state_stuff.block_id().shard.is_masterchain() {
            let new_mc_data = McData::build(new_state_stuff.clone())?;
            working_state_mut.mc_data = new_mc_data;
        }

        let prev_states = vec![new_state_stuff];
        Self::check_prev_states_and_master(&working_state_mut.mc_data, &prev_states)?;
        let (new_prev_shard_data, usage_tree) = PrevData::build(prev_states)?;
        working_state_mut.prev_shard_data = new_prev_shard_data;
        working_state_mut.usage_tree = usage_tree;

        tracing::debug!(
            target: tracing_targets::COLLATOR,
            "Collator ({}): working state updated from just collated block",
            self.collator_descr(),
        );

        self.collator_descr = Arc::new(new_collator_descr);

        Ok(())
    }

    /// Update McData in working state
    fn update_mc_data(&mut self, mc_state: Arc<ShardStateStuff>) -> Result<()> {
        let mc_state_block_id = mc_state.block_id().as_short_id();

        let new_mc_data = McData::build(mc_state)?;

        let working_state_mut = self
            .working_state
            .as_mut()
            .expect("should `init` collator before calling `update_mc_data`");

        working_state_mut.mc_data = new_mc_data;

        tracing::debug!(
            target: tracing_targets::COLLATOR,
            "Collator ({}): McData updated in working state from new master state on {}",
            self.collator_descr(),
            mc_state_block_id,
        );

        Ok(())
    }

    /// Load required initial states:
    /// master state + list of previous shard states
    async fn load_init_states(
        state_node_adapter: Arc<ST>,
        shard_id: ShardIdent,
        prev_blocks_ids: Vec<BlockId>,
        mc_state: Arc<ShardStateStuff>,
    ) -> Result<(Arc<ShardStateStuff>, Vec<Arc<ShardStateStuff>>)> {
        // if current shard is a masterchain then can take current master state
        if shard_id.is_masterchain() {
            return Ok((mc_state.clone(), vec![mc_state]));
        }

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

        Ok((mc_state, prev_states))
    }

    /// Build working state structure:
    /// * master state
    /// * observable previous state
    /// * usage tree that tracks data access to state cells
    ///
    /// Perform some validations on state
    fn build_and_validate_working_state(
        mc_state: Arc<ShardStateStuff>,
        prev_states: Vec<Arc<ShardStateStuff>>,
        prev_blocks_ids: Vec<BlockId>,
    ) -> Result<WorkingState> {
        //TODO: make real implementation

        let mc_data = McData::build(mc_state)?;
        Self::check_prev_states_and_master(&mc_data, &prev_states)?;
        let (prev_shard_data, usage_tree) = PrevData::build(prev_states)?;

        let working_state = WorkingState {
            mc_data,
            prev_shard_data,
            usage_tree,
        };

        Ok(working_state)
    }

    /// (TODO) Perform some checks on master state and prev states
    fn check_prev_states_and_master(
        _mc_data: &McData,
        _prev_states: &[Arc<ShardStateStuff>],
    ) -> Result<()> {
        //TODO: make real implementation
        // refer to the old node impl:
        //  Collator::unpack_last_state()
        Ok(())
    }

    /// 1. (TODO) Get last imported anchor from cache or last processed from `externals_processed_upto`
    /// 2. Await next anchor via mempool adapter
    /// 3. Store anchor in cache and return it
    async fn import_next_anchor(&mut self) -> Result<Arc<MempoolAnchor>> {
        //TODO: make real implementation

        //STUB: take 0 as last imported without checking `externals_processed_upto`
        let prev_anchor_id = self.last_imported_anchor_id.unwrap_or(0);

        let next_anchor = self.mpool_adapter.get_next_anchor(prev_anchor_id).await?;
        tracing::debug!(
            target: tracing_targets::COLLATOR,
            "Collator ({}): imported next anchor (id: {}, chain_time: {}, externals: {})",
            self.collator_descr(),
            next_anchor.id(),
            next_anchor.chain_time(),
            next_anchor.externals_count(),
        );

        self.last_imported_anchor_id = Some(next_anchor.id());
        self.last_imported_anchor_chain_time = Some(next_anchor.chain_time());
        self.anchors_cache
            .insert(next_anchor.id(), next_anchor.clone());

        if next_anchor.has_externals() {
            self.has_pending_externals = true;
        }

        Ok(next_anchor)
    }

    fn get_last_imported_anchor_chain_time(&self) -> u64 {
        self.last_imported_anchor_chain_time.unwrap()
    }

    /// (TODO) Should consider parallel processing for different accounts
    fn get_next_external(&mut self) -> Option<Arc<OwnedMessage>> {
        //TODO: make real implementation

        //STUB: just remove first anchor from cache to force next anchor import on `try_collate` run
        self.anchors_cache.pop_first();

        None
    }

    /// (TODO) TRUE - when internal messages queue has internals
    fn has_internals(&self) -> Result<bool> {
        //TODO: make real implementation
        //STUB: always return false emulating that all internals were processed in prev block
        Ok(false)
    }
}
