use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use everscale_types::models::{BlockId, BlockIdShort, OwnedMessage, ShardIdent};

use tycho_block_util::state::ShardStateStuff;
use tycho_core::internal_queue::types::ext_types_stubs::EnqueuedMessage;
use tycho_core::internal_queue::types::QueueDiff;

use crate::mempool::{MempoolAnchor, MempoolAnchorId};
use crate::msg_queue::{IterItem, QueueIterator};
use crate::tracing_targets;
use crate::{
    mempool::MempoolAdapter,
    method_to_async_task_closure,
    msg_queue::MessageQueueAdapter,
    state_node::StateNodeAdapter,
    types::{BlockCollationResult, CollationSessionId},
    utils::async_queued_dispatcher::AsyncQueuedDispatcher,
};

use super::types::{McData, PrevData};
use super::{
    do_collate::DoCollate, types::WorkingState, CollatorEventEmitter, CollatorEventListener,
};

// COLLATOR PROCESSOR

#[async_trait]
pub(super) trait CollatorProcessor<MQ, MP, ST>: DoCollate<MQ, MP, ST>
where
    ST: StateNodeAdapter,
{
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
            self.get_state_node_adapter(),
            *self.shard_id(),
            prev_blocks_ids.clone(),
            mc_state,
        )
        .await?;

        // build, validate and set working state
        tracing::info!(target: tracing_targets::COLLATOR, "Collator init ({}): building working state...", self.collator_descr());
        let working_state =
            Self::build_and_validate_working_state(mc_state, prev_states, prev_blocks_ids.clone())?;
        self.set_working_state(working_state);

        //TODO: fix work with internals, currently do not init mq iterator because do not need to integrate mq
        // init message queue iterator
        //self.init_mq_iterator().await?;

        // master block collations will be called by the collation manager directly

        // enqueue collation attempt of next shard block
        if !self.shard_id().is_masterchain() {
            self.get_dispatcher()
                .enqueue_task(method_to_async_task_closure!(try_collate,))
                .await?;
            tracing::info!(target: tracing_targets::COLLATOR, "Collator init ({}): collation attempt enqueued", self.collator_descr());
        }

        tracing::info!(target: tracing_targets::COLLATOR, "Collator init ({}): finished", self.collator_descr());

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
            let state = state_node_adapter
                .request_state(prev_block_id)
                .await?
                .try_recv()
                .await?;
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

        let mc_data = McData::new(mc_state)?;
        let (prev_shard_data, usage_tree) =
            PrevData::build(&mc_data, &prev_states, prev_blocks_ids)?;

        let working_state = WorkingState {
            mc_data,
            prev_shard_data,
            usage_tree,
        };

        Ok(working_state)
    }

    /// Attempt to collate next shard block
    /// 1. Run collation if there are internals or pending externals from previously imported anchors
    /// 2. Otherwise request next anchor with externals
    /// 3. If no internals or externals then notify manager about skipped empty anchor
    async fn try_collate(&mut self) -> Result<()> {
        tracing::trace!(
            target: tracing_targets::COLLATOR,
            "Collator ({}): checking if can collate next block",
            self.collator_descr(),
        );

        //TODO: fix the work with internals

        // check internals
        let has_internals = self.mq_iterator_has_next();
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
            has_externals = self.has_pending_externals();
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
            self.get_dispatcher()
                .enqueue_task(method_to_async_task_closure!(do_collate,))
                .await?;
            tracing::debug!(
                target: tracing_targets::COLLATOR,
                "Collator ({}): block collation task enqueued",
                self.collator_descr(),
            );
        } else {
            // notify manager when next anchor was imported but id does not contain externals
            if let Some(anchor) = next_anchor {
                tracing::debug!(
                    target: tracing_targets::COLLATOR,
                    "Collator ({}): just imported anchor has no externals, will notify collation manager",
                    self.collator_descr(),
                );
                self.on_skipped_empty_anchor_event(*self.shard_id(), anchor)
                    .await?;
            }
        }

        // finally enqueue next collation attempt
        // which will be processed right after current one
        // or after previously scheduled collation
        self.get_dispatcher()
            .enqueue_task(method_to_async_task_closure!(try_collate,))
            .await
    }
}

#[async_trait]
impl<MQ, QI, MP, ST> CollatorProcessor<MQ, MP, ST> for CollatorProcessorStdImpl<MQ, QI, MP, ST>
where
    MQ: MessageQueueAdapter,
    QI: QueueIterator + Send + Sync + 'static,
    MP: MempoolAdapter,
    ST: StateNodeAdapter,
{
}

/// Trait declares functions that need specific implementation.
/// For test purposes you can re-implement only this trait.
#[async_trait]
pub(super) trait CollatorProcessorSpecific<MQ, MP, ST>: Sized {
    fn new(
        collator_descr: Arc<String>,
        dispatcher: Arc<AsyncQueuedDispatcher<Self, ()>>,
        listener: Arc<dyn CollatorEventListener>,
        mq_adapter: Arc<MQ>,
        mpool_adapter: Arc<MP>,
        state_node_adapter: Arc<ST>,
        shard_id: ShardIdent,
    ) -> Self;

    fn collator_descr(&self) -> &str;

    fn shard_id(&self) -> &ShardIdent;

    fn get_dispatcher(&self) -> Arc<AsyncQueuedDispatcher<Self, ()>>;

    fn get_mq_adapter(&self) -> Arc<MQ>;

    fn get_state_node_adapter(&self) -> Arc<ST>;

    fn working_state(&self) -> &WorkingState;
    fn set_working_state(&mut self, working_state: WorkingState);
    fn update_working_state(&mut self, new_prev_block_id: BlockId) -> Result<()>;

    async fn init_mq_iterator(&mut self) -> Result<()>;

    fn mq_iterator_has_next(&self) -> bool;
    fn mq_iterator_next(&mut self) -> Option<IterItem>;
    fn mq_iterator_commit(&mut self);
    fn mq_iterator_get_diff(&self, block_id_short: BlockIdShort) -> QueueDiff;
    fn mq_iterator_add_message(&mut self, message: Arc<EnqueuedMessage>) -> Result<()>;

    async fn import_next_anchor(&mut self) -> Result<Arc<MempoolAnchor>>;
    fn last_imported_anchor_id(&self) -> Option<&MempoolAnchorId>;
    fn get_last_imported_anchor_chain_time(&self) -> u64;

    /// TRUE - when exist imported anchors in cache and not all their externals were processed
    fn has_pending_externals(&self) -> bool;
    fn set_has_pending_externals(&mut self, value: bool);

    /// (TODO) Should consider parallel processing for different accounts
    fn get_next_external(&mut self) -> Option<Arc<OwnedMessage>>;
}

pub(crate) struct CollatorProcessorStdImpl<MQ, QI, MP, ST> {
    collator_descr: Arc<String>,
    dispatcher: Arc<AsyncQueuedDispatcher<Self, ()>>,
    listener: Arc<dyn CollatorEventListener>,
    mq_adapter: Arc<MQ>,
    mq_iterator: Option<QI>,
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
}

#[async_trait]
impl<MQ, QI, MP, ST> CollatorProcessorSpecific<MQ, MP, ST>
    for CollatorProcessorStdImpl<MQ, QI, MP, ST>
where
    MQ: MessageQueueAdapter,
    QI: QueueIterator + Send,
    MP: MempoolAdapter,
    ST: StateNodeAdapter,
{
    fn new(
        collator_descr: Arc<String>,
        dispatcher: Arc<AsyncQueuedDispatcher<Self, ()>>,
        listener: Arc<dyn CollatorEventListener>,
        mq_adapter: Arc<MQ>,
        mpool_adapter: Arc<MP>,
        state_node_adapter: Arc<ST>,
        shard_id: ShardIdent,
    ) -> Self {
        Self {
            collator_descr,
            dispatcher,
            listener,
            mq_adapter,
            mq_iterator: None,
            mpool_adapter,
            state_node_adapter,
            shard_id,
            working_state: None,

            anchors_cache: BTreeMap::new(),
            last_imported_anchor_id: None,
            last_imported_anchor_chain_time: None,

            externals_read_upto: BTreeMap::new(),
            has_pending_externals: false,
        }
    }

    fn collator_descr(&self) -> &str {
        &self.collator_descr
    }

    fn shard_id(&self) -> &ShardIdent {
        &self.shard_id
    }

    fn get_dispatcher(&self) -> Arc<AsyncQueuedDispatcher<Self, ()>> {
        self.dispatcher.clone()
    }

    fn get_mq_adapter(&self) -> Arc<MQ> {
        self.mq_adapter.clone()
    }

    fn get_state_node_adapter(&self) -> Arc<ST> {
        self.state_node_adapter.clone()
    }

    fn working_state(&self) -> &WorkingState {
        self.working_state
            .as_ref()
            .expect("should `init` collator before calling `working_state`")
    }
    fn set_working_state(&mut self, working_state: WorkingState) {
        self.working_state = Some(working_state);
    }

    ///(TODO) Update working state from new state after block collation
    ///
    ///STUB: currently have stub signature and implementation
    fn update_working_state(&mut self, new_prev_block_id: BlockId) -> Result<()> {
        let new_next_block_id = BlockIdShort {
            shard: new_prev_block_id.shard,
            seqno: new_prev_block_id.seqno + 1,
        };
        let new_collator_descr = format!("next block: {}", new_next_block_id);

        self.working_state
            .as_mut()
            .expect("should `init` collator before calling `update_working_state`")
            .prev_shard_data
            .update_state(vec![new_prev_block_id])?;

        tracing::debug!(
            target: tracing_targets::COLLATOR,
            "Collator ({}): STUB: working state updated from just collated block...",
            self.collator_descr(),
        );

        self.collator_descr = Arc::new(new_collator_descr);

        Ok(())
    }

    async fn init_mq_iterator(&mut self) -> Result<()> {
        let mq_iterator = self.mq_adapter.get_iterator(&self.shard_id).await?;
        self.mq_iterator = Some(mq_iterator);
        Ok(())
    }

    fn mq_iterator_has_next(&self) -> bool {
        //TODO: make real implementation
        //STUB: always return false emulating that all internals were processed in prev block
        false
    }
    fn mq_iterator_next(&mut self) -> Option<IterItem> {
        todo!()
    }
    fn mq_iterator_commit(&mut self) {
        todo!()
    }
    fn mq_iterator_get_diff(&self, block_id_short: BlockIdShort) -> QueueDiff {
        todo!()
    }
    fn mq_iterator_add_message(&mut self, message: Arc<EnqueuedMessage>) -> Result<()> {
        todo!()
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

    fn last_imported_anchor_id(&self) -> Option<&MempoolAnchorId> {
        self.last_imported_anchor_id.as_ref()
    }
    fn get_last_imported_anchor_chain_time(&self) -> u64 {
        self.last_imported_anchor_chain_time.unwrap()
    }

    fn has_pending_externals(&self) -> bool {
        self.has_pending_externals
    }
    fn set_has_pending_externals(&mut self, value: bool) {
        self.has_pending_externals = value;
    }

    fn get_next_external(&mut self) -> Option<Arc<OwnedMessage>> {
        //TODO: make real implementation

        //STUB: just remove first anchor from cache to force next anchor import on `try_collate` run
        self.anchors_cache.pop_first();

        None
    }
}

#[async_trait]
impl<MQ, QI, MP, ST> CollatorEventEmitter for CollatorProcessorStdImpl<MQ, QI, MP, ST>
where
    MQ: MessageQueueAdapter,
    QI: QueueIterator + Send + Sync + 'static,
    MP: MempoolAdapter,
    ST: StateNodeAdapter,
{
    async fn on_skipped_empty_anchor_event(
        &self,
        shard_id: ShardIdent,
        anchor: Arc<MempoolAnchor>,
    ) -> Result<()> {
        self.listener
            .on_skipped_empty_anchor(shard_id, anchor)
            .await
    }
    async fn on_block_candidate_event(&self, collation_result: BlockCollationResult) -> Result<()> {
        self.listener.on_block_candidate(collation_result).await
    }
    async fn on_collator_stopped_event(&self, stop_key: CollationSessionId) -> Result<()> {
        self.listener.on_collator_stopped(stop_key).await
    }
}
