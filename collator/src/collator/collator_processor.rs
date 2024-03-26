use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use everscale_types::models::{BlockId, BlockIdShort, ShardIdent};

use tycho_block_util::state::ShardStateStuff;
use tycho_core::internal_queue::types::ext_types_stubs::EnqueuedMessage;
use tycho_core::internal_queue::types::QueueDiff;

use crate::mempool::MempoolAnchor;
use crate::msg_queue::{IterItem, QueueIterator};
use crate::{
    mempool::MempoolAdapter,
    method_to_async_task_closure,
    msg_queue::MessageQueueAdapter,
    state_node::StateNodeAdapter,
    types::{BlockCollationResult, CollationSessionId},
    utils::async_queued_dispatcher::AsyncQueuedDispatcher,
};

use super::{
    do_collate::DoCollate, types::WorkingState, CollatorEventEmitter, CollatorEventListener,
};

// COLLATOR PROCESSOR

/// Trait declares functions that need specific implementation.
/// For test purposes you can re-implement only this trait.
#[async_trait]
pub(super) trait CollatorProcessorSpecific<MQ, MP, ST>: Sized {
    fn new(
        dispatcher: Arc<AsyncQueuedDispatcher<Self, ()>>,
        listener: Arc<dyn CollatorEventListener>,
        mq_adapter: Arc<MQ>,
        mpool_adapter: Arc<MP>,
        state_node_adapter: Arc<ST>,
        shard_id: ShardIdent,
    ) -> Self;

    fn shard_id(&self) -> &ShardIdent;

    fn get_dispatcher(&self) -> Arc<AsyncQueuedDispatcher<Self, ()>>;

    fn get_mq_adapter(&self) -> Arc<MQ>;

    fn get_state_node_adapter(&self) -> Arc<ST>;

    fn set_working_state(&mut self, working_state: WorkingState);

    async fn init_mq_iterator(&mut self) -> Result<()>;

    fn mq_iterator_has_next(&self) -> bool;
    fn mq_iterator_next(&mut self) -> Option<IterItem>;
    fn mq_iterator_commit(&mut self);
    fn mq_iterator_get_diff(&self, block_id_short: BlockIdShort) -> QueueDiff;
    fn mq_iterator_add_message(&mut self, message: Arc<EnqueuedMessage>) -> Result<()>;

    fn has_pending_externals(&self) -> bool;
}

#[async_trait]
pub(super) trait CollatorProcessor<MQ, MP, ST>: DoCollate<MQ, MP, ST> {
    // Initialize collator working state then run collation
    async fn init(
        &mut self,
        prev_blocks_ids: Vec<BlockId>,
        mc_state: Arc<ShardStateStuff>,
    ) -> Result<()>;

    // Load required initial states
    async fn load_init_states(
        state_node_adapter: Arc<ST>,
        shard_id: ShardIdent,
        prev_blocks_ids: Vec<BlockId>,
        mc_state: Arc<ShardStateStuff>,
    ) -> Result<(Arc<ShardStateStuff>, Vec<Arc<ShardStateStuff>>)>;

    fn build_and_validate_working_state(
        mc_state: Arc<ShardStateStuff>,
        prev_states: Vec<Arc<ShardStateStuff>>,
    ) -> Result<WorkingState> {
        todo!()
    }

    /// Attempt to collate next block
    /// 1. Run collation if there are internals or pending externals from previously imported anchors
    /// 2. Otherwise request next anchor with externals
    /// 3. If no internals or externals then notify manager about skipped empty anchor
    async fn try_collate(&mut self) -> Result<()> {
        // check internals
        let has_internals = self.mq_iterator_has_next();

        // check pending externals
        let mut has_externals = true;
        if !has_internals {
            has_externals = self.has_pending_externals();
        };

        // import next anchor if no internals and no pending externals for collation
        // otherwise it will be imported during collation when the parallel slot is free
        // or may be imported at the end of collation to update chain time
        let next_anchor = if !has_internals && !has_externals {
            let next_anchor = self.import_next_anchor().await?;
            has_externals = next_anchor.has_externals();
            Some(next_anchor)
        } else {
            None
        };

        // queue collation if has internals or externals
        if has_internals || has_externals {
            self.get_dispatcher()
                .enqueue_task(method_to_async_task_closure!(do_collate,))
                .await?;
        } else {
            // notify manager when next anchor was imported but id does not contain externals
            if let Some(anchor) = next_anchor {
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

    /// 1. (TODO) Get last imported anchor from cache
    /// 2. (TODO) Await next anchor via mempool adapter
    /// 3. (TODO) Store anchor in cache and return it
    async fn import_next_anchor(&mut self) -> Result<Arc<MempoolAnchor>> {
        todo!()
    }
}

pub(crate) struct CollatorProcessorStdImpl<MQ, QI, MP, ST> {
    dispatcher: Arc<AsyncQueuedDispatcher<Self, ()>>,
    listener: Arc<dyn CollatorEventListener>,
    mq_adapter: Arc<MQ>,
    mq_iterator: Option<QI>,
    mpool_adapter: Arc<MP>,
    state_node_adapter: Arc<ST>,
    shard_id: ShardIdent,
    working_state: Option<WorkingState>,
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
        dispatcher: Arc<AsyncQueuedDispatcher<Self, ()>>,
        listener: Arc<dyn CollatorEventListener>,
        mq_adapter: Arc<MQ>,
        mpool_adapter: Arc<MP>,
        state_node_adapter: Arc<ST>,
        shard_id: ShardIdent,
    ) -> Self {
        Self {
            dispatcher,
            listener,
            mq_adapter,
            mq_iterator: None,
            mpool_adapter,
            state_node_adapter,
            shard_id,
            working_state: None,
        }
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

    fn set_working_state(&mut self, working_state: WorkingState) {
        self.working_state = Some(working_state);
    }

    async fn init_mq_iterator(&mut self) -> Result<()> {
        let mq_iterator = self.mq_adapter.get_iterator(&self.shard_id).await?;
        self.mq_iterator = Some(mq_iterator);
        Ok(())
    }

    fn mq_iterator_has_next(&self) -> bool {
        todo!()
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

    fn has_pending_externals(&self) -> bool {
        todo!()
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
    async fn init(
        &mut self,
        prev_blocks_ids: Vec<BlockId>,
        mc_state: Arc<ShardStateStuff>,
    ) -> Result<()> {
        // init working state

        // load states
        let (mc_state, prev_states) = Self::load_init_states(
            self.get_state_node_adapter(),
            *self.shard_id(),
            prev_blocks_ids,
            mc_state,
        )
        .await?;

        // build, validate and set working state
        let working_state = Self::build_and_validate_working_state(mc_state, prev_states)?;
        self.set_working_state(working_state);

        // init message queue iterator
        self.init_mq_iterator().await?;

        // enqueue collation attempt
        self.get_dispatcher()
            .enqueue_task(method_to_async_task_closure!(try_collate,))
            .await
    }

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
            prev_states.push(state);
        }

        Ok((mc_state, prev_states))
    }
}
