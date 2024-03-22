use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use everscale_types::models::{BlockId, ShardIdent};
use tycho_block_util::state::ShardStateStuff;

use crate::{
    mempool::{MempoolAdapter, MempoolAnchor},
    method_to_async_task_closure,
    msg_queue::MessageQueueAdapter,
    state_node::StateNodeAdapter,
    types::{BlockCollationResult, CollationSessionId},
    utils::async_queued_dispatcher::{
        AsyncQueuedDispatcher, STANDARD_DISPATCHER_QUEUE_BUFFER_SIZE,
    },
};

use super::collator_processor::CollatorProcessor;

// EVENTS EMITTER AMD LISTENER

#[async_trait]
pub(crate) trait CollatorEventEmitter {
    /// When there are no internals and an empty anchor was received from mempool
    /// collator skips such anchor and notify listener. Manager may schedule
    /// a master block collation when the corresponding interval elapsed
    async fn on_skipped_empty_anchor_event(
        &self,
        shard_id: ShardIdent,
        anchor: Arc<MempoolAnchor>,
    ) -> Result<()>;
    /// When new shard or master block was collated
    async fn on_block_candidate_event(&self, collation_result: BlockCollationResult) -> Result<()>;
    /// When collator was stopped
    async fn on_collator_stopped_event(&self, stop_key: CollationSessionId) -> Result<()>;
}

#[async_trait]
pub(crate) trait CollatorEventListener: Send + Sync {
    /// Process empty anchor that was skipped without shard block collation
    async fn on_skipped_empty_anchor(
        &self,
        shard_id: ShardIdent,
        anchor: Arc<MempoolAnchor>,
    ) -> Result<()>;
    /// Process new collated shard or master block
    async fn on_block_candidate(&self, collation_result: BlockCollationResult) -> Result<()>;
    /// Process collator stopped event
    async fn on_collator_stopped(&self, stop_key: CollationSessionId) -> Result<()>;
}

// COLLATOR

#[async_trait]
pub(crate) trait Collator<MQ, MP, ST>: Send + Sync + 'static {
    /// Create collator, start its tasks queue, and equeue first initialization task
    fn start(
        listener: Arc<dyn CollatorEventListener>,
        mq_adapter: Arc<MQ>,
        mpool_adapter: Arc<MP>,
        state_node_adapter: Arc<ST>,
        shard_id: ShardIdent,
        prev_block_ids: Vec<BlockId>,
        mc_state: Arc<ShardStateStuff>,
    ) -> Self;
    /// Enqueue collator stop task
    async fn equeue_stop(&self, stop_key: CollationSessionId) -> Result<()>;
    /// Produce new block, return created block + updated shard state, and update working state
    async fn collate() -> Result<BlockCollationResult>;
}

#[allow(private_bounds)]
pub(crate) struct CollatorStdImpl<W, MQ, MP, ST>
where
    W: CollatorProcessor<MQ, MP, ST>,
{
    _marker_mq_adapter: std::marker::PhantomData<MQ>,
    _marker_mpool_adapter: std::marker::PhantomData<MP>,
    _marker_state_node_adapter: std::marker::PhantomData<ST>,

    dispatcher: Arc<AsyncQueuedDispatcher<W, ()>>,
}

#[async_trait]
impl<W, MQ, MP, ST> Collator<MQ, MP, ST> for CollatorStdImpl<W, MQ, MP, ST>
where
    W: CollatorProcessor<MQ, MP, ST>,
    MQ: MessageQueueAdapter,
    MP: MempoolAdapter,
    ST: StateNodeAdapter,
{
    fn start(
        listener: Arc<dyn CollatorEventListener>,
        mq_adapter: Arc<MQ>,
        mpool_adapter: Arc<MP>,
        state_node_adapter: Arc<ST>,
        shard_id: ShardIdent,
        prev_block_ids: Vec<BlockId>,
        mc_state: Arc<ShardStateStuff>,
    ) -> Self {
        // create dispatcher for own async tasks queue
        let (dispatcher, receiver) =
            AsyncQueuedDispatcher::new(STANDARD_DISPATCHER_QUEUE_BUFFER_SIZE);
        let dispatcher = Arc::new(dispatcher);

        // create processor and run dispatcher for own tasks queue
        let processor = W::new(
            dispatcher.clone(),
            listener,
            mq_adapter,
            mpool_adapter,
            state_node_adapter,
            shard_id,
        );
        AsyncQueuedDispatcher::run(processor, receiver);

        // create instance
        let res = Self {
            _marker_mq_adapter: std::marker::PhantomData,
            _marker_mpool_adapter: std::marker::PhantomData,
            _marker_state_node_adapter: std::marker::PhantomData,
            dispatcher: dispatcher.clone(),
        };

        // equeue first initialization task
        // sending to the receiver here cannot return Error because it is guaranteed not closed or dropped
        dispatcher
            .enqueue_task_blocking(method_to_async_task_closure!(
                init,
                prev_block_ids,
                mc_state
            ))
            .expect("task receiver had to be not closed or dropped");

        res
    }

    async fn equeue_stop(&self, stop_key: CollationSessionId) -> Result<()> {
        todo!()
    }

    async fn collate() -> Result<BlockCollationResult> {
        todo!()
    }
}
