use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use everscale_types::models::{BlockId, ShardIdent};

use crate::{
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
pub trait CollatorEventEmitter {
    /// When new shard or master block was collated
    async fn on_block_candidate_event(&self, collation_result: BlockCollationResult) -> Result<()>;
    /// When collator was stopped
    async fn on_collator_stopped_event(&self, stop_key: CollationSessionId) -> Result<()>;
}

#[async_trait]
pub trait CollatorEventListener: Send + Sync {
    /// Process new collated shard or master block
    async fn on_block_candidate(&self, collation_result: BlockCollationResult) -> Result<()>;
    /// Process collator stopped event
    async fn on_collator_stopped(&self, stop_key: CollationSessionId) -> Result<()>;
}

// COLLATOR

#[async_trait]
pub trait Collator<MQ, ST>: Send + Sync + 'static {
    /// Create collator, start its tasks queue, and equeue first initialization task
    fn start(
        listener: Arc<dyn CollatorEventListener>,
        mq_adapter: Arc<MQ>,
        state_node_adapter: Arc<ST>,
        shard_id: ShardIdent,
        prev_block_ids: Vec<BlockId>,
    ) -> Self;
    /// Enqueue collator stop task
    async fn equeue_stop(&self, stop_key: CollationSessionId) -> Result<()>;
    /// Produce new block, return created block + updated shard state, and update working state
    async fn collate() -> Result<BlockCollationResult>;
}

pub(crate) struct CollatorStdImpl<W, MQ, ST>
where
    W: CollatorProcessor<MQ, ST>,
{
    _marker_mq_adapter: std::marker::PhantomData<MQ>,
    _marker_state_node_adapter: std::marker::PhantomData<ST>,

    dispatcher: Arc<AsyncQueuedDispatcher<W, ()>>,
}

#[async_trait]
impl<W, MQ, ST> Collator<MQ, ST> for CollatorStdImpl<W, MQ, ST>
where
    W: CollatorProcessor<MQ, ST>,
    MQ: MessageQueueAdapter,
    ST: StateNodeAdapter,
{
    fn start(
        listener: Arc<dyn CollatorEventListener>,
        mq_adapter: Arc<MQ>,
        state_node_adapter: Arc<ST>,
        shard_id: ShardIdent,
        prev_block_ids: Vec<BlockId>,
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
            state_node_adapter,
            shard_id,
        );
        AsyncQueuedDispatcher::run(processor, receiver);

        // create instance
        let res = Self {
            _marker_mq_adapter: std::marker::PhantomData,
            _marker_state_node_adapter: std::marker::PhantomData,
            dispatcher: dispatcher.clone(),
        };

        // equeue first initialization task
        // sending to the receiver here cannot return Error because it is guaranteed not closed or dropped
        dispatcher
            .enqueue_task_blocking(method_to_async_task_closure!(init, prev_block_ids))
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
