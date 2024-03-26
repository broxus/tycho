use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use everscale_types::models::{BlockId, ShardIdent};

use crate::{
    msg_queue::MessageQueueAdapter,
    state_node::StateNodeAdapter,
    types::{BlockCollationResult, CollationSessionId},
    utils::async_queued_dispatcher::AsyncQueuedDispatcher,
};

use super::{types::WorkingState, CollatorEventEmitter, CollatorEventListener};

// COLLATOR PROCESSOR

/// Trait declares functions that need specific implementation.
/// For test purposes you can re-implement only this trait.
#[async_trait]
pub(super) trait CollatorProcessorSpecific<MQ, ST>: Sized {
    fn new(
        dispatcher: Arc<AsyncQueuedDispatcher<Self, ()>>,
        listener: Arc<dyn CollatorEventListener>,
        mq_adapter: Arc<MQ>,
        state_node_adapter: Arc<ST>,
        shard_id: ShardIdent,
    ) -> Self;
    async fn init(&mut self, prev_blocks_ids: Vec<BlockId>) -> Result<()>;
}

#[async_trait]
pub(super) trait CollatorProcessor<MQ, ST>:
    CollatorProcessorSpecific<MQ, ST> + CollatorEventEmitter + Sized + Send + Sync + 'static
{
}

pub(crate) struct CollatorProcessorStdImpl<MQ, ST> {
    dispatcher: Arc<AsyncQueuedDispatcher<Self, ()>>,
    listener: Arc<dyn CollatorEventListener>,
    mq_adapter: Arc<MQ>,
    state_node_adapter: Arc<ST>,
    shard_id: ShardIdent,
    working_state: Option<WorkingState>,
}

#[async_trait]
impl<MQ, ST> CollatorEventEmitter for CollatorProcessorStdImpl<MQ, ST>
where
    MQ: MessageQueueAdapter,
    ST: StateNodeAdapter,
{
    async fn on_block_candidate_event(&self, collation_result: BlockCollationResult) -> Result<()> {
        self.listener.on_block_candidate(collation_result).await
    }
    async fn on_collator_stopped_event(&self, stop_key: CollationSessionId) -> Result<()> {
        self.listener.on_collator_stopped(stop_key).await
    }
}

#[async_trait]
impl<MQ, ST> CollatorProcessorSpecific<MQ, ST> for CollatorProcessorStdImpl<MQ, ST>
where
    MQ: MessageQueueAdapter,
    ST: StateNodeAdapter,
{
    fn new(
        dispatcher: Arc<AsyncQueuedDispatcher<Self, ()>>,
        listener: Arc<dyn CollatorEventListener>,
        mq_adapter: Arc<MQ>,
        state_node_adapter: Arc<ST>,
        shard_id: ShardIdent,
    ) -> Self {
        Self {
            dispatcher,
            listener,
            mq_adapter,
            state_node_adapter,
            shard_id,
            working_state: None,
        }
    }

    async fn init(&mut self, prev_blocks_ids: Vec<BlockId>) -> Result<()> {
        todo!()
    }
}

#[async_trait]
impl<MQ, ST> CollatorProcessor<MQ, ST> for CollatorProcessorStdImpl<MQ, ST>
where
    MQ: MessageQueueAdapter,
    ST: StateNodeAdapter,
{
}
