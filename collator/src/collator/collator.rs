use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use everscale_types::models::{BlockId, BlockIdShort, ShardIdent};
use tycho_block_util::state::ShardStateStuff;

use crate::{
    mempool::{MempoolAdapter, MempoolAnchor},
    method_to_async_task_closure,
    msg_queue::MessageQueueAdapter,
    state_node::StateNodeAdapter,
    tracing_targets,
    types::{BlockCollationResult, CollationSessionId},
    utils::async_queued_dispatcher::{
        AsyncQueuedDispatcher, STANDARD_DISPATCHER_QUEUE_BUFFER_SIZE,
    },
};

use super::collator_processor::CollatorProcessor;

// EVENTS EMITTER AMD LISTENER

//TODO: remove emitter
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
    //TODO: use factory that takes CollationManager and creates Collator impl
    /// Create collator, start its tasks queue, and equeue first initialization task
    async fn start(
        listener: Arc<dyn CollatorEventListener>,
        mq_adapter: Arc<MQ>,
        mpool_adapter: Arc<MP>,
        state_node_adapter: Arc<ST>,
        shard_id: ShardIdent,
        prev_blocks_ids: Vec<BlockId>,
        mc_state: Arc<ShardStateStuff>,
    ) -> Self;
    /// Enqueue collator stop task
    async fn equeue_stop(&self, stop_key: CollationSessionId) -> Result<()>;
    /// Enqueue new block collation
    async fn equeue_do_collate(
        &self,
        next_chain_time: u64,
        top_shard_blocks_ids: Vec<BlockId>,
    ) -> Result<()>;
}

#[allow(private_bounds)]
pub(crate) struct CollatorStdImpl<W, MQ, MP, ST>
where
    W: CollatorProcessor<MQ, MP, ST>,
    ST: StateNodeAdapter,
{
    collator_descr: Arc<String>,

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
    async fn start(
        listener: Arc<dyn CollatorEventListener>,
        mq_adapter: Arc<MQ>,
        mpool_adapter: Arc<MP>,
        state_node_adapter: Arc<ST>,
        shard_id: ShardIdent,
        prev_blocks_ids: Vec<BlockId>,
        mc_state: Arc<ShardStateStuff>,
    ) -> Self {
        let max_prev_seqno = prev_blocks_ids.iter().map(|id| id.seqno).max().unwrap();
        let next_block_id = BlockIdShort {
            shard: shard_id,
            seqno: max_prev_seqno + 1,
        };
        let collator_descr = Arc::new(format!("next block: {}", next_block_id));
        tracing::info!(target: tracing_targets::COLLATOR, "Collator ({}) starting...", collator_descr);

        // create dispatcher for own async tasks queue
        let (dispatcher, receiver) =
            AsyncQueuedDispatcher::new(STANDARD_DISPATCHER_QUEUE_BUFFER_SIZE);
        let dispatcher = Arc::new(dispatcher);

        // create processor and run dispatcher for own tasks queue
        let processor = W::new(
            collator_descr.clone(),
            dispatcher.clone(),
            listener,
            mq_adapter,
            mpool_adapter,
            state_node_adapter,
            shard_id,
        );
        AsyncQueuedDispatcher::run(processor, receiver);
        tracing::trace!(target: tracing_targets::COLLATOR, "Tasks queue dispatcher started");

        // create instance
        let res = Self {
            collator_descr,
            _marker_mq_adapter: std::marker::PhantomData,
            _marker_mpool_adapter: std::marker::PhantomData,
            _marker_state_node_adapter: std::marker::PhantomData,
            dispatcher: dispatcher.clone(),
        };

        // equeue first initialization task
        // sending to the receiver here cannot return Error because it is guaranteed not closed or dropped
        dispatcher
            .enqueue_task(method_to_async_task_closure!(
                init,
                prev_blocks_ids,
                mc_state
            ))
            .await
            .expect("task receiver had to be not closed or dropped here");
        tracing::info!(target: tracing_targets::COLLATOR, "Collator ({}) initialization task enqueued", res.collator_descr);

        tracing::info!(target: tracing_targets::COLLATOR, "Collator ({}) started", res.collator_descr);

        res
    }

    async fn equeue_stop(&self, _stop_key: CollationSessionId) -> Result<()> {
        todo!()
    }

    async fn equeue_do_collate(
        &self,
        next_chain_time: u64,
        top_shard_blocks_ids: Vec<BlockId>,
    ) -> Result<()> {
        self.dispatcher
            .enqueue_task(method_to_async_task_closure!(
                do_collate,
                next_chain_time,
                top_shard_blocks_ids
            ))
            .await
    }
}
