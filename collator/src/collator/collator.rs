use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use everscale_types::models::{BlockId, BlockIdShort, BlockInfo, ShardIdent, ValueFlow};
use futures_util::Future;
use tycho_block_util::state::{MinRefMcStateTracker, ShardStateStuff};

use crate::collator::collator_processor::{
    CollatorProcessor, CollatorProcessorFactory, CollatorProcessorContext,
};
use crate::{
    mempool::{MempoolAdapter, MempoolAnchor},
    method_to_async_task_closure,
    msg_queue::MessageQueueAdapter,
    state_node::StateNodeAdapter,
    tracing_targets,
    types::{BlockCollationResult, CollationConfig, CollationSessionId, CollationSessionInfo},
    utils::async_queued_dispatcher::{
        AsyncQueuedDispatcher, STANDARD_DISPATCHER_QUEUE_BUFFER_SIZE,
    },
};

// FACTORY

pub struct CollatorContext {
    pub config: Arc<CollationConfig>,
    pub collation_session: Arc<CollationSessionInfo>,
    pub listener: Arc<dyn CollatorEventListener>,
    pub shard_id: ShardIdent,
    pub prev_blocks_ids: Vec<BlockId>,
    pub mc_state: ShardStateStuff,
    pub state_tracker: MinRefMcStateTracker,
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
pub(crate) trait Collator: Send + Sync + 'static {
    /// Enqueue collator stop task
    async fn equeue_stop(&self, stop_key: CollationSessionId) -> Result<()>;
    /// Enqueue update of McData in working state and run attempt to collate shard block
    async fn equeue_update_mc_data_and_resume_shard_collation(
        &self,
        mc_state: ShardStateStuff,
    ) -> Result<()>;
    /// Enqueue next attemt to collate block
    async fn equeue_try_collate(&self) -> Result<()>;
    /// Enqueue new block collation
    async fn equeue_do_collate(
        &self,
        next_chain_time: u64,
        top_shard_blocks_info: Vec<(BlockId, BlockInfo, ValueFlow)>,
    ) -> Result<()>;
}

pub struct CollatorStdFactory<MQ, MP, ST, CPF> {
    pub mq_adapter: Arc<MQ>,
    pub mpool_adapter: Arc<MP>,
    pub state_node_adapter: Arc<ST>,
    pub collator_processor_factory: CPF,
}

#[async_trait]
impl<MQ, MP, ST, CPF> CollatorFactory for CollatorStdFactory<MQ, MP, ST, CPF>
where
    MQ: MessageQueueAdapter,
    MP: MempoolAdapter,
    ST: StateNodeAdapter,
    CPF: CollatorProcessorFactory,
{
    type Collator = CollatorStdImpl<CPF::CollatorProcessor>;

    async fn start(&self, cx: CollatorContext) -> Self::Collator {
        let max_prev_seqno = cx.prev_blocks_ids.iter().map(|id| id.seqno).max().unwrap();
        let next_block_id = BlockIdShort {
            shard: cx.shard_id,
            seqno: max_prev_seqno + 1,
        };
        let collator_descr = Arc::new(format!("next block: {}", next_block_id));
        tracing::info!(target: tracing_targets::COLLATOR, "Collator ({}) starting...", collator_descr);

        // create dispatcher for own async tasks queue
        let (dispatcher, receiver) =
            AsyncQueuedDispatcher::new(STANDARD_DISPATCHER_QUEUE_BUFFER_SIZE);
        let dispatcher = Arc::new(dispatcher);

        // create processor and run dispatcher for own tasks queue
        let processor = self
            .collator_processor_factory
            .build(CollatorProcessorContext {
                collator_descr,
                config: cx.config,
                collation_session: cx.collation_session,
                dispatcher,
                listener: cx.listener,
                shard_id: cx.shard_id,
                state_tracker: cx.state_tracker,
            });

        AsyncQueuedDispatcher::run(processor, receiver);
        tracing::trace!(target: tracing_targets::COLLATOR, "Tasks queue dispatcher started");

        // create instance
        let res = CollatorStdImpl {
            collator_descr,
            dispatcher: dispatcher.clone(),
        };

        // equeue first initialization task
        // sending to the receiver here cannot return Error because it is guaranteed not closed or dropped
        dispatcher
            .enqueue_task(method_to_async_task_closure!(
                init,
                cx.prev_blocks_ids,
                cx.mc_state
            ))
            .await
            .expect("task receiver had to be not closed or dropped here");
        tracing::info!(target: tracing_targets::COLLATOR, "Collator ({}) initialization task enqueued", res.collator_descr);

        tracing::info!(target: tracing_targets::COLLATOR, "Collator ({}) started", res.collator_descr);

        res
    }
}

#[allow(private_bounds)]
pub(crate) struct CollatorStdImpl<W> {
    collator_descr: Arc<String>,
    dispatcher: Arc<AsyncQueuedDispatcher<W, ()>>,
}

#[async_trait]
impl<W: CollatorProcessor> Collator for CollatorStdImpl<W> {
    async fn equeue_stop(&self, _stop_key: CollationSessionId) -> Result<()> {
        todo!()
    }

    async fn equeue_update_mc_data_and_resume_shard_collation(
        &self,
        mc_state: ShardStateStuff,
    ) -> Result<()> {
        self.dispatcher
            .enqueue_task(method_to_async_task_closure!(
                update_mc_data_and_resume_collation,
                mc_state
            ))
            .await
    }

    async fn equeue_try_collate(&self) -> Result<()> {
        self.dispatcher
            .enqueue_task(method_to_async_task_closure!(try_collate_next_shard_block,))
            .await
    }

    async fn equeue_do_collate(
        &self,
        next_chain_time: u64,
        top_shard_blocks_info: Vec<(BlockId, BlockInfo, ValueFlow)>,
    ) -> Result<()> {
        self.dispatcher
            .enqueue_task(method_to_async_task_closure!(
                do_collate,
                next_chain_time,
                top_shard_blocks_info
            ))
            .await
    }
}
