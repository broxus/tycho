use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use everscale_types::models::{BlockId, ShardIdent};

use tycho_core::internal_queue::iterator::QueueIteratorImpl;

use crate::{
    collator::{
        collator_processor::CollatorProcessorStdImpl, Collator, CollatorEventListener,
        CollatorStdImpl,
    },
    mempool::{MempoolAdapter, MempoolAdapterBuilder, MempoolAnchor, MempoolEventListener},
    method_to_async_task_closure,
    msg_queue::{MessageQueueAdapter, MessageQueueAdapterStdImpl},
    state_node::{StateNodeAdapter, StateNodeAdapterBuilder, StateNodeEventListener},
    tracing_targets,
    types::{
        BlockCollationResult, CollationConfig, CollationSessionId, NodeNetwork,
        OnValidatedBlockEvent,
    },
    utils::{
        async_queued_dispatcher::{AsyncQueuedDispatcher, STANDARD_DISPATCHER_QUEUE_BUFFER_SIZE},
        schedule_async_action,
    },
    validator::{
        validator_processor::{ValidatorProcessor, ValidatorProcessorStdImpl},
        Validator, ValidatorEventListener, ValidatorStdImpl,
    },
};

use super::collation_processor::CollationProcessor;

/// Controls the whole collation process.
/// Monitors state sync,
/// runs collators to produce blocks,
/// executes blocks validation, sends signed blocks
/// to state node to update local sync state and broadcast.
#[allow(private_bounds)]
pub trait CollationManager<MP, ST>
where
    MP: MempoolAdapter,
    ST: StateNodeAdapter,
{
    /// Creates manager and starts all required async processes
    fn create(
        config: CollationConfig,
        mpool_adapter_builder: impl MempoolAdapterBuilder<MP> + Send,
        state_adapter_builder: impl StateNodeAdapterBuilder<ST> + Send,
        node_network: NodeNetwork,
    ) -> Self;
}

/// Generic implementation of [`CollationManager`]
pub(crate) struct CollationManagerGenImpl<C, V, MQ, MP, ST>
where
    V: Validator<ST>,
    C: Collator<MQ, MP, ST>,
    MQ: MessageQueueAdapter,
    MP: MempoolAdapter,
    ST: StateNodeAdapter,
{
    config: Arc<CollationConfig>,

    dispatcher: Arc<AsyncQueuedDispatcher<CollationProcessor<C, V, MQ, MP, ST>, ()>>,
}

#[allow(private_bounds)]
pub fn create_std_manager<MP, ST>(
    config: CollationConfig,
    mpool_adapter_builder: impl MempoolAdapterBuilder<MP> + Send,
    state_adapter_builder: impl StateNodeAdapterBuilder<ST> + Send,
    node_network: NodeNetwork,
) -> impl CollationManager<MP, ST>
where
    MP: MempoolAdapter,
    ST: StateNodeAdapter,
{
    CollationManagerGenImpl::<
        CollatorStdImpl<CollatorProcessorStdImpl<_, QueueIteratorImpl, _, _>, _, _, _>,
        ValidatorStdImpl<ValidatorProcessorStdImpl<_>, _>,
        MessageQueueAdapterStdImpl,
        MP,
        ST,
    >::create(
        config,
        mpool_adapter_builder,
        state_adapter_builder,
        node_network,
    )
}
#[allow(private_bounds)]
pub fn create_std_manager_with_validator<MP, ST, V>(
    config: CollationConfig,
    mpool_adapter_builder: impl MempoolAdapterBuilder<MP> + Send,
    state_adapter_builder: impl StateNodeAdapterBuilder<ST> + Send,
    node_network: NodeNetwork,
) -> impl CollationManager<MP, ST>
where
    MP: MempoolAdapter,
    ST: StateNodeAdapter,
    V: ValidatorProcessor<ST>,
{
    CollationManagerGenImpl::<
        CollatorStdImpl<CollatorProcessorStdImpl<_, QueueIteratorImpl, _, _>, _, _, _>,
        ValidatorStdImpl<V, _>,
        MessageQueueAdapterStdImpl,
        MP,
        ST,
    >::create(
        config,
        mpool_adapter_builder,
        state_adapter_builder,
        node_network,
    )
}

impl<C, V, MQ, MP, ST> CollationManager<MP, ST> for CollationManagerGenImpl<C, V, MQ, MP, ST>
where
    C: Collator<MQ, MP, ST>,
    V: Validator<ST>,
    MQ: MessageQueueAdapter,
    MP: MempoolAdapter,
    ST: StateNodeAdapter,
{
    fn create(
        config: CollationConfig,
        mpool_adapter_builder: impl MempoolAdapterBuilder<MP> + Send,
        state_adapter_builder: impl StateNodeAdapterBuilder<ST> + Send,
        node_network: NodeNetwork,
    ) -> Self {
        tracing::info!(target: tracing_targets::COLLATION_MANAGER, "Creating collation manager...");

        let config = Arc::new(config);

        // create dispatcher for own async tasks queue
        let (dispatcher, receiver) =
            AsyncQueuedDispatcher::new(STANDARD_DISPATCHER_QUEUE_BUFFER_SIZE);
        let dispatcher = Arc::new(dispatcher);

        // build mempool adapter
        let mpool_adapter = mpool_adapter_builder.build(dispatcher.clone());
        let mpool_adapter = Arc::new(mpool_adapter);

        // build state node adapter
        let state_node_adapter = state_adapter_builder.build(dispatcher.clone());
        let state_node_adapter = Arc::new(state_node_adapter);

        // create validator and start its tasks queue
        let validator = Arc::new(Validator::create(
            dispatcher.clone(),
            state_node_adapter.clone(),
            node_network.into(),
        ));

        // create collation processor that will use these adapters
        // and run dispatcher for its own tasks queue
        let processor = CollationProcessor::new(
            config.clone(),
            dispatcher.clone(),
            mpool_adapter.clone(),
            state_node_adapter.clone(),
            validator,
        );
        AsyncQueuedDispatcher::run(processor, receiver);
        tracing::trace!(target: tracing_targets::COLLATION_MANAGER, "Tasks queue dispatcher started");

        // create manager instance
        let mgr = Self {
            config,
            dispatcher: dispatcher.clone(),
        };

        // start other async processes

        // schedule to check collation sessions and force refresh
        // if not initialized (when started from zerostate)
        schedule_async_action(
            tokio::time::Duration::from_secs(10),
            || async move {
                dispatcher
                    .enqueue_task(method_to_async_task_closure!(
                        check_refresh_collation_sessions,
                    ))
                    .await
            },
            "CollationProcessor::check_refresh_collation_sessions()".into(),
        );

        tracing::info!(target: tracing_targets::COLLATION_MANAGER, "Action scheduled in 10s: CollationProcessor::check_refresh_collation_sessions()");
        tracing::info!(target: tracing_targets::COLLATION_MANAGER, "Collation manager created");

        // return manager
        mgr
    }
}

#[async_trait]
impl<C, V, MQ, MP, ST> MempoolEventListener
    for AsyncQueuedDispatcher<CollationProcessor<C, V, MQ, MP, ST>, ()>
where
    C: Collator<MQ, MP, ST>,
    V: Validator<ST>,
    MQ: MessageQueueAdapter,
    MP: MempoolAdapter,
    ST: StateNodeAdapter,
{
    async fn on_new_anchor(&self, anchor: Arc<MempoolAnchor>) -> Result<()> {
        self.enqueue_task(method_to_async_task_closure!(
            process_new_anchor_from_mempool,
            anchor
        ))
        .await
    }
}

#[async_trait]
impl<C, V, MQ, MP, ST> StateNodeEventListener
    for AsyncQueuedDispatcher<CollationProcessor<C, V, MQ, MP, ST>, ()>
where
    C: Collator<MQ, MP, ST>,
    V: Validator<ST>,
    MQ: MessageQueueAdapter,
    MP: MempoolAdapter,
    ST: StateNodeAdapter,
{
    async fn on_mc_block(&self, mc_block_id: BlockId) -> Result<()> {
        self.enqueue_task(method_to_async_task_closure!(
            process_mc_block_from_bc,
            mc_block_id
        ))
        .await
    }
}

#[async_trait]
impl<C, V, MQ, MP, ST> CollatorEventListener
    for AsyncQueuedDispatcher<CollationProcessor<C, V, MQ, MP, ST>, ()>
where
    C: Collator<MQ, MP, ST>,
    V: Validator<ST>,
    MQ: MessageQueueAdapter,
    MP: MempoolAdapter,
    ST: StateNodeAdapter,
{
    async fn on_skipped_empty_anchor(
        &self,
        shard_id: ShardIdent,
        anchor: Arc<MempoolAnchor>,
    ) -> Result<()> {
        self.enqueue_task(method_to_async_task_closure!(
            process_empty_skipped_anchor,
            shard_id,
            anchor
        ))
        .await
    }
    async fn on_block_candidate(&self, collation_result: BlockCollationResult) -> Result<()> {
        self.enqueue_task(method_to_async_task_closure!(
            process_block_candidate,
            collation_result
        ))
        .await
    }
    async fn on_collator_stopped(&self, stop_key: CollationSessionId) -> Result<()> {
        self.enqueue_task(method_to_async_task_closure!(
            process_collator_stopped,
            stop_key
        ))
        .await
    }
}

#[async_trait]
impl<C, V, MQ, MP, ST> ValidatorEventListener
    for AsyncQueuedDispatcher<CollationProcessor<C, V, MQ, MP, ST>, ()>
where
    C: Collator<MQ, MP, ST>,
    V: Validator<ST>,
    MQ: MessageQueueAdapter,
    MP: MempoolAdapter,
    ST: StateNodeAdapter,
{
    async fn on_block_validated(
        &self,
        block_id: BlockId,
        event: OnValidatedBlockEvent,
    ) -> Result<()> {
        self.enqueue_task(method_to_async_task_closure!(
            process_validated_block,
            block_id,
            event
        ))
        .await
    }
}
