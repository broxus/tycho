use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use everscale_types::models::BlockId;

use crate::{
    collator::{
        collator_processor::CollatorProcessorStdImpl, Collator, CollatorEventListener,
        CollatorStdImpl,
    },
    mempool::MempoolAdapter,
    method_to_async_task_closure,
    msg_queue::{MessageQueueAdapter, MessageQueueAdapterStdImpl, QueueImpl},
    state_node::{StateNodeAdapter, StateNodeAdapterBuilder, StateNodeEventListener},
    types::{BlockCollationResult, CollationConfig, CollationSessionId, ValidatedBlock},
    utils::{
        async_queued_dispatcher::{AsyncQueuedDispatcher, STANDARD_DISPATCHER_QUEUE_BUFFER_SIZE},
        schedule_async_action,
    },
    validator::{
        validator_processor::ValidatorProcessorStdImpl, Validator, ValidatorEventListener,
        ValidatorStdImpl,
    },
};

use super::collation_processor::{CollationProcessor, CollationProcessorTaskResult};

/// Controls the whole collation process.
/// Monitors state sync, receives ext msgs from mempool,
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
        mpool_adapter: MP,
        state_adapter_builder: impl StateNodeAdapterBuilder<ST> + Send,
    ) -> Self;
}

/// Generic implementation of [`CollationManager`]
pub(crate) struct CollationManagerGenImpl<C, V, MQ, MP, ST>
where
    C: Collator<MQ, ST>,
    V: Validator<ST>,
    MQ: MessageQueueAdapter,
    MP: MempoolAdapter,
    ST: StateNodeAdapter,
{
    config: Arc<CollationConfig>,

    dispatcher: Arc<
        AsyncQueuedDispatcher<CollationProcessor<C, V, MQ, MP, ST>, CollationProcessorTaskResult>,
    >,
}

#[allow(private_bounds)]
pub fn create_std_manager<MP, ST>(
    config: CollationConfig,
    mpool_adapter: MP,
    state_adapter_builder: impl StateNodeAdapterBuilder<ST> + Send,
) -> impl CollationManager<MP, ST>
where
    MP: MempoolAdapter,
    ST: StateNodeAdapter,
{
    CollationManagerGenImpl::<
        CollatorStdImpl<CollatorProcessorStdImpl<_, _>, _, _>,
        ValidatorStdImpl<ValidatorProcessorStdImpl<_>, _>,
        MessageQueueAdapterStdImpl<QueueImpl>,
        MP,
        ST,
    >::create(config, mpool_adapter, state_adapter_builder)
}

impl<C, V, MQ, MP, ST> CollationManager<MP, ST> for CollationManagerGenImpl<C, V, MQ, MP, ST>
where
    C: Collator<MQ, ST>,
    V: Validator<ST>,
    MQ: MessageQueueAdapter,
    MP: MempoolAdapter,
    ST: StateNodeAdapter,
{
    fn create(
        config: CollationConfig,
        mempool_adapter: MP,
        state_adapter_builder: impl StateNodeAdapterBuilder<ST> + Send,
    ) -> Self {
        let config = Arc::new(config);

        // create dispatcher for own async tasks queue
        let (dispatcher, receiver) =
            AsyncQueuedDispatcher::new(STANDARD_DISPATCHER_QUEUE_BUFFER_SIZE);
        let dispatcher = Arc::new(dispatcher);

        //TODO: build mempool adapter and start its tasks queue
        let mempool_adapter = Arc::new(mempool_adapter);

        // build state node adapter and start its tasks queue
        let state_node_adapter = state_adapter_builder.build(dispatcher.clone());
        let state_node_adapter = Arc::new(state_node_adapter);

        // create validator and start its tasks queue
        let validator = Validator::create(dispatcher.clone(), state_node_adapter.clone());
        let validator = Arc::new(validator);

        // create collation processor that will use these adapters
        // and run dispatcher for own tasks queue
        let processor = CollationProcessor::new(
            config.clone(),
            dispatcher.clone(),
            mempool_adapter.clone(),
            state_node_adapter.clone(),
            validator,
        );
        AsyncQueuedDispatcher::run(processor, receiver);

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

        // return manager
        mgr
    }
}

#[async_trait]
impl<C, V, MQ, MP, ST> StateNodeEventListener
    for AsyncQueuedDispatcher<CollationProcessor<C, V, MQ, MP, ST>, CollationProcessorTaskResult>
where
    C: Collator<MQ, ST>,
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
    for AsyncQueuedDispatcher<CollationProcessor<C, V, MQ, MP, ST>, CollationProcessorTaskResult>
where
    C: Collator<MQ, ST>,
    V: Validator<ST>,
    MQ: MessageQueueAdapter,
    MP: MempoolAdapter,
    ST: StateNodeAdapter,
{
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
    for AsyncQueuedDispatcher<CollationProcessor<C, V, MQ, MP, ST>, CollationProcessorTaskResult>
where
    C: Collator<MQ, ST>,
    V: Validator<ST>,
    MQ: MessageQueueAdapter,
    MP: MempoolAdapter,
    ST: StateNodeAdapter,
{
    async fn on_block_validated(&self, signed_block: ValidatedBlock) -> Result<()> {
        self.enqueue_task(method_to_async_task_closure!(
            process_validated_block,
            signed_block
        ))
        .await
    }
}
