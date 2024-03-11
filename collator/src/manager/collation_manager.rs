use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;

use super::collation_processor::{CollationProcessor, CollationProcessorTaskResult};

use crate::{
    collator::{Collator, CollatorEventListener, CollatorStdImpl},
    mempool::{MempoolAdapter, MempoolAdapterStdImpl},
    method_to_async_task_closure,
    msg_queue::{
        MessageQueueAdapter, MessageQueueAdapterStdImpl, QueueIterator, QueueIteratorImpl,
    },
    state_node::{
        StateNodeAdapter, StateNodeAdapterBuilder, StateNodeAdapterStdImpl, StateNodeEventListener,
    },
    types::{
        ext_types::{BlockIdExt, ShardIdent},
        BlockCollationResult, CollationConfig, CollatorSubset, ShardStateStuff, ValidatedBlock,
    },
    utils::{
        async_queued_dispatcher::{AsyncQueuedDispatcher, STANDART_DISPATCHER_QUEUE_BUFFER_SIZE},
        schedule_async_action,
    },
    validator::{self, Validator, ValidatorEventListener, ValidatorStdImpl},
};

/// Controls the whole collation process.
/// Monitors state sync, receives ext msgs from mempool,
/// runs collators to produce blocks,
/// executes blocks validation, sends signed blocks
/// to state node to update local sync state and broadcast.
#[async_trait]
pub trait CollationManager<C, V, MQ, MP, ST>
where
    C: Collator,
    V: Validator<ST>,
    MQ: MessageQueueAdapter,
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
pub struct CollationManagerGenImpl<C, V, MQ, MP, ST>
where
    C: Collator,
    V: Validator<ST>,
    MQ: MessageQueueAdapter,
    MP: MempoolAdapter,
    ST: StateNodeAdapter,
{
    config: Arc<CollationConfig>,

    _marker_collator: std::marker::PhantomData<C>,
    _marker_validator: std::marker::PhantomData<V>,
    _marker_mq_adapter: std::marker::PhantomData<MQ>,

    mempool_adapter: Arc<MP>,
    state_node_adapter: Arc<ST>,

    dispatcher: Arc<
        AsyncQueuedDispatcher<CollationProcessor<C, V, MQ, MP, ST>, CollationProcessorTaskResult>,
    >,
}

#[async_trait]
impl<C, V, MQ, MP, ST> CollationManager<C, V, MQ, MP, ST>
    for CollationManagerGenImpl<C, V, MQ, MP, ST>
where
    C: Collator,
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
            AsyncQueuedDispatcher::new(STANDART_DISPATCHER_QUEUE_BUFFER_SIZE);
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
            _marker_collator: std::marker::PhantomData,
            _marker_validator: std::marker::PhantomData,
            _marker_mq_adapter: std::marker::PhantomData,
            mempool_adapter,
            state_node_adapter,
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
    C: Collator,
    V: Validator<ST>,
    MQ: MessageQueueAdapter,
    MP: MempoolAdapter,
    ST: StateNodeAdapter,
{
    async fn on_mc_block(&self, mc_block_id: BlockIdExt) -> Result<()> {
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
    C: Collator,
    V: Validator<ST>,
    MQ: MessageQueueAdapter,
    MP: MempoolAdapter,
    ST: StateNodeAdapter,
{
    async fn on_block_candidat(&self, collation_result: BlockCollationResult) -> Result<()> {
        self.enqueue_task(method_to_async_task_closure!(
            process_block_candidate,
            collation_result
        ))
        .await
    }
}

#[async_trait]
impl<C, V, MQ, MP, ST> ValidatorEventListener
    for AsyncQueuedDispatcher<CollationProcessor<C, V, MQ, MP, ST>, CollationProcessorTaskResult>
where
    C: Collator,
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
