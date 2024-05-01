use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;

use everscale_types::models::{BlockId, ShardIdent};
use tycho_block_util::state::ShardStateStuff;

use crate::collator::CollatorFactory;
use crate::validator::config::ValidatorConfig;
use crate::validator::{ValidatorContext, ValidatorFactory};
use crate::{
    collator::CollatorEventListener,
    mempool::{MempoolAdapter, MempoolAnchor, MempoolEventListener},
    method_to_async_task_closure,
    msg_queue::MessageQueueAdapter,
    state_node::{StateNodeAdapter, StateNodeEventListener},
    tracing_targets,
    types::{BlockCollationResult, CollationConfig, CollationSessionId, OnValidatedBlockEvent},
    utils::{
        async_queued_dispatcher::{AsyncQueuedDispatcher, STANDARD_DISPATCHER_QUEUE_BUFFER_SIZE},
        schedule_async_action,
    },
    validator::{Validator, ValidatorEventListener},
};

use super::collation_processor::CollationProcessor;

/// Controls the whole collation process.
/// Monitors state sync,
/// runs collators to produce blocks,
/// executes blocks validation, sends signed blocks
/// to state node to update local sync state and broadcast.
/// Generic implementation of [`CollationManager`]
pub(crate) struct CollationManager<ST, MQ, MP, CF, V>
where
    CF: CollatorFactory,
{
    config: Arc<CollationConfig>,
    dispatcher: Arc<AsyncQueuedDispatcher<CollationProcessor<ST, MQ, MP, CF, V>, ()>>,
    state_node_adapter: Arc<ST>,
}

impl<ST, MQ, MP, CF, V> CollationManager<ST, MQ, MP, CF, V>
where
    ST: StateNodeAdapter,
    MQ: MessageQueueAdapter,
    MP: MempoolAdapter,
    CF: CollatorFactory,
    V: Validator,
{
    pub fn new<VF>(
        state_node_adapter: Arc<ST>,
        mpool_adapter: Arc<MP>,
        mq_adapter: Arc<MQ>,
        validator_factory: VF,
        collator_factory: CF,
        config: CollationConfig,
    ) -> Self
    where
        VF: ValidatorFactory<Validator = V>,
    {
        tracing::info!(target: tracing_targets::COLLATION_MANAGER, "Creating collation manager...");

        let config = Arc::new(config);

        // create dispatcher for own async tasks queue
        let (dispatcher, receiver) =
            AsyncQueuedDispatcher::new(STANDARD_DISPATCHER_QUEUE_BUFFER_SIZE);
        let dispatcher = Arc::new(dispatcher);

        let validator_config = ValidatorConfig {
            base_loop_delay: Duration::from_millis(50),
            max_loop_delay: Duration::from_secs(10),
        };

        // create validator and start its tasks queue
        let validator = validator_factory.build(ValidatorContext {
            listeners: vec![dispatcher.clone()],
        });

        // create collation processor that will use these adapters
        // and run dispatcher for its own tasks queue
        let processor = CollationProcessor::new(
            config.clone(),
            dispatcher.clone(),
            state_node_adapter.clone(),
            mpool_adapter,
            mq_adapter,
            validator,
            collator_factory,
        );
        AsyncQueuedDispatcher::run(processor, receiver);
        tracing::trace!(target: tracing_targets::COLLATION_MANAGER, "Tasks queue dispatcher started");

        // create manager instance
        let mgr = Self {
            config,
            dispatcher: dispatcher.clone(),
            state_node_adapter,
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

    fn get_state_node_adapter(&self) -> Arc<ST> {
        self.state_node_adapter.clone()
    }
}

#[async_trait]
impl<ST, MQ, MP, CF, V> MempoolEventListener
    for AsyncQueuedDispatcher<CollationProcessor<ST, MQ, MP, CF, V>, ()>
where
    ST: StateNodeAdapter,
    MQ: MessageQueueAdapter,
    MP: MempoolAdapter,
    CF: CollatorFactory,
    V: Validator,
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
impl<ST, MQ, MP, CF, V> StateNodeEventListener
    for AsyncQueuedDispatcher<CollationProcessor<ST, MQ, MP, CF, V>, ()>
where
    ST: StateNodeAdapter,
    MQ: MessageQueueAdapter,
    MP: MempoolAdapter,
    CF: CollatorFactory,
    V: Validator,
{
    async fn on_block_accepted(&self, block_id: &BlockId) -> Result<()> {
        //TODO: remove accepted block from cache
        //STUB: do nothing, currently we remove block from cache when it sent to state node
        Ok(())
    }

    async fn on_block_accepted_external(&self, state: &ShardStateStuff) -> Result<()> {
        //TODO: should store block info from blockchain if it was not already collated
        //      and validated by ourself. Will use this info for faster validation further:
        //      will consider that just collated block is already validated if it have the
        //      same root hash and file hash
        if state.block_id().is_masterchain() {
            let mc_block_id = *state.block_id();
            self.enqueue_task(method_to_async_task_closure!(
                process_mc_block_from_bc,
                mc_block_id
            ))
            .await
        } else {
            Ok(())
        }
    }
}

#[async_trait]
impl<ST, MQ, MP, CF, V> CollatorEventListener
    for AsyncQueuedDispatcher<CollationProcessor<ST, MQ, MP, CF, V>, ()>
where
    ST: StateNodeAdapter,
    MQ: MessageQueueAdapter,
    MP: MempoolAdapter,
    CF: CollatorFactory,
    V: Validator,
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
impl<ST, MQ, MP, CF, V> ValidatorEventListener
    for AsyncQueuedDispatcher<CollationProcessor<ST, MQ, MP, CF, V>, ()>
where
    ST: StateNodeAdapter,
    MQ: MessageQueueAdapter,
    MP: MempoolAdapter,
    CF: CollatorFactory,
    V: Validator,
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
