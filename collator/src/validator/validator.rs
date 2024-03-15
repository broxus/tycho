use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use everscale_types::models::BlockId;

use crate::{
    method_to_async_task_closure,
    state_node::StateNodeAdapter,
    types::CollationSessionInfo,
    types::ValidatedBlock,
    utils::async_queued_dispatcher::{
        AsyncQueuedDispatcher, STANDARD_DISPATCHER_QUEUE_BUFFER_SIZE,
    },
};

use super::validator_processor::{ValidatorProcessor, ValidatorTaskResult};

// EVENTS EMITTER AMD LISTENER

#[async_trait]
pub trait ValidatorEventEmitter {
    /// When shard or master block was validated by validator
    async fn on_block_validated_event(&self, validated_block: ValidatedBlock) -> Result<()>;
}

#[async_trait]
pub trait ValidatorEventListener: Send + Sync {
    /// Process validated shard or master block
    async fn on_block_validated(&self, validated_block: ValidatedBlock) -> Result<()>;
}

// ADAPTER

#[async_trait]
pub trait Validator<ST>: Send + Sync + 'static
where
    ST: StateNodeAdapter,
{
    fn create(
        listener: Arc<dyn ValidatorEventListener>,
        state_node_adapter: Arc<ST>, /*, overlay_adapter: Arc<OA> */
    ) -> Self;
    /// Enqueue block candidate validation task
    async fn enqueue_candidate_validation(
        &self,
        candidate: BlockId,
        session_info: Arc<CollationSessionInfo>,
    ) -> Result<()>;
}

#[allow(private_bounds)]
pub(crate) struct ValidatorStdImpl<W, ST>
where
    W: ValidatorProcessor<ST>,
    ST: StateNodeAdapter,
    // OA: OverlayAdapter,
{
    _marker_state_node_adapter: std::marker::PhantomData<ST>,
    dispatcher: Arc<AsyncQueuedDispatcher<W, ValidatorTaskResult>>,
}

#[async_trait]
impl<W, ST> Validator<ST> for ValidatorStdImpl<W, ST>
where
    W: ValidatorProcessor<ST>,
    ST: StateNodeAdapter,
    // OA: OverlayAdapter
{
    fn create(
        listener: Arc<dyn ValidatorEventListener>,
        state_node_adapter: Arc<ST>, /*, overlay_adapter: Arc<OA>*/
    ) -> Self {
        // create dispatcher for own async tasks queue
        let (dispatcher, receiver) =
            AsyncQueuedDispatcher::new(STANDARD_DISPATCHER_QUEUE_BUFFER_SIZE);
        let dispatcher = Arc::new(dispatcher);

        // create validation processor and run dispatcher for own tasks queue
        let processor = ValidatorProcessor::new(dispatcher.clone(), listener, state_node_adapter);
        AsyncQueuedDispatcher::run(processor, receiver);

        // create validator instance
        Self {
            _marker_state_node_adapter: std::marker::PhantomData,
            dispatcher,
        }
    }

    async fn enqueue_candidate_validation(
        &self,
        candidate: BlockId,
        session_info: Arc<CollationSessionInfo>,
    ) -> Result<()> {
        self.dispatcher
            .enqueue_task(method_to_async_task_closure!(
                start_candidate_validation,
                candidate,
                session_info
            ))
            .await
    }
}
