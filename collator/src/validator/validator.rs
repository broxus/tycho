use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use everscale_crypto::ed25519::KeyPair;
use everscale_types::models::BlockId;

use crate::types::{OnValidatedBlockEvent, ValidatorNetwork};
use crate::validator::types::ValidationSessionInfo;
use crate::{
    method_to_async_task_closure, state_node::StateNodeAdapter, tracing_targets,
    utils::async_queued_dispatcher::AsyncQueuedDispatcher,
};

use super::validator_processor::{ValidatorProcessor, ValidatorTaskResult};
const VALIDATOR_BUFFER_SIZE: usize = 1usize;
//TODO: remove emitter
#[async_trait]
pub trait ValidatorEventEmitter {
    /// When shard or master block was validated by validator
    async fn on_block_validated_event(
        &self,
        block_id: BlockId,
        event: OnValidatedBlockEvent,
    ) -> Result<()>;
}

#[async_trait]
pub trait ValidatorEventListener: Send + Sync {
    /// Process validated shard or master block
    async fn on_block_validated(
        &self,
        block_id: BlockId,
        event: OnValidatedBlockEvent,
    ) -> Result<()>;
}

#[async_trait]
pub trait Validator<ST>: Send + Sync + 'static
where
    ST: StateNodeAdapter,
{
    fn create(
        listener: Arc<dyn ValidatorEventListener>,
        state_node_adapter: Arc<ST>,
        network: ValidatorNetwork,
    ) -> Self;

    /// Enqueue block candidate validation task
    async fn enqueue_candidate_validation(
        &self,
        candidate: BlockId,
        session_seqno: u32,
        current_validator_keypair: KeyPair,
    ) -> Result<()>;
    async fn enqueue_stop_candidate_validation(&self, candidate: BlockId) -> Result<()>;

    async fn enqueue_add_session(&self, session_info: Arc<ValidationSessionInfo>) -> Result<()>;
}

#[allow(private_bounds)]
pub struct ValidatorStdImpl<W, ST>
where
    W: ValidatorProcessor<ST>,
    ST: StateNodeAdapter,
{
    _marker_state_node_adapter: std::marker::PhantomData<ST>,
    dispatcher: Arc<AsyncQueuedDispatcher<W, ValidatorTaskResult>>,
}

#[async_trait]
impl<W, ST> Validator<ST> for ValidatorStdImpl<W, ST>
where
    W: ValidatorProcessor<ST>,
    ST: StateNodeAdapter,
{
    fn create(
        listener: Arc<dyn ValidatorEventListener>,
        state_node_adapter: Arc<ST>,
        network: ValidatorNetwork,
    ) -> Self {
        tracing::info!(target: tracing_targets::VALIDATOR, "Creating validator...");

        // create dispatcher for own async tasks queue
        let (dispatcher, receiver) = AsyncQueuedDispatcher::new(VALIDATOR_BUFFER_SIZE);
        let dispatcher = Arc::new(dispatcher);

        // create validation processor and run dispatcher for own tasks queue
        let processor =
            ValidatorProcessor::new(dispatcher.clone(), listener, state_node_adapter, network);
        AsyncQueuedDispatcher::run(processor, receiver);
        tracing::trace!(target: tracing_targets::VALIDATOR, "Tasks queue dispatcher started");

        tracing::info!(target: tracing_targets::VALIDATOR, "Validator created");

        // create validator instance
        Self {
            _marker_state_node_adapter: std::marker::PhantomData,
            dispatcher,
        }
    }

    async fn enqueue_candidate_validation(
        &self,
        candidate: BlockId,
        session_seqno: u32,
        current_validator_keypair: KeyPair,
    ) -> Result<()> {
        self.dispatcher
            .enqueue_task(method_to_async_task_closure!(
                start_candidate_validation,
                candidate,
                session_seqno,
                current_validator_keypair
            ))
            .await
    }

    async fn enqueue_stop_candidate_validation(&self, candidate: BlockId) -> Result<()> {
        self.dispatcher
            .enqueue_task(method_to_async_task_closure!(
                stop_candidate_validation,
                candidate
            ))
            .await
    }

    async fn enqueue_add_session(&self, session_info: Arc<ValidationSessionInfo>) -> Result<()> {
        self.dispatcher
            .enqueue_task(method_to_async_task_closure!(try_add_session, session_info))
            .await
    }
}
