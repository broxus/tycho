use std::{future::Future, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;

use everscale_types::models::{BlockId, ShardIdent, Signature, ValidatorDescription};

use tycho_block_util::block::BlockStuff;

use crate::{
    method_to_async_task_closure,
    state_node::StateNodeAdapter,
    types::{CollationSessionInfo, ValidatedBlock},
    utils::async_queued_dispatcher::AsyncQueuedDispatcher,
};

use super::{ValidatorEventEmitter, ValidatorEventListener};

// ADAPTER PROCESSOR

pub enum ValidatorTaskResult {
    Void,
}

#[allow(private_bounds)]
#[async_trait]
pub(super) trait ValidatorProcessor<ST>:
    ValidatorProcessorSpecific<ST> + ValidatorEventEmitter + Sized + Send + Sync + 'static
where
    ST: StateNodeAdapter,
{
    fn new(
        dispatcher: Arc<AsyncQueuedDispatcher<Self, ValidatorTaskResult>>,
        listener: Arc<dyn ValidatorEventListener>,
        state_node_adapter: Arc<ST>,
    ) -> Self;

    fn get_dispatcher(&self) -> Arc<AsyncQueuedDispatcher<Self, ValidatorTaskResult>>;

    fn get_state_node_adapter(&self) -> Arc<ST>;

    // fn get_network_adapter(&self) -> Arc<NA>;

    /// Start block candidate validation process
    async fn start_candidate_validation(
        &self,
        candidate_id: BlockId,
        session_info: Arc<CollationSessionInfo>,
    ) -> Result<ValidatorTaskResult> {
        //TODO: we may received candidate signatures before with signature requests from neighbor collators

        // first, try request already signed block from state node
        // possibly we are slow and 2/3+1 fast nodes already signed this block
        let receiver = self
            .get_state_node_adapter()
            .request_block(candidate_id)
            .await?;

        let dispatcher = self.get_dispatcher();
        tokio::spawn(async move {
            if let Ok(Some(block_from_bc)) = receiver.try_recv().await {
                // if state node contains required block then schedule validation using it
                dispatcher
                    .clone()
                    .enqueue_task(method_to_async_task_closure!(
                        validate_candidate_by_block_from_bc,
                        candidate_id,
                        block_from_bc
                    ))
                    .await;
            } else {
                // if state node does not contain such a block
                // then request signatures from neighbor collators
                dispatcher
                    .enqueue_task(method_to_async_task_closure!(
                        request_candidate_signatures,
                        candidate_id,
                        Signature::default(),
                        session_info
                    ))
                    .await;

                //TODO: need to add a block waiting timeout and proceed to the signature request after it expires
            }
        });

        Ok(ValidatorTaskResult::Void)
    }

    async fn stop_candidate_validation(
        &self,
        candidate_id: BlockId,
    ) -> Result<ValidatorTaskResult> {
        Ok(ValidatorTaskResult::Void)
    }

    /// Send signature request to each neighbor passing callback closure
    /// that queue signatures responses processing
    async fn request_candidate_signatures(
        &mut self,
        candidate_id: BlockId,
        own_signature: Signature,
        session_info: Arc<CollationSessionInfo>,
    ) -> Result<ValidatorTaskResult> {
        for collator_descr in session_info.collators().validators.iter() {
            let dispatcher = self.get_dispatcher();
            let candidate_id = candidate_id;
            Self::request_cadidate_signature_from_neighbor(
                collator_descr,
                candidate_id.shard,
                candidate_id.seqno,
                own_signature,
                move |collator_descr, his_signature| async move {
                    dispatcher
                        .enqueue_task(method_to_async_task_closure!(
                            process_candidate_signature_response,
                            collator_descr,
                            his_signature,
                            candidate_id
                        ))
                        .await
                },
            )
            .await?;
        }
        Ok(ValidatorTaskResult::Void)
    }

    async fn process_candidate_signature_response(
        &mut self,
        collator_id: ValidatorDescription,
        his_signature: Signature,
        candidate_id: BlockId,
    ) -> Result<ValidatorTaskResult> {
        // skip signature if candidate already validated (does not matter if it valid or not)
        if self.is_candidate_validated(&candidate_id) {
            return Ok(ValidatorTaskResult::Void);
        }

        // get neighbor from local list
        let neighbor = match self.find_neighbor(&collator_id) {
            Some(n) => n,
            None => {
                // skip signature if collator is unknown
                return Ok(ValidatorTaskResult::Void);
            }
        };

        // check signature and update candidate score
        let signature_is_valid = Self::check_signature(&candidate_id, &his_signature, neighbor)?;
        self.update_candidate_score(
            candidate_id,
            signature_is_valid,
            his_signature,
            neighbor.clone(),
        )
        .await?;

        Ok(ValidatorTaskResult::Void)
    }

    async fn update_candidate_score(
        &mut self,
        candidate_id: BlockId,
        signature_is_valid: bool,
        his_signature: Signature,
        neighbor: ValidatorDescription,
    ) -> Result<()> {
        if let Some(validated_block) = self.append_candidate_signature_and_return_if_validated(
            candidate_id,
            signature_is_valid,
            his_signature,
            neighbor,
        ) {
            self.on_block_validated_event(validated_block).await?;
        }

        Ok(())
    }
}

/// Trait declares functions that need specific implementation.
/// For test purposes you can re-implement only this trait.
#[async_trait]
pub(super) trait ValidatorProcessorSpecific<ST>: Sized {
    /// Find a neighbor info by id in local sessions info
    fn find_neighbor(&self, neighbor: &ValidatorDescription) -> Option<&ValidatorDescription>;

    /// Use signatures of existing block from blockchain to validate candidate
    async fn validate_candidate_by_block_from_bc(
        &mut self,
        candidate_id: BlockId,
        block_from_bc: Arc<BlockStuff>,
    ) -> Result<ValidatorTaskResult>;

    /// Request signature from neighbor collator and run callback when receive response.
    /// Send own signature so neighbor can use it to validate his own candidate
    async fn request_cadidate_signature_from_neighbor<Fut>(
        collator_descr: &ValidatorDescription,
        shard_id: ShardIdent,
        seq_no: u32,
        own_signature: Signature,
        callback: impl FnOnce(ValidatorDescription, Signature) -> Fut + Send + 'static,
    ) -> Result<()>
    where
        Fut: Future<Output = Result<()>> + Send;

    fn check_signature(
        candidate_id: &BlockId,
        his_signature: &Signature,
        neighbor: &ValidatorDescription,
    ) -> Result<bool>;

    fn is_candidate_validated(&self, block_id: &BlockId) -> bool;

    fn append_candidate_signature_and_return_if_validated(
        &mut self,
        candidate_id: BlockId,
        signature_is_valid: bool,
        his_signature: Signature,
        neighbor: ValidatorDescription,
    ) -> Option<ValidatedBlock>;
}

pub(crate) struct ValidatorProcessorStdImpl<ST>
where
    ST: StateNodeAdapter,
{
    dispatcher: Arc<AsyncQueuedDispatcher<Self, ValidatorTaskResult>>,
    listener: Arc<dyn ValidatorEventListener>,
    // signatures: RwLock<HashMap<BlockId, HashMap<ValidatorDescription, Signature>>>,
    state_node_adapter: Arc<ST>,
}

#[async_trait]
impl<ST> ValidatorEventEmitter for ValidatorProcessorStdImpl<ST>
where
    ST: StateNodeAdapter,
{
    async fn on_block_validated_event(&self, validated_block: ValidatedBlock) -> Result<()> {
        self.listener.on_block_validated(validated_block).await
    }
}

#[async_trait]
impl<ST> ValidatorProcessor<ST> for ValidatorProcessorStdImpl<ST>
where
    ST: StateNodeAdapter,
{
    fn new(
        dispatcher: Arc<AsyncQueuedDispatcher<Self, ValidatorTaskResult>>,
        listener: Arc<dyn ValidatorEventListener>,
        state_node_adapter: Arc<ST>,
    ) -> Self {
        Self {
            dispatcher,
            listener,
            state_node_adapter,
        }
    }

    fn get_state_node_adapter(&self) -> Arc<ST> {
        self.state_node_adapter.clone()
    }

    fn get_dispatcher(&self) -> Arc<AsyncQueuedDispatcher<Self, ValidatorTaskResult>> {
        self.dispatcher.clone()
    }
}

#[async_trait]
impl<ST> ValidatorProcessorSpecific<ST> for ValidatorProcessorStdImpl<ST>
where
    ST: StateNodeAdapter,
{
    async fn validate_candidate_by_block_from_bc(
        &mut self,
        candidate_id: BlockId,
        block_from_bc: Arc<BlockStuff>,
    ) -> Result<ValidatorTaskResult> {
        todo!()
    }

    async fn request_cadidate_signature_from_neighbor<Fut>(
        collator_descr: &ValidatorDescription,
        shard_id: ShardIdent,
        seq_no: u32,
        own_signature: Signature,
        callback: impl FnOnce(ValidatorDescription, Signature) -> Fut + Send + 'static,
    ) -> Result<()>
    where
        Fut: Future<Output = Result<()>> + Send,
    {
        todo!()
    }

    fn find_neighbor(&self, neighbor: &ValidatorDescription) -> Option<&ValidatorDescription> {
        todo!()
    }

    fn check_signature(
        candidate_id: &BlockId,
        his_signature: &Signature,
        neighbor: &ValidatorDescription,
    ) -> Result<bool> {
        todo!()
    }

    fn is_candidate_validated(&self, block_id: &BlockId) -> bool {
        todo!()
    }

    fn append_candidate_signature_and_return_if_validated(
        &mut self,
        candidateid: BlockId,
        signature_is_valid: bool,
        his_signature: Signature,
        neighbor: ValidatorDescription,
    ) -> Option<ValidatedBlock> {
        todo!()
    }
}
