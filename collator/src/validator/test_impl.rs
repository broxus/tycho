use std::{collections::HashMap, future::Future, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;

use everscale_types::models::{BlockId, BlockIdShort, ShardIdent, Signature, ValidatorDescription};

use tycho_block_util::block::BlockStuff;

use crate::{
    state_node::StateNodeAdapter,
    tracing_targets,
    types::{BlockSignatures, ValidatedBlock},
    utils::async_queued_dispatcher::AsyncQueuedDispatcher,
};

use super::{
    validator_processor::{ValidatorProcessor, ValidatorProcessorSpecific, ValidatorTaskResult},
    ValidatorEventEmitter, ValidatorEventListener,
};

pub struct ValidatorProcessorTestImpl<ST>
where
    ST: StateNodeAdapter,
{
    dispatcher: Arc<AsyncQueuedDispatcher<Self, ValidatorTaskResult>>,
    listener: Arc<dyn ValidatorEventListener>,
    state_node_adapter: Arc<ST>,

    _stub_candidates_cache: HashMap<BlockId, bool>,
}

#[async_trait]
impl<ST> ValidatorEventEmitter for ValidatorProcessorTestImpl<ST>
where
    ST: StateNodeAdapter,
{
    async fn on_block_validated_event(&self, validated_block: ValidatedBlock) -> Result<()> {
        self.listener.on_block_validated(validated_block).await
    }
}

#[async_trait]
impl<ST> ValidatorProcessor<ST> for ValidatorProcessorTestImpl<ST>
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

            _stub_candidates_cache: HashMap::new(),
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
impl<ST> ValidatorProcessorSpecific<ST> for ValidatorProcessorTestImpl<ST>
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
        let candidate_id = BlockIdShort {
            shard: shard_id,
            seqno: seq_no,
        };
        tracing::debug!(
            target: tracing_targets::VALIDATOR,
            "Validator (block: {}): STUB: emulating signature request to neighbor {}",
            candidate_id, collator_descr.public_key,
        );
        tokio::spawn({
            let collator_descr = collator_descr.clone();
            tokio::time::sleep(tokio::time::Duration::from_millis(45)).await;
            async move {
                callback(collator_descr, Signature::default())
                    .await
                    .unwrap();
            }
        });

        Ok(())
    }

    fn find_neighbor(&self, neighbor: &ValidatorDescription) -> Option<&ValidatorDescription> {
        todo!()
    }

    fn check_signature(
        candidate_id: &BlockId,
        his_signature: &Signature,
        neighbor: &ValidatorDescription,
    ) -> Result<bool> {
        //STUB: always return true
        Ok(true)
    }

    fn is_candidate_validated(&self, block_id: &BlockId) -> bool {
        *self._stub_candidates_cache.get(block_id).unwrap_or(&false)
    }

    fn append_candidate_signature_and_return_if_validated(
        &mut self,
        candidate_id: BlockId,
        signature_is_valid: bool,
        his_signature: Signature,
        neighbor: ValidatorDescription,
    ) -> Option<ValidatedBlock> {
        //STUB: block is valid after a single valid signature

        self._stub_candidates_cache
            .entry(candidate_id)
            .or_insert(signature_is_valid);

        if self.is_candidate_validated(&candidate_id) {
            let validated_block = ValidatedBlock::new(
                candidate_id,
                BlockSignatures {
                    good_sigs: vec![(neighbor.public_key, his_signature)],
                    bad_sigs: vec![],
                },
            );

            Some(validated_block)
        } else {
            None
        }
    }
}
