use std::{collections::HashMap, future::Future, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use everscale_crypto::ed25519::{KeyPair, PublicKey};

use everscale_types::models::{BlockId, BlockIdShort, ShardIdent, Signature, ValidatorDescription};
use tokio::sync::broadcast::Sender;

use tycho_block_util::block::BlockStuff;
use tycho_block_util::state::ShardStateStuff;

use crate::types::ValidatorNetwork;
use crate::validator::state::ValidationState;
use crate::validator::types::ValidationSessionInfo;
use crate::{
    state_node::StateNodeAdapter,
    tracing_targets,
    types::{BlockSignatures, ValidatedBlock},
    utils::async_queued_dispatcher::AsyncQueuedDispatcher,
};

use super::{
    validator_processor::{ValidatorProcessor, ValidatorTaskResult},
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
    async fn enqueue_process_new_mc_block_state(
        &self,
        mc_state: Arc<ShardStateStuff>,
    ) -> Result<()> {
        todo!()
    }

    fn new(
        dispatcher: Arc<AsyncQueuedDispatcher<Self, ValidatorTaskResult>>,
        listener: Arc<dyn ValidatorEventListener>,
        state_node_adapter: Arc<ST>,
        network: ValidatorNetwork,
    ) -> Self {
        Self {
            dispatcher,
            listener,
            state_node_adapter,
            _stub_candidates_cache: HashMap::new(),
        }
    }

    async fn start_candidate_validation(
        &mut self,
        candidate_id: BlockId,
        session_seqno: u32,
        current_validator_keypair: KeyPair,
    ) -> Result<ValidatorTaskResult> {
        todo!()
    }

    fn get_dispatcher(&self) -> Arc<AsyncQueuedDispatcher<Self, ValidatorTaskResult>> {
        todo!()
    }

    async fn try_add_session(
        &mut self,
        session: Arc<ValidationSessionInfo>,
    ) -> Result<ValidatorTaskResult> {
        todo!()
    }

    async fn stop_candidate_validation(
        &self,
        candidate_id: BlockId,
    ) -> Result<ValidatorTaskResult> {
        todo!()
    }

    async fn get_block_signatures(
        &mut self,
        session_seqno: u32,
        block_id_short: &BlockIdShort,
    ) -> Result<ValidatorTaskResult> {
        todo!()
    }

    async fn process_candidate_signature_response(
        &mut self,
        session_seqno: u32,
        block_id_short: BlockIdShort,
        signatures: Vec<([u8; 32], [u8; 64])>,
    ) -> Result<ValidatorTaskResult> {
        todo!()
    }

    async fn validate_candidate(
        &mut self,
        candidate_id: BlockId,
        session_seqno: u32,
        current_validator_pubkey: PublicKey,
    ) -> Result<ValidatorTaskResult> {
        todo!()
    }

    async fn get_validation_status(
        &mut self,
        session_seqno: u32,
        block_id_short: &BlockIdShort,
    ) -> Result<ValidatorTaskResult> {
        todo!()
    }
}
