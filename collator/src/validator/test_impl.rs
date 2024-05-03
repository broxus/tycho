use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use everscale_crypto::ed25519::{KeyPair, PublicKey};

use everscale_types::models::{BlockId, BlockIdShort, Signature};
use tokio::sync::Semaphore;

use tycho_block_util::state::ShardStateStuff;
use tycho_util::FastHashMap;

use crate::tracing_targets;
use crate::types::{BlockSignatures, OnValidatedBlockEvent, ValidatorNetwork};
use crate::validator::state::SessionInfo;
use crate::validator::types::ValidationSessionInfo;
use crate::{state_node::StateNodeAdapter, utils::async_queued_dispatcher::AsyncQueuedDispatcher};

use super::{ValidatorEventEmitter, ValidatorEventListener};

pub struct ValidatorProcessorTestImpl<ST>
where
    ST: StateNodeAdapter,
{
    _dispatcher: AsyncQueuedDispatcher<Self, ValidatorTaskResult>,
    listener: Arc<dyn ValidatorEventListener>,
    _state_node_adapter: Arc<ST>,

    _stub_candidates_cache: HashMap<BlockId, bool>,
}

#[async_trait]
impl<ST> ValidatorEventEmitter for ValidatorProcessorTestImpl<ST>
where
    ST: StateNodeAdapter,
{
    async fn on_block_validated_event(
        &self,
        block_id: BlockId,
        event: OnValidatedBlockEvent,
    ) -> Result<()> {
        self.listener.on_block_validated(block_id, event).await
    }
}

#[async_trait]
impl<ST> ValidatorProcessor<ST> for ValidatorProcessorTestImpl<ST>
where
    ST: StateNodeAdapter,
{
    async fn enqueue_process_new_mc_block_state(
        &self,
        _mc_state: Arc<ShardStateStuff>,
    ) -> Result<()> {
        todo!()
    }

    fn new(
        listener: Arc<dyn ValidatorEventListener>,
        _state_node_adapter: Arc<ST>,
        _network: ValidatorNetwork,
    ) -> Self {
        Self {
            _dispatcher,
            listener,
            _state_node_adapter,
            _stub_candidates_cache: HashMap::new(),
        }
    }

    async fn start_candidate_validation(
        // &self,
        candidate_id: BlockId,
        session: &Arc<SessionInfo>,
        current_validator_keypair: KeyPair,
        listener: Vec<Arc<dyn ValidatorEventListener>>,
    ) -> Result<()> {
        let mut signatures = FastHashMap::default();
        signatures.insert(
            current_validator_keypair.public_key.to_bytes().into(),
            Signature::default(),
        );
        tracing::debug!(
            target: tracing_targets::VALIDATOR,
            "Validator (block: {}): STUB: emulated validation via signatures request",
            candidate_id.as_short_id(),
        );
        for listener in listener.iter() {
            listener
                .on_block_validated(
                    candidate_id,
                    OnValidatedBlockEvent::Valid(BlockSignatures {
                        signatures: signatures.clone(),
                    }),
                )
                .await?;
        }

        Ok(())
    }
}
