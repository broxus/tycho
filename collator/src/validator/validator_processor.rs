use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use everscale_crypto::ed25519::KeyPair;
use everscale_types::cell::HashBytes;
use everscale_types::models::{BlockId, BlockIdShort, Signature};
use tokio::sync::broadcast;
use tokio::time::interval;
use tracing::warn;
use tracing::{debug, error, trace};

use crate::types::{BlockSignatures, OnValidatedBlockEvent, ValidatedBlock, ValidatorNetwork};
use tycho_block_util::state::ShardStateStuff;
use tycho_network::{OverlayId, PeerId, PrivateOverlay, Request};

use crate::validator::network::dto::SignaturesQuery;
use crate::validator::network::network_service::NetworkService;
use crate::validator::state::{ValidationState, ValidationStateStdImpl};
use crate::validator::types::{
    BlockValidationCandidate, OverlayNumber, ValidationResult, ValidationSessionInfo,
};
use crate::{
    method_to_async_task_closure, state_node::StateNodeAdapter, tracing_targets,
    utils::async_queued_dispatcher::AsyncQueuedDispatcher,
};

use super::{ValidatorEventEmitter, ValidatorEventListener};

const MAX_VALIDATION_ATTEMPTS: u32 = 1000;
const VALIDATION_RETRY_TIMEOUT_SEC: u64 = 3;

#[derive(PartialEq, Debug)]
pub enum ValidatorTaskResult {
    Void,
    Signatures(HashMap<HashBytes, Signature>),
    ValidationStatus(ValidationResult),
}

#[derive(Debug, Clone, PartialEq)]
pub struct StopMessage {
    block_id: BlockId,
}

#[allow(private_bounds)]
#[async_trait]
pub trait ValidatorProcessor<ST>: ValidatorEventEmitter + Sized + Send + Sync + 'static
where
    ST: StateNodeAdapter,
{
    fn new(
        dispatcher: Arc<AsyncQueuedDispatcher<Self, ValidatorTaskResult>>,
        listener: Arc<dyn ValidatorEventListener>,
        state_node_adapter: Arc<ST>,
        network: ValidatorNetwork,
    ) -> Self;

    fn get_dispatcher(&self) -> Arc<AsyncQueuedDispatcher<Self, ValidatorTaskResult>>;

    async fn try_add_session(
        &mut self,
        session: Arc<ValidationSessionInfo>,
    ) -> Result<ValidatorTaskResult>;

    /// Start block candidate validation process
    async fn start_candidate_validation(
        &mut self,
        candidate_id: BlockId,
        session_seqno: u32,
        current_validator_keypair: KeyPair,
    ) -> Result<ValidatorTaskResult>;

    async fn stop_candidate_validation(&self, candidate_id: BlockId)
        -> Result<ValidatorTaskResult>;

    async fn enqueue_process_new_mc_block_state(
        &self,
        mc_state: Arc<ShardStateStuff>,
    ) -> Result<()>;

    async fn process_candidate_signature_response(
        &mut self,
        session_seqno: u32,
        block_id_short: BlockIdShort,
        signatures: Vec<([u8; 32], [u8; 64])>,
    ) -> Result<ValidatorTaskResult>;

    async fn validate_candidate_by_block_from_bc(
        &mut self,
        _candidate_id: BlockId,
    ) -> Result<ValidatorTaskResult> {
        // self.on_block_validated_event(ValidatedBlock::new(candidate_id, vec![], true))
        //     .await?;
        // Ok(ValidatorTaskResult::Void)
        todo!();
    }
    async fn get_block_signatures(
        &mut self,
        session_seqno: u32,
        block_id_short: &BlockIdShort,
    ) -> Result<ValidatorTaskResult>;
    async fn validate_candidate(
        &mut self,
        candidate_id: BlockId,
        session_seqno: u32,
        current_validator_pubkey: everscale_crypto::ed25519::PublicKey,
    ) -> Result<ValidatorTaskResult>;
    async fn get_validation_status(
        &mut self,
        session_seqno: u32,
        block_id_short: &BlockIdShort,
    ) -> Result<ValidatorTaskResult>;
}

pub(crate) struct ValidatorProcessorStdImpl<ST>
where
    ST: StateNodeAdapter,
{
    dispatcher: Arc<AsyncQueuedDispatcher<Self, ValidatorTaskResult>>,
    listener: Arc<dyn ValidatorEventListener>,
    validation_state: ValidationStateStdImpl,
    state_node_adapter: Arc<ST>,
    network: ValidatorNetwork,
    stop_sender: broadcast::Sender<StopMessage>,
}

#[async_trait]
impl<ST> ValidatorEventEmitter for ValidatorProcessorStdImpl<ST>
where
    ST: StateNodeAdapter,
{
    async fn on_block_validated_event(
        &self,
        block: BlockId,
        event: OnValidatedBlockEvent,
    ) -> Result<()> {
        self.listener.on_block_validated(block, event).await
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
        network: ValidatorNetwork,
    ) -> Self {
        let (stop_sender, _) = broadcast::channel(1000);
        let validation_state = ValidationStateStdImpl::new();
        Self {
            dispatcher,
            listener,
            state_node_adapter,
            validation_state,
            network,
            stop_sender,
        }
    }

    fn get_dispatcher(&self) -> Arc<AsyncQueuedDispatcher<Self, ValidatorTaskResult>> {
        self.dispatcher.clone()
    }

    async fn try_add_session(
        &mut self,
        session: Arc<ValidationSessionInfo>,
    ) -> Result<ValidatorTaskResult> {
        if self.validation_state.get_session(session.seqno).is_none() {
            let (peer_resolver, local_peer_id) = {
                let network = self.network.clone();
                (
                    network.clone().peer_resolver,
                    network.dht_client.network().peer_id().0,
                )
            };

            let overlay_id = OverlayNumber {
                session_seqno: session.seqno,
            };
            let overlay_id = OverlayId(tl_proto::hash(overlay_id));
            let network_service = NetworkService::new(self.get_dispatcher().clone());

            let private_overlay = PrivateOverlay::builder(overlay_id)
                .with_peer_resolver(peer_resolver)
                .build(network_service);

            let overlay_added = self
                .network
                .overlay_service
                .add_private_overlay(&private_overlay);

            if !overlay_added {
                bail!("Failed to add private overlay");
            }

            self.validation_state
                .add_session(session.clone(), private_overlay.clone());

            let mut entries = private_overlay.write_entries();

            for validator in session.validators.values() {
                if validator.public_key.to_bytes() == local_peer_id {
                    continue;
                }
                entries.insert(&PeerId(validator.public_key.to_bytes()));
            }
        }
        Ok(ValidatorTaskResult::Void)
    }

    /// Start block candidate validation process
    async fn start_candidate_validation(
        &mut self,
        candidate_id: BlockId,
        session_seqno: u32,
        current_validator_keypair: KeyPair,
    ) -> Result<ValidatorTaskResult> {
        let mut stop_receiver = self.stop_sender.subscribe();

        // Simplify session retrieval with clear, concise error handling.
        let session = self
            .validation_state
            .get_mut_session(session_seqno)
            .ok_or_else(|| anyhow!("Failed to start candidate validation. Session not found"))?;

        let our_signature = sign_block(&current_validator_keypair, &candidate_id)?;
        let current_validator_signature =
            HashBytes(current_validator_keypair.public_key.to_bytes());
        session.add_block(candidate_id)?;

        let enqueue_task_result = self
            .dispatcher
            .enqueue_task(method_to_async_task_closure!(
                process_candidate_signature_response,
                session_seqno,
                candidate_id.as_short_id(),
                vec![(current_validator_signature.0, our_signature.0)]
            ))
            .await;

        if let Err(e) = enqueue_task_result {
            bail!("Failed to enqueue task for processing signatures response {e:?}");
        }

        let dispatcher = self.get_dispatcher().clone();
        let current_validator_pubkey = current_validator_keypair.public_key;

        tokio::spawn(async move {
            let mut retry_interval = interval(Duration::from_secs(VALIDATION_RETRY_TIMEOUT_SEC));
            let max_retries = MAX_VALIDATION_ATTEMPTS;
            let mut attempts = 0;

            while attempts < max_retries {
                trace!(target: tracing_targets::VALIDATOR, block = %candidate_id, "Attempt to validate block");
                attempts += 1;
                let dispatcher_clone = dispatcher.clone();
                let cloned_candidate = candidate_id;

                tokio::select! {
                    Ok(message) = stop_receiver.recv() => {
                        if message.block_id == cloned_candidate {
                            trace!(target: tracing_targets::VALIDATOR, "Stopping validation for block {:?}", cloned_candidate);
                            break;
                        }
                    },
                    _ = retry_interval.tick() => {
                        let validation_task_result = dispatcher_clone.enqueue_task_with_responder(
                            method_to_async_task_closure!(
                                get_validation_status,
                                session_seqno,
                                &cloned_candidate.as_short_id())
                        ).await;

                        match validation_task_result {
                            Ok(receiver) => match receiver.await.unwrap() {
                                Ok(ValidatorTaskResult::ValidationStatus(validation_status)) => {
                                    if validation_status == ValidationResult::Valid || validation_status == ValidationResult::Invalid {
                                        trace!(target: tracing_targets::VALIDATOR, "Validation status is already set for block {:?}", cloned_candidate);
                                        break;
                                    }

                                    dispatcher_clone.enqueue_task(method_to_async_task_closure!(
                                        validate_candidate,
                                        cloned_candidate,
                                        session_seqno,
                                        current_validator_pubkey
                                    )).await.expect("Failed to validate candidate");
                                },
                                Ok(e) => panic!("Unexpected response from get_validation_status: {:?}", e),
                                Err(e) => panic!("Failed to get validation status: {:?}", e),
                            },
                            Err(e) => panic!("Failed to enqueue validation task: {:?}", e),
                        }

                        if attempts >= max_retries {
                            warn!(target: tracing_targets::VALIDATOR, "Max retries reached without successful validation for block {:?}.", cloned_candidate);
                            break;
                        }
                    }
                }
            }
        });
        Ok(ValidatorTaskResult::Void)
    }

    async fn stop_candidate_validation(
        &self,
        candidate_id: BlockId,
    ) -> Result<ValidatorTaskResult> {
        self.stop_sender.send(StopMessage {
            block_id: candidate_id,
        })?;
        Ok(ValidatorTaskResult::Void)
    }

    async fn enqueue_process_new_mc_block_state(
        &self,
        _mc_state: Arc<ShardStateStuff>,
    ) -> Result<()> {
        todo!()
    }

    async fn process_candidate_signature_response(
        &mut self,
        session_seqno: u32,
        block_id_short: BlockIdShort,
        signatures: Vec<([u8; 32], [u8; 64])>,
    ) -> Result<ValidatorTaskResult> {
        // Simplified session retrieval
        let session = self
            .validation_state
            .get_mut_session(session_seqno)
            .context("failed to process_candidate_signature_response. session not found")?;

        // Check if validation status is already determined
        let validation_status = session.validation_status(&block_id_short);
        if validation_status == ValidationResult::Valid
            || validation_status == ValidationResult::Invalid
        {
            debug!(
                "Validation status is already set for block {:?}.",
                block_id_short
            );
            return Ok(ValidatorTaskResult::Void);
        }

        if let Some(block) = session.get_block(&block_id_short).cloned() {
            // Process each signature for the existing block
            for (pub_key_bytes, sig_bytes) in signatures {
                let validator_id = HashBytes(pub_key_bytes);
                let signature = Signature(sig_bytes);
                let block_validation_candidate = BlockValidationCandidate::from(block);

                let is_valid = session
                    .get_validation_session_info()
                    .validators
                    .get(&validator_id)
                    .context("validator not found")?
                    .public_key
                    .verify(block_validation_candidate.as_bytes(), &signature.0);

                session.add_signature(&block, validator_id, signature, is_valid);
            }

            match session.validation_status(&block_id_short) {
                ValidationResult::Valid => {
                    let signatures = BlockSignatures {
                        signatures: session.get_valid_signatures(&block_id_short),
                    };

                    self.on_block_validated_event(block, OnValidatedBlockEvent::Valid(signatures))
                        .await?;
                }
                ValidationResult::Invalid => {
                    self.on_block_validated_event(block, OnValidatedBlockEvent::Invalid)
                        .await?;
                }
                ValidationResult::Insufficient => {
                    debug!("Insufficient signatures for block {:?}", block_id_short);
                }
            }
        } else {
            // add signatures to cache if previous block exists
            let previous_block = BlockIdShort::from((block_id_short.shard, block_id_short.seqno));
            let previous_block = session.get_block(&previous_block);
            let blocks_count = session.blocks_count();

            if blocks_count == 0 || previous_block.is_some() {
                session.add_cached_signatures(
                    &block_id_short,
                    signatures
                        .into_iter()
                        .map(|(k, v)| (HashBytes(k), Signature(v)))
                        .collect(),
                );
            }
        }
        Ok(ValidatorTaskResult::Void)
    }

    async fn get_block_signatures(
        &mut self,
        session_seqno: u32,
        block_id_short: &BlockIdShort,
    ) -> Result<ValidatorTaskResult> {
        let session = self
            .validation_state
            .get_session(session_seqno)
            .context("session not found")?;
        let signatures = session.get_valid_signatures(block_id_short);
        Ok(ValidatorTaskResult::Signatures(signatures))
    }

    async fn validate_candidate(
        &mut self,
        candidate_id: BlockId,
        session_seqno: u32,
        current_validator_pubkey: everscale_crypto::ed25519::PublicKey,
    ) -> Result<ValidatorTaskResult> {
        let block_id_short = candidate_id.as_short_id();

        let validation_state = &self.validation_state;
        let session = validation_state
            .get_session(session_seqno)
            .ok_or(anyhow!("Session not found"))?;

        let dispatcher = self.get_dispatcher();

        let receiver = self.state_node_adapter.request_block(candidate_id).await?;

        let validators = session.validators_without_signatures(&block_id_short);

        let private_overlay = session.get_overlay().clone();

        let current_signatures = session.get_valid_signatures(&candidate_id.as_short_id());

        let network = self.network.clone();

        tokio::spawn(async move {
            if let Ok(Some(_)) = receiver.try_recv().await {
                let result = dispatcher
                    .clone()
                    .enqueue_task(method_to_async_task_closure!(
                        validate_candidate_by_block_from_bc,
                        candidate_id
                    ))
                    .await;

                if let Err(e) = result {
                    error!(err = %e, "Failed to validate block by state");
                    panic!("Failed to validate block by state {e}");
                }
            } else {
                let payload = SignaturesQuery::create(
                    session_seqno,
                    candidate_id.as_short_id(),
                    &current_signatures,
                );

                for validator in validators {
                    if validator.public_key != current_validator_pubkey {
                        trace!(target: tracing_targets::VALIDATOR, validator_pubkey=?validator.public_key.as_bytes(), "trying to send request for getting signatures from validator");
                        let response = private_overlay
                            .query(
                                network.dht_client.network(),
                                &PeerId(validator.public_key.to_bytes()),
                                Request::from_tl(payload.clone()),
                            )
                            .await;
                        match response {
                            Ok(response) => {
                                let response = response.parse_tl::<SignaturesQuery>();
                                match response {
                                    Ok(signatures) => {
                                        let enqueue_task_result = dispatcher
                                            .enqueue_task(method_to_async_task_closure!(
                                                process_candidate_signature_response,
                                                signatures.session_seqno,
                                                signatures.block_id_short,
                                                signatures.signatures
                                            ))
                                            .await;

                                        if let Err(e) = enqueue_task_result {
                                            error!(err = %e, "Failed to enqueue task for processing signatures response");
                                        }
                                    }
                                    Err(e) => {
                                        error!(err = %e, "Failed convert signatures response to SignaturesQuery");
                                    }
                                }
                            }
                            Err(e) => {
                                error!(err = %e, "Failed to get response from overlay");
                            }
                        }
                    }
                }
            }
        });
        Ok(ValidatorTaskResult::Void)
    }

    async fn get_validation_status(
        &mut self,
        session_seqno: u32,
        block_id_short: &BlockIdShort,
    ) -> Result<ValidatorTaskResult> {
        let session = self
            .validation_state
            .get_session(session_seqno)
            .context("session not found")?;
        let validation_status = session.validation_status(block_id_short);
        Ok(ValidatorTaskResult::ValidationStatus(validation_status))
    }
}

fn sign_block(key_pair: &KeyPair, block: &BlockId) -> Result<Signature> {
    let block_validation_candidate = BlockValidationCandidate::from(*block);
    let signature = Signature(key_pair.sign(block_validation_candidate.as_bytes()));
    Ok(signature)
}
