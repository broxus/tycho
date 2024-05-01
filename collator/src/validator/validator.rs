use std::mem::take;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use everscale_crypto::ed25519::KeyPair;
use everscale_types::cell::HashBytes;
use everscale_types::models::{BlockId, BlockIdShort, Signature};
use futures_util::future::join_all;
use tokio::select;
use tokio::task::{JoinError, JoinHandle};
use tracing::{debug, trace, warn};
use tycho_network::{OverlayId, PeerId, PrivateOverlay, Request};
use tycho_util::FastHashMap;

use crate::state_node::StateNodeAdapterStdImpl;
use crate::types::{BlockSignatures, OnValidatedBlockEvent, ValidatorNetwork};
use crate::validator::network::dto::SignaturesQuery;
use crate::validator::network::network_service::NetworkService;
use crate::validator::state::{SessionInfo, ValidationState, ValidationStateStdImpl};
use crate::validator::types::{
    BlockValidationCandidate, OverlayNumber, ValidationResult, ValidationSessionInfo, ValidatorInfo,
};
use crate::{
    method_to_async_task_closure,
    state_node::StateNodeAdapter,
    tracing_targets,
    utils::async_queued_dispatcher::{
        AsyncQueuedDispatcher, STANDARD_DISPATCHER_QUEUE_BUFFER_SIZE,
    },
};

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
        listeners: Vec<Arc<dyn ValidatorEventListener>>,
        state_node_adapter: Arc<ST>,
        network: ValidatorNetwork,
        keypair: KeyPair,
    ) -> Self;

    /// Enqueue block candidate validation task
    async fn validate(&self, candidate: BlockId, session_seqno: u32) -> Result<()>;
    async fn enqueue_stop_candidate_validation(&self, candidate: BlockId) -> Result<()>;

    async fn add_session(&self, validators_session_info: Arc<ValidationSessionInfo>) -> Result<()>;
    fn get_keypair(&self) -> &KeyPair;
}

#[allow(private_bounds)]
pub struct ValidatorStdImpl<ST>
where
    ST: StateNodeAdapter,
{
    _marker_state_node_adapter: std::marker::PhantomData<ST>,
    validation_state: Arc<ValidationStateStdImpl>,
    validation_semaphore: Arc<tokio::sync::Semaphore>,
    listeners: Vec<Arc<dyn ValidatorEventListener>>,
    network: ValidatorNetwork,
    state_node_adapter: Arc<ST>,
    keypair: KeyPair,
}

#[async_trait]
impl<ST> Validator<ST> for ValidatorStdImpl<ST>
where
    ST: StateNodeAdapter,
{
    fn create(
        listeners: Vec<Arc<dyn ValidatorEventListener>>,
        state_node_adapter: Arc<ST>,
        network: ValidatorNetwork,
        keypair: KeyPair,
    ) -> Self {
        tracing::info!(target: tracing_targets::VALIDATOR, "Creating validator...");

        let validation_state = Arc::new(ValidationStateStdImpl::new());

        Self {
            _marker_state_node_adapter: std::marker::PhantomData,
            validation_semaphore: Arc::new(tokio::sync::Semaphore::new(1)),
            validation_state,
            listeners,
            network,
            state_node_adapter,
            keypair,
        }
    }

    async fn validate(&self, candidate: BlockId, session_seqno: u32) -> Result<()> {
        let session = self
            .validation_state
            .get_session(session_seqno)
            .await
            .ok_or_else(|| {
                anyhow::anyhow!("Validation session not found for seqno: {}", session_seqno)
            })?
            .clone();

        start_candidate_validation(
            candidate,
            session,
            &self.keypair,
            self.listeners.clone(),
            self.network.clone(),
            self.state_node_adapter.clone(),
        )
        .await?;
        Ok(())
    }

    async fn enqueue_stop_candidate_validation(&self, _candidate: BlockId) -> Result<()> {
        Ok(())
    }

    fn get_keypair(&self) -> &KeyPair {
        &self.keypair
    }

    async fn add_session(&self, validators_session_info: Arc<ValidationSessionInfo>) -> Result<()> {
        trace!(target: tracing_targets::VALIDATOR, "Trying to add session seqno {:?}", validators_session_info.seqno);
        let (peer_resolver, local_peer_id) = {
            let network = self.network.clone();
            (
                network.clone().peer_resolver,
                network.dht_client.network().peer_id().0,
            )
        };

        let overlay_id = OverlayNumber {
            session_seqno: validators_session_info.seqno,
        };
        trace!(target: tracing_targets::VALIDATOR, overlay_id = ?validators_session_info.seqno, "Creating private overlay");
        let overlay_id = OverlayId(tl_proto::hash(overlay_id));

        let seqno = validators_session_info.seqno;

        let network_service =
            NetworkService::new(self.listeners.clone(), self.validation_state.clone(), seqno);

        let private_overlay = PrivateOverlay::builder(overlay_id)
            .with_peer_resolver(peer_resolver)
            .build(network_service.clone());

        let overlay_added = self
            .network
            .overlay_service
            .add_private_overlay(&private_overlay.clone());

        if !overlay_added {
            bail!("Failed to add private overlay");
        }

        let session_info = SessionInfo::new(
            validators_session_info.seqno,
            validators_session_info.clone(),
            private_overlay.clone(),
        );

        self.validation_state.try_add_session(session_info).await?;

        let mut entries = private_overlay.write_entries();

        for validator in validators_session_info.validators.values() {
            if validator.public_key.to_bytes() == local_peer_id {
                continue;
            }
            entries.insert(&PeerId(validator.public_key.to_bytes()));
            trace!(target: tracing_targets::VALIDATOR, validator_pubkey = ?validator.public_key.as_bytes(), "Added validator to overlay");
        }

        trace!(target: tracing_targets::VALIDATOR, "Session seqno {:?} added", validators_session_info.seqno);
        Ok(())
    }
}

fn sign_block(key_pair: &KeyPair, block: &BlockId) -> anyhow::Result<Signature> {
    let block_validation_candidate = BlockValidationCandidate::from(*block);
    let signature = Signature(key_pair.sign(block_validation_candidate.as_bytes()));
    Ok(signature)
}

async fn start_candidate_validation<ST: StateNodeAdapter>(
    block_id: BlockId,
    session: Arc<SessionInfo>,
    current_validator_keypair: &KeyPair,
    listeners: Vec<Arc<dyn ValidatorEventListener>>,
    network: ValidatorNetwork,
    state_node_adapter: Arc<ST>,
) -> Result<()> {
    let base_delay_ms = 100;
    let cancellation_token = tokio_util::sync::CancellationToken::new();
    let short_id = block_id.as_short_id();
    let our_signature = sign_block(current_validator_keypair, &block_id)?;

    session.add_block(block_id).await?;
    let current_validator_pubkey = HashBytes(current_validator_keypair.public_key.to_bytes());

    let mut initial_signatures = vec![(current_validator_pubkey.0, our_signature.0)];

    let cached_signatures = session.get_cached_signatures_by_block(&block_id.as_short_id());

    if let Some(cached_signatures) = cached_signatures {
        initial_signatures.extend(cached_signatures.1.into_iter().map(|(k, v)| (k.0, v.0)));
    }

    let is_validation_finished = process_candidate_signature_response(
        session.clone(),
        short_id,
        vec![(current_validator_pubkey.0, our_signature.0)],
        listeners.clone(),
    )
    .await?;
    trace!(target: tracing_targets::VALIDATOR, "Validation finished: {:?}", is_validation_finished);

    if is_validation_finished {
        cancellation_token.cancel(); // Cancel all tasks if validation is finished
        return Ok(());
    }

    let validators = session.validators_without_signatures(&short_id).await;
    trace!(target: tracing_targets::VALIDATOR, "Validators without signatures: {:?}", validators.len());
    let filtered_validators: Vec<Arc<ValidatorInfo>> = validators
        .iter()
        .filter(|validator| validator.public_key != current_validator_keypair.public_key)
        .cloned()
        .collect();

    let block_from_state = state_node_adapter.load_block_handle(&block_id).await?;

    if block_from_state.is_some() {
        for listener in listeners.iter() {
            let cloned_listener = listener.clone();
            tokio::spawn(async move {
                cloned_listener
                    .on_block_validated(block_id, OnValidatedBlockEvent::ValidByState)
                    .await
                    .expect("Failed to notify listener");
            });
        }

        return Ok(());
    }

    let mut handlers: Vec<JoinHandle<Result<(), anyhow::Error>>> = Vec::new();

    for validator in filtered_validators {
        let cloned_private_overlay = session.get_overlay().clone();
        let cloned_network = network.dht_client.network().clone();
        let cloned_listeners = listeners.clone();
        let cloned_session = session.clone();
        let token_clone = cancellation_token.clone();

        let handler = tokio::spawn(async move {
            let mut attempt = 0;
            loop {
                if token_clone.is_cancelled() {
                    trace!(target: tracing_targets::VALIDATOR, "Validation task cancelled");
                    return Ok(());
                }

                let already_signed = cloned_session
                    .is_validator_signed(&short_id, HashBytes(validator.public_key.to_bytes()))
                    .await;
                if already_signed {
                    trace!(target: tracing_targets::VALIDATOR, "Validator {:?} already signed", validator.public_key.to_bytes());
                    return Ok(());
                }

                let validation_finished = cloned_session
                    .get_validation_status(&short_id)
                    .await?
                    .is_finished();
                if validation_finished {
                    trace!(target: tracing_targets::VALIDATOR, "Validation is finished");
                    token_clone.cancel(); // Signal cancellation to all tasks
                    return Ok(());
                }

                let payload = SignaturesQuery::create(
                    cloned_session.get_seqno(),
                    short_id,
                    &cloned_session.get_valid_signatures(&short_id).await,
                );

                let response = tokio::time::timeout(
                    Duration::from_secs(1),
                    cloned_private_overlay.query(
                        &cloned_network,
                        &PeerId(validator.public_key.to_bytes()),
                        Request::from_tl(payload),
                    ),
                )
                .await;

                match response {
                    Ok(Ok(response)) => {
                        if let Ok(signatures) = response.parse_tl::<SignaturesQuery>() {
                            trace!(target: tracing_targets::VALIDATOR, "Received signatures from validator {:?}", validator.public_key.to_bytes());

                            let is_finished = process_candidate_signature_response(
                                cloned_session.clone(),
                                short_id,
                                signatures.signatures,
                                cloned_listeners.clone(),
                            )
                            .await?;

                            if is_finished {
                                trace!(target: tracing_targets::VALIDATOR, "Validation is finished for block {:?}", short_id);
                                token_clone.cancel();
                                return Ok(());
                            }
                        }
                    }
                    Err(e) => {
                        warn!(target: tracing_targets::VALIDATOR, "Error receiving signatures from validator {:?}: {:?}", validator.public_key.to_bytes(), e);
                        let delay = base_delay_ms * 2_u64.pow(attempt);
                        tokio::time::sleep(Duration::from_millis(delay)).await;
                        attempt += 1;
                    }
                    Ok(Err(e)) => {
                        warn!(target: tracing_targets::VALIDATOR, "Error receiving signatures from validator {:?}: {:?}", validator.public_key.to_bytes(), e);
                        let delay = base_delay_ms * 2_u64.pow(attempt);
                        tokio::time::sleep(Duration::from_millis(delay)).await;
                        attempt += 1;
                    }
                }
                tokio::time::sleep(Duration::from_millis(base_delay_ms)).await;
            }
        });

        handlers.push(handler);
    }

    let results = futures_util::future::join_all(handlers).await;
    results
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .context("One or more validation tasks failed")?;
    Ok(())
}

pub async fn process_candidate_signature_response(
    session: Arc<SessionInfo>,
    block_id_short: BlockIdShort,
    signatures: Vec<([u8; 32], [u8; 64])>,
    listeners: Vec<Arc<dyn ValidatorEventListener>>,
) -> Result<bool> {
    trace!(target: tracing_targets::VALIDATOR, block = %block_id_short, "Processing candidate signature response");
    let validation_status = session.get_validation_status(&block_id_short).await?;
    trace!(target: tracing_targets::VALIDATOR, block = %block_id_short, "Validation status: {:?}", validation_status);
    if validation_status == ValidationResult::Valid
        || validation_status == ValidationResult::Invalid
    {
        debug!(
            "Validation status is already set for block {:?}.",
            block_id_short
        );
        return Ok(true);
    }

    if session.get_block(&block_id_short).await.is_some() {
        session
            .process_signatures_and_update_status(block_id_short, signatures, listeners)
            .await?;
    } else {
        trace!(target: tracing_targets::VALIDATOR, "Caching signatures for block {:?}", block_id_short);
        if block_id_short.seqno > 0 {
            let previous_block =
                BlockIdShort::from((block_id_short.shard, block_id_short.seqno - 1));
            let previous_block = session.get_block(&previous_block).await;

            if previous_block.is_some() {
                session
                    .add_cached_signatures(
                        &block_id_short,
                        signatures
                            .into_iter()
                            .map(|(k, v)| (HashBytes(k), Signature(v)))
                            .collect(),
                    )
                    .await;
            }
        }
    }
    Ok(false)
}
