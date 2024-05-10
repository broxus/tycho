use std::sync::Arc;

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use everscale_crypto::ed25519::{KeyPair, PublicKey};
use everscale_types::cell::HashBytes;
use everscale_types::models::{BlockId, BlockIdShort, ShardIdent, Signature, ValidatorDescription};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tycho_network::{OverlayId, PeerId, PrivateOverlay};
use tycho_util::FastHashMap;

use crate::state_node::StateNodeAdapter;
use crate::tracing_targets;
use crate::types::{OnValidatedBlockEvent, ValidatorNetwork};
use crate::validator::client::retry::RetryClient;
use crate::validator::client::ValidatorClient;
use crate::validator::config::ValidatorConfig;
use crate::validator::network::dto::SignaturesQuery;
use crate::validator::network::network_service::NetworkService;
use crate::validator::state::{SessionInfo, ValidationState, ValidationStateStdImpl};
use crate::validator::types::{BlockValidationCandidate, OverlayNumber, ValidatorInfo};

// FACTORY

pub struct ValidatorContext {
    pub listeners: Vec<Arc<dyn ValidatorEventListener>>,
    pub state_node_adapter: Arc<dyn StateNodeAdapter>,
    pub keypair: Arc<KeyPair>,
}

pub trait ValidatorFactory {
    type Validator: Validator;

    fn create(&self, cx: ValidatorContext) -> Self::Validator;
}

impl<F, R> ValidatorFactory for F
where
    F: Fn(ValidatorContext) -> R,
    R: Validator,
{
    type Validator = R;

    fn create(&self, cx: ValidatorContext) -> Self::Validator {
        self(cx)
    }
}

#[trait_variant::make(ValidatorEventEmitter: Send)]
pub trait ValidatorEventEmitterInternal {
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

#[trait_variant::make(Validator: Send)]
pub trait ValidatorInternal: Sync + 'static {
    /// Enqueue block candidate validation task
    async fn validate(&self, candidate: BlockId, session_seqno: u32) -> Result<()>;
    /// Stop block candidate validation task
    async fn stop_validation(&self, candidate: BlockId) -> Result<()>;
    /// Add new validation session
    async fn add_session(
        &self,
        shard_ident: ShardIdent,
        session_seqno: u32,
        validators_descriptions: &[ValidatorDescription],
    ) -> Result<()>;
    /// Get validator keypair
    fn keypair(&self) -> Arc<KeyPair>;
}

pub struct ValidatorStdImplFactory {
    pub network: ValidatorNetwork,
    pub config: ValidatorConfig,
}

impl ValidatorFactory for ValidatorStdImplFactory {
    type Validator = ValidatorStdImpl;

    fn create(&self, cx: ValidatorContext) -> Self::Validator {
        ValidatorStdImpl::new(
            cx.listeners,
            cx.state_node_adapter,
            Arc::new(self.network.clone()),
            cx.keypair,
            self.config.clone(),
        )
    }
}

pub struct ValidatorStdImpl {
    validation_state: Arc<ValidationStateStdImpl>,
    listeners: Vec<Arc<dyn ValidatorEventListener>>,
    network: Arc<ValidatorNetwork>,
    state_node_adapter: Arc<dyn StateNodeAdapter>,
    keypair: Arc<KeyPair>,
    config: ValidatorConfig,
}

impl ValidatorStdImpl {
    /// Create new validator
    #[tracing::instrument(skip(listeners, state_node_adapter, network, keypair, config))]
    pub fn new(
        listeners: Vec<Arc<dyn ValidatorEventListener>>,
        state_node_adapter: Arc<dyn StateNodeAdapter>,
        network: Arc<ValidatorNetwork>,
        keypair: Arc<KeyPair>,
        config: ValidatorConfig,
    ) -> Self {
        tracing::info!(target: tracing_targets::VALIDATOR, "Creating validator");
        let validation_state = Arc::new(ValidationStateStdImpl::new());

        Self {
            validation_state,
            listeners,
            network,
            state_node_adapter,
            keypair,
            config,
        }
    }
}

impl Validator for ValidatorStdImpl {
    #[tracing::instrument(skip(self), fields(block_id, session_seqno))]
    async fn validate(&self, block_id: BlockId, session_seqno: u32) -> Result<()> {
        tracing::info!(target: tracing_targets::VALIDATOR, "Validating block");
        let session = self
            .validation_state
            .get_session(block_id.shard, session_seqno)
            .await
            .ok_or_else(|| {
                anyhow::anyhow!("Validation session not found for seqno: {}", session_seqno)
            })?
            .clone();

        let cancellation_token = tokio_util::sync::CancellationToken::new();
        let block_short_id = block_id.as_short_id();

        let our_signature = sign_block(&self.keypair, &block_id)?;

        session.add_block(block_id).await?;

        let initial_signatures = prepare_initial_signatures(
            &session,
            &Validator::keypair(self).public_key,
            our_signature,
            &block_id,
        )
        .await?;

        let validation_is_finished = process_new_signatures(
            session.clone(),
            block_short_id,
            initial_signatures,
            &self.listeners,
        )
        .await?;

        if validation_is_finished {
            cancellation_token.cancel();
            return Ok(());
        }

        // Check if block is already validated by state
        if check_and_notify_validated_by_state(&self.state_node_adapter, &block_id, &self.listeners)
            .await?
        {
            tracing::info!(target: tracing_targets::VALIDATOR, "Block is already validated by state");
            return Ok(());
        }

        let validators_without_signature = session
            .validators_without_signatures(&block_short_id)
            .iter()
            .filter(|validator| validator.public_key_hash.0 != self.keypair.public_key.to_bytes())
            .cloned()
            .collect();

        let handlers = spawn_validation_tasks(
            validators_without_signature,
            session,
            &self.listeners,
            cancellation_token,
            &self.config,
            block_short_id,
        )
        .await;

        let results = futures_util::future::join_all(handlers).await;
        results
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .context("One or more validation tasks failed")?;

        Ok(())
    }

    #[tracing::instrument(skip(self), fields(candidate = %_candidate))]
    async fn stop_validation(&self, _candidate: BlockId) -> Result<()> {
        tracing::warn!(target: tracing_targets::VALIDATOR, "Stop validation is not implemented");
        Ok(())
    }

    #[tracing::instrument(skip(self, validators_descriptions), fields(session_seqno = session_seqno, shard_ident = %shard_ident))]
    async fn add_session(
        &self,
        shard_ident: ShardIdent,
        session_seqno: u32,
        validators_descriptions: &[ValidatorDescription],
    ) -> Result<()> {
        tracing::info!(target: tracing_targets::VALIDATOR, "Adding validation session");

        let (peer_resolver, local_peer_id) = {
            (
                self.network.peer_resolver.clone(),
                self.network.dht_client.network().peer_id(),
            )
        };

        let network = self.network.clone();
        let validation_state = self.validation_state.clone();
        let config = self.config.clone();

        let overlay_id = {
            let overlay_id = OverlayNumber {
                shard_ident,
                session_seqno,
            };

            OverlayId(tl_proto::hash(overlay_id))
        };

        let network_service = NetworkService::new(self.listeners.clone(), validation_state.clone());

        let private_overlay = PrivateOverlay::builder(overlay_id)
            .with_peer_resolver(peer_resolver)
            .build(network_service.clone());

        let is_overlay_added = network
            .overlay_service
            .add_private_overlay(&private_overlay);

        if !is_overlay_added {
            tracing::warn!(target: tracing_targets::VALIDATOR, "Failed to add private overlay");
            bail!("Failed to add private overlay");
        }

        let mut validators = FastHashMap::default();
        let mut validator_clients = FastHashMap::default();
        for validator_description in validators_descriptions.iter() {
            let validator_info = ValidatorInfo::try_from(validator_description)?;
            validators.insert(validator_info.public_key_hash, Arc::new(validator_info));
        }

        {
            let mut entries = private_overlay.write_entries();

            for validator_pubkey in validators.keys() {
                if validator_pubkey.0 == local_peer_id.0 {
                    continue;
                }

                let peer_id = PeerId(validator_pubkey.0);
                entries.insert(&peer_id);

                let validator_client = ValidatorClient::new(
                    network.dht_client.network().clone(),
                    private_overlay.clone(),
                    peer_id,
                );
                let retry_client =
                    RetryClient::new(Arc::new(validator_client), config.backoff_config.clone());

                validator_clients.insert(*validator_pubkey, Arc::new(retry_client));
            }
        }

        let session_info =
            SessionInfo::new(shard_ident, session_seqno, validators, validator_clients);

        self.validation_state.try_add_session(session_info).await?;

        tracing::info!(target: tracing_targets::VALIDATOR, session_seqno, %shard_ident, "Validation session added");
        Ok(())
    }

    fn keypair(&self) -> Arc<KeyPair> {
        self.keypair.clone()
    }
}

fn sign_block(key_pair: &KeyPair, block: &BlockId) -> anyhow::Result<Signature> {
    let block_validation_candidate = BlockValidationCandidate::from(*block);
    let signature = Signature(key_pair.sign(block_validation_candidate.as_bytes()));
    Ok(signature)
}

#[tracing::instrument(skip(session, block_id_short, signatures, listeners), fields(block_id = %block_id_short))]
pub async fn process_new_signatures(
    session: Arc<SessionInfo>,
    block_id_short: BlockIdShort,
    signatures: Vec<(HashBytes, Signature)>,
    listeners: &[Arc<dyn ValidatorEventListener>],
) -> Result<bool> {
    if session.get_block(&block_id_short).await.is_some() {
        session.add_signatures(block_id_short, signatures.clone())?;
        session
            .check_validation_and_notify(block_id_short, listeners)
            .await
    } else {
        tracing::debug!(target: tracing_targets::VALIDATOR, "Caching signatures for block");
        if block_id_short.seqno > 0 {
            let previous_block =
                BlockIdShort::from((block_id_short.shard, block_id_short.seqno - 1));
            let previous_block = session.get_block(&previous_block).await;

            if previous_block.is_some() {
                session.cache_signatures(&block_id_short, signatures).await;
            }
        }
        Ok(false)
    }
}

/// Prepare initial signatures for block validation
/// Returns list of signatures for block validation
async fn prepare_initial_signatures(
    session: &Arc<SessionInfo>,
    current_validator_pubkey: &PublicKey,
    our_signature: Signature,
    block_id: &BlockId,
) -> Result<Vec<(HashBytes, Signature)>> {
    let mut initial_signatures = vec![(
        HashBytes(current_validator_pubkey.to_bytes()),
        our_signature,
    )];
    if let Some(cached_signatures) = session.take_cached_signatures(&block_id.as_short_id()) {
        initial_signatures.extend(cached_signatures.1.into_iter());
    }
    Ok(initial_signatures)
}

/// Check if block is already validated by state
/// If block is validated by state, notify listeners
async fn check_and_notify_validated_by_state(
    state_node_adapter: &Arc<dyn StateNodeAdapter>,
    block_id: &BlockId,
    listeners: &[Arc<dyn ValidatorEventListener>],
) -> Result<bool> {
    if state_node_adapter
        .load_block_handle(block_id)
        .await?
        .is_some()
    {
        for listener in listeners.iter() {
            let cloned_listener = listener.clone();
            let cloned_block_id = *block_id;
            tokio::spawn(async move {
                cloned_listener
                    .on_block_validated(cloned_block_id, OnValidatedBlockEvent::ValidByState)
                    .await
                    .expect("Failed to notify listener");
            });
        }
        return Ok(true);
    }
    Ok(false)
}

#[tracing::instrument(skip(validators, session, listeners, cancellation_token, config), fields(block_id = %block_short_id))]
async fn spawn_validation_tasks(
    validators: Vec<Arc<ValidatorInfo>>,
    session: Arc<SessionInfo>,
    listeners: &[Arc<dyn ValidatorEventListener>],
    cancellation_token: CancellationToken,
    config: &ValidatorConfig,
    block_short_id: BlockIdShort,
) -> Vec<JoinHandle<Result<()>>> {
    tracing::info!(target: tracing_targets::VALIDATOR, "Spawning validation tasks");
    validators.into_iter().map(|validator| {
        let session_clone = Arc::clone(&session);
        let validator_client = session_clone.get_validator_client(&validator.public_key_hash)
            .unwrap_or_else(|| panic!("Validator client not found for validator: {:?}", validator.public_key_hash));

        let cancellation_token_clone = cancellation_token.clone();
        let request_timeout = config.request_timeout;
        let delay_between_requests = config.delay_between_requests;
        let listeners_clone = listeners.to_vec();

        tokio::spawn(async move {
            while !cancellation_token_clone.is_cancelled() {
                if session_clone.is_block_signed_by_validator(&block_short_id, validator.public_key_hash) {
                    break;
                }

                let query_payload = create_query_payload_for_validator(&session_clone, block_short_id).await;

                let result = tokio::select! {
                    result = validator_client.execute_with_retry(move |client: Arc<ValidatorClient>| {
                        let query_payload_clone = query_payload.clone();
                        async move {
                            client.request_signatures(query_payload_clone, request_timeout).await
                        }
                    }) => result,
                    _ = cancellation_token_clone.cancelled() => {
                        break;
                    },
                }?;

                let is_finished = process_new_signatures(
                    session_clone.clone(),
                    block_short_id,
                    result.wrapped_signatures(),
                    &listeners_clone,
                ).await?;

                if is_finished {
                    cancellation_token_clone.cancel();
                    break;
                }

                tokio::time::sleep(delay_between_requests).await;
            }

            Ok(())
        })
    }).collect()
}

async fn create_query_payload_for_validator(
    session: &Arc<SessionInfo>,
    block_short_id: BlockIdShort,
) -> SignaturesQuery {
    let seq_no = session.seqno();
    let valid_signatures = session.get_valid_signatures(&block_short_id);
    SignaturesQuery::new(seq_no, block_short_id, valid_signatures)
}
