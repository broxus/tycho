use std::sync::Arc;

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use backon::{BackoffBuilder, ExponentialBuilder};
use everscale_crypto::ed25519::{KeyPair, PublicKey};
use everscale_types::cell::HashBytes;
use everscale_types::models::{BlockId, BlockIdShort, ShardIdent, Signature, ValidatorDescription};
use tokio::sync::broadcast;
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
use crate::validator::network::dto::SignaturesQueryRequest;
use crate::validator::network::network_service::NetworkService;
use crate::validator::state::{
    NotificationStatus, SessionInfo, ValidationState, ValidationStateStdImpl,
};
use crate::validator::types::{
    BlockValidationCandidate, OverlayNumber, StopValidationCommand, ValidationStatus, ValidatorInfo,
};

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
    /// Run block candidate validation task
    async fn spawn_validate(
        self: Arc<Self>,
        candidate: BlockId,
        session_seqno: u32,
    ) -> JoinHandle<Result<()>>;
    /// Validate
    async fn validate(&self, candidate: BlockId, session_seqno: u32) -> Result<()>;
    /// Stop block candidate validation task
    async fn stop_validation(&self, command: StopValidationCommand) -> Result<()>;
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
    block_validated_broadcaster: (
        broadcast::Sender<StopValidationCommand>,
        broadcast::Receiver<StopValidationCommand>,
    ),
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

        let block_validated_broadcaster = broadcast::channel(100);

        Self {
            validation_state,
            listeners,
            network,
            state_node_adapter,
            keypair,
            config,
            block_validated_broadcaster,
        }
    }
}

impl Validator for ValidatorStdImpl {
    async fn spawn_validate(
        self: Arc<Self>,
        candidate: BlockId,
        session_seqno: u32,
    ) -> JoinHandle<Result<()>> {
        let validator = Arc::clone(&self);
        tokio::spawn(
            async move { Validator::validate(&*validator, candidate, session_seqno).await },
        )
    }

    #[tracing::instrument(skip(self), fields(%block_id, %session_seqno))]
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

        let process_signatures_result = process_new_signatures(
            session.clone(),
            block_short_id,
            initial_signatures,
            &self.listeners,
        )
        .await?;

        if process_signatures_result.0.is_finished() {
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
            self.listeners.clone(),
            cancellation_token,
            Arc::new(self.config.clone()),
            block_short_id,
            self.block_validated_broadcaster.0.clone(),
            our_signature,
        )
        .await;

        let results = futures_util::future::join_all(handlers).await;
        results
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .context("One or more validation tasks failed")?;

        Ok(())
    }

    #[tracing::instrument(skip(self), fields(?command))]
    async fn stop_validation(&self, command: StopValidationCommand) -> Result<()> {
        self.block_validated_broadcaster.0.send(command)?;
        Ok(())
    }

    #[tracing::instrument(skip(self, validators_descriptions), fields(%session_seqno, ?shard_ident))]
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

        let network_service = NetworkService::new(
            self.listeners.clone(),
            validation_state.clone(),
            self.block_validated_broadcaster.0.clone(),
        );

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
                let retry_client = RetryClient::new(
                    Arc::new(validator_client),
                    config.error_backoff_config.clone(),
                );

                validator_clients.insert(*validator_pubkey, Arc::new(retry_client));
            }
        }

        let session_info =
            SessionInfo::new(shard_ident, session_seqno, validators, validator_clients);

        self.validation_state.try_add_session(session_info).await?;

        tracing::info!(target: tracing_targets::VALIDATOR, "Validation session added");
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

#[tracing::instrument(skip(session, block_id_short, signatures, listeners), fields(%block_id_short))]
pub async fn process_new_signatures(
    session: Arc<SessionInfo>,
    block_id_short: BlockIdShort,
    signatures: Vec<(HashBytes, Signature)>,
    listeners: &[Arc<dyn ValidatorEventListener>],
) -> Result<(ValidationStatus, NotificationStatus)> {
    if session.get_block(&block_id_short).is_some() {
        session
            .add_signatures(block_id_short, signatures.clone())
            .await?;

        let validation_status = session.check_validation_status(&block_id_short)?;

        if validation_status.is_finished() {
            let notification_status =
                session.notify_listeners_if_not(block_id_short, &validation_status, listeners)?;
            return Ok((validation_status, notification_status));
        }

        Ok((validation_status, NotificationStatus::NotNotified))
    } else {
        if block_id_short.seqno > 0 {
            let previous_block =
                BlockIdShort::from((block_id_short.shard, block_id_short.seqno - 1));
            let previous_block = session.get_block(&previous_block);

            if previous_block.is_some() {
                tracing::trace!(target: tracing_targets::VALIDATOR, "Caching signatures for block");
                session.cache_signatures(&block_id_short, signatures);
            }
        }
        Ok((
            ValidationStatus::BlockNotExist,
            NotificationStatus::NotNotified,
        ))
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
    if let Some(cached_signatures) = session
        .take_cached_signatures(&block_id.as_short_id())
        .await
    {
        initial_signatures.extend(cached_signatures.into_iter());
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
            tokio::spawn({
                let listener = listener.clone();
                let block_id = *block_id;
                async move {
                    listener
                        .on_block_validated(block_id, OnValidatedBlockEvent::ValidByState)
                        .await
                        .expect("Failed to notify listener");
                }
            });
        }
        return Ok(true);
    }
    Ok(false)
}

#[allow(clippy::too_many_arguments)]
#[tracing::instrument(skip(validators, session, listeners, cancellation_token, config), fields(%block_short_id))]
async fn spawn_validation_tasks(
    validators: Vec<Arc<ValidatorInfo>>,
    session: Arc<SessionInfo>,
    listeners: Vec<Arc<dyn ValidatorEventListener>>,
    cancellation_token: CancellationToken,
    config: Arc<ValidatorConfig>,
    block_short_id: BlockIdShort,
    on_block_validated_event_sender: broadcast::Sender<StopValidationCommand>,
    self_signature: Signature,
) -> Vec<JoinHandle<Result<()>>> {
    tracing::info!(target: tracing_targets::VALIDATOR, "Spawning validation tasks");

    // Create a task for each validator
    validators.into_iter().map(|validator| {
        let mut subscriber = on_block_validated_event_sender.subscribe();
        let session_clone = Arc::clone(&session);
        let validator_client = session_clone.get_validator_client(&validator.public_key_hash)
            .unwrap_or_else(|| panic!("Validator client not found for validator: {:?}", validator.public_key_hash));

        let cancellation_token_clone = cancellation_token.clone();

        // Spawn a task to listen for block validation events
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    result = subscriber.recv() => {
                        match result{
                            Ok(comand) => {

                                match comand {
                                    StopValidationCommand::ByBlock(block_id) => {
                                        if block_id == block_short_id {
                                            tracing::trace!(target: tracing_targets::VALIDATOR, "Received block validation stop event ByBlock");
                                            cancellation_token_clone.cancel();
                                            return;
                                        }
                                    }
                                    StopValidationCommand::ByTopShardBlock(top_shard_block) => {
                                        if top_shard_block.shard == block_short_id.shard && top_shard_block.seqno >= block_short_id.seqno {
                                            tracing::trace!(target: tracing_targets::VALIDATOR, "Received block validation stop event ByTopShardBlock");
                                            cancellation_token_clone.cancel();
                                            return;
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::error!(target: tracing_targets::VALIDATOR, error = ?e, "Error receiving from subscriber");
                                return;
                            }
                        }
                    },
                    _ = cancellation_token_clone.cancelled() => {
                        return;
                    },
                };
            }
        });

        // Spawn a task to request and process signatures
        let cancellation_token_clone = cancellation_token.clone();
        tokio::spawn(
            {
                let listeners = listeners.clone();
                let config = config.clone();
                let request_timeout = config.request_timeout;
                async move {
                    let mut backoff = ExponentialBuilder::default()
                        .with_min_delay(config.request_signatures_backoff_config.min_delay)
                        .with_max_delay(config.request_signatures_backoff_config.max_delay)
                        .with_factor(config.request_signatures_backoff_config.factor)
                        .with_max_times(config.request_signatures_backoff_config.max_times)
                        .build();


                    while !cancellation_token_clone.is_cancelled() {
                        tracing::trace!(target: tracing_targets::VALIDATOR, "Requesting signatures");

                        // Check if the block is already signed by the validator
                        if session_clone.is_block_signed_by_validator(&block_short_id, validator.public_key_hash).await {
                            break;
                        }

                        // Create query payload
                        let query_payload = SignaturesQueryRequest::new(session_clone.seqno(), block_short_id, self_signature);

                        // Request signatures with retry
                        let result = tokio::select! {
                        result = validator_client.execute_with_retry(move |client: Arc<ValidatorClient>| {
                            let query_payload_clone = query_payload.clone();
                            async move {
                                client.request_signatures(query_payload_clone, request_timeout).await
                            }
                        }) => result,
                        _ = cancellation_token_clone.cancelled() => {
                            tracing::trace!(target: tracing_targets::VALIDATOR, "Validation task cancelled");
                            break;
                        },
                    }?;

                        // Process new signatures
                        let process_new_signatures_result = process_new_signatures(
                            session_clone.clone(),
                            block_short_id,
                            result.wrapped_signatures(),
                            &listeners,
                        ).await?;

                        // Check if validation is finished
                        if process_new_signatures_result.0.is_finished() {
                            tracing::trace!(target: tracing_targets::VALIDATOR, "Send message to stop validation tasks");
                            cancellation_token_clone.cancel();
                            break;
                        }

                        let delay = backoff.next().unwrap_or_else(|| config.request_signatures_backoff_config.max_delay);
                        tracing::trace!(target: tracing_targets::VALIDATOR, "Backing off for {:?}", delay);
                        tokio::time::sleep(delay).await;
                    }

                    Ok(())
                }
            })

    }
        ).collect()
}
