use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, bail, Result};
use everscale_types::cell::HashBytes;
use everscale_types::models::{BlockId, BlockIdShort, ShardIdent, Signature};
use tokio::sync::RwLock;
use tycho_util::{FastDashMap, FastHashMap};

use crate::tracing_targets;
use crate::types::{BlockSignatures, OnValidatedBlockEvent};
use crate::validator::client::retry::RetryClient;
use crate::validator::client::ValidatorClient;
use crate::validator::types::{BlockValidationCandidate, ValidationStatus, ValidatorInfo};
use crate::validator::ValidatorEventListener;

#[derive(Eq, PartialEq)]
pub enum NotificationStatus {
    NotNotified,
    Notified,
}

impl NotificationStatus {
    pub fn is_notified(&self) -> bool {
        matches!(self, Self::Notified)
    }
}

struct SignatureMaps {
    valid_signatures: FastHashMap<HashBytes, Signature>,
    event_dispatched: bool,
}

/// Represents the state of validation for blocks and sessions.
pub trait ValidationState: Send + Sync {
    /// Creates a new instance of a type implementing `ValidationState`.
    fn new() -> Self;

    /// Adds a new validation session.
    fn try_add_session(
        &self,
        session: Arc<SessionInfo>,
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    /// Retrieves an immutable reference to a session by its ID.
    fn get_session(
        &self,
        shard_ident: ShardIdent,
        session_id: u32,
    ) -> impl std::future::Future<Output = Option<Arc<SessionInfo>>> + Send;
}

/// Holds information about a validation session.
pub struct SessionInfo {
    shard_ident: ShardIdent,
    seqno: u32,
    max_weight: u64,
    blocks_signatures: FastDashMap<BlockIdShort, (BlockId, SignatureMaps)>,
    cached_signatures: FastDashMap<BlockIdShort, FastHashMap<HashBytes, Signature>>,
    clients: FastHashMap<HashBytes, Arc<RetryClient<ValidatorClient>>>,
    validators: FastHashMap<HashBytes, Arc<ValidatorInfo>>,
}

impl SessionInfo {
    pub fn new(
        shard_ident: ShardIdent,
        seqno: u32,
        validators: FastHashMap<HashBytes, Arc<ValidatorInfo>>,
        clients: FastHashMap<HashBytes, Arc<RetryClient<ValidatorClient>>>,
    ) -> Arc<SessionInfo> {
        let max_weight = validators.values().map(|vi| vi.weight).sum();

        Arc::new(Self {
            shard_ident,
            seqno,
            max_weight,
            blocks_signatures: Default::default(),
            cached_signatures: Default::default(),
            validators,
            clients,
        })
    }

    pub fn shard_ident(&self) -> &ShardIdent {
        &self.shard_ident
    }

    pub fn seqno(&self) -> u32 {
        self.seqno
    }

    pub fn get_validator_client(
        &self,
        validator_id: &HashBytes,
    ) -> Option<Arc<RetryClient<ValidatorClient>>> {
        self.clients.get(validator_id).cloned()
    }

    /// Retrieves a validator by its ID.
    pub async fn take_cached_signatures(
        &self,
        block_id_short: &BlockIdShort,
    ) -> Option<FastHashMap<HashBytes, Signature>> {
        let remove_result = self.cached_signatures.remove(block_id_short);
        remove_result.map(|(_, v)| v)
    }

    /// Checks if a block is signed by a validator.
    pub async fn is_block_signed_by_validator(
        &self,
        block_id_short: &BlockIdShort,
        validator_id: HashBytes,
    ) -> bool {
        if let Some(ref_data) = self.blocks_signatures.get(block_id_short) {
            ref_data.1.valid_signatures.contains_key(&validator_id)
        } else {
            false
        }
    }

    /// Adds a block to the session.
    pub async fn add_block(&self, block: BlockId) -> Result<()> {
        let block_header = block.as_short_id();

        self.blocks_signatures
            .entry(block_header)
            .or_insert_with(|| {
                (block, SignatureMaps {
                    valid_signatures: FastHashMap::default(),
                    event_dispatched: false,
                })
            });
        Ok(())
    }

    /// Retrieves a block by its short ID.
    pub fn get_block(&self, block_id_short: &BlockIdShort) -> Option<BlockId> {
        self.blocks_signatures
            .get(block_id_short)
            .map(|ref_data| ref_data.0)
    }

    /// Determines the validation status of a block.
    pub fn validation_status(&self, block_id_short: &BlockIdShort) -> Result<ValidationStatus> {
        let signatures = self.blocks_signatures.get(block_id_short);

        if let Some(ref_data) = signatures {
            Ok(self.calc_validation_status(&ref_data.1))
        } else {
            Ok(ValidationStatus::Insufficient(0, 0))
        }
    }

    /// Lists validators without signatures for a given block.
    pub fn validators_without_signatures(
        &self,
        block_id_short: &BlockIdShort,
    ) -> Vec<Arc<ValidatorInfo>> {
        if let Some(ref_data) = self.blocks_signatures.get(block_id_short) {
            let validators_with_signatures: std::collections::HashSet<_> =
                ref_data.1.valid_signatures.keys().collect();

            self.validators
                .iter()
                .filter_map(|(id, info)| {
                    if !validators_with_signatures.contains(id) {
                        Some(info.clone())
                    } else {
                        None
                    }
                })
                .collect()
        } else {
            self.validators.values().cloned().collect()
        }
    }

    /// Adds cached signatures for a block.
    pub fn cache_signatures(
        &self,
        block_id_short: &BlockIdShort,
        signatures: Vec<(HashBytes, Signature)>,
    ) {
        self.cached_signatures
            .insert(*block_id_short, signatures.into_iter().collect());
    }

    /// Retrieves valid signatures for a block.
    pub fn get_valid_signatures(
        &self,
        block_id_short: &BlockIdShort,
    ) -> Vec<(HashBytes, Signature)> {
        if let Some(ref_data) = self.blocks_signatures.get(block_id_short) {
            ref_data
                .1
                .valid_signatures
                .iter()
                .map(|(k, v)| (*k, *v))
                .collect()
        } else {
            Vec::default()
        }
    }

    /// Verifies and adds the signatures and updates the validation status.
    #[tracing::instrument(skip(self, signatures), fields(block_id_short))]
    pub async fn add_signatures(
        &self,
        block_id_short: BlockIdShort,
        signatures: Vec<(HashBytes, Signature)>,
    ) -> Result<()> {
        tracing::trace!(target: tracing_targets::VALIDATOR, block_id_short=%block_id_short, "Adding signatures");

        let mut to_verify = Vec::new();

        {
            let entry = self
                .blocks_signatures
                .entry(block_id_short)
                .or_insert_with(|| {
                    (BlockId::default(), SignatureMaps {
                        valid_signatures: FastHashMap::default(),
                        event_dispatched: false,
                    })
                });

            for (validator_id, signature) in &signatures {
                if entry.1.valid_signatures.contains_key(validator_id) {
                    continue;
                }

                let block_validation_candidate = BlockValidationCandidate::from(entry.0);

                if let Some(validator) = self.validators.get(validator_id) {
                    to_verify.push((
                        *validator_id,
                        validator.public_key,
                        block_validation_candidate,
                        *signature,
                    ));
                } else {
                    tracing::warn!(target: tracing_targets::VALIDATOR, validator_id=%validator_id, "Validator not found");
                }
            }
        }

        for (validator_id, public_key, block_validation_candidate, signature) in to_verify {
            let valid_signature = tokio::task::spawn_blocking({
                let block_bytes = block_validation_candidate.as_bytes();
                move || public_key.verify(block_bytes, &signature.0)
            })
            .await?;

            if valid_signature {
                tracing::trace!(target: tracing_targets::VALIDATOR, validator_id=%validator_id, "Valid signature");
                self.blocks_signatures
                    .entry(block_id_short)
                    .and_modify(|entry| {
                        entry.1.valid_signatures.insert(validator_id, signature);
                    });
            } else {
                tracing::warn!(target: tracing_targets::VALIDATOR, validator_id=%validator_id, "Invalid signature");
            }
        }

        Ok(())
    }

    #[tracing::instrument(skip(self), fields(block_id_short))]
    pub fn check_validation_status(
        &self,
        block_id_short: &BlockIdShort,
    ) -> Result<ValidationStatus> {
        tracing::trace!(target: tracing_targets::VALIDATOR, "Checking validation status");

        let block_signatures = self
            .blocks_signatures
            .get(block_id_short)
            .ok_or(anyhow!("Block not found"))?;

        let validation_status = self.calc_validation_status(&block_signatures.1);

        Ok(validation_status)
    }

    #[tracing::instrument(skip(self, listeners), fields(block_id_short))]
    pub fn notify_listeners_if_not(
        &self,
        block_id_short: BlockIdShort,
        validation_status: &ValidationStatus,
        listeners: &[Arc<dyn ValidatorEventListener>],
    ) -> Result<NotificationStatus> {
        {
            let mut blocks_signatures_guard = self
                .blocks_signatures
                .get_mut(&block_id_short)
                .ok_or_else(|| {
                    anyhow::anyhow!("Block signatures not found for block: {:?}", block_id_short)
                })?;

            if blocks_signatures_guard.1.event_dispatched {
                return Ok(NotificationStatus::NotNotified);
            }
            blocks_signatures_guard.1.event_dispatched = true;
        }

        // notify listeners without holding the lock
        let block_signatures = self.blocks_signatures.get(&block_id_short).ok_or_else(|| {
            anyhow::anyhow!("Block signatures not found for block: {:?}", block_id_short)
        })?;

        let event = match validation_status {
            ValidationStatus::Valid => OnValidatedBlockEvent::Valid(BlockSignatures {
                signatures: block_signatures.1.valid_signatures.clone(),
            }),
            ValidationStatus::Insufficient(..) | ValidationStatus::BlockNotExist => unreachable!(),
        };

        Self::notify_listeners(block_signatures.0, event, listeners);

        Ok(NotificationStatus::Notified)
    }

    /// Calculates the validation status of a block.
    fn calc_validation_status(&self, signature_maps: &SignatureMaps) -> ValidationStatus {
        fn calculate_total_weight<'a>(
            validator_ids: impl Iterator<Item = &'a HashBytes>,
            validators: &FastHashMap<HashBytes, Arc<ValidatorInfo>>,
        ) -> u64 {
            validator_ids
                .map(|validator_id| validators.get(validator_id).map_or(0, |vi| vi.weight))
                .sum()
        }

        let total_valid_weight =
            calculate_total_weight(signature_maps.valid_signatures.keys(), &self.validators);

        let valid_weight_threshold = self.max_weight * 2 / 3 + 1;

        if total_valid_weight >= valid_weight_threshold {
            ValidationStatus::Valid
        } else {
            ValidationStatus::Insufficient(total_valid_weight, valid_weight_threshold)
        }
    }

    /// Notifies listeners about block validation.
    /// This function spawns a new task for each listener.
    fn notify_listeners(
        block: BlockId,
        event: OnValidatedBlockEvent,
        listeners: &[Arc<dyn ValidatorEventListener>],
    ) {
        tracing::trace!(target: tracing_targets::VALIDATOR, "Notifying listeners about block validation");
        for listener in listeners {
            tokio::spawn({
                let listener = listener.clone();
                let event = event.clone();
                async move {
                    listener
                        .on_block_validated(block, event)
                        .await
                        .expect("Failed to notify listener");
                }
            });
        }
    }
}

/// Standard implementation of `ValidationState`.
pub struct ValidationStateStdImpl {
    sessions: RwLock<HashMap<(ShardIdent, u32), Arc<SessionInfo>>>,
}

impl ValidationState for ValidationStateStdImpl {
    fn new() -> Self {
        Self {
            sessions: Default::default(),
        }
    }

    /// Adds a new validation session.
    /// Fails if a session with the same ID already exists.
    #[tracing::instrument(skip(self, session), fields(session_id = session.seqno(), shard_ident = ?session.shard_ident))]
    async fn try_add_session(&self, session: Arc<SessionInfo>) -> Result<()> {
        tracing::info!(target: tracing_targets::VALIDATOR, "Saving new session");
        let shard_ident = session.shard_ident;
        let seqno = session.seqno;

        let session = self
            .sessions
            .write()
            .await
            .insert((shard_ident, seqno), session);

        if session.is_some() {
            bail!("Session already exists with seqno: ({shard_ident}, {seqno})");
        }

        Ok(())
    }

    /// Retrieves an immutable reference to a session by its ID.
    async fn get_session(
        &self,
        shard_ident: ShardIdent,
        session_id: u32,
    ) -> Option<Arc<SessionInfo>> {
        self.sessions
            .read()
            .await
            .get(&(shard_ident, session_id))
            .cloned()
    }
}
