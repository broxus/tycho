use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, bail, Result};
use everscale_types::cell::HashBytes;
use everscale_types::models::{BlockId, BlockIdShort, ShardIdent, Signature};
use tokio::sync::{Mutex, RwLock};
use tycho_util::{FastDashMap, FastHashMap};

use crate::tracing_targets;
use crate::types::{BlockSignatures, OnValidatedBlockEvent};
use crate::validator::client::retry::RetryClient;
use crate::validator::client::ValidatorClient;
use crate::validator::types::{BlockValidationCandidate, ValidationResult, ValidatorInfo};
use crate::validator::ValidatorEventListener;

struct SignatureMaps {
    valid_signatures: FastHashMap<HashBytes, Signature>,
    invalid_signatures: FastHashMap<HashBytes, Signature>,
    event_dispatched: Mutex<bool>,
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
    pub fn take_cached_signatures(
        &self,
        block_id_short: &BlockIdShort,
    ) -> Option<(BlockIdShort, FastHashMap<HashBytes, Signature>)> {
        self.cached_signatures.remove(block_id_short)
    }

    /// Checks if a block is signed by a validator.
    pub fn is_block_signed_by_validator(
        &self,
        block_id_short: &BlockIdShort,
        validator_id: HashBytes,
    ) -> bool {
        if let Some(ref_data) = self.blocks_signatures.get(block_id_short) {
            ref_data.1.valid_signatures.contains_key(&validator_id)
                || ref_data.1.invalid_signatures.contains_key(&validator_id)
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
                    invalid_signatures: FastHashMap::default(),
                    event_dispatched: Mutex::new(false),
                })
            });
        Ok(())
    }

    /// Retrieves a block by its short ID.
    pub async fn get_block(&self, block_id_short: &BlockIdShort) -> Option<BlockId> {
        self.blocks_signatures
            .get(block_id_short)
            .map(|ref_data| ref_data.0)
    }

    /// Determines the validation status of a block.
    pub async fn validation_status(
        &self,
        block_id_short: &BlockIdShort,
    ) -> Result<ValidationResult> {
        let signatures = self.blocks_signatures.get(block_id_short);

        if let Some(ref_data) = signatures {
            Ok(self.calc_validation_status(&ref_data.1))
        } else {
            Ok(ValidationResult::Insufficient(0, 0))
        }
    }

    /// Lists validators without signatures for a given block.
    pub fn validators_without_signatures(
        &self,
        block_id_short: &BlockIdShort,
    ) -> Vec<Arc<ValidatorInfo>> {
        if let Some(ref_data) = self.blocks_signatures.get(block_id_short) {
            let validators_with_signatures: std::collections::HashSet<_> = ref_data
                .1
                .valid_signatures
                .keys()
                .chain(ref_data.1.invalid_signatures.keys())
                .collect();

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
    pub async fn cache_signatures(
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
    pub fn add_signatures(
        &self,
        block_id_short: BlockIdShort,
        signatures: Vec<(HashBytes, Signature)>,
    ) -> Result<()> {
        let mut entry = self
            .blocks_signatures
            .entry(block_id_short)
            .or_insert_with(|| {
                (BlockId::default(), SignatureMaps {
                    valid_signatures: FastHashMap::default(),
                    invalid_signatures: FastHashMap::default(),
                    event_dispatched: Mutex::new(false),
                })
            });

        for (validator_id, signature) in signatures {
            // Skip if signature already exists
            if entry.1.valid_signatures.contains_key(&validator_id)
                || entry.1.invalid_signatures.contains_key(&validator_id)
            {
                continue;
            }

            let block_validation_candidate = BlockValidationCandidate::from(entry.0);

            let validator = self.validators.get(&validator_id);

            let validator = match validator {
                Some(validator) => validator,
                None => {
                    tracing::warn!(target: tracing_targets::VALIDATOR, validator_id=%validator_id, "Validator not found");
                    continue;
                }
            };

            match validator
                .public_key
                .verify(block_validation_candidate.as_bytes(), &signature.0)
            {
                true => entry.1.valid_signatures.insert(validator_id, signature),
                false => entry.1.invalid_signatures.insert(validator_id, signature),
            };
        }

        Ok(())
    }

    /// Checks the validation status of a block and notifies listeners.
    #[tracing::instrument(skip(self, listeners), fields(block_id_short))]
    pub async fn check_validation_and_notify(
        &self,
        block_id_short: BlockIdShort,
        listeners: &[Arc<dyn ValidatorEventListener>],
    ) -> Result<bool> {
        let block_signatures = self
            .blocks_signatures
            .get(&block_id_short)
            .ok_or(anyhow!("Block not found"))?;

        let mut event_guard = block_signatures.1.event_dispatched.lock().await;
        if *event_guard {
            return Ok(true);
        }

        let validation_status = self.calc_validation_status(&block_signatures.1);
        match validation_status {
            ValidationResult::Valid | ValidationResult::Invalid => {
                *event_guard = true;
                drop(event_guard);

                tracing::info!(target: tracing_targets::VALIDATOR, block_id_short=%block_id_short, "Block validation finished");

                let event = match validation_status {
                    ValidationResult::Valid => OnValidatedBlockEvent::Valid(BlockSignatures {
                        signatures: block_signatures.1.valid_signatures.clone(),
                    }),
                    ValidationResult::Invalid => OnValidatedBlockEvent::Invalid,
                    ValidationResult::Insufficient(..) => unreachable!(),
                };
                Self::notify_listeners(block_signatures.0, event, listeners);
            }
            ValidationResult::Insufficient(total_valid_weight, valid_weight_threshold) => {
                tracing::trace!(target: tracing_targets::VALIDATOR,
                total_valid_weight,
                valid_weight_threshold,
                "Insufficient signatures for block");
            }
        }

        Ok(validation_status.is_finished())
    }

    /// Calculates the validation status of a block.
    fn calc_validation_status(&self, signature_maps: &SignatureMaps) -> ValidationResult {
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
        let total_invalid_weight =
            calculate_total_weight(signature_maps.invalid_signatures.keys(), &self.validators);

        let valid_weight_threshold = self.max_weight * 2 / 3 + 1;
        let invalid_weight_threshold = self.max_weight / 3 + 1;

        if total_valid_weight >= valid_weight_threshold {
            ValidationResult::Valid
        } else if total_invalid_weight >= invalid_weight_threshold {
            ValidationResult::Invalid
        } else {
            ValidationResult::Insufficient(total_valid_weight, valid_weight_threshold)
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
            let cloned_event = event.clone();
            let listener = listener.clone();
            tokio::spawn(async move {
                listener
                    .on_block_validated(block, cloned_event)
                    .await
                    .expect("Failed to notify listener");
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
