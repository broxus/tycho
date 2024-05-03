use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{bail, Context};
use everscale_types::cell::HashBytes;
use everscale_types::models::{BlockId, BlockIdShort, Signature};
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, trace};

use crate::types::{BlockSignatures, OnValidatedBlockEvent};
use crate::validator::types::{
    BlockValidationCandidate, ValidationResult, ValidationSessionInfo, ValidatorInfo,
};
use crate::validator::ValidatorEventListener;
use tycho_network::PrivateOverlay;
use tycho_util::{FastDashMap, FastHashMap};

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
    ) -> impl std::future::Future<Output = anyhow::Result<()>> + Send;
    /// Retrieves an immutable reference to a session by its ID.
    fn get_session(
        &self,
        workchain: i32,
        session_id: u32,
    ) -> impl std::future::Future<Output = Option<Arc<SessionInfo>>> + Send;
}

/// Holds information about a validation session.
pub struct SessionInfo {
    workchain: i32,
    seqno: u32,
    max_weight: u64,
    blocks_signatures: FastDashMap<BlockIdShort, (BlockId, SignatureMaps)>,
    cached_signatures: FastDashMap<BlockIdShort, FastHashMap<HashBytes, Signature>>,
    validation_session_info: Arc<ValidationSessionInfo>,
    private_overlay: PrivateOverlay,
}

impl SessionInfo {
    pub fn new(
        workchain: i32,
        seqno: u32,
        validation_session_info: Arc<ValidationSessionInfo>,
        private_overlay: PrivateOverlay,
    ) -> Arc<SessionInfo> {
        let max_weight = validation_session_info
            .validators
            .values()
            .map(|vi| vi.weight)
            .sum();
        Arc::new(Self {
            workchain,
            seqno,
            max_weight,
            blocks_signatures: Default::default(),
            cached_signatures: Default::default(),
            validation_session_info,
            private_overlay,
        })
    }

    pub fn workchain(&self) -> i32 {
        self.workchain
    }

    pub fn get_seqno(&self) -> u32 {
        self.seqno
    }

    pub fn get_cached_signatures_by_block(
        &self,
        block_id_short: &BlockIdShort,
    ) -> Option<(BlockIdShort, FastHashMap<HashBytes, Signature>)> {
        self.cached_signatures.remove(block_id_short)
    }

    /// Returns the associated `PrivateOverlay`.
    pub(crate) fn get_overlay(&self) -> &PrivateOverlay {
        &self.private_overlay
    }

    /// Returns the `ValidationSessionInfo`.
    pub fn get_validation_session_info(&self) -> Arc<ValidationSessionInfo> {
        self.validation_session_info.clone()
    }

    pub async fn is_validator_signed(
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

    /// Adds a block to the session, moving cached signatures to block signatures.
    pub async fn add_block(&self, block: BlockId) -> anyhow::Result<()> {
        let block_header = block.as_short_id();

        self.blocks_signatures
            .entry(block_header)
            .or_insert_with(|| {
                (
                    block,
                    SignatureMaps {
                        valid_signatures: FastHashMap::default(),
                        invalid_signatures: FastHashMap::default(),
                        event_dispatched: Mutex::new(false),
                    },
                )
            });
        Ok(())
    }

    pub async fn get_block(&self, block_id_short: &BlockIdShort) -> Option<BlockId> {
        self.blocks_signatures
            .get(block_id_short)
            .map(|ref_data| ref_data.0)
    }

    /// Determines the validation status of a block.
    pub async fn get_validation_status(
        &self,
        block_id_short: &BlockIdShort,
    ) -> anyhow::Result<ValidationResult> {
        trace!("Getting validation status for block {:?}", block_id_short);
        // Bind the lock result to a variable to extend its lifetime
        // let block_signatures_guard = self.blocks_signatures;
        let signatures = self.blocks_signatures.get(block_id_short);

        if let Some(ref_data) = signatures {
            Ok(self.validation_status(&ref_data.1).await)
        } else {
            Ok(ValidationResult::Insufficient(0, 0))
        }
    }

    /// Lists validators without signatures for a given block.
    pub async fn validators_without_signatures(
        &self,
        block_id_short: &BlockIdShort,
    ) -> Vec<Arc<ValidatorInfo>> {
        // Retrieve the block signatures (both valid and invalid) if they exist.
        if let Some(ref_data) = self.blocks_signatures.get(block_id_short) {
            // Create a combined set of validator IDs who have signed (either validly or invalidly).
            let validators_with_signatures: std::collections::HashSet<_> = ref_data
                .1
                .valid_signatures
                .keys()
                .chain(ref_data.1.invalid_signatures.keys())
                .collect();

            // Filter validators who haven't provided a signature.
            self.validation_session_info
                .validators
                .iter()
                .filter_map(|(id, info)| {
                    if !validators_with_signatures.contains(&HashBytes(*id)) {
                        Some(info.clone())
                    } else {
                        None
                    }
                })
                .collect()
        } else {
            // If there are no signatures for this block, then all validators are considered without signatures.
            self.validation_session_info
                .validators
                .values()
                .cloned()
                .collect()
        }
    }

    /// Adds cached signatures for a block.
    pub async fn add_cached_signatures(
        &self,
        block_id_short: &BlockIdShort,
        signatures: Vec<(HashBytes, Signature)>,
    ) {
        self.cached_signatures
            .insert(*block_id_short, signatures.into_iter().collect());
    }

    /// Retrieves valid signatures for a block.
    pub async fn get_valid_signatures(
        &self,
        block_id_short: &BlockIdShort,
    ) -> FastHashMap<HashBytes, Signature> {
        let block_signatures = self.blocks_signatures.get(block_id_short);
        block_signatures.map(|ref_data| ref_data.1.invalid_signatures.clone());

        if let Some(ref_data) = self.blocks_signatures.get(block_id_short) {
            ref_data.1.valid_signatures.clone()
        } else {
            FastHashMap::default()
        }
    }

    /// Adds a signature for a block.
    pub async fn add_signature(
        &self,
        block_id: &BlockId,
        validator_id: HashBytes,
        signature: Signature,
        is_valid: bool,
    ) {
        let block_header = block_id.as_short_id();
        // let mut write_guard = self.blocks_signatures.write().await; // Hold onto the lock
        let mut entry = self
            .blocks_signatures
            .entry(block_header) // Use the guard to access the map
            .or_insert_with(|| {
                (
                    *block_id,
                    SignatureMaps {
                        valid_signatures: FastHashMap::default(),
                        invalid_signatures: FastHashMap::default(),
                        event_dispatched: Mutex::new(false),
                    },
                )
            });

        if is_valid {
            entry.1.valid_signatures.insert(validator_id, signature);
        } else {
            entry.1.invalid_signatures.insert(validator_id, signature);
        }
    }

    pub async fn process_signatures_and_update_status(
        &self,
        block_id_short: BlockIdShort,
        signatures: Vec<([u8; 32], [u8; 64])>,
        listeners: &[Arc<dyn ValidatorEventListener>],
    ) -> anyhow::Result<()> {
        trace!(
            "Processing signatures for block in state {:?}",
            block_id_short
        );
        let mut entry = self
            .blocks_signatures
            .entry(block_id_short)
            .or_insert_with(|| {
                (
                    BlockId::default(), // Default should be replaced with actual block retrieval logic if necessary
                    SignatureMaps {
                        valid_signatures: FastHashMap::default(),
                        invalid_signatures: FastHashMap::default(),
                        event_dispatched: Mutex::new(false),
                    },
                )
            });

        let event_guard = entry.1.event_dispatched.lock().await;
        if *event_guard {
            debug!(
                "Validation event already dispatched for block {:?}",
                block_id_short
            );
            return Ok(());
        }

        // Drop the guard to allow mutable access below
        drop(event_guard);

        // Process each signature
        for (pub_key_bytes, sig_bytes) in signatures {
            let validator_id = HashBytes(pub_key_bytes);
            let signature = Signature(sig_bytes);
            let block_validation_candidate = BlockValidationCandidate::from(entry.0);

            let is_valid = self
                .get_validation_session_info()
                .validators
                .get(&validator_id)
                .context("Validator not found")?
                .public_key
                .verify(block_validation_candidate.as_bytes(), &signature.0);

            if is_valid {
                entry.1.valid_signatures.insert(validator_id, signature);
            } else {
                entry.1.invalid_signatures.insert(validator_id, signature);
            }
        }

        let validation_status = self.validation_status(&entry.1).await;
        // Check if the validation status qualifies for dispatching the event
        match validation_status {
            ValidationResult::Valid => {
                let mut event_guard = entry.1.event_dispatched.lock().await;
                *event_guard = true; // Prevent further event dispatching for this block
                drop(event_guard); // Drop guard as soon as possible
                let event = OnValidatedBlockEvent::Valid(BlockSignatures {
                    signatures: entry.1.valid_signatures.clone(),
                });
                Self::notify_listeners(entry.0, event, listeners);
            }
            ValidationResult::Invalid => {
                let mut event_guard = entry.1.event_dispatched.lock().await;
                *event_guard = true; // Prevent further event dispatching for this block
                drop(event_guard); // Drop guard as soon as possible
                let event = OnValidatedBlockEvent::Invalid;
                Self::notify_listeners(entry.0, event, listeners);
            }

            ValidationResult::Insufficient(_, _) => {}
        }

        Ok(())
    }

    async fn validation_status(&self, signature_maps: &SignatureMaps) -> ValidationResult {
        let total_valid_weight: u64 = signature_maps
            .valid_signatures
            .keys()
            .map(|validator_id| {
                self.validation_session_info
                    .validators
                    .get(validator_id)
                    .map_or(0, |vi| vi.weight)
            })
            .sum();

        let total_invalid_weight: u64 = signature_maps
            .invalid_signatures
            .keys()
            .map(|validator_id| {
                self.validation_session_info
                    .validators
                    .get(validator_id)
                    .map_or(0, |vi| vi.weight)
            })
            .sum();

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

    fn notify_listeners(
        block: BlockId,
        event: OnValidatedBlockEvent,
        listeners: &[Arc<dyn ValidatorEventListener>],
    ) {
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
    sessions: RwLock<HashMap<(i32, u32), Arc<SessionInfo>>>,
}

impl ValidationState for ValidationStateStdImpl {
    fn new() -> Self {
        Self {
            sessions: Default::default(),
        }
    }

    async fn try_add_session(&self, session: Arc<SessionInfo>) -> anyhow::Result<()> {
        let workchain = session.workchain;
        let seqno = session.seqno;

        let session = self
            .sessions
            .write()
            .await
            .insert((workchain, seqno), session);

        if session.is_some() {
            bail!("Session already exists with seqno: ({workchain}, {seqno})");
        }

        Ok(())
    }

    async fn get_session(&self, workchain: i32, session_id: u32) -> Option<Arc<SessionInfo>> {
        self.sessions
            .read()
            .await
            .get(&(workchain, session_id))
            .cloned()
    }
}
