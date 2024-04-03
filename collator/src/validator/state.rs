use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{bail, Context};
use everscale_types::cell::HashBytes;
use everscale_types::models::{BlockId, BlockIdShort, Signature};

use tycho_network::PrivateOverlay;

use crate::validator::types::{
    BlockValidationCandidate, ValidationResult, ValidationSessionInfo, ValidatorInfo,
};

struct SignatureMaps {
    valid_signatures: HashMap<HashBytes, Signature>,
    invalid_signatures: HashMap<HashBytes, Signature>,
}

/// Represents the state of validation for blocks and sessions.
pub trait ValidationState: Send + Sync + 'static {
    /// Creates a new instance of a type implementing `ValidationState`.
    fn new() -> Self;

    /// Adds a new validation session.
    fn add_session(&mut self, session: Arc<ValidationSessionInfo>, private_overlay: PrivateOverlay);

    /// Retrieves an immutable reference to a session by its ID.
    fn get_session(&self, session_id: u32) -> Option<&SessionInfo>;

    /// Retrieves a mutable reference to a session by its ID.
    fn get_mut_session(&mut self, session_id: u32) -> Option<&mut SessionInfo>;
}

/// Holds information about a validation session.
pub(crate) struct SessionInfo {
    session_id: u32,
    max_weight: u64,
    blocks_signatures: HashMap<BlockIdShort, (BlockId, SignatureMaps)>,
    cached_signatures: HashMap<BlockIdShort, HashMap<HashBytes, Signature>>,
    validation_session_info: Arc<ValidationSessionInfo>,
    private_overlay: PrivateOverlay,
}

impl SessionInfo {
    /// Returns the associated `PrivateOverlay`.
    pub(crate) fn get_overlay(&self) -> &PrivateOverlay {
        &self.private_overlay
    }

    /// Returns the `ValidationSessionInfo`.
    pub fn get_validation_session_info(&self) -> &ValidationSessionInfo {
        &self.validation_session_info
    }

    /// Adds a block to the session, moving cached signatures to block signatures.
    pub fn add_block(&mut self, block: BlockId) -> anyhow::Result<()> {
        let block_header = block.as_short_id();
        self.blocks_signatures
            .entry(block_header)
            .or_insert_with(|| {
                (
                    block,
                    SignatureMaps {
                        valid_signatures: Default::default(),
                        invalid_signatures: Default::default(),
                    },
                )
            });

        if let Some(cached_signatures) = self.cached_signatures.remove(&block_header) {
            let candidate: BlockValidationCandidate = block.into();
            for (validator_id, signature) in cached_signatures {
                let validator = self
                    .validation_session_info
                    .validators
                    .get(&validator_id)
                    .context("Validator not found in session")?;
                let signature_is_valid = validator
                    .public_key
                    .verify(candidate.as_bytes(), &signature.0);
                if let Some((_, signature_maps)) = self.blocks_signatures.get_mut(&block_header) {
                    if signature_is_valid {
                        signature_maps
                            .valid_signatures
                            .insert(validator_id, signature);
                    } else {
                        signature_maps
                            .invalid_signatures
                            .insert(validator_id, signature);
                    }
                } else {
                    bail!("Block not found in session but was added before");
                }
            }
        }
        Ok(())
    }

    pub fn get_block(&self, block_id_short: &BlockIdShort) -> Option<&BlockId> {
        self.blocks_signatures
            .get(block_id_short)
            .map(|(block, _)| block)
    }

    pub(crate) fn blocks_count(&self) -> usize {
        self.blocks_signatures.len()
    }

    /// Determines the validation status of a block.
    pub fn validation_status(&self, block_id_short: &BlockIdShort) -> ValidationResult {
        if let Some((_, signature_maps)) = self.blocks_signatures.get(block_id_short) {
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

            let valid_weight = self.max_weight * 2 / 3 + 1;
            if total_valid_weight >= valid_weight {
                ValidationResult::Valid
            } else if self.is_invalid(signature_maps, valid_weight) {
                ValidationResult::Invalid
            } else {
                ValidationResult::Insufficient
            }
        } else {
            ValidationResult::Insufficient
        }
    }
    /// Lists validators without signatures for a given block.
    pub fn validators_without_signatures(
        &self,
        block_id_short: &BlockIdShort,
    ) -> Vec<Arc<ValidatorInfo>> {
        // Retrieve the block signatures (both valid and invalid) if they exist.
        if let Some((_, signature_maps)) = self.blocks_signatures.get(block_id_short) {
            // Create a combined set of validator IDs who have signed (either validly or invalidly).
            let validators_with_signatures: std::collections::HashSet<_> = signature_maps
                .valid_signatures
                .keys()
                .chain(signature_maps.invalid_signatures.keys())
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
    pub fn add_cached_signatures(
        &mut self,
        block_id_short: &BlockIdShort,
        signatures: Vec<(HashBytes, Signature)>,
    ) {
        self.cached_signatures
            .insert(*block_id_short, signatures.into_iter().collect());
    }

    // /// Checks if a block exists within the session.
    // pub fn is_block_exists(&self, block_id_short: &BlockIdShort) -> bool {
    //     self.blocks_signatures.contains_key(block_id_short)
    // }

    /// Retrieves valid signatures for a block.
    pub fn get_valid_signatures(
        &self,
        block_id_short: &BlockIdShort,
    ) -> HashMap<HashBytes, Signature> {
        if let Some((_, signature_maps)) = self.blocks_signatures.get(block_id_short) {
            signature_maps.valid_signatures.clone()
        } else {
            HashMap::new()
        }
    }

    /// Adds a signature for a block.
    pub fn add_signature(
        &mut self,
        block_id: &BlockId,
        validator_id: HashBytes,
        signature: Signature,
        is_valid: bool,
    ) {
        let block_header = block_id.as_short_id();
        let entry = self
            .blocks_signatures
            .entry(block_header)
            .or_insert_with(|| {
                (
                    *block_id,
                    SignatureMaps {
                        valid_signatures: HashMap::new(),
                        invalid_signatures: HashMap::new(),
                    },
                )
            });

        if is_valid {
            entry.1.valid_signatures.insert(validator_id, signature);
        } else {
            entry.1.invalid_signatures.insert(validator_id, signature);
        }
    }

    /// Determines if a block is considered invalid based on the signatures.
    fn is_invalid(&self, signature_maps: &SignatureMaps, valid_weight: u64) -> bool {
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

        let total_possible_weight = self
            .validation_session_info
            .validators
            .values()
            .map(|vi| vi.weight)
            .sum::<u64>();
        total_possible_weight - total_invalid_weight < valid_weight
    }
}

/// Standard implementation of `ValidationState`.
pub struct ValidationStateStdImpl {
    sessions: HashMap<u32, SessionInfo>,
}

impl ValidationState for ValidationStateStdImpl {
    fn new() -> Self {
        Self {
            sessions: Default::default(),
        }
    }

    fn add_session(
        &mut self,
        session: Arc<ValidationSessionInfo>,
        private_overlay: PrivateOverlay,
    ) {
        let session_info = SessionInfo {
            session_id: session.seqno,
            max_weight: session.validators.values().map(|info| info.weight).sum(),
            blocks_signatures: Default::default(),
            cached_signatures: Default::default(),
            validation_session_info: session,
            private_overlay,
        };
        self.sessions.insert(session_info.session_id, session_info);
    }

    fn get_session(&self, session_id: u32) -> Option<&SessionInfo> {
        self.sessions.get(&session_id)
    }

    fn get_mut_session(&mut self, session_id: u32) -> Option<&mut SessionInfo> {
        self.sessions.get_mut(&session_id)
    }
}
