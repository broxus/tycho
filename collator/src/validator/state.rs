use crate::types::BlockCandidate;
use crate::validator::types::{
    BlockValidationCandidate, ValidationResult, ValidationSessionInfo, ValidatorInfo, ValidatorsMap,
};
use everscale_types::cell::HashBytes;
use everscale_types::models::{BlockId, BlockIdShort, Signature};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error};
use tycho_network::PrivateOverlay;

pub trait ValidationState: Send + Sync + 'static {
    // fn add_signature(
    //     &mut self,
    //     session_id: u32,
    //     block_id: &BlockId,
    //     validator_id: HashBytes,
    //     signature: Signature,
    //     is_valid: bool,
    // );
    //
    // fn validation_status(
    //     &self,
    //     session_id: u32,
    //     block_id_short: &BlockIdShort,
    // ) -> ValidationResult;

    fn new() -> Self;

    // fn get_valid_signatures(
    //     &self,
    //     session_id: u32,
    //     block_id_short: &BlockIdShort,
    // ) -> HashMap<HashBytes, Signature>;
    //
    // fn validators_without_signatures(
    //     &self,
    //     session_id: u32,
    //     block_id: BlockId,
    // ) -> Vec<Arc<ValidatorInfo>>;

    fn add_session(&mut self, session: Arc<ValidationSessionInfo>, private_overlay: PrivateOverlay);

    fn get_session(&self, session_id: u32) -> Option<&SessionInfo>;
    fn get_mut_session(&mut self, session_id: u32) -> Option<&mut SessionInfo>;
    // fn add_block(&mut self, session_id: u32, block: BlockId);
    // fn add_cached_signatures(
    //     &mut self,
    //     session_id: u32,
    //     block_id_short: &BlockIdShort,
    //     signatures: Vec<(HashBytes, Signature)>,
    // );
    // fn is_block_exists(&self, session_id: u32, block_id_short: &BlockIdShort) -> bool;
}

pub(crate) struct SessionInfo {
    session_id: u32,
    max_weight: u64,
    total_weight: u64,
    blocks_signatures: HashMap<BlockIdShort, (BlockId, HashMap<HashBytes, (Signature, bool)>)>,
    cached_signatures: HashMap<BlockIdShort, HashMap<HashBytes, Signature>>,
    // blocks: HashMap<BlockValidationCandidate, BlockId>,
    validation_session_info: Arc<ValidationSessionInfo>,
    private_overlay: PrivateOverlay,
}

impl SessionInfo {
    pub(crate) fn get_overlay(&self) -> &PrivateOverlay {
        &self.private_overlay
    }

    pub fn get_validation_session_info(&self) -> &ValidationSessionInfo {
        &self.validation_session_info
    }


    pub fn add_block(&mut self, session_id: u32, block: BlockId) {
            let block_header = block.as_short_id();
            self.blocks_signatures.entry(block_header).or_insert_with(|| {
                (block, Default::default())
            });
            /// move cached signatures to block signatures
            if let Some(cached_signatures) = self.cached_signatures.remove(&block_header) {
                let candidate: BlockValidationCandidate = block.into();
                for (validator_id, signature) in cached_signatures {
                    /// check signature validity
                    let validator = self.validation_session_info.validators.get(&validator_id).expect("Validator not found in session");
                    let signature_is_valid = validator.public_key.verify(&candidate.to_bytes(), &signature.0);
                    if let Some((_, signatures_map)) = self.blocks_signatures.get_mut(&block_header) {
                        signatures_map.insert(validator_id, (signature, signature_is_valid));
                    } else {
                        error!("Block not found in session but was added before");
                        panic!("Block not found in session but was added before");
                    }
                }
            }

    }

    pub fn validation_status(
        &self,
        block_id_short: &BlockIdShort,
    ) -> ValidationResult {
            if let Some((_, signatures)) = self.blocks_signatures.get(block_id_short) {
                let total_weight: u64 = signatures
                    .iter()
                    .filter_map(|(validator_id, (_, is_valid))| {
                        if *is_valid {
                            self
                                .validation_session_info
                                .validators
                                .get(validator_id)
                                .map(|vi| vi.weight)
                        } else {
                            None
                        }
                    })
                    .sum();

                let valid_weight = self.max_weight * 2 / 3 + 1;
                if total_weight >= valid_weight {
                    return ValidationResult::Valid;
                } else {
                    let invalid_weight: u64 = signatures
                        .iter()
                        .filter_map(|(validator_id, (_, is_valid))| {
                            if !is_valid {
                                self
                                    .validation_session_info
                                    .validators
                                    .get(validator_id)
                                    .map(|vi| vi.weight)
                            } else {
                                None
                            }
                        })
                        .sum();

                    // Assuming total_possible_weight is the sum of weights of all validators
                    let total_possible_weight = self
                        .validation_session_info
                        .validators
                        .values()
                        .map(|vi| vi.weight)
                        .sum::<u64>();
                    if total_possible_weight - invalid_weight < valid_weight {
                        return ValidationResult::Invalid;
                    }
                }
            }

        ValidationResult::Insufficient
    }

    pub fn validators_without_signatures(
        &self,
        block_id_short: &BlockIdShort,
    ) -> Vec<Arc<ValidatorInfo>> {
            if let Some((_, signatures)) = self.blocks_signatures.get(block_id_short) {
                let mut validators = vec![];
                for v in &self.validation_session_info.validators {
                    if signatures.get(v.0).is_none() {
                        validators.push(v.1.clone());
                    }
                }
                return validators;
            }
        vec![]
    }

    pub fn add_cached_signatures(
        &mut self,
        block_id_short: &BlockIdShort,
        signatures: Vec<(HashBytes, Signature)>,
    ) {
        self.cached_signatures.insert(block_id_short.clone(), signatures.into_iter().collect());
    }

    pub  fn is_block_exists(&self, block_id_short: &BlockIdShort) -> bool {
            self.blocks_signatures.contains_key(&block_id_short)

    }

    pub fn get_valid_signatures(
        &self,
        block_id_short: &BlockIdShort,
    ) -> HashMap<HashBytes, Signature> {
            if let Some((_, signature_map)) = self.blocks_signatures.get(block_id_short) {
                return signature_map
                    .iter()
                    .filter_map(|(key, (signature, is_valid))| {
                        if *is_valid {
                            Some((key.clone(), signature.clone()))
                        } else {
                            None
                        }
                    })
                    .collect();
            }
        HashMap::new() // Return an empty HashMap if no valid signatures found or session/block_id not found
    }

    pub  fn add_signature(
        &mut self,
        block_id: &BlockId,
        validator_id: HashBytes,
        signature: Signature,
        is_valid: bool,
    ) {
        let block_header = block_id.as_short_id();
            if let Some((_, signatures_map)) = self.blocks_signatures.get_mut(&block_header) {
                signatures_map.insert(validator_id, (signature, is_valid));
            } else {
                error!("Block not found in session but was added before");
                panic!("Block not found in session but was added before");
        }
    }
}

pub struct ValidationStateStdImpl {
    pub sessions: HashMap<u32, SessionInfo>,
}

impl ValidationState for ValidationStateStdImpl {
    fn new() -> Self {
        Self {
            sessions: Default::default(),
        }
    }

    fn get_mut_session(&mut self, session_id: u32) -> Option<&mut SessionInfo> {
        self.sessions.get_mut(&session_id)
    }







    fn add_session(
        &mut self,
        session: Arc<ValidationSessionInfo>,
        private_overlay: PrivateOverlay,
    ) {
        let session = SessionInfo {
            session_id: session.seqno,
            max_weight: session.validators.values().map(|info| info.weight).sum(),
            total_weight: 0,
            blocks_signatures: Default::default(),
            cached_signatures: Default::default(),
            validation_session_info: session.clone(),
            private_overlay,
        };

        self.sessions.insert(session.session_id, session);
    }





    fn get_session(&self, session_id: u32) -> Option<&SessionInfo> {
        self.sessions.get(&session_id)
    }
}
