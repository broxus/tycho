use crate::types::BlockCandidate;
use crate::validator::types::{
    BlockValidationCandidate, ValidationSessionInfo, ValidatorInfo, ValidatorsMap,
};
use everscale_types::cell::HashBytes;
use everscale_types::models::{BlockId, BlockIdShort, Signature};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;
use tycho_network::PrivateOverlay;

pub trait ValidationState: Send + Sync + 'static {
    fn add_signature(
        &mut self,
        session_id: u32,
        block_id: BlockId,
        validator_id: HashBytes,
        signature: Signature,
        // validators: &ValidatorsMap
    );

    fn is_validated(&self, session_id: u32, block_id: &BlockValidationCandidate) -> bool;
    fn new() -> Self;
    // fn add_session_if_not_exists(&mut self, validation_session_info: Arc<ValidationSessionInfo>);
    fn get_signatures(
        &self,
        session_id: u32,
        block_id: BlockValidationCandidate,
    ) -> Option<&HashMap<HashBytes, Signature>>;
    fn validators_without_signatures(
        &self,
        session_id: u32,
        block_id: BlockId,
    ) -> Vec<Arc<ValidatorInfo>>;
    fn add_session(&mut self, session: Arc<ValidationSessionInfo>, private_overlay: PrivateOverlay);
    fn get_session(&self, session_id: u32) -> Option<&SessionInfo>;
}

pub(crate) struct SessionInfo {
    session_id: u32,
    max_weight: u64,
    total_weight: u64,
    blocks_signatures: HashMap<BlockValidationCandidate, HashMap<HashBytes, Signature>>,
    blocks: HashMap<BlockValidationCandidate, BlockId>,
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

    fn get_signatures(
        &self,
        session_id: u32,
        block_id: BlockValidationCandidate,
    ) -> Option<&HashMap<HashBytes, Signature>> {
        self.sessions
            .get(&session_id)
            .and_then(|session| session.blocks_signatures.get(&block_id))
    }

    // fn add_session_if_not_exists(&mut self, validation_session_info: Arc<ValidationSessionInfo>) {
    //     self.sessions.entry(validation_session_info.seqno).or_insert_with(|| {
    //         let max_weight = validation_session_info.validators.values().map(|info| info.weight).sum();
    //         SessionInfo {
    //             session_id: validation_session_info.seqno,
    //             max_weight,
    //             total_weight: 0,
    //             blocks: Default::default(),
    //             blocks_signatures: Default::default(),
    //             validation_session_info: validation_session_info.clone()
    //         }
    //     });
    // }

    fn add_session(
        &mut self,
        session: Arc<ValidationSessionInfo>,
        private_overlay: PrivateOverlay,
    ) {
        let session = SessionInfo {
            session_id: session.seqno,
            max_weight: session.validators.values().map(|info| info.weight).sum(),
            total_weight: 0,
            blocks: Default::default(),
            blocks_signatures: Default::default(),
            validation_session_info: session.clone(),
            private_overlay,
        };

        self.sessions.insert(session.session_id, session);
    }

    fn add_signature(
        &mut self,
        session_id: u32,
        block_id: BlockId,
        validator_id: HashBytes,
        signature: Signature,
        // validators: &ValidatorsMap
    ) {
        let block_validation_candidate = block_id.into();
        if let Some(session) = self.sessions.get_mut(&session_id) {
            if let Some(validator_info) = session
                .validation_session_info
                .validators
                .get(&validator_id)
            {
                // validator_info.public_key.verify(&signature)
                session.blocks.insert(block_validation_candidate, block_id);
                session
                    .blocks_signatures
                    .entry(block_validation_candidate)
                    .or_insert_with(|| {
                        session.max_weight = session.max_weight.max(validator_info.weight);
                        Default::default()
                    })
                    .insert(validator_id, signature);
            }
        }
    }

    // if we have 2/3+1 weight of total weight of validators, we can validate the block
    fn is_validated(&self, session_id: u32, block_id: &BlockValidationCandidate) -> bool {
        if let Some(session) = self.sessions.get(&session_id) {
            if let Some(signatures) = session.blocks_signatures.get(block_id) {
                let mut total_weight = 0;
                for (validator_id, _) in signatures.iter() {
                    if let Some(validator_info) =
                        session.validation_session_info.validators.get(validator_id)
                    {
                        // if validator_info.public_key.verify(&signature) {
                        total_weight += validator_info.weight;
                        // }
                    }
                }
                return total_weight >= session.max_weight * 2 / 3 + 1;
            }
        }
        false
    }
    fn validators_without_signatures(
        &self,
        session_id: u32,
        block_id: BlockId,
    ) -> Vec<Arc<ValidatorInfo>> {
        if let Some(session) = self.sessions.get(&session_id) {
            if let Some(signatures) = session.blocks_signatures.get(&block_id.into()) {
                let mut validators = vec![];
                for v in &session.validation_session_info.validators {
                    if signatures.get(v.0).is_none() {
                        validators.push(v.1.clone());
                    }
                }
                return validators;
            } else {
                debug!("BLOCK NOT FOUND IN SIGNATURES");
            }
        } else {
            debug!("ERR: SESSION {session_id} NOT FOUND");
        }
        vec![]
    }

    fn get_session(&self, session_id: u32) -> Option<&SessionInfo> {
        self.sessions.get(&session_id)
    }
}
