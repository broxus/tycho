use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::Arc;

use anyhow::bail;
use everscale_crypto::ed25519::{KeyPair, PublicKey};
use everscale_types::cell::HashBytes;
use everscale_types::models::{BlockId, ValidatorDescription};
use log::error;
use tl_proto::{TlRead, TlWrite};

use crate::types::CollationSessionInfo;

pub(crate) type ValidatorsMap = HashMap<[u8; 32], Arc<ValidatorInfo>>;

pub(crate) enum ValidatorInfoError {
    InvalidPublicKey,
}

#[derive(Clone)]
pub(crate) struct ValidatorInfo {
    pub public_key: PublicKey,
    pub weight: u64,
    pub adnl_addr: Option<HashBytes>,
}

impl TryFrom<&ValidatorDescription> for ValidatorInfo {
    type Error = ValidatorInfoError;

    fn try_from(value: &ValidatorDescription) -> Result<Self, Self::Error> {
        let pubkey = PublicKey::from_bytes(value.public_key.0)
            .ok_or(ValidatorInfoError::InvalidPublicKey)?;
        Ok(Self {
            public_key: pubkey,
            weight: value.weight,
            adnl_addr: value.adnl_addr.map(|addr| HashBytes(addr.0)),
        })
    }
}

pub struct ValidationSessionInfo {
    pub seqno: u32,
    pub validators: ValidatorsMap,
    pub current_validator_keypair: KeyPair,
}

impl TryFrom<Arc<CollationSessionInfo>> for ValidationSessionInfo {
    type Error = anyhow::Error;

    fn try_from(session_info: Arc<CollationSessionInfo>) -> std::result::Result<Self, Self::Error> {
        let current_validator_keypair = match session_info.current_collator_keypair() {
            Some(keypair) => *keypair,
            None => {
                bail!("Collator keypair is not set, skip candidate validation");
            }
        };

        let mut validators = HashMap::new();
        for validator_descr in session_info.collators().validators.iter() {
            let validator_info: anyhow::Result<ValidatorInfo, ValidatorInfoError> =
                validator_descr.try_into();
            match validator_info {
                Ok(validator_info) => {
                    validators.insert(
                        validator_info.public_key.to_bytes(),
                        Arc::new(validator_info),
                    );
                }
                Err(_) => {
                    error!("invalid validator public key");
                }
            }
        }

        let validation_session = ValidationSessionInfo {
            seqno: session_info.seqno(),
            validators,
            current_validator_keypair,
        };
        Ok(validation_session)
    }
}

/// Block candidate for validation
#[derive(Debug, Default, Clone, Copy, Eq, Hash, PartialEq, Ord, PartialOrd, TlRead, TlWrite)]
pub struct BlockValidationCandidate {
    pub root_hash: [u8; 32],
    pub file_hash: [u8; 32],
}

impl From<BlockId> for BlockValidationCandidate {
    fn from(block_id: BlockId) -> Self {
        Self {
            root_hash: block_id.root_hash.0,
            file_hash: block_id.file_hash.0,
        }
    }
}

impl BlockValidationCandidate {
    pub fn as_bytes(&self) -> [u8; 64] {
        let mut bytes = [0u8; 64];
        bytes[..32].copy_from_slice(&self.root_hash);
        bytes[32..].copy_from_slice(&self.file_hash);
        bytes
    }
}

#[derive(TlWrite, TlRead)]
#[tl(boxed, id = 0x12341111)]
pub struct OverlayNumber {
    pub session_seqno: u32,
}

#[derive(Eq, PartialEq, Debug)]
pub enum ValidationResult {
    Valid,
    Invalid,
    Insufficient,
}
