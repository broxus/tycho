use std::convert::TryFrom;

use everscale_crypto::ed25519::PublicKey;
use everscale_types::cell::HashBytes;
use everscale_types::models::{BlockId, ShardIdent, ValidatorDescription};
use tl_proto::{TlRead, TlWrite};

#[derive(Clone)]
pub struct ValidatorInfo {
    pub public_key_hash: HashBytes,
    pub weight: u64,
    pub public_key: PublicKey,
}

impl TryFrom<&ValidatorDescription> for ValidatorInfo {
    type Error = anyhow::Error;

    fn try_from(value: &ValidatorDescription) -> Result<Self, Self::Error> {
        let pubkey = PublicKey::from_bytes(value.public_key.0)
            .ok_or(anyhow::anyhow!("Invalid public key"))?;
        Ok(Self {
            public_key_hash: HashBytes(pubkey.to_bytes()),
            public_key: pubkey,
            weight: value.weight,
        })
    }
}

/// Block candidate for validation
#[derive(Debug, Default, Clone, Copy, Eq, Hash, PartialEq, Ord, PartialOrd, TlRead, TlWrite)]
pub(crate) struct BlockValidationCandidate {
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
#[tl(boxed, id = "types.overlayNumber", scheme = "proto.tl")]
pub struct OverlayNumber {
    #[tl(with = "tl_shard_ident")]
    pub shard_ident: ShardIdent,
    pub session_seqno: u32,
}

#[derive(Eq, PartialEq, Debug, Clone)]
pub enum ValidationStatus {
    Valid,
    Invalid,
    Insufficient(u64, u64),
    BlockNotExist,
}

impl ValidationStatus {
    pub fn is_finished(&self) -> bool {
        match self {
            Self::Valid | Self::Invalid => true,
            Self::Insufficient(..) | Self::BlockNotExist => false,
        }
    }
}

mod tl_shard_ident {
    use everscale_types::models::ShardIdent;
    use tl_proto::{TlError, TlPacket, TlRead, TlResult, TlWrite};

    pub const fn size_hint(_: &ShardIdent) -> usize {
        12
    }

    pub fn write<P: TlPacket>(shard: &ShardIdent, packet: &mut P) {
        shard.workchain().write_to(packet);
        shard.prefix().write_to(packet);
    }

    pub fn read(packet: &[u8], offset: &mut usize) -> TlResult<ShardIdent> {
        let workchain = i32::read_from(packet, offset)?;
        let prefix = u64::read_from(packet, offset)?;
        ShardIdent::new(workchain, prefix).ok_or(TlError::InvalidData)
    }
}
