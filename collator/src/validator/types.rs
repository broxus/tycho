use std::convert::TryFrom;

use everscale_crypto::ed25519::PublicKey;
use everscale_types::cell::HashBytes;
use everscale_types::models::{BlockIdShort, ShardIdent, ValidatorDescription};
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

#[derive(Clone, Debug)]
pub enum StopValidationCommand {
    ByTopShardBlock(BlockIdShort),
    ByBlock(BlockIdShort),
}

#[derive(TlWrite, TlRead)]
#[tl(boxed, id = "types.overlayNumber", scheme = "proto.tl")]
pub struct OverlayNumber {
    #[tl(with = "tl_shard_ident")]
    pub shard_ident: ShardIdent,
    pub session_seqno: u32,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ValidationStatus {
    Valid,
    Insufficient(u64, u64),
    BlockNotExist,
}

impl ValidationStatus {
    pub fn is_finished(&self) -> bool {
        match self {
            Self::Valid => true,
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
