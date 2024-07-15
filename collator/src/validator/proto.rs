use std::sync::Arc;

use everscale_types::models::*;
use tl_proto::{TlError, TlPacket, TlRead, TlResult, TlWrite};

/// Data for computing a private overlay id.
#[derive(Debug, Clone, PartialEq, Eq, TlRead, TlWrite)]
#[tl(boxed, id = "validator.overlayIdData", scheme = "proto.tl")]
pub struct OverlayIdData {
    pub zerostate_root_hash: [u8; 32],
    pub zerostate_file_hash: [u8; 32],
    #[tl(with = "tl_shard_ident")]
    pub shard_ident: ShardIdent,
    /// Timestamp of the corresponding validator set.
    pub session_id: u32,
}

#[derive(Debug, Clone, TlRead, TlWrite)]
#[tl(boxed, id = "validator.peerSignature", scheme = "proto.tl")]
pub struct PeerSignatureRef<'tl> {
    pub peer_id: &'tl [u8; 32],
    #[tl(with = "tycho_util::tl::signature_ref")]
    pub signature: &'tl [u8; 64],
}

#[derive(Debug, Clone, TlRead, TlWrite)]
#[tl(boxed, id = "validator.peerSignature", scheme = "proto.tl")]
pub struct PeerSignatureOwned {
    pub peer_id: [u8; 32],
    #[tl(with = "tycho_util::tl::signature_arc")]
    pub signature: Arc<[u8; 64]>,
}

#[derive(Debug, Clone, TlRead, TlWrite)]
#[tl(boxed, id = "validator.signatures", scheme = "proto.tl")]
pub struct SignaturesOwned {
    #[tl(with = "tycho_util::tl::VecWithMaxLen::<100>")]
    pub items: Vec<PeerSignatureOwned>,
}

pub mod rpc {
    use super::*;

    #[derive(Debug, Clone, TlRead, TlWrite)]
    #[tl(boxed, id = "validator.exchangeSignatures", scheme = "proto.tl")]
    pub struct ExchangeSignaturesOwned {
        pub block_seqno: u32,
        #[tl(with = "tycho_util::tl::signature_arc")]
        pub signature: Arc<[u8; 64]>,
    }

    #[derive(Debug, Clone, TlRead, TlWrite)]
    #[tl(boxed, id = "validator.exchangeSignatures", scheme = "proto.tl")]
    pub struct ExchangeSignaturesRef<'tl> {
        pub block_seqno: u32,
        #[tl(with = "tycho_util::tl::signature_ref")]
        pub signature: &'tl [u8; 64],
    }
}

mod tl_shard_ident {
    use super::*;

    pub const fn size_hint(_: &ShardIdent) -> usize {
        12
    }

    #[inline]
    pub fn write<P: TlPacket>(shard_ident: &ShardIdent, packet: &mut P) {
        shard_ident.workchain().write_to(packet);
        shard_ident.prefix().write_to(packet);
    }

    #[inline]
    pub fn read(data: &[u8], offset: &mut usize) -> TlResult<ShardIdent> {
        let workchain = i32::read_from(data, offset)?;
        let prefix = u64::read_from(data, offset)?;
        ShardIdent::new(workchain, prefix).ok_or(TlError::InvalidData)
    }
}
