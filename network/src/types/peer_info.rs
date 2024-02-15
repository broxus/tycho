use tl_proto::{TlRead, TlWrite};

use crate::types::{Address, PeerId};
use crate::util::{check_peer_signature, tl};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum PeerAffinity {
    High,
    Allowed,
    Never,
}

/// A signed node info.
#[derive(Debug, Clone, TlRead, TlWrite)]
pub struct PeerInfo {
    /// Node public key.
    pub id: PeerId,
    /// Multiple possible addresses for the same peer.
    #[tl(with = "tl_address_list")]
    pub address_list: Box<[Address]>,
    /// Unix timestamp when the info was generated.
    pub created_at: u32,
    /// Unix timestamp up to which the info is valid.
    pub expires_at: u32,
    /// A `ed25519` signature of the info.
    #[tl(signature, with = "tl::signature_owned")]
    pub signature: Box<[u8; 64]>,
}

impl PeerInfo {
    pub const MAX_ADDRESSES: usize = 4;

    pub fn is_valid(&self, at: u32) -> bool {
        const CLOCK_THRESHOLD: u32 = 1;

        self.created_at <= at + CLOCK_THRESHOLD
            && self.expires_at >= at
            && !self.address_list.is_empty()
            && check_peer_signature(&self.id, &self.signature, self)
    }

    pub fn is_expired(&self, at: u32) -> bool {
        self.expires_at < at
    }

    pub fn iter_addresses(&self) -> std::slice::Iter<'_, Address> {
        self.address_list.iter()
    }
}

mod tl_address_list {
    use super::*;

    pub fn size_hint(address_list: &[Address]) -> usize {
        4 + address_list
            .iter()
            .map(Address::max_size_hint)
            .sum::<usize>()
    }

    pub fn write<P: tl_proto::TlPacket>(address_list: &[Address], packet: &mut P) {
        address_list.write_to(packet);
    }

    pub fn read(packet: &[u8], offset: &mut usize) -> tl_proto::TlResult<Box<[Address]>> {
        use tl_proto::TlError;

        let len = u32::read_from(packet, offset)? as usize;
        if len == 0 || len > PeerInfo::MAX_ADDRESSES {
            return Err(TlError::InvalidData);
        }

        let mut items = Vec::with_capacity(len);
        for _ in 0..len {
            items.push(Address::read_from(packet, offset)?);
        }

        Ok(items.into_boxed_slice())
    }
}
