use std::collections::BTreeMap;

use tl_proto::{TlRead, TlWrite};

pub(crate) mod points_btree_map {
    use tl_proto::{TlPacket, TlResult};
    use tycho_network::PeerId;

    use super::*;
    use crate::models::Digest;

    /// We assume that the number of points is limited.
    const MAX_SIZE: usize = 10_000;

    pub fn size_hint(items: &BTreeMap<PeerId, Digest>) -> usize {
        const PER_ITEM: usize = 32 + 32;

        4 + items.len() * PER_ITEM
    }

    pub fn write<P: TlPacket>(items: &BTreeMap<PeerId, Digest>, packet: &mut P) {
        packet.write_u32(items.len() as u32);

        for (peer_id, digest) in items {
            peer_id.write_to(packet);
            digest.inner().write_to(packet);
        }
    }

    pub fn read(data: &[u8], offset: &mut usize) -> TlResult<BTreeMap<PeerId, Digest>> {
        let len = u32::read_from(data, offset)? as usize;
        if len > MAX_SIZE {
            return Err(tl_proto::TlError::InvalidData);
        }

        let mut items = BTreeMap::new();
        for _ in 0..len {
            let peer_id = PeerId::read_from(data, offset)?;
            let digest = <Digest>::read_from(data, offset)?;
            items.insert(peer_id, digest);
        }

        Ok(items)
    }
}

pub(crate) mod evidence_btree_map {
    use tl_proto::{TlPacket, TlResult};
    use tycho_network::PeerId;

    use super::*;
    use crate::models::Signature;

    /// We assume that the number of points is limited.
    const MAX_SIZE: usize = 10_000;

    pub fn size_hint(items: &BTreeMap<PeerId, Signature>) -> usize {
        const PER_ITEM: usize = 32 + 64;

        4 + items.len() * PER_ITEM
    }

    pub fn write<P: TlPacket>(items: &BTreeMap<PeerId, Signature>, packet: &mut P) {
        packet.write_u32(items.len() as u32);

        for (peer_id, signature) in items {
            peer_id.write_to(packet);
            signature.write_to(packet);
        }
    }

    pub fn read(data: &[u8], offset: &mut usize) -> TlResult<BTreeMap<PeerId, Signature>> {
        let len = u32::read_from(data, offset)? as usize;
        if len > MAX_SIZE {
            return Err(tl_proto::TlError::InvalidData);
        }

        let mut items = BTreeMap::new();
        for _ in 0..len {
            let peer_id = PeerId::read_from(data, offset)?;
            let inner = <Signature>::read_from(data, offset)?;
            items.insert(peer_id, inner);
        }

        Ok(items)
    }
}