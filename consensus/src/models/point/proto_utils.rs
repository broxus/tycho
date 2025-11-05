use tl_proto::{RawBytes, TlPacket, TlRead, TlResult, TlWrite};
use tycho_network::PeerId;
use tycho_util::FastHashMap;

use crate::models::Round;
use crate::models::point::{Digest, PointData, Signature};

#[derive(TlWrite)]
#[tl(boxed, id = "consensus.point", scheme = "proto.tl")]
pub struct PointWrite<'a, T>
where
    T: AsRef<[u8]>,
{
    pub digest: &'a Digest,
    pub signature: &'a Signature,
    pub body: PointBodyWrite<'a, T>,
}

#[derive(TlRead)]
#[tl(boxed, id = "consensus.point", scheme = "proto.tl")]
pub struct PointRead<'tl> {
    pub digest: Digest,
    pub signature: Signature,
    pub body: PointBodyRead<'tl>,
}

#[derive(TlRead)]
#[tl(boxed, id = "consensus.point", scheme = "proto.tl")]
pub struct PointRawRead<'tl> {
    pub digest: &'tl Digest,
    pub signature: &'tl Signature,
    pub body: RawBytes<'tl, tl_proto::Boxed>,
}

#[derive(TlWrite)]
#[tl(boxed, id = "consensus.pointBody", scheme = "proto.tl")]
pub struct PointBodyWrite<'a, T>
where
    T: AsRef<[u8]>,
{
    pub author: &'a PeerId,
    pub round: Round,
    pub payload: &'a [T],
    pub data: &'a PointData,
}

#[derive(TlRead)]
#[tl(boxed, id = "consensus.pointBody", scheme = "proto.tl")]
pub struct PointBodyRead<'tl> {
    pub author: PeerId,
    pub round: Round,
    pub payload: Vec<&'tl [u8]>,
    pub data: PointData,
}

impl PointRawRead<'_> {
    pub fn author(&self) -> TlResult<&PeerId> {
        #[derive(TlRead)]
        #[tl(boxed, id = "consensus.pointBody", scheme = "proto.tl")]
        struct PointBodyPrefix<'tl> {
            author: &'tl PeerId,
        }
        let body = <PointBodyPrefix<'_>>::read_from(&mut self.body.as_ref())?;
        Ok(body.author)
    }

    pub fn payload(&self) -> TlResult<Vec<&[u8]>> {
        #[derive(TlRead)]
        #[tl(boxed, id = "consensus.pointBody", scheme = "proto.tl")]
        struct PointBodyPrefix<'tl> {
            _author: &'tl PeerId,
            _round: Round,
            payload: Vec<&'tl [u8]>,
        }
        let body = <PointBodyPrefix<'_>>::read_from(&mut self.body.as_ref())?;
        Ok(body.payload)
    }
}

pub mod digests_map {
    use super::*;

    pub fn size_hint(items: &FastHashMap<PeerId, Digest>) -> usize {
        4 + (items.len() * (32 + 32))
    }

    pub fn write<P: TlPacket>(items: &FastHashMap<PeerId, Digest>, packet: &mut P) {
        hash_map::write(items, packet);
    }

    pub fn read(data: &mut &[u8]) -> TlResult<FastHashMap<PeerId, Digest>> {
        hash_map::read(data)
    }
}

pub mod signatures_map {
    use super::*;

    pub fn size_hint(items: &FastHashMap<PeerId, Signature>) -> usize {
        4 + (items.len() * (32 + 64))
    }

    pub fn write<P: TlPacket>(items: &FastHashMap<PeerId, Signature>, packet: &mut P) {
        hash_map::write(items, packet);
    }

    pub fn read(data: &mut &[u8]) -> TlResult<FastHashMap<PeerId, Signature>> {
        hash_map::read(data)
    }
}

mod hash_map {
    use ahash::HashMapExt;
    use tl_proto::TlError;

    use super::*;
    use crate::models::PeerCount;

    pub fn write<P, T>(items: &FastHashMap<PeerId, T>, packet: &mut P)
    where
        P: TlPacket,
        T: TlWrite,
    {
        packet.write_u32(items.len() as u32);

        for (peer_id, item) in items {
            peer_id.write_to(packet);
            item.write_to(packet);
        }
    }

    pub fn read<'a, T>(data: &mut &'a [u8]) -> TlResult<FastHashMap<PeerId, T>>
    where
        T: TlRead<'a>,
    {
        let len = u32::read_from(data)? as usize;
        if len > PeerCount::MAX.full() {
            tracing::debug!(%len, "Too large map");
            return Err(TlError::InvalidData);
        }

        let mut items = FastHashMap::with_capacity(len);
        for _ in 0..len {
            let peer_id = PeerId::read_from(data)?;
            let item = <T>::read_from(data)?;
            if items.insert(peer_id, item).is_some() {
                tracing::debug!(%peer_id, "Map already contains item for this author");
                return Err(TlError::InvalidData);
            }
        }

        Ok(items)
    }
}
