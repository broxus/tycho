use tl_proto::{TlRead, TlWrite};
use tycho_network::proto::dht::rpc;

#[derive(Debug, Clone, PartialEq, Eq, TlRead, TlWrite)]
#[tl(boxed, id = "overlay.ping", scheme = "proto.tl")]
pub struct Ping;

#[derive(Debug, Clone, PartialEq, Eq, TlRead, TlWrite)]
#[tl(boxed, id = "overlay.pong", scheme = "proto.tl")]
pub struct Pong;

/// A universal response for the all queries.
#[derive(Debug, Clone, PartialEq, Eq, TlRead, TlWrite)]
#[tl(boxed, scheme = "proto.tl")]
pub enum Response<T>
where
    T: tl_proto::TlWrite<Repr = tl_proto::Boxed>,
{
    #[tl(id = "publicOverlay.response.ok")]
    Ok(T),
    #[tl(id = "publicOverlay.response.error")]
    Err,
}

#[derive(Clone, TlRead, TlWrite)]
#[tl(boxed, id = "publicOverlay.getNextKeyBlockIds", scheme = "proto.tl")]
pub struct GetNextKeyBlockIds {
    #[tl(with = "tl_block_id")]
    pub block: everscale_types::models::BlockId,
    pub max_size: u32,
}

#[derive(Clone, TlRead, TlWrite)]
#[tl(boxed, id = "publicOverlay.keyBlockIdsResponse", scheme = "proto.tl")]
pub struct KeyBlockIdsResponse {
    #[tl(with = "tl_block_id_vec")]
    pub blocks: Vec<everscale_types::models::BlockId>,
    pub incomplete: bool,
}

mod tl_block_id {
    use everscale_types::models::{BlockId, ShardIdent};
    use everscale_types::prelude::HashBytes;
    use tl_proto::{TlPacket, TlRead, TlResult, TlWrite};

    pub const SIZE_HINT: usize = 80;

    pub const fn size_hint(_: &BlockId) -> usize {
        SIZE_HINT
    }

    pub fn write<P: TlPacket>(block_id: &BlockId, packet: &mut P) {
        block_id.shard.workchain().write_to(packet);
        block_id.shard.prefix().write_to(packet);
        block_id.seqno.write_to(packet);
        block_id.root_hash.0.write_to(packet);
        block_id.file_hash.0.write_to(packet);
    }

    pub fn read(packet: &[u8], offset: &mut usize) -> TlResult<BlockId> {
        let workchain = i32::read_from(packet, offset)?;
        let prefix = u64::read_from(packet, offset)?;
        let seqno = u32::read_from(packet, offset)?;

        let shard = ShardIdent::new(workchain, prefix);

        let shard = match shard {
            None => return Err(tl_proto::TlError::InvalidData),
            Some(shard) => shard,
        };

        let root_hash = HashBytes(<[u8; 32]>::read_from(packet, offset)?);
        let file_hash = HashBytes(<[u8; 32]>::read_from(packet, offset)?);

        Ok(BlockId {
            shard,
            seqno,
            root_hash,
            file_hash,
        })
    }
}

mod tl_block_id_vec {
    use everscale_types::models::BlockId;
    use tl_proto::{TlError, TlPacket, TlRead, TlResult};

    use crate::proto::overlay::tl_block_id;

    pub fn size_hint(ids: &[BlockId]) -> usize {
        4 + ids.len() * tl_block_id::SIZE_HINT
    }

    pub fn write<P: TlPacket>(blocks: &[BlockId], packet: &mut P) {
        packet.write_u32(blocks.len() as u32);
        for block in blocks {
            tl_block_id::write(block, packet);
        }
    }

    pub fn read(packet: &[u8], offset: &mut usize) -> TlResult<Vec<BlockId>> {
        let len = u32::read_from(packet, offset)?;
        if *offset + len as usize * tl_block_id::SIZE_HINT > packet.len() {
            return Err(TlError::UnexpectedEof);
        }

        let mut ids = Vec::with_capacity(len as usize);
        for _ in 0..len {
            ids.push(tl_block_id::read(packet, offset)?);
        }
        Ok(ids)
    }
}
