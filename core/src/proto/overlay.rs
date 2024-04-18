use bytes::Bytes;
use tl_proto::{TlRead, TlWrite};

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
    Err(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq, TlRead, TlWrite)]
#[tl(boxed, id = "publicOverlay.data", scheme = "proto.tl")]
pub struct Data {
    pub data: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq, TlRead, TlWrite)]
#[tl(boxed, id = "publicOverlay.keyBlockIds", scheme = "proto.tl")]
pub struct KeyBlockIds {
    #[tl(with = "tl_block_id_vec")]
    pub blocks: Vec<everscale_types::models::BlockId>,
    pub incomplete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, TlRead, TlWrite)]
#[tl(boxed, scheme = "proto.tl")]
pub enum BlockFull {
    #[tl(id = "publicOverlay.blockFull.found")]
    Found {
        #[tl(with = "tl_block_id")]
        block_id: everscale_types::models::BlockId,
        proof: Bytes,
        block: Bytes,
        is_link: bool,
    },
    #[tl(id = "publicOverlay.blockFull.empty")]
    Empty,
}

#[derive(Debug, Clone, PartialEq, Eq, TlRead, TlWrite)]
#[tl(boxed, scheme = "proto.tl")]
pub enum PersistentStatePart {
    #[tl(id = "publicOverlay.persistentStatePart.found")]
    Found { data: Bytes },
    #[tl(id = "publicOverlay.persistentStatePart.notFound")]
    NotFound,
}

#[derive(Debug, Clone, PartialEq, Eq, TlRead, TlWrite)]
#[tl(boxed, scheme = "proto.tl")]
pub enum ArchiveInfo {
    #[tl(id = "publicOverlay.archiveInfo", size_hint = 8)]
    Found { id: u64 },
    #[tl(id = "publicOverlay.archiveNotFound")]
    NotFound,
}

/// Overlay RPC models.
pub mod rpc {
    use super::*;

    #[derive(Clone, TlRead, TlWrite)]
    #[tl(boxed, id = "publicOverlay.getNextKeyBlockIds", scheme = "proto.tl")]
    pub struct GetNextKeyBlockIds {
        #[tl(with = "tl_block_id")]
        pub block: everscale_types::models::BlockId,
        pub max_size: u32,
    }

    #[derive(Clone, TlRead, TlWrite)]
    #[tl(boxed, id = "publicOverlay.getBlockFull", scheme = "proto.tl")]
    pub struct GetBlockFull {
        #[tl(with = "tl_block_id")]
        pub block: everscale_types::models::BlockId,
    }

    #[derive(Clone, TlRead, TlWrite)]
    #[tl(boxed, id = "publicOverlay.getNextBlockFull", scheme = "proto.tl")]
    pub struct GetNextBlockFull {
        #[tl(with = "tl_block_id")]
        pub prev_block: everscale_types::models::BlockId,
    }

    #[derive(Clone, TlRead, TlWrite)]
    #[tl(
        boxed,
        id = "publicOverlay.getPersistentStatePart",
        scheme = "proto.tl"
    )]
    pub struct GetPersistentStatePart {
        #[tl(with = "tl_block_id")]
        pub block: everscale_types::models::BlockId,
        #[tl(with = "tl_block_id")]
        pub mc_block: everscale_types::models::BlockId,
        pub offset: u64,
        pub max_size: u64,
    }

    #[derive(Clone, TlRead, TlWrite)]
    #[tl(
        boxed,
        id = "publicOverlay.getArchiveInfo",
        size_hint = 4,
        scheme = "proto.tl"
    )]
    pub struct GetArchiveInfo {
        pub mc_seqno: u32,
    }

    #[derive(Clone, TlRead, TlWrite)]
    #[tl(
        boxed,
        id = "publicOverlay.getArchiveSlice",
        size_hint = 20,
        scheme = "proto.tl"
    )]
    pub struct GetArchiveSlice {
        pub archive_id: u64,
        pub offset: u64,
        pub max_size: u32,
    }
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
