use std::num::{NonZeroU32, NonZeroU64};

use bytes::Bytes;
use tl_proto::{TlRead, TlWrite};
use tycho_block_util::tl::{block_id as tl_block_id, block_id_vec as tl_block_id_vec};
use tycho_util::tl;

pub type BigBytes = tl::BigBytes<{ 100 << 20 }>; // 100 MB

/// Data for computing a public overlay id.
#[derive(Debug, Clone, PartialEq, Eq, TlRead, TlWrite)]
#[tl(boxed, id = "blockchain.overlayIdData", scheme = "proto.tl")]
pub struct OverlayIdData {
    pub zerostate_root_hash: [u8; 32],
    pub zerostate_file_hash: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, TlRead, TlWrite)]
#[tl(boxed, id = "blockchain.data", scheme = "proto.tl")]
pub struct Data {
    #[tl(with = "BigBytes")]
    pub data: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq, TlRead, TlWrite)]
#[tl(boxed, id = "blockchain.keyBlockIds", scheme = "proto.tl")]
pub struct KeyBlockIds {
    #[tl(with = "tl_block_id_vec")]
    pub block_ids: Vec<everscale_types::models::BlockId>,
    pub incomplete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, TlRead, TlWrite)]
#[tl(boxed, scheme = "proto.tl")]
pub enum BlockFull {
    #[tl(id = "blockchain.blockFull.found")]
    Found {
        #[tl(with = "tl_block_id")]
        block_id: everscale_types::models::BlockId,
        block: BlockData,
        proof: Bytes,
        queue_diff: Bytes,
    },
    #[tl(id = "blockchain.blockFull.notFound")]
    NotFound,
}

#[derive(Debug, Clone, PartialEq, Eq, TlRead, TlWrite)]
#[tl(boxed, scheme = "proto.tl")]
pub enum KeyBlockProof {
    #[tl(id = "blockchain.keyBlockProof.found")]
    Found { proof: Bytes },
    #[tl(id = "blockchain.keyBlockProof.notFound")]
    NotFound,
}

#[derive(Debug, Clone, PartialEq, Eq, TlRead, TlWrite)]
#[tl(boxed, scheme = "proto.tl")]
pub enum PersistentStateInfo {
    #[tl(id = "blockchain.persistentStateInfo.found")]
    Found { size: u64 },
    #[tl(id = "blockchain.persistentStateInfo.notFound")]
    NotFound,
}

#[derive(Debug, Clone, PartialEq, Eq, TlRead, TlWrite)]
#[tl(boxed, scheme = "proto.tl")]
pub enum ArchiveInfo {
    #[tl(id = "blockchain.archiveInfo.found", size_hint = 20)]
    Found {
        id: u64,
        size: NonZeroU64,
        chunk_size: NonZeroU32,
    },
    #[tl(id = "blockchain.archiveInfo.notFound")]
    NotFound,
}

#[derive(Debug, Clone, PartialEq, Eq, TlRead, TlWrite)]
#[tl(boxed, id = "blockchain.broadcast.message", scheme = "proto.tl")]
pub struct MessageBroadcastRef<'tl> {
    pub data: &'tl [u8],
}

#[derive(Debug, Clone, PartialEq, Eq, TlRead, TlWrite)]
#[tl(boxed, id = "blockchain.blockData", scheme = "proto.tl")]
pub struct BlockData {
    pub data: Bytes,
    pub size: NonZeroU32,
    pub chunk_size: NonZeroU32,
}

/// Blockchain RPC models.
pub mod rpc {
    use super::*;

    #[derive(Debug, Clone, TlRead, TlWrite)]
    #[tl(boxed, id = "blockchain.getNextKeyBlockIds", scheme = "proto.tl")]
    pub struct GetNextKeyBlockIds {
        #[tl(with = "tl_block_id")]
        pub block_id: everscale_types::models::BlockId,
        pub max_size: u32,
    }

    #[derive(Debug, Clone, TlRead, TlWrite)]
    #[tl(boxed, id = "blockchain.getBlockFull", scheme = "proto.tl")]
    pub struct GetBlockFull {
        #[tl(with = "tl_block_id")]
        pub block_id: everscale_types::models::BlockId,
    }

    #[derive(Debug, Clone, TlRead, TlWrite)]
    #[tl(boxed, id = "blockchain.getNextBlockFull", scheme = "proto.tl")]
    pub struct GetNextBlockFull {
        #[tl(with = "tl_block_id")]
        pub prev_block_id: everscale_types::models::BlockId,
    }

    #[derive(Debug, Clone, TlRead, TlWrite)]
    #[tl(boxed, id = "blockchain.getBlockDataChunk", scheme = "proto.tl")]
    pub struct GetBlockDataChunk {
        #[tl(with = "tl_block_id")]
        pub block_id: everscale_types::models::BlockId,
        pub offset: u32,
    }

    #[derive(Debug, Clone, TlRead, TlWrite)]
    #[tl(boxed, id = "blockchain.getKeyBlockProof", scheme = "proto.tl")]
    pub struct GetKeyBlockProof {
        #[tl(with = "tl_block_id")]
        pub block_id: everscale_types::models::BlockId,
    }

    #[derive(Debug, Clone, TlRead, TlWrite)]
    #[tl(boxed, id = "blockchain.getPersistentStateInfo", scheme = "proto.tl")]
    pub struct GetPersistentStateInfo {
        #[tl(with = "tl_block_id")]
        pub block_id: everscale_types::models::BlockId,
    }

    #[derive(Debug, Clone, TlRead, TlWrite)]
    #[tl(boxed, id = "blockchain.getPersistentStatePart", scheme = "proto.tl")]
    pub struct GetPersistentStatePart {
        #[tl(with = "tl_block_id")]
        pub block_id: everscale_types::models::BlockId,
        pub limit: u32,
        pub offset: u64,
    }

    #[derive(Debug, Clone, TlRead, TlWrite)]
    #[tl(
        boxed,
        id = "blockchain.getArchiveInfo",
        size_hint = 4,
        scheme = "proto.tl"
    )]
    pub struct GetArchiveInfo {
        pub mc_seqno: u32,
    }

    #[derive(Debug, Clone, TlRead, TlWrite)]
    #[tl(
        boxed,
        id = "blockchain.getArchiveChunk",
        size_hint = 16,
        scheme = "proto.tl"
    )]
    pub struct GetArchiveChunk {
        pub archive_id: u64,
        pub offset: u64,
    }
}
