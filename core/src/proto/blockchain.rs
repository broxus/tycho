use std::num::{NonZeroU32, NonZeroU64};

use bytes::Bytes;
use tl_proto::{TlRead, TlWrite};
use tycho_block_util::tl::{block_id as tl_block_id, block_id_vec as tl_block_id_vec};

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
    pub data: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq, TlRead, TlWrite)]
#[tl(boxed, id = "blockchain.keyBlockIds", scheme = "proto.tl")]
pub struct KeyBlockIds {
    #[tl(with = "tl_block_id_vec")]
    pub block_ids: Vec<tycho_types::models::BlockId>,
    pub incomplete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, TlRead, TlWrite)]
#[tl(boxed, scheme = "proto.tl")]
pub enum BlockFull {
    #[tl(id = "blockchain.blockFull.found")]
    Found {
        #[tl(with = "tl_block_id")]
        block_id: tycho_types::models::BlockId,
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
    Found {
        size: NonZeroU64,
        chunk_size: NonZeroU32,
    },
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
    #[tl(id = "blockchain.archiveInfo.tooNew")]
    TooNew,
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
pub struct BlockData<T = Bytes> {
    pub data: T,
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
        pub block_id: tycho_types::models::BlockId,
        pub max_size: u32,
    }

    #[derive(Debug, Clone, TlRead, TlWrite)]
    #[tl(boxed, id = "blockchain.getBlockFull", scheme = "proto.tl")]
    pub struct GetBlockFull {
        #[tl(with = "tl_block_id")]
        pub block_id: tycho_types::models::BlockId,
    }

    #[derive(Debug, Clone, TlRead, TlWrite)]
    #[tl(boxed, id = "blockchain.getNextBlockFull", scheme = "proto.tl")]
    pub struct GetNextBlockFull {
        #[tl(with = "tl_block_id")]
        pub prev_block_id: tycho_types::models::BlockId,
    }

    #[derive(Debug, Clone, TlRead, TlWrite)]
    #[tl(boxed, id = "blockchain.getBlockDataChunk", scheme = "proto.tl")]
    pub struct GetBlockDataChunk {
        #[tl(with = "tl_block_id")]
        pub block_id: tycho_types::models::BlockId,
        pub offset: u32,
    }

    #[derive(Debug, Clone, TlRead, TlWrite)]
    #[tl(boxed, id = "blockchain.getKeyBlockProof", scheme = "proto.tl")]
    pub struct GetKeyBlockProof {
        #[tl(with = "tl_block_id")]
        pub block_id: tycho_types::models::BlockId,
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

    #[derive(Debug, Clone, TlRead, TlWrite)]
    #[tl(
        boxed,
        id = "blockchain.getPersistentShardStateInfo",
        scheme = "proto.tl"
    )]
    pub struct GetPersistentShardStateInfo {
        #[tl(with = "tl_block_id")]
        pub block_id: tycho_types::models::BlockId,
    }

    #[derive(Debug, Clone, TlRead, TlWrite)]
    #[tl(
        boxed,
        id = "blockchain.getPersistentShardStateChunk",
        scheme = "proto.tl"
    )]
    pub struct GetPersistentShardStateChunk {
        #[tl(with = "tl_block_id")]
        pub block_id: tycho_types::models::BlockId,
        pub offset: u64,
    }

    #[derive(Debug, Clone, TlRead, TlWrite)]
    #[tl(
        boxed,
        id = "blockchain.getPersistentQueueStateInfo",
        scheme = "proto.tl"
    )]
    pub struct GetPersistentQueueStateInfo {
        #[tl(with = "tl_block_id")]
        pub block_id: tycho_types::models::BlockId,
    }

    #[derive(Debug, Clone, TlRead, TlWrite)]
    #[tl(
        boxed,
        id = "blockchain.getPersistentQueueStateChunk",
        scheme = "proto.tl"
    )]
    pub struct GetPersistentQueueStateChunk {
        #[tl(with = "tl_block_id")]
        pub block_id: tycho_types::models::BlockId,
        pub offset: u64,
    }
}
