use bytes::Bytes;
use tl_proto::{TlRead, TlWrite};

use crate::proto::{tl_block_id, tl_block_id_vec};

#[derive(Debug, Clone, PartialEq, Eq, TlRead, TlWrite)]
#[tl(boxed, id = "blockchain.data", scheme = "proto.tl")]
pub struct Data {
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
        proof: Bytes,
        block: Bytes,
        is_link: bool,
    },
    #[tl(id = "blockchain.blockFull.empty")]
    Empty,
}

#[derive(Debug, Clone, PartialEq, Eq, TlRead, TlWrite)]
#[tl(boxed, scheme = "proto.tl")]
pub enum PersistentStatePart {
    #[tl(id = "blockchain.persistentStatePart.found")]
    Found { data: Bytes },
    #[tl(id = "blockchain.persistentStatePart.notFound")]
    NotFound,
}

#[derive(Debug, Clone, PartialEq, Eq, TlRead, TlWrite)]
#[tl(boxed, scheme = "proto.tl")]
pub enum ArchiveInfo {
    #[tl(id = "blockchain.archiveInfo", size_hint = 8)]
    Found { id: u64 },
    #[tl(id = "blockchain.archiveNotFound")]
    NotFound,
}

/// Blockchain RPC models.
pub mod rpc {
    use super::*;

    #[derive(Clone, TlRead, TlWrite)]
    #[tl(boxed, id = "blockchain.getNextKeyBlockIds", scheme = "proto.tl")]
    pub struct GetNextKeyBlockIds {
        #[tl(with = "tl_block_id")]
        pub block_id: everscale_types::models::BlockId,
        pub max_size: u32,
    }

    #[derive(Clone, TlRead, TlWrite)]
    #[tl(boxed, id = "blockchain.getBlockFull", scheme = "proto.tl")]
    pub struct GetBlockFull {
        #[tl(with = "tl_block_id")]
        pub block_id: everscale_types::models::BlockId,
    }

    #[derive(Clone, TlRead, TlWrite)]
    #[tl(boxed, id = "blockchain.getNextBlockFull", scheme = "proto.tl")]
    pub struct GetNextBlockFull {
        #[tl(with = "tl_block_id")]
        pub prev_block_id: everscale_types::models::BlockId,
    }

    #[derive(Clone, TlRead, TlWrite)]
    #[tl(boxed, id = "blockchain.getPersistentStatePart", scheme = "proto.tl")]
    pub struct GetPersistentStatePart {
        #[tl(with = "tl_block_id")]
        pub block_id: everscale_types::models::BlockId,
        #[tl(with = "tl_block_id")]
        pub mc_block_id: everscale_types::models::BlockId,
        pub offset: u64,
        pub max_size: u64,
    }

    #[derive(Clone, TlRead, TlWrite)]
    #[tl(
        boxed,
        id = "blockchain.getArchiveInfo",
        size_hint = 4,
        scheme = "proto.tl"
    )]
    pub struct GetArchiveInfo {
        pub mc_seqno: u32,
    }

    #[derive(Clone, TlRead, TlWrite)]
    #[tl(
        boxed,
        id = "blockchain.getArchiveSlice",
        size_hint = 20,
        scheme = "proto.tl"
    )]
    pub struct GetArchiveSlice {
        pub archive_id: u64,
        pub offset: u64,
        pub max_size: u32,
    }
}
