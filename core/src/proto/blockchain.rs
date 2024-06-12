use bytes::Bytes;
use tl_proto::{TlRead, TlWrite};

use crate::proto::{tl_block_id, tl_block_id_vec};

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
pub enum PersistentStateInfo {
    #[tl(id = "blockchain.persistentStateInfo.found")]
    Found { size: u64 },
    #[tl(id = "blockchain.persistentStateInfo.notFound")]
    NotFound,
}

#[derive(Debug, Clone, PartialEq, Eq, TlRead, TlWrite)]
#[tl(boxed, scheme = "proto.tl")]
pub enum ArchiveInfo {
    #[tl(id = "blockchain.archiveInfo.found", size_hint = 8)]
    Found { id: u64 },
    #[tl(id = "blockchain.archiveInfo.notFound")]
    NotFound,
}

#[derive(Debug, Clone, PartialEq, Eq, TlRead, TlWrite)]
#[tl(boxed, id = "blockchain.broadcast.message", scheme = "proto.tl")]
pub struct MessageBroadcastRef<'tl> {
    pub data: &'tl [u8],
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
        id = "blockchain.getArchiveSlice",
        size_hint = 20,
        scheme = "proto.tl"
    )]
    pub struct GetArchiveSlice {
        pub archive_id: u64,
        pub limit: u32,
        pub offset: u64,
    }
}
