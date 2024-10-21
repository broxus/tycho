use std::num::{NonZeroU32, NonZeroU64};

use everscale_types::models::BlockId;
use serde::{Deserialize, Serialize};
use tycho_core::block_strider::ManualGcTrigger;

use crate::error::ServerResult;

#[tarpc::service]
pub trait ControlServer {
    /// Ping a node. Returns node timestamp in milliseconds.
    async fn ping() -> u64;

    /// Trigger manual GC for archives.
    async fn trigger_archives_gc(trigger: ManualGcTrigger);

    /// Trigger manual GC for blocks.
    async fn trigger_blocks_gc(trigger: ManualGcTrigger);

    /// Trigger manual GC for states.
    async fn trigger_states_gc(trigger: ManualGcTrigger);

    /// Sets memory profiler state. Returns whether the state was changed.
    async fn set_memory_profiler_enabled(enabled: bool) -> bool;

    /// Returns memory profiler dump.
    async fn dump_memory_profiler() -> ServerResult<Vec<u8>>;

    /// Get block bytes
    async fn get_block(req: BlockRequest) -> ServerResult<BlockResponse>;

    /// Get proof bytes.
    async fn get_block_proof(req: BlockRequest) -> ServerResult<BlockResponse>;

    /// Get queue bytes.
    async fn get_queue_diff(req: BlockRequest) -> ServerResult<BlockResponse>;

    /// Get archive id
    async fn get_archive_info(req: ArchiveInfoRequest) -> ServerResult<ArchiveInfoResponse>;

    /// Download archive slice.
    async fn get_archive_chunk(req: ArchiveSliceRequest) -> ServerResult<ArchiveSliceResponse>;

    /// Returns list of all archive ids.
    async fn get_archive_ids() -> ServerResult<Vec<ArchiveInfo>>;

    /// Returns list of all block ids.
    async fn get_block_ids(req: BlockListRequest) -> ServerResult<BlockListResponse>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockRequest {
    pub block_id: BlockId,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum BlockResponse {
    Found { data: Vec<u8> },
    NotFound,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveInfoRequest {
    pub mc_seqno: u32,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ArchiveInfo {
    pub id: u32,
    pub size: NonZeroU64,
    pub chunk_size: NonZeroU32,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ArchiveInfoResponse {
    Found(ArchiveInfo),
    TooNew,
    NotFound,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveSliceRequest {
    pub archive_id: u32,
    pub offset: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ArchiveSliceResponse {
    pub data: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BlockListRequest {
    pub continuation: Option<BlockId>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BlockListResponse {
    pub blocks: Vec<BlockId>,
    pub continuation: Option<BlockId>,
}
