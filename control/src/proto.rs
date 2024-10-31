use std::net::SocketAddr;
use std::num::{NonZeroU32, NonZeroU64};

use bytes::Bytes;
use everscale_types::models::{BlockId, BlockIdShort, BlockchainConfig, ShardAccount, StdAddr};
use everscale_types::prelude::*;
use serde::{Deserialize, Serialize};
use tycho_util::serde_helpers;

use crate::error::ServerResult;

#[tarpc::service]
pub trait ControlServer {
    /// Ping a node. Returns node timestamp in milliseconds.
    async fn ping() -> u64;

    /// Returns a node info.
    async fn get_node_info() -> NodeInfoResponse;

    /// Trigger manual GC for archives.
    async fn trigger_archives_gc(req: TriggerGcRequest);

    /// Trigger manual GC for blocks.
    async fn trigger_blocks_gc(req: TriggerGcRequest);

    /// Trigger manual GC for states.
    async fn trigger_states_gc(req: TriggerGcRequest);

    /// Sets memory profiler state. Returns whether the state was changed.
    async fn set_memory_profiler_enabled(enabled: bool) -> bool;

    /// Returns memory profiler dump.
    async fn dump_memory_profiler() -> ServerResult<Vec<u8>>;

    /// Broadcast a message to validators.
    async fn broadcast_external_message(req: BroadcastExtMsgRequest) -> ServerResult<()>;

    /// Get account state.
    async fn get_account_state(req: AccountStateRequest) -> ServerResult<AccountStateResponse>;

    /// Get blockchain config.
    async fn get_blockchain_config() -> ServerResult<BlockchainConfigResponse>;

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

    /// Signs an elections payload.
    async fn sign_elections_payload(
        req: ElectionsPayloadRequest,
    ) -> ServerResult<ElectionsPayloadResponse>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfoResponse {
    // TODO: Somehow expose tycho_network::Address?
    pub public_addr: String,
    pub local_addr: SocketAddr,
    pub adnl_id: HashBytes,
    pub validator_public_key: Option<HashBytes>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "ty", content = "seqno")]
pub enum TriggerGcRequest {
    Exact(u32),
    Distance(u32),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BroadcastExtMsgRequest {
    /// A BOC with a [`Message`].
    ///
    /// [`Message`]: everscale_types::models::Message
    pub message: Bytes,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountStateRequest {
    pub address: StdAddr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountStateResponse {
    pub mc_seqno: u32,
    pub gen_utime: u32,

    /// A BOC with a [`ShardAccount`].
    ///
    /// NOTE: [`BocRepr`] cannot be safely used here because
    /// we must hold a state tracker handle to delay the GC,
    /// and it is very inconvenient to pass it around here.
    pub state: Bytes,
}

impl AccountStateResponse {
    pub fn parse(&self) -> Result<ParsedAccountState, everscale_types::boc::BocReprError> {
        Ok(ParsedAccountState {
            mc_seqno: self.mc_seqno,
            gen_utime: self.gen_utime,
            state: BocRepr::decode(&self.state)?,
        })
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ParsedAccountState {
    pub mc_seqno: u32,
    pub gen_utime: u32,
    #[serde(flatten)]
    pub state: ShardAccount,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockchainConfigResponse {
    pub global_id: i32,
    pub mc_seqno: u32,
    pub gen_utime: u32,

    /// A BOC with a [`BlockchainConfig`].
    pub config: Bytes,
}

impl BlockchainConfigResponse {
    pub fn parse(
        &self,
    ) -> Result<ParsedBlockchainConfigResponse, everscale_types::boc::BocReprError> {
        Ok(ParsedBlockchainConfigResponse {
            global_id: self.global_id,
            mc_seqno: self.mc_seqno,
            gen_utime: self.gen_utime,
            config: BocRepr::decode(&self.config)?,
        })
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ParsedBlockchainConfigResponse {
    pub global_id: i32,
    pub mc_seqno: u32,
    pub gen_utime: u32,
    #[serde(flatten)]
    pub config: BlockchainConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockRequest {
    pub block_id: BlockId,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum BlockResponse {
    Found { data: Bytes },
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
    pub data: Bytes,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BlockListRequest {
    pub continuation: Option<BlockIdShort>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BlockListResponse {
    pub blocks: Vec<BlockId>,
    pub continuation: Option<BlockIdShort>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ElectionsPayloadRequest {
    pub election_id: u32,
    pub address: HashBytes,
    pub stake_factor: u32,
    pub public_key: HashBytes,
    pub adnl_addr: HashBytes,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ElectionsPayloadResponse {
    // TODO: Add `serde(with = "base64")`
    pub data: Bytes,
    pub public_key: HashBytes,
    #[serde(with = "serde_helpers::signature")]
    pub signature: Box<[u8; 64]>,
}
