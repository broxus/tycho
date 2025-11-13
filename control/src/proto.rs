use std::net::SocketAddr;
use std::num::{NonZeroU32, NonZeroU64};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tycho_types::models::{
    BlockId, BlockIdShort, BlockchainConfig, GlobalVersion, ShardAccount, StdAddr,
};
use tycho_types::prelude::*;
use tycho_util::{FastHashSet, serde_helpers};

use crate::error::ServerResult;
use crate::mempool;

#[tarpc::service]
pub trait ControlServer {
    /// Ping a node. Returns node timestamp in milliseconds.
    async fn ping() -> u64;

    /// Get node status.
    async fn get_status() -> ServerResult<NodeStatusResponse>;

    /// Trigger manual GC for archives.
    async fn trigger_archives_gc(req: TriggerGcRequest);

    /// Trigger manual GC for blocks.
    async fn trigger_blocks_gc(req: TriggerGcRequest);

    /// Trigger manual GC for states.
    async fn trigger_states_gc(req: TriggerGcRequest);

    /// Trigger manual compaction.
    async fn trigger_compaction(req: TriggerCompactionRequest);

    /// Sets memory profiler state. Returns whether the state was changed.
    async fn set_memory_profiler_enabled(enabled: bool) -> bool;

    /// Returns memory profiler dump.
    async fn dump_memory_profiler() -> ServerResult<Vec<u8>>;

    /// Get node neighbours info
    async fn get_neighbours_info() -> ServerResult<NeighboursInfoResponse>;

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

    /// Returns list of all overlays.
    async fn get_overlay_ids() -> ServerResult<OverlayIdsResponse>;

    /// Get overlay peers.
    async fn get_overlay_peers(req: OverlayPeersRequest) -> ServerResult<OverlayPeersResponse>;

    /// Search for `k` closest nodes known to `peer_id` that store `key`.
    async fn dht_find_node(req: DhtFindNodeRequest) -> ServerResult<DhtFindNodeResponse>;

    /// Signs an elections payload.
    async fn sign_elections_payload(
        req: ElectionsPayloadRequest,
    ) -> ServerResult<ElectionsPayloadResponse>;

    async fn mempool_ban(req: mempool::BanRequest) -> ServerResult<String>;

    async fn mempool_unban(peer_id: HashBytes) -> ServerResult<()>;

    async fn mempool_list_events(
        req: mempool::ListEventsRequest,
    ) -> ServerResult<Vec<mempool::MempoolEventDisplay>>;

    async fn mempool_delete_events(millis: std::ops::Range<u64>) -> ServerResult<()>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeStatusResponse {
    pub status_at: u32,
    pub node_info: NodeInfo,
    pub init_block_id: Option<BlockId>,
    pub last_applied_block: Option<LastAppliedBlock>,
    pub validator_status: Option<ValidatorStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub version: String,
    pub build: String,

    // TODO: Somehow expose tycho_network::Address?
    pub public_addr: String,
    pub local_addr: SocketAddr,
    pub adnl_id: HashBytes,
    pub collator: Option<CollatorInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LastAppliedBlock {
    pub block_id: BlockId,
    pub gen_utime: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidatorStatus {
    pub public_key: HashBytes,
    pub in_current_vset: bool,
    pub in_next_vset: bool,
    pub is_elected: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollatorInfo {
    pub global_version: GlobalVersion,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TriggerGcRequest {
    Exact(u32),
    Distance(u32),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerCompactionRequest {
    pub database: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BroadcastExtMsgRequest {
    /// A BOC with a [`Message`].
    ///
    /// [`Message`]: tycho_types::models::Message
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
    pub fn parse(&self) -> Result<ParsedAccountState, tycho_types::boc::BocReprError> {
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
    pub fn parse(&self) -> Result<ParsedBlockchainConfigResponse, tycho_types::boc::BocReprError> {
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
    pub signature_id: Option<i32>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ElectionsPayloadResponse {
    // TODO: Add `serde(with = "base64")`
    pub data: Bytes,
    pub public_key: HashBytes,
    #[serde(with = "serde_helpers::signature")]
    pub signature: Box<[u8; 64]>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NeighboursInfoResponse {
    pub neighbours: Vec<NeighbourInfo>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NeighbourInfo {
    pub id: HashBytes,
    pub expires_at: u32,
    pub score: u8,
    pub failed_requests: u64,
    pub total_requests: u64,
    pub roundtrip_ms: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OverlayIdsResponse {
    pub public_overlays: FastHashSet<HashBytes>,
    pub private_overlays: FastHashSet<HashBytes>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OverlayPeersRequest {
    pub overlay_id: HashBytes,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OverlayPeersResponse {
    pub overlay_type: OverlayType,
    pub peers: Vec<OverlayPeer>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OverlayPeer {
    pub peer_id: HashBytes,
    pub entry_created_at: Option<u32>,
    pub info: Option<PeerInfo>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum OverlayType {
    Private,
    Public,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DhtFindNodeRequest {
    /// Remove peer id or `None` for local.
    pub peer_id: Option<HashBytes>,
    pub key: HashBytes,
    pub k: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DhtFindNodeResponse {
    pub nodes: Vec<DhtFindNodeResponseItem>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DhtFindNodeResponseItem {
    pub peer_id: HashBytes,
    pub info: PeerInfo,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PeerInfo {
    pub address_list: Vec<String>,
    pub created_at: u32,
    pub expires_at: u32,
}
