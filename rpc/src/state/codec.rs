use anyhow::{Context, Result, bail, ensure};
use tycho_storage::kv::InstanceId;
use tycho_types::models::{BlockId, BlockIdShort, ShardIdent, StdAddr};
use tycho_types::prelude::HashBytes;

use super::TransactionMask;

pub const LAYOUT_VERSION: u8 = 2;
pub const CONTROL_STATE_KEY: &[u8] = b"control_state";
pub const ACTIVE_PARTITION_KEY: &[u8] = b"active_partition";
pub const VISIBLE_FRONTIER_KEY: &[u8] = b"visible_frontier";
pub const MANIFEST_EPOCH_KEY: &[u8] = b"manifest_epoch";
pub const GC_INTENT_KEY: &[u8] = b"gc_intent";
pub const RPC_LAYOUT_MARKER_KEY: &[u8] = b"rpc_layout_marker";
pub const RPC_LAYOUT_MARKER_VALUE: &[u8] = b"tycho-rpc-layout-v3";
pub const TAIL_IDENTITY_KEY: &[u8] = b"tail_identity";
pub const PARTITION_ID_LEN: usize = 8;
pub const SHORT_BLOCK_ID_LEN: usize = 16;
pub const BLOCK_ID_LEN: usize = 80;
pub const ACCOUNT_KEY_LEN: usize = 33;
pub const TAIL_PAYLOAD_KEY_LEN: usize = 41;
pub const TRANSACTION_HASH_LEN: usize = 32;
pub const TRANSACTION_VALUE_PREFIX_LEN: usize = 4;
const CONTROL_STATE_LEN: usize = 1 + 16 + 8 + 8 + 8 + 1 + 8;
const ACTIVE_PARTITION_LEN: usize = 1 + PARTITION_ID_LEN;
const VISIBLE_FRONTIER_LEN: usize = 1 + BLOCK_ID_LEN;
const MANIFEST_EPOCH_LEN: usize = 1 + 8;
const MANIFEST_BOUND_LEN: usize = 1 + BLOCK_ID_LEN + 4 + 8 + 4;
const MANIFEST_RECORD_LEN: usize = 1 + 1 + PARTITION_ID_LEN + MANIFEST_BOUND_LEN * 2 + 8 * 5 + 1 + 8 + 8;
pub const PARTITION_COMMIT_KEY_LEN: usize = 4 + SHORT_BLOCK_ID_LEN;
pub const PARTITION_COMMIT_VALUE_LEN: usize = 1 + BLOCK_ID_LEN + 32 + 8 * 7 + 4;
const FILTER_NAMESPACE_DESCRIPTOR_LEN: usize = 1 + 1 + 1 + 1 + 4 + 8 + 8 + 8 + 1 + 8 + 8 + 32;
const FILTER_CATALOG_DESCRIPTOR_LEN: usize = 1 + PARTITION_ID_LEN + 16 + 32 + FILTER_NAMESPACE_DESCRIPTOR_LEN * 4;
pub const GC_INTENT_LEN: usize = 1 + 1 + 16 + 8 + 32 + 8 + 8 + 4 + 8 + 32;
pub const TAIL_IDENTITY_LEN: usize = 1 + 1 + 16;
const OPTIONAL_GENERATION_LEN: usize = 1 + 8;
pub const TAIL_ACCOUNT_LOCATOR_LEN: usize = 1 + TRANSACTION_HASH_LEN + 8 + OPTIONAL_GENERATION_LEN;
pub const TAIL_HASH_LOCATOR_LEN: usize = 1 + TAIL_PAYLOAD_KEY_LEN + BLOCK_ID_LEN + 4 + 8 + OPTIONAL_GENERATION_LEN;
pub const TAIL_IN_MSG_LOCATOR_LEN: usize = 1 + TAIL_PAYLOAD_KEY_LEN + 8 + OPTIONAL_GENERATION_LEN;
const TAIL_PROGRESS_CURSOR_LEN: usize = 1 + ACCOUNT_KEY_LEN;
pub const TAIL_GENERATION_PROGRESS_LEN: usize = 1 + 8 + 16 + 8 + 32 + 32 + TAIL_PROGRESS_CURSOR_LEN + 1 + 8 * 5 + 32;
pub const TAIL_GENERATION_COMMIT_LEN: usize = 1 + 1 + 8 + 16 + 8 + 32 + 8 + 4 + 8 + 32 + 8 * 5;
pub const RETIRED_TRANSACTION_KEY_LEN: usize = 8 + TAIL_PAYLOAD_KEY_LEN;
pub const EMPTY_TAIL_CHUNK_DIGEST: HashBytes = HashBytes::ZERO;
const RETENTION_POLICY_CODEC_VERSION: u8 = 1;

pub type AccountKey = [u8; ACCOUNT_KEY_LEN];
pub type TailPayloadKey = [u8; TAIL_PAYLOAD_KEY_LEN];

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct ControlState {
    pub node_instance_id: InstanceId,
    pub next_partition_id: u64,
    pub smallest_known_lt: u64,
    pub tail_visible_generation: u64,
    pub tail_layout_version: TailLayoutVersion,
    pub removed_through_partition_id: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TailLayoutVersion {
    MonolithicV1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ManifestLifecycle {
    Creating,
    Active,
    Sealing,
    Sealed,
    Retired,
    Deleting,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ManifestBound {
    pub block_id: Option<BlockId>,
    pub mc_seqno: u32,
    pub transaction_lt: u64,
    pub gen_utime: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PartitionManifest {
    pub partition_id: u64,
    pub lifecycle: ManifestLifecycle,
    pub first: ManifestBound,
    pub last: ManifestBound,
    pub transaction_count: u64,
    pub estimated_transaction_lsm_bytes: u64,
    pub estimated_transaction_blob_bytes: u64,
    pub transaction_index_record_count: u64,
    pub estimated_block_metadata_bytes: u64,
    pub last_transition: ManifestTransition,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ManifestTransition {
    pub lifecycle: ManifestLifecycle,
    pub epoch: u64,
    pub at_unix_time: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PartitionCommit {
    pub block_id: BlockId,
    pub digest: HashBytes,
    pub transaction_count: u64,
    pub estimated_transaction_lsm_bytes: u64,
    pub estimated_transaction_blob_bytes: u64,
    pub transaction_index_record_count: u64,
    pub estimated_block_metadata_bytes: u64,
    pub start_lt: u64,
    pub end_lt: u64,
    pub gen_utime: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FilterNamespaceDescriptor {
    pub algorithm_id: u8,
    pub hash_scheme_id: u8,
    pub key_codec_id: u8,
    pub key_len: u8,
    pub false_positive_rate_ppm: u32,
    pub source_key_count: u64,
    pub fingerprint_count: u64,
    pub capacity: u64,
    pub fingerprint_size: u8,
    pub resident_bytes: u64,
    pub payload_len: u64,
    pub payload_digest: HashBytes,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FilterCatalogDescriptor {
    pub partition_id: u64,
    pub generation_id: u128,
    pub manifest_digest: HashBytes,
    pub transactions: FilterNamespaceDescriptor,
    pub inbound_messages: FilterNamespaceDescriptor,
    pub blocks: FilterNamespaceDescriptor,
    pub accounts: FilterNamespaceDescriptor,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GcIntentPhase {
    Evacuating,
    Prepared,
    CutoverCommitted,
    Deleting,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GcIntent {
    pub phase: GcIntentPhase,
    pub operation_id: u128,
    pub source_partition_id: u64,
    pub source_manifest_digest: HashBytes,
    pub target_generation: u64,
    pub previous_visible_generation: u64,
    pub cutoff_utime: u32,
    pub keep_tx_per_account: u64,
    pub retention_policy_digest: HashBytes,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct TailIdentity {
    pub layout_version: TailLayoutVersion,
    pub node_instance_id: InstanceId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TailAccountLocator {
    pub transaction_hash: HashBytes,
    pub born_generation: u64,
    pub dead_generation: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TailHashLocator {
    pub payload_key: TailPayloadKey,
    pub block_id: BlockId,
    pub mc_seqno: u32,
    pub born_generation: u64,
    pub dead_generation: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TailInMsgLocator {
    pub payload_key: TailPayloadKey,
    pub born_generation: u64,
    pub dead_generation: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TailProgressCursor {
    Start,
    Account(AccountKey),
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TailGenerationCounters {
    pub processed_accounts: u64,
    pub promoted_records: u64,
    pub promoted_bytes: u64,
    pub retired_records: u64,
    pub retired_bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TailGenerationProgress {
    pub target_generation: u64,
    pub operation_id: u128,
    pub source_partition_id: u64,
    pub source_manifest_digest: HashBytes,
    pub retention_policy_digest: HashBytes,
    pub cursor: TailProgressCursor,
    pub eof: bool,
    pub counters: TailGenerationCounters,
    pub chunk_digest: HashBytes,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TailGenerationCommit {
    pub layout_version: TailLayoutVersion,
    pub target_generation: u64,
    pub operation_id: u128,
    pub source_partition_id: u64,
    pub source_manifest_digest: HashBytes,
    pub previous_visible_generation: u64,
    pub cutoff_utime: u32,
    pub keep_tx_per_account: u64,
    pub retention_policy_digest: HashBytes,
    pub counters: TailGenerationCounters,
}

pub struct TransactionValueRef<'a> {
    mc_seqno: u32,
    payload: &'a [u8],
}

impl<'a> TransactionValueRef<'a> {
    pub fn mc_seqno(&self) -> u32 {
        self.mc_seqno
    }

    pub fn payload(&self) -> &'a [u8] {
        self.payload
    }
}

pub fn control_state_key() -> &'static [u8] {
    CONTROL_STATE_KEY
}

pub fn active_partition_key() -> &'static [u8] {
    ACTIVE_PARTITION_KEY
}

pub fn visible_frontier_key() -> &'static [u8] {
    VISIBLE_FRONTIER_KEY
}

pub fn manifest_epoch_key() -> &'static [u8] {
    MANIFEST_EPOCH_KEY
}

pub fn gc_intent_key() -> &'static [u8] {
    GC_INTENT_KEY
}

pub fn rpc_layout_marker_key() -> &'static [u8] {
    RPC_LAYOUT_MARKER_KEY
}

pub fn rpc_layout_marker_value() -> &'static [u8] {
    RPC_LAYOUT_MARKER_VALUE
}

pub fn tail_identity_key() -> &'static [u8] {
    TAIL_IDENTITY_KEY
}

pub fn decode_rpc_layout_marker(bytes: &[u8]) -> Result<()> {
    ensure!(bytes == RPC_LAYOUT_MARKER_VALUE, "unsupported RPC layout marker");
    Ok(())
}

pub fn partition_manifest_key(partition_id: u64) -> [u8; PARTITION_ID_LEN] {
    partition_id.to_be_bytes()
}

pub fn encode_short_block_id(block_id: &BlockIdShort) -> [u8; SHORT_BLOCK_ID_LEN] {
    let mut result = [0; SHORT_BLOCK_ID_LEN];
    result[..4].copy_from_slice(&block_id.shard.workchain().to_be_bytes());
    result[4..12].copy_from_slice(&block_id.shard.prefix().to_be_bytes());
    result[12..].copy_from_slice(&block_id.seqno.to_be_bytes());
    result
}

pub fn decode_short_block_id(bytes: &[u8]) -> Result<BlockIdShort> {
    ensure_exact_len("short block id", bytes, SHORT_BLOCK_ID_LEN)?;
    let workchain = i32::from_be_bytes(bytes[..4].try_into().unwrap());
    let prefix = u64::from_be_bytes(bytes[4..12].try_into().unwrap());
    let shard = ShardIdent::new(workchain, prefix)
        .ok_or_else(|| anyhow::anyhow!("invalid short block id shard: workchain={workchain}, prefix={prefix:016x}"))?;
    Ok(BlockIdShort {
        shard,
        seqno: u32::from_be_bytes(bytes[12..].try_into().unwrap()),
    })
}

pub fn partition_commit_key(mc_seqno: u32, block_id: &BlockIdShort) -> [u8; PARTITION_COMMIT_KEY_LEN] {
    let mut result = [0; PARTITION_COMMIT_KEY_LEN];
    result[..4].copy_from_slice(&mc_seqno.to_be_bytes());
    result[4..].copy_from_slice(&encode_short_block_id(block_id));
    result
}

pub fn decode_partition_commit_key(bytes: &[u8]) -> Result<(u32, BlockIdShort)> {
    ensure_exact_len("partition commit key", bytes, PARTITION_COMMIT_KEY_LEN)?;
    Ok((
        u32::from_be_bytes(bytes[..4].try_into().unwrap()),
        decode_short_block_id(&bytes[4..])?,
    ))
}

pub fn encode_control_state(value: ControlState) -> [u8; CONTROL_STATE_LEN] {
    let mut result = [0; CONTROL_STATE_LEN];
    result[0] = LAYOUT_VERSION;
    result[1..17].copy_from_slice(value.node_instance_id.as_ref());
    result[17..25].copy_from_slice(&value.next_partition_id.to_be_bytes());
    result[25..33].copy_from_slice(&value.smallest_known_lt.to_be_bytes());
    result[33..41].copy_from_slice(&value.tail_visible_generation.to_be_bytes());
    result[41] = encode_tail_layout_version(value.tail_layout_version);
    result[42..].copy_from_slice(&value.removed_through_partition_id.to_be_bytes());
    result
}

pub fn decode_control_state(bytes: &[u8]) -> Result<ControlState> {
    ensure_version_and_len("control state", bytes, CONTROL_STATE_LEN)?;
    let result = ControlState {
        node_instance_id: InstanceId::from_slice(&bytes[1..17]),
        next_partition_id: u64::from_be_bytes(bytes[17..25].try_into().unwrap()),
        smallest_known_lt: u64::from_be_bytes(bytes[25..33].try_into().unwrap()),
        tail_visible_generation: u64::from_be_bytes(bytes[33..41].try_into().unwrap()),
        tail_layout_version: decode_tail_layout_version(bytes[41])?,
        removed_through_partition_id: u64::from_be_bytes(bytes[42..].try_into().unwrap()),
    };
    ensure!(result.next_partition_id > 0, "next transaction partition id must be non-zero");
    ensure!(result.removed_through_partition_id < result.next_partition_id, "removed-through partition id must precede the next partition id");
    Ok(result)
}

pub fn encode_active_partition(partition_id: u64) -> [u8; ACTIVE_PARTITION_LEN] {
    let mut result = [0; ACTIVE_PARTITION_LEN];
    result[0] = LAYOUT_VERSION;
    result[1..].copy_from_slice(&partition_id.to_be_bytes());
    result
}

pub fn decode_active_partition(bytes: &[u8]) -> Result<u64> {
    ensure_version_and_len("active partition", bytes, ACTIVE_PARTITION_LEN)?;
    Ok(u64::from_be_bytes(bytes[1..].try_into().unwrap()))
}

pub fn encode_visible_frontier(block_id: &BlockId) -> [u8; VISIBLE_FRONTIER_LEN] {
    let mut result = [0; VISIBLE_FRONTIER_LEN];
    result[0] = LAYOUT_VERSION;
    encode_block_id(block_id, &mut result[1..]);
    result
}

pub fn decode_visible_frontier(bytes: &[u8]) -> Result<BlockId> {
    ensure_version_and_len("visible frontier", bytes, VISIBLE_FRONTIER_LEN)?;
    decode_block_id(&bytes[1..])
}

pub fn encode_manifest_epoch(epoch: u64) -> [u8; MANIFEST_EPOCH_LEN] {
    let mut result = [0; MANIFEST_EPOCH_LEN];
    result[0] = LAYOUT_VERSION;
    result[1..].copy_from_slice(&epoch.to_be_bytes());
    result
}

pub fn decode_manifest_epoch(bytes: &[u8]) -> Result<u64> {
    ensure_version_and_len("manifest epoch", bytes, MANIFEST_EPOCH_LEN)?;
    Ok(u64::from_be_bytes(bytes[1..].try_into().unwrap()))
}

pub fn encode_manifest(value: &PartitionManifest) -> [u8; MANIFEST_RECORD_LEN] {
    let mut result = [0; MANIFEST_RECORD_LEN];
    result[0] = LAYOUT_VERSION;
    result[1] = encode_lifecycle(value.lifecycle);
    result[2..10].copy_from_slice(&value.partition_id.to_be_bytes());
    encode_manifest_bound(value.first, &mut result[10..10 + MANIFEST_BOUND_LEN]);
    let counters_offset = 10 + MANIFEST_BOUND_LEN * 2;
    encode_manifest_bound(value.last, &mut result[10 + MANIFEST_BOUND_LEN..counters_offset]);
    result[counters_offset..counters_offset + 8].copy_from_slice(&value.transaction_count.to_be_bytes());
    result[counters_offset + 8..counters_offset + 16].copy_from_slice(&value.estimated_transaction_lsm_bytes.to_be_bytes());
    result[counters_offset + 16..counters_offset + 24].copy_from_slice(&value.estimated_transaction_blob_bytes.to_be_bytes());
    result[counters_offset + 24..counters_offset + 32].copy_from_slice(&value.transaction_index_record_count.to_be_bytes());
    result[counters_offset + 32..counters_offset + 40].copy_from_slice(&value.estimated_block_metadata_bytes.to_be_bytes());
    result[counters_offset + 40] = encode_lifecycle(value.last_transition.lifecycle);
    result[counters_offset + 41..counters_offset + 49].copy_from_slice(&value.last_transition.epoch.to_be_bytes());
    result[counters_offset + 49..].copy_from_slice(&value.last_transition.at_unix_time.to_be_bytes());
    result
}

pub fn decode_manifest(bytes: &[u8]) -> Result<PartitionManifest> {
    ensure_version_and_len("partition manifest", bytes, MANIFEST_RECORD_LEN)?;
    let counters_offset = 10 + MANIFEST_BOUND_LEN * 2;
    let result = PartitionManifest {
        partition_id: u64::from_be_bytes(bytes[2..10].try_into().unwrap()),
        lifecycle: decode_lifecycle(bytes[1])?,
        first: decode_manifest_bound(&bytes[10..10 + MANIFEST_BOUND_LEN])?,
        last: decode_manifest_bound(&bytes[10 + MANIFEST_BOUND_LEN..counters_offset])?,
        transaction_count: u64::from_be_bytes(bytes[counters_offset..counters_offset + 8].try_into().unwrap()),
        estimated_transaction_lsm_bytes: u64::from_be_bytes(bytes[counters_offset + 8..counters_offset + 16].try_into().unwrap()),
        estimated_transaction_blob_bytes: u64::from_be_bytes(bytes[counters_offset + 16..counters_offset + 24].try_into().unwrap()),
        transaction_index_record_count: u64::from_be_bytes(bytes[counters_offset + 24..counters_offset + 32].try_into().unwrap()),
        estimated_block_metadata_bytes: u64::from_be_bytes(bytes[counters_offset + 32..counters_offset + 40].try_into().unwrap()),
        last_transition: ManifestTransition {
            lifecycle: decode_lifecycle(bytes[counters_offset + 40])?,
            epoch: u64::from_be_bytes(bytes[counters_offset + 41..counters_offset + 49].try_into().unwrap()),
            at_unix_time: u64::from_be_bytes(bytes[counters_offset + 49..].try_into().unwrap()),
        },
    };
    ensure!(result.partition_id > 0, "partition manifest id must be non-zero");
    ensure!(result.last_transition.lifecycle == result.lifecycle, "partition manifest transition lifecycle does not match current lifecycle");
    Ok(result)
}

pub fn encode_partition_commit(value: &PartitionCommit) -> [u8; PARTITION_COMMIT_VALUE_LEN] {
    let mut result = [0; PARTITION_COMMIT_VALUE_LEN];
    result[0] = LAYOUT_VERSION;
    encode_block_id(&value.block_id, &mut result[1..1 + BLOCK_ID_LEN]);
    result[1 + BLOCK_ID_LEN..33 + BLOCK_ID_LEN].copy_from_slice(value.digest.as_ref());
    let fields = [
        value.transaction_count,
        value.estimated_transaction_lsm_bytes,
        value.estimated_transaction_blob_bytes,
        value.transaction_index_record_count,
        value.estimated_block_metadata_bytes,
        value.start_lt,
        value.end_lt,
    ];
    let mut offset = 33 + BLOCK_ID_LEN;
    for field in fields {
        result[offset..offset + 8].copy_from_slice(&field.to_be_bytes());
        offset += 8;
    }
    result[offset..].copy_from_slice(&value.gen_utime.to_be_bytes());
    result
}

pub fn decode_partition_commit(bytes: &[u8]) -> Result<PartitionCommit> {
    ensure_version_and_len("partition commit", bytes, PARTITION_COMMIT_VALUE_LEN)?;
    let mut offset = 33 + BLOCK_ID_LEN;
    let mut next_u64 = || {
        let result = u64::from_be_bytes(bytes[offset..offset + 8].try_into().unwrap());
        offset += 8;
        result
    };
    let result = PartitionCommit {
        block_id: decode_block_id(&bytes[1..1 + BLOCK_ID_LEN])?,
        digest: HashBytes::from_slice(&bytes[1 + BLOCK_ID_LEN..33 + BLOCK_ID_LEN]),
        transaction_count: next_u64(),
        estimated_transaction_lsm_bytes: next_u64(),
        estimated_transaction_blob_bytes: next_u64(),
        transaction_index_record_count: next_u64(),
        estimated_block_metadata_bytes: next_u64(),
        start_lt: next_u64(),
        end_lt: next_u64(),
        gen_utime: u32::from_be_bytes(bytes[offset..].try_into().unwrap()),
    };
    ensure!(result.digest == result.block_id.root_hash, "partition commit digest does not match block root hash");
    Ok(result)
}

pub fn encode_filter_catalog_descriptor(value: &FilterCatalogDescriptor) -> [u8; FILTER_CATALOG_DESCRIPTOR_LEN] {
    let mut result = [0; FILTER_CATALOG_DESCRIPTOR_LEN];
    result[0] = LAYOUT_VERSION;
    result[1..9].copy_from_slice(&value.partition_id.to_be_bytes());
    result[9..25].copy_from_slice(&value.generation_id.to_be_bytes());
    result[25..57].copy_from_slice(value.manifest_digest.as_ref());
    encode_filter_namespace_descriptor(value.transactions, &mut result[57..57 + FILTER_NAMESPACE_DESCRIPTOR_LEN]);
    encode_filter_namespace_descriptor(value.inbound_messages, &mut result[57 + FILTER_NAMESPACE_DESCRIPTOR_LEN..57 + FILTER_NAMESPACE_DESCRIPTOR_LEN * 2]);
    encode_filter_namespace_descriptor(value.blocks, &mut result[57 + FILTER_NAMESPACE_DESCRIPTOR_LEN * 2..57 + FILTER_NAMESPACE_DESCRIPTOR_LEN * 3]);
    encode_filter_namespace_descriptor(value.accounts, &mut result[57 + FILTER_NAMESPACE_DESCRIPTOR_LEN * 3..]);
    result
}

pub fn decode_filter_catalog_descriptor(bytes: &[u8]) -> Result<FilterCatalogDescriptor> {
    ensure_version_and_len("filter catalog descriptor", bytes, FILTER_CATALOG_DESCRIPTOR_LEN)?;
    let transactions = decode_filter_namespace_descriptor(
        &bytes[57..57 + FILTER_NAMESPACE_DESCRIPTOR_LEN],
        32,
    )?;
    let inbound_messages = decode_filter_namespace_descriptor(
        &bytes[57 + FILTER_NAMESPACE_DESCRIPTOR_LEN..57 + FILTER_NAMESPACE_DESCRIPTOR_LEN * 2],
        32,
    )?;
    let blocks = decode_filter_namespace_descriptor(
        &bytes[57 + FILTER_NAMESPACE_DESCRIPTOR_LEN * 2..57 + FILTER_NAMESPACE_DESCRIPTOR_LEN * 3],
        13,
    )?;
    let accounts = decode_filter_namespace_descriptor(
        &bytes[57 + FILTER_NAMESPACE_DESCRIPTOR_LEN * 3..],
        ACCOUNT_KEY_LEN as u8,
    )?;
    let result = FilterCatalogDescriptor {
        partition_id: u64::from_be_bytes(bytes[1..9].try_into().unwrap()),
        generation_id: u128::from_be_bytes(bytes[9..25].try_into().unwrap()),
        manifest_digest: HashBytes::from_slice(&bytes[25..57]),
        transactions,
        inbound_messages,
        blocks,
        accounts,
    };
    ensure!(result.partition_id > 0, "filter catalog partition id must be non-zero");
    ensure!(result.generation_id > 0, "filter catalog generation id must be non-zero");
    Ok(result)
}

pub fn encode_gc_intent(value: &GcIntent) -> Result<[u8; GC_INTENT_LEN]> {
    validate_gc_identity(
        value.operation_id,
        value.source_partition_id,
        value.target_generation,
        value.previous_visible_generation,
    )?;
    let mut result = [0; GC_INTENT_LEN];
    result[0] = LAYOUT_VERSION;
    result[1] = encode_gc_intent_phase(value.phase);
    result[2..18].copy_from_slice(&value.operation_id.to_be_bytes());
    result[18..26].copy_from_slice(&value.source_partition_id.to_be_bytes());
    result[26..58].copy_from_slice(value.source_manifest_digest.as_ref());
    result[58..66].copy_from_slice(&value.target_generation.to_be_bytes());
    result[66..74].copy_from_slice(&value.previous_visible_generation.to_be_bytes());
    result[74..78].copy_from_slice(&value.cutoff_utime.to_be_bytes());
    result[78..86].copy_from_slice(&value.keep_tx_per_account.to_be_bytes());
    result[86..].copy_from_slice(value.retention_policy_digest.as_ref());
    Ok(result)
}

pub fn decode_gc_intent(bytes: &[u8]) -> Result<GcIntent> {
    ensure_version_and_len("GC intent", bytes, GC_INTENT_LEN)?;
    let result = GcIntent {
        phase: decode_gc_intent_phase(bytes[1])?,
        operation_id: u128::from_be_bytes(bytes[2..18].try_into().unwrap()),
        source_partition_id: u64::from_be_bytes(bytes[18..26].try_into().unwrap()),
        source_manifest_digest: HashBytes::from_slice(&bytes[26..58]),
        target_generation: u64::from_be_bytes(bytes[58..66].try_into().unwrap()),
        previous_visible_generation: u64::from_be_bytes(bytes[66..74].try_into().unwrap()),
        cutoff_utime: u32::from_be_bytes(bytes[74..78].try_into().unwrap()),
        keep_tx_per_account: u64::from_be_bytes(bytes[78..86].try_into().unwrap()),
        retention_policy_digest: HashBytes::from_slice(&bytes[86..]),
    };
    validate_gc_identity(
        result.operation_id,
        result.source_partition_id,
        result.target_generation,
        result.previous_visible_generation,
    )?;
    Ok(result)
}

pub fn encode_tail_identity(value: TailIdentity) -> [u8; TAIL_IDENTITY_LEN] {
    let mut result = [0; TAIL_IDENTITY_LEN];
    result[0] = LAYOUT_VERSION;
    result[1] = encode_tail_layout_version(value.layout_version);
    result[2..].copy_from_slice(value.node_instance_id.as_ref());
    result
}

pub fn decode_tail_identity(bytes: &[u8]) -> Result<TailIdentity> {
    ensure_version_and_len("tail identity", bytes, TAIL_IDENTITY_LEN)?;
    Ok(TailIdentity {
        layout_version: decode_tail_layout_version(bytes[1])?,
        node_instance_id: InstanceId::from_slice(&bytes[2..]),
    })
}

pub fn tail_payload_key(account: AccountKey, lt: u64) -> TailPayloadKey {
    let mut result = [0; TAIL_PAYLOAD_KEY_LEN];
    result[..ACCOUNT_KEY_LEN].copy_from_slice(&account);
    result[ACCOUNT_KEY_LEN..].copy_from_slice(&lt.to_be_bytes());
    result
}

pub fn decode_tail_payload_key(bytes: &[u8]) -> Result<(AccountKey, u64)> {
    ensure_exact_len("tail payload key", bytes, TAIL_PAYLOAD_KEY_LEN)?;
    Ok((
        bytes[..ACCOUNT_KEY_LEN].try_into().unwrap(),
        u64::from_be_bytes(bytes[ACCOUNT_KEY_LEN..].try_into().unwrap()),
    ))
}

pub fn encode_tail_account_locator(value: TailAccountLocator) -> Result<[u8; TAIL_ACCOUNT_LOCATOR_LEN]> {
    validate_generations(value.born_generation, value.dead_generation)?;
    let mut result = [0; TAIL_ACCOUNT_LOCATOR_LEN];
    result[0] = LAYOUT_VERSION;
    result[1..33].copy_from_slice(value.transaction_hash.as_ref());
    result[33..41].copy_from_slice(&value.born_generation.to_be_bytes());
    encode_optional_generation(value.dead_generation, &mut result[41..]);
    Ok(result)
}

pub fn decode_tail_account_locator(bytes: &[u8]) -> Result<TailAccountLocator> {
    ensure_version_and_len("tail account locator", bytes, TAIL_ACCOUNT_LOCATOR_LEN)?;
    let result = TailAccountLocator {
        transaction_hash: HashBytes::from_slice(&bytes[1..33]),
        born_generation: u64::from_be_bytes(bytes[33..41].try_into().unwrap()),
        dead_generation: decode_optional_generation(&bytes[41..])?,
    };
    validate_generations(result.born_generation, result.dead_generation)?;
    Ok(result)
}

pub fn encode_tail_hash_locator(value: &TailHashLocator) -> Result<[u8; TAIL_HASH_LOCATOR_LEN]> {
    validate_tail_hash_locator_identity(value)?;
    validate_generations(value.born_generation, value.dead_generation)?;
    let mut result = [0; TAIL_HASH_LOCATOR_LEN];
    result[0] = LAYOUT_VERSION;
    result[1..42].copy_from_slice(&value.payload_key);
    encode_block_id(&value.block_id, &mut result[42..122]);
    result[122..126].copy_from_slice(&value.mc_seqno.to_be_bytes());
    result[126..134].copy_from_slice(&value.born_generation.to_be_bytes());
    encode_optional_generation(value.dead_generation, &mut result[134..]);
    Ok(result)
}

pub fn decode_tail_hash_locator(bytes: &[u8]) -> Result<TailHashLocator> {
    ensure_version_and_len("tail hash locator", bytes, TAIL_HASH_LOCATOR_LEN)?;
    let result = TailHashLocator {
        payload_key: bytes[1..42].try_into().unwrap(),
        block_id: decode_block_id(&bytes[42..122])?,
        mc_seqno: u32::from_be_bytes(bytes[122..126].try_into().unwrap()),
        born_generation: u64::from_be_bytes(bytes[126..134].try_into().unwrap()),
        dead_generation: decode_optional_generation(&bytes[134..])?,
    };
    validate_tail_hash_locator_identity(&result)?;
    validate_generations(result.born_generation, result.dead_generation)?;
    Ok(result)
}

pub fn encode_tail_in_msg_locator(value: TailInMsgLocator) -> Result<[u8; TAIL_IN_MSG_LOCATOR_LEN]> {
    validate_generations(value.born_generation, value.dead_generation)?;
    let mut result = [0; TAIL_IN_MSG_LOCATOR_LEN];
    result[0] = LAYOUT_VERSION;
    result[1..42].copy_from_slice(&value.payload_key);
    result[42..50].copy_from_slice(&value.born_generation.to_be_bytes());
    encode_optional_generation(value.dead_generation, &mut result[50..]);
    Ok(result)
}

pub fn decode_tail_in_msg_locator(bytes: &[u8]) -> Result<TailInMsgLocator> {
    ensure_version_and_len("tail inbound-message locator", bytes, TAIL_IN_MSG_LOCATOR_LEN)?;
    let result = TailInMsgLocator {
        payload_key: bytes[1..42].try_into().unwrap(),
        born_generation: u64::from_be_bytes(bytes[42..50].try_into().unwrap()),
        dead_generation: decode_optional_generation(&bytes[50..])?,
    };
    validate_generations(result.born_generation, result.dead_generation)?;
    Ok(result)
}

pub fn tail_generation_progress_key(target_generation: u64) -> Result<[u8; 8]> {
    ensure_generation("tail progress target", target_generation)?;
    Ok(target_generation.to_be_bytes())
}

pub fn decode_tail_generation_progress_key(bytes: &[u8]) -> Result<u64> {
    decode_generation_key("tail progress", bytes)
}

pub fn tail_generation_commit_key(target_generation: u64) -> Result<[u8; 8]> {
    ensure_generation("tail commit target", target_generation)?;
    Ok(target_generation.to_be_bytes())
}

pub fn decode_tail_generation_commit_key(bytes: &[u8]) -> Result<u64> {
    decode_generation_key("tail commit", bytes)
}

pub fn encode_tail_generation_progress(value: &TailGenerationProgress) -> Result<[u8; TAIL_GENERATION_PROGRESS_LEN]> {
    validate_tail_generation_progress(value)?;
    let mut result = [0; TAIL_GENERATION_PROGRESS_LEN];
    result[0] = LAYOUT_VERSION;
    result[1..9].copy_from_slice(&value.target_generation.to_be_bytes());
    result[9..25].copy_from_slice(&value.operation_id.to_be_bytes());
    result[25..33].copy_from_slice(&value.source_partition_id.to_be_bytes());
    result[33..65].copy_from_slice(value.source_manifest_digest.as_ref());
    result[65..97].copy_from_slice(value.retention_policy_digest.as_ref());
    encode_tail_progress_cursor(value.cursor, &mut result[97..131]);
    result[131] = value.eof as u8;
    encode_tail_generation_counters(value.counters, &mut result[132..172]);
    result[172..204].copy_from_slice(value.chunk_digest.as_ref());
    Ok(result)
}

pub fn decode_tail_generation_progress(bytes: &[u8]) -> Result<TailGenerationProgress> {
    ensure_version_and_len("tail generation progress", bytes, TAIL_GENERATION_PROGRESS_LEN)?;
    let eof = match bytes[131] {
        0 => false,
        1 => true,
        value => bail!("invalid tail generation progress EOF discriminant: {value}"),
    };
    let result = TailGenerationProgress {
        target_generation: u64::from_be_bytes(bytes[1..9].try_into().unwrap()),
        operation_id: u128::from_be_bytes(bytes[9..25].try_into().unwrap()),
        source_partition_id: u64::from_be_bytes(bytes[25..33].try_into().unwrap()),
        source_manifest_digest: HashBytes::from_slice(&bytes[33..65]),
        retention_policy_digest: HashBytes::from_slice(&bytes[65..97]),
        cursor: decode_tail_progress_cursor(&bytes[97..131])?,
        eof,
        counters: decode_tail_generation_counters(&bytes[132..172])?,
        chunk_digest: HashBytes::from_slice(&bytes[172..204]),
    };
    validate_tail_generation_progress(&result)?;
    Ok(result)
}

pub fn encode_tail_generation_commit(value: &TailGenerationCommit) -> Result<[u8; TAIL_GENERATION_COMMIT_LEN]> {
    validate_gc_identity(
        value.operation_id,
        value.source_partition_id,
        value.target_generation,
        value.previous_visible_generation,
    )?;
    let mut result = [0; TAIL_GENERATION_COMMIT_LEN];
    result[0] = LAYOUT_VERSION;
    result[1] = encode_tail_layout_version(value.layout_version);
    result[2..10].copy_from_slice(&value.target_generation.to_be_bytes());
    result[10..26].copy_from_slice(&value.operation_id.to_be_bytes());
    result[26..34].copy_from_slice(&value.source_partition_id.to_be_bytes());
    result[34..66].copy_from_slice(value.source_manifest_digest.as_ref());
    result[66..74].copy_from_slice(&value.previous_visible_generation.to_be_bytes());
    result[74..78].copy_from_slice(&value.cutoff_utime.to_be_bytes());
    result[78..86].copy_from_slice(&value.keep_tx_per_account.to_be_bytes());
    result[86..118].copy_from_slice(value.retention_policy_digest.as_ref());
    encode_tail_generation_counters(value.counters, &mut result[118..]);
    Ok(result)
}

pub fn decode_tail_generation_commit(bytes: &[u8]) -> Result<TailGenerationCommit> {
    ensure_version_and_len("tail generation commit", bytes, TAIL_GENERATION_COMMIT_LEN)?;
    let result = TailGenerationCommit {
        layout_version: decode_tail_layout_version(bytes[1])?,
        target_generation: u64::from_be_bytes(bytes[2..10].try_into().unwrap()),
        operation_id: u128::from_be_bytes(bytes[10..26].try_into().unwrap()),
        source_partition_id: u64::from_be_bytes(bytes[26..34].try_into().unwrap()),
        source_manifest_digest: HashBytes::from_slice(&bytes[34..66]),
        previous_visible_generation: u64::from_be_bytes(bytes[66..74].try_into().unwrap()),
        cutoff_utime: u32::from_be_bytes(bytes[74..78].try_into().unwrap()),
        keep_tx_per_account: u64::from_be_bytes(bytes[78..86].try_into().unwrap()),
        retention_policy_digest: HashBytes::from_slice(&bytes[86..118]),
        counters: decode_tail_generation_counters(&bytes[118..])?,
    };
    validate_gc_identity(
        result.operation_id,
        result.source_partition_id,
        result.target_generation,
        result.previous_visible_generation,
    )?;
    Ok(result)
}

pub fn retired_transaction_key(dead_generation: u64, payload_key: TailPayloadKey) -> Result<[u8; RETIRED_TRANSACTION_KEY_LEN]> {
    ensure_generation("retired transaction", dead_generation)?;
    let mut result = [0; RETIRED_TRANSACTION_KEY_LEN];
    result[..8].copy_from_slice(&dead_generation.to_be_bytes());
    result[8..].copy_from_slice(&payload_key);
    Ok(result)
}

pub fn decode_retired_transaction_key(bytes: &[u8]) -> Result<(u64, TailPayloadKey)> {
    ensure_exact_len("retired transaction key", bytes, RETIRED_TRANSACTION_KEY_LEN)?;
    let generation = u64::from_be_bytes(bytes[..8].try_into().unwrap());
    ensure_generation("retired transaction", generation)?;
    Ok((generation, bytes[8..].try_into().unwrap()))
}

pub fn retention_policy_digest(ttl_seconds: u64, keep_tx_per_account: u64) -> HashBytes {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"tycho-rpc-retention-policy");
    hasher.update(&[RETENTION_POLICY_CODEC_VERSION]);
    hasher.update(&ttl_seconds.to_be_bytes());
    hasher.update(&keep_tx_per_account.to_be_bytes());
    HashBytes::from_slice(hasher.finalize().as_bytes())
}

pub fn format_filter_generation_directory(partition_id: u64, generation_id: u128) -> String {
    format!("{:016}/{generation_id:032x}", partition_id)
}

pub fn format_filter_temporary_generation_directory(partition_id: u64, generation_id: u128) -> String {
    format!("{:016}/.tmp-{generation_id:032x}", partition_id)
}

pub fn parse_filter_generation_directory(path: &str) -> Result<(u64, u128, bool)> {
    let (partition, generation) = path
        .split_once('/')
        .context("invalid filter generation directory")?;
    ensure!(partition.len() == 16 && partition.bytes().all(|byte| byte.is_ascii_digit()), "invalid filter generation partition id");
    let temporary = generation.starts_with(".tmp-");
    let generation = generation.strip_prefix(".tmp-").unwrap_or(generation);
    ensure!(generation.len() == 32 && generation.bytes().all(|byte| byte.is_ascii_digit() || byte.is_ascii_lowercase()), "invalid filter generation id");
    Ok((
        partition.parse().context("invalid filter generation partition id")?,
        u128::from_str_radix(generation, 16).context("invalid filter generation id")?,
        temporary,
    ))
}

fn encode_filter_namespace_descriptor(value: FilterNamespaceDescriptor, target: &mut [u8]) {
    target[0] = value.algorithm_id;
    target[1] = value.hash_scheme_id;
    target[2] = value.key_codec_id;
    target[3] = value.key_len;
    target[4..8].copy_from_slice(&value.false_positive_rate_ppm.to_be_bytes());
    target[8..16].copy_from_slice(&value.source_key_count.to_be_bytes());
    target[16..24].copy_from_slice(&value.fingerprint_count.to_be_bytes());
    target[24..32].copy_from_slice(&value.capacity.to_be_bytes());
    target[32] = value.fingerprint_size;
    target[33..41].copy_from_slice(&value.resident_bytes.to_be_bytes());
    target[41..49].copy_from_slice(&value.payload_len.to_be_bytes());
    target[49..].copy_from_slice(value.payload_digest.as_ref());
}

fn decode_filter_namespace_descriptor(bytes: &[u8], expected_key_len: u8) -> Result<FilterNamespaceDescriptor> {
    ensure_exact_len("filter namespace descriptor", bytes, FILTER_NAMESPACE_DESCRIPTOR_LEN)?;
    ensure!(bytes[0] == 1, "unsupported filter algorithm id: {}", bytes[0]);
    ensure!(bytes[1] == 1, "unsupported filter hash scheme id: {}", bytes[1]);
    ensure!(bytes[2] == 1, "unsupported filter key codec id: {}", bytes[2]);
    ensure!(bytes[3] == expected_key_len, "invalid filter key length: expected {expected_key_len}, got {}", bytes[3]);
    let false_positive_rate_ppm = u32::from_be_bytes(bytes[4..8].try_into().unwrap());
    ensure!((1..=500_000).contains(&false_positive_rate_ppm), "invalid filter false-positive rate: {false_positive_rate_ppm}");
    let source_key_count = u64::from_be_bytes(bytes[8..16].try_into().unwrap());
    let fingerprint_count = u64::from_be_bytes(bytes[16..24].try_into().unwrap());
    let capacity = u64::from_be_bytes(bytes[24..32].try_into().unwrap());
    ensure!(capacity > 0, "invalid filter capacity");
    ensure!(fingerprint_count <= source_key_count, "filter fingerprint count exceeds source key count");
    ensure!(fingerprint_count <= capacity, "filter fingerprint count exceeds capacity");
    let fingerprint_size = bytes[32];
    ensure!(fingerprint_size > 0, "invalid filter fingerprint size");
    Ok(FilterNamespaceDescriptor {
        algorithm_id: bytes[0],
        hash_scheme_id: bytes[1],
        key_codec_id: bytes[2],
        key_len: bytes[3],
        false_positive_rate_ppm,
        source_key_count,
        fingerprint_count,
        capacity,
        fingerprint_size,
        resident_bytes: u64::from_be_bytes(bytes[33..41].try_into().unwrap()),
        payload_len: u64::from_be_bytes(bytes[41..49].try_into().unwrap()),
        payload_digest: HashBytes::from_slice(&bytes[49..]),
    })
}

pub fn encode_transaction_value(mc_seqno: u32, payload: &[u8]) -> Result<Vec<u8>> {
    validate_transaction_payload(payload)?;
    let mut result = Vec::with_capacity(TRANSACTION_VALUE_PREFIX_LEN + payload.len());
    result.extend_from_slice(&mc_seqno.to_be_bytes());
    result.extend_from_slice(payload);
    Ok(result)
}

pub fn decode_transaction_value(bytes: &[u8]) -> Result<TransactionValueRef<'_>> {
    ensure!(bytes.len() > TRANSACTION_VALUE_PREFIX_LEN, "transaction value is missing payload after related mc_seqno");
    let payload = &bytes[TRANSACTION_VALUE_PREFIX_LEN..];
    validate_transaction_payload(payload)?;
    Ok(TransactionValueRef {
        mc_seqno: u32::from_be_bytes(bytes[..TRANSACTION_VALUE_PREFIX_LEN].try_into().unwrap()),
        payload,
    })
}

fn encode_manifest_bound(value: ManifestBound, target: &mut [u8]) {
    target[0] = value.block_id.is_some() as u8;
    if let Some(block_id) = value.block_id {
        encode_block_id(&block_id, &mut target[1..1 + BLOCK_ID_LEN]);
    }
    target[1 + BLOCK_ID_LEN..5 + BLOCK_ID_LEN].copy_from_slice(&value.mc_seqno.to_be_bytes());
    target[5 + BLOCK_ID_LEN..13 + BLOCK_ID_LEN].copy_from_slice(&value.transaction_lt.to_be_bytes());
    target[13 + BLOCK_ID_LEN..].copy_from_slice(&value.gen_utime.to_be_bytes());
}

fn decode_manifest_bound(bytes: &[u8]) -> Result<ManifestBound> {
    ensure_exact_len("partition manifest bound", bytes, MANIFEST_BOUND_LEN)?;
    let block_id = match bytes[0] {
        0 => {
            ensure!(bytes[1..1 + BLOCK_ID_LEN].iter().all(|byte| *byte == 0), "non-canonical absent partition manifest block id");
            None
        }
        1 => Some(decode_block_id(&bytes[1..1 + BLOCK_ID_LEN])?),
        value => bail!("invalid partition manifest block presence discriminant: {value}"),
    };
    Ok(ManifestBound {
        block_id,
        mc_seqno: u32::from_be_bytes(bytes[1 + BLOCK_ID_LEN..5 + BLOCK_ID_LEN].try_into().unwrap()),
        transaction_lt: u64::from_be_bytes(bytes[5 + BLOCK_ID_LEN..13 + BLOCK_ID_LEN].try_into().unwrap()),
        gen_utime: u32::from_be_bytes(bytes[13 + BLOCK_ID_LEN..].try_into().unwrap()),
    })
}

fn encode_block_id(value: &BlockId, target: &mut [u8]) {
    target[..4].copy_from_slice(&value.shard.workchain().to_be_bytes());
    target[4..12].copy_from_slice(&value.shard.prefix().to_be_bytes());
    target[12..16].copy_from_slice(&value.seqno.to_be_bytes());
    target[16..48].copy_from_slice(value.root_hash.as_ref());
    target[48..].copy_from_slice(value.file_hash.as_ref());
}

fn decode_block_id(bytes: &[u8]) -> Result<BlockId> {
    ensure_exact_len("block id", bytes, BLOCK_ID_LEN)?;
    let workchain = i32::from_be_bytes(bytes[..4].try_into().unwrap());
    let prefix = u64::from_be_bytes(bytes[4..12].try_into().unwrap());
    let shard = ShardIdent::new(workchain, prefix)
        .ok_or_else(|| anyhow::anyhow!("invalid block id shard: workchain={workchain}, prefix={prefix:016x}"))?;
    Ok(BlockId {
        shard,
        seqno: u32::from_be_bytes(bytes[12..16].try_into().unwrap()),
        root_hash: HashBytes::from_slice(&bytes[16..48]),
        file_hash: HashBytes::from_slice(&bytes[48..]),
    })
}

fn encode_lifecycle(value: ManifestLifecycle) -> u8 {
    match value {
        ManifestLifecycle::Creating => 0,
        ManifestLifecycle::Active => 1,
        ManifestLifecycle::Sealing => 2,
        ManifestLifecycle::Sealed => 3,
        ManifestLifecycle::Retired => 4,
        ManifestLifecycle::Deleting => 5,
    }
}

fn decode_lifecycle(value: u8) -> Result<ManifestLifecycle> {
    match value {
        0 => Ok(ManifestLifecycle::Creating),
        1 => Ok(ManifestLifecycle::Active),
        2 => Ok(ManifestLifecycle::Sealing),
        3 => Ok(ManifestLifecycle::Sealed),
        4 => Ok(ManifestLifecycle::Retired),
        5 => Ok(ManifestLifecycle::Deleting),
        value => bail!("invalid partition manifest lifecycle discriminant: {value}"),
    }
}

fn encode_tail_layout_version(value: TailLayoutVersion) -> u8 {
    match value {
        TailLayoutVersion::MonolithicV1 => 1,
    }
}

fn decode_tail_layout_version(value: u8) -> Result<TailLayoutVersion> {
    match value {
        1 => Ok(TailLayoutVersion::MonolithicV1),
        value => bail!("unsupported tail layout version: {value}"),
    }
}

fn encode_gc_intent_phase(value: GcIntentPhase) -> u8 {
    match value {
        GcIntentPhase::Evacuating => 0,
        GcIntentPhase::Prepared => 1,
        GcIntentPhase::CutoverCommitted => 2,
        GcIntentPhase::Deleting => 3,
    }
}

fn decode_gc_intent_phase(value: u8) -> Result<GcIntentPhase> {
    match value {
        0 => Ok(GcIntentPhase::Evacuating),
        1 => Ok(GcIntentPhase::Prepared),
        2 => Ok(GcIntentPhase::CutoverCommitted),
        3 => Ok(GcIntentPhase::Deleting),
        value => bail!("invalid GC intent phase discriminant: {value}"),
    }
}

fn validate_gc_identity(
    operation_id: u128,
    source_partition_id: u64,
    target_generation: u64,
    previous_visible_generation: u64,
) -> Result<()> {
    ensure!(operation_id > 0, "GC operation id must be non-zero");
    ensure!(source_partition_id > 0, "GC source partition id must be non-zero");
    ensure_generation("GC target", target_generation)?;
    let expected_target = previous_visible_generation
        .checked_add(1)
        .context("GC target generation overflow")?;
    ensure!(target_generation == expected_target, "GC target generation must immediately follow the previous visible generation");
    Ok(())
}

fn validate_generations(born_generation: u64, dead_generation: Option<u64>) -> Result<()> {
    ensure_generation("tail born", born_generation)?;
    if let Some(dead_generation) = dead_generation {
        ensure_generation("tail dead", dead_generation)?;
        ensure!(dead_generation > born_generation, "tail dead generation must be greater than born generation");
    }
    Ok(())
}

fn encode_optional_generation(value: Option<u64>, target: &mut [u8]) {
    debug_assert_eq!(target.len(), OPTIONAL_GENERATION_LEN);
    if let Some(value) = value {
        target[0] = 1;
        target[1..].copy_from_slice(&value.to_be_bytes());
    }
}

fn decode_optional_generation(bytes: &[u8]) -> Result<Option<u64>> {
    ensure_exact_len("optional tail generation", bytes, OPTIONAL_GENERATION_LEN)?;
    match bytes[0] {
        0 => {
            ensure!(bytes[1..].iter().all(|byte| *byte == 0), "non-canonical absent tail generation");
            Ok(None)
        }
        1 => Ok(Some(u64::from_be_bytes(bytes[1..].try_into().unwrap()))),
        value => bail!("invalid optional tail generation discriminant: {value}"),
    }
}

fn validate_tail_hash_locator_identity(value: &TailHashLocator) -> Result<()> {
    let (account, _) = decode_tail_payload_key(&value.payload_key)?;
    let account_workchain = account[0] as i8 as i32;
    ensure!(value.block_id.shard.workchain() == account_workchain, "tail hash locator block workchain does not match payload account");
    let account_address = StdAddr::new(account[0] as i8, HashBytes::from_slice(&account[1..]));
    ensure!(value.block_id.shard.contains_address(&account_address), "tail hash locator block shard does not contain payload account");
    Ok(())
}

fn encode_tail_progress_cursor(value: TailProgressCursor, target: &mut [u8]) {
    debug_assert_eq!(target.len(), TAIL_PROGRESS_CURSOR_LEN);
    if let TailProgressCursor::Account(account) = value {
        target[0] = 1;
        target[1..].copy_from_slice(&account);
    }
}

fn decode_tail_progress_cursor(bytes: &[u8]) -> Result<TailProgressCursor> {
    ensure_exact_len("tail progress cursor", bytes, TAIL_PROGRESS_CURSOR_LEN)?;
    match bytes[0] {
        0 => {
            ensure!(bytes[1..].iter().all(|byte| *byte == 0), "non-canonical tail Start cursor padding");
            Ok(TailProgressCursor::Start)
        }
        1 => Ok(TailProgressCursor::Account(bytes[1..].try_into().unwrap())),
        value => bail!("invalid tail progress cursor discriminant: {value}"),
    }
}

fn encode_tail_generation_counters(value: TailGenerationCounters, target: &mut [u8]) {
    debug_assert_eq!(target.len(), 8 * 5);
    for (index, field) in [
        value.processed_accounts,
        value.promoted_records,
        value.promoted_bytes,
        value.retired_records,
        value.retired_bytes,
    ]
    .into_iter()
    .enumerate()
    {
        target[index * 8..index * 8 + 8].copy_from_slice(&field.to_be_bytes());
    }
}

fn decode_tail_generation_counters(bytes: &[u8]) -> Result<TailGenerationCounters> {
    ensure_exact_len("tail generation counters", bytes, 8 * 5)?;
    let field = |index: usize| u64::from_be_bytes(bytes[index * 8..index * 8 + 8].try_into().unwrap());
    Ok(TailGenerationCounters {
        processed_accounts: field(0),
        promoted_records: field(1),
        promoted_bytes: field(2),
        retired_records: field(3),
        retired_bytes: field(4),
    })
}

fn validate_tail_generation_progress(value: &TailGenerationProgress) -> Result<()> {
    ensure_generation("tail progress target", value.target_generation)?;
    ensure!(value.operation_id > 0, "tail progress operation id must be non-zero");
    ensure!(value.source_partition_id > 0, "tail progress source partition id must be non-zero");
    match value.cursor {
        TailProgressCursor::Start => {
            ensure!(value.eof, "tail Start progress must be an EOF progress record");
            ensure!(value.counters == TailGenerationCounters::default(), "tail Start progress must have zero counters");
            ensure!(value.chunk_digest == EMPTY_TAIL_CHUNK_DIGEST, "tail Start progress must have the empty chunk digest");
        }
        TailProgressCursor::Account(_) => {
            ensure!(value.counters.processed_accounts > 0, "tail account progress must have a positive processed-account counter");
        }
    }
    Ok(())
}

fn decode_generation_key(format: &str, bytes: &[u8]) -> Result<u64> {
    ensure_exact_len(format, bytes, 8)?;
    let generation = u64::from_be_bytes(bytes.try_into().unwrap());
    ensure_generation(format, generation)?;
    Ok(generation)
}

fn ensure_generation(format: &str, generation: u64) -> Result<()> {
    ensure!(generation > 0, "{format} generation must be non-zero");
    Ok(())
}

fn ensure_version_and_len(format: &str, bytes: &[u8], expected_len: usize) -> Result<()> {
    ensure_exact_len(format, bytes, expected_len)?;
    ensure!(bytes[0] == LAYOUT_VERSION, "unsupported {format} version: {}", bytes[0]);
    Ok(())
}

fn ensure_exact_len(format: &str, bytes: &[u8], expected_len: usize) -> Result<()> {
    ensure!(bytes.len() == expected_len, "invalid {format} length: expected {expected_len}, got {}", bytes.len());
    Ok(())
}

fn validate_transaction_payload(payload: &[u8]) -> Result<()> {
    ensure!(!payload.is_empty(), "transaction value payload is empty");
    let mask = TransactionMask::from_bits(payload[0])
        .ok_or_else(|| anyhow::anyhow!("invalid transaction value mask: {}", payload[0]))?;
    let boc_start = if mask.has_msg_hash() { 65 } else { 33 };
    ensure!(payload.len() > boc_start, "transaction value payload is too short for its mask: expected more than {boc_start}, got {}", payload.len());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex_bytes(value: &str) -> Vec<u8> {
        let digits = value
            .bytes()
            .filter(|byte| !byte.is_ascii_whitespace())
            .collect::<Vec<_>>();
        assert_eq!(digits.len() % 2, 0);
        digits
            .chunks_exact(2)
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect()
    }

    fn block_id(seqno: u32) -> BlockId {
        BlockId {
            shard: ShardIdent::BASECHAIN,
            seqno,
            root_hash: HashBytes([1; 32]),
            file_hash: HashBytes([2; 32]),
        }
    }

    fn bound(block_id: Option<BlockId>) -> ManifestBound {
        ManifestBound {
            block_id,
            mc_seqno: 17,
            transaction_lt: 18,
            gen_utime: 19,
        }
    }

    fn account_key() -> AccountKey {
        let mut result = [7; ACCOUNT_KEY_LEN];
        result[0] = 0;
        result
    }

    fn generation_counters() -> TailGenerationCounters {
        TailGenerationCounters {
            processed_accounts: 1,
            promoted_records: 2,
            promoted_bytes: 3,
            retired_records: 4,
            retired_bytes: 5,
        }
    }

    fn gc_intent() -> GcIntent {
        GcIntent {
            phase: GcIntentPhase::Evacuating,
            operation_id: 5,
            source_partition_id: 42,
            source_manifest_digest: HashBytes([3; 32]),
            target_generation: 7,
            previous_visible_generation: 6,
            cutoff_utime: 8,
            keep_tx_per_account: 9,
            retention_policy_digest: HashBytes([4; 32]),
        }
    }

    #[test]
    fn v3_control_formats_are_exact_and_reject_v2_or_malformed_values() {
        let state = ControlState {
            node_instance_id: InstanceId([3; 16]),
            next_partition_id: 42,
            smallest_known_lt: 9,
            tail_visible_generation: 7,
            tail_layout_version: TailLayoutVersion::MonolithicV1,
            removed_through_partition_id: 4,
        };
        let encoded = encode_control_state(state);
        assert_eq!(
            encoded.as_slice(),
            hex_bytes(
                "
                0203030303030303030303030303030303000000000000002a00000000000000
                090000000000000007010000000000000004
                "
            )
        );
        assert!(decode_control_state(&encoded).unwrap() == state);
        let max_next = ControlState { next_partition_id: u64::MAX, ..state };
        assert!(decode_control_state(&encode_control_state(max_next)).unwrap() == max_next);
        assert_eq!(rpc_layout_marker_value(), b"tycho-rpc-layout-v3");
        assert!(decode_rpc_layout_marker(rpc_layout_marker_value()).is_ok());
        assert!(decode_rpc_layout_marker(b"tycho-rpc-layout-v2").is_err());
        assert_eq!(decode_active_partition(&encode_active_partition(u64::MAX)).unwrap(), u64::MAX);
        assert_eq!(decode_visible_frontier(&encode_visible_frontier(&block_id(u32::MAX))).unwrap(), block_id(u32::MAX));
        assert_eq!(decode_manifest_epoch(&encode_manifest_epoch(u64::MAX)).unwrap(), u64::MAX);
        for value in [
            &[][..],
            &encode_control_state(state)[..CONTROL_STATE_LEN - 1],
            &[1; CONTROL_STATE_LEN][..],
        ] {
            assert!(decode_control_state(value).is_err());
        }
        let mut invalid = encoded;
        invalid[17..25].fill(0);
        assert!(decode_control_state(&invalid).is_err());
        let mut invalid = encoded;
        invalid[41] = 2;
        assert!(decode_control_state(&invalid).is_err());
        let mut invalid = encoded;
        invalid[42..].copy_from_slice(&42u64.to_be_bytes());
        assert!(decode_control_state(&invalid).is_err());
        assert!(decode_active_partition(&[LAYOUT_VERSION; ACTIVE_PARTITION_LEN - 1]).is_err());
        assert!(decode_active_partition(&[1; ACTIVE_PARTITION_LEN]).is_err());
        assert!(decode_visible_frontier(&[1; VISIBLE_FRONTIER_LEN]).is_err());
        let mut invalid_block = encode_visible_frontier(&block_id(1));
        invalid_block[5..13].fill(0);
        assert!(decode_visible_frontier(&invalid_block).is_err());
        assert!(decode_manifest_epoch(&[LAYOUT_VERSION; MANIFEST_EPOCH_LEN - 1]).is_err());
        assert!(decode_manifest_epoch(&[1; MANIFEST_EPOCH_LEN]).is_err());
    }

    #[test]
    fn v3_manifest_round_trip_and_rejects_v2_or_invalid_identity() {
        let value = PartitionManifest {
            partition_id: 42,
            lifecycle: ManifestLifecycle::Sealing,
            first: bound(Some(block_id(1))),
            last: bound(None),
            transaction_count: 20,
            estimated_transaction_lsm_bytes: 21,
            estimated_transaction_blob_bytes: 22,
            transaction_index_record_count: 23,
            estimated_block_metadata_bytes: 24,
            last_transition: ManifestTransition {
                lifecycle: ManifestLifecycle::Sealing,
                epoch: 25,
                at_unix_time: 26,
            },
        };
        let encoded = encode_manifest(&value);
        assert_eq!(
            encoded.as_slice(),
            hex_bytes(
                "
                0202000000000000002a01000000008000000000000000000000010101010101
                0101010101010101010101010101010101010101010101010101010202020202
                0202020202020202020202020202020202020202020202020202020000001100
                0000000000001200000013000000000000000000000000000000000000000000
                0000000000000000000000000000000000000000000000000000000000000000
                0000000000000000000000000000000000000000000000000000000000000011
                0000000000000012000000130000000000000014000000000000001500000000
                0000001600000000000000170000000000000018020000000000000019000000
                000000001a
                "
            )
        );
        assert_eq!(decode_manifest(&encoded).unwrap(), value);
        assert!(decode_manifest(&encoded[..encoded.len() - 1]).is_err());
        assert!(decode_manifest(&encoded[..encoded.len() - 8]).is_err());
        let mut invalid_state = encoded;
        invalid_state[1] = 6;
        assert!(decode_manifest(&invalid_state).is_err());
        let mut invalid_version = encoded;
        invalid_version[0] = 1;
        assert!(decode_manifest(&invalid_version).is_err());
        let mut invalid_bound = encoded;
        invalid_bound[10] = 2;
        assert!(decode_manifest(&invalid_bound).is_err());
        let mut invalid_absent_padding = encoded;
        invalid_absent_padding[10 + MANIFEST_BOUND_LEN + 1] = 1;
        assert!(decode_manifest(&invalid_absent_padding).is_err());
        let mut invalid_transition = encoded;
        invalid_transition[10 + MANIFEST_BOUND_LEN * 2 + 40] = encode_lifecycle(ManifestLifecycle::Active);
        assert!(decode_manifest(&invalid_transition).is_err());
        let mut invalid_id = encoded;
        invalid_id[2..10].fill(0);
        assert!(decode_manifest(&invalid_id).is_err());
        for lifecycle in [ManifestLifecycle::Retired, ManifestLifecycle::Deleting] {
            let changed = PartitionManifest {
                lifecycle,
                last_transition: ManifestTransition { lifecycle, ..value.last_transition },
                ..value
            };
            assert_eq!(decode_manifest(&encode_manifest(&changed)).unwrap(), changed);
        }
    }

    #[test]
    fn v3_partition_commit_formats_round_trip_and_reject_malformed_values() {
        let short = block_id(u32::MAX).as_short_id();
        assert_eq!(decode_short_block_id(&encode_short_block_id(&short)).unwrap(), short);
        let key = partition_commit_key(u32::MAX, &short);
        assert_eq!(decode_partition_commit_key(&key).unwrap(), (u32::MAX, short));
        assert_eq!(partition_manifest_key(u64::MAX), u64::MAX.to_be_bytes());
        let commit = PartitionCommit {
            block_id: block_id(2),
            digest: HashBytes([1; 32]),
            transaction_count: 5,
            estimated_transaction_lsm_bytes: 6,
            estimated_transaction_blob_bytes: 7,
            transaction_index_record_count: 8,
            estimated_block_metadata_bytes: 9,
            start_lt: 10,
            end_lt: 11,
            gen_utime: u32::MAX,
        };
        let encoded = encode_partition_commit(&commit);
        assert_eq!(
            encoded.as_slice(),
            hex_bytes(
                "
                0200000000800000000000000000000002010101010101010101010101010101
                0101010101010101010101010101010101020202020202020202020202020202
                0202020202020202020202020202020202010101010101010101010101010101
                0101010101010101010101010101010101000000000000000500000000000000
                0600000000000000070000000000000008000000000000000900000000000000
                0a000000000000000bffffffff
                "
            )
        );
        assert_eq!(decode_partition_commit(&encoded).unwrap(), commit);
        assert!(decode_short_block_id(&[0; SHORT_BLOCK_ID_LEN - 1]).is_err());
        assert!(decode_short_block_id(&[0; SHORT_BLOCK_ID_LEN]).is_err());
        assert!(decode_partition_commit_key(&key[..key.len() - 1]).is_err());
        assert!(decode_partition_commit(&[LAYOUT_VERSION; PARTITION_COMMIT_VALUE_LEN - 1]).is_err());
        let mut invalid_version = encoded;
        invalid_version[0] = 1;
        assert!(decode_partition_commit(&invalid_version).is_err());
        let mut invalid_digest = encoded;
        invalid_digest[1 + BLOCK_ID_LEN] ^= 1;
        assert!(decode_partition_commit(&invalid_digest).is_err());
    }

    #[test]
    fn four_namespace_filter_catalog_and_generation_directories_are_validated() {
        let namespace = FilterNamespaceDescriptor {
            algorithm_id: 1,
            hash_scheme_id: 1,
            key_codec_id: 1,
            key_len: 32,
            false_positive_rate_ppm: 1_000,
            source_key_count: 1,
            fingerprint_count: 1,
            capacity: 1,
            fingerprint_size: 8,
            resident_bytes: 1,
            payload_len: 1,
            payload_digest: HashBytes([3; 32]),
        };
        let descriptor = FilterCatalogDescriptor {
            partition_id: 42,
            generation_id: 7,
            manifest_digest: HashBytes([4; 32]),
            transactions: namespace,
            inbound_messages: namespace,
            blocks: FilterNamespaceDescriptor { key_len: 13, ..namespace },
            accounts: FilterNamespaceDescriptor { key_len: ACCOUNT_KEY_LEN as u8, ..namespace },
        };
        let encoded = encode_filter_catalog_descriptor(&descriptor);
        assert_eq!(
            encoded.as_slice(),
            hex_bytes(
                "
                02000000000000002a0000000000000000000000000000000704040404040404
                0404040404040404040404040404040404040404040404040401010120000003
                e800000000000000010000000000000001000000000000000108000000000000
                0001000000000000000103030303030303030303030303030303030303030303
                0303030303030303030301010120000003e80000000000000001000000000000
                0001000000000000000108000000000000000100000000000000010303030303
                0303030303030303030303030303030303030303030303030303030101010d00
                0003e80000000000000001000000000000000100000000000000010800000000
                0000000100000000000000010303030303030303030303030303030303030303
                03030303030303030303030301010121000003e8000000000000000100000000
                0000000100000000000000010800000000000000010000000000000001030303
                0303030303030303030303030303030303030303030303030303030303
                "
            )
        );
        assert_eq!(decode_filter_catalog_descriptor(&encoded).unwrap(), descriptor);
        assert!(decode_filter_catalog_descriptor(&encoded[..encoded.len() - 1]).is_err());
        assert!(decode_filter_catalog_descriptor(&encoded[..encoded.len() - FILTER_NAMESPACE_DESCRIPTOR_LEN]).is_err());
        let mut trailing = encoded.to_vec();
        trailing.push(0);
        assert!(decode_filter_catalog_descriptor(&trailing).is_err());
        let mut invalid = encoded;
        invalid[0] = 1;
        assert!(decode_filter_catalog_descriptor(&invalid).is_err());
        let mut invalid = encoded;
        invalid[57] = 2;
        assert!(decode_filter_catalog_descriptor(&invalid).is_err());
        let mut invalid = encoded;
        invalid[60] = 31;
        assert!(decode_filter_catalog_descriptor(&invalid).is_err());
        let mut invalid = encoded;
        invalid[61..65].fill(0);
        assert!(decode_filter_catalog_descriptor(&invalid).is_err());
        let mut invalid = encoded;
        invalid[73..81].copy_from_slice(&2u64.to_be_bytes());
        assert!(decode_filter_catalog_descriptor(&invalid).is_err());
        let mut invalid = encoded;
        invalid[57 + FILTER_NAMESPACE_DESCRIPTOR_LEN * 3 + 3] = 32;
        assert!(decode_filter_catalog_descriptor(&invalid).is_err());
        let mut invalid = encoded;
        invalid[1..9].fill(0);
        assert!(decode_filter_catalog_descriptor(&invalid).is_err());
        let mut invalid = encoded;
        invalid[9..25].fill(0);
        assert!(decode_filter_catalog_descriptor(&invalid).is_err());
        let final_name = format_filter_generation_directory(42, 7);
        assert_eq!(parse_filter_generation_directory(&final_name).unwrap(), (42, 7, false));
        let temporary_name = format_filter_temporary_generation_directory(42, 7);
        assert_eq!(parse_filter_generation_directory(&temporary_name).unwrap(), (42, 7, true));
        assert!(parse_filter_generation_directory("unknown").is_err());
        assert!(parse_filter_generation_directory("0000000000000042/not-a-generation").is_err());
        assert!(parse_filter_generation_directory("0000000000000042/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA").is_err());
    }

    #[test]
    fn gc_intent_and_policy_digest_are_fixed_and_strict() {
        let value = gc_intent();
        let encoded = encode_gc_intent(&value).unwrap();
        assert_eq!(
            encoded.as_slice(),
            hex_bytes(
                "
                020000000000000000000000000000000005000000000000002a030303030303
                0303030303030303030303030303030303030303030303030303000000000000
                0007000000000000000600000008000000000000000904040404040404040404
                04040404040404040404040404040404040404040404
                "
            )
        );
        assert_eq!(decode_gc_intent(&encoded).unwrap(), value);
        assert!(decode_gc_intent(&encoded[..encoded.len() - 1]).is_err());
        let mut invalid = encoded;
        invalid[0] = 1;
        assert!(decode_gc_intent(&invalid).is_err());
        let mut invalid = encoded;
        invalid[1] = 4;
        assert!(decode_gc_intent(&invalid).is_err());
        let mut invalid = encoded;
        invalid[2..18].fill(0);
        assert!(decode_gc_intent(&invalid).is_err());
        let mut invalid = encoded;
        invalid[18..26].fill(0);
        assert!(decode_gc_intent(&invalid).is_err());
        let mut invalid = encoded;
        invalid[58..66].copy_from_slice(&6u64.to_be_bytes());
        assert!(decode_gc_intent(&invalid).is_err());
        let mut invalid_value = value;
        invalid_value.previous_visible_generation = u64::MAX;
        invalid_value.target_generation = u64::MAX;
        assert!(encode_gc_intent(&invalid_value).is_err());
        let digest = retention_policy_digest(604_800, 10);
        assert_eq!(
            &digest.0[..],
            hex_bytes("256b9c35921295255bce646530703ddded204a01b55dcb542ad6ba611286c82f").as_slice()
        );
        assert_ne!(digest, retention_policy_digest(604_801, 10));
        assert_ne!(digest, retention_policy_digest(604_800, 11));
    }

    #[test]
    fn tail_identity_and_logical_keys_are_fixed_and_strict() {
        let identity = TailIdentity {
            layout_version: TailLayoutVersion::MonolithicV1,
            node_instance_id: InstanceId([9; 16]),
        };
        let encoded = encode_tail_identity(identity);
        assert_eq!(encoded.as_slice(), hex_bytes("020109090909090909090909090909090909"));
        assert!(decode_tail_identity(&encoded).unwrap() == identity);
        assert!(decode_tail_identity(&encoded[..encoded.len() - 1]).is_err());
        let mut invalid = encoded;
        invalid[0] = 1;
        assert!(decode_tail_identity(&invalid).is_err());
        let mut invalid = encoded;
        invalid[1] = 2;
        assert!(decode_tail_identity(&invalid).is_err());

        let account = account_key();
        let payload_key = tail_payload_key(account, 11);
        assert_eq!(&payload_key[..ACCOUNT_KEY_LEN], &account);
        assert_eq!(&payload_key[ACCOUNT_KEY_LEN..], &11u64.to_be_bytes());
        assert_eq!(decode_tail_payload_key(&payload_key).unwrap(), (account, 11));
        assert!(decode_tail_payload_key(&payload_key[..payload_key.len() - 1]).is_err());
        assert_eq!(tail_generation_progress_key(7).unwrap(), 7u64.to_be_bytes());
        assert_eq!(decode_tail_generation_progress_key(&7u64.to_be_bytes()).unwrap(), 7);
        assert_eq!(tail_generation_commit_key(7).unwrap(), 7u64.to_be_bytes());
        assert_eq!(decode_tail_generation_commit_key(&7u64.to_be_bytes()).unwrap(), 7);
        assert!(tail_generation_progress_key(0).is_err());
        assert!(tail_generation_commit_key(0).is_err());
        assert!(decode_tail_generation_progress_key(&0u64.to_be_bytes()).is_err());
        assert!(decode_tail_generation_commit_key(&[0; 7]).is_err());
    }

    #[test]
    fn tail_locator_codecs_validate_versions_generations_and_identity() {
        let account = account_key();
        let payload_key = tail_payload_key(account, 11);
        let account_locator = TailAccountLocator {
            transaction_hash: HashBytes([5; 32]),
            born_generation: 7,
            dead_generation: Some(9),
        };
        let encoded_account = encode_tail_account_locator(account_locator).unwrap();
        assert_eq!(
            encoded_account.as_slice(),
            hex_bytes(
                "
                0205050505050505050505050505050505050505050505050505050505050505
                050000000000000007010000000000000009
                "
            )
        );
        assert_eq!(decode_tail_account_locator(&encoded_account).unwrap(), account_locator);
        assert!(decode_tail_account_locator(&encoded_account[..encoded_account.len() - 1]).is_err());
        let mut invalid = encoded_account;
        invalid[0] = 1;
        assert!(decode_tail_account_locator(&invalid).is_err());
        let mut invalid = encoded_account;
        invalid[33..41].fill(0);
        assert!(decode_tail_account_locator(&invalid).is_err());
        let mut invalid = encoded_account;
        invalid[41] = 2;
        assert!(decode_tail_account_locator(&invalid).is_err());
        let mut invalid = encoded_account;
        invalid[42..].copy_from_slice(&7u64.to_be_bytes());
        assert!(decode_tail_account_locator(&invalid).is_err());
        let absent = encode_tail_account_locator(TailAccountLocator { dead_generation: None, ..account_locator }).unwrap();
        let mut non_canonical_absent = absent;
        non_canonical_absent[49] = 1;
        assert!(decode_tail_account_locator(&non_canonical_absent).is_err());

        let hash_locator = TailHashLocator {
            payload_key,
            block_id: block_id(12),
            mc_seqno: 13,
            born_generation: 7,
            dead_generation: None,
        };
        let encoded_hash = encode_tail_hash_locator(&hash_locator).unwrap();
        assert_eq!(
            encoded_hash.as_slice(),
            hex_bytes(
                "
                0200070707070707070707070707070707070707070707070707070707070707
                0707000000000000000b0000000080000000000000000000000c010101010101
                0101010101010101010101010101010101010101010101010101020202020202
                02020202020202020202020202020202020202020202020202020000000d0000
                000000000007000000000000000000
                "
            )
        );
        assert_eq!(decode_tail_hash_locator(&encoded_hash).unwrap(), hash_locator);
        let mut invalid_identity = encoded_hash;
        invalid_identity[1] = 1;
        assert!(decode_tail_hash_locator(&invalid_identity).is_err());
        let mut invalid_block = encoded_hash;
        invalid_block[46..54].fill(0);
        assert!(decode_tail_hash_locator(&invalid_block).is_err());
        let mut wrong_identity = hash_locator;
        wrong_identity.payload_key[0] = 1;
        assert!(encode_tail_hash_locator(&wrong_identity).is_err());

        let in_msg_locator = TailInMsgLocator {
            payload_key,
            born_generation: 7,
            dead_generation: Some(9),
        };
        let encoded_in_msg = encode_tail_in_msg_locator(in_msg_locator).unwrap();
        assert_eq!(
            encoded_in_msg.as_slice(),
            hex_bytes(
                "
                0200070707070707070707070707070707070707070707070707070707070707
                0707000000000000000b0000000000000007010000000000000009
                "
            )
        );
        assert_eq!(decode_tail_in_msg_locator(&encoded_in_msg).unwrap(), in_msg_locator);
        assert!(decode_tail_in_msg_locator(&encoded_in_msg[..encoded_in_msg.len() - 1]).is_err());
        assert!(encode_tail_in_msg_locator(TailInMsgLocator { born_generation: 0, ..in_msg_locator }).is_err());
    }

    #[test]
    fn tail_progress_commit_and_retirement_codecs_are_fixed_and_strict() {
        let account = account_key();
        let progress = TailGenerationProgress {
            target_generation: 7,
            operation_id: 5,
            source_partition_id: 42,
            source_manifest_digest: HashBytes([3; 32]),
            retention_policy_digest: HashBytes([4; 32]),
            cursor: TailProgressCursor::Account(account),
            eof: false,
            counters: generation_counters(),
            chunk_digest: HashBytes([5; 32]),
        };
        let encoded_progress = encode_tail_generation_progress(&progress).unwrap();
        assert_eq!(
            encoded_progress.as_slice(),
            hex_bytes(
                "
                0200000000000000070000000000000000000000000000000500000000000000
                2a03030303030303030303030303030303030303030303030303030303030303
                0304040404040404040404040404040404040404040404040404040404040404
                0401000707070707070707070707070707070707070707070707070707070707
                0707070000000000000000010000000000000002000000000000000300000000
                0000000400000000000000050505050505050505050505050505050505050505
                050505050505050505050505
                "
            )
        );
        assert_eq!(decode_tail_generation_progress(&encoded_progress).unwrap(), progress);
        assert!(decode_tail_generation_progress(&encoded_progress[..encoded_progress.len() - 1]).is_err());
        let mut invalid = encoded_progress;
        invalid[0] = 1;
        assert!(decode_tail_generation_progress(&invalid).is_err());
        let mut invalid = encoded_progress;
        invalid[97] = 2;
        assert!(decode_tail_generation_progress(&invalid).is_err());
        let mut invalid = encoded_progress;
        invalid[97] = 0;
        assert!(decode_tail_generation_progress(&invalid).is_err());
        let mut invalid = encoded_progress;
        invalid[131] = 2;
        assert!(decode_tail_generation_progress(&invalid).is_err());
        let mut invalid = encoded_progress;
        invalid[132..140].fill(0);
        assert!(decode_tail_generation_progress(&invalid).is_err());
        let empty_eof = TailGenerationProgress {
            cursor: TailProgressCursor::Start,
            eof: true,
            counters: TailGenerationCounters::default(),
            chunk_digest: EMPTY_TAIL_CHUNK_DIGEST,
            ..progress
        };
        assert_eq!(decode_tail_generation_progress(&encode_tail_generation_progress(&empty_eof).unwrap()).unwrap(), empty_eof);
        assert!(encode_tail_generation_progress(&TailGenerationProgress { eof: false, ..empty_eof }).is_err());
        assert!(encode_tail_generation_progress(&TailGenerationProgress { chunk_digest: HashBytes([1; 32]), ..empty_eof }).is_err());

        let commit = TailGenerationCommit {
            layout_version: TailLayoutVersion::MonolithicV1,
            target_generation: 7,
            operation_id: 5,
            source_partition_id: 42,
            source_manifest_digest: HashBytes([3; 32]),
            previous_visible_generation: 6,
            cutoff_utime: 8,
            keep_tx_per_account: 9,
            retention_policy_digest: HashBytes([4; 32]),
            counters: generation_counters(),
        };
        let encoded_commit = encode_tail_generation_commit(&commit).unwrap();
        assert_eq!(
            encoded_commit.as_slice(),
            hex_bytes(
                "
                0201000000000000000700000000000000000000000000000005000000000000
                002a030303030303030303030303030303030303030303030303030303030303
                0303000000000000000600000008000000000000000904040404040404040404
                0404040404040404040404040404040404040404040400000000000000010000
                000000000002000000000000000300000000000000040000000000000005
                "
            )
        );
        assert_eq!(decode_tail_generation_commit(&encoded_commit).unwrap(), commit);
        assert!(decode_tail_generation_commit(&encoded_commit[..encoded_commit.len() - 1]).is_err());
        let mut invalid = encoded_commit;
        invalid[1] = 2;
        assert!(decode_tail_generation_commit(&invalid).is_err());
        let mut invalid = encoded_commit;
        invalid[2..10].copy_from_slice(&6u64.to_be_bytes());
        assert!(decode_tail_generation_commit(&invalid).is_err());
        let mut invalid = encoded_commit;
        invalid[10..26].fill(0);
        assert!(decode_tail_generation_commit(&invalid).is_err());

        let payload_key = tail_payload_key(account, 11);
        let retired_key = retired_transaction_key(9, payload_key).unwrap();
        assert_eq!(
            retired_key.as_slice(),
            hex_bytes(
                "
                0000000000000009000707070707070707070707070707070707070707070707
                070707070707070707000000000000000b
                "
            )
        );
        assert_eq!(decode_retired_transaction_key(&retired_key).unwrap(), (9, payload_key));
        assert!(retired_transaction_key(0, payload_key).is_err());
        assert!(decode_retired_transaction_key(&retired_key[..retired_key.len() - 1]).is_err());
        let mut invalid = retired_key;
        invalid[..8].fill(0);
        assert!(decode_retired_transaction_key(&invalid).is_err());
    }

    #[test]
    fn transaction_value_round_trip_and_rejects_malformed_payload() {
        let payload = [TransactionMask::HAS_MSG_HASH.bits(), 1, 2, 3]
            .into_iter()
            .chain([0; 32])
            .chain([4; 32])
            .chain([5])
            .collect::<Vec<_>>();
        let encoded = encode_transaction_value(u32::MAX, &payload).unwrap();
        let decoded = decode_transaction_value(&encoded).unwrap();
        assert_eq!(decoded.mc_seqno(), u32::MAX);
        assert_eq!(decoded.payload(), payload);
        assert!(decode_transaction_value(&[]).is_err());
        assert!(decode_transaction_value(&[0; TRANSACTION_VALUE_PREFIX_LEN]).is_err());
        assert!(encode_transaction_value(1, &[2; 34]).is_err());
        assert!(encode_transaction_value(1, &[0; 33]).is_err());
    }
}
