use anyhow::{Context, Result, bail, ensure};
use tycho_storage::kv::InstanceId;
use tycho_types::models::{BlockId, BlockIdShort, ShardIdent};
use tycho_types::prelude::HashBytes;

use super::TransactionMask;

pub const LAYOUT_VERSION: u8 = 1;
pub const CONTROL_STATE_KEY: &[u8] = b"control_state";
pub const ACTIVE_PARTITION_KEY: &[u8] = b"active_partition";
pub const VISIBLE_FRONTIER_KEY: &[u8] = b"visible_frontier";
pub const MANIFEST_EPOCH_KEY: &[u8] = b"manifest_epoch";
pub const RPC_LAYOUT_MARKER_KEY: &[u8] = b"rpc_layout_marker";
pub const RPC_LAYOUT_MARKER_VALUE: &[u8] = b"tycho-rpc-layout-v2";
pub const PARTITION_ID_LEN: usize = 8;
pub const SHORT_BLOCK_ID_LEN: usize = 16;
pub const BLOCK_ID_LEN: usize = 80;
pub const TRANSACTION_VALUE_PREFIX_LEN: usize = 4;
const CONTROL_STATE_LEN: usize = 1 + 16 + 8 + 8;
const ACTIVE_PARTITION_LEN: usize = 1 + PARTITION_ID_LEN;
const VISIBLE_FRONTIER_LEN: usize = 1 + BLOCK_ID_LEN;
const MANIFEST_EPOCH_LEN: usize = 1 + 8;
const MANIFEST_BOUND_LEN: usize = 1 + BLOCK_ID_LEN + 4 + 8 + 4;
const MANIFEST_RECORD_LEN: usize = 1 + 1 + PARTITION_ID_LEN + MANIFEST_BOUND_LEN * 2 + 8 * 4 + 1 + 8 + 8;
const PARTITION_COMMIT_VALUE_LEN: usize = 1 + BLOCK_ID_LEN + 32 + 8 * 6 + 4;
const FILTER_NAMESPACE_DESCRIPTOR_LEN: usize = 1 + 1 + 1 + 1 + 4 + 8 + 8 + 8 + 1 + 8 + 8 + 32;
const FILTER_CATALOG_DESCRIPTOR_LEN: usize = 1 + PARTITION_ID_LEN + 16 + 32 + FILTER_NAMESPACE_DESCRIPTOR_LEN * 3;

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct ControlState {
    pub node_instance_id: InstanceId,
    pub next_partition_id: u64,
    pub min_transaction_lt: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ManifestLifecycle {
    Creating,
    Active,
    Sealing,
    Sealed,
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
    pub estimated_lsm_bytes: u64,
    pub estimated_blob_bytes: u64,
    pub index_record_count: u64,
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
    pub estimated_lsm_bytes: u64,
    pub estimated_blob_bytes: u64,
    pub index_record_count: u64,
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

pub fn rpc_layout_marker_key() -> &'static [u8] {
    RPC_LAYOUT_MARKER_KEY
}

pub fn rpc_layout_marker_value() -> &'static [u8] {
    RPC_LAYOUT_MARKER_VALUE
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

pub fn partition_commit_key(mc_seqno: u32, block_id: &BlockIdShort) -> [u8; 4 + SHORT_BLOCK_ID_LEN] {
    let mut result = [0; 4 + SHORT_BLOCK_ID_LEN];
    result[..4].copy_from_slice(&mc_seqno.to_be_bytes());
    result[4..].copy_from_slice(&encode_short_block_id(block_id));
    result
}

pub fn decode_partition_commit_key(bytes: &[u8]) -> Result<(u32, BlockIdShort)> {
    ensure_exact_len("partition commit key", bytes, 4 + SHORT_BLOCK_ID_LEN)?;
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
    result[25..].copy_from_slice(&value.min_transaction_lt.to_be_bytes());
    result
}

pub fn decode_control_state(bytes: &[u8]) -> Result<ControlState> {
    ensure_version_and_len("control state", bytes, CONTROL_STATE_LEN)?;
    Ok(ControlState {
        node_instance_id: InstanceId::from_slice(&bytes[1..17]),
        next_partition_id: u64::from_be_bytes(bytes[17..25].try_into().unwrap()),
        min_transaction_lt: u64::from_be_bytes(bytes[25..].try_into().unwrap()),
    })
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
    result[counters_offset + 8..counters_offset + 16].copy_from_slice(&value.estimated_lsm_bytes.to_be_bytes());
    result[counters_offset + 16..counters_offset + 24].copy_from_slice(&value.estimated_blob_bytes.to_be_bytes());
    result[counters_offset + 24..counters_offset + 32].copy_from_slice(&value.index_record_count.to_be_bytes());
    result[counters_offset + 32] = encode_lifecycle(value.last_transition.lifecycle);
    result[counters_offset + 33..counters_offset + 41].copy_from_slice(&value.last_transition.epoch.to_be_bytes());
    result[counters_offset + 41..].copy_from_slice(&value.last_transition.at_unix_time.to_be_bytes());
    result
}

pub fn decode_manifest(bytes: &[u8]) -> Result<PartitionManifest> {
    ensure_version_and_len("partition manifest", bytes, MANIFEST_RECORD_LEN)?;
    let counters_offset = 10 + MANIFEST_BOUND_LEN * 2;
    Ok(PartitionManifest {
        partition_id: u64::from_be_bytes(bytes[2..10].try_into().unwrap()),
        lifecycle: decode_lifecycle(bytes[1])?,
        first: decode_manifest_bound(&bytes[10..10 + MANIFEST_BOUND_LEN])?,
        last: decode_manifest_bound(&bytes[10 + MANIFEST_BOUND_LEN..counters_offset])?,
        transaction_count: u64::from_be_bytes(bytes[counters_offset..counters_offset + 8].try_into().unwrap()),
        estimated_lsm_bytes: u64::from_be_bytes(bytes[counters_offset + 8..counters_offset + 16].try_into().unwrap()),
        estimated_blob_bytes: u64::from_be_bytes(bytes[counters_offset + 16..counters_offset + 24].try_into().unwrap()),
        index_record_count: u64::from_be_bytes(bytes[counters_offset + 24..counters_offset + 32].try_into().unwrap()),
        last_transition: ManifestTransition {
            lifecycle: decode_lifecycle(bytes[counters_offset + 32])?,
            epoch: u64::from_be_bytes(bytes[counters_offset + 33..counters_offset + 41].try_into().unwrap()),
            at_unix_time: u64::from_be_bytes(bytes[counters_offset + 41..].try_into().unwrap()),
        },
    })
}

pub fn encode_partition_commit(value: &PartitionCommit) -> [u8; PARTITION_COMMIT_VALUE_LEN] {
    let mut result = [0; PARTITION_COMMIT_VALUE_LEN];
    result[0] = LAYOUT_VERSION;
    encode_block_id(&value.block_id, &mut result[1..1 + BLOCK_ID_LEN]);
    result[1 + BLOCK_ID_LEN..33 + BLOCK_ID_LEN].copy_from_slice(value.digest.as_ref());
    let fields = [
        value.transaction_count,
        value.estimated_lsm_bytes,
        value.estimated_blob_bytes,
        value.index_record_count,
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
    Ok(PartitionCommit {
        block_id: decode_block_id(&bytes[1..1 + BLOCK_ID_LEN])?,
        digest: HashBytes::from_slice(&bytes[1 + BLOCK_ID_LEN..33 + BLOCK_ID_LEN]),
        transaction_count: next_u64(),
        estimated_lsm_bytes: next_u64(),
        estimated_blob_bytes: next_u64(),
        index_record_count: next_u64(),
        start_lt: next_u64(),
        end_lt: next_u64(),
        gen_utime: u32::from_be_bytes(bytes[offset..].try_into().unwrap()),
    })
}

pub fn encode_filter_catalog_descriptor(value: &FilterCatalogDescriptor) -> [u8; FILTER_CATALOG_DESCRIPTOR_LEN] {
    let mut result = [0; FILTER_CATALOG_DESCRIPTOR_LEN];
    result[0] = LAYOUT_VERSION;
    result[1..9].copy_from_slice(&value.partition_id.to_be_bytes());
    result[9..25].copy_from_slice(&value.generation_id.to_be_bytes());
    result[25..57].copy_from_slice(value.manifest_digest.as_ref());
    encode_filter_namespace_descriptor(value.transactions, &mut result[57..57 + FILTER_NAMESPACE_DESCRIPTOR_LEN]);
    encode_filter_namespace_descriptor(value.inbound_messages, &mut result[57 + FILTER_NAMESPACE_DESCRIPTOR_LEN..57 + FILTER_NAMESPACE_DESCRIPTOR_LEN * 2]);
    encode_filter_namespace_descriptor(value.blocks, &mut result[57 + FILTER_NAMESPACE_DESCRIPTOR_LEN * 2..]);
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
        &bytes[57 + FILTER_NAMESPACE_DESCRIPTOR_LEN * 2..],
        13,
    )?;
    Ok(FilterCatalogDescriptor {
        partition_id: u64::from_be_bytes(bytes[1..9].try_into().unwrap()),
        generation_id: u128::from_be_bytes(bytes[9..25].try_into().unwrap()),
        manifest_digest: HashBytes::from_slice(&bytes[25..57]),
        transactions,
        inbound_messages,
        blocks,
    })
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
        0 => None,
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
    }
}

fn decode_lifecycle(value: u8) -> Result<ManifestLifecycle> {
    match value {
        0 => Ok(ManifestLifecycle::Creating),
        1 => Ok(ManifestLifecycle::Active),
        2 => Ok(ManifestLifecycle::Sealing),
        3 => Ok(ManifestLifecycle::Sealed),
        value => bail!("invalid partition manifest lifecycle discriminant: {value}"),
    }
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

    #[test]
    fn control_formats_round_trip_and_reject_malformed_values() {
        let state = ControlState {
            node_instance_id: InstanceId([3; 16]),
            next_partition_id: u64::MAX,
            min_transaction_lt: 9,
        };
        assert!(decode_control_state(&encode_control_state(state)).unwrap() == state);
        assert_eq!(decode_active_partition(&encode_active_partition(u64::MAX)).unwrap(), u64::MAX);
        assert_eq!(decode_visible_frontier(&encode_visible_frontier(&block_id(u32::MAX))).unwrap(), block_id(u32::MAX));
        assert_eq!(decode_manifest_epoch(&encode_manifest_epoch(u64::MAX)).unwrap(), u64::MAX);
        for value in [
            &[][..],
            &encode_control_state(state)[..CONTROL_STATE_LEN - 1],
            &[2; CONTROL_STATE_LEN][..],
        ] {
            assert!(decode_control_state(value).is_err());
        }
        assert!(decode_active_partition(&[LAYOUT_VERSION; ACTIVE_PARTITION_LEN - 1]).is_err());
        assert!(decode_active_partition(&[2; ACTIVE_PARTITION_LEN]).is_err());
        assert!(decode_visible_frontier(&[2; VISIBLE_FRONTIER_LEN]).is_err());
        let mut invalid_block = encode_visible_frontier(&block_id(1));
        invalid_block[5..13].fill(0);
        assert!(decode_visible_frontier(&invalid_block).is_err());
        assert!(decode_manifest_epoch(&[LAYOUT_VERSION; MANIFEST_EPOCH_LEN - 1]).is_err());
        assert!(decode_manifest_epoch(&[2; MANIFEST_EPOCH_LEN]).is_err());
    }

    #[test]
    fn manifest_round_trip_and_rejects_invalid_state_and_bounds() {
        let value = PartitionManifest {
            partition_id: 42,
            lifecycle: ManifestLifecycle::Sealing,
            first: bound(Some(block_id(1))),
            last: bound(None),
            transaction_count: 20,
            estimated_lsm_bytes: 21,
            estimated_blob_bytes: 22,
            index_record_count: 23,
            last_transition: ManifestTransition {
                lifecycle: ManifestLifecycle::Active,
                epoch: 24,
                at_unix_time: 25,
            },
        };
        let encoded = encode_manifest(&value);
        assert_eq!(decode_manifest(&encoded).unwrap(), value);
        assert!(decode_manifest(&encoded[..encoded.len() - 1]).is_err());
        let mut invalid_state = encoded;
        invalid_state[1] = 4;
        assert!(decode_manifest(&invalid_state).is_err());
        let mut invalid_version = encoded;
        invalid_version[0] = 2;
        assert!(decode_manifest(&invalid_version).is_err());
        let mut invalid_bound = encoded;
        invalid_bound[10] = 2;
        assert!(decode_manifest(&invalid_bound).is_err());
    }

    #[test]
    fn partition_commit_formats_round_trip_and_reject_malformed_values() {
        let short = block_id(u32::MAX).as_short_id();
        assert_eq!(decode_short_block_id(&encode_short_block_id(&short)).unwrap(), short);
        let key = partition_commit_key(u32::MAX, &short);
        assert_eq!(decode_partition_commit_key(&key).unwrap(), (u32::MAX, short));
        assert_eq!(partition_manifest_key(u64::MAX), u64::MAX.to_be_bytes());
        let commit = PartitionCommit {
            block_id: block_id(2),
            digest: HashBytes([4; 32]),
            transaction_count: 5,
            estimated_lsm_bytes: 6,
            estimated_blob_bytes: 7,
            index_record_count: 8,
            start_lt: 9,
            end_lt: 10,
            gen_utime: u32::MAX,
        };
        assert_eq!(decode_partition_commit(&encode_partition_commit(&commit)).unwrap(), commit);
        assert!(decode_short_block_id(&[0; SHORT_BLOCK_ID_LEN - 1]).is_err());
        assert!(decode_short_block_id(&[0; SHORT_BLOCK_ID_LEN]).is_err());
        assert!(decode_partition_commit_key(&key[..key.len() - 1]).is_err());
        assert!(decode_partition_commit(&[LAYOUT_VERSION; PARTITION_COMMIT_VALUE_LEN - 1]).is_err());
        assert!(decode_partition_commit(&[2; PARTITION_COMMIT_VALUE_LEN]).is_err());
    }

    #[test]
    fn filter_catalog_descriptor_and_generation_directories_are_validated() {
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
        };
        let encoded = encode_filter_catalog_descriptor(&descriptor);
        assert_eq!(decode_filter_catalog_descriptor(&encoded).unwrap(), descriptor);
        assert!(decode_filter_catalog_descriptor(&encoded[..encoded.len() - 1]).is_err());
        let mut trailing = encoded.to_vec();
        trailing.push(0);
        assert!(decode_filter_catalog_descriptor(&trailing).is_err());
        let mut invalid = encoded;
        invalid[0] = 2;
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
        let final_name = format_filter_generation_directory(42, 7);
        assert_eq!(parse_filter_generation_directory(&final_name).unwrap(), (42, 7, false));
        let temporary_name = format_filter_temporary_generation_directory(42, 7);
        assert_eq!(parse_filter_generation_directory(&temporary_name).unwrap(), (42, 7, true));
        assert!(parse_filter_generation_directory("unknown").is_err());
        assert!(parse_filter_generation_directory("0000000000000042/not-a-generation").is_err());
        assert!(parse_filter_generation_directory("0000000000000042/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA").is_err());
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
