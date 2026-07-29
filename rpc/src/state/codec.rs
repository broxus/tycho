use anyhow::{Result, bail, ensure};
use tycho_storage::kv::InstanceId;
use tycho_types::models::{BlockId, BlockIdShort, ShardIdent};
use tycho_types::prelude::HashBytes;

use super::TransactionMask;

pub const LAYOUT_VERSION: u8 = 1;
pub const CONTROL_STATE_KEY: &[u8] = b"control_state";
pub const ACTIVE_PARTITION_KEY: &[u8] = b"active_partition";
pub const VISIBLE_FRONTIER_KEY: &[u8] = b"visible_frontier";
pub const MANIFEST_EPOCH_KEY: &[u8] = b"manifest_epoch";
pub const ROUTER_COMMIT_KEY: &[u8] = b"router_commit";
pub const PARTITION_ID_LEN: usize = 8;
pub const SHORT_BLOCK_ID_LEN: usize = 16;
pub const BLOCK_ID_LEN: usize = 80;
pub const TRANSACTION_VALUE_PREFIX_LEN: usize = 4;
const CONTROL_STATE_LEN: usize = 1 + 16 + 8 + 8;
const ACTIVE_PARTITION_LEN: usize = 1 + PARTITION_ID_LEN;
const VISIBLE_FRONTIER_LEN: usize = 1 + BLOCK_ID_LEN;
const MANIFEST_EPOCH_LEN: usize = 1 + 8;
const ROUTER_COMMIT_LEN: usize = 1 + BLOCK_ID_LEN;
const ROUTER_LOCATION_LEN: usize = 1 + PARTITION_ID_LEN + 4;
const MANIFEST_BOUND_LEN: usize = 1 + BLOCK_ID_LEN + 4 + 8 + 4;
const MANIFEST_RECORD_LEN: usize = 1 + 1 + PARTITION_ID_LEN + MANIFEST_BOUND_LEN * 2 + 8 * 4 + 1 + 8 + 8;
const PARTITION_COMMIT_VALUE_LEN: usize = 1 + BLOCK_ID_LEN + 32 + 8 * 6 + 4;

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
pub struct RouterLocation {
    pub partition_id: u64,
    pub mc_seqno: u32,
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

pub fn router_commit_key() -> &'static [u8] {
    ROUTER_COMMIT_KEY
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

pub fn encode_router_commit(block_id: &BlockId) -> [u8; ROUTER_COMMIT_LEN] {
    let mut result = [0; ROUTER_COMMIT_LEN];
    result[0] = LAYOUT_VERSION;
    encode_block_id(block_id, &mut result[1..]);
    result
}

pub fn decode_router_commit(bytes: &[u8]) -> Result<BlockId> {
    ensure_version_and_len("router commit", bytes, ROUTER_COMMIT_LEN)?;
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

pub fn encode_router_location(value: RouterLocation) -> [u8; ROUTER_LOCATION_LEN] {
    let mut result = [0; ROUTER_LOCATION_LEN];
    result[0] = LAYOUT_VERSION;
    result[1..9].copy_from_slice(&value.partition_id.to_be_bytes());
    result[9..].copy_from_slice(&value.mc_seqno.to_be_bytes());
    result
}

pub fn decode_router_location(bytes: &[u8]) -> Result<RouterLocation> {
    ensure_version_and_len("router location", bytes, ROUTER_LOCATION_LEN)?;
    Ok(RouterLocation {
        partition_id: u64::from_be_bytes(bytes[1..9].try_into().unwrap()),
        mc_seqno: u32::from_be_bytes(bytes[9..].try_into().unwrap()),
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
        let router_commit = BlockId {
            shard: ShardIdent::MASTERCHAIN,
            seqno: u32::MAX,
            root_hash: HashBytes([4; 32]),
            file_hash: HashBytes([5; 32]),
        };
        assert_eq!(decode_router_commit(&encode_router_commit(&router_commit)).unwrap(), router_commit);
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
        assert!(decode_router_commit(&[LAYOUT_VERSION; ROUTER_COMMIT_LEN - 1]).is_err());
        assert!(decode_router_commit(&[2; ROUTER_COMMIT_LEN]).is_err());
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
    fn router_and_commit_formats_round_trip_and_reject_malformed_values() {
        let short = block_id(u32::MAX).as_short_id();
        assert_eq!(decode_short_block_id(&encode_short_block_id(&short)).unwrap(), short);
        let key = partition_commit_key(u32::MAX, &short);
        assert_eq!(decode_partition_commit_key(&key).unwrap(), (u32::MAX, short));
        assert_eq!(partition_manifest_key(u64::MAX), u64::MAX.to_be_bytes());
        let router = RouterLocation {
            partition_id: u64::MAX,
            mc_seqno: u32::MAX,
        };
        assert_eq!(decode_router_location(&encode_router_location(router)).unwrap(), router);
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
        assert!(decode_router_location(&[LAYOUT_VERSION; ROUTER_LOCATION_LEN - 1]).is_err());
        assert!(decode_router_location(&[2; ROUTER_LOCATION_LEN]).is_err());
        assert!(decode_partition_commit(&[LAYOUT_VERSION; PARTITION_COMMIT_VALUE_LEN - 1]).is_err());
        assert!(decode_partition_commit(&[2; PARTITION_COMMIT_VALUE_LEN]).is_err());
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
