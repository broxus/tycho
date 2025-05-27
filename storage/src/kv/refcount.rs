use std::cmp::Ordering;
use std::convert::TryInto;

use weedb::rocksdb;
use weedb::rocksdb::compaction_filter::Decision;

pub type RcType = i64;

pub const RC_BYTES: usize = std::mem::size_of::<RcType>();

pub fn merge_operator(
    _key: &[u8],
    existing: Option<&[u8]>,
    operands: &rocksdb::MergeOperands,
) -> Option<Vec<u8>> {
    let (mut rc, mut payload) = existing.map_or((0, None), decode_value_with_rc);
    for (delta, new_payload) in operands.into_iter().map(decode_value_with_rc) {
        if payload.is_none() && delta > 0 {
            payload = new_payload;
        }
        rc += delta;
    }

    Some(match rc.cmp(&0) {
        Ordering::Less => rc.to_le_bytes().to_vec(),
        Ordering::Equal => Vec::new(),
        Ordering::Greater => {
            let payload = payload.unwrap_or(&[]);
            let mut result = Vec::with_capacity(RC_BYTES + payload.len());
            result.extend_from_slice(&rc.to_le_bytes());
            result.extend_from_slice(payload);
            result
        }
    })
}

pub fn compaction_filter(_level: u32, _key: &[u8], value: &[u8]) -> Decision {
    if value.is_empty() {
        #[cfg(feature = "cells-metrics")]
        metrics::counter!("tycho_compaction_removes").increment(1);

        Decision::Remove
    } else {
        #[cfg(feature = "cells-metrics")]
        metrics::counter!("tycho_compaction_keeps").increment(1);

        Decision::Keep
    }
}

pub fn decode_value_with_rc(bytes: &[u8]) -> (RcType, Option<&[u8]>) {
    let without_payload = match bytes.len().cmp(&RC_BYTES) {
        Ordering::Greater => false,
        Ordering::Equal => true,
        Ordering::Less => return (0, None),
    };

    let rc = RcType::from_le_bytes(bytes[..RC_BYTES].try_into().unwrap());
    if rc <= 0 || without_payload {
        (rc, None)
    } else {
        (rc, Some(&bytes[RC_BYTES..]))
    }
}

// will be use in persistent storage writer
pub fn strip_refcount(bytes: &[u8]) -> Option<&[u8]> {
    if bytes.len() < RC_BYTES {
        return None;
    }
    if RcType::from_le_bytes(bytes[..RC_BYTES].try_into().unwrap()) > 0 {
        Some(&bytes[RC_BYTES..])
    } else {
        None
    }
}

pub fn add_positive_refount(rc: u32, data: Option<&[u8]>, target: &mut Vec<u8>) {
    target.extend_from_slice(&RcType::from(rc).to_le_bytes());
    if let Some(data) = data {
        target.extend_from_slice(data);
    }
}

pub fn encode_positive_refcount(rc: u32) -> [u8; RC_BYTES] {
    RcType::from(rc).to_le_bytes()
}

pub fn encode_negative_refcount(rc: u32) -> [u8; RC_BYTES] {
    (-RcType::from(rc)).to_le_bytes()
}
