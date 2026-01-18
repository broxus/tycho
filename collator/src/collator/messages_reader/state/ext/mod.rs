//! Module for managing external messages reading state.
//!
//! # State hierarchy for externals
//!
//! ExternalsReaderState
//! ├── ranges: BTreeMap<BlockSeqno, ExternalsRangeReaderState>
//! │   └── by_partitions: BTreeMap<QueuePartitionIdx, ExternalsPartitionRangeReaderState>
//! └── by_partitions: BTreeMap<QueuePartitionIdx, ExternalsPartitionReaderState>
//!
//! - `ExternalsReaderState` - top-level state, tracks all external message ranges and per-partition progress
//! - `ExternalsRangeReaderState` - state for a block range (anchor interval), tracks partition states within range
//! - `ExternalsPartitionReaderState` - global partition state, stores `processed_to` position across all ranges
//! - `ExternalsPartitionRangeReaderState` - partition state within a range, contains message buffer and offsets

use crate::collator::messages_reader::state::ext::partition_range_reader::ExternalsPartitionRangeReaderState;
use crate::mempool::MempoolAnchorId;
use crate::types::processed_upto::ExternalsRangeInfo;

pub mod partition_range_reader;
pub mod partition_reader;
pub mod range_reader;
pub mod reader;

#[derive(Debug, Clone)]
pub struct ExternalsReaderRange {
    pub from: ExternalKey,
    pub to: ExternalKey,
    pub current_position: ExternalKey,
    /// Chain time of the block during whose collation the range was read
    pub chain_time: u64,
}

impl ExternalsReaderRange {
    pub fn from_range_info(range_info: &ExternalsRangeInfo, processed_to: ExternalKey) -> Self {
        let from = range_info.from.into();
        let to = range_info.to.into();
        let current_position = if processed_to < from {
            from
        } else if processed_to < to {
            processed_to
        } else {
            to
        };
        Self {
            from,
            to,
            current_position,
            chain_time: range_info.chain_time,
        }
    }
}

impl From<(&ExternalsReaderRange, &ExternalsPartitionRangeReaderState)> for ExternalsRangeInfo {
    fn from((range, state): (&ExternalsReaderRange, &ExternalsPartitionRangeReaderState)) -> Self {
        Self {
            from: range.from.into(),
            to: range.to.into(),
            chain_time: range.chain_time,
            skip_offset: state.skip_offset,
            processed_offset: state.processed_offset,
        }
    }
}

#[derive(Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ExternalKey {
    pub anchor_id: MempoolAnchorId,
    pub msgs_offset: u64,
}

impl std::fmt::Debug for ExternalKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({}, {})", self.anchor_id, self.msgs_offset)
    }
}

impl From<(MempoolAnchorId, u64)> for ExternalKey {
    fn from(value: (MempoolAnchorId, u64)) -> Self {
        Self {
            anchor_id: value.0,
            msgs_offset: value.1,
        }
    }
}

impl From<ExternalKey> for (MempoolAnchorId, u64) {
    fn from(value: ExternalKey) -> Self {
        (value.anchor_id, value.msgs_offset)
    }
}
