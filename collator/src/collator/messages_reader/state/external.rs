use std::collections::BTreeMap;

use anyhow::Context;
use tycho_block_util::queue::QueuePartitionIdx;

use crate::collator::messages_buffer::{
    BufferFillStateByCount, BufferFillStateBySlots, MessagesBuffer, MessagesBufferLimits,
};
use crate::collator::state::DisplayRangeReaderStateByPartition;
use crate::mempool::MempoolAnchorId;
use crate::types::DebugIter;
use crate::types::processed_upto::{BlockSeqno, ExternalsRangeInfo};

#[derive(Default)]
pub struct ExternalsReaderState {
    /// We fully read each externals range
    /// because we unable to get remaning messages info
    /// in any other way.
    /// We need this for not to get messages for account `A` from range `2`
    /// when we still have messages for account `A` in range `1`.
    ///
    /// Ranges will be extracted during collation process.
    /// Should access them only before collation and after reader finalization.
    pub ranges: BTreeMap<BlockSeqno, ExternalsRangeReaderState>,

    /// Partition related externals reader state
    pub by_partitions: BTreeMap<QueuePartitionIdx, ExternalsPartitionReaderState>,

    /// last read to anchor chain time
    pub last_read_to_anchor_chain_time: Option<u64>,
}

// impl ExternalsReaderState {
//     pub fn get_state_by_partition_mut<T: Into<QueuePartitionIdx>>(
//         &mut self,
//         par_id: T,
//     ) -> anyhow::Result<&mut ExternalsReaderStateByPartition> {
//         let par_id = par_id.into();
//         self.by_partitions
//             .get_mut(&par_id)
//             .with_context(|| format!("externals reader state not exists for partition {par_id}"))
//     }
//
//     pub fn get_state_by_partition<T: Into<QueuePartitionIdx>>(
//         &self,
//         par_id: T,
//     ) -> anyhow::Result<&ExternalsReaderStateByPartition> {
//         let par_id = par_id.into();
//         self.by_partitions
//             .get(&par_id)
//             .with_context(|| format!("externals reader state not exists for partition {par_id}"))
//     }
// }

#[derive(Debug, Default, Clone)]
pub struct ExternalsPartitionReaderState {
    /// The last processed external message from all ranges
    pub processed_to: ExternalKey,

    /// Actual current processed offset
    /// during the messages reading.
    /// Is incremented before collect.
    pub curr_processed_offset: u32,
}

pub struct ExternalsRangeReaderState {
    /// Range info
    pub range: ExternalsReaderRange,
    /// Partition related externals range reader state
    pub by_partitions: BTreeMap<QueuePartitionIdx, ExternalsPartitionRangeReaderState>,
    pub fully_read_calculated: bool,
}

impl ExternalsRangeReaderState {
    pub fn get_state_by_partition_mut<T: Into<QueuePartitionIdx>>(
        &mut self,
        par_id: T,
    ) -> anyhow::Result<&mut ExternalsPartitionRangeReaderState> {
        let par_id = par_id.into();
        self.by_partitions.get_mut(&par_id).with_context(|| {
            format!("externals range reader state not exists for partition {par_id}")
        })
    }

    pub fn get_state_by_partition<T: Into<QueuePartitionIdx>>(
        &self,
        par_id: T,
    ) -> anyhow::Result<&ExternalsPartitionRangeReaderState> {
        let par_id = par_id.into();
        self.by_partitions.get(&par_id).with_context(|| {
            format!("externals range reader state not exists for partition {par_id}")
        })
    }
}

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

pub struct ExternalsPartitionRangeReaderState {
    /// Buffer to store external messages
    /// before collect them to the next execution group
    pub buffer: MessagesBuffer,
    /// Skip offset before collecting messages from this range.
    /// Because we should collect from others.
    pub skip_offset: u32,
    /// How many times externals messages were collected from all ranges.
    /// Every range contains offset that was reached when range was the last.
    /// So the current last range contains the actual offset.
    pub processed_offset: u32,
    /// Last chain time used to check externals expiration.
    /// If `next_chain_time` was not changed on collect,
    /// we can omit the expire check.
    pub last_expire_check_on_ct: Option<u64>,
}

impl ExternalsPartitionRangeReaderState {
    pub fn check_buffer_fill_state(
        &self,
        buffer_limits: &MessagesBufferLimits,
    ) -> (BufferFillStateByCount, BufferFillStateBySlots) {
        self.buffer.check_is_filled(buffer_limits)
    }
}

impl From<&ExternalsRangeInfo> for ExternalsPartitionRangeReaderState {
    fn from(value: &ExternalsRangeInfo) -> Self {
        Self {
            buffer: Default::default(),
            skip_offset: value.skip_offset,
            processed_offset: value.processed_offset,
            last_expire_check_on_ct: None,
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

pub struct DebugExternalsRangeReaderState<'a>(pub &'a ExternalsRangeReaderState);

impl std::fmt::Debug for DebugExternalsRangeReaderState<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("")
            .field("range", &self.0.range)
            .field(
                "by_partitions",
                &DebugIter(
                    self.0
                        .by_partitions
                        .iter()
                        .map(|(par_id, par)| (par_id, DisplayRangeReaderStateByPartition(par))),
                ),
            )
            .finish()
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
