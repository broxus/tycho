use std::collections::BTreeMap;

use everscale_types::models::ShardIdent;
use tycho_block_util::queue::QueueKey;

use super::super::messages_buffer::MessagesBuffer;
use crate::mempool::MempoolAnchorId;
use crate::types::processed_upto::{
    BlockSeqno, ExternalsProcessedUptoStuff, ExternalsRangeInfo, InternalsProcessedUptoStuff,
    InternalsRangeStuff, Lt, PartitionId, PartitionProcessedUptoStuff, ProcessedUptoInfoStuff,
    ShardRangeInfo,
};
use crate::types::ProcessedTo;

//=========
// READER STATE
//=========

#[derive(Default)]
pub struct ReaderState {
    pub externals: ExternalsReaderState,
    pub internals: InternalsReaderState,
}

impl ReaderState {
    pub fn new(processed_upto: &ProcessedUptoInfoStuff) -> Self {
        Self {
            externals: ExternalsReaderState {
                curr_processed_offset: 0,
                processed_to: processed_upto.externals.processed_to.into(),
                ranges: processed_upto
                    .externals
                    .ranges
                    .iter()
                    .map(|(k, v)| (*k, v.into()))
                    .collect(),
                last_read_to_anchor_chain_time: None,
            },
            internals: InternalsReaderState {
                partitions: processed_upto
                    .internals
                    .partitions
                    .iter()
                    .map(|(k, v)| (*k, v.into()))
                    .collect(),
            },
        }
    }

    pub fn get_updated_processed_upto(&self) -> ProcessedUptoInfoStuff {
        ProcessedUptoInfoStuff {
            externals: ExternalsProcessedUptoStuff {
                processed_to: self.externals.processed_to.into(),
                ranges: self
                    .externals
                    .ranges
                    .iter()
                    .map(|(k, v)| (*k, v.into()))
                    .collect(),
            },
            internals: InternalsProcessedUptoStuff {
                partitions: self
                    .internals
                    .partitions
                    .iter()
                    .map(|(k, v)| (*k, v.into()))
                    .collect(),
            },
        }
    }

    pub fn check_has_non_zero_processed_offset(&self) -> bool {
        let check_externals = self
            .externals
            .ranges
            .iter()
            .any(|(_, r_s)| r_s.processed_offset > 0);

        if check_externals {
            return check_externals;
        }

        let check_internals = self
            .internals
            .partitions
            .iter()
            .any(|(_, p_s)| p_s.ranges.iter().any(|(_, r_s)| r_s.processed_offset > 0));

        check_internals
    }

    pub fn count_messages_in_buffers(&self) -> usize {
        let ext_count = self
            .externals
            .ranges
            .values()
            .fold(0, |acc, range| acc + range.buffer.msgs_count());
        let int_count = self.internals.partitions.values().fold(0, |p_acc, p| {
            p_acc
                + p.ranges
                    .values()
                    .fold(0, |r_acc, r| r_acc + r.buffer.msgs_count())
        });
        ext_count + int_count
    }
}

#[derive(Default)]
pub struct ExternalsReaderState {
    /// We fully read each externals range
    /// because we unable to get remaning messages info
    /// in any other way.
    /// We need this for not to get messages for account `A` from range `2`
    /// when we still have messages for account `A` in range `1`.
    pub ranges: BTreeMap<BlockSeqno, ExternalsRangeReaderState>,

    /// When all messages from the range (and from all its prev ranges)
    /// are processed we can update `processed_to` from this range.
    pub processed_to: ExternalKey,

    /// Actual current processed offset
    /// during the messages reading.
    pub curr_processed_offset: u16,

    /// last read to anchor chain time
    pub last_read_to_anchor_chain_time: Option<u64>,
}

#[derive(Default)]
pub struct ExternalsRangeReaderState {
    /// Buffer to store external messages
    /// before collect them to the next execution group
    pub buffer: MessagesBuffer,

    pub from: ExternalKey,
    pub to: ExternalKey,

    pub current_position: ExternalKey,

    /// Chain time of the block during whose collation the range was read
    pub chain_time: u64,

    /// How many times external messages were collected from the ranges buffers.
    /// Every range contains offset that was reached when range was the last.
    /// So the current last range contains the actual offset.
    pub processed_offset: u16,
}

impl From<&ExternalsRangeInfo> for ExternalsRangeReaderState {
    fn from(value: &ExternalsRangeInfo) -> Self {
        Self {
            buffer: Default::default(),
            from: value.from.into(),
            to: value.to.into(),
            // on init current position is on the from
            current_position: value.from.into(),
            chain_time: value.chain_time,
            processed_offset: value.processed_offset,
        }
    }
}
impl From<&ExternalsRangeReaderState> for ExternalsRangeInfo {
    fn from(value: &ExternalsRangeReaderState) -> Self {
        Self {
            from: value.from.into(),
            to: value.to.into(),
            chain_time: value.chain_time,
            processed_offset: value.processed_offset,
        }
    }
}

pub struct DebugExternalsRangeReaderState<'a>(pub &'a ExternalsRangeReaderState);
impl std::fmt::Debug for DebugExternalsRangeReaderState<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExternalsRangeReaderState")
            .field("from", &self.0.from)
            .field("to", &self.0.to)
            .field("current_position", &self.0.current_position)
            .field("chain_time", &self.0.chain_time)
            .field("processed_offset", &self.0.processed_offset)
            .finish()
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ExternalKey {
    pub anchor_id: MempoolAnchorId,
    pub msgs_offset: u64,
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

#[derive(Default)]
pub struct InternalsReaderState {
    pub partitions: BTreeMap<PartitionId, PartitionReaderState>,
}

impl InternalsReaderState {
    pub fn get_min_processed_to_by_shards(&self) -> ProcessedTo {
        let mut shards_processed_to = ProcessedTo::default();
        for par_s in self.partitions.values() {
            for (shard_id, key) in &par_s.processed_to {
                shards_processed_to
                    .entry(*shard_id)
                    .and_modify(|min_key| *min_key = std::cmp::min(*min_key, *key))
                    .or_insert(*key);
            }
        }
        shards_processed_to
    }
}

#[derive(Default)]
pub struct PartitionReaderState {
    pub ranges: BTreeMap<BlockSeqno, InternalsRangeReaderState>,
    pub processed_to: BTreeMap<ShardIdent, QueueKey>,

    /// Actual current processed offset
    /// during the messages reading.
    pub curr_processed_offset: u16,
}

impl From<&PartitionProcessedUptoStuff> for PartitionReaderState {
    fn from(value: &PartitionProcessedUptoStuff) -> Self {
        Self {
            curr_processed_offset: 0,
            processed_to: value.processed_to.clone(),
            ranges: value.ranges.iter().map(|(k, v)| (*k, v.into())).collect(),
        }
    }
}
impl From<&PartitionReaderState> for PartitionProcessedUptoStuff {
    fn from(value: &PartitionReaderState) -> Self {
        Self {
            processed_to: value.processed_to.clone(),
            ranges: value.ranges.iter().map(|(k, v)| (*k, v.into())).collect(),
        }
    }
}

#[derive(Default)]
pub struct InternalsRangeReaderState {
    /// Buffer to store messages from the next iterator
    /// for accounts that have messages in the previous iterator
    /// until all messages from previous iterator are not read
    pub buffer: MessagesBuffer,

    pub shards: BTreeMap<ShardIdent, ShardReaderState>,

    /// How many times internal messages were collected from the ranges buffers.
    /// Every range contains offset that was reached when range was the last.
    /// So the current last range contains the actual offset.
    pub processed_offset: u16,
}

impl From<&InternalsRangeStuff> for InternalsRangeReaderState {
    fn from(value: &InternalsRangeStuff) -> Self {
        Self {
            buffer: Default::default(),
            processed_offset: value.processed_offset,
            shards: value.shards.iter().map(|(k, v)| (*k, v.into())).collect(),
        }
    }
}
impl From<&InternalsRangeReaderState> for InternalsRangeStuff {
    fn from(value: &InternalsRangeReaderState) -> Self {
        Self {
            processed_offset: value.processed_offset,
            shards: value.shards.iter().map(|(k, v)| (*k, v.into())).collect(),
        }
    }
}

pub struct DebugInternalsRangeReaderState<'a>(pub &'a InternalsRangeReaderState);
impl std::fmt::Debug for DebugInternalsRangeReaderState<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InternalsRangeReaderState")
            .field("processed_offset", &self.0.processed_offset)
            .field("shards", &self.0.shards)
            .finish()
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ShardReaderState {
    pub from: Lt,
    pub to: Lt,
    pub current_position: QueueKey,
}

impl From<&ShardRangeInfo> for ShardReaderState {
    fn from(value: &ShardRangeInfo) -> Self {
        Self {
            from: value.from,
            to: value.to,
            // on init current position is on the from
            current_position: QueueKey::min_for_lt(value.from),
        }
    }
}
impl From<&ShardReaderState> for ShardRangeInfo {
    fn from(value: &ShardReaderState) -> Self {
        Self {
            from: value.from,
            to: value.to,
        }
    }
}
