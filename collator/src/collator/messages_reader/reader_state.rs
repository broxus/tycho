use std::collections::BTreeMap;

use anyhow::{Context, Result};
use everscale_types::models::{IntAddr, ShardIdent};
use tycho_block_util::queue::{QueueKey, QueuePartitionIdx};

use super::super::messages_buffer::MessagesBuffer;
use crate::collator::messages_buffer::{
    BufferFillStateByCount, BufferFillStateBySlots, MessagesBufferLimits,
};
use crate::collator::types::CumulativeStatistics;
use crate::internal_queue::types::QueueStatistics;
use crate::mempool::MempoolAnchorId;
use crate::types::processed_upto::{
    BlockSeqno, ExternalsProcessedUptoStuff, ExternalsRangeInfo, InternalsProcessedUptoStuff,
    InternalsRangeStuff, Lt, ProcessedUptoInfoStuff, ProcessedUptoPartitionStuff, ShardRangeInfo,
};
use crate::types::{DebugIter, ProcessedTo};

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
        let mut ext_reader_state = ExternalsReaderState::default();
        for (par_id, par) in &processed_upto.partitions {
            let processed_to = par.externals.processed_to.into();
            ext_reader_state
                .by_partitions
                .insert(*par_id, ExternalsReaderStateByPartition {
                    processed_to,
                    curr_processed_offset: 0,
                });
            for (seqno, range_info) in &par.externals.ranges {
                ext_reader_state
                    .ranges
                    .entry(*seqno)
                    .and_modify(|r| {
                        r.by_partitions.insert(*par_id, range_info.into());
                    })
                    .or_insert(ExternalsRangeReaderState {
                        range: ExternalsReaderRange::from_range_info(range_info, processed_to),
                        by_partitions: [(*par_id, range_info.into())].into(),
                    });
            }
        }
        Self {
            internals: InternalsReaderState {
                partitions: processed_upto
                    .partitions
                    .iter()
                    .map(|(k, v)| (*k, (&v.internals).into()))
                    .collect(),
                cumulative_statistics: None,
            },
            externals: ext_reader_state,
        }
    }

    pub fn get_updated_processed_upto(&self) -> ProcessedUptoInfoStuff {
        let mut processed_upto = ProcessedUptoInfoStuff::default();
        for (par_id, par) in &self.internals.partitions {
            let ext_reader_state_by_partition =
                self.externals.get_state_by_partition(*par_id).unwrap();
            processed_upto
                .partitions
                .insert(*par_id, ProcessedUptoPartitionStuff {
                    externals: ExternalsProcessedUptoStuff {
                        processed_to: ext_reader_state_by_partition.processed_to.into(),
                        ranges: self
                            .externals
                            .ranges
                            .iter()
                            .map(|(k, v)| {
                                let ext_range_reader_state_by_partition =
                                    v.get_state_by_partition(*par_id).unwrap();
                                (*k, (&v.range, ext_range_reader_state_by_partition).into())
                            })
                            .collect(),
                    },
                    internals: par.into(),
                });
        }
        processed_upto
    }

    pub fn check_has_non_zero_processed_offset(&self) -> bool {
        let check_internals = self
            .internals
            .partitions
            .values()
            .any(|par| par.ranges.values().any(|r| r.processed_offset > 0));
        if check_internals {
            return check_internals;
        }

        self.externals
            .ranges
            .values()
            .any(|r| r.by_partitions.values().any(|par| par.processed_offset > 0))
    }

    pub fn has_messages_in_buffers(&self) -> bool {
        self.has_internals_in_buffers() || self.has_externals_in_buffers()
    }

    pub fn has_internals_in_buffers(&self) -> bool {
        self.internals
            .partitions
            .values()
            .any(|par| par.ranges.values().any(|r| r.buffer.msgs_count() > 0))
    }

    pub fn has_externals_in_buffers(&self) -> bool {
        self.externals.ranges.values().any(|r| {
            r.by_partitions
                .values()
                .any(|par| par.buffer.msgs_count() > 0)
        })
    }
}

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
    pub by_partitions: BTreeMap<QueuePartitionIdx, ExternalsReaderStateByPartition>,

    /// last read to anchor chain time
    pub last_read_to_anchor_chain_time: Option<u64>,
}

impl ExternalsReaderState {
    pub fn get_state_by_partition_mut<T: Into<QueuePartitionIdx>>(
        &mut self,
        par_id: T,
    ) -> Result<&mut ExternalsReaderStateByPartition> {
        let par_id = par_id.into();
        self.by_partitions
            .get_mut(&par_id)
            .with_context(|| format!("externals reader state not exists for partition {}", par_id))
    }

    pub fn get_state_by_partition<T: Into<QueuePartitionIdx>>(
        &self,
        par_id: T,
    ) -> Result<&ExternalsReaderStateByPartition> {
        let par_id = par_id.into();
        self.by_partitions
            .get(&par_id)
            .with_context(|| format!("externals reader state not exists for partition {}", par_id))
    }
}

#[derive(Debug, Default)]
pub struct ExternalsReaderStateByPartition {
    /// The last processed external message from all ranges
    pub processed_to: ExternalKey,

    /// Actual current processed offset
    /// during the messages reading.
    pub curr_processed_offset: u32,
}

pub struct ExternalsRangeReaderState {
    /// Range info
    pub range: ExternalsReaderRange,
    /// Partition related externals range reader state
    pub by_partitions: BTreeMap<QueuePartitionIdx, ExternalsRangeReaderStateByPartition>,
}

impl ExternalsRangeReaderState {
    pub fn get_state_by_partition_mut<T: Into<QueuePartitionIdx>>(
        &mut self,
        par_id: T,
    ) -> Result<&mut ExternalsRangeReaderStateByPartition> {
        let par_id = par_id.into();
        self.by_partitions.get_mut(&par_id).with_context(|| {
            format!(
                "externals range reader state not exists for partition {}",
                par_id
            )
        })
    }

    pub fn get_state_by_partition<T: Into<QueuePartitionIdx>>(
        &self,
        par_id: T,
    ) -> Result<&ExternalsRangeReaderStateByPartition> {
        let par_id = par_id.into();
        self.by_partitions.get(&par_id).with_context(|| {
            format!(
                "externals range reader state not exists for partition {}",
                par_id
            )
        })
    }
}

#[derive(Debug)]
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

pub struct ExternalsRangeReaderStateByPartition {
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
}

impl ExternalsRangeReaderStateByPartition {
    pub fn check_buffer_fill_state(
        &self,
        buffer_limits: &MessagesBufferLimits,
    ) -> (BufferFillStateByCount, BufferFillStateBySlots) {
        self.buffer.check_is_filled(buffer_limits)
    }
}

impl From<&ExternalsRangeInfo> for ExternalsRangeReaderStateByPartition {
    fn from(value: &ExternalsRangeInfo) -> Self {
        Self {
            buffer: Default::default(),
            skip_offset: value.skip_offset,
            processed_offset: value.processed_offset,
        }
    }
}

impl From<(&ExternalsReaderRange, &ExternalsRangeReaderStateByPartition)> for ExternalsRangeInfo {
    fn from(
        (range, state): (&ExternalsReaderRange, &ExternalsRangeReaderStateByPartition),
    ) -> Self {
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

pub struct DisplayRangeReaderStateByPartition<'a>(pub &'a ExternalsRangeReaderStateByPartition);
impl std::fmt::Debug for DisplayRangeReaderStateByPartition<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::fmt::Display for DisplayRangeReaderStateByPartition<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("")
            .field("skip_offset", &self.0.skip_offset)
            .field("processed_offset", &self.0.processed_offset)
            .field("buffer.msgs_count", &self.0.buffer.msgs_count())
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

#[derive(Default)]
pub struct InternalsReaderState {
    pub partitions: BTreeMap<QueuePartitionIdx, InternalsPartitionReaderState>,
    pub cumulative_statistics: Option<CumulativeStatistics>,
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
pub struct InternalsPartitionReaderState {
    /// Ranges will be extracted during collation process.
    /// Should access them only before collation and after reader finalization.
    pub ranges: BTreeMap<BlockSeqno, InternalsRangeReaderState>,

    pub processed_to: ProcessedTo,

    /// Actual current processed offset
    /// during the messages reading.
    pub curr_processed_offset: u32,
}

impl From<&InternalsProcessedUptoStuff> for InternalsPartitionReaderState {
    fn from(value: &InternalsProcessedUptoStuff) -> Self {
        Self {
            curr_processed_offset: 0,
            processed_to: value.processed_to.clone(),
            ranges: value
                .ranges
                .iter()
                .map(|(k, v)| {
                    (
                        *k,
                        InternalsRangeReaderState::from_range_info(v, &value.processed_to),
                    )
                })
                .collect(),
        }
    }
}

impl From<&InternalsPartitionReaderState> for InternalsProcessedUptoStuff {
    fn from(value: &InternalsPartitionReaderState) -> Self {
        Self {
            processed_to: value.processed_to.clone(),
            ranges: value.ranges.iter().map(|(k, v)| (*k, v.into())).collect(),
        }
    }
}

pub struct InternalsRangeReaderState {
    /// Buffer to store messages from the next iterator
    /// for accounts that have messages in the previous iterator
    /// until all messages from previous iterator are not read
    pub buffer: MessagesBuffer,

    /// Statistics shows all messages in current range
    pub msgs_stats: Option<QueueStatistics>,
    /// Statistics shows remaining not read messages from currebt range.
    /// We reduce initial statistics by the number of messages that were read.
    pub remaning_msgs_stats: Option<QueueStatistics>,
    /// Statistics shows read messages in current range
    pub read_stats: QueueStatistics,

    pub shards: BTreeMap<ShardIdent, ShardReaderState>,

    /// Skip offset before collecting messages from this range.
    /// Because we should collect from others.
    pub skip_offset: u32,
    /// How many times internal messages were collected from all ranges.
    /// Every range contains offset that was reached when range was the last.
    /// So the current last range contains the actual offset.
    pub processed_offset: u32,
}

impl InternalsRangeReaderState {
    pub fn contains_account_addr_in_remaning_msgs_stats(&self, account_addr: &IntAddr) -> bool {
        match &self.remaning_msgs_stats {
            None => false,
            Some(remaning_msgs_stats) => {
                remaning_msgs_stats.statistics().contains_key(account_addr)
            }
        }
    }
}

impl InternalsRangeReaderState {
    pub fn from_range_info(range_info: &InternalsRangeStuff, processed_to: &ProcessedTo) -> Self {
        let mut res = Self {
            buffer: Default::default(),
            msgs_stats: None,
            remaning_msgs_stats: None,
            read_stats: Default::default(),
            skip_offset: range_info.skip_offset,
            processed_offset: range_info.processed_offset,
            shards: Default::default(),
        };

        for (shard_id, shard_range_info) in &range_info.shards {
            let shard_processed_to = processed_to.get(shard_id).copied().unwrap_or_default();
            let reader_state =
                ShardReaderState::from_range_info(shard_range_info, shard_processed_to);
            res.shards.insert(*shard_id, reader_state);
        }

        res
    }
}

impl From<&InternalsRangeReaderState> for InternalsRangeStuff {
    fn from(value: &InternalsRangeReaderState) -> Self {
        Self {
            skip_offset: value.skip_offset,
            processed_offset: value.processed_offset,
            shards: value.shards.iter().map(|(k, v)| (*k, v.into())).collect(),
        }
    }
}

pub struct DebugInternalsRangeReaderState<'a>(pub &'a InternalsRangeReaderState);
impl std::fmt::Debug for DebugInternalsRangeReaderState<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("")
            .field("skip_offset", &self.0.skip_offset)
            .field("processed_offset", &self.0.processed_offset)
            .field("buffer.msgs_count", &self.0.buffer.msgs_count())
            .field(
                "remaning_msgs_stats.accounts_count",
                &self
                    .0
                    .remaning_msgs_stats
                    .as_ref()
                    .map(|s| s.statistics().len()),
            )
            .field(
                "read_stats.accounts_count",
                &self.0.read_stats.statistics().len(),
            )
            .field(
                "shards",
                &DebugIter(
                    self.0
                        .shards
                        .iter()
                        .map(|(shard_id, r_s)| (shard_id, DisplayShardReaderState(r_s))),
                ),
            )
            .finish()
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ShardReaderState {
    pub from: Lt,
    pub to: Lt,
    pub current_position: QueueKey,
}

impl ShardReaderState {
    pub fn from_range_info(range_info: &ShardRangeInfo, processed_to: QueueKey) -> Self {
        let current_position = if processed_to.lt < range_info.from {
            QueueKey::max_for_lt(range_info.from)
        } else if processed_to.lt < range_info.to {
            processed_to
        } else {
            QueueKey::max_for_lt(range_info.to)
        };
        Self {
            from: range_info.from,
            to: range_info.to,
            current_position,
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

struct DisplayShardReaderState<'a>(pub &'a ShardReaderState);
impl std::fmt::Debug for DisplayShardReaderState<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self, f)
    }
}
impl std::fmt::Display for DisplayShardReaderState<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("")
            .field("from", &self.0.from)
            .field("to", &self.0.to)
            .field("current_position", &self.0.current_position)
            .finish()
    }
}
