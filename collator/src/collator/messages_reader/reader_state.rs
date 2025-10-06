use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use tycho_block_util::queue::{QueueKey, QueuePartitionIdx};
use tycho_types::models::{IntAddr, ShardIdent};

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

#[derive(Default, Clone)]
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

#[derive(Default, Clone)]
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
    /// Clone with detailed timing metrics
    pub fn clone_with_metrics(&self, labels: &[(&str, String)]) -> Self {
        let ranges_start = std::time::Instant::now();
        let ranges = self.ranges.clone();
        let ranges_elapsed = ranges_start.elapsed();

        let partitions_start = std::time::Instant::now();
        let by_partitions = self.by_partitions.clone();
        let partitions_elapsed = partitions_start.elapsed();

        // Convert labels to Vec to avoid lifetime issues
        let labels_vec: Vec<(String, String)> = labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect();

        // Record metrics using the same pattern as other metrics in the codebase
        metrics::histogram!("tycho_collator_externals_ranges_clone_time", &labels_vec)
            .record(ranges_elapsed.as_millis() as f64);
        metrics::histogram!(
            "tycho_collator_externals_partitions_clone_time",
            &labels_vec
        )
        .record(partitions_elapsed.as_millis() as f64);

        println!(
            "Cloned ExternalsReaderState: ranges_elapsed = {:?}, partitions_elapsed = {:?}",
            ranges_elapsed, partitions_elapsed
        );

        Self {
            ranges,
            by_partitions,
            last_read_to_anchor_chain_time: self.last_read_to_anchor_chain_time,
        }
    }
}

impl ExternalsReaderState {
    pub fn get_state_by_partition_mut<T: Into<QueuePartitionIdx>>(
        &mut self,
        par_id: T,
    ) -> Result<&mut ExternalsReaderStateByPartition> {
        let par_id = par_id.into();
        self.by_partitions
            .get_mut(&par_id)
            .with_context(|| format!("externals reader state not exists for partition {par_id}"))
    }

    pub fn get_state_by_partition<T: Into<QueuePartitionIdx>>(
        &self,
        par_id: T,
    ) -> Result<&ExternalsReaderStateByPartition> {
        let par_id = par_id.into();
        self.by_partitions
            .get(&par_id)
            .with_context(|| format!("externals reader state not exists for partition {par_id}"))
    }
}

#[derive(Debug, Default, Clone)]
pub struct ExternalsReaderStateByPartition {
    /// The last processed external message from all ranges
    pub processed_to: ExternalKey,

    /// Actual current processed offset
    /// during the messages reading.
    /// Is incremented before collect.
    pub curr_processed_offset: u32,
}

#[derive(Clone)]
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
            format!("externals range reader state not exists for partition {par_id}")
        })
    }

    pub fn get_state_by_partition<T: Into<QueuePartitionIdx>>(
        &self,
        par_id: T,
    ) -> Result<&ExternalsRangeReaderStateByPartition> {
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

#[derive(Clone)]
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
    /// Last chain time used to check externals expiration.
    /// If `next_chain_time` was not changed on collect,
    /// we can omit the expire check.
    pub last_expire_check_on_ct: Option<u64>,
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
            last_expire_check_on_ct: None,
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

#[derive(Default, Clone)]
pub struct InternalsReaderState {
    pub partitions: BTreeMap<QueuePartitionIdx, InternalsPartitionReaderState>,
    pub cumulative_statistics: Option<CumulativeStatistics>,
}

impl InternalsReaderState {
    /// Clone with detailed timing metrics
    pub fn clone_with_metrics(&self, labels: &[(&str, String)]) -> Self {
        let partitions_start = std::time::Instant::now();

        // Clone partitions with detailed metrics for each InternalsRangeReaderState
        let mut partitions = BTreeMap::new();
        for (partition_id, partition_state) in &self.partitions {
            let partition_start = std::time::Instant::now();

            // Clone ranges with detailed metrics
            let mut ranges = BTreeMap::new();
            for (seqno, range_state) in &partition_state.ranges {
                let range_labels = [labels, &[
                    ("partition_id", partition_id.to_string()),
                    ("seqno", seqno.to_string()),
                ]]
                .concat();
                ranges.insert(*seqno, range_state.clone_with_metrics(&range_labels));
            }

            let partition_elapsed = partition_start.elapsed();

            // Convert labels to Vec to avoid lifetime issues
            let labels_vec: Vec<(String, String)> = labels
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect();

            // Record metrics for partition cloning
            metrics::histogram!("tycho_collator_internals_partition_clone_time", &labels_vec)
                .record(partition_elapsed.as_millis() as f64);

            println!(
                "Cloned partition {}: elapsed = {:?}",
                partition_id, partition_elapsed
            );

            partitions.insert(*partition_id, InternalsPartitionReaderState {
                ranges,
                processed_to: partition_state.processed_to.clone(),
                curr_processed_offset: partition_state.curr_processed_offset,
            });
        }

        let partitions_elapsed = partitions_start.elapsed();

        let stats_start = std::time::Instant::now();
        let cumulative_statistics = self.cumulative_statistics.clone();
        let stats_elapsed = stats_start.elapsed();

        // Convert labels to Vec to avoid lifetime issues
        let labels_vec: Vec<(String, String)> = labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect();

        // Record metrics using the same pattern as other metrics in the codebase
        metrics::histogram!(
            "tycho_collator_internals_partitions_clone_time",
            &labels_vec
        )
        .record(partitions_elapsed.as_millis() as f64);
        metrics::histogram!("tycho_collator_internals_stats_clone_time", &labels_vec)
            .record(stats_elapsed.as_millis() as f64);

        println!(
            "Cloned InternalsReaderState: partitions_elapsed = {:?}, stats_elapsed = {:?}",
            partitions_elapsed, stats_elapsed
        );

        Self {
            partitions,
            cumulative_statistics,
        }
    }
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

#[derive(Default, Clone)]
pub struct InternalsPartitionReaderState {
    /// Ranges will be extracted during collation process.
    /// Should access them only before collation and after reader finalization.
    pub ranges: BTreeMap<BlockSeqno, InternalsRangeReaderState>,

    pub processed_to: ProcessedTo,

    /// Actual current processed offset
    /// during the messages reading.
    /// Is incremented before collect.
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

#[derive(Clone)]
pub struct InternalsRangeReaderState {
    /// Buffer to store messages from the next iterator
    /// for accounts that have messages in the previous iterator
    /// until all messages from previous iterator are not read
    pub buffer: MessagesBuffer,

    /// Statistics shows all messages in current range
    pub msgs_stats: Option<Arc<QueueStatistics>>,
    /// Statistics shows remaining not read messages from current range.
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

    /// Clone with detailed timing metrics for each component
    pub fn clone_with_metrics(&self, labels: &[(&str, String)]) -> Self {
        let buffer_start = std::time::Instant::now();
        let buffer = self.buffer.clone();
        let buffer_elapsed = buffer_start.elapsed();

        let msgs_stats_start = std::time::Instant::now();
        let msgs_stats = self.msgs_stats.clone();
        let msgs_stats_elapsed = msgs_stats_start.elapsed();

        let remaning_msgs_stats_start = std::time::Instant::now();
        let remaning_msgs_stats = self.remaning_msgs_stats.clone();
        let remaning_msgs_stats_elapsed = remaning_msgs_stats_start.elapsed();

        let read_stats_start = std::time::Instant::now();
        let read_stats = self.read_stats.clone();
        let read_stats_elapsed = read_stats_start.elapsed();

        let shards_start = std::time::Instant::now();
        let shards = self.shards.clone();
        let shards_elapsed = shards_start.elapsed();

        // Convert labels to Vec to avoid lifetime issues
        let labels_vec: Vec<(String, String)> = labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect();

        // Record metrics for each component
        metrics::histogram!(
            "tycho_collator_internals_range_buffer_clone_time",
            &labels_vec
        )
        .record(buffer_elapsed.as_millis() as f64);
        metrics::histogram!(
            "tycho_collator_internals_range_msgs_stats_clone_time",
            &labels_vec
        )
        .record(msgs_stats_elapsed.as_millis() as f64);
        metrics::histogram!(
            "tycho_collator_internals_range_remaning_msgs_stats_clone_time",
            &labels_vec
        )
        .record(remaning_msgs_stats_elapsed.as_millis() as f64);
        metrics::histogram!(
            "tycho_collator_internals_range_read_stats_clone_time",
            &labels_vec
        )
        .record(read_stats_elapsed.as_millis() as f64);
        metrics::histogram!(
            "tycho_collator_internals_range_shards_clone_time",
            &labels_vec
        )
        .record(shards_elapsed.as_millis() as f64);

        println!(
            "Cloned InternalsRangeReaderState: buffer={:?}, msgs_stats={:?}, remaning_msgs_stats={:?}, read_stats={:?}, shards={:?}",
            buffer_elapsed,
            msgs_stats_elapsed,
            remaning_msgs_stats_elapsed,
            read_stats_elapsed,
            shards_elapsed
        );

        Self {
            buffer,
            msgs_stats,
            remaning_msgs_stats,
            read_stats,
            shards,
            skip_offset: self.skip_offset,
            processed_offset: self.processed_offset,
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
