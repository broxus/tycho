use std::collections::BTreeMap;
use std::sync::Arc;

use tycho_block_util::queue::QueuePartitionIdx;
use tycho_types::models::{IntAddr, ShardIdent};

use crate::collator::messages_buffer::MessagesBuffer;
use crate::collator::messages_reader::state::{DisplayShardReaderState, ShardReaderState};
use crate::collator::types::CumulativeStatistics;
use crate::internal_queue::types::stats::QueueStatistics;
use crate::types::processed_upto::{BlockSeqno, InternalsProcessedUptoStuff, InternalsRangeStuff};
use crate::types::{DebugIter, ProcessedTo};

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
