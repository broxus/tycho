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
