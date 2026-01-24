use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Context;
use tycho_block_util::queue::QueuePartitionIdx;
use tycho_types::models::{IntAddr, ShardIdent};
use tycho_util::FastHashMap;
use tycho_util_proc::Transactional;

use crate::collator::messages_buffer::MessagesBuffer;
use crate::collator::messages_reader::state::{DisplayShardReaderState, ShardReaderState};
use crate::collator::statistics::cumulative::CumulativeStatistics;
use crate::internal_queue::types::stats::QueueStatistics;
use crate::types::processed_upto::{BlockSeqno, InternalsProcessedUptoStuff, InternalsRangeStuff};
use crate::types::{DebugIter, ProcessedTo};

#[derive(Transactional, Default)]
pub struct InternalsReaderState {
    #[tx(collection)]
    partitions: FastHashMap<QueuePartitionIdx, InternalsPartitionReaderState>,

    #[tx(transactional)]
    cumulative_statistics: Option<CumulativeStatistics>,

    #[tx(state)]
    tx: Option<InternalsReaderStateTx>,
}

impl InternalsReaderState {
    pub fn new(
        partitions: FastHashMap<QueuePartitionIdx, InternalsPartitionReaderState>,
        cumulative_statistics: Option<CumulativeStatistics>,
    ) -> Self {
        Self {
            partitions,
            cumulative_statistics,
            tx: None,
        }
    }

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

    pub fn insert_partition(
        &mut self,
        par_id: QueuePartitionIdx,
        state: InternalsPartitionReaderState,
    ) {
        self.tx_insert_partitions(par_id, state);
    }

    pub fn get_partition(
        &self,
        par_id: &QueuePartitionIdx,
    ) -> anyhow::Result<&InternalsPartitionReaderState> {
        self.partitions
            .get(par_id)
            .with_context(|| format!("internals reader state not exists for partition {par_id}"))
    }

    pub fn partitions(&self) -> &FastHashMap<QueuePartitionIdx, InternalsPartitionReaderState> {
        &self.partitions
    }

    pub fn cumulative_statistics(&self) -> &Option<CumulativeStatistics> {
        &self.cumulative_statistics
    }

    pub fn set_cumulative_statistics(&mut self, stats: Option<CumulativeStatistics>) {
        self.tx_set_cumulative_statistics(stats);
    }

    pub fn tx_cumulative_statistics_mut(&mut self) -> &mut Option<CumulativeStatistics> {
        &mut self.cumulative_statistics
    }

    pub fn tx_partitions_mut(
        &mut self,
    ) -> &mut FastHashMap<QueuePartitionIdx, InternalsPartitionReaderState> {
        &mut self.partitions
    }

    pub fn ensure_partition(&mut self, par_id: QueuePartitionIdx) {
        if !self.partitions.contains_key(&par_id) {
            self.tx_insert_partitions(par_id, Default::default());
        }
    }

    pub fn tx_partitions_and_cumulative_stats_mut(
        &mut self,
    ) -> (
        &mut FastHashMap<QueuePartitionIdx, InternalsPartitionReaderState>,
        &mut Option<CumulativeStatistics>,
    ) {
        (&mut self.partitions, &mut self.cumulative_statistics)
    }
}

#[derive(Transactional, Default)]
pub struct InternalsPartitionReaderState {
    /// Ranges will be extracted during collation process.
    /// Should access them only before collation and after reader finalization.
    #[tx(collection)]
    pub ranges: BTreeMap<BlockSeqno, InternalsRangeReaderState>,

    pub processed_to: ProcessedTo,

    /// Actual current processed offset
    /// during the messages reading.
    /// Is incremented before collect.
    pub curr_processed_offset: u32,

    #[tx(state)]
    tx: Option<InternalsPartitionReaderStateTx>,
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
            tx: None,
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

#[derive(Default, Transactional)]
pub struct InternalsRangeReaderState {
    /// Buffer to store messages from the next iterator
    /// for accounts that have messages in the previous iterator
    /// until all messages from previous iterator are not read
    #[tx(transactional)]
    pub buffer: MessagesBuffer,

    /// Statistics shows all messages in current range
    #[tx(skip)]
    pub msgs_stats: Option<Arc<QueueStatistics>>,
    /// Statistics shows remaining not read messages from current range.
    /// We reduce initial statistics by the number of messages that were read.
    #[tx(transactional)]
    pub remaning_msgs_stats: Option<QueueStatistics>,
    /// Statistics shows read messages in current range
    #[tx(transactional)]
    pub read_stats: QueueStatistics,

    #[tx(skip)]
    pub shards: BTreeMap<ShardIdent, ShardReaderState>,

    /// Skip offset before collecting messages from this range.
    /// Because we should collect from others.
    pub skip_offset: u32,
    /// How many times internal messages were collected from all ranges.
    /// Every range contains offset that was reached when range was the last.
    /// So the current last range contains the actual offset.
    pub processed_offset: u32,

    #[tx(state)]
    pub tx: Option<InternalsRangeReaderStateTx>,
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

    pub fn is_fully_read(&self) -> bool {
        self.shards.values().all(|s| s.is_fully_read())
    }

    pub fn from_range_info(range_info: &InternalsRangeStuff, processed_to: &ProcessedTo) -> Self {
        let mut res = Self {
            skip_offset: range_info.skip_offset,
            processed_offset: range_info.processed_offset,
            ..Default::default()
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
