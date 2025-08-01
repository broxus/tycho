use anyhow::Result;
use tracing::instrument;
use tycho_block_util::queue::{QueueKey, QueuePartitionIdx};
use tycho_types::cell::HashBytes;
use tycho_types::models::{BlockId, BlockIdShort, ShardIdent};
use tycho_util::metrics::HistogramGuard;
use tycho_util::{FastHashMap, FastHashSet};

use crate::internal_queue::iterator::{QueueIterator, QueueIteratorImpl};
use crate::internal_queue::queue::{Queue, QueueImpl};
use crate::internal_queue::state::states_iterators_manager::StatesIteratorsManager;
use crate::internal_queue::state::storage::QueueStateStdImpl;
use crate::internal_queue::types::{
    DiffStatistics, DiffZone, InternalMessageValue, PartitionRouter, QueueDiffWithMessages,
    QueueShardBoundedRange, QueueShardRange, QueueStatistics, SeparatedStatisticsByPartitions,
};
use crate::storage::models::DiffInfo;
use crate::storage::snapshot::AccountStatistics;
use crate::tracing_targets;
use crate::types::{DebugDisplayOpt, DebugIter, DisplayIter, DisplayTupleRef};

pub struct MessageQueueAdapterStdImpl<V: InternalMessageValue> {
    queue: QueueImpl<QueueStateStdImpl, V>,
}

pub trait MessageQueueAdapter<V>: Send + Sync
where
    V: InternalMessageValue + Send + Sync,
{
    /// Create iterator for specified shard and return it
    fn create_iterator(
        &self,
        for_shard_id: ShardIdent,
        partition: QueuePartitionIdx,
        ranges: Vec<QueueShardBoundedRange>,
    ) -> Result<Box<dyn QueueIterator<V>>>;

    /// Returns statistics for the specified ranges by partition
    /// and source shards (equal to iterator ranges)
    fn get_statistics(
        &self,
        partitions: &FastHashSet<QueuePartitionIdx>,
        ranges: &[QueueShardBoundedRange],
    ) -> Result<QueueStatistics>;

    /// Apply diff to the current queue uncommitted state (waiting for the operation to complete)
    fn apply_diff(
        &self,
        diff: QueueDiffWithMessages<V>,
        block_id_short: BlockIdShort,
        diff_hash: &HashBytes,
        statistics: DiffStatistics,
        check_sequence: Option<DiffZone>,
    ) -> Result<()>;

    /// Commit previously applied diff, saving changes to committed state (waiting for the operation to complete).
    /// Return `None` if specified diff does not exist.
    fn commit_diff(
        &self,
        mc_top_blocks: Vec<(BlockId, bool)>,
        partitions: &FastHashSet<QueuePartitionIdx>,
    ) -> Result<()>;

    fn clear_uncommitted_state(&self, top_shards: &[ShardIdent]) -> Result<()>;

    /// Get diff for the given block from committed and uncommitted state
    fn get_diff_info(
        &self,
        shard_ident: &ShardIdent,
        seqno: u32,
        zone: DiffZone,
    ) -> Result<Option<DiffInfo>>;
    /// Check if diff exists in the cache
    fn is_diff_exists(&self, block_id_short: &BlockIdShort) -> Result<bool>;
    /// Get last committed mc block id
    fn get_last_commited_mc_block_id(&self) -> Result<Option<BlockId>>;
    /// Get diffs tail len from uncommitted state and committed state
    /// `from` - start key for the tail. Diff with `max_message` == `from` will be excluded from the tail
    fn get_diffs_tail_len(&self, shard_ident: &ShardIdent, from: &QueueKey) -> u32;
    /// Load separated diff statistics for the specified partitions and range
    /// `range.from` = diff with `max_message == range.from` will be excluded in statistics
    /// `range.to` = diff with `max_message == range.to` will be included in statistics
    fn load_separated_diff_statistics(
        &self,
        partitions: &FastHashSet<QueuePartitionIdx>,
        range: &QueueShardBoundedRange,
    ) -> Result<SeparatedStatisticsByPartitions>;
    /// Get partition router and statistics for the specified block
    fn get_router_and_statistics(
        &self,
        block_id_short: &BlockIdShort,
        diff_info: DiffInfo,
        partition: QueuePartitionIdx,
    ) -> Result<(PartitionRouter, DiffStatistics)>;
}

impl<V: InternalMessageValue> MessageQueueAdapterStdImpl<V> {
    pub fn new(queue: QueueImpl<QueueStateStdImpl, V>) -> Self {
        Self { queue }
    }
}

impl<V: InternalMessageValue> MessageQueueAdapter<V> for MessageQueueAdapterStdImpl<V> {
    #[instrument(skip_all, fields(%for_shard_id, partition, ?ranges))]
    fn create_iterator(
        &self,
        for_shard_id: ShardIdent,
        partition: QueuePartitionIdx,
        ranges: Vec<QueueShardBoundedRange>,
    ) -> Result<Box<dyn QueueIterator<V>>> {
        let histogram = HistogramGuard::begin("tycho_internal_queue_create_iterator_time");

        let ranges = ranges.into_iter().map(Into::into).collect::<Vec<_>>();

        metrics::counter!("tycho_collator_queue_adapter_iterators_count").increment(1);

        let state_iterator = self.queue.iterator(partition, &ranges, for_shard_id)?;
        let states_iterators_manager = StatesIteratorsManager::new(state_iterator);
        let iterator = QueueIteratorImpl::new(states_iterators_manager)?;

        let elapsed = histogram.finish();
        tracing::debug!(target: tracing_targets::MQ_ADAPTER,
            elapsed = %humantime::format_duration(elapsed),
            "create_iterator completed"
        );

        Ok(Box::new(iterator))
    }

    #[instrument(skip_all, fields(?partitions, ?ranges))]
    fn get_statistics(
        &self,
        partitions: &FastHashSet<QueuePartitionIdx>,
        ranges: &[QueueShardBoundedRange],
    ) -> Result<QueueStatistics> {
        let start_time = std::time::Instant::now();

        let mut result = AccountStatistics::default();

        let ranges: Vec<QueueShardRange> = ranges.iter().cloned().map(Into::into).collect();

        for range in &ranges {
            for partition in partitions {
                self.queue
                    .load_diff_statistics(*partition, range, &mut result)?;
            }
        }

        let stats = QueueStatistics::with_statistics(result);

        let elapsed = start_time.elapsed();
        tracing::debug!(target: tracing_targets::MQ_ADAPTER,
            elapsed = %humantime::format_duration(elapsed),
            "get_statistics completed"
        );

        Ok(stats)
    }

    #[instrument(skip_all, fields(%block_id_short, %diff_hash, ?check_sequence))]
    fn apply_diff(
        &self,
        diff: QueueDiffWithMessages<V>,
        block_id_short: BlockIdShort,
        diff_hash: &HashBytes,
        statistics: DiffStatistics,
        check_sequence: Option<DiffZone>,
    ) -> Result<()> {
        let start_time = std::time::Instant::now();

        tracing::debug!(target: tracing_targets::MQ_ADAPTER,
            "apply_diff started"
        );

        let new_messages_len = diff.messages.len();
        let min_message = diff.min_message().copied();
        let max_message = diff.max_message().copied();
        let processed_to = diff.processed_to.clone();

        self.queue
            .apply_diff(diff, block_id_short, diff_hash, statistics, check_sequence)?;

        let elapsed = start_time.elapsed();
        tracing::info!(target: tracing_targets::MQ_ADAPTER,
            new_messages_len,
            ?min_message, ?max_message,
            ?processed_to,
            elapsed = %humantime::format_duration(elapsed),
            "apply_diff completed"
        );

        Ok(())
    }

    #[instrument(skip_all, fields(?partitions))]
    fn commit_diff(
        &self,
        mc_top_blocks: Vec<(BlockId, bool)>,
        // TODO: get partitions from queue state
        partitions: &FastHashSet<QueuePartitionIdx>,
    ) -> Result<()> {
        let start_time = std::time::Instant::now();

        self.queue.commit_diff(&mc_top_blocks, partitions)?;

        let elapsed = start_time.elapsed();
        tracing::info!(target: tracing_targets::MQ_ADAPTER,
            mc_top_blocks = %DisplayIter(mc_top_blocks.iter().map(DisplayTupleRef)),
            elapsed = %humantime::format_duration(elapsed),
            "commit_diff completed"
        );

        Ok(())
    }

    #[instrument(skip_all)]
    fn clear_uncommitted_state(&self, top_shards: &[ShardIdent]) -> Result<()> {
        let start_time = std::time::Instant::now();

        tracing::debug!(
            target: tracing_targets::MQ_ADAPTER,
            "clear_uncommitted_state started"
        );

        // TODO: get partitions from queue state
        let partitions = FastHashSet::from_iter([QueuePartitionIdx(0), QueuePartitionIdx(1)]);

        self.queue
            .clear_uncommitted_state(&partitions, top_shards)?;

        let elapsed = start_time.elapsed();
        tracing::info!(target: tracing_targets::MQ_ADAPTER,
            ?partitions,
            elapsed = %humantime::format_duration(elapsed),
            "clear_uncommitted_state completed"
        );

        Ok(())
    }

    #[instrument(skip_all, fields(%shard_ident, seqno, ?zone))]
    fn get_diff_info(
        &self,
        shard_ident: &ShardIdent,
        seqno: u32,
        zone: DiffZone,
    ) -> Result<Option<DiffInfo>> {
        let start_time = std::time::Instant::now();
        let diff = self.queue.get_diff_info(shard_ident, seqno, zone)?;
        let elapsed = start_time.elapsed();
        tracing::debug!(target: tracing_targets::MQ_ADAPTER,
            elapsed = %humantime::format_duration(elapsed),
            "get_diff_info completed"
        );
        Ok(diff)
    }

    #[instrument(skip_all, fields(%block_id_short))]
    fn is_diff_exists(&self, block_id_short: &BlockIdShort) -> Result<bool> {
        let start_time = std::time::Instant::now();
        let exists = self.queue.is_diff_exists(block_id_short)?;
        let elapsed = start_time.elapsed();
        tracing::info!(target: tracing_targets::MQ_ADAPTER,
            elapsed = %humantime::format_duration(elapsed),
            exists,
            "is_diff_exists completed"
        );
        Ok(exists)
    }

    #[instrument(skip_all)]
    fn get_last_commited_mc_block_id(&self) -> Result<Option<BlockId>> {
        let start_time = std::time::Instant::now();
        let block_id = self.queue.get_last_committed_mc_block_id()?;
        let elapsed = start_time.elapsed();
        tracing::info!(target: tracing_targets::MQ_ADAPTER,
            elapsed = %humantime::format_duration(elapsed),
            block_id = ?DebugDisplayOpt(block_id),
            "get_last_commited_mc_block_id completed"
        );
        Ok(block_id)
    }

    #[instrument(skip_all, fields(%shard_ident, %from))]
    fn get_diffs_tail_len(&self, shard_ident: &ShardIdent, from: &QueueKey) -> u32 {
        let start_time = std::time::Instant::now();
        let from = from.next_value();
        let tail_len = self.queue.get_diffs_tail_len(shard_ident, &from);
        let elapsed = start_time.elapsed();
        tracing::info!(target: tracing_targets::MQ_ADAPTER,
            elapsed = %humantime::format_duration(elapsed),
            tail_len,
            "get_diffs_tail_len completed"
        );
        tail_len
    }

    #[instrument(skip_all, fields(?partitions, ?range))]
    fn load_separated_diff_statistics(
        &self,
        partitions: &FastHashSet<QueuePartitionIdx>,
        range: &QueueShardBoundedRange,
    ) -> Result<SeparatedStatisticsByPartitions> {
        let start_time = std::time::Instant::now();

        tracing::debug!(target: tracing_targets::MQ_ADAPTER,
            "load_separated_diff_statistics started"
        );

        let res = self
            .queue
            .load_separated_diff_statistics(partitions, &range.clone().into())?;

        let elapsed = start_time.elapsed();
        tracing::info!(target: tracing_targets::MQ_ADAPTER,
            stats_len = ?DebugIter(res.iter().map(|(par_id, map)| (*par_id, map.len()))),
            elapsed = %humantime::format_duration(elapsed),
            "load_separated_diff_statistics completed"
        );

        Ok(res)
    }

    #[instrument(skip_all, fields(%block_id_short, partition))]
    fn get_router_and_statistics(
        &self,
        block_id_short: &BlockIdShort,
        diff_info: DiffInfo,
        partition: QueuePartitionIdx,
    ) -> Result<(PartitionRouter, DiffStatistics)> {
        let start_time = std::time::Instant::now();

        let partition_router = PartitionRouter::with_partitions(
            &diff_info.router_partitions_src,
            &diff_info.router_partitions_dst,
        );

        let statistics_range = QueueShardRange {
            shard_ident: block_id_short.shard,
            from: diff_info.min_message,
            to: diff_info.max_message.next_value(),
        };

        let mut statistics = AccountStatistics::default();

        self.queue
            .load_diff_statistics(partition, &statistics_range, &mut statistics)?;

        let queue_statistics = QueueStatistics::with_statistics(statistics.clone());

        let mut diff_statistics = FastHashMap::default();
        diff_statistics.insert(partition, queue_statistics.statistics().clone());

        let diff_statistics = DiffStatistics::new(
            block_id_short.shard,
            diff_info.min_message,
            diff_info.max_message,
            diff_statistics,
            queue_statistics.shard_messages_count(),
        );

        let elapsed = start_time.elapsed();

        tracing::debug!(target: tracing_targets::MQ_ADAPTER,
            elapsed = %humantime::format_duration(elapsed),
            "get_router_and_statistics completed"
        );

        Ok((partition_router, diff_statistics))
    }
}
