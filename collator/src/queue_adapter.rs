use anyhow::{anyhow, Result};
use everscale_types::cell::HashBytes;
use everscale_types::models::{BlockId, BlockIdShort, ShardIdent};
use tracing::instrument;
use tycho_block_util::queue::{QueueKey, QueuePartitionIdx};
use tycho_storage::model::DiffInfo;
use tycho_util::metrics::HistogramGuard;
use tycho_util::{FastHashMap, FastHashSet};

use crate::internal_queue::iterator::{QueueIterator, QueueIteratorImpl};
use crate::internal_queue::queue::{Queue, QueueImpl};
use crate::internal_queue::state::states_iterators_manager::StatesIteratorsManager;
use crate::internal_queue::state::storage::QueueStateStdImpl;
use crate::internal_queue::types::{
    DiffStatistics, DiffZone, InternalMessageValue, PartitionRouter, QueueDiffWithMessages,
    QueueShardRange, QueueStatistics,
};
use crate::tracing_targets;
use crate::types::{DisplayIter, DisplayTupleRef};

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
        ranges: &[QueueShardRange],
    ) -> Result<Box<dyn QueueIterator<V>>>;

    /// Returns statistics for the specified ranges by partition
    /// and source shards (equal to iterator ranges)
    fn get_statistics(
        &self,
        partition: QueuePartitionIdx,
        ranges: &[QueueShardRange],
    ) -> Result<QueueStatistics>;

    /// Apply diff to the current queue uncommitted state (waiting for the operation to complete)
    fn apply_diff(
        &self,
        diff: QueueDiffWithMessages<V>,
        block_id_short: BlockIdShort,
        diff_hash: &HashBytes,
        statistics: DiffStatistics,
        skip_check_sequence: bool,
    ) -> Result<()>;

    /// Commit previously applied diff, saving changes to committed state (waiting for the operation to complete).
    /// Return `None` if specified diff does not exist.
    fn commit_diff(
        &self,
        mc_top_blocks: Vec<(BlockId, bool)>,
        partitions: &FastHashSet<QueuePartitionIdx>,
    ) -> Result<()>;

    /// Add new messages to the iterator
    fn add_message_to_iterator(
        &self,
        iterator: &mut dyn QueueIterator<V>,
        message: V,
    ) -> Result<()>;
    /// Commit processed messages in the iterator
    /// Save last message position for each shard
    fn commit_messages_to_iterator(
        &self,
        iterator: &mut dyn QueueIterator<V>,
        messages: &[(ShardIdent, QueueKey)],
    ) -> Result<()>;

    fn clear_uncommitted_state(&self, partitions: &FastHashSet<QueuePartitionIdx>) -> Result<()>;
    /// Get diff for the given block from committed and uncommitted state
    fn get_diff(
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
    fn get_diffs_tail_len(&self, shard_ident: &ShardIdent, from: &QueueKey) -> u32;
    /// Get partition router and statistics for the specified block
    fn get_router_and_statistics(
        &self,
        block_id_short: &BlockIdShort,
        partition: QueuePartitionIdx,
    ) -> Result<(PartitionRouter, DiffStatistics)>;
}

impl<V: InternalMessageValue> MessageQueueAdapterStdImpl<V> {
    pub fn new(queue: QueueImpl<QueueStateStdImpl, V>) -> Self {
        Self { queue }
    }
}

impl<V: InternalMessageValue> MessageQueueAdapter<V> for MessageQueueAdapterStdImpl<V> {
    #[instrument(skip_all, fields(%for_shard_id, partition, ranges = ?ranges))]
    fn create_iterator(
        &self,
        for_shard_id: ShardIdent,
        partition: QueuePartitionIdx,
        ranges: &[QueueShardRange],
    ) -> Result<Box<dyn QueueIterator<V>>> {
        let _histogram = HistogramGuard::begin("tycho_internal_queue_create_iterator_time");

        let start_time = std::time::Instant::now();

        metrics::counter!("tycho_collator_queue_adapter_iterators_count").increment(1);

        let state_iterator = self.queue.iterator(partition, ranges, for_shard_id)?;
        let states_iterators_manager = StatesIteratorsManager::new(state_iterator);
        let iterator = QueueIteratorImpl::new(states_iterators_manager, for_shard_id)?;

        let elapsed = start_time.elapsed();
        tracing::info!(
            target: tracing_targets::MQ_ADAPTER,
            elapsed = %humantime::format_duration(elapsed),
            "create_iterator completed"
        );
        Ok(Box::new(iterator))
    }

    #[instrument(skip_all, fields(partition, ranges = ?ranges))]
    fn get_statistics(
        &self,
        partition: QueuePartitionIdx,
        ranges: &[QueueShardRange],
    ) -> Result<QueueStatistics> {
        let start_time = std::time::Instant::now();

        let stats = self.queue.load_statistics(partition, ranges)?;

        let elapsed = start_time.elapsed();
        tracing::info!(
            target: tracing_targets::MQ_ADAPTER,
            elapsed = %humantime::format_duration(elapsed),
            "get_statistics completed"
        );

        Ok(stats)
    }

    #[instrument(skip_all, fields(%block_id_short, %diff_hash, skip_check_sequence))]
    fn apply_diff(
        &self,
        diff: QueueDiffWithMessages<V>,
        block_id_short: BlockIdShort,
        diff_hash: &HashBytes,
        statistics: DiffStatistics,
        skip_check_sequence: bool,
    ) -> Result<()> {
        let start_time = std::time::Instant::now();
        let len = diff.messages.len();
        let processed_to = diff.processed_to.clone();
        self.queue.apply_diff(
            diff,
            block_id_short,
            diff_hash,
            statistics,
            skip_check_sequence,
        )?;

        let elapsed = start_time.elapsed();
        tracing::info!(
            target: tracing_targets::MQ_ADAPTER,
            new_messages_len = len,
            elapsed = %humantime::format_duration(elapsed),
            processed_to = ?processed_to,
            "apply_diff completed"
        );
        Ok(())
    }

    #[instrument(skip_all, fields(?partitions))]
    fn commit_diff(
        &self,
        mc_top_blocks: Vec<(BlockId, bool)>,
        partitions: &FastHashSet<QueuePartitionIdx>,
    ) -> Result<()> {
        let start_time = std::time::Instant::now();

        self.queue.commit_diff(&mc_top_blocks, partitions)?;

        let elapsed = start_time.elapsed();
        tracing::info!(
            target: tracing_targets::MQ_ADAPTER,
            mc_top_blocks = %DisplayIter(mc_top_blocks.iter().map(DisplayTupleRef)),
            elapsed = %humantime::format_duration(elapsed),
            "commit_diff completed"
        );
        Ok(())
    }

    fn add_message_to_iterator(
        &self,
        iterator: &mut dyn QueueIterator<V>,
        message: V,
    ) -> Result<()> {
        let start_time = std::time::Instant::now();
        iterator.add_message(message)?;
        let elapsed = start_time.elapsed();
        tracing::info!(
            target: tracing_targets::MQ_ADAPTER,
            elapsed = %humantime::format_duration(elapsed),
            "add_message_to_iterator completed"
        );
        Ok(())
    }

    fn commit_messages_to_iterator(
        &self,
        iterator: &mut dyn QueueIterator<V>,
        messages: &[(ShardIdent, QueueKey)],
    ) -> Result<()> {
        let start_time = std::time::Instant::now();
        iterator.commit(messages)?;
        let elapsed = start_time.elapsed();

        tracing::info!(
            target: tracing_targets::MQ_ADAPTER,
            elapsed = %humantime::format_duration(elapsed),
            "commit_messages_to_iterator completed"
        );
        Ok(())
    }

    #[instrument(skip_all, fields(?partitions))]
    fn clear_uncommitted_state(&self, partitions: &FastHashSet<QueuePartitionIdx>) -> Result<()> {
        if partitions.is_empty() {
            tracing::warn!(target: tracing_targets::MQ_ADAPTER, "clear_uncommitted_state: partitions is empty");
        }

        let start_time = std::time::Instant::now();
        self.queue.clear_uncommitted_state(partitions)?;
        let elapsed = start_time.elapsed();

        tracing::info!(
            target: tracing_targets::MQ_ADAPTER,
            elapsed = %humantime::format_duration(elapsed),
            "clear_uncommitted_state completed"
        );
        Ok(())
    }

    #[instrument(skip_all, fields(%shard_ident, seqno))]
    fn get_diff(
        &self,
        shard_ident: &ShardIdent,
        seqno: u32,
        zone: DiffZone,
    ) -> Result<Option<DiffInfo>> {
        let start_time = std::time::Instant::now();
        let diff = self.queue.get_diff(shard_ident, seqno, zone)?;
        let elapsed = start_time.elapsed();
        tracing::info!(
            target: tracing_targets::MQ_ADAPTER,
            elapsed = %humantime::format_duration(elapsed),
            "get_diff completed"
        );
        Ok(diff)
    }

    #[instrument(skip_all, fields(%block_id_short))]
    fn is_diff_exists(&self, block_id_short: &BlockIdShort) -> Result<bool> {
        let start_time = std::time::Instant::now();
        let exists = self.queue.is_diff_exists(block_id_short)?;
        let elapsed = start_time.elapsed();
        tracing::info!(
            target: tracing_targets::MQ_ADAPTER,
            elapsed = %humantime::format_duration(elapsed),
            exists,
            "is_diff_exists completed"
        );
        Ok(exists)
    }

    fn get_last_commited_mc_block_id(&self) -> Result<Option<BlockId>> {
        let start_time = std::time::Instant::now();
        let block_id = self.queue.get_last_committed_mc_block_id()?;
        let elapsed = start_time.elapsed();
        tracing::info!(
            target: tracing_targets::MQ_ADAPTER,
            elapsed = %humantime::format_duration(elapsed),
            "get_last_commited_mc_block_id completed"
        );
        Ok(block_id)
    }

    fn get_diffs_tail_len(&self, shard_ident: &ShardIdent, from: &QueueKey) -> u32 {
        let start_time = std::time::Instant::now();
        let tail_len = self.queue.get_diffs_tail_len(shard_ident, from);
        let elapsed = start_time.elapsed();
        tracing::info!(
            target: tracing_targets::MQ_ADAPTER,
            elapsed = %humantime::format_duration(elapsed),
            tail_len,
            "get_diffs_tail_len completed"
        );
        tail_len
    }

    fn get_router_and_statistics(
        &self,
        block_id_short: &BlockIdShort,
        partition: QueuePartitionIdx,
    ) -> Result<(PartitionRouter, DiffStatistics)> {
        let start_time = std::time::Instant::now();
        let diff = self
            .get_diff(&block_id_short.shard, block_id_short.seqno, DiffZone::Both)?
            .ok_or_else(|| anyhow!("Diff for block {} not found", block_id_short))?;

        let partition_router = PartitionRouter::with_partitions(
            &diff.router_partitions_src,
            &diff.router_partitions_dst,
        );

        let statistics_range = QueueShardRange {
            shard_ident: block_id_short.shard,
            from: diff.min_message,
            to: diff.max_message,
        };

        let queue_statistics = self.get_statistics(partition, &[statistics_range])?;

        let mut diff_statistics = FastHashMap::default();
        diff_statistics.insert(partition, queue_statistics.statistics().clone());

        let diff_statistics = DiffStatistics::new(
            block_id_short.shard,
            diff.min_message,
            diff.max_message,
            diff_statistics,
            queue_statistics.shard_messages_count(),
        );

        let elapsed = start_time.elapsed();

        tracing::info!(
            target: tracing_targets::MQ_ADAPTER,
            elapsed = %humantime::format_duration(elapsed),
            "get_router_and_statistics completed"
        );

        Ok((partition_router, diff_statistics))
    }
}
