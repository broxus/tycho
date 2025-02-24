use anyhow::Result;
use everscale_types::cell::HashBytes;
use everscale_types::models::{BlockIdShort, ShardIdent};
use tracing::instrument;
use tycho_block_util::queue::{QueueKey, QueuePartitionIdx};
use tycho_util::metrics::HistogramGuard;

use crate::internal_queue::iterator::{QueueIterator, QueueIteratorImpl};
use crate::internal_queue::queue::{Queue, QueueImpl, ShortQueueDiff};
use crate::internal_queue::state::commited_state::CommittedStateStdImpl;
use crate::internal_queue::state::states_iterators_manager::StatesIteratorsManager;
use crate::internal_queue::state::uncommitted_state::UncommittedStateStdImpl;
use crate::internal_queue::types::{
    DiffStatistics, InternalMessageValue, QueueDiffWithMessages, QueueShardRange, QueueStatistics,
};
use crate::tracing_targets;
use crate::types::{DisplayIter, DisplayTupleRef};

pub struct MessageQueueAdapterStdImpl<V: InternalMessageValue> {
    queue: QueueImpl<UncommittedStateStdImpl, CommittedStateStdImpl, V>,
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
        max_message: QueueKey,
    ) -> Result<()>;

    /// Commit previously applied diff, saving changes to committed state (waiting for the operation to complete).
    /// Return `None` if specified diff does not exist.
    fn commit_diff(&self, mc_top_blocks: Vec<(BlockIdShort, bool)>) -> Result<()>;

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

    fn clear_uncommitted_state(&self) -> Result<()>;
    /// Removes all diffs from the cache that are less than `inclusive_until` which source shard is `source_shard`
    fn trim_diffs(&self, source_shard: &ShardIdent, inclusive_until: &QueueKey) -> Result<()>;
    /// Get diff for the given block from committed and uncommitted state
    fn get_diff(&self, shard_ident: &ShardIdent, seqno: u32) -> Option<ShortQueueDiff>;
    /// Returns the number of diffs in cache for the given shard
    fn get_diffs_count_by_shard(&self, shard_ident: &ShardIdent) -> usize;
    /// Check if diff exists in the cache
    fn is_diff_exists(&self, block_id_short: &BlockIdShort) -> bool;
}

impl<V: InternalMessageValue> MessageQueueAdapterStdImpl<V> {
    pub fn new(queue: QueueImpl<UncommittedStateStdImpl, CommittedStateStdImpl, V>) -> Self {
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

        metrics::counter!("tycho_collator_queue_adapter_iterators_count").increment(1);

        let time_start = std::time::Instant::now();

        let states_iterators = self.queue.iterator(partition, ranges, for_shard_id)?;
        let states_iterators_manager = StatesIteratorsManager::new(states_iterators);
        let iterator = QueueIteratorImpl::new(states_iterators_manager, for_shard_id)?;

        tracing::info!(
            target: tracing_targets::MQ_ADAPTER,
            elapsed = %humantime::format_duration(time_start.elapsed()),
            "Iterator created"
        );
        Ok(Box::new(iterator))
    }

    #[instrument(skip_all, fields(partition, ranges = ?ranges))]
    fn get_statistics(
        &self,
        partition: QueuePartitionIdx,
        ranges: &[QueueShardRange],
    ) -> Result<QueueStatistics> {
        let time_start = std::time::Instant::now();

        let stats = self.queue.load_statistics(partition, ranges)?;

        tracing::info!(
            target: tracing_targets::MQ_ADAPTER,
            elapsed = %humantime::format_duration(time_start.elapsed()),
            "Loaded statistics"
        );

        Ok(stats)
    }

    #[instrument(skip_all, fields(%block_id_short, %max_message, %diff_hash))]
    fn apply_diff(
        &self,
        diff: QueueDiffWithMessages<V>,
        block_id_short: BlockIdShort,
        diff_hash: &HashBytes,
        statistics: DiffStatistics,
        max_message: QueueKey,
    ) -> Result<()> {
        let time = std::time::Instant::now();
        let len = diff.messages.len();
        let processed_to = diff.processed_to.clone();
        self.queue
            .apply_diff(diff, block_id_short, diff_hash, statistics, max_message)?;

        tracing::info!(target: tracing_targets::MQ_ADAPTER,
            new_messages_len = len,
            elapsed = ?time.elapsed(),
            processed_to = ?processed_to,
            "Diff applied",
        );
        Ok(())
    }

    fn commit_diff(&self, mc_top_blocks: Vec<(BlockIdShort, bool)>) -> Result<()> {
        let time = std::time::Instant::now();

        self.queue.commit_diff(&mc_top_blocks)?;

        tracing::info!(target: tracing_targets::MQ_ADAPTER,
            mc_top_blocks = %DisplayIter(mc_top_blocks.iter().map(DisplayTupleRef)),
            elapsed = ?time.elapsed(),
            "Diff committed",
        );

        Ok(())
    }

    fn add_message_to_iterator(
        &self,
        iterator: &mut dyn QueueIterator<V>,
        message: V,
    ) -> Result<()> {
        iterator.add_message(message)?;
        Ok(())
    }

    fn commit_messages_to_iterator(
        &self,
        iterator: &mut dyn QueueIterator<V>,
        messages: &[(ShardIdent, QueueKey)],
    ) -> Result<()> {
        tracing::info!(
            target: tracing_targets::MQ_ADAPTER,
            messages_len = messages.len(),
            "Committing messages to iterator"
        );
        iterator.commit(messages)
    }

    fn clear_uncommitted_state(&self) -> Result<()> {
        tracing::info!(target: tracing_targets::MQ_ADAPTER, "Clearing uncommitted state");
        self.queue.clear_uncommitted_state()
    }

    fn trim_diffs(&self, source_shard: &ShardIdent, inclusive_until: &QueueKey) -> Result<()> {
        tracing::info!(
            target: tracing_targets::MQ_ADAPTER,
            source_shard = ?source_shard,
            inclusive_until = ?inclusive_until,
            "Trimming diffs"
        );
        self.queue.trim_diffs(source_shard, inclusive_until)
    }

    fn get_diff(&self, shard_ident: &ShardIdent, seqno: u32) -> Option<ShortQueueDiff> {
        self.queue.get_diff(shard_ident, seqno)
    }

    fn get_diffs_count_by_shard(&self, shard_ident: &ShardIdent) -> usize {
        self.queue.get_diffs_count_by_shard(shard_ident)
    }

    fn is_diff_exists(&self, block_id_short: &BlockIdShort) -> bool {
        self.queue.is_diff_exists(block_id_short)
    }
}
