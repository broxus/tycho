use std::time::Duration;

use anyhow::Result;
use everscale_types::cell::HashBytes;
use everscale_types::models::{BlockId, BlockIdShort, ShardIdent};
use tracing::instrument;
use tycho_block_util::queue::{QueueKey, QueuePartitionIdx};
use tycho_storage::model::DiffInfo;
use tycho_util::metrics::HistogramGuard;
use tycho_util::FastHashSet;

use crate::internal_queue::iterator::{QueueIterator, QueueIteratorImpl};
use crate::internal_queue::queue::{Queue, QueueImpl};
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

    fn clear_uncommitted_state(&self) -> Result<()>;
    /// Get diff for the given block from committed and uncommitted state
    fn get_diff(&self, shard_ident: &ShardIdent, seqno: u32) -> Result<Option<DiffInfo>>;
    /// Check if diff exists in the cache
    fn is_diff_exists(&self, block_id_short: &BlockIdShort) -> Result<bool>;
    /// Get last applied mc block id from committed state
    fn get_last_applied_mc_block_id(&self) -> Result<Option<BlockId>>;
    /// Get diffs tail len from uncommitted state and committed state
    fn get_diffs_tail_len(&self, shard_ident: &ShardIdent, max_message_from: &QueueKey) -> u32;
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
        let start_time = std::time::Instant::now();

        metrics::counter!("tycho_collator_queue_adapter_iterators_count").increment(1);

        let states_iterators = self.queue.iterator(partition, ranges, for_shard_id)?;
        let states_iterators_manager = StatesIteratorsManager::new(states_iterators);
        let iterator = QueueIteratorImpl::new(states_iterators_manager, for_shard_id)?;

        let elapsed = start_time.elapsed();
        if elapsed > Duration::from_millis(1) {
            tracing::info!(
                target: "local_debug",
                elapsed = %humantime::format_duration(elapsed),
                "create_iterator completed"
            );
        }
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
        if elapsed > Duration::from_millis(1) {
            tracing::info!(
                target: "local_debug",
                elapsed = %humantime::format_duration(elapsed),
                "get_statistics completed"
            );
        }

        Ok(stats)
    }

    #[instrument(skip_all, fields(%block_id_short, %diff_hash))]
    fn apply_diff(
        &self,
        diff: QueueDiffWithMessages<V>,
        block_id_short: BlockIdShort,
        diff_hash: &HashBytes,
        statistics: DiffStatistics,
    ) -> Result<()> {
        let start_time = std::time::Instant::now();
        let len = diff.messages.len();
        let processed_to = diff.processed_to.clone();
        self.queue
            .apply_diff(diff, block_id_short, diff_hash, statistics)?;

        let elapsed = start_time.elapsed();
        if elapsed > Duration::from_millis(1) {
            tracing::info!(
                target: "local_debug",
                new_messages_len = len,
                elapsed = %humantime::format_duration(elapsed),
                processed_to = ?processed_to,
                "apply_diff completed"
            );
        }
        Ok(())
    }

    fn commit_diff(
        &self,
        mc_top_blocks: Vec<(BlockId, bool)>,
        partitions: &FastHashSet<QueuePartitionIdx>,
    ) -> Result<()> {
        let start_time = std::time::Instant::now();

        self.queue.commit_diff(&mc_top_blocks, partitions)?;

        let elapsed = start_time.elapsed();
        if elapsed > Duration::from_millis(1) {
            tracing::info!(
                target: "local_debug",
                mc_top_blocks = %DisplayIter(mc_top_blocks.iter().map(DisplayTupleRef)),
                elapsed = %humantime::format_duration(elapsed),
                partitions = ?partitions,
                "commit_diff completed"
            );
        }
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
        if elapsed > Duration::from_millis(1) {
            tracing::info!(
                target: "local_debug",
                elapsed = %humantime::format_duration(elapsed),
                "add_message_to_iterator completed"
            );
        }
        Ok(())
    }

    fn commit_messages_to_iterator(
        &self,
        iterator: &mut dyn QueueIterator<V>,
        messages: &[(ShardIdent, QueueKey)],
    ) -> Result<()> {
        let start_time = std::time::Instant::now();
        tracing::info!(
            target: "local_debug",
            messages_len = messages.len(),
            "commit_messages_to_iterator started"
        );
        iterator.commit(messages)?;
        let elapsed = start_time.elapsed();
        if elapsed > Duration::from_millis(1) {
            tracing::info!(
                target: "local_debug",
                elapsed = %humantime::format_duration(elapsed),
                "commit_messages_to_iterator completed"
            );
        }
        Ok(())
    }

    fn clear_uncommitted_state(&self) -> Result<()> {
        let start_time = std::time::Instant::now();
        tracing::info!(target: "local_debug", "clear_uncommitted_state started");
        self.queue.clear_uncommitted_state()?;
        let elapsed = start_time.elapsed();
        if elapsed > Duration::from_millis(1) {
            tracing::info!(
                target: "local_debug",
                elapsed = %humantime::format_duration(elapsed),
                "clear_uncommitted_state completed"
            );
        }
        Ok(())
    }

    fn get_diff(&self, shard_ident: &ShardIdent, seqno: u32) -> Result<Option<DiffInfo>> {
        let start_time = std::time::Instant::now();
        let diff = self.queue.get_diff(shard_ident, seqno)?;
        let elapsed = start_time.elapsed();
        if elapsed > Duration::from_millis(1) {
            tracing::info!(
                target: "local_debug",
                elapsed = %humantime::format_duration(elapsed),
                "get_diff completed"
            );
        }
        Ok(diff)
    }

    fn is_diff_exists(&self, block_id_short: &BlockIdShort) -> Result<bool> {
        let start_time = std::time::Instant::now();
        let exists = self.queue.is_diff_exists(block_id_short)?;
        let elapsed = start_time.elapsed();
        if elapsed > Duration::from_millis(1) {
            tracing::info!(
                target: "local_debug",
                elapsed = %humantime::format_duration(elapsed),
                "is_diff_exists completed"
            );
        }
        Ok(exists)
    }

    fn get_last_applied_mc_block_id(&self) -> Result<Option<BlockId>> {
        let start_time = std::time::Instant::now();
        let block_id = self.queue.get_last_applied_mc_block_id()?;
        let elapsed = start_time.elapsed();
        if elapsed > Duration::from_millis(1) {
            tracing::info!(
                target: "local_debug",
                elapsed = %humantime::format_duration(elapsed),
                "get_last_applied_mc_block_id completed"
            );
        }
        Ok(block_id)
    }

    fn get_diffs_tail_len(&self, shard_ident: &ShardIdent, max_message_from: &QueueKey) -> u32 {
        let start_time = std::time::Instant::now();
        let tail_len = self.queue.get_diffs_tail_len(shard_ident, max_message_from);
        let elapsed = start_time.elapsed();
        if elapsed > Duration::from_millis(1) {
            tracing::info!(
                target: "local_debug",
                elapsed = %humantime::format_duration(elapsed),
                "get_diffs_tail_len completed"
            );
        }
        tail_len
    }
}
