use anyhow::{Result, bail};
use async_trait::async_trait;
use tracing::instrument;
use tycho_block_util::queue::{QueueKey, QueuePartitionIdx};
use tycho_block_util::state::ShardStateStuff;
use tycho_types::cell::HashBytes;
use tycho_types::models::{BlockId, BlockIdShort, ShardIdent};
use tycho_util::metrics::HistogramGuard;
use tycho_util::{FastHashMap, FastHashSet};

use crate::internal_queue::iterator::{QueueIterator, QueueIteratorImpl};
use crate::internal_queue::queue::{PendingQueueDiff, Queue, QueueImpl};
use crate::internal_queue::state::states_iterators_manager::StatesIteratorsManager;
use crate::internal_queue::state::storage::QueueStateStdImpl;
use crate::internal_queue::types::diff::{DiffZone, QueueDiffWithMessages};
use crate::internal_queue::types::message::InternalMessageValue;
use crate::internal_queue::types::ranges::{QueueShardBoundedRange, QueueShardRange};
use crate::internal_queue::types::router::PartitionRouter;
use crate::internal_queue::types::stats::{
    DiffStatistics, QueueStatistics, SeparatedStatisticsByPartitions,
};
use crate::state_node::DiffLoader;
use crate::storage::models::DiffInfo;
use crate::storage::snapshot::AccountStatistics;
use crate::tracing_targets;
use crate::types::{
    DebugDisplayOpt, DebugIter, ShardDescriptionShortExt, TopBlockId, TopBlockIdUpdated,
};

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

    /// Prepare diff for applying to queue. Returns transaction that should be committed later.
    /// Returns None if diff is already applied (duplicate).
    fn prepare_diff(
        &self,
        diff: QueueDiffWithMessages<V>,
        block_id_short: BlockIdShort,
        diff_hash: &HashBytes,
        statistics: DiffStatistics,
        check_sequence: Option<DiffZone>,
    ) -> Result<Option<PendingQueueDiff>>;

    /// Apply diff by storing it to the queue uncommitted zone (waiting for the operation to complete)
    fn apply_diff(
        &self,
        diff: QueueDiffWithMessages<V>,
        block_id_short: BlockIdShort,
        diff_hash: &HashBytes,
        statistics: DiffStatistics,
        check_sequence: Option<DiffZone>,
    ) -> Result<()>;

    /// Commit previously applied diffs, updating commit pointers (waiting for the operation to complete)
    fn commit_diff(
        &self,
        mc_top_blocks: Vec<TopBlockIdUpdated>,
        partitions: &FastHashSet<QueuePartitionIdx>,
    ) -> Result<()>;

    fn clear_uncommitted_state(&self, top_shards: &[ShardIdent]) -> Result<()>;

    /// Get diff for the given block from committed and/or uncommitted zone
    fn get_diff_info(
        &self,
        shard_ident: &ShardIdent,
        seqno: u32,
        zone: DiffZone,
    ) -> Result<Option<DiffInfo>>;

    /// Check if diff exists in state
    fn is_diff_exists(&self, block_id_short: &BlockIdShort) -> Result<bool>;

    /// Get mc block id on which the queue was committed.
    /// Returns None if queue was not committed
    fn get_last_committed_mc_block_id(&self) -> Result<Option<BlockId>>;

    /// Get diffs tail len.
    /// `from` - start key for the tail. Diff with `max_message` == `from` will be excluded from the tail
    fn get_diffs_tail_len(&self, shard_ident: &ShardIdent, from: &QueueKey) -> u32;

    /// Load separated diff statistics for the specified partitions and range.
    /// `range.from` = diff with `max_message == range.from` will be excluded in statistics;
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
    fn prepare_diff(
        &self,
        diff: QueueDiffWithMessages<V>,
        block_id_short: BlockIdShort,
        diff_hash: &HashBytes,
        statistics: DiffStatistics,
        check_sequence: Option<DiffZone>,
    ) -> Result<Option<PendingQueueDiff>> {
        let start_time = std::time::Instant::now();

        tracing::debug!(target: tracing_targets::MQ_ADAPTER,
            "prepare_diff started"
        );

        let new_messages_len = diff.messages.len();
        let min_message = diff.min_message().copied();
        let max_message = diff.max_message().copied();
        let processed_to = diff.processed_to.clone();

        let tx =
            self.queue
                .prepare_diff(diff, block_id_short, diff_hash, statistics, check_sequence)?;

        let elapsed = start_time.elapsed();
        tracing::info!(target: tracing_targets::MQ_ADAPTER,
            new_messages_len,
            ?min_message, ?max_message,
            ?processed_to,
            elapsed = %humantime::format_duration(elapsed),
            "prepare_diff completed"
        );

        Ok(tx)
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
        mc_top_blocks: Vec<TopBlockIdUpdated>,
        // TODO: get partitions from queue state
        partitions: &FastHashSet<QueuePartitionIdx>,
    ) -> Result<()> {
        let start_time = std::time::Instant::now();

        self.queue.commit_diff(&mc_top_blocks, partitions)?;

        let elapsed = start_time.elapsed();
        tracing::info!(target: tracing_targets::MQ_ADAPTER,
            mc_top_blocks = ?mc_top_blocks,
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
        tracing::debug!(target: tracing_targets::MQ_ADAPTER,
            elapsed = %humantime::format_duration(elapsed),
            exists,
            "is_diff_exists completed"
        );
        Ok(exists)
    }

    #[instrument(skip_all)]
    fn get_last_committed_mc_block_id(&self) -> Result<Option<BlockId>> {
        let start_time = std::time::Instant::now();
        let block_id = self.queue.get_last_committed_mc_block_id()?;
        let elapsed = start_time.elapsed();
        tracing::info!(target: tracing_targets::MQ_ADAPTER,
            elapsed = %humantime::format_duration(elapsed),
            block_id = ?DebugDisplayOpt(&block_id),
            "get_last_committed_mc_block_id completed"
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

        let mut diff_statistics = FastHashMap::default();
        diff_statistics.insert(partition, statistics);

        let diff_statistics = DiffStatistics::new(
            block_id_short.shard,
            diff_info.min_message,
            diff_info.max_message,
            diff_statistics,
        );

        let elapsed = start_time.elapsed();

        tracing::debug!(target: tracing_targets::MQ_ADAPTER,
            elapsed = %humantime::format_duration(elapsed),
            "get_router_and_statistics completed"
        );

        Ok((partition_router, diff_statistics))
    }
}

#[async_trait]
pub trait MessageQueueAdapterAsync<V>: MessageQueueAdapter<V>
where
    V: InternalMessageValue + Send + Sync,
{
    /// Recovers message queue state after restart.
    async fn recover_after_restart(
        &self,
        mc_state: &ShardStateStuff,
        diff_loader: DiffLoader<'_>,
    ) -> Result<()>;
}

#[async_trait]
impl<V: InternalMessageValue> MessageQueueAdapterAsync<V> for MessageQueueAdapterStdImpl<V> {
    async fn recover_after_restart(
        &self,
        mc_state: &ShardStateStuff,
        diff_loader: DiffLoader<'_>,
    ) -> Result<()> {
        let mc_block_id = mc_state.block_id();
        assert!(
            mc_block_id.is_masterchain(),
            "latest masterchain block id and state must be provided"
        );

        let mut top_shards_for_queue_clean = mc_state.get_top_shards()?;

        // rollback commit pointers if committed queue is ahead of last applied mc state
        if let Some(last_committed_mc_block_id) = self.get_last_committed_mc_block_id()? {
            if last_committed_mc_block_id.seqno > mc_block_id.seqno {
                // collect top blocks info
                let top_shard_blocks_info = mc_state
                    .shards()?
                    .iter()
                    .map(|item| {
                        let (shard_id, shard_descr) = item?;
                        Ok(TopBlockIdUpdated {
                            block: TopBlockId {
                                ref_by_mc_seqno: shard_descr.reg_mc_seqno,
                                block_id: shard_descr.get_block_id(shard_id),
                            },
                            updated: shard_descr.top_sc_block_updated,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let removed_commit_pointer_shards = self
                    .rollback_commit_pointers(mc_block_id, top_shard_blocks_info, diff_loader)
                    .await?;
                for shard in removed_commit_pointer_shards {
                    if !top_shards_for_queue_clean.contains(&shard) {
                        top_shards_for_queue_clean.push(shard);
                    }
                }
            } else {
                anyhow::ensure!(
                    last_committed_mc_block_id.seqno != mc_block_id.seqno
                        || last_committed_mc_block_id == *mc_block_id,
                    "internal messages queue is committed on different masterchain block: \
                    queue={last_committed_mc_block_id}, applied={mc_block_id}",
                );
            }
        }

        // We should clear uncommitted queue state because it may contain incorrect diffs
        // that were created before node restart. We will restore queue strictly above last committed state
        self.clear_uncommitted_state(&top_shards_for_queue_clean)
    }
}

impl<V: InternalMessageValue> MessageQueueAdapterStdImpl<V> {
    #[instrument(skip_all, fields(%to_mc_block_id))]
    async fn rollback_commit_pointers(
        &self,
        to_mc_block_id: &BlockId,
        to_top_shard_blocks: Vec<TopBlockIdUpdated>,
        diff_loader: DiffLoader<'_>,
    ) -> Result<Vec<ShardIdent>> {
        let start_time = std::time::Instant::now();

        let zerostate_id = self.queue.zerostate_id();

        let mut to_top_blocks = to_top_shard_blocks;
        to_top_blocks.push(TopBlockIdUpdated {
            block: crate::types::TopBlockId {
                ref_by_mc_seqno: to_mc_block_id.seqno,
                block_id: *to_mc_block_id,
            },
            updated: true,
        });

        let mut clear_commit_state = false;

        // build commit pointers for rollback
        let mut to_commit_pointers = FastHashMap::default();
        for item in &to_top_blocks {
            let block_id = &item.block.block_id;

            // try get diff max message
            let diff_max_message = if let Some(diff) =
                self.get_diff_info(&block_id.shard, block_id.seqno, DiffZone::Both)?
            {
                Some(diff.max_message)
            } else {
                diff_loader
                    .load_diff(block_id)
                    .await?
                    .map(|d| d.diff().max_message)
            };

            // SAFETY: Queue is empty at master zerostate, while zerostate shard
            // entries only remove their own commit pointers.
            if diff_max_message.is_none() && item.block.ref_by_mc_seqno == zerostate_id.seqno {
                if !block_id.is_masterchain() {
                    tracing::warn!(
                        target: tracing_targets::MQ,
                        "Dropping commit pointer after missing zerostate shard diff for {} \
                        with mc ref {} (zerostate {}) during rollback",
                        block_id.as_short_id(),
                        item.block.ref_by_mc_seqno,
                        zerostate_id.seqno,
                    );
                    continue;
                }
                tracing::warn!(
                    target: tracing_targets::MQ,
                    "Clearing all commit pointers after missing diff for {} \
                    with mc ref {} (zerostate {}) during rollback",
                    block_id.as_short_id(),
                    item.block.ref_by_mc_seqno,
                    zerostate_id.seqno,
                );
                clear_commit_state = true;
                break;
            }

            if to_commit_pointers
                .insert(block_id.shard, (diff_max_message, block_id.seqno))
                .is_some()
            {
                bail!(
                    "Duplicate shard in rollback_commit_pointers: {}",
                    block_id.shard
                );
            }
        }

        // fully clear commit state when unable to rollback
        if clear_commit_state {
            let removed_commit_pointer_shards = self.queue.clear_commit_pointers()?;
            tracing::debug!(target: tracing_targets::MQ,
                ?removed_commit_pointer_shards,
                "rollback_commit_pointers: clear_commit_pointers",
            );
            return Ok(removed_commit_pointer_shards);
        }

        let removed_commit_pointer_shards = self
            .queue
            .rollback_commit_pointers(to_mc_block_id, to_commit_pointers)?;

        let elapsed = start_time.elapsed();
        tracing::info!(target: tracing_targets::MQ_ADAPTER,
            top_blocks = ?to_top_blocks,
            ?removed_commit_pointer_shards,
            elapsed = %humantime::format_duration(elapsed),
            "rollback_commit_pointers: completed"
        );

        Ok(removed_commit_pointer_shards)
    }
}
