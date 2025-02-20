use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Result;
use everscale_types::models::{BlockId, BlockIdShort, IntAddr, ShardIdent};
use tycho_block_util::queue::{QueueKey, QueuePartitionIdx, RouterAddr};
use tycho_storage::model::{DiffTailKey, QueueRange, ShardsInternalMessagesKey, StatKey};
use tycho_storage::{InternalQueueSnapshot, InternalQueueTransaction, Storage};
use tycho_util::metrics::HistogramGuard;
use tycho_util::{FastHashMap, FastHashSet};

use crate::internal_queue::state::state_iterator::{StateIterator, StateIteratorImpl};
use crate::internal_queue::types::{
    DiffStatistics, InternalMessageValue, PartitionRouter, QueueShardRange,
};

// CONFIG

pub struct UncommittedStateConfig {
    pub storage: Storage,
}

// FACTORY

impl<F, R, V> UncommittedStateFactory<V> for F
where
    F: Fn() -> R,
    R: UncommittedState<V>,
    V: InternalMessageValue,
{
    type UncommittedState = R;

    fn create(&self) -> Self::UncommittedState {
        self()
    }
}

pub struct UncommittedStateImplFactory {
    pub storage: Storage,
}

impl UncommittedStateImplFactory {
    pub fn new(storage: Storage) -> Self {
        Self { storage }
    }
}

impl<V: InternalMessageValue> UncommittedStateFactory<V> for UncommittedStateImplFactory {
    type UncommittedState = UncommittedStateStdImpl;

    fn create(&self) -> Self::UncommittedState {
        UncommittedStateStdImpl::new(self.storage.clone())
    }
}

pub trait UncommittedStateFactory<V: InternalMessageValue> {
    type UncommittedState: LocalUncommittedState<V>;

    fn create(&self) -> Self::UncommittedState;
}

// TRAIT

#[trait_variant::make(UncommittedState: Send)]
pub trait LocalUncommittedState<V: InternalMessageValue> {
    /// Create iterator for given partition and ranges
    fn iterator(
        &self,
        snapshot: &InternalQueueSnapshot,
        receiver: ShardIdent,
        partition: QueuePartitionIdx,
        ranges: &[QueueShardRange],
    ) -> Result<Box<dyn StateIterator<V>>>;

    /// Move messages and statistics from uncommitted to committed state with given partitions and ranges
    fn commit(
        &self,
        partitions: FastHashSet<QueuePartitionIdx>,
        ranges: &[QueueShardRange],
        mc_block_id: &BlockId,
    ) -> Result<()>;

    /// Delete all uncommitted messages and statistics
    fn truncate(&self) -> Result<()>;

    fn add_messages_with_statistics(
        &self,
        block_id_short: &BlockIdShort,
        partition_router: &PartitionRouter,
        messages: &BTreeMap<QueueKey, Arc<V>>,
        statistics: &DiffStatistics,
        max_message: &QueueKey,
    ) -> Result<()>;

    /// Load statistics for given partition and ranges
    fn load_statistics(
        &self,
        result: &mut FastHashMap<IntAddr, u64>,
        snapshot: &InternalQueueSnapshot,
        partition: QueuePartitionIdx,
        ranges: &[QueueShardRange],
    ) -> Result<()>;

    /// Get diffs tail length
    fn get_diffs_tail_len(&self, shard_ident: &ShardIdent, from: &QueueKey) -> u32;
}

// IMPLEMENTATION

pub struct UncommittedStateStdImpl {
    storage: Storage,
}

impl UncommittedStateStdImpl {
    pub fn new(storage: Storage) -> Self {
        Self { storage }
    }
}

impl<V: InternalMessageValue> UncommittedState<V> for UncommittedStateStdImpl {
    fn iterator(
        &self,
        snapshot: &InternalQueueSnapshot,
        receiver: ShardIdent,
        partition: QueuePartitionIdx,
        ranges: &[QueueShardRange],
    ) -> Result<Box<dyn StateIterator<V>>> {
        let mut shards_iters = Vec::new();

        for range in ranges {
            let from_key = range.from.next_value();
            // exclude from key
            let from = ShardsInternalMessagesKey::new(partition, range.shard_ident, from_key);
            // include to key
            let to_key = range.to.next_value();
            let to = ShardsInternalMessagesKey::new(partition, range.shard_ident, to_key);
            shards_iters.push((
                snapshot.iter_messages_uncommited(from, to),
                range.shard_ident,
            ));
        }

        let iterator = StateIteratorImpl::new(shards_iters, receiver)?;
        Ok(Box::new(iterator))
    }

    fn commit(
        &self,
        partitions: FastHashSet<QueuePartitionIdx>,
        ranges: &[QueueShardRange],
        mc_block_id: &BlockId,
    ) -> Result<()> {
        let ranges = partitions.iter().flat_map(|&partition| {
            ranges.iter().map(move |range| QueueRange {
                partition,
                shard_ident: range.shard_ident,
                from: range.from,
                to: range.to,
            })
        });
        let snapshot = self.storage.internal_queue_storage().make_snapshot();
        let mut tx = self.storage.internal_queue_storage().begin_transaction();

        tx.commit_messages(&snapshot, ranges)?;
        tx.set_last_applied_mc_block_id(mc_block_id);

        tx.write()
    }

    fn truncate(&self) -> Result<()> {
        self.storage.internal_queue_storage().clear_uncommited()
    }

    fn add_messages_with_statistics(
        &self,
        block_id_short: &BlockIdShort,
        partition_router: &PartitionRouter,
        messages: &BTreeMap<QueueKey, Arc<V>>,
        statistics: &DiffStatistics,
        max_message: &QueueKey,
    ) -> Result<()> {
        let mut tx = self.storage.internal_queue_storage().begin_transaction();

        Self::add_messages(&mut tx, block_id_short.shard, partition_router, messages)?;
        Self::add_statistics(&mut tx, statistics)?;
        Self::add_diff_tail(&mut tx, block_id_short, max_message);

        let _histogram =
            HistogramGuard::begin("tycho_internal_queue_add_messages_with_statistics_write_time");

        tx.write()
    }

    fn load_statistics(
        &self,
        result: &mut FastHashMap<IntAddr, u64>,
        snapshot: &InternalQueueSnapshot,
        partition: QueuePartitionIdx,
        ranges: &[QueueShardRange],
    ) -> Result<()> {
        let _histogram =
            HistogramGuard::begin("tycho_internal_queue_uncommitted_statistics_load_time");

        for range in ranges {
            snapshot.collect_uncommitted_stats_in_range(
                range.shard_ident,
                partition,
                &range.from,
                &range.to,
                result,
            )?;
        }

        Ok(())
    }

    fn get_diffs_tail_len(&self, shard_ident: &ShardIdent, from: &QueueKey) -> u32 {
        let snapshot = self.storage.internal_queue_storage().make_snapshot();
        snapshot.calc_diffs_tail_uncommitted(&DiffTailKey {
            shard_ident: *shard_ident,
            max_message: *from,
        })
    }
}

impl UncommittedStateStdImpl {
    /// write new messages to storage
    fn add_messages<V: InternalMessageValue>(
        internal_queue_tx: &mut InternalQueueTransaction,
        source: ShardIdent,
        partition_router: &PartitionRouter,
        messages: &BTreeMap<QueueKey, Arc<V>>,
    ) -> Result<()> {
        let _histogram = HistogramGuard::begin("tycho_internal_queue_apply_diff_add_messages_time");
        let mut buffer = Vec::new();
        for (internal_message_key, message) in messages {
            let destination = message.destination();
            let partition = partition_router.get_partition(Some(message.source()), destination);

            buffer.clear();
            message.serialize(&mut buffer);

            internal_queue_tx.insert_message_uncommitted(
                &tycho_storage::model::ShardsInternalMessagesKey::new(
                    partition,
                    source,
                    *internal_message_key,
                ),
                destination,
                &buffer,
            );
        }

        Ok(())
    }

    /// write new statistics to storage
    fn add_statistics(
        internal_queue_tx: &mut InternalQueueTransaction,
        diff_statistics: &DiffStatistics,
    ) -> Result<()> {
        let _histogram =
            HistogramGuard::begin("tycho_internal_queue_apply_diff_add_statistics_time");
        let shard_ident = diff_statistics.shard_ident();
        let min_message = diff_statistics.min_message();
        let max_message = diff_statistics.max_message();

        for (partition, values) in diff_statistics.iter() {
            for (addr, count) in values {
                let Some(dest) = RouterAddr::from_int_addr(addr) else {
                    anyhow::bail!("cannot add VarAddr to router statistics");
                };

                let key = StatKey {
                    shard_ident: *shard_ident,
                    partition: *partition,
                    min_message: *min_message,
                    max_message: *max_message,
                    dest,
                };

                internal_queue_tx.insert_statistics_uncommitted(&key, *count);
            }
            metrics::counter!(
                "tycho_internal_queue_apply_diff_add_statistics_accounts_count",
                "partition" => partition.to_string(),
            )
            .increment(values.len() as u64);
        }

        Ok(())
    }

    fn add_diff_tail(
        internal_queue_tx: &mut InternalQueueTransaction,
        block_id_short: &BlockIdShort,
        max_message: &QueueKey,
    ) {
        internal_queue_tx.insert_diff_tail_uncommitted(
            &DiffTailKey {
                shard_ident: block_id_short.shard,
                max_message: *max_message,
            },
            block_id_short.seqno.to_le_bytes().as_slice(),
        );
    }
}
