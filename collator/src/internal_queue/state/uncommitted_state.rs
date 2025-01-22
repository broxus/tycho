use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Result;
use everscale_types::models::{IntAddr, ShardIdent};
use tycho_block_util::queue::{QueueKey, QueuePartitionIdx, RouterAddr};
use tycho_storage::model::{QueueRange, StatKey};
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
    ) -> Result<()>;

    /// Delete all uncommitted messages and statistics
    fn truncate(&self) -> Result<()>;

    fn add_messages_with_statistics(
        &self,
        source: ShardIdent,
        partition_router: &PartitionRouter,
        messages: &BTreeMap<QueueKey, Arc<V>>,
        statistics: &DiffStatistics,
    ) -> Result<()>;

    /// Load statistics for given partition and ranges
    fn load_statistics(
        &self,
        result: &mut FastHashMap<IntAddr, u64>,
        snapshot: &InternalQueueSnapshot,
        partition: QueuePartitionIdx,
        ranges: &[QueueShardRange],
    ) -> Result<()>;
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
        let mut shard_iters_with_ranges = Vec::new();

        for range in ranges {
            shard_iters_with_ranges.push((snapshot.iter_messages_uncommited(), range.clone()));
        }

        let iterator = StateIteratorImpl::new(partition, shard_iters_with_ranges, receiver)?;
        Ok(Box::new(iterator))
    }

    fn commit(
        &self,
        partitions: FastHashSet<QueuePartitionIdx>,
        ranges: &[QueueShardRange],
    ) -> Result<()> {
        let ranges = partitions.iter().flat_map(|&partition| {
            ranges.iter().map(move |range| QueueRange {
                partition,
                shard_ident: range.shard_ident,
                from: range.from,
                to: range.to,
            })
        });

        self.storage.internal_queue_storage().commit(ranges)
    }

    fn truncate(&self) -> Result<()> {
        self.storage.internal_queue_storage().clear_uncommited()
    }

    fn add_messages_with_statistics(
        &self,
        source: ShardIdent,
        partition_router: &PartitionRouter,
        messages: &BTreeMap<QueueKey, Arc<V>>,
        statistics: &DiffStatistics,
    ) -> Result<()> {
        let mut tx = self.storage.internal_queue_storage().begin_transaction();

        Self::add_messages(&mut tx, source, partition_router, messages)?;
        Self::add_statistics(&mut tx, statistics)?;

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
}

impl UncommittedStateStdImpl {
    /// write new messages to storage
    fn add_messages<V: InternalMessageValue>(
        internal_queue_tx: &mut InternalQueueTransaction,
        source: ShardIdent,
        partition_router: &PartitionRouter,
        messages: &BTreeMap<QueueKey, Arc<V>>,
    ) -> Result<()> {
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
        }

        Ok(())
    }
}
