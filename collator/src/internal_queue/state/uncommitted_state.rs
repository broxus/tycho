use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Result;
use everscale_types::models::{IntAddr, ShardIdent};
use tycho_block_util::queue::{QueueKey, QueuePartition, RouterAddr};
use tycho_storage::model::{QueueRange, StatKey};
use tycho_storage::Storage;
use tycho_util::metrics::HistogramGuard;
use tycho_util::{FastHashMap, FastHashSet};
use weedb::rocksdb::WriteBatch;
use weedb::OwnedSnapshot;

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
        snapshot: &OwnedSnapshot,
        receiver: ShardIdent,
        partition: QueuePartition,
        ranges: Vec<QueueShardRange>,
    ) -> Result<Box<dyn StateIterator<V>>>;

    /// Move messages and statistics from uncommitted to committed state with given partitions and ranges
    fn commit(
        &self,
        partitions: FastHashSet<QueuePartition>,
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
        snapshot: &OwnedSnapshot,
        partition: QueuePartition,
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
        snapshot: &OwnedSnapshot,
        receiver: ShardIdent,
        partition: QueuePartition,
        ranges: Vec<QueueShardRange>,
    ) -> Result<Box<dyn StateIterator<V>>> {
        let mut shard_iters_with_ranges = Vec::new();

        for range in ranges {
            let iter = self
                .storage
                .internal_queue_storage()
                .build_iterator_uncommitted(snapshot);

            shard_iters_with_ranges.push((iter, range));
        }

        let iterator = StateIteratorImpl::new(partition, shard_iters_with_ranges, receiver)?;
        Ok(Box::new(iterator))
    }

    fn commit(
        &self,
        partitions: FastHashSet<QueuePartition>,
        ranges: &[QueueShardRange],
    ) -> Result<()> {
        let mut queue_ranges = vec![];
        for partition in partitions {
            for range in ranges {
                queue_ranges.push(QueueRange {
                    partition,
                    shard_ident: range.shard_ident,
                    from: range.from,
                    to: range.to,
                });
            }
        }
        self.storage.internal_queue_storage().commit(queue_ranges)
    }

    fn truncate(&self) -> Result<()> {
        self.storage
            .internal_queue_storage()
            .clear_uncommitted_state()
    }

    fn add_messages_with_statistics(
        &self,
        source: ShardIdent,
        partition_router: &PartitionRouter,
        messages: &BTreeMap<QueueKey, Arc<V>>,
        statistics: &DiffStatistics,
    ) -> Result<()> {
        let mut batch = WriteBatch::default();

        self.add_messages(&mut batch, source, partition_router, messages)?;
        self.add_statistics(&mut batch, statistics)?;

        self.storage.internal_queue_storage().write_batch(batch)?;
        Ok(())
    }

    fn load_statistics(
        &self,
        result: &mut FastHashMap<IntAddr, u64>,
        snapshot: &OwnedSnapshot,
        partition: QueuePartition,
        ranges: &[QueueShardRange],
    ) -> Result<()> {
        let _histogram =
            HistogramGuard::begin("tycho_internal_queue_uncommitted_statistics_load_time");
        for range in ranges {
            self.storage
                .internal_queue_storage()
                .collect_uncommitted_stats_in_range(
                    snapshot,
                    range.shard_ident,
                    partition,
                    range.from,
                    range.to,
                    result,
                )?;
        }

        Ok(())
    }
}

impl UncommittedStateStdImpl {
    /// write new messages to storage
    fn add_messages<V: InternalMessageValue>(
        &self,
        batch: &mut WriteBatch,
        source: ShardIdent,
        partition_router: &PartitionRouter,
        messages: &BTreeMap<QueueKey, Arc<V>>,
    ) -> Result<()> {
        for (internal_message_key, message) in messages {
            let destination = message.destination();
            let partition = partition_router.get_partition(Some(message.source()), destination);

            self.storage
                .internal_queue_storage()
                .insert_message_uncommitted(
                    batch,
                    tycho_storage::model::ShardsInternalMessagesKey::new(
                        partition,
                        source,
                        *internal_message_key,
                    ),
                    destination,
                    &message.serialize()?,
                )?;
        }

        Ok(())
    }

    /// write new statistics to storage
    fn add_statistics(
        &self,
        batch: &mut WriteBatch,
        diff_statistics: &DiffStatistics,
    ) -> Result<()> {
        let shard_ident = diff_statistics.shard_ident();
        let min_message = diff_statistics.min_message();
        let max_message = diff_statistics.max_message();

        for (partition, values) in diff_statistics.iter() {
            for value in values {
                let (addr, count) = value;
                let dest = RouterAddr::try_from(addr.clone())?;
                let key = StatKey {
                    shard_ident: *shard_ident,
                    partition: *partition,
                    min_message: *min_message,
                    max_message: *max_message,
                    dest,
                };

                self.storage
                    .internal_queue_storage()
                    .insert_statistics_uncommitted(batch, &key, *count)?;
            }
        }

        Ok(())
    }
}
