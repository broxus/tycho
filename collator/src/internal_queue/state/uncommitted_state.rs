use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use ahash::HashMapExt;
use anyhow::Result;
use everscale_types::cell::{Cell, CellBuilder, CellFamily, Store};
use everscale_types::models::{IntAddr, ShardIdent};
use everscale_types::prelude::Boc;
use serde::Serialize;
use tycho_block_util::queue::{QueueKey, QueuePartition};
use tycho_storage::model::{QueueRange, StatKey};
use tycho_storage::Storage;
use tycho_util::{FastHashMap, FastHashSet};
use weedb::rocksdb::WriteBatch;
use weedb::OwnedSnapshot;

use crate::internal_queue::state::state_iterator::{
    ShardIteratorWithRange, StateIterator, StateIteratorImpl,
};
use crate::internal_queue::types::{
    DiffStatistics, InternalMessageValue, PartitionRouter, QueueShardRange, QueueStatistics,
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
    fn iterator(
        &self,
        snapshot: &OwnedSnapshot,
        receiver: ShardIdent,
        partition: QueuePartition,
        ranges: Vec<QueueShardRange>,
    ) -> Result<Box<dyn StateIterator<V>>>;

    fn commit(&self, partitions: &[QueuePartition], ranges: &[QueueShardRange]) -> Result<()>;
    fn truncate(&self) -> Result<()>;

    fn add_messages_with_statistics(
        &self,
        source: ShardIdent,
        partition_router: &PartitionRouter,
        messages: &BTreeMap<QueueKey, Arc<V>>,
        statistics: DiffStatistics,
    ) -> Result<()>;

    fn load_statistics(
        &self,
        result: &mut FastHashMap<IntAddr, u64>,
        snapshot: &OwnedSnapshot,
        partition: QueuePartition,
        ranges: &Vec<QueueShardRange>,
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
    fn add_messages_with_statistics(
        &self,
        source: ShardIdent,
        partition_router: &PartitionRouter,
        messages: &BTreeMap<QueueKey, Arc<V>>,
        statistics: DiffStatistics,
    ) -> Result<()> {
        let mut batch = WriteBatch::default();

        self.add_messages(&mut batch, source, partition_router, messages)?;
        self.add_statistics(&mut batch, statistics)?;

        self.storage.internal_queue_storage().write_batch(batch)?;
        Ok(())
    }

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

    fn truncate(&self) -> Result<()> {
        self.storage
            .internal_queue_storage()
            .clear_uncommitted_queue()
    }

    fn load_statistics(
        &self,
        result: &mut FastHashMap<IntAddr, u64>,
        snapshot: &OwnedSnapshot,
        partition: QueuePartition,
        ranges: &Vec<QueueShardRange>,
    ) -> Result<()> {
        for range in ranges {
            self.storage
                .internal_queue_storage()
                .collect_uncommited_stats_in_range(
                    &snapshot,
                    range.shard_ident,
                    partition,
                    range.from,
                    range.to,
                    result,
                )?;
        }

        Ok(())
    }

    fn commit(&self, partitions: &[QueuePartition], ranges: &[QueueShardRange]) -> Result<()> {
        let mut queue_ranges = vec![];
        for partition in partitions {
            for range in ranges {
                queue_ranges.push(QueueRange {
                    partition: *partition,
                    shard_ident: range.shard_ident,
                    from: range.from,
                    to: range.to,
                });
            }
        }
        self.storage.internal_queue_storage().commit(queue_ranges)
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

            let partition = partition_router
                .get(&destination)
                .unwrap_or(&QueuePartition::default())
                .clone();

            self.storage
                .internal_queue_storage()
                .insert_message_uncommitted(
                    batch,
                    tycho_storage::model::ShardsInternalMessagesKey::new(
                        partition,
                        source.clone(),
                        *internal_message_key,
                    ),
                    destination,
                    &message.serialize()?,
                )?;
        }

        Ok(())
    }

    fn add_statistics(
        &self,
        batch: &mut WriteBatch,
        diff_statistics: DiffStatistics,
    ) -> Result<()> {
        let shard_ident = diff_statistics.shard_ident();
        let min_message = diff_statistics.min_message();
        let max_message = diff_statistics.max_message();

        for (index, (partition, values)) in diff_statistics.iter().enumerate() {
            let cx = &mut Cell::empty_context();

            for value in values {
                let mut key_builder = CellBuilder::new();

                let (addr, count) = value;

                addr.store_into(&mut key_builder, cx)?;
                let dest = key_builder.build()?;

                let dest = Boc::encode(dest);

                let key = StatKey {
                    shard_ident: *shard_ident,
                    partition: partition.clone(),
                    min_message: min_message.clone(),
                    max_message: max_message.clone(),
                    index: index as u64,
                };

                self.storage
                    .internal_queue_storage()
                    .insert_destination_stat_uncommitted(batch, &key, &dest, *count)?;
            }
        }

        Ok(())
    }
}
