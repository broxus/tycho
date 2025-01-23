use anyhow::Result;
use everscale_types::models::{IntAddr, ShardIdent};
use tycho_block_util::queue::QueuePartition;
use tycho_storage::Storage;
use tycho_util::metrics::HistogramGuard;
use tycho_util::FastHashMap;
use weedb::OwnedSnapshot;

use crate::internal_queue::state::state_iterator::{StateIterator, StateIteratorImpl};
use crate::internal_queue::types::{InternalMessageValue, QueueShardRange};
// CONFIG

pub struct CommittedStateConfig {
    pub storage: Storage,
}

// FACTORY

impl<F, R, V> CommittedStateFactory<V> for F
where
    F: Fn() -> R,
    R: CommittedState<V>,
    V: InternalMessageValue,
{
    type CommittedState = R;

    fn create(&self) -> Self::CommittedState {
        self()
    }
}

pub struct CommittedStateImplFactory {
    pub storage: Storage,
}

impl CommittedStateImplFactory {
    pub fn new(storage: Storage) -> Self {
        Self { storage }
    }
}

impl<V: InternalMessageValue> CommittedStateFactory<V> for CommittedStateImplFactory {
    type CommittedState = CommittedStateStdImpl;

    fn create(&self) -> Self::CommittedState {
        CommittedStateStdImpl::new(self.storage.clone())
    }
}

pub trait CommittedStateFactory<V: InternalMessageValue> {
    type CommittedState: CommittedState<V>;

    fn create(&self) -> Self::CommittedState;
}

// TRAIT

pub trait CommittedState<V: InternalMessageValue>: Send + Sync {
    /// Create snapshot
    fn snapshot(&self) -> OwnedSnapshot;
    /// Create iterator for given partition and ranges
    fn iterator(
        &self,
        snapshot: &OwnedSnapshot,
        receiver: ShardIdent,
        partition: QueuePartition,
        ranges: Vec<QueueShardRange>,
    ) -> Result<Box<dyn StateIterator<V>>>;

    /// Delete messages in given partition and ranges
    fn delete(&self, partition: QueuePartition, ranges: &[QueueShardRange]) -> Result<()>;

    /// Load statistics for given partition and ranges
    fn load_statistics(
        &self,
        result: &mut FastHashMap<IntAddr, u64>,
        snapshot: &OwnedSnapshot,
        partition: QueuePartition,
        range: &[QueueShardRange],
    ) -> Result<()>;
}

// IMPLEMENTATION

pub struct CommittedStateStdImpl {
    storage: Storage,
}

impl CommittedStateStdImpl {
    pub fn new(storage: Storage) -> Self {
        Self { storage }
    }
}

impl<V: InternalMessageValue> CommittedState<V> for CommittedStateStdImpl {
    fn snapshot(&self) -> OwnedSnapshot {
        let _histogram = HistogramGuard::begin("tycho_internal_queue_snapshot_time");
        self.storage.internal_queue_storage().snapshot()
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
                .build_iterator_committed(snapshot);

            shard_iters_with_ranges.push((iter, range));
        }

        let iterator = StateIteratorImpl::new(partition, shard_iters_with_ranges, receiver)?;
        Ok(Box::new(iterator))
    }

    fn delete(&self, partition: QueuePartition, ranges: &[QueueShardRange]) -> Result<()> {
        let mut queue_ranges = vec![];
        for range in ranges {
            queue_ranges.push(tycho_storage::model::QueueRange {
                partition,
                shard_ident: range.shard_ident,
                from: range.from,
                to: range.to,
            });
        }
        self.storage.internal_queue_storage().delete(queue_ranges)
    }

    fn load_statistics(
        &self,
        result: &mut FastHashMap<IntAddr, u64>,
        snapshot: &OwnedSnapshot,
        partition: QueuePartition,
        ranges: &[QueueShardRange],
    ) -> Result<()> {
        let _histogram =
            HistogramGuard::begin("tycho_internal_queue_committed_statistics_load_time");
        for range in ranges {
            self.storage
                .internal_queue_storage()
                .collect_committed_stats_in_range(
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
