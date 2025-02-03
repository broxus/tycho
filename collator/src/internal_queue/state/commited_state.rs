use anyhow::Result;
use everscale_types::models::{IntAddr, ShardIdent};
use tycho_block_util::queue::QueuePartitionIdx;
use tycho_storage::model::ShardsInternalMessagesKey;
use tycho_storage::{InternalQueueSnapshot, Storage};
use tycho_util::metrics::HistogramGuard;
use tycho_util::FastHashMap;

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
    fn snapshot(&self) -> InternalQueueSnapshot;

    /// Create iterator for given partition and ranges
    fn iterator(
        &self,
        snapshot: &InternalQueueSnapshot,
        receiver: ShardIdent,
        partition: QueuePartitionIdx,
        ranges: &[QueueShardRange],
    ) -> Result<Box<dyn StateIterator<V>>>;

    /// Delete messages in given partition and ranges
    fn delete(&self, partition: QueuePartitionIdx, ranges: &[QueueShardRange]) -> Result<()>;

    /// Load statistics for given partition and ranges
    fn load_statistics(
        &self,
        result: &mut FastHashMap<IntAddr, u64>,
        snapshot: &InternalQueueSnapshot,
        partition: QueuePartitionIdx,
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
    fn snapshot(&self) -> InternalQueueSnapshot {
        let _histogram = HistogramGuard::begin("tycho_internal_queue_snapshot_time");
        self.storage.internal_queue_storage().make_snapshot()
    }

    fn iterator(
        &self,
        snapshot: &InternalQueueSnapshot,
        receiver: ShardIdent,
        partition: QueuePartitionIdx,
        ranges: &[QueueShardRange],
    ) -> Result<Box<dyn StateIterator<V>>> {
        let mut shards_iters = Vec::new();

        for range in ranges {
            // exclude from key
            let from_key = range.from.next_value();
            let from = ShardsInternalMessagesKey::new(partition, range.shard_ident, from_key);
            // include to key
            let to_key = range.to.next_value();
            let to = ShardsInternalMessagesKey::new(partition, range.shard_ident, to_key);
            shards_iters.push((snapshot.iter_messages_commited(from, to), range.shard_ident));
        }

        let iterator = StateIteratorImpl::new(shards_iters, receiver)?;
        Ok(Box::new(iterator))
    }

    fn delete(&self, partition: QueuePartitionIdx, ranges: &[QueueShardRange]) -> Result<()> {
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
        snapshot: &InternalQueueSnapshot,
        partition: QueuePartitionIdx,
        ranges: &[QueueShardRange],
    ) -> Result<()> {
        let _histogram =
            HistogramGuard::begin("tycho_internal_queue_committed_statistics_load_time");

        for range in ranges {
            snapshot.collect_committed_stats_in_range(
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
