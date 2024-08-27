use ahash::HashMapExt;
use everscale_types::models::ShardIdent;
use tycho_block_util::queue::QueueKey;
use tycho_storage::Storage;
use tycho_util::FastHashMap;
use weedb::OwnedSnapshot;

use crate::internal_queue::state::state_iterator::{
    ShardIteratorWithRange, StateIterator, StateIteratorImpl,
};
use crate::internal_queue::types::InternalMessageValue;

// CONFIG

pub struct PersistentStateConfig {
    pub storage: Storage,
}

// FACTORY

impl<F, R, V> PersistentStateFactory<V> for F
where
    F: Fn() -> R,
    R: PersistentState<V>,
    V: InternalMessageValue,
{
    type PersistentState = R;

    fn create(&self) -> Self::PersistentState {
        self()
    }
}

pub struct PersistentStateImplFactory {
    pub storage: Storage,
}

impl PersistentStateImplFactory {
    pub fn new(storage: Storage) -> Self {
        Self { storage }
    }
}

impl<V: InternalMessageValue> PersistentStateFactory<V> for PersistentStateImplFactory {
    type PersistentState = PersistentStateStdImpl;

    fn create(&self) -> Self::PersistentState {
        PersistentStateStdImpl::new(self.storage.clone())
    }
}

pub trait PersistentStateFactory<V: InternalMessageValue> {
    type PersistentState: PersistentState<V>;

    fn create(&self) -> Self::PersistentState;
}

// TRAIT

pub trait PersistentState<V: InternalMessageValue>: Send + Sync {
    fn snapshot(&self) -> OwnedSnapshot;

    fn iterator(
        &self,
        snapshot: &OwnedSnapshot,
        receiver: ShardIdent,
        ranges: &FastHashMap<ShardIdent, (QueueKey, QueueKey)>,
    ) -> Box<dyn StateIterator<V>>;

    fn delete_messages(&self, shard: ShardIdent, key: &QueueKey) -> anyhow::Result<()>;
}

// IMPLEMENTATION

pub struct PersistentStateStdImpl {
    storage: Storage,
}

impl PersistentStateStdImpl {
    pub fn new(storage: Storage) -> Self {
        Self { storage }
    }
}

impl<V: InternalMessageValue> PersistentState<V> for PersistentStateStdImpl {
    fn snapshot(&self) -> OwnedSnapshot {
        self.storage.internal_queue_storage().snapshot()
    }

    fn iterator(
        &self,
        snapshot: &OwnedSnapshot,
        receiver: ShardIdent,
        ranges: &FastHashMap<ShardIdent, (QueueKey, QueueKey)>,
    ) -> Box<dyn StateIterator<V>> {
        let mut shard_iters_with_ranges = FastHashMap::with_capacity(ranges.len());

        for (&shard, range) in ranges {
            let iter = self
                .storage
                .internal_queue_storage()
                .build_iterator_persistent(snapshot);

            shard_iters_with_ranges
                .insert(shard, ShardIteratorWithRange::new(iter, range.0, range.1));
        }

        Box::new(StateIteratorImpl::new(shard_iters_with_ranges, receiver))
    }

    fn delete_messages(&self, shard: ShardIdent, until: &QueueKey) -> anyhow::Result<()> {
        self.storage
            .internal_queue_storage()
            .delete_messages(shard, &QueueKey::MIN, until)
    }
}
