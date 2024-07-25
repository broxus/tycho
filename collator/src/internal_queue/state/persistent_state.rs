use everscale_types::models::ShardIdent;
use tycho_storage::Storage;
use tycho_util::FastHashMap;
use weedb::OwnedSnapshot;

use crate::internal_queue::state::state_iterator::{ShardRange, StateIterator, StateIteratorImpl};
use crate::internal_queue::types::InternalMessageKey;

// CONFIG

pub struct PersistentStateConfig {
    pub storage: Storage,
}

// FACTORY

impl<F, R> PersistentStateFactory for F
where
    F: Fn() -> R,
    R: PersistentState,
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

impl PersistentStateFactory for PersistentStateImplFactory {
    type PersistentState = PersistentStateStdImpl;

    fn create(&self) -> Self::PersistentState {
        PersistentStateStdImpl::new(self.storage.clone())
    }
}

pub trait PersistentStateFactory {
    type PersistentState: LocalPersistentState;

    fn create(&self) -> Self::PersistentState;
}

// TRAIT

#[trait_variant::make(PersistentState: Send)]
pub trait LocalPersistentState {
    fn snapshot(&self) -> OwnedSnapshot;
    fn state(&self);
    // fn add_messages(
    //     &self,
    //     shard: ShardIdent,
    //     messages: Vec<(u64, HashBytes, i8, HashBytes, Vec<u8>)>,
    // ) -> anyhow::Result<i32>;

    fn iterator(
        &self,
        snapshot: &OwnedSnapshot,
        receiver: ShardIdent,
        ranges: &FastHashMap<ShardIdent, ShardRange>,
    ) -> Box<dyn StateIterator>;

    fn delete_messages(&self, shard: ShardIdent, key: InternalMessageKey) -> anyhow::Result<()>;
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

impl PersistentState for PersistentStateStdImpl {
    fn snapshot(&self) -> OwnedSnapshot {
        self.storage.internal_queue_storage().snapshot()
    }

    fn state(&self) {
        self.storage
            .internal_queue_storage()
            .print_cf_sizes()
            .unwrap();
    }

    fn iterator(
        &self,
        snapshot: &OwnedSnapshot,
        receiver: ShardIdent,
        ranges: &FastHashMap<ShardIdent, ShardRange>,
    ) -> Box<dyn StateIterator> {
        let shards = ranges.keys().cloned().collect::<Vec<_>>();
        let iter = self
            .storage
            .internal_queue_storage()
            .build_iterator(&snapshot, shards);

        Box::new(StateIteratorImpl::new(iter, receiver, ranges.clone()))
    }

    fn delete_messages(&self, shard: ShardIdent, key: InternalMessageKey) -> anyhow::Result<()> {
        self.storage
            .internal_queue_storage()
            .delete_messages(shard, (key.lt, key.hash))
    }
}
