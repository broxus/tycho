use std::collections::HashMap;
use std::sync::Arc;

use everscale_types::cell::{Cell, HashBytes};
use everscale_types::models::{BlockIdShort, ShardIdent};
use tycho_storage::Storage;
use weedb::rocksdb::DBRawIterator;

use crate::internal_queue::persistent::persistent_state_snapshot::PersistentStateSnapshot;
use crate::internal_queue::snapshot::{ShardRange, StateSnapshot};
use crate::internal_queue::types::EnqueuedMessage;
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

impl<'a> PersistentStateImplFactory {
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
    async fn add_messages(
        &self,
        block_id_short: BlockIdShort,
        messages: Vec<Arc<EnqueuedMessage>>,
    ) -> anyhow::Result<()>;
    async fn snapshot(
        &self,
        ranges: &HashMap<ShardIdent, ShardRange>,
        for_shard_id: ShardIdent,
    ) -> Box<dyn StateSnapshot>;
    async fn gc();
}

// IMPLEMENTATION

pub struct PersistentStateStdImpl {
    // TODO remove static and use owned_snapshot
    storage: Storage,
}

impl PersistentStateStdImpl {
    pub fn new(storage: Storage) -> Self {
        Self { storage }
    }
}

impl PersistentState for PersistentStateStdImpl {
    async fn add_messages(
        &self,
        block_id_short: BlockIdShort,
        messages: Vec<Arc<EnqueuedMessage>>,
    ) -> anyhow::Result<()> {
        let messages: Vec<(u64, HashBytes, Cell)> = messages
            .iter()
            .map(|m| (m.info.created_lt, m.hash, m.cell.clone()))
            .collect();
        self.storage
            .internal_queue_storage()
            .insert_messages(block_id_short.shard, &messages)?;
        Ok(())
    }

    async fn snapshot(
        &self,
        ranges: &HashMap<ShardIdent, ShardRange>,
        for_shard_id: ShardIdent,
    ) -> Box<dyn StateSnapshot> {
        let snapshot = self.storage.internal_queue_storage().snapshot();

        let mut readopts = self
            .storage
            .internal_queue_storage()
            .iterator_readopts(&snapshot);

        let cf1_handle = self
            .storage
            .base_db()
            .rocksdb()
            .cf_handle("internal_messages")
            .unwrap();
        let mut iter = self
            .storage
            .base_db()
            .rocksdb()
            .raw_iterator_cf_opt(&cf1_handle, readopts);

        let iter = unsafe { extend_lifetime(iter) };

        Box::new(PersistentStateSnapshot::new(iter))
    }

    async fn gc() {
        // Garbage collection logic
    }
}

unsafe fn extend_lifetime<'a>(r: DBRawIterator<'a>) -> DBRawIterator<'static> {
    std::mem::transmute::<DBRawIterator<'a>, DBRawIterator<'static>>(r)
}
