use std::sync::Arc;

use everscale_types::cell::{Cell, HashBytes};
use everscale_types::models::{BlockIdShort, ShardIdent};
use tycho_storage::Storage;

use crate::internal_queue::state::persistent::persistent_state_iterator::PersistentStateIterator;
use crate::internal_queue::state::state_iterator::StateIterator;
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
    async fn add_messages(
        &self,
        block_id_short: BlockIdShort,
        messages: Vec<Arc<EnqueuedMessage>>,
    ) -> anyhow::Result<()>;

    fn iterator(&self, receiver: ShardIdent) -> Box<dyn StateIterator>;

    async fn delete_messages(
        &self,
        source_shards: Vec<ShardIdent>,
        receiver: ShardIdent,
        lt_from: u64,
        lt_to: u64,
    ) -> anyhow::Result<()>;
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

    fn iterator(&self, receiver: ShardIdent) -> Box<dyn StateIterator> {
        let snapshot = self.storage.internal_queue_storage().snapshot();

        let iter = self
            .storage
            .internal_queue_storage()
            .build_iterator(&snapshot);

        Box::new(PersistentStateIterator::new(iter, receiver))
    }

    async fn delete_messages(
        &self,
        source_shards: Vec<ShardIdent>,
        receiver: ShardIdent,
        lt_from: u64,
        lt_to: u64,
    ) -> anyhow::Result<()> {
        for shard in source_shards {
            self.storage
                .internal_queue_storage()
                .delete_messages(shard, receiver, lt_from, lt_to)?;
        }
        Ok(())
    }
}
