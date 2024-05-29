use std::sync::Arc;

use everscale_types::models::BlockIdShort;
use tycho_storage::Storage;

// use crate::internal_queue::persistent::persistent_state_snapshot::PersistentStateSnapshot;
use crate::internal_queue::snapshot::StateSnapshot;
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
        // self.storage.persistent_state_storage()
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
    async fn snapshot(&self) -> Box<dyn StateSnapshot>;
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
        _block_id_short: BlockIdShort,
        _messages: Vec<Arc<EnqueuedMessage>>,
    ) -> anyhow::Result<()> {
        Ok(())
        // self.storage.internal_queue_storage().add_messages(_messages).await
    }

    async fn snapshot(&self) -> Box<dyn StateSnapshot> {
        todo!("Implement snapshot")
        // let snapshot = self.storage.internal_queue_storage().snapshot();
        // Box::new(PersistentStateSnapshot::new(snapshot))
    }

    async fn gc() {
        // Garbage collection logic
    }
}
