use std::sync::Arc;

use everscale_types::models::BlockIdShort;

use crate::internal_queue::persistent::persistent_state_snapshot::PersistentStateSnapshot;
use crate::internal_queue::snapshot::StateSnapshot;
use crate::internal_queue::types::ext_types_stubs::EnqueuedMessage;

// CONFIG

pub struct PersistentStateConfig {
    pub database_url: String,
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
    pub config: PersistentStateConfig,
}

impl PersistentStateImplFactory {
    pub fn new(config: PersistentStateConfig) -> Self {
        Self { config }
    }
}

impl PersistentStateFactory for PersistentStateImplFactory {
    type PersistentState = PersistentStateStdImpl;

    fn create(&self) -> Self::PersistentState {
        PersistentStateStdImpl::new(self.config.database_url.clone())
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

pub struct PersistentStateStdImpl {}

impl PersistentStateStdImpl {
    pub fn new(_database_url: String) -> Self {
        Self {}
    }
}

impl PersistentState for PersistentStateStdImpl {
    async fn add_messages(
        &self,
        _block_id_short: BlockIdShort,
        _messages: Vec<Arc<EnqueuedMessage>>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn snapshot(&self) -> Box<dyn StateSnapshot> {
        Box::new(PersistentStateSnapshot {})
    }

    async fn gc() {
        // Garbage collection logic
    }
}
