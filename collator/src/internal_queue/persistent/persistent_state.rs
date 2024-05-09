use std::sync::Arc;

use everscale_types::models::BlockIdShort;

use crate::internal_queue::persistent::persistent_state_snapshot::PersistentStateSnapshot;
use crate::internal_queue::snapshot::StateSnapshot;
use crate::internal_queue::types::ext_types_stubs::EnqueuedMessage;

#[trait_variant::make(PersistentState: Send)]
pub trait LocalPersistentState<S>
where
    S: StateSnapshot,
{
    fn new() -> Self;
    async fn add_messages(
        &self,
        block_id_short: BlockIdShort,
        messages: Vec<Arc<EnqueuedMessage>>,
    ) -> anyhow::Result<()>;
    async fn snapshot(&self) -> Box<S>;
    async fn gc();
}

pub struct PersistentStateImpl {}

impl PersistentState<PersistentStateSnapshot> for PersistentStateImpl {
    fn new() -> Self {
        Self {}
    }

    async fn add_messages(
        &self,
        _block_id_short: BlockIdShort,
        _messages: Vec<Arc<EnqueuedMessage>>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn snapshot(&self) -> Box<PersistentStateSnapshot> {
        Box::new(PersistentStateSnapshot {})
    }

    async fn gc() {}
}
