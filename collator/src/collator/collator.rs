use anyhow::Result;
use async_trait::async_trait;

use crate::types::ext_types::ShardIdent;
use crate::types::BlockCandidate;

use super::types::WorkingState;

// EVENTS EMITTER AMD LISTENER

#[async_trait]
pub trait CollatorEventEmitter {
    /// When new shard or master block was collated
    async fn on_block_candidat_event(&self, candidate: BlockCandidate);
}

#[async_trait]
pub trait CollatorEventListener: Send + Sync {
    /// Process new collated shard or master block
    async fn on_block_candidat(&self, candidate: BlockCandidate) -> Result<()>;
}

#[async_trait]
pub trait Collator: Send + Sync + 'static {
    async fn collate() -> Result<BlockCandidate>;
}

pub(crate) struct CollatorStdImpl {
    shard_id: ShardIdent,
    working_state: WorkingState,
}

impl CollatorStdImpl {
    pub fn new() -> Self {
        Self {
            shard_id: todo!(),
            working_state: todo!(),
        }
    }
}

#[async_trait]
impl Collator for CollatorStdImpl {
    async fn collate() -> Result<BlockCandidate> {
        todo!()
    }
}
