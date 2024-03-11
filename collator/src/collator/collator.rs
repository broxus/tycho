use anyhow::Result;
use async_trait::async_trait;

use crate::types::ext_types::ShardIdent;
use crate::types::BlockCollationResult;

use super::types::WorkingState;

// EVENTS EMITTER AMD LISTENER

#[async_trait]
pub trait CollatorEventEmitter {
    /// When new shard or master block was collated
    async fn on_block_candidat_event(&self, collation_result: BlockCollationResult);
}

#[async_trait]
pub trait CollatorEventListener: Send + Sync {
    /// Process new collated shard or master block
    async fn on_block_candidat(&self, collation_result: BlockCollationResult) -> Result<()>;
}

#[async_trait]
pub trait Collator: Send + Sync + 'static {
    /// Produce new block, return created block + updated shard state, and update working state
    async fn collate() -> Result<BlockCollationResult>;
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
    async fn collate() -> Result<BlockCollationResult> {
        todo!()
    }
}
