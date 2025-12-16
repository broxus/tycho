use std::sync::Mutex;

use anyhow::Result;
use tycho_block_util::block::{BlockStuff, ShardHeights};
use tycho_types::models::BlockId;

use crate::storage::CoreStorage;

pub struct UpdateGcState<'a> {
    /// Related masterchain block id.
    /// In case of context for mc block this id is the same as `block.id()`.
    pub mc_block_id: &'a BlockId,
    /// Related masterchain block flag.
    /// In case of context for mc block this flag is the same as `is_key_block`.
    pub mc_is_key_block: bool,
    /// Whether the `block` from this context is a key block.
    pub is_key_block: bool,
    /// Parsed block data.
    pub block: &'a BlockStuff,
}

#[derive(Debug, Clone, Copy)]
pub struct CommitMasterBlock<'a> {
    pub block_id: &'a BlockId,
    pub is_key_block: bool,
    pub shard_heights: &'a ShardHeights,
}

#[derive(Debug, Clone, Copy)]
pub struct CommitShardBlock<'a> {
    pub block_id: &'a BlockId,
}

pub trait BlockStriderState: Send + Sync + 'static {
    fn load_last_mc_block_id(&self) -> BlockId;

    fn is_committed(&self, block_id: &BlockId) -> bool;

    fn update_gc_state(&self, ctx: UpdateGcState<'_>) -> Result<()>;

    fn commit_master(&self, ctx: CommitMasterBlock<'_>);
    fn commit_shard(&self, ctx: CommitShardBlock<'_>);
}

pub struct PersistentBlockStriderState {
    zerostate_id: BlockId,
    storage: CoreStorage,
}

impl PersistentBlockStriderState {
    pub fn new(zerostate_id: BlockId, storage: CoreStorage) -> Self {
        Self {
            zerostate_id,
            storage,
        }
    }
}

impl BlockStriderState for PersistentBlockStriderState {
    fn load_last_mc_block_id(&self) -> BlockId {
        match self.storage.node_state().load_last_mc_block_id() {
            Some(block_id) => block_id,
            None => self.zerostate_id,
        }
    }

    fn is_committed(&self, block_id: &BlockId) -> bool {
        if block_id.is_masterchain() {
            let last_mc = self.load_last_mc_block_id();
            last_mc.seqno >= block_id.seqno
        } else {
            match self.storage.block_handle_storage().load_handle(block_id) {
                Some(handle) => handle.is_committed(),
                None => false,
            }
        }
    }

    fn update_gc_state(&self, ctx: UpdateGcState<'_>) -> Result<()> {
        self.storage.update_gc_state(ctx.is_key_block, ctx.block);
        Ok(())
    }

    fn commit_master(&self, ctx: CommitMasterBlock<'_>) {
        assert!(ctx.block_id.is_masterchain());
        self.storage
            .node_state()
            .store_last_mc_block_id(ctx.block_id);
    }

    fn commit_shard(&self, ctx: CommitShardBlock<'_>) {
        assert!(!ctx.block_id.is_masterchain());

        let handles = self.storage.block_handle_storage();
        if let Some(handle) = handles.load_handle(ctx.block_id) {
            handles.set_block_committed(&handle);
        } else {
            tracing::warn!(
                block_id = %ctx.block_id,
                "committing shard block without a block handle",
            );
        }
    }
}

pub struct TempBlockStriderState {
    top_blocks: Mutex<(BlockId, ShardHeights)>,
}

impl TempBlockStriderState {
    pub fn new(mc_block_id: BlockId, shard_heights: ShardHeights) -> Self {
        Self {
            top_blocks: Mutex::new((mc_block_id, shard_heights)),
        }
    }
}

impl BlockStriderState for TempBlockStriderState {
    fn load_last_mc_block_id(&self) -> BlockId {
        self.top_blocks.lock().unwrap().0
    }

    fn is_committed(&self, block_id: &BlockId) -> bool {
        let commited = self.top_blocks.lock().unwrap();
        let (mc_block_id, shard_heights) = &*commited;
        if block_id.is_masterchain() {
            block_id.seqno <= mc_block_id.seqno
        } else {
            shard_heights.contains_ext(block_id, |top_block, seqno| seqno <= top_block)
        }
    }

    fn update_gc_state(&self, _ctx: UpdateGcState<'_>) -> Result<()> {
        Ok(())
    }

    fn commit_master(&self, ctx: CommitMasterBlock<'_>) {
        assert!(ctx.block_id.is_masterchain());
        let mut commited = self.top_blocks.lock().unwrap();
        if commited.0.seqno < ctx.block_id.seqno {
            *commited = (*ctx.block_id, ctx.shard_heights.clone());
        }
    }

    fn commit_shard(&self, ctx: CommitShardBlock<'_>) {
        assert!(!ctx.block_id.is_masterchain());
        // TODO: Update shard height
    }
}
