use std::sync::Mutex;

use everscale_types::models::BlockId;
use tycho_block_util::block::ShardHeights;
use tycho_storage::Storage;

pub trait BlockStriderState: Send + Sync + 'static {
    fn load_last_mc_block_id(&self) -> BlockId;

    fn is_commited(&self, block_id: &BlockId) -> bool;

    fn commit_master(&self, block_id: &BlockId, shard_heights: &ShardHeights);
    fn commit_shard(&self, block_id: &BlockId);
}

pub struct PersistentBlockStriderState {
    zerostate_id: BlockId,
    storage: Storage,
}

impl PersistentBlockStriderState {
    pub fn new(zerostate_id: BlockId, storage: Storage) -> Self {
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

    fn is_commited(&self, block_id: &BlockId) -> bool {
        match self.storage.block_handle_storage().load_handle(block_id) {
            Some(handle) => handle.is_applied(),
            None => false,
        }
    }

    fn commit_master(&self, block_id: &BlockId, _shard_heights: &ShardHeights) {
        assert!(block_id.is_masterchain());
        self.storage.node_state().store_last_mc_block_id(block_id);
    }

    fn commit_shard(&self, block_id: &BlockId) {
        assert!(!block_id.is_masterchain());
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

    fn is_commited(&self, block_id: &BlockId) -> bool {
        let commited = self.top_blocks.lock().unwrap();
        let (mc_block_id, shard_heights) = &*commited;
        if block_id.is_masterchain() {
            block_id.seqno <= mc_block_id.seqno
        } else {
            shard_heights.contains_ext(block_id, |top_block, seqno| seqno <= top_block)
        }
    }

    fn commit_master(&self, block_id: &BlockId, shard_heights: &ShardHeights) {
        assert!(block_id.is_masterchain());
        let mut commited = self.top_blocks.lock().unwrap();
        if commited.0.seqno < block_id.seqno {
            *commited = (*block_id, shard_heights.clone());
        }
    }

    fn commit_shard(&self, block_id: &BlockId) {
        assert!(!block_id.is_masterchain());
        // TODO: Update shard height
    }
}
