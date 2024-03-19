use everscale_types::models::BlockId;
use parking_lot::Mutex;

pub trait BlockStriderState: Send + Sync + 'static {
    fn load_last_traversed_master_block_id(&self) -> BlockId;
    fn is_traversed(&self, block_id: &BlockId) -> bool;
    fn commit_traversed(&self, block_id: BlockId);
}

impl<T: BlockStriderState> BlockStriderState for Box<T> {
    fn load_last_traversed_master_block_id(&self) -> BlockId {
        <T as BlockStriderState>::load_last_traversed_master_block_id(self)
    }

    fn is_traversed(&self, block_id: &BlockId) -> bool {
        <T as BlockStriderState>::is_traversed(self, block_id)
    }

    fn commit_traversed(&self, block_id: BlockId) {
        <T as BlockStriderState>::commit_traversed(self, block_id);
    }
}

#[cfg(test)]
pub struct InMemoryBlockStriderState {
    last_traversed_master_block_id: Mutex<BlockId>,
    traversed_blocks: tycho_util::FastDashSet<BlockId>,
}

#[cfg(test)]
impl InMemoryBlockStriderState {
    pub fn new(id: BlockId) -> Self {
        Self {
            last_traversed_master_block_id: Mutex::new(id),
            traversed_blocks: tycho_util::FastDashSet::default(),
        }
    }
}

#[cfg(test)]
impl BlockStriderState for InMemoryBlockStriderState {
    fn load_last_traversed_master_block_id(&self) -> BlockId {
        *self.last_traversed_master_block_id.lock()
    }

    fn is_traversed(&self, block_id: &BlockId) -> bool {
        self.traversed_blocks.contains(block_id)
    }

    fn commit_traversed(&self, block_id: BlockId) {
        if block_id.is_masterchain() {
            *self.last_traversed_master_block_id.lock() = block_id;
        }

        self.traversed_blocks.insert(block_id);
    }
}
