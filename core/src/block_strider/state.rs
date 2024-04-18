use std::sync::Arc;

use everscale_types::models::BlockId;

use tycho_storage::Storage;

pub trait BlockStriderState: Send + Sync + 'static {
    fn load_last_traversed_master_block_id(&self) -> BlockId;
    fn is_traversed(&self, block_id: &BlockId) -> bool;
    fn commit_traversed(&self, block_id: BlockId);
}

impl BlockStriderState for Arc<Storage> {
    fn load_last_traversed_master_block_id(&self) -> BlockId {
        self.node_state()
            .load_last_mc_block_id()
            .expect("db is not initialized")
    }

    fn is_traversed(&self, block_id: &BlockId) -> bool {
        self.block_handle_storage()
            .load_handle(block_id)
            .expect("db is dead")
            .is_some()
    }

    fn commit_traversed(&self, block_id: BlockId) {
        if block_id.is_masterchain() {
            self.node_state()
                .store_last_mc_block_id(&block_id)
                .expect("db is dead");
        }
        // other blocks are stored with state applier: todo rework this?
    }
}

pub struct InMemoryBlockStriderState {
    last_traversed_master_block_id: parking_lot::Mutex<BlockId>,
    // TODO: Use topblocks here.
    traversed_blocks: tycho_util::FastDashSet<BlockId>,
}

impl InMemoryBlockStriderState {
    pub fn with_initial_id(id: BlockId) -> Self {
        let traversed_blocks = tycho_util::FastDashSet::default();
        traversed_blocks.insert(id);

        Self {
            last_traversed_master_block_id: parking_lot::Mutex::new(id),
            traversed_blocks,
        }
    }
}

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
