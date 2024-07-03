use std::sync::Arc;

use parking_lot::lock_api::RwLock;

pub use self::persistent_state_keeper::PersistentStateKeeper;
use super::BlockHandleStorage;

mod persistent_state_keeper;

pub struct RuntimeStorage {
    persistent_state_keeper: PersistentStateKeeper,
    last_gc_block_collection: parking_lot::RwLock<Option<u32>>,
}

impl RuntimeStorage {
    pub fn new(block_handle_storage: Arc<BlockHandleStorage>) -> Self {
        Self {
            persistent_state_keeper: PersistentStateKeeper::new(block_handle_storage),
            last_gc_block_collection: RwLock::new(None),
        }
    }

    #[inline(always)]
    pub fn persistent_state_keeper(&self) -> &PersistentStateKeeper {
        &self.persistent_state_keeper
    }

    #[inline(always)]
    pub fn set_last_block_gc_collection(&self, mc_seqno: u32) {
        let mut guard = self.last_gc_block_collection.write();
        *guard = Some(mc_seqno);
    }

    #[inline(always)]
    pub fn get_last_block_gc_collection(&self) -> Option<u32> {
        let mut guard = self.last_gc_block_collection.read();
        *guard
    }
}
