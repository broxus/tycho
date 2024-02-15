use std::sync::Arc;

pub use self::persistent_state_keeper::PersistentStateKeeper;
use super::BlockHandleStorage;

mod persistent_state_keeper;

pub struct RuntimeStorage {
    persistent_state_keeper: PersistentStateKeeper,
}

impl RuntimeStorage {
    pub fn new(block_handle_storage: Arc<BlockHandleStorage>) -> Self {
        Self {
            persistent_state_keeper: PersistentStateKeeper::new(block_handle_storage),
        }
    }

    #[inline(always)]
    pub fn persistent_state_keeper(&self) -> &PersistentStateKeeper {
        &self.persistent_state_keeper
    }
}
