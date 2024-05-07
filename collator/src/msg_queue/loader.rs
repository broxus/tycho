use anyhow::Result;

use super::cache_persistent::PersistentCacheService;
use super::queue::MessageQueueImpl;
use super::state_persistent::PersistentStateService;
use super::storage::StorageService;

// This code part contains the logic of messages loading to the queue state,
// including lazy loading, etc.
//
// We use partials just to separate the codebase on smaller and easier maintainable parts.
impl<CH, ST, DB> MessageQueueImpl<CH, ST, DB>
where
    CH: PersistentCacheService,
    ST: PersistentStateService,
    DB: StorageService,
{
    fn some_internal_method_for_loading_logic(&mut self) -> Result<()> {
        todo!()
    }
    pub(super) fn some_module_internal_method_for_loading_logic(&mut self) -> Result<()> {
        todo!()
    }
}
