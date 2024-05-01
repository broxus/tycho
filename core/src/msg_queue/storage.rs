use std::fmt::Debug;

use anyhow::Result;

use super::{
    cache_persistent::PersistentCacheService, state_persistent::PersistentStateService,
    MessageQueueImpl,
};

pub trait StorageService: Debug + Sized {
    fn new() -> Result<Self>;
}

/*
This part of the code contains logic of working with storage.

We use partials just to separate the codebase on smaller and easier maintainable parts.
 */
impl<CH, ST, DB> MessageQueueImpl<CH, ST, DB>
where
    CH: PersistentCacheService,
    ST: PersistentStateService,
    DB: StorageService,
{
    fn some_internal_method_for_storage(&mut self) -> Result<()> {
        todo!()
    }
    pub(super) fn some_module_internal_method_for_storage(&mut self) -> Result<()> {
        todo!()
    }
}

// STUBS

#[derive(Debug)]
pub struct StorageServiceStubImpl {}
impl StorageService for StorageServiceStubImpl {
    fn new() -> Result<Self> {
        Ok(Self {})
    }
}
