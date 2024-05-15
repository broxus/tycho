use std::any::Any;
use std::fmt::Debug;

use anyhow::{anyhow, Result};

use super::queue::MessageQueueImpl;
use super::state_persistent::PersistentStateService;
use super::storage::StorageService;

#[cfg(test)]
#[path = "tests/test_cache_persistent.rs"]
pub(super) mod tests;

pub trait PersistentCacheService: Debug {
    fn new(config: &dyn PersistentCacheConfig) -> Result<Self>
    where
        Self: Sized;
}

pub trait PersistentCacheConfig: Debug {
    fn as_any(&self) -> &dyn Any;
}

// This part of the code contains logic of working with persistent cache.
//
// We use partials just to separate the codebase on smaller and easier maintainable parts.
impl<CH, ST, DB> MessageQueueImpl<CH, ST, DB>
where
    CH: PersistentCacheService,
    ST: PersistentStateService,
    DB: StorageService,
{
    fn some_internal_method_for_persistent_cache(&mut self) -> Result<()> {
        todo!()
    }
    pub(super) fn some_moddule_internal_method_for_persistent_cache(&mut self) -> Result<()> {
        todo!()
    }
}

// STUBS

#[derive(Debug)]
pub struct PersistentCacheServiceStubImpl {
    pub config: PersistentCacheConfigStubImpl,
}
impl PersistentCacheService for PersistentCacheServiceStubImpl {
    fn new(config: &dyn PersistentCacheConfig) -> Result<Self> {
        let config = config
            .as_any()
            .downcast_ref::<PersistentCacheConfigStubImpl>()
            .ok_or(anyhow!("error"))?
            .clone();

        let ret = Self { config };

        Ok(ret)
    }
}

#[derive(Debug, Clone)]
pub struct PersistentCacheConfigStubImpl {
    pub cfg_value1: String,
}
impl PersistentCacheConfig for PersistentCacheConfigStubImpl {
    fn as_any(&self) -> &dyn Any {
        self
    }
}
