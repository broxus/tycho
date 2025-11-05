use std::sync::Arc;

use anyhow::Result;

#[async_trait::async_trait]
pub trait MemoryProfiler: Send + Sync + 'static {
    async fn set_enabled(&self, enabled: bool) -> bool;
    async fn dump(&self) -> Result<Vec<u8>>;
}

#[async_trait::async_trait]
impl<T: MemoryProfiler> MemoryProfiler for Arc<T> {
    async fn set_enabled(&self, enabled: bool) -> bool {
        T::set_enabled(self, enabled).await
    }

    async fn dump(&self) -> Result<Vec<u8>> {
        T::dump(self).await
    }
}

#[derive(Debug, Clone, Copy)]
pub struct StubMemoryProfiler;

#[async_trait::async_trait]
impl MemoryProfiler for StubMemoryProfiler {
    async fn set_enabled(&self, _: bool) -> bool {
        false
    }

    async fn dump(&self) -> Result<Vec<u8>> {
        anyhow::bail!("stub memory profiler does not support dumping data")
    }
}
