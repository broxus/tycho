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
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(set_enabled)),
            file!(),
            13u32,
        );
        let enabled = enabled;
        {
            __guard.end_section(14u32);
            let __result = T::set_enabled(self, enabled).await;
            __guard.start_section(14u32);
            __result
        }
    }
    async fn dump(&self) -> Result<Vec<u8>> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(dump)),
            file!(),
            17u32,
        );
        {
            __guard.end_section(18u32);
            let __result = T::dump(self).await;
            __guard.start_section(18u32);
            __result
        }
    }
}
#[derive(Debug, Clone, Copy)]
pub struct StubMemoryProfiler;
#[async_trait::async_trait]
impl MemoryProfiler for StubMemoryProfiler {
    async fn set_enabled(&self, _: bool) -> bool {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(set_enabled)),
            file!(),
            27u32,
        );
        false
    }
    async fn dump(&self) -> Result<Vec<u8>> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(dump)),
            file!(),
            31u32,
        );
        anyhow::bail!("stub memory profiler does not support dumping data")
    }
}
