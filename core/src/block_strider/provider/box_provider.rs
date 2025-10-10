use std::sync::atomic::{AtomicPtr, Ordering};
use anyhow::Result;
use futures_util::FutureExt;
use futures_util::future::BoxFuture;
use tycho_block_util::block::BlockIdRelation;
use tycho_types::models::BlockId;
use crate::block_strider::provider::{BlockProvider, OptionalBlockStuff};
pub struct BoxBlockProvider {
    data: AtomicPtr<()>,
    vtable: &'static Vtable,
}
impl BoxBlockProvider {
    pub fn new<P>(provider: P) -> Self
    where
        P: BlockProvider,
    {
        let ptr = Box::into_raw(Box::new(provider));
        Self {
            data: AtomicPtr::new(ptr.cast()),
            vtable: const { Vtable::new::<P>() },
        }
    }
}
impl BlockProvider for BoxBlockProvider {
    type GetNextBlockFut<'a> = GetBlockFut<'a>;
    type GetBlockFut<'a> = GetBlockFut<'a>;
    type CleanupFut<'a> = ClenaupFut<'a>;
    fn get_next_block<'a>(
        &'a self,
        prev_block_id: &'a BlockId,
    ) -> Self::GetNextBlockFut<'a> {
        unsafe { (self.vtable.get_next_block)(&self.data, prev_block_id) }
    }
    fn get_block<'a>(
        &'a self,
        block_id_relation: &'a BlockIdRelation,
    ) -> Self::GetBlockFut<'a> {
        unsafe { (self.vtable.get_block)(&self.data, block_id_relation) }
    }
    fn cleanup_until(&self, mc_seqno: u32) -> Self::CleanupFut<'_> {
        unsafe { (self.vtable.cleanup_until)(&self.data, mc_seqno) }
    }
}
impl Drop for BoxBlockProvider {
    fn drop(&mut self) {
        unsafe { (self.vtable.drop)(&mut self.data) }
    }
}
unsafe impl Send for BoxBlockProvider {}
unsafe impl Sync for BoxBlockProvider {}
struct Vtable {
    get_next_block: GetNextBlockFn,
    get_block: GetBlockFn,
    cleanup_until: CleanupFn,
    drop: DropFn,
}
impl Vtable {
    const fn new<P: BlockProvider>() -> &'static Self {
        &Self {
            get_next_block: |ptr, prev_block_id| {
                let provider = unsafe { &*ptr.load(Ordering::Relaxed).cast::<P>() };
                provider.get_next_block(prev_block_id).boxed()
            },
            get_block: |ptr, block_id_relation| {
                let provider = unsafe { &*ptr.load(Ordering::Relaxed).cast::<P>() };
                provider.get_block(block_id_relation).boxed()
            },
            cleanup_until: |ptr, mc_seqno| {
                let provider = unsafe { &*ptr.load(Ordering::Relaxed).cast::<P>() };
                provider.cleanup_until(mc_seqno).boxed()
            },
            drop: |ptr| {
                drop(unsafe { Box::<P>::from_raw(ptr.get_mut().cast::<P>()) });
            },
        }
    }
}
type GetNextBlockFn = for<'a> unsafe fn(&AtomicPtr<()>, &'a BlockId) -> GetBlockFut<'a>;
type GetBlockFn = for<'a> unsafe fn(
    &AtomicPtr<()>,
    &'a BlockIdRelation,
) -> GetBlockFut<'a>;
type CleanupFn = for<'a> unsafe fn(&AtomicPtr<()>, u32) -> ClenaupFut<'_>;
type DropFn = unsafe fn(&mut AtomicPtr<()>);
type GetBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;
type ClenaupFut<'a> = BoxFuture<'a, Result<()>>;
#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use anyhow::Result;
    use tycho_block_util::block::BlockIdExt;
    use super::*;
    #[tokio::test]
    async fn boxed_provider_works() -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(boxed_provider_works)),
            file!(),
            106u32,
        );
        struct ProviderState {
            get_next_called: AtomicUsize,
            get_called: AtomicUsize,
            cleanup_called: AtomicUsize,
            dropped: AtomicUsize,
        }
        struct TestProvider {
            state: Arc<ProviderState>,
        }
        impl Drop for TestProvider {
            fn drop(&mut self) {
                self.state.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
        impl BlockProvider for TestProvider {
            type GetNextBlockFut<'a> = futures_util::future::Ready<OptionalBlockStuff>;
            type GetBlockFut<'a> = futures_util::future::Ready<OptionalBlockStuff>;
            type CleanupFut<'a> = futures_util::future::Ready<Result<()>>;
            fn get_next_block<'a>(
                &'a self,
                _: &'a BlockId,
            ) -> Self::GetNextBlockFut<'a> {
                self.state.get_next_called.fetch_add(1, Ordering::Relaxed);
                futures_util::future::ready(None)
            }
            fn get_block<'a>(&'a self, _: &'a BlockIdRelation) -> Self::GetBlockFut<'a> {
                self.state.get_called.fetch_add(1, Ordering::Relaxed);
                futures_util::future::ready(None)
            }
            fn cleanup_until(&self, _: u32) -> Self::CleanupFut<'_> {
                self.state.cleanup_called.fetch_add(1, Ordering::Relaxed);
                futures_util::future::ready(Ok(()))
            }
        }
        let state = Arc::new(ProviderState {
            get_next_called: AtomicUsize::new(0),
            get_called: AtomicUsize::new(0),
            cleanup_called: AtomicUsize::new(0),
            dropped: AtomicUsize::new(0),
        });
        let boxed = BoxBlockProvider::new(TestProvider {
            state: state.clone(),
        });
        assert_eq!(state.get_next_called.load(Ordering::Acquire), 0);
        assert_eq!(state.get_called.load(Ordering::Acquire), 0);
        assert_eq!(state.cleanup_called.load(Ordering::Acquire), 0);
        assert_eq!(state.dropped.load(Ordering::Acquire), 0);
        let mc_block_id = BlockId::default();
        assert!(boxed.get_next_block(& mc_block_id). await .is_none());
        assert_eq!(state.get_next_called.load(Ordering::Acquire), 1);
        assert_eq!(state.get_called.load(Ordering::Acquire), 0);
        assert_eq!(state.cleanup_called.load(Ordering::Acquire), 0);
        assert_eq!(state.dropped.load(Ordering::Acquire), 0);
        assert!(boxed.get_next_block(& mc_block_id). await .is_none());
        assert_eq!(state.get_next_called.load(Ordering::Acquire), 2);
        assert_eq!(state.get_called.load(Ordering::Acquire), 0);
        assert_eq!(state.cleanup_called.load(Ordering::Acquire), 0);
        assert_eq!(state.dropped.load(Ordering::Acquire), 0);
        let relation = mc_block_id.relative_to_self();
        assert!(boxed.get_block(& relation). await .is_none());
        assert_eq!(state.get_next_called.load(Ordering::Acquire), 2);
        assert_eq!(state.get_called.load(Ordering::Acquire), 1);
        assert_eq!(state.cleanup_called.load(Ordering::Acquire), 0);
        assert_eq!(state.dropped.load(Ordering::Acquire), 0);
        assert!(boxed.get_block(& relation). await .is_none());
        assert_eq!(state.get_next_called.load(Ordering::Acquire), 2);
        assert_eq!(state.get_called.load(Ordering::Acquire), 2);
        assert_eq!(state.cleanup_called.load(Ordering::Acquire), 0);
        assert_eq!(state.dropped.load(Ordering::Acquire), 0);
        {
            __guard.end_section(186u32);
            let __result = boxed.cleanup_until(123).await;
            __guard.start_section(186u32);
            __result
        }
            .unwrap();
        assert_eq!(state.get_next_called.load(Ordering::Acquire), 2);
        assert_eq!(state.get_called.load(Ordering::Acquire), 2);
        assert_eq!(state.cleanup_called.load(Ordering::Acquire), 1);
        assert_eq!(state.dropped.load(Ordering::Acquire), 0);
        {
            __guard.end_section(192u32);
            let __result = boxed.cleanup_until(321).await;
            __guard.start_section(192u32);
            __result
        }
            .unwrap();
        assert_eq!(state.get_next_called.load(Ordering::Acquire), 2);
        assert_eq!(state.get_called.load(Ordering::Acquire), 2);
        assert_eq!(state.cleanup_called.load(Ordering::Acquire), 2);
        assert_eq!(state.dropped.load(Ordering::Acquire), 0);
        assert_eq!(Arc::strong_count(& state), 2);
        drop(boxed);
        assert_eq!(state.get_next_called.load(Ordering::Acquire), 2);
        assert_eq!(state.get_called.load(Ordering::Acquire), 2);
        assert_eq!(state.dropped.load(Ordering::Acquire), 1);
        assert_eq!(Arc::strong_count(& state), 1);
        Ok(())
    }
}
