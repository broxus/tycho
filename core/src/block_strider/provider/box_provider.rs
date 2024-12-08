use std::sync::atomic::{AtomicPtr, Ordering};

use everscale_types::models::BlockId;
use futures_util::future::BoxFuture;
use futures_util::FutureExt;
use tycho_block_util::block::BlockIdRelation;

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
    type GetNextBlockFut<'a> = BlockFut<'a>;
    type GetBlockFut<'a> = BlockFut<'a>;

    fn get_next_block<'a>(&'a self, prev_block_id: &'a BlockId) -> Self::GetNextBlockFut<'a> {
        unsafe { (self.vtable.get_next_block)(&self.data, prev_block_id) }
    }

    fn get_block<'a>(&'a self, block_id_relation: &'a BlockIdRelation) -> Self::GetBlockFut<'a> {
        unsafe { (self.vtable.get_block)(&self.data, block_id_relation) }
    }
}

impl Drop for BoxBlockProvider {
    fn drop(&mut self) {
        unsafe { (self.vtable.drop)(&mut self.data) }
    }
}

// Vtable must enforce this behavior
unsafe impl Send for BoxBlockProvider {}
unsafe impl Sync for BoxBlockProvider {}

struct Vtable {
    get_next_block: GetNextBlockFn,
    get_block: GetBlockFn,
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
            drop: |ptr| {
                drop(unsafe { Box::<P>::from_raw(ptr.get_mut().cast::<P>()) });
            },
        }
    }
}

type GetNextBlockFn = for<'a> unsafe fn(&AtomicPtr<()>, &'a BlockId) -> BlockFut<'a>;
type GetBlockFn = for<'a> unsafe fn(&AtomicPtr<()>, &'a BlockIdRelation) -> BlockFut<'a>;
type DropFn = unsafe fn(&mut AtomicPtr<()>);

type BlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;

    use anyhow::Result;
    use tycho_block_util::block::BlockIdExt;

    use super::*;

    #[tokio::test]
    async fn boxed_provider_works() -> Result<()> {
        struct ProviderState {
            get_next_called: AtomicUsize,
            get_called: AtomicUsize,
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

            fn get_next_block<'a>(&'a self, _: &'a BlockId) -> Self::GetNextBlockFut<'a> {
                self.state.get_next_called.fetch_add(1, Ordering::Relaxed);
                futures_util::future::ready(None)
            }

            fn get_block<'a>(&'a self, _: &'a BlockIdRelation) -> Self::GetBlockFut<'a> {
                self.state.get_called.fetch_add(1, Ordering::Relaxed);
                futures_util::future::ready(None)
            }
        }

        let state = Arc::new(ProviderState {
            get_next_called: AtomicUsize::new(0),
            get_called: AtomicUsize::new(0),
            dropped: AtomicUsize::new(0),
        });
        let boxed = BoxBlockProvider::new(TestProvider {
            state: state.clone(),
        });

        assert_eq!(state.get_next_called.load(Ordering::Acquire), 0);
        assert_eq!(state.get_called.load(Ordering::Acquire), 0);
        assert_eq!(state.dropped.load(Ordering::Acquire), 0);

        let mc_block_id = BlockId::default();
        assert!(boxed.get_next_block(&mc_block_id).await.is_none());
        assert_eq!(state.get_next_called.load(Ordering::Acquire), 1);
        assert_eq!(state.get_called.load(Ordering::Acquire), 0);
        assert_eq!(state.dropped.load(Ordering::Acquire), 0);

        assert!(boxed.get_next_block(&mc_block_id).await.is_none());
        assert_eq!(state.get_next_called.load(Ordering::Acquire), 2);
        assert_eq!(state.get_called.load(Ordering::Acquire), 0);
        assert_eq!(state.dropped.load(Ordering::Acquire), 0);

        let relation = mc_block_id.relative_to_self();
        assert!(boxed.get_block(&relation).await.is_none());
        assert_eq!(state.get_next_called.load(Ordering::Acquire), 2);
        assert_eq!(state.get_called.load(Ordering::Acquire), 1);
        assert_eq!(state.dropped.load(Ordering::Acquire), 0);

        assert!(boxed.get_block(&relation).await.is_none());
        assert_eq!(state.get_next_called.load(Ordering::Acquire), 2);
        assert_eq!(state.get_called.load(Ordering::Acquire), 2);
        assert_eq!(state.dropped.load(Ordering::Acquire), 0);

        assert_eq!(Arc::strong_count(&state), 2);
        drop(boxed);

        assert_eq!(state.get_next_called.load(Ordering::Acquire), 2);
        assert_eq!(state.get_called.load(Ordering::Acquire), 2);
        assert_eq!(state.dropped.load(Ordering::Acquire), 1);

        assert_eq!(Arc::strong_count(&state), 1);

        Ok(())
    }
}
