use std::any::Any;
use std::sync::atomic::{AtomicPtr, Ordering};

use anyhow::Result;
use futures_util::FutureExt;
use futures_util::future::BoxFuture;

use super::{StateSubscriber, StateSubscriberContext};
use crate::block_strider::subscriber::{BlockSubscriber, BlockSubscriberContext};

// === Boxed BlockSubscriber ===

pub struct BoxBlockSubscriber {
    data: AtomicPtr<()>,
    vtable: &'static BlockVtable,
}

impl BoxBlockSubscriber {
    pub fn new<S: BlockSubscriber>(subscriber: S) -> Self {
        let ptr = Box::into_raw(Box::new(subscriber));

        Self {
            data: AtomicPtr::new(ptr.cast()),
            vtable: const { BlockVtable::new::<S>() },
        }
    }
}

impl BlockSubscriber for BoxBlockSubscriber {
    type Prepared = BoxPrepared;

    type PrepareBlockFut<'a> = PrepareBlockFut<'a>;
    type HandleBlockFut<'a> = HandleBlockFut<'a>;

    #[inline]
    fn prepare_block<'a>(&'a self, cx: &'a BlockSubscriberContext) -> Self::PrepareBlockFut<'a> {
        unsafe { (self.vtable.prepare_block)(&self.data, cx) }
    }

    #[inline]
    fn handle_block<'a>(
        &'a self,
        cx: &'a BlockSubscriberContext,
        prepared: Self::Prepared,
    ) -> Self::HandleBlockFut<'a> {
        unsafe { (self.vtable.handle_block)(&self.data, cx, prepared) }
    }
}

impl Drop for BoxBlockSubscriber {
    fn drop(&mut self) {
        unsafe { (self.vtable.drop)(&mut self.data) }
    }
}

// Vtable must enforce this behavior
unsafe impl Send for BoxBlockSubscriber {}
unsafe impl Sync for BoxBlockSubscriber {}

struct BlockVtable {
    prepare_block: PrepareBlockFn,
    handle_block: HandleBlockFn,
    drop: DropFn,
}

impl BlockVtable {
    const fn new<S: BlockSubscriber>() -> &'static Self {
        &Self {
            prepare_block: |ptr, cx| {
                let subscriber = unsafe { &*ptr.load(Ordering::Relaxed).cast::<S>() };
                subscriber
                    .prepare_block(cx)
                    .map(|result| result.map(|data| Box::new(data) as BoxPrepared))
                    .boxed()
            },
            handle_block: |ptr, cx, prepared| {
                let subscriber = unsafe { &*ptr.load(Ordering::Relaxed).cast::<S>() };
                match prepared.downcast::<S::Prepared>() {
                    Ok(prepared) => subscriber.handle_block(cx, *prepared).boxed(),
                    Err(_) => Box::pin(futures_util::future::ready(Err(anyhow::Error::from(
                        PreparedTypeMismatch,
                    )))),
                }
            },
            drop: |ptr| {
                drop(unsafe { Box::<S>::from_raw(ptr.get_mut().cast::<S>()) });
            },
        }
    }
}

type BoxPrepared = Box<dyn Any + Send>;

type PrepareBlockFn =
    for<'a> unsafe fn(&AtomicPtr<()>, &'a BlockSubscriberContext) -> PrepareBlockFut<'a>;
type HandleBlockFn = for<'a> unsafe fn(
    &AtomicPtr<()>,
    &'a BlockSubscriberContext,
    BoxPrepared,
) -> HandleBlockFut<'a>;

type PrepareBlockFut<'a> = BoxFuture<'a, Result<BoxPrepared>>;
type HandleBlockFut<'a> = BoxFuture<'a, Result<()>>;

#[derive(thiserror::Error, Debug)]
#[error("prepared type mismatch")]
struct PreparedTypeMismatch;

// === Boxed StateSubscriber ===

pub struct BoxStateSubscriber {
    data: AtomicPtr<()>,
    vtable: &'static StateVtable,
}

impl BoxStateSubscriber {
    pub fn new<S: StateSubscriber>(subscriber: S) -> Self {
        let ptr = Box::into_raw(Box::new(subscriber));

        Self {
            data: AtomicPtr::new(ptr.cast()),
            vtable: const { StateVtable::new::<S>() },
        }
    }
}

impl StateSubscriber for BoxStateSubscriber {
    type HandleStateFut<'a> = HandleStateFut<'a>;

    #[inline]
    fn handle_state<'a>(&'a self, cx: &'a StateSubscriberContext) -> Self::HandleStateFut<'a> {
        unsafe { (self.vtable.handle_state)(&self.data, cx) }
    }
}

impl Drop for BoxStateSubscriber {
    fn drop(&mut self) {
        unsafe { (self.vtable.drop)(&mut self.data) }
    }
}

// Vtable must enforce this behavior
unsafe impl Send for BoxStateSubscriber {}
unsafe impl Sync for BoxStateSubscriber {}

struct StateVtable {
    handle_state: HandleStateFn,
    drop: DropFn,
}

impl StateVtable {
    const fn new<S: StateSubscriber>() -> &'static Self {
        &Self {
            handle_state: |ptr, cx| {
                let provider = unsafe { &*ptr.load(Ordering::Relaxed).cast::<S>() };
                provider.handle_state(cx).boxed()
            },
            drop: |ptr| {
                drop(unsafe { Box::<S>::from_raw(ptr.get_mut().cast::<S>()) });
            },
        }
    }
}

type HandleStateFn =
    for<'a> unsafe fn(&AtomicPtr<()>, &'a StateSubscriberContext) -> HandleStateFut<'a>;

type HandleStateFut<'a> = BoxFuture<'a, Result<()>>;

// === Common Stuff ===

type DropFn = unsafe fn(&mut AtomicPtr<()>);

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;

    use tycho_block_util::archive::ArchiveData;
    use tycho_block_util::block::BlockStuff;
    use tycho_block_util::state::{MinRefMcStateTracker, ShardStateStuff};
    use tycho_types::cell::{Cell, CellBuilder, CellFamily, Lazy};
    use tycho_types::models::{ShardIdent, ShardStateUnsplit};

    use super::*;
    use crate::block_strider::DelayedTasks;

    #[tokio::test]
    async fn boxed_block_subscriber_works() -> Result<()> {
        struct SubscriberState {
            prepare_block_called: AtomicUsize,
            handle_block_called: AtomicUsize,
            dropped: AtomicUsize,
        }

        #[derive(Debug, PartialEq, Eq)]
        struct Prepared(u32);

        struct Subscriber {
            state: Arc<SubscriberState>,
        }

        impl Drop for Subscriber {
            fn drop(&mut self) {
                self.state.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }

        impl BlockSubscriber for Subscriber {
            type Prepared = Prepared;
            type PrepareBlockFut<'a> = futures_util::future::Ready<Result<Self::Prepared>>;
            type HandleBlockFut<'a> = futures_util::future::Ready<Result<()>>;

            fn prepare_block<'a>(
                &'a self,
                _cx: &'a BlockSubscriberContext,
            ) -> Self::PrepareBlockFut<'a> {
                self.state
                    .prepare_block_called
                    .fetch_add(1, Ordering::Relaxed);
                futures_util::future::ready(Ok(Prepared(123)))
            }

            fn handle_block<'a>(
                &'a self,
                _cx: &'a BlockSubscriberContext,
                _prepared: Self::Prepared,
            ) -> Self::HandleBlockFut<'a> {
                self.state
                    .handle_block_called
                    .fetch_add(1, Ordering::Relaxed);
                futures_util::future::ready(Ok(()))
            }
        }

        let state = Arc::new(SubscriberState {
            prepare_block_called: AtomicUsize::new(0),
            handle_block_called: AtomicUsize::new(0),
            dropped: AtomicUsize::new(0),
        });
        let boxed = BoxBlockSubscriber::new(Subscriber {
            state: state.clone(),
        });

        let cx = BlockSubscriberContext {
            mc_block_id: Default::default(),
            mc_is_key_block: false,
            is_key_block: false,
            block: BlockStuff::new_empty(ShardIdent::MASTERCHAIN, 0),
            archive_data: ArchiveData::Existing,
            delayed: DelayedTasks::new().1,
        };

        assert_eq!(state.prepare_block_called.load(Ordering::Relaxed), 0);
        assert_eq!(state.handle_block_called.load(Ordering::Relaxed), 0);
        assert_eq!(state.dropped.load(Ordering::Relaxed), 0);

        for i in 0..2 {
            let res = boxed.prepare_block(&cx).await.unwrap();
            assert_eq!(res.downcast_ref::<Prepared>(), Some(&Prepared(123)));
            assert_eq!(state.prepare_block_called.load(Ordering::Relaxed), i + 1);
            assert_eq!(state.handle_block_called.load(Ordering::Relaxed), i);
            assert_eq!(state.dropped.load(Ordering::Relaxed), 0);

            boxed.handle_block(&cx, res).await.unwrap();
            assert_eq!(state.prepare_block_called.load(Ordering::Relaxed), i + 1);
            assert_eq!(state.handle_block_called.load(Ordering::Relaxed), i + 1);
            assert_eq!(state.dropped.load(Ordering::Relaxed), 0);
        }

        assert_eq!(Arc::strong_count(&state), 2);
        drop(boxed);

        assert_eq!(state.prepare_block_called.load(Ordering::Acquire), 2);
        assert_eq!(state.handle_block_called.load(Ordering::Acquire), 2);
        assert_eq!(state.dropped.load(Ordering::Acquire), 1);

        assert_eq!(Arc::strong_count(&state), 1);

        Ok(())
    }

    #[tokio::test]
    async fn boxed_state_subscriber_works() -> Result<()> {
        struct SubscriberState {
            handle_state_called: AtomicUsize,
            dropped: AtomicUsize,
        }

        struct Subscriber {
            state: Arc<SubscriberState>,
        }

        impl Drop for Subscriber {
            fn drop(&mut self) {
                self.state.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }

        impl StateSubscriber for Subscriber {
            type HandleStateFut<'a> = futures_util::future::Ready<Result<()>>;

            fn handle_state<'a>(
                &'a self,
                _cx: &'a StateSubscriberContext,
            ) -> Self::HandleStateFut<'a> {
                self.state
                    .handle_state_called
                    .fetch_add(1, Ordering::Relaxed);
                futures_util::future::ready(Ok(()))
            }
        }

        let state = Arc::new(SubscriberState {
            handle_state_called: AtomicUsize::new(0),
            dropped: AtomicUsize::new(0),
        });
        let boxed = BoxStateSubscriber::new(Subscriber {
            state: state.clone(),
        });

        let cx = StateSubscriberContext {
            mc_block_id: Default::default(),
            mc_is_key_block: false,
            is_key_block: false,
            block: BlockStuff::new_empty(ShardIdent::MASTERCHAIN, 0),
            archive_data: ArchiveData::Existing,
            state: ShardStateStuff::from_root(
                &Default::default(),
                CellBuilder::build_from(ShardStateUnsplit {
                    global_id: 0,
                    shard_ident: ShardIdent::MASTERCHAIN,
                    seqno: 0,
                    vert_seqno: 0,
                    gen_utime: 0,
                    gen_utime_ms: 0,
                    gen_lt: 0,
                    min_ref_mc_seqno: 0,
                    processed_upto: Lazy::from_raw(Cell::empty_cell())?,
                    before_split: false,
                    accounts: Lazy::from_raw(Cell::empty_cell())?,
                    overload_history: 0,
                    underload_history: 0,
                    total_balance: Default::default(),
                    total_validator_fees: Default::default(),
                    libraries: Default::default(),
                    master_ref: None,
                    custom: None,
                })?,
                MinRefMcStateTracker::new().insert_untracked(),
            )?,
            delayed: DelayedTasks::new().1,
        };

        assert_eq!(state.handle_state_called.load(Ordering::Relaxed), 0);
        assert_eq!(state.dropped.load(Ordering::Relaxed), 0);

        for i in 0..2 {
            boxed.handle_state(&cx).await.unwrap();
            assert_eq!(state.handle_state_called.load(Ordering::Relaxed), i + 1);
            assert_eq!(state.dropped.load(Ordering::Relaxed), 0);
        }

        assert_eq!(Arc::strong_count(&state), 2);
        drop(boxed);

        assert_eq!(state.handle_state_called.load(Ordering::Acquire), 2);
        assert_eq!(state.dropped.load(Ordering::Acquire), 1);

        assert_eq!(Arc::strong_count(&state), 1);

        Ok(())
    }
}
