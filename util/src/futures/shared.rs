use std::cell::UnsafeCell;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::task::{Context, Poll};
use tokio::sync::{AcquireError, OwnedSemaphorePermit, Semaphore, TryAcquireError};
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Shared<Fut: Future> {
    inner: Option<Arc<Inner<Fut>>>,
    permit_fut: Option<SyncBoxFuture<Result<OwnedSemaphorePermit, AcquireError>>>,
    permit: Option<OwnedSemaphorePermit>,
}
type SyncBoxFuture<T> = Pin<Box<dyn Future<Output = T> + Sync + Send + 'static>>;
impl<Fut: Future> Clone for Shared<Fut> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            permit_fut: None,
            permit: None,
        }
    }
}
impl<Fut: Future> Shared<Fut> {
    pub fn new(future: Fut) -> Self {
        let semaphore = Arc::new(Semaphore::new(1));
        let inner = Arc::new(Inner {
            state: AtomicUsize::new(POLLING),
            future_or_output: UnsafeCell::new(FutureOrOutput::Future(future)),
            semaphore,
        });
        Self {
            inner: Some(inner),
            permit_fut: None,
            permit: None,
        }
    }
    pub fn weak_future(&self) -> Option<WeakShared<Fut>> {
        self.inner
            .as_ref()
            .map(|inner| WeakShared {
                inner: Some(Arc::downgrade(inner)),
                permit_fut: None,
                permit: None,
            })
    }
    pub fn downgrade(&self) -> Option<WeakSharedHandle<Fut>> {
        self.inner.as_ref().map(|inner| WeakSharedHandle(Arc::downgrade(inner)))
    }
    /// Drops the future, returning whether it was the last instance.
    pub fn consume(mut self) -> bool {
        self.inner
            .take()
            .map(|inner| Arc::into_inner(inner).is_some())
            .unwrap_or_default()
    }
}
fn poll_impl<'cx, Fut>(
    this_inner: &mut Option<Arc<Inner<Fut>>>,
    this_permit_fut: &mut Option<
        SyncBoxFuture<Result<OwnedSemaphorePermit, AcquireError>>,
    >,
    this_permit: &mut Option<OwnedSemaphorePermit>,
    cx: &mut Context<'cx>,
) -> Poll<(Fut::Output, bool)>
where
    Fut: Future,
    Fut::Output: Clone,
{
    let inner = this_inner.take().expect("Shared future polled again after completion");
    if inner.state.load(Ordering::Acquire) == COMPLETE {
        return unsafe { Poll::Ready(inner.take_or_clone_output()) };
    }
    if this_permit.is_none() {
        *this_permit = Some('permit: {
            let permit_fut = if let Some(fut) = this_permit_fut.as_mut() {
                fut
            } else {
                match Arc::clone(&inner.semaphore).try_acquire_owned() {
                    Ok(permit) => break 'permit permit,
                    Err(TryAcquireError::NoPermits) => {}
                    Err(TryAcquireError::Closed) => unreachable!(),
                }
                let next_fut = Arc::clone(&inner.semaphore).acquire_owned();
                this_permit_fut.get_or_insert(Box::pin(next_fut))
            };
            match permit_fut.as_mut().poll(cx) {
                Poll::Pending => {
                    *this_inner = Some(inner);
                    return Poll::Pending;
                }
                Poll::Ready(Ok(permit)) => {
                    *this_permit_fut = None;
                    permit
                }
                Poll::Ready(Err(_e)) => unreachable!(),
            }
        });
    }
    assert!(this_permit_fut.is_none(), "permit already acquired");
    match inner.state.load(Ordering::Acquire) {
        COMPLETE => {
            return unsafe { Poll::Ready(inner.take_or_clone_output()) };
        }
        POISONED => panic!("inner future panicked during poll"),
        _ => {}
    }
    struct Reset<'a> {
        state: &'a AtomicUsize,
        did_not_panic: bool,
    }
    impl Drop for Reset<'_> {
        fn drop(&mut self) {
            if !self.did_not_panic {
                self.state.store(POISONED, Ordering::Release);
            }
        }
    }
    let mut reset = Reset {
        state: &inner.state,
        did_not_panic: false,
    };
    let output = {
        let future = unsafe {
            match &mut *inner.future_or_output.get() {
                FutureOrOutput::Future(fut) => Pin::new_unchecked(fut),
                FutureOrOutput::Output(_) => unreachable!(),
            }
        };
        let poll_result = future.poll(cx);
        reset.did_not_panic = true;
        match poll_result {
            Poll::Pending => {
                drop(reset);
                *this_inner = Some(inner);
                return Poll::Pending;
            }
            Poll::Ready(output) => output,
        }
    };
    unsafe {
        *inner.future_or_output.get() = FutureOrOutput::Output(output);
    }
    inner.state.store(COMPLETE, Ordering::Release);
    drop(reset);
    unsafe { Poll::Ready(inner.take_or_clone_output()) }
}
impl<Fut> Future for Shared<Fut>
where
    Fut: Future,
    Fut::Output: Clone,
{
    type Output = (Fut::Output, bool);
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Shared { inner, permit_fut, permit } = &mut *self;
        poll_impl(inner, permit_fut, permit, cx)
    }
}
/// A future that preserves its place in wait queue but does not own a shared future.
/// Use [`WeakSharedHandle`] if you want to poll an upgraded future and only pass a weak ref around.
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct WeakShared<Fut: Future> {
    inner: Option<Weak<Inner<Fut>>>,
    permit_fut: Option<SyncBoxFuture<Result<OwnedSemaphorePermit, AcquireError>>>,
    permit: Option<OwnedSemaphorePermit>,
}
impl<Fut> Future for WeakShared<Fut>
where
    Fut: Future,
    Fut::Output: Clone,
{
    type Output = Option<(Fut::Output, bool)>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let WeakShared { inner, permit_fut, permit } = &mut *self;
        let weak_inner = inner
            .take()
            .expect("Weak shared future polled again after completion");
        let mut strong_inner = weak_inner.upgrade();
        if strong_inner.is_none() {
            return Poll::Ready(None);
        }
        let poll_result = poll_impl(&mut strong_inner, permit_fut, permit, cx);
        *inner = strong_inner.is_some().then_some(weak_inner);
        poll_result.map(Some)
    }
}
/// A handle can be upgraded to a shared future, but cannot be directly awaited.
/// Use [`WeakShared`] if you want to poll without an upgrade.
#[repr(transparent)]
pub struct WeakSharedHandle<Fut: Future>(Weak<Inner<Fut>>);
impl<Fut: Future> WeakSharedHandle<Fut> {
    pub fn upgrade(&self) -> Option<Shared<Fut>> {
        self.0
            .upgrade()
            .map(|inner| Shared {
                inner: Some(inner),
                permit_fut: None,
                permit: None,
            })
    }
    pub fn strong_count(&self) -> usize {
        self.0.strong_count()
    }
}
struct Inner<Fut: Future> {
    state: AtomicUsize,
    future_or_output: UnsafeCell<FutureOrOutput<Fut>>,
    semaphore: Arc<Semaphore>,
}
impl<Fut> Inner<Fut>
where
    Fut: Future,
    Fut::Output: Clone,
{
    /// Safety: callers must first ensure that `inner.state`
    /// is `COMPLETE`
    unsafe fn take_or_clone_output(self: Arc<Self>) -> (Fut::Output, bool) {
        match Arc::try_unwrap(self) {
            Ok(inner) => {
                match inner.future_or_output.into_inner() {
                    FutureOrOutput::Output(item) => (item, true),
                    FutureOrOutput::Future(_) => unreachable!(),
                }
            }
            Err(inner) => {
                match unsafe { &*inner.future_or_output.get() } {
                    FutureOrOutput::Output(item) => (item.clone(), false),
                    FutureOrOutput::Future(_) => unreachable!(),
                }
            }
        }
    }
}
unsafe impl<Fut> Send for Inner<Fut>
where
    Fut: Future + Send,
    Fut::Output: Send + Sync,
{}
unsafe impl<Fut> Sync for Inner<Fut>
where
    Fut: Future + Send,
    Fut::Output: Send + Sync,
{}
enum FutureOrOutput<Fut: Future> {
    Future(Fut),
    Output(Fut::Output),
}
const POLLING: usize = 0;
const COMPLETE: usize = 2;
const POISONED: usize = 3;
#[cfg(test)]
mod tests {
    //! Addresses the original `Shared` futures issue:
    //! <https://github.com/rust-lang/futures-rs/issues/2706/>
    use futures_util::FutureExt;
    use super::*;
    async fn yield_now() {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(yield_now)),
            file!(),
            326u32,
        );
        /// Yield implementation
        struct YieldNow {
            yielded: bool,
        }
        impl Future for YieldNow {
            type Output = ();
            fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
                if self.yielded {
                    {
                        __guard.end_section(337u32);
                        return Poll::Ready(());
                    };
                }
                self.yielded = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
        {
            __guard.end_section(346u32);
            let __result = YieldNow { yielded: false }.await;
            __guard.start_section(346u32);
            __result
        };
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn must_not_hang_up() {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(must_not_hang_up)),
            file!(),
            350u32,
        );
        for _ in 0..200 {
            __guard.checkpoint(351u32);
            for _ in 0..1000 {
                __guard.checkpoint(352u32);
                {
                    __guard.end_section(353u32);
                    let __result = test_fut().await;
                    __guard.start_section(353u32);
                    __result
                };
            }
        }
        println!();
    }
    async fn test_fut() {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(test_fut)),
            file!(),
            359u32,
        );
        let f1 = Shared::new(yield_now());
        let f2 = f1.clone();
        let x1 = tokio::spawn(async move {
            let mut __guard = crate::__async_profile_guard__::Guard::new(
                concat!(module_path!(), "::async_block"),
                file!(),
                362u32,
            );
            f1.now_or_never();
        });
        let x2 = tokio::spawn(async move {
            let mut __guard = crate::__async_profile_guard__::Guard::new(
                concat!(module_path!(), "::async_block"),
                file!(),
                365u32,
            );
            {
                __guard.end_section(366u32);
                let __result = f2.await;
                __guard.start_section(366u32);
                __result
            };
        });
        {
            __guard.end_section(368u32);
            let __result = x1.await;
            __guard.start_section(368u32);
            __result
        }
            .ok();
        {
            __guard.end_section(369u32);
            let __result = x2.await;
            __guard.start_section(369u32);
            __result
        }
            .ok();
    }
}
