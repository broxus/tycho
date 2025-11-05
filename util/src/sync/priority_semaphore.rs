//! See <https://github.com/tokio-rs/tokio/blob/c9273f1aee9927b16ee3a789a382c99ad600c8b6/tokio/src/sync/batch_semaphore.rs>.
use std::cell::UnsafeCell;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll, Waker};
use futures_util::Future;
use crate::util::linked_list::{Link, LinkedList, Pointers};
use crate::util::wake_list::WakeList;
pub struct PrioritySemaphore {
    waiters: Mutex<Waitlist>,
    permits: AtomicUsize,
}
impl PrioritySemaphore {
    const MAX_PERMITS: usize = usize::MAX >> 3;
    const CLOSED: usize = 1;
    const PERMIT_SHIFT: usize = 1;
    pub fn new(permits: usize) -> Self {
        assert!(
            permits <= Self::MAX_PERMITS,
            "a semaphore may not have more than MAX_PERMITS permits ({})",
            Self::MAX_PERMITS
        );
        Self {
            permits: AtomicUsize::new(permits << Self::PERMIT_SHIFT),
            waiters: Mutex::new(Waitlist {
                ordinary_queue: LinkedList::new(),
                priority_queue: LinkedList::new(),
                closed: false,
            }),
        }
    }
    pub const fn const_new(permits: usize) -> Self {
        assert!(permits <= Self::MAX_PERMITS);
        Self {
            permits: AtomicUsize::new(permits << Self::PERMIT_SHIFT),
            waiters: Mutex::new(Waitlist {
                ordinary_queue: LinkedList::new(),
                priority_queue: LinkedList::new(),
                closed: false,
            }),
        }
    }
    pub fn available_permits(&self) -> usize {
        self.permits.load(Ordering::Acquire) >> Self::PERMIT_SHIFT
    }
    pub fn close(&self) {
        fn clear_queue(queue: &mut LinkedList<Waiter, <Waiter as Link>::Target>) {
            while let Some(mut waiter) = queue.pop_back() {
                let waker = unsafe { (*waiter.as_mut().waker.get()).take() };
                if let Some(waker) = waker {
                    waker.wake();
                }
            }
        }
        let mut waiters = self.waiters.lock().unwrap();
        self.permits.fetch_or(Self::CLOSED, Ordering::Release);
        waiters.closed = true;
        clear_queue(&mut waiters.ordinary_queue);
        clear_queue(&mut waiters.priority_queue);
    }
    pub fn is_closed(&self) -> bool {
        self.permits.load(Ordering::Acquire) & Self::CLOSED == Self::CLOSED
    }
    pub fn try_acquire(&self) -> Result<SemaphorePermit<'_>, TryAcquireError> {
        self.try_acquire_impl(1)
            .map(|()| SemaphorePermit {
                semaphore: self,
                permits: 1,
            })
    }
    pub fn try_acquire_owned(
        self: Arc<Self>,
    ) -> Result<OwnedSemaphorePermit, TryAcquireError> {
        self.try_acquire_impl(1)
            .map(|()| OwnedSemaphorePermit {
                semaphore: self,
                permits: 1,
            })
    }
    pub async fn acquire(
        &self,
        priority: bool,
    ) -> Result<SemaphorePermit<'_>, AcquireError> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(acquire)),
            file!(),
            97u32,
        );
        let priority = priority;
        match {
            __guard.end_section(98u32);
            let __result = self.acquire_impl(1, priority).await;
            __guard.start_section(98u32);
            __result
        } {
            Ok(()) => {
                Ok(SemaphorePermit {
                    semaphore: self,
                    permits: 1,
                })
            }
            Err(e) => Err(e),
        }
    }
    pub async fn acquire_owned(
        self: Arc<Self>,
        priority: bool,
    ) -> Result<OwnedSemaphorePermit, AcquireError> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(acquire_owned)),
            file!(),
            110u32,
        );
        let priority = priority;
        match {
            __guard.end_section(111u32);
            let __result = self.acquire_impl(1, priority).await;
            __guard.start_section(111u32);
            __result
        } {
            Ok(()) => {
                Ok(OwnedSemaphorePermit {
                    semaphore: self,
                    permits: 1,
                })
            }
            Err(e) => Err(e),
        }
    }
    pub fn add_permits(&self, n: usize) {
        if n == 0 {
            return;
        }
        self.add_permits_locked(n, self.waiters.lock().unwrap());
    }
    fn try_acquire_impl(&self, num_permits: usize) -> Result<(), TryAcquireError> {
        assert!(
            num_permits <= Self::MAX_PERMITS,
            "a semaphore may not have more than MAX_PERMITS permits ({})",
            Self::MAX_PERMITS
        );
        let num_permits = num_permits << Self::PERMIT_SHIFT;
        let mut curr = self.permits.load(Ordering::Acquire);
        loop {
            if curr & Self::CLOSED == Self::CLOSED {
                return Err(TryAcquireError::Closed);
            }
            if curr < num_permits {
                return Err(TryAcquireError::NoPermits);
            }
            let next = curr - num_permits;
            match self
                .permits
                .compare_exchange(curr, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return Ok(()),
                Err(actual) => curr = actual,
            }
        }
    }
    fn acquire_impl(&self, num_permits: usize, priority: bool) -> Acquire<'_> {
        Acquire::new(self, num_permits, priority)
    }
    fn add_permits_locked(&self, mut rem: usize, waiters: MutexGuard<'_, Waitlist>) {
        let mut wakers = WakeList::new();
        let mut lock = Some(waiters);
        let mut is_empty = false;
        while rem > 0 {
            let mut waiters = lock
                .take()
                .unwrap_or_else(|| self.waiters.lock().unwrap());
            {
                let waiters = &mut *waiters;
                'inner: while wakers.can_push() {
                    let queue = 'queue: {
                        for queue in [
                            &mut waiters.priority_queue,
                            &mut waiters.ordinary_queue,
                        ] {
                            if let Some(waiter) = queue.last() {
                                if !waiter.assign_permits(&mut rem) {
                                    continue;
                                }
                                break 'queue queue;
                            }
                        }
                        is_empty = true;
                        break 'inner;
                    };
                    let mut waiter = queue.pop_back().unwrap();
                    if let Some(waker) = unsafe {
                        (*waiter.as_mut().waker.get()).take()
                    } {
                        wakers.push(waker);
                    }
                }
            }
            if rem > 0 && is_empty {
                let permits = rem;
                assert!(
                    permits <= Self::MAX_PERMITS,
                    "cannot add more than MAX_PERMITS permits ({})", Self::MAX_PERMITS
                );
                let prev = self
                    .permits
                    .fetch_add(rem << Self::PERMIT_SHIFT, Ordering::Release);
                let prev = prev >> Self::PERMIT_SHIFT;
                assert!(
                    prev + permits <= Self::MAX_PERMITS,
                    "number of added permits ({}) would overflow MAX_PERMITS ({})", rem,
                    Self::MAX_PERMITS
                );
                rem = 0;
            }
            drop(waiters);
            wakers.wake_all();
        }
        assert_eq!(rem, 0);
    }
    fn poll_acquire(
        &self,
        cx: &mut Context<'_>,
        num_permits: usize,
        node: Pin<&mut Waiter>,
        queued: bool,
        priority: bool,
    ) -> Poll<Result<(), AcquireError>> {
        let mut acquired = 0;
        let needed = if queued {
            node.state.load(Ordering::Acquire) << Self::PERMIT_SHIFT
        } else {
            num_permits << Self::PERMIT_SHIFT
        };
        let mut lock = None;
        let mut curr = self.permits.load(Ordering::Acquire);
        let mut waiters = loop {
            if curr & Self::CLOSED > 0 {
                return Poll::Ready(Err(AcquireError(())));
            }
            let mut remaining = 0;
            let total = curr
                .checked_add(acquired)
                .expect("number of permits must not overflow");
            let (next, acq) = if total >= needed {
                let next = curr - (needed - acquired);
                (next, needed >> Self::PERMIT_SHIFT)
            } else {
                remaining = (needed - acquired) - curr;
                (0, curr >> Self::PERMIT_SHIFT)
            };
            if remaining > 0 && lock.is_none() {
                lock = Some(self.waiters.lock().unwrap());
            }
            match self
                .permits
                .compare_exchange(curr, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    acquired += acq;
                    if remaining == 0 {
                        if !queued {
                            return Poll::Ready(Ok(()));
                        } else if lock.is_none() {
                            break self.waiters.lock().unwrap();
                        }
                    }
                    break lock.expect("lock must be acquired before waiting");
                }
                Err(actual) => curr = actual,
            }
        };
        if waiters.closed {
            return Poll::Ready(Err(AcquireError(())));
        }
        if node.assign_permits(&mut acquired) {
            self.add_permits_locked(acquired, waiters);
            return Poll::Ready(Ok(()));
        }
        assert_eq!(acquired, 0);
        let mut old_waker = None;
        {
            let waker = unsafe { &mut *node.waker.get() };
            if waker.as_ref().is_none_or(|waker| !waker.will_wake(cx.waker())) {
                old_waker = waker.replace(cx.waker().clone());
            }
        }
        if !queued {
            let node = unsafe {
                let node = Pin::into_inner_unchecked(node) as *mut _;
                NonNull::new_unchecked(node)
            };
            waiters.queue_mut(priority).push_front(node);
        }
        drop(waiters);
        drop(old_waker);
        Poll::Pending
    }
}
#[must_use]
#[clippy::has_significant_drop]
pub struct SemaphorePermit<'a> {
    semaphore: &'a PrioritySemaphore,
    permits: u32,
}
impl Drop for SemaphorePermit<'_> {
    fn drop(&mut self) {
        self.semaphore.add_permits(self.permits as usize);
    }
}
#[must_use]
#[clippy::has_significant_drop]
pub struct OwnedSemaphorePermit {
    semaphore: Arc<PrioritySemaphore>,
    permits: u32,
}
impl Drop for OwnedSemaphorePermit {
    fn drop(&mut self) {
        self.semaphore.add_permits(self.permits as usize);
    }
}
struct Acquire<'a> {
    node: Waiter,
    semaphore: &'a PrioritySemaphore,
    num_permits: usize,
    queued: bool,
    priority: bool,
}
impl<'a> Acquire<'a> {
    fn new(
        semaphore: &'a PrioritySemaphore,
        num_permits: usize,
        priority: bool,
    ) -> Self {
        Self {
            node: Waiter::new(num_permits),
            semaphore,
            num_permits,
            queued: false,
            priority,
        }
    }
    fn project(
        self: Pin<&mut Self>,
    ) -> (Pin<&mut Waiter>, &PrioritySemaphore, usize, &mut bool, bool) {
        fn is_unpin<T: Unpin>() {}
        unsafe {
            is_unpin::<&PrioritySemaphore>();
            is_unpin::<&mut bool>();
            is_unpin::<usize>();
            let this = self.get_unchecked_mut();
            (
                Pin::new_unchecked(&mut this.node),
                this.semaphore,
                this.num_permits,
                &mut this.queued,
                this.priority,
            )
        }
    }
}
impl Drop for Acquire<'_> {
    fn drop(&mut self) {
        if !self.queued {
            return;
        }
        let mut waiters = self.semaphore.waiters.lock().unwrap();
        let node = NonNull::from(&mut self.node);
        unsafe { waiters.queue_mut(self.priority).remove(node) };
        let acquired_permits = self.num_permits
            - self.node.state.load(Ordering::Acquire);
        if acquired_permits > 0 {
            self.semaphore.add_permits_locked(acquired_permits, waiters);
        }
    }
}
unsafe impl Sync for Acquire<'_> {}
impl Future for Acquire<'_> {
    type Output = Result<(), AcquireError>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let (node, semaphore, needed, queued, priority) = self.project();
        match semaphore.poll_acquire(cx, needed, node, *queued, priority) {
            Poll::Pending => {
                *queued = true;
                Poll::Pending
            }
            Poll::Ready(r) => {
                r?;
                *queued = false;
                Poll::Ready(Ok(()))
            }
        }
    }
}
#[derive(Debug, thiserror::Error)]
#[error("semaphore closed")]
pub struct AcquireError(());
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum TryAcquireError {
    /// The semaphore has been [closed] and cannot issue new permits.
    ///
    /// [closed]: crate::sync::PrioritySemaphore::close
    #[error("semaphore closed")]
    Closed,
    /// The semaphore has no available permits.
    #[error("no permits available")]
    NoPermits,
}
struct Waitlist {
    ordinary_queue: LinkedList<Waiter, <Waiter as Link>::Target>,
    priority_queue: LinkedList<Waiter, <Waiter as Link>::Target>,
    closed: bool,
}
impl Waitlist {
    fn queue_mut(
        &mut self,
        priority: bool,
    ) -> &mut LinkedList<Waiter, <Waiter as Link>::Target> {
        if priority { &mut self.priority_queue } else { &mut self.ordinary_queue }
    }
}
struct Waiter {
    state: AtomicUsize,
    waker: UnsafeCell<Option<Waker>>,
    pointers: Pointers<Waiter>,
    _pin: PhantomPinned,
}
impl Waiter {
    fn new(num_permits: usize) -> Self {
        Waiter {
            state: AtomicUsize::new(num_permits),
            waker: UnsafeCell::new(None),
            pointers: Pointers::new(),
            _pin: PhantomPinned,
        }
    }
    /// Assign permits to the waiter.
    ///
    /// Returns `true` if the waiter should be removed from the queue
    fn assign_permits(&self, n: &mut usize) -> bool {
        let mut curr = self.state.load(Ordering::Acquire);
        loop {
            let assign = std::cmp::min(curr, *n);
            let next = curr - assign;
            match self
                .state
                .compare_exchange(curr, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    *n -= assign;
                    return next == 0;
                }
                Err(actual) => curr = actual,
            }
        }
    }
    unsafe fn addr_of_pointers(target: NonNull<Waiter>) -> NonNull<Pointers<Self>> {
        let target = target.as_ptr();
        let field = unsafe { std::ptr::addr_of_mut!((* target).pointers) };
        unsafe { NonNull::new_unchecked(field) }
    }
}
unsafe impl Link for Waiter {
    type Handle = NonNull<Self>;
    type Target = Self;
    #[inline]
    fn as_raw(handle: &Self::Handle) -> NonNull<Self::Target> {
        *handle
    }
    #[inline]
    unsafe fn from_raw(ptr: NonNull<Self::Target>) -> Self::Handle {
        ptr
    }
    #[inline]
    unsafe fn pointers(
        target: NonNull<Self::Target>,
    ) -> NonNull<Pointers<Self::Target>> {
        unsafe { Self::addr_of_pointers(target) }
    }
}
#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;
    use super::*;
    #[tokio::test(flavor = "multi_thread")]
    async fn priority_semaphore_works() {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(priority_semaphore_works)),
            file!(),
            559u32,
        );
        let permits = Arc::new(PrioritySemaphore::new(1));
        let flag = Arc::new(AtomicBool::new(false));
        tokio::spawn({
            let permits = permits.clone();
            async move {
                let mut __guard = crate::__async_profile_guard__::Guard::new(
                    concat!(module_path!(), "::async_block"),
                    file!(),
                    566u32,
                );
                println!("BACKGROUND BEFORE");
                let _guard = {
                    __guard.end_section(568u32);
                    let __result = permits.acquire(false).await;
                    __guard.start_section(568u32);
                    __result
                }
                    .unwrap();
                println!("BACKGROUND AFTER");
                {
                    __guard.end_section(570u32);
                    let __result = tokio::time::sleep(Duration::from_millis(100)).await;
                    __guard.start_section(570u32);
                    __result
                };
                println!("BACKGROUND FINISH");
            }
        });
        {
            __guard.end_section(575u32);
            let __result = tokio::time::sleep(Duration::from_micros(10)).await;
            __guard.start_section(575u32);
            __result
        };
        let ordinary_task = tokio::spawn({
            let permits = permits.clone();
            let flag = flag.clone();
            async move {
                let mut __guard = crate::__async_profile_guard__::Guard::new(
                    concat!(module_path!(), "::async_block"),
                    file!(),
                    581u32,
                );
                println!("ORDINARY BEFORE");
                let _guard = {
                    __guard.end_section(583u32);
                    let __result = permits.acquire(false).await;
                    __guard.start_section(583u32);
                    __result
                }
                    .unwrap();
                println!("ORDINARY AFTER");
                assert!(flag.load(Ordering::Acquire));
            }
        });
        {
            __guard.end_section(590u32);
            let __result = tokio::time::sleep(Duration::from_micros(10)).await;
            __guard.start_section(590u32);
            __result
        };
        let priority_task = tokio::spawn({
            let flag = flag.clone();
            async move {
                let mut __guard = crate::__async_profile_guard__::Guard::new(
                    concat!(module_path!(), "::async_block"),
                    file!(),
                    594u32,
                );
                println!("PRIORITY BEFORE");
                let _guard = {
                    __guard.end_section(596u32);
                    let __result = permits.acquire(true).await;
                    __guard.start_section(596u32);
                    __result
                }
                    .unwrap();
                println!("PRIORITY");
                flag.store(true, Ordering::Release);
            }
        });
        {
            __guard.end_section(602u32);
            let __result = ordinary_task.await;
            __guard.start_section(602u32);
            __result
        }
            .unwrap();
        {
            __guard.end_section(603u32);
            let __result = priority_task.await;
            __guard.start_section(603u32);
            __result
        }
            .unwrap();
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn priority_semaphore_is_fair() {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(priority_semaphore_is_fair)),
            file!(),
            607u32,
        );
        let permits = Arc::new(PrioritySemaphore::new(10));
        let flag = AtomicBool::new(false);
        {
            __guard.end_section(611u32);
            tokio::join!(
                non_cooperative_task(permits, & flag), poor_little_task(& flag),
            );
            __guard.start_section(611u32);
        };
    }
    async fn non_cooperative_task(permits: Arc<PrioritySemaphore>, flag: &AtomicBool) {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(non_cooperative_task)),
            file!(),
            617u32,
        );
        let permits = permits;
        let flag = flag;
        while !flag.load(Ordering::Acquire) {
            __guard.checkpoint(618u32);
            let _permit = {
                __guard.end_section(619u32);
                let __result = permits.acquire(false).await;
                __guard.start_section(619u32);
                __result
            }
                .unwrap();
            {
                __guard.end_section(622u32);
                let __result = tokio::task::yield_now().await;
                __guard.start_section(622u32);
                __result
            };
        }
    }
    async fn poor_little_task(flag: &AtomicBool) {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(poor_little_task)),
            file!(),
            626u32,
        );
        let flag = flag;
        {
            __guard.end_section(627u32);
            let __result = tokio::time::sleep(Duration::from_secs(1)).await;
            __guard.start_section(627u32);
            __result
        };
        flag.store(true, Ordering::Release);
    }
}
