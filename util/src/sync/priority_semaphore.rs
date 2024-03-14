//! Based on https://github.com/tokio-rs/tokio/blob/e37bd6385430620f850a644d58945ace541afb6e/tokio/src/sync/batch_semaphore.rs#L550

use std::cell::UnsafeCell;
use std::marker::PhantomPinned;
use std::mem::MaybeUninit;
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::task::{Context, Poll, Waker};

use futures_util::Future;

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
        fn clear_queue(queue: &mut LinkedList) {
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

    pub fn try_acquire(&self, num_permits: usize) -> Result<(), TryAcquireError> {
        assert!(
            num_permits <= Self::MAX_PERMITS,
            "a semaphore may not have more than MAX_PERMITS permits ({})",
            Self::MAX_PERMITS
        );

        let num_permits = num_permits << Self::PERMIT_SHIFT;
        let mut curr = self.permits.load(Ordering::Acquire);
        loop {
            // Has the semaphore closed?
            if curr & Self::CLOSED == Self::CLOSED {
                return Err(TryAcquireError::Closed);
            }

            // Are there enough permits remaining?
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

    pub fn acquire(&self, num_permits: usize, priority: bool) -> Acquire<'_> {
        Acquire::new(self, num_permits, priority)
    }

    fn add_permits_locked(&self, mut rem: usize, waiters: MutexGuard<'_, Waitlist>) {
        let mut wakers = WakeList::new();
        let mut lock = Some(waiters);
        let mut is_empty = false;
        while rem > 0 {
            let mut waiters = lock.take().unwrap_or_else(|| self.waiters.lock().unwrap());

            {
                let waiters = &mut *waiters;
                'inner: while wakers.can_push() {
                    // Was the waiter assigned enough permits to wake it?
                    let queue = 'queue: {
                        for queue in [&mut waiters.priority_queue, &mut waiters.ordinary_queue] {
                            if let Some(waiter) = queue.last() {
                                if !waiter.assign_permits(&mut rem) {
                                    continue;
                                }
                                break 'queue queue;
                            }
                        }

                        is_empty = true;
                        // If we assigned permits to all the waiters in the queue, and there are
                        // still permits left over, assign them back to the semaphore.
                        break 'inner;
                    };

                    let mut waiter = queue.pop_back().unwrap();
                    if let Some(waker) = unsafe { (*waiter.as_mut().waker.get()).take() } {
                        wakers.push(waker);
                    }
                }
            }

            if rem > 0 && is_empty {
                let permits = rem;
                assert!(
                    permits <= Self::MAX_PERMITS,
                    "cannot add more than MAX_PERMITS permits ({})",
                    Self::MAX_PERMITS
                );
                let prev = self
                    .permits
                    .fetch_add(rem << Self::PERMIT_SHIFT, Ordering::Release);
                let prev = prev >> Self::PERMIT_SHIFT;
                assert!(
                    prev + permits <= Self::MAX_PERMITS,
                    "number of added permits ({}) would overflow MAX_PERMITS ({})",
                    rem,
                    Self::MAX_PERMITS
                );

                rem = 0;
            }

            drop(waiters); // release the lock

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
        // First, try to take the requested number of permits from the
        // semaphore.
        let mut curr = self.permits.load(Ordering::Acquire);
        let mut waiters = loop {
            // Has the semaphore closed?
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
                // No permits were immediately available, so this permit will
                // (probably) need to wait. We'll need to acquire a lock on the
                // wait queue before continuing. We need to do this _before_ the
                // CAS that sets the new value of the semaphore's `permits`
                // counter. Otherwise, if we subtract the permits and then
                // acquire the lock, we might miss additional permits being
                // added while waiting for the lock.
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

        // Otherwise, register the waker & enqueue the node.
        {
            // Safety: the wait list is locked, so we may modify the waker.
            let waker = unsafe { &mut *node.waker.get() };

            // Do we need to register the new waker?
            if waker
                .as_ref()
                .map_or(true, |waker| !waker.will_wake(cx.waker()))
            {
                old_waker = std::mem::replace(waker, Some(cx.waker().clone()));
            }
        }

        // If the waiter is not already in the wait queue, enqueue it.
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

pub struct Acquire<'a> {
    node: Waiter,
    semaphore: &'a PrioritySemaphore,
    num_permits: usize,
    queued: bool,
    priority: bool,
}

impl<'a> Acquire<'a> {
    fn new(semaphore: &'a PrioritySemaphore, num_permits: usize, priority: bool) -> Self {
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
            // Safety: all fields other than `node` are `Unpin`

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
        // Safety: we have locked the wait list.
        unsafe { waiters.queue_mut(self.priority).remove(node) };

        let acquired_permits = self.num_permits - self.node.state.load(Ordering::Acquire);
        if acquired_permits > 0 {
            self.semaphore.add_permits_locked(acquired_permits, waiters);
        }
    }
}

// Safety: the `Acquire` future is not `Sync` automatically because it contains
// a `Waiter`, which, in turn, contains an `UnsafeCell`. However, the
// `UnsafeCell` is only accessed when the future is borrowed mutably (either in
// `poll` or in `drop`). Therefore, it is safe (although not particularly
// _useful_) for the future to be borrowed immutably across threads.
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
    ordinary_queue: LinkedList,
    priority_queue: LinkedList,
    closed: bool,
}

impl Waitlist {
    fn queue_mut(&mut self, priority: bool) -> &mut LinkedList {
        if priority {
            &mut self.priority_queue
        } else {
            &mut self.ordinary_queue
        }
    }
}

#[derive(Default)]
struct LinkedList {
    head: Option<NonNull<Waiter>>,
    tail: Option<NonNull<Waiter>>,
}

unsafe impl Send for LinkedList {}
unsafe impl Sync for LinkedList {}

impl LinkedList {
    const fn new() -> Self {
        Self {
            head: None,
            tail: None,
        }
    }

    fn last(&self) -> Option<&Waiter> {
        let tail = self.tail.as_ref()?;
        unsafe { Some(&*tail.as_ptr()) }
    }

    fn push_front(&mut self, ptr: NonNull<Waiter>) {
        unsafe {
            Waiter::addr_of_pointers(ptr).as_mut().set_next(self.head);
            Waiter::addr_of_pointers(ptr).as_mut().set_prev(None);

            if let Some(head) = self.head {
                Waiter::addr_of_pointers(head).as_mut().set_prev(Some(ptr));
            }

            self.head = Some(ptr);

            if self.tail.is_none() {
                self.tail = Some(ptr);
            }
        }
    }

    fn pop_back(&mut self) -> Option<NonNull<Waiter>> {
        unsafe {
            let last = self.tail?;
            self.tail = Waiter::addr_of_pointers(last).as_ref().get_prev();

            if let Some(prev) = Waiter::addr_of_pointers(last).as_ref().get_prev() {
                Waiter::addr_of_pointers(prev).as_mut().set_next(None);
            } else {
                self.head = None;
            }

            Waiter::addr_of_pointers(last).as_mut().set_prev(None);
            Waiter::addr_of_pointers(last).as_mut().set_next(None);

            Some(last)
        }
    }

    /// Removes the specified node from the list
    ///
    /// # Safety
    ///
    /// The caller **must** ensure that exactly one of the following is true:
    /// - `node` is currently contained by `self`,
    /// - `node` is not contained by any list,
    /// - `node` is currently contained by some other `GuardedLinkedList` **and**
    ///   the caller has an exclusive access to that list. This condition is
    ///   used by the linked list in `sync::Notify`.
    unsafe fn remove(&mut self, node: NonNull<Waiter>) -> Option<NonNull<Waiter>> {
        if let Some(prev) = Waiter::addr_of_pointers(node).as_ref().get_prev() {
            debug_assert_eq!(
                Waiter::addr_of_pointers(prev).as_ref().get_next(),
                Some(node)
            );

            Waiter::addr_of_pointers(prev)
                .as_mut()
                .set_next(Waiter::addr_of_pointers(node).as_ref().get_next());
        } else {
            if self.head != Some(node) {
                return None;
            }

            self.head = Waiter::addr_of_pointers(node).as_ref().get_next();
        }

        if let Some(next) = Waiter::addr_of_pointers(node).as_ref().get_next() {
            debug_assert_eq!(
                Waiter::addr_of_pointers(next).as_ref().get_prev(),
                Some(node)
            );
            Waiter::addr_of_pointers(next)
                .as_mut()
                .set_prev(Waiter::addr_of_pointers(node).as_ref().get_prev());
        } else {
            // This might be the last item in the list
            if self.tail != Some(node) {
                return None;
            }

            self.tail = Waiter::addr_of_pointers(node).as_ref().get_prev();
        }

        Waiter::addr_of_pointers(node).as_mut().set_next(None);
        Waiter::addr_of_pointers(node).as_mut().set_prev(None);

        Some(node)
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
        let field = std::ptr::addr_of_mut!((*target).pointers);
        NonNull::new_unchecked(field)
    }
}

struct Pointers<T> {
    inner: UnsafeCell<PointersInner<T>>,
}

impl<T> Pointers<T> {
    fn new() -> Self {
        Self {
            inner: UnsafeCell::new(PointersInner {
                _prev: None,
                _next: None,
                _pin: PhantomPinned,
            }),
        }
    }

    fn get_prev(&self) -> Option<NonNull<T>> {
        // SAFETY: prev is the first field in PointersInner, which is #[repr(C)].
        unsafe {
            let inner = self.inner.get();
            let prev = inner as *const Option<NonNull<T>>;
            std::ptr::read(prev)
        }
    }

    fn get_next(&self) -> Option<NonNull<T>> {
        // SAFETY: next is the second field in PointersInner, which is #[repr(C)].
        unsafe {
            let inner = self.inner.get();
            let prev = inner as *const Option<NonNull<T>>;
            let next = prev.add(1);
            std::ptr::read(next)
        }
    }

    fn set_prev(&mut self, value: Option<NonNull<T>>) {
        // SAFETY: prev is the first field in PointersInner, which is #[repr(C)].
        unsafe {
            let inner = self.inner.get();
            let prev = inner as *mut Option<NonNull<T>>;
            std::ptr::write(prev, value);
        }
    }

    fn set_next(&mut self, value: Option<NonNull<T>>) {
        // SAFETY: next is the second field in PointersInner, which is #[repr(C)].
        unsafe {
            let inner = self.inner.get();
            let prev = inner as *mut Option<NonNull<T>>;
            let next = prev.add(1);
            std::ptr::write(next, value);
        }
    }
}

#[repr(C)]
struct PointersInner<T> {
    _prev: Option<NonNull<T>>,
    _next: Option<NonNull<T>>,
    _pin: PhantomPinned,
}

struct WakeList {
    inner: [MaybeUninit<Waker>; NUM_WAKERS],
    curr: usize,
}

impl WakeList {
    fn new() -> Self {
        const UNINIT_WAKER: MaybeUninit<Waker> = MaybeUninit::uninit();

        Self {
            inner: [UNINIT_WAKER; NUM_WAKERS],
            curr: 0,
        }
    }

    fn can_push(&self) -> bool {
        self.curr < NUM_WAKERS
    }

    fn push(&mut self, val: Waker) {
        debug_assert!(self.can_push());

        self.inner[self.curr] = MaybeUninit::new(val);
        self.curr += 1;
    }

    fn wake_all(&mut self) {
        assert!(self.curr <= NUM_WAKERS);
        while self.curr > 0 {
            self.curr -= 1;
            // SAFETY: The first `curr` elements of `WakeList` are initialized, so by decrementing
            // `curr`, we can take ownership of the last item.
            let waker = unsafe { std::ptr::read(self.inner[self.curr].as_mut_ptr()) };
            waker.wake();
        }
    }
}

impl Drop for WakeList {
    fn drop(&mut self) {
        let slice =
            std::ptr::slice_from_raw_parts_mut(self.inner.as_mut_ptr().cast::<Waker>(), self.curr);
        // SAFETY: The first `curr` elements are initialized, so we can drop them.
        unsafe { std::ptr::drop_in_place(slice) };
    }
}

const NUM_WAKERS: usize = 32;
