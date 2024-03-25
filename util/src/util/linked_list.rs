//! See <https://github.com/tokio-rs/tokio/blob/c9273f1aee9927b16ee3a789a382c99ad600c8b6/tokio/src/util/linked_list.rs>.

use std::cell::UnsafeCell;
use std::marker::{PhantomData, PhantomPinned};
use std::mem::ManuallyDrop;
use std::ptr::NonNull;

pub(crate) struct LinkedList<L, T> {
    /// Linked list head
    head: Option<NonNull<T>>,

    /// Linked list tail
    tail: Option<NonNull<T>>,

    /// Node type marker.
    _marker: PhantomData<*const L>,
}

unsafe impl<L: Link> Send for LinkedList<L, L::Target> where L::Target: Send {}
unsafe impl<L: Link> Sync for LinkedList<L, L::Target> where L::Target: Sync {}

impl<L, T> LinkedList<L, T> {
    pub const fn new() -> LinkedList<L, T> {
        LinkedList {
            head: None,
            tail: None,
            _marker: PhantomData,
        }
    }
}

impl<L: Link> LinkedList<L, L::Target> {
    /// Adds an element first in the list.
    pub fn push_front(&mut self, val: L::Handle) {
        let val = ManuallyDrop::new(val);
        let ptr = L::as_raw(&val);
        assert_ne!(self.head, Some(ptr));
        unsafe {
            L::pointers(ptr).as_mut().set_next(self.head);
            L::pointers(ptr).as_mut().set_prev(None);

            if let Some(head) = self.head {
                L::pointers(head).as_mut().set_prev(Some(ptr));
            }

            self.head = Some(ptr);

            if self.tail.is_none() {
                self.tail = Some(ptr);
            }
        }
    }

    /// Removes the last element from a list and returns it, or None if it is
    /// empty.
    pub fn pop_back(&mut self) -> Option<L::Handle> {
        unsafe {
            let last = self.tail?;
            self.tail = L::pointers(last).as_ref().get_prev();

            if let Some(prev) = L::pointers(last).as_ref().get_prev() {
                L::pointers(prev).as_mut().set_next(None);
            } else {
                self.head = None;
            }

            L::pointers(last).as_mut().set_prev(None);
            L::pointers(last).as_mut().set_next(None);

            Some(L::from_raw(last))
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
    pub unsafe fn remove(&mut self, node: NonNull<L::Target>) -> Option<L::Handle> {
        if let Some(prev) = L::pointers(node).as_ref().get_prev() {
            debug_assert_eq!(L::pointers(prev).as_ref().get_next(), Some(node));
            L::pointers(prev)
                .as_mut()
                .set_next(L::pointers(node).as_ref().get_next());
        } else {
            if self.head != Some(node) {
                return None;
            }

            self.head = L::pointers(node).as_ref().get_next();
        }

        if let Some(next) = L::pointers(node).as_ref().get_next() {
            debug_assert_eq!(L::pointers(next).as_ref().get_prev(), Some(node));
            L::pointers(next)
                .as_mut()
                .set_prev(L::pointers(node).as_ref().get_prev());
        } else {
            // This might be the last item in the list
            if self.tail != Some(node) {
                return None;
            }

            self.tail = L::pointers(node).as_ref().get_prev();
        }

        L::pointers(node).as_mut().set_next(None);
        L::pointers(node).as_mut().set_prev(None);

        Some(L::from_raw(node))
    }

    pub(crate) fn last(&self) -> Option<&L::Target> {
        let tail = self.tail.as_ref()?;
        unsafe { Some(&*tail.as_ptr()) }
    }
}

impl<L: Link> Default for LinkedList<L, L::Target> {
    fn default() -> Self {
        Self::new()
    }
}

/// # Safety
///
/// Implementations must guarantee that `Target` types are pinned in memory.
pub(crate) unsafe trait Link {
    type Handle;
    type Target;

    #[allow(clippy::wrong_self_convention)]
    fn as_raw(handle: &Self::Handle) -> NonNull<Self::Target>;

    unsafe fn from_raw(ptr: NonNull<Self::Target>) -> Self::Handle;

    unsafe fn pointers(target: NonNull<Self::Target>) -> NonNull<Pointers<Self::Target>>;
}

pub(crate) struct Pointers<T> {
    inner: UnsafeCell<PointersInner<T>>,
}

impl<T> Pointers<T> {
    /// Create a new set of empty pointers
    pub(crate) fn new() -> Pointers<T> {
        Pointers {
            inner: UnsafeCell::new(PointersInner {
                _prev: None,
                _next: None,
                _pin: PhantomPinned,
            }),
        }
    }

    pub(crate) fn get_prev(&self) -> Option<NonNull<T>> {
        // SAFETY: prev is the first field in PointersInner, which is #[repr(C)].
        unsafe {
            let inner = self.inner.get();
            let prev = inner as *const Option<NonNull<T>>;
            std::ptr::read(prev)
        }
    }
    pub(crate) fn get_next(&self) -> Option<NonNull<T>> {
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
            let prev = inner.cast::<Option<NonNull<T>>>();
            std::ptr::write(prev, value);
        }
    }
    fn set_next(&mut self, value: Option<NonNull<T>>) {
        // SAFETY: next is the second field in PointersInner, which is #[repr(C)].
        unsafe {
            let inner = self.inner.get();
            let prev = inner.cast::<Option<NonNull<T>>>();
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

unsafe impl<T: Send> Send for Pointers<T> {}
unsafe impl<T: Sync> Sync for Pointers<T> {}
