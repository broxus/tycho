//! See <https://github.com/tokio-rs/tokio/blob/c9273f1aee9927b16ee3a789a382c99ad600c8b6/tokio/src/util/wake_list.rs>.

use std::mem::MaybeUninit;
use std::task::Waker;

pub(crate) struct WakeList {
    inner: [MaybeUninit<Waker>; NUM_WAKERS],
    curr: usize,
}

impl WakeList {
    pub fn new() -> Self {
        const UNINIT_WAKER: MaybeUninit<Waker> = MaybeUninit::uninit();

        Self {
            inner: [UNINIT_WAKER; NUM_WAKERS],
            curr: 0,
        }
    }

    pub fn can_push(&self) -> bool {
        self.curr < NUM_WAKERS
    }

    pub fn push(&mut self, val: Waker) {
        debug_assert!(self.can_push());

        self.inner[self.curr] = MaybeUninit::new(val);
        self.curr += 1;
    }

    pub fn wake_all(&mut self) {
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
