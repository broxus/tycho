use std::sync::atomic::{AtomicU32, Ordering};

use crossbeam_queue::SegQueue;

use crate::memory::MemCounter;

#[derive(Debug)]
pub struct IdPool<T> {
    max_id: u32,
    next: AtomicU32,
    free_list: SegQueue<T>,
    counter: MemCounter,
}

impl<T> IdPool<T>
where
    T: From<u32> + Into<u32> + Copy,
{
    pub fn new(max_id: u32, counter: MemCounter) -> Self {
        Self {
            max_id,
            next: AtomicU32::new(0),
            free_list: SegQueue::new(),
            counter,
        }
    }

    pub fn alloc(&self) -> Option<T> {
        if let Some(id) = self.free_list.pop() {
            self.counter.sub_bytes(size_of::<T>());
            return Some(id);
        }

        self.next
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                if cur < self.max_id {
                    Some(cur + 1)
                } else {
                    None
                }
            })
            .ok()
            .map(T::from)
    }

    #[inline]
    pub fn free(&self, id: T) {
        debug_assert!(id.into() < self.max_id);
        self.free_list.push(id);
        self.counter.add_bytes(size_of::<T>());
    }

    #[inline]
    pub fn capacity(&self) -> u32 {
        self.max_id
    }

    #[cfg(test)]
    pub fn free_len(&self) -> usize {
        self.free_list.len()
    }
}
