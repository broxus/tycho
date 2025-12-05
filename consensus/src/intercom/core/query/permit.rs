use std::num::NonZeroU8;
use std::sync::atomic::AtomicU8;
use std::sync::{Arc, atomic};

#[derive(Clone)]
pub struct QueryPermits(Arc<AtomicU8>);

impl QueryPermits {
    pub fn new(limit: NonZeroU8) -> Self {
        Self(Arc::new(AtomicU8::new(limit.get())))
    }

    pub fn try_acquire(&self) -> Option<QueryPermit> {
        self.0
            .fetch_update(
                atomic::Ordering::Relaxed,
                atomic::Ordering::Relaxed,
                |permits| permits.checked_sub(1),
            )
            .is_ok()
            .then(|| QueryPermit(self.0.clone()))
    }
}

pub struct QueryPermit(Arc<AtomicU8>);

impl Drop for QueryPermit {
    fn drop(&mut self) {
        self.0.fetch_add(1, atomic::Ordering::Relaxed);
    }
}
