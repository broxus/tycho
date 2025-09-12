use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Ensure that the runtime does not spend too much time without yielding.
pub async fn yield_on_complex(complex: bool) {
    if complex {
        tokio::task::yield_now().await;
    } else {
        tokio::task::consume_budget().await;
    }
}

#[derive(Default, Clone, Debug)]
pub struct CancellationFlag(Arc<AtomicBool>);

impl CancellationFlag {
    pub fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }

    #[must_use]
    pub fn check(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }

    pub fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }

    pub fn debounce(&self, debounce: usize) -> DebounceCancellationFlag {
        DebounceCancellationFlag {
            inner: self.clone(),
            counter: 0,
            debounce: NonZeroUsize::new(debounce.max(1)).unwrap(),
        }
    }
}

pub struct DebounceCancellationFlag {
    inner: CancellationFlag,
    counter: usize,
    debounce: NonZeroUsize,
}

impl DebounceCancellationFlag {
    pub fn into_inner(self) -> CancellationFlag {
        self.inner
    }

    #[must_use]
    pub fn check(&mut self) -> bool {
        let mut cancelled = false;

        if self.counter % self.debounce.get() == 0 {
            self.counter = 0;
            cancelled |= self.inner.check();
        }

        self.counter += 1;
        cancelled
    }

    pub fn cancel(&self) {
        self.inner.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancellation_flag() {
        // Simple
        let flag = CancellationFlag::new();
        assert!(!flag.check());
        flag.cancel();
        assert!(flag.check());

        // Debounced
        let flag = CancellationFlag::new();
        let mut debounce = flag.debounce(10);
        for _ in 0..5 {
            assert!(!debounce.check());
        }
        debounce.cancel();
        for _ in 0..5 {
            assert!(!debounce.check());
        }
        assert!(debounce.check());
    }
}
