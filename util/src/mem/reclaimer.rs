//! Delayed drop queue: offload heavy drops to a background thread.
//!
//! Use `Reclaimer::instance().drop(value)` to enqueue any `Send + 'static`
//! value for background drop.
//!
//! Metrics:
//! - `tycho_delayed_drop_enqueued` — incremented when an item is enqueued.
//! - `tycho_delayed_drop_dropped` — incremented when an item is actually dropped.
//!   The current queue size can be estimated as `enqueued - dropped`.

use std::cell::Cell;
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use crossbeam_deque::{Injector, Steal};

use crate::metrics::HistogramGuard;

static INSTANCE: OnceLock<Reclaimer> = OnceLock::new();

/// Reclaimer: wrapper around a bounded work-stealing queue that drops values
/// on background worker threads.
pub struct Reclaimer {
    inner: Arc<Inner>,
}

impl Reclaimer {
    const QUEUE_CAPACITY: usize = 10;
    const WARN_THRESHOLD: Duration = Duration::from_millis(10);
    const MAX_WORKERS: usize = 8;

    pub fn init(queue_capacity: usize, worker_count: usize) -> &'static Reclaimer {
        INSTANCE.get_or_init(|| Self::with_workers(queue_capacity, worker_count))
    }

    fn with_workers(queue_capacity: usize, worker_count: usize) -> Self {
        let inner = Arc::new(Inner::new(queue_capacity));
        Self::start_workers(inner.clone(), worker_count);
        Self { inner }
    }

    /// Global singleton instance, initialized on first use.
    pub fn instance() -> &'static Reclaimer {
        Self::init(Self::QUEUE_CAPACITY, Self::MAX_WORKERS)
    }

    /// Enqueue a value to be dropped later by the background worker.
    pub fn drop<T>(&self, value: T)
    where
        T: Send + 'static,
    {
        DROP_FLAGS.with(|flags| {
            let flags_before = flags.get();
            if flags_before & FLAG_DROPPING != 0 {
                // Value will be dropped here inplace without using the queue.
                return;
            }

            let inside_tokio = tokio::runtime::Handle::try_current().is_ok();
            if inside_tokio || flags_before & FLAG_ALLOW_IN_PLACE == 0 {
                // Prevent recursive channel drops.
                flags.set(flags_before | FLAG_DROPPING);

                let start = Instant::now();
                metrics::counter!("tycho_delayed_drop_enqueued").increment(1);

                self.inner.enqueue(Box::new(value), inside_tokio, start);

                // Reset flags
                flags.set(flags_before);
            } else {
                // DROP_IN_PLACE flag was set for blocking task.
                drop(value);
            }
        });
    }

    /// Enqueue a value to be dropped later by the background worker.
    ///
    /// Drops value inplace if called outside a tokio context.
    pub fn drop_in_place<T>(&self, value: T)
    where
        T: Send + 'static,
    {
        DROP_FLAGS.with(|flags| {
            let flags_before = flags.get();
            flags.set(flags_before | FLAG_ALLOW_IN_PLACE);
            self.drop(value);
            flags.set(flags_before);
        });
    }
}

thread_local! {
    static DROP_FLAGS: Cell<u8> = const { Cell::new(0) };
}

const FLAG_DROPPING: u8 = 0x01;
const FLAG_ALLOW_IN_PLACE: u8 = 0b10;

struct Inner {
    queue: Injector<Box<dyn Send>>,
    state: Mutex<State>,
    not_empty: Condvar,
    not_full: Condvar,
    capacity: usize,
}

struct State {
    len: usize,
}

impl Reclaimer {
    fn start_workers(inner: Arc<Inner>, worker_total: usize) {
        for worker_index in 0..worker_total.max(1) {
            let inner = inner.clone();
            thread::Builder::new()
                .name(format!("tycho-reclaimer-{worker_index}"))
                .spawn(move || Inner::worker_loop(inner, worker_index))
                .expect("failed to spawn reclaimer worker");
        }
    }
}

impl Inner {
    fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        Self {
            queue: Injector::new(),
            state: Mutex::new(State { len: 0 }),
            not_empty: Condvar::new(),
            not_full: Condvar::new(),
            capacity,
        }
    }

    fn enqueue(&self, item: Box<dyn Send>, inside_tokio: bool, start: Instant) {
        {
            let mut state = self.state.lock().expect("poisoned");

            while state.len >= self.capacity {
                state = self.not_full.wait(state).expect("poisoned");
            }
            state.len += 1;
        }

        self.queue.push(item);
        self.not_empty.notify_one();

        if inside_tokio {
            let elapsed = start.elapsed();
            if elapsed > Reclaimer::WARN_THRESHOLD {
                tracing::warn!(
                    elapsed_ms = elapsed.as_millis(),
                    "delayed drop queue was full for too long"
                );
            }
        }
    }

    fn pop(&self) -> Box<dyn Send> {
        loop {
            match self.queue.steal() {
                Steal::Success(item) => {
                    {
                        let mut state = self.state.lock().expect("poisoned");
                        assert!(state.len > 0);
                        state.len -= 1;
                        self.not_full.notify_one();
                    }
                    return item;
                }
                Steal::Retry => {
                    std::hint::spin_loop();
                }
                Steal::Empty => {
                    let mut state = self.state.lock().expect("poisoned");
                    while state.len == 0 {
                        state = self.not_empty.wait(state).expect("poisoned");
                    }
                }
            }
        }
    }

    fn worker_loop(inner: Arc<Self>, worker_index: usize) {
        tracing::info!(?worker_index, "reclaimer worker started");
        scopeguard::defer! { tracing::info!(?worker_index, "reclaimer worker finished"); };

        DROP_FLAGS.set(FLAG_DROPPING);
        let mut histogram = HistogramGuard::begin("tycho_delayed_drop_time");

        loop {
            let item = inner.pop();
            histogram.reset();
            metrics::counter!("tycho_delayed_drop_dropped").increment(1);
            drop(item);
            histogram.finish_ref();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;
    use std::time::Duration;

    use super::*;

    struct Tracer(Arc<AtomicBool>);
    impl Drop for Tracer {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Relaxed);
        }
    }

    #[test]
    fn drops_in_background() {
        let dropped = Arc::new(AtomicBool::new(false));
        Reclaimer::instance().drop(Tracer(dropped.clone()));

        let deadline = Instant::now() + Duration::from_secs(3);
        while !dropped.load(Ordering::Relaxed) {
            assert!(Instant::now() < deadline, "value was not dropped in time");
            thread::sleep(Duration::from_millis(5));
        }
    }
}
