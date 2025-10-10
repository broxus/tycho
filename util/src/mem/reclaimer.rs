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
use std::num::NonZeroUsize;
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
    const QUEUE_CAPACITY: NonZeroUsize = NonZeroUsize::new(10).unwrap();
    const WARN_THRESHOLD: Duration = Duration::from_millis(10);
    const DEFAULT_WORKERS: NonZeroUsize = NonZeroUsize::new(2).unwrap();

    pub fn init(
        queue_capacity: NonZeroUsize,
        worker_count: NonZeroUsize,
    ) -> Result<(), ReclaimerError> {
        let mut did_init = false;

        let _ = INSTANCE.get_or_init(|| {
            did_init = true;
            Self::with_workers(queue_capacity, worker_count)
        });

        if did_init {
            Ok(())
        } else {
            Err(ReclaimerError::AlreadyInitialized)
        }
    }

    fn with_workers(queue_capacity: NonZeroUsize, worker_count: NonZeroUsize) -> Self {
        let inner = Arc::new(Inner::new(queue_capacity));
        Self::start_workers(inner.clone(), worker_count);
        Self { inner }
    }

    /// Global singleton instance, initialized on first use.
    pub fn instance() -> &'static Reclaimer {
        INSTANCE.get_or_init(|| Self::with_workers(Self::QUEUE_CAPACITY, Self::DEFAULT_WORKERS))
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
    capacity: NonZeroUsize,
}

struct State {
    len: usize,
}

impl Reclaimer {
    fn start_workers(inner: Arc<Inner>, worker_total: NonZeroUsize) {
        for worker_index in 0..worker_total.get() {
            let inner = inner.clone();
            thread::Builder::new()
                .name("tycho-reclaimer".into())
                .spawn(move || Inner::worker_loop(inner, worker_index))
                .expect("failed to spawn reclaimer worker");
        }
    }
}

impl Inner {
    fn new(capacity: NonZeroUsize) -> Self {
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

            while state.len >= self.capacity.get() {
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
                    }
                    self.not_full.notify_one();
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

        loop {
            let item = inner.pop();
            let histogram = HistogramGuard::begin("tycho_delayed_drop_time");
            metrics::counter!("tycho_delayed_drop_dropped").increment(1);
            drop(item);
            histogram.finish();
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ReclaimerError {
    #[error("Reclaimer was already initialized")]
    AlreadyInitialized,
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    use super::*;

    /// Sends the current thread ID when dropped.
    struct Tracer(mpsc::Sender<thread::ThreadId>);

    impl Drop for Tracer {
        fn drop(&mut self) {
            // Ignore send errors (e.g., if the test already timed out/panicked).
            let _ = self.0.send(thread::current().id());
        }
    }

    #[test]
    fn drops_in_background() {
        let (tx, rx) = mpsc::channel::<thread::ThreadId>();
        let origin = thread::current().id();

        Reclaimer::instance().drop(Tracer(tx));

        let dropped_on = rx
            .recv_timeout(Duration::from_secs(3))
            .expect("value was not dropped in time");

        // Should be dropped by a worker thread, not the caller.
        assert_ne!(dropped_on, origin, "drop did not occur on a worker thread");
    }

    #[test]
    fn drops_in_place() {
        let (tx, rx) = mpsc::channel::<thread::ThreadId>();
        let origin = thread::current().id();

        Reclaimer::instance().drop_in_place(Tracer(tx));

        let dropped_on = rx
            .recv_timeout(Duration::from_secs(3))
            .expect("value was not dropped in time");

        assert_eq!(dropped_on, origin, "didn't drop in place");
    }

    #[test]
    fn double_init_will_err() {
        // First call to init can succeed, but tests are run in parallel,
        // so we need to ensure that it does not panic.
        let _ = Reclaimer::init(Reclaimer::QUEUE_CAPACITY, Reclaimer::DEFAULT_WORKERS);

        // The second call in *this* test must fail.
        let second = Reclaimer::init(Reclaimer::QUEUE_CAPACITY, Reclaimer::DEFAULT_WORKERS);
        assert!(
            matches!(second, Err(ReclaimerError::AlreadyInitialized)),
            "second init should always fail"
        );
    }

    #[test]
    fn burst_is_dropped() {
        let reclaimer = Reclaimer::instance();

        assert_eq!(
            reclaimer.inner.capacity.get(),
            Reclaimer::QUEUE_CAPACITY.get()
        );

        assert_eq!(reclaimer.inner.state.lock().unwrap().len, 0);

        let total = Reclaimer::QUEUE_CAPACITY.get() * 100;

        // Single channel; clone the sender per item.
        let (tx, rx) = mpsc::channel::<thread::ThreadId>();
        let origin = thread::current().id();

        for _ in 0..total {
            Reclaimer::instance().drop(Tracer(tx.clone()));
        }
        drop(tx); // Close the sending side when all enqueues are done.

        let now = Instant::now();
        let mut received = 0usize;

        while received < total {
            let elapsed = now.elapsed();
            if elapsed > Duration::from_secs(3) {
                panic!(
                    "timed out waiting for drops: {received} of {total} received in {elapsed:?}"
                );
            }

            match rx.recv_timeout(Duration::from_millis(100)) {
                Ok(worker_id) => {
                    assert_ne!(
                        worker_id, origin,
                        "Each drop should come from a worker thread"
                    );
                    received += 1;
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    // keep looping until deadline
                }
                Err(e) => panic!("{e}"),
            }
        }

        assert_eq!(reclaimer.inner.state.lock().unwrap().len, 0);
    }
}
