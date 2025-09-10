//! Delayed drop queue: offload heavy drops to a background thread.
//!
//! Use `Reclaimer::instance().drop_later(value)` to enqueue any `Send + 'static`
//! value for background drop.
//!
//! Metrics:
//! - `tycho_delayed_drop_enqueued` — incremented when an item is enqueued.
//! - `tycho_delayed_drop_dropped` — incremented when an item is actually dropped.
//!   The current queue size can be estimated as `enqueued - dropped`.

use std::cell::Cell;
use std::sync::{OnceLock, mpsc};
use std::thread;

use crate::metrics::HistogramGuard;

// Thread-local flag to indicate an explicit, in-place drop is happening.
thread_local! {
    static DROP_IN_PLACE: Cell<bool> = const { Cell::new(false) };
}

/// Reclaimer: wrapper around a bounded mpsc queue that drops values
/// on a background thread.
pub struct Reclaimer {
    tx: mpsc::SyncSender<Box<dyn Send>>,
}

impl Reclaimer {
    const QUEUE_CAPACITY: usize = 10;

    pub fn new(queue_capacity: usize) -> Self {
        let (tx, rx) = mpsc::sync_channel(queue_capacity);

        thread::Builder::new()
            .name("tycho-reclaimer".into())
            .spawn(move || {
                for item in rx {
                    let _guard = HistogramGuard::begin("tycho_delayed_drop_time");
                    metrics::counter!("tycho_delayed_drop_dropped").increment(1);
                    drop(item);
                }
            })
            .expect("failed to spawn reclaimer worker");

        Self { tx }
    }

    /// Global singleton instance, initialized on first use.
    pub fn instance() -> &'static Reclaimer {
        static INSTANCE: OnceLock<Reclaimer> = OnceLock::new();
        INSTANCE.get_or_init(|| Self::new(Self::QUEUE_CAPACITY))
    }

    /// Enqueue a value to be dropped later by the background worker.
    pub fn drop_later<T>(&self, value: T)
    where
        T: Send + 'static,
    {
        let start = std::time::Instant::now();
        metrics::counter!("tycho_delayed_drop_enqueued").increment(1);
        self.tx
            .send(Box::new(value))
            .expect("reclaimer channel send dead");
        let elapsed = start.elapsed();
        let inside_tokio = tokio::runtime::Handle::try_current().is_ok();
        if inside_tokio && elapsed.as_millis() > 10 {
            tracing::warn!(
                elapsed_ms = elapsed.as_millis(),
                "delayed drop enqueued took too long"
            );
        }
    }

    /// Drop a value while setting a TLS flag to signal "in-place" drop semantics.
    /// ```rust,no_run
    /// use std::sync::Arc;
    /// use std::sync::atomic::{AtomicBool, Ordering};
    ///
    /// use tycho_util::drop::Reclaimer;
    ///
    /// struct Loud;
    ///
    /// impl Drop for Loud {
    ///     fn drop(&mut self) {
    ///         if Reclaimer::is_dropping_in_place() {
    ///             Reclaimer::drop_in_place("bla");
    ///         } else {
    ///             Reclaimer::instance().drop_later("bla");
    ///         }
    ///     }
    /// }
    ///
    /// let droper = Reclaimer::instance();
    ///
    /// Reclaimer::want_drop_in_place();
    /// drop(Loud);
    /// ```
    pub fn drop_in_place<T>(value: T) {
        DROP_IN_PLACE.with(|flag| {
            let prev = flag.get();
            flag.set(true);
            drop(value);
            flag.set(prev);
        });
    }

    /// Whether caller wants to drop in-place.
    /// This is useful for blocking tasks, where we can wait for drop
    pub fn is_dropping_in_place() -> bool {
        DROP_IN_PLACE.get()
    }

    pub fn want_drop_in_place() {
        DROP_IN_PLACE.set(true);
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
        Reclaimer::instance().drop_later(Tracer(dropped.clone()));

        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        while !dropped.load(Ordering::Relaxed) {
            assert!(
                std::time::Instant::now() < deadline,
                "value was not dropped in time"
            );
            thread::sleep(Duration::from_millis(5));
        }
    }
}
