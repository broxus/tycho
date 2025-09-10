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
use std::sync::{OnceLock, mpsc};
use std::thread;
use std::time::Duration;

use crate::metrics::HistogramGuard;

/// Reclaimer: wrapper around a bounded mpsc queue that drops values
/// on a background thread.
pub struct Reclaimer {
    tx: mpsc::SyncSender<Box<dyn Send>>,
}

impl Reclaimer {
    const QUEUE_CAPACITY: usize = 10;
    const WARN_THRESHOLD: Duration = Duration::from_millis(10);

    pub fn new(queue_capacity: usize) -> Self {
        let (tx, rx) = mpsc::sync_channel(queue_capacity);

        thread::Builder::new()
            .name("tycho-reclaimer".into())
            .spawn(move || {
                tracing::info!("reclaimer started");
                scopeguard::defer! { tracing::info!("reclaimer finished"); };

                // Disable recursive queue drop.
                DROP_FLAGS.set(FLAG_DROPPING);

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

                let start = std::time::Instant::now();
                metrics::counter!("tycho_delayed_drop_enqueued").increment(1);

                // NOTE: We ignore error to just drop the value inplace if
                // reclaimer thread panicked. This will prevent useless double panic.
                self.tx.send(Box::new(value)).ok();

                let elapsed = start.elapsed();
                if inside_tokio && elapsed > Self::WARN_THRESHOLD {
                    tracing::warn!(
                        elapsed_ms = elapsed.as_millis(),
                        "delayed drop queue was full for too long"
                    );
                }

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
