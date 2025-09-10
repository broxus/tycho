//! Delayed drop queue: offload heavy drops to a background thread.
//!
//! Initialize once with `init_drop_guard()`, then enqueue any `Send + 'static`
//! value via `delayed_drop(value)` to drop it asynchronously.
//!
//! Metrics:
//! - `tycho_delayed_drop_enqueued` — incremented when an item is enqueued.
//! - `tycho_delayed_drop_dropped` — incremented when an item is actually dropped.
//!   The current queue size can be estimated as `enqueued - dropped`.

use std::any::Any;
use std::sync::{OnceLock, mpsc};
use std::thread;

static SENDER: OnceLock<mpsc::Sender<Box<dyn Any + Send>>> = OnceLock::new();

/// Initializes the delayed drop background worker.
///
/// Panics if called more than once or if the worker thread cannot be spawned.
pub fn init_drop_guard() {
    let (tx, rx) = mpsc::channel::<Box<dyn Any + Send>>();

    // Set the global sender; panic if already initialized as requested.
    SENDER.set(tx).expect("drop guard already initialized");

    // Spawn a single background worker that drains and drops items.
    thread::Builder::new()
        .name("tycho-delayed-drop".into())
        .spawn(move || {
            for item in rx {
                metrics::counter!("tycho_delayed_drop_dropped").increment(1);
                drop(item);
            }
        })
        .expect("failed to spawn delayed drop worker");
}

/// Enqueue a value to be dropped on the background thread.
///
/// Panics if the drop guard wasn't initialized or if the channel is closed.
pub fn delayed_drop<T>(t: T)
where
    T: Send + 'static,
{
    let sender = SENDER.get().expect("drop guard not initialized");
    metrics::counter!("tycho_delayed_drop_enqueued").increment(1);
    sender
        .send(Box::new(t) as Box<dyn Any + Send>)
        .expect("delayed drop channel closed");
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
        init_drop_guard();

        let dropped = Arc::new(AtomicBool::new(false));
        delayed_drop(Tracer(dropped.clone()));

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
