use std::sync::atomic::{AtomicU32, Ordering};

use crate::metrics::HistogramGuard;

static LIFO_COUNTER: AtomicU32 = AtomicU32::new(0);
static FIFO_COUNTER: AtomicU32 = AtomicU32::new(0);

macro_rules! rayon_run_impl {
    ($func_name:ident, $spawn_method:ident, $counter:ident, $prefix:expr) => {
        pub async fn $func_name<T: 'static + Send>(f: impl 'static + Send + FnOnce() -> T) -> T {
            let guard = Guard { finished: false };

            let (send, recv) = tokio::sync::oneshot::channel();
            let queue_wait_timer = HistogramGuard::begin(concat!($prefix, "_queue_time"));
            let threads_hist = metrics::histogram!(concat!($prefix, "_threads"));

            rayon::$spawn_method(move || {
                queue_wait_timer.finish();

                let hist = HistogramGuard::begin(concat!($prefix, "_task_time"));
                let in_flight = $counter.fetch_add(1, Ordering::Acquire);
                threads_hist.record(in_flight as f64);

                let res = f();
                let in_flight = $counter.fetch_sub(1, Ordering::Release);
                threads_hist.record((in_flight - 1) as f64); // returns previous value
                hist.finish();

                _ = send.send(res);
            });

            let res = recv.await.unwrap();
            guard.disarm();
            res
        }
    };
}

rayon_run_impl!(rayon_run, spawn, LIFO_COUNTER, "tycho_rayon_lifo");
rayon_run_impl!(rayon_run_fifo, spawn_fifo, FIFO_COUNTER, "tycho_rayon_fifo");

struct Guard {
    finished: bool,
}

impl Guard {
    fn disarm(mut self) {
        self.finished = true;
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        if !self.finished {
            tracing::warn!("rayon_run has been aborted");
        }
    }
}
