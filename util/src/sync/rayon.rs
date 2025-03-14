use std::sync::atomic::{AtomicU32, Ordering};

use tracing::Span;

use crate::metrics::HistogramGuard;

macro_rules! rayon_run_impl {
    ($func_name:ident, $spawn_method:ident, $prefix:expr) => {
        pub async fn $func_name<T: 'static + Send>(f: impl FnOnce() -> T + Send + 'static) -> T {
            static COUNTER: AtomicU32 = AtomicU32::new(0);

            let guard = Guard {
                span: Span::current(),
                finished: false,
            };

            let (send, recv) = tokio::sync::oneshot::channel();
            let wait_time_histogram = HistogramGuard::begin(concat!($prefix, "_queue_time"));

            rayon::$spawn_method(move || {
                drop(wait_time_histogram);

                let _task_time = HistogramGuard::begin(concat!($prefix, "_task_time"));

                COUNTER.fetch_add(1, Ordering::Relaxed);
                let res = f();
                let in_flight = COUNTER.fetch_sub(1, Ordering::Relaxed);

                metrics::histogram!(concat!($prefix, "_threads")).record(in_flight as f64);

                _ = send.send(res);
            });

            let res = recv.await.unwrap();
            guard.disarm();
            res
        }
    };
}

rayon_run_impl!(rayon_run, spawn, "tycho_rayon_lifo");
rayon_run_impl!(rayon_run_fifo, spawn_fifo, "tycho_rayon_fifo");

struct Guard {
    span: Span,
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
            tracing::warn!(
                parent: &self.span,
                "rayon_run has been aborted"
            );
        }
    }
}
