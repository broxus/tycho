use std::sync::atomic::{AtomicU32, Ordering};

use crate::metrics::HistogramGuard;

macro_rules! rayon_run_impl {
    ($func_name:ident, $spawn_method:ident, $prefix:expr $(, $thread_pool:ident)?) => {
        pub async fn $func_name<T: 'static + Send>($($thread_pool: &rayon::ThreadPool,)? f: impl FnOnce() -> T + Send + 'static) -> T {
            static COUNTER: AtomicU32 = AtomicU32::new(0);

            let guard = Guard { finished: false };

            let (send, recv) = tokio::sync::oneshot::channel();
            let wait_time_histogram = HistogramGuard::begin(concat!($prefix, "_queue_time"));

            rayon_run_impl!(@spawn $spawn_method, $($thread_pool,)? || {
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

    (@spawn $method:ident, $thread_pool:ident, || $($rest:tt)*) => {
        $thread_pool.$method(move || $($rest)*)
    };
    (@spawn $method:ident, || $($rest:tt)*) => {
        rayon::$method(move || $($rest)*)
    };
}

rayon_run_impl!(rayon_run, spawn, "tycho_rayon_lifo");
rayon_run_impl!(rayon_run_fifo, spawn_fifo, "tycho_rayon_fifo", thread_pool);

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
