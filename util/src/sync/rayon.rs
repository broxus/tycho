use std::sync::atomic::{AtomicU32, Ordering};

use tracing::Span;

use crate::metrics::HistogramGuard;

macro_rules! rayon_run_impl {
    ($func_name:ident, $spawn_method:ident, $prefix:expr) => {
        pub async fn $func_name<T: 'static + Send>(f: impl FnOnce() -> T + Send + 'static) -> T {
            static COUNTER: AtomicU32 = AtomicU32::new(0);
            static QUEUED: AtomicU32 = AtomicU32::new(0);

            let guard = Guard {
                span: Span::current(),
                finished: false,
            };

            let (send, recv) = tokio::sync::oneshot::channel();
            let wait_time_histogram = HistogramGuard::begin(concat!($prefix, "_queue_time"));
            let queue_guard = QueueDepthGuard::enter(
                concat!($prefix, "_queue_depth"),
                concat!($prefix, "_queue_backlog"),
                &QUEUED,
            );

            rayon::$spawn_method(move || {
                let mut queue_guard = queue_guard;
                queue_guard.dequeue();

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

struct QueueDepthGuard {
    depth_metric: &'static str,
    backlog_metric: &'static str,
    counter: &'static AtomicU32,
    active: bool,
}

impl QueueDepthGuard {
    fn enter(
        depth_metric: &'static str,
        backlog_metric: &'static str,
        counter: &'static AtomicU32,
    ) -> Self {
        let queued_now = counter.fetch_add(1, Ordering::Relaxed) + 1;
        metrics::gauge!(depth_metric).set(queued_now as f64);

        Self {
            depth_metric,
            backlog_metric,
            counter,
            active: true,
        }
    }

    fn dequeue(&mut self) {
        if !self.active {
            return;
        }

        let queued_before = self.counter.fetch_sub(1, Ordering::Relaxed);
        let queued_left = queued_before.saturating_sub(1);
        metrics::gauge!(self.depth_metric).set(queued_left as f64);
        metrics::histogram!(self.backlog_metric).record(queued_left as f64);
        self.active = false;
    }
}

impl Drop for QueueDepthGuard {
    fn drop(&mut self) {
        self.dequeue();
    }
}

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
