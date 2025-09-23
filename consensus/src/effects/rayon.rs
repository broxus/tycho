use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use tracing::Span;
use tycho_util::metrics::HistogramGuard;

use crate::effects::{Cancelled, TaskResult};

/// Separate twin of [`tycho_util::sync::rayon_run_fifo`]
#[derive(Clone)]
pub struct MempoolRayon(Arc<rayon::ThreadPool>);

impl MempoolRayon {
    pub fn new(num_threads: NonZeroUsize) -> Result<Self, rayon::ThreadPoolBuildError> {
        let thread_pool = rayon::ThreadPoolBuilder::new()
            .stack_size(8 * 1024 * 1024)
            .thread_name(|id| format!("rayon-mempool-{id}"))
            .num_threads(num_threads.get())
            .build()?;
        Ok(Self(Arc::new(thread_pool)))
    }

    pub(crate) async fn run_fifo<T: 'static + Send>(
        &self,
        f: impl FnOnce() -> T + Send + 'static,
    ) -> TaskResult<T> {
        static COUNTER: AtomicU32 = AtomicU32::new(0);

        let guard = Guard {
            span: Span::current(),
            finished: false,
        };

        let (send, recv) = tokio::sync::oneshot::channel();
        let wait_time_histogram = HistogramGuard::begin("tycho_rayon_mempool_queue_time");

        self.0.spawn_fifo(move || {
            drop(wait_time_histogram);

            let _task_time = HistogramGuard::begin("tycho_rayon_mempool_task_time");

            COUNTER.fetch_add(1, Ordering::Relaxed);
            let res = f();
            let in_flight = COUNTER.fetch_sub(1, Ordering::Relaxed);

            metrics::histogram!("tycho_rayon_mempool_threads").record(in_flight as f64);

            _ = send.send(res);
        });

        let res = recv.await.map_err(|_closed| Cancelled());

        guard.disarm();
        res
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
                "rayon run has been aborted in mempool"
            );
        }
    }
}
