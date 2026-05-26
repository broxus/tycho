use std::num::NonZeroU8;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use tycho_util::metrics::HistogramGuard;

/// Separate twin of [`tycho_util::sync::rayon_run_fifo`]
#[derive(Clone)]
pub struct MempoolRayon(Arc<rayon::ThreadPool>);

impl MempoolRayon {
    pub fn new(num_threads: NonZeroU8) -> Result<Self, rayon::ThreadPoolBuildError> {
        let thread_pool = rayon::ThreadPoolBuilder::new()
            .stack_size(8 * 1024 * 1024)
            .thread_name(|_| "mempool-rayon".to_string())
            .num_threads(num_threads.get() as usize)
            .build()?;
        Ok(Self(Arc::new(thread_pool)))
    }

    pub(crate) async fn run_fifo<T: 'static + Send>(
        &self,
        f: impl FnOnce() -> T + Send + 'static,
    ) -> T {
        static COUNTER: AtomicU32 = AtomicU32::new(0);

        let (send, recv) = tokio::sync::oneshot::channel();
        let wait_time_histogram = HistogramGuard::begin("tycho_rayon_mempool_queue_time");

        self.0.spawn_fifo(move || {
            drop(wait_time_histogram);

            let _task_time = HistogramGuard::begin("tycho_rayon_mempool_task_time");

            COUNTER.fetch_add(1, Ordering::Relaxed);
            let res = f();
            let in_flight = COUNTER.fetch_sub(1, Ordering::Relaxed);

            metrics::histogram!("tycho_rayon_mempool_threads").record(in_flight as f64);

            send.send(res).ok();
        });

        // mempool parse point task may be not interested in result (finishes earlier)

        recv.await
            .expect("mempool rayon thread pool completes all work before terminated")
    }
}
