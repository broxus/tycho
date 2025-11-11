use std::num::NonZeroUsize;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Instant;

use anyhow::Context;
use tracing::Span;

use crate::metrics::HistogramGuard;

static POOL: OnceLock<futures_executor::ThreadPool> = OnceLock::new();

const STACK_SIZE: usize = 8 * 1024 * 1024;

/// default pool with num threads equal to num CPUs
fn default_thread_pool() -> futures_executor::ThreadPool {
    futures_executor::ThreadPoolBuilder::new()
        .stack_size(STACK_SIZE)
        .name_prefix("fifo-")
        .create()
        .expect("create fifo thread pool")
}

pub fn install_global_fifo_pool(num_threads: NonZeroUsize) -> anyhow::Result<()> {
    let thread_pool = futures_executor::ThreadPoolBuilder::new()
        .pool_size(num_threads.get())
        .stack_size(STACK_SIZE)
        .name_prefix("fifo-")
        .create()
        .context("create fifo thread pool")?;

    if POOL.set(thread_pool).is_err() {
        anyhow::bail!("global fifo thread pool is already initialized");
    }
    Ok(())
}

pub async fn fifo_run<T: 'static + Send>(f: impl FnOnce() -> T + Send + 'static) -> T {
    static COUNTER: AtomicU32 = AtomicU32::new(0);

    let guard = Guard {
        span: Span::current(),
        finished: false,
    };

    let (send, recv) = tokio::sync::oneshot::channel();
    let wait_time_histogram = HistogramGuard::begin("tycho_rayon_fifo_queue_time");

    POOL.get_or_init(default_thread_pool).spawn_ok(async {
        drop(wait_time_histogram);

        let _task_time = HistogramGuard::begin("tycho_rayon_fifo_task_time");

        COUNTER.fetch_add(1, Ordering::Relaxed);
        let res = f();
        let in_flight = COUNTER.fetch_sub(1, Ordering::Relaxed);

        metrics::histogram!("tycho_rayon_fifo_threads").record(in_flight as f64);

        _ = send.send((Instant::now(), res));
    });

    let (send_time, res) = recv.await.unwrap();
    guard.disarm();
    metrics::histogram!("tycho_rayon_fifo_receive_time").record(send_time.elapsed());
    res
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
                "fifo_run has been aborted"
            );
        }
    }
}
