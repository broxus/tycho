use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};

use metrics::{Counter, Gauge, Histogram};
use rayon_core::{MetricsRecorder, WorkerStateEvent, WorkerStateKind};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(default)]
pub struct ThreadPoolConfig {
    /// should be `true` only for full node build
    reserve_mempool_rayon: bool,
    rayon_threads: NonZeroUsize,
    tokio_workers: NonZeroUsize,
}

impl ThreadPoolConfig {
    // don't assign unique names to threads, for example using indexes:
    // that way they'll be merged into a pretty-looking single one in flame graphs

    pub fn init_global_rayon_pool(&self) -> Result<(), rayon::ThreadPoolBuildError> {
        let mut rayon_threads = self.rayon_threads.get();
        if self.reserve_mempool_rayon {
            rayon_threads = rayon_threads.div_ceil(2);
        }
        let pool = PoolRecorder::new(rayon_threads, "global");

        rayon::ThreadPoolBuilder::new()
            .stack_size(8 * 1024 * 1024)
            .thread_name(|_| "rayon_worker".to_string())
            .num_threads(rayon_threads)
            .metrics_recorder(pool)
            .build_global()
    }

    pub fn mempool_rayon_threads(&self) -> NonZeroUsize {
        let mut rayon_threads = self.rayon_threads.get();
        if self.reserve_mempool_rayon {
            rayon_threads = rayon_threads.div_ceil(2);
        }
        rayon_threads.try_into().expect("cannot be zero")
    }

    pub fn build_tokio_runtime(&self) -> std::io::Result<tokio::runtime::Runtime> {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(self.tokio_workers.get())
            .build()
    }
}

impl Default for ThreadPoolConfig {
    fn default() -> Self {
        let total_threads = std::thread::available_parallelism()
            .expect("failed to get total threads")
            .get();
        let half = total_threads
            .div_ceil(2)
            .try_into()
            .expect("cannot be zero");
        Self {
            reserve_mempool_rayon: false,
            rayon_threads: half,
            tokio_workers: half,
        }
    }
}

pub struct PoolRecorder {
    num_workers: usize,
    locals: Box<[AtomicUsize]>, // last-seen local depth per worker
    locals_total: AtomicUsize,  // running sum of locals via deltas
    global: AtomicUsize,        // last-seen injector depth
    metrics: PoolRecorderMetrics,
}

impl PoolRecorder {
    pub fn new(num_workers: usize, label: &'static str) -> Self {
        let v: Vec<AtomicUsize> = (0..num_workers).map(|_| AtomicUsize::new(0)).collect();
        let label = metrics::Label::from_static_parts("pool", label);
        let metrics = PoolRecorderMetrics::new(label.clone());
        Self {
            metrics,
            num_workers,
            locals: v.into_boxed_slice(),
            locals_total: AtomicUsize::new(0),
            global: AtomicUsize::new(0),
        }
    }

    #[inline]
    fn update_local(&self, idx: usize, new: usize) {
        debug_assert!(idx < self.locals.len(), "worker index out of range");
        let prev = self.locals[idx].swap(new, Ordering::Relaxed);
        if new >= prev {
            self.locals_total.fetch_add(new - prev, Ordering::Relaxed);
        } else {
            self.locals_total.fetch_sub(prev - new, Ordering::Relaxed);
        }
    }

    #[inline]
    fn set_global(&self, depth: usize) {
        self.global.store(depth, Ordering::Relaxed);
    }

    #[inline]
    fn totals(&self) -> (usize, usize, usize) {
        let locals_sum = self.locals_total.load(Ordering::Relaxed);
        let global = self.global.load(Ordering::Relaxed);
        (locals_sum, global, locals_sum + global)
    }
}

impl MetricsRecorder for PoolRecorder {
    fn worker_event(&self, e: WorkerStateEvent) {
        // Keep aggregates current.
        if let Some(ld) = e.local_queue_depth {
            self.update_local(e.worker_index, ld);
        }
        if let Some(gd) = e.global_queue_depth {
            self.set_global(gd);
        }

        let (locals_sum, global, total) = self.totals();

        self.metrics.locals_total.set(locals_sum as f64);
        self.metrics.global_depth.set(global as f64);
        self.metrics.total_depth.set(total as f64);

        // Thread state (pool-wide)
        self.metrics.threads_inactive.set(e.inactive_threads as f64);
        self.metrics.threads_sleeping.set(e.sleeping_threads as f64);
        self.metrics
            .threads_awake_idle
            .set(e.awake_but_idle_threads as f64);

        // Active = total workers - inactive
        let active = self.num_workers.saturating_sub(e.inactive_threads);
        self.metrics.threads_active.set(active as f64);

        // ---- Histograms (derivable times/backlog) ----
        self.metrics.queue_backlog.record(total as f64);

        if let Some(d) = e.search_latency {
            self.metrics.search_latency.record(d.as_secs_f64());
        }
        if let Some(d) = e.sleep_duration {
            self.metrics.sleep_duration.record(d.as_secs_f64());
        }

        self.metrics.worker_events.record(e.kind);
    }
}

struct PoolRecorderMetrics {
    locals_total: Gauge,
    global_depth: Gauge,
    total_depth: Gauge,
    threads_inactive: Gauge,
    threads_sleeping: Gauge,
    threads_awake_idle: Gauge,
    threads_active: Gauge,
    queue_backlog: Histogram,
    search_latency: Histogram,
    sleep_duration: Histogram,
    worker_events: WorkerEventCounters,
}

impl PoolRecorderMetrics {
    fn new(label: metrics::Label) -> Self {
        Self {
            locals_total: Self::make_gauge("rayon_queue_locals_total", label.clone()),
            global_depth: Self::make_gauge("rayon_queue_global_depth", label.clone()),
            total_depth: Self::make_gauge("rayon_queue_total_depth", label.clone()),
            threads_inactive: Self::make_gauge("rayon_threads_inactive", label.clone()),
            threads_sleeping: Self::make_gauge("rayon_threads_sleeping", label.clone()),
            threads_awake_idle: Self::make_gauge("rayon_threads_awake_idle", label.clone()),
            threads_active: Self::make_gauge("rayon_threads_active", label.clone()),
            queue_backlog: Self::make_histogram("rayon_queue_backlog", label.clone()),
            search_latency: Self::make_histogram("rayon_search_latency_seconds", label.clone()),
            sleep_duration: Self::make_histogram("rayon_sleep_duration_seconds", label.clone()),
            worker_events: WorkerEventCounters::new(label),
        }
    }

    fn make_gauge(name: &'static str, label: metrics::Label) -> Gauge {
        metrics::gauge!(name, [label].iter())
    }

    fn make_histogram(name: &'static str, label: metrics::Label) -> Histogram {
        metrics::histogram!(name, [label].iter())
    }
}

struct WorkerEventCounters {
    start_looking: Counter,
    work_found: Counter,
    sleepy: Counter,
    sleeping: Counter,
    woken: Counter,
    resumed: Counter,
}

impl WorkerEventCounters {
    fn new(pool_label: metrics::Label) -> Self {
        Self {
            start_looking: Self::make_counter(pool_label.clone(), "start_looking"),
            work_found: Self::make_counter(pool_label.clone(), "work_found"),
            sleepy: Self::make_counter(pool_label.clone(), "sleepy"),
            sleeping: Self::make_counter(pool_label.clone(), "sleeping"),
            woken: Self::make_counter(pool_label.clone(), "woken"),
            resumed: Self::make_counter(pool_label, "resumed"),
        }
    }

    fn record(&self, kind: WorkerStateKind) {
        match kind {
            WorkerStateKind::StartLooking => self.start_looking.increment(1),
            WorkerStateKind::WorkFound => self.work_found.increment(1),
            WorkerStateKind::Sleepy => self.sleepy.increment(1),
            WorkerStateKind::Sleeping => self.sleeping.increment(1),
            WorkerStateKind::Woken => self.woken.increment(1),
            WorkerStateKind::Resumed => self.resumed.increment(1),
            _ => unreachable!(),
        }
    }

    fn make_counter(pool_label: metrics::Label, kind: &'static str) -> Counter {
        metrics::counter!(
            "rayon_worker_event_total",
            [pool_label, metrics::Label::from_static_parts("kind", kind)].iter()
        )
    }
}
