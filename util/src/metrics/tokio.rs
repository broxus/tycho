use std::time::Duration;

use tokio::runtime::{Handle, RuntimeMetrics};
use tokio::task::JoinHandle;

pub fn spawn_runtime_metrics_exporter(interval: Duration) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            if let Ok(handle) = Handle::try_current() {
                export_runtime_metrics(&handle.metrics());
            } else {
                return;
            }
        }
    })
}

pub fn export_runtime_metrics(metrics: &RuntimeMetrics) {
    let num_workers = metrics.num_workers();

    metrics::gauge!("tokio_num_workers").set(num_workers as f64);
    metrics::gauge!("tokio_num_alive_tasks").set(metrics.num_alive_tasks() as f64);
    metrics::gauge!("tokio_global_queue_depth").set(metrics.global_queue_depth() as f64);

    let total = (0..num_workers).fold(Duration::from_secs(0), |acc, worker| {
        acc.saturating_add(metrics.worker_total_busy_duration(worker))
    });

    metrics::gauge!("tokio_worker_total_busy_duration_seconds").set(total.as_secs_f64());
}
