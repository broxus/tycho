use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Once;
use std::time::{Duration, Instant};

use anyhow::Context;
use serde::{Deserialize, Serialize};
pub use tikv_jemalloc_ctl::Error;
use tikv_jemalloc_ctl::{epoch, stats};

#[macro_export]
macro_rules! set_metrics {
    ($($metric_name:expr => $metric_value:expr),* $(,)?) => {
        $(
            metrics::gauge!($metric_name).set($metric_value as f64);
        )*
    };
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MetricsConfig {
    /// Listen address of metrics. Used by the client to gather prometheus metrics.
    /// Default: `127.0.0.1:10000`
    #[serde(with = "crate::serde_helpers::string")]
    pub listen_addr: SocketAddr,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            listen_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 10000),
        }
    }
}

/// Initializes metrics exporter once.
///
/// All invocations after the successfull initialization will fail.
///
/// Spawns a new thread which will export
pub fn init_metrics(config: &MetricsConfig) -> anyhow::Result<()> {
    use metrics_exporter_prometheus::Matcher;

    const EXPONENTIAL_SECONDS: &[f64] = &[
        0.000001, 0.00001, 0.0001, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.75, 1.0, 2.5, 5.0,
        7.5, 10.0, 30.0, 60.0, 120.0, 180.0, 240.0, 300.0,
    ];

    const EXPONENTIAL_SECONDS_HIGH: &[f64] = &[
        0.00001, 0.0001, 0.001, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.125, 0.15, 0.175, 0.2,
        0.25, 0.3, 0.35, 0.4, 0.45, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2, 1.3, 1.5, 2.0, 2.5,
        3.0,
    ];

    const EXPONENTIAL_LONG_SECONDS: &[f64] = &[
        0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 240.0, 300.0, 600.0, 1800.0, 3600.0, 7200.0,
        14400.0, 28800.0, 43200.0, 86400.0,
    ];

    const EXPONENTIAL_THREADS: &[f64] = &[1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0];

    static ONCE: Once = Once::new();

    let mut result = None;
    ONCE.call_once(|| {
        result = Some((|| {
            metrics_exporter_prometheus::PrometheusBuilder::new()
                .set_buckets_for_metric(Matcher::Suffix("_time".to_string()), EXPONENTIAL_SECONDS)?
                .set_buckets_for_metric(
                    Matcher::Suffix("_time_high".to_string()),
                    EXPONENTIAL_SECONDS_HIGH,
                )?
                .set_buckets_for_metric(
                    Matcher::Suffix("_threads".to_string()),
                    EXPONENTIAL_THREADS,
                )?
                .set_buckets_for_metric(
                    Matcher::Suffix("_time_long".to_string()),
                    EXPONENTIAL_LONG_SECONDS,
                )?
                .with_http_listener(config.listen_addr)
                .install()
                .context("failed to initialize a metrics exporter")
        })());
    });

    match result {
        Some(res) => res?,
        None => anyhow::bail!("metrics exporter was already initialized"),
    };

    const INTERVAL: Duration = Duration::from_secs(5);

    // NOTE: We are using a simple thread there instead of tokio
    // so that there is no surprise when using it during the app start.
    std::thread::Builder::new()
        .name("export_metrics".to_owned())
        .spawn(move || {
            let mut deadline = Instant::now();
            loop {
                let s = match fetch_stats() {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::error!("failed to fetch jemalloc stats: {e}");
                        return;
                    }
                };

                set_metrics!(
                    "jemalloc_allocated_bytes" => s.allocated,
                    "jemalloc_active_bytes" => s.active,
                    "jemalloc_metadata_bytes" => s.metadata,
                    "jemalloc_resident_bytes" => s.resident,
                    "jemalloc_mapped_bytes" => s.mapped,
                    "jemalloc_retained_bytes" => s.retained,
                    "jemalloc_dirty_bytes" => s.dirty,
                    "jemalloc_fragmentation_bytes" => s.fragmentation,
                );

                deadline += INTERVAL;
                if let Some(delay) = deadline.checked_duration_since(Instant::now()) {
                    std::thread::sleep(delay);
                }
            }
        })
        .unwrap();

    Ok(())
}

fn fetch_stats() -> Result<JemallocStats, Error> {
    // Stats are cached. Need to advance epoch to refresh.
    epoch::advance()?;

    Ok(JemallocStats {
        allocated: stats::allocated::read()? as u64,
        active: stats::active::read()? as u64,
        metadata: stats::metadata::read()? as u64,
        resident: stats::resident::read()? as u64,
        mapped: stats::mapped::read()? as u64,
        retained: stats::retained::read()? as u64,
        dirty: stats::resident::read()?
            .saturating_sub(stats::active::read()?)
            .saturating_sub(stats::metadata::read()?) as u64,
        fragmentation: stats::active::read()?.saturating_sub(stats::allocated::read()?) as u64,
    })
}

struct JemallocStats {
    pub allocated: u64,
    pub active: u64,
    pub metadata: u64,
    pub resident: u64,
    pub mapped: u64,
    pub retained: u64,
    pub dirty: u64,
    pub fragmentation: u64,
}
