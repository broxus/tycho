pub use tikv_jemalloc_ctl::Error;
use tikv_jemalloc_ctl::{epoch, stats};
macro_rules! set_metrics {
    ($($metric_name:expr => $metric_value:expr),* $(,)?) => {
        $(
            metrics::gauge!($metric_name).set($metric_value as f64);
        )*
    };}

pub fn spawn_allocator_metrics_loop() {
    tokio::spawn(async move {
        loop {
            let Ok(s) = fetch_stats() else { break };
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
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }
    });
}

pub fn fetch_stats() -> Result<JemallocStats, Error> {
    // Stats are cached. Need to advance epoch to refresh.
    epoch::advance()?;

    Ok(JemallocStats {
        allocated: stats::allocated::read()? as u64,
        active: stats::active::read()? as u64,
        metadata: stats::metadata::read()? as u64,
        resident: stats::resident::read()? as u64,
        mapped: stats::mapped::read()? as u64,
        retained: stats::retained::read()? as u64,
        dirty: (stats::resident::read()?
            .saturating_sub(stats::active::read()?)
            .saturating_sub(stats::metadata::read()?)) as u64,
        fragmentation: (stats::active::read()?.saturating_sub(stats::allocated::read()?)) as u64,
    })
}

pub struct JemallocStats {
    pub allocated: u64,
    pub active: u64,
    pub metadata: u64,
    pub resident: u64,
    pub mapped: u64,
    pub retained: u64,
    pub dirty: u64,
    pub fragmentation: u64,
}
