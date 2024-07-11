use std::ffi::{c_char, CString};
use std::os::unix::prelude::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub use tikv_jemalloc_ctl::Error;
use tikv_jemalloc_ctl::{epoch, stats};
use tokio::sync::mpsc;

macro_rules! set_metrics {
    ($($metric_name:expr => $metric_value:expr),* $(,)?) => {
        $(
            metrics::gauge!($metric_name).set($metric_value as f64);
        )*
    };
}

pub fn spawn_allocator_metrics_loop() {
    tokio::spawn(async move {
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

pub async fn memory_profiler(
    profiling_dir: PathBuf,
    mut trigger: mpsc::UnboundedReceiver<bool>,
    profiler_state: Arc<AtomicBool>,
) {
    if std::env::var("MALLOC_CONF").is_err() {
        tracing::warn!(
            "MALLOC_CONF is not set, memory profiler is disabled. \
            set MALLOC_CONF=prof:true to enable"
        );
        return;
    }

    if let Err(e) = std::fs::create_dir_all(&profiling_dir) {
        tracing::error!(
            path = %profiling_dir.display(),
            "failed to create profiling directory: {e:?}"
        );
        return;
    }

    profiler_state.store(false, Ordering::Release);
    while trigger.recv().await.is_some() {
        tracing::info!("memory profiler signal received");
        let is_active = profiler_state.load(Ordering::Acquire);
        if !is_active {
            tracing::info!("activating memory profiler");
            if let Err(e) = profiler_start() {
                tracing::error!("failed to activate memory profiler: {e:?}");
                continue;
            }
        } else {
            let invocation_time = chrono::Local::now();
            let filename = format!("{}.dump", invocation_time.format("%Y-%m-%d_%H-%M-%S"));
            let path = profiling_dir.join(filename);
            if let Err(e) = profiler_dump(&path) {
                tracing::error!(path = %path.display(), "failed to dump prof: {e:?}");
            }
            if let Err(e) = profiler_stop() {
                tracing::error!("failed to deactivate memory profiler: {e:?}");
                continue;
            }
        }

        profiler_state.store(!is_active, Ordering::Release);
    }
}

fn profiler_start() -> Result<(), Error> {
    tracing::info!("starting jemalloc profiler");
    unsafe { tikv_jemalloc_ctl::raw::update(PROF_ACTIVE, true)? };
    Ok(())
}

fn profiler_stop() -> Result<(), Error> {
    tracing::info!("stopping jemalloc profiler");
    unsafe { tikv_jemalloc_ctl::raw::update(PROF_ACTIVE, false)? };
    Ok(())
}

/// Dump the profile to the `path`.
fn profiler_dump<P>(path: P) -> Result<(), Error>
where
    P: AsRef<Path>,
{
    let path = path.as_ref();
    let mut bytes = CString::new(path.as_os_str().as_bytes())
        .unwrap()
        .into_bytes_with_nul();

    let ptr = bytes.as_mut_ptr().cast::<c_char>();
    unsafe { tikv_jemalloc_ctl::raw::write(PROF_DUMP, ptr)? };

    tracing::info!(path = %path.display(), "saved the jemalloc profiling dump");
    Ok(())
}

const PROF_ACTIVE: &[u8] = b"prof.active\0";
const PROF_DUMP: &[u8] = b"prof.dump\0";
