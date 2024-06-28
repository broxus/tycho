use std::ffi::{c_char, CString};
use std::os::unix::prelude::OsStrExt;
use std::path::{Path, PathBuf};

pub use tikv_jemalloc_ctl::Error;
use tikv_jemalloc_ctl::{epoch, stats};

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

pub async fn memory_profiler(profiling_dir: PathBuf) {
    use tokio::signal::unix;

    if std::env::var("MALLOC_CONF").is_err() {
        tracing::warn!("MALLOC_CONF is not set, memory profiler is disabled");
        tracing::warn!("set MALLOC_CONF=prof:true to enable");
        return;
    }

    let signal = unix::SignalKind::user_defined1();
    let mut stream = unix::signal(signal).expect("failed to create signal stream");

    if let Err(e) = std::fs::create_dir_all(&profiling_dir) {
        tracing::error!(
            path=%profiling_dir.display(),
            "failed to create profiling directory: {e:?}"
        );
        return;
    }

    let mut is_active = false;
    while stream.recv().await.is_some() {
        tracing::info!("memory profiler signal received");
        if !is_active {
            tracing::info!("activating memory profiler");
            if let Err(e) = start() {
                tracing::error!("failed to activate memory profiler: {e:?}");
            }
        } else {
            let invocation_time = chrono::Local::now();
            let filename = format!("{}.dump", invocation_time.format("%Y-%m-%d_%H-%M-%S"));
            let path = profiling_dir.join(filename);
            if let Err(e) = dump(&path) {
                tracing::error!("failed to dump prof: {e:?}");
            }
            if let Err(e) = stop() {
                tracing::error!("failed to deactivate memory profiler: {e:?}");
            }
        }

        is_active = !is_active;
    }
}

const PROF_ACTIVE: &[u8] = b"prof.active\0";
const PROF_DUMP: &[u8] = b"prof.dump\0";

pub fn start() -> Result<(), Error> {
    tracing::info!("starting profiler");
    unsafe { tikv_jemalloc_ctl::raw::update(PROF_ACTIVE, true)? };
    Ok(())
}

pub fn stop() -> Result<(), Error> {
    tracing::info!("stopping profiler");
    unsafe { tikv_jemalloc_ctl::raw::update(PROF_ACTIVE, false)? };
    Ok(())
}

/// Dump the profile to the `path`.
pub fn dump<P>(path: P) -> Result<(), DumpError>
where
    P: AsRef<Path>,
{
    let path = path.as_ref();
    let mut bytes = CString::new(path.as_os_str().as_bytes())?.into_bytes_with_nul();

    let ptr = bytes.as_mut_ptr().cast::<c_char>();
    let res = unsafe { tikv_jemalloc_ctl::raw::write(PROF_DUMP, ptr) };
    match res {
        Ok(_) => {
            tracing::info!("saved the profiling dump to {}", path.display());
            Ok(())
        }
        Err(e) => {
            tracing::error!(
                "failed to dump the profiling info to {}: {e:?}",
                path.display()
            );
            Err(DumpError::JemallocError(e.to_string()))
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum DumpError {
    #[error("failed to dump the profiling info: {0}")]
    JemallocError(String),
    #[error("failed to convert path to CString: {0}")]
    NullError(#[from] std::ffi::NulError),
}
