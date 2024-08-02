use std::ffi::{c_char, CString};
use std::os::unix::prelude::OsStrExt;
use std::sync::Arc;

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

#[derive(Clone)]
pub struct JemallocMemoryProfiler {
    inner: Arc<Inner>,
}

impl JemallocMemoryProfiler {
    pub fn connect() -> Option<Self> {
        if std::env::var("MALLOC_CONF").is_err() {
            tracing::warn!(
                "MALLOC_CONF is not set, memory profiler is disabled. \
                set MALLOC_CONF=prof:true to enable"
            );
            return None;
        }

        let Ok::<bool, _>(active) = (unsafe { tikv_jemalloc_ctl::raw::read(PROF_ACTIVE) }) else {
            tracing::error!("failed to read memory profiler state");
            return None;
        };

        Some(Self {
            inner: Arc::new(Inner {
                active: tokio::sync::Mutex::new(active),
            }),
        })
    }
}

#[async_trait::async_trait]
impl tycho_control::MemoryProfiler for JemallocMemoryProfiler {
    async fn set_enabled(&self, enabled: bool) -> bool {
        let mut state = self.inner.active.lock().await;
        match unsafe { tikv_jemalloc_ctl::raw::update(PROF_ACTIVE, enabled) } {
            Ok(was_enabled) => {
                *state = enabled;
                was_enabled != enabled
            }
            Err(e) => {
                tracing::error!("failed to update memory profiler state: {e:?}");
                false
            }
        }
    }

    async fn dump(&self) -> anyhow::Result<Vec<u8>> {
        let state = self.inner.active.lock().await;
        anyhow::ensure!(*state, "memory profiler is not active");

        // TODO: Revisit this. What if the system deletes this temp file?
        let temp_file = tempfile::NamedTempFile::new()?;

        let path = temp_file.path();
        {
            let mut bytes = CString::new(path.as_os_str().as_bytes())
                .unwrap()
                .into_bytes_with_nul();

            let ptr = bytes.as_mut_ptr().cast::<c_char>();
            if let Err(e) = unsafe { tikv_jemalloc_ctl::raw::write(PROF_DUMP, ptr) } {
                anyhow::bail!("failed to dump jemalloc profiling data: {e:?}");
            }
        }

        tracing::info!(path = %path.display(), "saved the jemalloc profiling dump");

        let data = tokio::fs::read(path).await?;
        Ok(data)
    }
}

struct Inner {
    active: tokio::sync::Mutex<bool>,
}

const PROF_ACTIVE: &[u8] = b"prof.active\0";
const PROF_DUMP: &[u8] = b"prof.dump\0";
