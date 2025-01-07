use std::ffi::{c_char, CString};
use std::os::unix::prelude::OsStrExt;
use std::sync::Arc;

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
