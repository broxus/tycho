use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};

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
    pub fn init_global_rayon_pool(&self) -> Result<(), rayon::ThreadPoolBuildError> {
        let mut rayon_threads = self.rayon_threads.get();
        if self.reserve_mempool_rayon {
            rayon_threads = rayon_threads.div_ceil(2);
        }
        rayon::ThreadPoolBuilder::new()
            .stack_size(8 * 1024 * 1024)
            .thread_name(|id| format!("rayon-global-{id}"))
            .num_threads(rayon_threads)
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
            .thread_name_fn(|| {
                static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
                let id = ATOMIC_ID.fetch_add(1, Ordering::Relaxed);
                format!("tokio-{id}")
            })
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
