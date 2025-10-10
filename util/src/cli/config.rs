use std::num::NonZeroUsize;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(default)]
pub struct ThreadPoolConfig {
    rayon_threads: NonZeroUsize,
    tokio_workers: NonZeroUsize,
    // How many threads to use for dropping heavy objects in the background Default is 4.
    reclaimer_threads: NonZeroUsize,
    // How many objects to keep in the reclaimer queue before blocking the main thread. Default is 10.
    reclaimer_queue_depth: NonZeroUsize,
}

impl ThreadPoolConfig {
    // don't assign unique names to threads, for example using indexes:
    // that way they'll be merged into a pretty-looking single one in flame graphs

    pub fn init_global_rayon_pool(&self) -> Result<(), rayon::ThreadPoolBuildError> {
        rayon::ThreadPoolBuilder::new()
            .stack_size(8 * 1024 * 1024)
            .thread_name(|_| "rayon_worker".to_string())
            .num_threads(self.rayon_threads.get())
            .build_global()
    }

    pub fn build_tokio_runtime(&self) -> std::io::Result<tokio::runtime::Runtime> {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(self.tokio_workers.get())
            .build()
    }

    pub fn init_reclaimer(&self) -> Result<(), crate::mem::ReclaimerError> {
        crate::mem::Reclaimer::init(self.reclaimer_queue_depth, self.reclaimer_threads)
    }
}

impl Default for ThreadPoolConfig {
    fn default() -> Self {
        let total_threads =
            std::thread::available_parallelism().expect("failed to get total threads");
        Self {
            rayon_threads: total_threads,
            tokio_workers: total_threads,
            reclaimer_threads: NonZeroUsize::new(4).unwrap(),
            reclaimer_queue_depth: NonZeroUsize::new(10).unwrap(),
        }
    }
}
