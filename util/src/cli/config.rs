use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(default)]
pub struct ThreadPoolConfig {
    pub rayon_threads: usize,
    pub tokio_workers: usize,
}

impl ThreadPoolConfig {
    pub fn init_global_rayon_pool(&self) -> Result<(), rayon::ThreadPoolBuildError> {
        rayon::ThreadPoolBuilder::new()
            .stack_size(8 * 1024 * 1024)
            .thread_name(|_| "rayon_worker".to_string())
            .num_threads(self.rayon_threads)
            .build_global()
    }

    pub fn build_tokio_runtime(&self) -> std::io::Result<tokio::runtime::Runtime> {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(self.tokio_workers)
            .build()
    }
}

impl Default for ThreadPoolConfig {
    fn default() -> Self {
        let total_threads = std::thread::available_parallelism()
            .expect("failed to get total threads")
            .get();
        Self {
            rayon_threads: total_threads,
            tokio_workers: total_threads,
        }
    }
}
