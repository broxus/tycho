use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ThreadPoolConfig {
    pub executor_threads: usize,
    pub rayon_threads: usize,
    pub tokio_workers: usize,
}

impl Default for ThreadPoolConfig {
    fn default() -> Self {
        let total_threads = std::thread::available_parallelism()
            .expect("failed to get total threads")
            .get();
        let executor_threads = (total_threads / 2).clamp(4, 8);
        Self {
            executor_threads,
            rayon_threads: total_threads,
            tokio_workers: total_threads,
        }
    }
}
