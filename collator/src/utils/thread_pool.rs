use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::OnceLock;

pub fn set_num_threads(num_threads: usize) {
    NUM_THREADS.store(num_threads, Ordering::Release);
}

pub fn get() -> &'static rayon::ThreadPool {
    static THREAD_POOL: OnceLock<rayon::ThreadPool> = OnceLock::new();
    THREAD_POOL.get_or_init(|| {
        rayon::ThreadPoolBuilder::new()
            .stack_size(8 * 1024 * 1024)
            .thread_name(|_| "rayon_executor_worker".to_string())
            .num_threads(NUM_THREADS.load(Ordering::Acquire))
            .build()
            .unwrap()
    })
}

static NUM_THREADS: AtomicUsize = AtomicUsize::new(8);
