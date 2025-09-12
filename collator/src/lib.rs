#[doc(hidden)]
#[allow(dead_code)]
mod __async_profile_guard__ {
    use std::time::{Duration, Instant};
    const THRESHOLD_MS: u64 = 10u64;
    pub struct Guard {
        name: &'static str,
        file: &'static str,
        from_line: u32,
        current_start: Option<Instant>,
    }
    impl Guard {
        pub fn new(name: &'static str, file: &'static str, line: u32) -> Self {
            Guard {
                name,
                file,
                from_line: line,
                current_start: Some(Instant::now()),
            }
        }
        pub fn end_section(&mut self, to_line: u32) {
            if let Some(start) = self.current_start.take() {
                let elapsed = start.elapsed();
                if elapsed > Duration::from_millis(THRESHOLD_MS) {
                    tracing::warn!(
                        elapsed_ms = elapsed.as_millis(), name = % self.name, file = %
                        self.file, from_line = % self.from_line, to_line = % to_line,
                        "long poll"
                    );
                }
            }
        }
        pub fn start_section(&mut self, new_line: u32) {
            self.from_line = new_line;
            self.current_start = Some(Instant::now());
        }
    }
    impl Drop for Guard {
        fn drop(&mut self) {
            if let Some(start) = self.current_start {
                let elapsed = start.elapsed();
                if elapsed > Duration::from_millis(THRESHOLD_MS) {
                    tracing::warn!(
                        elapsed_ms = elapsed.as_millis(), name = % self.name, file = %
                        self.file, from_line = % self.from_line, to_line = % self
                        .from_line, "long poll"
                    );
                }
            }
        }
    }
}
pub mod collator;
pub mod internal_queue;
pub mod manager;
pub mod mempool;
pub mod state_node;
pub mod storage;
pub mod types;
pub mod validator;
#[cfg(any(test, feature = "test"))]
pub mod test_utils;
pub mod queue_adapter;
mod tracing_targets;
pub mod utils;
