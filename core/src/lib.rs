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
                    if to_line < self.from_line {
                        tracing::warn!(
                            elapsed_ms = elapsed.as_millis(), name = % self.name, file =
                            % self.file, from_line = % self.from_line, to_line = %
                            to_line,
                            "long poll (loop wraparound: lines {}-end then start-{})",
                            self.from_line, to_line
                        );
                    } else {
                        tracing::warn!(
                            elapsed_ms = elapsed.as_millis(), name = % self.name, file =
                            % self.file, from_line = % self.from_line, to_line = %
                            to_line, "long poll"
                        );
                    }
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
pub mod block_strider;
pub mod blockchain_rpc;
pub mod global_config;
pub mod node;
pub mod overlay_client;
pub mod proto;
pub mod storage;
