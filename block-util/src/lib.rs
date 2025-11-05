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
        consecutive_hits: u32,
    }
    impl Guard {
        pub fn new(name: &'static str, file: &'static str, line: u32) -> Self {
            Guard {
                name,
                file,
                from_line: line,
                current_start: Some(Instant::now()),
                consecutive_hits: 0,
            }
        }
        pub fn checkpoint(&mut self, new_line: u32) {
            if let Some(start) = self.current_start.take() {
                let elapsed = start.elapsed();
                if elapsed > Duration::from_millis(THRESHOLD_MS) {
                    self.consecutive_hits = self.consecutive_hits.saturating_add(1);
                    let span = format!(
                        "{file}:{from}-{to}", file = self.file, from = self.from_line, to
                        = new_line
                    );
                    let wraparound = new_line < self.from_line;
                    if wraparound {
                        tracing::warn!(
                            elapsed_ms = elapsed.as_millis(), name = % self.name, span =
                            % span, hits = self.consecutive_hits, wraparound =
                            wraparound, "long poll (iteration tail wraparound)"
                        );
                    } else {
                        tracing::warn!(
                            elapsed_ms = elapsed.as_millis(), name = % self.name, span =
                            % span, hits = self.consecutive_hits, wraparound =
                            wraparound, "long poll (iteration tail)"
                        );
                    }
                } else {
                    self.consecutive_hits = 0;
                }
            }
            self.from_line = new_line;
            self.current_start = Some(Instant::now());
        }
        pub fn end_section(&mut self, to_line: u32) {
            if let Some(start) = self.current_start.take() {
                let elapsed = start.elapsed();
                if elapsed > Duration::from_millis(THRESHOLD_MS) {
                    self.consecutive_hits = self.consecutive_hits.saturating_add(1);
                    let span = format!(
                        "{file}:{from}-{to}", file = self.file, from = self.from_line, to
                        = to_line
                    );
                    let wraparound = to_line < self.from_line;
                    if wraparound {
                        tracing::warn!(
                            elapsed_ms = elapsed.as_millis(), name = % self.name, span =
                            % span, hits = self.consecutive_hits, wraparound =
                            wraparound, "long poll (loop wraparound)"
                        );
                    } else {
                        tracing::warn!(
                            elapsed_ms = elapsed.as_millis(), name = % self.name, span =
                            % span, hits = self.consecutive_hits, wraparound =
                            wraparound, "long poll"
                        );
                    }
                } else {
                    self.consecutive_hits = 0;
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
                    self.consecutive_hits = self.consecutive_hits.saturating_add(1);
                    let span = format!(
                        "{file}:{line}-{line}", file = self.file, line = self.from_line
                    );
                    tracing::warn!(
                        elapsed_ms = elapsed.as_millis(), name = % self.name, span = %
                        span, hits = self.consecutive_hits, wraparound = false,
                        "long poll"
                    );
                }
            }
        }
    }
}
pub mod archive;
pub mod block;
pub mod config;
pub mod dict;
pub mod message;
pub mod queue;
pub mod state;
pub mod tl;
