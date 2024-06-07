use std::time::Instant;

#[must_use = "The guard is used to update the histogram when it is dropped"]
pub struct HistogramGuard {
    name: &'static str,
    started_at: Instant,
}

impl HistogramGuard {
    pub fn begin(name: &'static str) -> Self {
        Self {
            name,
            started_at: Instant::now(),
        }
    }
}

impl Drop for HistogramGuard {
    fn drop(&mut self) {
        metrics::histogram!(self.name).record(self.started_at.elapsed());
    }
}
