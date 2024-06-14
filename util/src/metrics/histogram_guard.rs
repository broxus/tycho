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

#[must_use = "The guard is used to update the histogram when it is dropped"]
pub struct HistogramGuardWithLabels<'a, T: 'static>
where
    &'a T: metrics::IntoLabels,
{
    name: &'static str,
    started_at: Instant,
    labels: &'a T,
}

impl<'a, T> HistogramGuardWithLabels<'a, T>
where
    &'a T: metrics::IntoLabels,
{
    pub fn begin(name: &'static str, labels: &'a T) -> Self {
        Self {
            name,
            started_at: Instant::now(),
            labels,
        }
    }
}

impl<'a, T> Drop for HistogramGuardWithLabels<'a, T>
where
    &'a T: metrics::IntoLabels,
{
    fn drop(&mut self) {
        metrics::histogram!(self.name, self.labels).record(self.started_at.elapsed());
    }
}
