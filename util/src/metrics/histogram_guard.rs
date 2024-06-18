use std::time::{Duration, Instant};

use metrics::{Label, SharedString};

#[must_use = "The guard is used to update the histogram when it is dropped"]
pub struct HistogramGuard {
    name: Option<&'static str>,
    started_at: Instant,
}

impl HistogramGuard {
    pub fn begin(name: &'static str) -> Self {
        Self {
            name: Some(name),
            started_at: Instant::now(),
        }
    }

    pub fn begin_with_labels<'a, const N: usize, K, V>(
        name: &'static str,
        labels: &'a [(K, V); N],
    ) -> HistogramGuardWithLabels<'a, N, K, V>
    where
        K: Into<SharedString> + Clone,
        V: Into<SharedString> + Clone,
    {
        HistogramGuardWithLabels::begin(name, labels)
    }

    pub fn finish(mut self) -> Duration {
        let duration = self.started_at.elapsed();
        if let Some(name) = self.name.take() {
            metrics::histogram!(name).record(duration);
        }
        duration
    }
}

impl Drop for HistogramGuard {
    fn drop(&mut self) {
        if let Some(name) = self.name.take() {
            metrics::histogram!(name).record(self.started_at.elapsed());
        }
    }
}

#[must_use = "The guard is used to update the histogram when it is dropped"]

pub struct HistogramGuardWithLabels<'a, const N: usize, K, V>
where
    K: Into<SharedString> + Clone,
    V: Into<SharedString> + Clone,
{
    name: Option<&'static str>,
    started_at: Instant,
    labels: &'a [(K, V); N],
}

impl<'a, const N: usize, K, V> HistogramGuardWithLabels<'a, N, K, V>
where
    K: Into<SharedString> + Clone,
    V: Into<SharedString> + Clone,
{
    pub fn begin(name: &'static str, labels: &'a [(K, V); N]) -> Self {
        Self {
            name: Some(name),
            started_at: Instant::now(),
            labels,
        }
    }

    pub fn finish(mut self) -> Duration {
        let duration = self.started_at.elapsed();
        if let Some(name) = self.name.take() {
            metrics::histogram!(name, self.labels).record(duration);
        }
        duration
    }
}

impl<'a, const N: usize, K, V> Drop for HistogramGuardWithLabels<'a, N, K, V>
where
    K: Into<SharedString> + Clone,
    V: Into<SharedString> + Clone,
{
    fn drop(&mut self) {
        if let Some(name) = self.name.take() {
            metrics::histogram!(name, self.labels).record(self.started_at.elapsed());
        }
    }
}
