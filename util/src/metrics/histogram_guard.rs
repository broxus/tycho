use std::time::{Duration, Instant};

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

    pub fn begin_with_labels<'a, T>(
        name: &'static str,
        labels: &'a T,
    ) -> HistogramGuardWithLabels<'a, T>
    where
        &'a T: metrics::IntoLabels,
    {
        HistogramGuardWithLabels::begin(name, labels)
    }

    pub fn begin_with_labels_owned<T>(
        name: &'static str,
        labels: T,
    ) -> HistogramGuardWithLabelsOwned<T>
    where
        T: metrics::IntoLabels,
    {
        HistogramGuardWithLabelsOwned::begin(name, labels)
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
pub struct HistogramGuardWithLabels<'a, T: 'static>
where
    &'a T: metrics::IntoLabels,
{
    name: Option<&'static str>,
    started_at: Instant,
    labels: &'a T,
}

impl<'a, T> HistogramGuardWithLabels<'a, T>
where
    &'a T: metrics::IntoLabels,
{
    pub fn begin(name: &'static str, labels: &'a T) -> Self {
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

impl<'a, T> Drop for HistogramGuardWithLabels<'a, T>
where
    &'a T: metrics::IntoLabels,
{
    fn drop(&mut self) {
        if let Some(name) = self.name.take() {
            metrics::histogram!(name, self.labels).record(self.started_at.elapsed());
        }
    }
}

#[must_use = "The guard is used to update the histogram when it is dropped"]
pub struct HistogramGuardWithLabelsOwned<T>
where
    T: metrics::IntoLabels,
{
    name: Option<&'static str>,
    started_at: Instant,
    labels: Option<T>,
}

impl<T> HistogramGuardWithLabelsOwned<T>
where
    T: metrics::IntoLabels,
{
    pub fn begin(name: &'static str, labels: T) -> Self {
        Self {
            name: Some(name),
            started_at: Instant::now(),
            labels: Some(labels),
        }
    }

    pub fn finish(mut self) -> Duration {
        let duration = self.started_at.elapsed();
        if let Some(name) = self.name.take() {
            let labels = self.labels.take().unwrap();
            metrics::histogram!(name, labels).record(duration);
        }
        duration
    }
}

impl<T> Drop for HistogramGuardWithLabelsOwned<T>
where
    T: metrics::IntoLabels,
{
    fn drop(&mut self) {
        if let Some(name) = self.name.take() {
            let labels = self.labels.take().unwrap();
            metrics::histogram!(name, labels).record(self.started_at.elapsed());
        }
    }
}
