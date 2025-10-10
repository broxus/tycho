use std::time::{Duration, Instant};

#[must_use = "The guard is used to update the histogram when it is dropped"]
pub struct HistogramGuard {
    name: Option<&'static str>,
    started_at: Instant,
    armed: bool,
}

impl HistogramGuard {
    pub fn begin(name: &'static str) -> Self {
        Self {
            name: Some(name),
            started_at: Instant::now(),
            armed: true,
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

    pub fn finish(mut self) -> Duration {
        self.finish_internal()
    }

    pub fn finish_ref(&mut self) -> Duration {
        self.finish_internal()
    }

    fn finish_internal(&mut self) -> Duration {
        let duration = self.started_at.elapsed();
        if self.armed {
            if let Some(name) = self.name {
                metrics::histogram!(name).record(duration);
            }
            self.armed = false;
        }
        duration
    }

    pub fn reset(&mut self) {
        if self.name.is_some() {
            self.started_at = Instant::now();
            self.armed = true;
        }
    }
}

impl Drop for HistogramGuard {
    fn drop(&mut self) {
        self.finish_internal();
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
    armed: bool,
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
            armed: true,
        }
    }

    pub fn finish(mut self) -> Duration {
        self.finish_internal()
    }

    pub fn finish_ref(&mut self) -> Duration {
        self.finish_internal()
    }

    fn finish_internal(&mut self) -> Duration {
        let duration = self.started_at.elapsed();
        if self.armed {
            if let Some(name) = self.name {
                metrics::histogram!(name, self.labels).record(duration);
            }
            self.armed = false;
        }
        duration
    }

    pub fn reset(&mut self) {
        if self.name.is_some() {
            self.started_at = Instant::now();
            self.armed = true;
        }
    }
}

impl<'a, T> Drop for HistogramGuardWithLabels<'a, T>
where
    &'a T: metrics::IntoLabels,
{
    fn drop(&mut self) {
        self.finish_internal();
    }
}
