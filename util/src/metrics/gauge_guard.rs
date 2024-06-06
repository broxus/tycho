#[must_use = "The guard is used to decrement the gauge when it is dropped"]
pub struct GaugeGuard {
    name: &'static str,
    n: f64,
}

impl GaugeGuard {
    pub fn increment<T: metrics::IntoF64>(name: &'static str, n: T) -> Self {
        let n = n.into_f64();
        metrics::gauge!(name).increment(n);
        Self { name, n }
    }
}

impl Drop for GaugeGuard {
    fn drop(&mut self) {
        metrics::gauge!(self.name).decrement(self.n);
    }
}
