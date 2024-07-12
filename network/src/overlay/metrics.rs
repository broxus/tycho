use metrics::Counter;

#[derive(Default)]
pub(super) enum Metrics {
    Enabled {
        tx: Counter,
        rx: Counter,
    },
    #[default]
    Disabled,
}

impl Metrics {
    pub fn new(name: &'static str, label: &'static str) -> Self {
        Metrics::Enabled {
            tx: metrics::counter!(format!("{name}_tx"), "service" => label),
            rx: metrics::counter!(format!("{name}_rx"), "service" => label),
        }
    }

    pub fn record_tx(&self, value: usize) {
        if let Metrics::Enabled { tx, .. } = self {
            tx.increment(value as u64);
        }
    }

    pub fn record_rx(&self, value: usize) {
        if let Metrics::Enabled { rx, .. } = self {
            rx.increment(value as u64);
        }
    }
}
