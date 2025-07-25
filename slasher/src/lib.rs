use std::sync::Arc;

use tycho_slasher_traits::ValidatorEventsListener;

use self::collector::ValidatorEventsCollector;

pub mod collector {
    pub use self::validator_events::*;

    mod validator_events;
    // TODO: mod mempool_events;
}

// NOTE: Stub
#[derive(Default)]
pub struct Slasher {
    validator_events_collector: Arc<ValidatorEventsCollector>,
}

impl Slasher {
    pub fn validator_events_listener(&self) -> Arc<dyn ValidatorEventsListener> {
        self.validator_events_collector.clone()
    }
}
