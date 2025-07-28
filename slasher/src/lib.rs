use std::sync::Arc;

use tycho_slasher_traits::{MempoolEventsCache, MempoolEventsListener, ValidatorEventsListener};

use self::collector::ValidatorEventsCollector;
use crate::collector::MempoolStatsCache;

pub mod collector {
    pub use mempool_events::*;
    pub use validator_events::*;

    mod mempool_events;
    mod validator_events;
}

// NOTE: Stub
#[derive(Default)]
pub struct Slasher {
    validator_events_collector: Arc<ValidatorEventsCollector>,
    mempool_stats_collector: Arc<MempoolStatsCache>,
}

impl Slasher {
    pub fn validator_events_listener(&self) -> Arc<dyn ValidatorEventsListener> {
        self.validator_events_collector.clone()
    }
    pub fn mempool_events_listener(&self) -> Arc<dyn MempoolEventsListener> {
        self.mempool_stats_collector.clone()
    }
    pub fn mempool_events_cache(&self) -> Arc<dyn MempoolEventsCache> {
        self.mempool_stats_collector.clone()
    }
}
