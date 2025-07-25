use std::sync::Arc;

use tokio::task::AbortHandle;
use tycho_slasher_traits::ValidatorEventsListener;

use self::collector::ValidatorEventsCollector;

pub mod collector {
    pub use self::validator_events::*;

    mod validator_events;
    // TODO: mod mempool_events;
}

// NOTE: Stub
pub struct Slasher {
    validator_events_collector: Arc<ValidatorEventsCollector>,
    validator_events_task_handle: AbortHandle,
}

impl Slasher {
    pub fn new() -> Self {
        let collector = Arc::new(ValidatorEventsCollector::default());
        let collector_task = tokio::task::spawn(process_validator_events(collector.clone()));

        Self {
            validator_events_collector: collector,
            validator_events_task_handle: collector_task.abort_handle(),
        }
    }

    pub fn validator_events_listener(&self) -> Arc<dyn ValidatorEventsListener> {
        self.validator_events_collector.clone()
    }
}

impl Drop for Slasher {
    fn drop(&mut self) {
        self.validator_events_task_handle.abort();
    }
}

// === Tasks ===

#[tracing::instrument(skip_all)]
async fn process_validator_events(collector: Arc<ValidatorEventsCollector>) {
    tracing::info!("started");
    scopeguard::defer! { tracing::info!("finished"); };

    const BATCH_STEP: u32 = 100;

    let mut latest_block_seqno = collector.subscribe_to_latest_block_seqno();

    let mut processed_upto = 0u32;
    let mut buffer = Vec::with_capacity(BATCH_STEP as _);
    loop {
        let current_seqno = *latest_block_seqno.borrow_and_update();
        if current_seqno <= processed_upto + BATCH_STEP {
            latest_block_seqno
                .changed()
                .await
                .expect("sender is never dropped while `collector` is alive");
            continue;
        }

        buffer.clear();
        collector.take_batch(current_seqno, &mut buffer);

        // TODO: Build a voting matrix from completed blocks

        processed_upto = current_seqno;
    }
}
