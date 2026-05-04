use std::sync::Arc;

use tycho_block_util::state::ShardStateStuff;

use super::CollationManager;
use crate::collator::CollatorFactory;
use crate::tracing_targets;
use crate::validator::Validator;

#[derive(Default)]
pub(super) struct CancelValidationRunnerState {
    running: bool,
    pending: Option<ShardStateStuff>,
}

impl<CF, V> CollationManager<CF, V>
where
    CF: CollatorFactory,
    V: Validator,
{
    pub(super) fn schedule_cancel_validation_sessions_until_block(
        self: &Arc<Self>,
        state: ShardStateStuff,
    ) {
        // will make this only for master blocks
        if !state.block_id().is_masterchain() {
            return;
        }

        let mut guard = self.cancel_validation_runner.lock();

        // schedule next task if cancellation is already running
        if guard.running {
            guard.pending = Some(state);
            return;
        }

        // run validation cancellation in backgound
        guard.running = true;
        drop(guard);

        let mgr = self.clone();
        tokio::spawn(async move {
            mgr.run_cancel_validation_sessions_until_block(state).await;
        });
    }

    async fn run_cancel_validation_sessions_until_block(
        self: Arc<Self>,
        mut state: ShardStateStuff,
    ) {
        loop {
            // execute validation cancellation
            if let Err(e) = self.cancel_validation_sessions_until_block(state) {
                tracing::error!(
                    target: tracing_targets::COLLATION_MANAGER,
                    "failed to cancel validation sessions: {e:?}",
                );
            }

            // get next task if exists
            let next = {
                let mut guard = self.cancel_validation_runner.lock();

                match guard.pending.take() {
                    Some(next) => Some(next),
                    None => {
                        guard.running = false;
                        None
                    }
                }
            };

            // run next cancellation task or exit
            match next {
                Some(next) => state = next,
                None => break,
            }
        }
    }
}
