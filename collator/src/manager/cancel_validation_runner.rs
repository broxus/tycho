use std::sync::Arc;

use anyhow::Result;
use tracing::Instrument;
use tycho_block_util::state::ShardStateStuff;

use super::CollationManager;
use crate::collator::CollatorFactory;
use crate::tracing_targets;
use crate::validator::{ValidationSessionId, Validator};

struct CancelValidationTask {
    state: ShardStateStuff,
    session_id: Option<ValidationSessionId>,
}

#[derive(Default)]
pub(super) struct CancelValidationRunnerState {
    running: bool,
    pending: Option<CancelValidationTask>,
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

        // capture session id and create cancellation task
        let session_id = self
            .active_collation_sessions
            .read()
            .get(&state.block_id().shard)
            .map(|session_info| session_info.get_validation_session_id());
        let task = CancelValidationTask { state, session_id };

        let mut guard = self.cancel_validation_runner.lock();

        // schedule next task if cancellation is already running
        if guard.running {
            guard.pending = Some(task);
            return;
        }

        // run validation cancellation in backgound
        guard.running = true;
        drop(guard);

        let mgr = self.clone();
        tokio::spawn(
            async move {
                mgr.run_cancel_validation_sessions_until_block(task).await;
            }
            .instrument(tracing::Span::current()),
        );
    }

    async fn run_cancel_validation_sessions_until_block(
        self: Arc<Self>,
        mut task: CancelValidationTask,
    ) {
        loop {
            // execute validation cancellation
            if let Err(e) = self.cancel_validation_sessions_until_block(task) {
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
                Some(next) => task = next,
                None => break,
            }
        }
    }

    #[tracing::instrument(skip_all, fields(block_id = %task.state.block_id().as_short_id()))]
    fn cancel_validation_sessions_until_block(&self, task: CancelValidationTask) -> Result<()> {
        let block_id = task.state.block_id().as_short_id();
        self.validator
            .cancel_validation(&block_id, task.session_id)?;
        Ok(())
    }
}
