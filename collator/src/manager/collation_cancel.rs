use std::sync::Arc;

use anyhow::Result;
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use tracing::Instrument;
use tycho_types::models::{BlockId, BlockIdShort};
use tycho_util::futures::JoinTask;

use crate::collator::{Collator, CollatorFactory, CollatorResult, DebugCollatorResult};
use crate::manager::types::CollatorJoinTask;
use crate::manager::{CollationManager, CollatorState, ProcessMcStateUpdateMode};
use crate::tracing_targets;
use crate::types::DebugDisplay;
use crate::types::processed_upto::BlockSeqno;
use crate::validator::Validator;

type CollationCancelResult = Result<()>;

#[derive(Debug)]
pub(super) enum ActionAfterCancel {
    Noop,
    SyncToAppliedMcBlock {
        trigger_block_id_short: BlockIdShort,
        last_collated_mc_block_id: Option<BlockId>,
        applied_range: Option<(BlockSeqno, BlockSeqno)>,
        process_state_update_mode: ProcessMcStateUpdateMode,
    },
}

pub(super) struct DisplayActionAfterCancel<'a>(pub &'a ActionAfterCancel);
impl std::fmt::Display for DisplayActionAfterCancel<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            ActionAfterCancel::SyncToAppliedMcBlock {
                trigger_block_id_short,
                last_collated_mc_block_id,
                applied_range,
                process_state_update_mode,
            } => f
                .debug_struct("SyncToAppliedMcBlock")
                .field("trigger_block_id", &DebugDisplay(trigger_block_id_short))
                .field("last_collated_mc_block_id", last_collated_mc_block_id)
                .field("applied_range", &applied_range)
                .field("process_state_update_mode", &process_state_update_mode)
                .finish(),
            ActionAfterCancel::Noop => "Noop".fmt(f),
        }
    }
}
impl std::fmt::Debug for DisplayActionAfterCancel<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

pub(super) fn update_action_after(
    curr_action: &mut Option<ActionAfterCancel>,
    new_action: Option<ActionAfterCancel>,
) -> bool {
    let Some(new_action) = new_action else {
        return false;
    };

    // sync must not be downgraded to a drain-only cancel
    if matches!(
        (curr_action.as_ref(), &new_action),
        (
            Some(ActionAfterCancel::SyncToAppliedMcBlock { .. }),
            ActionAfterCancel::Noop,
        )
    ) {
        return false;
    }

    *curr_action = Some(new_action);
    true
}

pub(super) struct CollationCancelHandle {
    action_after: Option<ActionAfterCancel>,
    task_wrapper: FuturesUnordered<JoinTask<CollationCancelResult>>,
}

impl CollationCancelHandle {
    pub fn new() -> Self {
        Self {
            action_after: Default::default(),
            task_wrapper: Default::default(),
        }
    }
}

impl CollationCancelHandle {
    #[tracing::instrument(name = "collation_cancel", skip_all, fields(collator_tasks_count = collator_tasks.len()))]
    pub(super) async fn start_or_update<CF: CollatorFactory, V: Validator>(
        &mut self,
        collation_manager: &Arc<CollationManager<CF, V>>,
        action: Option<ActionAfterCancel>,
        collator_tasks: &mut FuturesUnordered<CollatorJoinTask<CF::Collator>>,
    ) -> Result<Vec<CollatorJoinTask<CF::Collator>>> {
        // if current action exists,
        // previous cancel task is not finished or cancel result is not handled,
        // so we can update action
        if self.action_after.is_some() {
            let action_updated = update_action_after(&mut self.action_after, action);
            if action_updated {
                tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                    action_after = ?self.action_after.as_ref().map(DisplayActionAfterCancel),
                    "collation cancel task already exists, action updated",
                );
            }

            // if no new running collation tasks,
            // we can exit and let to handle previous cancel task result with updated action
            if collator_tasks.is_empty() {
                return Ok(vec![]);
            }
        } else {
            // no current action, no running cancel task
            assert!(
                self.task_wrapper.is_empty(),
                "cancel task should be already drained here"
            );

            if !update_action_after(&mut self.action_after, action) {
                // will not cancel if action after sync was not provided
                return Ok(vec![]);
            };

            // if no new running collation tasks,
            // then just handle action after cancel
            if collator_tasks.is_empty() {
                let action = self
                    .action_after
                    .take()
                    .expect("action after cancel should exist here");
                tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                    action_after = %DisplayActionAfterCancel(&action),
                    "no collator tasks, will handle action right now",
                );

                // mark every collator `Cancelled`
                // next sync will resume collation
                collation_manager.set_collators_state(
                    |_, ac| ac.state != CollatorState::Cancelled,
                    |ac| ac.state = CollatorState::Cancelled,
                );

                return collation_manager
                    .handle_collation_cancel_action(action)
                    .await;
            } else {
                // otherwise store action because we will run cancel task
            }
        }

        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            "collation cancel task spawned",
        );

        collation_manager.request_cancel_collations();

        // new collation tasks exist
        // we should cancel new collation tasks
        // awaiting previous cancel task before if exists
        let mut previous_task_wrapper = std::mem::take(&mut self.task_wrapper);
        let mut collator_tasks = std::mem::take(collator_tasks);
        let collation_manager = collation_manager.clone();

        let task = JoinTask::new(
            async move {
                // await prev cancel task
                while let Some(prev_task_res) = previous_task_wrapper.next().await {
                    prev_task_res?;
                }

                // await collation tasks
                while let Some(collator_res) = collator_tasks.next().await {
                    let (collator, res) = collator_res?;

                    tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                        next_block_id = %collator.next_block_id_short(),
                        "collator task cancelled for {}",
                        collator.shard_id(),
                    );
                    tracing::trace!(target: tracing_targets::COLLATION_MANAGER,
                        next_block_id = %collator.next_block_id_short(),
                        result = ?DebugCollatorResult(&res),
                        "cancelled collator task result for {}",
                        collator.shard_id(),
                    );

                    // clear uncomitted queue state on block candidate
                    if matches!(res, CollatorResult::BlockCandidate { .. }) {
                        collation_manager.clear_uncommitted_queue_state()?;
                    }

                    // store collator with `Cancelled` state
                    collation_manager.set_collator_and_state(collator, |ac| {
                        ac.state = CollatorState::Cancelled;
                    })?;
                }

                // mark every collator `Cancelled`
                // next sync will resume collation
                collation_manager.set_collators_state(
                    |_, ac| ac.state != CollatorState::Cancelled,
                    |ac| ac.state = CollatorState::Cancelled,
                );

                Ok::<_, anyhow::Error>(())
            }
            .instrument(tracing::Span::current()),
        );

        self.task_wrapper.push(task);

        Ok(vec![])
    }

    pub(super) fn is_empty(&self) -> bool {
        self.task_wrapper.is_empty()
    }

    pub(super) fn running_and_will_sync_after(&self) -> bool {
        !self.is_empty()
            && matches!(
                self.action_after,
                Some(ActionAfterCancel::SyncToAppliedMcBlock {
                    applied_range: Some(_),
                    ..
                })
            )
    }

    pub(super) async fn wait(&mut self) -> Option<Result<ActionAfterCancel>> {
        let res = self.task_wrapper.next().await;
        res.map(|cancel_res| {
            cancel_res.map(|_| {
                self.action_after
                    .take()
                    .expect("action after cancel should exist here")
            })
        })
    }
}

impl<CF, V> CollationManager<CF, V>
where
    CF: CollatorFactory,
    V: Validator,
{
    pub(super) async fn handle_new_collator_tasks_and_cancel_request(
        self: &Arc<Self>,
        collation_cancel_task: &mut CollationCancelHandle,
        collator_tasks_handle: &mut FuturesUnordered<CollatorJoinTask<CF::Collator>>,
        new_collator_tasks: Vec<CollatorJoinTask<CF::Collator>>,
        cancel_action: Option<ActionAfterCancel>,
    ) -> Result<()> {
        // collect new collator tasks
        for task in new_collator_tasks {
            collator_tasks_handle.push(task);
        }

        // run collation cancel task if requested
        for task in collation_cancel_task
            .start_or_update(self, cancel_action, collator_tasks_handle)
            .await?
        {
            collator_tasks_handle.push(task);
        }

        Ok(())
    }

    pub(super) async fn handle_collation_cancel_action(
        self: &Arc<Self>,
        action: ActionAfterCancel,
    ) -> Result<Vec<CollatorJoinTask<CF::Collator>>> {
        match action {
            ActionAfterCancel::SyncToAppliedMcBlock {
                trigger_block_id_short,
                last_collated_mc_block_id,
                applied_range,
                process_state_update_mode,
            } => {
                self.sync_to_applied_mc_block_if_exist(
                    trigger_block_id_short,
                    last_collated_mc_block_id,
                    applied_range,
                    process_state_update_mode,
                )
                .await
            }
            ActionAfterCancel::Noop => {
                // do nothing
                Ok(vec![])
            }
        }
    }
}
