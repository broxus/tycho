use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use everscale_types::models::{ShardIdent, TickTock};
use tycho_block_util::queue::{QueueDiffStuff, QueueKey};
use tycho_util::futures::JoinTask;
use tycho_util::metrics::HistogramGuard;

use super::execution_wrapper::ExecutorWrapper;
use super::phase::{Phase, PhaseState};
use crate::collator::mq_iterator_adapter::QueueIteratorAdapter;
use crate::collator::types::UpdateQueueDiffResult;
use crate::internal_queue::types::EnqueuedMessage;
use crate::queue_adapter::MessageQueueAdapter;
use crate::tracing_targets;

pub struct ExecuteResult {
    pub execute_groups_wu_vm_only: u64,
    pub process_txs_wu: u64,
    pub execute_groups_wu_total: u64,
    pub prepare_groups_wu_total: u64,
    pub fill_msgs_total_elapsed: Duration,
    pub execute_msgs_total_elapsed: Duration,
    pub process_txs_total_elapsed: Duration,
    pub init_iterator_elapsed: Duration,
    pub read_existing_messages_elapsed: Duration,
    pub read_ext_messages_elapsed: Duration,
    pub read_new_messages_elapsed: Duration,
    pub add_to_message_groups_elapsed: Duration,

    pub last_read_to_anchor_chain_time: Option<u64>,
}

pub struct ExecuteState {
    pub execute_result: ExecuteResult,
}

impl PhaseState for ExecuteState {}

impl Phase<ExecuteState> {
    pub async fn execute_special_transactions(
        &mut self,
        executor_wrapper: &mut ExecutorWrapper,
    ) -> Result<()> {
        executor_wrapper
            .create_ticktock_transactions(
                &self.state.mc_data.config,
                TickTock::Tock,
                &mut self.state.collation_data,
            )
            .await?;

        Ok(())
    }

    pub async fn update_queue_diff(
        &mut self,
        mq_iterator_adapter: QueueIteratorAdapter<EnqueuedMessage>,
        shard_id: ShardIdent,
        mq_adapter: Arc<dyn MessageQueueAdapter<EnqueuedMessage>>,
    ) -> Result<UpdateQueueDiffResult> {
        let labels = [("workchain", shard_id.workchain().to_string())];

        let prev_hash = self
            .state
            .prev_shard_data
            .prev_queue_diff_hashes()
            .first()
            .cloned()
            .unwrap_or_default();

        // get queue diff and check for pending internals
        let histogram_create_queue_diff =
            HistogramGuard::begin_with_labels("tycho_do_collate_create_queue_diff_time", &labels);

        let (has_pending_internals_in_iterator, diff_with_messages) = mq_iterator_adapter.release(
            !self.state.msgs_buffer.has_pending_messages(),
            &mut self.state.msgs_buffer.current_iterator_positions,
        )?;

        let create_queue_diff_elapsed = histogram_create_queue_diff.finish();

        let diff_messages_len = diff_with_messages.messages.len();
        let has_unprocessed_messages =
            self.state.msgs_buffer.has_pending_messages() || has_pending_internals_in_iterator;

        let (min_message, max_message) = {
            let messages = &diff_with_messages.messages;
            match messages.first_key_value().zip(messages.last_key_value()) {
                Some(((min, _), (max, _))) => (*min, *max),
                None => (
                    QueueKey::min_for_lt(self.state.collation_data.start_lt),
                    QueueKey::max_for_lt(self.state.collation_data.next_lt),
                ),
            }
        };

        let queue_diff = QueueDiffStuff::builder(
            shard_id,
            self.state.collation_data.block_id_short.seqno,
            &prev_hash,
        )
        .with_processed_upto(
            diff_with_messages
                .processed_upto
                .iter()
                .map(|(k, v)| (*k, v.lt, &v.hash)),
        )
        .with_messages(
            &min_message,
            &max_message,
            diff_with_messages.messages.keys().map(|k| &k.hash),
        )
        .serialize();

        let queue_diff_hash = *queue_diff.hash();
        tracing::debug!(target: tracing_targets::COLLATOR, queue_diff_hash = %queue_diff_hash);

        // start async update queue task
        let update_queue_task: JoinTask<std::result::Result<Duration, anyhow::Error>> =
            JoinTask::<Result<_>>::new({
                let block_id_short = self.state.collation_data.block_id_short;
                let labels = labels.clone();
                async move {
                    // apply queue diff
                    let histogram = HistogramGuard::begin_with_labels(
                        "tycho_do_collate_apply_queue_diff_time",
                        &labels,
                    );

                    mq_adapter.apply_diff(diff_with_messages, block_id_short, &queue_diff_hash)?;
                    let apply_queue_diff_elapsed = histogram.finish();

                    Ok(apply_queue_diff_elapsed)
                }
            });

        Ok(UpdateQueueDiffResult {
            queue_diff,
            update_queue_task,
            has_unprocessed_messages,
            diff_messages_len,
            create_queue_diff_elapsed,
        })
    }
}
