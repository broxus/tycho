use std::time::Duration;

use anyhow::Result;
use tycho_types::models::TickTock;
use tycho_util::metrics::HistogramGuard;

use super::execution_wrapper::ExecutorWrapper;
use super::finalize::FinalizeState;
use super::phase::{Phase, PhaseState};
use super::work_units::PrepareMsgGroupsWu;
use crate::collator::do_collate::work_units::ExecuteWu;
use crate::collator::error::{CollationCancelReason, CollatorError};
use crate::collator::messages_reader::{
    GetNextMessageGroupMode, MessagesReader, MessagesReaderMetrics,
};
use crate::collator::types::{BlockLimitsLevel, ExecuteMetrics, ExecuteResult};
use crate::internal_queue::types::EnqueuedMessage;
use crate::tracing_targets;

pub struct ExecuteState {
    pub messages_reader: MessagesReader<EnqueuedMessage>,
    pub executor: ExecutorWrapper,
    pub prepare_msg_groups_wu: Option<PrepareMsgGroupsWu>,
    /// Accumulated messages reader metrics across all partitions
    pub msgs_reader_metrics: Option<MessagesReaderMetrics>,
    pub execute_wu: ExecuteWu,
    pub execute_metrics: ExecuteMetrics,
}

impl PhaseState for ExecuteState {}

impl Phase<ExecuteState> {
    pub fn execute_special_transactions(&mut self) -> Result<()> {
        let labels = [(
            "workchain",
            self.extra.executor.shard_id.workchain().to_string(),
        )];
        let histogram =
            HistogramGuard::begin_with_labels("tycho_do_collate_execute_special_time", &labels);
        let new_messages = self.extra.executor.create_special_transactions(
            &self.state.mc_data.config,
            &mut self.state.collation_data,
        )?;
        self.extra.messages_reader.add_new_messages(new_messages);
        self.extra.execute_metrics.execute_special_elapsed = histogram.finish();
        Ok(())
    }

    pub fn execute_tick_transactions(&mut self) -> Result<()> {
        let labels = [(
            "workchain",
            self.extra.executor.shard_id.workchain().to_string(),
        )];
        let histogram =
            HistogramGuard::begin_with_labels("tycho_do_collate_execute_tick_time", &labels);
        let new_messages = self.extra.executor.create_ticktock_transactions(
            &self.state.mc_data.config,
            TickTock::Tick,
            &mut self.state.collation_data,
        )?;
        self.extra.messages_reader.add_new_messages(new_messages);
        self.extra.execute_metrics.execute_tick_elapsed = histogram.finish();
        Ok(())
    }

    pub fn execute_tock_transactions(&mut self) -> Result<()> {
        let labels = [(
            "workchain",
            self.extra.executor.shard_id.workchain().to_string(),
        )];
        let histogram =
            HistogramGuard::begin_with_labels("tycho_do_collate_execute_tock_time", &labels);
        let new_messages = self.extra.executor.create_ticktock_transactions(
            &self.state.mc_data.config,
            TickTock::Tock,
            &mut self.state.collation_data,
        )?;
        self.extra.messages_reader.add_new_messages(new_messages);
        self.extra.execute_metrics.execute_tock_elapsed = histogram.finish();
        Ok(())
    }

    pub fn execute_incoming_messages(&mut self) -> Result<(), CollatorError> {
        let labels = [(
            "workchain",
            self.extra.executor.shard_id.workchain().to_string(),
        )];
        let histogram =
            HistogramGuard::begin_with_labels("tycho_do_collate_execute_time_high", &labels);

        let mut fill_msgs_total_elapsed = Duration::ZERO;

        let mut executed_groups_count = 0;
        loop {
            // exit collation if cancelled
            if self.state.collation_is_cancelled.check() {
                return Err(CollatorError::Cancelled(
                    CollationCancelReason::ExternalCancel,
                ));
            }

            let timer = std::time::Instant::now();
            let msg_group_opt = self.extra.messages_reader.get_next_message_group(
                GetNextMessageGroupMode::Continue,
                self.extra.executor.executor.min_next_lt(),
            )?;
            fill_msgs_total_elapsed += timer.elapsed();

            if let Some(msg_group) = msg_group_opt {
                // Execute messages group
                self.extra
                    .execute_metrics
                    .execute_groups_vm_only_timer
                    .start();
                let group_result = self.extra.executor.executor.execute_group(msg_group)?;
                self.extra
                    .execute_metrics
                    .execute_groups_vm_only_timer
                    .stop();

                executed_groups_count += 1;
                let group_tx_count = group_result.items.len();
                self.state.collation_data.tx_count += group_tx_count as u64;
                self.state.collation_data.ext_msgs_error_count += group_result.ext_msgs_error_count;
                self.state.collation_data.ext_msgs_skipped_count += group_result.ext_msgs_skipped;

                self.extra.execute_wu.append_executed_group(
                    &self.state.collation_config.work_units_params.execute,
                    &group_result,
                );

                // Process transactions
                self.extra.execute_metrics.process_txs_timer.start();
                let mut new_messages_created_count = 0;
                for item in group_result.items {
                    let new_messages = self.extra.executor.process_transaction(
                        item.executed,
                        Some(item.in_message),
                        &mut self.state.collation_data,
                    )?;
                    new_messages_created_count += new_messages.len();
                    self.extra.messages_reader.add_new_messages(new_messages);
                }
                self.extra.execute_metrics.process_txs_timer.stop();

                tracing::debug!(target: tracing_targets::COLLATOR,
                    executed_groups_count,
                    group_tx_count,
                    ext_msgs_error_count = group_result.ext_msgs_error_count,
                    ext_msgs_skipped_count = group_result.ext_msgs_skipped,
                    new_messages_created_count,
                    "message group executed",
                );

                if self
                    .state
                    .collation_data
                    .block_limit
                    .reached(BlockLimitsLevel::Hard)
                {
                    tracing::debug!(target: tracing_targets::COLLATOR,
                        "block limits reached: {:?}", self.state.collation_data.block_limit,
                    );
                    metrics::counter!("tycho_do_collate_blocks_with_limits_reached_count", &labels)
                        .increment(1);
                    break;
                }
            } else if !self
                .extra
                .messages_reader
                .can_read_and_collect_more_messages()
            {
                // stop reading message groups when all buffers are empty
                // and all internals ranges, new messages, and externals fully read as well
                break;
            }
        }

        self.extra.execute_metrics.execute_incoming_msgs_elapsed = histogram.finish();

        // metrics
        metrics::gauge!("tycho_do_collate_exec_msgs_groups_per_block", &labels)
            .set(executed_groups_count as f64);
        metrics::histogram!("tycho_do_collate_fill_msgs_total_time_high", &labels)
            .record(fill_msgs_total_elapsed);
        metrics::histogram!("tycho_do_collate_exec_msgs_total_time_high", &labels).record(
            self.extra
                .execute_metrics
                .execute_groups_vm_only_timer
                .total_elapsed,
        );
        metrics::histogram!("tycho_do_collate_process_txs_total_time_high", &labels)
            .record(self.extra.execute_metrics.process_txs_timer.total_elapsed);
        self.state.collation_data.total_execute_msgs_time_mc = self
            .extra
            .execute_metrics
            .execute_groups_total_elapsed()
            .as_millis();

        // calc prepare wu
        let msgs_reader_metrics = self
            .extra
            .messages_reader
            .metrics_by_partitions()
            .get_total();
        let prepare_msg_groups_wu = PrepareMsgGroupsWu::calculate(
            &self.state.collation_config.work_units_params.prepare,
            &msgs_reader_metrics,
            fill_msgs_total_elapsed,
        );

        self.extra.prepare_msg_groups_wu = Some(prepare_msg_groups_wu);
        self.extra.msgs_reader_metrics = Some(msgs_reader_metrics);

        // calc execute wu
        self.extra.execute_wu.calculate(
            &self.state.collation_config.work_units_params.execute,
            &self.extra.execute_metrics,
            &self.state.collation_data,
        );

        Ok(())
    }

    pub fn finish(self) -> (Phase<FinalizeState>, MessagesReader<EnqueuedMessage>) {
        self.report_execute_metrics();
        let executor = self.extra.executor.executor;
        (
            Phase::<FinalizeState> {
                extra: FinalizeState {
                    executor,
                    execute_result: ExecuteResult {
                        prepare_msg_groups_wu: self.extra.prepare_msg_groups_wu.unwrap(),
                        msgs_reader_metrics: self.extra.msgs_reader_metrics.unwrap(),
                        execute_wu: self.extra.execute_wu,
                        execute_metrics: self.extra.execute_metrics,
                    },
                    finalize_wu: Default::default(),
                    finalize_metrics: Default::default(),
                },
                state: self.state,
            },
            self.extra.messages_reader,
        )
    }

    fn report_execute_metrics(&self) {
        let shard_id = self.extra.executor.shard_id;
        let labels = [("workchain", shard_id.workchain().to_string())];

        metrics::counter!("tycho_do_collate_tx_total", &labels)
            .increment(self.state.collation_data.tx_count);
        metrics::gauge!("tycho_do_collate_tx_per_block", &labels)
            .set(self.state.collation_data.tx_count as f64);

        metrics::counter!("tycho_do_collate_int_enqueue_count")
            .increment(self.state.collation_data.int_enqueue_count);
        metrics::counter!("tycho_do_collate_int_dequeue_count")
            .increment(self.state.collation_data.int_dequeue_count);
        metrics::counter!("tycho_do_collate_msgs_exec_count_all", &labels)
            .increment(self.state.collation_data.execute_count_all);

        // messages metrics by partitions
        for (par_id, par_metrics) in self.extra.messages_reader.metrics_by_partitions().iter() {
            if par_id.is_zero() {
                metrics::counter!("tycho_do_collate_msgs_read_count_int", &labels)
                    .increment(par_metrics.read_existing_msgs_count);
                metrics::counter!("tycho_do_collate_msgs_read_count_new_int", &labels)
                    .increment(par_metrics.read_new_msgs_count);
                metrics::counter!("tycho_do_collate_msgs_read_count_ext", &labels)
                    .increment(par_metrics.read_ext_msgs_count);
            } else {
                let labels = [
                    ("workchain", shard_id.workchain().to_string()),
                    ("par_id", par_id.to_string()),
                ];
                metrics::counter!(
                    "tycho_do_collate_msgs_read_count_int_by_partitions",
                    &labels
                )
                .increment(par_metrics.read_existing_msgs_count);
                metrics::counter!(
                    "tycho_do_collate_msgs_read_count_new_int_by_partitions",
                    &labels
                )
                .increment(par_metrics.read_new_msgs_count);
                metrics::counter!(
                    "tycho_do_collate_msgs_read_count_ext_by_partitions",
                    &labels
                )
                .increment(par_metrics.read_ext_msgs_count);
            }
        }

        // total messages metrics
        // externals
        metrics::counter!("tycho_do_collate_msgs_exec_count_ext", &labels)
            .increment(self.state.collation_data.execute_count_ext);
        metrics::counter!("tycho_do_collate_msgs_error_count_ext", &labels)
            .increment(self.state.collation_data.ext_msgs_error_count);
        metrics::counter!("tycho_do_collate_msgs_skipped_count_ext", &labels)
            .increment(self.state.collation_data.ext_msgs_skipped_count);

        // existing internals messages
        metrics::counter!("tycho_do_collate_msgs_exec_count_int", &labels)
            .increment(self.state.collation_data.execute_count_int);

        // new internals messages
        metrics::counter!("tycho_do_collate_new_msgs_created_count", &labels)
            .increment(self.state.collation_data.new_msgs_created_count);
        metrics::counter!("tycho_do_collate_new_msgs_inserted_count", &labels)
            .increment(self.state.collation_data.inserted_new_msgs_count);
        metrics::counter!("tycho_do_collate_msgs_exec_count_new_int", &labels)
            .increment(self.state.collation_data.execute_count_new_int);

        // messages collecting timings
        if let Some(metrics) = &self.extra.msgs_reader_metrics {
            metrics::histogram!("tycho_do_collate_init_iterator_time_high", &labels)
                .record(metrics.init_iterator_timer.total_elapsed);
            metrics::histogram!("tycho_do_collate_read_int_msgs_time_high", &labels)
                .record(metrics.read_existing_messages_timer.total_elapsed);
            metrics::histogram!("tycho_do_collate_read_new_msgs_time_high", &labels)
                .record(metrics.read_new_messages_timer.total_elapsed);
            metrics::histogram!("tycho_do_collate_read_ext_msgs_time_high", &labels)
                .record(metrics.read_ext_messages_timer.total_elapsed);
            metrics::histogram!("tycho_do_collate_add_to_msg_groups_time_high", &labels)
                .record(metrics.add_to_message_groups_timer.total_elapsed);
        }
    }
}
