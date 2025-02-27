use std::time::Duration;

use anyhow::Result;
use everscale_types::models::{TickTock, WorkUnitsParamsExecute};

use super::execution_wrapper::ExecutorWrapper;
use super::finalize::FinalizeState;
use super::phase::{Phase, PhaseState};
use super::work_units::PrepareMsgGroupsWu;
use crate::collator::error::{CollationCancelReason, CollatorError};
use crate::collator::messages_reader::{GetNextMessageGroupMode, MessagesReader};
use crate::collator::types::{BlockCollationData, BlockLimitsLevel, ExecuteResult};
use crate::internal_queue::types::EnqueuedMessage;
use crate::tracing_targets;

pub struct ExecuteState {
    pub messages_reader: MessagesReader<EnqueuedMessage>,
    pub execute_result: Option<ExecuteResult>,
    pub executor: ExecutorWrapper,
}

impl PhaseState for ExecuteState {}

impl Phase<ExecuteState> {
    pub fn execute_special_transactions(&mut self) -> Result<()> {
        let new_messages = self.extra.executor.create_special_transactions(
            &self.state.mc_data.config,
            &mut self.state.collation_data,
        )?;
        self.extra.messages_reader.add_new_messages(new_messages);
        Ok(())
    }

    pub fn execute_tick_transactions(&mut self) -> Result<()> {
        let new_messages = self.extra.executor.create_ticktock_transactions(
            &self.state.mc_data.config,
            TickTock::Tick,
            &mut self.state.collation_data,
        )?;
        self.extra.messages_reader.add_new_messages(new_messages);
        Ok(())
    }

    pub fn execute_tock_transactions(&mut self) -> Result<()> {
        let new_messages = self.extra.executor.create_ticktock_transactions(
            &self.state.mc_data.config,
            TickTock::Tock,
            &mut self.state.collation_data,
        )?;
        self.extra.messages_reader.add_new_messages(new_messages);
        Ok(())
    }

    pub fn run(&mut self) -> Result<(), CollatorError> {
        let labels = [(
            "workchain",
            self.extra.executor.shard_id.workchain().to_string(),
        )];

        let mut fill_msgs_total_elapsed = Duration::ZERO;
        let mut execute_msgs_total_elapsed = Duration::ZERO;
        let mut process_txs_total_elapsed = Duration::ZERO;
        let mut execute_groups_wu_vm_only = 0u64;

        let mut executed_groups_count = 0;
        loop {
            // exit collation if cancelled
            if self.state.collation_is_cancelled.check() {
                return Err(CollatorError::Cancelled(
                    CollationCancelReason::ExternalCancel,
                ));
            }

            let mut timer = std::time::Instant::now();
            let msg_group_opt = self.extra.messages_reader.get_next_message_group(
                GetNextMessageGroupMode::Continue,
                self.extra.executor.executor.min_next_lt(),
            )?;
            fill_msgs_total_elapsed += timer.elapsed();

            if let Some(msg_group) = msg_group_opt {
                // Execute messages group
                timer = std::time::Instant::now();
                let group_result = self.extra.executor.executor.execute_group(msg_group)?;
                execute_msgs_total_elapsed += timer.elapsed();
                executed_groups_count += 1;
                let group_tx_count = group_result.items.len();
                self.state.collation_data.tx_count += group_tx_count as u64;
                self.state.collation_data.ext_msgs_error_count += group_result.ext_msgs_error_count;
                self.state.collation_data.ext_msgs_skipped_count += group_result.ext_msgs_skipped;
                execute_groups_wu_vm_only = execute_groups_wu_vm_only
                    .saturating_add(group_result.total_exec_wu)
                    .saturating_add(
                        group_result.ext_msgs_error_count.saturating_mul(
                            self.state
                                .collation_config
                                .work_units_params
                                .execute
                                .execute_err as u64,
                        ),
                    );

                // Process transactions
                timer = std::time::Instant::now();
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
                process_txs_total_elapsed += timer.elapsed();

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

        // metrics
        self.state.collation_data.total_execute_msgs_time_mc =
            execute_msgs_total_elapsed.as_millis();
        metrics::gauge!("tycho_do_collate_exec_msgs_groups_per_block", &labels)
            .set(executed_groups_count as f64);
        metrics::histogram!("tycho_do_collate_fill_msgs_total_time", &labels)
            .record(fill_msgs_total_elapsed);
        metrics::histogram!("tycho_do_collate_exec_msgs_total_time", &labels)
            .record(execute_msgs_total_elapsed);
        metrics::histogram!("tycho_do_collate_process_txs_total_time", &labels)
            .record(process_txs_total_elapsed);

        let last_read_to_anchor_chain_time =
            self.extra.messages_reader.last_read_to_anchor_chain_time();

        let process_txs_wu = calc_process_txs_wu(
            &self.state.collation_data,
            &self.state.collation_config.work_units_params.execute,
        );
        let execute_groups_wu_total = execute_groups_wu_vm_only.saturating_add(process_txs_wu);

        let msgs_reader_metrics_total = self
            .extra
            .messages_reader
            .metrics_by_partitions()
            .get_total();

        let prepare_msg_groups_wu = PrepareMsgGroupsWu::calculate(
            &self.state.collation_config.work_units_params.prepare,
            &msgs_reader_metrics_total,
            fill_msgs_total_elapsed,
        );

        self.extra.execute_result = Some(ExecuteResult {
            execute_groups_wu_vm_only,
            process_txs_wu,
            execute_groups_wu_total,
            prepare_msg_groups_wu,
            execute_msgs_total_elapsed,
            process_txs_total_elapsed,
            msgs_reader_metrics: msgs_reader_metrics_total,
            last_read_to_anchor_chain_time,
        });

        Ok(())
    }

    pub fn finish(self) -> (Phase<FinalizeState>, MessagesReader<EnqueuedMessage>) {
        self.report_execute_metrics();
        let executor = self.extra.executor.executor;
        (
            Phase::<FinalizeState> {
                extra: FinalizeState {
                    execute_result: self.extra.execute_result.unwrap(),
                    executor,
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
        metrics::gauge!("tycho_do_collate_int_msgs_queue_calc").increment(
            (self.state.collation_data.int_enqueue_count as i64
                - self.state.collation_data.int_dequeue_count as i64) as f64,
        );
        metrics::counter!("tycho_do_collate_msgs_exec_count_all", &labels)
            .increment(self.state.collation_data.execute_count_all);

        // messages metrics by partitions
        for (par_id, par_metrics) in self.extra.messages_reader.metrics_by_partitions().iter() {
            if *par_id == 0 {
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
        if let Some(ExecuteResult {
            msgs_reader_metrics: metrics,
            ..
        }) = &self.extra.execute_result
        {
            metrics::histogram!("tycho_do_collate_init_iterator_time", &labels)
                .record(metrics.init_iterator_timer.total_elapsed);
            metrics::histogram!("tycho_do_collate_read_int_msgs_time", &labels)
                .record(metrics.read_existing_messages_timer.total_elapsed);
            metrics::histogram!("tycho_do_collate_read_new_msgs_time", &labels)
                .record(metrics.read_new_messages_timer.total_elapsed);
            metrics::histogram!("tycho_do_collate_read_ext_msgs_time", &labels)
                .record(metrics.read_ext_messages_timer.total_elapsed);
            metrics::histogram!("tycho_do_collate_add_to_msg_groups_time", &labels)
                .record(metrics.add_to_message_groups_timer.total_elapsed);
        }
    }
}

fn calc_process_txs_wu(
    collation_data: &BlockCollationData,
    wu_params_execute: &WorkUnitsParamsExecute,
) -> u64 {
    let &WorkUnitsParamsExecute {
        serialize_enqueue,
        serialize_dequeue,
        insert_new_msgs,
        ..
    } = wu_params_execute;

    (collation_data.int_enqueue_count)
        .saturating_mul(serialize_enqueue as u64)
        .saturating_add((collation_data.int_dequeue_count).saturating_mul(serialize_dequeue as u64))
        .saturating_add(
            (collation_data.inserted_new_msgs_count).saturating_mul(insert_new_msgs as u64),
        )
}
