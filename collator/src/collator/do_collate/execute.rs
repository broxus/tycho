use std::time::Duration;

use anyhow::Result;
use everscale_types::models::{TickTock, WorkUnitsParamsExecute, WorkUnitsParamsPrepare};

use super::execution_wrapper::ExecutorWrapper;
use super::finalize::FinalizeState;
use super::phase::{Phase, PhaseState};
use crate::collator::messages_reader::{GetNextMessageGroupMode, MessagesReader};
use crate::collator::types::{BlockCollationData, BlockLimitsLevel, ExecuteResult};
use crate::tracing_targets;

pub struct ExecuteState {
    pub messages_reader: MessagesReader,
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

    pub fn run(&mut self) -> Result<()> {
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
            let mut timer = std::time::Instant::now();
            let msg_group_opt = self
                .extra
                .messages_reader
                .get_next_message_group(GetNextMessageGroupMode::Continue)?;
            fill_msgs_total_elapsed += timer.elapsed();

            if let Some(msg_group) = msg_group_opt {
                // Execute messages group
                timer = std::time::Instant::now();
                let group_result = self.extra.executor.executor.execute_group(msg_group)?;
                execute_msgs_total_elapsed += timer.elapsed();
                executed_groups_count += 1;
                self.state.collation_data.tx_count += group_result.items.len() as u64;
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
                for item in group_result.items {
                    let new_messages = self.extra.executor.process_transaction(
                        item.executed,
                        Some(item.in_message),
                        &mut self.state.collation_data,
                    )?;
                    self.extra.messages_reader.add_new_messages(new_messages);
                }
                process_txs_total_elapsed += timer.elapsed();

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
            } else if !self.extra.messages_reader.has_messages_in_buffers()
                && !self
                    .extra
                    .messages_reader
                    .has_not_fully_read_internals_ranges()
                && !self.extra.messages_reader.has_pending_new_messages()
                && !self.extra.messages_reader.has_pending_externals_in_cache()
            {
                // stop reading message groups when all buffers are empty
                // and all internals ranges, new messages, and externals fully read as well
                break;
            }
        }

        self.state.collation_data.total_execute_msgs_time_mc =
            execute_msgs_total_elapsed.as_millis();

        // update counters
        self.state.collation_data.read_int_msgs_from_iterator_count = self
            .extra
            .messages_reader
            .metrics()
            .read_int_msgs_from_iterator_count;
        self.state.collation_data.read_ext_msgs_count =
            self.extra.messages_reader.metrics().read_ext_msgs_count;
        self.state.collation_data.read_new_msgs_from_iterator_count = self
            .extra
            .messages_reader
            .metrics()
            .read_new_msgs_from_iterator_count;

        metrics::gauge!("tycho_do_collate_exec_msgs_groups_per_block", &labels)
            .set(executed_groups_count as f64);

        metrics::histogram!("tycho_do_collate_fill_msgs_total_time", &labels)
            .record(fill_msgs_total_elapsed);

        let init_iterator_elapsed = self
            .extra
            .messages_reader
            .metrics()
            .init_iterator_total_elapsed;
        metrics::histogram!("tycho_do_collate_init_iterator_time", &labels)
            .record(init_iterator_elapsed);
        let read_existing_messages_elapsed = self
            .extra
            .messages_reader
            .metrics()
            .read_existing_messages_total_elapsed;
        metrics::histogram!("tycho_do_collate_read_int_msgs_time", &labels)
            .record(read_existing_messages_elapsed);
        let read_new_messages_elapsed = self
            .extra
            .messages_reader
            .metrics()
            .read_new_messages_total_elapsed;
        metrics::histogram!("tycho_do_collate_read_new_msgs_time", &labels)
            .record(read_new_messages_elapsed);
        let read_ext_messages_elapsed = self
            .extra
            .messages_reader
            .metrics()
            .read_ext_messages_total_elapsed;
        metrics::histogram!("tycho_do_collate_read_ext_msgs_time", &labels)
            .record(read_ext_messages_elapsed);
        let add_to_message_groups_elapsed = self
            .extra
            .messages_reader
            .metrics()
            .add_to_message_groups_total_elapsed;
        metrics::histogram!("tycho_do_collate_add_to_msg_groups_time", &labels)
            .record(add_to_message_groups_elapsed);

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

        let prepare_groups_wu_total = calc_prepare_groups_wu_total(
            &self.state.collation_data,
            &self.state.collation_config.work_units_params.prepare,
        );

        self.extra.execute_result = Some(ExecuteResult {
            execute_groups_wu_vm_only,
            process_txs_wu,
            execute_groups_wu_total,
            prepare_groups_wu_total,
            fill_msgs_total_elapsed,
            execute_msgs_total_elapsed,
            process_txs_total_elapsed,
            init_iterator_elapsed,
            read_existing_messages_elapsed,
            read_ext_messages_elapsed,
            read_new_messages_elapsed,
            add_to_message_groups_elapsed,
            last_read_to_anchor_chain_time,
        });

        Ok(())
    }

    pub fn finish(self) -> (Phase<FinalizeState>, MessagesReader) {
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
}

fn calc_process_txs_wu(
    collation_data: &BlockCollationData,
    wu_params_execute: &WorkUnitsParamsExecute,
) -> u64 {
    let &WorkUnitsParamsExecute {
        serialize_enqueue,
        serialize_dequeue,
        insert_new_msgs_to_iterator,
        ..
    } = wu_params_execute;

    (collation_data.int_enqueue_count)
        .saturating_mul(serialize_enqueue as u64)
        .saturating_add((collation_data.int_dequeue_count).saturating_mul(serialize_dequeue as u64))
        .saturating_add(
            (collation_data.inserted_new_msgs_to_iterator_count)
                .saturating_mul(insert_new_msgs_to_iterator as u64),
        )
}

fn calc_prepare_groups_wu_total(
    collation_data: &BlockCollationData,
    wu_params_prepare: &WorkUnitsParamsPrepare,
) -> u64 {
    let &WorkUnitsParamsPrepare {
        fixed_part,
        read_ext_msgs,
        read_int_msgs,
        read_new_msgs,
    } = wu_params_prepare;

    (fixed_part as u64)
        .saturating_add(
            collation_data
                .read_ext_msgs_count
                .saturating_mul(read_ext_msgs as u64),
        )
        .saturating_add(
            collation_data
                .read_int_msgs_from_iterator_count
                .saturating_mul(read_int_msgs as u64),
        )
        .saturating_add(
            collation_data
                .read_new_msgs_from_iterator_count
                .saturating_mul(read_new_msgs as u64),
        )
}
