use std::time::Duration;

use anyhow::Result;
use everscale_types::models::{TickTock, WorkUnitsParamsExecute, WorkUnitsParamsPrepare};

use super::execution_wrapper::ExecutorWrapper;
use super::finalize::FinalizeState;
use super::phase::{Phase, PhaseState};
use super::BlockCollationData;
use crate::collator::execution_manager::{
    GetNextMessageGroupContext, GetNextMessageGroupMode, MessagesReader,
};
use crate::collator::types::{AnchorsCache, BlockLimitsLevel, ExecuteResult};
use crate::tracing_targets;
use crate::types::DisplayExternalsProcessedUpto;

pub struct ExecuteState {
    pub messages_reader: MessagesReader,
    pub execute_result: Option<ExecuteResult>,
    pub anchors_cache: AnchorsCache,
    pub executor: ExecutorWrapper,
}

impl PhaseState for ExecuteState {}

impl Phase<ExecuteState> {
    pub fn execute_special_transactions(&mut self) -> Result<()> {
        self.extra
            .executor
            .create_special_transactions(&self.state.mc_data.config, &mut self.state.collation_data)
    }

    pub fn execute_tick_transaction(&mut self) -> Result<()> {
        self.extra.executor.create_ticktock_transactions(
            &self.state.mc_data.config,
            TickTock::Tick,
            &mut self.state.collation_data,
        )
    }

    pub fn execute_tock_transaction(&mut self) -> Result<()> {
        self.extra.executor.create_ticktock_transactions(
            &self.state.mc_data.config,
            TickTock::Tock,
            &mut self.state.collation_data,
        )
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
            let msgs_group_opt = self.extra.messages_reader.get_next_message_group(
                GetNextMessageGroupContext {
                    next_chain_time: self.state.collation_data.get_gen_chain_time(),
                    max_new_message_key_to_current_shard: self
                        .extra
                        .executor
                        .max_new_message_key_to_current_shard,
                    mode: GetNextMessageGroupMode::Continue,
                },
                &mut self.state.collation_data.processed_upto,
                &mut self.state.msgs_buffer,
                &mut self.extra.anchors_cache,
                &mut self.extra.executor.mq_iterator_adapter,
            )?;
            fill_msgs_total_elapsed += timer.elapsed();

            if let Some(msgs_group) = msgs_group_opt {
                // Execute messages group
                timer = std::time::Instant::now();
                let group_result = self.extra.executor.executor.execute_group(msgs_group)?;
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
                    self.extra.executor.process_transaction(
                        item.executor_output,
                        Some(item.in_message),
                        &mut self.state.collation_data,
                    )?;
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
            } else if self
                .extra
                .executor
                .mq_iterator_adapter
                .no_pending_existing_internals()
                && self
                    .extra
                    .executor
                    .mq_iterator_adapter
                    .no_pending_new_messages()
            {
                break;
            }
        }

        self.state.collation_data.total_execute_msgs_time_mc =
            execute_msgs_total_elapsed.as_millis();

        // update counters
        self.state.collation_data.read_int_msgs_from_iterator_count = self
            .extra
            .messages_reader
            .read_int_msgs_from_iterator_count();
        self.state.collation_data.read_ext_msgs_count =
            self.extra.messages_reader.read_ext_msgs_count();
        self.state.collation_data.read_new_msgs_from_iterator_count = self
            .extra
            .messages_reader
            .read_new_msgs_from_iterator_count();

        metrics::gauge!("tycho_do_collate_exec_msgs_groups_per_block", &labels)
            .set(executed_groups_count as f64);

        metrics::histogram!("tycho_do_collate_fill_msgs_total_time", &labels)
            .record(fill_msgs_total_elapsed);

        let init_iterator_elapsed = self
            .extra
            .executor
            .mq_iterator_adapter
            .init_iterator_total_elapsed();
        metrics::histogram!("tycho_do_collate_init_iterator_time", &labels)
            .record(init_iterator_elapsed);
        let read_existing_messages_elapsed = self
            .extra
            .messages_reader
            .read_existing_messages_total_elapsed();
        metrics::histogram!("tycho_do_collate_read_int_msgs_time", &labels)
            .record(read_existing_messages_elapsed);
        let read_new_messages_elapsed =
            self.extra.messages_reader.read_new_messages_total_elapsed();
        metrics::histogram!("tycho_do_collate_read_new_msgs_time", &labels)
            .record(read_new_messages_elapsed);
        let read_ext_messages_elapsed =
            self.extra.messages_reader.read_ext_messages_total_elapsed();
        metrics::histogram!("tycho_do_collate_read_ext_msgs_time", &labels)
            .record(read_ext_messages_elapsed);
        let add_to_message_groups_elapsed = self
            .extra
            .messages_reader
            .add_to_message_groups_total_elapsed();
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

        // show updated processed upto
        tracing::debug!(target: tracing_targets::COLLATOR, "updated processed_upto.offset = {}",
            self.state.collation_data.processed_upto.processed_offset,
        );
        tracing::debug!(target: tracing_targets::COLLATOR, "updated processed_upto.externals = {:?}",
            self.state.collation_data.processed_upto.externals.as_ref().map(DisplayExternalsProcessedUpto),
        );
        self.state
            .collation_data
            .processed_upto
            .internals
            .iter()
            .for_each(|(shard_ident, processed_upto)| {
                tracing::debug!(target: tracing_targets::COLLATOR,
                    "updated processed_upto.internals for shard {}: {}",
                    shard_ident, processed_upto,
                );
            });

        Ok(())
    }

    pub fn destruct(self) -> (Phase<FinalizeState>, AnchorsCache, ExecutorWrapper) {
        (
            Phase::<FinalizeState> {
                extra: FinalizeState {
                    execute_result: self.extra.execute_result.unwrap(),
                },
                state: self.state,
            },
            self.extra.anchors_cache,
            self.extra.executor,
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
