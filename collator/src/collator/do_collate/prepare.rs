use std::time::Duration;

use anyhow::Result;
use everscale_types::models::{TickTock, WorkUnitsParamsExecute, WorkUnitsParamsPrepare};

use super::execute::{ExecuteResult, ExecuteState};
use super::execution_wrapper::ExecutorWrapper;
use super::phase::{Phase, PhaseState};
use super::BlockCollationData;
use crate::collator::execution_manager::{GetNextMessageGroupMode, MessagesReader};
use crate::collator::types::{AnchorsCache, BlockLimitsLevel};
use crate::tracing_targets;

pub struct PreparedState {
    pub messages_reader: MessagesReader,
}

impl PhaseState for PreparedState {}

impl Phase<PreparedState> {
    pub async fn execute_special_transactions(
        &mut self,
        executor_wrapper: &mut ExecutorWrapper,
    ) -> Result<()> {
        executor_wrapper
            .create_ticktock_transactions(
                &self.state.mc_data.config,
                TickTock::Tick,
                &mut self.state.collation_data,
            )
            .await?;

        executor_wrapper
            .create_special_transactions(&self.state.mc_data.config, &mut self.state.collation_data)
            .await?;

        Ok(())
    }

    pub async fn execute(
        mut self,
        anchors_cache: &mut AnchorsCache,
        executor_wrapper: &mut ExecutorWrapper,
    ) -> Result<Phase<ExecuteState>> {
        let labels = [(
            "workchain",
            executor_wrapper.shard_id.workchain().to_string(),
        )];

        let mut fill_msgs_total_elapsed = Duration::ZERO;
        let mut execute_msgs_total_elapsed = Duration::ZERO;
        let mut process_txs_total_elapsed = Duration::ZERO;
        let mut execute_groups_wu_vm_only = 0u64;

        let mut executed_groups_count = 0;
        loop {
            let mut timer = std::time::Instant::now();
            let msgs_group_opt = self
                .extra
                .messages_reader
                .get_next_message_group(
                    &mut self.state.msgs_buffer,
                    anchors_cache,
                    &mut self.state.collation_data,
                    &mut executor_wrapper.mq_iterator_adapter,
                    &executor_wrapper.max_new_message_key_to_current_shard,
                    GetNextMessageGroupMode::Continue,
                )
                .await?;
            fill_msgs_total_elapsed += timer.elapsed();

            if let Some(msgs_group) = msgs_group_opt {
                // Execute messages group
                timer = std::time::Instant::now();
                let group_result = executor_wrapper.executor.execute_group(msgs_group).await?;
                execute_msgs_total_elapsed += timer.elapsed();
                executed_groups_count += 1;
                self.state.collation_data.tx_count += group_result.items.len() as u64;
                self.state.collation_data.ext_msgs_error_count += group_result.ext_msgs_error_count;
                self.state.collation_data.ext_msgs_skipped += group_result.ext_msgs_skipped;
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
                    executor_wrapper.process_transaction(
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
            } else if executor_wrapper
                .mq_iterator_adapter
                .no_pending_existing_internals()
                && executor_wrapper
                    .mq_iterator_adapter
                    .no_pending_new_messages()
            {
                break;
            }
        }

        self.state.collation_data.total_execute_msgs_time_mc =
            execute_msgs_total_elapsed.as_millis();

        metrics::gauge!("tycho_do_collate_exec_msgs_groups_per_block", &labels)
            .set(executed_groups_count as f64);

        metrics::histogram!("tycho_do_collate_fill_msgs_total_time", &labels)
            .record(fill_msgs_total_elapsed);

        let init_iterator_elapsed = executor_wrapper
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

        Ok(Phase::<ExecuteState> {
            extra: ExecuteState {
                execute_result: ExecuteResult {
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
                },
            },
            state: self.state,
        })
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
            (collation_data.inserted_new_msgs_to_iterator)
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
                .read_ext_msgs
                .saturating_mul(read_ext_msgs as u64),
        )
        .saturating_add(
            collation_data
                .read_int_msgs_from_iterator
                .saturating_mul(read_int_msgs as u64),
        )
        .saturating_add(
            collation_data
                .read_new_msgs_from_iterator
                .saturating_mul(read_new_msgs as u64),
        )
}
