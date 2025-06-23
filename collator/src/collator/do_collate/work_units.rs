use std::time::Duration;

use everscale_types::models::{
    WorkUnitsParamsExecute, WorkUnitsParamsFinalize, WorkUnitsParamsPrepare,
};

use crate::collator::execution_manager::ExecutedGroup;
use crate::collator::messages_reader::MessagesReaderMetrics;
use crate::collator::types::{BlockCollationData, ExecuteMetrics, FinalizeMetrics};
use crate::tracing_targets;

pub struct PrepareMsgGroupsWu {
    pub total_wu: u64,
    pub total_elapsed: Duration,

    pub read_ext_msgs_wu: u64,
    pub read_ext_msgs_elapsed: Duration,

    pub read_existing_int_msgs_wu: u64,
    pub read_existing_int_msgs_elapsed: Duration,

    pub read_new_int_msgs_wu: u64,
    pub read_new_int_msgs_elapsed: Duration,

    pub add_msgs_to_groups_wu: u64,
    pub add_msgs_to_groups_elapsed: Duration,
}

impl PrepareMsgGroupsWu {
    pub fn calculate(
        wu_params_prepare: &WorkUnitsParamsPrepare,
        msgs_reader_metrics: &MessagesReaderMetrics,
        prepare_msg_groups_total_elapsed: Duration,
    ) -> Self {
        let &WorkUnitsParamsPrepare {
            fixed_part,
            read_ext_msgs,
            read_int_msgs,
            read_new_msgs,
            add_to_msg_groups,
            ..
        } = wu_params_prepare;

        let read_ext_msgs_wu = msgs_reader_metrics
            .read_ext_msgs_count
            .saturating_mul(read_ext_msgs as u64);
        let read_existing_int_msgs_wu = msgs_reader_metrics
            .read_existing_msgs_count
            .saturating_mul(read_int_msgs as u64);
        let read_new_int_msgs_wu = msgs_reader_metrics
            .read_new_msgs_count
            .saturating_mul(read_new_msgs as u64);

        let add_msgs_to_groups_wu = msgs_reader_metrics
            .add_to_msgs_groups_ops_count
            .saturating_mul(add_to_msg_groups as u64);

        let total_wu = (fixed_part as u64)
            .saturating_add(read_ext_msgs_wu)
            .saturating_add(read_existing_int_msgs_wu)
            .saturating_add(read_new_int_msgs_wu)
            .saturating_add(add_msgs_to_groups_wu);

        let res = Self {
            total_wu,
            total_elapsed: prepare_msg_groups_total_elapsed,

            read_ext_msgs_wu,
            read_ext_msgs_elapsed: msgs_reader_metrics.read_ext_messages_timer.total_elapsed,

            read_existing_int_msgs_wu,
            read_existing_int_msgs_elapsed: msgs_reader_metrics
                .read_existing_messages_timer
                .total_elapsed,

            read_new_int_msgs_wu,
            read_new_int_msgs_elapsed: msgs_reader_metrics.read_new_messages_timer.total_elapsed,

            add_msgs_to_groups_wu,
            add_msgs_to_groups_elapsed: msgs_reader_metrics
                .add_to_message_groups_timer
                .total_elapsed,
        };

        tracing::debug!(target: tracing_targets::COLLATOR,
            "read_msg_groups_wu: total: (wu={}, elapsed={}, price={}), \
            read_ext_msgs: (count={}, param={}, wu={}, elapsed={}, price={}), \
            read_existing_int_msgs: (count={}, param={}, wu={}, elapsed={}, price={}), \
            read_new_int_msgs: (count={}, param={}, wu={}, elapsed={}, price={}), \
            add_to_msgs_groups_ops: (count={}, param={}, wu={}, elapsed={}, price={})",
            res.total_wu, res.total_elapsed.as_nanos(), res.total_wu_price(),
            msgs_reader_metrics.read_ext_msgs_count, read_ext_msgs,
            res.read_ext_msgs_wu, res.read_ext_msgs_elapsed.as_nanos(), res.read_ext_msgs_wu_price(),
            msgs_reader_metrics.read_existing_msgs_count, read_int_msgs,
            res.read_existing_int_msgs_wu, res.read_existing_int_msgs_elapsed.as_nanos(), res.read_existing_int_msgs_wu_price(),
            msgs_reader_metrics.read_new_msgs_count, read_new_msgs,
            res.read_new_int_msgs_wu, res.read_new_int_msgs_elapsed.as_nanos(), res.read_new_int_msgs_wu_price(),
            msgs_reader_metrics.add_to_msgs_groups_ops_count, add_to_msg_groups,
            res.add_msgs_to_groups_wu, res.add_msgs_to_groups_elapsed.as_nanos(), res.add_msgs_to_groups_wu_price(),
        );

        res
    }

    pub fn total_wu_price(&self) -> f64 {
        if self.total_wu == 0 {
            return 0.0;
        }
        self.total_elapsed.as_nanos() as f64 / self.total_wu as f64
    }
    pub fn read_ext_msgs_wu_price(&self) -> f64 {
        if self.read_ext_msgs_wu == 0 {
            return 0.0;
        }
        self.read_ext_msgs_elapsed.as_nanos() as f64 / self.read_ext_msgs_wu as f64
    }
    pub fn read_existing_int_msgs_wu_price(&self) -> f64 {
        if self.read_existing_int_msgs_wu == 0 {
            return 0.0;
        }
        self.read_existing_int_msgs_elapsed.as_nanos() as f64
            / self.read_existing_int_msgs_wu as f64
    }
    pub fn read_new_int_msgs_wu_price(&self) -> f64 {
        if self.read_new_int_msgs_wu == 0 {
            return 0.0;
        }
        self.read_new_int_msgs_elapsed.as_nanos() as f64 / self.read_new_int_msgs_wu as f64
    }
    pub fn add_msgs_to_groups_wu_price(&self) -> f64 {
        if self.add_msgs_to_groups_wu == 0 {
            return 0.0;
        }
        self.add_msgs_to_groups_elapsed.as_nanos() as f64 / self.add_msgs_to_groups_wu as f64
    }
}

#[derive(Default)]
pub struct ExecuteWu {
    pub execute_groups_vm_only_wu: u64,
    pub execute_groups_vm_only_elapsed: Duration,

    pub process_txs_wu: u64,
    pub process_txs_elapsed: Duration,
}
impl ExecuteWu {
    pub fn append_executed_group(
        &mut self,
        _wu_params_execute: &WorkUnitsParamsExecute,
        executed_group: &ExecutedGroup,
    ) {
        self.execute_groups_vm_only_wu = self
            .execute_groups_vm_only_wu
            .saturating_add(executed_group.total_exec_wu);
    }

    pub fn calculate(
        &mut self,
        wu_params_execute: &WorkUnitsParamsExecute,
        execute_metrics: &ExecuteMetrics,
        collation_data: &BlockCollationData,
    ) {
        let &WorkUnitsParamsExecute {
            serialize_enqueue: insert_in_msgs,
            serialize_dequeue: insert_out_msgs,
            insert_new_msgs,
            ..
        } = wu_params_execute;

        let in_msgs_len = collation_data.in_msgs.len();
        let in_msgs_len_log = in_msgs_len.checked_ilog2().unwrap_or_default() as usize;
        let out_msgs_len = collation_data.out_msgs.len();
        let out_msgs_len_log = out_msgs_len.checked_ilog2().unwrap_or_default() as usize;
        let inserted_new_msgs_count_log = collation_data
            .inserted_new_msgs_count
            .checked_ilog2()
            .unwrap_or_default() as usize;

        self.process_txs_wu = in_msgs_len
            .saturating_mul(in_msgs_len_log)
            .saturating_mul(insert_in_msgs as usize)
            .saturating_add(
                out_msgs_len
                    .saturating_mul(out_msgs_len_log)
                    .saturating_mul(insert_out_msgs as usize),
            )
            .saturating_add(
                (collation_data.inserted_new_msgs_count as usize)
                    .saturating_mul(inserted_new_msgs_count_log)
                    .saturating_mul(insert_new_msgs as usize),
            ) as u64;

        self.execute_groups_vm_only_elapsed =
            execute_metrics.execute_groups_vm_only_timer.total_elapsed;
        self.process_txs_elapsed = execute_metrics.process_txs_timer.total_elapsed;
    }

    pub fn total_wu(&self) -> u64 {
        self.execute_groups_vm_only_wu
            .saturating_add(self.process_txs_wu)
    }
    pub fn total_elapsed(&self) -> Duration {
        self.execute_groups_vm_only_elapsed
            .saturating_add(self.process_txs_elapsed)
    }
    pub fn total_wu_price(&self) -> f64 {
        let total_wu = self.total_wu();
        if total_wu == 0 {
            return 0.0;
        }
        self.total_elapsed().as_nanos() as f64 / total_wu as f64
    }

    pub fn execute_groups_vm_only_wu_price(&self) -> f64 {
        if self.execute_groups_vm_only_wu == 0 {
            return 0.0;
        }
        self.execute_groups_vm_only_elapsed.as_nanos() as f64
            / self.execute_groups_vm_only_wu as f64
    }

    pub fn process_txs_wu_price(&self) -> f64 {
        if self.process_txs_wu == 0 {
            return 0.0;
        }
        self.process_txs_elapsed.as_nanos() as f64 / self.process_txs_wu as f64
    }
}

#[derive(Default, Clone)]
pub struct FinalizeWu {
    pub create_queue_diff_wu: u64,
    pub create_queue_diff_elapsed: Duration,

    pub apply_queue_diff_wu: u64,
    pub apply_queue_diff_elapsed: Duration,

    pub build_shard_accounts_wu: u64,
    pub build_accounts_blocks_wu: u64,
    pub build_accounts_elapsed: Duration,

    pub build_in_msgs_wu: u64,
    pub build_in_msgs_elapsed: Duration,

    pub build_out_msgs_wu: u64,
    pub build_out_msgs_elapsed: Duration,

    pub build_accounts_and_messages_in_parallel_elased: Duration,

    pub build_state_update_wu: u64,
    pub build_state_update_elapsed: Duration,

    pub build_block_wu: u64,
    pub build_block_elapsed: Duration,

    pub finalize_block_elapsed: Duration,

    pub total_elapsed: Duration,
}
impl FinalizeWu {
    pub fn calculate_queue_diff_wu(
        &mut self,
        wu_params_finalize: &WorkUnitsParamsFinalize,
        diff_msgs_count: u64,
    ) {
        let &WorkUnitsParamsFinalize {
            create_diff,
            apply_diff,
            ..
        } = wu_params_finalize;

        self.create_queue_diff_wu = diff_msgs_count.saturating_mul(create_diff as u64);
        self.apply_queue_diff_wu = diff_msgs_count.saturating_mul(apply_diff as u64);
    }

    pub fn calculate_finalize_block_wu(
        &mut self,
        wu_params_finalize: &WorkUnitsParamsFinalize,
        wu_params_execute: &WorkUnitsParamsExecute,
        accounts_count: u64,
        in_msgs_len: u64,
        out_msgs_len: u64,
    ) {
        let &WorkUnitsParamsFinalize {
            build_accounts,
            build_transactions,
            build_in_msg,
            build_out_msg,
            serialize_min,
            serialize_accounts,
            serialize_msg,
            state_update_min,
            state_update_accounts,
            ..
        } = wu_params_finalize;

        let &WorkUnitsParamsExecute {
            subgroup_size: max_threads_count,
            ..
        } = wu_params_execute;

        let threads_count = (max_threads_count as u64).min(accounts_count).max(1);

        let accounts_count_log = accounts_count.checked_ilog2().unwrap_or_default() as u64;
        let in_msgs_len_log = in_msgs_len.checked_ilog2().unwrap_or_default() as u64;
        let out_msgs_len_log = out_msgs_len.checked_ilog2().unwrap_or_default() as u64;

        // TODO: should use accounts count in state to calc wu for updating shards accounts map

        // calc update shard accounts:
        //  * prepare account modifications in (accounts_count)
        //  * then update map of shard accounts in parallel in (accounts_count)*log(state_accounts_count)/(threads_count)
        self.build_shard_accounts_wu = accounts_count
            .saturating_add(
                accounts_count
                    .saturating_mul(accounts_count_log)
                    .saturating_div(threads_count),
            )
            .saturating_mul(build_accounts as u64);

        // calc build account blocks:
        //  * build maps of transactions by accounts in parallel in (txs_count)*log(txs_count/threads_count)/(threads_count)
        //  * then build AugDict of accounts blocks in (accounts_count)*log(accounts_count)
        let in_msgs_len_div_threads_count_log = in_msgs_len
            .saturating_div(threads_count)
            .checked_ilog2()
            .unwrap_or_default() as u64;
        self.build_accounts_blocks_wu = in_msgs_len
            .saturating_mul(in_msgs_len_div_threads_count_log)
            .saturating_div(threads_count)
            .saturating_add(accounts_count.saturating_mul(accounts_count_log))
            .saturating_mul(build_transactions as u64);

        // calc build in msgs:
        //  * build AugDict of in msgs in (in_msgs_len)*log(in_msgs_len)
        self.build_in_msgs_wu = in_msgs_len
            .saturating_mul(in_msgs_len_log)
            .saturating_mul(build_in_msg as u64);

        // calc build out msgs:
        //  * build AugDict of out msgs in (out_msgs_len)*log(out_msgs_len)
        self.build_out_msgs_wu = out_msgs_len
            .saturating_mul(out_msgs_len_log)
            .saturating_mul(build_out_msg as u64);

        // TODO: should use accounts count in state to calc wu for merkle update

        // calc build state update
        //  * calc update of all updated accounts in parallel in (accounts_count)*log(state_accounts_count)/(threads_count)
        //  * use min value if calculated is less
        self.build_state_update_wu = std::cmp::max(
            state_update_min as u64,
            accounts_count
                .saturating_mul(accounts_count_log)
                .saturating_div(threads_count)
                .saturating_mul(state_update_accounts as u64),
        );

        // calc build block
        //  * serialize each account block in (accounts_count) and use min value if calculated is less
        //  * serialize in msgs and out msgs in (in_msgs_len + out_msgs_len)
        self.build_block_wu = std::cmp::max(
            serialize_min as u64,
            accounts_count.saturating_mul(serialize_accounts as u64),
        )
        .saturating_add((in_msgs_len + out_msgs_len).saturating_mul(serialize_msg as u64));
    }

    pub fn append_elapsed_timings(&mut self, finalize_metrics: &FinalizeMetrics) {
        self.create_queue_diff_elapsed = finalize_metrics.create_queue_diff_elapsed;
        self.apply_queue_diff_elapsed = finalize_metrics.apply_queue_diff_elapsed;

        self.build_accounts_elapsed = finalize_metrics.build_accounts_elapsed;
        self.build_in_msgs_elapsed = finalize_metrics.build_in_msgs_elapsed;
        self.build_out_msgs_elapsed = finalize_metrics.build_out_msgs_elapsed;

        self.build_accounts_and_messages_in_parallel_elased =
            finalize_metrics.build_accounts_and_messages_in_parallel_elased;

        self.build_state_update_elapsed = finalize_metrics.build_state_update_elapsed;
        self.build_block_elapsed = finalize_metrics.build_block_elapsed;

        self.finalize_block_elapsed = finalize_metrics.finalize_block_elapsed;

        self.total_elapsed = finalize_metrics.total_timer.total_elapsed;
    }

    pub fn build_accounts_wu(&self) -> u64 {
        self.build_shard_accounts_wu + self.build_accounts_blocks_wu
    }

    pub fn build_accounts_wu_price(&self) -> f64 {
        let build_accounts_wu = self.build_accounts_wu();
        if build_accounts_wu == 0 {
            return 0.0;
        }
        self.build_accounts_elapsed.as_nanos() as f64 / build_accounts_wu as f64
    }

    pub fn build_in_msgs_wu_price(&self) -> f64 {
        if self.build_in_msgs_wu == 0 {
            return 0.0;
        }
        self.build_in_msgs_elapsed.as_nanos() as f64 / self.build_in_msgs_wu as f64
    }

    pub fn build_out_msgs_wu_price(&self) -> f64 {
        if self.build_out_msgs_wu == 0 {
            return 0.0;
        }
        self.build_out_msgs_elapsed.as_nanos() as f64 / self.build_out_msgs_wu as f64
    }

    pub fn max_accounts_in_out_msgs_wu(&self) -> u64 {
        self.build_accounts_wu()
            .max(self.build_in_msgs_wu)
            .max(self.build_out_msgs_wu)
    }

    pub fn build_accounts_and_messages_in_parallel_wu_price(&self) -> f64 {
        let max_wu = self.max_accounts_in_out_msgs_wu();
        if max_wu == 0 {
            return 0.0;
        }
        self.build_accounts_and_messages_in_parallel_elased
            .as_nanos() as f64
            / max_wu as f64
    }

    pub fn build_state_update_wu_price(&self) -> f64 {
        if self.build_state_update_wu == 0 {
            return 0.0;
        }
        self.build_state_update_elapsed.as_nanos() as f64 / self.build_state_update_wu as f64
    }

    pub fn build_block_wu_price(&self) -> f64 {
        if self.build_block_wu == 0 {
            return 0.0;
        }
        self.build_block_elapsed.as_nanos() as f64 / self.build_block_wu as f64
    }

    pub fn finalize_block_wu(&self) -> u64 {
        self.max_accounts_in_out_msgs_wu()
            .saturating_add(self.build_state_update_wu)
            .saturating_add(self.build_block_wu)
    }

    pub fn finalize_block_wu_price(&self) -> f64 {
        let finalize_block_wu = self.finalize_block_wu();
        if finalize_block_wu == 0 {
            return 0.0;
        }
        self.finalize_block_elapsed.as_nanos() as f64 / finalize_block_wu as f64
    }

    pub fn create_queue_diff_wu_price(&self) -> f64 {
        if self.create_queue_diff_wu == 0 {
            return 0.0;
        }
        self.create_queue_diff_elapsed.as_nanos() as f64 / self.create_queue_diff_wu as f64
    }

    pub fn apply_queue_diff_wu_price(&self) -> f64 {
        if self.apply_queue_diff_wu == 0 {
            return 0.0;
        }
        self.apply_queue_diff_elapsed.as_nanos() as f64 / self.apply_queue_diff_wu as f64
    }

    pub fn total_wu(&self) -> u64 {
        // take max from finalize block and apply diff
        // and sum with create diff
        self.finalize_block_wu()
            .max(self.apply_queue_diff_wu)
            .saturating_add(self.create_queue_diff_wu)
    }

    pub fn total_wu_price(&self) -> f64 {
        let total_wu = self.total_wu();
        if total_wu == 0 {
            return 0.0;
        }
        self.total_elapsed.as_nanos() as f64 / total_wu as f64
    }
}
