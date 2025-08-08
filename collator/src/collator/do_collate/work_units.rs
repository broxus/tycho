use std::time::Duration;

use num_bigint::BigUint;
use tycho_types::models::{
    ShardIdent, WorkUnitsParams, WorkUnitsParamsExecute, WorkUnitsParamsFinalize,
    WorkUnitsParamsPrepare,
};
use tycho_util::num::SafeUnsignedAvg;

use crate::collator::messages_reader::MessagesReaderMetrics;
use crate::collator::types::{ExecuteMetrics, FinalizeMetrics};
use crate::tracing_targets;
use crate::types::McData;
use crate::types::processed_upto::BlockSeqno;

#[derive(Default)]
pub struct PrepareMsgGroupsWu {
    pub total_elapsed: Duration,

    pub fixed_part: u64,

    pub read_ext_msgs_count: u64,
    pub read_ext_msgs_wu: u64,
    pub read_ext_msgs_elapsed: Duration,

    pub read_existing_int_msgs_count: u64,
    pub read_existing_int_msgs_wu: u64,
    pub read_existing_int_msgs_elapsed: Duration,

    pub read_new_int_msgs_count: u64,
    pub read_new_int_msgs_wu: u64,
    pub read_new_int_msgs_elapsed: Duration,

    pub add_to_msgs_groups_ops_count: u64,
    pub add_msgs_to_groups_wu: u64,
    pub add_msgs_to_groups_elapsed: Duration,
}

impl PrepareMsgGroupsWu {
    pub(super) fn calculate(
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

        let &MessagesReaderMetrics {
            read_ext_msgs_count,
            read_existing_msgs_count: read_existing_int_msgs_count,
            read_new_msgs_count: read_new_int_msgs_count,
            add_to_msgs_groups_ops_count,
            ..
        } = msgs_reader_metrics;

        let read_ext_msgs_wu = read_ext_msgs_count.saturating_mul(read_ext_msgs as u64);
        let read_existing_int_msgs_wu =
            read_existing_int_msgs_count.saturating_mul(read_int_msgs as u64);
        let read_new_int_msgs_wu = read_new_int_msgs_count.saturating_mul(read_new_msgs as u64);

        let add_msgs_to_groups_wu =
            add_to_msgs_groups_ops_count.saturating_mul(add_to_msg_groups as u64);

        Self {
            total_elapsed: prepare_msg_groups_total_elapsed,

            fixed_part: fixed_part as u64,

            read_ext_msgs_count,
            read_ext_msgs_wu,
            read_ext_msgs_elapsed: msgs_reader_metrics.read_ext_messages_timer.total_elapsed,

            read_existing_int_msgs_count,
            read_existing_int_msgs_wu,
            read_existing_int_msgs_elapsed: msgs_reader_metrics
                .read_existing_messages_timer
                .total_elapsed,

            read_new_int_msgs_count,
            read_new_int_msgs_wu,
            read_new_int_msgs_elapsed: msgs_reader_metrics.read_new_messages_timer.total_elapsed,

            add_to_msgs_groups_ops_count,
            add_msgs_to_groups_wu,
            add_msgs_to_groups_elapsed: msgs_reader_metrics
                .add_to_message_groups_timer
                .total_elapsed,
        }
    }

    pub fn log_wu_metrics(&self, wu_params_prepare: &WorkUnitsParamsPrepare) {
        let &WorkUnitsParamsPrepare {
            read_ext_msgs,
            read_int_msgs,
            read_new_msgs,
            add_to_msg_groups,
            ..
        } = wu_params_prepare;

        tracing::debug!(target: tracing_targets::COLLATOR,
            "read_msg_groups_wu: total: (wu={}, elapsed={}, price={}), \
            read_ext_msgs: (count={}, param={}, wu={}, elapsed={}, price={}), \
            read_existing_int_msgs: (count={}, param={}, wu={}, elapsed={}, price={}), \
            read_new_int_msgs: (count={}, param={}, wu={}, elapsed={}, price={}), \
            add_to_msgs_groups_ops: (count={}, param={}, wu={}, elapsed={}, price={})",
            self.total_wu(), self.total_elapsed.as_nanos(), self.total_wu_price(),
            self.read_ext_msgs_count, read_ext_msgs, self.read_ext_msgs_wu,
            self.read_ext_msgs_elapsed.as_nanos(), self.read_ext_msgs_wu_price(),
            self.read_existing_int_msgs_count, read_int_msgs, self.read_existing_int_msgs_wu,
            self.read_existing_int_msgs_elapsed.as_nanos(), self.read_existing_int_msgs_wu_price(),
            self.read_new_int_msgs_count, read_new_msgs, self.read_new_int_msgs_wu,
            self.read_new_int_msgs_elapsed.as_nanos(), self.read_new_int_msgs_wu_price(),
            self.add_to_msgs_groups_ops_count, add_to_msg_groups, self.add_msgs_to_groups_wu,
            self.add_msgs_to_groups_elapsed.as_nanos(), self.add_msgs_to_groups_wu_price(),
        );
    }

    pub fn total_wu(&self) -> u64 {
        self.fixed_part
            .saturating_add(self.read_ext_msgs_wu)
            .saturating_add(self.read_existing_int_msgs_wu)
            .saturating_add(self.read_new_int_msgs_wu)
            .saturating_add(self.add_msgs_to_groups_wu)
    }

    pub fn total_wu_price(&self) -> f64 {
        let total_wu = self.total_wu();
        if total_wu == 0 {
            return 0.0;
        }
        self.total_elapsed.as_nanos() as f64 / total_wu as f64
    }

    pub fn calc_target_read_ext_msgs_wu_param(&self, target_wu: u64) -> Option<u64> {
        let base = self.read_ext_msgs_count;
        if base == 0 {
            return None;
        }
        Some(target_wu.saturating_div(base))
    }

    pub fn calc_target_read_ext_msgs_wu(
        &self,
        target_build_in_msgs_wu: u64,
        build_in_msgs_elapsed_ns: u128,
    ) -> u64 {
        FinalizeWu::calc_target_wu_from_build_in_msgs_wu_and_elapsed(
            self.read_ext_msgs_elapsed.as_nanos(),
            target_build_in_msgs_wu,
            build_in_msgs_elapsed_ns,
        )
    }

    pub fn calc_target_read_ext_msgs_wu_by_price(&self, target_wu_price: f64) -> u64 {
        let target_wu = self.read_ext_msgs_elapsed.as_nanos() as f64 / target_wu_price;
        target_wu.saturating_to_u64()
    }

    pub fn read_ext_msgs_wu_price(&self) -> f64 {
        if self.read_ext_msgs_wu == 0 {
            return 0.0;
        }
        self.read_ext_msgs_elapsed.as_nanos() as f64 / self.read_ext_msgs_wu as f64
    }

    pub fn calc_target_read_existing_int_msgs_wu_param(&self, target_wu: u64) -> Option<u64> {
        let base = self.read_existing_int_msgs_count;
        if base == 0 {
            return None;
        }
        Some(target_wu.saturating_div(base))
    }

    pub fn calc_target_read_existing_int_msgs_wu(
        &self,
        target_build_in_msgs_wu: u64,
        build_in_msgs_elapsed_ns: u128,
    ) -> u64 {
        FinalizeWu::calc_target_wu_from_build_in_msgs_wu_and_elapsed(
            self.read_existing_int_msgs_elapsed.as_nanos(),
            target_build_in_msgs_wu,
            build_in_msgs_elapsed_ns,
        )
    }

    pub fn calc_target_read_existing_int_msgs_wu_by_price(&self, target_wu_price: f64) -> u64 {
        let target_wu = self.read_existing_int_msgs_elapsed.as_nanos() as f64 / target_wu_price;
        target_wu.saturating_to_u64()
    }

    pub fn read_existing_int_msgs_wu_price(&self) -> f64 {
        if self.read_existing_int_msgs_wu == 0 {
            return 0.0;
        }
        self.read_existing_int_msgs_elapsed.as_nanos() as f64
            / self.read_existing_int_msgs_wu as f64
    }

    pub fn calc_target_read_new_int_msgs_wu_param(&self, target_wu: u64) -> Option<u64> {
        let base = self.read_new_int_msgs_count;
        if base == 0 {
            return None;
        }
        Some(target_wu.saturating_div(base))
    }

    pub fn calc_target_read_new_int_msgs_wu(
        &self,
        target_build_in_msgs_wu: u64,
        build_in_msgs_elapsed_ns: u128,
    ) -> u64 {
        FinalizeWu::calc_target_wu_from_build_in_msgs_wu_and_elapsed(
            self.read_new_int_msgs_elapsed.as_nanos(),
            target_build_in_msgs_wu,
            build_in_msgs_elapsed_ns,
        )
    }

    pub fn calc_target_read_new_int_msgs_wu_by_price(&self, target_wu_price: f64) -> u64 {
        let target_wu = self.read_new_int_msgs_elapsed.as_nanos() as f64 / target_wu_price;
        target_wu.saturating_to_u64()
    }

    pub fn read_new_int_msgs_wu_price(&self) -> f64 {
        if self.read_new_int_msgs_wu == 0 {
            return 0.0;
        }
        self.read_new_int_msgs_elapsed.as_nanos() as f64 / self.read_new_int_msgs_wu as f64
    }

    pub fn calc_target_add_msgs_to_groups_wu_param(&self, target_wu: u64) -> Option<u64> {
        let base = self.add_to_msgs_groups_ops_count;
        if base == 0 {
            return None;
        }
        Some(target_wu.saturating_div(base))
    }

    pub fn calc_target_add_msgs_to_groups_wu(
        &self,
        target_build_in_msgs_wu: u64,
        build_in_msgs_elapsed_ns: u128,
    ) -> u64 {
        FinalizeWu::calc_target_wu_from_build_in_msgs_wu_and_elapsed(
            self.add_msgs_to_groups_elapsed.as_nanos(),
            target_build_in_msgs_wu,
            build_in_msgs_elapsed_ns,
        )
    }

    pub fn calc_target_add_msgs_to_groups_wu_by_price(&self, target_wu_price: f64) -> u64 {
        let target_wu = self.add_msgs_to_groups_elapsed.as_nanos() as f64 / target_wu_price;
        target_wu.saturating_to_u64()
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
    pub in_msgs_len: u64,
    pub out_msgs_len: u64,
    pub inserted_new_msgs_count: u64,

    pub groups_count: u64,

    pub avg_group_accounts_count: SafeUnsignedAvg,
    pub avg_threads_count: SafeUnsignedAvg,

    pub sum_gas: u128,

    pub execute_groups_vm_only_wu: u64,
    pub execute_groups_vm_only_elapsed: Duration,

    pub process_txs_wu: u64,
    pub process_txs_elapsed: Duration,
}
impl ExecuteWu {
    pub fn append_group_exec_wu(
        &mut self,
        wu_params_execute: &WorkUnitsParamsExecute,
        group_accounts_count: u64,
        group_gas: u128,
    ) -> u128 {
        let &WorkUnitsParamsExecute {
            prepare,
            execute,
            execute_delimiter,
            subgroup_size: max_threads_count,
            ..
        } = wu_params_execute;

        self.groups_count += 1;

        self.avg_group_accounts_count.accum(group_accounts_count);

        let threads_count = calc_threads_count(max_threads_count as u64, group_accounts_count);
        self.avg_threads_count.accum(threads_count);

        self.sum_gas = self.sum_gas.saturating_add(group_gas);

        // calculate total group execute wu by sum gas and accounts count
        let group_exec_wu = (group_accounts_count as u128)
            .saturating_mul(prepare as u128)
            .saturating_add(
                group_gas
                    .saturating_mul(execute as u128)
                    .saturating_div(execute_delimiter as u128),
            )
            .saturating_div(threads_count as u128);

        self.execute_groups_vm_only_wu = self
            .execute_groups_vm_only_wu
            .saturating_add(group_exec_wu as u64);

        group_exec_wu
    }

    pub fn calculate_process_txs_wu(
        &mut self,
        wu_params_execute: &WorkUnitsParamsExecute,
        in_msgs_len: u64,
        out_msgs_len: u64,
        inserted_new_msgs_count: u64,
    ) {
        let &WorkUnitsParamsExecute {
            serialize_enqueue: insert_in_msgs,
            serialize_dequeue: insert_out_msgs,
            insert_new_msgs,
            ..
        } = wu_params_execute;

        self.in_msgs_len = in_msgs_len;
        let in_msgs_len_log = self.in_msgs_len_log();
        self.out_msgs_len = out_msgs_len;
        let out_msgs_len_log = self.out_msgs_len_log();
        self.inserted_new_msgs_count = inserted_new_msgs_count;
        let inserted_new_msgs_count_log = self.inserted_new_msgs_count_log();

        self.process_txs_wu = in_msgs_len
            .saturating_mul(in_msgs_len_log)
            .saturating_mul(insert_in_msgs as u64)
            .saturating_add(
                out_msgs_len
                    .saturating_mul(out_msgs_len_log)
                    .saturating_mul(insert_out_msgs as u64),
            )
            .saturating_add(
                inserted_new_msgs_count
                    .saturating_mul(inserted_new_msgs_count_log)
                    .saturating_mul(insert_new_msgs as u64),
            );
    }

    pub(super) fn append_elapsed_timings(&mut self, execute_metrics: &ExecuteMetrics) {
        self.execute_groups_vm_only_elapsed =
            execute_metrics.execute_groups_vm_only_timer.total_elapsed;
        self.process_txs_elapsed = execute_metrics.process_txs_timer.total_elapsed;
    }

    pub fn in_msgs_len_log(&self) -> u64 {
        self.in_msgs_len.checked_ilog2().unwrap_or_default() as u64
    }

    pub fn out_msgs_len_log(&self) -> u64 {
        self.out_msgs_len.checked_ilog2().unwrap_or_default() as u64
    }

    pub fn inserted_new_msgs_count_log(&self) -> u64 {
        self.inserted_new_msgs_count
            .checked_ilog2()
            .unwrap_or_default() as u64
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

    pub fn calc_target_execute_wu_param(
        &self,
        target_wu: u64,
        prepare_wu_param: u64,
        execute_delimiter: u64,
    ) -> Option<u64> {
        // WU = SUM[i; 1..=N; (P * Ai + Gi * C / D) / Ti]
        // WU ~ (N * P * avg(A) + sum(G) * C / D) / avg(T)
        // sum(G) * C / D ~ WU * avg(T) - N * P * avg(A)
        // C ~ (WU * avg(T) - N * P * avg(A)) * D / sum(G)

        let avg_threads_count = self.avg_threads_count.get_avg_checked();
        let avg_group_accounts_count = self.avg_group_accounts_count.get_avg_checked();
        if self.groups_count == 0
            || avg_threads_count.is_none()
            || avg_group_accounts_count.is_none()
            || self.sum_gas == 0
        {
            return None;
        }

        let avg_threads_count = avg_threads_count.unwrap();
        let avg_group_accounts_count = avg_group_accounts_count.unwrap();

        let target_wu_param = (target_wu as u128)
            .saturating_mul(avg_threads_count)
            .saturating_sub(
                (self.groups_count as u128)
                    .saturating_mul(prepare_wu_param as u128)
                    .saturating_mul(avg_group_accounts_count),
            )
            .saturating_mul(execute_delimiter as u128)
            .saturating_div(self.sum_gas);

        Some(target_wu_param as u64)
    }

    pub fn calc_target_execute_groups_vm_only_wu(
        &self,
        target_build_in_msgs_wu: u64,
        build_in_msgs_elapsed_ns: u128,
    ) -> u64 {
        FinalizeWu::calc_target_wu_from_build_in_msgs_wu_and_elapsed(
            self.execute_groups_vm_only_elapsed.as_nanos(),
            target_build_in_msgs_wu,
            build_in_msgs_elapsed_ns,
        )
    }

    pub fn calc_target_execute_groups_vm_only_wu_by_price(&self, target_wu_price: f64) -> u64 {
        let target_wu = self.execute_groups_vm_only_elapsed.as_nanos() as f64 / target_wu_price;
        target_wu.saturating_to_u64()
    }

    pub fn execute_groups_vm_only_wu_price(&self) -> f64 {
        if self.execute_groups_vm_only_wu == 0 {
            return 0.0;
        }
        self.execute_groups_vm_only_elapsed.as_nanos() as f64
            / self.execute_groups_vm_only_wu as f64
    }

    pub fn calc_target_process_txs_wu_param(&self, target_wu: u64) -> Option<u64> {
        let base = self
            .in_msgs_len
            .saturating_mul(self.in_msgs_len_log())
            .saturating_add(self.out_msgs_len.saturating_mul(self.out_msgs_len_log()))
            .saturating_add(
                self.inserted_new_msgs_count
                    .saturating_mul(self.inserted_new_msgs_count_log()),
            );
        if base == 0 {
            return None;
        }
        Some(target_wu.saturating_div(base))
    }

    pub fn calc_target_process_txs_wu(
        &self,
        target_build_in_msgs_wu: u64,
        build_in_msgs_elapsed_ns: u128,
    ) -> u64 {
        FinalizeWu::calc_target_wu_from_build_in_msgs_wu_and_elapsed(
            self.process_txs_elapsed.as_nanos(),
            target_build_in_msgs_wu,
            build_in_msgs_elapsed_ns,
        )
    }

    pub fn calc_target_process_txs_wu_by_price(&self, target_wu_price: f64) -> u64 {
        let target_wu = self.process_txs_elapsed.as_nanos() as f64 / target_wu_price;
        target_wu.saturating_to_u64()
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
    pub diff_msgs_count: u64,

    pub create_queue_diff_wu: u64,
    pub create_queue_diff_elapsed: Duration,

    pub apply_queue_diff_wu: u64,
    pub apply_queue_diff_elapsed: Duration,

    pub shard_accounts_count: u64,
    pub updated_accounts_count: u64,
    pub in_msgs_len: u64,
    pub out_msgs_len: u64,

    pub update_shard_accounts_wu: u64,
    pub update_shard_accounts_elapsed: Duration,

    pub build_accounts_blocks_wu: u64,
    pub build_accounts_blocks_elapsed: Duration,

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

        self.diff_msgs_count = diff_msgs_count;

        self.create_queue_diff_wu = diff_msgs_count.saturating_mul(create_diff as u64);
        self.apply_queue_diff_wu = diff_msgs_count.saturating_mul(apply_diff as u64);
    }

    pub fn calculate_finalize_block_wu(
        &mut self,
        wu_params_finalize: &WorkUnitsParamsFinalize,
        wu_params_execute: &WorkUnitsParamsExecute,
        shard_accounts_count: u64,
        updated_accounts_count: u64,
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
            state_update_msg: state_pow_coeff,
            ..
        } = wu_params_finalize;

        let &WorkUnitsParamsExecute {
            subgroup_size: max_threads_count,
            ..
        } = wu_params_execute;

        let threads_count = calc_threads_count(max_threads_count as u64, updated_accounts_count);

        self.in_msgs_len = in_msgs_len;
        self.out_msgs_len = out_msgs_len;
        self.updated_accounts_count = updated_accounts_count;
        self.shard_accounts_count = shard_accounts_count;

        let shard_accounts_count_log = self.shard_accounts_count_log();

        let scale = 10;
        let pow_shard_accounts_count = self.pow_shard_accounts_count(state_pow_coeff, scale);

        // calc update shard accounts
        self.calc_update_shard_accounts_wu(
            threads_count,
            shard_accounts_count_log,
            pow_shard_accounts_count,
            scale,
            build_accounts as u64,
        );

        // calc build account blocks
        self.calc_build_accounts_blocks_wu(threads_count, build_transactions as u64);

        // calc build in msgs
        self.calc_build_in_msgs_wu(build_in_msg as u64);

        // calc build out msgs
        self.calc_build_out_msgs_wu(build_out_msg as u64);

        // calc build state update
        self.calc_build_state_update_wu(
            threads_count,
            shard_accounts_count_log,
            pow_shard_accounts_count,
            scale,
            state_update_min as u64,
            state_update_accounts as u64,
        );

        // calc build block
        self.calc_build_block_wu(
            serialize_min as u64,
            serialize_accounts as u64,
            serialize_msg as u64,
        );
    }

    pub(super) fn append_elapsed_timings(&mut self, finalize_metrics: &FinalizeMetrics) {
        self.create_queue_diff_elapsed = finalize_metrics.create_queue_diff_elapsed;
        self.apply_queue_diff_elapsed = finalize_metrics.apply_queue_diff_elapsed;

        self.update_shard_accounts_elapsed = finalize_metrics.update_shard_accounts_elapsed;
        self.build_accounts_blocks_elapsed = finalize_metrics.build_accounts_blocks_elapsed;

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

    pub fn shard_accounts_count_log(&self) -> u64 {
        self.shard_accounts_count
            .checked_ilog2()
            .unwrap_or_default() as u64
    }

    pub fn pow_shard_accounts_count(&self, state_pow_coeff: u16, scale: u128) -> u128 {
        let (numerator, denominator) = unpack_from_u16(state_pow_coeff);
        compute_scaled_pow(
            self.shard_accounts_count as u128,
            numerator as u32,
            denominator as u32,
            scale,
        )
    }

    pub fn updated_accounts_count_log(&self) -> u64 {
        self.updated_accounts_count
            .checked_ilog2()
            .unwrap_or_default() as u64
    }

    fn calc_update_shard_accounts_wu(
        &mut self,
        threads_count: u64,
        shard_accounts_count_log: u64,
        pow_shard_accounts_count: u128,
        scale: u128,
        update_shard_accounts_wu_param: u64,
    ) {
        self.update_shard_accounts_wu = self
            .calc_update_shard_accounts_wu_base(
                threads_count,
                shard_accounts_count_log,
                pow_shard_accounts_count,
                scale,
            )
            .saturating_mul(update_shard_accounts_wu_param as u128)
            .saturating_div(scale) as u64;
    }

    fn calc_update_shard_accounts_wu_base(
        &self,
        threads_count: u64,
        shard_accounts_count_log: u64,
        pow_shard_accounts_count: u128,
        scale: u128,
    ) -> u128 {
        //  * prepare account modifications in (updated_accounts_count)
        //  * then update map of shard accounts in parallel in (updated_accounts_count)*log(shard_accounts_count)/(threads_count)
        //  * additionally multiply on (shard_accounts_count^a) for better precision on a large state
        (self.updated_accounts_count as u128)
            .saturating_mul(scale)
            .saturating_add(
                (self.updated_accounts_count as u128)
                    .saturating_mul(shard_accounts_count_log as u128)
                    .saturating_div(threads_count as u128)
                    .saturating_mul(pow_shard_accounts_count),
            )
    }

    pub fn calc_target_update_shard_accounts_wu_param(
        &self,
        target_wu: u64,
        threads_count: u64,
        shard_accounts_count_log: u64,
        pow_shard_accounts_count: u128,
        scale: u128,
    ) -> Option<u64> {
        // A * C + A * L / T * P * C / S = (A * S + A * L / T * P) * C / S = WU
        // C = WU * S / (A * S + A * L / T * P)
        let base = self.calc_update_shard_accounts_wu_base(
            threads_count,
            shard_accounts_count_log,
            pow_shard_accounts_count,
            scale,
        );
        if base == 0 {
            return None;
        }
        Some(
            (target_wu as u128)
                .saturating_mul(scale)
                .saturating_div(base) as u64,
        )
    }

    pub fn calc_target_update_shard_accounts_wu(&self, target_build_in_msgs_wu: u64) -> u64 {
        self.calc_target_wu_from_build_in_msgs_wu(
            self.update_shard_accounts_elapsed.as_nanos(),
            target_build_in_msgs_wu,
        )
    }

    pub fn calc_target_update_shard_accounts_wu_by_price(&self, target_wu_price: f64) -> u64 {
        let target_wu = self.update_shard_accounts_elapsed.as_nanos() as f64 / target_wu_price;
        target_wu.saturating_to_u64()
    }

    pub fn update_shard_accounts_wu_price(&self) -> f64 {
        if self.update_shard_accounts_wu == 0 {
            return 0.0;
        }
        self.update_shard_accounts_elapsed.as_nanos() as f64 / self.update_shard_accounts_wu as f64
    }

    fn calc_build_accounts_blocks_wu(
        &mut self,
        threads_count: u64,
        build_accounts_blocks_wu_param: u64,
    ) {
        let updated_accounts_count_log = self.updated_accounts_count_log();
        self.build_accounts_blocks_wu = self
            .calc_build_accounts_blocks_wu_base(threads_count, updated_accounts_count_log)
            .saturating_mul(build_accounts_blocks_wu_param);
    }

    fn calc_build_accounts_blocks_wu_base(
        &self,
        threads_count: u64,
        updated_accounts_count_log: u64,
    ) -> u64 {
        //  * build maps of transactions by accounts in parallel in (txs_count)*log(txs_count/threads_count)/(threads_count)
        //  * then build AugDict of accounts blocks in (updated_accounts_count)*log(updated_accounts_count)
        let in_msgs_len_div_threads_count_log =
            self.in_msgs_len_div_threads_count_log(threads_count);
        self.in_msgs_len
            .saturating_mul(in_msgs_len_div_threads_count_log)
            .saturating_div(threads_count)
            .saturating_add(
                self.updated_accounts_count
                    .saturating_mul(updated_accounts_count_log),
            )
    }

    pub fn calc_target_build_accounts_blocks_wu_param(
        &self,
        target_wu: u64,
        threads_count: u64,
    ) -> Option<u64> {
        let updated_accounts_count_log = self.updated_accounts_count_log();
        let base =
            self.calc_build_accounts_blocks_wu_base(threads_count, updated_accounts_count_log);
        if base == 0 {
            return None;
        }
        Some(target_wu.saturating_div(base))
    }

    pub fn calc_target_build_accounts_blocks_wu(&self, target_build_in_msgs_wu: u64) -> u64 {
        self.calc_target_wu_from_build_in_msgs_wu(
            self.build_accounts_blocks_elapsed.as_nanos(),
            target_build_in_msgs_wu,
        )
    }

    pub fn calc_target_build_accounts_blocks_wu_by_price(&self, target_wu_price: f64) -> u64 {
        let target_wu = self.build_accounts_blocks_elapsed.as_nanos() as f64 / target_wu_price;
        target_wu.saturating_to_u64()
    }

    pub fn build_accounts_blocks_wu_price(&self) -> f64 {
        if self.build_accounts_blocks_wu == 0 {
            return 0.0;
        }
        self.build_accounts_blocks_elapsed.as_nanos() as f64 / self.build_accounts_blocks_wu as f64
    }

    pub fn build_accounts_wu(&self) -> u64 {
        self.update_shard_accounts_wu + self.build_accounts_blocks_wu
    }

    pub fn build_accounts_wu_price(&self) -> f64 {
        let build_accounts_wu = self.build_accounts_wu();
        if build_accounts_wu == 0 {
            return 0.0;
        }
        self.build_accounts_elapsed.as_nanos() as f64 / build_accounts_wu as f64
    }

    pub fn in_msgs_len_log(&self) -> u64 {
        self.in_msgs_len.checked_ilog2().unwrap_or_default() as u64
    }

    pub fn in_msgs_len_div_threads_count_log(&self, threads_count: u64) -> u64 {
        self.in_msgs_len
            .saturating_div(threads_count)
            .checked_ilog2()
            .unwrap_or_default() as u64
    }

    fn calc_build_in_msgs_wu(&mut self, build_in_msg_wu_param: u64) {
        let in_msgs_len_log = self.in_msgs_len_log();
        self.build_in_msgs_wu = self
            .calc_build_in_msgs_wu_base(in_msgs_len_log)
            .saturating_mul(build_in_msg_wu_param);
    }

    fn calc_build_in_msgs_wu_base(&self, in_msgs_len_log: u64) -> u64 {
        //  * build AugDict of in msgs in (in_msgs_len)*log(in_msgs_len)
        self.in_msgs_len.saturating_mul(in_msgs_len_log)
    }

    pub fn calc_target_build_in_msg_wu_param(&self, target_wu: u64) -> Option<u64> {
        let in_msgs_len_log = self.in_msgs_len_log();
        let base = self.calc_build_in_msgs_wu_base(in_msgs_len_log);
        if base == 0 {
            return None;
        }
        Some(target_wu.saturating_div(base))
    }

    pub fn calc_target_build_in_msgs_wu_by_price(&self, target_wu_price: f64) -> u64 {
        let target_wu = self.build_in_msgs_elapsed.as_nanos() as f64 / target_wu_price;
        target_wu.saturating_to_u64()
    }

    pub fn build_in_msgs_wu_price(&self) -> f64 {
        if self.build_in_msgs_wu == 0 {
            return 0.0;
        }
        self.build_in_msgs_elapsed.as_nanos() as f64 / self.build_in_msgs_wu as f64
    }

    pub fn out_msgs_len_log(&self) -> u64 {
        self.out_msgs_len.checked_ilog2().unwrap_or_default() as u64
    }

    fn calc_build_out_msgs_wu(&mut self, build_out_msg_wu_param: u64) {
        let out_msgs_len_log = self.out_msgs_len_log();
        self.build_out_msgs_wu = self
            .calc_build_out_msgs_wu_base(out_msgs_len_log)
            .saturating_mul(build_out_msg_wu_param);
    }

    fn calc_build_out_msgs_wu_base(&self, out_msgs_len_log: u64) -> u64 {
        //  * build AugDict of out msgs in (out_msgs_len)*log(out_msgs_len)
        self.out_msgs_len.saturating_mul(out_msgs_len_log)
    }

    pub fn calc_target_build_out_msg_wu_param(&self, target_wu: u64) -> Option<u64> {
        let out_msgs_len_log = self.out_msgs_len_log();
        let base = self.calc_build_out_msgs_wu_base(out_msgs_len_log);
        if base == 0 {
            return None;
        }
        Some(target_wu.saturating_div(base))
    }

    pub fn calc_target_build_out_msgs_wu(&self, target_build_in_msgs_wu: u64) -> u64 {
        self.calc_target_wu_from_build_in_msgs_wu(
            self.build_out_msgs_elapsed.as_nanos(),
            target_build_in_msgs_wu,
        )
    }

    pub fn calc_target_build_out_msgs_wu_by_price(&self, target_wu_price: f64) -> u64 {
        let target_wu = self.build_out_msgs_elapsed.as_nanos() as f64 / target_wu_price;
        target_wu.saturating_to_u64()
    }

    fn calc_target_wu_from_build_in_msgs_wu(
        &self,
        target_elapsed_ns: u128,
        target_build_in_msgs_wu: u64,
    ) -> u64 {
        Self::calc_target_wu_from_build_in_msgs_wu_and_elapsed(
            target_elapsed_ns,
            target_build_in_msgs_wu,
            self.build_in_msgs_elapsed.as_nanos(),
        )
    }

    pub fn calc_target_wu_from_build_in_msgs_wu_and_elapsed(
        target_elapsed_ns: u128,
        target_build_in_msgs_wu: u64,
        build_in_msgs_elapsed_ns: u128,
    ) -> u64 {
        let target_wu = target_elapsed_ns as f64 / build_in_msgs_elapsed_ns as f64
            * target_build_in_msgs_wu as f64;
        target_wu.saturating_to_u64()
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

    fn calc_build_state_update_wu(
        &mut self,
        threads_count: u64,
        shard_accounts_count_log: u64,
        pow_shard_accounts_count: u128,
        scale: u128,
        build_state_update_wu_min: u64,
        build_state_update_wu_param: u64,
    ) {
        //  * use min value if calculated is less
        self.build_state_update_wu = std::cmp::max(
            build_state_update_wu_min,
            self.calc_build_state_update_wu_base(
                threads_count,
                shard_accounts_count_log,
                pow_shard_accounts_count,
            )
            .saturating_mul(build_state_update_wu_param as u128)
            .saturating_div(scale) as u64,
        );
    }

    fn calc_build_state_update_wu_base(
        &self,
        threads_count: u64,
        shard_accounts_count_log: u64,
        pow_shard_accounts_count: u128,
    ) -> u128 {
        //  * calc update of all updated accounts in parallel in (updated_accounts_count)*log(shard_accounts_count)/(threads_count)
        //  * additionally multiply on (shard_accounts_count^0.1504) for better precision on a large state
        (self.updated_accounts_count as u128)
            .saturating_mul(shard_accounts_count_log as u128)
            .saturating_div(threads_count as u128)
            .saturating_mul(pow_shard_accounts_count)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn calc_target_build_state_update_wu_param(
        &self,
        target_wu: u64,
        threads_count: u64,
        shard_accounts_count_log: u64,
        pow_shard_accounts_count: u128,
        scale: u128,
        build_state_update_wu_min: u64,
        curr_build_state_update_wu_param: u64,
    ) -> Option<u64> {
        if target_wu < build_state_update_wu_min {
            Some(curr_build_state_update_wu_param)
        } else {
            let base = self.calc_build_state_update_wu_base(
                threads_count,
                shard_accounts_count_log,
                pow_shard_accounts_count,
            );
            if base == 0 {
                return None;
            }
            Some(
                (target_wu as u128)
                    .saturating_mul(scale)
                    .saturating_div(base) as u64,
            )
        }
    }

    pub fn calc_target_build_state_update_wu(&self, target_build_in_msgs_wu: u64) -> u64 {
        self.calc_target_wu_from_build_in_msgs_wu(
            self.build_state_update_elapsed.as_nanos(),
            target_build_in_msgs_wu,
        )
    }

    pub fn calc_target_build_state_update_wu_by_price(&self, target_wu_price: f64) -> u64 {
        let target_wu = self.build_state_update_elapsed.as_nanos() as f64 / target_wu_price;
        target_wu.saturating_to_u64()
    }

    pub fn build_state_update_wu_price(&self) -> f64 {
        if self.build_state_update_wu == 0 {
            return 0.0;
        }
        self.build_state_update_elapsed.as_nanos() as f64 / self.build_state_update_wu as f64
    }

    fn calc_build_block_wu(
        &mut self,
        serialize_min_wu: u64,
        serialize_account_wu_param: u64,
        serialize_msg_wu_param: u64,
    ) {
        //  * serialize each account block in (updated_accounts_count)
        //  * serialize in msgs and out msgs in (in_msgs_len + out_msgs_len)
        //  * use min value if calculated is less
        self.build_block_wu = std::cmp::max(
            serialize_min_wu,
            self.updated_accounts_count
                .saturating_mul(serialize_account_wu_param)
                .saturating_add(
                    self.in_msgs_len
                        .saturating_add(self.out_msgs_len)
                        .saturating_mul(serialize_msg_wu_param),
                ),
        );
    }

    pub fn calc_target_build_block_wu_param(
        &self,
        target_wu: u64,
        serialize_min_wu: u64,
        curr_serialize_wu_param: u64,
    ) -> Option<u64> {
        if target_wu < serialize_min_wu {
            Some(curr_serialize_wu_param)
        } else {
            let base = self
                .updated_accounts_count
                .saturating_add(self.in_msgs_len)
                .saturating_add(self.out_msgs_len);
            if base == 0 {
                return None;
            }
            Some(target_wu.saturating_div(base))
        }
    }

    pub fn calc_target_build_block_wu(&self, target_build_in_msgs_wu: u64) -> u64 {
        self.calc_target_wu_from_build_in_msgs_wu(
            self.build_block_elapsed.as_nanos(),
            target_build_in_msgs_wu,
        )
    }

    pub fn calc_target_build_block_wu_by_price(&self, target_wu_price: f64) -> u64 {
        let target_wu = self.build_block_elapsed.as_nanos() as f64 / target_wu_price;
        target_wu.saturating_to_u64()
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

    pub fn calc_target_create_queue_diff_wu_param(&self, target_wu: u64) -> Option<u64> {
        let base = self.diff_msgs_count;
        if base == 0 {
            return None;
        }
        Some(target_wu.saturating_div(base))
    }

    pub fn calc_target_create_queue_diff_wu(&self, target_build_in_msgs_wu: u64) -> u64 {
        self.calc_target_wu_from_build_in_msgs_wu(
            self.create_queue_diff_elapsed.as_nanos(),
            target_build_in_msgs_wu,
        )
    }

    pub fn calc_target_create_queue_diff_wu_by_price(&self, target_wu_price: f64) -> u64 {
        let target_wu = self.create_queue_diff_elapsed.as_nanos() as f64 / target_wu_price;
        target_wu.saturating_to_u64()
    }

    pub fn create_queue_diff_wu_price(&self) -> f64 {
        if self.create_queue_diff_wu == 0 {
            return 0.0;
        }
        self.create_queue_diff_elapsed.as_nanos() as f64 / self.create_queue_diff_wu as f64
    }

    pub fn calc_target_apply_queue_diff_wu_param(&self, target_wu: u64) -> Option<u64> {
        let base = self.diff_msgs_count;
        if base == 0 {
            return None;
        }
        Some(target_wu.saturating_div(base))
    }

    pub fn calc_target_apply_queue_diff_wu(&self, target_build_in_msgs_wu: u64) -> u64 {
        self.calc_target_wu_from_build_in_msgs_wu(
            self.apply_queue_diff_elapsed.as_nanos(),
            target_build_in_msgs_wu,
        )
    }

    pub fn calc_target_apply_queue_diff_wu_by_price(&self, target_wu_price: f64) -> u64 {
        let target_wu = self.apply_queue_diff_elapsed.as_nanos() as f64 / target_wu_price;
        target_wu.saturating_to_u64()
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

#[derive(Default, Clone)]
pub struct DoCollateWu {
    pub shard_accounts_count: u64,
    pub updated_accounts_count: u64,

    pub resume_collation_wu: u64,
    pub resume_collation_elapsed: Duration,

    pub resume_collation_wu_per_block: u64,
    pub resume_collation_elapsed_per_block_ns: u128,

    pub collation_total_elapsed: Duration,
}

impl DoCollateWu {
    pub fn calculate_resume_collation_wu(
        &mut self,
        wu_params_finalize: &WorkUnitsParamsFinalize,
        wu_params_execute: &WorkUnitsParamsExecute,
        shard_accounts_count: u64,
        updated_accounts_count: u64,
        mc_data: &McData,
        current_shard: &ShardIdent,
    ) {
        let &WorkUnitsParamsFinalize {
            state_update_msg: state_pow_coeff,
            serialize_diff: updated_accounts_count_pow_coeff,
            diff_tail_len: store_state_wu_param,
            ..
        } = wu_params_finalize;

        let &WorkUnitsParamsExecute {
            subgroup_size: max_threads_count,
            ..
        } = wu_params_execute;

        let threads_count = calc_threads_count(max_threads_count as u64, updated_accounts_count);

        self.shard_accounts_count = shard_accounts_count;
        let shard_accounts_count_log = self.shard_accounts_count_log();
        self.updated_accounts_count = updated_accounts_count;

        let scale = 10;
        let pow_shard_accounts_count = self.pow_shard_accounts_count(state_pow_coeff, scale);
        let pow_updated_accounts_count =
            self.pow_updated_accounts_count(updated_accounts_count_pow_coeff, scale);

        self.calc_resume_collation_wu(
            threads_count,
            pow_updated_accounts_count,
            shard_accounts_count_log,
            pow_shard_accounts_count,
            scale,
            store_state_wu_param as u64,
        );

        let blocks_count_between_masters = mc_data.get_blocks_count_between_masters(current_shard);
        self.resume_collation_wu_per_block = self
            .resume_collation_wu
            .saturating_div(blocks_count_between_masters.max(1));
        self.resume_collation_elapsed_per_block_ns = self
            .resume_collation_elapsed
            .as_nanos()
            .saturating_div(blocks_count_between_masters.max(1) as u128);
    }

    pub fn shard_accounts_count_log(&self) -> u64 {
        self.shard_accounts_count
            .checked_ilog2()
            .unwrap_or_default() as u64
    }

    pub fn pow_shard_accounts_count(&self, state_pow_coeff: u16, scale: u128) -> u128 {
        let (numerator, denominator) = unpack_from_u16(state_pow_coeff);
        compute_scaled_pow(
            self.shard_accounts_count as u128,
            numerator as u32,
            denominator as u32,
            scale,
        )
    }

    pub fn normalized_updated_accounts_count(&self) -> u64 {
        if self.updated_accounts_count == 0 {
            0
        } else {
            self.updated_accounts_count.max(4000)
        }
    }

    pub fn pow_updated_accounts_count(&self, state_pow_coeff: u16, scale: u128) -> u128 {
        let normilized_updated_accounts_count = self.normalized_updated_accounts_count();
        let (numerator, denominator) = unpack_from_u16(state_pow_coeff);
        compute_scaled_pow(
            normilized_updated_accounts_count as u128,
            numerator as u32,
            denominator as u32,
            scale,
        )
    }

    fn calc_resume_collation_wu(
        &mut self,
        threads_count: u64,
        pow_updated_accounts_count: u128,
        shard_accounts_count_log: u64,
        pow_shard_accounts_count: u128,
        scale: u128,
        store_state_wu_param: u64,
    ) {
        self.resume_collation_wu = self
            .calc_resume_collation_wu_base(
                pow_updated_accounts_count,
                shard_accounts_count_log,
                pow_shard_accounts_count,
            )
            .saturating_mul(store_state_wu_param as u128)
            .saturating_div(threads_count as u128)
            .saturating_div(scale)
            .saturating_div(scale) as u64;
    }

    fn calc_resume_collation_wu_base(
        &self,
        pow_updated_accounts_count: u128,
        shard_accounts_count_log: u64,
        pow_shard_accounts_count: u128,
    ) -> u128 {
        (self.updated_accounts_count as u128)
            .saturating_mul(pow_updated_accounts_count)
            .saturating_mul(shard_accounts_count_log as u128)
            .saturating_mul(pow_shard_accounts_count)
    }

    pub fn calc_target_resume_collation_wu_param(
        &self,
        target_wu: u64,
        threads_count: u64,
        pow_updated_accounts_count: u128,
        shard_accounts_count_log: u64,
        pow_shard_accounts_count: u128,
        scale: u128,
    ) -> Option<u64> {
        let base = self.calc_resume_collation_wu_base(
            pow_updated_accounts_count,
            shard_accounts_count_log,
            pow_shard_accounts_count,
        );
        if base == 0 {
            return None;
        }
        Some(
            (target_wu as u128)
                .saturating_mul(scale)
                .saturating_mul(scale)
                .saturating_mul(threads_count as u128)
                .saturating_div(base) as u64,
        )
    }

    pub fn calc_target_resume_collation_wu(
        &self,
        target_build_in_msgs_wu: u64,
        build_in_msgs_elapsed_ns: u128,
    ) -> u64 {
        FinalizeWu::calc_target_wu_from_build_in_msgs_wu_and_elapsed(
            self.resume_collation_elapsed.as_nanos(),
            target_build_in_msgs_wu,
            build_in_msgs_elapsed_ns,
        )
    }

    pub fn calc_target_resume_collation_wu_by_price(&self, target_wu_price: f64) -> u64 {
        let target_wu = self.resume_collation_elapsed.as_nanos() as f64 / target_wu_price;
        target_wu.saturating_to_u64()
    }

    pub fn resume_collation_wu_price(&self) -> f64 {
        if self.resume_collation_wu == 0 {
            return 0.0;
        }
        self.resume_collation_elapsed.as_nanos() as f64 / self.resume_collation_wu as f64
    }

    pub fn total_elapsed_ns(&self) -> u128 {
        self.collation_total_elapsed
            .as_nanos()
            .saturating_add(self.resume_collation_elapsed_per_block_ns)
    }
}

pub fn calc_threads_count(max_threads_count: u64, updated_accounts_count: u64) -> u64 {
    (max_threads_count).min(updated_accounts_count).max(1)
}

pub struct WuEvent {
    pub shard: ShardIdent,
    pub seqno: BlockSeqno,
    pub data: WuEventData,
}
pub enum WuEventData {
    Metrics(Box<WuMetrics>),
    AnchorLag(MempoolAnchorLag),
}

#[derive(Debug, Default, Clone)]
pub struct MempoolAnchorLag {
    pub requested_at: u64,
    pub chain_time: u64,
}

impl MempoolAnchorLag {
    pub fn lag(&self) -> i64 {
        (self.requested_at as i64).saturating_sub(self.chain_time as i64)
    }
}

#[derive(Default)]
pub struct WuMetrics {
    pub wu_params: WorkUnitsParams,
    pub wu_on_prepare_msg_groups: PrepareMsgGroupsWu,
    pub wu_on_execute: ExecuteWu,
    pub wu_on_finalize: FinalizeWu,
    pub wu_on_do_collate: DoCollateWu,
    pub has_pending_messages: bool,
}

impl WuMetrics {
    pub fn collation_total_wu(&self) -> u64 {
        self.wu_on_prepare_msg_groups.total_wu()
            + self.wu_on_execute.total_wu()
            + self.wu_on_finalize.total_wu()
    }

    pub fn collation_total_wu_price(&self) -> f64 {
        let collation_total_wu = self.collation_total_wu();
        if collation_total_wu == 0 {
            0.0
        } else {
            self.wu_on_do_collate.collation_total_elapsed.as_nanos() as f64
                / collation_total_wu as f64
        }
    }

    pub fn report_metrics(&self, shard: &ShardIdent) {
        let labels = [("workchain", shard.workchain().to_string())];

        // prepare
        metrics::gauge!("tycho_do_collate_wu_on_prepare", &labels)
            .set(self.wu_on_prepare_msg_groups.total_wu() as f64);
        metrics::gauge!("tycho_do_collate_wu_price_on_prepare", &labels)
            .set(self.wu_on_prepare_msg_groups.total_wu_price());
        metrics::gauge!("tycho_do_collate_wu_on_prepare_read_ext_msgs", &labels)
            .set(self.wu_on_prepare_msg_groups.read_ext_msgs_wu as f64);
        metrics::gauge!(
            "tycho_do_collate_wu_price_on_prepare_read_ext_msgs",
            &labels
        )
        .set(self.wu_on_prepare_msg_groups.read_ext_msgs_wu_price());
        metrics::gauge!(
            "tycho_do_collate_wu_on_prepare_read_existing_int_msgs",
            &labels
        )
        .set(self.wu_on_prepare_msg_groups.read_existing_int_msgs_wu as f64);
        metrics::gauge!(
            "tycho_do_collate_wu_price_on_prepare_read_existing_int_msgs",
            &labels
        )
        .set(
            self.wu_on_prepare_msg_groups
                .read_existing_int_msgs_wu_price(),
        );
        metrics::gauge!("tycho_do_collate_wu_on_prepare_read_new_int_msgs", &labels)
            .set(self.wu_on_prepare_msg_groups.read_new_int_msgs_wu as f64);
        metrics::gauge!(
            "tycho_do_collate_wu_price_on_prepare_read_new_int_msgs",
            &labels
        )
        .set(self.wu_on_prepare_msg_groups.read_new_int_msgs_wu_price());
        metrics::gauge!("tycho_do_collate_wu_on_prepare_add_msgs_to_groups", &labels)
            .set(self.wu_on_prepare_msg_groups.add_msgs_to_groups_wu as f64);
        metrics::gauge!(
            "tycho_do_collate_wu_price_on_prepare_add_msgs_to_groups",
            &labels
        )
        .set(self.wu_on_prepare_msg_groups.add_msgs_to_groups_wu_price());

        // execute
        metrics::gauge!("tycho_do_collate_wu_on_execute", &labels)
            .set(self.wu_on_execute.total_wu() as f64);
        metrics::gauge!("tycho_do_collate_wu_price_on_execute", &labels)
            .set(self.wu_on_execute.total_wu_price());
        metrics::gauge!("tycho_do_collate_wu_on_execute_txs", &labels)
            .set(self.wu_on_execute.execute_groups_vm_only_wu as f64);
        metrics::gauge!("tycho_do_collate_wu_price_on_execute_txs", &labels)
            .set(self.wu_on_execute.execute_groups_vm_only_wu_price());
        metrics::gauge!("tycho_do_collate_wu_on_process_txs", &labels)
            .set(self.wu_on_execute.process_txs_wu as f64);
        metrics::gauge!("tycho_do_collate_wu_price_on_process_txs", &labels)
            .set(self.wu_on_execute.process_txs_wu_price());

        // create queue diff
        metrics::gauge!("tycho_do_collate_wu_on_create_queue_diff", &labels)
            .set(self.wu_on_finalize.create_queue_diff_wu as f64);
        metrics::gauge!("tycho_do_collate_wu_price_on_create_queue_diff", &labels)
            .set(self.wu_on_finalize.create_queue_diff_wu_price());

        // apply queue diff
        metrics::gauge!("tycho_do_collate_wu_on_apply_queue_diff", &labels)
            .set(self.wu_on_finalize.apply_queue_diff_wu as f64);
        metrics::gauge!("tycho_do_collate_wu_price_on_apply_queue_diff", &labels)
            .set(self.wu_on_finalize.apply_queue_diff_wu_price());

        // finalize block
        metrics::gauge!("tycho_do_collate_wu_on_finalize_total", &labels)
            .set(self.wu_on_finalize.total_wu() as f64);
        metrics::gauge!("tycho_do_collate_wu_price_on_finalize_total", &labels)
            .set(self.wu_on_finalize.total_wu_price());
        metrics::gauge!("tycho_do_collate_wu_on_finalize_block", &labels)
            .set(self.wu_on_finalize.finalize_block_wu() as f64);
        metrics::gauge!("tycho_do_collate_wu_price_on_finalize_block", &labels)
            .set(self.wu_on_finalize.finalize_block_wu_price());
        metrics::gauge!("tycho_do_collate_wu_on_build_block", &labels)
            .set(self.wu_on_finalize.build_block_wu as f64);
        metrics::gauge!("tycho_do_collate_wu_price_on_build_block", &labels)
            .set(self.wu_on_finalize.build_block_wu_price());
        metrics::gauge!("tycho_do_collate_wu_on_build_state_update", &labels)
            .set(self.wu_on_finalize.build_state_update_wu as f64);
        metrics::gauge!("tycho_do_collate_wu_price_on_build_state_update", &labels)
            .set(self.wu_on_finalize.build_state_update_wu_price());
        metrics::gauge!(
            "tycho_do_collate_wu_on_build_accounts_and_messages",
            &labels
        )
        .set(self.wu_on_finalize.max_accounts_in_out_msgs_wu() as f64);
        metrics::gauge!(
            "tycho_do_collate_wu_price_on_build_accounts_and_messages",
            &labels
        )
        .set(
            self.wu_on_finalize
                .build_accounts_and_messages_in_parallel_wu_price(),
        );
        metrics::gauge!("tycho_do_collate_wu_on_build_in_msgs", &labels)
            .set(self.wu_on_finalize.build_in_msgs_wu as f64);
        metrics::gauge!("tycho_do_collate_wu_price_on_build_in_msgs", &labels)
            .set(self.wu_on_finalize.build_in_msgs_wu_price());
        metrics::gauge!("tycho_do_collate_wu_on_build_out_msgs", &labels)
            .set(self.wu_on_finalize.build_out_msgs_wu as f64);
        metrics::gauge!("tycho_do_collate_wu_price_on_build_out_msgs", &labels)
            .set(self.wu_on_finalize.build_out_msgs_wu_price());
        metrics::gauge!("tycho_do_collate_wu_on_update_shard_accounts", &labels)
            .set(self.wu_on_finalize.update_shard_accounts_wu as f64);
        metrics::gauge!(
            "tycho_do_collate_wu_price_on_update_shard_accounts",
            &labels
        )
        .set(self.wu_on_finalize.update_shard_accounts_wu_price());
        metrics::gauge!("tycho_do_collate_wu_on_build_accounts_blocks", &labels)
            .set(self.wu_on_finalize.build_accounts_blocks_wu as f64);
        metrics::gauge!(
            "tycho_do_collate_wu_price_on_build_accounts_blocks",
            &labels
        )
        .set(self.wu_on_finalize.build_accounts_blocks_wu_price());
        metrics::gauge!("tycho_do_collate_wu_on_build_accounts", &labels)
            .set(self.wu_on_finalize.build_accounts_wu() as f64);
        metrics::gauge!("tycho_do_collate_wu_price_on_build_accounts", &labels)
            .set(self.wu_on_finalize.build_accounts_wu_price());

        // total on collation
        let collation_total_wu = self.collation_total_wu();
        metrics::gauge!("tycho_do_collate_wu_total", &labels).set(collation_total_wu as f64);
        metrics::gauge!("tycho_do_collate_wu_price_total", &labels)
            .set(self.collation_total_wu_price());

        // resume collation
        metrics::gauge!("tycho_do_collate_wu_on_resume_collation", &labels)
            .set(self.wu_on_do_collate.resume_collation_wu as f64);
        metrics::gauge!("tycho_do_collate_wu_price_on_resume_collation", &labels)
            .set(self.wu_on_do_collate.resume_collation_wu_price());

        // total
        let total_wu = collation_total_wu + self.wu_on_do_collate.resume_collation_wu_per_block;
        let total_wu_price = if total_wu == 0 {
            0.0
        } else {
            self.wu_on_do_collate.total_elapsed_ns() as f64 / total_wu as f64
        };
        metrics::gauge!("tycho_do_collate_wu_total_full", &labels).set(total_wu as f64);
        metrics::gauge!("tycho_do_collate_wu_price_total_full", &labels).set(total_wu_price);
    }
}

pub fn report_anchor_lag_to_metrics(shard: &ShardIdent, lag: i64) {
    let labels = [("workchain", shard.workchain().to_string())];
    metrics::gauge!("tycho_collator_anchor_importing_lag_ms", &labels).set(lag as f64);
}

pub trait F64Ext {
    fn saturating_to_u64(self) -> u64;
}

impl F64Ext for f64 {
    fn saturating_to_u64(self) -> u64 {
        if self >= u64::MAX as f64 {
            u64::MAX
        } else if self < 0.0 {
            u64::MIN
        } else {
            self.trunc() as u64
        }
    }
}

/// Packs two numbers (0..=99) into a single u16: format "AA BB" -> AA*100 + BB
pub fn pack_into_u16(a: u16, b: u16) -> u16 {
    assert!(a <= 99, "{a} is out of range 0..=99");
    assert!(b <= 99, "{b} is out of range 0..=99");
    a * 100 + b
}

/// Unpacks two numbers from u16.
/// Numbers should be in range 0..=99
pub fn unpack_from_u16(packed: u16) -> (u16, u16) {
    if packed > 99 * 100 + 99 {
        return (1, 1);
    }
    let a = packed / 100;

    let mut b = packed % 100;
    if b == 0 {
        b = 1;
    }

    (a, b)
}

pub fn compute_scaled_pow(
    x: u128,
    pow_coeff_numerator: u32,
    pow_coeff_denominator: u32,
    scale: u128,
) -> u128 {
    assert_ne!(pow_coeff_denominator, 0);

    fn compute_root(y: BigUint, denominator: u32) -> BigUint {
        let one = BigUint::from(1u128);
        let mut low = BigUint::ZERO;
        let mut high = &y + &one;

        while &low + &one < high {
            let mid: BigUint = (&low + &high) >> 1;
            let power = mid.pow(denominator);
            if power <= y {
                low = mid;
            } else {
                high = mid;
            }
        }

        low
    }

    let x_pow = BigUint::from(x).pow(pow_coeff_numerator);
    let scale_pow = BigUint::from(scale).pow(pow_coeff_denominator);
    let y = x_pow * scale_pow;

    let res = compute_root(y, pow_coeff_denominator);
    res.try_into().unwrap_or(u128::MAX)
}

#[test]
fn test_compute_scaled_pow() {
    let scale = 100;

    // 20k^1.2
    let x = 20000;
    let pow_coeff = 105;
    let (pow_coeff_numerator, pow_coeff_denominator) = unpack_from_u16(pow_coeff);

    let res = compute_scaled_pow(
        x,
        pow_coeff_numerator as u32,
        pow_coeff_denominator as u32,
        scale,
    );
    println!(
        "{x}^({pow_coeff_numerator}/{pow_coeff_denominator}) = {}",
        (res as f64 / scale as f64),
    );

    let res = x * res;
    println!(
        "{x}^(1+{pow_coeff_numerator}/{pow_coeff_denominator}) = {}",
        (res as f64 / scale as f64),
    );

    // 20k^0/0
    let pow_coeff = 0;
    let (pow_coeff_numerator, pow_coeff_denominator) = unpack_from_u16(pow_coeff);
    let res = compute_scaled_pow(
        x,
        pow_coeff_numerator as u32,
        pow_coeff_denominator as u32,
        scale,
    );
    println!(
        "{x}^({pow_coeff_numerator}/{pow_coeff_denominator}) = {}",
        (res as f64 / scale as f64),
    );

    // 10M^0.16
    let x = 10000000;
    let pow_coeff = 425;
    let (pow_coeff_numerator, pow_coeff_denominator) = unpack_from_u16(pow_coeff);

    let res = compute_scaled_pow(
        x,
        pow_coeff_numerator as u32,
        pow_coeff_denominator as u32,
        scale,
    );
    println!(
        "{x}^({pow_coeff_numerator}/{pow_coeff_denominator}) = {}",
        (res as f64 / scale as f64),
    );
}
