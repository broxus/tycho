use std::time::Duration;

use tycho_collator::collator::work_units::{ExecuteWu, WuAccumSums, WuAccumSumsExecute};
use tycho_util::num::SafeUnsignedAvg;

use crate::unit_cost_clipper::UnitCostClippers;

#[derive(Default)]
pub struct ExecuteElapsedClipSnapshot {
    pub execute_groups_vm_only_elapsed_ns: Option<u128>,
    pub process_txs_elapsed_ns: Option<u128>,
}

#[derive(Default)]
pub struct ExecuteWuAvg {
    pub inserted_new_msgs_count: SafeUnsignedAvg,

    pub groups_count: SafeUnsignedAvg,

    pub execute_groups_vm_sum_accounts_over_threads: SafeUnsignedAvg,
    pub execute_groups_vm_sum_gas_over_threads: SafeUnsignedAvg,

    pub execute_groups_vm_only_wu: SafeUnsignedAvg,
    pub execute_groups_vm_only_elapsed_ns: SafeUnsignedAvg,

    pub process_txs_base: SafeUnsignedAvg,
    pub process_txs_wu: SafeUnsignedAvg,
    pub process_txs_elapsed_ns: SafeUnsignedAvg,

    /// Stores `(clipped_elapsed_ns, accounts_over_threads, gas_over_threads)` for the target `execute` param calculation
    pub execute_groups_vm_sums_accum: WuAccumSumsExecute,
    /// Stores `(clipped_elapsed_ns, process_txs_base)` for the target param calculation
    pub process_txs_sums_accum: WuAccumSums,
}

impl ExecuteWuAvg {
    pub fn accum(&mut self, v: &ExecuteWu, unit_cost_clippers: &mut UnitCostClippers) {
        // reuse one clipped snapshot for both displayed averages and raw sums for the target params calculation
        let clipped_snapshot = Self::clip_elapsed(v, unit_cost_clippers);
        self.accum_avg_result(v, &clipped_snapshot);
        self.accum_raw_sums(v, &clipped_snapshot);
    }

    fn clip_elapsed(
        v: &ExecuteWu,
        unit_cost_clippers: &mut UnitCostClippers,
    ) -> ExecuteElapsedClipSnapshot {
        ExecuteElapsedClipSnapshot {
            execute_groups_vm_only_elapsed_ns: unit_cost_clippers
                .execute
                .execute_groups_vm
                .clip_elapsed_ns(
                    v.execute_groups_vm_sum_gas_over_threads,
                    v.execute_groups_vm_only_elapsed.as_nanos(),
                ),
            process_txs_elapsed_ns: unit_cost_clippers
                .execute
                .process_txs
                .clip_elapsed_ns(v.process_txs_base, v.process_txs_elapsed.as_nanos()),
        }
    }

    pub fn accum_avg_result(
        &mut self,
        v: &ExecuteWu,
        clipped_snapshot: &ExecuteElapsedClipSnapshot,
    ) {
        self.inserted_new_msgs_count
            .accum(v.inserted_new_msgs_count);
        self.groups_count.accum(v.groups_count);
        self.execute_groups_vm_sum_accounts_over_threads
            .accum(v.execute_groups_vm_sum_accounts_over_threads);
        self.execute_groups_vm_sum_gas_over_threads
            .accum(v.execute_groups_vm_sum_gas_over_threads);
        self.execute_groups_vm_only_wu
            .accum(v.execute_groups_vm_only_wu);
        self.execute_groups_vm_only_elapsed_ns.accum(
            clipped_snapshot
                .execute_groups_vm_only_elapsed_ns
                .unwrap_or(v.execute_groups_vm_only_elapsed.as_nanos()),
        );
        self.process_txs_base.accum(v.process_txs_base);
        self.process_txs_wu.accum(v.process_txs_wu);
        self.process_txs_elapsed_ns.accum(
            clipped_snapshot
                .process_txs_elapsed_ns
                .unwrap_or(v.process_txs_elapsed.as_nanos()),
        );
    }

    fn accum_raw_sums(&mut self, v: &ExecuteWu, clipped_snapshot: &ExecuteElapsedClipSnapshot) {
        if let Some(execute_groups_vm_elapsed_clip_ns) =
            clipped_snapshot.execute_groups_vm_only_elapsed_ns
        {
            self.execute_groups_vm_sums_accum.add((
                execute_groups_vm_elapsed_clip_ns,
                v.execute_groups_vm_sum_accounts_over_threads,
                v.execute_groups_vm_sum_gas_over_threads,
            ));
        }
        if let Some(process_txs_elapsed_clip_ns) = clipped_snapshot.process_txs_elapsed_ns {
            self.process_txs_sums_accum
                .add((process_txs_elapsed_clip_ns, v.process_txs_base));
        }
    }

    pub fn merge_sums_accum(&mut self, other: &Self) {
        self.execute_groups_vm_sums_accum
            .merge(&other.execute_groups_vm_sums_accum);
        self.process_txs_sums_accum
            .merge(&other.process_txs_sums_accum);
    }

    pub fn get_avg(&self, avg_in_msgs_len: u64, avg_out_msgs_len: u64) -> ExecuteWu {
        let avg_u64 = |avg: &SafeUnsignedAvg| avg.get_avg_checked().unwrap_or_default() as u64;
        let avg_u128 = |avg: &SafeUnsignedAvg| avg.get_avg_checked().unwrap_or_default();

        ExecuteWu {
            in_msgs_len: avg_in_msgs_len,
            out_msgs_len: avg_out_msgs_len,
            inserted_new_msgs_count: avg_u64(&self.inserted_new_msgs_count),

            groups_count: avg_u64(&self.groups_count),

            execute_groups_vm_sum_accounts_over_threads: avg_u128(
                &self.execute_groups_vm_sum_accounts_over_threads,
            ),
            execute_groups_vm_sum_gas_over_threads: avg_u128(
                &self.execute_groups_vm_sum_gas_over_threads,
            ),

            execute_groups_vm_only_wu: avg_u64(&self.execute_groups_vm_only_wu),
            execute_groups_vm_only_elapsed: Duration::from_nanos(avg_u64(
                &self.execute_groups_vm_only_elapsed_ns,
            )),

            process_txs_base: avg_u128(&self.process_txs_base),
            process_txs_wu: avg_u64(&self.process_txs_wu),
            process_txs_elapsed: Duration::from_nanos(avg_u64(&self.process_txs_elapsed_ns)),
        }
    }
}
