use std::time::Duration;

use tycho_collator::collator::work_units::{PrepareMsgGroupsWu, WuAccumSums};
use tycho_util::num::SafeUnsignedAvg;

use crate::unit_cost_clipper::UnitCostClippers;

#[derive(Default)]
pub struct PrepareElapsedClipSnapshot {
    pub read_ext_msgs_elapsed_ns: Option<u128>,
    pub read_existing_int_msgs_elapsed_ns: Option<u128>,
    pub read_new_int_msgs_elapsed_ns: Option<u128>,
    pub add_msgs_to_groups_elapsed_ns: Option<u128>,
    pub total_elapsed_ns: Option<u128>,
}

#[derive(Default)]
pub struct PrepareMsgGroupsWuAvg {
    pub fixed_part: SafeUnsignedAvg,

    pub read_ext_msgs_count: SafeUnsignedAvg,
    pub read_ext_msgs_wu: SafeUnsignedAvg,
    pub read_ext_msgs_elapsed_ns: SafeUnsignedAvg,
    pub read_ext_msgs_base: SafeUnsignedAvg,

    pub read_existing_int_msgs_count: SafeUnsignedAvg,
    pub read_existing_int_msgs_wu: SafeUnsignedAvg,
    pub read_existing_int_msgs_elapsed_ns: SafeUnsignedAvg,
    pub read_existing_int_msgs_base: SafeUnsignedAvg,

    pub read_new_int_msgs_count: SafeUnsignedAvg,
    pub read_new_int_msgs_wu: SafeUnsignedAvg,
    pub read_new_int_msgs_elapsed_ns: SafeUnsignedAvg,
    pub read_new_int_msgs_base: SafeUnsignedAvg,

    pub add_to_msgs_groups_ops_count: SafeUnsignedAvg,
    pub add_msgs_to_groups_wu: SafeUnsignedAvg,
    pub add_msgs_to_groups_elapsed_ns: SafeUnsignedAvg,
    pub add_msgs_to_groups_base: SafeUnsignedAvg,

    /// Stores `(clipped_elapsed_ns, read_ext_msgs_base)` for the target `read_ext_msgs` param calculation
    pub read_ext_msgs_sums_accum: WuAccumSums,
    /// Stores `(clipped_elapsed_ns, read_existing_int_msgs_base)` for the target `read_int_msgs` param calculation
    pub read_existing_int_msgs_sums_accum: WuAccumSums,
    /// Stores `(clipped_elapsed_ns, read_new_int_msgs_base)` for the target `read_new_msgs` param calculation
    pub read_new_int_msgs_sums_accum: WuAccumSums,
    /// Stores `(clipped_elapsed_ns, add_msgs_to_groups_base)` for the target `add_to_msg_groups` param calculation
    pub add_msgs_to_groups_sums_accum: WuAccumSums,

    pub total_elapsed_ns: SafeUnsignedAvg,
}

impl PrepareMsgGroupsWuAvg {
    pub fn accum(&mut self, v: &PrepareMsgGroupsWu, unit_cost_clippers: &mut UnitCostClippers) {
        // reuse one clipped snapshot for both displayed averages and raw sums for the target params calculation
        let clipped_snapshot = Self::clip_elapsed(v, unit_cost_clippers);
        self.accum_avg_result(v, &clipped_snapshot);
        self.accum_raw_sums(v, &clipped_snapshot);
    }

    fn clip_elapsed(
        v: &PrepareMsgGroupsWu,
        unit_cost_clippers: &mut UnitCostClippers,
    ) -> PrepareElapsedClipSnapshot {
        PrepareElapsedClipSnapshot {
            read_ext_msgs_elapsed_ns: unit_cost_clippers
                .prepare
                .read_ext_msgs
                .clip_elapsed_ns(v.read_ext_msgs_base, v.read_ext_msgs_elapsed.as_nanos()),
            read_existing_int_msgs_elapsed_ns: unit_cost_clippers
                .prepare
                .read_existing_int_msgs
                .clip_elapsed_ns(
                    v.read_existing_int_msgs_base,
                    v.read_existing_int_msgs_elapsed.as_nanos(),
                ),
            read_new_int_msgs_elapsed_ns: unit_cost_clippers
                .prepare
                .read_new_int_msgs
                .clip_elapsed_ns(
                    v.read_new_int_msgs_base,
                    v.read_new_int_msgs_elapsed.as_nanos(),
                ),
            add_msgs_to_groups_elapsed_ns: unit_cost_clippers
                .prepare
                .add_msgs_to_groups
                .clip_elapsed_ns(
                    v.add_msgs_to_groups_base,
                    v.add_msgs_to_groups_elapsed.as_nanos(),
                ),
            total_elapsed_ns: unit_cost_clippers
                .prepare
                .total_elapsed
                .clip_elapsed_ns(v.total_wu() as u128, v.total_elapsed.as_nanos()),
        }
    }

    pub fn accum_avg_result(
        &mut self,
        v: &PrepareMsgGroupsWu,
        clipped_snapshot: &PrepareElapsedClipSnapshot,
    ) {
        self.fixed_part.accum(v.fixed_part);
        self.read_ext_msgs_count.accum(v.read_ext_msgs_count);
        self.read_ext_msgs_wu.accum(v.read_ext_msgs_wu);
        self.read_ext_msgs_elapsed_ns.accum(
            clipped_snapshot
                .read_ext_msgs_elapsed_ns
                .unwrap_or(v.read_ext_msgs_elapsed.as_nanos()),
        );
        self.read_ext_msgs_base.accum(v.read_ext_msgs_base);
        self.read_existing_int_msgs_count
            .accum(v.read_existing_int_msgs_count);
        self.read_existing_int_msgs_wu
            .accum(v.read_existing_int_msgs_wu);
        self.read_existing_int_msgs_elapsed_ns.accum(
            clipped_snapshot
                .read_existing_int_msgs_elapsed_ns
                .unwrap_or(v.read_existing_int_msgs_elapsed.as_nanos()),
        );
        self.read_existing_int_msgs_base
            .accum(v.read_existing_int_msgs_base);
        self.read_new_int_msgs_count
            .accum(v.read_new_int_msgs_count);
        self.read_new_int_msgs_wu.accum(v.read_new_int_msgs_wu);
        self.read_new_int_msgs_elapsed_ns.accum(
            clipped_snapshot
                .read_new_int_msgs_elapsed_ns
                .unwrap_or(v.read_new_int_msgs_elapsed.as_nanos()),
        );
        self.read_new_int_msgs_base.accum(v.read_new_int_msgs_base);
        self.add_to_msgs_groups_ops_count
            .accum(v.add_to_msgs_groups_ops_count);
        self.add_msgs_to_groups_wu.accum(v.add_msgs_to_groups_wu);
        self.add_msgs_to_groups_elapsed_ns.accum(
            clipped_snapshot
                .add_msgs_to_groups_elapsed_ns
                .unwrap_or(v.add_msgs_to_groups_elapsed.as_nanos()),
        );
        self.add_msgs_to_groups_base
            .accum(v.add_msgs_to_groups_base);
        self.total_elapsed_ns.accum(
            clipped_snapshot
                .total_elapsed_ns
                .unwrap_or(v.total_elapsed.as_nanos()),
        );
    }

    fn accum_raw_sums(
        &mut self,
        v: &PrepareMsgGroupsWu,
        clipped_snapshot: &PrepareElapsedClipSnapshot,
    ) {
        // only clipped timings used for futher target params calculation
        if let Some(elapsed_clip_ns) = clipped_snapshot.read_ext_msgs_elapsed_ns {
            self.read_ext_msgs_sums_accum
                .add((elapsed_clip_ns, v.read_ext_msgs_base));
        }
        if let Some(elapsed_clip_ns) = clipped_snapshot.read_existing_int_msgs_elapsed_ns {
            self.read_existing_int_msgs_sums_accum
                .add((elapsed_clip_ns, v.read_existing_int_msgs_base));
        }
        if let Some(elapsed_clip_ns) = clipped_snapshot.read_new_int_msgs_elapsed_ns {
            self.read_new_int_msgs_sums_accum
                .add((elapsed_clip_ns, v.read_new_int_msgs_base));
        }
        if let Some(elapsed_clip_ns) = clipped_snapshot.add_msgs_to_groups_elapsed_ns {
            self.add_msgs_to_groups_sums_accum
                .add((elapsed_clip_ns, v.add_msgs_to_groups_base));
        }
    }

    pub fn merge_sums_accum(&mut self, other: &Self) {
        self.read_ext_msgs_sums_accum
            .merge(&other.read_ext_msgs_sums_accum);
        self.read_existing_int_msgs_sums_accum
            .merge(&other.read_existing_int_msgs_sums_accum);
        self.read_new_int_msgs_sums_accum
            .merge(&other.read_new_int_msgs_sums_accum);
        self.add_msgs_to_groups_sums_accum
            .merge(&other.add_msgs_to_groups_sums_accum);
    }

    pub fn get_avg(&self) -> PrepareMsgGroupsWu {
        let avg_u64 = |avg: &SafeUnsignedAvg| avg.get_avg_checked().unwrap_or_default() as u64;
        let avg_u128 = |avg: &SafeUnsignedAvg| avg.get_avg_checked().unwrap_or_default();

        PrepareMsgGroupsWu {
            fixed_part: avg_u64(&self.fixed_part),

            read_ext_msgs_count: avg_u64(&self.read_ext_msgs_count),
            read_ext_msgs_wu: avg_u64(&self.read_ext_msgs_wu),
            read_ext_msgs_elapsed: Duration::from_nanos(avg_u64(&self.read_ext_msgs_elapsed_ns)),
            read_ext_msgs_base: avg_u128(&self.read_ext_msgs_base),

            read_existing_int_msgs_count: avg_u64(&self.read_existing_int_msgs_count),
            read_existing_int_msgs_wu: avg_u64(&self.read_existing_int_msgs_wu),
            read_existing_int_msgs_elapsed: Duration::from_nanos(avg_u64(
                &self.read_existing_int_msgs_elapsed_ns,
            )),
            read_existing_int_msgs_base: avg_u128(&self.read_existing_int_msgs_base),

            read_new_int_msgs_count: avg_u64(&self.read_new_int_msgs_count),
            read_new_int_msgs_wu: avg_u64(&self.read_new_int_msgs_wu),
            read_new_int_msgs_elapsed: Duration::from_nanos(avg_u64(
                &self.read_new_int_msgs_elapsed_ns,
            )),
            read_new_int_msgs_base: avg_u128(&self.read_new_int_msgs_base),

            add_to_msgs_groups_ops_count: avg_u64(&self.add_to_msgs_groups_ops_count),
            add_msgs_to_groups_wu: avg_u64(&self.add_msgs_to_groups_wu),
            add_msgs_to_groups_elapsed: Duration::from_nanos(avg_u64(
                &self.add_msgs_to_groups_elapsed_ns,
            )),
            add_msgs_to_groups_base: avg_u128(&self.add_msgs_to_groups_base),

            total_elapsed: Duration::from_nanos(avg_u64(&self.total_elapsed_ns)),
        }
    }
}
