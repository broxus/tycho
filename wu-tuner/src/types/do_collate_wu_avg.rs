use std::time::Duration;

use tycho_collator::collator::work_units::{DoCollateWu, WuAccumSums};
use tycho_util::num::SafeUnsignedAvg;

use crate::unit_cost_clipper::UnitCostClippers;

#[derive(Default)]
pub struct DoCollateElapsedClipSnapshot {
    pub resume_collation_elapsed_ns: Option<u128>,
    pub resume_collation_elapsed_per_block_ns: Option<u128>,
    pub collation_total_elapsed_ns: Option<u128>,
}

#[derive(Default)]
pub struct DoCollateWuAvg {
    pub resume_collation_wu: SafeUnsignedAvg,
    pub resume_collation_elapsed_ns: SafeUnsignedAvg,
    pub resume_collation_base: SafeUnsignedAvg,
    /// Stores `(clipped_elapsed_ns, resume_collation_base)` for the target param calculation
    pub resume_collation_sums_accum: WuAccumSums,

    pub resume_collation_wu_per_block: SafeUnsignedAvg,
    pub resume_collation_elapsed_per_block_ns: SafeUnsignedAvg,

    pub collation_total_elapsed_ns: SafeUnsignedAvg,
}

impl DoCollateWuAvg {
    pub fn accum(
        &mut self,
        v: &DoCollateWu,
        collation_total_wu: u64,
        unit_cost_clippers: &mut UnitCostClippers,
    ) {
        // reuse one clipped snapshot for both displayed averages and raw sums for the target params calculation
        let clipped_snapshot = Self::clip_elapsed(v, collation_total_wu, unit_cost_clippers);
        self.accum_avg_result(v, &clipped_snapshot);
        self.accum_raw_sums(v, &clipped_snapshot);
    }

    fn clip_elapsed(
        v: &DoCollateWu,
        collation_total_wu: u64,
        unit_cost_clippers: &mut UnitCostClippers,
    ) -> DoCollateElapsedClipSnapshot {
        DoCollateElapsedClipSnapshot {
            resume_collation_elapsed_ns: unit_cost_clippers
                .do_collate
                .resume_collation
                .clip_elapsed_ns(
                    v.resume_collation_base,
                    v.resume_collation_elapsed.as_nanos(),
                ),
            resume_collation_elapsed_per_block_ns: unit_cost_clippers
                .do_collate
                .resume_collation_per_block
                .clip_elapsed_ns(
                    v.resume_collation_wu_per_block as u128,
                    v.resume_collation_elapsed_per_block_ns,
                ),
            collation_total_elapsed_ns: unit_cost_clippers
                .do_collate
                .collation_total_elapsed
                .clip_elapsed_ns(
                    collation_total_wu as u128,
                    v.collation_total_elapsed.as_nanos(),
                ),
        }
    }

    pub fn accum_avg_result(
        &mut self,
        v: &DoCollateWu,
        clipped_snapshot: &DoCollateElapsedClipSnapshot,
    ) {
        self.resume_collation_wu.accum(v.resume_collation_wu);
        self.resume_collation_elapsed_ns.accum(
            clipped_snapshot
                .resume_collation_elapsed_ns
                .unwrap_or(v.resume_collation_elapsed.as_nanos()),
        );
        self.resume_collation_base.accum(v.resume_collation_base);
        self.resume_collation_wu_per_block
            .accum(v.resume_collation_wu_per_block);
        self.resume_collation_elapsed_per_block_ns.accum(
            clipped_snapshot
                .resume_collation_elapsed_per_block_ns
                .unwrap_or(v.resume_collation_elapsed_per_block_ns),
        );
        self.collation_total_elapsed_ns.accum(
            clipped_snapshot
                .collation_total_elapsed_ns
                .unwrap_or(v.collation_total_elapsed.as_nanos()),
        );
    }

    fn accum_raw_sums(&mut self, v: &DoCollateWu, clipped_snapshot: &DoCollateElapsedClipSnapshot) {
        if let Some(resume_collation_elapsed_clip_ns) = clipped_snapshot.resume_collation_elapsed_ns
        {
            self.resume_collation_sums_accum
                .add((resume_collation_elapsed_clip_ns, v.resume_collation_base));
        }
    }

    pub fn merge_sums_accum(&mut self, other: &Self) {
        self.resume_collation_sums_accum
            .merge(&other.resume_collation_sums_accum);
    }

    pub fn get_avg(
        &self,
        last_shard_accounts_count: u64,
        avg_updated_accounts_count: u64,
    ) -> DoCollateWu {
        let avg_u64 = |avg: &SafeUnsignedAvg| avg.get_avg_checked().unwrap_or_default() as u64;
        let avg_u128 = |avg: &SafeUnsignedAvg| avg.get_avg_checked().unwrap_or_default();

        DoCollateWu {
            shard_accounts_count: last_shard_accounts_count,
            updated_accounts_count: avg_updated_accounts_count,

            resume_collation_wu: avg_u64(&self.resume_collation_wu),
            resume_collation_elapsed: Duration::from_nanos(avg_u64(
                &self.resume_collation_elapsed_ns,
            )),
            resume_collation_base: avg_u128(&self.resume_collation_base),

            resume_collation_wu_per_block: avg_u64(&self.resume_collation_wu_per_block),
            resume_collation_elapsed_per_block_ns: avg_u128(
                &self.resume_collation_elapsed_per_block_ns,
            ),

            collation_total_elapsed: Duration::from_nanos(avg_u64(
                &self.collation_total_elapsed_ns,
            )),
        }
    }
}
