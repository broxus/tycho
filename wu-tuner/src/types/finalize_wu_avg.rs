use std::time::Duration;

use tycho_collator::collator::work_units::{FinalizeWu, WuAccumSums};
use tycho_util::num::SafeUnsignedAvg;

use crate::unit_cost_clipper::UnitCostClippers;

#[derive(Default)]
pub struct FinalizeElapsedClipSnapshot {
    pub create_queue_diff_elapsed_ns: Option<u128>,
    pub apply_queue_diff_elapsed_ns: Option<u128>,
    pub update_shard_accounts_elapsed_ns: Option<u128>,
    pub build_accounts_blocks_elapsed_ns: Option<u128>,
    pub build_accounts_elapsed_ns: Option<u128>,
    pub build_in_msgs_elapsed_ns: Option<u128>,
    pub build_out_msgs_elapsed_ns: Option<u128>,
    pub build_accounts_and_messages_in_parallel_elased_ns: Option<u128>,
    pub build_state_update_elapsed_ns: Option<u128>,
    pub build_block_elapsed_ns: Option<u128>,
    pub finalize_block_elapsed_ns: Option<u128>,
    pub total_elapsed_ns: Option<u128>,
}

#[derive(Default)]
pub struct FinalizeWuAvg {
    pub diff_msgs_count: SafeUnsignedAvg,
    pub create_diff_base: SafeUnsignedAvg,
    pub apply_diff_base: SafeUnsignedAvg,

    pub create_queue_diff_wu: SafeUnsignedAvg,
    pub create_queue_diff_elapsed_ns: SafeUnsignedAvg,

    pub apply_queue_diff_wu: SafeUnsignedAvg,
    pub apply_queue_diff_elapsed_ns: SafeUnsignedAvg,

    pub updated_accounts_count: SafeUnsignedAvg,
    pub in_msgs_len: SafeUnsignedAvg,
    pub out_msgs_len: SafeUnsignedAvg,

    pub update_shard_accounts_wu: SafeUnsignedAvg,
    pub update_shard_accounts_elapsed_ns: SafeUnsignedAvg,
    pub update_shard_accounts_base: SafeUnsignedAvg,

    pub build_accounts_blocks_wu: SafeUnsignedAvg,
    pub build_accounts_blocks_elapsed_ns: SafeUnsignedAvg,
    pub build_accounts_blocks_base: SafeUnsignedAvg,

    pub build_accounts_elapsed_ns: SafeUnsignedAvg,

    pub build_in_msgs_wu: SafeUnsignedAvg,
    pub build_in_msgs_elapsed_ns: SafeUnsignedAvg,
    pub build_in_msg_base: SafeUnsignedAvg,

    pub build_out_msgs_wu: SafeUnsignedAvg,
    pub build_out_msgs_elapsed_ns: SafeUnsignedAvg,
    pub build_out_msg_base: SafeUnsignedAvg,

    pub build_accounts_and_messages_in_parallel_elased_ns: SafeUnsignedAvg,

    pub build_state_update_wu: SafeUnsignedAvg,
    pub build_state_update_elapsed_ns: SafeUnsignedAvg,
    pub build_state_update_base: SafeUnsignedAvg,

    pub build_block_wu: SafeUnsignedAvg,
    pub build_block_elapsed_ns: SafeUnsignedAvg,
    pub build_block_base: SafeUnsignedAvg,

    pub finalize_block_elapsed_ns: SafeUnsignedAvg,

    /// Stores `(clipped_elapsed_ns, build_in_msg_base)` for the target `build_in_msg` param calculation
    pub build_in_msg_sums_accum: WuAccumSums,
    /// Stores `(clipped_elapsed_ns, build_out_msg_base)` for the target `build_out_msg` param calculation
    pub build_out_msg_sums_accum: WuAccumSums,
    /// Stores `(clipped_elapsed_ns, build_accounts_blocks_base)` for the target `build_transactions` param calculation
    pub build_accounts_blocks_sums_accum: WuAccumSums,
    /// Stores `(clipped_elapsed_ns, update_shard_accounts_base)` for the target `build_accounts` param calculation
    pub update_shard_accounts_sums_accum: WuAccumSums,
    /// Stores `(clipped_elapsed_ns, build_state_update_base)` for the target `state_update_accounts` param calculation
    pub build_state_update_sums_accum: WuAccumSums,
    /// Stores `(clipped_elapsed_ns, build_block_base)` for the target serialization params calculation
    pub build_block_sums_accum: WuAccumSums,
    /// Stores `(clipped_elapsed_ns, create_diff_base)` for the target `create_diff` param calculation
    pub create_diff_sums_accum: WuAccumSums,
    /// Stores `(clipped_elapsed_ns, apply_diff_base)` for the target `apply_diff` param calculation
    pub apply_diff_sums_accum: WuAccumSums,

    pub total_elapsed_ns: SafeUnsignedAvg,
}

impl FinalizeWuAvg {
    pub fn accum(
        &mut self,
        v: &FinalizeWu,
        state_update_min_wu: u64,
        serialize_min_wu: u64,
        unit_cost_clippers: &mut UnitCostClippers,
    ) {
        // reuse one clipped snapshot for both displayed averages and raw sums for the target params calculation
        let clipped_snapshot =
            Self::clip_elapsed(v, state_update_min_wu, serialize_min_wu, unit_cost_clippers);
        self.accum_avg_result(v, &clipped_snapshot);
        self.accum_raw_sums(v, &clipped_snapshot);
    }

    fn clip_elapsed(
        v: &FinalizeWu,
        state_update_min_wu: u64,
        serialize_min_wu: u64,
        unit_cost_clippers: &mut UnitCostClippers,
    ) -> FinalizeElapsedClipSnapshot {
        FinalizeElapsedClipSnapshot {
            create_queue_diff_elapsed_ns: unit_cost_clippers
                .finalize
                .create_diff
                .clip_elapsed_ns(v.create_diff_base, v.create_queue_diff_elapsed.as_nanos()),
            apply_queue_diff_elapsed_ns: unit_cost_clippers
                .finalize
                .apply_diff
                .clip_elapsed_ns(v.apply_diff_base, v.apply_queue_diff_elapsed.as_nanos()),
            update_shard_accounts_elapsed_ns: unit_cost_clippers
                .finalize
                .update_shard_accounts
                .clip_elapsed_ns(
                    v.update_shard_accounts_base,
                    v.update_shard_accounts_elapsed.as_nanos(),
                ),
            build_accounts_blocks_elapsed_ns: unit_cost_clippers
                .finalize
                .build_accounts_blocks
                .clip_elapsed_ns(
                    v.build_accounts_blocks_base,
                    v.build_accounts_blocks_elapsed.as_nanos(),
                ),
            build_accounts_elapsed_ns: unit_cost_clippers.finalize.build_accounts.clip_elapsed_ns(
                v.build_accounts_wu() as u128,
                v.build_accounts_elapsed.as_nanos(),
            ),
            build_in_msgs_elapsed_ns: unit_cost_clippers
                .finalize
                .build_in_msg
                .clip_elapsed_ns(v.build_in_msg_base, v.build_in_msgs_elapsed.as_nanos()),
            build_out_msgs_elapsed_ns: unit_cost_clippers
                .finalize
                .build_out_msg
                .clip_elapsed_ns(v.build_out_msg_base, v.build_out_msgs_elapsed.as_nanos()),
            build_accounts_and_messages_in_parallel_elased_ns: unit_cost_clippers
                .finalize
                .build_accounts_and_messages_in_parallel
                .clip_elapsed_ns(
                    v.max_accounts_in_out_msgs_wu() as u128,
                    v.build_accounts_and_messages_in_parallel_elased.as_nanos(),
                ),

            // skip min-dominated samples so as not to distort the target params calculation
            build_state_update_elapsed_ns: if v.build_state_update_base > 0
                && v.build_state_update_wu > state_update_min_wu
            {
                unit_cost_clippers
                    .finalize
                    .build_state_update
                    .clip_elapsed_ns(
                        v.build_state_update_base,
                        v.build_state_update_elapsed.as_nanos(),
                    )
            } else {
                None
            },
            build_block_elapsed_ns: if v.build_block_base > 0 && v.build_block_wu > serialize_min_wu
            {
                unit_cost_clippers
                    .finalize
                    .build_block
                    .clip_elapsed_ns(v.build_block_base, v.build_block_elapsed.as_nanos())
            } else {
                None
            },

            finalize_block_elapsed_ns: unit_cost_clippers.finalize.finalize_block.clip_elapsed_ns(
                v.finalize_block_wu() as u128,
                v.finalize_block_elapsed.as_nanos(),
            ),
            total_elapsed_ns: unit_cost_clippers
                .finalize
                .total_elapsed
                .clip_elapsed_ns(v.total_wu() as u128, v.total_elapsed.as_nanos()),
        }
    }

    pub fn accum_avg_result(
        &mut self,
        v: &FinalizeWu,
        clipped_snapshot: &FinalizeElapsedClipSnapshot,
    ) {
        self.diff_msgs_count.accum(v.diff_msgs_count);
        self.create_diff_base.accum(v.create_diff_base);
        self.apply_diff_base.accum(v.apply_diff_base);
        self.create_queue_diff_wu.accum(v.create_queue_diff_wu);
        self.create_queue_diff_elapsed_ns.accum(
            clipped_snapshot
                .create_queue_diff_elapsed_ns
                .unwrap_or(v.create_queue_diff_elapsed.as_nanos()),
        );
        self.apply_queue_diff_wu.accum(v.apply_queue_diff_wu);
        self.apply_queue_diff_elapsed_ns.accum(
            clipped_snapshot
                .apply_queue_diff_elapsed_ns
                .unwrap_or(v.apply_queue_diff_elapsed.as_nanos()),
        );
        self.updated_accounts_count.accum(v.updated_accounts_count);
        self.in_msgs_len.accum(v.in_msgs_len);
        self.out_msgs_len.accum(v.out_msgs_len);
        self.update_shard_accounts_wu
            .accum(v.update_shard_accounts_wu);
        self.update_shard_accounts_elapsed_ns.accum(
            clipped_snapshot
                .update_shard_accounts_elapsed_ns
                .unwrap_or(v.update_shard_accounts_elapsed.as_nanos()),
        );
        self.update_shard_accounts_base
            .accum(v.update_shard_accounts_base);
        self.build_accounts_blocks_wu
            .accum(v.build_accounts_blocks_wu);
        self.build_accounts_blocks_elapsed_ns.accum(
            clipped_snapshot
                .build_accounts_blocks_elapsed_ns
                .unwrap_or(v.build_accounts_blocks_elapsed.as_nanos()),
        );
        self.build_accounts_blocks_base
            .accum(v.build_accounts_blocks_base);
        self.build_accounts_elapsed_ns.accum(
            clipped_snapshot
                .build_accounts_elapsed_ns
                .unwrap_or(v.build_accounts_elapsed.as_nanos()),
        );
        self.build_in_msgs_wu.accum(v.build_in_msgs_wu);
        self.build_in_msgs_elapsed_ns.accum(
            clipped_snapshot
                .build_in_msgs_elapsed_ns
                .unwrap_or(v.build_in_msgs_elapsed.as_nanos()),
        );
        self.build_in_msg_base.accum(v.build_in_msg_base);
        self.build_out_msgs_wu.accum(v.build_out_msgs_wu);
        self.build_out_msgs_elapsed_ns.accum(
            clipped_snapshot
                .build_out_msgs_elapsed_ns
                .unwrap_or(v.build_out_msgs_elapsed.as_nanos()),
        );
        self.build_out_msg_base.accum(v.build_out_msg_base);
        self.build_accounts_and_messages_in_parallel_elased_ns
            .accum(
                clipped_snapshot
                    .build_accounts_and_messages_in_parallel_elased_ns
                    .unwrap_or(v.build_accounts_and_messages_in_parallel_elased.as_nanos()),
            );
        self.build_state_update_wu.accum(v.build_state_update_wu);
        self.build_state_update_elapsed_ns.accum(
            clipped_snapshot
                .build_state_update_elapsed_ns
                .unwrap_or(v.build_state_update_elapsed.as_nanos()),
        );
        self.build_state_update_base
            .accum(v.build_state_update_base);
        self.build_block_wu.accum(v.build_block_wu);
        self.build_block_elapsed_ns.accum(
            clipped_snapshot
                .build_block_elapsed_ns
                .unwrap_or(v.build_block_elapsed.as_nanos()),
        );
        self.build_block_base.accum(v.build_block_base);
        self.finalize_block_elapsed_ns.accum(
            clipped_snapshot
                .finalize_block_elapsed_ns
                .unwrap_or(v.finalize_block_elapsed.as_nanos()),
        );
        self.total_elapsed_ns.accum(
            clipped_snapshot
                .total_elapsed_ns
                .unwrap_or(v.total_elapsed.as_nanos()),
        );
    }

    fn accum_raw_sums(&mut self, v: &FinalizeWu, clipped_snapshot: &FinalizeElapsedClipSnapshot) {
        if let Some(create_diff_elapsed_clip_ns) = clipped_snapshot.create_queue_diff_elapsed_ns {
            self.create_diff_sums_accum
                .add((create_diff_elapsed_clip_ns, v.create_diff_base));
        }
        if let Some(apply_diff_elapsed_clip_ns) = clipped_snapshot.apply_queue_diff_elapsed_ns {
            self.apply_diff_sums_accum
                .add((apply_diff_elapsed_clip_ns, v.apply_diff_base));
        }
        if let Some(update_shard_accounts_elapsed_clip_ns) =
            clipped_snapshot.update_shard_accounts_elapsed_ns
        {
            self.update_shard_accounts_sums_accum.add((
                update_shard_accounts_elapsed_clip_ns,
                v.update_shard_accounts_base,
            ));
        }
        if let Some(build_accounts_blocks_elapsed_clip_ns) =
            clipped_snapshot.build_accounts_blocks_elapsed_ns
        {
            self.build_accounts_blocks_sums_accum.add((
                build_accounts_blocks_elapsed_clip_ns,
                v.build_accounts_blocks_base,
            ));
        }
        if let Some(build_in_msg_elapsed_clip_ns) = clipped_snapshot.build_in_msgs_elapsed_ns {
            self.build_in_msg_sums_accum
                .add((build_in_msg_elapsed_clip_ns, v.build_in_msg_base));
        }
        if let Some(build_out_msg_elapsed_clip_ns) = clipped_snapshot.build_out_msgs_elapsed_ns {
            self.build_out_msg_sums_accum
                .add((build_out_msg_elapsed_clip_ns, v.build_out_msg_base));
        }
        if let Some(build_state_update_elapsed_clip_ns) =
            clipped_snapshot.build_state_update_elapsed_ns
        {
            self.build_state_update_sums_accum.add((
                build_state_update_elapsed_clip_ns,
                v.build_state_update_base,
            ));
        }
        if let Some(build_block_elapsed_clip_ns) = clipped_snapshot.build_block_elapsed_ns {
            self.build_block_sums_accum
                .add((build_block_elapsed_clip_ns, v.build_block_base));
        }
    }

    pub fn merge_sums_accum(&mut self, other: &Self) {
        self.build_in_msg_sums_accum
            .merge(&other.build_in_msg_sums_accum);
        self.build_out_msg_sums_accum
            .merge(&other.build_out_msg_sums_accum);
        self.build_accounts_blocks_sums_accum
            .merge(&other.build_accounts_blocks_sums_accum);
        self.update_shard_accounts_sums_accum
            .merge(&other.update_shard_accounts_sums_accum);
        self.build_state_update_sums_accum
            .merge(&other.build_state_update_sums_accum);
        self.build_block_sums_accum
            .merge(&other.build_block_sums_accum);
        self.create_diff_sums_accum
            .merge(&other.create_diff_sums_accum);
        self.apply_diff_sums_accum
            .merge(&other.apply_diff_sums_accum);
    }

    pub fn get_avg(&self, last_shard_accounts_count: u64) -> Option<FinalizeWu> {
        let avg_u64 = |avg: &SafeUnsignedAvg| avg.get_avg_checked().unwrap_or_default() as u64;
        let avg_u128 = |avg: &SafeUnsignedAvg| avg.get_avg_checked().unwrap_or_default();

        Some(FinalizeWu {
            diff_msgs_count: avg_u64(&self.diff_msgs_count),
            create_diff_base: avg_u128(&self.create_diff_base),
            apply_diff_base: avg_u128(&self.apply_diff_base),

            create_queue_diff_wu: avg_u64(&self.create_queue_diff_wu),
            create_queue_diff_elapsed: Duration::from_nanos(avg_u64(
                &self.create_queue_diff_elapsed_ns,
            )),

            apply_queue_diff_wu: avg_u64(&self.apply_queue_diff_wu),
            apply_queue_diff_elapsed: Duration::from_nanos(avg_u64(
                &self.apply_queue_diff_elapsed_ns,
            )),

            shard_accounts_count: last_shard_accounts_count,
            updated_accounts_count: self
                .updated_accounts_count
                .get_avg_checked()
                .map(|v| v as u64)?,
            in_msgs_len: avg_u64(&self.in_msgs_len),
            out_msgs_len: avg_u64(&self.out_msgs_len),

            update_shard_accounts_wu: avg_u64(&self.update_shard_accounts_wu),
            update_shard_accounts_elapsed: Duration::from_nanos(avg_u64(
                &self.update_shard_accounts_elapsed_ns,
            )),
            update_shard_accounts_base: avg_u128(&self.update_shard_accounts_base),

            build_accounts_blocks_wu: avg_u64(&self.build_accounts_blocks_wu),
            build_accounts_blocks_elapsed: Duration::from_nanos(avg_u64(
                &self.build_accounts_blocks_elapsed_ns,
            )),
            build_accounts_blocks_base: avg_u128(&self.build_accounts_blocks_base),

            build_accounts_elapsed: Duration::from_nanos(avg_u64(&self.build_accounts_elapsed_ns)),

            build_in_msgs_wu: avg_u64(&self.build_in_msgs_wu),
            build_in_msgs_elapsed: Duration::from_nanos(avg_u64(&self.build_in_msgs_elapsed_ns)),
            build_in_msg_base: avg_u128(&self.build_in_msg_base),

            build_out_msgs_wu: avg_u64(&self.build_out_msgs_wu),
            build_out_msgs_elapsed: Duration::from_nanos(avg_u64(&self.build_out_msgs_elapsed_ns)),
            build_out_msg_base: avg_u128(&self.build_out_msg_base),

            build_accounts_and_messages_in_parallel_elased: Duration::from_nanos(avg_u64(
                &self.build_accounts_and_messages_in_parallel_elased_ns,
            )),

            build_state_update_wu: avg_u64(&self.build_state_update_wu),
            build_state_update_elapsed: Duration::from_nanos(avg_u64(
                &self.build_state_update_elapsed_ns,
            )),
            build_state_update_base: avg_u128(&self.build_state_update_base),

            build_block_wu: avg_u64(&self.build_block_wu),
            build_block_elapsed: Duration::from_nanos(avg_u64(&self.build_block_elapsed_ns)),
            build_block_base: avg_u128(&self.build_block_base),

            finalize_block_elapsed: Duration::from_nanos(avg_u64(&self.finalize_block_elapsed_ns)),

            total_elapsed: Duration::from_nanos(avg_u64(&self.total_elapsed_ns)),
        })
    }
}
