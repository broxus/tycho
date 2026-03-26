use tycho_util::num::RollingPercentiles;

const UNIT_COST_SCALE: u128 = 1_000_000_000;
const UNIT_COST_P_LOW: u8 = 1;
const UNIT_COST_P_HIGH: u8 = 99;

pub struct RollingUnitCostClipper {
    pct: RollingPercentiles<u128>,
}

impl RollingUnitCostClipper {
    pub fn new(window: usize) -> Self {
        Self {
            pct: RollingPercentiles::new(window),
        }
    }

    pub fn clip_elapsed_ns(&mut self, base: u128, elapsed_ns: u128) -> Option<u128> {
        if base == 0 {
            return None;
        }
        // compare samples by unit cost instead of raw elapsed time so different bases stay comparable
        let unit_cost_raw = elapsed_ns as f64 / base as f64;
        if !unit_cost_raw.is_finite() || unit_cost_raw < 0.0 {
            return None;
        }
        let unit_cost_q = quantize_unit_cost(unit_cost_raw)?;
        let unit_cost_clip_q =
            self.pct
                .push_and_clip(unit_cost_q, UNIT_COST_P_LOW, UNIT_COST_P_HIGH);
        dequantize_elapsed_ns(unit_cost_clip_q, base)
    }

    pub fn window_is_filled(&self) -> bool {
        self.pct.window_is_filled()
    }
}

pub struct PrepareUnitCostClippers {
    pub read_ext_msgs: RollingUnitCostClipper,
    pub read_existing_int_msgs: RollingUnitCostClipper,
    pub read_new_int_msgs: RollingUnitCostClipper,
    pub add_msgs_to_groups: RollingUnitCostClipper,
    pub total_elapsed: RollingUnitCostClipper,
}

impl PrepareUnitCostClippers {
    pub fn new(window: usize) -> Self {
        Self {
            read_ext_msgs: RollingUnitCostClipper::new(window),
            read_existing_int_msgs: RollingUnitCostClipper::new(window),
            read_new_int_msgs: RollingUnitCostClipper::new(window),
            add_msgs_to_groups: RollingUnitCostClipper::new(window),
            total_elapsed: RollingUnitCostClipper::new(window),
        }
    }
}

pub struct ExecuteUnitCostClippers {
    pub execute_groups_vm: RollingUnitCostClipper,
    pub process_txs: RollingUnitCostClipper,
}

impl ExecuteUnitCostClippers {
    pub fn new(window: usize) -> Self {
        Self {
            execute_groups_vm: RollingUnitCostClipper::new(window),
            process_txs: RollingUnitCostClipper::new(window),
        }
    }
}

pub struct FinalizeUnitCostClippers {
    pub create_diff: RollingUnitCostClipper,
    pub apply_diff: RollingUnitCostClipper,
    pub update_shard_accounts: RollingUnitCostClipper,
    pub build_accounts_blocks: RollingUnitCostClipper,
    pub build_accounts: RollingUnitCostClipper,
    pub build_in_msg: RollingUnitCostClipper,
    pub build_out_msg: RollingUnitCostClipper,
    pub build_accounts_and_messages_in_parallel: RollingUnitCostClipper,
    pub build_state_update: RollingUnitCostClipper,
    pub build_block: RollingUnitCostClipper,
    pub finalize_block: RollingUnitCostClipper,
    pub total_elapsed: RollingUnitCostClipper,
}

impl FinalizeUnitCostClippers {
    pub fn new(window: usize) -> Self {
        Self {
            create_diff: RollingUnitCostClipper::new(window),
            apply_diff: RollingUnitCostClipper::new(window),
            update_shard_accounts: RollingUnitCostClipper::new(window),
            build_accounts_blocks: RollingUnitCostClipper::new(window),
            build_accounts: RollingUnitCostClipper::new(window),
            build_in_msg: RollingUnitCostClipper::new(window),
            build_out_msg: RollingUnitCostClipper::new(window),
            build_accounts_and_messages_in_parallel: RollingUnitCostClipper::new(window),
            build_state_update: RollingUnitCostClipper::new(window),
            build_block: RollingUnitCostClipper::new(window),
            finalize_block: RollingUnitCostClipper::new(window),
            total_elapsed: RollingUnitCostClipper::new(window),
        }
    }
}

pub struct DoCollateUnitCostClippers {
    pub resume_collation: RollingUnitCostClipper,
    pub resume_collation_per_block: RollingUnitCostClipper,
    pub collation_total_elapsed: RollingUnitCostClipper,
}

impl DoCollateUnitCostClippers {
    pub fn new(window: usize) -> Self {
        Self {
            resume_collation: RollingUnitCostClipper::new(window),
            resume_collation_per_block: RollingUnitCostClipper::new(window),
            collation_total_elapsed: RollingUnitCostClipper::new(window),
        }
    }
}

pub struct UnitCostClippers {
    pub prepare: PrepareUnitCostClippers,
    pub execute: ExecuteUnitCostClippers,
    pub finalize: FinalizeUnitCostClippers,
    pub do_collate: DoCollateUnitCostClippers,
}

impl UnitCostClippers {
    pub fn new(window: usize) -> Self {
        Self {
            prepare: PrepareUnitCostClippers::new(window),
            execute: ExecuteUnitCostClippers::new(window),
            finalize: FinalizeUnitCostClippers::new(window),
            do_collate: DoCollateUnitCostClippers::new(window),
        }
    }

    pub fn missing_mandatory_windows(&self) -> Vec<&'static str> {
        let mut missing = Vec::new();
        if !self.prepare.add_msgs_to_groups.window_is_filled() {
            missing.push("prepare.add_msgs_to_groups");
        }
        if !self.execute.execute_groups_vm.window_is_filled() {
            missing.push("execute.execute_groups_vm");
        }
        if !self.execute.process_txs.window_is_filled() {
            missing.push("execute.process_txs");
        }
        if !self.finalize.update_shard_accounts.window_is_filled() {
            missing.push("finalize.update_shard_accounts");
        }
        if !self.finalize.build_accounts_blocks.window_is_filled() {
            missing.push("finalize.build_accounts_blocks");
        }
        if !self.finalize.build_in_msg.window_is_filled() {
            missing.push("finalize.build_in_msg");
        }
        if !self.finalize.build_out_msg.window_is_filled() {
            missing.push("finalize.build_out_msg");
        }
        if !self.do_collate.resume_collation.window_is_filled() {
            missing.push("do_collate.resume_collation");
        }
        missing
    }

    #[cfg(test)]
    pub fn mandatory_windows_filled(&self) -> bool {
        self.missing_mandatory_windows().is_empty()
    }
}

fn quantize_unit_cost(unit_cost_raw: f64) -> Option<u128> {
    // store unit cost as a scaled integer so percentile clipping stays deterministic
    let scaled = unit_cost_raw * UNIT_COST_SCALE as f64;
    if scaled < 0.0 {
        return None;
    }
    if !scaled.is_finite() {
        return Some(u128::MAX);
    }
    if scaled >= u128::MAX as f64 {
        return Some(u128::MAX);
    }
    Some(scaled.trunc() as u128)
}

fn dequantize_elapsed_ns(unit_cost_q: u128, base: u128) -> Option<u128> {
    // convert the clipped unit cost back into elapsed time for downstream averages and target params calculation
    let elapsed_ns = (unit_cost_q as f64 * base as f64) / UNIT_COST_SCALE as f64;
    if elapsed_ns < 0.0 {
        return None;
    }
    if !elapsed_ns.is_finite() {
        return Some(u128::MAX);
    }
    if elapsed_ns >= u128::MAX as f64 {
        return Some(u128::MAX);
    }
    Some(elapsed_ns.trunc() as u128)
}

#[cfg(test)]
mod tests {
    use super::{RollingUnitCostClipper, UnitCostClippers};

    #[test]
    fn clipper_returns_input_before_window_is_filled() {
        let mut clipper = RollingUnitCostClipper::new(100);
        for _ in 0..99 {
            assert_eq!(clipper.clip_elapsed_ns(1, 10), Some(10));
        }
        assert_eq!(clipper.clip_elapsed_ns(1, 100), Some(100));
    }

    #[test]
    fn clipper_clips_against_history_only_after_window_is_filled() {
        let mut clipper = RollingUnitCostClipper::new(100);
        for _ in 0..99 {
            assert_eq!(clipper.clip_elapsed_ns(1, 10), Some(10));
        }
        assert_eq!(clipper.clip_elapsed_ns(1, 100), Some(100));
        assert_eq!(clipper.clip_elapsed_ns(1, 1000), Some(10));
    }

    #[test]
    fn clipper_rejects_zero_base_and_accepts_zero_elapsed() {
        let mut clipper = RollingUnitCostClipper::new(100);
        assert_eq!(clipper.clip_elapsed_ns(0, 10), None);
        assert_eq!(clipper.clip_elapsed_ns(1, 0), Some(0));
    }

    #[test]
    fn clipper_window_is_filled_after_window_samples() {
        let mut clipper = RollingUnitCostClipper::new(100);
        assert!(!clipper.window_is_filled());
        for _ in 0..99 {
            assert_eq!(clipper.clip_elapsed_ns(1, 10), Some(10));
            assert!(!clipper.window_is_filled());
        }
        assert_eq!(clipper.clip_elapsed_ns(1, 10), Some(10));
        assert!(clipper.window_is_filled());
    }

    #[test]
    fn mandatory_windows_filled_is_false_when_mandatory_window_is_not_filled() {
        let mut clippers = UnitCostClippers::new(100);
        for _ in 0..100 {
            assert_eq!(
                clippers.prepare.add_msgs_to_groups.clip_elapsed_ns(1, 10),
                Some(10)
            );
            assert_eq!(
                clippers.execute.execute_groups_vm.clip_elapsed_ns(1, 10),
                Some(10)
            );
            assert_eq!(
                clippers.execute.process_txs.clip_elapsed_ns(1, 10),
                Some(10)
            );
            assert_eq!(
                clippers
                    .finalize
                    .update_shard_accounts
                    .clip_elapsed_ns(1, 10),
                Some(10)
            );
            assert_eq!(
                clippers
                    .finalize
                    .build_accounts_blocks
                    .clip_elapsed_ns(1, 10),
                Some(10)
            );
            assert_eq!(
                clippers.finalize.build_in_msg.clip_elapsed_ns(1, 10),
                Some(10)
            );
            assert_eq!(
                clippers.finalize.build_out_msg.clip_elapsed_ns(1, 10),
                Some(10)
            );
        }
        assert!(!clippers.mandatory_windows_filled());
        assert_eq!(clippers.missing_mandatory_windows(), vec![
            "do_collate.resume_collation"
        ]);
    }

    #[test]
    fn mandatory_windows_filled_is_true_when_only_excluded_windows_are_not_filled() {
        let mut clippers = UnitCostClippers::new(100);
        for _ in 0..100 {
            assert_eq!(
                clippers.prepare.add_msgs_to_groups.clip_elapsed_ns(1, 10),
                Some(10)
            );
            assert_eq!(
                clippers.execute.execute_groups_vm.clip_elapsed_ns(1, 10),
                Some(10)
            );
            assert_eq!(
                clippers.execute.process_txs.clip_elapsed_ns(1, 10),
                Some(10)
            );
            assert_eq!(
                clippers
                    .finalize
                    .update_shard_accounts
                    .clip_elapsed_ns(1, 10),
                Some(10)
            );
            assert_eq!(
                clippers
                    .finalize
                    .build_accounts_blocks
                    .clip_elapsed_ns(1, 10),
                Some(10)
            );
            assert_eq!(
                clippers.finalize.build_in_msg.clip_elapsed_ns(1, 10),
                Some(10)
            );
            assert_eq!(
                clippers.finalize.build_out_msg.clip_elapsed_ns(1, 10),
                Some(10)
            );
            assert_eq!(
                clippers.do_collate.resume_collation.clip_elapsed_ns(1, 10),
                Some(10)
            );
        }
        assert!(clippers.mandatory_windows_filled());
        assert!(clippers.missing_mandatory_windows().is_empty());
        assert!(!clippers.prepare.read_ext_msgs.window_is_filled());
        assert!(!clippers.prepare.read_existing_int_msgs.window_is_filled());
        assert!(!clippers.prepare.read_new_int_msgs.window_is_filled());
        assert!(!clippers.prepare.total_elapsed.window_is_filled());
        assert!(!clippers.finalize.create_diff.window_is_filled());
        assert!(!clippers.finalize.apply_diff.window_is_filled());
        assert!(!clippers.finalize.build_accounts.window_is_filled());
        assert!(
            !clippers
                .finalize
                .build_accounts_and_messages_in_parallel
                .window_is_filled()
        );
        assert!(!clippers.finalize.build_state_update.window_is_filled());
        assert!(!clippers.finalize.build_block.window_is_filled());
        assert!(!clippers.finalize.finalize_block.window_is_filled());
        assert!(!clippers.finalize.total_elapsed.window_is_filled());
        assert!(
            !clippers
                .do_collate
                .resume_collation_per_block
                .window_is_filled()
        );
        assert!(
            !clippers
                .do_collate
                .collation_total_elapsed
                .window_is_filled()
        );
    }
}
