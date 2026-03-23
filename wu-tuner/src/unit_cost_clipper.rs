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
        let unit_cost_raw = elapsed_ns as f64 / base as f64;
        if !unit_cost_raw.is_finite() || unit_cost_raw < 0.0 {
            return None;
        }
        let unit_cost_q = quantize_unit_cost(unit_cost_raw)?;
        let unit_cost_clip_q = self
            .pct
            .push_and_clip(unit_cost_q, UNIT_COST_P_LOW, UNIT_COST_P_HIGH);
        dequantize_elapsed_ns(unit_cost_clip_q, base)
    }
}

pub struct PrepareUnitCostClippers {
    pub read_ext_msgs: RollingUnitCostClipper,
    pub read_existing_int_msgs: RollingUnitCostClipper,
    pub read_new_int_msgs: RollingUnitCostClipper,
    pub add_msgs_to_groups: RollingUnitCostClipper,
}

impl PrepareUnitCostClippers {
    pub fn new(window: usize) -> Self {
        Self {
            read_ext_msgs: RollingUnitCostClipper::new(window),
            read_existing_int_msgs: RollingUnitCostClipper::new(window),
            read_new_int_msgs: RollingUnitCostClipper::new(window),
            add_msgs_to_groups: RollingUnitCostClipper::new(window),
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
    pub build_in_msg: RollingUnitCostClipper,
    pub build_out_msg: RollingUnitCostClipper,
    pub build_state_update: RollingUnitCostClipper,
    pub build_block: RollingUnitCostClipper,
}

impl FinalizeUnitCostClippers {
    pub fn new(window: usize) -> Self {
        Self {
            create_diff: RollingUnitCostClipper::new(window),
            apply_diff: RollingUnitCostClipper::new(window),
            update_shard_accounts: RollingUnitCostClipper::new(window),
            build_accounts_blocks: RollingUnitCostClipper::new(window),
            build_in_msg: RollingUnitCostClipper::new(window),
            build_out_msg: RollingUnitCostClipper::new(window),
            build_state_update: RollingUnitCostClipper::new(window),
            build_block: RollingUnitCostClipper::new(window),
        }
    }
}

pub struct DoCollateUnitCostClippers {
    pub resume_collation: RollingUnitCostClipper,
}

impl DoCollateUnitCostClippers {
    pub fn new(window: usize) -> Self {
        Self {
            resume_collation: RollingUnitCostClipper::new(window),
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
}

fn quantize_unit_cost(unit_cost_raw: f64) -> Option<u128> {
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
    use super::RollingUnitCostClipper;

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
}
