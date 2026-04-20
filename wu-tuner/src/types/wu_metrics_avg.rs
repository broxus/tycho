use tycho_types::models::WorkUnitsParams;

use super::do_collate_wu_avg::{DoCollateElapsedClipSnapshot, DoCollateWuAvg};
use super::execute_wu_avg::{ExecuteElapsedClipSnapshot, ExecuteWuAvg};
use super::finalize_wu_avg::{FinalizeElapsedClipSnapshot, FinalizeWuAvg};
use super::prepare_wu_avg::{PrepareElapsedClipSnapshot, PrepareMsgGroupsWuAvg};
use crate::WuMetrics;
use crate::unit_cost_clipper::UnitCostClippers;

pub struct WuMetricsAvg {
    pub last_wu_params: WorkUnitsParams,
    pub last_shard_accounts_count: u64,
    pub had_pending_messages: bool,
    pub prepare: PrepareMsgGroupsWuAvg,
    pub execute: ExecuteWuAvg,
    pub finalize: FinalizeWuAvg,
    pub do_collate: DoCollateWuAvg,
}

impl WuMetricsAvg {
    pub fn new() -> Self {
        Self {
            last_wu_params: Default::default(),
            last_shard_accounts_count: 0,
            had_pending_messages: true,
            prepare: PrepareMsgGroupsWuAvg::default(),
            execute: ExecuteWuAvg::default(),
            finalize: FinalizeWuAvg::default(),
            do_collate: DoCollateWuAvg::default(),
        }
    }

    pub fn accum(&mut self, v: &WuMetrics, unit_cost_clippers: &mut UnitCostClippers) {
        self.last_wu_params = v.wu_params.clone();
        self.had_pending_messages = self.had_pending_messages && v.has_pending_messages;
        self.last_shard_accounts_count = v.wu_on_finalize.shard_accounts_count;
        let state_update_min_wu = v.wu_params.finalize.state_update_min as u64;
        let serialize_min_wu = v.wu_params.finalize.serialize_min as u64;
        // reuse the latest min params while other parts are accumulated through clipped sums
        self.prepare
            .accum(&v.wu_on_prepare_msg_groups, unit_cost_clippers);
        self.execute.accum(&v.wu_on_execute, unit_cost_clippers);
        self.finalize.accum(
            &v.wu_on_finalize,
            state_update_min_wu,
            serialize_min_wu,
            unit_cost_clippers,
        );
        self.do_collate.accum(
            &v.wu_on_do_collate,
            v.collation_total_wu(),
            unit_cost_clippers,
        );
    }

    pub fn accum_materialized_avg_result(&mut self, v: &WuMetrics) {
        self.last_wu_params = v.wu_params.clone();
        self.had_pending_messages = self.had_pending_messages && v.has_pending_messages;
        self.last_shard_accounts_count = v.wu_on_finalize.shard_accounts_count;
        // replay the already materialized span average without re-clipping it during MA merges
        let prepare_clipped_snapshot = PrepareElapsedClipSnapshot::default();
        let execute_clipped_snapshot = ExecuteElapsedClipSnapshot::default();
        let finalize_clipped_snapshot = FinalizeElapsedClipSnapshot::default();
        let do_collate_clipped_snapshot = DoCollateElapsedClipSnapshot::default();
        self.prepare
            .accum_avg_result(&v.wu_on_prepare_msg_groups, &prepare_clipped_snapshot);
        self.execute
            .accum_avg_result(&v.wu_on_execute, &execute_clipped_snapshot);
        self.finalize
            .accum_avg_result(&v.wu_on_finalize, &finalize_clipped_snapshot);
        self.do_collate
            .accum_avg_result(&v.wu_on_do_collate, &do_collate_clipped_snapshot);
    }

    fn merge_sums_accum(&mut self, other: &Self) {
        self.prepare.merge_sums_accum(&other.prepare);
        self.execute.merge_sums_accum(&other.execute);
        self.finalize.merge_sums_accum(&other.finalize);
        self.do_collate.merge_sums_accum(&other.do_collate);
    }

    pub fn merge_span_avg(&mut self, other: &WuMetricsAvg) -> Option<()> {
        // combine both span representations: raw clipped sums and the materialized averages
        let span_result = other.get_avg()?;
        self.merge_sums_accum(other);
        self.accum_materialized_avg_result(&span_result);
        Some(())
    }

    pub fn get_avg(&self) -> Option<WuMetrics> {
        let wu_on_prepare_msg_groups = self.prepare.get_avg();
        let wu_on_finalize = self.finalize.get_avg(self.last_shard_accounts_count)?;
        let wu_on_execute = self
            .execute
            .get_avg(wu_on_finalize.in_msgs_len, wu_on_finalize.out_msgs_len);
        let wu_on_do_collate = self.do_collate.get_avg(
            self.last_shard_accounts_count,
            wu_on_finalize.updated_accounts_count,
        );

        Some(WuMetrics {
            wu_params: self.last_wu_params.clone(),
            wu_on_prepare_msg_groups,
            wu_on_execute,
            wu_on_finalize,
            wu_on_do_collate,
            has_pending_messages: self.had_pending_messages,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::future;
    use std::future::Future;
    use std::sync::Arc;
    use std::time::Duration;

    use anyhow::Result;

    use super::*;
    use crate::config::WuTunerConfig;
    use crate::tuner::{WuMetricsSpan, WuTuner, safe_metrics_avg};
    use crate::updater::WuParamsUpdater;

    struct TestUpdater;

    impl WuParamsUpdater for TestUpdater {
        fn update_wu_params(
            &self,
            _config: Arc<WuTunerConfig>,
            _target_wu_params: WorkUnitsParams,
        ) -> impl Future<Output = Result<()>> + Send {
            future::ready(Ok(()))
        }
    }

    fn test_unit_cost_clippers() -> UnitCostClippers {
        test_unit_cost_clippers_with_window(100)
    }

    fn test_unit_cost_clippers_with_window(window: usize) -> UnitCostClippers {
        UnitCostClippers::new(window)
    }

    fn calc_target_params(target_wu_price: f64, wu_metrics_avg: &WuMetricsAvg) -> WorkUnitsParams {
        let wu_metrics = wu_metrics_avg.get_avg().expect("average should exist");
        WuTuner::<TestUpdater>::calculate_target_wu_params(
            target_wu_price,
            wu_metrics_avg,
            &wu_metrics,
        )
    }

    fn roundtrip_elapsed_ns(elapsed_ns: u128, base: u128) -> u128 {
        ((elapsed_ns * 1_000_000_000) / base * base) / 1_000_000_000
    }

    fn sample_metrics(
        base: u64,
        has_pending_messages: bool,
        execute_prepare: u32,
        shard_accounts_count: u64,
    ) -> WuMetrics {
        let mut metrics = WuMetrics::default();
        metrics.wu_params.execute.prepare = execute_prepare;
        metrics.has_pending_messages = has_pending_messages;
        metrics.wu_on_prepare_msg_groups.fixed_part = base + 1;
        metrics.wu_on_prepare_msg_groups.read_ext_msgs_count = base + 2;
        metrics.wu_on_prepare_msg_groups.read_ext_msgs_wu = base + 3;
        metrics.wu_on_prepare_msg_groups.read_ext_msgs_elapsed = Duration::from_nanos(base + 4);
        metrics.wu_on_prepare_msg_groups.read_ext_msgs_base = (base + 2) as u128;
        metrics
            .wu_on_prepare_msg_groups
            .read_existing_int_msgs_count = base + 5;
        metrics.wu_on_prepare_msg_groups.read_existing_int_msgs_wu = base + 6;
        metrics
            .wu_on_prepare_msg_groups
            .read_existing_int_msgs_elapsed = Duration::from_nanos(base + 7);
        metrics.wu_on_prepare_msg_groups.read_existing_int_msgs_base = (base + 5) as u128;
        metrics.wu_on_prepare_msg_groups.read_new_int_msgs_count = base + 8;
        metrics.wu_on_prepare_msg_groups.read_new_int_msgs_wu = base + 9;
        metrics.wu_on_prepare_msg_groups.read_new_int_msgs_elapsed =
            Duration::from_nanos(base + 10);
        metrics.wu_on_prepare_msg_groups.read_new_int_msgs_base = (base + 8) as u128;
        metrics
            .wu_on_prepare_msg_groups
            .add_to_msgs_groups_ops_count = base + 11;
        metrics.wu_on_prepare_msg_groups.add_msgs_to_groups_wu = base + 12;
        metrics.wu_on_prepare_msg_groups.add_msgs_to_groups_elapsed =
            Duration::from_nanos(base + 13);
        metrics.wu_on_prepare_msg_groups.add_msgs_to_groups_base = (base + 11) as u128;
        metrics.wu_on_prepare_msg_groups.total_elapsed = Duration::from_nanos(base + 14);
        metrics.wu_on_execute.in_msgs_len = base + 200;
        metrics.wu_on_execute.out_msgs_len = base + 201;
        metrics.wu_on_execute.inserted_new_msgs_count = base + 15;
        metrics.wu_on_execute.groups_count = base + 16;
        metrics
            .wu_on_execute
            .execute_groups_vm_sum_accounts_over_threads = (base as u128) + 5000;
        metrics.wu_on_execute.execute_groups_vm_sum_gas_over_threads = (base as u128) + 6000;
        metrics.wu_on_execute.process_txs_base = (base as u128) + 7000;
        metrics.wu_on_execute.execute_groups_vm_only_wu = base + 19;
        metrics.wu_on_execute.execute_groups_vm_only_elapsed = Duration::from_nanos(base + 20);
        metrics.wu_on_execute.process_txs_wu = base + 21;
        metrics.wu_on_execute.process_txs_elapsed = Duration::from_nanos(base + 22);
        metrics.wu_on_finalize.diff_msgs_count = base + 23;
        metrics.wu_on_finalize.create_queue_diff_wu = base + 24;
        metrics.wu_on_finalize.create_queue_diff_elapsed = Duration::from_nanos(base + 25);
        metrics.wu_on_finalize.apply_queue_diff_wu = base + 26;
        metrics.wu_on_finalize.apply_queue_diff_elapsed = Duration::from_nanos(base + 27);
        metrics.wu_on_finalize.create_diff_base = (base as u128) + 80;
        metrics.wu_on_finalize.apply_diff_base = (base as u128) + 81;
        metrics.wu_on_finalize.shard_accounts_count = shard_accounts_count;
        metrics.wu_on_finalize.updated_accounts_count = base + 28;
        metrics.wu_on_finalize.in_msgs_len = base + 29;
        metrics.wu_on_finalize.out_msgs_len = base + 30;
        metrics.wu_on_finalize.update_shard_accounts_wu = base + 31;
        metrics.wu_on_finalize.update_shard_accounts_elapsed = Duration::from_nanos(base + 32);
        metrics.wu_on_finalize.update_shard_accounts_base = (base as u128) + 82;
        metrics.wu_on_finalize.build_accounts_blocks_wu = base + 33;
        metrics.wu_on_finalize.build_accounts_blocks_elapsed = Duration::from_nanos(base + 34);
        metrics.wu_on_finalize.build_accounts_blocks_base = (base as u128) + 83;
        metrics.wu_on_finalize.build_accounts_elapsed = Duration::from_nanos(base + 35);
        metrics.wu_on_finalize.build_in_msgs_wu = base + 36;
        metrics.wu_on_finalize.build_in_msgs_elapsed = Duration::from_nanos(base + 37);
        metrics.wu_on_finalize.build_in_msg_base = (base as u128) + 84;
        metrics.wu_on_finalize.build_out_msgs_wu = base + 38;
        metrics.wu_on_finalize.build_out_msgs_elapsed = Duration::from_nanos(base + 39);
        metrics.wu_on_finalize.build_out_msg_base = (base as u128) + 85;
        metrics
            .wu_on_finalize
            .build_accounts_and_messages_in_parallel_elased = Duration::from_nanos(base + 40);
        metrics.wu_on_finalize.build_state_update_wu = base + 41;
        metrics.wu_on_finalize.build_state_update_elapsed = Duration::from_nanos(base + 42);
        metrics.wu_on_finalize.build_state_update_base = (base as u128) + 86;
        metrics.wu_on_finalize.build_block_wu = base + 43;
        metrics.wu_on_finalize.build_block_elapsed = Duration::from_nanos(base + 44);
        metrics.wu_on_finalize.build_block_base = (base as u128) + 87;
        metrics.wu_on_finalize.finalize_block_elapsed = Duration::from_nanos(base + 45);
        metrics.wu_on_finalize.total_elapsed = Duration::from_nanos(base + 46);
        metrics.wu_on_do_collate.resume_collation_wu = base + 47;
        metrics.wu_on_do_collate.resume_collation_elapsed = Duration::from_nanos(base + 48);
        metrics.wu_on_do_collate.resume_collation_base = (base as u128) + 88;
        metrics.wu_on_do_collate.resume_collation_wu_per_block = base + 49;
        metrics
            .wu_on_do_collate
            .resume_collation_elapsed_per_block_ns = (base as u128) + 6000;
        metrics.wu_on_do_collate.collation_total_elapsed = Duration::from_nanos(base + 50);
        metrics
    }

    fn metrics_with_read_ext_elapsed(elapsed_ns: u64, base: u128) -> WuMetrics {
        let mut metrics = WuMetrics::default();
        metrics.wu_on_prepare_msg_groups.read_ext_msgs_base = base;
        metrics.wu_on_prepare_msg_groups.read_ext_msgs_elapsed = Duration::from_nanos(elapsed_ns);
        metrics.wu_on_finalize.updated_accounts_count = 1;
        metrics.wu_on_finalize.shard_accounts_count = 1;
        metrics
    }

    fn metrics_with_derived_elapsed(
        prepare_total_elapsed_ns: u64,
        build_accounts_elapsed_ns: u64,
        build_accounts_and_messages_in_parallel_elased_ns: u64,
        finalize_block_elapsed_ns: u64,
        finalize_total_elapsed_ns: u64,
        resume_collation_elapsed_per_block_ns: u128,
        collation_total_elapsed_ns: u64,
    ) -> WuMetrics {
        let mut metrics = sample_metrics(10, true, 11, 1000);
        metrics.wu_on_prepare_msg_groups.total_elapsed =
            Duration::from_nanos(prepare_total_elapsed_ns);
        metrics.wu_on_finalize.build_accounts_elapsed =
            Duration::from_nanos(build_accounts_elapsed_ns);
        metrics
            .wu_on_finalize
            .build_accounts_and_messages_in_parallel_elased =
            Duration::from_nanos(build_accounts_and_messages_in_parallel_elased_ns);
        metrics.wu_on_finalize.finalize_block_elapsed =
            Duration::from_nanos(finalize_block_elapsed_ns);
        metrics.wu_on_finalize.total_elapsed = Duration::from_nanos(finalize_total_elapsed_ns);
        metrics
            .wu_on_do_collate
            .resume_collation_elapsed_per_block_ns = resume_collation_elapsed_per_block_ns;
        metrics.wu_on_do_collate.collation_total_elapsed =
            Duration::from_nanos(collation_total_elapsed_ns);
        metrics
    }

    fn metrics_with_derived_elapsed_and_zero_clip_bases(
        prepare_total_elapsed_ns: u64,
        build_accounts_elapsed_ns: u64,
        build_accounts_and_messages_in_parallel_elased_ns: u64,
        finalize_block_elapsed_ns: u64,
        finalize_total_elapsed_ns: u64,
        resume_collation_elapsed_per_block_ns: u128,
        collation_total_elapsed_ns: u64,
    ) -> WuMetrics {
        let mut metrics = WuMetrics::default();
        metrics.wu_on_prepare_msg_groups.total_elapsed =
            Duration::from_nanos(prepare_total_elapsed_ns);
        metrics.wu_on_finalize.build_accounts_elapsed =
            Duration::from_nanos(build_accounts_elapsed_ns);
        metrics
            .wu_on_finalize
            .build_accounts_and_messages_in_parallel_elased =
            Duration::from_nanos(build_accounts_and_messages_in_parallel_elased_ns);
        metrics.wu_on_finalize.finalize_block_elapsed =
            Duration::from_nanos(finalize_block_elapsed_ns);
        metrics.wu_on_finalize.total_elapsed = Duration::from_nanos(finalize_total_elapsed_ns);
        metrics.wu_on_finalize.updated_accounts_count = 1;
        metrics.wu_on_finalize.shard_accounts_count = 1;
        metrics
            .wu_on_do_collate
            .resume_collation_elapsed_per_block_ns = resume_collation_elapsed_per_block_ns;
        metrics.wu_on_do_collate.collation_total_elapsed =
            Duration::from_nanos(collation_total_elapsed_ns);
        metrics
    }

    #[test]
    fn wu_metrics_avg_accumulates_fields() {
        let mut avg = WuMetricsAvg::new();
        let mut unit_cost_clippers = test_unit_cost_clippers();
        let first = sample_metrics(10, true, 11, 1000);
        let second = sample_metrics(30, false, 22, 2000);
        avg.accum(&first, &mut unit_cost_clippers);
        avg.accum(&second, &mut unit_cost_clippers);
        assert_eq!(
            avg.finalize.updated_accounts_count.get_avg_checked(),
            Some(
                (first.wu_on_finalize.updated_accounts_count
                    + second.wu_on_finalize.updated_accounts_count) as u128
                    / 2,
            )
        );
        assert_eq!(
            avg.prepare.total_elapsed_ns.get_avg_checked(),
            Some(
                (roundtrip_elapsed_ns(
                    first.wu_on_prepare_msg_groups.total_elapsed.as_nanos(),
                    first.wu_on_prepare_msg_groups.total_wu() as u128,
                ) + roundtrip_elapsed_ns(
                    second.wu_on_prepare_msg_groups.total_elapsed.as_nanos(),
                    second.wu_on_prepare_msg_groups.total_wu() as u128,
                )) / 2,
            ),
        );
        assert_eq!(
            avg.execute
                .execute_groups_vm_sum_gas_over_threads
                .get_avg_checked(),
            Some(
                (first.wu_on_execute.execute_groups_vm_sum_gas_over_threads
                    + second.wu_on_execute.execute_groups_vm_sum_gas_over_threads)
                    / 2,
            ),
        );
        assert_eq!(
            avg.finalize
                .build_accounts_and_messages_in_parallel_elased_ns
                .get_avg_checked(),
            Some(
                (roundtrip_elapsed_ns(
                    first
                        .wu_on_finalize
                        .build_accounts_and_messages_in_parallel_elased
                        .as_nanos(),
                    first.wu_on_finalize.max_accounts_in_out_msgs_wu() as u128,
                ) + roundtrip_elapsed_ns(
                    second
                        .wu_on_finalize
                        .build_accounts_and_messages_in_parallel_elased
                        .as_nanos(),
                    second.wu_on_finalize.max_accounts_in_out_msgs_wu() as u128,
                )) / 2,
            ),
        );
        assert_eq!(
            avg.do_collate
                .resume_collation_elapsed_per_block_ns
                .get_avg_checked(),
            Some(
                (roundtrip_elapsed_ns(
                    first.wu_on_do_collate.resume_collation_elapsed_per_block_ns,
                    first.wu_on_do_collate.resume_collation_wu_per_block as u128,
                ) + roundtrip_elapsed_ns(
                    second
                        .wu_on_do_collate
                        .resume_collation_elapsed_per_block_ns,
                    second.wu_on_do_collate.resume_collation_wu_per_block as u128,
                )) / 2,
            ),
        );
    }

    #[test]
    fn wu_metrics_avg_get_avg_preserves_mapping() {
        let mut avg = WuMetricsAvg::new();
        let mut unit_cost_clippers = test_unit_cost_clippers();
        let first = sample_metrics(10, true, 11, 1000);
        let second = sample_metrics(30, false, 22, 2000);
        avg.accum(&first, &mut unit_cost_clippers);
        avg.accum(&second, &mut unit_cost_clippers);
        let result = avg.get_avg().expect("average should exist");
        assert_eq!(
            result.wu_params.execute.prepare,
            second.wu_params.execute.prepare
        );
        assert_eq!(result.has_pending_messages, second.has_pending_messages);
        assert_eq!(
            result.wu_on_finalize.shard_accounts_count,
            second.wu_on_finalize.shard_accounts_count
        );
        assert_eq!(
            result.wu_on_finalize.updated_accounts_count,
            (first.wu_on_finalize.updated_accounts_count
                + second.wu_on_finalize.updated_accounts_count)
                / 2
        );
        assert_eq!(
            result.wu_on_finalize.in_msgs_len,
            (first.wu_on_finalize.in_msgs_len + second.wu_on_finalize.in_msgs_len) / 2
        );
        assert_eq!(
            result.wu_on_finalize.out_msgs_len,
            (first.wu_on_finalize.out_msgs_len + second.wu_on_finalize.out_msgs_len) / 2
        );
        assert_eq!(
            result.wu_on_execute.in_msgs_len,
            (first.wu_on_finalize.in_msgs_len + second.wu_on_finalize.in_msgs_len) / 2
        );
        assert_eq!(
            result.wu_on_execute.out_msgs_len,
            (first.wu_on_finalize.out_msgs_len + second.wu_on_finalize.out_msgs_len) / 2
        );
        assert_eq!(
            result.wu_on_execute.inserted_new_msgs_count,
            (first.wu_on_execute.inserted_new_msgs_count
                + second.wu_on_execute.inserted_new_msgs_count)
                / 2
        );
        assert_eq!(
            result.wu_on_prepare_msg_groups.fixed_part,
            (first.wu_on_prepare_msg_groups.fixed_part
                + second.wu_on_prepare_msg_groups.fixed_part)
                / 2
        );
        assert_eq!(
            result.wu_on_execute.execute_groups_vm_sum_gas_over_threads,
            (first.wu_on_execute.execute_groups_vm_sum_gas_over_threads
                + second.wu_on_execute.execute_groups_vm_sum_gas_over_threads)
                / 2
        );
        assert_eq!(
            result.wu_on_execute.process_txs_base,
            (first.wu_on_execute.process_txs_base + second.wu_on_execute.process_txs_base) / 2
        );
        assert_eq!(
            result
                .wu_on_do_collate
                .resume_collation_elapsed_per_block_ns,
            (roundtrip_elapsed_ns(
                first.wu_on_do_collate.resume_collation_elapsed_per_block_ns,
                first.wu_on_do_collate.resume_collation_wu_per_block as u128,
            ) + roundtrip_elapsed_ns(
                second
                    .wu_on_do_collate
                    .resume_collation_elapsed_per_block_ns,
                second.wu_on_do_collate.resume_collation_wu_per_block as u128,
            )) / 2,
        );
        assert_eq!(
            result.wu_on_finalize.total_elapsed.as_nanos(),
            (roundtrip_elapsed_ns(
                first.wu_on_finalize.total_elapsed.as_nanos(),
                first.wu_on_finalize.total_wu() as u128,
            ) + roundtrip_elapsed_ns(
                second.wu_on_finalize.total_elapsed.as_nanos(),
                second.wu_on_finalize.total_wu() as u128,
            )) / 2,
        );
        assert_eq!(
            result.wu_on_do_collate.collation_total_elapsed.as_nanos(),
            (roundtrip_elapsed_ns(
                first.wu_on_do_collate.collation_total_elapsed.as_nanos(),
                first.collation_total_wu() as u128,
            ) + roundtrip_elapsed_ns(
                second.wu_on_do_collate.collation_total_elapsed.as_nanos(),
                second.collation_total_wu() as u128,
            )) / 2
        );
    }

    #[test]
    fn wu_metrics_avg_get_avg_uses_clipped_elapsed_for_clip_enabled_metrics() {
        let mut avg = WuMetricsAvg::new();
        let mut unit_cost_clippers = test_unit_cost_clippers();
        for _ in 0..100 {
            avg.accum(
                &metrics_with_read_ext_elapsed(10, 10),
                &mut unit_cost_clippers,
            );
        }
        avg.accum(
            &metrics_with_read_ext_elapsed(1_000, 10),
            &mut unit_cost_clippers,
        );
        let result = avg.get_avg().expect("average should exist");
        assert_eq!(
            result
                .wu_on_prepare_msg_groups
                .read_ext_msgs_elapsed
                .as_nanos(),
            10
        );
        assert_eq!(*avg.prepare.read_ext_msgs_sums_accum, (1010, 1010));
    }

    #[test]
    fn wu_metrics_avg_get_avg_falls_back_to_raw_when_clip_unavailable() {
        let mut avg = WuMetricsAvg::new();
        let mut unit_cost_clippers = test_unit_cost_clippers_with_window(2);
        avg.accum(
            &metrics_with_read_ext_elapsed(100, 0),
            &mut unit_cost_clippers,
        );
        avg.accum(
            &metrics_with_read_ext_elapsed(200, 0),
            &mut unit_cost_clippers,
        );
        let result = avg.get_avg().expect("average should exist");
        assert_eq!(
            result
                .wu_on_prepare_msg_groups
                .read_ext_msgs_elapsed
                .as_nanos(),
            150
        );
        assert_eq!(*avg.prepare.read_ext_msgs_sums_accum, (0, 0));
    }

    #[test]
    fn safe_metrics_avg_merges_span_result_and_sums() {
        let mut spans = BTreeMap::new();
        let mut unit_cost_clippers = test_unit_cost_clippers();
        let mut span1 = WuMetricsSpan::new(
            Box::new(sample_metrics(10, true, 11, 1000)),
            &mut unit_cost_clippers,
        );
        span1
            .accum(
                Box::new(sample_metrics(12, true, 11, 1000)),
                &mut unit_cost_clippers,
            )
            .expect("span accum should succeed");
        let mut span2 = WuMetricsSpan::new(
            Box::new(sample_metrics(20, true, 11, 1000)),
            &mut unit_cost_clippers,
        );
        span2
            .accum(
                Box::new(sample_metrics(22, true, 11, 1000)),
                &mut unit_cost_clippers,
            )
            .expect("span accum should succeed");
        spans.insert(1u32, span1);
        spans.insert(2u32, span2);

        let avg_range =
            spans.range_mut((std::ops::Bound::Included(0), std::ops::Bound::Excluded(3)));
        let avg = safe_metrics_avg(avg_range).expect("average should exist");
        let avg_metrics = avg.get_avg().expect("average result should exist");

        assert_eq!(avg_metrics.wu_on_prepare_msg_groups.fixed_part, 17);
        assert_eq!(*avg.prepare.read_ext_msgs_sums_accum, (76, 72));
    }

    #[test]
    fn merge_span_avg_preserves_clipped_elapsed_values() {
        let mut span_avg = WuMetricsAvg::new();
        let mut unit_cost_clippers = test_unit_cost_clippers();
        for _ in 0..100 {
            span_avg.accum(
                &metrics_with_read_ext_elapsed(10, 10),
                &mut unit_cost_clippers,
            );
        }
        span_avg.accum(
            &metrics_with_read_ext_elapsed(1_000, 10),
            &mut unit_cost_clippers,
        );

        let mut merged = WuMetricsAvg::new();
        merged
            .merge_span_avg(&span_avg)
            .expect("span merge should succeed");

        let result = merged.get_avg().expect("average should exist");
        assert_eq!(
            result
                .wu_on_prepare_msg_groups
                .read_ext_msgs_elapsed
                .as_nanos(),
            10
        );
        assert_eq!(*merged.prepare.read_ext_msgs_sums_accum, (1010, 1010));
    }

    #[test]
    fn wu_metrics_avg_clips_remaining_derived_elapsed_metrics() {
        let mut avg = WuMetricsAvg::new();
        let mut unit_cost_clippers = test_unit_cost_clippers();
        let baseline = metrics_with_derived_elapsed(100, 110, 120, 130, 140, 150, 160);
        let outlier =
            metrics_with_derived_elapsed(10_000, 11_000, 12_000, 13_000, 14_000, 15_000, 16_000);

        for _ in 0..100 {
            avg.accum(&baseline, &mut unit_cost_clippers);
        }
        avg.accum(&outlier, &mut unit_cost_clippers);

        let result = avg.get_avg().expect("average should exist");
        let expected_prepare_total = roundtrip_elapsed_ns(
            baseline.wu_on_prepare_msg_groups.total_elapsed.as_nanos(),
            baseline.wu_on_prepare_msg_groups.total_wu() as u128,
        );
        let expected_build_accounts = roundtrip_elapsed_ns(
            baseline.wu_on_finalize.build_accounts_elapsed.as_nanos(),
            baseline.wu_on_finalize.build_accounts_wu() as u128,
        );
        let expected_parallel = roundtrip_elapsed_ns(
            baseline
                .wu_on_finalize
                .build_accounts_and_messages_in_parallel_elased
                .as_nanos(),
            baseline.wu_on_finalize.max_accounts_in_out_msgs_wu() as u128,
        );
        let expected_finalize_block = roundtrip_elapsed_ns(
            baseline.wu_on_finalize.finalize_block_elapsed.as_nanos(),
            baseline.wu_on_finalize.finalize_block_wu() as u128,
        );
        let expected_finalize_total = roundtrip_elapsed_ns(
            baseline.wu_on_finalize.total_elapsed.as_nanos(),
            baseline.wu_on_finalize.total_wu() as u128,
        );
        let expected_resume_per_block = roundtrip_elapsed_ns(
            baseline
                .wu_on_do_collate
                .resume_collation_elapsed_per_block_ns,
            baseline.wu_on_do_collate.resume_collation_wu_per_block as u128,
        );
        let expected_collation_total = roundtrip_elapsed_ns(
            baseline.wu_on_do_collate.collation_total_elapsed.as_nanos(),
            baseline.collation_total_wu() as u128,
        );
        assert_eq!(
            result.wu_on_prepare_msg_groups.total_elapsed.as_nanos(),
            expected_prepare_total
        );
        assert_eq!(
            result.wu_on_finalize.build_accounts_elapsed.as_nanos(),
            expected_build_accounts
        );
        assert_eq!(
            result
                .wu_on_finalize
                .build_accounts_and_messages_in_parallel_elased
                .as_nanos(),
            expected_parallel,
        );
        assert_eq!(
            result.wu_on_finalize.finalize_block_elapsed.as_nanos(),
            expected_finalize_block
        );
        assert_eq!(
            result.wu_on_finalize.total_elapsed.as_nanos(),
            expected_finalize_total
        );
        assert_eq!(
            result
                .wu_on_do_collate
                .resume_collation_elapsed_per_block_ns,
            expected_resume_per_block
        );
        assert_eq!(
            result.wu_on_do_collate.collation_total_elapsed.as_nanos(),
            expected_collation_total
        );
        let expected_total_price =
            expected_collation_total as f64 / baseline.collation_total_wu() as f64;
        assert!((result.collation_total_wu_price() - expected_total_price).abs() < f64::EPSILON);
    }

    #[test]
    fn wu_metrics_avg_derived_elapsed_falls_back_to_raw_when_clip_unavailable() {
        let mut avg = WuMetricsAvg::new();
        let mut unit_cost_clippers = test_unit_cost_clippers_with_window(2);
        let first =
            metrics_with_derived_elapsed_and_zero_clip_bases(100, 110, 120, 130, 140, 150, 160);
        let second =
            metrics_with_derived_elapsed_and_zero_clip_bases(300, 310, 320, 330, 340, 350, 360);

        avg.accum(&first, &mut unit_cost_clippers);
        avg.accum(&second, &mut unit_cost_clippers);

        let result = avg.get_avg().expect("average should exist");
        assert_eq!(
            result.wu_on_prepare_msg_groups.total_elapsed.as_nanos(),
            200
        );
        assert_eq!(result.wu_on_finalize.build_accounts_elapsed.as_nanos(), 210);
        assert_eq!(
            result
                .wu_on_finalize
                .build_accounts_and_messages_in_parallel_elased
                .as_nanos(),
            220,
        );
        assert_eq!(result.wu_on_finalize.finalize_block_elapsed.as_nanos(), 230);
        assert_eq!(result.wu_on_finalize.total_elapsed.as_nanos(), 240);
        assert_eq!(
            result
                .wu_on_do_collate
                .resume_collation_elapsed_per_block_ns,
            250
        );
        assert_eq!(
            result.wu_on_do_collate.collation_total_elapsed.as_nanos(),
            260
        );
        assert_eq!(result.collation_total_wu_price(), 0.0);
    }

    #[test]
    fn calculate_target_wu_params_saturates_and_falls_back() {
        let mut metrics = WuMetrics::default();
        metrics.wu_params.prepare.read_ext_msgs = 10;
        metrics.wu_params.prepare.read_int_msgs = 123;
        let mut avg = WuMetricsAvg::new();
        avg.accum_materialized_avg_result(&metrics);
        avg.prepare
            .read_ext_msgs_sums_accum
            .add((((u16::MAX as u128) + 100) * 2, 1));
        let target = calc_target_params(2.0, &avg);
        assert_eq!(target.prepare.read_ext_msgs, u16::MAX);
        assert_eq!(
            target.prepare.read_int_msgs,
            metrics.wu_params.prepare.read_int_msgs
        );
    }

    fn synthetic_metrics_for_expected_params()
    -> (WuMetricsAvg, f64, WorkUnitsParams, WorkUnitsParams) {
        let target_wu_price = 2.0;
        let mut metrics = WuMetrics::default();
        metrics.wu_params.prepare.read_ext_msgs = 70;
        metrics.wu_params.prepare.read_int_msgs = 90;
        metrics.wu_params.prepare.read_new_msgs = 50;
        metrics.wu_params.prepare.add_to_msg_groups = 30;
        metrics.wu_params.execute.prepare = 5;
        metrics.wu_params.execute.execute = 300;
        metrics.wu_params.execute.execute_delimiter = 10;
        metrics.wu_params.execute.serialize_enqueue = 400;
        metrics.wu_params.execute.serialize_dequeue = 401;
        metrics.wu_params.execute.insert_new_msgs = 402;
        metrics.wu_params.execute.subgroup_size = 8;
        metrics.wu_params.finalize.build_in_msg = 110;
        metrics.wu_params.finalize.build_out_msg = 130;
        metrics.wu_params.finalize.build_transactions = 170;
        metrics.wu_params.finalize.build_accounts = 190;
        metrics.wu_params.finalize.state_update_accounts = 230;
        metrics.wu_params.finalize.serialize_accounts = 290;
        metrics.wu_params.finalize.serialize_msg = 291;
        metrics.wu_params.finalize.create_diff = 310;
        metrics.wu_params.finalize.apply_diff = 370;
        metrics.wu_params.finalize.diff_tail_len = 410;
        metrics.wu_params.finalize.state_update_min = 50;
        metrics.wu_params.finalize.serialize_min = 50;
        metrics.wu_on_finalize.updated_accounts_count = 1000;
        metrics.wu_on_finalize.shard_accounts_count = 100_000;
        metrics.wu_on_do_collate.updated_accounts_count = 1000;
        metrics.wu_on_do_collate.shard_accounts_count = 100_000;

        let mut avg = WuMetricsAvg::new();
        let mut unit_cost_clippers = test_unit_cost_clippers();
        avg.accum(&metrics, &mut unit_cost_clippers);
        avg.prepare.read_ext_msgs_sums_accum.add((140, 10));
        avg.prepare.read_existing_int_msgs_sums_accum.add((180, 10));
        avg.prepare.read_new_int_msgs_sums_accum.add((100, 10));
        avg.prepare.add_msgs_to_groups_sums_accum.add((60, 10));
        avg.execute
            .execute_groups_vm_sums_accum
            .add((4000, 1_000_000, 5_000_000));
        avg.execute.process_txs_sums_accum.add((1600, 20));
        avg.finalize.build_in_msg_sums_accum.add((220, 10));
        avg.finalize.build_out_msg_sums_accum.add((312, 12));
        avg.finalize.build_accounts_blocks_sums_accum.add((238, 7));
        avg.finalize.update_shard_accounts_sums_accum.add((190, 50));
        avg.finalize.build_state_update_sums_accum.add((184, 40));
        avg.finalize.build_block_sums_accum.add((522, 9));
        avg.finalize.create_diff_sums_accum.add((496, 8));
        avg.finalize.apply_diff_sums_accum.add((444, 6));
        avg.do_collate.resume_collation_sums_accum.add((41, 50));
        let fallback_params = metrics.wu_params;

        let mut expected_params = WorkUnitsParams::default();
        expected_params.prepare.read_ext_msgs = 7;
        expected_params.prepare.read_int_msgs = 9;
        expected_params.prepare.read_new_msgs = 5;
        expected_params.prepare.add_to_msg_groups = 3;
        expected_params.execute.execute = 30;
        expected_params.execute.serialize_enqueue = 40;
        expected_params.execute.serialize_dequeue = 40;
        expected_params.execute.insert_new_msgs = 40;
        expected_params.finalize.build_in_msg = 11;
        expected_params.finalize.build_out_msg = 13;
        expected_params.finalize.build_transactions = 17;
        expected_params.finalize.build_accounts = 19;
        expected_params.finalize.state_update_accounts = 23;
        expected_params.finalize.serialize_accounts = 29;
        expected_params.finalize.serialize_msg = 29;
        expected_params.finalize.create_diff = 31;
        expected_params.finalize.apply_diff = 37;
        expected_params.finalize.diff_tail_len = 41;

        (avg, target_wu_price, fallback_params, expected_params)
    }

    #[test]
    fn calculate_target_wu_params_matches_expected_params() {
        let (avg_metrics, target_wu_price, fallback_params, expected_params) =
            synthetic_metrics_for_expected_params();
        let target = calc_target_params(target_wu_price, &avg_metrics);
        assert_eq!(
            target.prepare.read_ext_msgs,
            expected_params.prepare.read_ext_msgs
        );
        assert_ne!(
            target.prepare.read_ext_msgs,
            fallback_params.prepare.read_ext_msgs
        );
        assert_eq!(
            target.prepare.read_int_msgs,
            expected_params.prepare.read_int_msgs
        );
        assert_ne!(
            target.prepare.read_int_msgs,
            fallback_params.prepare.read_int_msgs
        );
        assert_eq!(
            target.prepare.read_new_msgs,
            expected_params.prepare.read_new_msgs
        );
        assert_ne!(
            target.prepare.read_new_msgs,
            fallback_params.prepare.read_new_msgs
        );
        assert_eq!(
            target.prepare.add_to_msg_groups,
            expected_params.prepare.add_to_msg_groups
        );
        assert_ne!(
            target.prepare.add_to_msg_groups,
            fallback_params.prepare.add_to_msg_groups
        );
        assert_eq!(target.execute.execute, expected_params.execute.execute);
        assert_ne!(target.execute.execute, fallback_params.execute.execute);
        assert_eq!(
            target.execute.serialize_enqueue,
            expected_params.execute.serialize_enqueue
        );
        assert_ne!(
            target.execute.serialize_enqueue,
            fallback_params.execute.serialize_enqueue
        );
        assert_eq!(
            target.execute.serialize_dequeue,
            expected_params.execute.serialize_dequeue
        );
        assert_ne!(
            target.execute.serialize_dequeue,
            fallback_params.execute.serialize_dequeue
        );
        assert_eq!(
            target.execute.insert_new_msgs,
            expected_params.execute.insert_new_msgs
        );
        assert_ne!(
            target.execute.insert_new_msgs,
            fallback_params.execute.insert_new_msgs
        );
        assert_eq!(
            target.finalize.build_in_msg,
            expected_params.finalize.build_in_msg
        );
        assert_ne!(
            target.finalize.build_in_msg,
            fallback_params.finalize.build_in_msg
        );
        assert_eq!(
            target.finalize.build_out_msg,
            expected_params.finalize.build_out_msg
        );
        assert_ne!(
            target.finalize.build_out_msg,
            fallback_params.finalize.build_out_msg
        );
        assert_eq!(
            target.finalize.build_transactions,
            expected_params.finalize.build_transactions
        );
        assert_ne!(
            target.finalize.build_transactions,
            fallback_params.finalize.build_transactions
        );
        assert_eq!(
            target.finalize.build_accounts,
            expected_params.finalize.build_accounts
        );
        assert_ne!(
            target.finalize.build_accounts,
            fallback_params.finalize.build_accounts
        );
        assert_eq!(
            target.finalize.state_update_accounts,
            expected_params.finalize.state_update_accounts
        );
        assert_ne!(
            target.finalize.state_update_accounts,
            fallback_params.finalize.state_update_accounts
        );
        assert_eq!(
            target.finalize.serialize_accounts,
            expected_params.finalize.serialize_accounts
        );
        assert_ne!(
            target.finalize.serialize_accounts,
            fallback_params.finalize.serialize_accounts
        );
        assert_eq!(
            target.finalize.serialize_msg,
            expected_params.finalize.serialize_msg
        );
        assert_ne!(
            target.finalize.serialize_msg,
            fallback_params.finalize.serialize_msg
        );
        assert_eq!(
            target.finalize.create_diff,
            expected_params.finalize.create_diff
        );
        assert_ne!(
            target.finalize.create_diff,
            fallback_params.finalize.create_diff
        );
        assert_eq!(
            target.finalize.apply_diff,
            expected_params.finalize.apply_diff
        );
        assert_ne!(
            target.finalize.apply_diff,
            fallback_params.finalize.apply_diff
        );
        assert_eq!(
            target.finalize.diff_tail_len,
            expected_params.finalize.diff_tail_len
        );
        assert_ne!(
            target.finalize.diff_tail_len,
            fallback_params.finalize.diff_tail_len
        );
    }

    #[test]
    fn target_params_from_sums_do_not_depend_on_avg_elapsed_view() {
        let mut metrics = WuMetrics::default();
        metrics.wu_params.prepare.read_ext_msgs = 77;
        metrics.wu_on_finalize.updated_accounts_count = 1;
        metrics.wu_on_finalize.shard_accounts_count = 1;

        let mut avg_fast = WuMetricsAvg::new();
        avg_fast.accum_materialized_avg_result(&metrics);
        avg_fast.prepare.read_ext_msgs_sums_accum.add((140, 10));
        avg_fast.prepare.read_ext_msgs_elapsed_ns.accum(10u64);

        let mut avg_slow = WuMetricsAvg::new();
        avg_slow.accum_materialized_avg_result(&metrics);
        avg_slow.prepare.read_ext_msgs_sums_accum.add((140, 10));
        avg_slow.prepare.read_ext_msgs_elapsed_ns.accum(10_000u64);

        let target_fast = calc_target_params(2.0, &avg_fast);
        let target_slow = calc_target_params(2.0, &avg_slow);
        assert_eq!(
            target_fast.prepare.read_ext_msgs,
            target_slow.prepare.read_ext_msgs
        );
    }

    #[test]
    fn threshold_filters_are_applied_in_aggregation() {
        let mut first = WuMetrics::default();
        first.wu_params.finalize.state_update_min = 100;
        first.wu_params.finalize.serialize_min = 120;
        first.wu_params.finalize.state_update_accounts = 200;
        first.wu_params.finalize.serialize_accounts = 100;
        first.wu_params.finalize.serialize_msg = 100;
        first.wu_params.execute.subgroup_size = 4;
        first.wu_on_finalize.updated_accounts_count = 10;
        first.wu_on_finalize.shard_accounts_count = 1000;
        first.wu_on_finalize.build_state_update_base = 10;
        first.wu_on_finalize.build_state_update_wu = 200;
        first.wu_on_finalize.build_state_update_elapsed = Duration::from_nanos(200);
        first.wu_on_finalize.build_block_base = 5;
        first.wu_on_finalize.build_block_wu = 200;
        first.wu_on_finalize.build_block_elapsed = Duration::from_nanos(500);

        let mut second = WuMetrics {
            wu_params: first.wu_params.clone(),
            ..Default::default()
        };
        second.wu_on_finalize.updated_accounts_count = first.wu_on_finalize.updated_accounts_count;
        second.wu_on_finalize.shard_accounts_count = first.wu_on_finalize.shard_accounts_count;
        second.wu_on_finalize.build_state_update_base = 20;
        second.wu_on_finalize.build_state_update_wu = 80;
        second.wu_on_finalize.build_state_update_elapsed = Duration::from_nanos(10_000);
        second.wu_on_finalize.build_block_base = 8;
        second.wu_on_finalize.build_block_wu = 100;
        second.wu_on_finalize.build_block_elapsed = Duration::from_nanos(8_000);

        let mut avg = WuMetricsAvg::new();
        let mut unit_cost_clippers = test_unit_cost_clippers();
        avg.accum(&first, &mut unit_cost_clippers);
        avg.accum(&second, &mut unit_cost_clippers);
        assert_eq!(*avg.finalize.build_state_update_sums_accum, (200, 10));
        assert_eq!(*avg.finalize.build_block_sums_accum, (500, 5));
        let avg_metrics = avg.get_avg().expect("average should exist");
        assert_eq!(
            avg_metrics
                .wu_on_finalize
                .build_state_update_elapsed
                .as_nanos(),
            5_100,
        );
        assert_eq!(
            avg_metrics.wu_on_finalize.build_block_elapsed.as_nanos(),
            4_250
        );
        let target = calc_target_params(1.0, &avg);
        assert_eq!(
            target.finalize.state_update_accounts,
            first.wu_params.finalize.state_update_accounts
        );
        assert_eq!(
            target.finalize.serialize_accounts,
            first.wu_params.finalize.serialize_accounts
        );
        assert_eq!(
            target.finalize.serialize_msg,
            first.wu_params.finalize.serialize_msg
        );
    }

    #[test]
    fn filtered_samples_lead_to_zero_denominator_and_fallback() {
        let mut sample = WuMetrics::default();
        sample.wu_params.prepare.read_ext_msgs = 77;
        sample.wu_on_finalize.updated_accounts_count = 1;
        sample.wu_on_finalize.shard_accounts_count = 1;
        sample.wu_on_prepare_msg_groups.read_ext_msgs_base = 0;
        sample.wu_on_prepare_msg_groups.read_ext_msgs_elapsed = Duration::from_nanos(100);

        let mut avg = WuMetricsAvg::new();
        let mut unit_cost_clippers = test_unit_cost_clippers();
        avg.accum(&sample, &mut unit_cost_clippers);
        assert_eq!(*avg.prepare.read_ext_msgs_sums_accum, (0, 0));
        let target = calc_target_params(1.0, &avg);
        assert_eq!(
            target.prepare.read_ext_msgs,
            sample.wu_params.prepare.read_ext_msgs
        );
    }
}
