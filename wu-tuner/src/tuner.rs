use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tycho_collator::collator::work_units::{
    DoCollateWu, ExecuteWu, FinalizeWu, PrepareMsgGroupsWu, report_anchor_lag_to_metrics,
};
use tycho_collator::types::processed_upto::BlockSeqno;
use tycho_types::models::{ShardIdent, WorkUnitsParams};

use crate::config::WuTunerConfig;
use crate::{MempoolAnchorLag, WuEvent, WuEventData, WuMetrics};

#[derive(Default)]
pub struct WuHistory {
    metrics: BTreeMap<BlockSeqno, Box<WuMetrics>>,
    avg_metrics: BTreeMap<BlockSeqno, WuMetrics>,
    anchors_lag: BTreeMap<BlockSeqno, MempoolAnchorLag>,
    avg_anchors_lag: BTreeMap<BlockSeqno, i64>,
}

impl WuHistory {
    fn clear(&mut self) {
        self.metrics.clear();
        self.avg_metrics.clear();
        self.anchors_lag.clear();
        self.avg_anchors_lag.clear();
    }

    fn gc(&mut self, gc_boundary: u32) {
        self.metrics.retain(|k, _| k > &gc_boundary);
        self.avg_metrics.retain(|k, _| k > &gc_boundary);
        self.anchors_lag.retain(|k, _| k > &gc_boundary);
        self.avg_anchors_lag.retain(|k, _| k > &gc_boundary);
    }
}

pub struct WuAdjustment {
    pub old: WorkUnitsParams,
    pub new: WorkUnitsParams,
}

pub struct WuTuner {
    config: Arc<WuTunerConfig>,
    history: BTreeMap<ShardIdent, WuHistory>,
    adjustments: BTreeMap<BlockSeqno, WuAdjustment>,
}

impl WuTuner {
    pub fn new() -> Self {
        Self {
            config: Default::default(),
            history: Default::default(),
            adjustments: Default::default(),
        }
    }

    pub fn with_config(config: Arc<WuTunerConfig>) -> Self {
        Self {
            config,
            history: Default::default(),
            adjustments: Default::default(),
        }
    }

    pub fn update_config(&mut self, config: Arc<WuTunerConfig>) {
        self.config = config;
    }

    pub async fn handle_wu_event(&mut self, event: WuEvent) -> Result<()> {
        let WuEvent { shard, seqno, data } = event;

        let ma_interval = self.config.ma_interval.max(1);

        // normilized seqno on which we store the average value for further tune
        let avg_seqno = seqno / ma_interval as u32 * ma_interval as u32;

        let history = self.history.entry(shard).or_default();

        match data {
            WuEventData::Metrics(metrics) => {
                let has_pending_messages = metrics.has_pending_messages;

                // drop history on a gap in metrics
                if let Some((&last_key, _)) = history.metrics.last_key_value() {
                    if last_key + 1 < seqno {
                        history.clear();

                        tracing::debug!(
                            %shard,
                            seqno,
                            last_seqno = last_key,
                            "drop wu history on the gap",
                        );
                    }
                }

                // append history
                history.metrics.insert(seqno, metrics);

                tracing::debug!(
                    %shard,
                    seqno,
                    has_pending_messages,
                    metrics_history_len = history.metrics.len(),
                    "wu metrics received",
                );

                // calculate MA
                if history.metrics.len() >= ma_interval as usize {
                    let avg_range = history.metrics.range((
                        Bound::Excluded(seqno.saturating_sub(ma_interval as u32)),
                        Bound::Included(seqno),
                    ));
                    let avg = safe_metrics_avg(avg_range);

                    // report avg wu metrics
                    avg.report_metrics(&shard);

                    tracing::debug!(
                        %shard,
                        seqno,
                        "avg wu metrics calculated",
                    );

                    // store avg wu metrics to use further for tune
                    // if had pending messages during the interval
                    if avg.has_pending_messages && avg_seqno == seqno {
                        history.avg_metrics.insert(seqno, avg);
                    }
                }

                // clear outdated history
                let gc_boundary = avg_seqno.saturating_sub(ma_interval as u32);
                if let Some((&first_key, _)) = history.metrics.first_key_value() {
                    if first_key <= gc_boundary {
                        history.gc(gc_boundary);

                        tracing::debug!(
                            %shard,
                            seqno,
                            gc_boundary,
                            "wu metrics history gc",
                        );
                    }
                }
            }
            WuEventData::AnchorLag(anchor_lag) => {
                // store lag data
                history.anchors_lag.insert(seqno, anchor_lag.clone());

                tracing::debug!(
                    %shard,
                    seqno,
                    ?anchor_lag,
                    lag_history_len = history.anchors_lag.len(),
                    "anchor lag received",
                );

                // calculate MA lag
                let avg_range = history.anchors_lag.range((
                    Bound::Excluded(seqno.saturating_sub(ma_interval as u32)),
                    Bound::Included(seqno),
                ));
                let avg = safe_anchors_lag_avg(avg_range);

                tracing::debug!(
                    %shard,
                    seqno,
                    avg,
                    max_lag_ms = self.config.max_lag_ms,
                    "avg anchor lag calculated",
                );

                // report avg anchor importing lag to metrics
                report_anchor_lag_to_metrics(&shard, avg);

                // store avg lag for previous interval if this is the first event after interval
                if history.avg_anchors_lag.contains_key(&avg_seqno) {
                    return Ok(());
                }
                history.avg_anchors_lag.insert(seqno, avg);

                // TODO: check lag and calculate adjustment
                let lag_abs = avg.unsigned_abs();
                if lag_abs > self.config.max_lag_ms as u64 {
                    //
                }
            }
        }

        Ok(())
    }
}

fn safe_metrics_avg<'a, I>(range: I) -> WuMetrics
where
    I: Iterator<Item = (&'a u32, &'a Box<WuMetrics>)>,
{
    let mut avg = WuMetricsAvg::default();

    let mut had_pending_messages = true;
    for (_, v) in range {
        had_pending_messages = had_pending_messages && v.has_pending_messages;

        // wu_on_prepare_msg_groups
        avg.accum(0, v.wu_on_prepare_msg_groups.read_ext_msgs_wu);
        avg.accum(
            1,
            v.wu_on_prepare_msg_groups.read_ext_msgs_elapsed.as_nanos(),
        );
        avg.accum(2, v.wu_on_prepare_msg_groups.read_existing_int_msgs_wu);
        avg.accum(
            3,
            v.wu_on_prepare_msg_groups
                .read_existing_int_msgs_elapsed
                .as_nanos(),
        );
        avg.accum(4, v.wu_on_prepare_msg_groups.read_new_int_msgs_wu);
        avg.accum(
            5,
            v.wu_on_prepare_msg_groups
                .read_new_int_msgs_elapsed
                .as_nanos(),
        );
        avg.accum(6, v.wu_on_prepare_msg_groups.add_msgs_to_groups_wu);
        avg.accum(
            7,
            v.wu_on_prepare_msg_groups
                .add_msgs_to_groups_elapsed
                .as_nanos(),
        );
        avg.accum(8, v.wu_on_prepare_msg_groups.total_wu);
        avg.accum(9, v.wu_on_prepare_msg_groups.total_elapsed.as_nanos());

        // wu_on_execute
        avg.accum(10, v.wu_on_execute.execute_groups_vm_only_wu);
        avg.accum(
            11,
            v.wu_on_execute.execute_groups_vm_only_elapsed.as_nanos(),
        );
        avg.accum(12, v.wu_on_execute.process_txs_wu);
        avg.accum(13, v.wu_on_execute.process_txs_elapsed.as_nanos());

        // wu_on_finalize
        avg.accum(14, v.wu_on_finalize.create_queue_diff_wu);
        avg.accum(15, v.wu_on_finalize.create_queue_diff_elapsed.as_nanos());
        avg.accum(16, v.wu_on_finalize.apply_queue_diff_wu);
        avg.accum(17, v.wu_on_finalize.apply_queue_diff_elapsed.as_nanos());
        avg.accum(18, v.wu_on_finalize.update_shard_accounts_wu);
        avg.accum(
            19,
            v.wu_on_finalize.update_shard_accounts_elapsed.as_nanos(),
        );
        avg.accum(20, v.wu_on_finalize.build_accounts_blocks_wu);
        avg.accum(
            21,
            v.wu_on_finalize.build_accounts_blocks_elapsed.as_nanos(),
        );
        avg.accum(22, v.wu_on_finalize.build_accounts_elapsed.as_nanos());
        avg.accum(23, v.wu_on_finalize.build_in_msgs_wu);
        avg.accum(24, v.wu_on_finalize.build_in_msgs_elapsed.as_nanos());
        avg.accum(25, v.wu_on_finalize.build_out_msgs_wu);
        avg.accum(26, v.wu_on_finalize.build_out_msgs_elapsed.as_nanos());
        avg.accum(
            27,
            v.wu_on_finalize
                .build_accounts_and_messages_in_parallel_elased
                .as_nanos(),
        );
        avg.accum(28, v.wu_on_finalize.build_state_update_wu);
        avg.accum(29, v.wu_on_finalize.build_state_update_elapsed.as_nanos());
        avg.accum(30, v.wu_on_finalize.build_block_wu);
        avg.accum(31, v.wu_on_finalize.build_block_elapsed.as_nanos());
        avg.accum(32, v.wu_on_finalize.finalize_block_elapsed.as_nanos());
        avg.accum(33, v.wu_on_finalize.total_elapsed.as_nanos());

        // wu_on_do_collate
        avg.accum(34, v.wu_on_do_collate.resume_collation_wu);
        avg.accum(35, v.wu_on_do_collate.resume_collation_elapsed.as_nanos());
        avg.accum(36, v.wu_on_do_collate.resume_collation_wu_per_block);
        avg.accum(37, v.wu_on_do_collate.resume_collation_elapsed_per_block_ns);
        avg.accum(38, v.wu_on_do_collate.collation_total_elapsed.as_nanos());
    }

    WuMetrics {
        wu_on_prepare_msg_groups: PrepareMsgGroupsWu {
            read_ext_msgs_wu: avg.get_avg(0) as u64,
            read_ext_msgs_elapsed: Duration::from_nanos(avg.get_avg(1) as u64),
            read_existing_int_msgs_wu: avg.get_avg(2) as u64,
            read_existing_int_msgs_elapsed: Duration::from_nanos(avg.get_avg(3) as u64),
            read_new_int_msgs_wu: avg.get_avg(4) as u64,
            read_new_int_msgs_elapsed: Duration::from_nanos(avg.get_avg(5) as u64),
            add_msgs_to_groups_wu: avg.get_avg(6) as u64,
            add_msgs_to_groups_elapsed: Duration::from_nanos(avg.get_avg(7) as u64),
            total_wu: avg.get_avg(8) as u64,
            total_elapsed: Duration::from_nanos(avg.get_avg(9) as u64),
        },
        wu_on_execute: ExecuteWu {
            execute_groups_vm_only_wu: avg.get_avg(10) as u64,
            execute_groups_vm_only_elapsed: Duration::from_nanos(avg.get_avg(11) as u64),
            process_txs_wu: avg.get_avg(12) as u64,
            process_txs_elapsed: Duration::from_nanos(avg.get_avg(13) as u64),
        },
        wu_on_finalize: FinalizeWu {
            create_queue_diff_wu: avg.get_avg(14) as u64,
            create_queue_diff_elapsed: Duration::from_nanos(avg.get_avg(15) as u64),
            apply_queue_diff_wu: avg.get_avg(16) as u64,
            apply_queue_diff_elapsed: Duration::from_nanos(avg.get_avg(17) as u64),
            update_shard_accounts_wu: avg.get_avg(18) as u64,
            update_shard_accounts_elapsed: Duration::from_nanos(avg.get_avg(19) as u64),
            build_accounts_blocks_wu: avg.get_avg(20) as u64,
            build_accounts_blocks_elapsed: Duration::from_nanos(avg.get_avg(21) as u64),
            build_accounts_elapsed: Duration::from_nanos(avg.get_avg(22) as u64),
            build_in_msgs_wu: avg.get_avg(23) as u64,
            build_in_msgs_elapsed: Duration::from_nanos(avg.get_avg(24) as u64),
            build_out_msgs_wu: avg.get_avg(25) as u64,
            build_out_msgs_elapsed: Duration::from_nanos(avg.get_avg(26) as u64),
            build_accounts_and_messages_in_parallel_elased: Duration::from_nanos(
                avg.get_avg(27) as u64
            ),
            build_state_update_wu: avg.get_avg(28) as u64,
            build_state_update_elapsed: Duration::from_nanos(avg.get_avg(29) as u64),
            build_block_wu: avg.get_avg(30) as u64,
            build_block_elapsed: Duration::from_nanos(avg.get_avg(31) as u64),
            finalize_block_elapsed: Duration::from_nanos(avg.get_avg(32) as u64),
            total_elapsed: Duration::from_nanos(avg.get_avg(33) as u64),
        },
        wu_on_do_collate: DoCollateWu {
            resume_collation_wu: avg.get_avg(34) as u64,
            resume_collation_elapsed: Duration::from_nanos(avg.get_avg(35) as u64),
            resume_collation_wu_per_block: avg.get_avg(36) as u64,
            resume_collation_elapsed_per_block_ns: avg.get_avg(37),
            collation_total_elapsed: Duration::from_nanos(avg.get_avg(38) as u64),
        },
        has_pending_messages: had_pending_messages,
    }
}

struct WuMetricsAvg {
    counters: [u128; 39],
    sums: [u128; 39],
}

impl Default for WuMetricsAvg {
    fn default() -> Self {
        Self {
            counters: [0; 39],
            sums: [0; 39],
        }
    }
}

impl WuMetricsAvg {
    pub fn accum<V: Into<u128>>(&mut self, idx: usize, v: V) {
        let v = v.into();
        self.sums[idx] = self.sums[idx].checked_add(v).unwrap_or_else(|| {
            let partial = self.sums[idx] / self.counters[idx];
            self.counters[idx] = 0;
            partial.saturating_add(v)
        });
        self.counters[idx] += 1;
    }

    pub fn get_avg(&mut self, idx: usize) -> u128 {
        if self.counters[idx] == 0 {
            0
        } else {
            self.sums[idx].saturating_div(self.counters[idx])
        }
    }
}

fn safe_anchors_lag_avg<'a, I>(range: I) -> i64
where
    I: Iterator<Item = (&'a u32, &'a MempoolAnchorLag)>,
{
    let mut counter = 0_i128;
    let mut sum = 0_i128;

    for (_, v) in range {
        let lag = v.lag() as i128;
        sum = sum.checked_add(lag).unwrap_or_else(|| {
            let partial = sum / counter;
            counter = 0;
            partial.saturating_add(lag)
        });
        counter += 1;
    }

    if counter == 0 {
        0
    } else {
        sum.saturating_div(counter) as i64
    }
}
