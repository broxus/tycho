use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tycho_collator::collator::work_units::{
    DoCollateWu, ExecuteWu, FinalizeWu, PrepareMsgGroupsWu, calc_threads_count,
    report_anchor_lag_to_metrics,
};
use tycho_collator::types::processed_upto::BlockSeqno;
use tycho_types::models::{
    ShardIdent, WorkUnitsParams, WorkUnitsParamsExecute, WorkUnitsParamsFinalize,
    WorkUnitsParamsPrepare,
};
use tycho_util::FastHashSet;
use tycho_util::num::{SafeSignedAvg, SafeUnsignedAvg, VecOfStreamingUnsignedMedian};

use crate::config::{WuTuneType, WuTunerConfig};
use crate::updater::WuParamsUpdater;
use crate::{MempoolAnchorLag, WuEvent, WuEventData, WuMetrics};

pub enum WuMetricsSpanValue {
    Accum(WuMetricsAvg),
    Result(Box<WuMetrics>),
}

pub struct WuMetricsSpan {
    pub avg: WuMetricsSpanValue,
    pub last: Box<WuMetrics>,
}

impl WuMetricsSpan {
    pub fn new(metrics: Box<WuMetrics>) -> Self {
        let mut avg = WuMetricsAvg::new();
        avg.accum(&metrics);
        Self {
            avg: WuMetricsSpanValue::Accum(avg),
            last: metrics,
        }
    }

    pub fn accum(&mut self, v: Box<WuMetrics>) -> Result<()> {
        match &mut self.avg {
            WuMetricsSpanValue::Accum(avg) => avg.accum(&v),
            WuMetricsSpanValue::Result(_) => {
                anyhow::bail!("WuMetricsSpan.avg should be Accum here")
            }
        }
        self.last = v;
        Ok(())
    }

    pub fn get_result(&mut self) -> Option<&WuMetrics> {
        if let WuMetricsSpanValue::Accum(accum) = &mut self.avg {
            let avg = accum.get_avg()?;
            self.avg = WuMetricsSpanValue::Result(Box::new(avg));
        }

        let WuMetricsSpanValue::Result(avg) = &self.avg else {
            unreachable!()
        };

        Some(avg)
    }
}

pub enum AnchorsLagSpanValue {
    Accum(SafeSignedAvg),
    Result(i64),
}

pub struct AnchorsLagSpan {
    pub avg: AnchorsLagSpanValue,
    pub last: Option<MempoolAnchorLag>,
    pub last_seqno: u32,
}

impl AnchorsLagSpan {
    pub fn new(lag: MempoolAnchorLag, seqno: u32) -> Self {
        Self {
            avg: AnchorsLagSpanValue::Accum(SafeSignedAvg::default()),
            last: Some(lag),
            last_seqno: seqno,
        }
    }

    pub fn accum(&mut self, v: MempoolAnchorLag, seqno: u32) -> Result<()> {
        if self.last_seqno != seqno {
            // accum lag value only for the last anchor after block seqno
            self.accum_last()?;
            self.last_seqno = seqno;
        }
        self.last = Some(v);
        Ok(())
    }

    fn accum_last(&mut self) -> Result<()> {
        if let Some(last) = self.last.take() {
            match &mut self.avg {
                AnchorsLagSpanValue::Accum(accum) => accum.accum(last.lag()),
                AnchorsLagSpanValue::Result(_) => {
                    anyhow::bail!("AnchorsLagSpan.avg should be Accum here")
                }
            }
        }
        Ok(())
    }

    pub fn get_result(&mut self) -> i64 {
        if let AnchorsLagSpanValue::Accum(accum) = &mut self.avg {
            if let Some(last) = self.last.take() {
                accum.accum(last.lag());
            }
            self.avg = AnchorsLagSpanValue::Result(accum.get_avg() as i64);
        }

        let AnchorsLagSpanValue::Result(avg) = self.avg else {
            unreachable!()
        };

        avg
    }
}

#[derive(Default)]
pub struct WuHistory {
    metrics: BTreeMap<BlockSeqno, WuMetricsSpan>,
    avg_metrics: BTreeMap<BlockSeqno, WuMetrics>,
    anchors_lag: BTreeMap<BlockSeqno, AnchorsLagSpan>,
    avg_anchors_lag: BTreeMap<BlockSeqno, i64>,
}

impl WuHistory {
    fn clear(&mut self) {
        self.metrics.clear();
        self.avg_metrics.clear();
        self.anchors_lag.clear();
        self.avg_anchors_lag.clear();
    }

    fn gc_wu_metrics(&mut self, gc_boundary: u32) {
        self.metrics.retain(|k, _| k >= &gc_boundary);
        self.avg_metrics.retain(|k, _| k >= &gc_boundary);
    }

    fn gc_anchors_lag(&mut self, gc_boundary: u32) {
        self.anchors_lag.retain(|k, _| k >= &gc_boundary);
        self.avg_anchors_lag.retain(|k, _| k >= &gc_boundary);
    }
}

pub struct WuAdjustment {
    pub target_wu_price: f64,
}

pub struct WuTuner<U>
where
    U: WuParamsUpdater,
{
    config: Arc<WuTunerConfig>,
    updater: U,
    history: BTreeMap<ShardIdent, WuHistory>,
    target_wu_params_history: BTreeMap<BlockSeqno, WorkUnitsParams>,
    avg_target_wu_params_history: BTreeMap<BlockSeqno, WorkUnitsParams>,
    wu_once_reported: FastHashSet<ShardIdent>,
    lag_once_reported: bool,
    adjustments: BTreeMap<BlockSeqno, WuAdjustment>,
}

impl<U> WuTuner<U>
where
    U: WuParamsUpdater,
{
    pub fn new(config: Arc<WuTunerConfig>, updater: U) -> Self {
        Self {
            config,
            updater,
            history: Default::default(),
            target_wu_params_history: Default::default(),
            avg_target_wu_params_history: Default::default(),
            wu_once_reported: Default::default(),
            lag_once_reported: false,
            adjustments: Default::default(),
        }
    }

    pub fn update_config(&mut self, config: Arc<WuTunerConfig>) {
        self.config = config;
        self.clear_history();
    }

    fn clear_history(&mut self) {
        self.history.clear();
        self.target_wu_params_history.clear();
        self.avg_target_wu_params_history.clear();
    }

    pub async fn handle_wu_event(&mut self, event: WuEvent) -> Result<()> {
        let WuEvent { shard, seqno, data } = event;

        let wu_span = self.config.wu_span.max(10) as u32;
        let wu_ma_interval = self.config.wu_ma_interval.max(5) as u32;
        let wu_ma_interval = wu_ma_interval.saturating_mul(wu_span);

        let lag_span = self.config.lag_span.max(10) as u32;
        let lag_ma_interval = self.config.lag_ma_interval.max(4) as u32;
        let lag_ma_interval = lag_ma_interval.saturating_mul(lag_span);

        let tune_interval = self.config.tune_interval.max(100) as u32;
        let tune_seqno = seqno / tune_interval * tune_interval;

        // normilized seqno for calculations
        // e.g. seqno = 244
        let wu_span_seqno = seqno / wu_span * wu_span; // 240
        let wu_ma_seqno = seqno / wu_ma_interval * wu_ma_interval; // 200
        let lag_span_seqno = seqno / lag_span * lag_span; // 240
        let lag_ma_seqno = seqno / lag_ma_interval * lag_ma_interval; // 240

        let history = self.history.entry(shard).or_default();

        match data {
            WuEventData::Metrics(metrics) => {
                let has_pending_messages = metrics.has_pending_messages;

                // drop history on a gap in metrics
                if let Some((&last_key, _)) = history.metrics.last_key_value() {
                    // e.g. (240 + 10 < 244) == false
                    if last_key + wu_span < seqno {
                        history.clear();
                        self.target_wu_params_history.clear();
                        self.avg_target_wu_params_history.clear();

                        tracing::debug!(
                            %shard, seqno,
                            last_seqno = last_key,
                            wu_span,
                            "drop wu history on the gap",
                        );
                    }
                }

                // report wu metrics and params on the start to avoid empty graphs
                if !self.wu_once_reported.contains(&shard) {
                    metrics.report_metrics(&shard);
                    report_wu_params(&metrics.wu_params, &metrics.wu_params);
                    self.wu_once_reported.insert(shard);
                }

                // handle if wu params changed
                if let Some((_, last)) = history.metrics.last_key_value()
                    && metrics.wu_params != last.last.wu_params
                {
                    tracing::info!(
                        %shard, seqno,
                        prev_params = ?last.last.wu_params,
                        curr_params = ?metrics.wu_params,
                        "wu params updated",
                    );

                    // clear history
                    history.clear();
                    self.target_wu_params_history.clear();
                    self.avg_target_wu_params_history.clear();

                    // report updated wu params to metrics
                    report_wu_params(&metrics.wu_params, &metrics.wu_params);
                }

                // update history
                match history.metrics.entry(wu_span_seqno) {
                    std::collections::btree_map::Entry::Vacant(vacant) => {
                        vacant.insert(WuMetricsSpan::new(metrics));
                    }
                    std::collections::btree_map::Entry::Occupied(mut occupied) => {
                        let span = occupied.get_mut();
                        span.accum(metrics)?;
                    }
                }

                tracing::trace!(
                    %shard, seqno,
                    has_pending_messages,
                    metrics_history_len = history.metrics.len(),
                    "wu metrics received",
                );

                // calculate MA wu metrics
                if seqno != wu_ma_seqno {
                    return Ok(());
                }

                // e.g. seqno = 200 -> avg_range = [150..200)
                let avg_from_boundary = wu_ma_seqno.saturating_sub(wu_ma_interval);
                let avg_range = history.metrics.range_mut((
                    Bound::Included(avg_from_boundary),
                    Bound::Excluded(wu_ma_seqno),
                ));
                let Some(avg) = safe_metrics_avg(avg_range) else {
                    return Ok(());
                };

                // report avg wu metrics
                avg.report_metrics(&shard);

                tracing::trace!(
                    %shard, seqno,
                    "avg wu metrics calculated on [{0}..{1})",
                    avg_from_boundary, wu_ma_seqno,
                );

                // store avg wu metrics to use further for tune
                history.avg_metrics.insert(seqno, avg);

                // clear outdated history
                let gc_boundary = wu_ma_seqno.saturating_sub(wu_ma_interval); // e.g. seqno = 200 -> gc_boundary = 150
                if let Some((&first_key, _)) = history.metrics.first_key_value() {
                    if first_key < gc_boundary {
                        history.gc_wu_metrics(gc_boundary);

                        tracing::debug!(
                            %shard, seqno,
                            "wu metrics history gc < {0}",
                            gc_boundary,
                        );
                    }
                }
            }
            WuEventData::AnchorLag(anchor_lag) => {
                // report current anchors lag on the start to avoid empty graphs
                if !self.lag_once_reported {
                    report_anchor_lag_to_metrics(&shard, anchor_lag.lag());
                    self.lag_once_reported = true;
                }

                // update history
                match history.anchors_lag.entry(lag_span_seqno) {
                    std::collections::btree_map::Entry::Vacant(vacant) => {
                        vacant.insert(AnchorsLagSpan::new(anchor_lag.clone(), seqno));
                    }
                    std::collections::btree_map::Entry::Occupied(mut occupied) => {
                        let span = occupied.get_mut();
                        span.accum(anchor_lag.clone(), seqno)?;
                    }
                }

                tracing::trace!(
                    %shard, seqno,
                    lag = anchor_lag.lag(),
                    max_lag_ms = self.config.max_lag_ms,
                    lag_history_len = history.anchors_lag.len(),
                    "anchor lag received",
                );

                // calculate MA lag
                if seqno < lag_ma_seqno
                    || lag_ma_seqno == 0
                    || history.avg_anchors_lag.contains_key(&lag_ma_seqno)
                {
                    return Ok(());
                }

                // e.g. seqno = 244 -> avg_range = [200..240)
                let avg_from_boundary = lag_ma_seqno.saturating_sub(lag_ma_interval);
                let avg_range = history.anchors_lag.range_mut((
                    Bound::Included(avg_from_boundary),
                    Bound::Excluded(lag_ma_seqno),
                ));
                let Some(avg_lag) = safe_anchors_lag_avg(avg_range) else {
                    return Ok(());
                };

                // report avg anchor importing lag to metrics
                report_anchor_lag_to_metrics(&shard, avg_lag);

                tracing::debug!(
                    %shard, seqno,
                    avg_lag,
                    max_lag_ms = self.config.max_lag_ms,
                    "avg anchor lag calculated on [{0}..{1})",
                    avg_from_boundary, lag_ma_seqno,
                );

                // store avg lag
                history.avg_anchors_lag.insert(lag_ma_seqno, avg_lag);

                // clear outdated history
                let gc_boundary = lag_ma_seqno.saturating_sub(lag_ma_interval); // e.g. seqno = 244 -> gc_boundary = 200
                if let Some((&first_key, _)) = history.anchors_lag.first_key_value() {
                    if first_key < gc_boundary {
                        history.gc_anchors_lag(gc_boundary);

                        tracing::debug!(
                            %shard, seqno,
                            "anchors lag history gc < {0}",
                            gc_boundary,
                        );
                    }
                }

                // check lag and calculate target wu params
                // NOTE: ONLY by shard blocks
                if shard != ShardIdent::BASECHAIN {
                    return Ok(());
                }

                // get last avg wu metrics
                if let Some(avg_wu_metrics) = history.avg_metrics.get(&wu_ma_seqno) {
                    let mut target_wu_params = None;

                    // first, get actual wu price
                    let mut target_wu_price =
                        avg_wu_metrics.wu_on_finalize.build_in_msgs_wu_price();

                    // when lag is negative but we do not have pending messages, it is okay
                    let avg_lag_abs = avg_lag.unsigned_abs();
                    if avg_lag_abs > self.config.max_lag_ms as u64
                        && (avg_lag > 0 || (avg_lag < 0 && avg_wu_metrics.has_pending_messages))
                    {
                        // define new target wu price
                        // get prev adjustment if exists
                        let prev_wu_price = self
                            .adjustments
                            .last_key_value()
                            .map_or(target_wu_price, |(_, v)| v.target_wu_price);
                        // if current lag is > 0 then we should reduce target wu price
                        if avg_lag > 0 {
                            target_wu_price = (prev_wu_price - 0.1).max(0.1);
                        }
                        // if current lag is < 0 then we should increase target wu price
                        else {
                            target_wu_price = prev_wu_price + 0.1;
                        }

                        // calculate target wu params
                        let target_params =
                            Self::calculate_target_wu_params(target_wu_price, avg_wu_metrics);

                        tracing::debug!(
                            %shard, seqno,
                            avg_lag,
                            max_lag_ms = self.config.max_lag_ms,
                            current_build_in_msgs_wu_price = avg_wu_metrics.wu_on_finalize.build_in_msgs_wu_price(),
                            prev_wu_price,
                            target_wu_price,
                            current_build_in_msg_wu_param = avg_wu_metrics.wu_params.finalize.build_in_msg,
                            target_build_in_msg_wu_param = target_params.finalize.build_in_msg,
                            "calculated target wu params",
                        );

                        report_target_wu_price(prev_wu_price, target_wu_price);

                        target_wu_params = Some(target_params);
                    }

                    match target_wu_params {
                        None => {
                            // do not store long history of unchanged existing wu params
                            // to calculate new params faster when lag appears
                            self.target_wu_params_history.clear();
                            self.avg_target_wu_params_history.clear();
                        }
                        Some(target_wu_params) => {
                            // store target wu params
                            self.target_wu_params_history
                                .insert(lag_ma_seqno, target_wu_params);

                            // calculate MA target wu params
                            // e.g. seqno = 244 -> avg_range = (140..240] = [160, 200, 240]
                            let avg_from_boundary = lag_ma_seqno.saturating_sub(tune_interval);
                            let avg_range = self.target_wu_params_history.range((
                                Bound::Excluded(avg_from_boundary),
                                Bound::Included(lag_ma_seqno),
                            ));
                            let Some(avg_target_wu_params) = safe_wu_params_avg(avg_range) else {
                                return Ok(());
                            };

                            // store MA target wu params
                            self.avg_target_wu_params_history
                                .insert(lag_ma_seqno, avg_target_wu_params.clone());

                            // calculate MA from MA target wu params
                            // e.g. seqno = 244 -> avg_range = (140..240] = [160, 200, 240]
                            let avg_from_boundary = lag_ma_seqno.saturating_sub(tune_interval);
                            let avg_range = self.avg_target_wu_params_history.range((
                                Bound::Excluded(avg_from_boundary),
                                Bound::Included(lag_ma_seqno),
                            ));
                            let Some(avg_target_wu_params) = safe_wu_params_avg(avg_range) else {
                                return Ok(());
                            };

                            tracing::debug!(
                                %shard, seqno,
                                current_build_in_msg_wu_param = avg_wu_metrics.wu_params.finalize.build_in_msg,
                                avg_target_build_in_msg_wu_param = avg_target_wu_params.finalize.build_in_msg,
                                "calculated avg target wu params on ({0}..{1}]",
                                avg_from_boundary, lag_ma_seqno,
                            );

                            // report target wu params to metrics
                            report_wu_params(&avg_wu_metrics.wu_params, &avg_target_wu_params);

                            // update wu params in blockchain if tune interval elapsed
                            if !self.adjustments.contains_key(&tune_seqno) {
                                match &self.config.tune {
                                    WuTuneType::Rpc { rpc, .. } => {
                                        tracing::info!(
                                            %shard, seqno,
                                            tune_seqno,
                                            avg_lag, target_wu_price,
                                            rpc,
                                            ?avg_target_wu_params,
                                            "updating target wu params in blockchain config via rpc",
                                        );

                                        // store current adjustment and gc previous
                                        self.adjustments
                                            .insert(tune_seqno, WuAdjustment { target_wu_price });
                                        let gc_boundary = tune_seqno.saturating_sub(tune_interval);
                                        if let Some((&first_key, _)) =
                                            self.adjustments.first_key_value()
                                            && first_key < gc_boundary
                                        {
                                            self.adjustments.retain(|k, _| k >= &gc_boundary);
                                        }

                                        // make adjustment
                                        self.updater
                                            .update_wu_params(
                                                self.config.clone(),
                                                avg_target_wu_params,
                                            )
                                            .await?;
                                    }
                                    WuTuneType::No => {
                                        // do nothing
                                    }
                                }
                            }
                        }
                    }

                    // clear outdated target wu params history
                    let gc_boundary = tune_seqno.saturating_sub(tune_interval);
                    if let Some((&first_key, _)) = self.target_wu_params_history.first_key_value() {
                        if first_key < gc_boundary {
                            self.target_wu_params_history
                                .retain(|k, _| k >= &gc_boundary);

                            tracing::debug!(
                                %shard, seqno,
                                "target wu params history gc <= {0}",
                                gc_boundary,
                            );
                        }
                    }
                    let gc_boundary = tune_seqno.saturating_sub(tune_interval);
                    if let Some((&first_key, _)) =
                        self.avg_target_wu_params_history.first_key_value()
                    {
                        if first_key < gc_boundary {
                            self.avg_target_wu_params_history
                                .retain(|k, _| k >= &gc_boundary);

                            tracing::debug!(
                                %shard, seqno,
                                "avg target wu params history gc <= {0}",
                                gc_boundary,
                            );
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn calculate_target_wu_params(target_wu_price: f64, wu_metrics: &WuMetrics) -> WorkUnitsParams {
        let mut target_wu_params = wu_metrics.wu_params.clone();

        // FINALIZE WU PARAMS

        // calculate target build_in_msgs_wu and target wu param for it
        let target_build_in_msgs_wu = wu_metrics
            .wu_on_finalize
            .calc_target_build_in_msgs_wu_by_price(target_wu_price);
        if let Some(target_wu_param) = wu_metrics
            .wu_on_finalize
            .calc_target_build_in_msg_wu_param(target_build_in_msgs_wu)
        {
            target_wu_params.finalize.build_in_msg = target_wu_param as u16;
        }

        // calculate target build_out_msgs_wu and param
        let target_build_out_msgs_wu = wu_metrics
            .wu_on_finalize
            .calc_target_build_out_msgs_wu_by_price(target_wu_price);
        if let Some(target_wu_param) = wu_metrics
            .wu_on_finalize
            .calc_target_build_out_msg_wu_param(target_build_out_msgs_wu)
        {
            target_wu_params.finalize.build_out_msg = target_wu_param as u16;
        }

        // calculate target build_accounts_blocks_wu and param
        let threads_count = calc_threads_count(
            wu_metrics.wu_params.execute.subgroup_size as u64,
            wu_metrics.wu_on_finalize.updated_accounts_count,
        );
        let target_build_accounts_blocks_wu = wu_metrics
            .wu_on_finalize
            .calc_target_build_accounts_blocks_wu_by_price(target_wu_price);
        if let Some(target_wu_param) = wu_metrics
            .wu_on_finalize
            .calc_target_build_accounts_blocks_wu_param(
                target_build_accounts_blocks_wu,
                threads_count,
            )
        {
            target_wu_params.finalize.build_transactions = target_wu_param as u16;
        }

        // calculate target update_shard_accounts_wu and param
        let shard_accounts_count_log = wu_metrics.wu_on_finalize.shard_accounts_count_log();
        let scale = 10;
        let pow_shard_accounts_count = wu_metrics
            .wu_on_finalize
            .pow_shard_accounts_count(wu_metrics.wu_params.finalize.state_update_msg, scale);
        let target_update_shard_accounts_wu = wu_metrics
            .wu_on_finalize
            .calc_target_update_shard_accounts_wu_by_price(target_wu_price);
        if let Some(target_wu_param) = wu_metrics
            .wu_on_finalize
            .calc_target_update_shard_accounts_wu_param(
                target_update_shard_accounts_wu,
                threads_count,
                shard_accounts_count_log,
                pow_shard_accounts_count,
                scale,
            )
        {
            target_wu_params.finalize.build_accounts = target_wu_param as u16;
        }

        // calculate target build_state_update_wu and param
        let target_build_state_update_wu = wu_metrics
            .wu_on_finalize
            .calc_target_build_state_update_wu_by_price(target_wu_price);
        if let Some(target_wu_param) = wu_metrics
            .wu_on_finalize
            .calc_target_build_state_update_wu_param(
                target_build_state_update_wu,
                threads_count,
                shard_accounts_count_log,
                pow_shard_accounts_count,
                scale,
                target_wu_params.finalize.state_update_min as u64,
                target_wu_params.finalize.state_update_accounts as u64,
            )
        {
            target_wu_params.finalize.state_update_accounts = target_wu_param as u16;
        }

        // calculate target build_block_wu and param
        let target_build_block_wu = wu_metrics
            .wu_on_finalize
            .calc_target_build_block_wu_by_price(target_wu_price);
        if let Some(target_wu_param) = wu_metrics.wu_on_finalize.calc_target_build_block_wu_param(
            target_build_block_wu,
            target_wu_params.finalize.serialize_min as u64,
            target_wu_params.finalize.serialize_accounts as u64,
        ) {
            target_wu_params.finalize.serialize_accounts = target_wu_param as u16;
            target_wu_params.finalize.serialize_msg = target_wu_params.finalize.serialize_accounts;
        }

        // calculate target create_queue_diff_wu and param
        let target_create_queue_diff_wu = wu_metrics
            .wu_on_finalize
            .calc_target_create_queue_diff_wu_by_price(target_wu_price);
        if let Some(target_wu_param) = wu_metrics
            .wu_on_finalize
            .calc_target_create_queue_diff_wu_param(target_create_queue_diff_wu)
        {
            target_wu_params.finalize.create_diff = target_wu_param as u16;
        }

        // calculate target apply_queue_diff_wu and param
        let target_apply_queue_diff_wu = wu_metrics
            .wu_on_finalize
            .calc_target_apply_queue_diff_wu_by_price(target_wu_price);
        if let Some(target_wu_param) = wu_metrics
            .wu_on_finalize
            .calc_target_apply_queue_diff_wu_param(target_apply_queue_diff_wu)
        {
            target_wu_params.finalize.apply_diff = target_wu_param as u16;
        }

        // READ MSGS GROUPS (PREPARE) WU PARAMS

        // calulate target read_ext_msgs_wu and param
        let target_read_ext_msgs_wu = wu_metrics
            .wu_on_prepare_msg_groups
            .calc_target_read_ext_msgs_wu_by_price(target_wu_price);
        if let Some(target_wu_param) = wu_metrics
            .wu_on_prepare_msg_groups
            .calc_target_read_ext_msgs_wu_param(target_read_ext_msgs_wu)
        {
            target_wu_params.prepare.read_ext_msgs = target_wu_param as u16;
        }

        // calulate target read_existing_int_msgs_wu and param
        let target_read_existing_int_msgs_wu = wu_metrics
            .wu_on_prepare_msg_groups
            .calc_target_read_existing_int_msgs_wu_by_price(target_wu_price);
        if let Some(target_wu_param) = wu_metrics
            .wu_on_prepare_msg_groups
            .calc_target_read_existing_int_msgs_wu_param(target_read_existing_int_msgs_wu)
        {
            target_wu_params.prepare.read_int_msgs = target_wu_param as u16;
        }

        // calulate target read_new_int_msgs_wu and param
        let target_read_new_int_msgs_wu = wu_metrics
            .wu_on_prepare_msg_groups
            .calc_target_read_new_int_msgs_wu_by_price(target_wu_price);
        if let Some(target_wu_param) = wu_metrics
            .wu_on_prepare_msg_groups
            .calc_target_read_new_int_msgs_wu_param(target_read_new_int_msgs_wu)
        {
            target_wu_params.prepare.read_new_msgs = target_wu_param as u16;
        }

        // calulate target add_msgs_to_groups_wu and param
        let target_add_msgs_to_groups_wu = wu_metrics
            .wu_on_prepare_msg_groups
            .calc_target_add_msgs_to_groups_wu_by_price(target_wu_price);
        if let Some(target_wu_param) = wu_metrics
            .wu_on_prepare_msg_groups
            .calc_target_add_msgs_to_groups_wu_param(target_add_msgs_to_groups_wu)
        {
            target_wu_params.prepare.add_to_msg_groups = target_wu_param as u16;
        }

        // EXECUTE WU PARAMS

        // calulate target execute_groups_vm_only_wu and param
        let target_execute_groups_vm_only_wu = wu_metrics
            .wu_on_execute
            .calc_target_execute_groups_vm_only_wu_by_price(target_wu_price);
        if let Some(target_wu_param) = wu_metrics.wu_on_execute.calc_target_execute_wu_param(
            target_execute_groups_vm_only_wu,
            target_wu_params.execute.prepare as u64,
            target_wu_params.execute.execute_delimiter as u64,
        ) {
            target_wu_params.execute.execute = target_wu_param as u16;
        }

        // calulate target target_process_txs_wu and param
        let target_process_txs_wu = wu_metrics
            .wu_on_execute
            .calc_target_process_txs_wu_by_price(target_wu_price);
        if let Some(target_wu_param) = wu_metrics
            .wu_on_execute
            .calc_target_process_txs_wu_param(target_process_txs_wu)
        {
            let target_wu_param = target_wu_param as u16;
            target_wu_params.execute.serialize_enqueue = target_wu_param;
            target_wu_params.execute.serialize_dequeue = target_wu_param;
            target_wu_params.execute.insert_new_msgs = target_wu_param;
        }

        // DO COLLATE WU PARAMS

        // calculate target resume_collation_wu and param
        let target_resume_collation_wu = wu_metrics
            .wu_on_do_collate
            .calc_target_resume_collation_wu_by_price(target_wu_price);
        let pow_updated_accounts_count = wu_metrics
            .wu_on_do_collate
            .pow_updated_accounts_count(wu_metrics.wu_params.finalize.serialize_diff, scale);
        if let Some(target_wu_param) = wu_metrics
            .wu_on_do_collate
            .calc_target_resume_collation_wu_param(
                target_resume_collation_wu,
                threads_count,
                pow_updated_accounts_count,
                shard_accounts_count_log,
                pow_shard_accounts_count,
                scale,
            )
        {
            target_wu_params.finalize.diff_tail_len = target_wu_param as u16;
        }

        target_wu_params
    }
}

fn safe_metrics_avg<'a, I>(range: I) -> Option<WuMetrics>
where
    I: Iterator<Item = (&'a u32, &'a mut WuMetricsSpan)>,
{
    let mut avg = WuMetricsAvg::new();
    for (_, v) in range {
        let Some(span_res) = v.get_result() else {
            continue;
        };
        avg.accum(span_res);
    }

    avg.get_avg()
}

pub struct WuMetricsAvg {
    last_wu_params: WorkUnitsParams,
    last_shard_accounts_count: u64,
    had_pending_messages: bool,
    avg: VecOfStreamingUnsignedMedian,
}

impl WuMetricsAvg {
    pub fn new() -> Self {
        Self {
            last_wu_params: Default::default(),
            last_shard_accounts_count: 0,
            had_pending_messages: true,
            avg: VecOfStreamingUnsignedMedian::new(53),
        }
    }

    pub fn accum(&mut self, v: &WuMetrics) {
        self.last_wu_params = v.wu_params.clone();
        self.had_pending_messages = self.had_pending_messages && v.has_pending_messages;

        self.last_shard_accounts_count = v.wu_on_finalize.shard_accounts_count;

        self.avg.accum(0, v.wu_on_finalize.updated_accounts_count);
        self.avg.accum_next(v.wu_on_finalize.in_msgs_len);
        self.avg.accum_next(v.wu_on_finalize.out_msgs_len);
        self.avg.accum_next(v.wu_on_execute.inserted_new_msgs_count);

        // wu_on_prepare_msg_groups
        self.avg.accum_next(v.wu_on_prepare_msg_groups.fixed_part);
        self.avg
            .accum_next(v.wu_on_prepare_msg_groups.read_ext_msgs_count);
        self.avg
            .accum_next(v.wu_on_prepare_msg_groups.read_ext_msgs_wu);
        self.avg
            .accum_next(v.wu_on_prepare_msg_groups.read_ext_msgs_elapsed.as_nanos());
        self.avg
            .accum_next(v.wu_on_prepare_msg_groups.read_existing_int_msgs_count);
        self.avg
            .accum_next(v.wu_on_prepare_msg_groups.read_existing_int_msgs_wu);
        self.avg.accum_next(
            v.wu_on_prepare_msg_groups
                .read_existing_int_msgs_elapsed
                .as_nanos(),
        );
        self.avg
            .accum_next(v.wu_on_prepare_msg_groups.read_new_int_msgs_count);
        self.avg
            .accum_next(v.wu_on_prepare_msg_groups.read_new_int_msgs_wu);
        self.avg.accum_next(
            v.wu_on_prepare_msg_groups
                .read_new_int_msgs_elapsed
                .as_nanos(),
        );
        self.avg
            .accum_next(v.wu_on_prepare_msg_groups.add_to_msgs_groups_ops_count);
        self.avg
            .accum_next(v.wu_on_prepare_msg_groups.add_msgs_to_groups_wu);
        self.avg.accum_next(
            v.wu_on_prepare_msg_groups
                .add_msgs_to_groups_elapsed
                .as_nanos(),
        );
        self.avg
            .accum_next(v.wu_on_prepare_msg_groups.total_elapsed.as_nanos());

        // wu_on_execute
        self.avg.accum_next(v.wu_on_execute.groups_count);
        self.avg.accum_next(v.wu_on_execute.sum_gas);
        self.avg.accum_next(
            v.wu_on_execute
                .avg_group_accounts_count
                .get_avg_checked()
                .unwrap_or_default(),
        );
        self.avg.accum_next(
            v.wu_on_execute
                .avg_threads_count
                .get_avg_checked()
                .unwrap_or_default(),
        );
        self.avg
            .accum_next(v.wu_on_execute.execute_groups_vm_only_wu);
        self.avg
            .accum_next(v.wu_on_execute.execute_groups_vm_only_elapsed.as_nanos());
        self.avg.accum_next(v.wu_on_execute.process_txs_wu);
        self.avg
            .accum_next(v.wu_on_execute.process_txs_elapsed.as_nanos());

        // wu_on_finalize
        self.avg.accum_next(v.wu_on_finalize.diff_msgs_count);
        self.avg.accum_next(v.wu_on_finalize.create_queue_diff_wu);
        self.avg
            .accum_next(v.wu_on_finalize.create_queue_diff_elapsed.as_nanos());
        self.avg.accum_next(v.wu_on_finalize.apply_queue_diff_wu);
        self.avg
            .accum_next(v.wu_on_finalize.apply_queue_diff_elapsed.as_nanos());
        self.avg
            .accum_next(v.wu_on_finalize.update_shard_accounts_wu);
        self.avg
            .accum_next(v.wu_on_finalize.update_shard_accounts_elapsed.as_nanos());
        self.avg
            .accum_next(v.wu_on_finalize.build_accounts_blocks_wu);
        self.avg
            .accum_next(v.wu_on_finalize.build_accounts_blocks_elapsed.as_nanos());
        self.avg
            .accum_next(v.wu_on_finalize.build_accounts_elapsed.as_nanos());
        self.avg.accum_next(v.wu_on_finalize.build_in_msgs_wu);
        self.avg
            .accum_next(v.wu_on_finalize.build_in_msgs_elapsed.as_nanos());
        self.avg.accum_next(v.wu_on_finalize.build_out_msgs_wu);
        self.avg
            .accum_next(v.wu_on_finalize.build_out_msgs_elapsed.as_nanos());
        self.avg.accum_next(
            v.wu_on_finalize
                .build_accounts_and_messages_in_parallel_elased
                .as_nanos(),
        );
        self.avg.accum_next(v.wu_on_finalize.build_state_update_wu);
        self.avg
            .accum_next(v.wu_on_finalize.build_state_update_elapsed.as_nanos());
        self.avg.accum_next(v.wu_on_finalize.build_block_wu);
        self.avg
            .accum_next(v.wu_on_finalize.build_block_elapsed.as_nanos());
        self.avg
            .accum_next(v.wu_on_finalize.finalize_block_elapsed.as_nanos());
        self.avg
            .accum_next(v.wu_on_finalize.total_elapsed.as_nanos());

        // wu_on_do_collate
        self.avg.accum_next(v.wu_on_do_collate.resume_collation_wu);
        self.avg
            .accum_next(v.wu_on_do_collate.resume_collation_elapsed.as_nanos());
        self.avg
            .accum_next(v.wu_on_do_collate.resume_collation_wu_per_block);
        self.avg
            .accum_next(v.wu_on_do_collate.resume_collation_elapsed_per_block_ns);
        self.avg
            .accum_next(v.wu_on_do_collate.collation_total_elapsed.as_nanos());
    }

    pub fn get_avg(&mut self) -> Option<WuMetrics> {
        let updated_accounts_count = self.avg.get_avg_checked(0).map(|v| v as u64)?;

        let in_msgs_len = self.avg.get_avg_next() as u64;
        let out_msgs_len = self.avg.get_avg_next() as u64;
        let inserted_new_msgs_count = self.avg.get_avg_next() as u64;

        Some(WuMetrics {
            wu_params: self.last_wu_params.clone(),
            wu_on_prepare_msg_groups: PrepareMsgGroupsWu {
                fixed_part: self.avg.get_avg_next() as u64,
                read_ext_msgs_count: self.avg.get_avg_next() as u64,
                read_ext_msgs_wu: self.avg.get_avg_next() as u64,
                read_ext_msgs_elapsed: Duration::from_nanos(self.avg.get_avg_next() as u64),
                read_existing_int_msgs_count: self.avg.get_avg_next() as u64,
                read_existing_int_msgs_wu: self.avg.get_avg_next() as u64,
                read_existing_int_msgs_elapsed: Duration::from_nanos(self.avg.get_avg_next() as u64),
                read_new_int_msgs_count: self.avg.get_avg_next() as u64,
                read_new_int_msgs_wu: self.avg.get_avg_next() as u64,
                read_new_int_msgs_elapsed: Duration::from_nanos(self.avg.get_avg_next() as u64),
                add_to_msgs_groups_ops_count: self.avg.get_avg_next() as u64,
                add_msgs_to_groups_wu: self.avg.get_avg_next() as u64,
                add_msgs_to_groups_elapsed: Duration::from_nanos(self.avg.get_avg_next() as u64),
                total_elapsed: Duration::from_nanos(self.avg.get_avg_next() as u64),
            },
            wu_on_execute: ExecuteWu {
                in_msgs_len,
                out_msgs_len,
                inserted_new_msgs_count,
                groups_count: self.avg.get_avg_next() as u64,
                sum_gas: self.avg.get_avg_next(),
                avg_group_accounts_count: SafeUnsignedAvg::with_initial(self.avg.get_avg_next()),
                avg_threads_count: SafeUnsignedAvg::with_initial(self.avg.get_avg_next()),
                execute_groups_vm_only_wu: self.avg.get_avg_next() as u64,
                execute_groups_vm_only_elapsed: Duration::from_nanos(self.avg.get_avg_next() as u64),
                process_txs_wu: self.avg.get_avg_next() as u64,
                process_txs_elapsed: Duration::from_nanos(self.avg.get_avg_next() as u64),
            },
            wu_on_finalize: FinalizeWu {
                shard_accounts_count: self.last_shard_accounts_count,
                diff_msgs_count: self.avg.get_avg_next() as u64,
                create_queue_diff_wu: self.avg.get_avg_next() as u64,
                create_queue_diff_elapsed: Duration::from_nanos(self.avg.get_avg_next() as u64),
                apply_queue_diff_wu: self.avg.get_avg_next() as u64,
                apply_queue_diff_elapsed: Duration::from_nanos(self.avg.get_avg_next() as u64),
                updated_accounts_count,
                in_msgs_len,
                out_msgs_len,
                update_shard_accounts_wu: self.avg.get_avg_next() as u64,
                update_shard_accounts_elapsed: Duration::from_nanos(self.avg.get_avg_next() as u64),
                build_accounts_blocks_wu: self.avg.get_avg_next() as u64,
                build_accounts_blocks_elapsed: Duration::from_nanos(self.avg.get_avg_next() as u64),
                build_accounts_elapsed: Duration::from_nanos(self.avg.get_avg_next() as u64),
                build_in_msgs_wu: self.avg.get_avg_next() as u64,
                build_in_msgs_elapsed: Duration::from_nanos(self.avg.get_avg_next() as u64),
                build_out_msgs_wu: self.avg.get_avg_next() as u64,
                build_out_msgs_elapsed: Duration::from_nanos(self.avg.get_avg_next() as u64),
                build_accounts_and_messages_in_parallel_elased: Duration::from_nanos(
                    self.avg.get_avg_next() as u64,
                ),
                build_state_update_wu: self.avg.get_avg_next() as u64,
                build_state_update_elapsed: Duration::from_nanos(self.avg.get_avg_next() as u64),
                build_block_wu: self.avg.get_avg_next() as u64,
                build_block_elapsed: Duration::from_nanos(self.avg.get_avg_next() as u64),
                finalize_block_elapsed: Duration::from_nanos(self.avg.get_avg_next() as u64),
                total_elapsed: Duration::from_nanos(self.avg.get_avg_next() as u64),
            },
            wu_on_do_collate: DoCollateWu {
                shard_accounts_count: self.last_shard_accounts_count,
                updated_accounts_count,
                resume_collation_wu: self.avg.get_avg_next() as u64,
                resume_collation_elapsed: Duration::from_nanos(self.avg.get_avg_next() as u64),
                resume_collation_wu_per_block: self.avg.get_avg_next() as u64,
                resume_collation_elapsed_per_block_ns: self.avg.get_avg_next(),
                collation_total_elapsed: Duration::from_nanos(self.avg.get_avg_next() as u64),
            },
            has_pending_messages: self.had_pending_messages,
        })
    }
}

fn safe_anchors_lag_avg<'a, I>(range: I) -> Option<i64>
where
    I: Iterator<Item = (&'a u32, &'a mut AnchorsLagSpan)>,
{
    let mut avg = SafeSignedAvg::default();
    for (_, v) in range {
        avg.accum(v.get_result());
    }

    avg.get_avg_checked().map(|v| v as i64)
}

fn safe_wu_params_avg<'a, I>(range: I) -> Option<WorkUnitsParams>
where
    I: Iterator<Item = (&'a u32, &'a WorkUnitsParams)>,
{
    let mut avg = VecOfStreamingUnsignedMedian::new(26);

    for (_, v) in range {
        avg.accum(0, v.prepare.fixed_part);
        avg.accum_next(v.prepare.read_ext_msgs);
        avg.accum_next(v.prepare.read_int_msgs);
        avg.accum_next(v.prepare.read_new_msgs);
        avg.accum_next(v.prepare.add_to_msg_groups);

        avg.accum_next(v.execute.prepare);
        avg.accum_next(v.execute.execute);
        avg.accum_next(v.execute.execute_delimiter);
        avg.accum_next(v.execute.serialize_enqueue);
        avg.accum_next(v.execute.serialize_dequeue);
        avg.accum_next(v.execute.insert_new_msgs);
        avg.accum_next(v.execute.subgroup_size);

        avg.accum_next(v.finalize.build_transactions);
        avg.accum_next(v.finalize.build_accounts);
        avg.accum_next(v.finalize.build_in_msg);
        avg.accum_next(v.finalize.build_out_msg);
        avg.accum_next(v.finalize.state_update_min);
        avg.accum_next(v.finalize.state_update_accounts);
        avg.accum_next(v.finalize.state_update_msg);
        avg.accum_next(v.finalize.serialize_diff);
        avg.accum_next(v.finalize.serialize_min);
        avg.accum_next(v.finalize.serialize_accounts);
        avg.accum_next(v.finalize.serialize_msg);
        avg.accum_next(v.finalize.create_diff);
        avg.accum_next(v.finalize.apply_diff);
        avg.accum_next(v.finalize.diff_tail_len);
    }

    let fixed_part = avg.get_avg_checked(0).map(|v| v as u32)?;

    Some(WorkUnitsParams {
        prepare: WorkUnitsParamsPrepare {
            fixed_part,
            msgs_stats: 0,
            remaning_msgs_stats: 0,
            read_ext_msgs: avg.get_avg_next() as u16,
            read_int_msgs: avg.get_avg_next() as u16,
            read_new_msgs: avg.get_avg_next() as u16,
            add_to_msg_groups: avg.get_avg_next() as u16,
        },
        execute: WorkUnitsParamsExecute {
            prepare: avg.get_avg_next() as u32,
            execute: avg.get_avg_next() as u16,
            execute_err: 0,
            execute_delimiter: avg.get_avg_next() as u32,
            serialize_enqueue: avg.get_avg_next() as u16,
            serialize_dequeue: avg.get_avg_next() as u16,
            insert_new_msgs: avg.get_avg_next() as u16,
            subgroup_size: avg.get_avg_next() as u16,
        },
        finalize: WorkUnitsParamsFinalize {
            build_transactions: avg.get_avg_next() as u16,
            build_accounts: avg.get_avg_next() as u16,
            build_in_msg: avg.get_avg_next() as u16,
            build_out_msg: avg.get_avg_next() as u16,
            state_update_min: avg.get_avg_next() as u32,
            state_update_accounts: avg.get_avg_next() as u16,
            state_update_msg: avg.get_avg_next() as u16,
            serialize_diff: avg.get_avg_next() as u16,
            serialize_min: avg.get_avg_next() as u32,
            serialize_accounts: avg.get_avg_next() as u16,
            serialize_msg: avg.get_avg_next() as u16,
            create_diff: avg.get_avg_next() as u16,
            apply_diff: avg.get_avg_next() as u16,
            diff_tail_len: avg.get_avg_next() as u16,
        },
    })
}

fn report_target_wu_price(prev_wu_price: f64, target_wu_price: f64) {
    metrics::gauge!("tycho_do_collate_wu_tuner_prev_wu_price").set(prev_wu_price);
    metrics::gauge!("tycho_do_collate_wu_tuner_target_wu_price").set(target_wu_price);
}

fn report_wu_params(curr_wu_params: &WorkUnitsParams, target_wu_params: &WorkUnitsParams) {
    // read msgs and prepare msgs groups
    metrics::gauge!("tycho_do_collate_wu_param_prepare_fixed_part_curr")
        .set(curr_wu_params.prepare.fixed_part as f64);
    metrics::gauge!("tycho_do_collate_wu_param_prepare_fixed_part_target")
        .set(target_wu_params.prepare.fixed_part as f64);
    metrics::gauge!("tycho_do_collate_wu_param_prepare_read_ext_msgs_curr")
        .set(curr_wu_params.prepare.read_ext_msgs as f64);
    metrics::gauge!("tycho_do_collate_wu_param_prepare_read_ext_msgs_target")
        .set(target_wu_params.prepare.read_ext_msgs as f64);
    metrics::gauge!("tycho_do_collate_wu_param_prepare_read_int_msgs_curr")
        .set(curr_wu_params.prepare.read_int_msgs as f64);
    metrics::gauge!("tycho_do_collate_wu_param_prepare_read_int_msgs_target")
        .set(target_wu_params.prepare.read_int_msgs as f64);
    metrics::gauge!("tycho_do_collate_wu_param_prepare_read_new_msgs_curr")
        .set(curr_wu_params.prepare.read_new_msgs as f64);
    metrics::gauge!("tycho_do_collate_wu_param_prepare_read_new_msgs_target")
        .set(target_wu_params.prepare.read_new_msgs as f64);
    metrics::gauge!("tycho_do_collate_wu_param_prepare_add_to_msg_groups_curr")
        .set(curr_wu_params.prepare.add_to_msg_groups as f64);
    metrics::gauge!("tycho_do_collate_wu_param_prepare_add_to_msg_groups_target")
        .set(target_wu_params.prepare.add_to_msg_groups as f64);

    // execute
    metrics::gauge!("tycho_do_collate_wu_param_execute_prepare_curr")
        .set(curr_wu_params.execute.prepare as f64);
    metrics::gauge!("tycho_do_collate_wu_param_execute_prepare_target")
        .set(target_wu_params.execute.prepare as f64);
    metrics::gauge!("tycho_do_collate_wu_param_execute_execute_curr")
        .set(curr_wu_params.execute.execute as f64);
    metrics::gauge!("tycho_do_collate_wu_param_execute_execute_target")
        .set(target_wu_params.execute.execute as f64);
    metrics::gauge!("tycho_do_collate_wu_param_execute_execute_delimiter_curr")
        .set(curr_wu_params.execute.execute_delimiter as f64);
    metrics::gauge!("tycho_do_collate_wu_param_execute_execute_delimiter_target")
        .set(target_wu_params.execute.execute_delimiter as f64);
    metrics::gauge!("tycho_do_collate_wu_param_execute_insert_in_msg_curr")
        .set(curr_wu_params.execute.serialize_enqueue as f64);
    metrics::gauge!("tycho_do_collate_wu_param_execute_insert_in_msg_target")
        .set(target_wu_params.execute.serialize_enqueue as f64);
    metrics::gauge!("tycho_do_collate_wu_param_execute_insert_out_msg_curr")
        .set(curr_wu_params.execute.serialize_dequeue as f64);
    metrics::gauge!("tycho_do_collate_wu_param_execute_insert_out_msg_target")
        .set(target_wu_params.execute.serialize_dequeue as f64);
    metrics::gauge!("tycho_do_collate_wu_param_execute_insert_new_msg_curr")
        .set(curr_wu_params.execute.insert_new_msgs as f64);
    metrics::gauge!("tycho_do_collate_wu_param_execute_insert_new_msg_target")
        .set(target_wu_params.execute.insert_new_msgs as f64);
    metrics::gauge!("tycho_do_collate_wu_param_execute_max_threads_curr")
        .set(curr_wu_params.execute.subgroup_size as f64);
    metrics::gauge!("tycho_do_collate_wu_param_execute_max_threads_target")
        .set(target_wu_params.execute.subgroup_size as f64);

    // finalize
    metrics::gauge!("tycho_do_collate_wu_param_finalize_build_transactions_curr")
        .set(curr_wu_params.finalize.build_transactions as f64);
    metrics::gauge!("tycho_do_collate_wu_param_finalize_build_transactions_target")
        .set(target_wu_params.finalize.build_transactions as f64);
    metrics::gauge!("tycho_do_collate_wu_param_finalize_build_accounts_curr")
        .set(curr_wu_params.finalize.build_accounts as f64);
    metrics::gauge!("tycho_do_collate_wu_param_finalize_build_accounts_target")
        .set(target_wu_params.finalize.build_accounts as f64);
    metrics::gauge!("tycho_do_collate_wu_param_finalize_build_in_msg_curr")
        .set(curr_wu_params.finalize.build_in_msg as f64);
    metrics::gauge!("tycho_do_collate_wu_param_finalize_build_in_msg_target")
        .set(target_wu_params.finalize.build_in_msg as f64);
    metrics::gauge!("tycho_do_collate_wu_param_finalize_build_out_msg_curr")
        .set(curr_wu_params.finalize.build_out_msg as f64);
    metrics::gauge!("tycho_do_collate_wu_param_finalize_build_out_msg_target")
        .set(target_wu_params.finalize.build_out_msg as f64);
    metrics::gauge!("tycho_do_collate_wu_param_finalize_serialize_min_curr")
        .set(curr_wu_params.finalize.serialize_min as f64);
    metrics::gauge!("tycho_do_collate_wu_param_finalize_serialize_min_target")
        .set(target_wu_params.finalize.serialize_min as f64);
    metrics::gauge!("tycho_do_collate_wu_param_finalize_serialize_accounts_curr")
        .set(curr_wu_params.finalize.serialize_accounts as f64);
    metrics::gauge!("tycho_do_collate_wu_param_finalize_serialize_accounts_target")
        .set(target_wu_params.finalize.serialize_accounts as f64);
    metrics::gauge!("tycho_do_collate_wu_param_finalize_serialize_msg_curr")
        .set(curr_wu_params.finalize.serialize_msg as f64);
    metrics::gauge!("tycho_do_collate_wu_param_finalize_serialize_msg_target")
        .set(target_wu_params.finalize.serialize_msg as f64);
    metrics::gauge!("tycho_do_collate_wu_param_finalize_state_update_min_curr")
        .set(curr_wu_params.finalize.state_update_min as f64);
    metrics::gauge!("tycho_do_collate_wu_param_finalize_state_update_min_target")
        .set(target_wu_params.finalize.state_update_min as f64);
    metrics::gauge!("tycho_do_collate_wu_param_finalize_state_update_accounts_curr")
        .set(curr_wu_params.finalize.state_update_accounts as f64);
    metrics::gauge!("tycho_do_collate_wu_param_finalize_state_update_accounts_target")
        .set(target_wu_params.finalize.state_update_accounts as f64);
    metrics::gauge!("tycho_do_collate_wu_param_finalize_state_pow_coeff_curr")
        .set(curr_wu_params.finalize.state_update_msg as f64);
    metrics::gauge!("tycho_do_collate_wu_param_finalize_state_pow_coeff_target")
        .set(target_wu_params.finalize.state_update_msg as f64);
    metrics::gauge!("tycho_do_collate_wu_param_finalize_updated_accounts_pow_coeff_curr")
        .set(curr_wu_params.finalize.serialize_diff as f64);
    metrics::gauge!("tycho_do_collate_wu_param_finalize_updated_accounts_pow_coeff_target")
        .set(target_wu_params.finalize.serialize_diff as f64);
    metrics::gauge!("tycho_do_collate_wu_param_finalize_create_diff_curr")
        .set(curr_wu_params.finalize.create_diff as f64);
    metrics::gauge!("tycho_do_collate_wu_param_finalize_create_diff_target")
        .set(target_wu_params.finalize.create_diff as f64);
    metrics::gauge!("tycho_do_collate_wu_param_finalize_apply_diff_curr")
        .set(curr_wu_params.finalize.apply_diff as f64);
    metrics::gauge!("tycho_do_collate_wu_param_finalize_apply_diff_target")
        .set(target_wu_params.finalize.apply_diff as f64);
    metrics::gauge!("tycho_do_collate_wu_param_finalize_resume_collation_curr")
        .set(curr_wu_params.finalize.diff_tail_len as f64);
    metrics::gauge!("tycho_do_collate_wu_param_finalize_resume_collation_target")
        .set(target_wu_params.finalize.diff_tail_len as f64);
}
