use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;

use anyhow::Result;
use tycho_collator::collator::work_units::{
    ExecuteWu, calc_target_scaled_wu_param_from_sums, calc_target_wu_param_from_sums,
    report_anchor_lag_to_metrics,
};
use tycho_collator::types::processed_upto::BlockSeqno;
use tycho_types::models::{ShardIdent, WorkUnitsParams};
use tycho_util::FastHashSet;
use tycho_util::num::{RollingPercentiles, SafeSignedAvg};

use crate::config::{WuTuneType, WuTunerConfig};
use crate::types::WuMetricsAvg;
use crate::unit_cost_clipper::UnitCostClippers;
use crate::updater::WuParamsUpdater;
use crate::{MempoolAnchorLag, WuEvent, WuEventData, WuMetrics};

pub struct WuMetricsSpan {
    pub avg: Box<WuMetricsAvg>,
    pub last: Box<WuMetrics>,
}

impl WuMetricsSpan {
    pub fn new(metrics: Box<WuMetrics>, unit_cost_clippers: &mut UnitCostClippers) -> Self {
        let mut avg = WuMetricsAvg::new();
        // materialize the first sample immediately so later MA merges can work on span-level aggregates
        avg.accum(&metrics, unit_cost_clippers);
        Self {
            avg: Box::new(avg),
            last: metrics,
        }
    }

    pub fn accum(
        &mut self,
        v: Box<WuMetrics>,
        unit_cost_clippers: &mut UnitCostClippers,
    ) -> Result<()> {
        self.avg.accum(&v, unit_cost_clippers);
        // keep the latest raw sample because param-change detection compares against the newest input
        self.last = v;
        Ok(())
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

    pub fn accum<F>(&mut self, v: MempoolAnchorLag, seqno: u32, clip_value: F) -> Result<()>
    where
        F: FnMut(i64) -> i64,
    {
        if self.last_seqno != seqno {
            // accum lag value only for the last anchor after block seqno
            self.accum_last(clip_value)?;
            self.last_seqno = seqno;
        }
        // keep only the latest anchor inside one block seqno so repeated imports do not overweight the bucket
        self.last = Some(v);
        Ok(())
    }

    fn accum_last<F>(&mut self, mut clip_value: F) -> Result<()>
    where
        F: FnMut(i64) -> i64,
    {
        if let Some(last) = self.last.take() {
            // clip each finalized lag sample before it joins the seqno bucket average
            let lag_clip = clip_value(last.lag());
            match &mut self.avg {
                AnchorsLagSpanValue::Accum(accum) => accum.accum(lag_clip),
                AnchorsLagSpanValue::Result(_) => {
                    anyhow::bail!("AnchorsLagSpan.avg should be Accum here")
                }
            }
        }
        Ok(())
    }

    pub fn get_result<F>(&mut self, mut clip_value: F) -> i64
    where
        F: FnMut(i64) -> i64,
    {
        if let AnchorsLagSpanValue::Accum(accum) = &mut self.avg {
            if let Some(last) = self.last.take() {
                let lag_clip = clip_value(last.lag());
                accum.accum(lag_clip);
            }
            // freeze the result on first read so repeated consumers do not re-add the last sample
            self.avg = AnchorsLagSpanValue::Result(accum.get_avg() as i64);
        }

        let AnchorsLagSpanValue::Result(avg) = self.avg else {
            unreachable!()
        };

        avg
    }
}

const DEFAULT_PERCENTILE_WINDOW: usize = 100;

pub struct WuHistory {
    metrics: BTreeMap<BlockSeqno, WuMetricsSpan>,
    metrics_min_seqno: Option<u32>,
    avg_metrics: BTreeMap<BlockSeqno, WuMetricsAvg>,
    avg_metrics_last_calculated_on_seqno: u32,
    anchors_lag: BTreeMap<BlockSeqno, AnchorsLagSpan>,
    anchors_lag_min_seqno: Option<u32>,
    anchors_lag_pct: RollingPercentiles<i64>,
    unit_cost_clippers: UnitCostClippers,
    avg_anchors_lag: BTreeMap<BlockSeqno, i64>,
    avg_anchors_lag_last_calculated_on_seqno: u32,
}

impl WuHistory {
    fn new(percentile_window: usize) -> Self {
        let anchors_lag_pct = RollingPercentiles::new(percentile_window);
        let unit_cost_clippers = UnitCostClippers::new(percentile_window);
        Self {
            metrics: Default::default(),
            metrics_min_seqno: None,
            avg_metrics: Default::default(),
            avg_metrics_last_calculated_on_seqno: Default::default(),
            anchors_lag: Default::default(),
            anchors_lag_min_seqno: None,
            anchors_lag_pct,
            unit_cost_clippers,
            avg_anchors_lag: Default::default(),
            avg_anchors_lag_last_calculated_on_seqno: Default::default(),
        }
    }

    fn clear(&mut self) {
        // preserve the configured window length across resets triggered by gaps or param changes
        let percentile_window = self.anchors_lag_pct.window();
        *self = Self::new(percentile_window);
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

#[allow(dead_code)]
#[derive(Clone, Copy)]
struct TunerParams {
    pub wu_span: u32,
    pub wu_ma_interval: u32,
    pub wu_ma_range: u32,

    pub lag_span: u32,
    pub lag_ma_interval: u32,
    pub lag_ma_range: u32,

    pub wu_params_calc_interval: u32,

    pub tune_interval: u32,

    pub tune_seqno: u32,

    pub wu_span_seqno: u32,
    pub wu_ma_seqno: u32,
    pub lag_span_seqno: u32,
    pub lag_ma_seqno: u32,

    pub wu_params_calc_seqno: u32,
}

impl TunerParams {
    fn calculate(config: &WuTunerConfig, seqno: BlockSeqno) -> Self {
        let wu_span = config.wu_span as u32;
        let wu_ma_spans = config.wu_ma_spans as u32;
        let wu_ma_interval = wu_ma_spans.saturating_mul(wu_span);
        let wu_ma_range = config.wu_ma_range as u32;

        let lag_span = config.lag_span as u32;
        let lag_ma_spans = config.lag_ma_spans as u32;
        let lag_ma_interval = lag_ma_spans.saturating_mul(lag_span);
        let lag_ma_range = config.lag_ma_range as u32;

        let wu_params_calc_interval = config.wu_params_calc_interval as u32;

        let tune_interval = config.tune_interval as u32;

        // floor all buckets to deterministic boundaries so lag and wu MAs can be matched by seqno
        let tune_seqno = seqno / tune_interval * tune_interval;

        // normilized seqno for calculations
        // e.g. seqno = 254, wu_span = 10, wu_ma_spans = 2, lag_span = 5, lag_ma_spans = 2, wu_params_ma_interval = 40
        let wu_span_seqno = seqno / wu_span * wu_span; // 250
        let wu_ma_seqno = seqno / wu_ma_interval * wu_ma_interval; // 240
        let lag_span_seqno = seqno / lag_span * lag_span; // 250
        let lag_ma_seqno = seqno / lag_ma_interval * lag_ma_interval; // 250

        let wu_params_calc_seqno = seqno / wu_params_calc_interval * wu_params_calc_interval; // 240

        Self {
            wu_span,
            wu_ma_interval,
            wu_ma_range,
            lag_span,
            lag_ma_interval,
            lag_ma_range,
            wu_params_calc_interval,
            tune_interval,
            tune_seqno,
            wu_span_seqno,
            wu_ma_seqno,
            lag_span_seqno,
            lag_ma_seqno,
            wu_params_calc_seqno,
        }
    }
}

pub struct WuTuner<U>
where
    U: WuParamsUpdater,
{
    config: Arc<WuTunerConfig>,
    updater: U,
    history: BTreeMap<ShardIdent, WuHistory>,
    wu_params_last_calculated_on_seqno: u32,
    wu_params_last_updated_on_seqno: u32,
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
            wu_params_last_calculated_on_seqno: 0,
            wu_params_last_updated_on_seqno: 0,
            wu_once_reported: Default::default(),
            lag_once_reported: false,
            adjustments: Default::default(),
        }
    }

    pub fn update_config(&mut self, config: Arc<WuTunerConfig>) {
        self.config = config;
        self.history.clear();
    }

    pub fn pause_on_invalid_config(&mut self) {
        self.report_zero_metrics();
        self.history.clear();
        self.wu_once_reported.clear();
        self.lag_once_reported = false;
        self.adjustments.clear();
    }

    fn report_zero_metrics(&self) {
        let shards: Vec<_> = self.history.keys().cloned().collect();
        for shard in shards {
            WuMetrics::default().report_metrics(&shard);
            report_anchor_lag_to_metrics(&shard, 0);
        }

        report_zero_wu_price();
        report_wu_params(&WorkUnitsParams::default(), &WorkUnitsParams::default());
    }

    pub async fn handle_wu_event(&mut self, event: WuEvent) -> Result<()> {
        let WuEvent { shard, seqno, data } = event;

        match data {
            WuEventData::Metrics(metrics) => self.handle_wu_metrics(shard, seqno, metrics)?,
            WuEventData::AnchorLag(anchor_lag) => {
                self.handle_anchor_lag(shard, seqno, anchor_lag).await?;
            }
        }

        Ok(())
    }

    fn handle_wu_metrics(
        &mut self,
        shard: ShardIdent,
        seqno: BlockSeqno,
        metrics: Box<WuMetrics>,
    ) -> Result<()> {
        let TunerParams {
            wu_span,
            wu_ma_range,
            tune_interval,
            wu_span_seqno,
            wu_ma_seqno,
            lag_ma_seqno,
            ..
        } = TunerParams::calculate(&self.config, seqno);

        let history = self
            .history
            .entry(shard)
            .or_insert_with(|| WuHistory::new(DEFAULT_PERCENTILE_WINDOW));

        let has_pending_messages = metrics.has_pending_messages;

        // drop history on a gap in metrics
        if let Some((&last_key, _)) = history.metrics.last_key_value() {
            // e.g. (240 + 10 < 244) == false
            if last_key + wu_span < seqno {
                history.clear();

                tracing::info!(
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

            // report updated wu params to metrics
            report_wu_params(&metrics.wu_params, &metrics.wu_params);

            // we update wu params by shard blocks data, so we remember ashard block seqno of the last update
            // for futher check for the minimal interval before the next update
            if !shard.is_masterchain() {
                self.wu_params_last_updated_on_seqno = seqno;
            }
        }

        // on start set wu params last updated on current shard block seqno
        if self.wu_params_last_updated_on_seqno == 0 && !shard.is_masterchain() {
            self.wu_params_last_updated_on_seqno = seqno;
        }

        // update history
        let (metrics_history, unit_cost_clippers) =
            (&mut history.metrics, &mut history.unit_cost_clippers);
        match metrics_history.entry(wu_span_seqno) {
            std::collections::btree_map::Entry::Vacant(vacant) => {
                vacant.insert(WuMetricsSpan::new(metrics, unit_cost_clippers));
            }
            std::collections::btree_map::Entry::Occupied(mut occupied) => {
                let span = occupied.get_mut();
                span.accum(metrics, unit_cost_clippers)?;
            }
        }
        let wu_metrics_history_min_seqno = *history.metrics_min_seqno.get_or_insert(seqno);

        tracing::trace!(
            %shard, seqno,
            has_pending_messages,
            metrics_history_len = history.metrics.len(),
            "wu metrics received",
        );

        // calculate MA wu metrics both on wu MA seqno and anchors lag MA seqno
        // use the newer boundary so wu metrics can later join the lag bucket picked for tuning
        let ma_seqno = std::cmp::max(wu_ma_seqno, lag_ma_seqno);

        // avoid the MA recalculation on the same bucket
        if ma_seqno == 0 || history.avg_metrics_last_calculated_on_seqno == ma_seqno {
            return Ok(());
        }

        // do not calculate MA until the required range of samples is collected
        if ma_seqno.saturating_sub(wu_metrics_history_min_seqno) < wu_ma_range {
            return Ok(());
        }

        // e.g. seqno = 240 -> avg_range = [140..240)
        let avg_from_boundary = ma_seqno.saturating_sub(wu_ma_range);
        let avg_range = history.metrics.range_mut((
            Bound::Included(avg_from_boundary),
            Bound::Excluded(ma_seqno),
        ));
        let Some(avg) = safe_metrics_avg(avg_range) else {
            return Ok(());
        };
        let Some(avg_metrics_res) = avg.get_avg() else {
            return Ok(());
        };

        // report avg wu metrics
        avg_metrics_res.report_metrics(&shard);

        tracing::trace!(
            %shard, seqno,
            "avg wu metrics calculated on [{0}..{1})",
            avg_from_boundary, ma_seqno,
        );

        // store avg wu metrics
        history.avg_metrics.insert(ma_seqno, avg);

        history.avg_metrics_last_calculated_on_seqno = ma_seqno;

        // clear outdated history
        let gc_boundary = wu_ma_seqno.saturating_sub(tune_interval); // e.g. seqno = 240 -> gc_boundary = 40
        if let Some((&first_key, _)) = history.metrics.first_key_value()
            && first_key < gc_boundary
        {
            history.gc_wu_metrics(gc_boundary);

            tracing::trace!(
                %shard, seqno,
                "wu metrics history gc < {0}",
                gc_boundary,
            );
        }

        Ok(())
    }

    async fn handle_anchor_lag(
        &mut self,
        shard: ShardIdent,
        seqno: BlockSeqno,
        anchor_lag: MempoolAnchorLag,
    ) -> Result<()> {
        let tuner_params = TunerParams::calculate(&self.config, seqno);
        let TunerParams {
            lag_ma_range,
            tune_interval,
            lag_span_seqno,
            lag_ma_seqno,
            ..
        } = tuner_params;

        let history = self
            .history
            .entry(shard)
            .or_insert_with(|| WuHistory::new(DEFAULT_PERCENTILE_WINDOW));

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
                span.accum(anchor_lag.clone(), seqno, |lag| {
                    // clip anchor lag by P1..P99 percentile
                    history.anchors_lag_pct.push_and_clip(lag, 1, 99)
                })?;
            }
        }
        let anchors_lag_history_min_seqno = *history.anchors_lag_min_seqno.get_or_insert(seqno);

        tracing::trace!(
            %shard, seqno,
            lag = anchor_lag.lag(),
            lag_bounds = ?self.config.lag_bounds_ms,
            lag_history_len = history.anchors_lag.len(),
            "anchor lag received",
        );

        // calculate MA lag
        // avoid the MA recalculation on the same bucket
        if lag_ma_seqno == 0 || history.avg_anchors_lag_last_calculated_on_seqno == lag_ma_seqno {
            return Ok(());
        }

        // do not calculate MA until the required range of samples is collected
        if lag_ma_seqno.saturating_sub(anchors_lag_history_min_seqno) < lag_ma_range {
            return Ok(());
        }

        // e.g. seqno = 244 -> avg_range = [140..240)
        let avg_from_boundary = lag_ma_seqno.saturating_sub(lag_ma_range);
        let avg_range = history.anchors_lag.range_mut((
            Bound::Included(avg_from_boundary),
            Bound::Excluded(lag_ma_seqno),
        ));
        let Some(avg_lag) = safe_anchors_lag_avg(avg_range, |lag| {
            // clip anchor lag by P1..P99 percentile
            history.anchors_lag_pct.push_and_clip(lag, 1, 99)
        }) else {
            return Ok(());
        };

        // report avg anchor importing lag to metrics
        report_anchor_lag_to_metrics(&shard, avg_lag);

        tracing::debug!(
            %shard, seqno,
            avg_lag,
            lag_bounds = ?self.config.lag_bounds_ms,
            "avg anchor lag calculated on [{0}..{1})",
            avg_from_boundary, lag_ma_seqno,
        );

        // do not process MA until the percentile windows is filled
        // lag clipping must be warmed up before its MA can drive wu tuning decisions
        if !history.anchors_lag_pct.window_is_filled() {
            return Ok(());
        }

        // store avg lag
        history.avg_anchors_lag.insert(lag_ma_seqno, avg_lag);

        history.avg_anchors_lag_last_calculated_on_seqno = lag_ma_seqno;

        // clear outdated history
        let gc_boundary = lag_ma_seqno.saturating_sub(tune_interval); // e.g. seqno = 244 -> gc_boundary = 40
        if let Some((&first_key, _)) = history.anchors_lag.first_key_value()
            && first_key < gc_boundary
        {
            history.gc_anchors_lag(gc_boundary);

            tracing::trace!(
                %shard, seqno,
                "anchors lag history gc < {0}",
                gc_boundary,
            );
        }

        // check lag and calculate target wu params
        // NOTE: ONLY by shard blocks
        if shard == ShardIdent::MASTERCHAIN {
            return Ok(());
        }

        self.try_calculate_target_wu_params_and_perform_adjustment(shard, seqno, &tuner_params)
            .await
    }

    async fn try_calculate_target_wu_params_and_perform_adjustment(
        &mut self,
        shard: ShardIdent,
        seqno: u32,
        tuner_params: &TunerParams,
    ) -> Result<()> {
        let &TunerParams {
            tune_interval,
            tune_seqno,
            wu_params_calc_seqno,
            ..
        } = tuner_params;

        let history = self
            .history
            .entry(shard)
            .or_insert_with(|| WuHistory::new(DEFAULT_PERCENTILE_WINDOW));

        // calculate MA target wu params
        // avoid the MA recalculation on the same bucket
        if wu_params_calc_seqno == 0
            || self.wu_params_last_calculated_on_seqno == wu_params_calc_seqno
        {
            return Ok(());
        }

        // take last recorder avg lag not ahead of target params calculation seqno
        let search_from_boundary = wu_params_calc_seqno.saturating_sub(tune_interval);
        let search_range = history.avg_anchors_lag.range((
            Bound::Included(search_from_boundary),
            Bound::Included(wu_params_calc_seqno),
        ));
        // use the latest lag MA that does not look ahead of the target wu-params bucket
        let Some((&lag_ma_seqno, &avg_lag)) = search_range.last() else {
            return Ok(());
        };

        // take avg wu metrics at the same seqno
        let Some(avg_wu_metrics) = history.avg_metrics.get(&lag_ma_seqno) else {
            return Ok(());
        };
        let Some(avg_wu_metrics_res) = avg_wu_metrics.get_avg() else {
            return Ok(());
        };

        let missing_mandatory_windows = history.unit_cost_clippers.missing_mandatory_windows();
        if !missing_mandatory_windows.is_empty() {
            // wait for stable percentile bounds before calculating target params
            tracing::debug!(
                %shard, seqno,
                ?missing_mandatory_windows,
                "skip target wu params calculation because mandatory clipper windows are not ready",
            );
            return Ok(());
        }

        // calculate target wu params if avg lag does not fit bounds
        let mut target_wu_params = None;

        // get actual wu price
        let actual_wu_price = avg_wu_metrics_res.collation_total_wu_price();

        // get wu price from last adjustment
        let last_adjustment_wu_price = self
            .adjustments
            .last_key_value()
            .map(|(_, v)| v.target_wu_price);

        // initial target wu price from config, or last adjustment, or actual
        let mut target_wu_price = if !self.config.adaptive_wu_price {
            self.config.target_wu_price as f64 / 100.0
        } else {
            last_adjustment_wu_price.unwrap_or(actual_wu_price)
        };
        // initial adaptive wu price from actual
        let mut adaptive_wu_price = actual_wu_price;

        let lag_lower_bound = self.config.lag_bounds_ms.0 as i64;
        let lag_upper_bound = self.config.lag_bounds_ms.1 as i64;

        // it is okay when lag is negative but we do not have messages
        // avoid adjusting wu params when anchors arrive early during idle periods
        if (avg_wu_metrics_res.wu_on_execute.groups_count > 0 || avg_lag >= 0)
            && !(lag_lower_bound..lag_upper_bound).contains(&avg_lag)
        {
            // get prev wu price from last adjustment or actual
            let prev_wu_price = last_adjustment_wu_price.unwrap_or(actual_wu_price);

            // calculate adaptive wu price
            adaptive_wu_price =
                        // if current lag is > 0 then we should reduce target wu price
                        if avg_lag >= lag_upper_bound {
                            let delta = avg_lag - lag_upper_bound;
                            let adj = if delta > 10000 {
                                0.2
                            } else if delta > 2000 {
                                0.1
                            } else {
                                0.05
                            };
                            prev_wu_price.saturating_add_floor(-adj)
                        }
                        // if current lag is < 0 then we should increase target wu price
                        else if avg_lag < lag_lower_bound {
                            let adj = if (lag_lower_bound - avg_lag) > 500 {
                                0.2
                            } else {
                                0.1
                            };
                            prev_wu_price.saturating_add_floor(adj)
                        } else {
                            unreachable!()
                        };

            // wu price above 2.0 is bad, get initial target price
            if adaptive_wu_price > 2.0 {
                adaptive_wu_price = target_wu_price;
            }

            // use adaptive wu price if required
            if self.config.adaptive_wu_price {
                target_wu_price = adaptive_wu_price;
            }

            // calculate target wu params
            let target_params = Self::calculate_target_wu_params(
                target_wu_price,
                avg_wu_metrics,
                &avg_wu_metrics_res,
            );

            tracing::debug!(
                %shard, seqno,
                tune_seqno, tune_interval,
                wu_params_last_updated_on_seqno = self.wu_params_last_updated_on_seqno,
                has_pending_messages = avg_wu_metrics_res.has_pending_messages,
                avg_lag, lag_bounds = ?self.config.lag_bounds_ms,
                current_wu_price = actual_wu_price,
                prev_wu_price, adaptive_wu_price, target_wu_price,
                current_build_in_msg_wu_param = avg_wu_metrics_res.wu_params.finalize.build_in_msg,
                target_build_in_msg_wu_param = target_params.finalize.build_in_msg,
                "calculated target wu params",
            );

            target_wu_params = Some(target_params);
        }

        report_wu_price(
            actual_wu_price,
            last_adjustment_wu_price,
            adaptive_wu_price,
            target_wu_price,
        );

        // if target wu params calculated
        if let Some(target_wu_params) = target_wu_params {
            // report target wu params to metrics
            report_wu_params(&avg_wu_metrics_res.wu_params, &target_wu_params);

            self.wu_params_last_calculated_on_seqno = wu_params_calc_seqno;

            // do not update in the current tune bucket if wu params changed
            // in the second half of the previous bucket or later
            // to prevent next update before 1/2 of the interval has passed
            let tune_half_seqno = tune_seqno.saturating_sub(tune_interval / 2);

            // update wu params in blockchain if tune interval elapsed
            if self.wu_params_last_updated_on_seqno < tune_half_seqno
                && !self.adjustments.contains_key(&tune_seqno)
            {
                match &self.config.tune {
                    WuTuneType::Rpc { rpc, .. } => {
                        tracing::info!(
                            %shard, seqno,
                            tune_seqno,
                            avg_lag, target_wu_price,
                            rpc,
                            ?target_wu_params,
                            "updating target wu params in blockchain config via rpc",
                        );

                        // make adjustment
                        self.updater
                            .update_wu_params(self.config.clone(), target_wu_params)
                            .await?;

                        // store current adjustment and gc previous
                        self.adjustments
                            .insert(tune_seqno, WuAdjustment { target_wu_price });
                        let gc_boundary = tune_seqno.saturating_sub(tune_interval);
                        if let Some((&first_key, _)) = self.adjustments.first_key_value()
                            && first_key < gc_boundary
                        {
                            self.adjustments.retain(|k, _| k >= &gc_boundary);
                        }
                    }
                    WuTuneType::No => {
                        // do nothing
                    }
                }
            }
        }

        Ok(())
    }

    pub fn calculate_target_wu_params(
        target_wu_price: f64,
        wu_metrics_avg: &WuMetricsAvg,
        wu_metrics_avg_res: &WuMetrics,
    ) -> WorkUnitsParams {
        // start from the latest observed params and only replace values that can be calculated safely
        let mut target_wu_params = wu_metrics_avg_res.wu_params.clone();

        // FINALIZE WU PARAMS

        let target_build_in_msg_wu_param = calc_target_wu_param_from_sums(
            target_wu_price,
            &wu_metrics_avg.finalize.build_in_msg_sums_accum,
        )
        .unwrap_or(wu_metrics_avg_res.wu_params.finalize.build_in_msg as u64);
        target_wu_params.finalize.build_in_msg = sat_u16_from_u64(target_build_in_msg_wu_param);

        let target_build_out_msg_wu_param = calc_target_wu_param_from_sums(
            target_wu_price,
            &wu_metrics_avg.finalize.build_out_msg_sums_accum,
        )
        .unwrap_or(wu_metrics_avg_res.wu_params.finalize.build_out_msg as u64);
        target_wu_params.finalize.build_out_msg = sat_u16_from_u64(target_build_out_msg_wu_param);

        let target_build_accounts_blocks_wu_param = calc_target_wu_param_from_sums(
            target_wu_price,
            &wu_metrics_avg.finalize.build_accounts_blocks_sums_accum,
        )
        .unwrap_or(wu_metrics_avg_res.wu_params.finalize.build_transactions as u64);
        target_wu_params.finalize.build_transactions =
            sat_u16_from_u64(target_build_accounts_blocks_wu_param);

        // calculate target update_shard_accounts_wu and param
        let scale = 10;
        let target_update_shard_accounts_wu_param = calc_target_scaled_wu_param_from_sums(
            target_wu_price,
            scale,
            &wu_metrics_avg.finalize.update_shard_accounts_sums_accum,
        )
        .unwrap_or(wu_metrics_avg_res.wu_params.finalize.build_accounts as u64);
        target_wu_params.finalize.build_accounts =
            sat_u16_from_u64(target_update_shard_accounts_wu_param);

        // calculate target build_state_update_wu and param
        let target_build_state_update_wu_param = calc_target_scaled_wu_param_from_sums(
            target_wu_price,
            scale,
            &wu_metrics_avg.finalize.build_state_update_sums_accum,
        )
        .unwrap_or(wu_metrics_avg_res.wu_params.finalize.state_update_accounts as u64);
        target_wu_params.finalize.state_update_accounts =
            sat_u16_from_u64(target_build_state_update_wu_param);

        // calculate target build_block_wu and param
        if let Some(target_wu_param) = calc_target_wu_param_from_sums(
            target_wu_price,
            &wu_metrics_avg.finalize.build_block_sums_accum,
        ) {
            target_wu_params.finalize.serialize_accounts = sat_u16_from_u64(target_wu_param);
            target_wu_params.finalize.serialize_msg = target_wu_params.finalize.serialize_accounts;
        } else {
            target_wu_params.finalize.serialize_accounts =
                wu_metrics_avg_res.wu_params.finalize.serialize_accounts;
            target_wu_params.finalize.serialize_msg =
                wu_metrics_avg_res.wu_params.finalize.serialize_msg;
        }

        let target_create_queue_diff_wu_param = calc_target_wu_param_from_sums(
            target_wu_price,
            &wu_metrics_avg.finalize.create_diff_sums_accum,
        )
        .unwrap_or(wu_metrics_avg_res.wu_params.finalize.create_diff as u64);
        target_wu_params.finalize.create_diff = sat_u16_from_u64(target_create_queue_diff_wu_param);

        let target_apply_queue_diff_wu_param = calc_target_wu_param_from_sums(
            target_wu_price,
            &wu_metrics_avg.finalize.apply_diff_sums_accum,
        )
        .unwrap_or(wu_metrics_avg_res.wu_params.finalize.apply_diff as u64);
        target_wu_params.finalize.apply_diff = sat_u16_from_u64(target_apply_queue_diff_wu_param);

        // READ MSGS GROUPS (PREPARE) WU PARAMS

        let target_read_ext_msgs_wu_param = calc_target_wu_param_from_sums(
            target_wu_price,
            &wu_metrics_avg.prepare.read_ext_msgs_sums_accum,
        )
        .unwrap_or(wu_metrics_avg_res.wu_params.prepare.read_ext_msgs as u64);
        target_wu_params.prepare.read_ext_msgs = sat_u16_from_u64(target_read_ext_msgs_wu_param);

        let target_read_existing_int_msgs_wu_param = calc_target_wu_param_from_sums(
            target_wu_price,
            &wu_metrics_avg.prepare.read_existing_int_msgs_sums_accum,
        )
        .unwrap_or(wu_metrics_avg_res.wu_params.prepare.read_int_msgs as u64);
        target_wu_params.prepare.read_int_msgs =
            sat_u16_from_u64(target_read_existing_int_msgs_wu_param);

        let target_read_new_int_msgs_wu_param = calc_target_wu_param_from_sums(
            target_wu_price,
            &wu_metrics_avg.prepare.read_new_int_msgs_sums_accum,
        )
        .unwrap_or(wu_metrics_avg_res.wu_params.prepare.read_new_msgs as u64);
        target_wu_params.prepare.read_new_msgs =
            sat_u16_from_u64(target_read_new_int_msgs_wu_param);

        let target_add_msgs_to_groups_wu_param = calc_target_wu_param_from_sums(
            target_wu_price,
            &wu_metrics_avg.prepare.add_msgs_to_groups_sums_accum,
        )
        .unwrap_or(wu_metrics_avg_res.wu_params.prepare.add_to_msg_groups as u64);
        target_wu_params.prepare.add_to_msg_groups =
            sat_u16_from_u64(target_add_msgs_to_groups_wu_param);

        // EXECUTE WU PARAMS

        let target_execute_wu_param = ExecuteWu::calc_target_execute_wu_param(
            target_wu_price,
            target_wu_params.execute.prepare as u64,
            target_wu_params.execute.execute_delimiter as u64,
            &wu_metrics_avg.execute.execute_groups_vm_sums_accum,
        )
        .unwrap_or(wu_metrics_avg_res.wu_params.execute.execute as u64);
        target_wu_params.execute.execute = sat_u16_from_u64(target_execute_wu_param);

        let target_process_txs_wu_param = calc_target_wu_param_from_sums(
            target_wu_price,
            &wu_metrics_avg.execute.process_txs_sums_accum,
        );
        if let Some(target_wu_param) = target_process_txs_wu_param {
            // these three terms share one `n * log2(n)` base, so keep them locked to one calculated target value
            let target_wu_param = sat_u16_from_u64(target_wu_param);
            target_wu_params.execute.serialize_enqueue = target_wu_param;
            target_wu_params.execute.serialize_dequeue = target_wu_param;
            target_wu_params.execute.insert_new_msgs = target_wu_param;
        } else {
            target_wu_params.execute.serialize_enqueue =
                wu_metrics_avg_res.wu_params.execute.serialize_enqueue;
            target_wu_params.execute.serialize_dequeue =
                wu_metrics_avg_res.wu_params.execute.serialize_dequeue;
            target_wu_params.execute.insert_new_msgs =
                wu_metrics_avg_res.wu_params.execute.insert_new_msgs;
        }

        // DO COLLATE WU PARAMS

        let target_resume_collation_wu_param = calc_target_scaled_wu_param_from_sums(
            target_wu_price,
            // resume collation base already contains two scaled power terms, so invert it with `scale^2`
            scale * scale,
            &wu_metrics_avg.do_collate.resume_collation_sums_accum,
        )
        .unwrap_or(wu_metrics_avg_res.wu_params.finalize.diff_tail_len as u64);
        target_wu_params.finalize.diff_tail_len =
            sat_u16_from_u64(target_resume_collation_wu_param);

        target_wu_params
    }
}

trait SaturatingAddFloor {
    fn saturating_add_floor(self, adj: Self) -> Self;
}

impl SaturatingAddFloor for f64 {
    fn saturating_add_floor(self, adj: Self) -> Self {
        // keep wu price strictly positive so inverse price formulas never divide by zero
        (self + adj).max(0.02)
    }
}

fn sat_u16_from_u64(v: u64) -> u16 {
    // wu params are stored as u16 values, so clamp calculated target values before assignment
    v.min(u16::MAX as u64) as u16
}

pub fn safe_metrics_avg<'a, I>(range: I) -> Option<WuMetricsAvg>
where
    I: Iterator<Item = (&'a u32, &'a mut WuMetricsSpan)>,
{
    let mut avg = WuMetricsAvg::new();
    let mut has_merged = false;
    for (_, v) in range {
        // each span already holds clipped stage averages and clipped raw sums, so merge at the span level
        let Some(()) = avg.merge_span_avg(&v.avg) else {
            continue;
        };
        has_merged = true;
    }
    has_merged.then_some(avg)
}

fn safe_anchors_lag_avg<'a, I, F>(range: I, mut clip_value: F) -> Option<i64>
where
    I: Iterator<Item = (&'a u32, &'a mut AnchorsLagSpan)>,
    F: FnMut(i64) -> i64,
{
    let mut avg = SafeSignedAvg::default();
    for (_, v) in range {
        // finalize each span once so the last lag sample is not accumulated multiple times
        avg.accum(v.get_result(&mut clip_value));
    }

    avg.get_avg_checked().map(|v| v as i64)
}

fn report_wu_price(
    actual_wu_price: f64,
    last_adjustment_wu_price: Option<f64>,
    adaptive_wu_price: f64,
    target_wu_price: f64,
) {
    metrics::gauge!("tycho_wu_tuner_actual_wu_price").set(actual_wu_price);
    metrics::gauge!("tycho_wu_tuner_adaptive_wu_price").set(adaptive_wu_price);
    metrics::gauge!("tycho_wu_tuner_target_wu_price").set(target_wu_price);
    if let Some(last_adjustment_wu_price) = last_adjustment_wu_price {
        metrics::gauge!("tycho_wu_tuner_last_adjustment_wu_price").set(last_adjustment_wu_price);
    }
}

fn report_zero_wu_price() {
    report_wu_price(0.0, Some(0.0), 0.0, 0.0);
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

#[cfg(test)]
mod tests {
    use std::future;
    use std::future::Future;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use anyhow::Result;

    use super::*;

    #[derive(Clone, Default)]
    struct TestUpdater {
        update_calls: Arc<AtomicUsize>,
    }

    impl TestUpdater {
        fn new() -> Self {
            Self::default()
        }

        fn update_calls(&self) -> usize {
            self.update_calls.load(Ordering::SeqCst)
        }
    }

    impl WuParamsUpdater for TestUpdater {
        fn update_wu_params(
            &self,
            _config: Arc<WuTunerConfig>,
            _target_wu_params: WorkUnitsParams,
        ) -> impl Future<Output = Result<()>> + Send {
            self.update_calls.fetch_add(1, Ordering::SeqCst);
            future::ready(Ok(()))
        }
    }

    fn test_unit_cost_clippers() -> UnitCostClippers {
        UnitCostClippers::new(DEFAULT_PERCENTILE_WINDOW)
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

    fn test_tuner_config() -> Arc<WuTunerConfig> {
        Arc::new(WuTunerConfig {
            wu_params_calc_interval: 10,
            tune_interval: 50,
            lag_bounds_ms: (100, 5000),
            adaptive_wu_price: true,
            tune: WuTuneType::Rpc {
                secret: Default::default(),
                rpc: Default::default(),
            },
            ..Default::default()
        })
    }

    fn fill_mandatory_windows(clippers: &mut UnitCostClippers, window: usize) {
        for _ in 0..window {
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
    }

    fn seed_test_history(history: &mut WuHistory, lag_ma_seqno: u32, avg_lag: i64) {
        let mut avg = WuMetricsAvg::new();
        let mut clippers = test_unit_cost_clippers();
        avg.accum(&sample_metrics(10, true, 11, 1000), &mut clippers);
        history.avg_metrics.insert(lag_ma_seqno, avg);
        history.avg_anchors_lag.insert(lag_ma_seqno, avg_lag);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn target_flow_skips_when_mandatory_windows_are_not_ready() {
        let updater = TestUpdater::new();
        let config = test_tuner_config();
        let mut tuner = WuTuner::new(config.clone(), updater.clone());
        let shard = ShardIdent::BASECHAIN;
        let seqno = 400;
        let tuner_params = TunerParams::calculate(&config, seqno);
        let history = tuner
            .history
            .entry(shard)
            .or_insert_with(|| WuHistory::new(DEFAULT_PERCENTILE_WINDOW));
        seed_test_history(history, tuner_params.wu_params_calc_seqno, 6000);

        tuner
            .try_calculate_target_wu_params_and_perform_adjustment(shard, seqno, &tuner_params)
            .await
            .expect("target flow should return Ok on readiness skip");

        assert_eq!(tuner.wu_params_last_calculated_on_seqno, 0);
        assert_eq!(updater.update_calls(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn target_flow_proceeds_when_only_excluded_windows_are_not_ready() {
        let updater = TestUpdater::new();
        let config = test_tuner_config();
        let mut tuner = WuTuner::new(config.clone(), updater.clone());
        let shard = ShardIdent::BASECHAIN;
        let seqno = 400;
        let tuner_params = TunerParams::calculate(&config, seqno);
        let history = tuner
            .history
            .entry(shard)
            .or_insert_with(|| WuHistory::new(DEFAULT_PERCENTILE_WINDOW));
        seed_test_history(history, tuner_params.wu_params_calc_seqno, 6000);
        fill_mandatory_windows(&mut history.unit_cost_clippers, DEFAULT_PERCENTILE_WINDOW);
        assert!(history.unit_cost_clippers.mandatory_windows_filled());
        assert!(
            !history
                .unit_cost_clippers
                .prepare
                .read_ext_msgs
                .window_is_filled()
        );
        assert!(
            !history
                .unit_cost_clippers
                .finalize
                .create_diff
                .window_is_filled()
        );
        assert!(
            !history
                .unit_cost_clippers
                .finalize
                .apply_diff
                .window_is_filled()
        );

        tuner
            .try_calculate_target_wu_params_and_perform_adjustment(shard, seqno, &tuner_params)
            .await
            .expect("target flow should run when only excluded windows are not ready");

        assert_eq!(
            tuner.wu_params_last_calculated_on_seqno,
            tuner_params.wu_params_calc_seqno
        );
        assert_eq!(updater.update_calls(), 1);
    }
}
