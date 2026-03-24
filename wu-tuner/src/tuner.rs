use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tycho_collator::collator::work_units::{
    DoCollateWu, ExecuteWu, FinalizeWu, PrepareMsgGroupsWu, calc_target_scaled_wu_param_from_sums,
    calc_target_wu_param_from_sums, report_anchor_lag_to_metrics,
};
use tycho_collator::types::processed_upto::BlockSeqno;
use tycho_types::models::{ShardIdent, WorkUnitsParams};
use tycho_util::FastHashSet;
use tycho_util::num::{RollingPercentiles, SafeAccum, SafeSignedAvg, SafeUnsignedAvg};

use crate::config::{WuTuneType, WuTunerConfig};
use crate::unit_cost_clipper::UnitCostClippers;
use crate::updater::WuParamsUpdater;
use crate::{MempoolAnchorLag, WuEvent, WuEventData, WuMetrics};

pub struct WuMetricsSpan {
    pub avg: Box<WuMetricsAvg>,
    pub last: Box<WuMetrics>,
}

impl WuMetricsSpan {
    fn new(metrics: Box<WuMetrics>, unit_cost_clippers: &mut UnitCostClippers) -> Self {
        let mut avg = WuMetricsAvg::new();
        avg.accum(&metrics, unit_cost_clippers);
        Self {
            avg: Box::new(avg),
            last: metrics,
        }
    }

    fn accum(
        &mut self,
        v: Box<WuMetrics>,
        unit_cost_clippers: &mut UnitCostClippers,
    ) -> Result<()> {
        self.avg.accum(&v, unit_cost_clippers);
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
        self.last = Some(v);
        Ok(())
    }

    fn accum_last<F>(&mut self, mut clip_value: F) -> Result<()>
    where
        F: FnMut(i64) -> i64,
    {
        if let Some(last) = self.last.take() {
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

    pub wu_params_ma_interval: u32,

    pub tune_interval: u32,
    pub tune_seqno: u32,

    pub wu_span_seqno: u32,
    pub wu_ma_seqno: u32,
    pub lag_span_seqno: u32,
    pub lag_ma_seqno: u32,
    pub wu_params_ma_seqno: u32,
}

impl TunerParams {
    fn calculate(config: &WuTunerConfig, seqno: BlockSeqno) -> Self {
        let wu_span = config.wu_span.max(10) as u32;
        let wu_ma_interval = config.wu_ma_interval.max(2) as u32;
        let wu_ma_interval = wu_ma_interval.saturating_mul(wu_span);
        let wu_ma_range = config.wu_ma_range.max(100) as u32;

        let lag_span = config.lag_span.max(10) as u32;
        let lag_ma_interval = config.lag_ma_interval.max(2) as u32;
        let lag_ma_interval = lag_ma_interval.saturating_mul(lag_span);
        let lag_ma_range = config.lag_ma_range.max(100) as u32;

        let wu_params_ma_interval = config.wu_params_ma_interval.max(40) as u32;

        let tune_interval = config.tune_interval.max(200) as u32;
        let tune_seqno = seqno / tune_interval * tune_interval;

        // normilized seqno for calculations
        // e.g. seqno = 254
        let wu_span_seqno = seqno / wu_span * wu_span; // 250
        let wu_ma_seqno = seqno / wu_ma_interval * wu_ma_interval; // 240
        let lag_span_seqno = seqno / lag_span * lag_span; // 250
        let lag_ma_seqno = seqno / lag_ma_interval * lag_ma_interval; // 240
        let wu_params_ma_seqno = seqno / wu_params_ma_interval * wu_params_ma_interval; // 240

        Self {
            wu_span,
            wu_ma_interval,
            wu_ma_range,
            lag_span,
            lag_ma_interval,
            lag_ma_range,
            wu_params_ma_interval,
            tune_interval,
            tune_seqno,
            wu_span_seqno,
            wu_ma_seqno,
            lag_span_seqno,
            lag_ma_seqno,
            wu_params_ma_seqno,
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

            self.wu_params_last_updated_on_seqno = seqno;
        }

        // on start set wu params last updated on current seqno
        if self.wu_params_last_updated_on_seqno == 0 {
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

        // do not calculate MA until the percentile windows is filled
        if !history.anchors_lag_pct.window_is_filled() {
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
            wu_params_ma_seqno,
            ..
        } = tuner_params;

        let history = self
            .history
            .entry(shard)
            .or_insert_with(|| WuHistory::new(DEFAULT_PERCENTILE_WINDOW));

        // calculate MA target wu params
        // avoid the MA recalculation on the same bucket
        if wu_params_ma_seqno == 0 || self.wu_params_last_calculated_on_seqno == wu_params_ma_seqno
        {
            return Ok(());
        }

        // take last recorder avg lag not ahead of target params calculation seqno
        let search_from_boundary = wu_params_ma_seqno.saturating_sub(tune_interval);
        let search_range = history.avg_anchors_lag.range((
            Bound::Included(search_from_boundary),
            Bound::Included(wu_params_ma_seqno),
        ));
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

        // calculate target wu params if avg lag does not fit bounds
        let mut target_wu_params = None;

        // get actual wu price
        let actual_wu_price = avg_wu_metrics_res.collation_total_wu_price();

        // get wu price from last adjustment
        let last_adjustment_wu_price = self
            .adjustments
            .last_key_value()
            .map(|(_, v)| v.target_wu_price);

        // initial target and adaptive wu price from last adjustment or actual
        let mut target_wu_price = last_adjustment_wu_price.unwrap_or(actual_wu_price);
        let mut adaptive_wu_price = target_wu_price;

        let lag_lower_bound = self.config.lag_bounds_ms.0 as i64;
        let lag_upper_bound = self.config.lag_bounds_ms.1 as i64;

        // it is okay when lag is negative but we do not have messages
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
            } else {
                // otherwise get target wu price from config
                target_wu_price = self.config.target_wu_price as f64 / 100.0;
            }

            // calculate target wu params
            let target_params = Self::calculate_target_wu_params(
                target_wu_price,
                avg_wu_metrics,
                &avg_wu_metrics_res,
            );

            tracing::debug!(
                %shard, seqno,
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

            self.wu_params_last_calculated_on_seqno = wu_params_ma_seqno;

            // needs to collect history more then 1/2 of tune interval to be able to perform update
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

    fn calculate_target_wu_params(
        target_wu_price: f64,
        wu_metrics_avg: &WuMetricsAvg,
        wu_metrics_avg_res: &WuMetrics,
    ) -> WorkUnitsParams {
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
        (self + adj).max(0.02)
    }
}

fn sat_u16_from_u64(v: u64) -> u16 {
    v.min(u16::MAX as u64) as u16
}

fn safe_metrics_avg<'a, I>(range: I) -> Option<WuMetricsAvg>
where
    I: Iterator<Item = (&'a u32, &'a mut WuMetricsSpan)>,
{
    let mut avg = WuMetricsAvg::new();
    let mut has_merged = false;
    for (_, v) in range {
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
        avg.accum(v.get_result(&mut clip_value));
    }

    avg.get_avg_checked().map(|v| v as i64)
}

fn safe_anchors_lag_avg_2<'a, I>(range: I) -> Option<i64>
where
    I: Iterator<Item = (&'a u32, &'a i64)>,
{
    let mut avg = SafeSignedAvg::default();
    for (_, v) in range {
        avg.accum(*v);
    }

    avg.get_avg_checked().map(|v| v as i64)
}

#[derive(Default)]
struct PrepareElapsedClipSnapshot {
    read_ext_msgs_elapsed_ns: Option<u128>,
    read_existing_int_msgs_elapsed_ns: Option<u128>,
    read_new_int_msgs_elapsed_ns: Option<u128>,
    add_msgs_to_groups_elapsed_ns: Option<u128>,
    total_elapsed_ns: Option<u128>,
}

#[derive(Default)]
struct PrepareMsgGroupsWuAvg {
    fixed_part: SafeUnsignedAvg,

    read_ext_msgs_count: SafeUnsignedAvg,
    read_ext_msgs_wu: SafeUnsignedAvg,
    read_ext_msgs_elapsed_ns: SafeUnsignedAvg,
    read_ext_msgs_base: SafeUnsignedAvg,

    read_existing_int_msgs_count: SafeUnsignedAvg,
    read_existing_int_msgs_wu: SafeUnsignedAvg,
    read_existing_int_msgs_elapsed_ns: SafeUnsignedAvg,
    read_existing_int_msgs_base: SafeUnsignedAvg,

    read_new_int_msgs_count: SafeUnsignedAvg,
    read_new_int_msgs_wu: SafeUnsignedAvg,
    read_new_int_msgs_elapsed_ns: SafeUnsignedAvg,
    read_new_int_msgs_base: SafeUnsignedAvg,

    add_to_msgs_groups_ops_count: SafeUnsignedAvg,
    add_msgs_to_groups_wu: SafeUnsignedAvg,
    add_msgs_to_groups_elapsed_ns: SafeUnsignedAvg,
    add_msgs_to_groups_base: SafeUnsignedAvg,

    read_ext_msgs_sums_accum: SafeAccum<(u128, u128)>,
    read_existing_int_msgs_sums_accum: SafeAccum<(u128, u128)>,
    read_new_int_msgs_sums_accum: SafeAccum<(u128, u128)>,
    add_msgs_to_groups_sums_accum: SafeAccum<(u128, u128)>,

    total_elapsed_ns: SafeUnsignedAvg,
}

impl PrepareMsgGroupsWuAvg {
    fn accum(&mut self, v: &PrepareMsgGroupsWu, unit_cost_clippers: &mut UnitCostClippers) {
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

    fn accum_avg_result(
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

    fn merge_sums_accum(&mut self, other: &Self) {
        self.read_ext_msgs_sums_accum
            .merge(&other.read_ext_msgs_sums_accum);
        self.read_existing_int_msgs_sums_accum
            .merge(&other.read_existing_int_msgs_sums_accum);
        self.read_new_int_msgs_sums_accum
            .merge(&other.read_new_int_msgs_sums_accum);
        self.add_msgs_to_groups_sums_accum
            .merge(&other.add_msgs_to_groups_sums_accum);
    }
}

#[derive(Default)]
struct ExecuteElapsedClipSnapshot {
    execute_groups_vm_only_elapsed_ns: Option<u128>,
    process_txs_elapsed_ns: Option<u128>,
}

#[derive(Default)]
struct ExecuteWuAvg {
    inserted_new_msgs_count: SafeUnsignedAvg,

    groups_count: SafeUnsignedAvg,

    execute_groups_vm_sum_accounts_over_threads: SafeUnsignedAvg,
    execute_groups_vm_sum_gas_over_threads: SafeUnsignedAvg,

    execute_groups_vm_only_wu: SafeUnsignedAvg,
    execute_groups_vm_only_elapsed_ns: SafeUnsignedAvg,

    process_txs_base: SafeUnsignedAvg,
    process_txs_wu: SafeUnsignedAvg,
    process_txs_elapsed_ns: SafeUnsignedAvg,

    execute_groups_vm_sums_accum: SafeAccum<(u128, u128, u128)>,
    process_txs_sums_accum: SafeAccum<(u128, u128)>,
}

impl ExecuteWuAvg {
    fn accum(&mut self, v: &ExecuteWu, unit_cost_clippers: &mut UnitCostClippers) {
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

    fn accum_avg_result(&mut self, v: &ExecuteWu, clipped_snapshot: &ExecuteElapsedClipSnapshot) {
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

    fn merge_sums_accum(&mut self, other: &Self) {
        self.execute_groups_vm_sums_accum
            .merge(&other.execute_groups_vm_sums_accum);
        self.process_txs_sums_accum
            .merge(&other.process_txs_sums_accum);
    }
}

#[derive(Default)]
struct FinalizeElapsedClipSnapshot {
    create_queue_diff_elapsed_ns: Option<u128>,
    apply_queue_diff_elapsed_ns: Option<u128>,
    update_shard_accounts_elapsed_ns: Option<u128>,
    build_accounts_blocks_elapsed_ns: Option<u128>,
    build_accounts_elapsed_ns: Option<u128>,
    build_in_msgs_elapsed_ns: Option<u128>,
    build_out_msgs_elapsed_ns: Option<u128>,
    build_accounts_and_messages_in_parallel_elased_ns: Option<u128>,
    build_state_update_elapsed_ns: Option<u128>,
    build_block_elapsed_ns: Option<u128>,
    finalize_block_elapsed_ns: Option<u128>,
    total_elapsed_ns: Option<u128>,
}

#[derive(Default)]
struct FinalizeWuAvg {
    diff_msgs_count: SafeUnsignedAvg,
    create_diff_base: SafeUnsignedAvg,
    apply_diff_base: SafeUnsignedAvg,

    create_queue_diff_wu: SafeUnsignedAvg,
    create_queue_diff_elapsed_ns: SafeUnsignedAvg,

    apply_queue_diff_wu: SafeUnsignedAvg,
    apply_queue_diff_elapsed_ns: SafeUnsignedAvg,

    updated_accounts_count: SafeUnsignedAvg,
    in_msgs_len: SafeUnsignedAvg,
    out_msgs_len: SafeUnsignedAvg,

    update_shard_accounts_wu: SafeUnsignedAvg,
    update_shard_accounts_elapsed_ns: SafeUnsignedAvg,
    update_shard_accounts_base: SafeUnsignedAvg,

    build_accounts_blocks_wu: SafeUnsignedAvg,
    build_accounts_blocks_elapsed_ns: SafeUnsignedAvg,
    build_accounts_blocks_base: SafeUnsignedAvg,

    build_accounts_elapsed_ns: SafeUnsignedAvg,

    build_in_msgs_wu: SafeUnsignedAvg,
    build_in_msgs_elapsed_ns: SafeUnsignedAvg,
    build_in_msg_base: SafeUnsignedAvg,

    build_out_msgs_wu: SafeUnsignedAvg,
    build_out_msgs_elapsed_ns: SafeUnsignedAvg,
    build_out_msg_base: SafeUnsignedAvg,

    build_accounts_and_messages_in_parallel_elased_ns: SafeUnsignedAvg,

    build_state_update_wu: SafeUnsignedAvg,
    build_state_update_elapsed_ns: SafeUnsignedAvg,
    build_state_update_base: SafeUnsignedAvg,

    build_block_wu: SafeUnsignedAvg,
    build_block_elapsed_ns: SafeUnsignedAvg,
    build_block_base: SafeUnsignedAvg,

    finalize_block_elapsed_ns: SafeUnsignedAvg,

    build_in_msg_sums_accum: SafeAccum<(u128, u128)>,
    build_out_msg_sums_accum: SafeAccum<(u128, u128)>,
    build_accounts_blocks_sums_accum: SafeAccum<(u128, u128)>,
    update_shard_accounts_sums_accum: SafeAccum<(u128, u128)>,
    build_state_update_sums_accum: SafeAccum<(u128, u128)>,
    build_block_sums_accum: SafeAccum<(u128, u128)>,
    create_diff_sums_accum: SafeAccum<(u128, u128)>,
    apply_diff_sums_accum: SafeAccum<(u128, u128)>,

    total_elapsed_ns: SafeUnsignedAvg,
}

impl FinalizeWuAvg {
    fn accum(
        &mut self,
        v: &FinalizeWu,
        state_update_min_wu: u64,
        serialize_min_wu: u64,
        unit_cost_clippers: &mut UnitCostClippers,
    ) {
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

    fn accum_avg_result(&mut self, v: &FinalizeWu, clipped_snapshot: &FinalizeElapsedClipSnapshot) {
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

    fn merge_sums_accum(&mut self, other: &Self) {
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
}

#[derive(Default)]
struct DoCollateElapsedClipSnapshot {
    resume_collation_elapsed_ns: Option<u128>,
    resume_collation_elapsed_per_block_ns: Option<u128>,
    collation_total_elapsed_ns: Option<u128>,
}

#[derive(Default)]
struct DoCollateWuAvg {
    resume_collation_wu: SafeUnsignedAvg,
    resume_collation_elapsed_ns: SafeUnsignedAvg,
    resume_collation_base: SafeUnsignedAvg,
    resume_collation_sums_accum: SafeAccum<(u128, u128)>,

    resume_collation_wu_per_block: SafeUnsignedAvg,
    resume_collation_elapsed_per_block_ns: SafeUnsignedAvg,

    collation_total_elapsed_ns: SafeUnsignedAvg,
}

impl DoCollateWuAvg {
    fn accum(
        &mut self,
        v: &DoCollateWu,
        collation_total_wu: u64,
        unit_cost_clippers: &mut UnitCostClippers,
    ) {
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

    fn accum_avg_result(
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

    fn merge_sums_accum(&mut self, other: &Self) {
        self.resume_collation_sums_accum
            .merge(&other.resume_collation_sums_accum);
    }
}

pub struct WuMetricsAvg {
    last_wu_params: WorkUnitsParams,
    last_shard_accounts_count: u64,
    had_pending_messages: bool,
    prepare: PrepareMsgGroupsWuAvg,
    execute: ExecuteWuAvg,
    finalize: FinalizeWuAvg,
    do_collate: DoCollateWuAvg,
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

    fn accum(&mut self, v: &WuMetrics, unit_cost_clippers: &mut UnitCostClippers) {
        self.last_wu_params = v.wu_params.clone();
        self.had_pending_messages = self.had_pending_messages && v.has_pending_messages;
        self.last_shard_accounts_count = v.wu_on_finalize.shard_accounts_count;
        let state_update_min_wu = v.wu_params.finalize.state_update_min as u64;
        let serialize_min_wu = v.wu_params.finalize.serialize_min as u64;
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

    fn accum_materialized_avg_result(&mut self, v: &WuMetrics) {
        self.last_wu_params = v.wu_params.clone();
        self.had_pending_messages = self.had_pending_messages && v.has_pending_messages;
        self.last_shard_accounts_count = v.wu_on_finalize.shard_accounts_count;
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

    fn merge_span_avg(&mut self, other: &WuMetricsAvg) -> Option<()> {
        let span_result = other.get_avg()?;
        self.merge_sums_accum(other);
        self.accum_materialized_avg_result(&span_result);
        Some(())
    }
}

impl WuMetricsAvg {
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

impl PrepareMsgGroupsWuAvg {
    fn get_avg(&self) -> PrepareMsgGroupsWu {
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

impl ExecuteWuAvg {
    fn get_avg(&self, avg_in_msgs_len: u64, avg_out_msgs_len: u64) -> ExecuteWu {
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

impl FinalizeWuAvg {
    fn get_avg(&self, last_shard_accounts_count: u64) -> Option<FinalizeWu> {
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

impl DoCollateWuAvg {
    fn get_avg(
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

fn report_wu_price(
    actual_wu_price: f64,
    last_adjustment_wu_price: Option<f64>,
    adaptive_wu_price: f64,
    target_wu_price: f64,
) {
    metrics::gauge!("tycho_wu_tuner_actual_wu_price").set(actual_wu_price);
    metrics::gauge!("tycho_wu_tuner_adaptive_wu_price").set(adaptive_wu_price);
    metrics::gauge!("tycho_wu_tuner_target_wu_price").set(target_wu_price);

    if let Some(last) = last_adjustment_wu_price {
        metrics::gauge!("tycho_wu_tuner_last_adjustment_wu_price").set(last);
    }
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

    use anyhow::Result;

    use super::*;

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
        test_unit_cost_clippers_with_window(DEFAULT_PERCENTILE_WINDOW)
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

    fn clip_two_sample_avg(base1: u128, elapsed1: u128, base2: u128, elapsed2: u128) -> u128 {
        let mut clipper =
            crate::unit_cost_clipper::RollingUnitCostClipper::new(DEFAULT_PERCENTILE_WINDOW);
        let first = clipper
            .clip_elapsed_ns(base1, elapsed1)
            .expect("clip should be available");
        let second = clipper
            .clip_elapsed_ns(base2, elapsed2)
            .expect("clip should be available");
        (first + second) / 2
    }

    fn clip_baseline_outlier_avg(
        baseline_base: u128,
        baseline_elapsed: u128,
        outlier_base: u128,
        outlier_elapsed: u128,
    ) -> u128 {
        let mut clipper =
            crate::unit_cost_clipper::RollingUnitCostClipper::new(DEFAULT_PERCENTILE_WINDOW);
        let mut sum = 0u128;
        for _ in 0..100 {
            sum = sum.saturating_add(
                clipper
                    .clip_elapsed_ns(baseline_base, baseline_elapsed)
                    .expect("clip should be available"),
            );
        }
        sum = sum.saturating_add(
            clipper
                .clip_elapsed_ns(outlier_base, outlier_elapsed)
                .expect("clip should be available"),
        );
        sum / 101
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
            Some(clip_two_sample_avg(
                first.wu_on_prepare_msg_groups.total_wu() as u128,
                first.wu_on_prepare_msg_groups.total_elapsed.as_nanos(),
                second.wu_on_prepare_msg_groups.total_wu() as u128,
                second.wu_on_prepare_msg_groups.total_elapsed.as_nanos(),
            )),
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
            Some(clip_two_sample_avg(
                first.wu_on_finalize.max_accounts_in_out_msgs_wu() as u128,
                first
                    .wu_on_finalize
                    .build_accounts_and_messages_in_parallel_elased
                    .as_nanos(),
                second.wu_on_finalize.max_accounts_in_out_msgs_wu() as u128,
                second
                    .wu_on_finalize
                    .build_accounts_and_messages_in_parallel_elased
                    .as_nanos(),
            )),
        );
        assert_eq!(
            avg.do_collate
                .resume_collation_elapsed_per_block_ns
                .get_avg_checked(),
            Some(clip_two_sample_avg(
                first.wu_on_do_collate.resume_collation_wu_per_block as u128,
                first.wu_on_do_collate.resume_collation_elapsed_per_block_ns,
                second.wu_on_do_collate.resume_collation_wu_per_block as u128,
                second
                    .wu_on_do_collate
                    .resume_collation_elapsed_per_block_ns,
            )),
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
            clip_two_sample_avg(
                first.wu_on_do_collate.resume_collation_wu_per_block as u128,
                first.wu_on_do_collate.resume_collation_elapsed_per_block_ns,
                second.wu_on_do_collate.resume_collation_wu_per_block as u128,
                second
                    .wu_on_do_collate
                    .resume_collation_elapsed_per_block_ns,
            ),
        );
        assert_eq!(
            result.wu_on_finalize.total_elapsed.as_nanos(),
            clip_two_sample_avg(
                first.wu_on_finalize.total_wu() as u128,
                first.wu_on_finalize.total_elapsed.as_nanos(),
                second.wu_on_finalize.total_wu() as u128,
                second.wu_on_finalize.total_elapsed.as_nanos(),
            ),
        );
        assert_eq!(
            result.wu_on_do_collate.collation_total_elapsed.as_nanos(),
            clip_two_sample_avg(
                first.collation_total_wu() as u128,
                first.wu_on_do_collate.collation_total_elapsed.as_nanos(),
                second.collation_total_wu() as u128,
                second.wu_on_do_collate.collation_total_elapsed.as_nanos(),
            )
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
        let expected_prepare_total = clip_baseline_outlier_avg(
            baseline.wu_on_prepare_msg_groups.total_wu() as u128,
            baseline.wu_on_prepare_msg_groups.total_elapsed.as_nanos(),
            outlier.wu_on_prepare_msg_groups.total_wu() as u128,
            outlier.wu_on_prepare_msg_groups.total_elapsed.as_nanos(),
        );
        let expected_build_accounts = clip_baseline_outlier_avg(
            baseline.wu_on_finalize.build_accounts_wu() as u128,
            baseline.wu_on_finalize.build_accounts_elapsed.as_nanos(),
            outlier.wu_on_finalize.build_accounts_wu() as u128,
            outlier.wu_on_finalize.build_accounts_elapsed.as_nanos(),
        );
        let expected_parallel = clip_baseline_outlier_avg(
            baseline.wu_on_finalize.max_accounts_in_out_msgs_wu() as u128,
            baseline
                .wu_on_finalize
                .build_accounts_and_messages_in_parallel_elased
                .as_nanos(),
            outlier.wu_on_finalize.max_accounts_in_out_msgs_wu() as u128,
            outlier
                .wu_on_finalize
                .build_accounts_and_messages_in_parallel_elased
                .as_nanos(),
        );
        let expected_finalize_block = clip_baseline_outlier_avg(
            baseline.wu_on_finalize.finalize_block_wu() as u128,
            baseline.wu_on_finalize.finalize_block_elapsed.as_nanos(),
            outlier.wu_on_finalize.finalize_block_wu() as u128,
            outlier.wu_on_finalize.finalize_block_elapsed.as_nanos(),
        );
        let expected_finalize_total = clip_baseline_outlier_avg(
            baseline.wu_on_finalize.total_wu() as u128,
            baseline.wu_on_finalize.total_elapsed.as_nanos(),
            outlier.wu_on_finalize.total_wu() as u128,
            outlier.wu_on_finalize.total_elapsed.as_nanos(),
        );
        let expected_resume_per_block = clip_baseline_outlier_avg(
            baseline.wu_on_do_collate.resume_collation_wu_per_block as u128,
            baseline
                .wu_on_do_collate
                .resume_collation_elapsed_per_block_ns,
            outlier.wu_on_do_collate.resume_collation_wu_per_block as u128,
            outlier
                .wu_on_do_collate
                .resume_collation_elapsed_per_block_ns,
        );
        let expected_collation_total = clip_baseline_outlier_avg(
            baseline.collation_total_wu() as u128,
            baseline.wu_on_do_collate.collation_total_elapsed.as_nanos(),
            outlier.collation_total_wu() as u128,
            outlier.wu_on_do_collate.collation_total_elapsed.as_nanos(),
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
            expected_collation_total as f64 / result.collation_total_wu() as f64;
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
    fn target_methods_return_none_on_zero_denominator() {
        let sums_2 = SafeAccum::<(u128, u128)>::default();
        let sums_3 = SafeAccum::<(u128, u128, u128)>::default();
        assert_eq!(calc_target_wu_param_from_sums(1.0, &sums_2), None);

        assert_eq!(
            ExecuteWu::calc_target_execute_wu_param(1.0, 10, 10, &sums_3),
            None
        );
        assert_eq!(calc_target_wu_param_from_sums(1.0, &sums_2), None);

        assert_eq!(calc_target_wu_param_from_sums(1.0, &sums_2), None);
        assert_eq!(
            calc_target_scaled_wu_param_from_sums(1.0, 10, &sums_2),
            None
        );
        assert_eq!(
            calc_target_scaled_wu_param_from_sums(1.0, 10, &sums_2),
            None
        );
        assert_eq!(calc_target_wu_param_from_sums(1.0, &sums_2), None);

        assert_eq!(
            calc_target_scaled_wu_param_from_sums(1.0, 10 * 10, &sums_2),
            None
        );
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

    fn synthetic_metrics_for_expected_params() -> (WuMetricsAvg, f64, WorkUnitsParams, WorkUnitsParams) {
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
