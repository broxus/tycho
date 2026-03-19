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
use tycho_types::models::{ShardIdent, WorkUnitsParams};
use tycho_util::FastHashSet;
use tycho_util::num::{SafeSignedAvg, SafeUnsignedAvg};

use crate::config::{WuTuneType, WuTunerConfig};
use crate::rolling_percentile::RollingPercentiles;
use crate::updater::WuParamsUpdater;
use crate::{MempoolAnchorLag, WuEvent, WuEventData, WuMetrics};

pub enum WuMetricsSpanValue {
    Accum(Box<WuMetricsAvg>),
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
            avg: WuMetricsSpanValue::Accum(Box::new(avg)),
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
    avg_metrics: BTreeMap<BlockSeqno, WuMetrics>,
    avg_metrics_last_calculated_on_seqno: u32,
    anchors_lag: BTreeMap<BlockSeqno, AnchorsLagSpan>,
    anchors_lag_min_seqno: Option<u32>,
    anchors_lag_pct: RollingPercentiles<i64>,
    avg_anchors_lag: BTreeMap<BlockSeqno, i64>,
    avg_anchors_lag_last_calculated_on_seqno: u32,
}

impl WuHistory {
    fn new(percentile_window: usize) -> Self {
        let anchors_lag_pct = RollingPercentiles::new(percentile_window);
        Self {
            metrics: Default::default(),
            metrics_min_seqno: None,
            avg_metrics: Default::default(),
            avg_metrics_last_calculated_on_seqno: Default::default(),
            anchors_lag: Default::default(),
            anchors_lag_min_seqno: None,
            anchors_lag_pct,
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
        match history.metrics.entry(wu_span_seqno) {
            std::collections::btree_map::Entry::Vacant(vacant) => {
                vacant.insert(WuMetricsSpan::new(metrics));
            }
            std::collections::btree_map::Entry::Occupied(mut occupied) => {
                let span = occupied.get_mut();
                span.accum(metrics)?;
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

        // report avg wu metrics
        avg.report_metrics(&shard);

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

        // calculate target wu params if avg lag does not fit bounds
        let mut target_wu_params = None;

        // get actual wu price
        let actual_wu_price = avg_wu_metrics.collation_total_wu_price();

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
        if (avg_wu_metrics.wu_on_execute.groups_count > 0 || avg_lag >= 0)
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
            let target_params = Self::calculate_target_wu_params(target_wu_price, avg_wu_metrics);

            tracing::debug!(
                %shard, seqno,
                has_pending_messages = avg_wu_metrics.has_pending_messages,
                avg_lag, lag_bounds = ?self.config.lag_bounds_ms,
                current_wu_price = avg_wu_metrics.collation_total_wu_price(),
                prev_wu_price, adaptive_wu_price, target_wu_price,
                current_build_in_msg_wu_param = avg_wu_metrics.wu_params.finalize.build_in_msg,
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
            report_wu_params(&avg_wu_metrics.wu_params, &target_wu_params);

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

trait SaturatingAddFloor {
    fn saturating_add_floor(self, adj: Self) -> Self;
}

impl SaturatingAddFloor for f64 {
    fn saturating_add_floor(self, adj: Self) -> Self {
        (self + adj).max(0.02)
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
struct PrepareMsgGroupsWuAvg {
    fixed_part: SafeUnsignedAvg,

    read_ext_msgs_count: SafeUnsignedAvg,
    read_ext_msgs_wu: SafeUnsignedAvg,
    read_ext_msgs_elapsed_ns: SafeUnsignedAvg,

    read_existing_int_msgs_count: SafeUnsignedAvg,
    read_existing_int_msgs_wu: SafeUnsignedAvg,
    read_existing_int_msgs_elapsed_ns: SafeUnsignedAvg,

    read_new_int_msgs_count: SafeUnsignedAvg,
    read_new_int_msgs_wu: SafeUnsignedAvg,
    read_new_int_msgs_elapsed_ns: SafeUnsignedAvg,

    add_to_msgs_groups_ops_count: SafeUnsignedAvg,
    add_msgs_to_groups_wu: SafeUnsignedAvg,
    add_msgs_to_groups_elapsed_ns: SafeUnsignedAvg,

    total_elapsed_ns: SafeUnsignedAvg,
}

impl PrepareMsgGroupsWuAvg {
    fn accum(&mut self, v: &PrepareMsgGroupsWu) {
        self.fixed_part.accum(v.fixed_part);

        self.read_ext_msgs_count.accum(v.read_ext_msgs_count);
        self.read_ext_msgs_wu.accum(v.read_ext_msgs_wu);
        self.read_ext_msgs_elapsed_ns
            .accum(v.read_ext_msgs_elapsed.as_nanos());

        self.read_existing_int_msgs_count
            .accum(v.read_existing_int_msgs_count);
        self.read_existing_int_msgs_wu
            .accum(v.read_existing_int_msgs_wu);
        self.read_existing_int_msgs_elapsed_ns
            .accum(v.read_existing_int_msgs_elapsed.as_nanos());

        self.read_new_int_msgs_count
            .accum(v.read_new_int_msgs_count);
        self.read_new_int_msgs_wu.accum(v.read_new_int_msgs_wu);
        self.read_new_int_msgs_elapsed_ns
            .accum(v.read_new_int_msgs_elapsed.as_nanos());

        self.add_to_msgs_groups_ops_count
            .accum(v.add_to_msgs_groups_ops_count);
        self.add_msgs_to_groups_wu.accum(v.add_msgs_to_groups_wu);
        self.add_msgs_to_groups_elapsed_ns
            .accum(v.add_msgs_to_groups_elapsed.as_nanos());

        self.total_elapsed_ns.accum(v.total_elapsed.as_nanos());
    }
}

#[derive(Default)]
struct ExecuteWuAvg {
    inserted_new_msgs_count: SafeUnsignedAvg,

    groups_count: SafeUnsignedAvg,

    avg_group_accounts_count: SafeUnsignedAvg,
    avg_threads_count: SafeUnsignedAvg,

    sum_gas: SafeUnsignedAvg,

    execute_groups_vm_only_wu: SafeUnsignedAvg,
    execute_groups_vm_only_elapsed_ns: SafeUnsignedAvg,

    process_txs_wu: SafeUnsignedAvg,
    process_txs_elapsed_ns: SafeUnsignedAvg,
}

impl ExecuteWuAvg {
    fn accum(&mut self, v: &ExecuteWu) {
        self.inserted_new_msgs_count
            .accum(v.inserted_new_msgs_count);

        self.groups_count.accum(v.groups_count);

        self.avg_group_accounts_count.accum(
            v.avg_group_accounts_count
                .get_avg_checked()
                .unwrap_or_default(),
        );
        self.avg_threads_count
            .accum(v.avg_threads_count.get_avg_checked().unwrap_or_default());

        self.sum_gas.accum(v.sum_gas);

        self.execute_groups_vm_only_wu
            .accum(v.execute_groups_vm_only_wu);
        self.execute_groups_vm_only_elapsed_ns
            .accum(v.execute_groups_vm_only_elapsed.as_nanos());

        self.process_txs_wu.accum(v.process_txs_wu);
        self.process_txs_elapsed_ns
            .accum(v.process_txs_elapsed.as_nanos());
    }
}

#[derive(Default)]
struct FinalizeWuAvg {
    diff_msgs_count: SafeUnsignedAvg,

    create_queue_diff_wu: SafeUnsignedAvg,
    create_queue_diff_elapsed_ns: SafeUnsignedAvg,

    apply_queue_diff_wu: SafeUnsignedAvg,
    apply_queue_diff_elapsed_ns: SafeUnsignedAvg,

    updated_accounts_count: SafeUnsignedAvg,
    in_msgs_len: SafeUnsignedAvg,
    out_msgs_len: SafeUnsignedAvg,

    update_shard_accounts_wu: SafeUnsignedAvg,
    update_shard_accounts_elapsed_ns: SafeUnsignedAvg,

    build_accounts_blocks_wu: SafeUnsignedAvg,
    build_accounts_blocks_elapsed_ns: SafeUnsignedAvg,

    build_accounts_elapsed_ns: SafeUnsignedAvg,

    build_in_msgs_wu: SafeUnsignedAvg,
    build_in_msgs_elapsed_ns: SafeUnsignedAvg,

    build_out_msgs_wu: SafeUnsignedAvg,
    build_out_msgs_elapsed_ns: SafeUnsignedAvg,

    build_accounts_and_messages_in_parallel_elased_ns: SafeUnsignedAvg,

    build_state_update_wu: SafeUnsignedAvg,
    build_state_update_elapsed_ns: SafeUnsignedAvg,

    build_block_wu: SafeUnsignedAvg,
    build_block_elapsed_ns: SafeUnsignedAvg,

    finalize_block_elapsed_ns: SafeUnsignedAvg,

    total_elapsed_ns: SafeUnsignedAvg,
}

impl FinalizeWuAvg {
    fn accum(&mut self, v: &FinalizeWu) {
        self.diff_msgs_count.accum(v.diff_msgs_count);

        self.create_queue_diff_wu.accum(v.create_queue_diff_wu);
        self.create_queue_diff_elapsed_ns
            .accum(v.create_queue_diff_elapsed.as_nanos());

        self.apply_queue_diff_wu.accum(v.apply_queue_diff_wu);
        self.apply_queue_diff_elapsed_ns
            .accum(v.apply_queue_diff_elapsed.as_nanos());

        self.updated_accounts_count.accum(v.updated_accounts_count);
        self.in_msgs_len.accum(v.in_msgs_len);
        self.out_msgs_len.accum(v.out_msgs_len);

        self.update_shard_accounts_wu
            .accum(v.update_shard_accounts_wu);
        self.update_shard_accounts_elapsed_ns
            .accum(v.update_shard_accounts_elapsed.as_nanos());

        self.build_accounts_blocks_wu
            .accum(v.build_accounts_blocks_wu);
        self.build_accounts_blocks_elapsed_ns
            .accum(v.build_accounts_blocks_elapsed.as_nanos());

        self.build_accounts_elapsed_ns
            .accum(v.build_accounts_elapsed.as_nanos());

        self.build_in_msgs_wu.accum(v.build_in_msgs_wu);
        self.build_in_msgs_elapsed_ns
            .accum(v.build_in_msgs_elapsed.as_nanos());

        self.build_out_msgs_wu.accum(v.build_out_msgs_wu);
        self.build_out_msgs_elapsed_ns
            .accum(v.build_out_msgs_elapsed.as_nanos());

        self.build_accounts_and_messages_in_parallel_elased_ns
            .accum(v.build_accounts_and_messages_in_parallel_elased.as_nanos());

        self.build_state_update_wu.accum(v.build_state_update_wu);
        self.build_state_update_elapsed_ns
            .accum(v.build_state_update_elapsed.as_nanos());

        self.build_block_wu.accum(v.build_block_wu);
        self.build_block_elapsed_ns
            .accum(v.build_block_elapsed.as_nanos());

        self.finalize_block_elapsed_ns
            .accum(v.finalize_block_elapsed.as_nanos());

        self.total_elapsed_ns.accum(v.total_elapsed.as_nanos());
    }
}

#[derive(Default)]
struct DoCollateWuAvg {
    resume_collation_wu: SafeUnsignedAvg,
    resume_collation_elapsed_ns: SafeUnsignedAvg,

    resume_collation_wu_per_block: SafeUnsignedAvg,
    resume_collation_elapsed_per_block_ns: SafeUnsignedAvg,

    collation_total_elapsed_ns: SafeUnsignedAvg,
}

impl DoCollateWuAvg {
    fn accum(&mut self, v: &DoCollateWu) {
        self.resume_collation_wu.accum(v.resume_collation_wu);
        self.resume_collation_elapsed_ns
            .accum(v.resume_collation_elapsed.as_nanos());

        self.resume_collation_wu_per_block
            .accum(v.resume_collation_wu_per_block);
        self.resume_collation_elapsed_per_block_ns
            .accum(v.resume_collation_elapsed_per_block_ns);

        self.collation_total_elapsed_ns
            .accum(v.collation_total_elapsed.as_nanos());
    }
}

#[derive(Default)]
struct WuMetricsAvgInner {
    prepare: PrepareMsgGroupsWuAvg,
    execute: ExecuteWuAvg,
    finalize: FinalizeWuAvg,
    do_collate: DoCollateWuAvg,
}

impl WuMetricsAvgInner {
    fn accum(&mut self, v: &WuMetrics) {
        self.prepare.accum(&v.wu_on_prepare_msg_groups);
        self.execute.accum(&v.wu_on_execute);
        self.finalize.accum(&v.wu_on_finalize);
        self.do_collate.accum(&v.wu_on_do_collate);
    }
}

pub struct WuMetricsAvg {
    last_wu_params: WorkUnitsParams,
    last_shard_accounts_count: u64,
    had_pending_messages: bool,
    avg: WuMetricsAvgInner,
}

impl WuMetricsAvg {
    pub fn new() -> Self {
        Self {
            last_wu_params: Default::default(),
            last_shard_accounts_count: 0,
            had_pending_messages: true,
            avg: WuMetricsAvgInner::default(),
        }
    }

    pub fn accum(&mut self, v: &WuMetrics) {
        self.last_wu_params = v.wu_params.clone();
        self.had_pending_messages = self.had_pending_messages && v.has_pending_messages;
        self.last_shard_accounts_count = v.wu_on_finalize.shard_accounts_count;
        self.avg.accum(v);
    }
}

impl WuMetricsAvg {
    pub fn get_avg(&mut self) -> Option<WuMetrics> {
        let (wu_on_prepare_msg_groups, wu_on_execute, wu_on_finalize, wu_on_do_collate) =
            self.avg.get_avg(self.last_shard_accounts_count)?;

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

impl WuMetricsAvgInner {
    fn get_avg(
        &self,
        last_shard_accounts_count: u64,
    ) -> Option<(PrepareMsgGroupsWu, ExecuteWu, FinalizeWu, DoCollateWu)> {
        let wu_on_prepare_msg_groups = self.prepare.get_avg();
        let wu_on_finalize = self.finalize.get_avg(last_shard_accounts_count)?;
        let wu_on_execute = self
            .execute
            .get_avg(wu_on_finalize.in_msgs_len, wu_on_finalize.out_msgs_len);
        let wu_on_do_collate = self.do_collate.get_avg(
            last_shard_accounts_count,
            wu_on_finalize.updated_accounts_count,
        );

        Some((
            wu_on_prepare_msg_groups,
            wu_on_execute,
            wu_on_finalize,
            wu_on_do_collate,
        ))
    }
}

impl PrepareMsgGroupsWuAvg {
    fn get_avg(&self) -> PrepareMsgGroupsWu {
        let avg_u64 = |avg: &SafeUnsignedAvg| avg.get_avg_checked().unwrap_or_default() as u64;

        PrepareMsgGroupsWu {
            fixed_part: avg_u64(&self.fixed_part),

            read_ext_msgs_count: avg_u64(&self.read_ext_msgs_count),
            read_ext_msgs_wu: avg_u64(&self.read_ext_msgs_wu),
            read_ext_msgs_elapsed: Duration::from_nanos(avg_u64(&self.read_ext_msgs_elapsed_ns)),

            read_existing_int_msgs_count: avg_u64(&self.read_existing_int_msgs_count),
            read_existing_int_msgs_wu: avg_u64(&self.read_existing_int_msgs_wu),
            read_existing_int_msgs_elapsed: Duration::from_nanos(avg_u64(
                &self.read_existing_int_msgs_elapsed_ns,
            )),

            read_new_int_msgs_count: avg_u64(&self.read_new_int_msgs_count),
            read_new_int_msgs_wu: avg_u64(&self.read_new_int_msgs_wu),
            read_new_int_msgs_elapsed: Duration::from_nanos(avg_u64(
                &self.read_new_int_msgs_elapsed_ns,
            )),

            add_to_msgs_groups_ops_count: avg_u64(&self.add_to_msgs_groups_ops_count),
            add_msgs_to_groups_wu: avg_u64(&self.add_msgs_to_groups_wu),
            add_msgs_to_groups_elapsed: Duration::from_nanos(avg_u64(
                &self.add_msgs_to_groups_elapsed_ns,
            )),

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

            avg_group_accounts_count: SafeUnsignedAvg::with_initial(avg_u128(
                &self.avg_group_accounts_count,
            )),
            avg_threads_count: SafeUnsignedAvg::with_initial(avg_u128(&self.avg_threads_count)),

            sum_gas: avg_u128(&self.sum_gas),

            execute_groups_vm_only_wu: avg_u64(&self.execute_groups_vm_only_wu),
            execute_groups_vm_only_elapsed: Duration::from_nanos(avg_u64(
                &self.execute_groups_vm_only_elapsed_ns,
            )),

            process_txs_wu: avg_u64(&self.process_txs_wu),
            process_txs_elapsed: Duration::from_nanos(avg_u64(&self.process_txs_elapsed_ns)),
        }
    }
}

impl FinalizeWuAvg {
    fn get_avg(&self, last_shard_accounts_count: u64) -> Option<FinalizeWu> {
        let avg_u64 = |avg: &SafeUnsignedAvg| avg.get_avg_checked().unwrap_or_default() as u64;

        Some(FinalizeWu {
            diff_msgs_count: avg_u64(&self.diff_msgs_count),

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

            build_accounts_blocks_wu: avg_u64(&self.build_accounts_blocks_wu),
            build_accounts_blocks_elapsed: Duration::from_nanos(avg_u64(
                &self.build_accounts_blocks_elapsed_ns,
            )),

            build_accounts_elapsed: Duration::from_nanos(avg_u64(&self.build_accounts_elapsed_ns)),

            build_in_msgs_wu: avg_u64(&self.build_in_msgs_wu),
            build_in_msgs_elapsed: Duration::from_nanos(avg_u64(&self.build_in_msgs_elapsed_ns)),

            build_out_msgs_wu: avg_u64(&self.build_out_msgs_wu),
            build_out_msgs_elapsed: Duration::from_nanos(avg_u64(&self.build_out_msgs_elapsed_ns)),

            build_accounts_and_messages_in_parallel_elased: Duration::from_nanos(avg_u64(
                &self.build_accounts_and_messages_in_parallel_elased_ns,
            )),

            build_state_update_wu: avg_u64(&self.build_state_update_wu),
            build_state_update_elapsed: Duration::from_nanos(avg_u64(
                &self.build_state_update_elapsed_ns,
            )),

            build_block_wu: avg_u64(&self.build_block_wu),
            build_block_elapsed: Duration::from_nanos(avg_u64(&self.build_block_elapsed_ns)),

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
    use super::*;

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
        metrics
            .wu_on_prepare_msg_groups
            .read_existing_int_msgs_count = base + 5;
        metrics.wu_on_prepare_msg_groups.read_existing_int_msgs_wu = base + 6;
        metrics
            .wu_on_prepare_msg_groups
            .read_existing_int_msgs_elapsed = Duration::from_nanos(base + 7);
        metrics.wu_on_prepare_msg_groups.read_new_int_msgs_count = base + 8;
        metrics.wu_on_prepare_msg_groups.read_new_int_msgs_wu = base + 9;
        metrics.wu_on_prepare_msg_groups.read_new_int_msgs_elapsed =
            Duration::from_nanos(base + 10);
        metrics
            .wu_on_prepare_msg_groups
            .add_to_msgs_groups_ops_count = base + 11;
        metrics.wu_on_prepare_msg_groups.add_msgs_to_groups_wu = base + 12;
        metrics.wu_on_prepare_msg_groups.add_msgs_to_groups_elapsed =
            Duration::from_nanos(base + 13);
        metrics.wu_on_prepare_msg_groups.total_elapsed = Duration::from_nanos(base + 14);
        metrics.wu_on_execute.in_msgs_len = base + 200;
        metrics.wu_on_execute.out_msgs_len = base + 201;
        metrics.wu_on_execute.inserted_new_msgs_count = base + 15;
        metrics.wu_on_execute.groups_count = base + 16;
        metrics.wu_on_execute.avg_group_accounts_count =
            SafeUnsignedAvg::with_initial((base + 17) as u128);
        metrics.wu_on_execute.avg_threads_count =
            SafeUnsignedAvg::with_initial((base + 18) as u128);
        metrics.wu_on_execute.sum_gas = (base as u128) + 5000;
        metrics.wu_on_execute.execute_groups_vm_only_wu = base + 19;
        metrics.wu_on_execute.execute_groups_vm_only_elapsed = Duration::from_nanos(base + 20);
        metrics.wu_on_execute.process_txs_wu = base + 21;
        metrics.wu_on_execute.process_txs_elapsed = Duration::from_nanos(base + 22);
        metrics.wu_on_finalize.diff_msgs_count = base + 23;
        metrics.wu_on_finalize.create_queue_diff_wu = base + 24;
        metrics.wu_on_finalize.create_queue_diff_elapsed = Duration::from_nanos(base + 25);
        metrics.wu_on_finalize.apply_queue_diff_wu = base + 26;
        metrics.wu_on_finalize.apply_queue_diff_elapsed = Duration::from_nanos(base + 27);
        metrics.wu_on_finalize.shard_accounts_count = shard_accounts_count;
        metrics.wu_on_finalize.updated_accounts_count = base + 28;
        metrics.wu_on_finalize.in_msgs_len = base + 29;
        metrics.wu_on_finalize.out_msgs_len = base + 30;
        metrics.wu_on_finalize.update_shard_accounts_wu = base + 31;
        metrics.wu_on_finalize.update_shard_accounts_elapsed = Duration::from_nanos(base + 32);
        metrics.wu_on_finalize.build_accounts_blocks_wu = base + 33;
        metrics.wu_on_finalize.build_accounts_blocks_elapsed = Duration::from_nanos(base + 34);
        metrics.wu_on_finalize.build_accounts_elapsed = Duration::from_nanos(base + 35);
        metrics.wu_on_finalize.build_in_msgs_wu = base + 36;
        metrics.wu_on_finalize.build_in_msgs_elapsed = Duration::from_nanos(base + 37);
        metrics.wu_on_finalize.build_out_msgs_wu = base + 38;
        metrics.wu_on_finalize.build_out_msgs_elapsed = Duration::from_nanos(base + 39);
        metrics
            .wu_on_finalize
            .build_accounts_and_messages_in_parallel_elased = Duration::from_nanos(base + 40);
        metrics.wu_on_finalize.build_state_update_wu = base + 41;
        metrics.wu_on_finalize.build_state_update_elapsed = Duration::from_nanos(base + 42);
        metrics.wu_on_finalize.build_block_wu = base + 43;
        metrics.wu_on_finalize.build_block_elapsed = Duration::from_nanos(base + 44);
        metrics.wu_on_finalize.finalize_block_elapsed = Duration::from_nanos(base + 45);
        metrics.wu_on_finalize.total_elapsed = Duration::from_nanos(base + 46);
        metrics.wu_on_do_collate.resume_collation_wu = base + 47;
        metrics.wu_on_do_collate.resume_collation_elapsed = Duration::from_nanos(base + 48);
        metrics.wu_on_do_collate.resume_collation_wu_per_block = base + 49;
        metrics
            .wu_on_do_collate
            .resume_collation_elapsed_per_block_ns = (base as u128) + 6000;
        metrics.wu_on_do_collate.collation_total_elapsed = Duration::from_nanos(base + 50);
        metrics
    }

    #[test]
    fn wu_metrics_avg_inner_accumulates_fields() {
        let mut inner = WuMetricsAvgInner::default();
        let first = sample_metrics(10, true, 11, 1000);
        let second = sample_metrics(30, false, 22, 2000);
        inner.accum(&first);
        inner.accum(&second);
        assert_eq!(
            inner.finalize.updated_accounts_count.get_avg_checked(),
            Some(48)
        );
        assert_eq!(inner.prepare.total_elapsed_ns.get_avg_checked(), Some(34));
        assert_eq!(inner.execute.sum_gas.get_avg_checked(), Some(5020));
        assert_eq!(
            inner
                .finalize
                .build_accounts_and_messages_in_parallel_elased_ns
                .get_avg_checked(),
            Some(60),
        );
        assert_eq!(
            inner
                .do_collate
                .resume_collation_elapsed_per_block_ns
                .get_avg_checked(),
            Some(6020),
        );
    }

    #[test]
    fn wu_metrics_avg_get_avg_preserves_mapping() {
        let mut avg = WuMetricsAvg::new();
        avg.accum(&sample_metrics(10, true, 11, 1000));
        avg.accum(&sample_metrics(30, false, 22, 2000));
        let result = avg.get_avg().expect("average should exist");
        assert_eq!(result.wu_params.execute.prepare, 22);
        assert!(!result.has_pending_messages);
        assert_eq!(result.wu_on_finalize.shard_accounts_count, 2000);
        assert_eq!(result.wu_on_finalize.updated_accounts_count, 48);
        assert_eq!(result.wu_on_finalize.in_msgs_len, 49);
        assert_eq!(result.wu_on_finalize.out_msgs_len, 50);
        assert_eq!(result.wu_on_execute.in_msgs_len, 49);
        assert_eq!(result.wu_on_execute.out_msgs_len, 50);
        assert_eq!(result.wu_on_execute.inserted_new_msgs_count, 35);
        assert_eq!(result.wu_on_prepare_msg_groups.fixed_part, 21);
        assert_eq!(result.wu_on_execute.sum_gas, 5020);
        assert_eq!(
            result
                .wu_on_execute
                .avg_group_accounts_count
                .get_avg_checked(),
            Some(37),
        );
        assert_eq!(
            result
                .wu_on_do_collate
                .resume_collation_elapsed_per_block_ns,
            6020,
        );
        assert_eq!(result.wu_on_finalize.total_elapsed.as_nanos(), 66);
        assert_eq!(
            result.wu_on_do_collate.collation_total_elapsed.as_nanos(),
            70
        );
    }
}
