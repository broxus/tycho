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
use tycho_util::num::{SafeSignedAvg, SafeUnsignedAvg, SafeUnsignedVecAvg};

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
    last_calculated_avg_anchors_lag_seqno: u32,
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
        // e.g. seqno = 244
        let wu_span_seqno = seqno / wu_span * wu_span; // 240
        let wu_ma_seqno = seqno / wu_ma_interval * wu_ma_interval; // 240
        let lag_span_seqno = seqno / lag_span * lag_span; // 240
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
    last_calculated_wu_params_seqno: u32,
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
            last_calculated_wu_params_seqno: 0,
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

        let history = self.history.entry(shard).or_default();

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
        if let Some((_, last)) = history.metrics.last_key_value() {
            if metrics.wu_params != last.last.wu_params {
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

        tracing::trace!(
            %shard, seqno,
            has_pending_messages,
            metrics_history_len = history.metrics.len(),
            "wu metrics received",
        );

        // calculate MA wu metrics both on wu MA seqno and anchors lag MA seqno
        let ma_seqno = std::cmp::max(wu_ma_seqno, lag_ma_seqno);
        if seqno != ma_seqno {
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

        // clear outdated history
        let gc_boundary = wu_ma_seqno.saturating_sub(tune_interval); // e.g. seqno = 240 -> gc_boundary = 40
        if let Some((&first_key, _)) = history.metrics.first_key_value() {
            if first_key < gc_boundary {
                history.gc_wu_metrics(gc_boundary);

                tracing::trace!(
                    %shard, seqno,
                    "wu metrics history gc < {0}",
                    gc_boundary,
                );
            }
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

        let history = self.history.entry(shard).or_default();

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
            lag_bounds = ?self.config.lag_bounds_ms,
            lag_history_len = history.anchors_lag.len(),
            "anchor lag received",
        );

        // calculate MA lag
        if seqno < lag_ma_seqno
            || lag_ma_seqno == 0
            || history.last_calculated_avg_anchors_lag_seqno == lag_ma_seqno
        {
            return Ok(());
        }

        // e.g. seqno = 244 -> avg_range = [140..240)
        let avg_from_boundary = lag_ma_seqno.saturating_sub(lag_ma_range);
        let avg_range = history.anchors_lag.range_mut((
            Bound::Included(avg_from_boundary),
            Bound::Excluded(lag_ma_seqno),
        ));
        let Some(avg_lag) = safe_anchors_lag_avg(avg_range) else {
            return Ok(());
        };
        history.last_calculated_avg_anchors_lag_seqno = lag_ma_seqno;

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

        // clear outdated history
        let gc_boundary = lag_ma_seqno.saturating_sub(tune_interval); // e.g. seqno = 244 -> gc_boundary = 40
        if let Some((&first_key, _)) = history.anchors_lag.first_key_value() {
            if first_key < gc_boundary {
                history.gc_anchors_lag(gc_boundary);

                tracing::trace!(
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

        let history = self.history.entry(shard).or_default();

        if seqno < wu_params_ma_seqno
            || wu_params_ma_seqno == 0
            || self.last_calculated_wu_params_seqno == wu_params_ma_seqno
        {
            return Ok(());
        }

        // get MA from MA lag on 1/2 of tune interval
        let avg_from_boundary = wu_params_ma_seqno.saturating_sub(tune_interval / 2);
        let avg_range = history.avg_anchors_lag.range((
            Bound::Included(avg_from_boundary),
            Bound::Excluded(wu_params_ma_seqno),
        ));
        let Some(avg_lag) = safe_anchors_lag_avg_2(avg_range) else {
            return Ok(());
        };

        // take just last avg wu metrics
        let Some((_, avg_wu_metrics)) = history.avg_metrics.last_key_value() else {
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
                        if let Some((&first_key, _)) = self.adjustments.first_key_value() {
                            if first_key < gc_boundary {
                                self.adjustments.retain(|k, _| k >= &gc_boundary);
                            }
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

macro_rules! avg_accum {
    ($self:ident, $field:expr) => {
        $self.avg.accum_next($field);
    };
}

macro_rules! avg_accum_list {
    ($self:ident; $($field:expr),* $(,)?) => {
        $( avg_accum!($self, $field); )*
    };
}

pub struct WuMetricsAvg {
    last_wu_params: WorkUnitsParams,
    last_shard_accounts_count: u64,
    had_pending_messages: bool,
    avg: SafeUnsignedVecAvg,
}

impl WuMetricsAvg {
    pub fn new() -> Self {
        Self {
            last_wu_params: Default::default(),
            last_shard_accounts_count: 0,
            had_pending_messages: true,
            avg: SafeUnsignedVecAvg::new(53),
        }
    }

    pub fn accum(&mut self, v: &WuMetrics) {
        self.last_wu_params = v.wu_params.clone();
        self.had_pending_messages = self.had_pending_messages && v.has_pending_messages;

        self.last_shard_accounts_count = v.wu_on_finalize.shard_accounts_count;

        self.avg.accum(0, v.wu_on_finalize.updated_accounts_count);
        avg_accum_list!(self;
            v.wu_on_finalize.in_msgs_len,
            v.wu_on_finalize.out_msgs_len,
            v.wu_on_execute.inserted_new_msgs_count,
        );

        // wu_on_prepare_msg_groups
        avg_accum_list!(self;
            v.wu_on_prepare_msg_groups.fixed_part,
            v.wu_on_prepare_msg_groups.read_ext_msgs_count,
            v.wu_on_prepare_msg_groups.read_ext_msgs_wu,
            v.wu_on_prepare_msg_groups.read_ext_msgs_elapsed.as_nanos(),
            v.wu_on_prepare_msg_groups.read_existing_int_msgs_count,
            v.wu_on_prepare_msg_groups.read_existing_int_msgs_wu,
            v.wu_on_prepare_msg_groups.read_existing_int_msgs_elapsed.as_nanos(),
            v.wu_on_prepare_msg_groups.read_new_int_msgs_count,
            v.wu_on_prepare_msg_groups.read_new_int_msgs_wu,
            v.wu_on_prepare_msg_groups.read_new_int_msgs_elapsed.as_nanos(),
            v.wu_on_prepare_msg_groups.add_to_msgs_groups_ops_count,
            v.wu_on_prepare_msg_groups.add_msgs_to_groups_wu,
            v.wu_on_prepare_msg_groups.add_msgs_to_groups_elapsed.as_nanos(),
            v.wu_on_prepare_msg_groups.total_elapsed.as_nanos(),
        );

        // wu_on_execute
        avg_accum_list!(self;
            v.wu_on_execute.groups_count,
            v.wu_on_execute.sum_gas,
            v.wu_on_execute.avg_group_accounts_count.get_avg_checked().unwrap_or_default(),
            v.wu_on_execute.avg_threads_count.get_avg_checked().unwrap_or_default(),
            v.wu_on_execute.execute_groups_vm_only_wu,
            v.wu_on_execute.execute_groups_vm_only_elapsed.as_nanos(),
            v.wu_on_execute.process_txs_wu,
            v.wu_on_execute.process_txs_elapsed.as_nanos(),
        );

        // wu_on_finalize
        avg_accum_list!(self;
            v.wu_on_finalize.diff_msgs_count,
            v.wu_on_finalize.create_queue_diff_wu,
            v.wu_on_finalize.create_queue_diff_elapsed.as_nanos(),
            v.wu_on_finalize.apply_queue_diff_wu,
            v.wu_on_finalize.apply_queue_diff_elapsed.as_nanos(),
            v.wu_on_finalize.update_shard_accounts_wu,
            v.wu_on_finalize.update_shard_accounts_elapsed.as_nanos(),
            v.wu_on_finalize.build_accounts_blocks_wu,
            v.wu_on_finalize.build_accounts_blocks_elapsed.as_nanos(),
            v.wu_on_finalize.build_accounts_elapsed.as_nanos(),
            v.wu_on_finalize.build_in_msgs_wu,
            v.wu_on_finalize.build_in_msgs_elapsed.as_nanos(),
            v.wu_on_finalize.build_out_msgs_wu,
            v.wu_on_finalize.build_out_msgs_elapsed.as_nanos(),
            v.wu_on_finalize.build_accounts_and_messages_in_parallel_elased.as_nanos(),
            v.wu_on_finalize.build_state_update_wu,
            v.wu_on_finalize.build_state_update_elapsed.as_nanos(),
            v.wu_on_finalize.build_block_wu,
            v.wu_on_finalize.build_block_elapsed.as_nanos(),
            v.wu_on_finalize.finalize_block_elapsed.as_nanos(),
            v.wu_on_finalize.total_elapsed.as_nanos(),
        );

        // wu_on_do_collate
        avg_accum_list!(self;
            v.wu_on_do_collate.resume_collation_wu,
            v.wu_on_do_collate.resume_collation_elapsed.as_nanos(),
            v.wu_on_do_collate.resume_collation_wu_per_block,
            v.wu_on_do_collate.resume_collation_elapsed_per_block_ns,
            v.wu_on_do_collate.collation_total_elapsed.as_nanos(),
        );
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
