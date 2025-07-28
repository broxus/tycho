use std::mem;
use std::sync::Arc;
use std::time::Duration;

use itertools::Itertools;
use tokio::sync::mpsc;
use tokio::time::Interval;
use tycho_slasher_traits::{MempoolEventsListener, MempoolPeerStats, MempoolStatsMergeError};

use crate::dag::{Committer, HistoryConflict};
use crate::effects::{AltFormat, Cancelled, Ctx, EngineCtx, RoundCtx, Task};
use crate::engine::lifecycle::EngineError;
use crate::engine::{ConsensusConfigExt, EngineResult, MempoolConfig, NodeConfig};
use crate::models::{AnchorData, MempoolOutput, MempoolStatsOutput, PointInfo, Round};
use crate::moderator::Moderator;

pub struct CommitterTask {
    state: State,
    pub interval: Interval,
}

enum State {
    Uninit,
    Ready(Box<Inner>),
    Running(Task<Result<Box<Inner>, EngineError>>),
}

struct Inner {
    committer: Committer,
    moderator: Moderator,
}

impl CommitterTask {
    pub fn new(committer: Committer, moderator: &Moderator, conf: &MempoolConfig) -> Self {
        let mut interval = tokio::time::interval(Duration::from_millis(
            conf.consensus.broadcast_retry_millis.get() as _,
        ));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        Self {
            state: State::Ready(Box::new(Inner {
                committer,
                moderator: moderator.clone(),
            })),
            interval,
        }
    }

    pub async fn ready_mut(&mut self) -> EngineResult<Option<&mut Committer>> {
        if let Some(inner) = self.state.take_ready().await? {
            self.state = State::Ready(inner);
        };
        Ok(match &mut self.state {
            State::Ready(inner) => Some(&mut inner.committer),
            _ => None,
        })
    }

    pub async fn update_task(
        &mut self,
        full_history_bottom: Option<Round>,
        anchors_tx: mpsc::UnboundedSender<MempoolOutput>,
        stats_tx: Arc<dyn MempoolEventsListener>,
        round_ctx: &RoundCtx,
    ) -> EngineResult<()> {
        let Some(inner) = self.state.take_ready().await? else {
            return Ok(());
        };
        self.state = State::running(inner, full_history_bottom, anchors_tx, stats_tx, round_ctx);
        Ok(())
    }
}

impl State {
    async fn take_ready(&mut self) -> EngineResult<Option<Box<Inner>>> {
        Ok(match mem::replace(self, State::Uninit) {
            State::Uninit => panic!("must be taken only once"),
            State::Ready(inner) => Some(inner),
            State::Running(task) => {
                if task.is_finished() {
                    Some(task.await??)
                } else {
                    *self = Self::Running(task);
                    None
                }
            }
        })
    }

    fn running(
        mut inner: Box<Inner>,
        mut full_history_bottom: Option<Round>,
        anchors_tx: mpsc::UnboundedSender<MempoolOutput>,
        stats_tx: Arc<dyn MempoolEventsListener>,
        round_ctx: &RoundCtx,
    ) -> Self {
        let task_ctx = round_ctx.task();
        let round_ctx = round_ctx.clone();
        let task = move || {
            // may run for long several times in a row and commit nothing, because of missed points
            let _span = round_ctx.span().enter();

            let start_bottom = inner.committer.bottom_round().0;
            let start_dag_len = inner.committer.dag_len();

            let mut attempt = 0;
            let committed = loop {
                attempt += 1;
                let is_dropping =
                    inner.committer.dag_len() > round_ctx.conf().consensus.min_front_rounds() as _;
                match inner.committer.commit(round_ctx.conf()) {
                    Ok(data) => break Some(data),
                    Err(HistoryConflict(round)) if is_dropping => {
                        let result = inner.committer.drop_upto(round.next(), round_ctx.conf());
                        full_history_bottom = Some(result.unwrap_or_else(|x| x));
                        tracing::info!(
                            start_bottom,
                            start_dag_len,
                            current_bottom = ?inner.committer.bottom_round(),
                            current_dag_len = inner.committer.dag_len(),
                            attempt,
                            "comitter rounds were dropped as impossible to sync"
                        );
                        if result.is_err() {
                            break None; // dropped all except top round
                        } else if attempt > start_dag_len {
                            panic!(
                                "infinite loop on dropping dag rounds: attempt {attempt}, \
                                 start dag len {start_dag_len}, start bottom {start_bottom} \
                                 resulting {:?}",
                                inner.committer.alt()
                            )
                        };
                    }
                    Err(history_conflict) => {
                        tracing::warn!(
                            err = %history_conflict,
                            start_bottom,
                            start_dag_len,
                            current_bottom = ?inner.committer.bottom_round(),
                            current_dag_len = inner.committer.dag_len(),
                            attempt,
                            "will try to fix local history"
                        );
                        return Err(history_conflict.into());
                    }
                }
            };

            if let Some(new_bottom) = full_history_bottom {
                anchors_tx
                    .send(MempoolOutput::NewStartAfterGap(new_bottom))
                    .map_err(|_closed| Cancelled())?;
            }

            if let Some(committed) = committed {
                round_ctx.log_committed(&committed);
                let anchor_rounds =
                    (committed.iter().map(|a| a.anchor.round())).collect::<Vec<_>>();
                for data in committed {
                    round_ctx.commit_metrics(&data.anchor);
                    anchors_tx
                        .send(MempoolOutput::NextAnchor(data))
                        .map_err(|_closed| Cancelled())?;
                }
                // stats should be reported for each round separately - to be grouped by consumer
                let mut all_stats = Vec::with_capacity(anchor_rounds.len());
                for anchor_round in anchor_rounds {
                    let (stats, events) =
                        (inner.committer).remove_committed(anchor_round, round_ctx.conf())?;
                    all_stats.push(stats);
                    _ = stats_tx; // TODO stats_tx.put_stats(stats.anchor_round.0, stats.data);
                    for event in events {
                        inner.moderator.report(event);
                    }
                    anchors_tx
                        .send(MempoolOutput::CommitFinished(anchor_round))
                        .map_err(|_closed| Cancelled())?;
                }
                round_ctx.meter_stats(all_stats);
            }

            EngineCtx::meter_dag_len(inner.committer.dag_len());

            Ok(inner)
        };

        Self::Running(task_ctx.spawn_blocking(task))
    }
}

impl RoundCtx {
    fn commit_metrics(&self, anchor: &PointInfo) {
        metrics::counter!("tycho_mempool_commit_anchors").increment(1);
        metrics::gauge!("tycho_mempool_commit_latency_rounds").set(self.depth(anchor.round()));
    }

    fn log_committed(&self, committed: &[AnchorData]) {
        if !committed.is_empty() && tracing::enabled!(tracing::Level::DEBUG) {
            let committed = committed
                .iter()
                .map(|data| {
                    let history = data
                        .history
                        .iter()
                        .map(|point| format!("{:?}", point.id().alt()))
                        .join(", ");
                    format!(
                        "anchor {:?} time {} : [ {history} ]",
                        data.anchor.id().alt(),
                        data.anchor.time()
                    )
                })
                .join("  ;  ");
            tracing::debug!(
                parent: self.span(),
                "committed {committed}"
            );
        }
    }

    fn merge_stats(&self, acc: &mut MempoolPeerStats, other: &MempoolPeerStats) {
        let error = match acc.merge_with(other) {
            Ok(()) => return,
            Err(error) => error,
        };
        tracing::error!(
            parent: self.span(),
            %error,
            "ok only after unrecoverable gap"
        );
        let kind = match error {
            MempoolStatsMergeError::RoundOutOfOrder(_, _) => "rounds order",
            MempoolStatsMergeError::OverlappingRanges(_, _) => "ranges overlap",
        };
        metrics::counter!("tycho_mempool_stats_merge_errors", "kind" => kind).increment(1);
    }

    fn meter_stats(&self, all_stats: Vec<MempoolStatsOutput>) {
        let reduced = all_stats
            .into_iter()
            .map(|stats| stats.data)
            .reduce(|mut acc, other_map| {
                for (peer_id, stats) in other_map {
                    acc.entry(peer_id)
                        .and_modify(|occupied| self.merge_stats(occupied, &stats))
                        .or_insert(stats);
                }
                acc
            });

        let Some(reduced) = reduced else {
            return;
        };

        if !NodeConfig::get().emit_stats_metrics {
            return; // reduce anyway to catch merge error
        }

        macro_rules! emit_counters {
            ($prefix:literal, $stats:expr, $labels:expr, [ $($field:ident),* $(,)? ]) => {
                $(
                    metrics::counter!(concat!($prefix, stringify!($field)), $labels)
                        .increment(u64::from($stats.$field));
                )*
            };
        }

        for (peer_id, stats) in &reduced {
            let labels = [("peer_id", format!("{:.4}", peer_id))];
            metrics::counter!("tycho_mempool_stats_filled_rounds", &labels)
                .increment(u64::from(stats.filled_rounds()));
            if let Some(counters) = stats.counters() {
                metrics::gauge!("tycho_mempool_stats_last_round", &labels)
                    .set(f64::from(counters.last_round));
                emit_counters!("tycho_mempool_stats_", counters, &labels, [
                    was_leader,
                    was_not_leader,
                    skipped_rounds,
                    valid_points,
                    equivocated,
                    invalid_points,
                    ill_formed_points,
                    references_skipped,
                ]);
            }
        }
    }
}
