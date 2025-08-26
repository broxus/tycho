use std::mem;
use std::time::Duration;

use itertools::Itertools;
use tokio::sync::mpsc;
use tokio::time::Interval;

use crate::dag::{Committer, HistoryConflict};
use crate::effects::{AltFormat, Cancelled, Ctx, EngineCtx, RoundCtx, Task};
use crate::engine::lifecycle::EngineError;
use crate::engine::{ConsensusConfigExt, EngineResult, MempoolConfig};
use crate::models::{AnchorData, MempoolOutput, PointInfo, Round};

pub struct CommitterTask {
    inner: Inner,
    pub interval: Interval,
}

enum Inner {
    Uninit,
    Ready(Committer),
    Running(Task<Result<Committer, EngineError>>),
}

impl CommitterTask {
    pub fn new(committer: Committer, conf: &MempoolConfig) -> Self {
        let mut interval = tokio::time::interval(Duration::from_millis(
            conf.consensus.broadcast_retry_millis as _,
        ));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        Self {
            inner: Inner::Ready(committer),
            interval,
        }
    }

    pub async fn ready_mut(&mut self) -> EngineResult<Option<&mut Committer>> {
        if let Some(committer) = self.inner.take_ready().await? {
            self.inner = Inner::Ready(committer);
        };
        Ok(match &mut self.inner {
            Inner::Ready(committer) => Some(committer),
            _ => None,
        })
    }

    pub async fn update_task(
        &mut self,
        full_history_bottom: Option<Round>,
        anchors_tx: mpsc::UnboundedSender<MempoolOutput>,
        round_ctx: &RoundCtx,
    ) -> EngineResult<()> {
        let Some(committer) = self.inner.take_ready().await? else {
            return Ok(());
        };
        self.inner = Inner::running(committer, full_history_bottom, anchors_tx, round_ctx);
        Ok(())
    }
}

impl Inner {
    async fn take_ready(&mut self) -> EngineResult<Option<Committer>> {
        Ok(match mem::replace(self, Inner::Uninit) {
            Inner::Uninit => panic!("must be taken only once"),
            Inner::Ready(committer) => Some(committer),
            Inner::Running(task) => {
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
        mut committer: Committer,
        mut full_history_bottom: Option<Round>,
        anchors_tx: mpsc::UnboundedSender<MempoolOutput>,
        round_ctx: &RoundCtx,
    ) -> Self {
        let task_ctx = round_ctx.task();
        let round_ctx = round_ctx.clone();
        let task = move || {
            // may run for long several times in a row and commit nothing, because of missed points
            let _span = round_ctx.span().enter();

            let start_bottom = committer.bottom_round().0;
            let start_dag_len = committer.dag_len();

            let mut attempt = 0;
            let committed = loop {
                attempt += 1;
                let is_dropping =
                    committer.dag_len() > round_ctx.conf().consensus.min_front_rounds() as _;
                match committer.commit(round_ctx.conf()) {
                    Ok(data) => break Some(data),
                    Err(HistoryConflict(round)) if is_dropping => {
                        let result = committer.drop_upto(round.next(), round_ctx.conf());
                        full_history_bottom = Some(result.unwrap_or_else(|x| x));
                        tracing::info!(
                            start_bottom,
                            start_dag_len,
                            current_bottom = ?committer.bottom_round(),
                            current_dag_len = committer.dag_len(),
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
                                committer.alt()
                            )
                        };
                    }
                    Err(history_conflict) => {
                        tracing::warn!(
                            err = %history_conflict,
                            start_bottom,
                            start_dag_len,
                            current_bottom = ?committer.bottom_round(),
                            current_dag_len = committer.dag_len(),
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
                for data in committed {
                    let anchor_round = data.anchor.round();
                    round_ctx.commit_metrics(&data.anchor);
                    anchors_tx
                        .send(MempoolOutput::NextAnchor(data))
                        .map_err(|_closed| Cancelled())?;
                    anchors_tx
                        .send(MempoolOutput::CommitFinished(anchor_round))
                        .map_err(|_closed| Cancelled())?;
                }
            }

            EngineCtx::meter_dag_len(committer.dag_len());

            Ok(committer)
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
}
