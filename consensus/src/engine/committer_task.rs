use std::mem;
use std::time::Duration;

use itertools::Itertools;
use tokio::sync::mpsc;
use tokio::time::Interval;

use crate::dag::Committer;
use crate::effects::{AltFormat, Cancelled, Ctx, EngineCtx, RoundCtx, Task};
use crate::engine::MempoolConfig;
use crate::models::{AnchorData, MempoolOutput, PointInfo, Round};

pub struct CommitterTask {
    inner: Inner,
    pub interval: Interval,
}

enum Inner {
    Uninit,
    Ready(Committer),
    Dropping(Task<Result<Committer, Cancelled>>),
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

    pub async fn ready_mut(&mut self) -> Result<Option<&mut Committer>, Cancelled> {
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
        committed_info_tx: mpsc::UnboundedSender<MempoolOutput>,
        round_ctx: &RoundCtx,
    ) -> Result<(), Cancelled> {
        let Some(committer) = self.inner.take_ready().await? else {
            return Ok(());
        };
        self.inner = Inner::dropping(committer, full_history_bottom, committed_info_tx, round_ctx);
        Ok(())
    }
}

impl Inner {
    async fn take_ready(&mut self) -> Result<Option<Committer>, Cancelled> {
        Ok(match mem::replace(self, Inner::Uninit) {
            Inner::Uninit => panic!("must be taken only once"),
            Inner::Ready(committer) => Some(committer),
            Inner::Dropping(task) => {
                if task.is_finished() {
                    Some(task.await??)
                } else {
                    *self = Self::Dropping(task);
                    None
                }
            }
        })
    }

    /// This version does not throw `HistoryConflict`
    fn dropping(
        mut committer: Committer,
        full_history_bottom: Option<Round>,
        committed_info_tx: mpsc::UnboundedSender<MempoolOutput>,
        round_ctx: &RoundCtx,
    ) -> Self {
        let task_ctx = round_ctx.task();
        let round_ctx = round_ctx.clone();
        let dropping_task = move || {
            // may run for long several times in a row and commit nothing, because of missed points
            let _span = round_ctx.span().enter();

            let mut committed = None;
            let mut new_full_history_bottom = None;

            let start_bottom = committer.bottom_round().0;
            let start_dag_len = committer.dag_len();

            for attempt in 0.. {
                match committer.commit(round_ctx.conf()) {
                    Ok(data) => {
                        committed = Some(data);
                        break;
                    }
                    Err(round) => {
                        let result = committer.drop_upto(round.next(), round_ctx.conf());
                        new_full_history_bottom = Some(result.unwrap_or_else(|x| x));
                        tracing::info!(
                            start_bottom,
                            start_dag_len,
                            current_bottom = ?committer.bottom_round(),
                            current_dag_len = committer.dag_len(),
                            attempt,
                            "comitter rounds were dropped as impossible to sync"
                        );
                        if result.is_err() {
                            break; // dropped all except top round
                        } else if attempt > start_dag_len {
                            panic!(
                                "infinite loop on dropping dag rounds: \
                                 start dag len {start_dag_len}, start bottom {start_bottom} \
                                 resulting {:?}",
                                committer.alt()
                            )
                        };
                    }
                }
            }

            if let Some(new_bottom) = new_full_history_bottom.or(full_history_bottom) {
                committed_info_tx
                    .send(MempoolOutput::NewStartAfterGap(new_bottom))
                    .map_err(|_closed| Cancelled())?;
            }

            if let Some(committed) = committed {
                round_ctx.log_committed(&committed);
                for data in committed {
                    round_ctx.commit_metrics(&data.anchor);
                    committed_info_tx
                        .send(MempoolOutput::NextAnchor(data))
                        .map_err(|_closed| Cancelled())?;
                }
            }

            EngineCtx::meter_dag_len(committer.dag_len());

            Ok(committer)
        };

        Self::Dropping(task_ctx.spawn_blocking(dropping_task))
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
                        data.anchor.data().time
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
