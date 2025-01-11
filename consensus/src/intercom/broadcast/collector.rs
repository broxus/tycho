use std::cmp;
use std::time::Duration;

use tokio::sync::{oneshot, watch};
use tokio::time::MissedTickBehavior;

use crate::dag::{DagHead, DagRound};
use crate::dyn_event;
use crate::effects::{CollectCtx, Ctx};
use crate::engine::round_watch::{Consensus, RoundWatcher};
use crate::intercom::BroadcasterSignal;
use crate::models::Round;

/// collector may run without broadcaster, as if broadcaster signalled Ok
#[derive(Copy, Clone, Debug)]
pub enum CollectorSignal {
    Retry { ready: bool },
}

pub struct Collector {
    consensus_round: RoundWatcher<Consensus>,
}

impl Collector {
    pub fn new(consensus_round: RoundWatcher<Consensus>) -> Self {
        Self { consensus_round }
    }

    pub async fn run(
        self,
        ctx: CollectCtx,
        head: DagHead,
        collector_signal: watch::Sender<CollectorSignal>,
        bcaster_signal: oneshot::Receiver<BroadcasterSignal>,
    ) -> Self {
        let mut task = CollectorTask {
            consensus_round: self.consensus_round,
            ctx,
            current_dag_round: head.current().clone(),
            next_round: head.next().round(),
            is_includes_ready: false,
            collector_signal,
            is_bcaster_ready_ok: false,
        };

        task.run(bcaster_signal).await;

        metrics::counter!("tycho_mempool_collected_broadcasts_count")
            .increment(head.current().threshold().count().total() as u64);
        Self {
            consensus_round: task.consensus_round,
        }
    }
}
struct CollectorTask {
    consensus_round: RoundWatcher<Consensus>,
    // for node running @ r+0:
    ctx: CollectCtx,

    current_dag_round: DagRound,
    next_round: Round,

    is_includes_ready: bool,
    /// Receiver may be closed (bcaster finished), so do not require `Ok` on send
    collector_signal: watch::Sender<CollectorSignal>,
    is_bcaster_ready_ok: bool,
}

impl CollectorTask {
    /// includes @ r+0 must include own point @ r+0 iff the one is produced
    /// returns includes for our point at the next round
    async fn run(&mut self, mut bcaster_signal: oneshot::Receiver<BroadcasterSignal>) {
        let mut retry_interval = tokio::time::interval(Duration::from_millis(
            self.ctx.conf().consensus.broadcast_retry_millis as _,
        ));
        // no `interval.reset()` as may receive bcaster_signal after jump immediately
        retry_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let current_dag_round = self.current_dag_round.clone();
        let mut threshold = std::pin::pin!(current_dag_round.threshold().reached());

        loop {
            tokio::select! {
                biased; // mandatory priority: signals lifecycle, updates, data lifecycle
                () = &mut threshold, if !self.is_includes_ready => {
                    self.is_includes_ready = true;
                    if self.is_ready() {
                        return;
                    }
                },
                // broadcaster signal is rare and must not be postponed
                Ok(bcaster_signal) = &mut bcaster_signal, if !self.is_bcaster_ready_ok => {
                    if self.should_fail(bcaster_signal) {
                        // has to jump over one round
                        // return Err(self.next_dag_round.round().next())
                        return; // step to next round, preserving next includes
                    }
                    // bcaster sends its signal immediately after receiving Signal::Retry,
                    // so we don't have to wait for one more interval
                    if self.is_ready() {
                        return;
                    }
                },
                // tick is more frequent than bcaster signal, leads to completion too
                _ = retry_interval.tick() => {
                    if self.is_ready() {
                        return;
                    } else {
                        self.collector_signal.send_replace(
                            CollectorSignal::Retry { ready: self.is_includes_ready }
                        );
                    }
                },
                // very frequent event that may seldom cause completion
                consensus_round = self.consensus_round.next() => {
                    if self.match_consensus(consensus_round).is_err() {
                        return;
                    }
                },
                else => unreachable!("unhandled match arm in Collector tokio::select"),
            }
        }
    }

    fn should_fail(&mut self, signal: BroadcasterSignal) -> bool {
        let result = match signal {
            BroadcasterSignal::Ok => {
                self.is_bcaster_ready_ok = true;
                false
            }
            BroadcasterSignal::Err => true,
        };
        tracing::debug!(
            parent: self.ctx.span(),
            result = result,
            bcaster_signal = debug(signal),
            "should fail?",
        );
        result
    }

    fn is_ready(&self) -> bool {
        // point @ r+1 has to include 2F+1 broadcasts @ r+0 (we are @ r+0)
        let result = self.is_includes_ready && self.is_bcaster_ready_ok;
        tracing::debug!(
            parent: self.ctx.span(),
            includes = display(self.current_dag_round.threshold().count()),
            bcaster_ready = self.is_bcaster_ready_ok,
            result = result,
            "ready?",
        );
        result
    }

    fn match_consensus(&self, consensus_round: Round) -> Result<(), ()> {
        #[allow(clippy::match_same_arms, reason = "comments")]
        let should_fail = match consensus_round.cmp(&self.next_round) {
            // we're too late, consensus moved forward
            cmp::Ordering::Greater => true,
            // we still have a chance to finish current round
            cmp::Ordering::Equal => false,
            // we are among the fastest nodes of consensus
            cmp::Ordering::Less => false,
        };
        let level = if should_fail {
            tracing::Level::INFO
        } else {
            tracing::Level::DEBUG
        };
        dyn_event!(
            parent: self.ctx.span(),
            level,
            round = consensus_round.0,
            "from bcast filter",
        );
        if should_fail {
            Err(())
        } else {
            Ok(())
        }
    }
}
