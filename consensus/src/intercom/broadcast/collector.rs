use std::cmp;
use std::time::Duration;
use tokio::sync::{oneshot, watch};
use tokio::time::MissedTickBehavior;
use crate::dag::{DagHead, DagRound};
use crate::dyn_event;
use crate::effects::{CollectCtx, Ctx, TaskResult};
use crate::engine::round_watch::{Consensus, RoundWatcher};
use crate::intercom::BroadcasterSignal;
use crate::models::Round;
#[derive(Clone, Copy, Default)]
pub struct CollectorStatus {
    /// collector fires attempt=1 immediately when starts;
    /// wraps around if a loop takes too long to retry broadcast as a heartbeat
    pub attempt: u8,
    /// `true` is "soft success", while watch channel close is "hard fail"
    pub ready: bool,
}
pub struct Collector {
    consensus_round: RoundWatcher<Consensus>,
}
impl Collector {
    pub fn new(consensus_round: RoundWatcher<Consensus>) -> Self {
        Self { consensus_round }
    }
    /// to run collector without broadcaster, send Ok to `bcaster_signal`
    pub async fn run(
        self,
        ctx: CollectCtx,
        head: DagHead,
        status: watch::Sender<CollectorStatus>,
        bcaster_signal: oneshot::Receiver<BroadcasterSignal>,
    ) -> TaskResult<Self> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(run)),
            file!(),
            39u32,
        );
        let ctx = ctx;
        let head = head;
        let status = status;
        let bcaster_signal = bcaster_signal;
        let mut task = CollectorTask {
            consensus_round: self.consensus_round,
            ctx,
            current_dag_round: head.current().clone(),
            next_round: head.next().round(),
            is_includes_ready: false,
            status,
            is_bcaster_ready_ok: false,
        };
        {
            __guard.end_section(50u32);
            let __result = task.run(bcaster_signal).await;
            __guard.start_section(50u32);
            __result
        }?;
        metrics::counter!("tycho_mempool_collected_broadcasts_count")
            .increment(head.current().threshold().count().total() as u64);
        Ok(Self {
            consensus_round: task.consensus_round,
        })
    }
}
struct CollectorTask {
    consensus_round: RoundWatcher<Consensus>,
    ctx: CollectCtx,
    current_dag_round: DagRound,
    next_round: Round,
    is_includes_ready: bool,
    /// Receiver may be closed (bcaster finished), so do not require `Ok` on send
    status: watch::Sender<CollectorStatus>,
    is_bcaster_ready_ok: bool,
}
impl CollectorTask {
    /// includes @ r+0 must include own point @ r+0 iff the one is produced
    /// returns includes for our point at the next round
    async fn run(
        &mut self,
        mut bcaster_signal: oneshot::Receiver<BroadcasterSignal>,
    ) -> TaskResult<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(run)),
            file!(),
            79u32,
        );
        let mut bcaster_signal = bcaster_signal;
        let mut retry_interval = tokio::time::interval(
            Duration::from_millis(
                self.ctx.conf().consensus.broadcast_retry_millis.get() as _,
            ),
        );
        retry_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let current_dag_round = self.current_dag_round.clone();
        let mut threshold = std::pin::pin!(current_dag_round.threshold().reached());
        loop {
            __guard.checkpoint(89u32);
            {
                __guard.end_section(90u32);
                let __result = tokio::select! {
                    biased; () = & mut threshold, if ! self.is_includes_ready => { self
                    .is_includes_ready = true; if self.is_ready() { break; } },
                    Ok(bcaster_signal) = & mut bcaster_signal, if ! self
                    .is_bcaster_ready_ok => { if self.should_fail(bcaster_signal) {
                    break; } if self.is_ready() { break; } }, _ = retry_interval.tick()
                    => { if self.is_ready() { break; } else { self.status.send_modify(|
                    status | { status.attempt = status.attempt.wrapping_add(1); status
                    .ready = self.is_includes_ready; }); } }, consensus_round = self
                    .consensus_round.next() => { if self.match_consensus(consensus_round
                    ?).is_err() { break; } }, else =>
                    unreachable!("unhandled match arm in Collector tokio::select"),
                };
                __guard.start_section(90u32);
                __result
            }
        }
        Ok(())
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
            parent : self.ctx.span(), result = result, bcaster_signal = debug(signal),
            "should fail?",
        );
        result
    }
    fn is_ready(&self) -> bool {
        let result = self.is_includes_ready && self.is_bcaster_ready_ok;
        tracing::debug!(
            parent : self.ctx.span(), includes = display(self.current_dag_round
            .threshold().count()), bcaster_ready = self.is_bcaster_ready_ok, result =
            result, "ready?",
        );
        result
    }
    fn match_consensus(&self, consensus_round: Round) -> Result<(), ()> {
        #[allow(clippy::match_same_arms, reason = "comments")]
        let should_fail = match consensus_round.cmp(&self.next_round) {
            cmp::Ordering::Greater => true,
            cmp::Ordering::Equal => false,
            cmp::Ordering::Less => false,
        };
        let level = if should_fail {
            tracing::Level::INFO
        } else {
            tracing::Level::DEBUG
        };
        dyn_event!(
            parent : self.ctx.span(), level, round = consensus_round.0,
            "from bcast filter",
        );
        if should_fail { Err(()) } else { Ok(()) }
    }
}
