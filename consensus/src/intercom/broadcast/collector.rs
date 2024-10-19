use std::{cmp, mem};

use ahash::HashSetExt;
use futures_util::future::BoxFuture;
use futures_util::stream::FuturesUnordered;
use futures_util::{future, FutureExt, StreamExt};
use tokio::sync::{mpsc, oneshot, watch};
use tycho_network::PeerId;
use tycho_util::FastHashSet;

use crate::dag::{DagRound, InclusionState};
use crate::dyn_event;
use crate::effects::{AltFormat, CollectorContext, Effects};
use crate::engine::{CachedConfig, Genesis};
use crate::intercom::broadcast::dto::ConsensusEvent;
use crate::intercom::BroadcasterSignal;
use crate::models::Round;

/// collector may run without broadcaster, as if broadcaster signalled Ok
#[derive(Copy, Clone, Debug)]
pub enum CollectorSignal {
    Finish, // must be sent last
    Err,    // must be sent last
    Retry,
}

pub struct Collector {
    from_bcast_filter: mpsc::UnboundedReceiver<ConsensusEvent>,
    next_round: Round,
    next_includes: Option<FuturesUnordered<BoxFuture<'static, InclusionState>>>,
    max_ready_includes_round: Round,
}

impl Collector {
    pub fn new(from_bcast_filter: mpsc::UnboundedReceiver<ConsensusEvent>) -> Self {
        Self {
            from_bcast_filter,
            next_round: Round::BOTTOM,
            next_includes: None,
            max_ready_includes_round: Genesis::round(),
        }
    }

    pub fn init(
        &mut self,
        start_round: Round,
        start_with_includes: FuturesUnordered<BoxFuture<'static, InclusionState>>,
    ) {
        self.next_round = start_round;
        self.next_includes = Some(start_with_includes);
    }

    pub fn includes_ready_round(&self) -> Round {
        self.max_ready_includes_round
    }

    pub async fn run(
        &mut self,
        effects: Effects<CollectorContext>,
        next_dag_round: DagRound, // r+1
        own_point_state: BoxFuture<'static, InclusionState>,
        collector_signal: watch::Sender<CollectorSignal>,
        bcaster_signal: oneshot::Receiver<BroadcasterSignal>,
    ) -> Round {
        let span_guard = effects.span().clone().entered();

        let current_dag_round = next_dag_round
            .prev()
            .upgrade()
            .expect("current DAG round must be linked into DAG chain");

        let includes = match current_dag_round.round().cmp(&self.next_round) {
            cmp::Ordering::Less => panic!(
                "attempt to run at {:?}, expected at least {:?}",
                current_dag_round.round(),
                self.next_round
            ),
            cmp::Ordering::Equal => {
                // just ok, no jump by engine happened (but includes may be removed by last run)
                mem::take(&mut self.next_includes).unwrap_or_default()
            }
            cmp::Ordering::Greater => {
                // engine jumped to a future round, driven by broadcasts; includes are outdated
                self.next_includes = None;
                FuturesUnordered::new()
            }
        };
        includes.push(own_point_state);

        self.next_round = next_dag_round.round();
        let includes_ready = FastHashSet::with_capacity(current_dag_round.peer_count().full());
        let mut task = CollectorTask {
            effects,
            current_round: current_dag_round.clone(),
            next_dag_round,
            includes,
            includes_ready,
            is_includes_ready: false,
            next_includes: FuturesUnordered::new(),
            max_ready_includes_round: self.max_ready_includes_round,
            collector_signal,
            is_bcaster_ready_ok: false,
        };

        drop(span_guard);

        let result = task.run(&mut self.from_bcast_filter, bcaster_signal).await;
        metrics::counter!("tycho_mempool_collected_includes_count")
            .increment(task.includes_ready.len() as _);

        match result {
            Ok(()) => {
                // no jump by task - prepare for Engine will not jump too
                self.next_includes = Some(task.next_includes);
                // self.next_round is up-to-date
            }
            Err(round) => {
                // self.next_includes are already cleared
                self.next_round = round;
            }
        }
        self.max_ready_includes_round = task.max_ready_includes_round;

        self.next_round
    }
}
struct CollectorTask {
    // for node running @ r+0:
    effects: Effects<CollectorContext>,
    current_round: DagRound,  // = r+0
    next_dag_round: DagRound, /* = r+1 is always in DAG; contains the keypair to produce point @ r+1 */

    // @ r+0, will become includes in point @ r+1
    // needed in order to not include same point twice - as an include and as a witness;
    // need to drop them with round change
    includes: FuturesUnordered<BoxFuture<'static, InclusionState>>,
    includes_ready: FastHashSet<PeerId>,
    max_ready_includes_round: Round,
    is_includes_ready: bool,
    /// do not poll during this round, just pass to next round;
    /// anyway should rewrite signing mechanics - look for comments inside [`DagRound`::`add_exact`]
    next_includes: FuturesUnordered<BoxFuture<'static, InclusionState>>,
    /// Receiver may be closed (bcaster finished), so do not require `Ok` on send
    collector_signal: watch::Sender<CollectorSignal>,
    is_bcaster_ready_ok: bool,
}

impl CollectorTask {
    /// includes @ r+0 must include own point @ r+0 iff the one is produced

    /// returns includes for our point at the next round
    async fn run(
        &mut self,
        from_bcast_filter: &mut mpsc::UnboundedReceiver<ConsensusEvent>,
        mut bcaster_signal: oneshot::Receiver<BroadcasterSignal>,
    ) -> Result<(), Round> {
        let mut retry_interval = tokio::time::interval(CachedConfig::broadcast_retry());
        loop {
            tokio::select! {
                biased; // mandatory priority: signals lifecycle, updates, data lifecycle
                // jump is rare and must not be postponed
                Some(state) = self.next_includes.next(), if ! self.is_includes_ready => {
                    if let Some(result) = self.jump_up(state) {
                        return result
                    }
                },
                // broadcaster signal is rare and must not be postponed
                Ok(bcaster_signal) = &mut bcaster_signal, if !self.is_bcaster_ready_ok => {
                    if self.should_fail(bcaster_signal) {
                        // has to jump over one round
                        // return Err(self.next_dag_round.round().next())
                        return Ok(()) // step to next round, preserving next includes
                    }
                    // bcaster sends its signal immediately after receiving Signal::Retry,
                    // so we don't have to wait for one more interval
                    if self.is_ready() {
                        return Ok(())
                    }
                },
                // tick is more frequent than bcaster signal, leads to completion too
                _ = retry_interval.tick() => {
                    if self.is_ready() {
                        return Ok(())
                    } else {
                        _ = self.collector_signal.send_replace(CollectorSignal::Retry);
                    }
                },
                // very frequent event that may seldom cause completion
                filtered = from_bcast_filter.recv() => match filtered {
                    Some(consensus_event) => {
                        if let Err(round) = self.match_filtered(consensus_event) {
                            _ = self.collector_signal.send_replace(CollectorSignal::Err); // last signal
                            return Err(round)
                        }
                    },
                    None => panic!("channel from Broadcast Filter closed"),
                },
                // frequent event that does not cause completion by itself
                Some(state) = self.includes.next() => {
                    self.on_inclusion_validated(&state);
                },
                else => panic!("unhandled match arm in Collector tokio::select"),
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
            parent: self.effects.span(),
            result = result,
            bcaster_signal = debug(signal),
            includes = self.includes_ready.len(),
            majority = self.current_round.peer_count().majority(),
            "should fail?",
        );
        result
    }

    fn is_ready(&mut self) -> bool {
        // point @ r+1 has to include 2F+1 broadcasts @ r+0 (we are @ r+0)
        self.is_includes_ready |=
            self.includes_ready.len() >= self.current_round.peer_count().majority();
        if self.is_includes_ready {
            self.max_ready_includes_round = self
                .max_ready_includes_round
                .max(self.current_round.round());
        }
        let result = self.is_includes_ready && self.is_bcaster_ready_ok;
        if result {
            _ = self.collector_signal.send_replace(CollectorSignal::Finish); // last signal
        }
        tracing::debug!(
            parent: self.effects.span(),
            result = result,
            includes = self.includes_ready.len(),
            majority = self.current_round.peer_count().majority(),
            "ready?",
        );
        result
    }

    fn jump_up(&mut self, state: InclusionState) -> Option<Result<(), Round>> {
        // its ok to discard invalid state from `next_includes` queue
        let point_round = state.point()?.valid()?.info.round();
        // will be signed on the next round
        self.next_includes.push(future::ready(state).boxed());
        self.is_includes_ready = true;
        self.max_ready_includes_round = self.max_ready_includes_round.max(point_round.prev());
        let result = match point_round.cmp(&self.next_dag_round.round()) {
            cmp::Ordering::Less => {
                let _guard = self.effects.span().enter();
                panic!("Coding error: next includes futures contain current or previous round")
            }
            cmp::Ordering::Greater => {
                tracing::error!(
                    parent: self.effects.span(),
                    "Collector was left behind while broadcast filter advanced ?"
                );
                _ = self.collector_signal.send_replace(CollectorSignal::Err); // last signal
                Some(Err(point_round))
            }
            cmp::Ordering::Equal => {
                if self.is_ready() {
                    Some(Ok(()))
                } else {
                    None
                }
            }
        };
        if let Some(decided) = result {
            tracing::info!(
                parent: self.effects.span(),
                finished = decided.is_ok(),
                to_round = point_round.0,
                "jump"
            );
        }
        result
    }

    fn match_filtered(&self, consensus_event: ConsensusEvent) -> Result<(), Round> {
        match consensus_event {
            ConsensusEvent::Forward(consensus_round) => {
                #[allow(clippy::match_same_arms)] // for comments
                let should_fail = match consensus_round.cmp(&self.next_dag_round.round()) {
                    // we're too late, consensus moved forward
                    // FIXME (bug!) engine can jump and try to produce own point at received round,
                    //  expecting that some point (from this received round) is locally validated
                    //  so that its dependencies are ready to be reused.
                    //  But validation may not be finished yet and deps are not ready.
                    //  Have to wait to the end of validation with 'inclusion state'.
                    cmp::Ordering::Greater => true,
                    // we still have a chance to finish current round
                    cmp::Ordering::Equal => false,
                    // we are among the fastest nodes of consensus
                    cmp::Ordering::Less => false,
                };
                let level = if should_fail {
                    tracing::Level::INFO
                } else {
                    tracing::Level::TRACE
                };
                dyn_event!(
                    parent: self.effects.span(),
                    level,
                    event = display("Forward"),
                    round = consensus_round.0,
                    "from bcast filter",
                );
                if should_fail {
                    // next local round may be finished shortly after first point at consensus
                    // round is validated, to produce point at consensus round
                    // (it's a temporary workaround for a bug described above)
                    return Err(consensus_round.prev());
                }
            }
            ConsensusEvent::Validating { point_id, task } => {
                if point_id.round > self.next_dag_round.round() {
                    let _guard = self.effects.span().enter();
                    panic!(
                        "Coding error: broadcast filter advanced \
                         while collector left behind; Validating {:?}",
                        point_id.alt()
                    )
                } else if point_id.round == self.next_dag_round.round() {
                    self.next_includes.push(task);
                } else if point_id.round == self.current_round.round() {
                    self.includes.push(task);
                } // else maybe other's dependency, but too old to be included
                tracing::debug!(
                    parent: self.effects.span(),
                    event = display("Validating"),
                    author = display(point_id.author.alt()),
                    round = point_id.round.0,
                    digest = display(point_id.digest.alt()),
                    "from bcast filter",
                );
            }
        };
        Ok(())
    }

    // FIXME not so great: some signature requests will be retried,
    //  just because this futures were not polled. Refactor `InclusionState`
    fn on_inclusion_validated(&mut self, state: &InclusionState) {
        let Some(dag_point) = state.point() else {
            let _guard = self.effects.span().enter();
            panic!("Coding error: validated inclusion state must be non empty")
        };
        let signed = match state.signable() {
            Some(signable) => {
                signable.sign(self.current_round.round(), self.next_dag_round.key_pair())
            }
            None => false,
        };
        let point_signed = state.signed().map_or(false, |result| result.is_ok());
        let point_included = point_signed && dag_point.round() == self.current_round.round();
        if point_included {
            self.includes_ready.insert(dag_point.author());
        }
        let level = if dag_point.trusted().is_some() {
            tracing::Level::TRACE
        } else {
            tracing::Level::WARN
        };
        dyn_event!(
            parent: self.effects.span(),
            level,
            result = display(dag_point.alt()),
            author = display(dag_point.author().alt()),
            round = dag_point.round().0,
            digest = display(dag_point.digest().alt()),
            signed = signed,
            point_signed = point_signed,
            point_included = point_included,
            includes = self.includes_ready.len(),
            "inclusion validated"
        );
    }
}
