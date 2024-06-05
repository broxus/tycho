use std::mem;

use futures_util::future::BoxFuture;
use futures_util::stream::FuturesUnordered;
use futures_util::{FutureExt, StreamExt};
use tokio::sync::{mpsc, oneshot};
use tycho_network::PeerId;
use tycho_util::FastHashSet;

use crate::dag::{DagRound, InclusionState};
use crate::dyn_event;
use crate::effects::{AltFormat, CurrentRoundContext, Effects, EffectsContext};
use crate::engine::MempoolConfig;
use crate::intercom::broadcast::dto::ConsensusEvent;
use crate::intercom::dto::SignatureResponse;
use crate::intercom::{BroadcasterSignal, Downloader};
use crate::models::Round;

/// collector may run without broadcaster, as if broadcaster signalled Ok
#[derive(Debug)]
pub enum CollectorSignal {
    Finish,
    Err,
    Retry,
}

type SignatureRequest = (PeerId, Round, oneshot::Sender<SignatureResponse>);

pub struct Collector {
    downloader: Downloader,
    from_bcast_filter: mpsc::UnboundedReceiver<ConsensusEvent>,
    next_round: Round,
    next_includes: FuturesUnordered<BoxFuture<'static, InclusionState>>,
}

impl Collector {
    pub fn new(
        downloader: &Downloader,
        from_bcast_filter: mpsc::UnboundedReceiver<ConsensusEvent>,
    ) -> Self {
        Self {
            downloader: downloader.clone(),
            from_bcast_filter,
            next_round: Round::BOTTOM,
            next_includes: FuturesUnordered::new(),
        }
    }

    pub fn init(&mut self, next_round: Round, next_includes: impl Iterator<Item = InclusionState>) {
        self.next_round = next_round;
        self.next_includes
            .extend(next_includes.map(|a| futures_util::future::ready(a).boxed()));
    }

    pub async fn run(
        mut self,
        round_effects: Effects<CurrentRoundContext>,
        next_dag_round: DagRound, // r+1
        own_point_state: oneshot::Receiver<InclusionState>,
        collector_signal: mpsc::UnboundedSender<CollectorSignal>,
        bcaster_signal: mpsc::Receiver<BroadcasterSignal>,
    ) -> Self {
        let effects = Effects::<CollectorContext>::new(&round_effects);
        let span_guard = effects.span().clone().entered();

        let current_dag_round = next_dag_round
            .prev()
            .get()
            .expect("current DAG round must be linked into DAG chain");
        let includes = mem::take(&mut self.next_includes);
        includes.push(
            (async move {
                match own_point_state.await {
                    Ok(state) => state,
                    Err(_) => {
                        futures_util::pending!();
                        unreachable!("dropped own point state in collector")
                    }
                }
            })
            .boxed(),
        );

        assert_eq!(
            current_dag_round.round(),
            self.next_round,
            "attempt to run at {:?}, expected {:?}",
            current_dag_round.round(),
            self.next_round
        );
        self.next_round = next_dag_round.round();
        let includes_ready = FastHashSet::with_capacity_and_hasher(
            current_dag_round.node_count().full(),
            Default::default(),
        );
        let task = CollectorTask {
            effects,
            downloader: self.downloader.clone(),
            current_round: current_dag_round.clone(),
            next_dag_round,
            includes,
            includes_ready,
            is_includes_ready: false,
            next_includes: FuturesUnordered::new(),

            collector_signal,
            bcaster_signal,
            is_bcaster_ready_ok: false,
        };

        drop(span_guard);
        let result = task.run(&mut self.from_bcast_filter).await;
        match result {
            Ok(includes) => self.next_includes = includes,
            Err(round) => self.next_round = round,
        }
        self
    }

    pub fn next_round(&self) -> Round {
        self.next_round
    }
}
struct CollectorTask {
    // for node running @ r+0:
    effects: Effects<CollectorContext>,
    downloader: Downloader,
    current_round: DagRound,  // = r+0
    next_dag_round: DagRound, /* = r+1 is always in DAG; contains the keypair to produce point @ r+1 */

    // @ r+0, will become includes in point @ r+1
    // needed in order to not include same point twice - as an include and as a witness;
    // need to drop them with round change
    includes: FuturesUnordered<BoxFuture<'static, InclusionState>>,
    includes_ready: FastHashSet<PeerId>,
    is_includes_ready: bool,
    /// do not poll during this round, just pass to next round;
    /// anyway should rewrite signing mechanics - look for comments inside [`DagRound`::`add_exact`]
    next_includes: FuturesUnordered<BoxFuture<'static, InclusionState>>,
    /// Receiver may be closed (bcaster finished), so do not require `Ok` on send
    collector_signal: mpsc::UnboundedSender<CollectorSignal>,
    bcaster_signal: mpsc::Receiver<BroadcasterSignal>,
    is_bcaster_ready_ok: bool,
}

impl CollectorTask {
    /// includes @ r+0 must include own point @ r+0 iff the one is produced

    /// returns includes for our point at the next round
    async fn run(
        mut self,
        from_bcast_filter: &mut mpsc::UnboundedReceiver<ConsensusEvent>,
    ) -> Result<FuturesUnordered<BoxFuture<'static, InclusionState>>, Round> {
        let mut retry_interval = tokio::time::interval(MempoolConfig::RETRY_INTERVAL);
        loop {
            tokio::select! {
                Some(bcaster_signal) = self.bcaster_signal.recv() => {
                    if self.should_fail(bcaster_signal) {
                        // has to jump over one round
                        return Err(self.next_dag_round.round().next())
                    }
                    // bcaster sends its signal immediately after receiving Signal::Retry,
                    // so we don't have to wait for one more interval
                    if self.is_ready() {
                        return Ok(self.next_includes)
                    }
                },
                _ = retry_interval.tick() => {
                    if self.is_ready() {
                        return Ok(self.next_includes)
                    } else {
                        _ = self.collector_signal.send(CollectorSignal::Retry);
                    }
                },
                filtered = from_bcast_filter.recv() => match filtered {
                    Some(consensus_event) => {
                        if let Err(round) = self.match_filtered(&consensus_event) {
                            _ = self.collector_signal.send(CollectorSignal::Err);
                            return Err(round)
                        }
                    },
                    None => panic!("channel from Broadcast Filter closed"),
                },
                Some(state) = self.includes.next() => {
                    self.on_inclusion_validated(&state);
                },
                Some(state) = self.next_includes.next(), if ! self.is_includes_ready => {
                    if let Some(result) = self.jump_up(state) {
                        return result.map(|_ | self.next_includes)
                    }
                },
                else => panic!("unhandled match arm in Collector tokio::select"),
            }
        }
    }

    fn should_fail(&mut self, signal: BroadcasterSignal) -> bool {
        let result = match signal {
            BroadcasterSignal::Ok => {
                self.is_bcaster_ready_ok = true;
                self.bcaster_signal.close();
                false
            }
            BroadcasterSignal::Err => true,
        };
        tracing::debug!(
            parent: self.effects.span(),
            result = result,
            bcaster_signal = debug(signal),
            includes = self.includes_ready.len(),
            majority = self.current_round.node_count().majority(),
            "should fail?",
        );
        result
    }

    fn is_ready(&mut self) -> bool {
        // point @ r+1 has to include 2F+1 broadcasts @ r+0 (we are @ r+0)
        self.is_includes_ready |=
            self.includes_ready.len() >= self.current_round.node_count().majority();
        let result = self.is_includes_ready && self.is_bcaster_ready_ok;
        if result {
            _ = self.collector_signal.send(CollectorSignal::Finish);
        }
        tracing::debug!(
            parent: self.effects.span(),
            result = result,
            includes = self.includes_ready.len(),
            majority = self.current_round.node_count().majority(),
            "ready?",
        );
        result
    }

    fn jump_up(&mut self, state: InclusionState) -> Option<Result<(), Round>> {
        // its ok to discard invalid state from `next_includes` queue
        let point_round = state.point()?.valid()?.point.body.location.round;
        // will be signed on the next round
        self.next_includes
            .push(futures_util::future::ready(state).boxed());
        self.is_includes_ready = true;
        let result = match point_round.cmp(&self.next_dag_round.round()) {
            std::cmp::Ordering::Less => {
                let _guard = self.effects.span().enter();
                panic!("Coding error: next includes futures contain current or previous round")
            }
            std::cmp::Ordering::Greater => {
                tracing::error!(
                    parent: self.effects.span(),
                    "Collector was left behind while broadcast filter advanced ?"
                );
                _ = self.collector_signal.send(CollectorSignal::Err);
                Some(Err(point_round))
            }
            std::cmp::Ordering::Equal => {
                if self.is_ready() {
                    Some(Ok(()))
                } else {
                    None
                }
            }
        };
        if let Some(decided) = result {
            tracing::warn!(
                parent: self.effects.span(),
                finished = decided.is_ok(),
                to_round = point_round.0,
                "jump"
            );
        }
        result
    }

    fn match_filtered(&self, consensus_event: &ConsensusEvent) -> Result<(), Round> {
        match consensus_event {
            ConsensusEvent::Forward(consensus_round) => {
                #[allow(clippy::match_same_arms)]
                let should_fail = match consensus_round.cmp(&self.next_dag_round.round()) {
                    // we're too late, consensus moved forward
                    std::cmp::Ordering::Greater => true,
                    // we still have a chance to finish current round
                    std::cmp::Ordering::Equal => false,
                    // we are among the fastest nodes of consensus
                    std::cmp::Ordering::Less => false,
                };
                let level = if should_fail {
                    tracing::Level::INFO
                } else {
                    tracing::Level::TRACE
                };
                dyn_event!(
                    parent: self.effects.span(),
                    level,
                    event = display(consensus_event.alt()),
                    round = consensus_round.0,
                    "from bcast filter",
                );
                if should_fail {
                    return Err(*consensus_round);
                }
            }
            ConsensusEvent::Verified(point) => {
                let is_first = match point.body.location.round {
                    x if x > self.next_dag_round.round() => {
                        let _guard = self.effects.span().enter();
                        panic!(
                            "Coding error: broadcast filter advanced \
                             while collector left behind; event: {} {:?}",
                            consensus_event.alt(),
                            point.id()
                        )
                    }
                    x if x == self.next_dag_round.round() => self
                        .next_dag_round
                        .add(point, &self.downloader, self.effects.span())
                        .map(|task| self.next_includes.push(task))
                        .is_some(),
                    x if x == self.current_round.round() => self
                        .current_round
                        .add(point, &self.downloader, self.effects.span())
                        .map(|task| self.includes.push(task))
                        .is_some(),
                    _ => self
                        .current_round
                        .add(point, &self.downloader, self.effects.span())
                        // maybe other's dependency, but too old to be included
                        .is_some(),
                };
                tracing::debug!(
                    parent: self.effects.span(),
                    event = display(consensus_event.alt()),
                    new = is_first,
                    author = display(point.body.location.author.alt()),
                    round = point.body.location.round.0,
                    digest = display(point.digest.alt()),
                    "from bcast filter",
                );
            }
            ConsensusEvent::Invalid(dag_point) => {
                if dag_point.location().round > self.next_dag_round.round() {
                    let _guard = self.effects.span().enter();
                    panic!(
                        "Coding error: broadcast filter advanced \
                         while collector left behind; event: {} {:?}",
                        consensus_event.alt(),
                        dag_point.id()
                    )
                } else {
                    let is_first = self.next_dag_round.insert_invalid(dag_point).is_some();
                    tracing::warn!(
                        parent: self.effects.span(),
                        event = display(consensus_event.alt()),
                        new = is_first,
                        author = display(dag_point.location().author.alt()),
                        round = dag_point.location().round.0,
                        digest = display(dag_point.digest().alt()),
                        "from bcast filter",
                    );
                }
            }
        };
        Ok(())
    }

    // FIXME not so great: some signature requests will be retried,
    //  just because this futures were not polled. Use global 'current dag round' round
    //  and sign inside shared join task in dag location,
    //  do not return location from DagLocation::add_validate(point)
    fn on_inclusion_validated(&mut self, state: &InclusionState) {
        let Some(dag_point) = state.point() else {
            let _guard = self.effects.span().enter();
            panic!("Coding error: validated inclusion state must be non empty")
        };
        let signed = match state.signable() {
            Some(signable) => signable.sign(
                self.current_round.round(),
                self.next_dag_round.key_pair(),
                MempoolConfig::sign_time_range(),
            ),
            None => false,
        };
        let point_signed = state.signed().map_or(false, |result| result.is_ok());
        let point_included =
            match point_signed && dag_point.location().round == self.current_round.round() {
                true => self.includes_ready.insert(dag_point.location().author),
                false => self.includes_ready.contains(&dag_point.location().author),
            };
        let level = if dag_point.trusted().is_some() {
            tracing::Level::TRACE
        } else {
            tracing::Level::WARN
        };
        dyn_event!(
            parent: self.effects.span(),
            level,
            author = display(dag_point.location().author.alt()),
            round = dag_point.location().round.0,
            digest = display(dag_point.digest().alt()),
            signed = signed,
            point_signed = point_signed,
            point_included = point_included,
            includes = self.includes_ready.len(),
            "inclusion validated"
        );
    }
}

struct CollectorContext;
impl EffectsContext for CollectorContext {}

impl Effects<CollectorContext> {
    fn new(parent: &Effects<CurrentRoundContext>) -> Self {
        Self::new_child(parent.span(), || tracing::error_span!("collector"))
    }
}
