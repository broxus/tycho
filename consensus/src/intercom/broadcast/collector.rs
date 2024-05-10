use std::cmp::Ordering;
use std::mem;
use std::sync::Arc;

use futures_util::future::BoxFuture;
use futures_util::stream::FuturesUnordered;
use futures_util::{FutureExt, StreamExt};
use tokio::sync::{mpsc, oneshot};
use tycho_network::PeerId;
use tycho_util::FastHashSet;

use crate::dag::{DagRound, InclusionState};
use crate::engine::MempoolConfig;
use crate::intercom::broadcast::dto::ConsensusEvent;
use crate::intercom::dto::SignatureResponse;
use crate::intercom::{BroadcasterSignal, Downloader};
use crate::models::{Round, Ugly};

/// collector may run without broadcaster, as if broadcaster signalled Ok
#[derive(Debug)]
pub enum CollectorSignal {
    Finish,
    Err,
    Retry,
}

pub struct Collector {
    log_id: Arc<String>,
    downloader: Downloader,
    from_bcast_filter: mpsc::UnboundedReceiver<ConsensusEvent>,
    signature_requests: mpsc::UnboundedReceiver<SignatureRequest>,
    next_round: Round,
    next_includes: FuturesUnordered<BoxFuture<'static, InclusionState>>,
}

impl Collector {
    pub fn new(
        log_id: Arc<String>,
        downloader: &Downloader,
        from_bcast_filter: mpsc::UnboundedReceiver<ConsensusEvent>,
        signature_requests: mpsc::UnboundedReceiver<SignatureRequest>,
    ) -> Self {
        Self {
            log_id,
            downloader: downloader.clone(),
            from_bcast_filter,
            signature_requests,
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
        next_dag_round: DagRound, // r+1
        own_point_state: oneshot::Receiver<InclusionState>,
        collector_signal: mpsc::UnboundedSender<CollectorSignal>,
        bcaster_signal: mpsc::Receiver<BroadcasterSignal>,
    ) -> Self {
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
                        unreachable!()
                    }
                }
            })
            .boxed(),
        );

        assert_eq!(
            current_dag_round.round(),
            &self.next_round,
            "collector expected to be run at {:?}",
            &self.next_round
        );
        self.next_round = next_dag_round.round().clone();
        let includes_ready = FastHashSet::with_capacity_and_hasher(
            current_dag_round.node_count().full(),
            Default::default(),
        );
        let task = CollectorTask {
            log_id: self.log_id.clone(),
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
        let result = task
            .run(&mut self.from_bcast_filter, &mut self.signature_requests)
            .await;
        match result {
            Ok(includes) => self.next_includes = includes,
            Err(round) => self.next_round = round,
        }
        self
    }

    pub fn next_round(&self) -> &'_ Round {
        &self.next_round
    }
}

type SignatureRequest = (Round, PeerId, oneshot::Sender<SignatureResponse>);
struct CollectorTask {
    // for node running @ r+0:
    log_id: Arc<String>,
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
    /// anyway should rewrite signing mechanics - look for comments inside [DagRound::add_exact]
    next_includes: FuturesUnordered<BoxFuture<'static, InclusionState>>,

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
        signature_requests: &mut mpsc::UnboundedReceiver<SignatureRequest>,
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
                            self.collector_signal.send(CollectorSignal::Err).ok();
                            return Err(round)
                        }
                    },
                    None => panic!("channel from Broadcast Filter closed"),
                },
                Some(state) = self.includes.next() => {
                    self.on_inclusion_validated(&state)
                },
                Some(state) = self.next_includes.next(), if ! self.is_includes_ready => {
                    if let Some(result) = self.jump_up(state) {
                        return result.map(|_ | self.next_includes)
                    }
                },
                request = signature_requests.recv() => match request {
                    Some((round, author, callback)) => {
                        _ = callback.send(self.signature_response(&round, &author));
                    }
                    None => panic!("channel with signature requests closed")
                },
                else => {
                    panic!("collector unhandled");
                }
            }
        }
    }

    fn should_fail(&mut self, signal: BroadcasterSignal) -> bool {
        tracing::debug!(
            "{} @ {:.4?} collector <= Bcaster::{signal:?} : includes {} of {}",
            self.log_id,
            self.current_round.round(),
            self.includes_ready.len(),
            self.current_round.node_count().majority()
        );
        match signal {
            BroadcasterSignal::Ok => {
                self.is_bcaster_ready_ok = true;
                self.bcaster_signal.close();
                false
            }
            BroadcasterSignal::Err => true,
        }
    }

    fn is_ready(&mut self) -> bool {
        tracing::debug!(
            "{} @ {:.4?} collector self-check : includes {} of {}",
            self.log_id,
            self.current_round.round(),
            self.includes_ready.len(),
            self.current_round.node_count().majority()
        );
        // point @ r+1 has to include 2F+1 broadcasts @ r+0 (we are @ r+0)
        self.is_includes_ready |=
            self.includes_ready.len() >= self.current_round.node_count().majority();
        if self.is_includes_ready && self.is_bcaster_ready_ok {
            _ = self.collector_signal.send(CollectorSignal::Finish);
        }
        self.is_includes_ready && self.is_bcaster_ready_ok
    }

    fn jump_up(&mut self, state: InclusionState) -> Option<Result<(), Round>> {
        // its ok to discard invalid state from `next_includes` queue
        let point_round = state.point()?.valid()?.point.body.location.round;
        tracing::info!(
            "{} @ {:?} maybe jump to {point_round:?}",
            self.log_id,
            self.current_round.round()
        );
        // will be signed on the next round
        self.next_includes
            .push(futures_util::future::ready(state).boxed());
        self.is_includes_ready = true;
        match point_round.cmp(self.next_dag_round.round()) {
            Ordering::Less => {
                panic!("Coding error: next includes futures contain current or previous round")
            }
            Ordering::Greater => {
                tracing::error!("Collector was left behind while broadcast filter advanced ?");
                self.collector_signal.send(CollectorSignal::Err).ok();
                Some(Err(point_round))
            }
            Ordering::Equal => {
                if self.is_ready() {
                    Some(Ok(()))
                } else {
                    None
                }
            }
        }
    }

    fn match_filtered(&self, consensus_event: &ConsensusEvent) -> Result<(), Round> {
        tracing::debug!(
            "{} @ {:?} collector <= bcast filter : {:?}",
            self.log_id,
            self.current_round.round(),
            consensus_event.ugly()
        );
        match consensus_event {
            ConsensusEvent::Forward(consensus_round) => {
                match consensus_round.cmp(self.next_dag_round.round()) {
                    // we're too late, consensus moved forward
                    std::cmp::Ordering::Greater => return Err(consensus_round.clone()),
                    // we still have a chance to finish current round
                    std::cmp::Ordering::Equal => {}
                    // we are among the fastest nodes of consensus
                    std::cmp::Ordering::Less => {}
                }
            }
            ConsensusEvent::Verified(point) => match &point.body.location.round {
                x if x > self.next_dag_round.round() => {
                    panic!(
                        "{} @ {:?} Coding error: broadcast filter advanced \
                            while collector left behind; event: {:?}",
                        self.log_id,
                        self.current_round.round(),
                        consensus_event.ugly()
                    )
                }
                x if x == self.next_dag_round.round() => {
                    if let Some(task) = self.next_dag_round.add(point, &self.downloader) {
                        self.next_includes.push(task)
                    }
                }
                x if x == self.current_round.round() => {
                    if let Some(task) = self.current_round.add(point, &self.downloader) {
                        self.includes.push(task)
                    }
                }
                _ => _ = self.current_round.add(&point, &self.downloader), /* maybe other's dependency */
            },
            ConsensusEvent::Invalid(dag_point) => {
                if &dag_point.location().round > self.next_dag_round.round() {
                    panic!(
                        "{} @ {:?} Coding error: broadcast filter advanced \
                            while collector left behind; event: {:?}",
                        self.log_id,
                        self.current_round.round(),
                        consensus_event.ugly()
                    )
                } else {
                    _ = self.next_dag_round.insert_invalid(&dag_point);
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
        // slow but at least may work
        if let Some(signable) = state.signable() {
            signable.sign(
                self.current_round.round(),
                self.next_dag_round.key_pair(),
                MempoolConfig::sign_time_range(),
            );
        };
        if let Some(Ok(_)) = state.signed() {
            if let Some(dag_point) = state
                .point()
                .filter(|dp| dp.location().round == *self.current_round.round())
            {
                self.includes_ready.insert(dag_point.location().author);
                tracing::debug!(
                    "{} @ {:.4?} includes {} +1 : {:?}",
                    self.log_id,
                    self.current_round.round(),
                    self.includes_ready.len(),
                    dag_point.id().ugly()
                );
                return;
            }
            return;
        }
        tracing::debug!(
            "{} @ {:.4?} includes {} : {:?} {:.4?}",
            self.log_id,
            self.current_round.round(),
            self.includes_ready.len(),
            state.point().map(|a| a.id()).as_ref().map(|a| a.ugly()),
            state.signed()
        );
    }

    fn signature_response(&mut self, round: &Round, author: &PeerId) -> SignatureResponse {
        if round > self.current_round.round() {
            return SignatureResponse::TryLater; // hold fast nodes from moving forward
        };
        let Some(dag_round) = self.current_round.scan(round) else {
            return SignatureResponse::Rejected; // lagged too far from consensus and us
        };
        // TODO do not state().clone() - mutating closure on location is easily used;
        //  need to remove inner locks from InclusionState and leave it guarded by DashMap;
        //  also sign points during their validation, see comments in DagLocation::add_validate()
        let Some(state) = dag_round.view(author, |loc| loc.state().clone()) else {
            return SignatureResponse::NoPoint; // retry broadcast
        };
        if let Some(signable) = state.signable() {
            let key_pair = match self.next_dag_round.key_pair() {
                // points @ current local dag round are includes for next round point
                Some(key_pair) if round == self.current_round.round() => Some(key_pair),
                // points @ previous local dag round are witness for next round point
                Some(_) if round == &self.current_round.round().prev() => {
                    self.current_round.key_pair()
                }
                // point is too old, cannot include;
                // Note: requests for future rounds are filtered out at the beginning of this method
                _ => None,
            };
            if signable.sign(
                &self.current_round.round(),
                key_pair,
                MempoolConfig::sign_time_range(),
            ) {
                if round == self.current_round.round() {
                    self.includes_ready.insert(author.clone());
                }
            }
        }
        let response = match state.signed() {
            Some(Ok(signed)) => SignatureResponse::Signature(signed.with.clone()),
            Some(Err(())) => SignatureResponse::Rejected,
            None => SignatureResponse::TryLater,
        };
        tracing::debug!(
            "{} @ {:?} collector => bcaster {author:.4?} @ {round:?} : {response:.4?}",
            self.log_id,
            self.current_round.round()
        );
        response
    }
}
