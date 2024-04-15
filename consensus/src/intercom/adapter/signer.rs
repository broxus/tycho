use std::mem;
use std::sync::Arc;

use futures_util::future::BoxFuture;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use tokio::sync::{mpsc, oneshot, Notify};

use tycho_network::PeerId;

use crate::dag::{DagRound, InclusionState};
use crate::engine::MempoolConfig;
use crate::intercom::adapter::dto::{ConsensusEvent, SignerSignal};
use crate::intercom::dto::SignatureResponse;
use crate::models::{Point, Round};

pub struct Signer {
    from_bcast_filter: mpsc::UnboundedReceiver<ConsensusEvent>,
    signature_requests: mpsc::UnboundedReceiver<SignatureRequest>,
    next_round: Round,
    next_includes: FuturesUnordered<BoxFuture<'static, InclusionState>>,
}

impl Signer {
    pub fn new(
        from_bcast_filter: mpsc::UnboundedReceiver<ConsensusEvent>,
        signature_requests: mpsc::UnboundedReceiver<SignatureRequest>,
        last_round: &Round,
    ) -> Self {
        Self {
            from_bcast_filter,
            signature_requests,
            next_round: last_round.next(),
            next_includes: FuturesUnordered::new(),
        }
    }

    pub async fn run(
        mut self,
        next_dag_round: DagRound, // r+1
        has_own_point: Option<Arc<Point>>,
        signer_signal: mpsc::UnboundedSender<SignerSignal>,
        bcaster_ready: Arc<Notify>,
    ) -> Self {
        let current_dag_round = next_dag_round
            .prev()
            .get()
            .expect("current DAG round must be linked into DAG chain");
        let mut includes = mem::take(&mut self.next_includes);
        if current_dag_round.round() != &self.next_round {
            includes.clear();
        };
        self.next_round = next_dag_round.round().clone();
        let task = SignerTask {
            next_dag_round,
            current_round: current_dag_round.clone(),
            includes,
            includes_ready: has_own_point.into_iter().count(),
            next_includes: FuturesUnordered::new(),

            signer_signal,
            bcaster_ready,
            is_bcaster_ready: false,
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
struct SignerTask {
    // for node running @ r+0:

    // @ r+0, will become includes in point @ r+1
    // needed in order to not include same point twice - as an include and as a witness;
    // need to drop them with round change
    includes: FuturesUnordered<BoxFuture<'static, InclusionState>>,
    includes_ready: usize,
    /// do not poll during this round, just pass to next round;
    /// anyway should rewrite signing mechanics - look for comments inside [DagRound::add_exact]
    next_includes: FuturesUnordered<BoxFuture<'static, InclusionState>>,

    next_dag_round: DagRound, // = r+1 is always in DAG; contains the keypair to produce point @ r+1
    current_round: DagRound,  // = r+0

    signer_signal: mpsc::UnboundedSender<SignerSignal>,
    bcaster_ready: Arc<Notify>,
    is_bcaster_ready: bool,
}

impl SignerTask {
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
                request = signature_requests.recv() => match request {
                    Some((round, peer_id, callback)) =>
                        _ = callback.send(self.signature_response(&round, &peer_id)),
                    None => panic!("channel with signature requests closed")
                },
                filtered = from_bcast_filter.recv() => match filtered {
                    Some(consensus_event) => {
                        if let Err(round) = self.match_filtered(&consensus_event) {
                            _ = self.signer_signal.send(SignerSignal::Err);
                            return Err(round)
                        }
                    },
                    None => panic!("channel from Broadcast Filter closed"),
                },
                _ = self.bcaster_ready.notified() => {
                    self.is_bcaster_ready = true;
                    if self.includes_ready >= self.current_round.node_count().majority() {
                        return Ok(self.next_includes)
                    }
                },
                _ = retry_interval.tick() => {
                    // point @ r+1 has to include 2F+1 broadcasts @ r+0 (we are @ r+0)
                    if self.includes_ready >= self.current_round.node_count().majority() {
                        _ = self.signer_signal.send(SignerSignal::Ok);
                        _ = self.signer_signal.send(SignerSignal::Retry);
                        if self.is_bcaster_ready {
                            return Ok(self.next_includes)
                        }
                    } else {
                        _ = self.signer_signal.send(SignerSignal::Retry);
                    }
                },
                // FIXME not so great: some signature requests will be retried,
                //  just because this futures were not polled. Use global 'current dag round' round
                //  and sign inside shared join task in dag location,
                //  do not return location from DagLocation::add_validate(point)
                Some(state) = self.includes.next() => {
                    // slow but at least may work
                    if let Some(signable) = state.signable() {
                        if signable.sign(
                            self.current_round.round(),
                            self.next_dag_round.key_pair(),
                            MempoolConfig::sign_time_range(),
                        ) {
                            self.includes_ready += 1;
                        }
                    }
                },
            }
        }
    }

    fn signature_response(&mut self, round: &Round, author: &PeerId) -> SignatureResponse {
        if round > self.current_round.round() {
            return SignatureResponse::TryLater; // hold fast nodes from moving forward
        };
        let Some(dag_round) = self.next_dag_round.scan(round) else {
            return SignatureResponse::Rejected; // lagged too far from consensus and us
        };
        // TODO do not state().clone() - mutating closure on location is easily used;
        //  need to remove inner locks from InclusionState and leave it guarded by DashMap;
        //  also sign points during their validation, see comments in DagLocation::add_validate()
        let Some(state) = dag_round.view(author, |loc| loc.state().clone()) else {
            return SignatureResponse::NoPoint; // retry broadcast, point was replaced in filter
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
                    self.includes_ready += 1;
                }
            }
        }
        match state.signed() {
            Some(Ok(signed)) => SignatureResponse::Signature(signed.with.clone()),
            Some(Err(())) => SignatureResponse::Rejected,
            None => SignatureResponse::TryLater,
        }
    }
    fn match_filtered(&self, filtered: &ConsensusEvent) -> Result<(), Round> {
        match filtered {
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
                    panic!("Coding error: broadcast filter advanced while signer left behind")
                }
                x if x == self.next_dag_round.round() => {
                    if let Some(task) = self.next_dag_round.add(point) {
                        self.next_includes.push(task)
                    }
                }
                x if x == self.current_round.round() => {
                    if let Some(task) = self.current_round.add(point) {
                        self.includes.push(task)
                    }
                }
                _ => _ = self.current_round.add(&point), // maybe other's dependency
            },
            ConsensusEvent::Invalid(dag_point) => {
                if &dag_point.location().round > self.next_dag_round.round() {
                    panic!("Coding error: broadcast filter advanced while signer left behind")
                } else {
                    _ = self.next_dag_round.insert_invalid(&dag_point);
                }
            }
        };
        Ok(())
    }
}
