#![allow(dead_code, reason = "proof carrier quorum wireframe")]

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use parking_lot::Mutex;
use tokio::sync::mpsc;
use tycho_network::PeerId;
use tycho_util::{FastHashMap, FastHashSet};

use crate::dag::dag_point_future::WeakDagPointFuture;
use crate::effects::TaskResult;
use crate::models::{
    AnchorStageRole, AnyLink, DagPoint, PeerCount, PointId, PointInfo, Round, ValidPoint,
};

#[derive(Clone, Copy)]
pub(super) struct CarrierVote {
    carrier: PointId,
    proof: PointId,
}

pub(super) struct ProofCarrierRound {
    round: Round,
    target: usize,
    by_author: FastHashMap<PeerId, CarrierVote>,
    by_proof: FastHashMap<PointId, Vec<PointId>>,
    formed: FastHashSet<PointId>,
}

impl ProofCarrierRound {
    pub fn new(round: Round, peer_count: PeerCount) -> Self {
        Self {
            round,
            target: peer_count.majority(),
            by_author: Default::default(),
            by_proof: Default::default(),
            formed: Default::default(),
        }
    }

    pub fn observe(&mut self, valid: &ValidPoint) -> Option<ProofCarrierQuorum> {
        let info = valid.info();
        assert_eq!(info.round(), self.round, "carrier round mismatch");
        assert!(valid.is_first_valid(), "proof carrier must be first-valid");

        if self.by_author.contains_key(info.author()) {
            return None;
        }

        let vote = CarrierVote {
            carrier: *info.id(),
            proof: info.anchor_id(AnchorStageRole::Proof),
        };
        self.by_author.insert(*info.author(), vote);

        let carriers = self.by_proof.entry(vote.proof).or_default();
        carriers.push(vote.carrier);
        if carriers.len() < self.target || !self.formed.insert(vote.proof) {
            return None;
        }

        Some(ProofCarrierQuorum {
            proof: vote.proof,
            carrier_round: self.round,
            carriers: carriers.clone().into(),
        })
    }
}

#[derive(Clone)]
pub(super) struct ProofCarrierQuorum {
    proof: PointId,
    carrier_round: Round,
    carriers: Arc<[PointId]>,
}

impl ProofCarrierQuorum {
    pub fn proof(&self) -> PointId {
        self.proof
    }

    pub fn carrier_round(&self) -> Round {
        self.carrier_round
    }
}

pub(super) struct ProofCarrierCounts {
    includes: ProofCarrierBucket,
    witness: Option<ProofCarrierBucket>,
    proof_parents: FastHashMap<PointId, PointId>,
}

// One bucket is one dependency round. The point map already names at most one
// exact version per author, independently of this node's local first-valid choice.
struct ProofCarrierBucket {
    target: usize,
    round: Option<Round>,
    by_author: FastHashSet<PeerId>,
    by_proof: FastHashMap<PointId, usize>,
    quorum: Option<PointId>,
}

impl ProofCarrierBucket {
    fn new(peer_count: PeerCount) -> Self {
        Self {
            target: peer_count.majority(),
            round: None,
            by_author: Default::default(),
            by_proof: Default::default(),
            quorum: None,
        }
    }

    fn observe(&mut self, info: &PointInfo) {
        match self.round {
            Some(round) => assert_eq!(info.round(), round, "mixed carrier rounds"),
            None => self.round = Some(info.round()),
        }

        if !self.by_author.insert(*info.author()) {
            return;
        }

        let proof = info.anchor_id(AnchorStageRole::Proof);
        let count = self.by_proof.entry(proof).or_default();
        *count += 1;

        if *count == self.target {
            assert!(
                self.quorum.replace(proof).is_none(),
                "more than one proof carrier quorum in one round"
            );
        }
    }

    fn required_proof(&self) -> Option<&PointId> {
        self.quorum.as_ref()
    }
}

impl ProofCarrierCounts {
    pub fn new(includes: PeerCount, witness: Option<PeerCount>) -> Self {
        Self {
            includes: ProofCarrierBucket::new(includes),
            witness: witness.map(ProofCarrierBucket::new),
            proof_parents: Default::default(),
        }
    }

    pub fn from_dependencies<'a>(
        includes: PeerCount,
        witness: Option<PeerCount>,
        include_infos: impl Iterator<Item = &'a PointInfo>,
        witness_infos: impl Iterator<Item = &'a PointInfo>,
    ) -> Self {
        let mut this = Self::new(includes, witness);
        for info in include_infos {
            this.observe_include(info);
        }
        for info in witness_infos {
            this.observe_witness(info);
        }
        this
    }

    pub fn observe_dependency(&mut self, point: &PointInfo, dependency: &PointInfo) {
        // Ignore other versions spawned only to validate the author's previous location.
        let is_include = dependency.round() == point.round().prev()
            && point.includes().get(dependency.author()) == Some(dependency.digest());
        if is_include {
            self.observe_include(dependency);
            return;
        }

        let is_witness = dependency.round() == point.round().prev().prev()
            && point.witness().get(dependency.author()) == Some(dependency.digest());
        if is_witness {
            self.observe_witness(dependency);
        }
    }

    pub fn required_proof(&self) -> Option<&PointId> {
        // Includes are the newer carrier round and supersede witness evidence.
        self.includes
            .required_proof()
            .or_else(|| self.witness.as_ref()?.required_proof())
    }

    pub fn incompatible_proof(&self, point: &PointInfo) -> Option<&PointId> {
        let required = self.required_proof()?;
        (!self.proof_extends(point, *required)).then_some(required)
    }

    fn observe_include(&mut self, info: &PointInfo) {
        self.observe_proof_parent(info);
        self.includes.observe(info);
    }

    fn observe_witness(&mut self, info: &PointInfo) {
        self.observe_proof_parent(info);
        self.witness
            .as_mut()
            .expect("witness dependency without its peer schedule")
            .observe(info);
    }

    fn observe_proof_parent(&mut self, info: &PointInfo) {
        if info.anchor_proof() != AnyLink::ToSelf {
            return;
        }
        let Some(parent) = info.chained_anchor_proof_to() else {
            return;
        };
        if parent.round >= info.round() {
            return;
        }
        self.proof_parents.entry(*info.id()).or_insert(parent);
    }

    fn proof_extends(&self, point: &PointInfo, required: PointId) -> bool {
        let mut proof = point.anchor_id(AnchorStageRole::Proof);
        if proof == required {
            return true;
        }

        // Use only exact, round-decreasing proof links visible in this dependency cut.
        // One extra edge may be introduced by `point` itself.
        for _ in 0..=self.proof_parents.len() {
            if proof.round <= required.round {
                return false;
            }

            let parent = if proof == *point.id() && point.anchor_proof() == AnyLink::ToSelf {
                point.chained_anchor_proof_to()
            } else {
                self.proof_parents.get(&proof).copied()
            };
            let Some(parent) = parent else {
                return false;
            };

            if parent == required {
                return true;
            }
            if parent.round >= proof.round {
                return false;
            }
            proof = parent;
        }

        false
    }
}

struct ProofCommitGateState {
    // Prevent a late task from restoring evidence after its DAG round was dropped.
    bottom_round: Round,
    by_proof: FastHashMap<PointId, ProofCommitState>,
}

impl Default for ProofCommitGateState {
    fn default() -> Self {
        Self {
            bottom_round: Round::BOTTOM,
            by_proof: Default::default(),
        }
    }
}

#[derive(Default)]
struct ProofCommitState {
    quorum: Option<Arc<ProofCarrierQuorum>>,
    pending: FastHashMap<PointId, WeakDagPointFuture>,
    // Keep released IDs too, so repeated registration cannot enqueue a trigger twice.
    seen: FastHashSet<PointId>,
}

pub(super) struct ProofCommitGate {
    state: Mutex<ProofCommitGateState>,
    ready_tx: mpsc::UnboundedSender<CommitterSignal>,
}

impl ProofCommitGate {
    pub fn new(ready_tx: mpsc::UnboundedSender<CommitterSignal>) -> Self {
        Self {
            state: Default::default(),
            ready_tx,
        }
    }

    pub fn register_trigger(
        &self,
        trigger_id: PointId,
        proof_id: PointId,
        trigger: WeakDagPointFuture,
    ) {
        let ready = {
            let mut state = self.state.lock();
            if trigger_id.round < state.bottom_round {
                return;
            }

            let proof_state = state.by_proof.entry(proof_id).or_default();
            if !proof_state.seen.insert(trigger_id) {
                return;
            }

            match proof_state.quorum.clone() {
                Some(quorum) => Some(SupportedTrigger { trigger, quorum }),
                None => {
                    proof_state.pending.insert(trigger_id, trigger);
                    None
                }
            }
        };

        if let Some(ready) = ready {
            self.ready_tx.send(CommitterSignal::Supported(ready)).ok();
        }
    }

    pub fn register_history_conflict(&self, trigger: WeakDagPointFuture) {
        // This is a recovery signal, not a valid trigger waiting for proof support.
        self.ready_tx
            .send(CommitterSignal::HistoryConflict(trigger))
            .ok();
    }

    pub fn register_quorum(&self, quorum: ProofCarrierQuorum) {
        let ready = {
            let mut state = self.state.lock();
            if quorum.carrier_round < state.bottom_round {
                return;
            }

            let proof_state = state.by_proof.entry(quorum.proof).or_default();
            let quorum = Arc::new(quorum);
            // Later carrier evidence for the same proof remains retained for longer.
            let retained = match &proof_state.quorum {
                Some(current) if current.carrier_round >= quorum.carrier_round => current.clone(),
                _ => {
                    proof_state.quorum = Some(quorum.clone());
                    quorum
                }
            };

            std::mem::take(&mut proof_state.pending)
                .into_values()
                .map(|trigger| SupportedTrigger {
                    trigger,
                    quorum: retained.clone(),
                })
                .collect::<Vec<_>>()
        };

        for ready in ready {
            self.ready_tx.send(CommitterSignal::Supported(ready)).ok();
        }
    }

    pub fn clean(&self, bottom_round: Round) {
        let mut state = self.state.lock();
        if bottom_round <= state.bottom_round {
            return;
        }
        state.bottom_round = bottom_round;

        state.by_proof.retain(|_, proof_state| {
            proof_state
                .pending
                .retain(|trigger, _| trigger.round >= bottom_round);
            proof_state
                .seen
                .retain(|trigger| trigger.round >= bottom_round);

            if proof_state
                .quorum
                .as_ref()
                .is_some_and(|quorum| quorum.carrier_round < bottom_round)
            {
                proof_state.quorum = None;
            }

            proof_state.quorum.is_some()
                || !proof_state.pending.is_empty()
                || !proof_state.seen.is_empty()
        });
    }
}

pub(super) struct SupportedTrigger {
    trigger: WeakDagPointFuture,
    quorum: Arc<ProofCarrierQuorum>,
}

pub(super) enum CommitterSignal {
    Supported(SupportedTrigger),
    HistoryConflict(WeakDagPointFuture),
}

pub(super) enum ResolvedCommitterSignal {
    Supported {
        trigger: DagPoint,
        quorum: Arc<ProofCarrierQuorum>,
    },
    HistoryConflict(DagPoint),
}

impl Future for CommitterSignal {
    type Output = TaskResult<Option<ResolvedCommitterSignal>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.get_mut() {
            Self::Supported(supported) => match Pin::new(&mut supported.trigger).poll(cx) {
                Poll::Ready(Ok(Some(trigger))) => {
                    Poll::Ready(Ok(Some(ResolvedCommitterSignal::Supported {
                        trigger,
                        quorum: supported.quorum.clone(),
                    })))
                }
                Poll::Ready(Ok(None)) => Poll::Ready(Ok(None)),
                Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
                Poll::Pending => Poll::Pending,
            },
            Self::HistoryConflict(future) => match Pin::new(future).poll(cx) {
                Poll::Ready(Ok(Some(trigger))) => {
                    Poll::Ready(Ok(Some(ResolvedCommitterSignal::HistoryConflict(trigger))))
                }
                Poll::Ready(Ok(None)) => Poll::Ready(Ok(None)),
                Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
                Poll::Pending => Poll::Pending,
            },
        }
    }
}
