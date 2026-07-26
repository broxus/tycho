#![allow(dead_code, reason = "proof carrier quorum wireframe")]

use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::mpsc;
use tycho_network::PeerId;
use tycho_util::{FastHashMap, FastHashSet};

use crate::dag::dag_point_future::WeakDagPointFuture;
use crate::models::{AnchorStageRole, AnyLink, PeerCount, PointId, PointInfo, Round, ValidPoint};

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

    fn required_proof(&self) -> Option<PointId> {
        self.quorum
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

    pub fn required_proof(&self) -> Option<PointId> {
        // Includes are the newer carrier round and supersede witness evidence.
        self.includes
            .required_proof()
            .or_else(|| self.witness.as_ref()?.required_proof())
    }

    pub fn incompatible_proof(&self, point: &PointInfo) -> Option<PointId> {
        let required = self.required_proof()?;
        (!self.proof_extends(point, required)).then_some(required)
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

#[derive(Default)]
struct ProofCommitGateState {
    by_proof: FastHashMap<PointId, ProofCommitState>,
}

#[derive(Default)]
struct ProofCommitState {
    quorum: Option<Arc<ProofCarrierQuorum>>,
    pending: FastHashMap<PointId, WeakDagPointFuture>,
}

pub(super) struct ProofCommitGate {
    state: Mutex<ProofCommitGateState>,
    ready_tx: mpsc::UnboundedSender<WeakDagPointFuture>,
}

impl ProofCommitGate {
    pub fn new(ready_tx: mpsc::UnboundedSender<WeakDagPointFuture>) -> Self {
        Self {
            state: Default::default(),
            ready_tx,
        }
    }

    pub fn register_trigger(
        &self,
        _trigger_id: PointId,
        _proof_id: PointId,
        trigger: WeakDagPointFuture,
    ) {
        // TODO retain the trigger until its exact proof has a carrier quorum.
        self.ready_tx.send(trigger).ok();
    }

    pub fn register_history_conflict(&self, trigger: WeakDagPointFuture) {
        self.ready_tx.send(trigger).ok();
    }

    pub fn register_quorum(&self, _quorum: ProofCarrierQuorum) {
        // TODO retain the quorum and release pending triggers for its exact proof.
    }

    pub fn clean(&self, _bottom_round: Round) {
        // TODO remove pending triggers and quorum evidence below retained DAG history.
    }
}

pub(super) struct SupportedTrigger {
    trigger: WeakDagPointFuture,
    quorum: Arc<ProofCarrierQuorum>,
}
