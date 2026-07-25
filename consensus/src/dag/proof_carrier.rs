#![allow(dead_code, reason = "proof carrier quorum wireframe")]

use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::mpsc;
use tycho_network::PeerId;
use tycho_util::{FastHashMap, FastHashSet};

use crate::dag::dag_point_future::WeakDagPointFuture;
use crate::models::{PeerCount, PointId, PointInfo, Round, ValidPoint};

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

    pub fn observe(&mut self, _valid: &ValidPoint) -> Option<ProofCarrierQuorum> {
        // TODO count one first-valid carrier per author and exact proof id at this round.
        None
    }
}

#[derive(Clone)]
pub(super) struct ProofCarrierQuorum {
    proof: PointId,
    carrier_round: Round,
    carriers: Arc<[PointId]>,
}

pub(super) struct ProofCarrierCounts {
    includes_target: usize,
    witness_target: Option<usize>,
}

impl ProofCarrierCounts {
    pub fn new(includes: PeerCount, witness: Option<PeerCount>) -> Self {
        Self {
            includes_target: includes.majority(),
            witness_target: witness.map(|count| count.majority()),
        }
    }

    pub fn from_dependencies<'a>(
        includes: PeerCount,
        witness: Option<PeerCount>,
        _include_infos: impl Iterator<Item = &'a PointInfo>,
        _witness_infos: impl Iterator<Item = &'a PointInfo>,
    ) -> Self {
        // TODO group semantic anchor-proof ids by carrier round and distinct author.
        Self::new(includes, witness)
    }

    pub fn observe_dependency(&mut self, _point: &PointInfo, _dependency: &PointInfo) {
        // TODO count only exact dependencies from the point's includes or witness map.
    }

    pub fn required_proof(&self) -> Option<PointId> {
        // TODO return the proof lock established by a same-round carrier quorum.
        None
    }

    pub fn incompatible_proof(&self, _point: &PointInfo) -> Option<PointId> {
        // TODO return the required proof when the selected proof neither matches nor extends it.
        None
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
