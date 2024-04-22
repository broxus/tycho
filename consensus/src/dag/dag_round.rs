use std::sync::{Arc, Weak};

use ahash::RandomState;
use everscale_crypto::ed25519::KeyPair;
use futures_util::future::BoxFuture;
use futures_util::FutureExt;

use tycho_network::PeerId;
use tycho_util::FastDashMap;

use crate::dag::anchor_stage::AnchorStage;
use crate::dag::{DagLocation, InclusionState, Verifier};
use crate::engine::MempoolConfig;
use crate::intercom::PeerSchedule;
use crate::models::{DagPoint, Digest, NodeCount, Point, PointId, Round, ValidPoint};

#[derive(Clone)]
pub struct WeakDagRound(Weak<DagRoundInner>);

#[derive(Clone)]
pub struct DagRound(Arc<DagRoundInner>);

struct DagRoundInner {
    round: Round,          // immutable
    node_count: NodeCount, // immutable
    /// if key_pair is not empty, then the node may produce block at this round,
    /// and also sign broadcasts during previous round
    key_pair: Option<Arc<KeyPair>>, // immutable
    anchor_stage: Option<AnchorStage>, // immutable
    locations: FastDashMap<PeerId, DagLocation>,
    prev: WeakDagRound, // immutable ?
}

impl WeakDagRound {
    pub const BOTTOM: Self = WeakDagRound(Weak::new());
    pub fn get(&self) -> Option<DagRound> {
        self.0.upgrade().map(DagRound)
    }
}

impl DagRound {
    pub fn new(round: Round, peer_schedule: &PeerSchedule, prev: WeakDagRound) -> Self {
        let peers = peer_schedule.peers_for(&round);
        let locations = FastDashMap::with_capacity_and_hasher(peers.len(), RandomState::new());
        Self(Arc::new(DagRoundInner {
            round,
            node_count: NodeCount::try_from(peers.len())
                .expect(&format!("peer schedule updated for {round:?}")),
            key_pair: peer_schedule.local_keys(&round),
            anchor_stage: AnchorStage::of(round, peer_schedule),
            locations,
            prev,
        }))
    }

    pub fn next(&self, peer_schedule: &PeerSchedule) -> Self {
        let next_round = self.round().next();
        let peers = peer_schedule.peers_for(&next_round);
        let locations = FastDashMap::with_capacity_and_hasher(peers.len(), RandomState::new());
        Self(Arc::new(DagRoundInner {
            round: next_round,
            node_count: NodeCount::try_from(peers.len())
                .expect(&format!("peer schedule updated for {next_round:?}")),
            key_pair: peer_schedule.local_keys(&next_round),
            anchor_stage: AnchorStage::of(next_round, peer_schedule),
            locations,
            prev: self.as_weak(),
        }))
    }

    pub fn genesis(genesis: &Arc<Point>, peer_schedule: &PeerSchedule) -> Self {
        let locations = FastDashMap::with_capacity_and_hasher(1, RandomState::new());
        let round = genesis.body.location.round;
        Self(Arc::new(DagRoundInner {
            round,
            node_count: NodeCount::GENESIS,
            key_pair: None,
            anchor_stage: AnchorStage::of(round, peer_schedule),
            locations,
            prev: WeakDagRound::BOTTOM,
        }))
    }

    pub fn round(&self) -> &'_ Round {
        &self.0.round
    }

    pub fn node_count(&self) -> &'_ NodeCount {
        &self.0.node_count
    }

    pub fn key_pair(&self) -> Option<&'_ KeyPair> {
        self.0.key_pair.as_deref()
    }

    pub fn anchor_stage(&self) -> Option<&'_ AnchorStage> {
        self.0.anchor_stage.as_ref()
    }

    pub fn edit<F, R>(&self, author: &PeerId, edit: F) -> R
    where
        F: FnOnce(&mut DagLocation) -> R,
    {
        let mut loc = self.0.locations.entry(*author).or_default();
        edit(loc.value_mut())
    }

    pub fn view<F, R>(&self, author: &PeerId, view: F) -> Option<R>
    where
        F: FnOnce(&DagLocation) -> R,
    {
        self.0.locations.view(author, |_, v| view(v))
    }

    pub fn select<'a, F, R>(&'a self, mut filter_map: F) -> impl Iterator<Item = R> + 'a
    where
        F: FnMut((&PeerId, &DagLocation)) -> Option<R> + 'a,
    {
        self.0
            .locations
            .iter()
            .filter_map(move |a| filter_map(a.pair()))
    }

    pub fn prev(&self) -> &'_ WeakDagRound {
        &self.0.prev
    }

    pub fn as_weak(&self) -> WeakDagRound {
        WeakDagRound(Arc::downgrade(&self.0))
    }

    pub async fn vertex_by_proof(&self, proof: &ValidPoint) -> Option<ValidPoint> {
        match proof.point.body.proof {
            Some(ref proven) => {
                let dag_round = self.scan(&proof.point.body.location.round.prev())?;
                dag_round
                    .valid_point_exact(&proof.point.body.location.author, &proven.digest)
                    .await
            }
            None => None,
        }
    }

    pub async fn valid_point(&self, point_id: &PointId) -> Option<ValidPoint> {
        match self.scan(&point_id.location.round) {
            Some(linked) => {
                linked
                    .valid_point_exact(&point_id.location.author, &point_id.digest)
                    .await
            }
            None => None,
        }
    }

    pub async fn valid_point_exact(&self, node: &PeerId, digest: &Digest) -> Option<ValidPoint> {
        let point_fut = self.view(node, |loc| loc.versions().get(digest).cloned())??;
        point_fut.await.0.valid().cloned()
    }

    pub fn add(&self, point: &Arc<Point>) -> Option<BoxFuture<'static, InclusionState>> {
        self.scan(&point.body.location.round)
            .and_then(|linked| linked.add_exact(&point))
    }

    fn add_exact(&self, point: &Arc<Point>) -> Option<BoxFuture<'static, InclusionState>> {
        if &point.body.location.round != self.round() {
            panic!("Coding error: dag round mismatches point round on add")
        }
        let dag_round = self.clone();
        let digest = &point.digest;
        self.edit(&point.body.location.author, |loc| {
            let state = loc.state().clone();
            let point = point.clone();
            loc.add_validate(digest, || Verifier::validate(point, dag_round))
                .map(|first| first.clone().map(|_| state).boxed())
        })
    }

    // Todo leave for genesis, use for own points in tests
    pub async fn insert_exact_validate(
        &self,
        point: &Arc<Point>,
        peer_schedule: &PeerSchedule,
    ) -> Option<InclusionState> {
        if !Verifier::verify(point, peer_schedule).is_ok() {
            panic!("Coding error: malformed point")
        }
        let point = Verifier::validate(point.clone(), self.clone()).await;
        if point.valid().is_none() {
            panic!("Coding error: not a valid point")
        }
        let Some(state) = self.insert_exact(&point) else {
            return None;
        };
        let state = state.await;
        if let Some(signable) = state.signable() {
            signable.sign(
                self.round(),
                peer_schedule.local_keys(&self.round().next()).as_deref(),
                MempoolConfig::sign_time_range(),
            );
        }
        if state.signed_point(self.round()).is_none() {
            panic!("Coding or configuration error: valid point cannot be signed; time issue?")
        }
        Some(state)
    }

    pub fn insert_invalid(
        &self,
        dag_point: &DagPoint,
    ) -> Option<BoxFuture<'static, InclusionState>> {
        if dag_point.valid().is_some() {
            panic!("Coding error: failed to insert valid point as invalid")
        }
        self.scan(&dag_point.location().round)
            .map(|linked| linked.insert_exact(dag_point))
            .flatten()
    }

    fn insert_exact(&self, dag_point: &DagPoint) -> Option<BoxFuture<'static, InclusionState>> {
        if &dag_point.location().round != self.round() {
            panic!("Coding error: dag round mismatches point round on insert")
        }
        self.edit(&dag_point.location().author, |loc| {
            let state = loc.state().clone();
            loc.add_validate(dag_point.digest(), || {
                futures_util::future::ready(dag_point.clone())
            })
            .map(|first| first.clone().map(|_| state).boxed())
        })
    }

    pub fn scan(&self, round: &Round) -> Option<Self> {
        assert!(
            round <= self.round(),
            "Coding error: cannot scan DAG rounds chain for a future round"
        );
        let mut visited = self.clone();
        if round == self.round() {
            return Some(visited);
        }
        while let Some(dag_round) = visited.prev().get() {
            match dag_round.round().cmp(&round) {
                core::cmp::Ordering::Less => return None,
                core::cmp::Ordering::Equal => return Some(dag_round),
                core::cmp::Ordering::Greater => visited = dag_round,
            }
        }
        None
    }
}
