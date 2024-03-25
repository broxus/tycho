use std::collections::{btree_map, BTreeMap, VecDeque};
use std::num::NonZeroU8;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock, Weak};

use ahash::RandomState;
use rand::{Rng, SeedableRng};

use tycho_network::PeerId;
use tycho_util::futures::{JoinTask, Shared};
use tycho_util::FastDashMap;

use crate::engine::node_count::NodeCount;
use crate::engine::peer_schedule::PeerSchedule;
use crate::engine::verifier::Verifier;
use crate::models::point::{Digest, Point, PointId, Round, Signature};

pub struct IndexedPoint {
    pub point: Point,
    // proof_for: Option<Weak<IndexedPoint>>,
    // includes: Vec<Weak<IndexedPoint>>,
    // witness: Vec<Weak<IndexedPoint>>,
    pub is_committed: AtomicBool,
}

impl IndexedPoint {
    pub fn new(point: Point) -> Self {
        Self {
            point,
            is_committed: AtomicBool::new(false),
        }
    }
}

#[derive(Clone)]
pub enum DagPoint {
    // valid without demur, needed to blame equivocation or graph connectivity violations
    Trusted(Arc<IndexedPoint>),
    // is a valid container, but we doubt author's fairness at the moment of validating;
    // we do not sign such point, but others may include it without consequences;
    // consensus will decide whether to sign its proof or not; we shall ban the author
    Suspicious(Arc<IndexedPoint>),
    Invalid(Arc<Point>), // invalidates dependent point; needed to blame equivocation
    NotExists(Arc<PointId>), // invalidates dependent point; blame author of dependent point
}

impl DagPoint {
    pub fn is_valid(&self) -> bool {
        match self {
            DagPoint::Trusted(_) => true,
            DagPoint::Suspicious(_) => true,
            _ => false,
        }
    }

    pub fn valid(&self) -> Option<Arc<IndexedPoint>> {
        match self {
            DagPoint::Trusted(point) => Some(point.clone()),
            DagPoint::Suspicious(point) => Some(point.clone()),
            _ => None,
        }
    }
}

#[derive(Default)]
pub struct DagLocation {
    // one of the points at current location
    // was proven by the next point of a node;
    // even if we marked this point as invalid, consensus may override our decision
    // and we will have to sync
    /* vertex: Option<Digest>, */
    // we can sign just a single point at the current location;
    // other (equivocated) points may be received as includes, witnesses or a proven vertex;
    // we have to include signed points as dependencies in our next block
    signed_by_me: OnceLock<(Digest, Round, Signature)>,
    // only one of the point versions at current location
    // may become proven by the next round point(s) of a node;
    // even if we marked a proven point as invalid, consensus may override our decision
    pub versions: BTreeMap<Digest, Shared<JoinTask<DagPoint>>>,
}

pub enum AnchorStage {
    Candidate(PeerId),
    Proof(PeerId),
    Trigger(PeerId),
}

impl AnchorStage {
    pub fn of(round: Round, peer_schedule: &PeerSchedule) -> Option<Self> {
        const WAVE_SIZE: u32 = 4;
        let anchor_candidate_round = (round.0 / WAVE_SIZE) * WAVE_SIZE + 1;

        let [leader_peers, current_peers] =
            peer_schedule.peers_for_array([Round(anchor_candidate_round), round]);
        // reproducible global coin
        let leader_index = rand_pcg::Pcg32::seed_from_u64(anchor_candidate_round as u64)
            .gen_range(0..leader_peers.len());
        let Some(leader) = leader_peers
            .iter()
            .nth(leader_index)
            .map(|(peer_id, _)| peer_id)
        else {
            panic!("Fatal: selecting a leader from an empty validator set")
        };
        if !current_peers.contains_key(leader) {
            return None;
        };
        match round.0 % WAVE_SIZE {
            0 => None, // both genesis and trailing (proof inclusion) round
            1 => Some(AnchorStage::Candidate(leader.clone())),
            2 => Some(AnchorStage::Proof(leader.clone())),
            3 => Some(AnchorStage::Trigger(leader.clone())),
            _ => unreachable!(),
        }
    }
}

pub struct DagRound {
    pub round: Round,
    node_count: NodeCount,
    pub anchor_stage: Option<AnchorStage>,
    pub locations: FastDashMap<PeerId, DagLocation>,
    pub prev: Weak<DagRound>,
}

impl DagRound {
    fn new(round: Round, peer_schedule: &PeerSchedule, prev: Option<Weak<DagRound>>) -> Self {
        let peers = peer_schedule.peers_for(round);
        let locations = FastDashMap::with_capacity_and_hasher(peers.len(), RandomState::new());
        Self {
            round,
            node_count: NodeCount::new(peers.len()),
            anchor_stage: AnchorStage::of(round, peer_schedule),
            locations,
            prev: prev.unwrap_or_else(|| Weak::new()),
        }
    }

    pub async fn valid_point(&self, node: &PeerId, digest: &Digest) -> Option<Arc<IndexedPoint>> {
        let point_fut = {
            let location = self.locations.get(node)?;
            location.versions.get(digest)?.clone()
        };
        point_fut.await.0.valid()
    }

    pub fn add(
        self: Arc<Self>,
        point: Box<Point>,
        peer_schedule: &PeerSchedule,
    ) -> Shared<JoinTask<DagPoint>> {
        if &point.body.location.round != &self.round {
            panic! {"Coding error: dag round mismatches point round"}
        }

        let mut location = self
            .locations
            .entry(point.body.location.author)
            .or_default();

        match location.versions.entry(point.digest.clone()) {
            btree_map::Entry::Occupied(entry) => entry.get().clone(),
            btree_map::Entry::Vacant(entry) => entry
                .insert(Shared::new(Verifier::verify(
                    self.clone(),
                    point,
                    peer_schedule,
                )))
                .clone(),
        }
    }
}

pub struct Dag {
    current: Round,
    // from the oldest to the current round; newer ones are in the future
    rounds: BTreeMap<Round, Arc<DagRound>>,
    peer_schedule: PeerSchedule,
}

impl Dag {
    pub fn new(round: Round, peer_schedule: PeerSchedule) -> Self {
        Self {
            current: round,
            rounds: BTreeMap::from([(round, Arc::new(DagRound::new(round, &peer_schedule, None)))]),
            peer_schedule,
        }
    }

    // TODO new point is checked against the dag only if it has valid sig, time and round
    // TODO download from neighbours
    pub fn fill_up_to(&mut self, round: Round) {
        match self.rounds.last_key_value() {
            None => unreachable!("DAG empty"),
            Some((last, _)) => {
                for round in (last.0..round.0).into_iter().map(|i| Round(i + 1)) {
                    let prev = self.rounds.last_key_value().map(|(_, v)| Arc::downgrade(v));
                    self.rounds.entry(round).or_insert_with(|| {
                        Arc::new(DagRound::new(round, &self.peer_schedule, prev))
                    });
                }
            }
        }
    }

    // TODO the next "little anchor candidate that could" must have at least full dag depth
    pub fn drop_tail(&mut self, anchor_at: Round, dag_depth: NonZeroU8) {
        if let Some(tail) = anchor_at.0.checked_sub(dag_depth.get() as u32) {
            self.rounds = self.rounds.split_off(&Round(tail));
        };
    }

    pub async fn vertex_by(&self, proof: &IndexedPoint) -> Option<Arc<IndexedPoint>> {
        let digest = &proof.point.body.proof.as_ref()?.digest;
        let round = proof.point.body.location.round.prev();
        let dag_round = self.rounds.get(&round)?;
        dag_round
            .valid_point(&proof.point.body.location.author, digest)
            .await
    }

    // @return historically ordered vertices (back to front is older to newer)
    pub async fn gather_uncommitted(
        &self,
        anchor_trigger: &IndexedPoint,
        // dag_depth: usize,
    ) -> VecDeque<Arc<IndexedPoint>> {
        // anchor must be a vertex @ r+1, proven with point @ r+2
        let Some(anchor_proof) = self.vertex_by(&anchor_trigger).await else {
            panic!(
                "Coding error: anchor trigger @ {} is not in DAG",
                &anchor_trigger.point.body.location.round.0
            );
        };
        _ = anchor_trigger; // no more needed for commit
        let Some(anchor) = self.vertex_by(&anchor_proof).await else {
            panic!(
                "Coding error: anchor proof @ {} is not in DAG",
                &anchor_proof.point.body.location.round.0
            );
        };
        _ = anchor_proof; // no more needed for commit

        let mut cur_includes_round = anchor.point.body.location.round.prev(); /* r+0 */

        let mut r = [
            anchor.point.body.includes.clone(), // points @ r+0
            anchor.point.body.witness.clone(),  // points @ r-1
            BTreeMap::new(),                    // points @ r-2
            BTreeMap::new(),                    // points @ r-3
        ];
        _ = anchor; // anchor payload will be committed the next time

        let mut uncommitted = VecDeque::new();

        // TODO visited rounds count must be equal to dag depth:
        //  read/download non-existent rounds and drop too old ones
        while let Some((proof_round /* r+0 */, vertex_round /* r-1 */)) = self
            .rounds
            .get(&cur_includes_round)
            .and_then(|cur| cur.prev.upgrade().map(|prev| (cur, prev)))
            .filter(|_| !r.iter().all(BTreeMap::is_empty))
        {
            // take points @ r+0, and select their vertices @ r-1 for commit
            // the order is of NodeId (public key)
            while let Some((node, digest)) = &r[0].pop_first() {
                // Every point must be valid (we've validated anchor dependencies already),
                // but some points don't have previous one to proof as vertex.
                // Any valid point among equivocated will do, as they include the same vertex.
                if let Some(proof /* point @ r+0 */) = proof_round.valid_point(node, digest).await {
                    if proof.is_committed.load(Ordering::Acquire) {
                        continue;
                    }
                    let author = &proof.point.body.location.author;
                    r[1].extend(proof.point.body.includes.clone()); // points @ r-1
                    r[2].extend(proof.point.body.witness.clone()); // points @ r-2
                    let Some(digest) = proof.point.body.proof.as_ref().map(|a| &a.digest) else {
                        continue;
                    };
                    if let Some(vertex /* point @ r-1 */) = vertex_round
                        .valid_point(author, &digest)
                        .await
                        // select uncommitted ones, marking them as committed
                        // to exclude from the next commit
                        .filter(|vertex| {
                            vertex
                                .is_committed
                                .compare_exchange(false, true, Ordering::Release, Ordering::Relaxed)
                                .is_ok()
                        })
                    {
                        // vertex will be skipped in r_1 as committed
                        r[2].extend(vertex.point.body.includes.clone()); // points @ r-2
                        r[3].extend(vertex.point.body.witness.clone()); // points @ r-3
                        uncommitted.push_back(vertex); // LIFO
                    }
                }
            }
            cur_includes_round = vertex_round.round; // next r+0
            r.rotate_left(1);
        }
        uncommitted
    }
}
