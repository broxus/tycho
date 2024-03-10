use std::collections::{BTreeMap, VecDeque};
use std::num::{NonZeroU8, NonZeroUsize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock, Weak};

use ahash::RandomState;
use anyhow::{anyhow, Result};
use futures_util::FutureExt;

use tycho_util::FastDashMap;

use crate::engine::promise::Promise;
use crate::models::point::{Digest, NodeId, Point, Round, Signature};
use crate::tasks::downloader::DownloadTask;

pub struct IndexedPoint {
    point: Point,
    // proof_for: Option<Weak<IndexedPoint>>,
    // includes: Vec<Weak<IndexedPoint>>,
    // witness: Vec<Weak<IndexedPoint>>,
    is_committed: AtomicBool,
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
    /* Downloading,              // -> Validating | Invalid | Unknown */
    /* Validating(Arc<Point>),   // -> Valid | Invalid */
    Valid(Arc<IndexedPoint>), // needed to blame equivocation or graph connectivity violations
    Invalid(Arc<Point>),      // invalidates dependent point; needed to blame equivocation
    NotExists,                // invalidates dependent point; blame with caution
}

impl DagPoint {
    pub fn is_valid(&self) -> bool {
        match self {
            DagPoint::Valid(_) => true,
            _ => false,
        }
    }

    pub fn valid(&self) -> Option<Arc<IndexedPoint>> {
        match self {
            DagPoint::Valid(point) => Some(point.clone()),
            _ => None,
        }
    }
}

#[derive(Default)]
struct DagLocation {
    // one of the points at current location
    // was proven by the next point of a node;
    // even if we marked this point as invalid, consensus may override our decision
    // and we will have to sync
    /* vertex: Option<Digest>, */
    // we can sign just a single point at the current location;
    // other (equivocated) points may be received as includes, witnesses or a proven vertex;
    // we have to include signed points as dependencies in our next block
    signed_by_me: OnceLock<(Digest, Round, Signature)>,
    // if we rejected to sign previous point,
    // we require a node to skip the current round;
    // if we require to skip after responding with a signature -
    // our node cannot invalidate a block retrospectively
    no_points_expected: AtomicBool,
    // only one of the point versions at current location
    // may become proven by the next round point(s) of a node;
    // even if we marked a proven point as invalid, consensus may override our decision
    versions: BTreeMap<Digest, Promise<DagPoint>>,
}

struct DagRound {
    round: Round,
    node_count: u8,
    locations: FastDashMap<NodeId, DagLocation>,
    prev: Weak<DagRound>,
}

impl DagRound {
    fn new(round: Round, node_count: NonZeroU8, prev: Option<&Arc<DagRound>>) -> Self {
        Self {
            round,
            node_count: ((node_count.get() + 2) / 3) * 3 + 1, // 3F+1
            locations: FastDashMap::with_capacity_and_hasher(
                node_count.get() as usize,
                RandomState::new(),
            ),
            prev: prev.map_or(Weak::new(), |a| Arc::downgrade(a)),
        }
    }

    pub async fn valid_point(&self, node: &NodeId, digest: &Digest) -> Option<Arc<IndexedPoint>> {
        let location = self.locations.get(node)?;
        let promise = location.versions.get(digest)?;
        let point = promise.get().await.ok()?;
        point.valid()
    }

    pub async fn add(&mut self, point: Point) -> Result<Promise<DagPoint>> {
        if point.body.location.round != self.round {
            return Err(anyhow!("wrong point round"));
        }
        if !point.is_integrity_ok() {
            return Err(anyhow!("point integrity check failed"));
        }
        let mut dependencies = vec![];
        if let Some(r_1) = self.prev.upgrade() {
            for (node, digest) in point.body.includes.clone() {
                let mut loc = r_1.locations.entry(node).or_default();
                let promise = loc
                    .versions
                    .entry(digest)
                    .or_insert(Promise::new(Box::pin(DownloadTask {})))
                    .clone();
                dependencies.push(promise);
            }
            if let Some(r_2) = r_1.prev.upgrade() {
                for (node, digest) in point.body.witness.clone() {
                    let mut loc = r_2.locations.entry(node).or_default();
                    let promise = loc
                        .versions
                        .entry(digest)
                        .or_insert(Promise::new(Box::pin(DownloadTask {})))
                        .clone();
                    dependencies.push(promise);
                }
            };
        };

        /*
        Ok(Promise::new(|| {
            Box::pin(async move {
                let res: Result<Vec<_>, _> = join_all(dependencies.into_iter().map(|p| p.get()))
                    .await
                    .into_iter()
                    .collect();
                res.map(|deps| {
                    if deps.iter().any(|point| !point.is_valid()) {
                        DagPoint::Invalid(Arc::new(point))
                    } else {
                        DagPoint::Valid(Arc::new(IndexedPoint::new(point)))
                    }
                })
            })
        }))
        */
        /*
        let task = Box::pin({
            try_join_all(dependencies.iter().map(|p| p.get())).map(|res| {
                res.map(|deps| {
                    if deps.iter().any(|point| !point.is_valid()) {
                        DagPoint::Invalid(Arc::new(point))
                    } else {
                        DagPoint::Valid(Arc::new(IndexedPoint::new(point)))
                    }
                })
            })
        })
        .boxed();
        Ok(Promise::new(task))
        */
        Ok(Promise::ready(DagPoint::NotExists))
        // Ok(Promise::new(task)) // FIXME make fn sync
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DagError {
    #[error("Dag empty")]
    Empty,
    #[error("Point not in dag")]
    PointNotInDag,
    #[error("Round not in dag")]
    RoundNotInDag,
}
pub struct Dag {
    current: Round,
    // from the oldest to the current round; newer ones are in the future
    rounds: VecDeque<Arc<DagRound>>, // TODO VecDeque<Promise<Arc<DagRound>>> for sync
}

impl Dag {
    pub fn new(round: Round, node_count: NonZeroU8) -> Self {
        Self {
            current: round,
            rounds: VecDeque::from([Arc::new(DagRound::new(round, node_count, None))]),
        }
    }

    // TODO new point is checked against the dag only if it has valid sig, time and round
    // TODO download from neighbours
    pub fn fill_up_to(&mut self, round: Round, node_count: NonZeroU8) -> Result<()> {
        match self.rounds.front().map(|f| f.round) {
            None => unreachable!("DAG empty"),
            Some(front) => {
                for round in front.0..round.0 {
                    self.rounds.push_front(Arc::new(DagRound::new(
                        Round(round + 1),
                        node_count,
                        self.rounds.front(),
                    )))
                }
                Ok(())
            }
        }
    }

    pub fn drop_tail(&mut self, anchor_at: Round, dag_depth: NonZeroUsize) {
        if let Some(tail) = self
            .index_of(anchor_at)
            .and_then(|a| a.checked_sub(dag_depth.get()))
        {
            self.rounds.drain(0..tail);
        };
    }

    fn round_at(&self, round: Round) -> Option<Arc<DagRound>> {
        self.rounds.get(self.index_of(round)?).map(|r| r.clone())
    }

    fn index_of(&self, round: Round) -> Option<usize> {
        match self.rounds.back().map(|b| b.round) {
            Some(back) if back <= round => Some((round.0 - back.0) as usize),
            _ => None,
        }
    }

    pub async fn vertex_by(&self, proof: &IndexedPoint) -> Option<Arc<IndexedPoint>> {
        let digest = &proof.point.body.proof.as_ref()?.digest;
        let round = proof.point.body.location.round.prev()?;
        let dag_round = self.round_at(round)?;
        dag_round
            .valid_point(&proof.point.body.location.author, digest)
            .await
    }

    // @return historically ordered vertices (back to front is older to newer)
    pub async fn gather_uncommitted(
        &self,
        anchor_proof: &IndexedPoint,
        // dag_depth: usize,
    ) -> Result<VecDeque<Arc<IndexedPoint>>> {
        // anchor must be a vertex @ r+1, proven with point @ r+2
        let Some(anchor) = self.vertex_by(&anchor_proof).await else {
            return Err(anyhow!(
                "anchor proof @ {} not in dag",
                &anchor_proof.point.body.location.round.0
            ));
        };
        _ = anchor_proof; // needed no more

        let Some(mut cur_includes_round) = anchor.point.body.location.round.prev() else {
            return Err(anyhow!("anchor proof @ 0 cannot exist"));
        };

        let mut r_0 = anchor.point.body.includes.clone(); // points @ r+0
        let mut r_1 = anchor.point.body.witness.clone(); // points @ r-1
        let mut r_2 = BTreeMap::new(); // points @ r-2
        let mut r_3 = BTreeMap::new(); // points @ r-3
        _ = anchor; // anchor payload will be committed the next time

        let mut uncommitted = VecDeque::new();

        // TODO visited rounds count must be equal to dag depth:
        //  read/download non-existent rounds and drop too old ones
        while let Some((proof_round /* r+0 */, vertex_round /* r-1 */)) = self
            .round_at(cur_includes_round)
            .and_then(|cur| cur.prev.upgrade().map(|prev| (cur, prev)))
            .filter(|_| !(r_0.is_empty() && r_1.is_empty() && r_2.is_empty() && r_3.is_empty()))
        {
            // take points @ r+0, and select their vertices @ r-1 for commit
            // the order is of NodeId (public key)
            while let Some((node, digest)) = &r_0.pop_first() {
                // Every point must be valid (we've validated anchor dependencies already),
                // but some points don't have previous one to proof as vertex.
                // Any valid point among equivocated will do, as they include the same vertex.
                if let Some(proof /* point @ r+0 */) = proof_round.valid_point(node, digest).await {
                    if proof.is_committed.load(Ordering::Relaxed) {
                        continue;
                    }
                    let author = &proof.point.body.location.author;
                    r_1.extend(proof.point.body.includes.clone()); // points @ r-1
                    r_2.extend(proof.point.body.witness.clone()); // points @ r-2
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
                                .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
                                .is_ok()
                        })
                    {
                        // vertex will be skipped in r_1 as committed
                        r_2.extend(vertex.point.body.includes.clone()); // points @ r-2
                        r_3.extend(vertex.point.body.witness.clone()); // points @ r-3
                        uncommitted.push_back(vertex); // LIFO
                    }
                }
            }
            cur_includes_round = vertex_round.round; // next r+0
            r_0 = r_1; // next r+0
            r_1 = r_2; // next r-1
            r_2 = r_3; // next r-2
            r_3 = BTreeMap::new(); // next r-3
        }
        Ok(uncommitted)
    }
}
