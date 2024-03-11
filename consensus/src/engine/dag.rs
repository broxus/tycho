use std::collections::{btree_map, BTreeMap, VecDeque};
use std::num::{NonZeroU8, NonZeroUsize};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use std::task::{Context, Poll};

use ahash::RandomState;
use anyhow::{anyhow, Result};
use futures_util::future::BoxFuture;
use futures_util::{Future, FutureExt};
use tokio::task::{JoinHandle, JoinSet};
use tycho_network::PeerId;
use tycho_util::futures::Shared;
use tycho_util::FastDashMap;

use crate::models::point::{Digest, Point, Round, Signature};
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
    versions: BTreeMap<Digest, DagPointFut>,
}

struct DagRound {
    round: Round,
    node_count: u8,
    locations: FastDashMap<PeerId, DagLocation>,
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

    pub async fn valid_point(&self, node: &PeerId, digest: &Digest) -> Option<Arc<IndexedPoint>> {
        let point_fut = {
            let location = self.locations.get(node)?;
            location.versions.get(digest)?.clone()
        };
        point_fut.await.valid()
    }

    pub fn add(&self, point: Point) -> Result<DagPointFut> {
        anyhow::ensure!(point.body.location.round == self.round, "wrong point round");
        anyhow::ensure!(point.is_integrity_ok(), "point integrity check failed");

        let mut location = self
            .locations
            .entry(point.body.location.author)
            .or_default();

        fn add_dependency(
            round: &Arc<DagRound>,
            node: &PeerId,
            digest: &Digest,
            dependencies: &mut JoinSet<DagPoint>,
        ) {
            let mut loc = round.locations.entry(*node).or_default();
            let fut = loc
                .versions
                .entry(*digest)
                .or_insert_with(|| DagPointFut::new(DownloadTask {}))
                .clone();
            dependencies.spawn(fut);
        }

        Ok(match location.versions.entry(point.digest) {
            btree_map::Entry::Occupied(entry) => entry.get().clone(),
            btree_map::Entry::Vacant(entry) => {
                let mut dependencies = JoinSet::new();
                if let Some(r_1) = self.prev.upgrade() {
                    for (node, digest) in &point.body.includes {
                        add_dependency(&r_1, &node, &digest, &mut dependencies);
                    }
                    if let Some(r_2) = r_1.prev.upgrade() {
                        for (node, digest) in &point.body.witness {
                            add_dependency(&r_2, &node, &digest, &mut dependencies);
                        }
                    };
                };

                let fut = DagPointFut::new(async move {
                    while let Some(res) = dependencies.join_next().await {
                        match res {
                            Ok(value) if value.is_valid() => continue,
                            Ok(_) => return DagPoint::Invalid(Arc::new(point)),
                            Err(e) => {
                                if e.is_panic() {
                                    std::panic::resume_unwind(e.into_panic());
                                }
                                unreachable!();
                            }
                        }
                    }

                    DagPoint::Valid(Arc::new(IndexedPoint::new(point)))
                });

                entry.insert(fut).clone()
            }
        })
    }
}

#[derive(Clone)]
#[repr(transparent)]
pub struct DagPointFut {
    inner: Shared<BoxFuture<'static, DagPoint>>,
}

impl DagPointFut {
    fn new<F>(f: F) -> Self
    where
        F: Future<Output = DagPoint> + Send + 'static,
    {
        struct FutGuard {
            handle: JoinHandle<DagPoint>,
            complete: bool,
        }

        impl Drop for FutGuard {
            fn drop(&mut self) {
                if !self.complete {
                    self.handle.abort();
                }
            }
        }

        let mut guard = FutGuard {
            handle: tokio::spawn(f),
            complete: false,
        };

        Self {
            inner: Shared::new(Box::pin(async move {
                match (&mut guard.handle).await {
                    Ok(value) => {
                        guard.complete = true;
                        value
                    }
                    Err(e) => {
                        if e.is_panic() {
                            std::panic::resume_unwind(e.into_panic());
                        }
                        unreachable!()
                    }
                }
            })),
        }
    }
}

impl Future for DagPointFut {
    type Output = DagPoint;

    #[inline]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let (value, _) = futures_util::ready!(self.inner.poll_unpin(cx));
        Poll::Ready(value)
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
        self.rounds.get(self.index_of(round)?).cloned()
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

        let Some(mut cur_includes_round) = anchor.point.body.location.round.prev() else {
            return Err(anyhow!("anchor proof @ 0 cannot exist"));
        };

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
            .round_at(cur_includes_round)
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
        Ok(uncommitted)
    }
}
