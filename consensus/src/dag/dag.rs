use std::collections::{BTreeMap, VecDeque};
use std::num::NonZeroU8;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::dag::DagRound;
use crate::models::{Point, PointId, Round, ValidPoint};

pub struct Dag {
    // from the oldest to the current round; newer ones are in the future
    rounds: BTreeMap<Round, DagRound>,
}

impl Dag {
    // pub fn new(peer_schedule: &PeerSchedule) -> Self {
    //     Self {
    //         rounds: BTreeMap::from([(Arc::new(DagRound::new(round, &peer_schedule, None)))]),
    //         peer_schedule,
    //     }
    // }
    //
    // // TODO new point is checked against the dag only if it has valid sig, time and round
    // // TODO download from neighbours
    // pub fn fill_up_to(&mut self, round: Round) {
    //     match self.rounds.last_key_value() {
    //         None => unreachable!("DAG empty"),
    //         Some((last, _)) => {
    //             for round in (last.0..round.0).into_iter().map(|i| Round(i + 1)) {
    //                 let prev = self.rounds.last_key_value().map(|(_, v)| Arc::downgrade(v));
    //                 self.rounds.entry(round).or_insert_with(|| {
    //                     Arc::new(DagRound::new(round, &self.peer_schedule, prev))
    //                 });
    //             }
    //         }
    //     }
    // }

    pub fn new() -> Self {
        Self {
            rounds: Default::default(),
        }
    }

    pub fn get_or_insert(&mut self, dag_round: DagRound) -> DagRound {
        self.rounds
            .entry(dag_round.round().clone())
            .or_insert(dag_round)
            .clone()
    }

    // TODO the next "little anchor candidate that could" must have at least full dag depth
    pub fn drop_tail(&mut self, anchor_at: Round, dag_depth: NonZeroU8) {
        if let Some(tail) = anchor_at.0.checked_sub(dag_depth.get() as u32) {
            self.rounds = self.rounds.split_off(&Round(tail));
        };
    }

    async fn point_by_id(&self, point_id: &PointId) -> Option<ValidPoint> {
        let dag_round = self.rounds.get(&point_id.location.round)?;
        dag_round.valid_point(&point_id).await
    }

    async fn vertex_by_proof(&self, proof: &ValidPoint) -> Option<ValidPoint> {
        let dag_round = self.rounds.get(&proof.point.body.location.round.prev())?;
        match &proof.point.body.proof {
            Some(proven) => {
                dag_round
                    .valid_point_exact(&proof.point.body.location.author, &proven.digest)
                    .await
            }
            None => None,
        }
    }

    // @return historically ordered vertices (back to front is older to newer)
    pub async fn gather_uncommitted(
        &self,
        anchor_trigger: &PointId,
        // dag_depth: usize,
    ) -> VecDeque<Arc<Point>> {
        let Some(anchor_trigger) = self.point_by_id(anchor_trigger).await else {
            panic!(
                "Coding error: anchor trigger @ {:?} is not in DAG",
                &anchor_trigger.location.round
            );
        };
        // anchor must be a vertex @ r+1, proven with point @ r+2
        let Some(anchor_proof) = self.vertex_by_proof(&anchor_trigger).await else {
            panic!(
                "Coding error: anchor proof @ {:?} is not in DAG",
                &anchor_trigger.point.body.location.round.prev()
            );
        };
        _ = anchor_trigger; // no more needed for commit
        let Some(anchor) = self.vertex_by_proof(&anchor_proof).await else {
            panic!(
                "Coding error: anchor @ {:?} is not in DAG",
                &anchor_proof.point.body.location.round.prev()
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
            .and_then(|cur| cur.prev().get().map(|prev| (cur, prev)))
            .filter(|_| !r.iter().all(BTreeMap::is_empty))
        {
            // take points @ r+0, and select their vertices @ r-1 for commit
            // the order is of NodeId (public key) TODO shuffle deterministically, eg with anchor digest as a seed
            while let Some((node, digest)) = &r[0].pop_first() {
                // Every point must be valid (we've validated anchor dependencies already),
                // but some points don't have previous one to proof as vertex.
                // Any valid point among equivocated will do, as they include the same vertex.
                if let Some(proof /* point @ r+0 */) =
                    proof_round.valid_point_exact(node, digest).await
                {
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
                        .valid_point_exact(author, &digest)
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
                        uncommitted.push_back(vertex.point); // LIFO
                    }
                }
            }
            cur_includes_round = vertex_round.round().clone(); // next r+0
            r.rotate_left(1);
        }
        uncommitted
    }
}
