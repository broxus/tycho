use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use parking_lot::Mutex;

use crate::dag::anchor_stage::AnchorStage;
use crate::dag::DagRound;
use crate::engine::MempoolConfig;
use crate::models::{Point, Round, ValidPoint};

#[derive(Clone)]
pub struct Dag {
    // from the oldest to the current round; newer ones are in the future
    rounds: Arc<Mutex<BTreeMap<Round, DagRound>>>,
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

    pub fn get_or_insert(&self, dag_round: DagRound) -> DagRound {
        let mut rounds = self.rounds.lock();
        rounds
            .entry(dag_round.round().clone())
            .or_insert(dag_round)
            .clone()
    }

    // fixme must not be async
    pub async fn commit(
        self,
        next_dag_round: DagRound,
    ) -> VecDeque<(Arc<Point>, VecDeque<Arc<Point>>)> {
        let Some(latest_trigger) = Self::latest_trigger(&next_dag_round).await else {
            return VecDeque::new();
        };
        let mut anchor_stack = Self::anchor_stack(&latest_trigger, next_dag_round.clone()).await;
        let mut ordered = VecDeque::new();
        while let Some((anchor, anchor_round)) = anchor_stack.pop() {
            self.drop_tail(anchor.point.body.location.round);
            let committed = Self::gather_uncommitted(&anchor.point, &anchor_round).await;
            ordered.push_back((anchor.point, committed));
        }
        ordered
    }

    async fn latest_trigger(next_round: &DagRound) -> Option<ValidPoint> {
        let mut next_dag_round = next_round.clone();
        let mut latest_trigger = None;
        while let Some(current_dag_round) = next_dag_round.prev().get() {
            if let Some(AnchorStage::Trigger {
                ref is_used,
                ref leader,
            }) = current_dag_round.anchor_stage()
            {
                if is_used.load(Ordering::Relaxed) {
                    break;
                };
                let mut futs = FuturesUnordered::new();
                current_dag_round.view(leader, |loc| {
                    for (_, version) in loc.versions() {
                        futs.push(version.clone())
                    }
                });
                // FIXME traversing the DAG must not be async: we need the way to determine completed tasks
                //  its sufficient to use only ready futures at this point, must ignore downloading tasks
                while let Some((found, _)) = futs.next().await {
                    if let Some(valid) = found.into_valid() {
                        _ = latest_trigger.insert(valid);
                        is_used.store(true, Ordering::Relaxed);
                        break;
                    }
                }
            };
            next_dag_round = current_dag_round;
        }
        latest_trigger
    }

    /// return order: newest (in depth) to oldest (on top); use with `vec.pop()`
    async fn anchor_stack(
        last_trigger: &ValidPoint,
        mut future_round: DagRound,
    ) -> Vec<(ValidPoint, DagRound)> {
        assert_eq!(
            last_trigger.point.prev_id(),
            Some(last_trigger.point.anchor_proof_id()),
            "invalid anchor proof link, trigger point must have been invalidated"
        );
        let mut anchor_stack = Vec::new();
        let Some(mut proof) = future_round.vertex_by_proof(last_trigger).await else {
            panic!("anchor proof round not in DAG")
        };
        loop {
            let Some(proof_round) = future_round.scan(&proof.point.body.location.round) else {
                panic!("anchor proof round not in DAG while a point from it was received")
            };
            if proof_round.round() == &MempoolConfig::GENESIS_ROUND {
                break;
            }
            match proof_round.anchor_stage() {
                Some(AnchorStage::Proof {
                    ref leader,
                    ref is_used,
                }) => {
                    assert_eq!(
                        proof.point.body.location.round,
                        *proof_round.round(),
                        "anchor proof round does not match"
                    );
                    assert_eq!(
                        proof.point.body.location.author, leader,
                        "anchor proof author does not match prescribed by round"
                    );
                    let Some(anchor_round) = proof_round.prev().get() else {
                        break;
                    };
                    if is_used.load(Ordering::Relaxed) {
                        break;
                    };
                    let mut proofs = FuturesUnordered::new();
                    proof_round.view(leader, |loc| {
                        for (_, version) in loc.versions() {
                            proofs.push(version.clone())
                        }
                    });
                    let mut anchor = None;
                    'v: while let Some((proof, _)) = proofs.next().await {
                        if let Some(valid) = proof.into_valid() {
                            let Some(valid) = proof_round.vertex_by_proof(&valid).await else {
                                panic!("anchor proof is not linked to anchor, validation broken")
                            };
                            _ = anchor.insert(valid);
                            is_used.store(true, Ordering::Relaxed);
                            break 'v;
                        }
                    }
                    let anchor = anchor
                        .expect("any anchor proof points to anchor point, validation is broken");
                    anchor_stack.push((anchor.clone(), anchor_round.clone()));

                    let Some(next_proof) = proof_round
                        .valid_point(&anchor.point.anchor_proof_id())
                        .await
                    else {
                        break;
                    };
                    proof = next_proof;
                    future_round = anchor_round;
                }
                _ => panic!("anchor proof round is not expected, validation is broken"),
            }
        }
        anchor_stack
    }

    // TODO the next "little anchor candidate that could" must have at least full dag depth
    fn drop_tail(&self, anchor_at: Round) {
        if let Some(tail) = anchor_at.0.checked_sub(MempoolConfig::COMMIT_DEPTH as u32) {
            let mut rounds = self.rounds.lock();
            // TODO if sync is implemented as a second sub-graph - drop up to last linked
            *rounds = rounds.split_off(&Round(tail));
        };
    }

    /// returns historically ordered vertices (back to front is older to newer)
    ///
    /// Note: at this point there is no way to check if passed point is really an anchor
    async fn gather_uncommitted(
        anchor /* @ r+1 */: &Point,
        anchor_round /* r+1 */: &DagRound,
    ) -> VecDeque<Arc<Point>> {
        assert_eq!(
            *anchor_round.round(),
            anchor.body.location.round,
            "passed anchor round does not match anchor point's round"
        );
        let mut proof_round /* r+0 */ = anchor_round
            .prev()
            .get()
            .expect("previous round for anchor point round must stay in DAG");
        let mut r = [
            anchor.body.includes.clone(), // points @ r+0
            anchor.body.witness.clone(),  // points @ r-1
            BTreeMap::new(),              // points @ r-2
            BTreeMap::new(),              // points @ r-3
        ];
        _ = anchor; // anchor payload will be committed the next time

        let mut uncommitted = VecDeque::new();

        // TODO visited rounds count must be equal to dag depth:
        //  read/download non-existent rounds and drop too old ones
        while let Some(vertex_round /* r-1 */) = proof_round
            .prev()
            .get()
            .filter(|_| !r.iter().all(BTreeMap::is_empty))
        {
            // take points @ r+0, and select their vertices @ r-1 for commit
            // the order is of NodeId (public key) TODO shuffle deterministically, eg with anchor digest as a seed
            while let Some((node, digest)) = &r[0].pop_first() {
                // Every point must be valid (we've validated anchor dependencies already),
                // but some points don't have previous one to proof as vertex.
                // Any valid point among equivocated will do, as they include the same vertex.
                let Some(proof /* point @ r+0 */) =
                    proof_round.valid_point_exact(node, digest).await
                else {
                    panic!("point to commit not found in DAG")
                };
                let author = &proof.point.body.location.author;
                r[1].extend(proof.point.body.includes.clone()); // points @ r-1
                r[2].extend(proof.point.body.witness.clone()); // points @ r-2
                let Some(digest) = proof.point.body.proof.as_ref().map(|a| &a.digest) else {
                    continue;
                };
                let Some(vertex /* point @ r-1 */) =
                    vertex_round.valid_point_exact(author, &digest).await
                else {
                    panic!("point to commit not found in DAG or wrong round")
                };
                // select uncommitted ones, marking them as committed
                // to exclude from the next commit
                if vertex
                    .is_committed
                    .compare_exchange(false, true, Ordering::Release, Ordering::Relaxed)
                    .is_ok()
                {
                    // vertex will be skipped in r_1 as committed
                    r[2].extend(vertex.point.body.includes.clone()); // points @ r-2
                    r[3].extend(vertex.point.body.witness.clone()); // points @ r-3
                    uncommitted.push_back(vertex.point); // LIFO
                }
            }
            proof_round = vertex_round; // next r+0
            r.rotate_left(1);
        }
        uncommitted
    }
}
