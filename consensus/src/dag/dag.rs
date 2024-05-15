use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use itertools::Itertools;
use parking_lot::Mutex;
use tokio::sync::mpsc::UnboundedSender;

use crate::dag::anchor_stage::AnchorStage;
use crate::dag::DagRound;
use crate::engine::MempoolConfig;
use crate::intercom::PeerSchedule;
use crate::models::{Point, Round, Ugly, ValidPoint};

#[derive(Clone)]
pub struct Dag {
    // from the oldest to the current round; newer ones are in the future;
    rounds: Arc<Mutex<BTreeMap<Round, DagRound>>>,
}

impl Dag {
    pub fn new() -> Self {
        Self {
            rounds: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub fn init(&self, dag_round: DagRound) {
        let mut rounds = self.rounds.lock();
        assert!(rounds.is_empty(), "DAG already initialized");
        rounds.insert(dag_round.round().clone(), dag_round);
    }

    pub fn top(&self, round: &Round, peer_schedule: &PeerSchedule) -> DagRound {
        let mut rounds = self.rounds.lock();
        let mut top = match rounds.last_key_value() {
            None => unreachable!("DAG cannot be empty if properly initialized?"),
            Some((_, top)) => top.clone(),
        };
        if (top.round().0 + MempoolConfig::COMMIT_DEPTH as u32) < round.0 {
            unimplemented!("sync")
        }
        for _ in top.round().next().0..=round.0 {
            top = rounds
                .entry(top.round().next())
                .or_insert(top.next(peer_schedule))
                .clone();
        }
        top
    }

    // Note: cannot be non-async, as we cannot use only InclusionState:
    //   some committed point may be DagPoint::Suspicious thus not the first validated locally
    /// result is in historical order
    pub async fn commit(
        self,
        log_id: Arc<String>,
        next_dag_round: DagRound,
        committed: UnboundedSender<(Arc<Point>, Vec<Arc<Point>>)>,
    ) {
        // TODO finding the latest trigger must not take long, better try later
        //   than wait long for some DagPoint::NotFound, slowing down whole Engine
        let Some(latest_trigger) = Self::latest_trigger(&next_dag_round).await else {
            return;
        };
        // when we have a valid trigger, its every point of it's subdag is validated successfully
        let mut anchor_stack = Self::anchor_stack(&latest_trigger, next_dag_round.clone()).await;
        let mut ordered = Vec::new();
        while let Some((anchor, anchor_round)) = anchor_stack.pop() {
            // Note every next "little anchor candidate that could" must have at least full dag depth
            // Note if sync is implemented as a second sub-graph - drop up to the last linked in chain
            self.drop_tail(anchor.point.body.location.round);
            let committed = Self::gather_uncommitted(&anchor.point, &anchor_round).await;
            ordered.push((anchor.point, committed));
        }

        Self::log_committed(&log_id, next_dag_round.round().prev(), &ordered);

        for anchor_with_history in ordered {
            committed
                .send(anchor_with_history) // not recoverable
                .expect("Failed to send anchor commit message tp mpsc channel");
        }
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
                // Fixme We may take any first completed valid point, but we should not wait long;
                //   can we determine the trigger some other way, maybe inside Collector?
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

    fn drop_tail(&self, anchor_at: Round) {
        if let Some(tail) = anchor_at.0.checked_sub(MempoolConfig::COMMIT_DEPTH as u32) {
            let mut rounds = self.rounds.lock();
            *rounds = rounds.split_off(&Round(tail));
        };
    }

    /// returns historically ordered vertices
    ///
    /// Note: at this point there is no way to check if passed point is really an anchor
    async fn gather_uncommitted(
        anchor: &Point,          // @ r+1
        anchor_round: &DagRound, // r+1
    ) -> Vec<Arc<Point>> {
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

        let mut uncommitted = Vec::new();

        while let Some(vertex_round /* r-1 */) = proof_round
            .prev()
            .get()
            .filter(|_| !r.iter().all(BTreeMap::is_empty))
        {
            // take points @ r+0, and select their vertices @ r-1 for commit
            // the order is of NodeId (public key)
            // TODO shuffle deterministically, eg with anchor digest as a seed
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
                    uncommitted.push(vertex.point);
                }
            }
            proof_round = vertex_round; // next r+0
            r.rotate_left(1);
        }
        uncommitted.reverse();
        uncommitted
    }

    fn log_committed(
        log_id: &str,
        current_round: Round,
        committed: &Vec<(Arc<Point>, Vec<Arc<Point>>)>,
    ) {
        if committed.is_empty() {
            return;
        }
        if tracing::enabled!(tracing::Level::INFO) {
            let committed = committed
                .into_iter()
                .map(|(anchor, history)| {
                    let history = history
                        .iter()
                        .map(|point| format!("{:?}", point.id().ugly()))
                        .join(", ");
                    format!(
                        "anchor {:?} time {} : [ {history} ]",
                        anchor.id().ugly(),
                        anchor.body.time
                    )
                })
                .join("  ;  ");
            tracing::info!("{log_id} @ {current_round:?} committed {committed}");
        }
    }
}
