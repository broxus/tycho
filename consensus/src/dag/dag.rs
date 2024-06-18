use std::collections::BTreeMap;
use std::convert::identity;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use futures_util::FutureExt;
use parking_lot::Mutex;
use tokio::sync::mpsc::UnboundedSender;
use tycho_network::PeerId;

use crate::dag::anchor_stage::AnchorStage;
use crate::dag::DagRound;
use crate::effects::{AltFormat, CurrentRoundContext, Effects};
use crate::engine::MempoolConfig;
use crate::intercom::PeerSchedule;
use crate::models::{Digest, LinkField, Location, Point, PointId, Round, ValidPoint};

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
        rounds.insert(dag_round.round(), dag_round);
    }

    pub fn top(&self, round: Round, peer_schedule: &PeerSchedule) -> DagRound {
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
    pub fn commit(
        self,
        next_dag_round: DagRound,
        committed: UnboundedSender<(Point, Vec<Point>)>,
        effects: Effects<CurrentRoundContext>,
    ) {
        // finding the latest trigger must not take long, better try later
        // than wait long for some DagPoint::NotFound, slowing down whole Engine
        let _parent_guard = effects.span().enter();
        let Some(latest_trigger) = Self::latest_trigger(&next_dag_round) else {
            return;
        };
        let _span = tracing::error_span!(
            "commit trigger",
            author = display(&latest_trigger.point.body().location.author.alt()),
            round = latest_trigger.point.body().location.round.0,
            digest = display(&latest_trigger.point.digest().alt()),
        )
        .entered();
        // when we have a valid trigger, its every point of it's subdag is validated successfully
        let mut anchor_stack = Self::anchor_stack(&latest_trigger, next_dag_round.clone());
        let mut ordered = Vec::new();
        while let Some((anchor, anchor_round)) = anchor_stack.pop() {
            // Note every next "little anchor candidate that could" must have at least full dag depth
            // Note if sync is implemented as a second sub-graph - drop up to the last linked in chain
            self.drop_tail(anchor.point.body().location.round);
            let committed = Self::gather_uncommitted(&anchor.point, &anchor_round);
            ordered.push((anchor.point, committed));
        }

        effects.log_committed(&ordered);

        for points in ordered {
            committed
                .send(points) // not recoverable
                .expect("Failed to send anchor commit message tp mpsc channel");
        }
    }

    fn latest_trigger(next_dag_round: &DagRound) -> Option<ValidPoint> {
        let mut next_dag_round = next_dag_round.clone();
        let mut latest_trigger = None;
        while let Some(current_dag_round) = next_dag_round.prev().upgrade() {
            if let Some(AnchorStage::Trigger {
                ref is_used,
                ref leader,
            }) = current_dag_round.anchor_stage()
            {
                if is_used.load(Ordering::Relaxed) {
                    break;
                };
                if let Some(valid) = current_dag_round
                    .view(leader, |loc| {
                        loc.versions()
                            .values()
                            // better try later than wait now if some point is still downloading
                            .filter_map(|version| version.clone().now_or_never())
                            // take any suitable
                            .find_map(move |dag_point| dag_point.into_valid())
                    })
                    .flatten()
                {
                    _ = latest_trigger.insert(valid);
                    is_used.store(true, Ordering::Relaxed);
                    break;
                };
            };
            next_dag_round = current_dag_round;
        }
        latest_trigger
    }

    /// return order: newest (in depth) to oldest (on top); use with `vec.pop()`
    fn anchor_stack(
        last_trigger: &ValidPoint,
        future_round: DagRound,
    ) -> Vec<(ValidPoint, DagRound)> {
        assert_eq!(
            last_trigger.point.prev_id(),
            Some(last_trigger.point.anchor_id(LinkField::Proof)),
            "invalid anchor proof link, trigger point must have been invalidated"
        );
        let mut anchor_stack = Vec::new();
        let mut proof_round = future_round
            .scan(last_trigger.point.body().location.round.prev())
            .expect("anchor proof round not in DAG while a point from it was received");
        let mut proof = Self::ready_valid_point(
            &proof_round,
            &last_trigger.point.body().location.author,
            &last_trigger
                .point
                .body()
                .proof
                .as_ref()
                .expect("validation broken: anchor trigger with empty proof field")
                .digest,
        );
        loop {
            if proof_round.round() == MempoolConfig::GENESIS_ROUND {
                break;
            }
            let Some(AnchorStage::Proof {
                ref leader,
                ref is_used,
            }) = proof_round.anchor_stage()
            else {
                panic!("anchor proof round is not expected, validation is broken")
            };
            assert_eq!(
                proof.point.body().location.round,
                proof_round.round(),
                "anchor proof round does not match"
            );
            assert_eq!(
                proof.point.body().location.author,
                leader,
                "anchor proof author does not match prescribed by round"
            );
            let Some(anchor_round) = proof_round.prev().upgrade() else {
                break;
            };
            if is_used.load(Ordering::Relaxed) {
                break;
            };
            let anchor_digest = match &proof.point.body().proof {
                Some(prev) => &prev.digest,
                None => panic!("anchor proof must prove to anchor point, validation is broken"),
            };
            let anchor = anchor_round
                .view(leader, |loc| {
                    let dag_point = loc
                        .versions()
                        .get(anchor_digest)
                        .expect("anchor proof is not linked to anchor, validation broken")
                        .clone()
                        .now_or_never()
                        .expect("validation must be completed");
                    dag_point.into_valid().expect("anchor point must be valid")
                })
                .expect("leader location not found in dag round for anchor");
            is_used.store(true, Ordering::Relaxed);

            let next_proof_id = anchor.point.anchor_id(LinkField::Proof);
            anchor_stack.push((anchor, anchor_round));

            match proof_round.scan(next_proof_id.location.round) {
                Some(next_proof_round) => proof_round = next_proof_round,
                None => break,
            };
            proof = Self::ready_valid_point(
                &proof_round,
                &next_proof_id.location.author,
                &next_proof_id.digest,
            );
        }
        anchor_stack
    }

    fn drop_tail(&self, anchor_at: Round) {
        if let Some(tail) = anchor_at.0.checked_sub(MempoolConfig::COMMIT_DEPTH as u32) {
            let mut rounds = self.rounds.lock();
            rounds.retain(|k, _| k.0 >= tail);
        };
    }

    /// returns historically ordered vertices
    ///
    /// Note: at this point there is no way to check if passed point is really an anchor
    fn gather_uncommitted(
        anchor: &Point,          // @ r+1
        anchor_round: &DagRound, // r+1
    ) -> Vec<Point> {
        assert_eq!(
            anchor_round.round(),
            anchor.body().location.round,
            "passed anchor round does not match anchor point's round"
        );
        let mut proof_round /* r+0 */ = anchor_round
            .prev()
            .upgrade()
            .expect("previous round for anchor point round must stay in DAG");
        let mut r = [
            anchor.body().includes.clone(), // points @ r+0
            anchor.body().witness.clone(),  // points @ r-1
            BTreeMap::new(),                // points @ r-2
            BTreeMap::new(),                // points @ r-3
        ];
        _ = anchor; // anchor payload will be committed the next time

        let mut uncommitted = Vec::new();

        while let Some(vertex_round /* r-1 */) = proof_round
            .prev()
            .upgrade()
            .filter(|_| !r.iter().all(BTreeMap::is_empty))
        {
            // take points @ r+0, and select their vertices @ r-1 for commit
            // the order is of NodeId (public key)
            // TODO shuffle deterministically, eg with anchor digest as a seed
            while let Some((node, digest)) = &r[0].pop_first() {
                // Every point must be valid (we've validated anchor dependencies already),
                // but some points don't have previous one to proof as vertex.
                // Any valid point among equivocated will do, as they include the same vertex.
                let proof = // point @ r+0
                    Self::ready_valid_point(&proof_round, node, digest);
                let author = &proof.point.body().location.author;
                r[1].extend(proof.point.body().includes.clone()); // points @ r-1
                r[2].extend(proof.point.body().witness.clone()); // points @ r-2
                let Some(digest) = proof.point.body().proof.as_ref().map(|a| &a.digest) else {
                    continue;
                };
                let vertex = // point @ r-1
                    Self::ready_valid_point(&vertex_round, author, digest);
                // select uncommitted ones, marking them as committed
                // to exclude from the next commit
                if vertex
                    .is_committed
                    .compare_exchange(false, true, Ordering::Release, Ordering::Relaxed)
                    .is_ok()
                {
                    // vertex will be skipped in r_1 as committed
                    r[2].extend(vertex.point.body().includes.clone()); // points @ r-2
                    r[3].extend(vertex.point.body().witness.clone()); // points @ r-3
                    uncommitted.push(vertex.point);
                }
            }
            proof_round = vertex_round; // next r+0
            r.rotate_left(1);
        }
        uncommitted.reverse();
        uncommitted
    }

    // needed only in commit where all points are validated and stored in DAG
    fn ready_valid_point(dag_round: &DagRound, author: &PeerId, digest: &Digest) -> ValidPoint {
        dag_round
            .view(author, |loc| {
                loc.versions()
                    .get(digest)
                    .cloned()
                    .ok_or("point digest not found in location's versions")
            })
            .ok_or("point author not found among dag round's locations")
            .and_then(identity) // flatten result
            .and_then(|fut| fut.now_or_never().ok_or("validation must be completed"))
            .and_then(|dag_point| dag_point.into_valid().ok_or("point is not valid"))
            .unwrap_or_else(|msg| {
                let point_id = PointId {
                    location: Location {
                        round: dag_round.round(),
                        author: *author,
                    },
                    digest: digest.clone(),
                };
                panic!("{msg}: {:?}", point_id.alt())
            })
    }
}
