use std::collections::BTreeMap;
use std::convert::identity;
use std::sync::atomic::Ordering;
use std::{array, mem};

use futures_util::FutureExt;
use tokio::sync::mpsc::UnboundedSender;
use tycho_network::PeerId;

use crate::dag::anchor_stage::AnchorStage;
use crate::dag::DagRound;
use crate::effects::{AltFormat, CurrentRoundContext, Effects};
use crate::engine::MempoolConfig;
use crate::intercom::PeerSchedule;
use crate::models::{Digest, LinkField, Location, Point, PointId, Round, ValidPoint};

pub struct Dag {
    // from the oldest to the current round; newer ones are in the future;
    rounds: BTreeMap<Round, DagRound>,
}

impl Dag {
    pub fn new() -> Self {
        Self {
            rounds: BTreeMap::new(),
        }
    }

    pub fn init(&mut self, dag_round: DagRound) {
        assert!(self.rounds.is_empty(), "DAG already initialized");
        self.rounds.insert(dag_round.round(), dag_round);
    }

    pub fn fill_to_top(
        &mut self,
        next_round: Round,
        peer_schedule: &PeerSchedule,
        effects: &Effects<CurrentRoundContext>,
    ) -> DagRound {
        let mut top = match self.rounds.last_key_value() {
            None => unreachable!("DAG cannot be empty if properly initialized"),
            Some((_, top)) => top.clone(),
        };
        if (top.round().0 + MempoolConfig::COMMIT_DEPTH as u32) < next_round.0 {
            tracing::warn!(
                parent: effects.span(),
                lag = next_round.0 - (top.round().0 + MempoolConfig::COMMIT_DEPTH as u32),
                "far behind consensus"
            );
            unimplemented!("sync")
        }
        for _ in top.round().next().0..=next_round.0 {
            top = self
                .rounds
                .entry(top.round().next())
                .or_insert(top.next(peer_schedule))
                .clone();
        }
        top
    }

    fn drop_tail(&mut self, anchor_at: Round) {
        if let Some(tail) = anchor_at.0.checked_sub(MempoolConfig::COMMIT_DEPTH as u32) {
            self.rounds.retain(|k, _| k.0 >= tail);
        };
    }

    /// result is in historical order
    pub fn commit(
        &mut self,
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
            let committed = Self::gather_uncommitted(&anchor.point, anchor_round);
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
            if is_used.swap(true, Ordering::Relaxed) {
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

    /// returns historically ordered vertices
    ///
    /// Note: at this point there is no way to check if passed point is really an anchor
    fn gather_uncommitted(
        anchor: &Point,              // @ r+1
        mut current_round: DagRound, // r+1
    ) -> Vec<Point> {
        fn extend(to: &mut BTreeMap<PeerId, Digest>, from: &BTreeMap<PeerId, Digest>) {
            if to.is_empty() {
                *to = from.clone();
            } else {
                for (peer, digest) in from {
                    to.insert(*peer, digest.clone());
                }
            }
        }
        assert_eq!(
            current_round.round(),
            anchor.body().location.round,
            "passed anchor round does not match anchor point's round"
        );
        let mut r = array::from_fn::<_, 3, _>(|_| BTreeMap::new()); // [r+0, r-1, r-2]
        extend(&mut r[0], &anchor.body().includes); // points @ r+0
        extend(&mut r[1], &anchor.body().witness); // points @ r-1

        let mut uncommitted = Vec::new();

        while let Some(point_round /* r+0 */) = current_round
            .prev()
            .upgrade()
            .filter(|_| !r.iter().all(BTreeMap::is_empty))
        {
            // take points @ r+0, shuffle deterministically with anchor digest as a seed
            let sorted = mem::take(&mut r[0]).into_iter().collect::<Vec<_>>();
            // TODO shuffle deterministically, eg with anchor digest as a seed
            for (node, digest) in &sorted {
                // Every point must be valid (we've validated anchor dependencies already),
                // but some points don't have previous one to proof as vertex.
                // Any equivocated point (except anchor) is ok, as they are globally available
                // because of anchor, and their payload is deduplicated after mempool anyway.
                let global =  // point @ r+0
                    Self::ready_valid_point(&point_round, node, digest);
                // select uncommitted ones, marking them as committed
                // to exclude from the next commit
                if !global.is_committed.swap(true, Ordering::Relaxed) {
                    extend(&mut r[1], &global.point.body().includes); // points @ r-1
                    extend(&mut r[2], &global.point.body().witness); // points @ r-2
                    uncommitted.push(global.point);
                }
            }
            current_round = point_round; // r+0 is a new r+1
            r.rotate_left(1); // [empty r_0, r-1, r-2] => [r-1 as r+0, r-2 as r-1, empty as r-2]
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
