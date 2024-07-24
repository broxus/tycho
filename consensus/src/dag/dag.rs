use std::collections::BTreeMap;
use std::convert::identity;
use std::sync::atomic::Ordering;
use std::{array, mem};

use futures_util::FutureExt;
use rand::prelude::SliceRandom;
use rand::SeedableRng;
use tycho_network::PeerId;

use crate::dag::anchor_stage::AnchorStage;
use crate::dag::DagRound;
use crate::effects::{AltFormat, Effects, EngineContext};
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

    pub fn init(&mut self, dag_round: DagRound, next_dag_round: DagRound) {
        assert_eq!(
            Some(dag_round.round()),
            next_dag_round
                .prev()
                .upgrade()
                .map(|dag_round| dag_round.round()),
            "incorrect rounds to init DAG"
        );
        assert!(self.rounds.is_empty(), "DAG already initialized");
        self.rounds.insert(dag_round.round(), dag_round);
        self.rounds.insert(next_dag_round.round(), next_dag_round);
    }

    /// the next after current engine round
    pub fn top(&self) -> DagRound {
        match self.rounds.last_key_value() {
            None => unreachable!("DAG cannot be empty if properly initialized"),
            Some((_, top)) => top.clone(),
        }
    }

    pub fn fill_to_top(
        &mut self,
        next_round: Round,
        peer_schedule: &PeerSchedule,
        effects: &Effects<EngineContext>,
    ) -> DagRound {
        let mut top = match self.rounds.last_key_value() {
            None => unreachable!("DAG cannot be empty if properly initialized"),
            Some((_, top)) => top.clone(),
        };
        if (top.round().0 + MempoolConfig::COMMIT_DEPTH as u32) < next_round.0 {
            tracing::warn!(
                parent: effects.span(),
                lag = next_round.0 - top.round().0,
                "far behind consensus"
            );
        }
        if (top.round().0 + MempoolConfig::ROUNDS_LAG_BEFORE_SYNC as u32) < next_round.0 {
            tracing::warn!(
                parent: effects.span(),
                lag = next_round.0 - top.round().0,
                "need sync"
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
    pub fn commit(&mut self, next_dag_round: DagRound) -> Vec<(Point, Vec<Point>)> {
        // The call must not take long, better try later than wait now, slowing down whole Engine.
        // Try to collect longest anchor chain in historical order, until any unready point is met:
        // * take all ready and uncommitted triggers, skipping not ready ones
        // * recover the longest ready anchor chain in historical order, remember anchor triplets
        // * take anchors one-by one and commit every whole history, while all points are ready
        // * mark as committed all at once: every point in anchor history, proof and trigger
        // So any non-ready point:
        // * in chain of anchor triplets:
        // * * if it's a trigger - it may be ignored
        // * * otherwise: breaks the chain, so that only its prefix can be committed
        // * in anchor history: cancels current commit and the latter anchor chain

        let mut ordered = Vec::new();

        // take all ready triggers, skipping not ready ones
        let mut trigger_stack = Self::trigger_stack(next_dag_round);
        let _span = if let Some((latest_trigger, _)) = trigger_stack.first() {
            tracing::error_span!(
                "commit trigger",
                author = display(&latest_trigger.body().location.author.alt()),
                round = latest_trigger.body().location.round.0,
                digest = display(&latest_trigger.digest().alt()),
            )
            .entered()
        } else {
            return ordered; // empty
        };

        let mut anchors = BTreeMap::new(); // sorted and unique
        let mut bottom_proof_round = MempoolConfig::GENESIS_ROUND;
        // traverse from oldest to newest;
        // ignore non-ready triggers as chain may be restored without them
        while let Some((trigger, trigger_round)) = trigger_stack.pop() {
            Self::anchor_stack(
                &trigger,
                trigger_round.clone(),
                bottom_proof_round,
                &mut anchors,
            );
            if let Some((_, _, last_proof_round, _)) = anchors.values().last() {
                // should not traverse deeper than last proof
                bottom_proof_round = last_proof_round.round();
            }
        }

        for (_, (anchor, anchor_round, proof_round, trigger_round)) in anchors {
            // Note every next "little anchor candidate that could" must have at least full dag depth
            // Note if sync is implemented as a second sub-graph - drop up to the last linked in chain
            self.drop_tail(anchor_round.round());
            let Some(uncommitted_rev) = Self::gather_uncommitted_rev(&anchor.point, anchor_round)
            else {
                break; // will continue at the next call
            };
            match proof_round.anchor_stage() {
                Some(AnchorStage::Proof { is_used, .. }) => {
                    is_used.store(true, Ordering::Relaxed);
                }
                _ => panic!("expected AnchorStage::Proof"),
            };
            // Note a proof may be marked as used while it is fired by a future tigger, which
            //   may be left unmarked at the current run until upcoming points become ready
            match trigger_round.as_ref().map(|tr| tr.anchor_stage()) {
                Some(Some(AnchorStage::Trigger { is_used, .. })) => {
                    is_used.store(true, Ordering::Relaxed);
                }
                Some(_) => panic!("expected AnchorStage::Trigger"),
                None => {} // anchor triplet without direct trigger (not ready/valid/exists)
            };
            // Note every iteration marks committed points before next uncommitted are gathered
            let committed = uncommitted_rev
                .into_iter()
                .rev() // return historical order
                .map(|valid| {
                    valid.is_committed.store(true, Ordering::Relaxed);
                    valid.point
                })
                .collect::<Vec<_>>();
            ordered.push((anchor.point, committed));
        }
        ordered
    }

    /// not yet used commit triggers in reverse order (newest in front and oldest in back);
    /// use with `vec::pop()`
    fn trigger_stack(mut dag_round: DagRound) -> Vec<(Point, DagRound)> {
        let mut latest_trigger = Vec::new();
        loop {
            let prev_dag_round = dag_round.prev().upgrade();

            if let Some(AnchorStage::Trigger {
                ref is_used,
                ref leader,
            }) = dag_round.anchor_stage()
            {
                if is_used.load(Ordering::Relaxed) {
                    break;
                };

                if let Some(valid) = dag_round
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
                    latest_trigger.push((valid.point, dag_round.clone()));
                };
            };

            match prev_dag_round {
                Some(prev_dag_round) if prev_dag_round.round() > MempoolConfig::GENESIS_ROUND => {
                    dag_round = prev_dag_round;
                }
                _ => break,
            };
        }
        latest_trigger
    }

    /// return order: newest (in depth) to oldest (on top); use with `vec.pop()`
    /// return values: anchor round, anchor point, anchor round, proof round, direct trigger round
    fn anchor_stack(
        trigger: &Point,
        trigger_round: DagRound,
        bottom_proof_round: Round,
        result: &mut BTreeMap<Round, (ValidPoint, DagRound, DagRound, Option<DagRound>)>,
    ) {
        assert_eq!(
            trigger.prev_id(),
            Some(trigger.anchor_id(LinkField::Proof)),
            "invalid anchor proof link, trigger point must have been invalidated"
        );
        assert_eq!(
            trigger.body().location.round,
            trigger_round.round(),
            "trigger round does not match trigger point"
        );
        let mut proof_id = trigger
            .prev_id()
            .expect("validation broken: anchor trigger with empty proof field");
        let mut proof_round = trigger_round
            .prev()
            .upgrade()
            .expect("anchor proof round not in DAG while a point from it was received");
        let mut trigger_round = Some(trigger_round); // use only as a part of matching triplet
        loop {
            assert_eq!(
                proof_id.location.round,
                proof_round.round(),
                "anchor proof id round does not match"
            );
            if proof_id.location.round == bottom_proof_round {
                break;
            }
            let Some(proof) =
                Self::ready_valid_point(&proof_round, &proof_id.location.author, &proof_id.digest)
            else {
                break;
            };
            assert_eq!(
                proof.point.body().location.round,
                proof_round.round(),
                "anchor proof round does not match"
            );
            let Some(AnchorStage::Proof {
                ref leader,
                ref is_used,
            }) = proof_round.anchor_stage()
            else {
                panic!("anchor proof round is not expected, validation is broken")
            };
            assert_eq!(
                proof.point.body().location.author,
                leader,
                "anchor proof author does not match prescribed by round"
            );
            if is_used.load(Ordering::Relaxed) {
                break;
            };
            let anchor_digest = match &proof.point.body().proof {
                Some(prev) => &prev.digest,
                None => panic!("anchor proof must prove to anchor point, validation is broken"),
            };
            let Some(anchor_round) = proof_round.prev().upgrade() else {
                break;
            };
            let Some(anchor) = anchor_round
                .view(leader, |loc| {
                    loc.versions()
                        .get(anchor_digest)
                        .expect("anchor proof is not linked to anchor, validation broken")
                        .clone()
                        .now_or_never() // pass this option to statement result
                        .map(|dag_point| {
                            dag_point.into_valid().expect("anchor point must be valid")
                        })
                })
                .expect("leader location not found in dag round for anchor")
            else {
                break;
            };

            proof_id = anchor.point.anchor_id(LinkField::Proof);
            let next_proof_round = anchor_round.scan(proof_id.location.round);

            // safety net: as rounds are traversed from oldest to newest,
            // trigger can be met only at first time its candidate round is met;
            // although logic keeps trigger from being overwritten, ensure with `entry` API
            let trigger_round =
                mem::take(&mut trigger_round).filter(|tr| proof_round.round() == tr.round().prev());
            result.entry(anchor_round.round()).or_insert((
                anchor,
                anchor_round,
                proof_round,
                trigger_round,
            ));

            match next_proof_round {
                Some(next_proof_round) => proof_round = next_proof_round,
                None => break,
            };
        }
    }

    /// returns globally available points in reversed historical order;
    /// `None` is a signal to break whole assembled commit chain and retry later
    ///
    /// Note: at this point there is no way to check if passed point is really an anchor
    fn gather_uncommitted_rev(
        anchor: &Point,              // @ r+1
        mut current_round: DagRound, // r+1
    ) -> Option<Vec<ValidPoint>> {
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

        let mut rng = rand_pcg::Pcg64::from_seed(*anchor.digest().inner());
        let mut uncommitted_rev = Vec::new();

        while let Some(point_round /* r+0 */) = current_round
            .prev()
            .upgrade()
            .filter(|_| !r.iter().all(BTreeMap::is_empty))
        {
            // take points @ r+0, shuffle deterministically with anchor digest as a seed
            let mut sorted = mem::take(&mut r[0]).into_iter().collect::<Vec<_>>();
            sorted.shuffle(&mut rng);
            for (node, digest) in &sorted {
                // Every point must be valid (we've validated anchor dependencies already),
                // but some points don't have previous one to proof as vertex.
                // Any equivocated point (except anchor) is ok, as they are globally available
                // because of anchor, and their payload is deduplicated after mempool anyway.
                let global = // point @ r+0; break and return `None` if not ready yet
                    Self::ready_valid_point(&point_round, node, digest)?;
                // select only uncommitted ones
                if !global.is_committed.load(Ordering::Relaxed) {
                    extend(&mut r[1], &global.point.body().includes); // points @ r-1
                    extend(&mut r[2], &global.point.body().witness); // points @ r-2
                    uncommitted_rev.push(global);
                }
            }
            current_round = point_round; // r+0 is a new r+1
            r.rotate_left(1); // [empty r_0, r-1, r-2] => [r-1 as r+0, r-2 as r-1, empty as r-2]
        }
        Some(uncommitted_rev)
    }

    // needed only in commit where all points are validated and stored in DAG
    /// returns only valid point (panics on invalid); `None` if not ready yet
    fn ready_valid_point(
        dag_round: &DagRound,
        author: &PeerId,
        digest: &Digest,
    ) -> Option<ValidPoint> {
        dag_round
            .view(author, |loc| {
                loc.versions()
                    .get(digest)
                    .cloned()
                    .ok_or("point digest not found in location's versions")
            })
            .ok_or("point author not found among dag round's locations")
            .and_then(identity) // flatten result
            .and_then(|fut| {
                fut.now_or_never()
                    .map(|dag_point| dag_point.into_valid().ok_or("point is not valid"))
                    .transpose()
            })
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
