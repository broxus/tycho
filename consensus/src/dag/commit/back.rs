use std::collections::{BTreeMap, VecDeque};
use std::ops::{Bound, RangeInclusive};
use std::sync::atomic;
use std::{array, mem};

use ahash::HashMapExt;
use futures_util::FutureExt;
use itertools::Itertools;
use rand::SeedableRng;
use rand::prelude::SliceRandom;
use tycho_network::PeerId;
use tycho_util::FastHashMap;

use crate::dag::commit::SyncError;
use crate::dag::commit::anchor_chain::EnqueuedAnchor;
use crate::dag::{DagRound, HistoryConflict};
use crate::effects::{AltFmt, AltFormat, Cancelled};
use crate::engine::MempoolConfig;
use crate::models::{AnchorStageRole, DagPoint, Digest, Link, PointInfo, Round, ValidPoint};

#[derive(Default)]
pub struct DagBack {
    // from the oldest to the current round and the next one - when they are set
    rounds: BTreeMap<Round, DagRound>,
}

impl DagBack {
    pub fn init(&mut self, dag_bottom: &DagRound) {
        assert!(self.rounds.is_empty(), "already init");
        self.rounds.insert(dag_bottom.round(), dag_bottom.clone());
    }

    /// the next after current engine round
    pub fn top(&self) -> &DagRound {
        match self.rounds.last_key_value() {
            None => unreachable!("DAG cannot be empty if properly initialized"),
            Some((_, top)) => top,
        }
    }

    pub fn bottom_round(&self) -> Round {
        match self.rounds.first_key_value() {
            None => unreachable!("DAG cannot be empty if properly initialized"),
            Some((bottom, _)) => *bottom,
        }
    }

    pub fn len(&self) -> usize {
        self.assert_len();
        self.rounds.len()
    }

    // TODO keep DagRounds in EnqueuedAnchor and hide method under "test" feature
    pub fn get(&self, round: Round) -> Option<&DagRound> {
        self.rounds.get(&round)
    }

    pub fn extend_from_front(&mut self, front: &[DagRound]) {
        let front_bottom = match front.first() {
            None => return,
            Some(first) => first, // lowest in input
        };

        let self_top = self.top().round();
        assert!(
            self.top().round().next() >= front_bottom.round(),
            "{} front slice passed to back dag must not contain gaps, front bottom {}",
            self.alt(),
            front_bottom.round().0,
        );

        if front.len() >= 2 {
            assert!(
                front
                    .windows(2)
                    .all(|w| w[0].round().next() == w[1].round()),
                "dag slice must be contiguous: {:?}",
                front
                    .iter()
                    .map(|dag_round| dag_round.round().0)
                    .collect::<Vec<_>>()
            );
        }

        for source_dag_round in front {
            if source_dag_round.round() <= self_top {
                continue; // skip duplicates
            }
            self.rounds
                .insert(source_dag_round.round(), source_dag_round.clone());
        }
        self.assert_len();
    }

    fn assert_len(&self) {
        let top = self.top().round();
        let bottom = self.bottom_round();
        assert_eq!(
            (top.0 - bottom.0) as usize + 1,
            self.rounds.len(),
            "DAG has invalid length to be contiguous"
        );
    }

    pub fn drop_upto(&mut self, new_bottom_round: Round) {
        // TODO use `std::cmp::Reverse` for keys + `BTreeMap::split_off()`; this will also make
        //   order of rounds in DagBack same as in DagFront: newer in back and older in front
        while let Some(entry) = self.rounds.first_entry() {
            if *entry.key() < new_bottom_round {
                entry.remove();
            } else {
                break;
            }
        }
    }

    pub fn drain_upto(&mut self, new_bottom_round: Round) -> Vec<DagRound> {
        let to_drain = (new_bottom_round - self.bottom_round().0).0 as usize;
        let mut drained = Vec::with_capacity(to_drain);
        while let Some(entry) = self.rounds.first_entry() {
            if *entry.key() < new_bottom_round {
                drained.push(entry.remove());
            } else {
                break;
            }
        }
        assert_eq!(to_drain, drained.len(), "drained not contiguous dag part");
        drained
    }

    /// not yet used commit triggers in historical order;
    /// `last_proof_round` allows to continue chain from its end
    pub(super) fn triggers(
        &self,
        range: RangeInclusive<Round>,
    ) -> Result<VecDeque<PointInfo>, HistoryConflict> {
        let mut triggers = VecDeque::new();

        if range.is_empty() {
            // may happen after history is invalidated, do not panic here
            return Ok(triggers);
        }
        let rev_iter = self.rounds.range(range).rev();

        for (_, dag_round) in rev_iter {
            let stage = match dag_round.anchor_stage() {
                Some(stage) if stage.role == AnchorStageRole::Trigger => stage,
                _ => continue,
            };
            if stage.is_used.load(atomic::Ordering::Relaxed) {
                break;
            };
            match Self::any_ready_valid_trigger(dag_round, &stage.leader) {
                Ok(trigger) => {
                    // iter is from newest to oldest, restore historical order
                    triggers.push_front(trigger.info().clone());
                }
                Err(SyncError::TryLater) => {} // skip
                Err(SyncError::HistoryConflict(round)) => return Err(HistoryConflict(round)),
            }
        }
        // tracing::warn!("dag length {} all_triggers: {string}", self.rounds.len());

        Ok(triggers)
    }

    pub(super) fn last_unusable_proof_round(
        &self,
        trigger: &PointInfo,
    ) -> Result<Round, SyncError> {
        // anchor chain is not init yet, and exactly anchor trigger or proof is at the bottom -
        // dag cannot contain corresponding anchor candidate

        let bottom_round = self.bottom_round();
        let mut last_proof = trigger.anchor_id(AnchorStageRole::Proof);

        if last_proof.round <= bottom_round {
            return Ok(last_proof.round);
        };

        // iter for proof->candidate->proof chain
        let mut rev_iter = self
            .rounds
            .range((Bound::Unbounded, Bound::Included(last_proof.round)))
            .rev()
            .peekable();

        while rev_iter.peek().is_some() {
            let (_, proof_dag_round) = rev_iter.next().expect("peek in line above");
            if proof_dag_round.round() > last_proof.round {
                continue;
            }
            assert_eq!(
                proof_dag_round.round(),
                last_proof.round,
                "{} is not contiguous: iter skipped proof round",
                self.alt(),
            );

            match proof_dag_round.anchor_stage() {
                Some(stage) if stage.role == AnchorStageRole::Proof => {
                    assert_eq!(
                        last_proof.author,
                        stage.leader,
                        "validate() is broken: anchor proof author is not leader {:?}",
                        last_proof.alt()
                    );
                }
                _ => panic!(
                    "validate() is broken: anchor stage is not for anchor proof {:?}",
                    last_proof.alt()
                ),
            }

            let proof = Self::ready_valid_point(
                proof_dag_round,
                &last_proof.author,
                &last_proof.digest,
                "anchor proof",
            )?
            .info()
            .clone();

            let Some(anchor_id) = proof.prev_id() else {
                last_proof = proof.anchor_id(AnchorStageRole::Proof);
                if last_proof.round <= bottom_round {
                    return Ok(last_proof.round);
                }
                continue;
            };

            let Some((_, anchor_dag_round)) = rev_iter.next() else {
                return Ok(proof.round());
            };

            assert_eq!(
                anchor_dag_round.round(),
                anchor_id.round,
                "{} is not contiguous: iter skipped anchor round",
                self.alt(),
            );

            let anchor = Self::ready_valid_point(
                anchor_dag_round,
                &anchor_id.author,
                &anchor_id.digest,
                "anchor candidate",
            )?
            .info()
            .clone();

            last_proof = anchor.anchor_id(AnchorStageRole::Proof);

            if last_proof.round <= bottom_round {
                return Ok(last_proof.round);
            };
        }
        unreachable!("iter exhausted, last unusable proof not found")
    }

    // Some contiguous part of anchor chain in historical order; None in case of a gap
    pub(super) fn anchor_chain(
        &self,
        last_proof_round: Round,
        trigger: &PointInfo,
    ) -> Result<VecDeque<EnqueuedAnchor>, SyncError> {
        assert_eq!(
            trigger.anchor_link(AnchorStageRole::Trigger),
            &Link::ToSelf,
            "passed point is not a trigger: {:?}",
            trigger.id().alt()
        );

        if last_proof_round >= trigger.round().prev() {
            // some trigger (point future) from a later round resolved earlier than current one,
            // so this proof is already in chain with `direct_trigger: None`
            // this proof can even be the last element in chain, as those next trigger and proof
            // still wait for corresponding anchor point future to resolve
            return Ok(VecDeque::new());
        }

        let range = RangeInclusive::new(
            // exclude used or unusable proof (it may be out of range)
            last_proof_round.next(),
            // include topmost proof only
            trigger.round().prev(),
        );
        if range.is_empty() {
            // may happen after history is invalidated, do not panic here
            return Ok(VecDeque::new());
        }
        let mut rev_iter = self.rounds.range(range).rev().peekable();

        let mut lookup_proof_id = trigger
            .prev_id()
            .expect("validation broken: anchor trigger without prev point");

        let mut result = VecDeque::new();
        while rev_iter.peek().is_some() {
            let (_, proof_dag_round) = rev_iter.next().expect("peek in line above");
            if proof_dag_round.round() > lookup_proof_id.round {
                continue;
            }
            assert_eq!(
                proof_dag_round.round(),
                lookup_proof_id.round,
                "{} is not contiguous: iter skipped proof round, last proof at {last_proof_round:?}",
                self.alt(),
            );

            match proof_dag_round.anchor_stage() {
                Some(stage) if stage.role == AnchorStageRole::Proof => {
                    assert_eq!(
                        lookup_proof_id.author,
                        stage.leader,
                        "validate() is broken: anchor proof author is not leader {:?}",
                        lookup_proof_id.alt()
                    );
                    if stage.is_used.load(atomic::Ordering::Relaxed) {
                        // this branch must be visited only with engine round change
                        // (new call to commit), when `anchor_chain` is emptied;
                        // during the same commit call, expect `anchor_chain` to serve its purpose
                        assert!(
                            last_proof_round <= self.bottom_round(),
                            "limit by round range is broken: visiting already committed proof {:?}",
                            lookup_proof_id.alt()
                        );
                        // reached already committed proof, so dag is contiguous
                        return Ok(result);
                    }
                }
                _ => panic!(
                    "validate() is broken: anchor stage is not for anchor proof {:?}",
                    lookup_proof_id.alt()
                ),
            }
            let proof = Self::ready_valid_point(
                proof_dag_round,
                &lookup_proof_id.author,
                &lookup_proof_id.digest,
                "anchor proof",
            )?
            .info()
            .clone();

            let Some(anchor_id) = proof.prev_id() else {
                lookup_proof_id = proof.anchor_id(AnchorStageRole::Proof);
                continue;
            };

            let Some((_, anchor_dag_round)) = rev_iter.next() else {
                assert!(
                    anchor_id.round <= self.bottom_round(), // bottom is excluded by iter bound
                    "{} cannot retrieve anchor {:?}, last proof at {last_proof_round:?}",
                    self.alt(),
                    anchor_id.alt(),
                );
                break;
            };
            assert_eq!(
                anchor_dag_round.round(),
                anchor_id.round,
                "{} is not contiguous: iter skipped anchor round, last proof at {last_proof_round:?}",
                self.alt(),
            );

            let anchor = Self::ready_valid_point(
                anchor_dag_round,
                &anchor_id.author,
                &anchor_id.digest,
                "anchor candidate",
            )?
            .info()
            .clone();

            let mut direct_trigger = None;
            if proof.round() == trigger.round().prev()
                && proof.id() == trigger.anchor_id(AnchorStageRole::Proof)
            {
                direct_trigger = Some(trigger.clone());
            }
            lookup_proof_id = anchor.anchor_id(AnchorStageRole::Proof);

            // iter is from newest to oldest, restore historical order
            result.push_front(EnqueuedAnchor {
                anchor,
                proof,
                direct_trigger,
            });
        }

        let linked_to_proof_round = result
            .front()
            .ok_or(SyncError::TryLater)?
            .anchor
            .anchor_round(AnchorStageRole::Proof);
        if linked_to_proof_round <= last_proof_round {
            Ok(result)
        } else {
            Err(SyncError::TryLater)
        }
    }

    /// returns globally available points in historical order;
    /// `None` is a signal to break whole assembled commit chain and retry later
    ///
    /// Note: at this point there is no way to check if passed point is really an anchor
    pub(super) fn gather_uncommitted(
        &self,
        full_history_bottom: Round,
        anchor: &PointInfo, // @ r+1
        conf: &MempoolConfig,
    ) -> Result<VecDeque<ValidPoint>, SyncError> {
        fn extend(to: &mut FastHashMap<Digest, PeerId>, from: &FastHashMap<PeerId, Digest>) {
            if to.is_empty() {
                to.reserve(from.len());
            }
            for (peer, digest) in from {
                to.insert(*digest, *peer);
            }
        }
        // do not commit genesis - we may place some arbitrary payload in it,
        // also mempool adapter does not expect it, and collator cannot use it too
        let history_limit = (conf.genesis_round.next())
            .max(anchor.round() - conf.consensus.commit_history_rounds.get());

        let mut r = array::from_fn::<_, 3, _>(|_| FastHashMap::new()); // [r+0, r-1, r-2]
        extend(&mut r[0], anchor.includes()); // points @ r+0
        extend(&mut r[1], anchor.witness()); // points @ r-1

        let mut rng = rand_pcg::Pcg64::from_seed(*anchor.digest().inner());
        let mut uncommitted = VecDeque::new();

        let rev_iter = self
            .rounds
            .range(RangeInclusive::new(history_limit, anchor.round().prev()))
            .rev();

        let mut next_round = anchor.round();
        for (_, point_round /* r+0 */) in rev_iter {
            assert_eq!(
                point_round.round().next(),
                next_round,
                "{} is not contiguous",
                self.alt(),
            );
            next_round = point_round.round();

            // take points @ r+0, shuffle deterministically with anchor digest as a seed
            let mut sorted = mem::take(&mut r[0]).into_iter().collect::<Vec<_>>();
            sorted.sort_unstable();
            sorted.shuffle(&mut rng);
            for (digest, peer) in &sorted {
                // Every point must be valid (we've validated anchor dependencies already),
                // but some points don't have previous one to proof as vertex.
                // Any equivocated point (except anchor) is ok, as they are globally available
                // because of anchor, and their payload is deduplicated after mempool anyway.
                let global = // point @ r+0; break and return `None` if not ready yet
                    Self::ready_valid_point(point_round, peer, digest, "point")?;
                // select only uncommitted ones
                if !global.is_committed().load(atomic::Ordering::Relaxed) {
                    extend(&mut r[1], global.info().includes()); // points @ r-1
                    extend(&mut r[2], global.info().witness()); // points @ r-2
                    uncommitted.push_front(global);
                }
            }
            r.rotate_left(1); // [empty r_0, r-1, r-2] => [r-1 as r+0, r-2 as r-1, empty as r-2]
        }
        // we should commit first anchors at COMMIT_ROUNDS from bottom (inclusive), discarding them
        // (because some history may be lost) in adapter when bottom is not genesis
        // (in case dag bottom is moved after a large gap or severe collator lag behind consensus);
        // note inclusive bound (`bottom`, not `after`) because anchor payload not committed
        if history_limit >= full_history_bottom {
            assert_eq!(
                next_round,
                history_limit,
                "{} doesn't contain full anchor history",
                self.alt(),
            );
        }
        Ok(uncommitted)
    }

    fn any_ready_valid_trigger(
        dag_round: &DagRound,
        author: &PeerId,
    ) -> Result<ValidPoint, SyncError> {
        dag_round
            .view(author, |loc| {
                loc.versions
                    .values()
                    // better try later than wait now if some point is still downloading
                    .filter_map(|version| version.clone().now_or_never())
                    .map(|task_result| match task_result {
                        Ok(dag_point) => Ok(dag_point),
                        Err(Cancelled()) => Err(SyncError::TryLater),
                    })
                    // take any suitable
                    .filter_map_ok(move |dag_point| match dag_point {
                        DagPoint::Valid(valid) => {
                            if valid.info().anchor_trigger() == &Link::ToSelf {
                                Some(Ok(valid))
                            } else {
                                None
                            }
                        }
                        not_valid if not_valid.is_certified() => {
                            Some(Err(SyncError::HistoryConflict(dag_round.round())))
                        }
                        DagPoint::Invalid(_) | DagPoint::NotFound(_) | DagPoint::IllFormed(_) => {
                            None
                        }
                    })
                    .flatten()
                    .find_or_first(|result| result.is_ok())
            })
            .flatten()
            .unwrap_or(Err(SyncError::TryLater))
    }

    // needed only in commit where all points are validated and stored in DAG
    /// returns only valid point (panics on invalid); `None` if not ready yet
    fn ready_valid_point(
        dag_round: &DagRound,
        author: &PeerId,
        digest: &Digest,
        point_kind: &'static str,
    ) -> Result<ValidPoint, SyncError> {
        let Some(dag_point_result) = dag_round
            .view(author, |loc| loc.versions.get(digest).cloned()) // not yet created
            .flatten()
            .and_then(|p| p.now_or_never())
        else {
            return Err(SyncError::TryLater);
        }; // not yet resolved;
        let dag_point = match dag_point_result {
            Ok(dag_point) => dag_point,
            Err(Cancelled()) => return Err(SyncError::TryLater),
        };
        match dag_point {
            DagPoint::Valid(valid) => Ok(valid),
            not_valid if not_valid.is_certified() => {
                Err(SyncError::HistoryConflict(dag_round.round()))
            }
            dp @ (DagPoint::Invalid(_) | DagPoint::NotFound(_) | DagPoint::IllFormed(_)) => {
                panic!("{point_kind} {}: {:?}", dp.alt(), dp.id().alt())
            }
        }
    }
}

impl AltFormat for DagBack {}
impl std::fmt::Debug for AltFmt<'_, DagBack> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (round, dag_round) in &AltFormat::unpack(self).rounds {
            write!(f, "{}={:?} ", round.0, dag_round.alt())?;
        }
        Ok(())
    }
}
impl std::fmt::Display for AltFmt<'_, DagBack> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let this = &AltFormat::unpack(self);
        write!(
            f,
            "DagBack len {} [{}..{}]",
            this.rounds.len(),
            this.bottom_round().0,
            this.top().round().0,
        )
    }
}
