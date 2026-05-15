use std::collections::{BTreeMap, VecDeque};
use std::ops::RangeInclusive;
use std::{array, mem};

use ahash::HashMapExt;
use futures_util::FutureExt;
use rand::SeedableRng;
use rand::prelude::SliceRandom;
use tycho_network::PeerId;
use tycho_util::FastHashMap;

use crate::dag::commit::EnqueuedAnchor;
use crate::dag::{DagRound, HistoryConflict};
use crate::effects::{AltFmt, AltFormat};
use crate::engine::{EngineResult, MempoolConfig};
use crate::models::{AnchorLink, Committable, DagPoint, Digest, PointInfo, Round, ValidPoint};

#[derive(Default)]
pub struct DagBack {
    // from the oldest to the current round and the next one - when they are set
    rounds: BTreeMap<Round, DagRound>,
    pub last_committed_proof: Option<Round>,
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

    // TODO: use `std::cmp::Reverse` for keys + `BTreeMap::split_off()`; this will also make
    //   order of rounds in DagBack same as in DagFront: newer in back and older in front
    pub fn drain_upto(&mut self, new_bottom_round: Round) -> Vec<DagRound> {
        let to_drain = (new_bottom_round - self.bottom_round().0).0 as usize;
        let drained = (self.rounds)
            .extract_if(..new_bottom_round, |_, _| true)
            .map(|(_, value)| value)
            .collect::<Vec<_>>();
        assert_eq!(to_drain, drained.len(), "drained not contiguous dag part");
        drained
    }

    // Some contiguous part of anchor chain in historical order; None in case of a gap
    pub(super) fn anchor_chain(
        &self,
        trigger: &PointInfo,
    ) -> EngineResult<VecDeque<EnqueuedAnchor>> {
        let bottom_round =
            (self.last_committed_proof).map_or(self.bottom_round(), |proof| proof.next());

        let range = RangeInclusive::new(
            // exclude used or unusable proof (it may be out of range)
            bottom_round,
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
                "{} is not contiguous: iter skipped proof round, bottom at {bottom_round:?}",
                self.alt(),
            );

            if let Some(committed_author) = proof_dag_round.used_anchor_proof().get() {
                assert_eq!(
                    lookup_proof_id.author,
                    committed_author,
                    "broken anchor chain: searched for {:?} but committed proof author is {}",
                    lookup_proof_id.alt(),
                    committed_author.alt(),
                );
                assert!(
                    bottom_round <= proof_dag_round.round(),
                    "limit by round range is broken: visiting already committed proof {:?}",
                    lookup_proof_id.alt()
                );
                // reached already committed proof, so dag is contiguous
                break;
            }

            let proof = Self::chained_proof(
                proof_dag_round,
                &lookup_proof_id.author,
                &lookup_proof_id.digest,
            )?;

            let Some(anchor_id) = proof.prev_id() else {
                panic!("validate() is broken: anchor proof doesn't have a prev point");
            };

            let Some((_, anchor_dag_round)) = rev_iter.next() else {
                assert!(
                    anchor_id.round <= self.bottom_round(), // bottom is excluded by iter bound
                    "{} cannot retrieve anchor {:?}, bottom at {bottom_round:?}",
                    self.alt(),
                    anchor_id.alt(),
                );
                break;
            };
            assert_eq!(
                anchor_dag_round.round(),
                anchor_id.round,
                "{} is not contiguous: iter skipped anchor round, bottom at {bottom_round:?}",
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

            let direct_trigger = (proof.round() == trigger.round().prev()
                && proof.author() == trigger.author()
                && Some(proof.digest()) == trigger.prev_digest())
            .then(|| trigger.clone());

            lookup_proof_id = proof
                .chained_anchor_proof()
                .expect("verify() is broken: anchor proof doesn't have a chained one")
                .to;

            // iter is from newest to oldest, restore historical order
            result.push_front(EnqueuedAnchor {
                anchor,
                proof,
                prev_proof_round: lookup_proof_id.round,
                direct_trigger,
            });
        }

        assert!(
            (result.front()).is_none_or(|i| i.prev_proof_round <= bottom_round),
            "{} is not contiguous: chain does not link across bottom at {bottom_round:?}",
            self.alt(),
        );
        Ok(result)
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
    ) -> EngineResult<VecDeque<Committable>> {
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
        r[0].insert(*anchor.digest(), *anchor.author());

        let mut rng = rand_pcg::Pcg64::from_seed(*anchor.digest().inner());
        let mut uncommitted = VecDeque::new();

        let rev_iter = self
            .rounds
            .range(RangeInclusive::new(history_limit, anchor.round()))
            .rev();

        let mut next_round = anchor.round().next();
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
                    Self::committable_point(point_round, peer, digest)?;
                // select only uncommitted ones
                if !global.is_committed() {
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

    fn chained_proof(
        dag_round: &DagRound,
        author: &PeerId,
        digest: &Digest,
    ) -> EngineResult<PointInfo> {
        let proof = Self::ready_valid_point(dag_round, author, digest, "chained anchor proof")?
            .info()
            .clone();

        assert_eq!(
            proof.anchor_proof(),
            &AnchorLink::ToSelf,
            "validate() is broken: skipped anchor proofs are not allowed in proof chain"
        );

        Ok(proof)
    }

    /// returns only valid point (panics on invalid); `None` if not ready yet
    fn ready_valid_point(
        dag_round: &DagRound,
        author: &PeerId,
        digest: &Digest,
        point_kind: &'static str,
    ) -> EngineResult<ValidPoint> {
        let dag_point = dag_round
            .view(author, |loc| loc.versions.get(digest).cloned()) // not yet created
            .flatten()
            .and_then(|p| p.now_or_never())
            .transpose()? // cancelled
            .expect("point to commit must be resolved");
        match dag_point {
            DagPoint::Valid(valid) => Ok(valid),
            not_valid if not_valid.is_certified() => Err(HistoryConflict(dag_round.round()).into()),
            dp => {
                panic!("{point_kind} {}: {:?}", dp.alt(), dp.id().alt())
            }
        }
    }

    // needed only in commit where all points are validated and stored in DAG
    fn committable_point(
        dag_round: &DagRound,
        author: &PeerId,
        digest: &Digest,
    ) -> EngineResult<Committable> {
        let dag_point = dag_round
            .view(author, |loc| loc.versions.get(digest).cloned()) // not yet created
            .flatten()
            .and_then(|p| p.now_or_never())
            .transpose()? // cancelled
            .expect("point to commit must be resolved");
        match dag_point {
            DagPoint::Valid(valid) => Ok(valid.committable()),
            DagPoint::TransInvalid(invalid) => {
                if invalid.has_proof() {
                    return Err(HistoryConflict(dag_round.round()).into());
                }
                Ok(invalid.committable())
            }
            not_valid if not_valid.is_certified() => Err(HistoryConflict(dag_round.round()).into()),
            dp @ (DagPoint::Invalid(_) | DagPoint::NotFound(_) | DagPoint::IllFormed(_)) => {
                panic!("not committable {}: {:?}", dp.alt(), dp.id().alt())
            }
        }
    }
}

impl AltFormat for DagBack {}
impl std::fmt::Debug for AltFmt<'_, DagBack> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = AltFormat::unpack(self);
        f.write_str("{ last_committed_proof=")?;
        write!(f, "{:?}; ", inner.last_committed_proof)?;
        for (round, dag_round) in &inner.rounds {
            write!(f, "{}={:?} ", round.0, dag_round.alt())?;
        }
        f.write_str(" }")?;
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
