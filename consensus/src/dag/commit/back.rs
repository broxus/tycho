use std::collections::{BTreeMap, VecDeque};
use std::convert::identity;
use std::ops::Bound;
use std::sync::atomic;
use std::{array, cmp, mem};

use futures_util::FutureExt;
use itertools::Itertools;
use rand::prelude::SliceRandom;
use rand::SeedableRng;
use tycho_network::PeerId;
use tycho_storage::point_status::PointStatus;

use crate::dag::{DagRound, EnqueuedAnchor};
use crate::effects::{AltFmt, AltFormat, Effects, EngineContext, MempoolStore};
use crate::engine::round_watch::Consensus;
use crate::engine::MempoolConfig;
use crate::intercom::{Downloader, PeerSchedule};
use crate::models::{AnchorStageRole, Digest, Link, PointId, PointInfo, Round, ValidPoint};

#[derive(Default)]
pub struct DagBack {
    // from the oldest to the current round and the next one - when they are set
    rounds: BTreeMap<Round, DagRound>,
}

impl DagBack {
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

    #[cfg(feature = "test")]
    #[allow(dead_code, reason = "false positive as used inside macro call only")]
    pub fn len(&self) -> usize {
        self.assert_len();
        self.rounds.len()
    }

    // TODO keep DagRounds in EnqueuedAnchor and hide method under "test" feature
    pub fn get(&self, round: Round) -> Option<&DagRound> {
        self.rounds.get(&round)
    }

    pub fn fill_restore(
        &mut self,
        sorted: Vec<(PointInfo, PointStatus)>,
        downloader: &Downloader,
        store: &MempoolStore,
        effects: &Effects<EngineContext>,
    ) {
        assert!(
            sorted.windows(2).all(|w| w[0].0.round() <= w[1].0.round()),
            "input must be sorted: [{}]",
            sorted
                .iter()
                .map(|(info, _)| format!("{:?}", info.id().alt()))
                .join("; ")
        );

        // must be initialized
        let mut rounds_iter = self.rounds.iter().peekable();

        'sorted: for (info, status) in sorted {
            while let Some((round, dag_round)) = rounds_iter.peek() {
                if **round < info.round() {
                    _ = rounds_iter.next();
                } else {
                    assert_eq!(
                        dag_round.round(),
                        info.round(),
                        "dag is not contiguous: next dag round skips over point round"
                    );
                    _ = dag_round.restore_exact(&info, status, downloader, store, effects);
                    continue 'sorted;
                }
            }
        }

        self.assert_len();
    }

    pub fn extend_from_front(&mut self, front: &[DagRound], peer_schedule: &PeerSchedule) {
        let front_first = match front.first() {
            None => return,
            Some(first) => first, // lowest in input
        };
        let history_bottom = match front.last() {
            None => return,
            // extend to the max possible - will be shortened when top known is determined
            Some(last) => Consensus::history_bottom(last.round()),
        };
        if front.len() >= 2 {
            // TODO debug_assert when whole sync feature gets stabilized
            assert!(
                front
                    .windows(2)
                    .all(|w| w[0].round() < history_bottom || w[0].round().next() == w[1].round()),
                "dag slice must be contiguous above history bottom {}: {:?}",
                history_bottom.0,
                front
                    .iter()
                    .map(|dag_round| dag_round.round().0)
                    .collect::<Vec<_>>()
            );
        }

        if self.rounds.is_empty() || self.top().round() < history_bottom {
            self.rounds.clear();
            // init with bottom exactly;
            // older rounds behind weak refs will be dropped by going out of scope
            match front_first.round().cmp(&history_bottom) {
                cmp::Ordering::Less => {
                    if let Some(bottom) = front
                        .iter()
                        .find(|ahead| ahead.round() >= history_bottom)
                        .filter(|ahead| ahead.round() == history_bottom)
                    {
                        self.rounds.insert(bottom.round(), bottom.clone());
                    } else {
                        let bottom = DagRound::new_bottom(history_bottom, peer_schedule);
                        self.rounds.insert(bottom.round(), bottom);
                    }
                }
                cmp::Ordering::Equal => {
                    self.rounds.insert(front_first.round(), front_first.clone());
                }
                cmp::Ordering::Greater => {
                    let bottom = DagRound::new_bottom(history_bottom, peer_schedule);
                    self.rounds.insert(bottom.round(), bottom);
                }
            }
        } else if self.bottom_round() < history_bottom {
            _ = self.drain_upto(history_bottom);
            self.assert_len();
        }

        for source_dag_round in front {
            // skip duplicates
            if source_dag_round.round() <= self.top().round() {
                continue;
            }
            // fill gap if there is one
            for _ in self.top().round().0..source_dag_round.round().0 {
                let next = self.top().new_next(peer_schedule);
                self.rounds.insert(next.round(), next);
            }
            self.rounds
                .insert(source_dag_round.round(), source_dag_round.clone());
        }
        self.assert_len();

        metrics::gauge!("tycho_mempool_rounds_dag_length").set(self.rounds.len() as u32);
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

    pub fn drain_upto(&mut self, new_bottom_round: Round) -> Vec<DagRound> {
        // strictly `COMMIT_DEPTH` rounds will precede the current one, which is +1 to the new length
        // let new_bottom_round = Round((current.0).saturating_sub(MempoolConfig::COMMIT_DEPTH as u32));

        let bottom_round = self.bottom_round(); // assures that dag is contiguous
        let amount = new_bottom_round.0.saturating_sub(bottom_round.0) as usize;
        let mut result = Vec::with_capacity(amount);

        // TODO use `std::cmp::Reverse` for keys + `BTreeMap::split_off()`; this will also make
        // order of rounds in DagBack same as in DagFront: newer in back and older in front
        while let Some(entry) = self.rounds.first_entry() {
            if *entry.key() < new_bottom_round {
                result.push(entry.remove());
            } else {
                break;
            }
        }
        result
    }

    /// not yet used commit triggers in historical order;
    /// `last_proof_round` allows to continue chain from its end
    pub fn triggers(&self, last_proof_round_or_bottom: Round, up_to: Round) -> VecDeque<PointInfo> {
        let mut triggers = VecDeque::new();
        // let mut string = String::new();
        let rev_iter = self
            .rounds
            .range((
                Bound::Excluded(last_proof_round_or_bottom.next()),
                Bound::Included(up_to),
            ))
            .rev();

        for (_, dag_round) in rev_iter {
            let stage = match dag_round.anchor_stage() {
                Some(stage) if stage.role == AnchorStageRole::Trigger => stage,
                _ => continue,
            };
            if stage.is_used.load(atomic::Ordering::Relaxed) {
                break;
            };
            if let Some(trigger) =
                Self::any_ready_valid_point(dag_round, &stage.leader /* , &mut string */)
            {
                // iter is from newest to oldest, restore historical order
                triggers.push_front(trigger.info);
            }
        }
        // tracing::warn!("dag length {} all_triggers: {string}", self.rounds.len());

        triggers
    }

    // Some contiguous part of anchor chain in historical order; None in case of a gap
    pub fn anchor_chain(
        &self,
        last_proof_round: Option<Round>,
        trigger: &PointInfo,
    ) -> Option<VecDeque<EnqueuedAnchor>> {
        assert_eq!(
            trigger.anchor_link(AnchorStageRole::Trigger),
            &Link::ToSelf,
            "passed point is not a trigger: {:?}",
            trigger.id().alt()
        );

        if match last_proof_round {
            // anchor chain is not init yet, and exactly anchor trigger or proof is at the bottom -
            // dag cannot contain corresponding anchor candidate
            None => self.bottom_round() >= trigger.round().prev(),
            // some trigger (point future) from a later round resolved earlier than current one,
            // so this proof is already in chain with `direct_trigger: None`
            // this proof can even be the last element in chain, as those next trigger and proof
            // still wait for corresponding anchor point future to resolve
            Some(last_proof_round) => last_proof_round == trigger.round().prev(),
        } {
            return Some(VecDeque::new());
        }

        let last_proof_or_bottom_round = last_proof_round.unwrap_or_else(|| self.bottom_round());

        let mut rev_iter = self
            .rounds
            .range((
                Bound::Excluded(last_proof_or_bottom_round), // exclude used proof
                Bound::Excluded(trigger.round()),            // include topmost proof only
            ))
            .rev()
            .peekable();

        let mut lookup_proof_id = trigger
            .prev_id()
            .expect("validation broken: anchor trigger without prev point");

        let mut result = VecDeque::new();
        while rev_iter.peek().is_some() && lookup_proof_id.round > last_proof_or_bottom_round {
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
                        lookup_proof_id.author, stage.leader,
                        "validate() is broken: anchor proof author is not leader"
                    );
                    assert!(
                        !stage.is_used.load(atomic::Ordering::Relaxed),
                        "limit by round range is broken: visiting already committed proof {:?}",
                        lookup_proof_id.alt()
                    );
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
            .info;

            let Some(anchor_id) = proof.prev_id() else {
                panic!(
                    "verify() is broken: anchor proof without prev id; proof id {:?}",
                    proof.id()
                )
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
            .info;

            let mut direct_trigger = None;
            if proof.round() == trigger.round().prev() {
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

        let linked_to_proof_round = result.front()?.anchor.anchor_round(AnchorStageRole::Proof);
        if linked_to_proof_round <= last_proof_or_bottom_round {
            Some(result)
        } else {
            None
        }
    }

    /// returns globally available points in historical order;
    /// `None` is a signal to break whole assembled commit chain and retry later
    ///
    /// Note: at this point there is no way to check if passed point is really an anchor
    pub fn gather_uncommitted(
        &self,
        anchor: &PointInfo, // @ r+1
    ) -> Option<VecDeque<ValidPoint>> {
        fn extend(to: &mut BTreeMap<PeerId, Digest>, from: &BTreeMap<PeerId, Digest>) {
            if to.is_empty() {
                *to = from.clone();
            } else {
                for (peer, digest) in from {
                    to.insert(*peer, *digest);
                }
            }
        }
        let history_limit = Round(
            (anchor.round().0)
                .saturating_sub(MempoolConfig::COMMIT_DEPTH as _)
                // do not commit genesis - we may place some arbitrary payload in it
                .max(MempoolConfig::genesis_round().next().0),
        );

        let mut r = array::from_fn::<_, 3, _>(|_| BTreeMap::new()); // [r+0, r-1, r-2]
        extend(&mut r[0], &anchor.data().includes); // points @ r+0
        extend(&mut r[1], &anchor.data().witness); // points @ r-1

        let mut rng = rand_pcg::Pcg64::from_seed(*anchor.digest().inner());
        let mut uncommitted = VecDeque::new();

        let rev_iter = self
            .rounds
            .range((
                Bound::Included(history_limit),
                Bound::Excluded(anchor.round()),
            ))
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
            sorted.shuffle(&mut rng);
            for (node, digest) in &sorted {
                // Every point must be valid (we've validated anchor dependencies already),
                // but some points don't have previous one to proof as vertex.
                // Any equivocated point (except anchor) is ok, as they are globally available
                // because of anchor, and their payload is deduplicated after mempool anyway.
                let global = // point @ r+0; break and return `None` if not ready yet
                    Self::ready_valid_point(point_round, node, digest, "point")?;
                // select only uncommitted ones
                if !global.is_committed.load(atomic::Ordering::Relaxed) {
                    extend(&mut r[1], &global.info.data().includes); // points @ r-1
                    extend(&mut r[2], &global.info.data().witness); // points @ r-2
                    uncommitted.push_front(global);
                }
            }
            r.rotate_left(1); // [empty r_0, r-1, r-2] => [r-1 as r+0, r-2 as r-1, empty as r-2]
        }
        assert_eq!(
            next_round,
            history_limit,
            "{} doesn't contain full anchor history",
            self.alt(),
        );
        Some(uncommitted)
    }

    fn any_ready_valid_point(
        dag_round: &DagRound,
        author: &PeerId,
        // _string: &mut String,
    ) -> Option<ValidPoint> {
        dag_round
            .view(author, |loc| {
                loc.versions()
                    .iter()
                    // better try later than wait now if some point is still downloading
                    .filter_map(|(_digest, version)| {
                        version.clone().now_or_never()
                        // TODO log target for commit logic debug
                        // let b = if a.is_some() { "ready" } else { "noone" };
                        // string.push_str(&format!(
                        //     "{} @ {} # {} : {b};",
                        //     author.alt(),
                        //     dag_round.round().0,
                        //     digest.alt()
                        // ));
                        // a
                    })
                    // take any suitable
                    .find_map(move |dag_point| dag_point.into_valid())
            })
            .flatten()
    }

    // needed only in commit where all points are validated and stored in DAG
    /// returns only valid point (panics on invalid); `None` if not ready yet
    fn ready_valid_point(
        dag_round: &DagRound,
        author: &PeerId,
        digest: &Digest,
        point_kind: &'static str,
    ) -> Option<ValidPoint> {
        dag_round
            .view(author, |loc| {
                loc.versions().get(digest).cloned()
                // if a.is_none() {
                //     tracing::warn!(
                //         "!! NO DIGEST {} @ {} # {}",
                //         author.alt(),
                //         digest.alt(),
                //         dag_round.round().0
                //     );
                // };
                // a
            })
            // .expect("author")
            .and_then(identity) // flatten result
            .and_then(|fut| {
                fut.now_or_never()
                // if a.is_none() {
                //     tracing::warn!(
                //         "!! NEVER {} @ {} # {}",
                //         author.alt(),
                //         digest.alt(),
                //         dag_round.round().0
                //     );
                // };
                // a
            })
            .map(|dag_point| dag_point.into_valid().ok_or("is not valid"))
            .transpose()
            .unwrap_or_else(|msg| {
                let point_id = PointId {
                    author: *author,
                    round: dag_round.round(),
                    digest: *digest,
                };
                panic!("{point_kind} {msg}: {:?}", point_id.alt())
            })
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
            "DagBack len {} [{}..{}] ",
            this.rounds.len(),
            this.bottom_round().0,
            this.top().round().0,
        )
    }
}
