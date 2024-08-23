use std::collections::BTreeMap;
use std::convert::identity;
use std::sync::atomic::Ordering;
use std::{array, cmp, mem};

use futures_util::FutureExt;
use rand::prelude::SliceRandom;
use rand::SeedableRng;
use tycho_network::PeerId;

use crate::dag::anchor_stage::{AnchorStage, WAVE_ROUNDS};
use crate::dag::DagRound;
use crate::effects::{AltFormat, Effects, EngineContext};
use crate::engine::MempoolConfig;
use crate::intercom::PeerSchedule;
use crate::models::{AnchorStageRole, Digest, PointId, PointInfo, Round, ValidPoint};

#[derive(Default)]
pub struct Dag {
    // from the oldest to the current round; newer ones are in the future;
    rounds: BTreeMap<Round, DagRound>,
}

struct LinkedAnchor {
    anchor: ValidPoint,
    anchor_round: DagRound,
    proof_round: DagRound,
    trigger_round: Option<DagRound>,
}

impl Dag {
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
        if (top.round().0 + MempoolConfig::CACHE_AHEAD_ENGINE_ROUNDS as u32) < next_round.0 {
            tracing::warn!(
                parent: effects.span(),
                lag = next_round.0 - top.round().0,
                "need sync"
            );
            panic!("not implemented: sync")
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

    fn drop_tail_before_commit(&mut self, anchor_at: Round) {
        let tail = (anchor_at.0).saturating_sub(MempoolConfig::COMMIT_DEPTH as u32);
        self.rounds.retain(|k, _| k.0 >= tail);
    }

    fn drop_tail_after_commit(&mut self, anchor_at: Round) {
        let tail = (anchor_at.0)
            .saturating_add(WAVE_ROUNDS) // next anchor round
            .saturating_sub(MempoolConfig::COMMIT_DEPTH as u32);
        self.rounds.retain(|k, _| k.0 >= tail);
    }

    pub fn commit(&mut self) -> Vec<(PointInfo, Vec<PointInfo>)> {
        self.commit_up_to(self.top())
    }

    /// result is in historical order
    fn commit_up_to(&mut self, up_to: DagRound) -> Vec<(PointInfo, Vec<PointInfo>)> {
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

        let Some(mut oldest_proof_round) = self.oldest_proof_round() else {
            return ordered; // empty; nothing to commit yet
        };

        // take all ready triggers, skipping not ready ones
        let mut trigger_stack = Self::trigger_stack(up_to, oldest_proof_round);
        let _span = if let Some((latest_trigger, _)) = trigger_stack.first() {
            tracing::error_span!(
                "commit trigger",
                author = display(&latest_trigger.data().author.alt()),
                round = latest_trigger.round().0,
                digest = display(&latest_trigger.digest().alt()),
            )
            .entered()
        } else {
            return ordered; // empty
        };

        let mut anchors = BTreeMap::new(); // sorted and unique

        // traverse from oldest to newest;
        // ignore non-ready triggers as chain may be restored without them
        // if chain is broken - take the prefix until first gap
        while let Some((trigger, trigger_round)) = trigger_stack.pop() {
            let contiguous_part =
                Self::anchor_chain(&trigger, trigger_round.clone(), oldest_proof_round);
            match contiguous_part {
                None => break, // some dag point future is not yet resolved
                Some(chain) => {
                    for linked in chain {
                        // safety net: as rounds are traversed from oldest to newest,
                        // trigger can be met only at first time its candidate round is met;
                        // although triggers should not be overwritten, ensure with `entry` API
                        anchors.entry(linked.anchor.info.round()).or_insert(linked);
                    }
                }
            }
            if let Some(last_linked) = anchors.values().last() {
                // no reason to traverse deeper than last proof
                oldest_proof_round = last_linked.proof_round.round();
            }
        }

        for LinkedAnchor {
            anchor,
            anchor_round,
            proof_round,
            trigger_round,
        } in anchors.into_values()
        {
            // Note every next "little anchor candidate that could" must have at least full dag depth
            // in case previous anchor was triggered directly - rounds are already dropped
            self.drop_tail_before_commit(anchor_round.round());
            let Some(uncommitted_rev) = Self::gather_uncommitted_rev(&anchor.info, anchor_round)
            else {
                break; // will continue at the next call
            };
            match proof_round.anchor_stage() {
                Some(stage) if stage.role == AnchorStageRole::Proof => {
                    stage.is_used.store(true, Ordering::Relaxed);
                }
                _ => panic!("expected AnchorStage::Proof"),
            };
            // Note a proof may be marked as used while it is fired by a future tigger, which
            //   may be left unmarked at the current run until upcoming points become ready
            match trigger_round.as_ref().map(|tr| tr.anchor_stage()) {
                Some(Some(stage)) if stage.role == AnchorStageRole::Trigger => {
                    stage.is_used.store(true, Ordering::Relaxed);
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
                    valid.info
                })
                .collect::<Vec<_>>();
            ordered.push((anchor.info, committed));
        }
        if let Some((last_anchor, _)) = ordered.last() {
            // drop rounds that we'll never need again to free some memory
            self.drop_tail_after_commit(last_anchor.round());
        }
        ordered
    }

    /// to skip unusable anchor proofs and triggers; inclusive for proofs, exclusive for triggers
    fn oldest_proof_round(&self) -> Option<Round> {
        self.rounds
            .iter()
            .skip({
                let (first, _) = self.rounds.first_key_value().expect("DAG cannot be empty");
                // +1 to skip either proof round or a trigger right above the needed depth
                match MempoolConfig::GENESIS_ROUND.cmp(first) {
                    // cannot commit anchors with too shallow history
                    cmp::Ordering::Less => MempoolConfig::COMMIT_DEPTH as usize,
                    // commit right after genesis, do not wait for full depth
                    cmp::Ordering::Equal => 1,
                    cmp::Ordering::Greater => {
                        panic!("genesis round can be only at the beginning of DAG")
                    }
                }
            })
            .find_map(|(round, dag_round)| {
                dag_round
                    .anchor_stage()
                    .filter(|stage| {
                        stage.role == AnchorStageRole::Proof
                            && !stage.is_used.load(Ordering::Relaxed)
                    })
                    .map(|_| *round)
            })
    }

    /// not yet used commit triggers in reverse order (newest in front and oldest in back);
    /// use with `vec::pop()`
    fn trigger_stack(
        mut dag_round: DagRound,
        oldest_proof_round: Round,
    ) -> Vec<(PointInfo, DagRound)> {
        let mut latest_trigger = Vec::new();
        loop {
            let prev_dag_round = dag_round.prev().upgrade();

            if let Some(AnchorStage {
                role: AnchorStageRole::Trigger,
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
                    latest_trigger.push((valid.info, dag_round.clone()));
                };
            };

            match prev_dag_round {
                Some(prev_dag_round) if prev_dag_round.round() > oldest_proof_round => {
                    dag_round = prev_dag_round;
                }
                _ => break,
            };
        }
        latest_trigger
    }

    fn anchor_chain(
        trigger: &PointInfo,
        trigger_round: DagRound,
        oldest_proof_round: Round,
    ) -> Option<Vec<LinkedAnchor>> {
        assert_eq!(
            trigger.prev_id(),
            Some(trigger.anchor_id(AnchorStageRole::Proof)),
            "invalid anchor proof link, trigger point must have been invalidated"
        );
        assert_eq!(
            trigger.round(),
            trigger_round.round(),
            "trigger round does not match trigger point"
        );
        let mut proof_id = trigger
            .prev_id()
            .expect("validation broken: anchor trigger with empty proof field");
        let mut proof_round = trigger_round.prev().upgrade().expect("cannot be weak");
        let mut trigger_round = Some(trigger_round); // use only as a part of matching triplet
        let mut result_stack = Vec::new();
        loop {
            assert_eq!(
                proof_id.round,
                proof_round.round(),
                "anchor proof id round does not match"
            );
            let proof = Self::ready_valid_point(
                &proof_round,
                &proof_id.author,
                &proof_id.digest,
                "anchor proof",
            )?;
            assert_eq!(
                proof.info.round(),
                proof_round.round(),
                "anchor proof round does not match"
            );
            let Some(AnchorStage {
                role: AnchorStageRole::Proof,
                ref leader,
                ref is_used,
            }) = proof_round.anchor_stage()
            else {
                panic!("anchor proof round is not expected, validation is broken")
            };
            assert_eq!(
                proof.info.data().author,
                leader,
                "anchor proof author does not match prescribed by round"
            );
            if is_used.load(Ordering::Relaxed) {
                break Some(result_stack);
            };
            let anchor_digest = match proof.info.data().prev_digest.as_ref() {
                Some(anchor_digest) => anchor_digest,
                None => panic!("anchor proof must prove to anchor point, verify() is broken"),
            };
            let anchor_round = proof_round.prev().upgrade().expect("cannot be weak");
            let anchor = Self::ready_valid_point(&anchor_round, leader, anchor_digest, "anchor")?;

            proof_id = anchor.info.anchor_id(AnchorStageRole::Proof);
            let next_proof_round = anchor_round.scan(proof_id.round);

            let trigger_round =
                mem::take(&mut trigger_round).filter(|tr| proof_round.round() == tr.round().prev());
            result_stack.push(LinkedAnchor {
                anchor,
                anchor_round,
                proof_round,
                trigger_round,
            });

            match next_proof_round {
                Some(next_proof_round) if next_proof_round.round() >= oldest_proof_round => {
                    proof_round = next_proof_round;
                }
                _ => break Some(result_stack),
            };
        }
    }

    /// returns globally available points in reversed historical order;
    /// `None` is a signal to break whole assembled commit chain and retry later
    ///
    /// Note: at this point there is no way to check if passed point is really an anchor
    fn gather_uncommitted_rev(
        anchor: &PointInfo,          // @ r+1
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
            anchor.round(),
            "passed anchor round does not match anchor point's round"
        );
        let history_limit = Round(
            current_round
                .round()
                .0
                .saturating_sub(MempoolConfig::COMMIT_DEPTH as _)
                .max(MempoolConfig::GENESIS_ROUND.0),
        );

        let mut r = array::from_fn::<_, 3, _>(|_| BTreeMap::new()); // [r+0, r-1, r-2]
        extend(&mut r[0], &anchor.data().includes); // points @ r+0
        extend(&mut r[1], &anchor.data().witness); // points @ r-1

        let mut rng = rand_pcg::Pcg64::from_seed(*anchor.digest().inner());
        let mut uncommitted_rev = Vec::new();

        while let Some(point_round /* r+0 */) = current_round
            .prev()
            .upgrade()
            .filter(|dag_round| dag_round.round() >= history_limit)
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
                    Self::ready_valid_point(&point_round, node, digest, "point")?;
                // select only uncommitted ones
                if !global.is_committed.load(Ordering::Relaxed) {
                    extend(&mut r[1], &global.info.data().includes); // points @ r-1
                    extend(&mut r[2], &global.info.data().witness); // points @ r-2
                    uncommitted_rev.push(global);
                }
            }
            current_round = point_round; // r+0 is a new r+1
            r.rotate_left(1); // [empty r_0, r-1, r-2] => [r-1 as r+0, r-2 as r-1, empty as r-2]
        }
        assert_eq!(
            current_round.round(),
            history_limit,
            "dag doesn't contain full anchor history"
        );
        Some(uncommitted_rev)
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
            .view(author, |loc| loc.versions().get(digest).cloned())
            .and_then(identity) // flatten result
            .and_then(|fut| fut.now_or_never())
            .map(|dag_point| dag_point.into_valid().ok_or("is not valid"))
            .transpose()
            .unwrap_or_else(|msg| {
                let point_id = PointId {
                    author: *author,
                    round: dag_round.round(),
                    digest: digest.clone(),
                };
                panic!("{point_kind} {msg}: {:?}", point_id.alt())
            })
    }
}

#[cfg(test)]
mod test {
    use std::array;
    use std::io::Write;

    use everscale_crypto::ed25519::{KeyPair, SecretKey};
    use tycho_network::PeerId;
    use tycho_util::FastDashMap;

    use crate::dag::dag_location::DagLocation;
    use crate::dag::Dag;
    use crate::effects::{AltFormat, ChainedRoundsContext, Effects, EngineContext, MempoolStore};
    use crate::models::{AnchorStageRole, PointInfo, Round};
    use crate::test_utils;

    const PEER_COUNT: usize = 3;

    #[tokio::test]
    async fn test_commit_with_gap() {
        let stub_store = MempoolStore::no_read_stub();

        let genesis = test_utils::genesis();
        let peers: [(PeerId, KeyPair); PEER_COUNT] = array::from_fn(|i| {
            let keys = KeyPair::from(&SecretKey::from_bytes([i as u8; 32]));
            (PeerId::from(keys.public_key), keys)
        });

        let (mut dag, peer_schedule, stub_downloader) =
            test_utils::make_dag(&peers, &genesis, &stub_store);

        let effects = Effects::<ChainedRoundsContext>::new(genesis.round());

        for i in 1..=10 {
            let round = Round(i * 10);
            let effects = Effects::<EngineContext>::new(&effects, round);
            dag.fill_to_top(round, &peer_schedule, &effects);
        }

        println!("dag of {} rounds with {PEER_COUNT} peers", dag.rounds.len());

        test_utils::populate_dag(
            &peers,
            &peer_schedule,
            &stub_downloader,
            &stub_store,
            &effects,
            &genesis,
            0,
            0,
            &mut dag.rounds,
        )
        .await;

        println!("populated and validated");

        assert_eq!(commit(&mut dag, Some(Round(48))).len(), 11);

        let mut r_points = vec![];

        for i in 50..55 {
            r_points.push(remove_point(&mut dag, Round(i), &peers[1].0));
        }

        let r_leader = remove_leader(&mut dag, Round(62));

        let r_round = remove_round(&mut dag, Round(70));

        assert_eq!(commit(&mut dag, None).len(), 1);

        for pack in r_points {
            restore_point(&mut dag, pack);
        }

        assert_eq!(commit(&mut dag, None).len(), 2);

        restore_point(&mut dag, r_leader);

        assert_eq!(commit(&mut dag, None).len(), 2);

        restore_round(&mut dag, r_round);

        assert_eq!(commit(&mut dag, None).len(), 8);

        std::io::stderr().flush().ok();
        std::io::stdout().flush().ok();
    }

    fn remove_round(dag: &mut Dag, round: Round) -> (Round, FastDashMap<PeerId, DagLocation>) {
        let in_question = dag.rounds.get(&round).expect("in dag").clone();
        let removed = in_question.locations().clone();
        in_question.locations().clear();
        println!("removed {round:?}");
        (round, removed)
    }

    fn restore_round(dag: &mut Dag, pack: (Round, FastDashMap<PeerId, DagLocation>)) {
        let (round, removed) = pack;
        let in_question = dag.rounds.get(&round).expect("in dag").clone();
        println!("restored {round:?}");
        for (peer_id, loc) in removed {
            if in_question.locations().insert(peer_id, loc).is_some() {
                panic!("was not removed from dag: {} @ {round:?}", peer_id.alt());
            }
        }
    }

    fn remove_leader(dag: &mut Dag, round: Round) -> (Round, PeerId, DagLocation) {
        let in_question = dag.rounds.get(&round).expect("in dag").clone();
        let leader = match in_question.anchor_stage() {
            Some(stage) => {
                match stage.role {
                    AnchorStageRole::Trigger => println!("removed trigger @ {round:?}"),
                    AnchorStageRole::Proof => println!("removed proof @ {round:?}"),
                }
                stage.leader
            }
            None => panic!("no leader @ {round:?}"),
        };
        let (peer_id, loc) = in_question.locations().remove(&leader).expect("in dag");
        (round, peer_id, loc)
    }

    fn remove_point(dag: &mut Dag, round: Round, peer_id: &PeerId) -> (Round, PeerId, DagLocation) {
        let in_question = dag.rounds.get(&round).expect("in dag").clone();
        println!("removed point {} @ {round:?}", peer_id.alt());
        let (peer_id, loc) = in_question.locations().remove(peer_id).expect("in dag");
        (round, peer_id, loc)
    }

    fn restore_point(dag: &mut Dag, pack: (Round, PeerId, DagLocation)) {
        let (round, peer_id, loc) = pack;
        let in_question = dag.rounds.get(&round).expect("in dag").clone();
        match in_question.anchor_stage() {
            Some(stage) if stage.leader == peer_id => match stage.role {
                AnchorStageRole::Proof => println!("restored proof {} @ {round:?}", peer_id.alt()),
                AnchorStageRole::Trigger => {
                    println!("restored trigger {} @ {round:?}", peer_id.alt());
                }
            },
            _ => println!("restored point {} @ {round:?}", peer_id.alt()),
        };

        if in_question.locations().insert(peer_id, loc).is_some() {
            panic!("was not removed from dag: {} @ {round:?}", peer_id.alt());
        }
    }

    fn commit(dag: &mut Dag, up_to: Option<Round>) -> Vec<(PointInfo, Vec<PointInfo>)> {
        let committed = if let Some(up_to) = up_to {
            let up_to = dag.rounds.get(&up_to).unwrap().clone();
            dag.commit_up_to(up_to)
        } else {
            dag.commit()
        };
        for (anchor, _) in &committed {
            println!("anchor {:?}", anchor.id().alt());
        }
        if let Some(up_to) = up_to {
            println!("committed up to {:?}", up_to);
        } else {
            println!("committed");
        };
        committed
    }
}
