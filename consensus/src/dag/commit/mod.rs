mod anchor_chain;
mod back;

use std::sync::atomic::Ordering;

use tycho_util::metrics::HistogramGuard;

use crate::dag::commit::anchor_chain::{AnchorChain, EnqueuedAnchor};
use crate::dag::commit::back::DagBack;
use crate::dag::DagRound;
use crate::effects::{AltFmt, AltFormat};
use crate::engine::MempoolConfig;
use crate::models::{AnchorData, AnchorStageRole, Round};

enum SyncError {
    TryLater,
    Impossible(Round),
}

pub struct Committer {
    dag: DagBack,
    // from the oldest to the current round; newer ones are in the future;
    anchor_chain: AnchorChain,
    // some anchors won't contain full history after a gap (filled with sync),
    // so this determines least round at which fully reproducible anchor may be produced
    full_history_bottom: Round,
}

impl Default for Committer {
    fn default() -> Self {
        Self {
            dag: Default::default(),
            anchor_chain: Default::default(),
            full_history_bottom: Round::BOTTOM,
        }
    }
}

impl Committer {
    pub fn init(&mut self, bottom_round: &DagRound, conf: &MempoolConfig) -> Round {
        assert_eq!(
            self.full_history_bottom,
            Round::BOTTOM,
            "already initialized"
        );
        self.dag.init(bottom_round);
        self.full_history_bottom = bottom_round.round() + conf.consensus.commit_history_rounds;
        self.full_history_bottom // hidden in other cases
    }

    pub fn bottom_round(&self) -> Round {
        self.dag.bottom_round()
    }

    pub fn dag_len(&self) -> usize {
        self.dag.len()
    }

    /// returns new bottom after gap if it was moved, and `None` if no gap occurred
    pub fn extend_from_ahead(&mut self, rounds: &[DagRound]) {
        self.dag.extend_from_front(rounds);
    }

    /// returns new full history bottom
    /// as `Ok` if successfully dropped all given range, otherwise as `Err`
    pub fn drop_upto(
        &mut self,
        new_bottom_round: Round,
        conf: &MempoolConfig,
    ) -> Result<Round, Round> {
        // cannot leave dag empty
        let actual_bottom = new_bottom_round.min(self.dag.top().round());
        self.dag.drop_upto(actual_bottom);
        self.anchor_chain.drop_upto(actual_bottom);
        self.full_history_bottom = actual_bottom + conf.consensus.commit_history_rounds;
        if actual_bottom == new_bottom_round {
            Ok(self.full_history_bottom)
        } else {
            Err(self.full_history_bottom)
        }
    }

    pub fn commit(&mut self, conf: &MempoolConfig) -> Result<Vec<AnchorData>, Round> {
        // may run for long several times in a row and commit nothing, because of missed points
        let _guard = HistogramGuard::begin("tycho_mempool_engine_commit_time");

        // Note that it's always engine round in production, but may differ in local tests
        let current_round = self.dag.top().round().prev();

        self.commit_up_to(current_round, conf)
    }

    fn commit_up_to(
        &mut self,
        current_round: Round,
        conf: &MempoolConfig,
    ) -> Result<Vec<AnchorData>, Round> {
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

        self.enqueue_new_anchors(current_round)?;

        let mut committed = Vec::new();

        while let Some(next) = self.anchor_chain.next() {
            metrics::gauge!("tycho_mempool_rounds_engine_ahead_proof_chain")
                .set(current_round - next.proof.round());

            let _span = tracing::error_span!(
                "anchor",
                author = display(&next.anchor.data().author.alt()),
                round = next.anchor.round().0,
                digest = display(&next.anchor.digest().alt()),
                proof = display(&next.proof.digest().alt()),
                trigger = (next.direct_trigger.as_ref()).map(|p| display(p.digest().alt())),
            )
            .entered();

            match self.dequeue_anchor(next, conf) {
                Ok(data) => committed.push(data),
                Err(SyncError::TryLater) => break,
                Err(SyncError::Impossible(round)) => return Err(round),
            }
        }
        Ok(committed)
    }

    fn enqueue_new_anchors(&mut self, current_round: Round) -> Result<(), Round> {
        // some state may have restored from db or resolved from download

        // take all ready triggers, skipping not ready ones
        let triggers = self.dag.triggers(
            self.anchor_chain
                .top_proof_round()
                .unwrap_or(self.dag.bottom_round()),
            current_round,
        )?;

        if let Some(last_trigger) = triggers.back() {
            metrics::gauge!("tycho_mempool_rounds_engine_ahead_last_trigger")
                .set(current_round - last_trigger.round());
        }

        // traverse from oldest to newest;
        // ignore non-ready triggers as chain may be restored without them
        // if chain is broken - take the prefix until first gap

        for trigger in triggers {
            let last_proof_round = match self.anchor_chain.top_proof_round() {
                Some(top_proof_round) => top_proof_round,
                None => match self.dag.last_unusable_proof_round(&trigger) {
                    // init chain after each gap
                    Ok(last_unusable_proof_round) => last_unusable_proof_round,
                    Err(SyncError::TryLater) => return Ok(()), // cannot init
                    Err(SyncError::Impossible(round)) => return Err(round),
                },
            };
            let chain_part = match self.dag.anchor_chain(last_proof_round, &trigger) {
                Ok(chain_part) => chain_part,
                Err(SyncError::TryLater) => break, // some dag point future is not yet resolved
                Err(SyncError::Impossible(round)) => return Err(round),
            };
            for next in chain_part {
                if let Some(back) = self.anchor_chain.top() {
                    assert_eq!(
                        next.anchor.anchor_round(AnchorStageRole::Proof),
                        back.proof.round(),
                        "chain part is not contiguous by rounds"
                    );
                }
                self.anchor_chain.enqueue(next);
            }
        }
        Ok(())
    }

    fn dequeue_anchor(
        &mut self,
        next: EnqueuedAnchor,
        conf: &MempoolConfig,
    ) -> Result<AnchorData, SyncError> {
        // in case previous anchor was triggered directly - rounds are already dropped
        self.dag
            .drop_upto(next.anchor.round() - conf.consensus.commit_history_rounds);
        let uncommitted =
            match (self.dag).gather_uncommitted(self.full_history_bottom, &next.anchor, conf) {
                Ok(uncommitted) => uncommitted,
                Err(sync_error) => {
                    self.anchor_chain.undo_next(next);
                    return Err(sync_error);
                }
            };

        match self
            .dag
            .get(next.proof.round())
            .and_then(|r| r.anchor_stage())
        {
            Some(stage) if stage.role == AnchorStageRole::Proof => {
                stage.is_used.store(true, Ordering::Relaxed);
            }
            _ => panic!("expected AnchorStage::Proof"),
        };

        // Note a proof may be marked as used while it is fired by a future tigger, which
        //   may be left unmarked at the current run until upcoming points become ready
        match next
            .direct_trigger
            .as_ref()
            .map(|tr| self.dag.get(tr.round()).and_then(|r| r.anchor_stage()))
        {
            Some(Some(stage)) if stage.role == AnchorStageRole::Trigger => {
                stage.is_used.store(true, Ordering::Relaxed);
            }
            Some(_) => panic!("expected AnchorStage::Trigger"),
            None => {} // anchor triplet without direct trigger (not ready/valid/exists)
        };

        // Note every iteration marks committed points before next uncommitted are gathered
        let committed = uncommitted
            .into_iter()
            .map(|valid| {
                valid.is_committed().store(true, Ordering::Relaxed);
                valid.info().clone()
            })
            .collect::<Vec<_>>();
        Ok(AnchorData {
            prev_anchor: Some(next.anchor.anchor_round(AnchorStageRole::Proof))
                .filter(|r| *r > conf.genesis_round)
                .map(|r| r.prev()),
            anchor: next.anchor,
            history: committed,
        })
    }
}

impl AltFormat for Committer {}
impl std::fmt::Debug for AltFmt<'_, Committer> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = AltFormat::unpack(self);
        write!(
            f,
            "{:?} chain {:?} full history bottom {}",
            inner.dag.alt(),
            inner.anchor_chain.alt(),
            inner.full_history_bottom.0,
        )
    }
}
impl std::fmt::Display for AltFmt<'_, Committer> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = AltFormat::unpack(self);
        write!(
            f,
            "{} chain {} full history bottom {}",
            inner.dag.alt(),
            inner.anchor_chain.alt(),
            inner.full_history_bottom.0,
        )
    }
}

#[cfg(test)]
mod test {
    use std::array;
    use std::io::Write;
    use std::sync::Arc;

    use everscale_crypto::ed25519::{KeyPair, SecretKey};
    use tycho_network::PeerId;
    use tycho_util::FastDashMap;

    use super::*;
    use crate::dag::dag_location::DagLocation;
    use crate::dag::DagFront;
    use crate::effects::{AltFormat, Ctx, MempoolStore, RoundCtx};
    use crate::models::{AnchorData, AnchorStageRole, Round};
    use crate::test_utils;

    const PEER_COUNT: usize = 3;

    #[tokio::test]
    async fn test_commit_with_gap() {
        let stub_store = MempoolStore::no_read_stub();

        let peers: [(PeerId, Arc<KeyPair>); PEER_COUNT] = array::from_fn(|i| {
            let keys = KeyPair::from(&SecretKey::from_bytes([i as u8; 32]));
            (PeerId::from(keys.public_key), Arc::new(keys))
        });
        let local_keys = &peers[0].1;

        let (peer_schedule, stub_downloader, genesis, engine_ctx) =
            test_utils::make_engine_parts(&peers, local_keys.clone());
        let conf = engine_ctx.conf();

        let mut round_ctx = RoundCtx::new(&engine_ctx, conf.genesis_round);

        let genesis_round = DagRound::new_bottom(conf.genesis_round, &peer_schedule, conf);
        genesis_round
            .add_local(&genesis, Some(local_keys), &stub_store, &round_ctx)
            .await
            .expect("cannot be closed");

        let mut dag = DagFront::default();
        let mut committer = dag.init(genesis_round, conf);

        for round in (0..100).map(Round) {
            // println!("{}", round.0);

            if round <= conf.genesis_round {
                continue;
            }
            round_ctx = RoundCtx::new(&engine_ctx, round);

            if let Some(skip_to) =
                dag.fill_to_top(round, Some(&mut committer), &peer_schedule, &round_ctx)
            {
                println!("gap: next anchor with full history not earlier than {skip_to:?}");
            };

            // println!("front {:?}", dag.alt());
            // println!("back {:?}", committer.alt());

            test_utils::populate_points(
                dag.top(),
                &peers,
                local_keys,
                &peer_schedule,
                &stub_downloader,
                &stub_store,
                &round_ctx,
                0,
                0,
            )
            .await;

            if round.0 == 33 {
                assert_eq!(commit(&mut committer, Some(Round(48)), conf).len(), 7);
            }
            if round.0 == 66 {
                assert_eq!(commit(&mut committer, Some(Round(48)), conf).len(), 4);
            }
        }

        println!(
            "{} with {PEER_COUNT} peers populated and validated",
            committer.dag.alt()
        );

        assert_eq!(
            committer.dag.bottom_round(),
            Round(25),
            "test config changed? should update test then"
        );

        let mut r_points = vec![];

        for i in 50..55 {
            r_points.push(remove_point(&mut committer.dag, Round(i), &peers[1].0));
        }

        let r_leader = remove_leader(&mut committer.dag, Round(62));

        let r_round = remove_round(&mut committer.dag, Round(70));

        assert_eq!(commit(&mut committer, None, conf).len(), 1);

        for pack in r_points {
            restore_point(&mut committer.dag, pack);
        }

        assert_eq!(commit(&mut committer, None, conf).len(), 2);

        restore_point(&mut committer.dag, r_leader);

        assert_eq!(commit(&mut committer, None, conf).len(), 2);

        restore_round(&mut committer.dag, r_round);

        assert_eq!(commit(&mut committer, None, conf).len(), 7);

        std::io::stderr().flush().ok();
        std::io::stdout().flush().ok();
    }

    fn remove_round(dag: &mut DagBack, round: Round) -> (Round, FastDashMap<PeerId, DagLocation>) {
        let in_question = dag.get(round).expect("in dag").clone();
        let removed = in_question.locations().clone();
        in_question.locations().clear();
        println!("removed {round:?}");
        (round, removed)
    }

    fn restore_round(dag: &mut DagBack, pack: (Round, FastDashMap<PeerId, DagLocation>)) {
        let (round, removed) = pack;
        let in_question = dag.get(round).expect("in dag").clone();
        println!("restored {round:?}");
        for (peer_id, loc) in removed {
            if in_question.locations().insert(peer_id, loc).is_some() {
                panic!("was not removed from dag: {} @ {round:?}", peer_id.alt());
            }
        }
    }

    fn remove_leader(dag: &mut DagBack, round: Round) -> (Round, PeerId, DagLocation) {
        let in_question = dag.get(round).expect("in dag").clone();
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

    fn remove_point(
        dag: &mut DagBack,
        round: Round,
        peer_id: &PeerId,
    ) -> (Round, PeerId, DagLocation) {
        let in_question = dag.get(round).expect("in dag").clone();
        match in_question.anchor_stage() {
            Some(stage) if stage.leader == peer_id => match stage.role {
                AnchorStageRole::Trigger => println!("removed trigger @ {round:?}"),
                AnchorStageRole::Proof => println!("removed proof @ {round:?}"),
            },
            _ => println!("removed point {} @ {round:?}", peer_id.alt()),
        };
        let (peer_id, loc) = in_question.locations().remove(peer_id).expect("in dag");
        (round, peer_id, loc)
    }

    fn restore_point(dag: &mut DagBack, pack: (Round, PeerId, DagLocation)) {
        let (round, peer_id, loc) = pack;
        let in_question = dag.get(round).expect("in dag").clone();
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

    fn commit(
        committer: &mut Committer,
        up_to: Option<Round>,
        conf: &MempoolConfig,
    ) -> Vec<AnchorData> {
        let committed = if let Some(up_to) = up_to {
            committer
                .commit_up_to(up_to, conf)
                .expect("no certified NotFound or Invalid points")
        } else {
            committer
                .commit(conf)
                .expect("no certified NotFound or Invalid points")
        };
        for data in &committed {
            println!("anchor {:?}", data.anchor.id().alt());
        }
        if let Some(up_to) = up_to {
            println!("committed up to {:?}", up_to);
        } else {
            println!("committed");
        };
        committed
    }
}
