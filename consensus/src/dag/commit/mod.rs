mod back;
mod inspector;

use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use tokio::sync::mpsc;
use tycho_network::PeerId;
use tycho_util::FastHashMap;

use crate::dag::DagRound;
use crate::dag::commit::back::DagBack;
use crate::dag::commit::inspector::RoundInspector;
use crate::dag::dag_point_future::WeakDagPointFuture;
use crate::effects::{AltFmt, AltFormat, Cancelled, TaskResult};
use crate::engine::{EngineResult, MempoolConfig};
use crate::intercom::StatsRanges;
use crate::models::{
    AnchorData, AnyLink, DagPoint, MempoolPeerStats, PointInfo, Round, ValidPoint,
};
use crate::moderator::JournalDagEvent;

#[derive(thiserror::Error, Debug)]
#[error("Committer encountered local history conflict at round {}", .0.0)]
pub struct HistoryConflict(pub Round);

struct EnqueuedAnchor {
    pub anchor: PointInfo,
    pub proof: PointInfo,
    pub prev_proof_round: Round,
    pub direct_trigger: Option<PointInfo>,
}

pub struct Committer {
    triggers_rx: mpsc::UnboundedReceiver<WeakDagPointFuture>,
    futures: FuturesUnordered<WeakDagPointFuture>,
    dag: DagBack,
    // some anchors won't contain full history after a gap (filled with sync),
    // so this determines least round at which fully reproducible anchor may be produced
    full_history_bottom: Round,
    /// `None` for disarmed, `false` for non-executable and `true` for executable
    emit_first_after_gap: Option<bool>,
    pub first_executable: Round,
    inspector: RoundInspector,
}

impl Committer {
    pub fn new(triggers_rx: mpsc::UnboundedReceiver<WeakDagPointFuture>) -> Self {
        Self {
            triggers_rx,
            futures: FuturesUnordered::new(),
            dag: Default::default(),
            full_history_bottom: Round::BOTTOM,
            emit_first_after_gap: None,
            first_executable: Round::BOTTOM,
            inspector: Default::default(),
        }
    }

    pub fn reset(&mut self) {
        let old_dag = std::mem::take(&mut self.dag);
        tycho_util::mem::Reclaimer::instance().drop(old_dag);

        self.full_history_bottom = Round::BOTTOM;
        self.emit_first_after_gap = None;
        self.first_executable = Round::BOTTOM;
    }

    /// `true` for a path same as mempool restart: commit offset anchors for `Deduplicator` state
    pub fn init(&mut self, bottom_round: &DagRound, is_after_gap: bool, conf: &MempoolConfig) {
        assert_eq!(
            self.full_history_bottom,
            Round::BOTTOM,
            "already initialized"
        );
        self.dag.init(bottom_round);
        self.full_history_bottom = if bottom_round.round()
            <= conf.genesis_round + conf.consensus.commit_history_rounds.get()
        {
            bottom_round.round()
        } else {
            bottom_round.round() + conf.consensus.commit_history_rounds.get()
        };
        self.emit_first_after_gap = is_after_gap.then_some(false);
        self.first_executable = if is_after_gap {
            self.full_history_bottom + conf.consensus.deduplicate_rounds
        } else {
            bottom_round.round()
        }
    }

    pub async fn next_trigger(&mut self) -> TaskResult<DagPoint> {
        loop {
            tokio::select! {
                biased;
                Some(future) = self.triggers_rx.recv() => self.futures.push(future),
                Some(trigger_opt) = self.futures.next() => if let Some(trigger) = trigger_opt? {
                    break Ok(trigger);
                },
                else => return Err(Cancelled()),
            }
        }
    }

    pub fn top_round(&self) -> Round {
        self.dag.top().round()
    }

    pub fn bottom_round(&self) -> Round {
        self.dag.bottom_round()
    }

    pub fn dag_len(&self) -> usize {
        self.dag.len()
    }

    pub fn is_once_after_gap(&mut self) -> bool {
        if self.emit_first_after_gap == Some(false) {
            self.emit_first_after_gap = Some(true);
            true
        } else {
            false
        }
    }

    /// returns new bottom after gap if it was moved, and `None` if no gap occurred
    pub fn extend_from_ahead(&mut self, rounds: &[DagRound]) {
        self.dag.extend_from_front(rounds);
    }

    /// returns `true` if successfully dropped all given range
    pub fn drop_upto(&mut self, new_bottom_round: Round, conf: &MempoolConfig) -> bool {
        // cannot leave dag empty
        let actual_bottom = new_bottom_round.min(self.dag.top().round());
        let drained = (self.dag)
            .drain_upto(actual_bottom)
            .into_iter()
            .filter_map(DagRound::into_inner) // make it inaccessible to scan
            .collect::<Vec<_>>();
        if !drained.is_empty() {
            tycho_util::mem::Reclaimer::instance().drop(drained);
        }
        if (self.dag.last_committed_proof).is_some_and(|proof| proof < actual_bottom) {
            self.dag.last_committed_proof = None;
        }

        self.full_history_bottom = actual_bottom + conf.consensus.commit_history_rounds.get();
        self.emit_first_after_gap = Some(false);
        self.first_executable = self.full_history_bottom + conf.consensus.deduplicate_rounds;
        actual_bottom == new_bottom_round
    }

    pub fn remove_committed(
        &mut self,
        anchor_round: Round,
        stats_ranges: &StatsRanges,
        conf: &MempoolConfig,
    ) -> TaskResult<(FastHashMap<PeerId, MempoolPeerStats>, Vec<JournalDagEvent>)> {
        // in case previous anchor was triggered directly - rounds are already dropped
        let drained =
            (self.dag).drain_upto(anchor_round - conf.consensus.commit_history_rounds.get());
        let last_non_executable = self.first_executable.prev().max(conf.genesis_round);
        let stats_since = stats_ranges.stats_since(anchor_round, conf);
        for r_0 in &drained {
            if r_0.round() >= last_non_executable {
                self.inspector.inspect(r_0)?;
            }
            if r_0.round() == last_non_executable {
                self.inspector.take_stats(); // was used only to refill state
                self.inspector.take_events();
            }
            if stats_since.is_none_or(|stats_since| r_0.round() < stats_since) {
                self.inspector.take_stats(); // suppression: drop every unsuitable
            }
        }
        let drained = drained
            .into_iter()
            .filter_map(DagRound::into_inner) // make it inaccessible to scan
            .collect::<Vec<_>>();
        if !drained.is_empty() {
            tycho_util::mem::Reclaimer::instance().drop(drained);
        }
        let stats = self.inspector.take_stats();
        let events = self.inspector.take_events();
        Ok((stats, events))
    }

    pub fn commit(
        &mut self,
        trigger: &DagPoint,
        conf: &MempoolConfig,
    ) -> EngineResult<Vec<AnchorData>> {
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

        let trigger = match filter(trigger) {
            Some(Ok(valid)) if valid.info().round() >= self.dag.bottom_round() => valid.info(),
            Some(Err(HistoryConflict(round))) if round >= self.dag.bottom_round() => {
                return Err(HistoryConflict(round).into());
            }
            _ => return Ok(Vec::new()),
        };

        assert_eq!(
            trigger.anchor_trigger(),
            AnyLink::ToSelf,
            "passed point is not a trigger: {:?}",
            trigger.id().alt()
        );
        if trigger.round() == conf.genesis_round {
            return Ok(Vec::new());
        }

        let current_round = self.dag.top().round().prev();

        let chain_part = self.dag.anchor_chain(trigger)?;

        metrics::gauge!("tycho_mempool_rounds_engine_ahead_last_trigger")
            .set(current_round.diff_f64(trigger.round()));

        let mut committed = Vec::with_capacity(chain_part.len());

        for next in chain_part {
            metrics::gauge!("tycho_mempool_rounds_engine_ahead_proof_chain")
                .set(current_round.diff_f64(next.proof.round()));

            let _span = tracing::error_span!(
                "anchor",
                author = display(next.anchor.author().alt()),
                round = next.anchor.round().0,
                digest = display(next.anchor.digest().alt()),
                proof = display(next.proof.digest().alt()),
                trigger = (next.direct_trigger.as_ref()).map(|p| display(p.digest().alt())),
            )
            .entered();

            committed.push(self.dequeue_anchor(next, conf)?);
        }
        Ok(committed)
    }

    fn dequeue_anchor(
        &mut self,
        next: EnqueuedAnchor,
        conf: &MempoolConfig,
    ) -> EngineResult<AnchorData> {
        let uncommitted =
            (self.dag).gather_uncommitted(self.full_history_bottom, &next.anchor, conf)?;

        assert!(
            self.dag.last_committed_proof < Some(next.proof.round()),
            "committed rounds must be in order: last committed proof: {:?}, next proof: {}",
            self.dag.last_committed_proof,
            next.proof.round().0,
        );
        self.dag.last_committed_proof = Some(next.proof.round());

        match self.dag.get(next.proof.round()) {
            Some(dag_round) => dag_round
                .used_anchor_proof()
                .set(*next.anchor.author())
                .expect("anchor proof slot already used"),
            _ => panic!("expected anchor proof stage"),
        };

        // Note every iteration marks committed points before next uncommitted are gathered
        let committed = uncommitted
            .into_iter()
            .map(|committable| {
                committable.set_committed();
                committable.info().clone()
            })
            .collect::<Vec<_>>();

        let is_executable = next.anchor.round() >= self.first_executable;

        let is_first_executable = match self.emit_first_after_gap {
            Some(false) => panic!("gap flag was not consumed before commit"),
            Some(true) if is_executable => {
                self.emit_first_after_gap = None;
                true
            }
            _ => false,
        };

        Ok(AnchorData {
            // both first executable anchor and one right after genesis
            // don't have link to previous anchor; others do
            prev_anchor: Some(next.prev_proof_round.prev())
                .filter(|r| *r > conf.genesis_round && !is_first_executable),
            anchor: next.anchor,
            proof_key: next.proof.key(),
            history: committed,
            is_executable,
        })
    }
}

pub(super) fn filter(trigger: &DagPoint) -> Option<Result<&ValidPoint, HistoryConflict>> {
    match trigger {
        DagPoint::Valid(valid) => {
            (valid.info().anchor_trigger() == AnyLink::ToSelf).then_some(Ok(valid))
        }
        DagPoint::TransInvalid(invalid) => {
            (invalid.has_proof()).then_some(Err(HistoryConflict(invalid.info().round())))
        }
        not_valid => (not_valid.is_certified()).then_some(Err(HistoryConflict(not_valid.round()))),
    }
}

impl AltFormat for Committer {}
impl std::fmt::Debug for AltFmt<'_, Committer> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = AltFormat::unpack(self);
        write!(
            f,
            "{:?} full history bottom {}",
            inner.dag.alt(),
            inner.full_history_bottom.0,
        )
    }
}
impl std::fmt::Display for AltFmt<'_, Committer> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = AltFormat::unpack(self);
        write!(
            f,
            "{} full history bottom {}",
            inner.dag.alt(),
            inner.full_history_bottom.0,
        )
    }
}

#[cfg(test)]
mod test {
    use std::array;
    use std::num::NonZeroUsize;
    use std::sync::Arc;

    use tycho_crypto::ed25519::{KeyPair, SecretKey};
    use tycho_network::PeerId;

    use super::*;
    use crate::dag::DagFront;
    use crate::effects::{AltFormat, Ctx, RoundCtx};
    use crate::engine::InputBuffer;
    use crate::engine::lifecycle::FixHistoryFlag;
    use crate::models::{AnchorData, Round};
    use crate::storage::MempoolStore;
    use crate::test_utils;

    const PEER_COUNT: usize = 3;

    #[tokio::test]
    async fn test_commit_with_gap() {
        test_impl(0, Round(25), [9, 5, 17], Round(97)).await;

        test_impl(1, Round(26), [8, 6, 16], Round(95)).await;
        test_impl(2, Round(26), [12, 8, 25], Round(96)).await;
        test_impl(3, Round(26), [12, 6, 22], Round(94)).await;

        test_impl(4, Round(26), [15, 7, 28], Round(95)).await; // N mod 3 == 1
        test_impl(5, Round(26), [17, 9, 34], Round(96)).await; // N mod 3 == 2 is optimal
        test_impl(6, Round(26), [14, 9, 31], Round(98)).await; // N mod 3 == 0

        test_impl(7, Round(26), [16, 10, 35], Round(98)).await;
        test_impl(8, Round(26), [18, 11, 39], Round(98)).await;
        test_impl(9, Round(26), [18, 10, 32], Round(94)).await;

        test_impl(10, Round(26), [19, 11, 36], Round(95)).await;
        test_impl(11, Round(26), [20, 12, 40], Round(96)).await;
        test_impl(12, Round(26), [18, 10, 37], Round(94)).await;
    }

    async fn test_impl(
        sticky_anchors: u8,
        dag_bottom: Round,
        committed: [usize; 3],
        last_proof: Round,
    ) {
        let stub_store = MempoolStore::no_read_stub();

        let peers: [(PeerId, Arc<KeyPair>); PEER_COUNT] = array::from_fn(|i| {
            let keys = KeyPair::from(&SecretKey::from_bytes([i as u8; 32]));
            (PeerId::from(keys.public_key), Arc::new(keys))
        });
        let local_keys = &peers[0].1;

        let mut merged_conf = test_utils::default_test_config();
        merged_conf.conf.consensus.sticky_anchors = sticky_anchors;

        let (peer_schedule, stub_downloader, genesis, engine_ctx) =
            test_utils::make_engine_parts(&merged_conf, &peers, local_keys.clone());
        let conf = engine_ctx.conf();

        let input_buffer = InputBuffer::new_stub(0, NonZeroUsize::MIN, &conf.consensus);

        let mut round_ctx = RoundCtx::new(&engine_ctx, conf.genesis_round);

        let (mut dag, mut committer) = DagFront::new(
            genesis.info().id(),
            FixHistoryFlag(false),
            &peer_schedule,
            conf,
        );
        dag.top()
            .add_local(
                &genesis,
                Some(local_keys.clone()),
                stub_downloader.clone(),
                stub_store.clone(),
                &round_ctx,
            )
            .await
            .expect("cannot be closed");

        let stats_ranges = peer_schedule.atomic().stats_ranges();

        let commit =
            async |committer: &mut Committer| _commit(committer, &stats_ranges, conf).await;

        for round in (conf.genesis_round.next().0..100).map(Round) {
            // println!(
            //     "{round:?} back {}..={} front {}..={}  {:?} \n\n",
            //     committer.bottom_round().0,
            //     committer.top_round().0,
            //     dag.bottom_round().0,
            //     dag.top().round().0,
            //     dag.top().alt()
            // );

            round_ctx = RoundCtx::new(&engine_ctx, round);

            dag.fill_to_top(round, &peer_schedule, &round_ctx);
            dag.sync_back(&mut committer, &round_ctx);

            if committer.emit_first_after_gap == Some(false) {
                let skip_to = committer.first_executable;
                assert_eq!(
                    skip_to,
                    committer.bottom_round()
                        + conf.consensus.deduplicate_rounds
                        + conf.consensus.max_consensus_lag_rounds.get(),
                    "must define offset for collator history"
                );
                println!("gap @ {round:?}: next anchor with full history at least @ {skip_to:?}");
                assert_eq!(
                    committer.dag.last_committed_proof, None,
                    "last proof must be empty after a gap"
                );
                committer.emit_first_after_gap = None; // just disarm the flag though its internal
            };

            // println!("front {:?}", dag.alt());
            // println!("back {:?}", committer.alt());

            test_utils::populate_points(
                dag.top(),
                &peers,
                local_keys,
                &peer_schedule.atomic().clone(),
                &stub_downloader,
                &stub_store,
                &round_ctx,
                &input_buffer,
            )
            .await;

            if round.0 == 33 {
                assert_eq!(commit(&mut committer).await.len(), committed[0]);
            }
            if round.0 == 48 {
                assert_eq!(commit(&mut committer).await.len(), committed[1]);
            }
        }

        println!(
            "{} with {PEER_COUNT} peers populated and validated",
            committer.dag.alt()
        );

        assert_eq!(
            committer.dag.bottom_round(),
            dag_bottom,
            "test config changed? should update test then"
        );

        assert_eq!(commit(&mut committer).await.len(), committed[2]);

        assert_eq!(
            committer.dag.last_committed_proof,
            Some(last_proof),
            "last proof must be set after commit"
        );

        // `drop_upto()` must keep `last_committed_proof` while it is in dag

        assert!(committer.drop_upto(last_proof, conf));
        assert_eq!(committer.dag.last_committed_proof, Some(last_proof));

        assert!(committer.drop_upto(last_proof.next(), conf));
        assert_eq!(committer.dag.last_committed_proof, None);
    }

    async fn _commit(
        committer: &mut Committer,
        stats_ranges: &StatsRanges,
        conf: &MempoolConfig,
    ) -> Vec<AnchorData> {
        let mut committed = Vec::new();

        while let Ok(trigger) = committer.triggers_rx.try_recv() {
            let trigger = trigger.await.expect("trigger").expect("strong ref");
            let trigger_round = trigger.round();
            let batch = committer.commit(&trigger, conf).unwrap_or_else(|err| {
                panic!("commit on trigger @ {trigger_round:?} failed: {err}")
            });
            // let trigger = trigger.valid().expect("cannot commit non-valid").info();
            // for data in &batch {
            //     println!(
            //         "commit anchor {} @ {:?} sticky {:?}",
            //         data.anchor.author().alt(),
            //         data.anchor.round(),
            //         trigger.sticky_anchors(),
            //     );
            // }
            committed.extend(batch);
        }

        if let Some(last) = committed.last() {
            committer
                .remove_committed(last.anchor.round(), stats_ranges, conf)
                .expect("no task can be cancelled");
        }
        committed
    }
}
