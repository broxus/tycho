use std::ops::Neg;
use std::sync::Arc;
use std::time::{Duration, Instant};

use everscale_crypto::ed25519::KeyPair;
use futures_util::future::BoxFuture;
use futures_util::stream::FuturesUnordered;
use futures_util::{future, FutureExt};
use itertools::Itertools;
use tokio::sync::{mpsc, watch};
use tracing::Instrument;
use tycho_network::{Network, OverlayId, OverlayService, PeerId, PeerResolver, PrivateOverlay};
use tycho_storage::MempoolStorage;
use tycho_util::metrics::HistogramGuard;

use crate::dag::{Dag, DagRound, InclusionState, Verifier};
use crate::effects::{AltFormat, ChainedRoundsContext, Effects, EngineContext, MempoolStore};
use crate::engine::input_buffer::InputBuffer;
use crate::engine::outer_round::{Collator, Commit, Consensus, OuterRound};
use crate::engine::round_task::RoundTaskReady;
use crate::engine::MempoolConfig;
use crate::intercom::{CollectorSignal, Dispatcher, PeerSchedule, Responder};
use crate::models::{Point, PointInfo, Round, UnixTime};

pub struct Engine {
    dag: Dag,
    committed: mpsc::UnboundedSender<(PointInfo, Vec<Point>)>,
    consensus_round: OuterRound<Consensus>,
    commit_round: OuterRound<Commit>,
    round_task: RoundTaskReady,
    effects: Effects<ChainedRoundsContext>,
}

impl Engine {
    const PRIVATE_OVERLAY_ID: OverlayId = OverlayId(*b"ac87b6945b4f6f736963f7f65d025943");

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        key_pair: Arc<KeyPair>,
        network: &Network,
        peer_resolver: &PeerResolver,
        overlay_service: &OverlayService,
        mempool_storage: &MempoolStorage,
        committed: mpsc::UnboundedSender<(PointInfo, Vec<Point>)>,
        collator_round: &OuterRound<Collator>,
        input_buffer: InputBuffer,
        genesis_round: Option<u32>,
    ) -> Self {
        MempoolConfig::set_genesis_round(Round(genesis_round.unwrap_or_default()));

        let consensus_round = OuterRound::default();
        let commit_round = OuterRound::default();
        let effects = Effects::<ChainedRoundsContext>::new(consensus_round.get());
        let responder = Responder::default();

        let private_overlay = PrivateOverlay::builder(Self::PRIVATE_OVERLAY_ID)
            .with_peer_resolver(peer_resolver.clone())
            .named("tycho-consensus")
            .build(responder.clone());

        overlay_service.add_private_overlay(&private_overlay);

        let dispatcher = Dispatcher::new(network, &private_overlay);
        let peer_schedule = PeerSchedule::new(key_pair, private_overlay);

        let store = MempoolStore::new(
            mempool_storage.clone(),
            consensus_round.receiver(),
            commit_round.receiver(),
            collator_round.receiver(),
        );
        let round_task = RoundTaskReady::new(
            &dispatcher,
            peer_schedule,
            store,
            &consensus_round,
            collator_round.clone(),
            responder,
            input_buffer,
        );
        tokio::spawn({
            let peer_schedule = round_task.state.peer_schedule.clone();
            async move {
                peer_schedule.run_updater().await;
            }
        });
        Self {
            dag: Dag::default(),
            committed,
            consensus_round,
            commit_round,
            round_task,
            effects,
        }
    }

    pub fn init_with_genesis(&mut self, current_peers: &[PeerId]) {
        let genesis = crate::test_utils::genesis();
        // check only genesis round as it is widely used in point validation.
        // if some nodes use distinct genesis data, their first points will be rejected
        assert_eq!(
            genesis.id(),
            crate::test_utils::genesis_point_id(),
            "genesis point id does not match one from config"
        );
        {
            let mut guard = self.round_task.state.peer_schedule.write();
            let peer_schedule = self.round_task.state.peer_schedule.clone();

            // finished epoch
            guard.set_next_start(MempoolConfig::genesis_round(), &peer_schedule);
            guard.set_next_peers(
                &[crate::test_utils::genesis_point_id().author],
                &peer_schedule,
                false,
            );
            guard.rotate(&peer_schedule);

            // current epoch
            guard.set_next_start(MempoolConfig::genesis_round().next(), &peer_schedule);
            // start updater only after peers are populated into schedule
            guard.set_next_peers(current_peers, &peer_schedule, true);
            guard.rotate(&peer_schedule);
        }
        Verifier::verify(&genesis, &self.round_task.state.peer_schedule)
            .expect("genesis failed to verify");

        let genesis_round =
            DagRound::new_bottom(genesis.round(), &self.round_task.state.peer_schedule);

        let next_dag_round = genesis_round.new_next(&self.round_task.state.peer_schedule);

        let _ = genesis_round.insert_exact_sign(
            &genesis,
            next_dag_round.key_pair(),
            &self.round_task.state.store,
        );
    }

    // restore last two rounds into dag, return the last own point among them to repeat broadcast
    async fn pre_run(&mut self) -> Option<(Point, BoxFuture<'static, InclusionState>)> {
        let last_round = tokio::task::spawn_blocking({
            let store = self.round_task.state.store.clone();
            move || store.latest_round()
        })
        .await
        .expect("load last round from db");

        let first_round = MempoolConfig::genesis_round().max(Round(last_round.0.saturating_sub(2)));

        self.effects = Effects::<ChainedRoundsContext>::new(last_round);
        let round_effects = Effects::<EngineContext>::new(&self.effects, last_round);

        self.round_task.state.responder.update(
            &self.round_task.state.broadcast_filter,
            None,
            &self.round_task.state.downloader,
            &self.round_task.state.store,
            &round_effects,
        );

        let info_flags = tokio::task::spawn_blocking({
            let store = self.round_task.state.store.clone();
            move || store.load_rounds(first_round, last_round)
        })
        .instrument(round_effects.span().clone())
        .await
        .expect("load last info and flags from db");

        let _span_guard = round_effects.span().enter();
        assert!(
            info_flags.first().map(|(i, _)| i.round()) <= info_flags.last().map(|(i, _)| i.round()),
            "wrong order of data from db on init"
        );

        let bottom_round = MempoolConfig::genesis_round().max(Round(
            last_round
                .0
                .saturating_sub(MempoolConfig::COMMIT_DEPTH as _),
        ));

        let start_info = {
            let maybe_unfinished_round = last_round.prev();
            let local_id = {
                // if local id changed, then we can't reuse that old point
                let guard = self.round_task.state.peer_schedule.atomic();
                guard
                    .local_keys(last_round)
                    .or(guard.local_keys(maybe_unfinished_round))
                    .map(|key_pair| PeerId::from(key_pair.public_key))
            };
            match local_id {
                None => None,
                Some(local_id) => info_flags
                    .iter()
                    .rev()
                    .filter(|(info, _)| info.data().author == local_id)
                    .take_while(|(info, _)| info.round() >= maybe_unfinished_round)
                    .max_by_key(|(info, _)| info.round())
                    .map(|(info, _)| info)
                    .cloned(),
            }
        };

        let start_round = start_info.as_ref().map_or(last_round, |info| info.round());
        self.dag.init(DagRound::new_bottom(
            bottom_round,
            &self.round_task.state.peer_schedule,
        ));
        self.dag.fill_to_top(
            start_round,
            &self.round_task.state.peer_schedule,
            &round_effects,
        );
        let dag_round = self.dag.top();
        let mut point_dag_round = dag_round.clone();

        let includes = FuturesUnordered::new();
        for (info, flags) in info_flags {
            if info.round() > start_round {
                // if info.round() > start_info.round():
                // * either consensus is on the same start_round(+1) and keeps broadcasting points
                // * or consensus advanced further, and there's no need for start_round+1 includes
                break;
            }
            if point_dag_round.round() != info.round() {
                match dag_round.scan(info.round()) {
                    Some(found) => point_dag_round = found,
                    None => panic!("dag was incorrectly extended: cannot restore point info"),
                };
            };
            let inclusion_state = point_dag_round.restore_exact(
                &info,
                &flags,
                &self.round_task.state.downloader,
                &self.round_task.state.store,
                &round_effects,
            );
            if info.round() == start_round {
                tracing::info!("add inclusion_state {:?}", info.id().alt());
                includes.push(inclusion_state);
            }
        }

        drop(_span_guard);
        let start_point = match start_info {
            None => None,
            Some(start_info) => tokio::task::spawn_blocking({
                let store = self.round_task.state.store.clone();
                move || store.get_point(start_info.round(), start_info.digest())
            })
            .instrument(round_effects.span().clone())
            .await
            .expect("load last own point from db"),
        };

        let start_point_id = start_point.as_ref().map(|point| point.id());
        tracing::info!(
            parent: round_effects.span(),
            start_round = start_round.0,
            start_point = start_point_id.as_ref().map(|point_id| debug(point_id.alt())),
            "pre-run setup completed",
        );
        // engine or collector may jump to later round if it is determined by broadcast filter
        self.consensus_round.set_max(start_round);
        self.round_task.collector.init(start_round, includes);

        // start_point's inclusion state is already pushed into collector, just a stub is enough
        start_point.map(|a| (a, future::pending().boxed()))
    }

    pub async fn run(mut self) -> ! {
        let mut start_point = self.pre_run().await;
        let (committed_info_tx, committed_info_rx) =
            mpsc::unbounded_channel::<(PointInfo, Vec<PointInfo>)>();

        tokio::spawn({
            let store = self.round_task.state.store.clone();
            let committed_full_rx = self.committed;
            let commit_round = self.commit_round;
            Self::expand_commit(store, committed_info_rx, committed_full_rx, commit_round)
        });

        loop {
            let _round_duration = HistogramGuard::begin("tycho_mempool_engine_round_time");
            let (prev_round_ok, current_dag_round, round_effects) = {
                // do not repeat the `get()` - it can give non-reproducible result
                let consensus_round = self.consensus_round.get();
                let top_dag_round = self.dag.top();
                assert!(
                    consensus_round >= top_dag_round.round(),
                    "consensus round {} cannot be less than top dag round {}",
                    consensus_round.0,
                    top_dag_round.round().0,
                );
                metrics::gauge!("tycho_mempool_engine_rounds_skipped")
                    .increment((consensus_round.0 as f64) - (top_dag_round.round().0 as f64));

                // `true` if we collected enough dependencies and (optionally) signatures,
                // so `next_dag_round` from the previous loop is the current now
                let prev_round_ok = consensus_round == top_dag_round.round();
                if prev_round_ok {
                    // Round was not advanced by `BroadcastFilter`, and `Collector` gathered enough
                    // broadcasts during previous round. So if `Broadcaster` was run at previous
                    // round (controlled by `Collector`), then it gathered enough signatures.
                    // If our newly created current round repeats such success, then we've moved
                    // consensus with other 2F nodes during previous round
                    // (at this moment we are not sure to be free from network lag).
                    // Then any reliable point received _up to this moment_ should belong to a round
                    // not greater than new `next_dag_round`, and any future round cannot exist.

                    let round_effects =
                        Effects::<EngineContext>::new(&self.effects, consensus_round);
                    (prev_round_ok, top_dag_round, round_effects)
                } else {
                    self.effects = Effects::<ChainedRoundsContext>::new(consensus_round);
                    let round_effects =
                        Effects::<EngineContext>::new(&self.effects, consensus_round);
                    let current_dag_round = self.dag.fill_to_top(
                        consensus_round,
                        &self.round_task.state.peer_schedule,
                        &round_effects,
                    );
                    (prev_round_ok, current_dag_round, round_effects)
                }
            };
            metrics::gauge!("tycho_mempool_engine_current_round").set(current_dag_round.round().0);

            let next_dag_round = self.dag.fill_to_top(
                current_dag_round.round().next(),
                &self.round_task.state.peer_schedule,
                &round_effects,
            );

            let (collector_signal_tx, collector_signal_rx) = watch::channel(CollectorSignal::Retry);

            let (own_point_fut, own_point_state) = match start_point.take() {
                Some((point, own_point_state)) => {
                    let point_fut = future::ready(Ok(Some(point)));
                    (point_fut.boxed(), own_point_state)
                }
                None => self.round_task.own_point_task(
                    prev_round_ok,
                    &collector_signal_rx,
                    &current_dag_round,
                    &round_effects,
                ),
            };

            let round_task_run = self
                .round_task
                .run(
                    own_point_fut,
                    own_point_state,
                    collector_signal_tx,
                    collector_signal_rx,
                    &next_dag_round,
                    &round_effects,
                )
                .until_ready();

            let commit_run = tokio::task::spawn_blocking({
                let mut dag = self.dag;
                let committed_info_tx = committed_info_tx.clone();
                let round_effects = round_effects.clone();
                move || {
                    let task_start = Instant::now();
                    let _guard = round_effects.span().enter();

                    let committed = dag.commit();

                    round_effects.commit_metrics(&committed);
                    round_effects.log_committed(&committed);

                    if !committed.is_empty() {
                        for points in committed {
                            committed_info_tx
                                .send(points) // not recoverable
                                .expect("Failed to send anchor history info to mpsc channel");
                        }
                        metrics::histogram!("tycho_mempool_engine_commit_time")
                            .record(task_start.elapsed());
                    }
                    dag
                }
            });

            match tokio::try_join!(round_task_run, commit_run) {
                Ok(((round_task, next_round), dag)) => {
                    self.round_task = round_task;
                    self.consensus_round.set_max(next_round);
                    self.dag = dag;
                }
                Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
                Err(e) => panic!("mempool engine failed: {e:?}"),
            }
        }
    }

    async fn expand_commit(
        store: MempoolStore,
        mut collapsed: mpsc::UnboundedReceiver<(PointInfo, Vec<PointInfo>)>,
        expanded: mpsc::UnboundedSender<(PointInfo, Vec<Point>)>,
        commit_round: OuterRound<Commit>,
    ) -> ! {
        while let Some((anchor, history)) = collapsed.recv().await {
            let store = store.clone();
            let task = tokio::task::spawn_blocking(move || store.expand_anchor_history(&history));
            let history = task.await.expect("expand anchor history task failed");
            let round = anchor.round();
            expanded
                .send((anchor, history))
                .expect("Failed to send anchor history to mpsc channel");
            commit_round.set_max(round);
        }
        tracing::warn!("engine commit info channel closed");
        futures_util::future::pending().await
    }
}

impl Effects<EngineContext> {
    fn commit_metrics(&self, committed: &[(PointInfo, Vec<PointInfo>)]) {
        metrics::counter!("tycho_mempool_commit_anchors").increment(committed.len() as _);

        if let Some((first_anchor, _)) = committed.first() {
            metrics::gauge!("tycho_mempool_commit_latency_rounds")
                .set(self.depth(first_anchor.round()));
        }
        if let Some((last_anchor, _)) = committed.last() {
            let now = UnixTime::now().as_u64();
            let anchor_time = last_anchor.data().time.as_u64();
            let latency = if now >= anchor_time {
                Duration::from_millis(now - anchor_time).as_secs_f64()
            } else {
                Duration::from_millis(anchor_time - now).as_secs_f64().neg()
            };
            metrics::histogram!("tycho_mempool_commit_anchor_latency_time").record(latency);
        }
    }

    fn log_committed(&self, committed: &[(PointInfo, Vec<PointInfo>)]) {
        if !committed.is_empty() && tracing::enabled!(tracing::Level::DEBUG) {
            let committed = committed
                .iter()
                .map(|(anchor, history)| {
                    let history = history
                        .iter()
                        .map(|point| format!("{:?}", point.id().alt()))
                        .join(", ");
                    format!(
                        "anchor {:?} time {} : [ {history} ]",
                        anchor.id().alt(),
                        anchor.data().time
                    )
                })
                .join("  ;  ");
            tracing::debug!(
                parent: self.span(),
                "committed {committed}"
            );
        }
    }
}
