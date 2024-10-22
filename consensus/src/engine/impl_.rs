use std::mem;
use std::sync::Arc;

use everscale_crypto::ed25519::KeyPair;
use futures_util::{future, FutureExt};
use itertools::Itertools;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tycho_network::{Network, OverlayService, PeerId, PeerResolver, PrivateOverlay};
use tycho_util::futures::JoinTask;
use tycho_util::metrics::HistogramGuard;

use crate::dag::{Committer, DagFront, DagRound, InclusionState, Verifier};
use crate::effects::{
    AltFormat, ChainedRoundsContext, Effects, EngineContext, MempoolAdapterStore, MempoolStore,
};
use crate::engine::input_buffer::InputBuffer;
use crate::engine::round_task::RoundTaskReady;
use crate::engine::round_watch::{Consensus, RoundWatch, TopKnownAnchor};
use crate::engine::{CachedConfig, Genesis, MempoolConfig};
use crate::intercom::{CollectorSignal, Dispatcher, PeerSchedule, Responder};
use crate::models::{AnchorData, CommitResult, Point, PointInfo, Round};

pub struct Engine {
    dag: DagFront,
    committer_run: JoinHandle<Committer>,
    committed_info_tx: mpsc::UnboundedSender<CommitResult>,
    consensus_round: RoundWatch<Consensus>,
    round_task: RoundTaskReady,
    effects: Effects<ChainedRoundsContext>,
    init_task: Option<JoinTask<InclusionState>>,
}

#[derive(Clone)]
pub struct EngineHandle {
    peer_schedule: PeerSchedule,
}
impl EngineHandle {
    pub fn set_next_peers(&self, next_peers: &[PeerId], next_round: Option<u32>) {
        if let Some(next) = next_round {
            if !self.peer_schedule.set_next_start(Round(next)) {
                tracing::trace!("cannot schedule outdated round {next} and set");
                return;
            }
        }
        self.peer_schedule.set_next_peers(next_peers, true);
    }
}

impl Engine {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        key_pair: Arc<KeyPair>,
        network: &Network,
        peer_resolver: &PeerResolver,
        overlay_service: &OverlayService,
        mempool_adapter_store: &MempoolAdapterStore,
        input_buffer: InputBuffer,
        committed_info_tx: mpsc::UnboundedSender<CommitResult>,
        top_known_anchor: &RoundWatch<TopKnownAnchor>,
        bootstrap_peers: &[PeerId],
        config: &MempoolConfig,
    ) -> Self {
        // mostly everything depends on genesis - must init at the first line
        // MempoolConfig::init(&global_config);
        let (genesis, overlay_id) = CachedConfig::init(config);

        let consensus_round = RoundWatch::default();
        consensus_round.set_max(Genesis::round());
        let effects = Effects::<ChainedRoundsContext>::new(consensus_round.get());
        let responder = Responder::default();

        let private_overlay = PrivateOverlay::builder(overlay_id)
            .with_peer_resolver(peer_resolver.clone())
            .named("tycho-consensus")
            .build(responder.clone());

        overlay_service.add_private_overlay(&private_overlay);

        let dispatcher = Dispatcher::new(network, &private_overlay);
        let peer_schedule = PeerSchedule::new(key_pair.clone(), private_overlay);
        peer_schedule.set_next_peers(bootstrap_peers, true);
        peer_schedule.set_next_start(Genesis::round().next());
        peer_schedule.apply_scheduled(Genesis::round().next());

        genesis.verify_hash().expect("Failed to verify genesis");
        Verifier::verify(&genesis).expect("genesis failed to verify");

        let store = MempoolStore::new(
            mempool_adapter_store,
            consensus_round.receiver(),
            top_known_anchor.receiver(),
        );

        // Dag, created at genesis, will at first extend up to it's greatest length
        // (in case last broadcast is within it) without data,
        // and may shrink to its medium len (in case broadcast or consensus are further)
        // before being filled with data
        let mut dag = DagFront::default();
        let committer = dag.init(DagRound::new_bottom(Genesis::round(), &peer_schedule));
        let committer_run = tokio::spawn(future::ready(committer));

        let init_task = JoinTask::new({
            let store = store.clone();
            let genesis_dag_round = dag.top().clone();
            async move {
                let init_storage_task = tokio::task::spawn_blocking({
                    move || {
                        store.init_storage(&overlay_id);
                        // may be overwritten or left unused until next clean task, does not matter
                        genesis_dag_round.insert_exact_sign(&genesis, Some(&key_pair), &store)
                    }
                });
                match init_storage_task.await {
                    Ok(genesis_incl_state) => genesis_incl_state,
                    Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
                    Err(e) => panic!("failed to clean db on genesis {e:?}"),
                }
            }
        });

        let round_task = RoundTaskReady::new(
            &dispatcher,
            peer_schedule,
            store,
            &consensus_round,
            top_known_anchor.clone(),
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
            dag,
            committer_run,
            committed_info_tx,
            consensus_round,
            round_task,
            effects,
            init_task: Some(init_task),
        }
    }

    pub fn get_handle(&self) -> EngineHandle {
        EngineHandle {
            peer_schedule: self.round_task.state.peer_schedule.clone(),
        }
    }

    // restore last two rounds into dag, return the last own point among them to repeat broadcast
    async fn pre_run(&mut self) -> Option<(Point, InclusionState)> {
        let genesis_incl_state = self.init_task.take().expect("init task must be set").await;
        let broadcast_points = tokio::task::spawn_blocking({
            let store = self.round_task.state.store.clone();
            let peer_schedule = self.round_task.state.peer_schedule.clone();
            move || {
                store.load_last_broadcasts().and_then(|(last, prev)| {
                    peer_schedule
                        .atomic()
                        .local_keys(last.round()) // against db cloning
                        .map(|keys| (PeerId::from(keys.public_key), keys))
                        .filter(|(local_id, _)| local_id == last.data().author)
                        .map(|(_, keys)| (last, keys, prev))
                })
            }
        })
        .await
        .expect("load last round from db");

        // even if have some history in DB above broadcast rounds, we replay the last broadcasts
        let top_round = match &broadcast_points {
            Some((last_broadcast, _, _)) => last_broadcast.round(),
            None => Genesis::round(), // will reproduce the first point after genesis,
        };
        self.consensus_round.set_max(top_round);

        self.effects = Effects::<ChainedRoundsContext>::new(top_round);
        let round_effects = Effects::<EngineContext>::new(&self.effects, top_round);

        // store in committer (back dag) - no data to init with;
        // commiter must contain the same rounds as front dag, plus required history
        // as consensus may be far away while local history has some unfinished downloads
        let mut committer = take_committer(&mut self.committer_run).expect("init");
        (self.dag).fill_to_top(
            top_round,
            Some(&mut committer),
            &self.round_task.state.peer_schedule,
        );
        self.committer_run = tokio::spawn(future::ready(committer));

        // bcast filter will be init on round start

        match broadcast_points {
            Some((last, keys, pre_last)) => {
                if let Some(pre_last) = pre_last {
                    self.round_task
                        .init_prev_broadcast(pre_last, &round_effects);
                }
                // start point's inclusion state is pushed into collector during loop run
                let incl_state = self.dag.top().insert_exact_sign(
                    &last,
                    Some(&keys),
                    &self.round_task.state.store,
                );
                Some((last, incl_state))
            }
            None => {
                // dag top is genesis round
                self.round_task.collector.init(
                    top_round,
                    std::iter::once(future::ready(genesis_incl_state).boxed()).collect(),
                );
                None
            }
        }
    }

    pub async fn run(mut self) {
        let mut start_point_with_state = self.pre_run().await;
        let mut full_history_bottom = None;
        loop {
            let _round_duration = HistogramGuard::begin("tycho_mempool_engine_round_time");
            // commit may take longer than a round if it ends with a jump to catch up with consensus
            let mut ready_committer = take_committer(&mut self.committer_run);

            let (current_dag_round, round_effects) = {
                // do not repeat the `get()` - it can give non-reproducible result
                let consensus_round = self.consensus_round.get();
                let top_dag_round = self.dag.top().clone();
                assert!(
                    consensus_round >= top_dag_round.round(),
                    "consensus round {} cannot be less than top dag round {}",
                    consensus_round.0,
                    top_dag_round.round().0,
                );
                metrics::gauge!("tycho_mempool_engine_rounds_skipped")
                    .increment((consensus_round.0 as f64) - (top_dag_round.round().0 as f64));

                if consensus_round == top_dag_round.round() {
                    let round_effects =
                        Effects::<EngineContext>::new(&self.effects, consensus_round);
                    (top_dag_round, round_effects)
                } else {
                    self.effects = Effects::<ChainedRoundsContext>::new(consensus_round);
                    let round_effects =
                        Effects::<EngineContext>::new(&self.effects, consensus_round);
                    full_history_bottom = full_history_bottom.or(self.dag.fill_to_top(
                        consensus_round,
                        ready_committer.as_mut(),
                        &self.round_task.state.peer_schedule,
                    ));
                    (self.dag.top().clone(), round_effects)
                }
            };
            metrics::gauge!("tycho_mempool_engine_current_round").set(current_dag_round.round().0);

            full_history_bottom = full_history_bottom.or(self.dag.fill_to_top(
                current_dag_round.round().next(),
                ready_committer.as_mut(),
                &self.round_task.state.peer_schedule,
            ));
            let next_dag_round = self.dag.top().clone();

            let (collector_signal_tx, collector_signal_rx) = watch::channel(CollectorSignal::Retry);

            let (own_point_fut, own_point_state) = match start_point_with_state.take() {
                Some((point, state)) => (
                    future::ready(Ok(Some(point))).boxed(),
                    future::ready(state).boxed(),
                ),
                None => self.round_task.own_point_task(
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

            if let Some(mut committer) = ready_committer {
                let committed_info_tx = self.committed_info_tx.clone();
                let round_effects = round_effects.clone();

                self.committer_run = tokio::task::spawn_blocking(move || {
                    let _guard = round_effects.span().enter();

                    if let Some(full_history_bottom) = mem::take(&mut full_history_bottom) {
                        committed_info_tx
                            .send(CommitResult::NewStartAfterGap(full_history_bottom)) // not recoverable
                            .expect("Failed to send anchor history info to mpsc channel");
                    };

                    let committed = committer.commit();

                    round_effects.log_committed(&committed);

                    for data in committed {
                        round_effects.commit_metrics(&data.anchor);
                        committed_info_tx
                            .send(CommitResult::Next(data)) // not recoverable
                            .expect("Failed to send anchor history info to mpsc channel");
                    }

                    committer
                });
            }

            match round_task_run.await {
                Ok((round_task, next_round)) => {
                    self.round_task = round_task;
                    self.consensus_round.set_max(next_round);
                }
                Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
                Err(e) => panic!("mempool engine failed: {e:?}"),
            }
        }
    }
}

fn take_committer(committer_run: &mut JoinHandle<Committer>) -> Option<Committer> {
    if committer_run.is_finished() {
        let taken = mem::replace(committer_run, tokio::spawn(future::pending()));
        committer_run.abort();
        match taken.now_or_never() {
            Some(Ok(committer)) => Some(committer),
            Some(Err(e)) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
            Some(Err(e)) => panic!("committer task: {e:?}"),
            None => unreachable!("committer task is finished and can be taken only once"),
        }
    } else {
        None
    }
}

impl Effects<EngineContext> {
    fn commit_metrics(&self, anchor: &PointInfo) {
        metrics::counter!("tycho_mempool_commit_anchors").increment(1);
        metrics::gauge!("tycho_mempool_commit_latency_rounds").set(self.depth(anchor.round()));
    }

    fn log_committed(&self, committed: &[AnchorData]) {
        if !committed.is_empty() && tracing::enabled!(tracing::Level::DEBUG) {
            let committed = committed
                .iter()
                .map(|data| {
                    let history = data
                        .history
                        .iter()
                        .map(|point| format!("{:?}", point.id().alt()))
                        .join(", ");
                    format!(
                        "anchor {:?} time {} : [ {history} ]",
                        data.anchor.id().alt(),
                        data.anchor.data().time
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
