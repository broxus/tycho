use std::mem;
use std::sync::Arc;

use everscale_crypto::ed25519::KeyPair;
use futures_util::future::BoxFuture;
use futures_util::{future, FutureExt};
use itertools::Itertools;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tycho_network::{Network, OverlayService, PeerId, PeerResolver, PrivateOverlay};
use tycho_util::futures::JoinTask;
use tycho_util::metrics::HistogramGuard;

use crate::dag::{Committer, DagFront, DagRound, Verifier};
use crate::effects::{
    AltFormat, ChainedRoundsContext, Effects, EngineContext, MempoolAdapterStore, MempoolStore,
};
use crate::engine::input_buffer::InputBuffer;
use crate::engine::round_task::RoundTaskReady;
use crate::engine::round_watch::{Consensus, RoundWatch, RoundWatcher, TopKnownAnchor};
use crate::engine::{CachedConfig, Genesis, MempoolConfig};
use crate::intercom::{CollectorSignal, Dispatcher, PeerSchedule, Responder};
use crate::models::{AnchorData, MempoolOutput, Point, PointInfo, Round};

pub struct Engine {
    dag: DagFront,
    committer_run: JoinHandle<Committer>,
    committed_info_tx: mpsc::UnboundedSender<MempoolOutput>,
    consensus_round: RoundWatch<Consensus>,
    round_task: RoundTaskReady,
    effects: Effects<ChainedRoundsContext>,
    init_task: Option<JoinTask<()>>,
}

#[derive(Clone)]
pub struct EngineHandle {
    peer_schedule: PeerSchedule,
}
impl EngineHandle {
    pub fn set_next_peers(&self, set: &[PeerId], subset: Option<(u32, &[PeerId])>) {
        if let Some((switch_round, subset)) = subset {
            // specially for zerostate with unaligned genesis,
            // and for first (prev) vset after reboot or a new genesis
            let round = if switch_round <= Genesis::round().0 {
                Genesis::round().next()
            } else {
                Round(switch_round)
            };
            if !(self.peer_schedule).set_next_subset(set, round, subset) {
                tracing::trace!("cannot schedule outdated round {switch_round} and set");
                return;
            }
        }
        self.peer_schedule.set_next_set(set);
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
        committed_info_tx: mpsc::UnboundedSender<MempoolOutput>,
        top_known_anchor: &RoundWatch<TopKnownAnchor>,
        config: &MempoolConfig,
    ) -> Self {
        // mostly everything depends on genesis - must init at the first line
        // MempoolConfig::init(&global_config);
        let (genesis, overlay_id) = CachedConfig::init(config);

        let consensus_round = RoundWatch::default();
        consensus_round.set_max(Genesis::round());
        top_known_anchor.set_max(Genesis::round());
        let effects = Effects::<ChainedRoundsContext>::new(consensus_round.get());
        let responder = Responder::default();

        let private_overlay = PrivateOverlay::builder(overlay_id)
            .with_peer_resolver(peer_resolver.clone())
            .named("tycho-consensus")
            .build(responder.clone());

        overlay_service.add_private_overlay(&private_overlay);

        let dispatcher = Dispatcher::new(network, &private_overlay);
        let peer_schedule = PeerSchedule::new(key_pair.clone(), private_overlay);

        genesis.verify_hash().expect("Failed to verify genesis");
        Verifier::verify(&genesis, &peer_schedule).expect("genesis failed to verify");

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
                        genesis_dag_round.insert_exact_sign(&genesis, Some(&key_pair), &store);
                    }
                });
                match init_storage_task.await {
                    Ok(()) => (),
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

    #[cfg(any(test, feature = "test"))]
    pub fn set_start_peers(&self, peers: &[PeerId]) {
        let first = Genesis::round().next();
        (self.round_task.state.peer_schedule).set_next_subset(peers, first, peers);
    }

    // restore last two rounds into dag, return the last own point among them to repeat broadcast
    async fn pre_run(&mut self) -> Option<Point> {
        self.init_task.take().expect("init task must be set").await;
        let broadcast_points = tokio::task::spawn_blocking({
            let store = self.round_task.state.store.clone();
            let peer_schedule = self.round_task.state.peer_schedule.clone();
            // TKA is set in adapter from last signed block, so no need to repeat older broadcasts
            let top_known_anchor = self.round_task.state.top_known_anchor.get();
            move || {
                store
                    .load_last_broadcasts(top_known_anchor)
                    .and_then(|(last, prev)| {
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
            None => (Genesis::round().next()).max(self.round_task.state.top_known_anchor.get()),
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
                self.dag
                    .top()
                    .insert_exact_sign(&last, Some(&keys), &self.round_task.state.store);
                Some(last)
            }
            None => None,
        }
    }

    pub async fn run(mut self) {
        let mut start_point = self.pre_run().await;
        // Boxed for just not to move a Copy to other thread by mistake
        let mut full_history_bottom: Box<Option<Round>> = Box::new(None);
        let mut is_paused = true;
        loop {
            let _round_duration = HistogramGuard::begin("tycho_mempool_engine_round_time");
            // commit may take longer than a round if it ends with a jump to catch up with consensus
            let mut ready_committer = take_committer(&mut self.committer_run);

            // do not repeat the `get()` - it can give non-reproducible result
            let current_round = self.consensus_round.get();

            let round_effects = {
                let top_round = self.dag.top().round();
                assert!(
                    current_round >= top_round,
                    "consensus round {} cannot be less than top dag round {}",
                    current_round.0,
                    top_round.0,
                );
                metrics::gauge!("tycho_mempool_engine_rounds_skipped")
                    .increment((current_round.0 as f64) - (top_round.0 as f64));

                metrics::gauge!("tycho_mempool_engine_current_round").set(current_round.0);

                if current_round == top_round {
                    Effects::<EngineContext>::new(&self.effects, current_round)
                } else {
                    self.effects = Effects::<ChainedRoundsContext>::new(current_round);
                    Effects::<EngineContext>::new(&self.effects, current_round)
                }
            };

            if let Some(collator_lag) = wait_collator(
                self.round_task.state.top_known_anchor.receiver(),
                current_round,
                &round_effects,
            ) {
                tokio::time::timeout(CachedConfig::broadcast_retry(), collator_lag)
                    .await
                    .ok();

                if !is_paused {
                    is_paused = true;
                    self.committed_info_tx.send(MempoolOutput::Paused).ok();
                }

                if let Some(committer) = ready_committer {
                    self.committer_run = committer_task(
                        committer,
                        full_history_bottom.take(),
                        self.committed_info_tx.clone(),
                        round_effects.clone(),
                    );
                }

                continue;
            }

            if is_paused {
                is_paused = false;
                self.committed_info_tx.send(MempoolOutput::Running).ok();
            }

            *full_history_bottom = full_history_bottom.or(self.dag.fill_to_top(
                current_round.next(),
                ready_committer.as_mut(),
                &self.round_task.state.peer_schedule,
            ));

            let head = self.dag.head(&self.round_task.state.peer_schedule);

            let collector_signal_tx = watch::Sender::new(CollectorSignal::Retry);

            let own_point_fut = match start_point.take() {
                Some(point) => future::ready(Some(point)).boxed(),
                None => self.round_task.own_point_task(
                    &head,
                    collector_signal_tx.subscribe(),
                    &round_effects,
                ),
            };

            let round_task_run = self
                .round_task
                .run(own_point_fut, collector_signal_tx, &head, &round_effects)
                .until_ready();

            if let Some(committer) = ready_committer {
                self.committer_run = committer_task(
                    committer,
                    full_history_bottom.take(),
                    self.committed_info_tx.clone(),
                    round_effects.clone(),
                );
            }

            match round_task_run.await {
                Ok(round_task) => {
                    self.round_task = round_task;
                }
                Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
                Err(e) => panic!("mempool engine failed: {e:?}"),
            }
        }
    }
}

fn wait_collator(
    mut top_known_anchor_recv: RoundWatcher<TopKnownAnchor>,
    current_round: Round,
    round_effects: &Effects<EngineContext>,
) -> Option<BoxFuture<'static, ()>> {
    let top_known_anchor = top_known_anchor_recv.get();
    let silent_after = CachedConfig::silent_after(top_known_anchor);
    // Note silence bound is exclusive with `<` because new vset must be known for dag top
    //  (the next after engine round), while vset switch round is exactly silence bound + 1
    if current_round < silent_after {
        None
    } else {
        tracing::info!(
            parent: round_effects.span(),
            top_known_anchor = top_known_anchor.0,
            silent_after = silent_after.0,
            "enter silent mode by collator feedback",
        );
        let round_effects = round_effects.clone();
        let task = async move {
            loop {
                let top_known_anchor = top_known_anchor_recv.next().await;
                //  exit if ready to produce point: collator synced enough
                let silent_after = CachedConfig::silent_after(top_known_anchor);
                let exit = current_round < silent_after;
                tracing::info!(
                    parent: round_effects.span(),
                    top_known_anchor = top_known_anchor.0,
                    silent_after = silent_after.0,
                    exit = exit,
                    "collator feedback in silent mode"
                );
                if exit {
                    break;
                }
            }
        };
        Some(task.boxed())
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

fn committer_task(
    mut committer: Committer,
    full_history_bottom: Option<Round>,
    committed_info_tx: mpsc::UnboundedSender<MempoolOutput>,
    round_effects: Effects<EngineContext>,
) -> JoinHandle<Committer> {
    tokio::task::spawn_blocking(move || {
        // may run for long several times in a row and commit nothing, because of missed points
        let _span = round_effects.span().enter();

        let mut committed = None;
        let mut new_full_history_bottom = None;

        let start_bottom = committer.bottom_round();
        let start_dag_len = committer.dag_len();

        for attempt in 0.. {
            match committer.commit() {
                Ok(data) => {
                    committed = Some(data);
                    break;
                }
                Err(round) => {
                    let result = committer.drop_upto(round.next());
                    new_full_history_bottom = Some(result.unwrap_or_else(|x| x));
                    tracing::info!(
                        ?start_bottom,
                        start_dag_len,
                        current_bottom = ?committer.bottom_round(),
                        current_dag_len = committer.dag_len(),
                        attempt,
                        "comitter rounds were dropped as impossible to sync"
                    );
                    if result.is_err() {
                        break;
                    } else if attempt > start_dag_len {
                        panic!(
                            "infinite loop on dropping dag rounds: \
                             start dag len {start_dag_len}, start bottom {start_bottom:?} \
                             resulting {:?}",
                            committer.alt()
                        )
                    };
                }
            }
        }

        if let Some(new_bottom) = new_full_history_bottom.or(full_history_bottom) {
            committed_info_tx
                .send(MempoolOutput::NewStartAfterGap(new_bottom)) // not recoverable
                .expect("Failed to send anchor history info to mpsc channel");
        }

        if let Some(committed) = committed {
            round_effects.log_committed(&committed);
            for data in committed {
                round_effects.commit_metrics(&data.anchor);
                committed_info_tx
                    .send(MempoolOutput::NextAnchor(data)) // not recoverable
                    .expect("Failed to send anchor history info to mpsc channel");
            }
        }

        committer
    })
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
