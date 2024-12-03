use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use std::{cmp, mem};

use everscale_crypto::ed25519::KeyPair;
use futures_util::future::BoxFuture;
use futures_util::stream::FuturesUnordered;
use futures_util::{future, FutureExt, StreamExt};
use itertools::Itertools;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tracing::Instrument;
use tycho_network::{Network, OverlayService, PeerId, PeerResolver, PrivateOverlay};
use tycho_util::futures::JoinTask;
use tycho_util::metrics::HistogramGuard;

use crate::dag::{Committer, DagFront, DagRound, KeyGroup, Verifier};
use crate::effects::{AltFormat, Ctx, EngineCtx, MempoolAdapterStore, MempoolStore, RoundCtx};
use crate::engine::input_buffer::InputBuffer;
use crate::engine::round_task::RoundTaskReady;
use crate::engine::round_watch::{Consensus, RoundWatch, RoundWatcher, TopKnownAnchor};
use crate::engine::{CachedConfig, ConsensusConfigExt, Genesis, MempoolConfig};
use crate::intercom::{CollectorSignal, Dispatcher, PeerSchedule, Responder};
use crate::models::{AnchorData, MempoolOutput, Point, PointInfo, Round};

pub struct Engine {
    dag: DagFront,
    committer_run: JoinHandle<Committer>,
    committed_info_tx: mpsc::UnboundedSender<MempoolOutput>,
    consensus_round: RoundWatch<Consensus>,
    round_task: RoundTaskReady,
    ctx: EngineCtx,
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
            let round = if switch_round <= Genesis::id().round.0 {
                Genesis::id().round.next()
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
        consensus_round.set_max(Genesis::id().round);
        top_known_anchor.set_max(Genesis::id().round);
        let engine_ctx = EngineCtx::new(consensus_round.get());
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
        let committer = dag.init(DagRound::new_bottom(Genesis::id().round, &peer_schedule));
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
            ctx: engine_ctx,
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
        let first = Genesis::id().round.next();
        (self.round_task.state.peer_schedule).set_next_subset(peers, first, peers);
    }

    // restore last two rounds into dag, return the last own point among them to repeat broadcast
    async fn pre_run(&mut self) -> Option<(Point, Option<Point>)> {
        self.init_task.take().expect("init task must be set").await;

        // take last round from db

        let last_db_round = tokio::task::spawn_blocking({
            let store = self.round_task.state.store.clone();
            move || store.last_round()
        })
        .await
        .expect("load last round from db");

        tracing::info!("found last db round {}", last_db_round.0);

        self.ctx = EngineCtx::new(last_db_round);

        // wait collator to load blocks and update peer schedule

        let top_known_anchor = {
            let min_top_known_anchor =
                last_db_round - CachedConfig::get().consensus.max_consensus_lag_rounds;

            // NOTE collator have to apply mc state update to mempool first,
            //  and pass its top known anchor only after completion
            let mut top_known_anchor_recv = self.round_task.state.top_known_anchor.receiver();
            let mut top_known_anchor = top_known_anchor_recv.get();
            while top_known_anchor < min_top_known_anchor {
                tracing::info!(
                    ?top_known_anchor,
                    ?min_top_known_anchor,
                    "waiting collator to load up to last top known anchor"
                );
                top_known_anchor = top_known_anchor_recv.next().await;
            }
            top_known_anchor
        };

        // get ready to commit and return deduplicated top known anchor's history to collator

        let consensus_round = last_db_round
            .max(top_known_anchor)
            .max(Genesis::id().round.next());
        self.consensus_round.set_max(consensus_round);
        let round_ctx = RoundCtx::new(&self.ctx, consensus_round);
        let dag_bottom_round = (Genesis::id().round)
            .max(top_known_anchor - CachedConfig::get().consensus.replay_anchor_rounds());

        let mut committer = take_committer(&mut self.committer_run).expect("init");
        (self.dag).fill_to_top(
            consensus_round,
            Some(&mut committer),
            &self.round_task.state.peer_schedule,
        );
        committer.drop_upto(dag_bottom_round).ok();
        self.committer_run = tokio::spawn(future::ready(committer));

        // preload and sign last rounds if node may still participate in consensus

        let preloaded_ids = if top_known_anchor < last_db_round && dag_bottom_round < last_db_round
        {
            let task = tokio::task::spawn_blocking({
                // last 3 rounds is enough to create point at last round with all witness deps
                let preload_bottom = Genesis::id().round.max(last_db_round - 2_u8);
                let store = self.round_task.state.store.clone();
                move || {
                    let mut map = BTreeMap::<cmp::Reverse<Round>, Vec<PointInfo>>::new();
                    for (round, group) in &store
                        .load_info_rounds(preload_bottom, last_db_round)
                        .into_iter()
                        .group_by(|info| info.round())
                    {
                        map.insert(cmp::Reverse(round), group.collect());
                    }
                    map
                }
            });
            match task.await {
                Ok(result) => result,
                Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
                Err(e) => panic!("preload last rounds on init: {e}"),
            }
        } else {
            Default::default()
        };

        let mut dag_restore = FuturesUnordered::new();
        let mut broadcasts = Vec::new();
        for (cmp::Reverse(round), infos) in preloaded_ids {
            let dag_round = self.dag.top().scan(round).expect("must exist");
            for info in &infos {
                dag_round.add_evicted_broadcast_exact(
                    &info.data().author,
                    info.digest(),
                    &self.round_task.state.downloader,
                    &self.round_task.state.store,
                    &round_ctx,
                );
            }

            _ = dag_round.select(|(_, loc)| {
                // each restore future is eager, as it has a spawned task inside
                dag_restore.extend(loc.versions.values().cloned());
                None::<()>
            });

            // to repeat broadcasts from two last determined consensus rounds
            if round >= last_db_round.prev() {
                let keys = KeyGroup::new(round, &self.round_task.state.peer_schedule);
                let mut found = keys
                    .to_produce
                    .as_deref()
                    .map(|k| PeerId::from(k.public_key))
                    .iter()
                    .flat_map(|local_id| {
                        infos
                            .iter()
                            .filter(|info| info.data().author == *local_id)
                            .map(|info| info.id())
                    })
                    .collect::<Vec<_>>();
                assert!(
                    found.len() <= 1,
                    "created non-unique broadcasts at {round:?}: {found:?}"
                );
                if let Some(id) = found.pop() {
                    broadcasts.push(id);
                }
            }
        }

        broadcasts.sort_unstable_by_key(|a| cmp::Reverse(a.round));
        let replay_bcasts_ids = match broadcasts.as_slice() {
            [last] if last.round >= consensus_round.prev() => Some((*last, None)),
            [last, prev] if prev.round >= consensus_round.prev() => Some((*last, Some(*prev))),
            [] => None,
            _ => unreachable!(
                "need only 2 last broadcasts up to {consensus_round:?}, got {:?}",
                broadcasts.iter().map(|id| id.alt()).collect::<Vec<_>>()
            ),
        };

        let replay_bcasts = if let Some((last_bcast, prev_bcast)) = replay_bcasts_ids {
            // if there's a broadcast at last DB round - then it's round will be current for Engine
            let task = tokio::task::spawn_blocking({
                let store = self.round_task.state.store.clone();
                move || {
                    let last = store
                        .get_point(last_bcast.round, &last_bcast.digest)
                        .expect("last bcast by id");
                    if let Some(prev_id) = prev_bcast.as_ref() {
                        assert_eq!(
                            last.data().prev_digest(),
                            Some(&prev_id.digest),
                            "broadcasted ids mismatch: last {:?} has proof for {:?} but found prev {:?}",
                            last.id().alt(),
                            last.prev_id().as_ref().map(|id| id.alt()),
                            prev_id.alt()
                        );
                    };
                    let prev = prev_bcast.map(|id| {
                        store
                            .get_point(id.round, &id.digest)
                            .expect("prev bcast by id")
                    });
                    Some((last, prev))
                }
            });
            match task.await {
                Ok(result) => result,
                Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
                Err(e) => panic!("loading broadcasts on init: {e}"),
            }
        } else {
            None
        };

        while !dag_restore.is_empty() {
            _ = dag_restore.next().await;
        }
        // last broadcast may belong to any of the last 2 dag rounds
        replay_bcasts
    }

    pub async fn run(mut self) {
        let mut replay_bcasts = self.pre_run().await;
        // Boxed for just not to move a Copy to other thread by mistake
        let mut full_history_bottom: Box<Option<Round>> = Box::new(None);
        let mut is_paused = true;
        let mut round_ctx = RoundCtx::new(&self.ctx, self.dag.top().round());
        loop {
            let _round_duration = HistogramGuard::begin("tycho_mempool_engine_round_time");
            // commit may take longer than a round if it ends with a jump to catch up with consensus
            let mut ready_committer = take_committer(&mut self.committer_run);

            {
                let old_dag_top_round = self.dag.top().round();

                let consensus_round = if let Some((start_point, _)) = &replay_bcasts {
                    assert!(
                        start_point.round() == old_dag_top_round
                            || start_point.round() == old_dag_top_round.prev(),
                        "can repeat broadcast only from last two dag rounds, \
                         top {old_dag_top_round:?}, {:?}",
                        start_point.id().alt()
                    );
                    start_point.round()
                } else {
                    // do not repeat the `get()` - it can give non-reproducible result
                    let consensus_round = self.consensus_round.get();
                    assert!(
                        old_dag_top_round <= consensus_round,
                        "consensus round {} cannot be less than old top dag round {}",
                        consensus_round.0,
                        old_dag_top_round.0,
                    );
                    consensus_round
                };

                match collator_feedback(
                    self.round_task.state.top_known_anchor.receiver(),
                    old_dag_top_round,
                    &mut is_paused,
                    &self.committed_info_tx,
                    &round_ctx,
                ) {
                    Ok(pause_at) => {
                        *full_history_bottom = full_history_bottom.or(self.dag.fill_to_top(
                            consensus_round.next().min(pause_at),
                            ready_committer.as_mut(),
                            &self.round_task.state.peer_schedule,
                        ));
                    }
                    Err(collator_sync) => {
                        collator_sync.await;
                        if let Some(committer) = ready_committer {
                            self.committer_run = committer_task(
                                committer,
                                full_history_bottom.take(),
                                self.committed_info_tx.clone(),
                                round_ctx.clone(),
                            );
                        }
                        continue;
                    }
                }

                let dag_top_round = self.dag.top().round();

                assert!(
                    dag_top_round <= consensus_round.next(),
                    "new dag round {} cannot be grater than next consensus round {}",
                    dag_top_round.0,
                    consensus_round.0,
                );

                metrics::gauge!("tycho_mempool_rounds_dag_behind_consensus")
                    .increment(consensus_round - dag_top_round);

                assert!(
                    old_dag_top_round < dag_top_round,
                    "new dag round {} must be grater than old one {}",
                    dag_top_round.0,
                    old_dag_top_round.0,
                );

                if old_dag_top_round < dag_top_round.prev() {
                    self.ctx = EngineCtx::new(dag_top_round);
                }
            };

            let head = self.dag.head(&self.round_task.state.peer_schedule);
            round_ctx = RoundCtx::new(&self.ctx, head.current().round());
            metrics::gauge!("tycho_mempool_engine_current_round").set(head.current().round().0);

            let collector_signal_tx = watch::Sender::new(CollectorSignal::Retry { ready: false });

            let own_point_fut = match replay_bcasts.take() {
                Some((point, prev_bcast)) => {
                    if let Some(prev) = prev_bcast {
                        self.round_task.init_prev_broadcast(prev, round_ctx.clone());
                    }
                    future::ready(Some(point)).boxed()
                }
                None => self.round_task.own_point_task(
                    &head,
                    collector_signal_tx.subscribe(),
                    &round_ctx,
                ),
            };

            let round_task_run = self
                .round_task
                .run(own_point_fut, collector_signal_tx, &head, &round_ctx)
                .until_ready();

            if let Some(committer) = ready_committer {
                self.committer_run = committer_task(
                    committer,
                    full_history_bottom.take(),
                    self.committed_info_tx.clone(),
                    round_ctx.clone(),
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

fn collator_feedback(
    mut top_known_anchor_recv: RoundWatcher<TopKnownAnchor>,
    old_dag_top_round: Round,
    is_paused: &mut bool,
    committed_info_tx: &mpsc::UnboundedSender<MempoolOutput>,
    round_ctx: &RoundCtx,
) -> Result<Round, BoxFuture<'static, ()>> {
    let top_known_anchor = top_known_anchor_recv.get();
    // For example in `max_consensus_lag_rounds` comments this results to `217` of `8..=217`
    let pause_at = top_known_anchor + CachedConfig::get().consensus.max_consensus_lag_rounds;
    // Note pause bound is inclusive with `>=` because new vset may be unknown for next dag top
    //  (the next after next engine round), while vset switch round is exactly pause bound + 1
    if old_dag_top_round >= pause_at {
        if !*is_paused {
            tracing::info!(
                parent: round_ctx.span(),
                top_known_anchor = top_known_anchor.0,
                pause_at = pause_at.0,
                "enter pause by collator feedback",
            );
            *is_paused = true;
            committed_info_tx.send(MempoolOutput::Paused).ok();
        }

        let collator_sync = async move {
            loop {
                let top_known_anchor = top_known_anchor_recv.next().await;
                //  exit if ready to produce point: collator synced enough
                let pause_at =
                    top_known_anchor + CachedConfig::get().consensus.max_consensus_lag_rounds;
                let exit = old_dag_top_round < pause_at;
                tracing::debug!(
                    top_known_anchor = top_known_anchor.0,
                    pause_at = pause_at.0,
                    exit = exit,
                    "collator feedback in pause mode"
                );
                if exit {
                    break;
                }
            }
        };
        let task = tokio::time::timeout(
            Duration::from_millis(CachedConfig::get().consensus.broadcast_retry_millis as _),
            collator_sync.instrument(round_ctx.span().clone()),
        )
        .map(|_| ())
        .boxed();

        Err(task)
    } else if *is_paused {
        tracing::info!(
            parent: round_ctx.span(),
            top_known_anchor = top_known_anchor.0,
            pause_at = pause_at.0,
            "exit from pause by collator feedback",
        );
        *is_paused = false;
        committed_info_tx.send(MempoolOutput::Running).ok();
        Ok(pause_at)
    } else {
        Ok(pause_at)
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
    round_ctx: RoundCtx,
) -> JoinHandle<Committer> {
    tokio::task::spawn_blocking(move || {
        // may run for long several times in a row and commit nothing, because of missed points
        let _span = round_ctx.span().enter();

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
            round_ctx.log_committed(&committed);
            for data in committed {
                round_ctx.commit_metrics(&data.anchor);
                committed_info_tx
                    .send(MempoolOutput::NextAnchor(data)) // not recoverable
                    .expect("Failed to send anchor history info to mpsc channel");
            }
        }

        EngineCtx::meter_dag_len(committer.dag_len());

        committer
    })
}

impl RoundCtx {
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
