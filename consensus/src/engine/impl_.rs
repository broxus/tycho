use std::collections::BTreeMap;
use std::mem;
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::time::Duration;

use everscale_crypto::ed25519::KeyPair;
use futures_util::future::BoxFuture;
use futures_util::stream::FuturesUnordered;
use futures_util::{future, FutureExt, StreamExt};
use itertools::{Either, Itertools};
use rayon::prelude::IntoParallelRefIterator;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tracing::Instrument;
use tycho_network::{Network, OverlayService, PeerId, PeerResolver, PrivateOverlay};
use tycho_util::futures::JoinTask;
use tycho_util::metrics::HistogramGuard;

use crate::dag::{Committer, DagFront, DagRound, KeyGroup, Verifier};
use crate::effects::{
    AltFormat, Ctx, DbCleaner, EngineCtx, MempoolAdapterStore, MempoolStore, RoundCtx,
};
use crate::engine::input_buffer::InputBuffer;
use crate::engine::round_task::RoundTaskReady;
use crate::engine::round_watch::{RoundWatch, RoundWatcher, TopKnownAnchor};
use crate::engine::{CachedConfig, ConsensusConfigExt, Genesis, MempoolConfig};
use crate::intercom::{CollectorSignal, Dispatcher, PeerSchedule, Responder};
use crate::models::{
    AnchorData, DagPoint, MempoolOutput, Point, PointInfo, PointRestore, PointStatusStoredRef,
    Round,
};

pub struct Engine {
    dag: DagFront,
    committer_run: JoinHandle<Committer>,
    committed_info_tx: mpsc::UnboundedSender<MempoolOutput>,
    round_task: RoundTaskReady,
    db_cleaner: DbCleaner,
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

        let store = MempoolStore::new(mempool_adapter_store);
        let db_cleaner = DbCleaner::new(mempool_adapter_store);

        // Dag, created at genesis, will at first extend up to its greatest length
        // (in case last broadcast is within it) without data,
        // and may shrink to its medium len (in case broadcast or consensus are further)
        // before being filled with data
        let mut dag = DagFront::default();
        let mut committer = dag.init(DagRound::new_bottom(
            Genesis::id().round.prev(),
            &peer_schedule,
        ));
        dag.fill_to_top(Genesis::id().round, Some(&mut committer), &peer_schedule);
        let committer_run = tokio::spawn(future::ready(committer));

        let init_task = JoinTask::new({
            let store = store.clone();
            async move {
                let init_storage_task = tokio::task::spawn_blocking({
                    move || {
                        store.init_storage(&overlay_id);
                        store.insert_point(&genesis, PointStatusStoredRef::Exists);
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
            db_cleaner,
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

        let dag_top_round = last_db_round
            .max(top_known_anchor)
            .max(Genesis::id().round.next());
        self.round_task.state.consensus_round.set_max(dag_top_round);
        let round_ctx = RoundCtx::new(&self.ctx, dag_top_round);

        let mut committer = take_committer(&mut self.committer_run).expect("init");
        (self.dag).fill_to_top(
            dag_top_round,
            Some(&mut committer),
            &self.round_task.state.peer_schedule,
        );
        _ = committer.drop_upto(
            (top_known_anchor - CachedConfig::get().consensus.replay_anchor_rounds())
                .max(Genesis::id().round),
        );
        let dag_bottom_round = committer.bottom_round();
        self.committer_run = tokio::spawn(future::ready(committer));

        // preload and sign last rounds if node may still participate in consensus

        if dag_bottom_round <= last_db_round {
            tracing::info!(
                parent: round_ctx.span(),
                dag_bottom_round = dag_bottom_round.0,
                last_db_round = last_db_round.0,
                dag_top_round = dag_top_round.0,
                "will restore points from DB",
            );
            let range = RangeInclusive::new(dag_bottom_round, last_db_round);
            self.preload_points(range, &round_ctx).await;
        } else {
            tracing::info!(
                parent: round_ctx.span(),
                last_db_round = last_db_round.0,
                dag_bottom_round = dag_bottom_round.0,
                dag_top_round = dag_top_round.0,
                "will not restore points from DB",
            );
        };

        // to repeat broadcasts from two last determined consensus rounds
        // in case DB was deleted and point received - find and use it

        let mut last_broadcast = None;
        // also last 3 rounds is enough to create point at last round with all witness deps
        let head_min_round = dag_top_round.prev().prev(); // 3 DagHead rounds inclusive
        for round in (head_min_round.0..=dag_top_round.0).map(Round) {
            let dag_round = self.dag.top().scan(round).expect("must exist");
            let keys = KeyGroup::new(round, &self.round_task.state.peer_schedule);
            let first_valid = keys
                .to_produce
                .as_deref()
                .map(|k| PeerId::from(k.public_key))
                .and_then(|local_id| dag_round.view(&local_id, |loc| loc.first_valid()))
                .flatten()
                .and_then(|dag_point_fut| dag_point_fut.now_or_never())
                .map(|dag_point| dag_point.id());
            if let Some(last_id) = first_valid {
                tracing::info!(
                    parent: round_ctx.span(),
                    "found stored broadcast {:?}", last_id.alt()
                );
                last_broadcast = Some(last_id);
            }
        }

        // determine current round to place broadcasts
        let (new_top_round, replay_bcasts) = match last_broadcast {
            None => (dag_top_round, None),
            Some(last_id) if last_id.round == head_min_round => {
                // do not repeat such bcast @ r+0 because
                // * collected evidence are not stored so cannot produce point @ r+1
                // * not need to produce point @ r+1 because consensus moved to r+2
                (dag_top_round.next(), None)
            }
            Some(last_id) => {
                let task = tokio::task::spawn_blocking({
                    let store = self.round_task.state.store.clone();
                    move || {
                        let last = store
                            .get_point(last_id.round, &last_id.digest)
                            .expect("last bcast by id");
                        let prev = last.prev_id().map(|id| {
                            store
                                .get_point(id.round, &id.digest)
                                .expect("prev bcast by id")
                        });
                        (last, prev)
                    }
                });
                // if there's a broadcast at two last DB rounds - then it's round will be current
                match task.await {
                    Ok((last, prev)) => (last.round().next(), Some((last, prev))),
                    Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
                    Err(e) => {
                        let _span = round_ctx.span().enter();
                        panic!("loading broadcasts on init: {e}")
                    }
                }
            }
        };
        self.round_task.state.consensus_round.set_max(new_top_round);

        replay_bcasts
    }

    async fn preload_points(&self, range: RangeInclusive<Round>, round_ctx: &RoundCtx) {
        let task = tokio::task::spawn_blocking({
            let store = self.round_task.state.store.clone();
            let peer_schedule = self.round_task.state.peer_schedule.clone();
            let round_ctx = round_ctx.clone();
            move || {
                let _guard = round_ctx.span().enter();
                let restores = store.load_restore(&range);
                let (need_verify, ready): (Vec<_>, Vec<_>) =
                    restores.into_iter().partition_map(|r| match r {
                        PointRestore::Exists(info, _) => {
                            Either::Left((info.round(), *info.digest()))
                        }
                        other => Either::Right(other),
                    });
                let verified = need_verify
                    .chunks(1000) // seems enough for any case
                    .flat_map(|keys| {
                        use rayon::iter::ParallelIterator;
                        store
                            .multi_get_points(keys) // assume load result is sorted
                            .par_iter()
                            .map(|point| {
                                let is_ok = point.verify_hash().is_ok()
                                    && Verifier::verify(point, &peer_schedule).is_ok();
                                if is_ok {
                                    // return back as they were, now with prev_proof filled
                                    PointRestore::Exists(point.into(), point.prev_proof())
                                } else {
                                    PointRestore::IllFormed(point.id(), Default::default())
                                }
                            })
                            .collect::<Vec<_>>()
                    });

                let mut map = BTreeMap::<(Round, _), Vec<PointRestore>>::new();
                for pre in verified.chain(ready) {
                    let entry = map.entry((pre.round(), pre.restore_order_asc()));
                    entry.or_default().push(pre);
                }
                map
            }
        });
        let preloaded = match task.await {
            Ok(result) => result,
            Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
            Err(e) => {
                let _span = round_ctx.span().enter();
                panic!("preload last rounds on init: {e}");
            }
        };

        for ((round, _), point_restores) in preloaded {
            let dag_round = self.dag.top().scan(round).expect("must exist");
            let dag_restore = FuturesUnordered::new();
            for point_restore in point_restores {
                dag_restore.push(dag_round.restore(
                    point_restore,
                    &self.round_task.state.downloader,
                    &self.round_task.state.store,
                    round_ctx,
                ));
            }
            dag_restore.collect::<Vec<DagPoint>>().await;
        }
    }

    pub async fn run(mut self) {
        let mut replay_bcasts = self.pre_run().await;
        let _db_clean_task = self.db_cleaner.new_task(
            self.round_task.state.consensus_round.receiver(),
            self.round_task.state.top_known_anchor.receiver(),
        );

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

                let next_round = if let Some((start_point, _)) = &replay_bcasts {
                    let consensus_round = self.round_task.state.consensus_round.get();
                    let new_top_round = consensus_round.max(old_dag_top_round);
                    assert_eq!(
                        start_point.round(),
                        new_top_round.prev(),
                        "start point must be at current round"
                    );
                    new_top_round
                } else {
                    // do not repeat the `get()` - it can give non-reproducible result
                    let consensus_round = self.round_task.state.consensus_round.get();
                    assert!(
                        old_dag_top_round <= consensus_round,
                        "consensus round {} cannot be less than old top dag round {}",
                        consensus_round.0,
                        old_dag_top_round.0,
                    );
                    metrics::gauge!("tycho_mempool_rounds_dag_behind_consensus")
                        .increment(consensus_round - old_dag_top_round);
                    // if received from BcastFilter - produce point at round before it
                    consensus_round.max(old_dag_top_round.next())
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
                            next_round.min(pause_at),
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
                    dag_top_round <= next_round,
                    "new dag round {} cannot be greater than next expected round {}",
                    dag_top_round.0,
                    next_round.0,
                );

                if replay_bcasts.is_none() {
                    assert!(
                        old_dag_top_round < dag_top_round,
                        "new dag round {} must be greater than old one {}",
                        dag_top_round.0,
                        old_dag_top_round.0,
                    );
                }

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
                        self.round_task.init_prev_broadcast(prev, &round_ctx);
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
