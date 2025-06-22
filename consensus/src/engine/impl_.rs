use std::cmp;
use std::collections::BTreeMap;
use std::ops::RangeInclusive;
use std::time::Duration;

use futures_util::future::BoxFuture;
use futures_util::never::Never;
use futures_util::stream::FuturesUnordered;
use futures_util::{FutureExt, TryStreamExt};
use itertools::{Either, Itertools};
use tokio::sync::mpsc;
use tycho_network::PeerId;
use tycho_util::metrics::HistogramGuard;

use crate::dag::{DagFront, DagRound, KeyGroup, Verifier};
use crate::effects::{
    AltFormat, Cancelled, Ctx, DbCleaner, EngineCtx, MempoolStore, RoundCtx, Task, TaskResult,
    TaskTracker,
};
use crate::engine::committer_task::CommitterTask;
use crate::engine::lifecycle::{EngineBinding, EngineError, EngineNetwork, FixHistoryFlag};
use crate::engine::round_task::RoundTaskReady;
use crate::engine::round_watch::{RoundWatch, RoundWatcher, TopKnownAnchor};
use crate::engine::{ConsensusConfigExt, MempoolMergedConfig};
use crate::models::{
    DagPoint, MempoolOutput, Point, PointRestore, PointRestoreSelect, PointStatusStoredRef, Round,
};
pub type EngineResult<T> = std::result::Result<T, EngineError>;

pub struct Engine {
    dag: DagFront,
    committer_run: CommitterTask,
    output: mpsc::UnboundedSender<MempoolOutput>,
    round_task: RoundTaskReady,
    db_cleaner: DbCleaner,
    peer_schedule_updater: Task<Never>,
    init_task: Option<Task<FixHistoryFlag>>,
    ctx: EngineCtx,
}

impl Engine {
    pub fn new(
        task_tracker: &TaskTracker,
        bind: &EngineBinding,
        net: &EngineNetwork,
        merged_conf: &MempoolMergedConfig,
        fix_history: FixHistoryFlag,
    ) -> Engine {
        let conf = &merged_conf.conf;
        let genesis = merged_conf.genesis();

        Point::parse(genesis.serialized().to_vec())
            .expect("parse genesis: point tl serde is broken")
            .expect("parse genesis: integrity check is broken");
        Verifier::verify(genesis.info(), &net.peer_schedule, conf)
            .expect("failed to verify genesis");

        let consensus_round = RoundWatch::default();
        consensus_round.set_max(conf.genesis_round);
        bind.top_known_anchor.set_max(conf.genesis_round);

        let engine_ctx = EngineCtx::new(consensus_round.get(), conf, task_tracker);
        let round_ctx = RoundCtx::new(&engine_ctx, Round::BOTTOM);

        let store = MempoolStore::new(&bind.mempool_adapter_store);
        let db_cleaner = DbCleaner::new(&bind.mempool_adapter_store);

        // Dag, created at genesis, will at first extend up to its greatest length
        // (in case last broadcast is within it) without data,
        // and may shrink to its medium len (in case broadcast or consensus are further)
        // before being filled with data
        let mut dag = DagFront::default();
        let mut committer = dag.init(
            DagRound::new_bottom(conf.genesis_round.prev(), &net.peer_schedule, conf),
            conf,
        );
        dag.fill_to_top(
            conf.genesis_round,
            Some(&mut committer),
            &net.peer_schedule,
            &round_ctx,
        );
        let committer_run = CommitterTask::new(committer, engine_ctx.conf());

        let init_task = engine_ctx.task().spawn_blocking({
            let store = store.clone();
            let overlay_id = merged_conf.overlay_id;
            move || {
                store.init_storage(&overlay_id);
                store.insert_point(&genesis, PointStatusStoredRef::Exists);
                fix_history // just pass further
            }
        });

        let round_task = RoundTaskReady::new(
            &net.dispatcher,
            net.peer_schedule.clone(),
            store,
            &consensus_round,
            bind.top_known_anchor.clone(),
            net.responder.clone(),
            bind.input_buffer.clone(),
        );

        let peer_schedule_updater = engine_ctx.task().spawn({
            let peer_schedule = net.peer_schedule.clone();
            peer_schedule.downgrade().run_updater()
        });

        Self {
            dag,
            committer_run,
            output: bind.output.clone(),
            db_cleaner,
            round_task,
            peer_schedule_updater,
            init_task: Some(init_task),
            ctx: engine_ctx,
        }
    }

    // restore last two rounds into dag, return the last own point among them to repeat broadcast
    async fn pre_run(&mut self) -> EngineResult<Option<(Point, Option<Point>)>> {
        let fix_history = (self.init_task.take().expect("init task must be set")).await?;

        // take last round from db

        let last_db_round = (self.ctx.task())
            .spawn_blocking({
                let store = self.round_task.state.store.clone();
                move || store.last_round()
            })
            .await?;

        self.ctx.update(last_db_round);
        tracing::info!(parent: self.ctx.span(), "rounds set since found last db round");
        let conf = self.ctx.conf();

        // wait collator to load blocks and update peer schedule

        let top_known_anchor = {
            let min_top_known_anchor = last_db_round - conf.consensus.max_consensus_lag_rounds;

            // NOTE collator have to apply mc state update to mempool first,
            //  and pass its top known anchor only after completion
            let mut top_known_anchor_recv = self.round_task.state.top_known_anchor.receiver();
            let mut top_known_anchor = top_known_anchor_recv.get();
            while top_known_anchor < min_top_known_anchor {
                tracing::info!(
                    parent: self.ctx.span(),
                    ?top_known_anchor,
                    ?min_top_known_anchor,
                    "waiting collator to load up to last top known anchor"
                );
                top_known_anchor = top_known_anchor_recv.next().await?;
            }
            top_known_anchor
        };

        // get ready to commit and return deduplicated top known anchor's history to collator

        let dag_top_round = last_db_round
            .max(top_known_anchor)
            .max(conf.genesis_round.next());
        self.round_task.state.consensus_round.set_max(dag_top_round);
        let round_ctx = RoundCtx::new(&self.ctx, dag_top_round);
        tracing::info!(parent: round_ctx.span(), "current round set to dag top");

        let dag_bottom_round = {
            let committer = self.committer_run.ready_mut().await?.expect("ready");
            (self.dag).fill_to_top(
                dag_top_round,
                Some(committer),
                &self.round_task.state.peer_schedule,
                &round_ctx,
            );
            _ = committer.drop_upto(
                (top_known_anchor - conf.consensus.replay_anchor_rounds()).max(conf.genesis_round),
                conf,
            );
            committer.bottom_round()
        };
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
            self.preload_points(range, fix_history, &round_ctx).await?;
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
                .transpose()?
                .map(|dag_point| dag_point.id());
            if let Some(last_id) = first_valid {
                tracing::info!(
                    parent: round_ctx.span(),
                    "found stored broadcast {:?}", last_id.alt()
                );
                last_broadcast = Some(last_id);
            }
        }

        // determine current round to place broadcasts (or produce if none found)
        let (new_top_round, replay_bcasts) = match last_broadcast {
            None => (dag_top_round, None),
            Some(last_id) if last_id.round == head_min_round => {
                // do not repeat such bcast @ r+0 because
                // * collected evidence are not stored so cannot produce point @ r+1
                // * not need to produce point @ r+1 because consensus moved to r+2
                (dag_top_round.next(), None)
            }
            Some(last_id) => {
                let task = round_ctx.task().spawn_blocking({
                    let store = self.round_task.state.store.clone();
                    move || {
                        let last = store
                            .get_point(last_id.round, &last_id.digest)
                            .expect("last bcast by id");
                        let prev = last.info().prev_id().map(|id| {
                            store
                                .get_point(id.round, &id.digest)
                                .expect("prev bcast by id")
                        });
                        (last, prev)
                    }
                });
                let (last, prev) = task.await?;
                // if there's a broadcast at two last DB rounds - then it's round will be current
                (last.info().round().next(), Some((last, prev)))
            }
        };
        (self.dag).fill_to_top(
            new_top_round,
            Some(self.committer_run.ready_mut().await?.expect("ready")),
            &self.round_task.state.peer_schedule,
            &round_ctx,
        );

        self.round_task.state.consensus_round.set_max(new_top_round);

        Ok(replay_bcasts)
    }

    async fn preload_points(
        &self,
        range: RangeInclusive<Round>,
        fix_history: FixHistoryFlag,
        round_ctx: &RoundCtx,
    ) -> TaskResult<()> {
        let task = round_ctx.task().spawn_blocking({
            let store = self.round_task.state.store.clone();
            let peer_schedule = self.round_task.state.peer_schedule.clone();
            let round_ctx = round_ctx.clone();
            move || {
                let _guard = round_ctx.span().enter();
                if fix_history.0 {
                    store.reset_statuses(&range);
                }
                let restores = store.load_restore(&range);
                let (need_verify, ready): (Vec<_>, Vec<_>) =
                    restores.into_iter().partition_map(|r| match r {
                        PointRestoreSelect::NeedsVerify(round, digest) => {
                            Either::Left((round, digest))
                        }
                        PointRestoreSelect::Ready(ready) => Either::Right(ready),
                    });
                let verified = need_verify
                    .chunks(1000) // seems enough for any case
                    .flat_map(|keys| {
                        use rayon::prelude::{IntoParallelIterator, ParallelIterator};
                        store
                            .multi_get_info(keys) // assume load result is sorted
                            .into_par_iter()
                            .map(|info| {
                                if Verifier::verify(&info, &peer_schedule, round_ctx.conf()).is_ok()
                                {
                                    // return back as they were, now with prev_proof filled
                                    PointRestore::Exists(info)
                                } else {
                                    PointRestore::IllFormed(info.id(), Default::default())
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

        let mut reversed_futures = BTreeMap::new();
        for ((round, order), point_restores) in task.await? {
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
            reversed_futures.insert((cmp::Reverse(round), order), dag_restore);
        }
        for dag_restore in reversed_futures.into_values() {
            dag_restore.try_collect::<Vec<DagPoint>>().await?;
        }
        Ok(())
    }

    // this future never completes with Ok result
    pub async fn run(mut self) -> EngineResult<Never> {
        // Option<Option<_>> to distinguish first round, when dag must not be advanced before run
        let mut start_replay_bcasts = Some(self.pre_run().await?);
        let in_guard = (
            self.round_task.state.responder.clone(),
            self.ctx.span().clone(),
        );
        let _scopeguard = scopeguard::guard(in_guard, |(responder, start_span)| {
            responder.dispose();
            start_span.in_scope(|| tracing::warn!("mempool engine run stopped"));
        });
        let mut round_ctx = RoundCtx::new(&self.ctx, self.dag.top().round());
        let db_clean_task: Task<Never> = self.db_cleaner.new_task(
            self.round_task.state.consensus_round.receiver(),
            self.round_task.state.top_known_anchor.receiver(),
            &round_ctx,
        );

        // Boxed for just not to move a Copy to other thread by mistake
        let mut full_history_bottom: Box<Option<Round>> = Box::new(None);
        let mut is_paused = true;
        loop {
            let _round_duration = HistogramGuard::begin("tycho_mempool_engine_round_time");
            // commit may take longer than a round if it ends with a jump to catch up with consensus

            if self.peer_schedule_updater.is_finished() {
                match self.peer_schedule_updater.await {
                    Err(e) => return Err(e.into()), // never Ok
                }
            }
            if db_clean_task.is_finished() {
                match db_clean_task.await {
                    Err(e) => return Err(e.into()), // never Ok
                }
            }

            {
                let (old_dag_top_round, next_round) = if start_replay_bcasts.is_some() {
                    let dag_top_round = self.dag.top().round();
                    // dag top round is already set; Note first round is never paused
                    (dag_top_round.prev(), dag_top_round)
                } else {
                    let old_dag_top_round = self.dag.top().round();
                    // do not repeat the `get()` - it can give non-reproducible result
                    let consensus_round = self.round_task.state.consensus_round.get();
                    assert!(
                        old_dag_top_round <= consensus_round,
                        "consensus round {} cannot be less than old top dag round {}",
                        consensus_round.0,
                        old_dag_top_round.0,
                    );
                    metrics::gauge!("tycho_mempool_rounds_dag_behind_consensus")
                        .set(consensus_round.diff_f64(old_dag_top_round));
                    // if received from BcastFilter - produce point at round before it
                    let next_round = consensus_round.max(old_dag_top_round.next());
                    (old_dag_top_round, next_round)
                };

                let dag_top_round = match collator_feedback(
                    self.round_task.state.top_known_anchor.receiver(),
                    old_dag_top_round,
                    &mut is_paused,
                    &self.output,
                    &round_ctx,
                ) {
                    Ok(pause_at) => next_round.min(pause_at),
                    Err(collator_sync) => {
                        collator_sync.await?;
                        let committer_update = self.committer_run.update_task(
                            full_history_bottom.take(),
                            self.output.clone(),
                            &round_ctx,
                        );
                        committer_update.await?;
                        continue;
                    }
                };

                if old_dag_top_round < dag_top_round.prev() {
                    self.ctx.update(dag_top_round.prev());
                }

                round_ctx = RoundCtx::new(&self.ctx, dag_top_round.prev());

                *full_history_bottom = full_history_bottom.or(self.dag.fill_to_top(
                    dag_top_round,
                    self.committer_run.ready_mut().await?,
                    &self.round_task.state.peer_schedule,
                    &round_ctx,
                ));

                assert!(
                    dag_top_round <= next_round,
                    "new dag round {} cannot be greater than next expected round {}",
                    dag_top_round.0,
                    next_round.0,
                );

                assert!(
                    old_dag_top_round < dag_top_round,
                    "new dag round {} must be greater than old one {}",
                    dag_top_round.0,
                    old_dag_top_round.0,
                );
            };

            let head = self.dag.head(&self.round_task.state.peer_schedule);
            metrics::gauge!("tycho_mempool_engine_current_round").set(head.current().round().0);

            let mut round_task_run = std::pin::pin!(self
                .round_task
                .run(start_replay_bcasts.take().flatten(), &head, &round_ctx)
                .until_ready());

            loop {
                tokio::select! {
                    _ = self.committer_run.interval.tick() => {
                        self.committer_run.update_task(
                            full_history_bottom.take(),
                            self.output.clone(),
                            &round_ctx,
                        ).await?;
                    },
                    round_task = &mut round_task_run => {
                        self.round_task = round_task?;
                        break;
                    },
                }
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
) -> Result<Round, BoxFuture<'static, TaskResult<()>>> {
    let top_known_anchor = top_known_anchor_recv.get();
    // For example in `max_consensus_lag_rounds` comments this results to `217` of `8..=217`
    let pause_at = top_known_anchor + round_ctx.conf().consensus.max_consensus_lag_rounds;
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

        let timeout = Duration::from_millis(round_ctx.conf().consensus.broadcast_retry_millis as _);

        let round_ctx = round_ctx.clone();
        let collator_sync = async move {
            loop {
                let top_known_anchor = top_known_anchor_recv.next().await?;
                //  exit if ready to produce point: collator synced enough
                let pause_at =
                    top_known_anchor + round_ctx.conf().consensus.max_consensus_lag_rounds;
                let exit = old_dag_top_round < pause_at;
                tracing::debug!(
                    parent: round_ctx.span(),
                    top_known_anchor = top_known_anchor.0,
                    pause_at = pause_at.0,
                    exit = exit,
                    "collator feedback in pause mode"
                );
                if exit {
                    break TaskResult::Ok(());
                }
            }
        };
        let timeout_or_sync =
            tokio::time::timeout(timeout, collator_sync).map(|result| match result {
                Ok(Ok(())) | Err(_) => Ok(()),
                Ok(Err(Cancelled())) => Err(Cancelled()),
            });
        Err(timeout_or_sync.boxed())
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
