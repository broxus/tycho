use std::cmp;
use std::sync::Arc;
use std::time::Instant;
use futures_util::{FutureExt, future};
use tokio::sync::{oneshot, watch};
use crate::dag::{
    DagHead, LastOwnPoint, ProduceError, Producer, ValidateResult, Verifier, WeakDagRound,
};
use crate::effects::{
    AltFormat, CollectCtx, Ctx, RoundCtx, Task, TaskResult, ValidateCtx,
};
use crate::engine::MempoolConfig;
use crate::engine::input_buffer::InputBuffer;
use crate::engine::lifecycle::{EngineBinding, EngineNetwork};
use crate::engine::round_watch::{Consensus, RoundWatch, TopKnownAnchor};
use crate::intercom::{
    Broadcaster, BroadcasterSignal, Collector, CollectorStatus, Dispatcher, Downloader,
    PeerSchedule, Responder,
};
use crate::models::{Cert, Link, Point, PointInfo};
use crate::storage::MempoolStore;
pub struct RoundTaskState {
    pub store: MempoolStore,
    input_buffer: InputBuffer,
    pub top_known_anchor: RoundWatch<TopKnownAnchor>,
    pub consensus_round: RoundWatch<Consensus>,
    pub peer_schedule: PeerSchedule,
    dispatcher: Dispatcher,
    pub responder: Responder,
    pub downloader: Downloader,
}
impl RoundTaskState {
    pub fn init_responder(&self, head: &DagHead, round_ctx: &RoundCtx) {
        self.responder
            .init(
                &self.store,
                &self.consensus_round,
                &self.peer_schedule,
                &self.downloader,
                head,
                round_ctx,
                #[cfg(feature = "mock-feedback")]
                &self.top_known_anchor,
            );
    }
}
pub struct RoundTaskReady {
    pub state: RoundTaskState,
    last_own_point: Option<Arc<LastOwnPoint>>,
    prev_broadcast: Option<Task<()>>,
    pub collector: Collector,
}
impl RoundTaskReady {
    pub fn new(
        store: &MempoolStore,
        bind: &EngineBinding,
        consensus_round: &RoundWatch<Consensus>,
        net: &EngineNetwork,
        conf: &MempoolConfig,
    ) -> Self {
        let downloader = Downloader::new(
            &net.dispatcher,
            &net.peer_schedule,
            consensus_round.receiver(),
            conf,
        );
        Self {
            state: RoundTaskState {
                store: store.clone(),
                input_buffer: bind.input_buffer.clone(),
                top_known_anchor: bind.top_known_anchor.clone(),
                consensus_round: consensus_round.clone(),
                peer_schedule: net.peer_schedule.clone(),
                dispatcher: net.dispatcher.clone(),
                responder: net.responder.clone(),
                downloader,
            },
            collector: Collector::new(consensus_round.receiver()),
            last_own_point: None,
            prev_broadcast: None,
        }
    }
    fn init_prev_broadcast(&mut self, prev_last_point: Point, round_ctx: &RoundCtx) {
        assert!(self.prev_broadcast.is_none(), "previous broadcast is already set");
        let (bcaster_ready_tx, stub_rx) = oneshot::channel();
        let (stub_tx, collector_status_rx) = watch::channel(CollectorStatus {
            attempt: 0,
            ready: true,
        });
        let broadcaster = Broadcaster::new(
            self.state.dispatcher.clone(),
            prev_last_point,
            self.state.peer_schedule.clone(),
            bcaster_ready_tx,
            collector_status_rx,
            round_ctx,
        );
        let task_ctx = round_ctx.task();
        let round_ctx = round_ctx.clone();
        self.prev_broadcast = Some(
            task_ctx
                .spawn(async move {
                    let mut __guard = crate::__async_profile_guard__::Guard::new(
                        concat!(module_path!(), "::async_block"),
                        file!(),
                        109u32,
                    );
                    {
                        __guard.end_section(110u32);
                        let __result = broadcaster.run_continue(&round_ctx).await;
                        __guard.start_section(110u32);
                        __result
                    }?;
                    _ = stub_rx;
                    _ = stub_tx;
                    Ok(())
                }),
        );
    }
    async fn own_point_task(
        last_own_point: Option<Arc<LastOwnPoint>>,
        input_buffer: InputBuffer,
        store: MempoolStore,
        head: DagHead,
        mut collector_status_rx: watch::Receiver<CollectorStatus>,
        round_ctx: RoundCtx,
    ) -> TaskResult<Result<Point, ProduceError>> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(own_point_task)),
            file!(),
            124u32,
        );
        let last_own_point = last_own_point;
        let input_buffer = input_buffer;
        let store = store;
        let head = head;
        let mut collector_status_rx = collector_status_rx;
        let round_ctx = round_ctx;
        let allowed_to_produce = (last_own_point.as_ref())
            .is_none_or(|prev_own| {
                match prev_own.round.cmp(&head.prev().round()) {
                    cmp::Ordering::Less => true,
                    cmp::Ordering::Equal => {
                        prev_own.evidence.len() >= prev_own.signers.majority_of_others()
                    }
                    cmp::Ordering::Greater => {
                        let _guard = round_ctx.span().enter();
                        panic!(
                            "already produced point at {:?} and gathered {}/{} evidence, \
                             trying to produce point at {:?}",
                            prev_own.round, prev_own.evidence.len(), prev_own.signers
                            .majority_of_others(), head.current().round()
                        );
                    }
                }
            });
        if !allowed_to_produce {
            {
                __guard.end_section(146u32);
                return Ok(Err(ProduceError::NotAllowed));
            };
        }
        let mut threshold = std::pin::pin!(head.prev().threshold().reached());
        let is_in_time = loop {
            __guard.checkpoint(152u32);
            {
                __guard.end_section(153u32);
                tokio::select!(
                    () = & mut threshold => { break true; }, recv_status =
                    collector_status_rx.changed() => { if recv_status.is_err() ||
                    collector_status_rx.borrow().ready { break false; } }
                );
                __guard.start_section(153u32);
            };
        };
        if !is_in_time {
            {
                __guard.end_section(165u32);
                return Ok(Err(ProduceError::NextRoundThreshold));
            };
        }
        let task_start_time = Instant::now();
        let produced = (round_ctx.span())
            .in_scope(|| {
                Producer::new_point(
                    last_own_point.as_deref(),
                    &input_buffer,
                    &head,
                    round_ctx.conf(),
                )
            });
        let own_point = match produced {
            Ok(own_point) => own_point,
            Err(err) => {
                __guard.end_section(180u32);
                return Ok(Err(err));
            }
        };
        let wa_keys = head.keys().to_produce.as_ref();
        {
            __guard.end_section(189u32);
            let __result = (head.current())
                .add_local(&own_point, wa_keys, &store, &round_ctx)
                .await;
            __guard.start_section(189u32);
            __result
        }?;
        metrics::histogram!("tycho_mempool_engine_produce_time")
            .record(task_start_time.elapsed());
        Ok(Ok(own_point))
    }
    pub fn run(
        mut self,
        start_replay_bcasts: Option<(Point, Option<Point>)>,
        head: &DagHead,
        round_ctx: &RoundCtx,
    ) -> RoundTaskRunning {
        let (bcaster_ready_tx, bcaster_ready_rx) = oneshot::channel();
        self.state.responder.update(head, round_ctx);
        let collector_status_tx = watch::Sender::new(CollectorStatus::default());
        let broadcaster_run = round_ctx
            .task()
            .spawn({
                let produce_point_fut = match start_replay_bcasts {
                    Some((point, prev_bcast)) => {
                        if let Some(prev) = prev_bcast {
                            self.init_prev_broadcast(prev, round_ctx);
                        }
                        future::ready(Ok(Ok(point))).boxed()
                    }
                    None => {
                        Self::own_point_task(
                                self.last_own_point.clone(),
                                self.state.input_buffer.clone(),
                                self.state.store.clone(),
                                head.clone(),
                                collector_status_tx.subscribe(),
                                round_ctx.clone(),
                            )
                            .boxed()
                    }
                };
                let own_point_round = head.current().downgrade();
                let collector_status_rx = collector_status_tx.subscribe();
                let round_ctx = round_ctx.clone();
                let peer_schedule = self.state.peer_schedule.clone();
                let prev_bcast = self.prev_broadcast;
                let dispatcher = self.state.dispatcher.clone();
                let downloader = self.state.downloader.clone();
                let store = self.state.store.clone();
                async move {
                    let mut __guard = crate::__async_profile_guard__::Guard::new(
                        concat!(module_path!(), "::async_block"),
                        file!(),
                        236u32,
                    );
                    let own_point_result = {
                        __guard.end_section(237u32);
                        let __result = produce_point_fut.await;
                        __guard.start_section(237u32);
                        __result
                    }?;
                    round_ctx.own_point(own_point_result.as_ref().map(|p| p.info()));
                    if let Ok(own_point) = own_point_result {
                        let self_check = Self::expect_own_valid_point(
                            own_point_round,
                            own_point.clone(),
                            peer_schedule.clone(),
                            downloader,
                            store,
                            &round_ctx,
                        );
                        let mut broadcaster = Broadcaster::new(
                            dispatcher,
                            own_point,
                            peer_schedule,
                            bcaster_ready_tx,
                            collector_status_rx,
                            &round_ctx,
                        );
                        let new_last_own_point = {
                            __guard.end_section(256u32);
                            let __result = broadcaster.run().await;
                            __guard.start_section(256u32);
                            __result
                        }?;
                        drop(prev_bcast);
                        let task_ctx = round_ctx.task();
                        let round_ctx = round_ctx.clone();
                        let new_prev_bcast = task_ctx
                            .spawn(async move {
                                let mut __guard = crate::__async_profile_guard__::Guard::new(
                                    concat!(module_path!(), "::async_block"),
                                    file!(),
                                    261u32,
                                );
                                {
                                    __guard.end_section(261u32);
                                    let __result = broadcaster.run_continue(&round_ctx).await;
                                    __guard.start_section(261u32);
                                    __result
                                }
                            });
                        {
                            __guard.end_section(263u32);
                            let __result = self_check.await;
                            __guard.start_section(263u32);
                            __result
                        }?;
                        Ok(Some((new_prev_bcast, new_last_own_point)))
                    } else {
                        bcaster_ready_tx.send(BroadcasterSignal::Ok).ok();
                        drop(prev_bcast);
                        Ok(None)
                    }
                }
            });
        let collector_run = round_ctx
            .task()
            .spawn({
                let collector = self.collector;
                let consensus_round = self.state.consensus_round.clone();
                let collector_ctx = CollectCtx::new(round_ctx);
                let head = head.clone();
                async move {
                    let mut __guard = crate::__async_profile_guard__::Guard::new(
                        concat!(module_path!(), "::async_block"),
                        file!(),
                        279u32,
                    );
                    let next_round = head.next().round();
                    let collector = {
                        __guard.end_section(283u32);
                        let __result = collector
                            .run(
                                collector_ctx,
                                head,
                                collector_status_tx,
                                bcaster_ready_rx,
                            )
                            .await;
                        __guard.start_section(283u32);
                        __result
                    }?;
                    consensus_round.set_max(next_round);
                    Ok(collector)
                }
            });
        RoundTaskRunning {
            state: self.state,
            last_own_point: self.last_own_point,
            broadcaster_run,
            collector_run,
        }
    }
    fn expect_own_valid_point(
        point_round: WeakDagRound,
        point: Point,
        peer_schedule: PeerSchedule,
        downloader: Downloader,
        store: MempoolStore,
        round_ctx: &RoundCtx,
    ) -> Task<()> {
        let task_ctx = round_ctx.task();
        let round_ctx = round_ctx.clone();
        task_ctx
            .spawn(async move {
                let mut __guard = crate::__async_profile_guard__::Guard::new(
                    concat!(module_path!(), "::async_block"),
                    file!(),
                    307u32,
                );
                if let Err(error) = Verifier::verify(
                    point.info(),
                    &peer_schedule,
                    round_ctx.conf(),
                ) {
                    let _guard = round_ctx.span().enter();
                    panic!("Failed to verify own point: {error}, {point:?}")
                }
                let validate_ctx = ValidateCtx::new(&round_ctx, point.info());
                let validate = Verifier::validate(
                    point.info().clone(),
                    point_round,
                    downloader,
                    store,
                    Cert::default(),
                    validate_ctx,
                );
                match {
                    __guard.end_section(321u32);
                    let __result = validate.await;
                    __guard.start_section(321u32);
                    __result
                }? {
                    ValidateResult::Valid => Ok(()),
                    ValidateResult::Invalid(reason) => {
                        let _guard = round_ctx.span().enter();
                        panic!("Invalid own point: {reason} {point:?}")
                    }
                    ValidateResult::IllFormed(reason) => {
                        let _guard = round_ctx.span().enter();
                        panic!("Ill-formed own point: {reason} {point:?}")
                    }
                }
            })
    }
}
pub struct RoundTaskRunning {
    state: RoundTaskState,
    last_own_point: Option<Arc<LastOwnPoint>>,
    broadcaster_run: Task<Option<(Task<()>, Arc<LastOwnPoint>)>>,
    collector_run: Task<Collector>,
}
impl RoundTaskRunning {
    pub async fn until_ready(self) -> TaskResult<RoundTaskReady> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(until_ready)),
            file!(),
            344u32,
        );
        let (collector, bcast_result) = {
            __guard.end_section(345u32);
            let __result = tokio::try_join!(self.collector_run, self.broadcaster_run);
            __guard.start_section(345u32);
            __result
        }?;
        let (prev_broadcast, last_own_point) = match bcast_result {
            None => (None, self.last_own_point),
            Some((new_prev_bcast, new_last_own_point)) => {
                (Some(new_prev_bcast), Some(new_last_own_point))
            }
        };
        let ready = RoundTaskReady {
            state: self.state,
            collector,
            last_own_point,
            prev_broadcast,
        };
        Ok(ready)
    }
}
impl RoundCtx {
    pub fn own_point_time_skew(diff: f64) {
        metrics::gauge!("tycho_mempool_produced_point_time_skew").set(diff);
    }
    fn own_point(&self, own_point: Result<&PointInfo, &ProduceError>) {
        metrics::counter!("tycho_mempool_points_produced")
            .increment(own_point.is_ok() as _);
        let no_proof = own_point.is_ok_and(|info| info.evidence().is_empty());
        metrics::counter!("tycho_mempool_points_no_proof_produced")
            .increment(no_proof as _);
        let externals = own_point.map_or(0, |info| info.payload_len());
        metrics::counter!("tycho_mempool_point_payload_count").increment(externals as _);
        let payload_bytes = own_point.map_or(0, |info| info.payload_bytes());
        metrics::counter!("tycho_mempool_point_payload_bytes")
            .increment(payload_bytes as _);
        if own_point.is_err() {
            Self::own_point_time_skew(0.0);
        }
        match own_point {
            Ok(own_info) => {
                tracing::info!(
                    parent : self.span(), digest = display(own_info.digest().alt()),
                    externals, payload_bytes, is_proof = (own_info.anchor_proof() == &
                    Link::ToSelf).then_some(true), is_trigger = (own_info
                    .anchor_trigger() == & Link::ToSelf).then_some(true),
                    "produced point"
                );
                tracing::debug!(
                    parent : self.span(), ? own_point, "created point details"
                );
            }
            Err(reason) => {
                match reason {
                    ProduceError::NotAllowed | ProduceError::NotEnoughEvidence => {
                        tracing::warn!(
                            parent : self.span(), % reason, "produce point skipped"
                        );
                    }
                    ProduceError::NextRoundThreshold | ProduceError::NotScheduled => {
                        tracing::info!(
                            parent : self.span(), % reason, "produce point skipped"
                        );
                    }
                    ProduceError::PrevPointMismatch { .. } => {
                        tracing::error!(
                            parent : self.span(), % reason, "produce point skipped"
                        );
                    }
                };
                let label = match reason {
                    ProduceError::NotAllowed
                    | ProduceError::NotEnoughEvidence
                    | ProduceError::NextRoundThreshold => "late",
                    ProduceError::NotScheduled => "not in v_set",
                    ProduceError::PrevPointMismatch { .. } => "prev point",
                };
                metrics::counter!(
                    "tycho_mempool_engine_produce_skipped", "kind" => label
                )
                    .increment(1);
            }
        }
    }
}
