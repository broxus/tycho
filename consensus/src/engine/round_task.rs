use std::cmp;
use std::sync::Arc;
use std::time::Instant;

use futures_util::{FutureExt, future};
use tokio::sync::{oneshot, watch};

use crate::dag::{
    DagHead, LastOwnPoint, ProduceError, Producer, ValidateResult, Verifier, WeakDagRound,
};
use crate::effects::{AltFormat, CollectCtx, Ctx, RoundCtx, Task, TaskResult, ValidateCtx};
use crate::engine::MempoolConfig;
use crate::engine::input_buffer::InputBuffer;
use crate::engine::lifecycle::{EngineBinding, EngineNetwork};
use crate::engine::round_watch::{Consensus, RoundWatch, TopKnownAnchor};
use crate::intercom::{
    BroadcastFilter, Broadcaster, BroadcasterSignal, Collector, CollectorStatus, Dispatcher,
    Downloader, PeerSchedule, Responder,
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
    pub broadcast_filter: BroadcastFilter,
    pub downloader: Downloader,
}

pub struct RoundTaskReady {
    pub state: RoundTaskState,
    // contains all collected signatures, even if they are insufficient to produce valid point;
    // may reference own point older than from a last round, as its payload may be not resend
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
        let broadcast_filter = BroadcastFilter::new(&net.peer_schedule, consensus_round);
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
                broadcast_filter,
                downloader,
            },
            collector: Collector::new(consensus_round.receiver()),
            last_own_point: None,
            prev_broadcast: None,
        }
    }

    fn init_prev_broadcast(&mut self, prev_last_point: Point, round_ctx: &RoundCtx) {
        assert!(
            self.prev_broadcast.is_none(),
            "previous broadcast is already set"
        );

        let (bcaster_ready_tx, stub_rx) = oneshot::channel();
        let (stub_tx, collector_status_rx) = watch::channel(CollectorStatus {
            attempt: 0,  // default
            ready: true, // make broadcaster to resume its work not waiting for collector
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
        self.prev_broadcast = Some(task_ctx.spawn(async move {
            broadcaster.run_continue(&round_ctx).await?;
            _ = stub_rx;
            _ = stub_tx;
            Ok(())
        }));
    }

    async fn own_point_task(
        last_own_point: Option<Arc<LastOwnPoint>>,
        input_buffer: InputBuffer,
        store: MempoolStore,
        head: DagHead,
        mut collector_status_rx: watch::Receiver<CollectorStatus>,
        round_ctx: RoundCtx,
    ) -> TaskResult<Result<Point, ProduceError>> {
        let allowed_to_produce = (last_own_point.as_ref()).is_none_or(|prev_own| {
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
                        prev_own.round,
                        prev_own.evidence.len(),
                        prev_own.signers.majority_of_others(),
                        head.current().round()
                    );
                }
            }
        });

        if !allowed_to_produce {
            return Ok(Err(ProduceError::NotAllowed));
        }

        // this part must always be lazy: not started until polled

        let mut threshold = std::pin::pin!(head.prev().threshold().reached());
        let is_in_time = loop {
            tokio::select!(
                () = &mut threshold => {
                    break true;
                },
                recv_status = collector_status_rx.changed() => {
                    if recv_status.is_err() || collector_status_rx.borrow().ready {
                        break false;
                    }
                }
            );
        };
        if !is_in_time {
            return Ok(Err(ProduceError::NextRoundThreshold));
        }

        let task_start_time = Instant::now();

        let produced = (round_ctx.span()).in_scope(|| {
            Producer::new_point(
                last_own_point.as_deref(),
                &input_buffer,
                &head,
                round_ctx.conf(),
            )
        });
        let own_point = match produced {
            Ok(own_point) => own_point,
            Err(err) => return Ok(Err(err)),
        };
        // Note: actually we should use `keys().to_include`, this is a WorkAround to support
        //   an assert inside `.insert_exact_sign()` to catch broader range of mistakes;
        //   this is safe as any node never changes its keys until restart, after which
        //   the node does not recognise points signed with old keypair as locally created
        let wa_keys = head.keys().to_produce.as_ref();
        (head.current())
            .add_local(&own_point, wa_keys, &store, &round_ctx)
            .await?;
        metrics::histogram!("tycho_mempool_engine_produce_time").record(task_start_time.elapsed());
        Ok(Ok(own_point))
    }

    pub fn run(
        mut self,
        start_replay_bcasts: Option<(Point, Option<Point>)>,
        head: &DagHead,
        round_ctx: &RoundCtx,
    ) -> RoundTaskRunning {
        let (bcaster_ready_tx, bcaster_ready_rx) = oneshot::channel();

        // Signer must stop making new signatures for witness round before new point is produced
        // own point future must do nothing until polled (must not be spawned)
        self.state.responder.update(
            &self.state.store,
            &self.state.broadcast_filter,
            &self.state.downloader,
            head,
            round_ctx,
        );

        let collector_status_tx = watch::Sender::new(CollectorStatus::default());

        let broadcaster_run = round_ctx.task().spawn({
            let produce_point_fut = match start_replay_bcasts {
                Some((point, prev_bcast)) => {
                    if let Some(prev) = prev_bcast {
                        self.init_prev_broadcast(prev, round_ctx);
                    }
                    future::ready(Ok(Ok(point))).boxed()
                }
                None => Self::own_point_task(
                    self.last_own_point.clone(),
                    self.state.input_buffer.clone(),
                    self.state.store.clone(),
                    head.clone(),
                    collector_status_tx.subscribe(),
                    round_ctx.clone(),
                )
                .boxed(),
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
                let own_point_result = produce_point_fut.await?;
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
                    let new_last_own_point = broadcaster.run().await?;
                    drop(prev_bcast); // aborts after current broadcast finishes
                    let task_ctx = round_ctx.task();
                    let round_ctx = round_ctx.clone();
                    let new_prev_bcast =
                        task_ctx.spawn(async move { broadcaster.run_continue(&round_ctx).await });
                    // join the check, just not to miss it; it must have completed already
                    self_check.await?;
                    Ok(Some((new_prev_bcast, new_last_own_point)))
                } else {
                    // drop(collector_signal_rx); // goes out of scope
                    bcaster_ready_tx.send(BroadcasterSignal::Ok).ok();
                    drop(prev_bcast); // aborts immediately: don't keep for more than 1 round
                    Ok(None)
                }
            }
        });

        let collector_run = round_ctx.task().spawn({
            let collector = self.collector;
            let consensus_round = self.state.consensus_round.clone();
            let collector_ctx = CollectCtx::new(round_ctx);
            let head = head.clone();
            async move {
                let next_round = head.next().round();
                let collector = collector
                    .run(collector_ctx, head, collector_status_tx, bcaster_ready_rx)
                    .await?;
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
        task_ctx.spawn(async move {
            if let Err(error) = Verifier::verify(point.info(), &peer_schedule, round_ctx.conf()) {
                let _guard = round_ctx.span().enter();
                panic!("Failed to verify own point: {error}, {point:?}")
            }
            let validate_ctx = ValidateCtx::new(&round_ctx, point.info());
            let validate = Verifier::validate(
                point.info().clone(),
                point_round,
                downloader,
                store,
                Cert::default(), // off-line check that does not affect DAG
                validate_ctx,
            );
            match validate.await? {
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
        let (collector, bcast_result) = tokio::try_join!(self.collector_run, self.broadcaster_run)?;
        let (prev_broadcast, last_own_point) = match bcast_result {
            None => (None, self.last_own_point),
            Some((new_prev_bcast, new_last_own_point)) => {
                (Some(new_prev_bcast), Some(new_last_own_point))
            }
        };
        let ready = RoundTaskReady {
            state: self.state,
            collector,
            // do not reset to None, Producer decides whether to use old value or not
            last_own_point, // replaces prev point only when there is new one
            prev_broadcast, // continue prev broadcast for one adjacent round
        };
        Ok(ready)
    }
}

impl RoundCtx {
    pub fn own_point_time_skew(diff: f64) {
        metrics::gauge!("tycho_mempool_produced_point_time_skew").set(diff);
    }

    fn own_point(&self, own_point: Result<&PointInfo, &ProduceError>) {
        metrics::counter!("tycho_mempool_points_produced").increment(own_point.is_ok() as _);

        let no_proof = own_point.is_ok_and(|info| info.evidence().is_empty());
        metrics::counter!("tycho_mempool_points_no_proof_produced").increment(no_proof as _);

        let externals = own_point.map_or(0, |info| info.payload_len());
        metrics::counter!("tycho_mempool_point_payload_count").increment(externals as _);

        let payload_bytes = own_point.map_or(0, |info| info.payload_bytes());
        metrics::counter!("tycho_mempool_point_payload_bytes").increment(payload_bytes as _);

        if own_point.is_err() {
            // if point is produced, then method is called immediately when its time is known
            Self::own_point_time_skew(0.0);
        }

        match own_point {
            Ok(own_info) => {
                tracing::info!(
                    parent: self.span(),
                    digest = display(own_info.digest().alt()),
                    externals,
                    payload_bytes,
                    is_proof = (own_info.anchor_proof() == &Link::ToSelf).then_some(true),
                    is_trigger = (own_info.anchor_trigger() == &Link::ToSelf).then_some(true),
                    "produced point"
                );
                tracing::debug!(
                    parent: self.span(),
                    ?own_point,
                    "created point details"
                );
            }
            Err(reason) => {
                match reason {
                    ProduceError::NotAllowed | ProduceError::NotEnoughEvidence => {
                        tracing::warn!(parent: self.span(), %reason, "produce point skipped");
                    }
                    ProduceError::NextRoundThreshold | ProduceError::NotScheduled => {
                        tracing::info!(parent: self.span(), %reason, "produce point skipped");
                    }
                    ProduceError::PrevPointMismatch { .. } => {
                        tracing::error!(parent: self.span(), %reason, "produce point skipped");
                    }
                };
                let label = match reason {
                    ProduceError::NotAllowed
                    | ProduceError::NotEnoughEvidence
                    | ProduceError::NextRoundThreshold => "late",
                    ProduceError::NotScheduled => "not in v_set",
                    ProduceError::PrevPointMismatch { .. } => "prev point",
                };
                metrics::counter!("tycho_mempool_engine_produce_skipped", "kind" => label)
                    .increment(1);
            }
        }
    }
}
