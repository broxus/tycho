use std::cmp;
use std::sync::Arc;
use std::time::Instant;

use futures_util::future::BoxFuture;
use futures_util::{future, FutureExt};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::{AbortHandle, JoinError, JoinHandle};
use tracing::Instrument;
use tycho_util::futures::JoinTask;

use crate::dag::{DagHead, InclusionState, LastOwnPoint, Producer, Verifier, WeakDagRound};
use crate::effects::{
    AltFormat, CollectorContext, Effects, EngineContext, MempoolStore, ValidateContext,
};
use crate::engine::input_buffer::InputBuffer;
use crate::engine::round_watch::{Consensus, RoundWatch, TopKnownAnchor};
use crate::engine::Genesis;
use crate::intercom::{
    BroadcastFilter, Broadcaster, BroadcasterSignal, Collector, CollectorSignal, Dispatcher,
    Downloader, PeerSchedule, Responder,
};
use crate::models::{Link, Point, PointInfo, Round};

pub struct RoundTaskState {
    pub peer_schedule: PeerSchedule,
    pub store: MempoolStore,
    pub responder: Responder,
    pub top_known_anchor: RoundWatch<TopKnownAnchor>,
    input_buffer: InputBuffer,
    dispatcher: Dispatcher,
    pub broadcast_filter: BroadcastFilter,
    pub downloader: Downloader,
}

pub struct RoundTaskReady {
    pub state: RoundTaskState,
    // contains all collected signatures, even if they are insufficient to produce valid point;
    // may reference own point older than from a last round, as its payload may be not resend
    last_own_point: Option<Arc<LastOwnPoint>>,
    prev_broadcast: Option<AbortHandle>,
    pub collector: Collector,
}

impl RoundTaskReady {
    pub fn new(
        dispatcher: &Dispatcher,
        peer_schedule: PeerSchedule,
        store: MempoolStore,
        consensus_round: &RoundWatch<Consensus>,
        top_known_anchor: RoundWatch<TopKnownAnchor>,
        responder: Responder,
        input_buffer: InputBuffer,
    ) -> Self {
        let (bcast_tx, bcast_rx) = mpsc::unbounded_channel();
        let broadcast_filter = BroadcastFilter::new(&peer_schedule, consensus_round, bcast_tx);
        let downloader = Downloader::new(dispatcher, &peer_schedule, consensus_round.receiver());
        Self {
            state: RoundTaskState {
                peer_schedule,
                store,
                responder,
                top_known_anchor,
                input_buffer,
                dispatcher: dispatcher.clone(),
                broadcast_filter,
                downloader,
            },
            collector: Collector::new(bcast_rx),
            last_own_point: None,
            prev_broadcast: None,
        }
    }

    pub fn init_prev_broadcast(
        &mut self,
        prev_last_point: Point,
        round_effects: &Effects<EngineContext>,
    ) {
        assert!(
            self.prev_broadcast.is_none(),
            "previous broadcast is already set"
        );

        let (bcaster_ready_tx, stub_rx) = oneshot::channel();
        let (stub_tx, collector_signal_rx) = watch::channel(CollectorSignal::Finish);
        let broadcaster = Broadcaster::new(
            self.state.dispatcher.clone(),
            prev_last_point,
            self.state.peer_schedule.clone(),
            bcaster_ready_tx,
            collector_signal_rx,
            round_effects,
        );
        let task = async move {
            broadcaster.run_continue().await;
            _ = stub_rx;
            _ = stub_tx;
        };
        self.prev_broadcast = Some(tokio::spawn(task).abort_handle());
    }

    pub fn own_point_task(
        &self,
        head: &DagHead,
        round_effects: &Effects<EngineContext>,
    ) -> (
        BoxFuture<'static, Result<Option<Point>, JoinError>>,
        BoxFuture<'static, InclusionState>,
    ) {
        let current_round = head.current().round();

        metrics::gauge!("tycho_mempool_includes_ready_round_lag")
            .set(current_round.0 as f32 - self.collector.includes_ready_round().next().0 as f32);
        let (own_point_state_tx, own_point_state_rx) = oneshot::channel();
        let allowed_to_produce = match self
            .collector
            .includes_ready_round()
            .cmp(&current_round.prev().max(Genesis::round()))
        {
            cmp::Ordering::Less => false,
            cmp::Ordering::Equal => true,
            cmp::Ordering::Greater => panic!(
                "have includes ready at {:?}, but producing point at {:?}",
                self.collector.includes_ready_round(),
                current_round,
            ),
        } && self.last_own_point.as_ref().map_or(true, |prev_own| {
            match prev_own.round.next().cmp(&current_round) {
                cmp::Ordering::Less => true,
                cmp::Ordering::Equal => {
                    prev_own.evidence.len() >= prev_own.signers.majority_of_others()
                }
                cmp::Ordering::Greater => panic!(
                    "already produced point at {:?} and gathered {}/{} evidence, \
                     trying to produce point at {:?}",
                    prev_own.round,
                    prev_own.evidence.len(),
                    prev_own.signers.majority_of_others(),
                    current_round
                ),
            }
        });

        let point_fut = if allowed_to_produce {
            self.own_point_fut(own_point_state_tx, head.clone(), round_effects)
        } else {
            drop(own_point_state_tx);
            future::ready(Ok(None)).boxed()
        };
        let own_point_state = async move {
            match own_point_state_rx.await {
                Ok(state) => state,
                Err(_) => future::pending().await,
            }
        };
        (point_fut, own_point_state.boxed())
    }

    fn own_point_fut(
        &self,
        own_point_state_tx: oneshot::Sender<InclusionState>,
        head: DagHead,
        round_effects: &Effects<EngineContext>,
    ) -> BoxFuture<'static, Result<Option<Point>, JoinError>> {
        let input_buffer = self.state.input_buffer.clone();
        let last_own_point = self.last_own_point.clone();
        let store = self.state.store.clone();

        tokio::task::spawn_blocking(move || {
            let task_start_time = Instant::now();
            let point_opt = Producer::new_point(last_own_point.as_deref(), &input_buffer, &head);
            if let Some(own_point) = point_opt.as_ref() {
                // Note: actually we should use `.includes_keys()`, this is a WorkAround to support
                //   an assert inside `.insert_exact_sign()` to catch broader range of mistakes;
                //   this is safe as any node never changes its keys until restart, after which
                //   the node does not recognise points signed with old keypair as locally created
                let wa_keys = head.produce_keys();
                let state = head.current().insert_exact_sign(own_point, wa_keys, &store);
                own_point_state_tx.send(state).ok();
                metrics::histogram!("tycho_mempool_engine_produce_time")
                    .record(task_start_time.elapsed());
            };
            point_opt
            // if None: `drop(own_point_state_tx)`; it is moved and goes out of scope
        })
        .instrument(round_effects.span().clone())
        .boxed()
    }

    pub fn run(
        self,
        own_point_fut: BoxFuture<'static, Result<Option<Point>, JoinError>>,
        own_point_state: BoxFuture<'static, InclusionState>,
        collector_signal_tx: watch::Sender<CollectorSignal>,
        collector_signal_rx: watch::Receiver<CollectorSignal>,
        head: &DagHead,
        round_effects: &Effects<EngineContext>,
    ) -> RoundTaskRunning {
        let (bcaster_ready_tx, bcaster_ready_rx) = oneshot::channel();

        let broadcaster_run = tokio::spawn({
            let own_point_round = head.current().downgrade();
            let round_effects = round_effects.clone();
            let peer_schedule = self.state.peer_schedule.clone();
            let prev_bcast = self.prev_broadcast;
            let dispatcher = self.state.dispatcher.clone();
            let downloader = self.state.downloader.clone();
            let store = self.state.store.clone();
            async move {
                let own_point = own_point_fut.await.expect("cannot be cancelled");
                round_effects.own_point(own_point.as_ref());

                if let Some(own_point) = own_point {
                    let self_check = Self::expect_own_trusted_point(
                        own_point_round,
                        own_point.clone(),
                        peer_schedule.clone(),
                        downloader,
                        store,
                        round_effects.clone(),
                    );
                    let mut broadcaster = Broadcaster::new(
                        dispatcher,
                        own_point,
                        peer_schedule,
                        bcaster_ready_tx,
                        collector_signal_rx,
                        &round_effects,
                    );
                    let new_last_own_point = broadcaster.run().await;
                    prev_bcast.inspect(|task| task.abort());
                    let new_prev_bcast = tokio::spawn(broadcaster.run_continue()).abort_handle();
                    // join the check, just not to miss it; it must have completed already
                    self_check.await;
                    Some((new_prev_bcast, new_last_own_point))
                } else {
                    // drop(collector_signal_rx); // goes out of scope
                    bcaster_ready_tx.send(BroadcasterSignal::Ok).ok();
                    prev_bcast.inspect(|task| task.abort());
                    None
                }
            }
        });

        let collector_run = tokio::spawn({
            let mut collector = self.collector;
            let effects = Effects::<CollectorContext>::new(round_effects);
            let head = head.clone();
            async move {
                let next_round = collector
                    .run(
                        effects,
                        head,
                        own_point_state,
                        collector_signal_tx,
                        bcaster_ready_rx,
                    )
                    .await;
                (collector, next_round)
            }
        });

        self.state.responder.update(
            &self.state.broadcast_filter,
            head,
            &self.state.downloader,
            &self.state.store,
            round_effects,
        );

        RoundTaskRunning {
            state: self.state,
            last_own_point: self.last_own_point,
            broadcaster_run,
            collector_run,
        }
    }

    fn expect_own_trusted_point(
        point_round: WeakDagRound,
        point: Point,
        peer_schedule: PeerSchedule,
        downloader: Downloader,
        store: MempoolStore,
        effects: Effects<EngineContext>,
    ) -> JoinTask<()> {
        JoinTask::new(async move {
            point.verify_hash().expect("Failed to verify own point");

            if let Err(error) = Verifier::verify(&point, &peer_schedule) {
                let _guard = effects.span().enter();
                panic!("Failed to verify own point: {error}, {:?}", point)
            }
            let (_, do_not_certify_tx) = oneshot::channel();
            let info = PointInfo::from(&point);
            let validate_effects = Effects::<ValidateContext>::new(&effects, &info);
            let dag_point = Verifier::validate(
                info,
                point.prev_proof(),
                point_round,
                downloader,
                store,
                do_not_certify_tx,
                validate_effects,
            )
            .await;
            if dag_point.trusted().is_none() {
                let _guard = effects.span().enter();
                panic!(
                    "Failed to validate own point: {} {:?}",
                    dag_point.alt(),
                    point
                )
            };
        })
    }
}

pub struct RoundTaskRunning {
    state: RoundTaskState,
    last_own_point: Option<Arc<LastOwnPoint>>,
    broadcaster_run: JoinHandle<Option<(AbortHandle, Arc<LastOwnPoint>)>>,
    collector_run: JoinHandle<(Collector, Round)>,
}

impl RoundTaskRunning {
    pub async fn until_ready(self) -> Result<(RoundTaskReady, Round), JoinError> {
        match tokio::try_join!(self.collector_run, self.broadcaster_run) {
            Ok(((collector, next_round), bcast_result)) => {
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
                Ok((ready, next_round))
            }
            Err(join_error) => Err(join_error),
        }
    }
}

impl Effects<EngineContext> {
    fn own_point(&self, own_point: Option<&Point>) {
        // refresh counters with zeros every round
        metrics::counter!("tycho_mempool_engine_produce_skipped")
            .increment(own_point.is_none() as _);
        metrics::counter!("tycho_mempool_points_produced").increment(own_point.is_some() as _);

        let no_proof = own_point.map_or(false, |point| point.evidence().is_empty());
        metrics::counter!("tycho_mempool_points_no_proof_produced").increment(no_proof as _);

        metrics::counter!("tycho_mempool_point_payload_count")
            .increment(own_point.map_or(0, |point| point.payload().len() as _));
        let payload_bytes = own_point.map(|point| {
            point
                .payload()
                .iter()
                .fold(0, |acc, bytes| acc + bytes.len()) as _
        });
        metrics::counter!("tycho_mempool_point_payload_bytes")
            .increment(payload_bytes.unwrap_or_default());

        if let Some(own_point) = own_point {
            tracing::info!(
                parent: self.span(),
                digest = display(own_point.digest().alt()),
                payload_bytes = own_point
                    .payload().iter().map(|bytes| bytes.len()).sum::<usize>(),
                externals = own_point.payload().len(),
                is_proof = Some(own_point.data().anchor_proof == Link::ToSelf).filter(|x| *x),
                is_trigger = Some(own_point.data().anchor_trigger == Link::ToSelf).filter(|x| *x),
                "produced point"
            );
        } else {
            tracing::info!(parent: self.span(), "produce point skipped");
        };
    }
}
