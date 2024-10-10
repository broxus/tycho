use std::sync::Arc;
use std::time::Instant;

use futures_util::future::BoxFuture;
use futures_util::{future, FutureExt, TryFutureExt};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::{JoinError, JoinHandle};
use tracing::Instrument;

use crate::dag::{DagRound, InclusionState, LastOwnPoint, Producer, Verifier, WeakDagRound};
use crate::effects::{
    AltFormat, CollectorContext, Effects, EngineContext, MempoolStore, ValidateContext,
};
use crate::engine::input_buffer::InputBuffer;
use crate::engine::round_watch::{Consensus, RoundWatch, TopKnownAnchor};
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
    pub broadcast_filter: BroadcastFilter,
    pub downloader: Downloader,
}

pub struct RoundTaskReady {
    pub state: RoundTaskState,
    // contains all collected signatures, even if they are insufficient to produce valid point;
    // may reference own point older than from a last round, as its payload may be not resend
    last_own_point: Option<Arc<LastOwnPoint>>,
    broadcaster: Broadcaster,
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
        tokio::spawn({
            let this = broadcast_filter.clone();
            async move {
                this.clear_cache().await;
            }
        });
        let downloader = Downloader::new(dispatcher, &peer_schedule, consensus_round.receiver());
        Self {
            state: RoundTaskState {
                peer_schedule,
                store,
                responder,
                top_known_anchor,
                input_buffer,
                broadcast_filter,
                downloader,
            },
            broadcaster: Broadcaster::new(dispatcher),
            collector: Collector::new(bcast_rx),
            last_own_point: None,
        }
    }

    pub fn own_point_task(
        &self,
        prev_round_ok: bool,
        collector_signal_rx: &watch::Receiver<CollectorSignal>,
        current_dag_round: &DagRound,
        round_effects: &Effects<EngineContext>,
    ) -> (
        BoxFuture<'static, Result<Option<Point>, JoinError>>,
        BoxFuture<'static, InclusionState>,
    ) {
        let (own_point_state_tx, own_point_state_rx) = oneshot::channel();
        let point_fut = if prev_round_ok {
            self.own_point_fut(
                own_point_state_tx,
                collector_signal_rx,
                current_dag_round,
                round_effects,
            )
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
        collector_signal_rx: &watch::Receiver<CollectorSignal>,
        current_dag_round: &DagRound,
        round_effects: &Effects<EngineContext>,
    ) -> BoxFuture<'static, Result<Option<Point>, JoinError>> {
        let consensus_round = current_dag_round.round(); // latest reliably detected consensus round
        let mut top_known_anchor_recv = self.state.top_known_anchor.receiver();
        let top_known_anchor = top_known_anchor_recv.get();
        let silence_upper_bound = TopKnownAnchor::silence_upper_bound(top_known_anchor);
        #[allow(clippy::overly_complex_bool_expr)] // Fixme temporarily disable silent mode
        let wait_collator_ready = if true || consensus_round > silence_upper_bound {
            future::Either::Right(future::ready(Ok(true))) // ready; Ok for `JoinError`
        } else {
            tracing::info!(
                parent: round_effects.span(),
                top_known_anchor = top_known_anchor.0,
                silence_upper_bound = silence_upper_bound.0,
                "enter silent mode by collator feedback",
            );
            // must cancel on collector finish/err signal
            let mut collector_signal_rx = collector_signal_rx.clone();
            future::Either::Left(tokio::spawn(async move {
                loop {
                    tokio::select! {
                        top_known_anchor = top_known_anchor_recv.next() => {
                            //  exit if ready to produce point: collator synced enough
                            let silence_upper_bound = TopKnownAnchor::silence_upper_bound(top_known_anchor);
                            let exit = consensus_round > silence_upper_bound;
                            tracing::info!(
                                top_known_anchor = top_known_anchor.0,
                                silence_upper_bound = silence_upper_bound.0,
                                exit = exit,
                                "collator feedback in silent mode"
                            );
                            if exit {
                                break true;
                            }
                        },
                        collector_signal = collector_signal_rx.changed() => {
                            match collector_signal {
                                Ok(()) => {
                                    match *collector_signal_rx.borrow_and_update() {
                                        CollectorSignal::Finish | CollectorSignal::Err => break false,
                                        CollectorSignal::Retry => {}
                                    };
                                }
                                Err(_collector_exited) => break false,
                            }
                        }
                    }
                }
            }).instrument(round_effects.span().clone()))
        };

        let current_dag_round = current_dag_round.clone();
        let input_buffer = self.state.input_buffer.clone();
        let last_own_point = self.last_own_point.clone();
        let store = self.state.store.clone();

        wait_collator_ready
            .and_then(|is_ready_to_produce| {
                if is_ready_to_produce {
                    future::Either::Right(tokio::task::spawn_blocking(move || {
                        let task_start_time = Instant::now();
                        let point_opt = Producer::new_point(
                            &current_dag_round,
                            last_own_point.as_deref(),
                            &input_buffer,
                        );
                        if let Some(own_point) = point_opt.as_ref() {
                            let state = current_dag_round.insert_exact_sign(
                                own_point,
                                current_dag_round.key_pair(),
                                &store,
                            );
                            own_point_state_tx.send(state).ok();
                            metrics::histogram!("tycho_mempool_engine_produce_time")
                                .record(task_start_time.elapsed());
                        };
                        point_opt
                        // if None: `drop(own_point_state_tx)`; it is moved and goes out of scope
                    }))
                } else {
                    future::Either::Left(future::ready(Ok(None)))
                }
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
        next_dag_round: &DagRound,
        round_effects: &Effects<EngineContext>,
    ) -> RoundTaskRunning {
        let (bcaster_ready_tx, bcaster_ready_rx) = oneshot::channel();

        let broadcaster_run = tokio::spawn({
            let own_point_round = next_dag_round.prev().clone();
            let round_effects = round_effects.clone();
            let mut broadcaster = self.broadcaster;
            let peer_schedule = self.state.peer_schedule.clone();
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
                    let new_last_own_point = broadcaster
                        .run(
                            &round_effects,
                            &own_point,
                            &peer_schedule,
                            bcaster_ready_tx,
                            collector_signal_rx,
                        )
                        .await;
                    // join the check, just not to miss it; it must have completed already
                    self_check.await.expect("verify own produced point");
                    (broadcaster, Some(new_last_own_point))
                } else {
                    // drop(collector_signal_rx); // goes out of scope
                    bcaster_ready_tx.send(BroadcasterSignal::Ok).ok();
                    (broadcaster, None)
                }
            }
        });

        let collector_run = tokio::spawn({
            let mut collector = self.collector;
            let effects = Effects::<CollectorContext>::new(round_effects);
            let next_dag_round = next_dag_round.clone();
            async move {
                let next_round = collector
                    .run(
                        effects,
                        next_dag_round,
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
            Some(next_dag_round),
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
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            if !point.verify_hash() {
                panic!("Failed to verify own point hash");
            }
            if let Err(err) = Verifier::verify(&point, &peer_schedule) {
                let _guard = effects.span().enter();
                panic!("Failed to verify own point: {err:?} {:?}", point)
            }
            let (_, do_not_certify_tx) = oneshot::channel();
            let info = PointInfo::from(&point);
            let validate_effects = Effects::<ValidateContext>::new(&effects, &info);
            let dag_point = Verifier::validate(
                info,
                point.prev_proof(),
                point_round,
                false, // to be sure
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
    broadcaster_run: JoinHandle<(Broadcaster, Option<LastOwnPoint>)>,
    collector_run: JoinHandle<(Collector, Round)>,
}

impl RoundTaskRunning {
    pub async fn until_ready(mut self) -> Result<(RoundTaskReady, Round), JoinError> {
        match tokio::try_join!(self.collector_run, self.broadcaster_run) {
            Ok(((collector, next_round), (broadcaster, new_last_own_point))) => {
                // do not reset to None, Producer decides whether to use old value or not
                if let Some(new_last_own_point) = new_last_own_point {
                    // value is returned from the task without Arc for the sake of code clarity
                    self.last_own_point = Some(Arc::new(new_last_own_point));
                }
                let ready = RoundTaskReady {
                    state: self.state,
                    collector,
                    broadcaster,
                    last_own_point: self.last_own_point,
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
            tracing::info!(parent: self.span(), "will not produce point");
        };
    }
}
