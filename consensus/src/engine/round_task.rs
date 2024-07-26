use std::sync::Arc;
use std::time::Instant;
use std::{iter, panic};

use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::Instrument;
use tycho_util::sync::rayon_run;

use crate::dag::{DagRound, InclusionState, LastOwnPoint, Producer, Verifier, WeakDagRound};
use crate::effects::{AltFormat, CollectorContext, Effects, EngineContext, ValidateContext};
use crate::engine::input_buffer::InputBuffer;
use crate::intercom::{
    BroadcastFilter, Broadcaster, BroadcasterSignal, Collector, Dispatcher, Downloader,
    PeerSchedule, Responder,
};
use crate::models::{ConsensusRound, Link, Point, Round};

struct RoundTaskState {
    responder: Responder,
    downloader: Downloader,
    broadcast_filter: BroadcastFilter,
    input_buffer: InputBuffer,
    peer_schedule: PeerSchedule,
}

pub struct RoundTaskReady {
    state: RoundTaskState,
    // contains all collected signatures, even if they are insufficient to produce valid point;
    // may reference own point older than from a last round, as its payload may be not resend
    last_own_point: Option<Arc<LastOwnPoint>>,
    broadcaster: Broadcaster,
    collector: Collector,
}

impl RoundTaskReady {
    pub fn new(
        peer_schedule: &PeerSchedule,
        consensus_round: &ConsensusRound,
        dispatcher: &Dispatcher,
        responder: Responder,
        input_buffer: InputBuffer,
    ) -> Self {
        let (bcast_tx, bcast_rx) = mpsc::unbounded_channel();
        let broadcast_filter =
            BroadcastFilter::new(peer_schedule.clone(), bcast_tx, consensus_round.clone());
        tokio::spawn({
            let this = broadcast_filter.clone();
            async move {
                this.clear_cache().await;
            }
        });

        Self {
            state: RoundTaskState {
                responder,
                downloader: Downloader::new(dispatcher, peer_schedule),
                broadcast_filter,
                input_buffer,
                peer_schedule: peer_schedule.clone(),
            },
            broadcaster: Broadcaster::new(dispatcher),
            collector: Collector::new(bcast_rx),
            last_own_point: None,
        }
    }

    pub fn init(&mut self, next_round: Round, genesis_state: InclusionState) {
        self.collector.init(next_round, iter::once(genesis_state));
    }

    pub fn run(
        self,
        prev_round_ok: bool,
        current_dag_round: &DagRound,
        next_dag_round: &DagRound,
        round_effects: &Effects<EngineContext>,
    ) -> RoundTaskRunning {
        let (bcaster_ready_tx, bcaster_ready_rx) = oneshot::channel();
        // let this channel unbounded - there won't be many items, but every of them is essential
        let (collector_signal_tx, mut collector_signal_rx) = mpsc::unbounded_channel();
        let (own_point_state_tx, own_point_state_rx) = oneshot::channel();

        let own_point_fut = if prev_round_ok {
            let current_dag_round = current_dag_round.clone();
            let input_buffer = self.state.input_buffer.clone();
            let last_own_point = self.last_own_point.clone();
            futures_util::future::Either::Right(
                rayon_run(move || {
                    let task_start_time = Instant::now();
                    Producer::new_point(
                        &current_dag_round,
                        last_own_point.as_deref(),
                        &input_buffer,
                    )
                    .inspect(|own_point| {
                        let state = current_dag_round
                            .insert_exact_sign(own_point, current_dag_round.key_pair());
                        own_point_state_tx.send(state).ok();
                        metrics::histogram!(EngineContext::PRODUCE_POINT_DURATION)
                            .record(task_start_time.elapsed());
                    })
                    // if None: `drop(own_point_state_tx)`; it is moved and goes out of scope
                })
                .instrument(round_effects.span().clone()),
            )
        } else {
            drop(own_point_state_tx);
            futures_util::future::Either::Left(futures_util::future::ready(None::<Point>))
        };

        let broadcaster_run = tokio::spawn({
            let own_point_round = current_dag_round.downgrade();
            let round_effects = round_effects.clone();
            let mut broadcaster = self.broadcaster;
            let peer_schedule = self.state.peer_schedule.clone();
            let downloader = self.state.downloader.clone();
            async move {
                let own_point = own_point_fut.await;
                round_effects.own_point(own_point.as_ref());

                if let Some(own_point) = own_point {
                    let self_check = Self::expect_own_trusted_point(
                        own_point_round,
                        own_point.clone(),
                        peer_schedule.clone(),
                        downloader,
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
                    collector_signal_rx.close();
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
                        own_point_state_rx,
                        collector_signal_tx,
                        bcaster_ready_rx,
                    )
                    .await;
                (collector, next_round)
            }
        });

        self.state.responder.update(
            &self.state.broadcast_filter,
            next_dag_round,
            &self.state.downloader,
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
        effects: Effects<EngineContext>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            if let Err(dag_point) = Verifier::verify(&point, &peer_schedule) {
                let _guard = effects.span().enter();
                panic!(
                    "Failed to verify own point: {} {:?}",
                    dag_point.alt(),
                    point.id().alt()
                )
            }
            let (_, do_not_certify_tx) = oneshot::channel();
            let dag_point = Verifier::validate(
                point.clone(),
                point_round,
                downloader,
                do_not_certify_tx,
                Effects::<ValidateContext>::new(&effects, &point),
            )
            .await;
            if dag_point.trusted().is_none() {
                let _guard = effects.span().enter();
                panic!(
                    "Failed to validate own point: {} {:?}",
                    dag_point.alt(),
                    point.id()
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
    pub async fn until_ready(mut self) -> (RoundTaskReady, Round) {
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
                (ready, next_round)
            }
            Err(error) if error.is_panic() => panic::resume_unwind(error.into_panic()),
            Err(_) => unreachable!("engine round tasks must not be cancelled"),
        }
    }
}

impl EngineContext {
    const PRODUCE_POINT_DURATION: &'static str = "tycho_mempool_engine_produce_time";
}

impl Effects<EngineContext> {
    fn own_point(&self, own_point: Option<&Point>) {
        // refresh counters with zeros every round
        metrics::counter!("tycho_mempool_engine_produce_skipped")
            .increment(own_point.is_none() as _);
        metrics::counter!("tycho_mempool_points_produced").increment(own_point.is_some() as _);

        let proof = own_point.and_then(|point| point.body().proof.as_ref());
        metrics::counter!("tycho_mempool_points_no_proof_produced").increment(proof.is_none() as _);

        metrics::counter!("tycho_mempool_point_payload_count")
            .increment(own_point.map_or(0, |point| point.body().payload.len() as _));
        let payload_bytes = own_point.map(|point| {
            point
                .body()
                .payload
                .iter()
                .fold(0, |acc, bytes| acc + bytes.len()) as _
        });
        metrics::counter!("tycho_mempool_point_payload_bytes")
            .increment(payload_bytes.unwrap_or_default());

        match own_point {
            Some(own_point) => tracing::info!(
                parent: self.span(),
                digest = display(own_point.digest().alt()),
                payload_bytes = own_point
                    .body().payload.iter().map(|bytes| bytes.len()).sum::<usize>(),
                externals = own_point.body().payload.len(),
                is_proof = Some(own_point.body().anchor_proof == Link::ToSelf).filter(|x| *x),
                is_trigger = Some(own_point.body().anchor_trigger == Link::ToSelf).filter(|x| *x),
                "produced point"
            ),
            None => tracing::info!(parent: self.span(), "will not produce point"),
        };

        // FIXME all commented metrics needs `gauge.set_max()` or `gauge.set_min()`,
        //  or (better) should be accumulated per round as standalone values

        // let Some(own_point) = own_point else {
        //     return;
        // };

        // if let Some(_proof) = &own_point.body().proof {
        //     metrics::gauge!("tycho_mempool_point_evidence_count_min")
        //         .set_min(proof.evidence.len() as f64);
        //     metrics::gauge!("tycho_mempool_point_evidence_count_max")
        //         .set_max(proof.evidence.len() as f64);
        // }

        // metrics::gauge!("tycho_mempool_point_includes_count_min")
        //     .set_min(own_point.body().includes.len() as f64);
        // metrics::gauge!("tycho_mempool_point_includes_count_max")
        //     .set_max(own_point.body().includes.len() as f64);
        // metrics::gauge!("tycho_mempool_point_witness_count_max")
        //     .set_max(own_point.body().witness.len() as f64);
        //
        // metrics::gauge!("tycho_mempool_point_last_anchor_proof_rounds_ago")
        //     .set_max(own_point.body().location.round.0 - own_point.anchor_round(LinkField::Proof).0);
        // metrics::gauge!("tycho_mempool_point_last_anchor_trigger_rounds_ago")
        //     .set_max(own_point.body().location.round.0 - own_point.anchor_round(LinkField::Trigger).0);
    }
}
