use std::sync::Arc;
use std::time::Instant;
use std::{iter, panic};

use futures_util::{future, TryFutureExt};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tracing::Instrument;
use tycho_util::sync::rayon_run;

use crate::dag::{DagRound, InclusionState, LastOwnPoint, Producer, Verifier, WeakDagRound};
use crate::effects::{
    AltFormat, CollectorContext, Effects, EngineContext, MempoolStore, ValidateContext,
};
use crate::engine::input_buffer::InputBuffer;
use crate::intercom::{
    BroadcastFilter, Broadcaster, BroadcasterSignal, Collector, CollectorSignal, Dispatcher,
    Downloader, PeerSchedule, Responder,
};
use crate::models::{Link, Point, Round};
use crate::outer_round::{Collator, Consensus, OuterRound};
use crate::MempoolConfig;

struct RoundTaskState {
    peer_schedule: PeerSchedule,
    store: MempoolStore,
    responder: Responder,
    collator_round: OuterRound<Collator>,
    input_buffer: InputBuffer,
    broadcast_filter: BroadcastFilter,
    downloader: Downloader,
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
        dispatcher: &Dispatcher,
        peer_schedule: &PeerSchedule,
        store: &MempoolStore,
        consensus_round: &OuterRound<Consensus>,
        collator_round: OuterRound<Collator>,
        responder: Responder,
        input_buffer: InputBuffer,
    ) -> Self {
        let (bcast_tx, bcast_rx) = mpsc::unbounded_channel();
        let broadcast_filter = BroadcastFilter::new(peer_schedule, consensus_round, bcast_tx);
        tokio::spawn({
            let this = broadcast_filter.clone();
            async move {
                this.clear_cache().await;
            }
        });

        Self {
            state: RoundTaskState {
                peer_schedule: peer_schedule.clone(),
                store: store.clone(),
                responder,
                collator_round,
                input_buffer,
                broadcast_filter,
                downloader: Downloader::new(dispatcher, peer_schedule),
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
        let (collector_signal_tx, collector_signal_rx) = watch::channel(CollectorSignal::Retry);
        let (own_point_state_tx, own_point_state_rx) = oneshot::channel();

        let own_point_fut = if prev_round_ok {
            let not_silent_since = Round(
                (current_dag_round.round().0) // latest reliably detected consensus round
                    .saturating_sub(MempoolConfig::MAX_ANCHOR_DISTANCE as u32),
            );
            let wait_collator_ready = if self.state.collator_round.get() >= not_silent_since {
                future::Either::Right(future::ready(Ok(true))) // ready; Ok for `JoinError`
            } else {
                // must cancel on collector finish/err signal
                let mut collator_round = self.state.collator_round.receiver();
                let mut collector_signal_rx = collector_signal_rx.clone();
                future::Either::Left(tokio::spawn(async move {
                    loop {
                        tokio::select! {
                            collated = collator_round.next() => if collated >= not_silent_since {
                                break true; // ready to produce point: collator synced enough
                            },
                            Ok(()) = collector_signal_rx.changed() => {
                                let signal = *collector_signal_rx.borrow_and_update();
                                match signal {
                                    CollectorSignal::Finish | CollectorSignal::Err => break false,
                                    CollectorSignal::Retry => {}
                                };
                            }
                        }
                    }
                }))
            };

            let current_dag_round = current_dag_round.clone();
            let input_buffer = self.state.input_buffer.clone();
            let last_own_point = self.last_own_point.clone();
            let store = self.state.store.clone();
            let task = wait_collator_ready
                .and_then(|is_ready_to_produce| {
                    if is_ready_to_produce {
                        future::Either::Right(rayon_run(move || {
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
                                metrics::histogram!(EngineContext::PRODUCE_POINT_DURATION)
                                    .record(task_start_time.elapsed());
                            };
                            Ok(point_opt)
                            // if None: `drop(own_point_state_tx)`; it is moved and goes out of scope
                        }))
                    } else {
                        future::Either::Left(future::ready(Ok(None)))
                    }
                })
                .instrument(round_effects.span().clone());
            future::Either::Right(task)
        } else {
            drop(own_point_state_tx);
            future::Either::Left(future::ready(Ok(None::<Point>)))
        };

        let broadcaster_run = tokio::spawn({
            let own_point_round = current_dag_round.downgrade();
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
                store,
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
