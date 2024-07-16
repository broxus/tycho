use std::iter;
use std::ops::Neg;
use std::sync::Arc;
use std::time::{Duration, Instant};

use everscale_crypto::ed25519::KeyPair;
use itertools::Itertools;
use tokio::sync::{mpsc, oneshot};
use tokio::task::{JoinError, JoinHandle};
use tracing::Instrument;
use tycho_network::{DhtClient, OverlayService, PeerId};
use tycho_util::metrics::HistogramGuard;

use crate::dag::{Dag, DagRound, LastOwnPoint, Producer, Verifier, WeakDagRound};
use crate::effects::{
    AltFormat, ChainedRoundsContext, CollectorContext, Effects, EngineContext, ValidateContext,
};
use crate::engine::input_buffer::InputBuffer;
use crate::engine::MempoolConfig;
use crate::intercom::{
    BroadcastFilter, Broadcaster, BroadcasterSignal, Collector, Dispatcher, Downloader,
    PeerSchedule, Responder,
};
use crate::models::{ConsensusRound, Link, Point, UnixTime};
use crate::LogFlavor;

pub struct Engine {
    dag: Dag,
    peer_schedule: PeerSchedule,
    responder: Responder,
    downloader: Downloader,
    broadcaster: Broadcaster,
    broadcast_filter: BroadcastFilter,
    consensus_round: ConsensusRound,
    effects: Effects<ChainedRoundsContext>,
    collector: Collector,
    committed: mpsc::UnboundedSender<(Point, Vec<Point>)>,
    input_buffer: InputBuffer,
}

impl Engine {
    pub fn new(
        key_pair: Arc<KeyPair>,
        dht_client: &DhtClient,
        overlay_service: &OverlayService,
        committed: mpsc::UnboundedSender<(Point, Vec<Point>)>,
        input_buffer: InputBuffer,
    ) -> Self {
        let responder = Responder::default();
        let (dispatcher, overlay) = Dispatcher::new(dht_client, overlay_service, responder.clone());

        let peer_schedule = PeerSchedule::new(key_pair, overlay);

        let consensus_round = ConsensusRound::new();
        let effects = Effects::<ChainedRoundsContext>::new(consensus_round.get());
        let (bcast_tx, bcast_rx) = mpsc::unbounded_channel();

        let collector = Collector::new(bcast_rx);
        let broadcast_filter =
            BroadcastFilter::new(peer_schedule.clone(), bcast_tx, consensus_round.clone());

        let broadcaster = Broadcaster::new(&dispatcher);

        tokio::spawn({
            let peer_schedule = peer_schedule.clone();
            async move {
                peer_schedule.run_updater().await;
            }
        });
        tokio::spawn({
            let broadcast_filter = broadcast_filter.clone();
            async move {
                broadcast_filter.clear_cache().await;
            }
        });

        let downloader = Downloader::new(&dispatcher, &peer_schedule);

        Self {
            dag: Dag::new(),
            peer_schedule,
            responder,
            downloader,
            broadcaster,
            broadcast_filter,
            consensus_round,
            effects,
            collector,
            committed,
            input_buffer,
        }
    }

    pub async fn init_with_genesis(&mut self, next_peers: &[PeerId]) {
        let genesis = crate::test_utils::genesis();
        let entered_span = tracing::error_span!("init engine with genesis").entered();
        // check only genesis round as it is widely used in point validation.
        // if some nodes use distinct genesis data, their first points will be rejected
        assert_eq!(
            genesis.id(),
            crate::test_utils::genesis_point_id(),
            "genesis point id does not match one from config"
        );
        assert!(
            genesis.is_integrity_ok(),
            "genesis point does not pass integrity check"
        );
        assert!(genesis.is_well_formed(), "genesis point is not well formed");
        // finished epoch
        {
            let mut guard = self.peer_schedule.write();
            let peer_schedule = self.peer_schedule.clone();
            guard.set_next_start(MempoolConfig::GENESIS_ROUND, &peer_schedule);
            guard.set_next_peers(
                &[crate::test_utils::genesis_point_id().location.author],
                &peer_schedule,
                false,
            );
            guard.rotate(&peer_schedule);
            // current epoch
            guard.set_next_start(genesis.body().location.round.next(), &peer_schedule);
            // start updater only after peers are populated into schedule
            guard.set_next_peers(next_peers, &peer_schedule, true);
            guard.rotate(&peer_schedule);
        }

        let current_dag_round = DagRound::genesis(&genesis, &self.peer_schedule);
        let next_dag_round = current_dag_round.next(&self.peer_schedule);

        let genesis_state =
            current_dag_round.insert_exact_sign(&genesis, next_dag_round.key_pair());

        let next_round = next_dag_round.round();

        self.dag.init(current_dag_round, next_dag_round);

        self.collector.init(next_round, iter::once(genesis_state));

        self.consensus_round.set_max(next_round);

        drop(entered_span);
        self.effects = Effects::<ChainedRoundsContext>::new(next_round);
    }

    pub async fn run(mut self) -> ! {
        // contains all collected signatures, even if they are insufficient to produce valid point;
        // may reference own point older than from a last round, as its payload may be not resend
        let mut last_own_point: Option<Arc<LastOwnPoint>> = None;
        loop {
            let _round_duration = HistogramGuard::begin(EngineContext::ROUND_DURATION);
            let (prev_round_ok, current_dag_round, round_effects) = {
                // treat atomic as lock - do not leak its value or repeat the `get()`
                let consensus_round = self.consensus_round.get();
                let top_dag_round = self.dag.top();
                assert!(
                    consensus_round >= top_dag_round.round(),
                    "consensus round {} cannot be less than top dag round {}",
                    consensus_round.0,
                    top_dag_round.round().0,
                );
                metrics::counter!(EngineContext::ROUNDS_SKIP)
                    .increment((consensus_round.0 - top_dag_round.round().0) as _); // safe

                // `true` if we collected enough dependencies and (optionally) signatures,
                // so `next_dag_round` from the previous loop is the current now
                let prev_round_ok = consensus_round == top_dag_round.round();
                if prev_round_ok {
                    // Round was not advanced by `BroadcastFilter`, and `Collector` gathered enough
                    // broadcasts during previous round. So if `Broadcaster` was run at previous
                    // round (controlled by `Collector`), then it gathered enough signatures.
                    // If our newly created current round repeats such success, then we've moved
                    // consensus with other 2F nodes during previous round
                    // (at this moment we are not sure to be free from network lag).
                    // Then any reliable point received _up to this moment_ should belong to a round
                    // not greater than new `next_dag_round`, and any future round cannot exist.

                    let round_effects =
                        Effects::<EngineContext>::new(&self.effects, consensus_round);
                    (prev_round_ok, top_dag_round, round_effects)
                } else {
                    self.effects = Effects::<ChainedRoundsContext>::new(consensus_round);
                    let round_effects =
                        Effects::<EngineContext>::new(&self.effects, consensus_round);
                    let current_dag_round =
                        self.dag
                            .fill_to_top(consensus_round, &self.peer_schedule, &round_effects);
                    (prev_round_ok, current_dag_round, round_effects)
                }
            };
            metrics::gauge!(EngineContext::CURRENT_ROUND).set(current_dag_round.round().0);

            let next_dag_round = self.dag.fill_to_top(
                current_dag_round.round().next(),
                &self.peer_schedule,
                &round_effects,
            );

            let (bcaster_ready_tx, bcaster_ready_rx) = oneshot::channel();
            // let this channel unbounded - there won't be many items, but every of them is essential
            let (collector_signal_tx, mut collector_signal_rx) = mpsc::unbounded_channel();
            let (own_point_state_tx, own_point_state_rx) = oneshot::channel();

            let own_point_fut = if prev_round_ok {
                let current_dag_round = current_dag_round.clone();
                let input_buffer = self.input_buffer.clone();
                let last_own_point = last_own_point.clone();
                futures_util::future::Either::Right(
                    tokio::task::spawn_blocking(move || {
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
                futures_util::future::Either::Left(futures_util::future::ready(Ok(None::<Point>)))
            };

            let bcaster_run = tokio::spawn({
                let own_point_round = current_dag_round.downgrade();
                let round_effects = round_effects.clone();
                let peer_schedule = self.peer_schedule.clone();
                let mut broadcaster = self.broadcaster;
                let downloader = self.downloader.clone();
                async move {
                    let own_point = own_point_fut.await.expect("new point producer");
                    round_effects.own_point(own_point.as_ref());

                    if let Some(own_point) = own_point {
                        let paranoid = Self::expect_own_trusted_point(
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
                        paranoid.await.expect("verify own produced point");
                        (broadcaster, Some(new_last_own_point))
                    } else {
                        collector_signal_rx.close();
                        bcaster_ready_tx.send(BroadcasterSignal::Ok).ok();
                        (broadcaster, None)
                    }
                }
            });

            let commit_run = tokio::task::spawn_blocking({
                let mut dag = self.dag;
                let next_dag_round = next_dag_round.clone();
                let committed_tx = self.committed.clone();
                let round_effects = round_effects.clone();
                move || {
                    let task_start = Instant::now();
                    let _guard = round_effects.span().enter();

                    let committed = dag.commit(next_dag_round);

                    round_effects.commit_metrics(&committed);
                    round_effects.log_committed(&committed);

                    if !committed.is_empty() {
                        for points in committed {
                            committed_tx
                                .send(points) // not recoverable
                                .expect("Failed to send anchor commit message tp mpsc channel");
                        }
                        metrics::histogram!(EngineContext::COMMIT_DURATION)
                            .record(task_start.elapsed());
                    }
                    dag
                }
            });

            let collector_run = tokio::spawn({
                let mut collector = self.collector;
                let effects = Effects::<CollectorContext>::new(&round_effects);
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

            self.responder.update(
                &self.broadcast_filter,
                &next_dag_round,
                &self.downloader,
                &round_effects,
            );

            match tokio::join!(collector_run, bcaster_run, commit_run) {
                (Ok((collector, next_round)), Ok((bcaster, new_last_own_point)), Ok(dag)) => {
                    self.broadcaster = bcaster;
                    // do not reset to None, Producer decides whether to use old value or not
                    if let Some(new_last_own_point) = new_last_own_point {
                        // value is returned from the task without Arc for the sake of code clarity
                        last_own_point = Some(Arc::new(new_last_own_point));
                    }
                    self.consensus_round.set_max(next_round);
                    self.collector = collector;
                    self.dag = dag;
                }
                (collector, bcaster, commit) => {
                    let msg = Self::join_err_msg(&[
                        (collector.err(), "collector"),
                        (bcaster.err(), "broadcaster"),
                        (commit.err(), "commit"),
                    ]);
                    let _span = round_effects.span().enter();
                    panic!("{msg}")
                }
            }
        }
    }

    fn join_err_msg(maybe_err: &[(Option<JoinError>, &'static str)]) -> String {
        maybe_err
            .iter()
            .filter_map(|(res, name)| {
                res.as_ref()
                    .map(|err| format!("{name} task panicked: {err:?}"))
            })
            .join("; \n")
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
            let dag_point = Verifier::validate(
                point.clone(),
                point_round,
                downloader,
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

impl EngineContext {
    const CURRENT_ROUND: &'static str = "tycho_mempool_engine_current_round";
    const ROUNDS_SKIP: &'static str = "tycho_mempool_engine_rounds_skipped";
    const ROUND_DURATION: &'static str = "tycho_mempool_engine_round_time";
    const PRODUCE_POINT_DURATION: &'static str = "tycho_mempool_engine_produce_time";
    const COMMIT_DURATION: &'static str = "tycho_mempool_engine_commit_time";
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

    fn commit_metrics(&self, committed: &[(Point, Vec<Point>)]) {
        metrics::counter!("tycho_mempool_commit_anchors").increment(committed.len() as _);

        if let Some((first_anchor, _)) = committed.first() {
            metrics::gauge!("tycho_mempool_commit_latency_rounds")
                .set(self.depth(first_anchor.body().location.round));
        }
        if let Some((last_anchor, _)) = committed.last() {
            let now = UnixTime::now().as_u64();
            let anchor_time = last_anchor.body().time.as_u64();
            let latency = if now >= anchor_time {
                Duration::from_millis(now - anchor_time).as_secs_f64()
            } else {
                Duration::from_millis(anchor_time - now).as_secs_f64().neg()
            };
            metrics::histogram!("tycho_mempool_commit_anchor_latency_time").record(latency);
        }
    }

    fn log_committed(&self, committed: &[(Point, Vec<Point>)]) {
        if !committed.is_empty()
            && MempoolConfig::LOG_FLAVOR == LogFlavor::Truncated
            && tracing::enabled!(tracing::Level::DEBUG)
        {
            let committed = committed
                .iter()
                .map(|(anchor, history)| {
                    let history = history
                        .iter()
                        .map(|point| format!("{:?}", point.id().alt()))
                        .join(", ");
                    format!(
                        "anchor {:?} time {} : [ {history} ]",
                        anchor.id().alt(),
                        anchor.body().time
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
