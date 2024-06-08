use std::iter;
use std::sync::Arc;

use everscale_crypto::ed25519::KeyPair;
use itertools::Itertools;
use tokio::sync::{mpsc, oneshot};
use tokio::task::{JoinError, JoinHandle, JoinSet};
use tracing::Span;
use tycho_network::{DhtClient, OverlayService, PeerId};

use crate::dag::{Dag, DagRound, InclusionState, Producer, Verifier, WeakDagRound};
use crate::effects::{AltFormat, CurrentRoundContext, Effects, EffectsContext};
use crate::engine::input_buffer::InputBuffer;
use crate::engine::MempoolConfig;
use crate::intercom::{
    BroadcastFilter, Broadcaster, BroadcasterSignal, Collector, Dispatcher, Downloader,
    PeerSchedule, PeerScheduleUpdater, Responder,
};
use crate::models::{Point, PrevPoint, Round};
use crate::LogFlavor;

pub struct Engine {
    dag: Dag,
    peer_schedule: Arc<PeerSchedule>,
    peer_schedule_updater: PeerScheduleUpdater,
    dispatcher: Dispatcher,
    responder: Responder,
    downloader: Downloader,
    broadcaster: Broadcaster,
    broadcast_filter: BroadcastFilter,
    collector: Collector,
    tasks: JoinSet<()>, // should be JoinSet<!>
    committed: mpsc::UnboundedSender<(Arc<Point>, Vec<Arc<Point>>)>,
    input_buffer: InputBuffer,
}

impl Engine {
    pub fn new(
        key_pair: Arc<KeyPair>,
        dht_client: &DhtClient,
        overlay_service: &OverlayService,
        committed: mpsc::UnboundedSender<(Arc<Point>, Vec<Arc<Point>>)>,
        input_buffer: InputBuffer,
    ) -> Self {
        let peer_schedule = Arc::new(PeerSchedule::new(key_pair));

        let (bcast_tx, bcast_rx) = mpsc::unbounded_channel();

        let broadcast_filter = BroadcastFilter::new(peer_schedule.clone(), bcast_tx);

        let responder = Responder::default();

        let (dispatcher, overlay) = Dispatcher::new(dht_client, overlay_service, responder.clone());
        let broadcaster = Broadcaster::new(&dispatcher);

        let peer_schedule_updater = PeerScheduleUpdater::new(overlay, peer_schedule.clone());

        let mut tasks = JoinSet::new();
        tasks.spawn({
            let peer_schedule_updater = peer_schedule_updater.clone();
            async move {
                peer_schedule_updater.run().await;
            }
        });
        tasks.spawn({
            let broadcast_filter = broadcast_filter.clone();
            async move {
                broadcast_filter.clear_cache().await;
            }
        });

        let downloader = Downloader::new(&dispatcher, &peer_schedule);

        let collector = Collector::new(&downloader, bcast_rx);

        Self {
            dag: Dag::new(),
            peer_schedule,
            peer_schedule_updater,
            dispatcher,
            responder,
            downloader,
            broadcaster,
            broadcast_filter,
            collector,
            tasks,
            committed,
            input_buffer,
        }
    }

    pub async fn init_with_genesis(&mut self, next_peers: &[PeerId]) {
        let genesis = crate::test_utils::genesis();
        let span = tracing::error_span!("init engine with genesis");
        let span_guard = span.enter();
        // check only genesis round as it is widely used in point validation.
        // if some nodes use distinct genesis data, their first points will be rejected
        assert_eq!(
            genesis.body.location.round,
            MempoolConfig::GENESIS_ROUND,
            "genesis point round must match genesis round from config"
        );
        // finished epoch
        self.peer_schedule
            .set_next_start(genesis.body.location.round);
        self.peer_schedule_updater
            .set_next_peers(&[genesis.body.location.author], false);
        self.peer_schedule.rotate();
        // current epoch
        self.peer_schedule
            .set_next_start(genesis.body.location.round.next());
        // start updater only after peers are populated into schedule
        self.peer_schedule_updater.set_next_peers(next_peers, true);
        self.peer_schedule.rotate();

        let current_dag_round = DagRound::genesis(&genesis, &self.peer_schedule);
        self.dag.init(current_dag_round.clone());

        let genesis_state =
            current_dag_round.insert_exact_sign(&genesis, &self.peer_schedule, &span);
        self.collector
            .init(current_dag_round.round().next(), iter::once(genesis_state));
        drop(span_guard);
        Self::expect_trusted_point(
            current_dag_round.to_weak(),
            genesis.clone(),
            self.peer_schedule.clone(),
            self.downloader.clone(),
            span,
        )
        .await
        .expect("genesis must be valid");
    }

    pub async fn run(mut self) -> ! {
        let mut chained_rounds = Effects::<ChainedRoundsContext>::new(self.collector.next_round());
        let mut prev_point: Option<Arc<PrevPoint>> = None;
        let mut prev_round_success = true;
        loop {
            if !prev_round_success {
                chained_rounds = Effects::<ChainedRoundsContext>::new(self.collector.next_round());
            };
            let round_effects =
                Effects::<CurrentRoundContext>::new(&chained_rounds, self.collector.next_round());

            let current_dag_round = self
                .dag
                .top(self.collector.next_round(), &self.peer_schedule);
            let next_dag_round = self
                .dag
                .top(current_dag_round.round().next(), &self.peer_schedule);

            let (bcaster_ready_tx, bcaster_ready_rx) = mpsc::channel(1);
            // let this channel unbounded - there won't be many items, but every of them is essential
            let (collector_signal_tx, mut collector_signal_rx) = mpsc::unbounded_channel();
            let (own_point_state_tx, own_point_state_rx) = oneshot::channel();

            let own_point_fut = if prev_round_success {
                let round_effects = round_effects.clone();
                let current_dag_round = current_dag_round.clone();
                let peer_schedule = self.peer_schedule.clone();
                let input_buffer = self.input_buffer.clone();
                futures_util::future::Either::Right(tokio::task::spawn_blocking(move || {
                    Self::produce(
                        round_effects,
                        current_dag_round,
                        prev_point,
                        peer_schedule,
                        own_point_state_tx,
                        input_buffer,
                    )
                }))
            } else {
                drop(own_point_state_tx);
                futures_util::future::Either::Left(futures_util::future::ready(Ok(None::<
                    Arc<Point>,
                >)))
            };

            let bcaster_run = tokio::spawn({
                let own_ppint_round = current_dag_round.to_weak();
                let round_effects = round_effects.clone();
                let peer_schedule = self.peer_schedule.clone();
                let mut broadcaster = self.broadcaster;
                let downloader = self.downloader.clone();
                async move {
                    if let Some(own_point) = own_point_fut.await.expect("new point producer") {
                        let paranoid = Self::expect_trusted_point(
                            own_ppint_round,
                            own_point.clone(),
                            peer_schedule.clone(),
                            downloader,
                            round_effects.span().clone(),
                        );
                        let evidence = broadcaster
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
                        let prev_point = PrevPoint {
                            digest: own_point.digest.clone(),
                            evidence: evidence.into_iter().collect(),
                        };
                        (broadcaster, Some(Arc::new(prev_point)))
                    } else {
                        collector_signal_rx.close();
                        bcaster_ready_tx.send(BroadcasterSignal::Ok).await.ok();
                        (broadcaster, None)
                    }
                }
            });

            let commit_run = tokio::task::spawn_blocking({
                let dag = self.dag.clone();
                let next_dag_round = next_dag_round.clone();
                let committed = self.committed.clone();
                let round_effects = round_effects.clone();
                move || dag.commit(next_dag_round, committed, round_effects)
            });

            let collector_run = tokio::spawn(self.collector.run(
                round_effects.clone(),
                next_dag_round.clone(),
                own_point_state_rx,
                collector_signal_tx,
                bcaster_ready_rx,
            ));

            self.responder
                .update(&self.broadcast_filter, &next_dag_round, &round_effects);
            self.broadcast_filter
                .advance_round(next_dag_round.round(), &round_effects);

            match tokio::join!(collector_run, bcaster_run, commit_run) {
                (Ok(collector_upd), Ok((bcaster, new_prev_point)), Ok(())) => {
                    self.broadcaster = bcaster;
                    prev_point = new_prev_point;
                    prev_round_success = next_dag_round.round() == collector_upd.next_round();
                    self.collector = collector_upd;
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

    fn produce(
        round_effects: Effects<CurrentRoundContext>,
        current_dag_round: DagRound,
        prev_point: Option<Arc<PrevPoint>>,
        peer_schedule: Arc<PeerSchedule>,
        own_point_state: oneshot::Sender<InclusionState>,
        input_buffer: InputBuffer,
    ) -> Option<Arc<Point>> {
        if let Some(own_point) =
            Producer::new_point(&current_dag_round, prev_point.as_deref(), &input_buffer)
        {
            tracing::info!(
                parent: round_effects.span(),
                digest = display(own_point.digest.alt()),
                payload_bytes = own_point
                    .body.payload.iter().map(|bytes| bytes.len()).sum::<usize>(),
                externals = own_point.body.payload.len(),
                "produced point"
            );
            let state = current_dag_round.insert_exact_sign(
                &own_point,
                &peer_schedule,
                round_effects.span(),
            );
            own_point_state.send(state).ok();
            Some(own_point)
        } else {
            tracing::info!(parent: round_effects.span(), "will not produce point");
            // drop(own_point_state); goes out of scope
            None
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

    fn expect_trusted_point(
        point_round: WeakDagRound,
        point: Arc<Point>,
        peer_schedule: Arc<PeerSchedule>,
        downloader: Downloader,
        span: Span,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            if Verifier::verify(&point, &peer_schedule).is_err() {
                let _guard = span.enter();
                panic!("Failed to verify, expected Trusted point: {:?}", point.id())
            }
            let dag_point =
                Verifier::validate(point.clone(), point_round, downloader, span.clone()).await;
            if dag_point.trusted().is_none() {
                let _guard = span.enter();
                panic!(
                    "Failed to validate, expected Trusted point: {:?}",
                    point.id()
                )
            };
        })
    }
}

struct ChainedRoundsContext;
impl EffectsContext for ChainedRoundsContext {}
impl Effects<ChainedRoundsContext> {
    fn new(since: Round) -> Self {
        Self::new_root(tracing::error_span!("rounds", "since" = since.0))
    }
}

impl EffectsContext for CurrentRoundContext {}
impl Effects<CurrentRoundContext> {
    fn new(parent: &Effects<ChainedRoundsContext>, current: Round) -> Self {
        Self::new_child(parent.span(), || {
            tracing::error_span!("round", "current" = current.0)
        })
    }

    pub(crate) fn log_committed(&self, committed: &[(Arc<Point>, Vec<Arc<Point>>)]) {
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
                        anchor.body.time
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
