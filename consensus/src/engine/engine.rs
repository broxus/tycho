use std::sync::Arc;

use bytes::Bytes;
use everscale_crypto::ed25519::KeyPair;
use itertools::Itertools;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{mpsc, oneshot, RwLock};
use tokio::task::{JoinError, JoinSet};
use tycho_network::{DhtClient, OverlayService, PeerId};

use crate::dag::{Dag, DagRound, InclusionState, Producer};
use crate::engine::input_buffer::InputBuffer;
use crate::engine::MempoolConfig;
use crate::intercom::{
    BroadcastFilter, Broadcaster, BroadcasterSignal, Collector, CollectorSignal, Dispatcher,
    Downloader, PeerSchedule, PeerScheduleUpdater, Responder, Uploader,
};
use crate::models::{Point, PrevPoint, Ugly};

pub struct Engine {
    log_id: Arc<String>,
    dag: Dag,
    peer_schedule: Arc<PeerSchedule>,
    peer_schedule_updater: PeerScheduleUpdater,
    dispatcher: Dispatcher,
    downloader: Downloader,
    broadcaster: Broadcaster,
    collector: Collector,
    broadcast_filter: BroadcastFilter,
    top_dag_round: Arc<RwLock<DagRound>>,
    tasks: JoinSet<()>, // should be JoinSet<!>
    committed: UnboundedSender<(Arc<Point>, Vec<Arc<Point>>)>,
    input_buffer: Box<dyn InputBuffer>,
}

impl Engine {
    pub fn new(
        key_pair: Arc<KeyPair>,
        dht_client: &DhtClient,
        overlay_service: &OverlayService,
        committed: UnboundedSender<(Arc<Point>, Vec<Arc<Point>>)>,
        input_buffer: impl InputBuffer,
    ) -> Self {
        let log_id = Arc::new(format!("{:?}", PeerId::from(key_pair.public_key).ugly()));
        let peer_schedule = Arc::new(PeerSchedule::new(key_pair));

        let (bcast_tx, bcast_rx) = mpsc::unbounded_channel();

        let broadcast_filter =
            BroadcastFilter::new(log_id.clone(), peer_schedule.clone(), bcast_tx);

        let (sig_requests, sig_responses) = mpsc::unbounded_channel();

        let (uploader_tx, uploader_rx) = mpsc::unbounded_channel();

        let dispatcher = Dispatcher::new(
            &dht_client,
            &overlay_service,
            Responder::new(
                log_id.clone(),
                broadcast_filter.clone(),
                sig_requests,
                uploader_tx,
            ),
        );
        let broadcaster = Broadcaster::new(log_id.clone(), &dispatcher);

        let peer_schedule_updater =
            PeerScheduleUpdater::new(dispatcher.overlay.clone(), peer_schedule.clone());

        let top_dag_round = Arc::new(RwLock::new(DagRound::unusable()));

        let mut tasks = JoinSet::new();
        let uploader = Uploader::new(log_id.clone(), uploader_rx, top_dag_round.clone());
        tasks.spawn(async move {
            uploader.run().await;
        });
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

        let downloader = Downloader::new(log_id.clone(), &dispatcher, &peer_schedule);

        let collector = Collector::new(log_id.clone(), &downloader, bcast_rx, sig_responses);

        Self {
            log_id,
            dag: Dag::new(),
            peer_schedule,
            peer_schedule_updater,
            dispatcher,
            downloader,
            broadcaster,
            collector,
            broadcast_filter,
            top_dag_round,
            tasks,
            committed,
            input_buffer: Box::new(input_buffer),
        }
    }

    pub async fn init_with_genesis(&mut self, next_peers: &[PeerId]) {
        let genesis = crate::test_utils::genesis();
        assert!(
            genesis.body.location.round > self.top_dag_round.read().await.round(),
            "genesis point round is too low"
        );
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
            .set_next_peers(&vec![genesis.body.location.author], false);
        self.peer_schedule.rotate();
        // current epoch
        self.peer_schedule
            .set_next_start(genesis.body.location.round.next());
        // start updater only after peers are populated into schedule
        self.peer_schedule_updater.set_next_peers(next_peers, true);
        self.peer_schedule.rotate();

        let current_dag_round = DagRound::genesis(&genesis, &self.peer_schedule);
        self.dag.init(current_dag_round.clone());

        let genesis_state = current_dag_round
            .insert_exact_sign(&genesis, &self.peer_schedule, &self.downloader)
            .await;
        self.collector
            .init(current_dag_round.round().next(), genesis_state.into_iter());
    }

    pub async fn run(mut self) -> ! {
        let mut prev_point: Option<Arc<PrevPoint>> = None;
        let mut produce_own_point = true;
        loop {
            let current_dag_round = self
                .dag
                .top(self.collector.next_round(), &self.peer_schedule);
            let next_dag_round = self
                .dag
                .top(current_dag_round.round().next(), &self.peer_schedule);

            let produce_with_payload = if produce_own_point {
                Some(self.input_buffer.as_mut().fetch(prev_point.is_some()).await)
            } else {
                None
            };

            tracing::info!(
                "{} @ {:?} {}",
                self.log_id,
                current_dag_round.round(),
                produce_with_payload
                    .as_ref()
                    .map(|payload| payload.iter().map(|bytes| bytes.len()).sum::<usize>() / 1024)
                    .map_or("no point".to_string(), |kb| format!("payload {kb} KiB"))
            );

            let (bcaster_ready_tx, bcaster_ready_rx) = mpsc::channel(1);
            // let this channel unbounded - there won't be many items, but every of them is essential
            let (collector_signal_tx, collector_signal_rx) = mpsc::unbounded_channel();
            let (own_point_state_tx, own_point_state_rx) = oneshot::channel();

            let bcaster_run = tokio::spawn(Self::bcaster_run(
                produce_with_payload,
                self.broadcaster,
                self.peer_schedule.clone(),
                self.top_dag_round.clone(),
                next_dag_round.clone(),
                self.downloader.clone(),
                current_dag_round.clone(),
                prev_point,
                own_point_state_tx,
                bcaster_ready_tx,
                collector_signal_rx,
            ));

            let commit_run = tokio::spawn(self.dag.clone().commit(
                self.log_id.clone(),
                next_dag_round.clone(),
                self.committed.clone(),
            ));

            let bcast_filter_run = {
                let bcast_filter = self.broadcast_filter.clone();
                let round = current_dag_round.round();
                tokio::spawn(async move { bcast_filter.advance_round(round) })
            };

            let collector_run = tokio::spawn(self.collector.run(
                next_dag_round.clone(),
                own_point_state_rx,
                collector_signal_tx,
                bcaster_ready_rx,
            ));

            match tokio::join!(collector_run, bcaster_run, commit_run, bcast_filter_run) {
                (Ok(collector_upd), Ok((bcaster, new_prev_point)), Ok(()), Ok(())) => {
                    self.broadcaster = bcaster;
                    prev_point = new_prev_point;
                    produce_own_point = next_dag_round.round() == collector_upd.next_round();
                    self.collector = collector_upd;
                }
                (collector, bcaster, commit, bcast_filter_upd) => Self::panic_on_join(&[
                    (collector.err(), "collector"),
                    (bcaster.err(), "broadcaster"),
                    (commit.err(), "commit"),
                    (bcast_filter_upd.err(), "broadcast filter update"),
                ]),
            }
        }
    }

    async fn bcaster_run(
        produce_with_payload: Option<Vec<Bytes>>,
        mut broadcaster: Broadcaster,
        peer_schedule: Arc<PeerSchedule>,
        top_dag_round: Arc<RwLock<DagRound>>,
        next_dag_round: DagRound,
        downloader: Downloader,
        current_dag_round: DagRound,
        prev_point: Option<Arc<PrevPoint>>,
        own_point_state: oneshot::Sender<InclusionState>,
        bcaster_ready_tx: mpsc::Sender<BroadcasterSignal>,
        mut collector_signal_rx: mpsc::UnboundedReceiver<CollectorSignal>,
    ) -> (Broadcaster, Option<Arc<PrevPoint>>) {
        if let Some(payload) = produce_with_payload {
            let new_point = tokio::spawn(Self::produce(
                current_dag_round,
                prev_point,
                peer_schedule.clone(),
                downloader,
                own_point_state,
                payload,
            ));
            // must signal to uploader before start of broadcast
            let top_dag_round_upd =
                tokio::spawn(Self::update_top_round(top_dag_round, next_dag_round));
            let new_point = match tokio::join!(new_point, top_dag_round_upd) {
                (Ok(new_point), Ok(())) => new_point,
                (new_point, top_dag_round) => {
                    Self::panic_on_join(&[
                        (new_point.err(), "new point producer"),
                        (top_dag_round.err(), "top dag round update"),
                    ]);
                }
            };
            if let Some(own_point) = new_point {
                let evidence = broadcaster
                    .run(
                        &own_point,
                        &peer_schedule,
                        bcaster_ready_tx,
                        collector_signal_rx,
                    )
                    .await;
                let prev_point = PrevPoint {
                    digest: own_point.digest.clone(),
                    evidence: evidence.into_iter().collect(),
                };
                return (broadcaster, Some(Arc::new(prev_point)));
            }
        } else {
            _ = own_point_state;
            Self::update_top_round(top_dag_round, next_dag_round).await;
        }
        collector_signal_rx.close();
        bcaster_ready_tx.send(BroadcasterSignal::Ok).await.ok();
        (broadcaster, None)
    }

    async fn produce(
        current_dag_round: DagRound,
        prev_point: Option<Arc<PrevPoint>>,
        peer_schedule: Arc<PeerSchedule>,
        downloader: Downloader,
        own_point_state: oneshot::Sender<InclusionState>,
        payload: Vec<Bytes>,
    ) -> Option<Arc<Point>> {
        if let Some(own_point) =
            Producer::new_point(&current_dag_round, prev_point.as_deref(), payload)
        {
            let state = current_dag_round
                .insert_exact_sign(&own_point, &peer_schedule, &downloader)
                .await
                .expect("own produced point must be valid");
            own_point_state.send(state).ok();
            Some(own_point)
        } else {
            // _ = own_point_state; dropped
            None
        }
    }

    async fn update_top_round(top_dag_round: Arc<RwLock<DagRound>>, top: DagRound) {
        // must wait while active uploads (from previous round) are enqueued to read;
        // it would be incorrect to serve uploads from outdated round
        // since the start of new point broadcast
        let mut write = top_dag_round.write().await;
        *write = top;
    }

    fn panic_on_join(maybe_err: &[(Option<JoinError>, &'static str)]) -> ! {
        let msg = maybe_err
            .iter()
            .filter_map(|(res, name)| {
                res.as_ref()
                    .map(|err| format!("{name} task panicked: {err:?}"))
            })
            .join("; \n");
        panic!("{}", msg)
    }
}
