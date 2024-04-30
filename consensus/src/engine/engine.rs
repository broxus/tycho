use std::sync::Arc;

use everscale_crypto::ed25519::{KeyPair, SecretKey};
use itertools::Itertools;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinSet;

use tycho_network::{DhtClient, OverlayService, PeerId};

use crate::dag::{Dag, DagRound, InclusionState, Producer};
use crate::intercom::{
    BroadcastFilter, Broadcaster, BroadcasterSignal, Collector, CollectorSignal, Dispatcher,
    Downloader, PeerSchedule, PeerScheduleUpdater, Responder, Uploader,
};
use crate::models::{Point, PrevPoint, Ugly};

pub struct Engine {
    log_id: Arc<String>,
    dag: Dag,
    peer_schedule: Arc<PeerSchedule>,
    dispatcher: Dispatcher,
    downloader: Downloader,
    collector: Collector,
    broadcast_filter: BroadcastFilter,
    top_dag_round_watch: watch::Sender<DagRound>,
    tasks: JoinSet<()>, // should be JoinSet<!>
}

impl Engine {
    pub async fn new(
        secret_key: &SecretKey,
        dht_client: &DhtClient,
        overlay_service: &OverlayService,
        peers: &Vec<PeerId>,
    ) -> Self {
        let key_pair = KeyPair::from(secret_key);
        let log_id = Arc::new(format!("{:?}", PeerId::from(key_pair.public_key).ugly()));
        let peer_schedule = Arc::new(PeerSchedule::new(Arc::new(key_pair)));

        let (bcast_tx, bcast_rx) = mpsc::unbounded_channel();

        let broadcast_filter =
            BroadcastFilter::new(log_id.clone(), peer_schedule.clone(), bcast_tx);

        let (sig_requests, sig_responses) = mpsc::unbounded_channel();

        let (uploader_tx, uploader_rx) = mpsc::unbounded_channel();

        let dispatcher = Dispatcher::new(
            &dht_client,
            &overlay_service,
            peers,
            Responder::new(
                log_id.clone(),
                broadcast_filter.clone(),
                sig_requests,
                uploader_tx,
            ),
        );

        let genesis = Arc::new(crate::test_utils::genesis());
        let peer_schedule_updater =
            PeerScheduleUpdater::new(dispatcher.overlay.clone(), peer_schedule.clone());
        // finished epoch
        peer_schedule.set_next_start(genesis.body.location.round);
        peer_schedule_updater.set_next_peers(&vec![genesis.body.location.author]);
        peer_schedule.rotate();
        // current epoch
        peer_schedule.set_next_start(genesis.body.location.round.next());
        // start updater only after peers are populated into schedule
        peer_schedule_updater.set_next_peers(peers);
        peer_schedule.rotate();

        let current_dag_round = DagRound::genesis(&genesis, &peer_schedule);
        let dag = Dag::new(current_dag_round.clone());

        let (top_dag_round_tx, top_dag_round_rx) = watch::channel(current_dag_round.clone());

        let mut tasks = JoinSet::new();
        let uploader = Uploader::new(log_id.clone(), uploader_rx, top_dag_round_rx);
        tasks.spawn(async move {
            uploader.run().await;
        });
        tasks.spawn(async move {
            peer_schedule_updater.run().await;
        });
        tasks.spawn({
            let broadcast_filter = broadcast_filter.clone();
            async move {
                broadcast_filter.clear_cache().await;
            }
        });

        let downloader = Downloader::new(log_id.clone(), &dispatcher, &peer_schedule);

        let genesis_state = current_dag_round
            .insert_exact_validate(&genesis, &peer_schedule, &downloader)
            .await;
        let collector = Collector::new(
            log_id.clone(),
            &downloader,
            bcast_rx,
            sig_responses,
            genesis_state.into_iter(),
            current_dag_round.round().next(),
        );

        Self {
            log_id,
            dag,
            peer_schedule,
            dispatcher,
            downloader,
            collector,
            broadcast_filter,
            top_dag_round_watch: top_dag_round_tx,
            tasks,
        }
    }

    async fn bcaster_run(
        log_id: Arc<String>,
        produce_own_point: bool,
        dispatcher: Dispatcher,
        peer_schedule: Arc<PeerSchedule>,
        downloader: Downloader,
        current_dag_round: DagRound,
        prev_point: Option<PrevPoint>,
        own_point_state: oneshot::Sender<InclusionState>,
        bcaster_ready_tx: mpsc::Sender<BroadcasterSignal>,
        mut collector_signal_rx: mpsc::UnboundedReceiver<CollectorSignal>,
    ) -> Option<PrevPoint> {
        if produce_own_point {
            if let Some(own_point) =
                Producer::new_point(&current_dag_round, prev_point.as_ref(), vec![]).await
            {
                let state = current_dag_round
                    .insert_exact_validate(&own_point, &peer_schedule, &downloader)
                    .await
                    .expect("own produced point must be valid");
                own_point_state.send(state).ok();
                let evidence = Broadcaster::new(
                    log_id.clone(),
                    &own_point,
                    &dispatcher,
                    &peer_schedule,
                    bcaster_ready_tx,
                    collector_signal_rx,
                )
                .run()
                .await;
                return Some(PrevPoint {
                    digest: own_point.digest.clone(),
                    evidence: evidence.into_iter().collect(),
                });
            }
        }
        _ = own_point_state;
        collector_signal_rx.close();
        bcaster_ready_tx.send(BroadcasterSignal::Ok).await.ok();
        None
    }
    pub async fn run(mut self) -> ! {
        let mut prev_point: Option<PrevPoint> = None;
        let mut produce_own_point = true;
        loop {
            let current_dag_round = self
                .dag
                .top(self.collector.next_round(), &self.peer_schedule);
            let next_dag_round = self
                .dag
                .top(&current_dag_round.round().next(), &self.peer_schedule);
            self.top_dag_round_watch.send(next_dag_round.clone()).ok();

            tracing::info!("{} @ {:?}", self.log_id, current_dag_round.round());

            let (bcaster_ready_tx, bcaster_ready_rx) = mpsc::channel(1);
            // let this channel unbounded - there won't be many items, but every of them is essential
            let (collector_signal_tx, collector_signal_rx) = mpsc::unbounded_channel();
            let (own_point_state_tx, own_point_state_rx) = oneshot::channel();
            let bcaster_run = tokio::spawn(Self::bcaster_run(
                self.log_id.clone(),
                produce_own_point,
                self.dispatcher.clone(),
                self.peer_schedule.clone(),
                self.downloader.clone(),
                current_dag_round.clone(),
                prev_point,
                own_point_state_tx,
                bcaster_ready_tx,
                collector_signal_rx,
            ));

            let commit_run = tokio::spawn(self.dag.clone().commit(next_dag_round.clone()));
            let bcast_filter_upd = {
                let bcast_filter = self.broadcast_filter.clone();
                let round = current_dag_round.round().clone();
                tokio::spawn(async move { bcast_filter.advance_round(&round) })
            };

            let collector_run = tokio::spawn(self.collector.run(
                next_dag_round.clone(),
                own_point_state_rx,
                collector_signal_tx,
                bcaster_ready_rx,
            ));

            match tokio::join!(collector_run, bcaster_run, commit_run, bcast_filter_upd) {
                (Ok(collector_upd), Ok(new_prev_point), Ok(committed), Ok(_bcast_filter_upd)) => {
                    Self::log_committed(&self.log_id, &current_dag_round, &committed);
                    prev_point = new_prev_point;
                    produce_own_point = next_dag_round.round() == collector_upd.next_round();
                    self.collector = collector_upd;
                }
                (collector, bcaster, commit, bcast_filter_upd) => {
                    let msg = [
                        (collector.err(), "collector"),
                        (bcaster.err(), "broadcaster"),
                        (commit.err(), "commit"),
                        (bcast_filter_upd.err(), "broadcast filter update"),
                    ]
                    .into_iter()
                    .filter_map(|(res, name)| {
                        res.map(|err| format!("{name} task panicked: {err:?}"))
                    })
                    .join("; \n");
                    panic!("{}", msg)
                }
            }
        }
    }

    fn log_committed(
        log_id: &String,
        current_dag_round: &DagRound,
        committed: &Vec<(Arc<Point>, Vec<Arc<Point>>)>,
    ) {
        if committed.is_empty() {
            return;
        }
        if tracing::enabled!(tracing::Level::INFO) {
            let committed = committed
                .into_iter()
                .map(|(anchor, history)| {
                    let history = history
                        .iter()
                        .map(|point| format!("{:?}", point.id().ugly()))
                        .join(", ");
                    format!(
                        "anchor {:?} time {} : [ {history} ]",
                        anchor.id().ugly(),
                        anchor.body.time
                    )
                })
                .join("  ;  ");
            tracing::info!(
                "{log_id} @ {:?} committed {committed}",
                current_dag_round.round(),
            );
        }
    }
}
