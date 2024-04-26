use std::sync::Arc;

use everscale_crypto::ed25519::{KeyPair, SecretKey};
use itertools::Itertools;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;

use tycho_network::{DhtClient, OverlayService, PeerId};

use crate::dag::{Dag, DagRound, Producer, WeakDagRound};
use crate::intercom::{
    BroadcastFilter, Broadcaster, BroadcasterSignal, Collector, Dispatcher, Downloader,
    PeerSchedule, PeerScheduleUpdater, Responder, Uploader,
};
use crate::models::{Point, PrevPoint};

pub struct Engine {
    dag: Dag,
    local_id: Arc<String>,
    peer_schedule: Arc<PeerSchedule>,
    dispatcher: Dispatcher,
    downloader: Downloader,
    collector: Collector,
    broadcast_filter: BroadcastFilter,
    cur_point: Option<Arc<Point>>,
    current_dag_round: DagRound,
    top_dag_round_watch: watch::Sender<WeakDagRound>,
    tasks: JoinSet<()>, // should be JoinSet<!> https://github.com/rust-lang/rust/issues/35121
}

impl Engine {
    pub async fn new(
        secret_key: &SecretKey,
        dht_client: &DhtClient,
        overlay_service: &OverlayService,
        peers: &Vec<PeerId>,
    ) -> Self {
        let key_pair = KeyPair::from(secret_key);
        let local_id = Arc::new(format!("{:.4?}", PeerId::from(key_pair.public_key)));
        let peer_schedule = Arc::new(PeerSchedule::new(Arc::new(key_pair)));

        let (bcast_tx, bcast_rx) = mpsc::unbounded_channel();

        let broadcast_filter =
            BroadcastFilter::new(local_id.clone(), peer_schedule.clone(), bcast_tx);

        let (sig_requests, sig_responses) = mpsc::unbounded_channel();

        let (uploader_tx, uploader_rx) = mpsc::unbounded_channel();

        let dispatcher = Dispatcher::new(
            &dht_client,
            &overlay_service,
            peers,
            Responder::new(
                local_id.clone(),
                broadcast_filter.clone(),
                sig_requests,
                uploader_tx,
            ),
        );

        let genesis = Arc::new(crate::test_utils::genesis());
        // finished epoch
        peer_schedule.set_next_peers(&vec![(genesis.body.location.author, false)]);
        peer_schedule.set_next_start(genesis.body.location.round);
        peer_schedule.rotate();
        // current epoch
        peer_schedule.set_next_start(genesis.body.location.round.next());
        peer_schedule.set_next_peers(
            &dispatcher
                .overlay
                .read_entries()
                .iter()
                .map(|a| (a.peer_id, a.resolver_handle.is_resolved()))
                .collect(),
        );
        peer_schedule.rotate();
        // start updater only after peers are populated into schedule
        PeerScheduleUpdater::run(dispatcher.overlay.clone(), peer_schedule.clone());

        // tOdO define if the last round is finished based on peer schedule
        //   move out from bcaster & collector ? where to get our last point from ?

        // tOdO в конце каждого раунда берем точку с триггером
        //  и комиттим
        //  * either own point contains Trigger
        //  * or search through last round to find the latest trigger
        //   * * can U do so without scan of a round ???

        let dag = Dag::new();
        let current_dag_round = dag.get_or_insert(DagRound::genesis(&genesis, &peer_schedule));

        let (top_dag_round_watch, top_dag_round_rx) = watch::channel(current_dag_round.as_weak());

        let mut tasks = JoinSet::new();
        let uploader = Uploader::new(uploader_rx, top_dag_round_rx);
        tasks.spawn(async move {
            uploader.run().await;
        });

        let downloader = Downloader::new(local_id.clone(), &dispatcher, &peer_schedule);

        let genesis_state = current_dag_round
            .insert_exact_validate(&genesis, &peer_schedule, &downloader)
            .await;
        let collector = Collector::new(
            local_id.clone(),
            &downloader,
            bcast_rx,
            sig_responses,
            genesis_state.into_iter(),
            current_dag_round.round().clone(),
        );

        Self {
            dag,
            local_id,
            peer_schedule,
            dispatcher,
            downloader,
            collector,
            broadcast_filter,
            cur_point: None,
            current_dag_round,
            top_dag_round_watch,
            tasks,
        }
    }

    pub async fn run(mut self) -> ! {
        loop {
            let next_dag_round = self
                .dag
                .get_or_insert(self.current_dag_round.next(self.peer_schedule.as_ref()));
            self.top_dag_round_watch.send(next_dag_round.as_weak()).ok();

            let (bcaster_ready_tx, bcaster_ready_rx) = mpsc::channel(1);
            // let this channel unbounded - there won't be many items, but every of them is essential
            let (collector_signal_tx, mut collector_signal_rx) = mpsc::unbounded_channel();

            let commit_run = tokio::spawn(self.dag.clone().commit(next_dag_round.clone()));
            let bcast_filter_upd = {
                let bcast_filter = self.broadcast_filter.clone();
                let round = next_dag_round.round().clone();
                tokio::spawn(async move { bcast_filter.advance_round(&round) })
            };
            // TODO change round, then
            //   apply peer schedule and config changes if some
            //   spawn collector
            //   spawn producer + broadcaster
            //   spawn commit + drop dag tail (async?!) into futures ordered
            //     it shouldn't take longer than round;
            //     the other way it should make the change of rounds slower,
            //         in order to prevent unlimited DAG growth
            //   sync if collector detected a gap exceeding dag depth
            //   join
            if let Some(own_point) = self.cur_point {
                let own_state = self
                    .current_dag_round
                    .insert_exact_validate(&own_point, &self.peer_schedule, &self.downloader)
                    .await;
                let collector_run = tokio::spawn(self.collector.run(
                    next_dag_round.clone(),
                    Some(own_point.clone()),
                    collector_signal_tx,
                    bcaster_ready_rx,
                ));
                let bcaster_run = tokio::spawn(
                    Broadcaster::new(
                        &self.local_id,
                        &own_point,
                        &self.dispatcher,
                        &self.peer_schedule,
                        bcaster_ready_tx,
                        collector_signal_rx,
                    )
                    .run(),
                );
                match tokio::join!(collector_run, bcaster_run, commit_run, bcast_filter_upd) {
                    (Ok(collector_upd), Ok(evidence), Ok(committed), Ok(_bcast_filter_upd)) => {
                        tracing::info!("committed {:.4?}", committed);
                        let prev_point = Some(PrevPoint {
                            digest: own_point.digest.clone(),
                            evidence: evidence.into_iter().collect(),
                        });
                        if collector_upd.next_round() == next_dag_round.round() {
                            self.cur_point = Producer::new_point(
                                &self.current_dag_round,
                                &next_dag_round,
                                prev_point.as_ref(),
                                vec![],
                            )
                            .await;
                        } else {
                            todo!("must fill gaps with empty rounds")
                        }
                        self.current_dag_round = next_dag_round;
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
            } else {
                collector_signal_rx.close();
                _ = bcaster_ready_tx.send(BroadcasterSignal::Ok).await;
                let collector_run = tokio::spawn(self.collector.run(
                    next_dag_round.clone(),
                    None,
                    collector_signal_tx,
                    bcaster_ready_rx,
                ));
                match tokio::join!(collector_run, commit_run, bcast_filter_upd) {
                    (Ok(collector_upd), Ok(committed), Ok(_bcast_filter_upd)) => {
                        tracing::info!("committed {:.4?}", committed);
                        self.cur_point = Producer::new_point(
                            &self.current_dag_round,
                            &next_dag_round,
                            None,
                            vec![],
                        )
                        .await;
                        self.current_dag_round = next_dag_round; // FIXME must fill gaps with empty rounds
                        self.collector = collector_upd;
                    }
                    (collector, commit, bcast_filter_upd) => {
                        let msg = [
                            (collector.err(), "collector"),
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
    }
}
