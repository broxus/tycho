use std::sync::Arc;

use everscale_crypto::ed25519::{KeyPair, SecretKey};
use tokio::sync::{mpsc, Notify};

use tycho_network::{DhtClient, OverlayService, PeerId};

use crate::dag::{DagRound, Producer};
use crate::intercom::{
    BroadcastFilter, Broadcaster, Dispatcher, PeerSchedule, PeerScheduleUpdater, Responder, Signer,
};
use crate::models::{Point, PrevPoint};

pub struct Engine {
    // dag: Arc<Mutex<Dag>>,
    peer_schedule: Arc<PeerSchedule>,
    dispatcher: Dispatcher,
    finished_dag_round: DagRound,
    signer: Signer,
    prev_point: Option<PrevPoint>,
    cur_point: Option<Arc<Point>>,
}

impl Engine {
    pub async fn add_next_peers(&self, next_peers: Vec<PeerId>) {}

    pub async fn new(
        secret_key: &SecretKey,
        dht_client: &DhtClient,
        overlay_service: &OverlayService,
        peers: &Vec<PeerId>,
    ) -> Self {
        let peer_schedule = Arc::new(PeerSchedule::new(Arc::new(KeyPair::from(secret_key))));

        let (bcast_tx, bcast_rx) = mpsc::unbounded_channel();

        let broadcast_filter = BroadcastFilter::new(peer_schedule.clone(), bcast_tx);

        let (sig_requests, sig_responses) = mpsc::unbounded_channel();

        let dispatcher = Dispatcher::new(
            &dht_client,
            &overlay_service,
            peers,
            Responder::new(broadcast_filter.clone(), sig_requests),
        );

        let genesis = Arc::new(crate::test_utils::genesis());
        // finished epoch
        peer_schedule.set_next_peers(&vec![genesis.body.location.author]);
        peer_schedule.set_next_start(genesis.body.location.round);
        peer_schedule.rotate();
        // current epoch
        peer_schedule.set_next_start(genesis.body.location.round.next());
        peer_schedule.set_next_peers(peers);
        peer_schedule.rotate();
        // start updater only after peers are populated into schedule
        PeerScheduleUpdater::run(dispatcher.overlay.clone(), peer_schedule.clone());

        // tOdO define if the last round is finished based on peer schedule
        //   move out from bcaster & signer ? where to get our last point from ?

        // tOdO в конце каждого раунда берем точку с триггером
        //  и комиттим
        //  * either own point contains Trigger
        //  * or search through last round to find the latest trigger
        //   * * can U do so without scan of a round ???

        let finished_dag_round = DagRound::genesis(&genesis, &peer_schedule).await;
        let signer = Signer::new(bcast_rx, sig_responses, finished_dag_round.round());

        Self {
            // dag: Arc::new(Mutex::new(dag)),
            peer_schedule,
            dispatcher,
            finished_dag_round,
            signer,
            prev_point: None,
            cur_point: None,
        }
    }

    pub async fn run(mut self) {
        loop {
            // FIXME must there be any next round as in Signer? check broadcast filter
            let current_round = self.finished_dag_round.next(self.peer_schedule.as_ref());

            self.cur_point = Producer::new_point(
                &self.finished_dag_round,
                &current_round,
                self.prev_point.as_ref(),
                vec![],
            )
            .await;

            let bcaster_ready = Arc::new(Notify::new());
            // let this channel unbounded - there won't be many items, but every of them is essential
            let (signer_signal_tx, mut signer_signal_rx) = mpsc::unbounded_channel();

            // TODO change round, then
            //   apply peer schedule and config changes if some
            //   spawn signer
            //   spawn producer + broadcaster
            //   spawn commit + drop dag tail (async?!) into futures ordered
            //     it shouldn't take longer than round;
            //     the other way it should make the change of rounds slower,
            //         in order to prevent unlimited DAG growth
            //   sync if signer detected a gap exceeding dag depth
            //   join
            if let Some(own_point) = &self.cur_point {
                let own_state = current_round
                    .insert_exact_validate(&own_point, &self.peer_schedule)
                    .await;
                let signer_run = tokio::spawn(self.signer.run(
                    current_round.clone(),
                    Some(own_point.clone()),
                    signer_signal_tx,
                    bcaster_ready.clone(),
                ));
                let bcaster_run = tokio::spawn(
                    Broadcaster::new(
                        &own_point,
                        &self.dispatcher,
                        &self.peer_schedule,
                        bcaster_ready,
                        signer_signal_rx,
                    )
                    .run(),
                );
                let joined = tokio::join!(signer_run, bcaster_run);
                match joined {
                    (Ok(signer_upd), Ok(evidence_or_reject)) => {
                        self.signer = signer_upd;
                        self.finished_dag_round = current_round; // FIXME must fill gaps with empty rounds
                        self.prev_point = evidence_or_reject.ok().map(|evidence| PrevPoint {
                            digest: own_point.digest.clone(),
                            evidence,
                        });
                    }
                    (Err(se), Err(be)) => {
                        panic!(
                        "Both Signer and Broadcaster panicked. Signer: {se:?}. Broadcaster: {be:?}"
                    )
                    }
                    (Err(se), _) => {
                        panic!("Signer panicked: {se:?}")
                    }
                    (_, Err(be)) => {
                        panic!("Broadcaster panicked: {be:?}")
                    }
                }
            } else {
                signer_signal_rx.close();
                bcaster_ready.notify_one();
                let signer_run = tokio::spawn(self.signer.run(
                    current_round.clone(),
                    None,
                    signer_signal_tx,
                    bcaster_ready,
                ))
                .await;
                match signer_run {
                    Ok(signer_upd) => {
                        self.finished_dag_round = current_round; // FIXME must fill gaps with empty rounds
                        self.signer = signer_upd;
                        self.prev_point = None;
                    }
                    Err(se) => panic!("Signer panicked: {se:?}"),
                }
            }
        }
    }
}

pub trait EngineTestExt {
    fn dispatcher(&self) -> &'_ Dispatcher;
}

impl EngineTestExt for Engine {
    fn dispatcher(&self) -> &'_ Dispatcher {
        &self.dispatcher
    }
}

// task 0: continue from where we stopped
// * load last state into DAG: some (un)finished round
// * create new round and point, if last round is finished
// -> start 1 & 2
//
// (always)
// task 1: accept broadcasts though filter
//
// (@ r+0 iff in peer schedule for r+0)
// task 2: broadcast + ask for signatures (to/from peers scheduled @ r+1)
//    (to support point receivers, even if "me" is not in schedule @ r+1)
//
// (@ r+0 iff in peer schedule for r+1)
// task 3: respond to signature requests (from peers @ [r-1; r+0])
//    (point authors must reject signatures they consider invalid)
//    (new nodes at the beginning of a new validation epoch
//     must sign points from the last round of a previous epoch)
//    (fast nodes that reached the end of their validation epoch
//       must continue to sign points of lagging nodes
//       until new validator set starts producing its shard-blocks -
//       they cannot finish the last round by counting signatures
//       and will advance by receiving batch of points from broadcast filter)

/*
async fn produce(
    &self,
    finished_round: &Arc<DagRound>,
    prev_point: Option<PrevPoint>,
    payload: Vec<Bytes>,
    peer_schedule: &PeerSchedule,
) -> Option<Point> {
    let new_round = Arc::new(finished_round.next(peer_schedule));
    self.broadcast_filter.advance_round(new_round.round()).await;

    if let Some(for_next_point) = self.peer_schedule.local_keys(&new_round.round().next()) {
        // respond to signature requests (mandatory inclusions)
        // _ = Signer::consume_broadcasts(filtered_rx, new_round.clone());
        // _ = Signer::on_validated(filtered_rx, new_round.clone(), Some(on_validated_tx));

        if let Some(for_witness) = self.peer_schedule.local_keys(new_round.round()) {
            // respond to signature requests to be included as witness
        };
    } else {
        // consume broadcasts without signing them
        // _ = Signer::consume_broadcasts(filtered_rx, new_round.clone());
    };
    if let Some(for_current_point) = self.peer_schedule.local_keys(new_round.round()) {
        let point = Producer::create_point(
            finished_round,
            &new_round,
            &for_current_point,
            prev_point,
            payload,
        )
        .await;
        let bcaster = Broadcaster::new(&point, dispatcher, peer_schedule);
        _ = bcaster.run().await;
        // broadcast, gather signatures as a mean of delivery (even if not producing next block)
        Some(point)
    } else {
        None
    }
}*/
