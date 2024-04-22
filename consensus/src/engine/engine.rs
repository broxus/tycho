use std::sync::Arc;

use everscale_crypto::ed25519::{KeyPair, SecretKey};
use itertools::Itertools;
use tokio::sync::{mpsc, Notify};

use tycho_network::{DhtClient, OverlayService, PeerId};

use crate::dag::{Dag, DagRound, Producer};
use crate::intercom::{
    BroadcastFilter, Broadcaster, Dispatcher, PeerSchedule, PeerScheduleUpdater, Responder, Signer,
};
use crate::models::{Point, PrevPoint};

pub struct Engine {
    dag: Dag,
    local_id: Arc<String>,
    peer_schedule: Arc<PeerSchedule>,
    dispatcher: Dispatcher,
    signer: Signer,
    prev_point: Option<PrevPoint>,
    cur_point: Option<Arc<Point>>,
    current_dag_round: DagRound,
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
        //   move out from bcaster & signer ? where to get our last point from ?

        // tOdO в конце каждого раунда берем точку с триггером
        //  и комиттим
        //  * either own point contains Trigger
        //  * or search through last round to find the latest trigger
        //   * * can U do so without scan of a round ???

        let dag = Dag::new();
        let current_dag_round = dag.get_or_insert(DagRound::genesis(&genesis, &peer_schedule));

        let genesis_state = current_dag_round
            .insert_exact_validate(&genesis, &peer_schedule)
            .await;
        let signer = Signer::new(
            local_id.clone(),
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
            signer,
            prev_point: None,
            cur_point: None,
            current_dag_round,
        }
    }

    pub async fn run(mut self) {
        loop {
            let next_dag_round = self
                .dag
                .get_or_insert(self.current_dag_round.next(self.peer_schedule.as_ref()));

            let bcaster_ready = Arc::new(Notify::new());
            // let this channel unbounded - there won't be many items, but every of them is essential
            let (signer_signal_tx, mut signer_signal_rx) = mpsc::unbounded_channel();

            let commit_run = tokio::spawn(self.dag.clone().commit(next_dag_round.clone()));

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
            if let Some(own_point) = self.cur_point {
                let own_state = self
                    .current_dag_round
                    .insert_exact_validate(&own_point, &self.peer_schedule)
                    .await;
                let signer_run = tokio::spawn(self.signer.run(
                    next_dag_round.clone(),
                    Some(own_point.clone()),
                    signer_signal_tx,
                    bcaster_ready.clone(),
                ));
                let bcaster_run = tokio::spawn(
                    Broadcaster::new(
                        &self.local_id,
                        &own_point,
                        &self.dispatcher,
                        &self.peer_schedule,
                        bcaster_ready,
                        signer_signal_rx,
                    )
                    .run(),
                );
                let joined = tokio::join!(signer_run, bcaster_run, commit_run);
                match joined {
                    (Ok(signer_upd), Ok(evidence_or_reject), Ok(committed)) => {
                        tracing::info!("committed {:#.4?}", committed);
                        self.prev_point = evidence_or_reject.ok().map(|evidence| PrevPoint {
                            digest: own_point.digest.clone(),
                            evidence: evidence.into_iter().collect(),
                        });
                        self.cur_point = Producer::new_point(
                            &self.current_dag_round,
                            &next_dag_round,
                            self.prev_point.as_ref(),
                            vec![],
                        )
                        .await;
                        self.current_dag_round = next_dag_round; // FIXME must fill gaps with empty rounds
                        self.signer = signer_upd;
                    }
                    (signer, bcaster, commit) => {
                        let msg = [
                            (signer.err(), "signer"),
                            (bcaster.err(), "broadcaster"),
                            (commit.err(), "commit"),
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
                signer_signal_rx.close();
                bcaster_ready.notify_one();
                let signer_run = tokio::spawn(self.signer.run(
                    next_dag_round.clone(),
                    None,
                    signer_signal_tx,
                    bcaster_ready,
                ));
                match tokio::join!(signer_run, commit_run) {
                    (Ok(signer_upd), Ok(committed)) => {
                        tracing::info!("committed {:#.4?}", committed);
                        self.prev_point = None;
                        self.cur_point = Producer::new_point(
                            &self.current_dag_round,
                            &next_dag_round,
                            self.prev_point.as_ref(),
                            vec![],
                        )
                        .await;
                        self.current_dag_round = next_dag_round; // FIXME must fill gaps with empty rounds
                        self.signer = signer_upd;
                    }
                    (signer, commit) => {
                        let msg = [(signer.err(), "signer"), (commit.err(), "commit")]
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
