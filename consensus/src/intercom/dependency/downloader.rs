use std::iter;
use std::sync::Arc;

use futures_util::future::BoxFuture;
use futures_util::stream::FuturesUnordered;
use futures_util::{FutureExt, StreamExt};
use rand::prelude::{IteratorRandom, SmallRng};
use rand::SeedableRng;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::{broadcast, watch};
use tycho_network::PeerId;
use tycho_util::{FastHashMap, FastHashSet};

use crate::dag::{Verifier, WeakDagRound};
use crate::engine::MempoolConfig;
use crate::intercom::dto::{PeerState, PointByIdResponse};
use crate::intercom::{Dispatcher, PeerSchedule};
use crate::models::{DagPoint, NodeCount, PointId, Ugly};

type DownloadResult = anyhow::Result<PointByIdResponse>;

#[derive(Clone)]
pub struct Downloader {
    log_id: Arc<String>,
    dispatcher: Dispatcher,
    peer_schedule: PeerSchedule,
}

impl Downloader {
    pub fn new(log_id: Arc<String>, dispatcher: &Dispatcher, peer_schedule: &PeerSchedule) -> Self {
        Self {
            log_id,
            peer_schedule: peer_schedule.clone(),
            dispatcher: dispatcher.clone(),
        }
    }

    pub async fn run(
        self,
        point_id: PointId,
        point_round: WeakDagRound,
        // TODO it would be great to increase the number of dependants in-flight,
        //   but then the DAG needs to store some sort of updatable state machine
        //   instead of opaque Shared<JoinTask<DagPoint>>
        dependant: PeerId,
    ) -> DagPoint {
        let Some(point_round_temp) = point_round.get() else {
            return DagPoint::NotExists(Arc::new(point_id));
        };
        assert_eq!(
            point_id.location.round,
            *point_round_temp.round(),
            "point and DAG round mismatch"
        );
        // request point from its signers (any dependant is among them as point is already verified)
        let all_peers = self
            .peer_schedule
            .peers_for(&point_round_temp.round().next())
            .iter()
            .map(|(peer_id, state)| (*peer_id, *state))
            .collect::<FastHashMap<PeerId, PeerState>>();
        let Ok(node_count) = NodeCount::try_from(all_peers.len()) else {
            return DagPoint::NotExists(Arc::new(point_id));
        };
        // query author no matter if it is in the next round, but that can't affect 3F+1
        let completed = match !all_peers.contains_key(&point_id.location.author)
            && self.peer_schedule.is_resolved(&point_id.location.author)
        {
            true => -1,
            false => 0,
        };
        if all_peers.is_empty() {
            return DagPoint::NotExists(Arc::new(point_id));
        };
        let mandatory = iter::once(dependant)
            .chain(iter::once(point_id.location.author))
            .collect();
        let (has_resolved_tx, has_resolved_rx) = watch::channel(false);
        _ = point_round_temp; // do not leak strong ref across unlimited await
        DownloadTask {
            weak_dag_round: point_round,
            node_count,
            request: self.dispatcher.point_by_id_request(&point_id),
            point_id,
            updates: self.peer_schedule.updates(),
            has_resolved_tx,
            has_resolved_rx,
            in_flight: FuturesUnordered::new(),
            completed,
            mandatory,
            all_peers,
            parent: self,
            attempt: 0,
        }
        .run()
        .await
    }
}

struct DownloadTask {
    parent: Downloader,

    weak_dag_round: WeakDagRound,
    node_count: NodeCount,

    request: tycho_network::Request,
    point_id: PointId,

    all_peers: FastHashMap<PeerId, PeerState>,
    mandatory: FastHashSet<PeerId>,
    updates: broadcast::Receiver<(PeerId, PeerState)>,
    has_resolved_tx: watch::Sender<bool>,
    has_resolved_rx: watch::Receiver<bool>,
    in_flight: FuturesUnordered<BoxFuture<'static, (PeerId, anyhow::Result<PointByIdResponse>)>>,
    completed: i16,
    attempt: u8,
}

impl DownloadTask {
    // point's author is a top priority; fallback priority is (any) dependent point's author
    // recursively: every dependency is expected to be signed by 2/3+1
    pub async fn run(mut self) -> DagPoint {
        self.download_mandatory();
        self.download();
        let mut interval = tokio::time::interval(MempoolConfig::DOWNLOAD_SPAWN_INTERVAL);
        loop {
            tokio::select! {
                Some((peer_id, resolved)) = self.in_flight.next() =>
                     match self.match_resolved(peer_id, resolved).await {
                        Some(dag_point) => break dag_point,
                        None => continue
                    },
                _ = interval.tick() => self.download(),
                update = self.updates.recv() => self.match_peer_updates(update),
            }
        }
    }

    fn download_mandatory(&mut self) {
        let mandatory = self
            .mandatory
            .iter()
            .filter(|p| {
                self.all_peers
                    .get(p)
                    .map_or(false, |&s| s == PeerState::Resolved)
            })
            .cloned()
            .collect::<Vec<_>>();
        for peer_id in mandatory {
            self.all_peers.remove_entry(&peer_id);
            self.download_one(&peer_id);
        }
    }

    fn download(&mut self) {
        self.attempt += 1;
        let count = (MempoolConfig::DOWNLOAD_PEERS as usize)
            .saturating_mul(self.attempt as usize)
            .min(self.all_peers.len());

        for peer_id in self
            .all_peers
            .iter()
            .filter(|(_, &p)| p == PeerState::Resolved)
            .choose_multiple(&mut SmallRng::from_entropy(), count)
            .into_iter()
            .map(|(peer_id, _)| *peer_id)
            .collect::<Vec<_>>()
        {
            self.all_peers.remove_entry(&peer_id);
            self.download_one(&peer_id);
        }
    }

    fn download_one(&mut self, peer_id: &PeerId) {
        let peer_id = peer_id.clone();
        self.in_flight.push(
            self.parent
                .dispatcher
                .query::<PointByIdResponse>(&peer_id, &self.request)
                .boxed(),
        );
    }

    async fn match_resolved(
        &mut self,
        peer_id: PeerId,
        resolved: anyhow::Result<PointByIdResponse>,
    ) -> Option<DagPoint> {
        match resolved {
            Err(network_err) => {
                tracing::error!(
                    "{} : {peer_id:.4?} network error: {network_err}",
                    self.parent.log_id
                );
            }
            Ok(PointByIdResponse(None)) => {
                if self.mandatory.remove(&peer_id) {
                    // it's a ban in case permanent storage is used,
                    // the other way - peer can could have advanced on full DAG_DEPTH already
                    tracing::error!(
                        "{} : {peer_id:.4?} must have returned {:?}",
                        self.parent.log_id,
                        self.point_id.ugly()
                    );
                } else {
                    tracing::debug!(
                        "{} : {peer_id:.4?} didn't return {:?}",
                        self.parent.log_id,
                        self.point_id.ugly()
                    );
                }
            }
            Ok(PointByIdResponse(Some(point))) => {
                if point.id() != self.point_id {
                    // it's a ban
                    tracing::error!(
                        "{} : {peer_id:.4?} returned wrong point",
                        self.parent.log_id
                    );
                }
                match Verifier::verify(&point, &self.parent.peer_schedule) {
                    Ok(()) => {
                        let validated = Verifier::validate(
                            point,
                            self.weak_dag_round.clone(),
                            self.parent.clone(),
                        )
                        .await;
                        if validated.trusted().is_some() {
                            tracing::debug!(
                                "{} : downloaded dependency {:?}",
                                self.parent.log_id,
                                validated.ugly()
                            )
                        } else {
                            tracing::error!(
                                "{} : downloaded dependency validated as {:?}",
                                self.parent.log_id,
                                validated.ugly()
                            )
                        }
                        return Some(validated);
                    }
                    Err(invalid @ DagPoint::Invalid(_)) => {
                        tracing::error!(
                            "{} : downloaded dependency {:?}",
                            self.parent.log_id,
                            invalid.ugly()
                        );
                        return Some(invalid);
                    }
                    Err(_not_exists) => {
                        tracing::error!(
                            "{} : downloaded dependency {:?}, peer is not reliable",
                            self.parent.log_id,
                            _not_exists.ugly()
                        );
                    }
                }
            }
        };
        // the point does not exist when only 1F left unqueried,
        // assuming author and dependant are queried or unavailable
        self.completed += 1;
        if self.completed >= self.node_count.majority() as i16 {
            return Some(DagPoint::NotExists(Arc::new(self.point_id.clone())));
        }
        if self.in_flight.is_empty() {
            self.has_resolved_tx.send(false).ok();
            self.download();
        };
        if self.in_flight.is_empty() {
            // mempool inclusion guarantees must be satisfied if less than 2F+1 nodes are online;
            // so we should stall, waiting for peers to connect
            if let Err(e) = self.has_resolved_rx.wait_for(|is| *is).await {
                panic!("Downloader waiting for new resolved peer {e}")
            };
            self.download();
        };
        None
    }

    fn match_peer_updates(&mut self, result: Result<(PeerId, PeerState), RecvError>) {
        match result {
            Ok((peer_id, new)) => {
                let mut is_resolved = false;
                self.all_peers.entry(peer_id).and_modify(|old| {
                    if *old == PeerState::Unknown && new == PeerState::Resolved {
                        is_resolved = true;
                    }
                    *old = new;
                });
                if is_resolved {
                    self.has_resolved_tx.send(true).ok();
                }
            }
            Err(err @ RecvError::Lagged(_)) => {
                tracing::error!("Downloader peer updates {err}")
            }
            Err(err @ RecvError::Closed) => {
                panic!("Downloader peer updates {err}")
            }
        }
    }
}
