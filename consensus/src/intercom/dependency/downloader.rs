use std::iter;
use std::sync::Arc;

use futures_util::future::BoxFuture;
use futures_util::stream::FuturesUnordered;
use futures_util::{FutureExt, StreamExt};
use rand::prelude::{IteratorRandom, SmallRng};
use rand::SeedableRng;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::{broadcast, watch};
use tokio::time::error::Elapsed;

use tycho_network::PeerId;
use tycho_util::FastHashMap;

use crate::dag::{DagRound, Verifier, WeakDagRound};
use crate::engine::MempoolConfig;
use crate::intercom::dto::{PeerState, PointByIdResponse};
use crate::intercom::{Dispatcher, PeerSchedule};
use crate::models::{DagPoint, NodeCount, PointId};

type DownloadResult = anyhow::Result<PointByIdResponse>;

#[derive(Clone)]
pub struct Downloader {
    local_id: Arc<String>,
    dispatcher: Dispatcher,
    peer_schedule: PeerSchedule,
}

impl Downloader {
    pub fn new(
        local_id: Arc<String>,
        dispatcher: &Dispatcher,
        peer_schedule: &PeerSchedule,
    ) -> Self {
        Self {
            local_id,
            peer_schedule: peer_schedule.clone(),
            dispatcher: dispatcher.clone(),
        }
    }

    pub async fn run(
        self,
        point_id: PointId,
        point_round: DagRound,
        // TODO it would be great to increase the number of dependants in-flight,
        //   but then the DAG needs to store some sort of updatable state machine
        //   instead of opaque Shared<JoinTask<DagPoint>>
        dependant: PeerId,
    ) -> DagPoint {
        assert_eq!(
            point_id.location.round,
            *point_round.round(),
            "point and DAG round mismatch"
        );
        // request point from its signers (any dependant is among them as point is already verified)
        let all_peers = self.peer_schedule.peers_for(&point_round.round().next());
        let Ok(node_count) = NodeCount::try_from(all_peers.len()) else {
            return DagPoint::NotExists(Arc::new(point_id));
        };
        // query author no matter if it is in the next round, but that can't affect 3F+1
        let all_peers = iter::once((point_id.location.author, PeerState::Resolved))
            // overwrite author's entry if it isn't really resolved;
            .chain(all_peers.iter().map(|(peer_id, state)| (*peer_id, *state)))
            .collect::<FastHashMap<PeerId, PeerState>>();
        if all_peers.is_empty() {
            return DagPoint::NotExists(Arc::new(point_id));
        };
        let mut priorities = vec![dependant, point_id.location.author];
        priorities.dedup();
        let (has_resolved_tx, has_resolved_rx) = watch::channel(false);
        DownloadTask {
            weak_dag_round: point_round.as_weak(),
            node_count,
            request: self.dispatcher.point_by_id_request(&point_id),
            point_id,
            updates: self.peer_schedule.updates(),
            has_resolved_tx,
            has_resolved_rx,
            in_flight: FuturesUnordered::new(),
            all_peers,
            parent: self,
            attempt: 0,
        }
        .run(&priorities)
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
    updates: broadcast::Receiver<(PeerId, PeerState)>,
    has_resolved_tx: watch::Sender<bool>,
    has_resolved_rx: watch::Receiver<bool>,
    in_flight: FuturesUnordered<
        BoxFuture<'static, (PeerId, Result<anyhow::Result<PointByIdResponse>, Elapsed>)>,
    >,
    attempt: u8,
}

impl DownloadTask {
    // point's author is a top priority; fallback priority is (any) dependent point's author
    // recursively: every dependency is expected to be signed by 2/3+1
    pub async fn run(mut self, priorities: &Vec<PeerId>) -> DagPoint {
        self.download_priorities(priorities);
        self.download();
        loop {
            tokio::select! {
                Some((peer_id, resolved)) = self.in_flight.next() =>
                     match self.match_resolved(peer_id, resolved).await {
                        Some(dag_point) => break dag_point,
                        None => continue
                    },
                update = self.updates.recv() => self.match_peer_updates(update),
            }
        }
    }

    fn download_priorities(&mut self, priorities: &Vec<PeerId>) {
        let priorities = priorities
            .into_iter()
            .filter(|p| {
                self.all_peers
                    .get(p)
                    .map_or(false, |&s| s == PeerState::Resolved)
            })
            .collect::<Vec<_>>();
        for resolved_priority in priorities {
            self.all_peers.remove_entry(resolved_priority);
            self.download_one(resolved_priority);
        }
    }

    fn download(&mut self) {
        self.attempt += 1;
        let count = (MempoolConfig::DOWNLOAD_PEERS as usize)
            .saturating_pow(self.attempt as u32)
            .saturating_sub(self.in_flight.len());

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
            tokio::time::timeout(
                MempoolConfig::DOWNLOAD_TIMEOUT,
                self.parent
                    .dispatcher
                    .request::<PointByIdResponse>(&peer_id, &self.request),
            )
            .map(move |result| (peer_id, result.map(|(_, r)| r)))
            .boxed(),
        );
    }

    async fn match_resolved(
        &mut self,
        peer_id: PeerId,
        resolved: Result<anyhow::Result<PointByIdResponse>, Elapsed>,
    ) -> Option<DagPoint> {
        match resolved {
            Err(_timeout) => _ = self.all_peers.remove(&peer_id),
            Ok(Err(_network_err)) => _ = self.all_peers.remove(&peer_id),
            Ok(Ok(PointByIdResponse(None))) => _ = self.all_peers.remove(&peer_id),
            Ok(Ok(PointByIdResponse(Some(point)))) => {
                if point.id() != self.point_id {
                    _ = self.all_peers.remove(&peer_id);
                }
                let Some(dag_round) = self.weak_dag_round.get() else {
                    // no more retries, too late;
                    // DAG could not have moved if this point was needed for commit
                    return Some(DagPoint::NotExists(Arc::new(self.point_id.clone())));
                };
                let point = Arc::new(point);
                match Verifier::verify(&point, &self.parent.peer_schedule) {
                    Ok(()) => {
                        return Some(
                            Verifier::validate(point, dag_round, self.parent.clone()).await,
                        )
                    }
                    Err(invalid @ DagPoint::Invalid(_)) => return Some(invalid),
                    Err(_not_exists) => _ = self.all_peers.remove(&peer_id), // ain't reliable peer
                }
            }
        };
        // the point does not exist when only 1F left unqeried,
        // assuming author and dependant are queried or unavailable
        if self.all_peers.len() < self.node_count.reliable_minority() {
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
