use std::collections::hash_map::Entry;
use std::sync::Arc;

use futures_util::future::BoxFuture;
use futures_util::stream::FuturesUnordered;
use futures_util::{FutureExt, StreamExt};
use rand::{thread_rng, RngCore};
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::{broadcast, mpsc};
use tycho_network::PeerId;
use tycho_util::FastHashMap;

use crate::dag::{Verifier, WeakDagRound};
use crate::dyn_event;
use crate::effects::{AltFormat, Effects, EffectsContext, ValidateContext};
use crate::engine::MempoolConfig;
use crate::intercom::dto::{PeerState, PointByIdResponse};
use crate::intercom::{Dispatcher, PeerSchedule};
use crate::models::{DagPoint, NodeCount, PointId};

type DownloadResult = anyhow::Result<PointByIdResponse>;

#[derive(Clone)]
pub struct Downloader {
    inner: Arc<DownloaderInner>,
}

struct DownloaderInner {
    dispatcher: Dispatcher,
    peer_schedule: PeerSchedule,
}

#[derive(Debug)]
struct PeerStatus {
    state: PeerState,
    failed_attempts: usize,
    /// `true` for peers that depend on current point, i.e. included it directly;
    /// requests are made without waiting for next attempt;
    /// entries are never deleted, because they may be not resolved at the moment of insertion
    is_depender: bool,
    /// has uncompleted request just now
    is_in_flight: bool,
}

impl Downloader {
    pub fn new(dispatcher: &Dispatcher, peer_schedule: &PeerSchedule) -> Self {
        Self {
            inner: Arc::new(DownloaderInner {
                dispatcher: dispatcher.clone(),
                peer_schedule: peer_schedule.clone(),
            }),
        }
    }

    pub async fn run(
        self,
        point_id: PointId,
        point_dag_round: WeakDagRound,
        dependers: mpsc::UnboundedReceiver<PeerId>,
        parent_effects: Effects<ValidateContext>,
    ) -> DagPoint {
        let effects = Effects::<DownloadContext>::new(&parent_effects, &point_id);
        let span_guard = effects.span().enter();
        let peer_schedule = &self.inner.peer_schedule;
        let Some(point_round_temp) = point_dag_round.get() else {
            tracing::warn!(
                "marking point as NotExists as its round is already not in dag, \
                too deep recursion must be prevented by Verifier"
            );
            return DagPoint::NotExists(Arc::new(point_id));
        };
        assert_eq!(
            point_id.location.round,
            point_round_temp.round(),
            "point and DAG round mismatch"
        );
        // request point from its signers (any depender is among them as point is already verified)
        let mut undone_peers = peer_schedule
            .peers_for(point_round_temp.round().next())
            .iter()
            .map(|(peer_id, state)| {
                (*peer_id, PeerStatus {
                    state: *state,
                    failed_attempts: 0,
                    is_depender: peer_id == point_id.location.author,
                    is_in_flight: false,
                })
            })
            .collect::<FastHashMap<_, _>>();
        let Ok(node_count) = NodeCount::try_from(undone_peers.len()) else {
            tracing::warn!(
                "marking point as NotExists as its round peers are unknown, \
                too deep recursion must be prevented by Verifier"
            );
            return DagPoint::NotExists(Arc::new(point_id));
        };
        // query author no matter if it is in the next round, but that can't affect 3F+1
        let done_peers = match undone_peers.entry(point_id.location.author) {
            Entry::Occupied(_) => 0,
            Entry::Vacant(vacant) => {
                vacant.insert(PeerStatus {
                    state: peer_schedule
                        .peer_state(&point_id.location.author)
                        .unwrap_or(PeerState::Unknown),
                    failed_attempts: 0,
                    is_depender: true,
                    is_in_flight: false,
                });
                -1
            }
        };
        let updates = peer_schedule.updates();
        // do not leak strong ref across unlimited await
        drop(point_round_temp);
        drop(span_guard);
        DownloadTask {
            parent: self,
            effects,
            point_dag_round,
            node_count,
            request: Dispatcher::point_by_id_request(&point_id),
            point_id,
            undone_peers,
            done_peers,
            downloading: FuturesUnordered::new(),
            validating: FuturesUnordered::new(),
            dependers,
            updates,
            attempt: 0,
            skip_next_attempt: false,
        }
        .run()
        .await
    }
}

struct DownloadTask {
    parent: Downloader,
    effects: Effects<DownloadContext>,

    point_dag_round: WeakDagRound,
    node_count: NodeCount,

    request: tycho_network::Request,
    point_id: PointId,

    undone_peers: FastHashMap<PeerId, PeerStatus>,
    done_peers: i16,
    downloading: FuturesUnordered<BoxFuture<'static, (PeerId, anyhow::Result<PointByIdResponse>)>>,
    validating: FuturesUnordered<BoxFuture<'static, (PeerId, DagPoint)>>,

    /// populated by waiting validation tasks, source of [`mandatory`] set
    dependers: mpsc::UnboundedReceiver<PeerId>,
    updates: broadcast::Receiver<(PeerId, PeerState)>,

    attempt: u8,
    /// skip time-driven attempt if an attempt was init by empty task queue
    skip_next_attempt: bool,
}

impl DownloadTask {
    // point's author is a top priority; fallback priority is (any) dependent point's author
    // recursively: every dependency is expected to be signed by 2/3+1
    pub async fn run(mut self) -> DagPoint {
        // always ask the author
        let author = self.point_id.location.author;
        self.add_depender(&author);
        self.download_random(true);
        let mut interval = tokio::time::interval(MempoolConfig::DOWNLOAD_INTERVAL);
        let dag_point = loop {
            tokio::select! {
                Some((peer_id, downloaded)) = self.downloading.next() =>
                     match self.match_downloaded(peer_id, downloaded) {
                        Some(dag_point) => break dag_point,
                        None => continue
                    },
                Some((peer_id, validated)) = self.validating.next() =>
                     match self.match_validated(&peer_id, validated) {
                        Some(dag_point) => break dag_point,
                        None => continue
                    },
                Some(depender) = self.dependers.recv() => self.add_depender(&depender),
                _ = interval.tick() => self.download_random(false),
                update = self.updates.recv() => self.match_peer_updates(update),
            }
        };
        // clean the channel, it will stay in `DagPointFuture` that owns current task
        self.dependers.close();
        dag_point
    }

    fn add_depender(&mut self, peer_id: &PeerId) {
        let is_suitable = match self.undone_peers.get_mut(peer_id) {
            Some(state) if !state.is_depender => {
                state.is_depender = true;
                !state.is_in_flight
                    && state.state == PeerState::Resolved
                    // do not re-download immediately if already requested
                    && state.failed_attempts == 0
            }
            _ => false, // either already marked or requested and removed, no panic
        };
        if is_suitable {
            // request immediately just once
            self.download_one(peer_id);
        }
    }

    fn download_random(&mut self, force: bool) {
        if self.skip_next_attempt {
            // reset `skip_attempt` flag; do nothing, if not forced
            self.skip_next_attempt = false;
            if !force {
                return;
            }
        }
        self.attempt = self.attempt.wrapping_add(1);
        let count = (MempoolConfig::DOWNLOAD_PEERS as usize)
            .saturating_mul(self.attempt as usize)
            .min(self.undone_peers.len());

        let mut filtered = self
            .undone_peers
            .iter()
            .filter(|(_, p)| p.state == PeerState::Resolved && !p.is_in_flight)
            .map(|(peer_id, status)| {
                (
                    *peer_id,
                    (
                        // try every peer, until all are tried the same amount of times
                        status.failed_attempts,
                        // try mandatory peers before others each loop
                        u8::from(!status.is_depender),
                        // randomise within group
                        thread_rng().next_u32(),
                    ),
                )
            })
            .collect::<Vec<_>>();
        filtered.sort_unstable_by_key(|kv| kv.1);

        for (peer_id, _) in filtered.iter().take(count) {
            self.download_one(peer_id);
        }
    }

    fn download_one(&mut self, peer_id: &PeerId) {
        let status = self
            .undone_peers
            .get_mut(peer_id)
            .unwrap_or_else(|| panic!("Coding error: peer not in map {}", peer_id.alt()));
        assert!(
            !status.is_in_flight,
            "already downloading from peer {} status {:?}",
            peer_id.alt(),
            status
        );
        status.is_in_flight = true;

        self.downloading.push(
            self.parent
                .inner
                .dispatcher
                .query::<PointByIdResponse>(peer_id, &self.request)
                .boxed(),
        );
    }

    fn match_downloaded(
        &mut self,
        peer_id: PeerId,
        resolved: anyhow::Result<PointByIdResponse>,
    ) -> Option<DagPoint> {
        match resolved {
            Err(network_err) => {
                let status = self
                    .undone_peers
                    .get_mut(&peer_id)
                    .unwrap_or_else(|| panic!("Coding error: peer not in map {}", peer_id.alt()));
                status.is_in_flight = false;
                status.failed_attempts += 1;
                tracing::warn!(
                    parent: self.effects.span(),
                    peer = display(peer_id.alt()),
                    error = display(network_err),
                    "network error",
                );
            }
            Ok(PointByIdResponse(None)) => {
                self.done_peers += 1;
                match self.undone_peers.remove(&peer_id) {
                    Some(state) if state.is_depender => {
                        // if points are persisted in storage - it's a ban;
                        // else - peer evicted this point from its cache, as the point
                        // is at least DAG_DEPTH rounds older than current consensus round
                        tracing::warn!(
                            parent: self.effects.span(),
                            peer = display(peer_id.alt()),
                            "must have returned",
                        );
                    }
                    Some(_) => {
                        tracing::debug!(
                            parent: self.effects.span(),
                            peer = display(peer_id.alt()),
                            "didn't return",
                        );
                    }
                    None => {
                        let _guard = self.effects.span().enter();
                        panic!("already removed peer {}", peer_id.alt())
                    }
                }
            }
            Ok(PointByIdResponse(Some(point))) if point.id() != self.point_id => {
                self.done_peers += 1;
                self.undone_peers.remove(&peer_id);
                // it's a ban
                tracing::error!(
                    parent: self.effects.span(),
                    peer_id = display(peer_id.alt()),
                    author = display(point.body().location.author.alt()),
                    round = point.body().location.round.0,
                    digest = display(point.digest().alt()),
                    "returned wrong point",
                );
            }
            Ok(PointByIdResponse(Some(point))) => {
                self.undone_peers.remove(&peer_id);
                match Verifier::verify(&point, &self.parent.inner.peer_schedule) {
                    Ok(()) => {
                        // count peer as done only after validation
                        self.validating.push(
                            Verifier::validate(
                                point,
                                self.point_dag_round.clone(),
                                self.parent.clone(),
                                self.effects.span().clone(),
                            )
                            .map(move |dag_point| (peer_id, dag_point))
                            .boxed(),
                        );
                        tracing::trace!(
                            parent: self.effects.span(),
                            peer = display(peer_id.alt()),
                            "downloaded, now validating",
                        );
                    }
                    Err(dag_point) => {
                        // reliable peer won't return unverifiable point
                        self.done_peers += 1;
                        assert!(
                            dag_point.valid().is_none(),
                            "Coding error: verify() cannot result into a valid point"
                        );
                        tracing::error!(
                            parent: self.effects.span(),
                            result = display(dag_point.alt()),
                            peer = display(peer_id.alt()),
                            "downloaded",
                        );
                    }
                };
            }
        };
        self.maybe_not_downloaded()
    }

    fn match_validated(&mut self, peer_id: &PeerId, dag_point: DagPoint) -> Option<DagPoint> {
        // peer is already removed from undone
        self.done_peers += 1;
        let level = if dag_point.trusted().is_some() {
            tracing::Level::DEBUG
        } else if dag_point.valid().is_some() {
            tracing::Level::WARN
        } else {
            tracing::Level::ERROR
        };
        dyn_event!(
            parent: self.effects.span(),
            level,
            result = display(dag_point.alt()),
            peer = display(peer_id.alt()),
            "validated",
        );
        match dag_point {
            DagPoint::NotExists(_) => {
                // peer not reliable, it's a ban - in case Verifier prevents too deep recursion
                self.maybe_not_downloaded()
            }
            dag_point => Some(dag_point),
        }
    }

    fn maybe_not_downloaded(&mut self) -> Option<DagPoint> {
        if self.done_peers >= self.node_count.majority() as i16 {
            // the only normal case to resolve into `NotExists`
            tracing::warn!(
                parent: self.effects.span(),
                "not downloaded from majority",
            );
            Some(DagPoint::NotExists(Arc::new(self.point_id.clone())))
        } else {
            if self.downloading.is_empty() && self.validating.is_empty() {
                self.download_random(true);
                self.skip_next_attempt = true;
            }
            None
        }
    }

    fn match_peer_updates(&mut self, result: Result<(PeerId, PeerState), RecvError>) {
        match result {
            Ok((peer_id, new)) => {
                let mut is_suitable = false;
                self.undone_peers.entry(peer_id).and_modify(|status| {
                    is_suitable = !status.is_in_flight
                        && status.is_depender
                        && status.failed_attempts == 0
                        && status.state == PeerState::Unknown
                        && new == PeerState::Resolved;
                    status.state = new;
                });
                if is_suitable {
                    self.download_one(&peer_id);
                }
            }
            Err(err @ RecvError::Lagged(_)) => {
                tracing::error!(
                    parent: self.effects.span(),
                    error = display(err),
                    "peer updates"
                );
            }
            Err(err @ RecvError::Closed) => {
                let _span = self.effects.span().enter();
                panic!("peer updates {err}")
            }
        }
    }
}
struct DownloadContext;
impl EffectsContext for DownloadContext {}
impl Effects<DownloadContext> {
    fn new(parent: &Effects<ValidateContext>, point_id: &PointId) -> Self {
        Self::new_child(parent.span(), || {
            tracing::error_span!(
                "download",
                author = display(point_id.location.author.alt()),
                round = point_id.location.round.0,
                digest = display(point_id.digest.alt()),
            )
        })
    }
}
