use std::iter;
use std::sync::Arc;

use futures_util::future::BoxFuture;
use futures_util::stream::FuturesUnordered;
use futures_util::{FutureExt, StreamExt};
use rand::{thread_rng, RngCore};
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::time::{Interval, MissedTickBehavior};
use tracing::Instrument;
use tycho_network::PeerId;
use tycho_util::metrics::HistogramGuard;
use tycho_util::FastHashMap;

use crate::dag::{DagRound, Verifier};
use crate::effects::{AltFormat, DownloadContext, Effects, ValidateContext};
use crate::engine::MempoolConfig;
use crate::intercom::dto::{PeerState, PointByIdResponse};
use crate::intercom::{Dispatcher, PeerSchedule, QueryKind};
use crate::models::{DagPoint, PeerCount, PointId};
use crate::{dyn_event, Point};

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
    failed_queries: usize,
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
        // Download task holds weak reference to containing round and does not prevent its drop,
        // while passes weak ref to validate; so Verifier is able to break recursive validation
        // (trust consensus on `DAG_DEPTH` at least) and does not require too deep points
        // to be checked against their dependencies (if dag round is removed from DAG).
        // The task will be dropped in case DAG round is dropped and no validation waits this point.
        // Do not pass `WeakDagRound` here as it would be incorrect to return `DagPoint::NotExists`
        // if we need to download at a very deep round - let the start of this task hold strong ref.
        point_dag_round_strong: DagRound,
        dependers: mpsc::UnboundedReceiver<PeerId>,
        verified_broadcast: oneshot::Receiver<Point>,
        effects: Effects<DownloadContext>,
    ) -> DagPoint {
        let _task_duration = HistogramGuard::begin(DownloadContext::TASK_DURATION);
        effects.meter_start(&point_id);
        let span_guard = effects.span().enter();
        assert_eq!(
            point_id.location.round,
            point_dag_round_strong.round(),
            "point and DAG round mismatch"
        );
        // request point from its signers (any depender is among them as point is already verified)
        let (undone_peers, author_state, updates) = {
            let guard = self.inner.peer_schedule.read();
            let undone_peers = guard.data.peers_state_for(point_id.location.round.next());
            let author_state = guard.data.peer_state(&point_id.location.author);
            (undone_peers.clone(), author_state, guard.updates())
        };
        let peer_count = PeerCount::try_from(undone_peers.len())
            .expect("validator set is unknown, must keep prev epoch's set for DAG_DEPTH rounds");
        let undone_peers = undone_peers
            .iter()
            // query author no matter if it is scheduled for the next round or not;
            // it won't affect 2F reliable `NotFound`s to break the task with `DagPoint::NotExists`:
            // author is a depender for its point, so its `NotFound` response is not reliable
            .chain(iter::once((&point_id.location.author, &author_state)))
            .map(|(peer_id, state)| {
                let status = PeerStatus {
                    state: *state,
                    failed_queries: 0,
                    is_depender: false, // `true` comes from channel to start immediate download
                    is_in_flight: false,
                };
                (*peer_id, status)
            })
            .collect::<FastHashMap<_, _>>();

        let point_dag_round = point_dag_round_strong.downgrade();
        // do not leak span and strong round ref across await
        drop(point_dag_round_strong);
        drop(span_guard);

        let mut task = DownloadTask {
            parent: self.clone(),
            request: Dispatcher::point_by_id_request(&point_id),
            point_id: point_id.clone(),
            peer_count,
            reliably_not_found: 0, // this node is +1 to 2F
            unreliable_peers: 0,   // should not reach 1F+1
            dependers,
            updates,
            undone_peers,
            downloading: FuturesUnordered::new(),
            attempt: 0,
            interval: tokio::time::interval(MempoolConfig::DOWNLOAD_INTERVAL),
        };
        let downloaded = task
            .run(verified_broadcast)
            .instrument(effects.span().clone())
            .await;

        DownloadContext::meter_task(&task);

        match downloaded {
            None => DagPoint::NotExists(Arc::new(point_id)),
            Some(point) => {
                tracing::trace!(
                    parent: effects.span(),
                    peer = display(point.body().location.author.alt()),
                    "downloaded, now validating",
                );
                let dag_point = Verifier::validate(
                    point.clone(),
                    point_dag_round,
                    self.clone(),
                    Effects::<ValidateContext>::new(&effects, &point),
                )
                // this is the only `await` in the task, that resolves the download
                .await;
                let level = if dag_point.trusted().is_some() {
                    tracing::Level::DEBUG
                } else if dag_point.valid().is_some() {
                    tracing::Level::WARN
                } else {
                    tracing::Level::ERROR
                };
                dyn_event!(
                    parent: effects.span(),
                    level,
                    result = display(dag_point.alt()),
                    "validated",
                );
                dag_point
            }
        }
    }
}

struct DownloadTask {
    parent: Downloader,

    request: QueryKind,
    point_id: PointId,

    peer_count: PeerCount,
    reliably_not_found: u8, // count only responses considered reliable
    unreliable_peers: u8,   // count only responses considered unreliable

    /// populated by waiting validation tasks
    dependers: mpsc::UnboundedReceiver<PeerId>,
    updates: broadcast::Receiver<(PeerId, PeerState)>,

    undone_peers: FastHashMap<PeerId, PeerStatus>,
    downloading: FuturesUnordered<BoxFuture<'static, (PeerId, anyhow::Result<PointByIdResponse>)>>,

    attempt: u8,
    /// skip time-driven attempt if an attempt was init by empty task queue
    interval: Interval,
}

impl DownloadTask {
    // point's author is a top priority; fallback priority is (any) dependent point's author
    // recursively: every dependency is expected to be signed by 2/3+1
    pub async fn run(&mut self, mut verified_broadcast: oneshot::Receiver<Point>) -> Option<Point> {
        // give equal time to every attempt, ignoring local runtime delays; do not `Burst` requests
        self.interval
            .set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                biased; // mandatory priority: signals lifecycle, updates, data lifecycle
                Ok(point) = &mut verified_broadcast => break Some(point),
                Some(depender) = self.dependers.recv() => self.add_depender(&depender),
                update = self.updates.recv() => self.match_peer_updates(update),
                Some((peer_id, result)) = self.downloading.next() =>
                    match self.verify(&peer_id, result) {
                        Some(point) => break Some(point),
                        None => if self.shall_continue() {
                            continue
                        } else {
                            break None;
                        }
                    },
                // most rare arm to make progress despite slow responding peers
                _ = self.interval.tick() => self.download_random(), // first tick fires immediately
            }
        }
        // on exit futures are dropped and receivers are cleaned,
        // senders will stay in `DagPointFuture` that owns current task
    }

    fn add_depender(&mut self, peer_id: &PeerId) {
        let is_suitable = match self.undone_peers.get_mut(peer_id) {
            Some(status) if !status.is_depender => {
                status.is_depender = true;
                !status.is_in_flight
                    && status.state == PeerState::Resolved
                    // do not re-download immediately if already requested
                    && status.failed_queries == 0
            }
            _ => false, // either already marked or requested and removed, no panic
        };
        if is_suitable {
            // request immediately just once
            self.download_one(peer_id);
        }
    }

    fn download_random(&mut self) {
        let mut filtered = self
            .undone_peers
            .iter()
            .filter(|(_, p)| p.state == PeerState::Resolved && !p.is_in_flight)
            .map(|(peer_id, status)| {
                (
                    *peer_id,
                    (
                        // try every peer, until all are tried the same amount of times
                        status.failed_queries,
                        // try mandatory peers before others each loop
                        u8::from(!status.is_depender),
                        // randomise within group
                        thread_rng().next_u32(),
                    ),
                )
            })
            .collect::<Vec<_>>();
        filtered.sort_unstable_by(|(_, ord_l), (_, ord_r)| ord_l.cmp(ord_r));

        let count = (MempoolConfig::DOWNLOAD_PEERS as usize)
            .saturating_mul(
                (MempoolConfig::DOWNLOAD_PEERS as usize).saturating_pow(self.attempt as u32),
            )
            .min(filtered.len());

        for (peer_id, _) in filtered.iter().take(count) {
            self.download_one(peer_id);
        }

        self.attempt = self.attempt.wrapping_add(1);
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

    fn verify(
        &mut self,
        peer_id: &PeerId,
        result: anyhow::Result<PointByIdResponse>,
    ) -> Option<Point> {
        let defined_response =
            match result {
                Ok(PointByIdResponse::Defined(response)) => response,
                Ok(PointByIdResponse::TryLater) => {
                    let status = self.undone_peers.get_mut(peer_id).unwrap_or_else(|| {
                        panic!("Coding error: peer not in map {}", peer_id.alt())
                    });
                    status.is_in_flight = false;
                    // apply the same retry strategy as for network errors
                    status.failed_queries = status.failed_queries.saturating_add(1);
                    tracing::trace!(peer = display(peer_id.alt()), "try later");
                    return None;
                }
                Err(network_err) => {
                    let status = self.undone_peers.get_mut(peer_id).unwrap_or_else(|| {
                        panic!("Coding error: peer not in map {}", peer_id.alt())
                    });
                    status.is_in_flight = false;
                    status.failed_queries = status.failed_queries.saturating_add(1);
                    metrics::counter!(DownloadContext::FAILED_QUERY).increment(1);
                    tracing::warn!(
                        peer = display(peer_id.alt()),
                        error = display(network_err),
                        "network error",
                    );
                    return None;
                }
            };

        let Some(status) = self.undone_peers.remove(peer_id) else {
            panic!("peer {} was removed, concurrent download?", peer_id.alt());
        };
        assert!(
            status.is_in_flight,
            "peer {} is not in-flight",
            peer_id.alt(),
        );

        match defined_response {
            None => {
                if status.is_depender {
                    // if points are persisted in storage - it's a ban;
                    // else - peer evicted this point from its cache, as the point
                    // is at least DAG_DEPTH rounds older than current consensus round
                    self.unreliable_peers = self.unreliable_peers.saturating_add(1);
                    // FIXME remove next line when storage is ready
                    self.reliably_not_found = self.reliably_not_found.saturating_add(1);
                    tracing::warn!(peer = display(peer_id.alt()), "must have returned");
                } else {
                    self.reliably_not_found = self.reliably_not_found.saturating_add(1);
                    tracing::trace!(peer = display(peer_id.alt()), "didn't return");
                }
                None
            }
            Some(point) if point.id() != self.point_id => {
                // it's a ban
                self.unreliable_peers = self.unreliable_peers.saturating_add(1);
                tracing::error!(
                    peer_id = display(peer_id.alt()),
                    author = display(point.body().location.author.alt()),
                    round = point.body().location.round.0,
                    digest = display(point.digest().alt()),
                    "returned wrong point",
                );
                None
            }
            Some(point) => {
                match Verifier::verify(&point, &self.parent.inner.peer_schedule) {
                    Err(dag_point) => {
                        // reliable peer won't return unverifiable point
                        self.unreliable_peers = self.unreliable_peers.saturating_add(1);
                        assert!(
                            dag_point.valid().is_none(),
                            "Coding error: verify() cannot result into a valid point"
                        );
                        tracing::error!(
                            result = display(dag_point.alt()),
                            peer = display(peer_id.alt()),
                            "downloaded",
                        );
                        None
                    }
                    Ok(()) => Some(point), // breaks loop
                }
            }
        }
    }

    fn shall_continue(&mut self) -> bool {
        // if self.unreliable_peers as usize >= self.peer_count.reliable_minority() {
        //     panic!("too many unreliable peers: {}", self.unreliable_peers)
        // } else
        if self.reliably_not_found as usize >= self.peer_count.majority_of_others() {
            // the only normal case to resolve into `NotExists`
            tracing::warn!(
                unreliable = self.unreliable_peers,
                "not downloaded from majority",
            );
            false
        } else {
            if self.downloading.is_empty() {
                self.interval.reset(); // start new interval at current moment
                self.download_random();
            }
            true
        }
    }

    fn match_peer_updates(&mut self, result: Result<(PeerId, PeerState), RecvError>) {
        match result {
            Ok((peer_id, new)) => {
                let mut is_suitable = false;
                self.undone_peers.entry(peer_id).and_modify(|status| {
                    is_suitable = !status.is_in_flight
                        && status.is_depender
                        && status.failed_queries == 0
                        && status.state == PeerState::Unknown
                        && new == PeerState::Resolved;
                    status.state = new;
                });
                if is_suitable {
                    self.download_one(&peer_id);
                }
            }
            Err(err @ RecvError::Lagged(_)) => {
                tracing::error!(error = display(err), "peer updates");
            }
            Err(err @ RecvError::Closed) => {
                panic!("peer updates {err}")
            }
        }
    }
}
impl DownloadContext {
    const TASK_DURATION: &'static str = "tycho_mempool_download_task_time";
    const FAILED_QUERY: &'static str = "tycho_mempool_download_query_failed_count";

    fn meter_task(task: &DownloadTask) {
        metrics::counter!("tycho_mempool_download_not_found_responses")
            .increment(task.reliably_not_found as _);
        metrics::counter!("tycho_mempool_download_aborted_on_exit_count")
            .increment(task.downloading.len() as _);
        // metrics::histogram!("tycho_mempool_download_unreliable_responses")
        //     .set(task.unreliable_peers);
    }
}
impl Effects<DownloadContext> {
    fn meter_start(&self, point_id: &PointId) {
        metrics::counter!("tycho_mempool_download_task_count").increment(1);

        metrics::counter!(DownloadContext::FAILED_QUERY).increment(0); // refresh

        // FIXME not guaranteed to show the latest value as rounds advance, but better than nothing
        metrics::gauge!("tycho_mempool_download_depth_rounds")
            .set(self.download_max_depth(point_id.location.round));
    }
}
