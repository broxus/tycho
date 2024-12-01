use std::collections::{BTreeMap, VecDeque};
use std::iter;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use futures_util::future::BoxFuture;
use futures_util::stream::FuturesUnordered;
use futures_util::{future, StreamExt};
use parking_lot::Mutex;
use rand::{thread_rng, RngCore};
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::{broadcast, mpsc, oneshot, Semaphore};
use tokio::time::{Interval, MissedTickBehavior};
use tracing::Instrument;
use tycho_network::{PeerId, Request};
use tycho_util::metrics::HistogramGuard;
use tycho_util::FastHashMap;

use crate::dag::{Verifier, VerifyError};
use crate::effects::{AltFormat, Ctx, DownloadCtx};
use crate::engine::round_watch::{Consensus, RoundWatcher};
use crate::engine::{CachedConfig, ConsensusConfigExt};
use crate::intercom::dto::{PeerState, PointByIdResponse};
use crate::intercom::{Dispatcher, PeerSchedule};
use crate::models::{PeerCount, Point, PointId, Round};

#[derive(Clone)]
pub struct Downloader {
    inner: Arc<DownloaderInner>,
}

pub enum DownloadResult {
    Verified(Point),
    IllFormed(Point),
}

struct DownloaderInner {
    dispatcher: Dispatcher,
    peer_schedule: PeerSchedule,
    limiter: Mutex<Limiter>,
    consensus_round: RoundWatcher<Consensus>,
}

trait DownloadType: Send + 'static {
    fn next_peers(attempt: u8, available_peers: usize) -> usize;
}

/// Exponential increase of peers to query
struct ExponentialQuery;
impl DownloadType for ExponentialQuery {
    fn next_peers(attempt: u8, available_peers: usize) -> usize {
        // result increases exponentially
        let download_peers = CachedConfig::get().consensus.download_peers as usize;
        download_peers
            .saturating_mul(download_peers.saturating_pow(attempt as u32))
            .min(available_peers)
    }
}

/// Linear increase of peers to query
struct LinearQuery;
impl DownloadType for LinearQuery {
    fn next_peers(attempt: u8, available_peers: usize) -> usize {
        let download_peers = CachedConfig::get().consensus.download_peers as usize;
        download_peers
            .saturating_add(download_peers.saturating_mul(attempt as usize))
            .min(available_peers)
    }
}

#[derive(Default)]
struct Limiter {
    running: u16,
    waiters: BTreeMap<Round, VecDeque<Arc<Semaphore>>>,
}

impl Limiter {
    fn enter(&mut self, round: Round) -> Option<Arc<Semaphore>> {
        // cannot be strict equality: at least one is always allowed, others are concurrent to it
        if self.running <= CachedConfig::get().consensus.download_tasks {
            self.running += 1;
            None
        } else {
            // create locked
            let semaphore = Arc::new(Semaphore::new(0));
            self.waiters
                .entry(round)
                .or_default()
                .push_back(semaphore.clone());
            Some(semaphore)
        }
    }

    fn exit(&mut self) {
        // free the topmost waiter by round
        if let Some(mut entry) = self.waiters.last_entry() {
            // fifo among those with the same round
            match entry.get_mut().pop_front() {
                Some(semaphore) => {
                    assert_eq!(
                        semaphore.available_permits(),
                        0,
                        "dequeued semaphore must not have permits"
                    );
                    semaphore.add_permits(1); // unlock waiter
                }
                None => panic!("downloader limiter: round queue was left empty"),
            }
            if entry.get().is_empty() {
                entry.remove_entry();
            }
        } else {
            self.running = self
                .running
                .checked_sub(1)
                .expect("decrease running downloads counter");
        }
    }
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
    pub fn new(
        dispatcher: &Dispatcher,
        peer_schedule: &PeerSchedule,
        consensus_round: RoundWatcher<Consensus>,
    ) -> Self {
        Self {
            inner: Arc::new(DownloaderInner {
                dispatcher: dispatcher.clone(),
                peer_schedule: peer_schedule.clone(),
                limiter: Default::default(),
                consensus_round,
            }),
        }
    }

    /// 2F "point not found" responses lead to invalidation of all referencing points;
    /// failed network queries are retried after all peers were queried the same amount of times,
    /// and only successful responses that point is not found are taken into account.
    ///
    /// Reliable peers respond immediately with successfully validated points, or return `None`.
    pub async fn run(
        &self,
        point_id: &PointId,
        dependers_rx: mpsc::UnboundedReceiver<PeerId>,
        verified_broadcast: oneshot::Receiver<DownloadResult>,
        ctx: DownloadCtx,
    ) -> Option<DownloadResult> {
        let semaphore_opt = {
            let mut limiter = self.inner.limiter.lock();
            limiter.enter(point_id.round)
        };
        if let Some(semaphore) = semaphore_opt {
            match semaphore.acquire().await {
                Ok(_permit) => {}
                Err(err) => panic!("downloader limiter: {err}"),
            }
        }

        let result = if point_id.round + CachedConfig::get().consensus.min_front_rounds()
            >= self.inner.consensus_round.get()
        {
            // for validation
            self.run_task::<ExponentialQuery>(point_id, dependers_rx, verified_broadcast, ctx)
                .await
        } else {
            // for sync
            self.run_task::<LinearQuery>(point_id, dependers_rx, verified_broadcast, ctx)
                .await
        };

        {
            let mut limiter = self.inner.limiter.lock();
            limiter.exit();
        }
        result
    }

    async fn run_task<T: DownloadType>(
        &self,
        point_id: &PointId,
        dependers_rx: mpsc::UnboundedReceiver<PeerId>,
        broadcast_result: oneshot::Receiver<DownloadResult>,
        ctx: DownloadCtx,
    ) -> Option<DownloadResult> {
        let _task_duration = HistogramGuard::begin("tycho_mempool_download_task_time");
        ctx.meter_start(point_id);
        let span_guard = ctx.span().enter();
        // request point from its signers (any depender is among them as point is already verified)
        let (undone_peers, author_state, updates) = {
            let guard = self.inner.peer_schedule.read();
            let undone_peers = guard.data.peers_state_for(point_id.round.next());
            let author_state = guard.data.peer_state(&point_id.author);
            (undone_peers.clone(), author_state, guard.updates())
        };
        let peer_count = PeerCount::try_from(undone_peers.len())
            .expect("validator set is unknown, must keep prev epoch's set for sync");
        let undone_peers = undone_peers
            .iter()
            // query author no matter if it is scheduled for the next round or not;
            // it won't affect 2F reliable `None` responses to break the task with `DagPoint::NotFound`:
            // author is a depender for its point, so its `NotFound` response is not reliable
            .chain(iter::once((&point_id.author, &author_state)))
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

        drop(span_guard);

        let mut task = DownloadTask {
            parent: self.clone(),
            _phantom: PhantomData,
            request: Dispatcher::point_by_id_request(*point_id),
            point_id: *point_id,
            peer_count,
            reliably_not_found: 0, // this node is +1 to 2F
            unreliable_peers: 0,   // should not reach 1F+1
            updates,
            undone_peers,
            downloading: FuturesUnordered::new(),
            attempt: 0,
            interval: tokio::time::interval(Duration::from_millis(
                CachedConfig::get().consensus.download_retry_millis as _,
            )),
        };
        let downloaded = task
            .run(dependers_rx, broadcast_result)
            .instrument(ctx.span().clone())
            .await;

        DownloadCtx::meter_task::<T>(&task);

        downloaded
    }
}

struct DownloadTask<T> {
    parent: Downloader,
    _phantom: PhantomData<T>,

    request: Request,
    point_id: PointId,

    peer_count: PeerCount,
    reliably_not_found: u8, // count only responses considered reliable
    unreliable_peers: u8,   // count only responses considered unreliable

    updates: broadcast::Receiver<(PeerId, PeerState)>,

    undone_peers: FastHashMap<PeerId, PeerStatus>,
    downloading:
        FuturesUnordered<BoxFuture<'static, (PeerId, anyhow::Result<PointByIdResponse<Point>>)>>,

    attempt: u8,
    /// skip time-driven attempt if an attempt was init by empty task queue
    interval: Interval,
}

impl<T: DownloadType> DownloadTask<T> {
    // point's author is a top priority; fallback priority is (any) dependent point's author
    // recursively: every dependency is expected to be signed by 2/3+1
    pub async fn run(
        &mut self,
        mut dependers_rx: mpsc::UnboundedReceiver<PeerId>,
        mut broadcast_result: oneshot::Receiver<DownloadResult>,
    ) -> Option<DownloadResult> {
        // give equal time to every attempt, ignoring local runtime delays; do not `Burst` requests
        self.interval
            .set_missed_tick_behavior(MissedTickBehavior::Delay);

        while let Ok(peer_id) = dependers_rx.try_recv() {
            self.add_depender(&peer_id);
        }

        loop {
            tokio::select! {
                biased; // mandatory priority: signals lifecycle, updates, data lifecycle
                Ok(bcast_result) = &mut broadcast_result => break Some(bcast_result),
                Some(depender) = dependers_rx.recv() => self.add_depender(&depender),
                update = self.updates.recv() => if self.match_peer_updates(update).is_err() {
                    future::pending::<()>().await;
                },
                Some((peer_id, result)) = self.downloading.next() =>
                    match self.verify(&peer_id, result) {
                        Some(found) => break Some(found),
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
        match self.undone_peers.get_mut(peer_id) {
            Some(status) if !status.is_depender => {
                status.is_depender = true;
            }
            _ => {} // either already marked or requested and removed, no panic
        };
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

        let count = T::next_peers(self.attempt, filtered.len());

        for (peer_id, _) in &filtered[..count] {
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
                .query_point(peer_id, &self.request),
        );
    }

    fn verify(
        &mut self,
        peer_id: &PeerId,
        result: anyhow::Result<PointByIdResponse<Point>>,
    ) -> Option<DownloadResult> {
        let defined_response =
            match result {
                Ok(PointByIdResponse::Defined(point)) => Some(point),
                Ok(PointByIdResponse::DefinedNone) => None,
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
                    metrics::counter!("tycho_mempool_download_query_failed_count").increment(1);
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
                    // is at least DAG_ROUNDS older than current consensus round
                    self.unreliable_peers = self.unreliable_peers.saturating_add(1);
                    DownloadCtx::meter_unreliable();
                    tracing::warn!(peer = display(peer_id.alt()), "must have returned");
                } else {
                    self.reliably_not_found = self.reliably_not_found.saturating_add(1);
                    metrics::counter!("tycho_mempool_download_not_found_responses").increment(1);
                    tracing::trace!(peer = display(peer_id.alt()), "didn't return");
                }
                None
            }
            Some(point) if point.id() != self.point_id => {
                // it's a ban
                self.unreliable_peers = self.unreliable_peers.saturating_add(1);
                DownloadCtx::meter_unreliable();
                tracing::error!(
                    peer_id = display(peer_id.alt()),
                    author = display(point.data().author.alt()),
                    round = point.round().0,
                    digest = display(point.digest().alt()),
                    "returned wrong point",
                );
                None
            }
            Some(point) => {
                match Verifier::verify(&point, &self.parent.inner.peer_schedule) {
                    Ok(()) => Some(DownloadResult::Verified(point)), // `Some` breaks outer loop
                    Err(error @ VerifyError::BadSig) => {
                        // reliable peer won't return unverifiable point
                        self.unreliable_peers = self.unreliable_peers.saturating_add(1);
                        DownloadCtx::meter_unreliable();
                        tracing::error!(
                            result = display(error),
                            peer = display(peer_id.alt()),
                            "downloaded",
                        );
                        None
                    }
                    Err(
                        error @ (VerifyError::IllFormed
                        | VerifyError::LackOfPeers(_)
                        | VerifyError::UnknownPeers(_)),
                    ) => {
                        tracing::error!(
                            error = display(error),
                            point = debug(&point),
                            "downloaded illformed"
                        );
                        Some(DownloadResult::IllFormed(point))
                    }
                    Err(
                        error @ (VerifyError::BeforeGenesis
                        | VerifyError::UnknownRound
                        | VerifyError::UnknownAuthor
                        | VerifyError::Uninit(_)),
                    ) => {
                        panic!(
                            "should not receive {error} for downloaded {:?}",
                            point.id().alt()
                        )
                    }
                }
            }
        }
    }

    fn shall_continue(&mut self) -> bool {
        if self.reliably_not_found as usize >= self.peer_count.majority_of_others() {
            // the only normal case to resolve into `NotFound`
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

    fn match_peer_updates(
        &mut self,
        result: Result<(PeerId, PeerState), RecvError>,
    ) -> Result<(), ()> {
        match result {
            Ok((peer_id, new)) => {
                self.undone_peers.entry(peer_id).and_modify(|status| {
                    status.state = new;
                });
                Ok(())
            }
            Err(err @ RecvError::Lagged(_)) => {
                tracing::error!(error = display(err), "peer updates");
                Ok(())
            }
            Err(err @ RecvError::Closed) => {
                tracing::error!(error = display(err), "peer updates");
                Err(())
            }
        }
    }
}
impl DownloadCtx {
    fn meter_unreliable() {
        metrics::counter!("tycho_mempool_download_unreliable_responses").increment(1);
    }

    fn meter_task<T>(task: &DownloadTask<T>) {
        metrics::counter!("tycho_mempool_download_aborted_on_exit_count")
            .increment(task.downloading.len() as _);
    }

    fn meter_start(&self, point_id: &PointId) {
        metrics::counter!("tycho_mempool_download_task_count").increment(1);

        metrics::gauge!("tycho_mempool_download_depth_rounds")
            .set(self.download_max_depth(point_id.round));
    }
}
