use std::iter;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use futures_util::future::BoxFuture;
use futures_util::stream::FuturesUnordered;
use rand::RngCore;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::time::MissedTickBehavior;
use tracing::Instrument;
use tycho_network::{PeerId, Request};
use tycho_util::FastHashMap;
use tycho_util::metrics::HistogramGuard;

use crate::dag::{IllFormedReason, Verifier, VerifyError};
use crate::effects::{AltFormat, Cancelled, Ctx, DownloadCtx, TaskResult};
use crate::engine::round_watch::{Consensus, RoundWatcher};
use crate::engine::{ConsensusConfigExt, MempoolConfig, NodeConfig};
use crate::intercom::core::{PointByIdResponse, PointQueryResult, QueryRequest};
use crate::intercom::dependency::limiter::Limiter;
use crate::intercom::dependency::peer_limiter::PeerLimiter;
use crate::intercom::peer_schedule::PeerState;
use crate::intercom::{Dispatcher, PeerSchedule};
use crate::models::{PeerCount, Point, PointId};

#[derive(Clone)]
pub struct Downloader {
    inner: Arc<DownloaderInner>,
}

pub enum DownloadResult {
    Verified(Point),
    IllFormed(Point, IllFormedReason),
}

struct DownloaderInner {
    dispatcher: Dispatcher,
    peer_schedule: PeerSchedule,
    limiter: Limiter,
    peer_limiter: PeerLimiter,
    consensus_round: RoundWatcher<Consensus>,
}

trait DownloadType: Send + 'static {
    fn next_peers(
        attempt: u8,
        already_downloading: usize,
        peer_count: &PeerCount,
        conf: &MempoolConfig,
    ) -> usize {
        Self::max_downloads_per_attempt(attempt as usize, conf)
            .min(peer_count.reliable_minority())
            .saturating_sub(already_downloading)
    }

    fn max_downloads_per_attempt(attempt: usize, conf: &MempoolConfig) -> usize;
}

/// Exponential increase of peers to query
struct ExponentialQuery;
impl DownloadType for ExponentialQuery {
    fn max_downloads_per_attempt(attempt: usize, conf: &MempoolConfig) -> usize {
        let download_peers = conf.consensus.download_peers.get() as usize;
        download_peers.saturating_mul(download_peers.saturating_pow(attempt as u32))
    }
}

/// Linear increase of peers to query
struct LinearQuery;
impl DownloadType for LinearQuery {
    fn max_downloads_per_attempt(attempt: usize, conf: &MempoolConfig) -> usize {
        let download_peers = conf.consensus.download_peers.get() as usize;
        download_peers.saturating_add(download_peers.saturating_mul(attempt))
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
        conf: &MempoolConfig,
    ) -> Self {
        Self {
            inner: Arc::new(DownloaderInner {
                dispatcher: dispatcher.clone(),
                peer_schedule: peer_schedule.clone(),
                limiter: Limiter::new(NodeConfig::get().max_download_tasks),
                peer_limiter: PeerLimiter::new(conf.consensus.download_peer_queries.into()),
                consensus_round,
            }),
        }
    }

    pub fn peer_schedule(&self) -> &PeerSchedule {
        &self.inner.peer_schedule
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
    ) -> TaskResult<Option<DownloadResult>> {
        let _guard = self.inner.limiter.enter(point_id.round).await;

        if point_id.round + ctx.conf().consensus.min_front_rounds()
            >= self.inner.consensus_round.get()
        {
            // for validation
            self.run_task::<ExponentialQuery>(point_id, dependers_rx, verified_broadcast, ctx)
                .await
        } else {
            // for sync
            self.run_task::<LinearQuery>(point_id, dependers_rx, verified_broadcast, ctx)
                .await
        }
    }

    async fn run_task<T: DownloadType>(
        &self,
        point_id: &PointId,
        dependers_rx: mpsc::UnboundedReceiver<PeerId>,
        broadcast_result: oneshot::Receiver<DownloadResult>,
        ctx: DownloadCtx,
    ) -> TaskResult<Option<DownloadResult>> {
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
            ctx,
            request: QueryRequest::point_by_id(point_id),
            point_id: *point_id,
            peer_count,
            not_found: 0, // this node is +1 to 2F
            updates,
            undone_peers,
            downloading: FuturesUnordered::new(),
            attempt: 0,
        };
        let span = task.ctx.span().clone();
        let downloaded = task
            .run(dependers_rx, broadcast_result)
            .instrument(span)
            .await?;

        DownloadCtx::meter_task::<T>(&task);

        if downloaded.is_none() {
            tracing::warn!(
                parent: task.ctx.span(),
                "not downloaded",
            );
        }

        Ok(downloaded)
    }
}

struct DownloadTask<T> {
    parent: Downloader,
    _phantom: PhantomData<T>,
    ctx: DownloadCtx,

    request: Request,
    point_id: PointId,

    peer_count: PeerCount,
    not_found: u8, // count only responses considered reliable

    updates: broadcast::Receiver<(PeerId, PeerState)>,

    undone_peers: FastHashMap<PeerId, PeerStatus>,
    downloading: FuturesUnordered<BoxFuture<'static, (PeerId, PointQueryResult)>>,

    attempt: u8,
}

impl<T: DownloadType> DownloadTask<T> {
    // point's author is a top priority; fallback priority is (any) dependent point's author
    // recursively: every dependency is expected to be signed by 2/3+1
    pub async fn run(
        &mut self,
        mut dependers_rx: mpsc::UnboundedReceiver<PeerId>,
        mut broadcast_result: oneshot::Receiver<DownloadResult>,
    ) -> TaskResult<Option<DownloadResult>> {
        let mut interval = tokio::time::interval(Duration::from_millis(
            self.ctx.conf().consensus.download_retry_millis.get() as _,
        ));
        // no `interval.reset()`: download starts right after biased receive of all known dependers
        // give equal time to every attempt, ignoring local runtime delays; do not `Burst` requests
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let maybe_found = loop {
            tokio::select! {
                biased; // mandatory priority: signals lifecycle, updates, data lifecycle
                bcast_result = &mut broadcast_result => break bcast_result.ok(),
                Some(depender) = dependers_rx.recv() => self.add_depender(&depender),
                update = self.updates.recv() => self.match_peer_updates(update)?,
                Some((peer_id, result)) = self.downloading.next() =>
                    match self.verify(&peer_id, result) {
                        Some(found) => break Some(found),
                        None => if self.not_found as usize >= self.peer_count.majority_of_others() {
                            break None;
                        } else if self.downloading.is_empty() {
                            interval.reset_immediately(); // restart interval and tick immediately
                        }
                    },
                // most rare arm to make progress despite slow responding peers
                _ = interval.tick() => self.download_random(), // first tick fires immediately
            }
        };
        // on exit futures are dropped and receivers are cleaned,
        // senders will stay in `DagPointFuture` that owns current task
        Ok(maybe_found)
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
                        rand::rng().next_u32(),
                    ),
                )
            })
            .collect::<Vec<_>>();
        filtered.sort_unstable_by(|(_, ord_l), (_, ord_r)| ord_l.cmp(ord_r));

        let to_add = T::next_peers(
            self.attempt,
            self.downloading.len(),
            &self.peer_count,
            self.ctx.conf(),
        )
        .min(filtered.len());

        for (peer_id, _) in filtered.into_iter().take(to_add) {
            self.download_one(peer_id);
        }

        self.attempt = self.attempt.wrapping_add(1);
    }

    fn download_one(&mut self, peer_id: PeerId) {
        let status = self
            .undone_peers
            .get_mut(&peer_id)
            .unwrap_or_else(|| panic!("Coding error: peer not in map {}", peer_id.alt()));
        assert!(
            !status.is_in_flight,
            "already downloading from peer {} status {:?}",
            peer_id.alt(),
            status
        );
        status.is_in_flight = true;

        let parent = self.parent.clone();
        let request = self.request.clone();
        self.downloading.push(Box::pin(async move {
            let permit = parent.inner.peer_limiter.get(peer_id).await;
            parent.inner.dispatcher.query_point(permit, request).await
        }));
    }

    fn verify(&mut self, peer_id: &PeerId, result: PointQueryResult) -> Option<DownloadResult> {
        let defined_response =
            match result {
                Ok(PointByIdResponse::Defined(point_result)) => Some(point_result),
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
                self.not_found = self.not_found.saturating_add(1);
                DownloadCtx::meter_not_found();
                tracing::debug!(
                    peer = display(peer_id.alt()),
                    is_depender = Some(status.is_depender).filter(|x| *x),
                    "didn't return"
                );
                None
            }
            Some(Err(parse_error)) => {
                // reliable peer won't return unverifiable point
                self.not_found = self.not_found.saturating_add(1);
                DownloadCtx::meter_unreliable();
                tracing::error!(
                    result = display(parse_error),
                    peer = display(peer_id.alt()),
                    "downloaded",
                );
                None
            }
            Some(Ok(point)) if point.info().id() != self.point_id => {
                // it's a ban
                self.not_found = self.not_found.saturating_add(1);
                DownloadCtx::meter_unreliable();
                tracing::error!(
                    peer_id = display(peer_id.alt()),
                    author = display(point.info().author().alt()),
                    round = point.info().round().0,
                    digest = display(point.info().digest().alt()),
                    "returned wrong point",
                );
                None
            }
            Some(Ok(point)) => {
                match Verifier::verify(
                    point.info(),
                    &self.parent.inner.peer_schedule,
                    self.ctx.conf(),
                ) {
                    Ok(()) => Some(DownloadResult::Verified(point)), // `Some` breaks outer loop
                    Err(VerifyError::IllFormed(reason)) => {
                        tracing::error!(
                            error = display(&reason),
                            point = debug(&point),
                            "downloaded ill-formed"
                        );
                        Some(DownloadResult::IllFormed(point, reason))
                    }
                    Err(VerifyError::Fail(error)) => {
                        panic!(
                            "should not receive {error} for downloaded {:?}",
                            point.info().id().alt()
                        )
                    }
                }
            }
        }
    }

    fn match_peer_updates(
        &mut self,
        result: Result<(PeerId, PeerState), RecvError>,
    ) -> TaskResult<()> {
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
                Err(Cancelled())
            }
        }
    }
}

impl DownloadCtx {
    fn meter_unreliable() {
        metrics::counter!("tycho_mempool_download_unreliable_responses").increment(1);
        Self::meter_not_found();
    }

    fn meter_not_found() {
        metrics::counter!("tycho_mempool_download_not_found_responses").increment(1);
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

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use super::*;
    use crate::models::PeerCount;
    use crate::test_utils::default_test_config;

    fn test_impl<T: DownloadType>(
        expected_to_add: &[usize],
        already_downloading: usize,
        peer_count: PeerCount,
    ) -> Result<()> {
        let mut attempt = 0;

        let merged_conf = default_test_config();

        for (step, _) in expected_to_add.iter().enumerate() {
            let actual_peers =
                T::next_peers(attempt, already_downloading, &peer_count, &merged_conf.conf);

            anyhow::ensure!(
                expected_to_add[step] == actual_peers,
                "step {}, attempt {}, already_downloading {}, expected {}, got {}",
                step,
                attempt,
                already_downloading,
                expected_to_add[step],
                actual_peers,
            );

            anyhow::ensure!(actual_peers + already_downloading <= peer_count.reliable_minority());

            attempt = attempt.wrapping_add(1);
        }

        Ok(())
    }

    #[test]
    fn linear_basic() -> Result<()> {
        let expected_to_add = [2, 4, 6, 8, 10, 11, 11];
        let already_downloading = 0;
        let peer_count = PeerCount::try_from(30)?;

        test_impl::<LinearQuery>(&expected_to_add, already_downloading, peer_count)
    }

    #[test]
    fn linear_already_downloading() -> Result<()> {
        let expected_to_add = [0, 0, 2, 4, 6, 7, 7, 7];
        let already_downloading = 4;
        let peer_count = PeerCount::try_from(30)?;

        test_impl::<LinearQuery>(&expected_to_add, already_downloading, peer_count)
    }

    #[test]
    fn exponential_basic() -> Result<()> {
        let expected_to_add = [2, 4, 8, 11, 11];
        let already_downloading = 0;
        let peer_count = PeerCount::try_from(30)?;

        test_impl::<ExponentialQuery>(&expected_to_add, already_downloading, peer_count)
    }

    #[test]
    fn exponential_already_downloading() -> Result<()> {
        let expected_to_add = [0, 0, 4, 7, 7];
        let already_downloading = 4;
        let peer_count = PeerCount::try_from(30)?;

        test_impl::<ExponentialQuery>(&expected_to_add, already_downloading, peer_count)
    }
}
