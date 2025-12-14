use std::iter;
use std::sync::Arc;
use std::time::Duration;

use ahash::HashSetExt;
use bytes::Bytes;
use futures_util::future::BoxFuture;
use futures_util::stream::FuturesUnordered;
use futures_util::{FutureExt, StreamExt};
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::time::{Interval, MissedTickBehavior};
use tracing::Instrument;
use tycho_network::PeerId;
use tycho_util::FastHashSet;
use tycho_util::mem::Reclaimer;
use tycho_util::metrics::HistogramGuard;
use tycho_util::sync::rayon_run_fifo;

use crate::dag::{IllFormedReason, Verifier, VerifyError};
use crate::effects::{AltFormat, Cancelled, Ctx, DownloadCtx, TaskResult};
use crate::engine::{MempoolConfig, NodeConfig};
use crate::intercom::core::query::response::DownloadResponse;
use crate::intercom::core::query::{DownloadQuery, QueryError};
use crate::intercom::dependency::limiter::Limiter;
use crate::intercom::dependency::peer_queue::PeerQueue;
use crate::intercom::peer_schedule::PeerState;
use crate::intercom::{Dispatcher, PeerSchedule};
use crate::models::{ParseResult, PeerCount, Point, PointId, PointIntegrityError, StructureIssue};

#[derive(Clone)]
pub struct Downloader {
    inner: Arc<DownloaderInner>,
}

pub enum DownloadResult {
    NotFound,
    Verified(Point),
    IllFormed(Point, IllFormedReason),
}

struct DownloaderInner {
    dispatcher: Dispatcher,
    peer_schedule: PeerSchedule,
    limiter: Limiter,
}

impl Downloader {
    pub fn new(dispatcher: &Dispatcher, peer_schedule: &PeerSchedule) -> Self {
        Self {
            inner: Arc::new(DownloaderInner {
                dispatcher: dispatcher.clone(),
                peer_schedule: peer_schedule.clone(),
                limiter: Limiter::new(NodeConfig::get().max_download_tasks),
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
        broadcast_result: oneshot::Receiver<DownloadResult>,
        ctx: DownloadCtx,
    ) -> TaskResult<DownloadResult> {
        let _guard = self.inner.limiter.enter(point_id.round).await;

        let _task_duration = HistogramGuard::begin("tycho_mempool_download_task_time");
        ctx.meter_start(point_id);
        let entered_span = ctx.span().clone().entered();
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
            .chain(iter::once((&point_id.author, &author_state)));

        let mut interval = tokio::time::interval(Duration::from_millis(
            ctx.conf().consensus.download_retry_millis.get() as _,
        ));
        // no `interval.reset()`: download starts right after biased receive of all known dependers
        // give equal time to every attempt, ignoring local runtime delays; do not `Burst` requests
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let mut task = DownloadTask {
            peer_schedule: self.inner.peer_schedule.clone(),
            ctx,
            query: DownloadQuery::new(self.inner.dispatcher.clone(), point_id),
            peer_count,
            // this node is +1 to 2F; count every peer uniquely, as they may be removed and resolved
            not_found: FastHashSet::with_capacity(peer_count.majority_of_others()),
            updates,
            undone_peers: PeerQueue::new(undone_peers),
            parse_queue: Vec::with_capacity(peer_count.reliable_minority()),
            parse_slot: futures_util::future::pending().boxed(),
            downloading: FuturesUnordered::new(),
            interval,
            attempt: 0,
        };
        let downloaded = task
            .run(dependers_rx, broadcast_result)
            .instrument(entered_span.exit())
            .await?;

        if matches!(downloaded, DownloadResult::NotFound) {
            tracing::warn!(
                parent: task.ctx.span(),
                "not downloaded",
            );
        }

        Reclaimer::instance().drop(task);

        Ok(downloaded)
    }
}

struct DownloadTask {
    peer_schedule: PeerSchedule,
    ctx: DownloadCtx,

    query: DownloadQuery,

    peer_count: PeerCount,
    not_found: FastHashSet<PeerId>, // count only responses considered reliable

    updates: broadcast::Receiver<(PeerId, PeerState)>,

    undone_peers: PeerQueue,
    downloading: FuturesUnordered<BoxFuture<'static, QueryFutureOutput<Bytes>>>,

    parse_queue: Vec<(PeerId, Bytes)>,
    parse_slot: BoxFuture<'static, QueryFutureOutput<ParseResult>>,

    interval: Interval,
    attempt: u8,
}

struct QueryFutureOutput<T> {
    peer_id: PeerId,
    result: Result<DownloadResponse<T>, QueryError>,
}

impl DownloadTask {
    // point's author is a top priority; fallback priority is (any) dependent point's author
    // recursively: every dependency is expected to be signed by 2/3+1
    pub async fn run(
        &mut self,
        mut dependers_rx: mpsc::UnboundedReceiver<PeerId>,
        mut broadcast_result: oneshot::Receiver<DownloadResult>,
    ) -> TaskResult<DownloadResult> {
        // if parse slot is taken - neither create new queries nor increase task limit
        // but continue to poll created queries in order not to cancel them
        let mut parse_slot_taken = false;

        loop {
            tokio::select! {
                biased; // mandatory priority: signals lifecycle, updates, data lifecycle
                bcast_result = &mut broadcast_result => break bcast_result.map_err(|_e| Cancelled()),
                Some(depender) = dependers_rx.recv() => self.add_depender(&depender),
                update = self.updates.recv() => self.match_peer_updates(update)?,
                Some(out) = self.downloading.next() => {
                    let new_result = match out.result {
                        Ok(DownloadResponse::Defined(bytes)) => {
                            if parse_slot_taken {
                                self.parse_queue.push((out.peer_id, bytes));
                            } else {
                                self.parse_slot = Self::parse(out.peer_id, bytes).boxed();
                                parse_slot_taken = true;
                            }
                            continue;
                        },
                        Ok(DownloadResponse::DefinedNone) => Ok(DownloadResponse::DefinedNone),
                        Ok(DownloadResponse::TryLater) => Ok(DownloadResponse::TryLater),
                        Err(err) => Err(err),
                    };
                    let new_out = QueryFutureOutput {
                        peer_id: out.peer_id,
                        result: new_result,
                    };
                    if let Some(result) = self.inspect(new_out) {
                        break Ok(result);
                    } else if self.not_found.len() >= self.peer_count.majority_of_others() {
                        break Ok(DownloadResult::NotFound);
                    } else if !parse_slot_taken {
                        self.download_random(); // keep the current request amount
                    }
                },
                parsed_out = &mut self.parse_slot, if parse_slot_taken => {
                    if let Some(result) = self.inspect(parsed_out) {
                        break Ok(result);
                    } else if self.not_found.len() >= self.peer_count.majority_of_others() {
                        break Ok(DownloadResult::NotFound);
                    }
                    if let Some((peer_id, bytes)) = self.parse_queue.pop() {
                        self.parse_slot = Self::parse(peer_id, bytes).boxed();
                    } else {
                        parse_slot_taken = false;
                    }
                    if !parse_slot_taken {
                        self.download_random(); // keep the current request amount
                    }
                }
                // most rare arm to make progress despite slow responding peers
                _ = self.interval.tick(), if !parse_slot_taken => {
                    // first tick fires immediately
                    self.download_random();
                    self.attempt = self.attempt.wrapping_add(1);
                },
            }
        }
        // on exit futures are dropped and receivers are cleaned,
        // senders will stay in `DagPointFuture` that owns current task
    }

    fn add_depender(&mut self, peer_id: &PeerId) {
        // may have requested and removed the peer, no panic
        let _ = self.undone_peers.update(peer_id, |status| {
            status.is_independent = false;
        });
    }

    fn download_random(&mut self) {
        let amount = current_peers(
            self.attempt,
            self.downloading.len(),
            &self.peer_count,
            self.ctx.conf(),
        );

        for _ in 0..amount {
            let Some(peer_id) = self.undone_peers.take_to_flight() else {
                break;
            };
            let query = self.query.clone();
            self.downloading.push(Box::pin(async move {
                let result = query.send(&peer_id).await;
                QueryFutureOutput { peer_id, result }
            }));
        }
    }

    async fn parse(peer_id: PeerId, bytes: Bytes) -> QueryFutureOutput<ParseResult> {
        let start = std::time::Instant::now();
        let parse_result = rayon_run_fifo(|| Point::parse(bytes.into())).await;
        metrics::histogram!("tycho_mempool_engine_parse_point_time").record(start.elapsed());
        QueryFutureOutput {
            peer_id,
            result: Ok(DownloadResponse::Defined(parse_result)),
        }
    }

    fn inspect(&mut self, out: QueryFutureOutput<ParseResult>) -> Option<DownloadResult> {
        // remove peer status: will not repeat request to the peer in this download task
        enum LastResponse {
            Point(Point),
            IllFormed(Point, StructureIssue),
            BadPoint(PointIntegrityError),
            DefinedNone,
            TlError(tl_proto::TlError),
        }

        let last_response = match out.result {
            Ok(DownloadResponse::Defined(parse_result)) => match parse_result {
                Ok(Ok(Ok(point))) => LastResponse::Point(point),
                Ok(Ok(Err((point, issue)))) => LastResponse::IllFormed(point, issue),
                Ok(Err(bad_point)) => LastResponse::BadPoint(bad_point),
                Err(tl_error) => LastResponse::TlError(tl_error),
            },
            Ok(DownloadResponse::DefinedNone) => LastResponse::DefinedNone,
            Err(QueryError::TlError(tl_error)) => LastResponse::TlError(tl_error),
            Ok(DownloadResponse::TryLater) => {
                // apply the same retry strategy as for network errors
                if !self.undone_peers.update(&out.peer_id, |status| {
                    status.is_in_flight = false;
                    status.failed_queries = status.failed_queries.saturating_add(1);
                }) {
                    panic!("Coding error: peer not in map {}", out.peer_id.alt());
                };
                tracing::trace!(peer = display(out.peer_id.alt()), "try later");
                return None;
            }
            Err(QueryError::Network(network_err)) => {
                if !self.undone_peers.update(&out.peer_id, |status| {
                    status.is_in_flight = false;
                    status.failed_queries = status.failed_queries.saturating_add(1);
                }) {
                    panic!("Coding error: peer not in map {}", out.peer_id.alt());
                };
                metrics::counter!("tycho_mempool_download_query_failed_count").increment(1);
                tracing::debug!(
                    peer = display(out.peer_id.alt()),
                    error = display(network_err),
                    "network error",
                );
                return None;
            }
        };

        let Some(status) = self.undone_peers.remove(&out.peer_id) else {
            panic!("no peer {} in map, concurrent download?", out.peer_id.alt());
        };
        assert!(
            status.is_in_flight,
            "peer {} is not in-flight",
            out.peer_id.alt(),
        );

        match last_response {
            LastResponse::DefinedNone => {
                self.not_found.insert(out.peer_id);
                DownloadCtx::meter_not_found();
                tracing::debug!(
                    peer = display(out.peer_id.alt()),
                    is_depender = (!status.is_independent).then_some(true),
                    "didn't return"
                );
                None
            }
            LastResponse::TlError(tl_error) => {
                self.not_found.insert(out.peer_id);
                DownloadCtx::meter_unreliable();
                tracing::warn!(
                    result = display(&tl_error),
                    peer = display(out.peer_id.alt()),
                    "bad response",
                );
                None
            }
            LastResponse::BadPoint(bad_point) => {
                // reliable peer won't return unverifiable point
                self.not_found.insert(out.peer_id);
                DownloadCtx::meter_unreliable();
                tracing::error!(
                    result = display(&bad_point),
                    peer = display(out.peer_id.alt()),
                    "bad point",
                );
                None
            }
            LastResponse::IllFormed(point, issue) => {
                tracing::error!(
                    error = display(&issue),
                    point = debug(&point),
                    "downloaded ill-formed"
                );
                let reason = IllFormedReason::Structure(issue);
                Some(DownloadResult::IllFormed(point, reason))
            }
            LastResponse::Point(point) if point.info().id() != *self.query.point_id() => {
                self.not_found.insert(out.peer_id);
                DownloadCtx::meter_unreliable();
                let wrong_id = point.info().id();
                tracing::error!(
                    peer_id = display(out.peer_id.alt()),
                    author = display(wrong_id.author.alt()),
                    round = wrong_id.round.0,
                    digest = display(wrong_id.digest.alt()),
                    "returned wrong point",
                );
                None
            }
            LastResponse::Point(point) => {
                match Verifier::verify(point.info(), &self.peer_schedule, self.ctx.conf()) {
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
                // may receive updates from other vset, do not panic
                let _ = (self.undone_peers).update(&peer_id, |status| status.state.0 = new);
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

    fn meter_start(&self, point_id: &PointId) {
        metrics::counter!("tycho_mempool_download_task_count").increment(1);

        metrics::gauge!("tycho_mempool_download_depth_rounds")
            .set(self.download_max_depth(point_id.round));
    }
}

fn current_peers(
    attempt: u8,
    already_downloading: usize,
    peer_count: &PeerCount,
    conf: &MempoolConfig,
) -> usize {
    let download_peers = conf.consensus.download_peers.get() as usize;
    download_peers
        .saturating_mul(attempt as usize)
        .max(1) // for zero attempt
        .min(peer_count.reliable_minority())
        .saturating_sub(already_downloading)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::default_test_config;

    #[test]
    fn linear_basic() {
        let expected_to_add = [1, 2, 4, 6, 8, 10, 11, 11];
        let already_downloading = 0;
        let peer_count = PeerCount::try_from(30).expect("must be enough");

        test_impl(expected_to_add, already_downloading, peer_count);
    }

    #[test]
    fn linear_already_downloading() {
        let expected_to_add = [0, 0, 1, 3, 5, 7, 8, 8];
        let already_downloading = 3;
        let peer_count = PeerCount::try_from(30).expect("must be enough");

        test_impl(expected_to_add, already_downloading, peer_count);
    }

    fn test_impl<const N: usize>(
        expected: [usize; N],
        already_downloading: usize,
        peer_count: PeerCount,
    ) {
        let merged_conf = default_test_config();

        let actual: [usize; N] = std::array::from_fn(|attempt| {
            current_peers(
                attempt as u8,
                already_downloading,
                &peer_count,
                &merged_conf.conf,
            )
        });

        assert_eq!(expected, actual);
    }
}
