use std::mem;
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use futures_util::future::BoxFuture;
use futures_util::stream::FuturesUnordered;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::{broadcast, oneshot, watch};
use tokio::time::{Interval, MissedTickBehavior};
use tycho_network::PeerId;
use tycho_util::{FastHashMap, FastHashSet};

use crate::dag::LastOwnPoint;
use crate::dyn_event;
use crate::effects::{AltFormat, BroadcastCtx, Cancelled, Ctx, RoundCtx, TaskResult};
use crate::intercom::broadcast::collector::CollectorStatus;
use crate::intercom::core::query::response::{BroadcastResponse, SignatureResponse};
use crate::intercom::core::query::{BroadcastQuery, QueryError, SignatureQuery};
use crate::intercom::peer_schedule::PeerState;
use crate::intercom::{Dispatcher, PeerSchedule};
use crate::models::{PeerCount, Point, Signature};

#[derive(Copy, Clone, Debug)]
pub enum BroadcasterSignal {
    Ok,
    Err,
}

pub struct Broadcaster {
    ctx: BroadcastCtx,
    /// Receiver may be closed (collector finished), so do not require `Ok` on send
    continue_interval: Interval,

    peer_updates: broadcast::Receiver<(PeerId, PeerState)>,
    removed_peers: FastHashSet<PeerId>,
    inflight_peers: FastHashSet<PeerId>,
    // every connected peer should receive broadcast, but only signer's signatures are accountable
    signers: Arc<FastHashSet<PeerId>>,
    signers_count: PeerCount,
    // results
    rejections: FastHashSet<PeerId>,
    signatures: FastHashMap<PeerId, Signature>,

    bcast_query: Arc<BroadcastQuery>,
    bcast_peers: FastHashSet<PeerId>,
    bcast_futures: FuturesUnordered<BoxFuture<'static, BcastFutureOutput>>,

    sig_query: Arc<SignatureQuery>,
    sig_peers: FastHashSet<PeerId>,
    sig_futures: FuturesUnordered<BoxFuture<'static, SigFutureOutput>>,

    point: Point,
}

struct BcastFutureOutput {
    peer_id: PeerId,
    result: Result<BroadcastResponse, QueryError>,
}

struct SigFutureOutput {
    peer_id: PeerId,
    after_bcast: bool,
    result: Result<SignatureResponse, QueryError>,
}

struct CollectorWire {
    /// Receiver may be closed (collector finished), so do not require `Ok` on send
    bcaster_signal: Option<oneshot::Sender<BroadcasterSignal>>,
    collector_status: watch::Receiver<CollectorStatus>,
}

impl Broadcaster {
    pub fn new(
        dispatcher: Dispatcher,
        point: Point,
        peer_schedule: PeerSchedule,
        round_ctx: &RoundCtx,
    ) -> Self {
        let (signers, bcast_peers, peer_updates) = {
            let guard = peer_schedule.read();
            // `atomic` can be updated only under write lock, so view under read lock is consistent
            let signers = peer_schedule
                .atomic()
                .peers_for(point.info().round().next())
                .clone();
            let bcast_peers = guard.data.broadcast_receivers().clone();
            (signers, bcast_peers, guard.updates())
        };
        let signers_count =
            PeerCount::try_from(signers.len()).expect("validator set for current round is unknown");

        // cannot be copied from Collector, so just create a similar one and synchronise
        let mut continue_interval = tokio::time::interval(Duration::from_millis(
            round_ctx.conf().consensus.broadcast_retry_millis.get() as _,
        ));
        continue_interval.reset(); // no immediate tick
        continue_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        Self {
            ctx: BroadcastCtx::new(round_ctx, &point),
            continue_interval,

            peer_updates,
            signers,
            signers_count,
            removed_peers: FastHashSet::from_iter([*point.info().author()]), // no loopback
            inflight_peers: FastHashSet::default(),
            rejections: Default::default(),
            signatures: Default::default(),

            bcast_query: Arc::new(BroadcastQuery::new(dispatcher.clone(), &point)),
            bcast_peers,
            bcast_futures: FuturesUnordered::default(),

            sig_query: Arc::new(SignatureQuery::new(dispatcher, point.info().round())),
            sig_peers: FastHashSet::default(),
            sig_futures: FuturesUnordered::default(),

            point,
        }
    }

    /// returns evidence for broadcast point
    pub async fn run(
        &mut self,
        bcaster_signal: oneshot::Sender<BroadcasterSignal>,
        collector_status: watch::Receiver<CollectorStatus>,
    ) -> TaskResult<Arc<LastOwnPoint>> {
        // how this was supposed to work:
        // * in short: broadcast to all and gather signatures from those who accepted the point
        // * both broadcast and signature tasks have their own retry loop for every peer
        // * also, if a peer is not yet ready to accept, there is a loop between tasks of both sorts
        //   (we ping such a peer with a short signature request instead of sending the whole point)
        // * if any async task hangs for too long - try poll another sort of tasks
        // * if no task of some sort - try poll another sort of tasks
        // * periodically check if loop completion requirement is met (2F signs or 1F+1 fails) -
        //   is a tradeoff between gather at least 2F signatures and do not wait unresponsive peers
        //   (i.e. response bucketing where the last bucket is full and contains 2f-th element)
        // i.e. at any moment any peer may be in a single state:
        // * processing our broadcast request
        // * processing our signature request
        // * enqueued for any of two requests above
        // * rejected to sign our point (incl. rejection of the point itself and incorrect sig)
        // * successfully signed our point and dequeued
        tracing::debug!(
            parent: self.ctx.span(),
            current_peers = self.bcast_peers.len(),
            "start",
        );
        for peer in mem::take(&mut self.bcast_peers) {
            self.broadcast(&peer);
        }
        let mut wire = CollectorWire {
            bcaster_signal: Some(bcaster_signal),
            collector_status,
        };
        loop {
            tokio::select! {
                biased; // mandatory priority: signals lifecycle, updates, data lifecycle
                // rare event that may cause immediate completion
                result = wire.collector_status.changed() => {
                    if self.should_finish(&mut wire, result) {
                        break;
                    }
                },
                // rare event essential for up-to-date retries
                update = self.peer_updates.recv() => self.match_peer_updates(update)?,
                // either request signature immediately or postpone until retry
                Some(out) = self.bcast_futures.next() => {
                    self.match_broadcast_result(out);
                },
                // most frequent arm that provides data to decide if retry or fail or finish
                Some(out) = self.sig_futures.next() => {
                    self.match_signature_result(out);
                },
                else => {
                    let _guard = self.ctx.span().enter();
                    panic!("unhandled match arm in Broadcaster::run tokio::select");
                }
            }
        }
        Ok(Arc::new(LastOwnPoint {
            digest: *self.point.info().digest(),
            evidence: mem::take(&mut self.signatures).into_iter().collect(),
            includes: self.point.info().includes().clone(),
            round: self.point.info().round(),
            signers: self.signers_count,
        }))
    }

    pub async fn run_continue(mut self, round_ctx: &RoundCtx) -> TaskResult<()> {
        self.ctx = BroadcastCtx::new(round_ctx, &self.point);
        let mut attempt: u8 = 1; // we do nothing at attempt==1 that Collector fires immediately
        // `self.bcast_peers` is not re-populated, so will add work only if none was added earlier
        for peer in mem::take(&mut self.bcast_peers) {
            self.broadcast(&peer);
        }
        loop {
            tokio::select! {
                biased; // mandatory priority: signals lifecycle, updates, data lifecycle
                // rare event essential for up-to-date retries
                update = self.peer_updates.recv() => self.match_peer_updates(update)?,
                _ = self.continue_interval.tick() => {
                    attempt = attempt.wrapping_add(1);
                    self.match_attempt(attempt);
                }
                // either request signature immediately or postpone until retry
                Some(out) = self.bcast_futures.next() => {
                    self.match_broadcast_result(out);
                },
                // most frequent arm that provides data to decide if retry or fail or finish
                Some(out) = self.sig_futures.next() => {
                    self.match_signature_result(out);
                },
                else => {
                    let _guard = self.ctx.span().enter();
                    panic!("unhandled match arm in Broadcaster::run_continue tokio::select");
                }
            }
        }
    }

    fn should_finish(
        &mut self,
        wire: &mut CollectorWire,
        collector_signal: Result<(), watch::error::RecvError>,
    ) -> bool {
        let collector_status = *wire.collector_status.borrow_and_update();

        let is_ready = if collector_signal.is_ok() {
            self.continue_interval.reset(); // synchronise tick delays

            if self.rejections.len() >= self.signers_count.reliable_minority() {
                if let Some(sender) = mem::take(&mut wire.bcaster_signal) {
                    _ = sender.send(BroadcasterSignal::Err);
                };
                true
            } else {
                if collector_status.attempt >= self.ctx.conf().consensus.min_sign_attempts.get()
                    && self.signatures.len() >= self.signers_count.majority_of_others()
                    && let Some(sender) = mem::take(&mut wire.bcaster_signal)
                {
                    _ = sender.send(BroadcasterSignal::Ok);
                };
                self.match_attempt(collector_status.attempt);
                false
            }
        } else {
            true
        };
        tracing::debug!(
            parent: self.ctx.span(),
            result = is_ready,
            collector = format!(
                "{{ {collector_signal:?}, attempt={}, ready={} }}",
                collector_status.attempt,
                collector_status.ready
            ),
            signatures = self.signatures.len(),
            "2F" = self.signers_count.majority_of_others(),
            rejections = self.rejections.len(),
            "F+1" = self.signers_count.reliable_minority(),
            "ready?",
        );
        is_ready
    }

    fn match_attempt(&mut self, attempt: u8) {
        // `Collector` emits `1` immediately, that's when we start broadcasts, so ignored here;
        // don't bother to do nothing at 257th attempt
        if attempt == 0 {
            // network is stuck, give all broadcast filters a push; forget rejections
            self.sig_peers.clear();
            self.rejections.clear();
            self.sig_futures.clear();
            self.bcast_futures.clear();
            self.inflight_peers.clear();
            let peers = self.signers.clone();
            BroadcastCtx::retry(peers.len());
            for peer in &*peers {
                self.broadcast(peer);
            }
        } else if attempt > 1 {
            let peers = mem::take(&mut self.sig_peers);
            BroadcastCtx::retry(peers.len());
            for peer in &peers {
                self.request_signature(false, peer);
            }
        }
    }

    fn match_broadcast_result(&mut self, out: BcastFutureOutput) {
        assert!(
            self.inflight_peers.remove(&out.peer_id),
            "peer broadcast query not in flight"
        );
        match out.result {
            Err(QueryError::TlError(error)) => {
                self.rejections.insert(out.peer_id);
                tracing::warn!(
                    parent: self.ctx.span(),
                    peer = display(out.peer_id.alt()),
                    error = display(&error),
                    "bad response to broadcast from"
                );
                self.bcast_query.report(&out.peer_id, error);
            }
            Err(QueryError::Network(error)) => {
                self.sig_peers.insert(out.peer_id); // lighter weight retry loop
                tracing::debug!(
                    parent: self.ctx.span(),
                    peer = display(out.peer_id.alt()),
                    error = display(error),
                    "failed to send broadcast to"
                );
            }
            Ok(BroadcastResponse) => {
                // self.sig_peers.insert(*peer_id); // give some time to validate
                self.request_signature(true, &out.peer_id); // fast nodes may have delivered it as a dependency
                tracing::trace!(
                    parent: self.ctx.span(),
                    peer = display(out.peer_id.alt()),
                    "finished broadcast to"
                );
            }
        }
    }

    fn match_signature_result(&mut self, out: SigFutureOutput) {
        assert!(
            self.inflight_peers.remove(&out.peer_id),
            "peer signature query not in flight"
        );
        match out.result {
            Err(QueryError::TlError(error)) => {
                self.rejections.insert(out.peer_id);
                tracing::warn!(
                    parent: self.ctx.span(),
                    peer = display(out.peer_id.alt()),
                    error = display(&error),
                    "bad signature response from"
                );
                self.sig_query.report(&out.peer_id, error);
            }
            Err(QueryError::Network(error)) => {
                self.sig_peers.insert(out.peer_id); // let it retry
                tracing::debug!(
                    parent: self.ctx.span(),
                    peer = display(out.peer_id.alt()),
                    error = display(error),
                    "failed to query signature from"
                );
            }
            Ok(response) => {
                let level = match response {
                    SignatureResponse::Rejected(_) => tracing::Level::WARN,
                    _ => tracing::Level::DEBUG,
                };
                dyn_event!(
                    parent: self.ctx.span(),
                    level,
                    peer = display(out.peer_id.alt()),
                    response = display(response.alt()),
                    "signature response from"
                );
                match response {
                    SignatureResponse::Signature(signature) => {
                        if self.signers.contains(&out.peer_id) {
                            if signature.verifies(&out.peer_id, self.point.info().digest()) {
                                self.signatures.insert(out.peer_id, signature);
                                BroadcastCtx::sig_collected();
                            } else {
                                // any invalid signature lowers our chances
                                // to successfully finish current round
                                self.rejections.insert(out.peer_id);
                                BroadcastCtx::sig_rejected();
                                BroadcastCtx::sig_unreliable();
                            }
                        } else {
                            BroadcastCtx::sig_unreliable();
                        }
                    }
                    SignatureResponse::NoPoint => {
                        if out.after_bcast {
                            _ = self.sig_peers.insert(out.peer_id); // retry on next attempt
                        } else {
                            self.broadcast(&out.peer_id); // send data immediately
                        }
                    }
                    SignatureResponse::TryLater => _ = self.sig_peers.insert(out.peer_id),
                    SignatureResponse::Rejected(_) => {
                        if self.signers.contains(&out.peer_id) {
                            self.rejections.insert(out.peer_id);
                            BroadcastCtx::sig_rejected();
                        } else {
                            BroadcastCtx::sig_unreliable();
                        }
                    }
                }
            }
        }
    }

    fn broadcast(&mut self, peer_id: &PeerId) {
        if !self.removed_peers.contains(peer_id) {
            assert!(
                self.inflight_peers.insert(*peer_id),
                "cannot broadcast: peer already in flight"
            );
            self.bcast_futures.push({
                let bcast_query = self.bcast_query.clone();
                let peer_id = *peer_id;
                Box::pin(async move {
                    let result = bcast_query.send(&peer_id).await;
                    BcastFutureOutput { peer_id, result }
                })
            });
            tracing::trace!(
                parent: self.ctx.span(),
                peer = display(peer_id.alt()),
                "sending broadcast to"
            );
        } else {
            tracing::warn!(
                parent: self.ctx.span(),
                peer = display(peer_id.alt()),
                "will not broadcast to"
            );
        }
    }

    fn request_signature(&mut self, after_bcast: bool, peer_id: &PeerId) {
        if !self.removed_peers.contains(peer_id) && self.signers.contains(peer_id) {
            assert!(
                self.inflight_peers.insert(*peer_id),
                "cannot request signature: peer already in flight"
            );
            self.sig_futures.push({
                let sig_query = self.sig_query.clone();
                let peer_id = *peer_id;
                Box::pin(async move {
                    let result = sig_query.send(&peer_id).await;
                    SigFutureOutput {
                        peer_id,
                        after_bcast,
                        result,
                    }
                })
            });
            tracing::trace!(
                parent: self.ctx.span(),
                peer = display(peer_id.alt()),
                "requesting signature from"
            );
        } else {
            tracing::warn!(
                parent: self.ctx.span(),
                peer = display(peer_id.alt()),
                "will not request signature from"
            );
        }
    }

    fn match_peer_updates(
        &mut self,
        result: Result<(PeerId, PeerState), RecvError>,
    ) -> TaskResult<()> {
        match result {
            Ok((peer_id, new_state)) => {
                tracing::info!(
                    parent: self.ctx.span(),
                    peer = display(peer_id.alt()),
                    new_state = debug(new_state),
                    "peer state update",
                );
                match new_state {
                    PeerState::Resolved => {
                        self.removed_peers.remove(&peer_id);
                        self.rejections.remove(&peer_id);
                        if !self.inflight_peers.contains(&peer_id) {
                            self.bcast_peers.remove(&peer_id);
                            self.sig_peers.remove(&peer_id);
                            // let peer determine current consensus round
                            self.broadcast(&peer_id);
                        }
                    }
                    PeerState::Unknown => _ = self.removed_peers.insert(peer_id),
                }
                Ok(())
            }
            Err(err @ RecvError::Lagged(_)) => {
                tracing::error!(
                    parent: self.ctx.span(),
                    error = display(err),
                    "peer state update"
                );
                Ok(())
            }
            Err(err @ RecvError::Closed) => {
                tracing::error!(
                    parent: self.ctx.span(),
                    error = display(err),
                    "peer state update"
                );
                Err(Cancelled())
            }
        }
    }
}

impl BroadcastCtx {
    fn retry(amount: usize) {
        metrics::counter!("tycho_mempool_broadcaster_retry_count").increment(amount as u64);
    }
    fn sig_collected() {
        metrics::counter!("tycho_mempool_signatures_collected_count").increment(1);
    }
    fn sig_rejected() {
        metrics::counter!("tycho_mempool_signatures_rejected_count").increment(1);
    }
    fn sig_unreliable() {
        metrics::counter!("tycho_mempool_signatures_unreliable_count").increment(1);
    }
}
