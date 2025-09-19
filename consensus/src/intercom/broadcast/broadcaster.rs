use std::mem;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use futures_util::StreamExt;
use futures_util::future::BoxFuture;
use futures_util::stream::FuturesUnordered;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::{broadcast, oneshot, watch};
use tokio::time::MissedTickBehavior;
use tycho_network::{PeerId, Request};
use tycho_util::{FastHashMap, FastHashSet};

use crate::dag::LastOwnPoint;
use crate::dyn_event;
use crate::effects::{AltFormat, BroadcastCtx, Cancelled, Ctx, RoundCtx, TaskResult};
use crate::intercom::broadcast::collector::CollectorStatus;
use crate::intercom::core::{BroadcastResponse, QueryRequest, SignatureResponse};
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
    dispatcher: Dispatcher,
    /// Receiver may be closed (collector finished), so do not require `Ok` on send
    bcaster_signal: Option<oneshot::Sender<BroadcasterSignal>>,
    collector_status: watch::Receiver<CollectorStatus>,

    peer_updates: broadcast::Receiver<(PeerId, PeerState)>,
    removed_peers: FastHashSet<PeerId>,
    // every connected peer should receive broadcast, but only signer's signatures are accountable
    signers: Arc<FastHashSet<PeerId>>,
    signers_count: PeerCount,
    // results
    rejections: FastHashSet<PeerId>,
    signatures: FastHashMap<PeerId, Signature>,

    bcast_request: Request,
    bcast_peers: FastHashSet<PeerId>,
    bcast_futures: FuturesUnordered<BoxFuture<'static, (PeerId, Result<BroadcastResponse>)>>,

    sig_request: Request,
    sig_peers: FastHashSet<PeerId>,
    sig_futures: FuturesUnordered<BoxFuture<'static, (PeerId, bool, Result<SignatureResponse>)>>,

    point: Point,
}

impl Broadcaster {
    pub fn new(
        dispatcher: Dispatcher,
        point: Point,
        peer_schedule: PeerSchedule,
        bcaster_signal: oneshot::Sender<BroadcasterSignal>,
        collector_status: watch::Receiver<CollectorStatus>,
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

        Self {
            ctx: BroadcastCtx::new(round_ctx, &point),
            dispatcher,
            bcaster_signal: Some(bcaster_signal),
            collector_status,

            peer_updates,
            signers,
            signers_count,
            removed_peers: FastHashSet::from_iter([*point.info().author()]), // no loopback
            rejections: Default::default(),
            signatures: Default::default(),

            bcast_request: QueryRequest::broadcast(&point),
            bcast_peers,
            bcast_futures: FuturesUnordered::default(),

            sig_request: QueryRequest::signature(point.info().round()),
            sig_peers: FastHashSet::default(),
            sig_futures: FuturesUnordered::default(),

            point,
        }
    }

    /// returns evidence for broadcast point
    pub async fn run(&mut self) -> TaskResult<Arc<LastOwnPoint>> {
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
        loop {
            tokio::select! {
                biased; // mandatory priority: signals lifecycle, updates, data lifecycle
                // rare event that may cause immediate completion
                result = self.collector_status.changed() => {
                    if self.should_finish(result) {
                        break;
                    }
                },
                // rare event essential for up-to-date retries
                update = self.peer_updates.recv() => self.match_peer_updates(update)?,
                // either request signature immediately or postpone until retry
                Some((peer_id, result)) = self.bcast_futures.next() => {
                    self.match_broadcast_result(&peer_id, result);
                },
                // most frequent arm that provides data to decide if retry or fail or finish
                Some((peer_id, after_bcast, result)) = self.sig_futures.next() => {
                    self.match_signature_result(&peer_id, after_bcast, result);
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

        let mut retry_interval = tokio::time::interval(Duration::from_millis(
            self.ctx.conf().consensus.broadcast_retry_millis.get() as _,
        ));
        retry_interval.reset(); // query signatures after time passes, just to resend broadcast
        retry_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        for peer in mem::take(&mut self.bcast_peers) {
            self.broadcast(&peer);
        }
        loop {
            tokio::select! {
                biased; // mandatory priority: signals lifecycle, updates, data lifecycle
                // rare event essential for up-to-date retries
                update = self.peer_updates.recv() => self.match_peer_updates(update)?,
                _ = retry_interval.tick() => {
                    BroadcastCtx::retry(self.sig_peers.len());
                    for peer in mem::take(&mut self.sig_peers) {
                        self.request_signature(false, &peer);
                    }
                }
                // either request signature immediately or postpone until retry
                Some((peer_id, result)) = self.bcast_futures.next() => {
                    self.match_broadcast_result(&peer_id, result);
                },
                // most frequent arm that provides data to decide if retry or fail or finish
                Some((peer_id, after_bcast, result)) = self.sig_futures.next() => {
                    self.match_signature_result(&peer_id, after_bcast, result);
                },
                else => {
                    let _guard = self.ctx.span().enter();
                    panic!("unhandled match arm in Broadcaster::run_continue tokio::select");
                }
            }
        }
    }

    fn should_finish(&mut self, collector_signal: Result<(), watch::error::RecvError>) -> bool {
        // don't mark as seen, otherwise may skip ERR notification when collector exits
        let collector_status = *self.collector_status.borrow();

        let is_ready = if collector_signal.is_ok() {
            if self.rejections.len() >= self.signers_count.reliable_minority() {
                if let Some(sender) = mem::take(&mut self.bcaster_signal) {
                    _ = sender.send(BroadcasterSignal::Err);
                };
                true
            } else {
                if collector_status.attempt >= self.ctx.conf().consensus.min_sign_attempts.get()
                    && self.signatures.len() >= self.signers_count.majority_of_others()
                    && let Some(sender) = mem::take(&mut self.bcaster_signal)
                {
                    _ = sender.send(BroadcasterSignal::Ok);
                };
                if collector_status.attempt == 0 {
                    // network is stuck, give all broadcast filters a push; forget rejections
                    self.sig_peers.clear();
                    self.rejections.clear();
                    self.sig_futures.clear();
                    self.bcast_futures.clear();
                    let peers = self.signers.clone();
                    BroadcastCtx::retry(peers.len());
                    for peer in &*peers {
                        self.broadcast(peer);
                    }
                } else if collector_status.attempt > 1 {
                    let peers = mem::take(&mut self.sig_peers);
                    BroadcastCtx::retry(peers.len());
                    for peer in &peers {
                        self.request_signature(false, peer);
                    }
                }
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

    fn match_broadcast_result(&mut self, peer_id: &PeerId, result: Result<BroadcastResponse>) {
        match result {
            Err(error) => {
                self.sig_peers.insert(*peer_id); // lighter weight retry loop
                tracing::warn!(
                    parent: self.ctx.span(),
                    peer = display(peer_id.alt()),
                    error = display(error),
                    "failed to send broadcast to"
                );
            }
            Ok(BroadcastResponse) => {
                // self.sig_peers.insert(*peer_id); // give some time to validate
                self.request_signature(true, peer_id); // fast nodes may have delivered it as a dependency
                tracing::trace!(
                    parent: self.ctx.span(),
                    peer = display(peer_id.alt()),
                    "finished broadcast to"
                );
            }
        }
    }

    fn match_signature_result(
        &mut self,
        peer_id: &PeerId,
        after_bcast: bool,
        result: Result<SignatureResponse>,
    ) {
        match result {
            Err(error) => {
                self.sig_peers.insert(*peer_id); // let it retry
                tracing::warn!(
                    parent: self.ctx.span(),
                    peer = display(peer_id.alt()),
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
                    peer = display(peer_id.alt()),
                    response = display(response.alt()),
                    "signature response from"
                );
                match response {
                    SignatureResponse::Signature(signature) => {
                        if self.signers.contains(peer_id) {
                            if signature.verifies(peer_id, self.point.info().digest()) {
                                self.signatures.insert(*peer_id, signature);
                                BroadcastCtx::sig_collected();
                            } else {
                                // any invalid signature lowers our chances
                                // to successfully finish current round
                                self.rejections.insert(*peer_id);
                                BroadcastCtx::sig_rejected();
                                BroadcastCtx::sig_unreliable();
                            }
                        } else {
                            BroadcastCtx::sig_unreliable();
                        }
                    }
                    SignatureResponse::NoPoint => {
                        if after_bcast {
                            _ = self.sig_peers.insert(*peer_id); // retry on next attempt
                        } else {
                            self.broadcast(peer_id); // send data immediately
                        }
                    }
                    SignatureResponse::TryLater => _ = self.sig_peers.insert(*peer_id),
                    SignatureResponse::Rejected(_) => {
                        if self.signers.contains(peer_id) {
                            self.rejections.insert(*peer_id);
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
            self.bcast_futures.push(
                self.dispatcher
                    .query_broadcast(peer_id, &self.bcast_request),
            );
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
            self.sig_futures.push(self.dispatcher.query_signature(
                peer_id,
                after_bcast,
                &self.sig_request,
            ));
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
                        // let peer determine current consensus round
                        self.broadcast(&peer_id);
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
