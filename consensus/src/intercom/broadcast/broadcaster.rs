use std::mem;
use std::sync::Arc;

use anyhow::Result;
use futures_util::future::BoxFuture;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::{broadcast, oneshot, watch};
use tycho_network::{PeerId, Request};
use tycho_util::{FastHashMap, FastHashSet};

use crate::dag::LastOwnPoint;
use crate::dyn_event;
use crate::effects::{AltFormat, BroadcasterContext, Effects, EngineContext};
use crate::engine::CachedConfig;
use crate::intercom::broadcast::collector::CollectorSignal;
use crate::intercom::dto::{BroadcastResponse, PeerState, SignatureResponse};
use crate::intercom::{Dispatcher, PeerSchedule};
use crate::models::{PeerCount, Point, Signature};

#[derive(Copy, Clone, Debug)]
pub enum BroadcasterSignal {
    Ok,
    Err,
}

pub struct Broadcaster {
    effects: Effects<BroadcasterContext>,
    dispatcher: Dispatcher,
    /// Receiver may be closed (collector finished), so do not require `Ok` on send
    bcaster_signal: Option<oneshot::Sender<BroadcasterSignal>>,
    collector_signal: watch::Receiver<CollectorSignal>,

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
        collector_signal: watch::Receiver<CollectorSignal>,
        round_effects: &Effects<EngineContext>,
    ) -> Self {
        let (signers, bcast_peers, peer_updates) = {
            let guard = peer_schedule.read();
            // `atomic` can be updated only under write lock, so view under read lock is consistent
            let signers = peer_schedule
                .atomic()
                .peers_for(point.round().next())
                .clone();
            let bcast_peers = guard.data.broadcast_receivers().clone();
            (signers, bcast_peers, guard.updates())
        };
        let signers_count =
            PeerCount::try_from(signers.len()).expect("validator set for current round is unknown");

        Self {
            effects: Effects::<BroadcasterContext>::new(round_effects, point.digest()),
            dispatcher,
            bcaster_signal: Some(bcaster_signal),
            collector_signal,

            peer_updates,
            signers,
            signers_count,
            removed_peers: Default::default(),
            rejections: Default::default(),
            signatures: Default::default(),

            bcast_request: Dispatcher::broadcast_request(&point),
            bcast_peers,
            bcast_futures: FuturesUnordered::default(),

            sig_request: Dispatcher::signature_request(point.round()),
            sig_peers: FastHashSet::default(),
            sig_futures: FuturesUnordered::default(),

            point,
        }
    }

    /// returns evidence for broadcast point
    pub async fn run(&mut self) -> Arc<LastOwnPoint> {
        // how this was supposed to work:
        // * in short: broadcast to all and gather signatures from those who accepted the point
        // * both broadcast and signature tasks have their own retry loop for every peer
        // * also, if a peer is not yet ready to accept, there is a loop between tasks of both sorts
        //   (we ping such a peer with a short signature request instead of sending the whole point)
        // * if any async task hangs for too long - try poll another sort of tasks
        // * if no task of some sort - try poll another sort of tasks
        // * periodically check if loop completion requirement is met (2F++ signs or 1/3+1++ fails) -
        //   is a tradeoff between gather at least 2F signatures and do not wait unresponsive peers
        //   (i.e. response bucketing where the last bucket is full and contains 2f-th element)
        // i.e. at any moment any peer may be in a single state:
        // * processing our broadcast request
        // * processing our signature request
        // * enqueued for any of two requests above
        // * rejected to sign our point (incl. rejection of the point itself and incorrect sig)
        // * successfully signed our point and dequeued
        tracing::debug!(
            parent: self.effects.span(),
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
                Ok(()) = self.collector_signal.changed() => {
                    let signal = *self.collector_signal.borrow_and_update();
                    if self.should_finish(signal) {
                        break;
                    }
                }
                // rare event essential for up-to-date retries
                result = self.peer_updates.recv() => {
                    match result {
                        Ok((peer_id, new_state)) => {
                            self.match_peer_updates(peer_id, new_state);
                        }
                        Err(err @ RecvError::Lagged(_)) => {
                            tracing::error!(
                                parent: self.effects.span(),
                                error = display(err),
                                "peer state update"
                            );
                        }
                        Err(err @ RecvError::Closed) => {
                            tracing::error!("peer state update {err}");
                            break
                        }
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
                    let _guard = self.effects.span().enter();
                    panic!("unhandled match arm in Broadcaster::run tokio::select");
                }
            }
        }
        metrics::counter!("tycho_mempool_collected_signatures_count")
            .increment(self.signatures.len() as _);
        Arc::new(LastOwnPoint {
            digest: *self.point.digest(),
            evidence: mem::take(&mut self.signatures).into_iter().collect(),
            round: self.point.round(),
            signers: self.signers_count,
        })
    }

    pub async fn run_continue(mut self) {
        let mut retry_interval = tokio::time::interval(CachedConfig::broadcast_retry());
        retry_interval.reset_immediately();
        for peer in mem::take(&mut self.bcast_peers) {
            self.broadcast(&peer);
        }
        loop {
            tokio::select! {
                biased; // mandatory priority: signals lifecycle, updates, data lifecycle
                // rare event essential for up-to-date retries
                update = self.peer_updates.recv() => {
                    self.match_peer_updates(update);
                }
                _ = retry_interval.tick() => {
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
                    let _guard = self.effects.span().enter();
                    panic!("unhandled match arm in Broadcaster::run_continue tokio::select");
                }
            }
        }
    }

    fn should_finish(&mut self, collector_signal: CollectorSignal) -> bool {
        let result = match collector_signal {
            // though we return successful result, it will be discarded on Err
            CollectorSignal::Finish | CollectorSignal::Err => true,
            CollectorSignal::Retry => {
                if self.rejections.len() >= self.signers_count.reliable_minority() {
                    if let Some(sender) = mem::take(&mut self.bcaster_signal) {
                        _ = sender.send(BroadcasterSignal::Err);
                    };
                    true
                } else {
                    if self.signatures.len() >= self.signers_count.majority_of_others() {
                        if let Some(sender) = mem::take(&mut self.bcaster_signal) {
                            _ = sender.send(BroadcasterSignal::Ok);
                        };
                    }
                    for peer in mem::take(&mut self.sig_peers) {
                        self.request_signature(false, &peer);
                    }
                    false
                }
            }
        };
        tracing::debug!(
            parent: self.effects.span(),
            result = result,
            collector_signal = debug(collector_signal),
            signatures = self.signatures.len(),
            "2F" = self.signers_count.majority_of_others(),
            rejections = self.rejections.len(),
            "F+1" = self.signers_count.reliable_minority(),
            "ready?",
        );
        result
    }

    fn match_broadcast_result(&mut self, peer_id: &PeerId, result: Result<BroadcastResponse>) {
        match result {
            Err(error) => {
                self.sig_peers.insert(*peer_id); // lighter weight retry loop
                tracing::warn!(
                    parent: self.effects.span(),
                    peer = display(peer_id.alt()),
                    error = display(error),
                    "failed to send broadcast to"
                );
            }
            Ok(BroadcastResponse) => {
                // self.sig_peers.insert(*peer_id); // give some time to validate
                self.request_signature(true, peer_id); // fast nodes may have delivered it as a dependency
                tracing::trace!(
                    parent: self.effects.span(),
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
                    parent: self.effects.span(),
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
                    parent: self.effects.span(),
                    level,
                    peer = display(peer_id.alt()),
                    response = display(response.alt()),
                    "signature response from"
                );
                match response {
                    SignatureResponse::Signature(signature) => {
                        if self.signers.contains(peer_id) {
                            if signature.verifies(peer_id, self.point.digest()) {
                                self.signatures.insert(*peer_id, signature);
                            } else {
                                // any invalid signature lowers our chances
                                // to successfully finish current round
                                self.rejections.insert(*peer_id);
                            }
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
                parent: self.effects.span(),
                peer = display(peer_id.alt()),
                "sending broadcast to"
            );
        } else {
            tracing::warn!(
                parent: self.effects.span(),
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
                parent: self.effects.span(),
                peer = display(peer_id.alt()),
                "requesting signature from"
            );
        } else {
            tracing::warn!(
                parent: self.effects.span(),
                peer = display(peer_id.alt()),
                "will not request signature from"
            );
        }
    }

    fn match_peer_updates(&mut self, peer_id: PeerId, new_state: PeerState) {
        tracing::info!(
            parent: self.effects.span(),
            peer = display(peer_id.alt()),
            new_state = debug(new_state),
            "peer state update",
        );
        match new_state {
            PeerState::Resolved => {
                self.removed_peers.remove(&peer_id);
                self.rejections.remove(&peer_id);
                self.broadcast(&peer_id);
            }
            PeerState::Unknown => _ = self.removed_peers.insert(peer_id),
        }
    }
}
