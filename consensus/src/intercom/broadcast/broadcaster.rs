use std::mem;

use anyhow::Result;
use futures_util::future::BoxFuture;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::{broadcast, mpsc, oneshot};
use tycho_network::PeerId;
use tycho_util::{FastHashMap, FastHashSet};

use crate::dyn_event;
use crate::effects::{AltFormat, CurrentRoundContext, Effects, EffectsContext};
use crate::intercom::broadcast::collector::CollectorSignal;
use crate::intercom::broadcast::utils::QueryResponses;
use crate::intercom::dto::{BroadcastResponse, PeerState, SignatureResponse};
use crate::intercom::{Dispatcher, PeerSchedule};
use crate::models::{Digest, PeerCount, Point, Signature};

#[derive(Copy, Clone, Debug)]
pub enum BroadcasterSignal {
    Ok,
    Err,
}

pub struct Broadcaster {
    dispatcher: Dispatcher,
    // do not throw away unfinished broadcasts from previous round
    bcast_outdated: Option<QueryResponses<BroadcastResponse>>,
}

impl Broadcaster {
    pub fn new(dispatcher: &Dispatcher) -> Self {
        Self {
            dispatcher: dispatcher.clone(),
            // will be replaced with `None` during task execution
            bcast_outdated: Some(QueryResponses::default()),
        }
    }
    pub async fn run(
        &mut self,
        round_effects: &Effects<CurrentRoundContext>,
        point: &Point,
        peer_schedule: &PeerSchedule,
        bcaster_signal: oneshot::Sender<BroadcasterSignal>,
        collector_signal: mpsc::UnboundedReceiver<CollectorSignal>,
    ) -> FastHashMap<PeerId, Signature> {
        let signers = peer_schedule
            .peers_for(point.body().location.round.next())
            .iter()
            .map(|(peer_id, _)| *peer_id)
            .collect::<FastHashSet<_>>();
        let signers_count =
            PeerCount::try_from(signers.len()).expect("validator set for current round is unknown");

        let bcast_outdated = mem::take(&mut self.bcast_outdated).expect("cannot be unset");
        let mut bcast_peers = peer_schedule.all_resolved();
        bcast_peers.retain(|peer| !bcast_outdated.contains(peer));

        let mut task = BroadcasterTask {
            effects: Effects::<BroadcasterContext>::new(round_effects, point.digest()),
            dispatcher: self.dispatcher.clone(),
            point_digest: point.digest().clone(),
            bcaster_signal: Some(bcaster_signal),
            collector_signal,

            peer_updates: peer_schedule.updates(),
            signers,
            signers_count,
            removed_peers: Default::default(),
            rejections: Default::default(),
            signatures: Default::default(),

            bcast_request: Dispatcher::broadcast_request(point),
            bcast_current: QueryResponses::default(),
            bcast_outdated,

            sig_request: Dispatcher::signature_request(point.body().location.round),
            sig_peers: FastHashSet::default(),
            sig_current: FuturesUnordered::default(),
        };
        task.run(bcast_peers).await;
        // preserve only broadcasts from the last round and drop older ones as hung up
        self.bcast_outdated = Some(task.bcast_current);
        task.signatures
    }
}

struct BroadcasterTask {
    effects: Effects<BroadcasterContext>,
    dispatcher: Dispatcher,
    point_digest: Digest,
    /// Receiver may be closed (collector finished), so do not require `Ok` on send
    bcaster_signal: Option<oneshot::Sender<BroadcasterSignal>>,
    collector_signal: mpsc::UnboundedReceiver<CollectorSignal>,

    peer_updates: broadcast::Receiver<(PeerId, PeerState)>,
    removed_peers: FastHashSet<PeerId>,
    // every connected peer should receive broadcast, but only signer's signatures are accountable
    signers: FastHashSet<PeerId>,
    signers_count: PeerCount,
    // results
    rejections: FastHashSet<PeerId>,
    signatures: FastHashMap<PeerId, Signature>,

    bcast_request: tycho_network::Request,
    bcast_current: QueryResponses<BroadcastResponse>,
    bcast_outdated: QueryResponses<BroadcastResponse>,

    sig_request: tycho_network::Request,
    sig_peers: FastHashSet<PeerId>,
    sig_current: FuturesUnordered<BoxFuture<'static, (PeerId, Result<SignatureResponse>)>>,
}

impl BroadcasterTask {
    /// returns evidence for broadcast point
    pub async fn run(&mut self, bcast_peers: FastHashSet<PeerId>) {
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
            current_peers = bcast_peers.len(),
            outdated_peers = self.bcast_outdated.len(),
            "start",
        );
        for peer in bcast_peers {
            self.broadcast(&peer);
        }
        loop {
            tokio::select! {
                Some((peer, _)) = self.bcast_outdated.next() => {
                    self.broadcast(&peer);
                }
                Some((peer_id, result)) = self.bcast_current.next() => {
                    self.match_broadcast_result(&peer_id, result);
                },
                Some((peer_id, result)) = self.sig_current.next() =>  {
                    self.match_signature_result(&peer_id, result);
                },
                Some(collector_signal) = self.collector_signal.recv() => {
                    if self.should_finish(collector_signal) {
                        break;
                    }
                }
                update = self.peer_updates.recv() => {
                    self.match_peer_updates(update);
                }
                else => {
                    let _guard = self.effects.span().enter();
                    panic!("unhandled match arm in Broadcaster tokio::select");
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
                        self.request_signature(&peer);
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
                tracing::error!(
                    parent: self.effects.span(),
                    peer = display(peer_id.alt()),
                    error = display(error),
                    "failed to send broadcast to"
                );
            }
            Ok(BroadcastResponse) => {
                // self.sig_peers.insert(*peer_id); // give some time to validate
                self.request_signature(peer_id); // fast nodes may have delivered it as a dependency
                tracing::trace!(
                    parent: self.effects.span(),
                    peer = display(peer_id.alt()),
                    "finished broadcast to"
                );
            }
        }
    }

    fn match_signature_result(&mut self, peer_id: &PeerId, result: Result<SignatureResponse>) {
        match result {
            Err(error) => {
                self.sig_peers.insert(*peer_id); // let it retry
                tracing::error!(
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
                            if signature.verifies(peer_id, &self.point_digest) {
                                self.signatures.insert(*peer_id, signature);
                            } else {
                                // any invalid signature lowers our chances
                                // to successfully finish current round
                                self.rejections.insert(*peer_id);
                            }
                        }
                    }
                    SignatureResponse::NoPoint => self.broadcast(peer_id), // send data immediately
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
        if self.removed_peers.is_empty() || !self.removed_peers.remove(peer_id) {
            self.bcast_current
                .push(peer_id, self.dispatcher.query(peer_id, &self.bcast_request));
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

    fn request_signature(&mut self, peer_id: &PeerId) {
        if self.removed_peers.is_empty() || !self.removed_peers.remove(peer_id) {
            self.sig_current
                .push(self.dispatcher.query(peer_id, &self.sig_request));
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

    fn match_peer_updates(&mut self, result: Result<(PeerId, PeerState), RecvError>) {
        match result {
            Ok((peer_id, new_state)) => {
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
            Err(err @ RecvError::Lagged(_)) => {
                tracing::error!(
                    parent: self.effects.span(),
                    error = display(err),
                    "peer state update"
                );
            }
            Err(err @ RecvError::Closed) => {
                let _span = self.effects.span().enter();
                panic!("peer state update {err}");
            }
        }
    }
}

struct BroadcasterContext;
impl EffectsContext for BroadcasterContext {}

impl Effects<BroadcasterContext> {
    fn new(parent: &Effects<CurrentRoundContext>, digest: &Digest) -> Self {
        Self::new_child(parent.span(), || {
            tracing::error_span!("broadcaster", digest = display(digest.alt()))
        })
    }
}
