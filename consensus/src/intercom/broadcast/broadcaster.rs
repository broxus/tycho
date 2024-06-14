use std::mem;
use std::sync::Arc;

use futures_util::future::BoxFuture;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::broadcast::{self};
use tokio::sync::mpsc;
use tycho_network::PeerId;
use tycho_util::{FastHashMap, FastHashSet};

use crate::dyn_event;
use crate::effects::{AltFormat, CurrentRoundContext, Effects, EffectsContext};
use crate::intercom::broadcast::collector::CollectorSignal;
use crate::intercom::dto::{BroadcastResponse, PeerState, SignatureResponse};
use crate::intercom::{Dispatcher, PeerSchedule};
use crate::models::{Digest, NodeCount, Point, Round, Signature};

type BcastResult = anyhow::Result<BroadcastResponse>;
type SigResult = anyhow::Result<SignatureResponse>;

#[derive(Copy, Clone, Debug)]
pub enum BroadcasterSignal {
    Ok,
    Err,
}

pub struct Broadcaster {
    dispatcher: Dispatcher,
    // do not throw away unfinished broadcasts from previous round
    bcasts_outdated: FuturesUnordered<BoxFuture<'static, (PeerId, BcastResult)>>,
}

impl Broadcaster {
    pub fn new(dispatcher: &Dispatcher) -> Self {
        Self {
            dispatcher: dispatcher.clone(),
            bcasts_outdated: FuturesUnordered::new(),
        }
    }
    pub async fn run(
        &mut self,
        round_effects: &Effects<CurrentRoundContext>,
        point: &Arc<Point>,
        peer_schedule: &PeerSchedule,
        bcaster_signal: mpsc::Sender<BroadcasterSignal>,
        collector_signal: mpsc::UnboundedReceiver<CollectorSignal>,
    ) -> FastHashMap<PeerId, Signature> {
        let mut task = BroadcasterTask::new(
            Effects::<BroadcasterContext>::new(round_effects, &point.digest),
            point,
            &self.dispatcher,
            peer_schedule,
            bcaster_signal,
            collector_signal,
            mem::take(&mut self.bcasts_outdated),
        );
        task.run().await;
        // preserve only broadcasts from the last round and drop older ones as hung up
        self.bcasts_outdated.extend(task.bcast_futs);
        task.signatures
    }
}

struct BroadcasterTask {
    effects: Effects<BroadcasterContext>,
    dispatcher: Dispatcher,
    bcasts_outdated: FuturesUnordered<BoxFuture<'static, (PeerId, BcastResult)>>,
    current_round: Round,
    point_digest: Digest,
    /// Receiver may be closed (collector finished), so do not require `Ok` on send
    bcaster_signal: mpsc::Sender<BroadcasterSignal>,
    collector_signal: mpsc::UnboundedReceiver<CollectorSignal>,

    peer_updates: broadcast::Receiver<(PeerId, PeerState)>,
    removed_peers: FastHashSet<PeerId>,
    // every connected peer should receive broadcast, but only signer's signatures are accountable
    signers: FastHashSet<PeerId>,
    signers_count: NodeCount,
    // results
    rejections: FastHashSet<PeerId>,
    signatures: FastHashMap<PeerId, Signature>,

    bcast_request: tycho_network::Request,
    bcast_peers: FastHashSet<PeerId>,
    bcast_futs: FuturesUnordered<BoxFuture<'static, (PeerId, BcastResult)>>,
    sig_request: tycho_network::Request,
    sig_peers: FastHashSet<PeerId>,
    sig_futs: FuturesUnordered<BoxFuture<'static, (PeerId, SigResult)>>,
}

impl BroadcasterTask {
    fn new(
        effects: Effects<BroadcasterContext>,
        point: &Arc<Point>,
        dispatcher: &Dispatcher,
        peer_schedule: &PeerSchedule,
        bcaster_signal: mpsc::Sender<BroadcasterSignal>,
        collector_signal: mpsc::UnboundedReceiver<CollectorSignal>,
        bcasts_outdated: FuturesUnordered<BoxFuture<'static, (PeerId, BcastResult)>>,
    ) -> Self {
        let _guard = effects.span().clone().entered();
        let peer_updates = peer_schedule.updates();
        let signers = peer_schedule
            .peers_for(point.body.location.round.next())
            .iter()
            .map(|(peer_id, _)| *peer_id)
            .collect::<FastHashSet<_>>();
        let signers_count = NodeCount::new(signers.len());
        let collectors = peer_schedule.all_resolved();
        let bcast_request = Dispatcher::broadcast_request(point);
        let sig_request = Dispatcher::signature_request(point.body.location.round);
        Self {
            effects,
            dispatcher: dispatcher.clone(),
            bcasts_outdated,
            current_round: point.body.location.round,
            point_digest: point.digest.clone(),
            bcaster_signal,
            collector_signal,

            peer_updates,
            signers,
            signers_count,
            removed_peers: Default::default(),
            rejections: Default::default(),
            signatures: Default::default(),

            bcast_request,
            bcast_peers: collectors,
            bcast_futs: FuturesUnordered::new(),

            sig_request,
            sig_peers: Default::default(),
            sig_futs: FuturesUnordered::new(),
        }
    }
    /// returns evidence for broadcast point
    pub async fn run(&mut self) {
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
            collectors_count = self.bcast_peers.len(),
            "start",
        );
        for peer_id in mem::take(&mut self.bcast_peers) {
            self.broadcast(&peer_id);
        }
        loop {
            tokio::select! {
                 Some(_) = self.bcasts_outdated.next() => {} // let them complete
                Some(collector_signal) = self.collector_signal.recv() => {
                    if self.should_finish(collector_signal).await {
                        break;
                    }
                }
                Some((peer_id, result)) = self.bcast_futs.next() => {
                    self.match_broadcast_result(peer_id, result);
                },
                Some((peer_id, result)) = self.sig_futs.next() =>  {
                    self.match_signature_result(peer_id, result);
                },
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

    async fn should_finish(&mut self, collector_signal: CollectorSignal) -> bool {
        let result = match collector_signal {
            // though we return successful result, it will be discarded on Err
            CollectorSignal::Finish | CollectorSignal::Err => true,
            CollectorSignal::Retry => {
                if self.rejections.len() >= self.signers_count.reliable_minority() {
                    _ = self.bcaster_signal.send(BroadcasterSignal::Err).await;
                    true
                } else {
                    if self.signatures.len() >= self.signers_count.majority_of_others() {
                        _ = self.bcaster_signal.send(BroadcasterSignal::Ok).await;
                    }
                    for peer_id in mem::take(&mut self.sig_peers) {
                        self.request_signature(&peer_id);
                    }
                    for peer_id in mem::take(&mut self.bcast_peers) {
                        self.broadcast(&peer_id);
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

    fn match_broadcast_result(&mut self, peer_id: PeerId, result: BcastResult) {
        match result {
            Err(error) => {
                // TODO distinguish timeouts from models incompatibility etc
                // self.bcast_peers.push(peer_id); // let it retry
                self.sig_peers.insert(peer_id); // lighter weight retry loop
                tracing::error!(
                    parent: self.effects.span(),
                    peer = display(peer_id.alt()),
                    error = display(error),
                    "failed to send broadcast to"
                );
            }
            Ok(_) => {
                tracing::trace!(
                    parent: self.effects.span(),
                    peer = display(peer_id.alt()),
                    "finished broadcast to"
                );
                self.request_signature(&peer_id);
            }
        }
    }

    fn match_signature_result(&mut self, peer_id: PeerId, result: SigResult) {
        match result {
            Err(error) => {
                // TODO distinguish timeouts from models incompatibility etc
                self.sig_peers.insert(peer_id); // let it retry
                tracing::error!(
                    parent: self.effects.span(),
                    peer = display(peer_id.alt()),
                    error = display(error),
                    "failed to query signature from"
                );
            }
            Ok(response) => {
                let level = if response == SignatureResponse::Rejected {
                    tracing::Level::WARN
                } else {
                    tracing::Level::DEBUG
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
                        if self.signers.contains(&peer_id) {
                            if signature.verifies(&peer_id, &self.point_digest) {
                                self.signatures.insert(peer_id, signature);
                            } else {
                                // any invalid signature lowers our chances
                                // to successfully finish current round
                                self.rejections.insert(peer_id);
                            }
                        }
                    }
                    SignatureResponse::NoPoint => self.broadcast(&peer_id),
                    SignatureResponse::TryLater => _ = self.sig_peers.insert(peer_id),
                    SignatureResponse::Rejected => {
                        if self.signers.contains(&peer_id) {
                            self.rejections.insert(peer_id);
                        }
                    }
                }
            }
        }
    }

    fn broadcast(&mut self, peer_id: &PeerId) {
        if self.removed_peers.is_empty() || !self.removed_peers.remove(peer_id) {
            self.bcast_futs
                .push(self.dispatcher.query(peer_id, &self.bcast_request));
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
            self.sig_futs
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
