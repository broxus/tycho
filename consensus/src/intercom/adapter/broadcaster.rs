use std::mem;
use std::sync::Arc;

use futures_util::future::BoxFuture;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use tokio::sync::broadcast::{self, error::RecvError};
use tokio::sync::{mpsc, Notify};

use tycho_network::PeerId;
use tycho_util::{FastHashMap, FastHashSet};

use crate::intercom::adapter::dto::SignerSignal;
use crate::intercom::dto::{BroadcastResponse, PeerState, SignatureResponse};
use crate::intercom::{Dispatcher, PeerSchedule};
use crate::models::{NodeCount, Point, Round, Signature};

type BcastResult = anyhow::Result<BroadcastResponse>;
type SigResult = anyhow::Result<SignatureResponse>;

pub struct Broadcaster {
    local_id: Arc<String>,
    current_round: Round,

    point_body: Vec<u8>,
    dispatcher: Dispatcher,
    bcaster_ready: Arc<Notify>,
    signer_signal: mpsc::UnboundedReceiver<SignerSignal>,
    is_signer_ready_ok: bool,

    peer_updates: broadcast::Receiver<(PeerId, PeerState)>,
    removed_peers: FastHashSet<PeerId>,
    // every connected peer should receive broadcast, but only signer's signatures are accountable
    signers: FastHashSet<PeerId>,
    signers_count: NodeCount,
    // results
    rejections: FastHashSet<PeerId>,
    signatures: FastHashMap<PeerId, Signature>,
    // TODO move generic logic out of dispatcher
    bcast_request: tycho_network::Request,
    bcast_peers: FastHashSet<PeerId>,
    bcast_futs: FuturesUnordered<BoxFuture<'static, (PeerId, BcastResult)>>,
    sig_request: tycho_network::Request,
    sig_peers: FastHashSet<PeerId>,
    sig_futs: FuturesUnordered<BoxFuture<'static, (PeerId, SigResult)>>,
}

impl Broadcaster {
    pub fn new(
        local_id: &Arc<String>,
        point: &Point,
        dispatcher: &Dispatcher,
        peer_schedule: &PeerSchedule,
        bcaster_ready: Arc<Notify>,
        signer_signal: mpsc::UnboundedReceiver<SignerSignal>,
    ) -> Self {
        let point_body = bincode::serialize(&point.body).expect("own point serializes to bytes");
        let peer_updates = peer_schedule.updates();
        let signers = peer_schedule
            .peers_for(&point.body.location.round.next())
            .iter()
            .map(|(peer_id, _)| *peer_id)
            .collect::<FastHashSet<_>>();
        let signers_count = NodeCount::new(signers.len());
        let bcast_peers = peer_schedule.all_resolved();
        tracing::info!("bcast_peers {}", bcast_peers.len());
        let bcast_request = Dispatcher::broadcast_request(&point);
        let sig_request = Dispatcher::signature_request(&point.body.location.round);
        Self {
            local_id: local_id.clone(),
            current_round: point.body.location.round,
            point_body,
            dispatcher: dispatcher.clone(),
            bcaster_ready,
            signer_signal,
            is_signer_ready_ok: false,

            peer_updates,
            signers,
            signers_count,
            removed_peers: Default::default(),
            rejections: Default::default(),
            signatures: Default::default(),

            bcast_request,
            bcast_peers,
            bcast_futs: FuturesUnordered::new(),

            sig_request,
            sig_peers: Default::default(),
            sig_futs: FuturesUnordered::new(),
        }
    }
    /// returns evidence for broadcast point
    pub async fn run(mut self) -> Result<FastHashMap<PeerId, Signature>, ()> {
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
        for peer_id in mem::take(&mut self.bcast_peers) {
            self.broadcast(&peer_id)
        }
        loop {
            tokio::select! {
                Some((peer_id, result)) = self.bcast_futs.next() => {
                    self.match_broadcast_result(peer_id, result)
                },
                Some((peer_id, result)) = self.sig_futs.next() =>  {
                    self.match_signature_result(peer_id, result)
                },
                update = self.peer_updates.recv() => {
                    self.match_peer_updates(update)
                }
                Some(signer_signal) = self.signer_signal.recv() => {
                    if let Some(result) = self.match_signer_signal(signer_signal) {
                        break result.map(|_| self.signatures)
                    }
                }
                else => {
                    panic!("bcaster unhandled");
                }
            }
        }
    }
    fn match_signer_signal(&mut self, signer_signal: SignerSignal) -> Option<Result<(), ()>> {
        tracing::info!(
            "{} @ {:?} bcaster <= signer : {signer_signal:?}; sigs {} of {}; rejects {} of {}",
            self.local_id,
            self.current_round,
            self.signatures.len(),
            self.signers_count.majority_of_others(),
            self.rejections.len(),
            self.signers_count.reliable_minority(),
        );
        match signer_signal {
            SignerSignal::Ok => {
                self.is_signer_ready_ok = true;
                None
            }
            SignerSignal::Err => {
                // even if we can return successful result, it will be discarded
                Some(Err(()))
            }
            SignerSignal::Retry => self.check_if_ready(),
        }
    }
    fn check_if_ready(&mut self) -> Option<Result<(), ()>> {
        if self.rejections.len() >= self.signers_count.reliable_minority() {
            self.bcaster_ready.notify_one();
            if self.is_signer_ready_ok {
                return Some(Err(()));
            }
        } else if self.signatures.len() >= self.signers_count.majority_of_others() {
            self.bcaster_ready.notify_one();
            if self.is_signer_ready_ok {
                return Some(Ok(()));
            }
        }
        for peer_id in mem::take(&mut self.sig_peers) {
            self.request_signature(&peer_id);
        }
        for peer_id in mem::take(&mut self.bcast_peers) {
            self.broadcast(&peer_id);
        }
        None
    }
    fn match_peer_updates(&mut self, result: Result<(PeerId, PeerState), RecvError>) {
        match result {
            Ok(update) => {
                tracing::info!(
                    "{} @ {:?} bcaster peer update: {update:?}",
                    self.local_id,
                    self.current_round
                );
                match update {
                    (_peer_id, PeerState::Added) => { /* ignore */ }
                    (peer_id, PeerState::Resolved) => self.broadcast(&peer_id),
                    (peer_id, PeerState::Removed) => _ = self.removed_peers.insert(peer_id),
                }
            }
            Err(err @ RecvError::Lagged(_)) => {
                tracing::error!("Broadcaster peer updates {err}")
            }
            Err(err @ RecvError::Closed) => {
                panic!("Broadcaster peer updates {err}")
            }
        }
    }
    fn match_broadcast_result(&mut self, peer_id: PeerId, result: BcastResult) {
        match result {
            Err(error) => {
                // TODO distinguish timeouts from models incompatibility etc
                // self.bcast_peers.push(peer_id); // let it retry
                self.sig_peers.insert(peer_id); // lighter weight retry loop
                tracing::error!(
                    "{} @ {:?} bcaster <= signer {peer_id:.4?} broadcast error : {error}",
                    self.local_id,
                    self.current_round
                );
            }
            Ok(response) => {
                if response == BroadcastResponse::Rejected {
                    tracing::warn!(
                        "{} @ {:?} bcaster <= signer {peer_id:.4?} : {response:?}",
                        self.local_id,
                        self.current_round
                    );
                } else {
                    tracing::info!(
                        "{} @ {:?} bcaster <= signer {peer_id:.4?} : {response:?}",
                        self.local_id,
                        self.current_round
                    );
                }
                match response {
                    BroadcastResponse::Accepted => self.request_signature(&peer_id),
                    BroadcastResponse::TryLater => _ = self.sig_peers.insert(peer_id),
                    BroadcastResponse::Rejected => {
                        if self.signers.contains(&peer_id) {
                            self.rejections.insert(peer_id);
                        }
                    }
                }
            }
        }
    }
    fn match_signature_result(&mut self, peer_id: PeerId, result: SigResult) {
        match result {
            Err(error) => {
                // TODO distinguish timeouts from models incompatibility etc
                self.sig_peers.insert(peer_id); // let it retry
                tracing::error!(
                    "{} @ {:?} bcaster <= signer {peer_id:.4?} signature request error : {error}",
                    self.local_id,
                    self.current_round
                );
            }
            Ok(response) => {
                if response == SignatureResponse::Rejected {
                    tracing::warn!(
                        "{} @ {:?} bcaster <= signer {peer_id:.4?} : {response:?}",
                        self.local_id,
                        self.current_round
                    );
                } else {
                    tracing::info!(
                        "{} @ {:?} bcaster <= signer {peer_id:.4?} : {response:?}",
                        self.local_id,
                        self.current_round
                    );
                };
                match response {
                    SignatureResponse::Signature(signature) => {
                        if self.signers.contains(&peer_id) {
                            if self.is_signature_ok(&peer_id, &signature) {
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
        if self.removed_peers.is_empty() || !self.removed_peers.remove(&peer_id) {
            self.bcast_futs
                .push(self.dispatcher.request(&peer_id, &self.bcast_request));
            tracing::info!(
                "{} @ {:?} bcaster => signer {peer_id:.4?}: broadcast",
                self.local_id,
                self.current_round
            );
        } else {
            tracing::warn!(
                "{} @ {:?} bcaster => signer {peer_id:.4?}: broadcast impossible",
                self.local_id,
                self.current_round
            );
        }
    }
    fn request_signature(&mut self, peer_id: &PeerId) {
        if self.removed_peers.is_empty() || !self.removed_peers.remove(&peer_id) {
            self.sig_futs
                .push(self.dispatcher.request(&peer_id, &self.sig_request));
            tracing::info!(
                "{} @ {:?} bcaster => signer {peer_id:.4?}: signature request",
                self.local_id,
                self.current_round
            );
        } else {
            tracing::warn!(
                "{} @ {:?} bcaster => signer {peer_id:.4?}: signature request impossible",
                self.local_id,
                self.current_round
            );
        }
    }
    fn is_signature_ok(&self, peer_id: &PeerId, signature: &Signature) -> bool {
        let sig_raw: Result<[u8; 64], _> = signature.0.to_vec().try_into();
        sig_raw
            .ok()
            .zip(peer_id.as_public_key())
            .map_or(false, |(sig_raw, pub_key)| {
                pub_key.verify_raw(self.point_body.as_slice(), &sig_raw)
            })
    }
}
