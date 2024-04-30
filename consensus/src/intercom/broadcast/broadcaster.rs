use std::mem;
use std::sync::Arc;

use futures_util::future::BoxFuture;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use tokio::sync::broadcast::{self, error::RecvError};
use tokio::sync::mpsc;

use tycho_network::PeerId;
use tycho_util::{FastHashMap, FastHashSet};

use crate::intercom::broadcast::collector::CollectorSignal;
use crate::intercom::dto::{PeerState, SignatureResponse};
use crate::intercom::{Dispatcher, PeerSchedule};
use crate::models::{NodeCount, Point, Round, Signature};

type BcastResult = anyhow::Result<()>;
type SigResult = anyhow::Result<SignatureResponse>;

#[derive(Debug)]
pub enum BroadcasterSignal {
    Ok,
    Err,
}

pub struct Broadcaster {
    log_id: Arc<String>,
    current_round: Round,

    point_body: Vec<u8>,
    dispatcher: Dispatcher,
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
    // TODO move generic logic close to dispatcher, also check DownloadTask
    bcast_request: tycho_network::Request,
    bcast_peers: FastHashSet<PeerId>,
    bcast_futs: FuturesUnordered<BoxFuture<'static, (PeerId, BcastResult)>>,
    sig_request: tycho_network::Request,
    sig_peers: FastHashSet<PeerId>,
    sig_futs: FuturesUnordered<BoxFuture<'static, (PeerId, SigResult)>>,
}

impl Broadcaster {
    pub fn new(
        log_id: Arc<String>,
        point: &Point,
        dispatcher: &Dispatcher,
        peer_schedule: &PeerSchedule,
        bcaster_signal: mpsc::Sender<BroadcasterSignal>,
        collector_signal: mpsc::UnboundedReceiver<CollectorSignal>,
    ) -> Self {
        let point_body = bincode::serialize(&point.body).expect("own point serializes to bytes");
        let peer_updates = peer_schedule.updates();
        let signers = peer_schedule
            .peers_for(&point.body.location.round.next())
            .iter()
            .map(|(peer_id, _)| *peer_id)
            .collect::<FastHashSet<_>>();
        let signers_count = NodeCount::new(signers.len());
        let collectors = peer_schedule.all_resolved();
        tracing::debug!(
            "{log_id} @ {:?} collectors count = {}",
            point.body.location.round,
            collectors.len()
        );
        let bcast_request = Dispatcher::broadcast_request(&point);
        let sig_request = Dispatcher::signature_request(&point.body.location.round);
        Self {
            log_id,
            current_round: point.body.location.round,
            point_body,
            dispatcher: dispatcher.clone(),
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
    pub async fn run(mut self) -> FastHashMap<PeerId, Signature> {
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
                Some(collector_signal) = self.collector_signal.recv() => {
                    if self.should_finish(collector_signal).await {
                        break self.signatures
                    }
                }
                Some((peer_id, result)) = self.bcast_futs.next() => {
                    self.match_broadcast_result(peer_id, result)
                },
                Some((peer_id, result)) = self.sig_futs.next() =>  {
                    self.match_signature_result(peer_id, result)
                },
                update = self.peer_updates.recv() => {
                    self.match_peer_updates(update)
                }
                else => {
                    panic!("bcaster unhandled");
                }
            }
        }
    }

    async fn should_finish(&mut self, collector_signal: CollectorSignal) -> bool {
        tracing::debug!(
            "{} @ {:?} bcaster <= Collector::{collector_signal:?} : sigs {} of {}, rejects {} of {}",
            self.log_id,
            self.current_round,
            self.signatures.len(),
            self.signers_count.majority_of_others(),
            self.rejections.len(),
            self.signers_count.reliable_minority(),
        );
        match collector_signal {
            // though we return successful result, it will be discarded on Err
            CollectorSignal::Finish | CollectorSignal::Err => true,
            CollectorSignal::Retry => {
                if self.rejections.len() >= self.signers_count.reliable_minority() {
                    _ = self.bcaster_signal.send(BroadcasterSignal::Err).await;
                    return true;
                }
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
    }

    fn match_broadcast_result(&mut self, peer_id: PeerId, result: BcastResult) {
        match result {
            Err(error) => {
                // TODO distinguish timeouts from models incompatibility etc
                // self.bcast_peers.push(peer_id); // let it retry
                self.sig_peers.insert(peer_id); // lighter weight retry loop
                tracing::error!(
                    "{} @ {:?} bcaster <= collector {peer_id:.4?} broadcast error : {error}",
                    self.log_id,
                    self.current_round
                );
            }
            Ok(_) => {
                tracing::debug!(
                    "{} @ {:?} bcaster <= collector {peer_id:.4?} : broadcast accepted",
                    self.log_id,
                    self.current_round
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
                    "{} @ {:?} bcaster <= collector {peer_id:.4?} signature request error : {error}",
                    self.log_id,
                    self.current_round
                );
            }
            Ok(response) => {
                if response == SignatureResponse::Rejected {
                    tracing::warn!(
                        "{} @ {:?} bcaster <= collector {peer_id:.4?} : {response:.4?}",
                        self.log_id,
                        self.current_round
                    );
                } else {
                    tracing::debug!(
                        "{} @ {:?} bcaster <= collector {peer_id:.4?} : {response:.4?}",
                        self.log_id,
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
                .push(self.dispatcher.send(&peer_id, &self.bcast_request));
            tracing::debug!(
                "{} @ {:?} bcaster => collector {peer_id:.4?}: broadcast",
                self.log_id,
                self.current_round
            );
        } else {
            tracing::warn!(
                "{} @ {:?} bcaster => collector {peer_id:.4?}: broadcast impossible",
                self.log_id,
                self.current_round
            );
        }
    }

    fn request_signature(&mut self, peer_id: &PeerId) {
        if self.removed_peers.is_empty() || !self.removed_peers.remove(&peer_id) {
            self.sig_futs
                .push(self.dispatcher.query(&peer_id, &self.sig_request));
            tracing::debug!(
                "{} @ {:?} bcaster => collector {peer_id:.4?}: signature request",
                self.log_id,
                self.current_round
            );
        } else {
            tracing::warn!(
                "{} @ {:?} bcaster => collector {peer_id:.4?}: signature request impossible",
                self.log_id,
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

    fn match_peer_updates(&mut self, result: Result<(PeerId, PeerState), RecvError>) {
        match result {
            Ok((peer_id, new_state)) => {
                tracing::info!(
                    "{} @ {:?} bcaster peer update: {peer_id:?} -> {new_state:?}",
                    self.log_id,
                    self.current_round
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
                tracing::error!("Broadcaster peer updates {err}")
            }
            Err(err @ RecvError::Closed) => {
                panic!("Broadcaster peer updates {err}")
            }
        }
    }
}
