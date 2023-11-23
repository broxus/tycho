use std::sync::Arc;

use tokio::sync::broadcast;

use crate::connection::Connection;
use crate::types::{DisconnectReason, FastDashMap, PeerEvent, PeerId, PeerInfo};

#[derive(Clone)]
pub struct ActivePeers(Arc<ActivePeersInner>);

impl ActivePeers {
    pub fn new(channel_size: usize) -> Self {
        Self(Arc::new(ActivePeersInner::new(channel_size)))
    }

    pub fn get(&self, peer_id: &PeerId) -> Option<Connection> {
        self.0.get(peer_id)
    }
}

struct ActivePeersInner {
    connections: FastDashMap<PeerId, Connection>,
    events_tx: broadcast::Sender<PeerEvent>,
}

impl ActivePeersInner {
    fn new(channel_size: usize) -> Self {
        let (events_tx, _) = broadcast::channel(channel_size);
        Self {
            connections: Default::default(),
            events_tx,
        }
    }

    fn get(&self, peer_id: &PeerId) -> Option<Connection> {
        self.connections
            .get(peer_id)
            .map(|item| item.value().clone())
    }

    fn contains(&self, peer_id: &PeerId) -> bool {
        self.connections.contains_key(peer_id)
    }

    fn remove(&mut self, peer_id: &PeerId, reason: DisconnectReason) {
        if let Some((_, connection)) = self.connections.remove(peer_id) {
            connection.close();
            self.send_event(PeerEvent::LostPeer(*peer_id, reason));
        }
    }

    fn remove_with_stable_id(&self, peer_id: &PeerId, stable_id: usize, reason: DisconnectReason) {
        if let Some((_, connection)) = self
            .connections
            .remove_if(peer_id, |_, connection| connection.stable_id() == stable_id)
        {
            connection.close();
            self.send_event(PeerEvent::LostPeer(*peer_id, reason));
        }
    }

    fn send_event(&self, event: PeerEvent) {
        _ = self.events_tx.send(event);
    }
}

#[derive(Default)]
pub struct KnownPeers(FastDashMap<PeerId, PeerInfo>);

impl KnownPeers {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, peer_id: &PeerId) -> Option<PeerInfo> {
        self.0.get(peer_id).map(|item| item.value().clone())
    }

    pub fn insert(&self, peer_info: PeerInfo) -> Option<PeerInfo> {
        self.0.insert(peer_info.peer_id, peer_info)
    }

    pub fn remove(&self, peer_id: &PeerId) -> Option<PeerInfo> {
        self.0.remove(peer_id).map(|(_, value)| value)
    }
}
