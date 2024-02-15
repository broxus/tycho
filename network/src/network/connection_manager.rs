use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use ahash::HashMap;
use anyhow::Result;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::JoinSet;
use tycho_util::{FastDashMap, FastHashMap};

use crate::network::config::NetworkConfig;
use crate::network::connection::Connection;
use crate::network::endpoint::{Connecting, Endpoint};
use crate::network::request_handler::InboundRequestHandler;
use crate::network::wire::handshake;
use crate::types::{
    Address, BoxCloneService, Direction, DisconnectReason, PeerAffinity, PeerEvent, PeerId,
    PeerInfo, Response, ServiceRequest,
};

#[derive(Debug)]
pub(crate) enum ConnectionManagerRequest {
    Connect(Address, Option<PeerId>, oneshot::Sender<Result<PeerId>>),
    Shutdown(oneshot::Sender<()>),
}

pub(crate) struct ConnectionManager {
    config: Arc<NetworkConfig>,
    endpoint: Arc<Endpoint>,

    mailbox: mpsc::Receiver<ConnectionManagerRequest>,

    pending_connections: JoinSet<ConnectingOutput>,
    connection_handlers: JoinSet<()>,

    pending_dials: FastHashMap<PeerId, oneshot::Receiver<Result<PeerId>>>,
    dial_backoff_states: HashMap<PeerId, DialBackoffState>,

    active_peers: ActivePeers,
    known_peers: KnownPeers,

    service: BoxCloneService<ServiceRequest, Response>,
}

impl Drop for ConnectionManager {
    fn drop(&mut self) {
        self.endpoint.close();
    }
}

impl ConnectionManager {
    pub fn new(
        config: Arc<NetworkConfig>,
        endpoint: Arc<Endpoint>,
        active_peers: ActivePeers,
        known_peers: KnownPeers,
        service: BoxCloneService<ServiceRequest, Response>,
    ) -> (Self, mpsc::Sender<ConnectionManagerRequest>) {
        let (mailbox_tx, mailbox) = mpsc::channel(config.connection_manager_channel_capacity);
        let connection_manager = Self {
            config,
            endpoint,
            mailbox,
            pending_connections: Default::default(),
            connection_handlers: Default::default(),
            pending_dials: Default::default(),
            dial_backoff_states: Default::default(),
            active_peers,
            known_peers,
            service,
        };
        (connection_manager, mailbox_tx)
    }

    pub async fn start(mut self) {
        tracing::info!("connection manager started");

        let jitter = Duration::from_millis(1000).mul_f64(rand::random());
        let mut interval = tokio::time::interval(self.config.connectivity_check_interval + jitter);

        let mut shutdown_notifier = None;

        loop {
            tokio::select! {
                now = interval.tick() => {
                    self.handle_connectivity_check(now.into_std());
                }
                maybe_request = self.mailbox.recv() => {
                    let Some(request) = maybe_request else {
                        break;
                    };

                    match request {
                        ConnectionManagerRequest::Connect(address, peer_id, callback) => {
                            self.handle_connect_request(address, peer_id, callback);
                        }
                        ConnectionManagerRequest::Shutdown(oneshot) => {
                            shutdown_notifier = Some(oneshot);
                            break;
                        }
                    }
                }
                connecting = self.endpoint.accept() => {
                    if let Some(connecting) = connecting {
                        self.handle_incoming(connecting);
                    }
                }
                Some(connecting_output) = self.pending_connections.join_next() => {
                    // NOTE: unwrap here is to propagate panic from the spawned future
                    self.handle_connecting_result(connecting_output.unwrap());
                }
                Some(connection_handler_output) = self.connection_handlers.join_next() => {
                    // NOTE: unwrap here is to propagate panic from the spawned future
                    connection_handler_output.unwrap();
                }
            }
        }

        self.shutdown().await;

        if let Some(tx) = shutdown_notifier {
            _ = tx.send(());
        }

        tracing::info!("connection manager stopped");
    }

    async fn shutdown(mut self) {
        self.endpoint.close();
        self.pending_connections.shutdown().await;

        while self.connection_handlers.join_next().await.is_some() {}
        assert!(self.active_peers.is_empty());

        self.endpoint
            .wait_idle(self.config.shutdown_idle_timeout)
            .await;
    }

    fn handle_connectivity_check(&mut self, now: Instant) {
        use std::collections::hash_map::Entry;

        self.pending_dials
            .retain(|peer_id, oneshot| match oneshot.try_recv() {
                Ok(Ok(returned_peer_id)) => {
                    debug_assert_eq!(peer_id, &returned_peer_id);
                    self.dial_backoff_states.remove(peer_id);
                    false
                }
                Ok(Err(_)) => {
                    match self.dial_backoff_states.entry(*peer_id) {
                        Entry::Occupied(mut entry) => entry.get_mut().update(
                            now,
                            self.config.connection_backoff,
                            self.config.max_connection_backoff,
                        ),
                        Entry::Vacant(entry) => {
                            entry.insert(DialBackoffState::new(
                                now,
                                self.config.connection_backoff,
                                self.config.max_connection_backoff,
                            ));
                        }
                    }
                    false
                }
                Err(oneshot::error::TryRecvError::Closed) => {
                    panic!("BUG: connection manager never finished dialing a peer");
                }
                Err(oneshot::error::TryRecvError::Empty) => true,
            });

        let outstanding_connections_limit = self
            .config
            .max_concurrent_outstanding_connections
            .saturating_sub(self.pending_connections.len());

        let outstanding_connections = self
            .known_peers
            .0
            .iter()
            .filter(|item| {
                let KnownPeer {
                    peer_info,
                    affinity,
                } = item.value();

                *affinity == PeerAffinity::High
                    && &peer_info.id != self.endpoint.peer_id()
                    && !self.active_peers.contains(&peer_info.id)
                    && !self.pending_dials.contains_key(&peer_info.id)
                    && self
                        .dial_backoff_states
                        .get(&peer_info.id)
                        .map_or(true, |state| now > state.next_attempt_at)
            })
            .take(outstanding_connections_limit)
            .map(|item| item.value().peer_info.clone())
            .collect::<Vec<_>>();

        for peer_info in outstanding_connections {
            // TODO: handle multiple addresses
            let address = peer_info
                .iter_addresses()
                .next()
                .cloned()
                .expect("address list must have at least one item");

            let (tx, rx) = oneshot::channel();
            self.dial_peer(address, Some(peer_info.id), tx);
            self.pending_dials.insert(peer_info.id, rx);
        }
    }

    fn handle_connect_request(
        &mut self,
        address: Address,
        peer_id: Option<PeerId>,
        callback: oneshot::Sender<Result<PeerId>>,
    ) {
        self.dial_peer(address, peer_id, callback);
    }

    fn handle_incoming(&mut self, connecting: Connecting) {
        async fn handle_incoming_task(
            connecting: Connecting,
            config: Arc<NetworkConfig>,
            active_peers: ActivePeers,
            known_peers: KnownPeers,
        ) -> ConnectingOutput {
            let fut = async {
                let connection = connecting.await?;

                match known_peers.get_affinity(connection.peer_id()) {
                    Some(PeerAffinity::High | PeerAffinity::Allowed) => {}
                    Some(PeerAffinity::Never) => {
                        anyhow::bail!(
                            "rejecting connection from peer {} due to PeerAffinity::Never",
                            connection.peer_id(),
                        );
                    }
                    _ => {
                        if let Some(limit) = config.max_concurrent_connections {
                            anyhow::ensure!(
                                active_peers.len() < limit,
                                "rejecting connection from peer {} dut too many concurrent connections",
                                connection.peer_id(),
                            );
                        }
                    }
                }

                handshake(connection).await
            };

            let connecting_result = tokio::time::timeout(config.connect_timeout, fut)
                .await
                .map_err(Into::into)
                .and_then(std::convert::identity);

            ConnectingOutput {
                connecting_result,
                callback: None,
                target_address: None,
                target_peer_id: None,
            }
        }

        tracing::trace!("received new incoming connection");

        self.pending_connections.spawn(handle_incoming_task(
            connecting,
            self.config.clone(),
            self.active_peers.clone(),
            self.known_peers.clone(),
        ));
    }

    fn handle_connecting_result(&mut self, res: ConnectingOutput) {
        match res.connecting_result {
            Ok(connection) => {
                let peer_id = *connection.peer_id();
                tracing::debug!(%peer_id, "new connection");
                self.add_peer(connection);
                if let Some(callback) = res.callback {
                    _ = callback.send(Ok(peer_id));
                }
            }
            Err(e) => {
                tracing::debug!(
                    target_address = ?res.target_address,
                    target_peer_id = ?res.target_peer_id,
                    "connection failed: {e:?}"
                );
                if let Some(callback) = res.callback {
                    _ = callback.send(Err(e));
                }
            }
        }
    }

    fn add_peer(&mut self, connection: Connection) {
        if let Some(connection) = self.active_peers.add(self.endpoint.peer_id(), connection) {
            let handler = InboundRequestHandler::new(
                self.config.clone(),
                connection,
                self.service.clone(),
                self.active_peers.clone(),
            );
            self.connection_handlers.spawn(handler.start());
        }
    }

    #[tracing::instrument(level = "trace", skip_all, fields(peer_id = ?peer_id, address = %address))]
    fn dial_peer(
        &mut self,
        address: Address,
        peer_id: Option<PeerId>,
        callback: oneshot::Sender<Result<PeerId>>,
    ) {
        async fn dial_peer_task(
            connecting: Result<Connecting>,
            address: Address,
            peer_id: Option<PeerId>,
            callback: oneshot::Sender<Result<PeerId>>,
            config: Arc<NetworkConfig>,
        ) -> ConnectingOutput {
            let fut = async {
                let connection = connecting?.await?;
                handshake(connection).await
            };

            let connecting_result = tokio::time::timeout(config.connect_timeout, fut)
                .await
                .map_err(Into::into)
                .and_then(std::convert::identity);

            ConnectingOutput {
                connecting_result,
                callback: Some(callback),
                target_address: Some(address),
                target_peer_id: peer_id,
            }
        }

        let target_address = address.clone();
        let connecting = match peer_id {
            None => self.endpoint.connect(address),
            Some(peer_id) => self.endpoint.connect_with_expected_id(address, peer_id),
        };
        self.pending_connections.spawn(dial_peer_task(
            connecting,
            target_address,
            peer_id,
            callback,
            self.config.clone(),
        ));
    }
}

struct ConnectingOutput {
    connecting_result: Result<Connection>,
    callback: Option<oneshot::Sender<Result<PeerId>>>,
    target_address: Option<Address>,
    target_peer_id: Option<PeerId>,
}

#[derive(Debug)]
struct DialBackoffState {
    next_attempt_at: Instant,
    attempts: usize,
}

impl DialBackoffState {
    fn new(now: Instant, step: Duration, max: Duration) -> Self {
        let mut state = Self {
            next_attempt_at: now,
            attempts: 0,
        };
        state.update(now, step, max);
        state
    }

    fn update(&mut self, now: Instant, step: Duration, max: Duration) {
        self.attempts += 1;
        self.next_attempt_at = now
            + std::cmp::min(
                max,
                step.saturating_mul(self.attempts.try_into().unwrap_or(u32::MAX)),
            );
    }
}

#[derive(Clone)]
pub struct ActivePeers(Arc<ActivePeersInner>);

impl ActivePeers {
    pub fn new(channel_size: usize) -> Self {
        Self(Arc::new(ActivePeersInner::new(channel_size)))
    }

    pub fn get(&self, peer_id: &PeerId) -> Option<Connection> {
        self.0.get(peer_id)
    }

    pub fn contains(&self, peer_id: &PeerId) -> bool {
        self.0.contains(peer_id)
    }

    pub fn add(&self, local_id: &PeerId, new_connection: Connection) -> Option<Connection> {
        self.0.add(local_id, new_connection)
    }

    pub fn remove(&self, peer_id: &PeerId, reason: DisconnectReason) {
        self.0.remove(peer_id, reason);
    }

    pub fn remove_with_stable_id(
        &self,
        peer_id: &PeerId,
        stable_id: usize,
        reason: DisconnectReason,
    ) {
        self.0.remove_with_stable_id(peer_id, stable_id, reason);
    }

    pub fn subscribe(&self) -> broadcast::Receiver<PeerEvent> {
        self.0.subscribe()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn downgrade(this: &Self) -> WeakActivePeers {
        WeakActivePeers(Arc::downgrade(&this.0))
    }
}

#[derive(Clone)]
pub struct WeakActivePeers(Weak<ActivePeersInner>);

impl WeakActivePeers {
    pub fn upgrade(&self) -> Option<ActivePeers> {
        self.0.upgrade().map(ActivePeers)
    }
}

struct ActivePeersInner {
    connections: FastDashMap<PeerId, Connection>,
    connections_len: AtomicUsize,
    events_tx: broadcast::Sender<PeerEvent>,
}

impl ActivePeersInner {
    fn new(channel_size: usize) -> Self {
        let (events_tx, _) = broadcast::channel(channel_size);
        Self {
            connections: Default::default(),
            connections_len: Default::default(),
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

    #[must_use]
    fn add(&self, local_id: &PeerId, new_connection: Connection) -> Option<Connection> {
        use dashmap::mapref::entry::Entry;

        let remote_id = new_connection.peer_id();
        match self.connections.entry(*remote_id) {
            Entry::Occupied(mut entry) => {
                if simultaneous_dial_tie_breaking(
                    local_id,
                    remote_id,
                    entry.get().origin(),
                    new_connection.origin(),
                ) {
                    tracing::debug!(%remote_id, "closing old connection to mitigate simultaneous dial");
                    let old_connection = entry.insert(new_connection.clone());
                    old_connection.close();
                    self.send_event(PeerEvent::LostPeer(*remote_id, DisconnectReason::Requested));
                } else {
                    tracing::debug!(%remote_id, "closing new connection to mitigate simultaneous dial");
                    new_connection.close();
                    return None;
                }
            }
            Entry::Vacant(entry) => {
                self.connections_len.fetch_add(1, Ordering::Release);
                entry.insert(new_connection.clone());
            }
        }

        self.send_event(PeerEvent::NewPeer(*remote_id));
        Some(new_connection)
    }

    fn remove(&self, peer_id: &PeerId, reason: DisconnectReason) {
        if let Some((_, connection)) = self.connections.remove(peer_id) {
            connection.close();
            self.connections_len.fetch_sub(1, Ordering::Release);
            self.send_event(PeerEvent::LostPeer(*peer_id, reason));
        }
    }

    fn remove_with_stable_id(&self, peer_id: &PeerId, stable_id: usize, reason: DisconnectReason) {
        if let Some((_, connection)) = self
            .connections
            .remove_if(peer_id, |_, connection| connection.stable_id() == stable_id)
        {
            connection.close();
            self.connections_len.fetch_sub(1, Ordering::Release);
            self.send_event(PeerEvent::LostPeer(*peer_id, reason));
        }
    }

    fn subscribe(&self) -> broadcast::Receiver<PeerEvent> {
        self.events_tx.subscribe()
    }

    fn send_event(&self, event: PeerEvent) {
        _ = self.events_tx.send(event);
    }

    fn is_empty(&self) -> bool {
        self.connections.is_empty()
    }

    fn len(&self) -> usize {
        self.connections_len.load(Ordering::Acquire)
    }
}

fn simultaneous_dial_tie_breaking(
    local_id: &PeerId,
    remote_id: &PeerId,
    old_origin: Direction,
    new_origin: Direction,
) -> bool {
    match (old_origin, new_origin) {
        (Direction::Inbound, Direction::Inbound) | (Direction::Outbound, Direction::Outbound) => {
            true
        }
        (Direction::Inbound, Direction::Outbound) => remote_id < local_id,
        (Direction::Outbound, Direction::Inbound) => local_id < remote_id,
    }
}

#[derive(Default, Clone)]
pub struct KnownPeers(Arc<FastDashMap<PeerId, KnownPeer>>);

impl KnownPeers {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn contains(&self, peer_id: &PeerId) -> bool {
        self.0.contains_key(peer_id)
    }

    pub fn get(&self, peer_id: &PeerId) -> Option<KnownPeer> {
        self.0.get(peer_id).map(|item| item.value().clone())
    }

    pub fn get_affinity(&self, peer_id: &PeerId) -> Option<PeerAffinity> {
        self.0.get(peer_id).map(|item| item.value().affinity)
    }

    pub fn insert(&self, peer_info: Arc<PeerInfo>, affinity: PeerAffinity) -> Option<KnownPeer> {
        self.0.insert(
            peer_info.id,
            KnownPeer {
                peer_info,
                affinity,
            },
        )
    }

    pub fn remove(&self, peer_id: &PeerId) -> Option<KnownPeer> {
        self.0.remove(peer_id).map(|(_, value)| value)
    }
}

#[derive(Debug, Clone)]
pub struct KnownPeer {
    pub peer_info: Arc<PeerInfo>,
    pub affinity: PeerAffinity,
}
