use std::collections::{hash_map, VecDeque};
use std::mem::ManuallyDrop;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use anyhow::Result;
use arc_swap::{ArcSwap, AsRaw};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::{AbortHandle, JoinSet};
use tokio_util::time::{delay_queue, DelayQueue};
use tycho_util::{FastDashMap, FastHashMap};

use crate::network::config::NetworkConfig;
use crate::network::connection::Connection;
use crate::network::endpoint::{Connecting, Endpoint, Into0RttResult};
use crate::network::request_handler::InboundRequestHandler;
use crate::network::wire::handshake;
use crate::types::{
    Address, BoxCloneService, Direction, DisconnectReason, PeerAffinity, PeerEvent, PeerId,
    PeerInfo, Response, ServiceRequest,
};

// Histograms
const METRIC_CONNECTION_OUT_TIME: &str = "tycho_net_conn_out_time";
const METRIC_CONNECTION_IN_TIME: &str = "tycho_net_conn_in_time";

// Counters
const METRIC_CONNECTIONS_OUT_TOTAL: &str = "tycho_net_conn_out_total";
const METRIC_CONNECTIONS_IN_TOTAL: &str = "tycho_net_conn_in_total";
const METRIC_CONNECTIONS_OUT_FAIL_TOTAL: &str = "tycho_net_conn_out_fail_total";
const METRIC_CONNECTIONS_IN_FAIL_TOTAL: &str = "tycho_net_conn_in_fail_total";

// Gauges
const METRIC_CONNECTIONS_ACTIVE: &str = "tycho_net_conn_active";
const METRIC_CONNECTIONS_PENDING: &str = "tycho_net_conn_pending";
const METRIC_CONNECTIONS_PARTIAL: &str = "tycho_net_conn_partial";
const METRIC_CONNECTIONS_PENDING_DIALS: &str = "tycho_net_conn_pending_dials";

const METRIC_ACTIVE_PEERS: &str = "tycho_net_active_peers";
const METRIC_KNOWN_PEERS: &str = "tycho_net_known_peers";

#[derive(Debug)]
pub(crate) enum ConnectionManagerRequest {
    Connect(Address, PeerId, CallbackTx),
    Shutdown(oneshot::Sender<()>),
}

pub(crate) struct ConnectionManager {
    config: Arc<NetworkConfig>,
    endpoint: Arc<Endpoint>,

    mailbox: mpsc::Receiver<ConnectionManagerRequest>,

    pending_connection_callbacks: FastHashMap<Address, PendingConnectionCallbacks>,
    pending_partial_connections: JoinSet<Option<PartialConnection>>,
    pending_connections: JoinSet<ConnectingOutput>,
    connection_handlers: JoinSet<()>,
    delayed_callbacks: DelayedCallbacksQueue,

    pending_dials: FastHashMap<PeerId, CallbackRx>,
    dial_backoff_states: FastHashMap<PeerId, DialBackoffState>,

    active_peers: ActivePeers,
    known_peers: KnownPeers,

    service: BoxCloneService<ServiceRequest, Response>,
}

type CallbackTx = oneshot::Sender<Result<PeerId, Arc<anyhow::Error>>>;
type CallbackRx = oneshot::Receiver<Result<PeerId, Arc<anyhow::Error>>>;

impl Drop for ConnectionManager {
    fn drop(&mut self) {
        tracing::trace!("dropping connection manager");
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
            pending_connection_callbacks: Default::default(),
            pending_partial_connections: Default::default(),
            pending_connections: Default::default(),
            connection_handlers: Default::default(),
            delayed_callbacks: Default::default(),
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
                            self.handle_connect_request(address, &peer_id, callback);
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
                    metrics::gauge!(METRIC_CONNECTIONS_PENDING).decrement(1);
                    match connecting_output {
                        Ok(connecting) => self.handle_connecting_result(connecting),
                        Err(e) => {
                            if e.is_panic() {
                                std::panic::resume_unwind(e.into_panic());
                            }
                        }
                    }
                }
                Some(partial_connection) = self.pending_partial_connections.join_next() => {
                    metrics::gauge!(METRIC_CONNECTIONS_PARTIAL).decrement(1);
                    // NOTE: unwrap here is to propagate panic from the spawned future
                    if let Some(PartialConnection { connection, timeout_at }) = partial_connection.unwrap() {
                        self.handle_incoming_impl(connection, None, timeout_at);
                    }
                }
                Some(connection_handler_output) = self.connection_handlers.join_next() => {
                    metrics::gauge!(METRIC_CONNECTIONS_ACTIVE).decrement(1);

                    // NOTE: unwrap here is to propagate panic from the spawned future
                    if let Err(e) = connection_handler_output {
                        if e.is_panic() {
                            std::panic::resume_unwind(e.into_panic());
                        }
                    }
                }
                Some(peer_id) = self.delayed_callbacks.wait_for_next_expired() => {
                    self.delayed_callbacks.execute_expired(&peer_id);
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
        tracing::trace!("shutting down connection manager");

        self.endpoint.close();

        self.pending_partial_connections.shutdown().await;
        metrics::gauge!(METRIC_CONNECTIONS_PARTIAL).set(0);

        self.pending_connections.shutdown().await;
        metrics::gauge!(METRIC_CONNECTIONS_PENDING).set(0);

        while self.connection_handlers.join_next().await.is_some() {
            metrics::gauge!(METRIC_CONNECTIONS_ACTIVE).decrement(1);
        }
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
            .filter_map(|item| {
                let value = match item.value() {
                    KnownPeerState::Stored(item) => item.upgrade()?,
                    KnownPeerState::Banned => return None,
                };
                let peer_info = value.peer_info.load();
                let affinity = value.compute_affinity();

                (affinity == PeerAffinity::High
                    && peer_info.id != self.endpoint.peer_id()
                    && !self.active_peers.contains(&peer_info.id)
                    && !self.pending_dials.contains_key(&peer_info.id)
                    && self
                        .dial_backoff_states
                        .get(&peer_info.id)
                        .map_or(true, |state| now > state.next_attempt_at))
                .then(|| arc_swap::Guard::into_inner(peer_info))
            })
            .take(outstanding_connections_limit)
            .collect::<Vec<_>>();

        for peer_info in outstanding_connections {
            // TODO: handle multiple addresses
            let address = peer_info
                .iter_addresses()
                .next()
                .cloned()
                .expect("address list must have at least one item");

            let (tx, rx) = oneshot::channel();
            self.dial_peer(address, &peer_info.id, tx);
            self.pending_dials.insert(peer_info.id, rx);
        }

        metrics::gauge!(METRIC_CONNECTIONS_PENDING_DIALS).set(self.pending_dials.len() as f64);
    }

    fn handle_connect_request(&mut self, address: Address, peer_id: &PeerId, callback: CallbackTx) {
        self.dial_peer(address, peer_id, callback);
    }

    fn handle_incoming(&mut self, connecting: Connecting) {
        let remote_addr = connecting.remote_address();
        tracing::trace!(
            local_id = %self.endpoint.peer_id(),
            %remote_addr,
            "received an incoming connection",
        );

        // Split incoming connection into 0.5-RTT and 1-RTT parts.
        match connecting.into_0rtt() {
            Into0RttResult::Established(connection, accepted) => {
                let timeout_at = Instant::now() + self.config.connect_timeout;
                self.handle_incoming_impl(connection, Some(accepted), timeout_at);
            }
            Into0RttResult::WithoutIdentity(partial_connection) => {
                tracing::trace!("connection identity is not available yet");

                let timeout_at = Instant::now() + self.config.connect_timeout;
                self.pending_partial_connections.spawn(async move {
                    match tokio::time::timeout_at(timeout_at.into(), partial_connection).await {
                        Ok(Ok(connection)) => Some(PartialConnection {
                            connection,
                            timeout_at,
                        }),
                        Ok(Err(e)) => {
                            tracing::trace!(
                                %remote_addr,
                                "failed to establish an incoming connection: {e}",
                            );
                            None
                        }
                        Err(_) => {
                            tracing::trace!(
                                %remote_addr,
                                "incoming connection timed out",
                            );
                            None
                        }
                    }
                });
                metrics::gauge!(METRIC_CONNECTIONS_PARTIAL).increment(1);
            }
            Into0RttResult::InvalidConnection(e) => {
                // TODO: Lower log level to trace/debug?
                tracing::warn!(%remote_addr, "invalid incoming connection: {e}");
            }
            Into0RttResult::Unavailable(_) => unreachable!(
                "BUG: For incoming connections, a 0.5-RTT connection must \
                always be successfully constructed."
            ),
        };
    }

    fn handle_incoming_impl(
        &mut self,
        connection: Connection,
        accepted: Option<quinn::ZeroRttAccepted>,
        timeout_at: Instant,
    ) {
        async fn handle_incoming_task(
            seqno: u32,
            connection: ConnectionClosedOnDrop,
            accepted: Option<quinn::ZeroRttAccepted>,
            timeout_at: Instant,
        ) -> ConnectingOutput {
            let target_peer_id = *connection.peer_id();
            let target_address = connection.remote_address().into();
            let fut = async {
                if let Some(accepted) = accepted {
                    // NOTE: `bool` output of this future is meaningless for servers.
                    accepted.await;
                }
                handshake(&connection).await
            };

            let started_at = Instant::now();

            let connecting_result = tokio::time::timeout_at(timeout_at.into(), fut)
                .await
                .map_err(Into::into)
                .and_then(std::convert::identity)
                .map_err(Arc::new)
                .map(|_| connection.disarm());

            metrics::histogram!(METRIC_CONNECTION_IN_TIME).record(started_at.elapsed());

            ConnectingOutput {
                seqno,
                drop_result: true,
                connecting_result: ManuallyDrop::new(connecting_result),
                target_address,
                target_peer_id,
                origin: Direction::Inbound,
            }
        }

        let remote_addr = connection.remote_address();

        // Check if the peer is allowed before doing anything else.
        match self.known_peers.get_affinity(connection.peer_id()) {
            Some(PeerAffinity::High | PeerAffinity::Allowed) => {}
            Some(PeerAffinity::Never) => {
                // TODO: Lower log level to trace/debug?
                tracing::warn!(
                    %remote_addr,
                    peer_id = %connection.peer_id(),
                    "rejecting connection due to PeerAffinity::Never",
                );
                connection.close();
                return;
            }
            _ => {
                if matches!(
                    self.config.max_concurrent_connections,
                    Some(limit) if self.active_peers.len() >= limit
                ) {
                    // TODO: Lower log level to trace/debug?
                    tracing::warn!(
                        %remote_addr,
                        peer_id = %connection.peer_id(),
                        "rejecting connection due too many concurrent connections",
                    );
                    connection.close();
                    return;
                }
            }
        }

        let entry = match self.pending_connection_callbacks.entry(remote_addr.into()) {
            hash_map::Entry::Vacant(entry) => Some(entry.insert(PendingConnectionCallbacks {
                last_seqno: 0,
                origin: Direction::Inbound,
                callbacks: Default::default(),
                abort_handle: None,
            })),
            hash_map::Entry::Occupied(entry) => {
                let entry = entry.into_mut();

                // Check if the incoming connection is a simultaneous dial.
                if simultaneous_dial_tie_breaking(
                    self.endpoint.peer_id(),
                    connection.peer_id(),
                    entry.origin,
                    Direction::Inbound,
                ) {
                    // New connection wins the tie, abort the old one and spawn a new task.
                    tracing::debug!(
                        %remote_addr,
                        peer_id = %connection.peer_id(),
                        "cancelling old connection to mitigate simultaneous dial",
                    );

                    entry.origin = Direction::Inbound;
                    entry.last_seqno += 1;
                    if let Some(handle) = entry.abort_handle.take() {
                        handle.abort();
                    }
                    Some(entry)
                } else {
                    // Old connection wins the tie, gracefully close the new one.
                    tracing::debug!(
                        %remote_addr,
                        peer_id = %connection.peer_id(),
                        "cancelling new connection to mitigate simultaneous dial",
                    );

                    connection.close();
                    None
                }
            }
        };

        if let Some(entry) = entry {
            entry.abort_handle = Some(self.pending_connections.spawn(handle_incoming_task(
                entry.last_seqno,
                ConnectionClosedOnDrop::new(connection),
                accepted,
                timeout_at,
            )));
            metrics::gauge!(METRIC_CONNECTIONS_PENDING).increment(1);
        }
    }

    fn handle_connecting_result(&mut self, mut res: ConnectingOutput) {
        // Check seqno first to drop outdated results.
        {
            let Some(entry) = self.pending_connection_callbacks.get(&res.target_address) else {
                tracing::trace!("connection task reordering detected");
                return;
            };

            if entry.last_seqno != res.seqno {
                tracing::debug!(
                    local_id = %self.endpoint.peer_id(),
                    peer_id = %res.target_peer_id,
                    remote_addr = %res.target_address,
                    "connection result is outdated"
                );
                return;
            }
        }

        let callbacks = self
            .pending_connection_callbacks
            .remove(&res.target_address)
            .expect("Connection tasks must be tracked")
            .callbacks;

        res.drop_result = false;
        // SAFETY: `drop_result` is set to `false`.
        match unsafe { ManuallyDrop::take(&mut res.connecting_result) } {
            Ok(connection) => {
                let peer_id = *connection.peer_id();
                tracing::debug!(
                    local_id = %self.endpoint.peer_id(),
                    %peer_id,
                    remote_addr = %res.target_address,
                    "new connection",
                );
                self.add_peer(connection);

                self.delayed_callbacks.execute_resolved(&res.target_peer_id);

                for callback in callbacks {
                    _ = callback.send(Ok(peer_id));
                }
            }
            Err(e) => {
                tracing::debug!(
                    local_id = %self.endpoint.peer_id(),
                    peer_id = %res.target_peer_id,
                    remote_addr = %res.target_address,
                    "connection failed: {e}"
                );

                metrics::counter!(match res.origin {
                    Direction::Outbound => METRIC_CONNECTIONS_OUT_FAIL_TOTAL,
                    Direction::Inbound => METRIC_CONNECTIONS_IN_FAIL_TOTAL,
                })
                .increment(1);

                // Delay sending the error to callbacks as the target peer might be
                // in the process of connecting to us.
                if let Some(quinn::ConnectionError::ApplicationClosed(closed)) = e.downcast_ref() {
                    if closed.error_code.into_inner() == 0 && !callbacks.is_empty() {
                        self.delayed_callbacks.push(
                            &res.target_peer_id,
                            e,
                            callbacks,
                            &self.config.connection_error_delay,
                        );
                        return;
                    }
                }

                for callback in callbacks {
                    _ = callback.send(Err(e.clone()));
                }
            }
        }
    }

    fn add_peer(&mut self, connection: Connection) {
        if let Some(connection) = self.active_peers.add(self.endpoint.peer_id(), connection) {
            let origin = connection.origin();

            let handler = InboundRequestHandler::new(
                self.config.clone(),
                connection,
                self.service.clone(),
                self.active_peers.clone(),
            );

            metrics::counter!(match origin {
                Direction::Outbound => METRIC_CONNECTIONS_OUT_TOTAL,
                Direction::Inbound => METRIC_CONNECTIONS_IN_TOTAL,
            })
            .increment(1);

            metrics::gauge!(METRIC_CONNECTIONS_ACTIVE).increment(1);
            self.connection_handlers.spawn(handler.start());
        }
    }

    #[tracing::instrument(
        level = "trace",
        skip_all,
        fields(
            local_id = %self.endpoint.peer_id(),
            peer_id = %peer_id,
            remote_addr = %address,
        ),
    )]
    fn dial_peer(&mut self, address: Address, peer_id: &PeerId, callback: CallbackTx) {
        async fn dial_peer_task(
            seqno: u32,
            endpoint: Arc<Endpoint>,
            address: Address,
            peer_id: PeerId,
            config: Arc<NetworkConfig>,
        ) -> ConnectingOutput {
            let fut = async {
                let address = address.resolve().await?;
                let connecting = endpoint.connect_with_expected_id(&address, &peer_id)?;
                let connection = ConnectionClosedOnDrop::new(connecting.await?);
                handshake(&connection).await?;
                Ok(connection)
            };

            let started_at = Instant::now();

            let connecting_result = tokio::time::timeout(config.connect_timeout, fut)
                .await
                .map_err(Into::into)
                .and_then(std::convert::identity)
                .map_err(Arc::new)
                .map(ConnectionClosedOnDrop::disarm);

            metrics::histogram!(METRIC_CONNECTION_OUT_TIME).record(started_at.elapsed());

            ConnectingOutput {
                seqno,
                drop_result: true,
                connecting_result: ManuallyDrop::new(connecting_result),
                target_address: address,
                target_peer_id: peer_id,
                origin: Direction::Outbound,
            }
        }

        if self.active_peers.contains(peer_id) {
            tracing::debug!("peer is already connected");
            _ = callback.send(Ok(*peer_id));
            return;
        }

        tracing::trace!("connecting to peer");

        let entry = match self.pending_connection_callbacks.entry(address.clone()) {
            hash_map::Entry::Vacant(entry) => Some(entry.insert(PendingConnectionCallbacks {
                last_seqno: 0,
                origin: Direction::Outbound,
                callbacks: vec![callback],
                abort_handle: None,
            })),
            hash_map::Entry::Occupied(entry) => {
                let entry = entry.into_mut();

                // Add the callback to the existing entry.
                entry.callbacks.push(callback);

                // Check if the outgoing connection is a simultaneous dial.
                let break_tie = simultaneous_dial_tie_breaking(
                    self.endpoint.peer_id(),
                    peer_id,
                    entry.origin,
                    Direction::Outbound,
                );

                if break_tie && entry.origin != Direction::Outbound {
                    // New connection wins the tie, abort the old one and spawn a new task.
                    tracing::debug!("cancelling old connection to mitigate simultaneous dial");

                    entry.origin = Direction::Outbound;
                    entry.last_seqno += 1;
                    if let Some(handle) = entry.abort_handle.take() {
                        handle.abort();
                    }
                    Some(entry)
                } else {
                    // Old connection wins the tie, don't create a new one
                    tracing::trace!("reusing old connection to mitigate simultaneous dial");
                    None
                }
            }
        };

        if let Some(entry) = entry {
            entry.abort_handle = Some(self.pending_connections.spawn(dial_peer_task(
                entry.last_seqno,
                self.endpoint.clone(),
                address.clone(),
                *peer_id,
                self.config.clone(),
            )));
            metrics::gauge!(METRIC_CONNECTIONS_PENDING).increment(1);
        }
    }
}

struct PendingConnectionCallbacks {
    last_seqno: u32,
    origin: Direction,
    callbacks: Vec<CallbackTx>,
    abort_handle: Option<AbortHandle>,
}

struct PartialConnection {
    connection: Connection,
    timeout_at: Instant,
}

struct ConnectingOutput {
    seqno: u32,
    drop_result: bool,
    connecting_result: ManuallyDrop<Result<Connection, Arc<anyhow::Error>>>,
    target_address: Address,
    target_peer_id: PeerId,
    origin: Direction,
}

impl Drop for ConnectingOutput {
    fn drop(&mut self) {
        if self.drop_result {
            // SAFETY: `drop_result` is set to `true` only when the result is not used.
            unsafe { ManuallyDrop::drop(&mut self.connecting_result) };
        }
    }
}

struct ConnectionClosedOnDrop {
    connection: ManuallyDrop<Connection>,
    close_on_drop: bool,
}

impl ConnectionClosedOnDrop {
    fn new(connection: Connection) -> Self {
        Self {
            connection: ManuallyDrop::new(connection),
            close_on_drop: true,
        }
    }

    fn disarm(mut self) -> Connection {
        self.close_on_drop = false;
        // SAFETY: `drop` will not be called.
        unsafe { ManuallyDrop::take(&mut self.connection) }
    }
}

impl std::ops::Deref for ConnectionClosedOnDrop {
    type Target = Connection;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.connection
    }
}

impl Drop for ConnectionClosedOnDrop {
    fn drop(&mut self) {
        if self.close_on_drop {
            // SAFETY: `disarm` was not called.
            let connection = unsafe { ManuallyDrop::take(&mut self.connection) };
            connection.close();
        }
    }
}

#[derive(Default)]
struct DelayedCallbacksQueue {
    callbacks: FastHashMap<PeerId, VecDeque<DelayedCallbacks>>,
    expirations: DelayQueue<PeerId>,
}

impl DelayedCallbacksQueue {
    fn push(
        &mut self,
        peer_id: &PeerId,
        error: Arc<anyhow::Error>,
        callbacks: Vec<CallbackTx>,
        delay: &Duration,
    ) {
        tracing::debug!(%peer_id, %error, "delayed connection error");

        let expires_at = Instant::now() + *delay;
        let delay_key = self.expirations.insert_at(*peer_id, expires_at.into());

        let items = self.callbacks.entry(*peer_id).or_default();
        items.push_back(DelayedCallbacks {
            delay_key,
            error,
            callbacks,
            expires_at,
        });
    }

    async fn wait_for_next_expired(&mut self) -> Option<PeerId> {
        let res = futures_util::future::poll_fn(|cx| self.expirations.poll_expired(cx)).await?;
        Some(res.into_inner())
    }

    fn execute_resolved(&mut self, peer_id: &PeerId) {
        let Some(items) = self.callbacks.remove(peer_id) else {
            return;
        };

        let mut batches_executed = 0;
        let mut callbacks_executed = 0;

        for delayed in items {
            batches_executed += 1;
            callbacks_executed += delayed.callbacks.len();

            let key = delayed.execute_with_ok(peer_id);

            // NOTE: Delay key must exist in the queue.
            self.expirations.remove(&key);
        }

        tracing::debug!(
            %peer_id,
            batches_executed,
            callbacks_executed,
            "executed all delayed callbacks",
        );
    }

    fn execute_expired(&mut self, peer_id: &PeerId) {
        let now = Instant::now();

        let mut batches_executed = 0;
        let mut callbacks_executed = 0;

        'outer: {
            if let Some(items) = self.callbacks.get_mut(peer_id) {
                while let Some(front) = items.front() {
                    if !front.is_expired(&now) {
                        break 'outer;
                    }

                    if let Some(delayed) = items.pop_front() {
                        batches_executed += 1;
                        callbacks_executed += delayed.callbacks.len();

                        let key = delayed.execute_with_error();

                        // NOTE: Might not be necessary since items are stored in order,
                        // but it's better to be safe.
                        self.expirations.try_remove(&key);
                    }
                }
            }

            // There is no need to hold an empty queue for this peer
            self.callbacks.remove(peer_id);
        }

        tracing::debug!(
            %peer_id,
            batches_executed,
            callbacks_executed,
            "executed expired delayed callbacks"
        );
    }
}

struct DelayedCallbacks {
    delay_key: delay_queue::Key,
    error: Arc<anyhow::Error>,
    callbacks: Vec<CallbackTx>,
    expires_at: Instant,
}

impl DelayedCallbacks {
    fn execute_with_ok(self, peer_id: &PeerId) -> delay_queue::Key {
        for callback in self.callbacks {
            _ = callback.send(Ok(*peer_id));
        }
        self.delay_key
    }

    fn execute_with_error(self) -> delay_queue::Key {
        for callback in self.callbacks {
            _ = callback.send(Err(self.error.clone()));
        }
        self.delay_key
    }

    fn is_expired(&self, now: &Instant) -> bool {
        *now >= self.expires_at
    }
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

        let mut added = false;

        let peer_id = new_connection.peer_id();
        match self.connections.entry(*peer_id) {
            Entry::Occupied(mut entry) => {
                if simultaneous_dial_tie_breaking(
                    local_id,
                    peer_id,
                    entry.get().origin(),
                    new_connection.origin(),
                ) {
                    tracing::debug!(%peer_id, "closing old connection to mitigate simultaneous dial");
                    let old_connection = entry.insert(new_connection.clone());
                    old_connection.close();
                    self.send_event(PeerEvent::LostPeer(*peer_id, DisconnectReason::Requested));
                } else {
                    tracing::debug!(%peer_id, "closing new connection to mitigate simultaneous dial");
                    new_connection.close();
                    return None;
                }
            }
            Entry::Vacant(entry) => {
                self.connections_len.fetch_add(1, Ordering::Release);
                entry.insert(new_connection.clone());
                added = true;
            }
        }

        self.send_event(PeerEvent::NewPeer(*peer_id));

        if added {
            metrics::gauge!(METRIC_ACTIVE_PEERS).increment(1);
        }
        Some(new_connection)
    }

    fn remove(&self, peer_id: &PeerId, reason: DisconnectReason) {
        if let Some((_, connection)) = self.connections.remove(peer_id) {
            connection.close();
            self.connections_len.fetch_sub(1, Ordering::Release);
            self.send_event(PeerEvent::LostPeer(*peer_id, reason));

            metrics::gauge!(METRIC_ACTIVE_PEERS).decrement(1);
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

            metrics::gauge!(METRIC_ACTIVE_PEERS).decrement(1);
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
    peer_id: &PeerId,
    old_origin: Direction,
    new_origin: Direction,
) -> bool {
    match (old_origin, new_origin) {
        (Direction::Inbound, Direction::Inbound) | (Direction::Outbound, Direction::Outbound) => {
            true
        }
        (Direction::Inbound, Direction::Outbound) => peer_id < local_id,
        (Direction::Outbound, Direction::Inbound) => local_id < peer_id,
    }
}

#[derive(Default, Clone)]
#[repr(transparent)]
pub struct KnownPeers(Arc<FastDashMap<PeerId, KnownPeerState>>);

impl KnownPeers {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn contains(&self, peer_id: &PeerId) -> bool {
        self.0.contains_key(peer_id)
    }

    pub fn is_banned(&self, peer_id: &PeerId) -> bool {
        self.0
            .get(peer_id)
            .and_then(|item| {
                Some(match item.value() {
                    KnownPeerState::Stored(item) => item.upgrade()?.is_banned(),
                    KnownPeerState::Banned => true,
                })
            })
            .unwrap_or_default()
    }

    pub fn get(&self, peer_id: &PeerId) -> Option<Arc<PeerInfo>> {
        self.0.get(peer_id).and_then(|item| match item.value() {
            KnownPeerState::Stored(item) => {
                let inner = item.upgrade()?;
                Some(inner.peer_info.load_full())
            }
            KnownPeerState::Banned => None,
        })
    }

    pub fn get_affinity(&self, peer_id: &PeerId) -> Option<PeerAffinity> {
        self.0
            .get(peer_id)
            .and_then(|item| item.value().compute_affinity())
    }

    pub fn remove(&self, peer_id: &PeerId) {
        self.0.remove(peer_id);
        metrics::gauge!(METRIC_KNOWN_PEERS).decrement(1);
    }

    pub fn ban(&self, peer_id: &PeerId) {
        let mut added = false;
        match self.0.entry(*peer_id) {
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(KnownPeerState::Banned);
                added = true;
            }
            dashmap::mapref::entry::Entry::Occupied(mut entry) => match entry.get_mut() {
                KnownPeerState::Banned => {}
                KnownPeerState::Stored(item) => match item.upgrade() {
                    Some(item) => item.affinity.store(AFFINITY_BANNED, Ordering::Release),
                    None => *entry.get_mut() = KnownPeerState::Banned,
                },
            },
        }

        if added {
            // NOTE: "New" banned peer is a "new" known peer.
            metrics::gauge!(METRIC_KNOWN_PEERS).increment(1);
        }
    }

    pub fn make_handle(&self, peer_id: &PeerId, with_affinity: bool) -> Option<KnownPeerHandle> {
        let inner = match self.0.get(peer_id)?.value() {
            KnownPeerState::Stored(item) => {
                let inner = item.upgrade()?;
                if with_affinity && !inner.increase_affinity() {
                    return None;
                }
                inner
            }
            KnownPeerState::Banned => return None,
        };

        Some(KnownPeerHandle::from_inner(inner, with_affinity))
    }

    /// Inserts a new handle only if the provided info is not outdated
    /// and the peer is not banned.
    pub fn insert(
        &self,
        peer_info: Arc<PeerInfo>,
        with_affinity: bool,
    ) -> Result<KnownPeerHandle, KnownPeersError> {
        // TODO: add capacity limit for entries without affinity
        let mut added = false;
        let inner = match self.0.entry(peer_info.id) {
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                let inner = KnownPeerInner::new(peer_info, with_affinity, &self.0);
                entry.insert(KnownPeerState::Stored(Arc::downgrade(&inner)));
                added = true;
                inner
            }
            dashmap::mapref::entry::Entry::Occupied(mut entry) => match entry.get_mut() {
                KnownPeerState::Banned => return Err(KnownPeersError::from(PeerBannedError)),
                KnownPeerState::Stored(item) => match item.upgrade() {
                    Some(inner) => match inner.try_update_peer_info(&peer_info, with_affinity)? {
                        true => inner,
                        false => return Err(KnownPeersError::OutdatedInfo),
                    },
                    None => {
                        let inner = KnownPeerInner::new(peer_info, with_affinity, &self.0);
                        *item = Arc::downgrade(&inner);
                        inner
                    }
                },
            },
        };

        if added {
            metrics::gauge!(METRIC_KNOWN_PEERS).increment(1);
        }

        Ok(KnownPeerHandle::from_inner(inner, with_affinity))
    }

    /// Same as [`KnownPeers::insert`], but ignores outdated info.
    pub fn insert_allow_outdated(
        &self,
        peer_info: Arc<PeerInfo>,
        with_affinity: bool,
    ) -> Result<KnownPeerHandle, PeerBannedError> {
        // TODO: add capacity limit for entries without affinity
        let mut added = false;
        let inner = match self.0.entry(peer_info.id) {
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                let inner = KnownPeerInner::new(peer_info, with_affinity, &self.0);
                entry.insert(KnownPeerState::Stored(Arc::downgrade(&inner)));
                added = true;
                inner
            }
            dashmap::mapref::entry::Entry::Occupied(mut entry) => match entry.get_mut() {
                KnownPeerState::Banned => return Err(PeerBannedError),
                KnownPeerState::Stored(item) => match item.upgrade() {
                    Some(inner) => {
                        // NOTE: Outdated info is ignored here.
                        inner.try_update_peer_info(&peer_info, with_affinity)?;
                        inner
                    }
                    None => {
                        let inner = KnownPeerInner::new(peer_info, with_affinity, &self.0);
                        *item = Arc::downgrade(&inner);
                        inner
                    }
                },
            },
        };

        if added {
            metrics::gauge!(METRIC_KNOWN_PEERS).increment(1);
        }

        Ok(KnownPeerHandle::from_inner(inner, with_affinity))
    }
}

enum KnownPeerState {
    Stored(Weak<KnownPeerInner>),
    Banned,
}

impl KnownPeerState {
    fn compute_affinity(&self) -> Option<PeerAffinity> {
        Some(match self {
            Self::Stored(weak) => weak.upgrade()?.compute_affinity(),
            Self::Banned => PeerAffinity::Never,
        })
    }
}

#[derive(Clone)]
#[repr(transparent)]
pub struct KnownPeerHandle(KnownPeerHandleState);

impl KnownPeerHandle {
    fn from_inner(inner: Arc<KnownPeerInner>, with_affinity: bool) -> Self {
        KnownPeerHandle(if with_affinity {
            KnownPeerHandleState::WithAffinity(ManuallyDrop::new(Arc::new(
                KnownPeerHandleWithAffinity { inner },
            )))
        } else {
            KnownPeerHandleState::Simple(ManuallyDrop::new(inner))
        })
    }

    pub fn peer_info(&self) -> arc_swap::Guard<Arc<PeerInfo>, arc_swap::DefaultStrategy> {
        self.inner().peer_info.load()
    }

    pub fn load_peer_info(&self) -> Arc<PeerInfo> {
        arc_swap::Guard::into_inner(self.peer_info())
    }

    pub fn is_banned(&self) -> bool {
        self.inner().is_banned()
    }

    pub fn max_affinity(&self) -> PeerAffinity {
        self.inner().compute_affinity()
    }

    pub fn update_peer_info(&self, peer_info: &Arc<PeerInfo>) -> Result<(), KnownPeersError> {
        match self.inner().try_update_peer_info(peer_info, false) {
            Ok(true) => Ok(()),
            Ok(false) => Err(KnownPeersError::OutdatedInfo),
            Err(e) => Err(KnownPeersError::PeerBanned(e)),
        }
    }

    pub fn ban(&self) -> bool {
        let inner = self.inner();
        inner.affinity.swap(AFFINITY_BANNED, Ordering::AcqRel) != AFFINITY_BANNED
    }

    pub fn increase_affinity(&mut self) -> bool {
        match &mut self.0 {
            KnownPeerHandleState::Simple(inner) => {
                // NOTE: Handle will be updated even if the peer is banned.
                inner.increase_affinity();

                // SAFETY: Inner value was not dropped.
                let inner = unsafe { ManuallyDrop::take(inner) };

                // Replace the old state with the new one, ensuring that the old state
                // is not dropped (because we took the value out of it).
                let prev_state = std::mem::replace(
                    &mut self.0,
                    KnownPeerHandleState::WithAffinity(ManuallyDrop::new(Arc::new(
                        KnownPeerHandleWithAffinity { inner },
                    ))),
                );

                // Forget the old state to avoid dropping it.
                #[allow(clippy::mem_forget)]
                std::mem::forget(prev_state);

                true
            }
            KnownPeerHandleState::WithAffinity(_) => false,
        }
    }

    pub fn decrease_affinity(&mut self) -> bool {
        match &mut self.0 {
            KnownPeerHandleState::Simple(_) => false,
            KnownPeerHandleState::WithAffinity(inner) => {
                // NOTE: Handle will be updated even if the peer is banned.
                inner.inner.decrease_affinity();

                // SAFETY: Inner value was not dropped.
                let inner = unsafe { ManuallyDrop::take(inner) };

                // Get `KnownPeerInner` out of the wrapper.
                let inner = match Arc::try_unwrap(inner) {
                    Ok(KnownPeerHandleWithAffinity { inner }) => inner,
                    Err(inner) => inner.inner.clone(),
                };

                // Replace the old state with the new one, ensuring that the old state
                // is not dropped (because we took the value out of it).
                let prev_state = std::mem::replace(
                    &mut self.0,
                    KnownPeerHandleState::Simple(ManuallyDrop::new(inner)),
                );

                // Forget the old state to avoid dropping it.
                #[allow(clippy::mem_forget)]
                std::mem::forget(prev_state);

                true
            }
        }
    }

    pub fn downgrade(&self) -> WeakKnownPeerHandle {
        WeakKnownPeerHandle(match &self.0 {
            KnownPeerHandleState::Simple(data) => {
                WeakKnownPeerHandleState::Simple(Arc::downgrade(data))
            }
            KnownPeerHandleState::WithAffinity(data) => {
                WeakKnownPeerHandleState::WithAffinity(Arc::downgrade(data))
            }
        })
    }

    fn inner(&self) -> &KnownPeerInner {
        match &self.0 {
            KnownPeerHandleState::Simple(data) => data.as_ref(),
            KnownPeerHandleState::WithAffinity(data) => data.inner.as_ref(),
        }
    }
}

#[derive(Clone)]
enum KnownPeerHandleState {
    Simple(ManuallyDrop<Arc<KnownPeerInner>>),
    WithAffinity(ManuallyDrop<Arc<KnownPeerHandleWithAffinity>>),
}

impl Drop for KnownPeerHandleState {
    fn drop(&mut self) {
        let inner;
        let is_banned;
        match self {
            KnownPeerHandleState::Simple(data) => {
                // SAFETY: inner value is dropped only once
                inner = unsafe { ManuallyDrop::take(data) };
                is_banned = inner.is_banned();
            }
            KnownPeerHandleState::WithAffinity(data) => {
                // SAFETY: inner value is dropped only once
                match Arc::into_inner(unsafe { ManuallyDrop::take(data) }) {
                    Some(data) => {
                        inner = data.inner;
                        is_banned = !inner.decrease_affinity() || inner.is_banned();
                    }
                    None => return,
                }
            }
        };

        if is_banned {
            // Don't remove banned peers from the known peers cache
            return;
        }

        if let Some(inner) = Arc::into_inner(inner) {
            // If the last reference is dropped, remove the peer from the known peers cache
            if let Some(peers) = inner.weak_known_peers.upgrade() {
                peers.remove(&inner.peer_info.load().id);
                metrics::gauge!(METRIC_KNOWN_PEERS).decrement(1);
            }
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
#[repr(transparent)]
pub struct WeakKnownPeerHandle(WeakKnownPeerHandleState);

impl WeakKnownPeerHandle {
    pub fn upgrade(&self) -> Option<KnownPeerHandle> {
        Some(KnownPeerHandle(match &self.0 {
            WeakKnownPeerHandleState::Simple(weak) => {
                KnownPeerHandleState::Simple(ManuallyDrop::new(weak.upgrade()?))
            }
            WeakKnownPeerHandleState::WithAffinity(weak) => {
                KnownPeerHandleState::WithAffinity(ManuallyDrop::new(weak.upgrade()?))
            }
        }))
    }
}

#[derive(Clone)]
enum WeakKnownPeerHandleState {
    Simple(Weak<KnownPeerInner>),
    WithAffinity(Weak<KnownPeerHandleWithAffinity>),
}

impl Eq for WeakKnownPeerHandleState {}
impl PartialEq for WeakKnownPeerHandleState {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Simple(left), Self::Simple(right)) => Weak::ptr_eq(left, right),
            (Self::WithAffinity(left), Self::WithAffinity(right)) => Weak::ptr_eq(left, right),
            _ => false,
        }
    }
}

struct KnownPeerHandleWithAffinity {
    inner: Arc<KnownPeerInner>,
}

struct KnownPeerInner {
    peer_info: ArcSwap<PeerInfo>,
    affinity: AtomicUsize,
    weak_known_peers: Weak<FastDashMap<PeerId, KnownPeerState>>,
}

impl KnownPeerInner {
    fn new(
        peer_info: Arc<PeerInfo>,
        with_affinity: bool,
        known_peers: &Arc<FastDashMap<PeerId, KnownPeerState>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            peer_info: ArcSwap::from(peer_info),
            affinity: AtomicUsize::new(if with_affinity { 1 } else { 0 }),
            weak_known_peers: Arc::downgrade(known_peers),
        })
    }

    fn is_banned(&self) -> bool {
        self.affinity.load(Ordering::Acquire) == AFFINITY_BANNED
    }

    fn compute_affinity(&self) -> PeerAffinity {
        match self.affinity.load(Ordering::Acquire) {
            0 => PeerAffinity::Allowed,
            AFFINITY_BANNED => PeerAffinity::Never,
            _ => PeerAffinity::High,
        }
    }

    fn increase_affinity(&self) -> bool {
        let mut current = self.affinity.load(Ordering::Acquire);
        while current != AFFINITY_BANNED {
            debug_assert_ne!(current, AFFINITY_BANNED - 1);
            match self.affinity.compare_exchange_weak(
                current,
                current + 1,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(affinity) => current = affinity,
            }
        }

        false
    }

    fn decrease_affinity(&self) -> bool {
        let mut current = self.affinity.load(Ordering::Acquire);
        while current != AFFINITY_BANNED {
            debug_assert_ne!(current, 0);
            match self.affinity.compare_exchange_weak(
                current,
                current - 1,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(affinity) => current = affinity,
            }
        }

        false
    }

    fn try_update_peer_info(
        &self,
        peer_info: &Arc<PeerInfo>,
        with_affinity: bool,
    ) -> Result<bool, PeerBannedError> {
        struct AffinityGuard<'a> {
            inner: &'a KnownPeerInner,
            decrease_on_drop: bool,
        }

        impl AffinityGuard<'_> {
            fn increase_affinity_or_check_ban(&mut self, with_affinity: bool) -> bool {
                let with_affinity = with_affinity && !self.decrease_on_drop;
                let is_banned = if with_affinity {
                    !self.inner.increase_affinity()
                } else {
                    self.inner.is_banned()
                };

                if !is_banned && with_affinity {
                    self.decrease_on_drop = true;
                }

                is_banned
            }
        }

        impl Drop for AffinityGuard<'_> {
            fn drop(&mut self) {
                if self.decrease_on_drop {
                    self.inner.decrease_affinity();
                }
            }
        }

        // Create a guard to restore the peer affinity in case of an error
        let mut guard = AffinityGuard {
            inner: self,
            decrease_on_drop: false,
        };

        let mut cur = self.peer_info.load();
        let updated = loop {
            if guard.increase_affinity_or_check_ban(with_affinity) {
                // Do nothing for banned peers
                return Err(PeerBannedError);
            }

            match cur.created_at.cmp(&peer_info.created_at) {
                // Do nothing for the same creation time
                // TODO: is `created_at` equality enough?
                std::cmp::Ordering::Equal => break true,
                // Try to update peer info
                std::cmp::Ordering::Less => {
                    let prev = self.peer_info.compare_and_swap(&*cur, peer_info.clone());
                    if std::ptr::eq(cur.as_raw(), prev.as_raw()) {
                        break true;
                    } else {
                        cur = prev;
                    }
                }
                // Allow an outdated data
                std::cmp::Ordering::Greater => break false,
            }
        };

        guard.decrease_on_drop = false;
        Ok(updated)
    }
}

const AFFINITY_BANNED: usize = usize::MAX;

#[derive(Debug, thiserror::Error)]
pub enum KnownPeersError {
    #[error(transparent)]
    PeerBanned(#[from] PeerBannedError),
    #[error("provided peer info is outdated")]
    OutdatedInfo,
}

#[derive(Debug, Copy, Clone, thiserror::Error)]
#[error("peer is banned")]
pub struct PeerBannedError;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::make_peer_info_stub;

    #[test]
    fn remove_from_cache_on_drop_works() {
        let peers = KnownPeers::new();

        let peer_info = make_peer_info_stub(rand::random());
        let handle = peers.insert(peer_info.clone(), false).unwrap();
        assert!(peers.contains(&peer_info.id));
        assert!(!peers.is_banned(&peer_info.id));
        assert_eq!(peers.get(&peer_info.id), Some(peer_info.clone()));
        assert_eq!(
            peers.get_affinity(&peer_info.id),
            Some(PeerAffinity::Allowed)
        );

        assert_eq!(handle.peer_info().as_ref(), peer_info.as_ref());
        assert_eq!(handle.max_affinity(), PeerAffinity::Allowed);

        let other_handle = peers.insert(peer_info.clone(), false).unwrap();
        assert!(peers.contains(&peer_info.id));
        assert!(!peers.is_banned(&peer_info.id));
        assert_eq!(peers.get(&peer_info.id), Some(peer_info.clone()));
        assert_eq!(
            peers.get_affinity(&peer_info.id),
            Some(PeerAffinity::Allowed)
        );

        assert_eq!(other_handle.peer_info().as_ref(), peer_info.as_ref());
        assert_eq!(other_handle.max_affinity(), PeerAffinity::Allowed);

        drop(other_handle);
        assert!(peers.contains(&peer_info.id));
        assert!(!peers.is_banned(&peer_info.id));
        assert_eq!(peers.get(&peer_info.id), Some(peer_info.clone()));
        assert_eq!(
            peers.get_affinity(&peer_info.id),
            Some(PeerAffinity::Allowed)
        );

        drop(handle);
        assert!(!peers.contains(&peer_info.id));
        assert!(!peers.is_banned(&peer_info.id));
        assert_eq!(peers.get(&peer_info.id), None);
        assert_eq!(peers.get_affinity(&peer_info.id), None);

        peers.insert(peer_info.clone(), false).unwrap();
    }

    #[test]
    fn with_affinity_after_simple() {
        let peers = KnownPeers::new();

        let peer_info = make_peer_info_stub(rand::random());
        let handle_simple = peers.insert(peer_info.clone(), false).unwrap();
        assert!(peers.contains(&peer_info.id));
        assert_eq!(
            peers.get_affinity(&peer_info.id),
            Some(PeerAffinity::Allowed)
        );
        assert_eq!(handle_simple.max_affinity(), PeerAffinity::Allowed);

        let handle_with_affinity = peers.insert(peer_info.clone(), true).unwrap();
        assert!(peers.contains(&peer_info.id));
        assert_eq!(peers.get_affinity(&peer_info.id), Some(PeerAffinity::High));
        assert_eq!(handle_with_affinity.max_affinity(), PeerAffinity::High);
        assert_eq!(handle_simple.max_affinity(), PeerAffinity::High);

        drop(handle_with_affinity);
        assert!(peers.contains(&peer_info.id));
        assert_eq!(handle_simple.max_affinity(), PeerAffinity::Allowed);
        assert_eq!(
            peers.get_affinity(&peer_info.id),
            Some(PeerAffinity::Allowed)
        );

        drop(handle_simple);
        assert!(!peers.contains(&peer_info.id));
        assert_eq!(peers.get_affinity(&peer_info.id), None);
    }

    #[test]
    fn with_affinity_before_simple() {
        let peers = KnownPeers::new();

        let peer_info = make_peer_info_stub(rand::random());
        let handle_with_affinity = peers.insert(peer_info.clone(), true).unwrap();
        assert!(peers.contains(&peer_info.id));
        assert_eq!(peers.get_affinity(&peer_info.id), Some(PeerAffinity::High));
        assert_eq!(handle_with_affinity.max_affinity(), PeerAffinity::High);

        let handle_simple = peers.insert(peer_info.clone(), false).unwrap();
        assert!(peers.contains(&peer_info.id));
        assert_eq!(peers.get_affinity(&peer_info.id), Some(PeerAffinity::High));
        assert_eq!(handle_with_affinity.max_affinity(), PeerAffinity::High);
        assert_eq!(handle_simple.max_affinity(), PeerAffinity::High);

        drop(handle_simple);
        assert!(peers.contains(&peer_info.id));
        assert_eq!(handle_with_affinity.max_affinity(), PeerAffinity::High);
        assert_eq!(peers.get_affinity(&peer_info.id), Some(PeerAffinity::High));

        drop(handle_with_affinity);
        assert!(!peers.contains(&peer_info.id));
        assert_eq!(peers.get_affinity(&peer_info.id), None);
    }

    #[test]
    fn ban_while_handle_exists() {
        let peers = KnownPeers::new();

        let peer_info = make_peer_info_stub(rand::random());
        let handle = peers.insert(peer_info.clone(), false).unwrap();
        assert!(peers.contains(&peer_info.id));
        assert_eq!(handle.max_affinity(), PeerAffinity::Allowed);

        peers.ban(&peer_info.id);
        assert!(peers.contains(&peer_info.id));
        assert!(peers.is_banned(&peer_info.id));
        assert_eq!(handle.max_affinity(), PeerAffinity::Never);
        assert_eq!(peers.get(&peer_info.id), Some(peer_info.clone()));
        assert_eq!(peers.get_affinity(&peer_info.id), Some(PeerAffinity::Never));
    }
}
