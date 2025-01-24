use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::{Arc, Weak};

#[cfg(target_os = "linux")]
use anyhow::Context;
use anyhow::Result;
use everscale_crypto::ed25519;
use tokio::sync::{broadcast, mpsc, oneshot};

use self::config::EndpointConfig;
pub use self::config::{NetworkConfig, QuicConfig};
pub use self::connection::{Connection, RecvStream, SendStream};
use self::connection_manager::{ActivePeers, ConnectionManager, ConnectionManagerRequest};
pub use self::connection_manager::{
    KnownPeerHandle, KnownPeers, KnownPeersError, PeerBannedError, WeakKnownPeerHandle,
};
use self::endpoint::Endpoint;
pub use self::peer::Peer;
use crate::types::{
    Address, DisconnectReason, PeerEvent, PeerId, PeerInfo, Response, Service, ServiceExt,
    ServiceRequest,
};

mod config;
mod connection;
mod connection_manager;
mod crypto;
mod endpoint;
mod peer;
mod request_handler;
mod wire;

pub struct NetworkBuilder<MandatoryFields = ([u8; 32],)> {
    mandatory_fields: MandatoryFields,
    optional_fields: BuilderFields,
}

#[derive(Default)]
struct BuilderFields {
    config: Option<NetworkConfig>,
    remote_addr: Option<Address>,
}

impl<MandatoryFields> NetworkBuilder<MandatoryFields> {
    pub fn with_config(mut self, config: NetworkConfig) -> Self {
        self.optional_fields.config = Some(config);
        self
    }

    pub fn with_remote_addr<T: Into<Address>>(mut self, addr: T) -> Self {
        self.optional_fields.remote_addr = Some(addr.into());
        self
    }
}

impl NetworkBuilder<((),)> {
    pub fn with_private_key(self, private_key: [u8; 32]) -> NetworkBuilder<([u8; 32],)> {
        NetworkBuilder {
            mandatory_fields: (private_key,),
            optional_fields: self.optional_fields,
        }
    }

    pub fn with_random_private_key(self) -> NetworkBuilder<([u8; 32],)> {
        self.with_private_key(rand::random())
    }
}

impl NetworkBuilder {
    pub fn build<T: ToSocket, S>(self, bind_address: T, service: S) -> Result<Network>
    where
        S: Send + Sync + Clone + 'static,
        S: Service<ServiceRequest, QueryResponse = Response>,
    {
        let config = self.optional_fields.config.unwrap_or_default();
        let quic_config = config.quic.clone().unwrap_or_default();
        let (private_key,) = self.mandatory_fields;

        let keypair = ed25519::KeyPair::from(&ed25519::SecretKey::from_bytes(private_key));

        let endpoint_config = EndpointConfig::builder()
            .with_private_key(private_key)
            .with_0rtt_enabled(config.enable_0rtt)
            .with_transport_config(quic_config.make_transport_config())
            .build()?;

        let socket = bind_address.to_socket().map(socket2::Socket::from)?;

        let max_socket_size = MaxBufferSize::read()?;

        set_socket_buffer(
            &socket,
            quic_config.socket_send_buffer_size,
            max_socket_size.map(|m| m.send),
            |s, size| s.set_send_buffer_size(size),
            "send",
        );

        set_socket_buffer(
            &socket,
            quic_config.socket_recv_buffer_size,
            max_socket_size.map(|m| m.recv),
            |s, size| s.set_recv_buffer_size(size),
            "recv",
        );

        let config = Arc::new(config);
        let endpoint = Arc::new(Endpoint::new(endpoint_config, socket.into())?);
        let active_peers = ActivePeers::new(config.active_peers_event_channel_capacity);
        let known_peers = KnownPeers::new();

        let remote_addr = self.optional_fields.remote_addr.unwrap_or_else(|| {
            let addr = endpoint.local_addr();
            tracing::debug!(%addr, "using local address as remote address");
            addr.into()
        });

        let service = service.boxed_clone();

        let (connection_manager, connection_manager_handle) = ConnectionManager::new(
            config.clone(),
            endpoint.clone(),
            active_peers.clone(),
            known_peers.clone(),
            service,
        );

        tokio::spawn(connection_manager.start());

        Ok(Network(Arc::new(NetworkInner {
            config,
            remote_addr,
            endpoint,
            active_peers,
            known_peers,
            connection_manager_handle,
            keypair,
        })))
    }
}

fn set_socket_buffer(
    socket: &socket2::Socket,
    config_size: Option<usize>,
    max_size: Option<usize>,
    set_buffer_fn: impl Fn(&socket2::Socket, usize) -> std::io::Result<()>,
    buffer_type: &str,
) {
    if let Some(size) = config_size {
        if let Err(e) = set_buffer_fn(socket, size) {
            tracing::error!(%size, "failed to set socket {} buffer size: {e:?}", buffer_type);
        }
    } else if let Some(max) = max_size {
        if let Err(e) = set_buffer_fn(socket, max) {
            tracing::error!(%max, "failed to set socket {} buffer size to max value: {e:?}", buffer_type);
        }
        tracing::info!(
            "set socket {} buffer size to max value: {}",
            buffer_type,
            max
        );
    }
}

#[derive(Clone)]
#[repr(transparent)]
pub struct WeakNetwork(Weak<NetworkInner>);

impl WeakNetwork {
    pub fn upgrade(&self) -> Option<Network> {
        self.0
            .upgrade()
            .map(Network)
            .and_then(|network| (!network.is_closed()).then_some(network))
    }
}

#[derive(Clone)]
#[repr(transparent)]
pub struct Network(Arc<NetworkInner>);

impl Network {
    pub fn builder() -> NetworkBuilder<((),)> {
        NetworkBuilder {
            mandatory_fields: ((),),
            optional_fields: Default::default(),
        }
    }

    /// The public address of this node.
    pub fn remote_addr(&self) -> &Address {
        self.0.remote_addr()
    }

    /// The listening address of this node.
    pub fn local_addr(&self) -> SocketAddr {
        self.0.local_addr()
    }

    /// The local peer id of this node.
    pub fn peer_id(&self) -> &PeerId {
        self.0.peer_id()
    }

    /// Returns true if the peer is currently connected.
    pub fn is_active(&self, peer_id: &PeerId) -> bool {
        self.0.active_peers.contains(peer_id)
    }

    /// Returns a connection wrapper for the specified peer.
    pub fn peer(&self, peer_id: &PeerId) -> Option<Peer> {
        self.0.peer(peer_id)
    }

    /// A set of known peers.
    pub fn known_peers(&self) -> &KnownPeers {
        &self.0.known_peers
    }

    /// Subscribe to active peer changes.
    pub fn subscribe(&self) -> broadcast::Receiver<PeerEvent> {
        self.0.active_peers.subscribe()
    }

    /// Initiate a connection to the specified peer.
    pub async fn connect<T>(&self, addr: T, peer_id: &PeerId) -> Result<Peer, ConnectionError>
    where
        T: Into<Address>,
    {
        self.0.connect(addr.into(), peer_id).await
    }

    pub fn disconnect(&self, peer_id: &PeerId) {
        self.0.disconnect(peer_id);
    }

    pub async fn shutdown(&self) {
        self.0.shutdown().await;
    }

    pub fn is_closed(&self) -> bool {
        self.0.is_closed()
    }

    pub fn sign_tl<T: tl_proto::TlWrite>(&self, data: T) -> [u8; 64] {
        self.0.keypair.sign(data)
    }

    pub fn sign_raw(&self, data: &[u8]) -> [u8; 64] {
        self.0.keypair.sign_raw(data)
    }

    pub fn sign_peer_info(&self, now: u32, ttl: u32) -> PeerInfo {
        let mut res = PeerInfo {
            id: *self.0.peer_id(),
            address_list: vec![self.remote_addr().clone()].into_boxed_slice(),
            created_at: now,
            expires_at: now.saturating_add(ttl),
            signature: Box::new([0; 64]),
        };
        *res.signature = self.sign_tl(&res);
        res
    }

    pub fn downgrade(this: &Self) -> WeakNetwork {
        WeakNetwork(Arc::downgrade(&this.0))
    }

    /// returns the maximum size which can be potentially sent in a single frame
    pub fn max_frame_size(&self) -> usize {
        self.0.config.max_frame_size.0 as usize
    }
}

struct NetworkInner {
    config: Arc<NetworkConfig>,
    remote_addr: Address,
    endpoint: Arc<Endpoint>,
    active_peers: ActivePeers,
    known_peers: KnownPeers,
    connection_manager_handle: mpsc::Sender<ConnectionManagerRequest>,
    keypair: ed25519::KeyPair,
}

impl NetworkInner {
    fn remote_addr(&self) -> &Address {
        &self.remote_addr
    }

    fn local_addr(&self) -> SocketAddr {
        self.endpoint.local_addr()
    }

    fn peer_id(&self) -> &PeerId {
        self.endpoint.peer_id()
    }

    async fn connect(&self, addr: Address, peer_id: &PeerId) -> Result<Peer, ConnectionError> {
        let (tx, rx) = oneshot::channel();
        self.connection_manager_handle
            .send(ConnectionManagerRequest::Connect(addr, *peer_id, tx))
            .await
            .map_err(|_e| ConnectionError::Shutdown)?;

        let Ok(res) = rx.await else {
            return Err(ConnectionError::Shutdown);
        };

        res.map(|c| Peer::new(c, self.config.clone()))
    }

    fn disconnect(&self, peer_id: &PeerId) {
        self.active_peers
            .remove(peer_id, DisconnectReason::Requested);
    }

    fn peer(&self, peer_id: &PeerId) -> Option<Peer> {
        let connection = self.active_peers.get(peer_id)?;
        Some(Peer::new(connection, self.config.clone()))
    }

    async fn shutdown(&self) {
        let (sender, receiver) = oneshot::channel();
        if self
            .connection_manager_handle
            .send(ConnectionManagerRequest::Shutdown(sender))
            .await
            .is_err()
        {
            return;
        }

        receiver.await.ok();
    }

    fn is_closed(&self) -> bool {
        self.connection_manager_handle.is_closed()
    }
}

impl Drop for NetworkInner {
    fn drop(&mut self) {
        tracing::debug!("network dropped");
    }
}

pub trait ToSocket {
    fn to_socket(self) -> Result<std::net::UdpSocket>;
}

impl ToSocket for std::net::UdpSocket {
    fn to_socket(self) -> Result<std::net::UdpSocket> {
        Ok(self)
    }
}

macro_rules! impl_to_socket_for_addr {
    ($($ty:ty),*$(,)?) => {$(
        impl ToSocket for $ty {
            fn to_socket(self) -> Result<std::net::UdpSocket> {
                bind_socket_to_addr(self)
            }
        }
    )*};
}

impl_to_socket_for_addr! {
    SocketAddr,
    std::net::SocketAddrV4,
    std::net::SocketAddrV6,
    (std::net::IpAddr, u16),
    (std::net::Ipv4Addr, u16),
    (std::net::Ipv6Addr, u16),
    (&str, u16),
    (String, u16),
    &str,
    String,
    &[SocketAddr],
    Address,
}

fn bind_socket_to_addr<T: ToSocketAddrs>(bind_address: T) -> Result<std::net::UdpSocket> {
    use socket2::{Domain, Protocol, Socket, Type};

    let mut err = anyhow::anyhow!("no addresses to bind to");
    for addr in bind_address.to_socket_addrs()? {
        let s = Socket::new(Domain::for_address(addr), Type::DGRAM, Some(Protocol::UDP))?;
        if let Err(e) = s.bind(&socket2::SockAddr::from(addr)) {
            err = e.into();
        } else {
            return Ok(s.into());
        }
    }
    Err(err)
}

#[derive(Debug, Clone, Copy)]
struct MaxBufferSize {
    send: usize,
    recv: usize,
}

impl MaxBufferSize {
    #[cfg(target_os = "linux")]
    pub fn read() -> Result<Option<Self>> {
        const WMEM: &str = "wmem_max";
        const RMEM: &str = "rmem_max";

        #[cfg(any(feature = "test", test))]
        let proc_path = std::env::var("MOCK_PROC_PATH").unwrap_or_else(|_| "/proc".to_string());
        #[cfg(not(any(feature = "test", test)))]
        let proc_path = "/proc";
        let proc_path = std::path::Path::new(&proc_path).join("sys/net/core");

        let read_and_parse = |file_name: &str| -> Result<Option<usize>> {
            let path = proc_path.join(file_name);
            if !path.exists() {
                tracing::warn!("{} not found", path.display());
                return Ok(None);
            }
            let res = std::fs::read_to_string(&path)
                .with_context(|| format!("Failed to read {}", path.display()))?
                .trim()
                .parse()
                .with_context(|| format!("Failed to parse {}", path.display()))?;
            Ok(Some(res))
        };

        let rmem = read_and_parse(RMEM)?;
        let wmem = read_and_parse(WMEM)?;

        Ok(rmem.zip(wmem).map(|(recv, send)| Self { send, recv }))
    }

    #[cfg(not(target_os = "linux"))]
    pub fn read() -> std::io::Result<Option<Self>> {
        Ok(None)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConnectionError {
    #[error("invalid address")]
    InvalidAddress,
    #[error("connection init failed")]
    ConnectionInitFailed,
    #[error("invalid certificate: {0}")]
    InvalidCertificate(Arc<str>),
    #[error("handshake failed")]
    HandshakeFailed,
    #[error("connection timeout")]
    Timeout,
    #[error("network has been shutdown")]
    Shutdown,
}

#[cfg(test)]
mod tests {
    use futures_util::stream::FuturesUnordered;
    use futures_util::StreamExt;

    use super::*;
    use crate::types::{service_message_fn, service_query_fn, BoxCloneService, PeerInfo, Request};
    use crate::util::{NetworkExt, UnknownPeerError};

    fn echo_service() -> BoxCloneService<ServiceRequest, Response> {
        let handle = |request: ServiceRequest| async move {
            tracing::trace!("received: {}", request.body.escape_ascii());
            let response = Response {
                version: Default::default(),
                body: request.body,
            };
            Some(response)
        };
        service_query_fn(handle).boxed_clone()
    }

    fn make_network() -> Result<Network> {
        Network::builder()
            .with_config(NetworkConfig {
                enable_0rtt: true,
                ..Default::default()
            })
            .with_random_private_key()
            .build("127.0.0.1:0", echo_service())
    }

    fn make_peer_info(network: &Network) -> Arc<PeerInfo> {
        Arc::new(PeerInfo {
            id: *network.peer_id(),
            address_list: vec![network.remote_addr().clone()].into_boxed_slice(),
            created_at: 0,
            expires_at: u32::MAX,
            signature: Box::new([0; 64]),
        })
    }

    #[tokio::test]
    async fn connection_manager_works() -> Result<()> {
        tycho_util::test::init_logger("connection_manager_works", "debug");

        let peer1 = make_network()?;
        let peer2 = make_network()?;

        peer1
            .connect(peer2.local_addr(), peer2.peer_id())
            .await
            .unwrap();
        peer2
            .connect(peer1.local_addr(), peer1.peer_id())
            .await
            .unwrap();

        Ok(())
    }

    #[tokio::test]
    async fn invalid_peer_id_detectable() -> Result<()> {
        tycho_util::test::init_logger("invalid_peer_id_detectable", "debug");

        let peer1 = make_network()?;
        let peer2 = make_network()?;

        let make_invalid_peer_info = |network: &Network| {
            Arc::new(PeerInfo {
                id: PeerId([0; 32]),
                address_list: vec![network.remote_addr().clone()].into_boxed_slice(),
                created_at: 0,
                expires_at: u32::MAX,
                signature: Box::new([0; 64]),
            })
        };
        let _handle = peer1.known_peers().insert(make_peer_info(&peer2), false)?;
        let _handle = peer1
            .known_peers()
            .insert(make_invalid_peer_info(&peer2), false)?;

        let _handle = peer2.known_peers().insert(make_peer_info(&peer1), false)?;
        let _handle = peer2
            .known_peers()
            .insert(make_invalid_peer_info(&peer1), false)?;

        let req = Request {
            version: Default::default(),
            body: "hello".into(),
        };

        peer1.query(peer2.peer_id(), req.clone()).await?;
        peer2.query(peer1.peer_id(), req.clone()).await?;

        fn assert_is_invalid_certificate(e: anyhow::Error) {
            // A non-recursive downcast to find a connection error
            let e = (*e).downcast_ref::<ConnectionError>().unwrap();
            assert!(matches!(e, ConnectionError::InvalidCertificate(_)));
        }

        let err = peer1
            .query(&PeerId([0; 32]), req.clone())
            .await
            .map(|_| ())
            .unwrap_err();
        assert_is_invalid_certificate(err);

        let err = peer2
            .query(&PeerId([0; 32]), req.clone())
            .await
            .map(|_| ())
            .unwrap_err();
        assert_is_invalid_certificate(err);

        fn assert_is_unknown_peer(e: anyhow::Error, peer_id: &PeerId) {
            // A non-recursive downcast to find an error
            let e = (*e).downcast_ref::<UnknownPeerError>().unwrap();
            assert_eq!(e, &UnknownPeerError { peer_id: *peer_id });
        }

        let invalid_peer_id = PeerId([0xff; 32]);
        let err = peer1
            .query(&invalid_peer_id, req.clone())
            .await
            .map(|_| ())
            .unwrap_err();
        assert_is_unknown_peer(err, &invalid_peer_id);

        Ok(())
    }

    #[tokio::test]
    async fn simultaneous_queries() -> Result<()> {
        tycho_util::test::init_logger("simultaneous_queries", "debug");

        for _ in 0..10 {
            let peer1 = make_network()?;
            let peer2 = make_network()?;

            let _peer1_peer2_handle = peer1.known_peers().insert(make_peer_info(&peer2), false)?;
            let _peer2_peer1_handle = peer2.known_peers().insert(make_peer_info(&peer1), false)?;

            let req = Request {
                version: Default::default(),
                body: "hello".into(),
            };
            let peer1_fut = std::pin::pin!(peer1.query(peer2.peer_id(), req.clone()));
            let peer2_fut = std::pin::pin!(peer2.query(peer1.peer_id(), req.clone()));

            let (res1, res2) = futures_util::future::join(peer1_fut, peer2_fut).await;
            assert_eq!(res1?.body, req.body);
            assert_eq!(res2?.body, req.body);
        }

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn uni_message_handler() -> Result<()> {
        tycho_util::test::init_logger("uni_message_handler", "debug");

        fn noop_service() -> BoxCloneService<ServiceRequest, Response> {
            let handle = |request: ServiceRequest| async move {
                tracing::trace!("received: {} bytes", request.body.len());
            };
            service_message_fn(handle).boxed_clone()
        }

        fn make_network() -> Result<Network> {
            Network::builder()
                .with_config(NetworkConfig {
                    enable_0rtt: true,
                    ..Default::default()
                })
                .with_random_private_key()
                .build("127.0.0.1:0", noop_service())
        }

        let left = make_network()?;
        let right = make_network()?;

        let _left_to_right = left.known_peers().insert(make_peer_info(&right), false)?;
        let _right_to_left = right.known_peers().insert(make_peer_info(&left), false)?;

        let req = Request {
            version: Default::default(),
            body: vec![0xff; 750 * 1024].into(),
        };

        for _ in 0..10 {
            let mut futures = FuturesUnordered::new();
            for _ in 0..100 {
                futures.push(left.send(right.peer_id(), req.clone()));
            }

            while let Some(res) = futures.next().await {
                res?;
            }
        }

        Ok(())
    }

    #[test]
    fn socket_size_works() {
        if std::path::Path::new("/proc").exists() {
            let socket_size = MaxBufferSize::read()
                .unwrap()
                .expect("socket size not found");
            assert!(socket_size.send > 0);
            assert!(socket_size.recv > 0);
        } else {
            // github doesn't expose /proc and macos exists
            let procfs = tempfile::tempdir().unwrap();
            std::env::set_var("MOCK_PROC_PATH", procfs.path());

            std::fs::create_dir_all(procfs.path().join("sys/net/core")).unwrap();
            std::fs::write(procfs.path().join("sys/net/core/wmem_max"), "100000\n").unwrap();
            std::fs::write(procfs.path().join("sys/net/core/rmem_max"), "100000\n").unwrap();

            let socket_size = MaxBufferSize::read()
                .unwrap()
                .expect("socket size not found");

            assert_eq!(socket_size.send, 100000);
            assert_eq!(socket_size.recv, 100000);
        }
    }
}
