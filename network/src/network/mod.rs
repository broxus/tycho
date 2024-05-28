use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::{Arc, Weak};

use anyhow::Result;
use everscale_crypto::ed25519;
use tokio::sync::{broadcast, mpsc, oneshot};

use self::config::EndpointConfig;
pub use self::config::{NetworkConfig, QuicConfig};
pub use self::connection::{Connection, RecvStream, SendStream};
pub use self::connection_manager::{
    ActivePeers, KnownPeerHandle, KnownPeers, KnownPeersError, PeerBannedError, WeakActivePeers,
    WeakKnownPeerHandle,
};
use self::connection_manager::{ConnectionManager, ConnectionManagerRequest};
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

pub struct NetworkBuilder<MandatoryFields = (String, [u8; 32])> {
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

impl<T2> NetworkBuilder<((), T2)> {
    pub fn with_service_name<T: Into<String>>(self, name: T) -> NetworkBuilder<(String, T2)> {
        let (_, private_key) = self.mandatory_fields;
        NetworkBuilder {
            mandatory_fields: (name.into(), private_key),
            optional_fields: self.optional_fields,
        }
    }
}

impl<T1> NetworkBuilder<(T1, ())> {
    pub fn with_private_key(self, private_key: [u8; 32]) -> NetworkBuilder<(T1, [u8; 32])> {
        let (service_name, _) = self.mandatory_fields;
        NetworkBuilder {
            mandatory_fields: (service_name, private_key),
            optional_fields: self.optional_fields,
        }
    }

    pub fn with_random_private_key(self) -> NetworkBuilder<(T1, [u8; 32])> {
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
        let (service_name, private_key) = self.mandatory_fields;

        let keypair = ed25519::KeyPair::from(&ed25519::SecretKey::from_bytes(private_key));

        let endpoint_config = EndpointConfig::builder()
            .with_service_name(service_name)
            .with_private_key(private_key)
            .with_0rtt_enabled(config.enable_0rtt)
            .with_transport_config(quic_config.make_transport_config())
            .build()?;

        let socket = bind_address.to_socket().map(socket2::Socket::from)?;

        if let Some(send_buffer_size) = quic_config.socket_send_buffer_size {
            if let Err(e) = socket.set_send_buffer_size(send_buffer_size) {
                tracing::warn!(
                    send_buffer_size,
                    "failed to set socket send buffer size: {e:?}"
                );
            }
        }

        if let Some(recv_buffer_size) = quic_config.socket_recv_buffer_size {
            if let Err(e) = socket.set_recv_buffer_size(recv_buffer_size) {
                tracing::warn!(
                    recv_buffer_size,
                    "failed to set socket recv buffer size: {e:?}"
                );
            }
        }

        let config = Arc::new(config);
        let endpoint = Arc::new(Endpoint::new(endpoint_config, socket.into())?);
        let active_peers = ActivePeers::new(config.active_peers_event_channel_capacity);
        let weak_active_peers = ActivePeers::downgrade(&active_peers);
        let known_peers = KnownPeers::new();

        let remote_addr = self.optional_fields.remote_addr.unwrap_or_else(|| {
            let addr = endpoint.local_addr();
            tracing::debug!(%addr, "using local address as remote address");
            addr.into()
        });

        let inner = Arc::new_cyclic(move |_weak| {
            let service = service.boxed_clone();

            let (connection_manager, connection_manager_handle) = ConnectionManager::new(
                config.clone(),
                endpoint.clone(),
                active_peers,
                known_peers.clone(),
                service,
            );

            tokio::spawn(connection_manager.start());

            NetworkInner {
                config,
                remote_addr,
                endpoint,
                active_peers: weak_active_peers,
                known_peers,
                connection_manager_handle,
                keypair,
            }
        });

        Ok(Network(inner))
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
    pub fn builder() -> NetworkBuilder<((), ())> {
        NetworkBuilder {
            mandatory_fields: ((), ()),
            optional_fields: Default::default(),
        }
    }

    pub fn remote_addr(&self) -> &Address {
        self.0.remote_addr()
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.0.local_addr()
    }

    pub fn peer_id(&self) -> &PeerId {
        self.0.peer_id()
    }

    pub fn peer(&self, peer_id: &PeerId) -> Option<Peer> {
        self.0.peer(peer_id)
    }

    pub fn known_peers(&self) -> &KnownPeers {
        self.0.known_peers()
    }

    pub fn subscribe(&self) -> Result<broadcast::Receiver<PeerEvent>> {
        let active_peers = self.0.active_peers.upgrade().ok_or(NetworkShutdownError)?;
        Ok(active_peers.subscribe())
    }

    pub async fn connect<T>(&self, addr: T, peer_id: &PeerId) -> Result<PeerId>
    where
        T: Into<Address>,
    {
        self.0.connect(addr.into(), peer_id).await
    }

    pub fn disconnect(&self, peer_id: &PeerId) -> Result<()> {
        self.0.disconnect(peer_id)
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.0.shutdown().await
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
}

struct NetworkInner {
    config: Arc<NetworkConfig>,
    remote_addr: Address,
    endpoint: Arc<Endpoint>,
    active_peers: WeakActivePeers,
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

    fn known_peers(&self) -> &KnownPeers {
        &self.known_peers
    }

    async fn connect(&self, addr: Address, peer_id: &PeerId) -> Result<PeerId> {
        #[derive(thiserror::Error, Debug)]
        #[error(transparent)]
        struct ConnectionError(Arc<anyhow::Error>);

        let (tx, rx) = oneshot::channel();
        self.connection_manager_handle
            .send(ConnectionManagerRequest::Connect(addr, *peer_id, tx))
            .await
            .map_err(|_e| NetworkShutdownError)?;

        let res = rx.await?;
        res.map_err(|e| anyhow::Error::new(ConnectionError(e)))
    }

    fn disconnect(&self, peer_id: &PeerId) -> Result<()> {
        let Some(active_peers) = self.active_peers.upgrade() else {
            anyhow::bail!("network has been shutdown");
        };
        active_peers.remove(peer_id, DisconnectReason::Requested);
        Ok(())
    }

    fn peer(&self, peer_id: &PeerId) -> Option<Peer> {
        let active_peers = self.active_peers.upgrade()?;
        let connection = active_peers.get(peer_id)?;
        Some(Peer::new(connection, self.config.clone()))
    }

    async fn shutdown(&self) -> Result<()> {
        let (sender, receiver) = oneshot::channel();
        self.connection_manager_handle
            .send(ConnectionManagerRequest::Shutdown(sender))
            .await
            .map_err(|_e| NetworkShutdownError)?;
        receiver.await.map_err(Into::into)
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

#[derive(thiserror::Error, Debug)]
#[error("network has been shutdown")]
struct NetworkShutdownError;

#[cfg(test)]
mod tests {
    use futures_util::stream::FuturesUnordered;
    use futures_util::StreamExt;

    use super::*;
    use crate::types::{service_message_fn, service_query_fn, BoxCloneService, PeerInfo, Request};
    use crate::util::NetworkExt;

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

    fn make_network(service_name: &str) -> Result<Network> {
        Network::builder()
            .with_config(NetworkConfig {
                enable_0rtt: true,
                ..Default::default()
            })
            .with_random_private_key()
            .with_service_name(service_name)
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

        let peer1 = make_network("tycho")?;
        let peer2 = make_network("tycho")?;
        let peer3 = make_network("not-tycho")?;

        peer1
            .connect(peer2.local_addr(), peer2.peer_id())
            .await
            .unwrap();
        peer2
            .connect(peer1.local_addr(), peer1.peer_id())
            .await
            .unwrap();

        peer1
            .connect(peer3.local_addr(), peer3.peer_id())
            .await
            .unwrap_err();
        peer2
            .connect(peer3.local_addr(), peer3.peer_id())
            .await
            .unwrap_err();

        peer3
            .connect(peer1.local_addr(), peer1.peer_id())
            .await
            .unwrap_err();
        peer3
            .connect(peer2.local_addr(), peer2.peer_id())
            .await
            .unwrap_err();

        Ok(())
    }

    #[tokio::test]
    async fn simultaneous_queries() -> Result<()> {
        tycho_util::test::init_logger("simultaneous_queries", "debug");

        for _ in 0..10 {
            let peer1 = make_network("tycho")?;
            let peer2 = make_network("tycho")?;

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
                .with_service_name("tycho")
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
                futures.push(left.send(&right.peer_id(), req.clone()));
            }

            while let Some(res) = futures.next().await {
                res?;
            }
        }

        Ok(())
    }
}
