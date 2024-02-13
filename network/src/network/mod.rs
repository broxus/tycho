use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::{Arc, Weak};

use anyhow::Result;
use everscale_crypto::ed25519;
use rand::Rng;
use tokio::sync::{broadcast, mpsc, oneshot};

use self::config::EndpointConfig;
use self::connection_manager::{ConnectionManager, ConnectionManagerRequest};
use self::endpoint::Endpoint;
use crate::types::{
    Address, DisconnectReason, PeerEvent, PeerId, Response, Service, ServiceExt, ServiceRequest,
};

pub use self::config::{NetworkConfig, QuicConfig};
pub use self::connection::{Connection, RecvStream, SendStream};
pub use self::connection_manager::{ActivePeers, KnownPeers, WeakActivePeers};
pub use self::peer::Peer;

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
}

impl<MandatoryFields> NetworkBuilder<MandatoryFields> {
    pub fn with_config(mut self, config: NetworkConfig) -> Self {
        self.optional_fields.config = Some(config);
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
        self.with_private_key(rand::thread_rng().gen())
    }
}

impl NetworkBuilder {
    pub fn build<T: ToSocketAddrs, S>(self, bind_address: T, service: S) -> Result<Network>
    where
        S: Send + Sync + Clone + 'static,
        S: Service<ServiceRequest, QueryResponse = Response>,
    {
        use socket2::{Domain, Protocol, Socket, Type};

        let config = self.optional_fields.config.unwrap_or_default();
        let quic_config = config.quic.clone().unwrap_or_default();
        let (service_name, private_key) = self.mandatory_fields;

        let keypair = ed25519::KeyPair::from(&ed25519::SecretKey::from_bytes(private_key));

        let endpoint_config = EndpointConfig::builder()
            .with_service_name(service_name)
            .with_private_key(private_key)
            .with_transport_config(quic_config.make_transport_config())
            .build()?;

        let socket = 'socket: {
            let mut err = anyhow::anyhow!("no addresses to bind to");
            for addr in bind_address.to_socket_addrs()? {
                let s = Socket::new(Domain::for_address(addr), Type::DGRAM, Some(Protocol::UDP))?;
                if let Err(e) = s.bind(&socket2::SockAddr::from(addr)) {
                    err = e.into();
                } else {
                    break 'socket s;
                }
            }
            return Err(err);
        };

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

    pub async fn connect<T>(&self, addr: T) -> Result<PeerId>
    where
        T: Into<Address>,
    {
        self.0.connect(addr.into(), None).await
    }

    pub async fn connect_with_peer_id<T>(&self, addr: T, peer_id: &PeerId) -> Result<PeerId>
    where
        T: Into<Address>,
    {
        self.0.connect(addr.into(), Some(peer_id)).await
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

    pub fn downgrade(this: &Self) -> WeakNetwork {
        WeakNetwork(Arc::downgrade(&this.0))
    }
}

struct NetworkInner {
    config: Arc<NetworkConfig>,
    endpoint: Arc<Endpoint>,
    active_peers: WeakActivePeers,
    known_peers: KnownPeers,
    connection_manager_handle: mpsc::Sender<ConnectionManagerRequest>,
    keypair: ed25519::KeyPair,
}

impl NetworkInner {
    fn local_addr(&self) -> SocketAddr {
        self.endpoint.local_addr()
    }

    fn peer_id(&self) -> &PeerId {
        self.endpoint.peer_id()
    }

    fn known_peers(&self) -> &KnownPeers {
        &self.known_peers
    }

    async fn connect(&self, addr: Address, peer_id: Option<&PeerId>) -> Result<PeerId> {
        let (tx, rx) = oneshot::channel();
        self.connection_manager_handle
            .send(ConnectionManagerRequest::Connect(
                addr,
                peer_id.copied(),
                tx,
            ))
            .await
            .map_err(|_e| NetworkShutdownError)?;
        rx.await?
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

#[derive(thiserror::Error, Debug)]
#[error("network has been shutdown")]
struct NetworkShutdownError;

#[cfg(test)]
mod tests {
    use tracing_test::traced_test;

    use super::*;
    use crate::types::{service_query_fn, BoxCloneService};

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

    #[tokio::test]
    #[traced_test]
    async fn connection_manager_works() -> anyhow::Result<()> {
        let peer1 = Network::builder()
            .with_random_private_key()
            .with_service_name("tycho")
            .build("127.0.0.1:0", echo_service())?;

        let peer2 = Network::builder()
            .with_random_private_key()
            .with_service_name("tycho")
            .build("127.0.0.1:0", echo_service())?;

        let peer3 = Network::builder()
            .with_random_private_key()
            .with_service_name("not-tycho")
            .build("127.0.0.1:0", echo_service())?;

        assert!(peer1.connect(peer2.local_addr()).await.is_ok());
        assert!(peer2.connect(peer1.local_addr()).await.is_ok());

        assert!(peer1.connect(peer3.local_addr()).await.is_err());
        assert!(peer2.connect(peer3.local_addr()).await.is_err());

        assert!(peer3.connect(peer1.local_addr()).await.is_err());
        assert!(peer3.connect(peer2.local_addr()).await.is_err());

        Ok(())
    }
}
