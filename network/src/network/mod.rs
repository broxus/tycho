use std::convert::Infallible;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use rand::Rng;
use tokio::sync::{mpsc, oneshot};
use tower::ServiceExt;

use crate::config::{Config, EndpointConfig};
use crate::endpoint::Endpoint;
use crate::types::{Address, DisconnectReason, InboundServiceRequest, PeerId, Response};

use self::connection_manager::{
    ActivePeers, ConnectionManager, ConnectionManagerRequest, KnownPeers, WeakActivePeers,
};
use self::peer::Peer;

pub mod connection_manager;
pub mod peer;
pub mod request_handler;

pub struct Builder<MandatoryFields = (String, [u8; 32])> {
    mandatory_fields: MandatoryFields,
    optional_fields: BuilderFields,
}

#[derive(Default)]
struct BuilderFields {
    config: Option<Config>,
}

impl<MandatoryFields> Builder<MandatoryFields> {
    pub fn with_config(mut self, config: Config) -> Self {
        self.optional_fields.config = Some(config);
        self
    }
}

impl<T2> Builder<((), T2)> {
    pub fn with_service_name<T: Into<String>>(self, name: T) -> Builder<(String, T2)> {
        let (_, private_key) = self.mandatory_fields;
        Builder {
            mandatory_fields: (name.into(), private_key),
            optional_fields: self.optional_fields,
        }
    }
}

impl<T1> Builder<(T1, ())> {
    pub fn with_private_key(self, private_key: [u8; 32]) -> Builder<(T1, [u8; 32])> {
        let (service_name, _) = self.mandatory_fields;
        Builder {
            mandatory_fields: (service_name, private_key),
            optional_fields: self.optional_fields,
        }
    }

    pub fn with_random_private_key(self) -> Builder<(T1, [u8; 32])> {
        self.with_private_key(rand::thread_rng().gen())
    }
}

impl Builder {
    pub fn build<T: ToSocketAddrs, S>(self, bind_address: T, service: S) -> Result<Network>
    where
        S: Clone + Send + 'static,
        S: tower::Service<
            InboundServiceRequest<Bytes>,
            Response = Response<Bytes>,
            Error = Infallible,
        >,
        <S as tower::Service<InboundServiceRequest<Bytes>>>::Future: Send + 'static,
    {
        use socket2::{Domain, Protocol, Socket, Type};

        let config = self.optional_fields.config.unwrap_or_default();
        let quic_config = config.quic.clone().unwrap_or_default();
        let (service_name, private_key) = self.mandatory_fields;

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
            }
        });

        Ok(Network(inner))
    }
}

pub struct Network(Arc<NetworkInner>);

impl Network {
    pub fn builder() -> Builder<((), ())> {
        Builder {
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
}

pub struct NetworkInner {
    config: Arc<Config>,
    endpoint: Arc<Endpoint>,
    active_peers: WeakActivePeers,
    known_peers: KnownPeers,
    connection_manager_handle: mpsc::Sender<ConnectionManagerRequest>,
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
            .map_err(|_e| anyhow::anyhow!("network has been shutdown"))?;
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
}

#[cfg(test)]
mod tests {
    use tower::util::BoxCloneService;
    use tracing_test::traced_test;

    use super::*;

    fn echo_service() -> BoxCloneService<InboundServiceRequest<Bytes>, Response<Bytes>, Infallible>
    {
        let handle = |request: InboundServiceRequest<Bytes>| async move {
            tracing::trace!("received: {}", request.body.escape_ascii());
            let response = Response {
                version: Default::default(),
                body: request.body,
            };
            Ok::<_, Infallible>(response)
        };
        tower::service_fn(handle).boxed_clone()
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
