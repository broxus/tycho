use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use socket2::{Domain, Socket};
use tokio::sync::{mpsc, oneshot};

use crate::config::{Config, EndpointConfig};
use crate::endpoint::Endpoint;
use crate::types::{DisconnectReason, PeerId};

use self::connection_manager::{
    ActivePeers, ConnectionManager, ConnectionManagerRequest, KnownPeers, WeakActivePeers,
};
use self::peer::Peer;

pub mod connection_manager;
pub mod peer;

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
    pub fn with_server_name<T: Into<String>>(self, name: T) -> Builder<(String, T2)> {
        let (_, private_key) = self.mandatory_fields;
        Builder {
            mandatory_fields: (name.into(), private_key),
            optional_fields: self.optional_fields,
        }
    }
}

impl<T1> Builder<(T1, ())> {
    pub fn with_private_key(self, private_key: [u8; 32]) -> Builder<(T1, [u8; 32])> {
        let (server_name, _) = self.mandatory_fields;
        Builder {
            mandatory_fields: (server_name, private_key),
            optional_fields: self.optional_fields,
        }
    }
}

impl Builder {
    pub fn build(self, bind_address: SocketAddr) -> Result<Network> {
        let config = self.optional_fields.config.unwrap_or_default();
        let quic_config = config.quic.clone().unwrap_or_default();
        let (server_name, private_key) = self.mandatory_fields;

        let endpoint_config = EndpointConfig::builder()
            .with_server_name(server_name)
            .with_private_key(private_key)
            .with_transport_config(quic_config.make_transport_config())
            .build()?;

        let socket = Socket::new(
            Domain::for_address(bind_address),
            socket2::Type::DGRAM,
            Some(socket2::Protocol::UDP),
        )?;

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

        let (connection_manager, connection_manager_handle) = ConnectionManager::new(
            config.clone(),
            endpoint.clone(),
            active_peers,
            known_peers.clone(),
        );

        tokio::spawn(connection_manager.start());

        Ok(Network(Arc::new(NetworkInner {
            config,
            endpoint,
            active_peers: weak_active_peers,
            known_peers,
            connection_manager_handle,
        })))
    }
}

pub struct Network(Arc<NetworkInner>);

impl Network {
    pub fn builder<T: Into<String>>() -> Builder<((), ())> {
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

    pub async fn connect(&self, addr: SocketAddr) -> Result<PeerId> {
        self.0.connect(addr, None).await
    }

    pub async fn connect_with_peer_id(&self, addr: SocketAddr, peer_id: &PeerId) -> Result<PeerId> {
        self.0.connect(addr, Some(peer_id)).await
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

    async fn connect(&self, addr: SocketAddr, peer_id: Option<&PeerId>) -> Result<PeerId> {
        let (tx, rx) = oneshot::channel();
        self.connection_manager_handle
            .send(ConnectionManagerRequest::Connect(
                addr,
                peer_id.copied(),
                tx,
            ))
            .await
            .map_err(|_| anyhow::anyhow!("network has been shutdown"))?;
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
