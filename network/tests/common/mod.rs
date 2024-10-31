use std::net::Ipv4Addr;
use std::time::Duration;

use everscale_crypto::ed25519;
use tl_proto::{TlRead, TlWrite};
use tycho_network::{
    DhtConfig, DhtService, Network, OverlayConfig, OverlayService, PeerResolver, Response, Router,
    Service, ServiceRequest,
};

pub struct NodeBase {
    pub network: Network,
    pub dht_service: DhtService,
    pub overlay_service: OverlayService,
    pub peer_resolver: PeerResolver,
}

impl NodeBase {
    pub fn with_random_key() -> Self {
        let key = ed25519::SecretKey::generate(&mut rand::thread_rng());
        let local_id = ed25519::PublicKey::from(&key).into();

        let (dht_tasks, dht_service) = DhtService::builder(local_id)
            .with_config(make_fast_dht_config())
            .build();

        let (overlay_tasks, overlay_service) = OverlayService::builder(local_id)
            .with_config(make_fast_overlay_config())
            .with_dht_service(dht_service.clone())
            .build();

        let router = Router::builder()
            .route(dht_service.clone())
            .route(overlay_service.clone())
            .build();

        let network = Network::builder()
            .with_private_key(key.to_bytes())
            .build((Ipv4Addr::LOCALHOST, 0), router)
            .unwrap();

        dht_tasks.spawn(&network);
        overlay_tasks.spawn(&network);

        let peer_resolver = dht_service.make_peer_resolver().build(&network);

        Self {
            network,
            dht_service,
            overlay_service,
            peer_resolver,
        }
    }
}

pub fn make_fast_dht_config() -> DhtConfig {
    DhtConfig {
        local_info_announce_period: Duration::from_secs(1),
        local_info_announce_period_max_jitter: Duration::from_secs(1),
        routing_table_refresh_period: Duration::from_secs(1),
        routing_table_refresh_period_max_jitter: Duration::from_secs(1),
        ..Default::default()
    }
}

pub fn make_fast_overlay_config() -> OverlayConfig {
    OverlayConfig {
        public_overlay_peer_store_period: Duration::from_secs(1),
        public_overlay_peer_store_max_jitter: Duration::from_secs(1),
        public_overlay_peer_exchange_period: Duration::from_secs(1),
        public_overlay_peer_exchange_max_jitter: Duration::from_secs(1),
        public_overlay_peer_discovery_period: Duration::from_secs(1),
        public_overlay_peer_discovery_max_jitter: Duration::from_secs(1),
        ..Default::default()
    }
}

pub struct PingPongService;

impl Service<ServiceRequest> for PingPongService {
    type QueryResponse = Response;
    type OnQueryFuture = futures_util::future::Ready<Option<Self::QueryResponse>>;
    type OnMessageFuture = futures_util::future::Ready<()>;
    type OnDatagramFuture = futures_util::future::Ready<()>;

    fn on_query(&self, req: ServiceRequest) -> Self::OnQueryFuture {
        futures_util::future::ready(match req.parse_tl() {
            Ok(Ping { value }) => Some(Response::from_tl(Pong { value })),
            Err(e) => {
                tracing::error!(
                    peer_id = %req.metadata.peer_id,
                    addr = %req.metadata.remote_address,
                    "invalid request: {e:?}",
                );
                None
            }
        })
    }

    #[inline]
    fn on_message(&self, _req: ServiceRequest) -> Self::OnMessageFuture {
        futures_util::future::ready(())
    }

    #[inline]
    fn on_datagram(&self, _req: ServiceRequest) -> Self::OnDatagramFuture {
        futures_util::future::ready(())
    }
}

#[derive(Debug, Copy, Clone, TlRead, TlWrite)]
#[tl(boxed, id = 0x11223344)]
pub struct Ping {
    pub value: u64,
}

#[derive(Debug, Copy, Clone, TlRead, TlWrite)]
#[tl(boxed, id = 0x55667788)]
pub struct Pong {
    pub value: u64,
}
