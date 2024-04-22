use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Duration;

use everscale_crypto::ed25519;
use tycho_core::overlay_server::OverlayServer;

use tycho_network::{
    DhtClient, DhtConfig, DhtService, Network, OverlayConfig, OverlayId, OverlayService, PeerId,
    PeerResolver, PublicOverlay, Request, Router,
};
use tycho_storage::Storage;

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
            .with_service_name("test-service")
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

pub struct Node {
    network: Network,
    public_overlay: PublicOverlay,
    dht_client: DhtClient,
}

impl Node {
    pub fn network(&self) -> &Network {
        &self.network
    }

    pub fn public_overlay(&self) -> &PublicOverlay {
        &self.public_overlay
    }

    fn with_random_key(storage: Arc<Storage>) -> Self {
        let NodeBase {
            network,
            dht_service,
            overlay_service,
            peer_resolver,
        } = NodeBase::with_random_key();
        let public_overlay = PublicOverlay::builder(PUBLIC_OVERLAY_ID)
            .with_peer_resolver(peer_resolver)
            .build(OverlayServer::new(storage, true));
        overlay_service.add_public_overlay(&public_overlay);

        let dht_client = dht_service.make_client(&network);

        Self {
            network,
            public_overlay,
            dht_client,
        }
    }
}

pub fn make_network(storage: Arc<Storage>, node_count: usize) -> Vec<Node> {
    let nodes = (0..node_count)
        .map(|_| Node::with_random_key(storage.clone()))
        .collect::<Vec<_>>();

    let common_peer_info = nodes.first().unwrap().network.sign_peer_info(0, u32::MAX);

    for node in &nodes {
        node.dht_client
            .add_peer(Arc::new(common_peer_info.clone()))
            .unwrap();
    }

    nodes
}

pub static PUBLIC_OVERLAY_ID: OverlayId = OverlayId([1; 32]);
