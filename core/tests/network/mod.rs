use std::collections::BTreeMap;
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use tycho_core::blockchain_rpc::BlockchainRpcService;
use tycho_core::storage::CoreStorage;
use tycho_crypto::ed25519;
use tycho_network::{
    DhtClient, DhtConfig, DhtService, Network, OverlayConfig, OverlayId, OverlayService, PeerId,
    PeerResolver, PublicOverlay, Router,
};

pub struct NodeBase {
    pub network: Network,
    pub dht_service: DhtService,
    pub overlay_service: OverlayService,
    pub peer_resolver: PeerResolver,
}

impl NodeBase {
    pub fn with_random_key() -> Self {
        let key = rand::random::<ed25519::SecretKey>();
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

        dht_tasks.spawn_without_bootstrap(&network);
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

fn make_fast_dht_config() -> DhtConfig {
    DhtConfig {
        local_info_announce_period: Duration::from_secs(1),
        local_info_announce_period_max_jitter: Duration::from_secs(1),
        routing_table_refresh_period: Duration::from_secs(1),
        routing_table_refresh_period_max_jitter: Duration::from_secs(1),
        ..Default::default()
    }
}

fn make_fast_overlay_config() -> OverlayConfig {
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

    fn with_random_key(storage: CoreStorage) -> Self {
        let NodeBase {
            network,
            dht_service,
            overlay_service,
            peer_resolver,
        } = NodeBase::with_random_key();
        let public_overlay = PublicOverlay::builder(PUBLIC_OVERLAY_ID)
            .with_peer_resolver(peer_resolver.clone())
            .build(
                BlockchainRpcService::builder()
                    .with_storage(storage)
                    .without_broadcast_listener()
                    .build(),
            );
        overlay_service.add_public_overlay(&public_overlay);

        let dht_client = dht_service.make_client(&network);

        Self {
            network,
            public_overlay,
            dht_client,
        }
    }
}

pub fn make_network(storage: CoreStorage, node_count: usize) -> Vec<Node> {
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

#[allow(dead_code)]
pub trait TestNode {
    fn network(&self) -> &Network;
    fn public_overlay(&self) -> &PublicOverlay;
    fn force_update_validators(&self, peers: Vec<PeerId>);
}

impl TestNode for Node {
    fn network(&self) -> &Network {
        self.network()
    }

    fn public_overlay(&self) -> &PublicOverlay {
        self.public_overlay()
    }

    fn force_update_validators(&self, _: Vec<PeerId>) {}
}

#[allow(dead_code)]
pub async fn discover<N: TestNode>(nodes: &[N]) -> anyhow::Result<()> {
    tracing::info!("discovering nodes");
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;

        let mut peer_states = BTreeMap::<&PeerId, PeerState>::new();

        for (i, left) in nodes.iter().enumerate() {
            for (j, right) in nodes.iter().enumerate() {
                if i == j {
                    continue;
                }

                let left_id = left.network().peer_id();
                let right_id = right.network().peer_id();

                if left.public_overlay().read_entries().contains(right_id) {
                    peer_states.entry(left_id).or_default().knows_about += 1;
                    peer_states.entry(right_id).or_default().known_by += 1;
                }
            }
        }

        tracing::info!("{peer_states:#?}");

        let total_filled = peer_states
            .values()
            .filter(|state| state.knows_about == nodes.len() - 1)
            .count();

        tracing::info!(
            "peers with filled overlay: {} / {}",
            total_filled,
            nodes.len()
        );
        if total_filled == nodes.len() {
            break;
        }
    }

    tracing::info!("resolving entries...");
    for node in nodes {
        let resolved = FuturesUnordered::new();
        for entry in node.public_overlay().read_entries().iter() {
            let handle = entry.resolver_handle.clone();
            resolved.push(async move { handle.wait_resolved().await });
        }

        // Ensure all entries are resolved.
        resolved.collect::<Vec<_>>().await;
        tracing::info!(
            peer_id = %node.network().peer_id(),
            "all entries resolved",
        );
    }

    Ok(())
}

#[derive(Debug, Default)]
struct PeerState {
    knows_about: usize,
    known_by: usize,
}

static PUBLIC_OVERLAY_ID: OverlayId = OverlayId([1; 32]);
