//! Run tests with this env:
//! ```text
//! RUST_LOG=info,tycho_network=trace
//! ```

use std::net::Ipv4Addr;
use std::sync::Arc;

use anyhow::Result;
use everscale_crypto::ed25519;
use tl_proto::{TlRead, TlWrite};
use tycho_network::{
    proto, Address, DhtClient, DhtService, FindValueError, Network, PeerId, PeerInfo, Router,
};
use tycho_util::time::now_sec;

struct Node {
    network: Network,
    dht: DhtClient,
}

impl Node {
    fn new(key: &ed25519::SecretKey) -> Self {
        let keypair = ed25519::KeyPair::from(key);

        let (dht_tasks, dht_service) = DhtService::builder(keypair.public_key.into()).build();

        let router = Router::builder().route(dht_service.clone()).build();

        let network = Network::builder()
            .with_private_key(key.to_bytes())
            .with_service_name("test-service")
            .build((Ipv4Addr::LOCALHOST, 0), router)
            .unwrap();

        dht_tasks.spawn(&network);

        let dht = dht_service.make_client(network.clone());

        Self { network, dht }
    }

    fn make_peer_info(key: &ed25519::SecretKey, address: Address) -> PeerInfo {
        let keypair = ed25519::KeyPair::from(key);
        let peer_id = PeerId::from(keypair.public_key);

        let now = now_sec();
        let mut node_info = PeerInfo {
            id: peer_id,
            address_list: vec![address].into_boxed_slice(),
            created_at: now,
            expires_at: u32::MAX,
            signature: Box::new([0; 64]),
        };
        *node_info.signature = keypair.sign(&node_info);
        node_info
    }
}

fn make_network(node_count: usize) -> (Vec<Node>, Vec<Arc<PeerInfo>>) {
    let keys = (0..node_count)
        .map(|_| ed25519::SecretKey::generate(&mut rand::thread_rng()))
        .collect::<Vec<_>>();

    let nodes = keys.iter().map(Node::new).collect::<Vec<_>>();

    let bootstrap_info = std::iter::zip(&keys, &nodes)
        .map(|(key, node)| Arc::new(Node::make_peer_info(key, node.network.local_addr().into())))
        .collect::<Vec<_>>();
    for node in &nodes {
        for info in &bootstrap_info {
            node.dht.add_peer(info.clone()).unwrap();
        }
    }

    (nodes, bootstrap_info)
}

#[tokio::test]
async fn bootstrap_nodes_accessible() -> Result<()> {
    tracing_subscriber::fmt::try_init().ok();
    tracing::info!("bootstrap_nodes_accessible");

    let (nodes, _) = make_network(5);

    for i in 0..nodes.len() {
        for j in 0..nodes.len() {
            if i == j {
                continue;
            }

            let left = &nodes[i];
            let right = &nodes[j];
            left.dht.get_node_info(right.network.peer_id()).await?;
        }
    }

    Ok(())
}

#[tokio::test]
async fn bootstrap_nodes_store_value() -> Result<()> {
    tracing_subscriber::fmt::try_init().ok();
    tracing::info!("bootstrap_nodes_store_value");

    #[derive(Debug, Clone, PartialEq, Eq, TlWrite, TlRead)]
    struct SomeValue(u32);

    const VALUE: SomeValue = SomeValue(123123);

    let (nodes, _) = make_network(5);

    // Store value
    let first = &nodes[0].dht;

    first
        .entry(proto::dht::PeerValueKeyName::NodeInfo)
        .with_data(VALUE)
        .with_time(now_sec())
        .store()
        .await?;

    // Retrieve an existing value
    let value = first
        .entry(proto::dht::PeerValueKeyName::NodeInfo)
        .find_value::<SomeValue>(&first.network().peer_id())
        .await?;
    assert_eq!(value, VALUE);

    // Retrieve a non-existing value
    let res = first
        .entry(proto::dht::PeerValueKeyName::NodeInfo)
        .find_peer_value_raw(nodes[1].network.peer_id())
        .await;
    assert!(matches!(res, Err(FindValueError::NotFound)));

    Ok(())
}

#[tokio::test]
async fn connect_new_node_to_bootstrap() -> Result<()> {
    tracing_subscriber::fmt::try_init().ok();
    tracing::info!("connect_new_node_to_bootstrap");

    #[derive(Debug, Clone, PartialEq, Eq, TlWrite, TlRead)]
    struct SomeValue(u32);

    const VALUE: SomeValue = SomeValue(123123);

    let (bootstrap_nodes, global_config) = make_network(5);

    let node = Node::new(&ed25519::SecretKey::generate(&mut rand::thread_rng()));
    for peer_info in &global_config {
        node.dht.add_peer(peer_info.clone())?;
    }

    // Ensure that the node is not known by the bootstrap nodes
    let mut somebody_knows_the_peer = false;
    for bootstrap_node in &bootstrap_nodes {
        somebody_knows_the_peer |= bootstrap_node
            .network
            .known_peers()
            .contains(node.network.peer_id());
    }
    assert!(!somebody_knows_the_peer);

    // Store value and announce the peer info
    node.dht
        .entry(proto::dht::PeerValueKeyName::NodeInfo)
        .with_data(VALUE)
        .with_peer_info(true)
        .store()
        .await?;

    // The node must be known by some bootstrap nodes now
    let mut somebody_knows_the_peer = false;
    for bootstrap_node in &bootstrap_nodes {
        somebody_knows_the_peer |= bootstrap_node
            .network
            .known_peers()
            .contains(node.network.peer_id());
    }
    assert!(somebody_knows_the_peer);

    Ok(())
}
