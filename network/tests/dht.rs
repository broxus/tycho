//! Run tests with this env:
//! ```text
//! RUST_LOG=info,tycho_network=trace
//! ```

use std::net::Ipv4Addr;
use std::sync::Arc;

use anyhow::Result;
use everscale_crypto::ed25519;
use tl_proto::{TlRead, TlWrite};
use tycho_network::{proto, DhtClient, DhtService, FindValueError, Network, PeerInfo, Router};
use tycho_util::time::now_sec;

struct Node {
    network: Network,
    dht: DhtClient,
}

impl Node {
    fn with_random_key() -> Self {
        let key = ed25519::SecretKey::generate(&mut rand::thread_rng());
        let local_id = ed25519::PublicKey::from(&key).into();

        let (dht_tasks, dht_service) = DhtService::builder(local_id).build();

        let router = Router::builder().route(dht_service.clone()).build();

        let network = Network::builder()
            .with_private_key(key.to_bytes())
            .with_service_name("test-service")
            .build((Ipv4Addr::LOCALHOST, 0), router)
            .unwrap();

        dht_tasks.spawn(&network);

        let dht = dht_service.make_client(&network);

        Self { network, dht }
    }
}

fn make_network(node_count: usize) -> (Vec<Node>, Vec<Arc<PeerInfo>>) {
    let nodes = (0..node_count)
        .map(|_| Node::with_random_key())
        .collect::<Vec<_>>();

    let bootstrap_info = nodes
        .iter()
        .map(|node| Arc::new(node.network.sign_peer_info(0, u32::MAX)))
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

    let node = Node::with_random_key();
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
