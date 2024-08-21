//! Run tests with this env:
//! ```text
//! RUST_LOG=info,tycho_network=trace
//! ```

use std::collections::BTreeMap;
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use everscale_crypto::ed25519;
use tl_proto::{TlRead, TlWrite};
use tycho_network::{
    proto, DhtClient, DhtConfig, DhtService, FindValueError, Network, PeerInfo, Router,
};
use tycho_util::time::now_sec;

struct Node {
    network: Network,
    dht: DhtClient,
}

impl Node {
    fn with_random_key(spawn_dht_tasks: bool) -> Self {
        let key = ed25519::SecretKey::generate(&mut rand::thread_rng());
        let local_id = ed25519::PublicKey::from(&key).into();

        let (dht_tasks, dht_service) = DhtService::builder(local_id)
            .with_config(DhtConfig {
                max_k: 20,
                routing_table_refresh_period: Duration::from_secs(1),
                routing_table_refresh_period_max_jitter: Duration::from_secs(1),
                local_info_announce_period: Duration::from_secs(1),
                local_info_announce_period_max_jitter: Duration::from_secs(1),
                ..Default::default()
            })
            .build();

        let router = Router::builder().route(dht_service.clone()).build();

        let network = Network::builder()
            .with_private_key(key.to_bytes())
            .with_service_name("test-service")
            .build((Ipv4Addr::LOCALHOST, 0), router)
            .unwrap();

        if spawn_dht_tasks {
            dht_tasks.spawn(&network);
        }

        let dht = dht_service.make_client(&network);

        Self { network, dht }
    }
}

fn make_network(node_count: usize, spawn_dht_tasks: bool) -> (Vec<Node>, Vec<Arc<PeerInfo>>) {
    let nodes = (0..node_count)
        .map(|_| Node::with_random_key(spawn_dht_tasks))
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
    tycho_util::test::init_logger("bootstrap_nodes_accessible", "debug");

    let (nodes, _) = make_network(5, true);

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
    tycho_util::test::init_logger("bootstrap_nodes_store_value", "debug");

    #[derive(Debug, Clone, PartialEq, Eq, TlWrite, TlRead)]
    struct SomeValue(u32);

    const VALUE: SomeValue = SomeValue(123123);

    let (nodes, _) = make_network(5, false);

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
        .find_value::<SomeValue>(first.network().peer_id())
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
    tycho_util::test::init_logger("connect_new_node_to_bootstrap", "debug");

    #[derive(Debug, Clone, PartialEq, Eq, TlWrite, TlRead)]
    struct SomeValue(u32);

    const VALUE: SomeValue = SomeValue(123123);

    let (bootstrap_nodes, global_config) = make_network(5, true);

    let node = Node::with_random_key(false);
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn startup_from_single_bootstrap_node() -> Result<()> {
    tycho_util::test::init_logger("startup_from_single_bootstrap_node", "debug");

    #[derive(Debug, Default)]
    struct PeerState {
        knows_about: usize,
        known_by: usize,
        is_bootstrap: bool,
    }

    // Create network with only one bootstrap node available
    const NODE_COUNT: usize = 20;

    let nodes = (0..NODE_COUNT)
        .map(|_| Node::with_random_key(true))
        .collect::<Vec<_>>();

    let first_node_info = nodes
        .first()
        .map(|node| Arc::new(node.network.sign_peer_info(0, u32::MAX)))
        .unwrap();

    for node in &nodes {
        node.dht.add_peer(first_node_info.clone()).unwrap();
    }

    // Start the network and wait until all nodes are known to everyone
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    for _ in 0..60 {
        interval.tick().await;

        let mut states = BTreeMap::<_, PeerState>::new();

        let mut all_known = true;
        for (i, left) in nodes.iter().enumerate() {
            for (j, right) in nodes.iter().enumerate() {
                if i == j {
                    continue;
                }

                let left_id = left.network.peer_id();
                let right_id = right.network.peer_id();

                if left.dht.service().has_peer(right_id) {
                    states.entry(left_id).or_default().knows_about += 1;
                    states.entry(right_id).or_default().known_by += 1;
                } else {
                    all_known = false;
                }
            }
        }

        states.entry(&first_node_info.id).or_default().is_bootstrap = true;

        println!("known: {:#?}", states);

        if all_known {
            return Ok(());
        }
    }

    Err(anyhow::anyhow!(
        "failed to distribute peers info whithin the time limit"
    ))
}
