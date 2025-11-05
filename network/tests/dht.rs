//! Run tests with this env:
//! ```text
//! RUST_LOG=info,tycho_network=trace
//! ```
use std::collections::BTreeMap;
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Duration;
use anyhow::Result;
use tl_proto::{TlRead, TlWrite};
use tycho_crypto::ed25519;
use tycho_network::{
    DhtClient, DhtConfig, DhtService, FindValueError, Network, PeerInfo, Router, proto,
};
use tycho_util::time::now_sec;
struct Node {
    network: Network,
    dht: DhtClient,
}
impl Node {
    fn with_random_key(spawn_dht_tasks: bool) -> Self {
        let key = rand::random::<ed25519::SecretKey>();
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
            .build((Ipv4Addr::LOCALHOST, 0), router)
            .unwrap();
        if spawn_dht_tasks {
            dht_tasks.spawn(&network);
        }
        let dht = dht_service.make_client(&network);
        Self { network, dht }
    }
}
fn make_network(
    node_count: usize,
    spawn_dht_tasks: bool,
) -> (Vec<Node>, Vec<Arc<PeerInfo>>) {
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
    let mut __guard = crate::__async_profile_guard__::Guard::new(
        concat!(module_path!(), "::", stringify!(bootstrap_nodes_accessible)),
        file!(),
        77u32,
    );
    tycho_util::test::init_logger("bootstrap_nodes_accessible", "debug");
    let (nodes, _) = make_network(5, true);
    for i in 0..nodes.len() {
        __guard.checkpoint(82u32);
        for j in 0..nodes.len() {
            __guard.checkpoint(83u32);
            if i == j {
                {
                    __guard.end_section(85u32);
                    __guard.start_section(85u32);
                    continue;
                };
            }
            let left = &nodes[i];
            let right = &nodes[j];
            {
                __guard.end_section(90u32);
                let __result = left.dht.get_node_info(right.network.peer_id()).await;
                __guard.start_section(90u32);
                __result
            }?;
        }
    }
    Ok(())
}
#[tokio::test]
async fn bootstrap_nodes_store_value() -> Result<()> {
    let mut __guard = crate::__async_profile_guard__::Guard::new(
        concat!(module_path!(), "::", stringify!(bootstrap_nodes_store_value)),
        file!(),
        98u32,
    );
    tycho_util::test::init_logger("bootstrap_nodes_store_value", "debug");
    #[derive(Debug, Clone, PartialEq, Eq, TlWrite, TlRead)]
    struct SomeValue(u32);
    const VALUE: SomeValue = SomeValue(123123);
    let (nodes, _) = make_network(5, false);
    let first = &nodes[0].dht;
    {
        __guard.end_section(116u32);
        let __result = first
            .entry(proto::dht::PeerValueKeyName::NodeInfo)
            .with_data(VALUE)
            .with_time(now_sec())
            .store()
            .await;
        __guard.start_section(116u32);
        __result
    }?;
    let value = {
        __guard.end_section(122u32);
        let __result = first
            .entry(proto::dht::PeerValueKeyName::NodeInfo)
            .find_value::<SomeValue>(first.network().peer_id())
            .await;
        __guard.start_section(122u32);
        __result
    }?;
    assert_eq!(value, VALUE);
    let res = {
        __guard.end_section(129u32);
        let __result = first
            .entry(proto::dht::PeerValueKeyName::NodeInfo)
            .find_peer_value_raw(nodes[1].network.peer_id())
            .await;
        __guard.start_section(129u32);
        __result
    };
    assert!(matches!(res, Err(FindValueError::NotFound)));
    Ok(())
}
#[tokio::test]
async fn connect_new_node_to_bootstrap() -> Result<()> {
    let mut __guard = crate::__async_profile_guard__::Guard::new(
        concat!(module_path!(), "::", stringify!(connect_new_node_to_bootstrap)),
        file!(),
        136u32,
    );
    tycho_util::test::init_logger("connect_new_node_to_bootstrap", "debug");
    #[derive(Debug, Clone, PartialEq, Eq, TlWrite, TlRead)]
    struct SomeValue(u32);
    const VALUE: SomeValue = SomeValue(123123);
    let (bootstrap_nodes, global_config) = make_network(5, true);
    let node = Node::with_random_key(false);
    for peer_info in &global_config {
        __guard.checkpoint(147u32);
        node.dht.add_peer(peer_info.clone())?;
    }
    let mut somebody_knows_the_peer = false;
    for bootstrap_node in &bootstrap_nodes {
        __guard.checkpoint(153u32);
        somebody_knows_the_peer
            |= bootstrap_node.network.known_peers().contains(node.network.peer_id());
    }
    assert!(! somebody_knows_the_peer);
    {
        __guard.end_section(167u32);
        let __result = node
            .dht
            .entry(proto::dht::PeerValueKeyName::NodeInfo)
            .with_data(VALUE)
            .with_peer_info(true)
            .store()
            .await;
        __guard.start_section(167u32);
        __result
    }?;
    let mut somebody_knows_the_peer = false;
    for bootstrap_node in &bootstrap_nodes {
        __guard.checkpoint(171u32);
        somebody_knows_the_peer
            |= bootstrap_node.network.known_peers().contains(node.network.peer_id());
    }
    assert!(somebody_knows_the_peer);
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn startup_from_single_bootstrap_node() -> Result<()> {
    let mut __guard = crate::__async_profile_guard__::Guard::new(
        concat!(module_path!(), "::", stringify!(startup_from_single_bootstrap_node)),
        file!(),
        183u32,
    );
    tycho_util::test::init_logger("startup_from_single_bootstrap_node", "debug");
    #[derive(Debug, Default)]
    struct PeerState {
        knows_about: usize,
        known_by: usize,
        is_bootstrap: bool,
    }
    const NODE_COUNT: usize = 20;
    let nodes = (0..NODE_COUNT).map(|_| Node::with_random_key(true)).collect::<Vec<_>>();
    let first_node_info = nodes
        .first()
        .map(|node| Arc::new(node.network.sign_peer_info(0, u32::MAX)))
        .unwrap();
    for node in &nodes {
        __guard.checkpoint(205u32);
        node.dht.add_peer(first_node_info.clone()).unwrap();
    }
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    for _ in 0..60 {
        __guard.checkpoint(211u32);
        {
            __guard.end_section(212u32);
            let __result = interval.tick().await;
            __guard.start_section(212u32);
            __result
        };
        let mut states = BTreeMap::<_, PeerState>::new();
        let mut all_known = true;
        for (i, left) in nodes.iter().enumerate() {
            __guard.checkpoint(217u32);
            for (j, right) in nodes.iter().enumerate() {
                __guard.checkpoint(218u32);
                if i == j {
                    {
                        __guard.end_section(220u32);
                        __guard.start_section(220u32);
                        continue;
                    };
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
        println!("known: {states:#?}");
        if all_known {
            {
                __guard.end_section(240u32);
                return Ok(());
            };
        }
    }
    Err(anyhow::anyhow!("failed to distribute peers info whithin the time limit"))
}
