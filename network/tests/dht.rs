use std::net::Ipv4Addr;
use std::sync::Arc;

use anyhow::Result;
use everscale_crypto::ed25519;
use tycho_network::{proto, Address, AddressList, DhtClient, DhtService, Network, PeerId, Router};
use tycho_util::time::now_sec;

struct Node {
    network: Network,
    dht: DhtClient,
}

impl Node {
    fn new(key: &ed25519::SecretKey) -> Result<Self> {
        let keypair = everscale_crypto::ed25519::KeyPair::from(key);

        let (dht_client, dht) = DhtService::builder(keypair.public_key.into())
            .with_storage(|builder| builder)
            .build();

        let router = Router::builder().route(dht).build();

        let network = Network::builder()
            .with_private_key(key.to_bytes())
            .with_service_name("test-service")
            .build((Ipv4Addr::LOCALHOST, 0), router)
            .unwrap();

        let dht = dht_client.build(network.clone());

        Ok(Self { network, dht })
    }

    fn make_node_info(key: &ed25519::SecretKey, address: Address) -> proto::dht::NodeInfo {
        const TTL: u32 = 3600;

        let keypair = ed25519::KeyPair::from(key);
        let peer_id = PeerId::from(keypair.public_key);

        let now = now_sec();
        let mut node_info = proto::dht::NodeInfo {
            id: peer_id,
            address_list: AddressList {
                items: vec![address],
                created_at: now,
                expires_at: now + TTL,
            },
            created_at: now,
            signature: Default::default(),
        };
        node_info.signature = keypair.sign(&node_info).to_vec().into();
        node_info
    }
}

fn make_network(node_count: usize) -> Vec<Node> {
    let keys = (0..node_count)
        .map(|i| ed25519::SecretKey::generate(&mut rand::thread_rng()))
        .collect::<Vec<_>>();

    let nodes = keys
        .iter()
        .map(Node::new)
        .collect::<Result<Vec<_>>>()
        .unwrap();

    let bootstrap_info = std::iter::zip(&keys, &nodes)
        .map(|(key, node)| Arc::new(Node::make_node_info(key, node.network.local_addr().into())))
        .collect::<Vec<_>>();
    for node in &nodes {
        for info in &bootstrap_info {
            node.dht.add_peer(info.clone()).unwrap();
        }
    }

    nodes
}

#[tokio::test]
async fn bootstrap_nodes_accessible() -> Result<()> {
    tracing_subscriber::fmt::init();

    let nodes = make_network(5);

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
