//! Run tests with this env:
//! ```text
//! RUST_LOG=info,tycho_network=trace
//! ```

use std::net::Ipv4Addr;
use std::sync::Arc;

use anyhow::Result;
use everscale_crypto::ed25519;
use tl_proto::{TlRead, TlWrite};
use tycho_network::proto::dht;
use tycho_network::{
    Address, AddressList, DhtClient, DhtService, FindValueError, Network, PeerId, Router,
};
use tycho_util::time::now_sec;

struct Node {
    network: Network,
    dht: DhtClient,
}

impl Node {
    fn new(key: &ed25519::SecretKey) -> Result<Self> {
        let keypair = everscale_crypto::ed25519::KeyPair::from(key);

        let (dht_client, dht) = DhtService::builder(keypair.public_key.into()).build();

        let router = Router::builder().route(dht).build();

        let network = Network::builder()
            .with_private_key(key.to_bytes())
            .with_service_name("test-service")
            .build((Ipv4Addr::LOCALHOST, 0), router)
            .unwrap();

        let dht = dht_client.build(network.clone());

        Ok(Self { network, dht })
    }

    fn make_node_info(key: &ed25519::SecretKey, address: Address) -> dht::NodeInfo {
        const TTL: u32 = 3600;

        let keypair = ed25519::KeyPair::from(key);
        let peer_id = PeerId::from(keypair.public_key);

        let now = now_sec();
        let mut node_info = dht::NodeInfo {
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

fn make_network(node_count: usize) -> (Vec<Node>, Vec<Arc<dht::NodeInfo>>) {
    let keys = (0..node_count)
        .map(|_| ed25519::SecretKey::generate(&mut rand::thread_rng()))
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

    (nodes, bootstrap_info)
}

#[tokio::test]
async fn bootstrap_nodes_accessible() -> Result<()> {
    tracing_subscriber::fmt::try_init().ok();

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

    #[derive(Debug, Clone, PartialEq, Eq, TlWrite, TlRead)]
    struct SomeValue(u32);

    let (nodes, _) = make_network(5);

    // Store value
    let first = &nodes[0].dht;
    let value_to_store = first.make_signed_value("test", now_sec() + 600, SomeValue(123123));
    first.store_value(value_to_store.clone()).await?;

    // Retrieve an existing value
    let queried_value = first
        .find_value(&dht::SignedKey {
            name: "test".to_owned().into(),
            idx: 0,
            peer_id: *first.network().peer_id(),
        })
        .await?;
    assert_eq!(&dht::Value::Signed(queried_value), value_to_store.as_ref());

    // Retrieve a non-existing value
    let res = first
        .find_value(&dht::SignedKey {
            name: "not-existing".to_owned().into(),
            idx: 1,
            peer_id: *first.network().peer_id(),
        })
        .await;
    assert!(matches!(res, Err(FindValueError::NotFound)));

    Ok(())
}
