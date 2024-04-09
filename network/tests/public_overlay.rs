//! Run tests with this env:
//! ```text
//! RUST_LOG=info,tycho_network=trace
//! ```

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use tycho_network::{DhtClient, Network, OverlayId, PeerId, PublicOverlay, Request};

use self::common::{NodeBase, Ping, PingPongService, Pong};

mod common;

struct Node {
    network: Network,
    public_overlay: PublicOverlay,
    dht_client: DhtClient,
}

impl Node {
    fn with_random_key() -> Self {
        let NodeBase {
            network,
            dht_service,
            overlay_service,
            peer_resolver,
        } = NodeBase::with_random_key();

        let public_overlay = PublicOverlay::builder(PUBLIC_OVERLAY_ID)
            .with_peer_resolver(peer_resolver)
            .build(PingPongService);
        overlay_service.add_public_overlay(&public_overlay);

        let dht_client = dht_service.make_client(&network);

        Self {
            network,
            public_overlay,
            dht_client,
        }
    }

    async fn public_overlay_query<Q, A>(&self, peer_id: &PeerId, req: Q) -> Result<A>
    where
        Q: tl_proto::TlWrite<Repr = tl_proto::Boxed>,
        for<'a> A: tl_proto::TlRead<'a, Repr = tl_proto::Boxed>,
    {
        self.public_overlay
            .query(&self.network, peer_id, Request::from_tl(req))
            .await?
            .parse_tl::<A>()
            .map_err(Into::into)
    }
}

fn make_network(node_count: usize) -> Vec<Node> {
    let nodes = (0..node_count)
        .map(|_| Node::with_random_key())
        .collect::<Vec<_>>();

    let common_peer_info = nodes.first().unwrap().network.sign_peer_info(0, u32::MAX);

    for node in &nodes {
        node.dht_client
            .add_peer(Arc::new(common_peer_info.clone()))
            .unwrap();
    }

    nodes
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_overlays_accessible() -> Result<()> {
    tycho_util::test::init_logger("public_overlays_accessible");

    #[derive(Debug, Default)]
    struct PeerState {
        knows_about: usize,
        known_by: usize,
    }

    let nodes = make_network(20);

    tracing::info!("discovering nodes");
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;

        let mut peer_states = BTreeMap::<&PeerId, PeerState>::new();

        for (i, left) in nodes.iter().enumerate() {
            for (j, right) in nodes.iter().enumerate() {
                if i == j {
                    continue;
                }

                let left_id = left.network.peer_id();
                let right_id = right.network.peer_id();

                if left.public_overlay.read_entries().contains(right_id) {
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
    for node in &nodes {
        let resolved = FuturesUnordered::new();
        for entry in node.public_overlay.read_entries().iter() {
            let handle = entry.resolver_handle.clone();
            resolved.push(async move { handle.wait_resolved().await });
        }

        // Ensure all entries are resolved.
        resolved.collect::<Vec<_>>().await;
        tracing::info!(
            peer_id = %node.network.peer_id(),
            "all entries resolved",
        );
    }

    tracing::info!("checking connectivity...");
    for i in 0..nodes.len() {
        for j in 0..nodes.len() {
            if i == j {
                continue;
            }

            let left = &nodes[i];
            let right = &nodes[j];

            let value = (i * 1000 + j) as u64;
            let Pong { value: received } = left
                .public_overlay_query(right.network.peer_id(), Ping { value })
                .await?;
            assert_eq!(received, value);
        }
    }

    tracing::info!("done!");
    Ok(())
}

static PUBLIC_OVERLAY_ID: OverlayId = OverlayId([1; 32]);
