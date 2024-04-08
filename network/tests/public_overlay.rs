//! Run tests with this env:
//! ```text
//! RUST_LOG=info,tycho_network=trace
//! ```

use std::sync::Arc;

use anyhow::Result;
use tycho_network::{DhtClient, Network, OverlayId, PeerId, PublicOverlay, Request};

use self::common::{init_logger, NodeBase, Ping, PingPongService, Pong};

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
    init_logger();
    tracing::info!("public_overlays_accessible");

    let nodes = make_network(20);

    futures_util::future::pending::<()>().await;

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

    Ok(())
}

static PUBLIC_OVERLAY_ID: OverlayId = OverlayId([1; 32]);
