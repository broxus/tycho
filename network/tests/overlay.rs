//! Run tests with this env:
//! ```text
//! RUST_LOG=info,tycho_network=trace
//! ```

use anyhow::Result;
use everscale_crypto::ed25519;
use std::net::Ipv4Addr;
use std::sync::Arc;
use tl_proto::{TlRead, TlWrite};
use tycho_network::{
    Address, KnownPeerHandle, Network, OverlayId, OverlayService, PeerId, PeerInfo, PrivateOverlay,
    Request, Response, Router, Service, ServiceRequest,
};
use tycho_util::time::now_sec;

struct Node {
    network: Network,
    private_overlay: PrivateOverlay,
    known_peer_handles: Vec<KnownPeerHandle>,
}

impl Node {
    fn new(key: &ed25519::SecretKey) -> Self {
        let keypair = ed25519::KeyPair::from(key);
        let local_id = PeerId::from(keypair.public_key);

        let (overlay_tasks, overlay_service) = OverlayService::builder(local_id).build();

        let router = Router::builder().route(overlay_service.clone()).build();

        let network = Network::builder()
            .with_private_key(key.to_bytes())
            .with_service_name("test-service")
            .build((Ipv4Addr::LOCALHOST, 0), router)
            .unwrap();

        overlay_tasks.spawn(&network);

        let private_overlay = PrivateOverlay::builder(PRIVATE_OVERLAY_ID).build(PingPongService);
        overlay_service.add_private_overlay(&private_overlay);

        Self {
            network,
            private_overlay,
            known_peer_handles: Vec::new(),
        }
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

    async fn private_overlay_query<Q, A>(&self, peer_id: &PeerId, req: Q) -> Result<A>
    where
        Q: tl_proto::TlWrite<Repr = tl_proto::Boxed>,
        for<'a> A: tl_proto::TlRead<'a, Repr = tl_proto::Boxed>,
    {
        self.private_overlay
            .query(&self.network, peer_id, Request::from_tl(req))
            .await?
            .parse_tl::<A>()
            .map_err(Into::into)
    }
}

fn make_network(node_count: usize) -> Vec<Node> {
    let keys = (0..node_count)
        .map(|_| ed25519::SecretKey::generate(&mut rand::thread_rng()))
        .collect::<Vec<_>>();

    let mut nodes = keys.iter().map(Node::new).collect::<Vec<_>>();

    let bootstrap_info = std::iter::zip(&keys, &nodes)
        .map(|(key, node)| Arc::new(Node::make_peer_info(key, node.network.local_addr().into())))
        .collect::<Vec<_>>();

    for node in &mut nodes {
        let mut private_overlay_entries = node.private_overlay.write_entries();

        for info in &bootstrap_info {
            if info.id == node.network.peer_id() {
                continue;
            }

            node.known_peer_handles.push(
                node.network
                    .known_peers()
                    .insert(info.clone(), false)
                    .unwrap(),
            );

            private_overlay_entries.insert(&info.id);
        }
    }

    nodes
}

#[tokio::test]
async fn private_overlays_accessible() -> Result<()> {
    tracing_subscriber::fmt::try_init().ok();
    tracing::info!("bootstrap_nodes_accessible");

    let nodes = make_network(5);

    for i in 0..nodes.len() {
        for j in 0..nodes.len() {
            if i == j {
                continue;
            }

            let left = &nodes[i];
            let right = &nodes[j];

            let value = (i * 1000 + j) as u64;
            let Pong { value: received } = left
                .private_overlay_query(right.network.peer_id(), Ping { value })
                .await?;
            assert_eq!(received, value);
        }
    }

    Ok(())
}

struct PingPongService;

impl Service<ServiceRequest> for PingPongService {
    type QueryResponse = Response;
    type OnQueryFuture = futures_util::future::Ready<Option<Self::QueryResponse>>;
    type OnMessageFuture = futures_util::future::Ready<()>;
    type OnDatagramFuture = futures_util::future::Ready<()>;

    fn on_query(&self, req: ServiceRequest) -> Self::OnQueryFuture {
        futures_util::future::ready(match req.parse_tl() {
            Ok(Ping { value }) => Some(Response::from_tl(Pong { value })),
            Err(e) => {
                tracing::error!(
                    peer_id = %req.metadata.peer_id,
                    addr = %req.metadata.remote_address,
                    "invalid request: {e:?}",
                );
                None
            }
        })
    }

    #[inline]
    fn on_message(&self, _req: ServiceRequest) -> Self::OnMessageFuture {
        futures_util::future::ready(())
    }

    #[inline]
    fn on_datagram(&self, _req: ServiceRequest) -> Self::OnDatagramFuture {
        futures_util::future::ready(())
    }
}

#[derive(Debug, Copy, Clone, TlRead, TlWrite)]
#[tl(boxed, id = 0x11223344)]
struct Ping {
    value: u64,
}

#[derive(Debug, Copy, Clone, TlRead, TlWrite)]
#[tl(boxed, id = 0x55667788)]
struct Pong {
    value: u64,
}

static PRIVATE_OVERLAY_ID: OverlayId = OverlayId([0; 32]);
