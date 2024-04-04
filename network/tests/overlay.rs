//! Run tests with this env:
//! ```text
//! RUST_LOG=info,tycho_network=trace
//! ```

use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use everscale_crypto::ed25519;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use tl_proto::{TlRead, TlWrite};
use tycho_network::{
    DhtClient, DhtConfig, DhtService, Network, OverlayId, OverlayService, PeerId, PrivateOverlay,
    Request, Response, Router, Service, ServiceRequest,
};

struct Node {
    network: Network,
    private_overlay: PrivateOverlay,
    dht_client: DhtClient,
}

impl Node {
    fn with_random_key() -> Self {
        let key = ed25519::SecretKey::generate(&mut rand::thread_rng());
        let local_id = ed25519::PublicKey::from(&key).into();

        let (dht_tasks, dht_service) = DhtService::builder(local_id)
            .with_config(DhtConfig {
                local_info_announce_period: Duration::from_secs(1),
                max_local_info_announce_period_jitter: Duration::from_secs(1),
                routing_table_refresh_period: Duration::from_secs(1),
                max_routing_table_refresh_period_jitter: Duration::from_secs(1),
                ..Default::default()
            })
            .build();

        let (overlay_tasks, overlay_service) = OverlayService::builder(local_id)
            .with_dht_service(dht_service.clone())
            .build();

        let router = Router::builder()
            .route(dht_service.clone())
            .route(overlay_service.clone())
            .build();

        let network = Network::builder()
            .with_private_key(key.to_bytes())
            .with_service_name("test-service")
            .build((Ipv4Addr::LOCALHOST, 0), router)
            .unwrap();

        dht_tasks.spawn(&network);
        overlay_tasks.spawn(&network);

        let dht_client = dht_service.make_client(&network);
        let peer_resolver = dht_service.make_peer_resolver().build(&network);

        let private_overlay = PrivateOverlay::builder(PRIVATE_OVERLAY_ID)
            .with_peer_resolver(peer_resolver)
            .build(PingPongService);
        overlay_service.add_private_overlay(&private_overlay);

        Self {
            network,
            dht_client,
            private_overlay,
        }
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
    let nodes = (0..node_count)
        .map(|_| Node::with_random_key())
        .collect::<Vec<_>>();

    let common_peer_info = nodes.first().unwrap().network.sign_peer_info(0, u32::MAX);

    for node in &nodes {
        node.dht_client
            .add_peer(Arc::new(common_peer_info.clone()))
            .unwrap();

        let mut private_overlay_entries = node.private_overlay.write_entries();

        for peer_id in nodes.iter().map(|node| node.network.peer_id()) {
            if peer_id == node.network.peer_id() {
                continue;
            }
            private_overlay_entries.insert(peer_id);
        }
    }

    nodes
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn private_overlays_accessible() -> Result<()> {
    tracing_subscriber::fmt::try_init().ok();
    tracing::info!("bootstrap_nodes_accessible");

    std::panic::set_hook(Box::new(|info| {
        use std::io::Write;

        tracing::error!("{}", info);
        std::io::stderr().flush().ok();
        std::io::stdout().flush().ok();
        #[allow(clippy::exit)]
        std::process::exit(1);
    }));

    let nodes = make_network(20);

    for node in &nodes {
        let resolved = FuturesUnordered::new();
        for entry in node.private_overlay.read_entries().iter() {
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
