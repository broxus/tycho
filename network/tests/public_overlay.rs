//! Run tests with this env:
//! ```text
//! RUST_LOG=info,tycho_network=trace
//! ```
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use anyhow::Result;
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
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
        let NodeBase { network, dht_service, overlay_service, peer_resolver } = NodeBase::with_random_key();
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
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(public_overlay_query)),
            file!(),
            52u32,
        );
        let peer_id = peer_id;
        let req = req;
        {
            __guard.end_section(55u32);
            let __result = self
                .public_overlay
                .query(&self.network, peer_id, Request::from_tl(req))
                .await;
            __guard.start_section(55u32);
            __result
        }?
            .parse_tl::<A>()
            .map_err(Into::into)
    }
}
fn make_network(node_count: usize) -> Vec<Node> {
    let nodes = (0..node_count).map(|_| Node::with_random_key()).collect::<Vec<_>>();
    let common_peer_info = nodes.first().unwrap().network.sign_peer_info(0, u32::MAX);
    for node in &nodes {
        node.dht_client.add_peer(Arc::new(common_peer_info.clone())).unwrap();
    }
    nodes
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_overlays_accessible() -> Result<()> {
    let mut __guard = crate::__async_profile_guard__::Guard::new(
        concat!(module_path!(), "::", stringify!(public_overlays_accessible)),
        file!(),
        78u32,
    );
    tycho_util::test::init_logger("public_overlays_accessible", "debug");
    #[derive(Debug, Default)]
    struct PeerState {
        knows_about: usize,
        known_by: usize,
    }
    let nodes = make_network(20);
    tracing::info!("discovering nodes");
    loop {
        __guard.checkpoint(90u32);
        {
            __guard.end_section(91u32);
            let __result = tokio::time::sleep(Duration::from_secs(1)).await;
            __guard.start_section(91u32);
            __result
        };
        let mut peer_states = BTreeMap::<&PeerId, PeerState>::new();
        for (i, left) in nodes.iter().enumerate() {
            __guard.checkpoint(95u32);
            for (j, right) in nodes.iter().enumerate() {
                __guard.checkpoint(96u32);
                if i == j {
                    {
                        __guard.end_section(98u32);
                        __guard.start_section(98u32);
                        continue;
                    };
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
        tracing::info!("peers with filled overlay: {} / {}", total_filled, nodes.len());
        if total_filled == nodes.len() {
            {
                __guard.end_section(124u32);
                __guard.start_section(124u32);
                break;
            };
        }
    }
    tracing::info!("resolving entries...");
    for node in &nodes {
        __guard.checkpoint(129u32);
        let resolved = FuturesUnordered::new();
        for entry in node.public_overlay.read_entries().iter() {
            __guard.checkpoint(131u32);
            let handle = entry.resolver_handle.clone();
            resolved
                .push(async move {
                    let mut __guard = crate::__async_profile_guard__::Guard::new(
                        concat!(module_path!(), "::async_block"),
                        file!(),
                        133u32,
                    );
                    {
                        __guard.end_section(133u32);
                        let __result = handle.wait_resolved().await;
                        __guard.start_section(133u32);
                        __result
                    }
                });
        }
        {
            __guard.end_section(137u32);
            let __result = resolved.collect::<Vec<_>>().await;
            __guard.start_section(137u32);
            __result
        };
        tracing::info!(peer_id = % node.network.peer_id(), "all entries resolved",);
    }
    tracing::info!("checking connectivity...");
    for i in 0..nodes.len() {
        __guard.checkpoint(145u32);
        for j in 0..nodes.len() {
            __guard.checkpoint(146u32);
            if i == j {
                {
                    __guard.end_section(148u32);
                    __guard.start_section(148u32);
                    continue;
                };
            }
            let left = &nodes[i];
            let right = &nodes[j];
            let value = (i * 1000 + j) as u64;
            let Pong { value: received } = {
                __guard.end_section(157u32);
                let __result = left
                    .public_overlay_query(right.network.peer_id(), Ping { value })
                    .await;
                __guard.start_section(157u32);
                __result
            }?;
            assert_eq!(received, value);
        }
    }
    tracing::info!("done!");
    Ok(())
}
static PUBLIC_OVERLAY_ID: OverlayId = OverlayId([1; 32]);
