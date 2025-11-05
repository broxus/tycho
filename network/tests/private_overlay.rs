//! Run tests with this env:
//! ```text
//! RUST_LOG=info,tycho_network=trace
//! ```
use std::sync::Arc;
use anyhow::Result;
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use tycho_network::{DhtClient, Network, OverlayId, PeerId, PrivateOverlay, Request};
use self::common::{NodeBase, Ping, PingPongService, Pong};
mod common;
struct Node {
    network: Network,
    private_overlay: PrivateOverlay,
    dht_client: DhtClient,
}
impl Node {
    fn with_random_key() -> Self {
        let NodeBase { network, dht_service, overlay_service, peer_resolver } = NodeBase::with_random_key();
        let private_overlay = PrivateOverlay::builder(PRIVATE_OVERLAY_ID)
            .with_peer_resolver(peer_resolver)
            .build(PingPongService);
        overlay_service.add_private_overlay(&private_overlay);
        let dht_client = dht_service.make_client(&network);
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
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(private_overlay_query)),
            file!(),
            50u32,
        );
        let peer_id = peer_id;
        let req = req;
        {
            __guard.end_section(53u32);
            let __result = self
                .private_overlay
                .query(&self.network, peer_id, Request::from_tl(req))
                .await;
            __guard.start_section(53u32);
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
    let mut __guard = crate::__async_profile_guard__::Guard::new(
        concat!(module_path!(), "::", stringify!(private_overlays_accessible)),
        file!(),
        85u32,
    );
    tycho_util::test::init_logger("private_overlays_accessible", "debug");
    let nodes = make_network(20);
    for node in &nodes {
        __guard.checkpoint(90u32);
        let resolved = FuturesUnordered::new();
        for entry in node.private_overlay.read_entries().iter() {
            __guard.checkpoint(92u32);
            let handle = entry.resolver_handle.clone();
            resolved
                .push(async move {
                    let mut __guard = crate::__async_profile_guard__::Guard::new(
                        concat!(module_path!(), "::async_block"),
                        file!(),
                        94u32,
                    );
                    {
                        __guard.end_section(94u32);
                        let __result = handle.wait_resolved().await;
                        __guard.start_section(94u32);
                        __result
                    }
                });
        }
        {
            __guard.end_section(98u32);
            let __result = resolved.collect::<Vec<_>>().await;
            __guard.start_section(98u32);
            __result
        };
        tracing::info!(peer_id = % node.network.peer_id(), "all entries resolved",);
    }
    for i in 0..nodes.len() {
        __guard.checkpoint(105u32);
        for j in 0..nodes.len() {
            __guard.checkpoint(106u32);
            if i == j {
                {
                    __guard.end_section(108u32);
                    __guard.start_section(108u32);
                    continue;
                };
            }
            let left = &nodes[i];
            let right = &nodes[j];
            let value = (i * 1000 + j) as u64;
            let Pong { value: received } = {
                __guard.end_section(117u32);
                let __result = left
                    .private_overlay_query(right.network.peer_id(), Ping { value })
                    .await;
                __guard.start_section(117u32);
                __result
            }?;
            assert_eq!(received, value);
        }
    }
    Ok(())
}
static PRIVATE_OVERLAY_ID: OverlayId = OverlayId([0; 32]);
