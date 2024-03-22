use std::net::{Ipv4Addr, SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use everscale_crypto::ed25519;
use serde::{Deserialize, Serialize};

use tycho_network::{
    DhtClient, DhtConfig, DhtService, Network, OverlayConfig, OverlayId, OverlayService, PeerId,
    PrivateOverlay, Response, Router, Service, ServiceRequest, Version,
};
use tycho_util::futures::BoxFutureOrNoop;
use tycho_util::FastHashSet;

use crate::models::point::{Location, Point, PointId, Round, Signature};

#[derive(Serialize, Deserialize, Debug)]
enum MPRequest {
    Broadcast { point: Point },
    Point { id: PointId },
}

#[derive(Serialize, Deserialize, Debug)]
enum MPRemoteResult {
    Ok(MPResponse),
    Err(String),
}

#[derive(Serialize, Deserialize, Debug)]
enum MPResponse {
    Broadcast(BroadcastResponse),
    Point(PointResponse),
}

#[derive(Serialize, Deserialize, Debug)]
struct BroadcastResponse {
    // for requested point
    pub signature: Signature,
    // at the same round, if it was not skipped
    pub signer_point: Option<Point>,
}
#[derive(Serialize, Deserialize, Debug)]
struct PointResponse {
    pub point: Option<Point>,
}

pub struct Dispatcher {
    pub overlay: PrivateOverlay,
    pub dht_client: DhtClient,
    network: Network,
}

impl Dispatcher {
    const PRIVATE_OVERLAY_ID: OverlayId = OverlayId(*b"ac87b6945b4f6f736963f7f65d025943");

    pub fn new<T: ToSocketAddrs>(
        socket_addr: T,
        key: &ed25519::SecretKey,
        all_peers: &Vec<PeerId>,
    ) -> Self {
        let keypair = ed25519::KeyPair::from(key);
        let local_id = PeerId::from(keypair.public_key);

        // TODO receive configured services from general node,
        //  move current setup to test below as it provides acceptable timing

        let (dht_client_builder, dht_service) = DhtService::builder(local_id)
            .with_config(DhtConfig {
                local_info_announce_period: Duration::from_secs(1),
                max_local_info_announce_period_jitter: Duration::from_secs(1),
                routing_table_refresh_period: Duration::from_secs(1),
                max_routing_table_refresh_period_jitter: Duration::from_secs(1),
                ..Default::default()
            })
            .build();

        let private_overlay = PrivateOverlay::builder(Self::PRIVATE_OVERLAY_ID)
            .resolve_peers(true)
            .with_entries(all_peers)
            .build(Responder(Arc::new(ResponderInner {})));

        let (overlay_tasks, overlay_service) = OverlayService::builder(local_id)
            .with_config(OverlayConfig {
                private_overlay_peer_resolve_period: Duration::from_secs(1),
                private_overlay_peer_resolve_max_jitter: Duration::from_secs(1),
                ..Default::default()
            })
            .with_dht_service(dht_service.clone())
            .build();

        overlay_service.try_add_private_overlay(&private_overlay);

        let router = Router::builder()
            .route(dht_service)
            .route(overlay_service)
            .build();

        let network = Network::builder()
            .with_private_key(key.to_bytes())
            .with_service_name("mempool-network-service")
            .build(socket_addr, router)
            .unwrap();

        let dht_client = dht_client_builder.build(network.clone());

        overlay_tasks.spawn(network.clone());

        Self {
            overlay: private_overlay,
            dht_client,
            network,
        }
    }

    pub async fn broadcast(&self, node: &PeerId, point: Point) -> Result<BroadcastResponse> {
        // TODO: move MPRequest et al to TL - won't need to copy Point
        let response = self.query(node, &MPRequest::Broadcast { point }).await?;
        match Self::parse_response(node, &response.body)? {
            MPResponse::Broadcast(r) => Ok(r),
            _ => Err(anyhow!("MPResponse::Broadcast: mismatched response")),
        }
    }

    pub async fn get_point(&self, node: &PeerId, id: PointId) -> Result<PointResponse> {
        let response = self.query(node, &MPRequest::Point { id }).await?;
        match Self::parse_response(node, &response.body)? {
            MPResponse::Point(r) => Ok(r),
            _ => Err(anyhow!("MPResponse::Point: mismatched response")),
        }
    }

    async fn query(&self, node: &PeerId, data: &MPRequest) -> Result<Response> {
        let request = tycho_network::Request {
            version: Version::V1,
            body: Bytes::from(bincode::serialize(data)?),
        };

        self.overlay.query(&self.network, node, request).await
    }

    fn parse_response(node: &PeerId, body: &Bytes) -> Result<MPResponse> {
        match bincode::deserialize::<MPRemoteResult>(body) {
            Ok(MPRemoteResult::Ok(response)) => Ok(response),
            Ok(MPRemoteResult::Err(e)) => Err(anyhow::Error::msg(e)),
            Err(e) => Err(anyhow!(
                "failed to deserialize response from {node:?}: {e:?}"
            )),
        }
    }
}

struct Responder(Arc<ResponderInner>);

impl Service<ServiceRequest> for Responder {
    type QueryResponse = Response;
    type OnQueryFuture = BoxFutureOrNoop<Option<Self::QueryResponse>>;
    type OnMessageFuture = futures_util::future::Ready<()>;
    type OnDatagramFuture = futures_util::future::Ready<()>;

    #[inline]
    fn on_query(&self, req: ServiceRequest) -> Self::OnQueryFuture {
        BoxFutureOrNoop::future(self.0.clone().handle(req))
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

struct ResponderInner {
    // state and storage components go here
}

impl ResponderInner {
    async fn handle(self: Arc<Self>, req: ServiceRequest) -> Option<Response> {
        let body = match bincode::deserialize::<MPRequest>(&req.body) {
            Ok(body) => body,
            Err(e) => {
                tracing::error!("unexpected request from {:?}: {e:?}", req.metadata.peer_id);
                // malformed request is a reason to ignore it
                return None;
            }
        };

        let response = match body {
            MPRequest::Broadcast { point } => {
                // 1.1 sigs for my block + 1.2 my next includes
                // ?? + 3.1 ask last
                MPResponse::Broadcast(BroadcastResponse {
                    signature: Signature(Bytes::new()),
                    signer_point: None,
                })
            }
            MPRequest::Point { id } => {
                // 1.2 my next includes (merged with Broadcast flow)
                MPResponse::Point(PointResponse { point: None })
            }
        };

        Some(Response {
            version: Version::default(),
            body: Bytes::from(match bincode::serialize(&MPRemoteResult::Ok(response)) {
                Ok(data) => data,
                Err(e) => {
                    tracing::error!("failed to serialize response to {:?}: {e:?}", req.metadata);
                    bincode::serialize(&MPRemoteResult::Err(format!("internal error")))
                        .expect("must not fail")
                }
            }),
        })
    }
}

#[cfg(test)]
mod tests {
    use tycho_network::{Address, PeerInfo};
    use tycho_util::time::now_sec;

    use crate::engine::node_count::NodeCount;
    use crate::engine::peer_schedule::PeerSchedule;
    use crate::models::point::Digest;

    use super::*;

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

    async fn make_network(node_count: usize) -> Vec<Dispatcher> {
        let keys = (0..node_count)
            .map(|_| ed25519::SecretKey::generate(&mut rand::thread_rng()))
            .collect::<Vec<_>>();

        let all_peers = keys
            .iter()
            .map(|s| PeerId::from(ed25519::KeyPair::from(s).public_key))
            .collect::<Vec<_>>();

        let mut nodes = keys
            .iter()
            .map(|s| Dispatcher::new((Ipv4Addr::LOCALHOST, 0), s, &all_peers))
            .collect::<Vec<_>>();

        let bootstrap_info = std::iter::zip(&keys, &nodes)
            .map(|(key, node)| Arc::new(make_peer_info(key, node.network.local_addr().into())))
            .collect::<Vec<_>>();

        let schedules = std::iter::zip(&all_peers, &nodes)
            .map(|(peer_id, node)| PeerSchedule::new(Round(0), &all_peers, &node.overlay, peer_id))
            .collect::<Vec<_>>();

        if let Some(node) = nodes.first() {
            for info in &bootstrap_info {
                if info.id == node.network.peer_id() {
                    continue;
                }
                node.dht_client.add_peer(info.clone()).unwrap();
            }
        }

        let all_peers = FastHashSet::from_iter(all_peers.into_iter());
        for sch in &schedules {
            sch.wait_for_peers(Round(1), NodeCount::new(node_count).majority_except_me())
                .await;
            tracing::info!("found peers for {}", sch.local_id);
        }

        nodes
    }

    #[tokio::test]
    async fn dispatcher_works() -> Result<()> {
        tracing_subscriber::fmt::try_init().ok();
        tracing::info!("dispatcher_works");

        let nodes = make_network(3).await;

        let point_id = PointId {
            location: crate::models::point::Location {
                round: Round(0),
                author: PeerId([0u8; 32]),
            },
            digest: Digest([0u8; 32]),
        };

        for i in 0..nodes.len() {
            for j in 0..nodes.len() {
                if i == j {
                    continue;
                }

                let left = &nodes[i];
                let right = &nodes[j];

                let PointResponse { point } = left
                    .get_point(right.network.peer_id(), point_id.clone())
                    .await?;
                assert!(point.is_none());
            }
        }
        Ok(())
    }
}
