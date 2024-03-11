use std::net::{Ipv4Addr, SocketAddr, ToSocketAddrs};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use everscale_crypto::ed25519;
use serde::{Deserialize, Serialize};

use tycho_network::{
    Network, OverlayId, OverlayService, PeerId, PrivateOverlay, Response, Router, Service,
    ServiceRequest, Version,
};
use tycho_util::futures::BoxFutureOrNoop;

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
    network: Network,
    private_overlay: PrivateOverlay,
}

impl Dispatcher {
    const PRIVATE_OVERLAY_ID: OverlayId = OverlayId(*b"ac87b6945b4f6f736963f7f65d025943");

    pub fn new<T: ToSocketAddrs>(socket_addr: T, key: &ed25519::SecretKey) -> Self {
        let keypair = ed25519::KeyPair::from(key);
        let local_id = PeerId::from(keypair.public_key);

        let private_overlay = PrivateOverlay::builder(Self::PRIVATE_OVERLAY_ID)
            .build(Responder(Arc::new(ResponderInner {})));

        let (overlay_tasks, overlay_service) = OverlayService::builder(local_id)
            .with_private_overlay(&private_overlay)
            .build();

        let router = Router::builder().route(overlay_service).build();

        let network = Network::builder()
            .with_private_key(key.to_bytes())
            .with_service_name("mempool-network-service")
            .build(socket_addr, router)
            .unwrap();

        overlay_tasks.spawn(network.clone());

        Self {
            network,
            private_overlay,
        }
    }

    pub async fn broadcast(&self, node: &PeerId, point: Point) -> Result<BroadcastResponse> {
        // TODO: move MPRequest et al to TL - will need not copy Point
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

        self.private_overlay
            .query(&self.network, node, request)
            .await
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
                // NOTE: malformed request is a reason to ignore it
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

    fn make_network(node_count: usize) -> Vec<Dispatcher> {
        let keys = (0..node_count)
            .map(|_| ed25519::SecretKey::generate(&mut rand::thread_rng()))
            .collect::<Vec<_>>();

        let mut nodes = keys
            .iter()
            .map(|k| Dispatcher::new((Ipv4Addr::LOCALHOST, 0), k))
            .collect::<Vec<_>>();

        let bootstrap_info = std::iter::zip(&keys, &nodes)
            .map(|(key, node)| Arc::new(make_peer_info(key, node.network.local_addr().into())))
            .collect::<Vec<_>>();

        for node in &mut nodes {
            let mut private_overlay_entries = node.private_overlay.write_entries();

            for info in &bootstrap_info {
                if info.id == node.network.peer_id() {
                    continue;
                }

                let handle = node
                    .network
                    .known_peers()
                    .insert(info.clone(), false)
                    .unwrap();
                private_overlay_entries.insert(&info.id, Some(handle));
            }
        }

        nodes
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn dispatcher_works() -> Result<()> {
        tracing::info!("dispatcher_works");

        let nodes = make_network(2);

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
