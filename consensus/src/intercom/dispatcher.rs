use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use tycho_network::{service_query_fn, Network, NetworkConfig, NetworkExt, Response, ServiceRequest, Version, PeerId};

use crate::intercom::responses::*;
use crate::models::{Location, Point, PointId, RoundId, Signature};

#[derive(Serialize, Deserialize, Debug)]
enum MPRequest {
    // by author
    Broadcast { point: Point },
    Point { id: PointId },
    // any point from the last author's round;
    // 1/3+1 evidenced vertices determine current consensus round
    // PointLast,
    // unique point with known evidence
    Vertex { id: Location },
    // the next point by the same author
    // that contains >=2F signatures for requested vertex
    Evidence { vertex_id: Location },
    Vertices { round: RoundId },
}

#[derive(Serialize, Deserialize, Debug)]
enum MPResponse {
    Broadcast(BroadcastResponse),
    Point(PointResponse),
    //PointLast(Option<Point>),
    Vertex(VertexResponse),
    Evidence(EvidenceResponse),
    Vertices(VerticesResponse),
}

#[derive(Serialize, Deserialize, Debug)]
enum MPRemoteResult {
    Ok(MPResponse),
    Err(String),
}

pub struct Dispatcher {
    inner: Arc<DispatcherInner>,
    network: Network,
}

impl Dispatcher {
    pub fn new() -> Result<Self> {
        let inner = Arc::new(DispatcherInner {});
        let service_fn = service_query_fn({
            let inner = inner.clone();
            move |req| inner.clone().handle(req)
        });

        let network = Network::builder()
            .with_config(NetworkConfig::default())
            .with_random_private_key()
            .with_service_name("tycho-mempool-router")
            .build((Ipv4Addr::LOCALHOST, 0), service_fn)?;

        Ok(Self { inner, network })
    }

    pub async fn broadcast(&self, point: Point, from: SocketAddr) -> Result<BroadcastResponse> {
        let request = tycho_network::Request {
            version: Version::V1,
            body: Bytes::from(bincode::serialize(&MPRequest::Broadcast { point })?),
        };

        let remote_peer = self.network.connect(from, PeerId::wrap(&[0u8;32])).await?;

        let response = self.network.query(&remote_peer, request).await?;

        match parse_response(&response.body)? {
            MPResponse::Broadcast(r) => Ok(r),
            x => Err(anyhow!("wrong response")),
        }
    }

    pub async fn point(&self, id: PointId, from: SocketAddr) -> Result<PointResponse> {
        let request = tycho_network::Request {
            version: Version::V1,
            body: Bytes::from(bincode::serialize(&MPRequest::Point { id })?),
        };

        let remote_peer = self.network.connect(from, PeerId::wrap(&[0u8;32])).await?;

        let response = self.network.query(&remote_peer, request).await?;

        match parse_response(&response.body)? {
            MPResponse::Point(r) => Ok(r),
            x => Err(anyhow!("wrong response")),
        }
    }

    pub async fn vertex(&self, id: Location, from: SocketAddr) -> Result<VertexResponse> {
        let request = tycho_network::Request {
            version: Version::V1,
            body: Bytes::from(bincode::serialize(&MPRequest::Vertex { id })?),
        };

        let remote_peer = self.network.connect(from,  PeerId::wrap(&[0u8;32])).await?;

        let response = self.network.query(&remote_peer, request).await?;

        match parse_response(&response.body)? {
            MPResponse::Vertex(r) => Ok(r),
            x => Err(anyhow!("wrong response")),
        }
    }

    pub async fn evidence(
        &self,
        vertex_id: Location,
        from: SocketAddr,
    ) -> Result<EvidenceResponse> {
        let request = tycho_network::Request {
            version: Version::V1,
            body: Bytes::from(bincode::serialize(&MPRequest::Evidence { vertex_id })?),
        };

        let remote_peer = self.network.connect(from,  PeerId::wrap(&[0u8;32])).await?;

        let response = self.network.query(&remote_peer, request).await?;

        match parse_response(&response.body)? {
            MPResponse::Evidence(r) => Ok(r),
            x => Err(anyhow!("wrong response")),
        }
    }

    pub async fn vertices(&self, round: RoundId, from: SocketAddr) -> Result<VerticesResponse> {
        let request = tycho_network::Request {
            version: Version::V1,
            body: Bytes::from(bincode::serialize(&MPRequest::Vertices { round })?),
        };

        let remote_peer = self.network.connect(from, PeerId::wrap(&[0u8;32])).await?;

        let response = self.network.query(&remote_peer, request).await?;

        match parse_response(&response.body)? {
            MPResponse::Vertices(r) => Ok(r),
            x => Err(anyhow!("wrong response")),
        }
    }
}

struct DispatcherInner {
    // state and storage components go here
}

impl DispatcherInner {
    async fn handle(self: Arc<Self>, req: ServiceRequest) -> Option<Response> {
        let body = match bincode::deserialize::<MPRequest>(&req.body) {
            Ok(body) => body,
            Err(e) => {
                tracing::error!("unexpected request from {:?}: {e:?}", req.metadata);
                // NOTE: malformed request is a reason to ignore it
                return None;
            }
        };

        let response = match body {
            MPRequest::Broadcast { point } => {
                // 1.1 sigs for my block + 1.2 my next includes
                // ?? + 3.1 ask last
                MPResponse::Broadcast(BroadcastResponse {
                    current_round: RoundId(0),
                    signature: Signature(Bytes::new()),
                    signer_point: None,
                })
            }
            MPRequest::Point { id } => {
                // 1.2 my next includes (merged with Broadcast flow)
                MPResponse::Point(PointResponse {
                    current_round: RoundId(0),
                    point: None,
                })
            }
            MPRequest::Vertex { id } => {
                // verification flow: downloader
                MPResponse::Vertex(VertexResponse {
                    current_round: RoundId(0),
                    vertex: None,
                })
            }
            MPRequest::Evidence { vertex_id } => {
                // verification flow: downloader
                MPResponse::Evidence(EvidenceResponse {
                    current_round: RoundId(0),
                    point: None,
                })
            }
            MPRequest::Vertices { round } => {
                // cold sync flow: downloader
                MPResponse::Vertices(VerticesResponse {
                    vertices: Vec::new(),
                })
            }
        };

        Some(Response {
            version: Version::default(),
            body: Bytes::from(match bincode::serialize(&response) {
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

fn parse_response(body: &Bytes) -> anyhow::Result<MPResponse> {
    if body.is_empty() {
        return Err(anyhow::Error::msg(
            "remote response serialization exception is hidden by exception during serialization",
        ));
    }
    match bincode::deserialize::<MPRemoteResult>(body) {
        Ok(MPRemoteResult::Ok(response)) => Ok(response),
        Ok(MPRemoteResult::Err(e)) => Err(anyhow::Error::msg(e)),
        Err(e) => Err(anyhow!("failed to deserialize response: {e:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn underlying_network_works() -> Result<()> {
        let node1 = Dispatcher::new()?.network;
        let node2 = Dispatcher::new()?.network;

        let peer2 = node1.connect(node2.local_addr()).await?;
        let response = node1
            .query(
                &peer2,
                tycho_network::Request {
                    version: Version::V1,
                    body: Bytes::from("bites"),
                },
            )
            .await?;
        let response = parse_response(&response.body);

        tracing::info!("response '{response:?}'");

        assert!(response.is_err());
        Ok(())
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn dispatcher_works() -> Result<()> {
        let node1 = Dispatcher::new()?;
        let node2 = Dispatcher::new()?;

        let data = node1
            .vertices(RoundId(0), node2.network.local_addr())
            .await?;

        tracing::info!("response: '{data:?}'");

        assert!(data.vertices.is_empty());
        Ok(())
    }
}
