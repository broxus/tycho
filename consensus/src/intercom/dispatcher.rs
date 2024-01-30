use std::convert::Infallible;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tower::util::BoxCloneService;
use tower::ServiceExt;

use tycho_network::util::NetworkExt;
use tycho_network::{Config, InboundServiceRequest, Network, Response, Version};

use crate::intercom::responses::*;
use crate::models::{Location, Point, PointId, RoundId, Signature};

const LOCAL_ADDR: &str = "127.0.0.1:0";
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
        let handler = inner.clone();
        let service_fn: BoxCloneService<InboundServiceRequest<Bytes>, Response<Bytes>, Infallible> =
            tower::service_fn(move |a| handler.clone().handle(a)).boxed_clone();
        let network = Network::builder()
            .with_config(Config::default())
            .with_random_private_key()
            .with_service_name("tycho-mempool-router")
            .build(LOCAL_ADDR, service_fn)?;
        Ok(Self { inner, network })
    }

    pub async fn broadcast(&self, point: Point, from: SocketAddr) -> Result<BroadcastResponse> {
        let request = tycho_network::Request {
            version: Version::V1,
            body: Bytes::from(bincode::serialize(&MPRequest::Broadcast { point })?),
        };

        let remote_peer = self.network.connect(from).await?;

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

        let remote_peer = self.network.connect(from).await?;

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

        let remote_peer = self.network.connect(from).await?;

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

        let remote_peer = self.network.connect(from).await?;

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

        let remote_peer = self.network.connect(from).await?;

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
    async fn handle(
        self: Arc<Self>,
        request: InboundServiceRequest<Bytes>,
    ) -> Result<Response<Bytes>, Infallible> {
        let result = match bincode::deserialize::<MPRequest>(&request.body) {
            Ok(request_body) => {
                let result: Result<MPResponse> = match &request_body {
                    MPRequest::Broadcast { point } => {
                        // 1.1 sigs for my block + 1.2 my next includes
                        // ?? + 3.1 ask last
                        Ok(MPResponse::Broadcast(BroadcastResponse {
                            current_round: RoundId(0),
                            signature: Signature(Bytes::new()),
                            signer_point: None,
                        }))
                    }
                    MPRequest::Point { id } => {
                        // 1.2 my next includes (merged with Broadcast flow)
                        Ok(MPResponse::Point(PointResponse {
                            current_round: RoundId(0),
                            point: None,
                        }))
                    }
                    MPRequest::Vertex { id } => {
                        // verification flow: downloader
                        Ok(MPResponse::Vertex(VertexResponse {
                            current_round: RoundId(0),
                            vertex: None,
                        }))
                    }
                    MPRequest::Evidence { vertex_id } => {
                        // verification flow: downloader
                        Ok(MPResponse::Evidence(EvidenceResponse {
                            current_round: RoundId(0),
                            point: None,
                        }))
                    }
                    MPRequest::Vertices { round } => {
                        // cold sync flow: downloader
                        Ok(MPResponse::Vertices(VerticesResponse {
                            vertices: Vec::new(),
                        }))
                    }
                };
                result
                    .map(|r| MPRemoteResult::Ok(r))
                    .map_err(|e| {
                        let msg = format!("{e:?}");
                        tracing::error!(
                            "failed to process request {:?} from {:?}: {msg}",
                            request_body,
                            request.metadata.as_ref()
                        );
                        MPRemoteResult::Err(format!("remote exception in execution: {msg}"))
                    })
                    .unwrap()
            }
            Err(e) => {
                let msg = format!("{e:?}");
                tracing::warn!(
                    "unexpected request from {:?}: {msg}",
                    request.metadata.as_ref()
                );
                MPRemoteResult::Err(format!(
                    "remote exception on request deserialization: {msg}"
                ))
            }
        };

        let body = bincode::serialize(&result)
            .map(Bytes::from)
            .unwrap_or_else(|err| {
                let msg = format!("{err:?}");
                tracing::error!(
                    "cannot serialize response to {:?}: {msg}; data: {result:?}",
                    request.metadata.as_ref()
                );
                bincode::serialize(&MPRemoteResult::Err(format!(
                    "remote exception on response serialization: {msg}"
                )))
                .map(Bytes::from)
                // empty body denotes a failure during serialization of error serialization, unlikely to happen
                .unwrap_or(Bytes::new())
            });

        let response = Response {
            version: Version::default(),
            body,
        };
        Ok::<_, Infallible>(response)
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
