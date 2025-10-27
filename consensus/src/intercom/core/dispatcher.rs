use tycho_network::{Network, PeerId, PrivateOverlay, Request, Response};
use tycho_util::metrics::HistogramGuard;

use crate::intercom::core::query::error::PointByIdQueryError;
use crate::intercom::core::query::request::QueryRequest;
use crate::intercom::core::query::response::QueryResponse;
use crate::intercom::core::{BroadcastResponse, PointByIdResponse, QueryError, SignatureResponse};
use crate::intercom::dependency::PeerDownloadPermit;
use crate::models::{Point, PointId, Round};

#[derive(Clone)]
pub struct Dispatcher {
    network: Network,
    overlay: PrivateOverlay,
}

impl Dispatcher {
    pub fn new(network: &Network, private_overlay: &PrivateOverlay) -> Self {
        Self {
            network: network.clone(),
            overlay: private_overlay.clone(),
        }
    }
    async fn query(&self, peer_id: &PeerId, request: Request) -> anyhow::Result<Response> {
        self.overlay.query(&self.network, peer_id, request).await
    }
}

pub struct BroadcastRequest {
    dispatcher: Dispatcher,
    request: Request,
}
impl BroadcastRequest {
    pub fn new(dispatcher: Dispatcher, point: &Point) -> Self {
        Self {
            dispatcher,
            request: QueryRequest::broadcast(point),
        }
    }
    pub async fn query(&self, peer_id: &PeerId) -> Result<BroadcastResponse, QueryError> {
        let _task_duration = HistogramGuard::begin("tycho_mempool_broadcast_query_dispatcher_time");

        let response = match self.dispatcher.query(peer_id, self.request.clone()).await {
            Ok(response) => response,
            Err(e) => return Err(QueryError::Network(e)),
        };
        QueryResponse::parse_broadcast(&response).map_err(QueryError::TlError)
    }
}

pub struct SignatureRequest {
    dispatcher: Dispatcher,
    request: Request,
}
impl SignatureRequest {
    pub fn new(dispatcher: Dispatcher, round: Round) -> Self {
        Self {
            dispatcher,
            request: QueryRequest::signature(round),
        }
    }
    pub async fn query(&self, peer_id: &PeerId) -> Result<SignatureResponse, QueryError> {
        let _task_duration = HistogramGuard::begin("tycho_mempool_signature_query_dispatcher_time");

        let response = match self.dispatcher.query(peer_id, self.request.clone()).await {
            Ok(response) => response,
            Err(e) => return Err(QueryError::Network(e)),
        };
        QueryResponse::parse_signature(&response).map_err(QueryError::TlError)
    }
}

#[derive(Clone)]
pub struct PointByIdRequest(Request);
impl PointByIdRequest {
    pub fn new(id: &PointId) -> Self {
        Self(QueryRequest::point_by_id(id))
    }
    pub async fn query(
        self,
        dispatcher: &Dispatcher,
        peer_permit: PeerDownloadPermit,
    ) -> Result<PointByIdResponse<Point>, PointByIdQueryError> {
        let _task_duration = HistogramGuard::begin("tycho_mempool_download_query_dispatcher_time");

        let response = match dispatcher.query(&peer_permit.peer_id, self.0).await {
            Ok(response) => response,
            Err(e) => return Err(PointByIdQueryError::Network(e)),
        };

        drop(peer_permit);

        QueryResponse::parse_point_by_id(response).await
    }
}

#[cfg(feature = "mock-feedback")]
pub struct MockFeedbackMessage {
    dispatcher: Dispatcher,
    request: Request,
}
#[cfg(feature = "mock-feedback")]
impl MockFeedbackMessage {
    pub fn new(dispatcher: Dispatcher, round: Round) -> Self {
        Self {
            dispatcher,
            request: Request::from_tl(crate::mock_feedback::RoundBoxed { round }),
        }
    }
    pub async fn send(&self, peer_id: &PeerId) -> anyhow::Result<()> {
        let overlay = &self.dispatcher.overlay;
        let network = &self.dispatcher.network;
        overlay.send(network, peer_id, self.request.clone()).await
    }
}
