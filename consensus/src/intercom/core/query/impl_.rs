use bytes::Bytes;
use tl_proto::TlError;
use tycho_network::{PeerId, Request};
use tycho_util::metrics::HistogramGuard;

use crate::intercom::Dispatcher;
use crate::intercom::core::query::request::QueryRequest;
use crate::intercom::core::query::response::{
    BroadcastResponse, DownloadResponse, QueryResponse, SignatureResponse,
};
use crate::intercom::dependency::PeerDownloadPermit;
use crate::models::{Point, PointId, Round};

pub enum QueryError {
    Network(anyhow::Error),
    TlError(TlError),
}

pub struct BroadcastQuery {
    dispatcher: Dispatcher,
    request: Request,
}

impl BroadcastQuery {
    pub fn new(dispatcher: Dispatcher, point: &Point) -> Self {
        Self {
            dispatcher,
            request: QueryRequest::broadcast(point),
        }
    }
    pub async fn send(&self, peer_id: &PeerId) -> Result<BroadcastResponse, QueryError> {
        let _task_duration = HistogramGuard::begin("tycho_mempool_broadcast_query_dispatcher_time");

        let response = match self.dispatcher.query(peer_id, self.request.clone()).await {
            Ok(response) => response,
            Err(e) => return Err(QueryError::Network(e)),
        };
        QueryResponse::parse_broadcast(&response).map_err(QueryError::TlError)
    }
}

pub struct SignatureQuery {
    dispatcher: Dispatcher,
    request: Request,
}
impl SignatureQuery {
    pub fn new(dispatcher: Dispatcher, round: Round) -> Self {
        Self {
            dispatcher,
            request: QueryRequest::signature(round),
        }
    }
    pub async fn send(&self, peer_id: &PeerId) -> Result<SignatureResponse, QueryError> {
        let _task_duration = HistogramGuard::begin("tycho_mempool_signature_query_dispatcher_time");

        let response = match self.dispatcher.query(peer_id, self.request.clone()).await {
            Ok(response) => response,
            Err(e) => return Err(QueryError::Network(e)),
        };
        QueryResponse::parse_signature(&response).map_err(QueryError::TlError)
    }
}

#[derive(Clone)]
pub struct DownloadIdQuery(Request);
impl DownloadIdQuery {
    pub fn new(id: &PointId) -> Self {
        Self(QueryRequest::download(id))
    }

    pub async fn send_with(
        self,
        dispatcher: &Dispatcher,
        peer_permit: PeerDownloadPermit,
    ) -> Result<DownloadResponse<Bytes>, QueryError> {
        let _task_duration = HistogramGuard::begin("tycho_mempool_download_query_dispatcher_time");

        let response = match dispatcher.query(&peer_permit.peer_id, self.0).await {
            Ok(response) => response,
            Err(e) => return Err(QueryError::Network(e)),
        };

        drop(peer_permit);

        QueryResponse::parse_download(response).map_err(QueryError::TlError)
    }
}
