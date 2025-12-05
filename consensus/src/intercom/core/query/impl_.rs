use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use tl_proto::TlError;
use tycho_network::{PeerId, Request};
use tycho_util::metrics::HistogramGuard;

use crate::intercom::Dispatcher;
use crate::intercom::core::query::request::QueryRequest;
use crate::intercom::core::query::response::{
    BroadcastResponse, DownloadResponse, QueryResponse, SignatureResponse,
};
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
pub struct DownloadQuery(Arc<DownloadQueryInner>);
struct DownloadQueryInner {
    dispatcher: Dispatcher,
    request: Request,
    point_id: PointId,
}

impl DownloadQuery {
    pub fn new(dispatcher: Dispatcher, point_id: &PointId) -> Self {
        Self(Arc::new(DownloadQueryInner {
            dispatcher,
            request: QueryRequest::download(point_id),
            point_id: *point_id,
        }))
    }

    pub fn point_id(&self) -> &PointId {
        &self.0.point_id
    }

    pub async fn send(&self, peer_id: &PeerId) -> Result<DownloadResponse<Bytes>, QueryError> {
        let permit = self.0.dispatcher.download_rate_limit().get(peer_id).await;

        let query_abort_guard = scopeguard::guard((), |()| {
            metrics::counter!("tycho_mempool_download_aborted_on_exit_count").increment(1);
        });
        let start = Instant::now(); // only completed, not aborted

        let request = self.0.request.clone();
        let query_result = self.0.dispatcher.query(peer_id, request).await;

        drop(permit);
        scopeguard::ScopeGuard::into_inner(query_abort_guard); // defuse aborted only
        metrics::histogram!("tycho_mempool_download_query_dispatcher_time").record(start.elapsed());

        match query_result {
            Ok(response) => QueryResponse::parse_download(response).map_err(QueryError::TlError),
            Err(e) => Err(QueryError::Network(e)),
        }
    }
}
