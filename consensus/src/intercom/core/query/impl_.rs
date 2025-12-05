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

        let permit = match self.dispatcher.bcast_rate_limit().broadcast(peer_id) {
            Ok(permit) => permit,
            Err(()) => {
                return Err(QueryError::Network(anyhow::anyhow!(
                    "query rate limit on sender side"
                )));
            }
        };

        let response = match self.dispatcher.query(peer_id, self.request.clone()).await {
            Ok(response) => response,
            Err(e) => return Err(QueryError::Network(e)),
        };

        drop(permit);

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

        let permit = match self.dispatcher.bcast_rate_limit().sig_query(peer_id) {
            Ok(permit) => permit,
            Err(()) => {
                return Err(QueryError::Network(anyhow::anyhow!(
                    "query rate limit on sender side"
                )));
            }
        };

        let response = match self.dispatcher.query(peer_id, self.request.clone()).await {
            Ok(response) => response,
            Err(e) => return Err(QueryError::Network(e)),
        };

        drop(permit);

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
        let permit = {
            let permit_abort_guard = scopeguard::guard(Instant::now(), |start| {
                metrics::histogram!("tycho_mempool_download_permit_aborted_time")
                    .record(start.elapsed());
            });

            let permit = self.0.dispatcher.download_rate_limit().get(peer_id).await;

            let start = scopeguard::ScopeGuard::into_inner(permit_abort_guard);
            metrics::histogram!("tycho_mempool_download_permit_acquired_time")
                .record(start.elapsed());
            permit
        };
        let query_abort_guard = scopeguard::guard(Instant::now(), |_| {
            metrics::counter!("tycho_mempool_download_aborted_queries_count").increment(1);
        });

        let request = self.0.request.clone();
        let query_result = self.0.dispatcher.query(peer_id, request).await;

        drop(permit);
        let start = scopeguard::ScopeGuard::into_inner(query_abort_guard);
        metrics::histogram!("tycho_mempool_download_query_dispatcher_time").record(start.elapsed());

        match query_result {
            Ok(response) => QueryResponse::parse_download(response).map_err(QueryError::TlError),
            Err(e) => Err(QueryError::Network(e)),
        }
    }
}
