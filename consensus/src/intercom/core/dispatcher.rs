use futures_util::future::BoxFuture;
use tycho_network::{Network, PeerId, PrivateOverlay, Request};
use tycho_util::metrics::HistogramGuard;

use crate::intercom::core::{
    BroadcastResponse, PointByIdResponse, QueryResponse, SignatureResponse,
};
use crate::models::{Point, PointIntegrityError};

#[derive(Clone)]
pub struct Dispatcher {
    overlay: PrivateOverlay,
    network: Network,
}

pub type PointQueryResult = anyhow::Result<PointByIdResponse<Result<Point, PointIntegrityError>>>;

impl Dispatcher {
    pub fn new(network: &Network, private_overlay: &PrivateOverlay) -> Self {
        Self {
            overlay: private_overlay.clone(),
            network: network.clone(),
        }
    }

    pub fn query_broadcast(
        &self,
        peer_id: &PeerId,
        request: &Request,
    ) -> BoxFuture<'static, (PeerId, anyhow::Result<BroadcastResponse>)> {
        let peer_id = *peer_id;
        let metric = HistogramGuard::begin("tycho_mempool_broadcast_query_dispatcher_time");
        let overlay = self.overlay.clone();
        let network = self.network.clone();

        let request = request.clone();

        let future = async move {
            let _task_duration = metric;
            let response = match overlay.query(&network, &peer_id, request).await {
                Ok(response) => response,
                Err(e) => return (peer_id, Err(e)),
            };
            let result = QueryResponse::parse_broadcast(&response);
            (peer_id, result.map_err(Into::into))
        };
        Box::pin(future)
    }

    pub fn query_signature(
        &self,
        peer_id: &PeerId,
        after_bcast: bool,
        request: &Request,
    ) -> BoxFuture<'static, (PeerId, bool, anyhow::Result<SignatureResponse>)> {
        let peer_id = *peer_id;
        let metric = HistogramGuard::begin("tycho_mempool_signature_query_dispatcher_time");
        let overlay = self.overlay.clone();
        let network = self.network.clone();

        let request = request.clone();

        let future = async move {
            let _task_duration = metric;
            let response = match overlay.query(&network, &peer_id, request).await {
                Ok(response) => response,
                Err(e) => return (peer_id, after_bcast, Err(e)),
            };
            let result = QueryResponse::parse_signature(&response);
            (peer_id, after_bcast, result.map_err(Into::into))
        };
        Box::pin(future)
    }

    pub async fn query_point(
        &self,
        peer_id: PeerId,
        request: Request,
    ) -> (PeerId, PointQueryResult) {
        let _task_duration = HistogramGuard::begin("tycho_mempool_download_query_dispatcher_time");

        let response = match self.overlay.query(&self.network, &peer_id, request).await {
            Ok(response) => response,
            Err(e) => return (peer_id, Err(e)),
        };

        let result = QueryResponse::parse_point_by_id(response).await;
        (peer_id, result.map_err(Into::into))
    }

    #[cfg(feature = "mock-feedback")]
    pub fn send_feedback(
        &self,
        peer_id: &PeerId,
        request: &Request,
    ) -> BoxFuture<'static, anyhow::Result<()>> {
        let peer_id = *peer_id;
        let overlay = self.overlay.clone();
        let network = self.network.clone();

        let request = request.clone();

        let future = async move { overlay.send(&network, &peer_id, request).await };
        Box::pin(future)
    }
}
