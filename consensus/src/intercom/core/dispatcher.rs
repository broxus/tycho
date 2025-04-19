use anyhow::Result;
use futures_util::future::BoxFuture;
use tycho_network::{Network, PeerId, PrivateOverlay, Request};
use tycho_util::metrics::HistogramGuard;

use crate::intercom::core::{
    BroadcastResponse, PointByIdResponse, QueryResponse, SignatureResponse,
};
use crate::models::Point;

#[derive(Clone)]
pub struct Dispatcher {
    overlay: PrivateOverlay,
    network: Network,
}
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
    ) -> BoxFuture<'static, (PeerId, Result<BroadcastResponse>)> {
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
            let result = QueryResponse::parse_broadcast(&response).map_err(Into::into);
            (peer_id, result)
        };
        Box::pin(future)
    }

    pub fn query_signature(
        &self,
        peer_id: &PeerId,
        after_bcast: bool,
        request: &Request,
    ) -> BoxFuture<'static, (PeerId, bool, Result<SignatureResponse>)> {
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
            let result = QueryResponse::parse_signature(&response).map_err(Into::into);
            (peer_id, after_bcast, result)
        };
        Box::pin(future)
    }

    pub fn query_point(
        &self,
        peer_id: &PeerId,
        request: &Request,
    ) -> BoxFuture<'static, (PeerId, Result<PointByIdResponse<Point>>)> {
        let peer_id = *peer_id;
        let metric = HistogramGuard::begin("tycho_mempool_download_query_dispatcher_time");
        let overlay = self.overlay.clone();
        let network = self.network.clone();

        let request = request.clone();

        let future = async move {
            let _task_duration = metric;
            let response = match overlay.query(&network, &peer_id, request).await {
                Ok(response) => response,
                Err(e) => return (peer_id, Err(e)),
            };
            let result = QueryResponse::parse_point_by_id(response).map_err(Into::into);
            (peer_id, result)
        };
        Box::pin(future)
    }
}
