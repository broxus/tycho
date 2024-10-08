use anyhow::Result;
use futures_util::future::BoxFuture;
use tl_proto::TlError;
use tycho_network::{
    try_handle_prefix, try_handle_prefix_with_offset, Network, PeerId, PrivateOverlay, Request,
};
use tycho_util::metrics::HistogramGuard;

use crate::intercom::core::dto::{
    BroadcastMpResponse, BroadcastQuery, PointMpResponse, PointQuery, SignatureMpResponse,
    SignatureQuery,
};
use crate::intercom::dto::{BroadcastResponse, PointByIdResponse, SignatureResponse};
use crate::models::{Point, PointId, Round};

#[derive(Clone)]
pub struct Dispatcher {
    overlay: PrivateOverlay,
    network: Network,
}
impl Dispatcher {
    pub fn broadcast_request(point: &Point) -> Request {
        Request::from_tl(BroadcastQuery(point.clone()))
    }

    pub fn signature_request(round: Round) -> Request {
        Request::from_tl(SignatureQuery(round))
    }

    pub fn point_by_id_request(id: PointId) -> Request {
        Request::from_tl(PointQuery(id))
    }
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

            let (constructor, _) = match try_handle_prefix(&response.body) {
                Ok(data) => data,
                Err(e) => return (peer_id, Err(e.into())),
            };

            if constructor != BroadcastMpResponse::TL_ID {
                tracing::error!(received = constructor, tl_id = %BroadcastMpResponse::TL_ID, "Wrong constructor tag for broadcast response");
                return (peer_id, Err(TlError::InvalidData.into()));
            }

            (peer_id, Ok(BroadcastResponse))
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

            let (constructor, body) = match try_handle_prefix_with_offset(&response.body) {
                Ok(data) => data,
                Err(e) => return (peer_id, Err(e.into())),
            };

            if constructor != PointMpResponse::<Point>::TL_ID {
                tracing::error!(received = constructor, tl_id = %PointMpResponse::<Point>::TL_ID, "Wrong constructor tag for point response");
                return (peer_id, Err(TlError::InvalidData.into()));
            }

            let response = match tl_proto::deserialize::<PointByIdResponse<Point>>(body) {
                Ok(data) => data,
                Err(e) => return (peer_id, Err(e.into())),
            };

            (peer_id, Ok(response))
        };
        Box::pin(future)
    }

    pub fn query_signature(
        &self,
        peer_id: &PeerId,
        request: &Request,
    ) -> BoxFuture<'static, (PeerId, Result<SignatureResponse>)> {
        let peer_id = *peer_id;
        let metric = HistogramGuard::begin("tycho_mempool_signature_query_dispatcher_time");
        let overlay = self.overlay.clone();
        let network = self.network.clone();

        let request = request.clone();

        let future = async move {
            let _task_duration = metric;
            let response = match overlay.query(&network, &peer_id, request).await {
                Ok(response) => response,
                Err(e) => return (peer_id, Err(e)),
            };

            let (constructor, body) = match try_handle_prefix_with_offset(&response.body) {
                Ok(data) => data,
                Err(e) => return (peer_id, Err(e.into())),
            };

            if constructor != SignatureMpResponse::TL_ID {
                tracing::error!(received = constructor, tl_id = %SignatureMpResponse::TL_ID, "Wrong constructor tag for signature response");
                return (peer_id, Err(TlError::InvalidData.into()));
            }

            let response = match tl_proto::deserialize::<SignatureResponse>(body) {
                Ok(data) => data,
                Err(e) => return (peer_id, Err(e.into())),
            };

            (peer_id, Ok(response))
        };
        Box::pin(future)
    }
}
