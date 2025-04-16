use anyhow::Result;
use futures_util::future::BoxFuture;
use tycho_network::{Network, PeerId, PrivateOverlay, Request};
use tycho_util::metrics::HistogramGuard;

use crate::intercom::core::dto::{QueryTag, SendWrapper};
use crate::intercom::dto::{BroadcastResponse, PointByIdResponse, SignatureResponse};
use crate::models::{Point, PointId, Round};

#[derive(Clone)]
pub struct Dispatcher {
    overlay: PrivateOverlay,
    network: Network,
}
impl Dispatcher {
    pub fn broadcast_request(point: &Point) -> Request {
        Request::from_tl(SendWrapper {
            tag: QueryTag::Broadcast,
            body: point,
        })
    }

    pub fn signature_request(round: Round) -> Request {
        Request::from_tl(SendWrapper {
            tag: QueryTag::Signature,
            body: &round,
        })
    }

    pub fn point_by_id_request(id: &PointId) -> Request {
        Request::from_tl(SendWrapper {
            tag: QueryTag::PointById,
            body: id,
        })
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

            let result = tl_proto::deserialize::<BroadcastResponse>(&response.body);

            (peer_id, result.map_err(Into::into))
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

            let response = match tl_proto::deserialize::<PointByIdResponse<&[u8]>>(&response.body) {
                Ok(response) => response,
                Err(e) => return (peer_id, Err(e.into())),
            };

            let result = match response {
                PointByIdResponse::Defined(bytes) => Point::verify_hash_inner(bytes)
                    .and_then(|_| tl_proto::deserialize::<Point>(bytes))
                    .map(PointByIdResponse::Defined),
                PointByIdResponse::DefinedNone => Ok(PointByIdResponse::DefinedNone),
                PointByIdResponse::TryLater => Ok(PointByIdResponse::TryLater),
            };

            (peer_id, result.map_err(Into::into))
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

            let result = tl_proto::deserialize::<SignatureResponse>(&response.body);

            (peer_id, after_bcast, result.map_err(Into::into))
        };
        Box::pin(future)
    }
}
