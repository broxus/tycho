use futures_util::future::BoxFuture;
use tycho_network::{Network, PeerId, PrivateOverlay, Request};
use tycho_util::metrics::HistogramGuard;
use crate::intercom::core::{
    BroadcastResponse, PointByIdResponse, QueryResponse, SignatureResponse,
};
use crate::intercom::dependency::PeerDownloadPermit;
use crate::models::{Point, PointIntegrityError};
#[derive(Clone)]
pub struct Dispatcher {
    overlay: PrivateOverlay,
    network: Network,
}
pub type PointQueryResult = anyhow::Result<
    PointByIdResponse<Result<Point, PointIntegrityError>>,
>;
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
        let metric = HistogramGuard::begin(
            "tycho_mempool_broadcast_query_dispatcher_time",
        );
        let overlay = self.overlay.clone();
        let network = self.network.clone();
        let request = request.clone();
        let future = async move {
            let mut __guard = crate::__async_profile_guard__::Guard::new(
                concat!(module_path!(), "::async_block"),
                file!(),
                39u32,
            );
            let _task_duration = metric;
            let response = match {
                __guard.end_section(41u32);
                let __result = overlay.query(&network, &peer_id, request).await;
                __guard.start_section(41u32);
                __result
            } {
                Ok(response) => response,
                Err(e) => {
                    __guard.end_section(43u32);
                    return (peer_id, Err(e));
                }
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
        let metric = HistogramGuard::begin(
            "tycho_mempool_signature_query_dispatcher_time",
        );
        let overlay = self.overlay.clone();
        let network = self.network.clone();
        let request = request.clone();
        let future = async move {
            let mut __guard = crate::__async_profile_guard__::Guard::new(
                concat!(module_path!(), "::async_block"),
                file!(),
                64u32,
            );
            let _task_duration = metric;
            let response = match {
                __guard.end_section(66u32);
                let __result = overlay.query(&network, &peer_id, request).await;
                __guard.start_section(66u32);
                __result
            } {
                Ok(response) => response,
                Err(e) => {
                    __guard.end_section(68u32);
                    return (peer_id, after_bcast, Err(e));
                }
            };
            let result = QueryResponse::parse_signature(&response);
            (peer_id, after_bcast, result.map_err(Into::into))
        };
        Box::pin(future)
    }
    pub async fn query_point(
        &self,
        peer_permit: PeerDownloadPermit,
        request: Request,
    ) -> (PeerId, PointQueryResult) {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(query_point)),
            file!(),
            80u32,
        );
        let peer_permit = peer_permit;
        let request = request;
        let _task_duration = HistogramGuard::begin(
            "tycho_mempool_download_query_dispatcher_time",
        );
        let peer_id = peer_permit.peer_id;
        let response = match {
            __guard.end_section(84u32);
            let __result = self.overlay.query(&self.network, &peer_id, request).await;
            __guard.start_section(84u32);
            __result
        } {
            Ok(response) => response,
            Err(e) => {
                __guard.end_section(86u32);
                return (peer_id, Err(e));
            }
        };
        drop(peer_permit);
        let result = {
            __guard.end_section(91u32);
            let __result = QueryResponse::parse_point_by_id(response).await;
            __guard.start_section(91u32);
            __result
        };
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
        let future = async move {
            let mut __guard = crate::__async_profile_guard__::Guard::new(
                concat!(module_path!(), "::async_block"),
                file!(),
                107u32,
            );
            {
                __guard.end_section(107u32);
                let __result = overlay.send(&network, &peer_id, request).await;
                __guard.start_section(107u32);
                __result
            }
        };
        Box::pin(future)
    }
}
