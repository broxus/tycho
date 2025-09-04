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
            let mut __guard = crate::__async_profile_guard__::Guard::new(
                concat!(module_path!(), "::async_block"),
                file!(),
                line!(),
            );
            let _task_duration = metric;
            let response = match {
                __guard.end_section(line!());
                let __result = overlay.query(&network, &peer_id, request).await;
                __guard.start_section(line!());
                __result
            } {
                Ok(response) => response,
                Err(e) => return (peer_id, Err(e)),
            };
            let start = std::time::Instant::now();
            let result = QueryResponse::parse_broadcast(&response);
            let elapsed = start.elapsed();
            if elapsed.as_millis() > 10 {
                tracing::warn!(
                    elapsed_ms = elapsed.as_millis(),
                    peer_id = %peer_id,
                    "long broadcast query response"
                );
            }
            let start = std::time::Instant::now();
            drop(response);
            if elapsed.as_millis() > 10 {
                tracing::warn!(
                    elapsed_ms = elapsed.as_millis(),
                    peer_id = %peer_id,
                    "long broadcast query drop"
                );
            }
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
            let mut __guard = crate::__async_profile_guard__::Guard::new(
                concat!(module_path!(), "::async_block"),
                file!(),
                line!(),
            );
            let _task_duration = metric;
            let response = match {
                __guard.end_section(line!());
                let __result = overlay.query(&network, &peer_id, request).await;
                __guard.start_section(line!());
                __result
            } {
                Ok(response) => response,
                Err(e) => return (peer_id, after_bcast, Err(e)),
            };
            let result = QueryResponse::parse_signature(&response);
            (peer_id, after_bcast, result.map_err(Into::into))
        };
        Box::pin(future)
    }
    pub fn query_point(
        &self,
        peer_id: &PeerId,
        request: &Request,
    ) -> BoxFuture<'static, (PeerId, PointQueryResult)> {
        let peer_id = *peer_id;
        let metric = HistogramGuard::begin("tycho_mempool_download_query_dispatcher_time");
        let overlay = self.overlay.clone();
        let network = self.network.clone();
        let request = request.clone();
        let future = async move {
            let mut __guard = crate::__async_profile_guard__::Guard::new(
                concat!(module_path!(), "::async_block"),
                file!(),
                line!(),
            );
            let _task_duration = metric;
            let response = match {
                __guard.end_section(line!());
                let __result = overlay.query(&network, &peer_id, request).await;
                __guard.start_section(line!());
                __result
            } {
                Ok(response) => response,
                Err(e) => return (peer_id, Err(e)),
            };
            let result = {
                __guard.end_section(line!());
                let __result = QueryResponse::parse_point_by_id(response).await;
                __guard.start_section(line!());
                __result
            };
            (peer_id, result.map_err(Into::into))
        };
        Box::pin(future)
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
                line!(),
            );
            {
                __guard.end_section(line!());
                let __result = overlay.send(&network, &peer_id, request).await;
                __guard.start_section(line!());
                __result
            }
        };
        Box::pin(future)
    }
}
