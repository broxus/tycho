use std::future::IntoFuture;
use std::sync::Arc;
use anyhow::Result;
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use quinn::ConnectionError;
use tokio::task::JoinHandle;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use tycho_util::metrics::HistogramGuard;
use crate::network::config::NetworkConfig;
use crate::network::connection::{Connection, RecvStream, SendStream};
use crate::network::connection_manager::ActivePeers;
use crate::network::wire::{make_codec, recv_request, send_response};
use crate::types::{
    BoxCloneService, DisconnectReason, InboundRequestMeta, Response, Service,
    ServiceRequest,
};
const METRIC_IN_QUERIES_TIME: &str = "tycho_net_in_queries_time";
const METRIC_IN_MESSAGES_TIME: &str = "tycho_net_in_messages_time";
const METRIC_IN_QUERIES_TOTAL: &str = "tycho_net_in_queries_total";
const METRIC_IN_MESSAGES_TOTAL: &str = "tycho_net_in_messages_total";
const METRIC_IN_REQUESTS_REJECTED_TOTAL: &str = "tycho_net_in_requests_rejected_total";
const METRIC_REQ_HANDLERS: &str = "tycho_net_req_handlers";
const METRIC_REQ_HANDLERS_PER_PEER: &str = "tycho_net_req_handlers_per_peer";
pub(crate) struct InboundRequestHandler {
    config: Arc<NetworkConfig>,
    connection: Connection,
    service: BoxCloneService<ServiceRequest, Response>,
    active_peers: ActivePeers,
}
impl InboundRequestHandler {
    pub fn new(
        config: Arc<NetworkConfig>,
        connection: Connection,
        service: BoxCloneService<ServiceRequest, Response>,
        active_peers: ActivePeers,
    ) -> Self {
        Self {
            config,
            connection,
            service,
            active_peers,
        }
    }
    pub async fn start(self) {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(start)),
            file!(),
            55u32,
        );
        tracing::debug!(
            peer_id = % self.connection.peer_id(), "request handler started"
        );
        let mut tracker = RequestTracker::new(
            self.config.as_ref(),
            &self.connection,
            &self.active_peers,
        );
        let reason: ConnectionError = loop {
            __guard.checkpoint(61u32);
            tracker.update_inflight_metrics();
            {
                __guard.end_section(64u32);
                let __result = tokio::select! {
                    biased; true = tracker.join_next() => {} uni = self.connection
                    .accept_uni() => match uni { Ok(stream) => tracker.track_uni(& self
                    .service, stream), Err(e) => {
                    tracing::trace!("failed to accept an incoming uni stream: {e:?}");
                    break e; } }, bi = self.connection.accept_bi() => match bi { Ok((tx,
                    rx)) => tracker.track_bi(& self.service, tx, rx), Err(e) => {
                    tracing::trace!("failed to accept an incoming bi stream: {e:?}");
                    break e; } },
                };
                __guard.start_section(64u32);
                __result
            }
        };
        tracker.reason = reason.into();
        {
            __guard.end_section(91u32);
            let __result = tracker.shutdown().await;
            __guard.start_section(91u32);
            __result
        };
    }
}
struct RequestTracker<'a> {
    config: &'a NetworkConfig,
    connection: &'a Connection,
    active_peers: &'a ActivePeers,
    inflight_requests_len: usize,
    inflight_requests: FuturesUnordered<JoinHandle<()>>,
    reason: DisconnectReason,
    peer_id_str: Arc<str>,
}
impl<'a> RequestTracker<'a> {
    fn new(
        config: &'a NetworkConfig,
        connection: &'a Connection,
        active_peers: &'a ActivePeers,
    ) -> Self {
        let peer_id_str = Arc::from(connection.peer_id().to_string());
        Self {
            config,
            connection,
            active_peers,
            inflight_requests_len: 0,
            inflight_requests: Default::default(),
            reason: DisconnectReason::LocallyClosed,
            peer_id_str,
        }
    }
    fn is_limit_reached(&self) -> bool {
        self.inflight_requests_len >= self.config.max_concurrent_requests_per_peer
    }
    async fn shutdown(&mut self) {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(shutdown)),
            file!(),
            128u32,
        );
        for handle in &self.inflight_requests {
            __guard.checkpoint(130u32);
            handle.abort();
        }
        while {
            __guard.end_section(135u32);
            let __result = self.join_next().await;
            __guard.start_section(135u32);
            __result
        } {
            __guard.checkpoint(135u32);
        }
    }
    async fn join_next(&mut self) -> bool {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(join_next)),
            file!(),
            138u32,
        );
        let Some(req) = ({
            __guard.end_section(139u32);
            let __result = self.inflight_requests.next().await;
            __guard.start_section(139u32);
            __result
        }) else {
            {
                __guard.end_section(140u32);
                return false;
            };
        };
        self.inflight_requests_len -= 1;
        metrics::gauge!(METRIC_REQ_HANDLERS).decrement(1);
        if let Err(e) = req && e.is_panic() {
            tracing::error!("request handler panicked");
            std::panic::resume_unwind(e.into_panic());
        }
        true
    }
    #[inline]
    fn track_uni(
        &mut self,
        service: &BoxCloneService<ServiceRequest, Response>,
        mut stream: RecvStream,
    ) {
        tracing::trace!(id = % stream.id(), "incoming uni stream");
        if self.is_limit_reached() {
            tracing::debug!(
                peer_id = % self.peer_id_str,
                "request limit reached, rejecting uni stream"
            );
            let _ = stream.stop(Connection::LIMIT_EXCEEDED_ERROR_CODE);
            metrics::counter!(METRIC_IN_REQUESTS_REJECTED_TOTAL).increment(1);
            return;
        }
        let handler = UniStreamRequestHandler::new(
            self.config,
            self.connection.request_meta().clone(),
            service.clone(),
            stream,
        );
        self.spawn_handler(handler.handle());
        metrics::counter!(METRIC_IN_MESSAGES_TOTAL).increment(1);
    }
    #[inline]
    fn track_bi(
        &mut self,
        service: &BoxCloneService<ServiceRequest, Response>,
        mut tx: SendStream,
        mut rx: RecvStream,
    ) {
        tracing::trace!(id = % tx.id(), "incoming bi stream");
        if self.is_limit_reached() {
            tracing::debug!(
                peer_id = % self.peer_id_str,
                "request limit reached, rejecting bi stream"
            );
            let _ = tx.reset(Connection::LIMIT_EXCEEDED_ERROR_CODE);
            let _ = rx.stop(Connection::LIMIT_EXCEEDED_ERROR_CODE);
            metrics::counter!(METRIC_IN_REQUESTS_REJECTED_TOTAL).increment(1);
            return;
        }
        let handler = BiStreamRequestHandler::new(
            self.config,
            self.connection.request_meta().clone(),
            service.clone(),
            tx,
            rx,
        );
        self.spawn_handler(handler.handle());
        metrics::counter!(METRIC_IN_QUERIES_TOTAL).increment(1);
    }
    fn spawn_handler<F>(&mut self, handler: F)
    where
        F: IntoFuture<Output = (), IntoFuture: Send + 'static>,
    {
        self.inflight_requests_len += 1;
        self.inflight_requests.push(tokio::spawn(handler.into_future()));
        metrics::gauge!(METRIC_REQ_HANDLERS).increment(1);
    }
    fn update_inflight_metrics(&self) {
        let metrics = &self.config.connection_metrics;
        if metrics.is_some_and(|x| x.should_export_peer_id()) {
            metrics::gauge!(
                METRIC_REQ_HANDLERS_PER_PEER, "peer_id" => self.peer_id_str.clone()
            )
                .set(self.inflight_requests_len as f64);
        }
    }
}
impl Drop for RequestTracker<'_> {
    fn drop(&mut self) {
        self.update_inflight_metrics();
        for handle in &self.inflight_requests {
            handle.abort();
        }
        self.active_peers
            .remove_with_stable_id(
                self.connection.peer_id(),
                self.connection.stable_id(),
                self.reason,
            );
        tracing::debug!(peer_id = % self.peer_id_str, "request handler stopped");
    }
}
struct UniStreamRequestHandler {
    meta: Arc<InboundRequestMeta>,
    service: BoxCloneService<ServiceRequest, Response>,
    recv_stream: FramedRead<RecvStream, LengthDelimitedCodec>,
}
impl UniStreamRequestHandler {
    fn new(
        config: &NetworkConfig,
        meta: Arc<InboundRequestMeta>,
        service: BoxCloneService<ServiceRequest, Response>,
        recv_stream: RecvStream,
    ) -> Self {
        Self {
            meta,
            service,
            recv_stream: FramedRead::new(recv_stream, make_codec(config)),
        }
    }
    async fn handle(self) {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(handle)),
            file!(),
            272u32,
        );
        let _histogram = HistogramGuard::begin(METRIC_IN_MESSAGES_TIME);
        if let Err(e) = {
            __guard.end_section(275u32);
            let __result = self.do_handle().await;
            __guard.start_section(275u32);
            __result
        } {
            tracing::trace!("request handler task failed: {e}");
        }
    }
    async fn do_handle(mut self) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(do_handle)),
            file!(),
            280u32,
        );
        let req = {
            __guard.end_section(281u32);
            let __result = recv_request(&mut self.recv_stream).await;
            __guard.start_section(281u32);
            __result
        }?;
        {
            __guard.end_section(287u32);
            let __result = self
                .service
                .on_message(ServiceRequest {
                    metadata: self.meta,
                    body: req.body,
                })
                .await;
            __guard.start_section(287u32);
            __result
        };
        Ok(())
    }
}
struct BiStreamRequestHandler {
    meta: Arc<InboundRequestMeta>,
    service: BoxCloneService<ServiceRequest, Response>,
    send_stream: FramedWrite<SendStream, LengthDelimitedCodec>,
    recv_stream: FramedRead<RecvStream, LengthDelimitedCodec>,
}
impl BiStreamRequestHandler {
    fn new(
        config: &NetworkConfig,
        meta: Arc<InboundRequestMeta>,
        service: BoxCloneService<ServiceRequest, Response>,
        send_stream: SendStream,
        recv_stream: RecvStream,
    ) -> Self {
        Self {
            meta,
            service,
            send_stream: FramedWrite::new(send_stream, make_codec(config)),
            recv_stream: FramedRead::new(recv_stream, make_codec(config)),
        }
    }
    async fn handle(self) {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(handle)),
            file!(),
            315u32,
        );
        let _histogram = HistogramGuard::begin(METRIC_IN_QUERIES_TIME);
        if let Err(e) = {
            __guard.end_section(318u32);
            let __result = self.do_handle().await;
            __guard.start_section(318u32);
            __result
        } {
            tracing::trace!("request handler task failed: {e}");
        }
    }
    async fn do_handle(mut self) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(do_handle)),
            file!(),
            323u32,
        );
        let req = {
            __guard.end_section(324u32);
            let __result = recv_request(&mut self.recv_stream).await;
            __guard.start_section(324u32);
            __result
        }?;
        let handler = self
            .service
            .on_query(ServiceRequest {
                metadata: self.meta,
                body: req.body,
            });
        let stopped = self.send_stream.get_mut().stopped();
        {
            __guard.end_section(331u32);
            let __result = tokio::select! {
                res = handler => { if let Some(res) = res { send_response(& mut self
                .send_stream, res). await ?; } self.send_stream.get_mut().finish()
                .expect("must not be closed twise"); _ = self.send_stream.get_mut()
                .stopped(). await; Ok(()) }, _ = stopped =>
                anyhow::bail!("send_stream closed by remote"),
            };
            __guard.start_section(331u32);
            __result
        }
    }
}
