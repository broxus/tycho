use std::sync::Arc;

use anyhow::Result;
use quinn::{ConnectionError, ReadError, VarInt, WriteError};
use tokio::task::JoinSet;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use tycho_util::metrics::HistogramGuard;

use crate::network::config::NetworkConfig;
use crate::network::connection::{Connection, RecvStream, SendStream};
use crate::network::connection_manager::ActivePeers;
use crate::network::wire::{make_codec, recv_request, send_response};
use crate::types::{
    BoxCloneService, DisconnectReason, InboundRequestMeta, Response, Service, ServiceRequest,
};

// Histograms
const METRIC_IN_QUERIES_TIME: &str = "tycho_net_in_queries_time";
const METRIC_IN_MESSAGES_TIME: &str = "tycho_net_in_messages_time";

// Counters
const METRIC_IN_QUERIES_TOTAL: &str = "tycho_net_in_queries_total";
const METRIC_IN_MESSAGES_TOTAL: &str = "tycho_net_in_messages_total";
const METRIC_IN_REQUESTS_REJECTED_TOTAL: &str = "tycho_net_in_requests_rejected_total";

// Gauges
const METRIC_REQ_HANDLERS: &str = "tycho_net_req_handlers";
const METRIC_INFLIGHT_PER_PEER_HANDLERS: &str = "tycho_inflight_per_peer_handlers";
const METRIC_CONCURRENT_REQUESTS_PER_PEER: &str = "tycho_net_concurrent_requests_per_peer";

pub const LIMIT_EXCEEDED_ERROR_CODE: VarInt = unsafe { VarInt::from_u64_unchecked(0xdead) };

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
        tracing::debug!(peer_id = %self.connection.peer_id(), "request handler started");

        let mut guard =
            RequestHandlerGuard::new(self.config.clone(), &self.connection, &self.active_peers);

        let reason: ConnectionError = loop {
            guard.update_inflight_metrics();

            tokio::select! {
                // drain completed requests first
                biased;
                Some(req) = guard.inflight_requests.join_next() => {
                    metrics::gauge!(METRIC_REQ_HANDLERS).decrement(1);
                    if let Err(e) = req {
                        if e.is_panic() {
                            tracing::error!("request handler panicked");
                            std::panic::resume_unwind(e.into_panic());
                        }
                    }
                }

                uni = self.connection.accept_uni() => match uni {
                    Ok(mut stream) => {
                        tracing::trace!(id = %stream.id(), "incoming uni stream");
                        if guard.is_limit_reached() {
                             guard.reject_uni_stream(&mut stream);
                            continue;
                        }

                        let handler = UniStreamRequestHandler::new(
                            &self.config,
                            self.connection.request_meta().clone(),
                            self.service.clone(),
                            stream,
                        );

                        guard.inflight_requests.spawn(handler.handle());
                        metrics::counter!(METRIC_IN_MESSAGES_TOTAL).increment(1);
                        metrics::gauge!(METRIC_REQ_HANDLERS).increment(1);
                    },
                    Err(e) => {
                        tracing::trace!("failed to accept an incoming uni stream: {e:?}");
                        break e;
                    }
                },
                bi = self.connection.accept_bi() => match bi {
                    Ok((mut tx, mut rx)) => {
                        tracing::trace!(id = %tx.id(), "incoming bi stream");

                         if guard.is_limit_reached() {
                            guard.reject_bi_stream(&mut tx, &mut rx);
                            continue;
                        }

                        let handler = BiStreamRequestHandler::new(
                            &self.config,
                            self.connection.request_meta().clone(),
                            self.service.clone(),
                            tx,
                            rx,
                        );
                        guard.inflight_requests.spawn(handler.handle());
                        metrics::counter!(METRIC_IN_QUERIES_TOTAL).increment(1);
                        metrics::gauge!(METRIC_REQ_HANDLERS).increment(1);
                    }
                    Err(e) => {
                        tracing::trace!("failed to accept an incoming bi stream: {e:?}");
                        break e;
                    }
                },
            }
        };

        guard.reason = reason.into();
    }
}

struct RequestHandlerGuard<'a> {
    config: Arc<NetworkConfig>,
    connection: &'a Connection,
    active_peers: &'a ActivePeers,
    inflight_requests: JoinSet<()>,
    reason: DisconnectReason,
}

impl<'a> RequestHandlerGuard<'a> {
    fn new(
        config: Arc<NetworkConfig>,
        connection: &'a Connection,
        active_peers: &'a ActivePeers,
    ) -> Self {
        Self {
            config,
            connection,
            active_peers,
            inflight_requests: JoinSet::new(),
            reason: DisconnectReason::LocallyClosed,
        }
    }

    fn is_limit_reached(&self) -> bool {
        self.inflight_requests.len() >= self.config.max_concurrent_requests_per_peer
    }

    fn reject_uni_stream(&self, stream: &mut RecvStream) {
        tracing::debug!(
            peer_id = %self.connection.peer_id(),
            "request limit reached, rejecting uni stream"
        );
        let _ = stream.stop(LIMIT_EXCEEDED_ERROR_CODE);
        metrics::counter!(METRIC_IN_REQUESTS_REJECTED_TOTAL).increment(1);
    }

    fn reject_bi_stream(&self, tx: &mut SendStream, rx: &mut RecvStream) {
        tracing::debug!(
            peer_id = %self.connection.peer_id(),
            "request limit reached, rejecting bi stream"
        );
        let _ = tx.reset(LIMIT_EXCEEDED_ERROR_CODE);
        let _ = rx.stop(LIMIT_EXCEEDED_ERROR_CODE);
        metrics::counter!(METRIC_IN_REQUESTS_REJECTED_TOTAL).increment(1);
    }

    fn update_inflight_metrics(&self) {
        let inflight_count = self.inflight_requests.len() as f64;
        if self
            .config
            .connection_metrics
            .is_some_and(|x| x.should_export_peer_id())
        {
            metrics::gauge!(METRIC_INFLIGHT_PER_PEER_HANDLERS, "peer_id" => self.connection.peer_id().to_string()).set(inflight_count);
            metrics::gauge!(METRIC_CONCURRENT_REQUESTS_PER_PEER, "peer_id" => self.connection.peer_id().to_string()).set(inflight_count);
        }
    }
}

impl Drop for RequestHandlerGuard<'_> {
    fn drop(&mut self) {
        self.update_inflight_metrics();
        self.inflight_requests.abort_all();
        self.active_peers.remove_with_stable_id(
            self.connection.peer_id(),
            self.connection.stable_id(),
            self.reason,
        );
        tracing::debug!(peer_id = %self.connection.peer_id(), "request handler stopped");
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
        let _histogram = HistogramGuard::begin(METRIC_IN_MESSAGES_TIME);

        if let Err(e) = self.do_handle().await {
            tracing::trace!("request handler task failed: {e}");
        }
    }

    async fn do_handle(mut self) -> Result<()> {
        let req = recv_request(&mut self.recv_stream).await?;
        self.service
            .on_message(ServiceRequest {
                metadata: self.meta,
                body: req.body,
            })
            .await;
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
        let _histogram = HistogramGuard::begin(METRIC_IN_QUERIES_TIME);

        if let Err(e) = self.do_handle().await {
            let is_expected_error = e
                .downcast_ref::<WriteError>()
                .is_some_and(|e| matches!(e, WriteError::Stopped(_)))
                || e.downcast_ref::<ReadError>()
                    .is_some_and(|e| matches!(e, ReadError::Reset(_)));
            if !is_expected_error {
                tracing::debug!("bi stream handler failed: {e}");
            } else {
                tracing::trace!("bi stream ended: {e}");
            }
        }
    }

    async fn do_handle(mut self) -> Result<()> {
        let req = recv_request(&mut self.recv_stream).await?;
        let handler = self.service.on_query(ServiceRequest {
            metadata: self.meta,
            body: req.body,
        });

        let stopped = self.send_stream.get_mut().stopped();
        tokio::select! {
            res = handler => {
                if let Some(res) = res {
                    send_response(&mut self.send_stream, res).await?;
                }
                self.send_stream.get_mut().finish().expect("must not be closed twise");
                _ = self.send_stream.get_mut().stopped().await;
                Ok(())
            },
            _ = stopped => anyhow::bail!("send_stream closed by remote"),
        }
    }
}
