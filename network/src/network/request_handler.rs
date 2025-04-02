use std::sync::Arc;

use anyhow::Result;
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

// Gauges
const METRIC_REQ_HANDLERS: &str = "tycho_net_req_handlers";

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

        struct ClearOnDrop<'a> {
            handler: &'a InboundRequestHandler,
            reason: DisconnectReason,
        }

        impl Drop for ClearOnDrop<'_> {
            fn drop(&mut self) {
                self.handler.active_peers.remove_with_stable_id(
                    self.handler.connection.peer_id(),
                    self.handler.connection.stable_id(),
                    self.reason,
                );
            }
        }

        let mut clear_on_drop = ClearOnDrop {
            handler: &self,
            reason: DisconnectReason::LocallyClosed,
        };

        let mut inflight_requests = JoinSet::<()>::new();

        let reason: quinn::ConnectionError = loop {
            tokio::select! {
                uni = self.connection.accept_uni() => match uni {
                    Ok(stream) => {
                        tracing::trace!(id = %stream.id(), "incoming uni stream");
                        let handler = UniStreamRequestHandler::new(
                            &self.config,
                            self.connection.request_meta().clone(),
                            self.service.clone(),
                            stream,
                        );
                        inflight_requests.spawn(handler.handle());
                        metrics::counter!(METRIC_IN_MESSAGES_TOTAL).increment(1);
                        metrics::gauge!(METRIC_REQ_HANDLERS).increment(1);
                    },
                    Err(e) => {
                        tracing::trace!("failed to accept an incoming uni stream: {e:?}");
                        break e;
                    }
                },
                bi = self.connection.accept_bi() => match bi {
                    Ok((tx, rx)) => {
                        tracing::trace!(id = %tx.id(), "incoming bi stream");
                        let handler = BiStreamRequestHandler::new(
                            &self.config,
                            self.connection.request_meta().clone(),
                            self.service.clone(),
                            tx,
                            rx,
                        );
                        inflight_requests.spawn(handler.handle());
                        metrics::counter!(METRIC_IN_QUERIES_TOTAL).increment(1);
                        metrics::gauge!(METRIC_REQ_HANDLERS).increment(1);
                    }
                    Err(e) => {
                        tracing::trace!("failed to accept an incoming bi stream: {e:?}");
                        break e;
                    }
                },
                Some(req) = inflight_requests.join_next() => {
                    metrics::gauge!(METRIC_REQ_HANDLERS).decrement(1);
                    match req {
                        Ok(()) => tracing::trace!("request handler task completed"),
                        Err(e) => {
                            if e.is_panic() {
                                std::panic::resume_unwind(e.into_panic());
                            }
                            tracing::trace!("request handler task cancelled");
                        }
                    }
                }
            }
        };
        clear_on_drop.reason = reason.into();

        inflight_requests.shutdown().await;
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
            tracing::trace!("request handler task failed: {e}");
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
