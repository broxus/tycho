use std::sync::Arc;

use anyhow::Result;
use tokio::task::JoinSet;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

use crate::network::config::NetworkConfig;
use crate::network::connection::{Connection, RecvStream, SendStream};
use crate::network::connection_manager::ActivePeers;
use crate::network::wire::{make_codec, recv_request, send_response};
use crate::types::{
    BoxCloneService, DisconnectReason, InboundRequestMeta, Response, Service, ServiceRequest,
};

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
                    }
                    Err(e) => {
                        tracing::trace!("failed to accept an incoming bi stream: {e:?}");
                        break e;
                    }
                },
                datagram = self.connection.read_datagram() => match datagram {
                    Ok(datagram) => {
                        tracing::trace!(byte_len = datagram.len(), "incoming datagram");

                        inflight_requests.spawn({
                            let metadata = self.connection.request_meta().clone();
                            let service = self.service.clone();
                            async move {
                                service
                                    .on_datagram(ServiceRequest {
                                        metadata,
                                        body: datagram,
                                    })
                                    .await;
                            }
                        });
                    },
                    Err(e) => {
                        tracing::trace!("failed to read datagram: {e:?}");
                        break e;
                    }
                },
                Some(req) = inflight_requests.join_next() => match req {
                    Ok(()) => tracing::trace!("request handler task completed"),
                    Err(e) => {
                        if e.is_panic() {
                            std::panic::resume_unwind(e.into_panic());
                        }
                        tracing::trace!("request handler task cancelled");
                    }
                }
            }
        };

        self.active_peers.remove_with_stable_id(
            self.connection.peer_id(),
            self.connection.stable_id(),
            DisconnectReason::from(reason),
        );

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
                self.send_stream.get_mut().finish().await?;
                Ok(())
            },
            _ = stopped => anyhow::bail!("send_stream closed by remote"),
        }
    }
}
