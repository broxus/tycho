use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};

use tycho_network::{PeerId, Response, Service, ServiceRequest, Version};
use tycho_util::futures::BoxFutureOrNoop;

use crate::intercom::core::dto::{MPQuery, MPQueryResult, MPResponse};
use crate::intercom::dto::{PointByIdResponse, SignatureResponse};
use crate::intercom::BroadcastFilter;
use crate::models::{Point, PointId, Round, Ugly};

pub struct Responder(Arc<ResponderInner>);

impl Responder {
    pub fn new(
        log_id: Arc<String>,
        broadcast_filter: BroadcastFilter,
        signature_requests: mpsc::UnboundedSender<(
            Round,
            PeerId,
            oneshot::Sender<SignatureResponse>,
        )>,
        uploads: mpsc::UnboundedSender<(PointId, oneshot::Sender<PointByIdResponse>)>,
    ) -> Self {
        Self(Arc::new(ResponderInner {
            log_id,
            broadcast_filter,
            signature_requests,
            uploads,
        }))
    }
}

impl Service<ServiceRequest> for Responder {
    type QueryResponse = Response;
    type OnQueryFuture = BoxFutureOrNoop<Option<Self::QueryResponse>>;
    type OnMessageFuture = futures_util::future::Ready<()>;
    type OnDatagramFuture = futures_util::future::Ready<()>;

    #[inline]
    fn on_query(&self, req: ServiceRequest) -> Self::OnQueryFuture {
        BoxFutureOrNoop::future(self.0.clone().handle_query(req))
    }

    #[inline]
    fn on_message(&self, req: ServiceRequest) -> Self::OnMessageFuture {
        futures_util::future::ready(self.0.clone().handle_broadcast(req))
    }

    #[inline]
    fn on_datagram(&self, _req: ServiceRequest) -> Self::OnDatagramFuture {
        futures_util::future::ready(())
    }
}

struct ResponderInner {
    // state and storage components go here
    log_id: Arc<String>,
    broadcast_filter: BroadcastFilter,
    signature_requests: mpsc::UnboundedSender<(Round, PeerId, oneshot::Sender<SignatureResponse>)>,
    uploads: mpsc::UnboundedSender<(PointId, oneshot::Sender<PointByIdResponse>)>,
}

impl ResponderInner {
    async fn handle_query(self: Arc<Self>, req: ServiceRequest) -> Option<Response> {
        let body = match bincode::deserialize::<MPQuery>(&req.body) {
            Ok(body) => body,
            Err(e) => {
                tracing::error!("unexpected request from {:?}: {e:?}", req.metadata.peer_id);
                // malformed request is a reason to ignore it
                return None;
            }
        };

        let response = match body {
            MPQuery::PointById(point_id) => {
                let (tx, rx) = oneshot::channel();
                self.uploads.send((point_id.clone(), tx)).ok();
                match rx.await {
                    Ok(response) => {
                        tracing::debug!(
                            "{} upload to {:.4?} : {:?} {}",
                            self.log_id,
                            req.metadata.peer_id,
                            point_id.ugly(),
                            response.0.as_ref().map_or("not found", |_| "ok"),
                        );
                        MPResponse::PointById(response)
                    }
                    Err(e) => panic!("Responder point by id await of request failed: {e}"),
                }
            }
            MPQuery::Signature(round) => {
                let (tx, rx) = oneshot::channel();
                self.signature_requests
                    .send((round, req.metadata.peer_id.clone(), tx))
                    .ok();
                match rx.await {
                    Ok(response) => MPResponse::Signature(response),
                    Err(e) => {
                        let response = SignatureResponse::TryLater;
                        tracing::error!(
                            "{} responder => collector {:.4?} @ {round:?} : \
                            {response:?} due to oneshot {e}",
                            self.log_id,
                            req.metadata.peer_id
                        );
                        MPResponse::Signature(response)
                    }
                }
            }
        };

        Some(Response {
            version: Version::default(),
            body: Bytes::from(match bincode::serialize(&MPQueryResult::Ok(response)) {
                Ok(data) => data,
                Err(e) => {
                    tracing::error!("failed to serialize response to {:?}: {e:?}", req.metadata);
                    bincode::serialize(&MPQueryResult::Err("internal error".to_string()))
                        .expect("must not fail")
                }
            }),
        })
    }

    fn handle_broadcast(self: Arc<Self>, req: ServiceRequest) {
        match bincode::deserialize::<Point>(&req.body) {
            Ok(point) => self.broadcast_filter.add(Arc::new(point)),
            Err(e) => {
                tracing::error!(
                    "unexpected broadcast from {:?}: {e:?}",
                    req.metadata.peer_id
                );
                // malformed request is a reason to ignore it
                return;
            }
        };
    }
}
