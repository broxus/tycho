use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};

use tycho_network::{PeerId, Response, Service, ServiceRequest, Version};
use tycho_util::futures::BoxFutureOrNoop;

use crate::intercom::core::dto::{MPRemoteResult, MPRequest, MPResponse};
use crate::intercom::dto::{PointByIdResponse, SignatureResponse};
use crate::intercom::BroadcastFilter;
use crate::models::Round;

pub struct Responder(Arc<ResponderInner>);

impl Responder {
    pub fn new(
        broadcast_filter: Arc<BroadcastFilter>,
        signature_requests: mpsc::UnboundedSender<(
            Round,
            PeerId,
            oneshot::Sender<SignatureResponse>,
        )>,
    ) -> Self {
        Self(Arc::new(ResponderInner {
            broadcast_filter,
            signature_requests,
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
        BoxFutureOrNoop::future(self.0.clone().handle(req))
    }

    #[inline]
    fn on_message(&self, _req: ServiceRequest) -> Self::OnMessageFuture {
        futures_util::future::ready(())
    }

    #[inline]
    fn on_datagram(&self, _req: ServiceRequest) -> Self::OnDatagramFuture {
        futures_util::future::ready(())
    }
}

struct ResponderInner {
    // state and storage components go here
    broadcast_filter: Arc<BroadcastFilter>,
    signature_requests: mpsc::UnboundedSender<(Round, PeerId, oneshot::Sender<SignatureResponse>)>,
}

impl ResponderInner {
    async fn handle(self: Arc<Self>, req: ServiceRequest) -> Option<Response> {
        let body = match bincode::deserialize::<MPRequest>(&req.body) {
            Ok(body) => body,
            Err(e) => {
                tracing::error!("unexpected request from {:?}: {e:?}", req.metadata.peer_id);
                // malformed request is a reason to ignore it
                return None;
            }
        };

        let response = match body {
            MPRequest::PointById(point_id) => MPResponse::PointById(PointByIdResponse(None)),
            MPRequest::Broadcast(point) => {
                MPResponse::Broadcast(self.broadcast_filter.add(Arc::new(point)).await)
            }
            MPRequest::Signature(round) => {
                let (tx, rx) = oneshot::channel();
                _ = self
                    .signature_requests
                    .send((round, req.metadata.peer_id.clone(), tx));
                match rx.await {
                    Ok(response) => MPResponse::Signature(response),
                    Err(_) => MPResponse::Signature(SignatureResponse::TryLater),
                }
            }
        };

        let res = Some(Response {
            version: Version::default(),
            body: Bytes::from(match bincode::serialize(&MPRemoteResult::Ok(response)) {
                Ok(data) => data,
                Err(e) => {
                    tracing::error!("failed to serialize response to {:?}: {e:?}", req.metadata);
                    bincode::serialize(&MPRemoteResult::Err(format!("internal error")))
                        .expect("must not fail")
                }
            }),
        });
        res
    }
}
