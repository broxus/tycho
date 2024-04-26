use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};

use tycho_network::{PeerId, Response, Service, ServiceRequest, Version};
use tycho_util::futures::BoxFutureOrNoop;

use crate::intercom::core::dto::{MPRemoteResult, MPRequest, MPResponse};
use crate::intercom::dto::{PointByIdResponse, SignatureResponse};
use crate::intercom::BroadcastFilter;
use crate::models::{PointId, Round};

pub struct Responder(Arc<ResponderInner>);

impl Responder {
    pub fn new(
        local_id: Arc<String>,
        broadcast_filter: BroadcastFilter,
        signature_requests: mpsc::UnboundedSender<(
            Round,
            PeerId,
            oneshot::Sender<SignatureResponse>,
        )>,
        uploads: mpsc::UnboundedSender<(PointId, oneshot::Sender<PointByIdResponse>)>,
    ) -> Self {
        Self(Arc::new(ResponderInner {
            local_id,
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
    local_id: Arc<String>,
    broadcast_filter: BroadcastFilter,
    signature_requests: mpsc::UnboundedSender<(Round, PeerId, oneshot::Sender<SignatureResponse>)>,
    uploads: mpsc::UnboundedSender<(PointId, oneshot::Sender<PointByIdResponse>)>,
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
            MPRequest::PointById(point_id) => {
                let (tx, rx) = oneshot::channel();
                self.uploads.send((point_id, tx)).ok();
                match rx.await {
                    Ok(response) => MPResponse::PointById(response),
                    Err(e) => panic!("Responder point by id await of request failed: {e}"),
                };
                MPResponse::PointById(PointByIdResponse(None))
            }
            MPRequest::Broadcast(point) => {
                MPResponse::Broadcast(self.broadcast_filter.add(Arc::new(point)))
            }
            MPRequest::Signature(round) => {
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
                            self.local_id,
                            req.metadata.peer_id
                        );
                        MPResponse::Signature(response)
                    }
                }
            }
        };

        Some(Response {
            version: Version::default(),
            body: Bytes::from(match bincode::serialize(&MPRemoteResult::Ok(response)) {
                Ok(data) => data,
                Err(e) => {
                    tracing::error!("failed to serialize response to {:?}: {e:?}", req.metadata);
                    bincode::serialize(&MPRemoteResult::Err(format!("internal error")))
                        .expect("must not fail")
                }
            }),
        })
    }
}
