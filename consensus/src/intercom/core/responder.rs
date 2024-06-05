use std::sync::Arc;

use arc_swap::ArcSwapOption;
use tycho_network::{Response, Service, ServiceRequest};

use crate::dag::DagRound;
use crate::effects::{AltFormat, CurrentRoundContext, Effects};
use crate::intercom::broadcast::Signer;
use crate::intercom::core::dto::{MPQuery, MPResponse};
use crate::intercom::dto::{PointByIdResponse, SignatureResponse};
use crate::intercom::{BroadcastFilter, Uploader};
use crate::models::Point;

#[derive(Clone, Default)]
pub struct Responder(Arc<ArcSwapOption<ResponderInner>>);

struct ResponderInner {
    // state and storage components go here
    broadcast_filter: BroadcastFilter,
    top_dag_round: DagRound,
    effects: Effects<CurrentRoundContext>,
}

impl Responder {
    pub fn update(
        &self,
        broadcast_filter: &BroadcastFilter,
        top_dag_round: &DagRound,
        round_effects: &Effects<CurrentRoundContext>,
    ) {
        self.0.store(Some(Arc::new(ResponderInner {
            broadcast_filter: broadcast_filter.clone(),
            effects: round_effects.clone(),
            top_dag_round: top_dag_round.clone(),
        })));
    }
}

impl Service<ServiceRequest> for Responder {
    type QueryResponse = Response;
    type OnQueryFuture = futures_util::future::Ready<Option<Self::QueryResponse>>;
    type OnMessageFuture = futures_util::future::Ready<()>;
    type OnDatagramFuture = futures_util::future::Ready<()>;

    #[inline]
    fn on_query(&self, req: ServiceRequest) -> Self::OnQueryFuture {
        futures_util::future::ready(self.handle_query(&req))
    }

    #[inline]
    fn on_message(&self, req: ServiceRequest) -> Self::OnMessageFuture {
        self.handle_broadcast(&req);
        futures_util::future::ready(())
    }

    #[inline]
    fn on_datagram(&self, _req: ServiceRequest) -> Self::OnDatagramFuture {
        futures_util::future::ready(())
    }
}

impl Responder {
    fn handle_query(&self, req: &ServiceRequest) -> Option<Response> {
        let peer_id = req.metadata.peer_id;
        let body = MPQuery::try_from(req)
            .inspect_err(|e| {
                tracing::error!(
                    peer_id = display(peer_id.alt()),
                    error = debug(e),
                    "unexpected request",
                );
            })
            .ok()?; // malformed request is a reason to ignore it
        let inner = self.0.load_full();
        let response = match body {
            MPQuery::PointById(point_id) => MPResponse::PointById(match inner {
                None => PointByIdResponse(None),
                Some(inner) => {
                    Uploader::find(&peer_id, &point_id, &inner.top_dag_round, &inner.effects)
                }
            }),
            MPQuery::Signature(round) => MPResponse::Signature(match inner {
                None => SignatureResponse::TryLater,
                Some(inner) => Signer::signature_response(
                    round,
                    &peer_id,
                    &inner.top_dag_round,
                    &inner.effects,
                ),
            }),
        };
        Some(Response::try_from(&response).expect("should serialize own response"))
    }

    fn handle_broadcast(&self, req: &ServiceRequest) {
        let Some(inner) = self.0.load_full() else {
            return;
        };
        match bincode::deserialize::<Point>(&req.body) {
            Ok(point) => {
                let point = Arc::new(point);
                inner.broadcast_filter.add(
                    &req.metadata.peer_id,
                    &point,
                    inner.top_dag_round.round(),
                    &inner.effects,
                );
            }
            Err(e) => {
                // malformed request is a reason to ignore it
                tracing::error!(
                    parent: inner.effects.span(),
                    "cannot parse broadcast from {:?}: {e:?}",
                    req.metadata.peer_id
                );
            }
        };
    }
}
// ResponderContext is meaningless without metrics
