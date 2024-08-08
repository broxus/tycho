use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwapOption;
use futures_util::future;
use tycho_network::{Response, Service, ServiceRequest};

use crate::dag::DagRound;
use crate::effects::{AltFormat, Effects, EngineContext, MempoolStore};
use crate::intercom::broadcast::Signer;
use crate::intercom::core::dto::{MPQuery, MPResponse};
use crate::intercom::dto::{PointByIdResponse, SignatureResponse};
use crate::intercom::{BroadcastFilter, Downloader, Uploader};

#[derive(Clone, Default)]
pub struct Responder(Arc<ArcSwapOption<ResponderInner>>);

struct ResponderInner {
    // state and storage components go here
    broadcast_filter: BroadcastFilter,
    top_dag_round: DagRound,
    downloader: Downloader,
    store: MempoolStore,
    effects: Effects<EngineContext>,
}

impl Responder {
    pub fn update(
        &self,
        broadcast_filter: &BroadcastFilter,
        top_dag_round: &DagRound,
        downloader: &Downloader,
        store: &MempoolStore,
        round_effects: &Effects<EngineContext>,
    ) {
        broadcast_filter.advance_round(top_dag_round, downloader, store, round_effects);
        self.0.store(Some(Arc::new(ResponderInner {
            broadcast_filter: broadcast_filter.clone(),
            top_dag_round: top_dag_round.clone(),
            downloader: downloader.clone(),
            store: store.clone(),
            effects: round_effects.clone(),
        })));
    }
}

impl Service<ServiceRequest> for Responder {
    type QueryResponse = Response;
    type OnQueryFuture = future::Ready<Option<Self::QueryResponse>>;
    type OnMessageFuture = future::Ready<()>;
    type OnDatagramFuture = future::Ready<()>;

    #[inline]
    fn on_query(&self, req: ServiceRequest) -> Self::OnQueryFuture {
        future::ready(self.handle_query(&req))
    }

    #[inline]
    fn on_message(&self, _req: ServiceRequest) -> Self::OnMessageFuture {
        future::ready(())
    }

    #[inline]
    fn on_datagram(&self, _req: ServiceRequest) -> Self::OnDatagramFuture {
        future::ready(())
    }
}

impl Responder {
    fn handle_query(&self, req: &ServiceRequest) -> Option<Response> {
        let task_start = Instant::now();
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
        let mp_response = match body {
            MPQuery::Broadcast(point) => {
                match inner {
                    None => {} // do nothing: sender has retry loop via signature request
                    Some(inner) => inner.broadcast_filter.add(
                        &req.metadata.peer_id,
                        &Arc::new(point),
                        &inner.top_dag_round,
                        &inner.downloader,
                        &inner.store,
                        &inner.effects,
                    ),
                };
                MPResponse::Broadcast
            }
            MPQuery::PointById(point_id) => MPResponse::PointById(match inner {
                None => PointByIdResponse::TryLater,
                Some(inner) => Uploader::find(
                    &peer_id,
                    &point_id,
                    &inner.top_dag_round,
                    &inner.store,
                    &inner.effects,
                ),
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
        let response = Response::try_from(&mp_response).expect("should serialize own response");

        EngineContext::response_metrics(&mp_response, task_start.elapsed());

        Some(response)
    }
}

impl EngineContext {
    fn response_metrics(mp_response: &MPResponse, elapsed: Duration) {
        let metric_name = match mp_response {
            MPResponse::Broadcast => "tycho_mempool_broadcast_query_responder_time",
            MPResponse::Signature(SignatureResponse::NoPoint | SignatureResponse::TryLater) => {
                "tycho_mempool_signature_query_responder_pong_time"
            }
            MPResponse::Signature(
                SignatureResponse::Signature(_) | SignatureResponse::Rejected(_),
            ) => "tycho_mempool_signature_query_responder_data_time",
            MPResponse::PointById(PointByIdResponse::Defined(Some(_))) => {
                "tycho_mempool_download_query_responder_some_time"
            }
            MPResponse::PointById(
                PointByIdResponse::Defined(None) | PointByIdResponse::TryLater,
            ) => "tycho_mempool_download_query_responder_none_time",
        };
        metrics::histogram!(metric_name).record(elapsed);
    }
}
