use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwapOption;
use futures_util::future;
use tycho_network::{try_handle_prefix, Response, Service, ServiceRequest};

use crate::dag::DagHead;
use crate::effects::{AltFormat, Effects, EngineContext, MempoolStore};
use crate::intercom::broadcast::Signer;
use crate::intercom::core::dto::{
    BroadcastMpResponse, BroadcastQuery, PointMpResponse, PointQuery, SignatureMpResponse,
    SignatureQuery,
};
use crate::intercom::dto::{PointByIdResponse, SignatureResponse};
use crate::intercom::{BroadcastFilter, Downloader, Uploader};

#[derive(Clone, Default)]
pub struct Responder(Arc<ArcSwapOption<ResponderInner>>);

struct ResponderInner {
    // state and storage components go here
    broadcast_filter: BroadcastFilter,
    head: DagHead,
    downloader: Downloader,
    store: MempoolStore,
    effects: Effects<EngineContext>,
}

impl Responder {
    pub fn update(
        &self,
        broadcast_filter: &BroadcastFilter,
        head: &DagHead,
        downloader: &Downloader,
        store: &MempoolStore,
        round_effects: &Effects<EngineContext>,
    ) {
        broadcast_filter.advance_round(head, downloader, store, round_effects);
        // Note that `next_dag_round` for Signer should be advanced _after_ new points
        //  are moved from BroadcastFilter into DAG. Then Signer will look for points
        //  (of rounds greater than local engine round, including top dag round exactly)
        //  first in BroadcastFilter, then in DAG.
        //  Otherwise local Signer will skip BroadcastFilter check and search in DAG directly,
        //  will not find cached point and will ask to repeat broadcast once more, and local
        //  BroadcastFilter may account such repeated broadcast as malicious.
        //  Though some points of top_dag_round (exactly) may be cached in BroadcastFilter while
        //  others are being moved into Dag, all of them will be in DAG after one more round.
        self.0.store(Some(Arc::new(ResponderInner {
            broadcast_filter: broadcast_filter.clone(),
            head: head.clone(),
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
        let inner = self.0.load_full();
        future::ready(Self::handle_query(inner, &req))
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
    fn handle_query(inner: Option<Arc<ResponderInner>>, req: &ServiceRequest) -> Option<Response> {
        let task_start = Instant::now();

        let (constructor, body) = try_handle_prefix(&req)
            .inspect_err(|e| {
                tracing::error!(
                    peer_id = display(req.metadata.peer_id.alt()),
                    error = debug(e),
                    "unexpected prefix",
                );
            })
            .ok()?;

        let response = tycho_network::match_tl_request!(body, tag = constructor, {
            BroadcastQuery as r => {
                match inner {
                    None => {} // do nothing: sender has retry loop via signature request
                    Some(inner) => inner.broadcast_filter.add(
                        &req.metadata.peer_id,
                        &r.0,
                        &inner.head,
                        &inner.downloader,
                        &inner.store,
                        &inner.effects,
                    ),
                };
                let response = Response::from_tl(&BroadcastMpResponse);
                EngineContext::broadcast_response_metrics(task_start.elapsed());
                response
            },
            PointQuery as r => {
                let response = match &inner {
                    None => PointByIdResponse::TryLater,
                    Some(inner) => Uploader::find(
                        &req.metadata.peer_id,
                        &r.0,
                        &inner.head,
                        &inner.store,
                        &inner.effects,
                    ),
                };
                let response_body = PointMpResponse(response);
                let response = Response::from_tl(&response_body);
                EngineContext::point_by_id_response_metrics(response_body, task_start.elapsed());
                response
            },
            SignatureQuery as r => {
                let response = match inner {
                    None => SignatureResponse::TryLater,
                    Some(inner) => Signer::signature_response(
                        &req.metadata.peer_id,
                        r.0,
                        &inner.head,
                        &inner.broadcast_filter,
                        &inner.effects,
                    ),
                };
                let response_body = SignatureMpResponse(response);
                let response = Response::from_tl(&response_body);
                EngineContext::signature_response_metrics(response_body, task_start.elapsed());
                response
            }

        }, e => {
            tracing::error!(
                "unexpected query: {e}",
            );
            //ignore corrupted message
            return None;
        });

        Some(response)
    }
}

impl EngineContext {
    fn broadcast_response_metrics(elapsed: Duration) {
        let histogram = metrics::histogram!("tycho_mempool_broadcast_query_responder_time");
        histogram.record(elapsed);
    }

    fn signature_response_metrics(response: SignatureMpResponse, elapsed: Duration) {
        let histogram = match response.0 {
            SignatureResponse::NoPoint | SignatureResponse::TryLater => {
                metrics::histogram!("tycho_mempool_signature_query_responder_pong_time")
            }
            SignatureResponse::Signature(_) | SignatureResponse::Rejected(_) => {
                metrics::histogram!("tycho_mempool_signature_query_responder_data_time")
            }
        };
        histogram.record(elapsed);
    }

    fn point_by_id_response_metrics<T>(response: PointMpResponse<T>, elapsed: Duration) {
        let histogram = match response.0 {
            PointByIdResponse::Defined(_) => {
                metrics::histogram!("tycho_mempool_download_query_responder_some_time")
            }
            PointByIdResponse::DefinedNone | PointByIdResponse::TryLater => {
                metrics::histogram!("tycho_mempool_download_query_responder_none_time")
            }
        };
        histogram.record(elapsed);
    }
}
