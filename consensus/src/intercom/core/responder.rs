use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwapOption;
use futures_util::future;
use tycho_network::{Response, Service, ServiceRequest};

use crate::dag::DagHead;
use crate::effects::{AltFormat, MempoolStore, RoundCtx};
use crate::intercom::broadcast::Signer;
use crate::intercom::core::dto::{QueryTag, ReceiveWrapper};
use crate::intercom::dto::{BroadcastResponse, PointByIdResponse, SignatureResponse};
use crate::intercom::{BroadcastFilter, Downloader, Uploader};
use crate::models::{Point, PointId, Round};

#[derive(Clone, Default)]
pub struct Responder(Arc<ArcSwapOption<ResponderInner>>);

struct ResponderInner {
    // state and storage components go here
    broadcast_filter: BroadcastFilter,
    head: DagHead,
    downloader: Downloader,
    store: MempoolStore,
    round_ctx: RoundCtx,
}

impl Responder {
    pub fn update(
        &self,
        broadcast_filter: &BroadcastFilter,
        head: &DagHead,
        downloader: &Downloader,
        store: &MempoolStore,
        round_ctx: &RoundCtx,
    ) {
        broadcast_filter.flush_to_dag(head, downloader, store, round_ctx);
        // Note that `next_dag_round` for Signer should be advanced _after_ new points
        //  are moved from BroadcastFilter into DAG. Then Signer will look for points
        //  (of rounds greater than local engine round, including top dag round exactly)
        //  first in BroadcastFilter, then in DAG.
        //  Otherwise local Signer will skip BroadcastFilter check and search in DAG directly,
        //  will not find cached point and will ask to repeat broadcast once more, and local
        //  BroadcastFilter may account such repeated broadcast as malicious.
        self.0.store(Some(Arc::new(ResponderInner {
            broadcast_filter: broadcast_filter.clone(),
            head: head.clone(),
            downloader: downloader.clone(),
            store: store.clone(),
            round_ctx: round_ctx.clone(),
        })));
    }
}

impl Service<ServiceRequest> for Responder {
    type QueryResponse = Response;
    type OnQueryFuture = future::Ready<Option<Self::QueryResponse>>;
    type OnMessageFuture = future::Ready<()>;

    #[inline]
    fn on_query(&self, req: ServiceRequest) -> Self::OnQueryFuture {
        let inner = self.0.load_full();
        future::ready(Self::handle_query(inner, &req))
    }

    #[inline]
    fn on_message(&self, _req: ServiceRequest) -> Self::OnMessageFuture {
        future::ready(())
    }
}

impl Responder {
    fn handle_query(inner: Option<Arc<ResponderInner>>, req: &ServiceRequest) -> Option<Response> {
        enum Query {
            Broadcast(Point),
            PointById(PointId),
            Signature(Round),
        }

        let task_start = Instant::now();

        let wrapper = match tl_proto::deserialize::<ReceiveWrapper<'_>>(&req.body) {
            Ok(wrapper) => wrapper,
            Err(error) => {
                tracing::error!(
                    peer_id = display(req.metadata.peer_id.alt()),
                    %error,
                    "unexpected query",
                );
                return None;
            }
        };

        let query = match match wrapper.tag {
            QueryTag::Broadcast => Point::verify_hash_inner(wrapper.body.as_ref()).and_then(|_| {
                tl_proto::deserialize::<Point>(wrapper.body.as_ref()).map(Query::Broadcast)
            }),
            QueryTag::PointById => {
                tl_proto::deserialize::<PointId>(wrapper.body.as_ref()).map(Query::PointById)
            }
            QueryTag::Signature => {
                tl_proto::deserialize::<Round>(wrapper.body.as_ref()).map(Query::Signature)
            }
        } {
            Ok(query) => query,
            Err(error) => {
                tracing::error!(
                    tag = ?wrapper.tag,
                    peer_id = display(req.metadata.peer_id.alt()),
                    %error,
                    "bad query",
                );
                return None;
            }
        };

        let response = match query {
            Query::Broadcast(point) => {
                match inner {
                    None => {} // do nothing: sender has retry loop via signature request
                    Some(inner) => inner.broadcast_filter.add(
                        &req.metadata.peer_id,
                        &point,
                        &inner.head,
                        &inner.downloader,
                        &inner.store,
                        &inner.round_ctx,
                    ),
                };
                let response = Response::from_tl(&BroadcastResponse);
                RoundCtx::broadcast_response_metrics(task_start.elapsed());
                response
            }
            Query::PointById(point_id) => {
                let response_body = match &inner {
                    None => PointByIdResponse::TryLater,
                    Some(inner) => Uploader::find(
                        &req.metadata.peer_id,
                        &point_id,
                        &inner.head,
                        &inner.store,
                        &inner.round_ctx,
                    ),
                };
                let response = Response::from_tl(&response_body);
                RoundCtx::point_by_id_response_metrics(&response_body, task_start.elapsed());
                response
            }
            Query::Signature(round) => {
                let response_body = match inner {
                    None => SignatureResponse::TryLater,
                    Some(inner) => Signer::signature_response(
                        &req.metadata.peer_id,
                        round,
                        &inner.head,
                        &inner.broadcast_filter,
                        &inner.round_ctx,
                    ),
                };
                let response = Response::from_tl(&response_body);
                RoundCtx::signature_response_metrics(&response_body, task_start.elapsed());
                response
            }
        };

        Some(response)
    }
}

impl RoundCtx {
    fn broadcast_response_metrics(elapsed: Duration) {
        let histogram = metrics::histogram!("tycho_mempool_broadcast_query_responder_time");
        histogram.record(elapsed);
    }

    fn signature_response_metrics(response: &SignatureResponse, elapsed: Duration) {
        let histogram = match response {
            SignatureResponse::NoPoint | SignatureResponse::TryLater => {
                metrics::histogram!("tycho_mempool_signature_query_responder_pong_time")
            }
            SignatureResponse::Signature(_) | SignatureResponse::Rejected(_) => {
                metrics::histogram!("tycho_mempool_signature_query_responder_data_time")
            }
        };
        histogram.record(elapsed);
    }

    fn point_by_id_response_metrics<T>(response: &PointByIdResponse<T>, elapsed: Duration) {
        let histogram = match response {
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
