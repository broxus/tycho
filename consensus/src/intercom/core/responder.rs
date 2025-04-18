use std::sync::Arc;
use std::time::Instant;

use arc_swap::ArcSwapOption;
use futures_util::future;
use tycho_network::{Response, Service, ServiceRequest};

use crate::dag::DagHead;
use crate::effects::{AltFormat, MempoolStore, RoundCtx};
use crate::intercom::broadcast::Signer;
use crate::intercom::core::{
    PointByIdResponse, QueryRequest, QueryRequestRaw, QueryRequestTag, QueryResponse,
    SignatureResponse,
};
use crate::intercom::{BroadcastFilter, Downloader, Uploader};

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
        future::ready(Self::handle_query(inner, req))
    }

    #[inline]
    fn on_message(&self, _req: ServiceRequest) -> Self::OnMessageFuture {
        future::ready(())
    }
}

impl Responder {
    fn handle_query(inner: Option<Arc<ResponderInner>>, req: ServiceRequest) -> Option<Response> {
        let task_start = Instant::now();

        let raw_query = match QueryRequestRaw::new(req.body) {
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

        let Some(inner) = inner else {
            return Some(match raw_query.tag {
                QueryRequestTag::Broadcast => {
                    // do nothing: sender has retry loop via signature request
                    QueryResponse::broadcast(task_start)
                }
                QueryRequestTag::PointById => {
                    QueryResponse::point_by_id::<&[u8]>(task_start, PointByIdResponse::TryLater)
                }
                QueryRequestTag::Signature => {
                    QueryResponse::signature(task_start, SignatureResponse::TryLater)
                }
            });
        };

        let raw_query_tag = raw_query.tag;
        let query = match raw_query.parse() {
            Ok(query) => query,
            Err(error) => {
                tracing::error!(
                    tag = ?raw_query_tag,
                    peer_id = display(req.metadata.peer_id.alt()),
                    %error,
                    "bad query",
                );
                return None;
            }
        };

        Some(match query {
            QueryRequest::Broadcast(point) => {
                inner.broadcast_filter.add(
                    &req.metadata.peer_id,
                    &point,
                    &inner.head,
                    &inner.downloader,
                    &inner.store,
                    &inner.round_ctx,
                );
                QueryResponse::broadcast(task_start)
            }
            QueryRequest::PointById(point_id) => QueryResponse::point_by_id(
                task_start,
                Uploader::find(
                    &req.metadata.peer_id,
                    &point_id,
                    &inner.head,
                    &inner.store,
                    &inner.round_ctx,
                ),
            ),
            QueryRequest::Signature(round) => QueryResponse::signature(
                task_start,
                Signer::signature_response(
                    &req.metadata.peer_id,
                    round,
                    &inner.head,
                    &inner.broadcast_filter,
                    &inner.round_ctx,
                ),
            ),
        })
    }
}
