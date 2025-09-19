use std::sync::Arc;
use std::time::Instant;

use arc_swap::ArcSwapOption;
use futures_util::future::BoxFuture;
use futures_util::{FutureExt, future};
use tycho_network::{Response, Service, ServiceRequest};

use crate::dag::DagHead;
use crate::effects::{AltFormat, RoundCtx};
use crate::intercom::broadcast::Signer;
use crate::intercom::core::{
    PointByIdResponse, QueryRequest, QueryRequestRaw, QueryRequestTag, QueryResponse,
    SignatureResponse,
};
use crate::intercom::{BroadcastFilter, Downloader, Uploader};
use crate::storage::MempoolStore;

#[derive(Clone, Default)]
pub struct Responder(Arc<ResponderInner>);

#[derive(Default)]
struct ResponderInner {
    current: ArcSwapOption<ResponderCurrent>,
    #[cfg(feature = "mock-feedback")]
    top_known_anchor: std::sync::OnceLock<
        crate::engine::round_watch::RoundWatch<crate::engine::round_watch::TopKnownAnchor>,
    >,
}

struct ResponderCurrent {
    // state and storage components go here
    store: MempoolStore,
    broadcast_filter: BroadcastFilter,
    downloader: Downloader,
    head: DagHead,
    round_ctx: RoundCtx,
}

impl Responder {
    #[cfg(feature = "mock-feedback")]
    pub fn set_top_known_anchor(
        &self,
        top_known_anchor: &crate::engine::round_watch::RoundWatch<
            crate::engine::round_watch::TopKnownAnchor,
        >,
    ) {
        if (self.0.top_known_anchor.set(top_known_anchor.clone())).is_err() {
            panic!("top known anchor in responder is already initialized")
        };
    }

    /// as `Self` is passed to Overlay as a `Service` and may be cloned there,
    /// free `DagHead` and other resources upon `Engine` termination
    pub fn dispose(&self) {
        self.0.current.store(None);
    }

    pub fn update(
        &self,
        store: &MempoolStore,
        broadcast_filter: &BroadcastFilter,
        downloader: &Downloader,
        head: &DagHead,
        round_ctx: &RoundCtx,
    ) {
        // Note: first flush `BroadcastFilter`, then advance `Head` for `Signer`.
        //  That way, from Signer's point of view, BF will always have at least next round flushed.
        //  So Signer will access fully ready state of DAG up to `Head.next()`.
        //  Signer does not care for inconsistencies in BF later than `Head.next()` because
        //  no matter if it responds with TryLater or NoPoint, there is time and it won't sign now.
        //  It's important to respond with NoPoint by reason up to `Head.next()` inclusively.
        broadcast_filter.flush_to_dag(head, downloader, store, round_ctx);
        self.0.current.store(Some(Arc::new(ResponderCurrent {
            store: store.clone(),
            broadcast_filter: broadcast_filter.clone(),
            downloader: downloader.clone(),
            head: head.clone(),
            round_ctx: round_ctx.clone(),
        })));
    }
}

impl Service<ServiceRequest> for Responder {
    type QueryResponse = Response;
    type OnQueryFuture = BoxFuture<'static, Option<Self::QueryResponse>>;
    type OnMessageFuture = future::Ready<()>;

    #[inline]
    fn on_query(&self, req: ServiceRequest) -> Self::OnQueryFuture {
        self.clone().handle_query(req).boxed()
    }

    #[inline]
    fn on_message(&self, _req: ServiceRequest) -> Self::OnMessageFuture {
        #[cfg(feature = "mock-feedback")]
        {
            use crate::mock_feedback::RoundBoxed;
            if let Ok(data) = _req.parse_tl::<RoundBoxed>()
                && let Some(tka) = self.0.top_known_anchor.get()
            {
                tka.set_max(data.round);
            }
        }
        future::ready(())
    }
}

impl Responder {
    async fn handle_query(self, req: ServiceRequest) -> Option<Response> {
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

        let raw_query_tag = raw_query.tag;
        let query = match raw_query.parse().await {
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

        let Some(inner) = self.0.current.load_full() else {
            return Some(match raw_query_tag {
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
                    point_id,
                    &inner.head,
                    &inner.store,
                    &inner.round_ctx,
                )
                .await,
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
