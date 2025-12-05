use std::sync::Arc;
use std::time::Instant;

use arc_swap::ArcSwapOption;
use futures_util::future::BoxFuture;
use futures_util::{FutureExt, future};
use tycho_network::{Response, Service, ServiceRequest};
use tycho_util::sync::rayon_run_fifo;

use crate::dag::DagHead;
use crate::effects::{AltFormat, Ctx, RoundCtx};
use crate::engine::round_watch::{Consensus, RoundWatch};
use crate::intercom::broadcast::Signer;
use crate::intercom::core::query::request::{QueryRequest, QueryRequestMedium, QueryRequestRaw};
use crate::intercom::core::query::response::QueryResponse;
use crate::intercom::{BroadcastFilter, Downloader, PeerSchedule, Uploader};
use crate::models::Point;
use crate::storage::MempoolStore;

#[derive(Clone, Default)]
pub struct Responder(Arc<ArcSwapOption<ResponderInner>>);

struct ResponderInner {
    state: Arc<ResponderState>,
    head: DagHead,
    round_ctx: RoundCtx,
}

struct ResponderState {
    broadcast_filter: BroadcastFilter,
    // state and storage components go here
    store: MempoolStore,
    consensus_round: RoundWatch<Consensus>,
    peer_schedule: PeerSchedule,
    downloader: Downloader,
    #[cfg(feature = "mock-feedback")]
    top_known_anchor: RoundWatch<crate::engine::round_watch::TopKnownAnchor>,
}

impl Responder {
    #[allow(clippy::too_many_arguments)]
    pub fn init(
        &self,
        store: &MempoolStore,
        consensus_round: &RoundWatch<Consensus>,
        peer_schedule: &PeerSchedule,
        downloader: &Downloader,
        head: &DagHead,
        round_ctx: &RoundCtx,
        #[cfg(feature = "mock-feedback")] top_known_anchor: &RoundWatch<
            crate::engine::round_watch::TopKnownAnchor,
        >,
    ) {
        let state = ResponderState {
            broadcast_filter: BroadcastFilter::default(),
            store: store.clone(),
            consensus_round: consensus_round.clone(),
            peer_schedule: peer_schedule.clone(),
            downloader: downloader.clone(),
            #[cfg(feature = "mock-feedback")]
            top_known_anchor: top_known_anchor.clone(),
        };
        let old = self.0.swap(Some(Arc::new(ResponderInner {
            state: Arc::new(state),
            head: head.clone(),
            round_ctx: round_ctx.clone(),
        })));
        assert!(old.is_none(), "cannot init responder twice");
    }

    /// as `Self` is passed to Overlay as a `Service` and may be cloned there,
    /// free `DagHead` and other resources upon `Engine` termination
    pub fn dispose(&self) {
        self.0.store(None);
    }

    pub fn update(&self, head: &DagHead, round_ctx: &RoundCtx) {
        let Some(inner) = self.0.load_full() else {
            tracing::warn!(
                parent: round_ctx.span(),
                "cannot update Responder: not init or already disposed"
            );
            return;
        };
        let state = &inner.state;
        // Note: Signer must see DAG rounds completely flushed from BroadcastFilter,
        //  its OK if Signer uses outdated DagHead and doesn't see the DAG up to the latest top
        (state.broadcast_filter).flush_to_dag(head, &state.downloader, &state.store, round_ctx);
        self.0.store(Some(Arc::new(ResponderInner {
            state: state.clone(),
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
        match self.0.load_full() {
            Some(inner) => inner.handle_query(req).boxed(),
            None => futures_util::future::ready(None).boxed(),
        }
    }

    #[inline]
    fn on_message(&self, _req: ServiceRequest) -> Self::OnMessageFuture {
        #[cfg(feature = "mock-feedback")]
        {
            use crate::mock_feedback::RoundBoxed;
            if let Ok(data) = _req.parse_tl::<RoundBoxed>()
                && let Some(inner) = self.0.load_full()
            {
                inner.state.top_known_anchor.set_max(data.round);
            }
        }
        future::ready(())
    }
}

impl ResponderInner {
    async fn handle_query(self: Arc<Self>, req: ServiceRequest) -> Option<Response> {
        let mut response = QueryResponse::new();

        let ResponderInner {
            state,
            head,
            round_ctx,
        } = self.as_ref();
        let peer_id = &req.metadata.peer_id;

        let raw_query = match QueryRequestRaw::new(req.body) {
            Ok(wrapper) => wrapper,
            Err(tl_error) => {
                tracing::warn!(
                    peer_id = display(peer_id.alt()),
                    error = %tl_error,
                    "unexpected query",
                );
                return None;
            }
        };

        response.set_tag(raw_query.tag);

        let tag = raw_query.tag;

        let medium_query = match raw_query.parse() {
            Ok(medium_query) => medium_query,
            Err(tl_error) => {
                tracing::warn!(
                    ?tag,
                    peer_id = display(peer_id.alt()),
                    error = %tl_error,
                    "bad request",
                );
                return None;
            }
        };

        let query = match medium_query {
            QueryRequestMedium::Broadcast(bytes) => {
                let start = Instant::now();
                let parse_result = rayon_run_fifo(|| Point::parse(bytes.into())).await;
                metrics::histogram!("tycho_mempool_engine_parse_point_time")
                    .record(start.elapsed());
                match parse_result {
                    Ok(Ok(Ok(point))) if point.info().author() == peer_id => {
                        QueryRequest::Broadcast(point, None)
                    }
                    Ok(Ok(Ok(wrong_point))) => {
                        let wrong_id = wrong_point.info().id();
                        tracing::error!(
                            ?tag,
                            peer_id = display(peer_id.alt()),
                            author = display(wrong_id.author.alt()),
                            round = wrong_id.round.0,
                            digest = display(wrong_id.digest.alt()),
                            "broadcasted other's point",
                        );
                        return None;
                    }
                    Ok(Ok(Err((point, issue)))) => QueryRequest::Broadcast(point, Some(issue)),
                    Ok(Err(integrity_err)) => {
                        tracing::error!(
                            ?tag,
                            peer_id = display(peer_id.alt()),
                            error = %integrity_err,
                            "bad point",
                        );
                        return None;
                    }
                    Err(tl_error) => {
                        tracing::warn!(
                            ?tag,
                            peer_id = display(peer_id.alt()),
                            error = %tl_error,
                            "bad request",
                        );
                        return None;
                    }
                }
            }
            QueryRequestMedium::Signature(round) => QueryRequest::Signature(round),
            QueryRequestMedium::Download(point_id) => QueryRequest::Download(point_id),
        };

        Some(match query {
            QueryRequest::Broadcast(point, maybe_issue) => {
                let reached_threshold = state.broadcast_filter.add_check_threshold(
                    &point,
                    maybe_issue,
                    &state.store,
                    &state.peer_schedule,
                    &state.downloader,
                    head,
                    round_ctx,
                );
                if reached_threshold {
                    // notify Collector after max consensus round is updated
                    state.consensus_round.set_max(point.info().round());
                    // round is determined, so clean history;
                    // do not flush to DAG as it may have no needed rounds yet
                    if state.consensus_round.get() == point.info().round() {
                        (state.broadcast_filter).clean(point.info().round(), head, round_ctx);
                    } // else: engine is not paused, let it do its work
                }
                response.broadcast()
            }
            QueryRequest::Signature(round) => response.signature(Signer::signature_response(
                peer_id,
                round,
                &state.broadcast_filter,
                head,
                round_ctx,
            )),
            QueryRequest::Download(point_id) => response
                .download(Uploader::find(peer_id, point_id, &state.store, head, round_ctx).await),
        })
    }
}
