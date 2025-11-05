use std::sync::Arc;
use std::time::Instant;
use arc_swap::ArcSwapOption;
use futures_util::future::BoxFuture;
use futures_util::{FutureExt, future};
use tycho_network::{Response, Service, ServiceRequest};
use crate::dag::DagHead;
use crate::effects::{AltFormat, Ctx, RoundCtx};
use crate::engine::round_watch::{Consensus, RoundWatch};
use crate::intercom::broadcast::Signer;
use crate::intercom::core::{QueryRequest, QueryRequestRaw, QueryResponse};
use crate::intercom::{BroadcastFilter, Downloader, PeerSchedule, Uploader};
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
        #[cfg(feature = "mock-feedback")]
        top_known_anchor: &RoundWatch<crate::engine::round_watch::TopKnownAnchor>,
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
        let old = self
            .0
            .swap(
                Some(
                    Arc::new(ResponderInner {
                        state: Arc::new(state),
                        head: head.clone(),
                        round_ctx: round_ctx.clone(),
                    }),
                ),
            );
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
                parent : round_ctx.span(),
                "cannot update Responder: not init or already disposed"
            );
            return;
        };
        let state = &inner.state;
        (state.broadcast_filter)
            .flush_to_dag(head, &state.downloader, &state.store, round_ctx);
        self.0
            .store(
                Some(
                    Arc::new(ResponderInner {
                        state: state.clone(),
                        head: head.clone(),
                        round_ctx: round_ctx.clone(),
                    }),
                ),
            );
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
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(handle_query)),
            file!(),
            123u32,
        );
        let req = req;
        let task_start = Instant::now();
        let ResponderInner { state, head, round_ctx } = self.as_ref();
        let peer_id = &req.metadata.peer_id;
        let raw_query = match QueryRequestRaw::new(req.body) {
            Ok(wrapper) => wrapper,
            Err(error) => {
                tracing::error!(
                    peer_id = display(peer_id.alt()), % error, "unexpected query",
                );
                {
                    __guard.end_section(140u32);
                    return None;
                };
            }
        };
        let raw_query_tag = raw_query.tag;
        let query = match {
            __guard.end_section(145u32);
            let __result = raw_query.parse().await;
            __guard.start_section(145u32);
            __result
        } {
            Ok(query) => query,
            Err(error) => {
                tracing::error!(
                    tag = ? raw_query_tag, peer_id = display(peer_id.alt()), % error,
                    "bad query",
                );
                {
                    __guard.end_section(154u32);
                    return None;
                };
            }
        };
        Some(
            match query {
                QueryRequest::Broadcast(point) => {
                    let reached_threshold = state
                        .broadcast_filter
                        .add_check_threshold(
                            peer_id,
                            &point,
                            &state.store,
                            &state.peer_schedule,
                            &state.downloader,
                            head,
                            round_ctx,
                        );
                    if reached_threshold {
                        state.consensus_round.set_max(point.info().round());
                        if state.consensus_round.get() == point.info().round() {
                            (state.broadcast_filter)
                                .clean(point.info().round(), head, round_ctx);
                        }
                    }
                    QueryResponse::broadcast(task_start)
                }
                QueryRequest::PointById(point_id) => {
                    QueryResponse::point_by_id(
                        task_start,
                        {
                            __guard.end_section(182u32);
                            let __result = Uploader::find(
                                    peer_id,
                                    point_id,
                                    &state.store,
                                    head,
                                    round_ctx,
                                )
                                .await;
                            __guard.start_section(182u32);
                            __result
                        },
                    )
                }
                QueryRequest::Signature(round) => {
                    QueryResponse::signature(
                        task_start,
                        Signer::signature_response(
                            peer_id,
                            round,
                            &state.broadcast_filter,
                            head,
                            round_ctx,
                        ),
                    )
                }
            },
        )
    }
}
