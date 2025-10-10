use std::sync::{Arc, OnceLock};
use std::time::Instant;
use arc_swap::ArcSwapOption;
use futures_util::future::BoxFuture;
use futures_util::{FutureExt, future};
use tycho_network::{Response, Service, ServiceRequest};
use crate::dag::DagHead;
use crate::effects::{AltFormat, RoundCtx};
use crate::engine::round_watch::{Consensus, RoundWatch};
use crate::intercom::broadcast::Signer;
use crate::intercom::core::{
    PointByIdResponse, QueryRequest, QueryRequestRaw, QueryRequestTag, QueryResponse,
    SignatureResponse,
};
use crate::intercom::{BroadcastFilter, Downloader, PeerSchedule, Uploader};
use crate::storage::MempoolStore;
#[derive(Clone, Default)]
pub struct Responder(Arc<ResponderInner>);
#[derive(Default)]
struct ResponderInner {
    state: OnceLock<ResponderState>,
    broadcast_filter: BroadcastFilter,
    current: ArcSwapOption<ResponderCurrent>,
}
struct ResponderCurrent {
    head: DagHead,
    round_ctx: RoundCtx,
}
struct ResponderState {
    store: MempoolStore,
    consensus_round: RoundWatch<Consensus>,
    peer_schedule: PeerSchedule,
    downloader: Downloader,
    #[cfg(feature = "mock-feedback")]
    top_known_anchor: RoundWatch<crate::engine::round_watch::TopKnownAnchor>,
}
impl Responder {
    pub fn init(
        &self,
        store: &MempoolStore,
        consensus_round: &RoundWatch<Consensus>,
        peer_schedule: &PeerSchedule,
        downloader: &Downloader,
        #[cfg(feature = "mock-feedback")]
        top_known_anchor: &RoundWatch<crate::engine::round_watch::TopKnownAnchor>,
    ) {
        let result = self
            .0
            .state
            .set(ResponderState {
                store: store.clone(),
                consensus_round: consensus_round.clone(),
                peer_schedule: peer_schedule.clone(),
                downloader: downloader.clone(),
                #[cfg(feature = "mock-feedback")]
                top_known_anchor: top_known_anchor.clone(),
            });
        result.ok().expect("cannot init responder twice");
    }
    /// as `Self` is passed to Overlay as a `Service` and may be cloned there,
    /// free `DagHead` and other resources upon `Engine` termination
    pub fn dispose(&self) {
        self.0.current.store(None);
    }
    pub fn update(&self, head: &DagHead, round_ctx: &RoundCtx) {
        let state = self.0.state.get().expect("responder must be init");
        (self.0.broadcast_filter)
            .flush_to_dag(head, &state.downloader, &state.store, round_ctx);
        self.0
            .current
            .store(
                Some(
                    Arc::new(ResponderCurrent {
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
        self.clone().handle_query(req).boxed()
    }
    #[inline]
    fn on_message(&self, _req: ServiceRequest) -> Self::OnMessageFuture {
        #[cfg(feature = "mock-feedback")]
        {
            use crate::mock_feedback::RoundBoxed;
            if let Ok(data) = _req.parse_tl::<RoundBoxed>()
                && let Some(state) = self.0.state.get()
            {
                state.top_known_anchor.set_max(data.round);
            }
        }
        future::ready(())
    }
}
impl Responder {
    async fn handle_query(self, req: ServiceRequest) -> Option<Response> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(handle_query)),
            file!(),
            111u32,
        );
        let req = req;
        let task_start = Instant::now();
        let raw_query = match QueryRequestRaw::new(req.body) {
            Ok(wrapper) => wrapper,
            Err(error) => {
                tracing::error!(
                    peer_id = display(req.metadata.peer_id.alt()), % error,
                    "unexpected query",
                );
                {
                    __guard.end_section(122u32);
                    return None;
                };
            }
        };
        let raw_query_tag = raw_query.tag;
        let query = match {
            __guard.end_section(127u32);
            let __result = raw_query.parse().await;
            __guard.start_section(127u32);
            __result
        } {
            Ok(query) => query,
            Err(error) => {
                tracing::error!(
                    tag = ? raw_query_tag, peer_id = display(req.metadata.peer_id.alt()),
                    % error, "bad query",
                );
                {
                    __guard.end_section(136u32);
                    return None;
                };
            }
        };
        let Some(current) = self.0.current.load_full() else {
            {
                __guard.end_section(141u32);
                return Some(
                    match raw_query_tag {
                        QueryRequestTag::Broadcast => {
                            QueryResponse::broadcast(task_start)
                        }
                        QueryRequestTag::PointById => {
                            QueryResponse::point_by_id::<
                                &[u8],
                            >(task_start, PointByIdResponse::TryLater)
                        }
                        QueryRequestTag::Signature => {
                            QueryResponse::signature(
                                task_start,
                                SignatureResponse::TryLater,
                            )
                        }
                    },
                );
            };
        };
        let state = self.0.state.get().expect("responder must be init");
        Some(
            match query {
                QueryRequest::Broadcast(point) => {
                    let reached_threshold = self
                        .0
                        .broadcast_filter
                        .add_check_threshold(
                            &req.metadata.peer_id,
                            &point,
                            &state.store,
                            &state.peer_schedule,
                            &state.downloader,
                            &current.head,
                            &current.round_ctx,
                        );
                    if reached_threshold {
                        state.consensus_round.set_max(point.info().round());
                        if state.consensus_round.get() == point.info().round() {
                            self.0
                                .broadcast_filter
                                .clean(
                                    point.info().round(),
                                    &current.head,
                                    &current.round_ctx,
                                );
                        }
                    }
                    QueryResponse::broadcast(task_start)
                }
                QueryRequest::PointById(point_id) => {
                    QueryResponse::point_by_id(
                        task_start,
                        {
                            __guard.end_section(192u32);
                            let __result = Uploader::find(
                                    &req.metadata.peer_id,
                                    point_id,
                                    &state.store,
                                    &current.head,
                                    &current.round_ctx,
                                )
                                .await;
                            __guard.start_section(192u32);
                            __result
                        },
                    )
                }
                QueryRequest::Signature(round) => {
                    QueryResponse::signature(
                        task_start,
                        Signer::signature_response(
                            &req.metadata.peer_id,
                            round,
                            &self.0.broadcast_filter,
                            &current.head,
                            &current.round_ctx,
                        ),
                    )
                }
            },
        )
    }
}
