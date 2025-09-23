use std::sync::{Arc, OnceLock};
use std::time::Instant;

use arc_swap::ArcSwapOption;
use futures_util::future::BoxFuture;
use futures_util::{FutureExt, future};
use tycho_network::{Response, Service, ServiceRequest};

use crate::dag::DagHead;
use crate::effects::{AltFormat, MempoolRayon, RoundCtx};
use crate::engine::round_watch::{Consensus, RoundWatch};
use crate::intercom::broadcast::Signer;
use crate::intercom::core::{
    PointByIdResponse, QueryRequest, QueryRequestRaw, QueryRequestTag, QueryResponse,
    SignatureResponse,
};
use crate::intercom::{BroadcastFilter, Downloader, PeerSchedule, Uploader};
use crate::storage::MempoolStore;

#[derive(Clone)]
pub struct Responder(Arc<ResponderInner>);

impl Responder {
    pub fn new(mempool_rayon: &MempoolRayon) -> Self {
        Self(Arc::new(ResponderInner {
            state: OnceLock::default(),
            current: ArcSwapOption::empty(),
            mempool_rayon: mempool_rayon.clone(),
            broadcast_filter: Default::default(),
        }))
    }
}

struct ResponderInner {
    state: OnceLock<ResponderState>,
    mempool_rayon: MempoolRayon,
    broadcast_filter: BroadcastFilter,
    current: ArcSwapOption<ResponderCurrent>,
}

struct ResponderCurrent {
    head: DagHead,
    round_ctx: RoundCtx,
}

struct ResponderState {
    // state and storage components go here
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
        #[cfg(feature = "mock-feedback")] top_known_anchor: &RoundWatch<
            crate::engine::round_watch::TopKnownAnchor,
        >,
    ) {
        let result = self.0.state.set(ResponderState {
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
        // Note: Signer must see DAG rounds completely flushed from BroadcastFilter,
        //  its OK if Signer uses outdated DagHead and doesn't see the DAG up to the latest top
        (self.0.broadcast_filter).flush_to_dag(head, &state.downloader, &state.store, round_ctx);
        self.0.current.store(Some(Arc::new(ResponderCurrent {
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
        let query = match raw_query.parse(&self.0.mempool_rayon).await {
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

        let Some(current) = self.0.current.load_full() else {
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

        let state = self.0.state.get().expect("responder must be init");

        Some(match query {
            QueryRequest::Broadcast(point) => {
                let reached_threshold = self.0.broadcast_filter.add_check_threshold(
                    &req.metadata.peer_id,
                    &point,
                    &state.store,
                    &state.peer_schedule,
                    &state.downloader,
                    &current.head,
                    &current.round_ctx,
                );
                if reached_threshold {
                    // notify Collector after max consensus round is updated
                    state.consensus_round.set_max(point.info().round());
                    // round is determined, so clean history;
                    // do not flush to DAG as it may have no needed rounds yet
                    if state.consensus_round.get() == point.info().round() {
                        self.0.broadcast_filter.clean(
                            point.info().round(),
                            &current.head,
                            &current.round_ctx,
                        );
                    } // else: engine is not paused, let it do its work
                }
                QueryResponse::broadcast(task_start)
            }
            QueryRequest::PointById(point_id) => QueryResponse::point_by_id(
                task_start,
                Uploader::find(
                    &req.metadata.peer_id,
                    point_id,
                    &state.store,
                    &current.head,
                    &current.round_ctx,
                )
                .await,
            ),
            QueryRequest::Signature(round) => QueryResponse::signature(
                task_start,
                Signer::signature_response(
                    &req.metadata.peer_id,
                    round,
                    &self.0.broadcast_filter,
                    &current.head,
                    &current.round_ctx,
                ),
            ),
        })
    }
}
