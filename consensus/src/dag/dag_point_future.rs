use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures_util::FutureExt;
use tokio::sync::{mpsc, oneshot};
use tycho_network::PeerId;
use tycho_util::futures::{JoinTask, Shared};
use tycho_util::sync::OnceTake;

use crate::dag::dag_location::InclusionState;
use crate::dag::{DagRound, Verifier};
use crate::effects::{CurrentRoundContext, Effects, ValidateContext};
use crate::intercom::Downloader;
use crate::models::{DagPoint, Digest, Location, PointId};
use crate::Point;

#[derive(Clone)]
pub struct DagPointFuture(DagPointFutureType);

impl Future for DagPointFuture {
    type Output = DagPoint;

    #[inline]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match &mut self.0 {
            DagPointFutureType::Broadcast(task) | DagPointFutureType::Download { task, .. } => {
                match task.poll_unpin(cx) {
                    Poll::Ready((dag_point, _)) => Poll::Ready(dag_point),
                    Poll::Pending => Poll::Pending,
                }
            }
            DagPointFutureType::Local(ready) => ready.poll_unpin(cx),
        }
    }
}

#[derive(Clone)]
enum DagPointFutureType {
    Broadcast(Shared<JoinTask<DagPoint>>),
    Download {
        task: Shared<JoinTask<DagPoint>>,
        dependents: mpsc::UnboundedSender<PeerId>,
        verified: Arc<OnceTake<oneshot::Sender<Point>>>,
    },
    Local(futures_util::future::Ready<DagPoint>),
}

impl DagPointFuture {
    pub fn new_local(dag_point: &DagPoint, state: &InclusionState) -> Self {
        state.init(dag_point);
        Self(DagPointFutureType::Local(futures_util::future::ready(
            dag_point.clone(),
        )))
    }

    pub fn new_broadcast(
        point_dag_round: &DagRound,
        point: &Point,
        state: &InclusionState,
        downloader: &Downloader,
        effects: &Effects<CurrentRoundContext>,
    ) -> Self {
        let downloader = downloader.clone();
        let span = effects.span().clone();
        let point_dag_round = point_dag_round.downgrade();
        let point = point.clone();
        let state = state.clone();
        DagPointFuture(DagPointFutureType::Broadcast(Shared::new(JoinTask::new(
            Verifier::validate(point, point_dag_round, downloader, span)
                .inspect(move |dag_point| state.init(dag_point)),
        ))))
    }

    pub fn new_download(
        point_dag_round: &DagRound,
        author: &PeerId,
        digest: &Digest,
        state: &InclusionState,
        downloader: &Downloader,
        effects: &Effects<ValidateContext>,
    ) -> Self {
        let downloader = downloader.clone();
        let effects = effects.clone();
        let state = state.clone();
        let point_dag_round = point_dag_round.clone();
        let (dependents_tx, dependents_rx) = mpsc::unbounded_channel();
        let (broadcast_tx, broadcast_rx) = oneshot::channel();
        let point_id = PointId {
            location: Location {
                author: *author,
                round: point_dag_round.round(),
            },
            digest: digest.clone(),
        };
        DagPointFuture(DagPointFutureType::Download {
            task: Shared::new(JoinTask::new(
                downloader
                    .run(
                        point_id,
                        point_dag_round,
                        dependents_rx,
                        broadcast_rx,
                        effects,
                    )
                    .inspect(move |dag_point| state.init(dag_point)),
            )),
            dependents: dependents_tx,
            verified: Arc::new(OnceTake::new(broadcast_tx)),
        })
    }

    pub fn add_depender(&self, dependent: &PeerId) {
        if let DagPointFutureType::Download { dependents, .. } = &self.0 {
            // receiver is dropped upon completion
            _ = dependents.send(*dependent);
        }
    }

    pub fn resolve_download(&self, broadcast: &Point) {
        if let DagPointFutureType::Download { verified, .. } = &self.0 {
            if let Some(oneshot) = verified.take() {
                // receiver is dropped upon completion
                _ = oneshot.send(broadcast.clone());
            }
        }
    }
}
