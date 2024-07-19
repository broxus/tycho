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
use crate::effects::{DownloadContext, Effects, EngineContext, ValidateContext};
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
            DagPointFutureType::Broadcast { task, .. }
            | DagPointFutureType::Download { task, .. } => match task.poll_unpin(cx) {
                Poll::Ready((dag_point, _)) => Poll::Ready(dag_point),
                Poll::Pending => Poll::Pending,
            },
            DagPointFutureType::Local(ready) => ready.poll_unpin(cx),
        }
    }
}

#[derive(Clone)]
enum DagPointFutureType {
    Broadcast {
        task: Shared<JoinTask<DagPoint>>,
        // normally, if we are among the last nodes to validate some broadcast point,
        // we can receive its proof from author, trust its signatures and skip vertex validation;
        // also, any still not locally validated dependencies of a vertex become trusted
        is_trusted: Arc<OnceTake<oneshot::Sender<()>>>,
    },
    Download {
        task: Shared<JoinTask<DagPoint>>,
        // this could be a `Notify`, but both sender and receiver must be used only once
        is_trusted: Arc<OnceTake<oneshot::Sender<()>>>,
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
        effects: &Effects<EngineContext>,
    ) -> Self {
        let downloader = downloader.clone();
        let effects = Effects::<ValidateContext>::new(effects, point);
        let point_dag_round = point_dag_round.downgrade();
        let point = point.clone();
        let state = state.clone();
        let (is_trusted_tx, is_trusted_rx) = oneshot::channel();
        DagPointFuture(DagPointFutureType::Broadcast {
            task: Shared::new(JoinTask::new(
                Verifier::validate(point, point_dag_round, downloader, is_trusted_rx, effects)
                    .inspect(move |dag_point| state.init(dag_point)),
            )),
            is_trusted: Arc::new(OnceTake::new(is_trusted_tx)),
        })
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
        let state = state.clone();
        let point_dag_round = point_dag_round.clone();
        let (dependents_tx, dependents_rx) = mpsc::unbounded_channel();
        let (broadcast_tx, broadcast_rx) = oneshot::channel();
        let (is_trusted_tx, is_trusted_rx) = oneshot::channel();
        let point_id = PointId {
            location: Location {
                author: *author,
                round: point_dag_round.round(),
            },
            digest: digest.clone(),
        };
        let effects = Effects::<DownloadContext>::new(effects, &point_id);
        DagPointFuture(DagPointFutureType::Download {
            task: Shared::new(JoinTask::new(
                downloader
                    .run(
                        point_id,
                        point_dag_round,
                        is_trusted_rx,
                        dependents_rx,
                        broadcast_rx,
                        effects,
                    )
                    .inspect(move |dag_point| state.init(dag_point)),
            )),
            is_trusted: Arc::new(OnceTake::new(is_trusted_tx)),
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

    pub fn trust_consensus(&self) {
        if let DagPointFutureType::Broadcast { is_trusted, .. }
        | DagPointFutureType::Download { is_trusted, .. } = &self.0
        {
            if let Some(oneshot) = is_trusted.take() {
                // receiver is dropped upon completion
                _ = oneshot.send(());
            }
        }
    }
}
