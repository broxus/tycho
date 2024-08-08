use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures_util::FutureExt;
use tokio::sync::{mpsc, oneshot};
use tycho_network::PeerId;
use tycho_storage::PointFlags;
use tycho_util::futures::{JoinTask, Shared};
use tycho_util::sync::OnceTake;

use crate::dag::dag_location::InclusionState;
use crate::dag::{DagRound, Verifier};
use crate::effects::{DownloadContext, Effects, EngineContext, MempoolStore, ValidateContext};
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
        certified: Arc<OnceTake<oneshot::Sender<()>>>,
    },
    Download {
        task: Shared<JoinTask<DagPoint>>,
        // this could be a `Notify`, but both sender and receiver must be used only once
        certified: Arc<OnceTake<oneshot::Sender<()>>>,
        dependents: mpsc::UnboundedSender<PeerId>,
        verified: Arc<OnceTake<oneshot::Sender<Point>>>,
    },
    Local(futures_util::future::Ready<DagPoint>),
}

impl DagPointFuture {
    pub fn new_local(dag_point: &DagPoint, state: &InclusionState, store: &MempoolStore) -> Self {
        state.init(dag_point);
        // do not store invalid: after restart may not load smth or produce equivocation
        if let Some(valid) = dag_point.valid() {
            store.insert_point(&valid.point);
            store.set_flags(
                valid.point.body().location.round,
                valid.point.digest(),
                &PointFlags {
                    is_valid: true,
                    ..Default::default()
                },
            );
        }
        Self(DagPointFutureType::Local(futures_util::future::ready(
            dag_point.clone(),
        )))
    }

    pub fn new_broadcast(
        point_dag_round: &DagRound,
        point: &Point,
        state: &InclusionState,
        downloader: &Downloader,
        store: &MempoolStore,
        effects: &Effects<EngineContext>,
    ) -> Self {
        let downloader = downloader.clone();
        let effects = Effects::<ValidateContext>::new(effects, point);
        let point_dag_round = point_dag_round.downgrade();
        let point = point.clone();
        let state = state.clone();
        let store = store.clone();
        let (certified_tx, certified_rx) = oneshot::channel();
        let certified = Arc::new(OnceTake::new(certified_tx));
        let certified_clone = certified.clone();
        let task = async move {
            let point_id = point.id();
            let stored_fut = tokio::task::spawn_blocking({
                let verified = point.clone();
                let store = store.clone();
                move || store.insert_point(&verified)
            });
            let validated_fut = Verifier::validate(
                point,
                point_dag_round,
                downloader,
                store.clone(),
                certified_rx,
                effects,
            );
            // do not abort store if not valid
            let dag_point = match tokio::join!(stored_fut, validated_fut) {
                (Ok(_), validated) => validated,
                (Err(err), _) if err.is_panic() => std::panic::resume_unwind(err.into_panic()),
                (Err(_), _) => panic!("store point was cancelled"),
            };
            let flags = PointFlags {
                is_validated: true,
                is_valid: dag_point.valid().is_some(),
                is_certified: certified_clone.is_taken(),
                ..Default::default()
            };
            tokio::task::spawn_blocking(move || {
                store.set_flags(point_id.location.round, &point_id.digest, &flags);
            })
            .await
            .expect("db set point flags");
            state.init(&dag_point);
            dag_point
        };
        DagPointFuture(DagPointFutureType::Broadcast {
            task: Shared::new(JoinTask::new(task)),
            certified,
        })
    }

    pub fn new_download(
        point_dag_round: &DagRound,
        author: &PeerId,
        digest: &Digest,
        state: &InclusionState,
        downloader: &Downloader,
        store: &MempoolStore,
        effects: &Effects<ValidateContext>,
    ) -> Self {
        let downloader = downloader.clone();
        let state = state.clone();
        let (dependents_tx, dependents_rx) = mpsc::unbounded_channel();
        let (broadcast_tx, broadcast_rx) = oneshot::channel();
        let (certified_tx, certified_rx) = oneshot::channel();
        let certified = Arc::new(OnceTake::new(certified_tx));
        let certified_clone = certified.clone();
        let point_id = PointId {
            location: Location {
                author: *author,
                round: point_dag_round.round(),
            },
            digest: digest.clone(),
        };
        let point_dag_round = point_dag_round.downgrade();
        let store = store.clone();
        let effects = effects.clone();
        let task = async move {
            let downloaded = downloader
                .run(
                    &point_id,
                    dependents_rx,
                    broadcast_rx,
                    Effects::<DownloadContext>::new(&effects, &point_id),
                )
                .await;
            let dag_point = match downloaded {
                None => DagPoint::NotExists(Arc::new(point_id)),
                Some(verified) => {
                    let deeper_effects = Effects::<ValidateContext>::new(&effects, &verified);
                    tracing::trace!(
                        parent: deeper_effects.span(),
                        "downloaded, start validating",
                    );
                    let stored_fut = tokio::task::spawn_blocking({
                        let verified = verified.clone();
                        let store = store.clone();
                        move || store.insert_point(&verified)
                    });
                    let validated_fut = Verifier::validate(
                        verified,
                        point_dag_round,
                        downloader,
                        store.clone(),
                        certified_rx,
                        deeper_effects,
                    );
                    // do not abort store if not valid
                    let dag_point = match tokio::join!(stored_fut, validated_fut) {
                        (Ok(_), validated) => validated,
                        (Err(err), _) if err.is_panic() => {
                            std::panic::resume_unwind(err.into_panic())
                        }
                        (Err(_), _) => panic!("store point was cancelled"),
                    };
                    let flags = PointFlags {
                        is_validated: true,
                        is_valid: dag_point.valid().is_some(),
                        is_certified: certified_clone.is_taken(),
                        ..Default::default()
                    };
                    tokio::task::spawn_blocking(move || {
                        store.set_flags(point_id.location.round, &point_id.digest, &flags);
                    })
                    .await
                    .expect("db set point flags");
                    dag_point
                }
            };
            state.init(&dag_point);
            dag_point
        };
        DagPointFuture(DagPointFutureType::Download {
            task: Shared::new(JoinTask::new(task)),
            certified,
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

    pub fn make_certified(&self) {
        // every vertex is certified by definition,
        // but also every vertex dependency is certified transitively
        if let DagPointFutureType::Broadcast { certified, .. }
        | DagPointFutureType::Download { certified, .. } = &self.0
        {
            if let Some(oneshot) = certified.take() {
                // receiver is dropped upon completion
                _ = oneshot.send(());
            }
        }
    }
}
