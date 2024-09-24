use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures_util::{future, FutureExt};
use tokio::sync::{mpsc, oneshot};
use tracing::Instrument;
use tycho_network::PeerId;
use tycho_storage::PointStatus;
use tycho_util::futures::{JoinTask, Shared};
use tycho_util::sync::OnceTake;

use crate::dag::dag_location::InclusionState;
use crate::dag::{DagRound, Verifier};
use crate::effects::{DownloadContext, Effects, EngineContext, MempoolStore, ValidateContext};
use crate::intercom::{DownloadResult, Downloader};
use crate::models::{DagPoint, Digest, Point, PointId, PointInfo, ValidPoint};

#[derive(Clone)]
pub struct DagPointFuture(DagPointFutureType);

impl Future for DagPointFuture {
    type Output = DagPoint;

    #[inline]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match &mut self.0 {
            DagPointFutureType::Validate { task, .. } | DagPointFutureType::Load { task, .. } => {
                match task.poll_unpin(cx) {
                    Poll::Ready((dag_point, _)) => Poll::Ready(dag_point),
                    Poll::Pending => Poll::Pending,
                }
            }
            DagPointFutureType::Ready(ready) => ready.poll_unpin(cx),
        }
    }
}

#[derive(Clone)]
enum DagPointFutureType {
    Validate {
        task: Shared<JoinTask<DagPoint>>,
        // normally, if we are among the last nodes to validate some broadcast point,
        // we can receive its proof from author, trust its signatures and skip vertex validation;
        // also, any still not locally validated dependencies of a vertex become trusted
        certified: Arc<OnceTake<oneshot::Sender<()>>>,
    },
    Load {
        task: Shared<JoinTask<DagPoint>>,
        // this could be a `Notify`, but both sender and receiver must be used only once
        certified: Arc<OnceTake<oneshot::Sender<()>>>,
        dependents: mpsc::UnboundedSender<PeerId>,
        verified: Arc<OnceTake<oneshot::Sender<Point>>>,
    },
    Ready(future::Ready<DagPoint>),
}

impl DagPointFuture {
    /// locally created points are assumed to be valid, checked prior insertion if needed;
    /// for points of others - there are all other methods
    pub fn new_local_trusted(point: &Point, state: &InclusionState, store: &MempoolStore) -> Self {
        let status = PointStatus {
            is_ill_formed: false,
            is_validated: false,
            is_valid: true,
            is_trusted: true,
            ..Default::default()
        };
        store.insert_point(point, &status);
        let dag_point = DagPoint::Trusted(ValidPoint::new(point));
        state.init(&dag_point); // only after persisted
        Self(DagPointFutureType::Ready(future::ready(dag_point)))
    }

    pub fn new_ill_formed_broadcast(
        point: &Point,
        state: &InclusionState,
        store: &MempoolStore,
        effects: &Effects<EngineContext>,
    ) -> Self {
        let store_fut = tokio::task::spawn_blocking({
            let point = point.clone();
            let state = state.clone();
            let store = store.clone();
            move || {
                let status = PointStatus {
                    is_ill_formed: true,
                    ..Default::default()
                };
                store.insert_point(&point, &status);
                let dag_point = DagPoint::IllFormed(Arc::new(point.id()));
                state.init(&dag_point);
                dag_point
            }
        });
        let task = async move { store_fut.await.expect("db insert ill-formed broadcast") };
        Self(DagPointFutureType::Validate {
            task: Shared::new(JoinTask::new(task.instrument(effects.span().clone()))),
            certified: Arc::new(OnceTake::empty()),
        })
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
        let span = effects.span().clone();
        let point_dag_round = point_dag_round.downgrade();
        let point = point.clone();
        let state = state.clone();
        let store = store.clone();
        let (certified_tx, certified_rx) = oneshot::channel();
        let once_certified_tx = Arc::new(OnceTake::new(certified_tx));
        let once_certified_tx_clone = once_certified_tx.clone();
        let task = async move {
            let point_id = point.id();
            let stored_fut = tokio::task::spawn_blocking({
                let verified = point.clone();
                let store = store.clone();
                move || store.insert_point(&verified, &PointStatus::default())
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
                (Err(e), _) => panic!("store point was cancelled: {e:?}"),
            };
            let status = PointStatus {
                is_ill_formed: matches!(dag_point, DagPoint::IllFormed(_)),
                is_validated: true,
                is_valid: dag_point.valid().is_some(),
                is_trusted: dag_point.trusted().is_some(),
                is_certified: !once_certified_tx_clone.has_value(),
                ..Default::default()
            };
            tokio::task::spawn_blocking(move || {
                store.set_status(point_id.round, &point_id.digest, &status);
            })
            .await
            .expect("db set point status");
            state.init(&dag_point);
            dag_point
        };
        DagPointFuture(DagPointFutureType::Validate {
            task: Shared::new(JoinTask::new(task.instrument(span))),
            certified: once_certified_tx,
        })
    }

    pub fn new_restore(
        point_dag_round: &DagRound,
        info: &PointInfo,
        status: &PointStatus,
        state: &InclusionState,
        downloader: &Downloader,
        store: &MempoolStore,
        effects: &Effects<EngineContext>,
    ) -> Self {
        let ready_dag_point = if status.is_trusted | status.is_certified {
            Some(DagPoint::Trusted(ValidPoint::new(info.clone())))
        } else if status.is_valid {
            Some(DagPoint::Suspicious(ValidPoint::new(info.clone())))
        } else if status.is_ill_formed {
            Some(DagPoint::IllFormed(Arc::new(info.id())))
        } else if status.is_validated {
            Some(DagPoint::Invalid(info.clone()))
        } else {
            None
        };
        if let Some(dag_point) = ready_dag_point {
            if let Some(valid) = dag_point.valid() {
                if status.committed_at_round.is_some() {
                    valid.is_committed.store(true, Ordering::Relaxed);
                }
            }
            state.init(&dag_point);
            return Self(DagPointFutureType::Ready(future::ready(dag_point)));
        }
        let point_dag_round = point_dag_round.downgrade();
        let store = store.clone();
        let point_id = info.id();
        let state = state.clone();
        let downloader = downloader.clone();
        let (certified_tx, certified_rx) = oneshot::channel();
        let once_certified_tx = Arc::new(OnceTake::new(certified_tx));
        let once_certified_tx_clone = once_certified_tx.clone();
        let effects_clone = effects.clone();
        let task = async move {
            let point = tokio::task::spawn_blocking({
                let store = store.clone();
                let digest = point_id.digest.clone();
                move || store.get_point(point_id.round, &digest)
            })
            .await
            .expect("db get point")
            .expect("point with info and status was not loaded");
            let effects = Effects::<ValidateContext>::new(&effects_clone, &point);
            let dag_point = Verifier::validate(
                point,
                point_dag_round,
                downloader,
                store.clone(),
                certified_rx,
                effects,
            )
            .await;
            let status = PointStatus {
                is_ill_formed: matches!(dag_point, DagPoint::IllFormed(_)),
                is_validated: true,
                is_valid: dag_point.valid().is_some(),
                is_trusted: dag_point.trusted().is_some(),
                is_certified: !once_certified_tx_clone.has_value(),
                ..Default::default()
            };
            tokio::task::spawn_blocking(move || {
                store.set_status(point_id.round, &point_id.digest, &status);
            })
            .await
            .expect("db set point status");
            state.init(&dag_point); // only after persisted
            dag_point
        };
        Self(DagPointFutureType::Validate {
            task: Shared::new(JoinTask::new(task.instrument(effects.span().clone()))),
            certified: Arc::new(OnceTake::empty()),
        })
    }

    pub fn new_load(
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
        let once_certified_tx = Arc::new(OnceTake::new(certified_tx));
        let once_certified_tx_clone = once_certified_tx.clone();
        let point_id = PointId {
            author: *author,
            round: point_dag_round.round(),
            digest: digest.clone(),
        };
        let point_dag_round = point_dag_round.downgrade();
        let store = store.clone();
        let span = effects.span().clone();
        let effects = effects.clone();
        let task = async move {
            let stored_valid = tokio::task::spawn_blocking({
                let store = store.clone();
                let point_id = point_id.clone();
                move || match store.get_status(point_id.round, &point_id.digest) {
                    Some(status) if status.is_valid || status.is_certified => {
                        store.get_info(point_id.round, &point_id.digest)
                    }
                    _ => None,
                }
            })
            .await
            .expect("db get point info status");

            let stored_verified = match stored_valid {
                Some(info) => return DagPoint::Trusted(ValidPoint::new(info)),
                None => tokio::task::spawn_blocking({
                    let store = store.clone();
                    let point_id = point_id.clone();
                    move || store.get_point(point_id.round, &point_id.digest)
                })
                .await
                .expect("db get point"),
            };

            let (verified, storage_fut) = match stored_verified {
                Some(point) => (point, future::Either::Left(future::ready(Ok(())))),
                None => {
                    let downloaded = downloader
                        .run(
                            &point_id,
                            dependents_rx,
                            broadcast_rx,
                            Effects::<DownloadContext>::new(&effects, &point_id),
                        )
                        .await;
                    let verified = match downloaded {
                        DownloadResult::Verified(point) => point,
                        DownloadResult::IllFormed(point) => {
                            tokio::task::spawn_blocking({
                                let store = store.clone();
                                let status = PointStatus {
                                    is_ill_formed: true,
                                    ..Default::default()
                                };
                                move || store.insert_point(&point, &status)
                            })
                            .await
                            .expect("db store ill-formed download");
                            return DagPoint::IllFormed(Arc::new(point_id));
                        }
                        DownloadResult::NotFound => return DagPoint::NotFound(Arc::new(point_id)),
                    };
                    let stored_fut = future::Either::Right(tokio::task::spawn_blocking({
                        let verified = verified.clone();
                        let store = store.clone();
                        move || store.insert_point(&verified, &PointStatus::default())
                    }));
                    (verified, stored_fut)
                }
            };

            let deeper_effects = Effects::<ValidateContext>::new(&effects, &verified);
            tracing::trace!(
                parent: deeper_effects.span(),
                "downloaded, start validating",
            );

            let validated_fut = Verifier::validate(
                verified,
                point_dag_round,
                downloader,
                store.clone(),
                certified_rx,
                deeper_effects,
            );
            // do not abort store if not valid
            let dag_point = match tokio::join!(storage_fut, validated_fut) {
                (Ok(_), validated) => validated,
                (Err(err), _) if err.is_panic() => std::panic::resume_unwind(err.into_panic()),
                (Err(e), _) => panic!("store point was cancelled: {e:?}"),
            };
            let status = PointStatus {
                is_ill_formed: matches!(dag_point, DagPoint::IllFormed(_)),
                is_validated: true,
                is_valid: dag_point.valid().is_some(),
                is_trusted: dag_point.trusted().is_some(),
                is_certified: !once_certified_tx_clone.has_value(),
                ..Default::default()
            };
            tokio::task::spawn_blocking(move || {
                store.set_status(point_id.round, &point_id.digest, &status);
            })
            .await
            .expect("db set point status");

            state.init(&dag_point);
            dag_point
        };
        DagPointFuture(DagPointFutureType::Load {
            task: Shared::new(JoinTask::new(task.instrument(span))),
            certified: once_certified_tx,
            dependents: dependents_tx,
            verified: Arc::new(OnceTake::new(broadcast_tx)),
        })
    }

    pub fn add_depender(&self, dependent: &PeerId) {
        if let DagPointFutureType::Load { dependents, .. } = &self.0 {
            // receiver is dropped upon completion
            _ = dependents.send(*dependent);
        }
    }

    pub fn resolve_download(&self, broadcast: &Point) {
        if let DagPointFutureType::Load { verified, .. } = &self.0 {
            if let Some(oneshot) = verified.take() {
                // receiver is dropped upon completion
                _ = oneshot.send(broadcast.clone());
            }
        }
    }

    pub fn mark_certified(&self) {
        // every vertex is certified by definition,
        // but also every vertex dependency is certified transitively
        if let DagPointFutureType::Validate { certified, .. }
        | DagPointFutureType::Load { certified, .. } = &self.0
        {
            // FIXME limit by validation depth
            if let Some(oneshot) = certified.take() {
                // TODO store status when taken or follow only in-mem recursion?
                // receiver is dropped upon completion
                _ = oneshot.send(());
            }
        }
    }
}
