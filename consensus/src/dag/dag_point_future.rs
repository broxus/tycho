use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures_util::{future, FutureExt};
use tokio::sync::{mpsc, oneshot};
use tracing::Instrument;
use tycho_network::PeerId;
use tycho_storage::point_status::PointStatus;
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
            DagPointFutureType::Validate { task, .. }
            | DagPointFutureType::Load { task, .. }
            | DagPointFutureType::Store(task) => match task.poll_unpin(cx) {
                Poll::Ready((dag_point, _)) => Poll::Ready(dag_point),
                Poll::Pending => Poll::Pending,
            },
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
        dependers_tx: mpsc::UnboundedSender<PeerId>,
        verified: Arc<OnceTake<oneshot::Sender<Point>>>,
    },
    Store(Shared<JoinTask<DagPoint>>),
    Ready(future::Ready<DagPoint>),
}

impl DagPointFuture {
    /// locally created points are assumed to be valid, checked prior insertion if needed;
    /// for points of others - there are all other methods
    pub fn new_local_trusted(point: &Point, state: &InclusionState, store: &MempoolStore) -> Self {
        let status = PointStatus {
            is_ill_formed: false,
            is_validated: false, // Note this is distinct from other valid points' statuses
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
                    is_ill_formed: true, // Note: it was not validated, can't be certified
                    ..Default::default()
                };
                store.insert_point(&point, &status);
                let dag_point = DagPoint::IllFormed(Arc::new(point.id()));
                state.init(&dag_point);
                dag_point
            }
        });
        let task = async move { store_fut.await.expect("db insert ill-formed broadcast") };
        Self(DagPointFutureType::Store(Shared::new(JoinTask::new(
            task.instrument(effects.span().clone()),
        ))))
    }

    pub fn new_broadcast(
        point_dag_round: &DagRound,
        point: &Point,
        state: &InclusionState,
        downloader: &Downloader,
        store: &MempoolStore,
        effects: &Effects<EngineContext>,
    ) -> Self {
        let point_dag_round = point_dag_round.downgrade();
        let info = PointInfo::from(point);
        let point = point.clone();
        let state = state.clone();
        let downloader = downloader.clone();
        let store = store.clone();
        let validate_effects = Effects::<ValidateContext>::new(effects, &info);

        let (certified_tx, certified_rx) = oneshot::channel();
        let once_certified_tx = Arc::new(OnceTake::new(certified_tx));
        let once_certified_tx_clone = once_certified_tx.clone();

        let task = async move {
            let prev_proof = point.prev_proof();
            let point_id = point.id();
            let stored_fut = tokio::task::spawn_blocking({
                let store = store.clone();
                move || store.insert_point(&point, &PointStatus::default())
            });
            let validated_fut = Verifier::validate(
                info,
                prev_proof,
                point_dag_round,
                false, // mandatory
                downloader,
                store.clone(),
                certified_rx,
                validate_effects,
            );
            // do not abort store if not valid
            let dag_point = match tokio::join!(stored_fut, validated_fut) {
                (Ok(_), validated) => validated,
                (Err(err), _) if err.is_panic() => std::panic::resume_unwind(err.into_panic()),
                (Err(e), _) => panic!("store point was cancelled: {e:?}"),
            };
            let status = PointStatus {
                is_validated: true,
                is_certified: !once_certified_tx_clone.has_value(),
                ..dag_point.basic_status()
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
            task: Shared::new(JoinTask::new(task.instrument(effects.span().clone()))),
            certified: once_certified_tx,
        })
    }

    pub fn new_restore(
        point_dag_round: &DagRound,
        info: &PointInfo,
        status: PointStatus,
        state: &InclusionState,
        downloader: &Downloader,
        store: &MempoolStore,
        effects: &Effects<EngineContext>,
    ) -> Self {
        if status.is_ill_formed {
            return Self(DagPointFutureType::Ready(future::ready(
                DagPoint::IllFormed(Arc::new(info.id())),
            )));
        }

        let point_dag_round = point_dag_round.downgrade();
        let info = info.clone();
        let state = state.clone();
        let downloader = downloader.clone();
        let store = store.clone();
        let validate_effects = Effects::<ValidateContext>::new(effects, &info);

        let (certified_tx, certified_rx) = oneshot::channel();
        let once_certified_tx = Arc::new(OnceTake::new(certified_tx));
        let once_certified_tx_clone = once_certified_tx.clone();

        let task = async move {
            let dag_point = if status.is_trusted
                || status.is_certified
                || status.is_valid
                || status.is_validated
            {
                // must call `Self::new_load()` for dependencies not restored
                Verifier::validate(
                    info.clone(),
                    None, // no matter if already validated
                    point_dag_round,
                    true, // if true - just need resolve dependencies
                    downloader,
                    store.clone(),
                    certified_rx,
                    validate_effects,
                )
                .await;
                // no matter the current result, we restore the previous state
                if status.is_trusted || status.is_certified {
                    DagPoint::Trusted(ValidPoint::new(info.clone()))
                } else if status.is_valid {
                    DagPoint::Suspicious(ValidPoint::new(info.clone()))
                } else if status.is_validated {
                    DagPoint::Invalid(info.clone())
                } else {
                    unreachable!()
                }
            } else {
                // rare case when point validation was not completed, have to load full point
                let point = tokio::task::spawn_blocking({
                    let store = store.clone();
                    let round = info.round();
                    let digest = info.digest().clone();
                    move || {
                        store
                            .get_point(round, &digest)
                            .expect("point with info and status was not loaded")
                    }
                })
                .await
                .expect("db get point");
                let round = info.round();
                let digest = info.digest().clone();
                let prev_proof = point.prev_proof();
                drop(point);
                let dag_point = Verifier::validate(
                    info,
                    prev_proof,
                    point_dag_round,
                    false,
                    downloader,
                    store.clone(),
                    certified_rx,
                    validate_effects,
                )
                .await;
                let new_status = PointStatus {
                    is_validated: true,
                    is_certified: !once_certified_tx_clone.has_value(),
                    ..dag_point.basic_status()
                };
                tokio::task::spawn_blocking(move || store.set_status(round, &digest, &new_status))
                    .await
                    .expect("db set point status");
                dag_point
            };
            state.init(&dag_point); // only after persisted
            dag_point
        };

        Self(DagPointFutureType::Validate {
            task: Shared::new(JoinTask::new(task.instrument(effects.span().clone()))),
            certified: once_certified_tx,
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
        let point_id = PointId {
            author: *author,
            round: point_dag_round.round(),
            digest: digest.clone(),
        };
        let point_dag_round = point_dag_round.downgrade();
        let state = state.clone();
        let downloader = downloader.clone();
        let store = store.clone();
        let span = effects.span().clone();
        let effects = effects.clone();

        let (dependers_tx, dependers_rx) = mpsc::unbounded_channel();
        let (broadcast_tx, broadcast_rx) = oneshot::channel();
        let (certified_tx, certified_rx) = oneshot::channel();
        let once_certified_tx = Arc::new(OnceTake::new(certified_tx));
        let once_certified_tx_clone = once_certified_tx.clone();

        let task = async move {
            let stored_valid = tokio::task::spawn_blocking({
                let store = store.clone();
                let point_id = point_id.clone();
                move || match store.get_status(point_id.round, &point_id.digest) {
                    Some(status) if status.is_valid || status.is_certified => {
                        store.get_info(point_id.round, point_id.digest)
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
                            dependers_rx,
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
                                    is_ill_formed: true, // Note: it was not validated
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

            let info = PointInfo::from(&verified);
            let prev_proof = verified.prev_proof();
            drop(verified);

            let deeper_effects = Effects::<ValidateContext>::new(&effects, &info);
            tracing::trace!(
                parent: deeper_effects.span(),
                "downloaded, start validating",
            );

            let validated_fut = Verifier::validate(
                info,
                prev_proof,
                point_dag_round,
                false, // if we got here, point was not stored as valid
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
                is_validated: true,
                is_certified: !once_certified_tx_clone.has_value(),
                ..dag_point.basic_status()
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
            dependers_tx,
            verified: Arc::new(OnceTake::new(broadcast_tx)),
        })
    }

    pub fn add_depender(&self, depender: &PeerId) {
        if let DagPointFutureType::Load { dependers_tx, .. } = &self.0 {
            // receiver is dropped upon completion
            _ = dependers_tx.send(*depender);
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
