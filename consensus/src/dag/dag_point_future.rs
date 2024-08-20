use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures_util::{future, FutureExt};
use tokio::sync::{mpsc, oneshot};
use tracing::Instrument;
use tycho_network::PeerId;
use tycho_storage::PointFlags;
use tycho_util::futures::{JoinTask, Shared};
use tycho_util::sync::OnceTake;

use crate::dag::dag_location::InclusionState;
use crate::dag::{DagRound, Verifier};
use crate::effects::{DownloadContext, Effects, EngineContext, MempoolStore, ValidateContext};
use crate::intercom::{DownloadResult, Downloader};
use crate::models::{DagPoint, Digest, PointId, PointInfo, ValidPoint};
use crate::Point;

#[derive(Clone)]
pub struct DagPointFuture(DagPointFutureType);

impl Future for DagPointFuture {
    type Output = DagPoint;

    #[inline]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match &mut self.0 {
            DagPointFutureType::Broadcast { task, .. } | DagPointFutureType::Load { task, .. } => {
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
    Broadcast {
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
        let flags = PointFlags {
            is_valid: true,
            is_trusted: true,
            ..Default::default()
        };
        store.insert_point(point, Some(&flags));
        let dag_point = DagPoint::Trusted(ValidPoint::new(PointInfo::from(point)));
        state.init(&dag_point); // only after persisted
        Self(DagPointFutureType::Ready(future::ready(dag_point)))
    }

    pub fn new_invalid(dag_point: DagPoint, state: &InclusionState, _store: &MempoolStore) -> Self {
        assert!(
            dag_point.valid().is_none(),
            "point must be invalid, but it is valid"
        );
        // TODO separate table? maybe use flags to load invalids only from flags?
        state.init(&dag_point);
        Self(DagPointFutureType::Ready(future::ready(dag_point)))
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
        let certified = Arc::new(OnceTake::new(certified_tx));
        let certified_clone = certified.clone();
        let task = async move {
            let point_id = point.id();
            let stored_fut = tokio::task::spawn_blocking({
                let verified = point.clone();
                let store = store.clone();
                move || store.insert_point(&verified, None)
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
            let flags = PointFlags {
                is_validated: true,
                is_valid: dag_point.valid().is_some(),
                is_trusted: dag_point.trusted().is_some(),
                is_certified: certified_clone.is_taken(),
                ..Default::default()
            };
            tokio::task::spawn_blocking(move || {
                store.set_flags(point_id.round, &point_id.digest, &flags);
            })
            .await
            .expect("db set point flags");
            state.init(&dag_point);
            dag_point
        };
        DagPointFuture(DagPointFutureType::Broadcast {
            task: Shared::new(JoinTask::new(task.instrument(span))),
            certified,
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
        let certified = Arc::new(OnceTake::new(certified_tx));
        let certified_clone = certified.clone();
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
                move || {
                    let flags = store.get_flags(point_id.round, &point_id.digest);
                    if flags.is_valid || flags.is_certified {
                        store.get_info(point_id.round, &point_id.digest)
                    } else {
                        None
                    }
                }
            })
            .await
            .expect("db get point info flags");

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
                        DownloadResult::IllFormed(_) => {
                            // TODO use separate store for invalid points, as it has valid sig
                            return DagPoint::IllFormed(Arc::new(point_id));
                        }
                        DownloadResult::NotFound => return DagPoint::NotFound(Arc::new(point_id)),
                    };
                    let stored_fut = future::Either::Right(tokio::task::spawn_blocking({
                        let verified = verified.clone();
                        let store = store.clone();
                        move || store.insert_point(&verified, None)
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
            let flags = PointFlags {
                is_validated: true,
                is_valid: dag_point.valid().is_some(),
                is_trusted: dag_point.trusted().is_some(),
                is_certified: certified_clone.is_taken(),
                ..Default::default()
            };
            tokio::task::spawn_blocking(move || {
                store.set_flags(point_id.round, &point_id.digest, &flags);
            })
            .await
            .expect("db set point flags");

            state.init(&dag_point);
            dag_point
        };
        DagPointFuture(DagPointFutureType::Load {
            task: Shared::new(JoinTask::new(task.instrument(span))),
            certified,
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
        if let DagPointFutureType::Broadcast { certified, .. }
        | DagPointFutureType::Load { certified, .. } = &self.0
        {
            // FIXME limit by validation depth
            if let Some(oneshot) = certified.take() {
                // receiver is dropped upon completion
                _ = oneshot.send(());
            }
        }
    }
}
