use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use everscale_crypto::ed25519::KeyPair;
use futures_util::future::BoxFuture;
use futures_util::{future, FutureExt};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::Instrument;
use tycho_network::PeerId;
use tycho_storage::point_status::PointStatus;
use tycho_util::futures::{JoinTask, Shared};
use tycho_util::sync::OnceTake;

use crate::dag::dag_location::InclusionState;
use crate::dag::{DagRound, Verifier};
use crate::effects::{AltFormat, Ctx, DownloadCtx, MempoolStore, RoundCtx, ValidateCtx};
use crate::intercom::{DownloadResult, Downloader};
use crate::models::{Cert, DagPoint, Digest, Point, PointId, PointInfo, ValidPoint};

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
            DagPointFutureType::Store(task) => match task.poll_unpin(cx) {
                Poll::Ready((dag_point, _)) => Poll::Ready(dag_point),
                Poll::Pending => Poll::Pending,
            },
        }
    }
}

#[derive(Clone)]
enum DagPointFutureType {
    Validate {
        task: Shared<JoinTask<DagPoint>>,
        // normally, if we are among the last nodes to validate some broadcast point,
        // we can receive its proof from author, check its signatures and skip validation of the vertex and its deps
        certified: Arc<OnceTake<oneshot::Sender<()>>>,
    },
    Load {
        task: Shared<JoinTask<DagPoint>>,
        // this could be a `Notify`, but both sender and receiver must be used only once
        certified: Arc<OnceTake<oneshot::Sender<()>>>,
        dependers_tx: mpsc::UnboundedSender<PeerId>,
        resolve: Arc<OnceTake<oneshot::Sender<DownloadResult>>>,
    },
    Store(Shared<BoxFuture<'static, DagPoint>>),
}

impl DagPointFuture {
    /// locally created points are assumed to be valid, checked prior insertion if needed;
    /// for points of others - there are all other methods
    pub fn new_local_valid(
        point: &Point,
        state: &InclusionState,
        store: &MempoolStore,
        key_pair: Option<&Arc<KeyPair>>,
        round_ctx: &RoundCtx,
    ) -> Self {
        let handle = {
            let point = point.clone();
            let state = state.clone();
            let store = store.clone();
            let key_pair = key_pair.cloned();
            let round_ctx = round_ctx.clone();

            tokio::task::spawn_blocking(move || {
                let _span = round_ctx.span().enter();

                let status = PointStatus {
                    is_ill_formed: false,
                    is_validated: false, // Note this is distinct from other valid points' statuses
                    is_valid: true,
                    is_trusted: true,
                    ..Default::default()
                };
                store.insert_point(&point, &status);
                let dag_point = DagPoint::Trusted(ValidPoint::new(&point));
                let signable = state.init(&dag_point); // only after persisted
                signable.sign(point.round(), key_pair.as_deref());
                assert!(
                    signable.signed().is_some_and(|sig| sig.is_ok()),
                    "Coding or configuration error: local point cannot be signed; \
                    node is not in validator set?"
                );
                dag_point
            })
        };
        let boxed = Self::box_blocking(handle, point, round_ctx, "new_local_valid()");

        Self(DagPointFutureType::Store(Shared::new(boxed)))
    }

    pub fn new_ill_formed_broadcast(
        point: &Point,
        state: &InclusionState,
        store: &MempoolStore,
        round_ctx: &RoundCtx,
    ) -> Self {
        let handle = {
            let point = point.clone();
            let state = state.clone();
            let store = store.clone();
            let round_ctx = round_ctx.clone();

            tokio::task::spawn_blocking(move || {
                let _span = round_ctx.span().enter();

                let status = PointStatus {
                    is_ill_formed: true, // Note: it was not validated, can't be certified
                    ..Default::default()
                };
                store.insert_point(&point, &status);
                let dag_point = DagPoint::IllFormed(Arc::new(point.id()));
                state.init(&dag_point);
                dag_point
            })
        };
        let boxed = Self::box_blocking(handle, point, round_ctx, "new_ill_formed_broadcast()");

        Self(DagPointFutureType::Store(Shared::new(boxed)))
    }

    fn box_blocking(
        handle: JoinHandle<DagPoint>,
        point: &Point,
        round_ctx: &RoundCtx,
        method: &'static str,
    ) -> BoxFuture<'static, DagPoint> {
        let point_id = point.id();
        let round_ctx = round_ctx.clone();

        async move {
            match handle.await {
                Ok(dag_point) => dag_point,
                Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
                Err(e) => {
                    // should not happen, as blocking threads cannot be cancelled
                    tracing::error!(
                        parent: round_ctx.span(),
                        error = display(e),
                        method = method,
                        author = display(point_id.author.alt()),
                        round = point_id.round.0,
                        digest = display(point_id.digest.alt()),
                        "blocking thread exited, dag point future will not resolve"
                    );
                    future::pending().await
                }
            }
        }
        .boxed()
    }

    pub fn new_broadcast(
        point_dag_round: &DagRound,
        point: &Point,
        state: &InclusionState,
        downloader: &Downloader,
        store: &MempoolStore,
        round_ctx: &RoundCtx,
    ) -> Self {
        let point_dag_round = point_dag_round.downgrade();
        let info = PointInfo::from(point);
        let point = point.clone();
        let downloader = downloader.clone();
        let store = store.clone();
        let validate_ctx = ValidateCtx::new(round_ctx, &info);

        let (certified_tx, certified_rx) = oneshot::channel();
        let once_certified_tx = Arc::new(OnceTake::new(certified_tx));

        let task = async move {
            let point_id = point.id();
            let prev_proof = point.prev_proof();
            let stored_fut = tokio::task::spawn_blocking({
                let store = store.clone();
                move || store.insert_point(&point, &PointStatus::default())
            });
            let validated_fut = Verifier::validate(
                info,
                prev_proof,
                point_dag_round,
                downloader,
                store.clone(),
                certified_rx,
                validate_ctx,
            );
            // do not abort store if not valid
            let dag_point = match tokio::join!(stored_fut, validated_fut) {
                (Ok(_), validated) => validated,
                (Err(err), _) if err.is_panic() => std::panic::resume_unwind(err.into_panic()),
                (Err(e), _) => panic!("store point was cancelled: {e:?}"),
            };
            let status = PointStatus {
                is_validated: true,
                ..dag_point.basic_status()
            };
            tokio::task::spawn_blocking(move || {
                store.set_status(point_id.round, &point_id.digest, &status);
            })
            .await
            .expect("db set point status");
            dag_point
        };

        let state = state.clone();
        DagPointFuture(DagPointFutureType::Validate {
            task: Shared::new(JoinTask::new(
                task.inspect(move |dag_point| {
                    state.init(dag_point);
                })
                .instrument(round_ctx.span().clone()),
            )),
            certified: once_certified_tx,
        })
    }

    #[allow(clippy::too_many_arguments)] // TODO arch: make args less granular
    pub fn new_load<T>(
        point_dag_round: &DagRound,
        author: &PeerId,
        digest: &Digest,
        first_depender: Option<&PeerId>,
        state: &InclusionState,
        downloader: &Downloader,
        store: &MempoolStore,
        into_round_ctx: &T,
    ) -> Self
    where
        T: Ctx + Clone + Send + 'static,
        for<'a> &'a T: Into<RoundCtx>,
    {
        let point_id = PointId {
            author: *author,
            round: point_dag_round.round(),
            digest: *digest,
        };
        let point_dag_round = point_dag_round.downgrade();
        let downloader = downloader.clone();
        let store = store.clone();
        let span = into_round_ctx.span().clone();
        let into_round_ctx = into_round_ctx.clone();

        let (dependers_tx, dependers_rx) = mpsc::unbounded_channel();
        _ = dependers_tx.send(*author);
        if let Some(depender) = first_depender {
            _ = dependers_tx.send(*depender);
        }
        let (broadcast_tx, broadcast_rx) = oneshot::channel();
        let (certified_tx, certified_rx) = oneshot::channel();
        let once_certified_tx = Arc::new(OnceTake::new(certified_tx));
        let once_certified_tx_clone = once_certified_tx.clone();

        let task = async move {
            let stored = tokio::task::spawn_blocking({
                let store = store.clone();
                move || match store.get_status(point_id.round, &point_id.digest) {
                    Some(status)
                        if status.is_trusted || (status.is_valid && status.is_certified) =>
                    {
                        // should be often on reboot
                        let info = store
                            .get_info(point_id.round, point_id.digest)
                            .expect("info by status must exist in DB");
                        // Note: no need to check point's evidence one more time
                        Some(Ok((info, None, status)))
                    }
                    Some(status) if status.is_validated && !status.is_valid => {
                        let info = store
                            .get_info(point_id.round, point_id.digest)
                            .expect("info by status must exist in DB");
                        Some(Err(DagPoint::Invalid(Cert {
                            inner: info,
                            is_certified: status.is_certified,
                        })))
                    }
                    Some(status) if status.is_ill_formed => {
                        Some(Err(DagPoint::IllFormed(Arc::new(point_id))))
                    }
                    Some(status) => {
                        // have to load and drop the full point only because of evidence;
                        // should be the rarest case, when shutdown interrupted point validation
                        let point = store
                            .get_point(point_id.round, &point_id.digest)
                            .expect("point by status must exist in DB");
                        Some(Ok((PointInfo::from(&point), point.prev_proof(), status)))
                    }
                    _ => None, // normal
                }
            })
            .await
            .expect("db get point info status");

            let (verified, prev_proof, stored_status, storage_fut) = match stored {
                Some(Ok((info, prev_proof, status))) => (
                    info,
                    prev_proof,
                    Some(status),
                    future::Either::Left(future::ready(Ok(()))),
                ),
                Some(Err(dag_point)) => {
                    return dag_point;
                }
                None => {
                    let download_ctx = DownloadCtx::new(&into_round_ctx, &point_id);
                    let downloaded = downloader
                        .run(&point_id, dependers_rx, broadcast_rx, download_ctx)
                        .await;
                    let verified = match downloaded {
                        Some(DownloadResult::Verified(point)) => point,
                        Some(DownloadResult::IllFormed(point)) => {
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
                        None => {
                            return DagPoint::NotFound(Cert {
                                inner: Arc::new(point_id),
                                is_certified: once_certified_tx_clone.take().is_none(),
                            })
                        }
                    };
                    let stored_fut = future::Either::Right(tokio::task::spawn_blocking({
                        let verified = verified.clone();
                        let store = store.clone();
                        move || store.insert_point(&verified, &PointStatus::default())
                    }));
                    (
                        PointInfo::from(&verified),
                        verified.prev_proof(),
                        None,
                        stored_fut,
                    )
                }
            };

            // this may be root validation or child one
            let validate_ctx = ValidateCtx::new(&into_round_ctx, &verified);
            tracing::trace!(
                parent: validate_ctx.span(),
                "loaded, start validating",
            );

            if stored_status.is_some_and(|status| status.is_certified) {
                // note if the point contains valid evidence for a vertex,
                //  the vertex and its dependencies are marked certified, but not the point itself,
                //  so `Trusted` stored status does not trigger certification mark
                if let Some(certified) = once_certified_tx_clone.take() {
                    _ = certified.send(());
                }
            }
            let validated_fut = Verifier::validate(
                verified,
                prev_proof,
                point_dag_round,
                downloader,
                store.clone(),
                certified_rx,
                validate_ctx,
            );
            // do not abort store if not valid
            let dag_point = match tokio::join!(storage_fut, validated_fut) {
                (Ok(_), validated) => validated,
                (Err(err), _) if err.is_panic() => std::panic::resume_unwind(err.into_panic()),
                (Err(e), _) => panic!("store point was cancelled: {e:?}"),
            };
            let status = PointStatus {
                is_validated: true,
                ..dag_point.basic_status()
            };
            tokio::task::spawn_blocking(move || {
                store.set_status(point_id.round, &point_id.digest, &status);
            })
            .await
            .expect("db set point status");

            dag_point
        };

        let state = state.clone();
        DagPointFuture(DagPointFutureType::Load {
            task: Shared::new(JoinTask::new(
                task.inspect(move |dag_point| {
                    state.init(dag_point);
                })
                .instrument(span),
            )),
            certified: once_certified_tx,
            dependers_tx,
            resolve: Arc::new(OnceTake::new(broadcast_tx)),
        })
    }

    pub fn add_depender(&self, depender: &PeerId) {
        if let DagPointFutureType::Load { dependers_tx, .. } = &self.0 {
            // receiver is dropped upon completion
            _ = dependers_tx.send(*depender);
        }
    }

    pub fn resolve_download(&self, broadcast: &Point, is_ok: bool) {
        if let DagPointFutureType::Load { resolve, .. } = &self.0 {
            if let Some(oneshot) = resolve.take() {
                let result = if is_ok {
                    DownloadResult::Verified(broadcast.clone())
                } else {
                    DownloadResult::IllFormed(broadcast.clone())
                };
                // receiver is dropped upon completion
                oneshot.send(result).ok();
            }
        }
    }

    pub fn mark_certified(&self) {
        // every vertex is certified by definition,
        // but also every vertex dependency is certified transitively
        if let DagPointFutureType::Validate { certified, .. }
        | DagPointFutureType::Load { certified, .. } = &self.0
        {
            if let Some(oneshot) = certified.take() {
                // receiver is dropped upon completion
                _ = oneshot.send(());
            }
        }
    }
}
