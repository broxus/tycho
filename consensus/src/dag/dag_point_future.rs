use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use everscale_crypto::ed25519::KeyPair;
use futures_util::future::{BoxFuture, Either};
use futures_util::{future, FutureExt};
use tokio::sync::{mpsc, oneshot};
use tycho_network::PeerId;
use tycho_util::futures::Shared;
use tycho_util::sync::OnceTake;

use crate::dag::dag_location::InclusionState;
use crate::dag::{DagRound, IllFormedReason, ValidateResult, Verifier};
use crate::effects::{
    AltFormat, Ctx, DownloadCtx, MempoolStore, RoundCtx, TaskResult, ValidateCtx,
};
use crate::intercom::{DownloadResult, Downloader};
use crate::models::{
    DagPoint, Digest, Point, PointId, PointInfo, PointRestore, PointStatusIllFormed,
    PointStatusNotFound, PointStatusStored, PointStatusStoredRef, PointStatusValidated,
};

#[derive(Clone)]
pub struct DagPointFuture(DagPointFutureType);

impl future::Future for DagPointFuture {
    type Output = TaskResult<DagPoint>;

    #[inline]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match &mut self.0 {
            DagPointFutureType::Validate { task, .. }
            | DagPointFutureType::Download { task, .. }
            | DagPointFutureType::Simple(task) => match task.poll_unpin(cx) {
                Poll::Ready((result, _)) => Poll::Ready(result),
                Poll::Pending => Poll::Pending,
            },
        }
    }
}

#[derive(Clone)]
enum DagPointFutureType {
    Validate {
        task: Shared<BoxFuture<'static, TaskResult<DagPoint>>>,
        // normally, if we are among the last nodes to validate some broadcast point,
        // we can receive its proof from author, check its signatures and skip validation of the vertex and its deps
        certified: Arc<OnceTake<oneshot::Sender<()>>>,
    },
    Download {
        task: Shared<BoxFuture<'static, TaskResult<DagPoint>>>,
        // this could be a `Notify`, but both sender and receiver must be used only once
        certified: Arc<OnceTake<oneshot::Sender<()>>>,
        dependers_tx: mpsc::UnboundedSender<PeerId>,
        resolve: Arc<OnceTake<oneshot::Sender<DownloadResult>>>,
    },
    Simple(Shared<BoxFuture<'static, TaskResult<DagPoint>>>),
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
        let task = round_ctx.task().spawn_blocking({
            let point = point.clone();
            let state = state.clone();
            let store = store.clone();
            let key_pair = key_pair.cloned();
            let round_ctx = round_ctx.clone();

            move || {
                let _span = round_ctx.span().enter();

                let mut status = PointStatusValidated::default();
                status.is_valid = true;
                state.acquire(&point.id(), &mut status); // only after persisted

                assert!(
                    status.is_first_valid,
                    "local point must be first valid, got {status}"
                );

                let dag_point = DagPoint::new_validated(PointInfo::from(&point), &status);

                store.insert_point(
                    &point,
                    PointStatusStoredRef::Validated(&status),
                    round_ctx.conf(),
                );
                state.resolve(&dag_point);

                let signed = state.sign(point.round(), key_pair.as_deref(), round_ctx.conf());
                assert!(
                    signed.as_ref().is_some_and(|sig| sig.is_ok()),
                    "Coding or configuration error: local point cannot be signed; got {:?}",
                    signed.as_ref().map(|r| r.as_ref().map(|s| s.alt()))
                );
                dag_point
            }
        });

        Self(DagPointFutureType::Simple(Shared::new(task.boxed())))
    }

    pub fn new_ill_formed_broadcast(
        point: &Point,
        reason: &IllFormedReason,
        state: &InclusionState,
        store: &MempoolStore,
        round_ctx: &RoundCtx,
    ) -> Self {
        let task = round_ctx.task().spawn_blocking({
            let point = point.clone();
            let reason = reason.clone();
            let state = state.clone();
            let store = store.clone();
            let round_ctx = round_ctx.clone();

            move || {
                let _span = round_ctx.span().enter();
                let id = point.id();

                let mut status = PointStatusIllFormed::default();
                state.acquire(&id, &mut status); // only after persisted

                let dag_point = DagPoint::new_ill_formed(id, &status, reason);
                store.insert_point(
                    &point,
                    PointStatusStoredRef::IllFormed(&status),
                    round_ctx.conf(),
                );

                state.resolve(&dag_point);
                dag_point
            }
        });

        Self(DagPointFutureType::Simple(Shared::new(task.boxed())))
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
        let state = state.clone();
        let downloader = downloader.clone();
        let store = store.clone();
        let validate_ctx = ValidateCtx::new(round_ctx, &info);

        let (certified_tx, certified_rx) = oneshot::channel();

        let nested = round_ctx.task().spawn(async move {
            let point_id = point.id();
            let prev_proof = point.prev_proof();
            let stored_fut = validate_ctx.task().spawn_blocking({
                let store = store.clone();
                let ctx = validate_ctx.clone();
                move || store.insert_point(&point, PointStatusStoredRef::Exists, ctx.conf())
            });
            let validated = Verifier::validate(
                info.clone(),
                prev_proof,
                point_dag_round,
                downloader,
                store.clone(),
                certified_rx,
                validate_ctx.clone(),
            )
            .await?;
            stored_fut.await?;
            let (dag_point, status) =
                Self::acquire_validated(&state, info, validated, &validate_ctx);
            Ok(validate_ctx.task().spawn_blocking(move || {
                store.set_status(point_id.round, &point_id.digest, status.as_ref());
                state.resolve(&dag_point);
                dag_point
            }))
        });

        DagPointFuture(DagPointFutureType::Validate {
            task: Shared::new((async move { nested.await??.await }).boxed()),
            certified: Arc::new(OnceTake::new(certified_tx)),
        })
    }

    #[allow(clippy::too_many_arguments)] // TODO arch: make args less granular
    pub fn new_download<T>(
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
        let state = state.clone();
        let store = store.clone();
        let task_ctx = into_round_ctx.task();
        let into_round_ctx = into_round_ctx.clone();

        let (dependers_tx, dependers_rx) = mpsc::unbounded_channel();
        _ = dependers_tx.send(*author);
        if let Some(depender) = first_depender {
            _ = dependers_tx.send(*depender);
        }
        let (broadcast_tx, broadcast_rx) = oneshot::channel();
        let (certified_tx, certified_rx) = oneshot::channel();

        let nested = task_ctx.spawn(async move {
            let download_ctx = DownloadCtx::new(&into_round_ctx, &point_id);
            let store = store.clone();
            let downloaded = downloader
                .run(&point_id, dependers_rx, broadcast_rx, download_ctx)
                .await;
            match downloaded {
                Some(DownloadResult::Verified(point)) => {
                    let info = PointInfo::from(&point);
                    let prev_proof = point.prev_proof();
                    let storage_task = into_round_ctx.task().spawn_blocking({
                        let store = store.clone();
                        let ctx = into_round_ctx.clone();
                        move || store.insert_point(&point, PointStatusStoredRef::Exists, ctx.conf())
                    });
                    let validate_ctx = ValidateCtx::new(&into_round_ctx, &info);
                    let validated = Verifier::validate(
                        info.clone(),
                        prev_proof,
                        point_dag_round,
                        downloader,
                        store.clone(),
                        certified_rx,
                        validate_ctx,
                    )
                    .await?;
                    storage_task.await?;
                    let (dag_point, status) =
                        Self::acquire_validated(&state, info.clone(), validated, &into_round_ctx);
                    let ctx = into_round_ctx.clone();
                    Ok(into_round_ctx.task().spawn_blocking(move || {
                        let _guard = ctx.span().enter();
                        store.set_status(dag_point.round(), dag_point.digest(), status.as_ref());
                        state.resolve(&dag_point);
                        dag_point
                    }))
                }
                Some(DownloadResult::IllFormed(point, reason)) => {
                    let mut status = PointStatusIllFormed::default();
                    state.acquire(&point_id, &mut status);
                    let dag_point = DagPoint::new_ill_formed(point.id(), &status, reason);
                    let ctx = into_round_ctx.clone();
                    Ok(into_round_ctx.task().spawn_blocking(move || {
                        let _guard = ctx.span().enter();
                        store.insert_point(
                            &point,
                            PointStatusStoredRef::IllFormed(&status),
                            ctx.conf(),
                        );
                        state.resolve(&dag_point);
                        dag_point
                    }))
                }
                None => {
                    let mut status = PointStatusNotFound {
                        is_first_resolved: false,
                        is_certified: false,
                        author: point_id.author,
                    };
                    state.acquire(&point_id, &mut status);
                    let dag_point =
                        DagPoint::new_not_found(point_id.round, &point_id.digest, &status);
                    let ctx = into_round_ctx.clone();
                    Ok(into_round_ctx.task().spawn_blocking(move || {
                        let _guard = ctx.span().enter();
                        store.set_status(
                            point_id.round,
                            &point_id.digest,
                            PointStatusStoredRef::NotFound(&status),
                        );
                        state.resolve(&dag_point);
                        dag_point
                    }))
                }
            }
        });

        DagPointFuture(DagPointFutureType::Download {
            task: Shared::new((async move { nested.await??.await }).boxed()),
            certified: Arc::new(OnceTake::new(certified_tx)),
            dependers_tx,
            resolve: Arc::new(OnceTake::new(broadcast_tx)),
        })
    }

    pub fn new_restore(
        point_dag_round: &DagRound,
        point_restore: PointRestore,
        state: &InclusionState,
        downloader: &Downloader,
        store: &MempoolStore,
        round_ctx: &RoundCtx,
    ) -> Self {
        // keep this section sync so that call site may not wait for each result to resolve
        let validate_or_restore = match point_restore {
            PointRestore::Exists(info, prev_proof) => Either::Left((info, prev_proof)),
            PointRestore::Validated(info, status) => {
                let dag_point = DagPoint::new_validated(info, &status);
                state.acquire_restore(&dag_point.id(), &status);
                Either::Right(dag_point)
            }
            PointRestore::IllFormed(id, status) => {
                let dag_point =
                    DagPoint::new_ill_formed(id, &status, IllFormedReason::AfterLoadFromDb);
                state.acquire_restore(&dag_point.id(), &status);
                Either::Right(dag_point)
            }
            PointRestore::NotFound(round, digest, status) => {
                let dag_point = DagPoint::new_not_found(round, &digest, &status);
                state.acquire_restore(&dag_point.id(), &status);
                Either::Right(dag_point)
            }
        };

        match validate_or_restore {
            Either::Left((verified, prev_proof)) => {
                let point_dag_round = point_dag_round.downgrade();
                let state = state.clone();
                let downloader = downloader.clone();
                let store = store.clone();
                let round_ctx = round_ctx.clone();
                let ctx = round_ctx.clone();

                let (certified_tx, certified_rx) = oneshot::channel();

                let future = async move {
                    let validate_ctx = ValidateCtx::new(&round_ctx, &verified);
                    let validated = Verifier::validate(
                        verified.clone(),
                        prev_proof,
                        point_dag_round,
                        downloader,
                        store.clone(),
                        certified_rx,
                        validate_ctx,
                    )
                    .await;
                    let (dag_point, status) =
                        Self::acquire_validated(&state, verified, validated?, &round_ctx);
                    let ctx = round_ctx.clone();
                    Ok(round_ctx.task().spawn_blocking(move || {
                        let _guard = ctx.span().enter();
                        store.set_status(dag_point.round(), dag_point.digest(), status.as_ref());
                        state.resolve(&dag_point);
                        dag_point
                    }))
                };
                let lazy = (async move { ctx.task().spawn(future).await??.await }).boxed();
                DagPointFuture(DagPointFutureType::Validate {
                    task: Shared::new(lazy),
                    certified: Arc::new(OnceTake::new(certified_tx)),
                })
            }
            Either::Right(dag_point) => {
                state.resolve(&dag_point);
                let ready = future::ready(Ok(dag_point)).boxed();
                DagPointFuture(DagPointFutureType::Simple(Shared::new(ready)))
            }
        }
    }

    fn acquire_validated(
        state: &InclusionState,
        info: PointInfo,
        validated: ValidateResult,
        ctx: &impl Ctx,
    ) -> (DagPoint, PointStatusStored) {
        let _guard = ctx.span().enter();
        let id = info.id();
        let is_valid = matches!(validated, ValidateResult::Valid { .. });
        // TODO fill anchor flags in status
        match validated {
            ValidateResult::Valid { is_certified } | ValidateResult::Invalid { is_certified } => {
                let mut status = PointStatusValidated::default();
                status.is_valid = is_valid;
                status.is_certified = is_certified;
                state.acquire(&id, &mut status);
                (
                    DagPoint::new_validated(info, &status),
                    PointStatusStored::Validated(status),
                )
            }
            ValidateResult::IllFormed(reason) => {
                let mut status = PointStatusIllFormed::default();
                state.acquire(&id, &mut status);
                (
                    DagPoint::new_ill_formed(id, &status, reason),
                    PointStatusStored::IllFormed(status),
                )
            }
        }
    }

    pub fn add_depender(&self, depender: &PeerId) {
        if let DagPointFutureType::Download { dependers_tx, .. } = &self.0 {
            // receiver is dropped upon completion
            _ = dependers_tx.send(*depender);
        }
    }

    pub fn resolve_download(&self, broadcast: &Point, ill_formed_reason: Option<&IllFormedReason>) {
        if let DagPointFutureType::Download { resolve, .. } = &self.0 {
            if let Some(oneshot) = resolve.take() {
                let result = match ill_formed_reason {
                    None => DownloadResult::Verified(broadcast.clone()),
                    Some(reason) => DownloadResult::IllFormed(broadcast.clone(), reason.clone()),
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
        | DagPointFutureType::Download { certified, .. } = &self.0
        {
            if let Some(oneshot) = certified.take() {
                // receiver is dropped upon completion
                _ = oneshot.send(());
            }
        }
    }
}
