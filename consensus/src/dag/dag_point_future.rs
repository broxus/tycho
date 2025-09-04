use std::pin::Pin;
use std::sync::{Arc, LazyLock};
use std::task::{Context, Poll};

use ahash::HashMapExt;
use futures_util::future::{BoxFuture, Either};
use futures_util::{FutureExt, future};
use tokio::sync::{mpsc, oneshot};
use tycho_crypto::ed25519::KeyPair;
use tycho_network::PeerId;
use tycho_util::FastHashMap;
use tycho_util::futures::{Shared, WeakShared};
use tycho_util::sync::OnceTake;

use crate::dag::dag_location::InclusionState;
use crate::dag::{DagRound, IllFormedReason, ValidateResult, Verifier};
use crate::effects::{
    AltFormat, Cancelled, Ctx, DownloadCtx, RoundCtx, SpawnLimit, TaskResult, ValidateCtx,
};
use crate::engine::NodeConfig;
use crate::intercom::{DownloadResult, Downloader};
use crate::models::{
    Cert, CertDirectDeps, DagPoint, Digest, Point, PointId, PointInfo, PointRestore,
    PointStatusIllFormed, PointStatusNotFound, PointStatusStored, PointStatusStoredRef,
    PointStatusValidated, WeakCert,
};
use crate::storage::MempoolStore;
static LIMIT: LazyLock<SpawnLimit> =
    LazyLock::new(|| SpawnLimit::new(NodeConfig::get().max_blocking_tasks.get() as usize));
#[derive(Clone)]
pub struct DagPointFuture(DagPointFutureType);
impl future::Future for DagPointFuture {
    type Output = TaskResult<DagPoint>;
    #[inline]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match &mut self.0 {
            DagPointFutureType::Validate { task, .. }
            | DagPointFutureType::Download { task, .. } => match task.poll_unpin(cx) {
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
        cert: Cert,
    },
    Download {
        task: Shared<BoxFuture<'static, TaskResult<DagPoint>>>,
        cert: Cert,
        dependers_tx: mpsc::UnboundedSender<PeerId>,
        resolve: Arc<OnceTake<oneshot::Sender<DownloadResult>>>,
    },
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
        let point = point.clone();
        let state = state.clone();
        let store = store.clone();
        let key_pair = key_pair.cloned();
        let task_ctx = round_ctx.task();
        let round_ctx = round_ctx.clone();
        let cert = Cert::default();
        let cert_clone = cert.clone();
        #[allow(clippy::async_yields_async, reason = "spawn blocking task in async")]
        let nested = task_ctx.spawn(async move {
            let mut __guard = crate::__async_profile_guard__::Guard::new(
                concat!(module_path!(), "::async_block"),
                file!(),
                line!(),
            );
            let ctx = round_ctx.clone();
            let full_fn = move || {
                let _span = round_ctx.span().enter();
                let mut status = PointStatusValidated::default();
                status.is_valid = true;
                state.acquire(&point.info().id(), &mut status);
                assert!(
                    status.is_first_valid,
                    "local point must be first valid, got {status}"
                );
                let dag_point = DagPoint::new_validated(point.info().clone(), cert, &status);
                store.insert_point(&point, PointStatusStoredRef::Validated(&status));
                state.resolve(&dag_point);
                let signed =
                    state.sign(point.info().round(), key_pair.as_deref(), round_ctx.conf());
                assert!(
                    signed.as_ref().is_some_and(|sig| sig.is_ok()),
                    "Coding or configuration error: local point cannot be signed; got {:?}",
                    signed.as_ref().map(|r| r.as_ref().map(|s| s.alt()))
                );
                dag_point
            };
            Ok({
                __guard.end_section(line!());
                let __result = LIMIT.spawn_blocking(ctx.task(), full_fn).await;
                __guard.start_section(line!());
                __result
            })
        });
        Self(DagPointFutureType::Validate {
            task: Shared::new(
                (async move {
                    let mut __guard = crate::__async_profile_guard__::Guard::new(
                        concat!(module_path!(), "::async_block"),
                        file!(),
                        line!(),
                    );
                    {
                        __guard.end_section(line!());
                        let __result = nested.await?.await;
                        __guard.start_section(line!());
                        __result
                    }
                })
                .boxed(),
            ),
            cert: cert_clone,
        })
    }
    pub fn new_ill_formed_broadcast(
        point: &Point,
        reason: &IllFormedReason,
        state: &InclusionState,
        store: &MempoolStore,
        round_ctx: &RoundCtx,
    ) -> Self {
        let point = point.clone();
        let reason = reason.clone();
        let state = state.clone();
        let store = store.clone();
        let task_ctx = round_ctx.task();
        let round_ctx = round_ctx.clone();
        let cert = Cert::default();
        let cert_clone = cert.clone();
        #[allow(clippy::async_yields_async, reason = "spawn blocking task in async")]
        let nested = task_ctx.spawn(async move {
            let mut __guard = crate::__async_profile_guard__::Guard::new(
                concat!(module_path!(), "::async_block"),
                file!(),
                line!(),
            );
            let ctx = round_ctx.clone();
            let full_fn = move || {
                let _span = round_ctx.span().enter();
                let id = point.info().id();
                let mut status = PointStatusIllFormed::default();
                state.acquire(&id, &mut status);
                let dag_point = DagPoint::new_ill_formed(id, cert, &status, reason);
                store.insert_point(&point, PointStatusStoredRef::IllFormed(&status));
                state.resolve(&dag_point);
                dag_point
            };
            Ok({
                __guard.end_section(line!());
                let __result = LIMIT.spawn_blocking(ctx.task(), full_fn).await;
                __guard.start_section(line!());
                __result
            })
        });
        Self(DagPointFutureType::Validate {
            task: Shared::new(
                (async move {
                    let mut __guard = crate::__async_profile_guard__::Guard::new(
                        concat!(module_path!(), "::async_block"),
                        file!(),
                        line!(),
                    );
                    {
                        __guard.end_section(line!());
                        let __result = nested.await?.await;
                        __guard.start_section(line!());
                        __result
                    }
                })
                .boxed(),
            ),
            cert: cert_clone,
        })
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
        let info = point.info().clone();
        let point = point.clone();
        let state = state.clone();
        let downloader = downloader.clone();
        let store = store.clone();
        let validate_ctx = ValidateCtx::new(round_ctx, &info);
        let cert = Cert::default();
        let cert_clone = cert.clone();
        let nested = round_ctx.task().spawn(async move {
            let mut __guard = crate::__async_profile_guard__::Guard::new(
                concat!(module_path!(), "::async_block"),
                file!(),
                line!(),
            );
            let point_id = point.info().id();
            let store_fn = {
                let store = store.clone();
                let status_ref = PointStatusStoredRef::Exists;
                move || store.insert_point(&point, status_ref)
            };
            let store_task = {
                __guard.end_section(line!());
                let __result = LIMIT.spawn_blocking(validate_ctx.task(), store_fn).await;
                __guard.start_section(line!());
                __result
            };
            let validated = {
                __guard.end_section(line!());
                let __result = Verifier::validate(
                    info.clone(),
                    point_dag_round,
                    downloader,
                    store.clone(),
                    cert.clone(),
                    validate_ctx.clone(),
                )
                .await;
                __guard.start_section(line!());
                __result
            }?;
            {
                __guard.end_section(line!());
                let __result = store_task.await;
                __guard.start_section(line!());
                __result
            }?;
            let (dag_point, status) =
                Self::acquire_validated(&state, info, cert, validated, &validate_ctx);
            let store_fn = move || {
                store.set_status(point_id.round, &point_id.digest, status.as_ref());
                state.resolve(&dag_point);
                dag_point
            };
            let store_task = {
                __guard.end_section(line!());
                let __result = LIMIT.spawn_blocking(validate_ctx.task(), store_fn).await;
                __guard.start_section(line!());
                __result
            };
            Ok(store_task)
        });
        DagPointFuture(DagPointFutureType::Validate {
            task: Shared::new(
                (async move {
                    let mut __guard = crate::__async_profile_guard__::Guard::new(
                        concat!(module_path!(), "::async_block"),
                        file!(),
                        line!(),
                    );
                    {
                        __guard.end_section(line!());
                        let __result = nested.await?.await;
                        __guard.start_section(line!());
                        __result
                    }
                })
                .boxed(),
            ),
            cert: cert_clone,
        })
    }
    #[allow(clippy::too_many_arguments)]
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
        let cert = Cert::default();
        let cert_clone = cert.clone();
        let nested = task_ctx.spawn(async move {
            let mut __guard = crate::__async_profile_guard__::Guard::new(
                concat!(module_path!(), "::async_block"),
                file!(),
                line!(),
            );
            let download_ctx = DownloadCtx::new(&into_round_ctx, &point_id);
            let store = store.clone();
            let downloaded = {
                __guard.end_section(line!());
                let __result = downloader
                    .run(&point_id, dependers_rx, broadcast_rx, download_ctx)
                    .await;
                __guard.start_section(line!());
                __result
            }?;
            match downloaded {
                Some(DownloadResult::Verified(point)) => {
                    let info = point.info().clone();
                    let store_fn = {
                        let store = store.clone();
                        let status_ref = PointStatusStoredRef::Exists;
                        move || store.insert_point(&point, status_ref)
                    };
                    let store_task = {
                        __guard.end_section(line!());
                        let __result = LIMIT.spawn_blocking(into_round_ctx.task(), store_fn).await;
                        __guard.start_section(line!());
                        __result
                    };
                    let validate_ctx = ValidateCtx::new(&into_round_ctx, &info);
                    let validated = {
                        __guard.end_section(line!());
                        let __result = Verifier::validate(
                            info.clone(),
                            point_dag_round,
                            downloader,
                            store.clone(),
                            cert.clone(),
                            validate_ctx,
                        )
                        .await;
                        __guard.start_section(line!());
                        __result
                    }?;
                    {
                        __guard.end_section(line!());
                        let __result = store_task.await;
                        __guard.start_section(line!());
                        __result
                    }?;
                    let (dag_point, status) =
                        Self::acquire_validated(&state, info, cert, validated, &into_round_ctx);
                    let ctx = into_round_ctx.clone();
                    let store_fn = move || {
                        let _guard = ctx.span().enter();
                        store.set_status(dag_point.round(), dag_point.digest(), status.as_ref());
                        state.resolve(&dag_point);
                        dag_point
                    };
                    let store_task = {
                        __guard.end_section(line!());
                        let __result = LIMIT.spawn_blocking(into_round_ctx.task(), store_fn).await;
                        __guard.start_section(line!());
                        __result
                    };
                    Ok(store_task)
                }
                Some(DownloadResult::IllFormed(point, reason)) => {
                    let mut status = PointStatusIllFormed::default();
                    state.acquire(&point_id, &mut status);
                    let dag_point =
                        DagPoint::new_ill_formed(point.info().id(), cert, &status, reason);
                    let ctx = into_round_ctx.clone();
                    let store_fn = move || {
                        let _guard = ctx.span().enter();
                        let status_ref = PointStatusStoredRef::IllFormed(&status);
                        store.insert_point(&point, status_ref);
                        state.resolve(&dag_point);
                        dag_point
                    };
                    let store_task = {
                        __guard.end_section(line!());
                        let __result = LIMIT.spawn_blocking(into_round_ctx.task(), store_fn).await;
                        __guard.start_section(line!());
                        __result
                    };
                    Ok(store_task)
                }
                None => {
                    let mut status = PointStatusNotFound {
                        is_first_resolved: false,
                        is_certified: false,
                        author: point_id.author,
                    };
                    state.acquire(&point_id, &mut status);
                    let dag_point =
                        DagPoint::new_not_found(point_id.round, &point_id.digest, cert, &status);
                    let ctx = into_round_ctx.clone();
                    let store_fn = move || {
                        let _guard = ctx.span().enter();
                        let status_ref = PointStatusStoredRef::NotFound(&status);
                        store.set_status(point_id.round, &point_id.digest, status_ref);
                        state.resolve(&dag_point);
                        dag_point
                    };
                    let store_task = {
                        __guard.end_section(line!());
                        let __result = LIMIT.spawn_blocking(into_round_ctx.task(), store_fn).await;
                        __guard.start_section(line!());
                        __result
                    };
                    Ok(store_task)
                }
            }
        });
        DagPointFuture(DagPointFutureType::Download {
            task: Shared::new(
                (async move {
                    let mut __guard = crate::__async_profile_guard__::Guard::new(
                        concat!(module_path!(), "::async_block"),
                        file!(),
                        line!(),
                    );
                    {
                        __guard.end_section(line!());
                        let __result = nested.await?.await;
                        __guard.start_section(line!());
                        __result
                    }
                })
                .boxed(),
            ),
            cert: cert_clone,
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
        let cert = Cert::default();
        let cert_clone = cert.clone();
        if let Some((includes, witness)) = match &point_restore {
            PointRestore::Validated(info, _) => Some((info.includes(), info.witness())),
            PointRestore::IllFormed(_, _)
            | PointRestore::Exists(_)
            | PointRestore::NotFound(_, _, _) => None,
        } {
            let mut cert_deps = CertDirectDeps {
                includes: FastHashMap::with_capacity(point_dag_round.peer_count().full()),
                witness: FastHashMap::with_capacity(point_dag_round.peer_count().full()),
            };
            if let Some(r_1) = point_dag_round.prev().upgrade() {
                cert_deps.includes.extend(r_1.select(|(peer, loc)| {
                    includes.get(peer).and_then(|digest| {
                        loc.versions.get(digest).map(|a| (*digest, a.weak_cert()))
                    })
                }));
                if let Some(r_2) = r_1.prev().upgrade() {
                    cert_deps.witness.extend(r_2.select(|(peer, loc)| {
                        witness.get(peer).and_then(|digest| {
                            loc.versions.get(digest).map(|a| (*digest, a.weak_cert()))
                        })
                    }));
                }
            }
            cert.set_deps(cert_deps);
        }
        if match &point_restore {
            PointRestore::Exists(_) => false,
            PointRestore::Validated(_, status) => status.is_certified,
            PointRestore::IllFormed(_, status) => status.is_certified,
            PointRestore::NotFound(_, _, status) => status.is_certified,
        } {
            cert.certify();
        }
        let validate_or_restore = match point_restore {
            PointRestore::Exists(info) => Either::Left(info),
            PointRestore::Validated(info, status) => {
                let dag_point = DagPoint::new_validated(info, cert, &status);
                state.acquire_restore(&dag_point.id(), &status);
                Either::Right(dag_point)
            }
            PointRestore::IllFormed(id, status) => {
                let dag_point =
                    DagPoint::new_ill_formed(id, cert, &status, IllFormedReason::AfterLoadFromDb);
                state.acquire_restore(&dag_point.id(), &status);
                Either::Right(dag_point)
            }
            PointRestore::NotFound(round, digest, status) => {
                let dag_point = DagPoint::new_not_found(round, &digest, cert, &status);
                state.acquire_restore(&dag_point.id(), &status);
                Either::Right(dag_point)
            }
        };
        match validate_or_restore {
            Either::Left(verified) => {
                let point_dag_round = point_dag_round.downgrade();
                let state = state.clone();
                let downloader = downloader.clone();
                let store = store.clone();
                let round_ctx = round_ctx.clone();
                let ctx = round_ctx.clone();
                let cert = cert_clone.clone();
                let future = async move {
                    let mut __guard = crate::__async_profile_guard__::Guard::new(
                        concat!(module_path!(), "::async_block"),
                        file!(),
                        line!(),
                    );
                    let validate_ctx = ValidateCtx::new(&round_ctx, &verified);
                    let validated = {
                        __guard.end_section(line!());
                        let __result = Verifier::validate(
                            verified.clone(),
                            point_dag_round,
                            downloader,
                            store.clone(),
                            cert.clone(),
                            validate_ctx,
                        )
                        .await;
                        __guard.start_section(line!());
                        __result
                    };
                    let (dag_point, status) =
                        Self::acquire_validated(&state, verified, cert, validated?, &round_ctx);
                    let ctx = round_ctx.clone();
                    let store_fn = move || {
                        let _guard = ctx.span().enter();
                        store.set_status(dag_point.round(), dag_point.digest(), status.as_ref());
                        state.resolve(&dag_point);
                        dag_point
                    };
                    let store_task = {
                        __guard.end_section(line!());
                        let __result = LIMIT.spawn_blocking(round_ctx.task(), store_fn).await;
                        __guard.start_section(line!());
                        __result
                    };
                    Ok(store_task)
                };
                let lazy = (async move {
                    let mut __guard = crate::__async_profile_guard__::Guard::new(
                        concat!(module_path!(), "::async_block"),
                        file!(),
                        line!(),
                    );
                    {
                        __guard.end_section(line!());
                        let __result = ctx.task().spawn(future).await?.await;
                        __guard.start_section(line!());
                        __result
                    }
                })
                .boxed();
                DagPointFuture(DagPointFutureType::Validate {
                    task: Shared::new(lazy),
                    cert: cert_clone,
                })
            }
            Either::Right(dag_point) => {
                state.resolve(&dag_point);
                let ready = future::ready(Ok(dag_point)).boxed();
                DagPointFuture(DagPointFutureType::Validate {
                    task: Shared::new(ready),
                    cert: cert_clone,
                })
            }
        }
    }
    fn acquire_validated(
        state: &InclusionState,
        info: PointInfo,
        cert: Cert,
        validated: ValidateResult,
        ctx: &impl Ctx,
    ) -> (DagPoint, PointStatusStored) {
        let _guard = ctx.span().enter();
        let id = info.id();
        let is_valid = matches!(validated, ValidateResult::Valid);
        match validated {
            ValidateResult::Valid | ValidateResult::Invalid(_) => {
                let mut status = PointStatusValidated::default();
                status.is_valid = is_valid;
                status.is_certified = cert.is_certified();
                state.acquire(&id, &mut status);
                (
                    DagPoint::new_validated(info, cert, &status),
                    PointStatusStored::Validated(status),
                )
            }
            ValidateResult::IllFormed(reason) => {
                let mut status = PointStatusIllFormed {
                    is_certified: cert.is_certified(),
                    ..Default::default()
                };
                state.acquire(&id, &mut status);
                (
                    DagPoint::new_ill_formed(id, cert, &status, reason),
                    PointStatusStored::IllFormed(status),
                )
            }
        }
    }
    pub fn add_depender(&self, depender: &PeerId) {
        if let DagPointFutureType::Download { dependers_tx, .. } = &self.0 {
            _ = dependers_tx.send(*depender);
        }
    }
    pub fn resolve_download(&self, broadcast: &Point, ill_formed_reason: Option<&IllFormedReason>) {
        if let DagPointFutureType::Download { resolve, .. } = &self.0
            && let Some(oneshot) = resolve.take()
        {
            let result = match ill_formed_reason {
                None => DownloadResult::Verified(broadcast.clone()),
                Some(reason) => DownloadResult::IllFormed(broadcast.clone(), reason.clone()),
            };
            oneshot.send(result).ok();
        }
    }
    pub fn weak_cert(&self) -> WeakCert {
        match &self.0 {
            DagPointFutureType::Validate { cert, .. }
            | DagPointFutureType::Download { cert, .. } => cert.downgrade(),
        }
    }
    pub fn downgrade(&self) -> WeakDagPointFuture {
        let maybe_consumed = match &self.0 {
            DagPointFutureType::Validate { task, .. }
            | DagPointFutureType::Download { task, .. } => task.weak_future(),
        };
        WeakDagPointFuture(maybe_consumed.expect("shared dag point task cannot be consumed"))
    }
}
pub struct WeakDagPointFuture(WeakShared<BoxFuture<'static, TaskResult<DagPoint>>>);
impl future::Future for WeakDagPointFuture {
    type Output = TaskResult<Option<DagPoint>>;
    #[inline]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.0.poll_unpin(cx) {
            Poll::Ready(Some((Ok(result), _))) => Poll::Ready(Ok(Some(result))),
            Poll::Ready(Some((Err(Cancelled()), _))) => Poll::Ready(Err(Cancelled())),
            Poll::Ready(None) => Poll::Ready(Ok(None)),
            Poll::Pending => Poll::Pending,
        }
    }
}
