use std::pin::Pin;
use std::sync::{Arc, LazyLock};
use std::task::{Context, Poll};

use futures_util::future::{BoxFuture, Either};
use futures_util::{FutureExt, future};
use tokio::sync::{mpsc, oneshot};
use tycho_crypto::ed25519::KeyPair;
use tycho_network::PeerId;
use tycho_util::futures::{Shared, WeakShared};
use tycho_util::sync::OnceTake;

use crate::dag::dag_location::InclusionState;
use crate::dag::{
    DagRound, IllFormedReason, InvalidDependency, InvalidReason, ValidateResult, Verifier,
};
use crate::effects::{
    AltFormat, Cancelled, Ctx, DownloadCtx, RoundCtx, SpawnLimit, Task, TaskResult, ValidateCtx,
};
use crate::engine::NodeConfig;
use crate::intercom::{DownloadResult, Downloader};
use crate::models::point_status::*;
use crate::models::{
    AnchorStageRole, Cert, CertDirectDeps, DagPoint, Digest, Point, PointId, PointInfo,
    PointRestore, WeakCert,
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
            DagPointFutureType::Restore { lazy, .. } => match lazy.poll_unpin(cx) {
                Poll::Ready((result, _)) => Poll::Ready(result),
                Poll::Pending => Poll::Pending,
            },
        }
    }
}

#[derive(Clone)]
enum DagPointFutureType {
    Validate {
        task: Shared<Task<DagPoint>>,
        // normally, if we are among the last nodes to validate some broadcast point,
        // we can receive its proof from author, check its signatures and skip validation of the vertex and its deps
        cert: Cert,
    },
    Download {
        task: Shared<Task<DagPoint>>,
        // this could be a `Notify`, but both sender and receiver must be used only once
        cert: Cert,
        dependers_tx: mpsc::UnboundedSender<PeerId>,
        resolve: Arc<OnceTake<oneshot::Sender<DownloadResult>>>,
    },
    Restore {
        lazy: Shared<BoxFuture<'static, TaskResult<DagPoint>>>,
        cert: Cert,
    },
}

impl DagPointFuture {
    /// locally created points are assumed to be valid, checked prior insertion if needed;
    /// for points of others - there are all other methods
    #[allow(clippy::too_many_arguments)] // TODO arch: make args less granular
    pub fn new_local_valid(
        point_dag_round: &DagRound,
        point: &Point,
        role: Option<AnchorStageRole>,
        key_pair: Option<Arc<KeyPair>>,
        state: &InclusionState,
        downloader: Downloader,
        store: MempoolStore,
        round_ctx: &RoundCtx,
    ) -> Self {
        let point_dag_round = point_dag_round.downgrade();
        let info = point.info().clone();
        let point = point.clone();
        let state = state.clone();
        let validate_ctx = ValidateCtx::new(round_ctx, &info);
        let cert = Cert::default();
        let cert_clone = cert.clone();

        let task = round_ctx.task().spawn(async move {
            let peer_schedule = downloader.peer_schedule();
            if let Err(error) = Verifier::verify(&info, peer_schedule, validate_ctx.conf()) {
                let _span = validate_ctx.span().enter();
                panic!("Failed to verify own point: {error}, {info:?}")
            }

            // out of hot path: assume `Broadcaster` is good in rejection of bad signatures
            // and `Producer` puts signatures for the right `prev_digest` into the point
            let check_evidence_fn = {
                let info = info.clone();
                move || match info.check_evidence() {
                    Ok(()) => {}
                    Err(err) => panic!("bad evidence for own point: {err} {info:?}"),
                }
            };
            let check_evidence_task = validate_ctx.task().spawn_blocking(check_evidence_fn);

            let store_fn = {
                let store = store.clone();
                let status = PointStatusStored::Found(Default::default());
                move || store.insert_point(&point, &status)
            };
            let store_task = validate_ctx.task().spawn_blocking(store_fn);

            // all dependencies are resolved so expect the validation to finish earlier than store
            let validated = Verifier::validate(
                info.clone(),
                point_dag_round,
                downloader,
                store.clone(),
                cert.clone(),
                validate_ctx.clone(),
            )
            .await?;

            let span_guard = validate_ctx.span().enter();

            match &validated {
                ValidateResult::Valid => {}
                ValidateResult::TransInvalid(reason) => {
                    panic!("trans Invalid own point: {reason:?} {info:?}")
                }
                ValidateResult::Invalid(reason) => {
                    panic!("Invalid own point: {reason} {info:?}")
                }
                ValidateResult::IllFormed(reason) => {
                    panic!("Ill-formed own point: {reason} {info:?}")
                }
            }

            let (dag_point, status) = Self::acquire_validated(&state, info, role, cert, validated);

            match &status {
                PointStatusStored::Valid(status) if status.is_first_valid => {}
                _ => panic!("local point must be first valid, got {status}"),
            }

            // nobody knows our point yet, so we can afford to resolve state before store is joined
            state.resolve(&dag_point);

            let signed = state.sign(dag_point.round(), key_pair.as_deref(), validate_ctx.conf());
            if !signed.as_ref().is_some_and(|sig| sig.is_ok()) {
                panic!(
                    "Coding or configuration error: local point cannot be signed; got {:?}",
                    signed.as_ref().map(|r| r.as_ref().map(|s| s.alt()))
                );
            }

            // We store only found status for our point and rely on `Engine` to repeat validation
            // so that second status store is removed from hot path
            drop(span_guard);
            futures_util::try_join!(check_evidence_task, store_task)?;

            Ok(dag_point)
        });

        Self(DagPointFutureType::Validate {
            task: Shared::new(task),
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

        let task = task_ctx.spawn(async move {
            let ctx = round_ctx.clone();
            let full_fn = move || {
                let _span = round_ctx.span().enter();
                let id = point.info().id();

                let mut status = PointStatusIllFormed {
                    is_first_resolved: false,
                    has_proof: false,
                    is_reason_final: reason.is_final(),
                };
                state.acquire(id, &mut status); // only after persisted

                let dag_point = DagPoint::new_ill_formed(*id, cert, &status, reason);
                store.insert_point(&point, &PointStatusStored::IllFormed(status));

                state.resolve(&dag_point);
                dag_point
            };
            LIMIT.spawn_blocking(ctx.task(), full_fn).await.await
        });

        Self(DagPointFutureType::Validate {
            task: Shared::new(task),
            cert: cert_clone,
        })
    }

    pub fn new_broadcast(
        point_dag_round: &DagRound,
        point: &Point,
        role: Option<AnchorStageRole>,
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

        let task = round_ctx.task().spawn(async move {
            let store_fn = {
                let store = store.clone();
                let status = PointStatusStored::Found(Default::default());
                move || store.insert_point(&point, &status)
            };
            let store_task = LIMIT.spawn_blocking(validate_ctx.task(), store_fn).await;

            let validated = Verifier::validate(
                info.clone(),
                point_dag_round,
                downloader,
                store.clone(),
                cert.clone(),
                validate_ctx.clone(),
            )
            .await?;

            store_task.await?;

            let (dag_point, status) = (validate_ctx.span())
                .in_scope(|| Self::acquire_validated(&state, info, role, cert, validated));

            let store_fn = move || {
                store.set_status(&dag_point.key(), &status, dag_point.prev_digest());
                state.resolve(&dag_point);
                dag_point
            };
            let nested = LIMIT.spawn_blocking(validate_ctx.task(), store_fn);
            nested.await.await
        });

        DagPointFuture(DagPointFutureType::Validate {
            task: Shared::new(task),
            cert: cert_clone,
        })
    }

    #[allow(clippy::too_many_arguments)] // TODO arch: make args less granular
    pub fn new_download<T>(
        point_dag_round: &DagRound,
        author: &PeerId,
        digest: &Digest,
        role: Option<AnchorStageRole>,
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

        let task = task_ctx.spawn(async move {
            let download_ctx = DownloadCtx::new(&into_round_ctx, &point_id);
            let store = store.clone();
            let downloaded = downloader
                .run(&point_id, dependers_rx, broadcast_rx, download_ctx)
                .await?;
            match downloaded {
                DownloadResult::Verified(point) => {
                    let info = point.info().clone();

                    let store_fn = {
                        let store = store.clone();
                        let status = PointStatusStored::Found(Default::default());
                        move || store.insert_point(&point, &status)
                    };
                    let store_task = LIMIT.spawn_blocking(into_round_ctx.task(), store_fn).await;

                    let validate_ctx = ValidateCtx::new(&into_round_ctx, &info);
                    let validated = Verifier::validate(
                        info.clone(),
                        point_dag_round,
                        downloader,
                        store.clone(),
                        cert.clone(),
                        validate_ctx,
                    )
                    .await?;

                    store_task.await?;

                    let ctx = into_round_ctx.clone();

                    let (dag_point, status) = (ctx.span())
                        .in_scope(|| Self::acquire_validated(&state, info, role, cert, validated));

                    let store_fn = move || {
                        let _guard = ctx.span().enter();
                        store.set_status(&dag_point.key(), &status, dag_point.prev_digest());
                        state.resolve(&dag_point);
                        dag_point
                    };
                    let nested = LIMIT.spawn_blocking(into_round_ctx.task(), store_fn);
                    nested.await.await
                }
                DownloadResult::IllFormed(point, reason) => {
                    let mut status = PointStatusIllFormed {
                        is_first_resolved: false,
                        has_proof: cert.has_proof(),
                        is_reason_final: reason.is_final(),
                    };
                    state.acquire(&point_id, &mut status);
                    let dag_point = DagPoint::new_ill_formed(point_id, cert, &status, reason);
                    let ctx = into_round_ctx.clone();

                    let store_fn = move || {
                        let _guard = ctx.span().enter();
                        store.insert_point(&point, &PointStatusStored::IllFormed(status));
                        state.resolve(&dag_point);
                        dag_point
                    };
                    let nested = LIMIT.spawn_blocking(into_round_ctx.task(), store_fn);
                    nested.await.await
                }
                DownloadResult::NotFound => {
                    let mut status = PointStatusNotFound {
                        is_first_resolved: false,
                        has_proof: false,
                        author: point_id.author,
                    };
                    state.acquire(&point_id, &mut status);
                    let dag_point = DagPoint::new_not_found(point_id, cert, &status);
                    let ctx = into_round_ctx.clone();

                    let store_fn = move || {
                        let _guard = ctx.span().enter();
                        let status = PointStatusStored::NotFound(status);
                        store.set_status(&point_id.key(), &status, dag_point.prev_digest());
                        state.resolve(&dag_point);
                        dag_point
                    };
                    let nested = LIMIT.spawn_blocking(into_round_ctx.task(), store_fn);
                    nested.await.await
                }
            }
        });

        DagPointFuture(DagPointFutureType::Download {
            task: Shared::new(task),
            cert: cert_clone,
            dependers_tx,
            resolve: Arc::new(OnceTake::new(broadcast_tx)),
        })
    }

    pub fn new_restore(
        point_dag_round: &DagRound,
        point_restore: PointRestore,
        role: Option<AnchorStageRole>,
        state: &InclusionState,
        downloader: &Downloader,
        store: &MempoolStore,
        round_ctx: &RoundCtx,
    ) -> Self {
        let cert = Cert::default();
        let cert_clone = cert.clone();

        if let Some(info) = match &point_restore {
            PointRestore::Valid(info, _)
            | PointRestore::TransInvalid(info, _)
            | PointRestore::Invalid(info, _) => Some(info),
            // deps for `Found` are set in `validate()`
            PointRestore::IllFormed(_, _)
            | PointRestore::NotFound(_, _)
            | PointRestore::Found(_, _) => None,
        } {
            cert.set_deps(Self::gather_cert_deps(point_dag_round, info));
        }
        if point_restore.has_proof() {
            cert.certify(round_ctx.conf());
        }

        // ignore committed status flag to repeat commit after reboot

        // keep this section sync so that call site may not wait for each result to resolve
        let validate_or_restore = match point_restore {
            PointRestore::Valid(info, mut status) => {
                state.acquire_restore(info.id(), &mut status);
                let dag_point = DagPoint::new_valid(info, cert, &status);
                Either::Right(dag_point)
            }
            PointRestore::TransInvalid(info, mut status) => {
                let root_cause = InvalidDependency {
                    link: status.root_cause.clone(),
                    reason: InvalidReason::AfterLoadFromDb {
                        has_dag_round: status.has_dag_round,
                    },
                };
                state.acquire_restore(info.id(), &mut status);
                let dag_point = DagPoint::new_trans_invalid(info, cert, &status, root_cause);
                Either::Right(dag_point)
            }
            PointRestore::Invalid(info, mut status) => {
                let reason = InvalidReason::AfterLoadFromDb {
                    has_dag_round: status.has_dag_round,
                };
                state.acquire_restore(info.id(), &mut status);
                let dag_point = DagPoint::new_invalid(info, cert, &status, reason);
                Either::Right(dag_point)
            }
            PointRestore::IllFormed(id, mut status) => {
                let reason = IllFormedReason::AfterLoadFromDb {
                    is_final: status.is_reason_final,
                };
                state.acquire_restore(&id, &mut status);
                let dag_point = DagPoint::new_ill_formed(id, cert, &status, reason);
                Either::Right(dag_point)
            }
            PointRestore::NotFound(key, mut status) => {
                let id = PointId {
                    author: status.author,
                    round: key.round,
                    digest: key.digest,
                };
                state.acquire_restore(&id, &mut status);
                let dag_point = DagPoint::new_not_found(id, cert, &status);
                Either::Right(dag_point)
            }
            PointRestore::Found(info, _) => Either::Left(info),
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
                    let validate_ctx = ValidateCtx::new(&round_ctx, &verified);
                    let validated = Verifier::validate(
                        verified.clone(),
                        point_dag_round,
                        downloader,
                        store.clone(),
                        cert.clone(),
                        validate_ctx,
                    )
                    .await?;
                    let (dag_point, status) = round_ctx.span().in_scope(|| {
                        Self::acquire_validated(&state, verified, role, cert, validated)
                    });
                    let ctx = round_ctx.clone();

                    let store_fn = move || {
                        let _guard = ctx.span().enter();
                        store.set_status(&dag_point.key(), &status, dag_point.prev_digest());
                        state.resolve(&dag_point);
                        dag_point
                    };
                    let nested = LIMIT.spawn_blocking(round_ctx.task(), store_fn);
                    nested.await.await
                };
                let lazy = Box::pin(async move { ctx.task().spawn(future).await });
                DagPointFuture(DagPointFutureType::Restore {
                    lazy: Shared::new(lazy),
                    cert: cert_clone,
                })
            }
            Either::Right(dag_point) => {
                state.resolve(&dag_point);
                let ready = Box::pin(future::ready(Ok(dag_point)));
                DagPointFuture(DagPointFutureType::Restore {
                    lazy: Shared::new(ready),
                    cert: cert_clone,
                })
            }
        }
    }

    fn acquire_validated(
        state: &InclusionState,
        info: PointInfo,
        role: Option<AnchorStageRole>,
        cert: Cert,
        validated: ValidateResult,
    ) -> (DagPoint, PointStatusStored) {
        let id = info.id();
        match validated {
            ValidateResult::Valid => {
                let mut status = Self::new_valid_status(role, &cert);
                state.acquire(id, &mut status);
                (
                    DagPoint::new_valid(info, cert, &status),
                    PointStatusStored::Valid(status),
                )
            }
            ValidateResult::TransInvalid(inv_dep) => {
                let mut status = Self::new_trans_invalid_status(role, &cert, &inv_dep);
                state.acquire(id, &mut status);
                (
                    DagPoint::new_trans_invalid(info, cert, &status, inv_dep),
                    PointStatusStored::TransInvalid(status),
                )
            }
            ValidateResult::Invalid(reason) => {
                let mut status = PointStatusInvalid {
                    is_first_resolved: false,
                    has_proof: cert.has_proof(),
                    has_dag_round: reason.has_dag_round(),
                };
                state.acquire(id, &mut status);
                (
                    DagPoint::new_invalid(info, cert, &status, reason),
                    PointStatusStored::Invalid(status),
                )
            }
            ValidateResult::IllFormed(reason) => {
                let mut status = PointStatusIllFormed {
                    is_first_resolved: false,
                    has_proof: cert.has_proof(),
                    is_reason_final: reason.is_final(),
                };
                state.acquire(id, &mut status);
                (
                    DagPoint::new_ill_formed(*id, cert, &status, reason),
                    PointStatusStored::IllFormed(status),
                )
            }
        }
    }

    fn new_valid_status(role: Option<AnchorStageRole>, cert: &Cert) -> PointStatusValid {
        PointStatusValid {
            has_proof: cert.has_proof(),
            anchor_flags: match role {
                None => AnchorFlags::empty(),
                Some(AnchorStageRole::Proof) => AnchorFlags::Proof,
                Some(AnchorStageRole::Trigger) => AnchorFlags::Trigger,
            },
            ..Default::default()
        }
    }

    fn new_trans_invalid_status(
        role: Option<AnchorStageRole>,
        cert: &Cert,
        inv_dep: &InvalidDependency,
    ) -> PointStatusTransInvalid {
        PointStatusTransInvalid {
            is_first_resolved: false,
            has_proof: cert.has_proof(),
            has_dag_round: inv_dep.reason.has_dag_round(),
            anchor_flags: match role {
                None => AnchorFlags::empty(),
                Some(AnchorStageRole::Proof) => AnchorFlags::Proof,
                Some(AnchorStageRole::Trigger) => AnchorFlags::Trigger,
            },
            committed: None,
            root_cause: inv_dep.link.clone(),
            is_restored: false,
        }
    }

    fn gather_cert_deps(point_dag_round: &DagRound, info: &PointInfo) -> CertDirectDeps {
        let mut cert_deps = CertDirectDeps {
            includes: Vec::with_capacity(info.includes().len()),
            witness: Vec::with_capacity(info.witness().len()),
        };
        if let Some(r_1) = point_dag_round.prev().upgrade() {
            cert_deps.includes.extend(r_1.select(|(peer, loc)| {
                (info.includes().get(peer))
                    .and_then(|digest| loc.versions.get(digest).map(|a| (*digest, a.weak_cert())))
            }));
            if let Some(r_2) = r_1.prev().upgrade() {
                cert_deps.witness.extend(r_2.select(|(peer, loc)| {
                    info.witness().get(peer).and_then(|digest| {
                        loc.versions.get(digest).map(|a| (*digest, a.weak_cert()))
                    })
                }));
            }
        }
        cert_deps
    }

    pub fn add_depender(&self, depender: &PeerId) {
        if let DagPointFutureType::Download { dependers_tx, .. } = &self.0 {
            // receiver is dropped upon completion
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
            // receiver is dropped upon completion
            oneshot.send(result).ok();
        }
    }

    pub fn weak_cert(&self) -> WeakCert {
        match &self.0 {
            DagPointFutureType::Validate { cert, .. }
            | DagPointFutureType::Download { cert, .. }
            | DagPointFutureType::Restore { cert, .. } => cert.downgrade(),
        }
    }

    pub fn downgrade(&self) -> WeakDagPointFuture {
        WeakDagPointFuture(match &self.0 {
            DagPointFutureType::Validate { task, .. }
            | DagPointFutureType::Download { task, .. } => {
                let weak = task.weak_future().expect("must not be consumed");
                WeakDagPointFutureInner::Task(weak)
            }
            DagPointFutureType::Restore { lazy, .. } => {
                let weak = lazy.weak_future().expect("must not be consumed");
                WeakDagPointFutureInner::Lazy(weak)
            }
        })
    }
}

pub struct WeakDagPointFuture(WeakDagPointFutureInner);

enum WeakDagPointFutureInner {
    Task(WeakShared<Task<DagPoint>>),
    Lazy(WeakShared<BoxFuture<'static, TaskResult<DagPoint>>>),
}

impl future::Future for WeakDagPointFuture {
    type Output = TaskResult<Option<DagPoint>>;

    #[inline]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let polled = match &mut self.0 {
            WeakDagPointFutureInner::Task(task) => task.poll_unpin(cx),
            WeakDagPointFutureInner::Lazy(lazy) => lazy.poll_unpin(cx),
        };
        match polled {
            Poll::Ready(Some((Ok(result), _))) => Poll::Ready(Ok(Some(result))),
            Poll::Ready(Some((Err(Cancelled()), _))) => Poll::Ready(Err(Cancelled())),
            Poll::Ready(None) => Poll::Ready(Ok(None)),
            Poll::Pending => Poll::Pending,
        }
    }
}
