use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use ahash::HashMapExt;
use everscale_crypto::ed25519::KeyPair;
use futures_util::future::{BoxFuture, Either};
use futures_util::{future, FutureExt};
use tokio::sync::{mpsc, oneshot};
use tycho_network::PeerId;
use tycho_util::futures::Shared;
use tycho_util::sync::OnceTake;
use tycho_util::FastHashMap;

use crate::dag::dag_location::InclusionState;
use crate::dag::{DagRound, IllFormedReason, ValidateResult, Verifier};
use crate::effects::{
    AltFormat, Ctx, DownloadCtx, MempoolStore, RoundCtx, TaskResult, ValidateCtx,
};
use crate::intercom::{DownloadResult, Downloader};
use crate::models::{
    Cert, CertDirectDeps, DagPoint, Digest, Point, PointId, PointInfo, PointRestore,
    PointStatusIllFormed, PointStatusNotFound, PointStatusStored, PointStatusStoredRef,
    PointStatusValidated, WeakCert,
};

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
        // normally, if we are among the last nodes to validate some broadcast point,
        // we can receive its proof from author, check its signatures and skip validation of the vertex and its deps
        cert: Cert,
    },
    Download {
        task: Shared<BoxFuture<'static, TaskResult<DagPoint>>>,
        // this could be a `Notify`, but both sender and receiver must be used only once
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
            let ctx = round_ctx.clone();
            let full_fn = move || {
                let _span = round_ctx.span().enter();

                let mut status = PointStatusValidated::default();
                status.is_valid = true;
                state.acquire(&point.id(), &mut status); // only after persisted

                assert!(
                    status.is_first_valid,
                    "local point must be first valid, got {status}"
                );

                let dag_point = DagPoint::new_validated(PointInfo::from(&point), cert, &status);

                store.insert_point(&point, PointStatusStoredRef::Validated(&status));
                state.resolve(&dag_point);

                let signed = state.sign(point.round(), key_pair.as_deref(), round_ctx.conf());
                assert!(
                    signed.as_ref().is_some_and(|sig| sig.is_ok()),
                    "Coding or configuration error: local point cannot be signed; got {:?}",
                    signed.as_ref().map(|r| r.as_ref().map(|s| s.alt()))
                );
                dag_point
            };
            ctx.task().spawn_blocking_limited(full_fn).await
        });

        Self(DagPointFutureType::Validate {
            task: Shared::new((async move { nested.await?.await }).boxed()),
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
            let ctx = round_ctx.clone();
            let full_fn = move || {
                let _span = round_ctx.span().enter();
                let id = point.id();

                let mut status = PointStatusIllFormed::default();
                state.acquire(&id, &mut status); // only after persisted

                let dag_point = DagPoint::new_ill_formed(id, cert, &status, reason);
                store.insert_point(&point, PointStatusStoredRef::IllFormed(&status));

                state.resolve(&dag_point);
                dag_point
            };
            ctx.task().spawn_blocking_limited(full_fn).await
        });

        Self(DagPointFutureType::Validate {
            task: Shared::new((async move { nested.await?.await }).boxed()),
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
        let info = PointInfo::from(point);
        let point = point.clone();
        let state = state.clone();
        let downloader = downloader.clone();
        let store = store.clone();
        let validate_ctx = ValidateCtx::new(round_ctx, &info);
        let cert = Cert::default();
        let cert_clone = cert.clone();

        let nested = round_ctx.task().spawn(async move {
            let point_id = point.id();
            let prev_proof = point.prev_proof();

            let store_fn = {
                let store = store.clone();
                let status_ref = PointStatusStoredRef::Exists;
                move || store.insert_point(&point, status_ref)
            };
            let store_task = validate_ctx.task().spawn_blocking_limited(store_fn).await;

            let validated = Verifier::validate(
                info.clone(),
                prev_proof,
                point_dag_round,
                downloader,
                store.clone(),
                cert.clone(),
                validate_ctx.clone(),
            )
            .await?;

            store_task.await?;

            let (dag_point, status) =
                Self::acquire_validated(&state, info, cert, validated, &validate_ctx);

            let store_fn = move || {
                store.set_status(point_id.round, &point_id.digest, status.as_ref());
                state.resolve(&dag_point);
                dag_point
            };
            let store_task = validate_ctx.task().spawn_blocking_limited(store_fn).await;

            Ok(store_task)
        });

        DagPointFuture(DagPointFutureType::Validate {
            task: Shared::new((async move { nested.await??.await }).boxed()),
            cert: cert_clone,
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
        let cert = Cert::default();
        let cert_clone = cert.clone();

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

                    let store_fn = {
                        let store = store.clone();
                        let status_ref = PointStatusStoredRef::Exists;
                        move || store.insert_point(&point, status_ref)
                    };
                    let store_task = into_round_ctx.task().spawn_blocking_limited(store_fn).await;

                    let validate_ctx = ValidateCtx::new(&into_round_ctx, &info);
                    let validated = Verifier::validate(
                        info.clone(),
                        prev_proof,
                        point_dag_round,
                        downloader,
                        store.clone(),
                        cert.clone(),
                        validate_ctx,
                    )
                    .await?;

                    store_task.await?;

                    let (dag_point, status) =
                        Self::acquire_validated(&state, info, cert, validated, &into_round_ctx);
                    let ctx = into_round_ctx.clone();

                    let store_fn = move || {
                        let _guard = ctx.span().enter();
                        store.set_status(dag_point.round(), dag_point.digest(), status.as_ref());
                        state.resolve(&dag_point);
                        dag_point
                    };
                    let store_task = into_round_ctx.task().spawn_blocking_limited(store_fn).await;

                    Ok(store_task)
                }
                Some(DownloadResult::IllFormed(point, reason)) => {
                    let mut status = PointStatusIllFormed::default();
                    state.acquire(&point_id, &mut status);
                    let dag_point = DagPoint::new_ill_formed(point.id(), cert, &status, reason);
                    let ctx = into_round_ctx.clone();

                    let store_fn = move || {
                        let _guard = ctx.span().enter();
                        let status_ref = PointStatusStoredRef::IllFormed(&status);
                        store.insert_point(&point, status_ref);
                        state.resolve(&dag_point);
                        dag_point
                    };
                    let store_task = into_round_ctx.task().spawn_blocking_limited(store_fn).await;

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
                    let store_task = into_round_ctx.task().spawn_blocking_limited(store_fn).await;

                    Ok(store_task)
                }
            }
        });

        DagPointFuture(DagPointFutureType::Download {
            task: Shared::new((async move { nested.await??.await }).boxed()),
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
            PointRestore::Validated(info, _) => Some((&info.data().includes, &info.data().witness)),
            PointRestore::IllFormed(_, _) // will not certify its dependencies, can be certified
            | PointRestore::Exists(_, _) | PointRestore::NotFound(_, _, _) => None,
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
            PointRestore::Exists(_, _) => false,
            PointRestore::Validated(_, status) => status.is_certified,
            PointRestore::IllFormed(_, status) => status.is_certified,
            PointRestore::NotFound(_, _, status) => status.is_certified,
        } {
            cert.certify();
        }

        // keep this section sync so that call site may not wait for each result to resolve
        let validate_or_restore = match point_restore {
            PointRestore::Exists(info, prev_proof) => Either::Left((info, prev_proof)),
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
            Either::Left((verified, prev_proof)) => {
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
                        prev_proof,
                        point_dag_round,
                        downloader,
                        store.clone(),
                        cert.clone(),
                        validate_ctx,
                    )
                    .await;
                    let (dag_point, status) =
                        Self::acquire_validated(&state, verified, cert, validated?, &round_ctx);
                    let ctx = round_ctx.clone();

                    let store_fn = move || {
                        let _guard = ctx.span().enter();
                        store.set_status(dag_point.round(), dag_point.digest(), status.as_ref());
                        state.resolve(&dag_point);
                        dag_point
                    };
                    let store_task = round_ctx.task().spawn_blocking_limited(store_fn).await;

                    Ok(store_task)
                };
                let lazy = (async move { ctx.task().spawn(future).await??.await }).boxed();
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
        // TODO fill anchor flags in status
        match validated {
            ValidateResult::Valid | ValidateResult::Invalid => {
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

    pub fn weak_cert(&self) -> WeakCert {
        match &self.0 {
            DagPointFutureType::Validate { cert, .. }
            | DagPointFutureType::Download { cert, .. } => cert.downgrade(),
        }
    }
}
