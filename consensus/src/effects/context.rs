use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use tracing::Span;

use crate::effects::task::TaskTracker;
use crate::effects::{AltFormat, TaskCtx};
use crate::engine::MempoolConfig;
use crate::models::{Point, PointId, PointInfo, Round};

/// All side effects are scoped to their context, that often (but not always) equals to module.
pub trait Ctx {
    fn span(&self) -> &Span;
    fn conf(&self) -> &MempoolConfig;
    fn task(&self) -> TaskCtx<'_>;
}

/// Root context for uninterrupted sequence of engine rounds
pub struct EngineCtx(Arc<EngineCtxInner>);
#[derive(Clone)]
pub struct EngineCtxInner {
    span: Span,
    conf: MempoolConfig,
    task_tracker: TaskTracker,
}
impl Ctx for EngineCtx {
    fn span(&self) -> &Span {
        &self.0.span
    }
    fn conf(&self) -> &MempoolConfig {
        &self.0.conf
    }
    fn task(&self) -> TaskCtx<'_> {
        self.0.task_tracker.ctx()
    }
}
impl EngineCtx {
    pub fn new(since: Round, conf: &MempoolConfig, task_tracker: &TaskTracker) -> Self {
        Self(Arc::new(EngineCtxInner {
            span: tracing::error_span!("rounds", "since" = since.0),
            conf: conf.clone(),
            task_tracker: task_tracker.clone(),
        }))
    }
    pub fn update(&mut self, since: Round) {
        Arc::make_mut(&mut self.0).span = tracing::error_span!("rounds", "since" = since.0);
    }
    pub fn meter_dag_len(len: usize) {
        metrics::gauge!("tycho_mempool_rounds_dag_length").set(len as u32);
    }
}

#[derive(Clone)]
pub struct RoundCtx(Arc<RoundCtxInner>);
struct RoundCtxInner {
    span: Span,
    conf: MempoolConfig,
    task_tracker: TaskTracker,
    current_round: Round,
    download_max_depth: AtomicU32,
}
impl Ctx for RoundCtx {
    fn span(&self) -> &Span {
        &self.0.span
    }
    fn conf(&self) -> &MempoolConfig {
        &self.0.conf
    }
    fn task(&self) -> TaskCtx<'_> {
        self.0.task_tracker.ctx()
    }
}
impl RoundCtx {
    pub fn new(parent: &EngineCtx, current_round: Round) -> Self {
        Self(Arc::new(RoundCtxInner {
            span: parent
                .span()
                .in_scope(|| tracing::error_span!("round", "current" = current_round.0)),
            conf: parent.conf().clone(),
            task_tracker: parent.0.task_tracker.clone(),
            current_round,
            download_max_depth: Default::default(),
        }))
    }
    pub fn depth(&self, round: Round) -> f64 {
        self.0.current_round.diff_f64(round)
    }
}

pub struct CollectCtx {
    span: Span,
    parent: RoundCtx,
}
impl Ctx for CollectCtx {
    fn span(&self) -> &Span {
        &self.span
    }
    fn conf(&self) -> &MempoolConfig {
        self.parent.conf()
    }
    fn task(&self) -> TaskCtx<'_> {
        self.parent.task()
    }
}
impl CollectCtx {
    pub fn new(parent: &RoundCtx) -> Self {
        Self {
            span: parent.span().in_scope(|| tracing::error_span!("collect")),
            parent: parent.clone(),
        }
    }
}

pub struct BroadcastCtx {
    span: Span,
    parent: RoundCtx,
}
impl Ctx for BroadcastCtx {
    fn span(&self) -> &Span {
        &self.span
    }
    fn conf(&self) -> &MempoolConfig {
        self.parent.conf()
    }
    fn task(&self) -> TaskCtx<'_> {
        self.parent.task()
    }
}
impl BroadcastCtx {
    pub fn new(parent: &RoundCtx, point: &Point) -> Self {
        Self {
            span: parent.span().in_scope(|| {
                tracing::error_span!(
                    "broadcast",
                    round = point.round().0,
                    digest = display(point.digest().alt())
                )
            }),
            parent: parent.clone(),
        }
    }
}

pub struct DownloadCtx {
    span: Span,
    parent: RoundCtx,
}
impl Ctx for DownloadCtx {
    fn span(&self) -> &Span {
        &self.span
    }
    fn conf(&self) -> &MempoolConfig {
        self.parent.conf()
    }
    fn task(&self) -> TaskCtx<'_> {
        self.parent.task()
    }
}
impl DownloadCtx {
    pub fn new<'a, T>(parent: &'a T, point_id: &PointId) -> Self
    where
        T: Ctx,
        &'a T: Into<RoundCtx>,
    {
        Self {
            parent: parent.into(),
            span: parent.span().in_scope(|| {
                tracing::error_span!(
                    "download",
                    author = display(point_id.author.alt()),
                    round = point_id.round.0,
                    digest = display(point_id.digest.alt()),
                )
            }),
        }
    }
    // per round
    pub fn download_max_depth(&self, round: Round) -> u32 {
        let parent = &self.parent.0;
        let depth = (parent.current_round - round.0).0;
        let old = parent
            .download_max_depth
            .fetch_max(depth, Ordering::Relaxed);
        depth.max(old)
    }
}

impl From<&RoundCtx> for RoundCtx {
    fn from(ctx: &RoundCtx) -> Self {
        ctx.clone()
    }
}
impl From<&ValidateCtx> for RoundCtx {
    fn from(ctx: &ValidateCtx) -> Self {
        ctx.0.parent.clone()
    }
}

#[derive(Clone)]
pub struct ValidateCtx(Arc<ValidateCtxInner>);
struct ValidateCtxInner {
    span: Span,
    parent: RoundCtx,
}
impl Ctx for ValidateCtx {
    fn span(&self) -> &Span {
        &self.0.span
    }
    fn conf(&self) -> &MempoolConfig {
        self.0.parent.conf()
    }
    fn task(&self) -> TaskCtx<'_> {
        self.0.parent.task()
    }
}
impl ValidateCtx {
    pub fn new<'a, T>(parent: &'a T, info: &PointInfo) -> Self
    where
        T: Ctx,
        &'a T: Into<RoundCtx>,
    {
        let round_ctx = parent.into();
        let span = round_ctx.span().in_scope(|| {
            tracing::error_span!(
                "validate",
                author = display(info.data().author.alt()),
                round = info.round().0,
                digest = display(info.digest().alt()),
            )
        });
        Self(Arc::new(ValidateCtxInner {
            parent: round_ctx,
            span,
        }))
    }
}
