use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use tracing::Span;

use crate::effects::AltFormat;
use crate::models::{Digest, PointId, PointInfo, Round};

/// All side effects are scoped to their context, that often (but not always) equals to module.
pub trait Ctx {
    fn span(&self) -> &Span;
}

/// Root context for uninterrupted sequence of engine rounds
pub struct EngineCtx {
    span: Span,
}
impl Ctx for EngineCtx {
    fn span(&self) -> &Span {
        &self.span
    }
}
impl EngineCtx {
    pub fn new(since: Round) -> Self {
        Self {
            span: tracing::error_span!("rounds", "since" = since.0),
        }
    }
}

#[derive(Clone)]
pub struct RoundCtx(Arc<RoundCtxInner>);
struct RoundCtxInner {
    current_round: Round,
    download_max_depth: AtomicU32,
    span: Span,
}
impl Ctx for RoundCtx {
    fn span(&self) -> &Span {
        &self.0.span
    }
}
impl RoundCtx {
    pub fn new(parent: &EngineCtx, current_round: Round) -> Self {
        Self(Arc::new(RoundCtxInner {
            current_round,
            download_max_depth: Default::default(),
            span: parent
                .span()
                .in_scope(|| tracing::error_span!("round", "current" = current_round.0)),
        }))
    }
    pub fn depth(&self, round: Round) -> u32 {
        (self.0.current_round - round.0).0
    }
}

pub struct CollectCtx {
    span: Span,
}
impl Ctx for CollectCtx {
    fn span(&self) -> &Span {
        &self.span
    }
}
impl CollectCtx {
    pub fn new(parent: &RoundCtx) -> Self {
        Self {
            span: parent.span().in_scope(|| tracing::error_span!("collect")),
        }
    }
}

pub struct BroadcastCtx {
    span: Span,
}
impl Ctx for BroadcastCtx {
    fn span(&self) -> &Span {
        &self.span
    }
}
impl BroadcastCtx {
    pub fn new(parent: &RoundCtx, digest: &Digest) -> Self {
        Self {
            span: parent
                .span()
                .in_scope(|| tracing::error_span!("broadcast", digest = display(digest.alt()))),
        }
    }
}

pub struct DownloadCtx {
    parent: RoundCtx,
    span: Span,
}
impl Ctx for DownloadCtx {
    fn span(&self) -> &Span {
        &self.span
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
    parent: RoundCtx,
    span: Span,
}
impl Ctx for ValidateCtx {
    fn span(&self) -> &Span {
        &self.0.span
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
