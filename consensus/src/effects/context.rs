use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use tracing::Span;

use crate::effects::AltFormat;
use crate::models::{Digest, Point, PointId, Round};

/// All side effects are scoped to their context, that often (but not always) equals to module.
pub trait EffectsContext {}

#[derive(Clone)]
pub struct Effects<CTX: EffectsContext> {
    /// generally of `tracing::level::Error` as always visible
    span: Span,
    /// Context fields must be private to not affect application logic.
    context: CTX,
}

impl<CTX: EffectsContext> Effects<CTX> {
    /// forbids to create new effects outside the span tree
    fn new_child<U: EffectsContext, F>(&self, context: U, to_span: F) -> Effects<U>
    where
        F: FnOnce() -> Span,
    {
        Effects::<U> {
            span: self.span.in_scope(to_span),
            context,
        }
    }
    fn ctx(&self) -> &CTX {
        &self.context
    }
    pub fn span(&self) -> &Span {
        &self.span
    }
}

/// Root context for uninterrupted sequence of engine rounds
pub struct ChainedRoundsContext;
impl EffectsContext for ChainedRoundsContext {}
impl Effects<ChainedRoundsContext> {
    pub fn new(since: Round) -> Self {
        Self {
            span: tracing::error_span!("rounds", "since" = since.0),
            context: ChainedRoundsContext,
        }
    }
}

#[derive(Clone)]
pub struct EngineContext {
    current_round: Round,
    download_max_depth: Arc<AtomicU32>,
}
impl EffectsContext for EngineContext {}
impl Effects<EngineContext> {
    pub fn new(parent: &Effects<ChainedRoundsContext>, current_round: Round) -> Self {
        let new_context = EngineContext {
            current_round,
            download_max_depth: Default::default(),
        };
        parent.new_child(new_context, || {
            tracing::error_span!("round", "current" = current_round.0)
        })
    }
    pub fn depth(&self, round: Round) -> u32 {
        self.context.current_round.0.saturating_sub(round.0)
    }
}

pub struct CollectorContext;
impl EffectsContext for CollectorContext {}

impl Effects<CollectorContext> {
    pub fn new(parent: &Effects<EngineContext>) -> Self {
        parent.new_child(CollectorContext, || tracing::error_span!("collector"))
    }
}

pub struct BroadcasterContext;
impl EffectsContext for BroadcasterContext {}

impl Effects<BroadcasterContext> {
    pub fn new(parent: &Effects<EngineContext>, digest: &Digest) -> Self {
        parent.new_child(BroadcasterContext, || {
            tracing::error_span!("broadcaster", digest = display(digest.alt()))
        })
    }
}

pub struct DownloadContext {
    current_round: Round,
    download_max_depth: Arc<AtomicU32>,
}
impl EffectsContext for DownloadContext {}
impl Effects<DownloadContext> {
    pub fn new(parent: &Effects<ValidateContext>, point_id: &PointId) -> Self {
        parent.new_child(parent.ctx().into(), || {
            tracing::error_span!(
                "download",
                author = display(point_id.author.alt()),
                round = point_id.round.0,
                digest = display(point_id.digest.alt()),
            )
        })
    }
    // per round
    pub fn download_max_depth(&self, round: Round) -> u32 {
        let depth = self.context.current_round.0.saturating_sub(round.0);
        let old = self
            .context
            .download_max_depth
            .fetch_max(depth, Ordering::Relaxed);
        depth.max(old)
    }
}

impl From<&ValidateContext> for DownloadContext {
    fn from(parent: &ValidateContext) -> Self {
        Self {
            current_round: parent.parent_effects.context.current_round,
            download_max_depth: parent.parent_effects.context.download_max_depth.clone(),
        }
    }
}

#[derive(Clone)]
pub struct ValidateContext {
    parent_effects: Effects<EngineContext>,
}
impl EffectsContext for ValidateContext {}
impl Effects<ValidateContext> {
    pub fn new(parent: &Effects<EngineContext>, point: &Point) -> Self {
        parent.new_child(
            ValidateContext {
                parent_effects: parent.clone(),
            },
            || {
                tracing::error_span!(
                    "validate",
                    author = display(point.data().author.alt()),
                    round = point.round().0,
                    digest = display(point.digest().alt()),
                )
            },
        )
    }

    // Links every recursive validation to the same engine context
    // to produce shorter logs (skip intermediate validation spans).
    // Notice that current engine round may advance while this spans
    // will still report the round that initialized the current chain
    pub fn deeper(&self, point: &Point) -> Self {
        Self::new(&self.context.parent_effects, point)
    }
}
