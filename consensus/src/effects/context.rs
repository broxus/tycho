use std::marker::PhantomData;

use tracing::Span;

/// All side effects are scoped to their context, then often (but not always) equals to module
pub trait EffectsContext {}

#[derive(Clone)]
pub struct Effects<T: EffectsContext> {
    /// generally of `tracing::level::Error` as always visible
    span: Span,
    // TODO metrics scoped by use site (context)
    _metrics: PhantomData<T>,
}

impl<T: EffectsContext> Effects<T> {
    pub fn new_root(span: Span) -> Self {
        Self {
            span,
            _metrics: PhantomData,
        }
    }
    /// forbids to create new effects outside the span tree
    pub fn new_child<F, U>(parent_span: &Span, to_span: F) -> Effects<U>
    where
        F: FnOnce() -> Span,
        U: EffectsContext,
    {
        Effects::<U> {
            span: parent_span.in_scope(to_span),
            _metrics: PhantomData,
        }
    }
    pub fn span(&self) -> &'_ Span {
        &self.span
    }
}

// used as type parameters in several modules (to create children)
// while implementations are kept private to use site
#[derive(Clone)]
pub struct CurrentRoundContext;
#[derive(Clone)]
pub struct ValidateContext;
#[derive(Clone)]
pub struct CollectorContext;
