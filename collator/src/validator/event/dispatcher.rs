use std::sync::Arc;

use anyhow::Result;
use tycho_types::models::BlockId;

use crate::validator::event::{SessionCtx, SignatureEvent, ValidationEvents};

impl<T> ValidationEvents for Arc<T>
where
    T: ValidationEvents,
{
    fn on_session_open(&self, ctx: &SessionCtx) -> Result<()> {
        (**self).on_session_open(ctx)
    }
    fn on_session_drop(&self, ctx: &SessionCtx) -> Result<()> {
        (**self).on_session_drop(ctx)
    }
    fn on_signature_event(&self, ev: &SignatureEvent) -> Result<()> {
        (**self).on_signature_event(ev)
    }
    fn on_validation_skipped(&self, ctx: &SessionCtx, block_id_short: &BlockId) -> Result<()> {
        (**self).on_validation_skipped(ctx, block_id_short)
    }
    fn on_validation_complete(&self, ctx: &SessionCtx, block_id_short: &BlockId) -> Result<()> {
        (**self).on_validation_complete(ctx, block_id_short)
    }
}

impl ValidationEvents for Vec<Arc<dyn ValidationEvents>> {
    fn on_session_open(&self, ctx: &SessionCtx) -> Result<()> {
        propagate(self, |s| s.on_session_open(ctx))
    }
    fn on_session_drop(&self, ctx: &SessionCtx) -> Result<()> {
        propagate(self, |s| s.on_session_drop(ctx))
    }
    fn on_signature_event(&self, ev: &SignatureEvent) -> Result<()> {
        propagate(self, |s| s.on_signature_event(ev))
    }
    fn on_validation_skipped(&self, ctx: &SessionCtx, block_id_short: &BlockId) -> Result<()> {
        propagate(self, |s| s.on_validation_skipped(ctx, block_id_short))
    }
    fn on_validation_complete(&self, ctx: &SessionCtx, block_id_short: &BlockId) -> Result<()> {
        propagate(self, |s| s.on_validation_complete(ctx, block_id_short))
    }
}

/// helper: call all sinks, return the first error (if any)
fn propagate<F>(sinks: &[Arc<dyn ValidationEvents>], mut f: F) -> Result<()>
where
    F: FnMut(&Arc<dyn ValidationEvents>) -> Result<()>,
{
    let mut first_err: Option<anyhow::Error> = None;
    for s in sinks {
        if let Err(e) = f(s) {
            if first_err.is_none() {
                first_err = Some(e);
            }
        }
    }
    if let Some(e) = first_err {
        Err(e)
    } else {
        Ok(())
    }
}
