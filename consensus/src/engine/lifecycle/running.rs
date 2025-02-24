use std::sync::Arc;

use tokio::sync::oneshot;

use crate::effects::{Cancelled, Task};
use crate::engine::lifecycle::handle::EngineHandle;
use crate::prelude::EngineCreated;

pub struct EngineRunning {
    handle: Arc<EngineHandle>,
    _engine_run: Task<()>,
}

impl EngineRunning {
    pub(crate) fn new(created: EngineCreated, engine_stop_tx: oneshot::Sender<()>) -> Self {
        let EngineCreated { handle, engine } = created;
        let handle = Arc::new(handle);

        let _engine_run = handle.super_tracker.ctx().spawn(async move {
            match engine.run().await {
                Err(Cancelled()) => {
                    engine_stop_tx.send(()).ok(); // caller may be dropped earlier on shutdown
                }
            }
        });
        Self {
            handle,
            _engine_run,
        }
    }

    pub fn handle(&self) -> &EngineHandle {
        &self.handle
    }

    pub async fn stop(self) {
        let span = self.handle.stop_tracing_span();

        span.in_scope(|| tracing::warn!("waiting engine threads to exit"));

        let handle_super_tracker = self.handle.super_tracker.clone();

        drop(self);

        handle_super_tracker.stop().await;

        span.in_scope(|| tracing::warn!("stop completed"));
    }
}
