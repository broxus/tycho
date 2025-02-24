use std::sync::Arc;

use tokio::sync::oneshot;

use crate::effects::{Cancelled, Task};
use crate::engine::lifecycle::handle::EngineHandle;
use crate::engine::Engine;

pub struct EngineRunning {
    _engine_run: Task<()>,
    handle: Arc<EngineHandle>,
}

impl EngineRunning {
    pub(super) fn new(
        engine: Engine,
        handle: EngineHandle,
        engine_stop_tx: oneshot::Sender<()>,
    ) -> Self {
        let handle = Arc::new(handle);
        let engine_run = async move {
            match engine.run().await {
                Err(Cancelled()) => {
                    engine_stop_tx.send(()).ok(); // caller may be dropped earlier on shutdown
                }
            }
        };

        Self {
            _engine_run: handle.task_tracker.ctx().spawn(engine_run),
            handle,
        }
    }

    pub fn handle(&self) -> &EngineHandle {
        &self.handle
    }

    pub async fn stop(self) {
        let span = self.handle.tracing_span();
        let task_tracker = self.handle.task_tracker.clone();

        drop(self);

        span.in_scope(|| tracing::warn!("stop in progress: waiting threads to exit"));

        task_tracker.wait_closed().await;

        span.in_scope(|| tracing::warn!("stop completed"));
    }
}
