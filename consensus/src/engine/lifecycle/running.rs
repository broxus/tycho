use std::sync::Arc;

use tokio::sync::oneshot;
use tycho_util::futures::JoinTask;

use crate::engine::lifecycle::handle::EngineHandle;
use crate::engine::Engine;

pub struct EngineRunning {
    _engine_run: JoinTask<()>,
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
            engine.run().await;
            engine_stop_tx.send(()).ok(); // caller may be dropped earlier on shutdown
        };

        Self {
            _engine_run: JoinTask::new(engine_run),
            handle,
        }
    }

    pub fn handle(&self) -> &EngineHandle {
        &self.handle
    }
}
