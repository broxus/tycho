use std::sync::Arc;

use tokio::sync::oneshot;
use tycho_util::futures::JoinTask;

use crate::engine::lifecycle::handle::EngineHandle;
use crate::engine::Engine;

pub struct EngineRunning {
    handle: Arc<EngineHandle>,
    _engine_run: JoinTask<()>,
}

impl EngineRunning {
    pub(super) fn new(handle: EngineHandle, engine_stop_tx: oneshot::Sender<()>) -> Self {
        let handle = Arc::new(handle);
        let engine = Engine::new(&handle);
        let _engine_run = JoinTask::new(async move {
            engine.run().await;
            engine_stop_tx.send(()).ok(); // caller may be dropped earlier on shutdown
        });
        Self {
            handle,
            _engine_run,
        }
    }

    pub fn handle(&self) -> &EngineHandle {
        &self.handle
    }
}
