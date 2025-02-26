use std::sync::Arc;

use futures_util::never::Never;
use futures_util::FutureExt;
use parking_lot::Mutex;
use tokio::sync::oneshot;

use crate::effects::{Cancelled, Task, TaskTracker};
use crate::engine::lifecycle::handle::EngineHandle;
use crate::engine::lifecycle::{EngineError, FixHistoryFlag};
use crate::engine::Engine;
use crate::prelude::EngineCreated;

pub struct EngineRunning {
    handle: Arc<EngineHandle>,
    engine_stop_tx: oneshot::Sender<()>,
    engine_task_tracker: Arc<Mutex<GuardedTaskTracker>>,
    engine_recover_loop: Task<()>,
}

struct EngineRecoverLoop {
    handle: Arc<EngineHandle>,
    engine_task_tracker: Arc<Mutex<GuardedTaskTracker>>,
    engine_run: Task<Result<Never, EngineError>>,
}

struct GuardedTaskTracker {
    inner: TaskTracker,
    is_stop: bool,
}

impl EngineRunning {
    pub(crate) fn new(created: EngineCreated, engine_stop_tx: oneshot::Sender<()>) -> Self {
        let EngineCreated {
            handle,
            engine,
            engine_task_tracker,
        } = created;

        let handle = Arc::new(handle);
        let engine_task_tracker = Arc::new(Mutex::new(GuardedTaskTracker {
            inner: engine_task_tracker,
            is_stop: false,
        }));
        let engine_recover_loop = handle.super_tracker.ctx().spawn({
            let handle = handle.clone();
            let engine_task_tracker = engine_task_tracker.clone();
            EngineRecoverLoop::new(handle, engine, engine_task_tracker).run_loop()
        });
        Self {
            handle,
            engine_stop_tx,
            engine_task_tracker,
            engine_recover_loop,
        }
    }

    pub fn handle(&self) -> &EngineHandle {
        &self.handle
    }

    pub async fn stop(self) {
        let span = self.handle.stop_tracing_span();

        span.in_scope(|| tracing::warn!("waiting engine threads to exit"));

        let engine_tracker = {
            let mut guard = self.engine_task_tracker.lock();
            guard.is_stop = true;
            guard.inner.clone()
        };
        drop(self.engine_task_tracker);
        drop(self.engine_recover_loop); // aborts engine round task and drop second Arc<Handle>
        engine_tracker.stop().await;

        span.in_scope(|| tracing::warn!("engine threads exited, waiting handle threads"));

        let handle_super_tracker = self.handle.super_tracker.clone();
        drop(self.handle); // stops super tracker that spawned `engine_run` and network tasks
        handle_super_tracker.stop().await;

        span.in_scope(|| tracing::warn!("stop completed"));

        self.engine_stop_tx.send(()).ok();
    }
}

impl EngineRecoverLoop {
    pub fn new(
        handle: Arc<EngineHandle>,
        engine: Engine,
        engine_task_tracker: Arc<Mutex<GuardedTaskTracker>>,
    ) -> Self {
        let task_tracker = {
            let guard = engine_task_tracker.lock();
            guard.inner.clone()
        };
        let engine_run = task_tracker.ctx().spawn(engine.run());
        Self {
            engine_task_tracker,
            engine_run,
            handle,
        }
    }

    pub async fn run_loop(mut self) {
        loop {
            tracing::info!(
                peer_id = %self.handle.net.peer_id,
                overlay_id = %self.handle.net.overlay_id,
                genesis_info = ?self.handle.merged_conf.genesis_info(),
                conf = ?self.handle.merged_conf.conf,
                "mempool run"
            );

            let never_ok = self.engine_run.await;

            let mut task_tracker = {
                let guard = self.engine_task_tracker.lock();
                guard.inner.clone()
            };

            task_tracker.stop().await;

            let fix_history = match never_ok {
                Err(Cancelled()) | Ok(Err(EngineError::Cancelled)) => break,
                Ok(Err(EngineError::HistoryConflict(_))) => FixHistoryFlag(true),
            };

            task_tracker = {
                let mut guard = self.engine_task_tracker.lock();
                if guard.is_stop {
                    break; // do not update task tracker
                }
                guard.inner = TaskTracker::default();
                guard.inner.clone()
            };

            self.engine_run = task_tracker.ctx().spawn({
                let handle = self.handle.clone();
                let task_tracker = task_tracker.clone();
                Engine::new(&handle, &task_tracker, fix_history).run()
            });
        }
    }
}

// just in case the whole mempool thingy is dropped
impl Drop for GuardedTaskTracker {
    fn drop(&mut self) {
        let _guard = tracing::span::Span::current().entered();
        self.inner.stop().now_or_never();
        tracing::warn!("engine task tracker is stopped, will not spawn new threads");
    }
}
