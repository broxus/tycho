use tokio::sync::oneshot;
use tycho_network::{OverlayService, PeerId};

use crate::effects::{Cancelled, Task, TaskResult, TaskTracker};
use crate::engine::{Engine, MempoolMergedConfig};
use crate::intercom::PeerSchedule;
use crate::models::Round;

pub struct EngineCreated {
    engine: Engine,
    handle: EngineHandle,
}

pub struct EngineRunning {
    engine_run: Task<()>,
    handle: EngineHandle,
}

pub struct EngineHandle {
    pub merged_conf: MempoolMergedConfig,
    peer_schedule: PeerSchedule,
    overlay_service: OverlayService,
    task_tracker: TaskTracker,
}

impl EngineCreated {
    pub(crate) fn new(
        merged_conf: &MempoolMergedConfig,
        engine: Engine,
        peer_schedule: &PeerSchedule,
        overlay_service: &OverlayService,
        task_tracker: TaskTracker,
    ) -> Self {
        Self {
            engine,
            handle: EngineHandle {
                merged_conf: merged_conf.clone(),
                peer_schedule: peer_schedule.clone(),
                overlay_service: overlay_service.clone(),
                task_tracker,
            },
        }
    }

    pub fn handle(&self) -> &EngineHandle {
        &self.handle
    }

    pub fn run(self) -> (EngineRunning, oneshot::Receiver<Cancelled>) {
        let (tx, rx) = oneshot::channel();
        let engine = self.engine;
        let task_ctx = self.handle.task_tracker.ctx();
        let engine_run = task_ctx.spawn(async move {
            match engine.run().await {
                Ok(()) => tracing::error!("mempool engine run loop must not contain break"),
                Err(e) => _ = tx.send(e).ok(), // caller may drop if not interested
            }
        });
        let handle = EngineRunning {
            engine_run,
            handle: self.handle,
        };
        (handle, rx)
    }

    #[cfg(any(test, feature = "test"))]
    pub fn set_start_peers(&self, peers: &[PeerId]) {
        let first = self.handle.merged_conf.genesis.round().next();
        (self.handle.peer_schedule).set_next_subset(peers, first, peers);
    }
}

impl EngineRunning {
    pub fn handle(&self) -> &EngineHandle {
        &self.handle
    }

    pub async fn stop(self) {
        let EngineHandle {
            merged_conf,
            peer_schedule,
            overlay_service,
            task_tracker,
        } = self.handle;

        drop(peer_schedule);
        drop(self.engine_run);

        task_tracker.close().await;

        overlay_service.remove_private_overlay(&merged_conf.overlay_id);
        tracing::warn!("mempool stop completed");
    }

    #[cfg(any(test, feature = "test"))]
    pub async fn into_engine_run(self) -> TaskResult<()> {
        drop(self.handle);
        self.engine_run.await
    }
}

impl EngineHandle {
    pub fn set_next_peers(&self, set: &[PeerId], subset: Option<(u32, &[PeerId])>) {
        if let Some((switch_round, subset)) = subset {
            // specially for zerostate with unaligned genesis,
            // and for first (prev) vset after reboot or a new genesis
            let round = if switch_round <= self.merged_conf.genesis.round().0 {
                self.merged_conf.genesis.round().next()
            } else {
                Round(switch_round)
            };
            if !(self.peer_schedule).set_next_subset(set, round, subset) {
                tracing::trace!("cannot schedule outdated round {switch_round} and set");
                return;
            }
        }
        self.peer_schedule.set_next_set(set);
    }
}
