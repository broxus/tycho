use tokio::sync::oneshot;
use tracing::Span;
use tycho_network::PeerId;

use crate::effects::{AltFormat, TaskTracker};
use crate::engine::lifecycle::args::{EngineBinding, EngineNetwork, EngineNetworkArgs};
use crate::engine::lifecycle::FixHistoryFlag;
use crate::engine::{Engine, MempoolMergedConfig};
use crate::models::Round;
use crate::prelude::EngineRunning;

/// Keep handle alive to keep engine running
// Note: do not impl Clone to keep all refs counted for restart
pub struct EngineHandle {
    pub(crate) bind: EngineBinding,
    pub(crate) net: EngineNetwork,
    pub(crate) task_tracker: TaskTracker,
    pub(crate) merged_conf: MempoolMergedConfig,
}

impl EngineHandle {
    pub fn new(
        bind: EngineBinding,
        net_args: &EngineNetworkArgs,
        merged_conf: &MempoolMergedConfig,
    ) -> Self {
        let task_tracker = TaskTracker::default();
        let net = EngineNetwork::new(net_args, &task_tracker, merged_conf);

        Self {
            bind,
            net,
            task_tracker,
            merged_conf: merged_conf.clone(),
        }
    }

    pub fn run(self, engine_stop_tx: oneshot::Sender<()>) -> EngineRunning {
        let engine = Engine::new(&self, FixHistoryFlag::default());
        EngineRunning::new(engine, self, engine_stop_tx)
    }

    pub fn merged_conf(&self) -> &MempoolMergedConfig {
        &self.merged_conf
    }

    #[cfg(any(test, feature = "test"))]
    pub fn set_start_peers(&self, peers: &[PeerId]) {
        let first = (self.merged_conf.conf.genesis_round).next();
        (self.net.peer_schedule).set_next_subset(peers, first, peers);
    }

    pub fn set_next_peers(&self, set: &[PeerId], subset: Option<(u32, &[PeerId])>) {
        if let Some((switch_round, subset)) = subset {
            let genesis_round = self.merged_conf().conf.genesis_round;
            // specially for zerostate with unaligned genesis,
            // and for first (prev) vset after reboot or a new genesis
            let round = if switch_round <= genesis_round.0 {
                genesis_round.next()
            } else {
                Round(switch_round)
            };
            if !(self.net.peer_schedule).set_next_subset(set, round, subset) {
                tracing::trace!("cannot schedule outdated round {switch_round} and set");
                return;
            }
        }
        self.net.peer_schedule.set_next_set(set);
    }

    pub(super) fn tracing_span(&self) -> Span {
        tracing::error_span!(
            "mempool",
            peer = %self.net.peer_id.alt(),
            genesis_round = self.merged_conf.conf.genesis_round.0,
            overlay = %self.net.overlay_id,
        )
    }
}

impl Drop for EngineHandle {
    fn drop(&mut self) {
        let _guard = self.tracing_span().entered();
        tracing::warn!("stop in progress: engine handle is dropped, threads are aborted");
        self.task_tracker.close();
    }
}
