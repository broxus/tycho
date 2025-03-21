use futures_util::FutureExt;
use tracing::Span;
use tycho_network::PeerId;

use crate::effects::{AltFormat, TaskTracker};
use crate::engine::lifecycle::args::{EngineBinding, EngineNetwork};
use crate::engine::MempoolMergedConfig;
use crate::models::Round;

/// Keep handle alive to keep engine running
// Note: do not impl Clone to keep all refs counted for restart
pub struct EngineHandle {
    /// not the same tracker as for [`EngineRunning`]
    pub(super) super_tracker: TaskTracker,
    pub(crate) bind: EngineBinding,
    pub(crate) net: EngineNetwork,
    pub(crate) merged_conf: MempoolMergedConfig,
}

impl EngineHandle {
    pub fn merged_conf(&self) -> &MempoolMergedConfig {
        &self.merged_conf
    }

    pub fn set_next_peers(&self, set: &[PeerId], subset: Option<(u32, &[PeerId])>) {
        if let Some((switch_round, subset)) = subset {
            if !(self.net.peer_schedule).set_next_subset(set, Round(switch_round), subset) {
                tracing::trace!("cannot schedule outdated round {switch_round} and set");
            }
        } else {
            self.net.peer_schedule.set_next_set(set);
        }
    }

    pub(super) fn stop_tracing_span(&self) -> Span {
        tracing::error_span!(
            "mempool stop in progress",
            peer = %self.net.peer_id.alt(),
            genesis_round = self.merged_conf.conf.genesis_round.0,
            overlay = %self.net.overlay_id,
        )
    }
}

impl Drop for EngineHandle {
    fn drop(&mut self) {
        let _guard = self.stop_tracing_span().entered();
        self.super_tracker.stop().now_or_never();
        tracing::warn!("handle is dropped, will not spawn new threads");
    }
}
