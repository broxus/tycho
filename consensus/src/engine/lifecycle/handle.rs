use tycho_network::PeerId;

use crate::engine::lifecycle::args::{EngineBinding, EngineNetwork};
use crate::engine::MempoolMergedConfig;
use crate::models::Round;

/// Keep handle alive to keep engine running
// Note: do not impl Clone to keep all refs counted for restart
pub struct EngineHandle {
    pub(crate) bind: EngineBinding,
    pub(crate) net: EngineNetwork,
    pub(crate) merged_conf: MempoolMergedConfig,
}

impl EngineHandle {
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
}
