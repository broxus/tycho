use tycho_consensus::prelude::{EngineSession, InitPeers, MempoolConfigBuilder};

use super::state_update_queue::StateUpdateQueue;
use crate::mempool::StateUpdateContext;

pub struct ConfigAdapter {
    pub builder: MempoolConfigBuilder,
    pub state_update_queue: StateUpdateQueue,
    pub engine_session: Option<EngineSession>,
}

// TODO keep track of last applied v_set hash and round

impl ConfigAdapter {
    pub fn init_peers(new_cx: &StateUpdateContext) -> anyhow::Result<InitPeers> {
        let peers = InitPeers {
            prev_start_round: new_cx.consensus_info.prev_vset_switch_round,
            prev_v_set: new_cx.prev_v_set(),
            prev_v_subset: new_cx.prev_v_subset(),
            curr_start_round: new_cx.consensus_info.vset_switch_round,
            curr_v_set: new_cx.curr_v_set(),
            curr_v_subset: new_cx.curr_v_subset(),
            next_v_set: new_cx.next_v_set(),
        };

        anyhow::ensure!(
            peers.prev_v_set.is_empty() == peers.prev_v_subset.is_empty(),
            "Prev validator subset is empty after shuffle, vset len={}, start_round={}",
            peers.prev_v_set.len(),
            peers.prev_start_round,
        );

        anyhow::ensure!(
            !peers.curr_v_set.is_empty(),
            "Curr validator set is empty, start_round={}",
            peers.curr_start_round,
        );

        anyhow::ensure!(
            !peers.curr_v_subset.is_empty(),
            "Curr validator subset is empty after shuffle, vset len={}, start_round={}",
            peers.curr_v_set.len(),
            peers.curr_start_round,
        );

        Ok(peers)
    }
}
