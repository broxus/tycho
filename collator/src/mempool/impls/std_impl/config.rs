use tycho_consensus::prelude::{EngineHandle, EngineRunning, InitPeers, MempoolConfigBuilder};
use tycho_network::PeerId;

use super::state_update_queue::StateUpdateQueue;
use crate::mempool::StateUpdateContext;
use crate::tracing_targets;

pub struct ConfigAdapter {
    pub builder: MempoolConfigBuilder,
    pub state_update_queue: StateUpdateQueue,
    pub engine_running: Option<EngineRunning>,
}

// TODO keep track of last applied v_set hash and round

impl ConfigAdapter {
    pub fn init_peers(new_cx: &StateUpdateContext) -> InitPeers {
        InitPeers {
            prev_start_round: new_cx.consensus_info.prev_vset_switch_round,
            prev_v_set: new_cx.prev_v_set(),
            prev_v_subset: new_cx.prev_v_subset(),
            curr_start_round: new_cx.consensus_info.vset_switch_round,
            curr_v_set: new_cx.curr_v_set(),
            curr_v_subset: new_cx.curr_v_subset(),
            next_v_set: new_cx.next_v_set(),
        }
    }

    pub fn apply_next_vset(engine: &EngineHandle, new_cx: &StateUpdateContext) {
        let v_set = new_cx.next_v_set();
        if v_set.is_empty() {
            return;
        }
        // NOTE: do not try to calculate subset from next set
        //  because it is impossible without known future session_update_round
        tracing::info!(
            target: tracing_targets::MEMPOOL_ADAPTER,
            vset_len = v_set.len(),
            "Apply next validator set"
        );
        engine.set_next_peers(&v_set, None);
    }

    pub fn apply_curr_vset(
        engine: &EngineHandle,
        new_cx: &StateUpdateContext,
    ) -> anyhow::Result<()> {
        Self::apply_subset(
            engine,
            new_cx.consensus_info.vset_switch_round,
            &new_cx.curr_v_set(),
            &new_cx.curr_v_subset(),
            "Apply current validator subset",
        )
    }

    pub fn apply_prev_vset(
        engine: &EngineHandle,
        new_cx: &StateUpdateContext,
    ) -> anyhow::Result<()> {
        let prev_v_set = new_cx.prev_v_set();
        if prev_v_set.is_empty() {
            return Ok(());
        }
        Self::apply_subset(
            engine,
            new_cx.consensus_info.prev_vset_switch_round,
            &prev_v_set,
            &new_cx.prev_v_subset(),
            "Apply prev validator subset",
        )
    }

    fn apply_subset(
        engine: &EngineHandle,
        start_round: u32,
        v_set: &[PeerId],
        v_subset: &[PeerId],
        message: &'static str,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            !v_set.is_empty(),
            "{message}: v_set is empty, start_round={start_round}",
        );

        anyhow::ensure!(
            !v_subset.is_empty(),
            "{message}: subset is empty after shuffle, vset len={}, start_round={start_round}",
            v_set.len(),
        );

        tracing::info!(
            target: tracing_targets::MEMPOOL_ADAPTER,
            message, // field name "message" is used by macros to display a nameless value
            len = v_subset.len(),
            vset_len = v_set.len(),
            %start_round,
        );

        engine.set_next_peers(v_set, Some((start_round, v_subset)));
        Ok(())
    }
}
