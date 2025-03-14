use anyhow::Context;
use everscale_types::models::{BlockId, ValidatorSet};
use tycho_consensus::prelude::{EngineHandle, EngineRunning, MempoolConfigBuilder};
use tycho_network::PeerId;

use crate::mempool::StateUpdateContext;
use crate::tracing_targets;

pub struct ConfigAdapter {
    pub builder: MempoolConfigBuilder,
    pub state_update_ctx: Option<StateUpdateContext>,
    pub engine_running: Option<EngineRunning>,
}

impl ConfigAdapter {
    pub fn apply_next_vset(engine: &EngineHandle, new_cx: &StateUpdateContext) {
        let Some((_, next_set)) = &new_cx.next_validator_set else {
            return;
        };
        // NOTE: do not try to calculate subset from next set
        //  because it is impossible without known future session_update_round
        let whole_set = (next_set.list.iter())
            .map(|descr| PeerId(descr.public_key.0))
            .collect::<Vec<_>>();
        tracing::info!(
            target: tracing_targets::MEMPOOL_ADAPTER,
            vset_len = whole_set.len(),
            "Apply next validator set"
        );
        engine.set_next_peers(&whole_set, None);
    }

    pub fn apply_curr_vset(
        engine: &EngineHandle,
        new_cx: &StateUpdateContext,
    ) -> anyhow::Result<()> {
        let (_, curr_set) = &new_cx.current_validator_set;
        Self::compute_apply_subset(
            engine,
            &new_cx.mc_block_id,
            curr_set,
            new_cx.consensus_info.vset_switch_round,
            new_cx.shuffle_validators,
            "Apply current validator subset",
        )
    }

    pub fn apply_prev_vset(
        engine: &EngineHandle,
        new_cx: &StateUpdateContext,
    ) -> anyhow::Result<()> {
        let Some((_, prev_set)) = new_cx.prev_validator_set.as_ref() else {
            return Ok(());
        };
        Self::compute_apply_subset(
            engine,
            &new_cx.mc_block_id,
            prev_set,
            new_cx.consensus_info.prev_vset_switch_round,
            new_cx.consensus_info.prev_shuffle_mc_validators,
            "Apply prev validator subset",
        )
    }

    fn compute_apply_subset(
        engine: &EngineHandle,
        mc_block_id: &BlockId,
        validator_set: &ValidatorSet,
        session_start_round: u32,
        shuffle_validators: bool,
        message: &'static str,
    ) -> anyhow::Result<()> {
        let whole_set = (validator_set.list.iter())
            .map(|descr| PeerId(descr.public_key.0))
            .collect::<Vec<_>>();

        let (validator_sub_list, _) = validator_set
            .compute_mc_subset(session_start_round, shuffle_validators)
            .with_context(|| {
                format!("{message}: subset is empty after shuffle, mc_block_id: {mc_block_id}")
            })?;

        let subset = (validator_sub_list.into_iter())
            .map(|x| PeerId(x.public_key.0))
            .collect::<Vec<_>>();

        tracing::info!(
            target: tracing_targets::MEMPOOL_ADAPTER,
            message, // field name "message" is used by macros to display a nameless value
            len = subset.len(),
            vset_len = whole_set.len(),
            %session_start_round,
        );

        engine.set_next_peers(&whole_set, Some((session_start_round, &subset)));
        Ok(())
    }
}
