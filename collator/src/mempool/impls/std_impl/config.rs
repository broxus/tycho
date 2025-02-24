use anyhow::bail;
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
    pub fn apply_vset(engine: &EngineHandle, new_cx: &StateUpdateContext) -> anyhow::Result<()> {
        let round = new_cx.consensus_info.vset_switch_round;
        let whole_set = (new_cx.current_validator_set.1.list.iter())
            .map(|descr| PeerId(descr.public_key.0))
            .collect::<Vec<_>>();
        let subset = Self::compute_peers_subset(
            &new_cx.current_validator_set.1,
            &new_cx.mc_block_id,
            round,
            new_cx.shuffle_validators,
        )?;
        tracing::info!(
            target: tracing_targets::MEMPOOL_ADAPTER,
            len = subset.len(),
            vset_len = whole_set.len(),
            %round,
            "New current validator subset"
        );
        engine.set_next_peers(&whole_set, Some((round, &subset)));
        Ok(())
    }

    pub fn apply_next_vset(engine: &EngineHandle, new_cx: &StateUpdateContext) {
        if let Some((_, next)) = &new_cx.next_validator_set {
            // NOTE: do not try to calculate subset from next set
            //  because it is impossible without known future session_update_round
            let whole_set = (next.list.iter())
                .map(|descr| PeerId(descr.public_key.0))
                .collect::<Vec<_>>();
            tracing::info!(
                target: tracing_targets::MEMPOOL_ADAPTER,
                vset_len = whole_set.len(),
                "New net validator set"
            );
            engine.set_next_peers(&whole_set, None);
        }
    }

    pub fn apply_prev_vset(
        engine: &EngineHandle,
        new_cx: &StateUpdateContext,
    ) -> anyhow::Result<()> {
        if let Some((_, prev_set)) = new_cx.prev_validator_set.as_ref() {
            let round = new_cx.consensus_info.prev_vset_switch_round;
            let whole_set = prev_set
                .list
                .iter()
                .map(|descr| PeerId(descr.public_key.0))
                .collect::<Vec<_>>();
            let subset = Self::compute_peers_subset(
                prev_set,
                &new_cx.mc_block_id,
                round,
                new_cx.consensus_info.prev_shuffle_mc_validators,
            )?;
            tracing::info!(
                target: tracing_targets::MEMPOOL_ADAPTER,
                len = subset.len(),
                vset_len = whole_set.len(),
                %round,
                "New prev validator subset"
            );
            engine.set_next_peers(&whole_set, Some((round, &subset)));
        }

        Ok(())
    }

    fn compute_peers_subset(
        validator_set: &ValidatorSet,
        mc_block_id: &BlockId,
        session_update_round: u32,
        shuffle_validators: bool,
    ) -> anyhow::Result<Vec<PeerId>> {
        let Some((list, _)) =
            validator_set.compute_mc_subset(session_update_round, shuffle_validators)
        else {
            bail!(
                "Mempool peer set is empty after shuffle, mc_block_id: {}",
                mc_block_id,
            )
        };
        let result = list
            .into_iter()
            .map(|x| PeerId(x.public_key.0))
            .collect::<Vec<_>>();
        Ok(result)
    }
}
