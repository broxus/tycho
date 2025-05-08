use std::sync::Arc;

use everscale_types::cell::HashBytes;
use everscale_types::models::{
    BlockId, ConsensusConfig, ConsensusInfo, ValidatorDescription, ValidatorSet,
};
use tycho_network::PeerId;

use super::MempoolAnchorId;

#[derive(Debug)]
pub struct StateUpdateContext {
    pub mc_block_id: BlockId,
    pub mc_block_chain_time: u64,
    pub top_processed_to_anchor_id: MempoolAnchorId,
    pub consensus_info: ConsensusInfo,
    pub consensus_config: ConsensusConfig,
    pub shuffle_validators: bool,
    pub prev_validator_set: Option<(HashBytes, Arc<ValidatorSet>)>,
    pub current_validator_set: (HashBytes, Arc<ValidatorSet>),
    pub next_validator_set: Option<(HashBytes, Arc<ValidatorSet>)>,
}

impl StateUpdateContext {
    pub fn prev_v_set(&self) -> Vec<PeerId> {
        Self::peer_ids((self.prev_validator_set.iter()).flat_map(|(_, v_set)| v_set.list.iter()))
    }

    pub fn curr_v_set(&self) -> Vec<PeerId> {
        Self::peer_ids(self.current_validator_set.1.list.iter())
    }

    pub fn next_v_set(&self) -> Vec<PeerId> {
        Self::peer_ids((self.next_validator_set.iter()).flat_map(|(_, v_set)| v_set.list.iter()))
    }

    pub fn prev_v_subset(&self) -> Vec<PeerId> {
        let Some((_, prev_validator_set)) = self.prev_validator_set.as_ref() else {
            return Vec::new();
        };
        Self::compute_subset(
            prev_validator_set,
            self.consensus_info.prev_vset_switch_round,
            self.consensus_info.prev_shuffle_mc_validators,
        )
    }

    pub fn curr_v_subset(&self) -> Vec<PeerId> {
        Self::compute_subset(
            &self.current_validator_set.1,
            self.consensus_info.vset_switch_round,
            self.shuffle_validators,
        )
    }

    // NOTE: do not try to calculate subset from next set
    //  because it is impossible without known future session_update_round

    fn compute_subset(
        validator_set: &ValidatorSet,
        session_start_round: u32,
        shuffle_validators: bool,
    ) -> Vec<PeerId> {
        Self::peer_ids(
            validator_set
                .compute_mc_subset(session_start_round, shuffle_validators)
                .iter()
                .flat_map(|(sub_list, _)| sub_list.iter()),
        )
    }

    fn peer_ids<'a>(iter: impl Iterator<Item = &'a ValidatorDescription>) -> Vec<PeerId> {
        iter.map(|descr| PeerId(descr.public_key.0)).collect()
    }
}

pub struct DebugStateUpdateContext<'a>(pub &'a StateUpdateContext);
impl std::fmt::Debug for DebugStateUpdateContext<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StateUpdateContext")
            .field("mc_block_id", &self.0.mc_block_id.as_short_id())
            .field("mc_block_chain_time", &self.0.mc_block_chain_time)
            .field(
                "top_processed_to_anchor_id",
                &self.0.top_processed_to_anchor_id,
            )
            .field("consensus_info", &self.0.consensus_info)
            .field("consensus_config", &self.0.consensus_config)
            .field("shuffle_validators", &self.0.shuffle_validators)
            .field(
                "prev_validator_set.hash",
                &self.0.prev_validator_set.as_ref().map(|s| s.0),
            )
            .field(
                "current_validator_set.hash",
                &self.0.current_validator_set.0,
            )
            .field(
                "next_validator_set.hash",
                &self.0.next_validator_set.as_ref().map(|s| s.0),
            )
            .finish()
    }
}
