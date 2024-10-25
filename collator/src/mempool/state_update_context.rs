use std::sync::Arc;

use everscale_types::cell::HashBytes;
use everscale_types::models::{BlockId, ConsensusConfig, ConsensusInfo, ValidatorSet};

use super::MempoolAnchorId;

#[derive(Debug)]
pub struct StateUpdateContext {
    pub mc_block_id: BlockId,
    pub mc_block_chain_time: u64,
    pub mc_processed_to_anchor_id: MempoolAnchorId,
    pub consensus_info: ConsensusInfo,
    pub consensus_config: ConsensusConfig,
    pub shuffle_validators: bool,
    pub prev_validator_set: Option<(HashBytes, Arc<ValidatorSet>)>,
    pub current_validator_set: (HashBytes, Arc<ValidatorSet>),
    pub next_validator_set: Option<(HashBytes, Arc<ValidatorSet>)>,
}

pub struct DebugStateUpdateContext<'a>(pub &'a StateUpdateContext);
impl std::fmt::Debug for DebugStateUpdateContext<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StateUpdateContext")
            .field("mc_block_id", &self.0.mc_block_id.as_short_id())
            .field("mc_block_chain_time", &self.0.mc_block_chain_time)
            .field(
                "mc_processed_to_anchor_id",
                &self.0.mc_processed_to_anchor_id,
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
