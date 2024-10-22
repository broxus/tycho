use std::sync::Arc;

use everscale_types::cell::HashBytes;
use everscale_types::models::{BlockId, CatchainConfig, ConsensusConfig, ValidatorSet};

#[derive(Debug)]
pub struct StateUpdateContext {
    pub mc_block_id: BlockId,
    // TODO store and get ConsensusInfo from McStateExtra
    // instead of `mempool_switch_round`, `mempool_genesis_round`, `mempool_genesis_millis`
    pub mempool_switch_round: u32,
    pub mempool_genesis_round: u32,
    pub mempool_genesis_millis: u64,
    pub consensus_config: ConsensusConfig,
    pub catchain_config: CatchainConfig,
    pub prev_validator_set: Option<(HashBytes, Arc<ValidatorSet>)>,
    pub current_validator_set: (HashBytes, Arc<ValidatorSet>),
    pub next_validator_set: Option<(HashBytes, Arc<ValidatorSet>)>,
}

pub struct DebugStateUpdateContext<'a>(pub &'a StateUpdateContext);
impl std::fmt::Debug for DebugStateUpdateContext<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StateUpdateContext")
            .field("mc_block_id", &self.0.mc_block_id.as_short_id())
            .field("mempool_switch_round", &self.0.mempool_switch_round)
            .field("mempool_genesis_round", &self.0.mempool_genesis_round)
            .field("mempool_genesis_millis", &self.0.mempool_genesis_millis)
            .field("catchain_config", &self.0.catchain_config)
            .field("consensus_config", &self.0.consensus_config)
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
