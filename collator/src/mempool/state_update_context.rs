use std::sync::Arc;

use anyhow::Result;
use everscale_types::cell::HashBytes;
use everscale_types::models::{BlockId, CatchainConfig, ConsensusConfig, ValidatorSet};
use tycho_block_util::config::BlockchainConfigExt;
use tycho_block_util::state::ShardStateStuff;

#[derive(Debug)]
pub struct StateUpdateContext {
    pub mc_block_id: BlockId,
    // TODO either move `mempool_switch_round` in `ValidatorSet`,
    //   or store and get switch round for `prev_validator_set`,
    //   or remove `prev_validator_set` as unusable
    //     (to start mempool with zerostate peers if newer state was not found on boot),
    //   because previous subset cannot be defined without previous switch round
    pub mempool_switch_round: u32,
    // TODO store and get geensis_round u32 from McStateExtra
    // TODO store and get genesis_time_millis u64 from McStateExtra
    pub consensus_config: ConsensusConfig,
    pub catchain_config: CatchainConfig,
    pub prev_validator_set: Option<(HashBytes, Arc<ValidatorSet>)>,
    pub current_validator_set: (HashBytes, Arc<ValidatorSet>),
    pub next_validator_set: Option<(HashBytes, Arc<ValidatorSet>)>,
}

impl StateUpdateContext {
    /// Generally ets are cached in
    /// [`ValidatorSetCache`](crate::utils::vldr_set_cache::ValidatorSetCache)
    pub fn uncached_from(mc_state: &ShardStateStuff) -> Result<Self> {
        let config = mc_state.config_params()?;

        let prev_validator_set = (config.get_prev_validator_set_raw()?)
            .zip(config.get_previous_validator_set()?)
            .map(|(cell, set)| (*cell.repr_hash(), Arc::new(set)));

        let current_set_hash = *config.get_current_validator_set_raw()?.repr_hash();
        let current_set = config.get_current_validator_set()?;

        let next_validator_set = (config.get_next_validator_set_raw()?)
            .zip(config.get_next_validator_set()?)
            .map(|(cell, set)| (*cell.repr_hash(), Arc::new(set)));

        let mc_data = mc_state.state_extra()?;

        Ok(StateUpdateContext {
            mc_block_id: *mc_state.block_id(),
            // FIXME looks like by design changes either full validator set or catchain seqno
            mempool_switch_round: mc_data.validator_info.catchain_seqno,
            catchain_config: mc_data.config.get_catchain_config()?,
            consensus_config: mc_data.config.get_consensus_config()?,
            prev_validator_set,
            current_validator_set: (current_set_hash, Arc::new(current_set)),
            next_validator_set,
        })
    }
}

pub struct DebugStateUpdateContext<'a>(pub &'a StateUpdateContext);
impl std::fmt::Debug for DebugStateUpdateContext<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StateUpdateContext")
            .field("mc_block_id", &self.0.mc_block_id.as_short_id())
            .field("mempool_switch_round", &self.0.mempool_switch_round)
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
