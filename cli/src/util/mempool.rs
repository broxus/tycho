use tycho_block_util::state::ShardStateStuff;
use tycho_consensus::prelude::MempoolConfigBuilder;
use tycho_core::global_config::MempoolGlobalConfig;

pub fn set_mempool_config(
    config_builder: &mut MempoolConfigBuilder,
    mc_state: &ShardStateStuff,
    mempool_config_override: Option<&MempoolGlobalConfig>,
) -> anyhow::Result<()> {
    match mempool_config_override {
        None => {
            let config = mc_state.config_params()?;
            config_builder.set_consensus_config(&config.params.get_consensus_config()?);
            config_builder.set_genesis(mc_state.state_extra()?.validator_info.catchain_seqno, 0);
        }
        Some(global) => {
            config_builder.set_consensus_config(&global.consensus_config);
            config_builder.set_genesis(global.start_round, global.genesis_time_millis);
        }
    };
    Ok(())
}
