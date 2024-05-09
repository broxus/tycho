use anyhow::Result;
use everscale_types::dict::Dict;
use everscale_types::models::BlockchainConfig;

pub trait BlockchainConfigExt {
    /// Check that config is valid.
    fn validate_params(
        &self,
        relax_par0: bool,
        mandatory_params: Option<Dict<u32, ()>>,
    ) -> Result<bool>;
}

impl BlockchainConfigExt for BlockchainConfig {
    fn validate_params(
        &self,
        _relax_par0: bool,
        _mandatory_params: Option<Dict<u32, ()>>,
    ) -> Result<bool> {
        // TODO: refer to https://github.com/everx-labs/ever-block/blob/master/src/config_params.rs#L452
        // STUB: currently should not be invoked in prototype
        todo!()
    }
}
