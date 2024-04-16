use anyhow::Result;

use everscale_types::{dict::Dict, models::BlockchainConfig};

pub trait BlockchainConfigExt {
    /// Check that config is valid
    fn valid_config_data(
        &self,
        relax_par0: bool,
        mandatory_params: Option<Dict<u32, ()>>,
    ) -> Result<bool>;

    /// When important parameters changed the block must be marked as a key block
    fn important_config_parameters_changed(
        &self,
        other: &BlockchainConfig,
        coarse: bool,
    ) -> Result<bool>;
}

impl BlockchainConfigExt for BlockchainConfig {
    fn valid_config_data(
        &self,
        relax_par0: bool,
        mandatory_params: Option<Dict<u32, ()>>,
    ) -> Result<bool> {
        //TODO: refer to https://github.com/everx-labs/ever-block/blob/master/src/config_params.rs#L452
        //STUB: currently should not be invoked in prototype
        todo!()
    }

    fn important_config_parameters_changed(
        &self,
        other: &BlockchainConfig,
        coarse: bool,
    ) -> Result<bool> {
        //TODO: moved from old node, needs to review and check only important parameters
        if self.params == other.params {
            return Ok(false);
        }
        if coarse {
            return Ok(true);
        }
        // for now, all parameters are "important"
        // at least the parameters affecting the computations of validator sets must be considered important
        // ...
        Ok(true)
    }
}
