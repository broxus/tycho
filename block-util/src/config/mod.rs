use anyhow::Result;
use everscale_types::cell::CellSlice;
use everscale_types::dict::Dict;
use everscale_types::models::BlockchainConfig;

pub trait BlockchainConfigExt {
    /// Check that config is valid.
    fn validate_params(
        &self,
        relax_par0: bool,
        mandatory_params: Option<Dict<u32, ()>>,
    ) -> Result<bool>;

    /// Get raw cell from primary param.
    /// If None, then get raw from secondary.
    fn get_raw_or(
        &self,
        primary_param_id: u32,
        secondary_param_id: u32,
    ) -> Result<Option<CellSlice<'_>>>;
}

impl BlockchainConfigExt for BlockchainConfig {
    fn validate_params(
        &self,
        _relax_par0: bool,
        _mandatory_params: Option<Dict<u32, ()>>,
    ) -> Result<bool> {
        // TODO: refer to https://github.com/everx-labs/ever-block/blob/master/src/config_params.rs#L452
        // STUB: currently should not be invoked in prototype
        // todo!()
        Ok(true)
    }

    fn get_raw_or(
        &self,
        primary_param_id: u32,
        secondary_param_id: u32,
    ) -> Result<Option<CellSlice<'_>>> {
        let res = match self.get_raw(primary_param_id)? {
            None => self.get_raw(secondary_param_id)?,
            value => value,
        };
        Ok(res)
    }
}
