use anyhow::Result;
use everscale_types::cell::Cell;
use everscale_types::dict::Dict;
use everscale_types::error::Error;
use everscale_types::models::{
    BlockchainConfig, ConfigParam32, ConfigParam33, ConfigParam34, ConfigParam35, ConfigParam36,
    ConfigParam37, KnownConfigParam,
};

pub trait BlockchainConfigExt {
    /// Check that config is valid.
    fn validate_params(
        &self,
        relax_par0: bool,
        mandatory_params: Option<Dict<u32, ()>>,
    ) -> Result<bool>;

    /// Returns a cell with the previous validator set.
    ///
    /// Uses [`ConfigParam33`] (temp prev validators) or [`ConfigParam32`] (prev validators).
    fn get_prev_validator_set_raw(&self) -> Result<Option<Cell>, Error> {
        match self.get_raw_cell(ConfigParam33::ID)? {
            None => self.get_raw_cell(ConfigParam32::ID),
            set => Ok(set),
        }
    }

    /// Returns a cell with the current validator set.
    ///
    /// Uses [`ConfigParam35`] (temp validators) or [`ConfigParam34`] (current validators).
    fn get_current_validator_set(&self) -> Result<Cell, Error> {
        match self.get_raw_cell(ConfigParam35::ID)? {
            None => self.get_raw_cell(ConfigParam34::ID)?,
            set => set,
        }
        .ok_or(Error::CellUnderflow)
    }

    /// Returns a cell with the next validator set.
    ///
    /// Uses [`ConfigParam37`] (temp next validators) or [`ConfigParam36`] (next validators).
    fn get_next_validator_set(&self) -> Result<Option<Cell>, Error> {
        match self.get_raw_cell(ConfigParam37::ID)? {
            None => self.get_raw_cell(ConfigParam36::ID),
            set => Ok(set),
        }
    }

    fn get_raw_cell(&self, id: u32) -> Result<Option<Cell>, Error>;
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

    fn get_raw_cell(&self, id: u32) -> Result<Option<Cell>, Error> {
        let Some(value) = self.params.as_dict().get_raw(id)? else {
            return Ok(None);
        };
        value.get_reference_cloned(0).map(Some)
    }
}
