use anyhow::{Context, Result};
use tycho_types::error::Error;
use tycho_types::models::{
    BlockchainConfig, ConfigParam0, ConfigParam32, ConfigParam33, ConfigParam34, ConfigParam35,
    ConfigParam36, ConfigParam37, KnownConfigParam,
};
use tycho_types::prelude::*;

pub fn compute_gas_price_factor(is_masterchain: bool, gas_price: u64) -> Result<u64> {
    const fn base_gas_price(is_masterchain: bool) -> u64 {
        if is_masterchain {
            10_000 << 16
        } else {
            1_000 << 16
        }
    }

    let base_gas_price = base_gas_price(is_masterchain);
    Ok(gas_price.checked_shl(16).context("gas price is too big")? / base_gas_price)
}

pub const fn apply_price_factor(mut value: u128, price_factor: u64) -> u128 {
    value = value.saturating_mul(price_factor as u128);

    let r = value & 0xffff != 0;
    (value >> 16) + r as u128
}

pub fn build_elections_data_to_sign(
    election_id: u32,
    stake_factor: u32,
    address: &HashBytes,
    adnl_addr: &HashBytes,
) -> Vec<u8> {
    const TL_ID: u32 = 0x654C5074;

    let mut data = Vec::with_capacity(4 + 4 + 4 + 32 + 32);
    data.extend_from_slice(&TL_ID.to_be_bytes());
    data.extend_from_slice(&election_id.to_be_bytes());
    data.extend_from_slice(&stake_factor.to_be_bytes());
    data.extend_from_slice(address.as_slice());
    data.extend_from_slice(adnl_addr.as_array());
    data
}

pub trait BlockchainConfigExt {
    /// Check that config is valid.
    fn validate_params(&self) -> Result<()>;

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
    fn get_current_validator_set_raw(&self) -> Result<Cell, Error> {
        match self.get_raw_cell(ConfigParam35::ID)? {
            None => self.get_raw_cell(ConfigParam34::ID)?,
            set => set,
        }
        .ok_or(Error::CellUnderflow)
    }

    /// Returns a cell with the next validator set.
    ///
    /// Uses [`ConfigParam37`] (temp next validators) or [`ConfigParam36`] (next validators).
    fn get_next_validator_set_raw(&self) -> Result<Option<Cell>, Error> {
        match self.get_raw_cell(ConfigParam37::ID)? {
            None => self.get_raw_cell(ConfigParam36::ID),
            set => Ok(set),
        }
    }

    fn get_raw_cell(&self, id: u32) -> Result<Option<Cell>, Error>;
}

impl BlockchainConfigExt for BlockchainConfig {
    fn validate_params(&self) -> Result<()> {
        let Some(config_address) = self.params.get::<ConfigParam0>()? else {
            anyhow::bail!("config address is absent");
        };
        anyhow::ensure!(config_address == self.address, "config address mismatch");

        let params = self.get_mandatory_params()?;
        for id in params.keys() {
            let id = id?;
            if !self
                .contains_raw(id)
                .with_context(|| format!("failed to get config param {id}"))?
            {
                anyhow::bail!("mandatory config param {id} is absent");
            }
        }

        Ok(())
    }

    fn get_raw_cell(&self, id: u32) -> Result<Option<Cell>, Error> {
        let Some(value) = self.params.as_dict().get_raw(id)? else {
            return Ok(None);
        };
        value.get_reference_cloned(0).map(Some)
    }
}
