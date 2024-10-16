use std::sync::Arc;

use anyhow::Result;
use everscale_types::cell::{HashBytes, Load};
use everscale_types::models::{
    BlockchainConfig, ConfigParam32, ConfigParam33, ConfigParam34, ConfigParam35, ConfigParam36,
    ConfigParam37, KnownConfigParam, ValidatorSet,
};
use tycho_block_util::config::BlockchainConfigExt;
use tycho_util::FastDashMap;

#[derive(Default)]
pub struct ValidatorSetCache {
    inner: FastDashMap<u32, (HashBytes, Arc<ValidatorSet>)>,
}

impl ValidatorSetCache {
    fn get_validator_set(
        &self,
        config: &BlockchainConfig,
        primary_param_id: u32,
        secondary_param_id: u32,
    ) -> Result<Option<(HashBytes, Arc<ValidatorSet>)>> {
        let Some(mut raw_vldr_set) = config.get_raw_or(primary_param_id, secondary_param_id)?
        else {
            return Ok(None);
        };

        let vlrd_set_hash = raw_vldr_set.cell().repr_hash();

        let res = match self.inner.entry(primary_param_id) {
            tycho_util::DashMapEntry::Occupied(mut entry) => {
                let (existing_hash, existing_vldr_set) = entry.get();
                if vlrd_set_hash != existing_hash {
                    let vldr_set = Arc::new(ValidatorSet::load_from(&mut raw_vldr_set)?);
                    entry.insert((*vlrd_set_hash, vldr_set.clone()));
                    (*vlrd_set_hash, vldr_set)
                } else {
                    (*existing_hash, existing_vldr_set.clone())
                }
            }
            tycho_util::DashMapEntry::Vacant(entry) => {
                let vldr_set = Arc::new(ValidatorSet::load_from(&mut raw_vldr_set)?);
                entry.insert((*vlrd_set_hash, vldr_set.clone()));
                (*vlrd_set_hash, vldr_set)
            }
        };

        Ok(Some(res))
    }

    /// Returns cached previous validator set if hash was not changed,
    /// otherwise parses it from config.
    pub fn get_prev_validator_set(
        &self,
        config: &BlockchainConfig,
    ) -> Result<Option<(HashBytes, Arc<ValidatorSet>)>> {
        self.get_validator_set(config, ConfigParam33::ID, ConfigParam32::ID)
    }

    /// Returns cached current validator set if hash was not changed,
    /// otherwise parses it from config.
    pub fn get_current_validator_set(
        &self,
        config: &BlockchainConfig,
    ) -> Result<(HashBytes, Arc<ValidatorSet>)> {
        let (hash, vldr_set) = self
            .get_validator_set(config, ConfigParam35::ID, ConfigParam34::ID)?
            .unwrap();
        Ok((hash, vldr_set))
    }

    /// Returns cached next validator set if hash was not changed,
    /// otherwise parses it from config.
    pub fn get_next_validator_set(
        &self,
        config: &BlockchainConfig,
    ) -> Result<Option<(HashBytes, Arc<ValidatorSet>)>> {
        self.get_validator_set(config, ConfigParam37::ID, ConfigParam36::ID)
    }
}
