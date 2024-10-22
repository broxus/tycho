use std::sync::Arc;

use anyhow::Result;
use everscale_types::cell::HashBytes;
use everscale_types::models::{BlockchainConfig, ValidatorSet};
use tycho_block_util::config::BlockchainConfigExt;
use tycho_util::FastDashMap;

#[derive(Default)]
pub struct ValidatorSetCache {
    inner: FastDashMap<VsetType, (HashBytes, Arc<ValidatorSet>)>,
}

impl ValidatorSetCache {
    /// Returns cached previous validator set if hash was not changed,
    /// otherwise parses it from config.
    pub fn get_prev_validator_set(
        &self,
        config: &BlockchainConfig,
    ) -> Result<Option<(HashBytes, Arc<ValidatorSet>)>> {
        self.get_validator_set(config, VsetType::Prev)
    }

    /// Returns cached current validator set if hash was not changed,
    /// otherwise parses it from config.
    pub fn get_current_validator_set(
        &self,
        config: &BlockchainConfig,
    ) -> Result<(HashBytes, Arc<ValidatorSet>)> {
        let (hash, vset) = self.get_validator_set(config, VsetType::Current)?.unwrap();
        Ok((hash, vset))
    }

    /// Returns cached next validator set if hash was not changed,
    /// otherwise parses it from config.
    pub fn get_next_validator_set(
        &self,
        config: &BlockchainConfig,
    ) -> Result<Option<(HashBytes, Arc<ValidatorSet>)>> {
        self.get_validator_set(config, VsetType::Next)
    }

    fn get_validator_set(
        &self,
        config: &BlockchainConfig,
        ty: VsetType,
    ) -> Result<Option<(HashBytes, Arc<ValidatorSet>)>> {
        let Some(vset) = match ty {
            VsetType::Prev => config.get_prev_validator_set_raw(),
            VsetType::Current => config.get_current_validator_set().map(Some),
            VsetType::Next => config.get_next_validator_set(),
        }?
        else {
            return Ok(None);
        };

        let vset_hash = vset.repr_hash();

        // TODO: Use fixed array and ArcSwap with compare_exchange
        let res = match self.inner.entry(ty) {
            tycho_util::DashMapEntry::Occupied(mut entry) => {
                let (existing_hash, existing_vset) = entry.get();
                if vset_hash != existing_hash {
                    let vset = Arc::new(vset.parse::<ValidatorSet>()?);
                    entry.insert((*vset_hash, vset.clone()));
                    (*vset_hash, vset)
                } else {
                    (*existing_hash, existing_vset.clone())
                }
            }
            tycho_util::DashMapEntry::Vacant(entry) => {
                let vset = Arc::new(vset.parse::<ValidatorSet>()?);
                entry.insert((*vset_hash, vset.clone()));
                (*vset_hash, vset)
            }
        };

        Ok(Some(res))
    }
}

#[derive(Clone, Copy, Hash, Eq, PartialEq, PartialOrd, Ord)]
enum VsetType {
    Prev,
    Current,
    Next,
}
