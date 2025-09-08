use std::collections::BTreeMap;

use anyhow::Result;
use serde::Serialize;
use tycho_collator::types::IntAdrExt;
use tycho_crypto::ed25519;
use tycho_types::abi::extend_signature_with_id;
use tycho_types::cell::RefsIter;
use tycho_types::models::{
    Account, AccountState, BlockchainConfig, BlockchainConfigParams, ConfigProposalSetup,
    ValidatorSet,
};
use tycho_types::num::Tokens;
use tycho_types::prelude::*;
use tycho_util::FastHashSet;
use tycho_vm::tuple;

use super::getter::{AccountExt, FromStackValue, LazyTuple, LispList};

pub struct ConfigContract<'a>(pub &'a Account);

impl ConfigContract<'_> {
    pub const PARAM_IDX_OWN_PUBKEY: i32 = -999;
    pub const PARAM_IDX_CONFIG_CODE: i32 = -1000;
    pub const PARAM_IDX_ELECTOR_CODE: i32 = -1001;

    pub fn make_blockchain_config(&self) -> Result<BlockchainConfig> {
        if let AccountState::Active(state) = &self.0.state
            && let Some(data) = &state.data
        {
            let params_root = data.as_slice()?.load_reference_cloned()?;
            return Ok(BlockchainConfig {
                address: self.0.address.get_address(),
                params: BlockchainConfigParams::from_raw(params_root),
            });
        }

        anyhow::bail!("invalid config account state");
    }

    pub fn get_proposals(&self) -> Result<BTreeMap<HashBytes, ConfigProposal>> {
        let config = self.make_blockchain_config()?;
        self.0
            .bind(&config)
            .call_getter("list_proposals", Vec::new())?
            .parse()
            .map(|LispList::<(HashBytes, ConfigProposal)>(proposals)| {
                BTreeMap::from_iter(proposals)
            })
    }

    pub fn get_proposal(&self, proposal_hash: &HashBytes) -> Result<Option<ConfigProposal>> {
        let config = self.make_blockchain_config()?;
        self.0
            .bind(&config)
            .call_getter("get_proposal", tuple![int proposal_hash.as_bigint()])?
            .parse()
    }

    pub fn compute_proposal_price(
        proposal_config: &ConfigProposalSetup,
        mut ttl_seconds: u32,
        value: Option<&DynCell>,
    ) -> Result<Tokens> {
        let min = std::cmp::min(proposal_config.min_store_sec, proposal_config.max_store_sec);
        ttl_seconds = ttl_seconds.clamp(min, proposal_config.max_store_sec);

        let mut storage = StorageStatExt::with_limit(1024);
        if let Some(value) = value {
            anyhow::ensure!(value.repr_depth() < 128, "too deep value");
            anyhow::ensure!(storage.add_cell(value), "too big value");
        };

        let CellTreeStatsExt {
            bit_count,
            ref_count,
            ..
        } = storage.stats;

        Ok(Tokens::new(
            ttl_seconds as u128
                * (proposal_config.bit_price as u128 * (bit_count as u128 + 1024)
                    + proposal_config.cell_price as u128 * (ref_count as u128 + 2)),
        ))
    }

    pub fn create_proposal_raw_content(
        param_idx: i32,
        param_value: Option<Cell>,
        prev_param_value_hash: Option<&HashBytes>,
    ) -> Cell {
        const TAG: u8 = 0xf3;

        CellBuilder::build_from((TAG, param_idx, param_value, prev_param_value_hash)).unwrap()
    }

    pub fn create_proposal_payload(
        query_id: u64,
        ttl: u32,
        raw_content: Cell,
        is_critical: bool,
    ) -> Cell {
        const OP: u32 = 0x6e565052;

        CellBuilder::build_from((OP, query_id, ttl, raw_content, is_critical)).unwrap()
    }

    pub fn create_vote_payload(
        query_id: u64,
        validator_idx: u16,
        proposal_hash: &HashBytes,
        keypair: &ed25519::KeyPair,
        signature_id: Option<i32>,
    ) -> Cell {
        const TAG: u32 = 0x566f7465;
        const SIGNED_VOTE_TAG: u32 = 0x566f7445;

        let mut to_sign = Vec::with_capacity(4 + 2 + 32);
        to_sign.extend_from_slice(&SIGNED_VOTE_TAG.to_be_bytes());
        to_sign.extend_from_slice(&validator_idx.to_be_bytes());
        to_sign.extend_from_slice(proposal_hash.as_slice());

        let signature = {
            let to_sign = extend_signature_with_id(&to_sign, signature_id);
            keypair.sign_raw(&to_sign)
        };

        let mut b = CellBuilder::new();
        b.store_u32(TAG).unwrap();
        b.store_u64(query_id).unwrap();
        b.store_raw(&signature, 512).unwrap();
        b.store_raw(&to_sign, to_sign.len() as u16 * 8).unwrap();
        b.build().unwrap()
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ConfigProposal {
    pub expire_at: u32,
    pub is_critical: bool,
    pub param_idx: i32,
    #[serde(with = "Boc")]
    pub param_value: Option<Cell>,
    pub prev_param_value_hash: Option<HashBytes>,
    pub vset_id: HashBytes,
    pub voters: Vec<u16>,
    pub weight_remaining: i64,
    pub rounds_remaining: u8,
    pub losses: u8,
    pub wins: u8,
}

impl ConfigProposal {
    pub fn resolve_validator_idx(
        &self,
        params: &BlockchainConfigParams,
        pubkey: &ed25519::PublicKey,
    ) -> Result<u16> {
        let vset = 'vset: {
            for idx in 32..38 {
                if let Some(vset) = params.as_dict().get(idx)?
                    && vset.repr_hash() == &self.vset_id
                {
                    break 'vset vset.parse::<ValidatorSet>()?;
                }
            }
            anyhow::bail!("no validator set found with hash {}", self.vset_id);
        };

        for (idx, item) in vset.list.iter().enumerate() {
            if &item.public_key == HashBytes::wrap(pubkey.as_bytes()) {
                return u16::try_from(idx).map_err(|_e| anyhow::anyhow!("invalid validator idx"));
            }
        }
        anyhow::bail!(
            "no public key {pubkey} found in the validator set {}",
            self.vset_id
        );
    }
}

impl FromStackValue for ConfigProposal {
    fn from_stack_value(value: tycho_vm::RcStackValue) -> Result<Self> {
        let mut tuple = LazyTuple::from_stack_value(value)?;

        let expire_at = tuple.read_next::<u32>()?;
        let is_critical = tuple.read_next::<bool>()?;
        let mut content = tuple.read_next::<LazyTuple>()?;

        Ok(Self {
            expire_at,
            is_critical,
            param_idx: content.read_next()?,
            param_value: content.read_next()?,
            prev_param_value_hash: {
                let (sign, int) = content.read_next::<num_bigint::BigInt>()?.into_parts();
                if sign == num_bigint::Sign::Minus {
                    None
                } else {
                    Some(HashBytes::from_biguint_lossy(&int))
                }
            },
            vset_id: tuple.read_next()?,
            voters: tuple.read_next().map(LispList::into_inner)?,
            weight_remaining: tuple.read_next()?,
            rounds_remaining: tuple.read_next()?,
            losses: tuple.read_next()?,
            wins: tuple.read_next()?,
        })
    }
}

struct StorageStatExt<'a> {
    visited: FastHashSet<&'a HashBytes>,
    stack: Vec<RefsIter<'a>>,
    stats: CellTreeStatsExt,
    limit: u64,
}

impl<'a> StorageStatExt<'a> {
    pub fn with_limit(limit: u64) -> Self {
        Self {
            visited: Default::default(),
            stack: Vec::new(),
            stats: Default::default(),
            limit,
        }
    }

    fn add_cell(&mut self, cell: &'a DynCell) -> bool {
        if !self.visited.insert(cell.repr_hash()) {
            return true;
        }
        if self.stats.cell_count >= self.limit {
            return false;
        }

        let refs = cell.references();

        self.stats.bit_count += cell.bit_len() as u64;
        self.stats.ref_count += refs.len() as u64;
        self.stats.cell_count += 1;

        self.stack.clear();
        self.stack.push(refs);
        self.reduce_stack()
    }

    fn reduce_stack(&mut self) -> bool {
        'outer: while let Some(item) = self.stack.last_mut() {
            for cell in item.by_ref() {
                if !self.visited.insert(cell.repr_hash()) {
                    continue;
                }

                if self.stats.cell_count >= self.limit {
                    return false;
                }

                let next = cell.references();
                let ref_count = next.len();

                self.stats.bit_count += cell.bit_len() as u64;
                self.stats.ref_count += ref_count as u64;
                self.stats.cell_count += 1;

                if ref_count > 0 {
                    self.stack.push(next);
                    continue 'outer;
                }
            }

            self.stack.pop();
        }

        true
    }
}

#[derive(Default)]
struct CellTreeStatsExt {
    bit_count: u64,
    ref_count: u64,
    cell_count: u64,
}
