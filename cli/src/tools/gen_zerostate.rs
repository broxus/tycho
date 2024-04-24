use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::Result;
use everscale_crypto::ed25519;
use everscale_types::models::*;
use everscale_types::num::Tokens;
use everscale_types::prelude::*;
use serde::{Deserialize, Serialize};
use tycho_util::serde_helpers;

use crate::util::compute_storage_used;

/// Generate a zero state for a network.
#[derive(clap::Parser)]
pub struct Cmd {
    /// dump the template of the zero state config
    #[clap(short = 'i', long, exclusive = true)]
    init_config: Option<String>,

    /// path to the zero state config
    #[clap(required_unless_present = "init_config")]
    config: Option<PathBuf>,

    /// path to the output file
    #[clap(short, long, required_unless_present = "init_config")]
    output: Option<PathBuf>,

    /// explicit unix timestamp of the zero state
    #[clap(long)]
    now: Option<u32>,
}

impl Cmd {
    pub fn run(self) -> Result<()> {
        match self.init_config {
            Some(path) => {
                let config = ZerostateConfig::default();
                std::fs::write(path, serde_json::to_string_pretty(&config).unwrap())?;
                Ok(())
            }
            None => Ok(()),
        }
    }
}

#[derive(Serialize, Deserialize)]
struct ZerostateConfig {
    global_id: i32,

    #[serde(with = "serde_helpers::public_key")]
    config_public_key: ed25519::PublicKey,
    #[serde(with = "serde_helpers::public_key")]
    minter_public_key: ed25519::PublicKey,

    #[serde(with = "serde_account_states")]
    accounts: HashMap<HashBytes, OptionalAccount>,

    params: BlockchainConfigParams,
}

impl Default for ZerostateConfig {
    fn default() -> Self {
        Self {
            global_id: 0,
            config_public_key: ed25519::PublicKey::from_bytes([0; 32]).unwrap(),
            minter_public_key: ed25519::PublicKey::from_bytes([0; 32]).unwrap(),
            accounts: Default::default(),
            params: make_default_params().unwrap(),
        }
    }
}

fn make_default_params() -> Result<BlockchainConfigParams> {
    let mut params = BlockchainConfig::new_empty(HashBytes([0x55; 32])).params;

    // Param 1
    params.set_elector_address(&HashBytes([0x33; 32]))?;

    // Param 2
    params.set_minter_address(&HashBytes([0x00; 32]))?;

    // Param 8
    params.set_global_version(&GlobalVersion {
        version: 32,
        capabilities: GlobalCapabilities::from([
            GlobalCapability::CapCreateStatsEnabled,
            GlobalCapability::CapBounceMsgBody,
            GlobalCapability::CapReportVersion,
            GlobalCapability::CapShortDequeue,
            GlobalCapability::CapFastStorageStat,
            GlobalCapability::CapOffHypercube,
            GlobalCapability::CapMyCode,
            GlobalCapability::CapFixTupleIndexBug,
        ]),
    })?;

    // Param 9
    params.set_mandatory_params(&[
        0, 1, 9, 10, 12, 14, 15, 16, 17, 18, 20, 21, 22, 23, 24, 25, 28, 34,
    ])?;

    // Param 10
    params.set_critical_params(&[0, 1, 9, 10, 12, 14, 15, 16, 17, 32, 34, 36])?;

    // Param 11
    params.set::<ConfigParam11>(&ConfigVotingSetup {
        normal_params: Lazy::new(&ConfigProposalSetup {
            min_total_rounds: 2,
            max_total_rounds: 3,
            min_wins: 2,
            max_losses: 2,
            min_store_sec: 1000000,
            max_store_sec: 10000000,
            bit_price: 1,
            cell_price: 500,
        })?,
        critical_params: Lazy::new(&ConfigProposalSetup {
            min_total_rounds: 4,
            max_total_rounds: 7,
            min_wins: 4,
            max_losses: 2,
            min_store_sec: 5000000,
            max_store_sec: 20000000,
            bit_price: 2,
            cell_price: 1000,
        })?,
    })?;

    // Param 12 will always be overwritten

    // Param 14
    params.set_block_creation_rewards(&BlockCreationRewards {
        masterchain_block_fee: Tokens::new(1700000000),
        basechain_block_fee: Tokens::new(1000000000),
    })?;

    // Param 15
    params.set_election_timings(&ElectionTimings {
        validators_elected_for: 65536,
        elections_start_before: 32768,
        elections_end_before: 8192,
        stake_held_for: 32768,
    })?;

    // Param 16
    params.set_validator_count_params(&ValidatorCountParams {
        max_validators: 1000,
        max_main_validators: 100,
        min_validators: 13,
    })?;

    // Param 17
    params.set_validator_stake_params(&ValidatorStakeParams {
        min_stake: Tokens::new(10000000000000),
        max_stake: Tokens::new(10000000000000000),
        min_total_stake: Tokens::new(100000000000000),
        max_stake_factor: 196608,
    })?;

    // Param 18
    params.set_storage_prices(&[StoragePrices {
        utime_since: 0,
        bit_price_ps: 1,
        cell_price_ps: 500,
        mc_bit_price_ps: 1000,
        mc_cell_price_ps: 500000,
    }])?;

    // Param 20 (masterchain)
    params.set_gas_prices(
        true,
        &GasLimitsPrices {
            gas_price: 655360000,
            gas_limit: 1000000,
            special_gas_limit: 100000000,
            gas_credit: 10000,
            block_gas_limit: 11000000,
            freeze_due_limit: 100000000,
            delete_due_limit: 1000000000,
            flat_gas_limit: 1000,
            flat_gas_price: 10000000,
        },
    )?;

    // Param 21 (basechain)
    params.set_gas_prices(
        false,
        &GasLimitsPrices {
            gas_price: 65536000,
            gas_limit: 1000000,
            special_gas_limit: 1000000,
            gas_credit: 10000,
            block_gas_limit: 10000000,
            freeze_due_limit: 100000000,
            delete_due_limit: 1000000000,
            flat_gas_limit: 1000,
            flat_gas_price: 1000000,
        },
    )?;

    // Param 22 (masterchain)
    params.set_block_limits(
        true,
        &BlockLimits {
            bytes: BlockParamLimits {
                underload: 131072,
                soft_limit: 524288,
                hard_limit: 1048576,
            },
            gas: BlockParamLimits {
                underload: 900000,
                soft_limit: 1200000,
                hard_limit: 2000000,
            },
            lt_delta: BlockParamLimits {
                underload: 1000,
                soft_limit: 5000,
                hard_limit: 10000,
            },
        },
    )?;

    // Param 23 (basechain)
    params.set_block_limits(
        true,
        &BlockLimits {
            bytes: BlockParamLimits {
                underload: 131072,
                soft_limit: 524288,
                hard_limit: 1048576,
            },
            gas: BlockParamLimits {
                underload: 900000,
                soft_limit: 1200000,
                hard_limit: 2000000,
            },
            lt_delta: BlockParamLimits {
                underload: 1000,
                soft_limit: 5000,
                hard_limit: 10000,
            },
        },
    )?;

    // Param 24 (masterchain)
    params.set_msg_forward_prices(
        true,
        &MsgForwardPrices {
            lump_price: 10000000,
            bit_price: 655360000,
            cell_price: 65536000000,
            ihr_price_factor: 98304,
            first_frac: 21845,
            next_frac: 21845,
        },
    )?;

    // Param 25 (basechain)
    params.set_msg_forward_prices(
        false,
        &MsgForwardPrices {
            lump_price: 1000000,
            bit_price: 65536000,
            cell_price: 6553600000,
            ihr_price_factor: 98304,
            first_frac: 21845,
            next_frac: 21845,
        },
    )?;

    // Param 28
    params.set_catchain_config(&CatchainConfig {
        isolate_mc_validators: false,
        shuffle_mc_validators: true,
        mc_catchain_lifetime: 250,
        shard_catchain_lifetime: 250,
        shard_validators_lifetime: 1000,
        shard_validators_num: 11,
    })?;

    // Param 29
    params.set_consensus_config(&ConsensusConfig {
        new_catchain_ids: true,
        round_candidates: 3.try_into().unwrap(),
        next_candidate_delay_ms: 2000,
        consensus_timeout_ms: 16000,
        fast_attempts: 3,
        attempt_duration: 8,
        catchain_max_deps: 4,
        max_block_bytes: 2097152,
        max_collated_bytes: 2097152,
    })?;

    // Param 31
    params.set_fundamental_addresses(&[
        HashBytes([0x00; 32]),
        HashBytes([0x33; 32]),
        HashBytes([0x55; 32]),
    ])?;

    Ok(params)
}

fn build_minter_account(pubkey: &ed25519::PublicKey) -> Result<Account> {
    const MINTER_STATE: &[u8] = include_bytes!("../../res/minter_state.boc");

    let mut account = BocRepr::decode::<OptionalAccount, _>(MINTER_STATE)?
        .0
        .expect("invalid minter state");

    match &mut account.state {
        AccountState::Active(state_init) => {
            let mut data = CellBuilder::new();

            // Append pubkey first
            data.store_u256(HashBytes::wrap(pubkey.as_bytes()))?;

            // Append everything except the pubkey
            let prev_data = state_init
                .data
                .take()
                .expect("minter state must contain data");
            let mut prev_data = prev_data.as_slice()?;
            prev_data.advance(256, 0)?;

            data.store_slice(prev_data)?;

            // Update data
            state_init.data = Some(data.build()?);
        }
        _ => unreachable!("saved state is for the active account"),
    };

    account.balance = CurrencyCollection::ZERO;
    account.storage_stat.used = compute_storage_used(&account)?;

    Ok(account)
}

fn build_config_account(
    pubkey: &ed25519::PublicKey,
    address: &HashBytes,
    balance: Tokens,
) -> Result<Account> {
    const CONFIG_CODE: &[u8] = include_bytes!("../../res/config_code.boc");

    let code = Boc::decode(CONFIG_CODE)?;

    let mut data = CellBuilder::new();
    data.store_reference(Cell::empty_cell())?;
    data.store_u32(0)?;
    data.store_u256(HashBytes::wrap(pubkey.as_bytes()))?;
    data.store_bit_zero()?;
    let data = data.build()?;

    let mut account = Account {
        address: StdAddr::new(-1, *address).into(),
        storage_stat: Default::default(),
        last_trans_lt: 0,
        balance: balance.into(),
        state: AccountState::Active(StateInit {
            split_depth: None,
            special: Some(SpecialFlags {
                tick: false,
                tock: true,
            }),
            code: Some(code),
            data: Some(data),
            libraries: Dict::new(),
        }),
        init_code_hash: None,
    };

    account.storage_stat.used = compute_storage_used(&account)?;

    Ok(account)
}

fn build_elector_code(address: &HashBytes, balance: Tokens) -> Result<Account> {
    const ELECTOR_CODE: &[u8] = include_bytes!("../../res/elector_code.boc");

    let code = Boc::decode(ELECTOR_CODE)?;

    let mut data = CellBuilder::new();
    data.store_small_uint(0, 3)?; //empty dict, empty dict, empty dict
    data.store_small_uint(0, 4)?; // tokens
    data.store_u32(0)?; // elections id
    data.store_zeros(256)?; // elections hash
    let data = data.build()?;

    let mut account = Account {
        address: StdAddr::new(-1, *address).into(),
        storage_stat: Default::default(),
        last_trans_lt: 0,
        balance: balance.into(),
        state: AccountState::Active(StateInit {
            split_depth: None,
            special: Some(SpecialFlags {
                tick: true,
                tock: false,
            }),
            code: Some(code),
            data: Some(data),
            libraries: Dict::new(),
        }),
        init_code_hash: None,
    };

    account.storage_stat.used = compute_storage_used(&account)?;

    Ok(account)
}

mod serde_account_states {
    use std::collections::HashMap;

    use everscale_types::boc::BocRepr;
    use everscale_types::cell::HashBytes;
    use everscale_types::models::OptionalAccount;
    use serde::de::Deserializer;
    use serde::ser::{SerializeMap, Serializer};
    use serde::{Deserialize, Serialize};

    pub fn serialize<S>(
        value: &HashMap<HashBytes, OptionalAccount>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        #[repr(transparent)]
        struct WrapperValue<'a>(
            #[serde(serialize_with = "BocRepr::serialize")] &'a OptionalAccount,
        );

        let mut ser = serializer.serialize_map(Some(value.len()))?;
        for (key, value) in value {
            ser.serialize_entry(key, &WrapperValue(value))?;
        }
        ser.end()
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<HashMap<HashBytes, OptionalAccount>, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[repr(transparent)]
        struct WrappedValue(#[serde(with = "BocRepr")] OptionalAccount);

        <HashMap<HashBytes, WrappedValue>>::deserialize(deserializer)
            .map(|map| map.into_iter().map(|(k, v)| (k, v.0)).collect())
    }
}
