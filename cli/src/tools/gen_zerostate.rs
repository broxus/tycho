use std::collections::HashMap;
use std::io::IsTerminal;
use std::path::PathBuf;
use std::sync::OnceLock;

use anyhow::Result;
use everscale_crypto::ed25519;
use everscale_types::models::*;
use everscale_types::num::Tokens;
use everscale_types::prelude::*;
use serde::{Deserialize, Serialize};
use sha2::Digest;

use crate::util::compute_storage_used;

/// Generate a zero state for a network.
#[derive(clap::Parser)]
pub struct Cmd {
    /// dump the template of the zero state config
    #[clap(short = 'i', long, exclusive = true)]
    init_config: Option<PathBuf>,

    /// path to the zero state config
    #[clap(required_unless_present = "init_config")]
    config: Option<PathBuf>,

    /// path to the output file
    #[clap(short, long, required_unless_present = "init_config")]
    output: Option<PathBuf>,

    /// explicit unix timestamp of the zero state
    #[clap(long)]
    now: Option<u32>,

    #[clap(short, long)]
    force: bool,
}

impl Cmd {
    pub fn run(self) -> Result<()> {
        match self.init_config {
            Some(path) => write_default_config(&path, self.force),
            None => generate_zerostate(
                &self.config.unwrap(),
                &self.output.unwrap(),
                self.now.unwrap_or_else(tycho_util::time::now_sec),
                self.force,
            ),
        }
    }
}

fn write_default_config(config_path: &PathBuf, force: bool) -> Result<()> {
    if config_path.exists() && !force {
        anyhow::bail!("config file already exists, use --force to overwrite");
    }

    let config = ZerostateConfig::default();
    std::fs::write(config_path, serde_json::to_string_pretty(&config).unwrap())?;
    Ok(())
}

fn generate_zerostate(
    config_path: &PathBuf,
    output_path: &PathBuf,
    now: u32,
    force: bool,
) -> Result<()> {
    if output_path.exists() && !force {
        anyhow::bail!("output file already exists, use --force to overwrite");
    }

    let mut config = {
        let data = std::fs::read_to_string(config_path)?;
        let de = &mut serde_json::Deserializer::from_str(&data);
        serde_path_to_error::deserialize::<_, ZerostateConfig>(de)?
    };

    config
        .prepare_config_params(now)
        .map_err(|e| GenError::new("validator config is invalid", e))?;

    config
        .add_required_accounts()
        .map_err(|e| GenError::new("failed to add required accounts", e))?;

    let state = config
        .build_masterchain_state(now)
        .map_err(|e| GenError::new("failed to build masterchain zerostate", e))?;

    let boc = CellBuilder::build_from(&state)
        .map_err(|e| GenError::new("failed to serialize zerostate", e))?;

    let root_hash = *boc.repr_hash();
    let data = Boc::encode(&boc);
    let file_hash = HashBytes::from(sha2::Sha256::digest(&data));

    std::fs::write(output_path, data)
        .map_err(|e| GenError::new("failed to write masterchain zerostate", e))?;

    let hashes = serde_json::json!({
        "root_hash": root_hash,
        "file_hash": file_hash,
    });

    let output = if std::io::stdin().is_terminal() {
        serde_json::to_string_pretty(&hashes)
    } else {
        serde_json::to_string(&hashes)
    }?;
    println!("{output}");
    Ok(())
}

#[derive(Serialize, Deserialize)]
struct ZerostateConfig {
    global_id: i32,

    config_public_key: ed25519::PublicKey,
    #[serde(default)]
    minter_public_key: Option<ed25519::PublicKey>,

    config_balance: Tokens,
    elector_balance: Tokens,

    #[serde(with = "serde_account_states")]
    accounts: HashMap<HashBytes, OptionalAccount>,

    validators: Vec<ed25519::PublicKey>,

    params: BlockchainConfigParams,
}

impl ZerostateConfig {
    fn prepare_config_params(&mut self, now: u32) -> Result<()> {
        let Some(config_address) = self.params.get::<ConfigParam0>()? else {
            anyhow::bail!("config address is not set (param 0)");
        };
        let Some(elector_address) = self.params.get::<ConfigParam1>()? else {
            anyhow::bail!("elector address is not set (param 1)");
        };
        let minter_address = self.params.get::<ConfigParam2>()?;

        if self.params.get::<ConfigParam7>()?.is_none() {
            self.params
                .set::<ConfigParam7>(&ExtraCurrencyCollection::new())?;
        }

        anyhow::ensure!(
            self.params.get::<ConfigParam9>()?.is_some(),
            "required params list is required (param 9)"
        );

        {
            let Some(mut workchains) = self.params.get::<ConfigParam12>()? else {
                anyhow::bail!("workchains are not set (param 12)");
            };

            let mut updated = false;
            for entry in workchains.clone().iter() {
                let (id, mut workchain) = entry?;
                anyhow::ensure!(
                    id != ShardIdent::MASTERCHAIN.workchain(),
                    "masterchain is not configurable"
                );

                if workchain.zerostate_root_hash != HashBytes::ZERO {
                    continue;
                }

                let shard_ident = ShardIdent::new_full(id);
                let shard_state = make_shard_state(self.global_id, shard_ident, now);

                let cell = CellBuilder::build_from(&shard_state)?;
                workchain.zerostate_root_hash = *cell.repr_hash();
                let bytes = Boc::encode(&cell);
                workchain.zerostate_file_hash = sha2::Sha256::digest(bytes).into();

                workchains.set(id, &workchain)?;
                updated = true;
            }

            if updated {
                self.params.set_workchains(&workchains)?;
            }
        }

        {
            let mut fundamental_addresses = self.params.get::<ConfigParam31>()?.unwrap_or_default();
            fundamental_addresses.set(config_address, ())?;
            fundamental_addresses.set(elector_address, ())?;
            if let Some(minter_address) = minter_address {
                fundamental_addresses.set(minter_address, ())?;
            }
            self.params.set::<ConfigParam31>(&fundamental_addresses)?;
        }

        for id in 32..=37 {
            anyhow::ensure!(
                !self.params.contains_raw(id)?,
                "config param {id} must not be set manually as it is managed by the tool"
            );
        }

        {
            const VALIDATOR_WEIGHT: u64 = 1;

            anyhow::ensure!(!self.validators.is_empty(), "validator set is empty");

            let mut validator_set = ValidatorSet {
                utime_since: now,
                utime_until: now,
                main: (self.validators.len() as u16).try_into().unwrap(),
                total_weight: 0,
                list: Vec::with_capacity(self.validators.len()),
            };
            for pubkey in &self.validators {
                validator_set.list.push(ValidatorDescription {
                    public_key: HashBytes::from(*pubkey.as_bytes()),
                    weight: VALIDATOR_WEIGHT,
                    adnl_addr: None,
                    mc_seqno_since: 0,
                    prev_total_weight: validator_set.total_weight,
                });
                validator_set.total_weight += VALIDATOR_WEIGHT;
            }

            self.params.set::<ConfigParam34>(&validator_set)?;
        }

        let mandatory_params = self.params.get::<ConfigParam9>()?.unwrap();
        for entry in mandatory_params.keys() {
            let id = entry?;
            anyhow::ensure!(
                self.params.contains_raw(id)?,
                "required param {id} is not set"
            );
        }

        Ok(())
    }

    fn add_required_accounts(&mut self) -> Result<()> {
        // Config
        let Some(config_address) = self.params.get::<ConfigParam0>()? else {
            anyhow::bail!("config address is not set (param 0)");
        };
        anyhow::ensure!(
            &self.config_public_key != zero_public_key(),
            "config public key is not set"
        );
        self.accounts.insert(
            config_address,
            build_config_account(
                &self.config_public_key,
                &config_address,
                self.config_balance,
            )?
            .into(),
        );

        // Elector
        let Some(elector_address) = self.params.get::<ConfigParam1>()? else {
            anyhow::bail!("elector address is not set (param 1)");
        };
        self.accounts.insert(
            elector_address,
            build_elector_code(&elector_address, self.elector_balance)?.into(),
        );

        // Minter
        match (&self.minter_public_key, self.params.get::<ConfigParam2>()?) {
            (Some(public_key), Some(minter_address)) => {
                anyhow::ensure!(
                    public_key != zero_public_key(),
                    "minter public key is invalid"
                );
                self.accounts.insert(
                    minter_address,
                    build_minter_account(&public_key, &minter_address)?.into(),
                );
            }
            (None, Some(_)) => anyhow::bail!("minter_public_key is required"),
            (Some(_), None) => anyhow::bail!("minter address is not set (param 2)"),
            (None, None) => {}
        }

        // Done
        Ok(())
    }

    fn build_masterchain_state(&self, now: u32) -> Result<ShardStateUnsplit> {
        let mut state = make_shard_state(self.global_id, ShardIdent::MASTERCHAIN, now);

        for account in self.accounts.values() {
            if let Some(account) = account.as_ref() {
                state.total_balance = state
                    .total_balance
                    .checked_add(&account.balance)
                    .map_err(|e| GenError::new("failed ot compute total balance", e))?;
            }
        }

        state.custom = Some(Lazy::new(&McStateExtra {
            shards: Default::default(),
            config: BlockchainConfig {
                address: self.params.get::<ConfigParam0>()?.unwrap(),
                params: self.params.clone(),
            },
            validator_info: ValidatorInfo {
                validator_list_hash_short: 0,
                catchain_seqno: 0,
                nx_cc_updated: true,
            },
            prev_blocks: AugDict::new(),
            after_key_block: true,
            last_key_block: None,
            block_create_stats: None,
            global_balance: state.total_balance.clone(),
            copyleft_rewards: Dict::new(),
        })?);

        Ok(state)
    }
}

impl Default for ZerostateConfig {
    fn default() -> Self {
        Self {
            global_id: 0,
            config_public_key: *zero_public_key(),
            minter_public_key: None,
            config_balance: Tokens::new(500_000_000_000), // 500
            elector_balance: Tokens::new(500_000_000_000), // 500
            accounts: Default::default(),
            validators: Default::default(),
            params: make_default_params().unwrap(),
        }
    }
}

fn make_shard_state(global_id: i32, shard_ident: ShardIdent, now: u32) -> ShardStateUnsplit {
    ShardStateUnsplit {
        global_id,
        shard_ident,
        gen_utime: now,
        min_ref_mc_seqno: u32::MAX,
        ..Default::default()
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

    // Param 12
    {
        let mut workchains = Dict::new();
        workchains.set(
            0,
            WorkchainDescription {
                enabled_since: 0,
                actual_min_split: 0,
                min_split: 0,
                max_split: 3,
                active: true,
                accept_msgs: true,
                zerostate_root_hash: HashBytes::ZERO,
                zerostate_file_hash: HashBytes::ZERO,
                version: 0,
                format: WorkchainFormat::Basic(WorkchainFormatBasic {
                    vm_version: 0,
                    vm_mode: 0,
                }),
            },
        )?;
        params.set::<ConfigParam12>(&workchains)?;
    }

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
        false,
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
    data.store_u256(pubkey)?;
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

fn build_minter_account(pubkey: &ed25519::PublicKey, address: &HashBytes) -> Result<Account> {
    const MINTER_STATE: &[u8] = include_bytes!("../../res/minter_state.boc");

    let mut account = BocRepr::decode::<OptionalAccount, _>(MINTER_STATE)?
        .0
        .expect("invalid minter state");

    match &mut account.state {
        AccountState::Active(state_init) => {
            // Append everything except the pubkey
            let mut data = CellBuilder::new();
            data.store_u32(0)?;
            data.store_u256(pubkey)?;

            // Update data
            state_init.data = Some(data.build()?);
        }
        _ => unreachable!("saved state is for the active account"),
    };

    account.address = StdAddr::new(-1, *address).into();
    account.balance = CurrencyCollection::ZERO;
    account.storage_stat.used = compute_storage_used(&account)?;

    Ok(account)
}

fn zero_public_key() -> &'static ed25519::PublicKey {
    static KEY: OnceLock<ed25519::PublicKey> = OnceLock::new();
    KEY.get_or_init(|| ed25519::PublicKey::from_bytes([0; 32]).unwrap())
}

#[derive(thiserror::Error, Debug)]
#[error("{context}: {source}")]
struct GenError {
    context: String,
    #[source]
    source: anyhow::Error,
}

impl GenError {
    fn new(context: impl Into<String>, source: impl Into<anyhow::Error>) -> Self {
        Self {
            context: context.into(),
            source: source.into(),
        }
    }
}

mod serde_account_states {
    use super::*;

    use serde::de::Deserializer;
    use serde::ser::{SerializeMap, Serializer};

    pub fn serialize<S>(
        value: &HashMap<HashBytes, OptionalAccount>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        #[repr(transparent)]
        struct WrapperValue<'a>(#[serde(with = "BocRepr")] &'a OptionalAccount);

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
