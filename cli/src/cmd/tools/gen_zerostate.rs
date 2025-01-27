use std::path::PathBuf;
use std::sync::OnceLock;

use anyhow::{Context, Result};
use everscale_crypto::ed25519;
use everscale_types::models::*;
use everscale_types::num::Tokens;
use everscale_types::prelude::*;
use serde::{Deserialize, Serialize};
use tycho_util::{FastHashMap, FastHashSet};

use crate::util::{compute_storage_used, print_json};

/// Generate a zero state for a network.
#[derive(clap::Parser)]
pub struct Cmd {
    /// dump the template of the zero state config
    #[clap(
        short = 'i',
        long,
        conflicts_with_all = ["config", "output", "now"]
    )]
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
        .context("validator config is invalid")?;

    config
        .add_required_accounts()
        .context("failed to add required accounts")?;

    let state = config
        .build_masterchain_state(now)
        .context("failed to build masterchain zerostate")?;

    let boc = CellBuilder::build_from(&state).context("failed to serialize zerostate")?;

    let root_hash = *boc.repr_hash();
    let data = Boc::encode(&boc);
    let file_hash = Boc::file_hash_blake(&data);

    std::fs::write(output_path, data).context("failed to write masterchain zerostate")?;

    let hashes = serde_json::json!({
        "root_hash": root_hash,
        "file_hash": file_hash,
    });

    print_json(hashes)
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
    accounts: FastHashMap<HashBytes, OptionalAccount>,

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
                workchain.zerostate_file_hash = Boc::file_hash_blake(bytes);

                workchains.set(id, &workchain)?;
                updated = true;
            }

            if updated {
                self.params.set_workchains(&workchains)?;
            }
        }

        match self.params.get::<ConfigParam19>()? {
            None => {
                self.params.set_global_id(self.global_id)?;
            }
            Some(existing) => {
                anyhow::ensure!(
                    existing == self.global_id,
                    "global id mismatch in config parm 19"
                );
            }
        }

        {
            let mut fundamental_addresses = self.params.get::<ConfigParam31>()?.unwrap_or_default();

            // NOTE: Config address is handled separately and must not be in the list
            fundamental_addresses.remove(config_address)?;

            // Ensure that the elector and minter are in the list
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

            let max_main_validators = self
                .params
                .get_validator_count_params()?
                .max_main_validators;
            let mut validator_set = ValidatorSet {
                utime_since: now,
                utime_until: now,
                main: std::cmp::min(self.validators.len() as u16, max_main_validators)
                    .try_into()
                    .unwrap(),
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
                    build_minter_account(public_key, &minter_address)?.into(),
                );
            }
            (None, Some(_)) => anyhow::bail!("minter_public_key is required"),
            (Some(_), None) => anyhow::bail!("minter address is not set (param 2)"),
            (None, None) => {}
        }

        // Done
        Ok(())
    }

    fn build_masterchain_state(self, now: u32) -> Result<ShardStateUnsplit> {
        let mut state = make_shard_state(self.global_id, ShardIdent::MASTERCHAIN, now);

        let config = BlockchainConfig {
            address: self.params.get::<ConfigParam0>()?.unwrap(),
            params: self.params.clone(),
        };

        {
            let mut accounts = ShardAccounts::new();
            let mut libraries = FastHashMap::<HashBytes, (Cell, FastHashSet<HashBytes>)>::default();
            for (account, mut account_state) in self.accounts {
                let balance = match account_state.as_mut() {
                    Some(state) => {
                        if let AccountState::Active(state) = &state.state {
                            for entry in state.libraries.iter() {
                                let (hash, lib) = entry?;
                                let (prev_root, publishers) = libraries
                                    .entry(hash)
                                    .or_insert_with(|| (lib.root.clone(), Default::default()));
                                publishers.insert(account);
                                anyhow::ensure!(
                                    prev_root == &lib.root,
                                    "Multiple library roots is forbidden for the same lib hash"
                                );
                            }
                        }

                        // Always ensure that the account storage stat is up-to-date
                        state.address = StdAddr::new(-1, account).into();
                        state.storage_stat.used = compute_storage_used(state)?;
                        state.balance.clone()
                    }
                    None => continue,
                };

                let account_state_cell = Lazy::new(&account_state)?;

                state.total_balance = state
                    .total_balance
                    .checked_add(&balance)
                    .context("failed ot compute total balance")?;

                accounts.set(
                    account,
                    DepthBalanceInfo {
                        balance,
                        split_depth: 0,
                    },
                    ShardAccount {
                        account: account_state_cell,
                        last_trans_hash: HashBytes::ZERO,
                        last_trans_lt: 0,
                    },
                )?;
            }

            // Update the config account
            update_config_account(&mut accounts, &config)?;

            assert_eq!(state.total_balance, accounts.root_extra().balance);
            state.accounts = Lazy::new(&accounts)?;

            // Build lib dict
            let mut libs = Dict::new();
            for (hash, (lib, publishers)) in libraries {
                let mut publishers = publishers
                    .into_iter()
                    .map(|hash| (hash, ()))
                    .collect::<Vec<_>>();
                publishers.sort_unstable();
                libs.set(hash, LibDescr {
                    lib,
                    publishers: Dict::try_from_sorted_slice(&publishers)?,
                })?;
            }

            state.libraries = libs;
        }

        let workchains = self.params.get::<ConfigParam12>()?.unwrap();
        let mut shards = Vec::new();
        for entry in workchains.iter() {
            let (workchain, descr) = entry?;
            shards.push((ShardIdent::new_full(workchain), ShardDescription {
                seqno: 0,
                reg_mc_seqno: 0,
                start_lt: 0,
                end_lt: 0,
                root_hash: descr.zerostate_root_hash,
                file_hash: descr.zerostate_file_hash,
                before_split: false,
                before_merge: false,
                want_split: false,
                want_merge: false,
                nx_cc_updated: true,
                next_catchain_seqno: 0,
                ext_processed_to_anchor_id: 0,
                top_sc_block_updated: false,
                min_ref_mc_seqno: u32::MAX,
                gen_utime: now,
                split_merge_at: None,
                fees_collected: CurrencyCollection::ZERO,
                funds_created: CurrencyCollection::ZERO,
                copyleft_rewards: Dict::new(),
                proof_chain: None,
            }));
        }

        let curr_vset = self.params.get_current_validator_set()?;
        let collation_config = self.params.get_collation_config()?;
        let session_seqno = 0;
        let Some((_, validator_list_hash_short)) =
            curr_vset.compute_mc_subset(session_seqno, collation_config.shuffle_mc_validators)
        else {
            anyhow::bail!(
                "Failed to compute a validator subset for zerostate (shard_id = {}, session_seqno = {})",
                ShardIdent::MASTERCHAIN, session_seqno,
            );
        };

        state.custom = Some(Lazy::new(&McStateExtra {
            shards: ShardHashes::from_shards(shards.iter().map(|(ident, descr)| (ident, descr)))?,
            config,
            validator_info: ValidatorInfo {
                validator_list_hash_short,
                catchain_seqno: session_seqno,
                nx_cc_updated: true,
            },
            consensus_info: ConsensusInfo {
                vset_switch_round: session_seqno,
                prev_vset_switch_round: session_seqno,
                genesis_info: GenesisInfo::default(),
                prev_shuffle_mc_validators: collation_config.shuffle_mc_validators,
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
        workchains.set(0, WorkchainDescription {
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
        })?;
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

    // Param 19 will be added during state creation.

    // Param 20 (masterchain)
    params.set_gas_prices(true, &GasLimitsPrices {
        gas_price: 655360000,
        gas_limit: 1000000,
        special_gas_limit: 100000000,
        gas_credit: 10000,
        block_gas_limit: 11000000,
        freeze_due_limit: 100000000,
        delete_due_limit: 1000000000,
        flat_gas_limit: 1000,
        flat_gas_price: 10000000,
    })?;

    // Param 21 (basechain)
    params.set_gas_prices(false, &GasLimitsPrices {
        gas_price: 65536000,
        gas_limit: 1000000,
        special_gas_limit: 1000000,
        gas_credit: 10000,
        block_gas_limit: 10000000,
        freeze_due_limit: 100000000,
        delete_due_limit: 1000000000,
        flat_gas_limit: 1000,
        flat_gas_price: 1000000,
    })?;

    // Param 22 (masterchain)
    params.set_block_limits(true, &BlockLimits {
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
    })?;

    // Param 23 (basechain)
    params.set_block_limits(false, &BlockLimits {
        bytes: BlockParamLimits {
            underload: 131072,
            soft_limit: 524288,
            hard_limit: 1048576,
        },
        gas: BlockParamLimits {
            underload: 900000,
            soft_limit: 1200000,
            hard_limit: 80_000_000,
        },
        lt_delta: BlockParamLimits {
            underload: 1000,
            soft_limit: 5000,
            hard_limit: 10000,
        },
    })?;

    // Param 24 (masterchain)
    params.set_msg_forward_prices(true, &MsgForwardPrices {
        lump_price: 10000000,
        bit_price: 655360000,
        cell_price: 65536000000,
        ihr_price_factor: 98304,
        first_frac: 21845,
        next_frac: 21845,
    })?;

    // Param 25 (basechain)
    params.set_msg_forward_prices(false, &MsgForwardPrices {
        lump_price: 1000000,
        bit_price: 65536000,
        cell_price: 6553600000,
        ihr_price_factor: 98304,
        first_frac: 21845,
        next_frac: 21845,
    })?;

    // Param 28
    let mut group_slots_fractions = Dict::<u16, u8>::new();
    group_slots_fractions.set(0, 80)?;
    group_slots_fractions.set(1, 10)?;

    params.set_collation_config(&CollationConfig {
        shuffle_mc_validators: true,

        mc_block_min_interval_ms: 2500,
        empty_sc_block_interval_ms: 60_000,

        max_uncommitted_chain_length: 31,

        msgs_exec_params: MsgsExecutionParams {
            buffer_limit: 10_000,
            group_limit: 100,
            group_vert_size: 10,
            externals_expire_timeout: 60,
            open_ranges_limit: 100,
            par_0_int_msgs_count_limit: 50_000,
            par_0_ext_msgs_count_limit: 5_000,
            group_slots_fractions,
        },

        wu_used_to_import_next_anchor: 1_200_000_000,

        work_units_params: WorkUnitsParams {
            prepare: WorkUnitsParamsPrepare {
                fixed_part: 500_000, // 500 ns
                msgs_stats: 0,
                remaning_msgs_stats: 0,
                read_ext_msgs: 200,     // 200 ns
                read_int_msgs: 5_000,   // 5 mcs
                read_new_msgs: 500,     // 500 ns
                add_to_msg_groups: 150, // 150 ns
            },
            execute: WorkUnitsParamsExecute {
                prepare: 114_000,          // 114 mcs
                execute_err: 6_000,        // 6 mcs
                execute: 25_000,           // 25 mcs
                execute_delimiter: 10_000, //
                serialize_enqueue: 3_000,  // 3 mcs
                serialize_dequeue: 3_000,  // 3 mcs
                insert_new_msgs: 3_000,    // 3 mcs
                subgroup_size: 16,
            },
            finalize: WorkUnitsParamsFinalize {
                build_transactions: 1_000,    // 1 mcs
                build_accounts: 500,          // 0.5 mcs
                build_in_msg: 500,            // 0.5 mcs
                build_out_msg: 500,           // 0.5 mcs
                serialize_min: 15_000_000,    // 15 ms
                serialize_accounts: 1_000,    // 1 mcs
                serialize_msg: 2_000,         // 2 mcs
                state_update_min: 15_000_000, // 15 ms
                state_update_accounts: 500,   // 0.5 mcs
                state_update_msg: 2_000,      // 2 mcs
                create_diff: 0,
                serialize_diff: 0,
                apply_diff: 0,
                diff_tail_len: 0,
            },
        },
    })?;

    // Param 29
    params.set_consensus_config(&ConsensusConfig {
        clock_skew_millis: 5 * 1000,
        payload_batch_bytes: 768 * 1024,
        commit_history_rounds: 20,
        deduplicate_rounds: 140,
        max_consensus_lag_rounds: 210,
        payload_buffer_bytes: 50 * 1024 * 1024,
        broadcast_retry_millis: 150,
        download_retry_millis: 25,
        download_peers: 2,
        download_tasks: 260,
        sync_support_rounds: 840,
    })?;

    // Param 31
    params.set_fundamental_addresses(&[HashBytes([0x00; 32]), HashBytes([0x33; 32])])?;

    // Param 43
    params.set_size_limits(&SizeLimitsConfig {
        max_msg_bits: 1 << 21,
        max_msg_cells: 1 << 13,
        max_library_cells: 1000,
        max_vm_data_depth: 512,
        max_ext_msg_size: 65535,
        max_ext_msg_depth: 512,
        max_acc_state_cells: 1 << 16,
        max_acc_state_bits: (1 << 16) * 1023,
        max_acc_public_libraries: 256,
        defer_out_queue_size_limit: 256,
    })?;

    Ok(params)
}

fn update_config_account(accounts: &mut ShardAccounts, config: &BlockchainConfig) -> Result<()> {
    let Some(config_root) = config.params.as_dict().root().clone() else {
        anyhow::bail!("cannot set empty config account");
    };

    let Some((depth_balance, mut shard_account)) = accounts.get(config.address)? else {
        anyhow::bail!("config account not found");
    };

    let Some(mut account) = shard_account.load_account()? else {
        anyhow::bail!("empty config account");
    };

    // Update the first reference in the account data
    match &mut account.state {
        AccountState::Active(state) => {
            let mut builder = CellBuilder::new();
            builder.store_reference(config_root)?;

            if let Some(data) = state.data.take() {
                let mut data = data.as_slice()?;
                data.load_reference()?; // skip the first reference
                builder.store_slice(data)?;
            }

            state.data = Some(builder.build()?);
        }
        AccountState::Uninit | AccountState::Frozen(..) => {
            anyhow::bail!("config account is not active")
        }
    }

    shard_account.account = Lazy::new(&OptionalAccount(Some(account)))?;

    // Update the account entry in the dict
    accounts.set(config.address, depth_balance, shard_account)?;

    // Done
    Ok(())
}

fn build_config_account(
    pubkey: &ed25519::PublicKey,
    address: &HashBytes,
    balance: Tokens,
) -> Result<Account> {
    const CONFIG_CODE: &[u8] = include_bytes!("../../../res/config_code.boc");

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
    const ELECTOR_CODE: &[u8] = include_bytes!("../../../res/elector_code.boc");

    let code = Boc::decode(ELECTOR_CODE)?;

    let mut data = CellBuilder::new();
    data.store_small_uint(0, 3)?; // empty dict, empty dict, empty dict
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
    const MINTER_STATE: &[u8] = include_bytes!("../../../res/minter_state.boc");

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

mod serde_account_states {
    use serde::de::Deserializer;
    use serde::ser::{SerializeMap, Serializer};

    use super::*;

    pub fn serialize<S>(
        value: &FastHashMap<HashBytes, OptionalAccount>,
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
    ) -> Result<FastHashMap<HashBytes, OptionalAccount>, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[repr(transparent)]
        struct WrappedValue(#[serde(with = "BocRepr")] OptionalAccount);

        <FastHashMap<HashBytes, WrappedValue>>::deserialize(deserializer)
            .map(|map| map.into_iter().map(|(k, v)| (k, v.0)).collect())
    }
}
