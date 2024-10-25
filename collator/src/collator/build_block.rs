use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use everscale_types::merkle::*;
use everscale_types::models::*;
use everscale_types::prelude::*;
use humantime::format_duration;
use tycho_block_util::archive::WithArchiveData;
use tycho_block_util::block::BlockStuff;
use tycho_block_util::config::BlockchainConfigExt;
use tycho_block_util::dict::RelaxedAugDict;
use tycho_block_util::queue::SerializedQueueDiff;
use tycho_util::metrics::HistogramGuard;

use super::execution_manager::MessagesExecutor;
use super::types::WorkingState;
use super::CollatorStdImpl;
use crate::collator::debug_info::BlockDebugInfo;
use crate::collator::types::{BlockCollationData, PreparedInMsg, PreparedOutMsg, PrevData};
use crate::tracing_targets;
use crate::types::{BlockCandidate, CollationSessionInfo, FinalizeBlockGasParams, McData};

pub struct FinalizedBlock {
    pub collation_data: Box<BlockCollationData>,
    pub working_state: Box<WorkingState>,
    pub block_candidate: Box<BlockCandidate>,
    pub mc_data: Option<Arc<McData>>,
    pub new_state_root: Cell,
    pub new_observable_state: Box<ShardStateUnsplit>,
}

impl CollatorStdImpl {
    pub(super) fn finalize_block(
        mut collation_data: Box<BlockCollationData>,
        collation_session: Arc<CollationSessionInfo>,
        executor: MessagesExecutor,
        working_state: Box<WorkingState>,
        queue_diff: SerializedQueueDiff,
        finalize_block_gas_params: FinalizeBlockGasParams,
    ) -> Result<FinalizedBlock> {
        tracing::debug!(target: tracing_targets::COLLATOR, "finalize_block()");

        let shard = collation_data.block_id_short.shard;

        let labels = &[("workchain", shard.workchain().to_string())];
        let histogram =
            HistogramGuard::begin_with_labels("tycho_collator_finalize_block_time", labels);

        let mc_data = working_state.mc_data.as_ref();
        let prev_shard_data = working_state.prev_shard_data_ref();

        // update shard accounts tree and prepare accounts blocks
        let mut global_libraries = executor.executor_params().state_libs.clone();

        let is_masterchain = shard.is_masterchain();
        let config_address = &mc_data.config.address;

        // Compute a masterchain block seqno which will reference this block.
        let ref_by_mc_seqno = if is_masterchain {
            // The block itself for the masterchain
            collation_data.block_id_short.seqno
        } else {
            // And the next masterchain block for shards
            mc_data.block_id.seqno + 1
        };

        let mut processed_accounts_res = Ok(Default::default());
        let mut build_account_blocks_elapsed = Duration::ZERO;
        let mut in_msgs_res = Ok(Default::default());
        let mut build_in_msgs_elapsed = Duration::ZERO;
        let mut out_msgs_res = Ok(Default::default());
        let mut build_out_msgs_elapsed = Duration::ZERO;
        let histogram_build_account_blocks_and_messages = HistogramGuard::begin_with_labels(
            "tycho_collator_finalize_build_account_blocks_and_msgs_time",
            labels,
        );
        rayon::scope(|s| {
            s.spawn(|_| {
                let histogram = HistogramGuard::begin_with_labels(
                    "tycho_collator_finalize_build_in_msgs_time",
                    labels,
                );
                in_msgs_res = Self::build_in_msgs(&collation_data.in_msgs);
                build_in_msgs_elapsed = histogram.finish();
            });
            s.spawn(|_| {
                let histogram = HistogramGuard::begin_with_labels(
                    "tycho_collator_finalize_build_out_msgs_time",
                    labels,
                );
                out_msgs_res = Self::build_out_msgs(&collation_data.out_msgs);
                build_out_msgs_elapsed = histogram.finish();
            });

            let histogram = HistogramGuard::begin_with_labels(
                "tycho_collator_finalize_build_account_blocks_time",
                labels,
            );

            processed_accounts_res = Self::build_accounts(
                executor,
                prev_shard_data,
                is_masterchain,
                config_address,
                &mut global_libraries,
            );
            build_account_blocks_elapsed = histogram.finish();
        });

        let build_account_blocks_and_messages_elased =
            histogram_build_account_blocks_and_messages.finish();

        let processed_accounts = processed_accounts_res?;
        let in_msgs = in_msgs_res?;
        let out_msgs = out_msgs_res?;

        // TODO: update new_config_opt from hard fork
        // calc value flow
        let mut value_flow = collation_data.value_flow.clone();

        value_flow.imported = in_msgs.root_extra().value_imported.clone();
        value_flow.exported = out_msgs.root_extra().clone();
        value_flow.fees_collected = processed_accounts.account_blocks.root_extra().clone();
        value_flow
            .fees_collected
            .try_add_assign_tokens(in_msgs.root_extra().fees_collected)?;
        value_flow
            .fees_collected
            .try_add_assign(&value_flow.fees_imported)?;
        value_flow
            .fees_collected
            .try_add_assign(&value_flow.created)?;
        value_flow.to_next_block = processed_accounts
            .shard_accounts
            .root_extra()
            .balance
            .clone();

        // build master state extra or get a ref to last applied master block
        // TODO: extract min_ref_mc_seqno from processed_upto info when we have many shards
        // collation_data.update_ref_min_mc_seqno(min_ref_mc_seqno);
        let build_mc_state_extra_elapsed;
        let (mc_state_extra, master_ref) = if is_masterchain {
            let histogram = HistogramGuard::begin_with_labels(
                "tycho_collator_finish_build_mc_state_extra_time",
                labels,
            );

            let (extra, min_ref_mc_seqno) = Self::create_mc_state_extra(
                &mut collation_data,
                processed_accounts
                    .new_config_params
                    .map(|params| BlockchainConfig {
                        address: *config_address,
                        params,
                    }),
                &working_state,
            )?;
            collation_data.update_ref_min_mc_seqno(min_ref_mc_seqno);

            build_mc_state_extra_elapsed = histogram.finish();
            (Some(extra), None)
        } else {
            build_mc_state_extra_elapsed = Duration::ZERO;
            (None, Some(mc_data.make_block_ref()))
        };

        // build block info
        let mut new_block_info = BlockInfo {
            version: 0,
            key_block: matches!(&mc_state_extra, Some(extra) if extra.after_key_block),
            shard: collation_data.block_id_short.shard,
            seqno: collation_data.block_id_short.seqno,
            gen_utime: collation_data.gen_utime,
            gen_utime_ms: collation_data.gen_utime_ms,
            start_lt: collation_data.start_lt,
            end_lt: collation_data.next_lt,
            gen_validator_list_hash_short: collation_session.collators().short_hash,
            gen_catchain_seqno: collation_session.seqno(),
            min_ref_mc_seqno: collation_data.min_ref_mc_seqno,
            prev_key_block_seqno: mc_data.prev_key_block_seqno,
            master_ref: master_ref.as_ref().map(Lazy::new).transpose()?,
            ..Default::default()
        };
        let prev_ref = prev_shard_data.get_blocks_ref()?;
        new_block_info.set_prev_ref(&prev_ref);

        // TODO: should set when slpit/merge logic implemented
        // info.after_merge = false;
        // info.before_split = false;
        // info.after_split = false;
        // info.want_split = false;
        // info.want_merge = false;

        let capabilities = mc_data.config.get_global_version()?.capabilities;
        if capabilities.contains(GlobalCapability::CapReportVersion) {
            new_block_info.set_gen_software(Some(collation_data.global_version));
        }

        let build_state_update_elapsed;
        let new_state_root;
        let total_validator_fees;
        let (state_update, new_observable_state) = {
            let histogram = HistogramGuard::begin_with_labels(
                "tycho_collator_finalize_build_state_update_time",
                labels,
            );

            // compute total gas used from last anchor
            let mut gas_used_from_last_anchor = working_state
                .gas_used_from_last_anchor
                .saturating_add(collation_data.block_limit.gas_used as _);

            // add gas usage for in, out messages building, accounts storing and merkle calculation
            let FinalizeBlockGasParams {
                build_account,
                in_message,
                out_message,
                merkle_calc_account,
            } = finalize_block_gas_params;

            let accounts_count = processed_accounts.accounts_len as u64;
            let build = std::cmp::max(
                std::cmp::max(
                    build_account.saturating_mul(accounts_count),
                    in_message.saturating_mul(collation_data.in_msgs.len() as u64),
                ),
                out_message.saturating_mul(collation_data.out_msgs.len() as u64),
            );
            gas_used_from_last_anchor = gas_used_from_last_anchor
                .saturating_add(build)
                .saturating_add(merkle_calc_account.saturating_mul(accounts_count));

            // build new state
            let mut new_observable_state = Box::new(ShardStateUnsplit {
                global_id: mc_data.global_id,
                shard_ident: new_block_info.shard,
                seqno: new_block_info.seqno,
                vert_seqno: 0,
                gen_utime: new_block_info.gen_utime,
                gen_utime_ms: new_block_info.gen_utime_ms,
                gen_lt: new_block_info.end_lt,
                min_ref_mc_seqno: new_block_info.min_ref_mc_seqno,
                processed_upto: Lazy::new(&collation_data.processed_upto.clone().try_into()?)?,
                before_split: new_block_info.before_split,
                accounts: Lazy::new(&processed_accounts.shard_accounts)?,
                overload_history: gas_used_from_last_anchor,
                underload_history: 0,
                total_balance: value_flow.to_next_block.clone(),
                total_validator_fees: prev_shard_data.total_validator_fees().clone(),
                libraries: Dict::new(),
                master_ref,
                custom: mc_state_extra.as_ref().map(Lazy::new).transpose()?,
            });

            new_observable_state
                .total_validator_fees
                .try_add_assign(&value_flow.fees_collected)?;
            new_observable_state
                .total_validator_fees
                .try_sub_assign(&value_flow.recovered)?;

            if is_masterchain {
                new_observable_state.libraries = global_libraries.clone();
            }

            // TODO: update config smc on hard fork

            new_state_root = CellBuilder::build_from(&new_observable_state)?;

            total_validator_fees = new_observable_state.total_validator_fees.clone();

            // calc merkle update
            let merkle_update = create_merkle_update(
                &shard,
                prev_shard_data.pure_state_root(),
                &new_state_root,
                working_state.usage_tree.as_ref().unwrap(),
            )?;

            build_state_update_elapsed = histogram.finish();
            (merkle_update, new_observable_state)
        };

        let build_block_elapsed;
        let (new_block, new_block_extra, new_mc_block_extra) = {
            let histogram = HistogramGuard::begin_with_labels(
                "tycho_collator_finalize_build_block_time",
                labels,
            );

            // calc block extra
            let mut new_block_extra = BlockExtra {
                in_msg_description: Lazy::new(&in_msgs)?,
                out_msg_description: Lazy::new(&out_msgs)?,
                account_blocks: Lazy::new(&processed_accounts.account_blocks)?,
                rand_seed: collation_data.rand_seed,
                created_by: collation_data.created_by,
                ..Default::default()
            };

            let new_mc_block_extra = if let Some(mc_state_extra) = &mc_state_extra {
                let new_mc_block_extra = McBlockExtra {
                    shards: mc_state_extra.shards.clone(),
                    fees: collation_data.shard_fees.clone(),
                    // TODO: Signatures for previous blocks
                    prev_block_signatures: Default::default(),
                    mint_msg: collation_data
                        .mint_msg
                        .as_ref()
                        .map(Lazy::new)
                        .transpose()?,
                    recover_create_msg: collation_data
                        .recover_create_msg
                        .as_ref()
                        .map(Lazy::new)
                        .transpose()?,
                    copyleft_msgs: Default::default(),
                    config: if mc_state_extra.after_key_block {
                        Some(mc_state_extra.config.clone())
                    } else {
                        None
                    },
                };

                new_block_extra.custom = Some(Lazy::new(&new_mc_block_extra)?);

                Some(new_mc_block_extra)
            } else {
                None
            };

            // construct block
            let block = Block {
                global_id: mc_data.global_id,
                info: Lazy::new(&new_block_info)?,
                value_flow: Lazy::new(&value_flow)?,
                state_update: Lazy::new(&state_update)?,
                // do not use out msgs queue updates
                out_msg_queue_updates: OutMsgQueueUpdates {
                    diff_hash: *queue_diff.hash(),
                },
                extra: Lazy::new(&new_block_extra)?,
            };

            // TODO: Check (assert) whether the serialized block contains usage cells
            let root = CellBuilder::build_from(&block)?;

            let data = everscale_types::boc::Boc::encode_rayon(&root);
            let block_id = BlockId {
                shard: collation_data.block_id_short.shard,
                seqno: collation_data.block_id_short.seqno,
                root_hash: *root.repr_hash(),
                file_hash: Boc::file_hash_blake(&data),
            };

            build_block_elapsed = histogram.finish();

            let block = BlockStuff::from_block_and_root(&block_id, block, root, data.len());

            (
                WithArchiveData::new(block, data),
                new_block_extra,
                new_mc_block_extra,
            )
        };

        let new_block_id = *new_block.id();

        // log block debug info
        tracing::debug!(target: tracing_targets::COLLATOR,
            "collated_block_info: {:?}",
            BlockDebugInfo {
                block_id: &new_block_id,
                block_info: &new_block_info,
                prev_ref: &prev_ref,
                state: &new_observable_state,
                processed_upto: &collation_data.processed_upto,
                mc_state_extra: mc_state_extra.as_ref(),
                merkle_update: &state_update,
                block_extra: &new_block_extra,
                mc_block_extra: new_mc_block_extra.as_ref(),
            },
        );

        let mc_data = mc_state_extra.map(|extra| {
            let prev_key_block_seqno = if extra.after_key_block {
                new_block_id.seqno
            } else if let Some(block_ref) = &extra.last_key_block {
                block_ref.seqno
            } else {
                0
            };

            Arc::new(McData {
                global_id: new_block.as_ref().global_id,
                block_id: *new_block.id(),

                prev_key_block_seqno,
                gen_lt: new_block_info.end_lt,
                gen_chain_time: collation_data.get_gen_chain_time(),
                libraries: global_libraries,
                total_validator_fees,

                global_balance: extra.global_balance.clone(),
                shards: extra.shards,
                config: extra.config,
                validator_info: extra.validator_info,

                processed_upto: collation_data.processed_upto.clone(),

                ref_mc_state_handle: prev_shard_data.ref_mc_state_handle().clone(),
            })
        });

        // TODO: build collated data from collation_data.shard_top_block_descriptors
        let collated_data = vec![];

        let block_candidate = Box::new(BlockCandidate {
            ref_by_mc_seqno,
            block: new_block,
            is_key_block: new_block_info.key_block,
            prev_blocks_ids: prev_shard_data.blocks_ids().clone(),
            top_shard_blocks_ids: collation_data.top_shard_blocks_ids.clone(),
            collated_data,
            collated_file_hash: HashBytes::ZERO,
            chain_time: collation_data.get_gen_chain_time(),
            processed_to_anchor_id: collation_data
                .processed_upto
                .externals
                .as_ref()
                .map(|upto| upto.processed_to.0)
                .unwrap_or_default(),
            value_flow,
            created_by: collation_data.created_by,
            queue_diff_aug: queue_diff.build(&new_block_id),
        });

        let total_elapsed = histogram.finish();

        tracing::debug!(
            target: tracing_targets::COLLATOR,
            total = %format_duration(total_elapsed),
            parallel_build_accounts_and_msgs = %format_duration(build_account_blocks_and_messages_elased),
            only_build_account_blocks = %format_duration(build_account_blocks_elapsed),
            only_build_in_msgs = %format_duration(build_in_msgs_elapsed),
            only_build_out_msgs = %format_duration(build_out_msgs_elapsed),
            build_mc_state_extra = %format_duration(build_mc_state_extra_elapsed),
            build_state_update = %format_duration(build_state_update_elapsed),
            build_block = %format_duration(build_block_elapsed),
            "finalize block timings"
        );

        Ok(FinalizedBlock {
            collation_data,
            working_state,
            block_candidate,
            mc_data,
            new_state_root,
            new_observable_state,
        })
    }

    fn create_mc_state_extra(
        collation_data: &mut BlockCollationData,
        config_params: Option<BlockchainConfig>,
        working_state: &WorkingState,
    ) -> Result<(McStateExtra, u32)> {
        let prev_shard_data = working_state.prev_shard_data_ref();
        let prev_state = &prev_shard_data.observable_states()[0];

        // 1. update config params and detect key block
        let prev_state_extra = prev_state.state_extra()?;
        let prev_config = &prev_state_extra.config;
        let (config, is_key_block) = if let Some(new_config) = config_params {
            if !new_config.validate_params(true, None)? {
                bail!(
                    "configuration smart contract {} contains an invalid configuration in its data",
                    new_config.address
                );
            }
            let is_key_block = &new_config != prev_config;
            (new_config, is_key_block)
        } else {
            (prev_config.clone(), false)
        };

        let current_gen_utime = collation_data.gen_utime;
        let prev_gen_utime = prev_state.state().gen_utime;

        // 2. update shard_hashes and shard_fees
        let cc_config = config.get_catchain_config()?;
        let workchains = config.get_workchains()?;
        // check if need to start new collation session for shards
        let update_shard_cc = {
            let lifetimes = current_gen_utime / cc_config.shard_catchain_lifetime;
            let prev_lifetimes = prev_gen_utime / cc_config.shard_catchain_lifetime;
            is_key_block || (lifetimes > prev_lifetimes)
        };
        let min_ref_mc_seqno =
            Self::update_shard_config(collation_data, &workchains, update_shard_cc)?;

        // 3. save new shard_hashes
        let shards_iter = collation_data
            .shards()?
            .iter()
            .map(|(k, v)| (k, v.as_ref()));
        let shards = ShardHashes::from_shards(shards_iter)?;

        // 4. check extension flags
        // prev_state_extra.flags is checked in the McStateExtra::load_from

        // 5. update validator_info
        let max_anchor_distance = 210; // TODO: get from blockchain config
        let mut validator_info = None;
        if is_key_block {
            // check if validator set changed by the cells hash
            // NOTE: We intentionaly use current validator set from previous config
            //       instead of just using the `prev_vset`.
            let prev_vset = prev_config.get_current_validator_set_raw()?;
            let current_vset = config.get_current_validator_set_raw()?;
            if current_vset.repr_hash() != prev_vset.repr_hash() {
                // calc next mempool switch round (identifies next session_seqno)
                let prev_processed_to_anchor = prev_shard_data
                    .processed_upto()
                    .externals
                    .as_ref()
                    .map(|ext_upto| ext_upto.processed_to.0)
                    .unwrap_or_default();
                let next_session_seqno = prev_processed_to_anchor + max_anchor_distance;

                // calculate next validator subset and hash
                let current_vset = current_vset.parse::<ValidatorSet>()?;
                let subset_config = config.get_catchain_config()?;
                let (_, validator_list_hash_short) = current_vset.compute_subset(
                    working_state.next_block_id_short.shard,
                    &subset_config,
                    next_session_seqno,
                ).ok_or_else(|| anyhow!(
                    "Error calculating subset of validators for next session (shard_id = {}, next_session_seqno = {})",
                    working_state.next_block_id_short.shard,
                    next_session_seqno,
                ))?;

                validator_info = Some(ValidatorInfo {
                    validator_list_hash_short,
                    // TODO: rename field in types
                    catchain_seqno: next_session_seqno,
                    nx_cc_updated: true,
                });
            }
        }
        let validator_info = validator_info.unwrap_or(ValidatorInfo {
            nx_cc_updated: false,
            ..prev_state_extra.validator_info
        });

        // 6. update prev_blocks (add prev block's id to the dictionary)
        let prev_is_key_block = collation_data.block_id_short.seqno == 1 // prev block is a keyblock if it is a zerostate
            || prev_state_extra.after_key_block;
        let mut prev_blocks = prev_state_extra.prev_blocks.clone();
        let prev_blk_ref = BlockRef {
            end_lt: prev_state.state().gen_lt,
            seqno: prev_state.block_id().seqno,
            root_hash: prev_state.block_id().root_hash,
            file_hash: prev_state.block_id().file_hash,
        };

        prev_blocks.set(
            prev_state.block_id().seqno,
            KeyMaxLt {
                has_key_block: prev_is_key_block,
                max_end_lt: prev_state.state().gen_lt,
            },
            &KeyBlockRef {
                is_key_block: prev_is_key_block,
                block_ref: prev_blk_ref.clone(),
            },
        )?;

        // 7. update last_key_block
        let last_key_block = if prev_state_extra.after_key_block {
            Some(prev_blk_ref)
        } else {
            prev_state_extra.last_key_block.clone()
        };

        // 8. update global balance
        let mut global_balance = prev_state_extra.global_balance.clone();
        global_balance.try_add_assign(&collation_data.value_flow.created)?;
        global_balance.try_add_assign(&collation_data.value_flow.minted)?;
        global_balance.try_add_assign(&collation_data.shard_fees.root_extra().create)?;

        // 9. update block creator stats
        #[cfg(not(feature = "block-creator-stats"))]
        let block_create_stats = None;
        #[cfg(feature = "block-creator-stats")]
        let block_create_stats = if prev_state_extra
            .config
            .get_global_version()?
            .capabilities
            .contains(GlobalCapability::CapCreateStatsEnabled)
        {
            let mut stats = prev_state_extra
                .block_create_stats
                .clone()
                .unwrap_or_default();
            Self::update_block_creator_stats(collation_data, &mut stats)?;
            Some(stats)
        } else {
            None
        };

        // 10. pack new McStateExtra
        let mc_state_extra = McStateExtra {
            shards,
            config,
            validator_info,
            prev_blocks,
            after_key_block: is_key_block,
            last_key_block,
            block_create_stats,
            global_balance,
            copyleft_rewards: Default::default(),
        };

        Ok((mc_state_extra, min_ref_mc_seqno))
    }

    fn update_shard_config(
        collation_data: &mut BlockCollationData,
        _wc_set: &Dict<i32, WorkchainDescription>,
        _update_cc: bool,
    ) -> Result<u32> {
        // TODO: here should be the split/merge logic, refer to old node impl

        // STUB: just do nothing for now: no split/merge, no session rotation
        let mut min_ref_mc_seqno = u32::MAX;
        for shard_descr in collation_data.shards_mut()?.values_mut() {
            min_ref_mc_seqno = std::cmp::min(min_ref_mc_seqno, shard_descr.min_ref_mc_seqno);
        }

        Ok(min_ref_mc_seqno)
    }

    fn build_accounts(
        executor: MessagesExecutor,
        prev_shard_data: &PrevData,
        is_masterchain: bool,
        config_address: &HashBytes,
        global_libraries: &mut Dict<HashBytes, LibDescr>,
    ) -> Result<ProcessedAccounts> {
        let mut account_blocks = BTreeMap::new();
        let mut shard_accounts = RelaxedAugDict::from_full(prev_shard_data.observable_accounts());
        let mut new_config_params = None;

        for updated_account in executor.into_changed_accounts() {
            if updated_account.transactions.is_empty() {
                continue;
            }

            let loaded_account = updated_account.shard_account.load_account()?;
            match &loaded_account {
                Some(account) => {
                    if is_masterchain && &updated_account.account_addr == config_address {
                        if let AccountState::Active(StateInit {
                            data: Some(data), ..
                        }) = &account.state
                        {
                            new_config_params = Some(data.parse::<BlockchainConfigParams>()?);
                        }
                    }

                    shard_accounts.set_any(
                        &updated_account.account_addr,
                        &DepthBalanceInfo {
                            split_depth: 0, // NOTE: will need to set when we implement accounts split/merge logic
                            balance: account.balance.clone(),
                        },
                        &updated_account.shard_account,
                    )?;
                }
                None => {
                    shard_accounts.remove(&updated_account.account_addr)?;
                }
            }

            if is_masterchain {
                updated_account.update_public_libraries(&loaded_account, global_libraries)?;
            }

            let account_block = AccountBlock {
                state_update: updated_account.build_hash_update(),
                account: updated_account.account_addr,
                transactions: AugDict::try_from_btree(&updated_account.transactions)?,
            };

            account_blocks.insert(updated_account.account_addr, account_block);
        }

        let accounts_len = account_blocks.len();

        // TODO: Somehow consume accounts inside an iterator
        let account_blocks = RelaxedAugDict::try_from_sorted_iter_any(
            account_blocks
                .iter()
                .map(|(k, v)| (k, v.transactions.root_extra(), v as &dyn Store)),
        )?;

        Ok(ProcessedAccounts {
            account_blocks: account_blocks.build()?,
            shard_accounts: shard_accounts.build()?,
            new_config_params,
            accounts_len,
        })
    }

    fn build_in_msgs(items: &BTreeMap<HashBytes, PreparedInMsg>) -> Result<InMsgDescr> {
        RelaxedAugDict::try_from_sorted_iter_lazy(
            items
                .iter()
                .map(|(msg_id, msg)| (msg_id, &msg.import_fees, &msg.in_msg)),
        )?
        .build()
        .map_err(Into::into)
    }

    fn build_out_msgs(items: &BTreeMap<HashBytes, PreparedOutMsg>) -> Result<OutMsgDescr> {
        RelaxedAugDict::try_from_sorted_iter_lazy(
            items
                .iter()
                .map(|(msg_id, msg)| (msg_id, &msg.exported_value, &msg.out_msg)),
        )?
        .build()
        .map_err(Into::into)
    }

    #[cfg(feature = "block-creator-stats")]
    fn update_block_creator_stats(
        collation_data: &BlockCollationData,
        block_create_stats: &mut Dict<HashBytes, CreatorStats>,
    ) -> Result<()> {
        let mut mc_updated = false;
        for (creator, count) in &collation_data.block_create_count {
            let shard_scaled = count << 32;
            let total_mc = if collation_data.created_by == *creator {
                mc_updated = true;
                1
            } else {
                0
            };

            block_create_stats.set(creator, CreatorStats {
                mc_blocks: BlockCounters {
                    updated_at: collation_data.gen_utime,
                    total: total_mc,
                    cnt2048: total_mc,
                    cnt65536: total_mc,
                },
                shard_blocks: BlockCounters {
                    updated_at: collation_data.gen_utime,
                    total: *count,
                    cnt2048: shard_scaled,
                    cnt65536: shard_scaled,
                },
            })?;
        }
        if !mc_updated {
            block_create_stats.set(collation_data.created_by, CreatorStats {
                mc_blocks: BlockCounters {
                    updated_at: collation_data.gen_utime,
                    total: 1,
                    cnt2048: 1,
                    cnt65536: 1,
                },
                shard_blocks: BlockCounters {
                    updated_at: collation_data.gen_utime,
                    total: 0,
                    cnt2048: 0,
                    cnt65536: 0,
                },
            })?;
        }

        let default_shard_blocks_count = collation_data.block_create_count.values().sum();
        block_create_stats.set(HashBytes::default(), CreatorStats {
            mc_blocks: BlockCounters {
                updated_at: collation_data.gen_utime,
                total: 1,
                cnt2048: 1,
                cnt65536: 1,
            },
            shard_blocks: BlockCounters {
                updated_at: collation_data.gen_utime,
                total: default_shard_blocks_count,
                cnt2048: default_shard_blocks_count << 32,
                cnt65536: default_shard_blocks_count << 32,
            },
        })?;
        // TODO: prune CreatorStats https://github.com/ton-blockchain/ton/blob/master/validator/impl/collator.cpp#L4191
        Ok(())
    }
}

#[derive(Default)]
struct ProcessedAccounts {
    account_blocks: AccountBlocks,
    shard_accounts: ShardAccounts,
    new_config_params: Option<BlockchainConfigParams>,
    accounts_len: usize,
}

fn create_merkle_update(
    shard_id: &ShardIdent,
    old_state_root: &Cell,
    new_state_root: &Cell,
    usage_tree: &UsageTree,
) -> Result<MerkleUpdate> {
    let labels = [("workchain", shard_id.workchain().to_string())];
    let histogram =
        HistogramGuard::begin_with_labels("tycho_collator_create_merkle_update_time", &labels);

    let merkle_update_builder =
        MerkleUpdate::create(old_state_root.as_ref(), new_state_root.as_ref(), usage_tree);
    let state_update = merkle_update_builder.build()?;

    let elapsed = histogram.finish();

    tracing::debug!(
        target: tracing_targets::COLLATOR,
        elapsed = %humantime::format_duration(elapsed),
        "merkle update created"
    );

    Ok(state_update)
}
