use std::collections::BTreeMap;

use anyhow::{bail, Result};
use everscale_types::merkle::*;
use everscale_types::models::*;
use everscale_types::num::Tokens;
use everscale_types::prelude::*;
use humantime::format_duration;
use tokio::time::Instant;
use tycho_block_util::config::BlockchainConfigExt;
use tycho_block_util::dict::RelaxedAugDict;
use tycho_block_util::state::ShardStateStuff;
use tycho_util::metrics::HistogramGuard;

use super::execution_manager::ExecutionManager;
use super::CollatorStdImpl;
use crate::collator::types::{
    BlockCollationCommon, BlockCollationData, McData, PreparedInMsg, PreparedOutMsg, PrevData,
};
use crate::tracing_targets;
use crate::types::{BlockCandidate, FinalizeBlockInfo};

impl CollatorStdImpl {
    pub(super) fn finalize_block(
        &mut self,
        collation_data: &mut BlockCollationData,
        exec_manager: ExecutionManager,
    ) -> Result<(Box<BlockCandidate>, ShardStateStuff)> {
        tracing::debug!(target: tracing_targets::COLLATOR, "finalize_block()");

        let labels = &[("workchain", self.shard_id.workchain().to_string())];
        let histogram =
            HistogramGuard::begin_with_labels("tycho_collator_finalize_block_time", labels);

        let mc_data = &self.working_state().mc_data;
        let prev_shard_data = &self.working_state().prev_shard_data;

        let mut block_info_res = Ok(Default::default());
        let mut in_msgs_res = Ok(Default::default());
        let mut out_msgs_res = Ok(Default::default());
        let mut build_in_msgs_elapsed = Default::default();
        let mut build_out_msgs_elapsed = Default::default();
        let (in_msgs_fees_collected, in_msgs_values_imported) =
            collation_data.in_msgs.values().fold(
                (Tokens::default(), CurrencyCollection::default()),
                |(fees_collected, values_imported), x| {
                    (
                        fees_collected
                            .checked_add(x.import_fees.fees_collected)
                            .unwrap_or_default(),
                        values_imported
                            .checked_add(&x.import_fees.value_imported)
                            .unwrap_or_default(),
                    )
                },
            );

        let in_msgs = &mut collation_data.in_msgs;
        let out_msgs = &mut collation_data.out_msgs;
        let common = &mut collation_data.common;

        rayon::scope(|s| {
            s.spawn(|_| {
                block_info_res = self.calculate_block_info(
                    exec_manager,
                    prev_shard_data,
                    mc_data,
                    common,
                    in_msgs_fees_collected,
                    in_msgs_values_imported.clone(),
                );
            });
            s.spawn(|_| {
                let histogram = HistogramGuard::begin_with_labels(
                    "tycho_collator_finalize_build_in_msgs_time",
                    labels,
                );
                in_msgs_res = self.calculate_in_msgs(in_msgs);
                build_in_msgs_elapsed = histogram.finish();
            });
            s.spawn(|_| {
                let histogram = HistogramGuard::begin_with_labels(
                    "tycho_collator_finalize_build_out_msgs_time",
                    labels,
                );
                out_msgs_res = self.calculate_out_msgs(out_msgs);
                build_out_msgs_elapsed = histogram.finish();
            });
        });

        let FinalizeBlockInfo {
            block_info: new_block_info,
            merkle_update,
            account_blocks,
            fees_collected,
            total_balance,
            mc_state_extra,
            build_account_blocks_elapsed,
            build_mc_state_extra_elapsed,
            build_state_update_elapsed,
        } = block_info_res?;
        let in_msgs = in_msgs_res?;
        let out_msgs = out_msgs_res?;

        let mut value_flow = collation_data.common.value_flow.clone();
        value_flow.fees_collected = fees_collected;
        value_flow.imported = in_msgs_values_imported.clone();
        value_flow.to_next_block = total_balance;
        value_flow.exported = out_msgs.root_extra().clone();

        let build_block_elapsed;
        let new_block_id;
        let new_block_boc;
        let new_block = {
            let histogram = HistogramGuard::begin_with_labels(
                "tycho_collator_finalize_build_block_time",
                labels,
            );

            // calc block extra
            let mut new_block_extra = BlockExtra {
                in_msg_description: Lazy::new(&in_msgs)?,
                out_msg_description: Lazy::new(&out_msgs)?,
                account_blocks: Lazy::new(&account_blocks)?,
                rand_seed: collation_data.common.rand_seed,
                created_by: collation_data.created_by,
                ..Default::default()
            };

            if let Some(mc_state_extra) = mc_state_extra {
                let new_mc_block_extra = McBlockExtra {
                    shards: mc_state_extra.shards.clone(),
                    fees: collation_data.common.shard_fees.clone(),
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
            }

            // construct block
            let new_block = Block {
                global_id: mc_data.global_id(),
                info: Lazy::new(&new_block_info)?,
                value_flow: Lazy::new(&value_flow)?,
                state_update: Lazy::new(&merkle_update)?,
                // do not use out msgs queue updates
                out_msg_queue_updates: None,
                extra: Lazy::new(&new_block_extra)?,
            };

            build_block_elapsed = histogram.finish();

            // TODO: Check (assert) whether the serialized block contains usage cells
            let new_block_root = CellBuilder::build_from(&new_block)?;

            new_block_boc = everscale_types::boc::Boc::encode(&new_block_root);
            new_block_id = BlockId {
                shard: collation_data.common.block_id_short.shard,
                seqno: collation_data.common.block_id_short.seqno,
                root_hash: *new_block_root.repr_hash(),
                file_hash: Boc::file_hash(&new_block_boc),
            };
            new_block
        };

        // TODO: build collated data from collation_data.shard_top_block_descriptors
        let collated_data = vec![];

        let block_candidate = Box::new(BlockCandidate {
            block_id: new_block_id,
            block: new_block,
            prev_blocks_ids: prev_shard_data.blocks_ids().clone(),
            top_shard_blocks_ids: collation_data.top_shard_blocks_ids.clone(),
            data: new_block_boc,
            collated_data,
            collated_file_hash: HashBytes::ZERO,
            chain_time: (new_block_info.gen_utime as u64 * 1000)
                + new_block_info.gen_utime_ms as u64,
        });

        // build new shard state using merkle update
        // to get updated state without UsageTree

        let build_new_state_elapsed;
        let new_state_stuff = {
            let histogram = HistogramGuard::begin_with_labels(
                "tycho_collator_finalize_build_new_state_time",
                labels,
            );

            let pure_prev_state_root = prev_shard_data.pure_state_root();
            let new_state_root = merkle_update.apply(pure_prev_state_root)?;
            let new_state_stuff =
                ShardStateStuff::from_root(&new_block_id, new_state_root, &self.state_tracker)?;

            build_new_state_elapsed = histogram.finish();
            new_state_stuff
        };

        let total_elapsed = histogram.finish();

        tracing::debug!(
            target: tracing_targets::COLLATOR,
            total = %format_duration(total_elapsed),
            build_account_blocks = %format_duration(build_account_blocks_elapsed),
            build_in_msgs = %format_duration(build_in_msgs_elapsed),
            build_out_msgs = %format_duration(build_out_msgs_elapsed),
            build_mc_state_extra = %format_duration(build_mc_state_extra_elapsed),
            build_state_update = %format_duration(build_state_update_elapsed),
            build_block = %format_duration(build_block_elapsed),
            build_new_state = %format_duration(build_new_state_elapsed),
            "finalize block timings"
        );

        Ok((block_candidate, new_state_stuff))
    }

    fn build_state_update(
        &self,
        collation_data: &mut BlockCollationCommon,
        global_id: i32,
        prev_shard_data: &PrevData,
        global_libraries: Dict<HashBytes, LibDescr>,
        is_masterchain: bool,
        shard_accounts: ShardAccounts,
        total_validator_fees: CurrencyCollection,
        total_balance: CurrencyCollection,
        mc_state_extra: &Option<McStateExtra>,
        master_ref: Option<BlockRef>,
        new_block_info: &BlockInfo,
    ) -> Result<MerkleUpdate> {
        // build new state

        let mut new_observable_state = Box::new(ShardStateUnsplit {
            global_id,
            shard_ident: new_block_info.shard,
            seqno: new_block_info.seqno,
            vert_seqno: 0,
            gen_utime: new_block_info.gen_utime,
            gen_utime_ms: new_block_info.gen_utime_ms,
            gen_lt: new_block_info.end_lt,
            min_ref_mc_seqno: new_block_info.min_ref_mc_seqno,
            processed_upto: Lazy::new(&collation_data.processed_upto)?,
            before_split: new_block_info.before_split,
            accounts: Lazy::new(&shard_accounts)?,
            overload_history: prev_shard_data.gas_used_from_last_anchor()
                + collation_data.block_limit.gas_used as u64,
            underload_history: 0,
            total_balance,
            total_validator_fees,
            libraries: Dict::new(),
            master_ref,
            custom: mc_state_extra.as_ref().map(Lazy::new).transpose()?,
            #[cfg(feature = "venom")]
            shard_block_refs: None,
        });

        if is_masterchain {
            new_observable_state.libraries = global_libraries;
        }

        // TODO: update smc on hard fork

        let new_state_root = CellBuilder::build_from(&new_observable_state)?;

        // calc merkle update
        let merkle_update = create_merkle_update(
            prev_shard_data.pure_state_root(),
            &new_state_root,
            &self.working_state().usage_tree,
        )?;
        Ok(merkle_update)
    }

    fn create_mc_state_extra(
        &self,
        collation_data: &mut BlockCollationCommon,
        config_params: Option<BlockchainConfig>,
    ) -> Result<(McStateExtra, u32)> {
        let prev_shard_data = &self.working_state().prev_shard_data;
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
            self.update_shard_config(collation_data, &workchains, update_shard_cc)?;

        // 3. save new shard_hashes
        let shards_iter = collation_data
            .shards()?
            .iter()
            .map(|(k, v)| (k, v.as_ref()));
        let shards = ShardHashes::from_shards(shards_iter)?;

        // 4. check extension flags
        // prev_state_extra.flags is checked in the McStateExtra::load_from

        // 5. update validator_info
        // TODO: check `create_mc_state_extra()` for a reference implementation
        // STUB: currently we do not use validator_info and just do nothing there
        let validator_info = prev_state_extra.validator_info;

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
        &self,
        mut collation_data: &mut BlockCollationCommon,
        _wc_set: &Dict<i32, WorkchainDescription>,
        _update_cc: bool,
    ) -> Result<u32> {
        // TODO: here should be the split/merge logic, refer to old node impl

        // STUB: just do nothing for now: no split/merge, no session rotation
        let mut min_ref_mc_seqno = u32::max_value();
        for shard_descr in collation_data.shards_mut()?.values_mut() {
            min_ref_mc_seqno = std::cmp::min(min_ref_mc_seqno, shard_descr.min_ref_mc_seqno);
        }

        Ok(min_ref_mc_seqno)
    }

    fn calculate_shards_info(
        &self,
        exec_manager: ExecutionManager,
        prev_shard_data: &PrevData,
        is_masterchain: bool,
        config_address: HashBytes,
        global_libraries: &mut Dict<HashBytes, LibDescr>,
    ) -> Result<(AccountBlocks, ShardAccounts, Option<BlockchainConfigParams>)> {
        let mut account_blocks = RelaxedAugDict::new();
        let mut shard_accounts = RelaxedAugDict::from_full(prev_shard_data.observable_accounts());
        let mut new_config_params = None;

        for updated_account in exec_manager.into_changed_accounts() {
            if updated_account.transactions.is_empty() {
                continue;
            }

            let loaded_account = updated_account.shard_account.load_account()?;
            match &loaded_account {
                Some(account) => {
                    if is_masterchain && updated_account.account_addr == config_address {
                        if let AccountState::Active(StateInit { data, .. }) = &account.state {
                            if let Some(data) = data {
                                new_config_params = Some(data.parse::<BlockchainConfigParams>()?);
                            }
                        }
                    }

                    shard_accounts.set_any(
                        &updated_account.account_addr,
                        &DepthBalanceInfo {
                            split_depth: 0, // TODO: fix
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
                state_update: updated_account.build_hash_update(), // TODO: fix state update
                account: updated_account.account_addr,
                transactions: updated_account.transactions.build()?,
            };

            account_blocks.set_any(
                &updated_account.account_addr,
                account_block.transactions.root_extra(),
                &account_block,
            )?;
        }
        Ok((
            account_blocks.build()?,
            shard_accounts.build()?,
            new_config_params,
        ))
    }

    fn calculate_block_info(
        &self,
        exec_manager: ExecutionManager,
        prev_shard_data: &PrevData,
        mc_data: &McData,
        collation_data: &mut BlockCollationCommon,
        in_msgs_fees_collected: Tokens,
        in_msgs_values_imported: CurrencyCollection,
    ) -> Result<FinalizeBlockInfo> {
        let mut global_libraries = exec_manager.executor_params().state_libs.clone();
        let is_masterchain = collation_data.block_id_short.shard.is_masterchain();
        let config_address = self.working_state().mc_data.config().address;

        let labels = &[("workchain", self.shard_id.workchain().to_string())];
        let histogram = HistogramGuard::begin_with_labels(
            "tycho_collator_finalize_build_account_blocks_time",
            labels,
        );
        let (account_blocks, shard_accounts, new_config_params) = self.calculate_shards_info(
            exec_manager,
            prev_shard_data,
            is_masterchain,
            config_address,
            &mut global_libraries,
        )?;

        let build_account_blocks_elapsed = histogram.finish();

        let mut fees_collected = account_blocks.root_extra().clone();
        fees_collected.try_add_assign_tokens(in_msgs_fees_collected)?;
        fees_collected.try_add_assign(&in_msgs_values_imported)?;
        fees_collected.try_add_assign(&collation_data.value_flow.created)?;

        let mut total_validator_fees = prev_shard_data.total_validator_fees().clone();
        total_validator_fees.try_add_assign(&fees_collected)?;
        total_validator_fees.try_sub_assign(&collation_data.value_flow.recovered)?;

        let histogram = HistogramGuard::begin_with_labels(
            "tycho_collator_finish_build_mc_state_extra_time",
            labels,
        );
        let (mc_state_extra, master_ref) = self.calculate_extra(
            mc_data,
            collation_data,
            is_masterchain,
            config_address,
            new_config_params,
        )?;
        let build_mc_state_extra_elapsed = histogram.finish();

        // build block info
        let new_block_info = self.calculate_new_block_info(
            prev_shard_data,
            mc_data,
            &mc_state_extra,
            &master_ref,
            collation_data,
        )?;

        let total_balance = shard_accounts.root_extra().balance.clone();
        let global_id = mc_data.global_id();

        let histogram = HistogramGuard::begin_with_labels(
            "tycho_collator_finalize_build_state_update_time",
            labels,
        );

        let merkle_update = self.build_state_update(
            collation_data,
            global_id,
            prev_shard_data,
            global_libraries,
            is_masterchain,
            shard_accounts,
            total_balance.clone(),
            total_validator_fees,
            &mc_state_extra,
            master_ref,
            &new_block_info,
        )?;

        let build_state_update_elapsed = histogram.finish();

        Ok(FinalizeBlockInfo {
            block_info: new_block_info,
            merkle_update,
            account_blocks,
            fees_collected,
            total_balance,
            mc_state_extra,
            build_account_blocks_elapsed,
            build_mc_state_extra_elapsed,
            build_state_update_elapsed,
        })
    }

    fn calculate_new_block_info(
        &self,
        prev_shard_data: &PrevData,
        mc_data: &McData,
        mc_state_extra: &Option<McStateExtra>,
        master_ref: &Option<BlockRef>,
        collation_data: &mut BlockCollationCommon,
    ) -> Result<BlockInfo> {
        // build block info
        let mut new_block_info = BlockInfo {
            version: 0,
            key_block: matches!(mc_state_extra, Some(extra) if extra.after_key_block),
            shard: collation_data.block_id_short.shard,
            seqno: collation_data.block_id_short.seqno,
            gen_utime: collation_data.gen_utime,
            gen_utime_ms: collation_data.gen_utime_ms,
            start_lt: collation_data.start_lt,
            end_lt: collation_data.next_lt,
            gen_validator_list_hash_short: self.collation_session.collators().short_hash,
            gen_catchain_seqno: self.collation_session.seqno(),
            min_ref_mc_seqno: collation_data.min_ref_mc_seqno,
            prev_key_block_seqno: mc_data.prev_key_block_seqno(),
            master_ref: master_ref.as_ref().map(Lazy::new).transpose()?,
            ..Default::default()
        };
        new_block_info.set_prev_ref(&prev_shard_data.get_blocks_ref()?);

        // TODO: should set when slpit/merge logic implemented
        // info.after_merge = false;
        // info.before_split = false;
        // info.after_split = false;
        // info.want_split = false;
        // info.want_merge = false;

        let capabilities = mc_data.config().get_global_version()?.capabilities;
        if capabilities.contains(GlobalCapability::CapReportVersion) {
            new_block_info.set_gen_software(Some(GlobalVersion {
                version: self.config.supported_block_version,
                capabilities: self.config.supported_capabilities,
            }));
        }

        Ok(new_block_info)
    }

    fn calculate_extra(
        &self,
        mc_data: &McData,
        collation_data: &mut BlockCollationCommon,
        is_masterchain: bool,
        config_address: HashBytes,
        new_config_params: Option<BlockchainConfigParams>,
    ) -> Result<(Option<McStateExtra>, Option<BlockRef>)> {
        // build master state extra or get a ref to last applied master block
        // TODO: extract min_ref_mc_seqno from processed_upto info when we have many shards
        // collation_data.update_ref_min_mc_seqno(min_ref_mc_seqno);
        Ok(if is_masterchain {
            let (extra, min_ref_mc_seqno) = self.create_mc_state_extra(
                collation_data,
                new_config_params.map(|params| BlockchainConfig {
                    address: config_address,
                    params,
                }),
            )?;
            collation_data.update_ref_min_mc_seqno(min_ref_mc_seqno);

            (Some(extra), None)
        } else {
            (None, Some(mc_data.get_master_ref()))
        })
    }

    fn calculate_in_msgs(
        &self,
        in_msgs_map: &mut BTreeMap<HashBytes, PreparedInMsg>,
    ) -> Result<InMsgDescr> {
        let mut in_msgs = RelaxedAugDict::new();
        // TODO: use more effective algorithm than iter and set
        for (msg_id, msg) in in_msgs_map.iter() {
            in_msgs.set_as_lazy(msg_id, &msg.import_fees, &msg.in_msg)?;
        }
        Ok(in_msgs.build()?)
    }
    fn calculate_out_msgs(
        &self,
        out_msgs_map: &mut BTreeMap<HashBytes, PreparedOutMsg>,
    ) -> Result<OutMsgDescr> {
        let mut out_msgs = RelaxedAugDict::new();
        // TODO: use more effective algorithm than iter and set
        for (msg_id, msg) in out_msgs_map.iter() {
            out_msgs.set_as_lazy(msg_id, &msg.exported_value, &msg.out_msg)?;
        }
        Ok(out_msgs.build()?)
    }

    #[cfg(feature = "block-creator-stats")]
    fn update_block_creator_stats(
        collation_data: &BlockCollationData,
        block_create_stats: &mut Dict<HashBytes, CreatorStats>,
    ) -> Result<()> {
        let mut mc_updated = false;
        let gen_utime = collation_data.common.gen_utime;
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
                    updated_at: gen_utime,
                    total: total_mc,
                    cnt2048: total_mc,
                    cnt65536: total_mc,
                },
                shard_blocks: BlockCounters {
                    updated_at: gen_utime,
                    total: *count,
                    cnt2048: shard_scaled,
                    cnt65536: shard_scaled,
                },
            })?;
        }
        if !mc_updated {
            block_create_stats.set(collation_data.created_by, CreatorStats {
                mc_blocks: BlockCounters {
                    updated_at: gen_utime,
                    total: 1,
                    cnt2048: 1,
                    cnt65536: 1,
                },
                shard_blocks: BlockCounters {
                    updated_at: gen_utime,
                    total: 0,
                    cnt2048: 0,
                    cnt65536: 0,
                },
            })?;
        }

        let default_shard_blocks_count = collation_data.block_create_count.values().sum();
        block_create_stats.set(HashBytes::default(), CreatorStats {
            mc_blocks: BlockCounters {
                updated_at: gen_utime,
                total: 1,
                cnt2048: 1,
                cnt65536: 1,
            },
            shard_blocks: BlockCounters {
                updated_at: gen_utime,
                total: default_shard_blocks_count,
                cnt2048: default_shard_blocks_count << 32,
                cnt65536: default_shard_blocks_count << 32,
            },
        })?;
        // TODO: prune CreatorStats https://github.com/ton-blockchain/ton/blob/master/validator/impl/collator.cpp#L4191
        Ok(())
    }
}

fn create_merkle_update(
    old_state_root: &Cell,
    new_state_root: &Cell,
    usage_tree: &UsageTree,
) -> Result<MerkleUpdate> {
    let started_at = Instant::now();

    let merkle_update_builder =
        MerkleUpdate::create(old_state_root.as_ref(), new_state_root.as_ref(), usage_tree);
    let state_update = merkle_update_builder.build()?;

    let elapsed = started_at.elapsed();

    tracing::debug!(
        target: tracing_targets::COLLATOR,
        elapsed = %humantime::format_duration(elapsed),
        "merkle update created"
    );

    metrics::histogram!("tycho_collator_create_merkle_update_time").record(elapsed);
    Ok(state_update)
}
