use anyhow::{bail, Result};
use everscale_types::merkle::*;
use everscale_types::models::*;
use everscale_types::prelude::*;
use sha2::Digest;
use tycho_block_util::config::BlockchainConfigExt;
use tycho_block_util::state::ShardStateStuff;

use super::execution_manager::ExecutionManager;
use super::CollatorStdImpl;
use crate::collator::types::{AccountBlocksDict, BlockCollationData, PrevData};
use crate::tracing_targets;
use crate::types::BlockCandidate;

impl CollatorStdImpl {
    pub(super) async fn finalize_block(
        &mut self,
        collation_data: &mut BlockCollationData,
        mut exec_manager: ExecutionManager,
    ) -> Result<(BlockCandidate, ShardStateStuff)> {
        tracing::debug!(
            target: tracing_targets::COLLATOR,
            "Collator ({}): start finalize_block",
            self.collator_descr(),
        );

        let mc_data = &self.working_state().mc_data;
        let prev_shard_data = &self.working_state().prev_shard_data;

        // update shard accounts tree and prepare accounts blocks
        let mut shard_accounts = prev_shard_data.observable_accounts().clone();
        let mut account_blocks = AccountBlocksDict::default();

        let mut new_config_opt: Option<BlockchainConfig> = None;

        for (account_id, updated_shard_account_stuff) in exec_manager.changed_accounts.drain() {
            let config_address = self.working_state().mc_data.config().address;
            if collation_data.block_id_short.shard.is_masterchain() && config_address == account_id
            {
                let binding = &updated_shard_account_stuff.shard_account.account;
                let account_root = binding.inner().clone();
                let new_config = BlockchainConfig {
                    address: config_address,
                    params: BlockchainConfigParams::load_from(&mut account_root.as_slice()?)?,
                };
                new_config_opt = Some(new_config);
            }
            let account = updated_shard_account_stuff.shard_account.load_account()?;
            match &account {
                None => {
                    shard_accounts.remove(updated_shard_account_stuff.account_addr)?;
                }
                Some(account) => {
                    shard_accounts.set(
                        updated_shard_account_stuff.account_addr,
                        &DepthBalanceInfo {
                            split_depth: 0, // TODO: fix
                            balance: account.balance.clone(),
                        },
                        &updated_shard_account_stuff.shard_account,
                    )?;
                }
            }
            if collation_data.block_id_short.shard.is_masterchain() {
                updated_shard_account_stuff
                    .update_public_libraries(&mut exec_manager.libraries, account)?;
            }
            let acc_block = AccountBlock {
                account: updated_shard_account_stuff.account_addr,
                transactions: updated_shard_account_stuff.transactions,
                state_update: updated_shard_account_stuff.state_update, // TODO: fix state update
            };

            if !acc_block.transactions.is_empty() {
                account_blocks.set(account_id, acc_block.transactions.root_extra(), &acc_block)?;
            }
        }

        // TODO: update new_config_opt from hard fork

        // calc value flow
        let mut value_flow = collation_data.value_flow.clone();

        let mut in_msgs = InMsgDescr::new();
        // TODO: use more effective algorithm than iter and set
        for (msg_id, msg) in collation_data.in_msgs.iter() {
            in_msgs.set(msg_id, msg.compute_fees()?, msg)?;
        }
        value_flow.imported = in_msgs.root_extra().value_imported.clone();

        let mut out_msgs = OutMsgDescr::new();
        // TODO: use more effective algorithm than iter and set
        for (msg_id, msg) in collation_data.out_msgs.iter() {
            out_msgs.set(msg_id, msg.compute_exported_value()?, msg)?;
        }
        value_flow.exported = out_msgs.root_extra().clone();
        value_flow.fees_collected = account_blocks.root_extra().clone();
        value_flow
            .fees_collected
            .try_add_assign_tokens(in_msgs.root_extra().fees_collected)?;
        value_flow
            .fees_collected
            .try_add_assign(&value_flow.fees_imported)?;
        value_flow
            .fees_collected
            .try_add_assign(&value_flow.created)?;
        value_flow.to_next_block = shard_accounts.root_extra().balance.clone();

        // build master state extra or get a ref to last applied master block
        // TODO: extract min_ref_mc_seqno from processed_upto info when we have many shards
        // collation_data.update_ref_min_mc_seqno(min_ref_mc_seqno);
        let (mc_state_extra, master_ref) = if collation_data.block_id_short.shard.is_masterchain() {
            let (extra, min_ref_mc_seqno) =
                self.create_mc_state_extra(collation_data, new_config_opt)?;
            collation_data.update_ref_min_mc_seqno(min_ref_mc_seqno);
            (Some(extra), None)
        } else {
            (None, Some(mc_data.get_master_ref()))
        };

        // build block info
        let mut new_block_info = BlockInfo {
            version: 0,
            ..Default::default()
        };
        new_block_info.set_prev_ref(&prev_shard_data.get_blocks_ref()?);

        // TODO: should set when slpit/merge logic implemented
        // info.after_merge = false;
        // info.before_split = false;
        // info.after_split = false;
        // info.want_split = false;
        // info.want_merge = false;

        if matches!(mc_state_extra, Some(ref extra) if extra.after_key_block) {
            new_block_info.key_block = true;
        }
        new_block_info.shard = collation_data.block_id_short.shard;
        new_block_info.seqno = collation_data.block_id_short.seqno;
        new_block_info.gen_utime = collation_data.gen_utime;
        new_block_info.gen_utime_ms = collation_data.gen_utime_ms;
        new_block_info.start_lt = collation_data.start_lt;
        new_block_info.end_lt = collation_data.max_lt + 1;
        new_block_info.gen_validator_list_hash_short =
            self.collation_session.collators().short_hash;
        new_block_info.gen_catchain_seqno = self.collation_session.seqno();
        new_block_info.min_ref_mc_seqno = collation_data.min_ref_mc_seqno()?;
        new_block_info.prev_key_block_seqno = mc_data.prev_key_block_seqno();
        new_block_info.master_ref = master_ref.as_ref().map(Lazy::new).transpose()?;
        let global_version = mc_data.config().get_global_version()?;
        if global_version
            .capabilities
            .contains(GlobalCapability::CapReportVersion)
        {
            new_block_info.set_gen_software(Some(GlobalVersion {
                version: self.config.supported_block_version,
                capabilities: self.config.supported_capabilities.into(),
            }));
        }

        // build new state
        let global_id = prev_shard_data.observable_states()[0].state().global_id;
        let mut new_state = Box::new(ShardStateUnsplit {
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
            overload_history: 0,
            underload_history: 0,
            total_balance: value_flow.to_next_block.clone(),
            total_validator_fees: prev_shard_data.total_validator_fees().clone(),
            libraries: Dict::new(),
            master_ref,
            custom: mc_state_extra.as_ref().map(Lazy::new).transpose()?,
            #[cfg(feature = "venom")]
            shard_block_refs: None,
        });

        new_state
            .total_validator_fees
            .try_add_assign(&value_flow.fees_collected)?;
        new_state
            .total_validator_fees
            .try_sub_assign(&value_flow.recovered)?;

        if collation_data.block_id_short.shard.is_masterchain() {
            new_state.libraries = exec_manager.libraries;
        }

        // TODO: update smc on hard fork

        // calc merkle update
        let new_state_root = CellBuilder::build_from(&new_state)?;
        let state_update = Self::create_merkle_update(
            &self.collator_descr,
            prev_shard_data,
            &new_state_root,
            &self.working_state().usage_tree,
        )?;

        // calc block extra
        let mut new_block_extra = BlockExtra {
            in_msg_description: Lazy::new(&in_msgs)?,
            out_msg_description: Lazy::new(&out_msgs)?,
            account_blocks: Lazy::new(&account_blocks)?,
            rand_seed: collation_data.rand_seed,
            created_by: collation_data.created_by,
            ..Default::default()
        };

        if let Some(mc_state_extra) = mc_state_extra {
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
        }

        // construct block
        let new_block = Block {
            global_id,
            info: Lazy::new(&new_block_info)?,
            value_flow: Lazy::new(&value_flow)?,
            state_update: Lazy::new(&state_update)?,
            // do not use out msgs queue updates
            out_msg_queue_updates: None,
            extra: Lazy::new(&new_block_extra)?,
        };
        let new_block_root = CellBuilder::build_from(&new_block)?;
        let new_block_boc = everscale_types::boc::Boc::encode(&new_block_root);
        let new_block_id = BlockId {
            shard: collation_data.block_id_short.shard,
            seqno: collation_data.block_id_short.seqno,
            root_hash: *new_block_root.repr_hash(),
            file_hash: sha2::Sha256::digest(&new_block_boc).into(),
        };

        // TODO: build collated data from collation_data.shard_top_block_descriptors
        let collated_data = vec![];

        let block_candidate = BlockCandidate {
            block_id: new_block_id,
            block: new_block,
            prev_blocks_ids: prev_shard_data.blocks_ids().clone(),
            top_shard_blocks_ids: collation_data.top_shard_blocks_ids.clone(),
            data: new_block_boc,
            collated_data,
            collated_file_hash: HashBytes::ZERO,
            chain_time: (new_block_info.gen_utime as u64 * 1000)
                + new_block_info.gen_utime_ms as u64,
        };

        let new_state_stuff = ShardStateStuff::from_state_and_root(
            &new_block_id,
            new_state,
            new_state_root,
            &self.state_tracker,
        )?;

        Ok((block_candidate, new_state_stuff))
    }

    fn create_mc_state_extra(
        &self,
        collation_data: &mut BlockCollationData,
        new_config_opt: Option<BlockchainConfig>,
    ) -> Result<(McStateExtra, u32)> {
        let prev_shard_data = &self.working_state().prev_shard_data;
        let prev_state = &prev_shard_data.observable_states()[0];

        // 1. update config params and detect key block
        let prev_state_extra = prev_state.state_extra()?;
        let prev_config = &prev_state_extra.config;
        let (config, is_key_block) = if let Some(new_config) = new_config_opt {
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
            &prev_state.block_id().seqno,
            &KeyMaxLt {
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
        collation_data: &mut BlockCollationData,
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

    fn create_merkle_update(
        collator_descr: &str,
        prev_shard_data: &PrevData,
        new_state_root: &Cell,
        usage_tree: &UsageTree,
    ) -> Result<MerkleUpdate> {
        let timer = std::time::Instant::now();

        let merkle_update_builder = MerkleUpdate::create(
            prev_shard_data.pure_state_root().as_ref(),
            new_state_root.as_ref(),
            usage_tree,
        );
        let state_update = merkle_update_builder.build()?;

        tracing::debug!(
            "Collator ({}): merkle update created in {}ms",
            collator_descr,
            timer.elapsed().as_millis(),
        );

        Ok(state_update)
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
