use std::sync::atomic::Ordering;
use std::{collections::HashMap, ops::Add, sync::Arc};

use anyhow::{anyhow, bail, Result};

use everscale_types::cell::{Load, Store};
use everscale_types::models::{
    AccountBlock, BlockExtra, BlockInfo, CurrencyCollection, DepthBalanceInfo, PrevBlockRef,
};
use everscale_types::{
    cell::{Cell, CellBuilder, HashBytes, UsageTree},
    dict::Dict,
    merkle::MerkleUpdate,
    models::{
        Block, BlockId, BlockRef, BlockchainConfig, CreatorStats, GlobalCapability, GlobalVersion,
        KeyBlockRef, KeyMaxLt, Lazy, LibDescr, McBlockExtra, McStateExtra, ShardHashes,
        ShardStateUnsplit, WorkchainDescription,
    },
};
use sha2::Digest;
use tycho_block_util::config::BlockchainConfigExt;
use tycho_block_util::state::ShardStateStuff;

use crate::collator::types::{AccountBlocksDict, BlockCollationData, PrevData, ShardAccountStuff};
use crate::types::BlockCandidate;

use super::{execution_manager::ExecutionManager, CollatorStdImpl};

impl CollatorStdImpl {
    pub(super) async fn finalize_block(
        &mut self,
        collation_data: &mut BlockCollationData,
        mut exec_manager: ExecutionManager,
    ) -> Result<(BlockCandidate, ShardStateStuff)> {
        let mc_data = &self.working_state().mc_data;
        let prev_shard_data = &self.working_state().prev_shard_data;

        // update shard accounts tree and prepare accounts blocks
        let mut shard_accounts = prev_shard_data.observable_accounts().clone();
        let mut account_blocks = AccountBlocksDict::default();

        for (account_id, updated_shard_account) in exec_manager.changed_accounts.drain() {
            //TODO: get updated blockchain config if it stored in account
            let account = updated_shard_account.shard_account.load_account()?;
            match account {
                None => {
                    shard_accounts.remove(updated_shard_account.account_addr)?;
                }
                Some(account) => {
                    shard_accounts.set(
                        updated_shard_account.account_addr,
                        &DepthBalanceInfo {
                            split_depth: 0, // TODO: fix
                            balance: account.balance,
                        },
                        &updated_shard_account.shard_account,
                    )?;
                }
            }
            if self.shard_id.is_masterchain() {
                updated_shard_account.update_public_libraries(&mut exec_manager.libraries)?;
            }
            let acc_block = AccountBlock {
                account: updated_shard_account.account_addr,
                transactions: updated_shard_account.transactions,
                state_update: updated_shard_account.state_update, // TODO: fix state update
            };

            if !acc_block.transactions.is_empty() {
                account_blocks.set(account_id, acc_block.transactions.root_extra(), &acc_block)?;
            }
        }

        let mut new_config_opt: Option<BlockchainConfig> = None;

        //TODO: update new_config_opt from hard fork

        // calc value flow
        //TODO: init collation_data.value_flow
        let mut value_flow = collation_data.value_flow.clone();
        //TODO: init collation_data.in_msgs
        value_flow.imported = collation_data.in_msgs.root_extra().value_imported.clone();
        //TODO: init collation_data.out_msgs
        value_flow.exported = collation_data.out_msgs.root_extra().clone();
        value_flow.fees_collected = account_blocks.root_extra().clone();
        value_flow
            .fees_collected
            .try_add_assign_tokens(collation_data.in_msgs.root_extra().fees_collected)?;
        value_flow
            .fees_collected
            .try_add_assign(&value_flow.fees_imported)?;
        value_flow
            .fees_collected
            .try_add_assign(&value_flow.created)?;
        value_flow.to_next_block = shard_accounts.root_extra().balance.clone();

        // build master state extra or get a ref to last applied master block
        //TODO: extract min_ref_mc_seqno from processed_upto info when we have many shards
        let (out_msg_queue_info, _min_ref_mc_seqno) =
            collation_data.out_msg_queue_stuff.get_out_msg_queue_info();
        //collation_data.update_ref_min_mc_seqno(min_ref_mc_seqno);
        let (mc_state_extra, master_ref) = if self.shard_id.is_masterchain() {
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

        //TODO: should set when slpit/merge logic implemented
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
        new_block_info.gen_utime = collation_data.chain_time;
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
        let mut new_state = ShardStateUnsplit {
            global_id,
            shard_ident: new_block_info.shard,
            seqno: new_block_info.seqno,
            vert_seqno: 0,
            gen_utime: new_block_info.gen_utime,
            #[cfg(feature = "venom")]
            gen_utime_ms: info.gen_utime_ms,
            gen_lt: new_block_info.end_lt,
            min_ref_mc_seqno: new_block_info.min_ref_mc_seqno,
            out_msg_queue_info: Lazy::new(&out_msg_queue_info)?,
            // TODO: Check if total fits into 4 refs
            externals_processed_upto: collation_data.externals_processed_upto.clone(),
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
        };

        new_state
            .total_balance
            .try_add_assign(&value_flow.fees_collected)?;

        new_state
            .total_validator_fees
            .try_sub_assign(&value_flow.recovered)?;

        if self.shard_id.is_masterchain() {
            new_state.libraries = exec_manager.libraries;
        }

        //TODO: update smc on hard fork

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
            in_msg_description: Lazy::new(&collation_data.in_msgs)?,
            out_msg_description: Lazy::new(&collation_data.out_msgs)?,
            account_blocks: Lazy::new(&account_blocks)?,
            rand_seed: collation_data.rand_seed,
            ..Default::default()
        };

        //TODO: fill created_by
        //extra.created_by = self.created_by.clone();
        if let Some(mc_state_extra) = mc_state_extra {
            let new_mc_block_extra = McBlockExtra {
                shards: mc_state_extra.shards.clone(),
                fees: collation_data.shard_fees.clone(),
                //TODO: Signatures for previous blocks
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

        //TODO: build collated data from collation_data.shard_top_block_descriptors
        let collated_data = vec![];

        let block_candidate = BlockCandidate::new(
            new_block_id,
            new_block,
            prev_shard_data.blocks_ids().clone(),
            collation_data.top_shard_blocks_ids.clone(),
            new_block_boc,
            collated_data,
            HashBytes::ZERO,
            new_block_info.gen_utime as u64,
        );

        let new_state_stuff = ShardStateStuff::from_state_and_root(
            new_block_id,
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

        let current_chain_time = collation_data.chain_time;
        let prev_chain_time = prev_state.state().gen_utime;

        // 2. update shard_hashes and shard_fees
        let cc_config = config.get_catchain_config()?;
        let workchains = config.get_workchains()?;
        // check if need to start new collation session for shards
        let update_shard_cc = {
            let lifetimes = current_chain_time / cc_config.shard_catchain_lifetime;
            let prev_lifetimes = prev_chain_time / cc_config.shard_catchain_lifetime;
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
        //TODO: check `create_mc_state_extra()` for a reference implementation
        //STUB: currently we do not use validator_info and just do nothing there
        let validator_info = prev_state_extra.validator_info.clone();

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
        //TODO: use AugDict::set when it be implemented
        // prev_blocks.set(
        //     &prev_state.block_id().seqno,
        //     &KeyBlockRef {
        //         is_key_block,
        //         block_ref: prev_blk_ref.clone(),
        //     },
        //     &KeyMaxLt {
        //         has_key_block: is_key_block,
        //         max_end_lt: prev_state.state().gen_lt,
        //     },
        // )?;

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
            self.update_block_creator_stats(collation_data, &mut stats)?;
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
        wc_set: &Dict<i32, WorkchainDescription>,
        update_cc: bool,
    ) -> Result<u32> {
        //TODO: here should be the split/merge logic, refer to old node impl

        //STUB: just do nothing for now: no split/merge, no session rotation
        let mut min_ref_mc_seqno = u32::max_value();
        for (_shard_id, shard_descr) in collation_data.shards_mut()? {
            min_ref_mc_seqno = std::cmp::min(min_ref_mc_seqno, shard_descr.min_ref_mc_seqno);
        }

        Ok(min_ref_mc_seqno)
    }

    fn update_block_creator_stats(
        &self,
        collation_data: &BlockCollationData,
        block_create_stats: &mut Dict<HashBytes, CreatorStats>,
    ) -> Result<()> {
        //TODO: implement if we really need it
        //STUB: do not update anything
        Ok(())
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

        // do not need to calc out_queue_updates

        Ok(state_update)
    }
}
