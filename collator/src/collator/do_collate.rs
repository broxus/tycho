use std::{collections::HashMap, ops::Deref};
use std::sync::atomic::Ordering;

use anyhow::{anyhow, bail, Result};
use everscale_types::models::*;
use everscale_types::num::Tokens;
use everscale_types::prelude::*;
use sha2::Digest;

use super::CollatorStdImpl;
use crate::collator::execution_manager::ExecutionManager;
use crate::collator::types::{BlockCollationData, McData, PrevData, ShardDescriptionExt};
use crate::internal_queue::iterator::QueueIterator;
use crate::tracing_targets;
use crate::types::BlockCollationResult;

impl CollatorStdImpl {
    pub(super) async fn do_collate(
        &mut self,
        next_chain_time: u64,
        top_shard_blocks_info: Vec<(BlockId, BlockInfo, ValueFlow)>,
    ) -> Result<()> {
        //TODO: make real implementation
        let mc_data = &self.working_state().mc_data;
        let prev_shard_data = &self.working_state().prev_shard_data;

        let _tracing_top_shard_blocks_descr = if top_shard_blocks_info.is_empty() {
            "".to_string()
        } else {
            format!(
                ", top_shard_blocks: {:?}",
                top_shard_blocks_info
                    .iter()
                    .map(|(id, _, _)| id.as_short_id().to_string())
                    .collect::<Vec<_>>()
                    .as_slice(),
            )
        };
        tracing::trace!(
            target: tracing_targets::COLLATOR,
            "Collator ({}{}): next chain time: {}: start collating block...",
            self.collator_descr(),
            _tracing_top_shard_blocks_descr,
            next_chain_time,
        );

        // generate seed from the chain_time from the anchor
        let hash_bytes = sha2::Sha256::digest(next_chain_time.to_be_bytes());
        let rand_seed = HashBytes::from_slice(hash_bytes.as_slice());
        tracing::trace!(
            target: tracing_targets::COLLATOR,
            "Collator ({}{}): next chain time: {}: rand_seed from chain time: {}",
            self.collator_descr(),
            _tracing_top_shard_blocks_descr,
            next_chain_time,
            rand_seed,
        );

        // prepare block collation data
        //STUB: consider split/merge in future for taking prev_block_id
        let prev_block_id = prev_shard_data.blocks_ids()[0];
        let mut collation_data = BlockCollationData::default();
        collation_data.block_id_short = BlockIdShort {
            shard: prev_block_id.shard,
            seqno: prev_block_id.seqno + 1,
        };
        collation_data.rand_seed = rand_seed;

        // init ShardHashes descriptions for master
        if collation_data.block_id_short.shard.is_masterchain() {
            let mut shards = HashMap::new();
            for (top_block_id, top_block_info, top_block_value_flow) in top_shard_blocks_info {
                let mut shard_descr = ShardDescription::from_block_info(
                    top_block_id,
                    &top_block_info,
                    &top_block_value_flow,
                );
                shard_descr.reg_mc_seqno = collation_data.block_id_short.seqno;

                collation_data.update_shards_max_end_lt(shard_descr.end_lt);

                shards.insert(top_block_id.shard, Box::new(shard_descr));
                collation_data.top_shard_blocks_ids.push(top_block_id);
            }
            collation_data.set_shards(shards);

            //TODO: setup ShardFees and update `collation_data.value_flow.fees_*`
        }

        collation_data.update_ref_min_mc_seqno(mc_data.mc_state_stuff().state().seqno);
        collation_data.chain_time = next_chain_time as u32;
        collation_data.start_lt = Self::calc_start_lt(
            self.collator_descr(),
            mc_data,
            prev_shard_data,
            &collation_data,
        )?;
        collation_data.max_lt = collation_data.start_lt + 1;

        collation_data.processed_upto = prev_shard_data.processed_upto().clone();

        // compute created / minted / recovered / from_prev_block
        self.update_value_flow(mc_data, prev_shard_data, &mut collation_data)?;

        // init execution manager
        let mut exec_manager = ExecutionManager::new(
            collation_data.chain_time,
            collation_data.start_lt,
            collation_data.max_lt,
            collation_data.rand_seed,
            mc_data.mc_state_stuff().state().libraries.clone(),
            mc_data.config().clone(),
            self.config.supported_block_version,
            self.config.max_collate_threads as u32,
            prev_shard_data.observable_accounts().clone(),
        );

        // prepare to read and execute internals and externals
        let (max_messages_per_set, min_externals_per_set, group_size) =
            self.get_msgs_execution_params();
        let mut all_existing_internals_finished = false;
        let mut all_new_internals_finished = false;

        let mut block_limits_reached = false;
        let mut block_transactions_count = 0;

        let mut int_msgs_iter = self.init_internal_mq_iterator().await?;

        // temporary buffer to store first received new internals
        // because we should not take them until existing internals not processed
        let mut new_int_msgs_tmp_buffer: Vec<(MsgInfo, Cell)> = vec![];

        loop {
            // build messages set
            let mut msgs_set: Vec<(MsgInfo, Cell)> = vec![];

            // 1. First try to read min externals amount
            let mut ext_msgs = if self.has_pending_externals {
                self.read_next_externals(min_externals_per_set, &mut collation_data)
                    .await?
            } else {
                vec![]
            };

            // 2. Then iterate through existing internals and try to fill the set
            let mut remaining_capacity = max_messages_per_set - ext_msgs.len();
            while remaining_capacity > 0 && !all_existing_internals_finished {
                match int_msgs_iter.next() {
                    Some(int_msg) if !int_msg.is_new => {
                        //TODO: add existing internal message to set
                        // msgs_set.push((..., ...));
                        remaining_capacity -= 1;
                    }
                    Some(int_msg) => {
                        //TODO: store new internal in the buffer
                        // because we should not take it until existing internals not processed
                        // new_int_msgs_tmp_buffer.push((..., ...));
                        all_existing_internals_finished = true;
                        //TODO: we should pass `return_new: bool` into `QueueIterator::next()` and
                        //      do not return new internal if `return_new == false`
                    }
                    None => {
                        all_existing_internals_finished = true;
                    }
                }
            }

            // 3. Join existing internals and externals
            //    If not enough existing internals to fill the set then try read more externals
            msgs_set.append(&mut ext_msgs);
            remaining_capacity = max_messages_per_set - msgs_set.len();
            if remaining_capacity > 0 && self.has_pending_externals {
                ext_msgs = self
                    .read_next_externals(remaining_capacity, &mut collation_data)
                    .await?;
                msgs_set.append(&mut ext_msgs);
            }

            // 4. When all externals and existing internals finished (the collected set is empty here)
            //    fill next messages sets with new internals
            if msgs_set.is_empty() {
                if !new_int_msgs_tmp_buffer.is_empty() {
                    remaining_capacity -= new_int_msgs_tmp_buffer.len();
                    msgs_set.append(&mut new_int_msgs_tmp_buffer);
                }
                while remaining_capacity > 0 && !all_new_internals_finished {
                    match int_msgs_iter.next() {
                        Some(int_msg) => {
                            //TODO: add internal message to set
                            // msgs_set.push((..., ...));
                            remaining_capacity -= 1;
                        }
                        None => {
                            all_new_internals_finished = true;
                        }
                    }
                }
            }

            if msgs_set.is_empty() {
                // no any messages to process - exit loop
                break;
            }

            //TODO: here use exec_manager to execute msgs set, get transactions,
            //      and update collation_data from them
            exec_manager.execute_msgs_set(msgs_set);
            let mut msgs_set_offset: u32 = 0;
            let mut msgs_set_full_processed = false;
            let mut result = HashMap::new();
            //STUB: currently emulate msgs processing by groups
            //      check block limits by transactions count
            while !msgs_set_full_processed {
                let one_tick_processed_msgs: Vec<_> = if msgs_set.len() > group_size {
                    msgs_set.drain(..group_size).collect()
                } else {
                    msgs_set.drain(..).collect()
                };

                msgs_set_offset += one_tick_processed_msgs.len() as u32;

                let (new_offset, group) = exec_manager.tick(msgs_set_offset).await?;
                for (account_id, msg_info, transaction) in group {
                    let internal_msgs = new_transaction(&mut collation_data, &transaction, msg_info)?;
                    collation_data.max_lt = exec_manager.max_lt.load(Ordering::Acquire);
                    // if !check_limits(tick_res) {
                    //     break;
                    // }
                    // TODO: check our for internal
                    // our  = shard ident contain msg src
                    //
                    let transactions: &mut Vec<_> = result.entry(account_id).or_default();
                    transactions.push(transaction);
                }
                msgs_set_offset = new_offset;

                //TODO: when processing transactions we should check block limits
                //      currently we simply check only transactions count
                //      but needs to make good implementation futher
                block_transactions_count += one_tick_processed_msgs.len();
                if block_transactions_count >= 14 {
                    block_limits_reached = true;
                    break;
                }

                if msgs_set.is_empty() {
                    msgs_set_full_processed = true;
                }
            }

            // store how many msgs from the set were processed (offset)
            Self::update_processed_upto_execution_offset(
                &mut collation_data,
                msgs_set_full_processed,
                msgs_set_offset,
            );

            if block_limits_reached {
                // block is full - exit loop
                break;
            }
        }

        // build block candidate and new state
        let (candidate, new_state_stuff) = self
            .finalize_block(&mut collation_data, exec_manager)
            .await?;

        // return collation result
        let collation_result = BlockCollationResult {
            candidate,
            new_state_stuff: new_state_stuff.clone(),
        };
        self.listener.on_block_candidate(collation_result).await?;
        tracing::info!(
            target: tracing_targets::COLLATOR,
            "Collator ({}{}): STUB: created and sent empty block candidate...",
            self.collator_descr(),
            _tracing_top_shard_blocks_descr,
        );

        // update PrevData in working state
        self.update_working_state(new_state_stuff)?;

        Ok(())
    }

    /// `(set_size, min_externals_per_set, group_size)`
    /// * `set_size` - max num of messages to be processed in one iteration;
    /// * `min_externals_per_set` - min num of externals that should be included in the set
    ///                           when there are a lot of internals and externals
    /// * `group_size` - max num of accounts to be processed in one tick
    fn get_msgs_execution_params(&self) -> (usize, usize, usize) {
        //TODO: should get this from BlockchainConfig
        (10, 4, 3)
    }

    /// Read specified number of externals from imported anchors
    /// using actual `processed_upto` info
    async fn read_next_externals(
        &mut self,
        count: usize,
        collation_data: &mut BlockCollationData,
    ) -> Result<Vec<(MsgInfo, Cell)>> {
        //TODO: was written in a hurry, should be reviewed and optimized

        // when we already read externals in this collation then continue from read_to
        let read_from_opt = if collation_data.externals_reading_started {
            collation_data
                .processed_upto
                .externals
                .as_ref()
                .map(|upto| upto.read_to)
        } else {
            collation_data
                .processed_upto
                .externals
                .as_ref()
                .map(|upto| upto.processed_to)
        };
        collation_data.externals_reading_started = true;

        let read_from = read_from_opt.unwrap_or_default();

        // when read_from is not defined on the blockchain start
        // then read from the first available anchor
        let mut read_from_anchor_opt = None;
        let mut last_read_anchor_opt = None;
        let mut read_next_anchor_from_msg_idx = read_from.1 as usize;
        let mut msgs_read_from_last_anchor = 0;
        let mut total_msgs_read = 0;
        let mut ext_messages = vec![];
        let mut unread_msgs_left_in_last_read_anchor = false;
        'read_anchors: while let Some(entry) = self.anchors_cache.first_entry() {
            let key = *entry.key();
            if key < read_from.0 {
                // skip already read anchors
                let _ = entry.remove();
                continue;
            } else {
                if read_from_anchor_opt.is_none() {
                    read_from_anchor_opt = Some(key);
                }
                last_read_anchor_opt = Some(key);

                // get iterator and read messages
                let anchor = entry.get();
                let mut iter = anchor.externals_iterator(read_next_anchor_from_msg_idx);
                read_next_anchor_from_msg_idx = 0;
                msgs_read_from_last_anchor = 0;
                while let Some(ext_msg) = iter.next() {
                    ext_messages.push(ext_msg.deref().into());
                    msgs_read_from_last_anchor += 1;
                    total_msgs_read += 1;

                    // stop reading if target msgs count reached
                    if total_msgs_read == count {
                        unread_msgs_left_in_last_read_anchor = iter.count() > 0;
                        break 'read_anchors;
                    }
                }
            }
        }

        // update read up to info
        if let Some(last_read_anchor) = last_read_anchor_opt {
            if let Some(externals_upto) = collation_data.processed_upto.externals.as_mut() {
                externals_upto.read_to = (last_read_anchor, msgs_read_from_last_anchor);
            } else {
                collation_data.processed_upto.externals = Some(ExternalsProcessedUpto {
                    processed_to: (read_from_anchor_opt.unwrap(), read_from.1),
                    read_to: (last_read_anchor, msgs_read_from_last_anchor),
                });
            }
        }

        // check if we still have pending externals
        if unread_msgs_left_in_last_read_anchor {
            self.has_pending_externals = true;
        } else {
            //TODO: should make other implementation without iterating through all anchors in cache
            self.has_pending_externals = self.anchors_cache.iter().any(|(id, anchor)| {
                if let Some(ref last_read_anchor) = last_read_anchor_opt {
                    id != last_read_anchor && anchor.has_externals()
                } else {
                    anchor.has_externals()
                }
            });
        }

        Ok(ext_messages)
    }

    /// (TODO) Update `processed_upto` info in `collation_data`
    /// from the processed offset info
    fn update_processed_upto_execution_offset(
        collation_data: &mut BlockCollationData,
        msgs_set_full_processed: bool,
        msgs_set_offset: u32,
    ) {
        if msgs_set_full_processed {
            // externals read window full processed
            let ext_proc_upto = collation_data.processed_upto.externals.as_mut().unwrap();
            ext_proc_upto.processed_to = ext_proc_upto.read_to;

            //TODO: set internals read window full pocessed

            collation_data.processed_upto.processed_offset = 0;
        } else {
            // otherwise read windows stay unchanged, only update processed offset
            collation_data.processed_upto.processed_offset = msgs_set_offset;
        }
    }

    /// Get max LT from masterchain (and shardchain) then calc start LT
    fn calc_start_lt(
        collator_descr: &str,
        mc_data: &McData,
        prev_shard_data: &PrevData,
        collation_data: &BlockCollationData,
    ) -> Result<u64> {
        tracing::trace!("Collator ({}): calc_start_lt()", collator_descr);

        let mut start_lt = if !collation_data.block_id_short.shard.is_masterchain() {
            std::cmp::max(
                mc_data.mc_state_stuff().state().gen_lt,
                prev_shard_data.gen_lt(),
            )
        } else {
            std::cmp::max(
                mc_data.mc_state_stuff().state().gen_lt,
                collation_data.shards_max_end_lt(),
            )
        };

        let align = mc_data.get_lt_align();
        let incr = align - start_lt % align;
        if incr < align || 0 == start_lt {
            if start_lt >= (!incr + 1) {
                bail!("cannot compute start logical time (uint64 overflow)");
            }
            start_lt += incr;
        }

        tracing::debug!(
            "Collator ({}): start_lt set to {}",
            collator_descr,
            start_lt,
        );

        Ok(start_lt)
    }

    fn update_value_flow(
        &self,
        mc_data: &McData,
        prev_shard_data: &PrevData,
        collation_data: &mut BlockCollationData,
    ) -> Result<()> {
        tracing::trace!("Collator ({}): update_value_flow()", self.collator_descr);

        if collation_data.block_id_short.shard.is_masterchain() {
            collation_data.value_flow.created.tokens =
                mc_data.config().get_block_creation_reward(true)?;

            collation_data.value_flow.recovered = collation_data.value_flow.created.clone();
            collation_data
                .value_flow
                .recovered
                .try_add_assign(&collation_data.value_flow.fees_collected)?;
            collation_data
                .value_flow
                .recovered
                .try_add_assign(&mc_data.mc_state_stuff().state().total_validator_fees)?;

            match mc_data.config().get_fee_collector_address() {
                Err(_) => {
                    tracing::debug!(
                        "Collator ({}): fee recovery disabled (no collector smart contract defined in configuration)",
                        self.collator_descr,
                    );
                    collation_data.value_flow.recovered = CurrencyCollection::default();
                }
                Ok(_addr) => {
                    if collation_data.value_flow.recovered.tokens < Tokens::new(1_000_000_000) {
                        tracing::debug!(
                            "Collator({}): fee recovery skipped ({:?})",
                            self.collator_descr,
                            collation_data.value_flow.recovered,
                        );
                        collation_data.value_flow.recovered = CurrencyCollection::default();
                    }
                }
            };

            collation_data.value_flow.minted = self.compute_minted_amount(mc_data)?;

            if collation_data.value_flow.minted != CurrencyCollection::ZERO
                && mc_data.config().get_minter_address().is_err()
            {
                tracing::warn!(
                    "Collator ({}): minting of {:?} disabled: no minting smart contract defined",
                    self.collator_descr,
                    collation_data.value_flow.minted,
                );
                collation_data.value_flow.minted = CurrencyCollection::default();
            }
        } else {
            collation_data.value_flow.created.tokens =
                mc_data.config().get_block_creation_reward(false)?;
            //TODO: should check if it is good to cast `prefix_len` from u16 to u8
            collation_data.value_flow.created.tokens >>=
                collation_data.block_id_short.shard.prefix_len() as u8;
        }
        // info: `prev_data.observable_accounts().root_extra().balance` is `prev_data.total_balance()` in old node
        collation_data.value_flow.from_prev_block = prev_shard_data
            .observable_accounts()
            .root_extra()
            .balance
            .clone();
        Ok(())
    }

    fn compute_minted_amount(&self, mc_data: &McData) -> Result<CurrencyCollection> {
        //TODO: just copied from old node, needs to review
        tracing::trace!("Collator ({}): compute_minted_amount", self.collator_descr);

        let mut to_mint = CurrencyCollection::default();

        let to_mint_cp = match mc_data.config().get::<ConfigParam7>() {
            Ok(Some(v)) => v,
            _ => {
                tracing::warn!(
                    "Collator ({}): Can't get config param 7 (to_mint)",
                    self.collator_descr,
                );
                return Ok(to_mint);
            }
        };

        let old_global_balance = &mc_data.mc_state_extra().global_balance;
        for item in to_mint_cp.as_dict().iter() {
            let (key, amount) = item?;
            let amount2 = old_global_balance
                .other
                .as_dict()
                .get(key)?
                .unwrap_or_default();
            if amount > amount2 {
                let delta = amount.checked_sub(&amount2).ok_or_else(|| {
                    anyhow!(
                        "amount {:?} should sub amount2 {:?} without overflow",
                        amount,
                        amount2,
                    )
                })?;
                tracing::debug!(
                    "{}: currency #{}: existing {:?}, required {:?}, to be minted {:?}",
                    self.collator_descr,
                    key,
                    amount2,
                    amount,
                    delta,
                );
                if key != 0 {
                    to_mint.other.as_dict_mut().set(key, delta)?;
                }
            }
        }

        Ok(to_mint)
    }
}

/// add in and out messages from to block, and to new message queue
fn new_transaction(
    colator_data: &mut BlockCollationData,
    transaction: &Transaction,
    in_msg: MsgInfo,
) -> Result<Vec<OutMsg>> {
    tracing::trace!("new transaction, message {:?}\n{:?}", in_msg, transaction,);
    colator_data.execute_count += 1;
    // let gas_used = transaction.gas_used().unwrap_or(0);
    // colator_data
    //     .block_limit_status
    //     .add_gas_used(gas_used as u32);
    // colator_data
    //     .block_limit_status
    //     .add_transaction(transaction.lt == colator_data.start_lt + 1);

    if let Some(ref in_msg_cell) = transaction.in_msg {
        let in_msg = match in_msg {
            // TODO: fix routing
            MsgInfo::Int(IntMsgInfo { fwd_fee, .. }) => InMsg::Immediate(InMsgFinal {
                in_msg_envelope: Lazy::new(&MsgEnvelope {
                    cur_addr: IntermediateAddr::FULL_SRC_SAME_WORKCHAIN, // TODO: fix calculation
                    next_addr: IntermediateAddr::FULL_SRC_SAME_WORKCHAIN, // TODO: fix calculation
                    fwd_fee_remaining: Default::default(),               // TODO: fix calculation
                    message: Lazy::new(&OwnedMessage::load_from(&mut in_msg_cell.as_slice()?)?)?,
                })?,
                transaction: Lazy::new(&transaction.clone())?,
                fwd_fee,
            }),
            MsgInfo::ExtIn(_) => InMsg::External(InMsgExternal {
                in_msg: Lazy::new(&OwnedMessage::load_from(&mut in_msg_cell.as_slice()?)?)?,
                transaction: Lazy::new(&transaction.clone())?,
            }),
            MsgInfo::ExtOut(_) => {
                bail!("External outbound message cannot be input")
            }
        };
        colator_data.in_msgs.set(
            in_msg_cell.repr_hash().clone(),
            in_msg.compute_fees()?,
            in_msg,
        )?;
    }
    let mut result = vec![];
    for out_msg in transaction.iter_out_msgs() {
        let message = out_msg?;
        let msg_cell = CellBuilder::build_from(&message)?;
        let out_msg = match message.info {
            MsgInfo::Int(_) => {
                colator_data.enqueue_count += 1;
                colator_data.out_msg_count += 1;
                // TODO: fix routing
                // Add to message block here for counting time later it may be replaced
                OutMsg::New(OutMsgNew {
                    out_msg_envelope: Lazy::new(&MsgEnvelope {
                        cur_addr: IntermediateAddr::FULL_SRC_SAME_WORKCHAIN, // TODO: fix calculation
                        next_addr: IntermediateAddr::FULL_SRC_SAME_WORKCHAIN, // TODO: fix calculation
                        fwd_fee_remaining: Default::default(), // TODO: fix calculation
                        message: Lazy::new(&OwnedMessage::load_from(&mut msg_cell.as_slice()?)?)?,
                    })?,
                    transaction: Lazy::new(&transaction.clone())?,
                })
            }
            MsgInfo::ExtOut(_) => {
                colator_data.out_msg_count += 1;
                OutMsg::External(OutMsgExternal {
                    out_msg: Lazy::new(&OwnedMessage::load_from(&mut msg_cell.as_slice()?)?)?,
                    transaction: Lazy::new(&transaction.clone())?,
                })
            }
            MsgInfo::ExtIn(_) => bail!("External inbound message cannot be output"),
        };

        colator_data.out_msgs.set(
            msg_cell.repr_hash().clone(),
            out_msg.compute_exported_value()?,
            out_msg.clone(),
        )?;
        result.push(out_msg);
    }
    Ok(result)
}
