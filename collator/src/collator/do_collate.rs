use std::collections::HashMap;

use anyhow::{anyhow, bail, Result};
use async_trait::async_trait;

use everscale_types::cell::Cell;
use everscale_types::models::in_message::InMsg;
use everscale_types::models::out_message::OutMsg;
use everscale_types::models::Transaction;
use everscale_types::{
    cell::HashBytes,
    models::{
        AddSub, BlockId, BlockIdShort, BlockInfo, ConfigParam7, CurrencyCollection,
        ShardDescription, ValueFlow,
    },
    num::Tokens,
};
use rand::Rng;
use sha2::Digest;

use crate::{
    collator::{
        collator_processor::execution_manager::ExecutionManager,
        types::{BlockCollationData, McData, OutMsgQueueInfoStuff, PrevData, ShardDescriptionExt},
    },
    mempool::MempoolAdapter,
    msg_queue::{MessageQueueAdapter, QueueIterator},
    state_node::StateNodeAdapter,
    tracing_targets,
    types::BlockCollationResult,
};

use super::super::CollatorEventEmitter;

use super::{CollatorProcessorSpecific, CollatorProcessorStdImpl};

#[async_trait]
pub trait DoCollate<MQ, MP, ST>:
    CollatorProcessorSpecific<MQ, MP, ST> + CollatorEventEmitter + Sized + Send + Sync + 'static
{
    async fn do_collate(
        &mut self,
        next_chain_time: u64,
        top_shard_blocks_info: Vec<(BlockId, BlockInfo, ValueFlow)>,
    ) -> Result<()>;
}

#[async_trait]
impl<MQ, QI, MP, ST> DoCollate<MQ, MP, ST> for CollatorProcessorStdImpl<MQ, QI, MP, ST>
where
    MQ: MessageQueueAdapter,
    QI: QueueIterator + Send + Sync + 'static,
    MP: MempoolAdapter,
    ST: StateNodeAdapter,
{
    async fn do_collate(
        &mut self,
        mut next_chain_time: u64,
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

        //TODO: should consider split/merge in future
        let out_msg_queue_info = prev_shard_data.observable_states()[0]
            .state()
            .load_out_msg_queue_info()
            .unwrap_or_default(); //TODO: should not fail there
        collation_data.out_msg_queue_stuff = OutMsgQueueInfoStuff {
            proc_info: out_msg_queue_info.proc_info,
        };
        collation_data.externals_processed_upto = prev_shard_data.observable_states()[0]
            .state()
            .externals_processed_upto
            .clone();

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

        //STUB: just remove fisrt anchor from cache
        let _ext_msg = self.get_next_external();
        self.set_has_pending_externals(false);

        // TODO: load from DAG
        let msgs_set = vec![];

        // TODO check externals is not exist accounts needed ?

        exec_manager.execute_msgs_set(msgs_set);
        let mut offset = 0;
        let mut finish = false;
        let mut result = HashMap::new();
        while !finish {
            let (new_offset, group) = exec_manager.tick(offset).await?;
            for (account_id, msg_info, transaction) in group {
                result
                    .entry(&account_id)
                    .and_modify(|v: &mut Vec<_>| v.push(transaction))
                    .or_default();

                // TODO: finalize
                // let internal_msgs = new_transaction(&mut collation_data, &tr, tr_cell, in_msg_opt.as_ref())?;
                // collation_data.max_lt = execution_manager.max_lt.load(Ordering::Relaxed);
                // if !check_limits(tick_res) {
                //     break;
                // }
                // TODO: check our for internal
                // our  = shard ident contain msg src
                //
            }
            offset = new_offset;
            if offset == msgs_set.len() as u32 {
                finish = true;
            }
        }

        // TODO: check internals queue

        //STUB: do not execute transactions and produce empty block

        // build block candidate and new state
        let (candidate, new_state_stuff) = self
            .finalize_block(&mut collation_data, exec_manager)
            .await?;

        /*
        //STUB: just send dummy block to collation manager
        let prev_blocks_ids = prev_shard_data.blocks_ids().clone();
        let prev_block_id = prev_blocks_ids[0];
        let collated_block_id_short = BlockIdShort {
            shard: prev_block_id.shard,
            seqno: prev_block_id.seqno + 1,
        };
        let mut builder = CellBuilder::new();
        builder.store_bit(collated_block_id_short.shard.workchain().is_negative())?;
        builder.store_u32(collated_block_id_short.shard.workchain().unsigned_abs())?;
        builder.store_u64(collated_block_id_short.shard.prefix())?;
        builder.store_u32(collated_block_id_short.seqno)?;
        let cell = builder.build()?;
        let hash = cell.repr_hash();
        let collated_block_id = BlockId {
            shard: collated_block_id_short.shard,
            seqno: collated_block_id_short.seqno,
            root_hash: *hash,
            file_hash: *hash,
        };
        let mut new_state = prev_shard_data.pure_states()[0]
            .state()
            .clone();
        new_state.seqno = collated_block_id.seqno;
        let candidate = BlockCandidate::new(
            collated_block_id,
            prev_blocks_ids,
            top_shard_blocks_ids,
            vec![],
            vec![],
            collated_block_id.file_hash,
            next_chain_time,
        );
        */

        let collation_result = BlockCollationResult {
            candidate,
            new_state_stuff: new_state_stuff.clone(),
        };
        self.on_block_candidate_event(collation_result).await?;
        tracing::info!(
            target: tracing_targets::COLLATOR,
            "Collator ({}{}): STUB: created and sent empty block candidate...",
            self.collator_descr(),
            _tracing_top_shard_blocks_descr,
        );

        self.update_working_state(new_state_stuff)?;

        Ok(())
    }
}

impl<MQ, QI, MP, ST> CollatorProcessorStdImpl<MQ, QI, MP, ST> {
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
                .add(&collation_data.value_flow.fees_collected)?;
            collation_data
                .value_flow
                .recovered
                .add(&mc_data.mc_state_stuff().state().total_validator_fees)?;

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
    tr_cell: Cell,
    in_msg_opt: Option<&InMsg>,
) -> Result<Vec<OutMsg>> {
    // log::trace!(
    //     "new transaction, message {:x}\n{}",
    //     in_msg_opt.map(|m| m.message_cell().unwrap().repr_hash()).unwrap_or_default(),
    //     ever_block_json::debug_transaction(transaction.clone()).unwrap_or_default(),
    // );
    colator_data.execute_count += 1;
    let gas_used = transaction.gas_used().unwrap_or(0);
    colator_data
        .block_limit_status
        .add_gas_used(gas_used as u32);
    colator_data
        .block_limit_status
        .add_transaction(transaction.logical_time() == colator_data.start_lt()? + 1);
    if let Some(in_msg) = in_msg_opt {
        colator_data.add_in_msg_to_block(in_msg)?;
    }
    transaction.out_msgs.iterate_slices(|slice| {
        let msg_cell = slice.reference(0)?;
        let msg_hash = msg_cell.repr_hash();
        let msg = Message::construct_from_cell(msg_cell.clone())?;
        match msg.header() {
            CommonMsgInfo::IntMsgInfo(info) => {
                // Add out message to state for counting time and it may be removed if used
                let use_hypercube = !colator_data
                    .config
                    .has_capability(GlobalCapabilities::CapOffHypercube);
                let fwd_fee = *info.fwd_fee();
                let enq = MsgEnqueueStuff::new(
                    msg.clone(),
                    colator_data.out_msg_queue_info.shard(),
                    fwd_fee,
                    use_hypercube,
                )?;
                colator_data.enqueue_count += 1;
                colator_data.msg_queue_depth_sum +=
                    colator_data.out_msg_queue_info.add_message(&enq)?;
                // TODO: add message to internal queue
                // Add to message block here for counting time later it may be replaced
                let out_msg = OutMsg::new(enq.envelope_cell(), tr_cell.clone());
                colator_data.add_out_msg_to_block(msg_hash.clone(), &out_msg)?;
            }
            CommonMsgInfo::ExtOutMsgInfo(_) => {
                let out_msg = OutMsg::external(msg_cell, tr_cell.clone());
                colator_data.add_out_msg_to_block(out_msg.read_message_hash()?, &out_msg)?;
            }
            CommonMsgInfo::ExtInMsgInfo(_) => fail!("External inbound message cannot be output"),
        };
        Ok(true)
    })?;
    Ok(())
}
