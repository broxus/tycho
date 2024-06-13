use std::collections::VecDeque;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use everscale_types::models::*;
use everscale_types::num::Tokens;
use everscale_types::prelude::*;
use humantime::format_duration;
use sha2::Digest;
use ton_executor::blockchain_config::PreloadedBlockchainConfig;
use ton_executor::ExecuteParams;
use tycho_util::FastHashMap;

use super::types::CachedMempoolAnchor;
use super::CollatorStdImpl;
use crate::collator::execution_manager::ExecutionManager;
use crate::collator::types::{
    AsyncMessage, BlockCollationData, McData, PrevData, ShardDescriptionExt,
};
use crate::internal_queue::types::InternalMessageKey;
use crate::mempool::MempoolAnchorId;
use crate::tracing_targets;
use crate::types::{BlockCollationResult, ShardIdentExt, TopBlockDescription};

#[cfg(test)]
#[path = "tests/do_collate_tests.rs"]
pub(super) mod tests;

impl CollatorStdImpl {
    #[tracing::instrument(parent =  None, skip_all, fields(block_id = %self.next_block_id_short, ct = next_chain_time))]
    pub(super) async fn do_collate(
        &mut self,
        next_chain_time: u64,
        top_shard_blocks_info: Option<Vec<TopBlockDescription>>,
    ) -> Result<()> {
        let mut timer = std::time::Instant::now();

        let mc_data = &self.working_state().mc_data;
        let prev_shard_data = &self.working_state().prev_shard_data;

        tracing::info!(target: tracing_targets::COLLATOR,
            "Start collating block: top_shard_blocks_ids: {:?}",
            top_shard_blocks_info.as_ref().map(|v| {
                v.iter()
                    .map(|TopBlockDescription { block_id, .. }| block_id.as_short_id().to_string())
                    .collect::<Vec<_>>()
            }),
        );

        // generate seed from the chain_time from the anchor
        let hash_bytes = sha2::Sha256::digest(next_chain_time.to_be_bytes());
        let rand_seed = HashBytes::from_slice(hash_bytes.as_slice());
        tracing::trace!(target: tracing_targets::COLLATOR, "rand_seed from chain time: {}", rand_seed);

        // prepare block collation data
        // STUB: consider split/merge in future for taking prev_block_id
        let prev_block_id = prev_shard_data.blocks_ids()[0];
        let mut collation_data = BlockCollationData::default();
        collation_data.block_id_short = BlockIdShort {
            shard: prev_block_id.shard,
            seqno: prev_block_id.seqno + 1,
        };
        collation_data.rand_seed = rand_seed;
        collation_data.update_ref_min_mc_seqno(mc_data.mc_state_stuff().state().seqno);
        collation_data.gen_utime = (next_chain_time / 1000) as u32;
        collation_data.gen_utime_ms = (next_chain_time % 1000) as u16;
        collation_data.start_lt = Self::calc_start_lt(mc_data, prev_shard_data, &collation_data)?;
        collation_data.next_lt = collation_data.start_lt + 1;

        collation_data.processed_upto = prev_shard_data.processed_upto().clone();
        tracing::debug!(target: tracing_targets::COLLATOR, "initial processed_upto.externals = {:?}",
            collation_data.processed_upto.externals,
        );

        // show intenals proccessed upto
        collation_data
            .processed_upto
            .internals
            .iter()
            .for_each(|result| {
                let (shard_ident, processed_upto) = result.unwrap();
                tracing::debug!(target: tracing_targets::COLLATOR,
                    "initial processed_upto.internals for shard {:?}: {:?}",
                    shard_ident, processed_upto,
                );
            });

        // init ShardHashes descriptions for master
        if self.shard_id.is_masterchain() {
            let shards = prev_shard_data.observable_states()[0]
                .shards()?
                .iter()
                .filter_map(|entry| {
                    entry
                        .ok()
                        .map(|(shard_id, shard_descr)| (shard_id, Box::new(shard_descr)))
                })
                .collect::<FastHashMap<_, _>>();

            collation_data.set_shards(shards);

            if let Some(top_shard_blocks_info) = top_shard_blocks_info {
                self.import_new_shard_top_blocks_for_masterchain(
                    mc_data.config(),
                    &mut collation_data,
                    top_shard_blocks_info,
                )?;
            }
        }

        // compute created / minted / recovered / from_prev_block
        self.update_value_flow(mc_data, prev_shard_data, &mut collation_data)?;

        // prepare to read and execute internals and externals
        let (max_messages_per_set, min_externals_per_set, group_limit, group_vert_size) =
            self.get_msgs_execution_params();

        // init execution manager
        let mut exec_manager = ExecutionManager::new(
            collation_data.next_lt,
            Arc::new(PreloadedBlockchainConfig::with_config(
                mc_data.config().clone(),
                0, // TODO: fix global id
            )?),
            Arc::new(ExecuteParams {
                state_libs: mc_data.libraries().clone(),
                // generated unix time
                block_unixtime: collation_data.gen_utime,
                // block's start logical time
                block_lt: collation_data.start_lt,
                // block random seed
                seed_block: collation_data.rand_seed,
                block_version: self.config.supported_block_version,
                ..ExecuteParams::default()
            }),
            group_limit,
            group_vert_size,
            prev_shard_data.observable_accounts().clone(),
        );

        let do_collate_prepare_elapsed = timer.elapsed();
        timer = std::time::Instant::now();

        // execute tick transaction and special transactions (mint, recover)
        if self.shard_id.is_masterchain() {
            self.create_ticktock_transactions(false, &mut collation_data, &mut exec_manager)
                .await?;
            self.create_special_transactions(&mut collation_data, &mut exec_manager)
                .await?;
        }

        let mut all_existing_internals_finished = false;
        let mut all_new_internals_finished = false;

        let mut block_limits_reached = false;

        let mut do_collate_ticktock_special_elapsed = timer.elapsed();
        timer = std::time::Instant::now();

        let mut internal_messages_iterator = self.init_internal_mq_iterator().await?;

        let do_collate_init_iterator_elapsed = timer.elapsed();
        timer = std::time::Instant::now();

        // indicate that there are still unprocessed internals when collation loop finished
        let mut has_pending_internals = false;

        let mut do_collate_fill_msgs_set_elapsed = Duration::ZERO;
        let mut do_collate_exec_msgs_elapsed = Duration::ZERO;
        let mut do_collate_process_transactions_elapsed = Duration::ZERO;

        loop {
            // let mut one_set_timer = std::time::Instant::now();

            let mut executed_internal_messages = vec![];
            let mut internal_messages_sources = FastHashMap::default();
            // build messages set
            let mut msgs_set: Vec<AsyncMessage> = vec![];

            // 1. First try to read min externals amount
            let mut ext_msgs = if self.has_pending_externals {
                self.read_next_externals(min_externals_per_set, &mut collation_data)?
                    .into_iter()
                    .map(|(msg_info, cell)| AsyncMessage::Ext(msg_info, cell))
                    .collect()
            } else {
                vec![]
            };

            // 2. Then iterate through existing internals and try to fill the set
            let mut remaining_capacity = max_messages_per_set - ext_msgs.len();
            while remaining_capacity > 0 && !all_existing_internals_finished {
                match internal_messages_iterator.next(false)? {
                    Some(int_msg) => {
                        tracing::trace!(target: tracing_targets::COLLATOR,
                            "read existing internal message from iterator: {:?}",
                            int_msg.message_with_source,
                        );
                        let message_with_source = int_msg.message_with_source;
                        let int_msg_info = MsgInfo::Int(message_with_source.message.info.clone());
                        let cell = message_with_source.message.cell.clone();
                        let is_current_shard = message_with_source.shard_id == self.shard_id;

                        let async_message = AsyncMessage::Int(int_msg_info, cell, is_current_shard);

                        msgs_set.push(async_message);

                        internal_messages_sources.insert(
                            message_with_source.message.key(),
                            message_with_source.shard_id,
                        );

                        remaining_capacity -= 1;
                    }
                    None => {
                        all_existing_internals_finished = true;
                    }
                }
            }

            tracing::debug!(target: tracing_targets::COLLATOR,
                ext_count = ext_msgs.len(), int_count = internal_messages_sources.len(),
                "read externals and internals",
            );

            // 3. Join existing internals and externals
            //    If not enough existing internals to fill the set then try read more externals
            msgs_set.append(&mut ext_msgs);
            remaining_capacity = max_messages_per_set - msgs_set.len();
            if remaining_capacity > 0 && self.has_pending_externals {
                ext_msgs = self
                    .read_next_externals(remaining_capacity, &mut collation_data)?
                    .into_iter()
                    .map(|(msg_info, cell)| AsyncMessage::Ext(msg_info, cell))
                    .collect();
                tracing::debug!(target: tracing_targets::COLLATOR,
                    ext_count = ext_msgs.len(),
                    "read additional externals",
                );
                msgs_set.append(&mut ext_msgs);
            }

            // 4. When all externals and existing internals finished (the collected set is empty here)
            //    fill next messages sets with new internals
            if msgs_set.is_empty() {
                let mut new_internal_messages_in_set = 0;
                while remaining_capacity > 0 && !all_new_internals_finished {
                    match internal_messages_iterator.next(true)? {
                        Some(int_msg) => {
                            let message_with_source = int_msg.message_with_source;

                            tracing::trace!(target: tracing_targets::COLLATOR,
                                "read new internal message from iterator: {:?}",
                                message_with_source,
                            );

                            let int_msg_info =
                                MsgInfo::Int(message_with_source.message.info.clone());
                            let cell = message_with_source.message.cell.clone();

                            let async_message = if int_msg.is_new {
                                collation_data.read_new_msgs_from_iterator += 1;
                                AsyncMessage::NewInt(int_msg_info, cell)
                            } else {
                                let is_current_shard =
                                    message_with_source.shard_id == self.shard_id;
                                AsyncMessage::Int(int_msg_info, cell, is_current_shard)
                            };

                            msgs_set.push(async_message);

                            internal_messages_sources.insert(
                                message_with_source.message.key(),
                                message_with_source.shard_id,
                            );
                            new_internal_messages_in_set += 1;

                            remaining_capacity -= 1;
                        }
                        None => {
                            all_new_internals_finished = true;
                        }
                    }
                }
                tracing::debug!(target: tracing_targets::COLLATOR,
                    new_int_count = new_internal_messages_in_set,
                    "read new internals from iterator",
                );
            }

            if msgs_set.is_empty() {
                // no any messages to process
                // move left bracket of the read window
                Self::update_processed_upto_execution_offset(&mut collation_data, true, 0);
                // exit loop
                break;
            }

            let msgs_set_len = msgs_set.len() as u32;
            let mut msgs_set_executed_count = 0;
            exec_manager.set_msgs_for_execution(msgs_set);
            let mut msgs_set_offset = collation_data.processed_upto.processed_offset;
            let mut msgs_set_full_processed = false;

            do_collate_fill_msgs_set_elapsed += timer.elapsed();
            timer = std::time::Instant::now();

            let mut messages_inserted_to_iterator = 0;

            let mut executed_messages = 0;
            // execute msgs processing by groups
            while !msgs_set_full_processed {
                let (new_offset, group, finished) =
                    exec_manager.execute_tick(msgs_set_offset).await?;
                msgs_set_full_processed = finished;

                let one_tick_executed_count = group.len();

                do_collate_exec_msgs_elapsed += timer.elapsed();
                timer = std::time::Instant::now();

                for (_account_id, msg_info, transaction) in group {
                    match msg_info {
                        AsyncMessage::Int(ref msg_info, ref cell, _)
                        | AsyncMessage::NewInt(ref msg_info, ref cell) => {
                            if let MsgInfo::Int(int_msg_info) = msg_info {
                                let hash = cell.repr_hash();
                                let key = InternalMessageKey {
                                    lt: int_msg_info.created_lt,
                                    hash: *hash,
                                };

                                match internal_messages_sources.get(&key).cloned() {
                                    Some(shard_ident) => {
                                        executed_internal_messages.push((shard_ident, key));
                                    }
                                    None => {
                                        return Err(anyhow!(
                                            "Internal message source \
                                            shard not found for key: {:?}",
                                            key
                                        ));
                                    }
                                }
                            }
                        }
                        _ => {}
                    }

                    executed_messages += 1;

                    let new_messages = new_transaction(
                        &mut collation_data,
                        &self.shard_id,
                        transaction.1,
                        msg_info,
                    )?;

                    collation_data.new_msgs_created += new_messages.len() as u32;

                    for (msg_info, cell) in new_messages.iter() {
                        if let MsgInfo::Int(int_msg_info) = msg_info {
                            messages_inserted_to_iterator += 1;
                            collation_data.inserted_new_msgs_to_iterator += 1;

                            messages_inserted_to_iterator += new_messages.len();
                            self.mq_adapter.add_message_to_iterator(
                                &mut internal_messages_iterator,
                                (int_msg_info.clone(), cell.clone()),
                            )?;
                        }
                    }

                    collation_data.next_lt = exec_manager.min_next_lt;
                }
                msgs_set_offset = new_offset;

                do_collate_process_transactions_elapsed += timer.elapsed();
                timer = std::time::Instant::now();

                // TODO: when processing transactions we should check block limits
                //      currently we simply check only transactions count
                //      but needs to make good implementation futher
                msgs_set_executed_count += one_tick_executed_count;
                collation_data.tx_count += one_tick_executed_count as u32;
                tracing::debug!(target: tracing_targets::COLLATOR,
                    "processed messages from set {}/{}, total {}, offset = {}",
                    msgs_set_executed_count, msgs_set_len,
                    collation_data.tx_count, msgs_set_offset,
                );

                if msgs_set_offset == msgs_set_len {
                    msgs_set_full_processed = true;
                }
            }

            // HACK: temporary always full process msgs set and check block limits after
            if collation_data.tx_count >= self.config.block_txs_limit {
                tracing::debug!(target: tracing_targets::COLLATOR,
                    "STUB: block limit reached: {}/{}",
                    collation_data.tx_count, self.config.block_txs_limit,
                );
                block_limits_reached = true;
            }

            tracing::debug!(target: tracing_targets::COLLATOR,
                "Inserted message to iterator last set: {}",
                messages_inserted_to_iterator,
            );

            tracing::debug!(target: tracing_targets::COLLATOR,
                "Executed messages last set: {}",
                executed_messages,
            );

            // commit messages to iterator only if set was fully processed
            if msgs_set_full_processed {
                self.mq_adapter.commit_messages_to_iterator(
                    &mut internal_messages_iterator,
                    executed_internal_messages,
                )?;
            }

            // store how many msgs from the set were processed (offset)
            Self::update_processed_upto_execution_offset(
                &mut collation_data,
                msgs_set_full_processed,
                msgs_set_offset,
            );

            tracing::debug!(target: tracing_targets::COLLATOR, "updated processed_upto.externals = {:?}. offset = {}",
                collation_data.processed_upto.externals,
                collation_data.processed_upto.processed_offset,
            );

            has_pending_internals = internal_messages_iterator.peek(true)?.is_some();

            do_collate_process_transactions_elapsed += timer.elapsed();
            timer = std::time::Instant::now();

            if block_limits_reached {
                // block is full - exit loop
                break;
            }
        }

        // execute tock transaction
        if self.shard_id.is_masterchain() {
            self.create_ticktock_transactions(true, &mut collation_data, &mut exec_manager)
                .await?;
        }

        do_collate_ticktock_special_elapsed += timer.elapsed();
        timer = std::time::Instant::now();

        // let do_collate_execute_elapsed_ms = timer.elapsed().as_millis() as u32;
        // timer = std::time::Instant::now();

        let diff = Arc::new(internal_messages_iterator.take_diff());

        // update internal messages processed_upto info in collation_data
        for (shard_ident, message_key) in diff.processed_upto.iter() {
            let processed_upto = InternalsProcessedUpto {
                processed_to_msg: (message_key.lt, message_key.hash),
                read_to_msg: (message_key.lt, message_key.hash),
            };

            let shard_ident_full: ShardIdentFull = (*shard_ident).into();
            collation_data
                .processed_upto
                .internals
                .set(shard_ident_full, processed_upto.clone())?;
            tracing::debug!(target: tracing_targets::COLLATOR,
                "updated processed_upto.internals for shard {:?}: {:?}",
                shard_ident, processed_upto,
            );
        }

        // build block candidate and new state
        let (candidate, new_state_stuff) = self
            .finalize_block(&mut collation_data, exec_manager)
            .await?;

        self.mq_adapter
            .apply_diff(diff.clone(), candidate.block_id.as_short_id())
            .await?;

        let do_collate_build_block_elapsed = timer.elapsed();
        timer = std::time::Instant::now();

        // return collation result
        let collation_result = BlockCollationResult {
            candidate,
            new_state_stuff: new_state_stuff.clone(),
            has_pending_internals,
        };

        self.listener.on_block_candidate(collation_result).await?;

        // update PrevData in working state
        self.update_working_state(new_state_stuff)?;
        self.update_working_state_pending_internals(Some(has_pending_internals))?;

        let do_collate_update_state_elapsed = timer.elapsed();

        // metrics
        let labels = [("workchain", self.shard_id.workchain().to_string())];

        metrics::counter!("tycho_do_collate_tx_total", &labels)
            .increment(collation_data.tx_count as _);

        metrics::histogram!("tycho_do_collate_msgs_exec_count_all", &labels)
            .record(collation_data.execute_count_all);
        metrics::histogram!("tycho_do_collate_msgs_exec_count_ext", &labels)
            .record(collation_data.execute_count_ext);
        metrics::histogram!("tycho_do_collate_msgs_exec_count_int", &labels)
            .record(collation_data.execute_count_int);
        metrics::histogram!("tycho_do_collate_msgs_exec_count_new_int", &labels)
            .record(collation_data.execute_count_new_int);

        metrics::gauge!("tycho_do_collate_int_msgs_queue_length", &labels)
            .increment(collation_data.enqueue_count);
        metrics::gauge!("tycho_do_collate_int_msgs_queue_length", &labels)
            .decrement(collation_data.dequeue_count);

        let do_collate_execute_elapsed = do_collate_fill_msgs_set_elapsed
            + do_collate_exec_msgs_elapsed
            + do_collate_process_transactions_elapsed;
        let do_collate_total_elapsed = do_collate_prepare_elapsed
            + do_collate_init_iterator_elapsed
            + do_collate_execute_elapsed
            + do_collate_build_block_elapsed
            + do_collate_update_state_elapsed
            + do_collate_ticktock_special_elapsed;
        collation_data.total_execute_msgs_time_mc = do_collate_exec_msgs_elapsed.as_micros();

        metrics::histogram!("tycho_do_collate_total_time", &labels)
            .record(do_collate_total_elapsed);
        metrics::histogram!("tycho_do_collate_prepare_time", &labels)
            .record(do_collate_prepare_elapsed);
        metrics::histogram!("tycho_do_collate_init_iterator_time", &labels)
            .record(do_collate_init_iterator_elapsed);
        metrics::histogram!("tycho_do_collate_execute_time", &labels)
            .record(do_collate_execute_elapsed);
        metrics::histogram!("tycho_do_collate_fill_msgs_set_time", &labels)
            .record(do_collate_fill_msgs_set_elapsed);
        metrics::histogram!("tycho_do_collate_exec_msgs_time", &labels)
            .record(do_collate_exec_msgs_elapsed);
        metrics::histogram!("tycho_do_collate_process_transactions_time", &labels)
            .record(do_collate_process_transactions_elapsed);
        metrics::histogram!("tycho_do_collate_build_block_time", &labels)
            .record(do_collate_build_block_elapsed);
        metrics::histogram!("tycho_do_collate_update_state_time", &labels)
            .record(do_collate_update_state_elapsed);
        if self.shard_id.is_masterchain() {
            metrics::histogram!("tycho_do_collate_ticktock_special_time", &labels)
                .record(do_collate_ticktock_special_elapsed);
        }

        self.update_stats(&collation_data);
        tracing::info!(target: tracing_targets::COLLATOR, "{:?}", self.stats);

        tracing::info!(target: tracing_targets::COLLATOR,
            "Created and sent block candidate: collation_time={}, \
            start_lt={}, end_lt={}, exec_count={}, \
            exec_ext={}, exec_int={}, exec_new_int={}, \
            enqueue_count={}, dequeue_count={}, \
            new_msgs_created={}, new_msgs_added={}, \
            in_msgs={}, out_msgs={}, \
            read_new_msgs_from_iterator={}, inserted_new_msgs_to_iterator={}",
            do_collate_total_elapsed.as_millis(),
            collation_data.start_lt, collation_data.next_lt, collation_data.execute_count_all,
            collation_data.execute_count_ext, collation_data.execute_count_int, collation_data.execute_count_new_int,
            collation_data.enqueue_count, collation_data.dequeue_count,
            collation_data.new_msgs_created, diff.messages.len(),
            collation_data.in_msgs.len(), collation_data.out_msgs.len(),
            collation_data.read_new_msgs_from_iterator, collation_data.inserted_new_msgs_to_iterator,
        );

        assert_eq!(
            collation_data.enqueue_count,
            collation_data.inserted_new_msgs_to_iterator - collation_data.execute_count_new_int
        );

        tracing::info!(
            target: tracing_targets::COLLATOR,
            total = %format_duration(do_collate_total_elapsed),
            prepare = %format_duration(do_collate_prepare_elapsed),
            init_iterator = %format_duration(do_collate_init_iterator_elapsed),
            execute = %format_duration(do_collate_execute_elapsed),
            fill_msgs_set = %format_duration(do_collate_fill_msgs_set_elapsed),
            exec_msgs = %format_duration(do_collate_exec_msgs_elapsed),
            process_transactions = %format_duration(do_collate_process_transactions_elapsed),
            build_block = %format_duration(do_collate_build_block_elapsed),
            update_state = %format_duration(do_collate_update_state_elapsed),
            ticktop_special = %format_duration(do_collate_ticktock_special_elapsed),
            "timings"
        );

        Ok(())
    }

    /// `(set_size, min_externals_per_set, group_limit, group_vert_size)`
    /// * `set_size` - max num of messages to be processed in one iteration;
    /// * `min_externals_per_set` - min num of externals that should be included in the set
    ///                           when there are a lot of internals and externals
    /// * `group_limit` - max num of accounts to be processed in one tick
    /// * `group_vert_size` - max num of messages per account in group
    fn get_msgs_execution_params(&self) -> (usize, usize, usize, usize) {
        // TODO: should get this from BlockchainConfig
        let params = &self.config.msgs_exec_params;
        (
            params.set_size as _,
            params.min_externals_per_set as _,
            params.group_limit as _,
            params.group_vert_size as _,
        )
    }

    /// Read specified number of externals from imported anchors
    /// using actual `processed_upto` info
    fn read_next_externals(
        &mut self,
        count: usize,
        collation_data: &mut BlockCollationData,
    ) -> Result<Vec<(MsgInfo, Cell)>> {
        let (res, has_pending_externals) = Self::read_next_externals_impl(
            &self.shard_id,
            &mut self.anchors_cache,
            count,
            collation_data,
        )?;
        self.has_pending_externals = has_pending_externals;
        Ok(res)
    }

    fn read_next_externals_impl(
        shard_id: &ShardIdent,
        anchors_cache: &mut VecDeque<(MempoolAnchorId, CachedMempoolAnchor)>,
        count: usize,
        collation_data: &mut BlockCollationData,
    ) -> Result<(Vec<(MsgInfo, Cell)>, bool)> {
        tracing::info!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
            "shard: {}, count: {}", shard_id, count,
        );

        // when we already read externals in this collation then continue from read_to
        let was_read_upto_opt = if collation_data.externals_reading_started {
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
        let was_read_upto = was_read_upto_opt.unwrap_or_default();
        tracing::info!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
            "externals_reading_started: {}, was_read_upto: {:?}",
            collation_data.externals_reading_started, was_read_upto,
        );
        collation_data.externals_reading_started = true;

        // when read_from is not defined on the blockchain start
        // then read from the first available anchor
        let mut read_from_anchor_opt = None;
        let mut last_read_anchor_opt = None;
        let mut msgs_read_offset_in_last_anchor = 0;
        let mut total_msgs_collected = 0;
        let mut ext_messages = vec![];
        let mut has_pending_externals_in_last_read_anchor = false;

        let mut next_idx = 0;
        loop {
            tracing::debug!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
                "try read next anchor from cache",
            );
            // try read next anchor
            let next_entry = anchors_cache.get(next_idx);
            let entry = match next_entry {
                Some(entry) => entry,
                // stop reading if there is no next anchor
                None => {
                    tracing::debug!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
                        "no next entry in anchors cache",
                    );
                    break;
                }
            };

            let key = entry.0;
            if key < was_read_upto.0 {
                // skip and remove already processed anchor from cache
                let _ = anchors_cache.remove(next_idx);
                tracing::debug!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
                    "anchor with key {} already processed, removed from anchors cache", key,
                );
                // try read next anchor
                continue;
            } else {
                if read_from_anchor_opt.is_none() {
                    tracing::debug!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
                        "read_from_anchor: {}", key,
                    );
                    read_from_anchor_opt = Some(key);
                }
                last_read_anchor_opt = Some(key);
                tracing::debug!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
                    "last_read_anchor: {}", key,
                );

                let anchor = &entry.1.anchor;

                if key == was_read_upto.0 && anchor.externals_count() == was_read_upto.1 as usize {
                    // skip and remove fully processed anchor
                    let _ = anchors_cache.remove(next_idx);
                    tracing::debug!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
                        "anchor with key {} fully processed, removed from anchors cache", key,
                    );
                    // try read next anchor
                    continue;
                }

                tracing::debug!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
                    "next anchor externals count: {}", anchor.externals_count(),
                );

                if key == was_read_upto.0 {
                    // read first anchor from offset in processed upto
                    msgs_read_offset_in_last_anchor = was_read_upto.1;
                } else {
                    // read every next anchor from 0
                    msgs_read_offset_in_last_anchor = 0;
                }

                tracing::debug!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
                    "msgs_read_offset_in_last_anchor: {}", msgs_read_offset_in_last_anchor,
                );

                // get iterator and read messages
                let mut msgs_collected_from_last_anchor = 0;
                let iter = anchor.externals_iterator(msgs_read_offset_in_last_anchor as usize);
                for ext_msg in iter {
                    tracing::trace!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
                        "read ext_msg dst: {}", ext_msg.info().dst,
                    );
                    if total_msgs_collected < count {
                        msgs_read_offset_in_last_anchor += 1;
                    }
                    if shard_id.contains_address(&ext_msg.info().dst) {
                        if total_msgs_collected < count {
                            // get msgs for target shard until target count reached
                            ext_messages.push(ext_msg.deref().into());
                            total_msgs_collected += 1;
                            msgs_collected_from_last_anchor += 1;
                            tracing::trace!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
                                "collected ext_msg dst: {}", ext_msg.info().dst,
                            );
                        } else {
                            // when target msgs count reached
                            // continue looking thru remaining msgs
                            // to detect if exist unread for target shard
                            has_pending_externals_in_last_read_anchor = true;
                            break;
                        }
                    }
                }
                tracing::debug!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
                    "{} externals collected from anchor {}, msgs_read_offset_in_last_anchor: {}",
                    msgs_collected_from_last_anchor, key, msgs_read_offset_in_last_anchor,
                );
                if total_msgs_collected == count {
                    // stop reading anchors when target msgs count reached
                    break;
                }
                // try read next anchor
                next_idx += 1;
            }
        }

        tracing::info!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
            "total_msgs_collected: {}, target msgs count: {}, has_pending_externals_in_last_read_anchor: {}",
            total_msgs_collected, count, has_pending_externals_in_last_read_anchor,
        );

        // update read up to info
        if let Some(last_read_anchor) = last_read_anchor_opt {
            if let Some(externals_upto) = collation_data.processed_upto.externals.as_mut() {
                externals_upto.read_to = (last_read_anchor, msgs_read_offset_in_last_anchor);
            } else {
                collation_data.processed_upto.externals = Some(ExternalsProcessedUpto {
                    processed_to: (read_from_anchor_opt.unwrap(), was_read_upto.1),
                    read_to: (last_read_anchor, msgs_read_offset_in_last_anchor),
                });
            }
        }

        // check if we still have pending externals
        let has_pending_externals = if has_pending_externals_in_last_read_anchor {
            true
        } else {
            let has_pending_externals = anchors_cache.iter().any(|(id, cached_anchor)| {
                if let Some(ref last_read_anchor) = last_read_anchor_opt {
                    id > last_read_anchor && cached_anchor.has_externals
                } else {
                    cached_anchor.has_externals
                }
            });
            tracing::info!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
                "remaning anchors in cache has pending externals: {}", has_pending_externals,
            );
            has_pending_externals
        };

        Ok((ext_messages, has_pending_externals))
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
            if let Some(externals) = collation_data.processed_upto.externals.as_mut() {
                externals.processed_to = externals.read_to;
            }

            collation_data.processed_upto.processed_offset = 0;
        } else {
            // otherwise read windows stay unchanged, only update processed offset
            collation_data.processed_upto.processed_offset = msgs_set_offset;
        }
    }

    /// Get max LT from masterchain (and shardchain) then calc start LT
    fn calc_start_lt(
        mc_data: &McData,
        prev_shard_data: &PrevData,
        collation_data: &BlockCollationData,
    ) -> Result<u64> {
        tracing::trace!(target: tracing_targets::COLLATOR, "calc_start_lt()");

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

        tracing::debug!(target: tracing_targets::COLLATOR, "start_lt set to {}", start_lt);

        Ok(start_lt)
    }

    fn update_value_flow(
        &self,
        mc_data: &McData,
        prev_shard_data: &PrevData,
        collation_data: &mut BlockCollationData,
    ) -> Result<()> {
        tracing::trace!(target: tracing_targets::COLLATOR, "update_value_flow()");

        if self.shard_id.is_masterchain() {
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
                    tracing::debug!(target: tracing_targets::COLLATOR,
                        "fee recovery disabled (no collector smart contract defined in configuration)",
                    );
                    collation_data.value_flow.recovered = CurrencyCollection::default();
                }
                Ok(_addr) => {
                    if collation_data.value_flow.recovered.tokens < Tokens::new(1_000_000_000) {
                        tracing::debug!(target: tracing_targets::COLLATOR,
                            "fee recovery skipped ({:?})", collation_data.value_flow.recovered,
                        );
                        collation_data.value_flow.recovered = CurrencyCollection::default();
                    }
                }
            };

            collation_data.value_flow.minted = self.compute_minted_amount(mc_data)?;

            if collation_data.value_flow.minted != CurrencyCollection::ZERO
                && mc_data.config().get_minter_address().is_err()
            {
                tracing::warn!(target: tracing_targets::COLLATOR,
                    "minting of {:?} disabled: no minting smart contract defined",
                    collation_data.value_flow.minted,
                );
                collation_data.value_flow.minted = CurrencyCollection::default();
            }
        } else {
            collation_data.value_flow.created.tokens =
                mc_data.config().get_block_creation_reward(false)?;
            // TODO: should check if it is good to cast `prefix_len` from u16 to u8
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
        // TODO: just copied from old node, needs to review
        tracing::trace!(target: tracing_targets::COLLATOR, "compute_minted_amount()");

        let mut to_mint = CurrencyCollection::default();

        let to_mint_cp = match mc_data.config().get::<ConfigParam7>() {
            Ok(Some(v)) => v,
            _ => {
                tracing::warn!(target: tracing_targets::COLLATOR,
                    "Can't get config param 7 (to_mint)",
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
                tracing::debug!(target: tracing_targets::COLLATOR,
                    "currency #{}: existing {:?}, required {:?}, to be minted {:?}",
                    key, amount2, amount, delta,
                );
                if key != 0 {
                    to_mint.other.as_dict_mut().set(key, delta)?;
                }
            }
        }

        Ok(to_mint)
    }

    /// Create special transactions for the collator
    async fn create_special_transactions(
        &self,
        collator_data: &mut BlockCollationData,
        exec_manager: &mut ExecutionManager,
    ) -> Result<()> {
        tracing::trace!(target: tracing_targets::COLLATOR, "create_special_transactions()");

        let account_id = self
            .working_state()
            .mc_data
            .config()
            .get_fee_collector_address()?;
        tracing::trace!(target: tracing_targets::COLLATOR,
            "create_special_transaction(): mint {} to account {}",
            collator_data.value_flow.recovered.tokens, account_id,
        );
        self.create_special_transaction(
            account_id,
            collator_data.value_flow.recovered.clone(),
            |_, msg_cell| AsyncMessage::Recover(msg_cell),
            collator_data,
            exec_manager,
        )
        .await?;

        let account_id = self.working_state().mc_data.config().get_minter_address()?;
        tracing::trace!(target: tracing_targets::COLLATOR,
            "create_special_transaction(): mint {} to account {}",
            collator_data.value_flow.minted.tokens, account_id,
        );
        self.create_special_transaction(
            account_id,
            collator_data.value_flow.minted.clone(),
            |_, msg_cell| AsyncMessage::Mint(msg_cell),
            collator_data,
            exec_manager,
        )
        .await?;

        Ok(())
    }

    async fn create_special_transaction(
        &self,
        account_id: HashBytes,
        amount: CurrencyCollection,
        f: impl FnOnce(MsgInfo, Cell) -> AsyncMessage,
        collation_data: &mut BlockCollationData,
        exec_manager: &mut ExecutionManager,
    ) -> Result<()> {
        if amount.tokens.is_zero() {
            return Ok(());
        }
        let info = MsgInfo::Int(IntMsgInfo {
            ihr_disabled: false,
            bounce: true,
            bounced: false,
            src: IntAddr::from((-1, [0; 32].into())),
            dst: IntAddr::from((-1, account_id)),
            value: amount,
            ihr_fee: Default::default(),
            fwd_fee: Default::default(),
            created_lt: collation_data.start_lt,
            created_at: collation_data.gen_utime,
        });
        let msg = BaseMessage {
            info: info.clone(),
            init: None,
            body: CellSlice::default(),
            layout: None,
        };
        let cell = CellBuilder::build_from(msg)?;
        let async_message = f(info, cell);
        let shard_account_stuff = exec_manager.get_shard_account_stuff(account_id)?;
        let (async_message, transaction) = exec_manager
            .execute_special_transaction(account_id, async_message, shard_account_stuff)
            .await?;
        new_transaction(collation_data, &self.shard_id, transaction.1, async_message)?;
        collation_data.next_lt = exec_manager.min_next_lt;
        Ok(())
    }

    async fn create_ticktock_transactions(
        &self,
        tock: bool,
        collation_data: &mut BlockCollationData,
        exec_manager: &mut ExecutionManager,
    ) -> Result<()> {
        tracing::trace!(target: tracing_targets::COLLATOR, "create_ticktock_transactions()");
        let fundamental_dict = self
            .working_state()
            .mc_data
            .config()
            .get_fundamental_addresses()?;
        for account in fundamental_dict.keys() {
            self.create_ticktock_transaction(account?, tock, collation_data, exec_manager)
                .await?;
        }
        let config_address = self.working_state().mc_data.config().address;
        self.create_ticktock_transaction(config_address, tock, collation_data, exec_manager)
            .await?;
        Ok(())
    }

    async fn create_ticktock_transaction(
        &self,
        account_id: HashBytes,
        tock: bool,
        collation_data: &mut BlockCollationData,
        exec_manager: &mut ExecutionManager,
    ) -> Result<()> {
        tracing::trace!(target: tracing_targets::COLLATOR,
            "create_ticktock_transaction({}) to account {}",
            if tock { "tock" } else { "tick" },
            account_id
        );

        let shard_account_stuff = exec_manager.get_shard_account_stuff(account_id)?;
        let tick_tock = shard_account_stuff
            .shard_account
            .load_account()?
            .map(|account| {
                if let AccountState::Active(StateInit { ref special, .. }) = account.state {
                    special.unwrap_or_default()
                } else {
                    Default::default()
                }
            })
            .unwrap_or_default();

        if (tick_tock.tock && tock) || (tick_tock.tick && !tock) {
            let tt = if tock { TickTock::Tock } else { TickTock::Tick };
            let async_message = AsyncMessage::TickTock(tt);
            let (async_message, transaction) = exec_manager
                .execute_special_transaction(account_id, async_message, shard_account_stuff)
                .await?;
            new_transaction(collation_data, &self.shard_id, transaction.1, async_message)?;
            collation_data.next_lt = exec_manager.min_next_lt;
        }

        Ok(())
    }

    fn import_new_shard_top_blocks_for_masterchain(
        &self,
        config: &BlockchainConfig,
        collation_data: &mut BlockCollationData,
        top_shard_blocks_info: Vec<TopBlockDescription>,
    ) -> Result<()> {
        tracing::trace!(target: tracing_targets::COLLATOR,
            "import_new_shard_top_blocks_for_masterchain()",
        );

        let gen_utime = collation_data.gen_utime;
        for TopBlockDescription {
            block_id,
            block_info,
            value_flow,
            proof_funds,
            creators,
        } in top_shard_blocks_info
        {
            let mut shard_descr = Box::new(ShardDescription::from_block_info(
                block_id,
                &block_info,
                &value_flow,
            ));
            shard_descr.reg_mc_seqno = collation_data.block_id_short.seqno;

            collation_data.update_shards_max_end_lt(shard_descr.end_lt);

            let shard_id = block_id.shard;

            collation_data.top_shard_blocks_ids.push(block_id);

            if shard_descr.gen_utime > gen_utime {
                tracing::debug!(target: tracing_targets::COLLATOR,
                    "ShardTopBlockDescr for {} skipped: it claims to be generated at {} while it is still {}",
                    shard_id, shard_descr.gen_utime, gen_utime,
                );
                continue;
            }
            if config
                .get_global_version()?
                .capabilities
                .contains(GlobalCapability::CapWorkchains)
            {
                // NOTE: shard_descr proof_chain is used for transactions between workchains in TON
                // we skip it for now and will rework mechanism in the future
                // shard_descr.proof_chain = Some(sh_bd.top_block_descr().chain().clone());
            }
            // TODO: Check may update shard block info
            // TODO: Implement merge algorithm in future

            self.update_shard_block_info(
                collation_data.shards_mut()?,
                shard_id,
                shard_descr.clone(),
            )?;

            collation_data.store_shard_fees(shard_id, proof_funds)?;
            collation_data.register_shard_block_creators(creators)?;
        }

        let shard_fees = collation_data.shard_fees.root_extra().clone();

        collation_data
            .value_flow
            .fees_collected
            .checked_add(&shard_fees.fees)?;
        collation_data.value_flow.fees_imported = shard_fees.fees;

        Ok(())
    }

    pub fn update_shard_block_info(
        &self,
        shardes: &mut FastHashMap<ShardIdent, Box<ShardDescription>>,
        shard_id: ShardIdent,
        shard_description: Box<ShardDescription>,
    ) -> Result<()> {
        shardes.insert(shard_id, shard_description);
        Ok(())
    }

    fn update_stats(&mut self, collation_data: &BlockCollationData) {
        self.stats.total_execute_count_all += collation_data.execute_count_all;

        self.stats.total_execute_msgs_time_mc += collation_data.total_execute_msgs_time_mc;
        self.stats.avg_exec_msgs_per_1000_ms =
            (self.stats.total_execute_count_all as u128 * 1_000 * 1_000)
                .checked_div(self.stats.total_execute_msgs_time_mc)
                .unwrap_or_default();

        self.stats.total_execute_count_ext += collation_data.execute_count_ext;
        self.stats.total_execute_count_int += collation_data.execute_count_int;
        self.stats.total_execute_count_new_int += collation_data.execute_count_new_int;
        self.stats.int_queue_length += collation_data.enqueue_count;
        self.stats.int_queue_length = self
            .stats
            .int_queue_length
            .checked_sub(collation_data.dequeue_count)
            .unwrap_or_default();
    }
}

/// add in and out messages from to block
fn new_transaction(
    collation_data: &mut BlockCollationData,
    shard_id: &ShardIdent,
    transaction: Lazy<Transaction>,
    in_msg: AsyncMessage,
) -> Result<Vec<(MsgInfo, Cell)>> {
    tracing::trace!(target: tracing_targets::COLLATOR,
        "process new transaction from message {:?}\n{:?}",
        in_msg, transaction,
    );
    collation_data.execute_count_all += 1;
    let in_msg_original = in_msg.clone();
    let (in_msg, in_msg_cell) = match in_msg {
        AsyncMessage::Int(MsgInfo::Int(IntMsgInfo { fwd_fee, .. }), in_msg_cell, current_shard) => {
            collation_data.execute_count_int += 1;
            let next_addr = IntermediateAddr::FULL_DEST_SAME_WORKCHAIN;
            // NOTE: `cur_addr` is not used in current routing between shards logic, it's just here to make the code more readable
            let cur_addr = if current_shard {
                IntermediateAddr::FULL_DEST_SAME_WORKCHAIN
            } else {
                IntermediateAddr::FULL_SRC_SAME_WORKCHAIN
            };
            let msg_envelope = MsgEnvelope {
                cur_addr,
                next_addr,
                fwd_fee_remaining: fwd_fee,
                message: Lazy::new(&OwnedMessage::load_from(&mut in_msg_cell.as_slice()?)?)?,
            };

            let in_msg = InMsg::Final(InMsgFinal {
                in_msg_envelope: Lazy::new(&msg_envelope)?,
                transaction: transaction.clone(),
                fwd_fee,
            });

            if current_shard {
                let out_msg = OutMsg::DequeueImmediate(OutMsgDequeueImmediate {
                    out_msg_envelope: Lazy::new(&msg_envelope)?,
                    reimport: Lazy::new(&in_msg)?,
                });

                collation_data
                    .out_msgs
                    .insert(*in_msg_cell.repr_hash(), out_msg.clone());
                collation_data.dequeue_count += 1;
            }

            (in_msg, in_msg_cell)
        }
        AsyncMessage::NewInt(MsgInfo::Int(IntMsgInfo { fwd_fee, .. }), in_msg_cell) => {
            collation_data.execute_count_new_int += 1;

            let next_addr = IntermediateAddr::FULL_SRC_SAME_WORKCHAIN;
            let cur_addr = IntermediateAddr::FULL_SRC_SAME_WORKCHAIN;
            let msg_envelope = MsgEnvelope {
                cur_addr,
                next_addr,
                fwd_fee_remaining: fwd_fee,
                message: Lazy::new(&OwnedMessage::load_from(&mut in_msg_cell.as_slice()?)?)?,
            };
            let in_msg = InMsg::Immediate(InMsgFinal {
                in_msg_envelope: Lazy::new(&msg_envelope)?,
                transaction: transaction.clone(),
                fwd_fee,
            });

            let previous_transaction = match collation_data
                .out_msgs
                .get(in_msg_cell.repr_hash())
                .context("New Message was not found in out msgs")?
            {
                OutMsg::New(previous_out_msg) => previous_out_msg.transaction.clone(),
                _ => {
                    bail!(
                        "Out msgs doesn't contain out message with state New: {:?}\n{:?}",
                        in_msg_original,
                        transaction,
                    );
                }
            };

            let out_msg = OutMsg::Immediate(OutMsgImmediate {
                out_msg_envelope: Lazy::new(&msg_envelope)?,
                transaction: previous_transaction,
                reimport: Lazy::new(&in_msg)?,
            });

            collation_data
                .out_msgs
                .insert(*in_msg_cell.repr_hash(), out_msg.clone());
            collation_data.enqueue_count -= 1;
            (in_msg, in_msg_cell)
        }
        AsyncMessage::Mint(in_msg_cell) | AsyncMessage::Recover(in_msg_cell) => {
            let next_addr = IntermediateAddr::FULL_SRC_SAME_WORKCHAIN;
            let cur_addr = IntermediateAddr::FULL_SRC_SAME_WORKCHAIN;
            let msg_envelope = MsgEnvelope {
                cur_addr,
                next_addr,
                fwd_fee_remaining: Default::default(),
                message: Lazy::new(&OwnedMessage::load_from(&mut in_msg_cell.as_slice()?)?)?,
            };
            let in_msg = InMsg::Immediate(InMsgFinal {
                in_msg_envelope: Lazy::new(&msg_envelope)?,
                transaction: transaction.clone(),
                fwd_fee: Default::default(),
            });
            (in_msg, in_msg_cell)
        }
        AsyncMessage::Ext(MsgInfo::ExtIn(_), in_msg_cell) => {
            collation_data.execute_count_ext += 1;
            (
                InMsg::External(InMsgExternal {
                    in_msg: Lazy::new(&OwnedMessage::load_from(&mut in_msg_cell.as_slice()?)?)?,
                    transaction: transaction.clone(),
                }),
                in_msg_cell,
            )
        }
        AsyncMessage::TickTock(_) => {
            return Ok(vec![]);
        }
        s => {
            tracing::error!("wrong async message - {s:?}");
            unreachable!()
        }
    };
    collation_data
        .in_msgs
        .insert(*in_msg_cell.repr_hash(), in_msg);
    let mut result = vec![];
    let binding = transaction.load()?;
    let out_msgs = binding.iter_out_msgs();
    for out_msg in out_msgs {
        let message = out_msg?;
        let msg_cell = CellBuilder::build_from(&message)?;
        let message_info = message.info;
        let out_msg = match message_info.clone() {
            MsgInfo::Int(IntMsgInfo { fwd_fee, dst, .. }) => {
                collation_data.enqueue_count += 1;

                let dst_prefix = dst.prefix();
                let dst_workchain = dst.workchain();
                let cur_addr = IntermediateAddr::FULL_SRC_SAME_WORKCHAIN;
                // NOTE: `next_addr` is not used in current routing between shards logic, it's just here to make the code more readable
                let next_addr = if contains_prefix(shard_id, dst_workchain, dst_prefix) {
                    IntermediateAddr::FULL_DEST_SAME_WORKCHAIN
                } else {
                    IntermediateAddr::FULL_SRC_SAME_WORKCHAIN
                };
                tracing::trace!(target: tracing_targets::COLLATOR,
                    "adding OutMsg::New(OutMsgNew) to out_msgs: hash={}, info={:?}",
                    msg_cell.repr_hash(), message_info,
                );
                OutMsg::New(OutMsgNew {
                    out_msg_envelope: Lazy::new(&MsgEnvelope {
                        cur_addr,
                        next_addr,
                        fwd_fee_remaining: fwd_fee,
                        message: Lazy::new(&OwnedMessage::load_from(&mut msg_cell.as_slice()?)?)?,
                    })?,
                    transaction: transaction.clone(),
                })
            }
            MsgInfo::ExtOut(_) => {
                tracing::trace!(target: tracing_targets::COLLATOR,
                    "adding OutMsg::External(OutMsgExternal) to out_msgs: hash={}, info={:?}",
                    msg_cell.repr_hash(), message_info,
                );
                OutMsg::External(OutMsgExternal {
                    out_msg: Lazy::new(&OwnedMessage::load_from(&mut msg_cell.as_slice()?)?)?,
                    transaction: transaction.clone(),
                })
            }
            MsgInfo::ExtIn(_) => bail!("External inbound message cannot be output"),
        };

        collation_data
            .out_msgs
            .insert(*msg_cell.repr_hash(), out_msg.clone());
        result.push((message_info, msg_cell));
    }
    Ok(result)
}

pub fn contains_prefix(shard_id: &ShardIdent, workchain_id: i32, prefix_without_tag: u64) -> bool {
    if shard_id.workchain() == workchain_id {
        if shard_id.prefix() == 0x8000_0000_0000_0000u64 {
            return true;
        }
        let shift = 64 - shard_id.prefix_len();
        return (shard_id.prefix() >> shift) == (prefix_without_tag >> shift);
    }
    false
}
