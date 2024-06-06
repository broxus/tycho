use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::ops::Deref;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use everscale_types::models::*;
use everscale_types::num::Tokens;
use everscale_types::prelude::*;
use sha2::Digest;

use super::types::CachedMempoolAnchor;
use super::CollatorStdImpl;
use crate::collator::execution_manager::ExecutionManager;
use crate::collator::types::{
    AsyncMessage, BlockCollationData, McData, PrevData, ShardDescriptionExt,
};
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
        let mc_data = &self.working_state().mc_data;
        let prev_shard_data = &self.working_state().prev_shard_data;

        tracing::debug!(target: tracing_targets::COLLATOR,
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
                .collect::<HashMap<_, _>>();

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
        let (max_messages_per_set, min_externals_per_set, group_size) =
            self.get_msgs_execution_params();

        // init execution manager
        let mut exec_manager = ExecutionManager::new(
            collation_data.gen_utime,
            collation_data.start_lt,
            collation_data.next_lt,
            collation_data.rand_seed,
            mc_data.libraries().clone(),
            mc_data.config().clone(),
            self.config.supported_block_version,
            group_size as u32,
            prev_shard_data.observable_accounts().clone(),
        );

        const STUB_SKIP_EXECUTION: bool = false;

        // execute tick transaction and special transactions (mint, recover)
        if self.shard_id.is_masterchain() && !STUB_SKIP_EXECUTION {
            self.create_ticktock_transactions(false, &mut collation_data, &mut exec_manager)
                .await?;
            self.create_special_transactions(&mut collation_data, &mut exec_manager)
                .await?;
        }

        let mut all_existing_internals_finished = false;
        let mut all_new_internals_finished = false;

        let mut block_limits_reached = false;
        let mut block_transactions_count = 0;

        let mut internal_messages_iterator = self.init_internal_mq_iterator().await?;

        // indicate that there are still unprocessed internals when collation loop finished
        let mut has_pending_internals = false;

        loop {
            let mut internal_messages_in_set = vec![];

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

                        internal_messages_in_set.push((
                            message_with_source.shard_id,
                            message_with_source.message.key(),
                        ));

                        remaining_capacity -= 1;
                    }
                    None => {
                        all_existing_internals_finished = true;
                    }
                }
            }

            tracing::debug!(target: tracing_targets::COLLATOR,
                "read {} externals and {} internals",
                ext_msgs.len(), internal_messages_in_set.len(),
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
                    "read additional {} externals",
                    ext_msgs.len(),
                );
                msgs_set.append(&mut ext_msgs);
            }

            // 4. When all externals and existing internals finished (the collected set is empty here)
            //    fill next messages sets with new internals
            if msgs_set.is_empty() {
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

                            let async_message = AsyncMessage::NewInt(int_msg_info, cell);

                            msgs_set.push(async_message);

                            internal_messages_in_set.push((
                                message_with_source.shard_id,
                                message_with_source.message.key(),
                            ));

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

            let msgs_set_len = msgs_set.len() as u32;
            let mut msgs_set_executed_count = 0;
            exec_manager.set_msgs_for_execution(msgs_set.clone()); // STUB: clone to use msgs_set in stub logic
            let mut msgs_set_offset = collation_data.processed_upto.processed_offset;
            let mut msgs_set_full_processed = false;

            if STUB_SKIP_EXECUTION && msgs_set_offset > 0 {
                msgs_set = msgs_set.split_off(msgs_set_offset as usize);
            }

            // execute msgs processing by groups
            while !msgs_set_full_processed {
                let one_tick_executed_count = if STUB_SKIP_EXECUTION {
                    // STUB: skip real execution
                    let left_msgs = if msgs_set.len() > group_size {
                        msgs_set.split_off(group_size - 1)
                    } else {
                        vec![]
                    };
                    let one_tick_executed_count = msgs_set.len();
                    collation_data.next_lt = exec_manager.min_next_lt;
                    msgs_set_offset += one_tick_executed_count as u32;
                    msgs_set = left_msgs;
                    one_tick_executed_count
                } else {
                    let (new_offset, group) = exec_manager.execute_tick(msgs_set_offset).await?;
                    let one_tick_executed_count = group.len();
                    for (_account_id, msg_info, transaction) in group {
                        let new_internal_messages = new_transaction(
                            &mut collation_data,
                            &self.shard_id,
                            transaction.1,
                            msg_info,
                        )?;

                        if !new_internal_messages.is_empty() {
                            // tracing::trace!(target: tracing_targets::COLLATOR,
                            //     "created new internals to {:?}",
                            //     new_internal_messages
                            //     .iter()
                            //     .filter_map(|(msg_info, _)| {
                            //         if let MsgInfo::Int(int_msg_info) = msg_info {
                            //             Some(int_msg_info.dst.to_string())
                            //         } else {
                            //             None
                            //         }
                            //     })
                            //     .collect::<Vec<_>>()
                            //     .as_slice(),
                            // );
                            self.mq_adapter.add_messages_to_iterator(
                                &mut internal_messages_iterator,
                                new_internal_messages,
                            )?;
                        } else {
                            tracing::trace!(target: tracing_targets::COLLATOR,
                                "no new internal messages created",
                            );
                        }

                        collation_data.next_lt = exec_manager.min_next_lt;
                    }
                    msgs_set_offset = new_offset;
                    one_tick_executed_count
                };

                // TODO: when processing transactions we should check block limits
                //      currently we simply check only transactions count
                //      but needs to make good implementation futher
                msgs_set_executed_count += one_tick_executed_count;
                block_transactions_count += one_tick_executed_count;
                tracing::debug!(target: tracing_targets::COLLATOR,
                    "processed {}/{} messages from set, total {}, offset = {}",
                    msgs_set_executed_count, msgs_set_len,
                    block_transactions_count, msgs_set_offset,
                );
                if block_transactions_count >= 1000 {
                    tracing::debug!(target: tracing_targets::COLLATOR,
                        "STUB: block limit reached: {}/1000",
                        block_transactions_count,
                    );
                    block_limits_reached = true;
                    break;
                }

                if msgs_set_offset == msgs_set_len {
                    msgs_set_full_processed = true;
                }
            }

            // commit messages to iterator only if set was fully processed
            if msgs_set_full_processed {
                self.mq_adapter.commit_messages_to_iterator(
                    &mut internal_messages_iterator,
                    internal_messages_in_set,
                )?;
            }

            // store how many msgs from the set were processed (offset)
            Self::update_processed_upto_execution_offset(
                &mut collation_data,
                msgs_set_full_processed,
                msgs_set_offset,
            );

            tracing::debug!(target: tracing_targets::COLLATOR, "updated processed_upto.externals = {:?}",
                collation_data.processed_upto.externals,
            );

            has_pending_internals = internal_messages_iterator.peek(true)?.is_some();

            if block_limits_reached {
                // block is full - exit loop
                break;
            }
        }

        // execute tock transaction
        if self.shard_id.is_masterchain() && !STUB_SKIP_EXECUTION {
            self.create_ticktock_transactions(true, &mut collation_data, &mut exec_manager)
                .await?;
        }

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

        // STUB: sleep to slow down collation process for analysis
        // tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        // return collation result
        let collation_result = BlockCollationResult {
            candidate,
            new_state_stuff: new_state_stuff.clone(),
            has_pending_internals,
        };
        self.listener.on_block_candidate(collation_result).await?;

        tracing::info!(target: tracing_targets::COLLATOR,
            "created and sent block candidate: start_lt={}, end_lt={}, txs={}, new_msgs={}, in_msgs={}, out_msgs={}",
            collation_data.start_lt, collation_data.next_lt, block_transactions_count,
            diff.messages.len(), collation_data.in_msgs.len(), collation_data.out_msgs.len(),
        );

        // update PrevData in working state
        self.update_working_state(new_state_stuff)?;
        self.update_working_state_pending_internals(Some(has_pending_internals))?;

        Ok(())
    }

    /// `(set_size, min_externals_per_set, group_size)`
    /// * `set_size` - max num of messages to be processed in one iteration;
    /// * `min_externals_per_set` - min num of externals that should be included in the set
    ///                           when there are a lot of internals and externals
    /// * `group_size` - max num of accounts to be processed in one tick
    fn get_msgs_execution_params(&self) -> (usize, usize, usize) {
        // TODO: should get this from BlockchainConfig
        (93, 30, 18)
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
        anchors_cache: &mut BTreeMap<MempoolAnchorId, CachedMempoolAnchor>,
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

        let mut next_key = anchors_cache.first_key_value().map_or(0, |(key, _)| *key);
        loop {
            tracing::debug!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
                "try read next anchor next_key: {}", next_key,
            );
            // try read next anchor
            let next_entry = anchors_cache.entry(next_key);
            let entry = match next_entry {
                Entry::Occupied(entry) => entry,
                // stop reading if there is no next anchor
                Entry::Vacant(_) => {
                    tracing::debug!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
                        "no entry in anchors cache by key {}", next_key,
                    );
                    break;
                }
            };

            let key = *entry.key();
            if key < was_read_upto.0 {
                // skip and remove already processed anchor from cache
                let _ = entry.remove();
                tracing::debug!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
                    "anchor with key {} already processed, removed from anchors cache", key,
                );
                // try read next anchor
                next_key += 1;
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

                let anchor = &entry.get().anchor;

                if key == was_read_upto.0 && anchor.externals_count() == was_read_upto.1 as usize {
                    // skip and remove fully processed anchor
                    let _ = entry.remove();
                    tracing::debug!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
                        "anchor with key {} fully processed, removed from anchors cache", key,
                    );
                    // try read next anchor
                    next_key += 1;
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
                next_key += 1;
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

            // TODO: set internals read window full pocessed

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
        shardes: &mut HashMap<ShardIdent, Box<ShardDescription>>,
        shard_id: ShardIdent,
        shard_description: Box<ShardDescription>,
    ) -> Result<()> {
        shardes.insert(shard_id, shard_description);
        Ok(())
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
    collation_data.execute_count += 1;
    let in_msg_original = in_msg.clone();
    let (in_msg, in_msg_cell) = match in_msg {
        AsyncMessage::Int(MsgInfo::Int(IntMsgInfo { fwd_fee, .. }), in_msg_cell, current_shard) => {
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
                // TODO: del out msg from queue
            }

            (in_msg, in_msg_cell)
        }
        AsyncMessage::NewInt(MsgInfo::Int(IntMsgInfo { fwd_fee, .. }), in_msg_cell) => {
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
        AsyncMessage::Ext(MsgInfo::ExtIn(_), in_msg_cell) => (
            InMsg::External(InMsgExternal {
                in_msg: Lazy::new(&OwnedMessage::load_from(&mut in_msg_cell.as_slice()?)?)?,
                transaction: transaction.clone(),
            }),
            in_msg_cell,
        ),
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
                collation_data.out_msg_count += 1;
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
                collation_data.out_msg_count += 1;
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
