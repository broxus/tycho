use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Result};
use everscale_types::models::*;
use everscale_types::num::Tokens;
use everscale_types::prelude::*;
use humantime::format_duration;
use sha2::Digest;
use ton_executor::{ExecuteParams, ExecutorOutput, PreloadedBlockchainConfig};
use tycho_util::metrics::HistogramGuard;
use tycho_util::time::now_millis;
use tycho_util::FastHashMap;

use super::types::{
    BlockCollationDataBuilder, BlockLimitsLevel, CachedMempoolAnchor, SpecialOrigin,
};
use super::CollatorStdImpl;
use crate::collator::execution_manager::ExecutionManager;
use crate::collator::types::{
    BlockCollationData, Dequeued, McData, ParsedMessage, PreparedInMsg, PreparedOutMsg, PrevData,
    ShardDescriptionExt,
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
        let labels = &[("workchain", self.shard_id.workchain().to_string())];
        let histogram = HistogramGuard::begin_with_labels("tycho_do_collate_total_time", labels);

        let prepare_histogram =
            HistogramGuard::begin_with_labels("tycho_do_collate_prepare_time", labels);

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

        let is_masterchain = self.shard_id.is_masterchain();
        // prepare block collation data
        // STUB: consider split/merge in future for taking prev_block_id
        let prev_block_id = prev_shard_data.blocks_ids()[0];
        let block_id_short = BlockIdShort {
            shard: prev_block_id.shard,
            seqno: prev_block_id.seqno + 1,
        };
        let block_limits = mc_data.config().get_block_limits(is_masterchain)?;
        tracing::debug!(target: tracing_targets::COLLATOR,
            "Block limits: {:?}",
            block_limits
        );

        // TODO: get from anchor
        let created_by = HashBytes::default();
        let mut collation_data_builder = BlockCollationDataBuilder::new(
            block_id_short,
            rand_seed,
            mc_data.mc_state_stuff().state().seqno,
            next_chain_time,
            prev_shard_data.processed_upto().clone(),
            created_by,
        );

        // init ShardHashes descriptions for master
        if is_masterchain {
            let shards = prev_shard_data.observable_states()[0]
                .shards()?
                .iter()
                .filter_map(|entry| {
                    entry
                        .ok()
                        .map(|(shard_id, shard_descr)| (shard_id, Box::new(shard_descr)))
                })
                .collect::<FastHashMap<_, _>>();

            collation_data_builder.set_shards(shards);

            if let Some(top_shard_blocks_info) = top_shard_blocks_info {
                self.import_new_shard_top_blocks_for_masterchain(
                    mc_data.config(),
                    &mut collation_data_builder,
                    top_shard_blocks_info,
                )?;
            }
        }

        let start_lt = Self::calc_start_lt(
            mc_data,
            prev_shard_data,
            is_masterchain,
            collation_data_builder.shards_max_end_lt,
        )?;

        let mut collation_data = Box::new(collation_data_builder.build(start_lt, block_limits));

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

        // compute created / minted / recovered / from_prev_block
        self.update_value_flow(mc_data, prev_shard_data, &mut collation_data)?;

        // prepare to read and execute internals and externals
        let (max_messages_per_set, min_externals_per_set, group_limit, group_vert_size) =
            self.get_msgs_execution_params();

        metrics::gauge!("tycho_do_collate_msgs_exec_params_set_size")
            .set(max_messages_per_set as f64);
        metrics::gauge!("tycho_do_collate_msgs_exec_params_min_exts_per_set")
            .set(min_externals_per_set as f64);
        metrics::gauge!("tycho_do_collate_msgs_exec_params_group_limit").set(group_limit as f64);
        metrics::gauge!("tycho_do_collate_msgs_exec_params_group_vert_size")
            .set(group_vert_size as f64);

        // init execution manager
        let mut exec_manager = ExecutionManager::new(
            self.shard_id,
            collation_data.next_lt,
            Arc::new(PreloadedBlockchainConfig::with_config(
                mc_data.config().clone(),
                mc_data.global_id(),
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

        let prepare_elapsed = prepare_histogram.finish();

        let init_iterator_elapsed;
        let mut internal_messages_iterator;
        {
            let histogram =
                HistogramGuard::begin_with_labels("tycho_do_collate_init_iterator_time", labels);
            internal_messages_iterator = self.init_internal_mq_iterator().await?;
            init_iterator_elapsed = histogram.finish();
        }

        // execute tick transaction and special transactions (mint, recover)
        let execute_tick_elapsed;
        if is_masterchain {
            let histogram =
                HistogramGuard::begin_with_labels("tycho_do_collate_execute_tick_time", labels);

            self.create_ticktock_transactions(
                TickTock::Tick,
                &mut collation_data,
                &mut exec_manager,
            )
            .await?;

            self.create_special_transactions(&mut collation_data, &mut exec_manager)
                .await?;

            execute_tick_elapsed = histogram.finish();
        } else {
            execute_tick_elapsed = Duration::ZERO;
        }

        let execute_histogram =
            HistogramGuard::begin_with_labels("tycho_do_collate_execute_time", labels);

        let mut all_existing_internals_finished = false;
        let mut all_new_internals_finished = false;

        let mut block_limits_reached = false;

        // indicate that there are still unprocessed internals when collation loop finished
        let mut has_pending_internals = false;

        let mut fill_msgs_total_elapsed = Duration::ZERO;
        let mut execute_msgs_total_elapsed = Duration::ZERO;
        let mut process_txs_total_elapsed = Duration::ZERO;
        let mut exec_msgs_sets_count = 0;
        loop {
            let mut timer = Instant::now();

            let soft_level_reached = collation_data.block_limit.reached(BlockLimitsLevel::Soft);
            if soft_level_reached {
                tracing::debug!(target: tracing_targets::COLLATOR,
                    "STUB: soft block limit reached: {:?}",
                    collation_data.block_limit,
                );
            }
            let mut executed_internal_messages = vec![];
            let mut internal_messages_sources = FastHashMap::default();
            // build messages set
            let mut msgs_set: Vec<Box<ParsedMessage>> = vec![];

            // 1. First try to read min externals amount

            let mut ext_msgs = if !soft_level_reached && self.has_pending_externals {
                self.read_next_externals(min_externals_per_set, &mut collation_data)?
            } else {
                vec![]
            };
            collation_data.read_ext_msgs += ext_msgs.len() as u64;

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

                        msgs_set.push(Box::new(ParsedMessage {
                            info: MsgInfo::Int(message_with_source.message.info.clone()),
                            cell: message_with_source.message.cell.clone(),
                            special_origin: None,
                            dequeued: Some(Dequeued {
                                same_shard: message_with_source.shard_id == self.shard_id,
                            }),
                        }));

                        internal_messages_sources.insert(
                            message_with_source.message.key(),
                            message_with_source.shard_id,
                        );
                        collation_data.read_int_msgs_from_iterator += 1;

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
            if remaining_capacity > 0 && self.has_pending_externals && !soft_level_reached {
                ext_msgs = self.read_next_externals(remaining_capacity, &mut collation_data)?;
                tracing::debug!(target: tracing_targets::COLLATOR,
                    ext_count = ext_msgs.len(),
                    "read additional externals",
                );
                collation_data.read_ext_msgs += ext_msgs.len() as u64;
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

                            let dequeued = if int_msg.is_new {
                                collation_data.read_new_msgs_from_iterator += 1;
                                None
                            } else {
                                Some(Dequeued {
                                    same_shard: message_with_source.shard_id == self.shard_id,
                                })
                            };

                            msgs_set.push(Box::new(ParsedMessage {
                                info: MsgInfo::Int(message_with_source.message.info.clone()),
                                cell: message_with_source.message.cell.clone(),
                                special_origin: None,
                                dequeued,
                            }));

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

            exec_msgs_sets_count += 1;

            let msgs_set_len = msgs_set.len() as u32;
            let mut msgs_set_executed_count = 0;
            let mut exec_ticks_count = 0;
            exec_manager.set_msgs_for_execution(msgs_set);
            let mut msgs_set_offset = collation_data.processed_upto.processed_offset;
            let mut msgs_set_full_processed = false;

            fill_msgs_total_elapsed += timer.elapsed();

            // execute msgs processing by groups
            while !msgs_set_full_processed {
                // Exec messages
                exec_ticks_count += 1;
                timer = std::time::Instant::now();
                let tick = exec_manager.tick(msgs_set_offset).await?;
                execute_msgs_total_elapsed += timer.elapsed();

                msgs_set_full_processed = tick.finished;
                let one_tick_executed_count = tick.items.len();

                // Process transactions
                timer = std::time::Instant::now();
                for item in tick.items {
                    if let MsgInfo::Int(int_msg_info) = &item.in_message.info {
                        let key = InternalMessageKey {
                            lt: int_msg_info.created_lt,
                            hash: *item.in_message.cell.repr_hash(),
                        };

                        let Some(shard_ident) = internal_messages_sources.get(&key).cloned() else {
                            anyhow::bail!("internal message source shard not found for: {key:?}");
                        };
                        executed_internal_messages.push((shard_ident, key));
                    }

                    let new_messages = new_transaction(
                        &mut collation_data,
                        &self.shard_id,
                        item.executor_output,
                        item.in_message,
                    )?;

                    collation_data.new_msgs_created += new_messages.len() as u64;

                    for new_message in &new_messages {
                        let MsgInfo::Int(int_msg_info) = &new_message.info else {
                            continue;
                        };

                        collation_data.inserted_new_msgs_to_iterator += 1;

                        // TODO: Reduce size of parameters
                        self.mq_adapter.add_message_to_iterator(
                            &mut internal_messages_iterator,
                            (int_msg_info.clone(), new_message.cell.clone()),
                        )?;
                    }

                    collation_data.next_lt = exec_manager.min_next_lt();
                    collation_data.block_limit.lt_current = collation_data.next_lt;
                }

                msgs_set_offset = tick.new_offset;

                process_txs_total_elapsed += timer.elapsed();

                // TODO: when processing transactions we should check block limits
                //      currently we simply check only transactions count
                //      but needs to make good implementation futher
                msgs_set_executed_count += one_tick_executed_count;
                collation_data.tx_count += one_tick_executed_count as u64;
                collation_data.ext_msgs_error_count += tick.ext_msgs_error_count;
                tracing::debug!(target: tracing_targets::COLLATOR,
                    "processed messages from set {}/{}, total {}, offset = {}",
                    msgs_set_executed_count, msgs_set_len,
                    collation_data.tx_count, msgs_set_offset,
                );

                if msgs_set_offset == msgs_set_len {
                    msgs_set_full_processed = true;
                }

                if collation_data.block_limit.reached(BlockLimitsLevel::Hard) {
                    tracing::debug!(target: tracing_targets::COLLATOR,
                        "STUB: block limit reached: {:?}",
                        collation_data.block_limit,
                    );
                    block_limits_reached = true;
                    break;
                }
            }

            metrics::gauge!("tycho_do_collate_exec_ticks_per_msgs_set", labels)
                .set(exec_ticks_count as f64);

            timer = std::time::Instant::now();

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

            process_txs_total_elapsed += timer.elapsed();

            if block_limits_reached {
                // block is full - exit loop
                metrics::counter!("tycho_do_collate_blocks_with_limits_reached_count", labels)
                    .increment(1);
                break;
            }
        }

        metrics::gauge!("tycho_do_collate_exec_msgs_sets_per_block", labels)
            .set(exec_msgs_sets_count as f64);

        metrics::histogram!("tycho_do_collate_fill_msgs_total_time", labels)
            .record(fill_msgs_total_elapsed);
        metrics::histogram!("tycho_do_collate_exec_msgs_total_time", labels)
            .record(execute_msgs_total_elapsed);
        metrics::histogram!("tycho_do_collate_process_txs_total_time", labels)
            .record(process_txs_total_elapsed);

        // execute tock transaction
        let execute_tock_elapsed;
        if is_masterchain {
            let histogram =
                HistogramGuard::begin_with_labels("tycho_do_collate_execute_tock_time", labels);
            self.create_ticktock_transactions(
                TickTock::Tock,
                &mut collation_data,
                &mut exec_manager,
            )
            .await?;
            execute_tock_elapsed = histogram.finish();
        } else {
            execute_tock_elapsed = Duration::ZERO;
        }

        let execute_elapsed = execute_histogram.finish();
        let histogram_create_queue_diff =
            HistogramGuard::begin_with_labels("tycho_do_collate_create_queue_diff_time", labels);

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

        let create_queue_diff_elapsed = histogram_create_queue_diff.finish();

        // build block candidate and new state
        let finalize_block_timer = std::time::Instant::now();
        // TODO: Move into rayon
        tokio::task::yield_now().await;
        let (candidate, new_state_stuff) =
            tokio::task::block_in_place(|| self.finalize_block(&mut collation_data, exec_manager))?;
        tokio::task::yield_now().await;
        let finalize_block_elapsed = finalize_block_timer.elapsed();

        metrics::counter!("tycho_do_collate_blocks_count", labels).increment(1);
        metrics::gauge!("tycho_do_collate_block_seqno", labels).set(self.next_block_id_short.seqno);

        let apply_queue_diff_elapsed;
        {
            let histogram =
                HistogramGuard::begin_with_labels("tycho_do_collate_apply_queue_diff_time", labels);
            self.mq_adapter
                .apply_diff(diff.clone(), candidate.block_id.as_short_id())
                .await?;
            apply_queue_diff_elapsed = histogram.finish();
        }

        let handle_block_candidate_elapsed;
        {
            let histogram = HistogramGuard::begin_with_labels(
                "tycho_do_collate_handle_block_candidate_time",
                labels,
            );

            // return collation result
            self.listener
                .on_block_candidate(BlockCollationResult {
                    candidate,
                    new_state_stuff: new_state_stuff.clone(),
                    has_pending_internals,
                })
                .await?;

            // update PrevData in working state
            self.update_working_state(new_state_stuff)?;
            self.update_working_state_pending_internals(Some(has_pending_internals))?;

            handle_block_candidate_elapsed = histogram.finish();
        }

        // metrics
        metrics::counter!("tycho_do_collate_tx_total", labels).increment(collation_data.tx_count);

        metrics::counter!("tycho_do_collate_int_enqueue_count")
            .increment(collation_data.int_enqueue_count);
        metrics::counter!("tycho_do_collate_int_dequeue_count")
            .increment(collation_data.int_dequeue_count);
        metrics::gauge!("tycho_do_collate_int_msgs_queue_calc").increment(
            (collation_data.int_enqueue_count as i64 - collation_data.int_dequeue_count as i64)
                as f64,
        );

        metrics::counter!("tycho_do_collate_msgs_exec_count_all", labels)
            .increment(collation_data.execute_count_all);

        metrics::counter!("tycho_do_collate_msgs_read_count_ext", labels)
            .increment(collation_data.read_ext_msgs);
        metrics::counter!("tycho_do_collate_msgs_exec_count_ext", labels)
            .increment(collation_data.execute_count_ext);
        metrics::counter!("tycho_do_collate_msgs_error_count_ext", labels)
            .increment(collation_data.ext_msgs_error_count);

        metrics::counter!("tycho_do_collate_msgs_read_count_int", labels)
            .increment(collation_data.read_int_msgs_from_iterator);
        metrics::counter!("tycho_do_collate_msgs_exec_count_int", labels)
            .increment(collation_data.execute_count_int);

        // new internals messages
        metrics::counter!("tycho_do_collate_new_msgs_created_count", labels)
            .increment(collation_data.new_msgs_created);
        metrics::counter!(
            "tycho_do_collate_new_msgs_inserted_to_iterator_count",
            labels
        )
        .increment(collation_data.inserted_new_msgs_to_iterator);
        metrics::counter!("tycho_do_collate_msgs_read_count_new_int", labels)
            .increment(collation_data.read_new_msgs_from_iterator);
        metrics::counter!("tycho_do_collate_msgs_exec_count_new_int", labels)
            .increment(collation_data.execute_count_new_int);

        collation_data.total_execute_msgs_time_mc = execute_msgs_total_elapsed.as_micros();
        self.update_stats(&collation_data);

        let total_elapsed = histogram.finish();

        // time elapsed from prev block
        let elapsed_from_prev_block = self.timer.elapsed();
        let collation_mngmnt_overhead = elapsed_from_prev_block - total_elapsed;
        self.timer = std::time::Instant::now();
        metrics::histogram!("tycho_do_collate_from_prev_block_time", labels)
            .record(elapsed_from_prev_block);
        metrics::histogram!("tycho_do_collate_overhead_time", labels)
            .record(collation_mngmnt_overhead);

        // block time diff from now
        let block_time_diff = {
            let diff_time = now_millis() as i64 - next_chain_time as i64;
            metrics::gauge!("tycho_do_collate_block_time_diff", labels)
                .set(diff_time as f64 / 1000.0);
            diff_time
        };

        tracing::info!(target: tracing_targets::COLLATOR, "{:?}", self.stats);

        tracing::info!(target: tracing_targets::COLLATOR,
            "Created and sent block candidate: time_diff = {}, \
            collation_time={}, elapsed_from_prev_block={}, overhead = {}, \
            start_lt={}, end_lt={}, exec_count={}, \
            exec_ext={}, exec_int={}, exec_new_int={}, \
            enqueue_count={}, dequeue_count={}, \
            new_msgs_created={}, new_msgs_added={}, \
            in_msgs={}, out_msgs={}, \
            read_ext_msgs={}, read_int_msgs={}, \
            read_new_msgs_from_iterator={}, inserted_new_msgs_to_iterator={}",
            block_time_diff,
            total_elapsed.as_millis(), elapsed_from_prev_block.as_millis(), collation_mngmnt_overhead.as_millis(),
            collation_data.start_lt, collation_data.next_lt, collation_data.execute_count_all,
            collation_data.execute_count_ext, collation_data.execute_count_int, collation_data.execute_count_new_int,
            collation_data.int_enqueue_count, collation_data.int_dequeue_count,
            collation_data.new_msgs_created, diff.messages.len(),
            collation_data.in_msgs.len(), collation_data.out_msgs.len(),
            collation_data.read_ext_msgs, collation_data.read_int_msgs_from_iterator,
            collation_data.read_new_msgs_from_iterator, collation_data.inserted_new_msgs_to_iterator,
        );

        assert_eq!(
            collation_data.int_enqueue_count,
            collation_data.inserted_new_msgs_to_iterator - collation_data.execute_count_new_int
        );

        tracing::info!(
            target: tracing_targets::COLLATOR,
            block_time_diff = block_time_diff,
            from_prev_block = %format_duration(elapsed_from_prev_block),
            overhead = %format_duration(collation_mngmnt_overhead),
            total = %format_duration(total_elapsed),
            prepare = %format_duration(prepare_elapsed),
            init_iterator = %format_duration(init_iterator_elapsed),
            execute_tick = %format_duration(execute_tick_elapsed),
            execute_tock = %format_duration(execute_tock_elapsed),
            execute_total = %format_duration(execute_elapsed),

            fill_msgs_total = %format_duration(fill_msgs_total_elapsed),
            exec_msgs_total = %format_duration(execute_msgs_total_elapsed),
            process_txs_total = %format_duration(process_txs_total_elapsed),

            create_queue_diff = %format_duration(create_queue_diff_elapsed),
            apply_queue_diff = %format_duration(apply_queue_diff_elapsed),
            finalize_block = %format_duration(finalize_block_elapsed),
            handle_block_candidate = %format_duration(handle_block_candidate_elapsed),
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
    ) -> Result<Vec<Box<ParsedMessage>>> {
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
    ) -> Result<(Vec<Box<ParsedMessage>>, bool)> {
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
                            ext_messages.push(Box::new(ParsedMessage {
                                info: MsgInfo::ExtIn(ext_msg.info().clone()),
                                cell: ext_msg.cell().clone(),
                                special_origin: None,
                                dequeued: None,
                            }));
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
        is_masterchain: bool,
        shards_max_end_lt: u64,
    ) -> Result<u64> {
        tracing::trace!(target: tracing_targets::COLLATOR, "calc_start_lt()");

        let mut start_lt = if !is_masterchain {
            std::cmp::max(
                mc_data.mc_state_stuff().state().gen_lt,
                prev_shard_data.gen_lt(),
            )
        } else {
            std::cmp::max(mc_data.mc_state_stuff().state().gen_lt, shards_max_end_lt)
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
        tracing::trace!(target: tracing_targets::COLLATOR, "compute_minted_amount");

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
        tracing::trace!(target: tracing_targets::COLLATOR, "create_special_transactions");

        let config = self.working_state().mc_data.config();

        // TODO: Execute in parallel if addresses are distinct?

        if !collator_data.value_flow.recovered.tokens.is_zero() {
            self.create_special_transaction(
                &config.get_fee_collector_address()?,
                collator_data.value_flow.recovered.clone(),
                SpecialOrigin::Recover,
                collator_data,
                exec_manager,
            )
            .await?;
        }

        // FIXME: Minter only mints extra currencies, so this will not work
        // TODO: Add `is_zero` to `CurrencyCollection` (but it might be heavy,
        // since we must check all dict items)
        if !collator_data.value_flow.minted.tokens.is_zero() {
            self.create_special_transaction(
                &config.get_minter_address()?,
                collator_data.value_flow.minted.clone(),
                SpecialOrigin::Mint,
                collator_data,
                exec_manager,
            )
            .await?;
        }

        Ok(())
    }

    async fn create_special_transaction(
        &self,
        account_id: &HashBytes,
        amount: CurrencyCollection,
        special_origin: SpecialOrigin,
        collation_data: &mut BlockCollationData,
        exec_manager: &mut ExecutionManager,
    ) -> Result<()> {
        tracing::trace!(
            target: tracing_targets::COLLATOR,
            account_addr = %account_id,
            amount = %amount.tokens,
            ?special_origin,
            "create_special_transaction",
        );

        let Some(account_stuff) = exec_manager.take_account_stuff_if(account_id, |_| true)? else {
            return Ok(());
        };

        let in_message = {
            let info = MsgInfo::Int(IntMsgInfo {
                ihr_disabled: false,
                bounce: true,
                bounced: false,
                src: IntAddr::from((-1, HashBytes::ZERO)),
                dst: IntAddr::from((-1, *account_id)),
                value: amount,
                ihr_fee: Default::default(),
                fwd_fee: Default::default(),
                created_lt: collation_data.start_lt,
                created_at: collation_data.gen_utime,
            });
            let cell = CellBuilder::build_from(BaseMessage {
                info: info.clone(),
                init: None,
                body: CellSlice::default(),
                layout: None,
            })?;

            Box::new(ParsedMessage {
                info,
                cell,
                special_origin: Some(special_origin),
                dequeued: None,
            })
        };

        let executed = exec_manager
            .execute_ordinary_transaction(account_stuff, in_message)
            .await?;

        let executor_output = executed.result?;

        new_transaction(
            collation_data,
            &self.shard_id,
            executor_output,
            executed.in_message,
        )?;

        collation_data.next_lt = exec_manager.min_next_lt();
        Ok(())
    }

    async fn create_ticktock_transactions(
        &self,
        tick_tock: TickTock,
        collation_data: &mut BlockCollationData,
        exec_manager: &mut ExecutionManager,
    ) -> Result<()> {
        tracing::trace!(
            target: tracing_targets::COLLATOR,
            kind = ?tick_tock,
            "create_ticktock_transactions"
        );

        // TODO: Execute in parallel since these are unique accounts

        let config = self.working_state().mc_data.config();
        for account_id in config.get_fundamental_addresses()?.keys() {
            self.create_ticktock_transaction(&account_id?, tick_tock, collation_data, exec_manager)
                .await?;
        }

        self.create_ticktock_transaction(&config.address, tick_tock, collation_data, exec_manager)
            .await?;
        Ok(())
    }

    async fn create_ticktock_transaction(
        &self,
        account_id: &HashBytes,
        tick_tock: TickTock,
        collation_data: &mut BlockCollationData,
        exec_manager: &mut ExecutionManager,
    ) -> Result<()> {
        tracing::trace!(
            target: tracing_targets::COLLATOR,
            account_addr = %account_id,
            kind = ?tick_tock,
            "create_ticktock_transaction",
        );

        let Some(account_stuff) =
            exec_manager.take_account_stuff_if(&account_id, |stuff| match tick_tock {
                TickTock::Tick => stuff.special.tick,
                TickTock::Tock => stuff.special.tock,
            })?
        else {
            return Ok(());
        };

        let _executor_output = exec_manager
            .execute_ticktock_transaction(account_stuff, tick_tock)
            .await?;

        // TODO: It does nothing for ticktock, so commented for now
        // new_transaction(collation_data, &self.shard_id, transaction.1, async_message)?;

        collation_data.execute_count_all += 1;
        collation_data.next_lt = exec_manager.min_next_lt();
        Ok(())
    }

    fn import_new_shard_top_blocks_for_masterchain(
        &self,
        config: &BlockchainConfig,
        collation_data_builder: &mut BlockCollationDataBuilder,
        top_shard_blocks_info: Vec<TopBlockDescription>,
    ) -> Result<()> {
        tracing::trace!(target: tracing_targets::COLLATOR,
            "import_new_shard_top_blocks_for_masterchain()",
        );

        let gen_utime = collation_data_builder.gen_utime;
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
            shard_descr.reg_mc_seqno = collation_data_builder.block_id_short.seqno;

            collation_data_builder.update_shards_max_end_lt(shard_descr.end_lt);

            let shard_id = block_id.shard;

            collation_data_builder.top_shard_blocks_ids.push(block_id);

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
                collation_data_builder.shards_mut()?,
                shard_id,
                shard_descr,
            )?;

            collation_data_builder.store_shard_fees(shard_id, proof_funds)?;
            collation_data_builder.register_shard_block_creators(creators)?;
        }

        let shard_fees = collation_data_builder.shard_fees.root_extra().clone();

        collation_data_builder
            .value_flow
            .fees_collected
            .checked_add(&shard_fees.fees)?;
        collation_data_builder.value_flow.fees_imported = shard_fees.fees;

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
        self.stats.int_queue_length += collation_data.int_enqueue_count;
        self.stats.int_queue_length = self
            .stats
            .int_queue_length
            .checked_sub(collation_data.int_dequeue_count)
            .unwrap_or_default();
    }
}

/// add in and out messages from to block
fn new_transaction(
    collation_data: &mut BlockCollationData,
    shard_id: &ShardIdent,
    executor_output: ExecutorOutput,
    in_msg: Box<ParsedMessage>,
) -> Result<Vec<Box<ParsedMessage>>> {
    tracing::trace!(
        target: tracing_targets::COLLATOR,
        message_hash = %in_msg.cell.repr_hash(),
        transaction_hash = %executor_output.transaction.inner().repr_hash(),
        "process new transaction from message",
    );

    collation_data.execute_count_all += 1;
    collation_data.block_limit.gas_used += executor_output.gas_used as u32;

    let import_fees;
    let in_msg_hash = *in_msg.cell.repr_hash();
    let in_msg = match (in_msg.info, in_msg.special_origin) {
        // Messages with special origin are always immediate
        (_, Some(_)) => {
            let in_msg = InMsg::Immediate(InMsgFinal {
                in_msg_envelope: Lazy::new(&MsgEnvelope {
                    cur_addr: IntermediateAddr::FULL_SRC_SAME_WORKCHAIN,
                    next_addr: IntermediateAddr::FULL_SRC_SAME_WORKCHAIN,
                    fwd_fee_remaining: Default::default(),
                    message: Lazy::from_raw(in_msg.cell),
                })?,
                transaction: executor_output.transaction.clone(),
                fwd_fee: Default::default(),
            });

            import_fees = in_msg.compute_fees()?;
            Lazy::new(&in_msg)?
        }
        // External messages are added as is
        (MsgInfo::ExtIn(_), _) => {
            collation_data.execute_count_ext += 1;

            import_fees = ImportFees::default();
            Lazy::new(&InMsg::External(InMsgExternal {
                in_msg: Lazy::from_raw(in_msg.cell),
                transaction: executor_output.transaction.clone(),
            }))?
        }
        // Dequeued messages have a dedicated `InMsg` type
        (MsgInfo::Int(IntMsgInfo { fwd_fee, .. }), _) if in_msg.dequeued.is_some() => {
            collation_data.execute_count_int += 1;

            let same_shard = in_msg.dequeued.map(|d| d.same_shard).unwrap_or_default();

            let envelope = Lazy::new(&MsgEnvelope {
                // NOTE: `cur_addr` is not used in current routing between shards logic
                cur_addr: if same_shard {
                    IntermediateAddr::FULL_DEST_SAME_WORKCHAIN
                } else {
                    IntermediateAddr::FULL_SRC_SAME_WORKCHAIN
                },
                next_addr: IntermediateAddr::FULL_DEST_SAME_WORKCHAIN,
                fwd_fee_remaining: fwd_fee,
                message: Lazy::from_raw(in_msg.cell),
            })?;

            let in_msg = InMsg::Final(InMsgFinal {
                in_msg_envelope: envelope.clone(),
                transaction: executor_output.transaction.clone(),
                fwd_fee,
            });
            import_fees = in_msg.compute_fees()?;

            let in_msg = Lazy::new(&in_msg)?;

            if same_shard {
                let out_msg = OutMsg::DequeueImmediate(OutMsgDequeueImmediate {
                    out_msg_envelope: envelope.clone(),
                    reimport: in_msg.clone(),
                });
                let exported_value = out_msg.compute_exported_value()?;

                collation_data.out_msgs.insert(in_msg_hash, PreparedOutMsg {
                    out_msg: Lazy::new(&out_msg)?,
                    exported_value,
                    new_tx: None,
                });
            }
            collation_data.int_dequeue_count += 1;

            in_msg
        }
        // New messages are added as is
        (MsgInfo::Int(IntMsgInfo { fwd_fee, .. }), _) => {
            collation_data.execute_count_new_int += 1;

            let msg_envelope = MsgEnvelope {
                cur_addr: IntermediateAddr::FULL_SRC_SAME_WORKCHAIN,
                next_addr: IntermediateAddr::FULL_SRC_SAME_WORKCHAIN,
                fwd_fee_remaining: fwd_fee,
                message: Lazy::from_raw(in_msg.cell),
            };
            let in_msg = InMsg::Immediate(InMsgFinal {
                in_msg_envelope: Lazy::new(&msg_envelope)?,
                transaction: executor_output.transaction.clone(),
                fwd_fee,
            });

            import_fees = in_msg.compute_fees()?;
            let in_msg = Lazy::new(&in_msg)?;

            let prev_transaction = match collation_data.out_msgs.get(&in_msg_hash) {
                Some(prepared) => match &prepared.new_tx {
                    Some(tx) => tx.clone(),
                    None => anyhow::bail!("invalid out message state for in_msg {in_msg_hash}"),
                },
                None => anyhow::bail!("immediate in_msg {in_msg_hash} not found in out_msgs"),
            };

            let out_msg = OutMsg::Immediate(OutMsgImmediate {
                out_msg_envelope: Lazy::new(&msg_envelope)?,
                transaction: prev_transaction,
                reimport: in_msg.clone(),
            });
            let exported_value = out_msg.compute_exported_value()?;

            collation_data.out_msgs.insert(in_msg_hash, PreparedOutMsg {
                out_msg: Lazy::new(&out_msg)?,
                exported_value,
                new_tx: None,
            });
            collation_data.int_enqueue_count -= 1;

            in_msg
        }
        (msg_info, special_origin) => {
            unreachable!(
                "unexpected message. info: {msg_info:?}, \
                special_origin: {special_origin:?}"
            )
        }
    };

    collation_data.in_msgs.insert(in_msg_hash, PreparedInMsg {
        in_msg,
        import_fees,
    });

    let mut out_messages = vec![];

    for out_msg_cell in executor_output.out_msgs.values() {
        let out_msg_cell = out_msg_cell?;
        let out_msg_hash = *out_msg_cell.repr_hash();
        let out_msg_info = out_msg_cell.parse::<MsgInfo>()?;

        tracing::trace!(
            target: tracing_targets::COLLATOR,
            message_hash = %out_msg_hash,
            info = ?out_msg_info,
            "adding out message to out_msgs",
        );
        match &out_msg_info {
            MsgInfo::Int(IntMsgInfo { fwd_fee, dst, .. }) => {
                collation_data.int_enqueue_count += 1;

                let dst_prefix = dst.prefix();
                let dst_workchain = dst.workchain();

                let out_msg = OutMsg::New(OutMsgNew {
                    out_msg_envelope: Lazy::new(&MsgEnvelope {
                        cur_addr: IntermediateAddr::FULL_SRC_SAME_WORKCHAIN,
                        // NOTE: `next_addr` is not used in current routing between shards logic
                        next_addr: if contains_prefix(shard_id, dst_workchain, dst_prefix) {
                            IntermediateAddr::FULL_DEST_SAME_WORKCHAIN
                        } else {
                            IntermediateAddr::FULL_SRC_SAME_WORKCHAIN
                        },
                        fwd_fee_remaining: *fwd_fee,
                        message: Lazy::from_raw(out_msg_cell.clone()),
                    })?,
                    transaction: executor_output.transaction.clone(),
                });

                collation_data
                    .out_msgs
                    .insert(out_msg_hash, PreparedOutMsg {
                        out_msg: Lazy::new(&out_msg)?,
                        exported_value: out_msg.compute_exported_value()?,
                        new_tx: Some(executor_output.transaction.clone()),
                    });

                out_messages.push(Box::new(ParsedMessage {
                    info: out_msg_info,
                    cell: out_msg_cell,
                    special_origin: None,
                    dequeued: None,
                }));
            }
            MsgInfo::ExtOut(_) => {
                let out_msg = OutMsg::External(OutMsgExternal {
                    out_msg: Lazy::from_raw(out_msg_cell),
                    transaction: executor_output.transaction.clone(),
                });

                collation_data
                    .out_msgs
                    .insert(out_msg_hash, PreparedOutMsg {
                        out_msg: Lazy::new(&out_msg)?,
                        exported_value: out_msg.compute_exported_value()?,
                        new_tx: None,
                    });
            }
            MsgInfo::ExtIn(_) => bail!("External inbound message cannot be an output"),
        }
    }

    Ok(out_messages)
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
