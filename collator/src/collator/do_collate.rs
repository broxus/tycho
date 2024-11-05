use std::collections::hash_map;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use everscale_types::models::*;
use everscale_types::num::Tokens;
use everscale_types::prelude::*;
use humantime::format_duration;
use sha2::Digest;
use ton_executor::{ExecuteParams, ExecutorOutput, PreloadedBlockchainConfig};
use tycho_block_util::queue::{QueueDiffStuff, QueueKey};
use tycho_storage::NewBlockMeta;
use tycho_util::futures::JoinTask;
use tycho_util::metrics::HistogramGuard;
use tycho_util::time::now_millis;
use tycho_util::FastHashMap;

use super::execution_manager::{ExecutionManager, MessagesExecutor};
use super::mq_iterator_adapter::QueueIteratorAdapter;
use super::types::{
    AnchorsCache, BlockCollationDataBuilder, BlockLimitsLevel, ParsedExternals,
    ReadNextExternalsMode, SpecialOrigin, WorkingState,
};
use super::CollatorStdImpl;
use crate::collator::execution_manager::GetNextMessageGroupMode;
use crate::collator::mq_iterator_adapter::InitIteratorMode;
use crate::collator::types::{
    BlockCollationData, ParsedMessage, PreparedInMsg, PreparedOutMsg, ShardDescriptionExt,
};
use crate::internal_queue::iterator::QueueIterator;
use crate::internal_queue::types::EnqueuedMessage;
use crate::tracing_targets;
use crate::types::{
    BlockCollationResult, BlockIdExt, DisplayBlockIdsIntoIter, DisplayBlockIdsIter,
    DisplayExternalsProcessedUpto, McData, TopBlockDescription,
};

#[cfg(test)]
#[path = "tests/do_collate_tests.rs"]
pub(super) mod tests;

impl CollatorStdImpl {
    #[tracing::instrument(
        parent =  None,
        skip_all,
        fields(
            block_id = %self.next_block_info,
            ct = self.anchors_cache.get_last_imported_anchor_ct().unwrap_or_default()
        )
    )]
    pub(super) async fn do_collate(
        &mut self,
        working_state: Box<WorkingState>,
        top_shard_blocks_info: Option<Vec<TopBlockDescription>>,
        next_chain_time: u64,
    ) -> Result<()> {
        let labels = [("workchain", self.shard_id.workchain().to_string())];
        let total_collation_histogram =
            HistogramGuard::begin_with_labels("tycho_do_collate_total_time", &labels);

        let prepare_histogram =
            HistogramGuard::begin_with_labels("tycho_do_collate_prepare_time", &labels);

        let WorkingState {
            next_block_id_short,
            mc_data,
            collation_config,
            wu_used_from_last_anchor,
            prev_shard_data,
            usage_tree,
            has_unprocessed_messages: _, // TODO: Check why it is not used anywhere
            msgs_buffer,
        } = *working_state;

        let config = &mc_data.config;
        let prev_shard_data = prev_shard_data.unwrap();
        let mut msgs_buffer = msgs_buffer.unwrap();
        let usage_tree = usage_tree.unwrap();
        let prev_mc_block_id = mc_data.block_id;
        let tracker = prev_shard_data.ref_mc_state_handle().tracker().clone();

        tracing::info!(target: tracing_targets::COLLATOR,
            "Start collating block: next_block_id_short={}, prev_block_ids={}, top_shard_blocks_ids: {:?}",
            next_block_id_short,
            DisplayBlockIdsIntoIter(prev_shard_data.blocks_ids()),
            top_shard_blocks_info.as_ref().map(|v| DisplayBlockIdsIter(
                v.iter().map(|i| &i.block_id)
            )),
        );

        let created_by = self
            .anchors_cache
            .get_last_imported_anchor_author()
            .unwrap();

        // TODO: need to generate unique for each block
        // generate seed from the chain_time from the anchor
        let hash_bytes = sha2::Sha256::digest(next_chain_time.to_be_bytes());
        let rand_seed = HashBytes::from_slice(hash_bytes.as_slice());
        tracing::trace!(target: tracing_targets::COLLATOR, "rand_seed from chain time: {}", rand_seed);

        let is_masterchain = self.shard_id.is_masterchain();

        if !is_masterchain {
            self.shard_blocks_count_from_last_anchor += 1;
        }

        // prepare block collation data
        let block_limits = config.get_block_limits(is_masterchain)?;
        tracing::debug!(target: tracing_targets::COLLATOR,
            "Block limits: {:?}",
            block_limits
        );

        let mut collation_data_builder = BlockCollationDataBuilder::new(
            next_block_id_short,
            rand_seed,
            mc_data.block_id.seqno,
            next_chain_time,
            prev_shard_data.processed_upto().clone(),
            created_by,
            GlobalVersion {
                version: self.config.supported_block_version,
                capabilities: self.config.supported_capabilities,
            },
            self.mempool_config_override.clone(),
        );

        // init ShardHashes descriptions for master
        if is_masterchain {
            let shards = prev_shard_data.observable_states()[0]
                .shards()?
                .iter()
                .map(|entry| entry.map(|(shard_ident, descr)| (shard_ident, Box::new(descr))))
                .collect::<Result<FastHashMap<_, _>, _>>()?;
            collation_data_builder.set_shards(shards);

            if let Some(top_shard_blocks_info) = top_shard_blocks_info {
                Self::import_new_shard_top_blocks_for_masterchain(
                    config,
                    &mut collation_data_builder,
                    top_shard_blocks_info,
                )?;
            }
        }

        let start_lt = Self::calc_start_lt(
            &mc_data,
            prev_shard_data.gen_lt(),
            is_masterchain,
            collation_data_builder.shards_max_end_lt,
        )?;

        let mut collation_data = Box::new(collation_data_builder.build(start_lt, block_limits));

        tracing::debug!(target: tracing_targets::COLLATOR, "initial processed_upto.externals = {:?}",
            collation_data.processed_upto.externals.as_ref().map(DisplayExternalsProcessedUpto),
        );

        // show internals processed upto
        collation_data
            .processed_upto
            .internals
            .iter()
            .for_each(|(shard_ident, processed_upto)| {
                tracing::debug!(target: tracing_targets::COLLATOR,
                    "initial processed_upto.internals for shard {}: {}",
                    shard_ident, processed_upto,
                );
            });

        // compute created / minted / recovered / from_prev_block
        let prev_shard_accounts = prev_shard_data.observable_accounts().clone();

        let prev_data_total_balance = prev_shard_accounts.root_extra().balance.clone();

        self.update_value_flow(
            config,
            &mc_data.global_balance,
            prev_data_total_balance,
            &mc_data.total_validator_fees,
            &mut collation_data,
        )?;

        // prepare to read and execute internals and externals

        // init executor
        let mut executor = MessagesExecutor::new(
            self.shard_id,
            collation_data.next_lt,
            Arc::new(PreloadedBlockchainConfig::with_config(
                config.clone(),
                mc_data.global_id,
            )?),
            Arc::new(ExecuteParams {
                state_libs: mc_data.libraries.clone(),
                // generated unix time
                block_unixtime: collation_data.gen_utime,
                // block's start logical time
                block_lt: collation_data.start_lt,
                // block random seed
                seed_block: collation_data.rand_seed,
                block_version: self.config.supported_block_version,
                ..ExecuteParams::default()
            }),
            prev_shard_accounts,
            collation_config.work_units_params.execute.clone(),
        );

        // if this is a masterchain, we must take top shard blocks end lt
        let mc_top_shards_end_lts: Vec<_> = if self.shard_id.is_masterchain() {
            collation_data
                .shards()?
                .iter()
                .map(|(k, v)| (*k, v.end_lt))
                .collect()
        } else {
            mc_data.shards.iter().map(|(k, v)| (*k, v.end_lt)).collect()
        };

        // create iterator adapter
        let mut mq_iterator_adapter = QueueIteratorAdapter::new(
            self.shard_id,
            self.mq_adapter.clone(),
            msgs_buffer.current_iterator_positions.take().unwrap(),
            mc_data.gen_lt,
            prev_shard_data.gen_lt(),
        );

        // we need to init iterator anyway because we need it to add new messages to queue
        tracing::debug!(target: tracing_targets::COLLATOR,
            "init iterator for current ranges"
        );
        mq_iterator_adapter
            .try_init_next_range_iterator(
                &mut collation_data.processed_upto,
                mc_top_shards_end_lts.iter().copied(),
                // We always init first iterator during block collation
                // with current ranges from processed_upto info
                // and do not touch next range before we read all existing messages buffer.
                // In this case the initial iterator range will be equal both
                // on Refill and on Continue.
                InitIteratorMode::OmitNextRange,
            )
            .await?;

        // create exec_manager
        let mut exec_manager = ExecutionManager::new(
            self.shard_id,
            collation_config.msgs_exec_params.buffer_limit as _,
            mc_top_shards_end_lts,
        );

        // refill messages buffer and skip groups upto offset (on node restart)
        let prev_processed_offset = collation_data.processed_upto.processed_offset;
        if !msgs_buffer.has_pending_messages() && prev_processed_offset > 0 {
            tracing::debug!(target: tracing_targets::COLLATOR,
                prev_processed_offset,
                "refill messages buffer and skip groups upto",
            );

            while msgs_buffer.message_groups_offset() < prev_processed_offset {
                let msg_group = exec_manager
                    .get_next_message_group(
                        &mut msgs_buffer,
                        &mut self.anchors_cache,
                        &mut collation_data,
                        &mut mq_iterator_adapter,
                        &QueueKey::MIN,
                        GetNextMessageGroupMode::Refill,
                    )
                    .await?;
                if msg_group.is_none() {
                    // on restart from a new genesis we will not be able to refill buffer with externals
                    // so we stop refilling when there is no more groups in buffer
                    break;
                }
            }

            // next time we should read next message group like we did not make refill before
            // so we need to reset flags that control from where to read messages
            exec_manager.reset_read_flags();
        }

        let prepare_elapsed = prepare_histogram.finish();

        // holds the max LT_HASH of a new created messages to current shard
        // it needs to define the read range for new messages when we get next message group
        let mut max_new_message_key_to_current_shard = QueueKey::MIN;

        // execute tick transaction and special transactions (mint, recover)
        let execute_tick_elapsed;
        if is_masterchain {
            let histogram =
                HistogramGuard::begin_with_labels("tycho_do_collate_execute_tick_time", &labels);

            self.create_ticktock_transactions(
                config,
                TickTock::Tick,
                &mut collation_data,
                &mut executor,
                &mut max_new_message_key_to_current_shard,
                mq_iterator_adapter.iterator(),
            )
            .await?;

            self.create_special_transactions(
                config,
                &mut collation_data,
                &mut executor,
                &mut max_new_message_key_to_current_shard,
                mq_iterator_adapter.iterator(),
            )
            .await?;

            execute_tick_elapsed = histogram.finish();
        } else {
            execute_tick_elapsed = Duration::ZERO;
        }

        let execute_histogram =
            HistogramGuard::begin_with_labels("tycho_do_collate_execute_time", &labels);

        let mut fill_msgs_total_elapsed = Duration::ZERO;
        let mut execute_msgs_total_elapsed = Duration::ZERO;
        let mut process_txs_total_elapsed = Duration::ZERO;
        let mut execute_groups_wu_vm_only = 0u64;

        {
            let mut executed_groups_count = 0;
            loop {
                let mut timer = std::time::Instant::now();
                let msgs_group_opt = exec_manager
                    .get_next_message_group(
                        &mut msgs_buffer,
                        &mut self.anchors_cache,
                        &mut collation_data,
                        &mut mq_iterator_adapter,
                        &max_new_message_key_to_current_shard,
                        GetNextMessageGroupMode::Continue,
                    )
                    .await?;
                fill_msgs_total_elapsed += timer.elapsed();

                if let Some(msgs_group) = msgs_group_opt {
                    // Execute messages group
                    timer = std::time::Instant::now();
                    let group_result = executor.execute_group(msgs_group).await?;
                    execute_msgs_total_elapsed += timer.elapsed();
                    executed_groups_count += 1;
                    collation_data.tx_count += group_result.items.len() as u64;
                    collation_data.ext_msgs_error_count += group_result.ext_msgs_error_count;
                    collation_data.ext_msgs_skipped += group_result.ext_msgs_skipped;
                    execute_groups_wu_vm_only = execute_groups_wu_vm_only
                        .saturating_add(group_result.total_exec_wu)
                        .saturating_add(group_result.ext_msgs_error_count.saturating_mul(
                            collation_config.work_units_params.execute.execute_err as u64,
                        ));

                    // Process transactions
                    timer = std::time::Instant::now();
                    for item in group_result.items {
                        self.process_transaction(
                            item.executor_output,
                            Some(item.in_message),
                            &mut collation_data,
                            &executor,
                            &mut max_new_message_key_to_current_shard,
                            mq_iterator_adapter.iterator(),
                        )?;
                    }
                    process_txs_total_elapsed += timer.elapsed();

                    if collation_data.block_limit.reached(BlockLimitsLevel::Hard) {
                        tracing::debug!(target: tracing_targets::COLLATOR,
                            "block limits reached: {:?}", collation_data.block_limit,
                        );
                        metrics::counter!(
                            "tycho_do_collate_blocks_with_limits_reached_count",
                            &labels
                        )
                        .increment(1);
                        break;
                    }
                } else if mq_iterator_adapter.no_pending_existing_internals()
                    && mq_iterator_adapter.no_pending_new_messages()
                {
                    break;
                }
            }

            metrics::gauge!("tycho_do_collate_exec_msgs_groups_per_block", &labels)
                .set(executed_groups_count as f64);
        }

        metrics::histogram!("tycho_do_collate_fill_msgs_total_time", &labels)
            .record(fill_msgs_total_elapsed);

        let init_iterator_elapsed = mq_iterator_adapter.init_iterator_total_elapsed();
        metrics::histogram!("tycho_do_collate_init_iterator_time", &labels)
            .record(init_iterator_elapsed);
        let read_existing_messages_elapsed = exec_manager.read_existing_messages_total_elapsed();
        metrics::histogram!("tycho_do_collate_read_int_msgs_time", &labels)
            .record(read_existing_messages_elapsed);
        let read_new_messages_elapsed = exec_manager.read_new_messages_total_elapsed();
        metrics::histogram!("tycho_do_collate_read_new_msgs_time", &labels)
            .record(read_new_messages_elapsed);
        let read_ext_messages_elapsed = exec_manager.read_ext_messages_total_elapsed();
        metrics::histogram!("tycho_do_collate_read_ext_msgs_time", &labels)
            .record(read_ext_messages_elapsed);
        let add_to_message_groups_elapsed = exec_manager.add_to_message_groups_total_elapsed();
        metrics::histogram!("tycho_do_collate_add_to_msg_groups_time", &labels)
            .record(add_to_message_groups_elapsed);

        metrics::histogram!("tycho_do_collate_exec_msgs_total_time", &labels)
            .record(execute_msgs_total_elapsed);
        metrics::histogram!("tycho_do_collate_process_txs_total_time", &labels)
            .record(process_txs_total_elapsed);

        let execute_elapsed = execute_histogram.finish();

        // execute tock transaction
        let execute_tock_elapsed;
        if is_masterchain {
            let histogram =
                HistogramGuard::begin_with_labels("tycho_do_collate_execute_tock_time", &labels);
            self.create_ticktock_transactions(
                config,
                TickTock::Tock,
                &mut collation_data,
                &mut executor,
                &mut max_new_message_key_to_current_shard,
                mq_iterator_adapter.iterator(),
            )
            .await?;
            execute_tock_elapsed = histogram.finish();
        } else {
            execute_tock_elapsed = Duration::ZERO;
        }

        // get queue diff and check for pending internals
        let histogram_create_queue_diff =
            HistogramGuard::begin_with_labels("tycho_do_collate_create_queue_diff_time", &labels);

        let (has_pending_internals_in_iterator, diff_with_messages) = mq_iterator_adapter.release(
            !msgs_buffer.has_pending_messages(),
            &mut msgs_buffer.current_iterator_positions,
        )?;

        let create_queue_diff_elapsed = histogram_create_queue_diff.finish();

        let diff_messages_len = diff_with_messages.messages.len();
        let has_unprocessed_messages =
            msgs_buffer.has_pending_messages() || has_pending_internals_in_iterator;

        let (min_message, max_message) = {
            let messages = &diff_with_messages.messages;
            match messages.first_key_value().zip(messages.last_key_value()) {
                Some(((min, _), (max, _))) => (*min, *max),
                None => (
                    QueueKey::min_for_lt(collation_data.start_lt),
                    QueueKey::max_for_lt(collation_data.next_lt),
                ),
            }
        };

        let queue_diff = QueueDiffStuff::builder(
            self.shard_id,
            collation_data.block_id_short.seqno,
            &prev_shard_data
                .prev_queue_diff_hashes()
                .first()
                .cloned()
                .unwrap_or_default(),
        )
        .with_processed_upto(
            diff_with_messages
                .processed_upto
                .iter()
                .map(|(k, v)| (*k, v.lt, &v.hash)),
        )
        .with_messages(
            &min_message,
            &max_message,
            diff_with_messages.messages.keys().map(|k| &k.hash),
        )
        .serialize();

        let queue_diff_hash = *queue_diff.hash();
        tracing::debug!(target: tracing_targets::COLLATOR, queue_diff_hash = %queue_diff_hash);

        // start async update queue task
        let update_queue_task: JoinTask<std::result::Result<Duration, anyhow::Error>> =
            JoinTask::<Result<_>>::new({
                let block_id_short = collation_data.block_id_short;
                let mq_adapter = self.mq_adapter.clone();
                let labels = labels.clone();
                async move {
                    // apply queue diff
                    let histogram = HistogramGuard::begin_with_labels(
                        "tycho_do_collate_apply_queue_diff_time",
                        &labels,
                    );

                    mq_adapter
                        .apply_diff(diff_with_messages, block_id_short, &queue_diff_hash)
                        .await?;
                    let apply_queue_diff_elapsed = histogram.finish();

                    Ok(apply_queue_diff_elapsed)
                }
            });

        let prepare_groups_wu_total = calc_prepare_groups_wu_total(
            &collation_data,
            &collation_config.work_units_params.prepare,
        );

        let process_txs_wu =
            calc_process_txs_wu(&collation_data, &collation_config.work_units_params.execute);
        let execute_groups_wu_total = execute_groups_wu_vm_only.saturating_add(process_txs_wu);

        // build block candidate and new state
        let finalize_block_timer = std::time::Instant::now();
        let finalized = tycho_util::sync::rayon_run({
            let collation_session = self.collation_session.clone();
            let finalize_params = collation_config.work_units_params.finalize.clone();
            let old_mc_data = mc_data.clone();
            move || {
                Self::finalize_block(
                    collation_data,
                    collation_session,
                    executor,
                    old_mc_data,
                    prev_shard_data,
                    wu_used_from_last_anchor,
                    usage_tree,
                    queue_diff,
                    finalize_params,
                    prepare_groups_wu_total,
                    execute_groups_wu_total,
                )
            }
        })
        .await?;
        collation_data = finalized.collation_data;

        let block_id = *finalized.block_candidate.block.id();
        let is_key_block = finalized.block_candidate.is_key_block;
        let store_new_state_task = JoinTask::new({
            let meta = NewBlockMeta {
                is_key_block,
                gen_utime: collation_data.gen_utime,
                ref_by_mc_seqno: finalized.block_candidate.ref_by_mc_seqno,
            };
            let adapter = self.state_node_adapter.clone();
            let labels = labels.clone();
            let new_state_root = finalized.new_state_root.clone();
            async move {
                let _histogram = HistogramGuard::begin_with_labels(
                    "tycho_collator_build_new_state_time",
                    &labels,
                );
                adapter
                    .store_state_root(&block_id, meta, new_state_root)
                    .await
            }
        });

        // resolve update queue task
        let apply_queue_diff_elapsed = update_queue_task.await?;

        let handle_block_candidate_elapsed;
        {
            let histogram = HistogramGuard::begin_with_labels(
                "tycho_do_collate_handle_block_candidate_time",
                &labels,
            );

            let new_queue_diff_hash = *finalized.block_candidate.queue_diff_aug.diff_hash();

            let collation_config = match &finalized.mc_data {
                Some(mcd) => Arc::new(mcd.config.get_collation_config()?),
                None => collation_config,
            };

            // return collation result
            self.listener
                .on_block_candidate(BlockCollationResult {
                    collation_session_id: self.collation_session.id(),
                    candidate: finalized.block_candidate,
                    prev_mc_block_id,
                    mc_data: finalized.mc_data.clone(),
                    collation_config: collation_config.clone(),
                    has_unprocessed_messages,
                })
                .await?;

            let new_mc_data = finalized.mc_data.unwrap_or(mc_data.clone());

            // spawn update PrevData and working state
            self.prepare_working_state_update(
                block_id,
                finalized.new_observable_state,
                finalized.new_state_root,
                store_new_state_task,
                new_queue_diff_hash,
                new_mc_data,
                collation_config,
                has_unprocessed_messages,
                msgs_buffer,
                tracker,
            )
            .await?;

            tracing::debug!(target: tracing_targets::COLLATOR,
                "working state updated prepare spawned",
            );

            // update next block info
            self.next_block_info = block_id.get_next_id_short();

            handle_block_candidate_elapsed = histogram.finish();
        }

        // metrics
        metrics::counter!("tycho_do_collate_tx_total", &labels).increment(collation_data.tx_count);
        metrics::gauge!("tycho_do_collate_tx_per_block", &labels)
            .set(collation_data.tx_count as f64);
        metrics::gauge!("tycho_do_collate_accounts_per_block", &labels)
            .set(collation_data.accounts_count as f64);

        metrics::counter!("tycho_do_collate_int_enqueue_count")
            .increment(collation_data.int_enqueue_count);
        metrics::counter!("tycho_do_collate_int_dequeue_count")
            .increment(collation_data.int_dequeue_count);
        metrics::gauge!("tycho_do_collate_int_msgs_queue_calc").increment(
            (collation_data.int_enqueue_count as i64 - collation_data.int_dequeue_count as i64)
                as f64,
        );

        metrics::counter!("tycho_do_collate_msgs_exec_count_all", &labels)
            .increment(collation_data.execute_count_all);

        // external messages
        metrics::counter!("tycho_do_collate_msgs_read_count_ext", &labels)
            .increment(collation_data.read_ext_msgs);
        metrics::counter!("tycho_do_collate_msgs_exec_count_ext", &labels)
            .increment(collation_data.execute_count_ext);
        metrics::counter!("tycho_do_collate_msgs_error_count_ext", &labels)
            .increment(collation_data.ext_msgs_error_count);
        metrics::counter!("tycho_do_collate_msgs_skipped_count_ext", &labels)
            .increment(collation_data.ext_msgs_skipped);

        // existing internals messages
        metrics::counter!("tycho_do_collate_msgs_read_count_int", &labels)
            .increment(collation_data.read_int_msgs_from_iterator);
        metrics::counter!("tycho_do_collate_msgs_exec_count_int", &labels)
            .increment(collation_data.execute_count_int);

        // new internals messages
        metrics::counter!("tycho_do_collate_new_msgs_created_count", &labels)
            .increment(collation_data.new_msgs_created);
        metrics::counter!(
            "tycho_do_collate_new_msgs_inserted_to_iterator_count",
            &labels
        )
        .increment(collation_data.inserted_new_msgs_to_iterator);
        metrics::counter!("tycho_do_collate_msgs_read_count_new_int", &labels)
            .increment(collation_data.read_new_msgs_from_iterator);
        metrics::counter!("tycho_do_collate_msgs_exec_count_new_int", &labels)
            .increment(collation_data.execute_count_new_int);

        // blocks
        metrics::counter!("tycho_do_collate_blocks_count", &labels).increment(1);
        metrics::gauge!("tycho_do_collate_block_seqno", &labels)
            .set(collation_data.block_id_short.seqno);

        // work units
        let finalize_block_elapsed = finalize_block_timer.elapsed();
        let finalize_wu_total = finalized.finalize_wu_total;

        metrics::gauge!("tycho_do_collate_wu_on_prepare", &labels)
            .set(prepare_groups_wu_total as f64);
        metrics::gauge!("tycho_do_collate_wu_on_execute", &labels)
            .set(execute_groups_wu_total as f64);
        metrics::gauge!("tycho_do_collate_wu_on_finalize", &labels).set(finalize_wu_total as f64);
        metrics::gauge!("tycho_do_collate_wu_on_all", &labels).set(
            execute_groups_wu_total as f64
                + finalize_wu_total as f64
                + prepare_groups_wu_total as f64,
        );

        metrics::gauge!("tycho_do_collate_wu_to_mcs_prepare", &labels)
            .set(fill_msgs_total_elapsed.as_nanos() as f64 / prepare_groups_wu_total as f64);
        metrics::gauge!("tycho_do_collate_wu_to_mcs_execute", &labels).set(
            (execute_msgs_total_elapsed.as_nanos() as f64
                + process_txs_total_elapsed.as_nanos() as f64)
                / execute_groups_wu_total as f64,
        );
        metrics::gauge!("tycho_do_collate_execute_txs_to_wu", &labels)
            .set(execute_msgs_total_elapsed.as_nanos() as f64 / execute_groups_wu_vm_only as f64);
        metrics::gauge!("tycho_do_collate_process_txs_to_wu", &labels)
            .set(process_txs_total_elapsed.as_nanos() as f64 / process_txs_wu as f64);
        metrics::gauge!("tycho_do_collate_wu_to_mcs_finalize", &labels)
            .set(finalize_block_elapsed.as_nanos() as f64 / finalize_wu_total as f64);

        let total_elapsed = total_collation_histogram.finish();

        metrics::gauge!("tycho_do_collate_wu_to_mcs_total", &labels).set(
            total_elapsed.as_nanos() as f64
                / (execute_groups_wu_total + finalize_wu_total + prepare_groups_wu_total) as f64,
        );

        // time elapsed from prev block
        let elapsed_from_prev_block = self.timer.elapsed();
        let collation_mngmnt_overhead = elapsed_from_prev_block - total_elapsed;
        self.timer = std::time::Instant::now();
        metrics::histogram!("tycho_do_collate_from_prev_block_time", &labels)
            .record(elapsed_from_prev_block);
        metrics::histogram!("tycho_do_collate_overhead_time", &labels)
            .record(collation_mngmnt_overhead);

        // block time diff from now
        let block_time_diff = {
            let diff_time = now_millis() as i64 - next_chain_time as i64;
            metrics::gauge!("tycho_do_collate_block_time_diff", &labels)
                .set(diff_time as f64 / 1000.0);
            diff_time
        };

        // block time diff from min ext chain time
        let last_read_to_anchor_chain_time = exec_manager.get_last_read_to_anchor_chain_time();
        let diff_time =
            now_millis() as i64 - last_read_to_anchor_chain_time.unwrap_or(next_chain_time) as i64;
        metrics::gauge!("tycho_do_collate_ext_msgs_time_diff", &labels)
            .set(diff_time as f64 / 1000.0);

        collation_data.total_execute_msgs_time_mc = execute_msgs_total_elapsed.as_micros();
        self.update_stats(&collation_data);
        tracing::debug!(target: tracing_targets::COLLATOR, "{:?}", self.stats);

        tracing::info!(target: tracing_targets::COLLATOR,
            "collated_block_id={}, time_diff={}, \
            collation_time={}, elapsed_from_prev_block={}, overhead={}, \
            start_lt={}, end_lt={}, exec_count={}, \
            exec_ext={}, ext_err={}, exec_int={}, exec_new_int={}, \
            enqueue_count={}, dequeue_count={}, \
            new_msgs_created={}, new_msgs_added={}, \
            in_msgs={}, out_msgs={}, \
            read_ext_msgs={}, read_int_msgs={}, \
            read_new_msgs_from_iterator={}, inserted_new_msgs_to_iterator={} has_unprocessed_messages={}, \
            total_execute_msgs_time_mc={}, \
            wu_used_for_prepare_msgs_groups={}, wu_used_for_execute: {}, wu_used_for_finalize: {}",
            block_id, block_time_diff,
            total_elapsed.as_millis(), elapsed_from_prev_block.as_millis(), collation_mngmnt_overhead.as_millis(),
            collation_data.start_lt, collation_data.next_lt, collation_data.execute_count_all,
            collation_data.execute_count_ext, collation_data.ext_msgs_error_count,
            collation_data.execute_count_int, collation_data.execute_count_new_int,
            collation_data.int_enqueue_count, collation_data.int_dequeue_count,
            collation_data.new_msgs_created, diff_messages_len,
            collation_data.in_msgs.len(), collation_data.out_msgs.len(),
            collation_data.read_ext_msgs, collation_data.read_int_msgs_from_iterator,
            collation_data.read_new_msgs_from_iterator, collation_data.inserted_new_msgs_to_iterator, has_unprocessed_messages,
            collation_data.total_execute_msgs_time_mc,
            prepare_groups_wu_total, execute_groups_wu_total, finalize_wu_total
        );

        assert_eq!(
            collation_data.int_enqueue_count,
            collation_data.inserted_new_msgs_to_iterator - collation_data.execute_count_new_int
        );

        tracing::debug!(
            target: tracing_targets::COLLATOR,
            block_time_diff = block_time_diff,
            from_prev_block = %format_duration(elapsed_from_prev_block),
            overhead = %format_duration(collation_mngmnt_overhead),
            total = %format_duration(total_elapsed),
            prepare = %format_duration(prepare_elapsed),
            execute_tick = %format_duration(execute_tick_elapsed),
            execute_tock = %format_duration(execute_tock_elapsed),
            execute_total = %format_duration(execute_elapsed),

            fill_msgs_total = %format_duration(fill_msgs_total_elapsed),
            init_iterator = %format_duration(init_iterator_elapsed),
            read_existing = %format_duration(read_existing_messages_elapsed),
            read_ext = %format_duration(read_ext_messages_elapsed),
            read_new = %format_duration(read_new_messages_elapsed),
            add_to_groups = %format_duration(add_to_message_groups_elapsed),

            exec_msgs_total = %format_duration(execute_msgs_total_elapsed),
            process_txs_total = %format_duration(process_txs_total_elapsed),

            create_queue_diff = %format_duration(create_queue_diff_elapsed),
            apply_queue_diff = %format_duration(apply_queue_diff_elapsed),
            finalize_block = %format_duration(finalize_block_elapsed),
            handle_block_candidate = %format_duration(handle_block_candidate_elapsed),
            "collation timings"
        );

        Ok(())
    }

    /// Read specified number of externals from imported anchors
    /// using actual `processed_upto` info
    #[allow(clippy::vec_box)]
    pub(super) fn read_next_externals(
        shard_id: &ShardIdent,
        anchors_cache: &mut AnchorsCache,
        count: usize,
        next_chain_time: u64,
        processed_upto_externals: &mut Option<ExternalsProcessedUpto>,
        current_reader_position: Option<(u32, u64)>,
        read_mode: ReadNextExternalsMode,
    ) -> Result<ParsedExternals> {
        let labels = [("workchain", shard_id.workchain().to_string())];

        tracing::info!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
            "shard: {}, count: {}", shard_id, count,
        );

        // get previous read_to
        let (prev_read_to_anchor_id, prev_read_to_msgs_offset) = processed_upto_externals
            .as_ref()
            .map(|upto| upto.read_to)
            .unwrap_or_default();

        let was_read_to_opt = current_reader_position.or_else(|| {
            processed_upto_externals
                .as_ref()
                .map(|upto| upto.processed_to)
        });
        let (was_read_to_anchor_id, was_read_to_msgs_offset) = was_read_to_opt.unwrap_or_default();
        tracing::info!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
            "prev_read_to: ({}, {}), read_mode: {:?}, current_reader_position: {:?}, was_read_to: ({}, {})",
            prev_read_to_anchor_id, prev_read_to_msgs_offset,
            read_mode, current_reader_position,
            was_read_to_anchor_id, was_read_to_msgs_offset,
        );

        // when read_from is not defined on the blockchain start
        // then read from the first available anchor
        let mut read_from_anchor_id_opt = None;
        let mut last_read_anchor_id_opt = None;
        let mut msgs_read_offset_in_last_anchor = 0;
        let mut total_msgs_collected = 0;
        let mut ext_messages = vec![];
        let mut has_pending_externals_in_last_read_anchor = false;

        let mut anchors_cache_fully_read = false;
        let next_idx = 0;
        let mut last_read_to_anchor_chain_time = None;
        let mut was_stopped_on_prev_read_to_reached = false;
        loop {
            tracing::debug!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
                "try read next anchor from cache",
            );
            // try read next anchor
            let next_entry = anchors_cache.get(next_idx);
            let (anchor_id, anchor) = match next_entry {
                Some(entry) => entry,
                // stop reading if there is no next anchor
                None => {
                    tracing::debug!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
                        "no next entry in anchors cache",
                    );
                    anchors_cache_fully_read = true;
                    break;
                }
            };

            if anchor_id < was_read_to_anchor_id {
                // skip and remove already read anchor from cache
                assert_eq!(next_idx, 0);
                anchors_cache.remove(next_idx);
                tracing::debug!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
                    "anchor with id {} already read, removed from anchors cache", anchor_id,
                );
                // try read next anchor
                continue;
            }

            if read_from_anchor_id_opt.is_none() {
                tracing::debug!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
                    "read_from_anchor_id: {}", anchor_id,
                );
                read_from_anchor_id_opt = Some(anchor_id);
            }
            last_read_anchor_id_opt = Some(anchor_id);
            last_read_to_anchor_chain_time = Some(anchor.chain_time);
            tracing::debug!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
                "last_read_anchor_id: {}", anchor_id,
            );

            // skip and remove fully read anchor
            if anchor_id == was_read_to_anchor_id
                && anchor.externals.len() == was_read_to_msgs_offset as usize
            {
                assert_eq!(next_idx, 0);
                anchors_cache.remove(next_idx);
                tracing::debug!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
                    "anchor with id {} was fully read before, removed from anchors cache", anchor_id,
                );
                // try read next anchor
                continue;
            }

            // skip expired anchor
            const EXTERNALS_EXPIRE_TIMEOUT: u64 = 60_000; // 1 minute

            if next_chain_time.saturating_sub(anchor.chain_time) > EXTERNALS_EXPIRE_TIMEOUT {
                let iter_from = if anchor_id == was_read_to_anchor_id {
                    was_read_to_msgs_offset as usize
                } else {
                    0
                };
                let iter = anchor.iter_externals(iter_from);
                let mut expired_msgs_count = 0;
                for ext_msg in iter {
                    if shard_id.contains_address(&ext_msg.info.dst) {
                        tracing::trace!(target: tracing_targets::COLLATOR,
                            "ext_msg hash: {}, dst: {} is expired by timeout {} ms",
                            ext_msg.hash(), ext_msg.info.dst, EXTERNALS_EXPIRE_TIMEOUT,
                        );
                        expired_msgs_count += 1;
                    }
                }

                metrics::counter!("tycho_do_collate_ext_msgs_expired_count", &labels)
                    .increment(expired_msgs_count);
                metrics::gauge!("tycho_collator_ext_msgs_imported_queue_size", &labels)
                    .decrement(expired_msgs_count as f64);

                // skip and remove expired anchor
                assert_eq!(next_idx, 0);
                anchors_cache.remove(next_idx);
                tracing::debug!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
                    "anchor with id {} fully skipped due to expiration, removed from anchors cache", anchor_id,
                );

                // try read next anchor
                continue;
            }

            if anchor_id == was_read_to_anchor_id {
                // read first anchor from offset in processed upto
                msgs_read_offset_in_last_anchor = was_read_to_msgs_offset;
            } else {
                // read every next anchor from 0
                msgs_read_offset_in_last_anchor = 0;
            }

            tracing::debug!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
                "next anchor externals count: {}, msgs_read_offset_in_last_anchor: {}",
                anchor.externals.len(), msgs_read_offset_in_last_anchor,
            );

            // possibly previous read_to already reached
            if read_mode == ReadNextExternalsMode::ToPreviuosReadTo
                && anchor_id == prev_read_to_anchor_id
                && msgs_read_offset_in_last_anchor == prev_read_to_msgs_offset
            {
                was_stopped_on_prev_read_to_reached = true;
            }

            // get iterator and read messages
            let mut msgs_collected_from_last_anchor = 0;
            let iter = anchor.iter_externals(msgs_read_offset_in_last_anchor as usize);

            for ext_msg in iter {
                tracing::trace!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
                    "read ext_msg dst: {}", ext_msg.info.dst,
                );

                if total_msgs_collected < count && !was_stopped_on_prev_read_to_reached {
                    msgs_read_offset_in_last_anchor += 1;
                }
                let stop_reading = if shard_id.contains_address(&ext_msg.info.dst) {
                    if total_msgs_collected < count && !was_stopped_on_prev_read_to_reached {
                        // get msgs for target shard until target count reached
                        ext_messages.push(Box::new(ParsedMessage {
                            info: MsgInfo::ExtIn(ext_msg.info.clone()),
                            dst_in_current_shard: true,
                            cell: ext_msg.cell.clone(),
                            special_origin: None,
                            dequeued: None,
                        }));
                        total_msgs_collected += 1;
                        msgs_collected_from_last_anchor += 1;
                        tracing::trace!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
                            "collected ext_msg dst: {}", ext_msg.info.dst,
                        );
                        false
                    } else {
                        // when target msgs count reached
                        // continue looking thru remaining msgs
                        // to detect if exist unread for target shard
                        has_pending_externals_in_last_read_anchor = true;
                        true
                    }
                } else {
                    false
                };

                // if required, stop reading anchors when previous read_to reached
                if read_mode == ReadNextExternalsMode::ToPreviuosReadTo
                    && anchor_id == prev_read_to_anchor_id
                    && msgs_read_offset_in_last_anchor == prev_read_to_msgs_offset
                {
                    was_stopped_on_prev_read_to_reached = true;
                }

                if stop_reading {
                    break;
                }
            }

            metrics::gauge!("tycho_collator_ext_msgs_imported_queue_size", &labels)
                .decrement(msgs_collected_from_last_anchor as f64);

            tracing::debug!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
                "{} externals collected from anchor {}, msgs_read_offset_in_last_anchor: {}",
                msgs_collected_from_last_anchor, anchor_id, msgs_read_offset_in_last_anchor,
            );

            // remove fully read anchor
            if anchor.externals.len() == msgs_read_offset_in_last_anchor as usize {
                assert_eq!(next_idx, 0);
                anchors_cache.remove(next_idx);
                tracing::debug!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
                    "anchor with id {} just fully read, removed from anchors cache", anchor_id,
                );
            }

            if was_stopped_on_prev_read_to_reached {
                tracing::debug!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
                    "stopped reading externals when prev read_to reached: ({}, {})",
                    prev_read_to_anchor_id, prev_read_to_msgs_offset,
                );
                break;
            }

            if total_msgs_collected == count {
                // stop reading anchors when target msgs count reached
                break;
            }
        }

        tracing::info!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
            "total_msgs_collected: {}, target msgs count: {}, has_pending_externals_in_last_read_anchor: {}",
            total_msgs_collected, count, has_pending_externals_in_last_read_anchor,
        );

        // update read up to info
        if last_read_anchor_id_opt.is_none() || anchors_cache_fully_read {
            if let Some((id, all_exts_count)) =
                anchors_cache.get_last_imported_anchor_id_and_all_exts_counts()
            {
                last_read_anchor_id_opt = Some(id);
                msgs_read_offset_in_last_anchor = all_exts_count;
                if read_from_anchor_id_opt.is_none() {
                    read_from_anchor_id_opt = Some(id);
                }
            }
        }
        let current_reader_position = if let Some(last_read_anchor_id) = last_read_anchor_id_opt {
            let current_read_to = (last_read_anchor_id, msgs_read_offset_in_last_anchor);

            // update processed read to only when current position is above
            if let Some(externals_upto) = processed_upto_externals {
                if last_read_anchor_id > prev_read_to_anchor_id
                    || (current_read_to.0 == prev_read_to_anchor_id
                        && current_read_to.1 > prev_read_to_msgs_offset)
                {
                    externals_upto.read_to = current_read_to;
                }
            } else {
                *processed_upto_externals = Some(ExternalsProcessedUpto {
                    processed_to: (read_from_anchor_id_opt.unwrap(), was_read_to_msgs_offset),
                    read_to: current_read_to,
                });
            }

            Some(current_read_to)
        } else {
            None
        };

        // check if we still have pending externals
        let has_pending_externals = if has_pending_externals_in_last_read_anchor {
            true
        } else {
            let has_pending_externals = if last_read_anchor_id_opt.is_some() {
                anchors_cache.len() > 0
            } else {
                anchors_cache.has_pending_externals()
            };

            tracing::info!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
                "remaning anchors in cache has pending externals: {}", has_pending_externals,
            );
            has_pending_externals
        };

        if !has_pending_externals {
            tracing::debug!(target: tracing_targets::COLLATOR, "updated processed_upto.externals = {:?}",
                processed_upto_externals,
            );
        }

        anchors_cache.set_has_pending_externals(has_pending_externals);

        Ok(ParsedExternals {
            ext_messages,
            current_reader_position,
            last_read_to_anchor_chain_time,
            was_stopped_on_prev_read_to_reached,
        })
    }

    /// Get max LT from masterchain (and shardchain) then calc start LT
    fn calc_start_lt(
        mc_data: &McData,
        prev_gen_lt: u64,
        is_masterchain: bool,
        shards_max_end_lt: u64,
    ) -> Result<u64> {
        tracing::trace!(target: tracing_targets::COLLATOR, "calc_start_lt()");

        let mut start_lt = if !is_masterchain {
            std::cmp::max(mc_data.gen_lt, prev_gen_lt)
        } else {
            std::cmp::max(mc_data.gen_lt, shards_max_end_lt)
        };

        let align = mc_data.lt_align();
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
        config: &BlockchainConfig,
        old_global_balance: &CurrencyCollection,
        prev_data_total_balance: CurrencyCollection,
        total_validator_fees: &CurrencyCollection,
        collation_data: &mut BlockCollationData,
    ) -> Result<()> {
        tracing::trace!(target: tracing_targets::COLLATOR, "update_value_flow()");

        if self.shard_id.is_masterchain() {
            collation_data.value_flow.created.tokens = config.get_block_creation_reward(true)?;

            collation_data.value_flow.recovered = collation_data.value_flow.created.clone();
            collation_data
                .value_flow
                .recovered
                .try_add_assign(&collation_data.value_flow.fees_collected)?;
            collation_data
                .value_flow
                .recovered
                .try_add_assign(total_validator_fees)?;

            match config.get_fee_collector_address() {
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

            collation_data.value_flow.minted =
                Self::compute_minted_amount(config, old_global_balance)?;

            if collation_data.value_flow.minted != CurrencyCollection::ZERO
                && config.get_minter_address().is_err()
            {
                tracing::warn!(target: tracing_targets::COLLATOR,
                    "minting of {:?} disabled: no minting smart contract defined",
                    collation_data.value_flow.minted,
                );
                collation_data.value_flow.minted = CurrencyCollection::default();
            }
        } else {
            collation_data.value_flow.created.tokens = config.get_block_creation_reward(false)?;
            collation_data.value_flow.created.tokens >>=
                collation_data.block_id_short.shard.prefix_len() as u8;
        }
        collation_data.value_flow.from_prev_block = prev_data_total_balance;
        Ok(())
    }

    fn compute_minted_amount(
        config: &BlockchainConfig,
        old_global_balance: &CurrencyCollection,
    ) -> Result<CurrencyCollection> {
        tracing::trace!(target: tracing_targets::COLLATOR, "compute_minted_amount");

        let mut to_mint = CurrencyCollection::default();

        let to_mint_cp = match config.get::<ConfigParam7>() {
            Ok(Some(v)) => v,
            _ => {
                tracing::warn!(target: tracing_targets::COLLATOR,
                    "Can't get config param 7 (to_mint)",
                );
                return Ok(to_mint);
            }
        };

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

    fn process_transaction(
        &self,
        executor_output: ExecutorOutput,
        in_message: Option<Box<ParsedMessage>>,
        collation_data: &mut BlockCollationData,
        executor: &MessagesExecutor,
        max_new_message_key_to_current_shard: &mut QueueKey,
        iterator: &mut Box<dyn QueueIterator<EnqueuedMessage>>,
    ) -> Result<()> {
        let new_messages =
            new_transaction(collation_data, &self.shard_id, executor_output, in_message)?;

        collation_data.new_msgs_created += new_messages.len() as u64;
        for new_message in new_messages {
            let MsgInfo::Int(int_msg_info) = new_message.info else {
                continue;
            };

            if new_message.dst_in_current_shard {
                let new_message_key = QueueKey {
                    lt: int_msg_info.created_lt,
                    hash: *new_message.cell.repr_hash(),
                };
                *max_new_message_key_to_current_shard =
                    std::cmp::max(*max_new_message_key_to_current_shard, new_message_key);
            }

            collation_data.inserted_new_msgs_to_iterator += 1;

            let enqueued_message = EnqueuedMessage::from((int_msg_info, new_message.cell));

            self.mq_adapter
                .add_message_to_iterator(iterator, enqueued_message)?;
        }

        collation_data.next_lt = executor.min_next_lt();
        collation_data.block_limit.lt_current = collation_data.next_lt;

        Ok(())
    }

    /// Create special transactions for the collator
    async fn create_special_transactions(
        &self,
        config: &BlockchainConfig,
        collator_data: &mut BlockCollationData,
        executor: &mut MessagesExecutor,
        max_new_message_key_to_current_shard: &mut QueueKey,
        iterator: &mut Box<dyn QueueIterator<EnqueuedMessage>>,
    ) -> Result<()> {
        tracing::trace!(target: tracing_targets::COLLATOR, "create_special_transactions");

        // TODO: Execute in parallel if addresses are distinct?

        if !collator_data.value_flow.recovered.tokens.is_zero() {
            self.create_special_transaction(
                &config.get_fee_collector_address()?,
                collator_data.value_flow.recovered.clone(),
                SpecialOrigin::Recover,
                collator_data,
                executor,
                max_new_message_key_to_current_shard,
                iterator,
            )
            .await?;
        }

        if !collator_data.value_flow.minted.other.is_empty() {
            self.create_special_transaction(
                &config.get_minter_address()?,
                collator_data.value_flow.minted.clone(),
                SpecialOrigin::Mint,
                collator_data,
                executor,
                max_new_message_key_to_current_shard,
                iterator,
            )
            .await?;
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn create_special_transaction(
        &self,
        account_id: &HashBytes,
        amount: CurrencyCollection,
        special_origin: SpecialOrigin,
        collation_data: &mut BlockCollationData,
        executor: &mut MessagesExecutor,
        max_new_message_key_to_current_shard: &mut QueueKey,
        iterator: &mut Box<dyn QueueIterator<EnqueuedMessage>>,
    ) -> Result<()> {
        tracing::trace!(
            target: tracing_targets::COLLATOR,
            account_addr = %account_id,
            amount = %amount.tokens,
            ?special_origin,
            "create_special_transaction",
        );

        let Some(account_stuff) = executor.take_account_stuff_if(account_id, |_| true)? else {
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
                dst_in_current_shard: true,
                cell,
                special_origin: Some(special_origin),
                dequeued: None,
            })
        };

        let executed = executor
            .execute_ordinary_transaction(account_stuff, in_message)
            .await?;

        let executor_output = executed.result?;

        self.process_transaction(
            executor_output,
            Some(executed.in_message),
            collation_data,
            executor,
            max_new_message_key_to_current_shard,
            iterator,
        )?;

        Ok(())
    }

    async fn create_ticktock_transactions(
        &self,
        config: &BlockchainConfig,
        tick_tock: TickTock,
        collation_data: &mut BlockCollationData,
        executor: &mut MessagesExecutor,
        max_new_message_key_to_current_shard: &mut QueueKey,
        iterator: &mut Box<dyn QueueIterator<EnqueuedMessage>>,
    ) -> Result<()> {
        tracing::trace!(
            target: tracing_targets::COLLATOR,
            kind = ?tick_tock,
            "create_ticktock_transactions"
        );

        // TODO: Execute in parallel since these are unique accounts

        for account_id in config.get_fundamental_addresses()?.keys() {
            self.create_ticktock_transaction(
                &account_id?,
                tick_tock,
                collation_data,
                executor,
                max_new_message_key_to_current_shard,
                iterator,
            )
            .await?;
        }

        self.create_ticktock_transaction(
            &config.address,
            tick_tock,
            collation_data,
            executor,
            max_new_message_key_to_current_shard,
            iterator,
        )
        .await?;
        Ok(())
    }

    async fn create_ticktock_transaction(
        &self,
        account_id: &HashBytes,
        tick_tock: TickTock,
        collation_data: &mut BlockCollationData,
        executor: &mut MessagesExecutor,
        max_new_message_key_to_current_shard: &mut QueueKey,
        iterator: &mut Box<dyn QueueIterator<EnqueuedMessage>>,
    ) -> Result<()> {
        tracing::trace!(
            target: tracing_targets::COLLATOR,
            account_addr = %account_id,
            kind = ?tick_tock,
            "create_ticktock_transaction",
        );

        let Some(account_stuff) =
            executor.take_account_stuff_if(account_id, |stuff| match tick_tock {
                TickTock::Tick => stuff.special.tick,
                TickTock::Tock => stuff.special.tock,
            })?
        else {
            return Ok(());
        };

        let executor_output = executor
            .execute_ticktock_transaction(account_stuff, tick_tock)
            .await?;

        self.process_transaction(
            executor_output,
            None,
            collation_data,
            executor,
            max_new_message_key_to_current_shard,
            iterator,
        )?;

        Ok(())
    }

    fn import_new_shard_top_blocks_for_masterchain(
        config: &BlockchainConfig,
        collation_data_builder: &mut BlockCollationDataBuilder,
        top_shard_blocks_info: Vec<TopBlockDescription>,
    ) -> Result<()> {
        // TODO: consider split/merge logic

        tracing::trace!(target: tracing_targets::COLLATOR,
            "import_new_shard_top_blocks_for_masterchain()",
        );

        // convert to map for merging
        let top_shard_blocks_info_map = top_shard_blocks_info
            .into_iter()
            .map(|info| (info.block_id.shard, info))
            .collect::<FastHashMap<_, _>>();

        // update existing shard descriptions for which top blocks were not changed
        for (shard_id, prev_shard_descr) in collation_data_builder.shards_mut()? {
            if !top_shard_blocks_info_map.contains_key(shard_id) {
                prev_shard_descr.top_sc_block_updated = false;
            }
        }

        // update other shard descriptions from top blocks
        for (shard_id, top_block_descr) in top_shard_blocks_info_map {
            let TopBlockDescription {
                block_id,
                block_info,
                processed_to_anchor_id,
                value_flow,
                proof_funds,
                #[cfg(feature = "block-creator-stats")]
                creators,
            } = top_block_descr;

            let mut new_shard_descr = Box::new(ShardDescription::from_block_info(
                block_id,
                &block_info,
                processed_to_anchor_id,
                &value_flow,
            ));
            new_shard_descr.reg_mc_seqno = collation_data_builder.block_id_short.seqno;

            // run checks
            if new_shard_descr.gen_utime > collation_data_builder.gen_utime {
                tracing::warn!(target: tracing_targets::COLLATOR,
                    "ShardTopBlockDescr for {} skipped: it generated at {}, but master block should be generated at {}",
                    shard_id, new_shard_descr.gen_utime, collation_data_builder.gen_utime,
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

            // update shards and collation data
            collation_data_builder.update_shards_max_end_lt(new_shard_descr.end_lt);

            let top_sc_block_updated = match collation_data_builder.shards_mut()?.entry(shard_id) {
                hash_map::Entry::Vacant(entry) => {
                    // if shard was not present before consider top shard block was changed
                    let top_sc_block_updated = true;
                    new_shard_descr.top_sc_block_updated = top_sc_block_updated;
                    entry.insert(new_shard_descr);
                    top_sc_block_updated
                }
                hash_map::Entry::Occupied(mut entry) => {
                    // set flag if top shard block seqno changed
                    let prev_shard_descr = entry.get();
                    let top_sc_block_updated = prev_shard_descr.seqno != new_shard_descr.seqno;
                    new_shard_descr.top_sc_block_updated = top_sc_block_updated;
                    entry.insert(new_shard_descr);
                    top_sc_block_updated
                }
            };

            // we accumulate shard fees only when top shard block changed
            if top_sc_block_updated {
                collation_data_builder.store_shard_fees(shard_id, proof_funds)?;
            }

            collation_data_builder.top_shard_blocks_ids.push(block_id);
            #[cfg(feature = "block-creator-stats")]
            collation_data_builder.register_shard_block_creators(creators)?;
        }

        let shard_fees = collation_data_builder.shard_fees.root_extra().clone();

        collation_data_builder
            .value_flow
            .fees_collected
            .try_add_assign(&shard_fees.fees)?;
        collation_data_builder.value_flow.fees_imported = shard_fees.fees;

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

        self.stats.tps_block += 1;
        self.stats.tps_execute_count += collation_data.execute_count_all;
        if self.stats.tps_block == 10 {
            if let Some(timer) = self.stats.tps_timer {
                let elapsed = timer.elapsed();
                self.stats.tps = (self.stats.tps_execute_count as u128 * 1000 * 1000)
                    .checked_div(elapsed.as_micros())
                    .unwrap_or_default();
            }
            self.stats.tps_timer = Some(std::time::Instant::now());
            self.stats.tps_block = 0;
            self.stats.tps_execute_count = 0;
        }
    }
}

/// add in and out messages from to block
#[allow(clippy::vec_box)]
fn new_transaction(
    collation_data: &mut BlockCollationData,
    shard_id: &ShardIdent,
    executor_output: ExecutorOutput,
    in_msg: Option<Box<ParsedMessage>>,
) -> Result<Vec<Box<ParsedMessage>>> {
    tracing::trace!(
        target: tracing_targets::COLLATOR,
        message_hash = ?in_msg.as_ref().map(|m| m.cell.repr_hash()),
        transaction_hash = %executor_output.transaction.inner().repr_hash(),
        "process new transaction from message",
    );

    collation_data.execute_count_all += 1;

    let gas_used = &mut collation_data.block_limit.gas_used;
    *gas_used = gas_used.saturating_add(executor_output.gas_used);

    if let Some(in_msg) = in_msg {
        process_in_message(collation_data, executor_output.transaction.clone(), in_msg)?;
    }

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
                let dst_in_current_shard = contains_prefix(shard_id, dst_workchain, dst_prefix);

                let out_msg = OutMsg::New(OutMsgNew {
                    out_msg_envelope: Lazy::new(&MsgEnvelope {
                        cur_addr: IntermediateAddr::FULL_SRC_SAME_WORKCHAIN,
                        // NOTE: `next_addr` is not used in current routing between shards logic
                        next_addr: if dst_in_current_shard {
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
                    dst_in_current_shard,
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

fn process_in_message(
    collation_data: &mut BlockCollationData,
    transaction: Lazy<Transaction>,
    in_msg: Box<ParsedMessage>,
) -> Result<()> {
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
                transaction,
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
                transaction,
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
                transaction,
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
                transaction,
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

    Ok(())
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

fn calc_prepare_groups_wu_total(
    collation_data: &BlockCollationData,
    wu_params_prepare: &WorkUnitsParamsPrepare,
) -> u64 {
    let &WorkUnitsParamsPrepare {
        fixed_part,
        read_ext_msgs,
        read_int_msgs,
        read_new_msgs,
    } = wu_params_prepare;

    (fixed_part as u64)
        .saturating_add(
            collation_data
                .read_ext_msgs
                .saturating_mul(read_ext_msgs as u64),
        )
        .saturating_add(
            collation_data
                .read_int_msgs_from_iterator
                .saturating_mul(read_int_msgs as u64),
        )
        .saturating_add(
            collation_data
                .read_new_msgs_from_iterator
                .saturating_mul(read_new_msgs as u64),
        )
}

fn calc_process_txs_wu(
    collation_data: &BlockCollationData,
    wu_params_execute: &WorkUnitsParamsExecute,
) -> u64 {
    let &WorkUnitsParamsExecute {
        serialize_enqueue,
        serialize_dequeue,
        insert_new_msgs_to_iterator,
        ..
    } = wu_params_execute;

    (collation_data.int_enqueue_count)
        .saturating_mul(serialize_enqueue as u64)
        .saturating_add((collation_data.int_dequeue_count).saturating_mul(serialize_dequeue as u64))
        .saturating_add(
            (collation_data.inserted_new_msgs_to_iterator)
                .saturating_mul(insert_new_msgs_to_iterator as u64),
        )
}
