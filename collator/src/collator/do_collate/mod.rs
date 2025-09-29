use std::collections::hash_map;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use humantime::format_duration;
use phase::{ActualState, Phase};
use prepare::PrepareState;
use tycho_block_util::config::{apply_price_factor, compute_gas_price_factor};
use tycho_block_util::queue::{QueueDiffStuff, QueueKey, SerializedQueueDiff};
use tycho_block_util::state::RefMcStateHandle;
use tycho_core::global_config::ZerostateId;
use tycho_core::storage::{NewBlockMeta, StoreStateHint};
use tycho_types::models::*;
use tycho_types::num::Tokens;
use tycho_types::prelude::*;
use tycho_util::FastHashMap;
use tycho_util::futures::JoinTask;
use tycho_util::mem::Reclaimer;
use tycho_util::metrics::HistogramGuard;
use tycho_util::sync::CancellationFlag;
use tycho_util::time::now_millis;

use super::types::{
    AnchorInfo, AnchorsCache, BlockCollationData, BlockCollationDataBuilder, BlockSerializerCache,
    CollationResult, ExecuteResult, FinalResult, FinalizeBlockResult, FinalizeCollationResult,
    FinalizeMessagesReaderResult, PrevData, WorkingState,
};
use super::{CollatorStdImpl, ForceMasterCollation, ShardDescriptionExt};
use crate::collator::do_collate::finalize::FinalizeBlockContext;
use crate::collator::do_collate::work_units::{DoCollateWu, WuEvent, WuEventData};
use crate::collator::error::{CollationCancelReason, CollatorError};
use crate::collator::messages_reader::state::ReaderState;
use crate::collator::types::{FinalizeMetrics, PartialValueFlow, RandSeed};
use crate::internal_queue::types::diff::{DiffZone, QueueDiffWithMessages};
use crate::internal_queue::types::message::EnqueuedMessage;
use crate::internal_queue::types::ranges::{Bound, QueueShardBoundedRange};
use crate::internal_queue::types::stats::DiffStatistics;
use crate::queue_adapter::MessageQueueAdapter;
use crate::tracing_targets;
use crate::types::processed_upto::{
    build_all_shards_processed_to_by_partitions, find_min_processed_to_by_shards,
};
use crate::types::{
    BlockCollationResult, BlockIdExt, CollationSessionInfo, CollatorConfig,
    DisplayBlockIdsIntoIter, DisplayBlockIdsIter, McData, ProcessedTo, ShardDescriptionShort,
    ShardDescriptionShortExt, ShardPair, TopBlockDescription, TopBlockId, TopShardBlockInfo,
};

#[cfg(test)]
#[path = "../tests/do_collate_tests.rs"]
pub(super) mod tests;

mod execute;
mod execution_wrapper;
mod finalize;
mod phase;
mod prepare;
pub mod work_units;

struct CollationOutput {
    reader_state: ReaderState,
    anchors_cache: AnchorsCache,
    result: Result<CollationResult, CollatorError>,
}

pub struct FinalizeCollationCtx {
    /// Do we have unprocessed messages in internals
    /// or externals queue after block collation
    pub has_unprocessed_messages: bool,
    pub finalized: FinalizeBlockResult,
    pub reader_state: ReaderState,
    pub ref_mc_state_handle: RefMcStateHandle,
    pub force_next_mc_block: ForceMasterCollation,
    pub resume_collation_elapsed: Duration,
}

impl CollatorStdImpl {
    /// [`force_next_mc_block`] - should force next master block collation after this block
    #[tracing::instrument(
        parent =  None,
        skip_all,
        fields(
            block_id = %self.next_block_info,
            ct = next_chain_time,
        )
    )]
    pub(super) async fn do_collate(
        &mut self,
        working_state: Box<WorkingState>,
        top_shard_blocks_info: Option<Vec<TopBlockDescription>>,
        next_chain_time: u64,
        force_next_mc_block: ForceMasterCollation,
    ) -> Result<()> {
        let labels: [(&str, String); 1] = [("workchain", self.shard_id.workchain().to_string())];
        let total_collation_histogram =
            HistogramGuard::begin_with_labels("tycho_do_collate_total_time_high", &labels);

        let WorkingState {
            next_block_id_short,
            mc_data,
            collation_config,
            wu_used_from_last_anchor,
            resume_collation_elapsed,
            prev_shard_data,
            usage_tree,
            mut reader_state,
            ..
        } = *working_state;

        let mc_block_id = mc_data.block_id;
        let prev_shard_data = prev_shard_data.unwrap();
        let usage_tree = usage_tree.unwrap();
        let ref_mc_state_handle = prev_shard_data.ref_mc_state_handle().clone();

        tracing::info!(target: tracing_targets::COLLATOR,
            "Start collating block: mc_data_block_id={}, prev_block_ids={}, top_shard_blocks_ids: {:?}",
            mc_data.block_id.as_short_id(),
            DisplayBlockIdsIntoIter(prev_shard_data.blocks_ids()),
            top_shard_blocks_info.as_ref().map(|v| DisplayBlockIdsIter(
                v.iter().map(|i| &i.block_id)
            )),
        );

        // We should remove all previously imported anchors above next chain time.
        // We have cases when some shard can force master block collation (e.g. no pending messages after sc block)
        // but it is not clear how many anchors were already imported by master. So we take next chain time
        // from shard and should use anchors only up to chosen next chain time.
        let Some(AnchorInfo {
            ct: last_imported_chain_time,
            author,
            ..
        }) = self
            .anchors_cache
            .remove_last_imported_above(next_chain_time)
        else {
            bail!(
                "last_imported_anchor should exist when we collating block \
                    even after removing anchors above the next chain time"
            )
        };

        // TODO: needs to update metrics
        // metrics::gauge!("tycho_collator_ext_msgs_imported_queue_size", &labels).decrement(our_exts_count as f64);

        assert!(
            *last_imported_chain_time >= next_chain_time,
            "all anchors upto next chain time {} should be imported before collation, \
            but last imported chain time is {}",
            next_chain_time,
            last_imported_chain_time,
        );

        let created_by = author.to_bytes().into();

        let is_first_block_after_prev_master = is_first_block_after_prev_master(
            prev_shard_data.blocks_ids()[0], // TODO: consider split/merge
            &mc_data.shards,
        );
        let part_stat_ranges = if is_first_block_after_prev_master
            && mc_data.block_id.seqno > self.zerostate_id.seqno
        {
            if next_block_id_short.is_masterchain() {
                self.mc_compute_part_stat_ranges(&mc_data, top_shard_blocks_info.clone().unwrap())
                    .await?
            } else {
                self.compute_part_stat_ranges(&mc_data, &next_block_id_short)
                    .await?
            }
        } else {
            None
        };

        let collation_data = self.create_collation_data(
            next_block_id_short,
            next_chain_time,
            created_by,
            &mc_data,
            &prev_shard_data,
            top_shard_blocks_info,
        )?;

        let mut anchors_cache = std::mem::take(&mut self.anchors_cache);
        let block_serializer_cache = self.block_serializer_cache.clone();

        let state = Box::new(ActualState {
            collation_config: collation_config.clone(),
            collation_data,
            mc_data,
            prev_shard_data,
            shard_id: self.shard_id,
            collation_is_cancelled: CancellationFlag::new(),
            is_first_block_after_prev_master,
            part_stat_ranges,
            do_collate_wu: DoCollateWu {
                resume_collation_elapsed,
                ..Default::default()
            },
        });
        let collation_is_cancelled = state.collation_is_cancelled.clone();

        let zerostate_id = self.zerostate_id;

        let do_collate_fut = tycho_util::sync::rayon_run_fifo({
            let collation_session = self.collation_session.clone();
            let config = self.config.clone();
            let mq_adapter = self.mq_adapter.clone();
            let span = tracing::Span::current();
            move || {
                let _span = span.enter();

                let result = Self::run(
                    config,
                    mq_adapter,
                    &mut reader_state,
                    &mut anchors_cache,
                    block_serializer_cache,
                    state,
                    collation_session,
                    wu_used_from_last_anchor,
                    usage_tree,
                    zerostate_id,
                );

                CollationOutput {
                    reader_state,
                    anchors_cache,
                    result,
                }
            }
        });

        let collation_cancelled = self.cancel_collation.notified();

        let mut do_collate_res = None;
        tokio::select! {
            res = do_collate_fut => {
                do_collate_res =  Some(res);
            },
            _ = async move {
                collation_cancelled.await;
                tracing::info!(target: tracing_targets::COLLATOR,
                    "collation was cancelled by manager on do_collate",
                );
                collation_is_cancelled.cancel();
                std::future::pending::<()>().await;
            } => {}
        }
        let CollationOutput {
            reader_state,
            anchors_cache: restored_anchors_cache,
            result: do_collate_res,
        } = do_collate_res.unwrap();

        self.anchors_cache = restored_anchors_cache;

        let CollationResult {
            finalized,
            execute_result,
            final_result,
        } = match do_collate_res {
            Err(CollatorError::Cancelled(reason)) => {
                // cancel collation
                self.listener
                    .on_cancelled(mc_block_id, next_block_id_short, reason)
                    .await?;
                return Ok(());
            }
            res => res?,
        };

        let int_queue_len = reader_state
            .internals
            .cumulative_statistics
            .as_ref()
            .map(|cumulative_stats| cumulative_stats.remaining_total_for_own_shard());

        let last_read_to_anchor_chain_time = reader_state.externals.last_read_to_anchor_chain_time;

        let block_id = *finalized.block_candidate.block.id();
        let finalize_wu = finalized.finalize_wu.clone();
        let mut do_collate_wu = finalized.do_collate_wu.clone();

        // metrics and logs before collation finalize
        self.report_collation_metrics(&finalized.collation_data);

        self.update_stats(&finalized.collation_data);

        self.log_block_and_stats(&execute_result, &finalized, &final_result);

        // finalize collation
        let FinalizeCollationResult {
            handle_block_candidate_elapsed,
        } = self
            .finalize_collation(FinalizeCollationCtx {
                has_unprocessed_messages: final_result.has_unprocessed_messages,
                finalized,
                reader_state,
                ref_mc_state_handle,
                force_next_mc_block,
                resume_collation_elapsed,
            })
            .await?;

        let collation_total_elapsed = total_collation_histogram.finish();

        do_collate_wu.collation_total_elapsed = collation_total_elapsed;

        // send wu metrics to the tuner or just report them
        let wu_metrics = work_units::WuMetrics {
            wu_params: collation_config.work_units_params.clone(),
            wu_on_prepare_msg_groups: execute_result.prepare_msg_groups_wu,
            wu_on_execute: execute_result.execute_wu,
            wu_on_finalize: finalize_wu,
            wu_on_do_collate: do_collate_wu,
            has_pending_messages: final_result.has_unprocessed_messages,
        };
        if let Some(wu_tuner_event_sender) = &self.wu_tuner_event_sender {
            if let Err(err) = wu_tuner_event_sender
                .send(WuEvent {
                    shard: block_id.shard,
                    seqno: block_id.seqno,
                    data: WuEventData::Metrics(Box::new(wu_metrics)),
                })
                .await
            {
                tracing::warn!(target: tracing_targets::COLLATOR,
                    ?err,
                    "error sending wu metrics to the tuner service",
                );
            }
        } else {
            wu_metrics.report_metrics(&block_id.shard);
        }

        // final metrics and logs
        metrics::counter!("tycho_do_collate_blocks_count", &labels).increment(1);

        if let Some(int_queue_len) = int_queue_len {
            metrics::gauge!("tycho_do_collate_int_msgs_queue_by_stat", &labels)
                .set(int_queue_len as f64);
        }

        // time elapsed from prev block
        let elapsed_from_prev_block = self.timer.elapsed();
        let collation_mngmnt_overhead = elapsed_from_prev_block - collation_total_elapsed;
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
        let diff_time =
            now_millis() as i64 - last_read_to_anchor_chain_time.unwrap_or(next_chain_time) as i64;
        metrics::gauge!("tycho_do_collate_ext_msgs_time_diff", &labels)
            .set(diff_time as f64 / 1000.0);

        Self::log_final_timings(
            collation_total_elapsed,
            block_id,
            block_time_diff,
            collation_mngmnt_overhead,
            elapsed_from_prev_block,
            handle_block_candidate_elapsed,
        );

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn run(
        collator_config: Arc<CollatorConfig>,
        mq_adapter: Arc<dyn MessageQueueAdapter<EnqueuedMessage>>,
        reader_state: &mut ReaderState,
        anchors_cache: &mut AnchorsCache,
        block_serializer_cache: BlockSerializerCache,
        state: Box<ActualState>,
        collation_session: Arc<CollationSessionInfo>,
        wu_used_from_last_anchor: u64,
        usage_tree: UsageTree,
        zerostate_id: ZerostateId,
    ) -> Result<CollationResult, CollatorError> {
        let shard_id = state.shard_id;
        let labels = [("workchain", shard_id.workchain().to_string())];
        let mc_data = state.mc_data.clone();

        let collation_is_cancelled = state.collation_is_cancelled.clone();

        // prepare execution
        let histogram_prepare =
            HistogramGuard::begin_with_labels("tycho_do_collate_prepare_time_high", &labels);

        let shards_processed_to_by_partitions = state
            .collation_data
            .mc_shards_processed_to_by_partitions
            .clone();

        let block_id_short = state.collation_data.block_id_short;
        let prev_diff_hash = state
            .prev_shard_data
            .prev_queue_diff_hashes()
            .first()
            .cloned()
            .unwrap_or_default();

        let prepare_phase =
            Phase::<PrepareState<'_>>::new(mq_adapter.clone(), reader_state, anchors_cache, state);

        let mut execute_phase = prepare_phase.run()?;

        let prepare_elapsed = histogram_prepare.finish();

        // execute tick transaction and special transactions (mint, recover)
        if shard_id.is_masterchain() {
            execute_phase.execute_tick_transactions()?;
            execute_phase.execute_special_transactions()?;
        }

        // execute incoming messages
        execute_phase.execute_incoming_messages()?;

        // execute tock transaction
        if shard_id.is_masterchain() {
            execute_phase.execute_tock_transactions()?;
        }

        let (mut finalize_phase, messages_reader) = execute_phase.finish();

        // finalize
        finalize_phase.extra.finalize_metrics.total_timer.start();

        // finalize messages reader
        let FinalizeMessagesReaderResult {
            queue_diff_with_msgs,
            has_unprocessed_messages,
            current_msgs_exec_params,
        } = finalize_phase.finalize_messages_reader(
            messages_reader,
            mq_adapter.clone(),
            zerostate_id,
        )?;

        let histogram_create_queue_diff = HistogramGuard::begin_with_labels(
            "tycho_do_collate_create_queue_diff_time_high",
            &labels,
        );

        let (min_message, max_message) = {
            let messages = &queue_diff_with_msgs.messages;
            match messages.first_key_value().zip(messages.last_key_value()) {
                Some(((min, _), (max, _))) => (*min, *max),
                None => (
                    QueueKey::min_for_lt(finalize_phase.state.collation_data.start_lt),
                    QueueKey::max_for_lt(finalize_phase.state.collation_data.next_lt),
                ),
            }
        };

        histogram_create_queue_diff.finish();

        let histogram_serialize_queue_diff = HistogramGuard::begin_with_labels(
            "tycho_do_collate_serialize_queue_diff_time_high",
            &labels,
        );
        let serialized_diff = serialize_diff(
            &queue_diff_with_msgs,
            &min_message,
            &max_message,
            &block_id_short,
            &prev_diff_hash,
            reader_state.internals.get_min_processed_to_by_shards(),
        );

        histogram_serialize_queue_diff.finish();

        let update_queue_task = create_apply_diff_task(
            &mq_adapter,
            queue_diff_with_msgs,
            &block_id_short,
            min_message,
            max_message,
            *serialized_diff.hash(),
        )?;

        let mut processed_upto = reader_state.get_updated_processed_upto();

        // report actual ranges count to metrics
        for (par_id, par) in &processed_upto.partitions {
            let labels = [
                ("workchain", shard_id.workchain().to_string()),
                ("par_id", par_id.to_string()),
            ];
            metrics::gauge!("tycho_do_collate_processed_upto_ext_ranges", &labels)
                .set(par.externals.ranges.len() as f64);
            metrics::gauge!("tycho_do_collate_processed_upto_int_ranges", &labels)
                .set(par.internals.ranges.len() as f64);
        }

        processed_upto.msgs_exec_params = Some(current_msgs_exec_params);

        let internal_processed_upto_by_partitions =
            processed_upto.get_internals_processed_to_by_partitions();

        // Use unified helper methods for min_processed_to calculation
        let all_shards_processed_to = build_all_shards_processed_to_by_partitions(
            block_id_short,
            internal_processed_upto_by_partitions,
            mc_data
                .processed_upto
                .get_internals_processed_to_by_partitions(),
            shards_processed_to_by_partitions.clone(),
            &mc_data.shards,
        );

        let min_processed_to_by_shards = find_min_processed_to_by_shards(&all_shards_processed_to);
        let min_processed_to = min_processed_to_by_shards.get(&shard_id).cloned();

        // exit collation if cancelled
        if collation_is_cancelled.check() {
            return Err(CollatorError::Cancelled(
                CollationCancelReason::ExternalCancel,
            ));
        }

        let diff_tail_len =
            mq_adapter.get_diffs_tail_len(&shard_id, &min_processed_to.unwrap_or_default()) + 1;

        // finalize block
        let span = tracing::Span::current();
        let (finalize_phase_result, update_queue_task_result) = rayon::join(
            || {
                let _span = span.enter();

                finalize_phase.finalize_block(FinalizeBlockContext {
                    collation_session,
                    wu_used_from_last_anchor,
                    usage_tree,
                    serialized_diff,
                    collator_config,
                    processed_upto,
                    diff_tail_len,
                    block_serializer_cache,
                    zerostate_id,
                })
            },
            // run update queue task and wait before returning collation result
            // to be sure that queue was updated before block commit and next block collation
            update_queue_task,
        );
        let (mut finalized, execute_result) = finalize_phase_result?;

        // update finalize metrics and wu
        finalized.finalize_metrics.apply_queue_diff_elapsed = update_queue_task_result?;
        finalized.finalize_metrics.total_timer.stop();
        finalized
            .finalize_wu
            .append_elapsed_timings(&finalized.finalize_metrics);

        Self::log_finalize_timings(&finalized.finalize_metrics);

        assert_eq!(
            finalized.collation_data.int_enqueue_count,
            finalized.collation_data.inserted_new_msgs_count
                - finalized.collation_data.execute_count_new_int
        );

        let final_result = FinalResult {
            prepare_elapsed,
            has_unprocessed_messages,
        };

        Ok(CollationResult {
            finalized,
            execute_result,
            final_result,
        })
    }

    /// Get max LT from masterchain (and shardchain) then calc start LT
    fn calc_start_lt(
        mc_data_gen_lt: u64,
        lt_align: u64,
        prev_gen_lt: u64,
        is_masterchain: bool,
        shards_max_end_lt: u64,
    ) -> Result<u64> {
        tracing::trace!(target: tracing_targets::COLLATOR, "calc_start_lt");

        let mut start_lt = if !is_masterchain {
            std::cmp::max(mc_data_gen_lt, prev_gen_lt)
        } else {
            std::cmp::max(mc_data_gen_lt, shards_max_end_lt)
        };

        let incr = lt_align - start_lt % lt_align;
        if incr < lt_align || 0 == start_lt {
            if start_lt >= (!incr + 1) {
                bail!("cannot compute start logical time (uint64 overflow)");
            }
            start_lt += incr;
        }

        tracing::debug!(target: tracing_targets::COLLATOR, "start_lt set to {}", start_lt);

        Ok(start_lt)
    }

    fn init_value_flow(
        config: &BlockchainConfig,
        old_global_balance: &CurrencyCollection,
        prev_total_balance: &CurrencyCollection,
        total_validator_fees: &CurrencyCollection,
        collation_data: &mut BlockCollationData,
    ) -> Result<()> {
        tracing::trace!(target: tracing_targets::COLLATOR, "init_value_flow");

        let mut value_flow = PartialValueFlow::default();

        let block_id = collation_data.block_id_short;

        if block_id.is_masterchain() {
            // Burn a fraction of shard fees (except the created part).
            let shard = collation_data.shard_fees.root_extra();
            let simple_shard_fees = shard.fees.checked_sub(&shard.create)?.tokens;
            let burned = config
                .get_burning_config()
                .unwrap_or_default()
                .compute_burned_fees(simple_shard_fees)?;
            value_flow.burned = burned.into();

            // Collect shard fees.
            value_flow.fees_collected = shard.fees.clone();
            value_flow.fees_collected.try_sub_assign_tokens(burned)?;

            // Create block reward.
            let created = config.get_block_creation_reward(true)?;
            value_flow.created = created.into();

            // Try to recover accumulated fees.
            value_flow.recovered =
                Self::compute_recovered_amount(config, created, &shard.fees, total_validator_fees)?;

            // Mint new tokens based on the config.
            value_flow.minted =
                Self::compute_minted_amount(block_id.seqno, config, old_global_balance)?;
        } else {
            // Create block reward.
            let mut created = config.get_block_creation_reward(false)?;
            created >>= block_id.shard.prefix_len() as u8;
            value_flow.created = created.into();
        }

        // Save previous shard balance.
        value_flow.from_prev_block = prev_total_balance.clone();

        // Collect block reward.
        value_flow
            .fees_collected
            .try_add_assign(&value_flow.created)?;

        // Apply value flow.
        collation_data.value_flow = value_flow;
        Ok(())
    }

    fn compute_recovered_amount(
        config: &BlockchainConfig,
        created: Tokens,
        shard_fees: &CurrencyCollection,
        total_validator_fees: &CurrencyCollection,
    ) -> Result<CurrencyCollection> {
        const RECOVERY_BASE_THRESHOLD: u128 = 1_000_000_000;

        let mut recovered = total_validator_fees.clone();
        recovered.try_add_assign(shard_fees)?;
        recovered.try_add_assign_tokens(created)?;

        match config.get_fee_collector_address() {
            Ok(_addr) => {
                let gas_price = config.get_gas_prices(true)?.gas_price;
                let gas_price_factor = compute_gas_price_factor(true, gas_price)?;
                let threshold = apply_price_factor(RECOVERY_BASE_THRESHOLD, gas_price_factor);

                if recovered.tokens.into_inner() < threshold {
                    tracing::debug!(target: tracing_targets::COLLATOR,
                        "fee recovery skipped ({recovered:?})",
                    );
                    recovered = CurrencyCollection::ZERO;
                }
            }
            Err(_) => {
                tracing::debug!(target: tracing_targets::COLLATOR,
                    "fee recovery disabled (no collector smart contract defined in configuration)",
                );
                recovered = CurrencyCollection::ZERO;
            }
        };

        Ok(recovered)
    }

    fn compute_minted_amount(
        mc_seqno: u32,
        config: &BlockchainConfig,
        global_balance: &CurrencyCollection,
    ) -> Result<CurrencyCollection> {
        tracing::trace!(target: tracing_targets::COLLATOR, "compute_minted_amount");

        let mut minted = CurrencyCollection::ZERO;

        // Mint using config param 7 (if any).
        if let Some(target_ec) = config.get::<ConfigParam7>()? {
            let global_ec = global_balance.other.as_dict();

            for item in target_ec.as_dict().iter() {
                let (id, target) = item?;
                let global = global_ec.get(id)?.unwrap_or_default();

                if let Some(delta) = target.checked_sub(&global) {
                    tracing::debug!(target: tracing_targets::COLLATOR,
                        "Currency #{id}: existing={global}, required={target}, \
                        delta={delta}",
                    );

                    if id != 0 && !delta.is_zero() {
                        minted.other.as_dict_mut().set(id, delta)?;
                    }
                }
            }
        } else {
            tracing::debug!(target: tracing_targets::COLLATOR,
                "Mint is disabled (config param 7 is missing)",
            );
        }

        // One-time mint.
        if let Some(one_time) = config.get::<ConfigParam50>()? {
            // TODO: Add check for some offset before `mint_at` to give some time
            //       to react on config change?

            if one_time.mint_at == mc_seqno {
                if let Ok(new_minted) = minted.checked_add(&one_time.delta) {
                    minted = new_minted;
                } else {
                    tracing::warn!(target: tracing_targets::COLLATOR,
                        "Skipping one-time mint because of overflow, \
                        existing={global_balance:?}, delta={:?}",
                        one_time.delta,
                    );
                }
            }
        }

        // Done
        Ok(minted)
    }

    fn import_new_shard_top_blocks_for_masterchain(
        mc_data: &McData,
        collation_data_builder: &mut BlockCollationDataBuilder,
        top_shard_blocks_info: Vec<TopBlockDescription>,
    ) -> Result<()> {
        // TODO: consider split/merge logic

        tracing::trace!(target: tracing_targets::COLLATOR,
            "import_new_shard_top_blocks_for_masterchain",
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
                processed_to_by_partitions,
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
                bail!(
                    "Error updating master top shards from TopBlockDescription {}: \
                    it generated at {}, but master block should be generated at {}",
                    block_id.as_short_id(),
                    new_shard_descr.gen_utime,
                    collation_data_builder.gen_utime,
                )
            }
            if mc_data
                .config
                .get_global_version()?
                .capabilities
                .contains(GlobalCapability::CapWorkchains)
            {
                // NOTE: shard_descr proof_chain is used for transactions between workchains in TON
                // we skip it for now and will rework mechanism in the future
                // shard_descr.proof_chain = Some(sh_bd.top_block_descr().chain().clone());
            }

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

            collation_data_builder
                .top_shard_blocks
                .push(TopShardBlockInfo {
                    block_id,
                    processed_to_by_partitions,
                });

            #[cfg(feature = "block-creator-stats")]
            collation_data_builder.register_shard_block_creators(creators)?;
        }

        // filling mc_processed_to_by_partitions
        let mut shards_processed_to_by_partitions = FastHashMap::default();
        for (shard_id, shard_descr) in collation_data_builder.shards()?.iter() {
            // get from updated shard
            let shard_processed_to_by_partitions = collation_data_builder
                .top_shard_blocks
                .iter()
                .find(|top_block_info| top_block_info.block_id.shard == *shard_id)
                .map(|top_block_info| top_block_info.processed_to_by_partitions.clone())
                .or_else(|| {
                    // or get the previous value
                    mc_data
                        .shards_processed_to_by_partitions
                        .get(shard_id)
                        .map(|(_, value)| value)
                        .cloned()
                });

            if let Some(value) = shard_processed_to_by_partitions {
                shards_processed_to_by_partitions
                    .insert(*shard_id, (shard_descr.top_sc_block_updated, value));
            }
        }
        collation_data_builder.mc_shards_processed_to_by_partitions =
            shards_processed_to_by_partitions;

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

    fn create_collation_data(
        &mut self,
        next_block_id_short: BlockIdShort,
        next_chain_time: u64,
        created_by: HashBytes,
        mc_data: &McData,
        prev_shard_data: &PrevData,
        top_shard_blocks_info: Option<Vec<TopBlockDescription>>,
    ) -> Result<Box<BlockCollationData>> {
        // need to generate unique for each block
        // generate seed from the chain_time from the anchor
        let rand_seed = HashBytes(tl_proto::hash(RandSeed {
            shard: next_block_id_short.shard,
            seqno: next_block_id_short.seqno,
            next_chain_time,
        }));
        tracing::trace!(target: tracing_targets::COLLATOR, "rand_seed from chain time: {}", rand_seed);

        let is_masterchain = self.shard_id.is_masterchain();

        if !is_masterchain {
            self.shard_blocks_count_from_last_anchor =
                self.shard_blocks_count_from_last_anchor.saturating_add(1);
        }

        // prepare block collation data
        let block_limits = mc_data.config.get_block_limits(is_masterchain)?;
        tracing::debug!(target: tracing_targets::COLLATOR,
            "Block limits: {:?}",
            block_limits
        );

        let mut collation_data_builder = BlockCollationDataBuilder::new(
            next_block_id_short,
            rand_seed,
            mc_data.block_id.seqno,
            next_chain_time,
            created_by,
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
                    mc_data,
                    &mut collation_data_builder,
                    top_shard_blocks_info,
                )?;
            }
        } else {
            collation_data_builder.mc_shards_processed_to_by_partitions =
                mc_data.shards_processed_to_by_partitions.clone();
        }

        let start_lt = Self::calc_start_lt(
            mc_data.gen_lt,
            mc_data.lt_align(),
            prev_shard_data.gen_lt(),
            is_masterchain,
            collation_data_builder.shards_max_end_lt,
        )?;

        let mut collation_data = Box::new(collation_data_builder.build(start_lt, block_limits));

        // compute created / minted / recovered / from_prev_block
        let prev_total_balance = &prev_shard_data.observable_accounts().root_extra().balance;
        Self::init_value_flow(
            &mc_data.config,
            &mc_data.global_balance,
            prev_total_balance,
            &mc_data.total_validator_fees,
            &mut collation_data,
        )?;

        Ok(collation_data)
    }

    async fn finalize_collation(
        &mut self,
        ctx: FinalizeCollationCtx,
    ) -> Result<FinalizeCollationResult> {
        let labels = [("workchain", self.shard_id.workchain().to_string())];

        let FinalizeCollationCtx {
            has_unprocessed_messages,
            finalized,
            reader_state,
            ref_mc_state_handle,
            force_next_mc_block,
            resume_collation_elapsed,
        } = ctx;

        let block_id = *finalized.block_candidate.block.id();
        let is_key_block = finalized.block_candidate.is_key_block;
        let store_new_state_task = JoinTask::new({
            let meta = NewBlockMeta {
                is_key_block,
                gen_utime: finalized.collation_data.gen_utime,
                ref_by_mc_seqno: finalized.block_candidate.ref_by_mc_seqno,
            };
            let adapter = self.state_node_adapter.clone();
            let new_state_root = finalized.new_state_root.clone();

            let hint = StoreStateHint {
                block_data_size: Some(finalized.block_candidate.block.data_size()),
                updated_accounts: Some(finalized.collation_data.updated_accounts_count),
            };

            adapter.accept_shard_block(
                finalized.block_candidate.ref_by_mc_seqno,
                finalized.block_candidate.block.data.clone(),
            )?;

            async move {
                adapter
                    .store_state_root(&block_id, meta, new_state_root, hint)
                    .await
            }
        });

        let handle_block_candidate_elapsed;
        {
            let histogram = HistogramGuard::begin_with_labels(
                "tycho_do_collate_handle_block_candidate_time_high",
                &labels,
            );

            let new_queue_diff_hash = *finalized.block_candidate.queue_diff_aug.diff_hash();

            let collation_config = match &finalized.mc_data {
                Some(mcd) => Arc::new(mcd.config.get_collation_config()?),
                None => finalized.collation_config,
            };

            // force next master block if there are no pending messages after current shard block
            let force_next_mc_block =
                if !self.shard_id.is_masterchain() && !has_unprocessed_messages {
                    ForceMasterCollation::NoPendingMessagesAfterShardBlocks
                } else {
                    force_next_mc_block
                };

            // return collation result
            self.listener
                .on_block_candidate(BlockCollationResult {
                    collation_session_id: self.collation_session.id(),
                    candidate: finalized.block_candidate,
                    prev_mc_block_id: finalized.old_mc_data.block_id,
                    mc_data: finalized.mc_data.clone(),
                    collation_config: collation_config.clone(),
                    force_next_mc_block,
                    has_processed_externals: finalized.collation_data.execute_count_ext > 0,
                })
                .await?;

            let new_mc_data = finalized.mc_data.unwrap_or(finalized.old_mc_data);

            // spawn update PrevData and working state
            self.prepare_working_state_update(
                block_id,
                finalized.new_observable_state,
                finalized.new_state_root,
                finalized.state_update,
                store_new_state_task,
                new_queue_diff_hash,
                new_mc_data,
                collation_config,
                has_unprocessed_messages,
                reader_state,
                ref_mc_state_handle,
                resume_collation_elapsed,
            )?;

            tracing::debug!(target: tracing_targets::COLLATOR,
                "working state updated prepare spawned",
            );

            // update next block info
            self.next_block_info = block_id.get_next_id_short();

            handle_block_candidate_elapsed = histogram.finish();
        }

        // NOTE: Collation data can be quite large so drop it outside of tokio.
        Reclaimer::instance().drop(finalized.collation_data);

        Ok(FinalizeCollationResult {
            handle_block_candidate_elapsed,
        })
    }

    fn report_collation_metrics(&self, collation_data: &BlockCollationData) {
        let mut labels = vec![("workchain", self.shard_id.workchain().to_string())];

        metrics::gauge!("tycho_shard_accounts_count", &labels)
            .set(collation_data.shard_accounts_count as f64);
        metrics::gauge!("tycho_do_collate_accounts_per_block", &labels)
            .set(collation_data.updated_accounts_count as f64);
        metrics::gauge!("tycho_do_collate_added_accounts_count", &labels)
            .set(collation_data.added_accounts_count as f64);
        metrics::gauge!("tycho_do_collate_removed_accounts_count", &labels)
            .set(collation_data.removed_accounts_count as f64);

        metrics::gauge!("tycho_do_collate_block_diff_tail_len", &labels)
            .set(collation_data.diff_tail_len);

        // Report total_items limits (current value vs configured soft/hard limits)
        let lt_delta = &collation_data.block_limit.block_limits.lt_delta;
        let BlockParamLimits {
            soft_limit,
            hard_limit,
            ..
        } = lt_delta;
        metrics::gauge!("tycho_do_collate_total_items_current", &labels)
            .set(collation_data.block_limit.total_items as f64);
        metrics::gauge!("tycho_do_collate_total_items_soft_limit", &labels).set(*soft_limit as f64);
        metrics::gauge!("tycho_do_collate_total_items_hard_limit", &labels).set(*hard_limit as f64);

        // Report total_accounts limits (bytes: current value vs configured soft/hard limits)
        let bytes = &collation_data.block_limit.block_limits.bytes;
        let BlockParamLimits {
            soft_limit,
            hard_limit,
            ..
        } = bytes;
        metrics::gauge!("tycho_do_collate_total_accounts_current", &labels)
            .set(collation_data.block_limit.total_accounts as f64);
        metrics::gauge!("tycho_do_collate_total_accounts_soft_limit", &labels)
            .set(*soft_limit as f64);
        metrics::gauge!("tycho_do_collate_total_accounts_hard_limit", &labels)
            .set(*hard_limit as f64);

        labels.push(("src", "03_collated".to_string()));
        metrics::gauge!("tycho_last_block_seqno", &labels).set(collation_data.block_id_short.seqno);
    }

    fn log_block_and_stats(
        &self,
        execute_result: &ExecuteResult,
        finalize_result: &FinalizeBlockResult,
        final_result: &FinalResult,
    ) {
        tracing::debug!(target: tracing_targets::COLLATOR, "{:?}", self.stats);

        let collation_data = &finalize_result.collation_data;

        let queue_diff_messages_count = finalize_result
            .block_candidate
            .queue_diff_aug
            .data
            .diff()
            .messages
            .len();

        tracing::info!(target: tracing_targets::COLLATOR,
            "collated_block_id={}, \
            start_lt={}, end_lt={}, exec_count={}, \
            exec_ext={}, ext_err={}, exec_int={}, exec_new_int={}, \
            enqueue_count={}, dequeue_count={}, \
            new_msgs_created={}, inserted_new_msgs={}, \
            queue_diff_messages_count={}, \
            in_msgs={}, out_msgs={}, \
            read_ext_msgs={}, read_int_msgs={}, read_new_msgs={}, \
            has_unprocessed_messages={}, \
            total_execute_msgs_time_mc={}, \
            diffs_tail_len: {}",
            finalize_result.block_candidate.block.id(),
            collation_data.start_lt, collation_data.next_lt, collation_data.execute_count_all,
            collation_data.execute_count_ext, collation_data.ext_msgs_error_count,
            collation_data.execute_count_int, collation_data.execute_count_new_int,
            collation_data.int_enqueue_count, collation_data.int_dequeue_count,
            collation_data.new_msgs_created_count, collation_data.inserted_new_msgs_count,
            queue_diff_messages_count,
            collation_data.in_msgs.len(), collation_data.out_msgs.len(),
            execute_result.msgs_reader_metrics.read_ext_msgs_count,
            execute_result.msgs_reader_metrics.read_existing_msgs_count,
            execute_result.msgs_reader_metrics.read_new_msgs_count,
            final_result.has_unprocessed_messages,
            collation_data.total_execute_msgs_time_mc,
            collation_data.diff_tail_len,
        );

        tracing::debug!(
            target: tracing_targets::COLLATOR,
            prepare = %format_duration(final_result.prepare_elapsed),

            execute_tick = %format_duration(execute_result.execute_metrics.execute_tick_elapsed),
            execute_special = %format_duration(execute_result.execute_metrics.execute_special_elapsed),
            execute_incoming_msgs = %format_duration(execute_result.execute_metrics.execute_incoming_msgs_elapsed),
            execute_tock = %format_duration(execute_result.execute_metrics.execute_tock_elapsed),

            fill_msgs_total = %format_duration(execute_result.prepare_msg_groups_wu.total_elapsed),
            init_iterator = %format_duration(execute_result.msgs_reader_metrics.init_iterator_timer.total_elapsed),
            read_ext = %format_duration(execute_result.msgs_reader_metrics.read_ext_messages_timer.total_elapsed),
            read_existing = %format_duration(execute_result.msgs_reader_metrics.read_existing_messages_timer.total_elapsed),
            read_new = %format_duration(execute_result.msgs_reader_metrics.read_new_messages_timer.total_elapsed),
            add_to_groups = %format_duration(execute_result.msgs_reader_metrics.add_to_message_groups_timer.total_elapsed),

            exec_msgs_total = %format_duration(execute_result.execute_wu.execute_groups_vm_only_elapsed),
            process_txs_total = %format_duration(execute_result.execute_wu.process_txs_elapsed),

            create_queue_diff = %format_duration(finalize_result.finalize_metrics.create_queue_diff_elapsed),
            apply_queue_diff = %format_duration(finalize_result.finalize_metrics.apply_queue_diff_elapsed),
            finalize_block = %format_duration(finalize_result.finalize_metrics.finalize_block_elapsed),
            finalize_total = %format_duration(finalize_result.finalize_metrics.total_timer.total_elapsed),
            "collation timings"
        );
    }

    fn log_finalize_timings(finalize_metrics: &FinalizeMetrics) {
        tracing::debug!(
            target: tracing_targets::COLLATOR,
            total_elapsed = %format_duration(finalize_metrics.total_timer.total_elapsed),
            create_queue_diff = %format_duration(finalize_metrics.create_queue_diff_elapsed),
            apply_queue_diff = %format_duration(finalize_metrics.apply_queue_diff_elapsed),
            finalize_block = %format_duration(finalize_metrics.finalize_block_elapsed),
            parallel_build_accounts_and_msgs = %format_duration(finalize_metrics.build_accounts_and_messages_in_parallel_elased),
            only_build_accounts = %format_duration(finalize_metrics.build_accounts_elapsed),
            incl_update_shard_accounts = %format_duration(finalize_metrics.update_shard_accounts_elapsed),
            incl_build_accounts_blocks = %format_duration(finalize_metrics.build_accounts_blocks_elapsed),
            only_build_in_msgs = %format_duration(finalize_metrics.build_in_msgs_elapsed),
            only_build_out_msgs = %format_duration(finalize_metrics.build_out_msgs_elapsed),
            build_mc_state_extra = %format_duration(finalize_metrics.build_mc_state_extra_elapsed),
            build_state_update = %format_duration(finalize_metrics.build_state_update_elapsed),
            build_block = %format_duration(finalize_metrics.build_block_elapsed),
            "finalize block timings"
        );
    }

    fn log_final_timings(
        collation_total_elapsed: Duration,
        block_id: BlockId,
        block_time_diff: i64,
        collation_mngmnt_overhead: Duration,
        elapsed_from_prev_block: Duration,
        handle_block_candidate_elapsed: Duration,
    ) {
        tracing::info!(target: tracing_targets::COLLATOR,
            "collated_block_id={}, time_diff={}, \
            collation_time={}, elapsed_from_prev_block={}, overhead={}",
            block_id, block_time_diff, collation_total_elapsed.as_millis(),
            elapsed_from_prev_block.as_millis(), collation_mngmnt_overhead.as_millis(),
        );

        tracing::debug!(
            target: tracing_targets::COLLATOR,
            block_time_diff = block_time_diff,
            from_prev_block = %format_duration(elapsed_from_prev_block),
            overhead = %format_duration(collation_mngmnt_overhead),
            total = %format_duration(collation_total_elapsed),
            handle_block_candidate = %format_duration(handle_block_candidate_elapsed),
            "total collation timings"
        );
    }

    /// Collect ranges for loading statistics if current block is first after master
    /// Using previous masterchain block diff for range
    /// and other shards diffs between previous masterchain block - 1 and previous masterchain block
    async fn compute_part_stat_ranges(
        &self,
        mc_data: &McData,
        block_id_short: &BlockIdShort,
    ) -> Result<Option<Vec<QueueShardBoundedRange>>> {
        let Some(prev_mc_data) = &mc_data.prev_mc_data else {
            return Ok(None);
        };

        let prev_shards: FastHashMap<_, _> = prev_mc_data.shards.iter().cloned().collect();

        let mut shard_pairs = Vec::new();
        for (shard, curr) in mc_data.shards.iter() {
            if curr.top_sc_block_updated
                && *shard != block_id_short.shard
                && let Some(prev) = prev_shards.get(shard)
            {
                shard_pairs.push(ShardPair {
                    shard_ident: *shard,
                    prev_block_id: TopBlockId {
                        ref_by_mc_seqno: prev.reg_mc_seqno,
                        block_id: prev.get_block_id(*shard),
                    },
                    current_block_id: TopBlockId {
                        ref_by_mc_seqno: curr.reg_mc_seqno,
                        block_id: curr.get_block_id(*shard),
                    },
                });
            }
        }

        self.build_ranges(&mc_data.block_id, shard_pairs).await
    }

    /// Collect ranges for loading cumulative statistics if collation block is master
    /// Using range from previous masterchain block diff
    /// and diffs between previous masterchain top blocks and current top blocks
    async fn mc_compute_part_stat_ranges(
        &self,
        mc_data: &McData,
        top_shard_blocks_info: Vec<TopBlockDescription>,
    ) -> Result<Option<Vec<QueueShardBoundedRange>>> {
        let prev_shards: FastHashMap<_, _> = mc_data.shards.iter().cloned().collect();

        let mut shard_pairs = Vec::new();
        for top in top_shard_blocks_info {
            if let Some(prev) = prev_shards.get(&top.block_id.shard) {
                shard_pairs.push(ShardPair {
                    shard_ident: top.block_id.shard,
                    prev_block_id: TopBlockId {
                        ref_by_mc_seqno: prev.reg_mc_seqno,
                        block_id: prev.get_block_id(top.block_id.shard),
                    },
                    current_block_id: TopBlockId {
                        ref_by_mc_seqno: mc_data.block_id.seqno,
                        block_id: top.block_id,
                    },
                });
            }
        }

        self.build_ranges(&mc_data.block_id, shard_pairs).await
    }

    async fn build_ranges(
        &self,
        master_block_id: &BlockId,
        shard_pairs: Vec<ShardPair>,
    ) -> Result<Option<Vec<QueueShardBoundedRange>>> {
        let Some(master_max_msg) = self
            .get_diff_max_message(master_block_id)
            .await
            .context("loading diff for mc block")?
        else {
            return Ok(None);
        };

        // add mc block diff range
        let mut ranges = vec![QueueShardBoundedRange {
            shard_ident: master_block_id.shard,
            from: Bound::Included(master_max_msg),
            to: Bound::Included(master_max_msg),
        }];

        for pair in shard_pairs {
            if pair.prev_block_id.ref_by_mc_seqno == self.zerostate_id.seqno {
                return Ok(None);
            }

            let (first_msg, last_msg) = tokio::try_join!(
                self.get_diff_max_message(&pair.prev_block_id.block_id),
                self.get_diff_max_message(&pair.current_block_id.block_id)
            )
            .context("loading shard diffs")?;

            match (first_msg, last_msg) {
                (Some(first), Some(last)) => ranges.push(QueueShardBoundedRange {
                    shard_ident: pair.shard_ident,
                    from: Bound::Excluded(first),
                    to: Bound::Included(last),
                }),
                _ => return Ok(None),
            }
        }

        Ok(Some(ranges))
    }

    /// Helper function that retrieves `max_message` from either MQ or state diff
    async fn get_diff_max_message(&self, block_id: &BlockId) -> Result<Option<QueueKey>> {
        if let Some(diff) =
            self.mq_adapter
                .get_diff_info(&block_id.shard, block_id.seqno, DiffZone::Both)?
        {
            Ok(Some(diff.max_message))
        } else if let Some(diff) = self.state_node_adapter.load_diff(block_id).await? {
            Ok(Some(diff.diff().max_message))
        } else {
            tracing::warn!(target: tracing_targets::COLLATOR, "diff not found by block: {block_id:?}");
            Ok(None)
        }
    }
}

pub fn is_first_block_after_prev_master(
    prev_block_id: BlockId,
    mc_data_shards: &[(ShardIdent, ShardDescriptionShort)],
) -> bool {
    if prev_block_id.shard.is_masterchain() {
        return true;
    }

    for (shard, descr) in mc_data_shards {
        if shard == &prev_block_id.shard && descr.seqno >= prev_block_id.seqno {
            return true;
        }
    }

    false
}

fn serialize_diff(
    queue_diff_with_msgs: &QueueDiffWithMessages<EnqueuedMessage>,
    min_message: &QueueKey,
    max_message: &QueueKey,
    block_id_short: &BlockIdShort,
    prev_hash: &HashBytes,
    processed_to: ProcessedTo,
) -> SerializedQueueDiff {
    QueueDiffStuff::builder(block_id_short.shard, block_id_short.seqno, prev_hash)
        .with_processed_to(processed_to)
        .with_messages(
            min_message,
            max_message,
            queue_diff_with_msgs.messages.keys().map(|k| &k.hash),
        )
        .with_router(
            queue_diff_with_msgs
                .partition_router
                .to_router_partitions_src(),
            queue_diff_with_msgs
                .partition_router
                .to_router_partitions_dst(),
        )
        .serialize()
}

fn create_apply_diff_task(
    mq_adapter: &Arc<dyn MessageQueueAdapter<EnqueuedMessage>>,
    queue_diff_with_msgs: QueueDiffWithMessages<EnqueuedMessage>,
    block_id_short: &BlockIdShort,
    min_message: QueueKey,
    max_message: QueueKey,
    diff_hash: HashBytes,
) -> Result<impl FnOnce() -> Result<Duration>> {
    // create update queue task but do not run it
    let update_queue_task = {
        let labels = [("workchain", block_id_short.shard.workchain().to_string())];
        let span = tracing::Span::current();
        move || {
            let _span = span.enter();

            let histogram = HistogramGuard::begin_with_labels(
                "tycho_do_collate_build_statistics_time_high",
                &labels,
            );

            let statistics = DiffStatistics::from_diff(
                &queue_diff_with_msgs,
                block_id_short.shard,
                min_message,
                max_message,
            );

            histogram.finish();

            // apply queue diff
            let histogram = HistogramGuard::begin_with_labels(
                "tycho_do_collate_apply_queue_diff_time_high",
                &labels,
            );

            // check only uncommitted diffs because the last committed diff
            // may not be sequential after sync on a block ahead
            mq_adapter
                .apply_diff(
                    queue_diff_with_msgs,
                    *block_id_short,
                    &diff_hash,
                    statistics,
                    Some(DiffZone::Uncommitted),
                )
                .context("finalize")?;
            let apply_queue_diff_elapsed = histogram.finish();

            Ok(apply_queue_diff_elapsed)
        }
    };

    Ok(update_queue_task)
}
