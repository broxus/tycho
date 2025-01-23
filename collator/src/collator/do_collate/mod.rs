use std::collections::hash_map;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use everscale_types::models::*;
use everscale_types::num::Tokens;
use everscale_types::prelude::*;
use humantime::format_duration;
use phase::{ActualState, Phase};
use prepare::PrepareState;
use tycho_block_util::queue::QueueKey;
use tycho_block_util::state::MinRefMcStateTracker;
use tycho_storage::{NewBlockMeta, StoreStateHint};
use tycho_util::futures::JoinTask;
use tycho_util::metrics::HistogramGuard;
use tycho_util::time::now_millis;
use tycho_util::FastHashMap;

use super::messages_reader::ReaderState;
use super::types::{
    AnchorInfo, AnchorsCache, BlockCollationData, BlockCollationDataBuilder, CollationResult,
    ExecuteResult, FinalResult, FinalizeBlockResult, FinalizeCollationResult,
    FinalizeMessagesReaderResult, PrevData, ShardDescriptionExt, WorkingState,
};
use super::{CollatorStdImpl, ForceMasterCollation};
use crate::collator::do_collate::finalize::FinalizeBlockContext;
use crate::collator::types::RandSeed;
use crate::internal_queue::types::EnqueuedMessage;
use crate::queue_adapter::MessageQueueAdapter;
use crate::tracing_targets;
use crate::types::{
    BlockCollationResult, BlockIdExt, CollationSessionInfo, CollatorConfig,
    DisplayBlockIdsIntoIter, DisplayBlockIdsIter, McData, ProcessedTo, ShardDescriptionShort,
    TopBlockDescription, TopShardBlockInfo,
};

#[cfg(test)]
#[path = "../tests/do_collate_tests.rs"]
pub(super) mod tests;

mod execute;
mod execution_wrapper;
mod finalize;
mod phase;
mod prepare;

impl CollatorStdImpl {
    /// [`force_next_mc_block`] - should force next master block collation after this block
    #[tracing::instrument(
        parent =  None,
        skip_all,
        fields(
            block_id = %self.next_block_info,
            ct = self.anchors_cache.last_imported_anchor().map(|a| a.ct).unwrap_or_default(),
        )
    )]
    pub(super) async fn do_collate(
        &mut self,
        working_state: Box<WorkingState>,
        top_shard_blocks_info: Option<Vec<TopBlockDescription>>,
        force_next_mc_block: ForceMasterCollation,
    ) -> Result<()> {
        let labels: [(&str, String); 1] = [("workchain", self.shard_id.workchain().to_string())];
        let total_collation_histogram =
            HistogramGuard::begin_with_labels("tycho_do_collate_total_time", &labels);

        let WorkingState {
            next_block_id_short,
            mc_data,
            collation_config,
            wu_used_from_last_anchor,
            prev_shard_data,
            usage_tree,
            reader_state,
            ..
        } = *working_state;

        let prev_shard_data = prev_shard_data.unwrap();
        let usage_tree = usage_tree.unwrap();
        let tracker = prev_shard_data.ref_mc_state_handle().tracker().clone();

        tracing::info!(target: tracing_targets::COLLATOR,
            "Start collating block: mc_data_block_id={}, prev_block_ids={}, top_shard_blocks_ids: {:?}",
            mc_data.block_id.as_short_id(),
            DisplayBlockIdsIntoIter(prev_shard_data.blocks_ids()),
            top_shard_blocks_info.as_ref().map(|v| DisplayBlockIdsIter(
                v.iter().map(|i| &i.block_id)
            )),
        );

        let Some(&AnchorInfo {
            ct: next_chain_time,
            author,
            ..
        }) = self.anchors_cache.last_imported_anchor()
        else {
            bail!("last_imported_anchor should exist when we collating block")
        };
        let created_by = author.to_bytes().into();

        let collation_data = self.create_collation_data(
            next_block_id_short,
            next_chain_time,
            created_by,
            &mc_data,
            &prev_shard_data,
            top_shard_blocks_info,
        )?;

        let anchors_cache = std::mem::take(&mut self.anchors_cache);
        let state = Box::new(ActualState {
            collation_config,
            collation_data,
            mc_data,
            prev_shard_data,
            shard_id: self.shard_id,
        });

        let CollationResult {
            finalized,
            reader_state,
            anchors_cache,
            execute_result,
            final_result,
        } = tycho_util::sync::rayon_run_fifo({
            let collation_session = self.collation_session.clone();
            let config = self.config.clone();
            let mq_adapter = self.mq_adapter.clone();
            let span = tracing::Span::current();
            move || {
                let _span = span.enter();

                Self::run(
                    config,
                    mq_adapter,
                    reader_state,
                    anchors_cache,
                    state,
                    collation_session,
                    wu_used_from_last_anchor,
                    usage_tree,
                )
            }
        })
        .await?;

        self.anchors_cache = anchors_cache;

        let block_id = *finalized.block_candidate.block.id();
        let finalize_wu_total = finalized.finalize_wu_total;

        self.collation_data_metrics(&finalized.collation_data);

        self.update_stats(&finalized.collation_data);

        self.logs(
            &execute_result,
            &final_result,
            finalize_wu_total,
            &finalized.collation_data,
            block_id,
        );

        let FinalizeCollationResult {
            handle_block_candidate_elapsed,
        } = self
            .finalize_collation(
                final_result.has_unprocessed_messages,
                finalized,
                reader_state,
                tracker,
                force_next_mc_block,
            )
            .await?;

        let total_elapsed = total_collation_histogram.finish();

        // metrics
        metrics::counter!("tycho_do_collate_blocks_count", &labels).increment(1);

        self.wu_metrics(
            &execute_result,
            finalize_wu_total,
            final_result.finalize_block_elapsed,
            total_elapsed,
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
        let diff_time = now_millis() as i64
            - execute_result
                .last_read_to_anchor_chain_time
                .unwrap_or(next_chain_time) as i64;
        metrics::gauge!("tycho_do_collate_ext_msgs_time_diff", &labels)
            .set(diff_time as f64 / 1000.0);

        self.logs_total(
            total_elapsed,
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
        reader_state: ReaderState,
        anchors_cache: AnchorsCache,
        state: Box<ActualState>,
        collation_session: Arc<CollationSessionInfo>,
        wu_used_from_last_anchor: u64,
        usage_tree: UsageTree,
    ) -> Result<CollationResult> {
        let shard_id = state.shard_id;
        let labels: [(&str, String); 1] = [("workchain", shard_id.workchain().to_string())];
        let mc_data = state.mc_data.clone();

        // prepare execution
        let histogram_prepare =
            HistogramGuard::begin_with_labels("tycho_do_collate_prepare_time", &labels);

        let prepare_phase = Phase::<PrepareState>::new(
            collator_config.clone(),
            mq_adapter.clone(),
            reader_state,
            anchors_cache,
            state,
        );

        let mut execute_phase = prepare_phase.run()?;

        let prepare_elapsed = histogram_prepare.finish();

        // execute tick transaction and special transactions (mint, recover)
        let execute_tick_elapsed = if shard_id.is_masterchain() {
            let histogram =
                HistogramGuard::begin_with_labels("tycho_do_collate_execute_tick_time", &labels);

            execute_phase.execute_tick_transactions()?;

            execute_phase.execute_special_transactions()?;

            histogram.finish()
        } else {
            Duration::ZERO
        };

        // execute incoming messages
        let execute_elapsed = {
            let histogram =
                HistogramGuard::begin_with_labels("tycho_do_collate_execute_time", &labels);

            execute_phase.run()?;

            histogram.finish()
        };

        // execute tock transaction
        let execute_tock_elapsed = if shard_id.is_masterchain() {
            let histogram =
                HistogramGuard::begin_with_labels("tycho_do_collate_execute_tock_time", &labels);

            execute_phase.execute_tock_transactions()?;

            histogram.finish()
        } else {
            Duration::ZERO
        };

        let (mut finalize_phase, messages_reader) = execute_phase.finish();

        let (
            FinalizeMessagesReaderResult {
                queue_diff,
                queue_diff_messages_count,
                has_unprocessed_messages,
                reader_state,
                anchors_cache,
                create_queue_diff_elapsed,
            },
            update_queue_task,
        ) = finalize_phase.finalize_messages_reader(messages_reader, mq_adapter.clone())?;

        let finalize_block_timer = std::time::Instant::now();

        let mut processed_upto = reader_state.get_updated_processed_upto();
        // store actual messages execution params
        processed_upto.msgs_exec_params = Some(
            finalize_phase
                .state
                .collation_config
                .msgs_exec_params
                .clone(),
        );

        // log updated processed upto
        tracing::debug!(target: tracing_targets::COLLATOR, "updated processed_upto = {:?}", processed_upto);

        // get min processed to for current shard from shard and mc_data
        let current_min_processed_to = processed_upto
            .get_min_internals_processed_to_by_shards()
            .get(&shard_id)
            .cloned();
        let min_processed_to_from_mc_data = mc_data
            .processed_upto
            .get_min_internals_processed_to_by_shards()
            .get(&shard_id)
            .cloned();

        // calculate minimal internals processed_to for this shard
        let min_processed_to = calculate_min_internals_processed_to(
            &shard_id,
            current_min_processed_to,
            min_processed_to_from_mc_data,
            &mc_data.shards,
            &mc_data.shards_processed_to,
        );

        // trim outdated diffs and calc queue diffs tail lenght
        if let Some(value) = min_processed_to {
            mq_adapter.trim_diffs(&shard_id, &value)?;
        };
        let diff_tail_len = mq_adapter.get_diffs_count_by_shard(&shard_id) as u32 + 1;

        let span = tracing::Span::current();
        let (finalize_phase_result, update_queue_task_result) = rayon::join(
            || {
                let _span = span.enter();

                finalize_phase.finalize_block(FinalizeBlockContext {
                    collation_session,
                    wu_used_from_last_anchor,
                    usage_tree,
                    queue_diff,
                    collator_config,
                    processed_upto,
                    diff_tail_len,
                })
            },
            // wait update queue task before returning collation result
            // to be sure that queue was updated before block commit and next block collation
            update_queue_task,
        );
        let (finalized, execute_result) = finalize_phase_result?;

        let finalize_block_elapsed = finalize_block_timer.elapsed();

        let apply_queue_diff_elapsed = update_queue_task_result?;

        assert_eq!(
            finalized.collation_data.int_enqueue_count,
            finalized.collation_data.inserted_new_msgs_count
                - finalized.collation_data.execute_count_new_int
        );

        let final_result = FinalResult {
            prepare_elapsed,
            finalize_block_elapsed,
            has_unprocessed_messages,
            queue_diff_messages_count,
            execute_elapsed,
            execute_tick_elapsed,
            execute_tock_elapsed,
            create_queue_diff_elapsed,
            apply_queue_diff_elapsed,
        };

        Ok(CollationResult {
            finalized,
            reader_state,
            anchors_cache,
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

    fn update_value_flow(
        is_masterchain: bool,
        config: &BlockchainConfig,
        old_global_balance: &CurrencyCollection,
        prev_total_balance: &CurrencyCollection,
        total_validator_fees: &CurrencyCollection,
        collation_data: &mut BlockCollationData,
    ) -> Result<()> {
        tracing::trace!(target: tracing_targets::COLLATOR, "update_value_flow");

        if is_masterchain {
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
        collation_data.value_flow.from_prev_block = prev_total_balance.clone();
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

    fn import_new_shard_top_blocks_for_masterchain(
        config: &BlockchainConfig,
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
                processed_to,
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

            let top_shard_block_info = TopShardBlockInfo {
                block_id,
                processed_to,
            };

            collation_data_builder
                .top_shard_blocks
                .push(top_shard_block_info);
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

    fn create_collation_data(
        &mut self,
        next_block_id_short: BlockIdShort,
        next_chain_time: u64,
        created_by: HashBytes,
        mc_data: &Arc<McData>,
        prev_shard_data: &PrevData,
        top_shard_blocks_info: Option<Vec<TopBlockDescription>>,
    ) -> Result<Box<BlockCollationData>> {
        // TODO: need to generate unique for each block
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
                    &mc_data.config,
                    &mut collation_data_builder,
                    top_shard_blocks_info,
                )?;
            }
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
        Self::update_value_flow(
            is_masterchain,
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
        has_unprocessed_messages: bool,
        finalized: FinalizeBlockResult,
        reader_state: ReaderState,
        tracker: MinRefMcStateTracker,
        force_next_mc_block: ForceMasterCollation,
    ) -> Result<FinalizeCollationResult> {
        let labels = [("workchain", self.shard_id.workchain().to_string())];

        let block_id = *finalized.block_candidate.block.id();
        let is_key_block = finalized.block_candidate.is_key_block;
        let store_new_state_task = JoinTask::new({
            let meta = NewBlockMeta {
                is_key_block,
                gen_utime: finalized.collation_data.gen_utime,
                ref_by_mc_seqno: finalized.block_candidate.ref_by_mc_seqno,
            };
            let adapter = self.state_node_adapter.clone();
            let labels = labels.clone();
            let new_state_root = finalized.new_state_root.clone();
            let hint = StoreStateHint {
                block_data_size: Some(finalized.block_candidate.block.data_size()),
            };
            async move {
                let _histogram = HistogramGuard::begin_with_labels(
                    "tycho_collator_build_new_state_time",
                    &labels,
                );
                adapter
                    .store_state_root(&block_id, meta, new_state_root, hint)
                    .await
            }
        });

        let handle_block_candidate_elapsed;
        {
            let histogram = HistogramGuard::begin_with_labels(
                "tycho_do_collate_handle_block_candidate_time",
                &labels,
            );

            let new_queue_diff_hash = *finalized.block_candidate.queue_diff_aug.diff_hash();

            let collation_config = match &finalized.mc_data {
                Some(mcd) => Arc::new(mcd.config.get_collation_config()?),
                None => finalized.collation_config,
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
                })
                .await?;

            let new_mc_data = finalized.mc_data.unwrap_or(finalized.old_mc_data);

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
                reader_state,
                tracker,
            )?;

            tracing::debug!(target: tracing_targets::COLLATOR,
                "working state updated prepare spawned",
            );

            // update next block info
            self.next_block_info = block_id.get_next_id_short();

            handle_block_candidate_elapsed = histogram.finish();
        }
        Ok(FinalizeCollationResult {
            handle_block_candidate_elapsed,
        })
    }

    fn collation_data_metrics(&self, collation_data: &BlockCollationData) {
        let labels = [("workchain", self.shard_id.workchain().to_string())];
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
            .increment(collation_data.read_ext_msgs_count);
        metrics::counter!("tycho_do_collate_msgs_exec_count_ext", &labels)
            .increment(collation_data.execute_count_ext);
        metrics::counter!("tycho_do_collate_msgs_error_count_ext", &labels)
            .increment(collation_data.ext_msgs_error_count);
        metrics::counter!("tycho_do_collate_msgs_skipped_count_ext", &labels)
            .increment(collation_data.ext_msgs_skipped_count);
        // existing internals messages
        metrics::counter!("tycho_do_collate_msgs_read_count_int", &labels)
            .increment(collation_data.read_int_msgs_from_iterator_count);
        metrics::counter!("tycho_do_collate_msgs_exec_count_int", &labels)
            .increment(collation_data.execute_count_int);
        // new internals messages
        metrics::counter!("tycho_do_collate_new_msgs_created_count", &labels)
            .increment(collation_data.new_msgs_created_count);
        metrics::counter!(
            "tycho_do_collate_new_msgs_inserted_to_iterator_count",
            &labels
        )
        .increment(collation_data.inserted_new_msgs_count);
        metrics::counter!("tycho_do_collate_msgs_read_count_new_int", &labels)
            .increment(collation_data.read_new_msgs_count);
        metrics::counter!("tycho_do_collate_msgs_exec_count_new_int", &labels)
            .increment(collation_data.execute_count_new_int);
        metrics::gauge!("tycho_do_collate_block_seqno", &labels)
            .set(collation_data.block_id_short.seqno);
        metrics::gauge!("tycho_do_collate_block_diff_tail_len", &labels)
            .set(collation_data.diff_tail_len);
    }

    fn wu_metrics(
        &self,
        execute_result: &ExecuteResult,
        finalize_wu_total: u64,
        finalize_block_elapsed: Duration,
        total_elapsed: Duration,
    ) {
        let labels = [("workchain", self.shard_id.workchain().to_string())];
        metrics::gauge!("tycho_do_collate_wu_on_prepare", &labels)
            .set(execute_result.prepare_groups_wu_total as f64);
        metrics::gauge!("tycho_do_collate_wu_on_execute", &labels)
            .set(execute_result.execute_groups_wu_total as f64);
        metrics::gauge!("tycho_do_collate_wu_on_finalize", &labels).set(finalize_wu_total as f64);
        metrics::gauge!("tycho_do_collate_wu_on_all", &labels).set(
            execute_result.execute_groups_wu_total as f64
                + finalize_wu_total as f64
                + execute_result.prepare_groups_wu_total as f64,
        );

        metrics::gauge!("tycho_do_collate_wu_to_mcs_prepare", &labels).set(
            execute_result.fill_msgs_total_elapsed.as_nanos() as f64
                / execute_result.prepare_groups_wu_total as f64,
        );
        metrics::gauge!("tycho_do_collate_wu_to_mcs_execute", &labels).set(
            (execute_result.execute_msgs_total_elapsed.as_nanos() as f64
                + execute_result.process_txs_total_elapsed.as_nanos() as f64)
                / execute_result.execute_groups_wu_total as f64,
        );
        metrics::gauge!("tycho_do_collate_execute_txs_to_wu", &labels).set(
            execute_result.execute_msgs_total_elapsed.as_nanos() as f64
                / execute_result.execute_groups_wu_vm_only as f64,
        );
        metrics::gauge!("tycho_do_collate_process_txs_to_wu", &labels).set(
            execute_result.process_txs_total_elapsed.as_nanos() as f64
                / execute_result.process_txs_wu as f64,
        );
        metrics::gauge!("tycho_do_collate_wu_to_mcs_finalize", &labels)
            .set(finalize_block_elapsed.as_nanos() as f64 / finalize_wu_total as f64);

        metrics::gauge!("tycho_do_collate_wu_to_mcs_total", &labels).set(
            total_elapsed.as_nanos() as f64
                / (execute_result.execute_groups_wu_total
                    + finalize_wu_total
                    + execute_result.prepare_groups_wu_total) as f64,
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn logs(
        &self,
        execute_result: &ExecuteResult,
        final_result: &FinalResult,
        finalize_wu_total: u64,
        collation_data: &BlockCollationData,
        block_id: BlockId,
    ) {
        tracing::debug!(target: tracing_targets::COLLATOR, "{:?}", self.stats);

        tracing::info!(target: tracing_targets::COLLATOR,
            "collated_block_id={}, \
            start_lt={}, end_lt={}, exec_count={}, \
            exec_ext={}, ext_err={}, exec_int={}, exec_new_int={}, \
            enqueue_count={}, dequeue_count={}, \
            new_msgs_created={}, queue_diff_messages_count={}, \
            in_msgs={}, out_msgs={}, \
            read_ext_msgs={}, read_int_msgs={}, \
            read_new_msgs_from_iterator={}, inserted_new_msgs_to_iterator={} has_unprocessed_messages={}, \
            total_execute_msgs_time_mc={}, \
            wu_used_for_prepare_msgs_groups={}, wu_used_for_execute: {}, wu_used_for_finalize: {}, diffs_tail_len: {}",
            block_id,
            collation_data.start_lt, collation_data.next_lt, collation_data.execute_count_all,
            collation_data.execute_count_ext, collation_data.ext_msgs_error_count,
            collation_data.execute_count_int, collation_data.execute_count_new_int,
            collation_data.int_enqueue_count, collation_data.int_dequeue_count,
            collation_data.new_msgs_created_count, final_result.queue_diff_messages_count,
            collation_data.in_msgs.len(), collation_data.out_msgs.len(),
            collation_data.read_ext_msgs_count, collation_data.read_int_msgs_from_iterator_count,
            collation_data.read_new_msgs_count, collation_data.inserted_new_msgs_count, final_result.has_unprocessed_messages,
            collation_data.total_execute_msgs_time_mc,
            execute_result.prepare_groups_wu_total, execute_result.execute_groups_wu_total, finalize_wu_total, collation_data.diff_tail_len,
        );

        tracing::debug!(
            target: tracing_targets::COLLATOR,
            prepare = %format_duration(final_result.prepare_elapsed),
            execute_tick = %format_duration(final_result.execute_tick_elapsed),
            execute_tock = %format_duration(final_result.execute_tock_elapsed),
            execute_total = %format_duration(final_result.execute_elapsed),

            fill_msgs_total = %format_duration(execute_result.fill_msgs_total_elapsed),
            init_iterator = %format_duration(execute_result.init_iterator_elapsed),
            read_existing = %format_duration(execute_result.read_existing_messages_elapsed),
            read_ext = %format_duration(execute_result.read_ext_messages_elapsed),
            read_new = %format_duration(execute_result.read_new_messages_elapsed),
            add_to_groups = %format_duration(execute_result.add_to_message_groups_elapsed),

            exec_msgs_total = %format_duration(execute_result.execute_msgs_total_elapsed),
            process_txs_total = %format_duration(execute_result.process_txs_total_elapsed),

            create_queue_diff = %format_duration(final_result.create_queue_diff_elapsed),
            apply_queue_diff = %format_duration(final_result.apply_queue_diff_elapsed),
            finalize_block = %format_duration(final_result.finalize_block_elapsed),
            "collation timings"
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn logs_total(
        &self,
        total_elapsed: Duration,
        block_id: BlockId,
        block_time_diff: i64,
        collation_mngmnt_overhead: Duration,
        elapsed_from_prev_block: Duration,
        handle_block_candidate_elapsed: Duration,
    ) {
        tracing::debug!(target: tracing_targets::COLLATOR, "{:?}", self.stats);

        tracing::info!(target: tracing_targets::COLLATOR,
            "collated_block_id={}, time_diff={}, \
            collation_time={}, elapsed_from_prev_block={}, overhead={}",
                        block_id, block_time_diff,
            total_elapsed.as_millis(), elapsed_from_prev_block.as_millis(), collation_mngmnt_overhead.as_millis()

        );

        tracing::debug!(
            target: tracing_targets::COLLATOR,
            block_time_diff = block_time_diff,
            from_prev_block = %format_duration(elapsed_from_prev_block),
            overhead = %format_duration(collation_mngmnt_overhead),
            total = %format_duration(total_elapsed),
            handle_block_candidate = %format_duration(handle_block_candidate_elapsed),
            "total collation timings"
        );
    }
}

fn calculate_min_internals_processed_to(
    shard_id: &ShardIdent,
    current_min_processed_to: Option<QueueKey>,
    min_processed_to_from_mc_data: Option<QueueKey>,
    mc_data_shards: &Vec<(ShardIdent, ShardDescriptionShort)>,
    mc_data_shards_processed_to: &FastHashMap<ShardIdent, ProcessedTo>,
) -> Option<QueueKey> {
    fn find_min_processed_to(
        shards: &Vec<(ShardIdent, ShardDescriptionShort)>,
        mc_data_shards_processed_to: &FastHashMap<ShardIdent, ProcessedTo>,
        shard_id: &ShardIdent,
        min_processed_to: &mut Option<QueueKey>,
        skip_condition: impl Fn(&ShardIdent) -> bool,
    ) {
        // Iterate through shards with updated top shard blocks and find min processed_to
        for (shard, descr) in shards {
            if skip_condition(shard) {
                continue;
            }

            if descr.top_sc_block_updated {
                if let Some(value) = mc_data_shards_processed_to.get(shard) {
                    if let Some(v) = value.get(shard_id) {
                        *min_processed_to = match *min_processed_to {
                            Some(current_min) => Some(current_min.min(*v)),
                            None => Some(*v),
                        };
                    }
                }
            }
        }
    }

    let mut min_processed_to: Option<QueueKey> = None;

    if shard_id.is_masterchain() {
        find_min_processed_to(
            mc_data_shards,
            mc_data_shards_processed_to,
            shard_id,
            &mut min_processed_to,
            |_| false,
        );

        // Combine with current and masterchain values
        min_processed_to = [current_min_processed_to, min_processed_to]
            .into_iter()
            .flatten()
            .min();
    } else {
        find_min_processed_to(
            mc_data_shards,
            mc_data_shards_processed_to,
            shard_id,
            &mut min_processed_to,
            |shard| shard == shard_id || shard.is_masterchain(),
        );

        // Combine with current and masterchain values and shard values
        min_processed_to = [
            current_min_processed_to,
            min_processed_to,
            min_processed_to_from_mc_data,
        ]
        .into_iter()
        .flatten()
        .min();
    }

    min_processed_to
}
