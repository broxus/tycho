use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use everscale_types::merkle::*;
use everscale_types::models::{ShardIdent, *};
use everscale_types::prelude::*;
use humantime::format_duration;
use tycho_block_util::archive::WithArchiveData;
use tycho_block_util::block::BlockStuff;
use tycho_block_util::config::BlockchainConfigExt;
use tycho_block_util::dict::RelaxedAugDict;
use tycho_block_util::queue::{QueueDiffStuff, QueueKey, SerializedQueueDiff};
use tycho_block_util::state::ShardStateStuff;
use tycho_consensus::prelude::ConsensusConfigExt;
use tycho_util::metrics::HistogramGuard;
use tycho_util::FastHashMap;

use super::phase::{Phase, PhaseState};
use super::PrevData;
use crate::collator::debug_info::BlockDebugInfo;
use crate::collator::execution_manager::MessagesExecutor;
use crate::collator::messages_reader::{FinalizedMessagesReader, MessagesReader};
use crate::collator::types::{
    BlockCollationData, ExecuteResult, FinalizeBlockResult, FinalizeMessagesReaderResult,
    PreparedInMsg, PreparedOutMsg,
};
use crate::internal_queue::types::{
    DiffStatistics, EnqueuedMessage, PartitionRouter, QueueShardRange,
};
use crate::queue_adapter::MessageQueueAdapter;
use crate::tracing_targets;
use crate::types::processed_upto::{ProcessedUptoInfoExtension, ProcessedUptoInfoStuff};
use crate::types::{
    BlockCandidate, CollationSessionInfo, CollatorConfig, McData, ShardDescriptionExt,
    ShardHashesExt,
};
use crate::utils::block::detect_top_processed_to_anchor;

pub struct FinalizeState {
    pub execute_result: ExecuteResult,
    pub executor: MessagesExecutor,
}

impl PhaseState for FinalizeState {}

pub struct FinalizeBlockContext {
    pub collation_session: Arc<CollationSessionInfo>,
    pub wu_used_from_last_anchor: u64,
    pub usage_tree: UsageTree,
    pub queue_diff: SerializedQueueDiff,
    pub collator_config: Arc<CollatorConfig>,
    pub processed_upto: ProcessedUptoInfoStuff,
    pub diff_tail_len: u32,
}

impl Phase<FinalizeState> {
    pub fn finalize_messages_reader(
        &mut self,
        messages_reader: MessagesReader<EnqueuedMessage>,
        mq_adapter: Arc<dyn MessageQueueAdapter<EnqueuedMessage>>,
    ) -> Result<(
        FinalizeMessagesReaderResult,
        impl FnOnce() -> Result<Duration>,
    )> {
        let labels = [("workchain", self.state.shard_id.workchain().to_string())];

        let prev_hash = self
            .state
            .prev_shard_data
            .prev_queue_diff_hashes()
            .first()
            .cloned()
            .unwrap_or_default();

        // getting top shard blocks
        let top_shard_blocks = if self.state.collation_data.block_id_short.is_masterchain() {
            self.state
                .collation_data
                .top_shard_blocks
                .iter()
                .map(|b| b.block_id)
                .collect()
        } else {
            let mut top_blocks: Vec<BlockId> = self
                .state
                .mc_data
                .shards
                .iter()
                .filter(|(shard, descr)| {
                    descr.top_sc_block_updated && shard != &self.state.shard_id
                })
                .map(|(shard_ident, descr)| descr.get_block_id(*shard_ident))
                .collect();

            top_blocks.push(self.state.mc_data.block_id);

            top_blocks
        };

        // load top blocks diffs
        let mut diffs_info = FastHashMap::default();

        for top_shard_block in top_shard_blocks.iter() {
            if top_shard_block.seqno == 0 {
                continue;
            }

            let diff = mq_adapter
                .get_diff(&top_shard_block.shard, top_shard_block.seqno)?
                .ok_or_else(|| anyhow!("Diff for block {} not found", top_shard_block))?;

            let partition_router = PartitionRouter::with_partitions(
                &diff.router_partitions_src,
                &diff.router_partitions_dst,
            );

            // TODO use dynamic shards
            let range_mc = QueueShardRange {
                shard_ident: ShardIdent::MASTERCHAIN,
                from: diff.min_message,
                to: diff.max_message,
            };

            let range_shard = QueueShardRange {
                shard_ident: ShardIdent::new_full(0),
                from: diff.min_message,
                to: diff.max_message,
            };

            let ranges = vec![range_mc, range_shard];
            let queue_statistic = mq_adapter.get_statistics(0, &ranges)?;

            let mut diff_statistics = FastHashMap::default();
            diff_statistics.insert(0, queue_statistic.statistics().clone());

            let diff_statistic = DiffStatistics::new(
                top_shard_block.shard,
                diff.min_message,
                diff.max_message,
                diff_statistics,
                queue_statistic.shard_messages_count(),
            );

            diffs_info.insert(top_shard_block.shard, (partition_router, diff_statistic));
        }

        // get queue diff and check for pending internals
        let create_queue_diff_elapsed;
        let FinalizedMessagesReader {
            has_unprocessed_messages,
            queue_diff_with_msgs,
            reader_state,
            anchors_cache,
        } = {
            let histogram_create_queue_diff = HistogramGuard::begin_with_labels(
                "tycho_do_collate_create_queue_diff_time",
                &labels,
            );
            let finalize_message_reader_res =
                messages_reader.finalize(self.extra.executor.min_next_lt(), &diffs_info)?;
            create_queue_diff_elapsed = histogram_create_queue_diff.finish();
            finalize_message_reader_res
        };

        let (min_message, max_message) = {
            let messages = &queue_diff_with_msgs.messages;
            match messages.first_key_value().zip(messages.last_key_value()) {
                Some(((min, _), (max, _))) => (*min, *max),
                None => (
                    QueueKey::min_for_lt(self.state.collation_data.start_lt),
                    QueueKey::max_for_lt(self.state.collation_data.next_lt),
                ),
            }
        };

        let queue_diff = QueueDiffStuff::builder(
            self.state.shard_id,
            self.state.collation_data.block_id_short.seqno,
            &prev_hash,
        )
        .with_processed_to(reader_state.internals.get_min_processed_to_by_shards())
        .with_messages(
            &min_message,
            &max_message,
            queue_diff_with_msgs.messages.keys().map(|k| &k.hash),
        )
        .serialize();

        let queue_diff_hash = *queue_diff.hash();
        tracing::info!(target: tracing_targets::COLLATOR, queue_diff_hash = %queue_diff_hash);

        let queue_diff_messages_count = queue_diff_with_msgs.messages.len();

        // start async update queue task
        let update_queue_task = {
            let block_id_short = self.state.collation_data.block_id_short;
            let labels = labels.clone();
            let span = tracing::Span::current();
            move || {
                let _span = span.enter();

                let histogram = HistogramGuard::begin_with_labels(
                    "tycho_do_collate_build_statistics_time",
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
                    "tycho_do_collate_apply_queue_diff_time",
                    &labels,
                );

                mq_adapter.apply_diff(
                    queue_diff_with_msgs,
                    block_id_short,
                    &queue_diff_hash,
                    statistics,
                )?;
                let apply_queue_diff_elapsed = histogram.finish();

                Ok(apply_queue_diff_elapsed)
            }
        };

        Ok((
            FinalizeMessagesReaderResult {
                queue_diff,
                queue_diff_messages_count,
                has_unprocessed_messages,
                reader_state,
                anchors_cache,

                create_queue_diff_elapsed,
            },
            update_queue_task,
        ))
    }

    pub fn finalize_block(
        mut self,
        ctx: FinalizeBlockContext,
    ) -> Result<(FinalizeBlockResult, ExecuteResult)> {
        tracing::debug!(target: tracing_targets::COLLATOR, "finalize_block()");

        let FinalizeBlockContext {
            collation_session,
            wu_used_from_last_anchor,
            usage_tree,
            queue_diff,
            collator_config,
            processed_upto,
            diff_tail_len,
        } = ctx;

        let wu_params_finalize = self
            .state
            .collation_config
            .work_units_params
            .finalize
            .clone();

        let shard = self.state.collation_data.block_id_short.shard;

        let labels = &[("workchain", shard.workchain().to_string())];
        let histogram =
            HistogramGuard::begin_with_labels("tycho_collator_finalize_block_time", labels);

        let executor = self.extra.executor;

        // update shard accounts tree and prepare accounts blocks
        let mut global_libraries = executor.executor_params().state_libs.clone();

        let is_masterchain = shard.is_masterchain();
        let config_address = &self.state.mc_data.config.address;

        // Compute a masterchain block seqno which will reference this block.
        let ref_by_mc_seqno = if is_masterchain {
            // The block itself for the masterchain
            self.state.collation_data.block_id_short.seqno
        } else {
            // And the next masterchain block for shards
            self.state.mc_data.block_id.seqno + 1
        };

        let mut processed_accounts_res = Ok(Default::default());
        let mut build_account_blocks_elapsed = Duration::ZERO;
        let mut in_msgs_res = Ok(Default::default());
        let mut build_in_msgs_elapsed = Duration::ZERO;
        let mut out_msgs_res = Ok(Default::default());
        let mut build_out_msgs_elapsed = Duration::ZERO;
        let histogram_build_account_blocks_and_messages = HistogramGuard::begin_with_labels(
            "tycho_collator_finalize_build_account_blocks_and_msgs_time",
            labels,
        );
        rayon::scope(|s| {
            s.spawn(|_| {
                let histogram = HistogramGuard::begin_with_labels(
                    "tycho_collator_finalize_build_in_msgs_time",
                    labels,
                );
                in_msgs_res = Self::build_in_msgs(&self.state.collation_data.in_msgs);
                build_in_msgs_elapsed = histogram.finish();
            });
            s.spawn(|_| {
                let histogram = HistogramGuard::begin_with_labels(
                    "tycho_collator_finalize_build_out_msgs_time",
                    labels,
                );
                out_msgs_res = Self::build_out_msgs(&self.state.collation_data.out_msgs);
                build_out_msgs_elapsed = histogram.finish();
            });

            let histogram = HistogramGuard::begin_with_labels(
                "tycho_collator_finalize_build_account_blocks_time",
                labels,
            );

            processed_accounts_res = Self::build_accounts(
                executor,
                is_masterchain,
                config_address,
                &mut global_libraries,
            );
            build_account_blocks_elapsed = histogram.finish();
        });

        let build_account_blocks_and_messages_elased =
            histogram_build_account_blocks_and_messages.finish();

        let processed_accounts = processed_accounts_res?;
        self.state.collation_data.accounts_count = processed_accounts.accounts_len as u64;
        let in_msgs = in_msgs_res?;
        let out_msgs = out_msgs_res?;

        // TODO: update new_config_opt from hard fork
        // calc value flow
        let mut value_flow = self.state.collation_data.value_flow.clone();

        value_flow.imported = in_msgs.root_extra().value_imported.clone();
        value_flow.exported = out_msgs.root_extra().clone();
        value_flow.fees_collected = processed_accounts.account_blocks.root_extra().clone();
        value_flow
            .fees_collected
            .try_add_assign_tokens(in_msgs.root_extra().fees_collected)?;
        value_flow
            .fees_collected
            .try_add_assign(&value_flow.fees_imported)?;
        value_flow
            .fees_collected
            .try_add_assign(&value_flow.created)?;
        value_flow.to_next_block = processed_accounts
            .shard_accounts
            .root_extra()
            .balance
            .clone();

        if collator_config.check_value_flow {
            Self::check_value_flow(
                &value_flow,
                is_masterchain,
                &self.state.collation_data,
                &self.state.mc_data.config,
                &self.state.prev_shard_data,
                &in_msgs,
                &out_msgs,
                &processed_accounts,
            )?;
        }

        // build master state extra or get a ref to last applied master block
        // TODO: extract min_ref_mc_seqno from processed_upto info when we have many shards
        // collation_data.update_ref_min_mc_seqno(min_ref_mc_seqno);
        let build_mc_state_extra_elapsed;
        let (mc_state_extra, master_ref) = if is_masterchain {
            let histogram = HistogramGuard::begin_with_labels(
                "tycho_collator_finish_build_mc_state_extra_time",
                labels,
            );

            let prev_state = &self.state.prev_shard_data.observable_states()[0];
            let prev_processed_to_anchor = self
                .state
                .prev_shard_data
                .processed_upto()
                .get_min_externals_processed_to()?
                .0;
            let config_params =
                processed_accounts
                    .new_config_params
                    .map(|params| BlockchainConfig {
                        address: *config_address,
                        params,
                    });
            let (extra, min_ref_mc_seqno) = Self::create_mc_state_extra(
                &mut self.state.collation_data,
                config_params,
                prev_state,
                prev_processed_to_anchor,
                collator_config,
            )?;
            self.state
                .collation_data
                .update_ref_min_mc_seqno(min_ref_mc_seqno);

            build_mc_state_extra_elapsed = histogram.finish();
            (Some(extra), None)
        } else {
            build_mc_state_extra_elapsed = Duration::ZERO;
            (None, Some(self.state.mc_data.make_block_ref()))
        };

        // build block info
        let mut new_block_info = BlockInfo {
            version: 0,
            key_block: matches!(&mc_state_extra, Some(extra) if extra.after_key_block),
            shard: self.state.collation_data.block_id_short.shard,
            seqno: self.state.collation_data.block_id_short.seqno,
            gen_utime: self.state.collation_data.gen_utime,
            gen_utime_ms: self.state.collation_data.gen_utime_ms,
            start_lt: self.state.collation_data.start_lt,
            end_lt: self.state.collation_data.next_lt,
            gen_validator_list_hash_short: collation_session.collators().short_hash,
            gen_catchain_seqno: collation_session.seqno(),
            min_ref_mc_seqno: self.state.collation_data.min_ref_mc_seqno,
            prev_key_block_seqno: self.state.mc_data.prev_key_block_seqno,
            master_ref: master_ref.as_ref().map(Lazy::new).transpose()?,
            ..Default::default()
        };
        let prev_ref = self.state.prev_shard_data.get_blocks_ref()?;
        new_block_info.set_prev_ref(&prev_ref);

        // TODO: should set when slpit/merge logic implemented
        // info.after_merge = false;
        // info.before_split = false;
        // info.after_split = false;
        // info.want_split = false;
        // info.want_merge = false;

        let bc_global_version = self.state.mc_data.config.get_global_version()?;
        if bc_global_version
            .capabilities
            .contains(GlobalCapability::CapReportVersion)
        {
            new_block_info.set_gen_software(Some(bc_global_version));
        }

        let build_state_update_elapsed;
        let new_state_root;
        let total_validator_fees;
        let finalize_wu_total;
        let (state_update, new_observable_state) = {
            let histogram = HistogramGuard::begin_with_labels(
                "tycho_collator_finalize_build_state_update_time",
                labels,
            );

            let accounts_count = self.state.collation_data.accounts_count;
            let in_msgs_len = self.state.collation_data.in_msgs.len() as u64;
            let out_msgs_len = self.state.collation_data.out_msgs.len() as u64;

            finalize_wu_total = Self::calc_finalize_wu_total(
                accounts_count,
                in_msgs_len,
                out_msgs_len,
                wu_params_finalize,
            );

            tracing::debug!(target: tracing_targets::COLLATOR,
                "finalize_wu_total: {}, accounts_count: {}, in_msgs: {}, out_msgs: {} ",
                finalize_wu_total,
                accounts_count,
                in_msgs_len,
                out_msgs_len,
            );

            // compute total wu used from last anchor
            let new_wu_used_from_last_anchor = wu_used_from_last_anchor
                .saturating_add(self.extra.execute_result.prepare_msg_groups_wu.total_wu)
                .saturating_add(self.extra.execute_result.execute_groups_wu_total)
                .saturating_add(finalize_wu_total);

            tracing::info!(target: tracing_targets::COLLATOR,
                "wu_used_from_last_anchor: old={}, new={}, \
                prepare_msg_groups_wu_total={}, execute_groups_wu_total={}, finalize_wu_total={}",
                wu_used_from_last_anchor,
                new_wu_used_from_last_anchor,
                self.extra.execute_result.prepare_msg_groups_wu.total_wu,
                self.extra.execute_result.execute_groups_wu_total,
                finalize_wu_total,
            );

            // build new state
            let mut new_observable_state = Box::new(ShardStateUnsplit {
                global_id: self.state.mc_data.global_id,
                shard_ident: new_block_info.shard,
                seqno: new_block_info.seqno,
                vert_seqno: 0,
                gen_utime: new_block_info.gen_utime,
                gen_utime_ms: new_block_info.gen_utime_ms,
                gen_lt: new_block_info.end_lt,
                min_ref_mc_seqno: new_block_info.min_ref_mc_seqno,
                processed_upto: Lazy::new(&processed_upto.clone().try_into()?)?,
                before_split: new_block_info.before_split,
                accounts: Lazy::new(&processed_accounts.shard_accounts)?,
                overload_history: new_wu_used_from_last_anchor,
                underload_history: 0,
                total_balance: value_flow.to_next_block.clone(),
                total_validator_fees: self.state.prev_shard_data.total_validator_fees().clone(),
                libraries: Dict::new(),
                master_ref,
                custom: mc_state_extra.as_ref().map(Lazy::new).transpose()?,
            });

            new_observable_state
                .total_validator_fees
                .try_add_assign(&value_flow.fees_collected)?;
            new_observable_state
                .total_validator_fees
                .try_sub_assign(&value_flow.recovered)?;

            if is_masterchain {
                new_observable_state.libraries = global_libraries.clone();
            }

            // TODO: update config smc on hard fork

            new_state_root = CellBuilder::build_from(&new_observable_state)?;

            total_validator_fees = new_observable_state.total_validator_fees.clone();

            // calc merkle update
            let merkle_update = create_merkle_update(
                &shard,
                self.state.prev_shard_data.pure_state_root(),
                &new_state_root,
                &usage_tree,
            )?;

            build_state_update_elapsed = histogram.finish();
            (merkle_update, new_observable_state)
        };

        let build_block_elapsed;
        let (new_block, new_block_extra, new_mc_block_extra) = {
            let histogram = HistogramGuard::begin_with_labels(
                "tycho_collator_finalize_build_block_time",
                labels,
            );

            // calc block extra
            let mut new_block_extra = BlockExtra {
                in_msg_description: Lazy::new(&in_msgs)?,
                out_msg_description: Lazy::new(&out_msgs)?,
                account_blocks: Lazy::new(&processed_accounts.account_blocks)?,
                rand_seed: self.state.collation_data.rand_seed,
                created_by: self.state.collation_data.created_by,
                ..Default::default()
            };

            let new_mc_block_extra = if let Some(mc_state_extra) = &mc_state_extra {
                let new_mc_block_extra = McBlockExtra {
                    shards: mc_state_extra.shards.clone(),
                    fees: self.state.collation_data.shard_fees.clone(),
                    // TODO: Signatures for previous blocks
                    prev_block_signatures: Default::default(),
                    mint_msg: self
                        .state
                        .collation_data
                        .mint_msg
                        .as_ref()
                        .map(Lazy::new)
                        .transpose()?,
                    recover_create_msg: self
                        .state
                        .collation_data
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

                Some(new_mc_block_extra)
            } else {
                None
            };

            self.state.collation_data.diff_tail_len = diff_tail_len;

            // construct block
            let block = Block {
                global_id: self.state.mc_data.global_id,
                info: Lazy::new(&new_block_info)?,
                value_flow: Lazy::new(&value_flow)?,
                state_update: Lazy::new(&state_update)?,
                // do not use out msgs queue updates
                out_msg_queue_updates: OutMsgQueueUpdates {
                    diff_hash: *queue_diff.hash(),
                    tail_len: diff_tail_len,
                },
                extra: Lazy::new(&new_block_extra)?,
            };

            // TODO: Check (assert) whether the serialized block contains usage cells
            let root = CellBuilder::build_from(&block)?;

            let data = everscale_types::boc::Boc::encode_rayon(&root);
            let block_id = BlockId {
                shard: self.state.collation_data.block_id_short.shard,
                seqno: self.state.collation_data.block_id_short.seqno,
                root_hash: *root.repr_hash(),
                file_hash: Boc::file_hash_blake(&data),
            };

            build_block_elapsed = histogram.finish();

            let block = BlockStuff::from_block_and_root(&block_id, block, root, data.len());

            (
                WithArchiveData::new(block, data),
                new_block_extra,
                new_mc_block_extra,
            )
        };

        let new_block_id = *new_block.id();

        // log block debug info
        tracing::info!(target: tracing_targets::COLLATOR,
            "collated_block_info: {:?}",
            BlockDebugInfo {
                block_id: &new_block_id,
                block_info: &new_block_info,
                prev_ref: &prev_ref,
                state: &new_observable_state,
                processed_upto: &processed_upto,
                mc_top_shards: self.state.collation_data.shards(),
                mc_state_extra: mc_state_extra.as_ref(),
                merkle_update: &state_update,
                block_extra: &new_block_extra,
                mc_block_extra: new_mc_block_extra.as_ref(),
            },
        );

        let processed_to_anchor_id = processed_upto.get_min_externals_processed_to()?.0;

        let new_mc_data = match mc_state_extra {
            None => None,
            Some(extra) => {
                let prev_key_block_seqno = if extra.after_key_block {
                    new_block_id.seqno
                } else if let Some(block_ref) = &extra.last_key_block {
                    block_ref.seqno
                } else {
                    0
                };

                let shards = extra.shards.as_vec()?;

                let top_processed_to_anchor = detect_top_processed_to_anchor(
                    shards.iter().map(|(_, d)| *d),
                    processed_to_anchor_id,
                );

                let mut shards_processed_to = FastHashMap::default();

                for (shard_id, _) in shards.iter() {
                    // Extract processed information for updated shards
                    let shard_processed_to = self
                        .state
                        .collation_data
                        .top_shard_blocks
                        .iter()
                        .find(|top_block_info| top_block_info.block_id.shard == *shard_id)
                        .map(|top_block_info| top_block_info.processed_to.clone())
                        .or_else(|| {
                            self.state
                                .mc_data
                                .shards_processed_to
                                .get(shard_id)
                                .cloned()
                        });

                    if let Some(value) = shard_processed_to {
                        shards_processed_to.insert(*shard_id, value);
                    }
                }

                Some(Arc::new(McData {
                    global_id: new_block.as_ref().global_id,
                    block_id: *new_block.id(),

                    prev_key_block_seqno,
                    gen_lt: new_block_info.end_lt,
                    gen_chain_time: self.state.collation_data.get_gen_chain_time(),
                    libraries: global_libraries,
                    total_validator_fees,

                    global_balance: extra.global_balance.clone(),
                    shards,
                    config: extra.config,
                    validator_info: extra.validator_info,
                    consensus_info: extra.consensus_info,

                    processed_upto: processed_upto.clone(),
                    top_processed_to_anchor,
                    ref_mc_state_handle: self.state.prev_shard_data.ref_mc_state_handle().clone(),
                    shards_processed_to,
                }))
            }
        };

        // TODO: build collated data from collation_data.shard_top_block_descriptors
        let collated_data = vec![];

        let block_candidate = Box::new(BlockCandidate {
            ref_by_mc_seqno,
            block: new_block,
            is_key_block: new_block_info.key_block,
            prev_blocks_ids: self.state.prev_shard_data.blocks_ids().clone(),
            top_shard_blocks_ids: self
                .state
                .collation_data
                .top_shard_blocks
                .iter()
                .map(|b| b.block_id)
                .collect(),
            collated_data,
            collated_file_hash: HashBytes::ZERO,
            chain_time: self.state.collation_data.get_gen_chain_time(),
            processed_to_anchor_id,
            value_flow,
            created_by: self.state.collation_data.created_by,
            queue_diff_aug: queue_diff.build(&new_block_id),
            consensus_info: new_mc_data.as_ref().map_or_else(
                || self.state.mc_data.consensus_info,
                |mcd| mcd.consensus_info,
            ),
            processed_upto,
        });

        let total_elapsed = histogram.finish();

        tracing::debug!(
            target: tracing_targets::COLLATOR,
            total = %format_duration(total_elapsed),
            parallel_build_accounts_and_msgs = %format_duration(build_account_blocks_and_messages_elased),
            only_build_account_blocks = %format_duration(build_account_blocks_elapsed),
            only_build_in_msgs = %format_duration(build_in_msgs_elapsed),
            only_build_out_msgs = %format_duration(build_out_msgs_elapsed),
            build_mc_state_extra = %format_duration(build_mc_state_extra_elapsed),
            build_state_update = %format_duration(build_state_update_elapsed),
            build_block = %format_duration(build_block_elapsed),
            "finalize block timings"
        );

        Ok((
            FinalizeBlockResult {
                collation_data: self.state.collation_data,
                block_candidate,
                mc_data: new_mc_data,
                new_state_root,
                new_observable_state,
                finalize_wu_total,
                old_mc_data: self.state.mc_data,
                collation_config: self.state.collation_config,
            },
            self.extra.execute_result,
        ))
    }

    fn calc_finalize_wu_total(
        accounts_count: u64,
        in_msgs_len: u64,
        out_msgs_len: u64,
        wu_params_finalize: WorkUnitsParamsFinalize,
    ) -> u64 {
        let WorkUnitsParamsFinalize {
            build_transactions,
            build_in_msg,
            build_accounts,
            build_out_msg,
            serialize_min,
            serialize_accounts,
            serialize_msg,
            state_update_min,
            state_update_accounts,
            state_update_msg,
            ..
        } = wu_params_finalize;

        let accounts_count_logarithm = accounts_count.checked_ilog2().unwrap_or_default() as u64;
        let build = accounts_count
            .saturating_mul(accounts_count_logarithm)
            .saturating_mul(build_accounts as u64);
        let build_in_msg = in_msgs_len
            .saturating_mul(in_msgs_len.checked_ilog2().unwrap_or_default() as u64)
            .saturating_mul(build_in_msg as u64);
        let build_out_msg = out_msgs_len
            .saturating_mul(out_msgs_len.checked_ilog2().unwrap_or_default() as u64)
            .saturating_mul(build_out_msg as u64);
        let build = build.saturating_add(
            std::cmp::max(out_msgs_len, in_msgs_len).saturating_mul(build_transactions as u64),
        );
        let build = std::cmp::max(build, build_in_msg);
        let build = std::cmp::max(build, build_out_msg);

        let merkle_calc = std::cmp::max(
            state_update_min as u64,
            accounts_count
                .saturating_mul(accounts_count_logarithm)
                .saturating_mul(state_update_accounts as u64)
                .saturating_add(out_msgs_len.saturating_mul(state_update_msg as u64)),
        );

        let serialize = std::cmp::max(
            serialize_min as u64,
            accounts_count.saturating_mul(serialize_accounts as u64),
        )
        .saturating_add((in_msgs_len + out_msgs_len).saturating_mul(serialize_msg as u64));

        build.saturating_add(merkle_calc).saturating_add(serialize)
    }

    fn create_mc_state_extra(
        collation_data: &mut BlockCollationData,
        config_params: Option<BlockchainConfig>,
        prev_state: &ShardStateStuff,
        prev_processed_to_anchor: u32,
        collator_config: Arc<CollatorConfig>,
    ) -> Result<(McStateExtra, u32)> {
        // 1. update config params and detect key block
        let prev_state_extra = prev_state.state_extra()?;
        let prev_config = &prev_state_extra.config;
        let (config, mut is_key_block) = if let Some(new_config) = config_params {
            let is_key_block = &new_config != prev_config;

            if is_key_block && collator_config.validate_config {
                new_config
                    .validate_params()
                    .context("invalid blockchain config")?;
            }

            (new_config, is_key_block)
        } else {
            (prev_config.clone(), false)
        };

        // 1.10. update genesis round and time from the mempool global config if present and higher
        let mut consensus_info = prev_state_extra.consensus_info;

        if let Some(mp_cfg_override) = &collation_data.mempool_config_override {
            if (mp_cfg_override.genesis_info).overrides(&consensus_info.genesis_info) {
                consensus_info.genesis_info = mp_cfg_override.genesis_info;

                is_key_block = true;
            }
        }

        // 2. update shard_hashes and shard_fees
        let collation_config = config.get_collation_config()?;
        let workchains = config.get_workchains()?;
        // check if need to start new collation session for shards

        // NOTE: currently we do not implement the logic of node rotation inside one set,
        //      so we will not use session liftime and will check only for a key block
        // let current_gen_utime = collation_data.gen_utime;
        // let prev_gen_utime = prev_state.state().gen_utime;
        // let update_shard_cc = {
        //     let lifetimes = current_gen_utime / collation_config.shard_catchain_lifetime;
        //     let prev_lifetimes = prev_gen_utime / collation_config.shard_catchain_lifetime;
        //     is_key_block || (lifetimes > prev_lifetimes)
        // };
        let update_shard_cc = is_key_block;

        let min_ref_mc_seqno =
            Self::update_shard_config(collation_data, &workchains, update_shard_cc)?;

        // 3. save new shard_hashes
        let shards_iter = collation_data
            .get_shards()?
            .iter()
            .map(|(k, v)| (k, v.as_ref()));
        let shards = ShardHashes::from_shards(shards_iter)?;

        // 4. check extension flags
        // prev_state_extra.flags is checked in the McStateExtra::load_from

        // 5. update validator_info and consensus_info
        let consensus_config = config.params.get_consensus_config()?;
        let mut validator_info = None;
        if is_key_block {
            // check if validator set changed by the cells hash
            // NOTE: We intentionaly use current validator set from previous config
            //       instead of just using the `prev_vset`.
            let prev_vset = prev_config.get_current_validator_set_raw()?;
            let current_vset = config.get_current_validator_set_raw()?;
            if current_vset.repr_hash() != prev_vset.repr_hash() {
                // TODO: For now we cannot update `consensus_info` when just the
                // `shuffle_mc_validators` changes, because in that case we
                // still need to compute some round when this setting will apply.
                let prev_shuffle_mc_validators =
                    prev_config.get_collation_config()?.shuffle_mc_validators;

                // calc next session update round
                let session_update_round = {
                    // `prev_processed_to_anchor` is a round in the ending session, after which
                    // mempool can create `max_consensus_lag` rounds in DAG until it stops to wait
                    let mempool_last_round_to_create =
                        prev_processed_to_anchor + consensus_config.max_consensus_lag_rounds as u32;
                    let session_last_round = if consensus_info.prev_vset_switch_round
                        > consensus_info.genesis_info.start_round
                    {
                        // consensus session cannot abort until reaching full history amount of rounds,
                        // because mempool has to re-validate historical points during sync,
                        // and can hold just one previous vset to check peer authority
                        let full_history_round = consensus_info.prev_vset_switch_round
                            + consensus_config.max_total_rounds();
                        mempool_last_round_to_create.max(full_history_round)
                    } else {
                        // mempool history does not span across genesis,
                        // so can change vset earlier without invalidating points
                        mempool_last_round_to_create
                    };
                    // `+1` because it will be the first mempool round in the new session
                    session_last_round + 1
                };

                // currently we simultaneously update session_seqno in collation and session_update_round in consesus
                if consensus_info.vset_switch_round < session_update_round {
                    consensus_info.prev_vset_switch_round = consensus_info.vset_switch_round;
                    consensus_info.vset_switch_round = session_update_round;
                    consensus_info.prev_shuffle_mc_validators = prev_shuffle_mc_validators;
                }

                // calculate next validator subset and hash
                let current_vset = current_vset.parse::<ValidatorSet>()?;
                let (_, validator_list_hash_short) = current_vset.compute_mc_subset(
                    session_update_round,
                    collation_config.shuffle_mc_validators,
                ).ok_or_else(|| anyhow!(
                    "Error calculating subset of validators for next session (shard_id = {}, session_seqno = {})",
                    ShardIdent::MASTERCHAIN,
                    session_update_round,
                ))?;

                validator_info = Some(ValidatorInfo {
                    validator_list_hash_short,
                    // TODO: rename field in types
                    catchain_seqno: session_update_round,
                    nx_cc_updated: true,
                });
            }
        }
        let validator_info = validator_info.unwrap_or(ValidatorInfo {
            nx_cc_updated: false,
            ..prev_state_extra.validator_info
        });

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
            prev_state.block_id().seqno,
            KeyMaxLt {
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
            consensus_info,
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
        collation_data: &mut BlockCollationData,
        _wc_set: &Dict<i32, WorkchainDescription>,
        _update_cc: bool,
    ) -> Result<u32> {
        // TODO: here should be the split/merge logic, refer to old node impl

        // STUB: just do nothing for now: no split/merge, no session rotation
        let mut min_ref_mc_seqno = u32::MAX;
        for shard_descr in collation_data.get_shards_mut()?.values_mut() {
            min_ref_mc_seqno = std::cmp::min(min_ref_mc_seqno, shard_descr.min_ref_mc_seqno);
        }

        Ok(min_ref_mc_seqno)
    }

    fn build_accounts(
        executor: MessagesExecutor,
        is_masterchain: bool,
        config_address: &HashBytes,
        global_libraries: &mut Dict<HashBytes, LibDescr>,
    ) -> Result<ProcessedAccounts> {
        let mut account_blocks = BTreeMap::new();
        let mut new_config_params = None;
        let (updated_accounts, shard_accounts) = executor.into_accounts_cache_raw();
        let mut shard_accounts = RelaxedAugDict::from_full(&shard_accounts);

        for updated_account in updated_accounts {
            if updated_account.transactions.is_empty() {
                continue;
            }

            if updated_account.exists {
                shard_accounts.set_any(
                    &updated_account.account_addr,
                    &DepthBalanceInfo {
                        split_depth: 0, // NOTE: will need to set when we implement accounts split/merge logic
                        balance: updated_account.balance.clone(),
                    },
                    &updated_account.shard_account,
                )?;
            } else {
                shard_accounts.remove(&updated_account.account_addr)?;
            }

            if is_masterchain {
                if &updated_account.account_addr == config_address {
                    if let Some(Account {
                        state:
                            AccountState::Active(StateInit {
                                data: Some(data), ..
                            }),
                        ..
                    }) = updated_account.shard_account.load_account()?
                    {
                        new_config_params = Some(data.parse::<BlockchainConfigParams>()?);
                    }
                }

                updated_account.update_public_libraries(global_libraries)?;
            }

            let account_block = AccountBlock {
                state_update: updated_account.build_hash_update(),
                account: updated_account.account_addr,
                transactions: AugDict::try_from_btree(&updated_account.transactions)?,
            };

            account_blocks.insert(updated_account.account_addr, account_block);
        }

        let accounts_len = account_blocks.len();

        // TODO: Somehow consume accounts inside an iterator
        let account_blocks = RelaxedAugDict::try_from_sorted_iter_any(
            account_blocks
                .iter()
                .map(|(k, v)| (k, v.transactions.root_extra(), v as &dyn Store)),
        )?;

        Ok(ProcessedAccounts {
            account_blocks: account_blocks.build()?,
            shard_accounts: shard_accounts.build()?,
            new_config_params,
            accounts_len,
        })
    }

    fn build_in_msgs(items: &BTreeMap<HashBytes, PreparedInMsg>) -> Result<InMsgDescr> {
        RelaxedAugDict::try_from_sorted_iter_lazy(
            items
                .iter()
                .map(|(msg_id, msg)| (msg_id, &msg.import_fees, &msg.in_msg)),
        )?
        .build()
        .map_err(Into::into)
    }

    fn build_out_msgs(items: &BTreeMap<HashBytes, PreparedOutMsg>) -> Result<OutMsgDescr> {
        RelaxedAugDict::try_from_sorted_iter_lazy(
            items
                .iter()
                .map(|(msg_id, msg)| (msg_id, &msg.exported_value, &msg.out_msg)),
        )?
        .build()
        .map_err(Into::into)
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

    #[allow(clippy::too_many_arguments)]
    fn check_value_flow(
        value_flow: &ValueFlow,
        is_masterchain: bool,
        collation_data: &BlockCollationData,
        config: &BlockchainConfig,
        prev_shard_data: &PrevData,
        in_msgs: &InMsgDescr,
        out_msgs: &OutMsgDescr,
        processed_accounts: &ProcessedAccounts,
    ) -> Result<()> {
        if !is_masterchain && value_flow.minted != CurrencyCollection::default() {
            anyhow::bail!(
                "ValueFlow of block {} \
                is invalid (non-zero minted value in a non-masterchain block)",
                collation_data.block_id_short
            )
        }
        if !is_masterchain && value_flow.recovered != CurrencyCollection::default() {
            anyhow::bail!(
                "ValueFlow of block {} \
                is invalid (non-zero recovered value in a non-masterchain block)",
                collation_data.block_id_short
            )
        }
        if value_flow.recovered != CurrencyCollection::default()
            && collation_data.recover_create_msg.is_none()
        {
            anyhow::bail!(
                "ValueFlow of block {} \
                has a non-zero recovered fees value, but there is no recovery InMsg",
                collation_data.block_id_short
            )
        }
        if value_flow.recovered == CurrencyCollection::default()
            && collation_data.recover_create_msg.is_some()
        {
            anyhow::bail!(
                "ValueFlow of block {} \
                has a zero recovered fees value, but there is a recovery InMsg",
                collation_data.block_id_short
            )
        }
        if value_flow.minted != CurrencyCollection::default() && collation_data.mint_msg.is_none() {
            anyhow::bail!(
                "ValueFlow of block {} \
                has a non-zero minted value, but there is no mint InMsg",
                collation_data.block_id_short
            )
        }
        if value_flow.minted == CurrencyCollection::default() && collation_data.mint_msg.is_some() {
            anyhow::bail!(
                "ValueFlow of block {} \
                has a zero minted value, but there is a mint InMsg",
                collation_data.block_id_short
            )
        }

        let mut create_fee = config
            .get_block_creation_reward(is_masterchain)
            .unwrap_or_default();
        create_fee >>= collation_data.block_id_short.shard.prefix_len() as u8;
        if value_flow.created.tokens != create_fee {
            anyhow::bail!(
                "ValueFlow of block {} declares block creation fee {}, \
                but the current configuration expects it to be {}",
                collation_data.block_id_short,
                value_flow.created.tokens,
                create_fee
            )
        }
        if value_flow.fees_imported != CurrencyCollection::default() && !is_masterchain {
            anyhow::bail!(
                "ValueFlow of block {} \
                is invalid (non-zero fees_imported in a non-masterchain block)",
                collation_data.block_id_short
            )
        }

        let from_prev_block = prev_shard_data
            .observable_accounts()
            .root_extra()
            .balance
            .clone();
        if from_prev_block != value_flow.from_prev_block {
            anyhow::bail!(
                "ValueFlow for {} declares from_prev_blk={} \
                but the sum over all accounts present in the previous state is {}",
                collation_data.block_id_short,
                value_flow.from_prev_block.tokens,
                from_prev_block.tokens
            )
        }
        let to_next_block = processed_accounts
            .shard_accounts
            .root_extra()
            .balance
            .clone();
        if to_next_block != value_flow.to_next_block {
            anyhow::bail!(
                "ValueFlow for {} declares to_next_blk={} but the sum over all accounts \
                present in the new state is {}",
                collation_data.block_id_short,
                value_flow.to_next_block.tokens,
                to_next_block.tokens
            )
        }

        let imported = in_msgs.root_extra().value_imported.clone();
        if imported != value_flow.imported {
            anyhow::bail!(
                "ValueFlow for {} declares imported={} but the sum over all inbound messages \
                listed in InMsgDescr is {}",
                collation_data.block_id_short,
                value_flow.imported.tokens,
                imported.tokens
            );
        }
        let exported = out_msgs.root_extra().clone();
        if exported != value_flow.exported {
            anyhow::bail!(
                "ValueFlow for {} declares exported={} but the sum over all outbound messages \
                listed in OutMsgDescr is {}",
                collation_data.block_id_short,
                value_flow.exported.tokens,
                exported.tokens
            )
        }

        let transaction_fees = processed_accounts.account_blocks.root_extra().clone();
        let in_msgs_fees = in_msgs.root_extra().fees_collected;
        let mut expected_fees = transaction_fees.clone();
        expected_fees.try_add_assign_tokens(in_msgs_fees)?;
        expected_fees.try_add_assign_tokens(value_flow.fees_imported.tokens)?;
        expected_fees.try_add_assign_tokens(value_flow.created.tokens)?;

        if value_flow.fees_collected != expected_fees {
            anyhow::bail!("ValueFlow for {} declares fees_collected={} but the total message import fees are {}, \
                the total transaction fees are {}, creation fee for this block is {} \
                and the total imported fees from shards are {} with a total of {}", collation_data.block_id_short,
                    value_flow.fees_collected.tokens, in_msgs_fees, transaction_fees.tokens,
                    value_flow.created.tokens, value_flow.fees_imported.tokens, expected_fees.tokens)
        }
        Ok(())
    }
}

#[derive(Default)]
struct ProcessedAccounts {
    account_blocks: AccountBlocks,
    shard_accounts: ShardAccounts,
    new_config_params: Option<BlockchainConfigParams>,
    accounts_len: usize,
}

fn create_merkle_update(
    shard_id: &ShardIdent,
    old_state_root: &Cell,
    new_state_root: &Cell,
    usage_tree: &UsageTree,
) -> Result<MerkleUpdate> {
    let labels = [("workchain", shard_id.workchain().to_string())];
    let histogram =
        HistogramGuard::begin_with_labels("tycho_collator_create_merkle_update_time", &labels);

    let merkle_update_builder =
        MerkleUpdate::create(old_state_root.as_ref(), new_state_root.as_ref(), usage_tree);
    let state_update = merkle_update_builder.build()?;

    let elapsed = histogram.finish();

    tracing::debug!(
        target: tracing_targets::COLLATOR,
        elapsed = %humantime::format_duration(elapsed),
        "merkle update created"
    );

    Ok(state_update)
}
