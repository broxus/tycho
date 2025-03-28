use std::collections::{btree_map, BTreeMap};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use everscale_types::cell::HashBytes;
use everscale_types::models::{MsgsExecutionParams, ShardIdent};
use tycho_block_util::queue::{QueueKey, QueuePartitionIdx};
use tycho_util::{FastHashMap, FastHashSet};

use self::externals_reader::*;
use self::internals_reader::*;
use self::new_messages::*;
pub(super) use self::reader_state::*;
use super::error::CollatorError;
use super::messages_buffer::{DisplayMessageGroup, MessageGroup, MessagesBufferLimits};
use super::types::{AnchorsCache, CumulativeStatistics, MsgsExecutionParamsExtension};
use crate::collator::messages_buffer::DebugMessageGroup;
use crate::internal_queue::types::{
    DiffStatistics, InternalMessageValue, PartitionRouter, QueueDiffWithMessages, QueueStatistics,
};
use crate::queue_adapter::MessageQueueAdapter;
use crate::tracing_targets;
use crate::types::processed_upto::{BlockSeqno, Lt};
use crate::types::{DebugIter, IntAdrExt, ProcessedTo};

mod externals_reader;
mod internals_reader;
mod new_messages;
mod reader_state;

#[cfg(test)]
#[path = "../tests/messages_reader_tests.rs"]
pub(super) mod tests;

pub(super) struct FinalizedMessagesReader<V: InternalMessageValue> {
    pub has_unprocessed_messages: bool,
    pub reader_state: ReaderState,
    pub anchors_cache: AnchorsCache,
    pub queue_diff_with_msgs: QueueDiffWithMessages<V>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum GetNextMessageGroupMode {
    Continue,
    Refill,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MessagesReaderStage {
    FinishPreviousExternals,
    ExistingAndExternals,
    FinishCurrentExternals,
    ExternalsAndNew,
}

pub(super) struct MessagesReader<V: InternalMessageValue> {
    for_shard_id: ShardIdent,

    msgs_exec_params: Arc<MsgsExecutionParams>,

    /// Collect separate metrics by partitions
    metrics_by_partitions: MessagesReaderMetricsByPartitions,

    new_messages: NewMessagesState<V>,

    externals_reader: ExternalsReader,
    internals_partition_readers: BTreeMap<QueuePartitionIdx, InternalsPartitionReader<V>>,

    readers_stages: BTreeMap<QueuePartitionIdx, MessagesReaderStage>,
    internal_queue_statistics: CumulativeStatistics,
}

#[derive(Default)]
pub(super) struct MessagesReaderContext {
    pub for_shard_id: ShardIdent,
    pub block_seqno: BlockSeqno,
    pub next_chain_time: u64,
    pub msgs_exec_params: Arc<MsgsExecutionParams>,
    pub mc_state_gen_lt: Lt,
    pub prev_state_gen_lt: Lt,
    pub mc_top_shards_end_lts: Vec<(ShardIdent, Lt)>,
    pub all_shards_processed_to: FastHashMap<ShardIdent, ProcessedTo>,
    pub reader_state: ReaderState,
    pub anchors_cache: AnchorsCache,
    pub is_first_block_after_prev_master: bool,
}

impl<V: InternalMessageValue> MessagesReader<V> {
    pub fn new(
        cx: MessagesReaderContext,
        mq_adapter: Arc<dyn MessageQueueAdapter<V>>,
    ) -> Result<Self> {
        let slots_fractions = cx.msgs_exec_params.group_slots_fractions()?;

        // metrics: messages exec params
        metrics::gauge!("tycho_do_collate_msgs_exec_params_buffer_limit")
            .set(cx.msgs_exec_params.buffer_limit as f64);
        metrics::gauge!("tycho_do_collate_msgs_exec_params_group_limit")
            .set(cx.msgs_exec_params.group_limit as f64);
        metrics::gauge!("tycho_do_collate_msgs_exec_params_group_vert_size")
            .set(cx.msgs_exec_params.group_vert_size as f64);
        for (par_id, par_fraction) in &slots_fractions {
            let labels = [("par_id", par_id.to_string())];
            metrics::gauge!(
                "tycho_do_collate_msgs_exec_params_group_slots_fractions",
                &labels
            )
            .set(*par_fraction as f64);
        }
        metrics::gauge!("tycho_do_collate_msgs_exec_params_externals_expire_timeout")
            .set(cx.msgs_exec_params.externals_expire_timeout as f64);
        metrics::gauge!("tycho_do_collate_msgs_exec_params_open_ranges_limit")
            .set(cx.msgs_exec_params.open_ranges_limit as f64);
        metrics::gauge!("tycho_do_collate_msgs_exec_params_par_0_ext_msgs_count_limit")
            .set(cx.msgs_exec_params.par_0_ext_msgs_count_limit as f64);
        metrics::gauge!("tycho_do_collate_msgs_exec_params_par_0_int_msgs_count_limit")
            .set(cx.msgs_exec_params.par_0_int_msgs_count_limit as f64);

        // group limits by msgs kinds
        let msgs_buffer_max_count = cx.msgs_exec_params.buffer_limit as usize;
        let group_vert_size = (cx.msgs_exec_params.group_vert_size as usize).max(1);
        let group_limit = cx.msgs_exec_params.group_limit as usize;

        let mut internals_buffer_limits_by_partitions =
            BTreeMap::<QueuePartitionIdx, MessagesBufferLimits>::new();
        let mut externals_buffer_limits_by_partitions =
            BTreeMap::<QueuePartitionIdx, MessagesBufferLimits>::new();

        // TODO: msgs-v3: should create partitions 1+ only when exist in current processed_upto

        // internals: normal partition 0: 80% of `group_limit`, but min 1
        let par_0_slots_fraction = slots_fractions.get(&0).cloned().unwrap() as usize;
        internals_buffer_limits_by_partitions.insert(0, MessagesBufferLimits {
            max_count: msgs_buffer_max_count,
            slots_count: group_limit
                .saturating_mul(par_0_slots_fraction)
                .saturating_div(100)
                .max(1),
            slot_vert_size: group_vert_size,
        });
        // externals: normal partition 0: 100%, but min 2, vert size +1
        externals_buffer_limits_by_partitions.insert(0, MessagesBufferLimits {
            max_count: msgs_buffer_max_count,
            slots_count: group_limit.saturating_mul(100).saturating_div(100).max(2),
            slot_vert_size: group_vert_size + 1,
        });

        // internals: low-priority partition 1: 10%, but min 1
        let par_1_slots_fraction = slots_fractions.get(&1).cloned().unwrap() as usize;
        internals_buffer_limits_by_partitions.insert(1, MessagesBufferLimits {
            max_count: msgs_buffer_max_count,
            slots_count: group_limit
                .saturating_mul(par_1_slots_fraction)
                .saturating_div(100)
                .max(1),
            slot_vert_size: group_vert_size,
        });
        // externals: low-priority partition 1: equal to internals, vert size +1
        {
            let int_buffer_limits = internals_buffer_limits_by_partitions.get(&1).unwrap();
            externals_buffer_limits_by_partitions.insert(1, MessagesBufferLimits {
                max_count: msgs_buffer_max_count,
                slots_count: int_buffer_limits.slots_count,
                slot_vert_size: int_buffer_limits.slot_vert_size + 1,
            });
        }

        // metrics: buffer limits
        for (par_id, buffer_limits) in &internals_buffer_limits_by_partitions {
            let labels = [("par_id", par_id.to_string())];
            metrics::gauge!("tycho_do_collate_int_buffer_limits_max_count", &labels)
                .set(buffer_limits.max_count as f64);
            metrics::gauge!("tycho_do_collate_int_buffer_limits_slots_count", &labels)
                .set(buffer_limits.slots_count as f64);
            metrics::gauge!("tycho_do_collate_int_buffer_limits_slot_vert_size", &labels)
                .set(buffer_limits.slot_vert_size as f64);
        }
        for (par_id, buffer_limits) in &externals_buffer_limits_by_partitions {
            let labels = [("par_id", par_id.to_string())];
            metrics::gauge!("tycho_do_collate_ext_buffer_limits_max_count", &labels)
                .set(buffer_limits.max_count as f64);
            metrics::gauge!("tycho_do_collate_ext_buffer_limits_slots_count", &labels)
                .set(buffer_limits.slots_count as f64);
            metrics::gauge!("tycho_do_collate_ext_buffer_limits_slot_vert_size", &labels)
                .set(buffer_limits.slot_vert_size as f64);
        }

        // TODO: msgs-v3: remove if we do not need this field
        let _msg_group_max_limits = MessagesBufferLimits {
            max_count: msgs_buffer_max_count,
            slots_count: externals_buffer_limits_by_partitions
                .values()
                .map(|l| l.slots_count)
                .sum(),
            slot_vert_size: group_vert_size,
        };

        // create externals reader
        let externals_reader = ExternalsReader::new(
            cx.for_shard_id,
            cx.block_seqno,
            cx.next_chain_time,
            cx.msgs_exec_params.clone(),
            externals_buffer_limits_by_partitions.clone(),
            cx.anchors_cache,
            cx.reader_state.externals,
        );

        let mut cumulative_statistics = if cx.is_first_block_after_prev_master {
            // TODO use dynamic partitions
            let partitions = vec![0, 1].into_iter().collect();
            let mut cumulative_statistics = CumulativeStatistics::new(cx.all_shards_processed_to);
            cumulative_statistics.load(
                mq_adapter.clone(),
                &cx.for_shard_id,
                &partitions,
                cx.prev_state_gen_lt,
                cx.mc_state_gen_lt,
                &cx.mc_top_shards_end_lts.iter().copied().collect(),
            )?;
            cumulative_statistics
        } else {
            cx.reader_state
                .internals
                .cumulative_statistics
                .expect("cumulative statistics should exist")
        };

        let mut new_messages = NewMessagesState::new(cx.for_shard_id);

        new_messages.init_partition_router(
            1,
            &cumulative_statistics.result(),
            cx.msgs_exec_params.par_0_int_msgs_count_limit as u64,
        );

        let mut res = Self {
            for_shard_id: cx.for_shard_id,

            msgs_exec_params: cx.msgs_exec_params.clone(),

            metrics_by_partitions: Default::default(),

            new_messages,

            externals_reader,
            internals_partition_readers: Default::default(),

            readers_stages: Default::default(),
            internal_queue_statistics: cumulative_statistics,
        };

        // define the initial reader stage
        let initial_reader_stage = MessagesReaderStage::ExistingAndExternals;

        // create internals readers by partitions
        let mut partition_reader_states = cx.reader_state.internals.partitions;

        // normal partition 0
        let target_limits = internals_buffer_limits_by_partitions.remove(&0).unwrap();
        let max_limits = {
            let ext_limits = externals_buffer_limits_by_partitions.remove(&0).unwrap();
            MessagesBufferLimits {
                max_count: msgs_buffer_max_count,
                slots_count: ext_limits.slots_count,
                slot_vert_size: target_limits.slot_vert_size,
            }
        };
        let par_reader_state = partition_reader_states.remove(&0).unwrap_or_default();
        let par_reader = InternalsPartitionReader::new(
            InternalsPartitionReaderContext {
                partition_id: 0,
                for_shard_id: cx.for_shard_id,
                block_seqno: cx.block_seqno,
                target_limits,
                max_limits,
                msgs_exec_params: cx.msgs_exec_params.clone(),
                mc_state_gen_lt: cx.mc_state_gen_lt,
                prev_state_gen_lt: cx.prev_state_gen_lt,
                mc_top_shards_end_lts: cx.mc_top_shards_end_lts.clone(),
                reader_state: par_reader_state,
            },
            mq_adapter.clone(),
        )?;
        res.internals_partition_readers.insert(0, par_reader);
        res.readers_stages.insert(0, initial_reader_stage);

        // low-priority partition 1
        let target_limits = internals_buffer_limits_by_partitions.remove(&1).unwrap();
        let max_limits = {
            let ext_limits = externals_buffer_limits_by_partitions.remove(&1).unwrap();
            MessagesBufferLimits {
                max_count: msgs_buffer_max_count,
                slots_count: ext_limits.slots_count,
                slot_vert_size: target_limits.slot_vert_size,
            }
        };
        let par_reader_state = partition_reader_states.remove(&1).unwrap_or_default();

        let par_reader = InternalsPartitionReader::new(
            InternalsPartitionReaderContext {
                partition_id: 1,
                for_shard_id: cx.for_shard_id,
                block_seqno: cx.block_seqno,
                target_limits,
                max_limits,
                msgs_exec_params: cx.msgs_exec_params.clone(),
                mc_state_gen_lt: cx.mc_state_gen_lt,
                prev_state_gen_lt: cx.prev_state_gen_lt,
                mc_top_shards_end_lts: cx.mc_top_shards_end_lts,
                reader_state: par_reader_state,
            },
            mq_adapter,
        )?;
        res.internals_partition_readers.insert(1, par_reader);
        res.readers_stages.insert(1, initial_reader_stage);

        // get full statistics from partition 1 and init partition router in new messages state
        // let par_1_all_ranges_msgs_stats = res
        //     .internals_partition_readers
        //     .get(&1)
        //     .unwrap()
        //     .range_readers()
        //     .values()
        //     .filter_map(|r| r.reader_state.msgs_stats.as_ref());

        tracing::debug!(target: tracing_targets::COLLATOR,
            readers_stages = ?res.readers_stages,
            externals_all_ranges_read_and_collected = res.externals_reader.all_ranges_read_and_collected(),
            internals_all_read_existing_messages_collected = ?DebugIter(res
                .internals_partition_readers
                .iter()
                .map(|(par_id, par)| (par_id, par.all_read_existing_messages_collected()))),
            "messages reader created",
        );

        Ok(res)
    }

    pub fn reset_read_state(&mut self) {
        // reset metrics
        self.metrics_by_partitions = Default::default();

        // define the initial reader stage
        let initial_reader_stage = MessagesReaderStage::ExistingAndExternals;

        // reset internals reader stages
        for (_, par_reader_stage) in self.readers_stages.iter_mut() {
            *par_reader_stage = initial_reader_stage;
        }

        // reset internals readers
        for (_, par) in self.internals_partition_readers.iter_mut() {
            par.reset_read_state();
        }

        // reset externals reader
        self.externals_reader.reset_read_state();

        tracing::debug!(target: tracing_targets::COLLATOR,
            readers_stages = ?self.readers_stages,
            externals_all_ranges_read_and_collected = self.externals_reader.all_ranges_read_and_collected(),
            internals_all_read_existing_messages_collected = ?DebugIter(self
                .internals_partition_readers
                .iter()
                .map(|(par_id, par)| (par_id, par.all_read_existing_messages_collected()))),
            "messages reader state was reset",
        );
    }

    pub fn check_has_pending_internals_in_iterators(&mut self) -> Result<bool> {
        for (_, par_reader) in self.internals_partition_readers.iter_mut() {
            if par_reader.check_has_pending_internals_in_iterators()? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn drop_internals_next_range_readers(&mut self) {
        for (_, par_reader) in self.internals_partition_readers.iter_mut() {
            par_reader.drop_next_range_reader();
        }
    }

    fn get_min_internals_processed_to_by_shards(&self) -> ProcessedTo {
        let mut min_internals_processed_to = ProcessedTo::default();

        for par_reader in self.internals_partition_readers.values() {
            for (shard_id, key) in &par_reader.reader_state().processed_to {
                min_internals_processed_to
                    .entry(*shard_id)
                    .and_modify(|min_key| *min_key = std::cmp::min(*min_key, *key))
                    .or_insert(*key);
            }
        }

        min_internals_processed_to
    }

    pub fn finalize(
        mut self,
        current_next_lt: u64,
        diffs_info: &FastHashMap<ShardIdent, (PartitionRouter, DiffStatistics)>,
    ) -> Result<FinalizedMessagesReader<V>> {
        let mut has_unprocessed_messages = self.has_messages_in_buffers()
            || self.has_pending_new_messages()
            || self.has_pending_externals_in_cache();

        // aggregated messages stats from all ranges
        // we need it to detect target ratition for new messages from queue diff
        // let mut aggregated_stats = self.internal_queue_statistics.result();

        // collect internals partition readers states
        let mut internals_reader_state = InternalsReaderState::default();
        for (_par_id, par_reader) in self.internals_partition_readers.iter_mut() {
            // check pending internals in iterators
            if !has_unprocessed_messages {
                has_unprocessed_messages = par_reader.check_has_pending_internals_in_iterators()?;
            }

            // TODO: we should consider all partitions for this logic
            //      otherwise if we drop processing offset only in one partition
            //      when messages from other partitions are not collected
            //      then it will cause incorrect messages refill after sync
            // // handle last new messages range reader
            // if let Ok((_, last_int_range_reader)) = par_reader.get_last_range_reader() {
            //     if last_int_range_reader.kind == InternalsRangeReaderKind::NewMessages {
            //         // if skip offset in new messages reader and last externals range reader are same
            //         // then we can drop processed offset both in internals and externals readers
            //         let last_ext_range_reader = self
            //             .externals_reader
            //             .get_last_range_reader()?
            //             .1
            //             .reader_state()
            //             .get_state_by_partition(*par_id)?;

            //         if last_int_range_reader.reader_state.skip_offset
            //             == last_ext_range_reader.skip_offset
            //         {
            //             par_reader.drop_processing_offset(true)?;
            //             self.externals_reader
            //                 .drop_processing_offset(*par_id, true)?;
            //         }
            //     }
            // }
        }

        // build queue diff
        let min_internals_processed_to = self.get_min_internals_processed_to_by_shards();
        let mut queue_diff_with_msgs = self
            .new_messages
            .into_queue_diff_with_messages(min_internals_processed_to);

        let min_messages = queue_diff_with_msgs
            .messages
            .keys()
            .next()
            .cloned()
            .unwrap_or_default();
        let max_messages = queue_diff_with_msgs
            .messages
            .keys()
            .last()
            .cloned()
            .unwrap_or_default();

        // get current queue diff messages stats and merge with aggregated stats
        let queue_diff_msgs_stats = DiffStatistics::from_diff(
            &queue_diff_with_msgs,
            self.for_shard_id,
            min_messages,
            max_messages,
        );

        self.internal_queue_statistics.add(
            self.for_shard_id,
            *queue_diff_msgs_stats.max_message(),
            queue_diff_msgs_stats.total_statistics(),
        );

        if let Some(processed_to) = queue_diff_with_msgs.processed_to.get(&self.for_shard_id) {
            self.internal_queue_statistics
                .remove_until(self.for_shard_id, processed_to);
        }

        // reset queue diff partition router
        // according to actual aggregated stats
        let moved_from_par_0_accounts = Self::reset_partition_router_by_stats(
            &self.msgs_exec_params,
            &mut queue_diff_with_msgs.partition_router,
            &self.internal_queue_statistics.result(),
            self.for_shard_id,
            diffs_info,
        )?;

        {
            let partitions_stats = queue_diff_with_msgs.partition_router.partitions_stats();
            for par_id in self
                .internals_partition_readers
                .keys()
                .filter(|&&par_id| par_id > 0)
            {
                let count = partitions_stats.get(par_id).copied().unwrap_or_default();
                let labels = [
                    ("workchain", self.for_shard_id.workchain().to_string()),
                    ("par_id", par_id.to_string()),
                ];
                metrics::gauge!("tycho_do_collate_accounts_count_in_partitions", &labels)
                    .set(count as f64);
            }
        }

        // remove moved accounts from partition 0 buffer
        let par_reader = self.internals_partition_readers.get_mut(&0).unwrap();
        if let Ok(last_int_range_reader) = par_reader.get_last_range_reader_mut() {
            if last_int_range_reader.kind == InternalsRangeReaderKind::NewMessages {
                last_int_range_reader
                    .reader_state
                    .buffer
                    .remove_messages_by_accounts(&moved_from_par_0_accounts);
            }
        }

        // collect internals reader state
        for (par_id, par_reader) in self.internals_partition_readers {
            internals_reader_state
                .partitions
                .insert(par_id, par_reader.finalize(current_next_lt)?);
        }

        internals_reader_state.cumulative_statistics = Some(self.internal_queue_statistics);

        // collect externals reader state
        let FinalizedExternalsReader {
            externals_reader_state,
            anchors_cache,
        } = self.externals_reader.finalize()?;

        let reader_state = ReaderState {
            externals: externals_reader_state,
            internals: internals_reader_state,
        };

        Ok(FinalizedMessagesReader {
            has_unprocessed_messages,
            reader_state,
            anchors_cache,
            queue_diff_with_msgs,
        })
    }

    pub fn reset_partition_router_by_stats(
        msgs_exec_params: &MsgsExecutionParams,
        partition_router: &mut PartitionRouter,
        aggregated_stats: &QueueStatistics,
        for_shard_id: ShardIdent,
        diffs_info: &FastHashMap<ShardIdent, (PartitionRouter, DiffStatistics)>,
    ) -> Result<FastHashSet<HashBytes>> {
        let par_0_msgs_count_limit = msgs_exec_params.par_0_int_msgs_count_limit as u64;
        let mut moved_from_par_0_accounts = FastHashSet::default();

        for (dest_int_address, msgs_count) in aggregated_stats.statistics() {
            let existing_partition = partition_router.get_partition(None, dest_int_address);
            if existing_partition != 0 {
                continue;
            }

            if for_shard_id.contains_address(dest_int_address) {
                tracing::trace!(target: tracing_targets::COLLATOR,
                    "check address {} for partition 0 because it is in current shard",
                    dest_int_address,
                );

                // if we have account for current shard then check if we need to move it to partition 1
                // if we have less than limit then keep it in partition 0
                if *msgs_count > par_0_msgs_count_limit {
                    tracing::trace!(target: tracing_targets::COLLATOR,
                        "move address {} to partition 1 because it has {} messages",
                        dest_int_address, msgs_count,
                    );
                    partition_router.insert_dst(dest_int_address, 1)?;
                    moved_from_par_0_accounts.insert(dest_int_address.get_address());
                }
            } else {
                tracing::trace!(target: tracing_targets::COLLATOR,
                    "reset partition router for address {} because it is not in current shard",
                    dest_int_address,
                );
                // if we have account for another shard then take info from that shard
                let acc_shard_diff_info = diffs_info
                    .iter()
                    .find(|(shard_id, _)| shard_id.contains_address(dest_int_address))
                    .map(|(_, diff)| diff.clone());

                // try to get remote partition from diff
                let total_msgs = match acc_shard_diff_info {
                    // if we do not have diff then use aggregated stats
                    None => {
                        tracing::trace!(target: tracing_targets::COLLATOR,
                            "use aggregated stats for address {} because we do not have diff",
                            dest_int_address,
                        );
                        msgs_count
                    }
                    Some((router, statistics)) => {
                        tracing::trace!(target: tracing_targets::COLLATOR,
                            "use diff for address {} because we have diff",
                            dest_int_address,
                        );
                        // getting remote shard partition from diff
                        let remote_shard_partition = router.get_partition(None, dest_int_address);

                        tracing::trace!(target: tracing_targets::COLLATOR,
                            "remote shard partition for address {} is {}",
                            dest_int_address, remote_shard_partition,
                        );

                        if remote_shard_partition != 0 {
                            tracing::trace!(target: tracing_targets::COLLATOR,
                                "move address {} to partition {} because it has partition {} in diff",
                                dest_int_address, remote_shard_partition, remote_shard_partition,
                            );
                            partition_router
                                .insert_dst(dest_int_address, remote_shard_partition)?;
                            continue;
                        }

                        // if remote partition == 0 then we need to check statistics
                        let remote_msgs_count = match statistics.partition(0) {
                            None => {
                                tracing::trace!(target: tracing_targets::COLLATOR,
                                    "use aggregated stats for address {} because we do not have partition 0 stats in diff",
                                    dest_int_address,
                                );
                                0
                            }
                            Some(partition) => {
                                tracing::trace!(target: tracing_targets::COLLATOR,
                                    "use partition 0 stats for address {} because we have partition 0 stats in diff",
                                    dest_int_address,
                                );
                                partition.get(dest_int_address).copied().unwrap_or(0)
                            }
                        };

                        &(msgs_count + remote_msgs_count)
                    }
                };

                tracing::trace!(target: tracing_targets::COLLATOR,
                    "total messages for address {} is {}",
                    dest_int_address, total_msgs,
                );
                if *total_msgs > par_0_msgs_count_limit {
                    tracing::trace!(target: tracing_targets::COLLATOR,
                        "move address {} to partition 1 because it has {} messages",
                        dest_int_address, total_msgs,
                    );
                    partition_router.insert_dst(dest_int_address, 1)?;
                    moved_from_par_0_accounts.insert(dest_int_address.get_address());
                }
            }
        }

        Ok(moved_from_par_0_accounts)
    }

    pub fn last_read_to_anchor_chain_time(&self) -> Option<u64> {
        self.externals_reader.last_read_to_anchor_chain_time()
    }

    pub fn metrics_by_partitions(&self) -> &MessagesReaderMetricsByPartitions {
        &self.metrics_by_partitions
    }

    pub fn add_new_messages(&mut self, messages: impl IntoIterator<Item = Arc<V>>) {
        self.new_messages.add_messages(messages);
    }

    pub fn count_messages_in_buffers_by_partitions(&self) -> BTreeMap<QueuePartitionIdx, usize> {
        let mut res: BTreeMap<_, _> = self
            .internals_partition_readers
            .iter()
            .map(|(par_id, par)| (*par_id, par.count_messages_in_buffers()))
            .collect();
        for (par_id, ext_count) in self
            .externals_reader
            .count_messages_in_buffers_by_partitions()
        {
            res.entry(par_id)
                .and_modify(|count| *count += ext_count)
                .or_default();
        }
        res
    }

    pub fn has_messages_in_buffers(&self) -> bool {
        self.has_internals_in_buffers() || self.has_externals_in_buffers()
    }

    pub fn has_internals_in_buffers(&self) -> bool {
        self.internals_partition_readers
            .values()
            .any(|v| v.has_messages_in_buffers())
    }

    pub fn has_not_fully_read_internals_ranges(&self) -> bool {
        self.internals_partition_readers
            .values()
            .any(|v| !v.all_ranges_fully_read)
    }

    pub fn has_pending_new_messages(&self) -> bool {
        self.new_messages.has_pending_messages()
    }

    pub fn has_externals_in_buffers(&self) -> bool {
        self.externals_reader.has_messages_in_buffers()
    }

    pub fn has_not_fully_read_externals_ranges(&self) -> bool {
        self.externals_reader.has_not_fully_read_ranges()
    }

    pub fn can_read_and_collect_more_messages(&self) -> bool {
        self.has_not_fully_read_externals_ranges()
            || self.has_not_fully_read_internals_ranges()
            || self.has_pending_new_messages()
            || self.has_messages_in_buffers()
    }

    pub fn has_pending_externals_in_cache(&self) -> bool {
        self.externals_reader.has_pending_externals()
    }

    pub fn check_has_non_zero_processed_offset(&self) -> bool {
        let check_internals = self
            .internals_partition_readers
            .values()
            .any(|par_reader| par_reader.has_non_zero_processed_offset());
        if check_internals {
            return check_internals;
        }

        // NOTE: in current implementation processed_offset syncronized in internals and externals readers
        self.externals_reader.has_non_zero_processed_offset()
    }

    pub fn check_need_refill(&self) -> bool {
        if self.has_messages_in_buffers() {
            return false;
        }

        // check if hash non zero processed offset
        self.check_has_non_zero_processed_offset()
    }

    pub fn refill_buffers_upto_offsets<F>(
        &mut self,
        mut is_cancelled: F,
    ) -> Result<(), CollatorError>
    where
        F: FnMut() -> bool,
    {
        tracing::debug!(target: tracing_targets::COLLATOR,
            internals_processed_offsets = ?DebugIter(self.internals_partition_readers
                .iter()
                .map(|(par_id, par_r)| {
                    (
                        par_id,
                        par_r.get_last_range_reader()
                            .map(|(_, r)| r.reader_state.processed_offset)
                            .unwrap_or_default(),
                    )
                })),
            externals_processed_offset = ?self.externals_reader.get_last_range_reader_offsets_by_partitions(),
            "start: refill messages buffer and skip groups upto",
        );

        loop {
            // stop refill when collation cancelled
            if is_cancelled() {
                return Ok(());
            }

            let msg_group = self.get_next_message_group(
                GetNextMessageGroupMode::Refill,
                0, // can pass 0 because new messages reader was not initialized in this case
            )?;
            if msg_group.is_none() {
                // on restart from a new genesis we will not be able to refill buffer with externals
                // so we stop refilling when there is no more groups in buffer
                break;
            }
        }

        // next time we should read next message group like we did not make refill before
        // so we need to reset flags and states that control the read flow
        self.reset_read_state();

        tracing::debug!(target: tracing_targets::COLLATOR,
            "finished: refill messages buffer and skip groups upto",
        );

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    pub fn get_next_message_group(
        &mut self,
        read_mode: GetNextMessageGroupMode,
        current_next_lt: u64,
    ) -> Result<Option<MessageGroup>, CollatorError> {
        tracing::debug!(target: tracing_targets::COLLATOR,
            ?read_mode,
            current_next_lt,
            "start collecting next message group",
        );

        // we collect separate messages groups by partitions them merge them into one
        let mut msg_groups = BTreeMap::<QueuePartitionIdx, MessageGroup>::new();

        // TODO: msgs-v3: try to read all in parallel

        // check if we have FinishExternals stage in any partition
        let mut has_finish_externals_stage = false;

        // init local metrics
        let mut metrics_by_partitions = MessagesReaderMetricsByPartitions::default();

        //--------------------
        // read internals
        for (par_id, par_reader_stage) in self.readers_stages.iter_mut() {
            let par_reader = self
                .internals_partition_readers
                .get_mut(par_id)
                .context("reader for partition should exist")?;

            // check if we have FinishExternals stage in any partition
            if matches!(
                par_reader_stage,
                MessagesReaderStage::FinishPreviousExternals
                    | MessagesReaderStage::FinishCurrentExternals
            ) {
                has_finish_externals_stage = true;
            }

            // on refill read only until the last range processed offset reached
            if read_mode == GetNextMessageGroupMode::Refill
                && par_reader.last_range_offset_reached()
            {
                continue;
            }

            match par_reader_stage {
                MessagesReaderStage::ExistingAndExternals => {
                    let read_metrics = par_reader.read_existing_messages_into_buffers(read_mode)?;
                    metrics_by_partitions.get_mut(*par_id).append(&read_metrics);
                }
                MessagesReaderStage::FinishPreviousExternals
                | MessagesReaderStage::FinishCurrentExternals => {
                    // do not read internals when finishing to collect externals
                }
                MessagesReaderStage::ExternalsAndNew => {
                    let read_new_messages_res = par_reader
                        .read_new_messages_into_buffers(&mut self.new_messages, current_next_lt)?;
                    metrics_by_partitions
                        .get_mut(*par_id)
                        .append(&read_new_messages_res.metrics);
                }
            }
        }

        //--------------------
        // read externals
        'read_externals: {
            // do not read more externals on FinishExternals stage in any partition
            if has_finish_externals_stage {
                break 'read_externals;
            }

            // on refill read only until the last range processed offsets reached for all partitions
            if read_mode == GetNextMessageGroupMode::Refill
                && self
                    .externals_reader
                    .last_range_offsets_reached_in_all_partitions()
            {
                break 'read_externals;
            }

            let read_metrics = self
                .externals_reader
                .read_into_buffers(read_mode, self.new_messages.partition_router());
            metrics_by_partitions.append(read_metrics);
        }

        // messages buffers metrics
        {
            let mut total_msgs_count_in_buffers = 0;
            for (par_id, count) in self.count_messages_in_buffers_by_partitions() {
                let labels = [
                    ("workchain", self.for_shard_id.workchain().to_string()),
                    ("par_id", par_id.to_string()),
                ];
                metrics::gauge!(
                    "tycho_do_collate_msgs_exec_buffer_messages_count_by_partitions",
                    &labels
                )
                .set(count as f64);
                total_msgs_count_in_buffers += count;
            }
            let labels = [("workchain", self.for_shard_id.workchain().to_string())];
            metrics::gauge!("tycho_do_collate_msgs_exec_buffer_messages_count", &labels)
                .set(total_msgs_count_in_buffers as f64);
        }

        //----------
        // collect messages after reading
        let mut partitions_readers = BTreeMap::new();
        let mut can_drop_processing_offset_in_all_partitions = true;
        for (par_id, par_reader_stage) in self.readers_stages.iter_mut() {
            // extract partition reader from state to use partition 0 buffer
            // to check for account skip on collecting messages from partition 1
            let mut par_reader = self
                .internals_partition_readers
                .remove(par_id)
                .context("reader for partition should exist")?;

            // on refill collect only until the last ranges processed offsets reached
            if read_mode == GetNextMessageGroupMode::Refill
                && par_reader.last_range_offset_reached()
                && self.externals_reader.last_range_offset_reached(par_id)
            {
                partitions_readers.insert(*par_id, par_reader);
                can_drop_processing_offset_in_all_partitions = false;
                continue;
            }

            // collect existing internals, externals and new internals
            let has_pending_new_messages_for_partition = self
                .new_messages
                .has_pending_messages_from_partition(*par_id);
            let CollectMessageForPartitionResult {
                metrics,
                msg_group,
                collected_queue_msgs_keys,
                can_drop_processing_offset,
            } = Self::collect_messages_for_partition(
                read_mode,
                par_reader_stage,
                &mut par_reader,
                &mut self.externals_reader,
                has_pending_new_messages_for_partition,
                &partitions_readers,
                &msg_groups,
            )?;
            msg_groups.insert(*par_id, msg_group);
            metrics_by_partitions.get_mut(*par_id).append(&metrics);

            // detect if can drop procssing offset in all partitions
            if !can_drop_processing_offset {
                can_drop_processing_offset_in_all_partitions = false;
            }

            // remove collected new messages
            self.new_messages
                .remove_collected_messages(&collected_queue_msgs_keys);

            partitions_readers.insert(*par_id, par_reader);
        }
        // return partition readers to state
        self.internals_partition_readers = partitions_readers;

        //----------
        // check if prev processed offset reached
        // in internals and externals readers
        let all_prev_processed_offset_reached = self
            .externals_reader
            .last_range_offsets_reached_in_all_partitions()
            && self
                .internals_partition_readers
                .values()
                .all(|par_reader| par_reader.last_range_offset_reached());

        //----------
        // drop processing offsets in all partitions if can do this
        if can_drop_processing_offset_in_all_partitions {
            for (par_id, par_reader) in self.internals_partition_readers.iter_mut() {
                // drop processing offset for internals
                par_reader.drop_processing_offset(true)?;
                // and drop processing offset for externals
                self.externals_reader
                    .drop_processing_offset(*par_id, true)?;
            }
        }

        // log metrics from partitions
        for (par_id, par_metrics) in metrics_by_partitions.iter() {
            tracing::debug!(target: tracing_targets::COLLATOR,
                "messages read from partition {}: existing={}, ext={}, new={}",
                par_id,
                par_metrics.read_existing_msgs_count,
                par_metrics.read_ext_msgs_count,
                par_metrics.read_new_msgs_count,
            );
        }

        tracing::debug!(target: tracing_targets::COLLATOR,
            int_curr_processed_offset = ?DebugIter(self
                .internals_partition_readers.iter()
                .map(|(par_id, par)| (par_id, par.reader_state().curr_processed_offset))),
            ext_curr_processed_offset = ?DebugIter(self
                .externals_reader.reader_state()
                .by_partitions.iter()
                .map(|(par_id, par)| (par_id, par.curr_processed_offset))),
            int_msgs_count_in_buffers = ?DebugIter(self
                .internals_partition_readers.iter()
                .map(|(par_id, par)| (par_id, par.count_messages_in_buffers()))),
            ext_msgs_count_in_buffers = ?self.externals_reader.count_messages_in_buffers_by_partitions(),
            "collected message groups by partitions: {:?}",
            DebugIter(msg_groups.iter().map(|(par_id, g)| (*par_id, DisplayMessageGroup(g)))),
        );

        // aggregate message group
        let par_0_metrics = metrics_by_partitions.get_mut(0);
        par_0_metrics.add_to_message_groups_timer.start();
        let msg_group = msg_groups
            .into_iter()
            .fold(MessageGroup::default(), |acc, (_, next)| acc.add(next));
        par_0_metrics.add_to_message_groups_timer.stop();

        tracing::debug!(target: tracing_targets::COLLATOR,
            has_not_fully_read_externals_ranges = self.has_not_fully_read_externals_ranges(),
            has_not_fully_read_internals_ranges = self.has_not_fully_read_internals_ranges(),
            has_pending_new_messages = self.has_pending_new_messages(),
            has_messages_in_buffers = self.has_messages_in_buffers(),
            has_pending_externals_in_cache = self.has_pending_externals_in_cache(),
            ?read_mode,
            all_prev_processed_offset_reached,
            add_to_message_groups_total_elapsed_ms = metrics_by_partitions.add_to_message_groups_total_elapsed().as_millis(),
            "aggregated collected message group: {:?}",
            DebugMessageGroup(&msg_group),
        );

        // aggregate metrics from partitions
        for (par_id, par_metrics) in metrics_by_partitions.iter() {
            self.metrics_by_partitions
                .get_mut(*par_id)
                .append(par_metrics);
        }

        // retun None when messages group is empty
        if msg_group.len() == 0
            // and we reached previous processed offset on refill
            && ((read_mode == GetNextMessageGroupMode::Refill && all_prev_processed_offset_reached)
                // or we do not have messages in buffers and no pending new messages and all ranges fully read
                // so we cannot read more messages into buffers and then collect them
                || !self.can_read_and_collect_more_messages()
            )
        {
            Ok(None)
        } else {
            Ok(Some(msg_group))
        }
    }

    fn collect_messages_for_partition(
        read_mode: GetNextMessageGroupMode,
        par_reader_stage: &mut MessagesReaderStage,
        par_reader: &mut InternalsPartitionReader<V>,
        externals_reader: &mut ExternalsReader,
        has_pending_new_messages_for_partition: bool,
        prev_partitions_readers: &BTreeMap<QueuePartitionIdx, InternalsPartitionReader<V>>,
        prev_msg_groups: &BTreeMap<QueuePartitionIdx, MessageGroup>,
    ) -> Result<CollectMessageForPartitionResult> {
        let mut res = CollectMessageForPartitionResult::default();

        // on refill collect only until the last range processed offset reached
        let int_prev_processed_offset_reached_on_refill =
            read_mode == GetNextMessageGroupMode::Refill && par_reader.last_range_offset_reached();
        let ext_prev_processed_offsets_reached_on_refill = read_mode
            == GetNextMessageGroupMode::Refill
            && externals_reader.last_range_offset_reached(&par_reader.partition_id);

        // update processed offset anyway
        par_reader.increment_curr_processed_offset();
        externals_reader.increment_curr_processed_offset(&par_reader.partition_id)?;

        // remember if all internals or externals were collected before to reduce spam in logs further
        let mut all_internals_collected_before = false;
        let mut all_read_externals_collected_before = false;

        // collect existing internals
        if *par_reader_stage == MessagesReaderStage::ExistingAndExternals
            && !int_prev_processed_offset_reached_on_refill
        {
            all_internals_collected_before = par_reader.all_read_existing_messages_collected();

            let CollectInternalsResult { metrics, .. } = par_reader.collect_messages(
                par_reader_stage,
                &mut res.msg_group,
                prev_partitions_readers,
                prev_msg_groups,
            )?;

            res.metrics.append(&metrics);
        }

        // collect externals
        if !ext_prev_processed_offsets_reached_on_refill {
            all_read_externals_collected_before = !externals_reader.has_messages_in_buffers();

            let CollectExternalsResult { metrics } = externals_reader.collect_messages(
                par_reader.partition_id,
                &mut res.msg_group,
                prev_partitions_readers,
                prev_msg_groups,
            )?;
            res.metrics.append(&metrics);
        }

        // collect new internals
        if *par_reader_stage == MessagesReaderStage::ExternalsAndNew
            && !int_prev_processed_offset_reached_on_refill
        {
            all_internals_collected_before =
                par_reader.all_new_messages_collected(has_pending_new_messages_for_partition);

            let CollectInternalsResult {
                metrics,
                mut collected_queue_msgs_keys,
            } = par_reader.collect_messages(
                par_reader_stage,
                &mut res.msg_group,
                prev_partitions_readers,
                prev_msg_groups,
            )?;
            res.metrics.append(&metrics);
            res.collected_queue_msgs_keys
                .append(&mut collected_queue_msgs_keys);

            // set skip and processed offset to current offset
            // because we will not save collected new messages to the queue
            par_reader.set_skip_processed_offset_to_current()?;
        }

        // switch to the next reader stage if required

        // if all read externals collected
        let all_read_externals_collected = !externals_reader.has_messages_in_buffers();
        if all_read_externals_collected {
            // finalize externals read state
            {
                // drop all ranges except the last one
                externals_reader.retain_only_last_range_reader()?;
                // update reader state for each partitions
                let par_ids = externals_reader.get_partition_ids();
                for par_id in par_ids {
                    // mark all read messages processed
                    externals_reader.set_processed_to_current_position(par_id)?;
                    // set skip offset to current offset
                    externals_reader.set_skip_processed_offset_to_current(par_id)?;
                }
                // we can move "from" boundary to current position
                // because all messages up to current position processed
                externals_reader.set_from_to_current_position_in_last_range_reader()?;
                // drop last read to anchor chain time when no pending externals in cache
                // it used to calc externals time diff, but it does not update when there are no messages,
                // so time diff will grow endlessly, so we drop the last chain time to drop time diff
                if !externals_reader.has_pending_externals() {
                    externals_reader.drop_last_read_to_anchor_chain_time();
                }
            }

            // log only first time
            if !all_read_externals_collected_before {
                tracing::debug!(target: tracing_targets::COLLATOR,
                    has_pending_externals = externals_reader.has_pending_externals(),
                    ext_reader_states = ?externals_reader.reader_state().by_partitions,
                    last_range_reader_state = ?externals_reader.get_last_range_reader().map(|(seqno, r)| (seqno, DebugExternalsRangeReaderState(r.reader_state()))),
                    "all read externals collected when collecting from partition_id={}",
                    par_reader.partition_id,
                );
            }
        }

        let partition_id = par_reader.partition_id;
        let update_reader_stage = |curr: &mut MessagesReaderStage, new| {
            let old = *curr;
            *curr = new;
            tracing::debug!(target: tracing_targets::COLLATOR,
                partition_id,
                ?old,
                ?new,
                "messages partition reader stage updated",
            );
        };

        // if all read externals collected from the previous block collation
        // then we can switch to the "read existing internals stage"
        if all_read_externals_collected
            && *par_reader_stage == MessagesReaderStage::FinishPreviousExternals
            && read_mode != GetNextMessageGroupMode::Refill
        {
            // switch to the "read existing internals stage" stage
            update_reader_stage(par_reader_stage, MessagesReaderStage::ExistingAndExternals);
        }

        // if all existing internals collected
        // then we should collect all already read externals without reading more from cache
        // and only after that we can finalize existing internals read state
        if *par_reader_stage == MessagesReaderStage::ExistingAndExternals
            && par_reader.all_read_existing_messages_collected()
        {
            // log only first time
            if !all_internals_collected_before {
                tracing::debug!(target: tracing_targets::COLLATOR,
                    partition_id = par_reader.partition_id,
                    int_processed_to = ?par_reader.reader_state().processed_to,
                    int_curr_processed_offset = par_reader.reader_state().curr_processed_offset,
                    last_range_reader_state = ?par_reader.get_last_range_reader().map(|(seqno, r)| (seqno, DebugInternalsRangeReaderState(&r.reader_state))),
                    "all read existing internals collected from partition",
                );
            }

            if read_mode != GetNextMessageGroupMode::Refill {
                // switch to the "collect only already read externals" stage
                update_reader_stage(
                    par_reader_stage,
                    MessagesReaderStage::FinishCurrentExternals,
                );
            }
        }

        // if all read externals collected from current block collation
        // then we can finalize existing internals read state
        // and switch to the "new messages processing" stage
        if all_read_externals_collected
            && *par_reader_stage == MessagesReaderStage::FinishCurrentExternals
        {
            // finalize existing intenals read state
            // drop all ranges except the last one
            par_reader.retain_only_last_range_reader()?;
            // mark all read messages processed
            par_reader.set_processed_to_current_position()?;

            // NOTE: we can drop processing offset only when all read exiting messages
            //      collected in all partitions, otherwise skip offset could differ in partitions
            //      that may cause incorrect messages buffers refill after sync

            // mark that current partition can drop processed offset
            res.can_drop_processing_offset = true;

            // set skip and processed offset to current offset
            par_reader.set_skip_processed_offset_to_current()?;

            if read_mode != GetNextMessageGroupMode::Refill {
                // switch to the "new messages processing" stage
                // if all existing messages read (last range reader was created in current block)
                let (last_seqno, _) = par_reader.get_last_range_reader()?;
                if last_seqno == &par_reader.block_seqno {
                    update_reader_stage(par_reader_stage, MessagesReaderStage::ExternalsAndNew);
                } else {
                    // otherwise return to the reading of existing messages
                    update_reader_stage(
                        par_reader_stage,
                        MessagesReaderStage::ExistingAndExternals,
                    );
                }
            }
        }

        // if all new messages collected
        // finalize new messages read state
        if *par_reader_stage == MessagesReaderStage::ExternalsAndNew
            && par_reader.all_new_messages_collected(has_pending_new_messages_for_partition)
        {
            // mark all read messages processed
            par_reader.set_processed_to_current_position()?;

            // NOTE: we can drop processing offset only when all read exiting messages
            //      collected in all partitions, otherwise skip offset could differ in partitions
            //      that may cause incorrect messages buffers refill after sync

            // if all read externals collected
            // mark that current partition can drop processed offset
            if all_read_externals_collected {
                res.can_drop_processing_offset = true;
            }

            // log only first time
            if !all_internals_collected_before {
                tracing::debug!(target: tracing_targets::COLLATOR,
                    partition_id = par_reader.partition_id,
                    int_processed_to = ?par_reader.reader_state().processed_to,
                    int_curr_processed_offset = par_reader.reader_state().curr_processed_offset,
                    last_range_reader_state = ?par_reader.get_last_range_reader().map(|(seqno, r)| (seqno, DebugInternalsRangeReaderState(&r.reader_state))),
                    "all new internals collected from partition",
                );
            }
        }

        Ok(res)
    }
}

#[derive(Default)]
struct CollectMessageForPartitionResult {
    metrics: MessagesReaderMetrics,
    msg_group: MessageGroup,
    collected_queue_msgs_keys: Vec<QueueKey>,
    can_drop_processing_offset: bool,
}

#[derive(Default)]
pub struct MetricsTimer {
    timer: Option<std::time::Instant>,
    pub total_elapsed: Duration,
}
impl std::fmt::Debug for MetricsTimer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.total_elapsed)
    }
}
impl MetricsTimer {
    pub fn start(&mut self) {
        self.timer = Some(std::time::Instant::now());
    }
    pub fn stop(&mut self) -> Duration {
        match self.timer.take() {
            Some(timer) => {
                let elapsed = timer.elapsed();
                self.total_elapsed += elapsed;
                elapsed
            }
            None => Duration::default(),
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct MessagesReaderMetrics {
    /// sum total time of initializations of internal messages iterators
    pub init_iterator_timer: MetricsTimer,

    /// sum total time of reading existing internal messages
    pub read_existing_messages_timer: MetricsTimer,
    /// sum total time of reading new internal messages
    pub read_new_messages_timer: MetricsTimer,
    /// sum total time of reading external messages
    pub read_ext_messages_timer: MetricsTimer,
    /// sum total time of adding messages to buffers
    pub add_to_message_groups_timer: MetricsTimer,

    /// num of existing internal messages read
    pub read_existing_msgs_count: u64,
    /// num of new internal messages read
    pub read_new_msgs_count: u64,
    /// num of external messages read
    pub read_ext_msgs_count: u64,

    pub add_to_msgs_groups_ops_count: u64,
}

impl MessagesReaderMetrics {
    fn append(&mut self, other: &Self) {
        self.init_iterator_timer.total_elapsed += other.init_iterator_timer.total_elapsed;

        self.read_existing_messages_timer.total_elapsed +=
            other.read_existing_messages_timer.total_elapsed;
        self.read_new_messages_timer.total_elapsed += other.read_new_messages_timer.total_elapsed;
        self.read_ext_messages_timer.total_elapsed += other.read_ext_messages_timer.total_elapsed;
        self.add_to_message_groups_timer.total_elapsed +=
            other.add_to_message_groups_timer.total_elapsed;

        self.read_existing_msgs_count += other.read_existing_msgs_count;
        self.read_new_msgs_count += other.read_new_msgs_count;
        self.read_ext_msgs_count += other.read_ext_msgs_count;

        self.add_to_msgs_groups_ops_count = self
            .add_to_msgs_groups_ops_count
            .saturating_add(other.add_to_msgs_groups_ops_count);
    }
}

#[derive(Default)]
pub(super) struct MessagesReaderMetricsByPartitions {
    inner: BTreeMap<QueuePartitionIdx, MessagesReaderMetrics>,
}

impl MessagesReaderMetricsByPartitions {
    pub fn get_mut(&mut self, par_id: QueuePartitionIdx) -> &mut MessagesReaderMetrics {
        self.inner.entry(par_id).or_default()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&QueuePartitionIdx, &MessagesReaderMetrics)> {
        self.inner.iter()
    }

    pub fn add_to_message_groups_total_elapsed(&self) -> Duration {
        self.inner
            .iter()
            .fold(Duration::default(), |acc, (_, curr)| {
                acc.saturating_add(curr.add_to_message_groups_timer.total_elapsed)
            })
    }

    pub fn append(&mut self, other: Self) {
        for (par_id, metrics) in other.inner {
            match self.inner.entry(par_id) {
                btree_map::Entry::Occupied(mut occupied) => {
                    occupied.get_mut().append(&metrics);
                }
                btree_map::Entry::Vacant(vacant) => {
                    vacant.insert(metrics);
                }
            }
        }
    }

    pub fn get_total(&self) -> MessagesReaderMetrics {
        self.inner
            .values()
            .fold(MessagesReaderMetrics::default(), |mut acc, curr| {
                acc.append(curr);
                acc
            })
    }
}
