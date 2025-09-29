use std::collections::{BTreeMap, VecDeque};

use anyhow::{Context, Result};
use tycho_block_util::queue::{QueuePartitionIdx, get_short_addr_string, get_short_hash_string};
use tycho_types::cell::HashBytes;
use tycho_types::models::{IntAddr, MsgInfo, ShardIdent};
use tycho_util::FastHashSet;

use super::{
    DebugExternalsRangeReaderState, ExternalKey, ExternalsRangeReaderState,
    ExternalsRangeReaderStateByPartition, ExternalsReaderRange, ExternalsReaderState,
    GetNextMessageGroupMode, InternalsPartitionReader, MessagesReaderMetrics,
    MessagesReaderMetricsByPartitions,
};
use crate::collator::messages_buffer::{
    BufferFillStateByCount, BufferFillStateBySlots, FillMessageGroupResult, IncludeAllMessages,
    MessageGroup, MessagesBufferLimits, MsgFilter, SkipExpiredExternals,
};
use crate::collator::messages_reader::{DebugInternalsRangeReaderState, InternalsRangeReaderKind};
use crate::collator::types::{
    AnchorsCache, MsgsExecutionParamsExtension, MsgsExecutionParamsStuff, ParsedMessage,
};
use crate::internal_queue::types::{InternalMessageValue, PartitionRouter};
use crate::tracing_targets;
use crate::types::processed_upto::BlockSeqno;
use crate::types::{DebugIter, SaturatingAddAssign};

#[cfg(test)]
#[path = "../tests/externals_reader_tests.rs"]
pub(super) mod tests;

//=========
// EXTERNALS READER
//=========
pub(super) struct FinalizedExternalsReader {
    pub externals_reader_state: ExternalsReaderState,
    pub anchors_cache: AnchorsCache,
}

pub(super) struct ExternalsReader {
    for_shard_id: ShardIdent,
    block_seqno: BlockSeqno,
    next_chain_time: u64,
    msgs_exec_params: MsgsExecutionParamsStuff,
    /// Target limits for filling message group from the buffer
    buffer_limits_by_partitions: BTreeMap<QueuePartitionIdx, MessagesBufferLimits>,
    anchors_cache: AnchorsCache,
    /// Should not read `.ranges` after reader creation because they moved into `.range_readers`
    reader_state: ExternalsReaderState,
    range_readers: BTreeMap<BlockSeqno, ExternalsRangeReader>,
    all_ranges_fully_read: bool,
}

impl ExternalsReader {
    pub fn new(
        for_shard_id: ShardIdent,
        block_seqno: BlockSeqno,
        next_chain_time: u64,
        msgs_exec_params: MsgsExecutionParamsStuff,
        buffer_limits_by_partitions: BTreeMap<QueuePartitionIdx, MessagesBufferLimits>,
        anchors_cache: AnchorsCache,
        mut reader_state: ExternalsReaderState,
    ) -> Self {
        // init minimal partitions count in the state if not exist
        for par_id in buffer_limits_by_partitions.keys() {
            reader_state.by_partitions.entry(*par_id).or_default();
        }

        let mut reader = Self {
            for_shard_id,
            block_seqno,
            next_chain_time,
            msgs_exec_params,
            buffer_limits_by_partitions,
            anchors_cache,
            reader_state,
            range_readers: Default::default(),
            all_ranges_fully_read: false,
        };

        // create existing range readers
        reader.create_existing_range_readers();

        reader
    }

    pub(super) fn reset_read_state(&mut self) {
        self.all_ranges_fully_read = false;
    }

    pub fn finalize(mut self) -> Result<FinalizedExternalsReader> {
        // collect range reader states
        let mut range_readers = self.range_readers.into_iter().peekable();
        let mut max_processed_offsets = BTreeMap::<QueuePartitionIdx, u32>::new();
        while let Some((seqno, mut range_reader)) = range_readers.next() {
            // update offset in the last range reader state for partition
            // if current offset is greater than the maximum stored one among all ranges
            for (par_id, par) in &self.reader_state.by_partitions {
                let range_reader_state_by_partition = range_reader
                    .reader_state
                    .get_state_by_partition_mut(*par_id)?;
                let max_processed_offset = max_processed_offsets
                    .entry(*par_id)
                    .and_modify(|max| {
                        *max = range_reader_state_by_partition.processed_offset.max(*max);
                    })
                    .or_insert(range_reader_state_by_partition.processed_offset);

                if par.curr_processed_offset > *max_processed_offset
                    && range_readers.peek().is_none()
                {
                    range_reader_state_by_partition.processed_offset = par.curr_processed_offset;
                }
            }

            self.reader_state
                .ranges
                .insert(seqno, range_reader.reader_state);
        }

        // return updated externals reader state
        Ok(FinalizedExternalsReader {
            externals_reader_state: self.reader_state,
            anchors_cache: self.anchors_cache,
        })
    }

    pub fn open_ranges_limit_reached(&self) -> bool {
        self.range_readers.len() >= self.msgs_exec_params.current().open_ranges_limit()
    }

    pub fn reader_state(&self) -> &ExternalsReaderState {
        &self.reader_state
    }

    pub fn get_partition_ids(&self) -> Vec<QueuePartitionIdx> {
        self.reader_state.by_partitions.keys().copied().collect()
    }

    pub(super) fn drop_last_read_to_anchor_chain_time(&mut self) {
        self.reader_state.last_read_to_anchor_chain_time = None;
    }

    fn get_buffer_limits_by_partition(
        &self,
        par_id: QueuePartitionIdx,
    ) -> Result<MessagesBufferLimits> {
        self.buffer_limits_by_partitions
            .get(&par_id)
            .cloned()
            .with_context(|| format!(
                "externals reader does not contain buffer limits for partition {} (for_shard_id: {}, block_seqno: {})",
                par_id, self.for_shard_id, self.block_seqno,
            ))
    }

    pub fn set_buffer_limits_by_partition(
        &mut self,
        buffer_limits_by_partitions: BTreeMap<QueuePartitionIdx, MessagesBufferLimits>,
    ) {
        self.buffer_limits_by_partitions = buffer_limits_by_partitions;
    }

    pub fn has_non_zero_processed_offset(&self) -> bool {
        self.range_readers.values().any(|r| {
            r.reader_state
                .by_partitions
                .values()
                .any(|par| par.processed_offset > 0)
        })
    }

    pub fn last_range_offset_reached(&self, par_id: &QueuePartitionIdx) -> bool {
        self.reader_state
            .by_partitions
            .get(par_id)
            .map(|state_by_partition| {
                self.get_last_range_reader().map(|(_, r)| {
                    r.reader_state
                        .by_partitions
                        .get(par_id)
                        .map(|range_state_by_partition| {
                            range_state_by_partition.processed_offset
                                <= state_by_partition.curr_processed_offset
                        })
                })
            })
            .and_then(|res| res.ok())
            .and_then(|res| res)
            .unwrap_or(true)
    }

    pub fn last_range_offsets_reached_in_all_partitions(&self) -> bool {
        self.get_last_range_reader()
            .map(|(_, r)| {
                r.reader_state.by_partitions.iter().all(|(par_id, par)| {
                    par.processed_offset
                        <= self
                            .reader_state
                            .by_partitions
                            .get(par_id)
                            .unwrap()
                            .curr_processed_offset
                })
            })
            .unwrap_or(true)
    }

    pub fn get_last_range_reader_offsets_by_partitions(&self) -> Vec<(QueuePartitionIdx, u32)> {
        self.get_last_range_reader()
            .map(|(_, r)| {
                r.reader_state
                    .by_partitions
                    .iter()
                    .map(|(par_id, par)| (*par_id, par.processed_offset))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default()
    }

    pub fn count_messages_in_buffers_by_partitions(&self) -> BTreeMap<QueuePartitionIdx, usize> {
        self.range_readers
            .values()
            .fold(BTreeMap::new(), |mut curr, r| {
                for (par_id, par) in &r.reader_state.by_partitions {
                    let sum = curr.entry(*par_id).or_default();
                    *sum = sum.saturating_add(par.buffer.msgs_count());
                }
                curr
            })
    }

    pub fn has_messages_in_buffers(&self) -> bool {
        self.range_readers.values().any(|v| {
            v.reader_state
                .by_partitions
                .values()
                .any(|par| par.buffer.msgs_count() > 0)
        })
    }

    pub fn has_not_fully_read_ranges(&self) -> bool {
        !self.all_ranges_fully_read
    }

    pub fn all_ranges_read_and_collected(&self) -> bool {
        self.all_ranges_fully_read && !self.has_messages_in_buffers()
    }

    pub fn check_all_ranges_read_and_collected(&self) -> bool {
        self.range_readers.values().all(|v| v.fully_read) && !self.has_messages_in_buffers()
    }

    pub fn all_read_externals_collected(&self) -> bool {
        self.range_readers.values().all(|r| {
            r.reader_state.by_partitions.iter().all(|(par_id, s)| {
                s.buffer.msgs_count() == 0
                    && self
                        .reader_state
                        .by_partitions
                        .get(par_id)
                        .map(|r_s| r_s.curr_processed_offset)
                        .unwrap_or_default()
                        >= s.skip_offset
            })
        })
    }

    pub fn has_pending_externals(&self) -> bool {
        self.anchors_cache.has_pending_externals()
    }

    pub fn retain_only_last_range_reader(&mut self) -> Result<()> {
        let (last_seqno, last_range_reader) = self.range_readers.pop_last().context(
            "externals reader should have at least one range reader when retain_only_last_range_reader() called",
        )?;

        if last_seqno < self.block_seqno {
            // set that not all ranges fully read
            // to force create and read next range reader for block
            self.all_ranges_fully_read = false;
        }

        self.range_readers.clear();
        self.range_readers.insert(last_seqno, last_range_reader);
        Ok(())
    }

    pub fn set_from_to_current_position_in_last_range_reader(&mut self) -> Result<()> {
        let last_range_reader = self.get_last_range_reader_mut()?;
        last_range_reader.reader_state.range.from =
            last_range_reader.reader_state.range.current_position;
        Ok(())
    }

    pub fn pop_first_range_reader(&mut self) -> Option<(BlockSeqno, ExternalsRangeReader)> {
        self.range_readers.pop_first()
    }

    pub fn set_range_readers(
        &mut self,
        mut range_readers: BTreeMap<BlockSeqno, ExternalsRangeReader>,
    ) {
        self.range_readers.append(&mut range_readers);
    }

    pub fn get_last_range_reader(&self) -> Result<(&BlockSeqno, &ExternalsRangeReader)> {
        self.range_readers
            .last_key_value()
            .context("externals reader should have at least one range reader")
    }

    pub fn get_last_range_reader_mut(&mut self) -> Result<&mut ExternalsRangeReader> {
        let (&last_seqno, _) = self.get_last_range_reader()?;
        Ok(self.range_readers.get_mut(&last_seqno).unwrap())
    }

    pub fn increment_curr_processed_offset(&mut self, par_id: &QueuePartitionIdx) -> Result<()> {
        let reader_state_by_partition = self
            .reader_state
            .by_partitions
            .get_mut(par_id)
            .with_context(|| format!("externals reader state not exists for partition {par_id}"))?;
        reader_state_by_partition.curr_processed_offset += 1;
        Ok(())
    }

    /// Drop current offset and offset in the last range reader state
    pub fn drop_processing_offset(
        &mut self,
        par_id: QueuePartitionIdx,
        drop_skip_offset: bool,
    ) -> Result<()> {
        let reader_state_by_partition = self.reader_state.get_state_by_partition_mut(par_id)?;
        reader_state_by_partition.curr_processed_offset = 0;

        let last_range_reader = self.get_last_range_reader_mut()?;
        let last_range_reader_by_partition = last_range_reader
            .reader_state
            .get_state_by_partition_mut(par_id)?;
        last_range_reader_by_partition.processed_offset = 0;

        if drop_skip_offset {
            last_range_reader_by_partition.skip_offset = 0;
        }

        Ok(())
    }

    pub fn set_skip_processed_offset_to_current(
        &mut self,
        par_id: QueuePartitionIdx,
    ) -> Result<()> {
        let curr_processed_offset = self
            .reader_state
            .get_state_by_partition(par_id)?
            .curr_processed_offset;

        let last_range_reader = self.get_last_range_reader_mut()?;
        let last_range_reader_by_partition = last_range_reader
            .reader_state
            .get_state_by_partition_mut(par_id)?;
        // update processed offset only if current is greater
        // because this method could be called on refill before the processed_offset reached
        if curr_processed_offset > last_range_reader_by_partition.processed_offset {
            last_range_reader_by_partition.processed_offset = curr_processed_offset;
            last_range_reader_by_partition.skip_offset = curr_processed_offset;
        }

        Ok(())
    }

    pub fn set_processed_to_current_position(&mut self, par_id: QueuePartitionIdx) -> Result<()> {
        let (_, last_range_reader) = self.get_last_range_reader()?;
        let current_position = last_range_reader.reader_state.range.current_position;

        let reader_state_by_partition = self.reader_state.get_state_by_partition_mut(par_id)?;
        reader_state_by_partition.processed_to = current_position;

        Ok(())
    }

    fn create_existing_range_readers(&mut self) {
        while let Some((seqno, range_reader_state)) = self.reader_state.ranges.pop_first() {
            let reader = self.create_existing_externals_range_reader(range_reader_state, seqno);
            self.range_readers.insert(seqno, reader);
        }
    }

    #[tracing::instrument(skip_all)]
    fn create_existing_externals_range_reader(
        &self,
        range_reader_state: ExternalsRangeReaderState,
        seqno: BlockSeqno,
    ) -> ExternalsRangeReader {
        let reader = ExternalsRangeReader {
            for_shard_id: self.for_shard_id,
            seqno,
            buffer_limits_by_partitions: self.buffer_limits_by_partitions.clone(),
            fully_read: range_reader_state.range.current_position == range_reader_state.range.to,
            reader_state: range_reader_state,
        };

        tracing::debug!(target: tracing_targets::COLLATOR,
            seqno = reader.seqno,
            fully_read = reader.fully_read,
            reader_state = ?DebugExternalsRangeReaderState(&reader.reader_state),
            "externals reader: created existing range reader",
        );

        reader
    }

    fn create_append_next_range_reader(&mut self) {
        let reader = self.create_next_externals_range_reader();
        if self
            .range_readers
            .insert(self.block_seqno, reader)
            .is_some()
        {
            panic!(
                "externals range reader should not already exist (for_shard_id: {}, seqno: {})",
                self.for_shard_id, self.block_seqno
            )
        };
        self.all_ranges_fully_read = false;
    }

    #[tracing::instrument(skip_all)]
    fn create_next_externals_range_reader(&self) -> ExternalsRangeReader {
        let from = self
            .range_readers
            .values()
            .last()
            .map(|r| r.reader_state.range.to)
            .unwrap_or_default();

        // create range reader states by partitions
        let mut by_partitions = BTreeMap::new();
        for (par_id, par) in &self.reader_state.by_partitions {
            by_partitions.insert(*par_id, ExternalsRangeReaderStateByPartition {
                buffer: Default::default(),
                skip_offset: par.curr_processed_offset,
                processed_offset: par.curr_processed_offset,
                last_expire_check_on_ct: None,
            });
        }

        let reader = ExternalsRangeReader {
            for_shard_id: self.for_shard_id,
            seqno: self.block_seqno,
            fully_read: false,
            buffer_limits_by_partitions: self.buffer_limits_by_partitions.clone(),
            reader_state: ExternalsRangeReaderState {
                range: ExternalsReaderRange {
                    from,
                    to: from,
                    current_position: from,
                    chain_time: self.next_chain_time,
                },
                by_partitions,
            },
        };

        tracing::debug!(target: tracing_targets::COLLATOR,
            seqno = reader.seqno,
            fully_read = reader.fully_read,
            reader_state = ?DebugExternalsRangeReaderState(&reader.reader_state),
            "externals reader: created next range reader",
        );

        reader
    }

    pub fn read_into_buffers(
        &mut self,
        read_mode: GetNextMessageGroupMode,
        partition_router: &PartitionRouter,
    ) -> Result<MessagesReaderMetricsByPartitions> {
        let mut metrics_by_partitions = MessagesReaderMetricsByPartitions::default();

        // skip if all open ranges fully read
        if self.all_ranges_fully_read {
            tracing::trace!(target: tracing_targets::COLLATOR,
                "externals reader: all_ranges_fully_read=true",
            );
            return Ok(metrics_by_partitions);
        }

        let processed_to_by_partitions: BTreeMap<_, _> = self
            .reader_state
            .by_partitions
            .iter()
            .map(|(par_id, par)| (*par_id, par.processed_to))
            .collect();

        let mut ranges_seqno: VecDeque<_> = self.range_readers.keys().copied().collect();
        let mut last_seqno = 0;

        let mut last_ext_read_res_opt = None;

        let externals_expire_timeout =
            self.msgs_exec_params.current().externals_expire_timeout as u64;

        'main_loop: loop {
            let mut all_ranges_fully_read = true;
            while let Some(seqno) = ranges_seqno.pop_front() {
                let range_reader = self.range_readers.get_mut(&seqno).unwrap_or_else(||
                    panic!(
                        "externals range reader should exist (for_shard_id: {}, seqno: {}, block_seqno: {})",
                        self.for_shard_id, seqno, self.block_seqno,
                    )
                );

                tracing::trace!(target: tracing_targets::COLLATOR,
                    seqno,
                    fully_read = range_reader.fully_read,
                    range_reader_state = ?DebugExternalsRangeReaderState(&range_reader.reader_state),
                    "externals reader: try to read externals from range,"
                );

                // remember last existing range
                last_seqno = seqno;

                // skip fully read ranges
                if range_reader.fully_read {
                    continue;
                }

                // on refill skip last range reader created in this block
                if read_mode == GetNextMessageGroupMode::Refill && seqno == self.block_seqno {
                    all_ranges_fully_read = false;
                    continue;
                }

                // Do not read range until skip offset reached. In any partition.
                // Current offset is updated after reading and before importing,
                // so if current offset == skip offset here, it will be greater on importing,
                // and in this case we need to read messages
                if self.reader_state.by_partitions.iter().all(|(par_id, s)| {
                    s.curr_processed_offset
                        < range_reader
                            .reader_state
                            .by_partitions
                            .get(par_id)
                            .map(|s| s.skip_offset)
                            .unwrap_or_default()
                }) {
                    tracing::trace!(target: tracing_targets::COLLATOR,
                        externals_reader_state = ?DebugIter(self.reader_state.by_partitions.iter()),
                        "externals reader: skip offset not reached in all partitions",
                    );
                    all_ranges_fully_read = false;
                    continue;
                }

                // read externals, on refill only up to previous read_to
                let read_mode =
                    if seqno == self.block_seqno && read_mode != GetNextMessageGroupMode::Refill {
                        ReadNextExternalsMode::ToTheEnd
                    } else {
                        ReadNextExternalsMode::ToPreviuosReadTo
                    };

                let mut read_res = range_reader.read_externals_into_buffers(
                    &mut self.anchors_cache,
                    read_mode,
                    partition_router,
                    &processed_to_by_partitions,
                    externals_expire_timeout,
                )?;

                metrics_by_partitions.append(std::mem::take(&mut read_res.metrics_by_partitions));

                // if range was not fully read then buffer is full
                // and we should continue to read current range
                // next time the method is called
                if !range_reader.fully_read {
                    all_ranges_fully_read = false;
                } else if seqno == self.block_seqno {
                    // if current range is a last one and fully read
                    // then set current position to the end of the last imported anchor
                    if let Some(anchor_info) = self.anchors_cache.last_imported_anchor_info() {
                        range_reader.reader_state.range.current_position = ExternalKey {
                            anchor_id: anchor_info.id,
                            msgs_offset: anchor_info.all_exts_count as u64,
                        };
                        range_reader.reader_state.range.to =
                            range_reader.reader_state.range.current_position;
                    }
                }

                let max_fill_state_by_slots = read_res.max_fill_state_by_slots;
                let max_fill_state_by_count = read_res.max_fill_state_by_count;

                last_ext_read_res_opt = Some(read_res);

                // do not try to read from the next range
                // if we already can fill all slots in messages group from almost one buffer
                if max_fill_state_by_slots == BufferFillStateBySlots::CanFill {
                    break 'main_loop;
                }

                // do not try to read from the next range
                // if buffers are full but current range was not fully read
                // this is actual for refill when we need to read from multiple ranges
                if max_fill_state_by_count == BufferFillStateByCount::IsFull
                    && !range_reader.fully_read
                {
                    break 'main_loop;
                }
            }

            // if all ranges fully read try create next one
            if all_ranges_fully_read {
                if last_seqno < self.block_seqno {
                    if !self.open_ranges_limit_reached() {
                        if read_mode == GetNextMessageGroupMode::Continue {
                            if self.msgs_exec_params.new_is_some() {
                                tracing::debug!(target: tracing_targets::COLLATOR,
                                    last_seqno,
                                    "externals reader: do not create next range reader on Continue because new message exec params exists",
                                );
                                self.all_ranges_fully_read = true;
                                break;
                            }

                            self.create_append_next_range_reader();
                            ranges_seqno.push_back(self.block_seqno);
                        } else {
                            // do not create next range reader on refill
                            tracing::debug!(target: tracing_targets::COLLATOR,
                                last_seqno,
                                "externals reader: do not create next range reader on Refill",
                            );
                            self.all_ranges_fully_read = true;
                            break;
                        }
                    } else {
                        // otherwise set all open ranges read
                        // to collect messages from all open ranges
                        // and then create next range
                        tracing::debug!(target: tracing_targets::COLLATOR,
                            last_seqno,
                            open_ranges_limit = self.msgs_exec_params.current().open_ranges_limit,
                            "externals reader: open ranges limit reached",
                        );
                        self.all_ranges_fully_read = true;
                        break;
                    }
                } else {
                    // if cannot create next one then store flag and exit
                    self.all_ranges_fully_read = true;
                    break;
                }
            } else {
                // exit when we stopped reading and range was not fully read
                break;
            }
        }

        if let Some(read_res) = last_ext_read_res_opt {
            // update last read to anchor chain time only from the last range read result
            if let Some(ct) = read_res.last_read_to_anchor_chain_time {
                self.reader_state.last_read_to_anchor_chain_time = Some(ct);
            }
        }

        Ok(metrics_by_partitions)
    }

    pub fn collect_messages<V: InternalMessageValue>(
        &mut self,
        par_id: QueuePartitionIdx,
        msg_group: &mut MessageGroup,
        curr_partition_reader: Option<&InternalsPartitionReader<V>>,
        prev_partitions_readers: &BTreeMap<QueuePartitionIdx, InternalsPartitionReader<V>>,
        prev_msg_groups: &BTreeMap<QueuePartitionIdx, MessageGroup>,
        already_skipped_accounts: &mut FastHashSet<HashBytes>,
    ) -> Result<CollectExternalsResult> {
        let mut res = CollectExternalsResult::default();

        let buffer_limits = self.get_buffer_limits_by_partition(par_id)?;

        let curr_processed_offset = self
            .reader_state
            .get_state_by_partition(par_id)?
            .curr_processed_offset;

        // find actual chain time from range readers according to current processed offset
        // to check expired externals in buffers during collect
        let mut next_chain_time = 0;
        for reader in self.range_readers.values() {
            let range_reader_state_by_partition =
                reader.reader_state.get_state_by_partition(par_id)?;
            if curr_processed_offset > range_reader_state_by_partition.skip_offset {
                next_chain_time = reader.reader_state.range.chain_time;
            }
        }
        if next_chain_time == 0 {
            next_chain_time = self.next_chain_time;
        }
        anyhow::ensure!(next_chain_time > 0);

        let externals_expire_timeout_ms =
            self.msgs_exec_params.current().externals_expire_timeout as u64 * 1000;
        let mut expired_msgs_count = 0;

        // extract range readers from state to use previous readers buffers and stats
        // to check for account skip on collecting messages from the next
        let mut range_readers = BTreeMap::<BlockSeqno, ExternalsRangeReader>::new();
        while let Some((seqno, mut reader)) = self.pop_first_range_reader() {
            let range_reader_state_by_partition =
                reader.reader_state.get_state_by_partition_mut(par_id)?;

            // skip up to skip offset
            if curr_processed_offset > range_reader_state_by_partition.skip_offset {
                // setup messages filter
                // do not check for expired externals if check chain time was not changed
                // or if minimal chain time in buffer is above the threshold
                let mut msg_filter = MsgFilter::IncludeAll(IncludeAllMessages);
                if matches!(range_reader_state_by_partition.last_expire_check_on_ct, Some(last) if next_chain_time > last)
                    || range_reader_state_by_partition
                        .last_expire_check_on_ct
                        .is_none()
                {
                    range_reader_state_by_partition.last_expire_check_on_ct = Some(next_chain_time);
                    let chain_time_threshold_ms =
                        next_chain_time.saturating_sub(externals_expire_timeout_ms);
                    if range_reader_state_by_partition.buffer.min_ext_chain_time()
                        < chain_time_threshold_ms
                    {
                        msg_filter = MsgFilter::SkipExpiredExternals(SkipExpiredExternals {
                            chain_time_threshold_ms,
                            total_skipped: &mut expired_msgs_count,
                        });
                    }
                }

                res.metrics.add_to_message_groups_timer.start();
                let FillMessageGroupResult {
                    ops_count,
                    collected_count,
                    ..
                } = range_reader_state_by_partition.buffer.fill_message_group(
                    msg_group,
                    buffer_limits.slots_count,
                    buffer_limits.slot_vert_size,
                    already_skipped_accounts,
                    |account_id| {
                        let mut check_ops_count = 0;

                        let dst_addr =
                            IntAddr::from((self.for_shard_id.workchain() as i8, *account_id));

                        // check by msg group from previous partition (e.g. from partition 0 when collecting from 1)
                        for msg_group in prev_msg_groups.values() {
                            if msg_group.messages_count() > 0 {
                                check_ops_count.saturating_add_assign(1);
                                if msg_group.contains_account(account_id) {
                                    tracing::trace!(target: tracing_targets::COLLATOR,
                                        partition_id = %par_id,
                                        account_id = %get_short_hash_string(account_id),
                                        "external messages skipped for account - msg_group of prev partition",
                                    );
                                    return (true, check_ops_count);
                                }
                            }
                        }

                        // check by previous partitions
                        // NOTE: we can consider all readers and full stats from previous partitions
                        //      (e.g. from 0 when processing 1) even on refill because when account A
                        //      is moved from partition 0 to 1 all remaning messages for account A
                        //      always a from previous blocks so we cannot wrongly read in advance
                        for prev_par_reader in prev_partitions_readers.values() {
                            // check buffers in previous partition
                            for prev_par_range_reader in prev_par_reader.range_readers().values() {
                                if prev_par_range_reader.reader_state.buffer.msgs_count() > 0 {
                                    check_ops_count.saturating_add_assign(1);
                                    if prev_par_range_reader
                                        .reader_state
                                        .buffer
                                        .account_messages_count(account_id)
                                        > 0
                                    {
                                        tracing::trace!(target: tracing_targets::COLLATOR,
                                            partition_id = %par_id,
                                            account_id = %get_short_hash_string(account_id),
                                            "external messages skipped for account - prev partition range reader buffer",
                                        );
                                        return (true, check_ops_count);
                                    }
                                }
                            }

                            // check stats in previous partition
                            check_ops_count.saturating_add_assign(1);
                            if let Some(remaning_msgs_stats) = &prev_par_reader.remaning_msgs_stats
                                && remaning_msgs_stats.statistics().contains_key(&dst_addr)
                            {
                                tracing::trace!(target: tracing_targets::COLLATOR,
                                    partition_id = %par_id,
                                    account_id = %get_short_hash_string(account_id),
                                    "external messages skipped for account - prev partition reader remaning stats",
                                );
                                return (true, check_ops_count);
                            }
                        }

                        // check by previous externals ranges
                        for prev_reader in range_readers.values() {
                            // check buffer
                            let buffer = &prev_reader
                                .reader_state
                                .get_state_by_partition(par_id)
                                .unwrap()
                                .buffer;
                            if buffer.msgs_count() > 0 {
                                check_ops_count.saturating_add_assign(1);
                                if buffer.account_messages_count(account_id) > 0 {
                                    tracing::trace!(target: tracing_targets::COLLATOR,
                                        partition_id = %par_id,
                                        account_id = %get_short_hash_string(account_id),
                                        "external messages skipped for account - prev externals range reader buffer",
                                    );
                                    return (true, check_ops_count);
                                }
                            }
                        }

                        // check by current partition internals reader
                        if let Some(curr_partition_reader) = curr_partition_reader {
                            // check current partition internals range readers
                            for curr_par_range_reader in curr_partition_reader.range_readers().values()
                            {
                                // we omit internals range reader for new messages
                                if matches!(curr_par_range_reader.kind, InternalsRangeReaderKind::NewMessages) {
                                    break;
                                }

                                // NOTE: we use only range readers which skip offset is below current offset.
                                //      It is required on refill not to take into account messages from next ranges.
                                //      E.g. we collated blocks 10, 11, 12. Then on refill, when current offset corresponds
                                //      to block 11 we should not take into account messages from block 12.
                                if curr_processed_offset <= curr_par_range_reader.reader_state.skip_offset {
                                    break;
                                }

                                // check buffer
                                if curr_par_range_reader.reader_state.buffer.msgs_count() > 0 {
                                    check_ops_count.saturating_add_assign(1);
                                    if curr_par_range_reader
                                        .reader_state
                                        .buffer
                                        .account_messages_count(account_id)
                                        > 0
                                    {
                                        tracing::trace!(target: tracing_targets::COLLATOR,
                                            partition_id = %par_id,
                                            account_id = %get_short_hash_string(account_id),
                                            rr_seqno = curr_par_range_reader.seqno,
                                            rr_kind = ?curr_par_range_reader.kind,
                                            reader_state = ?DebugInternalsRangeReaderState(&curr_par_range_reader.reader_state),
                                            "external messages skipped for account - current partition range reader buffer",
                                        );
                                        return (true, check_ops_count);
                                    }
                                }

                                // check in remaning stats
                                check_ops_count.saturating_add_assign(1);
                                if curr_par_range_reader
                                    .reader_state.contains_account_addr_in_remaning_msgs_stats(&dst_addr)
                                {
                                    tracing::trace!(target: tracing_targets::COLLATOR,
                                        partition_id = %par_id,
                                        account_id = %get_short_hash_string(account_id),
                                        rr_seqno = curr_par_range_reader.seqno,
                                        rr_kind = ?curr_par_range_reader.kind,
                                        reader_state = ?DebugInternalsRangeReaderState(&curr_par_range_reader.reader_state),
                                        remaming_msgs_stats = ?curr_par_range_reader
                                            .reader_state
                                            .remaning_msgs_stats.as_ref()
                                            .map(|stats| DebugIter(stats.statistics().iter().map(|(addr, count)|
                                                (get_short_addr_string(addr), *count)
                                            ))),
                                        "external messages skipped for account - current partition range reader remaning stats",
                                    );
                                    return (true, check_ops_count);
                                }
                            }
                        }

                        (false, check_ops_count)
                    },
                    msg_filter,
                );
                res.metrics
                    .add_to_msgs_groups_ops_count
                    .saturating_add_assign(ops_count);
                res.metrics.add_to_message_groups_timer.stop();
                res.collected_count.saturating_add_assign(collected_count);
            }

            let range_reader_processed_offset = range_reader_state_by_partition.processed_offset;

            range_readers.insert(seqno, reader);

            // collect messages from the next range
            // only when current range processed offset is reached
            if curr_processed_offset <= range_reader_processed_offset {
                break;
            }
        }
        // return range readers to state
        self.set_range_readers(range_readers);

        // metrics: expired externals
        res.metrics.expired_ext_msgs_count += expired_msgs_count;

        tracing::debug!(target: tracing_targets::COLLATOR,
            partition_id = %par_id,
            ext_curr_processed_offset = curr_processed_offset,
            ext_msgs_collected_count = res.collected_count,
            expired_ext_msgs_count = res.metrics.expired_ext_msgs_count,
            "external messages collected",
        );

        let labels = [("workchain", self.for_shard_id.workchain().to_string())];
        metrics::counter!("tycho_do_collate_ext_msgs_expired_count", &labels)
            .increment(expired_msgs_count);

        Ok(res)
    }
}

#[derive(Default)]
pub(super) struct CollectExternalsResult {
    pub metrics: MessagesReaderMetrics,
    pub collected_count: usize,
}

pub(super) struct ExternalsRangeReader {
    for_shard_id: ShardIdent,
    seqno: BlockSeqno,
    /// Target limits for filling message group from the buffer
    buffer_limits_by_partitions: BTreeMap<QueuePartitionIdx, MessagesBufferLimits>,
    reader_state: ExternalsRangeReaderState,
    fully_read: bool,
}

impl ExternalsRangeReader {
    pub fn reader_state(&self) -> &ExternalsRangeReaderState {
        &self.reader_state
    }

    fn get_buffer_limits_by_partition(
        &self,
        partitions_id: &QueuePartitionIdx,
    ) -> Result<&MessagesBufferLimits> {
        self.buffer_limits_by_partitions
            .get(partitions_id)

            .with_context(|| format!(
                "externals range reader does not contain buffer limits for partition {} (for_shard_id: {}, seqno: {})",
                partitions_id, self.for_shard_id, self.seqno,
            ))
    }

    pub fn get_max_buffers_fill_state(
        &self,
    ) -> Result<(BufferFillStateByCount, BufferFillStateBySlots)> {
        let mut fill_state_by_count = BufferFillStateByCount::NotFull;
        let mut fill_state_by_slots = BufferFillStateBySlots::CanNotFill;

        for (par_id, par) in &self.reader_state.by_partitions {
            let buffer_limits = self.get_buffer_limits_by_partition(par_id)?;
            let (par_fill_state_by_count, par_fill_state_by_slots) =
                par.buffer.check_is_filled(buffer_limits);
            if par_fill_state_by_count == BufferFillStateByCount::IsFull {
                fill_state_by_count = BufferFillStateByCount::IsFull;
            }
            if par_fill_state_by_slots == BufferFillStateBySlots::CanFill {
                fill_state_by_slots = BufferFillStateBySlots::CanFill;
            }
            if matches!(
                (&fill_state_by_count, &fill_state_by_slots),
                (
                    BufferFillStateByCount::IsFull,
                    BufferFillStateBySlots::CanFill
                )
            ) {
                break;
            }
        }

        Ok((fill_state_by_count, fill_state_by_slots))
    }

    #[tracing::instrument(skip_all)]
    fn read_externals_into_buffers(
        &mut self,
        anchors_cache: &mut AnchorsCache,
        read_mode: ReadNextExternalsMode,
        partition_router: &PartitionRouter,
        processed_to_by_partitions: &BTreeMap<QueuePartitionIdx, ExternalKey>,
        externals_expire_timeout: u64,
    ) -> Result<ReadExternalsRangeResult> {
        let labels = [("workchain", self.for_shard_id.workchain().to_string())];

        let next_chain_time = self.reader_state.range.chain_time;

        tracing::info!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
            next_chain_time,
            ?read_mode,
            fully_read = self.fully_read,
            "read externals",
        );

        let mut metrics_by_partitions = MessagesReaderMetricsByPartitions::default();
        metrics_by_partitions
            .get_mut(QueuePartitionIdx::ZERO)
            .read_ext_messages_timer
            .start();

        let was_read_to = self.reader_state.range.current_position;
        let prev_to = self.reader_state.range.to;

        let mut prev_to_reached = false;

        // check if buffer is full
        // or we can already fill required slots
        let (mut max_fill_state_by_count, mut max_fill_state_by_slots) =
            self.get_max_buffers_fill_state()?;
        let mut has_filled_buffer = matches!(
            (&max_fill_state_by_count, &max_fill_state_by_slots),
            (BufferFillStateByCount::IsFull, _) | (_, BufferFillStateBySlots::CanFill)
        );

        let mut last_read_to_anchor_chain_time = None;
        let mut msgs_read_offset_in_last_anchor;
        let mut has_pending_externals_in_last_read_anchor = false;

        let mut total_msgs_imported = 0;

        let mut count_expired_anchors = 0_u32;
        let mut count_expired_messages = 0_u64;

        // read anchors from cache
        let next_idx = 0;
        loop {
            // try read next anchor
            let next_entry = anchors_cache.get(next_idx);
            let (anchor_id, anchor) = match next_entry {
                Some(entry) => entry,
                // stop reading if there is no next anchor
                None => {
                    tracing::debug!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
                        "no next entry in anchors cache",
                    );
                    self.fully_read = true;
                    break;
                }
            };

            // skip and remove already read anchor from cache
            if anchor_id < was_read_to.anchor_id {
                assert_eq!(next_idx, 0);
                anchors_cache.pop_front();
                tracing::debug!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
                    anchor_id,
                    "anchor already read, removed from anchors cache",
                );
                // try read next anchor
                continue;
            }

            last_read_to_anchor_chain_time = Some(anchor.chain_time);
            tracing::debug!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
                last_read_anchor_id = anchor_id,
                last_read_anchor_chain_time = anchor.chain_time,
            );

            // detect messages read offset for current anchor
            if anchor_id == was_read_to.anchor_id {
                // read first anchor from offset in processed upto
                msgs_read_offset_in_last_anchor = was_read_to.msgs_offset;
            } else {
                // read every next anchor from 0
                msgs_read_offset_in_last_anchor = 0;
            }

            tracing::debug!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
                anchor_id,
                msgs_read_offset_in_last_anchor,
                "externals count: {}", anchor.externals.len(),
            );

            // possibly prev_to already reached
            prev_to_reached = anchor_id > prev_to.anchor_id
                || (anchor_id == prev_to.anchor_id
                    && msgs_read_offset_in_last_anchor == prev_to.msgs_offset);

            // skip expired anchor
            let externals_expire_timeout_ms = externals_expire_timeout * 1000;

            if next_chain_time.saturating_sub(anchor.chain_time) > externals_expire_timeout_ms {
                let iter = anchor.iter_externals(msgs_read_offset_in_last_anchor as usize);
                let mut expired_msgs_count = 0;
                for ext_msg in iter {
                    if self.for_shard_id.contains_address(&ext_msg.info.dst) {
                        tracing::trace!(target: tracing_targets::COLLATOR,
                            anchor_id,
                            anchor_chain_time = anchor.chain_time,
                            next_chain_time,
                            "ext_msg hash: {}, dst: {} is expired by timeout {} ms",
                            ext_msg.hash(), ext_msg.info.dst, externals_expire_timeout_ms,
                        );
                        expired_msgs_count += 1;

                        // update expired messages count in reader metrics
                        let target_partition =
                            partition_router.get_partition(None, &ext_msg.info.dst);
                        let par_metrics = metrics_by_partitions.get_mut(target_partition);
                        par_metrics.expired_ext_msgs_count += 1;
                    }
                }

                // skip and remove expired anchor
                assert_eq!(next_idx, 0);
                anchors_cache.pop_front();

                tracing::debug!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
                    anchor_id,
                    anchor_chain_time = anchor.chain_time,
                    next_chain_time,
                    expired_msgs_count,
                    "anchor fully skipped due to expiration, removed from anchors cache",
                );

                count_expired_anchors = count_expired_anchors.saturating_add(1);
                count_expired_messages = count_expired_messages.saturating_add(expired_msgs_count);

                // update current position
                let curr_ext_key = ExternalKey {
                    anchor_id,
                    msgs_offset: msgs_read_offset_in_last_anchor,
                };
                self.reader_state.range.current_position = curr_ext_key;
                if self.reader_state.range.current_position > self.reader_state.range.to {
                    self.reader_state.range.to = self.reader_state.range.current_position;
                }

                // try read next anchor
                continue;
            }

            // import messages from anchor
            let mut msgs_imported_from_last_anchor = 0;
            let iter = anchor.iter_externals(msgs_read_offset_in_last_anchor as usize);
            for ext_msg in iter {
                tracing::trace!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
                    anchor_id,
                    "read ext_msg dst: {}", ext_msg.info.dst,
                );

                // add msg to buffer if it is not filled and prev_to not reached
                if !(has_filled_buffer
                    || read_mode == ReadNextExternalsMode::ToPreviuosReadTo && prev_to_reached)
                {
                    msgs_read_offset_in_last_anchor += 1;

                    // update current position
                    let curr_ext_key = ExternalKey {
                        anchor_id,
                        msgs_offset: msgs_read_offset_in_last_anchor,
                    };
                    self.reader_state.range.current_position = curr_ext_key;
                    if self.reader_state.range.current_position > self.reader_state.range.to {
                        self.reader_state.range.to = self.reader_state.range.current_position;
                    }

                    // check if prev_to reached
                    prev_to_reached = anchor_id > prev_to.anchor_id
                        || (anchor_id == prev_to.anchor_id
                            && msgs_read_offset_in_last_anchor == prev_to.msgs_offset);

                    if self.for_shard_id.contains_address(&ext_msg.info.dst) {
                        // detect target partition and add message to buffer
                        let target_partition =
                            partition_router.get_partition(None, &ext_msg.info.dst);
                        let par_metrics = metrics_by_partitions.get_mut(target_partition);
                        // we use one anchors cache for all partitions
                        // and read externals into all partitions at once
                        // so we add message to buffer only when it is above processed_to for partition
                        let processed_to =
                            processed_to_by_partitions.get(&target_partition).unwrap();
                        if &curr_ext_key > processed_to {
                            let reader_state_by_partition = self
                                .reader_state
                                .by_partitions
                                .get_mut(&target_partition)
                                .with_context(|| format!(
                                    "target partition {} should exist in range reader state (seqno={})",
                                    target_partition, self.seqno,
                                ))?;
                            reader_state_by_partition
                                .buffer
                                .add_message(ParsedMessage::new(
                                    MsgInfo::ExtIn(ext_msg.info.clone()),
                                    true,
                                    ext_msg.cell.clone(),
                                    None,
                                    None,
                                    None,
                                    Some(anchor.chain_time),
                                ));
                            par_metrics
                                .add_to_msgs_groups_ops_count
                                .saturating_add_assign(1);
                        }
                        par_metrics.add_to_message_groups_timer.stop();

                        par_metrics.read_ext_msgs_count += 1;

                        total_msgs_imported += 1;
                        msgs_imported_from_last_anchor += 1;

                        tracing::trace!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
                            anchor_id,
                            "imported ext_msg dst: {}", ext_msg.info.dst,
                        );

                        // check if buffer is full
                        // or we can already fill required slots
                        (max_fill_state_by_count, max_fill_state_by_slots) =
                            self.get_max_buffers_fill_state()?;
                        has_filled_buffer = matches!(
                            (&max_fill_state_by_count, &max_fill_state_by_slots),
                            (BufferFillStateByCount::IsFull, _)
                                | (_, BufferFillStateBySlots::CanFill)
                        );
                    }
                }
                // otherwise check if has pending externals in the anchor
                else if self.for_shard_id.contains_address(&ext_msg.info.dst) {
                    has_pending_externals_in_last_read_anchor = true;
                    break;
                }
            }

            tracing::debug!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
                anchor_id,
                msgs_read_offset_in_last_anchor,
                msgs_imported_from_last_anchor,
            );

            // remove fully read anchor
            if anchor.externals.len() == msgs_read_offset_in_last_anchor as usize {
                assert_eq!(next_idx, 0);
                anchors_cache.pop_front();

                tracing::debug!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
                    anchor_id,
                    "anchor just fully read, removed from anchors cache",
                );
            }

            // stop reading when prev_to reached
            if read_mode == ReadNextExternalsMode::ToPreviuosReadTo && prev_to_reached {
                tracing::debug!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
                    "stopped reading externals when prev_to reached: ({}, {})",
                    prev_to.anchor_id, prev_to.msgs_offset,
                );
                self.fully_read = true;
                break;
            }

            // stop reading when buffer filled
            if has_filled_buffer {
                break;
            }
        }

        if matches!(max_fill_state_by_slots, BufferFillStateBySlots::CanFill) {
            tracing::debug!(target: tracing_targets::COLLATOR,
                reader_state = ?DebugExternalsRangeReaderState(&self.reader_state),
                "externals reader: can fully fill all slots in message group",
            );
        } else if matches!(max_fill_state_by_count, BufferFillStateByCount::IsFull) {
            tracing::debug!(target: tracing_targets::COLLATOR,
                max_msgs_limits = ?DebugIter(self.buffer_limits_by_partitions.iter().map(|(par_id, limits)| (par_id, limits.max_count))),
                reader_state = ?DebugExternalsRangeReaderState(&self.reader_state),
                "externals reader: messages buffers filled up to limits",
            );
        }

        // check if we still have pending externals in range
        let has_pending_externals_in_range =
            if read_mode == ReadNextExternalsMode::ToPreviuosReadTo && prev_to_reached {
                // when was reading to prev_to and reached it we consider then
                // we do not have pending externals in the range
                false
            } else if has_pending_externals_in_last_read_anchor {
                // when we stopped reading and has pending externals in last anchor
                true
            } else if read_mode == ReadNextExternalsMode::ToPreviuosReadTo {
                // when was reading to prev_to and not reached it
                // then check by cache in the range
                anchors_cache.check_has_pending_externals_in_range(&prev_to)
            } else {
                // when was reading to the end then get from cache
                anchors_cache.has_pending_externals()
            };

        tracing::info!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
            total_msgs_imported,
            count_expired_messages,
            has_pending_externals_in_last_read_anchor,
            has_pending_externals_in_range,
            ?read_mode,
        );

        // report metrics
        metrics::gauge!("tycho_collator_ext_msgs_imported_queue_size", &labels)
            .decrement((total_msgs_imported + count_expired_messages) as f64);
        if count_expired_messages > 0 {
            metrics::counter!("tycho_do_collate_ext_msgs_expired_count", &labels)
                .increment(count_expired_messages);
        }

        // accumulate time metrics
        {
            metrics_by_partitions
                .get_mut(QueuePartitionIdx::ZERO)
                .read_ext_messages_timer
                .stop();
            let add_to_msgs_groups_total_elapsed =
                metrics_by_partitions.add_to_message_groups_total_elapsed();
            let par_0_metrics = metrics_by_partitions.get_mut(QueuePartitionIdx::ZERO);
            par_0_metrics.read_ext_messages_timer.stop();
            par_0_metrics.read_ext_messages_timer.total_elapsed -= add_to_msgs_groups_total_elapsed;
        }

        Ok(ReadExternalsRangeResult {
            last_read_to_anchor_chain_time,
            max_fill_state_by_count,
            max_fill_state_by_slots,
            metrics_by_partitions,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReadNextExternalsMode {
    ToTheEnd,
    ToPreviuosReadTo,
}

#[derive(Default)]
struct ReadExternalsRangeResult {
    /// The chain time of the last read anchor.
    /// Used to calc externals time diff.
    last_read_to_anchor_chain_time: Option<u64>,

    /// Shows if messages buffer is fully filled in any partition
    max_fill_state_by_count: BufferFillStateByCount,

    /// Shows if we can fill all slots in message group from almost one buffer
    max_fill_state_by_slots: BufferFillStateBySlots,

    metrics_by_partitions: MessagesReaderMetricsByPartitions,
}
