use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use anyhow::{Context, Result};
use everscale_types::models::{IntAddr, MsgInfo, MsgsExecutionParams, ShardIdent};
use tycho_block_util::queue::QueuePartitionIdx;

use super::{
    DebugExternalsRangeReaderState, ExternalKey, ExternalsRangeReaderState,
    ExternalsRangeReaderStateByPartition, ExternalsReaderRange, ExternalsReaderState,
    GetNextMessageGroupMode, InternalsParitionReader, MessagesReaderMetrics,
};
use crate::collator::messages_buffer::{
    BufferFillStateByCount, BufferFillStateBySlots, FillMessageGroupResult, MessageGroup,
    MessagesBufferLimits, SaturatingAddAssign,
};
use crate::collator::types::{AnchorsCache, MsgsExecutionParamsExtension, ParsedMessage};
use crate::internal_queue::types::PartitionRouter;
use crate::tracing_targets;
use crate::types::processed_upto::BlockSeqno;

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
    msgs_exec_params: Arc<MsgsExecutionParams>,
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
        msgs_exec_params: Arc<MsgsExecutionParams>,
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

        reader.create_existing_range_readers();

        reader
    }

    pub fn finalize(mut self) -> Result<FinalizedExternalsReader> {
        // collect range reader states
        let mut range_readers = self.range_readers.into_iter().peekable();
        let mut max_processed_offsets = BTreeMap::<QueuePartitionIdx, u32>::new();
        while let Some((seqno, mut range_reader)) = range_readers.next() {
            // TODO: msgs-v3: update offset in the last range reader on the go?

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
        self.range_readers.len() >= self.msgs_exec_params.open_ranges_limit()
    }

    pub fn reader_state(&self) -> &ExternalsReaderState {
        &self.reader_state
    }

    pub fn get_partition_ids(&self) -> Vec<QueuePartitionIdx> {
        self.reader_state.by_partitions.keys().copied().collect()
    }

    pub fn last_read_to_anchor_chain_time(&self) -> Option<u64> {
        self.reader_state.last_read_to_anchor_chain_time
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

    pub fn has_non_zero_processed_offset(&self) -> bool {
        self.range_readers.values().any(|r| {
            r.reader_state
                .by_partitions
                .values()
                .any(|par| par.processed_offset > 0)
        })
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

    pub fn count_messages_in_buffers(&self) -> usize {
        self.range_readers
            .values()
            .map(|v| {
                v.reader_state
                    .by_partitions
                    .values()
                    .map(|par| par.buffer.msgs_count())
                    .sum::<usize>()
            })
            .sum()
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
        self.range_readers.values().all(|v| {
            v.fully_read
                && v.reader_state
                    .by_partitions
                    .values()
                    .all(|par| par.buffer.msgs_count() == 0)
        })
    }

    pub fn has_pending_externals(&self) -> bool {
        self.anchors_cache.has_pending_externals()
    }

    pub fn retain_only_last_range_reader(&mut self) -> Result<()> {
        let (last_seqno, last_range_reader) = self.range_readers.pop_last().context(
            "externals reader should have at least one range reader after reading into buffer",
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
        self.range_readers.last_key_value().context(
            "externals reader should have at least one range reader after reading into buffer",
        )
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
            .with_context(|| {
                format!("externals reader state not exists for partition {}", par_id)
            })?;
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

    pub fn set_skip_offset_to_current(&mut self, par_id: QueuePartitionIdx) -> Result<()> {
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
            msgs_exec_params: self.msgs_exec_params.clone(),
            buffer_limits_by_partitions: self.buffer_limits_by_partitions.clone(),
            fully_read: range_reader_state.range.current_position == range_reader_state.range.to,
            reader_state: range_reader_state,
        };

        tracing::debug!(target: tracing_targets::COLLATOR,
            for_shard_id = %self.for_shard_id,
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
        let (from, processed_offsets_by_partitions) = self
            .range_readers
            .values()
            .last()
            .map(|r| {
                (
                    r.reader_state.range.to,
                    r.reader_state
                        .by_partitions
                        .iter()
                        .map(|(par_id, par)| (*par_id, par.processed_offset))
                        .collect::<BTreeMap<_, _>>(),
                )
            })
            .unwrap_or_default();

        // create range reader states by partitions
        let mut by_partitions = BTreeMap::new();
        for par_id in self.reader_state.by_partitions.keys() {
            let processed_offset = processed_offsets_by_partitions
                .get(par_id)
                .cloned()
                .unwrap_or_default();
            by_partitions.insert(*par_id, ExternalsRangeReaderStateByPartition {
                buffer: Default::default(),
                skip_offset: processed_offset,
                processed_offset,
            });
        }

        let reader = ExternalsRangeReader {
            for_shard_id: self.for_shard_id,
            seqno: self.block_seqno,
            msgs_exec_params: self.msgs_exec_params.clone(),
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
            for_shard_id = %self.for_shard_id,
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
    ) -> MessagesReaderMetrics {
        let mut metrics = MessagesReaderMetrics::default();

        // skip if all open ranges fully read
        if self.all_ranges_fully_read {
            return metrics;
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

        'main_loop: loop {
            let mut all_ranges_fully_read = true;
            while let Some(seqno) = ranges_seqno.pop_front() {
                let range_reader = self.range_readers.get_mut(&seqno).unwrap_or_else(||
                    panic!(
                        "externals range reader should exist (for_shard_id: {}, seqno: {}, block_seqno: {})",
                        self.for_shard_id, seqno, self.block_seqno,
                    )
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
                );

                metrics.append(std::mem::take(&mut read_res.metrics));

                // if range was not fully read then buffer is full
                // and we should continue to read current range
                // next time the method is called
                if !range_reader.fully_read {
                    all_ranges_fully_read = false;
                } else if seqno == self.block_seqno {
                    // if current range is a last one and fully read
                    // then set current position to the end of the last imported anchor
                    if let Some(last_imported_anchor) = self.anchors_cache.last_imported_anchor() {
                        range_reader.reader_state.range.current_position = ExternalKey {
                            anchor_id: last_imported_anchor.id,
                            msgs_offset: last_imported_anchor.all_exts_count as u64,
                        };
                        range_reader.reader_state.range.to =
                            range_reader.reader_state.range.current_position;
                    }
                }

                let max_fill_state_by_slots = read_res.max_fill_state_by_slots;

                last_ext_read_res_opt = Some(read_res);

                // do not try to read from the next range
                // if we already can fill all slots in messages group from almost one buffer
                if max_fill_state_by_slots == BufferFillStateBySlots::CanFill {
                    break 'main_loop;
                }
            }

            // if all ranges fully read try create next one
            if all_ranges_fully_read {
                if last_seqno < self.block_seqno {
                    if !self.open_ranges_limit_reached() {
                        if read_mode == GetNextMessageGroupMode::Continue {
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
                            open_ranges_limit = self.msgs_exec_params.open_ranges_limit,
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

            // update the pending externals flag from the actual range (created in current block)
            if last_seqno == self.block_seqno {
                self.anchors_cache
                    .set_has_pending_externals(read_res.has_pending_externals);
            }
        }

        metrics
    }

    pub fn collect_messages(
        &mut self,
        par_id: QueuePartitionIdx,
        msg_group: &mut MessageGroup,
        prev_partitions_readers: &BTreeMap<QueuePartitionIdx, InternalsParitionReader>,
        prev_msg_groups: &BTreeMap<QueuePartitionIdx, MessageGroup>,
    ) -> Result<CollectExternalsResult> {
        let mut res = CollectExternalsResult::default();

        let buffer_limits = self.get_buffer_limits_by_partition(par_id)?;

        let curr_processed_offset = self
            .reader_state
            .get_state_by_partition(par_id)?
            .curr_processed_offset;

        // extract range readers from state to use previous readers buffers and stats
        // to check for account skip on collecting messages from the next
        let mut range_readers = BTreeMap::<BlockSeqno, ExternalsRangeReader>::new();
        while let Some((seqno, mut reader)) = self.pop_first_range_reader() {
            let range_reader_state_by_partition =
                reader.reader_state.get_state_by_partition_mut(par_id)?;

            // skip up to skip offset
            if curr_processed_offset > range_reader_state_by_partition.skip_offset {
                res.metrics.add_to_message_groups_timer.start();
                let FillMessageGroupResult { ops_count, .. } =
                    range_reader_state_by_partition.buffer.fill_message_group(
                        msg_group,
                        buffer_limits.slots_count,
                        buffer_limits.slot_vert_size,
                        |account_id| {
                            let mut check_ops_count = 0;

                            let dst_addr =
                                IntAddr::from((self.for_shard_id.workchain() as i8, *account_id));

                            for msg_group in prev_msg_groups.values() {
                                if msg_group.messages_count() > 0 {
                                    check_ops_count.saturating_add_assign(1);
                                    if msg_group.contains_account(account_id) {
                                        return (true, check_ops_count);
                                    }
                                }
                            }

                            // check by previous partitions
                            for prev_partitions_reader in prev_partitions_readers.values() {
                                for prev_reader in prev_partitions_reader.range_readers().values() {
                                    if prev_reader.reader_state.buffer.msgs_count() > 0 {
                                        check_ops_count.saturating_add_assign(1);
                                        if prev_reader
                                            .reader_state
                                            .buffer
                                            .account_messages_count(account_id)
                                            > 0
                                        {
                                            return (true, check_ops_count);
                                        }
                                    }
                                    if !prev_reader.fully_read {
                                        check_ops_count.saturating_add_assign(1);
                                        if prev_reader
                                            .reader_state
                                            .contains_account_addr_in_remaning_msgs_stats(&dst_addr)
                                        {
                                            return (true, check_ops_count);
                                        }
                                    }
                                }
                            }

                            // check by previous ranges
                            for prev_reader in range_readers.values() {
                                let buffer = &prev_reader
                                    .reader_state
                                    .get_state_by_partition(par_id)
                                    .unwrap()
                                    .buffer;
                                if buffer.msgs_count() > 0 {
                                    check_ops_count.saturating_add_assign(1);
                                    if buffer.account_messages_count(account_id) > 0 {
                                        return (true, check_ops_count);
                                    }
                                }
                            }

                            (false, check_ops_count)
                        },
                    );
                res.metrics
                    .add_to_msgs_groups_ops_count
                    .saturating_add_assign(ops_count);
                res.metrics.add_to_message_groups_timer.stop();
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

        Ok(res)
    }
}

#[derive(Default)]
pub(super) struct CollectExternalsResult {
    pub metrics: MessagesReaderMetrics,
}

pub(super) struct ExternalsRangeReader {
    for_shard_id: ShardIdent,
    seqno: BlockSeqno,
    msgs_exec_params: Arc<MsgsExecutionParams>,
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

    #[tracing::instrument(skip_all, fields(for_shard_id = %self.for_shard_id, seqno = self.seqno))]
    fn read_externals_into_buffers(
        &mut self,
        anchors_cache: &mut AnchorsCache,
        read_mode: ReadNextExternalsMode,
        partition_router: &PartitionRouter,
        processed_to_by_partitions: &BTreeMap<QueuePartitionIdx, ExternalKey>,
    ) -> ReadExternalsRangeResult {
        let labels = [("workchain", self.for_shard_id.workchain().to_string())];

        let next_chain_time = self.reader_state.range.chain_time;

        tracing::info!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
            next_chain_time,
            ?read_mode,
            fully_read = self.fully_read,
            "read externals",
        );

        let mut metrics = MessagesReaderMetrics::default();
        metrics.read_ext_messages_timer.start();

        let was_read_to = self.reader_state.range.current_position;
        let prev_to = self.reader_state.range.to;

        let mut prev_to_reached = false;

        // check if buffer is full
        // or we can already fill required slots
        let (mut max_fill_state_by_count, mut max_fill_state_by_slots) =
            self.get_max_buffers_fill_state().unwrap(); // TODO: msgsv-v3: return error instead of panic
        let mut has_filled_buffer = matches!(
            (&max_fill_state_by_count, &max_fill_state_by_slots),
            (BufferFillStateByCount::IsFull, _) | (_, BufferFillStateBySlots::CanFill)
        );

        let mut last_read_anchor_id_opt = None;
        let mut last_read_to_anchor_chain_time = None;
        let mut msgs_read_offset_in_last_anchor;
        let mut has_pending_externals_in_last_read_anchor = false;
        let mut last_anchor_removed = false;

        let mut total_msgs_collected = 0;

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
                anchors_cache.remove(next_idx);
                tracing::debug!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
                    anchor_id,
                    "anchor already read, removed from anchors cache",
                );
                // try read next anchor
                continue;
            }

            last_read_anchor_id_opt = Some(anchor_id);
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
            let externals_expire_timeout_ms =
                self.msgs_exec_params.externals_expire_timeout as u64 * 1000;

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
                    }
                }

                metrics::counter!("tycho_do_collate_ext_msgs_expired_count", &labels)
                    .increment(expired_msgs_count);
                metrics::gauge!("tycho_collator_ext_msgs_imported_queue_size", &labels)
                    .decrement(expired_msgs_count as f64);

                // skip and remove expired anchor
                assert_eq!(next_idx, 0);
                anchors_cache.remove(next_idx);
                last_anchor_removed = true;
                tracing::debug!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
                    anchor_id,
                    anchor_chain_time = anchor.chain_time,
                    next_chain_time,
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

            // collect messages from anchor
            let mut msgs_collected_from_last_anchor = 0;
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
                        metrics.add_to_message_groups_timer.start();
                        let target_partition =
                            partition_router.get_partition(None, &ext_msg.info.dst);
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
                                .unwrap(); // TODO: msgs-v3: return error instead of panic
                            reader_state_by_partition
                                .buffer
                                .add_message(Box::new(ParsedMessage {
                                    info: MsgInfo::ExtIn(ext_msg.info.clone()),
                                    dst_in_current_shard: true,
                                    cell: ext_msg.cell.clone(),
                                    special_origin: None,
                                    block_seqno: None,
                                    from_same_shard: None,
                                }));
                            metrics
                                .add_to_msgs_groups_ops_count
                                .saturating_add_assign(1);
                        }
                        metrics.add_to_message_groups_timer.stop();

                        metrics.read_ext_msgs_count += 1;

                        total_msgs_collected += 1;
                        msgs_collected_from_last_anchor += 1;

                        tracing::trace!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
                            anchor_id,
                            "collected ext_msg dst: {}", ext_msg.info.dst,
                        );

                        // check if buffer is full
                        // or we can already fill required slots
                        (max_fill_state_by_count, max_fill_state_by_slots) =
                            self.get_max_buffers_fill_state().unwrap(); // TODO: msgs-v3: return error instead of panic
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

            metrics::gauge!("tycho_collator_ext_msgs_imported_queue_size", &labels)
                .decrement(msgs_collected_from_last_anchor as f64);

            tracing::debug!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
                anchor_id,
                msgs_read_offset_in_last_anchor,
                msgs_collected_from_last_anchor,
            );

            // remove fully read anchor
            if anchor.externals.len() == msgs_read_offset_in_last_anchor as usize {
                assert_eq!(next_idx, 0);
                anchors_cache.remove(next_idx);
                last_anchor_removed = true;
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
                reader_state = ?DebugExternalsRangeReaderState(&self.reader_state),
                "externals reader: messages buffers filled up to limits",
            );
        }

        // TODO: msgs-v3: try to merge `has_pending_externals` and a `fully_read` flag

        // check if we still have pending externals
        let has_pending_externals =
            if read_mode == ReadNextExternalsMode::ToPreviuosReadTo && prev_to_reached {
                // when was reading to prev_to and reached it we consider then
                // we do not have pending externals in the range
                false
            } else if has_pending_externals_in_last_read_anchor {
                // when we stopped reading and has pending externals in last anchor
                true
            }
            // TODO: msgs-v3: here we should check not the full anchors cache but
            //      an exact range that we are reading now
            else if last_read_anchor_id_opt.is_none() || last_anchor_removed {
                // when no any anchor was read or last read anchor was removed
                // then we have pending externals if we still have 1+ anchor in cache
                // because every anchor in cache has externals
                anchors_cache.len() > 0
            } else {
                // when last read anchor was not removed
                // and it does not have pending externals
                // it means that we have more externals
                // only if we have more anchors in cache except the last one
                anchors_cache.len() > 1
            };

        tracing::info!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
            total_msgs_collected,
            has_pending_externals_in_last_read_anchor,
            has_pending_externals,
        );

        metrics.read_ext_messages_timer.stop();
        metrics.read_ext_messages_timer.total_elapsed -=
            metrics.add_to_message_groups_timer.total_elapsed;

        ReadExternalsRangeResult {
            has_pending_externals,
            last_read_to_anchor_chain_time,
            max_fill_state_by_slots,
            metrics,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReadNextExternalsMode {
    ToTheEnd,
    ToPreviuosReadTo,
}

#[derive(Default)]
struct ReadExternalsRangeResult {
    /// `true` - when pending externals exist in cache after reading
    has_pending_externals: bool,

    /// The chain time of the last read anchor.
    /// Used to calc externals time diff.
    last_read_to_anchor_chain_time: Option<u64>,

    /// Shows if we can fill all slots in message group from almost one buffer
    max_fill_state_by_slots: BufferFillStateBySlots,

    metrics: MessagesReaderMetrics,
}
