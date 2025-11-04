use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail, ensure};
use tycho_block_util::queue::{QueueKey, QueuePartitionIdx, get_short_addr_string};
use tycho_types::cell::HashBytes;
use tycho_types::models::{BlockIdShort, IntAddr, MsgInfo, ShardIdent, StdAddr};
use tycho_util::FastHashSet;

use crate::collator::error::CollatorError;
use crate::collator::messages_buffer::{
    BufferFillStateByCount, BufferFillStateBySlots, FillMessageGroupResult, IncludeAllMessages,
    MessageGroup, MessagesBuffer, MessagesBufferLimits,
};
use crate::collator::messages_reader::internals_range_reader::{
    CollectMessagesFromRangeReaderResult, InternalsRangeReader, InternalsRangeReaderInfo,
    InternalsRangeReaderKind,
};
use crate::collator::messages_reader::state::ShardReaderState;
use crate::collator::messages_reader::state::internal::{
    DebugInternalsRangeReaderState, InternalsPartitionReaderState, InternalsRangeReaderState,
    InternalsReaderState,
};
use crate::collator::messages_reader::{
    GetNextMessageGroupMode, MessagesReaderMetrics, MessagesReaderStage, internals_range_reader,
};
use crate::collator::types::{
    ConcurrentQueueStatistics, MsgsExecutionParamsExtension, MsgsExecutionParamsStuff,
    ParsedMessage,
};
use crate::internal_queue::iterator::QueueIterator;
use crate::internal_queue::types::diff::DiffZone;
use crate::internal_queue::types::message::InternalMessageValue;
use crate::internal_queue::types::ranges::{Bound, QueueShardBoundedRange};
use crate::internal_queue::types::stats::QueueStatistics;
use crate::queue_adapter::MessageQueueAdapter;
use crate::tracing_targets;
use crate::types::processed_upto::{BlockSeqno, Lt};
use crate::types::{DebugIter, SaturatingAddAssign};

//=========
// INTERNALS READER
//=========

pub(super) struct InternalsPartitionReader<'a, V: InternalMessageValue> {
    pub partition_id: QueuePartitionIdx,
    pub for_shard_id: ShardIdent,
    pub block_seqno: BlockSeqno,
    target_limits: MessagesBufferLimits,
    max_limits: MessagesBufferLimits,
    msgs_exec_params: MsgsExecutionParamsStuff,

    /// mc state gen lt
    mc_state_gen_lt: Lt,
    /// prev shard state gen lt
    prev_state_gen_lt: Lt,
    /// end lt list from top shards of mc block
    mc_top_shards_end_lts: Vec<(ShardIdent, Lt)>,
    mq_adapter: Arc<dyn MessageQueueAdapter<V>>,
    reader_state: &'a mut InternalsPartitionReaderState,
    pub range_readers: BTreeMap<BlockSeqno, InternalsRangeReader<V>>,
    pub all_ranges_fully_read: bool,
    pub remaning_msgs_stats: Option<ConcurrentQueueStatistics>,
}

pub(super) struct InternalsPartitionReaderContext<'a> {
    pub partition_id: QueuePartitionIdx,
    pub for_shard_id: ShardIdent,
    pub block_seqno: BlockSeqno,
    pub target_limits: MessagesBufferLimits,
    pub max_limits: MessagesBufferLimits,
    pub msgs_exec_params: MsgsExecutionParamsStuff,
    pub mc_state_gen_lt: Lt,
    pub prev_state_gen_lt: Lt,
    pub mc_top_shards_end_lts: Vec<(ShardIdent, Lt)>,
    pub reader_state: &'a mut InternalsPartitionReaderState,
    pub remaning_msg_stats: Option<InternalsPartitionReaderRemainingStats>,
}

pub struct InternalsPartitionReaderRemainingStats {
    pub msgs_stats: ConcurrentQueueStatistics,
    pub stats_just_loaded: bool,
}

impl<'a, V: InternalMessageValue> InternalsPartitionReader<'a, V> {
    pub fn new(
        cx: InternalsPartitionReaderContext<'a>,
        mq_adapter: Arc<dyn MessageQueueAdapter<V>>,
    ) -> Result<Self> {
        let (remaning_msgs_stats, remaning_msgs_stats_just_loaded) =
            cx.remaning_msg_stats.map_or((None, false), |stats| {
                (Some(stats.msgs_stats), stats.stats_just_loaded)
            });

        let mut reader = Self {
            partition_id: cx.partition_id,
            for_shard_id: cx.for_shard_id,
            block_seqno: cx.block_seqno,
            target_limits: cx.target_limits,
            max_limits: cx.max_limits,
            msgs_exec_params: cx.msgs_exec_params,
            mc_state_gen_lt: cx.mc_state_gen_lt,
            prev_state_gen_lt: cx.prev_state_gen_lt,
            mc_top_shards_end_lts: cx.mc_top_shards_end_lts,
            mq_adapter,
            reader_state: cx.reader_state,
            range_readers: Default::default(),
            all_ranges_fully_read: false,
            remaning_msgs_stats,
        };

        log_remaining_msgs_stats(
            &reader,
            remaning_msgs_stats_just_loaded,
            "internals partition reader remaning_msgs_stats on create",
        );

        reader.create_existing_range_readers(remaning_msgs_stats_just_loaded)?;

        if remaning_msgs_stats_just_loaded {
            log_remaining_msgs_stats(
                &reader,
                remaning_msgs_stats_just_loaded,
                "reduced internals partition reader remaning_msgs_stats on create",
            );
        }

        Ok(reader)
    }

    pub fn state(&self) -> &InternalsPartitionReaderState {
        &self.reader_state
    }

    pub(super) fn reset_read_state(&mut self) {
        self.all_ranges_fully_read = false;
    }

    pub(super) fn drop_next_range_reader(&mut self) {
        self.range_readers
            .retain(|_, r| r.kind != InternalsRangeReaderKind::Next);
    }

    pub fn set_buffer_limits_by_partition(
        &mut self,
        target_limits: MessagesBufferLimits,
        max_limits: MessagesBufferLimits,
    ) {
        self.target_limits = target_limits;
        self.max_limits = max_limits;
    }

    pub fn target_limits(&self) -> MessagesBufferLimits {
        self.target_limits
    }

    pub fn max_limits(&self) -> MessagesBufferLimits {
        self.max_limits
    }

    pub fn finalize(&mut self, current_next_lt: u64) -> Result<()> {
        // update new messages "to" boundary on current block next lt
        self.update_new_messages_reader_to_boundary(current_next_lt)?;

        if let Some(remaning_msgs_stats) = &self.remaning_msgs_stats {
            tracing::trace!(target: tracing_targets::COLLATOR,
                partition_id = %self.partition_id,
                remaning_msgs_stats = ?DebugIter(remaning_msgs_stats.statistics().iter().map(|item| {
                    let (addr, count) = item.pair();
                    (get_short_addr_string(addr), *count)
                })),
                "internals partition reader remaning_msgs_stats on finalize",
            );
        }

        self.cleanup_redundant_range_readers();

        let mut max_processed_offset = 0;
        let seqnos: Vec<_> = self.reader_state.ranges.keys().copied().collect();
        let mut seqnos_iter = seqnos.into_iter().peekable();

        while let Some(seqno) = seqnos_iter.next() {
            let is_last = seqnos_iter.peek().is_none();

            let reader_state = self.reader_state.ranges.get_mut(&seqno).unwrap();

            let kind = self
                .range_readers
                .get(&seqno)
                .map(|r| r.kind)
                .context("internals range reader should exist for finalize")?;

            // otherwise update offset in the last range reader state
            // if current offset is greater than the maximum stored one among all ranges
            max_processed_offset = max_processed_offset.max(reader_state.processed_offset);

            if self.reader_state.curr_processed_offset > max_processed_offset && is_last {
                reader_state.processed_offset = self.reader_state.curr_processed_offset;
            }

            // drop buffer, read stats and current position in new messages range reader
            // they will be read again in the next block
            // and this will guarantee an equal order on continue and after refill
            if kind == InternalsRangeReaderKind::NewMessages {
                reader_state.buffer = Default::default();
                reader_state.read_stats = Default::default();
                let shard_reader_state = reader_state.shards.get_mut(&self.for_shard_id).unwrap();
                shard_reader_state.current_position = QueueKey::max_for_lt(shard_reader_state.from);
            }
        }

        Ok(())
    }

    pub fn open_ranges_limit_reached(&self) -> bool {
        self.range_readers.len() >= self.msgs_exec_params.current().open_ranges_limit()
    }

    pub fn has_non_zero_processed_offset(&self) -> bool {
        self.reader_state
            .ranges
            .values()
            .any(|r| r.processed_offset > 0)
    }

    pub fn last_range_offset_reached(&self) -> bool {
        self.get_last_range_state()
            .map(|(_, r)| r.processed_offset <= self.reader_state.curr_processed_offset)
            .unwrap_or(true)
    }

    pub fn count_messages_in_buffers(&self) -> usize {
        self.reader_state
            .ranges
            .values()
            .map(|v| v.buffer.msgs_count())
            .sum()
    }

    pub fn has_messages_in_buffers(&self) -> bool {
        self.reader_state
            .ranges
            .values()
            .any(|r| r.buffer.msgs_count() > 0)
    }

    pub fn all_read_existing_messages_collected(&self) -> bool {
        self.all_ranges_fully_read && !self.has_messages_in_buffers()
    }

    pub fn all_new_messages_collected(&self, has_pending_new_messages: bool) -> bool {
        !has_pending_new_messages && !self.has_messages_in_buffers()
    }

    pub fn range_readers(&self) -> &BTreeMap<BlockSeqno, InternalsRangeReader<V>> {
        &self.range_readers
    }

    pub fn retain_only_last_range_reader(&mut self) -> Result<()> {
        let last_seqno = self
            .range_readers
            .last_key_value()
            .map(|(k, _)| *k)
            .context("partition reader should have at least one range reader when retain_only_last_range_reader() called")?;

        self.range_readers.retain(|&seqno, _| seqno == last_seqno);
        self.reader_state
            .ranges
            .retain(|&seqno, _| seqno == last_seqno);
        Ok(())
    }

    pub fn insert_range_reader(
        &mut self,
        seqno: BlockSeqno,
        reader: InternalsRangeReader<V>,
    ) -> Result<(), CollatorError> {
        if self.range_readers.insert(seqno, reader).is_some() {
            return Err(anyhow!(
                "Range reader already exists (shard: {}, seqno: {})",
                self.for_shard_id,
                seqno
            )
            .into());
        }
        Ok(())
    }

    pub fn insert_range_state(
        &mut self,
        seqno: BlockSeqno,
        state: InternalsRangeReaderState,
    ) -> Result<(), CollatorError> {
        if self.reader_state.ranges.insert(seqno, state).is_some() {
            return Err(anyhow!(
                "Range reader state already exists (shard: {}, seqno: {})",
                self.for_shard_id,
                seqno
            )
            .into());
        }
        Ok(())
    }
    pub fn get_last_range_reader(&self) -> Result<&InternalsRangeReader<V>> {
        self.range_readers
            .last_key_value()
            .map(|(_, reader)| reader)
            .context("partition reader should have at least one range reader")
    }

    pub fn get_last_range_reader_mut(&mut self) -> Result<&mut InternalsRangeReader<V>> {
        self.range_readers
            .last_entry()
            .map(|entry| entry.into_mut())
            .context("partition reader should have at least one range reader")
    }

    pub fn get_last_range_state(&self) -> Result<(&BlockSeqno, &InternalsRangeReaderState)> {
        self.reader_state
            .ranges
            .last_key_value()
            .context("partition reader should have at least one range reader")
    }

    pub fn get_last_range_state_mut(&mut self) -> Result<&mut InternalsRangeReaderState> {
        let (&last_seqno, _) = self.get_last_range_state()?;
        Ok(self.reader_state.ranges.get_mut(&last_seqno).unwrap())
    }

    pub fn get_state_by_seqno_mut(
        &mut self,
        seqno: BlockSeqno,
    ) -> Result<&mut InternalsRangeReaderState> {
        self.reader_state
            .ranges
            .get_mut(&seqno)
            .with_context(|| format!("internals range reader state not exists for seqno {seqno}"))
    }

    pub fn get_state_by_seqno(&mut self, seqno: BlockSeqno) -> Result<&InternalsRangeReaderState> {
        self.reader_state
            .ranges
            .get(&seqno)
            .with_context(|| format!("internals range reader state not exists for seqno {seqno}"))
    }

    pub fn increment_curr_processed_offset(&mut self) {
        self.reader_state.curr_processed_offset += 1;
    }

    /// Drop current offset and offset in the last range reader state
    pub fn drop_processing_offset(&mut self, drop_skip_offset: bool) -> Result<()> {
        self.reader_state.curr_processed_offset = 0;
        let last_state = self.get_last_range_state_mut()?;
        last_state.processed_offset = 0;

        if drop_skip_offset {
            last_state.skip_offset = 0;
        }

        Ok(())
    }

    pub fn set_skip_processed_offset_to_current(&mut self) -> Result<()> {
        let curr_processed_offset = self.reader_state.curr_processed_offset;

        let last_range_reader = self.get_last_range_state_mut()?;
        last_range_reader.processed_offset = curr_processed_offset;
        last_range_reader.skip_offset = curr_processed_offset;

        Ok(())
    }

    pub fn set_processed_to_current_position(&mut self) -> Result<()> {
        let (_, last_range_reader) = self.get_last_range_state()?;
        self.reader_state.processed_to = last_range_reader
            .shards
            .iter()
            .map(|(k, v)| (*k, v.current_position))
            .collect();
        Ok(())
    }

    fn create_existing_range_readers(
        &mut self,
        reduce_cumulative_remaning_stats: bool,
    ) -> Result<()> {
        let &mut InternalsPartitionReaderState {
            ranges,
            processed_to,
            ..
        } = &mut self.reader_state;

        for (seqno, mut range_reader_state) in ranges {
            let reader = create_existing_range_reader(
                self.for_shard_id,
                &self.partition_id,
                self.mq_adapter.clone(),
                &mut range_reader_state,
                processed_to,
                *seqno,
                reduce_cumulative_remaning_stats,
                &mut self.remaning_msgs_stats,
                &self.target_limits,
            )?;
            self.range_readers.insert(*seqno, reader);
        }
        Ok(())
    }

    fn create_append_next_range_reader(&mut self) -> Result<BlockSeqno, CollatorError> {
        let range_max_messages = if self.msgs_exec_params.current().range_messages_limit == 0 {
            10_000
        } else {
            self.msgs_exec_params.current().range_messages_limit
        };

        let (reader, state) = self.create_next_range(Some(range_max_messages))?;
        let seqno = reader.seqno;
        // we should add created range reader using calculated reader seqno instead of current block seqno
        // otherwise the next range will exeed the max blocks limit
        if self.range_readers.insert(seqno, reader).is_some() {
            panic!(
                "internals range reader should not already exist (for_shard_id: {}, seqno: {})",
                self.for_shard_id, self.block_seqno,
            )
        };

        if self.reader_state.ranges.insert(seqno, state).is_some() {
            panic!(
                "internals range reader state should not already exist (for_shard_id: {}, seqno: {})",
                self.for_shard_id, self.block_seqno,
            )
        };

        self.all_ranges_fully_read = false;
        Ok(seqno)
    }

    #[tracing::instrument(skip_all)]
    fn create_next_range(
        &self,
        range_max_messages: Option<u32>,
    ) -> Result<(InternalsRangeReader<V>, InternalsRangeReaderState), CollatorError> {
        let last_range_reader_info_opt = self
            .get_last_range_state()
            .map(|(seqno, state)| {
                Some(InternalsRangeReaderInfo {
                    last_to_lts: state.shards.clone(),
                    last_range_block_seqno: *seqno,
                })
            })
            .unwrap_or_default();

        let mut shard_reader_states = BTreeMap::new();

        let all_end_lts = [(ShardIdent::MASTERCHAIN, self.mc_state_gen_lt)]
            .into_iter()
            .chain(self.mc_top_shards_end_lts.iter().cloned());

        let mut ranges = Vec::with_capacity(1 + self.mc_top_shards_end_lts.len());

        let InternalsRangeReaderInfo {
            last_to_lts,
            last_range_block_seqno,
        } = last_range_reader_info_opt.unwrap_or_default();

        let (range_seqno, current_shard_range_to) = match range_max_messages {
            None => (
                self.block_seqno,
                QueueKey::max_for_lt(self.prev_state_gen_lt),
            ),
            Some(max_messages) => {
                let mut next_seqno = last_range_block_seqno + 1;
                let mut range_to = QueueKey::max_for_lt(self.prev_state_gen_lt);

                let mut messages_count = 0;

                while next_seqno < self.block_seqno {
                    let Some(diff) = self.mq_adapter.get_diff_info(
                        &self.for_shard_id,
                        next_seqno,
                        DiffZone::Both,
                    )?
                    else {
                        let diff_block_id = BlockIdShort {
                            shard: self.for_shard_id,
                            seqno: next_seqno,
                        };
                        tracing::warn!(target: tracing_targets::COLLATOR,
                            "check range limit: cannot get diff with stats from queue for block {}",
                            diff_block_id,
                        );

                        next_seqno += 1;
                        continue;
                    };

                    range_to = diff.max_message;

                    messages_count += diff.get_messages_count_by_shard(&self.for_shard_id);

                    if messages_count > max_messages as u64 {
                        break;
                    }

                    next_seqno += 1;
                }

                (next_seqno, range_to)
            }
        };

        for (shard_id, end_lt) in all_end_lts {
            let last_to_lt_opt = last_to_lts.get(&shard_id).map(|s| s.to);
            let shard_range_from =
                last_to_lt_opt.map_or_else(|| QueueKey::min_for_lt(0), QueueKey::max_for_lt);

            let shard_range_to = if shard_id == self.for_shard_id {
                current_shard_range_to
            } else {
                QueueKey::max_for_lt(end_lt)
            };

            shard_reader_states.insert(shard_id, ShardReaderState {
                from: shard_range_from.lt,
                to: shard_range_to.lt,
                current_position: shard_range_from,
            });

            ranges.push(QueueShardBoundedRange {
                shard_ident: shard_id,
                from: Bound::Excluded(shard_range_from),
                to: Bound::Included(shard_range_to),
            });
        }

        let mut state = InternalsRangeReaderState {
            buffer: Default::default(),

            msgs_stats: None,
            remaning_msgs_stats: None,
            read_stats: Default::default(),

            shards: shard_reader_states,
            skip_offset: self.reader_state.curr_processed_offset,
            processed_offset: self.reader_state.curr_processed_offset,
        };

        // get statistics for the range
        load_msg_stats(
            self.partition_id,
            self.mq_adapter.clone(),
            &mut state,
            &ranges,
        )?;

        let reader = InternalsRangeReader {
            partition_id: self.partition_id,
            for_shard_id: self.for_shard_id,
            seqno: range_seqno,
            kind: InternalsRangeReaderKind::Next,
            buffer_limits: self.target_limits,
            iterator_opt: None,
            initialized: false,
        };

        tracing::debug!(target: tracing_targets::COLLATOR,
            partition_id = %reader.partition_id,
            seqno = reader.seqno,
            fully_read = state.is_fully_read(),
            reader_state = ?DebugInternalsRangeReaderState(&state),
            "internals reader: created next range reader state",
        );

        Ok((reader, state))
    }

    pub fn read_existing_messages_into_buffers(
        &mut self,
        read_mode: GetNextMessageGroupMode,
        other_par_readers: &BTreeMap<QueuePartitionIdx, InternalsPartitionReader<V>>,
    ) -> Result<MessagesReaderMetrics, CollatorError> {
        let mut metrics = MessagesReaderMetrics::default();

        metrics.read_existing_messages_timer.start();

        let mut ranges_seqno: VecDeque<_> = self.range_readers.keys().copied().collect();
        let mut last_seqno = 0;

        'main_loop: loop {
            // take next not fully read range and continue reading
            let mut all_ranges_fully_read = true;
            while let Some(seqno) = ranges_seqno.pop_front() {
                let range_reader = self.range_readers.get_mut(&seqno).unwrap_or_else(||
                    panic!(
                        "internals range reader should exist (for_shard_id: {}, seqno: {}, block_seqno: {})",
                        self.for_shard_id, seqno, self.block_seqno,
                    )
                );

                let reader_state = self.reader_state.ranges.get_mut(&seqno).unwrap_or_else(||
                    panic!(
                        "internals range reader state should exist (for_shard_id: {}, seqno: {}, block_seqno: {})",
                        self.for_shard_id, seqno, self.block_seqno,
                    )
                );

                // remember last existing range
                last_seqno = seqno;

                // skip fully read ranges
                if reader_state.is_fully_read() {
                    continue;
                }

                // on refill skip last range reader created in this block
                if read_mode == GetNextMessageGroupMode::Refill && seqno == self.block_seqno {
                    all_ranges_fully_read = false;
                    continue;
                }

                // Do not read range until skip offset reached.
                // Current offset is updated after reading and before collecting,
                // so if current offset == skip offset here, it will be greater on collecting,
                // and in this case we need to read messages
                if self.reader_state.curr_processed_offset < reader_state.skip_offset {
                    all_ranges_fully_read = false;
                    continue;
                }

                // init reader if not initialized
                if !range_reader.initialized {
                    metrics.init_iterator_timer.start();
                    range_reader.init(reader_state, &self.mq_adapter)?;
                    metrics.init_iterator_timer.stop();
                }

                // read into buffer from the range
                let Some(iterator) = range_reader.iterator_opt.as_mut() else {
                    return Err(CollatorError::Anyhow(anyhow!(
                        "not fully read range should have iterator"
                    )));
                };

                let reader_state = self.reader_state.ranges.get_mut(&seqno).unwrap();

                'read_range: loop {
                    // stop reading if buffer is full
                    // or we can already fill required slots

                    let (fill_state_by_count, fill_state_by_slots) =
                        reader_state.buffer.check_is_filled(&self.max_limits);
                    if matches!(
                        (&fill_state_by_count, &fill_state_by_slots),
                        (&BufferFillStateByCount::IsFull, _)
                            | (_, &BufferFillStateBySlots::CanFill)
                    ) {
                        // update current position from iterator
                        let iterator_current_positions = iterator.current_position();
                        for (shard_id, curr_pos) in iterator_current_positions {
                            let Some(shard_reader_state) = reader_state.shards.get_mut(&shard_id)
                            else {
                                return Err(CollatorError::Anyhow(anyhow!(
                                    "shard reader state for existing iterator should exist"
                                )));
                            };
                            shard_reader_state.current_position = curr_pos;
                        }

                        if matches!(fill_state_by_slots, BufferFillStateBySlots::CanFill) {
                            tracing::debug!(target: tracing_targets::COLLATOR,
                                partition_id = %self.partition_id,
                                last_seqno = seqno,
                                reader_state = ?DebugInternalsRangeReaderState(&reader_state),
                                "internals reader: can fill message group on ({}x{})",
                                self.max_limits.slots_count, self.max_limits.slot_vert_size,
                            );
                            // do not need to read other ranges if we can already fill messages group
                            break 'main_loop;
                        } else {
                            tracing::debug!(target: tracing_targets::COLLATOR,
                                partition_id = %self.partition_id,
                                last_seqno = seqno,
                                reader_state = ?DebugInternalsRangeReaderState(&reader_state),
                                "internals reader: message buffer filled on {}/{}",
                                reader_state.buffer.msgs_count(), self.max_limits.max_count,
                            );
                            break 'read_range;
                        }
                    }

                    match iterator.next(false)? {
                        Some(int_msg) => {
                            let msg = ParsedMessage::new(
                                MsgInfo::Int(int_msg.item.message.info().clone()),
                                true,
                                int_msg.item.message.cell().clone(),
                                None,
                                None,
                                Some(int_msg.item.source == self.for_shard_id),
                                None,
                            );

                            metrics.add_to_message_groups_timer.start();
                            reader_state.buffer.add_message(msg);
                            metrics
                                .add_to_msgs_groups_ops_count
                                .saturating_add_assign(1);
                            metrics.add_to_message_groups_timer.stop();

                            metrics.read_existing_msgs_count += 1;

                            // update read messages statistics in range reader
                            reader_state.read_stats.increment_for_account(
                                int_msg.item.message.destination().clone(),
                                1,
                            );

                            // update remaining messages statistics in range reader
                            if let Some(remaning_msgs_stats) =
                                reader_state.remaning_msgs_stats.as_mut()
                            {
                                remaning_msgs_stats.decrement_for_account(
                                    int_msg.item.message.destination().clone(),
                                    1,
                                );

                                // and remaining cumulative stats by partition
                                if let Some(remaning_msgs_stats) = &self.remaning_msgs_stats {
                                    remaning_msgs_stats.decrement_for_account(
                                        int_msg.item.message.destination().clone(),
                                        1,
                                    );
                                }

                                // NOTE: remaining stats will not be reduced on new messages reading
                                //      because read new messages will not be added to queue
                                //      and should not be taken into stats
                            }
                        }
                        None => {
                            // reader_state.fully_read = true;

                            // set current position to the end of the range
                            for (_, shard_reader_state) in reader_state.shards.iter_mut() {
                                shard_reader_state.set_fully_read();
                                // shard_reader_state.current_position =
                                //     QueueKey::max_for_lt(shard_reader_state.to);
                            }

                            break 'read_range;
                        }
                    }
                }

                if !reader_state.is_fully_read() {
                    all_ranges_fully_read = false;
                }
            }

            if !all_ranges_fully_read {
                // exit when we stopped reading and range was not fully read
                break;
            }

            // do not create next range reader if current one is the last already
            if last_seqno >= self.block_seqno {
                self.all_ranges_fully_read = true;
                break;
            }

            // do not create next range reader on refill
            if read_mode == GetNextMessageGroupMode::Refill {
                tracing::debug!(target: tracing_targets::COLLATOR,
                    partition_id = %self.partition_id,
                    "internals reader: do not create next range reader on Refill",
                );
                self.all_ranges_fully_read = true;
                break;
            }

            // should not create next range reader
            // if open ranges limit reached in current partition or others
            let mut should_create_next_range = if self.open_ranges_limit_reached() {
                tracing::debug!(target: tracing_targets::COLLATOR,
                    partition_id = %self.partition_id,
                    open_ranges_limit = self.msgs_exec_params.current().open_ranges_limit,
                    "internals reader: open ranges limit reached in current partition",
                );
                false
            } else {
                let limit_reached_in_other_parts = other_par_readers
                    .values()
                    .any(|par| par.open_ranges_limit_reached());

                if limit_reached_in_other_parts {
                    tracing::debug!(target: tracing_targets::COLLATOR,
                        partition_id = %self.partition_id,
                        open_ranges_limit = self.msgs_exec_params.current().open_ranges_limit,
                        "internals reader: open ranges limit reached in other partitions",
                    );
                    false
                } else {
                    true
                }
            };

            // in the normal partition 0 we should create next range reader
            // even if open ranges limit reached
            // when we have intersecting accounts with remaining messages with other partitions
            // otherwise collation may stuck
            if !should_create_next_range && self.partition_id.is_zero() {
                for other in other_par_readers.values() {
                    if let Some(intersected_account) =
                        internals_range_reader::partitions_have_intersecting_accounts(self, other)?
                    {
                        tracing::debug!(target: tracing_targets::COLLATOR,
                            partition_id = %self.partition_id,
                            intersected_account = get_short_addr_string(&intersected_account),
                            "internals reader: account with remaining messages is intersected with other partitions"
                        );
                        should_create_next_range = true;
                        break;
                    }
                }
            }

            tracing::debug!(target: tracing_targets::COLLATOR,
                partition_id = %self.partition_id,
                "internals reader: should create next range reader = {}",
                should_create_next_range
            );

            if should_create_next_range {
                if self.msgs_exec_params.new_is_some() {
                    tracing::debug!(target: tracing_targets::COLLATOR,
                        last_seqno,
                        "internals reader: do not create next range reader on Continue because new message exec params exists",
                    );
                    self.all_ranges_fully_read = true;
                    break;
                }

                let range_seqno = self.create_append_next_range_reader()?;
                ranges_seqno.push_back(range_seqno);
            } else {
                self.all_ranges_fully_read = true;
                break;
            }
        }

        metrics.read_existing_messages_timer.stop();
        metrics.read_existing_messages_timer.total_elapsed -=
            metrics.init_iterator_timer.total_elapsed;
        metrics.read_existing_messages_timer.total_elapsed -=
            metrics.add_to_message_groups_timer.total_elapsed;

        Ok(metrics)
    }

    pub fn check_has_pending_internals_in_iterators(&mut self) -> Result<bool, CollatorError> {
        let mut last_seqno = 0;

        for (seqno, range_reader) in self.range_readers.iter_mut() {
            let mut reader_state = self.reader_state.ranges.get_mut(&seqno).unwrap();

            // remember last existing range
            last_seqno = *seqno;

            // skip new messages range readers
            if range_reader.kind == InternalsRangeReaderKind::NewMessages {
                continue;
            }

            // skip fully read ranges
            if reader_state.is_fully_read() {
                continue;
            }

            // init reader if not initialized
            if !range_reader.initialized {
                range_reader.init(reader_state, &self.mq_adapter)?;
            }

            // check if has pending internals in iterator
            let Some(iterator) = range_reader.iterator_opt.as_mut() else {
                return Err(CollatorError::Anyhow(anyhow!(
                    "not fully read range should have iterator"
                )));
            };

            match iterator.next(false)? {
                Some(_) => {
                    // Re-init iterator to revert current position pointer
                    // to let us read this message again into buffer during collation.
                    // We do not add message to buffer during this check
                    // to avoid any possible inconsistency.
                    range_reader.init(reader_state, &self.mq_adapter)?;

                    return Ok(true);
                }
                None => {
                    // reader_state.fully_read = true;

                    // set current position to the end of the range
                    for (_, shard_reader_state) in reader_state.shards.iter_mut() {
                        shard_reader_state.set_fully_read();
                        // shard_reader_state.current_position =
                        //     QueueKey::max_for_lt(shard_reader_state.to);
                    }
                }
            }
        }

        // if last range is not from current block then create and check next range
        if last_seqno < self.block_seqno {
            // we should look thru the whole range to check for pending messages
            // so we do not pass `range_max_messages` to force use the prev block end lt
            let (mut range_reader, mut state) = self.create_next_range(None)?;

            if !state.is_fully_read() {
                range_reader.init(&mut state, &self.mq_adapter)?;

                // check if has pending internals in iterator
                let Some(iterator) = range_reader.iterator_opt.as_mut() else {
                    return Err(CollatorError::Anyhow(anyhow!(
                        "not fully read range should have iterator"
                    )));
                };

                if iterator.next(false)?.is_some() {
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    pub fn collect_messages(
        &mut self,
        par_reader_stage: &MessagesReaderStage,
        msg_group: &mut MessageGroup,
        prev_par_readers: &BTreeMap<QueuePartitionIdx, InternalsPartitionReader<V>>,
        prev_msg_groups: &BTreeMap<QueuePartitionIdx, MessageGroup>,
        already_skipped_accounts: &mut FastHashSet<HashBytes>,
    ) -> Result<CollectInternalsResult> {
        let mut res = CollectInternalsResult::default();

        // do not collect internals on FinishExternals stage
        if matches!(
            par_reader_stage,
            MessagesReaderStage::FinishPreviousExternals
                | MessagesReaderStage::FinishCurrentExternals
        ) {
            return Ok(res);
        }

        // extract range readers from state to use previous readers buffers and stats
        // to check for account skip on collecting messages from the next
        let mut prev_readers_states_seqnos = vec![];

        let Self {
            range_readers,
            reader_state,
            ..
        } = self;

        for (seqno, range_reader) in range_readers {
            if matches!(
                (par_reader_stage, range_reader.kind),
                // skip new messages reader when reading existing
                (MessagesReaderStage::ExistingAndExternals, InternalsRangeReaderKind::NewMessages)
                // skip existing messages reader when reading new
                | (MessagesReaderStage::ExternalsAndNew, InternalsRangeReaderKind::Existing | InternalsRangeReaderKind::Next)
            ) {
                prev_readers_states_seqnos.push(*seqno);
                continue;
            }

            let curr_processed_offset = reader_state.curr_processed_offset;

            let (prev_readers_states, current_range_state) =
                get_reader_states_refs(reader_state, seqno, prev_readers_states_seqnos.as_slice())?;

            // skip up to skip offset
            if curr_processed_offset > current_range_state.skip_offset {
                res.metrics.add_to_message_groups_timer.start();
                let CollectMessagesFromRangeReaderResult {
                    mut collected_int_msgs,
                    ops_count,
                } = range_reader.collect_messages(
                    msg_group,
                    prev_par_readers,
                    &prev_readers_states,
                    prev_msg_groups,
                    already_skipped_accounts,
                    current_range_state,
                );
                res.metrics
                    .add_to_msgs_groups_ops_count
                    .saturating_add_assign(ops_count);
                res.metrics.add_to_message_groups_timer.stop();
                res.collected_int_msgs.append(&mut collected_int_msgs);
            }

            let range_reader_processed_offset = current_range_state.processed_offset;

            prev_readers_states_seqnos.push(*seqno);
            // prev_readers_states.insert(*seqno, reader_state);
            // range_readers.insert(seqno, range_reader);

            // collect messages from the next range
            // only when current range processed offset is reached
            if reader_state.curr_processed_offset <= range_reader_processed_offset {
                break;
            }
        }
        // self.set_range_readers(range_readers);

        Ok(res)
    }

    fn cleanup_redundant_range_readers(&mut self) {
        // ignore next range reader if it was not fully read
        // and was not initialized (we did not read any messages from it)
        // to reduce stored range reader states in processed upto info
        let unused_next_seqnos: Vec<BlockSeqno> = self
            .range_readers
            .iter()
            .filter_map(|(seqno, r)| match r.kind {
                InternalsRangeReaderKind::Next if !r.initialized => {
                    let is_fully_read = self
                        .reader_state
                        .ranges
                        .get(seqno)
                        .map(|state| state.is_fully_read())
                        .unwrap_or(true);

                    if !is_fully_read { Some(*seqno) } else { None }
                }
                _ => None,
            })
            .collect();

        for seqno in unused_next_seqnos {
            self.reader_state.ranges.remove(&seqno);
        }
    }
}

pub(super) fn log_remaining_msgs_stats<V: InternalMessageValue>(
    par_reader: &InternalsPartitionReader<V>,
    remaning_msgs_stats_just_loaded: bool,
    msg: &str,
) {
    if let Some(remaning_msgs_stats) = &par_reader.remaning_msgs_stats {
        tracing::trace!(target: tracing_targets::COLLATOR,
            partition_id = %par_reader.partition_id,
            remaning_msgs_stats_just_loaded,
            remaning_msgs_stats = ?DebugIter(remaning_msgs_stats.statistics().iter().map(|item| {
                let (addr, count) = item.pair();
                (get_short_addr_string(addr), *count)
            })),
            "{}", msg,
        );
    }
}

#[derive(Default)]
pub(super) struct CollectInternalsResult {
    pub metrics: MessagesReaderMetrics,
    pub collected_int_msgs: Vec<QueueKey>,
}

#[tracing::instrument(skip_all)]
fn create_existing_range_reader<V: InternalMessageValue>(
    for_shard_id: ShardIdent,
    partition_id: &QueuePartitionIdx,
    mq_adapter: Arc<dyn MessageQueueAdapter<V>>,
    range_reader_state: &mut InternalsRangeReaderState,
    reader_state_processed_to: &BTreeMap<ShardIdent, QueueKey>,
    seqno: BlockSeqno,
    reduce_cumulative_remaning_stats: bool,
    remaining_msgs_stats: &mut Option<ConcurrentQueueStatistics>,
    buffer_limits: &MessagesBufferLimits,
) -> Result<InternalsRangeReader<V>> {
    // get statistics for the range if it was not loaded before
    if range_reader_state.msgs_stats.is_none() {
        let mut ranges = Vec::with_capacity(range_reader_state.shards.len());

        for (shard_id, shard_reader_state) in &range_reader_state.shards {
            ranges.push(QueueShardBoundedRange {
                shard_ident: *shard_id,
                from: Bound::Excluded(shard_reader_state.current_position),
                to: Bound::Included(QueueKey::max_for_lt(shard_reader_state.to)),
            });
        }

        load_msg_stats(*partition_id, mq_adapter, range_reader_state, &ranges)?;
    }

    // if remaining cumulative stats just loaded
    // then we need to reduce it by the read state
    // only if current range is above processed_to
    if let Some(remaining_msgs_stats) = remaining_msgs_stats
        && reduce_cumulative_remaning_stats
    {
        let current_shard_processed_to_key = reader_state_processed_to
            .get(&for_shard_id)
            .copied()
            .unwrap_or_default();

        let current_shard_range_to_key = range_reader_state
            .shards
            .get(&for_shard_id)
            .map(|r_s| QueueKey::max_for_lt(r_s.to))
            .unwrap_or_default();

        if current_shard_range_to_key > current_shard_processed_to_key {
            // ensure all other shards range to key is not below processed_to
            for (shard_id, &processed_to_key) in reader_state_processed_to {
                if shard_id == &for_shard_id {
                    continue;
                }

                let shard_range_to_key = range_reader_state
                    .shards
                    .get(shard_id)
                    .map(|r_s| QueueKey::max_for_lt(r_s.to))
                    .unwrap();

                ensure!(shard_range_to_key >= processed_to_key);
            }

            // reduce remaining stats
            tracing::trace!(target: tracing_targets::COLLATOR,
                partition_id = %partition_id,
                seqno,
                read_stats = ?DebugIter(range_reader_state.read_stats.statistics().iter().map(|(addr, count)| (addr.to_string(), *count))),
                "reduce cumulative remaning_msgs_stats by read_stats from range reader",
            );

            for (account_addr, &count) in range_reader_state.read_stats.statistics() {
                remaining_msgs_stats.decrement_for_account(account_addr.clone(), count);
            }
        }
    }

    let reader = InternalsRangeReader {
        partition_id: *partition_id,
        for_shard_id,
        seqno,
        kind: InternalsRangeReaderKind::Existing,
        buffer_limits: *buffer_limits,
        iterator_opt: None,
        initialized: false,
    };

    tracing::debug!(target: tracing_targets::COLLATOR,
        partition_id = %reader.partition_id,
        seqno = reader.seqno,
        fully_read = range_reader_state.is_fully_read(),
        reader_state = ?DebugInternalsRangeReaderState(&range_reader_state),
        "internals reader: created existing range reader",
    );

    Ok(reader)
}

fn load_msg_stats<V: InternalMessageValue>(
    partition_id: QueuePartitionIdx,
    mq_adapter: Arc<dyn MessageQueueAdapter<V>>,
    range_reader_state: &mut InternalsRangeReaderState,
    ranges: &[QueueShardBoundedRange],
) -> Result<()> {
    let msgs_stats =
        mq_adapter.get_statistics(&vec![partition_id].into_iter().collect(), ranges)?;

    if range_reader_state.is_fully_read() {
        range_reader_state.remaning_msgs_stats = Some(Default::default());
        range_reader_state.read_stats = msgs_stats.clone();
    } else {
        ensure!(range_reader_state.buffer.msgs_count() == 0);

        range_reader_state.remaning_msgs_stats = Some(msgs_stats.clone());
        range_reader_state.read_stats = Default::default();
    }

    range_reader_state.msgs_stats = Some(Arc::new(msgs_stats));

    Ok(())
}

fn get_reader_states_refs<'a>(
    reader_state: &'a mut InternalsPartitionReaderState,
    current_seqno: &'a BlockSeqno,
    prev_seqnos: &'a [BlockSeqno],
) -> Result<(
    Vec<&'a InternalsRangeReaderState>,
    &'a mut InternalsRangeReaderState,
)> {
    assert!(
        !prev_seqnos.contains(current_seqno),
        "current_seqno should not be in prev_seqnos"
    );

    // collect immutable references to previous states
    let mut prev_states = Vec::with_capacity(prev_seqnos.len());
    for seqno in prev_seqnos {
        let state_ptr = reader_state
            .ranges
            .get(seqno)
            .ok_or_else(|| anyhow::anyhow!("State not found: {:?}", seqno))?
            as *const InternalsRangeReaderState;

        // SAFETY: we ensure that mutable reference to current_state is not overlapping
        prev_states.push(unsafe { &*state_ptr });
    }

    // collect mutable reference to current state
    let current_state = reader_state
        .ranges
        .get_mut(current_seqno)
        .ok_or_else(|| anyhow::anyhow!("Current state not found: {:?}", current_seqno))?;

    Ok((prev_states, current_state))
}
