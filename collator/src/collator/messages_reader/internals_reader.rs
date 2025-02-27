use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use everscale_types::models::{BlockIdShort, IntAddr, MsgInfo, MsgsExecutionParams, ShardIdent};
use tycho_block_util::queue::{QueueKey, QueuePartitionIdx};

use super::{
    DebugInternalsRangeReaderState, GetNextMessageGroupMode, InternalsPartitionReaderState,
    InternalsRangeReaderState, MessagesReaderMetrics, MessagesReaderStage, ShardReaderState,
};
use crate::collator::error::{CollationCancelReason, CollatorError};
use crate::collator::messages_buffer::{
    BufferFillStateByCount, BufferFillStateBySlots, FillMessageGroupResult, MessageGroup,
    MessagesBuffer, MessagesBufferLimits, SaturatingAddAssign,
};
use crate::collator::types::{MsgsExecutionParamsExtension, ParsedMessage};
use crate::internal_queue::iterator::QueueIterator;
use crate::internal_queue::types::{InternalMessageValue, QueueShardRange, QueueStatistics};
use crate::queue_adapter::MessageQueueAdapter;
use crate::tracing_targets;
use crate::types::processed_upto::{BlockSeqno, Lt};

//=========
// INTERNALS READER
//=========

pub(super) struct InternalsPartitionReader<V: InternalMessageValue> {
    pub(super) partition_id: QueuePartitionIdx,
    pub(super) for_shard_id: ShardIdent,
    pub(super) block_seqno: BlockSeqno,

    pub(super) target_limits: MessagesBufferLimits,
    pub(super) max_limits: MessagesBufferLimits,

    msgs_exec_params: Arc<MsgsExecutionParams>,

    /// mc state gen lt
    mc_state_gen_lt: Lt,
    /// prev shard state gen lt
    prev_state_gen_lt: Lt,
    /// end lt list from top shards of mc block
    mc_top_shards_end_lts: Vec<(ShardIdent, Lt)>,

    mq_adapter: Arc<dyn MessageQueueAdapter<V>>,

    reader_state: InternalsPartitionReaderState,
    range_readers: BTreeMap<BlockSeqno, InternalsRangeReader<V>>,

    pub(super) all_ranges_fully_read: bool,
}

pub(super) struct InternalsPartitionReaderContext {
    pub partition_id: QueuePartitionIdx,
    pub for_shard_id: ShardIdent,
    pub block_seqno: BlockSeqno,
    pub target_limits: MessagesBufferLimits,
    pub max_limits: MessagesBufferLimits,
    pub msgs_exec_params: Arc<MsgsExecutionParams>,
    pub mc_state_gen_lt: Lt,
    pub prev_state_gen_lt: Lt,
    pub mc_top_shards_end_lts: Vec<(ShardIdent, Lt)>,
    pub reader_state: InternalsPartitionReaderState,
}

impl<V: InternalMessageValue> InternalsPartitionReader<V> {
    pub fn new(
        cx: InternalsPartitionReaderContext,
        mq_adapter: Arc<dyn MessageQueueAdapter<V>>,
    ) -> Result<Self> {
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
        };

        reader.create_existing_range_readers()?;

        Ok(reader)
    }

    pub(super) fn reset_read_state(&mut self) {
        self.all_ranges_fully_read = false;
    }

    pub(super) fn drop_next_range_reader(&mut self) {
        self.range_readers
            .retain(|_, r| r.kind != InternalsRangeReaderKind::Next);
    }

    pub fn finalize(mut self, current_next_lt: u64) -> Result<InternalsPartitionReaderState> {
        // update new messages "to" boundary on current block next lt
        self.update_new_messages_reader_to_boundary(current_next_lt)?;

        // collect range reader states
        // ignore next range reader if it was not fully read
        // and was not initialized (we did not read any messages from it)
        // to reduce stored range reader states in processed upto info
        let mut range_readers = self
            .range_readers
            .into_iter()
            .filter_map(|(seqno, r)| match r.kind {
                InternalsRangeReaderKind::Next if !r.fully_read && !r.initialized => None,
                _ => Some((seqno, r)),
            })
            .peekable();
        let mut max_processed_offset = 0;
        while let Some((seqno, mut range_reader)) = range_readers.next() {
            // TODO: msgs-v3: update offset in the last range reader on the go?

            // otherwise update offset in the last range reader state
            // if current offset is greater than the maximum stored one among all ranges
            max_processed_offset =
                max_processed_offset.max(range_reader.reader_state.processed_offset);
            if self.reader_state.curr_processed_offset > max_processed_offset
                && range_readers.peek().is_none()
            {
                range_reader.reader_state.processed_offset =
                    self.reader_state.curr_processed_offset;
            }

            // drop buffer and current position in new messages range reader
            // they will be read again in the next block
            // and this will guarantee an equal order on continue and after refill
            if range_reader.kind == InternalsRangeReaderKind::NewMessages {
                range_reader.reader_state.buffer = MessagesBuffer::default();
                let shard_reader_state = range_reader
                    .reader_state
                    .shards
                    .get_mut(&self.for_shard_id)
                    .unwrap();
                shard_reader_state.current_position = QueueKey::max_for_lt(shard_reader_state.from);
            }

            self.reader_state
                .ranges
                .insert(seqno, range_reader.reader_state);
        }

        // return updated partition reader state
        Ok(self.reader_state)
    }

    pub fn open_ranges_limit_reached(&self) -> bool {
        self.range_readers.len() >= self.msgs_exec_params.open_ranges_limit()
    }

    pub fn has_non_zero_processed_offset(&self) -> bool {
        self.range_readers
            .values()
            .any(|r| r.reader_state.processed_offset > 0)
    }

    pub fn last_range_offset_reached(&self) -> bool {
        self.get_last_range_reader()
            .map(|(_, r)| {
                r.reader_state.processed_offset <= self.reader_state.curr_processed_offset
            })
            .unwrap_or(true)
    }

    pub fn count_messages_in_buffers(&self) -> usize {
        self.range_readers
            .values()
            .map(|v| v.reader_state.buffer.msgs_count())
            .sum()
    }

    pub fn has_messages_in_buffers(&self) -> bool {
        self.range_readers
            .values()
            .any(|r| r.reader_state.buffer.msgs_count() > 0)
    }

    pub fn all_read_existing_messages_collected(&self) -> bool {
        self.all_ranges_fully_read && !self.has_messages_in_buffers()
    }

    pub fn all_new_messages_collected(&self, has_pending_new_messages: bool) -> bool {
        !has_pending_new_messages && !self.has_messages_in_buffers()
    }

    pub fn reader_state(&self) -> &InternalsPartitionReaderState {
        &self.reader_state
    }

    pub fn range_readers(&self) -> &BTreeMap<BlockSeqno, InternalsRangeReader<V>> {
        &self.range_readers
    }

    pub fn retain_only_last_range_reader(&mut self) -> Result<()> {
        let (last_seqno, last_range_reader) = self
            .range_readers
            .pop_last()
            .context("partition reader should have at least one range reader when retain_only_last_range_reader() called")?;

        if last_seqno < self.block_seqno {
            // set that not all ranges fully read
            // to force create and read next range reader for block
            self.all_ranges_fully_read = false;
        }

        self.range_readers.clear();
        self.range_readers.insert(last_seqno, last_range_reader);
        Ok(())
    }

    pub fn pop_first_range_reader(&mut self) -> Option<(BlockSeqno, InternalsRangeReader<V>)> {
        self.range_readers.pop_first()
    }

    pub fn set_range_readers(
        &mut self,
        mut range_readers: BTreeMap<BlockSeqno, InternalsRangeReader<V>>,
    ) {
        self.range_readers.append(&mut range_readers);
    }

    pub(super) fn insert_range_reader(
        &mut self,
        seqno: BlockSeqno,
        reader: InternalsRangeReader<V>,
    ) -> &mut InternalsRangeReader<V> {
        self.range_readers.insert(seqno, reader);
        self.range_readers
            .get_mut(&seqno)
            .expect("just inserted range reader should exist")
    }

    pub fn get_last_range_reader(&self) -> Result<(&BlockSeqno, &InternalsRangeReader<V>)> {
        self.range_readers
            .last_key_value()
            .context("partition reader should have at least one range reader")
    }

    pub fn get_last_range_reader_mut(&mut self) -> Result<&mut InternalsRangeReader<V>> {
        let (&last_seqno, _) = self.get_last_range_reader()?;
        Ok(self.range_readers.get_mut(&last_seqno).unwrap())
    }

    pub fn increment_curr_processed_offset(&mut self) {
        self.reader_state.curr_processed_offset += 1;
    }

    /// Drop current offset and offset in the last range reader state
    pub fn drop_processing_offset(&mut self, drop_skip_offset: bool) -> Result<()> {
        self.reader_state.curr_processed_offset = 0;
        let last_range_reader = self.get_last_range_reader_mut()?;
        last_range_reader.reader_state.processed_offset = 0;

        if drop_skip_offset {
            last_range_reader.reader_state.skip_offset = 0;
        }

        Ok(())
    }

    pub fn set_skip_processed_offset_to_current(&mut self) -> Result<()> {
        let curr_processed_offset = self.reader_state.curr_processed_offset;

        let last_range_reader = self.get_last_range_reader_mut()?;
        last_range_reader.reader_state.processed_offset = curr_processed_offset;
        last_range_reader.reader_state.skip_offset = curr_processed_offset;

        Ok(())
    }

    pub fn set_processed_to_current_position(&mut self) -> Result<()> {
        let (_, last_range_reader) = self.get_last_range_reader()?;
        self.reader_state.processed_to = last_range_reader
            .reader_state
            .shards
            .iter()
            .map(|(k, v)| (*k, v.current_position))
            .collect();
        Ok(())
    }

    fn create_existing_range_readers(&mut self) -> Result<()> {
        // create existing range readers
        while let Some((seqno, range_reader_state)) = self.reader_state.ranges.pop_first() {
            let reader = self.create_existing_internals_range_reader(range_reader_state, seqno)?;
            self.range_readers.insert(seqno, reader);
        }
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    fn create_existing_internals_range_reader(
        &self,
        mut range_reader_state: InternalsRangeReaderState,
        seqno: BlockSeqno,
    ) -> Result<InternalsRangeReader<V>> {
        let mut ranges = Vec::with_capacity(range_reader_state.shards.len());

        let mut fully_read = true;
        for (shard_id, shard_reader_state) in &range_reader_state.shards {
            let shard_range_to = QueueKey::max_for_lt(shard_reader_state.to);
            if shard_reader_state.current_position != shard_range_to {
                fully_read = false;
            }

            ranges.push(QueueShardRange {
                shard_ident: *shard_id,
                from: shard_reader_state.current_position,
                to: shard_range_to,
            });
        }

        // get statistics for the range if it was not loaded before
        if range_reader_state.msgs_stats.is_none() {
            self.load_msg_stats(&mut range_reader_state, fully_read, &ranges)?;
        }

        let reader = InternalsRangeReader {
            partition_id: self.partition_id,
            for_shard_id: self.for_shard_id,
            seqno,
            kind: InternalsRangeReaderKind::Existing,
            buffer_limits: self.target_limits,
            reader_state: range_reader_state,
            fully_read,
            mq_adapter: self.mq_adapter.clone(),
            iterator_opt: None,
            initialized: false,
        };

        tracing::debug!(target: tracing_targets::COLLATOR,
            partition_id = reader.partition_id,
            for_shard_id = %reader.for_shard_id,
            seqno = reader.seqno,
            fully_read = reader.fully_read,
            reader_state = ?DebugInternalsRangeReaderState(&reader.reader_state),
            "internals reader: created existing range reader",
        );

        Ok(reader)
    }

    fn load_msg_stats(
        &self,
        range_reader_state: &mut InternalsRangeReaderState,
        fully_read: bool,
        ranges: &[QueueShardRange],
    ) -> Result<()> {
        let msgs_stats = self.mq_adapter.get_statistics(self.partition_id, ranges)?;
        let remaning_msgs_stats = match fully_read {
            true => QueueStatistics::default(),
            false => {
                let mut remaning_msgs_stats = msgs_stats.clone();
                // should reduce remaning stats if buffer is not empty
                if range_reader_state.buffer.msgs_count() > 0 {
                    for (account_id, msgs) in range_reader_state.buffer.iter() {
                        let int_addr =
                            IntAddr::from((self.for_shard_id.workchain() as i8, *account_id));
                        remaning_msgs_stats.decrement_for_account(int_addr, msgs.len() as u64);
                    }
                }
                remaning_msgs_stats
            }
        };

        range_reader_state.msgs_stats = Some(msgs_stats);
        range_reader_state.remaning_msgs_stats = Some(remaning_msgs_stats);

        Ok(())
    }

    fn create_append_next_range_reader(&mut self) -> Result<BlockSeqno, CollatorError> {
        let range_max_messages = if self.msgs_exec_params.range_messages_limit == 0 {
            10_000
        } else {
            self.msgs_exec_params.range_messages_limit
        };

        let reader = self.create_next_internals_range_reader(Some(range_max_messages))?;
        let reader_seqno = reader.seqno;
        // we should add created range reader using calculated reader seqno instead of current block seqno
        // otherwise the next range will exeed the max blocks limit
        if self.range_readers.insert(reader_seqno, reader).is_some() {
            panic!(
                "internals range reader should not already exist (for_shard_id: {}, seqno: {})",
                self.for_shard_id, self.block_seqno,
            )
        };
        self.all_ranges_fully_read = false;
        Ok(reader_seqno)
    }

    #[tracing::instrument(skip_all)]
    fn create_next_internals_range_reader(
        &self,
        range_max_messages: Option<u32>,
    ) -> Result<InternalsRangeReader<V>, CollatorError> {
        let last_range_reader_info_opt = self
            .get_last_range_reader()
            .map(|(_, reader)| {
                Some(InternalsRangeReaderInfo {
                    last_to_lts: reader.reader_state.shards.clone(),
                    last_range_block_seqno: reader.seqno,
                })
            })
            .unwrap_or_default();

        let mut shard_reader_states = BTreeMap::new();

        let all_end_lts = [(ShardIdent::MASTERCHAIN, self.mc_state_gen_lt)]
            .into_iter()
            .chain(self.mc_top_shards_end_lts.iter().cloned());

        let mut ranges = Vec::with_capacity(1 + self.mc_top_shards_end_lts.len());

        let mut fully_read = true;

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
                    let diff = self
                        .mq_adapter
                        .get_diff(&self.for_shard_id, next_seqno)
                        .ok_or_else(|| {
                            let diff_block_id = BlockIdShort {
                                shard: self.for_shard_id,
                                seqno: next_seqno,
                            };
                            tracing::warn!(target: tracing_targets::COLLATOR,
                                "check range limit: cannot get diff with stats from queue for block {}",
                                diff_block_id,
                            );
                            CollatorError::Cancelled(CollationCancelReason::DiffNotFoundInQueue(
                                diff_block_id,
                            ))
                        })?;

                    range_to = *diff.max_message();

                    messages_count += diff
                        .statistics()
                        .get_messages_count_by_shard(&self.for_shard_id);

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

            if shard_range_from != shard_range_to {
                fully_read = false;
            }

            shard_reader_states.insert(shard_id, ShardReaderState {
                from: shard_range_from.lt,
                to: shard_range_to.lt,
                current_position: shard_range_from,
            });

            ranges.push(QueueShardRange {
                shard_ident: shard_id,
                from: shard_range_from,
                to: shard_range_to,
            });
        }

        let mut range_reader_state = InternalsRangeReaderState {
            buffer: Default::default(),

            msgs_stats: None,
            remaning_msgs_stats: None,

            shards: shard_reader_states,
            skip_offset: self.reader_state.curr_processed_offset,
            processed_offset: self.reader_state.curr_processed_offset,
        };

        // get statistics for the range
        self.load_msg_stats(&mut range_reader_state, fully_read, &ranges)?;

        let reader = InternalsRangeReader {
            partition_id: self.partition_id,
            for_shard_id: self.for_shard_id,
            seqno: range_seqno,
            kind: InternalsRangeReaderKind::Next,
            buffer_limits: self.target_limits,
            reader_state: range_reader_state,
            fully_read,
            mq_adapter: self.mq_adapter.clone(),
            iterator_opt: None,
            initialized: false,
        };

        tracing::debug!(target: tracing_targets::COLLATOR,
            partition_id = reader.partition_id,
            for_shard_id = %reader.for_shard_id,
            seqno = reader.seqno,
            fully_read = reader.fully_read,
            reader_state = ?DebugInternalsRangeReaderState(&reader.reader_state),
            "internals reader: created next range reader",
        );

        Ok(reader)
    }

    pub fn read_existing_messages_into_buffers(
        &mut self,
        read_mode: GetNextMessageGroupMode,
    ) -> Result<MessagesReaderMetrics, CollatorError> {
        let mut metrics = MessagesReaderMetrics::default();

        // skip if all open ranges fully read
        if self.all_ranges_fully_read {
            return Ok(metrics);
        }

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

                // init reader if not initialized
                if !range_reader.initialized {
                    metrics.init_iterator_timer.start();
                    range_reader.init()?;
                    metrics.init_iterator_timer.stop();
                }

                // read into buffer from the range
                let Some(iterator) = range_reader.iterator_opt.as_mut() else {
                    return Err(CollatorError::Anyhow(anyhow!(
                        "not fully read range should have iterator"
                    )));
                };

                'read_range: loop {
                    // stop reading if buffer is full
                    // or we can already fill required slots
                    let (fill_state_by_count, fill_state_by_slots) = range_reader
                        .reader_state
                        .buffer
                        .check_is_filled(&self.max_limits);
                    if matches!(
                        (&fill_state_by_count, &fill_state_by_slots),
                        (&BufferFillStateByCount::IsFull, _)
                            | (_, &BufferFillStateBySlots::CanFill)
                    ) {
                        // update current position from iterator
                        let iterator_current_positions = iterator.current_position();
                        for (shard_id, curr_pos) in iterator_current_positions {
                            let Some(shard_reader_state) =
                                range_reader.reader_state.shards.get_mut(&shard_id)
                            else {
                                return Err(CollatorError::Anyhow(anyhow!(
                                    "shard reader state for existing iterator should exist"
                                )));
                            };
                            shard_reader_state.current_position = curr_pos;
                        }

                        if matches!(fill_state_by_slots, BufferFillStateBySlots::CanFill) {
                            tracing::debug!(target: tracing_targets::COLLATOR,
                                partition_id = self.partition_id,
                                seqno,
                                reader_state = ?DebugInternalsRangeReaderState(&range_reader.reader_state),
                                "internals reader: can fill message group on ({}x{})",
                                self.max_limits.slots_count, self.max_limits.slot_vert_size,
                            );
                            // do not need to read other ranges if we can already fill messages group
                            break 'main_loop;
                        } else {
                            tracing::debug!(target: tracing_targets::COLLATOR,
                                partition_id = self.partition_id,
                                seqno,
                                reader_state = ?DebugInternalsRangeReaderState(&range_reader.reader_state),
                                "internals reader: message buffer filled on {}/{}",
                                range_reader.reader_state.buffer.msgs_count(), self.max_limits.max_count,
                            );
                            break 'read_range;
                        }
                    }

                    match iterator.next(false)? {
                        Some(int_msg) => {
                            let msg = Box::new(ParsedMessage {
                                info: MsgInfo::Int(int_msg.item.message.info().clone()),
                                dst_in_current_shard: true,
                                cell: int_msg.item.message.cell().clone(),
                                special_origin: None,
                                block_seqno: None,
                                from_same_shard: Some(int_msg.item.source == self.for_shard_id),
                            });

                            metrics.add_to_message_groups_timer.start();
                            range_reader.reader_state.buffer.add_message(msg);
                            metrics
                                .add_to_msgs_groups_ops_count
                                .saturating_add_assign(1);
                            metrics.add_to_message_groups_timer.stop();

                            metrics.read_existing_msgs_count += 1;

                            // update remaining messages statistics in iterator
                            if let Some(remaning_msgs_stats) =
                                range_reader.reader_state.remaning_msgs_stats.as_mut()
                            {
                                remaning_msgs_stats.decrement_for_account(
                                    int_msg.item.message.destination().clone(),
                                    1,
                                );
                            }
                        }
                        None => {
                            range_reader.fully_read = true;

                            // set current position to the end of the range
                            for (_, shard_reader_state) in
                                range_reader.reader_state.shards.iter_mut()
                            {
                                shard_reader_state.current_position =
                                    QueueKey::max_for_lt(shard_reader_state.to);
                            }

                            break 'read_range;
                        }
                    }
                }

                if !range_reader.fully_read {
                    all_ranges_fully_read = false;
                }
            }

            // if all ranges fully read try create next one
            if all_ranges_fully_read {
                if last_seqno < self.block_seqno {
                    // if open ranges limit not reached
                    if !self.open_ranges_limit_reached() {
                        if read_mode == GetNextMessageGroupMode::Continue {
                            let range_seqno = self.create_append_next_range_reader()?;
                            ranges_seqno.push_back(range_seqno);
                        } else {
                            // do not create next range reader on refill
                            tracing::debug!(target: tracing_targets::COLLATOR,
                                last_seqno,
                                "internals reader: do not create next range reader on Refill",
                            );
                            self.all_ranges_fully_read = true;
                            break;
                        }
                    } else {
                        // otherwise set all open ranges read
                        // to collect messages from all open ranges
                        // and then create next range
                        tracing::debug!(target: tracing_targets::COLLATOR,
                            partition_id = self.partition_id,
                            last_seqno,
                            open_ranges_limit = self.msgs_exec_params.open_ranges_limit,
                            "internals reader: open ranges limit reached",
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
            // remember last existing range
            last_seqno = *seqno;

            // skip new messages range readers
            if range_reader.kind == InternalsRangeReaderKind::NewMessages {
                continue;
            }

            // skip fully read ranges
            if range_reader.fully_read {
                continue;
            }

            // init reader if not initialized
            if !range_reader.initialized {
                range_reader.init()?;
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
                    range_reader.init()?;

                    return Ok(true);
                }
                None => {
                    range_reader.fully_read = true;

                    // set current position to the end of the range
                    for (_, shard_reader_state) in range_reader.reader_state.shards.iter_mut() {
                        shard_reader_state.current_position =
                            QueueKey::max_for_lt(shard_reader_state.to);
                    }
                }
            }
        }

        // if last range is not from current block then create and check next range
        if last_seqno < self.block_seqno {
            // we should look thru the whole range to check for pending messages
            // so we do not pass `range_max_messages` to force use the prev block end lt
            let mut range_reader = self.create_next_internals_range_reader(None)?;
            if !range_reader.fully_read {
                range_reader.init()?;

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
        let mut range_readers = BTreeMap::<BlockSeqno, InternalsRangeReader<V>>::new();
        while let Some((seqno, mut range_reader)) = self.pop_first_range_reader() {
            if matches!(
                (par_reader_stage, range_reader.kind),
                // skip new messages reader when reading existing
                (MessagesReaderStage::ExistingAndExternals, InternalsRangeReaderKind::NewMessages)
                // skip existing messages reader when reading new
                | (MessagesReaderStage::ExternalsAndNew, InternalsRangeReaderKind::Existing | InternalsRangeReaderKind::Next)
            ) {
                range_readers.insert(seqno, range_reader);
                continue;
            }

            // skip up to skip offset
            if self.reader_state.curr_processed_offset > range_reader.reader_state.skip_offset {
                res.metrics.add_to_message_groups_timer.start();
                let CollectMessagesFromRangeReaderResult {
                    mut collected_queue_msgs_keys,
                    ops_count,
                } = range_reader.collect_messages(
                    msg_group,
                    prev_par_readers,
                    &range_readers,
                    prev_msg_groups,
                );
                res.metrics
                    .add_to_msgs_groups_ops_count
                    .saturating_add_assign(ops_count);
                res.metrics.add_to_message_groups_timer.stop();
                res.collected_queue_msgs_keys
                    .append(&mut collected_queue_msgs_keys);
            }

            let range_reader_processed_offset = range_reader.reader_state.processed_offset;

            range_readers.insert(seqno, range_reader);

            // collect messages from the next range
            // only when current range processed offset is reached
            if self.reader_state.curr_processed_offset <= range_reader_processed_offset {
                break;
            }
        }
        self.set_range_readers(range_readers);

        Ok(res)
    }
}

#[derive(Debug, Default)]
struct InternalsRangeReaderInfo {
    last_to_lts: BTreeMap<ShardIdent, ShardReaderState>,
    last_range_block_seqno: BlockSeqno,
}

#[derive(Default)]
pub(super) struct CollectInternalsResult {
    pub metrics: MessagesReaderMetrics,
    pub collected_queue_msgs_keys: Vec<QueueKey>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum InternalsRangeReaderKind {
    Existing,
    Next,
    NewMessages,
}

pub(super) struct InternalsRangeReader<V: InternalMessageValue> {
    pub(super) partition_id: QueuePartitionIdx,
    pub(super) for_shard_id: ShardIdent,
    pub(super) seqno: BlockSeqno,
    pub(super) kind: InternalsRangeReaderKind,
    /// Target limits for filling message group from the buffer
    pub(super) buffer_limits: MessagesBufferLimits,
    pub(super) reader_state: InternalsRangeReaderState,
    pub(super) fully_read: bool,
    pub(super) mq_adapter: Arc<dyn MessageQueueAdapter<V>>,
    pub(super) iterator_opt: Option<Box<dyn QueueIterator<V>>>,
    pub(super) initialized: bool,
}

impl<V: InternalMessageValue> InternalsRangeReader<V> {
    fn init(&mut self) -> Result<()> {
        // do not init iterator if range is fully read
        if !self.fully_read {
            let mut ranges = Vec::with_capacity(self.reader_state.shards.len());

            for (shard_id, shard_reader_state) in &self.reader_state.shards {
                let shard_range_to = QueueKey::max_for_lt(shard_reader_state.to);
                ranges.push(QueueShardRange {
                    shard_ident: *shard_id,
                    from: shard_reader_state.current_position,
                    to: shard_range_to,
                });
            }

            let iterator =
                self.mq_adapter
                    .create_iterator(self.for_shard_id, self.partition_id, &ranges)?;

            self.iterator_opt = Some(iterator);
        }

        self.initialized = true;

        tracing::debug!(target: tracing_targets::COLLATOR,
            partition_id = self.partition_id,
            for_shard_id = %self.for_shard_id,
            seqno = self.seqno,
            fully_read = self.fully_read,
            "internals reader: initialized range reader",
        );

        Ok(())
    }

    fn collect_messages(
        &mut self,
        msg_group: &mut MessageGroup,
        prev_par_readers: &BTreeMap<QueuePartitionIdx, InternalsPartitionReader<V>>,
        prev_range_readers: &BTreeMap<BlockSeqno, InternalsRangeReader<V>>,
        prev_msg_groups: &BTreeMap<QueuePartitionIdx, MessageGroup>,
    ) -> CollectMessagesFromRangeReaderResult {
        let FillMessageGroupResult {
            collected_queue_msgs_keys,
            ops_count,
        } = self.reader_state.buffer.fill_message_group(
            msg_group,
            self.buffer_limits.slots_count,
            self.buffer_limits.slot_vert_size,
            |account_id| {
                let mut check_ops_count = 0;

                let dst_addr = IntAddr::from((self.for_shard_id.workchain() as i8, *account_id));

                for msg_group in prev_msg_groups.values() {
                    if msg_group.messages_count() > 0 {
                        check_ops_count.saturating_add_assign(1);
                        if msg_group.contains_account(account_id) {
                            return (true, check_ops_count);
                        }
                    }
                }

                // check by previous partitions
                for prev_par_reader in prev_par_readers.values() {
                    for prev_reader in prev_par_reader.range_readers().values() {
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

                // check by previous ranges in current partition
                for prev_reader in prev_range_readers.values() {
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

                (false, check_ops_count)
            },
        );

        CollectMessagesFromRangeReaderResult {
            collected_queue_msgs_keys,
            ops_count,
        }
    }
}

struct CollectMessagesFromRangeReaderResult {
    collected_queue_msgs_keys: Vec<QueueKey>,
    ops_count: u64,
}
