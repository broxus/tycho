use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use everscale_types::cell::HashBytes;
use everscale_types::models::{IntAddr, MsgInfo, ShardIdent};
use tycho_block_util::queue::QueueKey;
use tycho_util::FastHashMap;

use super::{
    DebugInternalsRangeReaderState, InternalsPartitionReaderState, InternalsRangeReaderState,
    MessagesReaderMetrics, MessagesReaderStage, ShardReaderState,
};
use crate::collator::messages_buffer::{
    BufferFillStateByCount, BufferFillStateBySlots, FastIndexSet, FillMessageGroupResult,
    MessageGroup, MessagesBufferLimits,
};
use crate::collator::types::ParsedMessage;
use crate::internal_queue::iterator::QueueIterator;
use crate::internal_queue::types::{EnqueuedMessage, QueueShardRange, QueueStatistics};
use crate::queue_adapter::MessageQueueAdapter;
use crate::tracing_targets;
use crate::types::processed_upto::{BlockSeqno, Lt, PartitionId};

//=========
// INTERNALS READER
//=========

// TODO: msgs-v3: rename to InternalsPartitionReader
pub(super) struct InternalsParitionReader {
    pub(super) partition_id: PartitionId,
    pub(super) for_shard_id: ShardIdent,
    pub(super) block_seqno: BlockSeqno,

    pub(super) target_limits: MessagesBufferLimits,
    pub(super) max_limits: MessagesBufferLimits,

    /// mc state gen lt
    mc_state_gen_lt: Lt,
    /// prev shard state gen lt
    prev_state_gen_lt: Lt,
    /// end lt list from top shards of mc block
    mc_top_shards_end_lts: Vec<(ShardIdent, Lt)>,

    mq_adapter: Arc<dyn MessageQueueAdapter<EnqueuedMessage>>,

    reader_state: InternalsPartitionReaderState,
    range_readers: BTreeMap<BlockSeqno, InternalsRangeReader>,

    pub(super) all_ranges_fully_read: bool,
}

pub(super) struct InternalsPartitionReaderContext {
    pub partition_id: PartitionId,
    pub for_shard_id: ShardIdent,
    pub block_seqno: BlockSeqno,
    pub target_limits: MessagesBufferLimits,
    pub max_limits: MessagesBufferLimits,
    pub mc_state_gen_lt: Lt,
    pub prev_state_gen_lt: Lt,
    pub mc_top_shards_end_lts: Vec<(ShardIdent, Lt)>,
    pub reader_state: InternalsPartitionReaderState,
}

impl InternalsParitionReader {
    pub fn new(
        cx: InternalsPartitionReaderContext,
        mq_adapter: Arc<dyn MessageQueueAdapter<EnqueuedMessage>>,
    ) -> Result<Self> {
        let mut reader = Self {
            partition_id: cx.partition_id,
            for_shard_id: cx.for_shard_id,
            block_seqno: cx.block_seqno,
            target_limits: cx.target_limits,
            max_limits: cx.max_limits,
            mc_state_gen_lt: cx.mc_state_gen_lt,
            prev_state_gen_lt: cx.prev_state_gen_lt,
            mc_top_shards_end_lts: cx.mc_top_shards_end_lts,
            mq_adapter,
            reader_state: cx.reader_state,
            range_readers: Default::default(),
            all_ranges_fully_read: false,
        };

        reader.create_range_readers()?;

        Ok(reader)
    }

    pub(super) fn drop_next_range_reader(&mut self) {
        self.range_readers
            .retain(|_, r| r.kind != InternalsRangeReaderKind::Next);
    }

    pub fn finalize(mut self, current_next_lt: u64) -> Result<InternalsPartitionReaderState> {
        // update new messages "to" boundary on current block next lt
        self.update_new_messages_reader_to_boundary(current_next_lt)?;

        // collect range reader states
        // ignore next range reader if it was not initialized
        // and we did not read any messages from it
        // to reduce stored range reader states in processed upto info
        let mut range_readers = self
            .range_readers
            .into_iter()
            .filter_map(|(seqno, r)| match r.kind {
                InternalsRangeReaderKind::Next if !r.initialized => None,
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

            self.reader_state
                .ranges
                .insert(seqno, range_reader.reader_state);
        }

        // return updated partition reader state
        Ok(self.reader_state)
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

    pub fn all_existing_messages_collected(&self) -> bool {
        self.all_ranges_fully_read && !self.has_messages_in_buffers()
    }

    pub fn all_new_messages_collected(&self, has_pending_new_messages: bool) -> bool {
        !has_pending_new_messages && !self.has_messages_in_buffers()
    }

    pub fn reader_state(&self) -> &InternalsPartitionReaderState {
        &self.reader_state
    }

    pub fn range_readers(&self) -> &BTreeMap<BlockSeqno, InternalsRangeReader> {
        &self.range_readers
    }

    pub fn retain_only_last_range_reader(&mut self) -> Result<()> {
        let (last_seqno, last_range_reader) = self
            .range_readers
            .pop_last()
            .context("partition reader should have at least one range reader")?;
        self.range_readers.clear();
        self.range_readers.insert(last_seqno, last_range_reader);
        Ok(())
    }

    pub fn pop_first_range_reader(&mut self) -> Option<(BlockSeqno, InternalsRangeReader)> {
        self.range_readers.pop_first()
    }

    pub fn set_range_readers(
        &mut self,
        mut range_readers: BTreeMap<BlockSeqno, InternalsRangeReader>,
    ) {
        self.range_readers.append(&mut range_readers);
    }

    pub(super) fn insert_range_reader(
        &mut self,
        seqno: BlockSeqno,
        reader: InternalsRangeReader,
    ) -> &mut InternalsRangeReader {
        self.range_readers.insert(seqno, reader);
        self.range_readers
            .get_mut(&seqno)
            .expect("just inserted range reader should exist")
    }

    pub fn get_last_range_reader(&self) -> Result<(&BlockSeqno, &InternalsRangeReader)> {
        self.range_readers
            .last_key_value()
            .context("partition reader should have at least one range reader")
    }

    pub fn get_last_range_reader_mut(&mut self) -> Result<&mut InternalsRangeReader> {
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

    pub fn set_skip_offset_to_current(&mut self) -> Result<()> {
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

    fn create_range_readers(&mut self) -> Result<()> {
        // create existing range readers
        let mut all_ranges_fully_read = true;
        while let Some((seqno, range_reader_state)) = self.reader_state.ranges.pop_first() {
            let reader = self.create_existing_internals_range_reader(range_reader_state, seqno)?;
            if !reader.fully_read {
                all_ranges_fully_read = false;
            }
            self.range_readers.insert(seqno, reader);
        }

        // and create next range reader if it not exist
        let (last_seqno, last_range_reader_shards_and_offset_opt) = self
            .range_readers
            .last_key_value()
            .map(|(seqno, reader)| {
                (
                    Some(*seqno),
                    Some((
                        reader.reader_state.shards.clone(),
                        reader.reader_state.processed_offset,
                    )),
                )
            })
            .unwrap_or_default();
        if !matches!(last_seqno, Some(seqno) if seqno == self.block_seqno) {
            let reader = self.create_next_internals_range_reader(
                last_range_reader_shards_and_offset_opt,
                self.block_seqno,
            )?;
            if !reader.fully_read {
                all_ranges_fully_read = false;
            }
            self.range_readers.insert(self.block_seqno, reader);
        }

        self.all_ranges_fully_read = all_ranges_fully_read;

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    fn create_existing_internals_range_reader(
        &self,
        range_reader_state: InternalsRangeReaderState,
        seqno: BlockSeqno,
    ) -> Result<InternalsRangeReader> {
        let mut ranges = vec![];

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

        // get statistics for the range
        let stats = self
            .mq_adapter
            .get_statistics(self.partition_id.try_into()?, ranges)?;

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

            msgs_stats: stats.clone(),
            remaning_msgs_stats: stats,
        };

        tracing::debug!(target: tracing_targets::COLLATOR,
            partition_id = reader.partition_id,
            for_shard_id = %reader.for_shard_id,
            seqno = reader.seqno,
            fully_read = reader.fully_read,
            reader_state = ?DebugInternalsRangeReaderState(&reader.reader_state),
            "created existing range reader",
        );

        Ok(reader)
    }

    #[tracing::instrument(skip_all)]
    fn create_next_internals_range_reader(
        &self,
        last_range_reader_shards_and_offset_opt: Option<(
            BTreeMap<ShardIdent, ShardReaderState>,
            u32,
        )>,
        seqno: BlockSeqno,
    ) -> Result<InternalsRangeReader> {
        let mut shard_reader_states = BTreeMap::new();

        let all_end_lts = [(ShardIdent::MASTERCHAIN, self.mc_state_gen_lt)]
            .into_iter()
            .chain(self.mc_top_shards_end_lts.iter().cloned());

        let mut ranges = vec![];

        let mut fully_read = true;
        for (shard_id, end_lt) in all_end_lts {
            let last_to_lt_opt = last_range_reader_shards_and_offset_opt
                .as_ref()
                .and_then(|(s_r, _)| s_r.get(&shard_id).map(|s| s.to));
            let shard_range_from =
                last_to_lt_opt.map_or_else(|| QueueKey::min_for_lt(0), QueueKey::max_for_lt);

            let to_lt = if shard_id == self.for_shard_id {
                self.prev_state_gen_lt
            } else {
                end_lt
            };
            let shard_range_to = QueueKey::max_for_lt(to_lt);

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

        // get statistics for the range
        let stats = self
            .mq_adapter
            .get_statistics(self.partition_id.try_into()?, ranges)?;

        let processed_offset = last_range_reader_shards_and_offset_opt
            .map(|(_, processed_offset)| processed_offset)
            .unwrap_or_default();

        let reader = InternalsRangeReader {
            partition_id: self.partition_id,
            for_shard_id: self.for_shard_id,
            seqno,
            kind: InternalsRangeReaderKind::Next,
            buffer_limits: self.target_limits,
            reader_state: InternalsRangeReaderState {
                buffer: Default::default(),
                shards: shard_reader_states,
                skip_offset: processed_offset,
                processed_offset,
            },
            fully_read,
            mq_adapter: self.mq_adapter.clone(),
            iterator_opt: None,
            initialized: false,

            msgs_stats: stats.clone(),
            remaning_msgs_stats: stats,
        };

        tracing::debug!(target: tracing_targets::COLLATOR,
            partition_id = reader.partition_id,
            for_shard_id = %reader.for_shard_id,
            seqno = reader.seqno,
            fully_read = reader.fully_read,
            reader_state = ?DebugInternalsRangeReaderState(&reader.reader_state),
            "created next range reader",
        );

        Ok(reader)
    }

    pub fn read_existing_messages_into_buffers(&mut self) -> Result<MessagesReaderMetrics> {
        let mut metrics = MessagesReaderMetrics::default();

        // skip if all ranges fully read
        if self.all_ranges_fully_read {
            return Ok(metrics);
        }

        metrics.read_existing_messages_timer.start();

        // take next not fully read range and continue reading
        let mut all_ranges_fully_read = true;
        'iter_ranges: for (seqno, range_reader) in self.range_readers.iter_mut() {
            // skip fully read ranges
            if range_reader.fully_read {
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
                bail!("not fully read range should have iterator");
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
                    (&BufferFillStateByCount::IsFull, _) | (_, &BufferFillStateBySlots::CanFill)
                ) {
                    // update current position from iterator
                    let iterator_current_positions = iterator.current_position();
                    for (shard_id, curr_pos) in iterator_current_positions {
                        let Some(shard_reader_state) =
                            range_reader.reader_state.shards.get_mut(&shard_id)
                        else {
                            bail!("shard reader state for existing iterator should exist")
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
                        all_ranges_fully_read = false;
                        break 'iter_ranges;
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
                            info: MsgInfo::Int(int_msg.item.message.info.clone()),
                            dst_in_current_shard: true,
                            cell: int_msg.item.message.cell.clone(),
                            special_origin: None,
                            block_seqno: None,
                            from_same_shard: Some(int_msg.item.source == self.for_shard_id),
                        });

                        metrics.add_to_message_groups_timer.start();
                        range_reader.reader_state.buffer.add_message(msg);
                        metrics.add_to_message_groups_timer.stop();

                        metrics.read_int_msgs_from_iterator_count += 1;

                        // update remaining messages statistics in iterator
                        range_reader
                            .remaning_msgs_stats
                            .decrement_for_account(int_msg.item.message.destination().clone(), 1);
                    }
                    None => {
                        range_reader.fully_read = true;

                        // set current position to the end of the range
                        for (_, shard_reader_state) in range_reader.reader_state.shards.iter_mut() {
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

        self.all_ranges_fully_read = all_ranges_fully_read;

        metrics.read_existing_messages_timer.stop();
        metrics.read_existing_messages_timer.total_elapsed -=
            metrics.init_iterator_timer.total_elapsed;
        metrics.read_existing_messages_timer.total_elapsed -=
            metrics.add_to_message_groups_timer.total_elapsed;

        Ok(metrics)
    }

    pub fn check_has_pending_internals_in_iterators(&mut self) -> Result<bool> {
        for (_, range_reader) in self.range_readers.iter_mut() {
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
                bail!("not fully read range should have iterator");
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

        Ok(false)
    }

    pub fn collect_messages(
        &mut self,
        par_reader_stage: &MessagesReaderStage,
        msg_group: &mut MessageGroup,
        unused_buffer_accounts_by_partitions: &mut FastHashMap<
            (PartitionId, BlockSeqno),
            FastIndexSet<HashBytes>,
        >,
        prev_par_readers: &BTreeMap<PartitionId, InternalsParitionReader>,
    ) -> Result<CollectInternalsResult> {
        let mut res = CollectInternalsResult::default();

        // do not collect internals on FinishExternals stage
        if matches!(par_reader_stage, MessagesReaderStage::FinishExternals) {
            return Ok(res);
        }

        // extract range readers from state to use previous readers buffers and stats
        // to check for account skip on collecting messages from the next
        let mut range_readers = BTreeMap::<BlockSeqno, InternalsRangeReader>::new();
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
                } = range_reader.collect_messages(
                    msg_group,
                    unused_buffer_accounts_by_partitions,
                    prev_par_readers,
                    &range_readers,
                );
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

pub(super) struct InternalsRangeReader {
    pub(super) partition_id: PartitionId,
    pub(super) for_shard_id: ShardIdent,
    pub(super) seqno: BlockSeqno,
    pub(super) kind: InternalsRangeReaderKind,
    /// Target limits for filling message group from the buffer
    pub(super) buffer_limits: MessagesBufferLimits,
    pub(super) reader_state: InternalsRangeReaderState,
    pub(super) fully_read: bool,
    pub(super) mq_adapter: Arc<dyn MessageQueueAdapter<EnqueuedMessage>>,
    pub(super) iterator_opt: Option<Box<dyn QueueIterator<EnqueuedMessage>>>,
    pub(super) initialized: bool,

    /// Statistics shows all messages in current range
    pub(super) msgs_stats: QueueStatistics,
    /// Statistics shows remaining not read messages from currebt range.
    /// We reduce initial statistics by the number of messages that were read.
    pub(super) remaning_msgs_stats: QueueStatistics,
}

impl InternalsRangeReader {
    fn init(&mut self) -> Result<()> {
        // do not init iterator if range is fully read
        if !self.fully_read {
            let mut ranges = vec![];

            for (shard_id, shard_reader_state) in &self.reader_state.shards {
                let shard_range_to = QueueKey::max_for_lt(shard_reader_state.to);
                ranges.push(QueueShardRange {
                    shard_ident: *shard_id,
                    from: shard_reader_state.current_position,
                    to: shard_range_to,
                });
            }

            let iterator = self.mq_adapter.create_iterator(
                self.for_shard_id,
                self.partition_id.try_into()?,
                ranges,
            )?;

            self.iterator_opt = Some(iterator);
        }

        self.initialized = true;

        tracing::debug!(target: tracing_targets::COLLATOR,
            partition_id = self.partition_id,
            for_shard_id = %self.for_shard_id,
            seqno = self.seqno,
            fully_read = self.fully_read,
            "initialized range reader",
        );

        Ok(())
    }

    fn collect_messages(
        &mut self,
        msg_group: &mut MessageGroup,
        unused_buffer_accounts_by_partitions: &mut FastHashMap<
            (PartitionId, BlockSeqno),
            FastIndexSet<HashBytes>,
        >,
        prev_par_readers: &BTreeMap<PartitionId, InternalsParitionReader>,
        prev_range_readers: &BTreeMap<BlockSeqno, InternalsRangeReader>,
    ) -> CollectMessagesFromRangeReaderResult {
        let FillMessageGroupResult {
            unused_buffer_accounts,
            collected_queue_msgs_keys,
        } = self.reader_state.buffer.fill_message_group(
            msg_group,
            self.buffer_limits.slots_count,
            self.buffer_limits.slot_vert_size,
            unused_buffer_accounts_by_partitions.remove(&(self.partition_id, self.seqno)),
            |account_id| {
                let dst_addr = IntAddr::from((self.for_shard_id.workchain() as i8, *account_id));

                // check by previous partitions
                for prev_par_reader in prev_par_readers.values() {
                    for prev_reader in prev_par_reader.range_readers().values() {
                        if prev_reader
                            .reader_state
                            .buffer
                            .account_messages_count(account_id)
                            > 0
                        {
                            return true;
                        }
                        if prev_reader
                            .remaning_msgs_stats
                            .statistics()
                            .contains_key(&dst_addr)
                        {
                            return true;
                        }
                    }
                }

                // check by previous ranges in current partition
                for prev_reader in prev_range_readers.values() {
                    if prev_reader
                        .reader_state
                        .buffer
                        .account_messages_count(account_id)
                        > 0
                    {
                        return true;
                    }
                    if prev_reader
                        .remaning_msgs_stats
                        .statistics()
                        .contains_key(&dst_addr)
                    {
                        return true;
                    }
                }
                false
            },
        );
        unused_buffer_accounts_by_partitions
            .insert((self.partition_id, self.seqno), unused_buffer_accounts);

        CollectMessagesFromRangeReaderResult {
            collected_queue_msgs_keys,
        }
    }
}

struct CollectMessagesFromRangeReaderResult {
    collected_queue_msgs_keys: Vec<QueueKey>,
}
