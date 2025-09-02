use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};
use std::sync::Arc;

use anyhow::{Context, Result};
use tycho_block_util::queue::{QueueKey, QueuePartitionIdx};
use tycho_types::models::{MsgInfo, ShardIdent};

use super::internals_reader::{
    InternalsPartitionReader, InternalsRangeReader, InternalsRangeReaderKind,
};
use super::{
    DebugInternalsRangeReaderState, InternalsRangeReaderState, MessagesReaderMetrics,
    ShardReaderState,
};
use crate::collator::messages_buffer::{BufferFillStateByCount, BufferFillStateBySlots};
use crate::collator::types::ParsedMessage;
use crate::internal_queue::state::state_iterator::MessageExt;
use crate::internal_queue::types::{
    AccountStatistics, InternalMessageValue, PartitionRouter, QueueDiffWithMessages,
};
use crate::tracing_targets;
use crate::types::{ProcessedTo, SaturatingAddAssign};

//=========
// NEW MESSAGES
//=========

pub(super) struct NewMessagesState<V: InternalMessageValue> {
    current_shard: ShardIdent,
    messages: BTreeMap<QueueKey, Arc<V>>,
    partition_router: PartitionRouter,

    messages_for_current_shard: BTreeMap<QueuePartitionIdx, BinaryHeap<Reverse<MessageExt<V>>>>,
}

impl<V: InternalMessageValue> NewMessagesState<V> {
    pub fn new(current_shard: ShardIdent) -> Self {
        Self {
            current_shard,
            messages: Default::default(),
            partition_router: Default::default(),

            messages_for_current_shard: Default::default(),
        }
    }

    pub fn partition_router(&self) -> &PartitionRouter {
        &self.partition_router
    }

    pub fn init_partition_router(
        &mut self,
        partition_id: QueuePartitionIdx,
        cumulative_partition_stats: &AccountStatistics,
    ) {
        for account_addr in cumulative_partition_stats.keys() {
            self.partition_router
                .insert_dst(account_addr, partition_id)
                .unwrap();
        }
    }

    pub fn has_pending_messages_from_partition(&self, partition_id: QueuePartitionIdx) -> bool {
        self.messages_for_current_shard
            .get(&partition_id)
            .is_some_and(|heap| !heap.is_empty())
    }

    pub fn has_pending_messages(&self) -> bool {
        self.messages_for_current_shard
            .values()
            .any(|heap| !heap.is_empty())
    }

    pub fn add_message(&mut self, message: Arc<V>) {
        self.messages.insert(message.key(), message.clone());
        if self.current_shard.contains_address(message.destination()) {
            let partition = self
                .partition_router
                .get_partition(Some(message.source()), message.destination());
            self.messages_for_current_shard
                .entry(partition)
                .or_default()
                .push(Reverse(MessageExt::new(self.current_shard, message)));
        };
    }

    pub fn add_messages(&mut self, messages: impl IntoIterator<Item = Arc<V>>) {
        for message in messages {
            self.add_message(message);
        }
    }

    pub fn messages_for_current_shard(
        &mut self,
        partition_id: QueuePartitionIdx,
    ) -> &mut BinaryHeap<Reverse<MessageExt<V>>> {
        self.messages_for_current_shard
            .get_mut(&partition_id)
            .unwrap()
    }

    pub fn remove_collected_messages(&mut self, collected_messages: &[QueueKey]) {
        for key in collected_messages {
            self.messages.remove(key);
        }
    }

    pub fn into_queue_diff_with_messages(
        self,
        processed_to: ProcessedTo,
    ) -> QueueDiffWithMessages<V> {
        QueueDiffWithMessages {
            messages: self.messages,
            processed_to,
            partition_router: self.partition_router,
        }
    }
}

impl<V: InternalMessageValue> InternalsPartitionReader<V> {
    /// Returns range reader for new messages, creates it if not yet exist
    pub fn get_new_messages_range_reader(
        &mut self,
        current_next_lt: u64,
    ) -> Result<&mut InternalsRangeReader<V>> {
        let (_, last_range_reader) = self.get_last_range_reader()?;

        // create range reader for new messages if it does not exist
        if !matches!(
            last_range_reader.kind,
            InternalsRangeReaderKind::NewMessages
        ) {
            let mut new_shard_reader_states = BTreeMap::new();
            for (shard_id, prev_shard_reader_state) in &last_range_reader.reader_state.shards {
                let shard_range_to = if shard_id == &self.for_shard_id {
                    current_next_lt
                } else {
                    prev_shard_reader_state.to
                };
                new_shard_reader_states.insert(*shard_id, ShardReaderState {
                    from: prev_shard_reader_state.to,
                    to: shard_range_to,
                    current_position: QueueKey::max_for_lt(prev_shard_reader_state.to),
                });
            }

            let reader = InternalsRangeReader {
                partition_id: last_range_reader.partition_id,
                for_shard_id: last_range_reader.for_shard_id,
                seqno: last_range_reader.seqno,
                kind: InternalsRangeReaderKind::NewMessages,
                buffer_limits: self.target_limits(),
                reader_state: InternalsRangeReaderState {
                    buffer: Default::default(),

                    // we do not use messages satistics when reading new messages
                    msgs_stats: None,
                    remaning_msgs_stats: None,
                    read_stats: Default::default(),

                    shards: new_shard_reader_states,
                    skip_offset: 0,
                    processed_offset: 0,
                },
                fully_read: false,
                mq_adapter: last_range_reader.mq_adapter.clone(),
                iterator_opt: None,
                // we do not need to additionally initialize new messages reader
                initialized: true,
            };

            // drop flag when we add new messages range reader
            self.all_ranges_fully_read = false;

            tracing::debug!(target: tracing_targets::COLLATOR,
                partition_id = %reader.partition_id,
                for_shard_id = %reader.for_shard_id,
                seqno = reader.seqno,
                fully_read = reader.fully_read,
                reader_state = ?DebugInternalsRangeReaderState(&reader.reader_state),
                "created new messages reader",
            );

            self.insert_range_reader(reader.seqno, reader);
        } else {
            // otherwise update new messages reader "to" boundary on current next lt
            self.update_new_messages_reader_to_boundary(current_next_lt)?;
        }

        // and return the mutable ref
        self.get_last_range_reader_mut()
    }

    pub(super) fn update_new_messages_reader_to_boundary(
        &mut self,
        current_next_lt: u64,
    ) -> Result<()> {
        let for_shard_id = self.for_shard_id;
        let Ok(last_range_reader) = self.get_last_range_reader_mut() else {
            return Ok(());
        };
        if last_range_reader.kind == InternalsRangeReaderKind::NewMessages {
            let current_shard_reader_state = last_range_reader
                .reader_state
                .shards
                .get_mut(&for_shard_id)
                .context("new messages range reader should have current shard reader state")?;
            if current_shard_reader_state.to < current_next_lt {
                current_shard_reader_state.to = current_next_lt;

                last_range_reader.fully_read = false;
                self.all_ranges_fully_read = false;
            }
        }
        Ok(())
    }

    fn set_new_messages_range_reader_fully_read(&mut self) -> Result<()> {
        let for_shard_id = self.for_shard_id;
        let last_range_reader = self.get_last_range_reader_mut()?;
        if last_range_reader.kind == InternalsRangeReaderKind::NewMessages {
            // set current position to the end of the range
            let current_shard_reader_state = last_range_reader
                .reader_state
                .shards
                .get_mut(&for_shard_id)
                .context("new messages range reader should have current shard reader state")?;
            current_shard_reader_state.current_position =
                QueueKey::max_for_lt(current_shard_reader_state.to);

            last_range_reader.fully_read = true;
            self.all_ranges_fully_read = true;
        }
        Ok(())
    }

    pub fn read_new_messages_into_buffers(
        &mut self,
        new_messages: &mut NewMessagesState<V>,
        current_next_lt: u64,
    ) -> Result<ReadNewMessagesResult> {
        // if no new messages for current partition then return earlier
        if !new_messages.has_pending_messages_from_partition(self.partition_id) {
            self.set_new_messages_range_reader_fully_read()?;
            return Ok(ReadNewMessagesResult::default());
        }

        // read new messages to buffer
        let res = self.read_new_messages_into_buffer_impl(
            new_messages.messages_for_current_shard(self.partition_id),
            current_next_lt,
        )?;

        Ok(res)
    }

    fn read_new_messages_into_buffer_impl(
        &mut self,
        new_messages: &mut BinaryHeap<Reverse<MessageExt<V>>>,
        current_next_lt: u64,
    ) -> Result<ReadNewMessagesResult> {
        let mut res = ReadNewMessagesResult::default();
        let block_seqno = self.block_seqno;

        // if there are no new messages, return early
        if new_messages.is_empty() {
            self.set_new_messages_range_reader_fully_read()?;
            return Ok(res);
        }

        res.metrics.read_new_messages_timer.start();

        // get range reader for new messages, create if not exists
        let partition_id = self.partition_id;
        let for_shard_id = self.for_shard_id;
        let max_limits = self.max_limits();

        let range_reader = self.get_new_messages_range_reader(current_next_lt)?;
        let shard_reader_state = range_reader
            .reader_state
            .shards
            .get_mut(&for_shard_id)
            .context("shard reader state should exist")?;

        loop {
            // read next new message and add it to buffer
            match new_messages.pop() {
                Some(Reverse(msg)) => {
                    // update current position
                    shard_reader_state.current_position = msg.message.key();

                    // remember taken message
                    res.taken_messages.push(msg.message.key());

                    // add message to buffer
                    res.metrics.add_to_message_groups_timer.start();
                    range_reader
                        .reader_state
                        .buffer
                        .add_message(Box::new(ParsedMessage {
                            info: MsgInfo::Int(msg.message.info().clone()),
                            dst_in_current_shard: true,
                            cell: msg.message.cell().clone(),
                            special_origin: None,
                            block_seqno: Some(block_seqno),
                            from_same_shard: Some(msg.source == for_shard_id),
                            ext_msg_chain_time: None,
                        }));
                    res.metrics
                        .add_to_msgs_groups_ops_count
                        .saturating_add_assign(1);
                    res.metrics.add_to_message_groups_timer.stop();

                    res.metrics.read_new_msgs_count += 1;
                }
                None => {
                    self.set_new_messages_range_reader_fully_read()?;
                    break;
                }
            }

            // stop reading if buffer is full
            // or we can already fill required slots
            let (fill_state_by_count, fill_state_by_slots) = range_reader
                .reader_state
                .buffer
                .check_is_filled(&max_limits);
            if matches!(
                (&fill_state_by_count, &fill_state_by_slots),
                (&BufferFillStateByCount::IsFull, _) | (_, &BufferFillStateBySlots::CanFill)
            ) {
                if matches!(fill_state_by_slots, BufferFillStateBySlots::CanFill) {
                    tracing::debug!(target: tracing_targets::COLLATOR,
                        %partition_id,
                        seqno = range_reader.seqno,
                        "new messages reader: can fill message group on ({}x{})",
                        max_limits.slots_count, max_limits.slot_vert_size,
                    );
                } else {
                    tracing::debug!(target: tracing_targets::COLLATOR,
                        %partition_id,
                        seqno = range_reader.seqno,
                        "new messages reader: message buffer filled on {}/{}",
                        range_reader.reader_state.buffer.msgs_count(), max_limits.max_count,
                    );
                }
                break;
            }
        }

        res.has_pending_new_messages = !new_messages.is_empty();

        res.metrics.read_new_messages_timer.stop();
        res.metrics.read_new_messages_timer.total_elapsed -=
            res.metrics.add_to_message_groups_timer.total_elapsed;

        Ok(res)
    }
}

#[derive(Default)]
pub(super) struct ReadNewMessagesResult {
    pub taken_messages: Vec<QueueKey>,
    pub has_pending_new_messages: bool,
    pub metrics: MessagesReaderMetrics,
}
