use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};
use std::sync::Arc;

use anyhow::{Context, Result};
use everscale_types::models::{MsgInfo, ShardIdent};
use tycho_block_util::queue::QueueKey;

use super::internals_reader::{
    InternalsParitionReader, InternalsRangeReader, InternalsRangeReaderKind,
};
use super::{InternalsRangeReaderState, ShardReaderState};
use crate::collator::messages_buffer::{BufferFillStateByCount, BufferFillStateBySlots};
use crate::collator::types::ParsedMessage;
use crate::internal_queue::state::state_iterator::MessageExt;
use crate::internal_queue::types::{
    EnqueuedMessage, InternalMessageValue, PartitionRouter, QueueDiffWithMessages, QueueStatistics,
};
use crate::tracing_targets;
use crate::types::processed_upto::PartitionId;
use crate::types::ProcessedTo;

//=========
// NEW MESSAGES
//=========

pub(super) struct NewMessagesState<V: InternalMessageValue> {
    current_shard: ShardIdent,
    messages: BTreeMap<QueueKey, Arc<V>>,
    partition_router: PartitionRouter,

    messages_for_current_shard: BTreeMap<PartitionId, BinaryHeap<Reverse<MessageExt<V>>>>,
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

    pub fn init_partition_router<'a>(
        &mut self,
        partition_id: u8,
        partition_all_ranges_msgs_stats: impl Iterator<Item = &'a QueueStatistics>,
    ) {
        for stats in partition_all_ranges_msgs_stats {
            for account_addr in stats.statistics().keys() {
                self.partition_router
                    .insert(account_addr.clone(), partition_id.try_into().unwrap());
            }
        }
    }

    pub fn has_pending_messages_from_partition(&self, partition_id: &u8) -> bool {
        self.messages_for_current_shard
            .get(partition_id)
            .map_or(false, |heap| !heap.is_empty())
    }

    pub fn has_pending_messages(&self) -> bool {
        self.messages_for_current_shard
            .values()
            .any(|heap| !heap.is_empty())
    }

    pub fn add_message(&mut self, message: Arc<V>) {
        self.messages.insert(message.key(), message.clone());
        if self.current_shard.contains_address(message.destination()) {
            let partition = self.partition_router.get_partition(message.destination());
            self.messages_for_current_shard
                .entry(partition as u8)
                .or_default()
                .push(Reverse(MessageExt::new(self.current_shard, message)));
        };
    }

    pub fn add_messages(&mut self, messages: impl IntoIterator<Item = Arc<V>>) {
        for message in messages {
            self.add_message(message);
        }
    }

    pub fn take_messages_for_current_shard(
        &mut self,
        partition_id: &u8,
    ) -> Option<BinaryHeap<Reverse<MessageExt<V>>>> {
        self.messages_for_current_shard.remove(partition_id)
    }

    pub fn restore_messages_for_current_shard(
        &mut self,
        partition_id: u8,
        messages: BinaryHeap<Reverse<MessageExt<V>>>,
        taken_messages: &[QueueKey],
    ) {
        self.messages_for_current_shard
            .insert(partition_id, messages);

        for key in taken_messages {
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

impl InternalsParitionReader {
    pub fn get_new_messages_range_reader(&mut self) -> Result<&mut InternalsRangeReader> {
        let (_, last_range_reader) = self.get_last_range_reader()?;

        // create range reader for new messages if it does not exist
        match last_range_reader.kind {
            InternalsRangeReaderKind::NewMessages => self.get_last_range_reader_mut(),
            InternalsRangeReaderKind::Existing | InternalsRangeReaderKind::Next => {
                let mut new_shard_reader_states = BTreeMap::default();
                for (shard_id, prev_shard_reader_state) in &last_range_reader.reader_state.shards {
                    new_shard_reader_states.insert(*shard_id, ShardReaderState {
                        from: prev_shard_reader_state.to,
                        to: prev_shard_reader_state.to,
                        current_position: QueueKey::min_for_lt(prev_shard_reader_state.to),
                    });
                }

                let reader = InternalsRangeReader {
                    partition_id: last_range_reader.partition_id,
                    for_shard_id: last_range_reader.for_shard_id,
                    seqno: last_range_reader.seqno,
                    kind: InternalsRangeReaderKind::NewMessages,
                    reader_state: InternalsRangeReaderState {
                        buffer: Default::default(),
                        shards: new_shard_reader_states,
                        processed_offset: 0,
                    },
                    fully_read: false,
                    mq_adapter: last_range_reader.mq_adapter.clone(),
                    iterator_opt: None,
                    // we do not need to additionally initialize new messages reader
                    initialized: true,

                    // we do not use messages satistics when reading new messages
                    msgs_stats: Default::default(),
                    remaning_msgs_stats: Default::default(),
                };

                self.insert_range_reader(reader.seqno, reader)
            }
        }
    }

    pub fn set_new_messages_range_reader_fully_read(&mut self) -> Result<()> {
        let last_range_reader = self.get_last_range_reader_mut()?;
        if matches!(
            last_range_reader.kind,
            InternalsRangeReaderKind::NewMessages
        ) {
            last_range_reader.fully_read = true;
        }
        Ok(())
    }

    pub fn read_new_messages_into_buffer(
        &mut self,
        new_messages: &mut BinaryHeap<Reverse<MessageExt<EnqueuedMessage>>>,
    ) -> Result<Vec<QueueKey>> {
        let mut taken_messages = vec![];

        // if there are no new messages, return early
        if new_messages.is_empty() {
            self.set_new_messages_range_reader_fully_read()?;
            return Ok(taken_messages);
        }

        // get range reader for new messages, create if not exists
        let partition_id = self.partition_id;
        let for_shard_id = self.for_shard_id;
        let max_limits = self.max_limits;

        let range_reader = self.get_new_messages_range_reader()?;
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
                    shard_reader_state.to = shard_reader_state.current_position.lt;

                    // remember taken message
                    taken_messages.push(msg.message.key());

                    // add message to buffer
                    range_reader
                        .reader_state
                        .buffer
                        .add_message(Box::new(ParsedMessage {
                            info: MsgInfo::Int(msg.message.info.clone()),
                            dst_in_current_shard: true,
                            cell: msg.message.cell.clone(),
                            special_origin: None,
                            dequeued: None,
                        }));
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
                        partition_id,
                        seqno = range_reader.seqno,
                        "new messages reader: can fill message group on ({}x{})",
                        max_limits.slots_count, max_limits.slot_vert_size,
                    );
                } else {
                    tracing::debug!(target: tracing_targets::COLLATOR,
                        partition_id,
                        seqno = range_reader.seqno,
                        "new messages reader: message buffer filled on {}/{}",
                        range_reader.reader_state.buffer.msgs_count(), max_limits.max_count,
                    );
                }
                break;
            }
        }

        Ok(taken_messages)
    }
}
