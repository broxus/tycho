use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use everscale_types::cell::HashBytes;
use everscale_types::models::ShardIdent;
use tycho_block_util::queue::QueueKey;
use tycho_util::FastHashMap;

use super::execution_manager::GetNextMessageGroupContext;
use super::messages_buffer::{MessageGroupV2, MessagesBufferV2};
use super::mq_iterator_adapter::QueueIteratorAdapter;
use super::types::AnchorsCache;
use crate::collator::execution_manager::GetNextMessageGroupMode;
use crate::internal_queue::state::state_iterator::MessageExt;
use crate::internal_queue::types::{EnqueuedMessage, InternalMessageValue, QueueDiffWithMessages};
use crate::mempool::MempoolAnchorId;
use crate::tracing_targets;
use crate::types::processed_upto::{
    ExternalsProcessedUptoStuff, ExternalsRangeInfo, InternalsProcessedUptoStuffV2,
    InternalsRangeStuff, PartitionProcessedUptoStuff, ProcessedUptoInfoStuffV2, ShardRangeInfo,
};

#[derive(Debug, Default)]
pub(super) struct MessagesReaderMetrics {
    /// sum total time of reading existing internal messages
    pub read_existing_messages_total_elapsed: Duration,
    /// sum total time of reading new internal messages
    pub read_new_messages_total_elapsed: Duration,
    /// sum total time of reading external messages
    pub read_ext_messages_total_elapsed: Duration,
    /// sum total time of adding messages to buffers
    pub add_to_message_groups_total_elapsed: Duration,

    /// num of existing internal messages read
    pub read_int_msgs_from_iterator_count: u64,
    /// num of external messages read
    pub read_ext_msgs_count: u64,
    /// num of new internal messages read
    pub read_new_msgs_from_iterator_count: u64,
}

pub(super) struct MessagesReaderV2 {
    shard_id: ShardIdent,

    /// max number of messages that could be loaded into buffer
    messages_buffer_limit: usize,
    group_limit: usize,
    group_vert_size: usize,

    /// last read to anchor chain time
    last_read_to_anchor_chain_time: Option<u64>,

    /// end lt list from top shards of mc block
    mc_top_shards_end_lts: Vec<(ShardIdent, u64)>,

    metrics: MessagesReaderMetrics,

    new_messages: NewMessagesState<EnqueuedMessage>,

    state: ReaderState,

    anchors_cache: AnchorsCache,

    mq_iterator_adapter: QueueIteratorAdapter<EnqueuedMessage>,
}

impl MessagesReaderV2 {
    pub fn new(
        shard_id: ShardIdent,
        messages_buffer_limit: usize,
        group_limit: usize,
        group_vert_size: usize,
        mc_top_shards_end_lts: Vec<(ShardIdent, u64)>,
        state: ReaderState,
        anchors_cache: AnchorsCache,
        mq_iterator_adapter: QueueIteratorAdapter<EnqueuedMessage>,
    ) -> Self {
        metrics::gauge!("tycho_do_collate_msgs_exec_params_buffer_limit")
            .set(messages_buffer_limit as f64);
        metrics::gauge!("tycho_do_collate_msgs_exec_params_group_limit").set(group_limit as f64);
        metrics::gauge!("tycho_do_collate_msgs_exec_params_group_vert_size")
            .set(group_vert_size as f64);

        Self {
            shard_id,

            messages_buffer_limit,
            group_limit,
            group_vert_size,

            last_read_to_anchor_chain_time: None,
            mc_top_shards_end_lts,

            metrics: Default::default(),

            new_messages: NewMessagesState::new(shard_id),

            state,
            anchors_cache,
            mq_iterator_adapter,
        }
    }

    pub fn reset_read_state(&mut self) {
        self.metrics = Default::default();
    }

    pub fn finalize(
        self,
    ) -> Result<(
        bool,
        ReaderState,
        AnchorsCache,
        QueueDiffWithMessages<EnqueuedMessage>,
    )> {
        // TODO: should check if has pending internals in iterators
        let check_pending_internals = !self.has_messages_in_buffers();
        let (has_pending_internals_in_iterators, _) = self
            .mq_iterator_adapter
            .release(check_pending_internals, &mut None)?;

        // TODO: should update current positions in reader_state from iterators

        Ok((
            has_pending_internals_in_iterators,
            self.state,
            self.anchors_cache,
            self.new_messages.queue_diff_with_messages,
        ))
    }

    pub fn last_read_to_anchor_chain_time(&self) -> Option<u64> {
        self.last_read_to_anchor_chain_time
    }

    pub fn metrics(&self) -> &MessagesReaderMetrics {
        &self.metrics
    }

    pub fn new_messages_mut(&mut self) -> &mut NewMessagesState<EnqueuedMessage> {
        &mut self.new_messages
    }

    pub fn has_messages_in_buffers(&self) -> bool {
        todo!()
    }

    pub fn no_pending_existing_internals(&self) -> bool {
        todo!()
    }

    pub fn no_pending_new_messages(&self) -> bool {
        todo!()
    }

    pub fn init_iterator_total_elapsed(&self) -> Duration {
        self.mq_iterator_adapter.init_iterator_total_elapsed()
    }
}

impl MessagesReaderV2 {
    pub fn refill_buffers_upto_offsets(
        &mut self,
        processed_upto: &mut ProcessedUptoInfoStuffV2,
    ) -> Result<()> {
        // holds the max LT_HASH of a new created messages to current shard
        // it needs to define the read range for new messages when we get next message group
        let max_new_message_key_to_current_shard = QueueKey::MIN;

        // when refill messages buffer on init or resume
        // we should check externals expiration
        // against previous block chain time

        // TODO: But this will not work when there are uprocessed
        //      externals in messages buffer from blocks before previous.
        //      We need to redesing how to store exhaustive processed_upto
        let prev_chain_time = 0;

        tracing::debug!(target: tracing_targets::COLLATOR,
            //prev_processed_offset,
            // prev_chain_time,
            "start: refill messages buffer and skip groups upto",
        );

        // while self.state.msgs_buffer.message_groups_offset() < prev_processed_offset {
        loop {
            let msg_group = self.get_next_message_group(
                GetNextMessageGroupContext {
                    next_chain_time: prev_chain_time,
                    max_new_message_key_to_current_shard,
                    mode: GetNextMessageGroupMode::Refill,
                },
                processed_upto,
            )?;
            if msg_group.is_none() {
                // on restart from a new genesis we will not be able to refill buffer with externals
                // so we stop refilling when there is no more groups in buffer
                break;
            }
        }

        // next time we should read next message group like we did not make refill before
        // so we need to reset flags that control from where to read messages
        self.reset_read_state();

        tracing::debug!(target: tracing_targets::COLLATOR,
            // prev_processed_offset,
            // prev_chain_time,
            // actual_offset = self.state.msgs_buffer.message_groups_offset(),
            "finished: refill messages buffer and skip groups upto",
        );

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    pub fn get_next_message_group(
        &mut self,
        cx: GetNextMessageGroupContext,
        processed_upto: &mut ProcessedUptoInfoStuffV2,
    ) -> Result<Option<MessageGroupV2>> {
        todo!()
    }
}

pub(super) struct NewMessagesState<V: InternalMessageValue> {
    current_shard: ShardIdent,
    queue_diff_with_messages: QueueDiffWithMessages<V>,
    messages_for_current_shard: BinaryHeap<Reverse<MessageExt<V>>>,
}

impl<V: InternalMessageValue> NewMessagesState<V> {
    pub fn new(current_shard: ShardIdent) -> Self {
        Self {
            current_shard,
            queue_diff_with_messages: QueueDiffWithMessages {
                messages: Default::default(),
                processed_upto: Default::default(),
            },
            messages_for_current_shard: Default::default(),
        }
    }

    pub fn add_message(&mut self, message: Arc<V>) {
        self.queue_diff_with_messages
            .messages
            .insert(message.key(), message.clone());
        if self.current_shard.contains_address(message.destination()) {
            let message_with_source = MessageExt::new(self.current_shard, message);
            self.messages_for_current_shard
                .push(Reverse(message_with_source));
        };
    }

    pub fn add_messages(&mut self, messages: impl IntoIterator<Item = Arc<V>>) {
        for message in messages {
            self.add_message(message);
        }
    }
}

type Lt = u64;
type BlockSeqno = u32;

pub(super) struct ReaderState {
    pub externals: ExternalsReaderState,
    pub internals: InternalsReaderState,
}

impl ReaderState {
    pub fn new(processed_upto: &ProcessedUptoInfoStuffV2) -> Self {
        Self {
            externals: ExternalsReaderState {
                curr_processed_offset: 0,
                processed_to: processed_upto.externals.processed_to.into(),
                ranges: processed_upto
                    .externals
                    .ranges
                    .iter()
                    .map(|(k, v)| (*k, v.into()))
                    .collect(),
            },
            internals: InternalsReaderState {
                partitions: processed_upto
                    .internals
                    .partitions
                    .iter()
                    .map(|(k, v)| (*k, v.into()))
                    .collect(),
            },
        }
    }

    fn update_last_processed_offsets(&mut self) {
        if let Some(mut occupied) = self.externals.ranges.last_entry() {
            let last_ext_range = occupied.get_mut();
            last_ext_range.processed_offset = self.externals.curr_processed_offset;
        }
        for (_, par) in self.internals.partitions.iter_mut() {
            if let Some(mut occupied) = par.ranges.last_entry() {
                let last_int_range = occupied.get_mut();
                last_int_range.processed_offset = par.curr_processed_offset;
            }
        }
    }

    pub fn get_updated_processed_upto(&mut self) -> ProcessedUptoInfoStuffV2 {
        // first update processed offset in last ranges
        self.update_last_processed_offsets();

        // build processed upto info stuff
        ProcessedUptoInfoStuffV2 {
            externals: ExternalsProcessedUptoStuff {
                processed_to: self.externals.processed_to.into(),
                ranges: self
                    .externals
                    .ranges
                    .iter()
                    .map(|(k, v)| (*k, v.into()))
                    .collect(),
            },
            internals: InternalsProcessedUptoStuffV2 {
                partitions: self
                    .internals
                    .partitions
                    .iter()
                    .map(|(k, v)| (*k, v.into()))
                    .collect(),
            },
        }
    }

    pub fn count_pending_messages_in_buffers(&self) -> usize {
        let ext_count = self
            .externals
            .ranges
            .values()
            .fold(0, |acc, range| acc + range.buffer.msgs_count());
        let int_count = self.internals.partitions.values().fold(0, |p_acc, p| {
            p_acc
                + p.buffer.msgs_count()
                + p.ranges
                    .values()
                    .fold(0, |r_acc, r| r_acc + r.buffer.msgs_count())
        });
        ext_count + int_count
    }
}

#[derive(Default)]
pub(super) struct ExternalsReaderState {
    /// We fully read each externals range
    /// because we unable to get remaning messages info
    /// in any other way.
    /// We need this for not to get messages for account `A` from range `2`
    /// when we still have messages for account `A` in range `1`.
    pub ranges: BTreeMap<BlockSeqno, ExternalsRangeReaderState>,

    /// When all messages from the range (and from all its prev ranges)
    /// are processed we can update `processed_to` from this range.
    pub processed_to: ExternalKey,

    /// Actual current processed offset
    /// during the messages reading.
    pub curr_processed_offset: u16,
}

#[derive(Default)]
pub(super) struct ExternalsRangeReaderState {
    /// Buffer to store external messages
    /// before collect them to the next execution group
    pub buffer: MessagesBufferV2,

    pub from: ExternalKey,
    pub to: ExternalKey,

    /// Chain time of the block during whose collation the range was read
    pub chain_time: u64,

    /// How many times external messages were collected from the ranges buffers.
    /// Every range contains offset that was reached when range was the last.
    /// So the current last range contains the actual offset.
    pub processed_offset: u16,
}

impl From<&ExternalsRangeInfo> for ExternalsRangeReaderState {
    fn from(value: &ExternalsRangeInfo) -> Self {
        Self {
            buffer: Default::default(),
            from: value.from.into(),
            to: value.to.into(),
            chain_time: value.ct,
            processed_offset: value.processed_offset,
        }
    }
}
impl From<&ExternalsRangeReaderState> for ExternalsRangeInfo {
    fn from(value: &ExternalsRangeReaderState) -> Self {
        Self {
            from: value.from.into(),
            to: value.to.into(),
            ct: value.chain_time,
            processed_offset: value.processed_offset,
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ExternalKey {
    pub anchor_id: MempoolAnchorId,
    pub msg_offset: u64,
}

impl From<(MempoolAnchorId, u64)> for ExternalKey {
    fn from(value: (MempoolAnchorId, u64)) -> Self {
        Self {
            anchor_id: value.0,
            msg_offset: value.1,
        }
    }
}
impl From<ExternalKey> for (MempoolAnchorId, u64) {
    fn from(value: ExternalKey) -> Self {
        (value.anchor_id, value.msg_offset)
    }
}

#[derive(Default)]
pub(super) struct InternalsReaderState {
    pub partitions: BTreeMap<u8, PartitionReaderState>,
}

#[derive(Default)]
pub(super) struct PartitionReaderState {
    /// Buffer to store messages from partition
    /// before collect them to the next execution group
    pub buffer: MessagesBufferV2,

    pub ranges: BTreeMap<BlockSeqno, InternalsRangeReaderState>,
    pub processed_to: BTreeMap<ShardIdent, QueueKey>,

    /// Actual current processed offset
    /// during the messages reading.
    pub curr_processed_offset: u16,
}

impl From<&PartitionProcessedUptoStuff> for PartitionReaderState {
    fn from(value: &PartitionProcessedUptoStuff) -> Self {
        Self {
            buffer: Default::default(),
            curr_processed_offset: 0,
            processed_to: value.processed_to.clone(),
            ranges: value.ranges.iter().map(|(k, v)| (*k, v.into())).collect(),
        }
    }
}
impl From<&PartitionReaderState> for PartitionProcessedUptoStuff {
    fn from(value: &PartitionReaderState) -> Self {
        Self {
            processed_to: value.processed_to.clone(),
            ranges: value.ranges.iter().map(|(k, v)| (*k, v.into())).collect(),
        }
    }
}

#[derive(Default)]
pub(super) struct InternalsRangeReaderState {
    /// Buffer to store messages from the next iterator
    /// for accounts that have messages in the previous iterator
    /// until all messages from previous iterator are not read
    pub buffer: MessagesBufferV2,

    pub shards: BTreeMap<ShardIdent, ShardReaderState>,

    /// How many times internal messages were collected from the ranges buffers.
    /// Every range contains offset that was reached when range was the last.
    /// So the current last range contains the actual offset.
    pub processed_offset: u16,

    pub remaning_msgs_stats: FastHashMap<HashBytes, usize>,
}

impl From<&InternalsRangeStuff> for InternalsRangeReaderState {
    fn from(value: &InternalsRangeStuff) -> Self {
        Self {
            buffer: Default::default(),
            remaning_msgs_stats: Default::default(),
            processed_offset: value.processed_offset,
            shards: value.shards.iter().map(|(k, v)| (*k, v.into())).collect(),
        }
    }
}
impl From<&InternalsRangeReaderState> for InternalsRangeStuff {
    fn from(value: &InternalsRangeReaderState) -> Self {
        Self {
            processed_offset: value.processed_offset,
            shards: value.shards.iter().map(|(k, v)| (*k, v.into())).collect(),
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub(super) struct ShardReaderState {
    pub from: Lt,
    pub to: Lt,
    pub current_position: QueueKey,
}

impl From<&ShardRangeInfo> for ShardReaderState {
    fn from(value: &ShardRangeInfo) -> Self {
        Self {
            from: value.from,
            to: value.to,
            current_position: QueueKey::min_for_lt(value.from),
        }
    }
}
impl From<&ShardReaderState> for ShardRangeInfo {
    fn from(value: &ShardReaderState) -> Self {
        Self {
            from: value.from,
            to: value.to,
        }
    }
}
