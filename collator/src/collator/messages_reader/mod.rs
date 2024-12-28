use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use everscale_types::cell::HashBytes;
use everscale_types::models::{MsgsExecutionParams, ShardIdent};
use tycho_block_util::queue::QueueKey;

use super::messages_buffer::{FastIndexSet, MessageGroup, MessagesBufferLimits};
use super::types::AnchorsCache;
use crate::internal_queue::state::state_iterator::MessageExt;
use crate::internal_queue::types::{EnqueuedMessage, InternalMessageValue, QueueDiffWithMessages};
use crate::queue_adapter::MessageQueueAdapter;
use crate::tracing_targets;
use crate::types::processed_upto::{BlockSeqno, Lt, PartitionId};

mod externals_reader;
mod internals_reader;
mod reader_state;

use externals_reader::*;
use internals_reader::*;
pub(super) use reader_state::*;

#[derive(Debug, Default)]
pub(super) struct MessagesReaderMetrics {
    /// sum total time of initializations of internal messages iterators
    pub init_iterator_total_elapsed: Duration,

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

pub(super) struct FinalizedMessagesReader {
    pub has_unprocessed_messages: bool,
    pub reader_state: ReaderState,
    pub anchors_cache: AnchorsCache,
    pub queue_diff_with_msgs: QueueDiffWithMessages<EnqueuedMessage>,
}

pub(super) enum GetNextMessageGroupMode {
    Continue,
    Refill,
}

#[derive(PartialEq, Eq)]
enum MessagesReaderStage {
    ExistingMessages,
    NewMessages,
}

pub(super) struct MessagesReader {
    for_shard_id: ShardIdent,
    block_seqno: BlockSeqno,

    msgs_exec_params: MsgsExecutionParams,

    metrics: MessagesReaderMetrics,

    new_messages: NewMessagesState<EnqueuedMessage>,

    externals_reader: ExternalsReader,
    internals_partition_readers: BTreeMap<PartitionId, InternalsParitionReader>,

    readers_stages: BTreeMap<PartitionId, MessagesReaderStage>,
}

#[derive(Default)]
pub(super) struct MessagesReaderContext {
    pub for_shard_id: ShardIdent,
    pub block_seqno: BlockSeqno,
    pub next_chain_time: u64,
    pub msgs_exec_params: MsgsExecutionParams,
    pub mc_state_gen_lt: Lt,
    pub prev_state_gen_lt: Lt,
    pub mc_top_shards_end_lts: Vec<(ShardIdent, Lt)>,
    pub reader_state: ReaderState,
    pub anchors_cache: AnchorsCache,
}

impl MessagesReader {
    pub fn new(
        cx: MessagesReaderContext,
        mq_adapter: Arc<dyn MessageQueueAdapter<EnqueuedMessage>>,
    ) -> Self {
        metrics::gauge!("tycho_do_collate_msgs_exec_params_buffer_limit")
            .set(cx.msgs_exec_params.buffer_limit as f64);
        metrics::gauge!("tycho_do_collate_msgs_exec_params_group_limit")
            .set(cx.msgs_exec_params.group_limit as f64);
        metrics::gauge!("tycho_do_collate_msgs_exec_params_group_vert_size")
            .set(cx.msgs_exec_params.group_vert_size as f64);

        // group limits by msgs kinds
        let msgs_buffer_max_count = cx.msgs_exec_params.buffer_limit as usize;
        let group_vert_size = cx.msgs_exec_params.group_vert_size as usize;
        let group_limit = cx.msgs_exec_params.group_limit as usize;
        // TODO: msgs-v3: should move the fraction value to params in blockchain config
        // internals: normal partition 0: 70% of `group_limit`, but min 1
        let par_0_slots_count = group_limit.saturating_mul(70).saturating_div(100).max(1);
        // internals: low-priority partition 1: + 10%, but min 1
        let par_1_slots_count = group_limit.saturating_mul(80).saturating_div(100).max(2);
        // externals: + 20%, but min 1
        let ext_slots_count = group_limit.max(3);

        // create externals reader
        let externals_reader = ExternalsReader::new(
            cx.for_shard_id,
            cx.block_seqno,
            cx.next_chain_time,
            MessagesBufferLimits {
                max_count: msgs_buffer_max_count,
                slots_count: ext_slots_count,
                slot_vert_size: group_vert_size + 1,
            },
            cx.anchors_cache,
            cx.reader_state.externals,
        );

        let mut res = Self {
            for_shard_id: cx.for_shard_id,
            block_seqno: cx.block_seqno,

            msgs_exec_params: cx.msgs_exec_params,

            metrics: Default::default(),

            new_messages: NewMessagesState::new(cx.for_shard_id),

            externals_reader,
            internals_partition_readers: Default::default(),

            readers_stages: Default::default(),
        };

        // create internals readers by partitions
        let mut partition_reader_states = cx.reader_state.internals.partitions;
        // normal partition 0
        let par_reader_state = partition_reader_states.remove(&0).unwrap_or_default();
        let par_reader = InternalsParitionReader::new(
            InternalsParitionReaderContext {
                partition_id: 0,
                for_shard_id: cx.for_shard_id,
                block_seqno: cx.block_seqno,
                messages_buffer_limits: MessagesBufferLimits {
                    max_count: msgs_buffer_max_count,
                    slots_count: par_0_slots_count,
                    slot_vert_size: group_vert_size,
                },
                mc_state_gen_lt: cx.mc_state_gen_lt,
                prev_state_gen_lt: cx.prev_state_gen_lt,
                mc_top_shards_end_lts: cx.mc_top_shards_end_lts.clone(),
                reader_state: par_reader_state,
            },
            mq_adapter.clone(),
        );
        res.internals_partition_readers.insert(0, par_reader);
        res.readers_stages
            .insert(0, MessagesReaderStage::ExistingMessages);

        // low-priority partition 1
        let par_reader_state = partition_reader_states.remove(&1).unwrap_or_default();
        let par_reader = InternalsParitionReader::new(
            InternalsParitionReaderContext {
                partition_id: 1,
                for_shard_id: cx.for_shard_id,
                block_seqno: cx.block_seqno,
                messages_buffer_limits: MessagesBufferLimits {
                    max_count: msgs_buffer_max_count,
                    slots_count: par_1_slots_count,
                    slot_vert_size: group_vert_size,
                },
                mc_state_gen_lt: cx.mc_state_gen_lt,
                prev_state_gen_lt: cx.prev_state_gen_lt,
                mc_top_shards_end_lts: cx.mc_top_shards_end_lts,
                reader_state: par_reader_state,
            },
            mq_adapter,
        );
        res.internals_partition_readers.insert(1, par_reader);
        res.readers_stages
            .insert(1, MessagesReaderStage::ExistingMessages);

        res
    }

    pub fn reset_read_state(&mut self) {
        self.metrics = Default::default();
    }

    pub fn check_has_pending_internals_in_iterators(&mut self) -> Result<bool> {
        for (_, par_reader) in self.internals_partition_readers.iter_mut() {
            if par_reader.check_has_pending_internals_in_iterators()? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn finalize(self) -> Result<FinalizedMessagesReader> {
        let mut has_unprocessed_messages = self.has_messages_in_buffers()
            || self.has_pending_new_messages()
            || self.has_pending_externals_in_cache();

        // collect externals reader state
        let FinalizedExternalsReader {
            externals_reader_state,
            anchors_cache,
        } = self.externals_reader.finalize();
        let mut reader_state = ReaderState {
            externals: externals_reader_state,
            internals: Default::default(),
        };

        // collect internals partition readers states
        for (par_id, mut par_reader) in self.internals_partition_readers {
            // check pending internals in iterators
            if !has_unprocessed_messages {
                has_unprocessed_messages = par_reader.check_has_pending_internals_in_iterators()?;
            }

            let par_reader_state = par_reader.finalize();
            reader_state
                .internals
                .partitions
                .insert(par_id, par_reader_state);
        }

        // TODO: msgs-v3: set processed_to in queue diff

        Ok(FinalizedMessagesReader {
            has_unprocessed_messages,
            reader_state,
            anchors_cache,
            queue_diff_with_msgs: self.new_messages.queue_diff_with_msgs,
        })
    }

    pub fn last_read_to_anchor_chain_time(&self) -> Option<u64> {
        self.externals_reader
            .reader_state
            .last_read_to_anchor_chain_time
    }

    pub fn metrics(&self) -> &MessagesReaderMetrics {
        &self.metrics
    }

    pub fn new_messages_mut(&mut self) -> &mut NewMessagesState<EnqueuedMessage> {
        &mut self.new_messages
    }

    pub fn has_messages_in_buffers(&self) -> bool {
        self.has_internals_in_buffers() || self.has_externals_in_buffers()
    }

    pub fn has_internals_in_buffers(&self) -> bool {
        self.internals_partition_readers
            .iter()
            .any(|(_, v)| v.has_messages_in_buffers())
    }

    pub fn has_not_fully_read_internals_ranges(&self) -> bool {
        self.internals_partition_readers
            .iter()
            .any(|(_, v)| !v.all_ranges_fully_read)
    }

    pub fn has_pending_new_messages(&self) -> bool {
        self.new_messages.has_pending_messages()
    }

    pub fn has_externals_in_buffers(&self) -> bool {
        self.externals_reader.has_messages_in_buffers()
    }

    pub fn has_pending_externals_in_cache(&self) -> bool {
        self.externals_reader.has_pending_externals()
    }
}

impl MessagesReader {
    pub fn check_has_non_zero_processed_offset(&self) -> bool {
        let check_externals = self.externals_reader.has_non_zero_processed_offset();
        if check_externals {
            return check_externals;
        }

        let check_internals = self
            .internals_partition_readers
            .iter()
            .any(|(_, par_reader)| par_reader.has_non_zero_processed_offset());

        check_internals
    }

    pub fn check_need_refill(&self) -> bool {
        if self.has_messages_in_buffers() {
            return false;
        }

        // check if hash non zero processed offset
        self.check_has_non_zero_processed_offset()
    }

    pub fn refill_buffers_upto_offsets(&mut self) -> Result<()> {
        // holds the max LT_HASH of a new created messages to current shard
        // it needs to define the read range for new messages when we get next message group
        let max_new_message_key_to_current_shard = QueueKey::MIN;

        // when refill messages buffer on init or resume
        // we should check externals expiration
        // against previous block chain time

        // TODO: msgs-v3: But this will not work when there are uprocessed
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
            let msg_group = self.get_next_message_group(GetNextMessageGroupMode::Refill)?;
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
        read_mode: GetNextMessageGroupMode,
    ) -> Result<Option<MessageGroup>> {
        let mut msg_group = MessageGroup::default();

        // TODO: msgs-v3: fill metrics during reading

        // TODO: msgs-v3: read internals from partitions and externals in parallel

        // read internals
        for (par_id, par_reader_stage) in self.readers_stages.iter_mut() {
            match par_reader_stage {
                MessagesReaderStage::ExistingMessages => {
                    let par_reader = self
                        .internals_partition_readers
                        .get_mut(par_id)
                        .context("reader for partition should exist")?;
                    par_reader.read_into_buffers()?;
                }
                MessagesReaderStage::NewMessages => {
                    todo!()
                }
            }
        }

        // collect internals after reading
        let mut unused_buffer_accounts_by_partitions =
            BTreeMap::<(PartitionId, BlockSeqno), FastIndexSet<HashBytes>>::default();
        let mut par_readers = BTreeMap::<PartitionId, InternalsParitionReader>::default();
        for (par_id, par_reader_stage) in self.readers_stages.iter_mut() {
            match par_reader_stage {
                MessagesReaderStage::ExistingMessages => {
                    // extract partition reader from state to use partition 0 buffer
                    // to check for account skip on collecting messages from partition 1
                    let mut par_reader = self
                        .internals_partition_readers
                        .remove(par_id)
                        .context("reader for partition should exist")?;

                    // try to fill messages group
                    let unused_buffer_accounts = par_reader.reader_state.buffer.fill_message_group(
                        &mut msg_group,
                        par_reader.messages_buffer_limits.slots_count,
                        par_reader.messages_buffer_limits.slot_vert_size,
                        unused_buffer_accounts_by_partitions.remove(&(*par_id, 0)),
                        |account_id| {
                            for prev_reader in par_readers.values() {
                                if prev_reader
                                    .reader_state
                                    .buffer
                                    .account_messages_count(account_id)
                                    > 0
                                {
                                    return true;
                                }
                            }
                            false
                        },
                    );
                    unused_buffer_accounts_by_partitions
                        .insert((*par_id, 0), unused_buffer_accounts);

                    // TODO: msgs-v3: read from ranges buffers as well

                    // TODO: msgs-v3: should drop offset when all ranges read
                    par_reader.reader_state.curr_processed_offset += 1;

                    // check if should switch on reading of new messages
                    if par_reader.all_ranges_fully_read && !par_reader.has_messages_in_buffers() {
                        *par_reader_stage = MessagesReaderStage::NewMessages;

                        // we can update processed_to when we collected all messages from the partition
                        par_reader.set_processed_to_current_position()?;
                    }

                    par_readers.insert(*par_id, par_reader);
                }
                MessagesReaderStage::NewMessages => {
                    todo!()
                }
            }
        }
        // return partion readers to state
        self.internals_partition_readers = par_readers;

        // read externals
        self.externals_reader.read_into_buffers();

        // collect externals
        let mut range_readers = BTreeMap::<BlockSeqno, ExternalsRangeReader>::default();
        // extract range readers from state to use previous readers buffers
        // to check for account skip on collecting messages from current one
        while let Some((seqno, mut reader)) = self.externals_reader.pop_first_range_reader() {
            reader.reader_state.buffer.fill_message_group(
                &mut msg_group,
                self.externals_reader.messages_buffer_limits.slots_count,
                self.externals_reader.messages_buffer_limits.slot_vert_size,
                None,
                |account_id| {
                    for prev_reader in range_readers.values() {
                        if prev_reader
                            .reader_state
                            .buffer
                            .account_messages_count(account_id)
                            > 0
                        {
                            return true;
                        }
                    }
                    false
                },
            );
            range_readers.insert(seqno, reader);
        }
        // return range readers to state
        self.externals_reader.set_range_readers(range_readers);

        self.externals_reader.reader_state.curr_processed_offset += 1;

        // check if all externals collected
        if self.externals_reader.all_ranges_fully_read
            && !self.externals_reader.has_messages_in_buffers()
        {
            // we can update processed_to when we collected all externals
            self.externals_reader.set_processed_to_current_position()?;
        }

        // if message group was not fully filled after externals
        // then try to append remaning internals
        if msg_group.check_is_filled(
            self.msgs_exec_params.group_limit as _,
            self.msgs_exec_params.group_vert_size as _,
        ) {
            // TODO: msgs-v3: fill messages group with internals again
        }

        if msg_group.len() == 0 {
            // TODO: msgs-v3: and no pending new messages
            Ok(Some(msg_group))
        } else {
            Ok(None)
        }
    }
}

//=========
// NEW MESSAGES
//=========

pub(super) struct NewMessagesState<V: InternalMessageValue> {
    current_shard: ShardIdent,
    queue_diff_with_msgs: QueueDiffWithMessages<V>,
    messages_for_current_shard: BinaryHeap<Reverse<MessageExt<V>>>,
    max_message_key_for_current_shard: QueueKey,
}

impl<V: InternalMessageValue> NewMessagesState<V> {
    pub fn new(current_shard: ShardIdent) -> Self {
        Self {
            current_shard,
            queue_diff_with_msgs: QueueDiffWithMessages {
                messages: Default::default(),
                processed_to: Default::default(),
                partition_router: Default::default(),
            },
            messages_for_current_shard: Default::default(),
            max_message_key_for_current_shard: QueueKey::MIN,
        }
    }

    pub fn has_pending_messages(&self) -> bool {
        !self.messages_for_current_shard.is_empty()
    }

    pub fn add_message(&mut self, message: Arc<V>) {
        self.queue_diff_with_msgs
            .messages
            .insert(message.key(), message.clone());
        if self.current_shard.contains_address(message.destination()) {
            self.max_message_key_for_current_shard =
                std::cmp::max(self.max_message_key_for_current_shard, message.key());
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