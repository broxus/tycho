use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use everscale_types::models::{MsgInfo, ShardIdent};
use tycho_block_util::queue::QueueKey;

use super::super::messages_buffer::{MessagesBufferLimits, MessagesBufferV2};
use super::super::types::{Dequeued, ParsedMessage};
use super::{InternalsRangeReaderState, PartitionReaderState, ShardReaderState};
use crate::internal_queue::iterator::{IterItem, QueueIterator};
use crate::internal_queue::types::{EnqueuedMessage, QueueShardRange};
use crate::queue_adapter::MessageQueueAdapter;
use crate::tracing_targets;
use crate::types::processed_upto::{BlockSeqno, Lt, PartitionId};

//=========
// INTERNALS READER
//=========
pub(super) struct InternalsParitionReader {
    partition_id: PartitionId,
    for_shard_id: ShardIdent,
    block_seqno: BlockSeqno,

    pub(super) messages_buffer_limits: MessagesBufferLimits,

    /// mc state gen lt
    mc_state_gen_lt: Lt,
    /// prev shard state gen lt
    prev_state_gen_lt: Lt,
    /// end lt list from top shards of mc block
    mc_top_shards_end_lts: Vec<(ShardIdent, Lt)>,

    mq_adapter: Arc<dyn MessageQueueAdapter<EnqueuedMessage>>,

    pub(super) reader_state: PartitionReaderState,
    range_readers: BTreeMap<BlockSeqno, InternalsRangeReader>,

    pub(super) all_ranges_fully_read: bool,
}

pub(super) struct InternalsParitionReaderContext {
    pub partition_id: PartitionId,
    pub for_shard_id: ShardIdent,
    pub block_seqno: BlockSeqno,
    pub messages_buffer_limits: MessagesBufferLimits,
    pub mc_state_gen_lt: Lt,
    pub prev_state_gen_lt: Lt,
    pub mc_top_shards_end_lts: Vec<(ShardIdent, Lt)>,
    pub reader_state: PartitionReaderState,
}

impl InternalsParitionReader {
    pub fn new(
        cx: InternalsParitionReaderContext,
        mq_adapter: Arc<dyn MessageQueueAdapter<EnqueuedMessage>>,
    ) -> Self {
        let mut reader = Self {
            partition_id: cx.partition_id,
            for_shard_id: cx.for_shard_id,
            block_seqno: cx.block_seqno,
            messages_buffer_limits: cx.messages_buffer_limits,
            mc_state_gen_lt: cx.mc_state_gen_lt,
            prev_state_gen_lt: cx.prev_state_gen_lt,
            mc_top_shards_end_lts: cx.mc_top_shards_end_lts,
            mq_adapter,
            reader_state: cx.reader_state,
            range_readers: Default::default(),
            all_ranges_fully_read: false,
        };

        reader.create_existing_range_readers();

        reader
    }

    pub fn finalize(mut self) -> PartitionReaderState {
        // collect range reader states
        let last_seqno = self
            .range_readers
            .last_key_value()
            .map(|(k, _)| *k)
            .unwrap_or_default();
        for (seqno, mut range_reader) in self.range_readers {
            // update offset in the last range reader state
            if seqno == last_seqno {
                range_reader.reader_state.processed_offset =
                    self.reader_state.curr_processed_offset;
            }

            self.reader_state
                .ranges
                .insert(seqno, range_reader.reader_state);
        }

        // return updated partition reader state
        self.reader_state
    }

    pub fn has_non_zero_processed_offset(&self) -> bool {
        self.range_readers
            .iter()
            .any(|(_, r)| r.reader_state.processed_offset > 0)
    }

    pub fn has_messages_in_buffers(&self) -> bool {
        if self.reader_state.buffer.msgs_count() > 0 {
            true
        } else {
            self.range_readers
                .iter()
                .any(|(_, r)| r.reader_state.buffer.msgs_count() > 0)
        }
    }

    pub fn set_processed_to_current_position(&mut self) -> Result<()> {
        let (_, last_range_reader) = self.range_readers.last_key_value().context(
            "partition reader should have at least one range reader after reading into buffer",
        )?;
        self.reader_state.processed_to = last_range_reader
            .reader_state
            .shards
            .iter()
            .map(|(k, v)| (*k, v.current_position))
            .collect();
        Ok(())
    }
}

impl InternalsParitionReader {
    fn create_existing_range_readers(&mut self) {
        while let Some((seqno, range_reader_state)) = self.reader_state.ranges.pop_first() {
            let reader = self.create_existing_internals_range_reader(range_reader_state, seqno);
            self.range_readers.insert(seqno, reader);
        }
    }

    #[tracing::instrument(skip_all)]
    fn create_existing_internals_range_reader(
        &self,
        range_reader_state: InternalsRangeReaderState,
        seqno: BlockSeqno,
    ) -> InternalsRangeReader {
        let mut fully_read = true;
        for shard_reader_state in range_reader_state.shards.values() {
            let shard_range_to = QueueKey::max_for_lt(shard_reader_state.to);
            if shard_reader_state.current_position != shard_range_to {
                fully_read = false;
            }
        }

        let reader = InternalsRangeReader {
            partition_id: self.partition_id,
            for_shard_id: self.for_shard_id,
            seqno,
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
            shard_reader_states = ?reader.reader_state.shards,
            "created existing range reader",
        );

        reader
    }

    #[tracing::instrument(skip_all)]
    fn create_next_internals_range_reader(
        &self,
        last_shard_reader_states_opt: Option<BTreeMap<ShardIdent, ShardReaderState>>,
        seqno: BlockSeqno,
    ) -> InternalsRangeReader {
        let mut shard_reader_states = BTreeMap::new();

        let all_end_lts = [(ShardIdent::MASTERCHAIN, self.mc_state_gen_lt)]
            .into_iter()
            .chain(self.mc_top_shards_end_lts.iter().cloned());

        let mut fully_read = true;
        for (shard_id, end_lt) in all_end_lts {
            let last_to_lt_opt = last_shard_reader_states_opt
                .as_ref()
                .and_then(|s_r| s_r.get(&shard_id).map(|s| s.to));
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
        }

        let reader = InternalsRangeReader {
            partition_id: self.partition_id,
            for_shard_id: self.for_shard_id,
            seqno,
            reader_state: InternalsRangeReaderState {
                buffer: Default::default(),
                shards: shard_reader_states,
                processed_offset: 0,
                remaning_msgs_stats: Default::default(),
            },
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
            shard_reader_states = ?reader.reader_state.shards,
            "created next range reader",
        );

        reader
    }

    pub fn read_into_buffers(&mut self) -> Result<()> {
        // skip if all ranges fully read
        if self.all_ranges_fully_read {
            return Ok(());
        }

        loop {
            // take next not fully read range and continue reading
            let mut last_seqno = 0;
            let mut seqno = self
                .range_readers
                .first_key_value()
                .map(|(k, _)| *k)
                .unwrap_or_default();
            let mut all_ranges_fully_read = true;
            while let Some(range_reader) = self.range_readers.get_mut(&seqno) {
                // remember last existing range
                last_seqno = seqno;

                // skip fully read ranges
                if range_reader.fully_read {
                    seqno += 1;
                    continue;
                }

                // init reader if not initialized
                if !range_reader.initialized {
                    range_reader.init()?;
                }

                // read into buffer from the range
                let Some(iterator) = range_reader.iterator_opt.as_mut() else {
                    bail!("not fully read range should have iterator");
                };

                loop {
                    // stop reading if buffer is full
                    // or we can already fill required slots
                    if Self::check_message_buffer_filled(
                        self.partition_id,
                        seqno,
                        &self.reader_state.buffer,
                        &self.messages_buffer_limits,
                    ) {
                        // update current position from iterator
                        let iterator_curent_positions = iterator.current_position();
                        for (shard_id, curr_pos) in iterator_curent_positions {
                            let Some(shard_reader_state) =
                                range_reader.reader_state.shards.get_mut(&shard_id)
                            else {
                                bail!("shard reader state for existing iterator should exist")
                            };
                            shard_reader_state.current_position = curr_pos;
                        }

                        break;
                    }

                    match iterator.next(false)? {
                        Some(int_msg) => {
                            let msg = Box::new(ParsedMessage {
                                info: MsgInfo::Int(int_msg.item.message.info.clone()),
                                dst_in_current_shard: true,
                                cell: int_msg.item.message.cell.clone(),
                                special_origin: None,
                                dequeued: Some(Dequeued {
                                    same_shard: int_msg.item.source == self.for_shard_id,
                                }),
                            });

                            // TODO: msgs-v3: should not add message to partition buffer
                            // if previous range was not fully read and contains remaning
                            // messages for destination account

                            self.reader_state.buffer.add_message(msg);
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

                            break;
                        }
                    }
                }

                // if range was not fully read then buffer is full
                // and we should continue to read current range
                // next time the method is called
                if !range_reader.fully_read {
                    all_ranges_fully_read = false;
                }

                // try to get next range
                seqno += 1;
            }

            if all_ranges_fully_read {
                // if all ranges fully read try create next one
                if last_seqno != self.block_seqno {
                    let last_shard_reader_states_opt = self
                        .reader_state
                        .ranges
                        .get(&last_seqno)
                        .map(|r| r.shards.clone());
                    let reader = self.create_next_internals_range_reader(
                        last_shard_reader_states_opt,
                        self.block_seqno,
                    );
                    self.range_readers.insert(self.block_seqno, reader);
                    self.all_ranges_fully_read = false;
                } else {
                    // if cannot create next one then store flag and exit
                    self.all_ranges_fully_read = true;
                    break;
                }
            } else {
                // exist when we stopped reading and range was not fully read
                break;
            }
        }

        Ok(())
    }

    fn check_message_buffer_filled(
        par_id: PartitionId,
        seqno: BlockSeqno,
        messages_buffer: &MessagesBufferV2,
        messages_buffer_limits: &MessagesBufferLimits,
    ) -> bool {
        if messages_buffer.msgs_count() >= messages_buffer_limits.max_count {
            tracing::debug!(target: tracing_targets::COLLATOR,
                "message_groups buffer filled on {}/{}, stop reading partition {} range # {}",
                messages_buffer.msgs_count(), messages_buffer_limits.max_count, par_id, seqno,
            );
            return true;
        }

        // TODO: msgs-v3: check if we can already fill required slots

        false
    }

    pub fn check_has_pending_internals_in_iterators(&mut self) -> Result<bool> {
        let mut next_range_reader_opt = None;
        loop {
            let mut last_seqno = 0;
            let mut seqno = self
                .range_readers
                .first_key_value()
                .map(|(k, _)| *k)
                .unwrap_or_default();
            while let Some(range_reader) = self.range_readers.get_mut(&seqno).or({
                // return temporary created next range reader when it not exist in the reader state
                if seqno == self.block_seqno {
                    next_range_reader_opt.as_mut()
                } else {
                    None
                }
            }) {
                // remember last existing range
                last_seqno = seqno;

                // skip fully read ranges
                if range_reader.fully_read {
                    seqno += 1;
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
                        if seqno != self.block_seqno {
                            // But do not need to do this for next range reader
                            // because we do not store it in the state
                            range_reader.init()?;
                        }

                        return Ok(true);
                    }
                    None => {
                        range_reader.fully_read = true;

                        // set current position to the end of the range
                        for (_, shard_reader_state) in range_reader.reader_state.shards.iter_mut() {
                            shard_reader_state.current_position =
                                QueueKey::max_for_lt(shard_reader_state.to);
                        }

                        // try to get next range
                        seqno += 1;
                        continue;
                    }
                }
            }

            // if no pending internals in all existing ranges
            // then create next range reader
            if last_seqno != self.block_seqno {
                let last_shard_reader_states_opt = self
                    .reader_state
                    .ranges
                    .get(&last_seqno)
                    .map(|r| r.shards.clone());
                let reader = self.create_next_internals_range_reader(
                    last_shard_reader_states_opt,
                    self.block_seqno,
                );
                next_range_reader_opt = Some(reader);
            } else {
                // when no pending internals in all existing ranges
                // and in the next range as well
                // then exist loop
                break;
            }
        }

        Ok(false)
    }
}

struct InternalsRangeReader {
    partition_id: PartitionId,
    for_shard_id: ShardIdent,
    seqno: BlockSeqno,
    reader_state: InternalsRangeReaderState,
    fully_read: bool,
    mq_adapter: Arc<dyn MessageQueueAdapter<EnqueuedMessage>>,
    iterator_opt: Option<Box<dyn QueueIterator<EnqueuedMessage>>>,
    initialized: bool,
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
                self.partition_id.into(),
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

    fn next(&mut self) -> Result<Option<IterItem<EnqueuedMessage>>> {
        if !self.initialized {
            bail!("should run `init()` first")
        }

        if self.fully_read {
            return Ok(None);
        }

        let Some(iterator) = self.iterator_opt.as_mut() else {
            bail!("not fully read range should have iterator");
        };

        iterator.next(false)
    }
}
