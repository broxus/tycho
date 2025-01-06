use std::collections::BTreeMap;

use anyhow::{Context, Result};
use everscale_types::models::{MsgInfo, ShardIdent};

use super::super::messages_buffer::{MessagesBufferLimits, MessagesBufferV2};
use super::super::types::{AnchorsCache, ParsedMessage};
use super::{
    DebugExternalsRangeReaderState, ExternalKey, ExternalsRangeReaderState, ExternalsReaderState,
};
use crate::tracing_targets;
use crate::types::processed_upto::BlockSeqno;

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
    pub(super) messages_buffer_limits: MessagesBufferLimits,
    anchors_cache: AnchorsCache,
    pub(super) reader_state: ExternalsReaderState,
    range_readers: BTreeMap<BlockSeqno, ExternalsRangeReader>,
    pub(super) all_ranges_fully_read: bool,
}

impl ExternalsReader {
    pub fn new(
        for_shard_id: ShardIdent,
        block_seqno: BlockSeqno,
        next_chain_time: u64,
        messages_buffer_limits: MessagesBufferLimits,
        anchors_cache: AnchorsCache,
        reader_state: ExternalsReaderState,
    ) -> Self {
        let mut reader = Self {
            for_shard_id,
            block_seqno,
            next_chain_time,
            messages_buffer_limits,
            anchors_cache,
            reader_state,
            range_readers: Default::default(),
            all_ranges_fully_read: false,
        };

        reader.create_existing_range_readers();

        reader
    }

    pub fn finalize(mut self) -> FinalizedExternalsReader {
        // collect range reader states
        let mut range_readers = self.range_readers.into_iter().peekable();
        let mut max_processed_offset = 0;
        while let Some((seqno, mut range_reader)) = range_readers.next() {
            // update offset in the last range reader state
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

        // return updated externals reader state
        FinalizedExternalsReader {
            externals_reader_state: self.reader_state,
            anchors_cache: self.anchors_cache,
        }
    }

    pub fn has_non_zero_processed_offset(&self) -> bool {
        self.range_readers
            .iter()
            .any(|(_, r)| r.reader_state.processed_offset > 0)
    }

    pub fn has_messages_in_buffers(&self) -> bool {
        self.range_readers
            .iter()
            .any(|(_, v)| v.reader_state.buffer.msgs_count() > 0)
    }

    pub fn has_pending_externals(&self) -> bool {
        self.anchors_cache.has_pending_externals()
    }

    pub fn retain_only_last_range_reader(&mut self) -> Result<()> {
        let (last_seqno, last_range_reader) = self.range_readers.pop_last().context(
            "externals reader should have at least one range reader after reading into buffer",
        )?;
        self.range_readers.clear();
        self.range_readers.insert(last_seqno, last_range_reader);
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

    /// Drop current offset and offset in the last range reader state
    pub fn drop_processing_offset(&mut self) -> Result<()> {
        self.reader_state.curr_processed_offset = 0;
        let last_range_reader = self.get_last_range_reader_mut()?;
        last_range_reader.reader_state.processed_offset = 0;
        Ok(())
    }

    pub fn set_processed_to_current_position(&mut self) -> Result<()> {
        let (_, last_range_reader) = self.get_last_range_reader()?;
        self.reader_state.processed_to = last_range_reader.reader_state.current_position;
        Ok(())
    }
}

impl ExternalsReader {
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
            messages_buffer_limits: self.messages_buffer_limits,
            fully_read: range_reader_state.current_position == range_reader_state.to,
            reader_state: range_reader_state,
        };

        tracing::debug!(target: tracing_targets::COLLATOR,
            for_shard_id = %self.for_shard_id,
            seqno = reader.seqno,
            fully_read = reader.fully_read,
            reader_state = ?DebugExternalsRangeReaderState(&reader.reader_state),
            "created existing range reader",
        );

        reader
    }

    #[tracing::instrument(skip_all)]
    fn create_next_externals_range_reader(
        &self,
        last_range_reader_to_opt: Option<ExternalKey>,
    ) -> ExternalsRangeReader {
        let from = last_range_reader_to_opt.unwrap_or_default();

        let reader = ExternalsRangeReader {
            for_shard_id: self.for_shard_id,
            seqno: self.block_seqno,
            messages_buffer_limits: self.messages_buffer_limits,
            fully_read: false,
            reader_state: ExternalsRangeReaderState {
                buffer: Default::default(),
                from,
                to: from,
                current_position: from,
                chain_time: self.next_chain_time,
                processed_offset: 0,
            },
        };

        tracing::debug!(target: tracing_targets::COLLATOR,
            for_shard_id = %self.for_shard_id,
            seqno = reader.seqno,
            fully_read = reader.fully_read,
            reader_state = ?DebugExternalsRangeReaderState(&reader.reader_state),
            "created next range reader",
        );

        reader
    }

    pub fn read_into_buffers(&mut self) {
        // skip if all ranges fully read
        if self.all_ranges_fully_read {
            return;
        }

        let mut last_seqno = 0;
        let mut seqno = self
            .range_readers
            .first_key_value()
            .map(|(k, _)| *k)
            .unwrap_or_default();

        loop {
            let mut last_ext_read_res = ReadExternalsRangeResult::default();
            let mut all_ranges_fully_read = true;
            while let Some(range_reader) = self.range_readers.get_mut(&seqno) {
                // remember last existing range
                last_seqno = seqno;

                // skip fully read ranges
                if range_reader.fully_read {
                    seqno += 1;
                    continue;
                }

                // read externals
                let read_mode = if seqno == self.block_seqno {
                    ReadNextExternalsMode::ToTheEnd
                } else {
                    ReadNextExternalsMode::ToPreviuosReadTo
                };
                last_ext_read_res = range_reader.read_externals_into_buffer(
                    self.next_chain_time,
                    &mut self.anchors_cache,
                    read_mode,
                );

                // if range was not fully read then buffer is full
                // and we should continue to read current range
                // next time the method is called
                if !range_reader.fully_read {
                    all_ranges_fully_read = false;
                } else if seqno == self.block_seqno {
                    // if current range is a last one and fully read
                    // then set current position to the end of the last imported anchor
                    if let Some(last_imported_anchor) = self.anchors_cache.last_imported_anchor() {
                        range_reader.reader_state.current_position = ExternalKey {
                            anchor_id: last_imported_anchor.id,
                            msgs_offset: last_imported_anchor.all_exts_count as u64,
                        };
                        range_reader.reader_state.to = range_reader.reader_state.current_position;
                    }
                }

                // try to get next range
                seqno += 1;
            }

            // update the pending externals flag from the last range
            if last_seqno == self.block_seqno {
                self.anchors_cache
                    .set_has_pending_externals(last_ext_read_res.has_pending_externals);
                if let Some(ct) = last_ext_read_res.last_read_to_anchor_chain_time {
                    self.reader_state.last_read_to_anchor_chain_time = Some(ct);
                }
            }

            // if all ranges fully read try create next one
            if all_ranges_fully_read {
                if last_seqno < self.block_seqno {
                    let last_range_reader_state_to_opt = self
                        .range_readers
                        .get(&last_seqno)
                        .map(|r| r.reader_state.to);
                    let reader =
                        self.create_next_externals_range_reader(last_range_reader_state_to_opt);
                    self.range_readers.insert(self.block_seqno, reader);
                    self.all_ranges_fully_read = false;
                    seqno = self.block_seqno;
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
    }
}

pub(super) struct ExternalsRangeReader {
    for_shard_id: ShardIdent,
    seqno: BlockSeqno,
    pub(super) messages_buffer_limits: MessagesBufferLimits,
    pub(super) reader_state: ExternalsRangeReaderState,
    fully_read: bool,
}

impl ExternalsRangeReader {
    #[tracing::instrument(skip_all, fields(for_shard_id = %self.for_shard_id, seqno = self.seqno))]
    fn read_externals_into_buffer(
        &mut self,
        next_chain_time: u64,
        anchors_cache: &mut AnchorsCache,
        read_mode: ReadNextExternalsMode,
    ) -> ReadExternalsRangeResult {
        let labels = [("workchain", self.for_shard_id.workchain().to_string())];

        tracing::info!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
            fully_read = self.fully_read,
            "read externals",
        );

        let was_read_to = self.reader_state.current_position;
        let prev_to = self.reader_state.to;

        let mut prev_to_reached = false;

        // TODO: msgs-v3: use buffer method for check
        // check if buffer is full
        // or we can already fill required slots
        let mut buffer_filled = Self::check_message_buffer_filled(
            self.seqno,
            &self.reader_state.buffer,
            &self.messages_buffer_limits,
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
            // if read_mode == ReadNextExternalsMode::ToPreviuosReadTo && prev_to_reached {
            //     // then do not read externals
            //     self.fully_read = true;

            //     // when was reading to prev_to and reached it we consider then
            //     // we do not have pending externals in the range
            //     return Ok(ReadExternalsRangeResult {
            //         has_pending_externals: false,
            //         last_read_to_anchor_chain_time,
            //     });
            // }

            // NOTE: fully read anchor will be removed after reading futher
            // // skip and remove fully read anchor
            // if anchor_id == was_read_to.anchor_id
            //     && anchor.externals.len() == was_read_to.msgs_offset as usize
            // {
            //     assert_eq!(next_idx, 0);
            //     anchors_cache.remove(next_idx);
            //     last_anchor_removed = true;
            //     tracing::debug!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
            //         "anchor with id {} was fully read before, removed from anchors cache", anchor_id,
            //     );
            //     // try read next anchor
            //     continue;
            // }

            // skip expired anchor
            // TODO: msgs-v3: should move this param to blockchain config
            const EXTERNALS_EXPIRE_TIMEOUT: u64 = 60_000; // 1 minute

            if next_chain_time.saturating_sub(anchor.chain_time) > EXTERNALS_EXPIRE_TIMEOUT {
                let iter = anchor.iter_externals(msgs_read_offset_in_last_anchor as usize);
                let mut expired_msgs_count = 0;
                for ext_msg in iter {
                    if self.for_shard_id.contains_address(&ext_msg.info.dst) {
                        tracing::trace!(target: tracing_targets::COLLATOR,
                            anchor_id,
                            "ext_msg hash: {}, dst: {} is expired by timeout {} ms",
                            ext_msg.hash(), ext_msg.info.dst, EXTERNALS_EXPIRE_TIMEOUT,
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
                    "anchor fully skipped due to expiration, removed from anchors cache",
                );

                count_expired_anchors = count_expired_anchors.saturating_add(1);
                count_expired_messages = count_expired_messages.saturating_add(expired_msgs_count);

                // update current position
                self.reader_state.current_position = ExternalKey {
                    anchor_id,
                    msgs_offset: msgs_read_offset_in_last_anchor,
                };
                self.reader_state.to = self.reader_state.current_position;

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
                if !(buffer_filled
                    || read_mode == ReadNextExternalsMode::ToPreviuosReadTo && prev_to_reached)
                {
                    msgs_read_offset_in_last_anchor += 1;

                    // update current position
                    self.reader_state.current_position = ExternalKey {
                        anchor_id,
                        msgs_offset: msgs_read_offset_in_last_anchor,
                    };
                    self.reader_state.to = self.reader_state.current_position;

                    // check if prev_to reached
                    prev_to_reached = anchor_id > prev_to.anchor_id
                        || (anchor_id == prev_to.anchor_id
                            && msgs_read_offset_in_last_anchor == prev_to.msgs_offset);

                    if self.for_shard_id.contains_address(&ext_msg.info.dst) {
                        // add message to buffer
                        self.reader_state
                            .buffer
                            .add_message(Box::new(ParsedMessage {
                                info: MsgInfo::ExtIn(ext_msg.info.clone()),
                                dst_in_current_shard: true,
                                cell: ext_msg.cell.clone(),
                                special_origin: None,
                                block_seqno: None,
                                from_same_shard: None,
                            }));

                        total_msgs_collected += 1;
                        msgs_collected_from_last_anchor += 1;

                        tracing::trace!(target: tracing_targets::COLLATOR_READ_NEXT_EXTS,
                            anchor_id,
                            "collected ext_msg dst: {}", ext_msg.info.dst,
                        );

                        // TODO: msgs-v3: use buffer method for check
                        // check if buffer is full
                        // or we can already fill required slots
                        buffer_filled = Self::check_message_buffer_filled(
                            self.seqno,
                            &self.reader_state.buffer,
                            &self.messages_buffer_limits,
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
            if buffer_filled {
                break;
            }
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

        ReadExternalsRangeResult {
            has_pending_externals,
            last_read_to_anchor_chain_time,
        }
    }

    fn check_message_buffer_filled(
        seqno: BlockSeqno,
        messages_buffer: &MessagesBufferV2,
        messages_buffer_limits: &MessagesBufferLimits,
    ) -> bool {
        if messages_buffer.msgs_count() >= messages_buffer_limits.max_count {
            tracing::debug!(target: tracing_targets::COLLATOR,
                "message_groups buffer filled on {}/{}, stop reading range # {}",
                messages_buffer.msgs_count(), messages_buffer_limits.max_count, seqno,
            );
            return true;
        }

        // TODO: msgs-v3: check if we can already fill required slots

        false
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
}
