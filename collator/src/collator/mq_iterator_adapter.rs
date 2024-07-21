use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use everscale_types::cell::HashBytes;
use everscale_types::models::ShardIdent;
use tycho_util::FastHashMap;

use super::execution_manager::{update_internals_processed_upto, ProcessedUptoUpdate};
use super::types::{ProcessedUptoInfoStuff, WorkingState};
use crate::internal_queue::iterator::{IterItem, QueueIterator};
use crate::internal_queue::types::{InternalMessageKey, QueueDiff};
use crate::queue_adapter::MessageQueueAdapter;
use crate::tracing_targets;

pub(super) struct QueueIteratorAdapter {
    shard_id: ShardIdent,
    /// internals mq adapter
    mq_adapter: Arc<dyn MessageQueueAdapter>,
    /// current internals mq iterator
    iterator_opt: Option<Box<dyn QueueIterator>>,
    /// there is no more existing internals in mq iterator
    no_pending_existing_internals: bool,
    /// there is no more new messages in mq iterator
    no_pending_new_messages: bool,
    /// current new messages to read_to border
    new_messages_read_to: InternalMessageKey,
    /// current read position of internals mq iterator
    current_positions: FastHashMap<ShardIdent, InternalMessageKey>,

    /// sum total iterators initialization time
    init_iterator_total_elapsed: Duration,
}

impl QueueIteratorAdapter {
    pub fn new(
        shard_id: ShardIdent,
        mq_adapter: Arc<dyn MessageQueueAdapter>,
        current_positions_opt: Option<FastHashMap<ShardIdent, InternalMessageKey>>,
    ) -> Self {
        Self {
            shard_id,
            mq_adapter,
            iterator_opt: None,
            no_pending_existing_internals: true,
            no_pending_new_messages: true,
            new_messages_read_to: InternalMessageKey::default(),
            current_positions: current_positions_opt.unwrap_or_default(),
            init_iterator_total_elapsed: Duration::ZERO,
        }
    }

    pub fn release(
        mut self,
    ) -> Result<(FastHashMap<ShardIdent, InternalMessageKey>, bool, QueueDiff)> {
        let mut has_pending_internals = self.iterator().has_new_messages_for_current_shard();
        if !has_pending_internals && !self.no_pending_existing_internals {
            has_pending_internals = self.iterator().next(false)?.is_some();
        }
        let full_diff = self.iterator().extract_full_diff();
        Ok((
            self.current_positions,
            has_pending_internals,
            full_diff.diff,
        ))
    }

    pub fn no_pending_existing_internals(&self) -> bool {
        self.no_pending_existing_internals
    }
    pub fn no_pending_new_messages(&self) -> bool {
        self.no_pending_new_messages
    }

    pub fn iterator_is_none(&self) -> bool {
        self.iterator_opt.is_none()
    }

    pub fn iterator(&mut self) -> &mut Box<dyn QueueIterator> {
        self.iterator_opt
            .as_mut()
            .expect("`iterator_opt` should be initialized first")
    }

    pub fn take_diff(&mut self) -> QueueDiff {
        self.iterator().take_diff()
    }

    pub fn init_iterator_total_elapsed(&self) -> Duration {
        self.init_iterator_total_elapsed
    }

    pub async fn try_init_next_range_iterator(
        &mut self,
        processed_upto: &mut ProcessedUptoInfoStuff,
        working_state: &WorkingState,
    ) -> Result<bool> {
        let timer = std::time::Instant::now();

        // get current ranges
        let mut ranges_from = FastHashMap::<_, InternalMessageKey>::default();
        let mut ranges_to = FastHashMap::<_, InternalMessageKey>::default();
        let mut ranges_fully_read = true;
        for (shard_id, int_processed_upto) in processed_upto.internals.iter() {
            if int_processed_upto.processed_to_msg != int_processed_upto.read_to_msg {
                ranges_fully_read = false;
            }
            ranges_from.insert(*shard_id, int_processed_upto.processed_to_msg.clone());
            ranges_to.insert(*shard_id, int_processed_upto.read_to_msg.clone());
        }

        tracing::debug!(target: tracing_targets::COLLATOR,
            "try_init_next_range_iterator: initialized={}, existing ranges_fully_read={}, \
            ranges_from={:?}, ranges_to={:?}",
            self.iterator_opt.is_some(), ranges_fully_read,
            ranges_from, ranges_to,
        );

        let new_iterator_opt = if self.iterator_opt.is_none() && !ranges_fully_read {
            // when current iterator is not initialized we have 2 possible cases:
            // 1. ranges were not fully read in previous collation
            // 2. ranges were fully read (processed_to == read_to)

            // we use existing ranges to init iterator only when they were not fully read
            // but will read from current position
            // and check if ranges are fully read from current position
            let current_ranges_from = ranges_from.clone();
            ranges_fully_read = true;
            for (shard_id, from) in ranges_from.iter_mut() {
                if let Some(curren_position) = self.current_positions.get(shard_id) {
                    from.lt = curren_position.lt;
                    from.hash = curren_position.hash;
                }
                let to = ranges_to.get(shard_id).unwrap();
                if from != to {
                    ranges_fully_read = false;
                }
            }
            tracing::debug!(target: tracing_targets::COLLATOR,
                "try_init_next_range_iterator: ranges updated from current position, ranges_fully_read={}, \
                ranges_from={:?}, ranges_to={:?}",
                ranges_fully_read,
                ranges_from, ranges_to,
            );

            let mut current_ranges_iterator = self
                .mq_adapter
                .create_iterator(self.shard_id, ranges_from, ranges_to)
                .await?;
            // set processed messages in iterator by original ranges_from
            current_ranges_iterator.commit(current_ranges_from.into_iter().collect())?;
            Some(current_ranges_iterator)
        } else {
            // when current iterator exists or existing ranges fully read

            // then try to calc new ranges from current states
            let mut ranges_updated = false;

            // add masterchain default range if not exist
            let mc_shard_id = ShardIdent::new_full(-1);
            ranges_from.entry(mc_shard_id).or_insert_with(|| {
                ranges_updated = true;
                InternalMessageKey::with_lt_and_min_hash(0)
            });
            let mc_read_to = ranges_to.entry(mc_shard_id).or_insert_with(|| {
                ranges_updated = true;
                InternalMessageKey::with_lt_and_max_hash(0)
            });

            // try update masterchain range read_to border
            let new_mc_read_to_lt = if self.shard_id.is_masterchain() {
                working_state.prev_shard_data.gen_lt()
            } else {
                working_state.mc_data.gen_lt
            };
            if mc_read_to.lt < new_mc_read_to_lt {
                mc_read_to.lt = new_mc_read_to_lt;
                mc_read_to.hash = HashBytes([255; 32]);
                ranges_updated = true;
            }

            // try update shardchains ranges
            for shard in working_state.mc_data.shards.iter() {
                let (shard_id, shard_descr) = shard?;

                // add default shardchain range if not exist
                ranges_from.entry(shard_id).or_insert_with(|| {
                    ranges_updated = true;
                    InternalMessageKey::with_lt_and_min_hash(0)
                });
                let sc_read_to = ranges_to.entry(shard_id).or_insert_with(|| {
                    ranges_updated = true;
                    InternalMessageKey::with_lt_and_max_hash(0)
                });

                // try update shardchain read_to
                let new_sc_read_to_lt = if self.shard_id == shard_id {
                    // get new read_to LT from PrevData
                    working_state.prev_shard_data.gen_lt()
                } else {
                    // get new read_to LT from ShardDescription
                    shard_descr.end_lt
                };
                if sc_read_to.lt < new_sc_read_to_lt {
                    sc_read_to.lt = new_sc_read_to_lt;
                    sc_read_to.hash = HashBytes([255; 32]);
                    ranges_updated = true;
                }
            }

            // if ranges changed then init new iterator
            if ranges_updated {
                // if ranges changed then to > from, so they are not fully read
                ranges_fully_read = false;

                tracing::debug!(target: tracing_targets::COLLATOR,
                    "try_init_next_range_iterator: updated ranges_from={:?}, ranges_to={:?}",
                    ranges_from, ranges_to,
                );
                // update processed_upto info
                for (shard_id, new_process_to_key) in ranges_from.iter() {
                    let new_read_to_key = ranges_to.get(shard_id).unwrap();
                    update_internals_processed_upto(
                        processed_upto,
                        *shard_id,
                        Some(ProcessedUptoUpdate::Force(new_process_to_key.clone())),
                        Some(ProcessedUptoUpdate::Force(new_read_to_key.clone())),
                    );
                }
                // and init iterator
                let mut new_ranges_iterator = self
                    .mq_adapter
                    .create_iterator(self.shard_id, ranges_from.clone(), ranges_to)
                    .await?;
                // set processed messages in iterator by ranges_from
                new_ranges_iterator.commit(ranges_from.into_iter().collect())?;
                // we created iterator for new ranges - clear current position
                self.current_positions.clear();
                Some(new_ranges_iterator)
            } else {
                None
            }
        };

        let res = if self.iterator_opt.is_none() {
            self.no_pending_existing_internals = ranges_fully_read;
            assert!(new_iterator_opt.is_some());
            self.iterator_opt = new_iterator_opt;
            true
        } else if let Some(new_iterator) = new_iterator_opt {
            self.no_pending_existing_internals = false;
            // replace current iterator
            let prev_iterator = self.iterator_opt.replace(new_iterator).unwrap();
            // move diff to new iterator
            // FIXME: add optimized apply_diff() method for iterator
            let new_iterator = self.iterator();
            let diff = prev_iterator.take_diff();
            for (_, new_msg) in diff.messages {
                new_iterator.add_message(new_msg)?;
            }
            true
        } else {
            false
        };

        self.init_iterator_total_elapsed += timer.elapsed();

        Ok(res)
    }

    pub fn next_existing_message(&mut self) -> Result<Option<IterItem>> {
        if self.no_pending_existing_internals {
            Ok(None)
        } else {
            match self.iterator_opt.as_mut().unwrap().next(false)? {
                Some(int_msg) => {
                    let message_key = int_msg.message_with_source.message.key();
                    self.current_positions
                        .insert(int_msg.message_with_source.shard_id, message_key);

                    Ok(Some(int_msg))
                }
                None => {
                    self.no_pending_existing_internals = true;

                    Ok(None)
                }
            }
        }
    }

    pub fn try_update_new_messages_read_to(
        &mut self,
        max_new_message_key_to_current_shard: InternalMessageKey,
    ) -> Result<bool> {
        // try to set new messages read_to border
        if self.no_pending_new_messages
            && max_new_message_key_to_current_shard > self.new_messages_read_to
        {
            tracing::debug!(target: tracing_targets::COLLATOR,
                "will read new messages for shard {} upto {:?}",
                self.shard_id, max_new_message_key_to_current_shard,
            );
            self.new_messages_read_to = max_new_message_key_to_current_shard;
            self.no_pending_new_messages = false;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn next_new_message(&mut self) -> Result<Option<IterItem>> {
        // fill messages groups from iterator until the first group filled
        // or current new messages read window finished
        if self.no_pending_new_messages {
            Ok(None)
        } else {
            match self.iterator_opt.as_mut().unwrap().next_new()? {
                Some(int_msg) => {
                    let message_key = int_msg.message_with_source.message.key();

                    self.no_pending_new_messages = message_key == self.new_messages_read_to;

                    self.current_positions
                        .insert(int_msg.message_with_source.shard_id, message_key);

                    Ok(Some(int_msg))
                }
                None => {
                    self.no_pending_new_messages = true;

                    Ok(None)
                }
            }
        }
    }
}
