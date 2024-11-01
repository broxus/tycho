use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use everscale_types::cell::HashBytes;
use everscale_types::models::ShardIdent;
use tycho_block_util::queue::QueueKey;
use tycho_util::FastHashMap;

use super::execution_manager::{update_internals_processed_upto, ProcessedUptoUpdate};
use crate::internal_queue::iterator::{IterItem, QueueIterator};
use crate::internal_queue::types::{InternalMessageValue, QueueDiffWithMessages};
use crate::queue_adapter::MessageQueueAdapter;
use crate::tracing_targets;
use crate::types::{DisplayIter, DisplayTuple, ProcessedUptoInfoStuff};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum InitIteratorMode {
    UseNextRange,
    OmitNextRange,
}

pub(super) struct QueueIteratorAdapter<V: InternalMessageValue> {
    shard_id: ShardIdent,
    /// internals mq adapter
    mq_adapter: Arc<dyn MessageQueueAdapter<V>>,
    /// current internals mq iterator
    iterator_opt: Option<Box<dyn QueueIterator<V>>>,
    /// there is no more existing internals in mq iterator
    no_pending_existing_internals: bool,
    /// there is no more new messages in mq iterator
    no_pending_new_messages: bool,
    /// current new messages to `read_to` border
    new_messages_read_to: QueueKey,
    /// current read position of internals mq iterator
    current_positions: FastHashMap<ShardIdent, QueueKey>,
    /// sum total iterators initialization time
    init_iterator_total_elapsed: Duration,
}

impl<V: InternalMessageValue> QueueIteratorAdapter<V> {
    pub fn new(
        shard_id: ShardIdent,
        mq_adapter: Arc<dyn MessageQueueAdapter<V>>,
        current_positions: FastHashMap<ShardIdent, QueueKey>,
    ) -> Self {
        Self {
            shard_id,
            mq_adapter,
            iterator_opt: None,
            no_pending_existing_internals: true,
            no_pending_new_messages: true,
            new_messages_read_to: QueueKey::MIN,
            current_positions,
            init_iterator_total_elapsed: Duration::ZERO,
        }
    }

    pub fn release(
        mut self,
        check_pending_internals: bool,
        current_positions: &mut Option<FastHashMap<ShardIdent, QueueKey>>,
    ) -> Result<(bool, QueueDiffWithMessages<V>)> {
        let current_position = self.iterator().current_position();

        for (shard_id, key) in current_position {
            if let Some(current_key) = self.current_positions.get(&shard_id) {
                if &key > current_key {
                    self.current_positions.insert(shard_id, key);
                }
            } else {
                self.current_positions.insert(shard_id, key);
            }
        }

        let mut has_pending_internals = self.iterator().has_new_messages_for_current_shard();
        if !has_pending_internals && !self.no_pending_existing_internals && check_pending_internals
        {
            has_pending_internals = self.iterator().next(false)?.is_some();
        }
        let full_diff = self.iterator().extract_full_diff();

        *current_positions = Some(std::mem::take(&mut self.current_positions));

        Ok((has_pending_internals, full_diff.diff))
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

    pub fn iterator(&mut self) -> &mut Box<dyn QueueIterator<V>> {
        self.iterator_opt
            .as_mut()
            .expect("`iterator_opt` should be initialized first")
    }

    pub fn init_iterator_total_elapsed(&self) -> Duration {
        self.init_iterator_total_elapsed
    }

    #[tracing::instrument(skip_all, fields(mode = ?mode))]
    pub async fn try_init_next_range_iterator<'a, I>(
        &'a mut self,
        processed_upto: &mut ProcessedUptoInfoStuff,
        shards: I,
        mc_gen_lt: u64,
        prev_shard_data_gen_lt: u64,
        mode: InitIteratorMode,
    ) -> Result<bool>
    where
        I: Iterator<Item = (ShardIdent, u64)>,
    {
        let timer = std::time::Instant::now();

        // get current ranges
        let mut ranges_from = FastHashMap::default();
        let mut ranges_to = FastHashMap::default();
        let mut ranges_fully_read = true;
        for (shard_id, int_processed_upto) in processed_upto.internals.iter() {
            if int_processed_upto.processed_to_msg != int_processed_upto.read_to_msg {
                ranges_fully_read = false;
            }
            ranges_from.insert(*shard_id, int_processed_upto.processed_to_msg);
            ranges_to.insert(*shard_id, int_processed_upto.read_to_msg);
        }

        tracing::debug!(target: tracing_targets::COLLATOR,
            "initialized={}, existing ranges_fully_read={}, \
            ranges_from={}, ranges_to={}",
            self.iterator_opt.is_some(), ranges_fully_read,
            DisplayIter(ranges_from.iter().map(DisplayTuple)),
            DisplayIter(ranges_to.iter().map(DisplayTuple)),
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
                    *from = *curren_position;
                }
                let to = ranges_to.get(shard_id).unwrap();
                if from != to {
                    ranges_fully_read = false;
                }
            }

            tracing::debug!(target: tracing_targets::COLLATOR,
                "ranges updated from current position, ranges_fully_read={}, \
                ranges_from={}, ranges_to={}",
                ranges_fully_read,
                DisplayIter(ranges_from.iter().map(DisplayTuple)),
                DisplayIter(ranges_to.iter().map(DisplayTuple)),
            );

            let mut current_ranges_iterator = self
                .mq_adapter
                .create_iterator(self.shard_id, ranges_from, ranges_to)
                .await?;
            // set processed messages in iterator by original ranges_from
            current_ranges_iterator.commit(current_ranges_from.into_iter().collect())?;
            Some(current_ranges_iterator)
        } else if mode == InitIteratorMode::OmitNextRange {
            // on refill we do not need to use next range at all
            if self.iterator_is_none() {
                tracing::debug!(target: tracing_targets::COLLATOR,
                    "init with last ranges, ranges_fully_read={}, \
                    ranges_from={}, ranges_to={}",
                    ranges_fully_read,
                    DisplayIter(ranges_from.iter().map(DisplayTuple)),
                    DisplayIter(ranges_to.iter().map(DisplayTuple)),
                );
                let mut current_ranges_iterator = self
                    .mq_adapter
                    .create_iterator(self.shard_id, ranges_from.clone(), ranges_to)
                    .await?;
                // set processed messages in iterator by ranges_from
                current_ranges_iterator.commit(ranges_from.into_iter().collect())?;
                Some(current_ranges_iterator)
            } else {
                None
            }
        } else {
            // when current iterator exists or existing ranges fully read

            // then try to calc new ranges from current states

            // add masterchain default range if not exist
            let mc_shard_id = ShardIdent::new_full(-1);

            let mut ranges_updated = Self::try_update_ranges_for_shard(
                mc_shard_id,
                mc_gen_lt,
                prev_shard_data_gen_lt,
                &mut ranges_from,
                &mut ranges_to,
                || self.shard_id.is_masterchain(),
            );

            for (shard_id, shard_description_end_lt) in shards {
                ranges_updated |= Self::try_update_ranges_for_shard(
                    shard_id,
                    shard_description_end_lt,
                    prev_shard_data_gen_lt,
                    &mut ranges_from,
                    &mut ranges_to,
                    || self.shard_id == shard_id,
                );
            }

            // if ranges changed then init new iterator
            if ranges_updated {
                // if ranges changed then to > from, so they are not fully read
                ranges_fully_read = false;

                tracing::debug!(target: tracing_targets::COLLATOR,
                    "updated ranges_from={}, ranges_to={}",
                    DisplayIter(ranges_from.iter().map(DisplayTuple)),
                    DisplayIter(ranges_to.iter().map(DisplayTuple)),
                );
                // update processed_upto info
                for (shard_id, new_process_to_key) in ranges_from.iter() {
                    let new_read_to_key = ranges_to.get(shard_id).unwrap();
                    update_internals_processed_upto(
                        processed_upto,
                        *shard_id,
                        Some(ProcessedUptoUpdate::Force(*new_process_to_key)),
                        Some(ProcessedUptoUpdate::Force(*new_read_to_key)),
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
            let mut prev_iterator = self.iterator_opt.replace(new_iterator).unwrap();
            // move new messages to new iterator
            let new_iterator = self.iterator();
            let full_diff = prev_iterator.extract_full_diff();
            new_iterator.set_new_messages_from_full_diff(full_diff);
            true
        } else {
            false
        };

        self.init_iterator_total_elapsed += timer.elapsed();

        Ok(res)
    }

    fn try_update_ranges_for_shard<F>(
        shard_id: ShardIdent,
        shard_description_end_lt: u64,
        prev_shard_data_gen_lt: u64,
        ranges_from: &mut FastHashMap<ShardIdent, QueueKey>,
        ranges_to: &mut FastHashMap<ShardIdent, QueueKey>,
        is_current_shard: F,
    ) -> bool
    where
        F: Fn() -> bool,
    {
        let mut ranges_updated = false;
        // try update shardchains ranges
        // add default shardchain range if not exist
        ranges_from.entry(shard_id).or_insert_with(|| {
            ranges_updated = true;
            QueueKey::MIN
        });
        let sc_read_to = ranges_to.entry(shard_id).or_insert_with(|| {
            ranges_updated = true;
            QueueKey::max_for_lt(0)
        });

        // try update shardchain read_to
        let new_sc_read_to_lt = if is_current_shard() {
            // get new read_to LT from PrevData
            prev_shard_data_gen_lt
        } else {
            // get new LT from ShardDescription
            shard_description_end_lt
        };
        if sc_read_to.lt < new_sc_read_to_lt {
            sc_read_to.lt = new_sc_read_to_lt;
            sc_read_to.hash = HashBytes([255; 32]);
            ranges_updated = true;
        }
        ranges_updated
    }

    pub fn next_existing_message(&mut self) -> Result<Option<IterItem<V>>> {
        if self.no_pending_existing_internals {
            Ok(None)
        } else {
            match self.iterator_opt.as_mut().unwrap().next(false)? {
                Some(int_msg) => Ok(Some(int_msg)),

                None => {
                    self.no_pending_existing_internals = true;
                    Ok(None)
                }
            }
        }
    }

    pub fn try_update_new_messages_read_to(
        &mut self,
        max_new_message_key_to_current_shard: &QueueKey,
    ) -> Result<bool> {
        // try to set new messages read_to border
        if self.no_pending_new_messages
            && max_new_message_key_to_current_shard > &self.new_messages_read_to
        {
            tracing::debug!(target: tracing_targets::COLLATOR,
                "will read new messages for shard {} upto {:?}",
                self.shard_id, max_new_message_key_to_current_shard,
            );
            self.new_messages_read_to = *max_new_message_key_to_current_shard;
            self.no_pending_new_messages = false;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn next_new_message(&mut self) -> Result<Option<IterItem<V>>> {
        // fill messages groups from iterator until the first group filled
        // or current new messages read window finished
        if self.no_pending_new_messages {
            Ok(None)
        } else {
            match self.iterator_opt.as_mut().unwrap().next_new()? {
                Some(int_msg) => {
                    let message_key = int_msg.item.message.key();

                    self.no_pending_new_messages = message_key == self.new_messages_read_to;

                    self.current_positions
                        .insert(int_msg.item.source, message_key);

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
