use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};
use std::sync::Arc;

use anyhow::Result;
use everscale_types::models::ShardIdent;
use tycho_block_util::queue::QueueKey;
use tycho_util::FastHashMap;

use crate::internal_queue::error::QueueError;
use crate::internal_queue::state::state_iterator::{IterRange, MessageExt};
use crate::internal_queue::state::states_iterators_manager::StatesIteratorsManager;
use crate::internal_queue::types::{InternalMessageValue, QueueDiffWithMessages, QueueFullDiff};

pub trait QueueIterator<V: InternalMessageValue>: Send {
    /// Get next message
    fn next(&mut self, with_new: bool) -> Result<Option<IterItem<V>>>;
    /// Get next only new message
    fn next_new(&mut self) -> Result<Option<IterItem<V>>>;
    /// Returns true if there are new messages to current shard
    fn has_new_messages_for_current_shard(&self) -> bool;
    fn current_position(&self) -> FastHashMap<ShardIdent, QueueKey>;
    fn process_new_messages(&mut self) -> Result<Option<IterItem<V>>>;
    /// Extract diff from iterator
    fn extract_full_diff(&mut self) -> QueueFullDiff<V>;
    /// Take diff from iterator
    fn take_diff(&self) -> QueueDiffWithMessages<V>;
    /// Commit processed messages
    /// It's getting last message position for each shard and save
    fn commit(&mut self, messages: Vec<(ShardIdent, QueueKey)>) -> Result<()>;
    /// Add new message to iterator
    fn add_message(&mut self, message: V) -> Result<()>;
    /// Set iterator new messages buffer from full diff
    fn set_new_messages_from_full_diff(&mut self, full_diff: QueueFullDiff<V>);
}

pub struct QueueIteratorImpl<V: InternalMessageValue> {
    for_shard: ShardIdent,
    messages_for_current_shard: BinaryHeap<Reverse<MessageExt<V>>>,
    new_messages: BTreeMap<QueueKey, Arc<V>>,
    iterators_manager: StatesIteratorsManager<V>,
    last_processed_message: FastHashMap<ShardIdent, QueueKey>,
}

impl<V: InternalMessageValue> QueueIteratorImpl<V> {
    pub fn new(
        iterators_manager: StatesIteratorsManager<V>,
        for_shard: ShardIdent,
    ) -> Result<Self, QueueError> {
        let messages_for_current_shard = BinaryHeap::default();

        Ok(Self {
            for_shard,
            messages_for_current_shard,
            new_messages: Default::default(),
            iterators_manager,
            last_processed_message: Default::default(),
        })
    }
}

pub struct IterItem<V: InternalMessageValue> {
    pub item: MessageExt<V>,
    pub is_new: bool,
}

fn update_shard_range(
    touched_shards: &mut FastHashMap<ShardIdent, (QueueKey, QueueKey)>,
    shard_id: ShardIdent,
    from: &QueueKey,
    to: &QueueKey,
) {
    touched_shards
        .entry(shard_id)
        .or_insert_with(|| (*from, *to));
}

impl<V: InternalMessageValue> QueueIterator<V> for QueueIteratorImpl<V> {
    fn next(&mut self, with_new: bool) -> Result<Option<IterItem<V>>> {
        // Process the next message from the snapshot manager
        if let Some(next_message) = self.iterators_manager.next()? {
            return Ok(Some(IterItem {
                item: next_message,
                is_new: false,
            }));
        }

        // Process the new messages if required
        if with_new {
            return self.process_new_messages();
        }

        Ok(None)
    }

    fn next_new(&mut self) -> Result<Option<IterItem<V>>> {
        // Process the new messages if required
        self.process_new_messages()
    }

    fn has_new_messages_for_current_shard(&self) -> bool {
        !self.messages_for_current_shard.is_empty()
    }

    fn current_position(&self) -> FastHashMap<ShardIdent, QueueKey> {
        self.iterators_manager.current_position()
    }

    // Function to process the new messages
    fn process_new_messages(&mut self) -> Result<Option<IterItem<V>>> {
        if let Some(next_message) = self.messages_for_current_shard.pop() {
            // remove message from new_messages
            self.new_messages.remove(&next_message.0.message.key());

            return Ok(Some(IterItem {
                item: next_message.0,
                is_new: true,
            }));
        }
        Ok(None)
    }

    fn extract_full_diff(&mut self) -> QueueFullDiff<V> {
        tracing::trace!(
            target: crate::tracing_targets::MQ,
            "Extracting diff from iterator. New messages count: {}",
            self.new_messages.len());

        let mut diff = QueueDiffWithMessages::new();

        // fill processed_upto
        for (shard_id, message_key) in self.last_processed_message.iter() {
            // TODO: may be `diff.processed_upto` should be a HashMap and we can consume it from iterator
            diff.processed_upto.insert(*shard_id, *message_key);
        }

        // move new messages
        std::mem::swap(&mut diff.messages, &mut self.new_messages);

        // move messages for current shard
        let mut full_diff = QueueFullDiff {
            diff,
            messages_for_current_shard: Default::default(),
        };
        std::mem::swap(
            &mut full_diff.messages_for_current_shard,
            &mut self.messages_for_current_shard,
        );

        full_diff
    }

    fn take_diff(&self) -> QueueDiffWithMessages<V> {
        tracing::trace!(
            target: crate::tracing_targets::MQ,
            "Taking diff from iterator. New messages count: {}",
            self.new_messages.len());

        let mut diff = QueueDiffWithMessages::new();

        // actually we update last processed message via commit()
        // during the execution, so we can just use value as is

        for (shard_id, message_key) in self.last_processed_message.iter() {
            // TODO: may be `diff.processed_upto` should be a HashMap and we can consume it from iterator
            diff.processed_upto.insert(*shard_id, *message_key);
        }

        diff.messages = self.new_messages.clone();

        diff
    }

    // Function to update the commit position
    fn commit(&mut self, messages: Vec<(ShardIdent, QueueKey)>) -> Result<()> {
        for (source_shard, message_key) in messages {
            self.last_processed_message
                .entry(source_shard)
                .and_modify(|e| {
                    if &message_key > e {
                        *e = message_key;
                    }
                })
                .or_insert(message_key);
        }
        Ok(())
    }

    fn add_message(&mut self, message: V) -> Result<()> {
        let message = Arc::new(message);
        self.new_messages.insert(message.key(), message.clone());
        if self.for_shard.contains_address(message.destination()) {
            let message_with_source = MessageExt::new(self.for_shard, message);
            self.messages_for_current_shard
                .push(Reverse(message_with_source));
        };
        Ok(())
    }

    fn set_new_messages_from_full_diff(&mut self, mut full_diff: QueueFullDiff<V>) {
        std::mem::swap(&mut self.new_messages, &mut full_diff.diff.messages);
        std::mem::swap(
            &mut self.messages_for_current_shard,
            &mut full_diff.messages_for_current_shard,
        );
    }
}

fn find_common_ancestor(shard1: ShardIdent, shard2: ShardIdent) -> Option<ShardIdent> {
    if shard1.is_ancestor_of(&shard2) {
        Some(shard1)
    } else if shard2.is_ancestor_of(&shard1) {
        Some(shard2)
    } else {
        None
    }
}

pub struct QueueIteratorExt;

impl QueueIteratorExt {
    pub fn collect_ranges(
        shards_from: FastHashMap<ShardIdent, QueueKey>,
        shards_to: FastHashMap<ShardIdent, QueueKey>,
    ) -> FastHashMap<ShardIdent, (QueueKey, QueueKey)> {
        let mut shards_with_ranges = FastHashMap::default();
        for from in shards_from {
            for to in &shards_to {
                let iter_range_from = IterRange {
                    shard_id: from.0,
                    key: from.1,
                };
                let iter_range_to = IterRange {
                    shard_id: *to.0,
                    key: *to.1,
                };
                Self::traverse_and_collect_ranges(
                    &mut shards_with_ranges,
                    &iter_range_from,
                    &iter_range_to,
                );
            }
        }

        shards_with_ranges
    }

    pub fn traverse_and_collect_ranges(
        touched_shards: &mut FastHashMap<ShardIdent, (QueueKey, QueueKey)>,
        from_range: &IterRange,
        to_range: &IterRange,
    ) {
        if from_range.shard_id == to_range.shard_id
            || from_range.shard_id.intersects(&to_range.shard_id)
        {
            update_shard_range(
                touched_shards,
                from_range.shard_id,
                &from_range.key,
                &to_range.key,
            );
        } else if from_range.shard_id.is_parent_of(&to_range.shard_id)
            || from_range.shard_id.is_child_of(&to_range.shard_id)
        {
            update_shard_range(
                touched_shards,
                from_range.shard_id,
                &from_range.key,
                &QueueKey::MAX,
            );
            update_shard_range(
                touched_shards,
                to_range.shard_id,
                &QueueKey::MIN,
                &to_range.key,
            );
        }

        if let Some(common_ancestor) = find_common_ancestor(from_range.shard_id, to_range.shard_id)
        {
            update_shard_range(
                touched_shards,
                from_range.shard_id,
                &from_range.key,
                &QueueKey::MAX,
            );
            update_shard_range(
                touched_shards,
                to_range.shard_id,
                &QueueKey::MIN,
                &to_range.key,
            );

            let mut current_shard = if from_range.shard_id.is_ancestor_of(&to_range.shard_id) {
                to_range.shard_id
            } else {
                from_range.shard_id
            };

            while current_shard != common_ancestor {
                if let Some(parent_shard) = current_shard.merge() {
                    update_shard_range(
                        touched_shards,
                        parent_shard,
                        &QueueKey::MIN,
                        &QueueKey::MAX,
                    );
                    current_shard = parent_shard;
                } else {
                    break;
                }
            }
        }
    }
}
