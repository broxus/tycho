use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};
use std::sync::Arc;

use anyhow::{bail, Result};
use everscale_types::models::ShardIdent;
use tycho_util::FastHashMap;

use crate::internal_queue::error::QueueError;
use crate::internal_queue::state::state_iterator::{IterRange, MessageWithSource, ShardRange};
use crate::internal_queue::state::states_iterators_manager::StatesIteratorsManager;
use crate::internal_queue::types::{EnqueuedMessage, InternalMessageKey, QueueDiff};

pub trait QueueIterator: Send {
    /// Get next message
    fn next(&mut self, with_new: bool) -> Result<Option<IterItem>>;
    /// Get next only new message
    fn next_new(&mut self) -> Result<Option<IterItem>>;
    /// Returns true if there are new messages to current shard
    fn has_new_messages_for_current_shard(&self) -> bool;
    fn update_last_read_message(
        &mut self,
        source_shard: ShardIdent,
        message_key: &InternalMessageKey,
    );
    fn process_new_messages(&mut self) -> Result<Option<IterItem>>;
    /// Extract diff from iterator and return has_pending_internals flag
    fn into_diff(self) -> Result<(QueueDiff, bool)>;
    /// Take diff from iterator
    fn take_diff(&self) -> QueueDiff;
    /// Commit processed messages
    /// It's getting last message position for each shard and save
    fn commit(&mut self, messages: Vec<(ShardIdent, InternalMessageKey)>) -> Result<()>;
    /// Add new message to iterator
    fn add_message(&mut self, message: Arc<EnqueuedMessage>) -> Result<()>;
}

pub struct QueueIteratorImpl {
    for_shard: ShardIdent,
    commited_current_position: BTreeMap<ShardIdent, InternalMessageKey>,
    messages_for_current_shard: BinaryHeap<Reverse<Arc<MessageWithSource>>>,
    new_messages: BTreeMap<InternalMessageKey, Arc<EnqueuedMessage>>,
    snapshot_manager: StatesIteratorsManager,
    last_processed_message: FastHashMap<ShardIdent, InternalMessageKey>,
    last_read_message_for_current_shard: FastHashMap<ShardIdent, InternalMessageKey>,
    read_position: BTreeMap<ShardIdent, InternalMessageKey>,
}

impl QueueIteratorImpl {
    pub fn new(
        snapshot_manager: StatesIteratorsManager,
        for_shard: ShardIdent,
    ) -> Result<Self, QueueError> {
        let messages_for_current_shard = BinaryHeap::default();

        Ok(Self {
            for_shard,
            messages_for_current_shard,
            new_messages: Default::default(),
            commited_current_position: Default::default(),
            snapshot_manager,
            last_processed_message: Default::default(),
            last_read_message_for_current_shard: Default::default(),
            read_position: Default::default(),
        })
    }
}

pub struct IterItem {
    pub message_with_source: Arc<MessageWithSource>,
    pub is_new: bool,
}

fn update_shard_range(
    touched_shards: &mut FastHashMap<ShardIdent, ShardRange>,
    shard_id: ShardIdent,
    from: Option<InternalMessageKey>,
    to: Option<InternalMessageKey>,
) {
    touched_shards
        .entry(shard_id)
        .or_insert_with(|| ShardRange { shard_id, from, to });
}

impl QueueIterator for QueueIteratorImpl {
    fn next(&mut self, with_new: bool) -> Result<Option<IterItem>> {
        // Process the next message from the snapshot manager
        while let Some(next_message) = self.snapshot_manager.next()? {
            self.update_last_read_message(next_message.shard_id, &next_message.message.key());

            if self
                .for_shard
                .contains_address(&next_message.message.info.dst)
            {
                self.last_read_message_for_current_shard
                    .insert(next_message.shard_id, next_message.message.key());

                return Ok(Some(IterItem {
                    message_with_source: next_message.clone(),
                    is_new: false,
                }));
            }
        }

        // Process the new messages if required
        if with_new {
            return self.process_new_messages();
        }

        Ok(None)
    }

    fn next_new(&mut self) -> Result<Option<IterItem>> {
        // Process the new messages if required
        self.process_new_messages()
    }

    fn has_new_messages_for_current_shard(&self) -> bool {
        !self.messages_for_current_shard.is_empty()
    }

    // Function to update the read position
    fn update_last_read_message(
        &mut self,
        source_shard: ShardIdent,
        message_key: &InternalMessageKey,
    ) {
        self.read_position
            .entry(source_shard)
            .and_modify(|e| {
                if message_key > e {
                    *e = message_key.clone();
                }
            })
            .or_insert(message_key.clone());
    }

    // Function to process the new messages
    fn process_new_messages(&mut self) -> Result<Option<IterItem>> {
        if let Some(next_message) = self.messages_for_current_shard.pop() {
            // remove message from new_messages
            self.new_messages.remove(&next_message.0.message.key());

            return Ok(Some(IterItem {
                message_with_source: next_message.0.clone(),
                is_new: true,
            }));
        }
        Ok(None)
    }

    fn into_diff(mut self) -> Result<(QueueDiff, bool)> {
        tracing::trace!(
            target: crate::tracing_targets::MQ,
            "Extracting diff from iterator. New messages count: {}",
            self.new_messages.len());

        let mut diff = QueueDiff::default();

        // fill processed_upto
        for (shard_id, message_key) in self.last_processed_message.iter() {
            // TODO: may be `diff.processed_upto` should be a HashMap and we can consume it from iterator
            diff.processed_upto.insert(*shard_id, message_key.clone());
        }

        // check if has pending internals in new messages
        let mut has_pending_internals = !self.messages_for_current_shard.is_empty();

        // check if has pending internals among existing
        if !has_pending_internals {
            has_pending_internals = self.next(false)?.is_some();
        }

        // fill new messages (new_messages shoul not be updated in previous operations)
        diff.messages = self.new_messages;

        Ok((diff, has_pending_internals))
    }

    fn take_diff(&self) -> QueueDiff {
        tracing::trace!(
            target: crate::tracing_targets::MQ,
            "Taking diff from iterator. New messages count: {}",
            self.new_messages.len());

        let mut diff = QueueDiff::default();

        // actually we update last processed message via commit()
        // during the execution, so we can just use value as is

        for (shard_id, message_key) in self.last_processed_message.iter() {
            // TODO: may be `diff.processed_upto` should be a HashMap and we can consume it from iterator
            diff.processed_upto.insert(*shard_id, message_key.clone());
        }

        diff.messages = self.new_messages.clone();

        // let mut read_position = self.read_position.clone();

        // for processed_last_message in self.last_processed_message.iter() {
        //     if !read_position.contains_key(&processed_last_message.0) {
        //         read_position.insert(
        //             processed_last_message.0.clone(),
        //             processed_last_message.1.clone(),
        //         );
        //     }
        // }

        // for (shard_id, last_read_key) in read_position.iter() {
        //     let last_read_message_for_current_shard = self
        //         .last_read_message_for_current_shard
        //         .get(&shard_id)
        //         .cloned();
        //     let processed_last_message = self.last_processed_message.get(&shard_id).cloned();

        //     match (last_read_message_for_current_shard, processed_last_message) {
        //         (Some(read_last_message), Some(processed_last_message)) => {
        //             if read_last_message == processed_last_message {
        //                 // processed all read messages
        //                 diff.processed_upto
        //                     .insert(*shard_id, read_last_message.clone());
        //             } else {
        //                 // processed greater than read or lower
        //                 diff.processed_upto
        //                     .insert(*shard_id, processed_last_message);
        //             }
        //         }
        //         (Some(_read_last_message), None) => {
        //             // read last message but no one processed. try again next time
        //             // diff.processed_upto
        //             //     .insert(*shard_id, read_last_message.clone());
        //         }
        //         (None, Some(processed_last_message)) => {
        //             // no old messages read, but some new processed
        //             diff.processed_upto
        //                 .insert(*shard_id, processed_last_message.clone());
        //         }
        //         (None, None) => {
        //             // no old messages read, no new processed
        //             diff.processed_upto.insert(*shard_id, last_read_key.clone());
        //         }
        //     }
        // }

        // for message in self.new_messages.values() {
        //     diff.messages.insert(message.key(), message.clone());
        // }

        diff
    }

    // Function to update the commit position
    fn commit(&mut self, messages: Vec<(ShardIdent, InternalMessageKey)>) -> Result<()> {
        for (source_shard, message_key) in messages {
            self.last_processed_message
                .entry(source_shard)
                .and_modify(|e| {
                    if message_key > *e {
                        *e = message_key.clone();
                    }
                })
                .or_insert(message_key.clone());
        }
        Ok(())
    }

    fn add_message(&mut self, message: Arc<EnqueuedMessage>) -> Result<()> {
        self.new_messages.insert(message.key(), message.clone());
        if self.for_shard.contains_address(&message.info.dst) {
            let message_with_source = MessageWithSource::new(self.for_shard, message);
            self.messages_for_current_shard
                .push(Reverse(Arc::new(message_with_source)));
        };
        Ok(())
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
        shards_from: FastHashMap<ShardIdent, InternalMessageKey>,
        shards_to: FastHashMap<ShardIdent, InternalMessageKey>,
    ) -> FastHashMap<ShardIdent, ShardRange> {
        let mut shards_with_ranges = FastHashMap::default();
        for from in shards_from {
            for to in &shards_to {
                let iter_range_from = IterRange {
                    shard_id: from.0,
                    key: from.1.clone(),
                };
                let iter_range_to = IterRange {
                    shard_id: *to.0,
                    key: to.1.clone(),
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
        touched_shards: &mut FastHashMap<ShardIdent, ShardRange>,
        from_range: &IterRange,
        to_range: &IterRange,
    ) {
        if from_range.shard_id == to_range.shard_id
            || from_range.shard_id.intersects(&to_range.shard_id)
        {
            update_shard_range(
                touched_shards,
                from_range.shard_id,
                Some(from_range.key.clone()),
                Some(to_range.key.clone()),
            );
        } else if from_range.shard_id.is_parent_of(&to_range.shard_id)
            || from_range.shard_id.is_child_of(&to_range.shard_id)
        {
            update_shard_range(
                touched_shards,
                from_range.shard_id,
                Some(from_range.key.clone()),
                None,
            );
            update_shard_range(
                touched_shards,
                to_range.shard_id,
                None,
                Some(to_range.key.clone()),
            );
        }

        if let Some(common_ancestor) = find_common_ancestor(from_range.shard_id, to_range.shard_id)
        {
            update_shard_range(
                touched_shards,
                from_range.shard_id,
                Some(from_range.key.clone()),
                None,
            );
            update_shard_range(
                touched_shards,
                to_range.shard_id,
                None,
                Some(to_range.key.clone()),
            );

            let mut current_shard = if from_range.shard_id.is_ancestor_of(&to_range.shard_id) {
                to_range.shard_id
            } else {
                from_range.shard_id
            };

            while current_shard != common_ancestor {
                if let Some(parent_shard) = current_shard.merge() {
                    update_shard_range(touched_shards, parent_shard, None, None);
                    current_shard = parent_shard;
                } else {
                    break;
                }
            }
        }
    }
}
