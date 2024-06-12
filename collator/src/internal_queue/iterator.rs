use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;

use anyhow::{bail, Result};
use everscale_types::cell::HashBytes;
use everscale_types::models::ShardIdent;
use tycho_util::FastHashMap;

use crate::internal_queue::error::QueueError;
use crate::internal_queue::state::state_iterator::{
    IterRangeFrom, IterRangeTo, MessageWithSource, ShardRange,
};
use crate::internal_queue::state::states_iterators_manager::StatesIteratorsManager;
use crate::internal_queue::types::{EnqueuedMessage, InternalMessageKey, Lt, QueueDiff};
pub trait QueueIterator: Send {
    /// Get next message
    fn next(&mut self, with_new: bool) -> Result<Option<IterItem>>;
    /// Peek next message
    fn peek(&mut self, with_new: bool) -> Result<Option<IterItem>>;
    /// Take diff from iterator
    /// Move current position to commited position
    /// Create new transaction
    fn take_diff(&mut self) -> QueueDiff;
    /// Commit processed messages
    /// It's getting last message position for each shard and save
    fn commit(&mut self, messages: Vec<(ShardIdent, InternalMessageKey)>) -> Result<()>;
    /// Add new message to iterator
    fn add_message(&mut self, message: Arc<EnqueuedMessage>) -> Result<()>;
}

pub struct QueueIteratorImpl {
    for_shard: ShardIdent,
    current_position: FastHashMap<ShardIdent, InternalMessageKey>,
    commited_current_position: FastHashMap<ShardIdent, InternalMessageKey>,
    messages_for_current_shard: BinaryHeap<Reverse<Arc<MessageWithSource>>>,
    new_messages: FastHashMap<InternalMessageKey, Arc<EnqueuedMessage>>,
    snapshot_manager: StatesIteratorsManager,
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
            current_position: Default::default(),
            new_messages: Default::default(),
            commited_current_position: Default::default(),
            snapshot_manager,
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
    from_lt: Option<Lt>,
    from_hash: Option<HashBytes>,
    to_lt: Option<Lt>,
) {
    touched_shards
        .entry(shard_id)
        .or_insert_with(|| ShardRange {
            shard_id,
            from_lt,
            from_hash,
            to_lt,
        });
}

impl QueueIterator for QueueIteratorImpl {
    fn next(&mut self, with_new: bool) -> Result<Option<IterItem>> {
        if let Some(next_message) = self.snapshot_manager.next()? {
            return Ok(Some(IterItem {
                message_with_source: next_message.clone(),
                is_new: false,
            }));
        }

        if with_new {
            if let Some(next_message) = self.messages_for_current_shard.pop() {
                let message_key = next_message.0.message.key();

                if self.new_messages.contains_key(&message_key) {
                    return Ok(Some(IterItem {
                        message_with_source: next_message.0.clone(),
                        is_new: true,
                    }));
                } else {
                    bail!(
                        "Message is not in new messages but in current shard messages: {:?}",
                        message_key
                    );
                }
            }
        }

        Ok(None)
    }

    fn peek(&mut self, with_new: bool) -> Result<Option<IterItem>> {
        if let Some(next_message) = self.snapshot_manager.peek()? {
            return Ok(Some(IterItem {
                message_with_source: next_message.clone(),
                is_new: false,
            }));
        }

        if with_new {
            if let Some(next_message) = self.messages_for_current_shard.peek() {
                let message_key = next_message.0.message.key();

                if self.new_messages.contains_key(&message_key) {
                    return Ok(Some(IterItem {
                        message_with_source: next_message.0.clone(),
                        is_new: true,
                    }));
                } else {
                    bail!(
                        "Message is not in new messages but in current shard messages: {:?}",
                        message_key
                    );
                }
            }
        }
        Ok(None)
    }

    fn take_diff(&mut self) -> QueueDiff {
        tracing::trace!(
            target: crate::tracing_targets::MQ,
            "Taking diff from iterator. New messages count: {}",
            self.new_messages.len());

        let mut diff = QueueDiff::default();

        for (shard_id, lt) in self.commited_current_position.iter() {
            diff.processed_upto.insert(*shard_id, lt.clone());
        }

        let current_shard_processed_upto = self
            .commited_current_position
            .get(&self.for_shard)
            .cloned()
            .unwrap_or_default();

        let amount_before = self.new_messages.len();

        let mut inserted_new_messages = 0;

        tracing::debug!(target: crate::tracing_targets::COLLATOR, "Current shard processed upto: {:?}",current_shard_processed_upto);

        for message in self.new_messages.values() {
            let (dest_workchain, dest_account) = message.destination().unwrap();
            if self.for_shard.contains_account(&dest_account)
                && self.for_shard.workchain() == dest_workchain as i32
            {
                if message.key() > current_shard_processed_upto {
                    diff.messages.push(message.clone());
                    inserted_new_messages += 1;
                }
            } else {
                diff.messages.push(message.clone());
                inserted_new_messages += 1;
            }
        }

        tracing::trace!(
            target: crate::tracing_targets::MQ,
            "Inserted {} messages out of {} to diff",
            inserted_new_messages,
            amount_before);

        self.current_position
            .clone_from(&self.commited_current_position);
        self.commited_current_position.clear();
        self.new_messages.clear();

        diff
    }

    fn commit(&mut self, messages: Vec<(ShardIdent, InternalMessageKey)>) -> Result<()> {
        tracing::debug!(
            target: crate::tracing_targets::MQ,
            "Committing messages to the iterator. Messages count: {}",
            messages.len());

        for message in messages {
            // insert only if key greater then current
            if let Some(current_key) = self.commited_current_position.get_mut(&message.0) {
                if message.1 > *current_key {
                    current_key.clone_from(&message.1);
                }
            } else {
                self.commited_current_position.insert(message.0, message.1);
            }
        }
        Ok(())
    }

    fn add_message(&mut self, message: Arc<EnqueuedMessage>) -> Result<()> {
        self.new_messages.insert(message.key(), message.clone());
        let (dest_workchain, dest_account) = message.destination()?;
        if self.for_shard.contains_account(&dest_account)
            && self.for_shard.workchain() == dest_workchain as i32
        {
            let message_with_source = MessageWithSource::new(self.for_shard, message.clone());
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
        shards_from: FastHashMap<ShardIdent, (u64, HashBytes)>,
        shards_to: FastHashMap<ShardIdent, u64>,
    ) -> FastHashMap<ShardIdent, ShardRange> {
        let mut shards_with_ranges = FastHashMap::default();
        for from in shards_from {
            for to in &shards_to {
                let iter_range_from = IterRangeFrom {
                    shard_id: from.0,
                    lt: from.1 .0,
                    hash: from.1 .1,
                };
                let iter_range_to = IterRangeTo {
                    shard_id: *to.0,
                    lt: *to.1,
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
        from_range: &IterRangeFrom,
        to_range: &IterRangeTo,
    ) {
        if from_range.shard_id == to_range.shard_id
            || from_range.shard_id.intersects(&to_range.shard_id)
        {
            update_shard_range(
                touched_shards,
                from_range.shard_id,
                Some(from_range.lt),
                Some(from_range.hash),
                Some(to_range.lt),
            );
        } else if from_range.shard_id.is_parent_of(&to_range.shard_id)
            || from_range.shard_id.is_child_of(&to_range.shard_id)
        {
            update_shard_range(
                touched_shards,
                from_range.shard_id,
                Some(from_range.lt),
                Some(from_range.hash),
                None,
            );
            update_shard_range(
                touched_shards,
                to_range.shard_id,
                None,
                None,
                Some(to_range.lt),
            );
        }

        if let Some(common_ancestor) = find_common_ancestor(from_range.shard_id, to_range.shard_id)
        {
            update_shard_range(
                touched_shards,
                from_range.shard_id,
                Some(from_range.lt),
                Some(from_range.hash),
                None,
            );
            update_shard_range(
                touched_shards,
                to_range.shard_id,
                None,
                None,
                Some(to_range.lt),
            );

            let mut current_shard = if from_range.shard_id.is_ancestor_of(&to_range.shard_id) {
                to_range.shard_id
            } else {
                from_range.shard_id
            };

            while current_shard != common_ancestor {
                if let Some(parent_shard) = current_shard.merge() {
                    update_shard_range(touched_shards, parent_shard, None, None, None);
                    current_shard = parent_shard;
                } else {
                    break;
                }
            }
        }
    }
}

// #[cfg(test)]
// mod tests {
//     use std::collections::HashMap;
//
//     use super::*;
//     use crate::internal_queue::session::session_state_snapshot::SessionStateSnapshot;
//     use crate::internal_queue::shard::Shard;
//     use crate::internal_queue::types::ext_types_stubs::{MessageContent, MessageEnvelope};
//
//     fn mock_snapshot() -> Box<dyn StateSnapshot> {
//         let shard_id = ShardIdent::new_full(0);
//         let mut shard = Shard::new(shard_id);
//         let split = shard_id.split().unwrap();
//         let shard2 = Shard::new(split.1);
//         let shard1 = Shard::new(split.0);
//
//         let message1 = EnqueuedMessage {
//             created_lt: 10,
//             enqueued_lt: 0,
//             hash: "somehash".to_string(),
//             env: MessageEnvelope {
//                 message: MessageContent {},
//                 from_contract: "0:46768a917036eb8dc0bf51465f6355cd64eeb3449ba31ae00a18226f65bb675a"
//                     .to_string(),
//                 to_contract: "0:46768a917036eb8dc0bf51465f6355cd64eeb3449ba31ae00a18226f65bb675a"
//                     .to_string(),
//             },
//         };
//
//         let message2 = EnqueuedMessage {
//             created_lt: 20,
//             enqueued_lt: 0,
//             hash: "somehash2".to_string(),
//             env: MessageEnvelope {
//                 message: MessageContent {},
//                 from_contract: "0:46768a917036eb8dc0bf51465f6355cd64eeb3449ba31ae00a18226f65bb675a"
//                     .to_string(),
//                 to_contract: "0:46768a917036eb8dc0bf51465f6355cd64eeb3449ba31ae00a18226f65bb675a"
//                     .to_string(),
//             },
//         };
//
//         shard
//             .outgoing_messages
//             .insert(message1.key(), Arc::new(message1));
//         shard
//             .outgoing_messages
//             .insert(message2.key(), Arc::new(message2));
//
//         let mut flat_shards = HashMap::new();
//         flat_shards.insert(shard_id, shard);
//         flat_shards.insert(split.1, shard2);
//         flat_shards.insert(split.0, shard1);
//
//         let session_state_snapshot = SessionStateSnapshot::new(flat_shards);
//
//         Box::new(session_state_snapshot)
//     }
//
//     #[test]
//     fn initialization_and_basic_iteration() {
//         let shard = ShardIdent::new_full(0);
//         let split = shard.split().unwrap();
//
//         let shards_from = vec![IterRange {
//             shard_id: shard,
//             lt: 0,
//         }];
//         let _shards_to = vec![IterRange {
//             shard_id: split.0,
//             lt: 20,
//         }];
//         let shards_to = vec![IterRange {
//             shard_id: split.1,
//             lt: 20,
//         }];
//
//         let snapshots = vec![mock_snapshot()]; // You would need mock snapshots here
//
//         let for_block = split.0;
//         let mut iterator =
//             QueueIteratorImpl::new(shards_from, shards_to, snapshots, for_block).unwrap();
//
//         assert_eq!(iterator.next().unwrap().enqueued_message.created_lt, 10);
//         let next = iterator.next().unwrap();
//         assert_eq!(next.enqueued_message.created_lt, 20);
//         assert!(!next.is_new);
//         assert!(iterator.next().is_none());
//         iterator
//             .add_message(Arc::new(EnqueuedMessage {
//                 created_lt: 30,
//                 enqueued_lt: 0,
//                 hash: "somehash3".to_string(),
//                 env: MessageEnvelope {
//                     message: MessageContent {},
//                     from_contract:
//                         "0:46768a917036eb8dc0bf51465f6355cd64eeb3449ba31ae00a18226f65bb675a"
//                             .to_string(),
//                     to_contract:
//                         "0:46768a917036eb8dc0bf51465f6355cd64eeb3449ba31ae00a18226f65bb675a"
//                             .to_string(),
//                 },
//             }))
//             .unwrap();
//
//         assert_eq!(iterator.new_messages.len(), 1);
//         let next = iterator.next().unwrap();
//
//         assert_eq!(next.enqueued_message.created_lt, 30);
//         assert!(next.is_new);
//         assert_eq!(iterator.new_messages.len(), 0);
//
//         iterator
//             .add_message(Arc::new(EnqueuedMessage {
//                 created_lt: 40,
//                 enqueued_lt: 0,
//                 hash: "somehash4".to_string(),
//                 env: MessageEnvelope {
//                     message: MessageContent {},
//                     from_contract:
//                         "0:46768a917036eb8dc0bf51465f6355cd64eeb3449ba31ae00a18226f65bb675a"
//                             .to_string(),
//                     to_contract:
//                         "0:46768a917036eb8dc0bf51465f6355cd64eeb3449ba31ae00a18226f65bb675a"
//                             .to_string(),
//                 },
//             }))
//             .unwrap();
//         assert_eq!(iterator.new_messages.len(), 1);
//         assert_eq!(iterator.messages.len(), 1);
//
//         // checking commit
//         iterator.commit();
//         assert_eq!(iterator.new_messages.len(), 0);
//         assert_eq!(iterator.messages.len(), 1);
//     }
// }
