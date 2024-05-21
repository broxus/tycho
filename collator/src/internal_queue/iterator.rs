use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::sync::Arc;

use anyhow::{bail, Result};
use everscale_types::models::{BlockIdShort, IntAddr, ShardIdent};

use crate::internal_queue::error::QueueError;
use crate::internal_queue::snapshot::{IterRange, MessageWithSource, ShardRange, StateSnapshot};
use crate::internal_queue::types::{EnqueuedMessage, InternalMessageKey, Lt, QueueDiff};
pub trait QueueIterator: Send {
    fn next(&mut self, with_new: bool) -> Option<IterItem>;
    fn take_diff(&mut self, block_id_short: BlockIdShort) -> QueueDiff;
    fn commit_processed_messages(
        &mut self,
        messages: Vec<(ShardIdent, InternalMessageKey)>,
    ) -> Result<()>;
    fn add_message(&mut self, message: Arc<EnqueuedMessage>) -> Result<()>;
}

pub struct QueueIteratorImpl {
    for_block: ShardIdent,
    current_position: HashMap<ShardIdent, InternalMessageKey>,
    commited_current_position: HashMap<ShardIdent, InternalMessageKey>,
    messages: BinaryHeap<Reverse<Arc<MessageWithSource>>>,
    new_messages: HashMap<InternalMessageKey, Arc<EnqueuedMessage>>,
}

impl QueueIteratorImpl {
    pub fn new(
        shards_from: Vec<IterRange>,
        shards_to: Vec<IterRange>,
        snapshots: Vec<Box<impl StateSnapshot + ?Sized>>,
        for_block: ShardIdent,
    ) -> Result<Self, QueueError> {
        let shards_with_ranges: &mut HashMap<ShardIdent, ShardRange> = &mut HashMap::new();
        for from in &shards_from {
            for to in &shards_to {
                Self::traverse_and_collect_ranges(shards_with_ranges, from, to);
            }
        }

        let mut messages = BinaryHeap::default();

        for snapshot in snapshots {
            let snapshot_messages =
                snapshot.get_outgoing_messages_by_shard(shards_with_ranges, &for_block)?;
            for snapshot_message in snapshot_messages {
                messages.push(Reverse(snapshot_message));
            }
        }

        Ok(Self {
            for_block,
            messages,
            current_position: Default::default(),
            new_messages: Default::default(),
            commited_current_position: Default::default(),
        })
    }
}

pub struct IterItem {
    pub message_with_source: Arc<MessageWithSource>,
    pub is_new: bool,
}

fn update_shard_range(
    touched_shards: &mut HashMap<ShardIdent, ShardRange>,
    shard_id: ShardIdent,
    from_lt: Option<Lt>,
    to_lt: Option<Lt>,
) {
    touched_shards
        .entry(shard_id)
        .or_insert_with(|| ShardRange {
            shard_id,
            from_lt,
            to_lt,
        });
}

impl QueueIteratorImpl {
    fn traverse_and_collect_ranges(
        touched_shards: &mut HashMap<ShardIdent, ShardRange>,
        from_range: &IterRange,
        to_range: &IterRange,
    ) {
        if from_range.shard_id == to_range.shard_id
            || from_range.shard_id.intersects(&to_range.shard_id)
        {
            update_shard_range(
                touched_shards,
                from_range.shard_id,
                Some(from_range.lt),
                Some(to_range.lt),
            );
        } else if from_range.shard_id.is_parent_of(&to_range.shard_id)
            || from_range.shard_id.is_child_of(&to_range.shard_id)
        {
            update_shard_range(
                touched_shards,
                from_range.shard_id,
                Some(from_range.lt),
                None,
            );
            update_shard_range(touched_shards, to_range.shard_id, None, Some(to_range.lt));
        }

        if let Some(common_ancestor) = find_common_ancestor(from_range.shard_id, to_range.shard_id)
        {
            update_shard_range(
                touched_shards,
                from_range.shard_id,
                Some(from_range.lt),
                None,
            );
            update_shard_range(touched_shards, to_range.shard_id, None, Some(to_range.lt));

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

impl QueueIterator for QueueIteratorImpl {
    fn next(&mut self, with_new: bool) -> Option<IterItem> {
        if let Some(message_with_source) = self.messages.peek() {
            let message_key = message_with_source.0.message.key();

            let is_new = self.new_messages.contains_key(&message_key);

            if with_new || !is_new {
                let message_with_source = self.messages.pop()?.0;
                self.new_messages.remove(&message_key);
                return Some(IterItem {
                    message_with_source,
                    is_new,
                });
            } else {
                return None;
            }
        }
        None
    }

    fn take_diff(&mut self, block_id_short: BlockIdShort) -> QueueDiff {
        let mut diff = QueueDiff::new(block_id_short);
        for (shard_id, lt) in self.commited_current_position.iter() {
            diff.processed_upto.insert(*shard_id, lt.clone());
        }
        for message in self.new_messages.values() {
            diff.messages.push(message.clone());
        }

        self.current_position
            .clone_from(&self.commited_current_position);
        self.commited_current_position.clear();
        self.new_messages.clear();

        diff
    }

    fn commit_processed_messages(
        &mut self,
        messages: Vec<(ShardIdent, InternalMessageKey)>,
    ) -> Result<()> {
        if !self.commited_current_position.is_empty() {
            bail!("Already commited. Please take diff before commit again")
        }

        for message in messages {
            // insert only if key greater then current
            if let Some(current_key) = self.commited_current_position.get(&message.0) {
                if message.1 > *current_key {
                    self.commited_current_position.insert(message.0, message.1);
                }
            } else {
                self.commited_current_position.insert(message.0, message.1);
            }
        }
        Ok(())
    }

    fn add_message(&mut self, message: Arc<EnqueuedMessage>) -> Result<()> {
        self.new_messages.insert(message.key(), message.clone());

        let message_destination = match &message.info.dst {
            IntAddr::Std(addr) => addr.address,
            IntAddr::Var(_) => {
                bail!("VarAddr is not supported")
            }
        };

        if self.for_block.contains_account(&message_destination) {
            let message_with_source = MessageWithSource::new(self.for_block, message.clone());
            self.messages.push(Reverse(Arc::new(message_with_source)));
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
