use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::str::FromStr;
use std::sync::Arc;

use everscale_types::cell::HashBytes;
use everscale_types::models::{BlockIdShort, ShardIdent};

use crate::internal_queue::error::QueueError;
use crate::internal_queue::snapshot::{IterRange, MessageWithSource, ShardRange, StateSnapshot};
use crate::internal_queue::types::ext_types_stubs::{EnqueuedMessage, EnqueuedMessageKey, Lt};
use crate::internal_queue::types::QueueDiff;

pub trait QueueIterator {
    fn next(&mut self) -> Option<IterItem>;
    fn commit(&mut self);
    fn get_diff(&self, block_id_short: BlockIdShort) -> QueueDiff;

    fn add_message(&mut self, message: Arc<EnqueuedMessage>) -> anyhow::Result<()>;
}

pub struct QueueIteratorImpl {
    for_block: ShardIdent,
    current_position: HashMap<ShardIdent, EnqueuedMessageKey>,
    messages: BinaryHeap<Reverse<Arc<MessageWithSource>>>,
    new_messages: HashMap<EnqueuedMessageKey, Arc<EnqueuedMessage>>,
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
        })
    }
}

pub struct IterItem {
    pub message: Arc<EnqueuedMessage>,
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
    fn next(&mut self) -> Option<IterItem> {
        let message = self.messages.pop();
        match message {
            Some(message_with_source) => {
                let message = message_with_source.0.message.clone();
                self.current_position.insert(
                    message_with_source.0.shard_id,
                    message_with_source.0.message.key(),
                );
                let is_new = self.new_messages.remove(&message.key()).is_some();
                let message = IterItem { message, is_new };
                Some(message)
            }
            None => None,
        }
    }

    fn commit(&mut self) {
        self.new_messages.clear();
    }

    fn get_diff(&self, block_id_short: BlockIdShort) -> QueueDiff {
        let mut diff = QueueDiff::new(block_id_short);
        for (shard_id, lt) in self.current_position.iter() {
            diff.processed_upto.insert(*shard_id, lt.clone());
        }
        for message in self.new_messages.values() {
            diff.messages.push(message.clone());
        }
        diff
    }

    fn add_message(&mut self, message: Arc<EnqueuedMessage>) -> anyhow::Result<()> {
        self.new_messages.insert(message.key(), message.clone());
        let bytes = HashBytes::from_str(&message.env.to_contract)?;

        if self.for_block.contains_account(&bytes) {
            self.messages.push(Reverse(Arc::new(MessageWithSource {
                shard_id: self.for_block,
                message: message.clone(),
            })));
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::internal_queue::session::session_state_snapshot::SessionStateSnapshot;
    use crate::internal_queue::shard::Shard;
    use crate::internal_queue::types::ext_types_stubs::{MessageContent, MessageEnvelope};

    fn mock_snapshot() -> Box<dyn StateSnapshot> {
        let shard_id = ShardIdent::new_full(0);
        let mut shard = Shard::new(shard_id);
        let split = shard_id.split().unwrap();
        let shard2 = Shard::new(split.1);
        let shard1 = Shard::new(split.0);

        let message1 = EnqueuedMessage {
            created_lt: 10,
            enqueued_lt: 0,
            hash: "somehash".to_string(),
            env: MessageEnvelope {
                message: MessageContent {},
                from_contract: "0:46768a917036eb8dc0bf51465f6355cd64eeb3449ba31ae00a18226f65bb675a"
                    .to_string(),
                to_contract: "0:46768a917036eb8dc0bf51465f6355cd64eeb3449ba31ae00a18226f65bb675a"
                    .to_string(),
            },
        };

        let message2 = EnqueuedMessage {
            created_lt: 20,
            enqueued_lt: 0,
            hash: "somehash2".to_string(),
            env: MessageEnvelope {
                message: MessageContent {},
                from_contract: "0:46768a917036eb8dc0bf51465f6355cd64eeb3449ba31ae00a18226f65bb675a"
                    .to_string(),
                to_contract: "0:46768a917036eb8dc0bf51465f6355cd64eeb3449ba31ae00a18226f65bb675a"
                    .to_string(),
            },
        };

        shard
            .outgoing_messages
            .insert(message1.key(), Arc::new(message1));
        shard
            .outgoing_messages
            .insert(message2.key(), Arc::new(message2));

        let mut flat_shards = HashMap::new();
        flat_shards.insert(shard_id, shard);
        flat_shards.insert(split.1, shard2);
        flat_shards.insert(split.0, shard1);

        let session_state_snapshot = SessionStateSnapshot::new(flat_shards);

        Box::new(session_state_snapshot)
    }

    #[test]
    fn initialization_and_basic_iteration() {
        let shard = ShardIdent::new_full(0);
        let split = shard.split().unwrap();

        let shards_from = vec![IterRange {
            shard_id: shard,
            lt: 0,
        }];
        let _shards_to = vec![IterRange {
            shard_id: split.0,
            lt: 20,
        }];
        let shards_to = vec![IterRange {
            shard_id: split.1,
            lt: 20,
        }];

        let snapshots = vec![mock_snapshot()]; // You would need mock snapshots here

        let for_block = split.0;
        let mut iterator =
            QueueIteratorImpl::new(shards_from, shards_to, snapshots, for_block).unwrap();

        assert_eq!(iterator.next().unwrap().message.created_lt, 10);
        let next = iterator.next().unwrap();
        assert_eq!(next.message.created_lt, 20);
        assert!(!next.is_new);
        assert!(iterator.next().is_none());
        iterator
            .add_message(Arc::new(EnqueuedMessage {
                created_lt: 30,
                enqueued_lt: 0,
                hash: "somehash3".to_string(),
                env: MessageEnvelope {
                    message: MessageContent {},
                    from_contract:
                        "0:46768a917036eb8dc0bf51465f6355cd64eeb3449ba31ae00a18226f65bb675a"
                            .to_string(),
                    to_contract:
                        "0:46768a917036eb8dc0bf51465f6355cd64eeb3449ba31ae00a18226f65bb675a"
                            .to_string(),
                },
            }))
            .unwrap();

        assert_eq!(iterator.new_messages.len(), 1);
        let next = iterator.next().unwrap();

        assert_eq!(next.message.created_lt, 30);
        assert!(next.is_new);
        assert_eq!(iterator.new_messages.len(), 0);

        iterator
            .add_message(Arc::new(EnqueuedMessage {
                created_lt: 40,
                enqueued_lt: 0,
                hash: "somehash4".to_string(),
                env: MessageEnvelope {
                    message: MessageContent {},
                    from_contract:
                        "0:46768a917036eb8dc0bf51465f6355cd64eeb3449ba31ae00a18226f65bb675a"
                            .to_string(),
                    to_contract:
                        "0:46768a917036eb8dc0bf51465f6355cd64eeb3449ba31ae00a18226f65bb675a"
                            .to_string(),
                },
            }))
            .unwrap();
        assert_eq!(iterator.new_messages.len(), 1);
        assert_eq!(iterator.messages.len(), 1);

        // checking commit
        iterator.commit();
        assert_eq!(iterator.new_messages.len(), 0);
        assert_eq!(iterator.messages.len(), 1);
    }
}
