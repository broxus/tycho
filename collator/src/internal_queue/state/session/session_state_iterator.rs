use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::ops::Bound;
use std::sync::Arc;

use anyhow::Result;
use everscale_types::models::ShardIdent;
use tycho_util::FastHashMap;

use crate::internal_queue::shard::Shard;
use crate::internal_queue::state::state_iterator::{MessageWithSource, ShardRange, StateIterator};
use crate::internal_queue::types::InternalMessageKey;

struct ShardIterator {
    shard_ident: ShardIdent,
    shard_range: ShardRange,
    current_key: Option<InternalMessageKey>,
    shard: Arc<Shard>,
}

impl ShardIterator {
    fn new(shard: Arc<Shard>, shard_ident: ShardIdent, shard_range: ShardRange) -> Self {
        Self {
            shard_ident,
            shard_range: shard_range.clone(),
            current_key: shard_range.from.clone(),
            shard: Arc::clone(&shard),
        }
    }

    fn next_message(&mut self) -> Option<(InternalMessageKey, Arc<MessageWithSource>)> {
        let range_start = match self.current_key.clone() {
            None => Bound::Included(InternalMessageKey::default()),
            Some(ref key) => Bound::Excluded(key.clone()),
        };

        let range_end = Bound::Included(
            self.shard_range
                .to
                .clone()
                .unwrap_or(InternalMessageKey::MAX),
        );

        let messages: Vec<_> = self
            .shard
            .outgoing_messages
            .range((range_start, range_end))
            .take(10)
            .map(|(key, message)| {
                (
                    key.clone(),
                    Arc::new(MessageWithSource::new(self.shard_ident, message.clone())),
                )
            })
            .collect();

        if let Some((key, message)) = messages.first().cloned() {
            self.current_key = Some(key.clone());
            return Some((key, message));
        }

        None
    }
}

pub struct SessionStateIterator {
    flat_shards: FastHashMap<ShardIdent, Arc<Shard>>,
    shard_ranges: FastHashMap<ShardIdent, ShardRange>,
    shard_id: ShardIdent,
    current_iterators: FastHashMap<ShardIdent, ShardIterator>,
    message_queue: BinaryHeap<Reverse<Arc<MessageWithSource>>>,
    iterators_initialized: bool, // Flag to track if iterators have been initialized
}

impl SessionStateIterator {
    pub fn new(
        flat_shards: FastHashMap<ShardIdent, Arc<Shard>>,
        shard_ranges: FastHashMap<ShardIdent, ShardRange>,
        shard_id: ShardIdent,
    ) -> Self {
        Self {
            flat_shards,
            shard_ranges,
            shard_id,
            current_iterators: FastHashMap::default(),
            message_queue: BinaryHeap::new(),
            iterators_initialized: false, // Initialize the flag to false
        }
    }

    fn initialize_iterators(&mut self) {
        if !self.iterators_initialized {
            self.current_iterators = self
                .flat_shards
                .clone()
                .into_iter()
                .filter_map(|(shard_ident, shard)| {
                    self.shard_ranges.get(&shard_ident).map(|shard_range| {
                        let shard_iterator =
                            ShardIterator::new(shard.clone(), shard_ident, shard_range.clone());
                        (shard_ident, shard_iterator)
                    })
                })
                .collect();
            self.iterators_initialized = true; // Set the flag to true after initialization
        }
    }

    fn refill_queue(&mut self) -> bool {
        self.initialize_iterators(); // Ensure iterators are initialized before refilling the queue

        let mut all_iterators_empty = true;

        for iter in self.current_iterators.values_mut() {
            if let Some((_key, message)) = iter.next_message() {
                all_iterators_empty = false;
                if let Ok((workchain, account_hash)) = message.message.destination() {
                    if self.shard_id.contains_account(&account_hash)
                        && self.shard_id.workchain() == workchain as i32
                    {
                        self.message_queue.push(Reverse(message));
                    }
                }
            }
        }

        all_iterators_empty
    }
}

impl StateIterator for SessionStateIterator {
    fn seek(&mut self, _range_start: Option<(&ShardIdent, InternalMessageKey)>) {
        todo!()
    }

    fn next(&mut self) -> Result<Option<Arc<MessageWithSource>>> {
        loop {
            if let Some(Reverse(message)) = self.message_queue.pop() {
                return Ok(Some(message));
            }

            if self.refill_queue() {
                // If refill_queue returns true, all iterators are empty and we break the loop.
                break;
            }
        }

        Ok(None)
    }

    fn peek(&mut self) -> Result<Option<Arc<MessageWithSource>>> {
        // false
        loop {
            if let Some(Reverse(message)) = self.message_queue.peek().cloned() {
                return Ok(Some(message));
            }

            if self.refill_queue() {
                // If refill_queue returns true, all iterators are empty and we break the loop.
                break;
            }
        }

        Ok(None)
    }
}
