use std::cmp::{Ordering, Reverse};
use std::collections::hash_map::Entry;
use std::collections::{BinaryHeap, HashSet};
use std::sync::Arc;

use ahash::HashMapExt;
use anyhow::{bail, Context, Result};
use everscale_types::models::ShardIdent;
use tycho_block_util::queue::{QueueKey, QueuePartitionIdx};
use tycho_storage::InternalQueueMessagesIter;
use tycho_util::FastHashMap;

use crate::internal_queue::state::shard_iterator::{IterResult, ShardIterator};
use crate::internal_queue::types::{InternalMessageValue, QueueShardRange};

pub struct ShardIteratorWithRange {
    pub iter: InternalQueueMessagesIter,
    pub range_start: QueueKey,
    pub range_end: QueueKey,
}

impl ShardIteratorWithRange {
    pub fn new(
        iter: InternalQueueMessagesIter,
        range_start: QueueKey,
        range_end: QueueKey,
    ) -> Self {
        ShardIteratorWithRange {
            iter,
            range_start,
            range_end,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MessageExt<V: InternalMessageValue> {
    pub source: ShardIdent,
    pub message: Arc<V>,
}

impl<V: InternalMessageValue> MessageExt<V> {
    pub fn new(source: ShardIdent, message: Arc<V>) -> Self {
        MessageExt { source, message }
    }
}

impl<V: InternalMessageValue> PartialEq for MessageExt<V> {
    fn eq(&self, other: &Self) -> bool {
        self.message == other.message
    }
}

impl<V: InternalMessageValue> Eq for MessageExt<V> {}

impl<V: InternalMessageValue> PartialOrd for MessageExt<V> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.message.cmp(&other.message))
    }
}

impl<V: InternalMessageValue> Ord for MessageExt<V> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.message.cmp(&other.message)
    }
}

#[derive(Debug, Clone)]
pub struct IterRange {
    pub shard_id: ShardIdent,
    pub key: QueueKey,
}

pub trait StateIterator<V: InternalMessageValue>: Send {
    fn next(&mut self) -> Result<Option<MessageExt<V>>>;
    fn current_position(&self) -> FastHashMap<ShardIdent, QueueKey>;
}

pub struct StateIteratorImpl<V: InternalMessageValue> {
    iters: FastHashMap<ShardIdent, ShardIterator>,
    message_queue: BinaryHeap<Reverse<MessageExt<V>>>,
    in_queue: HashSet<ShardIdent>,
    current_position: FastHashMap<ShardIdent, QueueKey>,
    iters_to_remove: Vec<ShardIdent>,
}

impl<V: InternalMessageValue> StateIteratorImpl<V> {
    pub fn new(
        partition: QueuePartitionIdx,
        shard_iters_with_ranges: Vec<(InternalQueueMessagesIter, QueueShardRange)>,
        receiver: ShardIdent,
    ) -> Result<Self> {
        let mut iters = FastHashMap::with_capacity(shard_iters_with_ranges.len());

        for (iter, range) in shard_iters_with_ranges {
            let QueueShardRange {
                shard_ident,
                from,
                to,
            } = range;

            let shard_iterator =
                ShardIterator::new(partition, shard_ident, from, to, receiver, iter);

            match iters.entry(shard_ident) {
                Entry::Occupied(_) => {
                    bail!("Iterator already exists for shard {:?}", shard_ident);
                }
                Entry::Vacant(entry) => {
                    entry.insert(shard_iterator);
                }
            }
        }

        Ok(Self {
            iters,
            message_queue: BinaryHeap::new(),
            in_queue: HashSet::new(),
            current_position: Default::default(),
            iters_to_remove: Vec::new(),
        })
    }

    fn refill_queue(&mut self) -> Result<()> {
        self.iters_to_remove.clear();

        'outer: for (shard_ident, iter) in &mut self.iters {
            if self.in_queue.contains(shard_ident) {
                continue;
            }

            while let Some(msg) = iter.next()? {
                match msg {
                    IterResult::Value(value) => {
                        let message =
                            V::deserialize(value).context("Failed to deserialize message")?;

                        let message_ext = MessageExt::new(*shard_ident, Arc::new(message));

                        self.message_queue.push(Reverse(message_ext));
                        self.in_queue.insert(*shard_ident);
                        continue 'outer;
                    }
                    // skip if we are not receiver for this message
                    IterResult::Skip(Some((shard_partition, queue_key))) => {
                        self.current_position.insert(shard_partition, queue_key);
                    }
                    // skip if it's a first key in range
                    IterResult::Skip(None) => {}
                }
            }

            // remove iterator if it's empty
            self.iters_to_remove.push(*shard_ident);
        }

        for key in &self.iters_to_remove {
            self.iters.remove(key);
        }

        Ok(())
    }
}

impl<V: InternalMessageValue> StateIterator<V> for StateIteratorImpl<V> {
    fn next(&mut self) -> Result<Option<MessageExt<V>>> {
        // refill queue for each shard in range
        self.refill_queue()?;

        // take ordered by lt+hash message from filled queue
        if let Some(Reverse(message)) = self.message_queue.pop() {
            let message_key = message.message.key();
            self.current_position.insert(message.source, message_key);

            // set shard as not in queue for refilling next time
            self.in_queue.remove(&message.source);
            return Ok(Some(message));
        }

        Ok(None)
    }

    fn current_position(&self) -> FastHashMap<ShardIdent, QueueKey> {
        self.current_position.clone()
    }
}
