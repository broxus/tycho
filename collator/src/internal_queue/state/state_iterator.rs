use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashSet};
use std::sync::Arc;

use ahash::HashMapExt;
use anyhow::{Context, Result};
use everscale_types::models::ShardIdent;
use tycho_block_util::queue::QueueKey;
use tycho_storage::owned_iterator::OwnedIterator;
use tycho_util::FastHashMap;

use crate::internal_queue::state::shard_iterator::{IterResult, ShardIterator};
use crate::internal_queue::types::InternalMessageValue;

pub struct ShardIteratorWithRange {
    pub iter: OwnedIterator,
    pub range_start: QueueKey,
    pub range_end: QueueKey,
}

impl ShardIteratorWithRange {
    pub fn new(iter: OwnedIterator, range_start: QueueKey, range_end: QueueKey) -> Self {
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
    shards_to_remove: Vec<ShardIdent>,
}

impl<V: InternalMessageValue> StateIteratorImpl<V> {
    pub fn new(
        shard_iters_with_ranges: FastHashMap<ShardIdent, ShardIteratorWithRange>,
        receiver: ShardIdent,
    ) -> Self {
        let mut iters = FastHashMap::with_capacity(shard_iters_with_ranges.len());

        for (shard_ident, shard_iter_with_range) in shard_iters_with_ranges {
            let shard_iterator = ShardIterator::new(
                shard_ident,
                shard_iter_with_range.range_start,
                shard_iter_with_range.range_end,
                receiver,
                shard_iter_with_range.iter,
            );

            iters.insert(shard_ident, shard_iterator);
        }

        Self {
            iters,
            message_queue: BinaryHeap::new(),
            in_queue: HashSet::new(),
            current_position: Default::default(),
            shards_to_remove: Vec::new(),
        }
    }

    fn refill_queue(&mut self) -> Result<()> {
        self.shards_to_remove.clear();

        for (&shard_ident, iter) in &mut self.iters {
            if self.in_queue.contains(&shard_ident) {
                continue;
            }

            loop {
                match iter.current()? {
                    Some(IterResult::Value(value)) => {
                        let message =
                            V::deserialize(value).context("Failed to deserialize message")?;
                        let message_ext = MessageExt::new(shard_ident, Arc::new(message));
                        self.message_queue.push(Reverse(message_ext));
                        self.in_queue.insert(shard_ident);
                        iter.shift();
                        break;
                    }
                    Some(IterResult::Skip(Some(key))) => {
                        self.current_position
                            .insert(key.shard_ident, key.internal_message_key);
                        iter.shift();
                    }
                    Some(IterResult::Skip(None)) => {
                        iter.shift();
                    }
                    None => {
                        self.shards_to_remove.push(shard_ident);
                        break;
                    }
                }
            }
        }

        for &shard_ident in &self.shards_to_remove {
            self.iters.remove(&shard_ident);
        }

        Ok(())
    }
}

impl<V: InternalMessageValue> StateIterator<V> for StateIteratorImpl<V> {
    fn next(&mut self) -> Result<Option<MessageExt<V>>> {
        self.refill_queue()?;

        if let Some(Reverse(message)) = self.message_queue.pop() {
            let message_key = message.message.key();
            self.current_position.insert(message.source, message_key);

            self.in_queue.remove(&message.source);
            return Ok(Some(message));
        }

        Ok(None)
    }

    fn current_position(&self) -> FastHashMap<ShardIdent, QueueKey> {
        self.current_position.clone()
    }
}
