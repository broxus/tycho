use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, BinaryHeap, HashSet};
use std::sync::Arc;

use anyhow::{Context, Result};
use everscale_types::models::ShardIdent;
use tycho_storage::owned_iterator::OwnedIterator;
use tycho_util::FastHashMap;

use crate::internal_queue::state::shard_iterator::{IterResult, ShardIterator};
use crate::internal_queue::types::{InternalMessageKey, InternalMessageValue};

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
    pub key: InternalMessageKey,
}

pub trait StateIterator<V: InternalMessageValue>: Send {
    fn next(&mut self) -> Result<Option<MessageExt<V>>>;
    fn current_position(&self) -> FastHashMap<ShardIdent, InternalMessageKey>;
}

pub struct StateIteratorImpl<V: InternalMessageValue> {
    iters: BTreeMap<ShardIdent, ShardIterator>,
    message_queue: BinaryHeap<Reverse<MessageExt<V>>>,
    in_queue: HashSet<ShardIdent>,
}

impl<V: InternalMessageValue> StateIteratorImpl<V> {
    pub fn new(
        shard_iters: BTreeMap<ShardIdent, OwnedIterator>,
        receiver: ShardIdent,
        ranges: FastHashMap<ShardIdent, (InternalMessageKey, InternalMessageKey)>,
    ) -> Self {
        let mut iters = BTreeMap::new();
        for (shard_ident, iter) in shard_iters {
            let range = ranges
                .get(&shard_ident)
                .expect("Failed to find range for shard");

            let shard_iterator = ShardIterator::new(
                shard_ident,
                range.0.clone(),
                range.1.clone(),
                receiver,
                iter,
            );
            iters.insert(shard_ident, shard_iterator);
        }
        Self {
            iters,
            message_queue: BinaryHeap::new(),
            in_queue: HashSet::new(),
        }
    }

    fn refill_queue_if_needed(&mut self) -> Result<()> {
        for (shard_ident, iter) in self.iters.iter_mut() {
            if !self.in_queue.contains(shard_ident) {
                while let Ok(result) = iter.next() {
                    match result {
                        IterResult::Value(value) => {
                            let message =
                                V::deserialize(value).context("Failed to deserialize message")?;
                            let message_ext = MessageExt::new(*shard_ident, Arc::new(message));
                            self.message_queue.push(Reverse(message_ext));
                            self.in_queue.insert(*shard_ident);
                            iter.shift_position();
                            break;
                        }
                        IterResult::Skip => {
                            iter.shift_position();
                            continue;
                        }
                        IterResult::End => {
                            break;
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

impl<V: InternalMessageValue> StateIterator<V> for StateIteratorImpl<V> {
    fn next(&mut self) -> Result<Option<MessageExt<V>>> {
        self.refill_queue_if_needed()?;

        if let Some(Reverse(message)) = self.message_queue.pop() {
            self.in_queue.remove(&message.source);
            return Ok(Some(message));
        }

        Ok(None)
    }

    fn current_position(&self) -> FastHashMap<ShardIdent, InternalMessageKey> {
        let mut result = FastHashMap::default();
        for (shard_ident, iter) in self.iters.iter() {
            if let Some(key) = iter.current_position() {
                result.insert(*shard_ident, key);
            }
        }
        result
    }
}
