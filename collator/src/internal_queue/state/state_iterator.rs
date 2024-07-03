use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet};
use std::hash::Hash;
use std::sync::Arc;

use anyhow::Result;
use everscale_types::cell::HashBytes;
use everscale_types::models::ShardIdent;
use tycho_storage::owned_iterator::OwnedIterator;
use tycho_storage::ShardsInternalMessagesKey;
use tycho_util::FastHashMap;

use crate::internal_queue::state::shard_iterator::ShardIterator;
use crate::internal_queue::types::{EnqueuedMessage, InternalMessageKey};

#[derive(Debug, Clone, Eq)]
pub struct MessageWithSource {
    pub shard_id: ShardIdent,
    pub message: Arc<EnqueuedMessage>,
}

impl MessageWithSource {
    pub fn new(shard_id: ShardIdent, message: Arc<EnqueuedMessage>) -> Self {
        MessageWithSource { shard_id, message }
    }
}

impl PartialEq<Self> for MessageWithSource {
    fn eq(&self, other: &Self) -> bool {
        self.message == other.message
    }
}

impl PartialOrd<Self> for MessageWithSource {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.message.cmp(&other.message))
    }
}

impl Ord for MessageWithSource {
    fn cmp(&self, other: &Self) -> Ordering {
        self.message.cmp(&other.message)
    }
}

#[derive(Debug, Clone)]
pub struct IterRange {
    pub shard_id: ShardIdent,
    pub key: InternalMessageKey,
}

#[derive(Debug, Clone)]
pub struct ShardRange {
    pub shard_id: ShardIdent,
    pub from: Option<InternalMessageKey>,
    pub to: Option<InternalMessageKey>,
}

pub trait StateIterator: Send {
    fn next(&mut self) -> Result<Option<(ShardIdent, InternalMessageKey, i8, HashBytes, Vec<u8>)>>;
}

pub struct StateIteratorImpl {
    iters: BTreeMap<ShardIdent, ShardIterator>,
    receiver: ShardIdent,
    ranges: FastHashMap<ShardIdent, ShardRange>,
    message_queue: BTreeMap<ShardsInternalMessagesKey, (i8, HashBytes, Vec<u8>)>,
    in_queue: HashSet<ShardIdent>,
}

impl StateIteratorImpl {
    pub fn new(
        shard_iters: BTreeMap<ShardIdent, OwnedIterator>,
        receiver: ShardIdent,
        ranges: FastHashMap<ShardIdent, ShardRange>,
    ) -> Self {
        let mut iters = BTreeMap::new();
        for (shard_ident, iter) in shard_iters.into_iter() {
            let range = ranges
                .get(&shard_ident)
                .expect("Failed to find range for shard");
            let shard_iterator =
                ShardIterator::new(shard_ident, range.clone(), receiver.clone(), iter);
            iters.insert(shard_ident, shard_iterator);
        }
        Self {
            iters,
            receiver,
            ranges,
            message_queue: BTreeMap::new(),
            in_queue: HashSet::new(),
        }
    }

    fn refill_queue_if_needed(&mut self) {
        for (shard_ident, iter) in self.iters.iter_mut() {
            if !self.in_queue.contains(shard_ident) {
                if let Some((key, dest_workchain, dest_address, cell_bytes)) = iter.next_message() {
                    self.message_queue
                        .insert(key, (dest_workchain, dest_address, cell_bytes));
                    iter.iterator.next();
                }
            }
        }
    }
}

impl StateIterator for StateIteratorImpl {
    fn next(&mut self) -> Result<Option<(ShardIdent, InternalMessageKey, i8, HashBytes, Vec<u8>)>> {
        self.refill_queue_if_needed();

        if let Some((key, (dest_workchain, dest_address, cell_bytes))) =
            self.message_queue.pop_first()
        {
            self.in_queue.remove(&key.shard_ident);
            let source = key.shard_ident;
            let key = InternalMessageKey {
                lt: key.lt,
                hash: key.hash,
            };
            return Ok(Some((
                source,
                key,
                dest_workchain,
                dest_address,
                cell_bytes,
            )));
        }
        Ok(None)
    }
}
