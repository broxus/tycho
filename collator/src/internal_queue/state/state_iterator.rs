use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, BinaryHeap, HashSet};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use everscale_types::cell::{Cell, Load};
use everscale_types::models::{IntMsgInfo, Message, MsgInfo, ShardIdent};
use tycho_storage::owned_iterator::OwnedIterator;
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
    fn get_iter_upto(&self) -> FastHashMap<ShardIdent, InternalMessageKey>;
    fn next(&mut self) -> Result<Option<Arc<MessageWithSource>>>;
    fn peek(&mut self) -> Result<Option<Arc<MessageWithSource>>>;
}

pub struct StateIteratorImpl {
    iters: BTreeMap<ShardIdent, ShardIterator>,
    receiver: ShardIdent,
    ranges: FastHashMap<ShardIdent, ShardRange>,
    message_queue: BinaryHeap<Reverse<Arc<MessageWithSource>>>,
    in_queue: HashSet<ShardIdent>,
    saved_queue: BinaryHeap<Reverse<Arc<MessageWithSource>>>,
    saved_in_queue: HashSet<ShardIdent>,
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
            message_queue: BinaryHeap::new(),
            in_queue: HashSet::new(),
            saved_queue: Default::default(),
            saved_in_queue: Default::default(),
        }
    }

    fn load_message_from_cell(cell: Cell) -> Result<(IntMsgInfo, Cell)> {
        let message = Message::load_from(&mut cell.as_slice().context("failed to load message")?)?;
        match message.info {
            MsgInfo::Int(info) => Ok((info, cell)),
            _ => bail!("Expected internal message"),
        }
    }

    fn create_message_with_source(
        info: IntMsgInfo,
        cell: Cell,
        shard: ShardIdent,
    ) -> Arc<MessageWithSource> {
        let hash = *cell.repr_hash();
        let enqueued_message = EnqueuedMessage { info, cell, hash };
        Arc::new(MessageWithSource::new(shard, Arc::new(enqueued_message)))
    }

    fn refill_queue_if_needed(&mut self) {
        for (shard_ident, iter) in self.iters.iter_mut() {
            if !self.in_queue.contains(shard_ident) {
                if let Some((_key, cell)) = iter.next_message() {
                    if let Ok((info, cell)) = Self::load_message_from_cell(cell) {
                        let message_with_source =
                            Self::create_message_with_source(info, cell, iter.shard_ident);

                        for key in self.message_queue.iter() {
                            if key.0.message.key() == message_with_source.message.key() {
                                panic!("Duplicate message in the queue");
                            }
                        }

                        self.message_queue.push(Reverse(message_with_source));
                        self.in_queue.insert(shard_ident.clone());
                    } else {
                        panic!("Failed to load message from value")
                    }
                    iter.iterator.next();
                }
            }
        }
    }
}

impl StateIterator for StateIteratorImpl {
    fn get_iter_upto(&self) -> FastHashMap<ShardIdent, InternalMessageKey> {
        let mut processed_upto = FastHashMap::default();
        for (shard_ident, iter) in self.iters.iter() {
            if let Some(key) = iter.read_until.clone() {
                processed_upto.insert(shard_ident.clone(), key);
            }
        }
        processed_upto
    }

    fn next(&mut self) -> Result<Option<Arc<MessageWithSource>>> {
        self.refill_queue_if_needed(); // Refill the queue only if needed

        if let Some(Reverse(message)) = self.message_queue.pop() {
            // self.iters.get_mut(&message.shard_id).unwrap().read_until = Some(message.message.key());
            self.in_queue.remove(&message.shard_id);
            return Ok(Some(message));
        }

        Ok(None)
    }

    fn peek(&mut self) -> Result<Option<Arc<MessageWithSource>>> {
        // Save the current state before refilling the queue
        for iter in self.iters.values_mut() {
            iter.save_position();
        }

        // Save the current queue state
        self.saved_queue = self.message_queue.clone();
        self.saved_in_queue = self.in_queue.clone();

        // Temporarily refill the queue if needed
        self.refill_queue_if_needed();

        // Peek the next message without popping it from the queue
        let result = if let Some(Reverse(message)) = self.message_queue.peek().cloned() {
            Ok(Some(message))
        } else {
            Ok(None)
        };

        // Restore the iterator positions to the saved state
        for iter in self.iters.values_mut() {
            iter.restore_position();
        }

        // Restore the queue state
        self.message_queue = self.saved_queue.clone();
        self.in_queue = self.saved_in_queue.clone();

        // Clear the temporary saved state
        self.saved_queue.clear();
        self.saved_in_queue.clear();

        result
    }
}
