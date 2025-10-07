use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};
use std::sync::Arc;

use anyhow::Result;
use tycho_block_util::queue::QueueKey;
use tycho_types::models::ShardIdent;
use tycho_util::FastHashMap;

use crate::internal_queue::state::state_iterator::MessageExt;
use crate::internal_queue::state::states_iterators_manager::StatesIteratorsManager;
use crate::internal_queue::types::message::InternalMessageValue;

pub trait QueueIterator<V: InternalMessageValue>: Send {
    /// Get next message
    fn next(&mut self, with_new: bool) -> Result<Option<IterItem<V>>>;
    fn current_position(&self) -> FastHashMap<ShardIdent, QueueKey>;
    fn process_new_messages(&mut self) -> Result<Option<IterItem<V>>>;
}

pub struct QueueIteratorImpl<V: InternalMessageValue> {
    messages_for_current_shard: BinaryHeap<Reverse<MessageExt<V>>>,
    new_messages: BTreeMap<QueueKey, Arc<V>>,
    iterators_manager: StatesIteratorsManager<V>,
}

impl<V: InternalMessageValue> QueueIteratorImpl<V> {
    pub fn new(iterators_manager: StatesIteratorsManager<V>) -> Result<Self> {
        let messages_for_current_shard = BinaryHeap::default();

        Ok(Self {
            messages_for_current_shard,
            new_messages: Default::default(),
            iterators_manager,
        })
    }
}

pub struct IterItem<V: InternalMessageValue> {
    pub item: MessageExt<V>,
    pub is_new: bool,
}

impl<V: InternalMessageValue> QueueIterator<V> for QueueIteratorImpl<V> {
    fn next(&mut self, with_new: bool) -> Result<Option<IterItem<V>>> {
        // Process the next message from the snapshot manager
        if let Some(next_message) = self.iterators_manager.next()? {
            return Ok(Some(IterItem {
                item: next_message,
                is_new: false,
            }));
        }

        // Process the new messages if required
        if with_new {
            return self.process_new_messages();
        }

        Ok(None)
    }
    fn current_position(&self) -> FastHashMap<ShardIdent, QueueKey> {
        self.iterators_manager.current_position()
    }

    // Function to process the new messages
    fn process_new_messages(&mut self) -> Result<Option<IterItem<V>>> {
        if let Some(next_message) = self.messages_for_current_shard.pop() {
            // remove message from new_messages
            self.new_messages.remove(&next_message.0.message.key());

            return Ok(Some(IterItem {
                item: next_message.0,
                is_new: true,
            }));
        }
        Ok(None)
    }
}
