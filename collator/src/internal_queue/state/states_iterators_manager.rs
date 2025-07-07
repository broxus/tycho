use anyhow::Result;
use tycho_block_util::queue::QueueKey;
use tycho_types::models::ShardIdent;
use tycho_util::FastHashMap;

use crate::internal_queue::state::state_iterator::{MessageExt, StateIterator};
use crate::internal_queue::types::InternalMessageValue;

pub struct StatesIteratorsManager<V: InternalMessageValue> {
    iterator: Box<dyn StateIterator<V>>,
}

impl<V: InternalMessageValue> StatesIteratorsManager<V> {
    pub fn new(iterator: Box<dyn StateIterator<V>>) -> Self {
        StatesIteratorsManager { iterator }
    }

    /// if first snapshot doesn't have any messages, it will try to get messages from the next snapshot and mark it as the current one
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<MessageExt<V>>> {
        self.iterator.next()
    }

    pub fn current_position(&self) -> FastHashMap<ShardIdent, QueueKey> {
        self.iterator.current_position()
    }
}
