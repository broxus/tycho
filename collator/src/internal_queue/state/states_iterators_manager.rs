use std::sync::Arc;

use anyhow::Result;

use crate::internal_queue::state::state_iterator::{MessageWithSource, StateIterator};

pub struct StatesIteratorsManager {
    iterators: Vec<Box<dyn StateIterator>>,
    current_snapshot: usize,
}

impl StatesIteratorsManager {
    pub fn new(iterators: Vec<Box<dyn StateIterator>>) -> Self {
        StatesIteratorsManager {
            iterators,
            current_snapshot: 0,
        }
    }

    /// if first snapshot doesn't have any messages, it will try to get messages from the next snapshot and mark it as the current one
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<Arc<MessageWithSource>>> {
        while self.current_snapshot < self.iterators.len() {
            if let Some(message) = self.iterators[self.current_snapshot].next()? {
                return Ok(Some(message));
            }
            self.current_snapshot += 1;
        }
        Ok(None)
    }

    /// peek the next message without moving the cursor
    pub fn peek(&mut self) -> Result<Option<Arc<MessageWithSource>>> {
        let mut current_snapshot = self.current_snapshot;
        while current_snapshot < self.iterators.len() {
            if let Some(message) = self.iterators[current_snapshot].peek()? {
                return Ok(Some(message));
            }
            current_snapshot += 1;
        }
        Ok(None)
    }
}
