use std::sync::Arc;

use anyhow::Result;

use crate::internal_queue::state::state_iterator::{MessageWithSource, StateIterator};

pub struct StatesIteratorsManager {
    iterators: Vec<Box<dyn StateIterator>>,
    current_snapshot: usize,
    message_counts: Vec<usize>, // Add this line to keep track of message counts
}

impl StatesIteratorsManager {
    pub fn new(iterators: Vec<Box<dyn StateIterator>>) -> Self {
        StatesIteratorsManager {
            message_counts: vec![0; iterators.len()], // Initialize the message_counts vector
            iterators,
            current_snapshot: 0,
        }
    }

    /// if first snapshot doesn't have any messages, it will try to get messages from the next snapshot and mark it as the current one
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<Arc<MessageWithSource>>> {
        while self.current_snapshot < self.iterators.len() {
            if let Some(message) = self.iterators[self.current_snapshot].next()? {
                self.message_counts[self.current_snapshot] += 1; // Increment message count for the current iterator
                return Ok(Some(message));
            }
            tracing::debug!(target: "local_debug", "No messages in snapshot {}", self.current_snapshot);
            self.current_snapshot += 1;
        }

        for (index, count) in self.message_counts.iter().enumerate() {
            tracing::debug!(target: "local_debug", "Iterator {} read {} messages", index, count);
        }

        Ok(None)
    }
}
