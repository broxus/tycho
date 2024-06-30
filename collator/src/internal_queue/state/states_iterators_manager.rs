use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use anyhow::Result;
use everscale_types::models::ShardIdent;

use crate::internal_queue::state::state_iterator::{MessageWithSource, StateIterator};
use crate::internal_queue::types::InternalMessageKey;

pub struct StatesIteratorsManager {
    iterators: Vec<Box<dyn StateIterator>>,
    current_snapshot: usize,
    seen_keys: HashSet<InternalMessageKey>, // Add this line to keep track of seen keys
}

impl StatesIteratorsManager {
    pub fn new(iterators: Vec<Box<dyn StateIterator>>) -> Self {
        StatesIteratorsManager {
            iterators,
            current_snapshot: 0,
            seen_keys: HashSet::new(), // Initialize the seen_keys set
        }
    }

    /// if first snapshot doesn't have any messages, it will try to get messages from the next snapshot and mark it as the current one
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<Arc<MessageWithSource>>> {
        while self.current_snapshot < self.iterators.len() {
            if let Some(message) = self.iterators[self.current_snapshot].next()? {
                return Ok(Some(message));
            }
            tracing::error!(target: "local_debug", "No messages in snapshot {}", self.current_snapshot);
            self.current_snapshot += 1;
        }
        Ok(None)
    }

    /// Load read upto from all iterators
    pub fn get_iter_upto(&self) -> BTreeMap<ShardIdent, InternalMessageKey> {
        let mut processed_upto = BTreeMap::default();
        for (index, iterator) in self.iterators.iter().enumerate() {
            for read_upto in iterator.get_iter_upto() {
                if let Some(read_upto_value) = processed_upto.get(&read_upto.0) {
                    if read_upto_value < &read_upto.1 {
                        processed_upto.insert(read_upto.0, read_upto.1);
                    }
                } else {
                    processed_upto.insert(read_upto.0, read_upto.1);
                }
            }
        }

        processed_upto
    }
}
