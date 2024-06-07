use std::collections::BTreeMap;
use std::sync::Arc;

use everscale_types::models::BlockIdShort;

use crate::internal_queue::types::{EnqueuedMessage, InternalMessageKey, QueueDiff};

#[derive(Clone, Default)]
pub struct Shard {
    pub outgoing_messages: BTreeMap<InternalMessageKey, Arc<EnqueuedMessage>>,
    pub diffs: BTreeMap<BlockIdShort, Arc<QueueDiff>>,
}

impl Shard {
    pub fn add_diff(&mut self, diff: Arc<QueueDiff>, block_id_short: BlockIdShort) {
        self.diffs.insert(block_id_short, diff.clone());

        for message in &diff.messages {
            self.outgoing_messages
                .insert(message.key(), message.clone());
        }
    }

    pub fn remove_diff(&mut self, diff_id: &BlockIdShort) -> Option<Arc<QueueDiff>> {
        if let Some(diff) = self.diffs.remove(diff_id) {
            for message in &diff.messages {
                self.outgoing_messages.remove(&message.key());
            }
            return Some(diff);
        }
        None
    }
}
