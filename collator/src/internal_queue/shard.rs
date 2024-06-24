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
        // let mut remove_counter = 0;
        if let Some(diff) = self.diffs.remove(diff_id) {
            for message in &diff.messages {
                let _res = self.outgoing_messages.remove(&message.key());
                // if res.is_some() {
                //     remove_counter += 1;
                // }
            }
            return Some(diff);
        } else {
            panic!("Diff not found: {:?}", diff_id);
        }
    }
}
