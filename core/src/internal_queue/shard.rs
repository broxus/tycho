use crate::internal_queue::types::ext_types_stubs::{EnqueuedMessage, EnqueuedMessageKey};
use crate::internal_queue::types::QueueDiff;
use everscale_types::models::{BlockIdShort, ShardIdent};
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Clone)]
pub struct Shard {
    pub(crate) outgoing_messages: BTreeMap<EnqueuedMessageKey, Arc<EnqueuedMessage>>,
    pub(crate) diffs: BTreeMap<BlockIdShort, Arc<QueueDiff>>,
}

impl Shard {
    pub(crate) fn new(_id: ShardIdent) -> Self {
        Shard {
            outgoing_messages: BTreeMap::new(),
            diffs: BTreeMap::new(),
        }
    }

    pub fn add_diff(&mut self, diff: Arc<QueueDiff>) {
        self.diffs.insert(diff.id, diff.clone());

        for message in &diff.messages {
            self.outgoing_messages
                .insert(message.key(), message.clone());
        }
    }

    pub(crate) fn remove_diff(&mut self, diff_id: &BlockIdShort) -> Option<Arc<QueueDiff>> {
        if let Some(diff) = self.diffs.remove(diff_id) {
            for message in &diff.messages {
                self.outgoing_messages.remove(&message.key());
            }
            return Some(diff);
        }
        None
    }
}
