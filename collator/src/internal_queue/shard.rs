use std::collections::BTreeMap;
use std::sync::Arc;

use everscale_types::models::{BlockIdShort, ShardIdent};

use crate::internal_queue::types::{EnqueuedMessage, InternalMessageKey, QueueDiff};
use crate::tracing_targets;

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
        } else {
            tracing::warn!(target: tracing_targets::MQ, "Diff not found: {:?}", diff_id);
        }
        None
    }

    // stub
    pub fn clear_processed_messages(
        &mut self,
        diff_shard: ShardIdent,
        key: &InternalMessageKey,
    ) -> anyhow::Result<()> {
        let mut keys_to_remove = vec![];

        for (k, v) in self.outgoing_messages.iter() {
            if k > key {
                break;
            }

            let (chain, acc) = v.destination()?;
            // if we proccessed message then delete it
            if chain as i32 == diff_shard.workchain() && diff_shard.contains_account(&acc) {
                keys_to_remove.push(k.clone());
            }
        }

        for k in keys_to_remove {
            self.outgoing_messages.remove(&k);
        }

        Ok(())
    }
}
