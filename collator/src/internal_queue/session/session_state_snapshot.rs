use std::collections::HashMap;
use std::sync::Arc;

use everscale_types::models::ShardIdent;
use everscale_types::prelude::HashBytes;

use crate::internal_queue::error::QueueError;
use crate::internal_queue::shard::Shard;
use crate::internal_queue::snapshot::{MessageWithSource, ShardRange, StateSnapshot};
use crate::internal_queue::types::EnqueuedMessage;

pub struct SessionStateSnapshot {
    pub flat_shards: HashMap<ShardIdent, Shard>,
}

impl SessionStateSnapshot {
    pub fn new(flat_shards: HashMap<ShardIdent, Shard>) -> Self {
        Self { flat_shards }
    }
}

impl StateSnapshot for SessionStateSnapshot {
    fn get_outgoing_messages_by_shard(
        &self,
        shards: &mut HashMap<ShardIdent, ShardRange>,
        shard_id: &ShardIdent,
    ) -> Result<Vec<Arc<MessageWithSource>>, QueueError> {
        let mut messages = vec![];

        for shard_range in shards.iter() {
            let shard = self
                .flat_shards
                .get(shard_range.0)
                .ok_or(QueueError::ShardNotFound(*shard_range.0))?;

            for (_, message) in shard.outgoing_messages.iter() {
                let account_hash = message.destination()?;

                if !shard_id.contains_account(&account_hash) {
                    continue;
                }

                if let Some(from_lt) = shard_range.1.from_lt {
                    if message.info.created_lt < from_lt {
                        continue;
                    }
                }
                if let Some(to_lt) = shard_range.1.to_lt {
                    if message.info.created_lt > to_lt {
                        continue;
                    }
                }

                let message_with_source = MessageWithSource {
                    shard_id: *shard_range.0,
                    message: message.clone(),
                };
                messages.push(Arc::new(message_with_source));
            }
        }
        Ok(messages)
    }

    fn next(
        &self,
        _shard_range: &HashMap<ShardIdent, ShardRange>,
        _for_shard: &ShardIdent,
    ) -> Option<Arc<MessageWithSource>> {
        todo!()
    }
}

#[test]
fn test_get_outgoing_messages_by_shard() {
    let shard_id = ShardIdent::new_full(0);
    let mut shard = Shard::new(shard_id);

    let message1 = EnqueuedMessage {
        info: Default::default(),
        cell: Default::default(),
        hash: Default::default(),
    };

    let message2 = EnqueuedMessage {
        info: Default::default(),
        cell: Default::default(),
        hash: HashBytes([1; 32]),
    };

    shard
        .outgoing_messages
        .insert(message1.key(), Arc::new(message1));
    shard
        .outgoing_messages
        .insert(message2.key(), Arc::new(message2));

    let mut flat_shards = HashMap::new();
    flat_shards.insert(shard_id, shard);

    let session_state_snapshot = SessionStateSnapshot::new(flat_shards);

    let mut shards = HashMap::new();
    let range = ShardRange {
        shard_id,
        from_lt: Some(11),
        to_lt: Some(20),
    };
    shards.insert(shard_id, range);

    let result = session_state_snapshot.get_outgoing_messages_by_shard(&mut shards, &shard_id);

    assert!(result.is_ok());
    let messages = result.unwrap();
    assert_eq!(messages.len(), 1);
}
