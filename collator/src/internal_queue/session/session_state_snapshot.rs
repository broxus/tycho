use std::collections::HashMap;
use std::sync::Arc;

use everscale_types::models::ShardIdent;
use everscale_types::prelude::HashBytes;
use tracing::trace;

use crate::internal_queue::error::QueueError;
use crate::internal_queue::shard::Shard;
use crate::internal_queue::snapshot::{MessageWithSource, ShardRange, StateSnapshot};
use crate::internal_queue::types::EnqueuedMessage;
use crate::tracing_targets;

pub struct SessionStateSnapshot {
    pub flat_shards: HashMap<ShardIdent, Shard>,
}

impl SessionStateSnapshot {
    pub fn new(flat_shards: HashMap<ShardIdent, Shard>) -> Self {
        Self { flat_shards }
    }
}

impl StateSnapshot for SessionStateSnapshot {
    #[tracing::instrument(skip(self), fields(shard_id = ?shard_id))]
    fn get_outgoing_messages_by_shard(
        &self,
        shards: &mut HashMap<ShardIdent, ShardRange>,
        shard_id: &ShardIdent,
    ) -> Result<Vec<Arc<MessageWithSource>>, QueueError> {
        tracing::info!(target: tracing_targets::MQ, "get_outgoing_messages_by_shard: {:?}. checking shards len: {}", shard_id, shards.len());

        let mut messages = vec![];

        for shard_range in shards.iter() {
            tracing::debug!(target: tracing_targets::MQ, "shard_range: {:?}", shard_range);
            let shard = self
                .flat_shards
                .get(shard_range.0)
                .ok_or(QueueError::ShardNotFound(*shard_range.0))?;

            tracing::debug!(target: tracing_targets::MQ, "available messages in shard: {:?} {}", shard_id, shard.outgoing_messages.len());
            for (_, message) in shard.outgoing_messages.iter() {
                tracing::debug!(target: tracing_targets::MQ, "message in queue: {:?}", message);
                let (workchain, account_hash) = message.destination()?;

                if !shard_id.contains_account(&account_hash)
                    || shard_id.workchain() != workchain as i32
                {
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
