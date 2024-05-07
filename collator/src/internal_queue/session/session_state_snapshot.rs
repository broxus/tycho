use crate::internal_queue::error::QueueError;
use crate::internal_queue::shard::Shard;
use crate::internal_queue::snapshot::{MessageWithSource, ShardRange, StateSnapshot};
use everscale_types::cell::HashBytes;
use everscale_types::models::ShardIdent;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use tracing::error;

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
            for (_, message) in shard.outgoing_messages.iter().filter(|message| {
                let account_hash = HashBytes::from_str(&message.1.env.to_contract);
                match account_hash {
                    Ok(hash) => {
                        if !shard_id.contains_account(&hash) {
                            return false;
                        }
                    }
                    Err(e) => {
                        error!("failed to convert account to hashbytes {e:?}");
                        return false;
                    }
                }

                if let Some(from_lt) = shard_range.1.from_lt {
                    if message.1.created_lt < from_lt {
                        return false;
                    }
                }
                if let Some(to_lt) = shard_range.1.to_lt {
                    if message.1.created_lt > to_lt {
                        return false;
                    }
                }
                true
            }) {
                let message_with_source = MessageWithSource {
                    shard_id: *shard_range.0,
                    message: message.clone(),
                };
                messages.push(Arc::new(message_with_source));
            }
        }
        Ok(messages)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::internal_queue::types::ext_types_stubs::{
        EnqueuedMessage, MessageContent, MessageEnvelope,
    };

    #[test]
    fn test_get_outgoing_messages_by_shard() {
        let shard_id = ShardIdent::new_full(0);
        let mut shard = Shard::new(shard_id);

        let message1 = EnqueuedMessage {
            created_lt: 10,
            enqueued_lt: 0,
            hash: "somehash".to_string(),
            env: MessageEnvelope {
                message: MessageContent {},
                from_contract: "0:46768a917036eb8dc0bf51465f6355cd64eeb3449ba31ae00a18226f65bb675a"
                    .to_string(),
                to_contract: "0:46768a917036eb8dc0bf51465f6355cd64eeb3449ba31ae00a18226f65bb675a"
                    .to_string(),
            },
        };

        let message2 = EnqueuedMessage {
            created_lt: 20,
            enqueued_lt: 0,
            hash: "somehash2".to_string(),
            env: MessageEnvelope {
                message: MessageContent {},
                from_contract: "0:46768a917036eb8dc0bf51465f6355cd64eeb3449ba31ae00a18226f65bb675a"
                    .to_string(),
                to_contract: "0:46768a917036eb8dc0bf51465f6355cd64eeb3449ba31ae00a18226f65bb675a"
                    .to_string(),
            },
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
}
