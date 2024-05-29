use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use everscale_types::models::ShardIdent;

use crate::internal_queue::shard::Shard;
use crate::internal_queue::snapshot::{MessageWithSource, ShardRange, StateSnapshot};

pub struct SessionStateSnapshot {
    message_queue: VecDeque<Arc<MessageWithSource>>,
}

impl SessionStateSnapshot {
    pub fn new(
        flat_shards: HashMap<ShardIdent, Shard>,
        shard_ranges: &HashMap<ShardIdent, ShardRange>,
        shard_id: &ShardIdent,
    ) -> Self {
        let queue = Self::prepare_queue(flat_shards, shard_ranges, shard_id);
        Self {
            // flat_shards,
            message_queue: queue,
        }
    }

    fn prepare_queue(
        flat_shards: HashMap<ShardIdent, Shard>,
        shard_ranges: &HashMap<ShardIdent, ShardRange>,
        shard_id: &ShardIdent,
    ) -> VecDeque<Arc<MessageWithSource>> {
        let mut message_queue = VecDeque::new();
        if message_queue.is_empty() {
            for (shard_ident, shard) in &flat_shards {
                for (message_key, message) in &shard.outgoing_messages {
                    if let Ok((workchain, account_hash)) = message.destination() {
                        if shard_id.contains_account(&account_hash)
                            && shard_id.workchain() == workchain as i32
                        {
                            if let Some(shard_range) = shard_ranges.get(shard_ident) {
                                if (shard_range.from_lt.is_none()
                                    || message_key.lt
                                        > shard_range.from_lt.unwrap_or(message_key.lt))
                                    && (shard_range.to_lt.is_none()
                                        || message_key.lt
                                            <= shard_range.to_lt.unwrap_or(message_key.lt))
                                {
                                    message_queue.push_back(Arc::new(MessageWithSource::new(
                                        *shard_ident,
                                        message.clone(),
                                    )));
                                }
                            }
                        }
                    }
                }
            }
        }
        message_queue
    }
}

impl StateSnapshot for SessionStateSnapshot {
    fn next(&mut self) -> Option<Arc<MessageWithSource>> {
        self.message_queue.pop_front()
    }

    fn peek(&self) -> Option<Arc<MessageWithSource>> {
        self.message_queue.front().cloned()
    }
}

// #[test]
// fn test_get_outgoing_messages_by_shard() {
//     let shard_id = ShardIdent::new_full(0);
//     let mut shard = Shard::new(shard_id);
//
//     let message1 = EnqueuedMessage {
//         info: Default::default(),
//         cell: Default::default(),
//         hash: Default::default(),
//     };
//
//     let message2 = EnqueuedMessage {
//         info: Default::default(),
//         cell: Default::default(),
//         hash: HashBytes([1; 32]),
//     };
//
//     shard
//         .outgoing_messages
//         .insert(message1.key(), Arc::new(message1));
//     shard
//         .outgoing_messages
//         .insert(message2.key(), Arc::new(message2));
//
//     let mut flat_shards = HashMap::new();
//     flat_shards.insert(shard_id, shard);
//
//     let session_state_snapshot = SessionStateSnapshot::new(flat_shards);
//
//     let mut shards = HashMap::new();
//     let range = ShardRange {
//         shard_id,
//         from_lt: Some(11),
//         to_lt: Some(20),
//     };
//     shards.insert(shard_id, range);
//
//     let result = session_state_snapshot.get_outgoing_messages_by_shard(&mut shards, &shard_id);
//
//     assert!(result.is_ok());
//     let messages = result.unwrap();
//     assert_eq!(messages.len(), 1);
// }
