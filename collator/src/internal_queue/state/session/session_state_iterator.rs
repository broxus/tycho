use std::collections::VecDeque;
use std::sync::Arc;

use anyhow::Result;
use everscale_types::cell::HashBytes;
use everscale_types::models::ShardIdent;
use tycho_util::FastHashMap;

use crate::internal_queue::shard::Shard;
use crate::internal_queue::state::state_iterator::{MessageWithSource, ShardRange, StateIterator};
use crate::internal_queue::types::InternalMessageKey;
use crate::tracing_targets;

pub struct SessionStateIterator {
    message_queue: VecDeque<Arc<MessageWithSource>>,
}

impl SessionStateIterator {
    pub fn new(
        flat_shards: FastHashMap<ShardIdent, Shard>,
        shard_ranges: &FastHashMap<ShardIdent, ShardRange>,
        shard_id: &ShardIdent,
    ) -> Self {
        let queue = Self::prepare_queue(flat_shards, shard_ranges, shard_id);
        Self {
            message_queue: queue,
        }
    }

    fn prepare_queue(
        flat_shards: FastHashMap<ShardIdent, Shard>,
        shard_ranges: &FastHashMap<ShardIdent, ShardRange>,
        shard_id: &ShardIdent,
    ) -> VecDeque<Arc<MessageWithSource>> {
        let mut message_queue = VecDeque::new();

        for (shard_ident, shard) in flat_shards.iter() {
            if let Some(shard_range) = shard_ranges.get(shard_ident) {
                let from_lt = match shard_range.from_lt {
                    None => 0,
                    Some(from_lt) => from_lt + 1,
                };
                let range_start = InternalMessageKey {
                    lt: from_lt,
                    hash: HashBytes::ZERO,
                };
                let range_end = InternalMessageKey {
                    lt: shard_range.to_lt.unwrap_or(u64::MAX),
                    hash: HashBytes([255; 32]),
                };

                let shard_size = shard.outgoing_messages.len();

                tracing::trace!(
                    target: tracing_targets::MQ,
                    "Shard {} has {} messages",
                    shard_ident,
                    shard_size
                );

                // Perform a range query on the BTreeMap
                let range = shard.outgoing_messages.range(range_start..=range_end);

                for (_, message) in range {
                    if let Ok((workchain, account_hash)) = message.destination() {
                        if shard_id.contains_account(&account_hash)
                            && shard_id.workchain() == workchain as i32
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

        message_queue
    }
}

impl StateIterator for SessionStateIterator {
    fn next(&mut self) -> Result<Option<Arc<MessageWithSource>>> {
        Ok(self.message_queue.pop_front())
    }

    fn peek(&self) -> Result<Option<Arc<MessageWithSource>>> {
        Ok(self.message_queue.front().cloned())
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
