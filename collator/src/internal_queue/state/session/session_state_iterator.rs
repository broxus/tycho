use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::ops::Bound;
use std::sync::Arc;

use anyhow::Result;
use everscale_types::models::ShardIdent;
use tycho_util::FastHashMap;

use crate::internal_queue::shard::Shard;
use crate::internal_queue::state::state_iterator::{MessageWithSource, ShardRange, StateIterator};
use crate::internal_queue::types::InternalMessageKey;
use crate::tracing_targets;

pub struct SessionStateIterator {
    message_queue: BinaryHeap<Reverse<Arc<MessageWithSource>>>,
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
    ) -> BinaryHeap<Reverse<Arc<MessageWithSource>>> {
        let mut message_queue = BinaryHeap::new();

        for (shard_ident, shard) in flat_shards.iter() {
            if let Some(shard_range) = shard_ranges.get(shard_ident) {
                let range_start = match shard_range.clone().from {
                    None => Bound::Included(InternalMessageKey::default()),
                    Some(from_lt) => Bound::Excluded(from_lt),
                };

                let range_end =
                    Bound::Included(shard_range.clone().to.unwrap_or(InternalMessageKey::MAX));

                let shard_size = shard.outgoing_messages.len();

                tracing::trace!(
                    target: tracing_targets::MQ,
                    "Full queue has {} messages",
                    shard_size
                );

                // Perform a range query on the BTreeMap
                let range = shard.outgoing_messages.range((range_start, range_end));

                for (_, message) in range {
                    if let Ok((workchain, account_hash)) = message.destination() {
                        if shard_id.contains_account(&account_hash)
                            && shard_id.workchain() == workchain as i32
                        {
                            message_queue.push(Reverse(Arc::new(MessageWithSource::new(
                                *shard_ident,
                                message.clone(),
                            ))));
                        }
                    }
                }
            }
        }

        tracing::info!(
            target: tracing_targets::MQ,
            "Message queue for iterator {} has {} messages",
            shard_id,
            message_queue.len()
        );

        let labels = [("workchain", shard_id.workchain().to_string())];

        metrics::histogram!("tycho_session_iterator_subqueue_size", &labels)
            .record(message_queue.len() as f64);

        message_queue
    }
}

impl StateIterator for SessionStateIterator {
    fn next(&mut self) -> Result<Option<Arc<MessageWithSource>>> {
        Ok(self.message_queue.pop().map(|reverse| reverse.0))
    }

    fn peek(&self) -> Result<Option<Arc<MessageWithSource>>> {
        Ok(self.message_queue.peek().map(|reverse| reverse.clone().0))
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
