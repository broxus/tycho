use everscale_types::models::{BlockIdShort, ShardIdent};

use crate::internal_queue::types::ext_types_stubs::{EnqueuedMessage, EnqueuedMessageKey};
use std::collections::HashMap;
use std::sync::Arc;

pub struct QueueDiff {
    pub id: BlockIdShort,
    pub messages: Vec<Arc<EnqueuedMessage>>,
    pub processed_upto: HashMap<ShardIdent, EnqueuedMessageKey>,
}

impl QueueDiff {
    pub fn new(id: BlockIdShort) -> Self {
        QueueDiff {
            id,
            messages: Default::default(),
            processed_upto: Default::default(),
        }
    }
}

// STUBS FOR EXTERNAL TYPES
// further we should use types crate

pub mod ext_types_stubs {
    use std::cmp::Ordering;

    pub type Lt = u64;
    pub type MessageHash = UInt256;

    pub struct EnqueuedMessage {
        pub created_lt: Lt,
        pub enqueued_lt: Lt,
        pub hash: MessageHash,
        pub env: MessageEnvelope,
    }

    impl Eq for EnqueuedMessage {}

    impl PartialEq<Self> for EnqueuedMessage {
        fn eq(&self, other: &Self) -> bool {
            self.key() == other.key()
        }
    }

    impl PartialOrd<Self> for EnqueuedMessage {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.key().cmp(&other.key()))
        }
    }

    impl Ord for EnqueuedMessage {
        fn cmp(&self, other: &Self) -> Ordering {
            self.key().cmp(&other.key())
        }
    }

    #[derive(Hash)]
    pub struct MessageEnvelope {
        pub message: MessageContent,
        pub from_contract: Address,
        pub to_contract: Address,
    }

    #[derive(Ord, Eq, PartialEq, PartialOrd, Hash, Clone)]
    pub struct EnqueuedMessageKey {
        pub lt: Lt,
        pub hash: MessageHash,
    }

    impl EnqueuedMessage {
        pub fn key(&self) -> EnqueuedMessageKey {
            EnqueuedMessageKey {
                lt: self.created_lt,
                hash: self.hash.clone(),
            }
        }
    }

    pub type Address = String;
    pub type UInt256 = String;

    #[derive(Hash)]
    pub struct MessageContent {}
}
