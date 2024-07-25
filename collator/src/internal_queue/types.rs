use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, BinaryHeap};
use std::sync::Arc;

use everscale_types::cell::{Cell, HashBytes};
use everscale_types::models::{IntAddr, IntMsgInfo, ShardIdent};

use super::state::state_iterator::MessageWithSource;

pub type Lt = u64;

#[derive(Default, Debug, Clone)]
pub struct QueueDiff {
    pub messages: BTreeMap<InternalMessageKey, Arc<EnqueuedMessage>>,
    pub processed_upto: BTreeMap<ShardIdent, InternalMessageKey>,
    pub keys: Vec<InternalMessageKey>,
}

impl QueueDiff {
    pub fn save_keys(&mut self) {
        self.keys = self.messages.keys().cloned().collect();
        self.messages.clear();
    }
}

pub struct QueueFullDiff {
    pub diff: QueueDiff,
    pub messages_for_current_shard: BinaryHeap<Reverse<Arc<MessageWithSource>>>,
}

#[derive(Debug, Clone)]
pub struct EnqueuedMessage {
    pub info: IntMsgInfo,
    pub cell: Cell,
    pub hash: HashBytes,
}

impl From<(IntMsgInfo, Cell)> for EnqueuedMessage {
    fn from((info, cell): (IntMsgInfo, Cell)) -> Self {
        let hash = *cell.repr_hash();
        EnqueuedMessage { info, cell, hash }
    }
}

impl EnqueuedMessage {
    pub fn destination(&self) -> &IntAddr {
        &self.info.dst
    }
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

#[derive(Default, Debug, Ord, Eq, PartialEq, PartialOrd, Hash, Clone)]
pub struct InternalMessageKey {
    pub lt: Lt,
    pub hash: HashBytes,
}

impl InternalMessageKey {
    pub const MAX: Self = Self {
        lt: u64::MAX,
        hash: HashBytes([u8::MAX; 32]),
    };

    pub fn with_lt_and_min_hash(lt: Lt) -> Self {
        Self {
            lt,
            hash: HashBytes::ZERO,
        }
    }

    pub fn with_lt_and_max_hash(lt: Lt) -> Self {
        Self {
            lt,
            hash: HashBytes([255; 32]),
        }
    }

    pub fn into_tuple(self) -> (Lt, HashBytes) {
        (self.lt, self.hash)
    }
}

pub trait InternalMessageKeyOpt {
    fn map_into_tuple(self) -> Option<(Lt, HashBytes)>;
}
impl InternalMessageKeyOpt for Option<InternalMessageKey> {
    fn map_into_tuple(self) -> Option<(Lt, HashBytes)> {
        self.map(|value| (value.lt, value.hash))
    }
}

impl From<(Lt, HashBytes)> for InternalMessageKey {
    fn from(value: (Lt, HashBytes)) -> Self {
        Self {
            lt: value.0,
            hash: value.1,
        }
    }
}

impl EnqueuedMessage {
    pub fn key(&self) -> InternalMessageKey {
        InternalMessageKey {
            lt: self.info.created_lt,
            hash: self.hash,
        }
    }
}
