use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::Arc;

use everscale_types::cell::{Cell, HashBytes};
use everscale_types::models::{IntAddr, IntMsgInfo, ShardIdent};

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
}

impl EnqueuedMessage {
    pub fn key(&self) -> InternalMessageKey {
        InternalMessageKey {
            lt: self.info.created_lt,
            hash: self.hash,
        }
    }
}
