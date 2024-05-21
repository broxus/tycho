use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{bail, Result};
use everscale_types::cell::{Cell, HashBytes};
use everscale_types::models::{BlockIdShort, IntAddr, IntMsgInfo, ShardIdent};

pub type Lt = u64;

pub struct QueueDiff {
    pub id: BlockIdShort,
    pub messages: Vec<Arc<EnqueuedMessage>>,
    pub processed_upto: HashMap<ShardIdent, InternalMessageKey>,
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
    pub fn destination(&self) -> Result<HashBytes> {
        match &self.info.dst {
            IntAddr::Std(dst) => Ok(dst.address),
            IntAddr::Var(_) => {
                bail!("Var destination address is not supported")
            }
        }
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

#[derive(Ord, Eq, PartialEq, PartialOrd, Hash, Clone)]
pub struct InternalMessageKey {
    pub lt: Lt,
    pub hash: HashBytes,
}

impl EnqueuedMessage {
    pub fn key(&self) -> InternalMessageKey {
        InternalMessageKey {
            lt: self.info.created_lt,
            hash: self.hash,
        }
    }
}
