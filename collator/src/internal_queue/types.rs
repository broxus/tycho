use std::cmp::Ordering;
use std::sync::Arc;

use anyhow::{bail, Result};
use everscale_types::cell::{Cell, HashBytes};
use everscale_types::models::{IntAddr, IntMsgInfo, ShardIdent};
use tycho_util::FastHashMap;

pub type Lt = u64;

#[derive(Default, Debug, Clone)]
pub struct QueueDiff {
    pub messages: Vec<Arc<EnqueuedMessage>>,
    pub processed_upto: FastHashMap<ShardIdent, InternalMessageKey>,
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
    pub fn destination(&self) -> Result<(i8, HashBytes)> {
        match &self.info.dst {
            IntAddr::Std(dst) => Ok((dst.workchain, dst.address)),
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
