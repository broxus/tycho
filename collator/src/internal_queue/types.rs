use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, BinaryHeap};
use std::sync::Arc;

use anyhow::{bail, Context};
use everscale_types::boc::Boc;
use everscale_types::cell::{Cell, HashBytes, Load};
use everscale_types::models::{IntAddr, IntMsgInfo, Message, MsgInfo, ShardIdent};
use tycho_block_util::queue::QueueKey;

use super::state::state_iterator::MessageExt;

#[derive(Default, Debug, Clone)]
pub struct QueueDiffWithMessages<V: InternalMessageValue> {
    pub messages: BTreeMap<InternalMessageKey, Arc<V>>,
    pub processed_upto: BTreeMap<ShardIdent, InternalMessageKey>,
    pub keys: Vec<InternalMessageKey>,
}

impl<V: InternalMessageValue> QueueDiffWithMessages<V> {
    pub fn new() -> Self {
        Self {
            messages: BTreeMap::new(),
            processed_upto: BTreeMap::new(),
            keys: Vec::new(),
        }
    }

    pub fn exclude_data(&mut self) {
        self.keys = self.messages.keys().cloned().collect();
        self.messages.clear();
    }
}

pub struct QueueFullDiff<V: InternalMessageValue> {
    pub diff: QueueDiffWithMessages<V>,
    pub messages_for_current_shard: BinaryHeap<Reverse<MessageExt<V>>>,
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

    pub fn hash(&self) -> &HashBytes {
        self.cell.repr_hash()
    }
}

impl Eq for EnqueuedMessage {}

impl PartialEq for EnqueuedMessage {
    fn eq(&self, other: &Self) -> bool {
        self.key() == other.key()
    }
}

impl PartialOrd for EnqueuedMessage {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.key().cmp(&other.key()))
    }
}

impl Ord for EnqueuedMessage {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key().cmp(&other.key())
    }
}

#[derive(Default, Debug, Ord, Eq, PartialEq, PartialOrd, Hash, Clone, Copy)]
pub struct InternalMessageKey {
    pub lt: u64,
    pub hash: HashBytes,
}

impl InternalMessageKey {
    pub const MAX: Self = Self {
        lt: u64::MAX,
        hash: HashBytes([u8::MAX; 32]),
    };

    pub fn with_lt_and_max_hash(lt: u64) -> Self {
        Self {
            lt,
            hash: HashBytes([255; 32]),
        }
    }

    pub fn with_lt_and_min_hash(lt: u64) -> Self {
        Self {
            lt,
            hash: HashBytes([0; 32]),
        }
    }

    pub fn into_tuple(self) -> (u64, HashBytes) {
        (self.lt, self.hash)
    }
}

pub trait InternalMessageKeyOpt {
    fn map_into_tuple(self) -> Option<(u64, HashBytes)>;
}

impl InternalMessageKeyOpt for Option<InternalMessageKey> {
    fn map_into_tuple(self) -> Option<(u64, HashBytes)> {
        self.map(|value| (value.lt, value.hash))
    }
}

impl From<(u64, HashBytes)> for InternalMessageKey {
    fn from(value: (u64, HashBytes)) -> Self {
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

impl From<InternalMessageKey> for tycho_storage::model::InternalMessageKey {
    fn from(key: InternalMessageKey) -> Self {
        tycho_storage::model::InternalMessageKey {
            lt: key.lt,
            hash: key.hash,
        }
    }
}

impl From<InternalMessageKey> for QueueKey {
    fn from(key: InternalMessageKey) -> Self {
        QueueKey {
            lt: key.lt,
            hash: key.hash,
        }
    }
}

impl From<tycho_storage::model::InternalMessageKey> for InternalMessageKey {
    fn from(key: tycho_storage::model::InternalMessageKey) -> Self {
        InternalMessageKey {
            lt: key.lt,
            hash: key.hash,
        }
    }
}

pub trait InternalMessageValue: Send + Sync + Ord + 'static {
    fn deserialize(bytes: &[u8]) -> anyhow::Result<Self>
    where
        Self: Sized;

    fn serialize(&self) -> anyhow::Result<Vec<u8>>
    where
        Self: Sized;

    fn destination(&self) -> &IntAddr;

    fn key(&self) -> InternalMessageKey;
}

impl InternalMessageValue for EnqueuedMessage {
    fn deserialize(bytes: &[u8]) -> anyhow::Result<Self> {
        let cell = Boc::decode(bytes).context("Failed to load cell")?;
        let message = Message::load_from(&mut cell.as_slice().context("Failed to load message")?)?;

        match message.info {
            MsgInfo::Int(info) => {
                let hash = *cell.repr_hash();
                Ok(Self { info, cell, hash })
            }
            _ => bail!("Expected internal message"),
        }
    }

    fn serialize(&self) -> anyhow::Result<Vec<u8>>
    where
        Self: Sized,
    {
        Ok(Boc::encode(&self.cell))
    }

    fn destination(&self) -> &IntAddr {
        self.destination()
    }

    fn key(&self) -> InternalMessageKey {
        self.key()
    }
}
