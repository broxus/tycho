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
    pub messages: BTreeMap<QueueKey, Arc<V>>,
    pub processed_upto: BTreeMap<ShardIdent, QueueKey>,
    pub last_key: Option<QueueKey>,
}

impl<V: InternalMessageValue> QueueDiffWithMessages<V> {
    pub fn new() -> Self {
        Self {
            messages: BTreeMap::new(),
            processed_upto: BTreeMap::new(),
            last_key: None,
        }
    }

    pub fn exclude_last_key(&mut self) {
        self.last_key = self.messages.last_key_value().map(|(key, _)| *key);
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

impl EnqueuedMessage {
    pub fn key(&self) -> QueueKey {
        QueueKey {
            lt: self.info.created_lt,
            hash: self.hash,
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

    fn key(&self) -> QueueKey;
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

    fn key(&self) -> QueueKey {
        self.key()
    }
}
