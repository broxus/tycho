use std::cmp::Ordering;

use anyhow::Context;
use tycho_block_util::queue::QueueKey;
use tycho_types::boc::Boc;
use tycho_types::cell::{Cell, HashBytes, Load};
use tycho_types::models::{IntAddr, IntMsgInfo, Message, MsgInfo};

#[derive(Debug, Clone)]
pub struct EnqueuedMessage {
    pub info: IntMsgInfo,
    pub cell: Cell,
}

#[cfg(test)]
impl Default for EnqueuedMessage {
    fn default() -> Self {
        let info = IntMsgInfo::default();
        let cell = tycho_types::cell::CellBuilder::build_from(&info).unwrap();

        Self { info, cell }
    }
}

impl From<(IntMsgInfo, Cell)> for EnqueuedMessage {
    fn from((info, cell): (IntMsgInfo, Cell)) -> Self {
        EnqueuedMessage { info, cell }
    }
}

impl EnqueuedMessage {
    pub fn dest(&self) -> &IntAddr {
        &self.info.dst
    }

    pub fn src(&self) -> &IntAddr {
        &self.info.src
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
        Some(self.cmp(other))
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
            hash: *self.hash(),
        }
    }
}

pub trait InternalMessageValue: Send + Sync + Ord + Clone + 'static {
    fn deserialize(bytes: &[u8]) -> anyhow::Result<Self>
    where
        Self: Sized;

    fn serialize(&self, buffer: &mut Vec<u8>)
    where
        Self: Sized;

    fn source(&self) -> &IntAddr;

    fn destination(&self) -> &IntAddr;

    fn key(&self) -> QueueKey;

    fn info(&self) -> &IntMsgInfo;

    fn cell(&self) -> &Cell;
}

impl InternalMessageValue for EnqueuedMessage {
    fn deserialize(bytes: &[u8]) -> anyhow::Result<Self> {
        let cell = Boc::decode(bytes).context("Failed to load cell")?;
        let message = Message::load_from(&mut cell.as_slice().context("Failed to load message")?)?;

        match message.info {
            MsgInfo::Int(info) => Ok(Self { info, cell }),
            _ => anyhow::bail!("Expected internal message"),
        }
    }

    fn serialize(&self, buffer: &mut Vec<u8>)
    where
        Self: Sized,
    {
        tycho_types::boc::ser::BocHeader::<ahash::RandomState>::with_root(self.cell.as_ref())
            .encode(buffer);
    }

    fn source(&self) -> &IntAddr {
        self.src()
    }

    fn destination(&self) -> &IntAddr {
        self.dest()
    }

    fn key(&self) -> QueueKey {
        self.key()
    }

    fn info(&self) -> &IntMsgInfo {
        &self.info
    }

    fn cell(&self) -> &Cell {
        &self.cell
    }
}
