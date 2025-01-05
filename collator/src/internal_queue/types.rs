use std::cmp::{Ordering, Reverse};
use std::collections::{hash_map, BTreeMap, BTreeSet, BinaryHeap, HashSet};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use everscale_types::boc::Boc;
use everscale_types::cell::{Cell, HashBytes, Load};
use everscale_types::models::{IntAddr, IntMsgInfo, Message, MsgInfo, OutMsgDescr, ShardIdent};
use tycho_block_util::queue::{DestAddr, QueueDiff, QueueDiffStuff, QueueKey, QueuePartition};
use tycho_util::FastHashMap;

use super::state::state_iterator::MessageExt;
use crate::types::ProcessedTo;

#[derive(Default, Debug, Clone, Eq, PartialEq)]
pub struct PartitionRouter {
    inner: FastHashMap<QueuePartition, HashSet<IntAddr>>,
}

impl PartitionRouter {
    pub fn new() -> Self {
        Self {
            inner: FastHashMap::default(),
        }
    }

    pub fn get_partition(&self, addr: &IntAddr) -> QueuePartition {
        self.inner
            .iter()
            .find_map(|(partition, addrs)| {
                if addrs.contains(addr) {
                    Some(*partition)
                } else {
                    None
                }
            })
            .unwrap_or_default()
    }

    pub fn insert(&mut self, addr: IntAddr, partition: QueuePartition) -> Result<()> {
        if partition == QueuePartition::NormalPriority {
            bail!("Attempt to insert address into normal priority partition");
        }

        let _ = self.inner.entry(partition).or_default().insert(addr);

        Ok(())
    }

    pub fn clear(&mut self) {
        self.inner.clear();
    }
}

impl From<BTreeMap<QueuePartition, BTreeSet<DestAddr>>> for PartitionRouter {
    fn from(value: BTreeMap<QueuePartition, BTreeSet<DestAddr>>) -> Self {
        let inner = value
            .into_iter()
            .map(|(partition, addrs)| {
                let addrs = addrs.into_iter().map(|addr| addr.to_int_addr()).collect();
                (partition, addrs)
            })
            .collect();

        Self { inner }
    }
}

#[derive(Default, Debug, Clone)]
pub struct QueueDiffWithMessages<V: InternalMessageValue> {
    pub messages: BTreeMap<QueueKey, Arc<V>>,
    pub processed_to: ProcessedTo,
    pub partition_router: PartitionRouter,
}

impl<V: InternalMessageValue> QueueDiffWithMessages<V> {
    pub fn new() -> Self {
        Self {
            messages: BTreeMap::new(),
            processed_to: BTreeMap::new(),
            partition_router: Default::default(),
        }
    }
}

impl QueueDiffWithMessages<EnqueuedMessage> {
    pub fn from_queue_diff(
        queue_diff_stuff: &QueueDiffStuff,
        out_msg_description: &OutMsgDescr,
    ) -> Result<Self> {
        let QueueDiff {
            processed_to,
            partition_router,
            ..
        } = queue_diff_stuff.as_ref();
        let processed_to: BTreeMap<ShardIdent, QueueKey> = processed_to
            .iter()
            .map(|(shard_ident, key)| (*shard_ident, *key))
            .collect();

        let partition_router = PartitionRouter::from(partition_router.clone());

        let mut messages: BTreeMap<QueueKey, Arc<_>> = BTreeMap::new();
        for msg in queue_diff_stuff.zip(out_msg_description) {
            let lazy_msg = msg?;
            let cell = lazy_msg.into_inner();
            let hash = *cell.repr_hash();
            let info = MsgInfo::load_from(&mut cell.as_slice()?)?;
            if let MsgInfo::Int(out_msg_info) = info {
                let created_lt = out_msg_info.created_lt;
                let value = EnqueuedMessage::from((out_msg_info, cell));
                messages.insert((created_lt, hash).into(), Arc::new(value));
            }
        }

        Ok(Self {
            messages,
            processed_to,
            partition_router,
        })
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

#[cfg(test)]
impl Default for EnqueuedMessage {
    fn default() -> Self {
        let info = IntMsgInfo::default();
        let cell = everscale_types::cell::CellBuilder::build_from(&info).unwrap();

        Self {
            info,
            hash: *cell.repr_hash(),
            cell,
        }
    }
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

pub struct PartitionQueueKey {
    pub partition: QueuePartition,
    pub key: QueueKey,
}
#[derive(Debug, Clone)]
pub struct QueueShardRange {
    pub shard_ident: ShardIdent,
    pub from: QueueKey,
    pub to: QueueKey,
}

#[derive(Debug, Clone)]
pub struct QueueRange {
    pub partition: QueuePartition,
    pub shard_ident: ShardIdent,
    pub from: QueueKey,
    pub to: QueueKey,
}

#[derive(Debug, Default, Clone)]
pub struct QueueStatistics {
    statistics: FastHashMap<IntAddr, u64>,
}

impl QueueStatistics {
    pub fn with_statistics(statistics: FastHashMap<IntAddr, u64>) -> Self {
        Self { statistics }
    }

    pub fn statistics(&self) -> &FastHashMap<IntAddr, u64> {
        &self.statistics
    }

    pub fn decrement_for_account(&mut self, account_addr: IntAddr, count: u64) {
        if let hash_map::Entry::Occupied(mut occupied) = self.statistics.entry(account_addr) {
            let value = occupied.get_mut();
            *value -= count;
            if *value == 0 {
                occupied.remove();
            }
        }
    }

    pub fn append(&mut self, other: &Self) {
        for (account_addr, &msgs_count) in &other.statistics {
            self.statistics
                .entry(account_addr.clone())
                .and_modify(|count| *count += msgs_count)
                .or_insert(msgs_count);
        }
    }

    pub fn append_diff_statistics(&mut self, diff_statistics: &DiffStatistics) {
        for (_, par_stats) in diff_statistics.inner.statistics.clone() {
            for (account_addr, msgs_count) in par_stats {
                self.statistics
                    .entry(account_addr)
                    .and_modify(|count| *count += msgs_count)
                    .or_insert(msgs_count);
            }
        }
    }
}

impl PartialEq for QueueStatistics {
    fn eq(&self, other: &Self) -> bool {
        self.statistics == other.statistics
    }
}

impl Eq for QueueStatistics {}

impl IntoIterator for QueueStatistics {
    type Item = (IntAddr, u64);
    type IntoIter = hash_map::IntoIter<IntAddr, u64>;

    fn into_iter(self) -> Self::IntoIter {
        self.statistics.into_iter()
    }
}

pub struct DiffStatistics {
    inner: Arc<DiffStatisticsInner>,
}

impl DiffStatistics {
    pub fn iter(&self) -> impl Iterator<Item = (&QueuePartition, &FastHashMap<IntAddr, u64>)> {
        self.inner.statistics.iter()
    }

    pub fn shard_ident(&self) -> &ShardIdent {
        &self.inner.shard_ident
    }

    pub fn min_message(&self) -> &QueueKey {
        &self.inner.min_message
    }

    pub fn max_message(&self) -> &QueueKey {
        &self.inner.max_message
    }
}

struct DiffStatisticsInner {
    shard_ident: ShardIdent,
    min_message: QueueKey,
    max_message: QueueKey,
    statistics: FastHashMap<QueuePartition, FastHashMap<IntAddr, u64>>,
}

impl<V: InternalMessageValue> From<(&QueueDiffWithMessages<V>, ShardIdent)> for DiffStatistics {
    fn from(value: (&QueueDiffWithMessages<V>, ShardIdent)) -> Self {
        let (diff, shard_ident) = value;
        let min_message = diff.messages.keys().next().cloned().unwrap_or_default();
        let max_message = diff
            .messages
            .keys()
            .next_back()
            .cloned()
            .unwrap_or_default();

        let mut statistics = FastHashMap::default();

        for message in diff.messages.values() {
            let destination = message.destination();

            let partition = diff.partition_router.get_partition(destination);

            *statistics
                .entry(partition)
                .or_insert(FastHashMap::default())
                .entry(destination.clone())
                .or_insert(0) += 1;
        }

        Self {
            inner: Arc::new(DiffStatisticsInner {
                shard_ident,
                min_message,
                max_message,
                statistics,
            }),
        }
    }
}

#[test]
fn test_insert() {
    let mut map = PartitionRouter::new(); // Предположим, MyStruct имеет inner

    let addr1 = IntAddr::Std(StdAddr::new(1, HashBytes::from([1; 32])));
    let addr2 = IntAddr::Std(StdAddr::new(2, HashBytes::from([2; 32])));
    let partition = QueuePartition::LowPriority;

    // Первая вставка
    map.insert(addr1.clone(), partition).unwrap();

    // Вторая вставка
    map.insert(addr2.clone(), partition).unwrap();

    let partition1 = map.get_partition(&addr1);

    assert_eq!(partition, partition1);

    let partition2 = map.get_partition(&addr2);

    assert_eq!(partition, partition2);

    // Проверяем содержимое
    // assert!(map.inner.get(&partition).unwrap().contains(&addr1));
    // assert!(map.inner.get(&partition).unwrap().contains(&addr2));
}
