use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, hash_map};
use std::sync::Arc;

use anyhow::{Context, Result};
use tycho_block_util::queue::{
    QueueDiff, QueueDiffStuff, QueueKey, QueuePartitionIdx, RouterAddr, RouterPartitions,
};
use tycho_types::boc::Boc;
use tycho_types::cell::{Cell, HashBytes, Load};
use tycho_types::models::{IntAddr, IntMsgInfo, Message, MsgInfo, OutMsgDescr, ShardIdent};
use tycho_util::FastHashMap;

use super::state::state_iterator::MessageExt;
use crate::types::ProcessedTo;

#[derive(Default, Debug, Clone, Eq, PartialEq)]
pub struct PartitionRouter {
    src: FastHashMap<RouterAddr, QueuePartitionIdx>,
    dst: FastHashMap<RouterAddr, QueuePartitionIdx>,
    partitions_stats: FastHashMap<QueuePartitionIdx, usize>,
}

pub type AccountStatistics = FastHashMap<IntAddr, u64>;
pub type StatisticsByPartitions = FastHashMap<QueuePartitionIdx, AccountStatistics>;
pub type SeparatedStatisticsByPartitions =
    FastHashMap<QueuePartitionIdx, BTreeMap<QueueKey, AccountStatistics>>;

impl PartitionRouter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_partitions(src: &RouterPartitions, dst: &RouterPartitions) -> Self {
        let mut partitions_stats = FastHashMap::default();
        let mut convert = |partitions: &RouterPartitions| {
            let mut result =
                FastHashMap::with_capacity_and_hasher(partitions.len(), Default::default());

            for (partition, accounts) in partitions {
                partitions_stats
                    .entry(*partition)
                    .and_modify(|count| *count += accounts.len())
                    .or_insert(accounts.len());
                result.extend(accounts.iter().map(|account| (*account, *partition)));
            }

            result
        };

        Self {
            src: convert(src),
            dst: convert(dst),
            partitions_stats,
        }
    }

    /// Returns the partition for the given source and destination addresses.
    /// If the partition is not found, returns the default partition.
    pub fn get_partition(
        &self,
        src_addr: Option<&IntAddr>,
        dest_addr: &IntAddr,
    ) -> QueuePartitionIdx {
        RouterAddr::from_int_addr(dest_addr)
            .and_then(|dst| self.dst.get(&dst))
            .or_else(|| {
                src_addr
                    .and_then(RouterAddr::from_int_addr)
                    .and_then(|src| self.src.get(&src))
            })
            .copied()
            .unwrap_or_default()
    }

    /// Inserts the address into the inbound router.
    pub fn insert_src(&mut self, addr: &IntAddr, partition: QueuePartitionIdx) -> Result<()> {
        if partition == QueuePartitionIdx::MIN {
            anyhow::bail!("attempt to insert address into default priority partition");
        }
        let Some(addr) = RouterAddr::from_int_addr(addr) else {
            anyhow::bail!("attempt to insert a VarAddr into a priority partition");
        };
        if self.src.insert(addr, partition).is_none() {
            self.partitions_stats
                .entry(partition)
                .and_modify(|count| *count += 1)
                .or_insert(1);
        }
        Ok(())
    }

    /// Inserts the address into the outbound router.
    pub fn insert_dst(&mut self, addr: &IntAddr, partition: QueuePartitionIdx) -> Result<()> {
        if partition == QueuePartitionIdx::MIN {
            anyhow::bail!("attempt to insert address into default priority partition");
        }
        let Some(addr) = RouterAddr::from_int_addr(addr) else {
            anyhow::bail!("attempt to insert a VarAddr into a priority partition");
        };
        if self.dst.insert(addr, partition).is_none() {
            self.partitions_stats
                .entry(partition)
                .and_modify(|count| *count += 1)
                .or_insert(1);
        }
        Ok(())
    }

    pub fn partitions_stats(&self) -> &FastHashMap<QueuePartitionIdx, usize> {
        &self.partitions_stats
    }

    pub fn to_router_partitions_src(&self) -> RouterPartitions {
        let mut result = BTreeMap::new();
        for (addr, partition) in &self.src {
            result
                .entry(*partition)
                .or_insert(BTreeSet::new())
                .insert(*addr);
        }
        result
    }

    pub fn to_router_partitions_dst(&self) -> RouterPartitions {
        let mut result = BTreeMap::new();
        for (addr, partition) in &self.dst {
            result
                .entry(*partition)
                .or_insert(BTreeSet::new())
                .insert(*addr);
        }
        result
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

    pub fn min_message(&self) -> Option<&QueueKey> {
        self.messages.keys().next()
    }

    pub fn max_message(&self) -> Option<&QueueKey> {
        self.messages.keys().next_back()
    }
}

impl QueueDiffWithMessages<EnqueuedMessage> {
    pub fn from_queue_diff(
        queue_diff_stuff: &QueueDiffStuff,
        out_msg_description: &OutMsgDescr,
    ) -> Result<Self> {
        let QueueDiff {
            processed_to,
            router_partitions_src,
            router_partitions_dst,
            ..
        } = queue_diff_stuff.as_ref();

        let partition_router =
            PartitionRouter::with_partitions(router_partitions_src, router_partitions_dst);

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
            processed_to: processed_to.clone(),
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

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Bound<T> {
    Included(T),
    Excluded(T),
}

pub struct PartitionQueueKey {
    pub partition: QueuePartitionIdx,
    pub key: QueueKey,
}

#[derive(Debug, Clone)]
pub struct QueueShardRange {
    pub shard_ident: ShardIdent,
    pub from: QueueKey,
    pub to: QueueKey,
}

#[derive(Debug, Clone)]
pub struct QueueShardBoundedRange {
    pub shard_ident: ShardIdent,
    pub from: Bound<QueueKey>,
    pub to: Bound<QueueKey>,
}

impl From<QueueShardBoundedRange> for QueueShardRange {
    fn from(value: QueueShardBoundedRange) -> Self {
        let from = match value.from {
            Bound::Included(value) => value,
            Bound::Excluded(value) => value.next_value(),
        };

        let to = match value.to {
            Bound::Included(value) => value.next_value(),
            Bound::Excluded(value) => value,
        };

        QueueShardRange {
            shard_ident: value.shard_ident,
            from,
            to,
        }
    }
}
#[derive(Debug, Clone)]
pub struct QueueRange {
    pub partition: QueuePartitionIdx,
    pub shard_ident: ShardIdent,
    pub from: QueueKey,
    pub to: QueueKey,
}

#[derive(Debug, Default, Clone)]
pub struct QueueStatistics {
    statistics: AccountStatistics,
}

impl QueueStatistics {
    pub fn with_statistics(statistics: AccountStatistics) -> Self {
        Self { statistics }
    }

    pub fn statistics(&self) -> &AccountStatistics {
        &self.statistics
    }

    pub fn increment_for_account(&mut self, account_addr: IntAddr, count: u64) {
        self.statistics
            .entry(account_addr)
            .and_modify(|value| *value += count)
            .or_insert(count);
    }

    pub fn decrement_for_account(&mut self, account_addr: IntAddr, count: u64) {
        if let hash_map::Entry::Occupied(mut occupied) = self.statistics.entry(account_addr) {
            let value = occupied.get_mut();
            *value -= count;
            if *value == 0 {
                occupied.remove();
            }
        } else {
            panic!("attempted to decrement non-existent account");
        }
    }

    pub fn append(&mut self, other: &AccountStatistics) {
        for (account_addr, &msgs_count) in other {
            self.statistics
                .entry(account_addr.clone())
                .and_modify(|count| *count += msgs_count)
                .or_insert(msgs_count);
        }
    }

    pub fn shard_messages_count(&self) -> FastHashMap<ShardIdent, u64> {
        let mut shards_messages_count = FastHashMap::default();

        for stat in self.statistics.iter() {
            let (addr, msg_count) = stat;
            // TODO after split/merge implementation we should use detailed counter for 256 shards
            let dest_shard = if addr.is_masterchain() {
                ShardIdent::MASTERCHAIN
            } else {
                ShardIdent::new_full(0)
            };

            shards_messages_count
                .entry(dest_shard)
                .and_modify(|count: &mut u64| *count += *msg_count)
                .or_insert(*msg_count);
        }

        shards_messages_count
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

#[derive(Debug, Clone)]
pub struct DiffStatistics {
    inner: Arc<DiffStatisticsInner>,
}

impl DiffStatistics {
    pub fn iter(&self) -> impl Iterator<Item = (&QueuePartitionIdx, &AccountStatistics)> {
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

    pub fn statistics(&self) -> &StatisticsByPartitions {
        &self.inner.statistics
    }

    pub fn partition(&self, partition: QueuePartitionIdx) -> Option<&AccountStatistics> {
        self.inner.statistics.get(&partition)
    }

    pub fn get_messages_count_by_shard(&self, shard_ident: &ShardIdent) -> u64 {
        self.inner
            .shards_messages_count
            .get(shard_ident)
            .copied()
            .unwrap_or_default()
    }

    pub fn shards_messages_count(&self) -> &FastHashMap<ShardIdent, u64> {
        &self.inner.shards_messages_count
    }

    pub fn total_statistics(&self) -> AccountStatistics {
        let mut total_statistics = FastHashMap::default();
        for (_, partition_statistics) in self.inner.statistics.iter() {
            for (account_addr, msgs_count) in partition_statistics {
                total_statistics
                    .entry(account_addr.clone())
                    .and_modify(|count| *count += msgs_count)
                    .or_insert(*msgs_count);
            }
        }
        total_statistics
    }
}
#[derive(Debug, Clone)]
struct DiffStatisticsInner {
    shard_ident: ShardIdent,
    min_message: QueueKey,
    max_message: QueueKey,
    statistics: StatisticsByPartitions,
    shards_messages_count: FastHashMap<ShardIdent, u64>,
}

impl DiffStatistics {
    pub fn new(
        shard_ident: ShardIdent,
        min_message: QueueKey,
        max_message: QueueKey,
        statistics: StatisticsByPartitions,
        shards_messages_count: FastHashMap<ShardIdent, u64>,
    ) -> Self {
        Self {
            inner: Arc::new(DiffStatisticsInner {
                shard_ident,
                min_message,
                max_message,
                statistics,
                shards_messages_count,
            }),
        }
    }
}

impl DiffStatistics {
    pub fn from_diff<V: InternalMessageValue>(
        diff: &QueueDiffWithMessages<V>,
        shard_ident: ShardIdent,
        min_message: QueueKey,
        max_message: QueueKey,
    ) -> Self {
        let mut shards_messages_count = FastHashMap::default();
        let mut statistics = FastHashMap::default();

        for message in diff.messages.values() {
            let destination = message.destination();

            let partition = diff
                .partition_router
                .get_partition(Some(message.source()), destination);

            *statistics
                .entry(partition)
                .or_insert(FastHashMap::default())
                .entry(destination.clone())
                .or_insert(0) += 1;

            // TODO after split/merge implementation we should use detailed counter for 256 shards
            let dest_shard = if message.destination().is_masterchain() {
                ShardIdent::MASTERCHAIN
            } else {
                ShardIdent::new_full(0)
            };

            shards_messages_count
                .entry(dest_shard)
                .and_modify(|count| *count += 1)
                .or_insert(1);
        }

        Self {
            inner: Arc::new(DiffStatisticsInner {
                shard_ident,
                min_message,
                max_message,
                statistics,
                shards_messages_count,
            }),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CommitPointer {
    pub shard_ident: ShardIdent,
    pub queue_key: QueueKey,
}

#[derive(Debug)]
pub enum DiffZone {
    Committed,
    Uncommitted,
    Both,
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use tycho_util::FastHashSet;

    use super::*;
    use crate::storage::models::DiffInfo;

    #[test]
    fn test_partition_router_from_btreemap() {
        let addr1 = RouterAddr {
            workchain: 0,
            account: HashBytes([0x01; 32]),
        };
        let addr2 = RouterAddr {
            workchain: 0,
            account: HashBytes([0x02; 32]),
        };
        let addr3 = RouterAddr {
            workchain: 1,
            account: HashBytes([0x03; 32]),
        };
        let addr4 = RouterAddr {
            workchain: 1,
            account: HashBytes([0x04; 32]),
        };

        let mut dest_map = BTreeMap::new();
        dest_map.insert(QueuePartitionIdx(1), BTreeSet::from([addr1, addr2]));
        dest_map.insert(QueuePartitionIdx(2), BTreeSet::from([addr3]));

        let mut src_map = BTreeMap::new();
        src_map.insert(QueuePartitionIdx(10), BTreeSet::from([addr4]));

        let partition_router = PartitionRouter::with_partitions(&src_map, &dest_map);

        {
            let expected_partitions = FastHashSet::from_iter([1, 2, 10].map(QueuePartitionIdx));
            for par_id in partition_router.partitions_stats().keys() {
                assert!(expected_partitions.contains(par_id));
            }
        }

        {
            // Dest
            let dest_router = &partition_router.dst;
            // addr1 и addr2 -> partition 1
            assert_eq!(*dest_router.get(&addr1).unwrap(), QueuePartitionIdx(1));
            assert_eq!(*dest_router.get(&addr2).unwrap(), QueuePartitionIdx(1));
            // addr3 -> partition 2
            assert_eq!(*dest_router.get(&addr3).unwrap(), QueuePartitionIdx(2));

            // Src
            let src_router = &partition_router.src;
            // addr4 -> partition 10
            assert_eq!(*src_router.get(&addr4).unwrap(), QueuePartitionIdx(10));
        }
    }

    #[test]
    fn test_diff_info_value_serialization() {
        // 1) Create example data
        let mut map = FastHashMap::default();
        map.insert(ShardIdent::MASTERCHAIN, 123);
        map.insert(ShardIdent::BASECHAIN, 999);

        let mut processed_to = BTreeMap::new();
        processed_to.insert(ShardIdent::MASTERCHAIN, QueueKey {
            lt: 222,
            hash: HashBytes::from([0xCC; 32]),
        });

        let mut router_partitions_src = RouterPartitions::new();
        router_partitions_src.insert(
            QueuePartitionIdx(1),
            BTreeSet::from([RouterAddr {
                workchain: 0,
                account: HashBytes([0x01; 32]),
            }]),
        );

        let mut router_partitions_dst = RouterPartitions::new();
        router_partitions_dst.insert(
            QueuePartitionIdx(2),
            BTreeSet::from([RouterAddr {
                workchain: 0,
                account: HashBytes([0x02; 32]),
            }]),
        );

        let original = DiffInfo {
            min_message: QueueKey {
                lt: 111,
                hash: HashBytes::from([0xAA; 32]),
            },
            shards_messages_count: map,
            hash: HashBytes::from([0xBB; 32]),
            processed_to,
            router_partitions_src,
            max_message: QueueKey {
                lt: 222,
                hash: HashBytes::from([0xBB; 32]),
            },
            router_partitions_dst,
            seqno: 123,
        };

        // 2) Serialize
        let serialized = tl_proto::serialize(&original);

        // 3) Deserialize
        let deserialized = tl_proto::deserialize::<DiffInfo>(&serialized)
            .expect("Failed to deserialize DiffInfoValue");

        // 4) Compare original and deserialized
        assert_eq!(original, deserialized);
    }
}
