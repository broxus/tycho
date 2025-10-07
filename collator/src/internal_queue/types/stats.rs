use std::collections::{BTreeMap, hash_map};
use std::sync::Arc;

use tycho_block_util::queue::{QueueKey, QueuePartitionIdx};
use tycho_types::models::{IntAddr, ShardIdent};
use tycho_util::FastHashMap;

use crate::internal_queue::types::diff::QueueDiffWithMessages;
use crate::internal_queue::types::message::InternalMessageValue;

pub type AccountStatistics = FastHashMap<IntAddr, u64>;
pub type StatisticsByPartitions = FastHashMap<QueuePartitionIdx, AccountStatistics>;
pub type SeparatedStatisticsByPartitions =
    FastHashMap<QueuePartitionIdx, BTreeMap<QueueKey, AccountStatistics>>;

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
            let dest_shard = ShardIdent::new_full(addr.workchain());

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
            let dest_shard = ShardIdent::new_full(message.destination().workchain());

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
struct DiffStatisticsInner {
    shard_ident: ShardIdent,
    min_message: QueueKey,
    max_message: QueueKey,
    statistics: StatisticsByPartitions,
    shards_messages_count: FastHashMap<ShardIdent, u64>,
}
