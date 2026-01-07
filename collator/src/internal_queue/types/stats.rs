use std::collections::{BTreeMap, hash_map};
use std::sync::Arc;

use tycho_block_util::queue::{QueueKey, QueuePartitionIdx};
use tycho_network::quinn::rustls::pki_types::PrivateKeyDer::Sec1;
use tycho_types::models::{IntAddr, ShardIdent};
use tycho_util::FastHashMap;

use crate::internal_queue::types::diff::QueueDiffWithMessages;
use crate::internal_queue::types::message::InternalMessageValue;

pub type AccountStatistics = FastHashMap<IntAddr, u64>;
pub type StatisticsByPartitions = FastHashMap<QueuePartitionIdx, AccountStatistics>;
pub type SeparatedStatisticsByPartitions =
    FastHashMap<QueuePartitionIdx, BTreeMap<QueueKey, AccountStatistics>>;

#[derive(Debug, Clone, Copy, Default)]
struct VersionedValue {
    committed: u64,
    uncommitted: Option<u64>,
}

impl VersionedValue {
    pub fn committed(value: u64) -> Self {
        Self {
            committed: value,
            uncommitted: None,
        }
    }

    pub fn current(&self) -> u64 {
        self.uncommitted.unwrap_or(self.committed)
    }

    pub fn committed_value(&self) -> u64 {
        self.committed
    }

    pub fn is_dirty(&self) -> bool {
        self.uncommitted.is_some()
    }
}

#[derive(Debug, Default, Clone)]
pub struct QueueStatistics {
    statistics: FastHashMap<IntAddr, VersionedValue>,
}

impl QueueStatistics {
    pub fn with_statistics(statistics: AccountStatistics) -> Self {
        Self {
            statistics: statistics
                .into_iter()
                .map(|(k, v)| (k, VersionedValue::committed(v)))
                .collect(),
        }
    }

    pub fn statistics(&self) -> StatisticsView<'_> {
        StatisticsView {
            inner: &self.statistics,
        }
    }

    pub fn increment_for_account(&mut self, account_addr: IntAddr, count: u64) {
        self.statistics
            .entry(account_addr)
            .and_modify(|v| {
                v.uncommitted = Some(v.current() + count);
            })
            .or_insert(VersionedValue {
                committed: 0,
                uncommitted: Some(count),
            });
    }

    pub fn decrement_for_account(&mut self, account_addr: IntAddr, count: u64) {
        if let Some(v) = self.statistics.get_mut(&account_addr) {
            let current = v.current();
            if current < count {
                panic!(
                    "attempted to decrement below zero: current={}, decrement={}",
                    current, count
                );
            }
            v.uncommitted = Some(current - count);
        } else {
            panic!("attempted to decrement non-existent account");
        }
    }

    pub fn append(&mut self, other: &AccountStatistics) {
        for (account_addr, &msgs_count) in other {
            self.statistics
                .entry(account_addr.clone())
                .and_modify(|v| {
                    v.uncommitted = Some(v.current() + msgs_count);
                })
                .or_insert(VersionedValue {
                    committed: 0,
                    uncommitted: Some(msgs_count),
                });
        }
    }

    pub fn append_committed(&mut self, other: &AccountStatistics) {
        for (account_addr, &msgs_count) in other {
            self.statistics
                .entry(account_addr.clone())
                .and_modify(|v| v.committed += msgs_count)
                .or_insert(VersionedValue::committed(msgs_count));
        }
    }

    // Transactions

    pub fn commit(&mut self, affected: impl Iterator<Item = IntAddr>) {
        for addr in affected {
            if let hash_map::Entry::Occupied(mut entry) = self.statistics.entry(addr) {
                let v = entry.get_mut();
                if let Some(uncommitted) = v.uncommitted.take() {
                    if uncommitted == 0 {
                        entry.remove();
                    } else {
                        v.committed = uncommitted;
                    }
                }
            }
        }
    }

    pub fn rollback(&mut self, affected: impl Iterator<Item = IntAddr>) {
        for addr in affected {
            if let hash_map::Entry::Occupied(mut entry) = self.statistics.entry(addr) {
                let v = entry.get_mut();
                v.uncommitted = None;
                if v.committed == 0 {
                    entry.remove();
                }
            }
        }
    }

    pub fn commit_all(&mut self) {
        self.statistics.retain(|_, v| {
            if let Some(uncommitted) = v.uncommitted.take() {
                if uncommitted == 0 {
                    return false;
                }
                v.committed = uncommitted;
            }
            true
        });
    }

    pub fn rollback_all(&mut self) {
        self.statistics.retain(|_, v| {
            v.uncommitted = None;
            v.committed != 0
        });
    }

    pub fn has_uncommitted(&self) -> bool {
        self.statistics.values().any(|v| v.is_dirty())
    }
}

pub struct StatisticsView<'a> {
    inner: &'a FastHashMap<IntAddr, VersionedValue>,
}

impl<'a> StatisticsView<'a> {
    pub fn get(&self, addr: &IntAddr) -> Option<u64> {
        self.inner.get(addr).map(|v| v.current()).filter(|&v| v > 0)
    }

    pub fn len(&self) -> usize {
        self.inner.values().filter(|v| v.current() > 0).count()
    }

    pub fn is_empty(&self) -> bool {
        !self.inner.values().any(|v| v.current() > 0)
    }

    pub fn contains_key(&self, addr: &IntAddr) -> bool {
        self.get(addr).is_some()
    }

    pub fn iter(&self) -> StatisticsViewIter<'a> {
        StatisticsViewIter {
            inner: self.inner.iter(),
        }
    }
}

pub struct StatisticsViewIter<'a> {
    inner: hash_map::Iter<'a, IntAddr, VersionedValue>,
}

impl<'a> Iterator for StatisticsViewIter<'a> {
    type Item = (&'a IntAddr, u64);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (k, v) = self.inner.next()?;
            let c = v.current();
            if c > 0 {
                return Some((k, c));
            }
        }
    }
}

impl<'a> IntoIterator for StatisticsView<'a> {
    type Item = (&'a IntAddr, u64);
    type IntoIter = StatisticsViewIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        StatisticsViewIter {
            inner: self.inner.iter(),
        }
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
    ) -> Self {
        let mut shards_messages_count = FastHashMap::default();

        for account_statistics in statistics.values() {
            for (addr, msg_count) in account_statistics.iter() {
                // TODO after split/merge implementation we should use detailed counter for 256 shards
                let dest_shard = ShardIdent::new_full(addr.workchain());

                shards_messages_count
                    .entry(dest_shard)
                    .and_modify(|count: &mut u64| *count += msg_count)
                    .or_insert(*msg_count);
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(id: u8) -> IntAddr {
        IntAddr::Std(tycho_types::models::StdAddr::new(0, [id; 32].into()))
    }

    #[test]
    fn test_increment_creates_uncommitted() {
        let mut stats = QueueStatistics::default();

        stats.increment_for_account(addr(1), 5);

        assert!(stats.has_uncommitted());
        assert_eq!(stats.statistics().get(&addr(1)), Some(5));
    }

    #[test]
    fn test_decrement_creates_uncommitted() {
        let mut initial = FastHashMap::default();
        initial.insert(addr(1), 10);
        let mut stats = QueueStatistics::with_statistics(initial);

        stats.decrement_for_account(addr(1), 3);

        assert!(stats.has_uncommitted());
        assert_eq!(stats.statistics().get(&addr(1)), Some(7));
    }

    #[test]
    #[should_panic(expected = "decrement below zero")]
    fn test_decrement_below_zero_panics() {
        let mut initial = FastHashMap::default();
        initial.insert(addr(1), 5);
        let mut stats = QueueStatistics::with_statistics(initial);

        stats.decrement_for_account(addr(1), 10);
    }

    #[test]
    #[should_panic(expected = "non-existent account")]
    fn test_decrement_nonexistent_panics() {
        let mut stats = QueueStatistics::default();
        stats.decrement_for_account(addr(1), 5);
    }

    #[test]
    fn test_commit_applies_changes() {
        let mut stats = QueueStatistics::default();

        stats.increment_for_account(addr(1), 5);
        stats.increment_for_account(addr(2), 10);
        stats.commit(vec![addr(1), addr(2)].into_iter());

        assert!(!stats.has_uncommitted());
        assert_eq!(stats.statistics().get(&addr(1)), Some(5));
        assert_eq!(stats.statistics().get(&addr(2)), Some(10));
    }

    #[test]
    fn test_commit_partial() {
        let mut stats = QueueStatistics::default();

        stats.increment_for_account(addr(1), 5);
        stats.increment_for_account(addr(2), 10);
        stats.commit(vec![addr(1)].into_iter());

        assert!(stats.has_uncommitted()); // addr(2) still uncommitted
        assert_eq!(stats.statistics().get(&addr(1)), Some(5));
        assert_eq!(stats.statistics().get(&addr(2)), Some(10));
    }

    #[test]
    fn test_commit_removes_zero_entries() {
        let mut initial = FastHashMap::default();
        initial.insert(addr(1), 5);
        let mut stats = QueueStatistics::with_statistics(initial);

        stats.decrement_for_account(addr(1), 5);
        assert_eq!(stats.statistics().get(&addr(1)), None); // current = 0, filtered by view

        stats.commit(vec![addr(1)].into_iter());

        assert!(!stats.has_uncommitted());
        assert_eq!(stats.statistics().get(&addr(1)), None);
        assert!(stats.statistics().is_empty());
    }

    #[test]
    fn test_commit_all() {
        let mut stats = QueueStatistics::default();

        stats.increment_for_account(addr(1), 5);
        stats.increment_for_account(addr(2), 10);
        stats.increment_for_account(addr(3), 15);
        stats.commit_all();

        assert!(!stats.has_uncommitted());
        assert_eq!(stats.statistics().get(&addr(1)), Some(5));
        assert_eq!(stats.statistics().get(&addr(2)), Some(10));
        assert_eq!(stats.statistics().get(&addr(3)), Some(15));
    }

    #[test]
    fn test_rollback_discards_changes() {
        let mut initial = FastHashMap::default();
        initial.insert(addr(1), 5);
        let mut stats = QueueStatistics::with_statistics(initial);

        stats.increment_for_account(addr(1), 10);
        assert_eq!(stats.statistics().get(&addr(1)), Some(15));

        stats.rollback(vec![addr(1)].into_iter());

        assert!(!stats.has_uncommitted());
        assert_eq!(stats.statistics().get(&addr(1)), Some(5)); // back to committed
    }

    #[test]
    fn test_rollback_partial() {
        let mut stats = QueueStatistics::default();

        stats.increment_for_account(addr(1), 5);
        stats.increment_for_account(addr(2), 10);
        stats.rollback(vec![addr(1)].into_iter());

        assert!(stats.has_uncommitted()); // addr(2) still uncommitted
        assert_eq!(stats.statistics().get(&addr(1)), None); // rolled back, was new
        assert_eq!(stats.statistics().get(&addr(2)), Some(10));
    }

    #[test]
    fn test_rollback_removes_entries_with_zero_committed() {
        let mut stats = QueueStatistics::default();

        stats.increment_for_account(addr(1), 5);
        stats.rollback(vec![addr(1)].into_iter());

        assert!(stats.statistics().is_empty());
    }

    #[test]
    fn test_rollback_all() {
        let mut initial = FastHashMap::default();
        initial.insert(addr(1), 5);
        let mut stats = QueueStatistics::with_statistics(initial);

        stats.increment_for_account(addr(1), 10);
        stats.increment_for_account(addr(2), 20);
        stats.rollback_all();

        assert!(!stats.has_uncommitted());
        assert_eq!(stats.statistics().get(&addr(1)), Some(5)); // back to committed
        assert_eq!(stats.statistics().get(&addr(2)), None); // was new, removed
    }

    #[test]
    fn test_append_creates_uncommitted() {
        let mut stats = QueueStatistics::default();
        let mut other = FastHashMap::default();
        other.insert(addr(1), 5);
        other.insert(addr(2), 10);

        stats.append(&other);

        assert!(stats.has_uncommitted());
        assert_eq!(stats.statistics().get(&addr(1)), Some(5));
        assert_eq!(stats.statistics().get(&addr(2)), Some(10));
    }

    #[test]
    fn test_append_committed_no_transaction() {
        let mut stats = QueueStatistics::default();
        let mut other = FastHashMap::default();
        other.insert(addr(1), 5);

        stats.append_committed(&other);

        assert!(!stats.has_uncommitted());
        assert_eq!(stats.statistics().get(&addr(1)), Some(5));
    }

    #[test]
    fn test_multiple_increments_before_commit() {
        let mut stats = QueueStatistics::default();

        stats.increment_for_account(addr(1), 5);
        stats.increment_for_account(addr(1), 3);
        stats.increment_for_account(addr(1), 2);

        assert_eq!(stats.statistics().get(&addr(1)), Some(10));

        stats.commit_all();
        assert_eq!(stats.statistics().get(&addr(1)), Some(10));
    }

    #[test]
    fn test_increment_then_decrement_before_commit() {
        let mut stats = QueueStatistics::default();

        stats.increment_for_account(addr(1), 10);
        stats.decrement_for_account(addr(1), 3);

        assert_eq!(stats.statistics().get(&addr(1)), Some(7));

        stats.commit_all();
        assert_eq!(stats.statistics().get(&addr(1)), Some(7));
    }

    #[test]
    fn test_view_iterator() {
        let mut stats = QueueStatistics::default();
        stats.increment_for_account(addr(1), 5);
        stats.increment_for_account(addr(2), 10);
        stats.commit_all();

        let view = stats.statistics();
        let collected: FastHashMap<_, _> = view.into_iter().map(|(k, v)| (k.clone(), v)).collect();

        assert_eq!(collected.len(), 2);
        assert_eq!(collected.get(&addr(1)), Some(&5));
        assert_eq!(collected.get(&addr(2)), Some(&10));
    }

    #[test]
    fn test_view_filters_zero_values() {
        let mut initial = FastHashMap::default();
        initial.insert(addr(1), 5);
        initial.insert(addr(2), 10);
        let mut stats = QueueStatistics::with_statistics(initial);

        stats.decrement_for_account(addr(1), 5); // becomes 0

        let view = stats.statistics();
        assert_eq!(view.len(), 1);
        assert_eq!(view.get(&addr(1)), None);
        assert_eq!(view.get(&addr(2)), Some(10));
    }

    #[test]
    fn test_commit_nonexistent_addr_is_noop() {
        let mut stats = QueueStatistics::default();
        stats.increment_for_account(addr(1), 5);

        stats.commit(vec![addr(99)].into_iter()); // addr(99) doesn't exist

        assert!(stats.has_uncommitted()); // addr(1) still uncommitted
    }

    #[test]
    fn test_rollback_nonexistent_addr_is_noop() {
        let mut stats = QueueStatistics::default();
        stats.increment_for_account(addr(1), 5);

        stats.rollback(vec![addr(99)].into_iter());

        assert!(stats.has_uncommitted());
        assert_eq!(stats.statistics().get(&addr(1)), Some(5));
    }
}
