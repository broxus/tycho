use std::collections::{BTreeMap, hash_map};
use std::sync::Arc;

use tycho_block_util::queue::{QueueKey, QueuePartitionIdx};
use tycho_types::models::{IntAddr, ShardIdent};
use tycho_util::FastHashMap;
use tycho_util::transactional::Transactional;

use crate::internal_queue::types::diff::QueueDiffWithMessages;
use crate::internal_queue::types::message::InternalMessageValue;

pub type AccountStatistics = FastHashMap<IntAddr, u64>;
pub type StatisticsByPartitions = FastHashMap<QueuePartitionIdx, AccountStatistics>;
pub type SeparatedStatisticsByPartitions =
    FastHashMap<QueuePartitionIdx, BTreeMap<QueueKey, AccountStatistics>>;

#[derive(Debug, Default, Clone)]
pub struct QueueStatistics {
    statistics: FastHashMap<IntAddr, u64>,
    tx_changes: Option<FastHashMap<IntAddr, u64>>,
}

impl Transactional for QueueStatistics {
    fn begin(&mut self) {
        assert!(
            self.tx_changes.is_none(),
            "nested transactions not supported"
        );
        self.tx_changes = Some(FastHashMap::default());
    }

    fn commit(&mut self) {
        let tx_changes = self
            .tx_changes
            .take()
            .expect("no active transaction to commit");

        for (account_addr, value) in tx_changes {
            if value == 0 {
                self.statistics.remove(&account_addr);
            } else {
                self.statistics.insert(account_addr, value);
            }
        }
    }

    fn rollback(&mut self) {
        self.tx_changes
            .take()
            .expect("no active transaction to rollback");
    }

    fn in_tx(&self) -> bool {
        self.tx_changes.is_some()
    }
}

impl QueueStatistics {
    pub fn with_statistics(statistics: AccountStatistics) -> Self {
        Self {
            statistics,
            tx_changes: None,
        }
    }

    pub fn statistics(&self) -> StatisticsView<'_> {
        StatisticsView {
            base: &self.statistics,
            tx_changes: self.tx_changes.as_ref(),
        }
    }

    pub fn iter(&self) -> StatisticsViewIter<'_> {
        StatisticsViewIter {
            tx_iter: self.tx_changes.as_ref().map(|tx| tx.iter()),
            tx_shadow: self.tx_changes.as_ref(),
            base_iter: self.statistics.iter(),
        }
    }

    pub fn decrement_for_account(&mut self, account_addr: IntAddr, count: u64) {
        if let Some(tx_changes) = self.tx_changes.as_mut() {
            let base = &self.statistics;

            match tx_changes.entry(account_addr) {
                hash_map::Entry::Occupied(mut occupied) => {
                    let current = *occupied.get();
                    if current < count {
                        panic!(
                            "attempted to decrement below zero: current={}, decrement={}",
                            current, count
                        );
                    }
                    *occupied.get_mut() = current - count;
                }
                hash_map::Entry::Vacant(vacant) => {
                    let current = *base
                        .get(vacant.key())
                        .unwrap_or_else(|| panic!("attempted to decrement non-existent account"));
                    if current < count {
                        panic!(
                            "attempted to decrement below zero: current={}, decrement={}",
                            current, count
                        );
                    }
                    vacant.insert(current - count);
                }
            }
        } else {
            let Some(current) = self.statistics.get(&account_addr).copied() else {
                panic!("attempted to decrement non-existent account");
            };

            if current < count {
                panic!(
                    "attempted to decrement below zero: current={}, decrement={}",
                    current, count
                );
            }

            let new_value = current - count;
            if new_value == 0 {
                self.statistics.remove(&account_addr);
            } else {
                self.statistics.insert(account_addr, new_value);
            }
        }
    }

    pub fn increment_for_account(&mut self, account_addr: IntAddr, count: u64) {
        if let Some(tx_changes) = self.tx_changes.as_mut() {
            let base = &self.statistics;

            match tx_changes.entry(account_addr) {
                hash_map::Entry::Occupied(mut occupied) => {
                    *occupied.get_mut() += count;
                }
                hash_map::Entry::Vacant(vacant) => {
                    let current = base.get(vacant.key()).copied().unwrap_or_default();
                    vacant.insert(current + count);
                }
            }
        } else {
            self.statistics
                .entry(account_addr)
                .and_modify(|v| *v += count)
                .or_insert(count);
        }
    }

    pub fn append(&mut self, other: &AccountStatistics) {
        for (account_addr, &msgs_count) in other {
            self.increment_for_account(account_addr.clone(), msgs_count);
        }
    }
}

pub struct StatisticsView<'a> {
    base: &'a FastHashMap<IntAddr, u64>,
    tx_changes: Option<&'a FastHashMap<IntAddr, u64>>,
}

impl<'a> StatisticsView<'a> {
    pub fn get(&self, addr: &IntAddr) -> Option<u64> {
        if let Some(tx_changes) = self.tx_changes
            && let Some(value) = tx_changes.get(addr)
        {
            return (*value > 0).then_some(*value);
        }

        self.base.get(addr).copied().filter(|&v| v > 0)
    }

    pub fn len(&self) -> usize {
        self.iter().count()
    }

    pub fn is_empty(&self) -> bool {
        self.iter().next().is_none()
    }

    pub fn contains_key(&self, addr: &IntAddr) -> bool {
        self.get(addr).is_some()
    }

    pub fn iter(&self) -> StatisticsViewIter<'a> {
        StatisticsViewIter {
            tx_iter: self.tx_changes.map(|tx| tx.iter()),
            tx_shadow: self.tx_changes,
            base_iter: self.base.iter(),
        }
    }
}

#[derive(Clone)]
pub struct StatisticsViewIter<'a> {
    tx_iter: Option<hash_map::Iter<'a, IntAddr, u64>>,
    tx_shadow: Option<&'a FastHashMap<IntAddr, u64>>,
    base_iter: hash_map::Iter<'a, IntAddr, u64>,
}

impl<'a> Iterator for StatisticsViewIter<'a> {
    type Item = (&'a IntAddr, u64);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(tx_iter) = self.tx_iter.as_mut() {
                if let Some((k, v)) = tx_iter.next() {
                    if *v > 0 {
                        return Some((k, *v));
                    }
                    continue;
                }

                self.tx_iter = None;
            }

            let (k, v) = self.base_iter.next()?;
            if self.tx_shadow.is_some_and(|tx| tx.contains_key(k)) {
                continue;
            }
            if *v > 0 {
                return Some((k, *v));
            }
        }
    }
}

impl<'a> IntoIterator for StatisticsView<'a> {
    type Item = (&'a IntAddr, u64);
    type IntoIter = StatisticsViewIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        StatisticsViewIter {
            tx_iter: self.tx_changes.map(|tx| tx.iter()),
            tx_shadow: self.tx_changes,
            base_iter: self.base.iter(),
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
    use tycho_util::FastHashMap;

    use super::*;

    impl QueueStatistics {
        pub fn total_count(&self) -> u64 {
            self.statistics().iter().map(|(_, v)| v).sum()
        }
    }

    fn addr(id: u8) -> IntAddr {
        IntAddr::Std(tycho_types::models::StdAddr::new(0, [id; 32].into()))
    }

    // === Basic operations ===

    #[test]
    fn increment_outside_transaction() {
        let mut stats = QueueStatistics::default();
        stats.increment_for_account(addr(1), 5);

        assert_eq!(stats.statistics().get(&addr(1)), Some(5));
    }

    #[test]
    fn decrement_outside_transaction() {
        let mut stats = QueueStatistics::default();
        stats.increment_for_account(addr(1), 10);
        stats.decrement_for_account(addr(1), 3);

        assert_eq!(stats.statistics().get(&addr(1)), Some(7));
    }

    #[test]
    fn decrement_to_zero_removes_entry() {
        let mut stats = QueueStatistics::default();
        stats.increment_for_account(addr(1), 5);
        stats.decrement_for_account(addr(1), 5);

        assert_eq!(stats.statistics().get(&addr(1)), None);
    }

    #[test]
    fn append_multiple() {
        let mut stats = QueueStatistics::default();
        let mut other = FastHashMap::default();
        other.insert(addr(1), 5);
        other.insert(addr(2), 10);

        stats.append(&other);

        assert_eq!(stats.statistics().get(&addr(1)), Some(5));
        assert_eq!(stats.statistics().get(&addr(2)), Some(10));
    }

    #[test]
    fn with_statistics_constructor() {
        let mut initial = FastHashMap::default();
        initial.insert(addr(1), 5);

        let stats = QueueStatistics::with_statistics(initial);

        assert_eq!(stats.statistics().get(&addr(1)), Some(5));
    }

    // === Transaction: commit ===

    #[test]
    fn transaction_increment_commit() {
        let mut stats = QueueStatistics::default();

        stats.begin();
        stats.increment_for_account(addr(1), 5);
        assert_eq!(stats.statistics().get(&addr(1)), Some(5));

        stats.commit();
        assert_eq!(stats.statistics().get(&addr(1)), Some(5));
    }

    #[test]
    fn transaction_decrement_commit() {
        let mut stats = QueueStatistics::default();
        stats.increment_for_account(addr(1), 10);

        stats.begin();
        stats.decrement_for_account(addr(1), 3);
        stats.commit();

        assert_eq!(stats.statistics().get(&addr(1)), Some(7));
    }

    #[test]
    fn transaction_commit_removes_zero_entries() {
        let mut stats = QueueStatistics::default();
        stats.increment_for_account(addr(1), 5);

        stats.begin();
        stats.decrement_for_account(addr(1), 5);
        stats.commit();

        assert!(!stats.statistics().contains_key(&addr(1)));
    }

    // === Transaction: rollback ===

    #[test]
    fn transaction_increment_rollback() {
        let mut stats = QueueStatistics::default();
        stats.increment_for_account(addr(1), 5);

        stats.begin();
        stats.increment_for_account(addr(1), 10);
        stats.increment_for_account(addr(2), 20);
        stats.rollback();

        assert_eq!(stats.statistics().get(&addr(1)), Some(5));
        assert_eq!(stats.statistics().get(&addr(2)), None);
    }

    #[test]
    fn transaction_decrement_rollback() {
        let mut stats = QueueStatistics::default();
        stats.increment_for_account(addr(1), 10);

        stats.begin();
        stats.decrement_for_account(addr(1), 7);
        stats.rollback();

        assert_eq!(stats.statistics().get(&addr(1)), Some(10));
    }

    #[test]
    fn transaction_view_iter_overrides_base_and_hides_zero() {
        let mut stats = QueueStatistics::default();
        stats.increment_for_account(addr(1), 10);
        stats.increment_for_account(addr(2), 20);

        stats.begin();
        stats.increment_for_account(addr(1), 5);
        stats.decrement_for_account(addr(2), 20);
        stats.increment_for_account(addr(3), 7);

        let collected: FastHashMap<_, _> = stats
            .statistics()
            .iter()
            .map(|(k, v)| (k.clone(), v))
            .collect();

        assert_eq!(collected.len(), 2);
        assert_eq!(collected.get(&addr(1)), Some(&15));
        assert_eq!(collected.get(&addr(2)), None);
        assert_eq!(collected.get(&addr(3)), Some(&7));
        assert!(!stats.statistics().contains_key(&addr(2)));
    }

    #[test]
    fn repeated_updates_in_transaction_are_visible_before_commit() {
        let mut stats = QueueStatistics::default();
        stats.increment_for_account(addr(1), 1);

        stats.begin();
        for _ in 0..1000 {
            stats.increment_for_account(addr(1), 1);
        }

        assert_eq!(stats.statistics().get(&addr(1)), Some(1001));
        stats.commit();
        assert_eq!(stats.statistics().get(&addr(1)), Some(1001));
    }

    // === Transaction: edge cases ===

    #[test]
    #[should_panic(expected = "nested transactions")]
    fn nested_begin_panics() {
        let mut stats = QueueStatistics::default();
        stats.begin();
        stats.begin();
    }

    #[test]
    #[should_panic(expected = "no active transaction to commit")]
    fn commit_outside_transaction_panics() {
        let mut stats = QueueStatistics::default();
        stats.commit();
    }

    #[test]
    #[should_panic(expected = "no active transaction to rollback")]
    fn rollback_outside_transaction_panics() {
        let mut stats = QueueStatistics::default();
        stats.rollback();
    }

    #[test]
    fn multiple_transactions() {
        let mut stats = QueueStatistics::default();

        stats.begin();
        stats.increment_for_account(addr(1), 5);
        stats.commit();

        stats.begin();
        stats.increment_for_account(addr(1), 10);
        stats.rollback();

        stats.begin();
        stats.increment_for_account(addr(2), 20);
        stats.commit();

        assert_eq!(stats.statistics().get(&addr(1)), Some(5));
        assert_eq!(stats.statistics().get(&addr(2)), Some(20));
    }

    // === Panics ===

    #[test]
    #[should_panic(expected = "non-existent account")]
    fn decrement_nonexistent_panics() {
        let mut stats = QueueStatistics::default();
        stats.decrement_for_account(addr(1), 5);
    }

    #[test]
    #[should_panic(expected = "below zero")]
    fn decrement_below_zero_panics() {
        let mut stats = QueueStatistics::default();
        stats.increment_for_account(addr(1), 5);
        stats.decrement_for_account(addr(1), 10);
    }

    // === View ===

    #[test]
    fn statistics_view_len_and_empty() {
        let mut stats = QueueStatistics::default();

        assert!(stats.statistics().is_empty());
        assert_eq!(stats.statistics().len(), 0);

        stats.increment_for_account(addr(1), 5);

        assert!(!stats.statistics().is_empty());
        assert_eq!(stats.statistics().len(), 1);
    }

    #[test]
    fn statistics_view_iter() {
        let mut stats = QueueStatistics::default();
        stats.increment_for_account(addr(1), 5);
        stats.increment_for_account(addr(2), 10);

        let collected: FastHashMap<_, _> = stats
            .statistics()
            .iter()
            .map(|(k, v)| (k.clone(), v))
            .collect();

        assert_eq!(collected.len(), 2);
        assert_eq!(collected.get(&addr(1)), Some(&5));
        assert_eq!(collected.get(&addr(2)), Some(&10));
    }

    #[test]
    fn iter_method() {
        let mut stats = QueueStatistics::default();
        stats.increment_for_account(addr(1), 5);

        let collected: Vec<_> = stats.iter().collect();

        assert_eq!(collected.len(), 1);
    }
}
