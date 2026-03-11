use std::sync::Arc;

use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use tycho_types::models::{IntAddr, ShardIdent};
use tycho_util::transactional::Transactional;

use crate::internal_queue::types::stats::{AccountStatistics, QueueStatistics, StatisticsViewIter};

#[derive(Debug, Clone)]
pub struct TrackedQueueStatistics {
    inner: Arc<RwLock<TrackedQueueStatisticsInner>>,
    track_shard: ShardIdent,
}

#[derive(Debug, Default)]
struct TrackedQueueStatisticsInner {
    statistics: QueueStatistics,
    tracked_total: u64,
    tracked_total_snapshot: u64,
}

impl TrackedQueueStatistics {
    pub fn new(track_shard: ShardIdent) -> Self {
        Self {
            inner: Arc::new(RwLock::new(TrackedQueueStatisticsInner::default())),
            track_shard,
        }
    }

    pub fn tracked_total(&self) -> u64 {
        self.inner.read().tracked_total
    }

    pub fn contains(&self, account_addr: &IntAddr) -> bool {
        self.inner
            .read()
            .statistics
            .statistics()
            .contains_key(account_addr)
    }

    pub fn statistics(&self) -> TrackedQueueStatisticsView<'_> {
        TrackedQueueStatisticsView {
            guard: self.inner.read(),
        }
    }

    pub fn statistics_mut(&self) -> TrackedQueueStatisticsWriteGuard<'_> {
        TrackedQueueStatisticsWriteGuard {
            guard: self.inner.write(),
            track_shard: &self.track_shard,
        }
    }

    pub fn append(&self, other: &AccountStatistics) {
        let mut guard = self.inner.write();

        for (account_addr, &msgs_count) in other {
            if self.track_shard.contains_address(account_addr) {
                guard.tracked_total += msgs_count;
            }
        }
        guard.statistics.append(other);
    }
}

impl Transactional for TrackedQueueStatistics {
    fn begin(&mut self) {
        let mut guard = self.inner.write();
        guard.statistics.begin();
        guard.tracked_total_snapshot = guard.tracked_total;
    }

    fn commit(&mut self) {
        self.inner.write().statistics.commit();
    }

    fn rollback(&mut self) {
        let mut guard = self.inner.write();
        guard.statistics.rollback();
        guard.tracked_total = guard.tracked_total_snapshot;
    }

    fn in_tx(&self) -> bool {
        self.inner.read().statistics.in_tx()
    }
}

pub struct TrackedQueueStatisticsView<'a> {
    guard: RwLockReadGuard<'a, TrackedQueueStatisticsInner>,
}

impl<'a> TrackedQueueStatisticsView<'a> {
    pub fn iter(&self) -> StatisticsViewIter<'_> {
        self.guard.statistics.iter()
    }
}

pub struct TrackedQueueStatisticsWriteGuard<'a> {
    guard: RwLockWriteGuard<'a, TrackedQueueStatisticsInner>,
    track_shard: &'a ShardIdent,
}

impl TrackedQueueStatisticsWriteGuard<'_> {
    pub fn decrement_for_account(&mut self, account_addr: IntAddr, count: u64) {
        let is_tracked = self.track_shard.contains_address(&account_addr);
        self.guard
            .statistics
            .decrement_for_account(account_addr, count);
        if is_tracked {
            self.guard.tracked_total -= count;
        }
    }
}

#[cfg(test)]
impl TrackedQueueStatistics {
    pub fn get(&self, account_addr: &IntAddr) -> Option<u64> {
        self.inner.read().statistics.statistics().get(account_addr)
    }

    pub fn increment_for_account(&self, account_addr: IntAddr, count: u64) {
        let is_tracked = self.track_shard.contains_address(&account_addr);
        let mut guard = self.inner.write();

        guard.statistics.increment_for_account(account_addr, count);
        if is_tracked {
            guard.tracked_total += count;
        }
    }
}

#[cfg(test)]
mod tests {

    impl TrackedQueueStatistics {
        pub fn decrement_for_account(&self, account_addr: IntAddr, count: u64) {
            self.statistics_mut()
                .decrement_for_account(account_addr, count);
        }
    }
    use tycho_util::FastHashMap;

    use super::*;

    fn addr(id: u8) -> IntAddr {
        IntAddr::Std(tycho_types::models::StdAddr::new(0, [id; 32].into()))
    }

    fn track_all_shard() -> ShardIdent {
        ShardIdent::new_full(0)
    }

    fn track_none_shard() -> ShardIdent {
        ShardIdent::new_full(1)
    }

    // === Basic operations ===

    #[test]
    fn increment_outside_transaction() {
        let stats = TrackedQueueStatistics::new(track_all_shard());
        stats.increment_for_account(addr(1), 5);

        assert_eq!(stats.get(&addr(1)), Some(5));
        assert_eq!(stats.tracked_total(), 5);
    }

    #[test]
    fn decrement_outside_transaction() {
        let stats = TrackedQueueStatistics::new(track_all_shard());
        stats.increment_for_account(addr(1), 10);
        stats.decrement_for_account(addr(1), 3);

        assert_eq!(stats.get(&addr(1)), Some(7));
        assert_eq!(stats.tracked_total(), 7);
    }

    #[test]
    fn contains() {
        let stats = TrackedQueueStatistics::new(track_all_shard());
        stats.increment_for_account(addr(1), 5);

        assert!(stats.contains(&addr(1)));
        assert!(!stats.contains(&addr(2)));
    }

    #[test]
    fn append_multiple() {
        let stats = TrackedQueueStatistics::new(track_all_shard());
        let mut other = FastHashMap::default();
        other.insert(addr(1), 5);
        other.insert(addr(2), 10);

        stats.append(&other);

        assert_eq!(stats.get(&addr(1)), Some(5));
        assert_eq!(stats.get(&addr(2)), Some(10));
        assert_eq!(stats.tracked_total(), 15);
    }

    // === Tracked total ===

    #[test]
    fn tracked_total_untracked_shard() {
        let stats = TrackedQueueStatistics::new(track_none_shard());
        stats.increment_for_account(addr(1), 5);

        assert_eq!(stats.tracked_total(), 0);
        assert_eq!(stats.get(&addr(1)), Some(5));
    }

    #[test]
    fn tracked_total_decrement() {
        let stats = TrackedQueueStatistics::new(track_all_shard());
        stats.increment_for_account(addr(1), 10);
        stats.decrement_for_account(addr(1), 3);

        assert_eq!(stats.tracked_total(), 7);
    }

    // === Transaction: commit ===

    #[test]
    fn transaction_increment_commit() {
        let mut stats = TrackedQueueStatistics::new(track_all_shard());

        stats.begin();
        stats.increment_for_account(addr(1), 5);
        assert_eq!(stats.get(&addr(1)), Some(5));

        stats.commit();
        assert_eq!(stats.get(&addr(1)), Some(5));
        assert_eq!(stats.tracked_total(), 5);
    }

    #[test]
    fn transaction_decrement_commit() {
        let mut stats = TrackedQueueStatistics::new(track_all_shard());
        stats.increment_for_account(addr(1), 10);

        stats.begin();
        stats.decrement_for_account(addr(1), 3);
        stats.commit();

        assert_eq!(stats.get(&addr(1)), Some(7));
        assert_eq!(stats.tracked_total(), 7);
    }

    // === Transaction: rollback ===

    #[test]
    fn transaction_increment_rollback() {
        let mut stats = TrackedQueueStatistics::new(track_all_shard());
        stats.increment_for_account(addr(1), 5);

        stats.begin();
        stats.increment_for_account(addr(1), 10);
        stats.increment_for_account(addr(2), 20);
        stats.rollback();

        assert_eq!(stats.get(&addr(1)), Some(5));
        assert_eq!(stats.get(&addr(2)), None);
    }

    #[test]
    fn transaction_tracked_total_rollback() {
        let mut stats = TrackedQueueStatistics::new(track_all_shard());
        stats.increment_for_account(addr(1), 10);

        stats.begin();
        stats.increment_for_account(addr(1), 5);
        stats.increment_for_account(addr(2), 20);
        assert_eq!(stats.tracked_total(), 35);

        stats.rollback();
        assert_eq!(stats.tracked_total(), 10);
    }

    // === Transaction: edge cases ===

    #[test]
    #[should_panic(expected = "nested transactions")]
    fn nested_begin_panics() {
        let mut stats = TrackedQueueStatistics::new(track_all_shard());
        stats.begin();
        stats.begin();
    }

    #[test]
    #[should_panic(expected = "no active transaction to commit")]
    fn commit_outside_transaction_panics() {
        let mut stats = TrackedQueueStatistics::new(track_all_shard());
        stats.commit();
    }

    #[test]
    #[should_panic(expected = "no active transaction to rollback")]
    fn rollback_outside_transaction_panics() {
        let mut stats = TrackedQueueStatistics::new(track_all_shard());
        stats.rollback();
    }

    #[test]
    fn multiple_transactions() {
        let mut stats = TrackedQueueStatistics::new(track_all_shard());

        stats.begin();
        stats.increment_for_account(addr(1), 5);
        stats.commit();

        stats.begin();
        stats.increment_for_account(addr(1), 10);
        stats.rollback();

        stats.begin();
        stats.increment_for_account(addr(2), 20);
        stats.commit();

        assert_eq!(stats.get(&addr(1)), Some(5));
        assert_eq!(stats.get(&addr(2)), Some(20));
        assert_eq!(stats.tracked_total(), 25);
    }

    // === Panics ===

    #[test]
    #[should_panic(expected = "non-existent account")]
    fn decrement_nonexistent_panics() {
        let stats = TrackedQueueStatistics::new(track_all_shard());
        stats.decrement_for_account(addr(1), 5);
    }

    #[test]
    #[should_panic(expected = "below zero")]
    fn decrement_below_zero_panics() {
        let stats = TrackedQueueStatistics::new(track_all_shard());
        stats.increment_for_account(addr(1), 5);
        stats.decrement_for_account(addr(1), 10);
    }

    // === Clone ===

    #[test]
    fn clone_shares_data() {
        let stats1 = TrackedQueueStatistics::new(track_all_shard());
        stats1.increment_for_account(addr(1), 5);

        let stats2 = stats1.clone();
        stats1.increment_for_account(addr(2), 10);

        assert_eq!(stats2.get(&addr(2)), Some(10));
        assert_eq!(stats2.tracked_total(), 15);
    }

    // === View ===

    #[test]
    fn statistics_view_iter() {
        let stats = TrackedQueueStatistics::new(track_all_shard());
        stats.increment_for_account(addr(1), 5);
        stats.increment_for_account(addr(2), 10);

        let view = stats.statistics();
        let collected: FastHashMap<_, _> = view.iter().map(|(k, v)| (k.clone(), v)).collect();

        assert_eq!(collected.len(), 2);
        assert_eq!(collected.get(&addr(1)), Some(&5));
        assert_eq!(collected.get(&addr(2)), Some(&10));
    }
}
