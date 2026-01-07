use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::Entry as DashMapEntry;
use dashmap::mapref::multiple::RefMulti;
use tycho_types::models::{IntAddr, ShardIdent};
use tycho_util::FastDashMap;

use crate::internal_queue::types::stats::{AccountStatistics, QueueStatistics};

#[derive(Debug)]
pub struct QueueStatisticsWithRemaning {
    /// Statistics shows all messages count
    pub initial_stats: QueueStatistics,
    /// Statistics shows remaining not read messages.
    /// We reduce initial statistics by the number of messages that were read.
    pub remaning_stats: ConcurrentQueueStatistics,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct VersionedValue {
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

#[derive(Debug)]
pub struct ConcurrentQueueStatistics {
    statistics: Arc<FastDashMap<IntAddr, VersionedValue>>,
    track_shard: ShardIdent,
    tracked_total: Arc<AtomicU64>,
    tracked_total_snapshot: AtomicU64,
}

impl Clone for ConcurrentQueueStatistics {
    fn clone(&self) -> Self {
        Self {
            statistics: Arc::clone(&self.statistics),
            track_shard: self.track_shard,
            tracked_total: Arc::clone(&self.tracked_total),
            tracked_total_snapshot: AtomicU64::new(self.tracked_total.load(Ordering::Relaxed)),
        }
    }
}

impl ConcurrentQueueStatistics {
    pub fn new(track_shard: ShardIdent) -> Self {
        Self {
            statistics: Arc::new(Default::default()),
            track_shard,
            tracked_total: Arc::new(AtomicU64::new(0)),
            tracked_total_snapshot: AtomicU64::new(0),
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = RefMulti<'_, IntAddr, VersionedValue>> + '_ {
        self.statistics
            .iter()
            .filter(|entry| entry.value().current() > 0)
    }

    pub fn tracked_total(&self) -> u64 {
        self.tracked_total.load(Ordering::Relaxed)
    }

    pub fn get(&self, account_addr: &IntAddr) -> Option<u64> {
        self.statistics
            .get(account_addr)
            .map(|v| v.current())
            .filter(|&v| v > 0)
    }

    pub fn contains(&self, account_addr: &IntAddr) -> bool {
        self.statistics
            .get(account_addr)
            .map(|v| v.current() > 0)
            .unwrap_or(false)
    }

    pub fn increment_for_account(&self, account_addr: IntAddr, count: u64) {
        let is_tracked = self.track_shard.contains_address(&account_addr);

        self.statistics
            .entry(account_addr)
            .and_modify(|v| {
                v.uncommitted = Some(v.current() + count);
            })
            .or_insert(VersionedValue {
                committed: 0,
                uncommitted: Some(count),
            });

        if is_tracked {
            self.tracked_total.fetch_add(count, Ordering::Relaxed);
        }
    }

    pub fn decrement_for_account(&self, account_addr: IntAddr, count: u64) {
        let is_tracked = self.track_shard.contains_address(&account_addr);

        match self.statistics.entry(account_addr) {
            DashMapEntry::Occupied(mut occupied) => {
                let v = occupied.get_mut();
                let current = v.current();
                if current < count {
                    panic!(
                        "attempted to decrement below zero: current={}, decrement={}",
                        current, count
                    );
                }
                v.uncommitted = Some(current - count);
            }
            DashMapEntry::Vacant(_) => {
                panic!("attempted to decrement non-existent account");
            }
        }

        if is_tracked {
            self.tracked_total.fetch_sub(count, Ordering::Relaxed);
        }
    }

    pub fn append(&self, other: &AccountStatistics) {
        let mut delta_tracked: u64 = 0;

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

            if self.track_shard.contains_address(account_addr) {
                delta_tracked += msgs_count;
            }
        }

        if delta_tracked != 0 {
            self.tracked_total
                .fetch_add(delta_tracked, Ordering::Relaxed);
        }
    }

    pub fn append_committed(&self, other: &AccountStatistics) {
        let mut delta_tracked: u64 = 0;

        for (account_addr, &msgs_count) in other {
            self.statistics
                .entry(account_addr.clone())
                .and_modify(|v| v.committed += msgs_count)
                .or_insert(VersionedValue::committed(msgs_count));

            if self.track_shard.contains_address(account_addr) {
                delta_tracked += msgs_count;
            }
        }

        if delta_tracked != 0 {
            self.tracked_total
                .fetch_add(delta_tracked, Ordering::Relaxed);
            // also update snapshot, as this is not part of a transaction
            self.tracked_total_snapshot
                .fetch_add(delta_tracked, Ordering::Relaxed);
        }
    }

    // Transactions

    pub fn begin(&self) {
        debug_assert!(
            !self.has_uncommitted(),
            "begin() called with uncommitted changes"
        );

        let current = self.tracked_total.load(Ordering::Relaxed);
        self.tracked_total_snapshot
            .store(current, Ordering::Relaxed);
    }

    pub fn commit(&self, affected: impl Iterator<Item = IntAddr>) {
        for addr in affected {
            if let DashMapEntry::Occupied(mut entry) = self.statistics.entry(addr) {
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
        // snapshot = current (transaction completed)
        let current = self.tracked_total.load(Ordering::Relaxed);
        self.tracked_total_snapshot
            .store(current, Ordering::Relaxed);
    }

    pub fn commit_all(&self) {
        self.statistics.retain(|_, v| {
            if let Some(uncommitted) = v.uncommitted.take() {
                if uncommitted == 0 {
                    return false;
                }
                v.committed = uncommitted;
            }
            true
        });

        let current = self.tracked_total.load(Ordering::Relaxed);
        self.tracked_total_snapshot
            .store(current, Ordering::Relaxed);
    }

    pub fn rollback(&self, affected: impl Iterator<Item = IntAddr>) {
        for addr in affected {
            if let DashMapEntry::Occupied(mut entry) = self.statistics.entry(addr) {
                let v = entry.get_mut();
                v.uncommitted = None;
                if v.committed == 0 {
                    entry.remove();
                }
            }
        }
        let snapshot = self.tracked_total_snapshot.load(Ordering::Relaxed);
        self.tracked_total.store(snapshot, Ordering::Relaxed);
    }

    pub fn rollback_all(&self) {
        self.statistics.retain(|_, v| {
            v.uncommitted = None;
            v.committed != 0
        });

        let snapshot = self.tracked_total_snapshot.load(Ordering::Relaxed);
        self.tracked_total.store(snapshot, Ordering::Relaxed);
    }

    pub fn has_uncommitted(&self) -> bool {
        self.statistics.iter().any(|entry| entry.value().is_dirty())
    }
}

#[cfg(test)]
mod tests {
    use tycho_util::FastHashMap;

    use super::*;

    fn addr(id: u8) -> IntAddr {
        IntAddr::Std(tycho_types::models::StdAddr::new(0, [id; 32].into()))
    }

    // Shard that contains all addresses with workchain 0
    fn track_all_shard() -> ShardIdent {
        ShardIdent::new_full(0)
    }

    // Shard that does not contain our test addresses
    fn track_none_shard() -> ShardIdent {
        ShardIdent::new_full(1) // workchain 1, our addresses are in workchain 0
    }

    fn stats_with_initial(initial: &AccountStatistics) -> ConcurrentQueueStatistics {
        let stats = ConcurrentQueueStatistics::new(track_all_shard());
        stats.append_committed(initial);
        stats
    }

    // ==================== Basic Operations ====================

    #[test]
    fn test_increment_creates_uncommitted() {
        let stats = ConcurrentQueueStatistics::new(track_all_shard());

        stats.increment_for_account(addr(1), 5);

        assert!(stats.has_uncommitted());
        assert_eq!(stats.get(&addr(1)), Some(5));
    }

    #[test]
    fn test_decrement_creates_uncommitted() {
        let mut initial = FastHashMap::default();
        initial.insert(addr(1), 10);
        let stats = stats_with_initial(&initial);

        stats.decrement_for_account(addr(1), 3);

        assert!(stats.has_uncommitted());
        assert_eq!(stats.get(&addr(1)), Some(7));
    }

    #[test]
    #[should_panic(expected = "decrement below zero")]
    fn test_decrement_below_zero_panics() {
        let mut initial = FastHashMap::default();
        initial.insert(addr(1), 5);
        let stats = stats_with_initial(&initial);

        stats.decrement_for_account(addr(1), 10);
    }

    #[test]
    #[should_panic(expected = "non-existent account")]
    fn test_decrement_nonexistent_panics() {
        let stats = ConcurrentQueueStatistics::new(track_all_shard());
        stats.decrement_for_account(addr(1), 5);
    }

    // ==================== Commit ====================

    #[test]
    fn test_commit_applies_changes() {
        let stats = ConcurrentQueueStatistics::new(track_all_shard());

        stats.increment_for_account(addr(1), 5);
        stats.increment_for_account(addr(2), 10);
        stats.commit(vec![addr(1), addr(2)].into_iter());

        assert!(!stats.has_uncommitted());
        assert_eq!(stats.get(&addr(1)), Some(5));
        assert_eq!(stats.get(&addr(2)), Some(10));
    }

    #[test]
    fn test_commit_partial() {
        let stats = ConcurrentQueueStatistics::new(track_all_shard());

        stats.increment_for_account(addr(1), 5);
        stats.increment_for_account(addr(2), 10);
        stats.commit(vec![addr(1)].into_iter());

        assert!(stats.has_uncommitted()); // addr(2) still uncommitted
        assert_eq!(stats.get(&addr(1)), Some(5));
        assert_eq!(stats.get(&addr(2)), Some(10));
    }

    #[test]
    fn test_commit_removes_zero_entries() {
        let mut initial = FastHashMap::default();
        initial.insert(addr(1), 5);
        let stats = stats_with_initial(&initial);

        stats.decrement_for_account(addr(1), 5);
        assert_eq!(stats.get(&addr(1)), None); // current = 0, filtered

        stats.commit(vec![addr(1)].into_iter());

        assert!(!stats.has_uncommitted());
        assert_eq!(stats.get(&addr(1)), None);
        assert_eq!(stats.iter().count(), 0);
    }

    #[test]
    fn test_commit_all() {
        let stats = ConcurrentQueueStatistics::new(track_all_shard());

        stats.increment_for_account(addr(1), 5);
        stats.increment_for_account(addr(2), 10);
        stats.increment_for_account(addr(3), 15);
        stats.commit_all();

        assert!(!stats.has_uncommitted());
        assert_eq!(stats.get(&addr(1)), Some(5));
        assert_eq!(stats.get(&addr(2)), Some(10));
        assert_eq!(stats.get(&addr(3)), Some(15));
    }

    #[test]
    fn test_commit_nonexistent_addr_is_noop() {
        let stats = ConcurrentQueueStatistics::new(track_all_shard());
        stats.increment_for_account(addr(1), 5);

        stats.commit(vec![addr(99)].into_iter());

        assert!(stats.has_uncommitted());
    }

    // ==================== Rollback ====================

    #[test]
    fn test_rollback_discards_changes() {
        let mut initial = FastHashMap::default();
        initial.insert(addr(1), 5);
        let stats = stats_with_initial(&initial);

        stats.begin();
        stats.increment_for_account(addr(1), 10);
        assert_eq!(stats.get(&addr(1)), Some(15));

        stats.rollback(vec![addr(1)].into_iter());

        assert!(!stats.has_uncommitted());
        assert_eq!(stats.get(&addr(1)), Some(5)); // back to committed
    }

    #[test]
    fn test_rollback_partial() {
        let stats = ConcurrentQueueStatistics::new(track_all_shard());

        stats.increment_for_account(addr(1), 5);
        stats.increment_for_account(addr(2), 10);
        stats.rollback(vec![addr(1)].into_iter());

        assert!(stats.has_uncommitted()); // addr(2) still uncommitted
        assert_eq!(stats.get(&addr(1)), None); // rolled back, was new
        assert_eq!(stats.get(&addr(2)), Some(10));
    }

    #[test]
    fn test_rollback_removes_entries_with_zero_committed() {
        let stats = ConcurrentQueueStatistics::new(track_all_shard());

        stats.increment_for_account(addr(1), 5);
        stats.rollback(vec![addr(1)].into_iter());

        assert_eq!(stats.iter().count(), 0);
    }

    #[test]
    fn test_rollback_all() {
        let mut initial = FastHashMap::default();
        initial.insert(addr(1), 5);
        let stats = stats_with_initial(&initial);

        stats.begin();
        stats.increment_for_account(addr(1), 10);
        stats.increment_for_account(addr(2), 20);
        stats.rollback_all();

        assert!(!stats.has_uncommitted());
        assert_eq!(stats.get(&addr(1)), Some(5)); // back to committed
        assert_eq!(stats.get(&addr(2)), None); // was new, removed
    }

    #[test]
    fn test_rollback_nonexistent_addr_is_noop() {
        let stats = ConcurrentQueueStatistics::new(track_all_shard());
        stats.increment_for_account(addr(1), 5);

        stats.rollback(vec![addr(99)].into_iter());

        assert!(stats.has_uncommitted());
        assert_eq!(stats.get(&addr(1)), Some(5));
    }

    // ==================== Append ====================

    #[test]
    fn test_append_creates_uncommitted() {
        let stats = ConcurrentQueueStatistics::new(track_all_shard());
        let mut other = FastHashMap::default();
        other.insert(addr(1), 5);
        other.insert(addr(2), 10);

        stats.append(&other);

        assert!(stats.has_uncommitted());
        assert_eq!(stats.get(&addr(1)), Some(5));
        assert_eq!(stats.get(&addr(2)), Some(10));
    }

    #[test]
    fn test_append_committed_no_transaction() {
        let stats = ConcurrentQueueStatistics::new(track_all_shard());
        let mut other = FastHashMap::default();
        other.insert(addr(1), 5);

        stats.append_committed(&other);

        assert!(!stats.has_uncommitted());
        assert_eq!(stats.get(&addr(1)), Some(5));
    }

    // ==================== Complex Scenarios ====================

    #[test]
    fn test_multiple_increments_before_commit() {
        let stats = ConcurrentQueueStatistics::new(track_all_shard());

        stats.increment_for_account(addr(1), 5);
        stats.increment_for_account(addr(1), 3);
        stats.increment_for_account(addr(1), 2);

        assert_eq!(stats.get(&addr(1)), Some(10));

        stats.commit_all();
        assert_eq!(stats.get(&addr(1)), Some(10));
    }

    #[test]
    fn test_increment_then_decrement_before_commit() {
        let stats = ConcurrentQueueStatistics::new(track_all_shard());

        stats.increment_for_account(addr(1), 10);
        stats.decrement_for_account(addr(1), 3);

        assert_eq!(stats.get(&addr(1)), Some(7));

        stats.commit_all();
        assert_eq!(stats.get(&addr(1)), Some(7));
    }

    // ==================== Iterator ====================

    #[test]
    fn test_iter() {
        let stats = ConcurrentQueueStatistics::new(track_all_shard());
        stats.increment_for_account(addr(1), 5);
        stats.increment_for_account(addr(2), 10);
        stats.commit_all();

        let collected: FastHashMap<_, _> = stats
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().current()))
            .collect();

        assert_eq!(collected.len(), 2);
        assert_eq!(collected.get(&addr(1)), Some(&5));
        assert_eq!(collected.get(&addr(2)), Some(&10));
    }

    #[test]
    fn test_iter_filters_zero_values() {
        let mut initial = FastHashMap::default();
        initial.insert(addr(1), 5);
        initial.insert(addr(2), 10);
        let stats = stats_with_initial(&initial);

        stats.decrement_for_account(addr(1), 5); // becomes 0

        assert_eq!(stats.iter().count(), 1);
        assert_eq!(stats.get(&addr(1)), None);
        assert_eq!(stats.get(&addr(2)), Some(10));
    }

    #[test]
    fn test_contains() {
        let stats = ConcurrentQueueStatistics::new(track_all_shard());
        stats.increment_for_account(addr(1), 5);
        stats.commit_all();

        assert!(stats.contains(&addr(1)));
        assert!(!stats.contains(&addr(2)));
    }

    // ==================== Tracked Total ====================

    #[test]
    fn test_tracked_total_increments() {
        let stats = ConcurrentQueueStatistics::new(track_all_shard());

        stats.increment_for_account(addr(1), 5);
        stats.increment_for_account(addr(2), 10);

        assert_eq!(stats.tracked_total(), 15);
    }

    #[test]
    fn test_tracked_total_decrements() {
        let mut initial = FastHashMap::default();
        initial.insert(addr(1), 10);
        let stats = stats_with_initial(&initial);

        assert_eq!(stats.tracked_total(), 10);

        stats.decrement_for_account(addr(1), 3);
        assert_eq!(stats.tracked_total(), 7);
    }

    #[test]
    fn test_tracked_total_untracked_shard() {
        let stats = ConcurrentQueueStatistics::new(track_none_shard());

        stats.increment_for_account(addr(1), 5);
        stats.increment_for_account(addr(2), 10);

        // Addresses not in tracked shard, total doesn't change
        assert_eq!(stats.tracked_total(), 0);
        // But data exists
        assert_eq!(stats.get(&addr(1)), Some(5));
        assert_eq!(stats.get(&addr(2)), Some(10));
    }

    #[test]
    fn test_tracked_total_rollback() {
        let mut initial = FastHashMap::default();
        initial.insert(addr(1), 10);
        let stats = stats_with_initial(&initial);

        stats.begin();
        stats.increment_for_account(addr(1), 5);
        stats.increment_for_account(addr(2), 20);
        assert_eq!(stats.tracked_total(), 35);

        stats.rollback_all();
        assert_eq!(stats.tracked_total(), 10); // restored from snapshot
    }

    #[test]
    fn test_tracked_total_commit() {
        let stats = ConcurrentQueueStatistics::new(track_all_shard());

        stats.begin();
        stats.increment_for_account(addr(1), 5);
        assert_eq!(stats.tracked_total(), 5);

        stats.commit_all();
        assert_eq!(stats.tracked_total(), 5); // stays the same after commit
    }

    #[test]
    fn test_append_updates_tracked_total() {
        let stats = ConcurrentQueueStatistics::new(track_all_shard());
        let mut other = FastHashMap::default();
        other.insert(addr(1), 5);
        other.insert(addr(2), 10);

        stats.append(&other);

        assert_eq!(stats.tracked_total(), 15);
    }

    #[test]
    fn test_append_committed_updates_both_total_and_snapshot() {
        let stats = ConcurrentQueueStatistics::new(track_all_shard());
        let mut other = FastHashMap::default();
        other.insert(addr(1), 10);

        stats.append_committed(&other);

        // After append_committed rollback should not rollback this data
        stats.begin();
        stats.increment_for_account(addr(2), 5);
        assert_eq!(stats.tracked_total(), 15);

        stats.rollback_all();
        assert_eq!(stats.tracked_total(), 10); // only committed part
    }

    // ==================== Begin/Transaction ====================

    #[test]
    fn test_begin_saves_snapshot() {
        let mut initial = FastHashMap::default();
        initial.insert(addr(1), 10);
        let stats = stats_with_initial(&initial);

        stats.begin();
        stats.increment_for_account(addr(1), 100);
        stats.increment_for_account(addr(2), 200);

        // Rollback should return to state at begin()
        stats.rollback_all();

        assert_eq!(stats.tracked_total(), 10);
        assert_eq!(stats.get(&addr(1)), Some(10));
        assert_eq!(stats.get(&addr(2)), None);
    }

    // ==================== Clone Semantics ====================

    #[test]
    fn test_clone_shares_statistics() {
        let stats1 = ConcurrentQueueStatistics::new(track_all_shard());
        stats1.increment_for_account(addr(1), 5);
        stats1.commit_all();

        let stats2 = stats1.clone();

        // Changes through stats1 are visible in stats2
        stats1.increment_for_account(addr(2), 10);
        stats1.commit_all();

        assert_eq!(stats2.get(&addr(2)), Some(10));
    }

    #[test]
    fn test_clone_shares_tracked_total() {
        let stats1 = ConcurrentQueueStatistics::new(track_all_shard());
        let stats2 = stats1.clone();

        stats1.increment_for_account(addr(1), 5);

        // tracked_total shared
        assert_eq!(stats1.tracked_total(), 5);
        assert_eq!(stats2.tracked_total(), 5);
    }

    #[test]
    fn test_clone_has_independent_snapshot() {
        let mut initial = FastHashMap::default();
        initial.insert(addr(1), 10);
        let stats1 = stats_with_initial(&initial);

        stats1.begin(); // snapshot = 10

        stats1.increment_for_account(addr(1), 5);
        // stats1: total = 15, snapshot = 10

        let stats2 = stats1.clone();
        // stats2: total = 15 (shared), snapshot = 15 (copy of current total)

        stats1.increment_for_account(addr(2), 100);
        // total = 115

        // Rollback stats1 - rollback to its snapshot (10)
        stats1.rollback_all();
        assert_eq!(stats1.tracked_total(), 10);

        // stats2 sees the same total (they are shared)
        assert_eq!(stats2.tracked_total(), 10);
    }

    // ==================== Edge Cases ====================

    #[test]
    fn test_decrement_to_zero_then_increment() {
        let mut initial = FastHashMap::default();
        initial.insert(addr(1), 5);
        let stats = stats_with_initial(&initial);

        stats.decrement_for_account(addr(1), 5);
        assert_eq!(stats.get(&addr(1)), None); // filtered as zero

        stats.increment_for_account(addr(1), 3);
        assert_eq!(stats.get(&addr(1)), Some(3));
    }

    #[test]
    fn test_commit_then_new_transaction() {
        let stats = ConcurrentQueueStatistics::new(track_all_shard());

        // First transaction
        stats.begin();
        stats.increment_for_account(addr(1), 5);
        stats.commit_all();

        // Second transaction
        stats.begin();
        stats.increment_for_account(addr(1), 10);
        stats.rollback_all();

        // Should be at state after first commit
        assert_eq!(stats.get(&addr(1)), Some(5));
        assert_eq!(stats.tracked_total(), 5);
    }

    #[test]
    fn test_multiple_rollbacks() {
        let mut initial = FastHashMap::default();
        initial.insert(addr(1), 10);
        let stats = stats_with_initial(&initial);

        stats.begin();
        stats.increment_for_account(addr(1), 5);
        stats.rollback_all();

        // Repeated rollback without changes — should be safe
        stats.rollback_all();

        assert_eq!(stats.get(&addr(1)), Some(10));
        assert_eq!(stats.tracked_total(), 10);
    }

    #[test]
    fn test_empty_commit_and_rollback() {
        let stats = ConcurrentQueueStatistics::new(track_all_shard());

        stats.begin();
        // Do nothing
        stats.commit_all();

        assert!(!stats.has_uncommitted());
        assert_eq!(stats.tracked_total(), 0);

        stats.begin();
        stats.rollback_all();

        assert!(!stats.has_uncommitted());
        assert_eq!(stats.tracked_total(), 0);
    }
}
