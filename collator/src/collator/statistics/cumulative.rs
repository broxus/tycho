use std::collections::hash_map::Entry;

use anyhow::Context;
use tycho_block_util::queue::{QueueKey, QueuePartitionIdx};
use tycho_types::models::ShardIdent;
use tycho_util::transactional::Transactional;
use tycho_util::{FastHashMap, FastHashSet};

use crate::collator::statistics::queue::TrackedQueueStatistics;
use crate::internal_queue::types::message::InternalMessageValue;
use crate::internal_queue::types::ranges::QueueShardBoundedRange;
use crate::internal_queue::types::stats::{
    AccountStatistics, DiffStatistics, QueueStatistics, SeparatedStatisticsByPartitions,
};
use crate::queue_adapter::MessageQueueAdapter;
use crate::types::ProcessedToByPartitions;

#[derive(Clone, Copy)]
enum ProcessMode {
    /// Call inside transaction — we do decrement, but retain is delayed
    Staged,
    /// Call outside transaction — do both decrement and retain
    Immediate,
    /// Call on commit — only retain, decrement was already done
    CommitRetainOnly,
}

#[derive(Debug)]
pub struct QueueStatisticsWithRemaning {
    pub initial_stats: QueueStatistics,
    pub remaning_stats: TrackedQueueStatistics,
}

#[derive(Default)]
struct CumulativeStatisticsTx {
    /// Stores added diffs during the transaction
    added_diffs: Vec<(ShardIdent, QueuePartitionIdx, QueueKey)>,

    /// Stores newly added partitions during the transaction
    new_partitions: FastHashSet<QueuePartitionIdx>,

    /// Snapshot of `all_shards_processed_to_by_partitions` at the start of the transaction
    all_shards_processed_to_by_partitions: FastHashMap<ShardIdent, (bool, ProcessedToByPartitions)>,

    /// Stores `processed_to` updates during the transaction
    processed_updates: FastHashMap<ShardIdent, ProcessedToByPartitions>,
}

pub struct CumulativeStatistics {
    /// Cumulative statistics created for this shard. When reader reads messages, it decrements `remaining messages`
    /// Another shard stats can be decremented only by calling `update_processed_to_by_partitions`
    for_shard: ShardIdent,

    /// Actual processed to info for master and all shards
    all_shards_processed_to_by_partitions: FastHashMap<ShardIdent, (bool, ProcessedToByPartitions)>,

    /// Stores per-shard statistics, keyed by `ShardIdent`.
    /// `ShardIdent` is the source shard of the diff statistics
    /// Each shard has a `FastHashMap<QueuePartitionIdx, BTreeMap<QueueKey, AccountStatistics>>`
    shards_stats_by_partitions: FastHashMap<ShardIdent, SeparatedStatisticsByPartitions>,

    /// The final aggregated statistics (across all shards) by partitions.
    result: FastHashMap<QueuePartitionIdx, QueueStatisticsWithRemaning>,

    /// Transactional state
    tx: Option<CumulativeStatisticsTx>,
}

impl Transactional for CumulativeStatistics {
    fn begin(&mut self) {
        assert!(self.tx.is_none(), "transaction already in progress");

        self.tx = Some(CumulativeStatisticsTx {
            all_shards_processed_to_by_partitions: self
                .all_shards_processed_to_by_partitions
                .clone(),
            ..Default::default()
        });

        for stats in self.result.values_mut() {
            stats.initial_stats.begin();
            stats.remaning_stats.begin();
        }
    }

    fn commit(&mut self) {
        let Some(tx) = self.tx.take() else {
            panic!("no transaction in progress")
        };

        // Commit stats changes (skip newly added entries without active transaction)
        for stats in self.result.values_mut() {
            if stats.initial_stats.in_tx() {
                stats.initial_stats.commit();
            }
            if stats.remaning_stats.in_tx() {
                stats.remaning_stats.commit();
            }
        }

        for (dst_shard, processed_to) in &tx.processed_updates {
            Self::process_processed_to_update(
                &mut self.result,
                &mut self.shards_stats_by_partitions,
                &self.for_shard,
                dst_shard,
                processed_to,
                ProcessMode::CommitRetainOnly,
            );
        }
    }

    fn rollback(&mut self) {
        let Some(tx) = self.tx.take() else {
            panic!("no transaction in progress")
        };

        // Rollback result (skip newly added entries without active transaction)
        for stats in self.result.values_mut() {
            if stats.initial_stats.in_tx() {
                stats.initial_stats.rollback();
            }
            if stats.remaning_stats.in_tx() {
                stats.remaning_stats.rollback();
            }
        }

        // Remove added partitions
        for partition in tx.new_partitions {
            self.result.remove(&partition);
        }

        // Remove added diffs
        for (diff_shard, partition, diff_max_message) in tx.added_diffs {
            if let Some(shard_stats) = self.shards_stats_by_partitions.get_mut(&diff_shard)
                && let Some(partition_stats) = shard_stats.get_mut(&partition)
            {
                partition_stats.remove(&diff_max_message);
            }
        }

        self.shards_stats_by_partitions.retain(|_, by_partitions| {
            by_partitions.retain(|_, diffs| !diffs.is_empty());
            !by_partitions.is_empty()
        });

        // Restore processed_to info
        self.all_shards_processed_to_by_partitions = tx.all_shards_processed_to_by_partitions;
    }

    fn in_tx(&self) -> bool {
        self.tx.is_some()
    }
}

impl CumulativeStatistics {
    pub fn new(
        for_shard: ShardIdent,
        all_shards_processed_to_by_partitions: FastHashMap<
            ShardIdent,
            (bool, ProcessedToByPartitions),
        >,
    ) -> Self {
        Self {
            for_shard,
            all_shards_processed_to_by_partitions,
            shards_stats_by_partitions: Default::default(),
            result: Default::default(),
            tx: None,
        }
    }

    /// Load diff statistics for the specified ranges and partitions
    pub fn load_ranges<V: InternalMessageValue>(
        &mut self,
        mq_adapter: &dyn MessageQueueAdapter<V>,
        partitions: &FastHashSet<QueuePartitionIdx>,
        ranges: &[QueueShardBoundedRange],
    ) -> anyhow::Result<()> {
        for range in ranges {
            let stats_by_partitions = mq_adapter
                .load_separated_diff_statistics(partitions, range)
                .with_context(|| format!("partitions: {partitions:?}; range: {range:?}"))?;

            for (partition, partition_stats) in stats_by_partitions {
                for (diff_max_message, diff_partition_stats) in partition_stats {
                    self.apply_diff(
                        partition,
                        range.shard_ident,
                        diff_max_message,
                        diff_partition_stats,
                    );
                }
            }
        }
        Ok(())
    }

    /// Update `all_shards_processed_to_by_partitions` and remove
    /// processed data
    pub fn update_processed_to_by_partitions(
        &mut self,
        new_processed_to_by_shards: FastHashMap<ShardIdent, (bool, ProcessedToByPartitions)>,
    ) {
        if self.all_shards_processed_to_by_partitions == new_processed_to_by_shards {
            return;
        }

        for (dst_shard, (_, new_processed_to)) in &new_processed_to_by_shards {
            let changed = match self.all_shards_processed_to_by_partitions.get(dst_shard) {
                Some((_, old_processed_to)) => old_processed_to != new_processed_to,
                None => true,
            };

            if changed {
                self.handle_processed_to_update(dst_shard, new_processed_to);
            }
        }

        self.all_shards_processed_to_by_partitions = new_processed_to_by_shards;
    }

    /// Handle processed to update for a specific shard.
    pub fn handle_processed_to_update(
        &mut self,
        dst_shard: &ShardIdent,
        processed_to: &ProcessedToByPartitions,
    ) {
        let in_tx = self.in_tx();

        Self::process_processed_to_update(
            &mut self.result,
            &mut self.shards_stats_by_partitions,
            &self.for_shard,
            dst_shard,
            processed_to,
            if in_tx {
                ProcessMode::Staged
            } else {
                ProcessMode::Immediate
            },
        );

        if let Some(tx) = &mut self.tx {
            tx.processed_updates
                .insert(*dst_shard, processed_to.clone());
        }

        self.all_shards_processed_to_by_partitions
            .insert(*dst_shard, (true, processed_to.clone()));
    }

    /// Apply diff statistics to cumulative stats.
    pub fn apply_diff_stats(
        &mut self,
        diff_shard: ShardIdent,
        diff_max_message: QueueKey,
        diff_stats: DiffStatistics,
    ) {
        for (&partition, diff_partition_stats) in diff_stats.iter() {
            self.apply_diff(
                partition,
                diff_shard,
                diff_max_message,
                diff_partition_stats.clone(),
            );
        }
    }

    /// Returns a reference to the aggregated stats by partitions.
    pub fn result(&self) -> &FastHashMap<QueuePartitionIdx, QueueStatisticsWithRemaning> {
        &self.result
    }

    /// Calc aggregated stats among all partitions.
    pub fn get_aggregated_result(&self) -> QueueStatistics {
        let mut res: Option<QueueStatistics> = None;
        for stats in self.result.values() {
            if let Some(aggregated) = res.as_mut() {
                for (addr, count) in stats.initial_stats.statistics() {
                    aggregated.increment_for_account(addr.clone(), count);
                }
            } else {
                res.replace(stats.initial_stats.clone());
            }
        }
        res.unwrap_or_default()
    }

    pub fn remaining_total_for_own_shard(&self) -> u64 {
        self.result
            .values()
            .map(|stats| stats.remaning_stats.tracked_total())
            .sum()
    }

    // Private methods
    fn apply_diff(
        &mut self,
        partition: QueuePartitionIdx,
        diff_shard: ShardIdent,
        diff_max_message: QueueKey,
        mut diff_partition_stats: AccountStatistics,
    ) {
        let mut tx = self.tx.as_mut();

        for (dst_shard, (_, shard_processed_to_by_partitions)) in
            &self.all_shards_processed_to_by_partitions
        {
            if let Some(partition_processed_to) = shard_processed_to_by_partitions.get(&partition)
                && let Some(to_key) = partition_processed_to.get(&diff_shard)
                && diff_max_message <= *to_key
            {
                diff_partition_stats.retain(|dst_acc, _| !dst_shard.contains_address(dst_acc));
            }
        }

        let entry = match self.result.entry(partition) {
            Entry::Vacant(e) => {
                if let Some(tx) = &mut tx {
                    tx.new_partitions.insert(partition);
                }
                e.insert(QueueStatisticsWithRemaning {
                    initial_stats: QueueStatistics::default(),
                    remaning_stats: TrackedQueueStatistics::new(self.for_shard),
                })
            }
            Entry::Occupied(e) => e.into_mut(),
        };

        entry.initial_stats.append(&diff_partition_stats);
        entry.remaning_stats.append(&diff_partition_stats);

        self.shards_stats_by_partitions
            .entry(diff_shard)
            .or_default()
            .entry(partition)
            .or_default()
            .insert(diff_max_message, diff_partition_stats);

        if let Some(tx) = tx {
            tx.added_diffs
                .push((diff_shard, partition, diff_max_message));
        }
    }

    fn process_processed_to_update(
        result: &mut FastHashMap<QueuePartitionIdx, QueueStatisticsWithRemaning>,
        shards_stats: &mut FastHashMap<ShardIdent, SeparatedStatisticsByPartitions>,
        for_shard: &ShardIdent,
        dst_shard: &ShardIdent,
        processed_to: &ProcessedToByPartitions,
        mode: ProcessMode,
    ) {
        let do_decrement = matches!(mode, ProcessMode::Staged | ProcessMode::Immediate);
        let do_retain = matches!(mode, ProcessMode::Immediate | ProcessMode::CommitRetainOnly);

        for (src_shard, shard_stats) in shards_stats.iter_mut() {
            for (partition, diffs) in shard_stats.iter_mut() {
                let Some(partition_pt) = processed_to.get(partition) else {
                    continue;
                };
                let Some(to_key) = partition_pt.get(src_shard) else {
                    continue;
                };

                for (_, diff_stats) in diffs.range_mut(..=to_key) {
                    if do_decrement {
                        let cumulative_stats = result.entry(*partition).or_insert_with(|| {
                            QueueStatisticsWithRemaning {
                                initial_stats: QueueStatistics::default(),
                                remaning_stats: TrackedQueueStatistics::new(*for_shard),
                            }
                        });

                        let mut remaning_guard = (for_shard != dst_shard)
                            .then(|| cumulative_stats.remaning_stats.statistics_mut());

                        for (dest_addr, count) in diff_stats.iter() {
                            if dst_shard.contains_address(dest_addr) {
                                cumulative_stats
                                    .initial_stats
                                    .decrement_for_account(dest_addr.clone(), *count);
                                if let Some(guard) = &mut remaning_guard {
                                    guard.decrement_for_account(dest_addr.clone(), *count);
                                }
                            }
                        }
                    }
                    if do_retain {
                        diff_stats.retain(|acc, _| !dst_shard.contains_address(acc));
                    }
                }

                if do_retain {
                    diffs.retain(|_, stats| !stats.is_empty());
                }
            }
        }

        if do_retain {
            shards_stats.retain(|_, by_partitions| {
                by_partitions.retain(|_, diffs| !diffs.is_empty());
                !by_partitions.is_empty()
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use tycho_types::cell::HashBytes;
    use tycho_types::models::{IntAddr, ShardIdent, StdAddr};

    use super::*;

    // Helper to create a dummy shard
    fn mock_shard(id: i32) -> ShardIdent {
        ShardIdent::new_full(id)
    }

    fn mock_addr(id: i8) -> IntAddr {
        IntAddr::Std(StdAddr::new(id, HashBytes::default()))
    }

    #[test]
    fn test_apply_diff_and_result_consistency() {
        // GIVEN: A CumulativeStatistics instance
        let current_shard = mock_shard(1);
        let mut cs = CumulativeStatistics::new(current_shard, Default::default());

        let partition = QueuePartitionIdx(0);
        let from_shard = mock_shard(2);
        let key = QueueKey::default();

        let mut stats = AccountStatistics::default();
        let addr = IntAddr::default();
        stats.insert(addr.clone(), 10);

        // WHEN: Applying a new diff
        cs.apply_diff(partition, from_shard, key, stats);

        // THEN: Result should reflect the added statistics
        let res = cs.result().get(&partition).unwrap();
        assert_eq!(res.initial_stats.total_count(), 10);
        assert!(cs.shards_stats_by_partitions.contains_key(&from_shard));
    }

    #[test]
    fn test_transactional_commit_flow() {
        // GIVEN: Added data and a started transaction
        let for_shard = mock_shard(1);
        let mut cs = CumulativeStatistics::new(for_shard, Default::default());

        let partition = 0.into();
        let from_shard = mock_shard(2);

        let mut stats = AccountStatistics::default();

        // messages to shard wc1
        stats.insert(mock_addr(1), 5);
        // diff created from shard wc2 with messages to wc1
        cs.apply_diff(partition, from_shard, QueueKey::max_for_lt(10), stats);
        assert_eq!(
            cs.result()
                .get(&partition)
                .unwrap()
                .initial_stats
                .total_count(),
            5
        );
        assert!(
            !cs.shards_stats_by_partitions
                .get(&from_shard)
                .unwrap()
                .get(&partition)
                .unwrap()
                .is_empty()
        );

        cs.begin();

        // WHEN: Updating processed_to during transaction (Staged mode)
        let mut pt_map = ProcessedToByPartitions::default();
        let mut inner = BTreeMap::default();
        // messages in shard wc2 up to key 10 are processed
        inner.insert(from_shard, QueueKey::max_for_lt(10));
        pt_map.insert(partition, inner);

        cs.handle_processed_to_update(&for_shard, &pt_map);

        // THEN: Statistics are decremented, but data is NOT yet removed (no retain)
        assert_eq!(
            cs.result()
                .get(&partition)
                .unwrap()
                .initial_stats
                .total_count(),
            0
        );
        assert!(
            !cs.shards_stats_by_partitions
                .get(&from_shard)
                .unwrap()
                .get(&partition)
                .unwrap()
                .is_empty()
        );

        // WHEN: Commit is called
        cs.commit();

        // THEN: Data is physically removed from internal storage via retain
        assert!(cs.shards_stats_by_partitions.is_empty());
    }

    #[test]
    fn test_transactional_rollback_flow() {
        // GIVEN: Initial state and some changes in transaction
        let for_shard = mock_shard(1);
        let from_shard = mock_shard(2);
        let mut cs = CumulativeStatistics::new(for_shard, Default::default());
        let partition = 0.into();

        let mut stats = AccountStatistics::default();
        stats.insert(mock_addr(1), 100);
        cs.apply_diff(partition, from_shard, QueueKey::max_for_lt(10), stats);

        cs.begin();

        let mut stats = AccountStatistics::default();
        stats.insert(mock_addr(1), 10);
        cs.apply_diff(partition, from_shard, QueueKey::max_for_lt(20), stats);

        let mut pt_map = ProcessedToByPartitions::default();
        let mut inner = BTreeMap::default();
        inner.insert(from_shard, QueueKey::max_for_lt(10));
        pt_map.insert(partition, inner);

        assert_eq!(
            cs.result()
                .get(&partition)
                .unwrap()
                .initial_stats
                .total_count(),
            110
        );

        cs.handle_processed_to_update(&for_shard, &pt_map);
        assert_eq!(
            cs.result()
                .get(&partition)
                .unwrap()
                .initial_stats
                .total_count(),
            10
        );

        // WHEN: Rollback is called
        cs.rollback();

        // THEN: Everything must be restored to pre-transaction state
        assert_eq!(
            cs.result()
                .get(&partition)
                .unwrap()
                .initial_stats
                .total_count(),
            100
        );
        assert!(!cs.shards_stats_by_partitions.is_empty());
    }

    #[test]
    fn test_immediate_mode_without_tx() {
        // GIVEN: No active transaction
        let for_shard = mock_shard(1);
        let from_shard = mock_shard(2);

        let mut cs = CumulativeStatistics::new(for_shard, Default::default());
        let partition = 0.into();

        let dest_addr = mock_addr(1);

        let mut stats = AccountStatistics::default();
        // send messages to shard wc2
        stats.insert(dest_addr, 50);

        cs.apply_diff(partition, from_shard, QueueKey::max_for_lt(5), stats);

        assert_eq!(
            cs.result()
                .get(&partition)
                .unwrap()
                .initial_stats
                .total_count(),
            50
        );

        // WHEN: Updating processed_to (Immediate mode)
        let mut pt_map = ProcessedToByPartitions::default();
        let mut inner = BTreeMap::default();
        inner.insert(from_shard, QueueKey::max_for_lt(5));
        pt_map.insert(partition, inner);

        cs.handle_processed_to_update(&for_shard, &pt_map);

        // THEN: Both decrement and retain happen immediately
        assert_eq!(
            cs.result()
                .get(&partition)
                .unwrap()
                .initial_stats
                .total_count(),
            0
        );
        assert!(cs.shards_stats_by_partitions.is_empty());
    }

    #[test]
    fn test_cascading_retain_logic() {
        // GIVEN: Multiple nested entries
        let shard = mock_shard(1);
        let mut cs = CumulativeStatistics::new(shard, Default::default());

        let mut stats = AccountStatistics::default();
        stats.insert(mock_addr(1), 1);

        cs.apply_diff(0.into(), shard, QueueKey::max_for_lt(1), stats);

        // WHEN: Setting processed_to to cover the diff
        let mut pt_map = ProcessedToByPartitions::default();
        let mut inner = BTreeMap::default();
        inner.insert(shard, QueueKey::max_for_lt(1));
        pt_map.insert(0.into(), inner);

        // Immediate update to trigger retain
        cs.handle_processed_to_update(&shard, &pt_map);

        // THEN: Empty DiffStatistics -> Empty Partition Map -> Empty Shard Map
        // All levels should be cleaned up by nested retain calls
        assert!(
            cs.shards_stats_by_partitions.is_empty(),
            "Should clean up empty shards"
        );
    }

    #[test]
    fn test_remaning_stats_tracking_and_filters() {
        // GIVEN: Cumulative stats for Shard 1
        let for_shard = mock_shard(1);
        let other_shard = mock_shard(2);
        let mut cs = CumulativeStatistics::new(for_shard, Default::default());
        let partition = 0.into();

        let mut stats = AccountStatistics::default();
        // 10 messages for OUR shard (1)
        stats.insert(mock_addr(1), 10);
        // 5 messages for OTHER shard (2)
        stats.insert(mock_addr(2), 5);

        // WHEN: Applying diff
        cs.apply_diff(partition, other_shard, QueueKey::max_for_lt(100), stats);

        // THEN:
        // initial_stats tracks everything (10 + 5 = 15)
        // remaning_stats.tracked_total only tracks messages for shard 1 (10)
        let res = cs.result().get(&partition).unwrap();
        assert_eq!(res.initial_stats.total_count(), 15);
        assert_eq!(res.remaning_stats.tracked_total(), 10);
        assert_eq!(cs.remaining_total_for_own_shard(), 10);
    }

    #[test]
    fn test_remaning_stats_decrement_logic_for_different_shards() {
        let for_shard = mock_shard(1);
        let other_shard = mock_shard(2);
        let mut cs = CumulativeStatistics::new(for_shard, Default::default());
        let partition = 0.into();

        let mut stats = AccountStatistics::default();
        stats.insert(mock_addr(2), 5); // 5 messages for shard 2
        cs.apply_diff(partition, other_shard, QueueKey::max_for_lt(10), stats);

        // WHEN: Other shard (2) updates its processed_to
        let mut pt_map = ProcessedToByPartitions::default();
        let mut inner = BTreeMap::default();
        inner.insert(other_shard, QueueKey::max_for_lt(10));
        pt_map.insert(partition, inner);

        cs.handle_processed_to_update(&other_shard, &pt_map);

        // THEN:
        // 1. initial_stats must be 0 (messages processed)
        // 2. remaning_stats must ALSO be decremented because for_shard != dst_shard
        let res = cs.result().get(&partition).unwrap();
        assert_eq!(res.initial_stats.total_count(), 0);

        // Check that remaning_stats for shard 2's account is also 0
        assert_eq!(res.remaning_stats.get(&mock_addr(2)), None);
    }

    #[test]
    fn test_remaning_stats_no_decrement_for_own_shard() {
        let for_shard = mock_shard(1);
        let mut cs = CumulativeStatistics::new(for_shard, Default::default());
        let partition = 0.into();

        let mut stats = AccountStatistics::default();
        stats.insert(mock_addr(1), 10); // 10 messages for OUR shard
        cs.apply_diff(partition, for_shard, QueueKey::max_for_lt(10), stats);

        // WHEN: OUR shard (1) updates its processed_to
        let mut pt_map = ProcessedToByPartitions::default();
        let mut inner = BTreeMap::default();
        inner.insert(for_shard, QueueKey::max_for_lt(10));
        pt_map.insert(partition, inner);

        cs.handle_processed_to_update(&for_shard, &pt_map);

        // THEN:
        // 1. initial_stats IS decremented (global watermark moved)
        // 2. remaning_stats IS NOT decremented (as per `if for_shard != dst_shard` logic)
        let res = cs.result().get(&partition).unwrap();
        assert_eq!(res.initial_stats.total_count(), 0);
        assert_eq!(
            res.remaning_stats.tracked_total(),
            10,
            "Should NOT decrement remaning_stats for own shard"
        );
    }

    #[test]
    fn test_transactional_remaning_stats_rollback() {
        // GIVEN: initial state
        let for_shard = mock_shard(1);
        let mut cs = CumulativeStatistics::new(for_shard, Default::default());
        let partition = 0.into();

        let mut stats = AccountStatistics::default();
        stats.insert(mock_addr(1), 50);
        cs.apply_diff(partition, for_shard, QueueKey::max_for_lt(10), stats);

        assert_eq!(cs.remaining_total_for_own_shard(), 50);

        // WHEN: In transaction we add more and then rollback
        cs.begin();

        let mut stats_tx = AccountStatistics::default();
        stats_tx.insert(mock_addr(1), 30);
        cs.apply_diff(partition, for_shard, QueueKey::max_for_lt(20), stats_tx);

        assert_eq!(cs.remaining_total_for_own_shard(), 80);

        cs.rollback();

        // THEN: remaning_stats should be back to 50
        assert_eq!(cs.remaining_total_for_own_shard(), 50);
    }
}
