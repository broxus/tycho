use std::collections::hash_map::Entry;
use std::sync::Arc;

use anyhow::Context;
use tycho_block_util::queue::{QueueKey, QueuePartitionIdx};
use tycho_types::models::{IntAddr, ShardIdent};
use tycho_util::{FastHashMap, FastHashSet};

use crate::collator::statistics::queue::{ConcurrentQueueStatistics, QueueStatisticsWithRemaning};
use crate::internal_queue::types::message::InternalMessageValue;
use crate::internal_queue::types::ranges::{Bound, QueueShardBoundedRange};
use crate::internal_queue::types::stats::{
    AccountStatistics, DiffStatistics, QueueStatistics, SeparatedStatisticsByPartitions,
};
use crate::queue_adapter::MessageQueueAdapter;
use crate::tracing_targets;
use crate::types::ProcessedToByPartitions;
use crate::types::processed_upto::{Lt, find_min_processed_to_by_shards};

pub struct CumulativeStatistics {
    /// Cumulative statistics created for this shard. When reader reads messages, it decrements `remaining messages`
    /// Another shard stats can be decremented only by calling `update_processed_to_by_partitions`
    for_shard: ShardIdent,
    /// Actual processed to info for master and all shards
    all_shards_processed_to_by_partitions: FastHashMap<ShardIdent, (bool, ProcessedToByPartitions)>,

    /// Stores per-shard statistics, keyed by `ShardIdent`.
    /// Each shard has a `FastHashMap<QueuePartitionIdx, BTreeMap<QueueKey, AccountStatistics>>`
    shards_stats_by_partitions: FastHashMap<ShardIdent, SeparatedStatisticsByPartitions>,

    /// The final aggregated statistics (across all shards) by partitions.
    result: FastHashMap<QueuePartitionIdx, QueueStatisticsWithRemaning>,

    /// Transactions
    staged_additions: Vec<(QueuePartitionIdx, ShardIdent, QueueKey, AccountStatistics)>,
    new_partitions: FastHashSet<QueuePartitionIdx>,

    /// Backup of processed_to for rollback (copied at the start of transaction)
    processed_to_backup: Option<FastHashMap<ShardIdent, (bool, ProcessedToByPartitions)>>,

    /// Marks which (src_shard, partition, diff_key, dst_shard) are processed
    /// Instead of retain() — on commit we actually remove accounts
    processed_diff_shards: FastHashSet<(ShardIdent, QueuePartitionIdx, QueueKey, ShardIdent)>,
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
            staged_additions: vec![],
            new_partitions: Default::default(),
            processed_to_backup: None,
            processed_diff_shards: Default::default(),
        }
    }

    /// Create range and full load statistics and store it
    pub fn load<V: InternalMessageValue>(
        &mut self,
        mq_adapter: Arc<dyn MessageQueueAdapter<V>>,
        partitions: &FastHashSet<QueuePartitionIdx>,
        prev_state_gen_lt: Lt,
        mc_state_gen_lt: Lt,
        mc_top_shards_end_lts: &FastHashMap<ShardIdent, Lt>,
    ) -> anyhow::Result<()> {
        let ranges = Self::compute_cumulative_stats_ranges(
            &self.for_shard,
            &self.all_shards_processed_to_by_partitions,
            prev_state_gen_lt,
            mc_state_gen_lt,
            mc_top_shards_end_lts,
        );

        tracing::trace!(
            target: tracing_targets::COLLATOR,
            "cumulative_stats_ranges: {:?}",
            ranges
        );

        for range in ranges {
            let stats_by_partitions = mq_adapter
                .load_separated_diff_statistics(partitions, &range)
                .with_context(|| format!("partitions: {partitions:?}; range: {range:?}"))?;

            for (partition, partition_stats) in stats_by_partitions {
                for (diff_max_message, diff_partition_stats) in partition_stats {
                    self.apply_immediate(
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

    /// Partially loads diff statistics for the given ranges.
    /// Automatically applies `processed_to` filtering and updates internal state.
    pub fn load_partial<V: InternalMessageValue>(
        &mut self,
        mq_adapter: Arc<dyn MessageQueueAdapter<V>>,
        partitions: &FastHashSet<QueuePartitionIdx>,
        ranges: Vec<QueueShardBoundedRange>,
    ) -> anyhow::Result<()> {
        tracing::trace!(
            target: tracing_targets::COLLATOR,
            "cumulative_stats_partial_ranges: {:?}",
            ranges
        );

        // self.load_internal(mq_adapter, partitions, ranges)

        for range in ranges {
            let stats_by_partitions = mq_adapter
                .load_separated_diff_statistics(partitions, &range)
                .with_context(|| format!("partitions: {partitions:?}; range: {range:?}"))?;

            for (partition, partition_stats) in stats_by_partitions {
                for (diff_max_message, diff_partition_stats) in partition_stats {
                    self.apply(
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
        new_pt: FastHashMap<ShardIdent, (bool, ProcessedToByPartitions)>,
    ) {
        if self.all_shards_processed_to_by_partitions == new_pt {
            return;
        }
        for (&dst_shard, (_, processed_to)) in &new_pt {
            let changed = match self.all_shards_processed_to_by_partitions.get(&dst_shard) {
                Some((_, old_pt)) => old_pt != processed_to,
                None => true,
            };

            if changed {
                self.handle_processed_to_update(dst_shard, processed_to.clone());
            }
        }

        self.all_shards_processed_to_by_partitions = new_pt;
    }

    pub(crate) fn compute_cumulative_stats_ranges(
        current_shard: &ShardIdent,
        all_shards_processed_to_by_partitions: &FastHashMap<
            ShardIdent,
            (bool, ProcessedToByPartitions),
        >,
        prev_state_gen_lt: Lt,
        mc_state_gen_lt: Lt,
        mc_top_shards_end_lts: &FastHashMap<ShardIdent, Lt>,
    ) -> Vec<QueueShardBoundedRange> {
        let mut ranges = vec![];

        let from_ranges = find_min_processed_to_by_shards(all_shards_processed_to_by_partitions);

        for (shard_ident, from) in from_ranges {
            let to_lt = if shard_ident.is_masterchain() {
                mc_state_gen_lt
            } else if shard_ident == *current_shard {
                prev_state_gen_lt
            } else {
                *mc_top_shards_end_lts.get(&shard_ident).unwrap()
            };

            let to = Bound::Included(QueueKey::max_for_lt(to_lt));

            ranges.push(QueueShardBoundedRange {
                shard_ident,
                from: Bound::Excluded(from),
                to,
            });
        }

        ranges
    }

    fn apply_immediate(
        &mut self,
        partition: QueuePartitionIdx,
        diff_shard: ShardIdent,
        diff_max_message: QueueKey,
        mut diff_partition_stats: AccountStatistics,
    ) {
        for (dst_shard, (_, shard_processed_to_by_partitions)) in
            &self.all_shards_processed_to_by_partitions
        {
            if let Some(partition_processed_to) = shard_processed_to_by_partitions.get(&partition) {
                if let Some(to_key) = partition_processed_to.get(&diff_shard) {
                    if diff_max_message <= *to_key {
                        diff_partition_stats
                            .retain(|dst_acc, _| !dst_shard.contains_address(dst_acc));
                    }
                }
            }
        }

        let entry = self
            .result
            .entry(partition)
            .or_insert_with(|| QueueStatisticsWithRemaning {
                initial_stats: QueueStatistics::default(),
                remaning_stats: ConcurrentQueueStatistics::new(self.for_shard),
            });

        entry.initial_stats.append_committed(&diff_partition_stats);
        entry.remaning_stats.append_committed(&diff_partition_stats);

        self.shards_stats_by_partitions
            .entry(diff_shard)
            .or_default()
            .entry(partition)
            .or_default()
            .insert(diff_max_message, diff_partition_stats);
    }

    /// Staged application — with transaction support
    fn apply(
        &mut self,
        partition: QueuePartitionIdx,
        diff_shard: ShardIdent,
        diff_max_message: QueueKey,
        mut diff_partition_stats: AccountStatistics,
    ) {
        for (dst_shard, (_, shard_processed_to_by_partitions)) in
            &self.all_shards_processed_to_by_partitions
        {
            if let Some(partition_processed_to) = shard_processed_to_by_partitions.get(&partition) {
                if let Some(to_key) = partition_processed_to.get(&diff_shard) {
                    if diff_max_message <= *to_key {
                        diff_partition_stats
                            .retain(|dst_acc, _| !dst_shard.contains_address(dst_acc));
                    }
                }
            }
        }

        let entry = match self.result.entry(partition) {
            Entry::Vacant(e) => {
                self.new_partitions.insert(partition);
                e.insert(QueueStatisticsWithRemaning {
                    initial_stats: QueueStatistics::default(),
                    remaning_stats: ConcurrentQueueStatistics::new(self.for_shard),
                })
            }
            Entry::Occupied(e) => e.into_mut(),
        };

        entry.initial_stats.append(&diff_partition_stats);
        entry.remaning_stats.append(&diff_partition_stats);

        self.staged_additions.push((
            partition,
            diff_shard,
            diff_max_message,
            diff_partition_stats,
        ));
    }

    pub fn add_diff_stats(
        &mut self,
        diff_shard: ShardIdent,
        diff_max_message: QueueKey,
        diff_stats: DiffStatistics,
    ) {
        for (&partition, diff_partition_stats) in diff_stats.iter() {
            let entry = match self.result.entry(partition) {
                Entry::Vacant(e) => {
                    self.new_partitions.insert(partition);
                    e.insert(QueueStatisticsWithRemaning {
                        initial_stats: QueueStatistics::default(),
                        remaning_stats: ConcurrentQueueStatistics::new(self.for_shard),
                    })
                }
                Entry::Occupied(e) => e.into_mut(),
            };

            entry.initial_stats.append(diff_partition_stats);
            entry.remaning_stats.append(diff_partition_stats);

            self.staged_additions.push((
                partition,
                diff_shard,
                diff_max_message,
                diff_partition_stats.clone(),
            ));
        }
    }

    pub fn handle_processed_to_update(
        &mut self,
        dst_shard: ShardIdent,
        shard_processed_to_by_partitions: ProcessedToByPartitions,
    ) {
        for (src_shard, shard_stats_by_partitions) in self.shards_stats_by_partitions.iter() {
            for (partition, diffs) in shard_stats_by_partitions.iter() {
                if let Some(partition_processed_to) =
                    shard_processed_to_by_partitions.get(partition)
                {
                    if let Some(to_key) = partition_processed_to.get(src_shard) {
                        for (diff_max_message, diff_stats) in diffs.iter() {
                            if diff_max_message > to_key {
                                break;
                            }

                            let combo = (*src_shard, *partition, *diff_max_message, dst_shard);
                            if self.processed_diff_shards.contains(&combo) {
                                continue;
                            }

                            let cumulative_stats = self.result.get_mut(partition).unwrap();
                            for (dst_acc, count) in diff_stats.iter() {
                                if dst_shard.contains_address(dst_acc) {
                                    cumulative_stats
                                        .initial_stats
                                        .decrement_for_account(dst_acc.clone(), *count);
                                    if self.for_shard != dst_shard {
                                        cumulative_stats
                                            .remaning_stats
                                            .decrement_for_account(dst_acc.clone(), *count);
                                    }
                                }
                            }

                            self.processed_diff_shards.insert(combo);
                        }
                    }
                }
            }
        }

        self.all_shards_processed_to_by_partitions
            .insert(dst_shard, (true, shard_processed_to_by_partitions));
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

    // Transactions

    pub fn begin_transaction(&mut self) {
        self.processed_to_backup = Some(self.all_shards_processed_to_by_partitions.clone());
        self.staged_additions.clear();
        self.processed_diff_shards.clear();
        self.new_partitions.clear();

        for stats in self.result.values() {
            stats.remaning_stats.begin();
        }
    }

    pub fn commit(&mut self, affected: impl Iterator<Item = IntAddr>) {
        let affected: Vec<_> = affected.into_iter().collect();

        for stats in self.result.values_mut() {
            stats.initial_stats.commit(affected.iter().cloned());
            stats.remaning_stats.commit(affected.iter().cloned());
        }

        for (partition, diff_shard, diff_max_message, diff_partition_stats) in
            self.staged_additions.drain(..)
        {
            self.shards_stats_by_partitions
                .entry(diff_shard)
                .or_default()
                .entry(partition)
                .or_default()
                .insert(diff_max_message, diff_partition_stats);
        }

        for (src_shard, partition, diff_max_message, dst_shard) in
            self.processed_diff_shards.drain()
        {
            if let Some(shard_stats) = self.shards_stats_by_partitions.get_mut(&src_shard) {
                if let Some(partition_stats) = shard_stats.get_mut(&partition) {
                    if let Some(diff_stats) = partition_stats.get_mut(&diff_max_message) {
                        diff_stats.retain(|acc, _| !dst_shard.contains_address(acc));
                        if diff_stats.is_empty() {
                            partition_stats.remove(&diff_max_message);
                        }
                    }
                }
            }
        }

        self.shards_stats_by_partitions.retain(|_, by_partitions| {
            by_partitions.retain(|_, diffs| !diffs.is_empty());
            !by_partitions.is_empty()
        });

        self.processed_to_backup = None;
        self.new_partitions.clear();
    }

    pub fn rollback(&mut self) {
        for stats in self.result.values_mut() {
            // TODO rollback should use olny affected accounts
            stats.initial_stats.rollback_all();
            stats.remaning_stats.rollback_all();
        }

        self.staged_additions.clear();

        self.processed_diff_shards.clear();

        for partition in self.new_partitions.drain() {
            self.result.remove(&partition);
        }

        if let Some(backup) = self.processed_to_backup.take() {
            self.all_shards_processed_to_by_partitions = backup;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ProcessedTo;

    fn addr(workchain: i8, id: u8) -> IntAddr {
        IntAddr::Std(tycho_types::models::StdAddr::new(workchain, [id; 32].into()))
    }

    fn diff_statistics(
        shard: ShardIdent,
        min_lt: u64,
        max_lt: u64,
        parts: Vec<(QueuePartitionIdx, Vec<(IntAddr, u64)>)>,
    ) -> DiffStatistics {
        let mut stats: FastHashMap<QueuePartitionIdx, AccountStatistics> = Default::default();
        for (partition, entries) in parts {
            let mut partition_stats = AccountStatistics::default();
            for (address, count) in entries {
                partition_stats.insert(address, count);
            }
            stats.insert(partition, partition_stats);
        }

        DiffStatistics::new(
            shard,
            QueueKey::min_for_lt(min_lt),
            QueueKey::min_for_lt(max_lt),
            stats,
        )
    }

    fn processed_to_by_partitions(
        parts: Vec<(QueuePartitionIdx, Vec<(ShardIdent, QueueKey)>)>,
    ) -> ProcessedToByPartitions {
        let mut res = ProcessedToByPartitions::default();
        for (partition, entries) in parts {
            let mut processed_to = ProcessedTo::default();
            for (shard, key) in entries {
                processed_to.insert(shard, key);
            }
            res.insert(partition, processed_to);
        }
        res
    }

    #[test]
    fn test_compute_cumulative_stats_ranges_uses_min_processed_to_and_lts() {
        let current_shard = ShardIdent::new_full(0);
        let other_shard = ShardIdent::new_full(1);
        let partition0 = QueuePartitionIdx(0);
        let partition1 = QueuePartitionIdx(1);

        let by_partitions = processed_to_by_partitions(vec![
            (
                partition0,
                vec![
                    (ShardIdent::MASTERCHAIN, QueueKey::min_for_lt(5)),
                    (current_shard, QueueKey::min_for_lt(11)),
                    (other_shard, QueueKey::min_for_lt(21)),
                ],
            ),
            (
                partition1,
                vec![
                    (ShardIdent::MASTERCHAIN, QueueKey::min_for_lt(7)),
                    (current_shard, QueueKey::min_for_lt(9)),
                    (other_shard, QueueKey::min_for_lt(25)),
                ],
            ),
        ]);

        let mut all_shards = FastHashMap::default();
        all_shards.insert(current_shard, (true, by_partitions.clone()));
        all_shards.insert(ShardIdent::MASTERCHAIN, (true, by_partitions));

        let mut mc_top_shards_end_lts = FastHashMap::default();
        mc_top_shards_end_lts.insert(other_shard, 150);

        let ranges = CumulativeStatistics::compute_cumulative_stats_ranges(
            &current_shard,
            &all_shards,
            100,
            200,
            &mc_top_shards_end_lts,
        );

        assert_eq!(ranges.len(), 3);

        let mc_range = ranges
            .iter()
            .find(|range| range.shard_ident == ShardIdent::MASTERCHAIN)
            .unwrap();
        assert_eq!(mc_range.from, Bound::Excluded(QueueKey::min_for_lt(5)));
        assert_eq!(mc_range.to, Bound::Included(QueueKey::max_for_lt(200)));

        let current_range = ranges
            .iter()
            .find(|range| range.shard_ident == current_shard)
            .unwrap();
        assert_eq!(current_range.from, Bound::Excluded(QueueKey::min_for_lt(9)));
        assert_eq!(current_range.to, Bound::Included(QueueKey::max_for_lt(100)));

        let other_range = ranges
            .iter()
            .find(|range| range.shard_ident == other_shard)
            .unwrap();
        assert_eq!(other_range.from, Bound::Excluded(QueueKey::min_for_lt(21)));
        assert_eq!(other_range.to, Bound::Included(QueueKey::max_for_lt(150)));
    }

    #[test]
    fn test_begin_transaction_add_diff_stats_and_rollback() {
        let for_shard = ShardIdent::new_full(0);
        let diff_shard = ShardIdent::new_full(1);
        let partition_a = QueuePartitionIdx(1);
        let partition_b = QueuePartitionIdx(2);
        let addr_a = addr(0, 1);
        let addr_b = addr(1, 2);
        let addr_c = addr(0, 3);

        let diff_stats = diff_statistics(
            diff_shard,
            1,
            2,
            vec![
                (
                    partition_a,
                    vec![(addr_a.clone(), 2), (addr_b.clone(), 3)],
                ),
                (partition_b, vec![(addr_c.clone(), 4)]),
            ],
        );

        let mut stats = CumulativeStatistics::new(for_shard, FastHashMap::default());
        stats.begin_transaction();
        stats.add_diff_stats(diff_shard, QueueKey::min_for_lt(2), diff_stats);

        let aggregated = stats.get_aggregated_result();
        assert_eq!(aggregated.statistics().get(&addr_a), Some(2));
        assert_eq!(aggregated.statistics().get(&addr_b), Some(3));
        assert_eq!(aggregated.statistics().get(&addr_c), Some(4));
        assert_eq!(stats.remaining_total_for_own_shard(), 6);
        assert_eq!(stats.result().len(), 2);

        stats.rollback();

        assert!(stats.result().is_empty());
        assert_eq!(stats.remaining_total_for_own_shard(), 0);
        assert!(stats.get_aggregated_result().statistics().is_empty());
    }

    #[test]
    fn test_handle_processed_to_update_decrements_and_cleans_stats_on_commit() {
        let for_shard = ShardIdent::new_full(1);
        let dst_shard = ShardIdent::new_full(0);
        let diff_shard = ShardIdent::new_full(2);
        let partition = QueuePartitionIdx(0);
        let diff_max_message = QueueKey::min_for_lt(10);
        let addr_dst = addr(0, 1);
        let addr_keep = addr(1, 2);

        let diff_stats = diff_statistics(
            diff_shard,
            9,
            10,
            vec![(
                partition,
                vec![(addr_dst.clone(), 5), (addr_keep.clone(), 7)],
            )],
        );

        let mut stats = CumulativeStatistics::new(for_shard, FastHashMap::default());
        stats.begin_transaction();
        stats.add_diff_stats(diff_shard, diff_max_message, diff_stats);
        stats.commit(vec![addr_dst.clone(), addr_keep.clone()].into_iter());

        stats.begin_transaction();
        let mut processed_to = ProcessedToByPartitions::default();
        let mut per_partition = ProcessedTo::default();
        per_partition.insert(diff_shard, diff_max_message);
        processed_to.insert(partition, per_partition);

        stats.handle_processed_to_update(dst_shard, processed_to.clone());
        stats.handle_processed_to_update(dst_shard, processed_to);

        let partition_stats = stats.result().get(&partition).unwrap();
        assert_eq!(partition_stats.initial_stats.statistics().get(&addr_dst), None);
        assert_eq!(partition_stats.initial_stats.statistics().get(&addr_keep), Some(7));
        assert_eq!(partition_stats.remaning_stats.get(&addr_dst), None);
        assert_eq!(partition_stats.remaning_stats.get(&addr_keep), Some(7));

        stats.commit(vec![addr_dst.clone(), addr_keep.clone()].into_iter());

        let partition_stats = stats.result().get(&partition).unwrap();
        assert_eq!(partition_stats.initial_stats.statistics().get(&addr_dst), None);
        assert_eq!(partition_stats.initial_stats.statistics().get(&addr_keep), Some(7));
        assert_eq!(partition_stats.remaning_stats.get(&addr_dst), None);
        assert_eq!(partition_stats.remaning_stats.get(&addr_keep), Some(7));

        let shard_stats = stats.shards_stats_by_partitions.get(&diff_shard).unwrap();
        let partition_stats = shard_stats.get(&partition).unwrap();
        let diff_stats = partition_stats.get(&diff_max_message).unwrap();
        assert_eq!(diff_stats.get(&addr_dst), None);
        assert_eq!(diff_stats.get(&addr_keep), Some(&7));
    }
}
