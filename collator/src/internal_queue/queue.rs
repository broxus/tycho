use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Result};
use everscale_types::cell::HashBytes;
use everscale_types::models::{BlockIdShort, ShardIdent};
use serde::{Deserialize, Serialize};
use tycho_block_util::queue::{QueueKey, QueuePartitionIdx};
use tycho_util::metrics::HistogramGuard;
use tycho_util::{serde_helpers, FastDashMap, FastHashMap, FastHashSet};

use crate::internal_queue::gc::GcManager;
use crate::internal_queue::state::commited_state::{
    CommittedState, CommittedStateFactory, CommittedStateImplFactory, CommittedStateStdImpl,
};
use crate::internal_queue::state::state_iterator::StateIterator;
use crate::internal_queue::state::uncommitted_state::{
    UncommittedState, UncommittedStateFactory, UncommittedStateImplFactory, UncommittedStateStdImpl,
};
use crate::internal_queue::types::{
    DiffStatistics, InternalMessageValue, PartitionRouter, QueueDiffWithMessages, QueueShardRange,
    QueueStatistics,
};
use crate::tracing_targets;
use crate::types::ProcessedTo;

// FACTORY

#[derive(Debug, Serialize, Deserialize)]
pub struct QueueConfig {
    /// Default: 5 seconds.
    #[serde(with = "serde_helpers::humantime")]
    pub gc_interval: Duration,
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            gc_interval: Duration::from_secs(5),
        }
    }
}

pub trait QueueFactory<V: InternalMessageValue> {
    type Queue: Queue<V>;

    fn create(&self) -> Self::Queue;
}

impl<F, R, V: InternalMessageValue> QueueFactory<V> for F
where
    F: Fn() -> R,
    R: Queue<V>,
{
    type Queue = R;

    fn create(&self) -> Self::Queue {
        self()
    }
}

pub struct QueueFactoryStdImpl {
    pub uncommitted_state_factory: UncommittedStateImplFactory,
    pub committed_state_factory: CommittedStateImplFactory,
    pub config: QueueConfig,
}

// TRAIT

#[trait_variant::make(Queue: Send)]
pub trait LocalQueue<V>
where
    V: InternalMessageValue + Send + Sync,
{
    /// Create iterator for specified shard and return it
    fn iterator(
        &self,
        partition: QueuePartitionIdx,
        ranges: &[QueueShardRange],
        for_shard_id: ShardIdent,
    ) -> Result<Vec<Box<dyn StateIterator<V>>>>;
    /// Add messages to uncommitted state from `diff.messages` and add diff to the cache
    fn apply_diff(
        &self,
        diff: QueueDiffWithMessages<V>,
        block_id_short: BlockIdShort,
        hash: &HashBytes,
        statistics: DiffStatistics,
        max_message: QueueKey,
    ) -> Result<()>;
    /// Move messages from uncommitted state to committed state and update gc ranges
    fn commit_diff(&self, mc_top_blocks: &[(BlockIdShort, bool)]) -> Result<()>;
    /// remove all data in uncommitted state storage
    fn clear_uncommitted_state(&self) -> Result<()>;
    /// Returns the number of diffs in cache for the given shard
    fn get_diffs_count_by_shard(&self, shard_ident: &ShardIdent) -> usize;
    /// Removes all diffs from the cache that are less than `inclusive_until` which source shard is `source_shard`
    fn trim_diffs(&self, source_shard: &ShardIdent, inclusive_until: &QueueKey) -> Result<()>;
    /// Load statistics for the given range by accounts
    fn load_statistics(
        &self,
        partition: QueuePartitionIdx,
        ranges: &[QueueShardRange],
    ) -> Result<QueueStatistics>;
    /// Get diffs for the given blocks from committed and uncommitted state
    fn get_diffs(&self, blocks: FastHashMap<ShardIdent, u32>) -> Vec<(ShardIdent, ShortQueueDiff)>;
    /// Get diff for the given blocks from committed and uncommitted state
    fn get_diff(&self, shard_ident: ShardIdent, seqno: u32) -> Option<ShortQueueDiff>;
    /// Check if diff exists in the cache
    fn is_diff_exists(&self, block_id_short: &BlockIdShort) -> bool;
}

// IMPLEMENTATION

impl<V: InternalMessageValue> QueueFactory<V> for QueueFactoryStdImpl {
    type Queue = QueueImpl<UncommittedStateStdImpl, CommittedStateStdImpl, V>;

    fn create(&self) -> Self::Queue {
        let uncommitted_state = <UncommittedStateImplFactory as UncommittedStateFactory<V>>::create(
            &self.uncommitted_state_factory,
        );
        let committed_state = <CommittedStateImplFactory as CommittedStateFactory<V>>::create(
            &self.committed_state_factory,
        );
        let committed_state = Arc::new(committed_state);
        let gc = GcManager::start::<V>(committed_state.clone(), self.config.gc_interval);
        QueueImpl {
            uncommitted_state: Arc::new(uncommitted_state),
            committed_state,
            uncommitted_diffs: Default::default(),
            committed_diffs: Default::default(),
            gc,
            _phantom_data: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ShortQueueDiff {
    inner: Arc<ShortQueueDiffInner>,
}

impl ShortQueueDiff {
    pub fn new(
        processed_to: ProcessedTo,
        max_message: QueueKey,
        router: PartitionRouter,
        hash: HashBytes,
        statistics: DiffStatistics,
    ) -> Self {
        Self {
            inner: Arc::new(ShortQueueDiffInner {
                processed_to,
                max_message,
                router,
                hash,
                statistics,
            }),
        }
    }

    pub fn processed_to(&self) -> &ProcessedTo {
        &self.inner.processed_to
    }

    pub fn max_message(&self) -> &QueueKey {
        &self.inner.max_message
    }

    pub fn router(&self) -> &PartitionRouter {
        &self.inner.router
    }

    pub fn hash(&self) -> &HashBytes {
        &self.inner.hash
    }

    pub fn statistics(&self) -> &DiffStatistics {
        &self.inner.statistics
    }
}

#[derive(Debug)]
pub struct ShortQueueDiffInner {
    pub processed_to: ProcessedTo,
    pub max_message: QueueKey,
    pub router: PartitionRouter,
    pub hash: HashBytes,
    pub statistics: DiffStatistics,
}

pub struct QueueImpl<S, P, V>
where
    S: UncommittedState<V>,
    P: CommittedState<V>,
    V: InternalMessageValue,
{
    uncommitted_state: Arc<S>,
    committed_state: Arc<P>,
    uncommitted_diffs: FastDashMap<ShardIdent, BTreeMap<u32, ShortQueueDiff>>,
    committed_diffs: FastDashMap<ShardIdent, BTreeMap<u32, ShortQueueDiff>>,
    gc: GcManager,
    _phantom_data: PhantomData<V>,
}

impl<S, P, V> Queue<V> for QueueImpl<S, P, V>
where
    S: UncommittedState<V> + Send + Sync,
    P: CommittedState<V> + Send + Sync + 'static,
    V: InternalMessageValue + Send + Sync,
{
    fn iterator(
        &self,
        partition: QueuePartitionIdx,
        ranges: &[QueueShardRange],
        for_shard_id: ShardIdent,
    ) -> Result<Vec<Box<dyn StateIterator<V>>>> {
        let snapshot = self.committed_state.snapshot();

        let committed_state_iterator = {
            let _histogram =
                HistogramGuard::begin("tycho_internal_queue_commited_state_iterator_create_time");
            self.committed_state
                .iterator(&snapshot, for_shard_id, partition, ranges)?
        };

        let uncommitted_state_iterator = {
            let _histogram =
                HistogramGuard::begin("tycho_internal_queue_uncommited_state_iterator_create_time");

            self.uncommitted_state
                .iterator(&snapshot, for_shard_id, partition, ranges)?
        };

        Ok(vec![committed_state_iterator, uncommitted_state_iterator])
    }

    fn apply_diff(
        &self,
        diff: QueueDiffWithMessages<V>,
        block_id_short: BlockIdShort,
        hash: &HashBytes,
        statistics: DiffStatistics,
        max_message: QueueKey,
    ) -> Result<()> {
        // Get or insert the shard diffs for the given block_id_short.shard
        let mut shard_diffs = self
            .uncommitted_diffs
            .entry(block_id_short.shard)
            .or_default();

        // Check for duplicate diffs based on the block_id_short.seqno and hash
        let shard_diff = shard_diffs.get(&block_id_short.seqno);

        // Check if the diff is already applied
        // return if hash is the same
        if let Some(shard_diff) = shard_diff {
            // Check if the diff is already applied with different hash
            if shard_diff.hash() != hash {
                bail!(
                    "Duplicate diff with different hash: block_id={}, existing diff_hash={}, new diff_hash={}",
                    block_id_short, shard_diff.hash(),  hash,
                )
            }
            return Ok(());
        }

        let last_applied_seqno = shard_diffs.keys().last().cloned();

        if let Some(last_applied_seqno) = last_applied_seqno {
            // Check if the diff is already applied
            if block_id_short.seqno <= last_applied_seqno {
                return Ok(());
            }

            // Check if the diff is sequential
            if block_id_short.seqno != last_applied_seqno + 1 {
                bail!(
                    "Diff seqno is not sequential new seqno {}. last_applied_seqno {}",
                    block_id_short.seqno,
                    last_applied_seqno
                );
            }
        }

        // Add messages to uncommitted_state if there are any
        if !diff.messages.is_empty() {
            self.uncommitted_state.add_messages_with_statistics(
                block_id_short.shard,
                &diff.partition_router,
                &diff.messages,
                &statistics,
            )?;
        }

        let short_diff = ShortQueueDiff::new(
            diff.processed_to,
            max_message,
            diff.partition_router,
            *hash,
            statistics,
        );

        // Insert the diff into the shard diffs
        shard_diffs.insert(block_id_short.seqno, short_diff);

        Ok(())
    }

    fn commit_diff(&self, mc_top_blocks: &[(BlockIdShort, bool)]) -> Result<()> {
        let mut partitions = FastHashSet::default();
        // insert default partition  because we doesn't store it in router
        partitions.insert(QueuePartitionIdx::default());
        let mut shards_to_commit = FastHashMap::default();
        let mut gc_ranges = FastHashMap::default();

        for (block_id_short, top_shard_block_changed) in mc_top_blocks {
            let mut diffs_to_commit = vec![];

            // find all uncommited diffs for the given shard top block
            let prev_shard_uncommitted_diffs =
                self.uncommitted_diffs.get_mut(&block_id_short.shard);

            if let Some(mut shard_uncommitted_diffs) = prev_shard_uncommitted_diffs {
                // iterate over all uncommitted diffs for the given shard until the top block seqno
                shard_uncommitted_diffs
                    .range(..=block_id_short.seqno)
                    .for_each(|(block_seqno, shard_diff)| {
                        diffs_to_commit.push(*block_seqno);

                        let current_last_key = shards_to_commit
                            .entry(block_id_short.shard)
                            .or_insert_with(|| *shard_diff.max_message());

                        // Add all partitions from the router to the partitions set
                        // use it in commit and gc
                        for partition in shard_diff.router().partitions_stats().keys() {
                            partitions.insert(*partition);
                        }

                        // find max message for each shard for commit
                        if shard_diff.max_message() > current_last_key {
                            *current_last_key = *shard_diff.max_message();
                        }

                        // find min processed_to for each shard for GC
                        if *block_seqno == block_id_short.seqno && *top_shard_block_changed {
                            for (shard_ident, processed_to_key) in shard_diff.processed_to().iter()
                            {
                                let last_key = gc_ranges
                                    .entry(*shard_ident)
                                    .or_insert_with(|| *processed_to_key);

                                if processed_to_key < last_key {
                                    *last_key = *processed_to_key;
                                }
                            }
                        }
                    });

                // remove all diffs from uncommitted state that are going to be committed
                for seqno in diffs_to_commit {
                    if let Some(diff) = shard_uncommitted_diffs.remove(&seqno) {
                        // Move the diff to committed_diffs
                        let mut shard_committed_diffs = self
                            .committed_diffs
                            .entry(block_id_short.shard)
                            .or_default();
                        shard_committed_diffs.insert(seqno, diff);
                    }
                }
            }
        }

        let commit_ranges: Vec<QueueShardRange> = shards_to_commit
            .into_iter()
            .map(|(shard_ident, end_key)| QueueShardRange {
                shard_ident,
                from: QueueKey::default(),
                to: end_key,
            })
            .collect();

        // move all uncommitted diffs messages to committed state
        self.uncommitted_state
            .commit(partitions.clone(), &commit_ranges)?;

        let uncommitted_diffs_count: usize =
            self.uncommitted_diffs.iter().map(|r| r.value().len()).sum();

        metrics::counter!("tycho_internal_queue_uncommitted_diffs_count")
            .increment(uncommitted_diffs_count as u64);

        // run GC for each found partition in routers
        for partition in partitions {
            for (shard, end_key) in &gc_ranges {
                self.gc.update_delete_until(partition, *shard, *end_key);
            }
        }

        Ok(())
    }

    fn clear_uncommitted_state(&self) -> Result<()> {
        self.uncommitted_state.truncate()?;
        let diffs_before_clear: usize =
            self.uncommitted_diffs.iter().map(|r| r.value().len()).sum();
        self.uncommitted_diffs.clear();
        let diffs_after_clear: usize = self.uncommitted_diffs.iter().map(|r| r.value().len()).sum();
        tracing::info!(
            target: tracing_targets::MQ,
            diffs_before_clear,
            diffs_after_clear,
             "Cleared uncommitted diffs.",
        );
        Ok(())
    }

    fn get_diffs_count_by_shard(&self, shard_ident: &ShardIdent) -> usize {
        let uncommitted_count = self
            .uncommitted_diffs
            .get(shard_ident)
            .map_or(0, |diffs| diffs.len());
        let committed_count = self
            .committed_diffs
            .get(shard_ident)
            .map_or(0, |diffs| diffs.len());

        uncommitted_count + committed_count
    }

    fn trim_diffs(&self, source_shard: &ShardIdent, inclusive_until: &QueueKey) -> Result<()> {
        if let Some(mut shard_diffs) = self.uncommitted_diffs.get_mut(source_shard) {
            shard_diffs
                .value_mut()
                .retain(|_, diff| diff.max_message() > inclusive_until);
        }
        if let Some(mut shard_diffs) = self.committed_diffs.get_mut(source_shard) {
            shard_diffs
                .value_mut()
                .retain(|_, diff| diff.max_message() > inclusive_until);
        }
        Ok(())
    }

    fn load_statistics(
        &self,
        partition: QueuePartitionIdx,
        ranges: &[QueueShardRange],
    ) -> Result<QueueStatistics> {
        let snapshot = self.committed_state.snapshot();
        let mut statistics = FastHashMap::default();

        // load from committed state
        self.committed_state
            .load_statistics(&mut statistics, &snapshot, partition, ranges)?;

        // load from uncommitted state and add to the statistics
        self.uncommitted_state
            .load_statistics(&mut statistics, &snapshot, partition, ranges)?;

        let statistics = QueueStatistics::with_statistics(statistics);

        Ok(statistics)
    }

    fn get_diffs(&self, blocks: FastHashMap<ShardIdent, u32>) -> Vec<(ShardIdent, ShortQueueDiff)> {
        let mut result = Vec::new();

        for (shard_ident, seqno) in blocks {
            if let Some(shard_diffs) = self.uncommitted_diffs.get(&shard_ident) {
                for (block_seqno, diff) in shard_diffs.value() {
                    if *block_seqno <= seqno {
                        result.push((shard_ident, diff.clone()));
                    }
                }
            }

            if let Some(shard_diffs) = self.committed_diffs.get(&shard_ident) {
                for (block_seqno, diff) in shard_diffs.value() {
                    if *block_seqno <= seqno {
                        result.push((shard_ident, diff.clone()));
                    }
                }
            }
        }

        result
    }

    fn get_diff(&self, shard_ident: ShardIdent, seqno: u32) -> Option<ShortQueueDiff> {
        if let Some(shard_diffs) = self.uncommitted_diffs.get(&shard_ident) {
            if let Some(diff) = shard_diffs.get(&seqno) {
                return Some(diff.clone());
            }
        }

        if let Some(shard_diffs) = self.committed_diffs.get(&shard_ident) {
            if let Some(diff) = shard_diffs.get(&seqno) {
                return Some(diff.clone());
            }
        }

        None
    }

    fn is_diff_exists(&self, block_id_short: &BlockIdShort) -> bool {
        self.uncommitted_diffs
            .get(&block_id_short.shard)
            .is_some_and(|diffs| diffs.contains_key(&block_id_short.seqno))
            || self
                .committed_diffs
                .get(&block_id_short.shard)
                .is_some_and(|diffs| diffs.contains_key(&block_id_short.seqno))
    }
}
