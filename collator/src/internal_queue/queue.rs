use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Result};
use everscale_types::cell::HashBytes;
use everscale_types::models::{BlockIdShort, ShardIdent};
use serde::{Deserialize, Serialize};
use tycho_block_util::queue::{QueueKey, QueuePartition};
use tycho_util::{serde_helpers, FastDashMap, FastHashMap};

use crate::internal_queue::gc::GcManager;
use crate::internal_queue::state::commited_state::{
    CommittedState, CommittedStateFactory, CommittedStateImplFactory, CommittedStateStdImpl,
};
use crate::internal_queue::state::state_iterator::StateIterator;
use crate::internal_queue::state::uncommitted_state::{
    UncommittedState, UncommittedStateFactory, UncommittedStateImplFactory, UncommittedStateStdImpl,
};
use crate::internal_queue::types::{
    DiffStatistics, InternalMessageValue, QueueDiffWithMessages, QueueShardRange, QueueStatistics,
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
        partition: QueuePartition,
        ranges: Vec<QueueShardRange>,
        for_shard_id: ShardIdent,
    ) -> Result<Vec<Box<dyn StateIterator<V>>>>;
    /// Add messages to uncommitted state from `diff.messages` and add diff to the cache
    fn apply_diff(
        &self,
        diff: QueueDiffWithMessages<V>,
        block_id_short: BlockIdShort,
        hash: &HashBytes,
        statistics: DiffStatistics,
    ) -> Result<()>;
    /// Move messages from uncommitted state to committed state and update gc ranges
    fn commit_diff(&self, mc_top_blocks: &[(BlockIdShort, bool)]) -> Result<()>;
    /// remove all data in uncommitted state storage
    fn clear_uncommitted_state(&self) -> Result<()>;
    /// returns the number of diffs in cache for the given shard
    fn get_diffs_count_by_shard(&self, shard_ident: &ShardIdent) -> usize;
    /// removes all diffs from the cache that are less than `inclusive_until` which source shard is `source_shard`
    fn trim_diffs(&self, source_shard: &ShardIdent, inclusive_until: &QueueKey) -> Result<()>;
    /// load statistics for the given range by accounts
    fn load_statistics(
        &self,
        partition: QueuePartition,
        ranges: Vec<QueueShardRange>,
    ) -> Result<QueueStatistics>;
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

struct ShortQueueDiff {
    pub processed_to: ProcessedTo,
    pub end_key: QueueKey,
    pub hash: HashBytes,
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
        partition: QueuePartition,
        ranges: Vec<QueueShardRange>,
        for_shard_id: ShardIdent,
    ) -> Result<Vec<Box<dyn StateIterator<V>>>> {
        let snapshot = self.committed_state.snapshot();

        let committed_state_iterator =
            self.committed_state
                .iterator(&snapshot, for_shard_id, partition, ranges.clone())?;

        let uncommitted_state_iterator =
            self.uncommitted_state
                .iterator(&snapshot, for_shard_id, partition, ranges)?;

        Ok(vec![committed_state_iterator, uncommitted_state_iterator])
    }

    fn apply_diff(
        &self,
        diff: QueueDiffWithMessages<V>,
        block_id_short: BlockIdShort,
        hash: &HashBytes,
        statistics: DiffStatistics,
    ) -> Result<()> {
        // Get or insert the shard diffs for the given block_id_short.shard
        let mut shard_diffs = self
            .uncommitted_diffs
            .entry(block_id_short.shard)
            .or_default();

        // Check for duplicate diffs based on the block_id_short.seqno and hash
        let shard_diff = shard_diffs.get(&block_id_short.seqno);
        if let Some(shard_diff) = shard_diff {
            if &shard_diff.hash != hash {
                bail!(
                    "Duplicate diff with different hash: block_id={}, existing diff_hash={}, new diff_hash={}",
                    block_id_short, shard_diff.hash,  hash,
                )
            }
            return Ok(());
        }

        let last_applied_seqno = shard_diffs.keys().next_back().cloned();

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

        let max_message = diff
            .messages
            .keys()
            .next_back()
            .cloned()
            .unwrap_or_default();

        // Add messages to uncommitted_state if there are any
        if !diff.messages.is_empty() {
            self.uncommitted_state.add_messages_with_statistics(
                block_id_short.shard,
                &diff.partition_router,
                &diff.messages,
                statistics,
            )?;
        }

        let short_diff = ShortQueueDiff {
            processed_to: diff.processed_to,
            end_key: max_message,
            hash: *hash,
        };

        // Insert the diff into the shard diffs
        shard_diffs.insert(block_id_short.seqno, short_diff);

        Ok(())
    }

    fn commit_diff(&self, mc_top_blocks: &[(BlockIdShort, bool)]) -> Result<()> {
        let mut shards_to_commit = FastHashMap::default();
        let mut gc_ranges = FastHashMap::default();

        for (block_id_short, top_shard_block_changed) in mc_top_blocks {
            let mut diffs_to_commit = vec![];

            let prev_shard_uncommitted_diffs =
                self.uncommitted_diffs.get_mut(&block_id_short.shard);
            if let Some(mut shard_uncommitted_diffs) = prev_shard_uncommitted_diffs {
                shard_uncommitted_diffs
                    .range(..=block_id_short.seqno)
                    .for_each(|(block_seqno, shard_diff)| {
                        diffs_to_commit.push(*block_seqno);

                        let current_last_key = shards_to_commit
                            .entry(block_id_short.shard)
                            .or_insert_with(|| shard_diff.end_key);

                        if shard_diff.end_key > *current_last_key {
                            *current_last_key = shard_diff.end_key;
                        }

                        // find min processed_to for each shard for GC
                        if *block_seqno == block_id_short.seqno && *top_shard_block_changed {
                            for (shard_ident, processed_to_key) in shard_diff.processed_to.iter() {
                                let last_key = gc_ranges
                                    .entry(*shard_ident)
                                    .or_insert_with(|| *processed_to_key);

                                if processed_to_key < last_key {
                                    *last_key = *processed_to_key;
                                }
                            }
                        }
                    });

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

        self.uncommitted_state
            .commit(QueuePartition::all().as_slice(), commit_ranges.as_slice())?;

        let uncommitted_diffs_count: usize =
            self.uncommitted_diffs.iter().map(|r| r.value().len()).sum();

        metrics::counter!("tycho_internal_queue_uncommitted_diffs_count")
            .increment(uncommitted_diffs_count as u64);

        for (shard, end_key) in gc_ranges {
            self.gc.update_delete_until(shard, end_key);
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
                .retain(|_, diff| &diff.end_key > inclusive_until);
        }
        if let Some(mut shard_diffs) = self.committed_diffs.get_mut(source_shard) {
            shard_diffs
                .value_mut()
                .retain(|_, diff| &diff.end_key > inclusive_until);
        }
        Ok(())
    }

    fn load_statistics(
        &self,
        partition: QueuePartition,
        ranges: Vec<QueueShardRange>,
    ) -> Result<QueueStatistics> {
        let snapshot = self.committed_state.snapshot();
        let mut statistics = FastHashMap::default();

        self.committed_state
            .load_statistics(&mut statistics, &snapshot, partition, &ranges)?;
        self.uncommitted_state
            .load_statistics(&mut statistics, &snapshot, partition, &ranges)?;

        let statistics = QueueStatistics::with_statistics(statistics);

        Ok(statistics)
    }
}
