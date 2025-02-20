use std::cmp::max;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use everscale_types::cell::HashBytes;
use everscale_types::models::{BlockId, BlockIdShort, ShardIdent};
use serde::{Deserialize, Serialize};
use tycho_block_util::queue::{QueueKey, QueuePartitionIdx};
use tycho_storage::model::DiffInfo;
use tycho_util::metrics::HistogramGuard;
use tycho_util::{serde_helpers, FastHashMap, FastHashSet};

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
use crate::types::ProcessedTo;
use crate::{internal_queue, tracing_targets};

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
    ) -> Result<()>;
    /// Move messages from uncommitted state to committed state and update gc ranges
    fn commit_diff(
        &self,
        mc_top_blocks: &[(BlockId, bool)],
        partitions: &FastHashSet<QueuePartitionIdx>,
    ) -> Result<()>;
    /// remove all data in uncommitted state storage
    fn clear_uncommitted_state(&self) -> Result<()>;
    /// Returns the diffs tail len for the given shard
    fn get_diffs_tail_len(&self, shard_ident: &ShardIdent, from: &QueueKey) -> u32;
    /// Load statistics for the given range by accounts
    fn load_statistics(
        &self,
        partition: QueuePartitionIdx,
        ranges: &[QueueShardRange],
    ) -> Result<QueueStatistics>;
    /// Get diff for the given blocks from committed and uncommitted state
    fn get_diff(&self, shard_ident: &ShardIdent, seqno: u32) -> Result<Option<DiffInfo>>;
    /// Check if diff exists in the cache
    fn is_diff_exists(&self, block_id_short: &BlockIdShort) -> Result<bool>;
    /// Get last applied mc block id from committed state
    fn get_last_applied_mc_block_id(&self) -> Result<Option<BlockId>>;
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
            // uncommitted_diffs: Default::default(),
            // committed_diffs: Default::default(),
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
    // uncommitted_diffs: FastDashMap<ShardIdent, BTreeMap<u32, ShortQueueDiff>>,
    // committed_diffs: FastDashMap<ShardIdent, BTreeMap<u32, ShortQueueDiff>>,
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
    ) -> Result<()> {
        // Check for duplicate diffs based on the block_id_short.seqno and hash
        let shard_diff = internal_queue::queue::Queue::get_diff(
            self,
            &block_id_short.shard,
            block_id_short.seqno,
        )?;

        // Check if the diff is already applied
        // return if hash is the same
        if let Some(shard_diff) = shard_diff {
            // Check if the diff is already applied with different hash
            if shard_diff.hash != *hash {
                bail!(
                    "Duplicate diff with different hash: block_id={}, existing diff_hash={}, new diff_hash={}",
                    block_id_short, shard_diff.hash,  hash,
                )
            }
            return Ok(());
        }

        let last_applied_seqno_uncommitted = self
            .uncommitted_state
            .get_last_applied_block_seqno(&block_id_short.shard)?;

        let last_applied_seqno_committed = self
            .committed_state
            .get_last_applied_block_seqno(&block_id_short.shard)?;

        let last_applied_seqno = max(last_applied_seqno_uncommitted, last_applied_seqno_committed);

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
        self.uncommitted_state
            .write_diff(&block_id_short, &statistics, *hash, diff)?;

        Ok(())
    }

    fn commit_diff(
        &self,
        mc_top_blocks: &[(BlockId, bool)],
        partitions: &FastHashSet<QueuePartitionIdx>,
    ) -> Result<()> {
        let mc_block_id = mc_top_blocks
            .iter()
            .find(|(block_id, _)| block_id.is_masterchain())
            .map(|(block_id, _)| block_id)
            .ok_or_else(|| anyhow!("Masterchain block not found in commit_diff"))?;

        let mut gc_ranges = FastHashMap::default();

        let mut commit_ranges: Vec<QueueShardRange> = vec![];

        for (block_id, top_shard_block_changed) in mc_top_blocks {
            let diff = self
                .uncommitted_state
                .get_diff_info(&block_id.shard, block_id.seqno)?;

            let diff = match diff {
                None => {
                    if *top_shard_block_changed && mc_block_id.seqno != 0 {
                        bail!("Uncommitted diff not found for block_id: {}", block_id);
                    } else {
                        continue;
                    }
                }
                Some(diff) => diff,
            };

            commit_ranges.push(QueueShardRange {
                shard_ident: block_id.shard,
                from: QueueKey::default(),
                to: diff.max_message,
            });

            if *top_shard_block_changed {
                for (shard_ident, processed_to_key) in diff.processed_to.iter() {
                    let last_key = gc_ranges
                        .entry(*shard_ident)
                        .or_insert_with(|| *processed_to_key);

                    if processed_to_key < last_key {
                        *last_key = *processed_to_key;
                    }
                }
            }
        }

        // move all uncommitted diffs messages to committed state
        self.uncommitted_state
            .commit(partitions.clone(), &commit_ranges, mc_block_id)?;

        // run GC for each found partition in routers
        for partition in partitions {
            for (shard, end_key) in &gc_ranges {
                self.gc.update_delete_until(*partition, *shard, *end_key);
            }
        }

        Ok(())
    }

    fn clear_uncommitted_state(&self) -> Result<()> {
        self.uncommitted_state.truncate()?;
        tracing::info!(
            target: tracing_targets::MQ,
             "Cleared uncommitted diffs.",
        );
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

    fn get_diff(&self, shard_ident: &ShardIdent, seqno: u32) -> Result<Option<DiffInfo>> {
        if let Some(diff) = self.uncommitted_state.get_diff_info(shard_ident, seqno)? {
            return Ok(Some(diff));
        }

        if let Some(diff) = self.committed_state.get_diff_info(shard_ident, seqno)? {
            return Ok(Some(diff));
        }

        Ok(None)
    }

    fn get_diffs_tail_len(&self, shard_ident: &ShardIdent, max_message_from: &QueueKey) -> u32 {
        let uncommitted_tail_len = self
            .uncommitted_state
            .get_diffs_tail_len(shard_ident, max_message_from);

        let committed_tail_len = self
            .committed_state
            .get_diffs_tail_len(shard_ident, max_message_from);

        tracing::info!(
            target: tracing_targets::MQ,
            shard_ident = ?shard_ident,
            uncommitted_tail_len,
            committed_tail_len,
            "Get diffs tail len",
        );

        uncommitted_tail_len + committed_tail_len
    }

    fn is_diff_exists(&self, block_id_short: &BlockIdShort) -> Result<bool> {
        Ok(internal_queue::queue::Queue::get_diff(
            self,
            &block_id_short.shard,
            block_id_short.seqno,
        )?
        .is_some())
    }

    fn get_last_applied_mc_block_id(&self) -> Result<Option<BlockId>> {
        self.committed_state.get_last_applied_mc_block_id()
    }
}
