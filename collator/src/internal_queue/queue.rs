use std::marker::PhantomData;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use anyhow::{Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use tycho_block_util::queue::{QueueKey, QueuePartitionIdx};
use tycho_types::cell::HashBytes;
use tycho_types::models::{BlockId, BlockIdShort, ShardIdent};
use tycho_util::metrics::HistogramGuard;
use tycho_util::{FastDashMap, FastHashMap, FastHashSet, serde_helpers};

use super::gc::GcEndKey;
use super::types::SeparatedStatisticsByPartitions;
use crate::internal_queue::gc::GcManager;
use crate::internal_queue::state::state_iterator::StateIterator;
use crate::internal_queue::state::storage::{
    QueueState, QueueStateFactory, QueueStateImplFactory, QueueStateStdImpl,
};
use crate::internal_queue::types::{
    AccountStatistics, DiffStatistics, DiffZone, InternalMessageValue, QueueDiffWithMessages,
    QueueShardRange,
};
use crate::storage::models::DiffInfo;
use crate::{internal_queue, tracing_targets};

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

    fn create(&self) -> Result<Self::Queue>;
}

impl<F, R, V: InternalMessageValue> QueueFactory<V> for F
where
    F: Fn() -> Result<R>,
    R: Queue<V>,
{
    type Queue = R;

    fn create(&self) -> Result<Self::Queue> {
        self()
    }
}

pub struct QueueFactoryStdImpl {
    pub state: QueueStateImplFactory,
    pub config: QueueConfig,
}

// TRAIT

pub trait Queue<V>: Send
where
    V: InternalMessageValue + Send + Sync,
{
    /// Create iterator for specified shard and return it
    fn iterator(
        &self,
        partition: QueuePartitionIdx,
        ranges: &[QueueShardRange],
        for_shard_id: ShardIdent,
    ) -> Result<Box<dyn StateIterator<V>>>;

    /// Add messages to state from `diff.messages` and store diff info
    fn apply_diff(
        &self,
        diff: QueueDiffWithMessages<V>,
        block_id_short: BlockIdShort,
        hash: &HashBytes,
        statistics: DiffStatistics,
        check_sequence: Option<DiffZone>,
    ) -> Result<()>;

    /// Commit diffs to the state and update GC
    fn commit_diff(
        &self,
        mc_top_blocks: &[(BlockId, bool)],
        partitions: &FastHashSet<QueuePartitionIdx>,
    ) -> Result<()>;

    /// Remove all data in uncommitted zone
    fn clear_uncommitted_state(
        &self,
        partitions: &FastHashSet<QueuePartitionIdx>,
        top_shards: &[ShardIdent],
    ) -> Result<()>;

    /// Get diffs tail len
    fn get_diffs_tail_len(&self, shard_ident: &ShardIdent, from: &QueueKey) -> u32;

    /// Load statistics for the given range by accounts
    fn load_diff_statistics(
        &self,
        partition: QueuePartitionIdx,
        range: &QueueShardRange,
        result: &mut AccountStatistics,
    ) -> Result<()>;

    /// Get diff info for the given block from committed and/or uncommitted zones
    fn get_diff_info(
        &self,
        shard_ident: &ShardIdent,
        seqno: u32,
        zone: DiffZone,
    ) -> Result<Option<DiffInfo>>;

    /// Check if diff exists in state
    fn is_diff_exists(&self, block_id_short: &BlockIdShort) -> Result<bool>;

    /// Get mc block id on which the queue was committed.
    /// Returns None if queue was not committed
    fn get_last_committed_mc_block_id(&self) -> Result<Option<BlockId>>;

    /// Load separated diff statistics for the specified partitions and range
    fn load_separated_diff_statistics(
        &self,
        partitions: &FastHashSet<QueuePartitionIdx>,
        range: &QueueShardRange,
    ) -> Result<SeparatedStatisticsByPartitions>;
}

impl<V: InternalMessageValue> QueueFactory<V> for QueueFactoryStdImpl {
    type Queue = QueueImpl<QueueStateStdImpl, V>;

    fn create(&self) -> Result<Self::Queue> {
        let state = <QueueStateImplFactory as QueueStateFactory<V>>::create(&self.state)?;
        let state = Arc::new(state);
        let gc = GcManager::start::<V>(state.clone(), self.config.gc_interval);
        Ok(QueueImpl {
            state,
            gc,
            global_lock: RwLock::new(()),
            shard_locks: FastDashMap::default(),
            _phantom_data: Default::default(),
        })
    }
}

pub struct QueueImpl<P, V>
where
    P: QueueState<V>,
    V: InternalMessageValue,
{
    state: Arc<P>,
    gc: GcManager,
    global_lock: RwLock<()>,
    shard_locks: FastDashMap<ShardIdent, Arc<Mutex<()>>>,
    _phantom_data: PhantomData<V>,
}

impl<P, V> Queue<V> for QueueImpl<P, V>
where
    P: QueueState<V> + Send + Sync + 'static,
    V: InternalMessageValue + Send + Sync,
{
    fn iterator(
        &self,
        partition: QueuePartitionIdx,
        ranges: &[QueueShardRange],
        for_shard_id: ShardIdent,
    ) -> Result<Box<dyn StateIterator<V>>> {
        let snapshot = self.state.snapshot();

        let state_iterator = {
            let _histogram =
                HistogramGuard::begin("tycho_internal_queue_commited_state_iterator_create_time");
            self.state
                .iterator(&snapshot, for_shard_id, partition, ranges)?
        };

        Ok(state_iterator)
    }

    fn apply_diff(
        &self,
        diff: QueueDiffWithMessages<V>,
        block_id_short: BlockIdShort,
        hash: &HashBytes,
        statistics: DiffStatistics,
        check_sequence: Option<DiffZone>,
    ) -> Result<()> {
        // Take global lock. Lock commit and clear uncommitted state for execution
        let _global_read_guard = self.global_lock.read().unwrap_or_else(|e| e.into_inner());

        // Take specific shard lock
        let shard_lock = self.shard_locks.entry(block_id_short.shard).or_default();
        let _shard_guard = shard_lock.lock().unwrap_or_else(|e| e.into_inner());

        // Check for duplicate diffs based on the block_id_short.seqno and hash
        let shard_diff = internal_queue::queue::Queue::get_diff_info(
            self,
            &block_id_short.shard,
            block_id_short.seqno,
            DiffZone::Both,
        )?;

        // Check if the diff is already applied
        // return if hash is the same
        if let Some(shard_diff) = shard_diff {
            // Check if the diff is already applied with different hash
            if shard_diff.hash != *hash {
                bail!(
                    "Duplicate diff with different hash: block_id={}, existing diff_hash={}, new diff_hash={}",
                    block_id_short,
                    shard_diff.hash,
                    hash,
                )
            }
            return Ok(());
        }

        if let Some(zone) = check_sequence {
            let last_applied_seqno = self.state.get_last_applied_seqno(&block_id_short.shard)?;

            if let Some(last_applied_seqno) = last_applied_seqno {
                let last_applied_diff_opt = internal_queue::queue::Queue::get_diff_info(
                    self,
                    &block_id_short.shard,
                    last_applied_seqno,
                    zone,
                )?;

                if let Some(last_applied_diff) = last_applied_diff_opt {
                    // Check if the diff is already applied
                    if block_id_short.seqno <= last_applied_diff.seqno {
                        return Ok(());
                    }

                    // Check if the diff is sequential
                    if block_id_short.seqno != last_applied_diff.seqno + 1 {
                        bail!(
                            "Diff seqno is not sequential new seqno {}. last_applied_seqno {}",
                            block_id_short.seqno,
                            last_applied_diff.seqno
                        );
                    }
                }
            }
        }

        // Check that applied diff is above the commit pointer
        let commit_pointers = self.state.get_commit_pointers()?;
        if let Some(commit_pointer) = commit_pointers.get(&block_id_short.shard)
            && let Some(min_message) = diff.min_message()
            && min_message <= &commit_pointer.queue_key
        {
            bail!(
                "Diff min_message is less than commit_pointer: block_id={}, diff_min_message={}, commit_pointer={}",
                block_id_short.seqno,
                min_message,
                commit_pointer.queue_key
            );
        }

        self.state
            .write_diff(&block_id_short, &statistics, *hash, diff)?;

        Ok(())
    }

    fn commit_diff(
        &self,
        mc_top_blocks: &[(BlockId, bool)],
        partitions: &FastHashSet<QueuePartitionIdx>,
    ) -> Result<()> {
        // Take global lock
        let _global_write_guard = self.global_lock.write().unwrap_or_else(|e| e.into_inner());

        let mc_block_id = mc_top_blocks
            .iter()
            .find(|(block_id, _)| block_id.is_masterchain())
            .map(|(block_id, _)| block_id)
            .ok_or_else(|| anyhow!("Masterchain block not found in commit_diff"))?;

        // check current commit pointer. If it is greater than committing diff then skip
        let commit_pointers = self.state.get_commit_pointers()?;
        if let Some(commit_pointer) = commit_pointers.get(&mc_block_id.shard)
            && commit_pointer.seqno >= mc_block_id.seqno
        {
            tracing::debug!(
                target: tracing_targets::MQ,
                "Skip commit diff for block_id: {}. Committed by next mc_block_id: {}",
                mc_block_id,
                mc_pointer.seqno
            );
            // Skip commit because it was already committed
            return Ok(());
        }

        let mut gc_ranges = FastHashMap::default();

        let mut commit_pointer = FastHashMap::default();

        for (block_id, top_shard_block_changed) in mc_top_blocks {
            // Check if the diff is already applied
            let diff = self
                .state
                .get_diff_info(&block_id.shard, block_id.seqno, DiffZone::Both)?;

            let diff = match diff {
                // If top shard block changed and diff not found, then bail
                None if *top_shard_block_changed && mc_block_id.seqno != 0 => {
                    bail!("Diff not found for block_id: {}", block_id)
                }
                // If top shard block not changed and diff not found, then continue
                None => continue,
                Some(diff) => diff,
            };

            // Check for duplicate shard in commit_diff
            if commit_pointer
                .insert(block_id.shard, (diff.max_message, diff.seqno))
                .is_some()
            {
                bail!("Duplicate shard in commit_diff: {}", block_id.shard);
            }

            // Update gc ranges
            for (shard_ident, processed_to_key) in diff.processed_to.iter() {
                gc_ranges
                    .entry(*shard_ident)
                    .and_modify(|last: &mut GcEndKey| {
                        if processed_to_key < &last.end_key {
                            last.end_key = *processed_to_key;
                            last.on_top_block_id = *block_id;
                        }
                    })
                    .or_insert(GcEndKey {
                        end_key: *processed_to_key,
                        on_top_block_id: *block_id,
                    });
            }
        }

        // change pointer position
        self.state.commit(&commit_pointer, mc_block_id)?;

        // run GC for each found partition
        for (shard, gc_end_key) in gc_ranges {
            for partition in partitions {
                self.gc.update_delete_until(*partition, shard, gc_end_key);
            }
        }

        Ok(())
    }

    fn clear_uncommitted_state(
        &self,
        partitions: &FastHashSet<QueuePartitionIdx>,
        top_shards: &[ShardIdent],
    ) -> Result<()> {
        // Take global lock
        let _global_write_guard = self.global_lock.write().unwrap_or_else(|e| e.into_inner());
        self.state.clear_uncommitted(partitions, top_shards)
    }

    fn load_diff_statistics(
        &self,
        partition: QueuePartitionIdx,
        range: &QueueShardRange,
        result: &mut AccountStatistics,
    ) -> Result<()> {
        self.state.load_diff_statistics(partition, range, result)
    }

    fn load_separated_diff_statistics(
        &self,
        partitions: &FastHashSet<QueuePartitionIdx>,
        range: &QueueShardRange,
    ) -> Result<SeparatedStatisticsByPartitions> {
        let result = self
            .state
            .load_separated_diff_statistics(partitions, range)?;

        Ok(result)
    }

    fn get_diff_info(
        &self,
        shard_ident: &ShardIdent,
        seqno: u32,
        zone: DiffZone,
    ) -> Result<Option<DiffInfo>> {
        self.state.get_diff_info(shard_ident, seqno, zone)
    }

    fn get_diffs_tail_len(&self, shard_ident: &ShardIdent, from: &QueueKey) -> u32 {
        self.state.get_diffs_tail_len(shard_ident, from)
    }

    fn is_diff_exists(&self, block_id_short: &BlockIdShort) -> Result<bool> {
        Ok(internal_queue::queue::Queue::get_diff_info(
            self,
            &block_id_short.shard,
            block_id_short.seqno,
            DiffZone::Both,
        )?
        .is_some())
    }

    fn get_last_committed_mc_block_id(&self) -> Result<Option<BlockId>> {
        self.state.get_last_committed_mc_block_id()
    }
}
