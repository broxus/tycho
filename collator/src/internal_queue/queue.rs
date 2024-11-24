use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Result};
use everscale_types::cell::HashBytes;
use everscale_types::models::{BlockIdShort, ShardIdent};
use serde::{Deserialize, Serialize};
use tycho_block_util::queue::QueueKey;
use tycho_util::{serde_helpers, FastDashMap, FastHashMap};

use crate::internal_queue::gc::GcManager;
use crate::internal_queue::state::persistent_state::{
    PersistentState, PersistentStateFactory, PersistentStateImplFactory, PersistentStateStdImpl,
};
use crate::internal_queue::state::session_state::{
    SessionState, SessionStateFactory, SessionStateImplFactory, SessionStateStdImpl,
};
use crate::internal_queue::state::state_iterator::StateIterator;
use crate::internal_queue::types::{InternalMessageValue, QueueDiffWithMessages};

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
    pub session_state_factory: SessionStateImplFactory,
    pub persistent_state_factory: PersistentStateImplFactory,
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
        ranges: &FastHashMap<ShardIdent, (QueueKey, QueueKey)>,
        for_shard_id: ShardIdent,
    ) -> Vec<Box<dyn StateIterator<V>>>;
    /// Add messages to session state from `diff.messages` and add diff to the cache
    fn apply_diff(
        &self,
        diff: QueueDiffWithMessages<V>,
        block_id_short: BlockIdShort,
        diff_hash: &HashBytes,
        end_key: QueueKey,
    ) -> Result<()>;
    /// Move messages from session state to persistent state and update gc ranges
    fn commit_diff(&self, mc_top_blocks: &[(BlockIdShort, bool)]) -> Result<()>;
    /// remove all data in session state storage
    fn clear_session_state(&self) -> Result<()>;
    /// returns the number of diffs in cache for the given shard
    fn get_diffs_count_by_shard(&self, shard_ident: &ShardIdent) -> usize;
    /// removes all diffs from the cache that are less than `inclusive_until` which source shard is `source_shard`
    fn trim_diffs(&self, source_shard: &ShardIdent, inclusive_until: &QueueKey) -> Result<()>;
}

// IMPLEMENTATION

impl<V: InternalMessageValue> QueueFactory<V> for QueueFactoryStdImpl {
    type Queue = QueueImpl<SessionStateStdImpl, PersistentStateStdImpl, V>;

    fn create(&self) -> Self::Queue {
        let session_state = <SessionStateImplFactory as SessionStateFactory<V>>::create(
            &self.session_state_factory,
        );
        let persistent_state = <PersistentStateImplFactory as PersistentStateFactory<V>>::create(
            &self.persistent_state_factory,
        );
        let persistent_state = Arc::new(persistent_state);
        let gc = GcManager::start::<V>(persistent_state.clone(), self.config.gc_interval);
        QueueImpl {
            session_state: Arc::new(session_state),
            persistent_state,
            diffs: Default::default(),
            gc,
            _phantom_data: Default::default(),
        }
    }
}

struct ShortQueueDiff {
    pub processed_upto: BTreeMap<ShardIdent, QueueKey>,
    pub end_key: QueueKey,
    pub hash: HashBytes,
}
pub struct QueueImpl<S, P, V>
where
    S: SessionState<V>,
    P: PersistentState<V>,
    V: InternalMessageValue,
{
    session_state: Arc<S>,
    persistent_state: Arc<P>,
    diffs: FastDashMap<ShardIdent, BTreeMap<u32, ShortQueueDiff>>,
    gc: GcManager,
    _phantom_data: PhantomData<V>,
}

impl<S, P, V> Queue<V> for QueueImpl<S, P, V>
where
    S: SessionState<V> + Send + Sync,
    P: PersistentState<V> + Send + Sync + 'static,
    V: InternalMessageValue + Send + Sync,
{
    fn iterator(
        &self,
        ranges: &FastHashMap<ShardIdent, (QueueKey, QueueKey)>,
        for_shard_id: ShardIdent,
    ) -> Vec<Box<dyn StateIterator<V>>> {
        let snapshot = self.persistent_state.snapshot();
        let persistent_state_iterator =
            self.persistent_state
                .iterator(&snapshot, for_shard_id, ranges);
        let session_state_iterator = self.session_state.iterator(&snapshot, for_shard_id, ranges);

        vec![persistent_state_iterator, session_state_iterator]
    }

    fn apply_diff(
        &self,
        diff: QueueDiffWithMessages<V>,
        block_id_short: BlockIdShort,
        hash: &HashBytes,
        end_key: QueueKey,
    ) -> Result<()> {
        // Get or insert the shard diffs for the given block_id_short.shard
        let mut shard_diffs = self.diffs.entry(block_id_short.shard).or_default();

        // Check for duplicate diffs based on the block_id_short.seqno and hash
        let shard_diff = shard_diffs.get(&block_id_short.seqno);
        if let Some(shard_diff) = shard_diff {
            if &shard_diff.hash != hash {
                bail!(
                    "Duplicate diff with different hash: block_id={}, existing diff_hash={}, new diff_hash={}",
                    block_id_short, shard_diff.hash,  hash,
                )
            } else {
                return Ok(());
            }
        }

        let last_applied_seqno = shard_diffs.last_key_value().map(|(key, _)| *key);

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

        // Add messages to session_state if there are any
        if !diff.messages.is_empty() {
            self.session_state
                .add_messages(block_id_short.shard, &diff.messages)?;
        }

        let short_diff = ShortQueueDiff {
            processed_upto: diff.processed_upto,
            end_key,
            hash: *hash,
        };

        // Insert the diff into the shard diffs
        shard_diffs.insert(block_id_short.seqno, short_diff);

        Ok(())
    }

    fn commit_diff(&self, mc_top_blocks: &[(BlockIdShort, bool)]) -> Result<()> {
        let mut diffs_for_commit = vec![];
        let mut shards_to_commit = FastHashMap::default();
        let mut gc_ranges = FastHashMap::default();

        for (block_id_short, top_shard_block_changed) in mc_top_blocks {
            let prev_shard_diffs = self.diffs.get_mut(&block_id_short.shard);

            if let Some(shard_diffs) = prev_shard_diffs {
                shard_diffs
                    .range(..=block_id_short.seqno)
                    .for_each(|(block_seqno, shard_diff)| {
                        diffs_for_commit.push(*block_id_short);

                        let current_last_key = shards_to_commit
                            .entry(block_id_short.shard)
                            .or_insert_with(|| shard_diff.end_key);

                        if shard_diff.end_key > *current_last_key {
                            *current_last_key = shard_diff.end_key;
                        }

                        // find min processed_upto for each shard for GC
                        if *block_seqno == block_id_short.seqno && *top_shard_block_changed {
                            for processed_upto in shard_diff.processed_upto.iter() {
                                let last_key = gc_ranges
                                    .entry(*processed_upto.0)
                                    .or_insert_with(|| *processed_upto.1);

                                if processed_upto.1 < last_key {
                                    *last_key = *processed_upto.1;
                                }
                            }
                        }
                    });
            }
        }

        self.session_state.commit_messages(&shards_to_commit)?;

        let uncommitted_diffs_count: usize = self.diffs.iter().map(|r| r.value().len()).sum();
        metrics::counter!("tycho_internal_queue_uncommitted_diffs_count")
            .increment(uncommitted_diffs_count as u64);

        for (shard, end_key) in gc_ranges {
            self.gc.update_delete_until(shard, end_key);
        }

        Ok(())
    }

    fn clear_session_state(&self) -> Result<()> {
        self.session_state.truncate()
    }

    fn get_diffs_count_by_shard(&self, shard_ident: &ShardIdent) -> usize {
        self.diffs.get(shard_ident).map_or(0, |diffs| diffs.len())
    }

    fn trim_diffs(&self, source_shard: &ShardIdent, inclusive_until: &QueueKey) -> Result<()> {
        let shard_diffs = self.diffs.get_mut(source_shard);

        if let Some(mut shard_diffs) = shard_diffs {
            shard_diffs
                .value_mut()
                .retain(|_, diff| &diff.end_key > inclusive_until);
        }
        Ok(())
    }
}
