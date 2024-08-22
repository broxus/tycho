use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{bail, Result};
use everscale_types::cell::HashBytes;
use everscale_types::models::{BlockIdShort, ShardIdent};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::task;
use tycho_block_util::queue::QueueKey;
use tycho_util::metrics::HistogramGuard;
use tycho_util::{FastDashMap, FastHashMap};

use crate::internal_queue::state::persistent_state::{
    PersistentState, PersistentStateFactory, PersistentStateImplFactory, PersistentStateStdImpl,
};
use crate::internal_queue::state::session_state::{
    SessionState, SessionStateFactory, SessionStateImplFactory, SessionStateStdImpl,
};
use crate::internal_queue::state::state_iterator::StateIterator;
use crate::internal_queue::types::{InternalMessageValue, QueueDiffWithMessages};
use crate::tracing_targets;
// FACTORY

#[derive(Debug, Serialize, Deserialize)]
pub struct QueueConfig {
    pub gc_queue_buffer_size: usize,
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            gc_queue_buffer_size: 100,
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
    pub gc_queue_buffer_size: usize,
}

// TRAIT

#[trait_variant::make(Queue: Send)]
pub trait LocalQueue<V>
where
    V: InternalMessageValue + Send + Sync,
{
    async fn iterator(
        &self,
        ranges: &FastHashMap<ShardIdent, (QueueKey, QueueKey)>,
        for_shard_id: ShardIdent,
    ) -> Vec<Box<dyn StateIterator<V>>>;
    async fn apply_diff(
        &self,
        diff: QueueDiffWithMessages<V>,
        block_id_short: BlockIdShort,
        diff_hash: HashBytes,
    ) -> Result<()>;
    async fn commit_diff(&self, mc_top_blocks: Vec<(BlockIdShort, bool)>) -> Result<()>;
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
        QueueImpl {
            session_state: Arc::new(session_state),
            persistent_state: Arc::new(persistent_state),
            diffs: Default::default(),
            gc: GCQueue::new(self.gc_queue_buffer_size),
        }
    }
}

struct ShortQueueDiff {
    pub processed_upto: BTreeMap<ShardIdent, QueueKey>,
    pub last_key: Option<QueueKey>,
    pub hash: HashBytes,
}

impl<V: InternalMessageValue> From<(QueueDiffWithMessages<V>, HashBytes)> for ShortQueueDiff {
    fn from(value: (QueueDiffWithMessages<V>, HashBytes)) -> Self {
        Self {
            processed_upto: value.0.processed_upto,
            last_key: value.0.messages.last_key_value().map(|(key, _)| *key),
            hash: value.1,
        }
    }
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
    gc: GCQueue<V>,
}

impl<S, P, V> Queue<V> for QueueImpl<S, P, V>
where
    S: SessionState<V> + Send + Sync,
    P: PersistentState<V> + Send + Sync + 'static,
    V: InternalMessageValue + Send + Sync,
{
    async fn iterator(
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

    async fn apply_diff(
        &self,
        diff: QueueDiffWithMessages<V>,
        block_id_short: BlockIdShort,
        hash: HashBytes,
    ) -> Result<()> {
        // Get or insert the shard diffs for the given block_id_short.shard
        let mut shard_diffs = self.diffs.entry(block_id_short.shard).or_default();

        // Check for duplicate diffs based on the block_id_short.seqno and hash
        let shard_diff = shard_diffs.get(&block_id_short.seqno);
        if let Some(shard_diff) = shard_diff {
            if shard_diff.hash != hash {
                bail!("Duplicate diff with different hash")
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
                bail!("Diff seqno is not sequential");
            }
        }

        // Add messages to session_state if there are any
        if !diff.messages.is_empty() {
            self.session_state
                .add_messages(block_id_short.shard, &diff.messages)?;
        }

        // Insert the diff into the shard diffs
        shard_diffs.insert(block_id_short.seqno, (diff, hash).into());

        Ok(())
    }

    async fn commit_diff(&self, mc_top_blocks: Vec<(BlockIdShort, bool)>) -> Result<()> {
        let mut diffs_for_commit = vec![];
        let mut shards_to_commit = FastHashMap::default();
        let mut gc_ranges = FastHashMap::default();

        for (block_id_short, top_shard_block_changed) in mc_top_blocks.iter() {
            let mut diffs_to_remove = vec![];
            let prev_shard_diffs = self.diffs.get_mut(&block_id_short.shard);

            if let Some(mut shard_diffs) = prev_shard_diffs {
                shard_diffs
                    .range(..=block_id_short.seqno)
                    .for_each(|(block_seqno, shard_diff)| {
                        // find last key to commit for each shard
                        diffs_for_commit.push(*block_id_short);
                        let last_key = shard_diff.last_key.unwrap_or_default();

                        let current_last_key = shards_to_commit
                            .entry(block_id_short.shard)
                            .or_insert_with(|| last_key);

                        if last_key > *current_last_key {
                            *current_last_key = last_key;
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

                        diffs_to_remove.push(*block_seqno);
                    });

                for seqno in diffs_to_remove {
                    shard_diffs.remove(&seqno);
                }
            }
        }

        self.session_state.commit_messages(&shards_to_commit)?;

        let uncommitted_diffs_count: usize = self.diffs.iter().map(|r| r.value().len()).sum();
        metrics::counter!("tycho_internal_queue_uncommitted_diffs_count")
            .increment(uncommitted_diffs_count as u64);

        for (shard, end_key) in gc_ranges.iter() {
            let job = GCJob {
                shard: *shard,
                end_key: *end_key,
                persistent_state: self.persistent_state.clone(),
            };
            self.gc.enqueue(job).await;
        }

        Ok(())
    }
}

#[derive(Clone)]
pub struct GCQueue<V: InternalMessageValue> {
    sender: mpsc::Sender<GCJob<V>>,
    buffer_size: usize,
}

impl<V: InternalMessageValue> GCQueue<V> {
    pub fn new(buffer_size: usize) -> Self {
        let (sender, mut receiver) = mpsc::channel::<GCJob<V>>(buffer_size);

        // Spawn the worker thread
        task::spawn({
            let cloned_sender = sender.clone();
            async move {
                while let Some(job) = receiver.recv().await {
                    job.run().await;

                    let current_queue_size = buffer_size - cloned_sender.capacity();

                    metrics::counter!("tycho_internal_queue_gc_current_queue_size")
                        .increment(current_queue_size as u64);
                }
            }
        });

        GCQueue {
            sender,
            buffer_size,
        }
    }

    pub async fn enqueue(&self, job: GCJob<V>) {
        self.sender.send(job).await.unwrap();

        let current_queue_size = self.buffer_size - self.sender.capacity();

        metrics::counter!("tycho_internal_queue_gc_current_queue_size")
            .increment(current_queue_size as u64);
    }
}

pub struct GCJob<V: InternalMessageValue> {
    shard: ShardIdent,
    end_key: QueueKey,
    persistent_state: Arc<dyn PersistentState<V> + Send + Sync>,
}

impl<V: InternalMessageValue> GCJob<V> {
    pub async fn run(&self) {
        let shard = self.shard;
        let end_key = self.end_key;
        let persistent_state = self.persistent_state.clone();

        let histogram = HistogramGuard::begin("tycho_internal_queue_gc_time");
        task::spawn_blocking(move || {
            let histogram = HistogramGuard::begin("tycho_internal_queue_gc_time_run");
            if let Err(e) = persistent_state.delete_messages(shard, &end_key) {
                tracing::error!(target: tracing_targets::MQ, "Failed to delete messages: {e:?}");
            }
            histogram.finish();
        })
        .await
        .expect("Failed to spawn blocking task");
        histogram.finish();

        let labels = [("shard", shard.to_string())];
        metrics::gauge!("tycho_internal_queue_processed_upto", &labels).set(end_key.lt as f64);
    }
}
