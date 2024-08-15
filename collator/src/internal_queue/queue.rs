use std::collections::BTreeMap;
use std::sync::Arc;

use everscale_types::models::{BlockIdShort, ShardIdent};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, Semaphore};
use tokio::task;
use tycho_util::metrics::HistogramGuard;
use tycho_util::{FastDashMap, FastHashMap};

use crate::internal_queue::error::QueueError;
use crate::internal_queue::state::persistent_state::{
    PersistentState, PersistentStateFactory, PersistentStateImplFactory, PersistentStateStdImpl,
};
use crate::internal_queue::state::session_state::{
    SessionState, SessionStateFactory, SessionStateImplFactory, SessionStateStdImpl,
};
use crate::internal_queue::state::state_iterator::StateIterator;
use crate::internal_queue::types::{
    InternalMessageKey, InternalMessageValue, QueueDiffWithMessages,
};
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
        ranges: &FastHashMap<ShardIdent, (InternalMessageKey, InternalMessageKey)>,
        for_shard_id: ShardIdent,
    ) -> Vec<Box<dyn StateIterator<V>>>;
    async fn apply_diff(
        &self,
        diff: QueueDiffWithMessages<V>,
        block_id_short: BlockIdShort,
    ) -> Result<(), QueueError>;
    async fn commit_diff(&self, mc_top_blocks: Vec<(BlockIdShort, bool)>)
        -> Result<(), QueueError>;
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

pub struct QueueImpl<S, P, V>
where
    S: SessionState<V>,
    P: PersistentState<V>,
    V: InternalMessageValue,
{
    session_state: Arc<S>,
    persistent_state: Arc<P>,
    diffs: FastDashMap<ShardIdent, BTreeMap<u32, QueueDiffWithMessages<V>>>,
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
        ranges: &FastHashMap<ShardIdent, (InternalMessageKey, InternalMessageKey)>,
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
        mut diff: QueueDiffWithMessages<V>,
        block_id_short: BlockIdShort,
    ) -> Result<(), QueueError> {
        if self.diffs.contains_key(&block_id_short.shard) {
            let shard_diffs = self.diffs.get_mut(&block_id_short.shard).unwrap();
            if shard_diffs.contains_key(&block_id_short.seqno) {
                panic!("Duplicate diff for block_id_short: {:?}", block_id_short)
            }
        }

        if !diff.messages.is_empty() {
            self.session_state
                .add_messages(block_id_short.shard, &diff.messages)?;
            diff.exclude_last_key();
        }

        let mut diffs = self.diffs.entry(block_id_short.shard).or_default();

        diffs.insert(block_id_short.seqno, diff);

        Ok(())
    }

    async fn commit_diff(
        &self,
        mc_top_blocks: Vec<(BlockIdShort, bool)>,
    ) -> Result<(), QueueError> {
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
                        let last_key = shard_diff.last_key.clone().unwrap_or_default();

                        let current_last_key = shards_to_commit
                            .entry(block_id_short.shard)
                            .or_insert_with(|| last_key.clone());

                        if last_key > *current_last_key {
                            *current_last_key = last_key;
                        }

                        // find min processed_upto for each shard for GC
                        if *block_seqno == block_id_short.seqno && *top_shard_block_changed {
                            for processed_upto in shard_diff.processed_upto.iter() {
                                let last_key = gc_ranges
                                    .entry(*processed_upto.0)
                                    .or_insert_with(|| processed_upto.1.clone());

                                if processed_upto.1 < last_key {
                                    *last_key = processed_upto.1.clone();
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
                end_key: end_key.clone(),
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
        let semaphore = Arc::new(Semaphore::new(1));

        // Spawn the worker thread
        task::spawn({
            let semaphore = semaphore.clone();
            let cloned_sender = sender.clone();
            async move {
                while let Some(job) = receiver.recv().await {
                    job.run(semaphore.clone()).await;

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
    end_key: InternalMessageKey,
    persistent_state: Arc<dyn PersistentState<V> + Send + Sync>,
}

impl<V: InternalMessageValue> GCJob<V> {
    pub async fn run(&self, semaphore: Arc<Semaphore>) {
        let shard = self.shard;
        let end_key = self.end_key.clone();
        let persistent_state = self.persistent_state.clone();

        let _permit = semaphore
            .acquire_owned()
            .await
            .expect("Failed to acquire semaphore");

        let histogram = HistogramGuard::begin("tycho_internal_queue_gc_time");
        let cloned_end_key = end_key.clone();
        task::spawn_blocking(move || {
            if let Err(e) = persistent_state.delete_messages(shard, cloned_end_key) {
                tracing::error!(target: tracing_targets::MQ, "Failed to delete messages: {e:?}");
            }
        })
        .await
        .expect("Failed to spawn blocking task");
        histogram.finish();

        let labels = [("shard", shard.to_string())];
        metrics::gauge!("tycho_internal_queue_processed_upto", &labels).set(end_key.lt as f64);
    }
}
