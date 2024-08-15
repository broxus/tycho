use std::sync::Arc;

use anyhow::anyhow;
use everscale_types::models::{BlockIdShort, ShardIdent};
use tycho_util::{FastDashMap, FastHashMap};

use crate::internal_queue::error::QueueError;
use crate::internal_queue::state::persistent_state::{
    LocalPersistentState, PersistentState, PersistentStateConfig, PersistentStateFactory,
    PersistentStateImplFactory, PersistentStateStdImpl,
};
use crate::internal_queue::state::session_state::{
    SessionState, SessionStateConfig, SessionStateFactory, SessionStateImplFactory,
    SessionStateStdImpl,
};
use crate::internal_queue::state::state_iterator::StateIterator;
use crate::internal_queue::types::{
    InternalMessageKey, InternalMessageValue, QueueDiffWithMessages,
};
// FACTORY

pub struct QueueConfig {
    pub persistent_state_config: PersistentStateConfig,
    pub session_state_config: SessionStateConfig,
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
    async fn commit_diff(
        &self,
        diff_id: &BlockIdShort,
        mc_shards: Vec<BlockIdShort>,
    ) -> Result<(), QueueError>;
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
    diffs: FastDashMap<BlockIdShort, QueueDiffWithMessages<V>>,
}

impl<S, P, V> Queue<V> for QueueImpl<S, P, V>
where
    S: SessionState<V> + Send + Sync,
    P: PersistentState<V> + Send + Sync,
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
        tracing::info!(target: "local_debug", "apply diff {block_id_short:?}");
        if !diff.messages.is_empty() {
            self.session_state
                .add_messages(block_id_short.shard, &diff.messages)?;
            diff.exclude_data();
        }
        self.diffs.insert(block_id_short, diff);
        Ok(())
    }

    async fn commit_diff(
        &self,
        diff_id: &BlockIdShort,
        mc_top_shard_blocks: Vec<BlockIdShort>,
    ) -> Result<(), QueueError> {
        tracing::info!(target: "local_debug", "mc shards {mc_top_shard_blocks:?}");
        let diffs_count = self.diffs.len();
        tracing::info!(target: "local_debug", "state before commit {diff_id:?}. mc shards len {}. diffs_count: {diffs_count}", mc_top_shard_blocks.len());
        let mut diffs = vec![];

        let diff = self
            .diffs
            .remove(diff_id)
            .ok_or(anyhow!("masterchain block diff not found {diff_id:?}"))?
            .1;

        diffs.push(diff);

        for shard_block_id in mc_top_shard_blocks {
            // let shard_block_diff = self.diffs.remove(&shard_block_id).ok_or(anyhow!("shard block diff not found {shard_block_id:?}"))?.1;
            let shard_block_diff = self.diffs.remove(&shard_block_id);
            if let Some(shard_block_diff) = shard_block_diff {
                diffs.push(shard_block_diff.1);
            }
            // diffs.push(shard_block_diff);
        }

        let mut min_keys: FastHashMap<ShardIdent, InternalMessageKey> = FastHashMap::default();

        for diff in diffs.iter() {
            for (shard, key) in diff.processed_upto.iter() {
                if let Some(min_key) = min_keys.get_mut(shard) {
                    if key < min_key {
                        *min_key = key.clone();
                    }
                } else {
                    min_keys.insert(shard.clone(), key.clone());
                }
            }

            let first_key = Some(InternalMessageKey::default());
            let last_key = diff.keys.last();

            if let (Some(first_key), Some(last_key)) = (first_key, last_key) {
                self.session_state
                    .commit_messages(diff_id.shard, &first_key, last_key)?;
            }
        }

        tracing::info!(target: "local_debug", "state before gc");
        self.persistent_state.print_state();

        for (shard, key) in min_keys.iter() {
            self.persistent_state
                .delete_messages(shard.clone(), key.clone())?;
        }

        let diffs_count = self.diffs.len();

        tracing::info!(target: "local_debug", "state after gc. diffs_count {diffs_count}");
        self.persistent_state.print_state();

        Ok(())
    }
}

#[derive(Default)]
struct DiffBuffer<V: InternalMessageValue> {
    diffs: FastDashMap<BlockIdShort, QueueDiffWithMessages<V>>,
}

impl<V: InternalMessageValue> DiffBuffer<V> {
    pub fn insert(&mut self, block_id_short: BlockIdShort, diff: QueueDiffWithMessages<V>) {
        self.diffs.insert(block_id_short, diff);
    }
}
