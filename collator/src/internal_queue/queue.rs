use std::sync::Arc;

use anyhow::anyhow;
use everscale_types::models::{BlockIdShort, ShardIdent};
use tycho_util::{FastDashMap, FastHashMap};

use crate::internal_queue::error::QueueError;
use crate::internal_queue::state::persistent_state::{
    PersistentState, PersistentStateConfig, PersistentStateFactory, PersistentStateImplFactory,
    PersistentStateStdImpl,
};
use crate::internal_queue::state::session_state::{
    SessionState, SessionStateConfig, SessionStateFactory, SessionStateImplFactory,
    SessionStateStdImpl,
};
use crate::internal_queue::state::state_iterator::StateIterator;
use crate::internal_queue::types::{InternalMessageKey, InternalMessageValue, QueueDiff};

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
        diff: QueueDiff<V>,
        block_id_short: BlockIdShort,
    ) -> Result<(), QueueError>;
    async fn commit_diff(&self, diff_id: &BlockIdShort) -> Result<(), QueueError>;
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
    diffs: FastDashMap<BlockIdShort, QueueDiff<V>>,
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
        let persistent_iter = self
            .persistent_state
            .iterator(&snapshot, for_shard_id, ranges);
        let session_iter = self.session_state.iterator(&snapshot, for_shard_id, ranges);
        vec![persistent_iter, session_iter]
    }

    async fn apply_diff(
        &self,
        mut diff: QueueDiff<V>,
        block_id_short: BlockIdShort,
    ) -> Result<(), QueueError> {
        self.session_state
            .add_messages(block_id_short.shard, &diff.messages)?;

        diff.save_keys();
        self.diffs.insert(block_id_short, diff);
        Ok(())
    }

    async fn commit_diff(&self, diff_id: &BlockIdShort) -> Result<(), QueueError> {
        let diff = self
            .diffs
            .remove(diff_id)
            .ok_or(anyhow!("diff not found"))?
            .1;

        let first_key = Some(InternalMessageKey::default());
        let last_key = diff.keys.last();

        if let (Some(first_key), Some(last_key)) = (first_key, last_key) {
            self.session_state
                .commit_messages(diff_id.shard, &first_key, last_key)?;
        }

        // gc
        Ok(())
        // {
        //     let mut processed_uptos_lock = self.processed_uptos.lock().await;
        //     processed_uptos_lock.insert(for_shard, diff.processed_upto.clone());
        //
        //     let mut min_uptos: HashMap<ShardIdent, InternalMessageKey> = HashMap::default();
        //     let mut shard_count: HashMap<ShardIdent, usize> = HashMap::default();
        //
        //     if for_shard.is_masterchain() {
        //         let total_shards = processed_uptos_lock.len();
        //
        //         for (_, processed_upto) in processed_uptos_lock.iter() {
        //             for (processed_in_shard, key) in processed_upto.iter() {
        //                 if let Some(min_key) = min_uptos.get_mut(processed_in_shard) {
        //                     if key < min_key {
        //                         *min_key = key.clone();
        //                     }
        //                 } else {
        //                     min_uptos.insert(processed_in_shard.clone(), key.clone());
        //                 }
        //                 *shard_count.entry(processed_in_shard.clone()).or_insert(0) += 1;
        //             }
        //         }
        //
        //         for (shard, key) in min_uptos.iter() {
        //             if let Some(count) = shard_count.get(shard) {
        //                 if *count == total_shards {
        //                     let persistent_state = self.persistent_state.clone();
        //                     let shard_clone = shard.clone();
        //                     let key_clone = key.clone();
        //                     tokio::spawn(async move {
        //                         persistent_state
        //                             .delete_messages(shard_clone, key_clone)
        //                             .unwrap();
        //                     });
        //                 }
        //             }
        //         }
        //
        //         processed_uptos_lock.clear();
        //     }
        //     Ok(())
        // }
    }
}
