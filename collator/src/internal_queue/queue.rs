use std::sync::Arc;

use everscale_types::models::{BlockIdShort, ShardIdent};
use tokio::sync::{Mutex, RwLock};
use tycho_util::FastHashMap;

use crate::internal_queue::error::QueueError;
use crate::internal_queue::state::persistent::persistent_state::{
    PersistentState, PersistentStateConfig, PersistentStateFactory, PersistentStateImplFactory,
    PersistentStateStdImpl,
};
use crate::internal_queue::state::session::session_state::{
    SessionState, SessionStateFactory, SessionStateImplFactory, SessionStateStdImpl,
};
use crate::internal_queue::state::state_iterator::{ShardRange, StateIterator};
use crate::internal_queue::types::QueueDiff;
// FACTORY

pub struct QueueConfig {
    pub persistent_state_config: PersistentStateConfig,
}

pub trait QueueFactory {
    type Queue: Queue;

    fn create(&self) -> Self::Queue;
}

impl<F, R> QueueFactory for F
where
    F: Fn() -> R,
    R: Queue,
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
pub trait LocalQueue {
    async fn iterator(
        &self,
        ranges: &FastHashMap<ShardIdent, ShardRange>,
        for_shard_id: ShardIdent,
    ) -> Vec<Box<dyn StateIterator>>;
    async fn split_shard(&self, shard_id: &ShardIdent) -> Result<(), QueueError>;
    async fn merge_shards(
        &self,
        shard_1_id: &ShardIdent,
        shard_2_id: &ShardIdent,
    ) -> Result<(), QueueError>;
    async fn apply_diff(
        &self,
        diff: Arc<QueueDiff>,
        block_id_short: BlockIdShort,
    ) -> Result<(), QueueError>;
    async fn add_shard(&self, shard_id: &ShardIdent) -> Result<(), QueueError>;
    async fn commit_diff(
        &self,
        diff_id: &BlockIdShort,
    ) -> Result<Option<Arc<QueueDiff>>, QueueError>;
}

// IMPLEMENTATION

impl QueueFactory for QueueFactoryStdImpl {
    type Queue = QueueImpl<SessionStateStdImpl, PersistentStateStdImpl>;

    fn create(&self) -> Self::Queue {
        let session_state = self.session_state_factory.create();
        let persistent_state = self.persistent_state_factory.create();
        QueueImpl {
            session_state: Arc::new(Mutex::new(session_state)),
            persistent_state: Arc::new(RwLock::new(persistent_state)),
        }
    }
}

pub struct QueueImpl<S, P>
where
    S: SessionState,
    P: PersistentState,
{
    session_state: Arc<Mutex<S>>,
    persistent_state: Arc<RwLock<P>>,
}

impl<S, P> Queue for QueueImpl<S, P>
where
    S: SessionState + Send,
    P: PersistentState + Send + Sync,
{
    async fn iterator(
        &self,
        ranges: &FastHashMap<ShardIdent, ShardRange>,
        for_shard_id: ShardIdent,
    ) -> Vec<Box<dyn StateIterator>> {
        let session_iter = {
            let session_state_lock = self.session_state.lock().await;
            session_state_lock.iterator(ranges, for_shard_id).await
        };

        // let persistent_state_lock = self.persistent_state.read().await;
        // let persistent_iter = persistent_state_lock.iterator(for_shard_id);

        // vec![session_iter, persistent_iter]
        vec![session_iter]
    }

    async fn split_shard(&self, shard_id: &ShardIdent) -> Result<(), QueueError> {
        let session_state_lock = self.session_state.lock().await;
        session_state_lock.split_shard(shard_id).await
    }

    async fn merge_shards(
        &self,
        shard_1_id: &ShardIdent,
        shard_2_id: &ShardIdent,
    ) -> Result<(), QueueError> {
        let session_state_lock = self.session_state.lock().await;
        session_state_lock
            .merge_shards(shard_1_id, shard_2_id)
            .await
    }

    async fn apply_diff(
        &self,
        diff: Arc<QueueDiff>,
        block_id_short: BlockIdShort,
    ) -> Result<(), QueueError> {
        let session_state_lock = self.session_state.lock().await;
        session_state_lock.apply_diff(diff, block_id_short).await
    }

    async fn add_shard(&self, shard_id: &ShardIdent) -> Result<(), QueueError> {
        let session_state_lock = self.session_state.lock().await;
        session_state_lock.add_shard(shard_id).await
    }

    async fn commit_diff(
        &self,
        diff_id: &BlockIdShort,
    ) -> Result<Option<Arc<QueueDiff>>, QueueError> {
        let diff = {
            let session_state_lock = self.session_state.lock().await;
            session_state_lock.remove_diff(diff_id).await?
        };
        if let Some(diff) = &diff {
            let persistent_state_lock = self.persistent_state.write().await;
            persistent_state_lock
                .add_messages(*diff_id, diff.messages.clone())
                .await?;
        }
        Ok(diff)
    }
}
