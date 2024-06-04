use std::collections::HashMap;
use std::sync::Arc;

use everscale_types::models::{BlockIdShort, ShardIdent};
use tokio::sync::{Mutex, RwLock};
use tracing::info;

use crate::internal_queue::error::QueueError;
use crate::internal_queue::persistent::persistent_state::{
    PersistentState, PersistentStateConfig, PersistentStateFactory, PersistentStateImplFactory,
    PersistentStateStdImpl,
};
use crate::internal_queue::session::session_state::{
    SessionState, SessionStateFactory, SessionStateImplFactory, SessionStateStdImpl,
};
use crate::internal_queue::snapshot::{ShardRange, StateSnapshot};
use crate::internal_queue::types::QueueDiff;
use crate::tracing_targets;

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
    async fn snapshot(
        &self,
        ranges: &HashMap<ShardIdent, ShardRange>,
        for_shard_id: ShardIdent,
    ) -> Vec<Box<dyn StateSnapshot>>;
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
            session_state: Mutex::new(session_state),
            persistent_state: RwLock::new(persistent_state),
        }
    }
}

pub struct QueueImpl<S, P>
where
    S: SessionState,
    P: PersistentState,
{
    session_state: Mutex<S>,
    persistent_state: RwLock<P>,
}

impl<S, P> Queue for QueueImpl<S, P>
where
    S: SessionState + Send,
    P: PersistentState + Send + Sync,
{
    async fn snapshot(
        &self,
        ranges: &HashMap<ShardIdent, ShardRange>,
        for_shard_id: ShardIdent,
    ) -> Vec<Box<dyn StateSnapshot>> {
        let session_state_lock = self.session_state.lock().await;
        let persistent_state_lock = self.persistent_state.read().await;
        vec![
            // TODO parallel
            session_state_lock
                .snapshot(ranges, for_shard_id.clone())
                .await,
            persistent_state_lock.snapshot(ranges, for_shard_id).await,
        ]
    }

    async fn split_shard(&self, shard_id: &ShardIdent) -> Result<(), QueueError> {
        self.session_state.lock().await.split_shard(shard_id).await
    }

    async fn merge_shards(
        &self,
        shard_1_id: &ShardIdent,
        shard_2_id: &ShardIdent,
    ) -> Result<(), QueueError> {
        self.session_state
            .lock()
            .await
            .merge_shards(shard_1_id, shard_2_id)
            .await
    }

    async fn apply_diff(
        &self,
        diff: Arc<QueueDiff>,
        block_id_short: BlockIdShort,
    ) -> Result<(), QueueError> {
        self.session_state
            .lock()
            .await
            .apply_diff(diff, block_id_short)
            .await
    }

    async fn add_shard(&self, shard_id: &ShardIdent) -> Result<(), QueueError> {
        self.session_state.lock().await.add_shard(shard_id).await
    }

    async fn commit_diff(
        &self,
        diff_id: &BlockIdShort,
    ) -> Result<Option<Arc<QueueDiff>>, QueueError> {
        let session_state_lock = self.session_state.lock().await;
        let persistent_state_lock = self.persistent_state.write().await;
        let diff = session_state_lock.remove_diff(diff_id).await?;
        if let Some(diff) = &diff {
            persistent_state_lock
                .add_messages(*diff_id, diff.messages.clone())
                .await?;
        }
        Ok(diff)
    }
}
// #[cfg(test)]
// mod tests {
//     use everscale_types::models::ShardIdent;
//
//     use super::*;
//     use crate::internal_queue::persistent::persistent_state::{
//         PersistentStateImplFactory, PersistentStateStdImpl,
//     };
//
//     #[tokio::test]
//     async fn test_new_queue() {
//         let base_shard = ShardIdent::new_full(0);
//         let config = QueueConfig {
//             persistent_state_config: PersistentStateConfig {
//                 database_url: "db_url".to_string(),
//             },
//         };
//
//         let session_state_factory = SessionStateImplFactory::new(vec![ShardIdent::new_full(0)]);
//         let persistent_state_factory =
//             PersistentStateImplFactory::new(config.persistent_state_config);
//
//         let queue_factory = QueueFactoryStdImpl {
//             session_state_factory,
//             persistent_state_factory,
//         };
//
//         let queue = queue_factory.create();
//
//         Queue::split_shard(&queue, &base_shard).await.unwrap();
//
//         assert_eq!(queue.session_state.lock().await.shards_count().await, 3);
//     }
// }
