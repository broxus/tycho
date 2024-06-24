use std::collections::BTreeMap;
use std::sync::Arc;

use everscale_types::cell::HashBytes;
use everscale_types::models::{BlockIdShort, ShardIdent};
use tokio::sync::Mutex;
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
use crate::internal_queue::types::{InternalMessageKey, QueueDiff};
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
    async fn commit_diff(&self, diff_id: &BlockIdShort) -> Result<Arc<QueueDiff>, QueueError>;
}

// IMPLEMENTATION

impl QueueFactory for QueueFactoryStdImpl {
    type Queue = QueueImpl<SessionStateStdImpl, PersistentStateStdImpl>;

    fn create(&self) -> Self::Queue {
        let session_state = self.session_state_factory.create();
        let persistent_state = self.persistent_state_factory.create();
        QueueImpl {
            session_state: Arc::new(Mutex::new(session_state)),
            persistent_state: Arc::new(persistent_state),
            persistent_state_lock: Arc::new(Mutex::new(())),
            processed_uptos: Default::default(),
        }
    }
}

pub struct QueueImpl<S, P>
where
    S: SessionState,
    P: PersistentState,
{
    session_state: Arc<Mutex<S>>,
    persistent_state: Arc<P>,
    persistent_state_lock: Arc<Mutex<()>>,
    processed_uptos: Mutex<BTreeMap<ShardIdent, FastHashMap<ShardIdent, InternalMessageKey>>>,
}

impl<S, P> Queue for QueueImpl<S, P>
where
    S: SessionState + Send,
    P: PersistentState + Send + Sync + 'static,
{
    async fn iterator(
        &self,
        ranges: &FastHashMap<ShardIdent, ShardRange>,
        for_shard_id: ShardIdent,
    ) -> Vec<Box<dyn StateIterator>> {
        // Lock session state and get the session iterator
        let session_iter = {
            let session_state_lock = self.session_state.lock().await;
            let session_iter = session_state_lock.iterator(ranges, for_shard_id).await;
            session_iter
        };

        let mut persistent_iter = {
            let _ = self.persistent_state_lock.lock().await;
            let persistent_iter = self.persistent_state.iterator(for_shard_id, ranges);
            persistent_iter
        };

        let range_start = ranges
            .iter()
            .map(|(shard, range)| {
                let range_start = range.from.clone().unwrap_or_default();
                (shard, range_start)
            })
            .min_by_key(|(_, range_start)| range_start.clone());

        persistent_iter.seek(range_start);

        vec![persistent_iter, session_iter]
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

    async fn commit_diff(&self, diff_id: &BlockIdShort) -> Result<Arc<QueueDiff>, QueueError> {
        // let diff = {
        let _ = self.persistent_state_lock.lock().await;
        let session_state_lock = self.session_state.lock().await;

        let diff = session_state_lock.remove_diff(diff_id).await?;
        // diff
        // };

        let for_shard = diff_id.shard;

        self.persistent_state
            .add_messages(*diff_id, diff.messages.clone())?;

        {
            let mut processed_uptos_lock = self.processed_uptos.lock().await;
            processed_uptos_lock.insert(for_shard, diff.processed_upto.clone());

            if for_shard.is_masterchain() {
                let processed_uptos = processed_uptos_lock.clone();
                for processed_upto in processed_uptos {
                    let persistent_state = self.persistent_state.clone();
                    let delete_until: BTreeMap<ShardIdent, (u64, HashBytes)> = processed_upto
                        .1
                        .iter()
                        .map(|(shard, range)| (shard.clone(), (range.lt, range.hash)))
                        .collect();
                    let for_shard = processed_upto.0;
                    tokio::spawn(async move {
                        persistent_state
                            .delete_messages(for_shard, delete_until)
                            .unwrap();
                    });
                }
                processed_uptos_lock.clear();
            }
        }

        Ok(diff)
    }
}
