use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::anyhow;
use everscale_types::models::{BlockIdShort, ShardIdent};
use tokio::sync::Mutex;
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
use crate::internal_queue::state::state_iterator::{ShardRange, StateIterator};
use crate::internal_queue::types::{InternalMessageKey, QueueDiff};

// FACTORY

pub struct QueueConfig {
    pub persistent_state_config: PersistentStateConfig,
    pub session_state_config: SessionStateConfig,
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
            session_state: Arc::new(session_state),
            persistent_state: Arc::new(persistent_state),
            state_lock: Arc::new(Default::default()),
            processed_uptos: Default::default(),
            diffs: Default::default(),
        }
    }
}

pub struct QueueImpl<S, P>
where
    S: SessionState,
    P: PersistentState,
{
    session_state: Arc<S>,
    persistent_state: Arc<P>,
    state_lock: Arc<Mutex<()>>,
    processed_uptos: Mutex<BTreeMap<ShardIdent, BTreeMap<ShardIdent, InternalMessageKey>>>,
    diffs: FastDashMap<BlockIdShort, Arc<QueueDiff>>,
}

impl<S, P> Queue for QueueImpl<S, P>
where
    S: SessionState + Send + Sync,
    P: PersistentState + Send + Sync + 'static,
{
    async fn iterator(
        &self,
        ranges: &FastHashMap<ShardIdent, ShardRange>,
        for_shard_id: ShardIdent,
    ) -> Vec<Box<dyn StateIterator>> {
        let _state_lock = self.state_lock.lock().await;

        let snapshot = self.persistent_state.snapshot();
        let persistent_iter = self
            .persistent_state
            .iterator(&snapshot, for_shard_id, ranges);
        let session_iter = self.session_state.iterator(&snapshot, for_shard_id, ranges);

        vec![persistent_iter, session_iter]
    }

    async fn split_shard(&self, _shard_id: &ShardIdent) -> Result<(), QueueError> {
        Ok(())
    }

    async fn merge_shards(
        &self,
        _shard_1_id: &ShardIdent,
        _shard_2_id: &ShardIdent,
    ) -> Result<(), QueueError> {
        Ok(())
    }

    async fn apply_diff(
        &self,
        diff: Arc<QueueDiff>,
        block_id_short: BlockIdShort,
    ) -> Result<(), QueueError> {
        let _session_lock = self.state_lock.lock().await;
        self.session_state
            .add_messages(block_id_short.shard, &diff.messages)?;
        self.diffs.insert(block_id_short, diff);

        Ok(())
    }

    async fn add_shard(&self, _shard_id: &ShardIdent) -> Result<(), QueueError> {
        Ok(())
    }

    async fn commit_diff(&self, diff_id: &BlockIdShort) -> Result<Arc<QueueDiff>, QueueError> {
        let _session_lock = self.state_lock.lock().await;

        let diff = self
            .diffs
            .remove(diff_id)
            .ok_or(anyhow!("diff not found"))?
            .1;

        let for_shard = diff_id.shard;

        let first_key = diff.messages.keys().next();
        let last_key = diff.messages.keys().next_back();

        if let (Some(first_key), Some(last_key)) = (first_key, last_key) {
            if first_key.lt > last_key.lt {
                panic!("first_key.lt > last_key.lt");
            }

            let snapshot = self.persistent_state.snapshot();

            let len = diff.messages.len();
            let messages = self.session_state.retrieve_messages(
                &snapshot,
                for_shard,
                (&first_key, &last_key),
            )?;

            let len2 = diff.messages.len();
            let count = self.persistent_state.add_messages(for_shard, messages)?;
            assert_eq!(len2 as usize, len);
            assert_eq!(count as usize, len);
        }

        // for (shard, processed_upto) in diff.processed_upto.iter() {
        //     // let mut processed_uptos_lock = self.processed_uptos.lock().await;
        //     // processed_uptos_lock.insert(shard.clone(), range.clone());
        //     let persistent_state = self.persistent_state.clone();
        //
        //     tracing::error!(
        //         target: "local_debug",
        //         "delete messages diff : reader = {:?}, in shard = {:?}, processed_upto = {:?}",
        //         for_shard,
        //         shard,
        //         processed_upto
        //     );
        //
        //     let mut delete_until = BTreeMap::new();
        //     delete_until.insert(shard.clone(), (processed_upto.lt, processed_upto.hash));
        //
        //     tokio::spawn(async move {
        //         persistent_state
        //             .delete_messages(for_shard.clone(), delete_until)
        //             .unwrap();
        //     });
        // }

        // {
        //     let mut processed_uptos_lock = self.processed_uptos.lock().await;
        //     processed_uptos_lock.insert(for_shard, diff.processed_upto.clone());
        //
        //     if for_shard.is_masterchain() {
        //         let processed_uptos = processed_uptos_lock.clone();
        //         for processed_upto in processed_uptos {
        //             let persistent_state = self.persistent_state.clone();
        //             let delete_until: BTreeMap<ShardIdent, (u64, HashBytes)> = processed_upto
        //                 .1
        //                 .iter()
        //                 .map(|(shard, range)| (shard.clone(), (range.lt, range.hash)))
        //                 .collect();
        //             let for_shard = processed_upto.0;
        //             tokio::spawn(async move {
        //                 persistent_state
        //                     .delete_messages(for_shard, delete_until)
        //                     .unwrap();
        //             });
        //         }
        //         processed_uptos_lock.clear();
        //     }
        // }

        Ok(diff)
    }
}
