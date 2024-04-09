use crate::internal_queue::error::QueueError;
use crate::internal_queue::persistent::persistent_state::PersistentState;
use crate::internal_queue::session::session_state::SessionState;
use crate::internal_queue::snapshot::StateSnapshot;
use crate::internal_queue::types::QueueDiff;
use everscale_types::models::{BlockIdShort, ShardIdent};
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

pub trait Queue<SS, PS>
where
    SS: StateSnapshot + 'static,
    PS: StateSnapshot + 'static,
{
    fn new(base_shard: ShardIdent) -> Self
    where
        Self: Sized;
    async fn snapshot(&self) -> Vec<Box<dyn StateSnapshot>>;
    async fn split_shard(&self, shard_id: &ShardIdent) -> Result<(), QueueError>;
    async fn apply_diff(&self, diff: Arc<QueueDiff>) -> Result<(), QueueError>;
    async fn commit_diff(
        &self,
        diff_id: &BlockIdShort,
    ) -> Result<Option<Arc<QueueDiff>>, QueueError>;
}
pub struct QueueImpl<S, P, SS, PS>
where
    S: SessionState<SS>,
    P: PersistentState<PS>,
    SS: StateSnapshot,
    PS: StateSnapshot,
{
    session_state: Mutex<S>,
    persistent_state: RwLock<P>,
    _marker: PhantomData<SS>,
    _marker2: PhantomData<PS>,
}

impl<S, P, SS, PS> Queue<SS, PS> for QueueImpl<S, P, SS, PS>
where
    S: SessionState<SS>,
    P: PersistentState<PS>,
    SS: StateSnapshot + 'static,
    PS: StateSnapshot + 'static,
{
    fn new(base_shard: ShardIdent) -> Self {
        let session_state = Mutex::new(S::new(base_shard));
        let persistent_state = RwLock::new(P::new());
        Self {
            session_state,
            persistent_state,
            _marker: PhantomData,
            _marker2: PhantomData,
        }
    }

    async fn snapshot(&self) -> Vec<Box<dyn StateSnapshot>> {
        let session_state_lock = self.session_state.lock().await;
        let persistent_state_lock = self.persistent_state.read().await;
        vec![
            session_state_lock.snapshot().await,
            persistent_state_lock.snapshot().await,
        ]
    }

    async fn split_shard(&self, shard_id: &ShardIdent) -> Result<(), QueueError> {
        self.session_state.lock().await.split_shard(shard_id).await
    }

    async fn apply_diff(&self, diff: Arc<QueueDiff>) -> Result<(), QueueError> {
        self.session_state.lock().await.apply_diff(diff).await
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
                .add_messages(diff.id, diff.messages.clone())
                .await?;
        }
        Ok(diff)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::internal_queue::persistent::persistent_state::PersistentStateImpl;
    use crate::internal_queue::persistent::persistent_state_snapshot::PersistentStateSnapshot;
    use crate::internal_queue::session::session_state::SessionStateImpl;
    use crate::internal_queue::session::session_state_snapshot::SessionStateSnapshot;
    use everscale_types::models::ShardIdent;

    #[tokio::test]
    async fn test_new_queue() {
        let base_shard = ShardIdent::new_full(0);

        let queue: QueueImpl<
            SessionStateImpl,
            PersistentStateImpl,
            SessionStateSnapshot,
            PersistentStateSnapshot,
        > = QueueImpl::new(base_shard);

        queue.split_shard(&base_shard).await.unwrap();
    }
}
