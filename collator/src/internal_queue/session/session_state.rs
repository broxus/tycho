use std::collections::HashMap;
use std::sync::Arc;

use everscale_types::models::{BlockIdShort, ShardIdent};
use tokio::sync::RwLock;

use crate::internal_queue::error::QueueError;
use crate::internal_queue::session::session_state_snapshot::SessionStateSnapshot;
use crate::internal_queue::shard::Shard;
use crate::internal_queue::snapshot::StateSnapshot;
use crate::internal_queue::types::QueueDiff;

#[trait_variant::make(SessionState: Send)]
pub trait LocalSessionState<S>
where
    S: StateSnapshot,
{
    fn new(base_shard: ShardIdent) -> Self;
    async fn snapshot(&self) -> Box<S>;
    async fn split_shard(&self, shard_ident: &ShardIdent) -> Result<(), QueueError>;
    async fn apply_diff(&self, diff: Arc<QueueDiff>) -> Result<(), QueueError>;
    async fn remove_diff(
        &self,
        diff_id: &BlockIdShort,
    ) -> Result<Option<Arc<QueueDiff>>, QueueError>;
}

pub struct SessionStateImpl {
    shards_flat: RwLock<HashMap<ShardIdent, Arc<RwLock<Shard>>>>,
}

impl SessionState<SessionStateSnapshot> for SessionStateImpl {
    fn new(base_shard: ShardIdent) -> Self {
        let mut shards_flat = HashMap::new();
        shards_flat.insert(base_shard, Arc::new(RwLock::new(Shard::new(base_shard))));
        Self {
            shards_flat: RwLock::new(shards_flat),
        }
    }

    async fn snapshot(&self) -> Box<SessionStateSnapshot> {
        let shards_flat_read = self.shards_flat.read().await;
        let mut flat_shards = HashMap::new();
        for (shard_ident, shard_lock) in shards_flat_read.iter() {
            let shard = shard_lock.read().await;
            flat_shards.insert(*shard_ident, shard.clone());
        }
        Box::new(SessionStateSnapshot::new(flat_shards))
    }

    async fn split_shard(&self, shard_id: &ShardIdent) -> Result<(), QueueError> {
        let mut lock = self.shards_flat.write().await;
        lock.get(shard_id)
            .ok_or(QueueError::ShardNotFound(*shard_id))?;
        let split = shard_id.split();
        if let Some(split) = split {
            lock.insert(split.0, Arc::new(RwLock::new(Shard::new(split.0))));
            lock.insert(split.1, Arc::new(RwLock::new(Shard::new(split.1))));
        };
        Ok(())
    }

    async fn apply_diff(&self, diff: Arc<QueueDiff>) -> Result<(), QueueError> {
        let locker = self.shards_flat.write().await;
        let shard = locker
            .get(&diff.id.shard)
            .ok_or(QueueError::ShardNotFound(diff.id.shard))?;
        shard.write().await.add_diff(diff);
        Ok(())
    }

    async fn remove_diff(
        &self,
        diff_id: &BlockIdShort,
    ) -> Result<Option<Arc<QueueDiff>>, QueueError> {
        let lock = self.shards_flat.write().await;
        let shard = lock
            .get(&diff_id.shard)
            .ok_or(QueueError::ShardNotFound(diff_id.shard))?;
        let diff = shard.write().await.remove_diff(diff_id);
        Ok(diff)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use everscale_types::models::BlockIdShort;

    use super::*;
    use crate::internal_queue::types::ext_types_stubs::{
        EnqueuedMessage, MessageContent, MessageEnvelope,
    };

    fn test_shard_ident() -> ShardIdent {
        ShardIdent::new_full(0)
    }

    fn default_message() -> Arc<EnqueuedMessage> {
        Arc::new(EnqueuedMessage {
            created_lt: 0,
            enqueued_lt: 0,
            hash: "somehash".to_string(),
            env: MessageEnvelope {
                message: MessageContent {},
                from_contract: "0".to_string(),
                to_contract: "1".to_string(),
            },
        })
    }

    #[tokio::test]
    async fn test_split_shard() {
        let base_shard = test_shard_ident();
        let session_state =
            <SessionStateImpl as SessionState<SessionStateSnapshot>>::new(base_shard);
        let split_shard_result = SessionState::split_shard(&session_state, &base_shard).await;
        assert!(
            split_shard_result.is_ok(),
            "Splitting the shard should succeed."
        );
    }

    #[tokio::test]
    async fn test_apply_diff() {
        let base_shard = test_shard_ident();
        let session_state =
            <SessionStateImpl as SessionState<SessionStateSnapshot>>::new(base_shard);
        let block_id = BlockIdShort {
            shard: base_shard,
            seqno: 0,
        };
        let diff = Arc::new(QueueDiff {
            id: block_id,
            messages: vec![default_message()],
            processed_upto: Default::default(),
        });
        let apply_diff_result = SessionState::apply_diff(&session_state, diff).await;
        assert_eq!(
            session_state
                .shards_flat
                .read()
                .await
                .get(&block_id.shard)
                .unwrap()
                .read()
                .await
                .diffs
                .len(),
            1
        );
        assert_eq!(
            session_state
                .shards_flat
                .read()
                .await
                .get(&block_id.shard)
                .unwrap()
                .read()
                .await
                .outgoing_messages
                .len(),
            1
        );
        assert!(apply_diff_result.is_ok(), "Applying diff should succeed.");
    }

    #[tokio::test]
    async fn test_remove_diff() {
        let base_shard = test_shard_ident();
        let session_state =
            <SessionStateImpl as SessionState<SessionStateSnapshot>>::new(base_shard);
        let diff_id = BlockIdShort {
            shard: base_shard,
            seqno: 0,
        };
        let remove_diff_result = SessionState::remove_diff(&session_state, &diff_id).await;
        assert_eq!(
            session_state
                .shards_flat
                .read()
                .await
                .get(&base_shard)
                .unwrap()
                .read()
                .await
                .diffs
                .len(),
            0
        );
        assert_eq!(
            session_state
                .shards_flat
                .read()
                .await
                .get(&base_shard)
                .unwrap()
                .read()
                .await
                .outgoing_messages
                .len(),
            0
        );
        assert!(remove_diff_result.is_ok(), "Removing diff should succeed.");
    }

    #[tokio::test]
    async fn test_snapshot() {
        let base_shard = test_shard_ident();
        let session_state =
            <SessionStateImpl as SessionState<SessionStateSnapshot>>::new(base_shard);
        let block_id = BlockIdShort {
            shard: base_shard,
            seqno: 0,
        };
        let diff = Arc::new(QueueDiff {
            id: block_id,
            messages: vec![default_message()],
            processed_upto: Default::default(),
        });
        let _apply_diff_result = SessionState::apply_diff(&session_state, diff).await;

        let snapshot = SessionState::snapshot(&session_state).await;
        assert_eq!(snapshot.flat_shards.len(), 1);
        assert_eq!(
            snapshot
                .flat_shards
                .get(&base_shard)
                .unwrap()
                .outgoing_messages
                .len(),
            1
        );
    }
}
