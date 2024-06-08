use std::collections::HashMap;
use std::sync::Arc;

use everscale_types::models::{BlockIdShort, ShardIdent};
use tokio::sync::RwLock;

use crate::internal_queue::error::QueueError;
use crate::internal_queue::shard::Shard;
use crate::internal_queue::state::session::session_state_iterator::SessionStateIterator;
use crate::internal_queue::state::state_iterator::{ShardRange, StateIterator};
use crate::internal_queue::types::QueueDiff;

// FACTORY

pub trait SessionStateFactory {
    type SessionState: LocalSessionState;
    fn create(&self) -> Self::SessionState;
}

impl<F, R> SessionStateFactory for F
where
    F: Fn() -> R,
    R: LocalSessionState,
{
    type SessionState = R;

    fn create(&self) -> Self::SessionState {
        self()
    }
}

pub struct SessionStateImplFactory {
    shards: Vec<ShardIdent>,
}

impl SessionStateImplFactory {
    pub fn new(shards: Vec<ShardIdent>) -> Self {
        Self { shards }
    }
}

impl SessionStateFactory for SessionStateImplFactory {
    type SessionState = SessionStateStdImpl;

    fn create(&self) -> Self::SessionState {
        <SessionStateStdImpl as LocalSessionState>::new(self.shards.as_slice())
    }
}

// TRAIT

#[trait_variant::make(SessionState: Send)]
pub trait LocalSessionState {
    fn new(shards: &[ShardIdent]) -> Self;
    async fn iterator(
        &self,
        ranges: &HashMap<ShardIdent, ShardRange>,
        for_shard_id: ShardIdent,
    ) -> Box<dyn StateIterator>;
    async fn split_shard(&self, shard_ident: &ShardIdent) -> Result<(), QueueError>;
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
    async fn remove_diff(
        &self,
        diff_id: &BlockIdShort,
    ) -> Result<Option<Arc<QueueDiff>>, QueueError>;
}

// IMPLEMENTATION

pub struct SessionStateStdImpl {
    shards_flat: RwLock<HashMap<ShardIdent, Arc<RwLock<Shard>>>>,
}

impl SessionState for SessionStateStdImpl {
    fn new(shards: &[ShardIdent]) -> Self {
        let mut shards_flat = HashMap::new();
        for &shard in shards {
            shards_flat.insert(shard, Arc::new(RwLock::new(Shard::default())));
        }
        Self {
            shards_flat: RwLock::new(shards_flat),
        }
    }

    async fn iterator(
        &self,
        ranges: &HashMap<ShardIdent, ShardRange>,
        for_shard_id: ShardIdent,
    ) -> Box<dyn StateIterator> {
        let shards_flat_read = self.shards_flat.read().await;
        let mut flat_shards = HashMap::new();
        for (shard_ident, shard_lock) in shards_flat_read.iter() {
            let shard = shard_lock.read().await;
            flat_shards.insert(*shard_ident, shard.clone());
        }
        Box::new(SessionStateIterator::new(
            flat_shards,
            ranges,
            &for_shard_id,
        ))
    }

    async fn split_shard(&self, shard_id: &ShardIdent) -> Result<(), QueueError> {
        let shard_arc = {
            let lock = self.shards_flat.read().await;
            lock.get(shard_id)
                .ok_or(QueueError::ShardNotFound(*shard_id))?
                .clone()
        };

        let shard_messages_len = shard_arc.read().await.outgoing_messages.len();
        if shard_messages_len > 0 {
            return Err(QueueError::Other(anyhow::anyhow!(
                "Cannot split shard with messages"
            )));
        }

        let split = shard_id
            .split()
            .ok_or(QueueError::Other(anyhow::anyhow!("Failed to split shard")))?;

        let mut lock = self.shards_flat.write().await;
        if lock.contains_key(&split.0) || lock.contains_key(&split.1) {
            return Err(QueueError::Other(anyhow::anyhow!(
                "Splitted shards already exist in the shards"
            )));
        }

        lock.insert(split.0, Arc::new(RwLock::new(Shard::default())));
        lock.insert(split.1, Arc::new(RwLock::new(Shard::default())));

        Ok(())
    }

    async fn merge_shards(
        &self,
        shard_1_id: &ShardIdent,
        shard_2_id: &ShardIdent,
    ) -> Result<(), QueueError> {
        let shard_1_arc = {
            let lock = self.shards_flat.read().await;
            lock.get(shard_1_id)
                .ok_or(QueueError::ShardNotFound(*shard_1_id))?
                .clone()
        };

        let shard_2_arc = {
            let lock = self.shards_flat.read().await;
            lock.get(shard_2_id)
                .ok_or(QueueError::ShardNotFound(*shard_2_id))?
                .clone()
        };

        let shard_1_messages_len = shard_1_arc.read().await.outgoing_messages.len();
        let shard_2_messages_len = shard_2_arc.read().await.outgoing_messages.len();
        if shard_1_messages_len > 0 || shard_2_messages_len > 0 {
            return Err(QueueError::Other(anyhow::anyhow!(
                "Cannot merge shards with messages"
            )));
        }

        let merged_shard_1 = shard_1_id
            .merge()
            .ok_or(QueueError::Other(anyhow::anyhow!("Failed to merge shard")))?;
        let merged_shard_2 = shard_2_id
            .merge()
            .ok_or(QueueError::Other(anyhow::anyhow!("Failed to merge shard")))?;

        if merged_shard_1 != merged_shard_2 {
            return Err(QueueError::Other(anyhow::anyhow!(
                "Merge shards are not equal"
            )));
        }

        let mut lock = self.shards_flat.write().await;
        if lock.contains_key(&merged_shard_1) {
            return Err(QueueError::Other(anyhow::anyhow!(
                "Merged shard already exists in the shards"
            )));
        }

        lock.insert(merged_shard_1, Arc::new(RwLock::new(Shard::default())));

        Ok(())
    }

    async fn apply_diff(
        &self,
        diff: Arc<QueueDiff>,
        block_id_short: BlockIdShort,
    ) -> Result<(), QueueError> {
        let shard_arc = {
            let lock = self.shards_flat.read().await;
            lock.get(&block_id_short.shard)
                .ok_or(QueueError::ShardNotFound(block_id_short.shard))?
                .clone()
        };
        shard_arc.write().await.add_diff(diff, block_id_short);
        Ok(())
    }

    async fn add_shard(&self, shard_id: &ShardIdent) -> Result<(), QueueError> {
        let mut lock = self.shards_flat.write().await;
        if lock.contains_key(shard_id) {
            return Err(QueueError::ShardAlreadyExists(*shard_id));
        }
        lock.insert(*shard_id, Arc::new(RwLock::new(Shard::default())));
        Ok(())
    }

    async fn remove_diff(
        &self,
        diff_id: &BlockIdShort,
    ) -> Result<Option<Arc<QueueDiff>>, QueueError> {
        let shard_arc = {
            let lock = self.shards_flat.read().await;
            lock.get(&diff_id.shard)
                .ok_or(QueueError::ShardNotFound(diff_id.shard))?
                .clone()
        };
        let diff = shard_arc.write().await.remove_diff(diff_id);
        Ok(diff)
    }
}

impl SessionStateStdImpl {
    pub async fn shards_count(&self) -> usize {
        self.shards_flat.read().await.len()
    }
}
