use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use everscale_types::models::{BlockIdShort, ShardIdent};

use crate::internal_queue::iterator::QueueIterator;
use crate::internal_queue::persistent::persistent_state::PersistentStateStdImpl;
use crate::internal_queue::queue::{Queue, QueueImpl};
use crate::internal_queue::session::session_state::SessionStateStdImpl;
use crate::internal_queue::types::QueueDiff;
use crate::tracing_targets;
use crate::utils::shard::SplitMergeAction;

pub struct MessageQueueAdapterStdImpl {
    queue: QueueImpl<SessionStateStdImpl, PersistentStateStdImpl>,
}
#[async_trait]
pub trait MessageQueueAdapter: Send + Sync + 'static {
    /// Perform split and merge in the current queue state in accordance with the new shards set
    async fn update_shards(&self, split_merge_actions: Vec<SplitMergeAction>) -> Result<()>;
    /// Create iterator for specified shard and return it
    async fn get_iterator(&self, shard_id: &ShardIdent) -> Result<Box<dyn QueueIterator>>;
    /// Apply diff to the current queue session state (waiting for the operation to complete)
    async fn apply_diff(&self, diff: Arc<QueueDiff>) -> Result<()>;
    /// Commit previously applied diff, saving changes to persistent state (waiting for the operation to complete).
    /// Return `None` if specified diff does not exist.
    async fn commit_diff(&self, diff_id: &BlockIdShort) -> Result<Option<()>>;
}

impl MessageQueueAdapterStdImpl {
    pub fn new(queue: QueueImpl<SessionStateStdImpl, PersistentStateStdImpl>) -> Self {
        Self { queue }
    }
}

#[async_trait]
impl MessageQueueAdapter for MessageQueueAdapterStdImpl {
    async fn update_shards(&self, split_merge_actions: Vec<SplitMergeAction>) -> Result<()> {
        for sma in split_merge_actions {
            match sma {
                SplitMergeAction::Split(shard_id) => {
                    self.queue.split_shard(&shard_id).await?;
                    let (shard_l_id, shard_r_id) = shard_id
                        .split()
                        .expect("all split/merge actions should be valid there");
                    tracing::info!(
                        target: tracing_targets::MQ_ADAPTER,
                        "Shard {} splitted on {} and {} in message queue",
                        shard_id,
                        shard_l_id,
                        shard_r_id,
                    );
                }
                SplitMergeAction::Merge(_shard_id_1, _shard_id_2) => {
                    // do nothing because current queue impl does not need to perform merges
                }
            }
        }
        tracing::info!(target: tracing_targets::MQ_ADAPTER, "Updated shards in message queue");
        Ok(())
    }

    async fn get_iterator(&self, _shard_id: &ShardIdent) -> Result<Box<dyn QueueIterator>> {
        todo!()
    }

    async fn apply_diff(&self, _diff: Arc<QueueDiff>) -> Result<()> {
        todo!()
    }

    async fn commit_diff(&self, _diff_id: &BlockIdShort) -> Result<Option<()>> {
        // TODO: make real implementation
        // STUB: just return oks
        Ok(Some(()))
    }
}
