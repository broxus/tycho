use std::sync::Arc;

use anyhow::{bail, Result};
use async_trait::async_trait;
use everscale_types::cell::Cell;
use everscale_types::models::{BlockIdShort, MsgInfo, ShardIdent};
use tracing::instrument;
use tycho_util::FastHashMap;

use crate::internal_queue::iterator::{QueueIterator, QueueIteratorImpl};
use crate::internal_queue::persistent::persistent_state::PersistentStateStdImpl;
use crate::internal_queue::queue::{Queue, QueueImpl};
use crate::internal_queue::session::session_state::SessionStateStdImpl;
use crate::internal_queue::types::{EnqueuedMessage, InternalMessageKey, QueueDiff};
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
    async fn create_iterator(
        &self,
        for_shard_id: ShardIdent,
        shards_from: FastHashMap<ShardIdent, u64>,
        shards_to: FastHashMap<ShardIdent, u64>,
    ) -> Result<Box<dyn QueueIterator>>;
    /// Apply diff to the current queue session state (waiting for the operation to complete)
    async fn apply_diff(&self, diff: Arc<QueueDiff>, block_id_short: BlockIdShort) -> Result<()>;
    /// Commit previously applied diff, saving changes to persistent state (waiting for the operation to complete).
    /// Return `None` if specified diff does not exist.
    async fn commit_diff(&self, diff_id: &BlockIdShort) -> Result<Option<Arc<QueueDiff>>>;
    /// Add new messages to the iterator
    fn add_messages_to_iterator(
        &self,
        iterator: &mut Box<dyn QueueIterator>,
        messages: Vec<(MsgInfo, Cell)>,
    ) -> Result<()>;
    /// Commit processed messages in the iterator
    /// Save last message position for each shard
    fn commit_messages_to_iterator(
        &self,
        iterator: &mut Box<dyn QueueIterator>,
        messages: Vec<(ShardIdent, InternalMessageKey)>,
    ) -> Result<()>;
}

impl MessageQueueAdapterStdImpl {
    pub fn new(queue: QueueImpl<SessionStateStdImpl, PersistentStateStdImpl>) -> Self {
        Self { queue }
    }
}

#[async_trait]
impl MessageQueueAdapter for MessageQueueAdapterStdImpl {
    #[instrument(skip(self), fields(?split_merge_actions))]
    async fn update_shards(&self, split_merge_actions: Vec<SplitMergeAction>) -> Result<()> {
        tracing::info!(target: tracing_targets::MQ_ADAPTER, "Updated shards in message queue");
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
                SplitMergeAction::Merge(shard_id_1, shard_id_2) => {
                    self.queue.merge_shards(&shard_id_1, &shard_id_2).await?;
                }
                SplitMergeAction::Add(new_shard) => {
                    self.queue.add_shard(&new_shard).await?;
                }
            }
        }
        Ok(())
    }

    #[instrument(skip(self), fields(%for_shard_id, ?shards_from, ?shards_to))]
    async fn create_iterator(
        &self,
        for_shard_id: ShardIdent,
        shards_from: FastHashMap<ShardIdent, u64>,
        shards_to: FastHashMap<ShardIdent, u64>,
    ) -> Result<Box<dyn QueueIterator>> {
        tracing::info!(
            target: tracing_targets::MQ_ADAPTER,
            "Creating iterator"
        );
        let snapshots = self.queue.snapshot().await;

        let iterator = QueueIteratorImpl::new(shards_from, shards_to, snapshots, for_shard_id)?;
        Ok(Box::new(iterator))
    }

    async fn apply_diff(&self, diff: Arc<QueueDiff>, block_id_short: BlockIdShort) -> Result<()> {
        tracing::info!(
            target: tracing_targets::MQ_ADAPTER,
            id = ?block_id_short,
            new_messages_len = diff.messages.len(),
            processed_upto_len = ?diff.processed_upto,
            "Applying diff to the queue"
        );
        self.queue.apply_diff(diff, block_id_short).await?;
        Ok(())
    }

    #[instrument(skip(self), fields(%diff_id))]
    async fn commit_diff(&self, diff_id: &BlockIdShort) -> Result<Option<Arc<QueueDiff>>> {
        tracing::info!(
            target: tracing_targets::MQ_ADAPTER,
            "Committing diff to the queue"
        );
        let diff = self.queue.commit_diff(diff_id).await?;
        Ok(diff)
    }

    fn add_messages_to_iterator(
        &self,
        iterator: &mut Box<dyn QueueIterator>,
        messages: Vec<(MsgInfo, Cell)>,
    ) -> Result<()> {
        tracing::info!(
            target: tracing_targets::MQ_ADAPTER,
            messages_len = messages.len(),
            "Adding messages to the iterator"
        );
        for (msg_info, cell) in messages {
            let int_msg_info = match msg_info {
                MsgInfo::Int(int_msg_info) => int_msg_info,
                _ => bail!("Only internal messages are supported"),
            };
            let message = Arc::new(EnqueuedMessage::from((int_msg_info, cell)));
            iterator.add_message(message)?;
        }
        Ok(())
    }

    fn commit_messages_to_iterator(
        &self,
        iterator: &mut Box<dyn QueueIterator>,
        messages: Vec<(ShardIdent, InternalMessageKey)>,
    ) -> Result<()> {
        tracing::info!(
            target: tracing_targets::MQ_ADAPTER,
            messages_len = messages.len(),
            "Committing messages to the iterator"
        );
        iterator.commit(messages)
    }
}
