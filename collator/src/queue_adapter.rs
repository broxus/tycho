use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use everscale_types::cell::Cell;
use everscale_types::models::{BlockIdShort, IntMsgInfo, ShardIdent};
use tracing::instrument;
use tycho_util::FastHashMap;

use crate::internal_queue::iterator::{QueueIterator, QueueIteratorExt, QueueIteratorImpl};
use crate::internal_queue::queue::{Queue, QueueImpl};
use crate::internal_queue::state::persistent_state::PersistentStateStdImpl;
use crate::internal_queue::state::session_state::SessionStateStdImpl;
use crate::internal_queue::state::states_iterators_manager::StatesIteratorsManager;
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
        shards_from: FastHashMap<ShardIdent, InternalMessageKey>,
        shards_to: FastHashMap<ShardIdent, InternalMessageKey>,
    ) -> Result<Box<dyn QueueIterator>>;
    /// Apply diff to the current queue session state (waiting for the operation to complete)
    async fn apply_diff(&self, diff: Arc<QueueDiff>, block_id_short: BlockIdShort) -> Result<()>;
    /// Commit previously applied diff, saving changes to persistent state (waiting for the operation to complete).
    /// Return `None` if specified diff does not exist.
    async fn commit_diff(&self, diff_id: &BlockIdShort) -> Result<Arc<QueueDiff>>;
    /// Add new messages to the iterator
    fn add_message_to_iterator(
        &self,
        iterator: &mut Box<dyn QueueIterator>,
        message: (IntMsgInfo, Cell),
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
        tracing::info!(target: tracing_targets::MQ_ADAPTER, "Updating shards in message queue");
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
        shards_from: FastHashMap<ShardIdent, InternalMessageKey>,
        shards_to: FastHashMap<ShardIdent, InternalMessageKey>,
    ) -> Result<Box<dyn QueueIterator>> {
        let time_start = std::time::Instant::now();
        let ranges = QueueIteratorExt::collect_ranges(shards_from, shards_to);

        let states_iterators = self.queue.iterator(&ranges, for_shard_id).await;

        let states_iterators_manager = StatesIteratorsManager::new(states_iterators);

        let iterator = QueueIteratorImpl::new(states_iterators_manager, for_shard_id)?;
        tracing::info!(
            target: tracing_targets::MQ_ADAPTER,
            range = ?ranges,
            elapsed = %humantime::format_duration(time_start.elapsed()),
            for_shard_id = %for_shard_id,
            "Iterator created"
        );
        Ok(Box::new(iterator))
    }

    async fn apply_diff(&self, diff: Arc<QueueDiff>, block_id_short: BlockIdShort) -> Result<()> {
        let time = std::time::Instant::now();
        let len = diff.messages.len();
        self.queue.apply_diff(diff, block_id_short).await?;

        tracing::info!(
            target: tracing_targets::MQ_ADAPTER,
                        id = ?block_id_short,
                        new_messages_len = len,
                        elapsed = ?time.elapsed(),

            "Diff applied",
        );
        Ok(())
    }

    #[instrument(skip(self), fields(%diff_id))]
    async fn commit_diff(&self, diff_id: &BlockIdShort) -> Result<Arc<QueueDiff>> {
        let time = std::time::Instant::now();

        let diff = self.queue.commit_diff(diff_id).await?;
        tracing::info!(
            target: tracing_targets::MQ_ADAPTER,
            id = ?diff_id,
            elapsed = ?time.elapsed(),
            "Diff commited",
        );

        Ok(diff)
    }

    fn add_message_to_iterator(
        &self,
        iterator: &mut Box<dyn QueueIterator>,
        message: (IntMsgInfo, Cell),
    ) -> Result<()> {
        let message = Arc::new(EnqueuedMessage::from((message.0, message.1)));
        iterator.add_message(message)?;
        Ok(())
    }

    fn commit_messages_to_iterator(
        &self,
        iterator: &mut Box<dyn QueueIterator>,
        messages: Vec<(ShardIdent, InternalMessageKey)>,
    ) -> Result<()> {
        tracing::trace!(
            target: tracing_targets::MQ_ADAPTER,
            messages_len = messages.len(),
            "Committing messages to iterator"
        );
        iterator.commit(messages)
    }
}
