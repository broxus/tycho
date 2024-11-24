use anyhow::Result;
use everscale_types::cell::HashBytes;
use everscale_types::models::{BlockIdShort, ShardIdent};
use tracing::instrument;
use tycho_block_util::queue::QueueKey;
use tycho_util::FastHashMap;

use crate::internal_queue::iterator::{QueueIterator, QueueIteratorExt, QueueIteratorImpl};
use crate::internal_queue::queue::{Queue, QueueImpl};
use crate::internal_queue::state::persistent_state::PersistentStateStdImpl;
use crate::internal_queue::state::session_state::SessionStateStdImpl;
use crate::internal_queue::state::states_iterators_manager::StatesIteratorsManager;
use crate::internal_queue::types::{InternalMessageValue, QueueDiffWithMessages};
use crate::tracing_targets;
use crate::types::{DisplayIter, DisplayTuple, DisplayTupleRef};

pub struct MessageQueueAdapterStdImpl<V: InternalMessageValue> {
    queue: QueueImpl<SessionStateStdImpl, PersistentStateStdImpl, V>,
}

pub trait MessageQueueAdapter<V>: Send + Sync
where
    V: InternalMessageValue + Send + Sync,
{
    /// Create iterator for specified shard and return it
    fn create_iterator(
        &self,
        for_shard_id: ShardIdent,
        shards_from: FastHashMap<ShardIdent, QueueKey>,
        shards_to: FastHashMap<ShardIdent, QueueKey>,
    ) -> Result<Box<dyn QueueIterator<V>>>;
    /// Apply diff to the current queue session state (waiting for the operation to complete)
    fn apply_diff(
        &self,
        diff: QueueDiffWithMessages<V>,
        block_id_short: BlockIdShort,
        diff_hash: &HashBytes,
        end_key: QueueKey,
    ) -> Result<()>;

    /// Commit previously applied diff, saving changes to persistent state (waiting for the operation to complete).
    /// Return `None` if specified diff does not exist.
    fn commit_diff(&self, mc_top_blocks: Vec<(BlockIdShort, bool)>) -> Result<()>;

    /// Add new messages to the iterator
    fn add_message_to_iterator(
        &self,
        iterator: &mut Box<dyn QueueIterator<V>>,
        message: V,
    ) -> Result<()>;
    /// Commit processed messages in the iterator
    /// Save last message position for each shard
    fn commit_messages_to_iterator(
        &self,
        iterator: &mut Box<dyn QueueIterator<V>>,
        messages: Vec<(ShardIdent, QueueKey)>,
    ) -> Result<()>;

    fn clear_session_state(&self) -> Result<()>;
    /// removes all diffs from the cache that are less than `inclusive_until` which source shard is `source_shard`
    fn trim_diffs(&self, source_shard: &ShardIdent, inclusive_until: &QueueKey) -> Result<()>;

    /// returns the number of diffs in cache for the given shard
    fn get_diff_count_by_shard(&self, shard_ident: &ShardIdent) -> usize;
}

impl<V: InternalMessageValue> MessageQueueAdapterStdImpl<V> {
    pub fn new(queue: QueueImpl<SessionStateStdImpl, PersistentStateStdImpl, V>) -> Self {
        Self { queue }
    }
}

impl<V: InternalMessageValue> MessageQueueAdapter<V> for MessageQueueAdapterStdImpl<V> {
    #[instrument(skip_all, fields(%for_shard_id))]
    fn create_iterator(
        &self,
        for_shard_id: ShardIdent,
        shards_from: FastHashMap<ShardIdent, QueueKey>,
        shards_to: FastHashMap<ShardIdent, QueueKey>,
    ) -> Result<Box<dyn QueueIterator<V>>> {
        let time_start = std::time::Instant::now();
        let ranges = QueueIteratorExt::collect_ranges(shards_from, shards_to);

        let states_iterators = self.queue.iterator(&ranges, for_shard_id);

        let states_iterators_manager = StatesIteratorsManager::new(states_iterators);

        let iterator = QueueIteratorImpl::new(states_iterators_manager, for_shard_id)?;
        tracing::info!(
            target: tracing_targets::MQ_ADAPTER,
            range = %DisplayIter(ranges
                .iter()
                .map(|(k, v)| DisplayTuple((k, DisplayTupleRef(v))))
            ),
            elapsed = %humantime::format_duration(time_start.elapsed()),
            for_shard_id = %for_shard_id,
            "Iterator created"
        );
        Ok(Box::new(iterator))
    }

    #[instrument(skip_all, fields(%block_id_short))]
    fn apply_diff(
        &self,
        diff: QueueDiffWithMessages<V>,
        block_id_short: BlockIdShort,
        hash: &HashBytes,
        end_key: QueueKey,
    ) -> Result<()> {
        let time = std::time::Instant::now();
        let len = diff.messages.len();
        let processed_upto = diff.processed_upto.clone();
        self.queue.apply_diff(diff, block_id_short, hash, end_key)?;

        tracing::info!(target: tracing_targets::MQ_ADAPTER,
            new_messages_len = len,
            elapsed = ?time.elapsed(),
            processed_upto = %DisplayIter(processed_upto.iter().map(DisplayTuple)),
            "Diff applied",
        );
        Ok(())
    }

    fn commit_diff(&self, mc_top_blocks: Vec<(BlockIdShort, bool)>) -> Result<()> {
        let time = std::time::Instant::now();

        self.queue.commit_diff(&mc_top_blocks)?;

        tracing::info!(target: tracing_targets::MQ_ADAPTER,
            mc_top_blocks = %DisplayIter(mc_top_blocks.iter().map(DisplayTupleRef)),
            elapsed = ?time.elapsed(),
            "Diff committed",
        );

        Ok(())
    }

    fn add_message_to_iterator(
        &self,
        iterator: &mut Box<dyn QueueIterator<V>>,
        message: V,
    ) -> Result<()> {
        iterator.add_message(message)?;
        Ok(())
    }

    fn commit_messages_to_iterator(
        &self,
        iterator: &mut Box<dyn QueueIterator<V>>,
        messages: Vec<(ShardIdent, QueueKey)>,
    ) -> Result<()> {
        tracing::trace!(
            target: tracing_targets::MQ_ADAPTER,
            messages_len = messages.len(),
            "Committing messages to iterator"
        );
        iterator.commit(messages)
    }

    fn clear_session_state(&self) -> Result<()> {
        self.queue.clear_session_state()
    }

    fn trim_diffs(&self, source_shard: &ShardIdent, inclusive_until: &QueueKey) -> Result<()> {
        self.queue.trim_diffs(source_shard, inclusive_until)
    }

    fn get_diff_count_by_shard(&self, shard_ident: &ShardIdent) -> usize {
        self.queue.get_diffs_count_by_shard(shard_ident)
    }
}
