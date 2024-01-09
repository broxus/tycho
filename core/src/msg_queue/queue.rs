use anyhow::Result;

use super::types::ext_types_stubs::*;
use super::{cache_persistent::*, config::*, state_persistent::*, storage::*, types::*};

#[cfg(test)]
#[path = "tests/test_queue.rs"]
pub(super) mod tests;

pub trait MessageQueue<CH, ST, DB>
where
    CH: PersistentCacheService,
    ST: PersistentStateService,
    DB: StorageService,
{
    /// Create new queue with persistent cache, persistent state and storage
    /// or init from existing storage, persistent state and cache.
    /// Queue can be used only after state loading.
    fn init(config: MessageQueueConfig) -> Result<Self>
    where
        Self: Sized;

    /// Load queue state on the specified block by id or shard+`seq_no`
    /// from local persistent state and storage.
    /// When called again fully reload the state according to a new block.
    fn reload_state_on_block(&mut self, block_ident: BlockIdent) -> Result<()>;

    /// Add a new message to internal set. They will be used for creating diff.
    /// If all existing messages in the queue state are processed and the end of
    /// queue storage (EOQS) is reached, then new messages will be used for processing.
    fn add_new_message(&mut self, message: EnqueuedMessage);

    /// Remeber processed message by it's LT and HASH.
    /// Will be used for creating diff and to check if transaction updates could be committed.
    fn add_processed_upto(&mut self, lt: Lt, hash: MessageHash);

    /// Create pending diff containing remaining new messages and top processed upto marks.
    /// Diff will be stored to persistent cache.
    ///
    /// It is like moving current changes to the index in git.
    ///
    /// Future features:
    ///  1. The queue will be ready for further iteration and recording a new diff.
    ///
    /// Simplified features:
    ///  1. Further iteration and diff recording are disallowed until rollback or commit.
    fn create_diff(
        &mut self,
        block_id: BlockId,
        shard_id: ShardIdent,
        block_seq_no: SeqNo,
    ) -> Result<Option<QueueDiff>>;

    /// Save diff to the storage when `no_save == false`.
    /// Remove it from the persistent cache, and remove processed
    /// messages related to this diff from the queue state.
    ///
    /// If the end of queue storage (EOQS) is reached then add the remaining
    /// new messages related to this diff to the queue state for further processing.
    ///
    /// Future features:
    ///  1. If more than one pending diff for current shard exists, eg "diff1" and "diff2",
    /// and "diff2" is committed first, then we should hold the required changes and apply
    /// them only after "diff1" is committed.
    fn commit_diff(&mut self, diff_key: QueueDiffKey, no_save: bool) -> Result<()>;

    /// Rollback queue state changes:
    ///  1. return processed messages and move back the iterator pointer
    ///  2. clear processed upto info
    ///  3. remove diff from persistent cache
    ///  4. remove new messages
    ///  5. revert other related changes
    ///
    /// If `by_diff` is not specified then rollback all changes from the last commit,
    /// including all pending diffs.
    ///
    /// Future features:
    ///  1. When `by_diff` is specified then rollback changes related to this diff.
    /// If several pending diffs exist (eg "diff1" and "diff2") and "diff1" is specified,
    /// then we should rollback changes from both "diff1" and "diff2".
    fn rollback_changes(&mut self, by_diff: Option<QueueDiffKey>) -> Result<()>;

    /// Actions:
    ///  1. store diff to persistent cache
    ///  2. add new messages from diff to internal set
    ///  3. mark processed messages
    ///
    /// The queue should look like after the executing [`MessageQueue::create_diff`].
    ///
    /// Next the [`MessageQueue::commit_diff`] or [`MessageQueue::rollback_changes`] should be called.
    fn apply_diff(&mut self, diff: QueueDiff) -> Result<()>;

    /// Reload queue from external persistent state:
    ///  1. store submitted external persistent state
    ///  2. fill queue with messages from external persistent
    ///
    /// Should execute it first when syncing queue from other nodes.
    fn apply_persistent_state(&mut self, p_state: PersistentStateData) -> Result<()>;

    /// Get queue state data on the specified block for syncing to another node:
    ///  - persistent state  if exists
    ///  - all diffs upto specified block
    ///
    /// This state data can be loaded to another queue using [`MessageQueue::apply_persistent_state`],
    /// [`MessageQueue::apply_diff`], and [`MessageQueue::commit_diff`].
    ///
    /// **Use case:**
    /// We have an empty queue and know the last block, we want
    /// to load the queue to match the state after this last block.
    fn get_sync_state_on_block(
        &self,
        block_ident: BlockIdent,
    ) -> Result<(Option<PersistentStateData>, Vec<QueueDiff>)>;

    /// Get queue state updates for syncing from the specified block.
    ///
    /// May be used after [`MessageQueue::get_sync_state_on_block`].
    ///
    /// **Use case:**
    /// We have synced the queue to the previously specified "last block 1". While
    /// we were syncing the other nodes produced more blocks, and now we have
    /// a new "last block 12". So we need to sync fresh updates.
    ///
    /// Will return:
    ///  - new persistent state if it was created after the specified block
    ///  - all committed and pending diffs after the specified block
    fn get_sync_state_from_block(
        &self,
        block_ident: BlockIdent,
    ) -> Result<(Option<PersistentStateData>, Vec<QueueDiff>)>;
}

pub struct MessageQueueImpl<CH, ST, DB>
where
    CH: PersistentCacheService,
    ST: PersistentStateService,
    DB: StorageService,
{
    config: MessageQueueConfig,

    p_cache_service: CH,
    p_state_service: ST,
    storage_service: DB,
}

impl<CH, ST, DB> MessageQueue<CH, ST, DB> for MessageQueueImpl<CH, ST, DB>
where
    CH: PersistentCacheService,
    ST: PersistentStateService,
    DB: StorageService,
{
    fn init(config: MessageQueueConfig) -> Result<Self>
    where
        Self: Sized,
    {
        let p_cache_cfg = config.persistent_cache_config_ref();
        let p_cache_service = CH::new(p_cache_cfg)?;

        let p_state_service = ST::new()?;
        let storage_service = DB::new()?;

        Ok(Self {
            config,
            p_cache_service,
            p_state_service,
            storage_service,
        })
    }

    fn reload_state_on_block(&mut self, block_ident: BlockIdent) -> Result<()> {
        todo!()
    }

    fn add_new_message(&mut self, message: EnqueuedMessage) {
        todo!()
    }

    fn add_processed_upto(&mut self, lt: Lt, hash: MessageHash) {
        todo!()
    }

    fn create_diff(
        &mut self,
        block_id: BlockId,
        shard_id: ShardIdent,
        block_seq_no: SeqNo,
    ) -> Result<Option<QueueDiff>> {
        todo!()
    }

    fn commit_diff(&mut self, diff_key: QueueDiffKey, no_save: bool) -> Result<()> {
        todo!()
    }

    fn rollback_changes(&mut self, by_diff: Option<QueueDiffKey>) -> Result<()> {
        todo!()
    }

    fn apply_diff(&mut self, diff: QueueDiff) -> Result<()> {
        todo!()
    }

    fn apply_persistent_state(&mut self, p_state: PersistentStateData) -> Result<()> {
        todo!()
    }

    fn get_sync_state_on_block(
        &self,
        block_ident: BlockIdent,
    ) -> Result<(Option<PersistentStateData>, Vec<QueueDiff>)> {
        todo!()
    }

    fn get_sync_state_from_block(
        &self,
        block_ident: BlockIdent,
    ) -> Result<(Option<PersistentStateData>, Vec<QueueDiff>)> {
        todo!()
    }
}

/*
This part of the code contains logic that cannot be attributed specifically
to the persistent state and cache, storage, loader, diff management.

We use partials just to separate the codebase on smaller and easier maintainable parts.
 */
impl<CH, ST, DB> MessageQueueImpl<CH, ST, DB>
where
    CH: PersistentCacheService,
    ST: PersistentStateService,
    DB: StorageService,
{
    fn some_internal_method(&mut self) -> Result<()> {
        todo!()
    }
}

pub type MessageQueueImplOnStubs = MessageQueueImpl<
    PersistentCacheServiceStubImpl,
    PersistentStateServiceStubImpl,
    StorageServiceStubImpl,
>;
