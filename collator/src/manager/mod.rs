use std::collections::btree_map::{self};
use std::collections::{hash_map, BTreeMap, HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use async_trait::async_trait;
use everscale_crypto::ed25519::KeyPair;
use everscale_types::models::{BlockId, BlockIdShort, Lazy, OutMsgDescr, ShardIdent};
use parking_lot::{Mutex, RwLock};
use tokio::sync::Notify;
use tracing::Instrument;
use tycho_block_util::block::ValidatorSubsetInfo;
use tycho_block_util::queue::{QueueDiffStuff, QueueKey};
use tycho_block_util::state::ShardStateStuff;
use tycho_util::metrics::HistogramGuard;
use tycho_util::{FastDashMap, FastHashMap, FastHashSet};
use types::{
    ActiveCollator, AppliedBlockStuffContainer, BlockCacheEntryKind, BlockCacheStoreResult,
    BlockSeqno, CollatorState, DisplayBlockCacheStoreResult,
};

use self::types::{
    BlockCacheEntry, BlockCacheEntryStuff, BlockCacheKey, BlocksCache, ChainTimesSyncState,
    McBlockSubgraph, McBlockSubgraphExtract, SendSyncStatus,
};
use self::utils::find_us_in_collators_set;
use crate::collator::{
    CollationCancelReason, Collator, CollatorContext, CollatorEventListener, CollatorFactory,
};
use crate::internal_queue::types::{EnqueuedMessage, QueueDiffWithMessages};
use crate::mempool::{MempoolAdapter, MempoolAdapterFactory, MempoolAnchor, MempoolEventListener};
use crate::queue_adapter::MessageQueueAdapter;
use crate::state_node::{StateNodeAdapter, StateNodeAdapterFactory, StateNodeEventListener};
use crate::types::{
    BlockCandidate, BlockCollationResult, BlockIdExt, CollationConfig, CollationSessionId,
    CollationSessionInfo, DisplayFullBlockIdsSlice, McData, ShardDescriptionExt,
    TopBlockDescription,
};
use crate::utils::async_dispatcher::{AsyncDispatcher, STANDARD_ASYNC_DISPATCHER_BUFFER_SIZE};
use crate::utils::schedule_async_action;
use crate::utils::shard::calc_split_merge_actions;
use crate::validator::{AddSession, ValidationStatus, Validator};
use crate::{method_to_async_closure, tracing_targets};

mod types;
mod utils;

pub struct RunningCollationManager<CF, V>
where
    CF: CollatorFactory,
{
    dispatcher: AsyncDispatcher<CollationManager<CF, V>>,
    state_node_adapter: Arc<dyn StateNodeAdapter>,
    mpool_adapter: Arc<dyn MempoolAdapter>,
    mq_adapter: Arc<dyn MessageQueueAdapter<EnqueuedMessage>>,
}

impl<CF: CollatorFactory, V> RunningCollationManager<CF, V> {
    pub fn dispatcher(&self) -> &AsyncDispatcher<CollationManager<CF, V>> {
        &self.dispatcher
    }

    pub fn state_node_adapter(&self) -> &Arc<dyn StateNodeAdapter> {
        &self.state_node_adapter
    }

    pub fn mpool_adapter(&self) -> &Arc<dyn MempoolAdapter> {
        &self.mpool_adapter
    }

    pub fn mq_adapter(&self) -> &Arc<dyn MessageQueueAdapter<EnqueuedMessage>> {
        &self.mq_adapter
    }
}

pub struct CollationManager<CF, V>
where
    CF: CollatorFactory,
{
    keypair: Arc<KeyPair>,
    config: Arc<CollationConfig>,

    dispatcher: Arc<AsyncDispatcher<Self>>,
    state_node_adapter: Arc<dyn StateNodeAdapter>,
    mpool_adapter: Arc<dyn MempoolAdapter>,
    mq_adapter: Arc<dyn MessageQueueAdapter<EnqueuedMessage>>,

    collator_factory: CF,
    validator: Arc<V>,

    active_collation_sessions: RwLock<FastHashMap<ShardIdent, Arc<CollationSessionInfo>>>,
    collation_sessions_to_finish: FastDashMap<CollationSessionId, Arc<CollationSessionInfo>>,
    active_collators: FastDashMap<ShardIdent, ActiveCollator<Arc<CF::Collator>>>,
    collators_to_stop: FastDashMap<CollationSessionId, ActiveCollator<Arc<CF::Collator>>>,

    blocks_cache: BlocksCache,
    ready_to_sync: Arc<Notify>,
    /// id of last master block processed to refresh collation sessions
    last_processed_mc_block_id: Mutex<Option<BlockId>>,

    chain_times_sync_state: Mutex<ChainTimesSyncState>,

    #[cfg(any(test, feature = "test"))]
    test_validators_keypairs: Vec<Arc<KeyPair>>,
}

#[async_trait]
impl<CF, V> MempoolEventListener for AsyncDispatcher<CollationManager<CF, V>>
where
    CF: CollatorFactory,
    V: Validator,
{
    async fn on_new_anchor(&self, anchor: Arc<MempoolAnchor>) -> Result<()> {
        self.spawn_task(method_to_async_closure!(
            process_new_anchor_from_mempool,
            anchor
        ))
        .await
    }
}

#[async_trait]
impl<CF, V> StateNodeEventListener for AsyncDispatcher<CollationManager<CF, V>>
where
    CF: CollatorFactory,
    V: Validator,
{
    async fn on_block_accepted(&self, state: &ShardStateStuff) -> Result<()> {
        let state_cloned = state.clone();
        self.spawn_task(method_to_async_closure!(
            detect_top_processed_to_anchor_and_notify_mempool,
            state_cloned
        ))
        .await?;

        // TODO: remove accepted block from cache
        // STUB: do nothing, currently we remove block from cache when it sent to state node

        Ok(())
    }

    async fn on_block_accepted_external(&self, state: &ShardStateStuff) -> Result<()> {
        // TODO: should use received block info to cancel and prevent it collation

        let state_cloned = state.clone();
        self.spawn_task(method_to_async_closure!(
            detect_top_processed_to_anchor_and_notify_mempool,
            state_cloned
        ))
        .await?;

        let state_cloned = state.clone();
        self.enqueue_task(method_to_async_closure!(
            process_block_from_bc,
            state_cloned
        ))
        .await?;

        Ok(())
    }
}

#[async_trait]
impl<CF, V> CollatorEventListener for AsyncDispatcher<CollationManager<CF, V>>
where
    CF: CollatorFactory,
    V: Validator,
{
    async fn on_skipped_anchor(
        &self,
        prev_mc_block_id: BlockId,
        next_block_id_short: BlockIdShort,
        anchor_chain_time: u64,
        force_mc_block: bool,
    ) -> Result<()> {
        self.spawn_task(method_to_async_closure!(
            process_skipped_anchor,
            prev_mc_block_id,
            next_block_id_short,
            anchor_chain_time,
            force_mc_block
        ))
        .await
    }

    async fn on_cancelled(
        &self,
        prev_mc_block_id: BlockId,
        next_block_id_short: BlockIdShort,
        cancel_reason: CollationCancelReason,
    ) -> Result<()> {
        self.spawn_task(method_to_async_closure!(
            handle_collation_cancelled,
            prev_mc_block_id,
            next_block_id_short,
            cancel_reason
        ))
        .await
    }

    async fn on_block_candidate(&self, collation_result: BlockCollationResult) -> Result<()> {
        self.spawn_task(method_to_async_closure!(
            process_collated_block_candidate,
            collation_result
        ))
        .await
    }

    async fn on_collator_stopped(&self, stop_key: CollationSessionId) -> Result<()> {
        self.spawn_task(method_to_async_closure!(process_collator_stopped, stop_key))
            .await
    }
}

impl<CF, V> CollationManager<CF, V>
where
    CF: CollatorFactory,
    V: Validator,
{
    #[allow(clippy::too_many_arguments)]
    pub fn start<STF, MPF>(
        keypair: Arc<KeyPair>,
        config: CollationConfig,
        mq_adapter: Arc<dyn MessageQueueAdapter<EnqueuedMessage>>,
        state_node_adapter_factory: STF,
        mpool_adapter_factory: MPF,
        validator: V,
        collator_factory: CF,
        #[cfg(any(test, feature = "test"))] test_validators_keypairs: Vec<Arc<KeyPair>>,
    ) -> RunningCollationManager<CF, V>
    where
        STF: StateNodeAdapterFactory,
        MPF: MempoolAdapterFactory,
    {
        tracing::info!(target: tracing_targets::COLLATION_MANAGER, "Creating collation manager...");

        // create dispatcher for own tasks
        let (dispatcher, tasks_receiver) = AsyncDispatcher::new(
            "collation_manager_async_dispatcher",
            STANDARD_ASYNC_DISPATCHER_BUFFER_SIZE,
        );
        let arc_dispatcher = Arc::new(dispatcher.clone());

        // create state node adapter
        let state_node_adapter =
            Arc::new(state_node_adapter_factory.create(arc_dispatcher.clone()));

        // create mempool adapter
        let mpool_adapter = mpool_adapter_factory.create(arc_dispatcher.clone());

        let validator = Arc::new(validator);

        let ready_to_sync = Arc::new(Notify::new());
        ready_to_sync.notify_one();

        let processor = Self {
            keypair,
            config: Arc::new(config),
            dispatcher: arc_dispatcher.clone(),
            state_node_adapter: state_node_adapter.clone(),
            mpool_adapter: mpool_adapter.clone(),
            mq_adapter: mq_adapter.clone(),
            collator_factory,
            validator,

            active_collation_sessions: Default::default(),
            collation_sessions_to_finish: Default::default(),
            active_collators: Default::default(),
            collators_to_stop: Default::default(),

            blocks_cache: BlocksCache::default(),
            ready_to_sync,
            last_processed_mc_block_id: Default::default(),

            chain_times_sync_state: Default::default(),

            #[cfg(any(test, feature = "test"))]
            test_validators_keypairs,
        };
        arc_dispatcher.run(Arc::new(processor), tasks_receiver);
        tracing::trace!(target: tracing_targets::COLLATION_MANAGER, "Tasks dispatchers started");

        // start other async processes

        // TODO: Move outside of the start method?

        // schedule to check collation sessions and force refresh
        // if not initialized (when started from zerostate)
        schedule_async_action(
            tokio::time::Duration::from_secs(10),
            || async move {
                arc_dispatcher
                    .spawn_task(method_to_async_closure!(check_refresh_collation_sessions,))
                    .await
            },
            "CollationProcessor::check_refresh_collation_sessions()".into(),
        );

        tracing::info!(target: tracing_targets::COLLATION_MANAGER, "Action scheduled in 10s: CollationProcessor::check_refresh_collation_sessions()");
        tracing::info!(target: tracing_targets::COLLATION_MANAGER, "Collation manager created");

        RunningCollationManager {
            dispatcher,
            state_node_adapter,
            mpool_adapter,
            mq_adapter,
        }
    }

    fn get_last_processed_mc_block_id(&self) -> Option<BlockId> {
        *self.last_processed_mc_block_id.lock()
    }

    /// (TODO) Check sync status between mempool and blockchain state
    /// and pause collation when we are far behind other nodesб
    /// jusct sync blcoks from blockchain
    pub async fn process_new_anchor_from_mempool(&self, _anchor: Arc<MempoolAnchor>) -> Result<()> {
        // TODO: make real implementation, currently does nothing
        Ok(())
    }

    /// Tries to determine top anchor that was processed to
    /// by info from received state and notify mempool
    #[tracing::instrument(skip_all, fields(block_id = %state.block_id().as_short_id()))]
    async fn detect_top_processed_to_anchor_and_notify_mempool(
        &self,
        state: ShardStateStuff,
    ) -> Result<()> {
        let block_id = *state.block_id();

        // will make this only for master blocks
        if !block_id.is_masterchain() {
            return Ok(());
        }

        let mut min_top_anchor_id = 0;
        let mut mc_processed_to_anchor_id = None;
        if let Some(externals_processed_upto) = state.state().processed_upto.load()?.externals {
            // get top processed to anchor id for master block
            mc_processed_to_anchor_id = Some(externals_processed_upto.processed_to.0);
            min_top_anchor_id = externals_processed_upto.processed_to.0;

            // read from shard descriptions to get min
            for item in state.shards()?.iter() {
                let (_, shard_descr) = item?;
                if shard_descr.top_sc_block_updated {
                    min_top_anchor_id =
                        min_top_anchor_id.min(shard_descr.ext_processed_to_anchor_id);
                }
            }
        }

        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            mc_processed_to_anchor_id,
            "detected min_top_anchor_id={}, will notify mempool",
            min_top_anchor_id,
        );

        self.mpool_adapter
            .handle_top_processed_to_anchor(min_top_anchor_id)
            .await?;

        Ok(())
    }

    /// Tries to determine top anchor that was processed to
    /// by info from received state and notify mempool
    #[tracing::instrument(skip_all, fields(block_id = %state.block_id().as_short_id()))]
    async fn commit_block_queue_diff(&self, state: ShardStateStuff) -> Result<()> {
        let block_short_id = state.block_id().as_short_id();

        if !block_short_id.shard.is_masterchain() {
            return Ok(());
        }

        let histogram =
            HistogramGuard::begin("tycho_collator_send_blocks_to_sync_commit_diffs_time");

        let mut top_blocks = vec![];

        for item in state.shards()?.iter() {
            let (shard_ident, shard_descr) = item?;
            top_blocks.push((
                BlockIdShort::from((shard_ident, shard_descr.seqno)),
                shard_descr.top_sc_block_updated,
            ));
        }

        top_blocks.push((block_short_id, true));

        if let Err(err) = self.mq_adapter.commit_diff(top_blocks).await {
            bail!(
                "Block ({}) sync: error committing message queue diff: {:?}",
                block_short_id,
                err
            );
        }

        histogram.finish();

        tracing::debug!(
            target: tracing_targets::COLLATION_MANAGER,
            "Block ({}) sync: message queue diff was committed",
            block_short_id,
        );

        Ok(())
    }

    async fn apply_block_queue_diff_from_entry_stuff(
        mq_adapter: Arc<dyn MessageQueueAdapter<EnqueuedMessage>>,
        block_entry_stuff: &BlockCacheEntryStuff,
    ) -> Result<()> {
        if block_entry_stuff.key.seqno == 0 {
            return Ok(());
        }
        let (queue_diff_stuff, out_msgs) = block_entry_stuff.queue_diff_and_msgs()?;
        let queue_diff_with_msgs =
            QueueDiffWithMessages::from_queue_diff(queue_diff_stuff, &out_msgs.load()?)?;
        mq_adapter
            .apply_diff(
                queue_diff_with_msgs,
                queue_diff_stuff.block_id().as_short_id(),
                queue_diff_stuff.diff_hash(),
            )
            .await
    }

    /// Process new block from blockchain:
    /// 1. Save block to cache with status Synced
    /// 2. Stop block validation if needed
    /// 3. Notify mempool about new master block
    /// 4. Refresh collation sessions according to master block
    #[tracing::instrument(skip_all, fields(block_id = %state.block_id().as_short_id()))]
    pub async fn process_block_from_bc(&self, state: ShardStateStuff) -> Result<()> {
        let block_id = *state.block_id();

        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            "Start processing block from bc",
        );

        if block_id.is_masterchain() {
            self.ready_to_sync.notified().await;
        }

        let Some(store_res) = self.store_block_from_bc(state.clone()).await? else {
            self.ready_to_sync.notify_one();
            return Ok(());
        };

        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            "Saved block from bc to cache: {}",
            DisplayBlockCacheStoreResult(&store_res),
        );

        if block_id.is_masterchain() {
            // when received block is master

            // check if should sync to last applied mc block right now
            let should_sync_to_last_applied_mc_block = {
                let last_processed_mc_block_id_opt = self.get_last_processed_mc_block_id();
                if store_res.last_collated_mc_block_id.is_some()
                    || last_processed_mc_block_id_opt.is_some()
                {
                    // should wait for next collated mc block when collators are active
                    // but when all were cancelled we can process last received mc block
                    let all_cancelled = self
                        .active_collators
                        .iter()
                        .all(|ac| ac.state == CollatorState::Cancelled);
                    if all_cancelled {
                        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                            last_collated_mc_block_id = ?store_res.last_collated_mc_block_id.map(|id| id.as_short_id().to_string()),
                            last_processed_mc_block_id = ?last_processed_mc_block_id_opt.map(|id| id.as_short_id().to_string()),
                            "check_should_sync: should sync to last applied mc block \
                            when all collators were cancelled",
                        );
                        true
                    } else {
                        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                            last_collated_mc_block_id = ?store_res.last_collated_mc_block_id.map(|id| id.as_short_id().to_string()),
                            last_processed_mc_block_id = ?last_processed_mc_block_id_opt.map(|id| id.as_short_id().to_string()),
                            "check_should_sync: should wait for next collated own mc block \
                            because collation is active",
                        );
                        false
                    }
                } else {
                    // sync to last applied mc block when no last processed and collator not started
                    tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                        "should sync to last applied mc block when no last processed",
                    );
                    true
                }
            };

            if should_sync_to_last_applied_mc_block {
                if !self
                    .sync_to_applied_mc_block(store_res.applied_mc_queue_range.unwrap())
                    .await?
                {
                    tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                        "unable to sync to last applied mc block, need to receive next blocks from bc",
                    );
                }
                self.ready_to_sync.notify_one();
            } else {
                self.ready_to_sync.notify_one();
                // stop validation if block was collated first
                if store_res.kind == BlockCacheEntryKind::CollatedAndReceived {
                    self.validator.cancel_validation(&block_id.as_short_id())?;

                    // TODO: here master block subgraph could be already extracted,
                    //      sent to sync, and removed from cache, because validation task
                    //      could be finished after `store_block_from_bc` before this point.

                    // so block is valid - we can run post validation routines
                    self.commit_valid_master_block(&block_id).await?;
                }
            }
        }

        Ok(())
    }

    #[tracing::instrument(skip_all, fields(applied_range = ?applied_range))]
    async fn sync_to_applied_mc_block(
        &self,
        applied_range: (BlockSeqno, BlockSeqno),
    ) -> Result<bool> {
        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            "Start sync to applied mc block",
        );

        let first_applied_mc_block_key = BlockIdShort {
            shard: ShardIdent::MASTERCHAIN,
            seqno: applied_range.0,
        };
        let last_applied_mc_block_key = BlockIdShort {
            shard: ShardIdent::MASTERCHAIN,
            seqno: applied_range.1,
        };

        // get min internals processed upto
        let min_processed_to_by_shards = Self::read_min_processed_to_for_mc_block(
            self.state_node_adapter.clone(),
            &self.blocks_cache,
            &last_applied_mc_block_key,
        )
        .await?;

        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            min_processed_to_by_shards = ?min_processed_to_by_shards,
        );

        // find first applied mc block and tail shard blocks and get previous
        let before_tail_block_ids = Self::read_before_tail_ids_of_mc_block(
            &self.blocks_cache,
            &first_applied_mc_block_key,
        )?;

        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            tail_block_ids = ?before_tail_block_ids,
        );

        // try load required previous queue diffs
        let mut prev_queue_diffs = vec![];
        for (shard_id, min_processed_to) in min_processed_to_by_shards {
            let Some((_, prev_block_ids)) = before_tail_block_ids.get(&shard_id) else {
                continue;
            };
            let mut prev_block_ids: VecDeque<_> = prev_block_ids.iter().cloned().collect();

            while let Some(prev_block_id) = prev_block_ids.pop_front() {
                if prev_block_id.seqno == 0 {
                    continue;
                }

                let Some(queue_diff_stuff) =
                    self.state_node_adapter.load_diff(&prev_block_id).await?
                else {
                    tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                        prev_block_id = %prev_block_id,
                        "unable to load prev diff to sync queue state, cancel sync",
                    );
                    return Ok(false);
                };
                let diff_required = queue_diff_stuff.as_ref().max_message > min_processed_to;
                tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                    diff_block_id = %prev_block_id.as_short_id(),
                    diff_required,
                    max_message = %queue_diff_stuff.as_ref().max_message,
                    min_processed_to = %min_processed_to,
                    "check if diff required to restore queue working state on sync:",
                );
                if diff_required {
                    let block_stuff = self
                        .state_node_adapter
                        .load_block(&prev_block_id)
                        .await?
                        .unwrap();
                    let out_msgs = block_stuff
                        .block()
                        .load_extra()?
                        .out_msg_description
                        .load()?;

                    let queue_diff_with_messages =
                        QueueDiffWithMessages::from_queue_diff(&queue_diff_stuff, &out_msgs)?;
                    prev_queue_diffs.push((
                        queue_diff_with_messages,
                        *queue_diff_stuff.diff_hash(),
                        prev_block_id,
                    ));

                    let prev_ids_info = block_stuff.construct_prev_id()?;
                    prev_block_ids.push_back(prev_ids_info.0);
                    if let Some(id) = prev_ids_info.1 {
                        prev_block_ids.push_back(id);
                    }
                }
            }
        }

        // apply required previous queue diffs
        while let Some((diff, diff_hash, block_id)) = prev_queue_diffs.pop() {
            self.mq_adapter
                .apply_diff(diff, block_id.as_short_id(), &diff_hash)
                .await?;
        }

        // sync all applied blocks
        // and refresh collation session by the last one
        // with re-init of collators state
        loop {
            // pop first applied mc block and sync
            // actually we can sync more mc blocks than known in applied_range
            // because we can receive new blocks from bc during sync
            let (mc_block_subgraph_extract, is_last) =
                self.pop_front_applied_mc_block_subgraph(applied_range.0)?;

            let subgraph = match mc_block_subgraph_extract {
                McBlockSubgraphExtract::Extracted(subgraph) => subgraph,
                other => bail!("mc block subgraph extract result cannot be {}", other),
            };

            // apply queue diffs
            for sc_block_entry_stuff in subgraph.shard_blocks.iter() {
                Self::apply_block_queue_diff_from_entry_stuff(
                    self.mq_adapter.clone(),
                    sc_block_entry_stuff,
                )
                .await?;
            }
            let mc_block_entry_stuff = subgraph.master_block.as_ref().unwrap();
            Self::apply_block_queue_diff_from_entry_stuff(
                self.mq_adapter.clone(),
                mc_block_entry_stuff,
            )
            .await?;

            // if it is last one then commit diffs, notify mempool and refresh collation sessions
            if is_last {
                tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                    "Will notify mempool and refresh collation sessions with HARD RESET",
                );

                let state = mc_block_entry_stuff.state()?;

                Self::notify_mempool_about_mc_block(self.mpool_adapter.clone(), state.block_id())
                    .await?;

                self.commit_block_queue_diff(state.clone()).await?;

                // HACK: do not need to set master block latest chain time from zerostate when using mempool stub
                //      because anchors from stub have older chain time than in zerostate and it will brake collation
                if state.block_id().seqno != 0 {
                    self.renew_mc_block_latest_chain_time(state.get_gen_chain_time());
                }

                // TODO: refactor this logic
                // replace last collated block id with last applied
                {
                    let mut guard = self.blocks_cache.masters.lock();
                    guard.last_collated_mc_block_id = Some(*state.block_id());
                }

                let mc_data = McData::load_from_state(state)?;
                self.refresh_collation_sessions(mc_data, true).await?;

                // remove all previous blocks from cache
                let mut to_block_keys = vec![state.block_id().as_short_id()];
                for item in state.shards()?.latest_blocks() {
                    to_block_keys.push(item?.as_short_id());
                }
                self.remove_prev_blocks_from_cache(&to_block_keys);
            }

            // TODO: remove this when pop will remove from cache on extract
            // clean up blocks from cache
            self.cleanup_blocks_from_cache(
                subgraph.shard_blocks.iter().map(|sb| sb.key).collect(),
            )?;
            self.cleanup_blocks_from_cache(vec![mc_block_entry_stuff.key])?;

            if is_last {
                break;
            }
        }

        Ok(true)
    }

    fn check_should_process_and_update_last_processed_mc_block(&self, block_id: &BlockId) -> bool {
        let mut guard = self.last_processed_mc_block_id.lock();
        let last_processed_mc_block_id_opt = guard.as_ref();
        let (seqno_delta, is_equal) =
            Self::compare_mc_block_with(block_id, last_processed_mc_block_id_opt);
        if seqno_delta < 0 || is_equal {
            false
        } else {
            guard.replace(*block_id);
            true
        }
    }

    /// Returns: (seqno delta from other, true - if equal).
    /// If `other_mc_block_id_opt` is none, returns : (0, false)
    fn compare_mc_block_with(
        mc_block_id: &BlockId,
        other_mc_block_id_opt: Option<&BlockId>,
    ) -> (i32, bool) {
        let (seqno_delta, is_equal) = match other_mc_block_id_opt {
            None => {
                tracing::debug!(
                    target: tracing_targets::COLLATION_MANAGER,
                    "other mc block is None: current {} other ({:?}): is_equal = false, seqno_delta = 0",
                    mc_block_id.as_short_id(),
                    other_mc_block_id_opt.map(|b| b.as_short_id()),
                );
                (0, false)
            }
            Some(other_mc_block_id) => (
                mc_block_id.seqno as i32 - other_mc_block_id.seqno as i32,
                mc_block_id == other_mc_block_id,
            ),
        };
        if seqno_delta < 0 || is_equal {
            tracing::debug!(
                target: tracing_targets::COLLATION_MANAGER,
                "mc block is NOT AHEAD of other: current {} other ({:?}): is_equal = {}, seqno_delta = {}",
                mc_block_id.as_short_id(),
                other_mc_block_id_opt.map(|b| b.as_short_id()),
                is_equal, seqno_delta,
            );
        }
        (seqno_delta, is_equal)
    }

    /// Check if collation sessions initialized and try to force refresh them if they not.
    /// This needed when start from zerostate. State node adapter will be initialized after
    /// zerostate load and won't fire `[StateNodeListener::on_mc_block_event()]` for the 1 block.
    /// Also when whole network was restarted then nobody will produce next master block and we need
    /// to start collation sessions based on the actual state
    pub async fn check_refresh_collation_sessions(&self) -> Result<()> {
        // the sessions list is not enpty so the collation process was already started from
        // actual state or incoming master block from blockchain
        if !self.active_collation_sessions.read().is_empty() {
            tracing::info!(target: tracing_targets::COLLATION_MANAGER, "Collation sessions already activated");
            return Ok(());
        }

        // here we will wait for last applied master block then process it
        tracing::info!(
            target: tracing_targets::COLLATION_MANAGER,
            "Requesting last applied mc block to activate collation sessions...",
        );
        let last_mc_block_id = self
            .state_node_adapter
            .load_last_applied_mc_block_id()
            .await?;

        let span = tracing::span!(
            tracing::Level::TRACE,
            "check_refresh_collation_session",
            last_mc_block_id = %last_mc_block_id.as_short_id(),
        );
        async {
            tracing::info!(
                target: tracing_targets::COLLATION_MANAGER,
                "Running processing last mc block to activate collation sessions...",
            );

            let state = self
                .state_node_adapter
                .load_state(&last_mc_block_id)
                .await?;

            let mc_data = McData::load_from_state(&state)?;

            self.detect_top_processed_to_anchor_and_notify_mempool(state)
                .await?;

            self.refresh_collation_sessions(mc_data, false).await
        }
        .instrument(span)
        .await
    }

    /// Get shards info from the master state,
    /// then start missing sessions for these shards, or refresh existing.
    /// For each shard run collation process if current node is included in collators subset.
    #[tracing::instrument(skip_all)]
    pub async fn refresh_collation_sessions(
        &self,
        mc_data: Arc<McData>,
        reset_collators: bool,
    ) -> Result<()> {
        tracing::debug!(
            target: tracing_targets::COLLATION_MANAGER,
            "Trying to refresh collation sessions by mc stat ({})...",
            mc_data.block_id.as_short_id(),
        );

        let _histogram = HistogramGuard::begin("tycho_collator_refresh_collation_sessions_time");

        // TODO: Possibly we have already updated collation sessions for this master block,
        //      because we may have collated it by ourselves before receiving it from the blockchain
        //      or because we have received it from the blockchain before we collated it
        //
        //      It may be a situation when we have received new master block from the blockchain
        //      before we have collated it by ourselves, we can stop current block collations,
        //      update working state in active collators and then continue to collate.
        //      But this can produce a significant overhead for the little bit slower node
        //      because some 2/3f+1 nodes will always be little bit faster.
        //      So we should reset active collators only when master block from the blockchain is
        //      notably ahead of last collated by ourselves
        //
        //      So we will:
        //      1. Check if we should process master block from the blockchain in `process_block_from_bc`
        //      2. Skip refreshing sessions if this master was processed by any chance

        // do not re-process this master block if it is lower then last processed or equal to it
        // but process a new version of block with the same seqno
        if !self.check_should_process_and_update_last_processed_mc_block(&mc_data.block_id) {
            return Ok(());
        }

        tracing::trace!(target: tracing_targets::COLLATION_MANAGER, "mc_data: {:?}", mc_data);

        // get new shards info from updated master state
        let mut new_shards_info = HashMap::new();
        new_shards_info.insert(ShardIdent::MASTERCHAIN, vec![mc_data.block_id]);
        for shard in mc_data.shards.iter() {
            let (shard_id, descr) = shard?;
            let top_block_id = descr.get_block_id(shard_id);
            // TODO: consider split and merge
            new_shards_info.insert(shard_id, vec![top_block_id]);
        }

        // update shards in msgs queue
        let active_shards_ids: Vec<_> = self
            .active_collation_sessions
            .read()
            .keys()
            .cloned()
            .collect();
        let new_shards_ids: Vec<&ShardIdent> = new_shards_info.keys().collect();
        tracing::debug!(
            target: tracing_targets::COLLATION_MANAGER,
            "Detecting split/merge actions to move from current shards {:?} to new shards {:?}...",
            active_shards_ids.as_slice(),
            new_shards_ids
        );

        let split_merge_actions = calc_split_merge_actions(&active_shards_ids, new_shards_ids)?;
        if !split_merge_actions.is_empty() {
            tracing::info!(
                target: tracing_targets::COLLATION_MANAGER,
                "Detected split/merge actions: {:?}",
                split_merge_actions,
            );
            // self.mq_adapter.update_shards(split_merge_actions).await?;
        }

        // find out the actual collation session seqno from master state
        let new_session_seqno = mc_data.validator_info.catchain_seqno;

        // we need full validators set to define the subset for each session and to check if current node should collate
        let full_validators_set = mc_data.config.get_current_validator_set()?;
        tracing::trace!(target: tracing_targets::COLLATION_MANAGER, "full_validators_set {:?}", full_validators_set);

        // compare with active sessions and detect new sessions to start and outdated sessions to finish
        let mut sessions_to_keep = HashMap::new();
        let mut sessions_to_start = vec![];
        let mut to_finish_sessions = HashMap::new();
        let mut to_stop_collators = HashMap::new();
        {
            let mut active_collation_sessions_guard = self.active_collation_sessions.write();
            let mut missed_shards_ids: FastHashSet<_> = active_shards_ids.into_iter().collect();
            for shard_info in new_shards_info {
                missed_shards_ids.remove(&shard_info.0);
                match active_collation_sessions_guard.entry(shard_info.0) {
                    hash_map::Entry::Occupied(entry) => {
                        let existing_session = entry.get().clone();
                        if existing_session.seqno() >= new_session_seqno {
                            sessions_to_keep.insert(shard_info.0, (existing_session, shard_info.1));
                        } else {
                            to_finish_sessions
                                .insert((shard_info.0, new_session_seqno), existing_session);
                            sessions_to_start.push(shard_info);
                            entry.remove();
                        }
                    }
                    hash_map::Entry::Vacant(_) => {
                        sessions_to_start.push(shard_info);
                    }
                }
            }

            // if we still have some active sessions that do not match with new shards
            // then we need to finish them and stop their collators
            for shard_id in missed_shards_ids {
                // TODO: we should remove session from active and add to finished in one atomic operation
                //      to not to miss session on processing block candidate that could be from old session
                if let Some(current_active_session) =
                    active_collation_sessions_guard.remove(&shard_id)
                {
                    to_finish_sessions
                        .insert((shard_id, new_session_seqno), current_active_session);
                    if let Some(collator) = self.active_collators.remove(&shard_id) {
                        to_stop_collators.insert((shard_id, new_session_seqno), collator.1);
                    }
                }
            }
        }

        tracing::debug!(
            target: tracing_targets::COLLATION_MANAGER,
            "Will keep existing collation sessions: {:?}",
            sessions_to_keep.keys(),
        );
        if !sessions_to_start.is_empty() {
            tracing::info!(
                target: tracing_targets::COLLATION_MANAGER,
                "Will start new collation sessions: {:?}",
                sessions_to_start.iter().map(|(k, _)| k).collect::<Vec<_>>(),
            );
        }

        let cc_config = mc_data.config.get_catchain_config()?;

        // update master state in existing collators and resume collation
        for (shard_id, (_, prev_blocks_ids)) in sessions_to_keep {
            // if there is no active collator then current node does not collate this shard
            // so we do not need to do anything
            let collator = {
                let Some(mut active_collator) = self.active_collators.get_mut(&shard_id) else {
                    continue;
                };
                active_collator.state = CollatorState::Active;
                active_collator.collator.clone()
            };

            tracing::debug!(
                target: tracing_targets::COLLATION_MANAGER,
                "Resuming collation attempts in {}",
                shard_id,
            );
            collator
                .enqueue_resume_collation(mc_data.clone(), reset_collators, prev_blocks_ids)
                .await?;
        }

        // we may have sessions to finish, collators to stop, and sessions to start
        // additionally we may have some active collators
        // for each new session we should check if current node should collate,
        // then stop collators if should not, otherwise start missing collators
        for (shard_id, prev_blocks_ids) in sessions_to_start {
            let (subset, hash_short) = full_validators_set
                .compute_subset(shard_id, &cc_config, new_session_seqno)
                .ok_or(anyhow!(
                    "Error calculating subset of collators for the session (shard_id = {}, seqno = {})",
                    shard_id,
                    new_session_seqno,
                ))?;

            // TEST: override with test subset with test keypairs defined on test run
            #[cfg(feature = "test")]
            let subset = if self.test_validators_keypairs.is_empty() {
                subset
            } else {
                let mut test_subset = vec![];
                for (i, keypair) in self.test_validators_keypairs.iter().enumerate() {
                    let val_descr = &subset[i];
                    test_subset.push(everscale_types::models::ValidatorDescription {
                        public_key: keypair.public_key.to_bytes().into(),
                        adnl_addr: val_descr.adnl_addr,
                        weight: val_descr.weight,
                        mc_seqno_since: val_descr.mc_seqno_since,
                        prev_total_weight: val_descr.prev_total_weight,
                    });
                }
                test_subset
            };
            #[cfg(feature = "test")]
            tracing::warn!(
                target: tracing_targets::COLLATION_MANAGER,
                "FOR TEST: overrided subset of validators to collate shard {}: {:?}",
                shard_id,
                subset,
            );

            let local_pubkey_opt = find_us_in_collators_set(&self.keypair, &subset);

            let new_session_info = Arc::new(CollationSessionInfo::new(
                shard_id.workchain(),
                new_session_seqno,
                ValidatorSubsetInfo {
                    validators: subset,
                    short_hash: hash_short,
                },
                Some(self.keypair.clone()),
            ));

            if let Some(_local_pubkey) = local_pubkey_opt {
                let prev_seqno = prev_blocks_ids.iter().map(|b| b.seqno).max().unwrap_or(0);

                if !self.active_collators.contains_key(&shard_id) {
                    tracing::info!(
                        target: tracing_targets::COLLATION_MANAGER,
                        "There is no active collator for collation session {}. Will start it",
                        shard_id,
                    );
                    let collator = self
                        .collator_factory
                        .start(CollatorContext {
                            mq_adapter: self.mq_adapter.clone(),
                            mpool_adapter: self.mpool_adapter.clone(),
                            state_node_adapter: self.state_node_adapter.clone(),
                            config: self.config.clone(),
                            collation_session: new_session_info.clone(),
                            listener: self.dispatcher.clone(),
                            shard_id,
                            prev_blocks_ids,
                            mc_data: mc_data.clone(),
                        })
                        .await;

                    self.active_collators.insert(shard_id, ActiveCollator {
                        collator: Arc::new(collator),
                        state: CollatorState::Active,
                    });
                }

                // notify validator, it will start overlay initialization

                let session_id = new_session_info.seqno();

                // need to add session only for masterchain blocks, shard block are not being validated
                if shard_id.is_masterchain() {
                    self.validator.add_session(AddSession {
                        shard_ident: shard_id,
                        session_id,
                        start_block_seqno: prev_seqno + 1,
                        validators: &new_session_info.collators().validators,
                    })?;
                }
            } else {
                tracing::info!(
                    target: tracing_targets::COLLATION_MANAGER,
                    "Node was not athorized to collate shard {}",
                    shard_id,
                );
                if let Some((_, active_collator)) = self.active_collators.remove(&shard_id) {
                    to_stop_collators.insert((shard_id, new_session_seqno), active_collator);
                }
            }

            // TODO: possibly do not need to store collation sessions if we do not collate in them
            self.active_collation_sessions
                .write()
                .insert(shard_id, new_session_info);
        }

        if !to_finish_sessions.is_empty() {
            tracing::info!(
                target: tracing_targets::COLLATION_MANAGER,
                "Will finish outdated collation sessions: {:?}",
                to_finish_sessions.keys(),
            );
        }

        // enqueue outdated sessions finish tasks
        for (finish_key, session_info) in to_finish_sessions {
            self.collation_sessions_to_finish
                .insert(finish_key, session_info.clone());
            self.finish_collation_session(session_info, finish_key)
                .await?;
        }

        if !to_stop_collators.is_empty() {
            tracing::info!(
                target: tracing_targets::COLLATION_MANAGER,
                "Will stop collators for sessions that we do not serve: {:?}",
                to_stop_collators.keys(),
            );
        }

        // enqueue dangling collators stop tasks
        for (stop_key, active_collator) in to_stop_collators {
            active_collator.collator.enqueue_stop(stop_key).await?;
            self.collators_to_stop.insert(stop_key, active_collator);
        }

        Ok(())

        // finally we will have initialized `active_collation_sessions` and `active_collators`
        // which run async block collations processes
    }

    /// Execute collation session finalization routines
    pub async fn finish_collation_session(
        &self,
        _session_info: Arc<CollationSessionInfo>,
        finish_key: CollationSessionId,
    ) -> Result<()> {
        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            "finish_collation_session: {:?}", finish_key,
        );
        self.collation_sessions_to_finish.remove(&finish_key);
        Ok(())
    }

    /// Remove stopped collator from cache
    pub async fn process_collator_stopped(&self, stop_key: CollationSessionId) -> Result<()> {
        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            "process_collator_stopped: {:?}", stop_key,
        );
        self.collators_to_stop.remove(&stop_key);
        Ok(())
    }

    /// Process collated block candidate
    /// 1. Save block to cache
    /// 2. Spawn block validation
    /// 3. Check if the master block interval elapsed (according to chain time) and schedule collation
    /// 4. If master block then update last master block chain time
    /// 5. Notify mempool about new master block (it may perform gc or nodes rotation)
    /// 6. Execute master block processing routines
    #[tracing::instrument(
        skip_all,
        fields(
            block_id = %collation_result.candidate.block.id().as_short_id(),
            ct = collation_result.candidate.chain_time,
        ),
    )]
    pub async fn process_collated_block_candidate(
        &self,
        collation_result: BlockCollationResult,
    ) -> Result<()> {
        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            "Start processing block candidate",
        );

        let _histogram =
            HistogramGuard::begin("tycho_collator_process_collated_block_candidate_time");

        let block_id = *collation_result.candidate.block.id();

        debug_assert_eq!(
            block_id.is_masterchain(),
            collation_result.mc_data.is_some(),
        );

        if block_id.is_masterchain() {
            self.ready_to_sync.notified().await;
        }

        // find session related to this block by shard
        let Some(session_info) = self
            .active_collation_sessions
            .read()
            .get(&block_id.shard)
            .cloned()
        else {
            anyhow::bail!(
                "There is no active collation session for the shard that block belongs to"
            );
        };

        let candidate_chain_time = collation_result.candidate.chain_time;

        let store_res =
            self.store_candidate(collation_result.candidate, collation_result.mc_data.clone())?;

        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            "Saved block candidate to cache: {}",
            DisplayBlockCacheStoreResult(&store_res),
        );

        if block_id.is_masterchain() {
            // when candidate is master

            // run validation or commit block
            if store_res.kind == BlockCacheEntryKind::ReceivedAndCollated {
                self.commit_valid_master_block(&block_id).await?;
            } else {
                let validator = self.validator.clone();
                let session_seqno = session_info.seqno();
                let dispatcher = self.dispatcher.clone();
                tokio::spawn(async move {
                    // TODO: Fail collation instead of panicking?
                    let status = validator.validate(session_seqno, &block_id).await.unwrap();

                    _ = dispatcher
                        .spawn_task(method_to_async_closure!(
                            process_validated_master_block,
                            block_id,
                            status
                        ))
                        .await;
                });

                tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                    "Block candidate validation spawned",
                );
            }

            // check if should sync to last applied mc block instead of processing last collated
            let last_collated_mc_block_id = store_res.last_collated_mc_block_id.unwrap();
            let should_sync_to_last_applied_mc_block = if let Some(applied_range) =
                store_res.applied_mc_queue_range
            {
                let applied_range_start_delta = applied_range.0 - last_collated_mc_block_id.seqno;
                let applied_range_end_delta = applied_range.1 - last_collated_mc_block_id.seqno;
                if applied_range_start_delta > 1 {
                    // should collate next own mc block because first applied is not next
                    tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                        "check_should_sync: should collate next own mc block: \
                        first applied ({}) ahead last collated ({}) on {} > 1",
                        applied_range.0, last_collated_mc_block_id.seqno,
                        applied_range_start_delta,
                    );
                    false
                } else if applied_range_end_delta < 3 {
                    // should collate next own mc block because last applied is not far ahead
                    tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                        "check_should_sync: should collate next own mc block: \
                        last applied ({}) ahead last collated ({}) on {} < 3",
                        applied_range.1, last_collated_mc_block_id.seqno,
                        applied_range_end_delta,
                    );
                    false
                } else {
                    // should sync to last applied mc block from bc because it is far ahead
                    tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                        "check_should_sync: should sync to last applied mc block from bc: \
                        last applied ({}) ahead last collated ({}) on {} >= 3",
                        applied_range.1, last_collated_mc_block_id.seqno,
                        applied_range_end_delta,
                    );
                    true
                }
            } else {
                // should collate next own mc block because no applied ahead
                tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                    "check_should_sync: should collate next own mc block after because nothing applied ahead",
                );
                false
            };

            if should_sync_to_last_applied_mc_block {
                // INFO: last collated mc block subgraph is already committed here

                let applied_range = store_res.applied_mc_queue_range.unwrap();
                let last_applied_mc_block_id_short = BlockIdShort {
                    shard: ShardIdent::MASTERCHAIN,
                    seqno: applied_range.1,
                };
                if !self.sync_to_applied_mc_block(applied_range).await? {
                    tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                        last_applied_mc_block_id = %last_applied_mc_block_id_short,
                        "unable to sync to last applied mc block, need to receive next blocks from bc",
                    );
                }
                self.ready_to_sync.notify_one();
            } else {
                self.ready_to_sync.notify_one();
                // save mc block latest chain time
                self.renew_mc_block_latest_chain_time(candidate_chain_time);

                // enqueue next master block collation if there are pending internals
                if collation_result.has_pending_internals {
                    tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                        "There are pending messages in master queue, enqueue next collation",
                    );
                    self.enqueue_mc_block_collation(
                        collation_result.prev_mc_block_id.get_next_id_short(),
                        candidate_chain_time,
                        Some(block_id),
                    )
                    .await?;
                } else {
                    // otherwise execute master block processing routines
                    tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                        "Will notify mempool and refresh collation sessions",
                    );

                    Self::notify_mempool_about_mc_block(self.mpool_adapter.clone(), &block_id)
                        .await?;

                    self.refresh_collation_sessions(collation_result.mc_data.unwrap(), false)
                        .await?;
                }
            }
        } else {
            // when candidate is shard

            // chek if master block interval elapsed and it needs to collate new master block
            tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                "Will check if master block interval elapsed by chain time from shard block candidate",
            );
            self.collate_mc_block_by_interval_or_continue_shard_collation(
                &collation_result.prev_mc_block_id,
                block_id.shard,
                candidate_chain_time,
                false,
                Some(block_id),
            )
            .await?;
        }

        Ok(())
    }

    /// Send master state related to master block to mempool (it may perform gc or nodes rotation)
    async fn notify_mempool_about_mc_block(
        mpool_adapter: Arc<dyn MempoolAdapter>,
        mc_block_id: &BlockId,
    ) -> Result<()> {
        // TODO: in current implementation CollationProcessor should not notify mempool
        //      about one master block more than once, but better to handle repeated request here or at mempool
        mpool_adapter.on_new_mc_state(mc_block_id).await
    }

    #[tracing::instrument(skip_all, fields(next_block_id = %next_block_id_short))]
    async fn handle_collation_cancelled(
        &self,
        _prev_mc_block_id: BlockId,
        next_block_id_short: BlockIdShort,
        cancel_reason: CollationCancelReason,
    ) -> Result<()> {
        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            ?cancel_reason,
            "Start handle collation cancelled",
        );
        match cancel_reason {
            CollationCancelReason::AnchorNotFound(_)
            | CollationCancelReason::NextAnchorNotFound(_) => {
                // mark collator as cancelled
                if let Some(mut active_collator) =
                    self.active_collators.get_mut(&next_block_id_short.shard)
                {
                    active_collator.state = CollatorState::Cancelled;
                }

                // run sync if all collators cancelled
                self.ready_to_sync.notified().await;

                let all_cancelled = self
                    .active_collators
                    .iter()
                    .all(|ac| ac.state == CollatorState::Cancelled);
                if all_cancelled {
                    tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                        "Collator cancelled in every shard, will run sync to last applied mc block",
                    );
                    // get info about applied mc blocks in cache
                    let applied_mc_queue_range_opt = {
                        let master_cache = self.blocks_cache.masters.lock();
                        master_cache.applied_mc_queue_range
                    };

                    // run sync if has applied mc blocks
                    if let Some(applied_range) = applied_mc_queue_range_opt {
                        if !self.sync_to_applied_mc_block(applied_range).await? {
                            tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                                "unable to sync to last applied mc block, need to receive next blocks from bc",
                            );
                        }
                    } else {
                        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                            "there is no received applied mc blocks in cache, will wait for next blocks from bc",
                        );
                    }
                }

                self.ready_to_sync.notify_one();
            }
        }
        Ok(())
    }

    #[tracing::instrument(skip_all, fields(next_block_id = %next_block_id_short, ct = anchor_chain_time, force_mc_block))]
    async fn process_skipped_anchor(
        &self,
        prev_mc_block_id: BlockId,
        next_block_id_short: BlockIdShort,
        anchor_chain_time: u64,
        force_mc_block: bool,
    ) -> Result<()> {
        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            "Will check if should collate next master block",
        );
        self.collate_mc_block_by_interval_or_continue_shard_collation(
            &prev_mc_block_id,
            next_block_id_short.shard,
            anchor_chain_time,
            force_mc_block,
            None,
        )
        .await
    }

    /// 1. Check if should collate master
    /// 2. And schedule master block collation
    /// 3. Or schedule next collation attempt in current shard
    async fn collate_mc_block_by_interval_or_continue_shard_collation(
        &self,
        prev_mc_block_id: &BlockId,
        shard_id: ShardIdent,
        chain_time: u64,
        force_mc_block: bool,
        trigger_shard_block_id_opt: Option<BlockId>,
    ) -> Result<()> {
        // if should collate master block due to current shard chain time
        // then stop current shard collation
        let (should_collate_mc_block, next_mc_block_chain_time_opt) = self
            .update_last_collated_chain_time_and_check_should_collate_mc_block(
                shard_id,
                chain_time,
                force_mc_block,
            );
        if should_collate_mc_block {
            // and if chain time elapsed master block interval in every shard
            // then run master block collation
            if let Some(next_mc_block_chain_time) = next_mc_block_chain_time_opt {
                self.enqueue_mc_block_collation(
                    prev_mc_block_id.get_next_id_short(),
                    next_mc_block_chain_time,
                    trigger_shard_block_id_opt,
                )
                .await?;
            }
        } else {
            // if should not collate master block
            // then continue collation by running `try_collate` that will
            // - in masterchain: just import next anchor
            // - in workchain: run next attempt to collate shard block
            self.enqueue_try_collate(&shard_id).await?;
        }
        Ok(())
    }

    /// Set master block lates chain time to calc next interval for master block collation.
    /// Prune all previous cached chain times by shards
    fn renew_mc_block_latest_chain_time(&self, chain_time: u64) {
        let mut chain_times_guard = self.chain_times_sync_state.lock();

        if chain_times_guard.mc_block_latest_chain_time < chain_time {
            chain_times_guard.mc_block_latest_chain_time = chain_time;
        }

        // prune
        for (_, last_collated_chain_times) in chain_times_guard
            .last_collated_chain_times_by_shards
            .iter_mut()
        {
            last_collated_chain_times.retain(|(ct, _)| ct > &chain_time);
        }
    }

    /// 1. Store last collated chain time by shards
    /// 2. Check if should collate master block in current shard (by interval or "force" flag)
    /// 3. And if should in every shard, then return chain time for the next master block collation
    ///
    /// Returns: (`should_collate_mc_block`, `next_mc_block_chain_time`)
    /// * `next_mc_block_chain_time.is_some()` when master collation condition met in every shard
    fn update_last_collated_chain_time_and_check_should_collate_mc_block(
        &self,
        shard_id: ShardIdent,
        chain_time: u64,
        force_mc_block: bool,
    ) -> (bool, Option<u64>) {
        // Idea is to store collated chain times and "force" flags for each shard.
        // Then we can collate master block if interval elapsed or have "force" flag in every shards.
        // We should take the max of first chain times that meet master block collation condition from each shard.

        let _histogram = HistogramGuard::begin(
            "tycho_collator_update_last_collated_chain_time_and_check_should_collate_mc_block_time",
        );

        let mut chain_times_guard = self.chain_times_sync_state.lock();
        let last_collated_chain_times = chain_times_guard
            .last_collated_chain_times_by_shards
            .entry(shard_id)
            .or_default();
        last_collated_chain_times.push((chain_time, force_mc_block));

        let mc_block_min_interval_ms = self.config.mc_block_min_interval.as_millis() as u64;

        // check if should collate master in current shard
        let should_collate_mc_block = if force_mc_block {
            tracing::info!(
                target: tracing_targets::COLLATION_MANAGER,
                "Master collation forced in current shard {}",
                shard_id,
            );
            true
        } else {
            // check if master block interval elapsed in current shard
            let chain_time_elapsed = chain_time
                .checked_sub(chain_times_guard.mc_block_latest_chain_time)
                .unwrap_or_default();
            let mc_block_interval_elapsed = chain_time_elapsed > mc_block_min_interval_ms;
            if mc_block_interval_elapsed {
                tracing::debug!(
                    target: tracing_targets::COLLATION_MANAGER,
                    "Master block interval is {}ms, elapsed chain time {}ms exceeded the interval in current shard {}",
                    mc_block_min_interval_ms, chain_time_elapsed, shard_id,
                );
            } else {
                tracing::debug!(
                    target: tracing_targets::COLLATION_MANAGER,
                    "Elapsed chain time {}ms has not elapsed master block interval {}ms in current shard \
                    - do not need to collate next master block",
                    chain_time_elapsed, mc_block_min_interval_ms,
                );
            }
            mc_block_interval_elapsed
        };

        if should_collate_mc_block {
            // if master block should be collated in every shard
            let mut first_elapsed_chain_times = vec![];
            let mut should_collate_in_every_shard = true;
            let active_shards = self
                .active_collation_sessions
                .read()
                .keys()
                .cloned()
                .collect::<Vec<_>>();
            for active_shard in active_shards {
                if let Some(last_collated_chain_times) = chain_times_guard
                    .last_collated_chain_times_by_shards
                    .get(&active_shard)
                {
                    if let Some((chain_time_that_elapsed, _)) = last_collated_chain_times
                        .iter()
                        .find(|(chain_time, force)| {
                            *force
                                || (*chain_time)
                                    .checked_sub(chain_times_guard.mc_block_latest_chain_time)
                                    .unwrap_or_default()
                                    > mc_block_min_interval_ms
                        })
                    {
                        first_elapsed_chain_times.push(*chain_time_that_elapsed);
                    } else {
                        // we have collated chain times in active shard
                        // but master block should not be collated according to them
                        should_collate_in_every_shard = false;
                        break;
                    }
                } else {
                    // we do not have collated chain times in active shard
                    // master block should not be collated
                    should_collate_in_every_shard = false;
                    break;
                }
            }
            if should_collate_in_every_shard {
                let max_first_chain_time_that_elapsed = first_elapsed_chain_times
                    .into_iter()
                    .max()
                    .expect("Here `first_elapsed_chain_times` vec should not be empty");
                tracing::info!(
                    target: tracing_targets::COLLATION_MANAGER,
                    "Master block collation forced or interval {}ms elapsed in every shard - \
                    will collate next master block with chain time {}ms",
                    mc_block_min_interval_ms, max_first_chain_time_that_elapsed,
                );
                chain_times_guard.mc_block_latest_chain_time = max_first_chain_time_that_elapsed;
                return (
                    should_collate_mc_block,
                    Some(max_first_chain_time_that_elapsed),
                );
            }
        }

        (should_collate_mc_block, None)
    }

    /// Find top shard blocks in cache for the next master block collation
    fn get_top_shard_blocks_info_for_mc_block(
        &self,
        next_mc_block_id_short: BlockIdShort,
        _next_mc_block_chain_time: u64,
        _trigger_shard_block_id_opt: Option<BlockId>,
    ) -> Result<Vec<TopBlockDescription>> {
        let mut result = vec![];
        for mut shard_cache in self.blocks_cache.shards.iter_mut() {
            for (_, entry) in shard_cache.blocks.iter().rev() {
                if (entry.containing_mc_block.is_none()
                    || entry.containing_mc_block == Some(next_mc_block_id_short))
                    && matches!(
                        entry.kind,
                        BlockCacheEntryKind::Collated
                            | BlockCacheEntryKind::CollatedAndReceived
                            | BlockCacheEntryKind::ReceivedAndCollated
                    )
                {
                    let candidate_stuff = entry.candidate_stuff()?;
                    result.push(TopBlockDescription {
                        block_id: *entry.block_id(),
                        block_info: candidate_stuff.candidate.block.load_info()?,
                        ext_processed_to_anchor_id: candidate_stuff
                            .candidate
                            .ext_processed_upto_anchor_id,
                        value_flow: std::mem::take(&mut shard_cache.value_flow),
                        proof_funds: std::mem::take(&mut shard_cache.proof_funds),
                        creators: std::mem::take(&mut shard_cache.creators),
                    });
                    break;
                }
            }
        }

        Ok(result)
    }

    /// Enqueue master block collation task. Will determine top shard blocks for this collation
    async fn enqueue_mc_block_collation(
        &self,
        next_mc_block_id_short: BlockIdShort,
        next_mc_block_chain_time: u64,
        trigger_block_id_opt: Option<BlockId>,
    ) -> Result<()> {
        // TODO: make real implementation

        let _histogram = HistogramGuard::begin("tycho_collator_enqueue_mc_block_collation_time");

        // get masterchain collator if exists
        let Some(mc_collator) = self
            .active_collators
            .get(&ShardIdent::MASTERCHAIN)
            .map(|r| r.collator.clone())
        else {
            bail!("Masterchain collator is not started yet!");
        };

        // TODO: How to choose top shard blocks for master block collation when they are collated async and in parallel?
        //      We know the last anchor (An) used in shard (ShA) block that causes master block collation,
        //      so we search for block from other shard (ShB) that includes the same anchor (An).
        //      Or the first from previouses (An-x) that includes externals for that shard (ShB)
        //      if all next including required one ([An-x+1, An]) do not contain externals for shard (ShB).

        let top_shard_blocks_info = self.get_top_shard_blocks_info_for_mc_block(
            next_mc_block_id_short,
            next_mc_block_chain_time,
            trigger_block_id_opt,
        )?;

        mc_collator
            .enqueue_do_collate(top_shard_blocks_info)
            .await?;

        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            "Master block collation enqueued: (block_id={} ct={})",
            next_mc_block_id_short,
            next_mc_block_chain_time,
        );

        Ok(())
    }

    async fn enqueue_try_collate(&self, shard_id: &ShardIdent) -> Result<()> {
        // get collator if exists
        let Some(collator) = self
            .active_collators
            .get(shard_id)
            .map(|r| r.collator.clone())
        else {
            tracing::warn!(
                target: tracing_targets::COLLATION_MANAGER,
                "Node does not collate blocks for shard {}",
                shard_id,
            );
            return Ok(());
        };

        collator.enqueue_try_collate().await?;

        tracing::debug!(
            target: tracing_targets::COLLATION_MANAGER,
            "Equeued next attempt to collate block for shard {}",
            shard_id,
        );

        Ok(())
    }

    /// Process validated block
    /// 1. Process invalid block (currently, just panic)
    /// 2. Update block in cache with validation info
    /// 2. Execute processing for master or shard block
    #[tracing::instrument(skip_all, fields(block_id = %block_id.as_short_id()))]
    pub async fn process_validated_master_block(
        &self,
        block_id: BlockId,
        status: ValidationStatus,
    ) -> Result<()> {
        tracing::debug!(
            target: tracing_targets::COLLATION_MANAGER,
            is_complete = matches!(&status, ValidationStatus::Complete(_)),
            "Start processing block validation result...",
        );

        let _histogram = HistogramGuard::begin("tycho_collator_process_validated_block_time");

        // execute required actions if block invalid

        tracing::debug!(
            target: tracing_targets::COLLATION_MANAGER,
            "Saving block validation result to cache...",
        );
        // update block in cache with signatures info
        let updated = self.store_master_block_validation_result(block_id, status);
        if !updated {
            tracing::debug!(
                target: tracing_targets::COLLATION_MANAGER,
                "Block does not exist in cache - skip validation result",
            );
            return Ok(());
        }

        // process valid block
        self.commit_valid_master_block(&block_id).await?;

        Ok(())
    }

    /// Store block candidate in a cache
    #[tracing::instrument(skip_all)]
    fn store_candidate(
        &self,
        candidate: Box<BlockCandidate>,
        mc_data: Option<Arc<McData>>,
    ) -> Result<BlockCacheStoreResult> {
        let block_id = *candidate.block.id();

        let mut entry = BlockCacheEntry::from_candidate(candidate, mc_data)?;

        let result = if block_id.shard.is_masterchain() {
            // store master block to cache
            let mut guard = self.blocks_cache.masters.lock();
            let stored = match guard.blocks.entry(block_id.seqno) {
                btree_map::Entry::Occupied(mut occupied) => {
                    let existing = occupied.get_mut();

                    tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                        kind = ?existing.kind,
                        send_sync_status = ?existing.send_sync_status,
                        is_valid = existing.is_valid,
                        candidate_stuff.is_none = existing.candidate_stuff.is_none(),
                        "blocks_cache contains master block"
                    );

                    assert_eq!(
                        existing.block_id().root_hash,
                        entry.block_id().root_hash,
                        "Block received from bc root hash mismatch with collated one"
                    );
                    // TODO: check block_id file hash ?

                    if existing.send_sync_status == SendSyncStatus::Synced {
                        assert_eq!(existing.kind, BlockCacheEntryKind::Received);

                        existing.candidate_stuff = entry.candidate_stuff;
                        existing.kind = BlockCacheEntryKind::ReceivedAndCollated;
                        (existing.kind, existing.send_sync_status, VecDeque::new())
                    } else {
                        bail!(
                            "Should not collate the same master block again! ({})",
                            block_id,
                        );
                    }
                }
                btree_map::Entry::Vacant(vacant) => {
                    let prev_shard_blocks_ids = entry
                        .top_shard_blocks_ids_iter()
                        .cloned()
                        .collect::<VecDeque<_>>();

                    let inserted = vacant.insert(entry);
                    (
                        inserted.kind,
                        inserted.send_sync_status,
                        prev_shard_blocks_ids,
                    )
                }
            };

            // update last collated mc block id
            guard.last_collated_mc_block_id = Some(block_id);

            // update applied mc queue range info
            if let Some((range_start, range_end)) = guard.applied_mc_queue_range {
                if block_id.seqno >= range_start {
                    let new_range_start = block_id.seqno + 1;
                    if new_range_start > range_end {
                        guard.applied_mc_queue_range = None;
                    } else {
                        guard.applied_mc_queue_range = Some((new_range_start, range_end));
                    }
                }
            }

            let result = BlockCacheStoreResult {
                block_id,
                kind: stored.0,
                send_sync_status: stored.1,
                last_collated_mc_block_id: guard.last_collated_mc_block_id,
                applied_mc_queue_range: guard.applied_mc_queue_range,
            };
            drop(guard); // TODO: use scope instead

            if result.kind == BlockCacheEntryKind::Collated {
                // traverse through including shard blocks and update their link to the containing master block
                self.set_containing_mc_block(block_id.as_short_id(), stored.2);
            }

            result
        } else {
            // store shard block to cache
            let mut shard_cache = self.blocks_cache.shards.entry(block_id.shard).or_default();
            let (stored, aggregate) = match shard_cache.blocks.entry(block_id.seqno) {
                btree_map::Entry::Occupied(mut occupied) => {
                    let existing = occupied.get_mut();

                    tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                        kind = ?existing.kind,
                        send_sync_status = ?existing.send_sync_status,
                        is_valid = existing.is_valid,
                        candidate_stuff.is_none = existing.candidate_stuff.is_none(),
                        "blocks_cache contains shard block"
                    );

                    assert_eq!(
                        existing.block_id().root_hash,
                        entry.block_id().root_hash,
                        "Block received from bc root hash mismatch with collated one"
                    );
                    // TODO: check block_id file hash ?

                    if existing.send_sync_status == SendSyncStatus::Synced {
                        assert_eq!(existing.kind, BlockCacheEntryKind::Received);

                        existing.candidate_stuff = entry.candidate_stuff;
                        existing.kind = BlockCacheEntryKind::ReceivedAndCollated;
                        ((existing.kind, existing.send_sync_status), None)
                    } else {
                        bail!(
                            "Should not collate the same shard block again! ({})",
                            block_id,
                        );
                    }
                }
                btree_map::Entry::Vacant(vacant) => {
                    entry.send_sync_status = SendSyncStatus::Ready;
                    entry.is_valid = true;
                    let aggregate = entry.candidate_stuff.as_ref().map(|e| {
                        (
                            e.candidate.fees_collected.clone(),
                            e.candidate.funds_created.clone(),
                            e.candidate.created_by,
                        )
                    });
                    let inserted = vacant.insert(entry);
                    ((inserted.kind, inserted.send_sync_status), aggregate)
                }
            };

            // aggregate additional info for TopBlockDescription
            if let Some((fees_collected, funds_created, created_by)) = aggregate {
                shard_cache
                    .proof_funds
                    .fees_collected
                    .checked_add(&fees_collected)?;
                shard_cache
                    .proof_funds
                    .funds_created
                    .checked_add(&funds_created)?;
                shard_cache.creators.push(created_by);
            }
            drop(shard_cache); // TODO: use scope instead

            let mc_guard = self.blocks_cache.masters.lock();
            BlockCacheStoreResult {
                block_id,
                kind: stored.0,
                send_sync_status: stored.1,
                last_collated_mc_block_id: mc_guard.last_collated_mc_block_id,
                applied_mc_queue_range: mc_guard.applied_mc_queue_range,
            }
        };

        Ok(result)
    }

    fn set_containing_mc_block(
        &self,
        mc_block_key: BlockCacheKey,
        mut prev_shard_blocks_ids: VecDeque<BlockId>,
    ) {
        while let Some(prev_shard_block_id) = prev_shard_blocks_ids.pop_front() {
            if let Some(mut shard_cache) =
                self.blocks_cache.shards.get_mut(&prev_shard_block_id.shard)
            {
                if let Some(shard_block) = shard_cache.blocks.get_mut(&prev_shard_block_id.seqno) {
                    if shard_block.containing_mc_block.is_none() {
                        shard_block.containing_mc_block = Some(mc_block_key);
                        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                            sc_block_id = %shard_block.block_id().as_short_id(),
                            "containing_mc_block set"
                        );
                        shard_block
                            .prev_blocks_ids
                            .iter()
                            .for_each(|sub_prev| prev_shard_blocks_ids.push_back(*sub_prev));
                    }
                }
            }
        }
    }

    /// Store received from bc block in a cache
    #[tracing::instrument(skip_all)]
    async fn store_block_from_bc(
        &self,
        state: ShardStateStuff,
    ) -> Result<Option<BlockCacheStoreResult>> {
        let block_id = *state.block_id();

        // TODO: should build entry only on insert

        // load queue diff
        let (prev_block_ids, queue_diff_and_msgs) =
            Self::load_block_queue_diff_stuff(self.state_node_adapter.clone(), &block_id).await?;

        // build entry
        let entry = BlockCacheEntry::from_block_from_bc(
            state,
            prev_block_ids.clone(),
            queue_diff_and_msgs,
        )?;

        let result = if block_id.shard.is_masterchain() {
            // store master block to cache
            let mut guard = self.blocks_cache.masters.lock();

            // skip when received block is lower then last known synced one
            if let Some(last_known_synced) =
                types::check_refresh_last_known_synced(&mut guard.last_known_synced, block_id.seqno)
            {
                tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                    last_known_synced,
                    "received block is not newer than last known synced, skipped"
                );
                return Ok(None);
            }

            let stored = match guard.blocks.entry(block_id.seqno) {
                btree_map::Entry::Occupied(mut occupied) => {
                    let existing = occupied.get_mut();

                    tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                        kind = ?existing.kind,
                        send_sync_status = ?existing.send_sync_status,
                        is_valid = existing.is_valid,
                        candidate_stuff.is_none = existing.candidate_stuff.is_none(),
                        "blocks_cache contains master block"
                    );

                    assert_eq!(
                        block_id.root_hash,
                        existing.block_id().root_hash,
                        "Block received from bc root hash mismatch with collated one"
                    );
                    // TODO: check block_id file hash ?

                    match existing.send_sync_status {
                        SendSyncStatus::NotReady | SendSyncStatus::Ready => {
                            assert_eq!(existing.kind, BlockCacheEntryKind::Collated);

                            // No need to send to sync
                            existing.is_valid = true;
                            existing.send_sync_status = SendSyncStatus::Synced;
                            existing.kind = BlockCacheEntryKind::CollatedAndReceived;
                        }
                        SendSyncStatus::Sending | SendSyncStatus::Sent | SendSyncStatus::Synced => {
                            // Already syncing or synced - do nothing
                        }
                    }

                    (
                        existing.kind,
                        existing.send_sync_status,
                        VecDeque::new(),
                        vec![],
                    )
                }
                btree_map::Entry::Vacant(vacant) => {
                    let prev_shard_blocks_ids = entry
                        .top_shard_blocks_ids_iter()
                        .cloned()
                        .collect::<VecDeque<_>>();

                    let inserted = vacant.insert(entry);
                    (
                        inserted.kind,
                        inserted.send_sync_status,
                        prev_shard_blocks_ids,
                        prev_block_ids,
                    )
                }
            };

            // remove state from prev mc block because we need only last one
            for prev_block_id in stored.3 {
                if let Some(entry) = guard.blocks.get_mut(&prev_block_id.seqno) {
                    if let Some(applied_block_stuff) = entry.applied_block_stuff.as_mut() {
                        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                            prev_block_id = %prev_block_id.as_short_id(),
                            "state stuff removed from prev mc block in cache"
                        );
                        applied_block_stuff.state = None;
                    }
                }
            }

            // update applied mc queue range info
            if let Some((_, range_end)) = guard.applied_mc_queue_range.as_mut() {
                *range_end = block_id.seqno;
            } else {
                guard.applied_mc_queue_range = Some((block_id.seqno, block_id.seqno));
            }

            let result = BlockCacheStoreResult {
                block_id,
                kind: stored.0,
                send_sync_status: stored.1,
                last_collated_mc_block_id: guard.last_collated_mc_block_id,
                applied_mc_queue_range: guard.applied_mc_queue_range,
            };
            drop(guard); // TODO: use scope instead

            if result.kind == BlockCacheEntryKind::Received {
                // traverse through including shard blocks and update their link to the containing master block
                self.set_containing_mc_block(block_id.as_short_id(), stored.2);
            }

            result
        } else {
            // store shard block to cache
            let mut shard_cache = self.blocks_cache.shards.entry(block_id.shard).or_default();

            // skip when received block is lower then last known synced one
            if let Some(last_known_synced) = types::check_refresh_last_known_synced(
                &mut shard_cache.last_known_synced,
                block_id.seqno,
            ) {
                tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                    last_known_synced,
                    "received block is not newer than last known synced, skipped"
                );
                return Ok(None);
            }

            let stored = match shard_cache.blocks.entry(block_id.seqno) {
                btree_map::Entry::Occupied(mut occupied) => {
                    let existing = occupied.get_mut();

                    tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                        kind = ?existing.kind,
                        send_sync_status = ?existing.send_sync_status,
                        is_valid = existing.is_valid,
                        candidate_stuff.is_none = existing.candidate_stuff.is_none(),
                        "blocks_cache contains shard block"
                    );

                    assert_eq!(
                        block_id.root_hash,
                        existing.block_id().root_hash,
                        "Block received from bc root hash mismatch with collated one"
                    );
                    // TODO: check block_id file hash ?

                    match existing.send_sync_status {
                        SendSyncStatus::NotReady | SendSyncStatus::Ready => {
                            assert_eq!(existing.kind, BlockCacheEntryKind::Collated);

                            // No need to send to sync
                            existing.is_valid = true;
                            existing.send_sync_status = SendSyncStatus::Synced;
                            existing.kind = BlockCacheEntryKind::CollatedAndReceived;
                        }
                        SendSyncStatus::Sending | SendSyncStatus::Sent | SendSyncStatus::Synced => {
                            // Already syncing or synced - do nothing
                        }
                    }

                    (existing.kind, existing.send_sync_status)
                }
                btree_map::Entry::Vacant(vacant) => {
                    let inserted = vacant.insert(entry);
                    (inserted.kind, inserted.send_sync_status)
                }
            };

            // TODO: remove state from prev shard blocks that are not top blocks for masters

            drop(shard_cache); // TODO: use scope instead

            let mc_guard = self.blocks_cache.masters.lock();
            BlockCacheStoreResult {
                block_id,
                kind: stored.0,
                send_sync_status: stored.1,
                last_collated_mc_block_id: mc_guard.last_collated_mc_block_id,
                applied_mc_queue_range: mc_guard.applied_mc_queue_range,
            }
        };

        Ok(Some(result))
    }

    async fn load_block_queue_diff_stuff(
        state_node_adapter: Arc<dyn StateNodeAdapter>,
        block_id: &BlockId,
    ) -> Result<(Vec<BlockId>, Option<(QueueDiffStuff, Lazy<OutMsgDescr>)>)> {
        let mut prev_block_ids = vec![];

        let Some(block_stuff) = state_node_adapter.load_block(block_id).await? else {
            return Ok((prev_block_ids, None));
        };

        let lazy_out_msgs = block_stuff.block().load_extra()?.out_msg_description;
        let queue_diff_stuff = state_node_adapter.load_diff(block_id).await?.unwrap();

        let prev_ids_info = block_stuff.construct_prev_id()?;
        prev_block_ids.push(prev_ids_info.0);
        if let Some(id) = prev_ids_info.1 {
            prev_block_ids.push(id);
        }

        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            prev_block_ids = %DisplayFullBlockIdsSlice(&prev_block_ids),
            "loaded block and queue diff stuff",
        );

        Ok((prev_block_ids, Some((queue_diff_stuff, lazy_out_msgs))))
    }

    /// Find master block candidate in cache, append signatures info and return updated
    fn store_master_block_validation_result(
        &self,
        block_id: BlockId,
        validation_result: ValidationStatus,
    ) -> bool {
        let (is_valid, already_synced, signatures) = match validation_result {
            ValidationStatus::Skipped => (true, true, Default::default()),
            ValidationStatus::Complete(signatures) => (true, false, signatures),
        };

        let mut guard = self.blocks_cache.masters.lock();
        if let Some(entry) = guard.blocks.get_mut(&block_id.seqno) {
            entry.set_validation_result(is_valid, already_synced, signatures);
            return true;
        }

        false
    }

    /// Find shard block in cache and then get containing master block id if link exists
    fn _find_containing_mc_block(&self, shard_block_id: &BlockId) -> Option<(BlockId, bool)> {
        // TODO: handle when master block link exist but there is not block itself
        if let Some(shard_cache) = self.blocks_cache.shards.get(&shard_block_id.shard) {
            if let Some(mc_block_key) = shard_cache
                .value()
                .blocks
                .get(&shard_block_id.seqno)
                .and_then(|sbc| sbc.containing_mc_block)
            {
                let guard = self.blocks_cache.masters.lock();
                let res = guard
                    .blocks
                    .get(&mc_block_key.seqno)
                    .map(|block_container| (*block_container.block_id(), block_container.is_valid));
                return res;
            }
        }
        None
    }

    // Return min processed_to info from master and each shard
    async fn read_min_processed_to_for_mc_block(
        state_node_adapter: Arc<dyn StateNodeAdapter>,
        blocks_cache: &BlocksCache,
        mc_block_key: &BlockCacheKey,
    ) -> Result<BTreeMap<ShardIdent, QueueKey>> {
        let mut result = BTreeMap::new();

        if mc_block_key.seqno == 0 {
            return Ok(result);
        }

        let mut all_processed_to;
        let updated_top_shard_block_ids;
        {
            let master_cache = blocks_cache.masters.lock();
            let Some(mc_block_entry) = master_cache.blocks.get(&mc_block_key.seqno) else {
                bail!("Master block not found in cache! ({})", mc_block_key)
            };
            updated_top_shard_block_ids = mc_block_entry
                .top_shard_blocks_info
                .iter()
                .filter(|(_, updated)| *updated)
                .map(|(id, _)| id)
                .cloned()
                .collect::<Vec<_>>();
            let (queue_diff_stuff, _) = mc_block_entry.queue_diff_and_msgs()?;
            all_processed_to = vec![queue_diff_stuff.as_ref().processed_upto.clone()];
        }

        {
            for top_sc_block_id in updated_top_shard_block_ids {
                if top_sc_block_id.seqno == 0 {
                    continue;
                }

                // try to find in cache
                let mut queue_diff_stuff_opt = None;
                if let Some(shard_cache) = blocks_cache.shards.get(&top_sc_block_id.shard) {
                    if let Some(sc_block_entry) = shard_cache.blocks.get(&top_sc_block_id.seqno) {
                        let (queue_diff_stuff, _) = sc_block_entry.queue_diff_and_msgs()?;
                        queue_diff_stuff_opt = Some(queue_diff_stuff.clone());
                    }
                }
                // then try to find in storage
                let queue_diff_stuff = match queue_diff_stuff_opt {
                    Some(queue_diff_stuff) => queue_diff_stuff,
                    _ => {
                        // shard block of specified master will not exist in cache on restart
                        let (_, queue_diff_and_msgs) = Self::load_block_queue_diff_stuff(
                            state_node_adapter.clone(),
                            &top_sc_block_id,
                        )
                        .await?;
                        if let Some((queue_diff_stuff, _)) = queue_diff_and_msgs {
                            queue_diff_stuff
                        } else {
                            bail!(
                                "Shard block not found in cache and storage! ({})",
                                top_sc_block_id
                            )
                        }
                    }
                };
                all_processed_to = vec![queue_diff_stuff.as_ref().processed_upto.clone()];
            }
        }

        for item in all_processed_to {
            for (shard_id, processed_to) in item {
                result
                    .entry(shard_id)
                    .and_modify(|min| {
                        if &processed_to < min {
                            *min = processed_to;
                        }
                    })
                    .or_insert(processed_to);
            }
        }

        Ok(result)
    }

    fn read_before_tail_ids_of_mc_block(
        blocks_cache: &BlocksCache,
        mc_block_key: &BlockCacheKey,
    ) -> Result<BTreeMap<ShardIdent, (Option<BlockId>, Vec<BlockId>)>> {
        let mut result = BTreeMap::new();

        if mc_block_key.seqno == 0 {
            return Ok(result);
        }

        let mut prev_shard_blocks_ids;
        {
            let master_cache = blocks_cache.masters.lock();
            let Some(mc_block_entry) = master_cache.blocks.get(&mc_block_key.seqno) else {
                bail!("Master block not found in cache! ({})", mc_block_key)
            };

            result.insert(
                mc_block_entry.block_id().shard,
                (
                    Some(*mc_block_entry.block_id()),
                    mc_block_entry.prev_blocks_ids.clone(),
                ),
            );

            prev_shard_blocks_ids = mc_block_entry
                .top_shard_blocks_ids_iter()
                .map(|id| (*id, true))
                .collect::<VecDeque<_>>();
        }

        while let Some((prev_sc_block_id, force_include)) = prev_shard_blocks_ids.pop_front() {
            if prev_sc_block_id.seqno == 0 {
                // skip not existed shard block with seqno 0
                continue;
            }

            let mut prev_block_ids = None;
            let mut not_found = true;
            if let Some(shard_cache) = blocks_cache.shards.get(&prev_sc_block_id.shard) {
                if let Some(sc_block_entry) = shard_cache.blocks.get(&prev_sc_block_id.seqno) {
                    not_found = false;

                    // if shard block included in current master block subgraph
                    if force_include // top shard blocks consider included anyway in this case
                        || matches!(sc_block_entry.containing_mc_block, Some(key) if &key == mc_block_key)
                    {
                        prev_block_ids = Some((
                            Some(prev_sc_block_id),
                            sc_block_entry.prev_blocks_ids.clone(),
                        ));

                        sc_block_entry.prev_blocks_ids.iter().for_each(|sub_prev| {
                            prev_shard_blocks_ids.push_back((*sub_prev, false));
                        });
                    }
                }
            }

            if not_found && force_include {
                prev_block_ids = Some((None, vec![prev_sc_block_id]));
            }

            // collect min block id from subgraph for each shard
            if let Some((block_id_opt, prev_block_ids)) = prev_block_ids {
                result
                    .entry(prev_sc_block_id.shard)
                    .and_modify(|(min_id_opt, min_prev_ids)| {
                        if min_id_opt.is_none() || &block_id_opt < min_id_opt {
                            *min_id_opt = block_id_opt;
                            *min_prev_ids = prev_block_ids.clone();
                        }
                    })
                    .or_insert((block_id_opt, prev_block_ids));
            }
        }

        Ok(result)
    }

    fn pop_front_applied_mc_block_subgraph(
        &self,
        from_seqno: u32,
    ) -> Result<(McBlockSubgraphExtract, bool)> {
        let mut extracted_mc_block_entry = None;
        let mut is_last = true;
        {
            let mut guard = self.blocks_cache.masters.lock();
            let range = guard.blocks.range_mut(from_seqno..);

            for (_, mc_block_entry) in range {
                if matches!(
                    mc_block_entry.kind,
                    BlockCacheEntryKind::Received | BlockCacheEntryKind::ReceivedAndCollated
                ) {
                    if extracted_mc_block_entry.is_some() {
                        is_last = false;
                        break;
                    } else {
                        let mc_block_entry_stuff = mc_block_entry.extract_entry_stuff()?;
                        let prev_shard_blocks_ids = mc_block_entry
                            .top_shard_blocks_info
                            .iter()
                            .filter(|(_, updated)| *updated)
                            .map(|(id, _)| id)
                            .cloned()
                            .collect::<VecDeque<_>>();
                        extracted_mc_block_entry =
                            Some((mc_block_entry_stuff, prev_shard_blocks_ids));
                    }
                } else {
                    bail!(
                        "pop_front_appliend_mc_block_subgraph: should not be with kind {:?} ({})",
                        mc_block_entry.kind,
                        mc_block_entry.key(),
                    )
                }
            }
        }
        let (mc_block_entry_stuff, prev_shard_blocks_ids) = extracted_mc_block_entry.unwrap();
        let subgraph_extract = self.extract_mc_block_subgraph_internal(
            mc_block_entry_stuff,
            prev_shard_blocks_ids,
            false,
        )?;
        Ok((subgraph_extract, is_last))
    }

    /// Find all shard blocks included in master block subgraph.
    /// Then extract and return them and master block
    fn extract_mc_block_subgraph_for_sync(
        &self,
        block_id: &BlockId,
        only_if_valid: bool,
    ) -> Result<McBlockSubgraphExtract> {
        // 1. Find requested master block
        let mc_block_entry_stuff;
        let prev_shard_blocks_ids;
        {
            let mut guard = self.blocks_cache.masters.lock();

            let Some(mc_block_entry) = guard.blocks.get_mut(&block_id.seqno) else {
                return Ok(McBlockSubgraphExtract::AlreadyExtracted);
            };

            if !mc_block_entry.is_valid && only_if_valid {
                return Ok(McBlockSubgraphExtract::NotFullValid);
            }

            // 2. Extract stuff for sync
            mc_block_entry_stuff = mc_block_entry.extract_entry_stuff_for_sync()?;

            prev_shard_blocks_ids = mc_block_entry
                .top_shard_blocks_info
                .iter()
                .filter(|(_, updated)| *updated)
                .map(|(id, _)| id)
                .cloned()
                .collect::<VecDeque<_>>();

            // update last known synced block seqno in cache
            types::check_refresh_last_known_synced(&mut guard.last_known_synced, block_id.seqno);
        }

        self.extract_mc_block_subgraph_internal(mc_block_entry_stuff, prev_shard_blocks_ids, true)
    }

    fn extract_mc_block_subgraph_internal(
        &self,
        mc_block_entry_stuff: BlockCacheEntryStuff,
        mut prev_shard_blocks_ids: VecDeque<BlockId>,
        for_sync: bool,
    ) -> Result<McBlockSubgraphExtract> {
        let mc_block_key = mc_block_entry_stuff.key;

        let mut subgraph = McBlockSubgraph {
            master_block: Some(mc_block_entry_stuff),
            shard_blocks: vec![],
        };

        // 3. By the top shard blocks info find shard blocks of current master block
        // 4. Recursively find prev shard blocks until the end or top shard blocks of prev master reached
        while let Some(prev_shard_block_id) = prev_shard_blocks_ids.pop_front() {
            if prev_shard_block_id.seqno == 0 {
                // skip not existed shard block with seqno 0
                continue;
            }

            let Some(mut shard_cache) =
                self.blocks_cache.shards.get_mut(&prev_shard_block_id.shard)
            else {
                continue;
            };

            if let Some(sc_block_entry) = shard_cache.blocks.get_mut(&prev_shard_block_id.seqno) {
                let sc_block_id = *sc_block_entry.block_id();

                // if shard block included in current master block subgraph
                if matches!(sc_block_entry.containing_mc_block, Some(key) if key == mc_block_key) {
                    // 5. Extract stuff for sync
                    let sc_block_entry_stuff = if for_sync {
                        sc_block_entry.extract_entry_stuff_for_sync()?
                    } else {
                        sc_block_entry.extract_entry_stuff()?
                    };
                    subgraph.shard_blocks.push(sc_block_entry_stuff);
                    sc_block_entry
                        .prev_blocks_ids
                        .iter()
                        .for_each(|sub_prev| prev_shard_blocks_ids.push_back(*sub_prev));

                    // update last known synced block seqno in cache
                    types::check_refresh_last_known_synced(
                        &mut shard_cache.last_known_synced,
                        sc_block_id.seqno,
                    );
                }
            }
        }

        subgraph.shard_blocks.reverse();

        tracing::debug!(
            target: tracing_targets::COLLATION_MANAGER,
            "Extracted valid master block subgraph for sending to sync ({}): {:?}",
            mc_block_key,
            subgraph.shard_blocks.iter().map(|sb| sb.key.to_string())
                .collect::<Vec<_>>().as_slice(),
        );

        Ok(McBlockSubgraphExtract::Extracted(subgraph))
    }

    fn remove_prev_blocks_from_cache(&self, to_blocks_keys: &[BlockCacheKey]) {
        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            "Removing prev blocks from cache before: {}",
            DisplayFullBlockIdsSlice(to_blocks_keys),
        );
        scopeguard::defer! {
            tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                "Prev blocks removed from cache before: {}",
                DisplayFullBlockIdsSlice(to_blocks_keys),
            );
        };
        for block_key in to_blocks_keys.iter() {
            if block_key.shard.is_masterchain() {
                let mut guard = self.blocks_cache.masters.lock();
                guard.blocks.retain(|key, _| key >= &block_key.seqno);
            } else if let Some(mut shard_cache) = self.blocks_cache.shards.get_mut(&block_key.shard)
            {
                shard_cache.blocks.retain(|key, _| key >= &block_key.seqno);
            }
        }
    }

    /// Remove block entries from cache and compact cache
    fn cleanup_blocks_from_cache(&self, blocks_keys: Vec<BlockCacheKey>) -> Result<()> {
        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            "Cleaning up blocks from cache: {}",
            DisplayFullBlockIdsSlice(&blocks_keys),
        );
        scopeguard::defer! {
            tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                "Blocks cleaned up from cache: {}",
                DisplayFullBlockIdsSlice(&blocks_keys),
            );
        };
        for block_key in blocks_keys.iter() {
            if block_key.shard.is_masterchain() {
                let mut guard = self.blocks_cache.masters.lock();
                if let Some(mc_block_entry) = guard.blocks.remove(&block_key.seqno) {
                    if matches!(
                        mc_block_entry.kind,
                        BlockCacheEntryKind::Received | BlockCacheEntryKind::ReceivedAndCollated
                    ) {
                        if let Some((range_start, range_end)) = guard.applied_mc_queue_range {
                            if block_key.seqno >= range_start {
                                let new_range_start = block_key.seqno + 1;
                                if new_range_start > range_end {
                                    guard.applied_mc_queue_range = None;
                                } else {
                                    guard.applied_mc_queue_range =
                                        Some((new_range_start, range_end));
                                }
                            }
                        }
                    }
                }
            } else if let Some(mut shard_cache) = self.blocks_cache.shards.get_mut(&block_key.shard)
            {
                shard_cache.blocks.remove(&block_key.seqno);
            }
        }
        Ok(())
    }

    /// Try to commit validated and valid master block
    /// if it was not already committed before
    /// 1. Check if master block is valid
    /// 2. Extract master block subgraph with shard blocks
    /// 3. Send to sync
    /// 4. Commit queue diff
    /// 5. Clean up from cache
    async fn commit_valid_master_block(&self, block_id: &BlockId) -> Result<()> {
        tracing::debug!(
            target: tracing_targets::COLLATION_MANAGER,
            "Start to commit validated and valid master block ({})...",
            block_id.as_short_id(),
        );

        let histogram = HistogramGuard::begin("tycho_collator_process_valid_master_block_time");
        let histogram_extract =
            HistogramGuard::begin("tycho_collator_extract_master_block_subgraph_time");
        let mut extract_elapsed = Default::default();
        let mut sync_elapsed = Default::default();

        // extract master block with all shard blocks if valid, and process them
        match self.extract_mc_block_subgraph_for_sync(block_id, true)? {
            McBlockSubgraphExtract::Extracted(mut mc_block_subgraph) => {
                extract_elapsed = histogram_extract.finish();
                let timer = std::time::Instant::now();

                // send to sync only if was not received from bc
                if mc_block_subgraph.master_block.as_ref().unwrap().kind
                    == BlockCacheEntryKind::Collated
                {
                    let histogram =
                        HistogramGuard::begin("tycho_collator_send_blocks_to_sync_time");

                    let mut build_stuff_for_sync_elapsed = Duration::ZERO;
                    let mut sync_stuff_elapsed = Duration::ZERO;

                    let mut entries_to_sync = std::mem::take(&mut mc_block_subgraph.shard_blocks);
                    entries_to_sync.push(mc_block_subgraph.master_block.take().unwrap());

                    for mut entry_to_sync in entries_to_sync {
                        if !matches!(
                            entry_to_sync.send_sync_status,
                            SendSyncStatus::Sending | SendSyncStatus::Sent | SendSyncStatus::Synced,
                        ) {
                            let timer = std::time::Instant::now();
                            let candidate_stuff = entry_to_sync.candidate_stuff.take().unwrap();
                            let block_id = *candidate_stuff.candidate.block.id();
                            let block_stuff_for_sync = candidate_stuff.as_block_for_sync();
                            build_stuff_for_sync_elapsed += timer.elapsed();

                            let timer = std::time::Instant::now();
                            self.state_node_adapter
                                .accept_block(block_stuff_for_sync)
                                .await?;
                            tracing::debug!(
                                target: tracing_targets::COLLATION_MANAGER,
                                "Block was successfully sent to sync ({})",
                                block_id,
                            );
                            entry_to_sync.send_sync_status = SendSyncStatus::Sent;
                            sync_stuff_elapsed += timer.elapsed();
                        }
                        if entry_to_sync.key.shard.is_masterchain() {
                            mc_block_subgraph.master_block = Some(entry_to_sync);
                        } else {
                            mc_block_subgraph.shard_blocks.push(entry_to_sync);
                        }
                    }

                    metrics::histogram!("tycho_collator_build_block_stuff_for_sync_time")
                        .record(build_stuff_for_sync_elapsed);
                    metrics::histogram!("tycho_collator_sync_block_stuff_time")
                        .record(sync_stuff_elapsed);

                    tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                        total = histogram.finish().as_millis(),
                        build_stuff_for_sync = build_stuff_for_sync_elapsed.as_millis(),
                        sync = sync_stuff_elapsed.as_millis(),
                        "send_blocks_to_sync timings",
                    );
                }
                sync_elapsed = timer.elapsed();

                // TODO: commit queue diffs
                // self.commit_block_queue_diff(state)

                // clean up blocks from cache
                self.cleanup_blocks_from_cache(
                    mc_block_subgraph
                        .shard_blocks
                        .iter()
                        .map(|sb| sb.key)
                        .collect(),
                )?;
                self.cleanup_blocks_from_cache(vec![
                    mc_block_subgraph.master_block.as_ref().unwrap().key,
                ])?;
            }
            McBlockSubgraphExtract::NotFullValid => {
                bail!(
                    "Master block is not full valid ({}). Cannot commit",
                    block_id.as_short_id(),
                )
            }
            McBlockSubgraphExtract::AlreadyExtracted => {
                tracing::debug!(
                    target: tracing_targets::COLLATION_MANAGER,
                    "Master block subgraph is already extracted and cleaned up from cache ({}). Do nothing",
                    block_id.as_short_id(),
                );
            }
        }

        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            total = histogram.finish().as_millis(),
            extract_subgraph = extract_elapsed.as_millis(),
            sync = sync_elapsed.as_millis(),
            "commit_valid_master_block timings",
        );

        Ok(())
    }

    /// Try find master block and execute post validation routines
    async fn _commit_valid_shard_block(&self, block_id: &BlockId) -> Result<()> {
        if let Some((mc_block_id, is_valid)) = self._find_containing_mc_block(block_id) {
            if is_valid {
                tracing::debug!(
                    target: tracing_targets::COLLATION_MANAGER,
                    "Found containing master block ({}) for just validated shard block ({}) in cache",
                    mc_block_id.as_short_id(),
                    block_id.as_short_id(),
                );
                self.commit_valid_master_block(&mc_block_id).await?;
            } else {
                tracing::debug!(
                    target: tracing_targets::COLLATION_MANAGER,
                    "Containing master block ({}) for just validated shard block ({}) is not validated yet. Will wait for master block validation",
                    mc_block_id.as_short_id(),
                    block_id.as_short_id(),
                );
            }
        } else {
            tracing::debug!(
                target: tracing_targets::COLLATION_MANAGER,
                "There is no containing master block for just validated shard block ({}) in cache. Will wait for master block collation",
                block_id.as_short_id(),
            );
        }
        Ok(())
    }
}
