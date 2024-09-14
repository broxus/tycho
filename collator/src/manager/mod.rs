use std::collections::{hash_map, BTreeMap, VecDeque};
use std::sync::Arc;

use anyhow::{anyhow, bail, Result};
use async_trait::async_trait;
use everscale_crypto::ed25519::KeyPair;
use everscale_types::models::{
    BlockId, BlockIdShort, ExternalsProcessedUpto, ProcessedUptoInfo, ShardHashes, ShardIdent,
};
use parking_lot::{Mutex, RwLock};
use tokio::sync::Notify;
use tracing::Instrument;
use tycho_block_util::block::ValidatorSubsetInfo;
use tycho_block_util::queue::QueueKey;
use tycho_block_util::state::ShardStateStuff;
use tycho_util::metrics::HistogramGuard;
use tycho_util::{FastDashMap, FastHashMap, FastHashSet};
use types::{
    ActiveCollator, BlockCacheEntry, BlockCacheEntryData, BlockSeqno, CollatorState,
    McBlockSubgraph,
};

use self::blocks_cache::BlocksCache;
use self::types::{BlockCacheKey, CandidateStatus, ChainTimesSyncState, McBlockSubgraphExtract};
use self::utils::find_us_in_collators_set;
use crate::collator::{
    CollationCancelReason, Collator, CollatorContext, CollatorEventListener, CollatorFactory,
};
use crate::internal_queue::types::{EnqueuedMessage, QueueDiffWithMessages};
use crate::mempool::{
    MempoolAdapter, MempoolAdapterFactory, MempoolAnchor, MempoolAnchorId, MempoolEventListener,
};
use crate::queue_adapter::MessageQueueAdapter;
use crate::state_node::{StateNodeAdapter, StateNodeAdapterFactory, StateNodeEventListener};
use crate::types::{
    BlockCollationResult, BlockIdExt, CollationConfig, CollationSessionId, CollationSessionInfo,
    DebugIter, DisplayAsShortId, DisplayBlockIdsIntoIter, DisplayIter, DisplayTuple2, McData,
    ShardDescriptionExt,
};
use crate::utils::async_dispatcher::{AsyncDispatcher, STANDARD_ASYNC_DISPATCHER_BUFFER_SIZE};
use crate::utils::schedule_async_action;
use crate::utils::shard::calc_split_merge_actions;
use crate::validator::{AddSession, ValidationStatus, Validator};
use crate::{method_to_async_closure, tracing_targets};

mod blocks_cache;
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

    /// Round of a new consensus genesis
    mempool_start_round: Option<u32>,
    /// Last know applied master block seqno to recover from
    from_mc_block_seqno: Option<u32>,

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
        let processed_upto = state.state().processed_upto.load()?;

        metrics_report_last_applied_block_and_anchor(state, &processed_upto);

        let state_cloned = state.clone();
        self.spawn_task(method_to_async_closure!(
            detect_top_processed_to_anchor_and_notify_mempool,
            true,
            state_cloned,
            processed_upto
        ))
        .await?;

        // TODO: remove accepted block from cache
        // STUB: do nothing, currently we remove block from cache when it sent to state node

        Ok(())
    }

    async fn on_block_accepted_external(&self, state: &ShardStateStuff) -> Result<()> {
        // TODO: should use received block info to cancel and prevent it collation

        let processed_upto = state.state().processed_upto.load()?;

        metrics_report_last_applied_block_and_anchor(state, &processed_upto);

        let state_cloned = state.clone();
        self.spawn_task(method_to_async_closure!(
            detect_top_processed_to_anchor_and_notify_mempool,
            false,
            state_cloned,
            processed_upto
        ))
        .await?;

        let state_cloned = state.clone();
        self.enqueue_task(method_to_async_closure!(handle_block_from_bc, state_cloned))
            .await?;

        Ok(())
    }
}

fn metrics_report_last_applied_block_and_anchor(
    state: &ShardStateStuff,
    processed_upto: &ProcessedUptoInfo,
) {
    let block_id = state.block_id();
    let labels = [("workchain", block_id.shard.workchain().to_string())];

    let processed_to_anchor_id = processed_upto
        .externals
        .as_ref()
        .map(|upto| upto.processed_to.0)
        .unwrap_or_default();

    metrics::gauge!("tycho_last_applied_block_seqno", &labels).set(block_id.seqno);
    metrics::gauge!("tycho_last_processed_to_anchor_id", &labels).set(processed_to_anchor_id);

    tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
        block_id = %DisplayAsShortId(block_id),
        processed_to_anchor_id = processed_to_anchor_id,
        "last applied block",
    );
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
            handle_skipped_anchor,
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
            handle_collated_block_candidate,
            collation_result
        ))
        .await
    }

    async fn on_collator_stopped(&self, stop_key: CollationSessionId) -> Result<()> {
        self.spawn_task(method_to_async_closure!(handle_collator_stopped, stop_key))
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
        mempool_start_round: Option<u32>,
        from_mc_block_seqno: Option<u32>,
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

            mempool_start_round,
            from_mc_block_seqno,

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
                    .spawn_task(method_to_async_closure!(check_and_start_collation_sessions,))
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
        on_own_collated_block: bool,
        state: ShardStateStuff,
        processed_upto: ProcessedUptoInfo,
    ) -> Result<()> {
        let block_id = *state.block_id();

        // will make this only for master blocks
        if !block_id.is_masterchain() {
            return Ok(());
        }

        let (top_processed_to_anchor_id, mc_processed_to_anchor_id) =
            Self::detect_top_processed_to_anchor(
                state.shards()?,
                processed_upto.externals.as_ref(),
            )?;

        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            mc_processed_to_anchor_id,
            "detected min_top_processed_to_anchor_id={}, will notify mempool",
            top_processed_to_anchor_id,
        );

        self.mpool_adapter
            .handle_top_processed_to_anchor(top_processed_to_anchor_id)
            .await?;

        // clean anchors cache in mempool
        if on_own_collated_block {
            self.mpool_adapter
                .clear_anchors_cache(top_processed_to_anchor_id)
                .await?;
        }

        Ok(())
    }

    /// Returns (`min_top_processed_to_anchor_id`, Option<`mc_processed_to_anchor_id`>)
    fn detect_top_processed_to_anchor(
        shards_info: &ShardHashes,
        externals_processed_upto: Option<&ExternalsProcessedUpto>,
    ) -> Result<(MempoolAnchorId, Option<MempoolAnchorId>)> {
        let mut min_top_processed_to_anchor_id = 0;
        let mut mc_processed_to_anchor_id = None;
        if let Some(upto) = externals_processed_upto {
            // get top processed to anchor id for master block
            mc_processed_to_anchor_id = Some(upto.processed_to.0);
            min_top_processed_to_anchor_id = upto.processed_to.0;

            // read from shard descriptions to get min
            for item in shards_info.iter() {
                let (_, shard_descr) = item?;
                if shard_descr.top_sc_block_updated {
                    min_top_processed_to_anchor_id =
                        min_top_processed_to_anchor_id.min(shard_descr.ext_processed_to_anchor_id);
                }
            }
        }
        Ok((min_top_processed_to_anchor_id, mc_processed_to_anchor_id))
    }

    #[tracing::instrument(skip_all, fields(block_id = %block_id))]
    async fn commit_block_queue_diff(
        &self,
        block_id: &BlockId,
        top_shard_blocks_info: &[(BlockId, bool)],
    ) -> Result<()> {
        if !block_id.is_masterchain() {
            return Ok(());
        }

        let _histogram =
            HistogramGuard::begin("tycho_collator_send_blocks_to_sync_commit_diffs_time");

        let mut top_blocks: Vec<_> = top_shard_blocks_info
            .iter()
            .map(|(id, updated)| (id.as_short_id(), *updated))
            .collect();
        top_blocks.push((block_id.as_short_id(), true));

        if let Err(err) = self.mq_adapter.commit_diff(top_blocks).await {
            bail!(
                "Error committing message queue diff of block ({}): {:?}",
                block_id,
                err,
            )
        }

        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            "message queue diff was committed",
        );

        Ok(())
    }

    async fn apply_block_queue_diff_from_entry_stuff(
        mq_adapter: Arc<dyn MessageQueueAdapter<EnqueuedMessage>>,
        block_entry: &BlockCacheEntry,
    ) -> Result<()> {
        if block_entry.block_id.seqno == 0 {
            return Ok(());
        }

        let BlockCacheEntryData::Received {
            queue_diff,
            out_msgs,
            collated_after_receive: false,
            ..
        } = &block_entry.data
        else {
            // If block was collated then queue diff is already applied
            return Ok(());
        };

        let queue_diff_with_msgs =
            QueueDiffWithMessages::from_queue_diff(queue_diff, &out_msgs.load()?)?;
        mq_adapter
            .apply_diff(
                queue_diff_with_msgs,
                queue_diff.block_id().as_short_id(),
                queue_diff.diff_hash(),
            )
            .await
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
                self.set_collator_state(&next_block_id_short.shard, CollatorState::Cancelled);

                // run sync if all collators cancelled or waiting
                self.ready_to_sync.notified().await;

                let all_not_active = self
                    .active_collators
                    .iter()
                    .all(|ac| ac.state != CollatorState::Active);
                if all_not_active {
                    tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                        "collator cancelled or waiting in every shard and masterchain, \
                        will run sync to last applied mc block",
                    );

                    // get info about applied mc blocks in cache
                    let (last_collated_block_id, applied_range) = self
                        .blocks_cache
                        .get_last_collated_block_and_applied_mc_queue_range();

                    // run sync if have applied mc blocks
                    self.sync_to_applied_mc_block_if_exist(
                        last_collated_block_id.as_ref(),
                        applied_range.as_ref(),
                    )
                    .await?;
                }

                self.ready_to_sync.notify_one();
            }
        }
        Ok(())
    }

    #[tracing::instrument(skip_all, fields(next_block_id = %next_block_id_short, ct = anchor_chain_time, force_mc_block))]
    async fn handle_skipped_anchor(
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
                if !shard_id.is_masterchain() {
                    // shard collator will wait and master collator will work
                    self.set_collator_state(&ShardIdent::MASTERCHAIN, CollatorState::Active);
                    self.set_collator_state(&shard_id, CollatorState::Waiting);
                }

                self.enqueue_mc_block_collation(
                    prev_mc_block_id.get_next_id_short(),
                    next_mc_block_chain_time,
                    trigger_shard_block_id_opt,
                )
                .await?;
            } else {
                // current collator will wait
                self.set_collator_state(&shard_id, CollatorState::Waiting);
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

    /// Process collated block candidate
    /// 1. Save block to cache
    /// 2. Spawn block validation
    /// 3. Check if the master block interval elapsed (according to chain time) and schedule collation
    /// 4. If master block then update last master block chain time
    /// 5. Notify mempool about new master block (it may perform gc or nodes rotation)
    /// 6. Execute master block processing routines
    #[tracing::instrument(
        skip_all,
        fields(block_id = %collation_result.candidate.block.id().as_short_id()),
    )]
    pub async fn handle_collated_block_candidate(
        &self,
        collation_result: BlockCollationResult,
    ) -> Result<()> {
        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            ct = collation_result.candidate.chain_time,
            processed_to_anchor_id = collation_result.candidate.processed_to_anchor_id,
            "Start processing block candidate",
        );

        let _histogram =
            HistogramGuard::begin("tycho_collator_process_collated_block_candidate_time");

        let block_id = *collation_result.candidate.block.id();

        debug_assert_eq!(
            block_id.is_masterchain(),
            collation_result.mc_data.is_some(),
        );

        self.ready_to_sync.notified().await;

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

        let store_res = self
            .blocks_cache
            .store_collated(collation_result.candidate, collation_result.mc_data.clone())?;

        tracing::debug!(
            target: tracing_targets::COLLATION_MANAGER,
            ?store_res,
            "Saved block candidate to cache",
        );

        // check if should sync to last applied mc block
        let should_sync_to_last_applied_mc_block = {
            if let Some((_, applied_range_end)) = store_res.applied_mc_queue_range {
                if let Some(last_collated_mc_block_id) = store_res.last_collated_mc_block_id {
                    let applied_range_end_delta =
                        applied_range_end.saturating_sub(last_collated_mc_block_id.seqno);
                    if applied_range_end_delta < self.config.min_mc_block_delta_from_bc_to_sync {
                        // should collate next own mc block because last applied is not far ahead
                        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                            "check_should_sync: should collate next own mc block: \
                            last applied ({}) ahead last collated ({}) on {} < {}",
                            applied_range_end, last_collated_mc_block_id.seqno,
                            applied_range_end_delta, self.config.min_mc_block_delta_from_bc_to_sync,
                        );
                        false
                    } else {
                        // should sync to last applied mc block from bc because it is far ahead
                        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                            "check_should_sync: should sync to last applied mc block from bc: \
                            last applied ({}) ahead last collated ({}) on {} >= {}",
                            applied_range_end, last_collated_mc_block_id.seqno,
                            applied_range_end_delta, self.config.min_mc_block_delta_from_bc_to_sync,
                        );
                        true
                    }
                } else {
                    // should sync to last applied mc block from bc when lat collated not exist
                    tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                        "check_should_sync: should sync to last applied mc block from bc: \
                        last applied ({}) and last collated not exist",
                        applied_range_end,
                    );
                    true
                }
            } else {
                // should collate next own mc block because no applied ahead
                tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                    "check_should_sync: should collate next own mc block after because nothing applied ahead",
                );
                false
            }
        };

        // run validation or commit block
        if block_id.is_masterchain() {
            // run validation or commit block
            if store_res.received_and_collated {
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
                            handle_validated_master_block,
                            block_id,
                            status
                        ))
                        .await;
                });

                tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                    "Block candidate validation spawned",
                );
            }
        }

        // run sync or process just collated block
        if should_sync_to_last_applied_mc_block {
            // INFO: last collated mc block subgraph is already committed here

            let last_collated_block_id = store_res.last_collated_mc_block_id.as_ref();
            let applied_range = store_res.applied_mc_queue_range.as_ref();

            if block_id.is_masterchain() {
                // run sync if have applied mc blocks
                self.sync_to_applied_mc_block_if_exist(last_collated_block_id, applied_range)
                    .await?;
            } else {
                tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                    "sync_to_applied_mc_block: mark collator cancelled for shard",
                );

                // mark collator waiting
                self.set_collator_state(&block_id.shard, CollatorState::Waiting);

                // run sync if all collators cancelled or waiting and we have applied mc blocks
                let all_not_active = self
                    .active_collators
                    .iter()
                    .all(|ac| ac.state != CollatorState::Active);
                if all_not_active {
                    tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                        "sync_to_applied_mc_block: collator cancelled or waiting in every shard, \
                        will run sync to last applied mc block",
                    );

                    // run sync if have applied mc blocks
                    self.sync_to_applied_mc_block_if_exist(last_collated_block_id, applied_range)
                        .await?;
                }
            }
            self.ready_to_sync.notify_one();
        } else {
            self.ready_to_sync.notify_one();

            if block_id.is_masterchain() {
                // when candidate is master

                // save mc block latest chain time
                self.renew_mc_block_latest_chain_time(candidate_chain_time);

                // enqueue next master block collation if there are unprocessed messages
                if collation_result.has_unprocessed_messages {
                    tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                        "There are unprocessed messages in buffer or queue in master, enqueue next collation",
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
        }

        Ok(())
    }

    /// Process new block from blockchain:
    /// 1. Save block to cache with status Synced
    /// 2. Stop block validation if needed
    /// 3. Notify mempool about new master block
    /// 4. Refresh collation sessions according to master block
    #[tracing::instrument(skip_all, fields(block_id = %state.block_id().as_short_id()))]
    pub async fn handle_block_from_bc(&self, state: ShardStateStuff) -> Result<()> {
        let block_id = *state.block_id();

        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            "Start processing block from bc",
        );

        if block_id.is_masterchain() {
            if let Some(from_mc_block_seqno) = self.from_mc_block_seqno {
                if block_id.seqno < from_mc_block_seqno {
                    tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                        "recovery mode: skip block, should wait for specified last applied with seqno {}",
                        from_mc_block_seqno,
                    );
                    return Ok(());
                }
            }
        }

        if block_id.is_masterchain() {
            self.ready_to_sync.notified().await;
        }

        let Some(store_res) = self
            .blocks_cache
            .store_received(self.state_node_adapter.clone(), state)
            .await?
        else {
            self.ready_to_sync.notify_one();
            return Ok(());
        };

        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            res = ?store_res,
            "Saved block from bc to cache",
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
                    // but when all were cancelled or waiting, we can process last received mc block
                    let all_not_active = self
                        .active_collators
                        .iter()
                        .all(|ac| ac.state != CollatorState::Active);
                    if all_not_active {
                        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                            last_collated_mc_block_id = ?store_res.last_collated_mc_block_id.map(|id| id.as_short_id().to_string()),
                            last_processed_mc_block_id = ?last_processed_mc_block_id_opt.map(|id| id.as_short_id().to_string()),
                            "check_should_sync: should sync to last applied mc block \
                            when all collators were cancelled or waiting",
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
                // run sync if have applied mc blocks
                self.sync_to_applied_mc_block_if_exist(
                    store_res.last_collated_mc_block_id.as_ref(),
                    store_res.applied_mc_queue_range.as_ref(),
                )
                .await?;
                self.ready_to_sync.notify_one();
            } else {
                self.ready_to_sync.notify_one();
                // stop validation if block was collated first
                if store_res.received_and_collated {
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

    async fn sync_to_applied_mc_block_if_exist(
        &self,
        _last_collated_block_id: Option<&BlockId>, // TODO: use to skip queue recovery
        applied_range: Option<&(BlockSeqno, BlockSeqno)>,
    ) -> Result<()> {
        if let Some(applied_range) = applied_range {
            if !self.sync_to_applied_mc_block(applied_range).await? {
                let last_applied_mc_block_id_short = BlockIdShort {
                    shard: ShardIdent::MASTERCHAIN,
                    seqno: applied_range.1,
                };
                tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                    last_applied_mc_block_id = %last_applied_mc_block_id_short,
                    "sync_to_applied_mc_block: unable to sync to last applied mc block, \
                    need to receive next blocks from bc",
                );
            }
        } else {
            tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                "sync_to_applied_mc_block: there is no received applied mc blocks in cache, \
                will wait for next blocks from bc",
            );
        }
        Ok(())
    }

    #[tracing::instrument(skip_all, fields(applied_range = ?applied_range))]
    async fn sync_to_applied_mc_block(
        &self,
        applied_range: &(BlockSeqno, BlockSeqno),
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
        let min_processed_to_by_shards = self
            .read_min_processed_to_for_mc_block(&last_applied_mc_block_key)
            .await?;

        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            min_processed_to_by_shards = %DisplayIter(min_processed_to_by_shards.iter().map(DisplayTuple2)),
        );

        // find first applied mc block and tail shard blocks and get previous
        let before_tail_block_ids = self
            .blocks_cache
            .read_before_tail_ids_of_mc_block(&first_applied_mc_block_key)?;

        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            tail_block_ids = ?before_tail_block_ids
                .iter()
                .map(|(shard_id, (id, prev_ids))| {
                    format!(
                        "({}, id={:?}, prev_ids={})",
                        shard_id,
                        id.as_ref().map(DisplayAsShortId),
                        DisplayBlockIdsIntoIter(prev_ids),
                    )
                }).collect::<Vec<_>>().as_slice(),
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
            let (mc_block_subgraph_extract, is_last) = self
                .blocks_cache
                .pop_front_applied_mc_block_subgraph(applied_range.0)?;

            let subgraph = match mc_block_subgraph_extract {
                McBlockSubgraphExtract::Extracted(subgraph) => subgraph,
                other => bail!("mc block subgraph extract result cannot be {}", other),
            };

            // apply queue diffs
            for sc_block_entry in subgraph.shard_blocks.iter() {
                Self::apply_block_queue_diff_from_entry_stuff(
                    self.mq_adapter.clone(),
                    sc_block_entry,
                )
                .await?;
            }
            let mc_block_entry = &subgraph.master_block;
            Self::apply_block_queue_diff_from_entry_stuff(self.mq_adapter.clone(), mc_block_entry)
                .await?;

            // if it is last one then commit diffs, notify mempool and refresh collation sessions
            if is_last {
                tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                    "Will notify mempool and refresh collation sessions with HARD RESET",
                );

                Self::notify_mempool_about_mc_block(
                    self.mpool_adapter.clone(),
                    &mc_block_entry.block_id,
                )
                .await?;

                self.commit_block_queue_diff(
                    &mc_block_entry.block_id,
                    &mc_block_entry.top_shard_blocks_info,
                )
                .await?;

                let state = mc_block_entry.cached_state()?;

                // HACK: do not need to set master block latest chain time from zerostate when using mempool stub
                //      because anchors from stub have older chain time than in zerostate and it will brake collation
                if state.block_id().seqno != 0 {
                    self.renew_mc_block_latest_chain_time(state.get_gen_chain_time());
                }

                // TODO: refactor this logic
                // replace last collated block id with last applied
                self.blocks_cache
                    .update_last_collated_mc_block_id(*state.block_id());

                let mc_data = McData::load_from_state(state)?;

                // clean anchors cache in mempool
                let (top_processed_to_anchor_id, _) = Self::detect_top_processed_to_anchor(
                    &mc_data.shards,
                    state.state().processed_upto.load()?.externals.as_ref(),
                )?;
                self.mpool_adapter
                    .clear_anchors_cache(top_processed_to_anchor_id)
                    .await?;

                self.refresh_collation_sessions(mc_data, true).await?;

                // remove all previous blocks from cache
                let mut to_block_keys = vec![mc_block_entry.key()];
                to_block_keys.extend(
                    mc_block_entry
                        .iter_top_block_ids()
                        .map(|id| id.as_short_id()),
                );
                self.blocks_cache
                    .remove_prev_blocks_from_cache(&to_block_keys);

                break;
            }
        }

        Ok(true)
    }

    // Return min processed_to info from master and each shard
    async fn read_min_processed_to_for_mc_block(
        &self,
        mc_block_key: &BlockCacheKey,
    ) -> Result<BTreeMap<ShardIdent, QueueKey>> {
        let mut result = BTreeMap::new();

        if mc_block_key.seqno == 0 {
            return Ok(result);
        }

        let all_processed_to = self
            .blocks_cache
            .get_all_processed_to_by_mc_block_from_cache(mc_block_key)?;

        for (top_block_id, processed_to_opt) in all_processed_to {
            let processed_to = match processed_to_opt {
                Some(processed_to) => processed_to,
                None => {
                    // try get from storage
                    let loaded = utils::load_only_queue_diff_stuff(
                        self.state_node_adapter.as_ref(),
                        &top_block_id,
                    )
                    .await?;

                    loaded.as_ref().processed_upto.clone()
                }
            };

            for (shard_id, to_key) in processed_to {
                result
                    .entry(shard_id)
                    .and_modify(|min| {
                        if &to_key < min {
                            *min = to_key;
                        }
                    })
                    .or_insert(to_key);
            }
        }

        Ok(result)
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
    /// This needed when start from zerostate or whole network was restarted
    /// and nobody will produce next master block and we need
    /// to start collation sessions based on the actual state.
    pub async fn check_and_start_collation_sessions(&self) -> Result<()> {
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

            self.detect_top_processed_to_anchor_and_notify_mempool(
                false,
                state.clone(),
                state.state().processed_upto.load()?,
            )
            .await?;

            self.handle_block_from_bc(state).await
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
        let mut new_shards_info = FastHashMap::default();
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
        let mut sessions_to_keep = Vec::new();
        let mut sessions_to_start = Vec::new();
        let mut to_finish_sessions = Vec::new();
        let mut to_stop_collators = Vec::new();
        {
            let mut active_collation_sessions_guard = self.active_collation_sessions.write();
            let mut missed_shards_ids: FastHashSet<_> = active_shards_ids.into_iter().collect();
            for (shard_ident, block_ids) in new_shards_info {
                missed_shards_ids.remove(&shard_ident);
                match active_collation_sessions_guard.entry(shard_ident) {
                    hash_map::Entry::Occupied(entry) => {
                        let existing_session = entry.get();
                        if existing_session.seqno() >= new_session_seqno {
                            sessions_to_keep.push((
                                shard_ident,
                                existing_session.clone(),
                                block_ids,
                            ));
                        } else {
                            to_finish_sessions.push((
                                shard_ident,
                                new_session_seqno,
                                entry.remove(),
                            ));
                            sessions_to_start.push((shard_ident, block_ids));
                        }
                    }
                    hash_map::Entry::Vacant(_) => {
                        sessions_to_start.push((shard_ident, block_ids));
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
                    to_finish_sessions.push((shard_id, new_session_seqno, current_active_session));
                    if let Some((_, collator)) = self.active_collators.remove(&shard_id) {
                        to_stop_collators.push((shard_id, new_session_seqno, collator));
                    }
                }
            }
        }

        tracing::debug!(
            target: tracing_targets::COLLATION_MANAGER,
            "Will keep existing collation sessions: {:?}",
            DebugIter(sessions_to_keep.iter().map(|(shard_ident, _, _)| shard_ident)),
        );
        if !sessions_to_start.is_empty() {
            tracing::info!(
                target: tracing_targets::COLLATION_MANAGER,
                "Will start new collation sessions: {:?}",
                DebugIter(sessions_to_start.iter().map(|(k, _)| k)),
            );
        }

        let cc_config = mc_data.config.get_catchain_config()?;

        // update master state in existing collators and resume collation
        for (shard_id, _, prev_blocks_ids) in sessions_to_keep {
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
                            mempool_start_round: self.mempool_start_round,
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
                    to_stop_collators.push((shard_id, new_session_seqno, active_collator));
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
                DebugIter(to_finish_sessions.iter().map(|(s, seq, _)| (s, seq))),
            );
        }

        // enqueue outdated sessions finish tasks
        for (shard_ident, session_seqno, session_info) in to_finish_sessions {
            let finish_key = (shard_ident, session_seqno);
            self.collation_sessions_to_finish
                .insert(finish_key, session_info.clone());
            self.finish_collation_session(session_info, finish_key)
                .await?;
        }

        if !to_stop_collators.is_empty() {
            tracing::info!(
                target: tracing_targets::COLLATION_MANAGER,
                "Will stop collators for sessions that we do not serve: {:?}",
                DebugIter(to_stop_collators.iter().map(|(s, seq, _)| (s, seq))),
            );
        }

        // enqueue dangling collators stop tasks
        for (shard_ident, session_seqno, active_collator) in to_stop_collators {
            let stop_key = (shard_ident, session_seqno);
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
    pub async fn handle_collator_stopped(&self, stop_key: CollationSessionId) -> Result<()> {
        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            "process_collator_stopped: {:?}", stop_key,
        );
        self.collators_to_stop.remove(&stop_key);
        Ok(())
    }

    fn set_collator_state(&self, shard_id: &ShardIdent, state: CollatorState) {
        if let Some(mut active_collator) = self.active_collators.get_mut(shard_id) {
            active_collator.state = state;
        }
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

        let top_shard_blocks_info = self.blocks_cache.get_top_shard_blocks_info_for_mc_block(
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
    pub async fn handle_validated_master_block(
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
        let updated = self
            .blocks_cache
            .store_master_block_validation_result(&block_id, status);
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
        match self
            .blocks_cache
            .extract_mc_block_subgraph_for_sync(block_id)?
        {
            McBlockSubgraphExtract::Extracted(McBlockSubgraph {
                master_block,
                shard_blocks,
            }) => {
                extract_elapsed = histogram_extract.finish();

                // send to sync only if was not received from bc
                if matches!(&master_block.data, BlockCacheEntryData::Collated {
                    received_after_collation: false,
                    ..
                }) {
                    let histogram =
                        HistogramGuard::begin("tycho_collator_send_blocks_to_sync_time");

                    self.send_block_to_sync(master_block.data).await?;

                    for shard_block in shard_blocks {
                        self.send_block_to_sync(shard_block.data).await?;
                    }

                    sync_elapsed = histogram.finish();
                    tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                        total = sync_elapsed.as_millis(),
                        "send_blocks_to_sync timings",
                    );
                }

                self.commit_block_queue_diff(
                    &master_block.block_id,
                    &master_block.top_shard_blocks_info,
                )
                .await?;
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

    async fn send_block_to_sync(&self, data: BlockCacheEntryData) -> Result<()> {
        let candidate_stuff = match data {
            BlockCacheEntryData::Collated {
                candidate_stuff,
                status,
                received_after_collation: false,
                ..
            } if status != CandidateStatus::Synced => candidate_stuff,
            _ => return Ok(()),
        };

        let block_id = *candidate_stuff.candidate.block.id();
        let block_stuff_for_sync = candidate_stuff.into();
        self.state_node_adapter
            .accept_block(block_stuff_for_sync)
            .await?;
        tracing::debug!(
            target: tracing_targets::COLLATION_MANAGER,
            "Block was successfully sent to sync ({})",
            block_id,
        );
        Ok(())
    }

    /// Try find master block and execute post validation routines
    #[expect(unused)]
    async fn commit_valid_shard_block(&self, block_id: &BlockId) -> Result<()> {
        match self.blocks_cache.find_containing_mc_block(block_id) {
            Some((mc_block_id, true)) => {
                tracing::debug!(
                    target: tracing_targets::COLLATION_MANAGER,
                    "Found containing master block ({}) for just validated shard block ({}) in cache",
                    mc_block_id.as_short_id(),
                    block_id.as_short_id(),
                );
                self.commit_valid_master_block(&mc_block_id).await
            }
            Some((mc_block_id, false)) => {
                tracing::debug!(
                    target: tracing_targets::COLLATION_MANAGER,
                    "Containing master block ({}) for just validated shard block ({}) is not validated yet. Will wait for master block validation",
                    mc_block_id.as_short_id(),
                    block_id.as_short_id(),
                );
                Ok(())
            }
            None => {
                tracing::debug!(
                    target: tracing_targets::COLLATION_MANAGER,
                    "There is no containing master block for just validated shard block ({}) in cache. Will wait for master block collation",
                    block_id.as_short_id(),
                );
                Ok(())
            }
        }
    }
}
