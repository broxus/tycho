use std::collections::{hash_map, BTreeMap, VecDeque};
use std::sync::Arc;

use ahash::HashMapExt;
use anyhow::{anyhow, bail, Result};
use async_trait::async_trait;
use everscale_crypto::ed25519::KeyPair;
use everscale_types::models::{
    BlockId, BlockIdShort, CollationConfig, ExternalsProcessedUpto, ProcessedUptoInfo, ShardHashes,
    ShardIdent, ValidatorDescription,
};
use parking_lot::{Mutex, RwLock};
use tokio::sync::Notify;
use tycho_block_util::block::{calc_next_block_id_short, ValidatorSubsetInfo};
use tycho_block_util::queue::QueueKey;
use tycho_block_util::state::ShardStateStuff;
use tycho_core::global_config::MempoolGlobalConfig;
use tycho_util::metrics::HistogramGuard;
use tycho_util::{DashMapEntry, FastDashMap, FastHashMap, FastHashSet};
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
    StateUpdateContext,
};
use crate::queue_adapter::MessageQueueAdapter;
use crate::state_node::{StateNodeAdapter, StateNodeAdapterFactory, StateNodeEventListener};
use crate::types::{
    BlockCollationResult, BlockIdExt, CollationSessionId, CollationSessionInfo, CollatorConfig,
    DebugIter, DisplayAsShortId, DisplayBlockIdsIntoIter, DisplayIter, DisplayTuple, McData,
    ShardDescriptionExt,
};
use crate::utils::async_dispatcher::{AsyncDispatcher, STANDARD_ASYNC_DISPATCHER_BUFFER_SIZE};
use crate::utils::shard::calc_split_merge_actions;
use crate::utils::vset_cache::ValidatorSetCache;
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
    config: Arc<CollatorConfig>,

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

    /// Cache for validator sets from config
    validator_set_cache: ValidatorSetCache,

    /// Mempool config override for a new genesis
    mempool_config_override: Option<MempoolGlobalConfig>,
    /// Last know applied master block seqno
    /// to restart from a new genesis
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

        let state_cloned = state.clone();
        self.spawn_task(method_to_async_closure!(
            cancel_validation_sessions_until_block,
            state_cloned
        ))
        .await?;

        Ok(())
    }

    async fn on_block_accepted_external(&self, state: &ShardStateStuff) -> Result<()> {
        let processed_upto = state.state().processed_upto.load()?;

        metrics_report_last_applied_block_and_anchor(state, &processed_upto);

        let state = state.clone();
        self.enqueue_task(method_to_async_closure!(handle_block_from_bc, state))
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

    let block_ct = state.get_gen_chain_time();
    let processed_to_anchor_id = processed_upto
        .externals
        .as_ref()
        .map(|upto| upto.processed_to.0)
        .unwrap_or_default();

    metrics::gauge!("tycho_last_applied_block_seqno", &labels).set(block_id.seqno);
    metrics::gauge!("tycho_last_processed_to_anchor_id", &labels).set(processed_to_anchor_id);

    tracing::info!(target: tracing_targets::COLLATION_MANAGER,
        block_id = %DisplayAsShortId(block_id),
        block_ct,
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
        collation_config: Arc<CollationConfig>,
    ) -> Result<()> {
        self.spawn_task(method_to_async_closure!(
            handle_skipped_anchor,
            prev_mc_block_id,
            next_block_id_short,
            anchor_chain_time,
            force_mc_block,
            collation_config
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
        config: CollatorConfig,
        mq_adapter: Arc<dyn MessageQueueAdapter<EnqueuedMessage>>,
        state_node_adapter_factory: STF,
        mpool_adapter_factory: MPF,
        validator: V,
        collator_factory: CF,
        mempool_config_override: Option<MempoolGlobalConfig>,
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

            validator_set_cache: Default::default(),

            mempool_config_override,
            from_mc_block_seqno,

            #[cfg(any(test, feature = "test"))]
            test_validators_keypairs,
        };
        arc_dispatcher.run(Arc::new(processor), tasks_receiver);
        tracing::trace!(target: tracing_targets::COLLATION_MANAGER, "Tasks dispatchers started");

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

    #[tracing::instrument(skip_all, fields(block_id = %state.block_id().as_short_id()))]
    async fn cancel_validation_sessions_until_block(&self, state: ShardStateStuff) -> Result<()> {
        self.validator
            .cancel_validation(&state.block_id().as_short_id())?;
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

        let _histogram = HistogramGuard::begin("tycho_collator_commit_queue_diffs_time");

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

                let has_active = self
                    .active_collators
                    .iter()
                    .any(|ac| ac.state == CollatorState::Active);
                if !has_active {
                    tracing::info!(target: tracing_targets::COLLATION_MANAGER,
                        "no active collators in shards and masterchain, \
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
        collation_config: Arc<CollationConfig>,
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
            collation_config.mc_block_min_interval_ms as _,
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
        mc_block_min_interval_ms: u64,
    ) -> Result<()> {
        // if should collate master block due to current shard chain time
        // then stop current shard collation
        let (should_collate_mc_block, next_mc_block_chain_time_opt) = self
            .update_last_collated_chain_time_and_check_should_collate_mc_block(
                shard_id,
                chain_time,
                force_mc_block,
                mc_block_min_interval_ms,
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
            HistogramGuard::begin("tycho_collator_handle_collated_block_candidate_time");

        let block_id = *collation_result.candidate.block.id();

        debug_assert_eq!(
            block_id.is_masterchain(),
            collation_result.mc_data.is_some(),
        );

        self.ready_to_sync.notified().await;

        // find collation session related to this block by session id
        let session_info = match self
            .active_collation_sessions
            .read()
            .get(&block_id.shard)
            .cloned()
        {
            Some(session_info) if session_info.id() == collation_result.collation_session_id => {
                session_info
            }
            _ => {
                // otherwise skip block because we have synced to a newer mc block
                // and found out that we should not collated at all
                tracing::warn!(
                    target: tracing_targets::COLLATION_MANAGER,
                    "There is no active session related to collated block. Skipped",
                );

                self.set_collator_state(&block_id.shard, CollatorState::Waiting);

                return Ok(());
            }
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
                        tracing::info!(target: tracing_targets::COLLATION_MANAGER,
                            "check_should_sync: should sync to last applied mc block from bc: \
                            last applied ({}) ahead last collated ({}) on {} >= {}",
                            applied_range_end, last_collated_mc_block_id.seqno,
                            applied_range_end_delta, self.config.min_mc_block_delta_from_bc_to_sync,
                        );
                        true
                    }
                } else {
                    // should sync to last applied mc block from bc when last collated not exist
                    tracing::info!(target: tracing_targets::COLLATION_MANAGER,
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
                // cancel further collation of blocks in the current shard because we need to sync
                tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                    "sync_to_applied_mc_block: mark shard collator Cancelled for shard",
                );

                self.set_collator_state(&block_id.shard, CollatorState::Cancelled);

                // run sync if all collators cancelled, or waiting, or there are no collators
                // and we have applied mc blocks
                let has_active = self
                    .active_collators
                    .iter()
                    .any(|ac| ac.state == CollatorState::Active);
                if !has_active {
                    tracing::info!(target: tracing_targets::COLLATION_MANAGER,
                        "sync_to_applied_mc_block: no active collators in shards and master, \
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
                    tracing::info!(target: tracing_targets::COLLATION_MANAGER,
                        "There are unprocessed messages in buffer or queue in master, enqueue next collation",
                    );
                    self.enqueue_mc_block_collation(
                        collation_result.prev_mc_block_id.get_next_id_short(),
                        candidate_chain_time,
                        Some(block_id),
                    )
                    .await?;
                } else {
                    // otherwise execute master state update processing routines
                    self.process_mc_state_update(collation_result.mc_data.unwrap(), false)
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
                    collation_result.collation_config.mc_block_min_interval_ms as _,
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
        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            "Start processing block from bc",
        );

        let block_id = *state.block_id();

        let _histogram = HistogramGuard::begin("tycho_collator_handle_block_from_bc_time");

        if block_id.is_masterchain() {
            if let Some(from_mc_block_seqno) = self.from_mc_block_seqno {
                if block_id.seqno < from_mc_block_seqno {
                    tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                        "restart with a new genesis: skip block, should wait for specified last applied with seqno {}",
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
            ?store_res,
            "Saved block from bc to cache",
        );

        // stop any running validations up to this block
        self.validator.cancel_validation(&block_id.as_short_id())?;

        if block_id.is_masterchain() {
            // when received block is master

            // check if should sync to last applied mc block right now
            let should_sync_to_last_applied_mc_block = {
                let last_processed_mc_block_id_opt = self.get_last_processed_mc_block_id();
                if store_res.last_collated_mc_block_id.is_some()
                    || last_processed_mc_block_id_opt.is_some()
                {
                    // Should wait for next collated mc block when collators are active
                    // but when all were cancelled or waiting, we can process last received mc block.
                    // Also can process last received mc block when no active collators
                    let has_active = self
                        .active_collators
                        .iter()
                        .any(|ac| ac.state == CollatorState::Active);
                    if !has_active {
                        tracing::info!(target: tracing_targets::COLLATION_MANAGER,
                            last_collated_mc_block_id = ?store_res.last_collated_mc_block_id.map(|id| id.as_short_id().to_string()),
                            last_processed_mc_block_id = ?last_processed_mc_block_id_opt.map(|id| id.as_short_id().to_string()),
                            "check_should_sync: should sync to last applied mc block \
                            when all collators were cancelled, or waiting, or ther are no collators",
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
                    tracing::info!(target: tracing_targets::COLLATION_MANAGER,
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
                // try to commit block if it was collated first
                if store_res.received_and_collated {
                    // NOTE: here master block subgraph could be already extracted,
                    //      sent to sync, and removed from cache, because validation task
                    //      could be finished after `store_received()` but before this point.

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
                tracing::info!(target: tracing_targets::COLLATION_MANAGER,
                    last_applied_mc_block_id = %last_applied_mc_block_id_short,
                    "sync_to_applied_mc_block: unable to sync to last applied mc block, \
                    need to receive next blocks from bc",
                );
            }
        } else {
            tracing::info!(target: tracing_targets::COLLATION_MANAGER,
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
        tracing::info!(target: tracing_targets::COLLATION_MANAGER,
            "Start sync to applied mc block",
        );

        let _histogram = HistogramGuard::begin("tycho_collator_sync_to_applied_mc_block_time");

        let first_applied_mc_block_key = BlockIdShort {
            shard: ShardIdent::MASTERCHAIN,
            seqno: applied_range.0,
        };
        let last_applied_mc_block_key = BlockIdShort {
            shard: ShardIdent::MASTERCHAIN,
            seqno: applied_range.1,
        };

        // we need to drop uncommitted internal messages from the queue
        // when mempool config override has genesis higher then in the last consensus info
        if let Some(mp_cfg_override) = &self.mempool_config_override {
            let last_consesus_info = self
                .blocks_cache
                .get_consensus_info_for_mc_block(&last_applied_mc_block_key)?;
            if mp_cfg_override.start_round > last_consesus_info.genesis_round
                && mp_cfg_override.genesis_time_millis > last_consesus_info.genesis_millis
            {
                tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                    prev_genesis_start_round = last_consesus_info.genesis_round,
                    prev_genesis_time_millis = last_consesus_info.genesis_millis,
                    new_genesis_start_round = mp_cfg_override.start_round,
                    new_genesis_time_millis = mp_cfg_override.genesis_time_millis,
                    "Will drop uncommitted internal messages from queue on new genesis",
                );
                self.mq_adapter.clear_session_state()?;
            }
        }

        // get min internals processed upto
        let min_processed_to_by_shards = self
            .read_min_processed_to_for_mc_block(&last_applied_mc_block_key)
            .await?;

        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            min_processed_to_by_shards = %DisplayIter(min_processed_to_by_shards.iter().map(DisplayTuple)),
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

                let init_mc_block_id = self.state_node_adapter.load_init_block_id();

                if let Some(init_mc_block_id) = init_mc_block_id {
                    if prev_block_id.is_masterchain() {
                        if prev_block_id.seqno <= init_mc_block_id.seqno {
                            continue;
                        }
                    } else {
                        let prev_shard_block_handle = self
                            .state_node_adapter
                            .load_block_handle(&prev_block_id)
                            .await?
                            .unwrap();
                        if prev_shard_block_handle.mc_ref_seqno() <= init_mc_block_id.seqno {
                            continue;
                        }
                    }
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
                    let out_msgs = block_stuff.load_extra()?.out_msg_description.load()?;

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
                McBlockSubgraphExtract::AlreadyExtracted => {
                    bail!("mc block subgraph extract result cannot be AlreadyExtracted")
                }
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

                // reset top shard blocks info
                // because next we will start to collate new shard blocks after the sync
                self.blocks_cache.reset_top_shard_blocks_additional_info()?;

                let mc_data = McData::load_from_state(state)?;

                // clean anchors cache in mempool
                let (top_processed_to_anchor_id, _) = Self::detect_top_processed_to_anchor(
                    &mc_data.shards,
                    state.state().processed_upto.load()?.externals.as_ref(),
                )?;
                self.mpool_adapter
                    .clear_anchors_cache(top_processed_to_anchor_id)
                    .await?;

                self.mpool_adapter
                    .handle_top_processed_to_anchor(top_processed_to_anchor_id)
                    .await?;

                self.process_mc_state_update(mc_data, true).await?;

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

    async fn process_mc_state_update(
        &self,
        mc_data: Arc<McData>,
        reset_collators: bool,
    ) -> Result<()> {
        tracing::info!(target: tracing_targets::COLLATION_MANAGER,
            reset_collators,
            "Will notify mempool and refresh collation sessions",
        );

        self.notify_mc_state_update_to_mempool(mc_data.clone())
            .await?;

        self.refresh_collation_sessions(mc_data, reset_collators)
            .await
    }

    async fn notify_mc_state_update_to_mempool(&self, mc_data: Arc<McData>) -> Result<()> {
        let prev_validator_set = self
            .validator_set_cache
            .get_prev_validator_set(&mc_data.config)?;
        let current_validator_set = self
            .validator_set_cache
            .get_current_validator_set(&mc_data.config)?;
        let next_validator_set = self
            .validator_set_cache
            .get_next_validator_set(&mc_data.config)?;

        let mc_processed_to_anchor_id = mc_data
            .processed_upto
            .externals
            .as_ref()
            .map_or(0, |upto| upto.processed_to.0);

        let cx = StateUpdateContext {
            mc_block_id: mc_data.block_id,
            mc_block_chain_time: mc_data.gen_chain_time,
            mc_processed_to_anchor_id,
            consensus_info: mc_data.consensus_info,
            shuffle_validators: mc_data.config.get_collation_config()?.shuffle_mc_validators,
            consensus_config: mc_data.config.get_consensus_config()?,
            prev_validator_set,
            current_validator_set,
            next_validator_set,
        };

        self.mpool_adapter.handle_mc_state_update(cx).await
    }

    /// Get shards and validator set info from the master state,
    /// then start missing collation sessions, finish outdated, resume actual.
    /// Start/stop/resume collators and validators.
    #[tracing::instrument(skip_all)]
    async fn refresh_collation_sessions(
        &self,
        mc_data: Arc<McData>,
        reset_collators: bool,
    ) -> Result<()> {
        tracing::debug!(
            target: tracing_targets::COLLATION_MANAGER,
            "Trying to refresh collation sessions by mc state ({})...",
            mc_data.block_id.as_short_id(),
        );

        let _histogram = HistogramGuard::begin("tycho_collator_refresh_collation_sessions_time");

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
        let current_session_seqno = mc_data.validator_info.catchain_seqno;

        // we need full validators set to define the subset for each session and to check if current node should collate
        let full_validators_set = mc_data.config.get_current_validator_set()?;
        tracing::trace!(target: tracing_targets::COLLATION_MANAGER,
            "full_validators_set: since={}, until={}, main={}, total_weight={}, list={:?}",
            full_validators_set.utime_since, full_validators_set.utime_until,
            full_validators_set.main, full_validators_set.total_weight,
            DebugIter(full_validators_set.list.iter().map(|i| i.public_key)),
        );
        let collation_config = mc_data.config.get_collation_config()?;
        let mut subset_cache = FastHashMap::new();
        let mut get_validator_subset = |shard_id| match subset_cache.entry(shard_id) {
            hash_map::Entry::Occupied(entry) => {
                let (subset, hash_short): &(Arc<FastHashMap<[u8; 32], ValidatorDescription>>, u32) =
                    entry.get();
                anyhow::Result::<_>::Ok((subset.clone(), *hash_short))
            }
            hash_map::Entry::Vacant(entry) => {
                let (subset, hash_short) = full_validators_set
                    .compute_mc_subset(current_session_seqno, collation_config.shuffle_mc_validators)
                    .ok_or(anyhow!(
                        "Error calculating subset of validators for session (shard_id = {}, seqno = {})",
                        ShardIdent::MASTERCHAIN,
                        current_session_seqno,
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

                let subset: FastHashMap<_, _> = subset
                    .into_iter()
                    .map(|vldr| (vldr.public_key.into(), vldr))
                    .collect();
                let subset = Arc::new(subset);

                entry.insert((subset.clone(), hash_short));
                Ok((subset, hash_short))
            }
        };

        // detect sessions and collators to start and to finish
        let mut sessions_to_keep = Vec::new();
        let mut sessions_to_start = Vec::new();
        let mut to_finish_sessions = Vec::new();
        let mut to_stop_collators = Vec::new();
        {
            let mut active_collation_sessions_guard = self.active_collation_sessions.write();
            let mut missed_shards_ids: FastHashSet<_> = active_shards_ids.into_iter().collect();
            for (shard_id, block_ids) in new_shards_info {
                missed_shards_ids.remove(&shard_id);

                // check if current node is in subset
                let (subset, _) = get_validator_subset(shard_id)?;
                let local_pubkey = find_us_in_collators_set(&self.keypair, &subset);

                if local_pubkey.is_none() {
                    tracing::debug!(
                        target: tracing_targets::COLLATION_MANAGER,
                        public_key = %self.keypair.public_key,
                        "Current node was not authorized to collate shard {}",
                        shard_id,
                    );
                }

                match active_collation_sessions_guard.entry(shard_id) {
                    hash_map::Entry::Occupied(entry) => {
                        let existing_session_info = entry.get().clone();
                        if local_pubkey.is_some() {
                            if existing_session_info.seqno() >= current_session_seqno {
                                sessions_to_keep.push((shard_id, existing_session_info, block_ids));
                            } else {
                                to_finish_sessions.push(entry.remove());
                                sessions_to_start.push((shard_id, block_ids));
                            }
                        } else {
                            to_finish_sessions.push(entry.remove());
                            if let Some((_, collator)) = self.active_collators.remove(&shard_id) {
                                to_stop_collators.push((existing_session_info, collator));
                            }
                        }
                    }
                    hash_map::Entry::Vacant(_) => {
                        if local_pubkey.is_some() {
                            sessions_to_start.push((shard_id, block_ids));
                        }
                    }
                }
            }

            // if we still have some active sessions that do not match with new shards and validator subset
            // then we need to finish them and stop their collators
            for shard_id in missed_shards_ids {
                if let Some(existing_session_info) =
                    active_collation_sessions_guard.remove(&shard_id)
                {
                    to_finish_sessions.push(existing_session_info.clone());
                    if let Some((_, collator)) = self.active_collators.remove(&shard_id) {
                        to_stop_collators.push((existing_session_info, collator));
                    }
                }
            }
        }

        if !sessions_to_start.is_empty() {
            tracing::info!(
                target: tracing_targets::COLLATION_MANAGER,
                "Will start new collation sessions: {:?}",
                DebugIter(sessions_to_start.iter().map(|(s, _)| (s, current_session_seqno))),
            );
        }

        // we may have sessions to finish, collators to stop, and sessions to start,
        // and we start missing collators for new sessions
        for (shard_id, prev_blocks_ids) in sessions_to_start {
            let (subset, hash_short) = get_validator_subset(shard_id)?;

            let new_session_info = Arc::new(CollationSessionInfo::new(
                shard_id,
                current_session_seqno,
                ValidatorSubsetInfo {
                    validators: subset.values().cloned().collect(),
                    short_hash: hash_short,
                },
                Some(self.keypair.clone()),
            ));

            tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                "new_session_info.validators: {:?}",
                new_session_info.collators(),
            );

            let next_block_id_short = calc_next_block_id_short(&prev_blocks_ids);

            match self.active_collators.entry(shard_id) {
                DashMapEntry::Occupied(_) => {
                    tracing::info!(
                        target: tracing_targets::COLLATION_MANAGER,
                        "Active collator exists for collation session {:?}. Will resume it",
                        new_session_info.id(),
                    );
                    sessions_to_keep.push((shard_id, new_session_info.clone(), prev_blocks_ids));
                }
                DashMapEntry::Vacant(entry) => {
                    tracing::info!(
                        target: tracing_targets::COLLATION_MANAGER,
                        "There is no active collator for collation session {:?}. Will start it",
                        new_session_info.id(),
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
                            mempool_config_override: self.mempool_config_override.clone(),
                        })
                        .await;

                    entry.insert(ActiveCollator {
                        collator: Arc::new(collator),
                        state: CollatorState::Active,
                    });
                }
            }

            // need to add validation session only for masterchain blocks, shard blocks are not being validated
            if shard_id.is_masterchain() {
                self.validator.add_session(AddSession {
                    shard_ident: shard_id,
                    session_id: new_session_info.seqno(),
                    start_block_seqno: next_block_id_short.seqno,
                    validators: &new_session_info.collators().validators,
                })?;
            }

            self.active_collation_sessions
                .write()
                .insert(shard_id, new_session_info);
        }

        tracing::debug!(
            target: tracing_targets::COLLATION_MANAGER,
            "Will keep existing collation sessions: {:?}",
            DebugIter(sessions_to_keep.iter().map(|(_, s, _)| s.id())),
        );

        // update master state in existing collators and resume collation
        for (shard_id, new_session_info, prev_blocks_ids) in sessions_to_keep {
            let collator = {
                let Some(mut active_collator) = self.active_collators.get_mut(&shard_id) else {
                    bail!(
                        "Collator for shard should exist for active session {:?}",
                        new_session_info.id(),
                    )
                };
                active_collator.state = CollatorState::Active;
                active_collator.collator.clone()
            };

            tracing::debug!(
                target: tracing_targets::COLLATION_MANAGER,
                "Resuming collation attempts in shard session {:?}",
                new_session_info.id(),
            );
            collator
                .enqueue_resume_collation(
                    mc_data.clone(),
                    reset_collators,
                    new_session_info,
                    prev_blocks_ids,
                )
                .await?;
        }

        if !to_finish_sessions.is_empty() {
            tracing::info!(
                target: tracing_targets::COLLATION_MANAGER,
                "Will finish outdated collation sessions: {:?}",
                DebugIter(to_finish_sessions.iter().map(|s| s.id())),
            );
        }

        // enqueue outdated sessions finish tasks
        for session_info in to_finish_sessions {
            self.collation_sessions_to_finish
                .insert(session_info.id(), session_info.clone());
            self.finish_collation_session(session_info).await?;
        }

        if !to_stop_collators.is_empty() {
            tracing::info!(
                target: tracing_targets::COLLATION_MANAGER,
                "Will stop collators for sessions that we do not serve: {:?}",
                DebugIter(to_stop_collators.iter().map(|(s, _)| s.id())),
            );
        }

        // enqueue dangling collators stop tasks
        for (session_info, active_collator) in to_stop_collators {
            let collator = active_collator.collator.clone();
            self.collators_to_stop
                .insert(session_info.id(), active_collator);
            collator.enqueue_stop().await?;
        }

        Ok(())

        // finally we will have initialized `active_collation_sessions`
        // and `active_collators` which run async block collations processes
    }

    /// Execute collation session finalization routines
    pub async fn finish_collation_session(
        &self,
        collation_session: Arc<CollationSessionInfo>,
    ) -> Result<()> {
        tracing::info!(target: tracing_targets::COLLATION_MANAGER,
            "finish_collation_session: {:?}", collation_session.id(),
        );
        self.collation_sessions_to_finish
            .remove(&collation_session.id());
        Ok(())
    }

    /// Remove stopped collator from cache
    pub async fn handle_collator_stopped(
        &self,
        collation_session_id: CollationSessionId,
    ) -> Result<()> {
        tracing::info!(target: tracing_targets::COLLATION_MANAGER,
            "handle_collator_stopped: {:?}", collation_session_id,
        );
        self.collators_to_stop.remove(&collation_session_id);
        Ok(())
    }

    fn set_collator_state(&self, shard_id: &ShardIdent, state: CollatorState) {
        if let Some(mut active_collator) = self.active_collators.get_mut(shard_id) {
            active_collator.state = state;
        }
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
        mc_block_min_interval_ms: u64,
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
                tracing::info!(
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
            .enqueue_do_collate(top_shard_blocks_info, next_mc_block_chain_time)
            .await?;

        tracing::info!(target: tracing_targets::COLLATION_MANAGER,
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

        tracing::info!(
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

        let _histogram = HistogramGuard::begin("tycho_collator_handle_validated_master_block_time");

        // update block validation status
        let updated = self
            .blocks_cache
            .store_master_block_validation_result(&block_id, status);
        if !updated {
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

        let histogram = HistogramGuard::begin("tycho_collator_commit_valid_master_block_time");
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

                let _histogram =
                    HistogramGuard::begin("tycho_collator_send_blocks_to_sync_commit_diffs_time");

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
