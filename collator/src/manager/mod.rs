use std::collections::{BTreeMap, VecDeque, hash_map};
use std::sync::Arc;

use ahash::HashMapExt;
use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use parking_lot::{Mutex, RwLock};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tycho_block_util::block::{ValidatorSubsetInfo, calc_next_block_id_short};
use tycho_block_util::queue::{QueueKey, QueuePartitionIdx};
use tycho_block_util::state::ShardStateStuff;
use tycho_core::global_config::MempoolGlobalConfig;
use tycho_core::storage::ShardStateStorageError;
use tycho_crypto::ed25519::KeyPair;
use tycho_types::models::{
    BlockId, BlockIdShort, CollationConfig, GlobalCapabilities, ProcessedUptoInfo, ShardIdent,
    ValidatorDescription,
};
use tycho_util::metrics::HistogramGuard;
use tycho_util::{DashMapEntry, FastDashMap, FastHashMap, FastHashSet};
use types::{
    ActiveCollator, ActiveSync, BlockCacheEntry, BlockCacheEntryData, BlockSeqno, CollationStatus,
    CollatorState, McBlockSubgraph, NextCollationStep,
};

use self::blocks_cache::BlocksCache;
use self::types::{BlockCacheKey, CandidateStatus, CollationSyncState, McBlockSubgraphExtract};
use self::utils::find_us_in_collators_set;
use crate::collator::{
    CollationCancelReason, Collator, CollatorContext, CollatorEventListener, CollatorFactory,
    ForceMasterCollation,
};
use crate::internal_queue::types::{
    DiffStatistics, DiffZone, EnqueuedMessage, QueueDiffWithMessages,
};
use crate::manager::types::{
    BlockCacheStoreResult, CollationState, HandledBlockFromBcCtx, ImportedAnchorEvent,
};
use crate::mempool::{
    MempoolAdapter, MempoolAdapterFactory, MempoolAnchor, MempoolAnchorId, MempoolEventListener,
    StateUpdateContext,
};
use crate::queue_adapter::MessageQueueAdapter;
use crate::state_node::{StateNodeAdapter, StateNodeAdapterFactory, StateNodeEventListener};
use crate::types::processed_upto::{
    ProcessedUptoInfoExtension, ProcessedUptoInfoStuff, find_min_processed_to_by_shards,
};
use crate::types::{
    BlockCollationResult, BlockIdExt, CollationSessionId, CollationSessionInfo, CollatorConfig,
    DebugIter, DisplayAsShortId, DisplayBlockIdsIntoIter, McData, ProcessedToByPartitions,
    ShardDescriptionShort, ShardDescriptionShortExt, ShardHashesExt,
};
use crate::utils::async_dispatcher::{AsyncDispatcher, STANDARD_ASYNC_DISPATCHER_BUFFER_SIZE};
use crate::utils::block::detect_top_processed_to_anchor;
use crate::utils::shard::calc_split_merge_actions;
use crate::utils::vset_cache::ValidatorSetCache;
use crate::validator::{AddSession, ValidationStatus, Validator};
use crate::{method_to_async_closure, tracing_targets};

mod blocks_cache;
mod types;
mod utils;

#[cfg(test)]
#[path = "tests/manager_tests.rs"]
pub(super) mod tests;

const BLOCKS_FROM_BC_QUEUE_LIMIT: usize = 1000;

pub struct RunningCollationManager<CF, V>
where
    CF: CollatorFactory,
{
    cancel_async_tasks: CancellationToken,
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

impl<CF: CollatorFactory, V> Drop for RunningCollationManager<CF, V> {
    fn drop(&mut self) {
        self.cancel_async_tasks.cancel();
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

    /// Queue of received blocks from bc
    blocks_from_bc_queue_sender: tokio::sync::mpsc::Sender<HandledBlockFromBcCtx>,

    /// Used to sync tasks that may cause `sync_to_applied_mc_block`
    ready_to_sync: Arc<Notify>,

    /// block id of last processed master state (after collation or on sync)
    last_processed_mc_block_id: Arc<Mutex<Option<BlockId>>>,

    /// state to sync collation between master and shard chains
    collation_sync_state: Arc<Mutex<CollationSyncState>>,

    /// Cache for validator sets from config
    validator_set_cache: ValidatorSetCache,

    /// Mempool config override for a new genesis
    mempool_config_override: Option<MempoolGlobalConfig>,

    /// `McData` which processing was delayed until block is validated.
    delayed_mc_state_update: Arc<Mutex<Option<Arc<McData>>>>,
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

        metrics_report_last_applied_block_and_anchor(state, &processed_upto)?;

        let state_cloned = state.clone();

        self.spawn_task(method_to_async_closure!(
            detect_top_processed_to_anchor_and_notify_mempool,
            state_cloned,
            processed_upto.get_min_externals_processed_to()?.0
        ))
        .await?;

        let state_cloned = state.clone();
        self.spawn_task(move |worker| {
            Box::pin(async move { worker.cancel_validation_sessions_until_block(state_cloned) })
        })
        .await?;

        Ok(())
    }

    async fn on_block_accepted_external(&self, state: &ShardStateStuff) -> Result<()> {
        let processed_upto = state.state().processed_upto.load()?;

        metrics_report_last_applied_block_and_anchor(state, &processed_upto)?;

        let state = state.clone();
        let ctx = HandledBlockFromBcCtx {
            state,
            processed_upto,
        };

        self.enqueue_task(method_to_async_closure!(enqueue_handle_block_from_bc, ctx))
            .await?;

        Ok(())
    }
}

fn metrics_report_last_applied_block_and_anchor(
    state: &ShardStateStuff,
    processed_upto: &ProcessedUptoInfo,
) -> Result<()> {
    let block_id = state.block_id();
    let labels = [("workchain", block_id.shard.workchain().to_string())];

    let block_ct = state.get_gen_chain_time();
    let processed_to_anchor_id = processed_upto.get_min_externals_processed_to()?.0;

    metrics::gauge!("tycho_last_applied_block_seqno", &labels).set(block_id.seqno);
    metrics::gauge!("tycho_last_processed_to_anchor_id", &labels).set(processed_to_anchor_id);

    tracing::info!(target: tracing_targets::COLLATION_MANAGER,
        block_id = %DisplayAsShortId(block_id),
        block_ct,
        processed_to_anchor_id = processed_to_anchor_id,
        "last applied block",
    );

    Ok(())
}

#[async_trait]
impl<CF, V> CollatorEventListener for AsyncDispatcher<CollationManager<CF, V>>
where
    CF: CollatorFactory,
    V: Validator,
{
    async fn on_skipped(
        &self,
        prev_mc_block_id: BlockId,
        next_block_id_short: BlockIdShort,
        anchor_chain_time: u64,
        force_mc_block: ForceMasterCollation,
        collation_config: Arc<CollationConfig>,
    ) -> Result<()> {
        self.spawn_task(method_to_async_closure!(
            handle_collation_skipped,
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
        self.spawn_task(move |worker| {
            Box::pin(async move { worker.handle_collator_stopped(stop_key) })
        })
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
    ) -> RunningCollationManager<CF, V>
    where
        STF: StateNodeAdapterFactory,
        MPF: MempoolAdapterFactory,
    {
        tracing::info!(target: tracing_targets::COLLATION_MANAGER, "Creating collation manager...");

        // create dispatcher for own tasks
        let (dispatcher, dispatcher_ctx) = AsyncDispatcher::new(
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

        let (blocks_from_bc_queue_sender, blocks_from_bc_queue_receiver) =
            tokio::sync::mpsc::channel::<HandledBlockFromBcCtx>(BLOCKS_FROM_BC_QUEUE_LIMIT);

        let ready_to_sync = Arc::new(Notify::new());
        ready_to_sync.notify_one();

        let collation_manager = Self {
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

            blocks_cache: BlocksCache::new(),

            blocks_from_bc_queue_sender,

            ready_to_sync,

            last_processed_mc_block_id: Default::default(),

            collation_sync_state: Default::default(),

            validator_set_cache: Default::default(),

            mempool_config_override,

            delayed_mc_state_update: Arc::new(Mutex::new(None)),
        };
        let collation_manager = Arc::new(collation_manager);

        let cancel_async_tasks = CancellationToken::new();

        // start additional async tasks
        tokio::spawn(
            collation_manager
                .clone()
                .process_handle_block_from_bc_queue(
                    blocks_from_bc_queue_receiver,
                    cancel_async_tasks.clone(),
                ),
        );

        // start tasks dispatcher
        arc_dispatcher.run(collation_manager, dispatcher_ctx);
        tracing::trace!(target: tracing_targets::COLLATION_MANAGER, "Tasks dispatchers started");

        tracing::info!(target: tracing_targets::COLLATION_MANAGER, "Collation manager created");

        RunningCollationManager {
            cancel_async_tasks,
            dispatcher,
            state_node_adapter,
            mpool_adapter,
            mq_adapter,
        }
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
        mc_processed_to_anchor_id: MempoolAnchorId,
    ) -> Result<()> {
        // will make this only for master blocks
        if !state.block_id().is_masterchain() {
            return Ok(());
        }

        self.detect_top_processed_to_anchor_and_notify_mempool_impl(
            state.block_id().seqno,
            state
                .shards()?
                .as_vec()?
                .into_iter()
                .map(|(_, descr)| descr),
            mc_processed_to_anchor_id,
        )
        .await
    }

    async fn detect_top_processed_to_anchor_and_notify_mempool_impl<I>(
        &self,
        mc_block_seqno: BlockSeqno,
        mc_top_shards: I,
        mc_processed_to_anchor_id: MempoolAnchorId,
    ) -> Result<()>
    where
        I: Iterator<Item = ShardDescriptionShort>,
    {
        let top_processed_to_anchor =
            detect_top_processed_to_anchor(mc_top_shards, mc_processed_to_anchor_id);

        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            top_processed_to_anchor,
            mc_processed_to_anchor_id,
            "detected minimal top_processed_to_anchor, will notify mempool",

        );

        self.notify_top_processed_to_anchor_to_mempool(mc_block_seqno, top_processed_to_anchor)
            .await?;

        Ok(())
    }

    async fn notify_top_processed_to_anchor_to_mempool(
        &self,
        mc_block_seqno: BlockSeqno,
        top_processed_to_anchor: MempoolAnchorId,
    ) -> Result<()> {
        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            top_processed_to_anchor,
            "will notify top_processed_to_anchor to mempool",
        );

        self.mpool_adapter
            .handle_signed_mc_block(mc_block_seqno)
            .await?;

        self.mpool_adapter
            .clear_anchors_cache(top_processed_to_anchor)?;

        Ok(())
    }

    #[tracing::instrument(skip_all, fields(block_id = %state.block_id().as_short_id()))]
    fn cancel_validation_sessions_until_block(&self, state: ShardStateStuff) -> Result<()> {
        self.validator
            .cancel_validation(&state.block_id().as_short_id())?;
        Ok(())
    }

    #[tracing::instrument(skip_all, fields(block_id = %block_id))]
    fn commit_block_queue_diff(
        mq_adapter: Arc<dyn MessageQueueAdapter<EnqueuedMessage>>,
        block_id: &BlockId,
        top_shard_blocks_info: &[(BlockId, bool)],
        partitions: &FastHashSet<QueuePartitionIdx>,
    ) -> Result<()> {
        if !block_id.is_masterchain() {
            return Ok(());
        }

        let _histogram = HistogramGuard::begin("tycho_collator_commit_queue_diffs_time");

        let mut top_blocks: Vec<_> = top_shard_blocks_info
            .iter()
            .map(|(id, updated)| (*id, *updated))
            .collect();
        top_blocks.push((*block_id, true));

        if let Err(err) = mq_adapter.commit_diff(top_blocks, partitions) {
            bail!(
                "Error committing message queue diff of block ({}): {:?}",
                block_id,
                err,
            )
        }

        tracing::info!(target: tracing_targets::COLLATION_MANAGER,
            "message queue diff was committed",
        );

        Ok(())
    }

    /// Returns `BlockId` if diff was applied.
    /// * `first_required_diffs` - contains ids of known first required diffs for queue for each shard
    fn apply_block_queue_diff_from_entry_stuff(
        mq_adapter: Arc<dyn MessageQueueAdapter<EnqueuedMessage>>,
        block_entry: &BlockCacheEntry,
        min_processed_to: Option<&QueueKey>,
        first_required_diffs: &mut FastHashMap<ShardIdent, BlockId>,
    ) -> Result<Option<BlockId>> {
        let block_id = block_entry.block_id;

        if block_id.seqno == 0 {
            return Ok(None);
        }

        let queue_diff = match &block_entry.data {
            BlockCacheEntryData::Collated {
                candidate_stuff, ..
            } => &candidate_stuff.candidate.queue_diff_aug.data,
            BlockCacheEntryData::Received { queue_diff, .. } => queue_diff,
        };

        // skip diff below min processed to
        if let Some(min_pt) = min_processed_to
            && queue_diff.as_ref().max_message <= *min_pt
        {
            tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                "Skipping diff for block {}: max_message {} <= min_processed_to {}",
                block_id.as_short_id(),
                queue_diff.as_ref().max_message,
                min_pt,
            );
            return Ok(None);
        }

        // skip already applied diff
        if mq_adapter.is_diff_exists(&block_id.as_short_id())? {
            tracing::trace!(target: tracing_targets::COLLATION_MANAGER,
                queue_diff_block_id = %block_id.as_short_id(),
                "queue diff apply skipped because it is already applied",
            );
            // if diff for block from bc already applied
            // then we should check sequense for each next diff
            first_required_diffs.insert(block_id.shard, BlockId::default());
            return Ok(None);
        }

        // load out_msg
        let out_msgs = match &block_entry.data {
            BlockCacheEntryData::Collated {
                candidate_stuff, ..
            } => &candidate_stuff
                .candidate
                .block
                .data
                .load_extra()?
                .out_msg_description
                .load()?,
            BlockCacheEntryData::Received { out_msgs, .. } => &out_msgs.load()?,
        };

        let queue_diff_with_msgs = QueueDiffWithMessages::from_queue_diff(queue_diff, out_msgs)?;

        let statistics = DiffStatistics::from_diff(
            &queue_diff_with_msgs,
            queue_diff.block_id().shard,
            queue_diff.as_ref().min_message,
            queue_diff.as_ref().max_message,
        );

        let check_sequence = match first_required_diffs.get(&block_id.shard).copied() {
            None => {
                // if first required diff was not detected before
                // we consider that current is first
                first_required_diffs.insert(block_id.shard, block_id);
                None
            }
            Some(id) if id == block_id => None,
            _ => Some(DiffZone::Both),
        };

        mq_adapter
            .apply_diff(
                queue_diff_with_msgs,
                queue_diff.block_id().as_short_id(),
                queue_diff.diff_hash(),
                statistics,
                check_sequence,
            )
            .context("apply_block_queue_diff_from_entry_stuff")?;

        Ok(Some(block_id))
    }

    #[tracing::instrument(skip_all, fields(next_block_id = %next_block_id_short))]
    async fn handle_collation_cancelled(
        &self,
        _prev_mc_block_id: BlockId,
        next_block_id_short: BlockIdShort,
        cancel_reason: CollationCancelReason,
    ) -> Result<()> {
        tracing::info!(target: tracing_targets::COLLATION_MANAGER,
            ?cancel_reason,
            "start handle collation cancelled",
        );
        match cancel_reason {
            CollationCancelReason::AnchorNotFound(_)
            | CollationCancelReason::NextAnchorNotFound(_)
            | CollationCancelReason::ExternalCancel
            | CollationCancelReason::DiffNotFoundInQueue(_) => {
                // sync cache and collator state access
                self.ready_to_sync.notified().await;
                scopeguard::defer!(self.ready_to_sync.notify_one());

                // mark collator as cancelled
                self.set_collator_state(&next_block_id_short.shard, |ac| {
                    ac.state = CollatorState::Cancelled;
                });

                // run sync if all collators cancelled or waiting
                let has_active = self.active_collators.iter().any(|ac| {
                    ac.state == CollatorState::Active || ac.state == CollatorState::CancelPending
                });
                if !has_active {
                    tracing::info!(target: tracing_targets::COLLATION_MANAGER,
                        "no active collators in shards and masterchain, \
                        will run sync to last applied mc block",
                    );

                    // get info about applied mc blocks in cache
                    let (last_collated_mc_block_id, applied_range) = self
                        .blocks_cache
                        .get_last_collated_block_and_applied_mc_queue_range();

                    // run sync if have applied mc blocks
                    self.sync_to_applied_mc_block_if_exist(
                        last_collated_mc_block_id,
                        applied_range,
                    )
                    .await?;
                }
            }
        }
        Ok(())
    }

    #[tracing::instrument(skip_all, fields(next_block_id = %next_block_id_short, ct = anchor_chain_time, ?force_mc_block))]
    async fn handle_collation_skipped(
        &self,
        prev_mc_block_id: BlockId,
        next_block_id_short: BlockIdShort,
        anchor_chain_time: u64,
        force_mc_block: ForceMasterCollation,
        collation_config: Arc<CollationConfig>,
    ) -> Result<()> {
        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            "will run next collation step",
        );

        // sync cache and collator state access
        self.ready_to_sync.notified().await;
        scopeguard::defer!(self.ready_to_sync.notify_one());

        let updated_collator_state = self.set_collator_state(&next_block_id_short.shard, |ac| {
            if ac.state == CollatorState::CancelPending {
                ac.state = CollatorState::Cancelled;
            }
        });
        if updated_collator_state == Some(CollatorState::Cancelled) {
            tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                shard_id = %next_block_id_short.shard,
                "collator was cancelled before",
            );
            return Ok(());
        }

        self.run_next_collation_step(
            &prev_mc_block_id,
            next_block_id_short.shard,
            None,
            DetectNextCollationStepContext::new(
                anchor_chain_time,
                force_mc_block,
                false,
                collation_config.mc_block_min_interval_ms as _,
                collation_config.mc_block_max_interval_ms as _,
            ),
        )
        .await
    }

    /// 1. Check if should collate master
    /// 2. And schedule master block collation
    /// 3. Or schedule next collation attempt in current shard
    async fn run_next_collation_step(
        &self,
        prev_mc_block_id: &BlockId,
        shard_id: ShardIdent,
        trigger_shard_block_id_opt: Option<BlockId>,
        ctx: DetectNextCollationStepContext,
    ) -> Result<()> {
        let next_step = Self::detect_next_collation_step(
            &mut self.collation_sync_state.lock(),
            self.active_collation_sessions
                .read()
                .keys()
                .cloned()
                .collect(),
            shard_id,
            ctx,
        );

        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            next_step = ?next_step,
            "detected next collation step",
        );

        match next_step {
            NextCollationStep::CollateMaster(next_mc_block_chain_time) => {
                if !shard_id.is_masterchain() {
                    // shard collator will wait and master collator will work
                    self.set_collator_state(&shard_id, |ac| ac.state = CollatorState::Waiting);
                }

                self.set_collator_state(&ShardIdent::MASTERCHAIN, |ac| {
                    ac.state = CollatorState::Active;
                });

                self.enqueue_mc_block_collation(
                    prev_mc_block_id.get_next_id_short(),
                    next_mc_block_chain_time,
                    trigger_shard_block_id_opt,
                )
                .await?;
            }
            NextCollationStep::WaitForMasterStatus => {
                // current shard collator will wait
                self.set_collator_state(&shard_id, |ac| ac.state = CollatorState::Waiting);
            }
            NextCollationStep::ResumeAttemptsIn(shards_to_resume_attempts) => {
                // if should not collate master block
                // then continue collation by running `try_collate` that will
                // - in masterchain: just import next anchor
                // - in workchain: run next attempt to collate shard block
                let mut current_shard_should_wait = true;
                for shard_ident in shards_to_resume_attempts {
                    if shard_ident == shard_id {
                        current_shard_should_wait = false;
                    }
                    self.set_collator_state(&shard_ident, |ac| ac.state = CollatorState::Active);
                    self.enqueue_try_collate(&shard_ident).await?;
                }
                if current_shard_should_wait {
                    self.set_collator_state(&shard_id, |ac| ac.state = CollatorState::Waiting);
                }
            }
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
            "start processing block candidate",
        );

        let _histogram =
            HistogramGuard::begin("tycho_collator_handle_collated_block_candidate_time");

        let block_id = *collation_result.candidate.block.id();
        let candidate_chain_time = collation_result.candidate.chain_time;
        let consensus_config_changed = collation_result.candidate.consensus_config_changed;

        debug_assert_eq!(
            block_id.is_masterchain(),
            collation_result.mc_data.is_some(),
        );

        // sync cache and collator state access
        self.ready_to_sync.notified().await;
        scopeguard::defer!(self.ready_to_sync.notify_one());

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
                    "there is no active session related to collated block. Skipped",
                );

                self.set_collator_state(&block_id.shard, |ac| ac.state = CollatorState::Waiting);

                return Ok(());
            }
        };

        let updated_collator_state = self.set_collator_state(&block_id.shard, |ac| {
            if ac.state == CollatorState::CancelPending {
                ac.state = CollatorState::Cancelled;
            }
        });
        let collator_cancelled = updated_collator_state == Some(CollatorState::Cancelled);

        let store_res = if collator_cancelled {
            tracing::info!(target: tracing_targets::COLLATION_MANAGER,
                shard_id = %block_id.shard,
                "collator was cancelled before",
            );

            let top_shards = self.blocks_cache.get_last_top_shards();
            self.mq_adapter.clear_uncommitted_state(&top_shards)?;

            let (last_collated_mc_block_id, applied_mc_queue_range) = self
                .blocks_cache
                .get_last_collated_block_and_applied_mc_queue_range();

            let store_res = BlockCacheStoreResult {
                received_and_collated: false,
                block_mismatch: false,
                last_collated_mc_block_id,
                applied_mc_queue_range,
            };

            tracing::info!(
                target: tracing_targets::COLLATION_MANAGER,
                ?store_res,
                "block candidate was not saved to cache",
            );

            store_res
        } else {
            let top_shard_blocks_info = collation_result
                .mc_data
                .as_ref()
                .map(|mc_data| {
                    mc_data
                        .shards
                        .iter()
                        .map(|(shard_id, shard_descr)| {
                            (
                                shard_descr.get_block_id(*shard_id),
                                shard_descr.top_sc_block_updated,
                            )
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            let top_processed_to_anchor = collation_result
                .mc_data
                .as_ref()
                .map(|mc_data| mc_data.top_processed_to_anchor);
            let store_res = self.blocks_cache.store_collated(
                collation_result.candidate,
                top_shard_blocks_info,
                top_processed_to_anchor,
            )?;

            if store_res.block_mismatch {
                let labels = [("workchain", block_id.shard.workchain().to_string())];
                metrics::counter!("tycho_collator_block_mismatch_count", &labels).increment(1);

                self.set_collator_state(&block_id.shard, |ac| ac.state = CollatorState::Cancelled);

                // when master block mismatched then should cancel shard collators as well
                if block_id.is_masterchain() {
                    for mut ac in self
                        .active_collators
                        .iter_mut()
                        .filter(|ac| ac.key() != &block_id.shard)
                    {
                        ac.state = match ac.state {
                            CollatorState::Waiting | CollatorState::Cancelled => {
                                CollatorState::Cancelled
                            }
                            _ => CollatorState::CancelPending,
                        };
                    }
                }

                let top_shards = self.blocks_cache.get_last_top_shards();
                self.mq_adapter.clear_uncommitted_state(&top_shards)?;

                tracing::info!(
                    target: tracing_targets::COLLATION_MANAGER,
                    ?store_res,
                    "saved block candidate to cache",
                );
            } else {
                tracing::debug!(
                    target: tracing_targets::COLLATION_MANAGER,
                    ?store_res,
                    "saved block candidate to cache",
                );
            }

            store_res
        };

        let collation_cancelled = collator_cancelled || store_res.block_mismatch;

        // check if should sync to last applied mc block
        let should_sync_to_last_applied_mc_block = 'check_should_sync: {
            // do not sync to last applied mc block if newer already received
            if let Some((_, applied_range_end)) = store_res.applied_mc_queue_range {
                let last_received_mc_block_seqno = self.get_last_received_mc_block_seqno();
                if matches!(
                    last_received_mc_block_seqno,
                    Some(last_received_seqno) if last_received_seqno.saturating_sub(applied_range_end) > 1
                ) {
                    tracing::info!(target: tracing_targets::COLLATION_MANAGER,
                        last_received_mc_block_seqno,
                        applied_range_end,
                        "check_should_sync: should not sync to last applied mc block \
                        because a newer one already received",
                    );
                    break 'check_should_sync false;
                }
            }

            if collation_cancelled {
                true
            } else if let Some((_, applied_range_end)) = store_res.applied_mc_queue_range {
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

        // run sync or process just collated block
        if should_sync_to_last_applied_mc_block {
            // NOTE: we do not need to commit current master block candidate
            //      if it is "received and collated" because it is already applied to state
            //      and we do not need to notify state update and top processed to anchor
            //      because we will do this for block wich we sync to

            if block_id.is_masterchain() {
                // run sync if have applied mc blocks
                self.sync_to_applied_mc_block_if_exist(
                    store_res.last_collated_mc_block_id,
                    store_res.applied_mc_queue_range,
                )
                .await?;
            } else {
                // cancel further collation of blocks in the current shard because we need to sync
                tracing::info!(target: tracing_targets::COLLATION_MANAGER,
                    "sync_to_applied_mc_block: mark shard collator Cancelled for shard",
                );

                self.set_collator_state(&block_id.shard, |ac| ac.state = CollatorState::Cancelled);

                // run sync if all collators cancelled, or waiting, or there are no collators
                // and we have applied mc blocks
                let has_active = self.active_collators.iter().any(|ac| {
                    ac.state == CollatorState::Active || ac.state == CollatorState::CancelPending
                });
                if !has_active {
                    tracing::info!(target: tracing_targets::COLLATION_MANAGER,
                        "sync_to_applied_mc_block: no active collators in shards and master, \
                        will run sync to last applied mc block",
                    );

                    // run sync if have applied mc blocks
                    self.sync_to_applied_mc_block_if_exist(
                        store_res.last_collated_mc_block_id,
                        store_res.applied_mc_queue_range,
                    )
                    .await?;
                }
            }
        } else if block_id.is_masterchain() {
            // when candidate is master

            // if consensus config was changed we should wait until master block is validated
            if consensus_config_changed == Some(true) {
                let mut delayed_mc_state_update = self.delayed_mc_state_update.lock();
                *delayed_mc_state_update = collation_result.mc_data.clone();
            } else {
                // otherwise we can notify state update to mempool right now
                self.notify_mc_state_update_to_mempool(collation_result.mc_data.clone().unwrap())
                    .await?;
            }

            // process validation
            if store_res.received_and_collated {
                // NOTE: here commit will not cause on_block_accepted event
                //      because block already exist in bc state

                self.commit_valid_master_block(&block_id).await?;
            } else {
                let validator = self.validator.clone();
                let validation_session_id = session_info.get_validation_session_id();
                let dispatcher = self.dispatcher.clone();
                tokio::spawn(async move {
                    let validation_result =
                        validator.validate(validation_session_id, &block_id).await;

                    match validation_result {
                        Ok(status) => {
                            _ = dispatcher
                                .spawn_task(method_to_async_closure!(
                                    handle_validated_master_block,
                                    block_id,
                                    status
                                ))
                                .await;
                        }
                        Err(e) => {
                            tracing::error!(target: tracing_targets::COLLATION_MANAGER,
                                "block candidate validation failed: {e:?}",
                            );
                        }
                    }
                });
            }

            // if consensus config was not changed execute master state update processing routines right now
            if consensus_config_changed != Some(true) {
                self.process_mc_state_update(
                    collation_result.mc_data.unwrap(),
                    ProcessMcStateUpdateMode::StartCollation {
                        reset_collators: false,
                    },
                )
                .await?;
            }
        } else {
            // when candidate is shard

            // run master block collation if required or resume collation attempts in shard
            tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                "will run next collation step",
            );

            self.run_next_collation_step(
                &collation_result.prev_mc_block_id,
                block_id.shard,
                Some(block_id),
                DetectNextCollationStepContext::new(
                    candidate_chain_time,
                    collation_result.force_next_mc_block,
                    collation_result.is_first_block_after_prev_master,
                    collation_result.collation_config.mc_block_min_interval_ms as _,
                    collation_result.collation_config.mc_block_max_interval_ms as _,
                ),
            )
            .await?;
        }

        Ok(())
    }

    /// Finish active sync if it is not finished yet
    /// and enqueue received block
    #[tracing::instrument(skip_all, fields(block_id = %ctx.state.block_id().as_short_id()))]
    pub async fn enqueue_handle_block_from_bc(&self, ctx: HandledBlockFromBcCtx) -> Result<()> {
        // TODO: Needs to redesign the task management logic.
        //      Current implementation with strange semaphores
        //      is unclear and may be confusing.

        tracing::trace!(target: tracing_targets::COLLATION_MANAGER,
            "cancel sync: enqueue_handle_block_from_bc: started",
        );

        let labels = [
            (
                "workchain",
                ctx.state.block_id().shard.workchain().to_string(),
            ),
            ("src", "01_received".to_string()),
        ];
        metrics::gauge!("tycho_last_block_seqno", &labels).set(ctx.state.block_id().seqno);

        self.update_last_received_mc_block_seqno(ctx.state.block_id());

        // try to finish active sync
        self.finish_active_sync_to_applied(ctx.state.block_id());

        // enqueue received block from processing
        self.blocks_from_bc_queue_sender.send(ctx).await?;

        Ok(())
    }

    /// Process the queue of received blocks from bc
    #[tracing::instrument(skip_all)]
    async fn process_handle_block_from_bc_queue(
        self: Arc<Self>,
        mut blocks_from_bc_queue_receiver: tokio::sync::mpsc::Receiver<HandledBlockFromBcCtx>,
        cancel_task: CancellationToken,
    ) -> Result<()> {
        const BATCH_SIZE: usize = 300;

        tracing::info!(target: tracing_targets::COLLATION_MANAGER,
            "started",
        );

        loop {
            let mut batch = Vec::with_capacity(BATCH_SIZE);
            tokio::select! {
                received_count = blocks_from_bc_queue_receiver.recv_many(&mut batch, BATCH_SIZE) => {
                    if received_count == 0 {
                        tracing::info!(target: tracing_targets::COLLATION_MANAGER,
                            "channel closed",
                        );
                        break;
                    }

                    // find last master block in buffer
                    // will skip sync for all master blocks before it
                    let mut last_mc_block_id_opt = None;
                    for HandledBlockFromBcCtx {state, ..} in batch.iter().rev() {
                        if state.block_id().is_masterchain() {
                            last_mc_block_id_opt = Some(*state.block_id());
                        }
                    }

                    for ctx in batch {
                        let is_last_mc_block_in_batch = matches!(
                            last_mc_block_id_opt, Some(last_mc_block_id) if ctx.state.block_id() == &last_mc_block_id
                        );

                        // handle block from bc
                        self.handle_block_from_bc(ctx, is_last_mc_block_in_batch).await.map_err(|err| {
                            tracing::error!(target: tracing_targets::COLLATION_MANAGER,
                                "error handling block from bc: {err:?}",
                            );
                            err
                        })?;
                    }
                },
                _ = cancel_task.cancelled() => {
                    tracing::info!(target: tracing_targets::COLLATION_MANAGER,
                        "cancelled",
                    );
                    break;
                }
            }
        }

        tracing::info!(target: tracing_targets::COLLATION_MANAGER,
            "finished",
        );

        Ok(())
    }

    /// Process new block from blockchain:
    /// 1. Save block to cache
    /// 2. Stop block validation if needed
    /// 3. Commit block if it was collated first
    /// 4. Notify mempool about new master block
    /// 5. Sync to received block if it is far ahead last collated and last `synced_to`
    #[tracing::instrument(skip_all, fields(block_id = %ctx.state.block_id().as_short_id(), is_last_mc_block_in_batch))]
    async fn handle_block_from_bc(
        &self,
        ctx: HandledBlockFromBcCtx,
        is_last_mc_block_in_batch: bool,
    ) -> Result<()> {
        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            "start processing block from bc",
        );

        let block_id = *ctx.state.block_id();

        let _histogram = HistogramGuard::begin("tycho_collator_handle_block_from_bc_time");

        let state = ctx.state;

        // sync cache and collator state access
        self.ready_to_sync.notified().await;
        scopeguard::defer!(self.ready_to_sync.notify_one());

        let processed_upto = ProcessedUptoInfoStuff::try_from(ctx.processed_upto)?;

        let Some(store_res) = self
            .blocks_cache
            .store_received(
                self.state_node_adapter.clone(),
                state.clone(),
                processed_upto,
            )
            .await?
        else {
            return Ok(());
        };

        if store_res.block_mismatch {
            let labels = [("workchain", block_id.shard.workchain().to_string())];
            metrics::counter!("tycho_collator_block_mismatch_count", &labels).increment(1);

            self.set_collator_state(&block_id.shard, |ac| {
                ac.state = match ac.state {
                    CollatorState::Waiting | CollatorState::Cancelled => CollatorState::Cancelled,
                    _ => CollatorState::CancelPending,
                };
            });

            // when master block mismatched then should cancel shard collators as well
            if block_id.is_masterchain() {
                for mut ac in self
                    .active_collators
                    .iter_mut()
                    .filter(|ac| ac.key() != &block_id.shard)
                {
                    ac.state = match ac.state {
                        CollatorState::Waiting | CollatorState::Cancelled => {
                            CollatorState::Cancelled
                        }
                        _ => CollatorState::CancelPending,
                    };
                }
            }

            let top_shards = self.blocks_cache.get_last_top_shards();
            self.mq_adapter.clear_uncommitted_state(&top_shards)?;

            tracing::info!(target: tracing_targets::COLLATION_MANAGER,
                ?store_res,
                "saved block from bc to cache",
            );
        } else {
            tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                ?store_res,
                "saved block from bc to cache",
            );
        }

        if !block_id.is_masterchain() {
            return Ok(());
        }

        // when received block is master
        let is_key_block = state.state_extra()?.after_key_block;

        // stop any running validations up to this block
        self.validator.cancel_validation(&block_id.as_short_id())?;

        // check if should sync to last applied mc block right now
        let should_sync_to_last_applied_mc_block = 'check_should_sync: {
            // sync only last master block from batch or key block
            if !is_last_mc_block_in_batch && !is_key_block {
                break 'check_should_sync false;
            }

            // do not sync to last applied mc block if newer already received
            if let Some((_, applied_range_end)) = store_res.applied_mc_queue_range {
                let last_received_mc_block_seqno = self.get_last_received_mc_block_seqno();
                if matches!(
                    last_received_mc_block_seqno,
                    Some(last_received_seqno) if last_received_seqno.saturating_sub(applied_range_end) > 1
                ) {
                    tracing::info!(target: tracing_targets::COLLATION_MANAGER,
                        last_received_mc_block_seqno,
                        received_is_key_block = is_key_block,
                        "check_should_sync: should not sync to last applied mc block \
                        because a newer one already received",
                    );
                    break 'check_should_sync false;
                }
            }

            if let Some(top_mc_block_id_for_next_collation) =
                self.get_top_mc_block_id_for_next_collation(store_res.last_collated_mc_block_id)
            {
                // we can sync only when we have any applied block ahead
                if let Some((_, applied_range_end)) = store_res.applied_mc_queue_range {
                    // check if should sync according to master block delta
                    let should_sync = {
                        let applied_range_end_delta = applied_range_end
                            .saturating_sub(top_mc_block_id_for_next_collation.seqno);

                        // we should sync to every key block if node is not in current vset
                        let required_min_mc_block_delta = if is_key_block
                            && !self.active_collators.contains_key(&ShardIdent::MASTERCHAIN)
                        {
                            1
                        } else {
                            self.config.min_mc_block_delta_from_bc_to_sync
                        };

                        if applied_range_end_delta < required_min_mc_block_delta {
                            tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                                last_synced_to_mc_block_id = ?self.get_last_synced_to_mc_block_id().map(|id| id.as_short_id().to_string()),
                                last_collated_mc_block_id = ?store_res.last_collated_mc_block_id.map(|id| id.as_short_id().to_string()),
                                last_processed_mc_block_id = ?self.get_last_processed_mc_block_id().map(|id| id.as_short_id().to_string()),
                                received_is_key_block = is_key_block,
                                "check_should_sync: should wait for next collated own mc block: \
                                last applied ({}) ahead of top for collation ({}) on {} < {}",
                                applied_range_end, top_mc_block_id_for_next_collation.seqno,
                                applied_range_end_delta, required_min_mc_block_delta,
                            );
                            false
                        } else {
                            tracing::info!(target: tracing_targets::COLLATION_MANAGER,
                                last_synced_to_mc_block_id = ?self.get_last_synced_to_mc_block_id().map(|id| id.as_short_id().to_string()),
                                last_collated_mc_block_id = ?store_res.last_collated_mc_block_id.map(|id| id.as_short_id().to_string()),
                                last_processed_mc_block_id = ?self.get_last_processed_mc_block_id().map(|id| id.as_short_id().to_string()),
                                received_is_key_block = is_key_block,
                                "check_should_sync: should sync to last applied mc block from bc: \
                                last applied ({}) ahead of top for collation ({}) on {} >= {}",
                                applied_range_end, top_mc_block_id_for_next_collation.seqno,
                                applied_range_end_delta, required_min_mc_block_delta,
                            );
                            true
                        }
                    };

                    if should_sync {
                        // we should sync but we can run sync right now only when there are no active collators
                        let mut has_active = false;
                        for active_collator in self.active_collators.iter().filter(|ac| {
                            ac.state == CollatorState::Active
                                || ac.state == CollatorState::CancelPending
                        }) {
                            // try to gracefully cancel active collations
                            active_collator.cancel_collation.notify_one();
                            has_active = true;
                        }

                        if has_active {
                            tracing::info!(target: tracing_targets::COLLATION_MANAGER,
                                last_synced_to_mc_block_id = ?self.get_last_synced_to_mc_block_id().map(|id| id.as_short_id().to_string()),
                                last_collated_mc_block_id = ?store_res.last_collated_mc_block_id.map(|id| id.as_short_id().to_string()),
                                last_processed_mc_block_id = ?self.get_last_processed_mc_block_id().map(|id| id.as_short_id().to_string()),
                                received_is_key_block = is_key_block,
                                "check_should_sync: cannot sync when there are active collations, \
                                try to gracefully cancel them",
                            );
                            false
                        } else {
                            tracing::info!(target: tracing_targets::COLLATION_MANAGER,
                                last_synced_to_mc_block_id = ?self.get_last_synced_to_mc_block_id().map(|id| id.as_short_id().to_string()),
                                last_collated_mc_block_id = ?store_res.last_collated_mc_block_id.map(|id| id.as_short_id().to_string()),
                                last_processed_mc_block_id = ?self.get_last_processed_mc_block_id().map(|id| id.as_short_id().to_string()),
                                received_is_key_block = is_key_block,
                                "check_should_sync: can sync to last applied mc block \
                                when all collators were cancelled, or waiting, or there are no collators (node not in set)",
                            );
                            true
                        }
                    } else {
                        false
                    }
                } else {
                    // should collate next own mc block because no applied ahead
                    tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                        last_synced_to_mc_block_id = ?self.get_last_synced_to_mc_block_id().map(|id| id.as_short_id().to_string()),
                        last_collated_mc_block_id = ?store_res.last_collated_mc_block_id.map(|id| id.as_short_id().to_string()),
                        last_processed_mc_block_id = ?self.get_last_processed_mc_block_id().map(|id| id.as_short_id().to_string()),
                        "check_should_sync: should collate next own mc block after because nothing applied ahead",
                    );
                    false
                }
            } else {
                tracing::info!(target: tracing_targets::COLLATION_MANAGER,
                    received_is_key_block = is_key_block,
                    "should sync to last applied mc block when no last collated or prev sync to",
                );
                true
            }
        };

        if should_sync_to_last_applied_mc_block {
            // run sync if have applied mc blocks
            self.sync_to_applied_mc_block_if_exist(
                store_res.last_collated_mc_block_id,
                store_res.applied_mc_queue_range,
            )
            .await?;
        } else {
            // try to commit block if it was collated first
            if store_res.received_and_collated {
                // NOTE: here commit will not cause on_block_accepted event
                //      because block already exist in bc state

                // NOTE: here master block subgraph could be already extracted,
                //      sent to sync, and removed from cache, because validation task
                //      could be finished after `store_received()` but before this point.

                self.commit_valid_master_block(&block_id).await?;
            }
        }

        Ok(())
    }

    async fn sync_to_applied_mc_block_if_exist(
        &self,
        last_collated_mc_block_id: Option<BlockId>,
        applied_range: Option<(BlockSeqno, BlockSeqno)>,
    ) -> Result<()> {
        if let Some(applied_range) = applied_range {
            if !self
                .sync_to_applied_mc_block(applied_range, last_collated_mc_block_id)
                .await?
            {
                tracing::info!(target: tracing_targets::COLLATION_MANAGER,
                    last_applied_mc_block_id = %BlockIdShort {
                        shard: ShardIdent::MASTERCHAIN,
                        seqno: applied_range.1,
                    },
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

    /// Restores internals queue state,
    /// processes last applied mc state
    /// to run next blocks collation.
    ///
    /// Returns `false` if unable to
    /// load diff to restore queue state
    #[tracing::instrument(skip_all, fields(applied_range = ?applied_range))]
    async fn sync_to_applied_mc_block(
        &self,
        applied_range: (BlockSeqno, BlockSeqno),
        last_collated_mc_block_id: Option<BlockId>,
    ) -> Result<bool> {
        tracing::info!(target: tracing_targets::COLLATION_MANAGER,
            "start sync to applied mc block",
        );

        let _histogram = HistogramGuard::begin("tycho_collator_sync_to_applied_mc_block_time");
        metrics::counter!("tycho_collator_sync_to_applied_mc_block_count").increment(1);

        let first_applied_mc_block_key = BlockIdShort {
            shard: ShardIdent::MASTERCHAIN,
            seqno: applied_range.0,
        };
        let last_applied_mc_block_key = BlockIdShort {
            shard: ShardIdent::MASTERCHAIN,
            seqno: applied_range.1,
        };

        // store active sync info with cancellation token
        let cancelled = self.set_active_sync_info(last_applied_mc_block_key.seqno)?;
        tracing::trace!(target: tracing_targets::COLLATION_MANAGER,
            "cancel sync: sync_to_applied_mc_block: set_active_sync_info for seqno={}",
            last_applied_mc_block_key.seqno,
        );
        scopeguard::defer!({
            self.clean_active_sync_info();
            tracing::trace!(target: tracing_targets::COLLATION_MANAGER,
                "cancel sync: sync_to_applied_mc_block: active_sync_info cleaned",
            );
        });

        // gc blocks from cache when sync finished
        scopeguard::defer!(self.blocks_cache.gc_prev_blocks());

        // we need to drop uncommitted internal messages from the queue
        // when mempool config override has genesis higher then in the last consensus info
        if let Some(mp_cfg_override) = &self.mempool_config_override {
            let last_consesus_info = self
                .blocks_cache
                .get_consensus_info_for_mc_block(&last_applied_mc_block_key)?;
            if mp_cfg_override
                .genesis_info
                .overrides(&last_consesus_info.genesis_info)
            {
                tracing::info!(target: tracing_targets::COLLATION_MANAGER,
                    prev_genesis_start_round = last_consesus_info.genesis_info.start_round,
                    prev_genesis_time_millis = last_consesus_info.genesis_info.genesis_millis,
                    new_genesis_start_round = mp_cfg_override.genesis_info.start_round,
                    new_genesis_time_millis = mp_cfg_override.genesis_info.genesis_millis,
                    "will drop uncommitted internal messages from queue on new genesis",
                );

                let top_shards = self.blocks_cache.get_last_top_shards();
                self.mq_adapter.clear_uncommitted_state(&top_shards)?;
            }
        }

        // get internals processed_to from master and all shards
        // for last applied master block
        let all_shards_processed_to_by_partitions =
            Self::get_all_shards_processed_to_by_partitions_for_mc_block(
                &last_applied_mc_block_key,
                &self.blocks_cache,
                self.state_node_adapter.clone(),
            )
            .await?;

        // find internals min processed_to
        let min_processed_to_by_shards =
            find_min_processed_to_by_shards(&all_shards_processed_to_by_partitions);

        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            ?min_processed_to_by_shards,
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

        // metrics - sync is running
        for shard in before_tail_block_ids.keys() {
            let labels = [("workchain", shard.workchain().to_string())];
            metrics::gauge!("tycho_collator_sync_is_running", &labels).set(1);
        }

        // if `fast_sync` is enabled then we will skip already applied queue diffs
        let queue_diffs_applied_to_top_blocks = if self.config.fast_sync {
            // BACKWARD COMPATIBILITY: `last_committed_mc_block_id` will not exist in queue storage
            // in previous version. We will receive None and will use all required diffs to restore the queue
            if let Some(applied_to_mc_block_id) =
                self.get_queue_diffs_applied_to_mc_block_id(last_collated_mc_block_id)?
            {
                // collect top blocks queue diffs already applied to
                Self::get_top_blocks_seqno(
                    &applied_to_mc_block_id,
                    &self.blocks_cache,
                    self.state_node_adapter.clone(),
                )
                .await?
            } else {
                FastHashMap::default()
            }
        } else {
            FastHashMap::default()
        };

        // restore queue state and return latest applied master state
        let queue_restore_res = Self::restore_queue(
            &self.blocks_cache,
            self.state_node_adapter.clone(),
            self.mq_adapter.clone(),
            applied_range.0,
            min_processed_to_by_shards,
            before_tail_block_ids,
            queue_diffs_applied_to_top_blocks,
        )
        .await?;

        let Some(last_mc_state) = queue_restore_res.last_mc_state else {
            return Ok(false);
        };

        // process latest master state: notify mempool and refresh collation session

        // HACK: do not need to set master block latest chain time from zerostate when using mempool stub
        //      because anchors from stub have older chain time than in zerostate and it will brake collation
        if last_mc_state.block_id().seqno != 0 {
            Self::renew_mc_block_latest_chain_time(
                &mut self.collation_sync_state.lock(),
                last_mc_state.get_gen_chain_time(),
            );
        }

        Self::reset_collation_sync_status(&mut self.collation_sync_state.lock());

        // update last "synced to" info
        self.update_last_synced_to_mc_block_id(*last_mc_state.block_id());

        // reset top shard blocks info
        // because next we will start to collate new shard blocks after the sync
        self.blocks_cache.reset_top_shard_blocks_additional_info();

        let mc_data =
            McData::load_from_state(&last_mc_state, all_shards_processed_to_by_partitions)?;

        self.blocks_cache
            .remove_next_collated_blocks_from_cache(&queue_restore_res.synced_to_blocks_keys);

        // notify state update to mempool
        self.notify_mc_state_update_to_mempool(mc_data.clone())
            .await?;

        // when sync cancelled we do not exist sync but skip collation
        let process_state_update_mode = match cancelled.is_cancelled() {
            true => ProcessMcStateUpdateMode::SkipCollation,
            false => ProcessMcStateUpdateMode::StartCollation {
                reset_collators: true,
            },
        };

        self.process_mc_state_update(mc_data.clone(), process_state_update_mode)
            .await?;

        // handle top processed to anchor in mempool
        self.notify_top_processed_to_anchor_to_mempool(
            mc_data.block_id.seqno,
            mc_data.top_processed_to_anchor,
        )
        .await?;

        // report last "synced to" blocks to metrics
        for synced_to_block_id in queue_restore_res.synced_to_blocks_keys {
            let labels = [
                (
                    "workchain",
                    synced_to_block_id.shard.workchain().to_string(),
                ),
                ("src", "02_synced_to".to_string()),
            ];
            metrics::gauge!("tycho_last_block_seqno", &labels).set(synced_to_block_id.seqno);
        }

        Ok(true)
    }

    async fn get_all_shards_processed_to_by_partitions_for_mc_block(
        mc_block_key: &BlockCacheKey,
        blocks_cache: &BlocksCache,
        state_node_adapter: Arc<dyn StateNodeAdapter>,
    ) -> Result<FastHashMap<ShardIdent, (bool, ProcessedToByPartitions)>> {
        let mut result = FastHashMap::default();

        if mc_block_key.seqno == 0 {
            return Ok(result);
        }

        let from_cache = blocks_cache.get_top_blocks_processed_to_by_partitions(mc_block_key)?;

        for (top_block_id, (updated, processed_to_opt)) in from_cache {
            let processed_to = match processed_to_opt {
                Some(processed_to) => processed_to,
                None => {
                    if top_block_id.seqno == 0 {
                        FastHashMap::default()
                    } else {
                        // get from state
                        let state = state_node_adapter
                            .load_state(mc_block_key.seqno, &top_block_id)
                            .await?;
                        let processed_upto = state.state().processed_upto.load()?;
                        let processed_upto = ProcessedUptoInfoStuff::try_from(processed_upto)?;
                        processed_upto.get_internals_processed_to_by_partitions()
                    }
                }
            };

            result.insert(top_block_id.shard, (updated, processed_to));
        }

        Ok(result)
    }

    // Returns top master block id upto which all queue diffs applied
    fn get_queue_diffs_applied_to_mc_block_id(
        &self,
        last_collated_mc_block_id: Option<BlockId>,
    ) -> Result<Option<BlockId>> {
        match self.get_top_mc_block_id_for_next_collation(last_collated_mc_block_id) {
            // return last collated if it exists or last "synced to"
            Some(block_id) => Ok(Some(block_id)),
            // otherwise we just started and do not have stored last collated or "synced to"
            // in this case we read last master block id comitted into queue
            None => self.mq_adapter.get_last_commited_mc_block_id(),
        }
    }

    /// Return last collated if it is ahead of last received and "synced to".
    /// Or last received and "synced to" if it is ahead of last correct collated.
    /// Last collated may be incorrect but we don not know this until we receive block from bc.
    /// We use this to detect the lag from last received to check if we need to run next sync.
    fn get_top_mc_block_id_for_next_collation(
        &self,
        last_collated_mc_block_id: Option<BlockId>,
    ) -> Option<BlockId> {
        let last_synced_to_mc_block_id = self.get_last_synced_to_mc_block_id();
        match (last_synced_to_mc_block_id, last_collated_mc_block_id) {
            (Some(last_synced_to), Some(last_collated)) => {
                if last_synced_to.seqno > last_collated.seqno {
                    Some(last_synced_to)
                } else {
                    Some(last_collated)
                }
            }
            (Some(mc_block_id), _) | (_, Some(mc_block_id)) => Some(mc_block_id),
            _ => None,
        }
    }

    #[tracing::instrument(skip_all, fields(from_mc_block_seqno))]
    async fn restore_queue(
        blocks_cache: &BlocksCache,
        state_node_adapter: Arc<dyn StateNodeAdapter>,
        mq_adapter: Arc<dyn MessageQueueAdapter<EnqueuedMessage>>,
        from_mc_block_seqno: u32,
        min_processed_to_by_shards: BTreeMap<ShardIdent, QueueKey>,
        before_tail_block_ids: BTreeMap<ShardIdent, (Option<BlockId>, Vec<BlockId>)>,
        queue_diffs_applied_to_top_blocks: FastHashMap<ShardIdent, u32>,
    ) -> Result<RestoreQueueResult> {
        let mut res = RestoreQueueResult::default();

        // load init block (from persistent state) to check if required diff was already applied from persistent
        let init_mc_block_id = state_node_adapter.load_init_block_id();
        let mut init_mc_block_reached_on = FastHashMap::new();

        // try load required previous queue diffs
        let mut first_required_diffs = FastHashMap::new();
        for (shard_id, min_processed_to) in &min_processed_to_by_shards {
            let mut prev_queue_diffs = vec![];
            let Some((_, prev_block_ids)) = before_tail_block_ids.get(shard_id) else {
                continue;
            };
            let mut prev_block_ids: VecDeque<_> = prev_block_ids.iter().cloned().collect();

            while let Some(prev_block_id) = prev_block_ids.pop_front() {
                if prev_block_id.seqno == 0 {
                    continue;
                }

                // if diff is below top applied then skip
                if let Some(border) = queue_diffs_applied_to_top_blocks.get(shard_id)
                    && prev_block_id.seqno <= *border
                {
                    tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                        prev_block_id = %prev_block_id.as_short_id(),
                        top_applied_seqno = border,
                        "previous queue diff skipped because it below top applied",
                    );
                    continue;
                }

                // if diff is before init block (from persistent state)
                // we do not need to apply it because queue was already restored from persistent
                if let Some(init_mc_block_id) = init_mc_block_id {
                    let mut skip_diff = false;
                    if prev_block_id.is_masterchain() {
                        if prev_block_id.seqno <= init_mc_block_id.seqno {
                            tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                                prev_block_id = %prev_block_id.as_short_id(),
                                init_mc_block_id = %init_mc_block_id.as_short_id(),
                                "master block queue diff apply skipped because it is below init block from persistent state",
                            );
                            skip_diff = true;
                        }
                    } else {
                        // check if we already reached init mc block before
                        let mut init_mc_block_reached = matches!(
                            init_mc_block_reached_on.get(&prev_block_id.shard),
                            Some(reached_seqno) if prev_block_id.seqno <= *reached_seqno,
                        );
                        if !init_mc_block_reached {
                            // for shard block we should check it's `ref_by_mc_seqno`
                            let prev_ref_by_mc_seqno = state_node_adapter
                                .get_ref_by_mc_seqno(&prev_block_id)
                                .await?
                                .unwrap();
                            init_mc_block_reached = prev_ref_by_mc_seqno <= init_mc_block_id.seqno;
                            if init_mc_block_reached {
                                init_mc_block_reached_on
                                    .insert(prev_block_id.shard, prev_block_id.seqno);
                            }
                        }
                        if init_mc_block_reached {
                            tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                                prev_block_id = %prev_block_id.as_short_id(),
                                init_mc_block_id = %init_mc_block_id.as_short_id(),
                                "shard block queue diff apply skipped because it is below init block from persistent state",
                            );
                            skip_diff = true;
                        }
                    }
                    if skip_diff {
                        // if current diff is below init block
                        // then we should check sequense for each next diff
                        first_required_diffs.insert(prev_block_id.shard, BlockId::default());
                        continue;
                    }
                }

                // load diff to check if it is required
                let Some(queue_diff_stuff) = state_node_adapter.load_diff(&prev_block_id).await?
                else {
                    tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                        prev_block_id = %prev_block_id,
                        "unable to load prev diff to sync queue state, cancel sync",
                    );

                    // metrics - sync finished
                    for shard in before_tail_block_ids.keys() {
                        let labels = [("workchain", shard.workchain().to_string())];
                        metrics::gauge!("tycho_collator_sync_is_running", &labels).set(0);
                    }

                    return Ok(res);
                };
                let diff_required = &queue_diff_stuff.as_ref().max_message > min_processed_to;
                tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                    diff_block_id = %prev_block_id.as_short_id(),
                    diff_required,
                    max_message = %queue_diff_stuff.as_ref().max_message,
                    min_processed_to = %min_processed_to,
                    "check if diff required to restore queue working state on sync:",
                );
                if diff_required {
                    // if next diff is not required
                    // then current will be the first required
                    first_required_diffs.insert(prev_block_id.shard, prev_block_id);

                    let block_stuff = state_node_adapter
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
                        queue_diff_stuff.as_ref().min_message,
                        queue_diff_stuff.as_ref().max_message,
                    ));

                    let prev_ids_info = block_stuff.construct_prev_id()?;
                    prev_block_ids.push_back(prev_ids_info.0);
                    if let Some(id) = prev_ids_info.1 {
                        prev_block_ids.push_back(id);
                    }
                }
            }

            // apply required previous queue diffs for each shard
            while let Some((diff, diff_hash, block_id, min_message, max_message)) =
                prev_queue_diffs.pop()
            {
                let statistics =
                    DiffStatistics::from_diff(&diff, block_id.shard, min_message, max_message);

                // we can skip the sequense check for the first required diff only
                let check_sequence = match first_required_diffs.get(&block_id.shard) {
                    Some(id) if *id == block_id => None,
                    _ => Some(DiffZone::Both),
                };

                mq_adapter
                    .apply_diff(
                        diff,
                        block_id.as_short_id(),
                        &diff_hash,
                        statistics,
                        check_sequence,
                    )
                    .context("sync_to_applied_mc_block")?;

                res.applied_diffs_ids.insert(block_id);
            }
        }

        // extract all recevied blocks, apply required diffs
        // and return latest master state
        loop {
            // pop first applied mc block and sync
            // actually we can sync more mc blocks than known in applied_range
            // because we can receive new blocks from bc during sync
            let (mc_block_subgraph_extract, is_last) =
                blocks_cache.pop_front_applied_mc_block_subgraph(from_mc_block_seqno)?;

            let subgraph = match mc_block_subgraph_extract {
                McBlockSubgraphExtract::Extracted(subgraph) => subgraph,
                McBlockSubgraphExtract::AlreadyExtracted => {
                    bail!("mc block subgraph extract result cannot be AlreadyExtracted")
                }
            };

            let mc_block_entry = &subgraph.master_block;

            // apply queue diffs from blocks above 0
            // skip cached diffs below min_processed_to
            if subgraph.master_block.block_id.seqno != 0 {
                for block_entry in [mc_block_entry]
                    .into_iter()
                    .chain(subgraph.shard_blocks.iter())
                {
                    // if diff is below top applied then skip
                    if let Some(border) =
                        queue_diffs_applied_to_top_blocks.get(&block_entry.block_id.shard)
                        && block_entry.block_id.seqno <= *border
                    {
                        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                            received_block_id = %block_entry.block_id.as_short_id(),
                            top_applied_seqno = border,
                            "queue diff apply skipped because it is below top applied",
                        );
                        continue;
                    }

                    let min_processed_to =
                        min_processed_to_by_shards.get(&block_entry.block_id.shard);

                    if let Some(applied_diff_block_id) =
                        Self::apply_block_queue_diff_from_entry_stuff(
                            mq_adapter.clone(),
                            block_entry,
                            min_processed_to,
                            &mut first_required_diffs,
                        )?
                    {
                        res.applied_diffs_ids.insert(applied_diff_block_id);
                    }
                }
            }

            // we can gc to current master block when diffs were applied
            let to_blocks_keys = mc_block_entry.get_top_blocks_keys()?;
            blocks_cache.set_gc_to_boundary(&to_blocks_keys);

            // on sync finish we commit diffs
            if is_last {
                let partitions = subgraph.get_partitions();
                Self::commit_block_queue_diff(
                    mq_adapter.clone(),
                    &mc_block_entry.block_id,
                    &mc_block_entry.top_shard_blocks_info,
                    &partitions,
                )?;

                // when we run sync by any reason we should drop uncommitted queue updates
                // after restoring the required state
                // to avoid panics if next block was already collated before an it is incorrect
                let top_shards = blocks_cache.get_last_top_shards();
                mq_adapter.clear_uncommitted_state(&top_shards)?;

                let state = mc_block_entry.cached_state()?.clone();
                res.last_mc_state = Some(state);

                res.synced_to_blocks_keys.extend(to_blocks_keys.into_iter());

                return Ok(res);
            }
        }
    }

    fn get_last_processed_mc_block_id(&self) -> Option<BlockId> {
        *self.last_processed_mc_block_id.lock()
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
                tracing::info!(
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
            tracing::info!(
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
        mode: ProcessMcStateUpdateMode,
    ) -> Result<()> {
        tracing::info!(target: tracing_targets::COLLATION_MANAGER,
            ?mode,
            "will process master state update",
        );

        if let ProcessMcStateUpdateMode::StartCollation { reset_collators } = mode {
            let block_global = mc_data.config.get_global_version()?;
            if self.config.supported_block_version >= block_global.version
                && block_global
                    .capabilities
                    .is_subset_of(self.config.supported_capabilities)
            {
                self.refresh_collation_sessions(mc_data, reset_collators)
                    .await?;
            } else {
                tracing::info!(target: tracing_targets::COLLATION_MANAGER,
                    collator_supported_block_version = self.config.supported_block_version,
                    mc_block_version = block_global.version,
                    collator_supported_capabilities = ?self.config.supported_capabilities,
                    mc_block_capabilities = ?block_global.capabilities,
                    "Refresh collation sessions is skipped: collator does not support mc block version or capabilities",
                );
            }
        }

        Ok(())
    }

    /// Returns top processed to anchor id if delayed state was processed
    async fn notify_to_mempool_and_process_delayed_mc_state_update(
        &self,
        block_id: &BlockId,
    ) -> Result<Option<MempoolAnchorId>> {
        let mut delayed_mc_data = None;
        {
            let mut guard = self.delayed_mc_state_update.lock();
            if let Some(mc_data) = guard.clone()
                && mc_data.block_id <= *block_id
            {
                // process delayed mc state only if committed block is equal
                if mc_data.block_id == *block_id {
                    delayed_mc_data = Some(mc_data);
                }

                // remove delayed mc state even if committed block is ahead
                *guard = None;
            }
        }
        if let Some(mc_data) = delayed_mc_data {
            self.notify_mc_state_update_to_mempool(mc_data.clone())
                .await?;
            self.process_mc_state_update(
                mc_data.clone(),
                ProcessMcStateUpdateMode::StartCollation {
                    reset_collators: false,
                },
            )
            .await?;

            Ok(Some(mc_data.top_processed_to_anchor))
        } else {
            Ok(None)
        }
    }

    async fn notify_mc_state_update_to_mempool(&self, mc_data: Arc<McData>) -> Result<()> {
        tracing::info!(target: tracing_targets::COLLATION_MANAGER,
            block_id = %mc_data.block_id.as_short_id(),
            "will notify master state update to mempool",
        );

        let prev_validator_set = self
            .validator_set_cache
            .get_prev_validator_set(&mc_data.config)?;
        let current_validator_set = self
            .validator_set_cache
            .get_current_validator_set(&mc_data.config)?;
        let next_validator_set = self
            .validator_set_cache
            .get_next_validator_set(&mc_data.config)?;

        let cx = StateUpdateContext {
            mc_block_id: mc_data.block_id,
            mc_block_chain_time: mc_data.gen_chain_time,
            top_processed_to_anchor_id: mc_data.top_processed_to_anchor,
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
            "Start refresh collation sessions by mc state ({})...",
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
        for (shard_id, descr) in mc_data.shards.iter() {
            let top_block_id = descr.get_block_id(*shard_id);
            // TODO: consider split and merge
            new_shards_info.insert(*shard_id, vec![top_block_id]);
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

        // find out the actual collation session start round from master state
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
                Result::<_>::Ok((subset.clone(), *hash_short))
            }
            hash_map::Entry::Vacant(entry) => {
                let (subset, hash_short) = full_validators_set
                    .compute_mc_subset(current_session_seqno, collation_config.shuffle_mc_validators)
                    .ok_or_else(|| anyhow!(
                        "Error calculating subset of validators for session (shard_id = {}, seqno = {})",
                        ShardIdent::MASTERCHAIN,
                        current_session_seqno,
                    ))?;

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
                let (subset, hash_short) = get_validator_subset(shard_id)?;
                let local_pubkey = find_us_in_collators_set(&self.keypair, &subset);

                if local_pubkey.is_none() {
                    tracing::debug!(
                        target: tracing_targets::COLLATION_MANAGER,
                        public_key = %self.keypair.public_key,
                        "Current node was not authorized to collate shard {}",
                        shard_id,
                    );
                    metrics::gauge!("tycho_node_in_current_vset").set(0);
                } else {
                    metrics::gauge!("tycho_node_in_current_vset").set(1);
                }

                match active_collation_sessions_guard.entry(shard_id) {
                    hash_map::Entry::Occupied(entry) => {
                        let existing_session_info = entry.get().clone();
                        if local_pubkey.is_some() {
                            // start new session when seqno changed or subset changed for the same seqno
                            if existing_session_info.collators().short_hash == hash_short
                                && existing_session_info.seqno() == current_session_seqno
                            {
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
                "new_session_info: {:?}",
                new_session_info,
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

                    let cancel_collation_notify = Arc::new(Notify::new());

                    match self
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
                            cancel_collation: cancel_collation_notify.clone(),
                        })
                        .await
                    {
                        Err(err) => {
                            tracing::error!(target: tracing_targets::COLLATION_MANAGER,
                                session_id = ?new_session_info.id(),
                                ?err,
                                "error starting collator"
                            );
                        }
                        Ok(collator) => {
                            entry.insert(ActiveCollator {
                                collator: Arc::new(collator),
                                state: CollatorState::Active,
                                cancel_collation: cancel_collation_notify,
                            });
                        }
                    }
                }
            }

            // need to add validation session only for masterchain blocks, shard blocks are not being validated
            if shard_id.is_masterchain() {
                self.validator.add_session(AddSession {
                    shard_ident: shard_id,
                    session_id: new_session_info.get_validation_session_id(),
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
            self.finish_collation_session(session_info)?;
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
    pub fn finish_collation_session(
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
    pub fn handle_collator_stopped(&self, collation_session_id: CollationSessionId) -> Result<()> {
        tracing::info!(target: tracing_targets::COLLATION_MANAGER,
            "handle_collator_stopped: {:?}", collation_session_id,
        );
        self.collators_to_stop.remove(&collation_session_id);
        Ok(())
    }

    fn set_collator_state<F>(&self, shard_id: &ShardIdent, f: F) -> Option<CollatorState>
    where
        F: Fn(&mut ActiveCollator<Arc<CF::Collator>>),
    {
        match self.active_collators.get_mut(shard_id) {
            Some(mut active_collator) => {
                f(&mut active_collator);
                Some(active_collator.state)
            }
            None => None,
        }
    }

    fn set_active_sync_info(&self, target_mc_block_seqno: BlockSeqno) -> Result<CancellationToken> {
        let mut guard = self.collation_sync_state.lock();
        if let Some(active_sync) = &guard.active_sync_to_applied {
            bail!(
                "previous sync_to_applied_mc_block should be finished \
                before: previous seqno={}, target seqno={}",
                active_sync.target_mc_block_seqno,
                target_mc_block_seqno,
            )
        }

        let cancelled = CancellationToken::new();
        guard.active_sync_to_applied = Some(ActiveSync {
            target_mc_block_seqno,
            cancelled: cancelled.clone(),
        });

        Ok(cancelled)
    }

    fn clean_active_sync_info(&self) {
        let mut guard = self.collation_sync_state.lock();
        guard.active_sync_to_applied = None;
    }

    fn update_last_received_mc_block_seqno(&self, received_block_id: &BlockId) {
        if !received_block_id.is_masterchain() {
            return;
        }

        let mut guard = self.collation_sync_state.lock();
        guard.last_received_mc_block_seqno = Some(received_block_id.seqno);
    }

    fn get_last_received_mc_block_seqno(&self) -> Option<BlockSeqno> {
        let guard = self.collation_sync_state.lock();
        guard.last_received_mc_block_seqno
    }

    fn update_last_synced_to_mc_block_id(&self, mc_block_id: BlockId) {
        let mut guard = self.collation_sync_state.lock();
        guard.last_synced_to_mc_block_id = Some(mc_block_id);
    }

    fn get_last_synced_to_mc_block_id(&self) -> Option<BlockId> {
        let guard = self.collation_sync_state.lock();
        guard.last_synced_to_mc_block_id
    }

    /// Returns `true` if active sync was cancelled
    fn finish_active_sync_to_applied(&self, received_block_id: &BlockId) -> bool {
        // can finish active sync only if new master block received
        if !received_block_id.is_masterchain() {
            return false;
        }

        let guard = self.collation_sync_state.lock();

        // call to finish active sync if it exists and received block is newer
        if let Some(active_sync_info) = &guard.active_sync_to_applied {
            // call to finish active sync if new block is far ahead
            if received_block_id
                .seqno
                .saturating_sub(active_sync_info.target_mc_block_seqno)
                >= self.config.min_mc_block_delta_from_bc_to_sync
            {
                tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                    prev_target_block_id = %BlockIdShort {
                        shard: ShardIdent::MASTERCHAIN,
                        seqno: active_sync_info.target_mc_block_seqno,
                    },
                    "cancel sync: will force finish previous sync \
                    to applied master block if not started to process state update",
                );
                active_sync_info.cancelled.cancel();
                return true;
            } else {
                // otherwise allow to handle newer block from bc when previous process started
                tracing::trace!(target: tracing_targets::COLLATION_MANAGER,
                    prev_target_block_id = %BlockIdShort {
                        shard: ShardIdent::MASTERCHAIN,
                        seqno: active_sync_info.target_mc_block_seqno,
                    },
                    "cancel sync: will not force finish previous sync \
                    to applied master block, because new block is not far ahead",
                );
            }
        } else {
            tracing::trace!(target: tracing_targets::COLLATION_MANAGER,
                "cancel sync: route_handle_block_from_bc: no active sync to cancel",
            );
        }

        false
    }

    /// Set master block latest chain time to calc next interval for master block collation.
    /// Prune all cached chain times for all shards upto current
    fn renew_mc_block_latest_chain_time(guard: &mut CollationSyncState, chain_time: u64) {
        if guard.mc_block_latest_chain_time < chain_time {
            guard.mc_block_latest_chain_time = chain_time;
        }

        // prune
        for (_, collation_state) in guard.states.iter_mut() {
            collation_state
                .last_imported_anchor_events
                .retain(|it| it.ct > chain_time);
        }
    }

    /// Reset collation status from `WaitForMasterStatus` to `AttemptsInProgress` for every shard.
    ///
    /// Use this method before resuming collation after sync to avoid ambiguous situations.
    /// If any shard has collation status `WaitForMasterStatus` and sync was executed,
    /// when master collation check was finished first then it will enqueue one more resume for shard,
    /// so we will have two parallel collations for shard that will cause panic further.
    fn reset_collation_sync_status(guard: &mut CollationSyncState) {
        for (_, collation_state) in guard.states.iter_mut() {
            if collation_state.status == CollationStatus::WaitForMasterStatus {
                collation_state.status = CollationStatus::AttemptsInProgress;
            }
        }
    }

    /// 1. Store collation status for current shard
    /// 2. Detect the next step: wait for master status, resume attempts, run master collation
    fn detect_next_collation_step(
        guard: &mut CollationSyncState,
        active_shards: Vec<ShardIdent>,
        shard_id: ShardIdent,
        ctx: DetectNextCollationStepContext,
    ) -> NextCollationStep {
        let DetectNextCollationStepContext {
            last_imported_anchor_ct,
            force_mc_block,
            is_first_block_after_prev_master,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
        } = ctx;

        let _histogram = HistogramGuard::begin("detect_next_collation_step_time");
        assert!(
            active_shards.contains(&shard_id),
            "active_shards must include current shard"
        );

        let mc_block_latest_chain_time = guard.mc_block_latest_chain_time;

        // Check if masterchain collation state exists (already imported anchor events from MC shard)
        let mc_collation_state_exist = shard_id.is_masterchain()
            || guard
                .states
                .get(&ShardIdent::MASTERCHAIN)
                .is_some_and(|state| !state.last_imported_anchor_events.is_empty());

        // Force collation for all if MC shard explicitly requires it (unprocessed messages)
        if shard_id.is_masterchain()
            && matches!(force_mc_block, ForceMasterCollation::ByUnprocessedMessages)
        {
            guard.mc_collation_forced_for_all = true;
            tracing::debug!(
                target: tracing_targets::COLLATION_MANAGER,
                "MC force for all enabled by unprocessed messages"
            );
        }

        // Another forcing rule: no pending messages after shard blocks
        if matches!(
            force_mc_block,
            ForceMasterCollation::NoPendingMessagesAfterShardBlocks
        ) {
            guard.mc_forced_by_no_pending_msgs_on_ct = Some(last_imported_anchor_ct);
            tracing::debug!(
                target: tracing_targets::COLLATION_MANAGER,
                mc_forced_by_no_pending_msgs_on_ct=?guard.mc_forced_by_no_pending_msgs_on_ct,
                "MC force by no pending messages"
            );
        }

        let hard_forced_for_all = guard.mc_collation_forced_for_all;
        let mc_forced_by_no_pending_msgs_on_ct = guard.mc_forced_by_no_pending_msgs_on_ct;

        // Determine if the current shard collation is explicitly forced in new anchor event
        let forced_in_current_shard = force_mc_block.is_forced();

        // Add new anchor event for current shard
        let current_collation_state = guard.states.entry(shard_id).or_default();
        current_collation_state
            .last_imported_anchor_events
            .push(ImportedAnchorEvent {
                ct: last_imported_anchor_ct,
                mc_forced: forced_in_current_shard,
                is_first_block_after_prev_master,
            });

        tracing::trace!(
            target: tracing_targets::COLLATION_MANAGER,
            shard_id=?shard_id,
            last_imported_anchor_ct,
            forced_in_current_shard,
            is_first_block_after_prev_master,
            "anchor event appended"
        );

        // Per-shard facts to simplify decision-making
        #[derive(Debug, Clone)]
        struct ShardFact {
            shard_id: ShardIdent,
            status: CollationStatus,   // current collation status for shard
            first_ct: Option<u64>,     // first ct
            mc_forced_ct: Option<u64>, // first ct on which mc block collation forced
            min_ct: Option<u64>,       // first ct >= min interval
            max_ct: Option<u64>,       // first ct >= max interval
            has_collated_block_after_prev_mc: bool, // produced first shard block after master
        }
        impl ShardFact {
            fn with_status(shard_id: ShardIdent, status: CollationStatus) -> Self {
                Self {
                    shard_id,
                    status,
                    first_ct: None,
                    mc_forced_ct: None,
                    min_ct: None,
                    max_ct: None,
                    has_collated_block_after_prev_mc: false,
                }
            }
            fn calc(
                shard_id: ShardIdent,
                state: &CollationState,
                mc_ct: u64,
                min_interval_ms: u64,
                max_interval_ms: u64,
            ) -> Self {
                let mut fact = Self::with_status(shard_id, state.status);

                for s in &state.last_imported_anchor_events {
                    if fact.first_ct.is_none() {
                        fact.first_ct = Some(s.ct);
                    }

                    if s.mc_forced && fact.mc_forced_ct.is_none() {
                        fact.mc_forced_ct = Some(s.ct);
                    }

                    // remember if there was the first shard block after prev master
                    if s.is_first_block_after_prev_master {
                        fact.has_collated_block_after_prev_mc = true;
                    }

                    // we take only first ct that exceed min interval
                    // or the next that goes with the first shard block after prev master
                    if (fact.min_ct.is_none() || s.is_first_block_after_prev_master)
                        && s.ct.saturating_sub(mc_ct) >= min_interval_ms
                    {
                        fact.min_ct = Some(s.ct);
                    }

                    // we take only first ct that exceed max interval
                    if fact.max_ct.is_none() && s.ct.saturating_sub(mc_ct) >= max_interval_ms {
                        fact.max_ct = Some(s.ct);
                    }
                }

                fact
            }
        }

        // Collect facts for all active shards
        let mut facts = Vec::with_capacity(active_shards.len());
        for sid in &active_shards {
            if let Some(st) = guard.states.get(sid) {
                let f = ShardFact::calc(
                    *sid,
                    st,
                    mc_block_latest_chain_time,
                    mc_block_min_interval_ms,
                    mc_block_max_interval_ms,
                );
                facts.push(f);
            } else {
                facts.push(ShardFact::with_status(
                    *sid,
                    CollationStatus::AttemptsInProgress,
                ));
            }
        }
        tracing::debug!(
            target: tracing_targets::COLLATION_MANAGER,
            mc_block_latest_chain_time,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            ?facts,
            "calculated shard facts"
        );

        let any_shard_has_collated = facts.iter().any(|f| f.has_collated_block_after_prev_mc);

        fn choose_candidate(
            curr_sid: &ShardIdent,
            f: &ShardFact,
            mc_forced_by_shard_on_ct: Option<u64>,
            any_has_first_collated: bool,
            hard_forced_for_all: bool,
        ) -> Option<u64> {
            let ready_or_is_current_shard =
                f.status == CollationStatus::ReadyToCollateMaster || f.shard_id == *curr_sid;

            // chain time on which master was forced
            let ct = f
                .mc_forced_ct
                // first ct if master was forced for all
                .or(if hard_forced_for_all {
                    f.first_ct
                } else {
                    None
                })
                // chain time when master was forced by shard
                // if ready to collate master or is current shard
                .or(if ready_or_is_current_shard {
                    mc_forced_by_shard_on_ct
                } else {
                    None
                })
                .or(
                    // chain time that exceed min interval
                    // when any shard has first collated block after previous master
                    // if ready to collate master or is current shard
                    if any_has_first_collated {
                        if ready_or_is_current_shard {
                            f.min_ct
                        } else {
                            None
                        }
                    }
                    // finally take chain time that exceed max interval
                    else {
                        f.max_ct
                    },
                );

            tracing::trace!(
                target: tracing_targets::COLLATION_MANAGER,
                shard_id=?f.shard_id,
                status=?f.status,
                f_first_ct=?f.first_ct,
                f_mc_forced_ct=?f.mc_forced_ct,
                f_min_ct=?f.min_ct,
                f_max_ct=?f.max_ct,
                hard_forced_for_all,
                mc_forced_by_shard_on_ct,
                any_has_first_collated,
                chosen_ct=?ct,
                "choose_candidate decision"
            );

            ct
        }

        // get next mc block ct candidates from all shards
        // and detect if should collate by current shard
        let mut should_collate_by_current_shard = false;

        let candidates: Vec<_> = facts
            .iter()
            .map(|f| {
                let ct = choose_candidate(
                    &shard_id,
                    f,
                    mc_forced_by_no_pending_msgs_on_ct,
                    any_shard_has_collated,
                    hard_forced_for_all,
                );
                if f.shard_id == shard_id && ct.is_some() {
                    should_collate_by_current_shard = true;
                }
                ct
            })
            .collect();

        tracing::debug!(
            target: tracing_targets::COLLATION_MANAGER,
            shard_id=?shard_id,
            ?candidates,
            should_collate_by_current_shard,
            any_shard_has_collated,
            hard_forced_for_all,
            mc_forced_by_no_pending_msgs_on_ct,
            "candidates collected"
        );

        // check if should collate by every shard
        let should_collate_by_every_shard =
            !candidates.is_empty() && candidates.iter().all(|c| c.is_some());

        // If not all shards ready to collate next master then resume attempts
        if !should_collate_by_every_shard {
            let mut shards_to_resume_attempts = vec![];

            // If current shard is not ready to collate master then try to resume attempts
            let current_collation_state = guard.states.entry(shard_id).or_default();
            if !should_collate_by_current_shard {
                // wait for master collation status if it not exist yet
                if !mc_collation_state_exist {
                    current_collation_state.status = CollationStatus::WaitForMasterStatus;
                    let res = NextCollationStep::WaitForMasterStatus;

                    tracing::info!(
                        target: tracing_targets::COLLATION_MANAGER,
                        shard_id=?shard_id,
                        ?facts,
                        ?candidates,
                        decision=?res,
                        "step decision"
                    );
                    return res;
                } else {
                    // or resume attempts right now
                    current_collation_state.status = CollationStatus::AttemptsInProgress;
                    shards_to_resume_attempts.push(shard_id);
                }
            } else {
                // otherwise it is ready to collate master
                current_collation_state.status = CollationStatus::ReadyToCollateMaster;
            }

            // Masterchain shard resumes all dependent shard attempts
            if shard_id.is_masterchain() {
                // when we check master collation status and consider to resume attempts
                // then we should resume for all shards that have been waiting for master
                for (shard_ident, shard_collation_state) in
                    guard.states.iter_mut().filter(|(s, _)| !s.is_masterchain())
                {
                    if shard_collation_state.status == CollationStatus::WaitForMasterStatus {
                        shard_collation_state.status = CollationStatus::AttemptsInProgress;
                        shards_to_resume_attempts.push(*shard_ident);
                    }
                }
            }

            let res = NextCollationStep::ResumeAttemptsIn(shards_to_resume_attempts);
            tracing::info!(
                target: tracing_targets::COLLATION_MANAGER,
                shard_id=?shard_id,
                ?facts,
                ?candidates,
                decision=?res,
                "step decision"
            );

            return res;
        }

        // Otherwise: collate MC block using max candidate ct
        let next_mc_block_chain_time = candidates.into_iter().flatten().max().unwrap();

        tracing::info!(
            target: tracing_targets::COLLATION_MANAGER,
            hard_forced_for_all,
            forced_in_current_shard,
            mc_block_min_interval_ms,
            next_mc_block_chain_time,
            "Master block collation forced or interval exceeded in every shard - \
            will collate next master block",
        );

        // Mark all shards as "running" (attempts in progress)
        for st in guard.states.values_mut() {
            st.status = CollationStatus::AttemptsInProgress;
        }

        // Update MC block time and reset force flags
        Self::renew_mc_block_latest_chain_time(guard, next_mc_block_chain_time);

        // drop "forced for all" and "forced by no pending messages" flags
        // if we decided to collate master
        guard.mc_collation_forced_for_all = false;
        guard.mc_forced_by_no_pending_msgs_on_ct = None;

        NextCollationStep::CollateMaster(next_mc_block_chain_time)
    }

    /// Enqueue master block collation task. Will determine top shard blocks for this collation
    async fn enqueue_mc_block_collation(
        &self,
        next_mc_block_id_short: BlockIdShort,
        next_mc_block_chain_time: u64,
        _trigger_block_id_opt: Option<BlockId>,
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

        let top_shard_blocks_info = self
            .blocks_cache
            .get_top_shard_blocks_info_for_mc_block(next_mc_block_id_short)?;

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
            "Enqueued next attempt to collate block for shard {}",
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

        self.ready_to_sync.notified().await;

        // process valid block
        self.commit_valid_master_block(&block_id).await?;

        self.ready_to_sync.notify_one();

        Ok(())
    }

    /// Try to commit validated and valid master block
    /// if it was not already committed before
    /// 1. Check if master block is valid
    /// 2. Extract master block subgraph with shard blocks
    /// 3. Send to sync
    /// 4. Commit queue diff
    /// 5. Clean up from cache
    /// 6. Process delayed master state update if exists
    /// 7. Notify top processed anchor to mempool if block commited by received from bc
    async fn commit_valid_master_block(&self, mc_block_id: &BlockId) -> Result<()> {
        tracing::debug!(
            target: tracing_targets::COLLATION_MANAGER,
            "Start to commit validated and valid master block ({})...",
            mc_block_id.as_short_id(),
        );

        // gc blocks from cache when commit finished
        scopeguard::defer!(self.blocks_cache.gc_prev_blocks());

        // we can process delayed master state update now
        let mut top_processed_to_anchor_to_notify = self
            .notify_to_mempool_and_process_delayed_mc_state_update(mc_block_id)
            .await?;

        let histogram = HistogramGuard::begin("tycho_collator_commit_valid_master_block_time");
        let histogram_extract =
            HistogramGuard::begin("tycho_collator_extract_master_block_subgraph_time");
        let mut extract_elapsed = Default::default();
        let mut sync_elapsed = Default::default();

        // extract master block with all shard blocks if valid, and process them
        match self
            .blocks_cache
            .extract_mc_block_subgraph_for_sync(mc_block_id)
        {
            McBlockSubgraphExtract::Extracted(subgraph) => {
                extract_elapsed = histogram_extract.finish();

                let partitions = subgraph.get_partitions();

                let McBlockSubgraph {
                    master_block,
                    shard_blocks,
                } = subgraph;

                // we can gc upto to current master block after commit
                // because we do not need to commit all previous blocks
                // because all previous blocks are already in bc state
                // and all previous diffs already committed by current one
                let to_blocks_keys = master_block.get_top_blocks_keys()?;
                self.blocks_cache.set_gc_to_boundary(&to_blocks_keys);

                // send to sync only if was not received from bc
                if matches!(&master_block.data, BlockCacheEntryData::Collated {
                    received_after_collation: false,
                    ..
                }) {
                    let histogram =
                        HistogramGuard::begin("tycho_collator_send_blocks_to_sync_time");

                    self.send_block_to_sync(master_block.data)?;

                    for shard_block in shard_blocks {
                        self.send_block_to_sync(shard_block.data)?;
                    }

                    sync_elapsed = histogram.finish();
                    tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                        total = sync_elapsed.as_millis(),
                        "send_blocks_to_sync timings",
                    );

                    // if current master block was not applied to bc state yet
                    // then we should not notify top processed to anchor to mempool now
                    // because we are not sure that block will be applied
                    // and should wait until block_accepted event is received
                    top_processed_to_anchor_to_notify = None;
                } else {
                    // if current block was committed by received one
                    // then `on_block_accepted` will not be called further
                    // so we need to notify `top_processed_to_anchor` to mempool here
                    top_processed_to_anchor_to_notify = master_block.top_processed_to_anchor;
                }

                let _histogram =
                    HistogramGuard::begin("tycho_collator_send_blocks_to_sync_commit_diffs_time");

                Self::commit_block_queue_diff(
                    self.mq_adapter.clone(),
                    &master_block.block_id,
                    &master_block.top_shard_blocks_info,
                    &partitions,
                )?;
            }
            McBlockSubgraphExtract::AlreadyExtracted => {
                tracing::debug!(
                    target: tracing_targets::COLLATION_MANAGER,
                    "Master block subgraph is already extracted and cleaned up from cache ({}). Do nothing",
                    mc_block_id.as_short_id(),
                );
            }
        }

        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            total = histogram.finish().as_millis(),
            extract_subgraph = extract_elapsed.as_millis(),
            sync = sync_elapsed.as_millis(),
            "commit_valid_master_block timings",
        );

        // report last processed anchor to mempool
        if let Some(top_processed_to_anchor) = top_processed_to_anchor_to_notify {
            self.notify_top_processed_to_anchor_to_mempool(
                mc_block_id.seqno,
                top_processed_to_anchor,
            )
            .await?;
        }

        Ok(())
    }

    fn send_block_to_sync(&self, data: BlockCacheEntryData) -> Result<()> {
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
        self.state_node_adapter
            .accept_block(candidate_stuff.into_block_for_sync())?;
        tracing::debug!(
            target: tracing_targets::COLLATION_MANAGER,
            "Block was successfully sent to sync ({})",
            block_id,
        );
        Ok(())
    }

    /// Collect top blocks seqno from all shards by master block id.
    /// Master block seqno included
    async fn get_top_blocks_seqno(
        mc_block_id: &BlockId,
        blocks_cache: &BlocksCache,
        state_node_adapter: Arc<dyn StateNodeAdapter>,
    ) -> Result<FastHashMap<ShardIdent, BlockSeqno>> {
        let mut result = FastHashMap::default();

        result.insert(mc_block_id.shard, mc_block_id.seqno);

        let top_shard_blocks = blocks_cache.get_top_shard_blocks(mc_block_id.as_short_id());

        match top_shard_blocks {
            None => {
                let state = match state_node_adapter
                    .load_state(mc_block_id.seqno, mc_block_id)
                    .await
                {
                    Err(err) => match err.downcast_ref::<ShardStateStorageError>() {
                        Some(ShardStateStorageError::NotFound(_)) => {
                            tracing::warn!(target: tracing_targets::COLLATION_MANAGER,
                                %mc_block_id,
                                "master state not found in get_top_blocks_seqno",
                            );
                            return Ok(FastHashMap::default());
                        }
                        _ => Err(err),
                    },
                    state => state,
                }?;
                for item in state.shards()?.iter() {
                    let (shard_id, shard_descr) = item?;
                    result.insert(shard_id, shard_descr.get_block_id(shard_id).seqno);
                }
            }
            Some(top_shard_blocks) => {
                result.extend(top_shard_blocks);
            }
        }
        Ok(result)
    }
}

#[derive(Debug)]
enum ProcessMcStateUpdateMode {
    StartCollation { reset_collators: bool },
    SkipCollation,
}

#[derive(Default)]
struct RestoreQueueResult {
    last_mc_state: Option<ShardStateStuff>,
    synced_to_blocks_keys: Vec<BlockCacheKey>,
    applied_diffs_ids: FastHashSet<BlockId>,
}

// TODO: Move into `tycho_types`.
trait GlobalCapabilitiesExt {
    /// Checks whether this capabilities set is fully
    /// included into `other` (comparing raw bits to include unnamed variants).
    fn is_subset_of(&self, other: GlobalCapabilities) -> bool;
}

impl GlobalCapabilitiesExt for GlobalCapabilities {
    fn is_subset_of(&self, other: GlobalCapabilities) -> bool {
        let this = self.into_inner();
        let other = other.into_inner();
        this & !other == 0
    }
}

#[derive(Debug)]
struct DetectNextCollationStepContext {
    pub last_imported_anchor_ct: u64,
    pub force_mc_block: ForceMasterCollation,
    pub is_first_block_after_prev_master: bool,
    pub mc_block_min_interval_ms: u64,
    pub mc_block_max_interval_ms: u64,
}

impl DetectNextCollationStepContext {
    pub fn new(
        last_imported_anchor_ct: u64,
        force_mc_block: ForceMasterCollation,
        is_first_block_after_prev_master: bool,
        mc_block_min_interval_ms: u64,
        mc_block_max_interval_ms: u64,
    ) -> Self {
        Self {
            last_imported_anchor_ct,
            force_mc_block,
            is_first_block_after_prev_master,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
        }
    }
}
