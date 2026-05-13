use std::collections::{BTreeMap, VecDeque, hash_map};
use std::sync::Arc;

use ahash::HashMapExt;
use anyhow::{Context, Result, anyhow, bail};
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use parking_lot::{Mutex, RwLock};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tycho_block_util::block::{TopBlocks, ValidatorSubsetInfo, calc_next_block_id_short};
use tycho_block_util::queue::{QueueKey, QueuePartitionIdx};
use tycho_block_util::state::ShardStateStuff;
use tycho_core::global_config::MempoolGlobalConfig;
use tycho_core::storage::{LoadStateHint, StateNotFound};
use tycho_crypto::ed25519::KeyPair;
use tycho_types::models::{
    BlockId, BlockIdShort, CollationConfig, GlobalCapabilities, ProcessedUptoInfo, ShardIdent,
    ValidatorDescription,
};
use tycho_util::futures::{AwaitBlocking, JoinTask};
use tycho_util::metrics::HistogramGuard;
use tycho_util::{DashMapEntry, FastDashMap, FastHashMap, FastHashSet};

use self::blocks_cache::BlocksCache;
use self::cancel_validation_runner::CancelValidationRunnerState;
use self::collation_cancel::{ActionAfterCancel, CollationCancelHandle};
use self::state_event_listener::{ChannelStateEventListener, StateEvent};
use self::types::{
    ActiveCollator, ActiveSync, BlockCacheEntry, BlockCacheEntryData, BlockCacheKey,
    CandidateStatus, CollatedBlockInfo, CollationState, CollationStatus, CollationSyncState,
    CollatorJoinTask, CollatorState, HandledBlockFromBcCtx, ImportedAnchorEvent, McBlockSubgraph,
    McBlockSubgraphExtract, NextCollationStep, ValidatorJoinTask,
};
use self::utils::find_us_in_collators_set;
use crate::collator::{
    Collator, CollatorContext, CollatorFactory, CollatorResult, DebugCollatorResult,
    ForceMasterCollation,
};
use crate::internal_queue::types::diff::{DiffZone, QueueDiffWithMessages};
use crate::internal_queue::types::message::EnqueuedMessage;
use crate::internal_queue::types::stats::DiffStatistics;
use crate::mempool::{MempoolAdapter, MempoolAdapterFactory, MempoolAnchorId, StateUpdateContext};
use crate::queue_adapter::MessageQueueAdapter;
use crate::state_node::{StateNodeAdapter, StateNodeAdapterFactory};
use crate::tracing_targets;
use crate::types::processed_upto::{
    BlockSeqno, ProcessedUptoInfoExtension, ProcessedUptoInfoStuff, find_min_processed_to_by_shards,
};
use crate::types::{
    BlockCollationResult, BlockIdExt, CollationSessionInfo, CollatorConfig, DebugDisplayOpt,
    DebugIter, DisplayAsShortId, DisplayBlockIdsIntoIter, McData, ProcessedToByPartitions,
    ShardDescriptionShort, ShardDescriptionShortExt, ShardHashesExt, TopBlockId, TopBlockIdUpdated,
};
use crate::utils::block::detect_top_processed_to_anchor;
use crate::utils::shard::calc_split_merge_actions;
use crate::utils::vset_cache::ValidatorSetCache;
use crate::validator::{AddSession, ValidationStatus, Validator};

mod active_collators;
mod blocks_cache;
mod cancel_validation_runner;
mod collation_cancel;
mod state_event_listener;
mod types;
mod utils;

#[cfg(test)]
#[path = "tests/manager_tests.rs"]
pub(super) mod tests;

pub struct CollationManager<CF, V>
where
    CF: CollatorFactory,
{
    keypair: Arc<KeyPair>,
    config: Arc<CollatorConfig>,

    state_node_adapter: Arc<dyn StateNodeAdapter>,
    state_event_receiver: Mutex<Option<tokio::sync::mpsc::Receiver<StateEvent>>>,

    mpool_adapter: Arc<dyn MempoolAdapter>,
    mq_adapter: Arc<dyn MessageQueueAdapter<EnqueuedMessage>>,

    collator_factory: CF,

    validator: Arc<V>,
    cancel_validation_runner: Mutex<CancelValidationRunnerState>,

    active_collation_sessions: RwLock<FastHashMap<ShardIdent, Arc<CollationSessionInfo>>>,
    active_collators: FastDashMap<ShardIdent, ActiveCollator<Box<CF::Collator>>>,

    blocks_cache: BlocksCache,

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

impl<CF, V> CollationManager<CF, V>
where
    CF: CollatorFactory,
    V: Validator,
{
    #[allow(clippy::too_many_arguments)]
    pub fn create<STF, MPF>(
        keypair: Arc<KeyPair>,
        config: CollatorConfig,
        mq_adapter: Arc<dyn MessageQueueAdapter<EnqueuedMessage>>,
        state_node_adapter_factory: STF,
        mpool_adapter_factory: MPF,
        validator: V,
        collator_factory: CF,
        mempool_config_override: Option<MempoolGlobalConfig>,
    ) -> Arc<CollationManager<CF, V>>
    where
        STF: StateNodeAdapterFactory,
        MPF: MempoolAdapterFactory,
    {
        tracing::info!(target: tracing_targets::COLLATION_MANAGER, "Creating collation manager...");

        // create state node adapter
        const STATE_EVENTS_QUEUE_LIMIT: usize = 100;
        let (state_event_listener, state_event_receiver) =
            ChannelStateEventListener::build(STATE_EVENTS_QUEUE_LIMIT);
        let state_node_adapter = Arc::new(state_node_adapter_factory.create(state_event_listener));

        let mpool_adapter = mpool_adapter_factory.create();

        let validator = Arc::new(validator);

        let blocks_cache = BlocksCache::new(state_node_adapter.zerostate_id());

        let collation_manager = Self {
            keypair,
            config: Arc::new(config),

            state_node_adapter,
            state_event_receiver: Mutex::new(Some(state_event_receiver)),

            mpool_adapter,
            mq_adapter,

            collator_factory,

            validator,
            cancel_validation_runner: Default::default(),

            active_collation_sessions: Default::default(),
            active_collators: Default::default(),

            blocks_cache,

            last_processed_mc_block_id: Default::default(),

            collation_sync_state: Default::default(),

            validator_set_cache: Default::default(),

            mempool_config_override,

            delayed_mc_state_update: Arc::new(Mutex::new(None)),
        };

        tracing::info!(target: tracing_targets::COLLATION_MANAGER, "Collation manager created");

        Arc::new(collation_manager)
    }

    pub fn state_node_adapter(&self) -> &Arc<dyn StateNodeAdapter> {
        &self.state_node_adapter
    }

    /// Run main flow
    pub async fn run(self: &Arc<Self>) -> Result<()> {
        // take state events receiver
        let mut state_event_receiver =
            self.state_event_receiver.lock().take().context(
                "state_event_receiver already extracted, not allowed to call `run` again",
            )?;

        // create incoming blocks queue
        const BLOCKS_FROM_BC_QUEUE_LIMIT: usize = 1000;
        let (blocks_from_bc_queue_sender, mut blocks_from_bc_queue_receiver) =
            tokio::sync::mpsc::channel::<HandledBlockFromBcCtx>(BLOCKS_FROM_BC_QUEUE_LIMIT);

        // spawn parallel task to handle block applied events
        let mut handle_state_events_task = JoinTask::new({
            let mgr = self.clone();
            async move {
                tracing::info!(target: tracing_targets::COLLATION_MANAGER,
                    "state events processing: started",
                );
                scopeguard::defer!(tracing::info!(target: tracing_targets::COLLATION_MANAGER,
                    "state events processing: finished",
                ));
                loop {
                    tokio::select! {
                        event = state_event_receiver.recv() => match event {
                            None => {
                                tracing::warn!(target: tracing_targets::COLLATION_MANAGER,
                                    "state events channel closed: StateNodeAdapter dropped",
                                );
                                break;
                            }
                            Some(event) => match event {
                                StateEvent::OwnBlockApplied { state, processed_upto } => {
                                    // spawn validation cancellation
                                    mgr.schedule_cancel_validation_sessions_until_block(state.clone());

                                    // handle applied block in mempool
                                    mgr.detect_top_processed_to_anchor_and_notify_mempool(
                                        state,
                                        processed_upto.get_min_externals_processed_to()?.0
                                    )
                                    .await?;
                                }
                                StateEvent::ExternalBlockApplied { mc_block_id, state, processed_upto } => {
                                    let ctx = HandledBlockFromBcCtx {
                                        mc_block_id,
                                        state,
                                        processed_upto,
                                    };
                                    mgr.enqueue_handle_block_from_bc(ctx, &blocks_from_bc_queue_sender).await?;
                                }
                            }
                        }
                    }
                }
                Ok::<_, anyhow::Error>(())
            }
        });

        // set of collator tasks
        let mut collator_tasks = FuturesUnordered::new();
        let mut collation_cancel_task = CollationCancelHandle::new();

        // set of validator tasks
        let mut validator_tasks = FuturesUnordered::new();

        tracing::info!(target: tracing_targets::COLLATION_MANAGER,
            "collation manager main flow: started",
        );
        scopeguard::defer!(tracing::info!(target: tracing_targets::COLLATION_MANAGER,
            "collation manager main flow: finished",
        ););

        // main flow loop
        const BLOCKS_FROM_BC_BATCH_SIZE: usize = 100;
        loop {
            let mut blocks_from_bc_batch = Vec::with_capacity(BLOCKS_FROM_BC_BATCH_SIZE);
            tokio::select! {
                // just process state events handling error
                res = &mut handle_state_events_task => {
                    res?;
                },
                // handle blocks from bc
                received_count = blocks_from_bc_queue_receiver.recv_many(&mut blocks_from_bc_batch, BLOCKS_FROM_BC_BATCH_SIZE) => {
                    if received_count == 0 {
                        tracing::warn!(target: tracing_targets::COLLATION_MANAGER,
                            "blocks from bc queue channel closed: CollationManager dropped",
                        );
                        break;
                    }

                    let res = self.handle_blocks_from_bc_batch(blocks_from_bc_batch).await?;
                    self.handle_new_collator_tasks_and_cancel_request(
                        &mut collation_cancel_task,
                        &mut collator_tasks,
                        res.collator_tasks,
                        res.cancel_action,
                    ).await?;
                },
                // handle collator tasks
                Some(collator_res) = collator_tasks.next(), if !collator_tasks.is_empty() => {
                    let res = self.handle_collator_task(collator_res).await?;
                    if let Some(task) = res.validator_task {
                        validator_tasks.push(task);
                    }
                    self.handle_new_collator_tasks_and_cancel_request(
                        &mut collation_cancel_task,
                        &mut collator_tasks,
                        res.collator_tasks,
                        res.cancel_action,
                    ).await?;
                }
                // handle collation cancel task
                Some(res) = collation_cancel_task.wait(), if !collation_cancel_task.is_empty() => {
                    let action_after = res?;
                    for task in self.handle_collation_cancel_action(action_after).await? {
                        collator_tasks.push(task);
                    }
                }
                // handle validator tasks
                Some(validation_res) = validator_tasks.next(), if !validator_tasks.is_empty() => {
                    match validation_res {
                        Err(e) => {
                            tracing::error!(target: tracing_targets::COLLATION_MANAGER,
                                "block candidate validation failed: {e:?}",
                            );
                        }
                        Ok((block_id, validation_status)) => {
                            let new_collator_tasks = self.handle_validated_master_block(
                                block_id,
                                validation_status,
                                collation_cancel_task.running_and_will_sync_after(),
                            ).await?;
                            self.handle_new_collator_tasks_and_cancel_request(
                                &mut collation_cancel_task,
                                &mut collator_tasks,
                                new_collator_tasks,
                                None,
                            ).await?;
                        }

                    }
                }
            }
        }

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

    #[tracing::instrument(skip_all, fields(block_id = %block_id))]
    fn commit_block_queue_diff(
        mq_adapter: Arc<dyn MessageQueueAdapter<EnqueuedMessage>>,
        block_id: &BlockId,
        top_shard_blocks_info: &[TopBlockIdUpdated],
        partitions: &FastHashSet<QueuePartitionIdx>,
    ) -> Result<()> {
        if !block_id.is_masterchain() {
            return Ok(());
        }

        let _histogram = HistogramGuard::begin("tycho_collator_commit_queue_diffs_time");

        let mut top_blocks = top_shard_blocks_info.to_vec();
        top_blocks.push(TopBlockIdUpdated {
            block: TopBlockId {
                ref_by_mc_seqno: block_id.seqno,
                block_id: *block_id,
            },
            updated: true,
        });

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
        state_node_adapter: &dyn StateNodeAdapter,
        mq_adapter: Arc<dyn MessageQueueAdapter<EnqueuedMessage>>,
        block_entry: &BlockCacheEntry,
        min_processed_to: Option<&QueueKey>,
        first_required_diffs: &mut FastHashMap<ShardIdent, BlockId>,
    ) -> Result<Option<BlockId>> {
        let block_id = block_entry.block_id;

        // TODO: error if <
        if block_entry.ref_by_mc_seqno <= state_node_adapter.zerostate_id().seqno {
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

    async fn handle_collator_task(
        self: &Arc<Self>,
        collator_task_res: Result<(Box<CF::Collator>, CollatorResult)>,
    ) -> Result<HandleCollatorTaskResult<CF::Collator>> {
        let (collator, res) = collator_task_res?;

        tracing::trace!(target: tracing_targets::COLLATION_MANAGER,
            result = ?DebugCollatorResult(&res),
            "handle collator task result for {}",
            collator.shard_id(),
        );

        self.set_collator(collator)?;

        match res {
            CollatorResult::Skipped {
                prev_mc_block_id,
                next_block_id_short,
                anchor_chain_time,
                force_mc_block,
                collation_config,
            } => {
                let collator_tasks = self
                    .handle_collation_skipped(
                        prev_mc_block_id,
                        next_block_id_short,
                        anchor_chain_time,
                        force_mc_block,
                        collation_config,
                    )
                    .await?;
                Ok(HandleCollatorTaskResult {
                    validator_task: None,
                    collator_tasks,
                    cancel_action: None,
                })
            }
            CollatorResult::Cancelled(cancel_ctx) => {
                bail!(
                    "should not process `CollatorResult::Cancelled` in the main flow. cancel_ctx={:?}",
                    cancel_ctx,
                );
            }
            CollatorResult::BlockCandidate { collation_result } => {
                self.handle_collated_block_candidate(collation_result).await
            }
        }
    }

    #[tracing::instrument(skip_all, fields(next_block_id = %next_block_id_short, ct = anchor_chain_time, ?force_mc_block))]
    async fn handle_collation_skipped(
        &self,
        prev_mc_block_id: BlockId,
        next_block_id_short: BlockIdShort,
        anchor_chain_time: u64,
        force_mc_block: ForceMasterCollation,
        collation_config: Arc<CollationConfig>,
    ) -> Result<Vec<CollatorJoinTask<CF::Collator>>> {
        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            "will run next collation step",
        );

        // check invariant: collator should not be in `CancelPending` or `Cancelled` state
        // when processing collation skip in the main flow
        if let Some(collator_state) = self.get_collator_state(&next_block_id_short.shard) {
            assert!(
                !matches!(
                    collator_state,
                    CollatorState::CancelPending | CollatorState::Cancelled
                ),
                "collator should not be in 'cancel' state on collation skip processing",
            );
        }

        self.run_next_collation_step(
            &prev_mc_block_id,
            next_block_id_short.shard,
            None,
            DetectNextCollationStepContext::new(
                anchor_chain_time,
                force_mc_block,
                collation_config.mc_block_min_interval_ms as _,
                collation_config.mc_block_max_interval_ms as _,
                None,
            ),
        )
        .await
    }

    /// 1. Check if should collate master
    /// 2. And run master block collation
    /// 3. Or run next collation attempt in current shard
    async fn run_next_collation_step(
        &self,
        prev_mc_block_id: &BlockId,
        shard_id: ShardIdent,
        trigger_shard_block_id_opt: Option<BlockId>,
        ctx: DetectNextCollationStepContext,
    ) -> Result<Vec<CollatorJoinTask<CF::Collator>>> {
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

        let mut collator_tasks = vec![];

        match next_step {
            NextCollationStep::CollateMaster(next_mc_block_chain_time) => {
                if !shard_id.is_masterchain() {
                    // shard collator will wait and master collator will work
                    self.set_collator_state(&shard_id, |ac| ac.state = CollatorState::Waiting);
                }

                if let Some(task) = self
                    .run_mc_block_collation(
                        prev_mc_block_id.get_next_id_short(),
                        next_mc_block_chain_time,
                        trigger_shard_block_id_opt,
                    )
                    .await?
                {
                    collator_tasks.push(task);
                }
            }
            NextCollationStep::WaitForMasterStatus | NextCollationStep::WaitForShardStatus => {
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
                    if let Some(task) = self.run_try_collate(&shard_ident).await? {
                        collator_tasks.push(task);
                    }
                }
                if current_shard_should_wait {
                    self.set_collator_state(&shard_id, |ac| ac.state = CollatorState::Waiting);
                }
            }
        }

        Ok(collator_tasks)
    }

    fn handle_block_mismatch(&self, block_id: &BlockId) -> Result<()> {
        let labels = [("workchain", block_id.shard.workchain().to_string())];
        metrics::counter!("tycho_collator_block_mismatch_count", &labels).increment(1);

        // should drop uncommitted queue state on block mismatch
        // because created messages are incorrect
        self.clear_uncommitted_queue_state()?;

        Ok(())
    }

    fn clear_uncommitted_queue_state(&self) -> Result<()> {
        Self::clear_uncommitted_queue_state_impl(&self.blocks_cache, &self.mq_adapter)
    }

    fn clear_uncommitted_queue_state_impl(
        blocks_cache: &BlocksCache,
        mq_adapter: &Arc<dyn MessageQueueAdapter<EnqueuedMessage>>,
    ) -> Result<()> {
        tracing::info!(target: tracing_targets::COLLATION_MANAGER,
            "clear uncommitted queue state",
        );

        let top_shards = blocks_cache.get_last_top_shards();
        mq_adapter.clear_uncommitted_state(&top_shards)
    }

    /// Process collated block candidate
    /// 1. Save block to cache
    /// 2. Sync to last applied block from bc if required
    /// 3. For shard: just try run next collation attempt
    /// 4. For master:
    ///     - handle applied block in mempool
    ///     - spawn validation task
    ///     - execute updated master state processing routines
    #[tracing::instrument(
        skip_all,
        fields(block_id = %collation_result.candidate.block.id().as_short_id()),
    )]
    async fn handle_collated_block_candidate(
        self: &Arc<Self>,
        collation_result: BlockCollationResult,
    ) -> Result<HandleCollatorTaskResult<CF::Collator>> {
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

                return Ok(HandleCollatorTaskResult::empty());
            }
        };

        // check invariant: collator should not be in `CancelPending` or `Cancelled` state
        // when processing collated block candidate
        if let Some(collator_state) = self.get_collator_state(&block_id.shard) {
            assert!(
                !matches!(
                    collator_state,
                    CollatorState::CancelPending | CollatorState::Cancelled
                ),
                "collator should not be in 'cancel' state on block candidate processing",
            );
        }

        // store block to cache
        let store_res = {
            // if collator ws not cancelled then we consider that just collated block
            // should be correct and try store it to the cache
            let top_shard_blocks_info = collation_result
                .mc_data
                .as_ref()
                .map(|mc_data| {
                    mc_data
                        .shards
                        .iter()
                        .map(|(shard_id, shard_descr)| TopBlockIdUpdated {
                            block: TopBlockId {
                                ref_by_mc_seqno: shard_descr.reg_mc_seqno,
                                block_id: shard_descr.get_block_id(*shard_id),
                            },
                            updated: shard_descr.top_sc_block_updated,
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
                self.handle_block_mismatch(&block_id)?;

                tracing::info!(
                    target: tracing_targets::COLLATION_MANAGER,
                    ?store_res,
                    "saved block candidate to cache: mismatch",
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

            // should sync if collated block mismatched
            if store_res.block_mismatch {
                break 'check_should_sync true;
            }

            // check if should sync when we have applied blocks
            if let Some((_, applied_range_end)) = store_res.applied_mc_queue_range {
                // we can sync only when we have any applied block ahead
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
        let mut collator_tasks = vec![];
        if should_sync_to_last_applied_mc_block {
            // before sync will cancel any active collations
            Ok(HandleCollatorTaskResult {
                validator_task: None,
                collator_tasks: vec![],
                cancel_action: Some(ActionAfterCancel::SyncToAppliedMcBlock {
                    trigger_block_id_short: block_id.as_short_id(),
                    last_collated_mc_block_id: store_res.last_collated_mc_block_id,
                    applied_range: store_res.applied_mc_queue_range,
                    process_state_update_mode: ProcessMcStateUpdateMode::StartCollation {
                        reset_collators: true,
                    },
                }),
            })
        } else if store_res.block_mismatch {
            // only cancel all active collations
            // when block mismatched and newer already received
            Ok(HandleCollatorTaskResult {
                validator_task: None,
                collator_tasks: vec![],
                cancel_action: Some(ActionAfterCancel::Noop),
            })
        } else if block_id.is_masterchain() {
            // when candidate is master

            // if consensus config was changed we should wait until master block is validated
            if consensus_config_changed == Some(true) {
                let mut delayed_mc_state_update = self.delayed_mc_state_update.lock();
                *delayed_mc_state_update = collation_result.mc_data.clone();
            } else {
                // TODO: join notify and `process_mc_state_update` into one method
                // otherwise we can notify state update to mempool right now
                self.notify_mc_state_update_to_mempool(collation_result.mc_data.clone().unwrap())
                    .await?;
            }

            // process validation
            let mut validator_task = None;
            if store_res.received_and_collated {
                // NOTE: here commit will not cause on_block_accepted event
                //      because block already exist in bc state

                collator_tasks = self.commit_valid_master_block(&block_id, false).await?;
            } else {
                let validator = self.validator.clone();
                let validation_session_id = session_info.get_validation_session_id();
                validator_task = Some(JoinTask::new(async move {
                    let validation_result =
                        validator.validate(validation_session_id, &block_id).await?;
                    Ok((block_id, validation_result))
                }));
            }

            // if consensus config was not changed execute master state update processing routines right now
            if consensus_config_changed != Some(true) {
                // TODO: should simplify `if` branches to not check the `consensus_config_changed` in various places
                // check invariant
                assert!(
                    collator_tasks.is_empty(),
                    "when consensus config not changed the `commit_valid_master_block` should not run collator tasks"
                );

                collator_tasks = self
                    .process_mc_state_update(
                        collation_result.mc_data.unwrap(),
                        ProcessMcStateUpdateMode::StartCollation {
                            reset_collators: false,
                        },
                    )
                    .await?;
            }

            Ok(HandleCollatorTaskResult {
                validator_task,
                collator_tasks,
                cancel_action: None,
            })
        } else {
            // when candidate is shard

            // run master block collation if required or resume collation attempts in shard
            tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                "will run next collation step",
            );

            collator_tasks = self
                .run_next_collation_step(
                    &collation_result.prev_mc_block_id,
                    block_id.shard,
                    Some(block_id),
                    DetectNextCollationStepContext::new(
                        candidate_chain_time,
                        collation_result.force_next_mc_block,
                        collation_result.collation_config.mc_block_min_interval_ms as _,
                        collation_result.collation_config.mc_block_max_interval_ms as _,
                        Some(CollatedBlockInfo::new(
                            collation_result.prev_mc_block_id.seqno,
                            collation_result.has_processed_externals,
                        )),
                    ),
                )
                .await?;

            Ok(HandleCollatorTaskResult {
                validator_task: None,
                collator_tasks,
                cancel_action: None,
            })
        }
    }

    /// Finish active sync if it is not finished yet
    /// and enqueue received block
    #[tracing::instrument(skip_all, fields(block_id = %ctx.state.block_id().as_short_id()))]
    async fn enqueue_handle_block_from_bc(
        &self,
        ctx: HandledBlockFromBcCtx,
        blocks_from_bc_queue_sender: &tokio::sync::mpsc::Sender<HandledBlockFromBcCtx>,
    ) -> Result<()> {
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

        // enqueue received block for processing
        blocks_from_bc_queue_sender.send(ctx).await?;

        Ok(())
    }

    async fn handle_blocks_from_bc_batch(
        self: &Arc<Self>,
        batch: Vec<HandledBlockFromBcCtx>,
    ) -> Result<HandleBlockFromBcResult<CF::Collator>> {
        // find last master block in buffer
        // will skip sync for all master blocks before it
        let mut last_mc_block_id_opt = None;
        for HandledBlockFromBcCtx { state, .. } in batch.iter().rev() {
            if state.block_id().is_masterchain() {
                last_mc_block_id_opt = Some(*state.block_id());
            }
        }

        let mut collator_tasks = vec![];
        let mut cancel_action = None;

        for ctx in batch {
            let is_last_mc_block_in_batch = matches!(
                last_mc_block_id_opt, Some(last_mc_block_id) if ctx.state.block_id() == &last_mc_block_id
            );

            let shard_id = ctx.state.block_id().shard;

            // handle block from bc
            let res = self
                .handle_block_from_bc(ctx, is_last_mc_block_in_batch)
                .await
                .map_err(|err| {
                    tracing::error!(target: tracing_targets::COLLATION_MANAGER,
                        "error handling block from bc: {err:?}",
                    );
                    err
                })?;
            // TODO: `refresh_collations_sessions` should return only `init` or `resume` tasks,
            //      which can be awaited here, and `try_collate` should be called from the main flow
            // check invariants
            if !is_last_mc_block_in_batch {
                assert!(
                    res.collator_tasks.is_empty(),
                    "only last master block should run collator tasks",
                );
            }
            if !shard_id.is_masterchain() {
                assert!(
                    res.collator_tasks.is_empty(),
                    "should not run collator tasks on shard block processing",
                );
            }
            // get collator tasks only from the last master block processing
            if !res.collator_tasks.is_empty() {
                collator_tasks = res.collator_tasks;
            }
            // get the most actual action after cancel
            if res.cancel_action.is_some() {
                cancel_action = res.cancel_action;
            }
        }

        Ok(HandleBlockFromBcResult {
            collator_tasks,
            cancel_action,
        })
    }

    /// Process new block from blockchain:
    /// 1. Save block to cache
    /// 2. Stop block validation if needed
    /// 3. Commit block if it was collated first
    /// 4. Notify mempool about new master block
    /// 5. Sync to received block if it is far ahead last collated and last `synced_to`
    #[tracing::instrument(skip_all, fields(block_id = %ctx.state.block_id().as_short_id(), is_last_mc_block_in_batch))]
    async fn handle_block_from_bc(
        self: &Arc<Self>,
        ctx: HandledBlockFromBcCtx,
        is_last_mc_block_in_batch: bool,
    ) -> Result<HandleBlockFromBcResult<CF::Collator>> {
        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            "start processing block from bc",
        );

        let block_id = *ctx.state.block_id();
        debug_assert!(!block_id.is_masterchain() || block_id == ctx.mc_block_id);

        let _histogram = HistogramGuard::begin("tycho_collator_handle_block_from_bc_time");

        let Some(store_res) = self
            .blocks_cache
            .store_received(
                self.state_node_adapter.clone(),
                &ctx.mc_block_id,
                ctx.state.clone(),
                ProcessedUptoInfoStuff::try_from(ctx.processed_upto)?,
            )
            .await?
        else {
            // received block was not stored
            // because it is not newer than existed
            return Ok(HandleBlockFromBcResult::empty());
        };

        if store_res.block_mismatch {
            self.handle_block_mismatch(&block_id)?;

            tracing::info!(target: tracing_targets::COLLATION_MANAGER,
                ?store_res,
                "saved block from bc to cache: mismatch",
            );
        } else {
            tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                ?store_res,
                "saved block from bc to cache",
            );
        }

        // we cannot sync to applied shard block
        if !block_id.is_masterchain() {
            if store_res.block_mismatch {
                // should cancel all active collations when block mismatched
                return Ok(HandleBlockFromBcResult {
                    collator_tasks: vec![],
                    cancel_action: Some(ActionAfterCancel::Noop),
                });
            } else {
                // just do nothing otherwise
                return Ok(HandleBlockFromBcResult::empty());
            }
        }

        // check if received is a key block
        let is_key_block = ctx.state.state_extra()?.after_key_block;

        // stop any running validations up to this block
        self.schedule_cancel_validation_sessions_until_block(ctx.state.clone());

        // check if should sync to last applied mc block right now
        let should_sync_to_last_applied_mc_block = 'check_should_sync: {
            // sync only to the last master block from batch or to a key block
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

            // should sync if collated block mismatched
            if store_res.block_mismatch {
                break 'check_should_sync true;
            }

            // check if should sync
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
                        tracing::info!(target: tracing_targets::COLLATION_MANAGER,
                            last_synced_to_mc_block_id = ?self.get_last_synced_to_mc_block_id().map(|id| id.as_short_id().to_string()),
                            last_collated_mc_block_id = ?store_res.last_collated_mc_block_id.map(|id| id.as_short_id().to_string()),
                            last_processed_mc_block_id = ?self.get_last_processed_mc_block_id().map(|id| id.as_short_id().to_string()),
                            received_is_key_block = is_key_block,
                            has_active_collations = self.has_active_collations(),
                            "check_should_sync: will request sync to last applied mc block",
                        );
                    }

                    should_sync
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

        // run sync or commit block
        if should_sync_to_last_applied_mc_block {
            // should only refresh collation sessions when sync on key block
            let process_state_update_mode = if is_last_mc_block_in_batch {
                ProcessMcStateUpdateMode::StartCollation {
                    reset_collators: true,
                }
            } else {
                ProcessMcStateUpdateMode::RefreshSessionsOnly
            };
            // will cancel active collations and then run sync
            Ok(HandleBlockFromBcResult {
                collator_tasks: vec![],
                cancel_action: Some(ActionAfterCancel::SyncToAppliedMcBlock {
                    trigger_block_id_short: block_id.as_short_id(),
                    last_collated_mc_block_id: store_res.last_collated_mc_block_id,
                    applied_range: store_res.applied_mc_queue_range,
                    process_state_update_mode,
                }),
            })
        } else if store_res.block_mismatch {
            // only cancel all active collations
            // when block mismatched and should not sync by any reason
            Ok(HandleBlockFromBcResult {
                collator_tasks: vec![],
                cancel_action: Some(ActionAfterCancel::Noop),
            })
        } else {
            let mut collator_tasks = vec![];

            // try to commit block if it was collated first
            if store_res.received_and_collated {
                // NOTE: here commit will not cause on_block_accepted event
                //      because block already exist in bc state

                collator_tasks = self.commit_valid_master_block(&block_id, false).await?;
            }

            Ok(HandleBlockFromBcResult {
                collator_tasks,
                cancel_action: None,
            })
        }
    }

    #[tracing::instrument(name = "sync_to_applied_mc_block", skip_all, fields(trigger_block_id = %trigger_block_id_short, applied_range = ?applied_range))]
    async fn sync_to_applied_mc_block_if_exist(
        self: &Arc<Self>,
        trigger_block_id_short: BlockIdShort,
        last_collated_mc_block_id: Option<BlockId>,
        applied_range: Option<(BlockSeqno, BlockSeqno)>,
        process_state_update_mode: ProcessMcStateUpdateMode,
    ) -> Result<Vec<CollatorJoinTask<CF::Collator>>> {
        let mut collator_tasks = vec![];

        if let Some(applied_range) = applied_range {
            let this = self.clone();
            let span = tracing::Span::current();

            collator_tasks = tokio::task::spawn_blocking(move || {
                let _ = span.enter();

                this.sync_to_applied_mc_block(
                    trigger_block_id_short,
                    last_collated_mc_block_id,
                    applied_range,
                    process_state_update_mode,
                )
            })
            .await??;
        } else {
            tracing::info!(target: tracing_targets::COLLATION_MANAGER,
                "there is no received applied mc blocks in cache, \
                will wait for next blocks from bc",
            );
        }

        Ok(collator_tasks)
    }

    /// Restores internals queue state,
    /// processes last applied mc state
    /// to run next blocks collation.
    #[tracing::instrument(skip_all, fields(trigger_block_id = %trigger_block_id_short, applied_range = ?applied_range))]
    fn sync_to_applied_mc_block(
        &self,
        trigger_block_id_short: BlockIdShort,
        last_collated_mc_block_id: Option<BlockId>,
        applied_range: (BlockSeqno, BlockSeqno),
        process_state_update_mode: ProcessMcStateUpdateMode,
    ) -> Result<Vec<CollatorJoinTask<CF::Collator>>> {
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

                // E.g. we have applied mc block MC100 and collated some shard blocks after it (SC701, SC702)
                // so we have uncommitted queue diffs from these shard blocks SC701, SC702.
                // On restart from genesis we will start to collate SC701* again because last validated
                // and applied mc block is MC100. But mempool will not contain all old externals used to collate
                // the previous version of SC701. So the new diff will mismatch the old one and node will panic.
                // So we need to remove all uncommitted queue diffs because we have new mismatched externals queue.
                self.clear_uncommitted_queue_state()?;
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
            .await_blocking()?;

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
        let queue_diffs_applied_to_top_blocks = if self.config.fast_sync
            && let Some(applied_to_mc_block_id) =
                self.get_queue_diffs_applied_to_mc_block_id(last_collated_mc_block_id)?
        {
            // BACKWARD COMPATIBILITY: `last_committed_mc_block_id` will not exist in queue storage
            // in previous version. We will receive None and will use all required diffs to restore the queue

            // NOTE: when the node started to sync from a persistent state with a bunch of archives after it,
            //      the `last_collated_mc_block_id` will be None, and `applied_to_mc_block_id` will be equal
            //      to the top block of the persistent state - init block. But the persistent state can be already
            //      removed because of the long history after it when collator starts to sync by recent blocks.
            //      So we will not able to read top blocks ids and will fallback the full sync.

            // collect top blocks queue diffs already applied to
            Self::get_top_blocks_seqno(
                &applied_to_mc_block_id,
                &self.blocks_cache,
                self.state_node_adapter.clone(),
            )
            .await_blocking()?
        } else {
            None
        };
        if queue_diffs_applied_to_top_blocks.is_some() {
            tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                ?queue_diffs_applied_to_top_blocks,
                "will use fast sync to restore queue",
            );
        }

        // restore queue state and return latest applied master state
        let queue_restore_res = Self::restore_queue(
            &self.blocks_cache,
            self.state_node_adapter.clone(),
            self.mq_adapter.clone(),
            applied_range.0,
            min_processed_to_by_shards,
            before_tail_block_ids,
            queue_diffs_applied_to_top_blocks.unwrap_or_default(),
        )
        .await_blocking()?;

        let Some(last_mc_state) = queue_restore_res.last_mc_state else {
            tracing::info!(target: tracing_targets::COLLATION_MANAGER,
                last_applied_mc_block_id = %BlockIdShort {
                    shard: ShardIdent::MASTERCHAIN,
                    seqno: applied_range.1,
                },
                "sync_to_applied_mc_block: unable to sync to last applied mc block, \
                need to receive next blocks from bc",
            );
            return Ok(vec![]);
        };

        // process latest master state: notify mempool and refresh collation session
        let last_mc_block_id = *last_mc_state.block_id();

        // HACK: do not need to set master block latest chain time from zerostate when using mempool stub
        //      because anchors from stub have older chain time than in zerostate and it will brake collation
        if last_mc_block_id.seqno > self.state_node_adapter.zerostate_id().seqno {
            Self::renew_mc_block_latest_chain_time(
                &mut self.collation_sync_state.lock(),
                last_mc_state.get_gen_chain_time(),
            );
        }

        Self::reset_collation_sync_status(&mut self.collation_sync_state.lock());

        // update last "synced to" info
        self.update_last_synced_to_mc_block_id(last_mc_block_id);

        // reset top shard blocks info
        // because next we will start to collate new shard blocks after the sync
        self.blocks_cache.reset_top_shard_blocks_additional_info();

        let mc_data = Self::build_mc_data(
            &self.state_node_adapter,
            last_mc_state,
            queue_restore_res.prev_mc_state,
            queue_restore_res.prev_mc_block_id,
            all_shards_processed_to_by_partitions,
        )?;

        self.blocks_cache
            .remove_next_collated_blocks_from_cache(&queue_restore_res.synced_to_blocks_keys);

        // notify state update to mempool
        self.notify_mc_state_update_to_mempool(mc_data.clone())
            .await_blocking()?;

        // when sync cancelled we do not exist sync but skip collation
        let process_state_update_mode = if cancelled.is_cancelled() {
            ProcessMcStateUpdateMode::SkipProcess
        } else {
            process_state_update_mode
        };

        let collator_tasks = self
            .process_mc_state_update(mc_data.clone(), process_state_update_mode)
            .await_blocking()?;

        // handle top processed to anchor in mempool
        self.notify_top_processed_to_anchor_to_mempool(
            mc_data.block_id.seqno,
            mc_data.top_processed_to_anchor,
        )
        .await_blocking()?;

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

        Ok(collator_tasks)
    }

    /// Builds `McData` from provided parts.
    /// Tries to load the previous mc state from storage when passed `None`.
    fn build_mc_data(
        state_node_adapter: &Arc<dyn StateNodeAdapter>,
        last_mc_state: ShardStateStuff,
        prev_mc_state: Option<ShardStateStuff>,
        prev_mc_block_id: Option<BlockId>,
        all_shards_processed_to_by_partitions: FastHashMap<
            ShardIdent,
            (bool, ProcessedToByPartitions),
        >,
    ) -> Result<Arc<McData>> {
        let prev_mc_state = match (prev_mc_state, prev_mc_block_id) {
            (Some(mc_state), _) => Some(mc_state),
            (None, None) => None,
            (None, Some(prev_mc_block_id)) if prev_mc_block_id.seqno <= state_node_adapter.zerostate_id().seqno => None,
            // NOTE: Use zero epoch here since we don't need to reuse these states.
            (None, Some(prev_mc_block_id)) => state_node_adapter
                .load_state(0, &prev_mc_block_id, Default::default())
                .await_blocking()
                .map_err(|err| {
                    tracing::warn!(target: tracing_targets::COLLATION_MANAGER,
                        prev_mc_block_id = %prev_mc_block_id.as_short_id(),
                        error = ?err,
                        "failed to load previous mc state to build mc data, continue without prev_mc_data",
                    );
                    err
                }).ok(),
        };

        McData::load_from_state(
            &last_mc_state,
            prev_mc_state.as_ref(),
            all_shards_processed_to_by_partitions,
        )
    }

    async fn get_all_shards_processed_to_by_partitions_for_mc_block(
        mc_block_key: &BlockCacheKey,
        blocks_cache: &BlocksCache,
        state_node_adapter: Arc<dyn StateNodeAdapter>,
    ) -> Result<FastHashMap<ShardIdent, (bool, ProcessedToByPartitions)>> {
        let mut result = FastHashMap::default();

        let zerostate_mc_seqno = blocks_cache.zerostate_mc_seqno();
        if mc_block_key.seqno <= zerostate_mc_seqno {
            return Ok(result);
        }

        let from_cache = blocks_cache.get_top_blocks_processed_to_by_partitions(mc_block_key)?;

        for (top_block_id, item) in from_cache {
            let processed_to = match item.by {
                Some(processed_to) => processed_to,
                None => {
                    if item.ref_by_mc_seqno <= zerostate_mc_seqno {
                        FastHashMap::default()
                    } else {
                        // get from state
                        let state = state_node_adapter
                            .load_state(mc_block_key.seqno, &top_block_id, LoadStateHint {
                                // State must already be applied at this point.
                                allow_ignore_direct: false,
                            })
                            .await?;
                        let processed_upto = state.state().processed_upto.load()?;
                        let processed_upto = ProcessedUptoInfoStuff::try_from(processed_upto)?;
                        processed_upto.get_internals_processed_to_by_partitions()
                    }
                }
            };

            result.insert(top_block_id.shard, (item.updated, processed_to));
        }

        Ok(result)
    }

    // Returns top master block id upto which all queue diffs applied
    fn get_queue_diffs_applied_to_mc_block_id(
        &self,
        last_collated_mc_block_id: Option<BlockId>,
    ) -> Result<Option<BlockId>> {
        let last_queue_comitted_on = self.mq_adapter.get_last_committed_mc_block_id()?;
        let last_collated_or_synced_to =
            self.get_top_mc_block_id_for_next_collation(last_collated_mc_block_id);

        let mc_block_id = match (last_queue_comitted_on, last_collated_or_synced_to) {
            (Some(last_queue_comitted_on), Some(last_collated_or_synced_to)) => {
                // return last collated if it exists (or last "synced to")
                // if above mc block on which the queue was committed
                if last_collated_or_synced_to.seqno >= last_queue_comitted_on.seqno {
                    Some(last_collated_or_synced_to)
                } else {
                    Some(last_queue_comitted_on)
                }
            }
            (Some(mc_block_id), _) | (_, Some(mc_block_id)) => Some(mc_block_id),
            _ => None,
        };

        Ok(mc_block_id)
    }

    /// Return last collated if it is ahead of last received and "synced to".
    /// Or last received and "synced to" if it is ahead of last correct collated.
    /// Last collated may be incorrect but we don not know this until we receive block from bc.
    /// We use this to detect the lag from last received to check if we need to run next sync.
    #[tracing::instrument(skip_all)]
    fn get_top_mc_block_id_for_next_collation(
        &self,
        last_collated_mc_block_id: Option<BlockId>,
    ) -> Option<BlockId> {
        let last_synced_to_mc_block_id = self.get_last_synced_to_mc_block_id();

        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            last_synced_to_mc_block_id = ?DebugDisplayOpt(&last_synced_to_mc_block_id),
            last_collated_mc_block_id = ?DebugDisplayOpt(&last_collated_mc_block_id),,
        );

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
                // NOTE: We don't skip prev block ids for shard zerostates because
                // it is quite hard to propagate `ref_by_mc_seqno` here (we construct
                // prev ids based just on `BlockId` here). There seems to be no problems
                // with that because we are checking `init_mc_block_reached` using
                // the handle data so zerostate ids will be skipped in any case.
                // This check is just to not change the old behavior just in case.
                if prev_block_id.seqno == 0
                    || prev_block_id.is_masterchain()
                        && prev_block_id.seqno <= state_node_adapter.zerostate_id().seqno
                {
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

        // will track last mc state and previous before it
        let mut prev_mc_state = None;
        let mut last_mc_state;

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

            // apply queue diffs from blocks above zerostate seqno
            // skip cached diffs below min_processed_to
            if subgraph.master_block.block_id.seqno > state_node_adapter.zerostate_id().seqno {
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
                            state_node_adapter.as_ref(),
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

            last_mc_state = mc_block_entry.cached_state()?.clone();

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
                Self::clear_uncommitted_queue_state_impl(blocks_cache, &mq_adapter)?;

                res.last_mc_state = Some(last_mc_state);
                res.prev_mc_state = prev_mc_state;
                res.prev_mc_block_id = mc_block_entry.prev_blocks_ids.first().copied();
                res.synced_to_blocks_keys.extend(to_blocks_keys.into_iter());

                return Ok(res);
            }

            prev_mc_state = Some(last_mc_state.clone());
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
    ) -> Result<Vec<CollatorJoinTask<CF::Collator>>> {
        tracing::info!(target: tracing_targets::COLLATION_MANAGER,
            ?mode,
            "will process master state update",
        );

        let mut collator_tasks = vec![];

        if !matches!(mode, ProcessMcStateUpdateMode::SkipProcess) {
            let block_global = mc_data.config.get_global_version()?;
            if self.config.supported_block_version >= block_global.version
                && block_global
                    .capabilities
                    .is_subset_of(self.config.supported_capabilities)
            {
                collator_tasks = self.refresh_collation_sessions(mc_data, mode).await?;
            } else {
                tracing::warn!(target: tracing_targets::COLLATION_MANAGER,
                    collator_supported_block_version = self.config.supported_block_version,
                    mc_block_version = block_global.version,
                    collator_supported_capabilities = ?self.config.supported_capabilities,
                    mc_block_capabilities = ?block_global.capabilities,
                    "Refresh collation sessions is skipped: collator does not support mc block version or capabilities",
                );
            }
        }

        Ok(collator_tasks)
    }

    /// Returns top processed to anchor id if delayed state was processed
    async fn notify_to_mempool_and_process_delayed_mc_state_update(
        &self,
        block_id: &BlockId,
        skip_process_delayed_mc_state_update: bool,
    ) -> Result<Option<(MempoolAnchorId, Vec<CollatorJoinTask<CF::Collator>>)>> {
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

            // validation may finish when collation cancel task is running,
            // so we should not run new collation tasks
            let mut collator_tasks = vec![];
            if !skip_process_delayed_mc_state_update {
                collator_tasks = self
                    .process_mc_state_update(
                        mc_data.clone(),
                        ProcessMcStateUpdateMode::StartCollation {
                            reset_collators: false,
                        },
                    )
                    .await?;
            }

            Ok(Some((mc_data.top_processed_to_anchor, collator_tasks)))
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

        let cx = Box::new(StateUpdateContext {
            mc_block_id: mc_data.block_id,
            mc_block_chain_time: mc_data.gen_chain_time,
            top_processed_to_anchor_id: mc_data.top_processed_to_anchor,
            consensus_info: mc_data.consensus_info,
            shuffle_validators: mc_data.config.get_collation_config()?.shuffle_mc_validators,
            consensus_config: mc_data.config.get_consensus_config()?,
            prev_validator_set,
            current_validator_set,
            next_validator_set,
        });

        self.mpool_adapter.handle_mc_state_update(cx).await
    }

    /// Get shards and validator set info from the master state,
    /// then start missing collation sessions, finish outdated, resume actual.
    /// Start/stop/resume collators and validators.
    #[tracing::instrument(skip_all)]
    async fn refresh_collation_sessions(
        &self,
        mc_data: Arc<McData>,
        mode: ProcessMcStateUpdateMode,
    ) -> Result<Vec<CollatorJoinTask<CF::Collator>>> {
        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            "Start refresh collation sessions by mc state ({})...",
            mc_data.block_id.as_short_id(),
        );

        let _histogram = HistogramGuard::begin("tycho_collator_refresh_collation_sessions_time");

        let mut collator_tasks = vec![];

        // NOTE: in case of processing delayed state update
        //      after the validation of key block with consensus config changed
        //      we may try to process the state that is older then already processed from bc

        // do not re-process this master block if it is lower then last processed or equal to it
        // but process a new version of block with the same seqno
        if !self.check_should_process_and_update_last_processed_mc_block(&mc_data.block_id) {
            return Ok(collator_tasks);
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

        // apply split/merge actions
        self.apply_split_merge_actions(&new_shards_info)?;

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
            let mut missed_shards_ids: FastHashSet<_> =
                active_collation_sessions_guard.keys().cloned().collect();
            for (shard_id, block_ids) in new_shards_info {
                missed_shards_ids.remove(&shard_id);

                // check if current node is in subset
                let (subset, hash_short) = get_validator_subset(shard_id)?;
                let local_pubkey = find_us_in_collators_set(&self.keypair, &subset);

                if local_pubkey.is_none() {
                    tracing::debug!(
                        target: tracing_targets::COLLATION_MANAGER,
                        public_key = %self.keypair.public_key,
                        current_session_seqno,
                        hash_short,
                        "Current node was not authorized to collate shard {}. Use TRACE to see subset",
                        shard_id,
                    );
                    tracing::trace!(target: tracing_targets::COLLATION_MANAGER,
                        subset = ?DebugIter(subset.values()),
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
                        .create(CollatorContext {
                            mq_adapter: self.mq_adapter.clone(),
                            mpool_adapter: self.mpool_adapter.clone(),
                            state_node_adapter: self.state_node_adapter.clone(),
                            config: self.config.clone(),
                            collation_session: new_session_info.clone(),
                            zerostate_id: *self.state_node_adapter.zerostate_id(),
                            shard_id,
                            prev_blocks_ids: prev_blocks_ids.clone(),
                            mempool_config_override: self.mempool_config_override.clone(),
                            cancel_collation: cancel_collation_notify.clone(),
                        })
                        .await
                    {
                        Err(err) => {
                            tracing::error!(target: tracing_targets::COLLATION_MANAGER,
                                session_id = ?new_session_info.id(),
                                ?err,
                                "error creating collator"
                            );
                            // return error to stop node
                            return Err(err);
                        }
                        Ok(collator) => {
                            let collator = Box::new(collator);

                            // init collator
                            let collator =
                                if let ProcessMcStateUpdateMode::StartCollation { .. } = mode {
                                    let init_collator_task = JoinTask::new(
                                        collator.init(prev_blocks_ids, mc_data.clone()),
                                    );
                                    collator_tasks.push(init_collator_task);
                                    None
                                } else {
                                    Some(collator)
                                };

                            entry.insert(ActiveCollator {
                                state: if collator.is_none() {
                                    CollatorState::Active
                                } else {
                                    CollatorState::Waiting
                                },
                                collator,
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
            if let ProcessMcStateUpdateMode::StartCollation { reset_collators } = mode {
                let collator = self
                    .take_collator_and_set_state_if(
                        &shard_id,
                        |_| true,
                        |ac| ac.state = CollatorState::Active,
                    )
                    .with_context(|| {
                        format!(
                            "Collator for shard should exist for active session {:?}",
                            new_session_info.id()
                        )
                    })?
                    .context("an empty check should pass")?;
                tracing::debug!(
                    target: tracing_targets::COLLATION_MANAGER,
                    "Resuming collation attempts in shard session {:?}",
                    new_session_info.id(),
                );
                // resume collation
                let resume_collation_task = JoinTask::new(collator.resume_collation(
                    mc_data.clone(),
                    reset_collators,
                    new_session_info,
                    prev_blocks_ids,
                ));
                collator_tasks.push(resume_collation_task);
            } else {
                // just reset collator state
                self.set_collator_state(&shard_id, |ac| ac.state = CollatorState::Waiting);
            }
        }

        // finish outdated collation sessions
        if !to_finish_sessions.is_empty() {
            tracing::info!(target: tracing_targets::COLLATION_MANAGER,
                "Will finish outdated collation sessions: {:?}",
                DebugIter(to_finish_sessions.iter().map(|s| s.id())),
            );
        }

        // stop dangling collators
        if !to_stop_collators.is_empty() {
            tracing::info!(target: tracing_targets::COLLATION_MANAGER,
                "Will stop collators for sessions that we do not serve: {:?}",
                DebugIter(to_stop_collators.iter().map(|(s, _)| s.id())),
            );
        }

        Ok(collator_tasks)

        // finally we will have initialized `active_collation_sessions`
        // and `active_collators` which run async block collations processes
    }

    fn apply_split_merge_actions(
        &self,
        new_shards_info: &FastHashMap<ShardIdent, Vec<BlockId>>,
    ) -> Result<()> {
        let active_shards_ids: Vec<_> = self
            .active_collation_sessions
            .read()
            .keys()
            .cloned()
            .collect();
        let new_shards_ids: Vec<&ShardIdent> = new_shards_info.keys().collect();

        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            "Detecting split/merge actions to move from current shards {:?} to new shards {:?}...",
            active_shards_ids.as_slice(),
            new_shards_ids.as_slice(),
        );

        let split_merge_actions = calc_split_merge_actions(&active_shards_ids, new_shards_ids)?;
        if !split_merge_actions.is_empty() {
            tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                "Detected split/merge actions: {:?}",
                split_merge_actions,
            );
        }

        Ok(())
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
    /// when master collation check was finished first then it will run one more resume for shard,
    /// so we will have two parallel collations for shard that will cause panic further.
    fn reset_collation_sync_status(guard: &mut CollationSyncState) {
        for (_, collation_state) in guard.states.iter_mut() {
            if collation_state.status == CollationStatus::WaitForMasterStatus {
                collation_state.status = CollationStatus::AttemptsInProgress;
            }
        }
    }

    /// 1. Store collation status for current shard
    /// 2. Detect the next step
    #[tracing::instrument(skip_all)]
    fn detect_next_collation_step(
        guard: &mut CollationSyncState,
        active_shards: Vec<ShardIdent>,
        shard_id: ShardIdent,
        ctx: DetectNextCollationStepContext,
    ) -> NextCollationStep {
        let DetectNextCollationStepContext {
            last_imported_anchor_ct,
            force_mc_block,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            collated_block_info,
        } = ctx;

        // backward compatibility: if max interval is not defined take it equal to min interval
        let mc_block_max_interval_ms = if mc_block_max_interval_ms == 0 {
            mc_block_min_interval_ms
        } else {
            mc_block_max_interval_ms
        };

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
        }

        // Another forcing rule: no pending messages after shard blocks
        if matches!(
            force_mc_block,
            ForceMasterCollation::NoPendingMessagesAfterShardBlocks
        ) {
            guard.mc_forced_by_no_pending_msgs_on_ct = Some(last_imported_anchor_ct);
        }

        let hard_forced_for_all = guard.mc_collation_forced_for_all;
        let mc_forced_by_no_pending_msgs_on_ct = guard.mc_forced_by_no_pending_msgs_on_ct;

        // Determine if the current shard collation is explicitly forced in new anchor event
        let forced_in_current_shard = force_mc_block.is_forced();

        // Add new anchor event for current shard
        tracing::trace!(
            target: tracing_targets::COLLATION_MANAGER,
            shard_id=?shard_id,
            last_imported_anchor_ct,
            forced_in_current_shard,
            ?collated_block_info,
            "anchor event appended"
        );
        let current_collation_state = guard.states.entry(shard_id).or_default();
        current_collation_state
            .last_imported_anchor_events
            .push(ImportedAnchorEvent {
                ct: last_imported_anchor_ct,
                mc_forced: forced_in_current_shard,
                collated_block_info,
            });

        // Per-shard facts to simplify decision-making
        #[derive(Debug, Clone)]
        struct ShardFact {
            shard_id: ShardIdent,
            status: CollationStatus,   // current collation status for shard
            first_ct: Option<u64>,     // first ct
            mc_forced_ct: Option<u64>, // first ct on which mc block collation forced
            min_ct: Option<u64>,       // first ct >= min interval
            max_ct: Option<u64>,       // first ct >= max interval
            has_shard_block_with_externals: bool, // produced shard block with externals
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
                    has_shard_block_with_externals: false,
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

                let mut last_known_collated_block_info: Option<CollatedBlockInfo> = None;

                for s in &state.last_imported_anchor_events {
                    // check for the first shard block with externals
                    let mut is_first_shard_block_with_externals = false;
                    if let Some(curr_b_info) = s.collated_block_info {
                        // find first block after previous master
                        let is_first_after_prev_master = match &last_known_collated_block_info {
                            Some(last_b_info)
                                if last_b_info.prev_mc_block_seqno
                                    < curr_b_info.prev_mc_block_seqno =>
                            {
                                true
                            }
                            None => true,
                            _ => false,
                        };

                        if !shard_id.is_masterchain() {
                            // if found then will seek for the first shard block with externals
                            if is_first_after_prev_master {
                                fact.has_shard_block_with_externals = false;
                            }

                            // remember the first shard block with externals
                            is_first_shard_block_with_externals = curr_b_info
                                .has_processed_externals
                                && !fact.has_shard_block_with_externals;
                            if is_first_shard_block_with_externals {
                                fact.has_shard_block_with_externals = true;
                            }
                        }

                        last_known_collated_block_info = Some(curr_b_info);
                    }

                    if fact.first_ct.is_none() {
                        fact.first_ct = Some(s.ct);
                    }

                    if s.mc_forced && fact.mc_forced_ct.is_none() {
                        fact.mc_forced_ct = Some(s.ct);
                    }

                    // we take only first ct that exceed min interval
                    // or the next that goes with the first shard block with externals
                    if (fact.min_ct.is_none() || is_first_shard_block_with_externals)
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
            hard_forced_for_all,
            mc_forced_by_no_pending_msgs_on_ct,
            ?facts,
            "calculated shard facts"
        );

        let any_has_shard_block_with_externals =
            facts.iter().any(|f| f.has_shard_block_with_externals);

        fn choose_candidate(
            curr_sid: &ShardIdent,
            f: &ShardFact,
            mc_forced_by_shard_on_ct: Option<u64>,
            any_has_shard_block_with_externals: bool,
            hard_forced_for_all: bool,
        ) -> Option<u64> {
            // take into account shard if it is current
            // or it is ready to collate or waiting
            let ready_to_detect_next_step = f.shard_id == *curr_sid
                || matches!(
                    f.status,
                    CollationStatus::ReadyToCollateMaster
                        | CollationStatus::WaitForMasterStatus
                        | CollationStatus::WaitForShardStatus
                );

            // chain time on which master was forced
            f.mc_forced_ct
                // first ct if master was forced for all
                .or(if hard_forced_for_all {
                    f.first_ct
                } else {
                    None
                })
                .or(
                    // chain time that exceed min interval or when was forced by shard
                    // when any shard has collated shard block with externals
                    // or any shard has forced master block (e.g. by no more pending messages)
                    // if shard is ready to detect next step
                    if any_has_shard_block_with_externals || mc_forced_by_shard_on_ct.is_some() {
                        if ready_to_detect_next_step {
                            f.min_ct.map(|min_ct| {
                                mc_forced_by_shard_on_ct
                                    .map_or(min_ct, |forced_ct| min_ct.max(forced_ct))
                            })
                        } else {
                            None
                        }
                    }
                    // finally take chain time that exceed max interval
                    else {
                        f.max_ct
                    },
                )
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
                    any_has_shard_block_with_externals,
                    hard_forced_for_all,
                );
                if f.shard_id == shard_id && ct.is_some() {
                    should_collate_by_current_shard = true;
                }
                ct
            })
            .collect();

        // check if should collate by every shard
        let should_collate_by_every_shard =
            !candidates.is_empty() && candidates.iter().all(|c| c.is_some());

        tracing::debug!(
            target: tracing_targets::COLLATION_MANAGER,
            shard_id=?shard_id,
            ?candidates,
            should_collate_by_current_shard,
            should_collate_by_every_shard,
            any_has_shard_block_with_externals,
            hard_forced_for_all,
            mc_forced_by_no_pending_msgs_on_ct,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            "candidates collected"
        );

        // If not all shards ready to collate next master then resume attempts
        if !should_collate_by_every_shard {
            let mut shards_to_resume_attempts = vec![];

            // check if other shards ready to collate
            let all_other_shards_ready_to_collate = guard.states.iter().all(|(sid, s)| {
                *sid == shard_id || s.status == CollationStatus::ReadyToCollateMaster
            });

            // If current shard is not ready to collate master then try to resume attempts
            let current_collation_state = guard.states.entry(shard_id).or_default();
            if !should_collate_by_current_shard {
                if !mc_collation_state_exist {
                    // wait for master collation status if it not exist yet
                    current_collation_state.status = CollationStatus::WaitForMasterStatus;
                } else if shard_id.is_masterchain() && !all_other_shards_ready_to_collate {
                    // master always wait for next shard event if any is not ready to collate
                    // not to wait for one more anchor if shard forces master collation
                    current_collation_state.status = CollationStatus::WaitForShardStatus;
                } else {
                    // or resume attempts right now
                    current_collation_state.status = CollationStatus::AttemptsInProgress;
                    shards_to_resume_attempts.push(shard_id);
                }
            } else {
                // otherwise it is ready to collate master
                current_collation_state.status = CollationStatus::ReadyToCollateMaster;
            };

            for (sid, collation_state) in guard.states.iter_mut() {
                // master resumes all waiting shards
                if (shard_id.is_masterchain()
                            && collation_state.status == CollationStatus::WaitForMasterStatus)
                            // shard resumes waiting master
                            || (!shard_id.is_masterchain()
                            && collation_state.status == CollationStatus::WaitForShardStatus)
                {
                    collation_state.status = CollationStatus::AttemptsInProgress;
                    shards_to_resume_attempts.push(*sid);
                }
            }

            let res = if !shards_to_resume_attempts.is_empty() {
                NextCollationStep::ResumeAttemptsIn(shards_to_resume_attempts)
            } else if shard_id.is_masterchain() {
                NextCollationStep::WaitForShardStatus
            } else {
                NextCollationStep::WaitForMasterStatus
            };

            tracing::info!(
                target: tracing_targets::COLLATION_MANAGER,
                shard_id=?shard_id,
                ?candidates,
                should_collate_by_current_shard,
                should_collate_by_every_shard,
                any_has_shard_block_with_externals,
                hard_forced_for_all,
                mc_forced_by_no_pending_msgs_on_ct,
                mc_collation_state_exist,
                mc_block_min_interval_ms,
                mc_block_max_interval_ms,
                decision=?res,
                "step decision"
            );

            return res;
        }

        // Otherwise: collate MC block using max candidate ct
        let next_mc_block_chain_time = candidates.into_iter().flatten().max().unwrap();

        // Mark all shards as "running" (attempts in progress)
        for (sid, st) in guard.states.iter_mut() {
            st.status = CollationStatus::AttemptsInProgress;

            // we may use not last imported chain time to collate master
            // so we prune all cached chain times for master above next chain time
            // so next anchors will be imported again
            if sid.is_masterchain() {
                st.last_imported_anchor_events
                    .retain(|s| s.ct <= next_mc_block_chain_time);
            }
        }

        // Update MC block time and reset force flags
        Self::renew_mc_block_latest_chain_time(guard, next_mc_block_chain_time);

        // drop "forced for all" and "forced by no pending messages" flags
        // if we decided to collate master
        guard.mc_collation_forced_for_all = false;
        guard.mc_forced_by_no_pending_msgs_on_ct = None;

        let res = NextCollationStep::CollateMaster(next_mc_block_chain_time);

        tracing::info!(
            target: tracing_targets::COLLATION_MANAGER,
            shard_id=?shard_id,
            should_collate_by_current_shard,
            should_collate_by_every_shard,
            any_has_shard_block_with_externals,
            hard_forced_for_all,
            mc_forced_by_no_pending_msgs_on_ct,
            mc_collation_state_exist,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            decision=?res,
            "step decision"
        );

        res
    }

    /// Run master block collation task. Will determine top shard blocks for this collation
    async fn run_mc_block_collation(
        &self,
        next_mc_block_id_short: BlockIdShort,
        next_mc_block_chain_time: u64,
        _trigger_block_id_opt: Option<BlockId>,
    ) -> Result<Option<CollatorJoinTask<CF::Collator>>> {
        let _histogram = HistogramGuard::begin("tycho_collator_run_mc_block_collation_time");

        // get masterchain collator if exists
        let mc_collator = self.take_collator_and_set_state_if(
            &ShardIdent::MASTERCHAIN,
            |ac| ac.state.is_ready(),
            |ac| ac.state = CollatorState::Active,
        )?;

        let Some(mc_collator) = mc_collator else {
            tracing::info!(target: tracing_targets::COLLATION_MANAGER,
                "Master collator is not ready: (block_id={} ct={})",
                next_mc_block_id_short,
                next_mc_block_chain_time,
            );
            return Ok(None);
        };

        // TODO: How to choose top shard blocks for master block collation when they are collated async and in parallel?
        //      We know the last anchor (An) used in shard (ShA) block that causes master block collation,
        //      so we search for block from other shard (ShB) that includes the same anchor (An).
        //      Or the first from previouses (An-x) that includes externals for that shard (ShB)
        //      if all next including required one ([An-x+1, An]) do not contain externals for shard (ShB).

        // get top shard blocks info that were collated before next master block
        let top_shard_blocks_info = self
            .blocks_cache
            .get_top_shard_blocks_info_for_mc_block(next_mc_block_id_short)?;

        let task =
            JoinTask::new(mc_collator.do_collate(top_shard_blocks_info, next_mc_block_chain_time));

        tracing::info!(target: tracing_targets::COLLATION_MANAGER,
            "Master block collation started: (block_id={} ct={})",
            next_mc_block_id_short,
            next_mc_block_chain_time,
        );

        Ok(Some(task))
    }

    /// Run next shard block collation attempt
    async fn run_try_collate(
        &self,
        shard_id: &ShardIdent,
    ) -> Result<Option<CollatorJoinTask<CF::Collator>>> {
        // get collator if exists
        let collator = self.take_collator_and_set_state_if(
            shard_id,
            |ac| ac.state.is_ready(),
            |ac| ac.state = CollatorState::Active,
        )?;

        let Some(collator) = collator else {
            tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                "Collator is not ready for shard {}",
                shard_id,
            );
            return Ok(None);
        };

        let task = JoinTask::new(collator.try_collate());

        tracing::info!(target: tracing_targets::COLLATION_MANAGER,
            "Started next attempt to collate block for shard {}",
            shard_id,
        );

        Ok(Some(task))
    }

    /// Process validated block
    /// 1. Process invalid block (currently, just panic)
    /// 2. Update block in cache with validation info
    /// 2. Execute processing for master or shard block
    #[tracing::instrument(skip_all, fields(block_id = %block_id.as_short_id()))]
    async fn handle_validated_master_block(
        &self,
        block_id: BlockId,
        status: ValidationStatus,
        skip_process_delayed_mc_state_update: bool,
    ) -> Result<Vec<CollatorJoinTask<CF::Collator>>> {
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
            return Ok(vec![]);
        }

        // process valid block
        let collator_tasks = self
            .commit_valid_master_block(&block_id, skip_process_delayed_mc_state_update)
            .await?;

        Ok(collator_tasks)
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
    async fn commit_valid_master_block(
        &self,
        mc_block_id: &BlockId,
        skip_process_delayed_mc_state_update: bool,
    ) -> Result<Vec<CollatorJoinTask<CF::Collator>>> {
        tracing::debug!(
            target: tracing_targets::COLLATION_MANAGER,
            "Start to commit validated and valid master block ({})...",
            mc_block_id.as_short_id(),
        );

        // gc blocks from cache when commit finished
        scopeguard::defer!(self.blocks_cache.gc_prev_blocks());

        // we can process delayed master state update now
        let (mut top_processed_to_anchor_to_notify, collator_tasks) = self
            .notify_to_mempool_and_process_delayed_mc_state_update(
                mc_block_id,
                skip_process_delayed_mc_state_update,
            )
            .await?
            .unzip();

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

        Ok(collator_tasks.unwrap_or_default())
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

    /// Collect top blocks seqno from all shards by master block id. Master block seqno included.
    /// Returns None when unable to read related top shard blocks info.
    async fn get_top_blocks_seqno(
        mc_block_id: &BlockId,
        blocks_cache: &BlocksCache,
        state_node_adapter: Arc<dyn StateNodeAdapter>,
    ) -> Result<Option<FastHashMap<ShardIdent, BlockSeqno>>> {
        let mut result = FastHashMap::default();

        result.insert(mc_block_id.shard, mc_block_id.seqno);

        // try get top shard blocks from cache first
        if let Some(top_shard_blocks) = blocks_cache.get_top_shard_blocks(mc_block_id.as_short_id())
        {
            result.extend(top_shard_blocks);
            return Ok(Some(result));
        }

        // then try read from state
        match state_node_adapter
            .load_state(mc_block_id.seqno, mc_block_id, Default::default())
            .await
        {
            Err(err) => match err.downcast_ref::<StateNotFound>() {
                Some(_) => {
                    tracing::warn!(target: tracing_targets::COLLATION_MANAGER,
                        %mc_block_id,
                        "master state not found in get_top_blocks_seqno",
                    );
                }
                _ => {
                    tracing::warn!(target: tracing_targets::COLLATION_MANAGER,
                        ?err,
                        "error loading master state in get_top_blocks_seqno",
                    );
                }
            },
            Ok(state) => {
                for item in state.shards()?.latest_blocks() {
                    let top_sb = item?;
                    result.insert(top_sb.shard, top_sb.seqno);
                }
                return Ok(Some(result));
            }
        }

        // finally try read from block data
        match state_node_adapter.load_block(mc_block_id).await {
            Err(err) => {
                tracing::warn!(target: tracing_targets::COLLATION_MANAGER,
                    %mc_block_id,
                    ?err,
                    "error loading master block in get_top_blocks_seqno",
                );
            }
            Ok(None) => {
                tracing::warn!(target: tracing_targets::COLLATION_MANAGER,
                    %mc_block_id,
                    "master block was not found in get_top_blocks_seqno",
                );
            }
            Ok(Some(mc_block_stuff)) => {
                let top_blocks = TopBlocks::from_mc_block(&mc_block_stuff)?;
                for top_sb in top_blocks.shard_heights().iter() {
                    result.insert(top_sb.shard, top_sb.seqno);
                }
                return Ok(Some(result));
            }
        }

        // unable to load related shard blocks info, return None
        Ok(None)
    }
}

struct HandleCollatorTaskResult<C: Collator> {
    validator_task: Option<ValidatorJoinTask>,
    collator_tasks: Vec<CollatorJoinTask<C>>,
    cancel_action: Option<ActionAfterCancel>,
}

impl<C: Collator> HandleCollatorTaskResult<C> {
    fn empty() -> Self {
        Self {
            validator_task: None,
            collator_tasks: vec![],
            cancel_action: None,
        }
    }
}

struct HandleBlockFromBcResult<C: Collator> {
    collator_tasks: Vec<CollatorJoinTask<C>>,
    cancel_action: Option<ActionAfterCancel>,
}

impl<C: Collator> HandleBlockFromBcResult<C> {
    fn empty() -> Self {
        Self {
            collator_tasks: vec![],
            cancel_action: None,
        }
    }
}

#[derive(Debug, Clone)]
enum ProcessMcStateUpdateMode {
    StartCollation { reset_collators: bool },
    RefreshSessionsOnly,
    SkipProcess,
}

#[derive(Default)]
struct RestoreQueueResult {
    last_mc_state: Option<ShardStateStuff>,
    prev_mc_state: Option<ShardStateStuff>,
    prev_mc_block_id: Option<BlockId>,
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
    pub mc_block_min_interval_ms: u64,
    pub mc_block_max_interval_ms: u64,
    pub collated_block_info: Option<CollatedBlockInfo>,
}

impl DetectNextCollationStepContext {
    fn new(
        last_imported_anchor_ct: u64,
        force_mc_block: ForceMasterCollation,
        mc_block_min_interval_ms: u64,
        mc_block_max_interval_ms: u64,
        collated_block_info: Option<CollatedBlockInfo>,
    ) -> Self {
        Self {
            last_imported_anchor_ct,
            force_mc_block,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            collated_block_info,
        }
    }
}
