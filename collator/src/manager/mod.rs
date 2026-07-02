use std::sync::Arc;

use anyhow::{Context, Result, bail};
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use parking_lot::{Mutex, RwLock};
use tycho_block_util::state::ShardStateStuff;
use tycho_core::global_config::MempoolGlobalConfig;
use tycho_crypto::ed25519::KeyPair;
use tycho_slasher_traits::ValidatorEventsListener;
use tycho_types::models::{BlockId, BlockIdShort, CollationConfig, ProcessedUptoInfo, ShardIdent};
use tycho_util::futures::JoinTask;
use tycho_util::metrics::HistogramGuard;
use tycho_util::{FastDashMap, FastHashMap};

use self::blocks_cache::BlocksCache;
use self::cancel_validation_runner::CancelValidationRunnerState;
use self::collation_cancel::{ActionAfterCancel, CollationCancelHandle, update_action_after};
use self::state_event_listener::{ChannelStateEventListener, StateEvent};
use self::state_update_handler::ProcessMcStateUpdateMode;
use self::sync::ShouldSyncToAppliedMcBlock;
use self::types::{
    ActiveCollator, CandidateStatus, CollatedBlockInfo, CollationState, CollationStatus,
    CollationSyncState, CollatorJoinTask, CollatorState, HandledBlockFromBcCtx,
    ImportedAnchorEvent, NextCollationStep, ValidatorJoinTask,
};
use crate::collator::{
    CollationAbortReason, Collator, CollatorFactory, CollatorResult, DebugCollatorResult,
    ForceMasterCollation,
};
use crate::internal_queue::types::message::EnqueuedMessage;
use crate::mempool::{MempoolAdapter, MempoolAdapterFactory, MempoolAnchorId};
use crate::queue_adapter::MessageQueueAdapter;
use crate::state_node::{StateNodeAdapter, StateNodeAdapterFactory};
use crate::tracing_targets;
use crate::types::processed_upto::{
    BlockSeqno, ProcessedUptoInfoExtension, ProcessedUptoInfoStuff,
};
use crate::types::{
    BlockCollationResult, BlockIdExt, CollationSessionInfo, CollatorConfig, DebugDisplayOpt,
    DisplayAsShortId, McData, ShardDescriptionShortExt, ShardHashesExt, ShardHashesReadExt,
    TopBlockId, TopBlockIdUpdated,
};
use crate::utils::block::detect_top_processed_to_anchor;
use crate::utils::vset_cache::ValidatorSetCache;
use crate::validator::{ValidationStatus, Validator};

mod active_collators;
mod blocks_cache;
mod cancel_validation_runner;
mod collation_cancel;
mod commit;
mod msgs_queue;
mod state_event_listener;
mod state_update_handler;
mod sync;
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
    stats_recorder: Arc<dyn ValidatorEventsListener>,

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
        stats_recorder: Arc<dyn ValidatorEventsListener>,
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
            stats_recorder,

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
                                StateEvent::OwnBlockApplied { state, processed_upto, queue_partitions } => {
                                    // spawn validation cancellation
                                    mgr.schedule_cancel_validation_sessions_until_block(state.clone());

                                    // commit messages queue
                                    if state.block_id().is_masterchain() {
                                        let _histogram =
                                            HistogramGuard::begin("tycho_collator_send_blocks_to_sync_commit_diffs_time");
                                        Self::commit_block_queue_diff(
                                            mgr.state_node_adapter.clone(),
                                            mgr.mq_adapter.clone(),
                                            state.block_id(),
                                            &state.shards()?.top_shard_blocks_info()?,
                                            &queue_partitions.expect("should be Some for master block"),
                                        ).await?;
                                    }

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

        let mc_top_shards = state
            .shards()?
            .as_vec()?
            .into_iter()
            .map(|(_, descr)| descr);

        let top_processed_to_anchor =
            detect_top_processed_to_anchor(mc_top_shards, mc_processed_to_anchor_id);

        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            top_processed_to_anchor,
            mc_processed_to_anchor_id,
            "detected minimal top_processed_to_anchor, will notify mempool",

        );

        self.notify_top_processed_to_anchor_to_mempool(
            state.block_id().seqno,
            top_processed_to_anchor,
        )
        .await
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

    async fn handle_collator_task(
        self: &Arc<Self>,
        collator_task_res: Result<(Box<CF::Collator>, CollatorResult)>,
    ) -> Result<HandleTaskResult<CF::Collator>> {
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
                Ok(HandleTaskResult::with_tasks(None, collator_tasks))
            }
            CollatorResult::Aborted(abort_ctx) => {
                let collator_tasks = self
                    .handle_collation_aborted(
                        abort_ctx.prev_mc_block_id,
                        abort_ctx.next_block_id_short,
                        abort_ctx.reason,
                    )
                    .await?;
                Ok(HandleTaskResult::with_tasks(None, collator_tasks))
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

    #[tracing::instrument(skip_all, fields(next_block_id = %next_block_id_short))]
    async fn handle_collation_aborted(
        self: &Arc<Self>,
        _prev_mc_block_id: BlockId,
        next_block_id_short: BlockIdShort,
        reason: CollationAbortReason,
    ) -> Result<Vec<CollatorJoinTask<CF::Collator>>> {
        tracing::info!(target: tracing_targets::COLLATION_MANAGER,
            ?reason,
            "start handle collation aborted",
        );

        // NOTE: when collation aborted it guarantees
        //      that uncommitted queue diff was not saved
        match reason {
            CollationAbortReason::AnchorNotFound(_)
            | CollationAbortReason::NextAnchorNotFound(_)
            | CollationAbortReason::DiffNotFoundInQueue(_) => {
                // mark collator as cancelled
                self.set_collator_state(&next_block_id_short.shard, |ac| {
                    ac.state = CollatorState::Cancelled;
                });

                // run sync if there are no active collations
                if !self.has_active_collations() {
                    tracing::info!(target: tracing_targets::COLLATION_MANAGER,
                        "no active collations in shards and masterchain, \
                        will run sync to last applied mc block",
                    );

                    // get info about applied mc blocks in cache
                    let (last_collated_mc_block_id, applied_range) = self
                        .blocks_cache
                        .get_last_collated_block_and_applied_mc_queue_range();

                    // run sync only if has applied mc blocks and has not newer received
                    // master block was already received
                    if self.has_newer_received_mc_block(applied_range) {
                        // cleanup front applied blocks from cache if their diffs not required to restore queue
                        let cleanup_res = self
                            .gc_applied_blocks_with_not_required_diffs(
                                last_collated_mc_block_id,
                                applied_range,
                                None,
                            )
                            .await?;
                        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                            ?cleanup_res,
                            last_collated_mc_block_id = ?last_collated_mc_block_id,
                            applied_range = ?applied_range,
                            "cache cleanup after skipped sync finished",
                        );
                    } else {
                        return self
                            .sync_to_applied_mc_block_if_exist(
                                next_block_id_short,
                                last_collated_mc_block_id,
                                applied_range,
                                ProcessMcStateUpdateMode::StartCollation {
                                    reset_collators: true,
                                },
                            )
                            .await;
                    }
                }
            }
        }
        Ok(vec![])
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
    ) -> Result<HandleTaskResult<CF::Collator>> {
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

                return Ok(HandleTaskResult::empty());
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
        let top_mc_block_id_for_next_collation =
            self.get_top_mc_block_id_for_next_collation(store_res.last_collated_mc_block_id);
        let should_sync_to_last_applied_mc_block = self.check_should_sync_to_last_applied_mc_block(
            &store_res,
            top_mc_block_id_for_next_collation,
            self.config.min_mc_block_delta_from_bc_to_sync,
            collation_result
                .mc_data
                .as_ref()
                .map(|mc_data| mc_data.prev_key_block_seqno == mc_data.block_id.seqno),
        );

        // cleanup front applied blocks from cache if their diffs not required to restore queue
        if matches!(
            should_sync_to_last_applied_mc_block,
            ShouldSyncToAppliedMcBlock::NoBecauseNewerReceived
        ) {
            let cleanup_res = self
                .gc_applied_blocks_with_not_required_diffs(
                    store_res.last_collated_mc_block_id,
                    store_res.applied_mc_queue_range,
                    block_id.is_masterchain().then_some(block_id.seqno),
                )
                .await?;
            tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                ?cleanup_res,
                last_collated_mc_block_id = ?store_res.last_collated_mc_block_id,
                applied_range = ?store_res.applied_mc_queue_range,
                "cache cleanup after skipped sync finished",
            );
        }

        // run sync or process just collated block
        if matches!(
            should_sync_to_last_applied_mc_block,
            ShouldSyncToAppliedMcBlock::Yes
        ) {
            // before sync will cancel any active collations
            Ok(HandleTaskResult::with_cancel(
                ActionAfterCancel::SyncToAppliedMcBlock {
                    trigger_block_id_short: block_id.as_short_id(),
                    last_collated_mc_block_id: store_res.last_collated_mc_block_id,
                    applied_range: store_res.applied_mc_queue_range,
                    process_state_update_mode: ProcessMcStateUpdateMode::StartCollation {
                        reset_collators: true,
                    },
                },
            ))
        } else if store_res.block_mismatch {
            // only cancel all active collations
            // when block mismatched and newer already received
            Ok(HandleTaskResult::with_cancel(ActionAfterCancel::Noop))
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
            let mut collator_tasks = vec![];
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

            Ok(HandleTaskResult::with_tasks(validator_task, collator_tasks))
        } else {
            // when candidate is shard

            // run master block collation if required or resume collation attempts in shard
            tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                "will run next collation step",
            );

            let collator_tasks = self
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

            Ok(HandleTaskResult::with_tasks(None, collator_tasks))
        }
    }

    fn handle_block_mismatch(&self, block_id: &BlockId) -> Result<()> {
        let labels = [("workchain", block_id.shard.workchain().to_string())];
        metrics::counter!("tycho_collator_block_mismatch_count", &labels).increment(1);

        // should drop uncommitted queue state on block mismatch
        // because created messages are incorrect
        self.clear_uncommitted_queue_state()?;

        Ok(())
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
    ) -> Result<HandleTaskResult<CF::Collator>> {
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
            update_action_after(&mut cancel_action, res.cancel_action);
        }

        Ok(HandleTaskResult {
            validator_task: None,
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
    ) -> Result<HandleTaskResult<CF::Collator>> {
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
            return Ok(HandleTaskResult::empty());
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
                return Ok(HandleTaskResult::with_cancel(ActionAfterCancel::Noop));
            } else {
                // just do nothing otherwise
                return Ok(HandleTaskResult::empty());
            }
        }

        // check if received is a key block
        let is_key_block = ctx.state.state_extra()?.after_key_block;

        // stop any running validations up to this block
        self.schedule_cancel_validation_sessions_until_block(ctx.state.clone());

        // check if should sync to last applied mc block right now
        let top_mc_block_id_for_next_collation =
            self.get_top_mc_block_id_for_next_collation(store_res.last_collated_mc_block_id);
        // we should sync to every key block if node is not in current vset
        let required_min_mc_block_delta =
            if is_key_block && !self.active_collators.contains_key(&ShardIdent::MASTERCHAIN) {
                1
            } else {
                self.config.min_mc_block_delta_from_bc_to_sync
            };

        let should_sync_to_last_applied_mc_block = if is_last_mc_block_in_batch || is_key_block {
            self.check_should_sync_to_last_applied_mc_block(
                &store_res,
                top_mc_block_id_for_next_collation,
                required_min_mc_block_delta,
                Some(is_key_block),
            )
        } else {
            ShouldSyncToAppliedMcBlock::No
        };

        // cleanup front applied blocks from cache if their diffs not required to restore queue
        if matches!(
            should_sync_to_last_applied_mc_block,
            ShouldSyncToAppliedMcBlock::NoBecauseNewerReceived
        ) {
            let cleanup_res = self
                .gc_applied_blocks_with_not_required_diffs(
                    store_res.last_collated_mc_block_id,
                    store_res.applied_mc_queue_range,
                    Some(block_id.seqno),
                )
                .await?;
            tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                ?cleanup_res,
                last_collated_mc_block_id = ?store_res.last_collated_mc_block_id,
                applied_range = ?store_res.applied_mc_queue_range,
                "cache cleanup after skipped sync finished",
            );
        }

        // run sync or commit block
        if matches!(
            should_sync_to_last_applied_mc_block,
            ShouldSyncToAppliedMcBlock::Yes
        ) {
            // should only refresh collation sessions when sync on key block
            let process_state_update_mode = if is_last_mc_block_in_batch {
                ProcessMcStateUpdateMode::StartCollation {
                    reset_collators: true,
                }
            } else {
                ProcessMcStateUpdateMode::RefreshSessionsOnly
            };
            // will cancel active collations and then run sync
            Ok(HandleTaskResult::with_cancel(
                ActionAfterCancel::SyncToAppliedMcBlock {
                    trigger_block_id_short: block_id.as_short_id(),
                    last_collated_mc_block_id: store_res.last_collated_mc_block_id,
                    applied_range: store_res.applied_mc_queue_range,
                    process_state_update_mode,
                },
            ))
        } else if store_res.block_mismatch {
            // only cancel all active collations
            // when block mismatched and should not sync by any reason
            Ok(HandleTaskResult::with_cancel(ActionAfterCancel::Noop))
        } else {
            let mut collator_tasks = vec![];

            // try to commit block if it was collated first
            if store_res.received_and_collated {
                // NOTE: here commit will not cause on_block_accepted event
                //      because block already exist in bc state

                collator_tasks = self.commit_valid_master_block(&block_id, false).await?;
            }

            Ok(HandleTaskResult::with_tasks(None, collator_tasks))
        }
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
        let updated_status = self
            .blocks_cache
            .store_master_block_validation_result(&block_id, status);
        let collator_tasks = match updated_status {
            Some(CandidateStatus::Validated) => {
                self.commit_valid_master_block(&block_id, skip_process_delayed_mc_state_update)
                    .await?
            }
            Some(CandidateStatus::ValidationSkipped) => {
                tracing::debug!(
                    target: tracing_targets::COLLATION_MANAGER,
                    "Skip committing master block because validation was skipped",
                );
                vec![]
            }
            Some(status) => {
                tracing::debug!(
                    target: tracing_targets::COLLATION_MANAGER,
                    ?status,
                    "Skip committing master block for non-validated status",
                );
                vec![]
            }
            None => return Ok(vec![]),
        };

        Ok(collator_tasks)
    }
}

struct HandleTaskResult<C: Collator> {
    validator_task: Option<ValidatorJoinTask>,
    collator_tasks: Vec<CollatorJoinTask<C>>,
    cancel_action: Option<ActionAfterCancel>,
}

impl<C: Collator> HandleTaskResult<C> {
    fn empty() -> Self {
        Self {
            validator_task: None,
            collator_tasks: vec![],
            cancel_action: None,
        }
    }

    fn with_tasks(
        validator_task: Option<ValidatorJoinTask>,
        collator_tasks: Vec<CollatorJoinTask<C>>,
    ) -> Self {
        Self {
            validator_task,
            collator_tasks,
            cancel_action: None,
        }
    }

    fn with_cancel(cancel_action: ActionAfterCancel) -> Self {
        Self {
            validator_task: None,
            collator_tasks: vec![],
            cancel_action: Some(cancel_action),
        }
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
