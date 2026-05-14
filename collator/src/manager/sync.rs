use std::sync::Arc;

use anyhow::{Result, bail};
use tokio_util::sync::CancellationToken;
use tycho_block_util::block::TopBlocks;
use tycho_block_util::state::ShardStateStuff;
use tycho_core::storage::StateNotFound;
use tycho_types::models::{BlockId, BlockIdShort, ShardIdent};
use tycho_util::FastHashMap;
use tycho_util::futures::AwaitBlocking;
use tycho_util::metrics::HistogramGuard;

use super::CollationManager;
use super::blocks_cache::BlocksCache;
use super::state_update_handler::ProcessMcStateUpdateMode;
use super::types::{
    ActiveSync, BlockCacheStoreResult, CollationStatus, CollationSyncState, CollatorJoinTask,
};
use crate::collator::CollatorFactory;
use crate::state_node::StateNodeAdapter;
use crate::tracing_targets;
use crate::types::processed_upto::{BlockSeqno, find_min_processed_to_by_shards};
use crate::types::{DisplayAsShortId, DisplayBlockIdsIntoIter, McData, ProcessedToByPartitions};
use crate::validator::Validator;

impl<CF, V> CollationManager<CF, V>
where
    CF: CollatorFactory,
    V: Validator,
{
    // HELPERS

    pub(super) fn update_last_received_mc_block_seqno(&self, received_block_id: &BlockId) {
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

    pub(super) fn has_newer_received_mc_block(&self, applied_range: Option<(u32, u32)>) -> bool {
        if let Some((_, applied_range_end)) = applied_range {
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

                return true;
            }
        }
        false
    }

    fn update_last_synced_to_mc_block_id(&self, mc_block_id: BlockId) {
        let mut guard = self.collation_sync_state.lock();
        guard.last_synced_to_mc_block_id = Some(mc_block_id);
    }

    pub(super) fn get_last_synced_to_mc_block_id(&self) -> Option<BlockId> {
        let guard = self.collation_sync_state.lock();
        guard.last_synced_to_mc_block_id
    }

    /// Collect top blocks seqno from all shards by master block id. Master block seqno included.
    /// Returns None when unable to read related top shard blocks info.
    pub(super) async fn get_top_blocks_seqno(
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

    // SYNC LOCK

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

    /// Returns `true` if active sync was cancelled
    pub(super) fn finish_active_sync_to_applied(&self, received_block_id: &BlockId) -> bool {
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

    // HANDLERS

    pub(super) fn check_should_sync_to_last_applied_mc_block(
        &self,
        store_res: &BlockCacheStoreResult,
        top_mc_block_id_for_next_collation: Option<BlockId>,
        required_min_mc_block_delta: BlockSeqno,
        is_key_block: Option<bool>,
    ) -> bool {
        // do not sync to last applied mc block if newer already received
        if self.has_newer_received_mc_block(store_res.applied_mc_queue_range) {
            return false;
        }

        // should sync if collated block mismatched
        if store_res.block_mismatch {
            return true;
        }

        let has_active_collations = self.has_active_collations();
        let last_synced_to_mc_block_id = self
            .get_last_synced_to_mc_block_id()
            .map(|id| id.as_short_id().to_string());
        let last_collated_mc_block_id = store_res
            .last_collated_mc_block_id
            .map(|id| id.as_short_id().to_string());
        let last_processed_mc_block_id = self
            .last_processed_mc_block_id
            .lock()
            .map(|id| id.as_short_id().to_string());

        if let Some(top_mc_block_id_for_next_collation) = top_mc_block_id_for_next_collation {
            // we can sync only when we have any applied block ahead
            if let Some((_, applied_range_end)) = store_res.applied_mc_queue_range {
                // check if should sync according to master block delta
                let applied_range_end_delta =
                    applied_range_end.saturating_sub(top_mc_block_id_for_next_collation.seqno);

                if applied_range_end_delta < required_min_mc_block_delta {
                    tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                        last_synced_to_mc_block_id = ?last_synced_to_mc_block_id,
                        last_collated_mc_block_id = ?last_collated_mc_block_id,
                        last_processed_mc_block_id = ?last_processed_mc_block_id,
                        has_active_collations,
                        is_key_block = ?is_key_block,
                        "check_should_sync: should wait for next collated own mc block: \
                        last applied ({}) ahead of top for collation ({}) on {} < {}",
                        applied_range_end, top_mc_block_id_for_next_collation.seqno,
                        applied_range_end_delta, required_min_mc_block_delta,
                    );
                    false
                } else {
                    tracing::info!(target: tracing_targets::COLLATION_MANAGER,
                        last_synced_to_mc_block_id = ?last_synced_to_mc_block_id,
                        last_collated_mc_block_id = ?last_collated_mc_block_id,
                        last_processed_mc_block_id = ?last_processed_mc_block_id,
                        has_active_collations,
                        is_key_block = ?is_key_block,
                        "check_should_sync: should sync to last applied mc block from bc: \
                        last applied ({}) ahead of top for collation ({}) on {} >= {}",
                        applied_range_end, top_mc_block_id_for_next_collation.seqno,
                        applied_range_end_delta, required_min_mc_block_delta,
                    );
                    true
                }
            } else {
                // should collate next own mc block because no applied ahead
                tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                    last_synced_to_mc_block_id = ?last_synced_to_mc_block_id,
                    last_collated_mc_block_id = ?last_collated_mc_block_id,
                    last_processed_mc_block_id = ?last_processed_mc_block_id,
                    has_active_collations,
                    is_key_block = ?is_key_block,
                    "check_should_sync: should collate next own mc block after because nothing applied ahead",
                );
                false
            }
        } else {
            tracing::info!(target: tracing_targets::COLLATION_MANAGER,
                last_synced_to_mc_block_id = ?last_synced_to_mc_block_id,
                last_collated_mc_block_id = ?last_collated_mc_block_id,
                last_processed_mc_block_id = ?last_processed_mc_block_id,
                has_active_collations,
                is_key_block = ?is_key_block,
                "check_should_sync: should sync to last applied mc block when no last collated or prev sync to",
            );
            true
        }
    }

    #[tracing::instrument(name = "sync_to_applied_mc_block", skip_all, fields(trigger_block_id = %trigger_block_id_short, applied_range = ?applied_range))]
    pub(super) async fn sync_to_applied_mc_block_if_exist(
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
}
