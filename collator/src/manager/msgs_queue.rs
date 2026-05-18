use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use ahash::HashMapExt;
use anyhow::{Context, Result, bail};
use tycho_block_util::queue::{QueueDiffStuff, QueueKey, QueuePartitionIdx};
use tycho_block_util::state::ShardStateStuff;
use tycho_core::storage::LoadStateHint;
use tycho_types::models::{BlockId, BlockIdShort, ShardIdent};
use tycho_util::metrics::HistogramGuard;
use tycho_util::{FastHashMap, FastHashSet};

use super::CollationManager;
use super::blocks_cache::{BlocksCache, CachedMcBlockSubgraphView};
use super::types::{BlockCacheEntry, BlockCacheEntryData, BlockCacheKey, McBlockSubgraphExtract};
use crate::collator::CollatorFactory;
use crate::internal_queue::types::diff::{DiffZone, QueueDiffWithMessages};
use crate::internal_queue::types::message::EnqueuedMessage;
use crate::internal_queue::types::stats::DiffStatistics;
use crate::queue_adapter::MessageQueueAdapter;
use crate::state_node::StateNodeAdapter;
use crate::tracing_targets;
use crate::types::processed_upto::{
    BlockSeqno, ProcessedUptoInfoStuff, find_min_processed_to_by_shards,
};
use crate::types::{ProcessedTo, ProcessedToByPartitions, TopBlockId, TopBlockIdUpdated};
use crate::validator::Validator;

impl<CF, V> CollationManager<CF, V>
where
    CF: CollatorFactory,
    V: Validator,
{
    pub(super) fn clear_uncommitted_queue_state(&self) -> Result<()> {
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

    pub(super) async fn get_all_shards_processed_to_by_partitions_for_mc_block(
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

    pub(super) async fn get_queue_restore_processed_to_by_shards(
        &self,
        last_applied_mc_block_key: &BlockIdShort,
    ) -> Result<QueueRestoreProcessedTo> {
        // get internals processed_to from master and all shards for last applied master block
        let all_shards_processed_to_by_partitions =
            Self::get_all_shards_processed_to_by_partitions_for_mc_block(
                last_applied_mc_block_key,
                &self.blocks_cache,
                self.state_node_adapter.clone(),
            )
            .await?;

        // find internals min processed_to
        let min_processed_to_by_shards =
            find_min_processed_to_by_shards(&all_shards_processed_to_by_partitions);

        Ok(QueueRestoreProcessedTo {
            all_shards_processed_to_by_partitions,
            min_processed_to_by_shards,
        })
    }

    // Returns top master block id upto which all queue diffs applied
    pub(super) fn get_queue_diffs_applied_to_mc_block_id(
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

    pub(super) async fn get_queue_diffs_applied_to_top_blocks(
        &self,
        last_collated_mc_block_id: Option<BlockId>,
    ) -> Result<Option<FastHashMap<ShardIdent, BlockSeqno>>> {
        if !self.config.fast_sync {
            return Ok(None);
        }

        let Some(applied_to_mc_block_id) =
            self.get_queue_diffs_applied_to_mc_block_id(last_collated_mc_block_id)?
        else {
            // BACKWARD COMPATIBILITY: `last_committed_mc_block_id` will not exist in queue storage
            // in previous version. We will receive None and will use all required diffs to restore the queue
            return Ok(None);
        };

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
        .await
    }

    /// Returns false when any of top block diffs is required or unable to check
    pub(super) async fn check_top_blocks_diffs_not_required(
        mq_adapter: &Arc<dyn MessageQueueAdapter<EnqueuedMessage>>,
        mc_block_subgraph_view: &CachedMcBlockSubgraphView,
        min_processed_to_by_shards: &ProcessedTo,
        queue_diffs_applied_to_top_blocks: &FastHashMap<ShardIdent, BlockSeqno>,
        init_mc_block_id: Option<BlockId>,
        zerostate_mc_seqno: BlockSeqno,
    ) -> Result<bool> {
        // if top shard block is missing for any reason
        // we unable to check if his diff required,
        // so will not cleanup cache in this case
        if let Some(missing_top_shard_block) = mc_block_subgraph_view.missing_top_shard_block {
            tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                mc_block_id = %mc_block_subgraph_view.block_id.as_short_id(),
                %missing_top_shard_block,
                "skip cache cleanup because top shard block is missing in cache",
            );
            return Ok(false);
        }

        // check master block diff
        if !Self::check_queue_diff_required(
            mq_adapter,
            &mc_block_subgraph_view.queue_diff,
            mc_block_subgraph_view.ref_by_mc_seqno,
            min_processed_to_by_shards.get(&mc_block_subgraph_view.block_id.shard),
            queue_diffs_applied_to_top_blocks,
            init_mc_block_id,
            zerostate_mc_seqno,
        )?
        .is_not_required()
        {
            // return earlier and do not check shard blocks
            // if master block diff is required or unable to check
            return Ok(false);
        }

        // check if shard blocks diffs are required
        for top_shard_block in &mc_block_subgraph_view.top_shard_blocks {
            if !Self::check_queue_diff_required(
                mq_adapter,
                &top_shard_block.queue_diff,
                top_shard_block.ref_by_mc_seqno,
                min_processed_to_by_shards.get(&top_shard_block.block_id.shard),
                queue_diffs_applied_to_top_blocks,
                init_mc_block_id,
                zerostate_mc_seqno,
            )?
            .is_not_required()
            {
                // return earlier if any of shard block diff is required or unable to check
                return Ok(false);
            }
        }

        Ok(true)
    }

    fn check_queue_diff_required(
        mq_adapter: &Arc<dyn MessageQueueAdapter<EnqueuedMessage>>,
        queue_diff: &QueueDiffStuff,
        ref_by_mc_seqno: BlockSeqno,
        min_processed_to: Option<&QueueKey>,
        queue_diffs_applied_to_top_blocks: &FastHashMap<ShardIdent, BlockSeqno>,
        init_mc_block_id: Option<BlockId>,
        zerostate_mc_seqno: BlockSeqno,
    ) -> Result<QueueDiffRequired> {
        let block_id = queue_diff.block_id();

        if ref_by_mc_seqno <= zerostate_mc_seqno {
            return Ok(QueueDiffRequired::NotRequired);
        }

        if let Some(init_mc_block_id) = init_mc_block_id {
            assert!(
                ref_by_mc_seqno >= init_mc_block_id.seqno,
                "cached block {} ref_by_mc_seqno {} is below init master block {}",
                block_id,
                ref_by_mc_seqno,
                init_mc_block_id,
            );
            if ref_by_mc_seqno == init_mc_block_id.seqno {
                return Ok(QueueDiffRequired::NotRequired);
            }
        }

        if let Some(queue_diff_applied_to_top_block_seqno) =
            queue_diffs_applied_to_top_blocks.get(&block_id.shard)
            && block_id.seqno <= *queue_diff_applied_to_top_block_seqno
        {
            tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                queue_diff_block_id = %block_id.as_short_id(),
                top_applied_seqno = queue_diff_applied_to_top_block_seqno,
                "queue diff is not required because it is below top applied",
            );
            return Ok(QueueDiffRequired::NotRequired);
        }

        let Some(min_processed_to) = min_processed_to else {
            tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                block_id = %block_id.as_short_id(),
                shard = %block_id.shard,
                "unable to check if diff required for queue restore \
                because processed_to data for the shard is incomplete",
            );
            return Ok(QueueDiffRequired::Unknown);
        };

        if queue_diff.as_ref().max_message <= *min_processed_to {
            tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                queue_diff_block_id = %block_id.as_short_id(),
                "queue diff is not required: max_message {} <= min_processed_to {}",
                queue_diff.as_ref().max_message,
                min_processed_to,
            );
            return Ok(QueueDiffRequired::NotRequired);
        }

        if mq_adapter.is_diff_exists(&block_id.as_short_id())? {
            tracing::trace!(target: tracing_targets::COLLATION_MANAGER,
                queue_diff_block_id = %block_id.as_short_id(),
                "queue diff will be skipped because it is already applied",
            );
            return Ok(QueueDiffRequired::AlreadyApplied);
        }

        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            block_id = %block_id.as_short_id(),
            ref_by_mc_seqno,
            max_message = %queue_diff.as_ref().max_message,
            min_processed_to = %min_processed_to,
            "block diff is required to restore queue",
        );

        Ok(QueueDiffRequired::Required)
    }

    #[tracing::instrument(skip_all, fields(from_mc_block_seqno))]
    pub(super) async fn restore_queue(
        blocks_cache: &BlocksCache,
        state_node_adapter: Arc<dyn StateNodeAdapter>,
        mq_adapter: Arc<dyn MessageQueueAdapter<EnqueuedMessage>>,
        from_mc_block_seqno: u32,
        min_processed_to_by_shards: BTreeMap<ShardIdent, QueueKey>,
        before_tail_block_ids: BTreeMap<ShardIdent, (Option<BlockId>, Vec<BlockId>)>,
        queue_diffs_applied_to_top_blocks: FastHashMap<ShardIdent, u32>,
    ) -> Result<RestoreQueueResult> {
        let mut res = RestoreQueueResult::default();

        // NOTE: Queue restore is split into two adjacent ranges:
        // - First, find the first applied MC subgraph stored in cache, e.g. MB2 with top shard SB3.
        // - Then collect block ids immediately before that subgraph, e.g. MB1 and SB2.
        // - Walk backwards from those before-tail blocks through storage and apply required historical diffs.
        // - After that, pop applied MC subgraphs from cache starting from MB2 and apply required cached diffs forward.
        // These ranges should not overlap: storage diffs restore the queue before the cached applied range,
        // and cached diffs advance it through the applied range.

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

                // skip already applied diff
                if mq_adapter.is_diff_exists(&prev_block_id.as_short_id())? {
                    tracing::trace!(target: tracing_targets::COLLATION_MANAGER,
                        queue_diff_block_id = %prev_block_id.as_short_id(),
                        "previous queue diff apply skipped because it is already applied",
                    );
                    first_required_diffs.insert(prev_block_id.shard, BlockId::default());
                    continue;
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
                    let min_processed_to =
                        min_processed_to_by_shards.get(&block_entry.block_id.shard);

                    if let Some(applied_diff_block_id) =
                        Self::apply_block_queue_diff_from_entry_stuff(
                            mq_adapter.clone(),
                            block_entry,
                            min_processed_to,
                            &queue_diffs_applied_to_top_blocks,
                            init_mc_block_id,
                            state_node_adapter.zerostate_id().seqno,
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

    /// Returns `BlockId` if diff was applied.
    /// * `first_required_diffs` - contains ids of known first required diffs for queue for each shard
    fn apply_block_queue_diff_from_entry_stuff(
        mq_adapter: Arc<dyn MessageQueueAdapter<EnqueuedMessage>>,
        block_entry: &BlockCacheEntry,
        min_processed_to: Option<&QueueKey>,
        queue_diffs_applied_to_top_blocks: &FastHashMap<ShardIdent, BlockSeqno>,
        init_mc_block_id: Option<BlockId>,
        zerostate_mc_seqno: BlockSeqno,
        first_required_diffs: &mut FastHashMap<ShardIdent, BlockId>,
    ) -> Result<Option<BlockId>> {
        let block_id = block_entry.block_id;

        let queue_diff = match &block_entry.data {
            BlockCacheEntryData::Collated {
                candidate_stuff, ..
            } => &candidate_stuff.candidate.queue_diff_aug.data,
            BlockCacheEntryData::Received { queue_diff, .. } => queue_diff,
        };

        match Self::check_queue_diff_required(
            &mq_adapter,
            queue_diff,
            block_entry.ref_by_mc_seqno,
            min_processed_to,
            queue_diffs_applied_to_top_blocks,
            init_mc_block_id,
            zerostate_mc_seqno,
        )? {
            QueueDiffRequired::NotRequired => return Ok(None),
            QueueDiffRequired::AlreadyApplied => {
                // if diff for block from bc already applied
                // then we should check sequense for each next diff
                first_required_diffs.insert(block_id.shard, BlockId::default());
                return Ok(None);
            }
            QueueDiffRequired::Required | QueueDiffRequired::Unknown => {}
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

    #[tracing::instrument(skip_all, fields(block_id = %block_id))]
    pub(super) fn commit_block_queue_diff(
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
}

#[derive(Default)]
pub(crate) struct RestoreQueueResult {
    pub last_mc_state: Option<ShardStateStuff>,
    pub prev_mc_state: Option<ShardStateStuff>,
    pub prev_mc_block_id: Option<BlockId>,
    pub synced_to_blocks_keys: Vec<BlockCacheKey>,
    pub applied_diffs_ids: FastHashSet<BlockId>,
}

pub(super) struct QueueRestoreProcessedTo {
    pub all_shards_processed_to_by_partitions:
        FastHashMap<ShardIdent, (bool, ProcessedToByPartitions)>,
    pub min_processed_to_by_shards: ProcessedTo,
}

enum QueueDiffRequired {
    Required,
    NotRequired,
    // also means not required, but restore must update `first_required_diffs` for already applied diffs
    AlreadyApplied,
    Unknown,
}

impl QueueDiffRequired {
    fn is_not_required(&self) -> bool {
        matches!(self, Self::NotRequired | Self::AlreadyApplied)
    }
}
