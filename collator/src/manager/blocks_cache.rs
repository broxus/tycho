use std::collections::{btree_map, BTreeMap, VecDeque};
use std::sync::{Arc, OnceLock};

use anyhow::{bail, Result};
use everscale_types::cell::Lazy;
use everscale_types::models::{
    BlockId, BlockIdShort, ConsensusInfo, OutMsgDescr, ShardFeeCreated, ShardIdent, ValueFlow,
};
use parking_lot::Mutex;
use tycho_block_util::queue::QueueDiffStuff;
use tycho_block_util::state::ShardStateStuff;
use tycho_util::{FastDashMap, FastHashMap};

use super::types::{
    BlockCacheEntry, BlockCacheKey, BlockCacheStoreResult, BlockSeqno, CandidateStatus,
    McBlockSubgraph, McBlockSubgraphExtract,
};
use crate::manager::types::{AdditionalShardBlockCacheInfo, BlockCacheEntryData};
use crate::state_node::StateNodeAdapter;
use crate::tracing_targets;
use crate::types::{
    BlockCandidate, DisplayIntoIter, DisplayIter, McData, ProcessedTo, TopBlockDescription,
};
use crate::validator::ValidationStatus;

#[cfg(test)]
#[path = "tests/blocks_cache_tests.rs"]
pub(super) mod tests;

pub struct BlocksCache {
    masters: Mutex<MasterBlocksCache>,
    shards: FastDashMap<ShardIdent, ShardBlocksCache>,
}

impl BlocksCache {
    pub fn new() -> Self {
        metrics::gauge!("tycho_blocks_count_in_collation_manager_cache").set(0);

        Self {
            masters: Default::default(),
            shards: Default::default(),
        }
    }

    /// Find top shard blocks in cache for the next master block collation
    pub fn get_top_shard_blocks_info_for_mc_block(
        &self,
        next_mc_block_id_short: BlockIdShort,
    ) -> Result<Vec<TopBlockDescription>> {
        let mut result = vec![];
        for mut shard_cache in self.shards.iter_mut() {
            for (_, entry) in shard_cache.blocks.iter().rev() {
                if entry.ref_by_mc_seqno == next_mc_block_id_short.seqno {
                    let processed_to = entry
                        .int_processed_to()
                        .iter()
                        .map(|(shard, queue_key)| (*shard, *queue_key))
                        .collect();

                    if let Some(additional_info) =
                        entry.data.get_additional_shard_block_cache_info()?
                    {
                        result.push(TopBlockDescription {
                            block_id: entry.block_id,
                            block_info: additional_info.block_info,
                            processed_to_anchor_id: additional_info.processed_to_anchor_id,
                            value_flow: std::mem::take(&mut shard_cache.data.value_flow),
                            proof_funds: std::mem::take(&mut shard_cache.data.proof_funds),
                            #[cfg(feature = "block-creator-stats")]
                            creators: std::mem::take(&mut shard_cache.data.creators),
                            processed_to,
                        });
                        break;
                    }
                }
            }
        }

        Ok(result)
    }

    pub fn get_top_shard_blocks(
        &self,
        next_mc_block_id_short: BlockIdShort,
    ) -> Option<FastHashMap<ShardIdent, BlockSeqno>> {
        if let Some(master) = self
            .masters
            .lock()
            .blocks
            .get(&next_mc_block_id_short.seqno)
        {
            return Some(
                master
                    .top_shard_blocks_info
                    .iter()
                    .map(|(block_id, _)| (block_id.shard, block_id.seqno))
                    .collect(),
            );
        }

        None
    }

    pub fn get_consensus_info_for_mc_block(
        &self,
        mc_block_key: &BlockCacheKey,
    ) -> Result<ConsensusInfo> {
        let consensus_info;
        {
            let master_cache = self.masters.lock();
            let Some(mc_block_entry) = master_cache.blocks.get(&mc_block_key.seqno) else {
                bail!(
                    "get_consensus_info_for_mc_block: Master block not found in cache! ({})",
                    mc_block_key,
                )
            };

            consensus_info = mc_block_entry.cached_state()?.state_extra()?.consensus_info;
        }

        Ok(consensus_info)
    }

    pub fn reset_top_shard_blocks_additional_info(&self) {
        for mut shard_cache in self.shards.iter_mut() {
            shard_cache.data.reset_top_shard_block_additional_info();
        }
    }

    /// Find shard block in cache and then get containing master block id if link exists
    pub fn find_containing_mc_block(&self, shard_block_id: &BlockId) -> Option<(BlockId, bool)> {
        let mc_block_seqno = {
            let guard = self.shards.get(&shard_block_id.shard)?;
            guard
                .value()
                .blocks
                .get(&shard_block_id.seqno)
                .map(|sbc| sbc.ref_by_mc_seqno)?
        };

        let guard = self.masters.lock();
        guard.blocks.get(&mc_block_seqno).map(|block_container| {
            // NOTE: Assume the all collated shard blocks are valid since the
            // containing master block will be different otherwise and will be
            // discarded (stuck then cancelled) during the validation process.
            // FIXME: `is_valid` might have a different meaning like "validation finished"
            let is_valid = true;

            (block_container.block_id, is_valid)
        })
    }

    pub fn get_last_collated_block_and_applied_mc_queue_range(
        &self,
    ) -> (Option<BlockId>, Option<(BlockSeqno, BlockSeqno)>) {
        let master_cache = self.masters.lock();
        (
            master_cache.data.get_last_collated_block_id().cloned(),
            master_cache.data.applied_mc_queue_range,
        )
    }

    pub fn get_all_processed_to_by_mc_block_from_cache(
        &self,
        mc_block_key: &BlockCacheKey,
    ) -> Result<FastHashMap<BlockId, Option<ProcessedTo>>> {
        let mut all_processed_to = FastHashMap::default();

        if mc_block_key.seqno == 0 {
            return Ok(all_processed_to);
        }

        let updated_top_shard_block_ids;
        {
            let master_cache = self.masters.lock();
            let Some(mc_block_entry) = master_cache.blocks.get(&mc_block_key.seqno) else {
                bail!(
                    "get_all_processed_to_by_mc_block_from_cache: Master block not found in cache! ({})",
                    mc_block_key,
                )
            };

            let processed_to = mc_block_entry.int_processed_to().clone();

            updated_top_shard_block_ids = mc_block_entry
                .top_shard_blocks_info
                .iter()
                .filter(|(_, updated)| *updated)
                .map(|(id, _)| id)
                .cloned()
                .collect::<Vec<_>>();
            all_processed_to.insert(mc_block_entry.block_id, Some(processed_to));
        }

        for top_sc_block_id in updated_top_shard_block_ids {
            if top_sc_block_id.seqno == 0 {
                continue;
            }

            let mut processed_to_opt = None;

            // try to find in cache
            if let Some(shard_cache) = self.shards.get(&top_sc_block_id.shard) {
                if let Some(sc_block_entry) = shard_cache.blocks.get(&top_sc_block_id.seqno) {
                    processed_to_opt = Some(sc_block_entry.int_processed_to().clone());
                }
            }

            all_processed_to.insert(top_sc_block_id, processed_to_opt);
        }

        Ok(all_processed_to)
    }

    pub fn read_before_tail_ids_of_mc_block(
        &self,
        mc_block_key: &BlockCacheKey,
    ) -> Result<BeforeTailIdsResult> {
        let mut result = BTreeMap::new();

        if mc_block_key.seqno == 0 {
            return Ok(result);
        }

        let mut prev_shard_blocks_ids;
        {
            let master_cache = self.masters.lock();
            let Some(mc_block_entry) = master_cache.blocks.get(&mc_block_key.seqno) else {
                bail!(
                    "read_before_tail_ids_of_mc_block: Master block not found in cache! ({})",
                    mc_block_key,
                )
            };

            result.insert(
                mc_block_entry.block_id.shard,
                (
                    Some(mc_block_entry.block_id),
                    mc_block_entry.prev_blocks_ids.clone(),
                ),
            );

            prev_shard_blocks_ids = mc_block_entry
                .iter_top_block_ids()
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
            if let Some(shard_cache) = self.shards.get(&prev_sc_block_id.shard) {
                if let Some(sc_block_entry) = shard_cache.blocks.get(&prev_sc_block_id.seqno) {
                    not_found = false;

                    // if shard block included in current master block subgraph
                    if force_include // top shard blocks consider included anyway in this case
                        || sc_block_entry.ref_by_mc_seqno == mc_block_key.seqno
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

    /// Store block candidate in a cache
    #[tracing::instrument(skip_all)]
    pub fn store_collated(
        &self,
        candidate: Box<BlockCandidate>,
        mc_data: Option<Arc<McData>>,
    ) -> Result<BlockCacheStoreResult> {
        let block_id = *candidate.block.id();

        let received_and_collated;
        let last_collated_mc_block_id;
        let applied_mc_queue_range;

        let block_mismatch;
        if mc_data.is_some() {
            let mut masters_guard = self.masters.lock();
            let res = masters_guard.store_collated_block(candidate, mc_data)?;
            block_mismatch = res.block_mismatch;
            received_and_collated = res.received_and_collated;
            last_collated_mc_block_id = masters_guard.data.get_last_collated_block_id().cloned();
            applied_mc_queue_range = masters_guard.data.applied_mc_queue_range;
        } else {
            let res = {
                let mut g = self.shards.entry(block_id.shard).or_default();
                g.store_collated_block(candidate, mc_data)?
            };

            received_and_collated = res.received_and_collated;
            block_mismatch = res.block_mismatch;

            (last_collated_mc_block_id, applied_mc_queue_range) =
                self.get_last_collated_block_and_applied_mc_queue_range();
        };

        Ok(BlockCacheStoreResult {
            received_and_collated,
            block_mismatch,
            last_collated_mc_block_id,
            applied_mc_queue_range,
        })
    }

    /// Store received from bc block in a cache
    #[tracing::instrument(skip_all)]
    pub async fn store_received(
        &self,
        state_node_adapter: Arc<dyn StateNodeAdapter>,
        state: ShardStateStuff,
    ) -> Result<Option<BlockCacheStoreResult>> {
        let block_id = *state.block_id();

        let ctx = ReceivedBlockContext::load(state_node_adapter.as_ref(), state).await?;

        let received_and_collated;
        let last_collated_mc_block_id;
        let applied_mc_queue_range;

        let block_mismatch;
        let last_known_synced = 'sync: {
            if block_id.is_masterchain() {
                let mut masters_guard = self.masters.lock();
                if let Some(last_known_synced) =
                    masters_guard.check_refresh_last_known_synced(block_id.seqno)
                {
                    break 'sync last_known_synced;
                }

                let res = masters_guard.store_received_block(ctx)?;
                block_mismatch = res.block_mismatch;
                received_and_collated = res.received_and_collated;

                // if collated block mismatched remove it and all next from last collated blocks ids
                if block_mismatch {
                    masters_guard
                        .data
                        .remove_last_collated_block_ids_from(&block_id.seqno);
                }

                last_collated_mc_block_id =
                    masters_guard.data.get_last_collated_block_id().cloned();
                applied_mc_queue_range = masters_guard.data.applied_mc_queue_range;
            } else {
                let ref_by_mc_seqno = ctx.ref_by_mc_seqno;

                let res = {
                    let mut g = self.shards.entry(block_id.shard).or_default();
                    if let Some(last_known_synced) =
                        g.check_refresh_last_known_synced(block_id.seqno)
                    {
                        break 'sync last_known_synced;
                    }
                    g.store_received_block(ctx)?
                };

                received_and_collated = res.received_and_collated;
                block_mismatch = res.block_mismatch;

                // if collated block mismatched remove its master and all next from last collated blocks ids
                if block_mismatch {
                    let mut masters_guard = self.masters.lock();
                    masters_guard
                        .data
                        .remove_last_collated_block_ids_from(&ref_by_mc_seqno);
                }

                (last_collated_mc_block_id, applied_mc_queue_range) =
                    self.get_last_collated_block_and_applied_mc_queue_range();
            };

            return Ok(Some(BlockCacheStoreResult {
                received_and_collated,
                block_mismatch,
                last_collated_mc_block_id,
                applied_mc_queue_range,
            }));
        };

        // Fallback if the newer block exists
        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            last_known_synced,
            "received block is not newer than last known synced, skipped"
        );
        Ok(None)
    }

    /// Find master block candidate in cache, append signatures info and return updated
    pub fn store_master_block_validation_result(
        &self,
        block_id: &BlockId,
        validation_result: ValidationStatus,
    ) -> bool {
        let (new_status, signatures, total_weight) = match validation_result {
            ValidationStatus::Complete(res) => {
                (CandidateStatus::Validated, res.signatures, res.total_weight)
            }
            ValidationStatus::Skipped => (CandidateStatus::Synced, Default::default(), 0),
        };

        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            "Saving block validation result to cache: new_status={:?}",
            new_status,
        );

        let mut guard = self.masters.lock();
        let Some(entry) = guard.blocks.get_mut(&block_id.seqno) else {
            tracing::debug!(
                target: tracing_targets::COLLATION_MANAGER,
                "Block does not exist in cache - skip validation result",
            );
            return false;
        };

        match &mut entry.data {
            // Update lifecycle status
            BlockCacheEntryData::Collated {
                candidate_stuff,
                status,
                ..
            } => {
                let changed = *status != new_status;
                candidate_stuff.signatures = signatures;
                candidate_stuff.total_signature_weight = total_weight;
                *status = new_status;
                if !changed {
                    tracing::debug!(
                        target: tracing_targets::COLLATION_MANAGER,
                        "Block is Collated, validation status was not updated - skip validation result",
                    );
                }
                changed
            }
            // We have already received a block from bc, discard validation result
            BlockCacheEntryData::Received { .. } => {
                tracing::debug!(
                    target: tracing_targets::COLLATION_MANAGER,
                    "Block is Received, validation status was not updated - skip validation result",
                );
                false
            }
        }
    }

    pub fn pop_front_applied_mc_block_subgraph(
        &self,
        from_seqno: u32,
    ) -> Result<(McBlockSubgraphExtract, bool)> {
        let mut extracted_mc_block_entry = None;
        let mut is_last = true;
        {
            let mut guard = self.masters.lock();
            let keys = guard.blocks.keys().copied().collect::<Vec<_>>();
            for key in keys {
                if key < from_seqno {
                    continue;
                }

                let btree_map::Entry::Occupied(occupied_entry) = guard.blocks.entry(key) else {
                    bail!("Block cache entry should exist ({})", key)
                };

                if matches!(
                    occupied_entry.get().data,
                    BlockCacheEntryData::Received { .. }
                        | BlockCacheEntryData::Collated {
                            received_after_collation: true,
                            ..
                        }
                ) {
                    if extracted_mc_block_entry.is_some() {
                        is_last = false;
                        break;
                    } else {
                        let mc_block_entry = occupied_entry.remove();
                        // clean previous last collated blocks ids
                        guard
                            .data
                            .remove_last_collated_block_ids_before(&mc_block_entry.block_id.seqno);
                        // update range of received blocks
                        guard.data.move_range_start(mc_block_entry.block_id.seqno);
                        extracted_mc_block_entry = Some(mc_block_entry);
                    }
                }
            }
        }
        let mc_block_entry = extracted_mc_block_entry.unwrap();
        let subgraph_extract = self.extract_mc_block_subgraph(mc_block_entry);
        Ok((subgraph_extract, is_last))
    }

    /// Find all shard blocks included in master block subgraph.
    /// Then extract and return them and master block
    pub fn extract_mc_block_subgraph_for_sync(&self, block_id: &BlockId) -> McBlockSubgraphExtract {
        // 1. Find requested master block
        let mc_block_entry = {
            let mut guard = self.masters.lock();

            let Some(occupied_entry) = guard.blocks.remove(&block_id.seqno) else {
                return McBlockSubgraphExtract::AlreadyExtracted;
            };

            if matches!(
                &occupied_entry.data,
                BlockCacheEntryData::Received { .. }
                    | BlockCacheEntryData::Collated {
                        received_after_collation: true,
                        ..
                    }
            ) {
                guard.data.move_range_start(block_id.seqno);
            }

            // update last known synced block seqno in cache
            guard.check_refresh_last_known_synced(block_id.seqno);

            // clean previous last collated blocks ids
            guard
                .data
                .remove_last_collated_block_ids_before(&block_id.seqno);

            occupied_entry
        };

        self.extract_mc_block_subgraph(mc_block_entry)
    }

    fn extract_mc_block_subgraph(&self, mc_block_entry: BlockCacheEntry) -> McBlockSubgraphExtract {
        let mc_block_key = mc_block_entry.key();

        // 3. By the top shard blocks info find shard blocks of current master block
        let mut prev_shard_blocks_ids = mc_block_entry
            .top_shard_blocks_info
            .iter()
            .filter(|(_, updated)| *updated)
            .map(|(id, _)| id)
            .cloned()
            .collect::<VecDeque<_>>();

        let mut subgraph = McBlockSubgraph {
            master_block: mc_block_entry,
            shard_blocks: vec![],
        };

        // 4. Recursively find prev shard blocks until the end or top shard blocks of prev master reached
        while let Some(prev_shard_block_id) = prev_shard_blocks_ids.pop_front() {
            if prev_shard_block_id.seqno == 0 {
                // skip not existed shard block with seqno 0
                continue;
            }

            let Some(mut shard_cache) = self.shards.get_mut(&prev_shard_block_id.shard) else {
                continue;
            };

            if let Some(sc_block_entry) = shard_cache.blocks.remove(&prev_shard_block_id.seqno) {
                let sc_block_id = sc_block_entry.block_id;

                // if shard block included in current master block subgraph
                if sc_block_entry.ref_by_mc_seqno == mc_block_key.seqno {
                    // 5. Extract
                    sc_block_entry
                        .prev_blocks_ids
                        .iter()
                        .for_each(|sub_prev| prev_shard_blocks_ids.push_back(*sub_prev));
                    subgraph.shard_blocks.push(sc_block_entry);

                    // update last known synced block seqno in cache
                    shard_cache.check_refresh_last_known_synced(sc_block_id.seqno);
                }
            }
        }

        subgraph.shard_blocks.reverse();

        tracing::debug!(
            target: tracing_targets::COLLATION_MANAGER,
            "Extracted master block subgraph ({}): {}",
            mc_block_key,
            DisplayIter(
                subgraph.shard_blocks.iter().map(|sb| sb.key())
            ),
        );

        metrics::gauge!("tycho_blocks_count_in_collation_manager_cache")
            .decrement((subgraph.shard_blocks.len() + 1) as f64);

        McBlockSubgraphExtract::Extracted(subgraph)
    }

    pub fn set_gc_to_boundary(&self, to_blocks_keys: &[BlockCacheKey]) {
        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            to_blocks_keys = %DisplayIntoIter(to_blocks_keys),
            "Set cache prev blocks gc to boundary",
        );

        for block_key in to_blocks_keys {
            if block_key.is_masterchain() {
                let mut guard = self.masters.lock();
                guard.gc_to_boundary = Some(*block_key);
            } else if let Some(mut shard_cache) = self.shards.get_mut(&block_key.shard) {
                shard_cache.gc_to_boundary = Some(*block_key);
            }
        }
    }

    pub fn gc_prev_blocks(&self) {
        let mut removed_count = 0;

        // remove master blocks
        {
            let mut guard = self.masters.lock();
            let mut removed_seqno_list = vec![];
            if let Some(gc_to_block_key) = guard.gc_to_boundary {
                tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                    %gc_to_block_key,
                    "Removing prev mc blocks from cache before",
                );
                guard.blocks.retain(|key, _| {
                    let retained = key >= &gc_to_block_key.seqno;
                    if !retained {
                        tracing::trace!(target: tracing_targets::COLLATION_MANAGER,
                            key,
                            "Previous master block removed from cache",
                        );
                        removed_count += 1;
                        removed_seqno_list.push(*key);
                    }
                    retained
                });
                // remove from last collated blocks ids
                for removed_seqno in removed_seqno_list {
                    guard
                        .data
                        .remove_last_collated_block_ids_before(&removed_seqno);
                }
            }
        }

        // remove shard blocks
        for mut shard_cache in self.shards.iter_mut() {
            if let Some(gc_to_block_key) = shard_cache.gc_to_boundary {
                tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                    %gc_to_block_key,
                    "Removing prev shard blocks from cache before",
                );
                shard_cache.blocks.retain(|key, _| {
                    let retained = key >= &gc_to_block_key.seqno;
                    if !retained {
                        tracing::trace!(target: tracing_targets::COLLATION_MANAGER,
                            key,
                            "Previous shard block removed from cache",
                        );
                        removed_count += 1;
                    }
                    retained
                });
            }
        }

        metrics::gauge!("tycho_blocks_count_in_collation_manager_cache")
            .decrement(removed_count as f64);
    }

    pub fn remove_next_collated_blocks_from_cache(&self, after_blocks_keys: &[BlockCacheKey]) {
        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            after_blocks_keys = %DisplayIntoIter(after_blocks_keys),
            "Removing next blocks from cache after",
        );

        let mut removed_count = 0;

        for block_key in after_blocks_keys {
            if block_key.is_masterchain() {
                let mut guard = self.masters.lock();
                let mut removed_seqno_list = vec![];
                guard.blocks.retain(|key, value| {
                    let is_received = matches!(value.data, BlockCacheEntryData::Received { .. });
                    let retained = key <= &block_key.seqno || is_received;
                    if !retained {
                        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                            retained, is_received, key,
                            "Remove next collated master block from cache",
                        );
                        removed_count += 1;
                        removed_seqno_list.push(*key);
                    }
                    retained
                });
                // remove from last collated blocks ids
                for removed_seqno in removed_seqno_list {
                    guard
                        .data
                        .remove_last_collated_block_ids_from(&removed_seqno);
                }
            } else if let Some(mut shard_cache) = self.shards.get_mut(&block_key.shard) {
                shard_cache.blocks.retain(|key, value| {
                    let is_received = matches!(value.data, BlockCacheEntryData::Received { .. });
                    let retained = key <= &block_key.seqno || is_received;
                    if !retained {
                        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                            retained, is_received, key,
                            "Remove next collated shard block from cache",
                        );
                        removed_count += 1;
                    }
                    retained
                });
            }
        }

        metrics::gauge!("tycho_blocks_count_in_collation_manager_cache")
            .decrement(removed_count as f64);
    }
}

type MasterBlocksCache = BlocksCacheGroup<MasterBlocksCacheData>;
type ShardBlocksCache = BlocksCacheGroup<ShardBlocksCacheData>;

#[derive(Default)]
struct BlocksCacheGroup<T> {
    blocks: BTreeMap<BlockSeqno, BlockCacheEntry>,
    last_known_synced: Option<BlockSeqno>,
    data: T,
    gc_to_boundary: Option<BlockCacheKey>,
}

impl<T: BlocksCacheData> BlocksCacheGroup<T> {
    /// Adds block candidate to the blocks map
    fn store_collated_block(
        &mut self,
        candidate: Box<BlockCandidate>,
        mc_data: Option<Arc<McData>>,
    ) -> Result<StoredBlock> {
        let block_id = *candidate.block.id();
        match self.blocks.entry(block_id.seqno) {
            btree_map::Entry::Occupied(mut occupied) => {
                let entry = occupied.get_mut();
                tracing::debug!(
                    target: tracing_targets::COLLATION_MANAGER,
                    data = %entry.data,
                    "blocks_cache contains block"
                );
                let BlockCacheEntryData::Received {
                    collated_after_receive,
                    additional_shard_block_cache_info,
                    ..
                } = &mut entry.data
                else {
                    bail!("Should not store collated candidate with same block again: {block_id}",);
                };

                if entry.block_id != block_id {
                    return Ok(StoredBlock {
                        received_and_collated: false,
                        block_mismatch: true,
                    });
                }

                *collated_after_receive = true;

                if !block_id.is_masterchain() {
                    *additional_shard_block_cache_info = Some(AdditionalShardBlockCacheInfo {
                        processed_to_anchor_id: candidate.processed_to_anchor_id,
                        block_info: candidate.block.load_info()?.clone(),
                    });
                }

                self.data.on_update_collated(&candidate)?;

                Ok(StoredBlock {
                    received_and_collated: true,
                    block_mismatch: false,
                })
            }
            btree_map::Entry::Vacant(vacant) => {
                self.data.on_insert_collated(&candidate)?;

                vacant.insert(BlockCacheEntry::from_collated(candidate, mc_data)?);

                metrics::gauge!("tycho_blocks_count_in_collation_manager_cache").increment(1);

                Ok(StoredBlock {
                    received_and_collated: false,
                    block_mismatch: false,
                })
            }
        }
    }

    /// Adds an existing block info to the blocks map
    fn store_received_block(&mut self, ctx: ReceivedBlockContext) -> Result<StoredBlock> {
        let block_id = *ctx.state.block_id();

        // NOTE: do not remove state from prev received blocks because sync can finish on any block if cancelled

        let res = match self.blocks.entry(block_id.seqno) {
            btree_map::Entry::Occupied(mut occupied) => {
                let entry = occupied.get_mut();

                tracing::debug!(
                    target: tracing_targets::COLLATION_MANAGER,
                    data = %entry.data,
                    "blocks_cache contains block"
                );

                let BlockCacheEntryData::Collated {
                    received_after_collation,
                    status,
                    ..
                } = &mut entry.data
                else {
                    bail!("Should not store received block with same block again: {block_id}");
                };

                if entry.block_id != block_id {
                    // when collated block mismatched replace it
                    let new_entry = BlockCacheEntry::from_received(
                        ctx.state,
                        ctx.prev_ids,
                        ctx.queue_diff,
                        ctx.out_msgs,
                        ctx.ref_by_mc_seqno,
                    )?;

                    self.data.on_insert_received(&new_entry)?;
                    occupied.insert(new_entry);

                    StoredBlock {
                        received_and_collated: false,
                        block_mismatch: true,
                    }
                } else {
                    *received_after_collation = true;
                    *status = CandidateStatus::Synced;

                    self.data.on_update_received(entry)?;

                    StoredBlock {
                        received_and_collated: true,
                        block_mismatch: false,
                    }
                }
            }
            btree_map::Entry::Vacant(vacant) => {
                let entry = vacant.insert(BlockCacheEntry::from_received(
                    ctx.state,
                    ctx.prev_ids,
                    ctx.queue_diff,
                    ctx.out_msgs,
                    ctx.ref_by_mc_seqno,
                )?);

                metrics::gauge!("tycho_blocks_count_in_collation_manager_cache").increment(1);

                self.data.on_insert_received(entry)?;

                StoredBlock {
                    received_and_collated: false,
                    block_mismatch: false,
                }
            }
        };

        Ok(res)
    }

    /// Returns Some(seqno: u32) of last known synced block when it is newer than provided
    fn check_refresh_last_known_synced(&mut self, block_seqno: BlockSeqno) -> Option<BlockSeqno> {
        if let Some(last_known) = self.last_known_synced {
            if last_known >= block_seqno {
                return Some(last_known);
            }
        }
        self.last_known_synced = Some(block_seqno);
        None
    }
}

trait BlocksCacheData {
    type NewCollated;
    type NewReceived;

    fn on_update_collated(&mut self, candidate: &BlockCandidate) -> Result<()>;
    fn on_insert_collated(&mut self, candidate: &BlockCandidate) -> Result<Self::NewCollated>;

    fn on_update_received(&mut self, entry: &BlockCacheEntry) -> Result<()>;
    fn on_insert_received(&mut self, entry: &BlockCacheEntry) -> Result<Self::NewReceived>;
}

#[derive(Default)]
struct MasterBlocksCacheData {
    /// Queue of last blocks ids collated by ourselves
    last_collated_mc_block_ids: BTreeMap<BlockSeqno, BlockId>,
    applied_mc_queue_range: Option<(BlockSeqno, BlockSeqno)>,
}

impl MasterBlocksCacheData {
    fn insert_last_collated_block_id(&mut self, block_id: &BlockId) {
        self.move_range_start(block_id.seqno);
        self.last_collated_mc_block_ids
            .insert(block_id.seqno, *block_id);
    }

    fn remove_last_collated_block_ids_from(&mut self, from_block_seqno: &BlockSeqno) {
        self.last_collated_mc_block_ids
            .retain(|seqno, _| seqno < from_block_seqno);
    }

    fn remove_last_collated_block_ids_before(&mut self, before_block_seqno: &BlockSeqno) {
        self.last_collated_mc_block_ids
            .retain(|seqno, _| seqno >= before_block_seqno);
    }

    fn get_last_collated_block_id(&self) -> Option<&BlockId> {
        self.last_collated_mc_block_ids
            .last_key_value()
            .map(|(_, v)| v)
    }

    fn move_range_start(&mut self, block_seqno: u32) {
        if let Some((range_start, range_end)) = self.applied_mc_queue_range {
            if block_seqno >= range_start {
                let new_range_start = block_seqno + 1;
                self.applied_mc_queue_range =
                    (new_range_start <= range_end).then_some((new_range_start, range_end));
            }
        }
    }

    fn move_range_end(&mut self, block_seqno: u32) {
        if let Some((_, range_end)) = &mut self.applied_mc_queue_range {
            *range_end = block_seqno;
        } else {
            self.applied_mc_queue_range = Some((block_seqno, block_seqno));
        }
    }
}

impl BlocksCacheData for MasterBlocksCacheData {
    type NewCollated = ();
    type NewReceived = ();

    fn on_update_collated(&mut self, candidate: &BlockCandidate) -> Result<()> {
        self.insert_last_collated_block_id(candidate.block.id());
        Ok(())
    }

    fn on_insert_collated(&mut self, candidate: &BlockCandidate) -> Result<Self::NewCollated> {
        self.insert_last_collated_block_id(candidate.block.id());
        Ok(())
    }

    fn on_update_received(&mut self, entry: &BlockCacheEntry) -> Result<()> {
        self.move_range_end(entry.block_id.seqno);
        Ok(())
    }

    fn on_insert_received(&mut self, entry: &BlockCacheEntry) -> Result<Self::NewReceived> {
        self.move_range_end(entry.block_id.seqno);
        Ok(())
    }
}

#[derive(Default)]
struct ShardBlocksCacheData {
    value_flow: ValueFlow,
    proof_funds: ShardFeeCreated,
    #[cfg(feature = "block-creator-stats")]
    creators: Vec<everscale_types::cell::HashBytes>,
}

impl ShardBlocksCacheData {
    fn update_top_shard_block_additional_info(&mut self, candidate: &BlockCandidate) -> Result<()> {
        self.value_flow = candidate.value_flow.clone();

        self.proof_funds
            .fees
            .try_add_assign(&candidate.value_flow.fees_collected)?;
        self.proof_funds
            .create
            .try_add_assign(&candidate.value_flow.created)?;

        #[cfg(feature = "block-creator-stats")]
        self.creators.push(candidate.created_by);

        Ok(())
    }

    fn reset_top_shard_block_additional_info(&mut self) {
        self.value_flow = Default::default();
        self.proof_funds = Default::default();

        #[cfg(feature = "block-creator-stats")]
        self.creators.clear();
    }
}

impl BlocksCacheData for ShardBlocksCacheData {
    type NewCollated = ();
    type NewReceived = ();

    fn on_update_collated(&mut self, candidate: &BlockCandidate) -> Result<()> {
        self.update_top_shard_block_additional_info(candidate)
    }

    fn on_insert_collated(&mut self, candidate: &BlockCandidate) -> Result<Self::NewCollated> {
        self.update_top_shard_block_additional_info(candidate)
    }

    fn on_update_received(&mut self, _: &BlockCacheEntry) -> Result<()> {
        Ok(())
    }

    fn on_insert_received(&mut self, _: &BlockCacheEntry) -> Result<Self::NewReceived> {
        Ok(())
    }
}

struct ReceivedBlockContext {
    state: ShardStateStuff,
    prev_ids: Vec<BlockId>,
    queue_diff: QueueDiffStuff,
    out_msgs: Lazy<OutMsgDescr>,
    ref_by_mc_seqno: u32,
    // NOTE: `BlockStuff` can also be added here if needed since it's already loaded
}

impl ReceivedBlockContext {
    async fn load(
        state_node_adapter: &dyn StateNodeAdapter,
        state: ShardStateStuff,
    ) -> Result<Self> {
        static EMPTY_OUT_MSGS: OnceLock<Lazy<OutMsgDescr>> = OnceLock::new();

        let block_id = state.block_id();
        if block_id.seqno == 0 {
            let queue_diff = QueueDiffStuff::new_empty(block_id);

            return Ok(Self {
                state,
                prev_ids: Vec::new(),
                queue_diff,
                out_msgs: EMPTY_OUT_MSGS
                    .get_or_init(|| Lazy::new(&OutMsgDescr::new()).unwrap())
                    .clone(),
                ref_by_mc_seqno: 0,
            });
        }

        let Some(block_handle) = state_node_adapter.load_block_handle(block_id).await? else {
            bail!("block not found: {block_id}");
        };
        let ref_by_mc_seqno = block_handle.ref_by_mc_seqno();

        let Some(block_stuff) = state_node_adapter.load_block(block_id).await? else {
            bail!("block not found: {block_id}");
        };

        let out_msgs = block_stuff.load_extra()?.out_msg_description.clone();
        let queue_diff = state_node_adapter.load_diff(block_id).await?.unwrap();

        let (prev1, prev2) = block_stuff.construct_prev_id()?;
        let mut prev_ids = Vec::new();
        prev_ids.push(prev1);
        prev_ids.extend(prev2);

        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            prev_block_ids = %DisplayIntoIter(&prev_ids),
            "loaded block and queue diff stuff",
        );

        Ok(Self {
            state,
            prev_ids,
            queue_diff,
            out_msgs,
            ref_by_mc_seqno,
        })
    }
}

struct StoredBlock {
    received_and_collated: bool,
    block_mismatch: bool,
}

pub type BeforeTailIdsResult = BTreeMap<ShardIdent, (Option<BlockId>, Vec<BlockId>)>;
