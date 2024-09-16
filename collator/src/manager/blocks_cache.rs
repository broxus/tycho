use std::collections::{btree_map, BTreeMap, VecDeque};
use std::sync::{Arc, OnceLock};

use anyhow::{bail, Result};
use everscale_types::models::{BlockId, BlockIdShort, Lazy, OutMsgDescr, ShardIdent, ValueFlow};
use parking_lot::Mutex;
use tycho_block_util::queue::{QueueDiffStuff, QueueKey};
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
    BlockCandidate, DisplayIntoIter, DisplayIter, McData, ProofFunds, TopBlockDescription,
};
use crate::validator::ValidationStatus;

#[derive(Default)]
pub struct BlocksCache {
    masters: Mutex<MasterBlocksCache>,
    shards: FastDashMap<ShardIdent, ShardBlocksCache>,
}

impl BlocksCache {
    /// Find top shard blocks in cache for the next master block collation
    pub fn get_top_shard_blocks_info_for_mc_block(
        &self,
        next_mc_block_id_short: BlockIdShort,
        _next_mc_block_chain_time: u64,
        _trigger_shard_block_id_opt: Option<BlockId>,
    ) -> Result<Vec<TopBlockDescription>> {
        let mut result = vec![];
        for mut shard_cache in self.shards.iter_mut() {
            for (_, entry) in shard_cache.blocks.iter().rev() {
                if entry.containing_mc_block.is_none()
                    || entry.containing_mc_block == Some(next_mc_block_id_short)
                {
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
                        });
                        break;
                    }
                }
            }
        }

        Ok(result)
    }

    /// Find shard block in cache and then get containing master block id if link exists
    pub fn find_containing_mc_block(&self, shard_block_id: &BlockId) -> Option<(BlockId, bool)> {
        // TODO: handle when master block link exist but there is not block itself

        let mc_block_key = {
            let guard = self.shards.get(&shard_block_id.shard)?;
            guard
                .value()
                .blocks
                .get(&shard_block_id.seqno)
                .and_then(|sbc| sbc.containing_mc_block)?
        };

        let guard = self.masters.lock();
        guard
            .blocks
            .get(&mc_block_key.seqno)
            .map(|block_container| {
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
            master_cache.data.last_collated_mc_block_id,
            master_cache.data.applied_mc_queue_range,
        )
    }

    pub fn update_last_collated_mc_block_id(&self, block_id: BlockId) {
        let mut guard = self.masters.lock();
        guard.data.update_last_collated_block_id(&block_id);
    }

    pub fn get_all_processed_to_by_mc_block_from_cache(
        &self,
        mc_block_key: &BlockCacheKey,
    ) -> Result<FastHashMap<BlockId, Option<BTreeMap<ShardIdent, QueueKey>>>> {
        let mut all_processed_to = FastHashMap::default();

        if mc_block_key.seqno == 0 {
            return Ok(all_processed_to);
        }

        let updated_top_shard_block_ids;
        {
            let master_cache = self.masters.lock();
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
            all_processed_to.insert(
                mc_block_entry.block_id,
                Some(queue_diff_stuff.as_ref().processed_upto.clone()),
            );
        }

        for top_sc_block_id in updated_top_shard_block_ids {
            if top_sc_block_id.seqno == 0 {
                continue;
            }

            // try to find in cache
            let mut queue_diff_stuff_opt = None;
            if let Some(shard_cache) = self.shards.get(&top_sc_block_id.shard) {
                if let Some(sc_block_entry) = shard_cache.blocks.get(&top_sc_block_id.seqno) {
                    let (queue_diff_stuff, _) = sc_block_entry.queue_diff_and_msgs()?;
                    queue_diff_stuff_opt = Some(queue_diff_stuff.clone());
                }
            }

            all_processed_to.insert(
                top_sc_block_id,
                queue_diff_stuff_opt.map(|qds| qds.as_ref().processed_upto.clone()),
            );
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
                bail!("Master block not found in cache! ({})", mc_block_key)
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
        let mut set_containing_mc_block = None;
        if mc_data.is_some() {
            let mut g = self.masters.lock();
            let res = g.store_collated_block(candidate, mc_data)?;

            received_and_collated = res.received_and_collated;
            last_collated_mc_block_id = g.data.last_collated_mc_block_id;
            applied_mc_queue_range = g.data.applied_mc_queue_range;

            if let Some(ids) = res.new_data {
                set_containing_mc_block = Some(ids.top_shard_block_ids);
            }
        } else {
            let res = {
                let mut g = self.shards.entry(block_id.shard).or_default();
                g.store_collated_block(candidate, mc_data)?
            };

            received_and_collated = res.received_and_collated;

            let g = self.masters.lock();
            last_collated_mc_block_id = g.data.last_collated_mc_block_id;
            applied_mc_queue_range = g.data.applied_mc_queue_range;
        };

        if let Some(ids) = set_containing_mc_block {
            // Traverse shard blocks and update their link to the containing master block
            self.set_containing_mc_block(block_id.as_short_id(), ids);
        }

        Ok(BlockCacheStoreResult {
            received_and_collated,
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
        let mut set_containing_mc_block = None;

        let last_known_synced = 'sync: {
            if block_id.is_masterchain() {
                let mut g = self.masters.lock();
                if let Some(last_known_synced) = g.check_refresh_last_known_synced(block_id.seqno) {
                    break 'sync last_known_synced;
                }

                let res = g.store_received_block(ctx)?;

                received_and_collated = res.received_and_collated;
                last_collated_mc_block_id = g.data.last_collated_mc_block_id;
                applied_mc_queue_range = g.data.applied_mc_queue_range;

                if let Some(ids) = res.new_data {
                    set_containing_mc_block = Some(ids.top_shard_block_ids);
                }
            } else {
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

                let g = self.masters.lock();
                last_collated_mc_block_id = g.data.last_collated_mc_block_id;
                applied_mc_queue_range = g.data.applied_mc_queue_range;
            };

            if let Some(ids) = set_containing_mc_block {
                // Traverse shard blocks and update their link to the containing master block
                self.set_containing_mc_block(block_id.as_short_id(), ids);
            }

            return Ok(Some(BlockCacheStoreResult {
                received_and_collated,
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

    fn set_containing_mc_block(
        &self,
        mc_block_key: BlockCacheKey,
        mut top_block_ids: VecDeque<BlockId>,
    ) {
        while let Some(prev_shard_block_id) = top_block_ids.pop_front() {
            if let Some(mut shard_cache) = self.shards.get_mut(&prev_shard_block_id.shard) {
                if let Some(shard_block) = shard_cache.blocks.get_mut(&prev_shard_block_id.seqno) {
                    if shard_block.containing_mc_block.is_none() {
                        shard_block.containing_mc_block = Some(mc_block_key);
                        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                            block_id = %shard_block.block_id,
                            "containing_mc_block set"
                        );

                        top_block_ids.extend(&shard_block.prev_blocks_ids);
                    }
                }
            }
        }
    }

    /// Find master block candidate in cache, append signatures info and return updated
    pub fn store_master_block_validation_result(
        &self,
        block_id: &BlockId,
        validation_result: ValidationStatus,
    ) -> bool {
        let (new_status, signatures) = match validation_result {
            ValidationStatus::Complete(signatures) => (CandidateStatus::Validated, signatures),
            ValidationStatus::Skipped => (CandidateStatus::Synced, Default::default()),
        };

        let mut guard = self.masters.lock();
        let Some(entry) = guard.blocks.get_mut(&block_id.seqno) else {
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
                *status = new_status;
                changed
            }
            // We have already received a block from bc, discard validation result
            BlockCacheEntryData::Received { .. } => false,
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

                if let BlockCacheEntryData::Received { .. } = occupied_entry.get().data {
                    if extracted_mc_block_entry.is_some() {
                        is_last = false;
                        break;
                    } else {
                        let mc_block_entry = occupied_entry.remove();
                        guard.data.move_range_start(mc_block_entry.block_id.seqno);
                        extracted_mc_block_entry = Some(mc_block_entry);
                    }
                }
            }
        }
        let mc_block_entry = extracted_mc_block_entry.unwrap();
        let subgraph_extract = self.extract_mc_block_subgraph(mc_block_entry)?;
        Ok((subgraph_extract, is_last))
    }

    /// Find all shard blocks included in master block subgraph.
    /// Then extract and return them and master block
    pub fn extract_mc_block_subgraph_for_sync(
        &self,
        block_id: &BlockId,
    ) -> Result<McBlockSubgraphExtract> {
        // 1. Find requested master block
        let mc_block_entry = {
            let mut guard = self.masters.lock();

            let Some(occupied_entry) = guard.blocks.remove(&block_id.seqno) else {
                return Ok(McBlockSubgraphExtract::AlreadyExtracted);
            };

            if matches!(&occupied_entry.data, BlockCacheEntryData::Received { .. }) {
                guard.data.move_range_start(block_id.seqno);
            }

            // update last known synced block seqno in cache
            guard.check_refresh_last_known_synced(block_id.seqno);

            occupied_entry
        };

        self.extract_mc_block_subgraph(mc_block_entry)
    }

    fn extract_mc_block_subgraph(
        &self,
        mc_block_entry: BlockCacheEntry,
    ) -> Result<McBlockSubgraphExtract> {
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
                if matches!(sc_block_entry.containing_mc_block, Some(key) if key == mc_block_key) {
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

        Ok(McBlockSubgraphExtract::Extracted(subgraph))
    }

    pub fn remove_prev_blocks_from_cache(&self, to_blocks_keys: &[BlockCacheKey]) {
        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            "Removing prev blocks from cache before: {}",
            DisplayIntoIter(to_blocks_keys),
        );

        for block_key in to_blocks_keys {
            if block_key.is_masterchain() {
                let mut guard = self.masters.lock();
                guard.blocks.retain(|key, _| key >= &block_key.seqno);
            } else if let Some(mut shard_cache) = self.shards.get_mut(&block_key.shard) {
                shard_cache.blocks.retain(|key, _| key >= &block_key.seqno);
            }
        }
    }
}

type MasterBlocksCache = BlocksCacheGroup<MasterBlocksCacheData>;
type ShardBlocksCache = BlocksCacheGroup<ShardBlocksCacheData>;

#[derive(Default)]
struct BlocksCacheGroup<T> {
    blocks: BTreeMap<BlockSeqno, BlockCacheEntry>,
    last_known_synced: Option<BlockSeqno>,
    data: T,
}

impl<T: BlocksCacheData> BlocksCacheGroup<T> {
    /// Adds block candidate to the blocks map
    fn store_collated_block(
        &mut self,
        candidate: Box<BlockCandidate>,
        mc_data: Option<Arc<McData>>,
    ) -> Result<StoredBlock<T::NewCollated>> {
        let block_id = *candidate.block.id();
        match self.blocks.entry(block_id.seqno) {
            btree_map::Entry::Occupied(mut entry) => {
                let entry = entry.get_mut();

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
                    bail!(
                        "Should not store collated candidate with same block again: \
                        {block_id}",
                    );
                };

                assert_eq!(
                    block_id, entry.block_id,
                    "Block received from bc mismatch with collated one"
                );

                *collated_after_receive = true;

                if !block_id.is_masterchain() {
                    *additional_shard_block_cache_info = Some(AdditionalShardBlockCacheInfo {
                        processed_to_anchor_id: candidate.processed_to_anchor_id,
                        block_info: candidate.block.load_info()?,
                    });
                }

                self.data.on_update_collated(&candidate)?;

                Ok(StoredBlock {
                    received_and_collated: true,
                    // status: CandidateStatus::Synced,
                    new_data: None,
                })
            }
            btree_map::Entry::Vacant(vacant) => {
                let new_data = self.data.on_insert_collated(&candidate)?;

                vacant.insert(BlockCacheEntry::from_collated(candidate, mc_data)?);

                Ok(StoredBlock {
                    received_and_collated: false,
                    // status: CandidateStatus::Collated,
                    new_data: Some(new_data),
                })
            }
        }
    }

    /// Adds an existing block info to the blocks map
    fn store_received_block(
        &mut self,
        ctx: ReceivedBlockContext,
    ) -> Result<StoredBlock<T::NewReceived>> {
        let block_id = *ctx.state.block_id();

        let mut prev_ids_to_clear = Vec::new();
        let res = match self.blocks.entry(block_id.seqno) {
            btree_map::Entry::Occupied(mut entry) => {
                let entry = entry.get_mut();

                tracing::debug!(
                    target: tracing_targets::COLLATION_MANAGER,
                    data = %entry.data,
                    "blocks_cache contains block"
                );

                assert_eq!(
                    block_id, entry.block_id,
                    "Block received from bc mismatch with collated one"
                );

                let BlockCacheEntryData::Collated {
                    received_after_collation,
                    status,
                    ..
                } = &mut entry.data
                else {
                    bail!(
                        "Should not store collated candidate with same block again: \
                        {block_id}",
                    );
                };

                *received_after_collation = true;
                *status = CandidateStatus::Synced;

                // Remove state from prev block because we need only last one
                if block_id.is_masterchain() {
                    prev_ids_to_clear = entry.prev_blocks_ids.clone();
                }
                // TODO: remove state from prev shard blocks that are not top blocks for masters

                self.data.on_update_received(entry)?;

                StoredBlock {
                    received_and_collated: true,
                    // status: CandidateStatus::Synced,
                    new_data: None,
                }
            }
            btree_map::Entry::Vacant(vacant) => {
                let entry = vacant.insert(BlockCacheEntry::from_received(
                    ctx.state,
                    ctx.prev_ids,
                    ctx.queue_diff,
                    ctx.out_msgs,
                )?);

                // Remove state from prev block because we need only last one
                if block_id.is_masterchain() {
                    prev_ids_to_clear = entry.prev_blocks_ids.clone();
                }

                let new_data = self.data.on_insert_received(entry)?;

                StoredBlock {
                    received_and_collated: false,
                    // status: CandidateStatus::Synced,
                    new_data: Some(new_data),
                }
            }
        };

        // Remove state from prev block because we need only last one
        for block_id in prev_ids_to_clear {
            if let Some(entry) = self.blocks.get_mut(&block_id.seqno) {
                if let BlockCacheEntryData::Received { cached_state, .. } = &mut entry.data {
                    tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                        prev_block_id = %block_id.as_short_id(),
                        "state stuff removed from prev block in cache"
                    );
                    *cached_state = None;
                }
            }
        }

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
    /// id of last master block collated by ourselves
    last_collated_mc_block_id: Option<BlockId>,
    applied_mc_queue_range: Option<(BlockSeqno, BlockSeqno)>,
}

impl MasterBlocksCacheData {
    fn update_last_collated_block_id(&mut self, block_id: &BlockId) {
        self.move_range_start(block_id.seqno);
        self.last_collated_mc_block_id = Some(*block_id);
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
    type NewCollated = MasterBlockIds;
    type NewReceived = MasterBlockIds;

    fn on_update_collated(&mut self, candidate: &BlockCandidate) -> Result<()> {
        self.update_last_collated_block_id(candidate.block.id());
        Ok(())
    }

    fn on_insert_collated(&mut self, candidate: &BlockCandidate) -> Result<Self::NewCollated> {
        Ok(MasterBlockIds {
            top_shard_block_ids: candidate.top_shard_blocks_ids.iter().copied().collect(),
        })
    }

    fn on_update_received(&mut self, entry: &BlockCacheEntry) -> Result<()> {
        self.move_range_end(entry.block_id.seqno);
        Ok(())
    }

    fn on_insert_received(&mut self, entry: &BlockCacheEntry) -> Result<Self::NewReceived> {
        self.move_range_end(entry.block_id.seqno);

        Ok(MasterBlockIds {
            top_shard_block_ids: entry.iter_top_block_ids().copied().collect(),
        })
    }
}

#[derive(Default)]
struct ShardBlocksCacheData {
    value_flow: ValueFlow,
    proof_funds: ProofFunds,
    #[cfg(feature = "block-creator-stats")]
    creators: Vec<everscale_types::cell::HashBytes>,
}

impl BlocksCacheData for ShardBlocksCacheData {
    type NewCollated = ();
    type NewReceived = ();

    fn on_update_collated(&mut self, _: &BlockCandidate) -> Result<()> {
        Ok(())
    }

    fn on_insert_collated(&mut self, candidate: &BlockCandidate) -> Result<()> {
        self.proof_funds
            .fees_collected
            .try_add_assign(&candidate.fees_collected)?;
        self.proof_funds
            .funds_created
            .try_add_assign(&candidate.funds_created)?;

        #[cfg(feature = "block-creator-stats")]
        self.creators.push(candidate.created_by);

        Ok(())
    }

    fn on_update_received(&mut self, _: &BlockCacheEntry) -> Result<()> {
        Ok(())
    }

    fn on_insert_received(&mut self, _: &BlockCacheEntry) -> Result<Self::NewReceived> {
        // FIXME: Why we are not updating `value_flow` here?
        Ok(())
    }
}

struct ReceivedBlockContext {
    state: ShardStateStuff,
    prev_ids: Vec<BlockId>,
    queue_diff: QueueDiffStuff,
    out_msgs: Lazy<OutMsgDescr>,
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
            });
        }

        let Some(block_stuff) = state_node_adapter.load_block(block_id).await? else {
            bail!("block not found: {block_id}");
        };

        let out_msgs = block_stuff.block().load_extra()?.out_msg_description;
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
        })
    }
}

struct StoredBlock<T> {
    received_and_collated: bool,
    new_data: Option<T>,
}

struct MasterBlockIds {
    top_shard_block_ids: VecDeque<BlockId>,
}

pub type BeforeTailIdsResult = BTreeMap<ShardIdent, (Option<BlockId>, Vec<BlockId>)>;
