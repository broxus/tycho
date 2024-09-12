use std::collections::{btree_map, BTreeMap, VecDeque};
use std::sync::Arc;

use anyhow::{bail, Result};
use everscale_types::models::{BlockId, ShardIdent};
use parking_lot::MutexGuard;
use tycho_block_util::queue::QueueKey;
use tycho_block_util::state::ShardStateStuff;
use tycho_util::FastHashMap;

use super::types::{
    self, AppliedBlockStuffContainer, BlockCacheEntry, BlockCacheEntryKind, BlockCacheKey,
    BlockCacheStoreResult, BlockSeqno, BlocksCache, MasterBlocksCache, McBlockSubgraph,
    McBlockSubgraphExtract, SendSyncStatus,
};
use super::utils;
use crate::state_node::StateNodeAdapter;
use crate::tracing_targets;
use crate::types::{BlockCandidate, DisplaySlice, McData};
use crate::validator::ValidationStatus;

impl BlocksCache {
    pub(super) fn get_last_collated_block_and_applied_mc_queue_range(
        &self,
    ) -> (Option<BlockId>, Option<(BlockSeqno, BlockSeqno)>) {
        let master_cache = self.masters.lock();
        (
            master_cache.last_collated_mc_block_id,
            master_cache.applied_mc_queue_range,
        )
    }

    pub(super) fn update_last_collated_mc_block_id(&self, block_id: BlockId) {
        let mut guard = self.masters.lock();
        Self::update_last_collated_mc_block_id_and_applied_mc_queue_range(&mut guard, block_id);
    }

    fn update_last_collated_mc_block_id_and_applied_mc_queue_range(
        guard: &mut MutexGuard<'_, MasterBlocksCache>,
        block_id: BlockId,
    ) {
        Self::update_applied_mc_queue_range(guard, block_id.seqno);
        guard.last_collated_mc_block_id = Some(block_id);
    }

    fn update_applied_mc_queue_range(
        guard: &mut MutexGuard<'_, MasterBlocksCache>,
        block_seqno: u32,
    ) {
        if let Some((range_start, range_end)) = guard.applied_mc_queue_range {
            if block_seqno >= range_start {
                let new_range_start = block_seqno + 1;
                if new_range_start > range_end {
                    guard.applied_mc_queue_range = None;
                } else {
                    guard.applied_mc_queue_range = Some((new_range_start, range_end));
                }
            }
        }
    }

    pub(super) fn get_all_processed_to_by_mc_block_from_cache(
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
                *mc_block_entry.block_id(),
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

    pub(super) fn read_before_tail_ids_of_mc_block(
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
    pub(super) fn store_candidate(
        &self,
        candidate: Box<BlockCandidate>,
        mc_data: Option<Arc<McData>>,
    ) -> Result<BlockCacheStoreResult> {
        let block_id = *candidate.block.id();

        let mut entry = BlockCacheEntry::from_candidate(candidate, mc_data)?;

        let result = if block_id.shard.is_masterchain() {
            // store master block to cache
            let mut guard = self.masters.lock();
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

            // update last collated mc block id and applied mc queue range info
            Self::update_last_collated_mc_block_id_and_applied_mc_queue_range(&mut guard, block_id);

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
            let mut shard_cache = self.shards.entry(block_id.shard).or_default();
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
                            #[cfg(feature = "block-creator-stats")]
                            e.candidate.created_by,
                        )
                    });
                    let inserted = vacant.insert(entry);
                    ((inserted.kind, inserted.send_sync_status), aggregate)
                }
            };

            // aggregate additional info for TopBlockDescription
            if let Some(aggregate) = aggregate {
                shard_cache
                    .proof_funds
                    .fees_collected
                    .checked_add(&aggregate.0)?;
                shard_cache
                    .proof_funds
                    .funds_created
                    .checked_add(&aggregate.1)?;
                #[cfg(feature = "block-creator-stats")]
                shard_cache.creators.push(aggregate.2);
            }
            drop(shard_cache); // TODO: use scope instead

            let mc_guard = self.masters.lock();
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
            if let Some(mut shard_cache) = self.shards.get_mut(&prev_shard_block_id.shard) {
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
    pub(super) async fn store_block_from_bc(
        &self,
        state_node_adapter: Arc<dyn StateNodeAdapter>,
        state: ShardStateStuff,
    ) -> Result<Option<BlockCacheStoreResult>> {
        let block_id = *state.block_id();

        // TODO: should build entry only on insert

        // load queue diff
        let loaded =
            utils::load_block_queue_diff_stuff(state_node_adapter.as_ref(), &block_id).await?;

        // build entry
        let entry = BlockCacheEntry::from_block_from_bc(
            state,
            loaded.prev_ids,
            loaded.queue_diff,
            loaded.out_msgs,
        )?;

        let result = if block_id.shard.is_masterchain() {
            // store master block to cache
            let mut guard = self.masters.lock();

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

            let kind;
            let send_sync_status;
            let prev_shard_blocks_ids;
            let prev_ids;
            match guard.blocks.entry(block_id.seqno) {
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

                    kind = existing.kind;
                    send_sync_status = existing.send_sync_status;
                    prev_shard_blocks_ids = VecDeque::new();
                    prev_ids = Vec::new();
                }
                btree_map::Entry::Vacant(vacant) => {
                    let inserted = vacant.insert(entry);

                    kind = inserted.kind;
                    send_sync_status = inserted.send_sync_status;
                    prev_shard_blocks_ids = inserted.top_shard_blocks_ids_iter().cloned().collect();
                    prev_ids = inserted.prev_blocks_ids.clone();
                }
            };

            // remove state from prev mc block because we need only last one
            for prev_block_id in prev_ids {
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
                kind,
                send_sync_status,
                last_collated_mc_block_id: guard.last_collated_mc_block_id,
                applied_mc_queue_range: guard.applied_mc_queue_range,
            };
            drop(guard); // TODO: use scope instead

            if result.kind == BlockCacheEntryKind::Received {
                // traverse through including shard blocks and update their link to the containing master block
                self.set_containing_mc_block(block_id.as_short_id(), prev_shard_blocks_ids);
            }

            result
        } else {
            // store shard block to cache
            let mut shard_cache = self.shards.entry(block_id.shard).or_default();

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

            let (kind, send_sync_status) = match shard_cache.blocks.entry(block_id.seqno) {
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

            let mc_guard = self.masters.lock();
            BlockCacheStoreResult {
                block_id,
                kind,
                send_sync_status,
                last_collated_mc_block_id: mc_guard.last_collated_mc_block_id,
                applied_mc_queue_range: mc_guard.applied_mc_queue_range,
            }
        };

        Ok(Some(result))
    }

    /// Find master block candidate in cache, append signatures info and return updated
    pub(super) fn store_master_block_validation_result(
        &self,
        block_id: &BlockId,
        validation_result: ValidationStatus,
    ) -> bool {
        let (is_valid, already_synced, signatures) = match validation_result {
            ValidationStatus::Skipped => (true, true, Default::default()),
            ValidationStatus::Complete(signatures) => (true, false, signatures),
        };

        let mut guard = self.masters.lock();
        if let Some(entry) = guard.blocks.get_mut(&block_id.seqno) {
            entry.set_validation_result(is_valid, already_synced, signatures);
            return true;
        }

        false
    }

    pub(super) fn pop_front_applied_mc_block_subgraph(
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
                    occupied_entry.get().kind,
                    BlockCacheEntryKind::Received | BlockCacheEntryKind::ReceivedAndCollated
                ) {
                    if extracted_mc_block_entry.is_some() {
                        is_last = false;
                        break;
                    } else {
                        let mc_block_entry = occupied_entry.remove();
                        Self::update_applied_mc_queue_range(
                            &mut guard,
                            mc_block_entry.block_id().seqno,
                        );
                        extracted_mc_block_entry = Some(mc_block_entry);
                    }
                }
            }
        }
        let mc_block_entry = extracted_mc_block_entry.unwrap();
        let subgraph_extract = self.extract_mc_block_subgraph_internal(mc_block_entry)?;
        Ok((subgraph_extract, is_last))
    }

    /// Find all shard blocks included in master block subgraph.
    /// Then extract and return them and master block
    pub(super) fn extract_mc_block_subgraph_for_sync(
        &self,
        block_id: &BlockId,
        only_if_valid: bool,
    ) -> Result<McBlockSubgraphExtract> {
        // 1. Find requested master block
        let mc_block_entry;
        {
            let mut guard = self.masters.lock();

            let btree_map::Entry::Occupied(occupied_entry) = guard.blocks.entry(block_id.seqno)
            else {
                return Ok(McBlockSubgraphExtract::AlreadyExtracted);
            };

            {
                let mc_block_entry = occupied_entry.get();
                if !mc_block_entry.is_valid && only_if_valid {
                    return Ok(McBlockSubgraphExtract::NotFullValid);
                }
            }

            // 2. Extract
            mc_block_entry = occupied_entry.remove();

            if matches!(
                mc_block_entry.kind,
                BlockCacheEntryKind::Received | BlockCacheEntryKind::ReceivedAndCollated
            ) {
                Self::update_applied_mc_queue_range(&mut guard, block_id.seqno);
            }

            // update last known synced block seqno in cache
            types::check_refresh_last_known_synced(&mut guard.last_known_synced, block_id.seqno);
        }

        self.extract_mc_block_subgraph_internal(mc_block_entry)
    }

    fn extract_mc_block_subgraph_internal(
        &self,
        mc_block_entry: BlockCacheEntry,
    ) -> Result<McBlockSubgraphExtract> {
        let mc_block_key = *mc_block_entry.key();

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
                let sc_block_id = *sc_block_entry.block_id();

                // if shard block included in current master block subgraph
                if matches!(sc_block_entry.containing_mc_block, Some(key) if key == mc_block_key) {
                    // 5. Extract
                    sc_block_entry
                        .prev_blocks_ids
                        .iter()
                        .for_each(|sub_prev| prev_shard_blocks_ids.push_back(*sub_prev));
                    subgraph.shard_blocks.push(sc_block_entry);

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
            "Extracted master block subgraph ({}): {}",
            mc_block_key,
            DisplaySlice(
                subgraph.shard_blocks.iter().map(|sb| *sb.key())
                .collect::<Vec<_>>().as_slice()
            ),
        );

        Ok(McBlockSubgraphExtract::Extracted(subgraph))
    }

    pub(super) fn remove_prev_blocks_from_cache(&self, to_blocks_keys: &[BlockCacheKey]) {
        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            "Removing prev blocks from cache before: {}",
            DisplaySlice(to_blocks_keys),
        );
        scopeguard::defer! {
            tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                "Prev blocks removed from cache before: {}",
                DisplaySlice(to_blocks_keys),
            );
        };
        for block_key in to_blocks_keys.iter() {
            if block_key.shard.is_masterchain() {
                let mut guard = self.masters.lock();
                guard.blocks.retain(|key, _| key >= &block_key.seqno);
            } else if let Some(mut shard_cache) = self.shards.get_mut(&block_key.shard) {
                shard_cache.blocks.retain(|key, _| key >= &block_key.seqno);
            }
        }
    }

    /// Remove block entries from cache and compact cache
    pub(super) fn _cleanup_blocks_from_cache(&self, blocks_keys: Vec<BlockCacheKey>) -> Result<()> {
        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            "Cleaning up blocks from cache: {}",
            DisplaySlice(&blocks_keys),
        );
        scopeguard::defer! {
            tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                "Blocks cleaned up from cache: {}",
                DisplaySlice(&blocks_keys),
            );
        };
        for block_key in blocks_keys.iter() {
            if block_key.shard.is_masterchain() {
                let mut guard = self.masters.lock();
                if let Some(mc_block_entry) = guard.blocks.remove(&block_key.seqno) {
                    if matches!(
                        mc_block_entry.kind,
                        BlockCacheEntryKind::Received | BlockCacheEntryKind::ReceivedAndCollated
                    ) {
                        Self::update_applied_mc_queue_range(&mut guard, block_key.seqno);
                    }
                }
            } else if let Some(mut shard_cache) = self.shards.get_mut(&block_key.shard) {
                shard_cache.blocks.remove(&block_key.seqno);
            }
        }
        Ok(())
    }
}

pub(super) type BeforeTailIdsResult = BTreeMap<ShardIdent, (Option<BlockId>, Vec<BlockId>)>;
