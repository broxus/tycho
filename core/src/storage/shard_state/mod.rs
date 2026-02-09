use std::fs::File;
use std::io::Cursor;
use std::num::NonZeroU8;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::time::Instant;

use anyhow::{Context, Result};
use bytes::Bytes;
use tycho_block_util::block::*;
use tycho_block_util::dict::split_aug_dict_raw;
use tycho_block_util::state::*;
use tycho_storage::fs::TempFileStorage;
use tycho_storage::kv::StoredValue;
use tycho_types::merkle::MerkleUpdate;
use tycho_types::models::*;
use tycho_types::prelude::*;
use tycho_util::mem::Reclaimer;
use tycho_util::metrics::HistogramGuard;
use tycho_util::{FastDashMap, FastHashMap, FastHashSet};
use weedb::rocksdb;

use self::cell_storage::*;
use self::store_state_raw::StoreStateContext;
use super::{BlockHandle, BlockHandleStorage, BlockStorage, CellsDb, CoreStorageConfig};

mod cell_storage;
mod entries_buffer;
mod store_state_raw;

pub struct ShardStateStorage {
    cells_db: CellsDb,

    block_handle_storage: Arc<BlockHandleStorage>,
    block_storage: Arc<BlockStorage>,
    cell_storage: Arc<CellStorage>,
    temp_file_storage: TempFileStorage,

    gc_lock: Arc<tokio::sync::Mutex<()>>,
    min_ref_mc_state: MinRefMcStateTracker,
    max_new_mc_cell_count: AtomicUsize,
    max_new_sc_cell_count: AtomicUsize,

    accumulated_per_shard: parking_lot::Mutex<FastHashMap<ShardIdent, ShardAccumulator>>,

    shard_states_cache: FastDashMap<ShardIdent, FastHashMap<HashBytes, ShardStateStuff>>,

    shard_split_depth: u8,
    new_cells_threshold: usize,
    store_shard_state_step: NonZeroU8,
}

impl ShardStateStorage {
    // TODO: Replace args with a config.
    pub fn new(
        cells_db: CellsDb,
        block_handle_storage: Arc<BlockHandleStorage>,
        block_storage: Arc<BlockStorage>,
        temp_file_storage: TempFileStorage,
        config: &CoreStorageConfig,
    ) -> Result<Arc<Self>> {
        let cell_storage = CellStorage::new(
            cells_db.clone(),
            config.cells_cache_size,
            config.drop_interval,
        );

        Ok(Arc::new(Self {
            cells_db,
            block_handle_storage,
            block_storage,
            temp_file_storage,
            cell_storage,
            shard_split_depth: config.shard_split_depth,
            new_cells_threshold: config.max_new_cells_threshold,
            store_shard_state_step: config.store_shard_state_step,
            gc_lock: Default::default(),
            min_ref_mc_state: MinRefMcStateTracker::new(),
            max_new_mc_cell_count: AtomicUsize::new(0),
            max_new_sc_cell_count: AtomicUsize::new(0),
            accumulated_per_shard: parking_lot::Mutex::new(FastHashMap::default()),
            shard_states_cache: Default::default(),
        }))
    }

    pub fn metrics(&self) -> ShardStateStorageMetrics {
        ShardStateStorageMetrics {
            max_new_mc_cell_count: self.max_new_mc_cell_count.swap(0, Ordering::AcqRel),
            max_new_sc_cell_count: self.max_new_sc_cell_count.swap(0, Ordering::AcqRel),
        }
    }

    // TODO: implement metrics
    // pub fn cache_metrics(&self) -> CacheStats {
    // self.cell_storage.cache_stats()
    // }

    pub fn min_ref_mc_state(&self) -> &MinRefMcStateTracker {
        &self.min_ref_mc_state
    }

    pub fn cell_storage(&self) -> &Arc<CellStorage> {
        &self.cell_storage
    }

    /// Find mc block id from db snapshot
    pub fn load_mc_block_id(&self, seqno: u32) -> Result<Option<BlockId>> {
        let snapshot = self.cells_db.rocksdb().snapshot();
        self.find_mc_block_id(seqno, &snapshot)
    }

    pub async fn store_state(
        &self,
        handle: &BlockHandle,
        state: &ShardStateStuff,
        hint: StoreStateHint,
    ) -> Result<StoreStateStatus> {
        anyhow::ensure!(
            handle.id() == state.block_id(),
            ShardStateStorageError::BlockHandleIdMismatch {
                expected: state.block_id().as_short_id(),
                actual: handle.id().as_short_id(),
            }
        );

        self.store_state_root(handle, state.root_cell().clone(), hint)
            .await
    }

    pub async fn store_state_root(
        &self,
        handle: &BlockHandle,
        root_cell: Cell,
        hint: StoreStateHint,
    ) -> Result<StoreStateStatus> {
        if handle.has_state() {
            return Ok(StoreStateStatus::Exist);
        }

        let gc_lock = {
            let _hist = HistogramGuard::begin("tycho_storage_cell_gc_lock_store_time_high");
            self.gc_lock.clone().lock_owned().await
        };

        // Double check if the state is already stored
        if handle.has_state() {
            return Ok(StoreStateStatus::Exist);
        }

        let estimated_merkle_update_size = if handle.is_masterchain() {
            hint.new_cell_count
        } else {
            let mut guard = self.accumulated_per_shard.lock();

            // Accumulate current block's cells
            let acc = guard.entry(handle.id().shard).or_default();
            if acc.blocks.insert(handle.id().seqno) {
                acc.new_cells += hint.new_cell_count;
            }

            let force_store = hint.is_top_block == Some(true)
                || handle
                    .id()
                    .seqno
                    .is_multiple_of(self.store_shard_state_step.get() as u32);

            if !force_store && acc.new_cells < self.new_cells_threshold {
                metrics::counter!("tycho_storage_shard_state_skipped").increment(1);

                drop(guard);

                // NOTE: Cache is populated for SKIPPED states
                self.cache_shard_state_root(handle.id(), &root_cell)?;

                self.block_handle_storage
                    .set_has_virtual_shard_state(handle);

                return Ok(StoreStateStatus::Skipped);
            }

            metrics::counter!("tycho_storage_shard_state_stored").increment(1);

            guard
                .remove(&handle.id().shard)
                .map_or(hint.new_cell_count, |acc| acc.new_cells)
        };

        let _hist = HistogramGuard::begin("tycho_storage_state_store_time");

        let block_id = *handle.id();
        let raw_db = self.cells_db.rocksdb().clone();
        let cf = self.cells_db.shard_states.get_unbounded_cf();
        let cell_storage = self.cell_storage.clone();
        let block_handle_storage = self.block_handle_storage.clone();
        let handle = handle.clone();
        let shard_split_depth = self.shard_split_depth;

        // NOTE: `spawn_blocking` is used here instead of `rayon_run` as it is IO-bound task.
        let (new_cell_count, status) = tokio::task::spawn_blocking(move || {
            let root_hash = *root_cell.repr_hash();

            let estimated_update_size_bytes = estimated_merkle_update_size * 192; // p50 cell size in bytes
            let mut batch = rocksdb::WriteBatch::with_capacity_bytes(estimated_update_size_bytes);

            let in_mem_store = HistogramGuard::begin("tycho_storage_cell_in_mem_store_time_high");

            let new_cell_count = if block_id.is_masterchain() {
                cell_storage.store_cell(
                    &mut batch,
                    root_cell.as_ref(),
                    estimated_merkle_update_size,
                )?
            } else {
                let split_at = split_shard_accounts(&root_cell, shard_split_depth)?;

                cell_storage.store_cell_mt(
                    root_cell.as_ref(),
                    &mut batch,
                    split_at,
                    estimated_merkle_update_size,
                )?
            };

            in_mem_store.finish();
            metrics::histogram!("tycho_storage_cell_count").record(new_cell_count as f64);

            batch.put_cf(&cf.bound(), block_id.to_vec(), root_hash.as_slice());

            let hist = HistogramGuard::begin("tycho_storage_state_update_time_high");
            metrics::histogram!("tycho_storage_state_update_size_bytes")
                .record(batch.size_in_bytes() as f64);
            metrics::histogram!("tycho_storage_state_update_size_predicted_bytes")
                .record(estimated_update_size_bytes as f64);

            raw_db.write(batch)?;

            Reclaimer::instance().drop(root_cell);

            hist.finish();

            let updated = block_handle_storage.set_has_shard_state(&handle);

            let status = if updated {
                StoreStateStatus::Stored
            } else {
                StoreStateStatus::Exist
            };

            // NOTE: Ensure that GC lock is dropped only after storing the state.
            drop(gc_lock);

            Ok::<_, anyhow::Error>((new_cell_count, status))
        })
        .await??;

        let count = if block_id.shard.is_masterchain() {
            &self.max_new_mc_cell_count
        } else {
            &self.max_new_sc_cell_count
        };

        count.fetch_max(new_cell_count, Ordering::Release);

        // Clear cache when shard state is stored to ensure clean states on next load
        if !block_id.is_masterchain()
            && status.is_stored()
            && let Some(mut shard_cache) = self.shard_states_cache.get_mut(&block_id.shard)
        {
            let old = std::mem::take(&mut *shard_cache);
            Reclaimer::instance().drop(old);
        }

        Ok(status)
    }

    async fn store_state_inner<R>(&self, block_id: &BlockId, boc: R) -> Result<HashBytes>
    where
        R: std::io::Read + Send + 'static,
    {
        let ctx = StoreStateContext {
            cells_db: self.cells_db.clone(),
            cell_storage: self.cell_storage.clone(),
            temp_file_storage: self.temp_file_storage.clone(),
        };

        let block_id = *block_id;

        let gc_lock = self.gc_lock.clone().lock_owned().await;
        tokio::task::spawn_blocking(move || {
            // NOTE: Ensure that GC lock is captured by the spawned thread.
            let _gc_lock = gc_lock;

            ctx.store(&block_id, boc)
        })
        .await?
    }

    // Stores shard state and returns the hash of its root cell.
    pub async fn store_state_file(&self, block_id: &BlockId, boc: File) -> Result<HashBytes> {
        self.store_state_inner(block_id, boc).await
    }

    pub async fn store_state_bytes(&self, block_id: &BlockId, boc: Bytes) -> Result<HashBytes> {
        let cursor = Cursor::new(boc);
        self.store_state_inner(block_id, cursor).await
    }

    // NOTE: DO NOT try to make a separate `load_state_root` method
    // since the root must be properly tracked, and this tracking requires
    // knowing its `min_ref_mc_seqno` which can only be found out by
    // parsing the state. Creating a "Brief State" struct won't work either
    // because due to model complexity it is going to be error-prone.
    pub async fn load_state_direct(
        &self,
        ref_by_mc_seqno: u32,
        block_id: &BlockId,
    ) -> Result<ShardStateStuff> {
        // NOTE: only for metrics.
        static MAX_KNOWN_EPOCH: AtomicU32 = AtomicU32::new(0);

        let root_hash = self.load_state_root_hash(block_id)?;
        let root = self.cell_storage.load_cell(&root_hash, ref_by_mc_seqno)?;
        let root = Cell::from(root as Arc<_>);

        let max_known_epoch = MAX_KNOWN_EPOCH
            .fetch_max(ref_by_mc_seqno, Ordering::Relaxed)
            .max(ref_by_mc_seqno);
        metrics::gauge!("tycho_storage_state_max_epoch").set(max_known_epoch);

        let shard_state = root.parse::<Box<ShardStateUnsplit>>()?;
        let handle = self.min_ref_mc_state.insert(&shard_state);
        ShardStateStuff::from_state_and_root(block_id, shard_state, root, handle)
    }

    pub async fn load_state(
        &self,
        ref_by_mc_seqno: u32,
        block_id: &BlockId,
    ) -> Result<ShardStateStuff> {
        self.load_state_with_updates_cache(ref_by_mc_seqno, block_id, |_| None)
            .await
    }

    /// Loads state for specified block using optional cache with `CachedStateUpdate` lookups.
    pub async fn load_state_with_updates_cache<F>(
        &self,
        mut ref_by_mc_seqno: u32,
        block_id: &BlockId,
        get_cached_state_update: F,
    ) -> Result<ShardStateStuff>
    where
        F: Fn(&BlockId) -> Option<CachedStateUpdate>,
    {
        // Masterchain states are always stored
        if block_id.is_masterchain() {
            return self.load_state_direct(ref_by_mc_seqno, block_id).await;
        }

        // Walk backwards collecting merkle updates until a base state is found
        let mut chain = Vec::new();
        let mut current_block_id = *block_id;

        let base_state = loop {
            if let Some(state) = self.get_cached_shard_state(&current_block_id) {
                break state;
            }

            if self.contains_state(&current_block_id)? {
                break self
                    .load_state_direct(ref_by_mc_seqno, &current_block_id)
                    .await?;
            }

            match get_cached_state_update(&current_block_id) {
                // Try to get from state updates cache first
                Some(cached) => {
                    ref_by_mc_seqno = ref_by_mc_seqno.min(cached.ref_by_mc_seqno);
                    chain.push((current_block_id, cached.state_update));
                    current_block_id = cached.prev_block_id;
                }
                None => {
                    // Fallback to storage
                    let handle = self
                        .block_handle_storage
                        .load_handle(&current_block_id)
                        .ok_or(ShardStateStorageError::BlockHandleNotFound(
                            block_id.as_short_id(),
                        ))?;

                    // If handle has state flag but state no longer in storage (removed by GC)
                    anyhow::ensure!(
                        !handle.has_state(),
                        "state for block {} was removed by GC",
                        current_block_id.as_short_id(),
                    );

                    ref_by_mc_seqno = ref_by_mc_seqno.min(handle.meta().ref_by_mc_seqno());

                    let block = self.block_storage.load_block_data(&handle).await?;
                    chain.push((current_block_id, block.block().state_update.load()?));

                    let (prev_id, _prev_id_alt) = block.construct_prev_id()?;
                    current_block_id = prev_id;
                }
            }
        };

        // Apply collected merkle updates
        if chain.is_empty() {
            return Ok(base_state);
        }

        let split_at_depth = self.shard_split_depth;
        let state = tokio::task::spawn_blocking(move || {
            let mut state = base_state;
            while let Some((block_id, update)) = chain.pop() {
                let prev_block_id = *state.block_id();
                state = state
                    .par_make_next_state(&block_id, &update, Some(split_at_depth))
                    .with_context(|| {
                        format!("failed to apply merkle of block {block_id} to {prev_block_id}")
                    })?;
            }
            Ok::<_, anyhow::Error>(state)
        })
        .await??;

        Ok(state)
    }

    pub fn load_state_root_hash(&self, block_id: &BlockId) -> Result<HashBytes> {
        let shard_states = &self.cells_db.shard_states;
        let shard_state = shard_states.get(block_id.to_vec())?;
        match shard_state {
            Some(root) => Ok(HashBytes::from_slice(&root[..32])),
            None => {
                anyhow::bail!(ShardStateStorageError::NotFound(block_id.as_short_id()))
            }
        }
    }

    pub fn contains_state(&self, block_id: &BlockId) -> Result<bool> {
        let shard_states = &self.cells_db.shard_states;
        Ok(shard_states.get(block_id.to_vec())?.is_some())
    }

    #[tracing::instrument(skip(self))]
    pub async fn remove_outdated_states(&self, mc_seqno: u32) -> Result<()> {
        // Compute recent block ids for the specified masterchain seqno
        let Some(top_blocks) = self.compute_recent_blocks(mc_seqno).await? else {
            tracing::warn!("recent blocks edge not found");
            return Ok(());
        };

        let target_block_id = top_blocks.mc_block;
        tracing::info!(%target_block_id, "started states GC");

        let started_at = Instant::now();
        let block_handle_storage = self.block_handle_storage.clone();
        let cell_storage = self.cell_storage.clone();
        let cells_db = self.cells_db.clone();
        let gc_lock = self.gc_lock.clone();
        let shard_split_depth = self.shard_split_depth;

        let (removed_states, removed_cells) = tokio::task::spawn_blocking(move || {
            let raw = cells_db.rocksdb();

            // Manually get required column factory and r/w options
            let snapshot = raw.snapshot();
            let shard_states_cf = cells_db.shard_states.get_unbounded_cf();
            let mut states_read_options = cells_db.shard_states.new_read_config();
            states_read_options.set_snapshot(&snapshot);

            let mut alloc = bumpalo_herd::Herd::new();

            // Create iterator
            let mut iter = raw.raw_iterator_cf_opt(&shard_states_cf.bound(), states_read_options);
            iter.seek_to_first();

            // Iterate all states and remove outdated
            let mut removed_states = 0usize;
            let mut removed_cells = 0usize;

            loop {
                let _hist = HistogramGuard::begin("tycho_storage_state_gc_time_high");
                let (key, value) = match iter.item() {
                    Some(item) => item,
                    None => match iter.status() {
                        Ok(()) => break,
                        Err(e) => return Err(e.into()),
                    },
                };

                let block_id = BlockId::from_slice(key);
                let root_hash = HashBytes::from_slice(&value[0..32]);

                // Skip blocks from zero state and top blocks.
                // NOTE: We intentionally don't skip hardforked zerostates (seqno > 0),
                // because we don't really need to keep them. For proof checker we
                // use zerostate proof which is stored separately, and for serving the
                // state we use a persistent state (where we don't remove these states).
                if block_id.seqno == 0
                    || top_blocks.contains_shard_seqno(&block_id.shard, block_id.seqno)
                {
                    iter.next();
                    continue;
                }

                // skip block marked by SKIP_GC flag
                if let Some(handle) = block_handle_storage.load_handle(&block_id)
                    && handle.skip_states_gc()
                {
                    tracing::debug!(
                        block_id = %block_id,
                        "skipping states GC since it flagged by SKIP_STATES_GC"
                    );
                    iter.next();
                    continue;
                }

                alloc.reset();

                let guard = {
                    let _h = HistogramGuard::begin("tycho_storage_cell_gc_lock_remove_time_high");
                    gc_lock.blocking_lock()
                };

                let in_mem_remove =
                    HistogramGuard::begin("tycho_storage_cell_in_mem_remove_time_high");

                let (total, mut batch) = if block_id.is_masterchain() {
                    cell_storage.remove_cell(alloc.get().as_bump(), &root_hash)?
                } else {
                    // NOTE: We use epoch `0` here so that cells of old states
                    // will not be used by recent loads.
                    let root_cell = Cell::from(cell_storage.load_cell(&root_hash, 0)? as Arc<_>);

                    let split_at = split_shard_accounts(&root_cell, shard_split_depth)?
                        .into_keys()
                        .collect::<FastHashSet<HashBytes>>();
                    cell_storage.remove_cell_mt(&alloc, &root_hash, split_at)?
                };

                in_mem_remove.finish();

                batch.delete_cf(&cells_db.shard_states.get_unbounded_cf().bound(), key);
                cells_db
                    .raw()
                    .rocksdb()
                    .write_opt(batch, cells_db.cells.write_config())?;

                // NOTE: Ensure that guard is dropped only after writing the batch.
                drop(guard);

                removed_cells += total;
                tracing::debug!(removed_cells = total, %block_id);

                removed_states += 1;
                iter.next();

                metrics::counter!("tycho_storage_state_gc_count").increment(1);
                metrics::counter!("tycho_storage_state_gc_cells_count").increment(1);
                if block_id.is_masterchain() {
                    metrics::gauge!("tycho_gc_states_seqno").set(block_id.seqno as f64);
                }
                tracing::debug!(removed_states, removed_cells, %block_id, "removed state");
            }

            Ok::<_, anyhow::Error>((removed_states, removed_cells))
        })
        .await??;

        // Done
        tracing::info!(
            removed_states,
            removed_cells,
            block_id = %target_block_id,
            elapsed_sec = started_at.elapsed().as_secs_f64(),
            "finished states GC",
        );
        Ok(())
    }

    /// Searches for an edge with the least referenced masterchain block
    ///
    /// Returns `None` if all states are recent enough
    pub async fn compute_recent_blocks(&self, mut mc_seqno: u32) -> Result<Option<TopBlocks>> {
        // 0. Adjust masterchain seqno with minimal referenced masterchain state
        if let Some(min_ref_mc_seqno) = self.min_ref_mc_state.seqno()
            && min_ref_mc_seqno < mc_seqno
        {
            mc_seqno = min_ref_mc_seqno;
        }

        let snapshot = self.cells_db.rocksdb().snapshot();

        // 1. Find target block

        // Find block id using states table
        let mc_block_id = match self
            .find_mc_block_id(mc_seqno, &snapshot)
            .context("Failed to find block id by seqno")?
        {
            Some(block_id) => block_id,
            None => return Ok(None),
        };

        // Find block handle
        let handle = match self.block_handle_storage.load_handle(&mc_block_id) {
            Some(handle) if handle.has_data() => handle,
            // Skip blocks without handle or data
            _ => return Ok(None),
        };

        // 2. Find minimal referenced masterchain block from the target block

        let block_data = self.block_storage.load_block_data(&handle).await?;
        let block_info = block_data
            .load_info()
            .context("Failed to read target block info")?;

        // Find full min masterchain reference id
        let min_ref_mc_seqno = block_info.min_ref_mc_seqno;
        let min_ref_block_id = match self.find_mc_block_id(min_ref_mc_seqno, &snapshot)? {
            Some(block_id) => block_id,
            None => return Ok(None),
        };

        // Find block handle
        let min_ref_block_handle = match self.block_handle_storage.load_handle(&min_ref_block_id) {
            Some(handle) if handle.has_data() => handle,
            // Skip blocks without handle or data
            _ => return Ok(None),
        };

        // Compute `TopBlocks` from block data
        self.block_storage
            .load_block_data(&min_ref_block_handle)
            .await
            .and_then(|block_data| TopBlocks::from_mc_block(&block_data))
            .map(Some)
    }

    fn find_mc_block_id(
        &self,
        mc_seqno: u32,
        snapshot: &rocksdb::Snapshot<'_>,
    ) -> Result<Option<BlockId>> {
        let shard_states = &self.cells_db.shard_states;

        let mut bound = BlockId {
            shard: ShardIdent::MASTERCHAIN,
            seqno: mc_seqno,
            root_hash: HashBytes::ZERO,
            file_hash: HashBytes::ZERO,
        };

        let mut readopts = shard_states.new_read_config();
        readopts.set_snapshot(snapshot);
        readopts.set_iterate_lower_bound(bound.to_vec().as_slice());
        bound.seqno += 1;
        readopts.set_iterate_upper_bound(bound.to_vec().as_slice());

        let mut iter = self
            .cells_db
            .rocksdb()
            .raw_iterator_cf_opt(&shard_states.cf(), readopts);
        iter.seek_to_first();

        Ok(iter.key().map(BlockId::from_slice))
    }

    fn cache_shard_state_root(&self, block_id: &BlockId, state_root: &Cell) -> Result<()> {
        if block_id.is_masterchain() {
            return Ok(());
        }

        let shard_state = state_root.parse::<Box<ShardStateUnsplit>>()?;
        let handle = self.min_ref_mc_state.insert(&shard_state);

        let state = ShardStateStuff::from_state_and_root(
            block_id,
            shard_state,
            state_root.clone(),
            handle,
        )?;

        self.shard_states_cache
            .entry(block_id.shard)
            .or_default()
            .insert(block_id.root_hash, state);

        Ok(())
    }

    fn get_cached_shard_state(&self, block_id: &BlockId) -> Option<ShardStateStuff> {
        self.shard_states_cache
            .get(&block_id.shard)
            .and_then(|states| states.get(&block_id.root_hash).cloned())
    }
}

#[derive(Default, Debug, Clone, Copy)]
pub struct StoreStateHint {
    pub block_data_size: usize,
    pub new_cell_count: usize,
    pub is_top_block: Option<bool>,
}

impl StoreStateHint {
    #[allow(dead_code)]
    fn estimate_cell_count(&self) -> usize {
        // y = 3889.9821 + 14.7480 × √x
        // R-squared: 0.7035
        ((3889.9821 + 14.7480 * (self.block_data_size as f64).sqrt()) as usize).next_power_of_two()
    }
}

#[derive(Debug, Copy, Clone)]
pub struct ShardStateStorageMetrics {
    pub max_new_mc_cell_count: usize,
    pub max_new_sc_cell_count: usize,
}

#[derive(thiserror::Error, Debug)]
pub enum ShardStateStorageError {
    #[error("Shard state not found for block: {0}")]
    NotFound(BlockIdShort),
    #[error("Block handle id mismatch: expected {expected}, got {actual}")]
    BlockHandleIdMismatch {
        expected: BlockIdShort,
        actual: BlockIdShort,
    },
    #[error("Shard handle not found for block: {0}")]
    BlockHandleNotFound(BlockIdShort),
}

pub fn split_shard_accounts(
    root_cell: impl AsRef<DynCell>,
    split_depth: u8,
) -> Result<FastHashMap<HashBytes, Cell>> {
    // Cell#0 - processed_upto
    // Cell#1 - accounts
    let shard_accounts = root_cell
        .as_ref()
        .reference_cloned(1)
        .context("invalid shard state")?
        .parse::<ShardAccounts>()
        .context("failed to load shard accounts")?;

    split_aug_dict_raw(shard_accounts, split_depth).context("failed to split shard accounts")
}

pub struct CachedStateUpdate {
    pub prev_block_id: BlockId,
    pub ref_by_mc_seqno: u32,
    pub state_update: MerkleUpdate,
}

pub enum StoreStateStatus {
    Stored,
    Skipped,
    Exist,
}

impl StoreStateStatus {
    pub fn is_stored(&self) -> bool {
        matches!(self, Self::Stored)
    }
}

#[derive(Default)]
struct ShardAccumulator {
    new_cells: usize,
    blocks: FastHashSet<u32>,
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use tycho_block_util::archive::WithArchiveData;
    use tycho_block_util::block::BlockStuff;
    use tycho_block_util::state::ShardStateStuff;
    use tycho_storage::StorageContext;
    use tycho_types::boc::BocRepr;
    use tycho_types::cell::{CellBuilder, Lazy};
    use tycho_types::models::{
        BlockExtra, BlockId, BlockInfo, McBlockExtra, ShardHashes, ShardIdent, ShardStateUnsplit,
    };

    use crate::storage::{CoreStorage, CoreStorageConfig, NewBlockMeta};

    #[tokio::test]
    async fn states_gc_skip_lifecycle() -> Result<()> {
        let (ctx, _tmp_dir) = StorageContext::new_temp().await?;
        let storage = CoreStorage::open(ctx, CoreStorageConfig::new_potato()).await?;

        let handles = storage.block_handle_storage();
        let blocks = storage.block_storage();
        let states = storage.shard_state_storage();

        let target = 10u32;
        let prev = target - 1;

        let top = BlockStuff::new_with(ShardIdent::MASTERCHAIN, target, |block| {
            let info = BlockInfo {
                shard: ShardIdent::MASTERCHAIN,
                seqno: target,
                min_ref_mc_seqno: target,
                ..Default::default()
            };
            block.info = Lazy::new(&info).unwrap();

            let extra = BlockExtra {
                custom: Some(
                    Lazy::new(&McBlockExtra {
                        shards: ShardHashes::default(),
                        ..Default::default()
                    })
                    .unwrap(),
                ),
                ..Default::default()
            };
            block.extra = Lazy::new(&extra).unwrap();
        });
        let top_id = *top.id();

        let data = BocRepr::encode_rayon(top.as_ref()).unwrap();
        let top = WithArchiveData::new(top, data);

        let stored = blocks
            .store_block_data(&top, &top.archive_data, NewBlockMeta {
                is_key_block: false,
                gen_utime: 0,
                ref_by_mc_seqno: target,
            })
            .await?;
        let handle = stored.handle;

        let prev_id = *BlockStuff::new_empty(ShardIdent::MASTERCHAIN, prev).id();

        let make_state = |id: BlockId| -> Result<ShardStateStuff> {
            let state = ShardStateUnsplit {
                shard_ident: id.shard,
                seqno: id.seqno,
                min_ref_mc_seqno: target,
                ..Default::default()
            };

            let root = CellBuilder::build_from(&state)?;
            let handle = states.min_ref_mc_state().insert_untracked();
            ShardStateStuff::from_state_and_root(&id, Box::new(state), root, handle)
        };

        let top_state = make_state(top_id)?;
        states
            .store_state(&handle, &top_state, Default::default())
            .await?;

        let prev_state = make_state(prev_id)?;
        let (handle, _) = handles.create_or_load_handle(&prev_id, NewBlockMeta {
            is_key_block: false,
            gen_utime: 0,
            ref_by_mc_seqno: prev,
        });
        states
            .store_state(&handle, &prev_state, Default::default())
            .await?;

        handles.set_skip_states_gc(&handle);
        assert!(handle.skip_states_gc());

        states.remove_outdated_states(target).await?;
        assert!(states.contains_state(&prev_id)?);
        assert!(states.contains_state(&top_id)?);

        handles.set_skip_states_gc_finished(&handle);
        assert!(!handle.skip_states_gc());

        states.remove_outdated_states(target).await?;
        assert!(!states.contains_state(&prev_id)?);
        assert!(states.contains_state(&top_id)?);

        Ok(())
    }
}
