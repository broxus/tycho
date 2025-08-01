use std::fs::File;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use anyhow::{Context, Result};
use bytesize::ByteSize;
use tycho_block_util::block::*;
use tycho_block_util::dict::split_aug_dict_raw;
use tycho_block_util::state::*;
use tycho_storage::fs::TempFileStorage;
use tycho_storage::kv::StoredValue;
use tycho_types::models::*;
use tycho_types::prelude::*;
use tycho_util::metrics::HistogramGuard;
use tycho_util::{FastHashMap, FastHashSet};
use weedb::rocksdb;

use self::cell_storage::*;
use self::store_state_raw::StoreStateContext;
use super::{BlockFlags, BlockHandle, BlockHandleStorage, BlockStorage, CoreDb};

mod cell_storage;
mod entries_buffer;
mod store_state_raw;

pub struct ShardStateStorage {
    db: CoreDb,

    block_handle_storage: Arc<BlockHandleStorage>,
    block_storage: Arc<BlockStorage>,
    cell_storage: Arc<CellStorage>,
    temp_file_storage: TempFileStorage,

    gc_lock: tokio::sync::Mutex<()>,
    min_ref_mc_state: MinRefMcStateTracker,
    max_new_mc_cell_count: AtomicUsize,
    max_new_sc_cell_count: AtomicUsize,

    accounts_split_depth: u8,
}

impl ShardStateStorage {
    pub fn new(
        db: CoreDb,
        block_handle_storage: Arc<BlockHandleStorage>,
        block_storage: Arc<BlockStorage>,
        temp_file_storage: TempFileStorage,
        cache_size_bytes: ByteSize,
    ) -> Result<Arc<Self>> {
        let cell_storage = CellStorage::new(db.clone(), cache_size_bytes);

        Ok(Arc::new(Self {
            db,
            block_handle_storage,
            block_storage,
            temp_file_storage,
            cell_storage,
            gc_lock: Default::default(),
            min_ref_mc_state: MinRefMcStateTracker::new(),
            max_new_mc_cell_count: AtomicUsize::new(0),
            max_new_sc_cell_count: AtomicUsize::new(0),
            accounts_split_depth: 4,
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

    pub async fn store_state(
        &self,
        handle: &BlockHandle,
        state: &ShardStateStuff,
        hint: StoreStateHint,
    ) -> Result<bool> {
        if handle.id() != state.block_id() {
            return Err(ShardStateStorageError::BlockHandleIdMismatch.into());
        }

        self.store_state_root(handle, state.root_cell().clone(), hint)
            .await
    }

    pub async fn store_state_root(
        &self,
        handle: &BlockHandle,
        root_cell: Cell,
        hint: StoreStateHint,
    ) -> Result<bool> {
        if handle.has_state() {
            return Ok(false);
        }

        let _gc_lock = {
            let _hist = HistogramGuard::begin("tycho_storage_cell_gc_lock_store_time_high");
            self.gc_lock.lock().await
        };

        // Double check if the state is already stored
        if handle.has_state() {
            return Ok(false);
        }
        let _hist = HistogramGuard::begin("tycho_storage_state_store_time");

        let block_id = *handle.id();
        let raw_db = self.db.rocksdb().clone();
        let cf = self.db.shard_states.get_unbounded_cf();
        let cell_storage = self.cell_storage.clone();
        let block_handle_storage = self.block_handle_storage.clone();
        let handle = handle.clone();
        let accounts_split_depth = self.accounts_split_depth;

        // NOTE: `spawn_blocking` is used here instead of `rayon_run` as it is IO-bound task.
        let (new_cell_count, updated) = tokio::task::spawn_blocking(move || {
            let root_hash = *root_cell.repr_hash();
            let estimated_merkle_update_size = hint.estimate_cell_count();

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
                let split_at = split_shard_accounts(&root_cell, accounts_split_depth)?;

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

            drop(root_cell);

            hist.finish();

            let updated = handle.meta().add_flags(BlockFlags::HAS_STATE);
            if updated {
                block_handle_storage.store_handle(&handle, false);
            }

            Ok::<_, anyhow::Error>((new_cell_count, updated))
        })
        .await??;

        let count = if block_id.shard.is_masterchain() {
            &self.max_new_mc_cell_count
        } else {
            &self.max_new_sc_cell_count
        };

        count.fetch_max(new_cell_count, Ordering::Release);

        Ok(updated)
    }

    pub async fn store_state_file(&self, block_id: &BlockId, boc: File) -> Result<ShardStateStuff> {
        let ctx = StoreStateContext {
            db: self.db.clone(),
            cell_storage: self.cell_storage.clone(),
            temp_file_storage: self.temp_file_storage.clone(),
            min_ref_mc_state: self.min_ref_mc_state.clone(),
        };

        let block_id = *block_id;

        let _gc_lock = self.gc_lock.lock().await;
        tokio::task::spawn_blocking(move || ctx.store(&block_id, boc)).await?
    }

    pub async fn load_state(&self, block_id: &BlockId) -> Result<ShardStateStuff> {
        let cell_id = self.load_state_root(block_id)?;
        let cell = self.cell_storage.load_cell(cell_id)?;

        ShardStateStuff::from_root(block_id, Cell::from(cell as Arc<_>), &self.min_ref_mc_state)
    }

    #[tracing::instrument(skip(self))]
    pub async fn remove_outdated_states(&self, mc_seqno: u32) -> Result<()> {
        // Compute recent block ids for the specified masterchain seqno
        let Some(top_blocks) = self.compute_recent_blocks(mc_seqno).await? else {
            tracing::warn!("recent blocks edge not found");
            return Ok(());
        };

        tracing::info!(
            target_block_id = %top_blocks.mc_block,
            "started states GC",
        );
        let started_at = Instant::now();

        let raw = self.db.rocksdb();

        // Manually get required column factory and r/w options
        let snapshot = raw.snapshot();
        let shard_states_cf = self.db.shard_states.get_unbounded_cf();
        let mut states_read_options = self.db.shard_states.new_read_config();
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
            let root_hash = HashBytes::from_slice(value);

            // Skip blocks from zero state and top blocks
            if block_id.seqno == 0
                || top_blocks.contains_shard_seqno(&block_id.shard, block_id.seqno)
            {
                iter.next();
                continue;
            }

            alloc.reset();

            let guard = {
                let _h = HistogramGuard::begin("tycho_storage_cell_gc_lock_remove_time_high");
                self.gc_lock.lock().await
            };

            let db = self.db.clone();
            let cell_storage = self.cell_storage.clone();
            let key = key.to_vec();
            let accounts_split_depth = self.accounts_split_depth;
            let (total, inner_alloc) = tokio::task::spawn_blocking(move || {
                let in_mem_remove =
                    HistogramGuard::begin("tycho_storage_cell_in_mem_remove_time_high");

                let (stats, mut batch) = if block_id.is_masterchain() {
                    cell_storage.remove_cell(alloc.get().as_bump(), &root_hash)?
                } else {
                    let root_cell = Cell::from(cell_storage.load_cell(root_hash)? as Arc<_>);

                    let split_at = split_shard_accounts(&root_cell, accounts_split_depth)?
                        .into_keys()
                        .collect::<FastHashSet<HashBytes>>();
                    cell_storage.remove_cell_mt(&alloc, &root_hash, split_at)?
                };

                in_mem_remove.finish();

                batch.delete_cf(&db.shard_states.get_unbounded_cf().bound(), key);
                db.raw()
                    .rocksdb()
                    .write_opt(batch, db.cells.write_config())?;

                Ok::<_, anyhow::Error>((stats, alloc))
            })
            .await??;

            drop(guard);

            removed_cells += total;
            alloc = inner_alloc; // Reuse allocation without passing alloc by ref

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

        // Done
        tracing::info!(
            removed_states,
            removed_cells,
            block_id = %top_blocks.mc_block,
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
        if let Some(min_ref_mc_seqno) = self.min_ref_mc_state.seqno() {
            if min_ref_mc_seqno < mc_seqno {
                mc_seqno = min_ref_mc_seqno;
            }
        }

        let snapshot = self.db.rocksdb().snapshot();

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

    pub fn load_state_root(&self, block_id: &BlockId) -> Result<HashBytes> {
        let shard_states = &self.db.shard_states;
        let shard_state = shard_states.get(block_id.to_vec())?;
        match shard_state {
            Some(root) => Ok(HashBytes::from_slice(&root[..32])),
            None => Err(ShardStateStorageError::NotFound.into()),
        }
    }

    fn find_mc_block_id(
        &self,
        mc_seqno: u32,
        snapshot: &rocksdb::Snapshot<'_>,
    ) -> Result<Option<BlockId>> {
        let shard_states = &self.db.shard_states;

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
            .db
            .rocksdb()
            .raw_iterator_cf_opt(&shard_states.cf(), readopts);
        iter.seek_to_first();

        Ok(iter.key().map(BlockId::from_slice))
    }
}

#[derive(Default, Debug, Clone, Copy)]
pub struct StoreStateHint {
    pub block_data_size: Option<usize>,
}

impl StoreStateHint {
    fn estimate_cell_count(&self) -> usize {
        const MIN_BLOCK_SIZE: usize = 4 << 10; // 4 KB

        let block_data_size = self.block_data_size.unwrap_or(MIN_BLOCK_SIZE);

        // y = 3889.9821 + 14.7480 × √x
        // R-squared: 0.7035
        ((3889.9821 + 14.7480 * (block_data_size as f64).sqrt()) as usize).next_power_of_two()
    }
}

#[derive(Debug, Copy, Clone)]
pub struct ShardStateStorageMetrics {
    pub max_new_mc_cell_count: usize,
    pub max_new_sc_cell_count: usize,
}

#[derive(thiserror::Error, Debug)]
pub enum ShardStateStorageError {
    #[error("Not found")]
    NotFound,
    #[error("Block handle id mismatch")]
    BlockHandleIdMismatch,
}

fn split_shard_accounts(
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
