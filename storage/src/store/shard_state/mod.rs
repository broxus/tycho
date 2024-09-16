use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use everscale_types::models::*;
use everscale_types::prelude::{Cell, HashBytes};
use tycho_block_util::block::*;
use tycho_block_util::state::*;
use tycho_util::metrics::HistogramGuard;
use weedb::rocksdb;

use self::cell_storage::*;
use self::store_state_raw::StoreStateRaw;
use crate::db::*;
use crate::store::{BlockFlags, BlockHandle, BlockHandleStorage, BlockStorage};
use crate::util::*;

mod cell_storage;
mod entries_buffer;
mod shard_state_reader;
pub mod store_state_raw;

const DOWNLOADS_DIR: &str = "downloads";

pub struct ShardStateStorage {
    db: BaseDb,
    downloads_dir: FileDb,

    block_handle_storage: Arc<BlockHandleStorage>,
    block_storage: Arc<BlockStorage>,
    cell_storage: Arc<CellStorage>,

    gc_lock: tokio::sync::Mutex<()>,
    min_ref_mc_state: MinRefMcStateTracker,
    max_new_mc_cell_count: AtomicUsize,
    max_new_sc_cell_count: AtomicUsize,
}

impl ShardStateStorage {
    pub fn new(
        db: BaseDb,
        files_dir: &FileDb,
        block_handle_storage: Arc<BlockHandleStorage>,
        block_storage: Arc<BlockStorage>,
        cache_size_bytes: u64,
    ) -> Result<Self> {
        let downloads_dir = files_dir.create_subdir(DOWNLOADS_DIR)?;

        let cell_storage = CellStorage::new(db.clone(), cache_size_bytes);

        Ok(Self {
            db,
            block_handle_storage,
            block_storage,
            cell_storage,
            downloads_dir,
            gc_lock: Default::default(),
            min_ref_mc_state: Default::default(),
            max_new_mc_cell_count: AtomicUsize::new(0),
            max_new_sc_cell_count: AtomicUsize::new(0),
        })
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

    pub async fn store_state(&self, handle: &BlockHandle, state: &ShardStateStuff) -> Result<bool> {
        if handle.id() != state.block_id() {
            return Err(ShardStateStorageError::BlockHandleIdMismatch.into());
        }

        self.store_state_root(handle, state.root_cell().clone())
            .await
    }

    pub async fn store_state_root(&self, handle: &BlockHandle, root_cell: Cell) -> Result<bool> {
        if handle.has_state() {
            return Ok(false);
        }
        let _gc_lock = self.gc_lock.lock().await;

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

        // NOTE: `spawn_blocking` is used here instead of `rayon_run` as it is IO-bound task.
        let (new_cell_count, updated) = tokio::task::spawn_blocking(move || {
            let root_hash = *root_cell.repr_hash();

            let mut batch = rocksdb::WriteBatch::default();

            let new_cell_count = cell_storage.store_cell(&mut batch, root_cell)?;
            metrics::histogram!("tycho_storage_cell_count").record(new_cell_count as f64);

            batch.put_cf(&cf.bound(), block_id.to_vec(), root_hash.as_slice());

            let hist = HistogramGuard::begin("tycho_storage_state_update_time");
            raw_db.write(batch)?;
            hist.finish();

            let updated = handle.meta().add_flags(BlockFlags::HAS_STATE);
            if updated {
                block_handle_storage.store_handle(&handle);
            }

            // Ensure that pending operation guard is dropped after the batch is written
            // drop(pending_op);
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

    pub fn begin_store_state_raw(&self, block_id: &BlockId) -> Result<StoreStateRaw> {
        StoreStateRaw::new(
            block_id,
            &self.downloads_dir,
            self.db.clone(),
            self.cell_storage.clone(),
            self.min_ref_mc_state.clone(),
        )
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

        let cells_write_options = self.db.cells.write_config();

        let mut alloc = bumpalo::Bump::new();

        // Create iterator
        let mut iter = raw.raw_iterator_cf_opt(&shard_states_cf.bound(), states_read_options);
        iter.seek_to_first();

        // Iterate all states and remove outdated
        let mut removed_states = 0usize;
        let mut removed_cells = 0usize;
        loop {
            let (key, value) = match iter.item() {
                Some(item) => item,
                None => match iter.status() {
                    Ok(()) => break,
                    Err(e) => return Err(e.into()),
                },
            };

            let block_id = BlockId::from_slice(key);
            let root_hash = HashBytes::wrap(value.try_into().expect("invalid value"));

            // Skip blocks from zero state and top blocks
            if block_id.seqno == 0
                || top_blocks.contains_shard_seqno(&block_id.shard, block_id.seqno)
            {
                iter.next();
                continue;
            }

            alloc.reset();
            let mut batch = weedb::rocksdb::WriteBatch::default();
            {
                let _guard = self.gc_lock.lock().await;
                let total = self
                    .cell_storage
                    .remove_cell(&mut batch, &alloc, root_hash)?;
                batch.delete_cf(&shard_states_cf.bound(), key);
                raw.write_opt(batch, cells_write_options)?;

                // Ensure that pending operation guard is dropped after the batch is written
                // drop(pending_op);

                removed_cells += total;
                tracing::debug!(removed_cells = total, %block_id);
            }

            removed_states += 1;
            iter.next();
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

    fn load_state_root(&self, block_id: &BlockId) -> Result<HashBytes> {
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

#[derive(Debug, Copy, Clone)]
pub struct ShardStateStorageMetrics {
    pub max_new_mc_cell_count: usize,
    pub max_new_sc_cell_count: usize,
}

#[derive(thiserror::Error, Debug)]
enum ShardStateStorageError {
    #[error("Not found")]
    NotFound,
    #[error("Block handle id mismatch")]
    BlockHandleIdMismatch,
}
