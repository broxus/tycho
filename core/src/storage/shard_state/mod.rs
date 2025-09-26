use std::fs::File;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::time::Instant;

use anyhow::{Context, Result};
use bytesize::ByteSize;
use tycho_block_util::block::*;
use tycho_block_util::dict::split_aug_dict_raw_by_shards;
use tycho_block_util::state::*;
use tycho_storage::fs::TempFileStorage;
use tycho_storage::kv::StoredValue;
use tycho_types::models::*;
use tycho_types::prelude::*;
use tycho_util::mem::Reclaimer;
use tycho_util::metrics::HistogramGuard;
use tycho_util::{FastHashMap, FastHashSet};
use weedb::rocksdb;

use self::cell_storage::*;
use self::store_state_raw::StoreStateContext;
use super::{BlockFlags, BlockHandle, BlockHandleStorage, BlockStorage, CellsDb, CellsPartDb};

mod cell_storage;
mod entries_buffer;
mod store_state_raw;

use bumpalo::Bump;

use self::cell_storage::{CellStorage, CellStorageError};

pub type StoragePartsMap = FastHashMap<ShardIdent, Arc<dyn ShardStateStoragePart>>;

#[derive(Debug, Clone)]
pub struct SplitAccountEntry {
    pub shard: ShardIdent,
    pub cell: Cell,
}

pub trait ShardStateStoragePart: Send + Sync {
    fn shard(&self) -> ShardIdent;
    fn cell_storage(&self) -> &Arc<CellStorage>;
    fn store_accounts_subtree(
        &self,
        cell: &DynCell,
        estimated_cell_count: usize,
    ) -> Result<usize, CellStorageError>;
    fn remove_accounts_subtree(&self, root_hash: &HashBytes) -> Result<usize, CellStorageError>;
}

pub struct ShardStateStoragePartImpl {
    shard: ShardIdent,
    cells_db: CellsPartDb,
    cell_storage: Arc<CellStorage>,
}

impl ShardStateStoragePart for ShardStateStoragePartImpl {
    fn shard(&self) -> ShardIdent {
        self.shard
    }

    fn cell_storage(&self) -> &Arc<CellStorage> {
        &self.cell_storage
    }

    fn store_accounts_subtree(
        &self,
        cell: &DynCell,
        estimated_cell_count: usize,
    ) -> Result<usize, CellStorageError> {
        self.store_accounts_subtree_impl(cell, estimated_cell_count)
    }

    fn remove_accounts_subtree(&self, root_hash: &HashBytes) -> Result<usize, CellStorageError> {
        self.remove_accounts_subtree_impl(root_hash)
    }
}

impl ShardStateStoragePartImpl {
    pub fn new(
        shard: ShardIdent,
        cells_db: CellsPartDb,
        cache_size_bytes: ByteSize,
        drop_interval: u32,
        part_split_depth: u8,
    ) -> Self {
        let cell_storage = CellStorage::new_for_shard(
            cells_db.clone(),
            cache_size_bytes,
            drop_interval,
            shard,
            part_split_depth,
        );
        Self {
            shard,
            cells_db,
            cell_storage,
        }
    }

    fn store_accounts_subtree_impl(
        &self,
        cell: &DynCell,
        estimated_cell_count: usize,
    ) -> Result<usize, CellStorageError> {
        let mut batch = rocksdb::WriteBatch::default();
        let new_cells = self
            .cell_storage
            .store_cell(&mut batch, cell, estimated_cell_count)?;
        self.cells_db.rocksdb().write(batch)?;
        Ok(new_cells)
    }

    fn remove_accounts_subtree_impl(
        &self,
        root_hash: &HashBytes,
    ) -> Result<usize, CellStorageError> {
        let bump = Bump::new();
        let (removed, batch) = self.cell_storage.remove_cell(&bump, root_hash)?;
        self.cells_db.rocksdb().write(batch)?;
        Ok(removed)
    }
}

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

    accounts_split_depth: u8,

    /// The target split depth of shard accounts cells on partitions.
    /// E.g. `3` -> `2^2=8` partitions
    part_split_depth: u8,
    storage_parts: Arc<StoragePartsMap>,
}

impl ShardStateStorage {
    // TODO: Replace args with a config.
    pub fn new(
        cells_db: CellsDb,
        block_handle_storage: Arc<BlockHandleStorage>,
        block_storage: Arc<BlockStorage>,
        temp_file_storage: TempFileStorage,
        cache_size_bytes: ByteSize,
        drop_interval: u32,
        part_split_depth: u8,
        storage_parts: Arc<StoragePartsMap>,
    ) -> Result<Arc<Self>> {
        let expected_parts_count = if part_split_depth == 0 {
            0
        } else {
            1usize << part_split_depth
        };
        assert_eq!(storage_parts.len(), expected_parts_count);

        let cell_storage = CellStorage::new(
            cells_db.clone(),
            cache_size_bytes,
            drop_interval,
            part_split_depth,
            Some(storage_parts.clone()),
        );

        Ok(Arc::new(Self {
            cells_db,
            block_handle_storage,
            block_storage,
            temp_file_storage,
            cell_storage,
            gc_lock: Default::default(),
            min_ref_mc_state: MinRefMcStateTracker::new(),
            max_new_mc_cell_count: AtomicUsize::new(0),
            max_new_sc_cell_count: AtomicUsize::new(0),
            accounts_split_depth: 4,
            part_split_depth,
            storage_parts,
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

    #[tracing::instrument(skip_all, fields(block_id = %handle.id().as_short_id()))]
    pub async fn store_state_root(
        &self,
        handle: &BlockHandle,
        root_cell: Cell,
        hint: StoreStateHint,
    ) -> Result<bool> {
        if handle.has_state() {
            return Ok(false);
        }

        let gc_lock = {
            let _hist = HistogramGuard::begin("tycho_storage_cell_gc_lock_store_time_high");
            self.gc_lock.clone().lock_owned().await
        };

        // Double check if the state is already stored
        if handle.has_state() {
            return Ok(false);
        }
        let _hist = HistogramGuard::begin("tycho_storage_state_store_time");

        let block_id = *handle.id();
        let raw_db = self.cells_db.rocksdb().clone();
        let cf = self.cells_db.shard_states.get_unbounded_cf();
        let cell_storage = self.cell_storage.clone();
        let block_handle_storage = self.block_handle_storage.clone();
        let handle = handle.clone();

        // split accounts to store in partitions
        // otherwise split to store in parallel
        let accounts_split_depth = if self.part_split_depth > 0 {
            self.part_split_depth
        } else {
            self.accounts_split_depth
        };

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
                let split_at =
                    split_shard_accounts(&block_id.shard, &root_cell, accounts_split_depth)?;

                tracing::debug!(accounts_split_depth, ?split_at);

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

            let updated = handle.meta().add_flags(BlockFlags::HAS_STATE);
            if updated {
                block_handle_storage.store_handle(&handle, false);
            }

            // NOTE: Ensure that GC lock is dropped only after storing the state.
            drop(gc_lock);

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

    // Stores shard state and returns the hash of its root cell.
    pub async fn store_state_file(&self, block_id: &BlockId, boc: File) -> Result<HashBytes> {
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

    // NOTE: DO NOT try to make a separate `load_state_root` method
    // since the root must be properly tracked, and this tracking requires
    // knowing its `min_ref_mc_seqno` which can only be found out by
    // parsing the state. Creating a "Brief State" struct won't work either
    // because due to model complexity it is going to be error-prone.
    #[tracing::instrument(skip_all, fields(block_id = %block_id.as_short_id()))]
    pub async fn load_state(
        &self,
        ref_by_mc_seqno: u32,
        block_id: &BlockId,
    ) -> Result<ShardStateStuff> {
        // NOTE: only for metrics.
        static MAX_KNOWN_EPOCH: AtomicU32 = AtomicU32::new(0);

        let root_hash = self.load_state_root_hash(block_id)?;
        let root = self.cell_storage.load_cell(&root_hash, ref_by_mc_seqno)?;
        let mut root = Cell::from(root as Arc<_>);

        let max_known_epoch = MAX_KNOWN_EPOCH
            .fetch_max(ref_by_mc_seqno, Ordering::Relaxed)
            .max(ref_by_mc_seqno);
        metrics::gauge!("tycho_storage_state_max_epoch").set(max_known_epoch);

        if !block_id.shard.is_masterchain() {
            init_shard_partitions_router(
                &mut root,
                &self.cell_storage,
                ref_by_mc_seqno,
                &block_id.shard,
                self.part_split_depth,
            )?;
        }

        let shard_state = root.parse::<Box<ShardStateUnsplit>>()?;
        let handle = self.min_ref_mc_state.insert(&shard_state);
        ShardStateStuff::from_state_and_root(block_id, shard_state, root, handle)
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

        let raw = self.cells_db.rocksdb();

        // Manually get required column factory and r/w options
        let snapshot = raw.snapshot();
        let shard_states_cf = self.cells_db.shard_states.get_unbounded_cf();
        let mut states_read_options = self.cells_db.shard_states.new_read_config();
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
                self.gc_lock.clone().lock_owned().await
            };

            let db = self.cells_db.clone();
            let cell_storage = self.cell_storage.clone();
            let key = key.to_vec();
            let accounts_split_depth = self.accounts_split_depth;
            let (total, inner_alloc) = tokio::task::spawn_blocking(move || {
                let in_mem_remove =
                    HistogramGuard::begin("tycho_storage_cell_in_mem_remove_time_high");

                let (stats, mut batch) = if block_id.is_masterchain() {
                    cell_storage.remove_cell(alloc.get().as_bump(), &root_hash)?
                } else {
                    // NOTE: We use epoch `0` here so that cells of old states
                    // will not be used by recent loads.
                    let root_cell = Cell::from(cell_storage.load_cell(&root_hash, 0)? as Arc<_>);

                    let split_at =
                        split_shard_accounts(&block_id.shard, &root_cell, accounts_split_depth)?
                            .into_keys()
                            .collect::<FastHashSet<HashBytes>>();
                    cell_storage.remove_cell_mt(&alloc, &root_hash, split_at)?
                };

                in_mem_remove.finish();

                batch.delete_cf(&db.shard_states.get_unbounded_cf().bound(), key);
                db.raw()
                    .rocksdb()
                    .write_opt(batch, db.cells.write_config())?;

                // NOTE: Ensure that guard is dropped only after writing the batch.
                drop(guard);

                Ok::<_, anyhow::Error>((stats, alloc))
            })
            .await??;

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
    #[error("Shard state not found for block: {0}")]
    NotFound(BlockIdShort),
    #[error("Block handle id mismatch: expected {expected}, got {actual}")]
    BlockHandleIdMismatch {
        expected: BlockIdShort,
        actual: BlockIdShort,
    },
}

fn split_shard_accounts(
    state_shard: &ShardIdent,
    root_cell: impl AsRef<DynCell>,
    split_depth: u8,
) -> Result<FastHashMap<HashBytes, SplitAccountEntry>> {
    // Cell#0 - processed_upto
    // Cell#1 - accounts
    let shard_accounts = root_cell
        .as_ref()
        .reference_cloned(1)
        .context("invalid shard state")?
        .parse::<ShardAccounts>()
        .context("failed to load shard accounts")?;

    let shards = split_aug_dict_raw_by_shards(state_shard.workchain(), shard_accounts, split_depth)
        .context("failed to split shard accounts")?;

    let mut result = FastHashMap::default();

    for (shard, dict) in shards {
        if let Some(cell) = dict {
            result.insert(*cell.repr_hash(), SplitAccountEntry { shard, cell });
        }
    }

    Ok(result)
}

#[tracing::instrument(skip_all)]
fn init_shard_partitions_router(
    root_cell: &mut Cell,
    cell_storage: &Arc<CellStorage>,
    ref_by_mc_seqno: u32,
    state_shard: &ShardIdent,
    part_split_depth: u8,
) -> Result<()> {
    if part_split_depth == 0 {
        return Ok(());
    }

    // split shard accounts to load root cells for each partition subtree
    let split_at: FastHashMap<_, _> =
        split_shard_accounts(state_shard, &root_cell, part_split_depth)?
            .into_iter()
            .map(|(hash, SplitAccountEntry { shard, .. })| (hash, shard))
            .collect();

    tracing::debug!(part_split_depth, ?split_at);

    // reload state root cell with shard router
    let cell = cell_storage.load_cell_ext(
        root_cell.repr_hash(),
        ref_by_mc_seqno,
        Some(CellShardRouter::ChildIsShardAccountsRoot(
            1,
            Arc::new(split_at),
        )),
    )?;

    *root_cell = Cell::from(cell as Arc<_>);

    Ok(())
}
