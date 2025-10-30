use std::fs::File;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::time::Instant;

use anyhow::{Context, Result};
use bytes::Buf;
use bytesize::ByteSize;
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use tokio::sync::OwnedMutexGuard;
use tycho_block_util::block::*;
use tycho_block_util::dict::{split_aug_dict_raw_by_shards, split_dict_raw};
use tycho_block_util::state::*;
use tycho_storage::fs::TempFileStorage;
use tycho_storage::kv::{StoredValue, StoredValueBuffer};
use tycho_types::models::*;
use tycho_types::prelude::*;
use tycho_util::mem::Reclaimer;
use tycho_util::metrics::HistogramGuard;
use tycho_util::{FastHashMap, FastHashSet};
use weedb::rocksdb;

use self::cell_storage::*;
use self::store_state_raw::StoreStateContext;
use super::{BlockFlags, BlockHandle, BlockHandleStorage, BlockStorage, CellsDb, CellsPartDb};
use crate::storage::db::{CellStorageDb, CellsDbOps};

mod cell_storage;
mod entries_buffer;
mod store_state_raw;

pub type StoragePartsMap = FastHashMap<ShardPrefix, Arc<dyn ShardStateStoragePart>>;

#[derive(Debug, Clone)]
pub struct SplitAccountEntry {
    pub shard: Option<ShardIdent>,
    pub cell: Cell,
}

pub trait ShardStateStoragePart: Send + Sync {
    fn shard_prefix(&self) -> ShardPrefix;
    fn cell_storage(&self) -> &Arc<CellStorage>;
    fn store_accounts_subtree(
        self: Arc<Self>,
        block_id: &BlockId,
        cell: Cell,
        estimated_cell_count: usize,
        split_depth: u8,
    ) -> Pin<Box<dyn Future<Output = Result<usize>> + Send>>;
    fn blocking_store_accounts_subtree(
        self: Arc<Self>,
        block_id: &BlockId,
        cell: Cell,
        estimated_cell_count: usize,
        split_depth: u8,
    ) -> Result<usize>;
    fn remove_outdated_states_in_partition(
        self: Arc<Self>,
        top_blocks: TopBlocks,
        remaining_split_depth: u8,
    ) -> Pin<Box<dyn Future<Output = Result<usize>> + Send>>;
}

pub struct ShardStateStoragePartImpl {
    shard_prefix: ShardPrefix,
    cells_db: CellsPartDb,
    cell_storage: Arc<CellStorage>,
    gc_lock: Arc<tokio::sync::Mutex<()>>,
}

impl ShardStateStoragePart for ShardStateStoragePartImpl {
    fn shard_prefix(&self) -> ShardPrefix {
        self.shard_prefix
    }

    fn cell_storage(&self) -> &Arc<CellStorage> {
        &self.cell_storage
    }

    fn blocking_store_accounts_subtree(
        self: Arc<Self>,
        block_id: &BlockId,
        cell: Cell,
        estimated_cell_count: usize,
        split_depth: u8,
    ) -> Result<usize> {
        self.store_accounts_subtree_impl(block_id, cell, estimated_cell_count, split_depth)
    }

    fn store_accounts_subtree(
        self: Arc<Self>,
        block_id: &BlockId,
        cell: Cell,
        estimated_cell_count: usize,
        split_depth: u8,
    ) -> Pin<Box<dyn Future<Output = Result<usize>> + Send>> {
        let block_id = *block_id;
        let fut = async move {
            tokio::task::spawn_blocking(move || {
                self.store_accounts_subtree_impl(&block_id, cell, estimated_cell_count, split_depth)
            })
            .await?
        };
        Box::pin(fut)
    }

    fn remove_outdated_states_in_partition(
        self: Arc<Self>,
        top_blocks: TopBlocks,
        remaining_split_depth: u8,
    ) -> Pin<Box<dyn Future<Output = Result<usize>> + Send>> {
        let fut = self.remove_outdated_states_impl(top_blocks, remaining_split_depth);
        Box::pin(fut)
    }
}

impl ShardStateStoragePartImpl {
    pub fn new(
        shard_prefix: ShardPrefix,
        cells_db: CellsPartDb,
        cache_size_bytes: ByteSize,
        drop_interval: u32,
    ) -> Self {
        let cell_storage = CellStorage::new_for_shard(
            cells_db.clone(),
            cache_size_bytes,
            drop_interval,
            shard_prefix,
        );
        Self {
            shard_prefix,
            cells_db,
            cell_storage,
            gc_lock: Default::default(),
        }
    }

    fn store_accounts_subtree_impl(
        self: Arc<Self>,
        block_id: &BlockId,
        cell: Cell,
        estimated_cell_count: usize,
        split_depth: u8,
    ) -> Result<usize> {
        // check if subtree already stored
        let check_if_subtree_already_stored = || {
            let mut already_stored = false;
            if let Some(value) = self.cells_db.shard_states.get(block_id.to_vec())? {
                let entry = ShardStateEntry::from_slice(value.as_ref());
                already_stored = *cell.repr_hash() == entry.root_hash;
            }
            Ok::<_, anyhow::Error>(already_stored)
        };

        if check_if_subtree_already_stored()? {
            return Ok(0);
        }

        let _guard = {
            let _hist = HistogramGuard::begin("tycho_storage_cell_gc_lock_store_time_high");
            self.gc_lock.clone().blocking_lock_owned()
        };

        // Double check if subtree already stored
        if check_if_subtree_already_stored()? {
            return Ok(0);
        }

        let raw_db = self.cells_db.rocksdb().clone();
        let cf = self.cells_db.shard_states.get_unbounded_cf();
        let cell_storage = self.cell_storage.clone();

        let estimated_update_size_bytes = estimated_cell_count * 192; // p50 cell size in bytes
        let mut batch = rocksdb::WriteBatch::with_capacity_bytes(estimated_update_size_bytes);

        let split_at = split_accounts_subtree(cell.clone(), split_depth)?;

        let new_cell_count = cell_storage.store_cell_mt(
            cell.as_ref(),
            &mut batch,
            split_at,
            false,
            estimated_cell_count,
        )?;

        let entry = ShardStateEntry {
            root_hash: *cell.repr_hash(),
            partitions: None,
        };
        batch.put_cf(&cf.bound(), block_id.to_vec(), entry.to_vec());

        raw_db.write(batch)?;

        tracing::debug!(
            shard = %DisplayShardPrefix(&self.shard_prefix),
            %block_id,
            subtree_root_hash = %cell.repr_hash(),
            new_cell_count,
            "stored subtree in storage part"
        );

        Reclaimer::instance().drop(cell);

        Ok(new_cell_count)
    }

    async fn remove_outdated_states_impl(
        self: Arc<Self>,
        top_blocks: TopBlocks,
        remaining_split_depth: u8,
    ) -> Result<usize> {
        let cells_db = CellStorageDb::Part(self.cells_db.clone());

        let mut cx = RemoveOutdatedStatesContext::new(
            true,
            cells_db.clone(),
            self.cell_storage.clone(),
            remaining_split_depth,
            self.gc_lock.clone(),
        );

        cx.remove_outdated_states(&top_blocks, |removed_cells, block_id| {
            tracing::debug!(
                removed_cells,
                %block_id,
                part_shard = %DisplayShardPrefix(&self.shard_prefix),
                "removed state in partition",
            );
        })
        .await?;

        metrics::counter!("tycho_storage_state_gc_cells_count").increment(cx.removed_cells as u64);

        Ok(cx.removed_cells)
    }
}

pub struct ShardStateStorageContext {
    pub cells_db: CellsDb,
    pub block_handle_storage: Arc<BlockHandleStorage>,
    pub block_storage: Arc<BlockStorage>,
    pub temp_file_storage: TempFileStorage,
    pub cache_size_bytes: ByteSize,
    pub drop_interval: u32,
    pub part_split_depth: u8,
    pub storage_parts: Arc<StoragePartsMap>,
}

pub struct ShardStateStorage {
    cells_db: CellStorageDb,

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
    storage_parts: Option<Arc<StoragePartsMap>>,
}

impl ShardStateStorage {
    pub fn new(cx: ShardStateStorageContext) -> Result<Arc<Self>> {
        let ShardStateStorageContext {
            cells_db,
            block_handle_storage,
            block_storage,
            temp_file_storage,
            cache_size_bytes,
            drop_interval,
            part_split_depth,
            storage_parts,
        } = cx;

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
            Some(storage_parts.clone()),
        );

        Ok(Arc::new(Self {
            cells_db: CellStorageDb::Main(cells_db),
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
            storage_parts: Some(storage_parts),
        }))
    }

    fn uses_partitions(&self) -> bool {
        self.part_split_depth > 0
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
        // check if the main state and parts are already stored
        if handle.has_state() {
            return Ok(false);
        }

        let gc_lock = {
            let _hist = HistogramGuard::begin("tycho_storage_cell_gc_lock_store_time_high");
            self.gc_lock.clone().lock_owned().await
        };
        let mut gc_lock = Some(gc_lock);

        // Double check if the main state and parts are already stored
        if handle.has_state() {
            return Ok(false);
        }

        let _hist = HistogramGuard::begin("tycho_storage_state_store_time");

        let block_id = *handle.id();

        let estimated_merkle_update_size = hint.estimate_cell_count();

        // calculate split depth to store accounts in partitions
        // or to store in main in parallel
        let (accounts_split_depth, remaining_split_depth) =
            calc_split_depth(self.accounts_split_depth, self.part_split_depth);

        // NOTE: we split shard state only for shard blocks
        let split_at = if !block_id.is_masterchain() {
            split_shard_accounts(&block_id.shard, &root_cell, accounts_split_depth)?
        } else {
            FastHashMap::default()
        };

        // compute partitions info for ShardStateEntry
        let partitions_info =
            if !block_id.is_masterchain() && self.uses_partitions() && !split_at.is_empty() {
                let mut partitions = Vec::new();
                for (hash, entry) in &split_at {
                    if let Some(shard) = &entry.shard {
                        partitions.push(ShardStatePartition {
                            hash: *hash,
                            prefix: shard.prefix(),
                        });
                    }
                }
                partitions.sort_unstable();
                Some(partitions)
            } else {
                None
            };

        // run store tasks in partitions
        let mut part_store_tasks = FuturesUnordered::new();
        if !handle.has_state_parts() && !block_id.is_masterchain() {
            tracing::debug!(accounts_split_depth, remaining_split_depth, ?split_at);

            if let Some(storage_parts) = &self.storage_parts {
                // estimate partition cells count
                let denom = 1 << (self.part_split_depth.saturating_add(1));
                let part_estimated_cells_count =
                    (estimated_merkle_update_size / denom.max(1)).max(1);

                for v in split_at.values() {
                    if let Some(shard) = &v.shard
                        && let Some(storage_part) = storage_parts.get(&shard.prefix())
                    {
                        let storage_part = storage_part.clone();
                        part_store_tasks.push(tokio::spawn(storage_part.store_accounts_subtree(
                            &block_id,
                            v.cell.clone(),
                            part_estimated_cells_count,
                            remaining_split_depth,
                        )));
                    }
                }
            }
        }

        // store main state in a separate task
        let mut new_cell_count = 0;
        let mut updated = false;
        if !handle.has_state_main() {
            let root_hash = *root_cell.repr_hash();

            let block_handle_storage = self.block_handle_storage.clone();
            let handle = handle.clone();

            let update_block_handle = move || {
                // update block handle flags that main state stored
                let updated = handle.meta().add_flags(BlockFlags::HAS_STATE_MAIN);
                if updated {
                    block_handle_storage.store_handle(&handle, false);
                }
                updated
            };

            // check if state already stored
            let mut already_stored = false;
            if let Some(value) = self.cells_db.shard_states().get(block_id.to_vec())? {
                let entry = ShardStateEntry::from_slice(value.as_ref());
                if entry.root_hash == root_hash && entry.partitions == partitions_info {
                    already_stored = true;
                }
            }

            if already_stored {
                // update block handle flags that main state stored
                updated = update_block_handle();

                // NOTE: Ensure that GC lock is dropped only after storing the state.
                drop(gc_lock.take());
            } else {
                // NOTE: `spawn_blocking` is used here instead of `rayon_run` as it is IO-bound task.
                let raw_db = self.cells_db.rocksdb().clone();
                let cf = self.cells_db.shard_states().get_unbounded_cf();
                let cell_storage = self.cell_storage.clone();
                let uses_partitions = self.uses_partitions();
                let gc_lock = gc_lock.take();
                (new_cell_count, updated) = tokio::task::spawn_blocking(move || {
                    let estimated_merkle_update_size = hint.estimate_cell_count();
                    let estimated_update_size_bytes = estimated_merkle_update_size * 192; // p50 cell size in bytes
                    let mut batch =
                        rocksdb::WriteBatch::with_capacity_bytes(estimated_update_size_bytes);

                    let in_mem_store =
                        HistogramGuard::begin("tycho_storage_cell_in_mem_store_time_high");

                    let new_cell_count = if block_id.is_masterchain() {
                        cell_storage.store_cell(
                            &mut batch,
                            root_cell.as_ref(),
                            estimated_merkle_update_size,
                        )?
                    } else {
                        cell_storage.store_cell_mt(
                            root_cell.as_ref(),
                            &mut batch,
                            split_at,
                            uses_partitions,
                            estimated_merkle_update_size,
                        )?
                    };

                    in_mem_store.finish();

                    let entry = ShardStateEntry {
                        root_hash,
                        partitions: partitions_info,
                    };
                    batch.put_cf(&cf.bound(), block_id.to_vec(), entry.to_vec());

                    let hist = HistogramGuard::begin("tycho_storage_state_update_time_high");
                    metrics::histogram!("tycho_storage_state_update_size_bytes")
                        .record(batch.size_in_bytes() as f64);
                    metrics::histogram!("tycho_storage_state_update_size_predicted_bytes")
                        .record(estimated_update_size_bytes as f64);

                    raw_db.write(batch)?;

                    Reclaimer::instance().drop(root_cell);

                    hist.finish();

                    // update block handle flags that main state stored
                    let updated = update_block_handle();

                    // NOTE: Ensure that GC lock is dropped only after storing the state.
                    drop(gc_lock);

                    Ok::<_, anyhow::Error>((new_cell_count, updated))
                })
                .await??;
            }
        }

        // wait for all store tasks in partitions
        let mut parts_store_failed = false;
        while let Some(store_res) = part_store_tasks.next().await {
            match store_res {
                Ok(Ok(part_new_cells)) => {
                    new_cell_count += part_new_cells;
                }
                Ok(Err(store_error)) => {
                    parts_store_failed = true;
                    tracing::error!(?store_error, "error in store_accounts_subtree method");
                }
                Err(join_error) => {
                    parts_store_failed = true;
                    tracing::error!(?join_error, "error executing store_accounts_subtree task");
                }
            }
        }

        // update block handle flags that state parts stored
        if !parts_store_failed {
            let updated_state_parts = handle.meta().add_flags(BlockFlags::HAS_STATE_PARTS);
            if updated_state_parts {
                updated = true;

                // TODO: maybe this gc lock is excessive?

                // acquire new lock if it was dropped before
                if gc_lock.is_none() {
                    let lock = {
                        let _hist =
                            HistogramGuard::begin("tycho_storage_cell_gc_lock_store_time_high");
                        self.gc_lock.clone().lock_owned().await
                    };
                    gc_lock = Some(lock);
                }
                self.block_handle_storage.store_handle(handle, false);
                drop(gc_lock);
            }
        }

        metrics::histogram!("tycho_storage_cell_count").record(new_cell_count as f64);

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

        let Some(shard_state) = self.load_state_entry(block_id)? else {
            anyhow::bail!(ShardStateStorageError::NotFound(block_id.as_short_id()))
        };

        let root_hash = shard_state.root_hash;
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
                shard_state.partitions,
            )?;
        }

        let shard_state = root.parse::<Box<ShardStateUnsplit>>()?;
        let handle = self.min_ref_mc_state.insert(&shard_state);
        ShardStateStuff::from_state_and_root(block_id, shard_state, root, handle)
    }

    pub fn load_state_entry(&self, block_id: &BlockId) -> Result<Option<ShardStateEntry>> {
        let shard_states = &self.cells_db.shard_states();
        let value = shard_states.get(block_id.to_vec())?;
        let entry = value.map(|v| ShardStateEntry::from_slice(v.as_ref()));
        Ok(entry)
    }

    pub fn load_state_root_hash(&self, block_id: &BlockId) -> Result<HashBytes> {
        let entry = self.load_state_entry(block_id)?;
        match entry {
            Some(entry) => Ok(entry.root_hash),
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
        // TODO: add overall metric for main db and partitions
        let started_at = Instant::now();

        // calculate split depth
        let (accounts_split_depth, remaining_split_depth) =
            calc_split_depth(self.accounts_split_depth, self.part_split_depth);

        // run remove tasks in partitions
        let mut part_remove_tasks = FuturesUnordered::new();
        if let Some(storage_parts) = &self.storage_parts {
            for (_, storage_part) in storage_parts.iter() {
                let storage_part = storage_part.clone();
                part_remove_tasks.push(tokio::spawn(
                    storage_part.remove_outdated_states_in_partition(
                        top_blocks.clone(),
                        remaining_split_depth,
                    ),
                ));
            }
        }

        // remove state data in main db
        let mut cx = RemoveOutdatedStatesContext::new(
            false,
            self.cells_db.clone(),
            self.cell_storage.clone(),
            accounts_split_depth,
            self.gc_lock.clone(),
        );

        cx.remove_outdated_states(&top_blocks, |removed_cells, block_id| {
            tracing::debug!(removed_cells, %block_id, "removed state");
            if block_id.is_masterchain() {
                metrics::gauge!("tycho_gc_states_seqno").set(block_id.seqno as f64);
            }
        })
        .await?;

        metrics::counter!("tycho_storage_state_gc_count").increment(cx.removed_states as u64);
        metrics::counter!("tycho_storage_state_gc_cells_count").increment(cx.removed_cells as u64);

        // wait for all remove tasks in partitions
        while let Some(remove_res) = part_remove_tasks.next().await {
            match remove_res {
                Ok(Ok(removed_cells)) => {
                    cx.removed_cells += removed_cells;
                }
                Ok(Err(remove_error)) => {
                    tracing::error!(
                        ?remove_error,
                        "error in remove_outdated_states_in_partition method"
                    );
                }
                Err(join_error) => {
                    tracing::error!(
                        ?join_error,
                        "error executing remove_outdated_states_in_partition task"
                    );
                }
            }
        }

        // Done
        tracing::info!(
            removed_states = cx.removed_states,
            removed_cells = cx.removed_cells,
            block_id = %top_blocks.mc_block,
            elapsed_sec = started_at.elapsed().as_secs_f64(),
            "finished states GC",
        );
        Ok(())
    }

    /// Searches for an edge with the least referenced masterchain block
    ///
    /// Returns `None` if all states are recent enough
    async fn compute_recent_blocks(&self, mut mc_seqno: u32) -> Result<Option<TopBlocks>> {
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
        let shard_states = self.cells_db.shard_states();

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

#[derive(Clone)]
struct RemoveOutdatedStatesContext {
    in_partition: bool,

    cells_db: CellStorageDb,
    cell_storage: Arc<CellStorage>,

    split_depth: u8,

    gc_lock: Arc<tokio::sync::Mutex<()>>,

    // alloc: bumpalo_herd::Herd,
    removed_cells: usize,
    removed_states: usize,
}

impl RemoveOutdatedStatesContext {
    fn new(
        in_partition: bool,
        cells_db: CellStorageDb,
        cell_storage: Arc<CellStorage>,
        split_depth: u8,
        gc_lock: Arc<tokio::sync::Mutex<()>>,
    ) -> Self {
        Self {
            in_partition,
            cells_db,
            cell_storage,
            split_depth,
            gc_lock,
            removed_cells: 0,
            removed_states: 0,
        }
    }

    async fn acquire_gc_lock(&self) -> OwnedMutexGuard<()> {
        let _h = HistogramGuard::begin("tycho_storage_cell_gc_lock_remove_time_high");
        self.gc_lock.clone().lock_owned().await
    }

    fn create_states_iterator_to_top_blocks(
        cells_db: &CellStorageDb,
        top_blocks: &TopBlocks,
    ) -> impl Iterator<Item = Result<(BlockId, ShardStateEntry), weedb::rocksdb::Error>> {
        let raw = cells_db.rocksdb();

        // Manually get required column factory and r/w options
        let snapshot = raw.snapshot();
        let shard_states_cf = cells_db.shard_states().get_unbounded_cf();
        let mut states_read_options = cells_db.shard_states().new_read_config();
        states_read_options.set_snapshot(&snapshot);

        // Create iterator
        let iter = raw.iterator_cf_opt(
            &shard_states_cf.bound(),
            states_read_options,
            rocksdb::IteratorMode::Start,
        );

        // skip zerostae and states equal or above top blocks
        iter.filter_map(|item| match item {
            Ok((key, value)) => {
                let block_id = BlockId::from_slice(&key);
                if block_id.seqno == 0
                    || top_blocks.contains_shard_seqno(&block_id.shard, block_id.seqno)
                {
                    None
                } else {
                    Some(Ok((block_id, ShardStateEntry::from_slice(value.as_ref()))))
                }
            }
            Err(err) => Some(Err(err)),
        })
    }

    fn blocking_remove_state(
        &mut self,
        block_id: BlockId,
        entry: ShardStateEntry,
        guard: OwnedMutexGuard<()>,
        mut alloc: bumpalo_herd::Herd,
    ) -> Result<RemoveStateResult> {
        alloc.reset();

        let in_mem_remove = HistogramGuard::begin("tycho_storage_cell_in_mem_remove_time_high");

        let (removed_cells, mut batch) = if block_id.is_masterchain() {
            self.cell_storage
                .remove_cell(alloc.get().as_bump(), &entry.root_hash)?
        } else {
            // NOTE: We use epoch `0` here so that cells of old states
            // will not be used by recent loads.
            let root_cell = Cell::from(self.cell_storage.load_cell(&entry.root_hash, 0)? as Arc<_>);

            // try to split state
            let (split_at, split_by_partitions) = if self.in_partition {
                // in partition we split subtree for parallel processing
                let split_at = split_accounts_subtree(root_cell, self.split_depth)?;
                let split_at: FastHashSet<_> = split_at.into_keys().collect();
                (split_at, false)
            } else if let Some(partitions_info) = entry.partitions
                && !partitions_info.is_empty()
            {
                // in main tree we use stored split when exists
                let split_at: FastHashSet<_> =
                    partitions_info.into_iter().map(|p| p.hash).collect();
                (split_at, true)
            } else {
                // otherwise in main tree we split for parallel processing
                let split_at = split_shard_accounts(&block_id.shard, root_cell, self.split_depth)?;
                let split_at: FastHashSet<_> = split_at.into_keys().collect();
                (split_at, false)
            };

            self.cell_storage.remove_cell_mt(
                &alloc,
                &entry.root_hash,
                split_at,
                split_by_partitions,
            )?
        };

        in_mem_remove.finish();

        let db = &self.cells_db;
        batch.delete_cf(
            &db.shard_states().get_unbounded_cf().bound(),
            block_id.to_vec(),
        );
        db.rocksdb().write_opt(batch, db.cells().write_config())?;

        // NOTE: Ensure that guard is dropped only after writing the batch.
        drop(guard);

        self.removed_cells += removed_cells;
        self.removed_states += 1;

        Ok(RemoveStateResult {
            removed_cells,
            inner_alloc: alloc,
        })
    }

    async fn remove_state(
        &mut self,
        block_id: BlockId,
        entry: ShardStateEntry,
        guard: OwnedMutexGuard<()>,
        alloc: bumpalo_herd::Herd,
    ) -> Result<RemoveStateResult> {
        let mut this = self.clone();
        let (this, res) = tokio::task::spawn_blocking(move || {
            let res = this.blocking_remove_state(block_id, entry, guard, alloc)?;
            Ok::<_, anyhow::Error>((this, res))
        })
        .await??;
        *self = this;
        Ok(res)
    }

    async fn remove_outdated_states<F>(
        &mut self,
        top_blocks: &TopBlocks,
        handle_state_removed: F,
    ) -> Result<()>
    where
        F: Fn(usize, BlockId),
    {
        let cells_db = self.cells_db.clone();
        let iter = RemoveOutdatedStatesContext::create_states_iterator_to_top_blocks(
            &cells_db, top_blocks,
        );

        let mut alloc = bumpalo_herd::Herd::new();

        for item in iter {
            let _hist = HistogramGuard::begin("tycho_storage_state_gc_time_high");
            let (block_id, entry) = item?;
            let guard = self.acquire_gc_lock().await;
            let RemoveStateResult {
                removed_cells,
                inner_alloc,
            } = self.remove_state(block_id, entry, guard, alloc).await?;
            alloc = inner_alloc;

            handle_state_removed(removed_cells, block_id);
        }

        Ok(())
    }
}

struct RemoveStateResult {
    removed_cells: usize,
    inner_alloc: bumpalo_herd::Herd,
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

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ShardStatePartition {
    pub hash: HashBytes,
    pub prefix: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShardStateEntry {
    pub root_hash: HashBytes,
    pub partitions: Option<Vec<ShardStatePartition>>,
}

impl StoredValue for ShardStateEntry {
    /// 32 bytes root hash,
    /// 2 bytes partitions map size
    /// (
    ///   32 bytes partition root cell hash +
    ///   8 bytes shard ident prefix
    /// ) * 16 max partitions
    /// = 640 bytes partitions map
    const SIZE_HINT: usize = 32 + 2 + (32 + 8) * 16;

    type OnStackSlice = [u8; Self::SIZE_HINT];

    fn serialize<T: StoredValueBuffer>(&self, buffer: &mut T) {
        buffer.write_raw_slice(self.root_hash.as_slice());

        if let Some(partitions) = self.partitions.as_ref() {
            if partitions.is_empty() {
                return;
            }

            let count = partitions.len() as u16;
            assert!(count <= 16, "too many shard state partitions");
            buffer.write_raw_slice(&count.to_be_bytes());

            for partition in partitions {
                buffer.write_raw_slice(partition.hash.as_slice());
                buffer.write_raw_slice(&partition.prefix.to_be_bytes());
            }
        }
    }

    fn deserialize(reader: &mut &[u8]) -> Self
    where
        Self: Sized,
    {
        debug_assert!(reader.remaining() >= 32);

        let root_hash = HashBytes::from_slice(&reader[..32]);
        *reader = &reader[32..];

        if reader.is_empty() {
            return Self {
                root_hash,
                partitions: None,
            };
        }

        debug_assert!(reader.remaining() >= 2);

        let count = reader.get_u16();
        if count == 0 {
            return Self {
                root_hash,
                partitions: None,
            };
        }

        let mut partitions = Vec::with_capacity(count as usize);
        for _ in 0..count {
            debug_assert!(reader.remaining() >= 32 + 8);

            let hash = HashBytes::from_slice(&reader[..32]);
            *reader = &reader[32..];

            let prefix = reader.get_u64();
            partitions.push(ShardStatePartition { hash, prefix });
        }

        Self {
            root_hash,
            partitions: Some(partitions),
        }
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

/// Calculates `(target_split_depth, remaining_split_depth)`:
/// * `target_split_depth` - target accounts split depth. Returns `part_split_depth` when `part_split_depth > 0` (partitions used).
/// * `remaining_split_depth` - remaining accounts split depth inside partitions when partitions used.
///   When we split accounts on partitions we can split accounts inside partition more and store in parallel.
fn calc_split_depth(accounts_split_depth: u8, part_split_depth: u8) -> (u8, u8) {
    let target_split_depth = if part_split_depth > 0 {
        part_split_depth
    } else {
        accounts_split_depth
    };
    let mut remaining_split_depth = accounts_split_depth.saturating_sub(target_split_depth);
    if accounts_split_depth >= part_split_depth {
        remaining_split_depth += 1;
    }
    (target_split_depth, remaining_split_depth)
}

fn split_accounts_subtree(
    subtree_root_cell: Cell,
    split_depth: u8,
) -> Result<FastHashMap<HashBytes, SplitAccountEntry>> {
    let shards = split_dict_raw(Some(subtree_root_cell), HashBytes::BITS, split_depth)
        .context("failed to split shard accounts subtree")?;

    let mut result = FastHashMap::default();

    for (hash, cell) in shards {
        result.insert(hash, SplitAccountEntry { shard: None, cell });
    }

    Ok(result)
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
            result.insert(*cell.repr_hash(), SplitAccountEntry {
                shard: Some(shard),
                cell,
            });
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
    partitions_info: Option<Vec<ShardStatePartition>>,
) -> Result<()> {
    // get partitions info
    let partitions_info = match partitions_info {
        Some(p) if !p.is_empty() => p,
        _ => return Ok(()),
    };

    // build router map
    let split_at: FastHashMap<_, _> = partitions_info
        .into_iter()
        .map(|p| (p.hash, p.prefix))
        .collect();

    tracing::debug!(%state_shard, ?split_at);

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
