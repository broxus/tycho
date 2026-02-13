use std::collections::hash_map;
use std::fs::File;
use std::io::Cursor;
use std::mem::ManuallyDrop;
use std::num::NonZeroU8;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::time::Instant;

use anyhow::{Context, Result};
use bytes::Bytes;
use futures_util::FutureExt;
use futures_util::future::BoxFuture;
use tokio::sync::OwnedMutexGuard;
use tokio::task::JoinHandle;
use tycho_block_util::block::*;
use tycho_block_util::dict::split_aug_dict_raw;
use tycho_block_util::state::*;
use tycho_storage::fs::TempFileStorage;
use tycho_storage::kv::StoredValue;
use tycho_types::merkle::{FindCell, MerkleUpdate, ParMerkleUpdateApplier};
use tycho_types::models::*;
use tycho_types::prelude::*;
use tycho_util::futures::{AwaitBlocking, Shared};
use tycho_util::mem::Reclaimer;
use tycho_util::metrics::HistogramGuard;
use tycho_util::sync::rayon_run;
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
    counters: Arc<StoreStateCounters>,

    shard_split_depth: u8,
    new_cells_threshold: usize,
    store_shard_state_step: NonZeroU8,

    shard_states_cache: FastDashMap<ShardIdent, ShardStatesCache>,
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
            counters: Default::default(),
            shard_states_cache: Default::default(),
        }))
    }

    pub fn metrics(&self) -> ShardStateStorageMetrics {
        let counters = self.counters.as_ref();
        ShardStateStorageMetrics {
            max_new_mc_cell_count: counters.max_new_mc_cell_count.swap(0, Ordering::AcqRel),
            max_new_sc_cell_count: counters.max_new_sc_cell_count.swap(0, Ordering::AcqRel),
        }
    }

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

    pub async fn store_next_state(
        &self,
        prev_handle: &BlockHandle,
        next_handle: &BlockHandle,
        merkle_update: &MerkleUpdate,
        state: Option<ShardStateStuff>,
        hint: StoreStateHint,
    ) -> Result<StoreStateStatus> {
        enum StoreType {
            Direct,
            Virtual,
        }

        // TODO: Consider splits.
        let prev_block_id = prev_handle.id();
        let block_id = next_handle.id();
        anyhow::ensure!(
            prev_block_id.shard == block_id.shard,
            "handle shard mismatch: {} != {}",
            prev_block_id.shard,
            block_id.shard,
        );
        anyhow::ensure!(
            prev_block_id.seqno + 1 == block_id.seqno,
            "merkle update can only be applied to consecutive blocks: \
            prev={prev_block_id}, next={block_id}",
        );

        if next_handle.has_state() || next_handle.has_virtual_state() {
            return Ok(StoreStateStatus::Exists);
        }

        let make_applier = || {
            let ref_mc_state_handle = self.min_ref_mc_state.insert_seqno(prev_handle.id().seqno);

            Arc::new(MerkleUpdateApplier::new(
                block_id.seqno,
                self.cell_storage.clone(),
                ref_mc_state_handle,
            ))
        };

        let state_root = match state {
            Some(state) => DirectStoreRoot::Exact {
                root: state.root_cell().clone(),
                ref_mc_state_handle: state.ref_mc_state_handle().clone(),
            },
            None => {
                anyhow::ensure!(
                    prev_handle.has_state(),
                    "previous masterchain state must be already saved"
                );
                DirectStoreRoot::Next {
                    applier: make_applier(),
                    partial_root: merkle_update.new.clone(),
                }
            }
        };

        let mut entry = self
            .shard_states_cache
            .entry(block_id.shard)
            .or_insert_with(|| {
                let applier = match &state_root {
                    DirectStoreRoot::Exact { .. } => make_applier(),
                    DirectStoreRoot::Next { applier, .. } => applier.clone(),
                };

                ShardStatesCache {
                    pivot_block_id: *prev_block_id,
                    applier,
                    states: Default::default(),
                }
            });

        let task = match entry.states.entry(block_id.root_hash) {
            hash_map::Entry::Occupied(entry) => match &entry.get().state {
                CachedState::Pending(task) => task.clone(),
                CachedState::Stored(_) => return Ok(StoreStateStatus::Exists),
                CachedState::Failed(e) => return Err(e.clone().into()),
            },
            hash_map::Entry::Vacant(entry) => {
                let gc_lock_fut = self.gc_lock.clone().lock_owned().boxed();

                let complete = Arc::new(AtomicBool::new(false));
                let task = make_pending_store_task(self.spawn_store_state_root_direct(
                    next_handle.clone(),
                    state_root,
                    hint,
                    gc_lock_fut,
                    complete.clone(),
                ));

                entry.insert(ShardStatesCacheItem {
                    prev_block_id: *prev_block_id,
                    block_id: *block_id,
                    is_virtual: false,
                    partial_root_cell: merkle_update.new.clone(),
                    new_cell_count: hint.new_cell_count,
                    ref_by_mc_seqno: next_handle.ref_by_mc_seqno(),
                    state: CachedState::Pending(task.clone()),
                    complete,
                });

                task
            }
        };
        drop(entry);

        let (result, _) = task.await;
        Ok(StoreStateStatus::Stored {
            is_virtual: false,
            state: result?,
        })
    }

    fn spawn_store_state_root_indirect(
        &self,
        handle: BlockHandle,
        applier: Arc<MerkleUpdateApplier>,
        partial_root_cell: Cell,
        ref_mc_state_handle: RefMcStateHandle,
        complete: Arc<AtomicBool>,
    ) -> JoinHandle<Result<ShardStateStuff>> {
        let block_handle_storage = self.block_handle_storage.clone();

        let complete_on_drop = scopeguard::guard(complete, |c| c.store(true, Ordering::Release));

        tokio::task::spawn_blocking(move || {
            let _guard = complete_on_drop;
            let _hist = HistogramGuard::begin("tycho_storage_cell_indirect_store_time_high");

            let root_cell = 'apply: {
                if let Some(cell) = applier.find_cell.find_cell(partial_root_cell.hash(0)) {
                    break 'apply cell;
                }

                let new = rayon::scope(|scope| {
                    applier.run(partial_root_cell.as_ref(), 0, 0, Some(scope))
                })?;
                new.resolve(Cell::empty_context())?
            };

            let shard_state = root_cell.parse::<Box<ShardStateUnsplit>>()?;
            let state = ShardStateStuff::from_state_and_root(
                handle.id(),
                shard_state,
                root_cell,
                ref_mc_state_handle,
            )?;

            block_handle_storage.set_has_virtual_shard_state(&handle);

            Ok::<_, anyhow::Error>(state)
        })
    }

    /// Spawns a task for storing the specified root into the storage.
    ///
    /// Returns `false` if that was already present in storage.
    fn spawn_store_state_root_direct(
        &self,
        handle: BlockHandle,
        root_cell: DirectStoreRoot,
        hint: StoreStateHint,
        gc_lock_fut: BoxFuture<'static, OwnedMutexGuard<()>>,
        complete: Arc<AtomicBool>,
    ) -> JoinHandle<Result<ShardStateStuff>> {
        let cells_db = self.cells_db.clone();
        let cell_storage = self.cell_storage.clone();
        let block_handles = self.block_handle_storage.clone();
        let shard_split_depth = self.shard_split_depth;
        let counters = self.counters.clone();

        let complete_on_drop = scopeguard::guard(complete, |c| c.store(true, Ordering::Release));

        tokio::task::spawn_blocking(move || {
            let guard = complete_on_drop;

            let block_id = handle.id();
            let prev_ref_mc_state_handle;
            let root_hash = match &root_cell {
                DirectStoreRoot::Exact {
                    root,
                    ref_mc_state_handle,
                } => {
                    prev_ref_mc_state_handle = ref_mc_state_handle.clone();
                    *root.repr_hash()
                }
                DirectStoreRoot::Next {
                    partial_root,
                    applier,
                } => {
                    prev_ref_mc_state_handle = (&*applier.ref_mc_state_handle).clone();
                    *partial_root.hash(0)
                }
            };

            let next_ref_mc_state_handle = prev_ref_mc_state_handle
                .tracker()
                .insert_seqno(handle.ref_by_mc_seqno());

            let load_existing_state = || {
                let epoch = handle.ref_by_mc_seqno();
                let root = cell_storage.load_cell(&root_hash, epoch)?;
                let root = Cell::from(root as Arc<_>);

                track_max_epoch(epoch);

                let shard_state = root.parse::<Box<ShardStateUnsplit>>()?;
                ShardStateStuff::from_state_and_root(
                    block_id,
                    shard_state,
                    root,
                    next_ref_mc_state_handle,
                )
            };

            // Fast path if already exists (before a possibly long apply).
            if handle.has_state() {
                return load_existing_state();
            }

            // Resolve root if needed.
            let root_cell = match root_cell {
                DirectStoreRoot::Exact { root, .. } => root,
                DirectStoreRoot::Next {
                    applier,
                    partial_root,
                } => 'root: {
                    if let Some(cell) = applier.find_cell.find_cell(&root_hash) {
                        break 'root cell;
                    }

                    let new = rayon::scope(|scope| {
                        applier.run(partial_root.as_ref(), 0, 0, Some(scope))
                    })?;
                    new.resolve(Cell::empty_context())?
                }
            };

            // Fast path if already exists (before a possibly long gc lock).
            if handle.has_state() {
                return load_existing_state();
            }

            // Wait for GC lock.
            let gc_lock = {
                let _hist = HistogramGuard::begin("tycho_storage_cell_gc_lock_store_time_high");
                gc_lock_fut.await_blocking()
            };

            // Fast path if already exists (after a possibly long gc lock).
            if handle.has_state() {
                return load_existing_state();
            }

            // Build store cell transaction.
            let estimated_merkle_update_size = hint.new_cell_count;
            let estimated_update_size_bytes = estimated_merkle_update_size * 192; // p50 cell size in bytes
            let mut batch = rocksdb::WriteBatch::with_capacity_bytes(estimated_update_size_bytes);

            let in_mem_store = HistogramGuard::begin("tycho_storage_cell_in_mem_store_time_high");

            let new_cell_count = if handle.is_masterchain() {
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

            batch.put_cf(
                &cells_db.shard_states.cf(),
                block_id.to_vec(),
                root_hash.as_slice(),
            );

            // Apply store cell transaction.
            let _hist = HistogramGuard::begin("tycho_storage_state_update_time_high");

            metrics::histogram!("tycho_storage_cell_count").record(new_cell_count as f64);
            metrics::histogram!("tycho_storage_state_update_size_bytes")
                .record(batch.size_in_bytes() as f64);
            metrics::histogram!("tycho_storage_state_update_size_predicted_bytes")
                .record(estimated_update_size_bytes as f64);

            let counter = if handle.is_masterchain() {
                &counters.max_new_mc_cell_count
            } else {
                &counters.max_new_sc_cell_count
            };
            counter.fetch_max(new_cell_count, Ordering::Release);

            cells_db.rocksdb().write(batch)?;

            // NOTE: We can signal that the future is complete as soon as the longest
            //       part is done. Maybe we can event signal about that before the actual
            //       write, just after building a transaction.
            drop(guard);

            // NOTE: Cell tree is still alive, just in case couple the ref handle lifetime with it.
            Reclaimer::instance().drop((root_cell, prev_ref_mc_state_handle));

            block_handles.set_has_shard_state(&handle);

            // NOTE: Ensure that GC lock is dropped only after storing the state.
            drop(gc_lock);

            // Reload state.
            load_existing_state()
        })
    }

    // Stores shard state and returns the hash of its root cell.
    pub async fn store_state_file(&self, block_id: &BlockId, boc: File) -> Result<HashBytes> {
        self.store_state_raw_inner(block_id, boc).await
    }

    pub async fn store_state_bytes(&self, block_id: &BlockId, boc: Bytes) -> Result<HashBytes> {
        let cursor = Cursor::new(boc);
        self.store_state_raw_inner(block_id, cursor).await
    }

    async fn store_state_raw_inner<R>(&self, block_id: &BlockId, boc: R) -> Result<HashBytes>
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

    /// Loads state for specified block using optional cache with `CachedStateUpdate` lookups.
    pub async fn load_state(
        &self,
        ref_by_mc_seqno: u32,
        block_id: &BlockId,
        hint: LoadStateHint,
    ) -> Result<ShardStateStuff> {
        fn load_failed(error: StoreStateError) -> anyhow::Error {
            anyhow::anyhow!("unable to load a state that failed to save with error: {error}")
        }

        // TODO: Move into config.
        const MAX_TAIL: usize = 10;

        // Masterchain states are always stored.
        if block_id.is_masterchain() {
            return self.load_state_direct(ref_by_mc_seqno, block_id).await;
        }

        let mut pivot_block_id = *block_id;
        let mut to_apply = Vec::new();
        let mut pivot_state = 'pivot: {
            let Some(cache) = self.shard_states_cache.get(&pivot_block_id.shard) else {
                break 'pivot None;
            };

            while let Some(item) = cache.states.get(&pivot_block_id.root_hash) {
                match &item.state {
                    // State is in cache and already loaded.
                    CachedState::Stored(state) => {
                        break 'pivot Some((state.clone(), cache.applier.clone()));
                    }
                    // State is in cache but its store was unsuccessful.
                    CachedState::Failed(error) => return Err(load_failed(error.clone())),
                    // We can wait for the result if:
                    // - We explicitly hinted to wait for the operation.
                    // - The state is already virtual (we will be doing the same operation so there
                    //   is no need to do it twice).
                    // - The pending task is already complete.
                    // - There are too many pending states.
                    // - No previous state in cache.
                    CachedState::Pending(task)
                        if hint.fast_apply_depth.is_none()
                            || item.is_virtual
                            || item.complete.load(Ordering::Acquire)
                            || to_apply.len() >= MAX_TAIL
                            || !cache.states.contains_key(&item.prev_block_id.root_hash) =>
                    {
                        let task = task.clone();
                        drop(cache);

                        let (result, _) = task.await;

                        let Some(mut cache) =
                            self.shard_states_cache.get_mut(&pivot_block_id.shard)
                        else {
                            anyhow::bail!("shard dissapeared from cache");
                        };

                        cache.save_result(&pivot_block_id, result.clone(), &self.cell_storage);

                        let state = result.map_err(load_failed)?;
                        break 'pivot Some((state, cache.applier.clone()));
                    }
                    // Otherwise we assume that applying merkle updates will be faster.
                    // Search back for the first item with "pending virtual" of "stored" state.
                    CachedState::Pending(_) => {
                        to_apply.push(item.partial_root_cell.clone());
                        pivot_block_id = item.prev_block_id;
                    }
                }
            }

            // No blocks were found in cache.
            None
        };

        // Nothing was found in cache so we should try load blocks directly.
        if pivot_state.is_none() {
            while to_apply.len() < MAX_TAIL {
                let handle = self
                    .block_handle_storage
                    .load_handle(&pivot_block_id)
                    .ok_or(ShardStateStorageError::BlockHandleNotFound(
                        block_id.as_short_id(),
                    ))?;

                // Load pivot state if the handle has one.
                if handle.has_state() {
                    let state = self.load_state_direct(ref_by_mc_seqno, block_id).await?;
                    let ref_mc_state_handle = state.ref_mc_state_handle().clone();
                    pivot_state = Some((
                        state,
                        Arc::new(MerkleUpdateApplier::new(
                            ref_by_mc_seqno,
                            self.cell_storage.clone(),
                            ref_mc_state_handle,
                        )),
                    ));
                    break;
                }

                // Otherwise load a merkle update from a block.
                let block = self.block_storage.load_block_data(&handle).await?;
                let merkle_update = block.as_ref().state_update.load()?;
                to_apply.push(merkle_update.new);

                let (prev_id, prev_id_alt) = block.construct_prev_id()?;
                anyhow::ensure!(
                    prev_id_alt.is_none(),
                    "split/merge is not supported for now"
                );
                pivot_block_id = prev_id;
            }
        }

        // Build the loaded state using the pivot and updates.
        let Some((pivot_state, applier)) = pivot_state else {
            anyhow::bail!("state not found");
        };

        // Fast path when there are no updates.
        if to_apply.is_empty() {
            anyhow::ensure!(
                pivot_state.block_id() == block_id,
                "loaded state id mismatch"
            );
            return Ok(pivot_state);
        }

        let ref_mc_state_handle = pivot_state.ref_mc_state_handle().clone();
        let mut pivot_root = pivot_state.root_cell().clone();
        drop(pivot_state);

        // Full case with applies.
        let pivot_root = rayon_run(move || {
            while let Some(partial_new_root) = to_apply.pop() {
                pivot_root = 'apply: {
                    if let Some(cell) = applier.find_cell.find_cell(&partial_new_root.hash(0)) {
                        break 'apply cell;
                    }

                    let new = rayon::scope(|scope| {
                        applier.run(partial_new_root.as_ref(), 0, 0, Some(scope))
                    })?;
                    new.resolve(Cell::empty_context())?
                };
            }

            Ok::<_, anyhow::Error>(pivot_root)
        })
        .await?;

        let shard_state = pivot_root.parse::<Box<ShardStateUnsplit>>()?;
        ShardStateStuff::from_state_and_root(block_id, shard_state, pivot_root, ref_mc_state_handle)
    }

    // NOTE: DO NOT try to make a separate `load_state_root` method
    // since the root must be properly tracked, and this tracking requires
    // knowing its `min_ref_mc_seqno` which can only be found out by
    // parsing the state. Creating a "Brief State" struct won't work either
    // because due to model complexity it is going to be error-prone.
    async fn load_state_direct(
        &self,
        ref_by_mc_seqno: u32,
        block_id: &BlockId,
    ) -> Result<ShardStateStuff> {
        let root_hash = self.load_state_root_hash(block_id)?;
        let root = self.cell_storage.load_cell(&root_hash, ref_by_mc_seqno)?;
        let root = Cell::from(root as Arc<_>);

        track_max_epoch(ref_by_mc_seqno);
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

            alloc.reset();

            let guard = {
                let _h = HistogramGuard::begin("tycho_storage_cell_gc_lock_remove_time_high");
                self.gc_lock.clone().lock_owned().await
            };

            let db = self.cells_db.clone();
            let cell_storage = self.cell_storage.clone();
            let key = key.to_vec();
            let shard_split_depth = self.shard_split_depth;
            let (total, inner_alloc) = tokio::task::spawn_blocking(move || {
                let in_mem_remove =
                    HistogramGuard::begin("tycho_storage_cell_in_mem_remove_time_high");

                let (stats, mut batch) = if block_id.is_masterchain() {
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

fn make_pending_store_task(handle: JoinHandle<Result<ShardStateStuff>>) -> PendingStoreTask {
    Shared::new(Box::pin(async move {
        handle.await?.map_err(StoreStateError::from)
    }))
}

fn track_max_epoch(epoch: u32) {
    // NOTE: only for metrics.
    static MAX_KNOWN_EPOCH: AtomicU32 = AtomicU32::new(0);

    let max_known_epoch = MAX_KNOWN_EPOCH
        .fetch_max(epoch, Ordering::Relaxed)
        .max(epoch);
    metrics::gauge!("tycho_storage_state_max_epoch").set(max_known_epoch);
}

struct ShardStatesCache {
    pivot_block_id: BlockId,
    applier: Arc<MerkleUpdateApplier>,
    states: FastHashMap<HashBytes, ShardStatesCacheItem>,
}

impl ShardStatesCache {
    fn save_result(
        &mut self,
        block_id: &BlockId,
        result: Result<ShardStateStuff, StoreStateError>,
        cell_storage: &Arc<CellStorage>,
    ) {
        let Some(item) = self.states.get_mut(&block_id.root_hash) else {
            return;
        };

        match result {
            Ok(state) => {
                let ref_mc_state_handle = state.ref_mc_state_handle().clone();
                item.state = CachedState::Stored(state);

                // Reset cache tail on each saved block.
                if !item.is_virtual && block_id.seqno > self.pivot_block_id.seqno {
                    let ref_by_mc_seqno = item.ref_by_mc_seqno;
                    drop(item);

                    self.pivot_block_id = *block_id;
                    self.applier = Arc::new(MerkleUpdateApplier::new(
                        ref_by_mc_seqno,
                        cell_storage.clone(),
                        ref_mc_state_handle,
                    ));
                    self.states
                        .retain(|_, item| item.block_id.seqno >= block_id.seqno);
                }
            }
            Err(error) => item.state = CachedState::Failed(error),
        }
    }
}

enum DirectStoreRoot {
    Exact {
        root: Cell,
        ref_mc_state_handle: RefMcStateHandle,
    },
    Next {
        applier: Arc<MerkleUpdateApplier>,
        partial_root: Cell,
    },
}

struct ShardStatesCacheItem {
    prev_block_id: BlockId,
    block_id: BlockId,
    is_virtual: bool,
    partial_root_cell: Cell,
    new_cell_count: usize,
    ref_by_mc_seqno: u32,
    state: CachedState,
    complete: Arc<AtomicBool>,
}

impl Drop for ShardStatesCacheItem {
    fn drop(&mut self) {
        Reclaimer::instance().drop(std::mem::take(&mut self.partial_root_cell));
    }
}

enum CachedState {
    Pending(PendingStoreTask),
    Stored(ShardStateStuff),
    Failed(StoreStateError),
}

type PendingStoreTask = Shared<BoxFuture<'static, Result<ShardStateStuff, StoreStateError>>>;

#[derive(Debug, Clone, thiserror::Error)]
#[error(transparent)]
struct StoreStateError(Arc<anyhow::Error>);

impl From<anyhow::Error> for StoreStateError {
    fn from(value: anyhow::Error) -> Self {
        Self(Arc::new(value))
    }
}

impl From<tokio::task::JoinError> for StoreStateError {
    fn from(value: tokio::task::JoinError) -> Self {
        Self(Arc::new(anyhow::anyhow!("task failed: {value}")))
    }
}

/// A wrapper for par applier to drop a potentially huge map
/// with new cells on a [`Reclaimer`] thread.
struct MerkleUpdateApplier {
    inner: ParMerkleUpdateApplier<'static, MerkleCellsProvider>,
    ref_mc_state_handle: ManuallyDrop<RefMcStateHandle>,
}

impl MerkleUpdateApplier {
    fn new(
        epoch: u32,
        cell_storage: Arc<CellStorage>,
        ref_mc_state_handle: RefMcStateHandle,
    ) -> Self {
        Self {
            inner: ParMerkleUpdateApplier {
                new_cells: Default::default(),
                context: Cell::empty_context(),
                find_cell: MerkleCellsProvider {
                    epoch,
                    storage: cell_storage,
                },
                find_in_new_cells: true,
            },
            ref_mc_state_handle: ManuallyDrop::new(ref_mc_state_handle),
        }
    }
}

impl Drop for MerkleUpdateApplier {
    fn drop(&mut self) {
        // SAFETY: Drop is called only once.
        let ref_mc_state_handle = unsafe { ManuallyDrop::take(&mut self.ref_mc_state_handle) };

        // Replace with an empty dashmap but reduce the amount of shards in it.
        let new_cells = std::mem::replace(
            &mut self.0.new_cells,
            FastDashMap::with_capacity_and_hasher_and_shard_amount(0, Default::default(), 1),
        );

        // NOTE: Ensure that ref mc state handle outlives the cells map.
        Reclaimer::instance().drop((new_cells, ref_mc_state_handle));
    }
}

impl std::ops::Deref for MerkleUpdateApplier {
    type Target = ParMerkleUpdateApplier<'static, MerkleCellsProvider>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

struct MerkleCellsProvider {
    epoch: u32,
    storage: Arc<CellStorage>,
}

impl FindCell for MerkleCellsProvider {
    fn find_cell(&self, hash: &HashBytes) -> Option<Cell> {
        let cell = self.storage.load_cell(hash, self.epoch).ok()?;
        Some(Cell::from(cell as Arc<_>))
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

#[derive(Default, Debug, Clone, Copy)]
pub struct LoadStateHint {
    /// Allow applying merkle updates for at most the specified depth.
    /// Wait for previous state if `None`.
    pub fast_apply_depth: Option<u32>,
}

#[derive(Debug, Copy, Clone)]
pub struct ShardStateStorageMetrics {
    pub max_new_mc_cell_count: usize,
    pub max_new_sc_cell_count: usize,
}

#[derive(Default)]
struct StoreStateCounters {
    max_new_mc_cell_count: AtomicUsize,
    max_new_sc_cell_count: AtomicUsize,
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

pub enum StoreStateStatus {
    Stored {
        is_virtual: bool,
        state: ShardStateStuff,
    },
    Exists,
}
