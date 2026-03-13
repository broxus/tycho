use std::collections::hash_map;
use std::fs::File;
use std::io::Cursor;
use std::mem::ManuallyDrop;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

use anyhow::{Context, Result};
use bytes::Bytes;
use futures_util::FutureExt;
use futures_util::future::BoxFuture;
use tycho_block_util::block::*;
use tycho_block_util::dict::split_aug_dict_raw;
use tycho_block_util::state::*;
use tycho_storage::fs::TempFileStorage;
use tycho_storage::kv::StoredValue;
use tycho_types::merkle::{FindCell, MerkleUpdate, ParMerkleUpdateApplier};
use tycho_types::models::*;
use tycho_types::prelude::*;
use tycho_util::futures::Shared;
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
    // TODO: Use store state threshold
    #[expect(unused)]
    new_cells_threshold: usize,
    store_shard_state_step: NonZeroU32,

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

    pub async fn store_state_ignore_cache(
        &self,
        handle: &BlockHandle,
        state: &ShardStateStuff,
        hint: StoreStateHint,
    ) -> Result<()> {
        self.store_state_root_ignore_cache(
            handle,
            state.root_cell().clone(),
            state.ref_mc_state_handle().clone(),
            hint,
        )
        .await
    }

    pub async fn store_state_root_ignore_cache(
        &self,
        handle: &BlockHandle,
        root: Cell,
        ref_mc_state_handle: RefMcStateHandle,
        hint: StoreStateHint,
    ) -> Result<()> {
        let root_cell = DirectStoreRoot::Exact {
            root,
            ref_mc_state_handle,
        };
        let complete = Arc::new(AtomicBool::new(false));

        let f = self.make_store_state_root_direct_task(handle.clone(), root_cell, hint, complete);
        tokio::task::spawn_blocking(move || f(None)).await??;

        Ok(())
    }

    /// Stores a state for the specified block pair of blocks
    /// using their merkle update and and optional state root.
    ///
    /// ## How does it work
    ///
    /// State must be stored into [`CellsDb`] at least every [`store_shard_state_step`]
    /// blocks (where it is forced to be `1` for masterchain). When we first initialize
    /// the cache state for a shard we load the closest direct state for the target
    /// with all required "virtual" states. After that we decide how should the next
    /// state be stored (as "direct" or "virtual") and store a spawned task into the cache.
    ///
    /// [`store_shard_state_step`]: CoreStorageConfig::store_shard_state_step
    pub fn begin_store_next_state(
        self: &Arc<Self>,
        prev_handle: &BlockHandle,
        next_handle: &BlockHandle,
        merkle_update: &MerkleUpdate,
        state: Option<ShardStateStuff>,
        hint: StoreStateHint,
        get_merkle_update: Option<Box<FnGetBlockInfoForApply>>,
    ) -> Result<InitiatedStoreState> {
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
            return Ok(InitiatedStoreState::existing(next_handle, self));
        }

        let direct_store_step = if block_id.is_masterchain() {
            // store every masterchain state as direct
            NonZeroU32::MIN
        } else {
            self.store_shard_state_step
        };

        let store_type = if block_id.seqno.is_multiple_of(direct_store_step.get())
            || hint.is_top_block == Some(true)
        {
            StoreType::Direct
        } else {
            StoreType::Virtual
        };

        let mut cache = self.shard_states_cache.entry(block_id.shard).or_default();

        let spawn_cleanup = |task: PendingStoreTask| {
            let this = Arc::downgrade(self);
            let block_id = *block_id;
            tokio::task::spawn(async move {
                let (result, _) = task.await;

                let Some(this) = this.upgrade() else {
                    return;
                };

                this.shard_states_cache
                    .get_mut(&block_id.shard)
                    .expect("shard must not dissapear from cache")
                    .save_result(&block_id, result);
            });
        };

        let task = {
            let cache = &mut *cache;

            let prev_state_task = cache
                .states
                .get(&prev_block_id.root_hash)
                .map(|entry| entry.state.clone());

            let make_prev_task_fut = move || {
                let Some(cached) = prev_state_task else {
                    return self
                        .load_prev_state_root_no_cache(
                            next_handle.ref_by_mc_seqno(),
                            prev_block_id,
                            direct_store_step,
                            get_merkle_update,
                        )
                        .boxed();
                };

                Box::pin(async move {
                    match cached {
                        CachedState::Stored(res) => Ok(res),
                        CachedState::Failed(error) => Err(anyhow::Error::from(error)),
                        CachedState::Pending(task) => {
                            let (res, _) = task.await;
                            res.map_err(anyhow::Error::from)
                        }
                    }
                })
            };

            match cache.states.entry(block_id.root_hash) {
                hash_map::Entry::Occupied(entry) => match &entry.get().state {
                    CachedState::Pending(task) => task.clone(),
                    CachedState::Stored(res) => {
                        return Ok(InitiatedStoreState {
                            handle: next_handle.clone(),
                            pending: Some(futures_util::future::ok(res.state.clone()).boxed()),
                            storage: self.clone(),
                        });
                    }
                    CachedState::Failed(e) => return Err(e.clone().into()),
                },
                hash_map::Entry::Vacant(entry) => {
                    let complete = Arc::new(AtomicBool::new(false));
                    let is_virtual = matches!(store_type, StoreType::Virtual);
                    let partial_root = merkle_update.new.clone();
                    let task: PendingStoreTask = match store_type {
                        StoreType::Direct => {
                            let prev_task_fut;
                            let state_root = match state {
                                Some(state) => {
                                    prev_task_fut = None;
                                    DirectStoreRoot::Exact {
                                        root: state.root_cell().clone(),
                                        ref_mc_state_handle: state.ref_mc_state_handle().clone(),
                                    }
                                }
                                None => {
                                    prev_task_fut = Some(make_prev_task_fut());
                                    DirectStoreRoot::Next { partial_root }
                                }
                            };

                            let store_task = self.make_store_state_root_direct_task(
                                next_handle.clone(),
                                state_root,
                                hint,
                                complete.clone(),
                            );

                            Shared::new(Box::pin(async move {
                                let prev = match prev_task_fut {
                                    None => None,
                                    Some(fut) => Some(
                                        fut.await
                                            .context("previous state task failed (on direct)")?,
                                    ),
                                };
                                tokio::task::spawn_blocking(move || store_task(prev))
                                    .await?
                                    .context("direct state store failed")
                                    .map_err(StoreStateError::from)
                            }))
                        }
                        StoreType::Virtual => {
                            let prev_task_fut = make_prev_task_fut();
                            let store_task = self.make_store_state_root_virtual_task(
                                next_handle.clone(),
                                partial_root,
                                hint,
                                complete.clone(),
                            );

                            Shared::new(Box::pin(async move {
                                let prev = prev_task_fut
                                    .await
                                    .context("previous state task failed (on virtual)")?;
                                tokio::task::spawn_blocking(move || store_task(prev))
                                    .await?
                                    .context("virtual state store failed")
                                    .map_err(StoreStateError::from)
                            }))
                        }
                    };

                    spawn_cleanup(task.clone());

                    entry.insert(ShardStatesCacheItem {
                        prev_block_id: *prev_block_id,
                        block_id: *block_id,
                        is_virtual,
                        partial_root_cell: merkle_update.new.clone(),
                        state: CachedState::Pending(task.clone()),
                        complete,
                    });

                    task
                }
            }
        };

        metrics::gauge!(
            ShardStatesCache::METRIC_CACHE_SIZE,
            "workchain" => block_id.shard.workchain().to_string()
        )
        .set(clamp_u64_to_u32(cache.states.len() as _));

        drop(cache);

        Ok(InitiatedStoreState {
            handle: next_handle.clone(),
            pending: Some(Box::pin(async move {
                let (result, _) = task.await;
                Ok(result?.state)
            })),
            storage: self.clone(),
        })
    }

    fn load_prev_state_root_no_cache(
        &self,
        ref_by_mc_seqno: u32,
        block_id: &BlockId,
        max_tail: NonZeroU32,
        get_merkle_update: Option<Box<FnGetBlockInfoForApply>>,
    ) -> impl Future<Output = Result<StateWithApplier>> + Send + 'static {
        let block_id = *block_id;
        let block_handles = self.block_handle_storage.clone();
        let blocks = self.block_storage.clone();
        let cell_storage = self.cell_storage.clone();
        let tracker = self.min_ref_mc_state.clone();

        async move {
            let max_tail = max_tail.get() as usize - 1;

            let get_merkle_update: &FnGetBlockInfoForApply = match &get_merkle_update {
                Some(f) => f,
                None => &|_| None,
            };

            let mut to_apply = Vec::new();
            let mut pivot_block_id = block_id;
            let pivot = 'pivot: {
                while to_apply.len() <= max_tail {
                    let res = load_state_or_update(
                        ref_by_mc_seqno,
                        &pivot_block_id,
                        &block_handles,
                        &blocks,
                        &cell_storage,
                        &tracker,
                        get_merkle_update,
                    )
                    .await
                    .context("failed to load state or update on first access")?;

                    match res {
                        None => break,
                        Some(FromStorage::Virtual(f)) => {
                            to_apply.push(f.partial_root_cell);
                            pivot_block_id = f.prev_block_id;
                        }
                        Some(FromStorage::Applied(applied)) => {
                            break 'pivot Some(applied);
                        }
                    }
                }

                None
            };

            // Build the loaded state using the pivot and updates.
            let Some(StateWithApplier { state, applier }) = pivot else {
                anyhow::bail!(StateNotFound(pivot_block_id.as_short_id()));
            };

            // Fast path when there are no updates.
            if to_apply.is_empty() {
                anyhow::ensure!(state.block_id() == &block_id, "loaded state id mismatch");
                return Ok(StateWithApplier { state, applier });
            }

            let ref_mc_state_handle = state.ref_mc_state_handle().clone();
            let mut pivot_root = state.root_cell().clone();
            drop(state);

            while let Some(partial_new_root) = to_apply.pop() {
                pivot_root = applier
                    .make_next_state(partial_new_root)
                    .context("failed to apply next state for chain from storage")?;
            }

            let shard_state = pivot_root.parse::<Box<ShardStateUnsplit>>()?;
            let state = ShardStateStuff::from_state_and_root(
                &block_id,
                shard_state,
                pivot_root,
                ref_mc_state_handle,
            )?;
            Ok(StateWithApplier { state, applier })
        }
    }

    fn make_store_state_root_virtual_task(
        &self,
        handle: BlockHandle,
        partial_root: Cell,
        hint: StoreStateHint,
        complete: Arc<AtomicBool>,
    ) -> impl FnOnce(StateWithApplier) -> Result<StateWithApplier> + Send + 'static {
        let block_handles = self.block_handle_storage.clone();

        let complete_on_drop = scopeguard::guard(complete, |c| c.store(true, Ordering::Release));

        move |StateWithApplier { applier, state }| {
            let _guard = complete_on_drop;
            let _hist = HistogramGuard::begin("tycho_storage_cell_virtual_store_time_high");

            debug_assert_eq!(
                applier.shard(),
                handle.id().shard,
                "applier must always be created for the same shard"
            );
            debug_assert!(
                applier.pivot_block_seqno() < handle.id().seqno,
                "cannot use applier for the future"
            );

            let state = state.par_make_next_state(handle.id(), partial_root, &applier)?;
            block_handles.set_has_virtual_shard_state(&handle);

            applier.add_new_virtual_cells(hint.new_cell_count());

            Ok::<_, anyhow::Error>(StateWithApplier { state, applier })
        }
    }

    /// Spawns a task for storing the specified root into the storage.
    ///
    /// Returns `false` if that was already present in storage.
    fn make_store_state_root_direct_task(
        &self,
        handle: BlockHandle,
        root_cell: DirectStoreRoot,
        hint: StoreStateHint,
        complete: Arc<AtomicBool>,
    ) -> impl FnOnce(Option<StateWithApplier>) -> Result<StateWithApplier> + Send + 'static {
        let cells_db = self.cells_db.clone();
        let cell_storage = self.cell_storage.clone();
        let block_handles = self.block_handle_storage.clone();
        let shard_split_depth = self.shard_split_depth;
        let counters = self.counters.clone();
        let gc_lock = self.gc_lock.clone();

        let complete_on_drop = scopeguard::guard(complete, |c| c.store(true, Ordering::Release));

        move |prev| {
            let guard = complete_on_drop;

            let block_id = handle.id();
            let tracker;
            let root_hash = match &root_cell {
                DirectStoreRoot::Exact {
                    root,
                    ref_mc_state_handle,
                } => {
                    tracker = ref_mc_state_handle.tracker().clone();
                    *root.repr_hash()
                }
                DirectStoreRoot::Next { partial_root } => {
                    let prev = prev
                        .as_ref()
                        .expect("prev must be specified when storing next direct state");
                    tracker = prev.applier.ref_mc_state_handle().tracker().clone();
                    *partial_root.hash(0)
                }
            };

            let load_existing_state = || {
                let epoch = handle.ref_by_mc_seqno();
                let root = cell_storage.load_cell(&root_hash, epoch)?;
                let root = Cell::from(root as Arc<_>);

                track_max_epoch(epoch);

                let shard_state = root
                    .parse::<Box<ShardStateUnsplit>>()
                    .with_context(|| format!("failed to parse existing state: {block_id}"))?;
                let handle = tracker.insert(&shard_state);

                let state = ShardStateStuff::from_state_and_root(
                    block_id,
                    shard_state,
                    root,
                    handle.clone(),
                )?;

                let applier =
                    MerkleUpdateApplier::new(epoch, block_id, cell_storage.clone(), handle);

                Ok::<_, anyhow::Error>(StateWithApplier { state, applier })
            };

            // Fast path if already exists (before a possibly long apply).
            if handle.has_state() {
                return load_existing_state();
            }

            // Resolve root if needed.
            let virtual_cell_count;
            let prev_ref_mc_state_handle;
            let root_cell = match root_cell {
                // We already know the state so use it.
                DirectStoreRoot::Exact {
                    root,
                    ref_mc_state_handle,
                } => {
                    virtual_cell_count = 0;
                    prev_ref_mc_state_handle = ref_mc_state_handle;
                    root
                }
                // The most common case when we just use the applier to get the next state.
                // Applier MUST be created for a block which was BEFORE the current.
                DirectStoreRoot::Next { partial_root } => {
                    let prev = prev
                        .as_ref()
                        .expect("prev must be specified when storing next direct state");
                    debug_assert_eq!(
                        prev.applier.shard(),
                        handle.id().shard,
                        "applier must always be created for the same shard"
                    );
                    debug_assert!(
                        prev.applier.pivot_block_seqno() < handle.id().seqno,
                        "cannot use applier for the future"
                    );
                    virtual_cell_count = prev.applier.new_virtual_cells();
                    prev_ref_mc_state_handle = prev.applier.ref_mc_state_handle().clone();
                    prev.applier
                        .make_next_state(partial_root)
                        .context("failed to make next direct state")?
                }
            };
            drop(prev);

            // Fast path if already exists (before a possibly long gc lock).
            if handle.has_state() {
                return load_existing_state();
            }

            // Wait for GC lock.
            let gc_lock = {
                let _hist = HistogramGuard::begin("tycho_storage_cell_gc_lock_store_time_high");
                gc_lock.blocking_lock()
            };

            // Fast path if already exists (after a possibly long gc lock).
            if handle.has_state() {
                return load_existing_state();
            }

            // Build store cell transaction.
            let estimated_merkle_update_size = virtual_cell_count + hint.new_cell_count();
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
        }
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

    /// Loads (or computes) a state for the specified block.
    pub fn load_state(
        &self,
        ref_by_mc_seqno: u32,
        block_id: &BlockId,
    ) -> impl Future<Output = Result<ShardStateStuff>> {
        self.load_state_ext(ref_by_mc_seqno, block_id, Default::default(), |_| None)
    }

    /// Loads (or computes) a state for the specified block
    /// using an optional cache of yet unsaved blocks.
    ///
    /// ## How does it work
    ///
    /// State is stored into [`CellsDb`] at least every [`store_shard_state_step`]
    /// blocks. So when we load a state we must find some pivot state first and then
    /// apply required intermediate merkle updates to it. We know the maximum
    /// number of such updates so we can tell when the state is absent.
    ///
    /// Merkle updates are loaded either using some external `get_merkle_update`
    /// or from the storage.
    ///
    /// ## Store/load operation reuse
    ///
    /// Saving states takes some time. So while loading a state we can
    /// attach to the original "saving future" and use its result.
    /// We will always attach to the "virtual" state future because
    /// otherwise we will be doing the same work. And we attach to
    /// "direct" states when their future is either complete, or
    /// we are deep enough in our apply routine.
    ///
    /// [`store_shard_state_step`]: CoreStorageConfig::store_shard_state_step
    pub async fn load_state_ext<F>(
        &self,
        ref_by_mc_seqno: u32,
        block_id: &BlockId,
        hint: LoadStateHint,
        get_merkle_update: F,
    ) -> Result<ShardStateStuff>
    where
        F: Fn(&BlockId) -> Option<BlockInfoForApply>,
    {
        fn load_failed(error: StoreStateError) -> anyhow::Error {
            anyhow::anyhow!("unable to load a state that failed to save with error: {error:?}")
        }

        let try_load_from_storage = async |block_id: &BlockId| {
            load_state_or_update(
                ref_by_mc_seqno,
                block_id,
                &self.block_handle_storage,
                &self.block_storage,
                &self.cell_storage,
                &self.min_ref_mc_state,
                &get_merkle_update,
            )
            .await
        };

        let max_tail = self.store_shard_state_step.get() as usize;

        let mut pivot_block_id = *block_id;
        let mut to_apply = Vec::new();
        let pivot = 'pivot: {
            while to_apply.len() <= max_tail {
                if let Some(cache) = self.shard_states_cache.get(&pivot_block_id.shard)
                    && let Some(item) = cache.states.get(&pivot_block_id.root_hash)
                {
                    // We found a pending operation for that block id so we can try to reuse its state.
                    match &item.state {
                        // State is in cache and already loaded.
                        CachedState::Stored(stored) => {
                            break 'pivot Some(stored.clone());
                        }
                        // State is in cache but its store was unsuccessful.
                        CachedState::Failed(error) => return Err(load_failed(error.clone())),
                        // We can wait for the result if:
                        // - We explicitly hinted to wait for the operation.
                        // - The state is already virtual (we will be doing the same work so there
                        //   is no need to do it twice).
                        // - The pending task is already complete.
                        // - There are too many pending states.
                        // - No previous state in cache.
                        CachedState::Pending(task)
                            if !hint.allow_ignore_direct
                                || item.is_virtual
                                || item.complete.load(Ordering::Acquire)
                                || to_apply.len() >= max_tail
                                || !cache.states.contains_key(&item.prev_block_id.root_hash) =>
                        {
                            let task = task.clone();
                            drop(cache);

                            let (result, _) = task.await;
                            break 'pivot Some(result.map_err(load_failed)?);
                        }
                        // Otherwise we assume that applying merkle updates will be faster.
                        // Continue searching back for the first item with "pending virtual"
                        // or "stored" state (or we will find something in storage).
                        CachedState::Pending(_) => {
                            to_apply.push(item.partial_root_cell.clone());
                            pivot_block_id = item.prev_block_id;
                        }
                    }
                } else {
                    // NOTE: `cache` must be dropped here (we rely on Rust edition 2024 behavior).

                    // There was no such state in cache so we search in storage.
                    match try_load_from_storage(&pivot_block_id).await? {
                        // No handle or provided state for this id means that we can stop here.
                        None => break,
                        // Only merkle update was found for this block.
                        Some(FromStorage::Virtual(f)) => {
                            to_apply.push(f.partial_root_cell);
                            pivot_block_id = f.prev_block_id;
                        }
                        // A directly stored state was found for this block so we
                        // can use it as a pivot.
                        Some(FromStorage::Applied(applied)) => {
                            break 'pivot Some(applied);
                        }
                    }
                }
            }

            // No pivot states were found in cache or storage.
            None
        };

        // Build the loaded state using the pivot and updates.
        let Some(StateWithApplier { state, applier }) = pivot else {
            anyhow::bail!(StateNotFound(pivot_block_id.as_short_id()));
        };

        // Fast path when there are no updates.
        if to_apply.is_empty() {
            anyhow::ensure!(state.block_id() == block_id, "loaded state id mismatch");
            return Ok(state);
        }

        let ref_mc_state_handle = state.ref_mc_state_handle().clone();
        let mut pivot_root = state.root_cell().clone();
        drop(state);

        // Full case with applies.
        let pivot_root = rayon_run(move || {
            while let Some(partial_new_root) = to_apply.pop() {
                pivot_root = applier.make_next_state(partial_new_root)?;
            }
            Ok::<_, anyhow::Error>(pivot_root)
        })
        .await?;

        let shard_state = pivot_root.parse::<Box<ShardStateUnsplit>>()?;
        ShardStateStuff::from_state_and_root(block_id, shard_state, pivot_root, ref_mc_state_handle)
    }

    pub fn load_state_root_hash(&self, block_id: &BlockId) -> Result<HashBytes> {
        load_state_root_hash(&self.cells_db, block_id)
    }

    pub fn load_state_root_hash_opt(&self, block_id: &BlockId) -> Result<Option<HashBytes>> {
        load_state_root_hash_opt(&self.cells_db, block_id)
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

    #[cfg(test)]
    fn contains_state(&self, block_id: &BlockId) -> Result<bool> {
        let shard_states = &self.cells_db.shard_states;
        Ok(shard_states.get(block_id.to_vec())?.is_some())
    }
}

fn load_state_by_hash(
    ref_by_mc_seqno: u32,
    block_id: &BlockId,
    root_hash: &HashBytes,
    cell_storage: &Arc<CellStorage>,
    tracker: &MinRefMcStateTracker,
) -> Result<ShardStateStuff> {
    let root = cell_storage.load_cell(root_hash, ref_by_mc_seqno)?;
    let root = Cell::from(root as Arc<_>);

    track_max_epoch(ref_by_mc_seqno);
    let shard_state = root.parse::<Box<ShardStateUnsplit>>()?;
    let handle = tracker.insert(&shard_state);
    ShardStateStuff::from_state_and_root(block_id, shard_state, root, handle)
}

fn load_state_root_hash(cells_db: &CellsDb, block_id: &BlockId) -> Result<HashBytes> {
    match load_state_root_hash_opt(cells_db, block_id)? {
        Some(hash) => Ok(hash),
        None => anyhow::bail!(StateNotFound(block_id.as_short_id())),
    }
}

fn load_state_root_hash_opt(cells_db: &CellsDb, block_id: &BlockId) -> Result<Option<HashBytes>> {
    let Some(root) = cells_db.shard_states.get(block_id.to_vec())? else {
        return Ok(None);
    };
    Ok(Some(HashBytes::from_slice(&root[..32])))
}

enum FromStorage {
    Applied(StateWithApplier),
    Virtual(BlockInfoForApply),
}

async fn load_state_or_update<F>(
    ref_by_mc_seqno: u32,
    block_id: &BlockId,
    block_handles: &BlockHandleStorage,
    blocks: &BlockStorage,
    cell_storage: &Arc<CellStorage>,
    tracker: &MinRefMcStateTracker,
    get_merkle_update: F,
) -> Result<Option<FromStorage>>
where
    F: Fn(&BlockId) -> Option<BlockInfoForApply>,
{
    if let Some(root_hash) = load_state_root_hash_opt(cell_storage.db(), block_id)? {
        let state =
            load_state_by_hash(ref_by_mc_seqno, block_id, &root_hash, cell_storage, tracker)?;
        let ref_mc_state_handle = state.ref_mc_state_handle().clone();
        return Ok(Some(FromStorage::Applied(StateWithApplier {
            state,
            applier: MerkleUpdateApplier::new(
                ref_by_mc_seqno,
                block_id,
                cell_storage.clone(),
                ref_mc_state_handle,
            ),
        })));
    }

    let handle = match block_handles.load_handle(block_id) {
        Some(handle) => handle,
        None => return Ok(get_merkle_update(block_id).map(FromStorage::Virtual)),
    };

    let mut res = get_merkle_update(block_id);
    if res.is_none() {
        let block = blocks.load_block_data(&handle).await?;
        let merkle_update = block.as_ref().state_update.load()?;

        let (prev_id, prev_id_alt) = block.construct_prev_id()?;
        anyhow::ensure!(
            prev_id_alt.is_none(),
            "split/merge is not supported for now"
        );

        res = Some(BlockInfoForApply {
            prev_block_id: prev_id,
            partial_root_cell: merkle_update.new,
        });
    }

    Ok(res.map(FromStorage::Virtual))
}

pub struct InitiatedStoreState {
    handle: BlockHandle,
    pending: Option<BoxFuture<'static, Result<ShardStateStuff>>>,
    storage: Arc<ShardStateStorage>,
}

impl InitiatedStoreState {
    fn existing(handle: &BlockHandle, storage: &Arc<ShardStateStorage>) -> Self {
        Self {
            handle: handle.clone(),
            pending: None,
            storage: storage.clone(),
        }
    }

    pub fn handle(&self) -> &BlockHandle {
        &self.handle
    }

    pub async fn wait_store_only(self) -> Result<()> {
        if let Some(task) = self.pending {
            task.await?;
        }
        Ok(())
    }

    pub async fn wait_reload(self) -> Result<ShardStateStuff> {
        match self.pending {
            None => {
                self.storage
                    .load_state(self.handle.ref_by_mc_seqno(), self.handle.id())
                    .await
            }
            Some(pending) => pending.await,
        }
    }
}

fn track_max_epoch(epoch: u32) {
    // NOTE: only for metrics.
    static MAX_KNOWN_EPOCH: AtomicU32 = AtomicU32::new(0);

    let max_known_epoch = MAX_KNOWN_EPOCH
        .fetch_max(epoch, Ordering::Relaxed)
        .max(epoch);
    metrics::gauge!("tycho_storage_state_max_epoch").set(max_known_epoch);
}

#[derive(Default)]
struct ShardStatesCache {
    pivot_block_seqno: u32,
    states: FastHashMap<HashBytes, ShardStatesCacheItem>,
}

impl ShardStatesCache {
    const METRIC_PIVOT_SEQNO: &str = "tycho_storage_state_shard_cache_pivot_seqno";
    const METRIC_CACHE_SIZE: &str = "tycho_storage_state_shard_cache_size";

    fn save_result(&mut self, block_id: &BlockId, result: StoreTaskResult) {
        let Some(item) = self.states.get_mut(&block_id.root_hash) else {
            return;
        };

        match result {
            Ok(res) => {
                let pivot_block_seqno = res.applier.pivot_block_seqno();
                item.state = CachedState::Stored(res);

                // Reset cache tail on each saved block.
                if !item.is_virtual && pivot_block_seqno > self.pivot_block_seqno {
                    self.pivot_block_seqno = pivot_block_seqno;
                    self.states
                        .retain(|_, item| item.block_id.seqno >= pivot_block_seqno);

                    let labels = [("workchain", block_id.shard.workchain().to_string())];
                    metrics::gauge!(Self::METRIC_PIVOT_SEQNO, &labels).set(pivot_block_seqno);
                    metrics::gauge!(Self::METRIC_CACHE_SIZE, &labels)
                        .set(clamp_u64_to_u32(self.states.len() as _));
                }
            }
            Err(error) => {
                tracing::error!(%block_id, "store state failed: {error:?}");
                item.state = CachedState::Failed(error);
            }
        }
    }
}

enum DirectStoreRoot {
    Exact {
        root: Cell,
        ref_mc_state_handle: RefMcStateHandle,
    },
    Next {
        partial_root: Cell,
    },
}

struct ShardStatesCacheItem {
    prev_block_id: BlockId,
    block_id: BlockId,
    is_virtual: bool,
    partial_root_cell: Cell,
    state: CachedState,
    complete: Arc<AtomicBool>,
}

impl Drop for ShardStatesCacheItem {
    fn drop(&mut self) {
        Reclaimer::instance().drop(std::mem::take(&mut self.partial_root_cell));
    }
}

#[derive(Clone)]
enum CachedState {
    Pending(PendingStoreTask),
    Stored(StateWithApplier),
    Failed(StoreStateError),
}

#[derive(Clone)]
struct StateWithApplier {
    state: ShardStateStuff,
    applier: Arc<MerkleUpdateApplier>,
}

type PendingStoreTask = Shared<BoxFuture<'static, StoreTaskResult>>;
type StoreTaskResult = Result<StateWithApplier, StoreStateError>;

#[derive(Clone, thiserror::Error)]
#[error(transparent)]
struct StoreStateError(Arc<anyhow::Error>);

impl std::fmt::Debug for StoreStateError {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self.0.as_ref(), f)
    }
}

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
struct MerkleUpdateApplier(ManuallyDrop<MerkleUpdateApplierInner>);

struct MerkleUpdateApplierInner {
    shard: ShardIdent,
    pivot_block_seqno: u32,
    applier: ParMerkleUpdateApplier<'static, MerkleCellsProvider>,
    ref_mc_state_handle: RefMcStateHandle,
    new_virtual_cells: AtomicUsize,
    /// Separate atomic as a sum of all diffs.
    metric_total_new_cells: AtomicUsize,
}

impl MerkleUpdateApplier {
    const METRIC_ALIVE_APPLIERS: &str = "tycho_storage_state_applier_count";
    const METRIC_NEW_CELL_COUNT: &str = "tycho_storage_state_applier_all_new_cell_count";

    fn new(
        epoch: u32,
        pivot_block_id: &BlockId,
        cell_storage: Arc<CellStorage>,
        ref_mc_state_handle: RefMcStateHandle,
    ) -> Arc<Self> {
        let v = ALIVE_STATE_APPLIERS.fetch_add(1, Ordering::Relaxed) + 1;
        metrics::gauge!(Self::METRIC_ALIVE_APPLIERS).set(v);

        Arc::new(Self(ManuallyDrop::new(MerkleUpdateApplierInner {
            shard: pivot_block_id.shard,
            pivot_block_seqno: pivot_block_id.seqno,
            applier: ParMerkleUpdateApplier {
                new_cells: Default::default(),
                total_new_cells: Default::default(),
                context: Cell::empty_context(),
                find_cell: MerkleCellsProvider {
                    epoch,
                    storage: cell_storage,
                },
                find_in_new_cells: true,
            },
            ref_mc_state_handle,
            new_virtual_cells: AtomicUsize::new(0),
            metric_total_new_cells: AtomicUsize::new(0),
        })))
    }

    fn shard(&self) -> ShardIdent {
        self.0.shard
    }

    fn pivot_block_seqno(&self) -> u32 {
        self.0.pivot_block_seqno
    }

    fn add_new_virtual_cells(&self, count: usize) {
        self.0.new_virtual_cells.fetch_add(count, Ordering::Release);
    }

    fn new_virtual_cells(&self) -> usize {
        self.0.new_virtual_cells.load(Ordering::Acquire)
    }

    fn ref_mc_state_handle(&self) -> &RefMcStateHandle {
        &self.0.ref_mc_state_handle
    }

    fn make_next_state(&self, partial_new_root: Cell) -> Result<Cell, tycho_types::error::Error> {
        if let Some(cell) = self.find_cell.find_cell(partial_new_root.hash(0)) {
            return Ok(cell);
        }

        let new = rayon::scope(|scope| self.run(partial_new_root.as_ref(), 0, 0, Some(scope)))?;
        let res = new.resolve(Cell::empty_context());

        // Multiple applies can be run simultaneously so we must ensure
        // that only one diff is added to metrics.
        let diff = self.total_new_cells.swap(0, Ordering::Relaxed);
        self.0
            .metric_total_new_cells
            .fetch_add(diff, Ordering::Release);

        let v = NEW_CELL_COUNT
            .fetch_add(diff as u64, Ordering::Release)
            .saturating_add(diff as u64);
        metrics::gauge!(Self::METRIC_NEW_CELL_COUNT).set(clamp_u64_to_u32(v));

        res
    }
}

impl Drop for MerkleUpdateApplier {
    fn drop(&mut self) {
        // SAFETY: drop is called only once.
        let mut inner = unsafe { ManuallyDrop::take(&mut self.0) };

        // Explicitly drop new cells dictionary using the reclaimer.
        // NOTE: Ensure that ref mc state handle outlives the cells map.
        Reclaimer::instance().drop((inner.applier.new_cells, inner.ref_mc_state_handle));

        let v = ALIVE_STATE_APPLIERS.fetch_sub(1, Ordering::Relaxed) - 1;
        metrics::gauge!(Self::METRIC_ALIVE_APPLIERS).set(v);

        let diff = *inner.metric_total_new_cells.get_mut() as u64;
        let v = NEW_CELL_COUNT
            .fetch_sub(diff, Ordering::Release)
            .saturating_sub(diff);
        metrics::gauge!(Self::METRIC_NEW_CELL_COUNT).set(clamp_u64_to_u32(v));
    }
}

impl std::ops::Deref for MerkleUpdateApplier {
    type Target = ParMerkleUpdateApplier<'static, MerkleCellsProvider>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0.applier
    }
}

fn clamp_u64_to_u32(value: u64) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

static ALIVE_STATE_APPLIERS: AtomicU32 = AtomicU32::new(0);
static NEW_CELL_COUNT: AtomicU64 = AtomicU64::new(0);

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
    pub new_cell_count: Option<usize>,
    pub is_top_block: Option<bool>,
}

impl StoreStateHint {
    fn new_cell_count(&self) -> usize {
        match self.new_cell_count {
            None => estimate_cell_count(self.block_data_size),
            Some(count) => count,
        }
    }
}

fn estimate_cell_count(block_data_size: usize) -> usize {
    // y = 3889.9821 + 14.7480 × √x
    // R-squared: 0.7035
    ((3889.9821 + 14.7480 * (block_data_size as f64).sqrt()) as usize).next_power_of_two()
}

#[derive(Default, Debug, Clone, Copy)]
pub struct LoadStateHint {
    /// Allow applying merkle updates even when there is a pending
    /// direct store.
    pub allow_ignore_direct: bool,
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
#[error("shard state not found for block: {0}")]
pub struct StateNotFound(pub BlockIdShort);

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

#[derive(Debug, Clone)]
pub struct BlockInfoForApply {
    pub prev_block_id: BlockId,
    pub partial_root_cell: Cell,
}

type FnGetBlockInfoForApply = dyn Fn(&BlockId) -> Option<BlockInfoForApply> + Send + Sync + 'static;

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
            .store_state_ignore_cache(&handle, &top_state, Default::default())
            .await?;

        let prev_state = make_state(prev_id)?;
        let (handle, _) = handles.create_or_load_handle(&prev_id, NewBlockMeta {
            is_key_block: false,
            gen_utime: 0,
            ref_by_mc_seqno: prev,
        });
        states
            .store_state_ignore_cache(&handle, &prev_state, Default::default())
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
