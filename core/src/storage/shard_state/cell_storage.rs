//! Store/remove finalization publishes in-memory nursery/counter state before
//! the outer `RocksDB` write. Those writes use `expect`, and the release profile
//! has `panic = "abort"`, so a failed publish path is recovered by restart/WAL replay.

use std::cell::UnsafeCell;
use std::mem::{ManuallyDrop, MaybeUninit};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use bumpalo_herd::{Herd, Member};
use bytes::Bytes;
use hashbrown::hash_map;
use qfilter::Filter;
use quick_cache::sync::{Cache, DefaultLifecycle};
use rayon::{Scope, ThreadPool};
use triomphe::ThinArc;
use tycho_types::cell::*;
use tycho_util::metrics::{HistogramGuard, spawn_metrics_loop};
use tycho_util::{FastHashMap, FastHashSet};
use weedb::rocksdb::WriteBatch;
use weedb::{BoundedCfHandle, rocksdb};

use super::cell_nursery::{CellNursery, NurseryDelta, NurseryReadGuard};
use super::counters::{Counters, Idx, NextIdx};
use super::db_state::{CellsDbStateError, CellsDbStateKey, CountersStore};
use super::nursery_persistence::{NurseryPersistence, WalAppendProfile};
use super::row_format::{decode_indexed_value, encode_indexed_value};
use super::util::{BuildTrustedCellHasher, CellDashMap, CellHashMap, HashBytesKey, elapsed_us};
use crate::storage::shard_state::cell_nursery::NurseryEntryRecord;
use crate::storage::{CellsDb, CoreStorageConfig};

const PERSISTED_CELL_FILTER_MAX_CAPACITY: u64 = 2_000_000_000;
const PERSISTED_CELL_FILTER_FP_RATE: f64 = 0.001;

pub struct CellStorage {
    cells_db: CellsDb,
    cell_counters: Mutex<CountersStore>,
    pub(super) nursery: CellNursery,
    pub(super) nursery_persistence: Mutex<NurseryPersistence>,
    persisted_filter: Mutex<PersistedCellFilter>,
    cells_cache: Arc<CellsIndex>,
    raw_cells_cache: Arc<RawCellsCache>,
    worker_pool: Arc<ThreadPool>,
    drop_interval: u32,
    metrics: StorageMetrics,
}

type CellsIndex = CellDashMap<CachedCell>;

struct CachedCell {
    epoch: u32,
    weak: Weak<StorageCell>,
}

impl CellStorage {
    pub fn new(
        cells_db: CellsDb,
        cell_nursery_dir: PathBuf,
        cell_counters: CountersStore,
        worker_pool: Arc<ThreadPool>,
        config: &CoreStorageConfig,
    ) -> Result<Arc<Self>> {
        let cells_cache = Default::default();
        let raw_cells_cache = Arc::new(RawCellsCache::new(config.cells_cache_size.as_u64()));

        spawn_metrics_loop(
            &raw_cells_cache.clone(),
            Duration::from_secs(5),
            |c| async move { c.refresh_metrics() },
        );
        let persisted_filter = Self::build_persisted_filter(&cells_db)?;
        let (nursery_persistence, nursery) = NurseryPersistence::open(
            &cells_db,
            &cell_nursery_dir,
            config.cell_storage_threads,
            worker_pool.clone(),
            config.cell_nursery_checkpoint_wal_threshold.as_u64(),
        )?;
        nursery.record_metrics();
        persisted_filter.record_metrics();
        Ok(Arc::new(Self {
            cells_db,
            cell_counters: Mutex::new(cell_counters),
            nursery,
            nursery_persistence: Mutex::new(nursery_persistence),
            persisted_filter: Mutex::new(persisted_filter),
            cells_cache,
            raw_cells_cache,
            worker_pool,
            drop_interval: config.drop_interval,
            metrics: StorageMetrics::new(),
        }))
    }

    pub fn db(&self) -> &CellsDb {
        &self.cells_db
    }

    pub(super) fn prepare_persistent_state_save(&self) {
        // Serialize with store/remove finalization so the nursery snapshot,
        // WAL remove record, and in-memory publish describe the same entries.
        // This writes one WAL record, so an extremely large nursery must be
        // checkpointed/promoted before persistent-state save to stay below the
        // u32 WAL payload limit.
        let _cell_counters = self.cell_counters.lock().unwrap();
        let mut nursery_persistence = self.nursery_persistence.lock().unwrap();
        let entries = self.nursery.snapshot_entries();
        if entries.is_empty() {
            return;
        }

        let mut batch = WriteBatch::with_capacity_bytes(
            entries
                .iter()
                .map(|entry| size_of::<u64>() + entry.data.len())
                .sum(),
        );
        let mut commit = NurseryDelta::default();
        let mut buffer = Vec::with_capacity(512);
        let mut cell_bytes = 0usize;
        {
            let mut target = PersistedCellRowTarget {
                batch: &mut batch,
                commit: &mut commit,
                buffer: &mut buffer,
            };

            for entry in &entries {
                cell_bytes += entry.data.len();
                self.stage_persisted_cell_row(
                    &mut target,
                    entry.hash,
                    entry.idx,
                    entry.data.as_ref(),
                );
            }
        }

        nursery_persistence
            .append_commit(&self.cells_db, &mut batch, &commit)
            .expect("failed to append cell nursery WAL before persistent state save");
        // Persistent-state save reads cells from RocksDB, so nursery entries must
        // be promoted first. After the WAL append, a RocksDB failure is fatal:
        // restart recovery replays this branch from the WAL.
        self.cells_db
            .rocksdb()
            .write(batch)
            .expect("failed to flush cell nursery before persistent state save");

        if !commit.removes.is_empty() {
            let mut persisted_filter = self.persisted_filter.lock().unwrap();
            for hash in &commit.removes {
                persisted_filter.insert(hash);
            }
            persisted_filter.record_metrics();
        }

        self.nursery.apply_commit(commit);
        self.nursery.record_metrics();

        metrics::counter!("tycho_storage_cell_nursery_promoted_count")
            .increment(entries.len() as u64);
        metrics::counter!("tycho_storage_cell_nursery_promoted_bytes").increment(cell_bytes as u64);

        tracing::info!(
            cells = entries.len() as u64,
            bytes = cell_bytes as u64,
            "flushed cell nursery before persistent state save",
        );
    }

    pub fn apply_temp_cell(&self, root: &HashBytes) -> Result<()> {
        // Raw-state import assumes nursery entries were flushed first. This
        // path checks persisted data only; it intentionally does not consult
        // nursery while converting temporary cells into canonical rows.
        const MAX_NEW_CELLS_BATCH_SIZE: usize = 10000;

        struct TempCell {
            old_rc: u64,
            idx: Idx,
            additions: u32,
        }

        struct CellHashesIter<'a> {
            data: rocksdb::DBPinnableSlice<'a>,
            offset: usize,
            remaining_refs: u8,
        }

        impl Iterator for CellHashesIter<'_> {
            type Item = HashBytes;

            fn next(&mut self) -> Option<Self::Item> {
                if self.remaining_refs == 0 {
                    return None;
                }

                // NOTE: Unwrap is safe here because we have already checked
                // that data can contain all references.
                let item = HashBytes(self.data[self.offset..self.offset + 32].try_into().unwrap());

                self.remaining_refs -= 1;
                self.offset += 32;

                Some(item)
            }

            fn size_hint(&self) -> (usize, Option<usize>) {
                let r = self.remaining_refs as usize;
                (r, Some(r))
            }
        }

        enum InsertedCell<'a> {
            New(CellHashesIter<'a>),
            Existing,
        }

        struct Context<'a> {
            cells_cf: BoundedCfHandle<'a>,
            storage: &'a CellStorage,
            cell_counters: &'a mut CountersStore,
            persisted_filter: &'a mut PersistedCellFilter,
            buffer: Vec<u8>,
            transaction: CellHashMap<TempCell>,
            new_cells_batch: rocksdb::WriteBatch,
            new_cell_count: usize,
        }

        impl<'a> Context<'a> {
            fn new(
                storage: &'a CellStorage,
                cell_counters: &'a mut CountersStore,
                persisted_filter: &'a mut PersistedCellFilter,
            ) -> Self {
                Self {
                    cells_cf: storage.cells_db.cells.cf(),
                    storage,
                    cell_counters,
                    persisted_filter,
                    buffer: Vec::with_capacity(512),
                    transaction: Default::default(),
                    new_cells_batch: rocksdb::WriteBatch::default(),
                    new_cell_count: 0,
                }
            }

            fn load_temp(&self, key: &HashBytes) -> Result<CellHashesIter<'a>, CellStorageError> {
                let data = match self.storage.cells_db.temp_cells.get(key) {
                    Ok(Some(data)) => data,
                    Ok(None) => return Err(CellStorageError::CellNotFound),
                    Err(e) => return Err(CellStorageError::Internal(e)),
                };

                let (offset, remaining_refs) = {
                    let data = &mut data.as_ref();

                    let descriptor = CellDescriptor::new([data[0], data[1]]);
                    let byte_len = descriptor.byte_len() as usize;
                    let hash_count = descriptor.hash_count() as usize - 1;
                    let ref_count = descriptor.reference_count();

                    let offset = 6usize + byte_len + StorageCell::HASHES_ITEM_LEN * hash_count;
                    if data.len() < offset + (ref_count as usize) * 32 {
                        return Err(CellStorageError::InvalidCell);
                    }

                    (offset, ref_count)
                };

                Ok(CellHashesIter {
                    data,
                    offset,
                    remaining_refs,
                })
            }

            fn insert_cell(
                &mut self,
                key: &HashBytes,
            ) -> Result<InsertedCell<'a>, CellStorageError> {
                Ok(match self.transaction.entry(owned_hash_key(key)) {
                    hash_map::Entry::Occupied(mut entry) => {
                        entry.get_mut().additions += 1; // 1 new reference
                        InsertedCell::Existing
                    }
                    hash_map::Entry::Vacant(entry) => {
                        let existing = if self.persisted_filter.filter.contains(*key) {
                            self.storage.get_raw_idx_for_insert(key, usize::MAX)?
                        } else {
                            None
                        };
                        if let Some(idx) = existing {
                            entry.insert(TempCell {
                                idx,
                                old_rc: self.cell_counters.counters.get(idx),
                                additions: 1, // 1 new reference
                            });
                            return Ok(InsertedCell::Existing);
                        }

                        let idx = self.cell_counters.counters.alloc_idx();
                        entry.insert(TempCell {
                            idx,
                            old_rc: 0,
                            additions: 1,
                        });
                        let iter = self.load_temp(key)?;

                        encode_indexed_value(idx, iter.data.as_ref(), &mut self.buffer);

                        self.new_cells_batch
                            .put_cf(&self.cells_cf, key, self.buffer.as_slice());
                        self.persisted_filter.insert(key);

                        self.new_cell_count += 1;
                        if self.new_cell_count >= MAX_NEW_CELLS_BATCH_SIZE {
                            self.flush_new_cells()?;
                        }

                        InsertedCell::New(iter)
                    }
                })
            }

            fn flush_new_cells(&mut self) -> Result<(), rocksdb::Error> {
                if self.new_cell_count > 0 {
                    self.storage
                        .cells_db
                        .rocksdb()
                        .write(std::mem::take(&mut self.new_cells_batch))?;
                    self.new_cell_count = 0;
                }
                Ok(())
            }

            fn flush_existing_cells(self) -> Result<(), CellStorageError> {
                let mut batch = rocksdb::WriteBatch::default();
                let mut counter_batch = self.cell_counters.counters.begin();
                counter_batch.reserve(self.transaction.len());

                for (_, item) in self.transaction {
                    counter_batch.update_raw(
                        item.idx,
                        item.old_rc,
                        item.old_rc + u64::from(item.additions),
                    );
                }
                counter_batch.apply();
                self.cell_counters.counters.shrink_if_needed();

                self.cell_counters
                    .put_snapshot(&mut batch, CellsDbStateKey::CounterSnapshotLatest)
                    .map_err(CellStorageError::State)?;

                self.storage.cells_db.rocksdb().write(batch)?;
                Ok(())
            }
        }

        let mut cell_counters = self.cell_counters.lock().unwrap();
        let mut persisted_filter = self.persisted_filter.lock().unwrap();
        let mut ctx = Context::new(self, &mut cell_counters, &mut persisted_filter);

        {
            let mut stack = Vec::with_capacity(16);
            if let InsertedCell::New(iter) = ctx.insert_cell(root)? {
                stack.push(iter);
            }

            'outer: while let Some(iter) = stack.last_mut() {
                for ref child in iter {
                    if let InsertedCell::New(iter) = ctx.insert_cell(child)? {
                        stack.push(iter);
                        continue 'outer;
                    }
                }

                stack.pop();
            }
        }

        ctx.flush_new_cells()?;
        ctx.flush_existing_cells()?;

        Ok(())
    }

    pub fn store_cell_mt(
        &self,
        root: &DynCell,
        batch: &mut WriteBatch,
        split_at: FastHashMap<HashBytes, Cell>,
        capacity: usize,
        round: u32,
    ) -> Result<usize, CellStorageError> {
        type StoreResult = Result<(), CellStorageError>;

        struct StoreContext<'a> {
            storage: &'a CellStorage,
            next_idx: AtomicU64,
            /// Subtrees to process in parallel.
            split_at: FastHashMap<HashBytes, Cell>,
            /// Transaction items.
            transaction: CellDashMap<AddedCell>,
            /// References of detached subtrees.
            delayed_additions: std::sync::Mutex<CellHashMap<u32>>,
        }

        struct StoreReadContext<'a> {
            counters: &'a Counters,
            nursery: &'a NurseryReadGuard<'a>,
            persisted_filter: &'a PersistedCellFilter,
        }

        impl<'a> StoreContext<'a> {
            fn new(
                storage: &'a CellStorage,
                next_idx: NextIdx,
                split_accounts: FastHashMap<HashBytes, Cell>,
                capacity: usize,
            ) -> Self {
                let split_len = split_accounts.len();
                Self {
                    storage,
                    next_idx: AtomicU64::new(next_idx.get()),
                    split_at: split_accounts,
                    transaction: CellDashMap::with_capacity_and_hasher_and_shard_amount(
                        capacity,
                        Default::default(),
                        512,
                    ),
                    delayed_additions: std::sync::Mutex::new(
                        CellHashMap::with_capacity_and_hasher(split_len, Default::default()),
                    ),
                }
            }
        }

        impl<'a> StoreContext<'a> {
            fn traverse_cell<'scope>(
                &'scope self,
                read: &'scope StoreReadContext<'scope>,
                root: &'scope DynCell,
                scope: &Scope<'scope>,
            ) -> StoreResult {
                if !self.insert_cell(read, root.as_ref(), 0)? {
                    return Ok(());
                }

                let mut stack = Vec::with_capacity(16);
                stack.push(root.references());

                'outer: loop {
                    let depth = stack.len();
                    let Some(iter) = stack.last_mut() else {
                        break;
                    };

                    for child in &mut *iter {
                        // Skip cell to store it later in parallel
                        let child_hash = child.repr_hash();
                        if self.split_at.contains_key(child_hash) {
                            let mut delayed_additions = self.delayed_additions.lock().unwrap();
                            match delayed_additions.entry(owned_hash_key(child_hash)) {
                                hash_map::Entry::Vacant(entry) => {
                                    // This subtree will be added by another thread,
                                    // so no additions is needed on first occurence.
                                    entry.insert(0);
                                    drop(delayed_additions);

                                    // Spawn processing.
                                    // TODO: Handle error properly.
                                    scope.spawn(|scope| {
                                        self.traverse_cell(read, child, scope).unwrap();
                                    });
                                }
                                hash_map::Entry::Occupied(mut entry) => {
                                    // Other thread will add this subtree only once,
                                    // so we need to adjust references to keep them in sync.
                                    *entry.get_mut() += 1;
                                }
                            }

                            continue 'outer;
                        }

                        if self.insert_cell(read, child, depth)? {
                            stack.push(child.references());
                            continue 'outer;
                        }
                    }

                    stack.pop();
                }

                Ok(())
            }

            fn insert_cell(
                &self,
                read: &StoreReadContext<'_>,
                cell: &DynCell,
                depth: usize,
            ) -> Result<bool, CellStorageError> {
                use dashmap::mapref::entry::Entry;

                let key = cell.repr_hash();

                // Fast path: cell is already presented in this transaction, just bump refs.
                if let Some(mut value) = self.transaction.get_mut(hash_key(key)) {
                    value.additions += 1;
                    return Ok(false);
                }

                // Slow path: find an existing stored cell data.
                // NOTE: We dropped a dashmap lock on purpose so that parallel
                // threads can do this job without blocking each other on the
                // same shard (going to rocksdb might be slow).

                let existing = match read.nursery.get_idx(key) {
                    Some(idx) => Some(idx),
                    None if read.persisted_filter.filter.contains(*key) => {
                        self.storage.get_raw_idx_for_insert(key, depth)?
                    }
                    None => None,
                };

                // Try to insert once more.
                Ok(match self.transaction.entry(owned_hash_key(key)) {
                    Entry::Occupied(mut value) => {
                        // Some other thread has already inserted this cell.
                        // In this case we just bump refs.
                        value.get_mut().additions += 1;
                        false
                    }
                    Entry::Vacant(entry) => {
                        let (idx, old_rc, data) = if let Some(idx) = existing {
                            (idx, read.counters.get(idx), None)
                        } else {
                            let len = StorageCell::serialized_len(cell);

                            let mut bytes = Vec::with_capacity(len);
                            StorageCell::serialize_to(cell, &mut bytes)
                                .map_err(|_err| CellStorageError::InvalidCell)?;
                            debug_assert_eq!(bytes.len(), len);

                            let idx = Idx::new(self.next_idx.fetch_add(1, Ordering::Relaxed));
                            (idx, 0, Some(Bytes::from(bytes)))
                        };
                        let is_new = data.is_some();

                        // Add a new transaction entry.
                        entry.insert(AddedCell {
                            idx,
                            old_rc,
                            additions: 1,
                            data,
                        });
                        is_new
                    }
                })
            }

            fn finalize(
                self,
                batch: &mut WriteBatch,
                cell_counters: &mut CountersStore,
                nursery: &CellNursery,
                nursery_persistence: &mut NurseryPersistence,
                round: u32,
            ) -> Result<(usize, StoreFinalizeProfile), CellStorageError> {
                let mut profile = StoreFinalizeProfile::default();

                // Apply delayed additions before finalizing the transaction.
                for (hash, additions) in self.delayed_additions.into_inner().unwrap() {
                    if additions > 0 {
                        if let Some(mut item) = self.transaction.get_mut(&hash) {
                            // TODO: Assert that `item.additions == 1` at first?
                            item.additions += additions;
                        } else {
                            panic!("spawned subtree was not processed");
                        }
                    }
                }

                let total = self.transaction.len();
                let next_idx = NextIdx::new(self.next_idx.load(Ordering::Relaxed));

                cell_counters.counters.next_idx = next_idx;
                let mut counter_batch = cell_counters.counters.begin();
                counter_batch.reserve(total);
                let mut commit = NurseryDelta::default();
                commit.inserts.reserve(total);

                let started_at = Instant::now();
                for (key, item) in self.transaction {
                    match item.data {
                        Some(data) => {
                            debug_assert_eq!(item.old_rc, 0);
                            commit.inserts.push(NurseryEntryRecord {
                                hash: HashBytes(key.0),
                                idx: item.idx,
                                born_round: round,
                                data,
                            });
                            counter_batch.update_raw(item.idx, 0, u64::from(item.additions));
                        }
                        None => {
                            let new_rc = item.old_rc + u64::from(item.additions);
                            // Insert path only increases refs, so existing overrides cannot become ONE.
                            counter_batch.update_raw(item.idx, item.old_rc, new_rc);
                        }
                    }
                }
                profile.transaction_merge_us = elapsed_us(started_at);

                let mut buffer = Vec::with_capacity(512);
                let mut promoted_count = 0;
                let mut promoted_bytes = 0;
                let mut promoted_hashes = Vec::new();
                let started_at = Instant::now();
                // Raw cache is filled before the RocksDB write, matching the normal store path.
                // This keeps in-process loads working after promotion moved bytes out of nursery.
                {
                    let mut target = PersistedCellRowTarget {
                        batch,
                        commit: &mut commit,
                        buffer: &mut buffer,
                    };
                    nursery.drain_promotable(round, |hash, idx, data| {
                        promoted_count += 1;
                        promoted_bytes += data.len();
                        promoted_hashes.push(hash);
                        self.storage
                            .stage_persisted_cell_row(&mut target, hash, idx, data);
                    });
                }
                profile.promotion_us = elapsed_us(started_at);
                if promoted_count > 0 {
                    metrics::counter!("tycho_storage_cell_nursery_promoted_count")
                        .increment(promoted_count);
                    metrics::counter!("tycho_storage_cell_nursery_promoted_bytes")
                        .increment(promoted_bytes as u64);
                }
                // Keep the in-memory persisted filter in sync with promoted rows.
                // Without this batch update, a later store in the same process
                // can skip raw-cache/RocksDB lookup and duplicate a promoted cell.
                if !promoted_hashes.is_empty() {
                    let mut persisted_filter = self.storage.persisted_filter.lock().unwrap();
                    for hash in &promoted_hashes {
                        persisted_filter.insert(hash);
                    }
                    persisted_filter.record_metrics();
                }

                let started_at = Instant::now();
                counter_batch.apply();
                profile.counter_apply_us = elapsed_us(started_at);

                cell_counters.counters.shrink_if_needed();
                cell_counters
                    .put_snapshot(batch, CellsDbStateKey::CounterSnapshotLatest)
                    .expect("failed to stage store cell counter snapshot after mutation started");

                profile.wal = nursery_persistence
                    .append_commit(&self.storage.cells_db, batch, &commit)
                    .expect("failed to append cell nursery WAL after mutation started");

                let started_at = Instant::now();
                // Nursery publish intentionally happens before the caller writes the
                // RocksDB batch. If that write fails, the node restarts and replays
                // from WAL; state roots/counters are not persisted without the batch.
                self.storage.nursery.apply_commit(commit);
                self.storage.nursery.record_metrics();
                profile.nursery_apply_us = elapsed_us(started_at);

                Ok((total, profile))
            }
        }

        let total_started_at = Instant::now();
        let mut cell_counters = self.cell_counters.lock().unwrap();

        let ctx = StoreContext::new(self, cell_counters.counters.next_idx, split_at, capacity);

        let started_at = Instant::now();
        {
            let persisted_filter = self.persisted_filter.lock().unwrap();
            let nursery = self.nursery.read();
            let read = StoreReadContext {
                counters: &cell_counters.counters,
                nursery: &nursery,
                persisted_filter: &persisted_filter,
            };
            self.worker_pool
                .scope(|scope| ctx.traverse_cell(&read, root, scope))?;
        }
        let traverse_us = elapsed_us(started_at);

        let mut nursery_persistence = self.nursery_persistence.lock().unwrap();

        let started_at = Instant::now();
        let (total, finalize_profile) = ctx.finalize(
            batch,
            &mut cell_counters,
            &self.nursery,
            &mut nursery_persistence,
            round,
        )?;
        let finalize_us = elapsed_us(started_at);
        let wal = finalize_profile.wal;

        tracing::info!(
            operation = "store_cell_mt",
            total_us = elapsed_us(total_started_at),
            traverse_us,
            finalize_us,
            transaction_merge_us = finalize_profile.transaction_merge_us,
            promotion_us = finalize_profile.promotion_us,
            counter_apply_us = finalize_profile.counter_apply_us,
            nursery_apply_us = finalize_profile.nursery_apply_us,
            total_cells = total as u64,
            wal_bytes = wal.wal_bytes,
            wal_total_us = wal.total_us,
            "cell storage mt operation profile",
        );

        Ok(total)
    }

    pub fn load_cell(
        self: &Arc<Self>,
        hash: &HashBytes,
        epoch: u32,
    ) -> Result<Arc<StorageCell>, CellStorageError> {
        self.metrics.load_cell_total.increment(1);

        if let Some(cell) = self.cells_cache.get(hash_key(hash))
            && cell.epoch.saturating_add(self.drop_interval) >= epoch
            && let Some(cell) = cell.weak.upgrade()
        {
            // Tree cache stores already materialized cells for live state
            // objects. It is not an existence check against current RocksDB or
            // nursery contents; old loaded state can keep a removed cell alive.
            self.metrics.load_cell_cache_hit.increment(1);
            return Ok(cell);
        }

        let deserialize = |data: &[u8]| {
            StorageCell::deserialize(self.clone(), hash, data, epoch)
                .map(Arc::new)
                .ok_or(CellStorageError::InvalidCell)
        };

        self.metrics.load_cell_nursery_checked.increment(1);
        let nursery_data = self.nursery.get(hash);

        let cell = if let Some(data) = nursery_data {
            self.metrics.load_cell_nursery_hit.increment(1);
            deserialize(data.as_ref())?
        } else if let Some(value) = self.raw_cells_cache.inner.get(hash_key(hash)) {
            self.metrics.load_cell_raw_cache_hit.increment(1);
            deserialize(&value.slice)?
        } else {
            self.metrics.load_cell_raw_cache_miss.increment(1);

            let value = {
                #[cfg(feature = "cells-metrics")]
                let _timer = scopeguard::guard(Instant::now(), |started_at| {
                    self.raw_cells_cache
                        .rocksdb_access_histogram
                        .record(started_at.elapsed());
                });

                self.cells_db
                    .cells
                    .get(hash.as_slice())
                    .map_err(CellStorageError::Internal)?
            };

            match value.and_then(|value| {
                self.raw_cells_cache
                    .cache_decoded_value(hash, value.as_ref())
                    .map(|(_, item)| item)
            }) {
                Some(value) => {
                    self.metrics.load_cell_raw_hit.increment(1);
                    deserialize(&value.slice)?
                }
                None => {
                    self.metrics.load_cell_raw_miss.increment(1);
                    return Err(CellStorageError::CellNotFound);
                }
            }
        };

        let mut cell = cell;
        let has_new;
        match self.cells_cache.entry(owned_hash_key(hash)) {
            dashmap::Entry::Vacant(entry) => {
                has_new = true;
                entry.insert(CachedCell {
                    epoch,
                    weak: Arc::downgrade(&cell),
                });
            }
            dashmap::Entry::Occupied(mut entry) => {
                has_new = false;
                if entry.get().epoch >= epoch
                    && let Some(mut prev) = entry.get().weak.upgrade()
                {
                    drop(entry);
                    std::mem::swap(&mut prev, &mut cell);
                } else {
                    entry.insert(CachedCell {
                        epoch,
                        weak: Arc::downgrade(&cell),
                    });
                }
            }
        };

        if has_new {
            #[cfg(feature = "cells-metrics")]
            self.metrics.tree_cache_size.increment(1.0);
        }

        Ok(cell)
    }

    pub fn remove_cell_mt(
        &self,
        herd: &Herd,
        root: &HashBytes,
        split_at: FastHashSet<HashBytes>,
    ) -> Result<(usize, WriteBatch), CellStorageError> {
        type RemoveResult = Result<(), CellStorageError>;

        struct Alloc<'a> {
            bump: Member<'a>,
            buffer: Vec<HashBytes>,
        }

        struct RemoveContext<'a> {
            storage: &'a CellStorage,
            herd: &'a Herd,
            /// Subtrees to process in parallel.
            split_at: FastHashSet<HashBytes>,
            /// Transaction items.
            transaction: CellDashMap<RemovedCell<'a>>,
            /// References of detached subtrees.
            delayed_removes: std::sync::Mutex<CellHashMap<u32>>,
        }

        struct RemoveReadContext<'a> {
            counters: &'a Counters,
            nursery: &'a NurseryReadGuard<'a>,
        }

        impl<'a> RemoveContext<'a> {
            fn new(
                storage: &'a CellStorage,
                herd: &'a Herd,
                split_at: FastHashSet<HashBytes>,
            ) -> Self {
                let transaction_capacity = 128.max(split_at.len());
                let delayed_capacity = split_at.len();
                Self {
                    storage,
                    herd,
                    split_at,
                    transaction: CellDashMap::with_capacity_and_hasher_and_shard_amount(
                        transaction_capacity,
                        Default::default(),
                        512,
                    ),
                    delayed_removes: std::sync::Mutex::new(CellHashMap::with_capacity_and_hasher(
                        delayed_capacity,
                        Default::default(),
                    )),
                }
            }
        }

        impl<'a> RemoveContext<'a> {
            fn traverse_cell<'scope>(
                &'scope self,
                read: &'scope RemoveReadContext<'scope>,
                hash: &'scope HashBytes,
                scope: &Scope<'scope>,
            ) -> RemoveResult {
                let mut alloc = Alloc {
                    bump: self.herd.get(),
                    buffer: Vec::with_capacity(4),
                };

                let Some(refs) = self.remove_cell(read, hash, &mut alloc)? else {
                    return Ok(());
                };

                let mut stack = Vec::with_capacity(16);
                stack.push(refs.iter());

                // While some cells left
                'outer: while let Some(iter) = stack.last_mut() {
                    for child_hash in iter.by_ref() {
                        // Skip cell to remove it later in parallel
                        if self.split_at.contains(child_hash) {
                            let mut delayed_removes = self.delayed_removes.lock().unwrap();
                            match delayed_removes.entry(owned_hash_key(child_hash)) {
                                hash_map::Entry::Vacant(entry) => {
                                    // This subtree will be removed by another thread,
                                    // so no removes is needed on first occurrence.
                                    entry.insert(0);
                                    drop(delayed_removes);

                                    // Spawn processing.
                                    // TODO: Handle error properly.
                                    scope.spawn(|scope| {
                                        self.traverse_cell(read, child_hash, scope).unwrap();
                                    });
                                }
                                hash_map::Entry::Occupied(mut entry) => {
                                    // Other thread will remove this subtree only once,
                                    // so we need to adjust references to keep them in sync.
                                    *entry.get_mut() += 1;
                                }
                            }

                            continue 'outer;
                        }

                        // Process the current cell.
                        let refs = self.remove_cell(read, child_hash, &mut alloc)?;

                        if let Some(refs) = refs {
                            // And proceed to its refs if any.
                            stack.push(refs.iter());
                            continue 'outer;
                        }
                    }

                    stack.pop();
                }

                Ok(())
            }

            fn remove_cell(
                &self,
                read: &RemoveReadContext<'_>,
                repr_hash: &HashBytes,
                alloc: &mut Alloc<'a>,
            ) -> Result<Option<&'a [HashBytes]>, CellStorageError> {
                use dashmap::mapref::entry::Entry;

                // Fast path: cell is already presented in this transaction, just update refs.
                if let Some(mut value) = self.transaction.get_mut(hash_key(repr_hash)) {
                    return value.add_removes(1);
                }

                // Slow path: find an existing stored cell data.
                // NOTE: We dropped a dashmap lock on purpose so that parallel
                // threads can do this job without blocking each other on the
                // same shard (going to rocksdb might be slow).

                let buffer = &mut alloc.buffer;
                let (idx, source, data_len) = match read.nursery.get_data(repr_hash) {
                    Some((idx, data)) => {
                        buffer.clear();
                        if !StorageCell::deserialize_references(data, buffer) {
                            return Err(CellStorageError::InvalidCell);
                        }
                        (idx, CellSource::Nursery, data.len())
                    }
                    None => (
                        self.storage.get_raw_idx_for_delete(repr_hash, buffer)?,
                        CellSource::RocksDb,
                        0,
                    ),
                };
                let old_rc = read.counters.get(idx);

                // Try to remove once more.
                match self.transaction.entry(owned_hash_key(repr_hash)) {
                    // Some other thread has already removed this cell.
                    // In this case we just used the existing entry state.
                    Entry::Occupied(mut value) => value.get_mut().add_removes(1),
                    Entry::Vacant(v) => {
                        let refs = alloc.bump.alloc_slice_copy(buffer.as_slice());
                        v.insert(RemovedCell {
                            idx,
                            source,
                            data_len,
                            old_rc,
                            removes: 1,
                            refs,
                        });
                        Ok((old_rc == 1).then_some(refs))
                    }
                }
            }

            fn finalize(
                self,
                batch: &mut WriteBatch,
                cell_counters: &mut CountersStore,
                nursery_persistence: &mut NurseryPersistence,
            ) -> Result<(usize, RemoveMtFinalizeProfile), CellStorageError> {
                let _hist = HistogramGuard::begin("tycho_storage_batch_write_parallel_time_high");
                let mut profile = RemoveMtFinalizeProfile::default();

                // Write transaction to the `WriteBatch`
                // Apply delayed removes before finalizing the transaction.
                for (hash, removes) in self.delayed_removes.into_inner().unwrap() {
                    if removes > 0 {
                        if let Some(mut item) = self.transaction.get_mut(&hash) {
                            item.add_removes(removes)?;
                        } else {
                            panic!("spawned subtree was not processed");
                        }
                    }
                }

                let total = self.transaction.len();
                let mut counter_batch = cell_counters.counters.begin();
                counter_batch.reserve(total);
                let mut deletes = Vec::with_capacity(total);
                let mut commit = NurseryDelta::default();
                commit.removes.reserve(total);
                let mut discarded_before_promotion_count = 0;
                let mut discarded_before_promotion_bytes = 0;

                let started_at = Instant::now();
                for (key, item) in self.transaction {
                    let new_rc = item
                        .old_rc
                        .checked_sub(u64::from(item.removes))
                        .expect("checked in RemovedCell::remove");

                    counter_batch.update_raw(item.idx, item.old_rc, new_rc);

                    // Only rc=0 deletes the cell row; rc=1 keeps the row and clears the counter override.
                    if new_rc == 0 {
                        match item.source {
                            CellSource::Nursery => {
                                discarded_before_promotion_count += 1;
                                discarded_before_promotion_bytes += item.data_len as u64;
                                commit.removes.push(HashBytes(key.0));
                            }
                            CellSource::RocksDb => {
                                self.storage.raw_cells_cache.inner.remove(&key);
                                deletes.push(HashBytes(key.0));
                            }
                        }
                    }
                }
                profile.transaction_merge_us = elapsed_us(started_at);

                // Keep RocksDB skiplist deletes close to append order instead of hash-map order.
                profile.rocksdb_delete_count = deletes.len() as u64;
                deletes.sort_unstable();

                let cells_cf = &self.storage.cells_db.cells.cf();
                if !deletes.is_empty() {
                    let mut persisted_filter = self.storage.persisted_filter.lock().unwrap();
                    for hash in &deletes {
                        persisted_filter.filter.remove(*hash);
                    }
                    persisted_filter.record_metrics();
                }
                // TODO: Use `SingleDelete` here once vanilla rocksdb exposes
                // `WriteBatch::single_delete_cf`. This is the valid site: cell
                // keys are immutable hashes, and counter GC only reaches this
                // path after the last reference is gone. On devnet8 deploy-10kk,
                // `delete_cf` vs `single_delete_cf` had +2.77% compaction output
                // bytes, +5.41% RocksDB write time, and +7.89% write p99.
                for key in deletes {
                    batch.delete_cf(cells_cf, key.as_slice());
                }

                let started_at = Instant::now();
                counter_batch.apply();
                profile.counter_apply_us = elapsed_us(started_at);

                cell_counters.counters.shrink_if_needed();
                cell_counters
                    .put_snapshot(batch, CellsDbStateKey::CounterSnapshotLatest)
                    .expect("failed to stage remove cell counter snapshot after mutation started");

                profile.wal = nursery_persistence
                    .append_commit(&self.storage.cells_db, batch, &commit)
                    .expect("failed to append cell nursery WAL after remove mutation started");

                let started_at = Instant::now();
                // Nursery publish intentionally happens before the caller writes the
                // RocksDB batch. If that write fails, the node restarts and replays
                // from WAL; state roots/counters are not persisted without the batch.
                if discarded_before_promotion_count > 0 {
                    metrics::counter!(
                        "tycho_storage_cell_nursery_discarded_before_promotion_count"
                    )
                    .increment(discarded_before_promotion_count);
                    metrics::counter!(
                        "tycho_storage_cell_nursery_discarded_before_promotion_bytes"
                    )
                    .increment(discarded_before_promotion_bytes);
                }
                self.storage.nursery.apply_commit(commit);
                self.storage.nursery.record_metrics();
                profile.nursery_apply_us = elapsed_us(started_at);

                Ok((total, profile))
            }
        }

        let total_started_at = Instant::now();

        let mut cell_counters = self.cell_counters.lock().unwrap();

        let ctx = RemoveContext::new(self, herd, split_at);

        let started_at = Instant::now();
        {
            let nursery = self.nursery.read();
            let read = RemoveReadContext {
                counters: &cell_counters.counters,
                nursery: &nursery,
            };
            self.worker_pool
                .scope(|scope| ctx.traverse_cell(&read, root, scope))?;
        }
        let traverse_us = elapsed_us(started_at);

        let mut nursery_persistence = self.nursery_persistence.lock().unwrap();

        // NOTE: For each cell we have 32 bytes for key
        //       and a bit more just in case.
        let total = ctx.transaction.len();
        let batch_capacity_bytes = total * 32;
        let mut batch = WriteBatch::with_capacity_bytes(batch_capacity_bytes);

        let started_at = Instant::now();
        let (total, finalize_profile) =
            ctx.finalize(&mut batch, &mut cell_counters, &mut nursery_persistence)?;
        let finalize_us = elapsed_us(started_at);
        let wal = finalize_profile.wal;

        tracing::info!(
            operation = "remove_cell_mt",
            total_us = elapsed_us(total_started_at),
            traverse_us,
            finalize_us,
            transaction_merge_us = finalize_profile.transaction_merge_us,
            counter_apply_us = finalize_profile.counter_apply_us,
            nursery_apply_us = finalize_profile.nursery_apply_us,
            total_cells = total as u64,
            rocksdb_delete_count = finalize_profile.rocksdb_delete_count,
            wal_bytes = wal.wal_bytes,
            wal_total_us = wal.total_us,
            "cell storage mt operation profile",
        );

        Ok((total, batch))
    }

    pub fn drop_cell(&self, hash: &HashBytes) {
        if self
            .cells_cache
            .remove_if(hash_key(hash), |_, cell| cell.weak.strong_count() == 0)
            .is_some()
        {
            #[cfg(feature = "cells-metrics")]
            self.metrics.tree_cache_size.decrement(1.0);
        }
    }

    fn build_persisted_filter(cells_db: &CellsDb) -> Result<PersistedCellFilter> {
        let started_at = Instant::now();
        let estimated_capacity = cells_db
            .rocksdb()
            .property_int_value_cf(&cells_db.cells.cf(), rocksdb::properties::ESTIMATE_NUM_KEYS)?
            .unwrap_or_default();

        let mut filter =
            PersistedCellFilter::new(estimated_capacity, PERSISTED_CELL_FILTER_FP_RATE);
        let mut capacity = 0u64;
        let mut iterator = cells_db.cells.raw_iterator();
        iterator.seek_to_first();
        while let Some((key, _)) = iterator.item() {
            let hash = HashBytes::from_slice(key);
            filter.insert(&hash);
            capacity += 1;
            iterator.next();
        }
        iterator.status()?;

        tracing::info!(
            capacity,
            estimated_capacity,
            elapsed_sec = started_at.elapsed().as_secs_f64(),
            "built persisted cell filter",
        );
        Ok(filter)
    }

    // Previously used `get_value_or_guard` which inserts into cache on miss.
    // This caused two problems:
    fn get_raw_idx_for_insert(
        &self,
        key: &HashBytes,
        depth: usize,
    ) -> Result<Option<Idx>, CellStorageError> {
        // A constant which tells since which depth we should start to use cache.
        // This method is used mostly for inserting new states, so we can assume
        // that first N levels will mostly be new.
        //
        // This value was chosen empirically.
        const NEW_CELLS_DEPTH_THRESHOLD: usize = 4;

        if depth >= NEW_CELLS_DEPTH_THRESHOLD {
            // NOTE: `get` here is used to affect a "hotness" of the value, because
            // there is a big chance that we will need it soon during state processing.
            if let Some(entry) = self.raw_cells_cache.inner.get(hash_key(key)) {
                return Ok(Some(entry.header.header));
            }
        }

        let Some(value) = self
            .cells_db
            .cells
            .get(key)
            .map_err(CellStorageError::Internal)?
        else {
            return Ok(None);
        };

        if depth >= NEW_CELLS_DEPTH_THRESHOLD {
            let Some((idx, _)) = self
                .raw_cells_cache
                .cache_decoded_value(key, value.as_ref())
            else {
                return Err(CellStorageError::InvalidCell);
            };
            Ok(Some(idx))
        } else {
            let Some((idx, _)) = decode_indexed_value(value.as_ref()) else {
                return Err(CellStorageError::InvalidCell);
            };
            Ok(Some(idx))
        }
    }

    fn get_raw_idx_for_delete(
        &self,
        key: &HashBytes,
        refs_buffer: &mut Vec<HashBytes>,
    ) -> Result<Idx, CellStorageError> {
        refs_buffer.clear();

        // NOTE: `peek` here is used to avoid affecting a "hotness" of the value
        if let Some(value) = self.raw_cells_cache.inner.peek(hash_key(key)) {
            return StorageCell::deserialize_references(&value.slice, refs_buffer)
                .then_some(value.header.header)
                .ok_or(CellStorageError::InvalidCell);
        }

        let value = self
            .cells_db
            .cells
            .get(key.as_slice())
            .map_err(CellStorageError::Internal)?
            .ok_or(CellStorageError::CellNotFound)?;
        let (idx, data) =
            decode_indexed_value(value.as_ref()).ok_or(CellStorageError::InvalidCell)?;
        StorageCell::deserialize_references(data, refs_buffer)
            .then_some(idx)
            .ok_or(CellStorageError::InvalidCell)
    }

    fn stage_persisted_cell_row(
        &self,
        target: &mut PersistedCellRowTarget<'_>,
        hash: HashBytes,
        idx: Idx,
        data: &[u8],
    ) {
        target.commit.removes.push(hash);
        target.buffer.clear();
        encode_indexed_value(idx, data, target.buffer);
        target.batch.put_cf(
            &self.cells_db.cells.cf(),
            hash.as_slice(),
            target.buffer.as_slice(),
        );
        self.raw_cells_cache.inner.insert(
            owned_hash_key(&hash),
            RawCellsCacheItem::from_header_and_slice(idx, data),
        );
    }
}

struct StorageMetrics {
    tree_cache_size: metrics::Gauge,
    load_cell_total: metrics::Counter,
    load_cell_cache_hit: metrics::Counter,
    load_cell_nursery_checked: metrics::Counter,
    load_cell_nursery_hit: metrics::Counter,
    load_cell_raw_cache_hit: metrics::Counter,
    load_cell_raw_cache_miss: metrics::Counter,
    load_cell_raw_hit: metrics::Counter,
    load_cell_raw_miss: metrics::Counter,
}

impl StorageMetrics {
    fn new() -> Self {
        Self {
            tree_cache_size: metrics::gauge!("tycho_storage_cells_tree_cache_size"),
            load_cell_total: metrics::counter!("tycho_storage_load_cell_total"),
            load_cell_cache_hit: metrics::counter!("tycho_storage_load_cell_cache_hit_total"),
            load_cell_nursery_checked: metrics::counter!(
                "tycho_storage_load_cell_nursery_checked_total"
            ),
            load_cell_nursery_hit: metrics::counter!("tycho_storage_load_cell_nursery_hit_total"),
            load_cell_raw_cache_hit: metrics::counter!(
                "tycho_storage_load_cell_raw_cache_hit_total"
            ),
            load_cell_raw_cache_miss: metrics::counter!(
                "tycho_storage_load_cell_raw_cache_miss_total"
            ),
            load_cell_raw_hit: metrics::counter!("tycho_storage_load_cell_raw_hit_total"),
            load_cell_raw_miss: metrics::counter!("tycho_storage_load_cell_raw_miss_total"),
        }
    }
}

struct PersistedCellFilter {
    pub(super) filter: Filter,
}

impl PersistedCellFilter {
    fn new(capacity: u64, fp_rate: f64) -> Self {
        let capacity = capacity.clamp(1, PERSISTED_CELL_FILTER_MAX_CAPACITY);
        let filter = Filter::new_resizeable(capacity, PERSISTED_CELL_FILTER_MAX_CAPACITY, fp_rate)
            .unwrap_or_else(|err| panic!("failed to build persisted cell filter: {err}"));
        Self { filter }
    }

    fn insert(&mut self, hash: &HashBytes) {
        self.filter
            .insert_duplicated(hash)
            .unwrap_or_else(|err| panic!("failed to insert persisted cell filter entry: {err}"));
    }

    fn record_metrics(&self) {
        metrics::gauge!("tycho_storage_cell_persisted_filter_len").set(self.filter.len() as f64);
        metrics::gauge!("tycho_storage_cell_persisted_filter_capacity")
            .set(self.filter.capacity() as f64);
        metrics::gauge!("tycho_storage_cell_persisted_filter_memory_bytes")
            .set(self.filter.memory_usage() as f64);
        metrics::gauge!("tycho_storage_cell_persisted_filter_error_ratio")
            .set(self.filter.current_error_ratio());
    }
}

struct RemovedCell<'a> {
    idx: Idx,
    source: CellSource,
    data_len: usize,
    // Removal traversal needs old_rc to decide whether to descend into refs.
    // Keep it for finalization so counters do not decode the same idx again.
    old_rc: u64,
    removes: u32,
    refs: &'a [HashBytes],
}

impl<'a> RemovedCell<'a> {
    fn add_removes(&mut self, removes: u32) -> Result<Option<&'a [HashBytes]>, CellStorageError> {
        self.removes =
            self.removes
                .checked_add(removes)
                .ok_or(CellStorageError::CounterMismatch {
                    expected: self.old_rc,
                    actual: u32::MAX,
                })?;
        if u64::from(self.removes) > self.old_rc {
            return Err(CellStorageError::CounterMismatch {
                expected: self.old_rc,
                actual: self.removes,
            });
        }

        Ok((self.old_rc == u64::from(self.removes)).then_some(self.refs))
    }
}

struct AddedCell {
    idx: Idx,
    old_rc: u64,
    additions: u32,
    data: Option<Bytes>,
}

struct PersistedCellRowTarget<'a> {
    batch: &'a mut WriteBatch,
    commit: &'a mut NurseryDelta,
    buffer: &'a mut Vec<u8>,
}

#[derive(Clone, Copy)]
enum CellSource {
    Nursery,
    RocksDb,
}

#[derive(Default)]
struct StoreFinalizeProfile {
    transaction_merge_us: u64,
    promotion_us: u64,
    counter_apply_us: u64,
    nursery_apply_us: u64,
    wal: WalAppendProfile,
}

#[derive(Default)]
struct RemoveMtFinalizeProfile {
    transaction_merge_us: u64,
    counter_apply_us: u64,
    nursery_apply_us: u64,
    rocksdb_delete_count: u64,
    wal: WalAppendProfile,
}

#[inline]
fn hash_key(hash: &HashBytes) -> &HashBytesKey {
    // Keep map keys inline. Borrowed `&HashBytes` keys would copy less, but the
    // key locations would follow cell storage layout and make iteration random.
    HashBytesKey::wrap(hash.as_array())
}

#[inline]
fn owned_hash_key(hash: &HashBytes) -> HashBytesKey {
    HashBytesKey(*hash.as_array())
}

#[derive(thiserror::Error, Debug)]
pub enum CellStorageError {
    #[error("Cell not found in cell db")]
    CellNotFound,
    #[error("Invalid cell")]
    InvalidCell,
    #[error("Cell counter mismatch: expected refcount {expected}, got {actual} removes")]
    CounterMismatch { expected: u64, actual: u32 },
    #[error("Counters state error: {0}")]
    Counters(#[from] super::counters::CountersError),
    #[error("Cells state error: {0}")]
    State(#[from] CellsDbStateError),
    #[error("Internal rocksdb error")]
    Internal(#[from] rocksdb::Error),
}

pub struct StorageCell {
    cell_storage: Arc<CellStorage>,
    descriptor: CellDescriptor,
    bit_len: u16,
    data_ptr: *const u8,
    data_len: u8,
    other_hash_count: u8,
    epoch: u32,

    repr_depth: u16,
    repr_hash: HashBytes,

    reference_states: [AtomicU8; 4],
    reference_data: [UnsafeCell<StorageCellReferenceData>; 4],
}

impl StorageCell {
    const REF_EMPTY: u8 = 0x0;
    const REF_RUNNING: u8 = 0x1;
    const REF_STORAGE: u8 = 0x2;

    const HASHES_ITEM_LEN: usize = 32 + 2;
    const MAX_STORAGE_CELL_BYTES: usize = 6
        + MAX_BIT_LEN.div_ceil(8) as usize
        + Self::HASHES_ITEM_LEN * LevelMask::MAX_LEVEL as usize
        + 32 * MAX_REF_COUNT;

    pub fn deserialize(
        cell_storage: Arc<CellStorage>,
        repr_hash: &HashBytes,
        buffer: &[u8],
        epoch: u32,
    ) -> Option<Self> {
        if buffer.len() < 4 {
            return None;
        }

        let descriptor = CellDescriptor::new([buffer[0], buffer[1]]);
        let bit_len = u16::from_le_bytes([buffer[2], buffer[3]]);
        let repr_depth = u16::from_le_bytes([buffer[4], buffer[5]]);
        let level = descriptor.level_mask().level() as usize;
        let ref_count = descriptor.reference_count() as usize;

        let is_pruned = descriptor.is_exotic() && ref_count == 0 && level > 0;
        let other_hash_count = (!is_pruned) as usize * level;
        debug_assert!(other_hash_count <= 3);

        let byte_len = descriptor.byte_len() as usize;
        let allocated_len = byte_len + other_hash_count * Self::HASHES_ITEM_LEN;
        let total_len = 6usize + allocated_len + 32 * ref_count;
        if buffer.len() < total_len {
            return None;
        }

        let data_ptr = Box::into_raw(Box::<[u8]>::from(&buffer[6..6 + allocated_len])).cast::<u8>();

        let reference_states = Default::default();
        let mut reference_data = unsafe {
            MaybeUninit::<[UnsafeCell<StorageCellReferenceData>; 4]>::uninit().assume_init()
        };

        const { assert!(std::mem::size_of::<UnsafeCell<StorageCellReferenceData>>() == 32) };
        unsafe {
            std::ptr::copy_nonoverlapping(
                buffer.as_ptr().add(6 + allocated_len),
                reference_data.as_mut_ptr().cast::<u8>(),
                32 * ref_count,
            );
        }

        Some(Self {
            cell_storage,
            bit_len,
            descriptor,
            data_ptr,
            data_len: byte_len as u8,
            other_hash_count: other_hash_count as u8,
            epoch,
            repr_depth,
            repr_hash: *repr_hash,
            reference_states,
            reference_data,
        })
    }

    pub fn deserialize_references(data: &[u8], target: &mut Vec<HashBytes>) -> bool {
        if data.len() < 6 {
            return false;
        }

        let descriptor = CellDescriptor::new([data[0], data[1]]);
        let hash_count = descriptor.hash_count() - 1;
        let ref_count = descriptor.reference_count() as usize;

        let mut offset =
            6usize + descriptor.byte_len() as usize + Self::HASHES_ITEM_LEN * hash_count as usize;
        if data.len() < offset + 32 * ref_count {
            return false;
        }

        target.reserve(ref_count);
        for _ in 0..ref_count {
            target.push(HashBytes::from_slice(&data[offset..offset + 32]));
            offset += 32;
        }

        true
    }

    // NOTE: Repr hash is not stored into value because it is already a key.
    pub fn serialized_len(cell: &DynCell) -> usize {
        let descriptor = cell.descriptor();
        let level = descriptor.level_mask().level() as usize;
        let ref_count = descriptor.reference_count() as usize;

        let is_pruned = descriptor.is_exotic() && ref_count == 0 && level > 0;
        // Keep this in lockstep with `serialize_to`.
        let other_hash_count = (!is_pruned) as usize * level;

        6usize
            + descriptor.byte_len() as usize
            + Self::HASHES_ITEM_LEN * other_hash_count
            + 32 * ref_count
    }

    pub fn serialize_to(cell: &DynCell, target: &mut Vec<u8>) -> Result<()> {
        let descriptor = cell.descriptor();
        let level = descriptor.level_mask().level() as usize;
        let ref_count = descriptor.reference_count();

        let is_pruned = descriptor.is_exotic() && ref_count == 0 && level > 0;
        // The total amount of hashes is `1 + level`. We don't
        // need to store the `repr_hash` (hash for the highest level)
        // because it is already a key for this value. So we only
        // store `level` hashes for non-pruned cells, and no
        // hashes for pruned cells (they store everyting in data).
        let other_hash_count = (!is_pruned) as usize * level;

        target.reserve(
            6usize
                + descriptor.byte_len() as usize
                + Self::HASHES_ITEM_LEN * other_hash_count
                + 32 * ref_count as usize,
        );

        target.extend_from_slice(&[descriptor.d1, descriptor.d2]);
        target.extend_from_slice(&cell.bit_len().to_le_bytes());
        target.extend_from_slice(&cell.repr_depth().to_le_bytes());
        target.extend_from_slice(cell.data());
        assert_eq!(cell.data().len(), descriptor.byte_len() as usize);

        let len_before = target.len();
        for level in descriptor.level_mask().into_iter().take(other_hash_count) {
            target.extend_from_slice(cell.hash(level).as_array());
            target.extend_from_slice(&cell.depth(level).to_le_bytes());
        }
        debug_assert_eq!(
            (target.len() - len_before) / Self::HASHES_ITEM_LEN,
            other_hash_count
        );

        for i in 0..descriptor.reference_count() {
            let cell = cell.reference(i).context("Child not found")?;
            target.extend_from_slice(cell.repr_hash().as_array());
        }

        Ok(())
    }

    pub fn reference_raw(&self, index: u8) -> Option<&Arc<StorageCell>> {
        if index > 3 || index >= self.descriptor.reference_count() {
            return None;
        }

        let state = &self.reference_states[index as usize];
        let slot = self.reference_data[index as usize].get();

        let current_state = state.load(Ordering::Acquire);
        if current_state == Self::REF_STORAGE {
            return Some(unsafe { &(*slot).storage_cell });
        }

        let mut res = Ok(());
        Self::initialize_inner(state, &mut || match self
            .cell_storage
            .load_cell(unsafe { &(*slot).hash }, self.epoch)
        {
            Ok(cell) => unsafe {
                *slot = StorageCellReferenceData {
                    storage_cell: ManuallyDrop::new(cell),
                };
                true
            },
            Err(err) => {
                res = Err(err);
                false
            }
        });

        // TODO: just return none?
        res.unwrap();

        Some(unsafe { &(*slot).storage_cell })
    }

    /// Returns hash index, max level and whether the cell is pruned.
    #[inline]
    fn map_level(&self, level: u8) -> (u8, u8, bool) {
        let hash_index = self.descriptor.level_mask().hash_index(level);
        let max_level = self.descriptor.level_mask().level();
        let is_pruned = max_level > 0 && self.other_hash_count == 0;
        debug_assert!(hash_index <= max_level);
        (hash_index, max_level, is_pruned)
    }

    fn allocated_len(&self) -> usize {
        self.data_len as usize + (self.other_hash_count as usize) * Self::HASHES_ITEM_LEN
    }

    fn initialize_inner(state: &AtomicU8, init: &mut impl FnMut() -> bool) {
        struct Guard<'a> {
            state: &'a AtomicU8,
            new_state: u8,
        }

        impl Drop for Guard<'_> {
            fn drop(&mut self) {
                self.state.store(self.new_state, Ordering::Release);
                unsafe {
                    let key = self.state as *const AtomicU8 as usize;
                    parking_lot_core::unpark_all(key, parking_lot_core::DEFAULT_UNPARK_TOKEN);
                }
            }
        }

        loop {
            let exchange = state.compare_exchange_weak(
                Self::REF_EMPTY,
                Self::REF_RUNNING,
                Ordering::Acquire,
                Ordering::Acquire,
            );
            match exchange {
                Ok(_) => {
                    let mut guard = Guard {
                        state,
                        new_state: Self::REF_EMPTY,
                    };
                    if init() {
                        guard.new_state = Self::REF_STORAGE;
                    }
                    return;
                }
                Err(Self::REF_STORAGE) => return,
                Err(Self::REF_RUNNING) => unsafe {
                    let key = state as *const AtomicU8 as usize;
                    parking_lot_core::park(
                        key,
                        || state.load(Ordering::Relaxed) == Self::REF_RUNNING,
                        || (),
                        |_, _| (),
                        parking_lot_core::DEFAULT_PARK_TOKEN,
                        None,
                    );
                },
                Err(Self::REF_EMPTY) => (),
                Err(_) => debug_assert!(false),
            }
        }
    }
}

impl CellImpl for StorageCell {
    #[inline]
    fn untrack(self: CellInner<Self>) -> Cell {
        Cell::from(self)
    }

    fn descriptor(&self) -> CellDescriptor {
        self.descriptor
    }

    fn data(&self) -> &[u8] {
        // SAFETY: Data was not deallocated yet.
        unsafe { std::slice::from_raw_parts(self.data_ptr, self.data_len as usize) }
    }

    fn bit_len(&self) -> u16 {
        self.bit_len
    }

    fn reference(&self, index: u8) -> Option<&DynCell> {
        Some(self.reference_raw(index)?.as_ref())
    }

    fn reference_cloned(&self, index: u8) -> Option<Cell> {
        Some(Cell::from(self.reference_raw(index)?.clone() as Arc<_>))
    }

    fn reference_repr_hash(&self, index: u8) -> Option<HashBytes> {
        if index > 3 || index >= self.descriptor.reference_count() {
            return None;
        }

        let state = &self.reference_states[index as usize];
        let slot = self.reference_data[index as usize].get();

        // See https://github.com/Amanieu/seqlock/blob/b72653fd9c38141da7e588a97a1b654239f5d4df/src/lib.rs#L117-L150
        loop {
            let state_before = state.load(Ordering::Acquire);
            match state_before {
                Self::REF_STORAGE => {
                    // SAFETY: `REF_STORAGE` is the final state and is not going to change,
                    // so it is safe to assume that the slot is now a loaded storage cell.
                    return Some(unsafe { &(*slot).storage_cell }.repr_hash);
                }
                Self::REF_RUNNING => {
                    // Wait until the running operation is complete.
                    let key = state as *const AtomicU8 as usize;
                    unsafe {
                        parking_lot_core::park(
                            key,
                            || state.load(Ordering::Relaxed) == Self::REF_RUNNING,
                            || (),
                            |_, _| (),
                            parking_lot_core::DEFAULT_PARK_TOKEN,
                            None,
                        );
                    }
                    continue;
                }
                // Otherwise we are (were) at the `REF_EMPTY` state.
                _ => {}
            }

            // See https://github.com/rust-lang/rfcs/pull/3301
            // This "works" "fine", but is technically undefined behavior.

            // We need to use a volatile read here because the data may be
            // concurrently modified by a writer. We also use MaybeUninit in
            // case we read the data in the middle of a modification.
            let slot = unsafe {
                std::ptr::read_volatile(slot.cast::<MaybeUninit<StorageCellReferenceData>>())
            };

            // Make sure the state_after read occurs after reading the data.
            // What we ideally want is a load(Release), but the Release
            // ordering is not available on loads.
            std::sync::atomic::fence(Ordering::Acquire);

            // If the state is the same then the data wasn't modified
            // while we were reading it, and can be returned.
            let state_after = state.load(Ordering::Relaxed);
            if state_before == state_after {
                return Some(unsafe { slot.assume_init().hash });
            }
        }
    }

    fn virtualize(&self) -> &DynCell {
        VirtualCellWrapper::wrap(self)
    }

    fn hash(&self, level: u8) -> &HashBytes {
        let (hash_index, max_level, is_pruned) = self.map_level(level);
        if hash_index == max_level {
            &self.repr_hash
        } else {
            // Compute offset branchless.
            let offset = (is_pruned as usize) * (2 + hash_index as usize * 32)
                + (!is_pruned as usize)
                    * (self.data_len as usize + (hash_index as usize) * Self::HASHES_ITEM_LEN);

            debug_assert!(offset + 32 <= self.allocated_len());
            HashBytes::wrap(unsafe { &*self.data_ptr.add(offset).cast::<[u8; 32]>() })
        }
    }

    fn depth(&self, level: u8) -> u16 {
        let (hash_index, max_level, is_pruned) = self.map_level(level);
        if hash_index == max_level {
            self.repr_depth
        } else {
            // Compute offset branchless.
            let offset = (is_pruned as usize)
                * (2 + max_level as usize * 32 + hash_index as usize * 2)
                + (!is_pruned as usize)
                    * (self.data_len as usize + (hash_index as usize) * Self::HASHES_ITEM_LEN + 32);

            debug_assert!(offset + 2 <= self.allocated_len());
            let bytes = unsafe { *self.data_ptr.add(offset).cast::<[u8; 2]>() };
            if is_pruned {
                // NOTE: Pruned branches store embedded depths in big-endian (cell format),
                // while `StorageCell` stores depths in little-endian.
                // https://github.com/broxus/tycho-types/blob/863f612fc7f0ccd1fb2c2cd0a13747f92b3cadcc/src/cell/builder.rs#L760
                u16::from_be_bytes(bytes)
            } else {
                u16::from_le_bytes(bytes)
            }
        }
    }
}

impl Drop for StorageCell {
    fn drop(&mut self) {
        _ = unsafe {
            Box::from_raw(std::ptr::slice_from_raw_parts_mut(
                self.data_ptr.cast_mut(),
                self.allocated_len(),
            ))
        };

        self.cell_storage.drop_cell(DynCell::repr_hash(self));
        for i in 0..4 {
            let state = self.reference_states[i].load(Ordering::Acquire);
            let data = self.reference_data[i].get_mut();

            if state == Self::REF_STORAGE {
                let cell = unsafe { ManuallyDrop::take(&mut data.storage_cell) };
                if Arc::strong_count(&cell) == 1 {
                    SafeDeleter::retire(cell);
                }
            }
        }
    }
}

unsafe impl Send for StorageCell {}
unsafe impl Sync for StorageCell {}

const _: () = assert!(StorageCell::MAX_STORAGE_CELL_BYTES <= u16::MAX as usize);

pub union StorageCellReferenceData {
    /// Incplmete state.
    hash: HashBytes,
    /// Complete state.
    storage_cell: ManuallyDrop<Arc<StorageCell>>,
}

struct RawCellsCache {
    inner: Cache<HashBytesKey, RawCellsCacheItem, CellSizeEstimator, BuildTrustedCellHasher>,
    #[cfg(feature = "cells-metrics")]
    rocksdb_access_histogram: metrics::Histogram,
}

type RawCellsCacheItem = ThinArc<Idx, u8>;

#[derive(Clone, Copy)]
pub struct CellSizeEstimator;
impl quick_cache::Weighter<HashBytesKey, RawCellsCacheItem> for CellSizeEstimator {
    fn weight(&self, key: &HashBytesKey, val: &RawCellsCacheItem) -> u64 {
        const STATIC_SIZE: usize = std::mem::size_of::<RawCellsCacheItem>()
            + std::mem::size_of::<Idx>()
            + std::mem::size_of::<usize>() * 2; // ArcInner refs + HeaderWithLength length

        let len = key.0.len() + val.slice.len() + STATIC_SIZE;
        len as u64
    }
}

impl RawCellsCache {
    fn new(size_in_bytes: u64) -> Self {
        // Percentile 0.1%    from 96 to 127  => 1725119 count
        // Percentile 10%     from 128 to 191  => 82838849 count
        // Percentile 25%     from 128 to 191  => 82838849 count
        // Percentile 50%     from 128 to 191  => 82838849 count
        // Percentile 75%     from 128 to 191  => 82838849 count
        // Percentile 90%     from 192 to 255  => 22775080 count
        // Percentile 95%     from 192 to 255  => 22775080 count
        // Percentile 99%     from 192 to 255  => 22775080 count
        // Percentile 99.9%   from 256 to 383  => 484002 count
        // Percentile 99.99%  from 256 to 383  => 484002 count
        // Percentile 99.999% from 256 to 383  => 484002 count

        // from 64  to 95  - 15_267
        // from 96  to 127 - 1_725_119
        // from 128 to 191 - 82_838_849
        // from 192 to 255 - 22_775_080
        // from 256 to 383 - 484_002

        // we assume that 75% of cells are in range 128..191
        // so we can use use 192 as size for value in cache

        const MAX_CELL_SIZE: u64 = 192;
        const KEY_SIZE: u64 = 32;
        const SHARDS: usize = 512;

        let estimated_cell_cache_capacity = size_in_bytes / (KEY_SIZE + MAX_CELL_SIZE);
        tracing::info!(
            estimated_cell_cache_capacity,
            max_cell_cache_size = %bytesize::ByteSize(size_in_bytes),
        );

        let inner = Cache::with_options(
            quick_cache::OptionsBuilder::new()
                .shards(SHARDS)
                .estimated_items_capacity(estimated_cell_cache_capacity as usize)
                .weight_capacity(size_in_bytes)
                .hot_allocation(0.8)
                .build()
                .unwrap(),
            CellSizeEstimator,
            BuildTrustedCellHasher::default(),
            DefaultLifecycle::default(),
        );

        Self {
            inner,
            #[cfg(feature = "cells-metrics")]
            rocksdb_access_histogram: metrics::histogram!(
                "tycho_storage_get_cell_from_rocksdb_time"
            ),
        }
    }

    fn cache_decoded_value(
        &self,
        key: &HashBytes,
        value: &[u8],
    ) -> Option<(Idx, RawCellsCacheItem)> {
        let (idx, data) = decode_indexed_value(value)?;
        let item = RawCellsCacheItem::from_header_and_slice(idx, data);
        self.inner.insert(owned_hash_key(key), item.clone());
        Some((idx, item))
    }

    fn refresh_metrics(&self) {
        metrics::gauge!("tycho_storage_raw_cells_cache_size").set(self.inner.weight() as f64);
        let mem = self.inner.memory_used();
        // Actual LinkedSlab Vec memory (includes Resident, Ghost, free slots)
        metrics::gauge!("tycho_storage_raw_cells_cache_entries_mem").set(mem.entries as f64);
        // Actual HashTable memory (grows but never shrinks)
        metrics::gauge!("tycho_storage_raw_cells_cache_map_mem").set(mem.map as f64);
    }
}

#[cfg(test)]
mod tests {
    use tycho_storage::{StorageConfig, StorageContext};
    use tycho_types::merkle::make_pruned_branch;

    use super::super::cell_nursery::PROMOTION_DELAY_ROUNDS;
    use super::*;
    use crate::storage::{CoreStorage, CoreStorageConfig};

    fn store_test_cell(cell_storage: &CellStorage, root: &DynCell, round: u32) -> Result<()> {
        let mut batch = rocksdb::WriteBatch::default();
        cell_storage.store_cell_mt(root, &mut batch, Default::default(), 1, round)?;
        cell_storage
            .cells_db
            .rocksdb()
            .write_opt(batch, cell_storage.cells_db.cells.write_config())?;
        Ok(())
    }

    fn remove_test_cell(cell_storage: &CellStorage, alloc: &Herd, hash: &HashBytes) -> Result<()> {
        let (_, batch) = cell_storage.remove_cell_mt(alloc, hash, Default::default())?;
        cell_storage
            .cells_db
            .rocksdb()
            .write_opt(batch, cell_storage.cells_db.cells.write_config())?;
        Ok(())
    }

    #[test]
    fn persisted_filter_tracks_exact_insert_and_delete() {
        let mut filter = PersistedCellFilter::new(64, PERSISTED_CELL_FILTER_FP_RATE);
        let hash = HashBytes([7; 32]);

        filter.insert(&hash);
        assert!(filter.filter.contains(hash));

        filter.filter.remove(hash);
        assert!(!filter.filter.contains(hash));
    }

    #[test]
    fn persisted_filter_keeps_duplicate_fingerprints() {
        let mut filter = PersistedCellFilter::new(64, PERSISTED_CELL_FILTER_FP_RATE);
        let hash = HashBytes([11; 32]);

        filter.insert(&hash);
        filter.insert(&hash);
        filter.filter.remove(hash);

        assert!(filter.filter.contains(hash));
    }

    #[tokio::test]
    async fn pruned_cells_have_proper_hashes() -> Result<()> {
        let (ctx, _tmp_dir) = StorageContext::new_temp().await?;
        let config = CoreStorageConfig::new_potato();
        let storage = CoreStorage::open(ctx, config).await?;

        let original_cell = CellBuilder::build_from(CellBuilder::build_from(0u32)?)?;
        let pruned_level1 = make_pruned_branch(original_cell.as_ref(), 0, Cell::empty_context())?;
        let pruned_level2 = make_pruned_branch(pruned_level1.as_ref(), 1, Cell::empty_context())?;
        let pruned_level3 = make_pruned_branch(pruned_level2.as_ref(), 2, Cell::empty_context())?;
        let multilevel_cell =
            CellBuilder::build_from((original_cell.clone(), pruned_level2.clone()))?;

        let cells = [
            original_cell,
            pruned_level1,
            pruned_level2,
            pruned_level3,
            multilevel_cell,
        ];

        let cell_storage = &storage.shard_state_storage().cell_storage;

        // Insert all cells.
        for root in &cells {
            store_test_cell(cell_storage, root.as_ref(), 0)?;
        }

        // Check hashes
        for root in &cells {
            let loaded = cell_storage.load_cell(root.repr_hash(), 0)?;
            for level in 0..4 {
                assert_eq!(loaded.hash(level), root.hash(level));
                assert_eq!(loaded.depth(level), root.depth(level));
            }

            let loaded = Cell::from(loaded);
            for (root_child_hash, loaded_child_hash) in
                std::iter::zip(root.reference_repr_hashes(), loaded.reference_repr_hashes())
            {
                assert_eq!(root_child_hash, loaded_child_hash);
            }
        }

        // Remove all cells.
        let mut alloc = Herd::new();
        for root in &cells {
            remove_test_cell(cell_storage, &alloc, root.repr_hash())?;
            alloc.reset();
        }

        // Check if empty
        cell_storage.cells_db.trigger_compaction().await;
        cell_storage.cells_db.trigger_compaction().await;

        let mut iterator = cell_storage.cells_db.cells.raw_iterator();
        iterator.seek_to_first();
        assert!(iterator.item().is_none());

        Ok(())
    }

    #[tokio::test]
    async fn nursery_promotes_cells_after_delay() -> Result<()> {
        let (ctx, _tmp_dir) = StorageContext::new_temp().await?;
        let config = CoreStorageConfig::new_potato();
        let storage = CoreStorage::open(ctx, config).await?;
        let cell_storage = &storage.shard_state_storage().cell_storage;

        let root = CellBuilder::build_from(123u32)?;

        store_test_cell(cell_storage, root.as_ref(), 0)?;

        assert!(
            cell_storage
                .cells_db
                .cells
                .get(root.repr_hash().as_slice())?
                .is_none()
        );

        store_test_cell(cell_storage, root.as_ref(), PROMOTION_DELAY_ROUNDS)?;

        assert!(
            cell_storage
                .cells_db
                .cells
                .get(root.repr_hash().as_slice())?
                .is_some()
        );

        Ok(())
    }

    #[tokio::test]
    async fn promoted_cell_stays_visible_for_store_lookup_without_reopen() -> Result<()> {
        let (ctx, _tmp_dir) = StorageContext::new_temp().await?;
        let config = CoreStorageConfig::new_potato();
        let storage = CoreStorage::open(ctx, config).await?;
        let cell_storage = &storage.shard_state_storage().cell_storage;

        let root = CellBuilder::build_from(123u32)?;
        let root_hash = *root.repr_hash();

        store_test_cell(cell_storage, root.as_ref(), 0)?;
        store_test_cell(cell_storage, root.as_ref(), PROMOTION_DELAY_ROUNDS)?;

        assert!(
            cell_storage
                .cells_db
                .cells
                .get(root_hash.as_slice())?
                .is_some()
        );
        assert!(cell_storage.nursery.get(&root_hash).is_none());

        store_test_cell(cell_storage, root.as_ref(), PROMOTION_DELAY_ROUNDS + 1)?;

        assert!(cell_storage.nursery.get(&root_hash).is_none());
        assert!(
            cell_storage
                .cells_db
                .cells
                .get(root_hash.as_slice())?
                .is_some()
        );

        Ok(())
    }

    #[tokio::test]
    async fn nursery_remove_before_promotion_keeps_rocksdb_empty() -> Result<()> {
        let (ctx, _tmp_dir) = StorageContext::new_temp().await?;
        let config = CoreStorageConfig::new_potato();
        let storage = CoreStorage::open(ctx, config).await?;
        let cell_storage = &storage.shard_state_storage().cell_storage;

        let root = CellBuilder::build_from(123u32)?;

        store_test_cell(cell_storage, root.as_ref(), 0)?;

        let alloc = Herd::new();
        remove_test_cell(cell_storage, &alloc, root.repr_hash())?;

        assert!(
            cell_storage
                .cells_db
                .cells
                .get(root.repr_hash().as_slice())?
                .is_none()
        );
        assert!(matches!(
            cell_storage.load_cell(root.repr_hash(), 0),
            Err(CellStorageError::CellNotFound)
        ));

        Ok(())
    }

    #[tokio::test]
    async fn nursery_replays_insert_after_reopen() -> Result<()> {
        let (ctx, tmp_dir) = StorageContext::new_temp().await?;
        let config = CoreStorageConfig::new_potato();
        let root = CellBuilder::build_from(123u32)?;
        let root_hash = *root.repr_hash();

        {
            let storage = CoreStorage::open(ctx, config.clone()).await?;
            let cell_storage = &storage.shard_state_storage().cell_storage;
            store_test_cell(cell_storage, root.as_ref(), 0)?;
        }

        let ctx = StorageContext::new(StorageConfig::new_potato(tmp_dir.path())).await?;
        let storage = CoreStorage::open(ctx, config).await?;
        let loaded = storage
            .shard_state_storage()
            .cell_storage
            .load_cell(&root_hash, 0)?;

        assert_eq!(loaded.hash(0), root.hash(0));
        assert!(
            storage
                .shard_state_storage()
                .cell_storage
                .cells_db
                .cells
                .get(root_hash.as_slice())?
                .is_none()
        );
        Ok(())
    }

    #[tokio::test]
    async fn nursery_replays_remove_after_reopen() -> Result<()> {
        let (ctx, tmp_dir) = StorageContext::new_temp().await?;
        let config = CoreStorageConfig::new_potato();
        let root = CellBuilder::build_from(123u32)?;
        let root_hash = *root.repr_hash();

        {
            let storage = CoreStorage::open(ctx, config.clone()).await?;
            let cell_storage = &storage.shard_state_storage().cell_storage;

            store_test_cell(cell_storage, root.as_ref(), 0)?;

            let alloc = Herd::new();
            remove_test_cell(cell_storage, &alloc, &root_hash)?;
        }

        let ctx = StorageContext::new(StorageConfig::new_potato(tmp_dir.path())).await?;
        let storage = CoreStorage::open(ctx, config).await?;
        assert!(matches!(
            storage
                .shard_state_storage()
                .cell_storage
                .load_cell(&root_hash, 0),
            Err(CellStorageError::CellNotFound)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn nursery_replays_promotion_after_reopen() -> Result<()> {
        let (ctx, tmp_dir) = StorageContext::new_temp().await?;
        let config = CoreStorageConfig::new_potato();
        let root = CellBuilder::build_from(123u32)?;
        let root_hash = *root.repr_hash();

        {
            let storage = CoreStorage::open(ctx, config.clone()).await?;
            let cell_storage = &storage.shard_state_storage().cell_storage;

            store_test_cell(cell_storage, root.as_ref(), 0)?;

            store_test_cell(cell_storage, root.as_ref(), PROMOTION_DELAY_ROUNDS)?;
        }

        let ctx = StorageContext::new(StorageConfig::new_potato(tmp_dir.path())).await?;
        let storage = CoreStorage::open(ctx, config).await?;
        assert!(
            storage
                .shard_state_storage()
                .cell_storage
                .cells_db
                .cells
                .get(root_hash.as_slice())?
                .is_some()
        );
        let loaded = storage
            .shard_state_storage()
            .cell_storage
            .load_cell(&root_hash, 0)?;
        assert_eq!(loaded.hash(0), root.hash(0));
        Ok(())
    }
}
