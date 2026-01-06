use std::cell::UnsafeCell;
use std::collections::hash_map;
use std::io::{BufWriter, Write};
use std::mem::{ManuallyDrop, MaybeUninit};
use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Weak};
use std::thread::Scope;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
#[cfg(feature = "cells-metrics")]
use std::time::Instant;

use anyhow::{Context, Result};
use bumpalo::Bump;
use bumpalo_herd::{Herd, Member};
use bytesize::ByteSize;
use crossbeam_queue::SegQueue;
use dashmap::Map;
use quick_cache::sync::{Cache, DefaultLifecycle};
use triomphe::ThinArc;
use tycho_storage::fs::Dir;
use tycho_types::cell::*;
use tycho_util::metrics::{HistogramGuard, spawn_metrics_loop};
use tycho_util::{FastDashMap, FastHashMap, FastHashSet, FastHasherState};
use weedb::rocksdb::WriteBatch;
use weedb::{BoundedCfHandle, rocksdb};

use crate::storage::CellsDb;

pub struct CellStorage {
    cells_db: CellsDb,
    cells_cache: Arc<CellsIndex>,
    raw_cells_cache: Arc<RawCellsCache>,
    drop_interval: u32,
    counters: Vec<AtomicU64>,
    free_idx: SegQueue<u32>,
    next_idx: AtomicU32,
    op_log_tx: mpsc::Sender<CellOp>,
    op_log_seq: AtomicU64,
    files_dir: Dir,
}

type CellsIndex = FastDashMap<HashBytes, CachedCell>;

const CELL_INDEX_BYTES: usize = 4;
const MAX_CELLS: u32 = 1 << 30;

struct CachedCell {
    epoch: u32,
    weak: Weak<StorageCell>,
}

const OP_TYPE_BIT: u64 = 1u64 << 63;

struct CellOp {
    hash: HashBytes,
    ts_with_type: u64,
    tx: u64,
}

fn encode_cell_value(idx: u32, payload: &[u8], target: &mut Vec<u8>) {
    target.extend_from_slice(&idx.to_le_bytes());
    target.extend_from_slice(payload);
}

fn decode_cell_value(value: &[u8]) -> Option<(u32, &[u8])> {
    if value.len() < CELL_INDEX_BYTES {
        return None;
    }

    let idx = u32::from_le_bytes(value[..CELL_INDEX_BYTES].try_into().ok()?);
    if idx >= MAX_CELLS {
        return None;
    }
    Some((idx, &value[CELL_INDEX_BYTES..]))
}

impl CellStorage {
    pub fn new(
        cells_db: CellsDb,
        files_dir: &Dir,
        cache_size_bytes: ByteSize,
        drop_interval: u32,
    ) -> Arc<Self> {
        let cells_cache = Default::default();
        let raw_cells_cache = Arc::new(RawCellsCache::new(cache_size_bytes.as_u64()));
        let mut counters = Vec::with_capacity(MAX_CELLS as usize);
        counters.resize_with(MAX_CELLS as usize, || AtomicU64::new(0));
        let (op_log_tx, op_log_rx) = mpsc::channel();
        let files_dir = files_dir.clone();
        let cell_storage = Arc::new(Self {
            cells_db,
            cells_cache,
            raw_cells_cache: raw_cells_cache.clone(),
            drop_interval,
            counters,
            free_idx: SegQueue::new(),
            next_idx: AtomicU32::new(0),
            op_log_tx,
            op_log_seq: AtomicU64::new(0),
            files_dir: files_dir.clone(),
        });

        std::thread::spawn(move || {
            let file = files_dir
                .file("cells_ops.bin")
                .create(true)
                .append(true)
                .write(true)
                .open();
            let file = match file {
                Ok(file) => file,
                Err(e) => {
                    tracing::warn!(error = ?e, "failed to open cells ops log file");
                    return;
                }
            };

            let mut writer = BufWriter::new(file);
            for op in op_log_rx {
                let mut buf = [0u8; 48];
                buf[..8].copy_from_slice(&op.ts_with_type.to_le_bytes());
                buf[8..16].copy_from_slice(&op.tx.to_le_bytes());
                buf[16..].copy_from_slice(&op.hash.0);
                if writer.write_all(&buf).is_err() {
                    break;
                }
            }
        });

        spawn_metrics_loop(
            &cell_storage,
            Duration::from_secs(5),
            |storage| async move { storage.refresh_metrics() },
        );

        let snapshot_storage = cell_storage.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(300));
            loop {
                interval.tick().await;
                let storage = snapshot_storage.clone();
                let _ = tokio::task::spawn_blocking(move || storage.write_counters_snapshot()).await;
            }
        });

        cell_storage
    }

    pub fn db(&self) -> &CellsDb {
        &self.cells_db
    }

    fn next_op_tx(&self) -> u64 {
        self.op_log_seq.fetch_add(1, Ordering::Relaxed)
    }

    fn log_cell_op(&self, hash: &HashBytes, is_put: bool, tx: u64) {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        let ts_with_type = (ts & (OP_TYPE_BIT - 1)) | if is_put { OP_TYPE_BIT } else { 0 };
        let _ = self.op_log_tx.send(CellOp {
            hash: *hash,
            ts_with_type,
            tx,
        });
    }

    fn write_counters_snapshot(&self) -> Result<()> {
        let file = self
            .files_dir
            .file("cells_counters.bin")
            .create(true)
            .write(true)
            .truncate(true)
            .open()?;
        let mut writer = BufWriter::new(file);
        let next_idx = self.next_idx.load(Ordering::Acquire) as usize;
        let end = next_idx.min(self.counters.len());
        for counter in &self.counters[..end] {
            writer.write_all(&counter.load(Ordering::Acquire).to_le_bytes())?;
        }
        Ok(())
    }

    fn refresh_metrics(&self) {
        self.raw_cells_cache.refresh_metrics();
        metrics::gauge!("tycho_storage_cells_next_idx")
            .set(self.next_idx.load(Ordering::Acquire) as f64);
        metrics::gauge!("tycho_storage_cells_free_idx_len").set(self.free_idx.len() as f64);
    }

    fn alloc_idx(&self) -> Result<u32, CellStorageError> {
        if let Some(idx) = self.free_idx.pop() {
            return Ok(idx);
        }

        self.next_idx
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                if value < MAX_CELLS {
                    Some(value + 1)
                } else {
                    None
                }
            })
            .map_err(|_| CellStorageError::IndexOverflow)
    }

    pub fn apply_temp_cell(&self, root: &HashBytes) -> Result<()> {
        const MAX_NEW_CELLS_BATCH_SIZE: usize = 10000;

        struct TempCell {
            idx: u32,
            additions: u32,
            is_new: bool,
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
            cell_storage: &'a CellStorage,
            cells_cf: BoundedCfHandle<'a>,
            cells_db: &'a CellsDb,
            buffer: Vec<u8>,
            transaction: FastHashMap<HashBytes, TempCell>,
            new_cells_batch: rocksdb::WriteBatch,
            new_cell_count: usize,
            raw_cache: &'a RawCellsCache,
            op_tx: Option<u64>,
        }

        impl<'a> Context<'a> {
            fn new(
                cell_storage: &'a CellStorage,
                cells_db: &'a CellsDb,
                raw_cache: &'a RawCellsCache,
            ) -> Self {
                Self {
                    cell_storage,
                    cells_cf: cells_db.cells.cf(),
                    cells_db,
                    buffer: Vec::with_capacity(512),
                    transaction: Default::default(),
                    new_cells_batch: rocksdb::WriteBatch::default(),
                    new_cell_count: 0,
                    raw_cache,
                    op_tx: None,
                }
            }

            fn load_temp(&self, key: &HashBytes) -> Result<CellHashesIter<'a>, CellStorageError> {
                let data = match self.cells_db.temp_cells.get(key) {
                    Ok(Some(data)) => data,
                    Ok(None) => return Err(CellStorageError::CellNotFound),
                    Err(e) => return Err(CellStorageError::Internal(e)),
                };

                let (offset, remaining_refs) = {
                    let data = &mut data.as_ref();

                    let descriptor = CellDescriptor::new([data[0], data[1]]);
                    let byte_len = descriptor.byte_len() as usize;
                    let hash_count = descriptor.hash_count() as usize;
                    let ref_count = descriptor.reference_count();

                    let offset = 4usize + byte_len + (32 + 2) * hash_count;
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
                Ok(match self.transaction.entry(*key) {
                    hash_map::Entry::Occupied(mut entry) => {
                        entry.get_mut().additions += 1; // 1 new reference
                        InsertedCell::Existing
                    }
                    hash_map::Entry::Vacant(entry) => {
                        if let Some(idx) =
                            self.raw_cache.get_idx_for_insert(self.cells_db, key, 0)?
                        {
                            entry.insert(TempCell {
                                idx,
                                additions: 1, // 1 new reference
                                is_new: false,
                            });
                            return Ok(InsertedCell::Existing);
                        }

                        let idx = self.cell_storage.alloc_idx()?;
                        entry.insert(TempCell {
                            idx,
                            additions: 1,
                            is_new: true,
                        });
                        let iter = self.load_temp(key)?;

                        self.buffer.clear();
                        encode_cell_value(idx, iter.data.as_ref(), &mut self.buffer);

                        self.new_cells_batch
                            .put_cf(&self.cells_cf, key, self.buffer.as_slice());
                        let op_tx = *self
                            .op_tx
                            .get_or_insert_with(|| self.cell_storage.next_op_tx());
                        self.cell_storage.log_cell_op(key, true, op_tx);

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
                    self.cells_db
                        .rocksdb()
                        .write(std::mem::take(&mut self.new_cells_batch))?;
                    self.new_cell_count = 0;
                    self.op_tx = None;
                }
                Ok(())
            }

            fn flush_existing_cells(self) -> Result<(), rocksdb::Error> {
                for (key, item) in self.transaction {
                    if item.is_new {
                        self.cell_storage.counters[item.idx as usize]
                            .store(u64::from(item.additions), Ordering::Release);
                    } else {
                        self.cell_storage.counters[item.idx as usize]
                            .fetch_add(u64::from(item.additions), Ordering::Release);
                    }

                    self.raw_cache.on_insert_cell(&key, item.idx, None);
                }

                Ok(())
            }
        }

        let mut ctx = Context::new(self, &self.cells_db, &self.raw_cells_cache);

        let mut stack = Vec::with_capacity(16);
        if let InsertedCell::New(iter) = ctx.insert_cell(root)? {
            stack.push(iter);
        }

        'outer: loop {
            let Some(iter) = stack.last_mut() else {
                break;
            };

            for ref child in iter {
                if let InsertedCell::New(iter) = ctx.insert_cell(child)? {
                    stack.push(iter);
                    continue 'outer;
                }
            }

            stack.pop();
        }

        // Clear big chunks of data before finalization
        drop(stack);

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
    ) -> Result<usize, CellStorageError> {
        type StoreResult = Result<(), CellStorageError>;

        struct AddedCell<'a> {
            idx: u32,
            additions: u32,
            data: Option<&'a [u8]>,
        }

        struct Alloc<'a> {
            bump: Member<'a>,
            buffer: Vec<u8>,
        }

        struct StoreContext<'a> {
            cell_storage: &'a CellStorage,
            db: &'a CellsDb,
            herd: &'a Herd,
            raw_cache: &'a RawCellsCache,
            /// Subtrees to process in parallel.
            split_at: FastHashMap<HashBytes, Cell>,
            // TODO: Use `&'a HashBytes` for key?
            // Pros:
            //   - Less `memcpy` calls;
            //   - Less memory occupied (8 bytes per key vs 32 bytes);
            // Cons:
            //   - This reference is stored alongside with cell so
            //     key locations will be very random and iteration
            //     will be slower than it is with inplace keys.
            /// Transaction items.
            transaction: FastDashMap<HashBytes, AddedCell<'a>>,
            /// References of detached subtrees.
            delayed_additions: std::sync::Mutex<FastHashMap<HashBytes, u32>>,
            op_tx: u64,
        }

        impl<'a> StoreContext<'a> {
            fn new(
                cell_storage: &'a CellStorage,
                db: &'a CellsDb,
                herd: &'a Herd,
                raw_cache: &'a RawCellsCache,
                split_accounts: FastHashMap<HashBytes, Cell>,
                capacity: usize,
            ) -> Self {
                Self {
                    cell_storage,
                    db,
                    raw_cache,
                    herd,
                    split_at: split_accounts,
                    transaction: FastDashMap::with_capacity_and_hasher_and_shard_amount(
                        capacity,
                        Default::default(),
                        512,
                    ),
                    delayed_additions: Default::default(),
                    op_tx: cell_storage.next_op_tx(),
                }
            }

            fn traverse_cell<'c: 'scope, 'scope, 'env>(
                &'c self,
                root: &'c DynCell,
                scope: &'scope Scope<'scope, 'env>,
            ) -> StoreResult {
                let mut alloc = Alloc {
                    bump: self.herd.get(),
                    buffer: Vec::with_capacity(512),
                };

                if !self.insert_cell(root.as_ref(), &mut alloc, 0)? {
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
                            match delayed_additions.entry(*child_hash) {
                                hash_map::Entry::Vacant(entry) => {
                                    // This subtree will be added by another thread,
                                    // so no additions is needed on first occurence.
                                    entry.insert(0);
                                    drop(delayed_additions);

                                    // Spawn processing.
                                    // TODO: Handle error properly.
                                    scope.spawn(|| self.traverse_cell(child, scope).unwrap());
                                }
                                hash_map::Entry::Occupied(mut entry) => {
                                    // Other thread will add this subtree only once,
                                    // so we need to adjust references to keep them in sync.
                                    *entry.get_mut() += 1;
                                }
                            }

                            continue 'outer;
                        }

                        if self.insert_cell(child, &mut alloc, depth)? {
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
                cell: &DynCell,
                alloc: &mut Alloc<'a>,
                depth: usize,
            ) -> Result<bool, CellStorageError> {
                use dashmap::mapref::entry::Entry;

                let key = cell.repr_hash();

                // Fast path: cell is already presented in this transaction, just bump refs.
                if let Some(mut value) = self.transaction.get_mut(key) {
                    value.additions += 1;
                    return Ok(false);
                }

                // Slow path: find an existing stored cell data.
                // NOTE: We dropped a dashmap lock on purpose so that parallel
                // threads can do this job without blocking each other on the
                // same shard (going to rocksdb might be slow).

                let existing_idx = self.raw_cache.get_idx_for_insert(self.db, key, depth)?;
                let is_new = existing_idx.is_none();

                // Prepare `alloc.buffer` if the cell is new (but not flush it to
                // the bump allocator yet).
                if is_new {
                    let buffer = &mut alloc.buffer;
                    buffer.clear();
                    if StorageCell::serialize_to(cell, buffer).is_err() {
                        return Err(CellStorageError::InvalidCell);
                    }
                }

                // Try to insert once more.
                Ok(match self.transaction.entry(*key) {
                    Entry::Occupied(mut value) => {
                        // Some other thread has already inserted this cell.
                        // In this case we discard buffer data and juse bump refs.
                        value.get_mut().additions += 1;
                        false
                    }
                    Entry::Vacant(entry) => {
                        let idx = match existing_idx {
                            Some(idx) => idx,
                            None => self.cell_storage.alloc_idx()?,
                        };
                        // Copy buffer data to the bump allocator to extend its lifetime.
                        let data = if is_new {
                            Some(alloc.bump.alloc_slice_copy(alloc.buffer.as_slice()) as &[u8])
                        } else {
                            None
                        };

                        // Add a new transaction entry.
                        entry.insert(AddedCell {
                            idx,
                            additions: 1,
                            data,
                        });
                        is_new
                    }
                })
            }

            fn finalize(self, batch: &mut WriteBatch) -> usize {
                std::thread::scope(|s| {
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

                    // Split shards evenly between N threads and apply changes to cache.
                    let num_shards = self.transaction._shard_count();
                    let num_threads = std::thread::available_parallelism()
                        .map_or(1, usize::from)
                        .min(num_shards);

                    let chunk_size = num_shards / num_threads;
                    assert!(chunk_size >= 1);
                    let mut additional = num_shards % num_threads;

                    let mut range_start = 0;
                    for _ in 0..num_threads {
                        let mut range_end = range_start + chunk_size;
                        if additional > 0 {
                            additional -= 1;
                            range_end += 1;
                        }
                        assert!(range_end > range_start);

                        // SAFETY: Index must be in bounds.
                        let shards = unsafe {
                            (range_start..range_end).map(|i| self.transaction._get_read_shard(i))
                        };
                        range_start = range_end;

                        let cache = self.raw_cache;
                        let counters = &self.cell_storage.counters;
                        s.spawn(move || {
                            for shard in shards {
                                // SAFETY: `RawIter` will not outlibe the `RawTable`.
                                for value in unsafe { shard.iter() } {
                                    // SAFETY: `Bucket` is a valid item, received from a valid iterator.
                                    let (key, value) = unsafe { value.as_ref() };
                                    let item = value.get();
                                    if let Some(data) = item.data {
                                        counters[item.idx as usize]
                                            .store(u64::from(item.additions), Ordering::Release);
                                        cache.on_insert_cell(key, item.idx, Some(data));
                                    } else {
                                        counters[item.idx as usize]
                                            .fetch_add(u64::from(item.additions), Ordering::AcqRel);
                                        cache.on_insert_cell(key, item.idx, None);
                                    }
                                }
                            }
                        });
                    }
                    assert_eq!(range_start, num_shards);

                    // Merge transaction items into the final batch.
                    let mut buffer = Vec::with_capacity(512);
                    let total = self.transaction.len();
                    let cells_cf = &self.db.cells.cf();
                    for kv in self.transaction.iter() {
                        let key = kv.key();
                        let item = kv.value();

                        if let Some(data) = item.data {
                            buffer.clear();
                            encode_cell_value(item.idx, data, &mut buffer);
                            batch.put_cf(cells_cf, key.as_slice(), &buffer);
                            self.cell_storage.log_cell_op(key, true, self.op_tx);
                        }
                    }
                    total
                })
            }
        }

        let herd = Herd::new();
        let ctx = StoreContext::new(
            self,
            &self.cells_db,
            &herd,
            &self.raw_cells_cache,
            split_at,
            capacity,
        );

        std::thread::scope(|scope| ctx.traverse_cell(root, scope))?;

        Ok(ctx.finalize(batch))
    }

    pub fn store_cell(
        &self,
        batch: &mut WriteBatch,
        root: &DynCell,
        estimated_cell_count: usize,
    ) -> Result<usize, CellStorageError> {
        struct AddedCell<'a> {
            idx: u32,
            additions: u32,
            data: Option<&'a [u8]>,
        }

        struct Context<'a> {
            cell_storage: &'a CellStorage,
            db: &'a CellsDb,
            raw_cells_cache: &'a RawCellsCache,
            alloc: &'a Bump,
            transaction: FastHashMap<&'a HashBytes, AddedCell<'a>>,
            buffer: Vec<u8>,
            op_tx: u64,
        }

        impl<'a> Context<'a> {
            fn insert_cell(
                &mut self,
                cell: &'a DynCell,
                depth: usize,
            ) -> Result<bool, CellStorageError> {
                let key = cell.repr_hash();
                Ok(match self.transaction.entry(key) {
                    hash_map::Entry::Occupied(mut value) => {
                        value.get_mut().additions += 1;
                        false
                    }
                    hash_map::Entry::Vacant(entry) => {
                        let (idx, data) = match self
                            .raw_cells_cache
                            .get_idx_for_insert(self.db, key, depth)?
                        {
                            Some(idx) => (idx, None),
                            None => {
                                let idx = self.cell_storage.alloc_idx()?;
                                self.buffer.clear();
                                if StorageCell::serialize_to(cell, &mut self.buffer).is_err() {
                                    return Err(CellStorageError::InvalidCell);
                                }
                                (
                                    idx,
                                    Some(self.alloc.alloc_slice_copy(self.buffer.as_slice())
                                        as &[u8]),
                                )
                            }
                        };

                        entry.insert(AddedCell {
                            idx,
                            additions: 1,
                            data,
                        });
                        data.is_some()
                    }
                })
            }

            fn finalize(mut self, batch: &mut rocksdb::WriteBatch) -> usize {
                let total = self.transaction.len();
                let cells_cf = &self.db.cells.cf();

                for (key, item) in self.transaction {
                    self.buffer.clear();
                    // new cell
                    if let Some(data) = item.data {
                        encode_cell_value(item.idx, data, &mut self.buffer);
                        batch.put_cf(cells_cf, key.as_slice(), &self.buffer);
                        self.cell_storage.log_cell_op(key, true, self.op_tx);
                        self.cell_storage.counters[item.idx as usize]
                            .store(u64::from(item.additions), Ordering::Release);
                        self.raw_cells_cache
                            .on_insert_cell(key, item.idx, Some(data));
                    } else {
                        // only rc bump
                        self.cell_storage.counters[item.idx as usize]
                            .fetch_add(u64::from(item.additions), Ordering::AcqRel);
                        self.raw_cells_cache.on_insert_cell(key, item.idx, None);
                    }
                }

                total
            }
        }

        let alloc = bumpalo::Bump::new();

        // Prepare context and handles
        let mut ctx = Context {
            cell_storage: self,
            db: &self.cells_db,
            raw_cells_cache: &self.raw_cells_cache,
            alloc: &alloc,
            transaction: FastHashMap::with_capacity_and_hasher(
                estimated_cell_count,
                Default::default(),
            ),
            buffer: Vec::with_capacity(512),
            op_tx: self.next_op_tx(),
        };

        'visit: {
            // Check root cell
            if !ctx.insert_cell(root.as_ref(), 0)? {
                break 'visit;
            }
            let mut stack = Vec::with_capacity(16);
            stack.push(root.references());

            // Check other cells
            'outer: loop {
                let depth = stack.len();
                let Some(iter) = stack.last_mut() else {
                    break;
                };

                for child in &mut *iter {
                    if ctx.insert_cell(child, depth)? {
                        stack.push(child.references());
                        continue 'outer;
                    }
                }

                stack.pop();
            }
        }

        Ok(ctx.finalize(batch))
    }

    pub fn load_cell(
        self: &Arc<Self>,
        hash: &HashBytes,
        epoch: u32,
    ) -> Result<Arc<StorageCell>, CellStorageError> {
        #[cfg(feature = "cells-metrics")]
        let _histogram = HistogramGuard::begin("tycho_storage_load_cell_time");

        if let Some(cell) = self.cells_cache.get(hash)
            && cell.epoch.saturating_add(self.drop_interval) >= epoch
            && let Some(cell) = cell.weak.upgrade()
        {
            return Ok(cell);
        }

        let mut cell = match self.raw_cells_cache.get_raw(&self.cells_db, hash)? {
            Some(value) => match StorageCell::deserialize(self.clone(), &value.slice, epoch) {
                Some(cell) => Arc::new(cell),
                None => return Err(CellStorageError::InvalidCell),
            },
            None => return Err(CellStorageError::CellNotFound),
        };

        let has_new;
        match self.cells_cache.entry(*hash) {
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
            metrics::gauge!("tycho_storage_cells_tree_cache_size").increment(1f64);
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
            cell_storage: &'a CellStorage,
            db: &'a CellsDb,
            herd: &'a Herd,
            raw_cache: &'a RawCellsCache,
            /// Subtrees to process in parallel.
            split_at: FastHashSet<HashBytes>,
            // TODO: Use `&'a HashBytes` for key?
            // Pros:
            //   - Less `memcpy` calls;
            //   - Less memory occupied (8 bytes per key vs 32 bytes);
            // Cons:
            //   - This reference is stored alongside with cell so
            //     key locations will be very random and iteration
            //     will be slower than it is with inplace keys.
            /// Transaction items.
            transaction: FastDashMap<HashBytes, RemovedCell<'a>>,
            /// References of detached subtrees.
            delayed_removes: std::sync::Mutex<FastHashMap<HashBytes, u32>>,
            op_tx: u64,
        }

        impl<'a> RemoveContext<'a> {
            fn new(
                cell_storage: &'a CellStorage,
                db: &'a CellsDb,
                herd: &'a Herd,
                raw_cache: &'a RawCellsCache,
                split_at: FastHashSet<HashBytes>,
            ) -> Self {
                Self {
                    cell_storage,
                    db,
                    raw_cache,
                    herd,
                    split_at,
                    transaction: FastDashMap::with_capacity_and_hasher_and_shard_amount(
                        128,
                        Default::default(),
                        512,
                    ),
                    delayed_removes: Default::default(),
                    op_tx: cell_storage.next_op_tx(),
                }
            }

            fn traverse_cell<'c: 'scope, 'scope, 'env>(
                &'c self,
                hash: &'c HashBytes,
                scope: &'scope Scope<'scope, 'env>,
            ) -> RemoveResult {
                let mut alloc = Alloc {
                    bump: self.herd.get(),
                    buffer: Vec::with_capacity(4),
                };

                let Some(refs) = self.remove_cell(hash, &mut alloc)? else {
                    return Ok(());
                };

                let mut stack = Vec::with_capacity(16);
                stack.push(refs.iter());

                // While some cells left
                'outer: loop {
                    let Some(iter) = stack.last_mut() else {
                        break;
                    };

                    for child_hash in iter.by_ref() {
                        // Skip cell to remove it later in parallel
                        if self.split_at.contains(child_hash) {
                            let mut delayed_removes = self.delayed_removes.lock().unwrap();
                            match delayed_removes.entry(*child_hash) {
                                hash_map::Entry::Vacant(entry) => {
                                    // This subtree will be removed by another thread,
                                    // so no removes is needed on first occurrence.
                                    entry.insert(0);
                                    drop(delayed_removes);

                                    // Spawn processing.
                                    // TODO: Handle error properly.
                                    scope.spawn(|| {
                                        self.traverse_cell(child_hash, scope).unwrap();
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
                        let refs = self.remove_cell(child_hash, &mut alloc)?;

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
                repr_hash: &HashBytes,
                alloc: &mut Alloc<'a>,
            ) -> Result<Option<&'a [HashBytes]>, CellStorageError> {
                use dashmap::mapref::entry::Entry;

                // Fast path: cell is already presented in this transaction, just update refs.
                if let Some(mut value) = self.transaction.get_mut(repr_hash) {
                    return value.remove();
                }

                // Slow path: find an existing stored cell data.
                // NOTE: We dropped a dashmap lock on purpose so that parallel
                // threads can do this job without blocking each other on the
                // same shard (going to rocksdb might be slow).

                let buffer = &mut alloc.buffer;
                let idx = self.raw_cache.get_for_delete(self.db, repr_hash, buffer)?;
                let old_rc = self.cell_storage.counters[idx as usize].load(Ordering::Acquire);
                if old_rc == 0 {
                    return Err(CellStorageError::CellNotFound);
                }

                // Try to remove once more.
                match self.transaction.entry(*repr_hash) {
                    // Some other thread has already removed this cell.
                    // In this case we just used the existing entry state.
                    Entry::Occupied(mut value) => value.get_mut().remove(),
                    Entry::Vacant(v) => Ok(v
                        .insert(RemovedCell {
                            idx,
                            old_rc,
                            removes: 1,
                            refs: alloc.bump.alloc_slice_copy(buffer.as_slice()),
                        })
                        .next_refs()),
                }
            }

            fn finalize(self, batch: &mut WriteBatch) -> usize {
                let _hist = HistogramGuard::begin("tycho_storage_batch_write_parallel_time_high");

                // Write transaction to the `WriteBatch`
                std::thread::scope(|s| {
                    // Apply delayed removes before finalizing the transaction.
                    for (hash, removes) in self.delayed_removes.into_inner().unwrap() {
                        if removes > 0 {
                            if let Some(mut item) = self.transaction.get_mut(&hash) {
                                item.removes += removes;
                            } else {
                                panic!("spawned subtree was not processed");
                            }
                        }
                    }

                    // Split shards evenly between N threads and apply changes to cache.
                    let num_shards = self.transaction._shard_count();
                    let num_threads = std::thread::available_parallelism()
                        .map_or(1, usize::from)
                        .min(num_shards);

                    let chunk_size = num_shards / num_threads;
                    assert!(chunk_size >= 1);
                    let mut additional = num_shards % num_threads;

                    let mut range_start = 0;
                    for _ in 0..num_threads {
                        let mut range_end = range_start + chunk_size;
                        if additional > 0 {
                            additional -= 1;
                            range_end += 1;
                        }
                        assert!(range_end > range_start);

                        // SAFETY: Index must be in bounds.
                        let shards = unsafe {
                            (range_start..range_end).map(|i| self.transaction._get_read_shard(i))
                        };
                        range_start = range_end;

                        let cache = self.raw_cache;
                        let counters = &self.cell_storage.counters;
                        let free_idx = &self.cell_storage.free_idx;
                        s.spawn(move || {
                            for shard in shards {
                                // SAFETY: `RawIter` will not outlibe the `RawTable`.
                                for value in unsafe { shard.iter() } {
                                    // SAFETY: `Bucket` is a valid item, received from a valid iterator.
                                    let (key, value) = unsafe { value.as_ref() };
                                    let item = value.get();

                                    let new_rc = item.old_rc - u64::from(item.removes);
                                    counters[item.idx as usize].store(new_rc, Ordering::Release);
                                    if new_rc == 0 {
                                        cache.remove(key);
                                        free_idx.push(item.idx);
                                    }
                                }
                            }
                        });
                    }
                    assert_eq!(range_start, num_shards);

                    // Merge transaction items into the final batch.
                    let total = self.transaction.len();
                    let cells_cf = &self.db.cells.cf();
                    for kv in self.transaction.iter() {
                        let key = kv.key();
                        let item = kv.value();

                        if item.old_rc == u64::from(item.removes) {
                            batch.delete_cf(cells_cf, key.as_slice());
                            self.cell_storage.log_cell_op(key, false, self.op_tx);
                        }
                    }
                    total
                })
            }
        }

        let ctx = RemoveContext::new(self, &self.cells_db, herd, &self.raw_cells_cache, split_at);

        std::thread::scope(|scope| ctx.traverse_cell(root, scope))?;

        // NOTE: For each cell we have 32 bytes for key and 8 bytes for RC,
        //       and a bit more just in case.
        let total = ctx.transaction.len();
        let mut batch = WriteBatch::with_capacity_bytes(total * (32 + 8 + 8));

        Ok((ctx.finalize(&mut batch), batch))
    }

    #[allow(unused)]
    pub fn remove_cell(
        &self,
        alloc: &Bump,
        hash: &HashBytes,
    ) -> Result<(usize, WriteBatch), CellStorageError> {
        let cells = &self.cells_db.cells;
        let cells_cf = &cells.cf();

        let mut transaction: FastHashMap<&HashBytes, RemovedCell<'_>> =
            FastHashMap::with_capacity_and_hasher(128, Default::default());
        let mut buffer = Vec::with_capacity(4);
        let op_tx = self.next_op_tx();

        let mut stack = Vec::with_capacity(16);
        stack.push(std::slice::from_ref(hash).iter());

        // While some cells left
        'outer: loop {
            let Some(iter) = stack.last_mut() else {
                break;
            };

            for cell_id in iter.by_ref() {
                // Process the current cell.
                let refs = match transaction.entry(cell_id) {
                    hash_map::Entry::Occupied(mut v) => v.get_mut().remove()?,
                    hash_map::Entry::Vacant(v) => {
                        let idx = self.raw_cells_cache.get_for_delete(
                            &self.cells_db,
                            cell_id,
                            &mut buffer,
                        )?;
                        let old_rc = self.counters[idx as usize].load(Ordering::Acquire);
                        if old_rc == 0 {
                            return Err(CellStorageError::CellNotFound);
                        }
                        v.insert(RemovedCell {
                            idx,
                            old_rc,
                            removes: 1,
                            refs: alloc.alloc_slice_copy(buffer.as_slice()),
                        })
                        .next_refs()
                    }
                };

                if let Some(refs) = refs {
                    // And proceed to its refs if any.
                    stack.push(refs.iter());
                    continue 'outer;
                }
            }

            // Drop the current cell when all of its children were processed.
            stack.pop();
        }

        // Clear big chunks of data before finalization
        drop(stack);

        // Write transaction to the `WriteBatch`
        let _hist = HistogramGuard::begin("tycho_storage_batch_write_time_high");
        let total = transaction.len();

        // NOTE: For each cell we have 32 bytes for key and 8 bytes for RC,
        //       and a bit more just in case.
        let mut batch = WriteBatch::with_capacity_bytes(total * (32 + 8 + 8));

        for (key, item) in transaction {
            let new_rc = item.old_rc - u64::from(item.removes);
            self.counters[item.idx as usize].store(new_rc, Ordering::Release);
            if new_rc == 0 {
                batch.delete_cf(cells_cf, key.as_slice());
                self.log_cell_op(key, false, op_tx);
                self.raw_cells_cache.remove(key);
                self.free_idx.push(item.idx);
            }
        }

        Ok((total, batch))
    }

    pub fn drop_cell(&self, hash: &HashBytes) {
        if self
            .cells_cache
            .remove_if(hash, |_, cell| cell.weak.strong_count() == 0)
            .is_some()
        {
            #[cfg(feature = "cells-metrics")]
            metrics::gauge!("tycho_storage_cells_tree_cache_size").decrement(1f64);
        }
    }
}

struct RemovedCell<'a> {
    idx: u32,
    old_rc: u64,
    removes: u32,
    refs: &'a [HashBytes],
}

impl<'a> RemovedCell<'a> {
    fn remove(&mut self) -> Result<Option<&'a [HashBytes]>, CellStorageError> {
        self.removes += 1;
        if u64::from(self.removes) <= self.old_rc {
            Ok(self.next_refs())
        } else {
            Err(CellStorageError::CounterMismatch {
                expected: self.old_rc,
                actual: self.removes,
            })
        }
    }

    fn next_refs(&self) -> Option<&'a [HashBytes]> {
        if self.old_rc > u64::from(self.removes) {
            None
        } else {
            Some(self.refs)
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum CellStorageError {
    #[error("Cell not found in cell db")]
    CellNotFound,
    #[error("Invalid cell")]
    InvalidCell,
    #[error("Cell counter mismatch: expected refcount {expected}, got {actual} removes")]
    CounterMismatch { expected: u64, actual: u32 },
    #[error("Cell index overflow")]
    IndexOverflow,
    #[error("Internal rocksdb error")]
    Internal(#[from] rocksdb::Error),
}

pub struct StorageCell {
    cell_storage: Arc<CellStorage>,
    descriptor: CellDescriptor,
    bit_len: u16,
    data_ptr: *const u8,
    data_len: u8,
    hash_count: u8,
    epoch: u32,

    reference_states: [AtomicU8; 4],
    reference_data: [UnsafeCell<StorageCellReferenceData>; 4],
}

impl StorageCell {
    const REF_EMPTY: u8 = 0x0;
    const REF_RUNNING: u8 = 0x1;
    const REF_STORAGE: u8 = 0x2;

    const HASHES_ITEM_LEN: usize = 32 + 2;

    pub fn deserialize(cell_storage: Arc<CellStorage>, buffer: &[u8], epoch: u32) -> Option<Self> {
        if buffer.len() < 4 {
            return None;
        }

        let descriptor = CellDescriptor::new([buffer[0], buffer[1]]);
        let bit_len = u16::from_le_bytes([buffer[2], buffer[3]]);
        let byte_len = descriptor.byte_len() as usize;
        let hash_count = descriptor.hash_count() as usize;
        let ref_count = descriptor.reference_count() as usize;

        let allocated_len = byte_len + hash_count * Self::HASHES_ITEM_LEN;
        let total_len = 4usize + allocated_len + 32 * ref_count;
        if buffer.len() < total_len {
            return None;
        }

        let data_ptr = Box::into_raw(Box::<[u8]>::from(&buffer[4..4 + allocated_len])).cast::<u8>();

        let reference_states = Default::default();
        let mut reference_data = unsafe {
            MaybeUninit::<[UnsafeCell<StorageCellReferenceData>; 4]>::uninit().assume_init()
        };

        const { assert!(std::mem::size_of::<UnsafeCell<StorageCellReferenceData>>() == 32) };
        unsafe {
            std::ptr::copy_nonoverlapping(
                buffer.as_ptr().add(4 + allocated_len),
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
            hash_count: hash_count as u8,
            epoch,
            reference_states,
            reference_data,
        })
    }

    pub fn deserialize_references(data: &[u8], target: &mut Vec<HashBytes>) -> bool {
        if data.len() < 4 {
            return false;
        }

        let descriptor = CellDescriptor::new([data[0], data[1]]);
        let hash_count = descriptor.hash_count();
        let ref_count = descriptor.reference_count() as usize;

        let mut offset = 4usize + descriptor.byte_len() as usize + (32 + 2) * hash_count as usize;
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

    pub fn serialize_to(cell: &DynCell, target: &mut Vec<u8>) -> Result<()> {
        let descriptor = cell.descriptor();
        let hash_count = descriptor.hash_count();
        let ref_count = descriptor.reference_count();

        target.reserve(
            4usize
                + descriptor.byte_len() as usize
                + (32 + 2) * hash_count as usize
                + 32 * ref_count as usize,
        );

        target.extend_from_slice(&[descriptor.d1, descriptor.d2]);
        target.extend_from_slice(&cell.bit_len().to_le_bytes());
        target.extend_from_slice(cell.data());
        assert_eq!(cell.data().len(), descriptor.byte_len() as usize);

        for i in 0..descriptor.hash_count() {
            target.extend_from_slice(cell.hash(i).as_array());
            target.extend_from_slice(&cell.depth(i).to_le_bytes());
        }

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

    fn virtualize(&self) -> &DynCell {
        VirtualCellWrapper::wrap(self)
    }

    fn hash(&self, level: u8) -> &HashBytes {
        let i = self.descriptor.level_mask().hash_index(level);
        let offset = self.data_len as usize + (i as usize) * Self::HASHES_ITEM_LEN;
        HashBytes::wrap(unsafe { &*self.data_ptr.add(offset).cast::<[u8; 32]>() })
    }

    fn depth(&self, level: u8) -> u16 {
        let i = self.descriptor.level_mask().hash_index(level);
        let offset = self.data_len as usize + (i as usize) * Self::HASHES_ITEM_LEN + 32;
        u16::from_le_bytes(unsafe { *self.data_ptr.add(offset).cast::<[u8; 2]>() })
    }
}

impl Drop for StorageCell {
    fn drop(&mut self) {
        let allocated_len =
            self.data_len as usize + (self.hash_count as usize) * Self::HASHES_ITEM_LEN;

        _ = unsafe {
            Box::from_raw(std::ptr::slice_from_raw_parts_mut(
                self.data_ptr.cast_mut(),
                allocated_len,
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

pub union StorageCellReferenceData {
    /// Incplmete state.
    hash: HashBytes,
    /// Complete state.
    storage_cell: ManuallyDrop<Arc<StorageCell>>,
}

struct RawCellsCache {
    inner: Cache<HashBytes, RawCellsCacheItem, CellSizeEstimator, FastHasherState>,
    #[cfg(feature = "cells-metrics")]
    rocksdb_access_histogram: metrics::Histogram,
}

#[derive(Clone, Copy)]
struct RawCellHeader {
    idx: u32,
}

type RawCellsCacheItem = ThinArc<RawCellHeader, u8>;

#[derive(Clone, Copy)]
pub struct CellSizeEstimator;
impl quick_cache::Weighter<HashBytes, RawCellsCacheItem> for CellSizeEstimator {
    fn weight(&self, key: &HashBytes, val: &RawCellsCacheItem) -> u64 {
        const STATIC_SIZE: usize =
            size_of::<RawCellsCacheItem>() + size_of::<RawCellHeader>() + size_of::<usize>() * 2; // ArcInner refs + HeaderWithLength length

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
            FastHasherState::default(),
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

    fn get_raw(
        &self,
        db: &CellsDb,
        key: &HashBytes,
    ) -> Result<Option<RawCellsCacheItem>, CellStorageError> {
        use quick_cache::sync::GuardResult;

        match self.inner.get_value_or_guard(key, None) {
            GuardResult::Value(value) => Ok(Some(value)),
            GuardResult::Guard(g) => {
                let value = {
                    #[cfg(feature = "cells-metrics")]
                    let _timer = scopeguard::guard(Instant::now(), |started_at| {
                        self.rocksdb_access_histogram.record(started_at.elapsed());
                    });

                    db.cells
                        .get(key.as_slice())
                        .map_err(CellStorageError::Internal)?
                };

                match value {
                    Some(value) => {
                        let (idx, data) = decode_cell_value(value.as_ref())
                            .ok_or(CellStorageError::InvalidCell)?;
                        let value =
                            RawCellsCacheItem::from_header_and_slice(RawCellHeader { idx }, data);
                        _ = g.insert(value.clone());
                        Ok(Some(value))
                    }
                    None => Ok(None),
                }
            }
            GuardResult::Timeout => unreachable!(),
        }
    }

    fn get_idx_for_insert(
        &self,
        db: &CellsDb,
        key: &HashBytes,
        depth: usize,
    ) -> Result<Option<u32>, CellStorageError> {
        // A constant which tells since which depth we should start to use cache.
        // This method is used mostly for inserting new states, so we can assume
        // that first N levels will mostly be new.
        //
        // This value was chosen empirically.
        const NEW_CELLS_DEPTH_THRESHOLD: usize = 4;

        if depth >= NEW_CELLS_DEPTH_THRESHOLD {
            // NOTE: `get` here is used to affect a "hotness" of the value, because
            // there is a big chance that we will need it soon during state processing
            if let Some(entry) = self.inner.get(key) {
                return Ok(Some(entry.header.header.idx));
            }
        }

        match db.cells.get(key).map_err(CellStorageError::Internal)? {
            Some(value) => {
                let (idx, data) =
                    decode_cell_value(value.as_ref()).ok_or(CellStorageError::InvalidCell)?;
                self.inner.insert(
                    *key,
                    RawCellsCacheItem::from_header_and_slice(RawCellHeader { idx }, data),
                );
                Ok(Some(idx))
            }
            None => Ok(None),
        }
    }

    fn get_for_delete(
        &self,
        db: &CellsDb,
        key: &HashBytes,
        refs_buffer: &mut Vec<HashBytes>,
    ) -> Result<u32, CellStorageError> {
        refs_buffer.clear();

        // NOTE: `peek` here is used to avoid affecting a "hotness" of the value
        if let Some(value) = self.inner.peek(key) {
            return StorageCell::deserialize_references(&value.slice, refs_buffer)
                .then_some(value.header.header.idx)
                .ok_or(CellStorageError::InvalidCell);
        }

        match db
            .cells
            .get(key.as_slice())
            .map_err(CellStorageError::Internal)?
        {
            Some(value) => {
                let (idx, value) =
                    decode_cell_value(value.as_ref()).ok_or(CellStorageError::InvalidCell)?;
                StorageCell::deserialize_references(value, refs_buffer)
                    .then_some(idx)
                    .ok_or(CellStorageError::InvalidCell)
            }
            None => Err(CellStorageError::CellNotFound),
        }
    }

    fn on_insert_cell(&self, key: &HashBytes, idx: u32, data: Option<&[u8]>) {
        match data {
            None => {
                // NOTE: `get` here is used to affect a "hotness" of the value
                if let Some(v) = self.inner.get(key) {
                    debug_assert_eq!(v.header.header.idx, idx);
                }
            }
            Some(data) => self.inner.insert(
                *key,
                RawCellsCacheItem::from_header_and_slice(RawCellHeader { idx }, data),
            ),
        }
    }

    fn remove(&self, key: &HashBytes) {
        _ = self.inner.remove(key);
    }

    fn refresh_metrics(&self) {
        metrics::gauge!("tycho_storage_raw_cells_cache_size").set(self.inner.weight() as f64);
    }
}
