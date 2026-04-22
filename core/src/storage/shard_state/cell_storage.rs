use std::cell::UnsafeCell;
use std::collections::hash_map;
use std::mem::{ManuallyDrop, MaybeUninit};
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
#[cfg(feature = "cells-metrics")]
use std::time::Instant;

use anyhow::{Context, Result};
use bumpalo::Bump;
use bumpalo_herd::Herd;
use bytesize::ByteSize;
use quick_cache::sync::{Cache, DefaultLifecycle};
use triomphe::ThinArc;
use tycho_types::cell::*;
use tycho_util::metrics::{HistogramGuard, spawn_metrics_loop};
use tycho_util::{FastDashMap, FastHashMap, FastHashSet, FastHasherState};
use weedb::rocksdb::WriteBatch;
use weedb::{BoundedCfHandle, rocksdb};

use super::counters::{Counters, Idx, NextIdx, RefCount};
use super::db_state::{
    CellsDbStateError, CellsDbStateKey, ensure_table_is_empty, load_counter_snapshot,
    put_counter_snapshot,
};
use crate::storage::CellsDb;

pub struct CellStorage {
    cells_db: CellsDb,
    cells_cache: Arc<CellsIndex>,
    raw_cells_cache: Arc<RawCellsCache>,
    counters: Mutex<Counters>,
    drop_interval: u32,
}

type CellsIndex = FastDashMap<HashBytes, CachedCell>;

struct CachedCell {
    epoch: u32,
    weak: Weak<StorageCell>,
}

impl CellStorage {
    pub fn new(
        cells_db: CellsDb,
        cache_size_bytes: ByteSize,
        drop_interval: u32,
    ) -> Result<Arc<Self>> {
        let cells_cache = Default::default();
        let raw_cells_cache = Arc::new(RawCellsCache::new(cache_size_bytes.as_u64()));
        let counters =
            match load_counter_snapshot(&cells_db, CellsDbStateKey::CounterSnapshotLatest) {
                Ok(counters) => counters,
                Err(CellsDbStateError::MissingKey(CellsDbStateKey::CounterSnapshotLatest)) => {
                    ensure_table_is_empty(
                        &cells_db.cells,
                        "indexed cells DB is missing latest counters snapshot",
                    )?;
                    Counters::new(NextIdx::new(0))
                }
                Err(err) => return Err(err.into()),
            };

        spawn_metrics_loop(
            &raw_cells_cache.clone(),
            Duration::from_secs(5),
            |c| async move { c.refresh_metrics() },
        );

        Ok(Arc::new(Self {
            cells_db,
            cells_cache,
            raw_cells_cache,
            counters: Mutex::new(counters),
            drop_interval,
        }))
    }

    pub fn db(&self) -> &CellsDb {
        &self.cells_db
    }

    pub fn apply_temp_cell(&self, root: &HashBytes) -> Result<()> {
        const MAX_NEW_CELLS_BATCH_SIZE: usize = 10000;

        struct TempCell {
            idx: Idx,
            old_count: u64,
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
            cells_db: &'a CellsDb,
            counters: &'a mut Counters,
            buffer: Vec<u8>,
            transaction: FastHashMap<HashBytes, TempCell>,
            new_cells_batch: rocksdb::WriteBatch,
            new_cell_count: usize,
            raw_cache: &'a RawCellsCache,
        }

        impl<'a> Context<'a> {
            fn new(
                cells_db: &'a CellsDb,
                raw_cache: &'a RawCellsCache,
                counters: &'a mut Counters,
            ) -> Self {
                Self {
                    cells_cf: cells_db.cells.cf(),
                    cells_db,
                    counters,
                    buffer: Vec::with_capacity(512),
                    transaction: Default::default(),
                    new_cells_batch: rocksdb::WriteBatch::default(),
                    new_cell_count: 0,
                    raw_cache,
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
                Ok(match self.transaction.entry(*key) {
                    hash_map::Entry::Occupied(mut entry) => {
                        entry.get_mut().additions += 1; // 1 new reference
                        InsertedCell::Existing
                    }
                    hash_map::Entry::Vacant(entry) => {
                        if let Some(idx) = self.raw_cache.get_idx(self.cells_db, key)? {
                            entry.insert(TempCell {
                                idx,
                                old_count: self.counters.get(idx)?.get(),
                                additions: 1, // 1 new reference
                            });
                            return Ok(InsertedCell::Existing);
                        }

                        let idx = self.counters.alloc_idx();
                        entry.insert(TempCell {
                            idx,
                            old_count: 0,
                            additions: 1,
                        });
                        let iter = self.load_temp(key)?;

                        encode_indexed_value(idx, iter.data.as_ref(), &mut self.buffer);

                        self.new_cells_batch
                            .put_cf(&self.cells_cf, key, self.buffer.as_slice());

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
                }
                Ok(())
            }

            fn flush_existing_cells(self) -> Result<(), CellStorageError> {
                let mut batch = rocksdb::WriteBatch::default();

                for (_, item) in self.transaction {
                    let new_count = item.old_count + u64::from(item.additions);
                    if new_count != 1 {
                        self.counters.set(item.idx, RefCount::new(new_count))?;
                    }
                }

                put_counter_snapshot(
                    &mut batch,
                    self.cells_db,
                    CellsDbStateKey::CounterSnapshotLatest,
                    self.counters,
                )
                .map_err(CellStorageError::State)?;

                self.cells_db.rocksdb().write(batch)?;
                Ok(())
            }
        }

        let mut counters = self.counters.lock().unwrap();
        let mut ctx = Context::new(&self.cells_db, &self.raw_cells_cache, &mut counters);

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
        _split_at: FastHashMap<HashBytes, Cell>,
        capacity: usize,
    ) -> Result<usize, CellStorageError> {
        self.store_cell(batch, root, capacity)
    }

    pub fn store_cell(
        &self,
        batch: &mut WriteBatch,
        root: &DynCell,
        estimated_cell_count: usize,
    ) -> Result<usize, CellStorageError> {
        struct AddedCell<'a> {
            idx: Idx,
            old_count: u64,
            additions: u32,
            data: Option<&'a [u8]>,
        }

        struct Context<'a> {
            db: &'a CellsDb,
            raw_cells_cache: &'a RawCellsCache,
            counters: &'a mut Counters,
            alloc: &'a Bump,
            transaction: FastHashMap<&'a HashBytes, AddedCell<'a>>,
            buffer: Vec<u8>,
        }

        impl<'a> Context<'a> {
            fn insert_cell(&mut self, cell: &'a DynCell) -> Result<bool, CellStorageError> {
                let key = cell.repr_hash();
                Ok(match self.transaction.entry(key) {
                    hash_map::Entry::Occupied(mut value) => {
                        value.get_mut().additions += 1;
                        false
                    }
                    hash_map::Entry::Vacant(entry) => {
                        let (idx, old_count, data) =
                            match self.raw_cells_cache.get_idx(self.db, key)? {
                                Some(idx) => (idx, self.counters.get(idx)?.get(), None),
                                None => {
                                    self.buffer.clear();
                                    if StorageCell::serialize_to(cell, &mut self.buffer).is_err() {
                                        return Err(CellStorageError::InvalidCell);
                                    }
                                    let idx = self.counters.alloc_idx();
                                    let data = self.alloc.alloc_slice_copy(self.buffer.as_slice())
                                        as &[u8];
                                    (idx, 0, Some(data))
                                }
                            };

                        entry.insert(AddedCell {
                            idx,
                            old_count,
                            additions: 1,
                            data,
                        });
                        data.is_some()
                    }
                })
            }

            fn finalize(
                mut self,
                batch: &mut rocksdb::WriteBatch,
            ) -> Result<usize, CellStorageError> {
                let total = self.transaction.len();
                let cells_cf = &self.db.cells.cf();

                for (key, item) in self.transaction {
                    let new_count = item.old_count + u64::from(item.additions);

                    if let Some(data) = item.data {
                        encode_indexed_value(item.idx, data, &mut self.buffer);
                        batch.put_cf(cells_cf, key.as_slice(), &self.buffer);
                    }

                    if new_count == 1 {
                        if item.old_count > 0 {
                            self.counters.set(item.idx, RefCount::ONE)?;
                        }
                    } else {
                        self.counters.set(item.idx, RefCount::new(new_count))?;
                    }

                    if let Some(data) = item.data {
                        self.raw_cells_cache.inner.insert(
                            *key,
                            RawCellsCacheItem::from_header_and_slice(
                                AtomicU64::new(item.idx.get()),
                                data,
                            ),
                        );
                    }
                }

                put_counter_snapshot(
                    batch,
                    self.db,
                    CellsDbStateKey::CounterSnapshotLatest,
                    self.counters,
                )?;
                Ok(total)
            }
        }

        let alloc = bumpalo::Bump::new();
        let mut counters = self.counters.lock().unwrap();

        // Prepare context and handles
        let mut ctx = Context {
            db: &self.cells_db,
            raw_cells_cache: &self.raw_cells_cache,
            counters: &mut counters,
            alloc: &alloc,
            transaction: FastHashMap::with_capacity_and_hasher(
                estimated_cell_count,
                Default::default(),
            ),
            buffer: Vec::with_capacity(512),
        };

        'visit: {
            // Check root cell
            if !ctx.insert_cell(root.as_ref())? {
                break 'visit;
            }
            let mut stack = Vec::with_capacity(16);
            stack.push(root.references());

            // Check other cells
            'outer: loop {
                let Some(iter) = stack.last_mut() else {
                    break;
                };

                for child in &mut *iter {
                    if ctx.insert_cell(child)? {
                        stack.push(child.references());
                        continue 'outer;
                    }
                }

                stack.pop();
            }
        }

        ctx.finalize(batch)
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

        let mut cell = match self.raw_cells_cache.get_raw(&self.cells_db, hash) {
            Ok(Some(value)) => {
                match StorageCell::deserialize(self.clone(), hash, &value.slice, epoch) {
                    Some(cell) => Arc::new(cell),
                    None => return Err(CellStorageError::InvalidCell),
                }
            }
            Ok(None) => return Err(CellStorageError::CellNotFound),
            Err(e) => return Err(CellStorageError::Internal(e)),
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
        _herd: &Herd,
        root: &HashBytes,
        _split_at: FastHashSet<HashBytes>,
    ) -> Result<(usize, WriteBatch), CellStorageError> {
        let alloc = bumpalo::Bump::new();
        self.remove_cell(&alloc, root)
    }

    #[allow(unused)]
    pub fn remove_cell(
        &self,
        alloc: &Bump,
        hash: &HashBytes,
    ) -> Result<(usize, WriteBatch), CellStorageError> {
        let cells = &self.cells_db.cells;
        let cells_cf = &cells.cf();
        let mut counters = self.counters.lock().unwrap();

        let mut transaction: FastHashMap<&HashBytes, RemovedCell<'_>> =
            FastHashMap::with_capacity_and_hasher(128, Default::default());
        let mut buffer = Vec::with_capacity(4);

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
                        let idx = self.raw_cells_cache.get_idx_for_delete(
                            &self.cells_db,
                            cell_id,
                            &mut buffer,
                        )?;
                        let old_count = counters.get(idx)?.get();
                        let refs = alloc.alloc_slice_copy(buffer.as_slice()) as &[HashBytes];

                        v.insert(RemovedCell {
                            idx,
                            old_count,
                            removes: 1,
                            refs,
                        });
                        if old_count == 1 { Some(refs) } else { None }
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
            let new_count = item
                .old_count
                .checked_sub(u64::from(item.removes))
                .expect("checked in RemovedCell::remove");
            if new_count == 0 {
                batch.delete_cf(cells_cf, key.as_slice());
                counters.remove(item.idx)?;
                self.raw_cells_cache.inner.remove(key);
                continue;
            }

            counters.set(item.idx, RefCount::new(new_count))?;
        }

        put_counter_snapshot(
            &mut batch,
            &self.cells_db,
            CellsDbStateKey::CounterSnapshotLatest,
            &counters,
        )?;
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
    idx: Idx,
    old_count: u64,
    removes: u32,
    refs: &'a [HashBytes],
}

impl<'a> RemovedCell<'a> {
    fn remove(&mut self) -> Result<Option<&'a [HashBytes]>, CellStorageError> {
        self.removes += 1;
        if u64::from(self.removes) > self.old_count {
            return Err(CellStorageError::CounterMismatch {
                expected: self.old_count,
                actual: self.removes,
            });
        }

        Ok((self.old_count == u64::from(self.removes)).then_some(self.refs))
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

type RawCellsCacheItem = ThinArc<AtomicU64, u8>;

#[derive(Clone, Copy)]
pub struct CellSizeEstimator;
impl quick_cache::Weighter<HashBytes, RawCellsCacheItem> for CellSizeEstimator {
    fn weight(&self, key: &HashBytes, val: &RawCellsCacheItem) -> u64 {
        const STATIC_SIZE: usize = std::mem::size_of::<RawCellsCacheItem>()
            + std::mem::size_of::<u64>()
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

    // Previously used `get_value_or_guard` which inserts into cache on miss.
    // This caused two problems:
    // 1. Memory leak: `get_value_or_guard` allocates a Placeholder in LinkedSlab
    //    BEFORE eviction runs (unlike `insert()` which evicts first).
    // 2. Performance: `get_value_or_guard` holds the shard lock while the caller
    //    reads from RocksDB, blocking all other operations on the same shard.
    fn get_raw(
        &self,
        db: &CellsDb,
        key: &HashBytes,
    ) -> Result<Option<RawCellsCacheItem>, rocksdb::Error> {
        if let Some(value) = self.inner.get(key) {
            return Ok(Some(value));
        }

        // Fallback to RocksDB
        let value = {
            #[cfg(feature = "cells-metrics")]
            let _timer = scopeguard::guard(Instant::now(), |started_at| {
                self.rocksdb_access_histogram.record(started_at.elapsed());
            });

            db.cells.get(key.as_slice())?
        };

        Ok(value.and_then(|value| {
            let (idx, data) = decode_indexed_value(value.as_ref())?;
            let item = RawCellsCacheItem::from_header_and_slice(AtomicU64::new(idx.get()), data);
            self.inner.insert(*key, item.clone());
            Some(item)
        }))
    }

    fn get_idx(&self, db: &CellsDb, key: &HashBytes) -> Result<Option<Idx>, CellStorageError> {
        if let Some(entry) = self.inner.get(key) {
            return Ok(Some(Idx::new(entry.header.header.load(Ordering::Acquire))));
        }

        let Some(value) = db.cells.get(key).map_err(CellStorageError::Internal)? else {
            return Ok(None);
        };
        let Some((idx, data)) = decode_indexed_value(value.as_ref()) else {
            return Err(CellStorageError::InvalidCell);
        };
        let item = RawCellsCacheItem::from_header_and_slice(AtomicU64::new(idx.get()), data);
        self.inner.insert(*key, item);
        Ok(Some(idx))
    }

    fn get_idx_for_delete(
        &self,
        db: &CellsDb,
        key: &HashBytes,
        refs_buffer: &mut Vec<HashBytes>,
    ) -> Result<Idx, CellStorageError> {
        refs_buffer.clear();

        if let Some(value) = self.inner.peek(key) {
            return StorageCell::deserialize_references(&value.slice, refs_buffer)
                .then_some(Idx::new(value.header.header.load(Ordering::Acquire)))
                .ok_or(CellStorageError::InvalidCell);
        }

        let Some(value) = db
            .cells
            .get(key.as_slice())
            .map_err(CellStorageError::Internal)?
        else {
            return Err(CellStorageError::CellNotFound);
        };
        let Some((idx, data)) = decode_indexed_value(value.as_ref()) else {
            return Err(CellStorageError::InvalidCell);
        };
        let item = RawCellsCacheItem::from_header_and_slice(AtomicU64::new(idx.get()), data);
        self.inner.insert(*key, item);
        StorageCell::deserialize_references(data, refs_buffer)
            .then_some(idx)
            .ok_or(CellStorageError::InvalidCell)
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

pub fn encode_indexed_value(idx: Idx, data: &[u8], out: &mut Vec<u8>) {
    out.clear();
    out.reserve(size_of::<u64>() + data.len());
    out.extend_from_slice(&idx.get().to_le_bytes());
    out.extend_from_slice(data);
}

pub fn decode_indexed_value(value: &[u8]) -> Option<(Idx, &[u8])> {
    let (prefix, data) = value.split_at_checked(size_of::<u64>())?;
    let idx = Idx::new(u64::from_le_bytes(prefix.try_into().unwrap()));
    Some((idx, data))
}

#[cfg(test)]
mod tests {
    use tycho_storage::StorageContext;
    use tycho_types::merkle::make_pruned_branch;

    use super::*;
    use crate::storage::{CoreStorage, CoreStorageConfig};

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
            let mut batch = rocksdb::WriteBatch::default();
            cell_storage.store_cell(&mut batch, root.as_ref(), 1)?;
            cell_storage
                .cells_db
                .rocksdb()
                .write_opt(batch, cell_storage.cells_db.cells.write_config())?;
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
        let mut alloc = bumpalo::Bump::new();
        for root in &cells {
            let (_, batch) = cell_storage.remove_cell(&alloc, root.repr_hash())?;
            cell_storage
                .cells_db
                .rocksdb()
                .write_opt(batch, cell_storage.cells_db.cells.write_config())?;
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

    #[test]
    fn indexed_value_helpers_roundtrip() {
        let idx = Idx::new(42);
        let payload = [1u8, 2, 3, 4];
        let mut value = Vec::new();

        encode_indexed_value(idx, &payload, &mut value);

        assert_eq!(
            decode_indexed_value(&value),
            Some((idx, payload.as_slice()))
        );
    }

    #[test]
    fn indexed_value_helpers_reject_short_inputs() {
        assert_eq!(decode_indexed_value(&[1, 2, 3]), None);
    }
}
