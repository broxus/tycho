use std::cell::UnsafeCell;
use std::collections::hash_map;
use std::mem::{ManuallyDrop, MaybeUninit};
use std::sync::atomic::{AtomicI64, AtomicU8, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use bumpalo::Bump;
use bytesize::ByteSize;
use dashmap::mapref::entry::Entry;
use dashmap::Map;
use everscale_types::cell::*;
use quick_cache::sync::{Cache, DefaultLifecycle};
use triomphe::ThinArc;
use tycho_util::metrics::{spawn_metrics_loop, HistogramGuard};
use tycho_util::{FastDashMap, FastHashMap, FastHasherState};
use weedb::rocksdb::WriteBatch;
use weedb::{rocksdb, BoundedCfHandle};

use crate::db::*;

pub struct CellStorage {
    db: CellsDb,
    cells_cache: Arc<CellsCache>,
    raw_cells_cache: Arc<RawCellsCache>,
}

type CellsCache = FastDashMap<HashBytes, Weak<StorageCell>>;

impl CellStorage {
    pub fn new(db: CellsDb, cache_size_bytes: ByteSize) -> Arc<Self> {
        let cells_cache = Default::default();
        let raw_cells_cache = Arc::new(RawCellsCache::new(cache_size_bytes.as_u64()));

        spawn_metrics_loop(
            &raw_cells_cache.clone(),
            Duration::from_secs(5),
            |c| async move { c.refresh_metrics() },
        );

        Arc::new(Self {
            db,
            cells_cache,
            raw_cells_cache,
        })
    }

    pub fn create_store_ctx(&self, capacity: usize) -> StoreContext {
        StoreContext::new(&self.db, &self.raw_cells_cache, capacity)
    }

    pub fn apply_temp_cell(&self, root: &HashBytes) -> Result<()> {
        const MAX_NEW_CELLS_BATCH_SIZE: usize = 10000;

        struct TempCell {
            old_rc: i64,
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
            db: &'a CellsDb,
            buffer: Vec<u8>,
            transaction: FastHashMap<HashBytes, TempCell>,
            new_cells_batch: rocksdb::WriteBatch,
            new_cell_count: usize,
            raw_cache: &'a RawCellsCache,
        }

        impl<'a> Context<'a> {
            fn new(db: &'a CellsDb, raw_cache: &'a RawCellsCache) -> Self {
                Self {
                    cells_cf: db.cells.cf(),
                    db,
                    buffer: Vec::with_capacity(512),
                    transaction: Default::default(),
                    new_cells_batch: rocksdb::WriteBatch::default(),
                    new_cell_count: 0,
                    raw_cache,
                }
            }

            fn load_temp(&self, key: &HashBytes) -> Result<CellHashesIter<'a>, CellStorageError> {
                let data = match self.db.temp_cells.get(key) {
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
                        if let Some(value) = self.db.cells.get(key)? {
                            let (rc, value) = refcount::decode_value_with_rc(value.as_ref());
                            debug_assert!(rc > 0 && value.is_some() || rc == 0 && value.is_none());
                            if value.is_some() {
                                entry.insert(TempCell {
                                    old_rc: rc,
                                    additions: 1, // 1 new reference
                                });
                                return Ok(InsertedCell::Existing);
                            }
                        }

                        entry.insert(TempCell {
                            old_rc: 0,
                            additions: 1,
                        });
                        let iter = self.load_temp(key)?;

                        self.buffer.clear();
                        refcount::add_positive_refount(
                            1,
                            Some(iter.data.as_ref()),
                            &mut self.buffer,
                        );

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
                    self.db
                        .rocksdb()
                        .write(std::mem::take(&mut self.new_cells_batch))?;
                    self.new_cell_count = 0;
                }
                Ok(())
            }

            fn flush_existing_cells(mut self) -> Result<(), rocksdb::Error> {
                let mut batch = rocksdb::WriteBatch::default();

                for (key, item) in self.transaction {
                    let mut refs_diff = item.additions;
                    if item.old_rc == 0 {
                        // 1 reference was added with the data while traversing the tree.
                        refs_diff -= 1;
                    }

                    if refs_diff > 0 {
                        self.buffer.clear();
                        refcount::add_positive_refount(refs_diff, None, &mut self.buffer);
                        batch.merge_cf(&self.cells_cf, key, self.buffer.as_slice());
                    }

                    let new_rc = item.old_rc + item.additions as i64;
                    self.raw_cache.on_insert_cell(&key, new_rc, None);
                }

                self.db.rocksdb().write(batch)
            }
        }

        let mut ctx = Context::new(&self.db, &self.raw_cells_cache);

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

    pub fn store_cell(root: &DynCell, ctx: &StoreContext) -> Result<(), CellStorageError> {
        // Check root cell
        if !ctx.insert_cell(root.as_ref(), 0)? {
            return Ok(());
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

        // Write transaction to the `WriteBatch`
        Ok(())
    }

    pub fn load_cell(
        self: &Arc<Self>,
        hash: HashBytes,
    ) -> Result<Arc<StorageCell>, CellStorageError> {
        let _histogram = HistogramGuard::begin("tycho_storage_load_cell_time");

        if let Some(cell) = self.cells_cache.get(&hash) {
            if let Some(cell) = cell.upgrade() {
                return Ok(cell);
            }
        }

        let cell = match self.raw_cells_cache.get_raw(&self.db, &hash) {
            Ok(Some(value)) => match StorageCell::deserialize(self.clone(), &value.slice) {
                Some(cell) => Arc::new(cell),
                None => return Err(CellStorageError::InvalidCell),
            },
            Ok(None) => return Err(CellStorageError::CellNotFound),
            Err(e) => return Err(CellStorageError::Internal(e)),
        };

        if self
            .cells_cache
            .insert(hash, Arc::downgrade(&cell))
            .is_none()
        {
            metrics::gauge!("tycho_storage_cells_tree_cache_size").increment(1f64);
        }

        Ok(cell)
    }

    pub fn remove_cell(
        &self,
        alloc: &Bump,
        hash: &HashBytes,
    ) -> Result<(usize, WriteBatch), CellStorageError> {
        #[derive(Clone, Copy)]
        struct RemovedCell<'a> {
            old_rc: i64,
            removes: u32,
            refs: &'a [HashBytes],
        }

        impl<'a> RemovedCell<'a> {
            fn remove(&mut self) -> Result<Option<&'a [HashBytes]>, CellStorageError> {
                self.removes += 1;
                if self.removes as i64 <= self.old_rc {
                    Ok(self.next_refs())
                } else {
                    Err(CellStorageError::CounterMismatch)
                }
            }

            fn next_refs(&self) -> Option<&'a [HashBytes]> {
                if self.old_rc > self.removes as i64 {
                    None
                } else {
                    Some(self.refs)
                }
            }
        }

        let cells = &self.db.cells;
        let cells_cf = &cells.cf();

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
                        let old_rc = self.raw_cells_cache.get_rc_for_delete(
                            &self.db,
                            cell_id,
                            &mut buffer,
                        )?;
                        debug_assert!(old_rc > 0);

                        v.insert(RemovedCell {
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
        let _hist = HistogramGuard::begin("tycho_storage_batch_write_time");
        let total = transaction.len();

        // NOTE: For each cell we have 32 bytes for key and 8 bytes for RC,
        //       and a bit more just in case.
        let mut batch = WriteBatch::with_capacity_bytes(total * (32 + 8 + 8));

        for (key, item) in transaction {
            batch.merge_cf(
                cells_cf,
                key.as_slice(),
                refcount::encode_negative_refcount(item.removes),
            );

            let new_rc = item.old_rc - item.removes as i64;
            self.raw_cells_cache.on_remove_cell(key, new_rc);
        }

        Ok((total, batch))
    }

    pub fn drop_cell(&self, hash: &HashBytes) {
        if self.cells_cache.remove(hash).is_some() {
            metrics::gauge!("tycho_storage_cells_tree_cache_size").decrement(1f64);
        }
    }
}

struct CellWithRefs {
    rc: u32,
    data: Option<Vec<u8>>,
}

pub struct StoreContext {
    db: CellsDb,
    raw_cache: Arc<RawCellsCache>,
    transaction: FastDashMap<HashBytes, CellWithRefs>,
}

impl StoreContext {
    fn new(db: &CellsDb, raw_cache: &Arc<RawCellsCache>, capacity: usize) -> Self {
        Self {
            db: db.clone(),
            raw_cache: raw_cache.clone(),
            transaction: FastDashMap::with_capacity_and_hasher_and_shard_amount(
                capacity,
                Default::default(),
                512,
            ),
        }
    }

    fn insert_cell(&self, cell: &DynCell, depth: usize) -> Result<bool, CellStorageError> {
        let mut buffer = [0; 512];

        let key = cell.repr_hash();
        Ok(match self.transaction.entry(*key) {
            Entry::Occupied(mut value) => {
                value.get_mut().rc += 1;
                false
            }
            Entry::Vacant(entry) => {
                // A constant which tells since which depth we should start to use cache.
                // This method is used mostly for inserting new states, so we can assume
                // that first N levels will mostly be new.
                //
                // This value was chosen empirically.
                const NEW_CELLS_DEPTH_THRESHOLD: usize = 4;

                let (old_rc, has_value) = 'value: {
                    if depth >= NEW_CELLS_DEPTH_THRESHOLD {
                        // NOTE: `get` here is used to affect a "hotness" of the value, because
                        // there is a big chance that we will need it soon during state processing
                        if let Some(entry) = self.raw_cache.inner.get(key) {
                            let rc = entry.header.header.load(Ordering::Acquire);
                            break 'value (rc, rc > 0);
                        }
                    }

                    match self.db.cells.get(key).map_err(CellStorageError::Internal)? {
                        Some(value) => {
                            let (rc, value) = refcount::decode_value_with_rc(value.as_ref());
                            (rc, value.is_some())
                        }
                        None => (0, false),
                    }
                };

                // TODO: lower to `debug_assert` when sure
                assert!(has_value && old_rc > 0 || !has_value && old_rc == 0);

                let data = if !has_value {
                    match StorageCell::serialize_to(cell, &mut buffer) {
                        Err(_) => return Err(CellStorageError::InvalidCell),
                        Ok(size) => Some(buffer[..size].to_vec()),
                    }
                } else {
                    None
                };
                entry.insert(CellWithRefs { rc: 1, data });
                !has_value
            }
        })
    }

    pub fn finalize(self, batch: &mut WriteBatch) -> usize {
        std::thread::scope(|s| {
            let number_shards = self.transaction._shard_count();
            // safety: we hold only read locks
            let shards = unsafe { (0..number_shards).map(|i| self.transaction._get_read_shard(i)) };
            let cache = &self.raw_cache;

            // todo: clamp to number of cpus x2
            for shard in shards {
                // spawned threads will be joined at the end of the scope, so we don't need to store them
                s.spawn(move || {
                    for (key, value) in shard {
                        let value = value.get();
                        let rc = value.rc;
                        if let Some(data) = &value.data {
                            cache.insert(key, rc, data);
                        } else {
                            cache.add_refs(key, rc);
                        }
                    }
                });
            }

            let batch_update = s.spawn(|| {
                let mut buffer = Vec::with_capacity(512);
                let total = self.transaction.len();
                let cells_cf = &self.db.cells.cf();
                for kv in self.transaction.iter() {
                    let key = kv.key();
                    let value = kv.value();
                    let rc = value.rc;
                    let data = value.data.as_deref();

                    buffer.clear();
                    refcount::add_positive_refount(rc, data, &mut buffer);
                    batch.merge_cf(cells_cf, key.as_slice(), &buffer);
                }
                total
            });

            batch_update.join().expect("thread panicked")
        })
    }
}

#[derive(thiserror::Error, Debug)]
pub enum CellStorageError {
    #[error("Cell not found in cell db")]
    CellNotFound,
    #[error("Invalid cell")]
    InvalidCell,
    #[error("Cell counter mismatch")]
    CounterMismatch,
    #[error("Internal rocksdb error")]
    Internal(#[from] rocksdb::Error),
}

pub struct StorageCell {
    cell_storage: Arc<CellStorage>,
    descriptor: CellDescriptor,
    bit_len: u16,
    data: Vec<u8>,
    hashes: Vec<(HashBytes, u16)>,

    reference_states: [AtomicU8; 4],
    reference_data: [UnsafeCell<StorageCellReferenceData>; 4],
}

impl StorageCell {
    const REF_EMPTY: u8 = 0x0;
    const REF_RUNNING: u8 = 0x1;
    const REF_STORAGE: u8 = 0x2;
    const REF_REPLACED: u8 = 0x3;

    pub fn deserialize(cell_storage: Arc<CellStorage>, buffer: &[u8]) -> Option<Self> {
        if buffer.len() < 4 {
            return None;
        }

        let descriptor = CellDescriptor::new([buffer[0], buffer[1]]);
        let bit_len = u16::from_le_bytes([buffer[2], buffer[3]]);
        let byte_len = descriptor.byte_len() as usize;
        let hash_count = descriptor.hash_count() as usize;
        let ref_count = descriptor.reference_count() as usize;

        let total_len = 4usize + byte_len + (32 + 2) * hash_count + 32 * ref_count;
        if buffer.len() < total_len {
            return None;
        }

        let data = buffer[4..4 + byte_len].to_vec();

        let mut hashes = Vec::with_capacity(hash_count);
        let mut offset = 4 + byte_len;
        for _ in 0..hash_count {
            hashes.push((
                HashBytes::from_slice(&buffer[offset..offset + 32]),
                u16::from_le_bytes([buffer[offset + 32], buffer[offset + 33]]),
            ));
            offset += 32 + 2;
        }

        let reference_states = Default::default();
        let reference_data = unsafe {
            MaybeUninit::<[UnsafeCell<StorageCellReferenceData>; 4]>::uninit().assume_init()
        };

        for slot in reference_data.iter().take(ref_count) {
            let slot = slot.get().cast::<u8>();
            unsafe { std::ptr::copy_nonoverlapping(buffer.as_ptr().add(offset), slot, 32) };
            offset += 32;
        }

        Some(Self {
            cell_storage,
            bit_len,
            descriptor,
            data,
            hashes,
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

    pub fn serialize_to(cell: &DynCell, target: &mut [u8]) -> Result<usize> {
        let descriptor = cell.descriptor();
        let hash_count = descriptor.hash_count();
        let ref_count = descriptor.reference_count();

        let expected_len = 4usize
            + descriptor.byte_len() as usize
            + (32 + 2) * hash_count as usize
            + 32 * ref_count as usize;
        assert!(target.len() >= expected_len);

        let mut cursor = 0;

        // Copy descriptor bytes
        target[cursor..cursor + 2].copy_from_slice(&[descriptor.d1, descriptor.d2]);
        cursor += 2;

        // Copy bit length
        target[cursor..cursor + 2].copy_from_slice(&cell.bit_len().to_le_bytes());
        cursor += 2;

        // Copy cell data
        let data_len = descriptor.byte_len() as usize;
        target[cursor..cursor + data_len].copy_from_slice(cell.data());
        cursor += data_len;

        assert_eq!(cell.data().len(), descriptor.byte_len() as usize);

        for i in 0..hash_count {
            target[cursor..cursor + 32].copy_from_slice(cell.hash(i).as_array());
            cursor += 32;

            target[cursor..cursor + 2].copy_from_slice(&cell.depth(i).to_le_bytes());
            cursor += 2;
        }

        for i in 0..ref_count {
            let cell = cell.reference(i).context("Child not found")?;
            target[cursor..cursor + 32].copy_from_slice(cell.repr_hash().as_array());
            cursor += 32;
        }

        Ok(cursor)
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
            .load_cell(unsafe { (*slot).hash })
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
        &self.data
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
        &self.hashes[i as usize].0
    }

    fn depth(&self, level: u8) -> u16 {
        let i = self.descriptor.level_mask().hash_index(level);
        self.hashes[i as usize].1
    }

    fn take_first_child(&mut self) -> Option<Cell> {
        let state = self.reference_states[0].swap(Self::REF_EMPTY, Ordering::AcqRel);
        let data = self.reference_data[0].get_mut();
        match state {
            Self::REF_STORAGE => Some(unsafe { data.take_storage_cell() }),
            Self::REF_REPLACED => Some(unsafe { data.take_replaced_cell() }),
            _ => None,
        }
    }

    fn replace_first_child(&mut self, parent: Cell) -> std::result::Result<Cell, Cell> {
        let state = self.reference_states[0].load(Ordering::Acquire);
        if state < Self::REF_STORAGE {
            return Err(parent);
        }

        self.reference_states[0].store(Self::REF_REPLACED, Ordering::Release);
        let data = self.reference_data[0].get_mut();

        let cell = match state {
            Self::REF_STORAGE => unsafe { data.take_storage_cell() },
            Self::REF_REPLACED => unsafe { data.take_replaced_cell() },
            _ => return Err(parent),
        };
        data.replaced_cell = ManuallyDrop::new(parent);
        Ok(cell)
    }

    fn take_next_child(&mut self) -> Option<Cell> {
        while self.descriptor.reference_count() > 1 {
            self.descriptor.d1 -= 1;
            let idx = (self.descriptor.d1 & CellDescriptor::REF_COUNT_MASK) as usize;

            let state = self.reference_states[idx].swap(Self::REF_EMPTY, Ordering::AcqRel);
            let data = self.reference_data[idx].get_mut();

            return Some(match state {
                Self::REF_STORAGE => unsafe { data.take_storage_cell() },
                Self::REF_REPLACED => unsafe { data.take_replaced_cell() },
                _ => continue,
            });
        }

        None
    }

    fn stats(&self) -> CellTreeStats {
        // TODO: make real implementation

        // STUB: just return default value
        Default::default()
    }
}

impl Drop for StorageCell {
    fn drop(&mut self) {
        self.cell_storage.drop_cell(DynCell::repr_hash(self));
        for i in 0..4 {
            let state = self.reference_states[i].load(Ordering::Acquire);
            let data = self.reference_data[i].get_mut();

            unsafe {
                match state {
                    Self::REF_STORAGE => ManuallyDrop::drop(&mut data.storage_cell),
                    Self::REF_REPLACED => ManuallyDrop::drop(&mut data.replaced_cell),
                    _ => {}
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
    /// Replaced state.
    replaced_cell: ManuallyDrop<Cell>,
}

impl StorageCellReferenceData {
    unsafe fn take_storage_cell(&mut self) -> Cell {
        Cell::from(ManuallyDrop::take(&mut self.storage_cell) as Arc<_>)
    }

    unsafe fn take_replaced_cell(&mut self) -> Cell {
        ManuallyDrop::take(&mut self.replaced_cell)
    }
}

struct RawCellsCache {
    inner: Cache<HashBytes, RawCellsCacheItem, CellSizeEstimator, FastHasherState>,
    rocksdb_access_histogram: metrics::Histogram,
}

type RawCellsCacheItem = ThinArc<AtomicI64, u8>;

#[derive(Clone, Copy)]
pub struct CellSizeEstimator;
impl quick_cache::Weighter<HashBytes, RawCellsCacheItem> for CellSizeEstimator {
    fn weight(&self, key: &HashBytes, val: &RawCellsCacheItem) -> u64 {
        const STATIC_SIZE: usize = std::mem::size_of::<RawCellsCacheItem>()
            + std::mem::size_of::<i64>()
            + std::mem::size_of::<usize>() * 2; // ArcInner refs + HeaderWithLength length

        let len = key.0.len() + val.slice.len() + STATIC_SIZE;
        len as u64
    }
}

impl RawCellsCache {
    const RC_NAN: i64 = i64::MAX;

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
            rocksdb_access_histogram: metrics::histogram!(
                "tycho_storage_get_cell_from_rocksdb_time"
            ),
        }
    }

    fn get_raw(
        &self,
        db: &CellsDb,
        key: &HashBytes,
    ) -> Result<Option<RawCellsCacheItem>, rocksdb::Error> {
        use quick_cache::sync::GuardResult;

        match self.inner.get_value_or_guard(key, None) {
            GuardResult::Value(value) => Ok(Some(value)),
            GuardResult::Guard(g) => {
                let value = {
                    let started_at = Instant::now();
                    scopeguard::defer! {
                        self.rocksdb_access_histogram.record(started_at.elapsed());
                    }

                    db.cells.get(key.as_slice())?
                };

                Ok(if let Some(value) = value {
                    let (_, data) = refcount::decode_value_with_rc(value.as_ref());
                    data.map(|value| {
                        let value = RawCellsCacheItem::from_header_and_slice(
                            AtomicI64::new(Self::RC_NAN),
                            value,
                        );
                        _ = g.insert(value.clone());
                        value
                    })
                } else {
                    None
                })
            }
            GuardResult::Timeout => unreachable!(),
        }
    }

    fn get_rc_for_insert(
        &self,
        db: &CellsDb,
        key: &HashBytes,
        depth: usize,
    ) -> Result<i64, CellStorageError> {
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
                let rc = entry.header.header.load(Ordering::Acquire);
                if rc != Self::RC_NAN {
                    return Ok(rc);
                }
            }
        }

        match db.cells.get(key).map_err(CellStorageError::Internal)? {
            Some(value) => {
                let (rc, value) = refcount::decode_value_with_rc(value.as_ref());

                // TODO: lower to `debug_assert` when sure
                let has_value = value.is_some();
                assert!(has_value && rc > 0 || !has_value && rc == 0);

                Ok(rc)
            }
            None => Ok(0),
        }
    }

    fn get_rc_for_delete(
        &self,
        db: &CellsDb,
        key: &HashBytes,
        refs_buffer: &mut Vec<HashBytes>,
    ) -> Result<i64, CellStorageError> {
        refs_buffer.clear();

        // NOTE: `peek` here is used to avoid affecting a "hotness" of the value
        if let Some(value) = self.inner.peek(key) {
            let rc = value.header.header.load(Ordering::Acquire);
            if rc <= 0 {
                return Err(CellStorageError::CellNotFound);
            } else if rc != i64::MAX {
                return StorageCell::deserialize_references(&value.slice, refs_buffer)
                    .then_some(rc)
                    .ok_or(CellStorageError::InvalidCell);
            }
        }

        match db.cells.get(key.as_slice()) {
            Ok(value) => {
                if let Some(value) = value {
                    if let (rc, Some(value)) = refcount::decode_value_with_rc(&value) {
                        return StorageCell::deserialize_references(value, refs_buffer)
                            .then_some(rc)
                            .ok_or(CellStorageError::InvalidCell);
                    }
                }

                Err(CellStorageError::CellNotFound)
            }
            Err(e) => Err(CellStorageError::Internal(e)),
        }
    }

    fn on_insert_cell(&self, key: &HashBytes, rc: i64, data: Option<&[u8]>) {
        match data {
            None => {
                // NOTE: `get` here is used to affect a "hotness" of the value
                if let Some(v) = self.inner.get(key) {
                    v.header.header.store(rc, Ordering::Release);
                }
            }
            Some(data) => self.inner.insert(
                *key,
                RawCellsCacheItem::from_header_and_slice(AtomicI64::new(rc), data),
            ),
        }
    }

    fn on_remove_cell(&self, key: &HashBytes, rc: i64) {
        let v = if rc <= 0 {
            debug_assert_eq!(rc, 0, "too many removed cells");

            match self.inner.remove(key) {
                None => return,
                Some((_, v)) => v,
            }
        } else {
            // NOTE: `peek` here is used to avoid affecting a "hotness" of the value
            match self.inner.peek(key) {
                None => return,
                Some(v) => v,
            }
        };

        v.header.header.store(rc, Ordering::Release);
    }

    fn insert(&self, key: &HashBytes, refs: u32, value: &[u8]) {
        let value = RawCellsCacheItem::from_header_and_slice(AtomicI64::new(refs as _), value);
        self.inner.insert(*key, value);
    }

    fn add_refs(&self, key: &HashBytes, refs: u32) {
        // NOTE: `peek` here is used to avoid affecting a "hotness" of the value
        if let Some(v) = self.inner.peek(key) {
            v.header.header.fetch_add(refs as i64, Ordering::Release);
        }
    }

    fn refresh_metrics(&self) {
        metrics::gauge!("tycho_storage_raw_cells_cache_size").set(self.inner.weight() as f64);
    }
}
