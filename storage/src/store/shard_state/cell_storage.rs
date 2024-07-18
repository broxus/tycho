use std::cell::UnsafeCell;
use std::collections::hash_map;
use std::mem::{ManuallyDrop, MaybeUninit};
use std::sync::atomic::{AtomicI64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

use anyhow::{Context, Result};
use bumpalo::Bump;
use everscale_types::cell::*;
use quick_cache::sync::{Cache, DefaultLifecycle};
use triomphe::ThinArc;
use tycho_util::metrics::HistogramGuard;
use tycho_util::{FastDashMap, FastHashMap, FastHasherState};
use weedb::{rocksdb, BoundedCfHandle};

use crate::db::*;

pub struct CellStorage {
    db: BaseDb,
    cells_cache: Arc<CellsIndex>,
    raw_cells_cache: RawCellsCache,
}

type CellsIndex = FastDashMap<HashBytes, Weak<StorageCell>>;

impl CellStorage {
    pub fn new(db: BaseDb, cache_size_bytes: u64) -> Arc<Self> {
        let cells_cache = Default::default();
        let raw_cells_cache = RawCellsCache::new(cache_size_bytes);

        Arc::new(Self {
            db,
            cells_cache,
            raw_cells_cache,
        })
    }

    pub fn apply_temp_cell(&self, root: &HashBytes) -> Result<()> {
        const MAX_NEW_CELLS_BATCH_SIZE: usize = 10000;

        struct CellHashesIter<'a> {
            data: rocksdb::DBPinnableSlice<'a>,
            offset: usize,
            remaining_refs: u8,
        }

        impl<'a> Iterator for CellHashesIter<'a> {
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
            db: &'a BaseDb,
            buffer: Vec<u8>,
            transaction: FastHashMap<HashBytes, u32>,
            new_cells_batch: rocksdb::WriteBatch,
            new_cell_count: usize,
            raw_cache: &'a RawCellsCache,
        }

        impl<'a> Context<'a> {
            fn new(db: &'a BaseDb, raw_cache: &'a RawCellsCache) -> Self {
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
                        *entry.get_mut() += 1; // 1 new reference
                        InsertedCell::Existing
                    }
                    hash_map::Entry::Vacant(entry) => {
                        if let Some(value) = self.db.cells.get(key)? {
                            let (rc, value) = refcount::decode_value_with_rc(value.as_ref());
                            debug_assert!(rc > 0 && value.is_some() || rc == 0 && value.is_none());
                            if value.is_some() {
                                entry.insert(1); // 1 new reference
                                return Ok(InsertedCell::Existing);
                            }
                        }

                        entry.insert(0); // 0 new references (the first one is included in the merge below)
                        let iter = self.load_temp(key)?;

                        self.buffer.clear();
                        refcount::add_positive_refount(
                            1,
                            Some(iter.data.as_ref()),
                            &mut self.buffer,
                        );

                        self.raw_cache.add_refs(key, 1);

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

            fn flush_existing_cells(&mut self) -> Result<(), rocksdb::Error> {
                let mut batch = rocksdb::WriteBatch::default();

                for (key, &refs_diff) in &self.transaction {
                    if refs_diff == 0 {
                        continue;
                    }

                    self.buffer.clear();
                    refcount::add_positive_refount(refs_diff, None, &mut self.buffer);
                    self.raw_cache.add_refs(key, refs_diff);
                    batch.merge_cf(&self.cells_cf, key, self.buffer.as_slice());
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

    pub fn store_cell(
        &self,
        batch: &mut rocksdb::WriteBatch,
        root: Cell,
    ) -> Result<usize, CellStorageError> {
        struct CellWithRefs<'a> {
            rc: u32,
            data: Option<&'a [u8]>,
        }

        struct Context<'a> {
            db: &'a BaseDb,
            raw_cache: &'a RawCellsCache,
            alloc: &'a Bump,
            transaction: FastHashMap<HashBytes, CellWithRefs<'a>>,
            buffer: Vec<u8>,
        }

        impl Context<'_> {
            fn insert_cell(
                &mut self,
                key: &HashBytes,
                cell: &DynCell,
                depth: usize,
            ) -> Result<bool, CellStorageError> {
                Ok(match self.transaction.entry(*key) {
                    hash_map::Entry::Occupied(mut value) => {
                        value.get_mut().rc += 1;
                        false
                    }
                    hash_map::Entry::Vacant(entry) => {
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
                                if let Some(entry) = self.raw_cache.0.get(key) {
                                    let rc = entry.header.header.load(Ordering::Acquire);
                                    break 'value (rc, rc > 0);
                                }
                            }

                            match self.db.cells.get(key).map_err(CellStorageError::Internal)? {
                                Some(value) => {
                                    let (rc, value) =
                                        refcount::decode_value_with_rc(value.as_ref());
                                    (rc, value.is_some())
                                }
                                None => (0, false),
                            }
                        };

                        // TODO: lower to `debug_assert` when sure
                        assert!(has_value && old_rc > 0 || !has_value && old_rc == 0);

                        let data = if !has_value {
                            self.buffer.clear();
                            if StorageCell::serialize_to(cell, &mut self.buffer).is_err() {
                                return Err(CellStorageError::InvalidCell);
                            }
                            Some(self.alloc.alloc_slice_copy(self.buffer.as_slice()) as &[u8])
                        } else {
                            None
                        };
                        entry.insert(CellWithRefs { rc: 1, data });
                        !has_value
                    }
                })
            }

            fn finalize(mut self, batch: &mut rocksdb::WriteBatch) -> usize {
                let total = self.transaction.len();
                let cells_cf = &self.db.cells.cf();
                for (key, CellWithRefs { rc, data }) in self.transaction {
                    self.buffer.clear();
                    refcount::add_positive_refount(rc, data, &mut self.buffer);
                    if let Some(data) = data {
                        self.raw_cache.insert(&key, rc, data);
                    } else {
                        self.raw_cache.add_refs(&key, rc);
                    }
                    batch.merge_cf(cells_cf, key.as_slice(), &self.buffer);
                }
                total
            }
        }

        // Prepare context and handles
        let alloc = Bump::new();

        let mut ctx = Context {
            db: &self.db,
            raw_cache: &self.raw_cells_cache,
            alloc: &alloc,
            transaction: FastHashMap::with_capacity_and_hasher(128, Default::default()),
            buffer: Vec::with_capacity(512),
        };

        // Check root cell
        {
            let key = root.repr_hash();

            if !ctx.insert_cell(key, root.as_ref(), 0)? {
                return Ok(0);
            }
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
                let key = child.repr_hash();

                if ctx.insert_cell(key, child, depth)? {
                    stack.push(child.references());
                    continue 'outer;
                }
            }

            stack.pop();
        }

        // Clear big chunks of data before finalization
        drop(stack);

        // Write transaction to the `WriteBatch`
        Ok(ctx.finalize(batch))
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
            Ok(value) => 'cell: {
                if let Some(value) = value {
                    let rc = &value.header.header;
                    if rc.load(Ordering::Acquire) > 0 {
                        match StorageCell::deserialize(self.clone(), &value.slice) {
                            Some(cell) => break 'cell Arc::new(cell),
                            None => return Err(CellStorageError::InvalidCell),
                        }
                    }
                }
                return Err(CellStorageError::CellNotFound);
            }
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
        batch: &mut rocksdb::WriteBatch,
        alloc: &Bump,
        hash: &HashBytes,
    ) -> Result<usize, CellStorageError> {
        #[derive(Clone, Copy)]
        struct CellState<'a> {
            rc: i64,
            removes: u32,
            refs: &'a [HashBytes],
        }

        impl<'a> CellState<'a> {
            fn remove(&mut self) -> Result<Option<&'a [HashBytes]>, CellStorageError> {
                self.removes += 1;
                if self.removes as i64 <= self.rc {
                    Ok(self.next_refs())
                } else {
                    Err(CellStorageError::CounterMismatch)
                }
            }

            fn next_refs(&self) -> Option<&'a [HashBytes]> {
                if self.rc > self.removes as i64 {
                    None
                } else {
                    Some(self.refs)
                }
            }
        }

        let cells = &self.db.cells;
        let cells_cf = &cells.cf();

        let mut transaction: FastHashMap<&HashBytes, CellState<'_>> =
            FastHashMap::with_capacity_and_hasher(128, Default::default());
        let mut buffer = Vec::with_capacity(4);

        let mut stack = Vec::with_capacity(16);
        stack.push(hash);

        // While some cells left
        while let Some(cell_id) = stack.pop() {
            let refs = match transaction.entry(cell_id) {
                hash_map::Entry::Occupied(mut v) => v.get_mut().remove()?,
                hash_map::Entry::Vacant(v) => {
                    let rc =
                        self.raw_cells_cache
                            .get_raw_for_delete(&self.db, cell_id, &mut buffer)?;
                    debug_assert!(rc > 0);

                    v.insert(CellState {
                        rc,
                        removes: 1,
                        refs: alloc.alloc_slice_copy(buffer.as_slice()),
                    })
                    .next_refs()
                }
            };

            if let Some(refs) = refs {
                // Add all children
                for cell_id in refs {
                    // Unknown cell, push to the stack to process it
                    stack.push(cell_id);
                }
            }
        }

        // Clear big chunks of data before finalization
        drop(stack);

        // Write transaction to the `WriteBatch`
        let total = transaction.len();
        for (key, CellState { removes, .. }) in transaction {
            self.raw_cells_cache.remove_refs(key, removes);
            batch.merge_cf(
                cells_cf,
                key.as_slice(),
                refcount::encode_negative_refcount(removes),
            );
        }
        Ok(total)
    }

    pub fn drop_cell(&self, hash: &HashBytes) {
        if self.cells_cache.remove(hash).is_some() {
            metrics::gauge!("tycho_storage_cells_tree_cache_size").decrement(1f64);
        }
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

        impl<'a> Drop for Guard<'a> {
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

struct RawCellsCache(Cache<HashBytes, RawCellsCacheItem, CellSizeEstimator, FastHasherState>);

type RawCellsCacheItem = ThinArc<AtomicI64, u8>;

#[derive(Clone, Copy)]
pub struct CellSizeEstimator;
impl quick_cache::Weighter<HashBytes, RawCellsCacheItem> for CellSizeEstimator {
    fn weight(&self, key: &HashBytes, val: &RawCellsCacheItem) -> u32 {
        const STATIC_SIZE: usize = std::mem::size_of::<RawCellsCacheItem>()
            + std::mem::size_of::<i64>()
            + std::mem::size_of::<usize>() * 2; // ArcInner refs + HeaderWithLength length

        let len = key.0.len() + val.slice.len() + STATIC_SIZE;
        len as u32
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

        let estimated_cell_cache_capacity = size_in_bytes / (KEY_SIZE + MAX_CELL_SIZE);
        tracing::info!(
            estimated_cell_cache_capacity,
            max_cell_cache_size = %bytesize::ByteSize(size_in_bytes),
        );

        let raw_cache = Cache::with(
            estimated_cell_cache_capacity as usize,
            size_in_bytes,
            CellSizeEstimator,
            FastHasherState::default(),
            DefaultLifecycle::default(),
        );

        Self(raw_cache)
    }

    fn get_raw(
        &self,
        db: &BaseDb,
        key: &HashBytes,
        // pending: &PendingOperations,
    ) -> Result<Option<RawCellsCacheItem>, rocksdb::Error> {
        use quick_cache::sync::GuardResult;

        match self.0.get_value_or_guard(key, None) {
            GuardResult::Value(value) => Ok(Some(value)),
            GuardResult::Guard(g) => Ok(
                if let Some(value) = {
                    let _histogram =
                        HistogramGuard::begin("tycho_storage_get_cell_from_rocksdb_time");
                    db.cells.get(key.as_slice())?
                } {
                    let (rc, data) = refcount::decode_value_with_rc(value.as_ref());
                    data.map(|value| {
                        let value =
                            RawCellsCacheItem::from_header_and_slice(AtomicI64::new(rc), value);
                        // pending.run_if_none(|| {
                        // Insert value to the cache only if there are no pending operations
                        _ = g.insert(value.clone());
                        //});

                        value
                    })
                } else {
                    None
                },
            ),
            GuardResult::Timeout => unreachable!(),
        }
    }

    fn get_raw_for_delete(
        &self,
        db: &BaseDb,
        key: &HashBytes,
        refs_buffer: &mut Vec<HashBytes>,
    ) -> Result<i64, CellStorageError> {
        refs_buffer.clear();

        // NOTE: `peek` here is used to avoid affecting a "hotness" of the value
        if let Some(value) = self.0.peek(key) {
            let rc = value.header.header.load(Ordering::Acquire);
            if rc <= 0 {
                return Err(CellStorageError::CellNotFound);
            }

            StorageCell::deserialize_references(&value.slice, refs_buffer)
                .then_some(rc)
                .ok_or(CellStorageError::InvalidCell)
        } else {
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
    }

    fn insert(&self, key: &HashBytes, refs: u32, value: &[u8]) {
        let value = RawCellsCacheItem::from_header_and_slice(AtomicI64::new(refs as _), value);
        self.0.insert(*key, value);
    }

    fn add_refs(&self, key: &HashBytes, refs: u32) {
        // NOTE: `peek` here is used to avoid affecting a "hotness" of the value
        if let Some(v) = self.0.peek(key) {
            v.header.header.fetch_add(refs as i64, Ordering::Release);
        }
    }

    fn remove_refs(&self, key: &HashBytes, refs: u32) {
        // NOTE: `peek` here is used to avoid affecting a "hotness" of the value
        if let Some(v) = self.0.peek(key) {
            let old_refs = v.header.header.fetch_sub(refs as i64, Ordering::Release);
            debug_assert!(old_refs >= refs as i64);
        }
    }
}

// #[derive(Default)]
// struct PendingOperations {
//     // TODO: Replace with two atomic counters for inserts and pending operations
//     pending_count: Mutex<usize>,
// }

// impl PendingOperations {
//     fn begin(&self) -> PendingOperation<'_> {
//         *self.pending_count.lock().unwrap() += 1;
//         PendingOperation { operations: self }
//     }

//     #[inline]
//     fn run_if_none<F: FnOnce()>(&self, f: F) {
//         let guard = self.pending_count.lock().unwrap();
//         if *guard == 0 {
//             f();
//         }

//         // NOTE: Make sure to drop the lock only after the operation is executed
//         drop(guard);
//     }
// }

// pub struct PendingOperation<'a> {
//     operations: &'a PendingOperations,
// }

// impl Drop for PendingOperation<'_> {
//     fn drop(&mut self) {
//         *self.operations.pending_count.lock().unwrap() -= 1;
//     }
// }
