use std::collections::hash_map;
use std::path::Path;
use std::sync::{Arc, Weak};

use anyhow::{Context, Result};
use bumpalo::Bump;
use everscale_types::cell::{CellDescriptor, DynCell, HashBytes};
use quick_cache::sync::{Cache, DefaultLifecycle};
use saferlmdb::{self as lmdb, LmdbResultExt};
use tycho_util::metrics::HistogramGuard;
use tycho_util::progress_bar::ProgressBar;
use tycho_util::sync::CancellationFlag;
use tycho_util::{FastDashMap, FastHashMap, FastHasherState};

pub use self::storage_cell::StorageCell;

mod storage_cell;

#[derive(Clone)]
pub struct CellDb {
    inner: Arc<Inner>,
}

struct Inner {
    env: Arc<lmdb::Environment>,
    cell_data: lmdb::Database<'static>,
    cell_refs: lmdb::Database<'static>,
    cell_cache: Arc<CellCache>,
    raw_cell_cache: RawCellCache,
}

impl CellDb {
    pub fn new<P: AsRef<Path>>(
        path: P,
        mapsize_limit: usize,
        cache_size_bytes: u64,
    ) -> Result<Self> {
        let Some(path) = path.as_ref().to_str() else {
            anyhow::bail!("invalid CellDB path: `{}`", path.as_ref().display());
        };

        let mut builder = lmdb::EnvBuilder::new()?;
        builder.set_mapsize(mapsize_limit)?;
        builder.set_maxdbs(DB_COUNT)?;

        let env = Arc::new(unsafe {
            builder.open(
                path,
                lmdb::open::Flags::WRITEMAP
                    | lmdb::open::Flags::NORDAHEAD
                    | lmdb::open::Flags::MAPASYNC,
                0o600,
            )?
        });

        let cell_data = lmdb::Database::open(
            env.clone(),
            Some(CELL_DATA_DB_NAME),
            &lmdb::DatabaseOptions::new(lmdb::db::Flags::CREATE),
        )
        .context("failed to open cell data DB")?;

        let cell_refs = lmdb::Database::open(
            env.clone(),
            Some(CELL_REFS_DB_NAME),
            &lmdb::DatabaseOptions::new(lmdb::db::Flags::CREATE),
        )
        .context("failed to open cell refs DB")?;

        let cells_cache = Arc::new(Default::default());
        let raw_cells_cache = RawCellCache::new(cache_size_bytes);

        Ok(Self {
            inner: Arc::new(Inner {
                env,
                cell_data,
                cell_refs,
                cell_cache: cells_cache,
                raw_cell_cache: raw_cells_cache,
            }),
        })
    }

    pub fn preload_cell_refs(&self, cancelled: CancellationFlag) -> Result<usize, CellDbError> {
        let this = self.inner.as_ref();

        let mut cancelled = cancelled.debounce(10000);

        let mut pg =
            ProgressBar::builder().build(|msg| tracing::info!("preloading cell refs... {msg}"));
        pg.set_total(u16::MAX as u64 + 1);

        let mut total_cells = 0;
        let txn = lmdb::ReadTransaction::new(this.env.as_ref())?;
        {
            let access = txn.access();
            let mut cursor = txn.cursor(&this.cell_refs)?;

            let mut kv = cursor
                .first::<StoredHashBytes, StoredCellRefs>(&access)
                .to_opt()?;

            while let Some((key, value)) = kv {
                if cancelled.check() {
                    return Err(CellDbError::Cancelled);
                }

                if total_cells % 10000 == 0 {
                    // Interpret highest two bytes as progress
                    pg.set_progress(u16::from_be_bytes([key[0], key[1]]));
                }

                self.inner
                    .raw_cell_cache
                    .refs_shard(key)
                    .insert(**key, value.get());
                total_cells += 1;

                kv = cursor
                    .next::<StoredHashBytes, StoredCellRefs>(&access)
                    .to_opt()?;
            }
        }

        pg.complete();
        Ok(total_cells)
    }

    pub fn store_big_cell(&self) -> Result<StoreBigCellTransaction<'_>, CellDbError> {
        let this = self.inner.as_ref();
        Ok(StoreBigCellTransaction {
            db: this,
            txn: lmdb::WriteTransaction::new(this.env.as_ref())?,
        })
    }

    pub fn load_big_cell(&self) -> Result<LoadBigCellTransaction<'_>, CellDbError> {
        let this = self.inner.as_ref();
        Ok(LoadBigCellTransaction {
            db: this,
            txn: lmdb::ReadTransaction::new(this.env.as_ref())?,
        })
    }

    pub fn store_cell(&self, root: &DynCell) -> Result<usize, CellDbError> {
        struct CellWithRefs<'a> {
            inserts: u32,
            data: Option<&'a [u8]>,
        }

        struct Context<'a> {
            raw_cache: &'a RawCellCache,
            alloc: &'a Bump,
            transaction: FastHashMap<HashBytes, CellWithRefs<'a>>,
            buffer: Vec<u8>,
        }

        impl Context<'_> {
            fn insert_cell(&mut self, cell: &DynCell) -> bool {
                let key = cell.repr_hash();

                match self.transaction.entry(*key) {
                    hash_map::Entry::Occupied(mut value) => {
                        value.get_mut().inserts += 1;
                        false
                    }
                    hash_map::Entry::Vacant(entry) => {
                        let has_value = self.raw_cache.refs_shard(key).contains_key(key);

                        let data = if !has_value {
                            self.buffer.clear();
                            StorageCell::serialize_to(cell, &mut self.buffer);
                            Some(self.alloc.alloc_slice_copy(self.buffer.as_slice()) as &[u8])
                        } else {
                            None
                        };
                        entry.insert(CellWithRefs { inserts: 1, data });

                        !has_value
                    }
                }
            }

            fn finalize<'tx>(
                self,
                mut access: lmdb::WriteAccessor<'tx>,
                cell_data: &'tx lmdb::Database<'static>,
                cell_refs: &'tx lmdb::Database<'static>,
            ) -> Result<usize, CellDbError> {
                let total = self.transaction.len();

                for (key, CellWithRefs { inserts, data }) in &self.transaction {
                    if let Some(data) = *data {
                        self.raw_cache.insert_value(key, data);
                        access.put(
                            cell_data,
                            StoredHashBytes::wrap(key),
                            StoredCellData::wrap(data),
                            lmdb::put::Flags::empty(),
                        )?;
                    }

                    let rc = self.raw_cache.add_refs(key, *inserts);
                    access.put(
                        cell_refs,
                        StoredHashBytes::wrap(key),
                        &StoredCellRefs::new(rc),
                        lmdb::put::Flags::empty(),
                    )?;
                }

                Ok(total)
            }
        }

        let this = self.inner.as_ref();

        // Prepare context and handles
        let alloc = Bump::new();

        let txn = lmdb::WriteTransaction::new(this.env.as_ref())?;

        let mut ctx = Context {
            raw_cache: &this.raw_cell_cache,
            alloc: &alloc,
            transaction: FastHashMap::with_capacity_and_hasher(128, Default::default()),
            buffer: Vec::with_capacity(512),
        };

        // Check root cell
        // TODO: Increase cell count even for the root cell
        if !ctx.insert_cell(root.as_ref()) {
            return Ok(0);
        }

        let mut stack = Vec::with_capacity(16);
        stack.push(root.references());

        // Check other cells
        'outer: loop {
            let Some(iter) = stack.last_mut() else {
                break;
            };

            for child in iter.by_ref() {
                if ctx.insert_cell(child) {
                    stack.push(child.references());
                    continue 'outer;
                }
            }

            stack.pop();
        }

        // Clear big chunks of data before finalization
        drop(stack);

        // Write transaction to the `WriteBatch`
        let new_cells = ctx.finalize(txn.access(), &this.cell_data, &this.cell_refs)?;

        txn.commit()?;
        Ok(new_cells)
    }

    pub fn remove_cell(&self, alloc: &Bump, hash: &HashBytes) -> Result<usize, CellDbError> {
        #[derive(Clone, Copy)]
        struct CellState<'a> {
            rc: u64,
            removes: u32,
            refs: &'a [HashBytes],
        }

        impl<'a> CellState<'a> {
            fn remove(&mut self) -> Result<Option<&'a [HashBytes]>, CellDbError> {
                self.removes += 1;
                if self.removes as u64 <= self.rc {
                    Ok(self.next_refs())
                } else {
                    Err(CellDbError::CounterMismatch)
                }
            }

            fn next_refs(&self) -> Option<&'a [HashBytes]> {
                if self.rc > self.removes as u64 {
                    None
                } else {
                    Some(self.refs)
                }
            }
        }

        let this = self.inner.as_ref();

        let mut transaction: FastHashMap<&HashBytes, CellState<'_>> =
            FastHashMap::with_capacity_and_hasher(128, Default::default());
        let mut buffer = Vec::with_capacity(4);

        let mut stack = Vec::with_capacity(16);
        stack.push(std::slice::from_ref(hash).iter());

        let txn = lmdb::WriteTransaction::new(this.env.as_ref())?;

        let mut accessor = txn.access();

        // While some cells left
        'outer: loop {
            let Some(iter) = stack.last_mut() else {
                break;
            };

            for cell_id in iter.by_ref() {
                // Process the current cell
                let refs = match transaction.entry(cell_id) {
                    hash_map::Entry::Occupied(mut v) => v.get_mut().remove()?,
                    hash_map::Entry::Vacant(v) => {
                        let rc = this.raw_cell_cache.get_raw_for_delete(
                            &accessor,
                            &this.cell_data,
                            cell_id,
                            &mut buffer,
                        )?;
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
                    // And proceed to its refs if any
                    stack.push(refs.iter());
                    continue 'outer;
                }
            }

            stack.pop();
        }

        // Clear big chunks of data before finalization
        drop(stack);

        // Write transaction to the `WriteBatch`
        let total = transaction.len();
        for (key, CellState { removes, .. }) in transaction {
            let rc = this.raw_cell_cache.remove_refs(key, removes);

            let key = StoredHashBytes::wrap(key);
            if rc == 0 {
                accessor.del_key(&this.cell_data, key)?;
                accessor.del_key(&this.cell_refs, key)?;
            } else {
                accessor.put(
                    &this.cell_refs,
                    key,
                    &StoredCellRefs::new(rc),
                    lmdb::put::Flags::empty(),
                )?;
            }
        }

        drop(accessor);

        txn.commit()?;
        Ok(total)
    }

    pub fn load_cell(&self, hash: &HashBytes) -> Result<Arc<StorageCell>, CellDbError> {
        let _histogram = HistogramGuard::begin("tycho_storage_load_cell_time");

        let this = self.inner.as_ref();

        if let Some(cell) = this.cell_cache.get(hash) {
            return Ok(cell);
        }

        let cell = {
            let txn = lmdb::ReadTransaction::new(this.env.as_ref())?;
            let accessor = txn.access();

            match this
                .raw_cell_cache
                .get_raw(&accessor, &this.cell_data, hash)
            {
                Ok(Some(value)) => match StorageCell::deserialize(self.clone(), &value) {
                    Some(cell) => Arc::new(cell),
                    None => return Err(CellDbError::InvalidCell),
                },
                Ok(None) => return Err(CellDbError::CellNotFound),
                Err(e) => return Err(e),
            }
        };

        if this.cell_cache.insert(hash, Arc::downgrade(&cell)) {
            metrics::gauge!("tycho_storage_cells_tree_cache_size").increment(1f64);
        }

        Ok(cell)
    }

    pub fn drop_cell(&self, hash: &HashBytes) {
        if self.inner.cell_cache.remove(hash) {
            metrics::gauge!("tycho_storage_cells_tree_cache_size").decrement(1f64);
        }
    }
}

// === Load big cell tx ===

pub struct LoadBigCellTransaction<'db> {
    db: &'db Inner,
    txn: lmdb::ReadTransaction<'db>,
}

impl<'db> LoadBigCellTransaction<'db> {
    pub fn accessor(&'_ self) -> LoadBigCellAccessor<'db, '_> {
        LoadBigCellAccessor {
            inner: self,
            accessor: self.txn.access(),
        }
    }
}

pub struct LoadBigCellAccessor<'db, 'tx> {
    inner: &'tx LoadBigCellTransaction<'db>,
    accessor: lmdb::ConstAccessor<'tx>,
}

impl<'db, 'tx> LoadBigCellAccessor<'db, 'tx> {
    pub fn get_cell(&self, hash: &HashBytes) -> Result<Option<&'_ [u8]>, CellDbError> {
        let Some(value) = self
            .accessor
            .get::<_, StoredCellData>(&self.inner.db.cell_data, StoredHashBytes::wrap(hash))
            .to_opt()?
        else {
            return Ok(None);
        };

        Ok(Some(&value.0))
    }
}

// === Store big cell tx ===

pub struct StoreBigCellTransaction<'db> {
    db: &'db Inner,
    txn: lmdb::WriteTransaction<'db>,
}

impl<'db> StoreBigCellTransaction<'db> {
    pub fn write_data<'tx>(&'tx mut self) -> Result<StoreBigCellData<'db, 'tx>> {
        Ok(StoreBigCellData {
            db: self.db,
            txn: self.txn.child_tx()?,
        })
    }

    pub fn apply(self, _: BigCellDataStored, root: &HashBytes) -> Result<(), CellDbError> {
        enum InsertedCell<'a> {
            New(CellHashesIter<'a>),
            Existing,
        }

        struct ApplyTempContext<'tx, 'a> {
            cell_data: &'tx lmdb::Database<'static>,
            cell_refs: &'tx lmdb::Database<'static>,
            access: &'a lmdb::ConstAccessor<'tx>,
            new_refs: FastHashMap<HashBytes, u32>,
            new_cell_count: usize,
        }

        impl<'tx, 'a> ApplyTempContext<'tx, 'a> {
            fn load_temp(&self, key: &HashBytes) -> Result<CellHashesIter<'a>, CellDbError> {
                let data = self
                    .access
                    .get::<_, StoredCellData>(self.cell_data, StoredHashBytes::wrap(key))?;

                data.parse_hashes_iter().ok_or_else(|| {
                    CellDbError::Internal(lmdb::Error::ValRejected("invalid cell data".to_owned()))
                })
            }

            fn insert_cell(&mut self, key: &HashBytes) -> Result<InsertedCell<'a>, CellDbError> {
                match self.new_refs.entry(*key) {
                    hash_map::Entry::Occupied(mut entry) => {
                        *entry.get_mut() += 1; // 1 new reference
                        Ok(InsertedCell::Existing)
                    }
                    hash_map::Entry::Vacant(entry) => {
                        entry.insert(1);

                        match self
                            .access
                            .get::<_, StoredCellRefs>(self.cell_refs, StoredHashBytes::wrap(key))
                            .to_opt()?
                        {
                            Some(_) => Ok(InsertedCell::Existing),
                            None => {
                                self.new_cell_count += 1;
                                self.load_temp(key).map(InsertedCell::New)
                            }
                        }
                    }
                }
            }

            fn finish(self) -> (usize, FastHashMap<HashBytes, u32>) {
                (self.new_cell_count, self.new_refs)
            }
        }

        {
            let mut access = self.txn.access();

            let mut ctx = ApplyTempContext {
                cell_data: &self.db.cell_data,
                cell_refs: &self.db.cell_refs,
                access: &access,
                new_refs: Default::default(),
                new_cell_count: 0,
            };

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

            drop(stack);

            let (_, new_refs) = ctx.finish();
            for (key, &inserts) in &new_refs {
                debug_assert_ne!(inserts, 0);

                let rc = self.db.raw_cell_cache.add_refs(key, inserts);
                access.put(
                    &self.db.cell_refs,
                    StoredHashBytes::wrap(key),
                    &StoredCellRefs::new(rc),
                    lmdb::put::Flags::empty(),
                )?;
            }
        }

        self.txn.commit().map_err(CellDbError::Internal)
    }
}

pub struct StoreBigCellData<'db, 'tx> {
    db: &'db Inner,
    txn: lmdb::WriteTransaction<'tx>,
}

impl<'db, 'tx> StoreBigCellData<'db, 'tx> {
    pub fn accessor(&'_ mut self) -> StoreBigCellDataAccessor<'db, '_> {
        StoreBigCellDataAccessor {
            inner: self,
            accessor: self.txn.access(),
        }
    }

    pub fn commit(self) -> Result<BigCellDataStored, CellDbError> {
        self.txn.commit()?;
        Ok(BigCellDataStored)
    }
}

pub struct BigCellDataStored;

pub struct StoreBigCellDataAccessor<'db, 'tx> {
    inner: &'tx StoreBigCellData<'db, 'tx>,
    accessor: lmdb::WriteAccessor<'tx>,
}

impl<'db, 'tx> StoreBigCellDataAccessor<'db, 'tx> {
    pub fn insert_cell(&mut self, hash: &HashBytes, value: &[u8]) -> Result<(), CellDbError> {
        self.accessor
            .put(
                &self.inner.db.cell_data,
                StoredHashBytes::wrap(hash),
                StoredCellData::wrap(value),
                lmdb::put::Flags::empty(),
            )
            .map_err(CellDbError::Internal)
    }
}

// === KV Types ===

#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub struct StoredHashBytes(HashBytes);

impl StoredHashBytes {
    #[inline(always)]
    pub const fn wrap(inner: &'_ HashBytes) -> &'_ Self {
        // SAFETY: `StoredHashBytes` has the same layout as `HashBytes`
        unsafe { &*(inner as *const HashBytes).cast::<Self>() }
    }
}

impl std::ops::Deref for StoredHashBytes {
    type Target = HashBytes;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl lmdb::traits::AsLmdbBytes for StoredHashBytes {
    fn as_lmdb_bytes(&self) -> &[u8] {
        self.0.as_slice()
    }
}

impl lmdb::traits::FromLmdbBytes for StoredHashBytes {
    fn from_lmdb_bytes(val: &[u8]) -> Result<&Self, String> {
        match val.try_into() {
            Ok::<&[u8; 32], _>(bytes) => Ok(Self::wrap(HashBytes::wrap(bytes))),
            Err(_) => Err("invalid key".to_owned()),
        }
    }
}

#[repr(transparent)]
pub struct StoredCellData([u8]);

impl StoredCellData {
    #[inline(always)]
    pub const fn wrap(inner: &'_ [u8]) -> &'_ Self {
        // SAFETY: `StoredCell` has the same layout as `[u8]`
        unsafe { &*(inner as *const [u8] as *const Self) }
    }

    pub const fn parse_hashes_iter(&self) -> Option<CellHashesIter<'_>> {
        let data = &self.0;

        let descriptor = CellDescriptor::new([data[0], data[1]]);
        let byte_len = descriptor.byte_len() as usize;
        let hash_count = descriptor.hash_count() as usize;
        let ref_count = descriptor.reference_count();

        let offset = 4 + byte_len + (32 + 2) * hash_count;
        if data.len() < offset + (ref_count as usize) * 32 {
            return None;
        }

        Some(CellHashesIter {
            data,
            offset,
            remaining_refs: ref_count,
        })
    }
}

impl std::ops::Deref for StoredCellData {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl lmdb::traits::AsLmdbBytes for StoredCellData {
    #[inline]
    fn as_lmdb_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl lmdb::traits::FromLmdbBytes for StoredCellData {
    #[inline]
    fn from_lmdb_bytes(val: &[u8]) -> Result<&Self, String> {
        Ok(StoredCellData::wrap(val))
    }
}

pub struct CellHashesIter<'a> {
    data: &'a [u8],
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

#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub struct StoredCellRefs([u8; 8]);

impl StoredCellRefs {
    pub const fn new(value: u64) -> Self {
        Self(value.to_le_bytes())
    }

    pub const fn get(&self) -> u64 {
        u64::from_le_bytes(self.0)
    }
}

impl StoredCellRefs {
    #[inline(always)]
    const fn wrap(inner: &'_ [u8; 8]) -> &'_ Self {
        unsafe { &*(inner as *const [u8; 8]).cast::<Self>() }
    }
}

impl lmdb::traits::AsLmdbBytes for StoredCellRefs {
    fn as_lmdb_bytes(&self) -> &[u8] {
        self.0.as_slice()
    }
}

impl lmdb::traits::FromLmdbBytes for StoredCellRefs {
    fn from_lmdb_bytes(val: &[u8]) -> Result<&Self, String> {
        match val.try_into() {
            Ok::<&[u8; 8], _>(bytes) => Ok(Self::wrap(bytes)),
            Err(_) => Err("invalid cell refs".to_owned()),
        }
    }
}

// === Database names ===

// cell_data + cell_refs
const DB_COUNT: u32 = 2;
const CELL_DATA_DB_NAME: &str = "cell_data";
const CELL_REFS_DB_NAME: &str = "cell_refs";

// === Cells Cache ===

const CELL_SHARDS: usize = 256;

struct CellCache {
    shards: [CellCacheShard; CELL_SHARDS],
}

impl Default for CellCache {
    fn default() -> Self {
        Self {
            shards: [(); CELL_SHARDS].map(|_| Default::default()),
        }
    }
}

impl CellCache {
    #[inline(always)]
    fn shard(&self, key: &HashBytes) -> &CellCacheShard {
        &self.shards[key[0] as usize]
    }

    fn get(&self, key: &HashBytes) -> Option<Arc<StorageCell>> {
        self.shard(key).get(key).and_then(|v| v.upgrade())
    }

    /// Returns `true` if the value was inserted, `false` if the value was already present.
    fn insert(&self, key: &HashBytes, value: Weak<StorageCell>) -> bool {
        self.shard(key).insert(*key, value).is_none()
    }

    /// Returns `true` if the value was removed, `false` if the value was not present.
    fn remove(&self, key: &HashBytes) -> bool {
        self.shard(key).remove(key).is_some()
    }
}

type CellCacheShard = FastDashMap<HashBytes, Weak<StorageCell>>;

struct RawCellCache {
    data_shards: [CellDataCacheShard; CELL_SHARDS],
    refs_shards: [CellRefsCacheShard; CELL_SHARDS],
}

type CellDataCacheShard = Cache<HashBytes, CellDataCacheItem, CellSizeEstimator, FastHasherState>;
type CellDataCacheItem = Arc<[u8]>;

type CellRefsCacheShard = FastDashMap<HashBytes, u64>;

#[derive(Clone, Copy)]
pub struct CellSizeEstimator;
impl quick_cache::Weighter<HashBytes, CellDataCacheItem> for CellSizeEstimator {
    fn weight(&self, _: &HashBytes, val: &CellDataCacheItem) -> u64 {
        const STATIC_SIZE: usize = std::mem::size_of::<HashBytes>()
            + std::mem::size_of::<CellDataCacheItem>()
            + std::mem::size_of::<usize>() * 2; // strong + weak refs

        let len = val.len() + STATIC_SIZE;
        len as u64
    }
}

impl RawCellCache {
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

        const MIN_CACHE_SIZE: u64 = 1 << 20; // 1 MB

        let shard_size_in_bytes = std::cmp::max(size_in_bytes / CELL_SHARDS as u64, MIN_CACHE_SIZE);

        let estimated_cell_cache_capacity = shard_size_in_bytes / (KEY_SIZE + MAX_CELL_SIZE);
        tracing::info!(
            estimated_cell_cache_capacity,
            max_cell_cache_size = %bytesize::ByteSize(shard_size_in_bytes * CELL_SHARDS as u64),
            cell_shard_size = %bytesize::ByteSize(shard_size_in_bytes),
        );

        let data_shards = [(); CELL_SHARDS].map(|_| {
            Cache::with(
                estimated_cell_cache_capacity as usize,
                shard_size_in_bytes,
                CellSizeEstimator,
                FastHasherState::default(),
                DefaultLifecycle::default(),
            )
        });

        let refs_shards = [(); CELL_SHARDS].map(|_| Default::default());

        Self {
            data_shards,
            refs_shards,
        }
    }

    #[inline(always)]
    fn shard(&self, key: &HashBytes) -> (&CellDataCacheShard, &CellRefsCacheShard) {
        (
            &self.data_shards[key[0] as usize],
            &self.refs_shards[key[0] as usize],
        )
    }

    #[inline(always)]
    fn data_shard(&self, key: &HashBytes) -> &CellDataCacheShard {
        &self.data_shards[key[0] as usize]
    }

    #[inline(always)]
    fn refs_shard(&self, key: &HashBytes) -> &CellRefsCacheShard {
        &self.refs_shards[key[0] as usize]
    }

    fn get_raw<'a>(
        &self,
        accessor: &'a lmdb::ConstAccessor<'_>,
        cell_data: &'a lmdb::Database<'static>,
        key: &HashBytes,
    ) -> Result<Option<CellDataCacheItem>, CellDbError> {
        use quick_cache::sync::GuardResult;

        match self.data_shard(key).get_value_or_guard(key, None) {
            GuardResult::Value(value) => Ok(Some(value)),
            GuardResult::Guard(g) => Ok(
                if let Some(value) = {
                    let _histogram =
                        HistogramGuard::begin("tycho_storage_get_cell_from_rocksdb_time");
                    accessor
                        .get::<_, StoredCellData>(cell_data, StoredHashBytes::wrap(key))
                        .to_opt()?
                } {
                    let value = CellDataCacheItem::from(&value.0);
                    _ = g.insert(value.clone());
                    Some(value)
                } else {
                    None
                },
            ),
            GuardResult::Timeout => unreachable!(),
        }
    }

    fn get_raw_for_delete<'a>(
        &self,
        accessor: &'a lmdb::ConstAccessor<'_>,
        cell_data: &'a lmdb::Database<'static>,
        key: &HashBytes,
        refs_buffer: &mut Vec<HashBytes>,
    ) -> Result<u64, CellDbError> {
        refs_buffer.clear();

        let (data_shard, refs_shard) = self.shard(key);

        let Some(rc) = refs_shard.get(key).map(|v| *v) else {
            return Err(CellDbError::CellNotFound);
        };

        // NOTE: `peek` here is used to avoid affecting a "hotness" of the value
        if let Some(value) = data_shard.peek(key) {
            StorageCell::deserialize_references(&value, refs_buffer)
                .then_some(rc)
                .ok_or(CellDbError::InvalidCell)
        } else {
            match accessor
                .get::<_, StoredCellData>(cell_data, StoredHashBytes::wrap(key))
                .to_opt()
            {
                Ok(Some(value)) => StorageCell::deserialize_references(value, refs_buffer)
                    .then_some(rc)
                    .ok_or(CellDbError::InvalidCell),
                Ok(None) => Err(CellDbError::CellNotFound),
                Err(e) => Err(CellDbError::Internal(e)),
            }
        }
    }

    fn insert_value(&self, key: &HashBytes, value: &[u8]) {
        self.data_shard(key)
            .insert(*key, CellDataCacheItem::from(value));
    }

    /// Adds the specified amount of refs and returns the new value
    fn add_refs(&self, key: &HashBytes, inserts: u32) -> u64 {
        use dashmap::mapref::entry::Entry;

        match self.refs_shard(key).entry(*key) {
            Entry::Vacant(entry) => {
                entry.insert(inserts as _);
                inserts as _
            }
            Entry::Occupied(mut entry) => {
                *entry.get_mut() += inserts as u64;
                *entry.get()
            }
        }
    }

    /// Removes the specified amount of refs and returns the new value
    fn remove_refs(&self, key: &HashBytes, removes: u32) -> u64 {
        let (data_shard, refs_shard) = self.shard(key);

        let mut new_refs = 0;
        let remove_data = refs_shard
            .remove_if_mut(key, |_, current_refs| {
                *current_refs = current_refs.saturating_sub(removes as _); // TODO: Panic on overflow?
                new_refs = *current_refs;
                *current_refs == 0
            })
            .is_some();

        if remove_data {
            data_shard.remove(key);
        }

        new_refs
    }
}

// === Errors ===

#[derive(Debug, thiserror::Error)]
pub enum CellDbError {
    #[error(transparent)]
    Internal(#[from] lmdb::Error),
    #[error("operation cancelled")]
    Cancelled,
    #[error("invalid cell")]
    InvalidCell,
    #[error("cell not found")]
    CellNotFound,
    #[error("counter mismatch")]
    CounterMismatch,
}
