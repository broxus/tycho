use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{SyncSender, sync_channel};

use anyhow::Result;
use bytesize::ByteSize;
use crossbeam_queue::SegQueue;
use tycho_types::cell::{CellDescriptor, HashBytes};
use tycho_util::fs::MappedFile;
use weedb::rocksdb;

use super::{
    BuildTrustedCellHasher, CellDashMap, CellHashMap, CellStorage, CellStorageError, Counters,
    CountersStore, HashBytesKey, Idx, NextIdx, PersistedCellFilter, StorageCell, hash_key,
    owned_hash_key,
};
use crate::storage::shard_state::db_state::CellsDbStateKey;
use crate::storage::shard_state::row_format::encode_indexed_value;

impl CellStorage {
    pub fn apply_indexed_temp_cell(
        &self,
        root: &HashBytes,
        finalized_temp_cells: &FinalizedTempCells,
    ) -> Result<()> {
        const MAX_NEW_CELLS_BATCH_SIZE: usize = 100_000;
        const MAX_NEW_CELLS_BATCH_BYTES: usize = ByteSize::mib(16).0 as usize;
        const LOCAL_SPLIT_DEPTH: usize = 8;
        const MAX_LOCAL_STACK: usize = 4096;

        let limits = RawImportLimits {
            max_new_cells_batch_size: MAX_NEW_CELLS_BATCH_SIZE,
            max_new_cells_batch_bytes: MAX_NEW_CELLS_BATCH_BYTES,
            local_split_depth: LOCAL_SPLIT_DEPTH,
            max_local_stack: MAX_LOCAL_STACK,
        };

        let mut cell_counters = self.cell_counters.lock().unwrap();
        let mut persisted_filter = self.persisted_filter.lock().unwrap();
        let ctx = RawImportContext::new(
            self,
            finalized_temp_cells,
            limits,
            cell_counters.counters.next_idx,
        );
        ctx.queue.push(WorkItem {
            hash: *root,
            depth: 0,
        });

        let worker_count = self.worker_pool.current_num_threads().max(1);
        let (tx, rx) = sync_channel::<SealedBatch>(worker_count * 2);
        std::thread::scope(|thread_scope| {
            // Keep RocksDB writes outside the Rayon pool: with one storage
            // worker, a blocking sync_channel send would otherwise deadlock.
            let writer = thread_scope.spawn(move || {
                while let Ok(sealed) = rx.recv() {
                    self.cells_db.rocksdb().write(sealed.batch)?;
                }
                Ok::<_, CellStorageError>(())
            });

            let read = ReadContext {
                counters: &cell_counters.counters,
                persisted_filter: &persisted_filter,
            };
            self.worker_pool.scope(|scope| {
                for _ in 0..worker_count {
                    let tx = tx.clone();
                    let read = &read;
                    scope.spawn(|_| {
                        // Rayon scoped tasks cannot return a Result to this
                        // caller, so store the first error and ask peers to stop.
                        if let Err(e) = ctx.worker_loop(read, tx) {
                            ctx.set_error(e);
                        }
                    });
                }
                drop(tx);
            });

            match writer.join() {
                Ok(Ok(())) => {}
                Ok(Err(e)) => ctx.set_error(e),
                Err(_) => ctx.set_error(CellStorageError::RawImportWriterStopped),
            }
        });

        if let Some(e) = ctx.error.lock().unwrap().take() {
            return Err(e.into());
        }

        ctx.finalize(&mut cell_counters, &mut persisted_filter)?;

        Ok(())
    }
}

pub struct FinalizedTempCellsBuilder {
    file: BufWriter<File>,
    index: CellHashMap<FinalizedTempCell>,
    bytes: u64,
}

impl FinalizedTempCellsBuilder {
    pub fn new(file: File, cell_count: usize) -> Self {
        const BUFFER_CAPACITY: usize = ByteSize::mib(1).0 as usize;

        Self {
            file: BufWriter::with_capacity(BUFFER_CAPACITY, file),
            index: CellHashMap::with_capacity_and_hasher(
                cell_count,
                BuildTrustedCellHasher::default(),
            ),
            bytes: 0,
        }
    }

    pub fn append(&mut self, hash: &[u8; 32], data: &[u8]) -> Result<()> {
        let offset = self.bytes;
        self.file.write_all(data)?;
        self.bytes += data.len() as u64;
        self.index.insert(HashBytesKey(*hash), FinalizedTempCell {
            offset,
            len: data.len().try_into().unwrap(),
        });
        Ok(())
    }

    pub fn finish(mut self) -> Result<FinalizedTempCells> {
        self.file.flush()?;
        let file = MappedFile::from_existing_file(self.file.into_inner()?)?;
        Ok(FinalizedTempCells {
            file,
            index: self.index,
        })
    }
}

pub struct FinalizedTempCells {
    file: MappedFile,
    index: CellHashMap<FinalizedTempCell>,
}

impl FinalizedTempCells {
    fn get(&self, hash: &HashBytes) -> Option<&[u8]> {
        let cell = self.index.get(hash_key(hash))?;
        let offset = cell.offset as usize;
        let len = cell.len as usize;
        Some(&self.file.as_slice()[offset..offset + len])
    }

    fn len(&self) -> usize {
        self.index.len()
    }
}

struct FinalizedTempCell {
    offset: u64,
    len: u16,
}

struct RawImportContext<'a> {
    storage: &'a CellStorage,
    finalized_temp_cells: &'a FinalizedTempCells,
    limits: RawImportLimits,
    transaction: CellDashMap<TempCell>,
    next_idx: AtomicU64,
    queue: SegQueue<WorkItem>,
    in_flight: AtomicUsize,
    cancelled: AtomicBool,
    error: Mutex<Option<CellStorageError>>,
}

impl<'a> RawImportContext<'a> {
    fn new(
        storage: &'a CellStorage,
        finalized_temp_cells: &'a FinalizedTempCells,
        limits: RawImportLimits,
        next_idx: NextIdx,
    ) -> Self {
        Self {
            storage,
            finalized_temp_cells,
            limits,
            transaction: CellDashMap::with_capacity_and_hasher_and_shard_amount(
                finalized_temp_cells.len(),
                Default::default(),
                512,
            ),
            next_idx: AtomicU64::new(next_idx.get()),
            queue: SegQueue::new(),
            // Counts queued or currently processed cells. The root starts as
            // one unfinished item before it is pushed to the queue.
            in_flight: AtomicUsize::new(1),
            cancelled: AtomicBool::new(false),
            error: Mutex::new(None),
        }
    }

    fn worker_loop(
        &self,
        read: &ReadContext<'_>,
        tx: SyncSender<SealedBatch>,
    ) -> Result<(), CellStorageError> {
        let mut worker = Worker::new(self.limits);
        loop {
            if self.cancelled.load(Ordering::Acquire) {
                break;
            }

            let Some(item) = worker.local_stack.pop().or_else(|| self.queue.pop()) else {
                // Empty queues are not enough to stop: another worker may be
                // processing a parent that can still publish children.
                if self.in_flight.load(Ordering::Acquire) == 0 {
                    break;
                }
                std::thread::yield_now();
                continue;
            };

            let children = self.insert_cell(read, &mut worker, &item.hash, &tx)?;
            if let Some(children) = children {
                for child in children.into_iter() {
                    let child = WorkItem {
                        hash: child,
                        depth: item.depth + 1,
                    };
                    // Publish each child before retiring the parent below, so
                    // other workers never observe zero unfinished work early.
                    self.in_flight.fetch_add(1, Ordering::Release);
                    if child.depth <= self.limits.local_split_depth
                        || worker.local_stack.len() > self.limits.max_local_stack
                    {
                        self.queue.push(child);
                    } else {
                        worker.local_stack.push(child);
                    }
                }
            }
            self.in_flight.fetch_sub(1, Ordering::Release);
        }

        worker.flush(&tx)?;
        Ok(())
    }

    fn insert_cell(
        &self,
        read: &ReadContext<'_>,
        worker: &mut Worker,
        key: &HashBytes,
        tx: &SyncSender<SealedBatch>,
    ) -> Result<Option<ChildHashes>, CellStorageError> {
        use dashmap::mapref::entry::Entry;

        if let Some(value) = self.transaction.get(hash_key(key)) {
            value.additions.fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        }

        let existing = if read.persisted_filter.filter.contains(*key) {
            self.storage.get_raw_idx_for_insert(key, usize::MAX)?
        } else {
            None
        };

        let loaded = if existing.is_none() {
            let Some(data) = self.finalized_temp_cells.get(key) else {
                return Err(CellStorageError::CellNotFound);
            };
            Some((data, Self::read_references(data)?))
        } else {
            None
        };

        match self.transaction.entry(owned_hash_key(key)) {
            Entry::Occupied(value) => {
                value.get().additions.fetch_add(1, Ordering::Relaxed);
                Ok(None)
            }
            Entry::Vacant(entry) => {
                if let Some(idx) = existing {
                    entry.insert(TempCell {
                        idx,
                        old_rc: read.counters.get(idx),
                        additions: AtomicU32::new(1),
                        is_new: false,
                    });
                    Ok(None)
                } else {
                    let (data, children) = loaded.unwrap();
                    let idx = Idx::new(self.next_idx.fetch_add(1, Ordering::Relaxed));
                    entry.insert(TempCell {
                        idx,
                        old_rc: 0,
                        additions: AtomicU32::new(1),
                        is_new: true,
                    });
                    worker.stage_new(self.storage, key, idx, data, tx)?;
                    Ok(Some(children))
                }
            }
        }
    }

    fn read_references(data: &[u8]) -> Result<ChildHashes, CellStorageError> {
        if data.len() < 6 {
            return Err(CellStorageError::InvalidCell);
        }

        let descriptor = CellDescriptor::new([data[0], data[1]]);
        let byte_len = descriptor.byte_len() as usize;
        let hash_count = descriptor.hash_count() as usize - 1;
        let ref_count = descriptor.reference_count() as usize;

        let offset = 6usize + byte_len + StorageCell::HASHES_ITEM_LEN * hash_count;
        let end_offset = offset + ref_count * 32;
        if data.len() < end_offset {
            return Err(CellStorageError::InvalidCell);
        }

        let mut refs = ChildHashes::new();
        for chunk in data[offset..end_offset].chunks_exact(32) {
            refs.push(HashBytes::from_slice(chunk));
        }

        Ok(refs)
    }

    fn finalize(
        self,
        cell_counters: &mut CountersStore,
        persisted_filter: &mut PersistedCellFilter,
    ) -> Result<(), CellStorageError> {
        cell_counters.counters.next_idx = NextIdx::new(self.next_idx.load(Ordering::Relaxed));

        let mut batch = rocksdb::WriteBatch::default();
        let mut counter_batch = cell_counters.counters.begin();
        counter_batch.reserve(self.transaction.len());

        for item in &self.transaction {
            let additions = u64::from(item.additions.load(Ordering::Relaxed));
            counter_batch.update_raw(item.idx, item.old_rc, item.old_rc + additions);
        }
        counter_batch.apply();
        cell_counters.counters.shrink_if_needed();

        cell_counters
            .put_snapshot(&mut batch, CellsDbStateKey::CounterSnapshotLatest)
            .map_err(CellStorageError::State)?;

        self.storage.cells_db.rocksdb().write(batch)?;

        for item in self.transaction {
            if item.1.is_new {
                persisted_filter.insert(&HashBytes(item.0.0));
            }
        }
        persisted_filter.record_metrics();

        Ok(())
    }

    fn set_error(&self, e: CellStorageError) {
        self.cancelled.store(true, Ordering::Release);
        let mut slot = self.error.lock().unwrap();
        if slot.is_none() {
            *slot = Some(e);
        }
    }
}

struct Worker {
    limits: RawImportLimits,
    buffer: Vec<u8>,
    batch: rocksdb::WriteBatch,
    batch_cells: usize,
    batch_bytes: usize,
    local_stack: Vec<WorkItem>,
}

impl Worker {
    fn new(limits: RawImportLimits) -> Self {
        Self {
            limits,
            buffer: Vec::with_capacity(512),
            batch: rocksdb::WriteBatch::default(),
            batch_cells: 0,
            batch_bytes: 0,
            local_stack: Vec::with_capacity(1024),
        }
    }

    fn stage_new(
        &mut self,
        storage: &CellStorage,
        key: &HashBytes,
        idx: Idx,
        data: &[u8],
        tx: &SyncSender<SealedBatch>,
    ) -> Result<(), CellStorageError> {
        encode_indexed_value(idx, data, &mut self.buffer);
        let row_len = self.buffer.len();
        self.batch
            .put_cf(&storage.cells_db.cells.cf(), key, self.buffer.as_slice());
        self.batch_cells += 1;
        self.batch_bytes += row_len;

        if self.batch_cells >= self.limits.max_new_cells_batch_size
            || self.batch_bytes >= self.limits.max_new_cells_batch_bytes
        {
            self.flush(tx)?;
        }

        Ok(())
    }

    fn flush(&mut self, tx: &SyncSender<SealedBatch>) -> Result<(), CellStorageError> {
        if self.batch_cells == 0 {
            return Ok(());
        }

        let sealed = SealedBatch {
            batch: std::mem::take(&mut self.batch),
        };
        self.batch_cells = 0;
        self.batch_bytes = 0;
        tx.send(sealed)
            .map_err(|_err| CellStorageError::RawImportWriterStopped)
    }
}

#[derive(Clone, Copy)]
struct RawImportLimits {
    max_new_cells_batch_size: usize,
    max_new_cells_batch_bytes: usize,
    local_split_depth: usize,
    max_local_stack: usize,
}

struct ReadContext<'a> {
    counters: &'a Counters,
    persisted_filter: &'a PersistedCellFilter,
}

struct TempCell {
    old_rc: u64,
    idx: Idx,
    additions: AtomicU32,
    is_new: bool,
}

#[derive(Clone, Copy)]
struct WorkItem {
    hash: HashBytes,
    depth: usize,
}

struct SealedBatch {
    batch: rocksdb::WriteBatch,
}

struct ChildHashes {
    hashes: [HashBytes; 4],
    len: usize,
}

impl ChildHashes {
    fn new() -> Self {
        Self {
            hashes: [HashBytes([0; 32]); 4],
            len: 0,
        }
    }

    fn push(&mut self, hash: HashBytes) {
        self.hashes[self.len] = hash;
        self.len += 1;
    }

    fn into_iter(self) -> impl Iterator<Item = HashBytes> {
        self.hashes.into_iter().take(self.len)
    }
}
