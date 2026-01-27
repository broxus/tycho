use std::cell::UnsafeCell;
use std::cell::Cell as CounterSlot;
use std::collections::hash_map;
use std::mem::{ManuallyDrop, MaybeUninit};
use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::thread::Scope;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use bumpalo::Bump;
use bumpalo_herd::{Herd, Member};
use bytesize::ByteSize;
use crossbeam_channel::{RecvTimeoutError, Receiver, Sender};
use crossbeam_queue::SegQueue;
use dashmap::Map;
use faster_rs::{
    FasterError, FasterKv, FasterKvConfig, HlogCompactionConfig, ReadCacheConfig,
    status as faster_status,
};
use triomphe::ThinArc;
use tycho_storage::fs::Dir;
use tycho_types::cell::*;
use tycho_util::metrics::{HistogramGuard, spawn_metrics_loop};
use tycho_util::{FastDashMap, FastHashMap, FastHashSet};
use weedb::rocksdb;

use crate::storage::CellsDb;

const FASTER_REFRESH_EVERY: Duration = Duration::from_millis(10);
const FASTER_MAINTENANCE_INTERVAL: usize = 1024;

thread_local! {
    static NEXT_WORKER: CounterSlot<usize> = CounterSlot::new(0);
}

fn select_worker(len: usize) -> usize {
    NEXT_WORKER.with(|next| {
        let idx = next.get();
        next.set(idx.wrapping_add(1));
        idx % len
    })
}

struct WorkerState {
    last_refresh: Instant,
    ops_count: usize,
}

fn refresh_if_due(store: &FasterKv, state: &mut WorkerState) {
    if state.ops_count < FASTER_MAINTENANCE_INTERVAL
        && state.last_refresh.elapsed() < FASTER_REFRESH_EVERY
    {
        return;
    }

    // Keep the thread-affine session alive under load and idle periods.
    store.refresh();
    store.complete_pending(false);
    state.last_refresh = Instant::now();
    state.ops_count = 0;
}

struct FasterMetricsSnapshot {
    size: u64,
    num_active_sessions: u32,
    auto_compaction_scheduled: bool,
    hlog_max_size_reached: bool,
    hlog_begin_address: u64,
    hlog_tail_address: u64,
    hlog_head_address: u64,
    hlog_safe_head_address: u64,
    hlog_read_only_address: u64,
    hlog_safe_read_only_address: u64,
    hlog_flushed_until_address: u64,
}

enum Command {
    Read {
        key: [u8; 32],
        reply: Sender<Result<Option<(u16, [u8; PERSISTED_VALUE_MAX_BYTES])>, CellStorageError>>,
    },
    Upsert {
        key: [u8; 32],
        value: Vec<u8>,
        reply: Sender<Result<(), CellStorageError>>,
    },
    Delete {
        key: [u8; 32],
        reply: Sender<Result<bool, CellStorageError>>,
    },
    Metrics {
        reply: Sender<Result<FasterMetricsSnapshot, CellStorageError>>,
    },
    Shutdown,
}

struct Faster {
    workers: Vec<Sender<Command>>,
    joins: Mutex<Vec<std::thread::JoinHandle<()>>>,
}

impl Faster {
    fn new(store: FasterKv, workers: usize) -> Result<Self, CellStorageError> {
        let store = Arc::new(store);
        let serial = Arc::new(AtomicU64::new(1));
        let workers = workers.max(1);

        let mut senders = Vec::with_capacity(workers);
        let mut joins = Vec::with_capacity(workers);

        for idx in 0..workers {
            let (tx, rx) = crossbeam_channel::unbounded();
            senders.push(tx);

            let store = Arc::clone(&store);
            let serial = Arc::clone(&serial);
            let join = std::thread::Builder::new()
                .name(format!("faster-worker-{idx}"))
                .spawn(move || run_worker(store, serial, rx))
                .map_err(CellStorageError::WorkerThreadSpawnFailed)?;
            joins.push(join);
        }

        Ok(Self {
            workers: senders,
            joins: Mutex::new(joins),
        })
    }

    fn send(&self, command: Command) -> Result<(), CellStorageError> {
        let idx = select_worker(self.workers.len());
        self.workers[idx]
            .send(command)
            .map_err(|_| CellStorageError::WorkerChannelClosed)
    }

    fn read(
        &self,
        key: [u8; 32],
    ) -> Result<Option<(u16, [u8; PERSISTED_VALUE_MAX_BYTES])>, CellStorageError> {
        let (tx, rx) = crossbeam_channel::bounded(1);
        self.send(Command::Read { key, reply: tx })?;
        let result = rx
            .recv()
            .map_err(|_| CellStorageError::WorkerChannelClosed)?;
        result
    }

    fn upsert(&self, key: [u8; 32], value: Vec<u8>) -> Result<(), CellStorageError> {
        let (tx, rx) = crossbeam_channel::bounded(1);
        self.send(Command::Upsert {
            key,
            value,
            reply: tx,
        })?;
        let result = rx
            .recv()
            .map_err(|_| CellStorageError::WorkerChannelClosed)?;
        result
    }

    fn delete(&self, key: [u8; 32]) -> Result<bool, CellStorageError> {
        let (tx, rx) = crossbeam_channel::bounded(1);
        self.send(Command::Delete { key, reply: tx })?;
        let result = rx
            .recv()
            .map_err(|_| CellStorageError::WorkerChannelClosed)?;
        result
    }

    fn metrics_snapshot(&self) -> Result<FasterMetricsSnapshot, CellStorageError> {
        let (tx, rx) = crossbeam_channel::bounded(1);
        self.send(Command::Metrics { reply: tx })?;
        let result = rx
            .recv()
            .map_err(|_| CellStorageError::WorkerChannelClosed)?;
        result
    }
}

impl Drop for Faster {
    fn drop(&mut self) {
        for tx in &self.workers {
            let _ = tx.send(Command::Shutdown);
        }
        let Ok(mut joins) = self.joins.lock() else {
            return;
        };
        for join in joins.drain(..) {
            let _ = join.join();
        }
    }
}

fn run_worker(store: Arc<FasterKv>, serial: Arc<AtomicU64>, rx: Receiver<Command>) {
    let _ = store.start_session();
    let mut state = WorkerState {
        last_refresh: Instant::now(),
        ops_count: 0,
    };

    loop {
        let command = match rx.recv_timeout(FASTER_REFRESH_EVERY) {
            Ok(command) => command,
            Err(RecvTimeoutError::Timeout) => {
                refresh_if_due(&store, &mut state);
                continue;
            }
            Err(RecvTimeoutError::Disconnected) => break,
        };

        match command {
            Command::Read { key, reply } => {
                let result = read_cells_value_inner(&store, &serial, key);
                let _ = reply.send(result);
            }
            Command::Upsert { key, value, reply } => {
                let result = insert_cells_value_inner(&store, &serial, key, value);
                let _ = reply.send(result);
            }
            Command::Delete { key, reply } => {
                let result = delete_cells_value_inner(&store, &serial, key);
                let _ = reply.send(result);
            }
            Command::Metrics { reply } => {
                refresh_if_due(&store, &mut state);
                let result = Ok(FasterMetricsSnapshot {
                    size: store.size(),
                    num_active_sessions: store.num_active_sessions(),
                    auto_compaction_scheduled: store.auto_compaction_scheduled(),
                    hlog_max_size_reached: store.hlog_max_size_reached(),
                    hlog_begin_address: store.hlog_begin_address(),
                    hlog_tail_address: store.hlog_tail_address(),
                    hlog_head_address: store.hlog_head_address(),
                    hlog_safe_head_address: store.hlog_safe_head_address(),
                    hlog_read_only_address: store.hlog_read_only_address(),
                    hlog_safe_read_only_address: store.hlog_safe_read_only_address(),
                    hlog_flushed_until_address: store.hlog_flushed_until_address(),
                });
                let _ = reply.send(result);
            }
            Command::Shutdown => break,
        }

        state.ops_count += 1;
        refresh_if_due(&store, &mut state);
    }

    store.stop_session();
}

pub struct CellStorage {
    cells_db: CellsDb,
    cells_store: Faster,
    cells_cache: Arc<CellsIndex>,
    drop_interval: u32,
    counters: Vec<AtomicU64>,
    insert_cells_ns: AtomicU64,
    live_cells: AtomicU64,
    free_idx: SegQueue<u32>,
    next_idx: AtomicU32,
}

type CellsIndex = FastDashMap<HashBytes, CachedCell>;

const CELL_INDEX_BYTES: usize = 4;
const MAX_CELLS: u32 = u32::MAX; // god bless us

const FASTER_DIR_NAME: &str = "cells.faster";
const FASTER_TABLE_SIZE: u64 = 1 << 26;
const LOG_PAGE: u64 = 32 * 1024 * 1024;
const FASTER_LOG_SIZE_BYTES: u64 = LOG_PAGE * 512;
const FASTER_LOG_MUTABLE_FRACTION: f64 = 0.9;
const FASTER_WORKERS: usize = 4;
const PERSISTED_VALUE_MAX_BYTES: usize = 512;

struct CachedCell {
    epoch: u32,
    weak: Weak<StorageCell>,
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

fn read_cells_value_inner(
    cells_store: &FasterKv,
    serial: &AtomicU64,
    key: [u8; 32],
) -> Result<Option<(u16, [u8; PERSISTED_VALUE_MAX_BYTES])>, CellStorageError> {
    let monotonic_serial_number = serial.fetch_add(1, Ordering::Relaxed);
    let (status, receiver) = cells_store.read::<[u8; 32]>(&key, monotonic_serial_number);
    let value = match status {
        faster_status::OK => receiver
            .recv()
            .map_err(|_| CellStorageError::FasterReadEmpty)?,
        faster_status::NOT_FOUND => return Ok(None),
        faster_status::PENDING => {
            cells_store.complete_pending(true);
            match receiver.recv() {
                Ok(value) => value,
                Err(_) => return Ok(None),
            }
        }
        faster_status::OUT_OF_MEMORY => {
            if !cells_store.grow_index() {
                std::thread::yield_now();
            }
            let monotonic_serial_number = serial.fetch_add(1, Ordering::Relaxed);
            let (status, receiver) = cells_store.read::<[u8; 32]>(&key, monotonic_serial_number);
            match status {
                faster_status::OK => receiver
                    .recv()
                    .map_err(|_| CellStorageError::FasterReadEmpty)?,
                faster_status::NOT_FOUND => return Ok(None),
                faster_status::PENDING => {
                    cells_store.complete_pending(true);
                    match receiver.recv() {
                        Ok(value) => value,
                        Err(_) => return Ok(None),
                    }
                }
                faster_status::OUT_OF_MEMORY => return Err(CellStorageError::FasterGrowIndexFailed),
                _ => return Err(CellStorageError::FasterStatus(status)),
            }
        }
        _ => return Err(CellStorageError::FasterStatus(status)),
    };

    if value.len() > PERSISTED_VALUE_MAX_BYTES {
        return Err(CellStorageError::InvalidCell);
    }

    let len = u16::try_from(value.len()).map_err(|_| CellStorageError::InvalidCell)?;
    let mut buffer = [0u8; PERSISTED_VALUE_MAX_BYTES];
    buffer[..value.len()].copy_from_slice(&value);
    Ok(Some((len, buffer)))
}

fn insert_cells_value_inner(
    cells_store: &FasterKv,
    serial: &AtomicU64,
    key: [u8; 32],
    value: Vec<u8>,
) -> Result<(), CellStorageError> {
    let monotonic_serial_number = serial.fetch_add(1, Ordering::Relaxed);
    let status = cells_store.upsert::<[u8; 32], Vec<u8>>(&key, &value, monotonic_serial_number);
    match status {
        faster_status::OK => {
            Ok(())
        }
        faster_status::PENDING => {
            cells_store.complete_pending(true);
            Ok(())
        }
        faster_status::OUT_OF_MEMORY => {
            if !cells_store.grow_index() {
                std::thread::yield_now();
            }

            let monotonic_serial_number = serial.fetch_add(1, Ordering::Relaxed);
            let status =
                cells_store.upsert::<[u8; 32], Vec<u8>>(&key, &value, monotonic_serial_number);
            match status {
                faster_status::OK => {
                    Ok(())
                }
                faster_status::PENDING => {
                    cells_store.complete_pending(true);
                    Ok(())
                }
                faster_status::OUT_OF_MEMORY => Err(CellStorageError::FasterGrowIndexFailed),
                _ => Err(CellStorageError::FasterStatus(status)),
            }
        }
        _ => Err(CellStorageError::FasterStatus(status)),
    }
}

fn delete_cells_value_inner(
    cells_store: &FasterKv,
    serial: &AtomicU64,
    key: [u8; 32],
) -> Result<bool, CellStorageError> {
    let monotonic_serial_number = serial.fetch_add(1, Ordering::Relaxed);
    let status = cells_store.delete(&key, monotonic_serial_number);
    match status {
        faster_status::OK => Ok(true),
        faster_status::NOT_FOUND => Ok(false),
        faster_status::PENDING => {
            cells_store.complete_pending(true);
            Ok(true)
        }
        faster_status::OUT_OF_MEMORY => {
            if !cells_store.grow_index() {
                std::thread::yield_now();
            }
            let monotonic_serial_number = serial.fetch_add(1, Ordering::Relaxed);
            let status = cells_store.delete(&key, monotonic_serial_number);
            match status {
                faster_status::OK => Ok(true),
                faster_status::NOT_FOUND => Ok(false),
                faster_status::PENDING => {
                    cells_store.complete_pending(true);
                    Ok(true)
                }
                faster_status::OUT_OF_MEMORY => Err(CellStorageError::FasterGrowIndexFailed),
                _ => Err(CellStorageError::FasterStatus(status)),
            }
        }
        _ => Err(CellStorageError::FasterStatus(status)),
    }
}

fn insert_cells_value(
    cells_store: &Faster,
    key: &HashBytes,
    value: &[u8],
) -> Result<(), CellStorageError> {
    cells_store.upsert(key.0, value.to_vec())
}

fn delete_cells_value(cells_store: &Faster, key: &HashBytes) -> Result<(), CellStorageError> {
    _ = cells_store.delete(key.0)?;
    Ok(())
}

impl CellStorage {
    pub fn new(
        cells_db: CellsDb,
        files_dir: &Dir,
        _cache_size_bytes: ByteSize,
        drop_interval: u32,
    ) -> Result<Arc<Self>> {
        let cells_cache = Default::default();
        let mut counters = Vec::with_capacity(MAX_CELLS as usize);
        counters.resize_with(MAX_CELLS as usize, || AtomicU64::new(0));
        let cells_dir = files_dir.create_subdir(FASTER_DIR_NAME)?;
        let Some(path) = cells_dir.path().to_str() else {
            return Err(anyhow::anyhow!(CellStorageError::FasterPathNotUtf8));
        };

        const GIB: u64 = 1024 * 1024 * 1024;

        let builder = FasterKvConfig::builder()
            .log_size(FASTER_LOG_SIZE_BYTES)
            .table_size(FASTER_TABLE_SIZE)
            .hlog_compaction(
                HlogCompactionConfig::builder()
                    .num_threads(2)
                    .trigger_pct(0.9)
                    .max_compacted_size(4 * GIB)
                    .check_interval(Duration::from_millis(250))
                    .compact_pct(0.1)
                    .hlog_size_budget(FASTER_LOG_SIZE_BYTES * 4)
                    .build(),
            )
            .pre_allocate_log(true)
            .storage_path(path.to_string())
            .log_mutable_fraction(FASTER_LOG_MUTABLE_FRACTION)
            .read_cache(
                ReadCacheConfig::builder()
                    .pre_allocate(true)
                    .mem_size(4 * GIB)
                    .mutable_fraction(0.1)
                    .build(),
            )
            .build()
            .map_err(CellStorageError::FasterInit)?;

        let cells_store = Faster::new(builder, FASTER_WORKERS)?;
        let cell_storage = Arc::new(Self {
            cells_db,
            cells_store,
            cells_cache,
            drop_interval,
            counters,
            insert_cells_ns: AtomicU64::new(0),
            live_cells: AtomicU64::new(0),
            free_idx: SegQueue::new(),
            next_idx: AtomicU32::new(0),
        });

        spawn_metrics_loop(
            &cell_storage,
            Duration::from_secs(5),
            |storage| async move { storage.refresh_metrics() },
        );

        Ok(cell_storage)
    }

    pub fn db(&self) -> &CellsDb {
        &self.cells_db
    }

    pub fn take_insert_cells_ns(&self) -> u64 {
        self.insert_cells_ns.swap(0, Ordering::AcqRel)
    }

    /// Reads encoded cell value (index + payload) for persistent state writes.
    pub(crate) fn read_cell_value(
        &self,
        key: &HashBytes,
    ) -> Result<Option<Vec<u8>>, CellStorageError> {
        let mut buffer = vec![0u8; PERSISTED_VALUE_MAX_BYTES];
        let Some(value) = self.read_cells_value(key, &mut buffer)? else {
            return Ok(None);
        };
        Ok(Some(value.to_vec()))
    }

    pub fn count_cells(&self) -> Result<usize, CellStorageError> {
        Ok(self.live_cells.load(Ordering::Acquire) as usize)
    }

    fn refresh_metrics(&self) {
        let Ok(snapshot) = self.cells_store.metrics_snapshot() else {
            return;
        };
        metrics::gauge!("tycho_storage_cells_next_idx")
            .set(self.next_idx.load(Ordering::Acquire) as f64);
        metrics::gauge!("tycho_storage_cells_free_idx_len").set(self.free_idx.len() as f64);
        metrics::gauge!("tycho_storage_faster_size_bytes")
            .set(snapshot.size as f64);
        metrics::gauge!("tycho_storage_faster_num_active_sessions")
            .set(snapshot.num_active_sessions as f64);
        metrics::gauge!("tycho_storage_faster_auto_compaction_scheduled")
            .set(if snapshot.auto_compaction_scheduled { 1.0 } else { 0.0 });
        metrics::gauge!("tycho_storage_faster_hlog_max_size_reached")
            .set(if snapshot.hlog_max_size_reached { 1.0 } else { 0.0 });
        metrics::gauge!("tycho_storage_faster_hlog_begin_address")
            .set(snapshot.hlog_begin_address as f64);
        metrics::gauge!("tycho_storage_faster_hlog_tail_address")
            .set(snapshot.hlog_tail_address as f64);
        metrics::gauge!("tycho_storage_faster_hlog_head_address")
            .set(snapshot.hlog_head_address as f64);
        metrics::gauge!("tycho_storage_faster_hlog_safe_head_address")
            .set(snapshot.hlog_safe_head_address as f64);
        metrics::gauge!("tycho_storage_faster_hlog_read_only_address")
            .set(snapshot.hlog_read_only_address as f64);
        metrics::gauge!("tycho_storage_faster_hlog_safe_read_only_address")
            .set(snapshot.hlog_safe_read_only_address as f64);
        metrics::gauge!("tycho_storage_faster_hlog_flushed_until_address")
            .set(snapshot.hlog_flushed_until_address as f64);
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

    fn read_cells_value<'a>(
        &self,
        key: &HashBytes,
        buffer: &'a mut [u8],
    ) -> Result<Option<&'a [u8]>, CellStorageError> {
        #[cfg(feature = "cells-metrics")]
        let _timer = scopeguard::guard(Instant::now(), |started_at| {
            metrics::histogram!("tycho_storage_get_cell_from_bf_tree_time")
                .record(started_at.elapsed());
        });

        let key = key.0;
        let Some((len, value)) = self.cells_store.read(key)? else {
            return Ok(None);
        };
        let len = usize::from(len);
        if len > buffer.len() {
            return Err(CellStorageError::InvalidCell);
        }
        buffer[..len].copy_from_slice(&value[..len]);
        Ok(Some(&buffer[..len]))
    }

    fn get_raw_from_tree(
        &self,
        key: &HashBytes,
    ) -> Result<Option<RawCellsCacheItem>, CellStorageError> {
        let mut buffer = [0u8; PERSISTED_VALUE_MAX_BYTES];
        let Some(value) = self.read_cells_value(key, &mut buffer)? else {
            return Ok(None);
        };
        let (_idx, data) = decode_cell_value(value).ok_or(CellStorageError::InvalidCell)?;
        Ok(Some(RawCellsCacheItem::from_header_and_slice(
            RawCellHeader,
            data,
        )))
    }

    fn get_idx_for_insert_from_tree(
        &self,
        key: &HashBytes,
    ) -> Result<Option<u32>, CellStorageError> {
        let mut buffer = [0u8; PERSISTED_VALUE_MAX_BYTES];
        let Some(value) = self.read_cells_value(key, &mut buffer)? else {
            return Ok(None);
        };
        let (idx, data) = decode_cell_value(value).ok_or(CellStorageError::InvalidCell)?;
        _ = data;
        Ok(Some(idx))
    }

    fn get_for_delete_from_tree(
        &self,
        key: &HashBytes,
        refs_buffer: &mut Vec<HashBytes>,
    ) -> Result<u32, CellStorageError> {
        let mut buffer = [0u8; PERSISTED_VALUE_MAX_BYTES];
        let Some(value) = self.read_cells_value(key, &mut buffer)? else {
            return Err(CellStorageError::CellNotFound);
        };
        let (idx, data) = decode_cell_value(value).ok_or(CellStorageError::InvalidCell)?;
        refs_buffer.clear();
        StorageCell::deserialize_references(data, refs_buffer)
            .then_some(idx)
            .ok_or(CellStorageError::InvalidCell)
    }

    fn get_idx_for_insert(&self, key: &HashBytes) -> Result<Option<u32>, CellStorageError> {
        self.get_idx_for_insert_from_tree(key)
    }

    fn get_raw(&self, key: &HashBytes) -> Result<Option<RawCellsCacheItem>, CellStorageError> {
        self.get_raw_from_tree(key)
    }

    fn get_for_delete(
        &self,
        key: &HashBytes,
        refs_buffer: &mut Vec<HashBytes>,
    ) -> Result<u32, CellStorageError> {
        self.get_for_delete_from_tree(key, refs_buffer)
    }

    pub fn apply_temp_cell(&self, root: &HashBytes) -> Result<()> {
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
            cells_db: &'a CellsDb,
            buffer: Vec<u8>,
            transaction: FastHashMap<HashBytes, TempCell>,
            new_cell_count: usize,
            insert_started_at: Option<Instant>,
        }

        impl<'a> Context<'a> {
            fn new(cell_storage: &'a CellStorage, cells_db: &'a CellsDb) -> Self {
                Self {
                    cell_storage,
                    cells_db,
                    buffer: Vec::with_capacity(512),
                    transaction: Default::default(),
                    new_cell_count: 0,
                    insert_started_at: None,
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
                        if let Some(idx) = self.cell_storage.get_idx_for_insert(key)? {
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
                        if self.insert_started_at.is_none() {
                            self.insert_started_at = Some(Instant::now());
                        }
                        let result =
                            insert_cells_value(&self.cell_storage.cells_store, key, &self.buffer);
                        result?;
                        self.cell_storage.live_cells.fetch_add(1, Ordering::AcqRel);
                        self.new_cell_count += 1;

                        InsertedCell::New(iter)
                    }
                })
            }

            fn flush_existing_cells(self) -> Result<(), CellStorageError> {
                for (_key, item) in self.transaction {
                    if item.is_new {
                        self.cell_storage.counters[item.idx as usize]
                            .store(u64::from(item.additions), Ordering::Release);
                    } else {
                        self.cell_storage.counters[item.idx as usize]
                            .fetch_add(u64::from(item.additions), Ordering::Release);
                    }
                }

                Ok(())
            }
        }

        let mut ctx = Context::new(self, &self.cells_db);

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

        if let Some(started_at) = ctx.insert_started_at {
            let elapsed = u64::try_from(started_at.elapsed().as_nanos()).unwrap_or(u64::MAX);
            self.insert_cells_ns.fetch_add(elapsed, Ordering::Relaxed);
        }

        if ctx.new_cell_count > 0 {
            metrics::histogram!("tycho_storage_cells_write_batch_puts")
                .record(ctx.new_cell_count as f64);
            metrics::histogram!("tycho_storage_cells_write_batch_deletes").record(0f64);
        }
        ctx.flush_existing_cells()?;

        Ok(())
    }

    pub fn store_cell_mt(
        &self,
        root: &DynCell,
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
            herd: &'a Herd,
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
        }

        impl<'a> StoreContext<'a> {
            fn new(
                cell_storage: &'a CellStorage,
                herd: &'a Herd,
                split_accounts: FastHashMap<HashBytes, Cell>,
                capacity: usize,
            ) -> Self {
                Self {
                    cell_storage,
                    herd,
                    split_at: split_accounts,
                    transaction: FastDashMap::with_capacity_and_hasher_and_shard_amount(
                        capacity,
                        Default::default(),
                        512,
                    ),
                    delayed_additions: Default::default(),
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
                _depth: usize,
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

                let existing_idx = self.cell_storage.get_idx_for_insert(key)?;
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

            fn finalize(self) -> Result<usize, CellStorageError> {
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
                let mut insert_count = 0usize;
                let mut buffer = Vec::with_capacity(512);
                let mut did_work = false;
                {
                    let start = Instant::now();
                    for kv in self.transaction.iter() {
                        let key = kv.key();
                        let item = kv.value();
                        if let Some(data) = item.data {
                            buffer.clear();
                            encode_cell_value(item.idx, data, &mut buffer);
                            insert_cells_value(
                                &self.cell_storage.cells_store,
                                key,
                                &buffer,
                            )?;
                            self.cell_storage.live_cells.fetch_add(1, Ordering::AcqRel);
                            insert_count += 1;
                            did_work = true;
                        }
                    }
                    if did_work {
                        let elapsed = u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX);
                        self.cell_storage
                            .insert_cells_ns
                            .fetch_add(elapsed, Ordering::Relaxed);
                    }
                }

                std::thread::scope(|s| {
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

                        let counters = &self.cell_storage.counters;
                        s.spawn(move || {
                            for shard in shards {
                                // SAFETY: `RawIter` will not outlibe the `RawTable`.
                                for value in unsafe { shard.iter() } {
                                    // SAFETY: `Bucket` is a valid item, received from a valid iterator.
                                    let (_key, value) = unsafe { value.as_ref() };
                                    let item = value.get();
                                    if item.data.is_some() {
                                        counters[item.idx as usize]
                                            .store(u64::from(item.additions), Ordering::Release);
                                    } else {
                                        counters[item.idx as usize]
                                            .fetch_add(u64::from(item.additions), Ordering::AcqRel);
                                    }
                                }
                            }
                        });
                    }
                    assert_eq!(range_start, num_shards);
                });

                if insert_count > 0 {
                    metrics::histogram!("tycho_storage_cells_write_batch_puts")
                        .record(insert_count as f64);
                    metrics::histogram!("tycho_storage_cells_write_batch_deletes").record(0f64);
                }

                Ok(total)
            }
        }

        let herd = Herd::new();
        let ctx = StoreContext::new(self, &herd, split_at, capacity);

        std::thread::scope(|scope| ctx.traverse_cell(root, scope))?;

        Ok(ctx.finalize()?)
    }

    pub fn store_cell(
        &self,
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
            alloc: &'a Bump,
            transaction: FastHashMap<&'a HashBytes, AddedCell<'a>>,
            buffer: Vec<u8>,
        }

        impl<'a> Context<'a> {
            fn insert_cell(
                &mut self,
                cell: &'a DynCell,
                _depth: usize,
            ) -> Result<bool, CellStorageError> {
                let key = cell.repr_hash();
                Ok(match self.transaction.entry(key) {
                    hash_map::Entry::Occupied(mut value) => {
                        value.get_mut().additions += 1;
                        false
                    }
                    hash_map::Entry::Vacant(entry) => {
                        let (idx, data) = match self.cell_storage.get_idx_for_insert(key)? {
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

            fn finalize(self) -> Result<usize, CellStorageError> {
                let total = self.transaction.len();
                let mut insert_count = 0usize;
                let mut buffer = Vec::with_capacity(512);
                let mut did_work = false;
                {
                    let start = Instant::now();
                    for (key, item) in self.transaction.iter() {
                        if let Some(data) = item.data {
                            buffer.clear();
                            encode_cell_value(item.idx, data, &mut buffer);
                            insert_cells_value(
                                &self.cell_storage.cells_store,
                                key,
                                &buffer,
                            )?;
                            self.cell_storage.live_cells.fetch_add(1, Ordering::AcqRel);
                            insert_count += 1;
                            did_work = true;
                        }
                    }
                    if did_work {
                        let elapsed = u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX);
                        self.cell_storage
                            .insert_cells_ns
                            .fetch_add(elapsed, Ordering::Relaxed);
                    }
                }

                for (_key, item) in self.transaction {
                    // new cell
                    if item.data.is_some() {
                        self.cell_storage.counters[item.idx as usize]
                            .store(u64::from(item.additions), Ordering::Release);
                    } else {
                        // only rc bump
                        self.cell_storage.counters[item.idx as usize]
                            .fetch_add(u64::from(item.additions), Ordering::AcqRel);
                    }
                }

                if insert_count > 0 {
                    metrics::histogram!("tycho_storage_cells_write_batch_puts")
                        .record(insert_count as f64);
                    metrics::histogram!("tycho_storage_cells_write_batch_deletes").record(0f64);
                }

                Ok(total)
            }
        }

        let alloc = bumpalo::Bump::new();

        // Prepare context and handles
        let mut ctx = Context {
            cell_storage: self,
            alloc: &alloc,
            transaction: FastHashMap::with_capacity_and_hasher(
                estimated_cell_count,
                Default::default(),
            ),
            buffer: Vec::with_capacity(512),
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

        Ok(ctx.finalize()?)
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

        let mut cell = match self.get_raw(hash)? {
            Some(value) => {
                match StorageCell::deserialize(self.clone(), hash, &value.slice, epoch) {
                    Some(cell) => Arc::new(cell),
                    None => return Err(CellStorageError::InvalidCell),
                }
            }
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
    ) -> Result<usize, CellStorageError> {
        type RemoveResult = Result<(), CellStorageError>;

        struct Alloc<'a> {
            bump: Member<'a>,
            buffer: Vec<HashBytes>,
        }

        struct RemoveContext<'a> {
            cell_storage: &'a CellStorage,
            herd: &'a Herd,
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
        }

        impl<'a> RemoveContext<'a> {
            fn new(
                cell_storage: &'a CellStorage,
                herd: &'a Herd,
                split_at: FastHashSet<HashBytes>,
            ) -> Self {
                Self {
                    cell_storage,
                    herd,
                    split_at,
                    transaction: FastDashMap::with_capacity_and_hasher_and_shard_amount(
                        128,
                        Default::default(),
                        512,
                    ),
                    delayed_removes: Default::default(),
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
                let idx = self.cell_storage.get_for_delete(repr_hash, buffer)?;
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

            fn finalize(self) -> Result<(usize, usize), CellStorageError> {
                let _hist = HistogramGuard::begin("tycho_storage_batch_write_parallel_time_high");

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

                        let counters = &self.cell_storage.counters;
                        let free_idx = &self.cell_storage.free_idx;
                        s.spawn(move || {
                            for shard in shards {
                                // SAFETY: `RawIter` will not outlibe the `RawTable`.
                                for value in unsafe { shard.iter() } {
                                    // SAFETY: `Bucket` is a valid item, received from a valid iterator.
                                    let (_key, value) = unsafe { value.as_ref() };
                                    let item = value.get();

                                    let new_rc = item.old_rc - u64::from(item.removes);
                                    counters[item.idx as usize].store(new_rc, Ordering::Release);
                                    if new_rc == 0 {
                                        free_idx.push(item.idx);
                                    }
                                }
                            }
                        });
                    }
                    assert_eq!(range_start, num_shards);
                });

                let total = self.transaction.len();
                let mut delete_candidates = Vec::new();
                for kv in self.transaction.iter() {
                    let key = kv.key();
                    let item = kv.value();

                    if item.old_rc == u64::from(item.removes) {
                        delete_candidates.push(*key);
                    }
                }

                let mut delete_count = 0usize;
                if !delete_candidates.is_empty() {
                    for key in delete_candidates {
                        delete_cells_value(&self.cell_storage.cells_store, &key)?;
                        self.cell_storage.live_cells.fetch_sub(1, Ordering::AcqRel);
                        delete_count += 1;
                    }
                }

                Ok((total, delete_count))
            }
        }

        let ctx = RemoveContext::new(self, herd, split_at);

        std::thread::scope(|scope| ctx.traverse_cell(root, scope))?;

        let (total, delete_count) = ctx.finalize()?;
        if delete_count > 0 {
            metrics::histogram!("tycho_storage_cells_write_batch_puts").record(0f64);
            metrics::histogram!("tycho_storage_cells_write_batch_deletes")
                .record(delete_count as f64);
        }
        Ok(total)
    }

    pub fn remove_cell(&self, alloc: &Bump, hash: &HashBytes) -> Result<usize, CellStorageError> {
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
                        let idx = self.get_for_delete(cell_id, &mut buffer)?;
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

        let _hist = HistogramGuard::begin("tycho_storage_batch_write_time_high");
        let total = transaction.len();
        let mut delete_candidates = Vec::new();

        for (key, item) in transaction {
            let new_rc = item.old_rc - u64::from(item.removes);
            self.counters[item.idx as usize].store(new_rc, Ordering::Release);
            if new_rc == 0 {
                delete_candidates.push(*key);
                self.free_idx.push(item.idx);
            }
        }

        let mut delete_count = 0usize;
        if !delete_candidates.is_empty() {
            for key in delete_candidates {
                delete_cells_value(&self.cells_store, &key)?;
                self.live_cells.fetch_sub(1, Ordering::AcqRel);
                delete_count += 1;
            }
        }

        if delete_count > 0 {
            metrics::histogram!("tycho_storage_cells_write_batch_puts").record(0f64);
            metrics::histogram!("tycho_storage_cells_write_batch_deletes")
                .record(delete_count as f64);
        }
        Ok(total)
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
    #[error("FASTER init failed: {0:?}")]
    FasterInit(FasterError<'static>),
    #[error("FASTER index growth failed")]
    FasterGrowIndexFailed,
    #[error("FASTER status: {0}")]
    FasterStatus(u8),
    #[error("FASTER read returned empty value")]
    FasterReadEmpty,
    #[error("FASTER storage path is not valid UTF-8")]
    FasterPathNotUtf8,
    #[error("FASTER worker channel closed")]
    WorkerChannelClosed,
    #[error("FASTER worker thread spawn failed")]
    WorkerThreadSpawnFailed(#[source] std::io::Error),
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

#[derive(Clone, Copy)]
struct RawCellHeader;

type RawCellsCacheItem = ThinArc<RawCellHeader, u8>;

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
            cell_storage.store_cell(root.as_ref(), 1)?;
        }

        // Check hashes
        for root in &cells {
            let loaded = cell_storage.load_cell(root.repr_hash(), 0)?;
            for level in 0..4 {
                assert_eq!(loaded.hash(level), root.hash(level));
                assert_eq!(loaded.depth(level), root.depth(level));
            }
        }

        // Remove all cells.
        let mut alloc = bumpalo::Bump::new();
        for root in &cells {
            let _ = cell_storage.remove_cell(&alloc, root.repr_hash())?;
            alloc.reset();
        }

        let cells_left = cell_storage.count_cells()?;
        assert_eq!(cells_left, 0);

        Ok(())
    }

    #[tokio::test]
    async fn remove_cell_mt_with_persisted_cells_does_not_corrupt_refs() -> Result<()> {
        let (ctx, _tmp_dir) = StorageContext::new_temp().await?;
        let storage = CoreStorage::open(ctx, CoreStorageConfig::new_potato()).await?;

        let cell_storage = &storage.shard_state_storage().cell_storage;

        let child = CellBuilder::build_from(0u32)?;

        let mut root_builder = CellBuilder::new();
        root_builder.store_u32(1)?;
        root_builder.store_reference(child.clone())?;
        let root = root_builder.build()?;

        cell_storage.store_cell(root.as_ref(), 2)?;

        let herd = Herd::new();
        let _ = cell_storage.remove_cell_mt(&herd, root.repr_hash(), Default::default())?;

        let cells_left = cell_storage.count_cells()?;
        assert_eq!(cells_left, 0);

        Ok(())
    }
}
