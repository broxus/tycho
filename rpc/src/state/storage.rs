use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, ensure};
use arc_swap::ArcSwap;
use parking_lot::{Mutex, RwLock};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, TryAcquireError};
use tokio::task::JoinHandle;
use thiserror::Error;
use tycho_block_util::block::BlockStuff;
use tycho_block_util::state::ShardStateStuff;
use tycho_storage::StorageContext;
use tycho_storage::kv::InstanceId;
use tycho_storage::kv::DEFAULT_MIN_BLOB_SIZE;
use tycho_types::cell::Lazy;
use tycho_types::models::*;
use tycho_types::prelude::*;
use tycho_util::metrics::HistogramGuard;
use tycho_util::sync::CancellationFlag;
use tycho_util::{FastHashMap, FastHashSet};
use weedb::rocksdb;

use crate::config::RpcTransactionPartitionsConfig;

use super::db::{RpcCurrentStateDb, RpcTransactionsDb};
use super::filter::{FilterNamespace, FilterRegistry, FilterWorker, ValidatedFilterBundle};
use super::partition::{PartitionDescriptor, PartitionId, PartitionManager, PartitionReadLease};
use super::tables;
use super::codec;

#[derive(Default, Clone)]
pub struct BlacklistedAccounts {
    inner: Arc<BlacklistedAccountsInner>,
}

impl BlacklistedAccounts {
    pub fn update<I: IntoIterator<Item = StdAddr>>(&self, items: I) {
        let items = items
            .into_iter()
            .map(|item| {
                let mut key = [0; 33];
                key[0] = item.workchain as u8;
                key[1..33].copy_from_slice(item.address.as_array());
                key
            })
            .collect::<FastHashSet<_>>();

        self.inner.accounts.store(Arc::new(items));
    }

    pub fn load(&self) -> Arc<FastHashSet<AddressKey>> {
        self.inner.accounts.load_full()
    }
}

#[derive(Default)]
struct BlacklistedAccountsInner {
    accounts: ArcSwap<FastHashSet<AddressKey>>,
}

pub struct RpcStorage {
    partitions: Arc<Mutex<PartitionManager>>,
    block_set_admission: Mutex<Option<BlockSetAdmission>>,
    current_state: RpcCurrentStateDb,
    min_tx_lt: AtomicU64,
    min_tx_lt_guard: tokio::sync::Mutex<()>,
    snapshots: Arc<SnapshotPublisher>,
    sealing_notify: Arc<Notify>,
    sealing_cancel: CancellationFlag,
    sealing_task: Option<JoinHandle<()>>,
    filter_registry: Arc<FilterRegistry>,
    filter_worker: Arc<FilterWorker>,
    sealed_exact_lookup_semaphore: Arc<Semaphore>,
    sealed_exact_lookup_metrics: Arc<SealedExactLookupMetrics>,
    initial_snapshot_recorded: AtomicBool,
    #[cfg(test)]
    known_block_point_reads: AtomicU64,
    #[cfg(test)]
    sealed_exact_lookup_acquisitions: Arc<AtomicU64>,
}

#[derive(Clone, Copy)]
struct BlockSetAdmission {
    frontier: BlockId,
    block_set: BlockId,
    mode: BlockSetMode,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BlockSetMode {
    New,
    Replay,
    Same,
}

impl BlockSetMode {
    const fn as_str(self) -> &'static str {
        match self {
            Self::New => "new",
            Self::Replay => "replay",
            Self::Same => "same",
        }
    }
}

fn classify_admission_failure(error: &anyhow::Error) -> &'static str {
    let message = format!("{error:#}");
    if message.contains("sequence gap") || message.contains("sequence number overflow") {
        "sequence_gap"
    } else if message.contains("admitted token") || message.contains("admission is already held") {
        "token_conflict"
    } else if message.contains("full block id") || message.contains("different full") {
        "full_identity_mismatch"
    } else if message.contains("commit") {
        "commit_invalid"
    } else if message.contains("frontier") {
        "frontier_mismatch"
    } else if message.contains("lifecycle") || message.contains("creating") {
        "lifecycle_continuation"
    } else {
        "other"
    }
}

fn classify_predecessor_failure(error: &anyhow::Error) -> &'static str {
    let message = format!("{error:#}");
    if message.contains("missing") {
        "commit_missing"
    } else if message.contains("digest") {
        "commit_digest_mismatch"
    } else if message.contains("identity") || message.contains("different") || message.contains("predecessor") {
        "full_identity_mismatch"
    } else if message.contains("malformed") || message.contains("invalid") || message.contains("decode") {
        "commit_malformed"
    } else {
        "other"
    }
}

#[derive(Default)]
struct SnapshotPublisher {
    /// Keeps publication and sealing lease removal in one lock order.
    current: RwLock<Option<RpcSnapshot>>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BlockWriteStats {
    pub transaction_count: u64,
    pub index_record_count: u64,
    pub estimated_lsm_bytes: u64,
    pub estimated_blob_bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlockWriteResult {
    pub partition_id: u64,
    pub stats: BlockWriteStats,
    pub newly_committed: bool,
}

pub(crate) struct StartupReconciliation {
    pub effective_frontier: BlockId,
    pub rebuild_current_state: bool,
}

struct LocatedTransaction {
    partition: RpcTransactionPartitionRead,
    info: TransactionInfo,
    key: Vec<u8>,
    sealed_exact_lookup_permit: Option<TrackedSealedExactLookupPermit>,
}

struct LocatedInboundTransaction {
    partition: RpcTransactionPartitionRead,
    transaction: TransactionData<'static>,
    sealed_exact_lookup_permit: Option<TrackedSealedExactLookupPermit>,
}

struct LocatedBlock {
    partition: RpcTransactionPartitionRead,
    key: [u8; tables::KnownBlocks::KEY_LEN],
    value: Vec<u8>,
    sealed_exact_lookup_permit: Option<TrackedSealedExactLookupPermit>,
}

#[derive(Debug, Error, PartialEq, Eq)]
enum SealedExactLookupAvailabilityError {
    #[error("RPC sealed exact lookup capacity is exhausted")]
    Overloaded,
    #[error("RPC sealed exact lookup limiter is closed")]
    Closed,
}

#[derive(Default)]
struct SealedExactLookupMetrics {
    current: AtomicU64,
    peak: AtomicU64,
    filter_negative_skips: [AtomicU64; 3],
    filter_false_positives: [AtomicU64; 3],
}

struct TrackedSealedExactLookupPermit {
    _permit: OwnedSemaphorePermit,
    metrics: Arc<SealedExactLookupMetrics>,
}

impl Drop for TrackedSealedExactLookupPermit {
    fn drop(&mut self) {
        let current = self.metrics.current.fetch_sub(1, Ordering::AcqRel) - 1;
        metrics::gauge!("tycho_storage_rpc_sealed_exact_lookup_permits", "result" => "current")
            .set(current as f64);
    }
}

struct SealedExactLookupContext {
    semaphore: Arc<Semaphore>,
    metrics: Arc<SealedExactLookupMetrics>,
    permit: Option<TrackedSealedExactLookupPermit>,
    namespace: FilterNamespace,
    started_at: Instant,
    filters_checked: u64,
    negative_skips: u64,
    positives: u64,
    false_positives: u64,
    partitions_traversed: u64,
    result: &'static str,
    #[cfg(test)]
    acquisitions: Arc<AtomicU64>,
}

impl SealedExactLookupContext {
    fn new(
        semaphore: Arc<Semaphore>,
        metrics: Arc<SealedExactLookupMetrics>,
        namespace: FilterNamespace,
        #[cfg(test)] acquisitions: Arc<AtomicU64>,
    ) -> Self {
        Self {
            semaphore,
            metrics,
            permit: None,
            namespace,
            started_at: Instant::now(),
            filters_checked: 0,
            negative_skips: 0,
            positives: 0,
            false_positives: 0,
            partitions_traversed: 0,
            result: "error",
            #[cfg(test)]
            acquisitions,
        }
    }

    fn acquire(&mut self) -> Result<()> {
        if self.permit.is_some() {
            return Ok(());
        }
        let permit = match self.semaphore.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(TryAcquireError::NoPermits) => {
                metrics::counter!(
                    "tycho_storage_rpc_sealed_exact_lookup_overloads_total",
                    "reason" => "exhausted",
                )
                .increment(1);
                return Err(SealedExactLookupAvailabilityError::Overloaded.into());
            }
            Err(TryAcquireError::Closed) => {
                metrics::counter!(
                    "tycho_storage_rpc_sealed_exact_lookup_overloads_total",
                    "reason" => "closed",
                )
                .increment(1);
                return Err(SealedExactLookupAvailabilityError::Closed.into());
            }
        };
        #[cfg(test)]
        self.acquisitions.fetch_add(1, Ordering::AcqRel);
        let current = self.metrics.current.fetch_add(1, Ordering::AcqRel) + 1;
        let mut peak = self.metrics.peak.load(Ordering::Acquire);
        while current > peak {
            match self.metrics.peak.compare_exchange_weak(
                peak,
                current,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => peak = actual,
            }
        }
        metrics::gauge!("tycho_storage_rpc_sealed_exact_lookup_permits", "result" => "current")
            .set(current as f64);
        metrics::gauge!("tycho_storage_rpc_sealed_exact_lookup_permits", "result" => "peak")
            .set(self.metrics.peak.load(Ordering::Acquire) as f64);
        self.permit = Some(TrackedSealedExactLookupPermit {
            _permit: permit,
            metrics: self.metrics.clone(),
        });
        Ok(())
    }

    fn into_permit(mut self) -> Option<TrackedSealedExactLookupPermit> {
        self.permit.take()
    }

    fn consider_partition(&mut self) {
        self.partitions_traversed += 1;
    }

    fn record_filter(&mut self, might_contain: bool) {
        self.filters_checked += 1;
        if might_contain {
            self.positives += 1;
        } else {
            self.negative_skips += 1;
        }
    }

    fn record_exact_probe(&self, lifecycle: &'static str, reason: &'static str) {
        metrics::counter!(
            "tycho_storage_rpc_exact_lookup_probes_total",
            "namespace" => self.namespace.as_str(),
            "lifecycle" => lifecycle,
            "reason" => reason,
        )
        .increment(1);
    }

    fn record_false_positive(&mut self) {
        self.false_positives += 1;
    }

    fn record_rejection(&self, lifecycle: &'static str, reason: &'static str) {
        metrics::counter!(
            "tycho_storage_rpc_exact_lookup_rejections_total",
            "namespace" => self.namespace.as_str(),
            "lifecycle" => lifecycle,
            "reason" => reason,
        )
        .increment(1);
    }

    fn record_hit(&mut self, lifecycle: &'static str) {
        self.result = "hit";
        metrics::counter!(
            "tycho_storage_rpc_exact_lookup_hits_total",
            "namespace" => self.namespace.as_str(),
            "lifecycle" => lifecycle,
        )
        .increment(1);
    }

    fn record_miss(&mut self) {
        self.result = "miss";
    }
}

impl Drop for SealedExactLookupContext {
    fn drop(&mut self) {
        let namespace = self.namespace.as_str();
        metrics::counter!(
            "tycho_storage_rpc_exact_lookup_requests_total",
            "namespace" => namespace,
            "result" => self.result,
        )
        .increment(1);
        metrics::counter!(
            "tycho_storage_rpc_filters_checked_total",
            "namespace" => namespace,
        )
        .increment(self.filters_checked);
        metrics::counter!(
            "tycho_storage_rpc_filter_negative_skips_total",
            "namespace" => namespace,
        )
        .increment(self.negative_skips);
        metrics::counter!(
            "tycho_storage_rpc_filter_positives_total",
            "namespace" => namespace,
        )
        .increment(self.positives);
        metrics::counter!(
            "tycho_storage_rpc_filter_false_positives_total",
            "namespace" => namespace,
        )
        .increment(self.false_positives);
        metrics::histogram!(
            "tycho_storage_rpc_exact_lookup_partitions_traversed",
            "namespace" => namespace,
        )
        .record(self.partitions_traversed as f64);
        metrics::histogram!(
            "tycho_storage_rpc_exact_lookup_duration_seconds",
            "namespace" => namespace,
            "result" => self.result,
        )
        .record(self.started_at.elapsed());
        let index = self.namespace as usize - 1;
        let negative_skips = self.metrics.filter_negative_skips[index]
            .fetch_add(self.negative_skips, Ordering::AcqRel)
            + self.negative_skips;
        let false_positives = self.metrics.filter_false_positives[index]
            .fetch_add(self.false_positives, Ordering::AcqRel)
            + self.false_positives;
        metrics::gauge!(
            "tycho_storage_rpc_filter_observed_error_ratio",
            "namespace" => namespace,
        )
        .set(if negative_skips + false_positives == 0 {
            0.0
        } else {
            false_positives as f64 / (negative_skips + false_positives) as f64
        });
    }
}

impl BlockWriteStats {
    fn add_record(&mut self, key_len: usize, value_len: usize, blob_value: bool) -> Result<()> {
        let key_len = u64::try_from(key_len).context("transaction record key length exceeds u64")?;
        let value_len = u64::try_from(value_len).context("transaction record value length exceeds u64")?;
        self.estimated_lsm_bytes = self.estimated_lsm_bytes.checked_add(key_len).context("transaction LSM estimate overflow")?;
        if blob_value {
            self.estimated_blob_bytes = self.estimated_blob_bytes.checked_add(value_len).context("transaction blob estimate overflow")?;
        } else {
            self.estimated_lsm_bytes = self.estimated_lsm_bytes.checked_add(value_len).context("transaction LSM estimate overflow")?;
        }
        Ok(())
    }

    fn add_transaction(&mut self, tx_value_len: usize, has_in_msg: bool) -> Result<()> {
        self.transaction_count = self.transaction_count.checked_add(1).context("transaction count overflow")?;
        self.index_record_count = self.index_record_count.checked_add(if has_in_msg { 4 } else { 3 }).context("transaction index count overflow")?;
        let tx_value_len_u64 = u64::try_from(tx_value_len).context("transaction value length exceeds u64")?;
        self.add_record(tables::Transactions::KEY_LEN, tx_value_len, tx_value_len_u64 >= DEFAULT_MIN_BLOB_SIZE)?;
        self.add_record(32, tables::TransactionsByHash::VALUE_FULL_LEN, false)?;
        if has_in_msg { self.add_record(32, tables::Transactions::KEY_LEN, false)?; }
        self.add_record(tables::BlockTransactions::KEY_LEN, 32, false)
    }
}

fn spawn_sealing_worker(
    partitions: Arc<Mutex<PartitionManager>>,
    snapshots: Arc<SnapshotPublisher>,
    filter_registry: Arc<FilterRegistry>,
    notify: Arc<Notify>,
    cancelled: CancellationFlag,
    filter_worker: Arc<FilterWorker>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut retry_delay = Duration::from_secs(1);
        loop {
            notify.notified().await;
            while !cancelled.check() {
                let Some(id) = partitions.lock().next_sealing_partition() else {
                    break;
                };
                let started_at = Instant::now();
                let result = seal_partition(
                    partitions.clone(),
                    snapshots.clone(),
                    filter_registry.clone(),
                    id,
                    cancelled.clone(),
                    Some(filter_worker.clone()),
                )
                .await;
                metrics::histogram!("tycho_storage_rpc_partition_sealing_time")
                    .record(started_at.elapsed());
                if let Err(e) = result {
                    if !cancelled.check() {
                        metrics::counter!(
                            "tycho_storage_rpc_partition_sealing_failures_total"
                        )
                        .increment(1);
                    }
                    tracing::error!(partition_id = id.0, "failed to seal RPC transaction partition: {e:#}");
                    tokio::time::sleep(retry_delay).await;
                    retry_delay = retry_delay.saturating_mul(2).min(Duration::from_secs(30));
                } else {
                    retry_delay = Duration::from_secs(1);
                }
            }
            if cancelled.check() {
                break;
            }
        }
    })
}

async fn seal_partition(
    partitions: Arc<Mutex<PartitionManager>>,
    snapshots: Arc<SnapshotPublisher>,
    filter_registry: Arc<FilterRegistry>,
    id: PartitionId,
    cancelled: CancellationFlag,
    filter_worker: Option<Arc<FilterWorker>>,
) -> Result<()> {
    tokio::task::spawn_blocking(move || {
        let db = partitions.lock().begin_sealing(id)?;
        // reduce the final gated flush while old readers and pre-closing writers drain
        flush_transaction_partition(&db)?;

        let mut wait_delay = Duration::from_millis(10);
        loop {
            anyhow::ensure!(
                !cancelled.check(),
                "RPC transaction partition sealing cancelled"
            );
            let ready = {
                let published = snapshots.current.read();
                sealing_snapshot_can_be_withdrawn(&partitions, published.as_ref(), id)
            };
            if ready {
                let mut published = snapshots.current.write();
                if sealing_snapshot_can_be_withdrawn(&partitions, published.as_ref(), id) {
                    let previous = published.take();
                    let frontier =
                        previous.as_ref().map(|snapshot| *snapshot.visible_frontier());
                    drop(previous);

                    // a lease acquired before closing may have written after the preliminary flush
                    let seal_result = flush_transaction_partition(&db)
                        .and_then(|()| finish_sealing_partition(&partitions, id, db));
                    if seal_result.is_ok()
                        && let Some(filter_worker) = &filter_worker
                    {
                        filter_worker.notify_sealed(id);
                    }
                    let snapshot_result = match frontier {
                        Some(frontier) => rebuild_composite_snapshot_until_ready(
                            &partitions,
                            &filter_registry,
                            frontier,
                            &cancelled,
                        )
                        .map(Some),
                        None => Ok(None),
                    };
                    let snapshot_error = match snapshot_result {
                        Ok(Some(snapshot)) => {
                            *published = Some(snapshot);
                            None
                        }
                        Ok(None) => None,
                        Err(e) => Some(e),
                    };
                    return match (seal_result, snapshot_error) {
                        (Ok(()), None) => Ok(()),
                        (Err(e), None) | (Ok(()), Some(e)) => Err(e),
                        (Err(seal_error), Some(snapshot_error)) => Err(anyhow::anyhow!(
                            "sealing failed: {seal_error:#}; composite snapshot rebuild failed: {snapshot_error:#}"
                        )),
                    };
                }
            }
            std::thread::sleep(wait_delay);
            wait_delay = wait_delay
                .saturating_mul(2)
                .min(Duration::from_millis(250));
        }
    })
    .await?
}

fn sealing_snapshot_can_be_withdrawn(
    partitions: &Arc<Mutex<PartitionManager>>,
    published: Option<&RpcSnapshot>,
    id: PartitionId,
) -> bool {
    let current_is_unshared = published.is_none_or(|snapshot| Arc::strong_count(&snapshot.0) == 1);
    if !current_is_unshared {
        return false;
    }
    let current_has_lease = published
        .is_some_and(|snapshot| snapshot.0.writable_partitions.contains_key(&id));
    let expected_handles = 2 + usize::from(current_has_lease);
    partitions.lock().sealing_handle_strong_count(id) == Some(expected_handles)
}

fn rebuild_composite_snapshot_until_ready(
    partitions: &Arc<Mutex<PartitionManager>>,
    filter_registry: &Arc<FilterRegistry>,
    frontier: BlockId,
    cancelled: &CancellationFlag,
) -> Result<RpcSnapshot> {
    let mut retry_delay = Duration::from_millis(10);
    loop {
        match build_composite_snapshot(&mut partitions.lock(), filter_registry, frontier) {
            Ok(snapshot) => return Ok(snapshot),
            Err(e) if cancelled.check() => {
                return Err(e).context(
                    "RPC composite snapshot rebuild cancelled after sealing",
                );
            }
            Err(e) => {
                tracing::error!(
                    retry_delay_ms = retry_delay.as_millis(),
                    "failed to rebuild RPC composite snapshot after sealing: {e:#}"
                );
                std::thread::sleep(retry_delay);
                retry_delay = retry_delay
                    .saturating_mul(2)
                    .min(Duration::from_secs(1));
            }
        }
    }
}

fn finish_sealing_partition(
    partitions: &Arc<Mutex<PartitionManager>>,
    id: PartitionId,
    db: Arc<RpcTransactionsDb>,
) -> Result<()> {
    let closed = partitions.lock().take_sealing_handle(id)?;
    drop(closed);
    drop(db);

    let read_only = match partitions.lock().open_sealed_read_only(id) {
        Ok(db) => db,
        Err(read_only_error) => {
            let reopened = partitions.lock().reopen_sealing_writable(id)
                .context("failed to restore writable sealing partition after read-only reopen failure")?;
            partitions.lock().restore_sealing_handle(id, reopened);
            return Err(read_only_error).context("failed to reopen sealed transaction partition read-only");
        }
    };
    if let Err(e) = partitions.lock().complete_sealing(id, read_only.clone()) {
        drop(read_only);
        let reopened = partitions.lock().reopen_sealing_writable(id)
            .context("failed to restore writable sealing partition after control commit failure")?;
        partitions.lock().restore_sealing_handle(id, reopened);
        return Err(e).context("failed to persist sealed transaction partition");
    }
    Ok(())
}

fn flush_transaction_partition(db: &RpcTransactionsDb) -> Result<()> {
    let raw = db.rocksdb();
    raw.flush_wal(true)?;
    let mut options = rocksdb::FlushOptions::default();
    options.set_wait(true);
    raw.flush_cf_opt(&db.transactions.cf(), &options)?;
    raw.flush_cf_opt(&db.transactions_by_hash.cf(), &options)?;
    raw.flush_cf_opt(&db.transactions_by_in_msg.cf(), &options)?;
    raw.flush_cf_opt(&db.known_blocks.cf(), &options)?;
    raw.flush_cf_opt(&db.block_transactions.cf(), &options)?;
    raw.flush_cf_opt(&db.blocks_by_mc_seqno.cf(), &options)?;
    raw.flush_cf_opt(&db.partition_commits.cf(), &options)?;
    Ok(())
}

impl RpcStorage {
    pub fn open(context: StorageContext, config: RpcTransactionPartitionsConfig) -> Result<Self> {
        let filter_context = context.clone();
        let filter_config = config.filters.clone();
        let sealed_exact_lookup_semaphore = Arc::new(Semaphore::new(
            filter_config.max_concurrent_sealed_exact_lookups,
        ));
        let sealed_exact_lookup_metrics = Arc::new(SealedExactLookupMetrics::default());
        #[cfg(test)]
        let sealed_exact_lookup_acquisitions = Arc::new(AtomicU64::new(0));
        let partitions = Arc::new(Mutex::new(PartitionManager::open(context, config)?));
        let current_state = partitions.lock().current_state_db().clone();
        let persisted_min_lt = partitions.lock().min_transaction_lt();
        let snapshots = Arc::new(SnapshotPublisher::default());
        let filter_registry = Arc::new(FilterRegistry::default());
        let filter_publisher = {
            let partitions = partitions.clone();
            let snapshots = snapshots.clone();
            let filter_registry = filter_registry.clone();
            Arc::new(move |id, bundle| {
                publish_filter_snapshot(
                    &partitions,
                    &snapshots,
                    &filter_registry,
                    id,
                    bundle,
                )
            })
        };
        let filter_worker = Arc::new(FilterWorker::new(
            filter_context,
            filter_config,
            partitions.clone(),
            filter_registry.clone(),
            filter_publisher,
        ));
        let sealing_notify = Arc::new(Notify::new());
        let sealing_cancel = CancellationFlag::new();
        let sealing_task = Some(spawn_sealing_worker(
            partitions.clone(),
            snapshots.clone(),
            filter_registry.clone(),
            sealing_notify.clone(),
            sealing_cancel.clone(),
            filter_worker.clone(),
        ));
        let this = Self {
            partitions,
            block_set_admission: Default::default(),
            current_state,
            min_tx_lt: AtomicU64::new(u64::MAX),
            min_tx_lt_guard: Default::default(),
            snapshots,
            sealing_notify,
            sealing_cancel,
            sealing_task,
            filter_registry,
            filter_worker,
            sealed_exact_lookup_semaphore,
            sealed_exact_lookup_metrics,
            initial_snapshot_recorded: AtomicBool::new(false),
            #[cfg(test)]
            known_block_point_reads: AtomicU64::new(0),
            #[cfg(test)]
            sealed_exact_lookup_acquisitions,
        };

        let state = &this.current_state.state;
        if state.get(INSTANCE_ID)?.is_none() {
            state.insert(INSTANCE_ID, rand::random::<InstanceId>())?;
        }

        let min_lt = (persisted_min_lt != u64::MAX).then_some(persisted_min_lt);

        this.min_tx_lt
            .store(min_lt.unwrap_or(u64::MAX), Ordering::Release);

        tracing::debug!(?min_lt, "rpc storage initialized");

        Ok(this)
    }

    pub fn min_tx_lt(&self) -> u64 {
        self.min_tx_lt.load(Ordering::Acquire)
    }

    pub(crate) fn reconcile_startup(
        &self,
        core_frontier: &BlockId,
    ) -> Result<StartupReconciliation> {
        let started_at = Instant::now();
        let sealed_before = self.partitions.lock().sealed_cache_entry_count();
        let result = self.reconcile_startup_inner(core_frontier);
        let sealed_after = self.partitions.lock().sealed_cache_entry_count();
        debug_assert!(sealed_after.saturating_sub(sealed_before) <= 1);
        let result_label = if result.is_ok() { "success" } else { "failure" };
        metrics::histogram!(
            "tycho_storage_rpc_startup_reconciliation_duration_seconds",
            "result" => result_label,
        )
        .record(started_at.elapsed());
        metrics::histogram!(
            "tycho_storage_rpc_startup_synchronous_sealed_opens",
            "result" => result_label,
        )
        .record(sealed_after.saturating_sub(sealed_before) as f64);
        result
    }

    fn reconcile_startup_inner(
        &self,
        core_frontier: &BlockId,
    ) -> Result<StartupReconciliation> {
        anyhow::ensure!(
            core_frontier.is_masterchain(),
            "core RPC startup frontier must be a masterchain block"
        );
        let partitions = self.partitions.lock();
        let control_frontier = partitions.visible_frontier().copied();
        match control_frontier {
            Some(control_frontier) if control_frontier.seqno < core_frontier.seqno => {
                anyhow::bail!(
                    "RPC visible frontier {} is behind core committed masterchain frontier {}; clear the RPC DB and reindex",
                    control_frontier,
                    core_frontier
                );
            }
            Some(control_frontier) if control_frontier.seqno == core_frontier.seqno => {
                anyhow::ensure!(
                    control_frontier == *core_frontier,
                    "RPC and core frontiers have different full block ids at masterchain seqno {}",
                    core_frontier.seqno
                );
            }
            None if core_frontier.seqno > 0 => {
                anyhow::bail!(
                    "RPC visible frontier is missing while core committed masterchain frontier is {}; clear the RPC DB and reindex",
                    core_frontier
                );
            }
            _ => {}
        }

        if let Some(control_frontier) = control_frontier
            && control_frontier.seqno > 0
        {
            partitions
                .validate_masterchain_commit(&control_frontier)
                .context("invalid persisted RPC visible frontier")?;
        }

        let rebuild_current_state = control_frontier.is_some_and(|frontier| frontier.seqno > core_frontier.seqno)
            || partitions.active_has_commit_after(core_frontier.seqno)?;
        Ok(StartupReconciliation {
            effective_frontier: *core_frontier,
            rebuild_current_state,
        })
    }

    pub fn continue_lifecycle(&self) -> Result<()> {
        let started_at = Instant::now();
        let (result, notify_sealing) = {
            let mut partitions = self.partitions.lock();
            let result = partitions.continue_lifecycle();
            let notify_sealing = result.is_ok() && partitions.next_sealing_partition().is_some();
            (result, notify_sealing)
        };
        metrics::histogram!(
            "tycho_storage_rpc_deferred_lifecycle_duration_seconds",
            "result" => if result.is_ok() { "success" } else { "failure" },
        )
        .record(started_at.elapsed());
        if notify_sealing {
            self.sealing_notify.notify_one();
        }
        result
    }

    pub fn publish_snapshot(&self, visible_frontier: &BlockId) -> Result<()> {
        let started_at = Instant::now();
        let result = self.publish_snapshot_inner(visible_frontier);
        if !self.initial_snapshot_recorded.swap(true, Ordering::AcqRel) {
            metrics::histogram!(
                "tycho_storage_rpc_initial_local_snapshot_duration_seconds",
                "result" => if result.is_ok() { "success" } else { "failure" },
            )
            .record(started_at.elapsed());
        }
        result
    }

    fn publish_snapshot_inner(&self, visible_frontier: &BlockId) -> Result<()> {
        let mut published = self.snapshots.current.write();
        // an older replay must not regress an already published effective frontier
        let visible_frontier = match published.as_ref() {
            Some(current) if current.visible_frontier().seqno > visible_frontier.seqno => {
                *current.visible_frontier()
            }
            Some(current) if current.visible_frontier().seqno == visible_frontier.seqno => {
                anyhow::ensure!(
                    current.visible_frontier() == visible_frontier,
                    "cannot publish a different RPC snapshot frontier at masterchain seqno {}",
                    visible_frontier.seqno
                );
                *visible_frontier
            }
            _ => *visible_frontier,
        };
        let snapshot = build_composite_snapshot(
            &mut self.partitions.lock(),
            &self.filter_registry,
            visible_frontier,
        )?;
        *published = Some(snapshot);
        Ok(())
    }

    pub fn load_snapshot(&self) -> Option<RpcSnapshot> {
        self.snapshots.current.read().clone()
    }

    pub(crate) fn start_filter_worker(&self) {
        self.filter_worker.start();
    }

    #[cfg(test)]
    fn reset_known_block_point_reads(&self) {
        self.known_block_point_reads.store(0, Ordering::Release);
    }

    #[cfg(test)]
    fn known_block_point_reads(&self) -> u64 {
        self.known_block_point_reads.load(Ordering::Acquire)
    }

    #[cfg(test)]
    /// Test-only boundary helper that bypasses admission after a fixture has written its commit.
    pub fn commit_masterchain_block_set(&self, block_id: &BlockId) -> Result<()> {
        self.partitions.lock().commit_masterchain_block_set(block_id)?;
        self.publish_snapshot(block_id)?;
        if self.partitions.lock().next_sealing_partition().is_some() {
            self.sealing_notify.notify_one();
        }
        Ok(())
    }

    pub(crate) fn commit_masterchain_block_set_with_predecessor(
        &self,
        block_id: &BlockId,
        predecessor: &BlockId,
    ) -> Result<()> {
        let mut admission = self.block_set_admission.lock();
        let mut published = self.snapshots.current.write();
        let mut token = admission
            .as_ref()
            .copied()
            .context("RPC block-set boundary has no admitted token")?;
        ensure!(
            token.block_set == *block_id,
            "RPC block-set boundary does not match the admitted block set"
        );
        ensure!(
            published.as_ref().map(|snapshot| *snapshot.visible_frontier()) == Some(token.frontier),
            "RPC effective frontier changed after block-set admission"
        );

        let notify_sealing = {
            let mut partitions = self.partitions.lock();
            let persisted = partitions.visible_frontier().copied();
            if token.mode == BlockSetMode::New && persisted == Some(*block_id) {
                token.mode = BlockSetMode::Replay;
                *admission = Some(token);
            }
            if matches!(token.mode, BlockSetMode::New | BlockSetMode::Replay) {
                let started_at = Instant::now();
                let predecessor_result = (|| {
                    ensure!(
                        predecessor == &token.frontier,
                        "RPC masterchain block-set predecessor does not match the admitted frontier"
                    );
                    Ok(())
                })();
                metrics::histogram!(
                    "tycho_storage_rpc_predecessor_validation_duration_seconds",
                    "stage" => "full_identity",
                    "result" => if predecessor_result.is_ok() { "success" } else { "failure" },
                )
                .record(started_at.elapsed());
                if let Err(error) = &predecessor_result {
                    metrics::counter!(
                        "tycho_storage_rpc_predecessor_validation_failures_total",
                        "reason" => classify_predecessor_failure(error),
                    )
                    .increment(1);
                }
                predecessor_result?;
            }
            match token.mode {
                BlockSetMode::New => ensure!(
                    persisted == Some(token.frontier) || (token.frontier.seqno == 0 && persisted.is_none()),
                    "RPC persisted frontier changed before a new block-set boundary"
                ),
                BlockSetMode::Replay => {
                    let persisted = persisted.context("RPC replay boundary is missing the durable frontier")?;
                    ensure!(
                        persisted.seqno >= block_id.seqno,
                        "RPC replay boundary durable frontier is behind the admitted block set"
                    );
                    if persisted.seqno == block_id.seqno {
                        ensure!(
                            persisted == *block_id,
                            "RPC replay boundary has a different full durable frontier id"
                        );
                    }
                }
                BlockSetMode::Same => ensure!(
                    persisted.is_some_and(|persisted| {
                        persisted.seqno > token.frontier.seqno || persisted == token.frontier
                    }),
                    "RPC same-boundary retry has an invalid durable frontier"
                ),
            }
            partitions.commit_masterchain_block_set_after_admission(
                block_id,
                token.mode == BlockSetMode::Same,
            )?;
            if token.mode != BlockSetMode::Same {
                let snapshot =
                    build_composite_snapshot(&mut partitions, &self.filter_registry, *block_id)?;
                *published = Some(snapshot);
            }
            partitions.next_sealing_partition().is_some()
        };

        admission.take();
        drop(published);
        drop(admission);
        if notify_sealing {
            self.sealing_notify.notify_one();
        }
        Ok(())
    }
    fn admit_block_set(&self, block_set: &BlockId) -> Result<BlockSetMode> {
        let mode = {
            let published = self.snapshots.current.read();
            let effective = published.as_ref().map(|snapshot| *snapshot.visible_frontier());
            let persisted = self.partitions.lock().visible_frontier().copied();
            match effective {
                Some(effective) if effective == *block_set => BlockSetMode::Same,
                _ if persisted.is_some_and(|persisted| persisted.seqno >= block_set.seqno) => {
                    BlockSetMode::Replay
                }
                _ => BlockSetMode::New,
            }
        };
        let result = self.admit_block_set_inner(block_set);
        let recorded_mode = result.as_ref().copied().unwrap_or(mode);
        metrics::counter!(
            "tycho_storage_rpc_block_set_admission_checks_total",
            "stage" => recorded_mode.as_str(),
            "result" => if result.is_ok() { "success" } else { "failure" },
        )
        .increment(1);
        if let Err(error) = &result {
            metrics::counter!(
                "tycho_storage_rpc_block_set_admission_failures_total",
                "stage" => recorded_mode.as_str(),
                "reason" => classify_admission_failure(error),
            )
            .increment(1);
        }
        result
    }

    fn admit_block_set_inner(&self, block_set: &BlockId) -> Result<BlockSetMode> {
        let mut admission = self.block_set_admission.lock();
        let mut published = self.snapshots.current.write();
        let mut frontier = published
            .as_ref()
            .context("RPC block-set admission requires a published snapshot")?
            .0
            .visible_frontier;
        let mut partitions = self.partitions.lock();

        if partitions.has_creating_partition() {
            let started_at = Instant::now();
            let result = partitions.continue_lifecycle();
            metrics::histogram!(
                "tycho_storage_rpc_deferred_lifecycle_duration_seconds",
                "result" => if result.is_ok() { "success" } else { "failure" },
            )
            .record(started_at.elapsed());
            result?;
            let snapshot =
                build_composite_snapshot(&mut partitions, &self.filter_registry, frontier)?;
            *published = Some(snapshot);
            frontier = *published.as_ref().unwrap().visible_frontier();
            self.sealing_notify.notify_one();
        }

        if let Some(token) = admission.as_mut() {
            ensure!(
                token.block_set == *block_set,
                "RPC block-set admission is already held for {}, cannot write {}",
                token.block_set,
                block_set
            );
            ensure!(
                frontier == token.frontier,
                "RPC effective frontier changed while a block set is admitted"
            );
            let persisted = partitions.visible_frontier().copied();
            match token.mode {
                BlockSetMode::New => match persisted {
                    Some(persisted) if persisted == token.frontier => {}
                    None if token.frontier.seqno == 0 => {}
                    Some(persisted) if persisted == *block_set => {
                        token.mode = BlockSetMode::Replay;
                    }
                    Some(_) | None => anyhow::bail!(
                        "RPC durable frontier changed while a new block set is admitted"
                    ),
                },
                BlockSetMode::Replay => match persisted {
                    Some(persisted) if persisted.seqno > block_set.seqno => {}
                    Some(persisted) if persisted == *block_set => {}
                    Some(_) | None => anyhow::bail!(
                        "RPC replay token has an invalid durable frontier"
                    ),
                },
                BlockSetMode::Same => ensure!(
                    persisted.is_some_and(|persisted| {
                        persisted.seqno > token.frontier.seqno || persisted == token.frontier
                    }),
                    "RPC same-boundary token has an invalid durable frontier"
                ),
            }
            return Ok(token.mode);
        }

        let persisted = partitions.visible_frontier().copied();
        let mode = if block_set == &frontier {
            ensure!(
                persisted.is_some_and(|persisted| {
                    persisted.seqno > frontier.seqno || persisted == frontier
                }),
                "RPC same-boundary retry has an invalid durable frontier"
            );
            if block_set.seqno != 0 {
                partitions.validate_masterchain_commit(block_set)?;
            }
            BlockSetMode::Same
        } else {
            ensure!(
                block_set.seqno
                    == frontier
                        .seqno
                        .checked_add(1)
                        .context("RPC visible frontier sequence number overflow")?,
                "RPC block-set sequence gap: admitted frontier is {}, incoming set is {}",
                frontier.seqno,
                block_set.seqno
            );
            match persisted {
                Some(persisted) if persisted.seqno == frontier.seqno => {
                    ensure!(
                        persisted == frontier,
                        "RPC persisted and effective frontiers have different full block ids at masterchain seqno {}",
                        frontier.seqno
                    );
                    BlockSetMode::New
                }
                Some(persisted) if persisted.seqno >= block_set.seqno => {
                    if persisted.seqno == block_set.seqno {
                        ensure!(
                            persisted == *block_set,
                            "RPC replay frontier has a different full block id at masterchain seqno {}",
                            block_set.seqno
                        );
                    }
                    BlockSetMode::Replay
                }
                Some(persisted) => anyhow::bail!(
                    "RPC persisted frontier {} cannot admit block set {} from effective frontier {}",
                    persisted.seqno,
                    block_set.seqno,
                    frontier.seqno
                ),
                None => {
                    ensure!(
                        frontier.seqno == 0,
                        "RPC persisted frontier is missing for effective frontier {frontier}"
                    );
                    BlockSetMode::New
                }
            }
        };
        if matches!(mode, BlockSetMode::New | BlockSetMode::Replay) && frontier.seqno != 0 {
            partitions.validate_masterchain_commit(&frontier)?;
        }
        *admission = Some(BlockSetAdmission {
            frontier,
            block_set: *block_set,
            mode,
        });
        Ok(mode)
    }

    fn require_snapshot(&self, snapshot: Option<&RpcSnapshot>) -> Result<RpcSnapshot> {
        snapshot
            .cloned()
            .or_else(|| self.load_snapshot())
            .context("No RPC snapshot available")
    }

    fn sealed_exact_lookup_context(
        &self,
        namespace: FilterNamespace,
    ) -> SealedExactLookupContext {
        SealedExactLookupContext::new(
            self.sealed_exact_lookup_semaphore.clone(),
            self.sealed_exact_lookup_metrics.clone(),
            namespace,
            #[cfg(test)]
            self.sealed_exact_lookup_acquisitions.clone(),
        )
    }

    fn transaction_partition(
        &self,
        hash: &HashBytes,
        snapshot: RpcSnapshot,
    ) -> Result<Option<LocatedTransaction>> {
        let mut exact_lookup = self.sealed_exact_lookup_context(FilterNamespace::Transactions);
        for descriptor in snapshot.0.descriptors.iter().rev() {
            if descriptor.first.block_id.is_none() || descriptor.first.mc_seqno > snapshot.visible_frontier().seqno {
                continue;
            }
            exact_lookup.consider_partition();
            let mut filtered_positive = false;
            if descriptor.lifecycle == codec::ManifestLifecycle::Sealed {
                if snapshot.filter_bundle(descriptor.id).is_some() {
                    let might_contain = snapshot.filter_might_contain(
                        descriptor.id,
                        FilterNamespace::Transactions,
                        hash.as_slice(),
                    )?;
                    exact_lookup.record_filter(might_contain);
                    if !might_contain {
                        continue;
                    }
                    filtered_positive = true;
                }
                exact_lookup.acquire()?;
                exact_lookup.record_exact_probe(
                    "sealed",
                    if filtered_positive { "filter_positive" } else { "unfiltered" },
                );
            } else {
                exact_lookup.record_exact_probe(
                    if descriptor.lifecycle == codec::ManifestLifecycle::Active {
                        "active"
                    } else {
                        "sealing"
                    },
                    "local_only",
                );
            }
            let partition = acquire_partition_read(&self.partitions, snapshot.clone(), descriptor.id)?;
            if let Some(value) = partition.get(&partition.lease.transactions_by_hash, hash)? {
                let info = TransactionInfo::from_bytes(&value).context("invalid local transaction hash locator")?;
                if info.mc_seqno <= snapshot.visible_frontier().seqno {
                    exact_lookup.record_hit(if descriptor.lifecycle == codec::ManifestLifecycle::Sealed {
                        "sealed"
                    } else if descriptor.lifecycle == codec::ManifestLifecycle::Active {
                        "active"
                    } else {
                        "sealing"
                    });
                    return Ok(Some(LocatedTransaction {
                        partition,
                        info,
                        key: value[..tables::Transactions::KEY_LEN].to_vec(),
                        sealed_exact_lookup_permit: exact_lookup.into_permit(),
                    }));
                }
                exact_lookup.record_rejection(
                    if descriptor.lifecycle == codec::ManifestLifecycle::Sealed {
                        "sealed"
                    } else if descriptor.lifecycle == codec::ManifestLifecycle::Active {
                        "active"
                    } else {
                        "sealing"
                    },
                    "future_record",
                );
            } else if filtered_positive {
                exact_lookup.record_false_positive();
            }
        }
        exact_lookup.record_miss();
        Ok(None)
    }

    fn inbound_message_partition(
        &self,
        hash: &HashBytes,
        snapshot: RpcSnapshot,
    ) -> Result<Option<LocatedInboundTransaction>> {
        let mut exact_lookup = self.sealed_exact_lookup_context(FilterNamespace::InboundMessages);
        for descriptor in snapshot.0.descriptors.iter().rev() {
            if descriptor.first.block_id.is_none() || descriptor.first.mc_seqno > snapshot.visible_frontier().seqno {
                continue;
            }
            exact_lookup.consider_partition();
            let mut filtered_positive = false;
            if descriptor.lifecycle == codec::ManifestLifecycle::Sealed {
                if snapshot.filter_bundle(descriptor.id).is_some() {
                    let might_contain = snapshot.filter_might_contain(
                        descriptor.id,
                        FilterNamespace::InboundMessages,
                        hash.as_slice(),
                    )?;
                    exact_lookup.record_filter(might_contain);
                    if !might_contain {
                        continue;
                    }
                    filtered_positive = true;
                }
                exact_lookup.acquire()?;
                exact_lookup.record_exact_probe(
                    "sealed",
                    if filtered_positive { "filter_positive" } else { "unfiltered" },
                );
            } else {
                exact_lookup.record_exact_probe(
                    if descriptor.lifecycle == codec::ManifestLifecycle::Active {
                        "active"
                    } else {
                        "sealing"
                    },
                    "local_only",
                );
            }
            let partition = acquire_partition_read(&self.partitions, snapshot.clone(), descriptor.id)?;
            if let Some(key) = partition.get(&partition.lease.transactions_by_in_msg, hash)? {
                anyhow::ensure!(key.len() == tables::Transactions::KEY_LEN, "invalid local inbound-message locator length");
                let tx = partition.get(&partition.lease.transactions, &key)?.context("inbound-message locator points to a missing local transaction")?;
                if TransactionData::related_mc_seqno(&tx)? <= snapshot.visible_frontier().seqno {
                    let transaction = TransactionData::from_owned(tx);
                    anyhow::ensure!(
                        transaction.in_msg_hash().as_ref() == Some(hash),
                        "inbound-message locator points to a transaction with a different message hash"
                    );
                    exact_lookup.record_hit(if descriptor.lifecycle == codec::ManifestLifecycle::Sealed {
                        "sealed"
                    } else if descriptor.lifecycle == codec::ManifestLifecycle::Active {
                        "active"
                    } else {
                        "sealing"
                    });
                    return Ok(Some(LocatedInboundTransaction {
                        partition,
                        transaction,
                        sealed_exact_lookup_permit: exact_lookup.into_permit(),
                    }));
                }
                exact_lookup.record_rejection(
                    if descriptor.lifecycle == codec::ManifestLifecycle::Sealed {
                        "sealed"
                    } else if descriptor.lifecycle == codec::ManifestLifecycle::Active {
                        "active"
                    } else {
                        "sealing"
                    },
                    "future_record",
                );
            } else if filtered_positive {
                exact_lookup.record_false_positive();
            }
        }
        exact_lookup.record_miss();
        Ok(None)
    }

    fn block_partition(
        &self,
        block_id: &BlockIdShort,
        snapshot: RpcSnapshot,
    ) -> Result<Option<LocatedBlock>> {
        let Ok(workchain) = i8::try_from(block_id.shard.workchain()) else {
            return Ok(None);
        };
        let mut key = [0; tables::KnownBlocks::KEY_LEN];
        key[0] = workchain as u8;
        key[1..9].copy_from_slice(&block_id.shard.prefix().to_be_bytes());
        key[9..13].copy_from_slice(&block_id.seqno.to_be_bytes());
        let is_masterchain = block_id.is_masterchain();
        let descriptors = if is_masterchain {
            let Some(descriptor) = snapshot.descriptor_for_mc_seqno(block_id.seqno) else {
                return Ok(None);
            };
            std::slice::from_ref(descriptor)
        } else {
            snapshot.0.descriptors.as_slice()
        };
        let mut exact_lookup = self.sealed_exact_lookup_context(FilterNamespace::Blocks);
        for descriptor in descriptors.iter().rev() {
            if descriptor.first.block_id.is_none() || descriptor.first.mc_seqno > snapshot.visible_frontier().seqno {
                continue;
            }
            exact_lookup.consider_partition();
            let mut filtered_positive = false;
            if descriptor.lifecycle == codec::ManifestLifecycle::Sealed {
                if !is_masterchain && snapshot.filter_bundle(descriptor.id).is_some() {
                    let might_contain = snapshot.filter_might_contain(
                        descriptor.id,
                        FilterNamespace::Blocks,
                        &key,
                    )?;
                    exact_lookup.record_filter(might_contain);
                    if !might_contain {
                        continue;
                    }
                    filtered_positive = true;
                }
                exact_lookup.acquire()?;
                exact_lookup.record_exact_probe(
                    "sealed",
                    if filtered_positive { "filter_positive" } else { "unfiltered" },
                );
            } else {
                exact_lookup.record_exact_probe(
                    if descriptor.lifecycle == codec::ManifestLifecycle::Active {
                        "active"
                    } else {
                        "sealing"
                    },
                    "local_only",
                );
            }
            let partition = acquire_partition_read(&self.partitions, snapshot.clone(), descriptor.id)?;
            #[cfg(test)]
            self.known_block_point_reads.fetch_add(1, Ordering::AcqRel);
            if let Some(value) = partition.get(&partition.lease.known_blocks, key)? {
                anyhow::ensure!(value.len() >= 68, "invalid known block value");
                let related_mc_seqno = u32::from_le_bytes(value[64..68].try_into().unwrap());
                if is_masterchain {
                    anyhow::ensure!(related_mc_seqno == block_id.seqno, "known masterchain block has a different related masterchain seqno");
                }
                if related_mc_seqno <= snapshot.visible_frontier().seqno {
                    exact_lookup.record_hit(if descriptor.lifecycle == codec::ManifestLifecycle::Sealed {
                        "sealed"
                    } else if descriptor.lifecycle == codec::ManifestLifecycle::Active {
                        "active"
                    } else {
                        "sealing"
                    });
                    return Ok(Some(LocatedBlock {
                        partition,
                        key,
                        value,
                        sealed_exact_lookup_permit: exact_lookup.into_permit(),
                    }));
                }
                exact_lookup.record_rejection(
                    if descriptor.lifecycle == codec::ManifestLifecycle::Sealed {
                        "sealed"
                    } else if descriptor.lifecycle == codec::ManifestLifecycle::Active {
                        "active"
                    } else {
                        "sealing"
                    },
                    "future_record",
                );
            } else if filtered_positive {
                exact_lookup.record_false_positive();
            }
        }
        exact_lookup.record_miss();
        Ok(None)
    }

    pub fn store_instance_id(&self, id: InstanceId) {
        let rpc_states = &self.current_state.state;
        rpc_states.insert(INSTANCE_ID, id).unwrap();
    }

    pub fn load_instance_id(&self) -> InstanceId {
        let id = self.current_state.state.get(INSTANCE_ID).unwrap().unwrap();
        InstanceId::from_slice(id.as_ref())
    }

    pub fn get_known_mc_blocks_range(
        &self,
        snapshot: Option<&RpcSnapshot>,
    ) -> Result<Option<(u32, u32)>> {
        let snapshot = self.require_snapshot(snapshot)?;
        let mut range: Option<(u32, u32)> = None;
        for descriptor in &snapshot.0.descriptors {
            if descriptor.first.block_id.is_none()
                || descriptor.first.mc_seqno > snapshot.visible_frontier().seqno
            {
                continue;
            }
            let from = descriptor.first.mc_seqno;
            let to = descriptor
                .last
                .mc_seqno
                .min(snapshot.visible_frontier().seqno);
            range = Some(match range {
                Some((range_from, range_to)) => {
                    (range_from.min(from), range_to.max(to))
                }
                None => (from, to),
            });
        }
        Ok(range)
    }

    pub fn get_blocks_by_mc_seqno(
        &self,
        mc_seqno: u32,
        snapshot: Option<RpcSnapshot>,
    ) -> Result<Option<BlocksByMcSeqnoIter>> {
        let mut key = [0; tables::KnownBlocks::KEY_LEN];
        key[0] = -1i8 as u8;
        key[1..9].copy_from_slice(&ShardIdent::PREFIX_FULL.to_be_bytes());
        key[9..13].copy_from_slice(&mc_seqno.to_be_bytes());

        let snapshot = self.require_snapshot(snapshot.as_ref())?;
        if mc_seqno > snapshot.visible_frontier().seqno {
            return Ok(None);
        }
        let Some(partition_id) = snapshot
            .descriptor_for_mc_seqno(mc_seqno)
            .map(|descriptor| descriptor.id)
        else {
            return Ok(None);
        };
        let partition =
            acquire_partition_read(&self.partitions, snapshot, partition_id)?;
        let table = &partition.lease.known_blocks;
        let Some(value) = partition.get(table, key)? else {
            return Ok(None);
        };
        anyhow::ensure!(value.len() >= 68, "invalid known masterchain block value");
        let related_mc_seqno = u32::from_le_bytes(value[64..68].try_into().unwrap());
        anyhow::ensure!(
            related_mc_seqno == mc_seqno,
            "known masterchain block has a different related masterchain seqno"
        );

        let mut range_from = [0x00; tables::BlocksByMcSeqno::KEY_LEN];
        range_from[0..4].clone_from_slice(&mc_seqno.to_be_bytes());
        let mut range_to = [0xff; tables::BlocksByMcSeqno::KEY_LEN];
        range_to[0..4].clone_from_slice(&mc_seqno.to_be_bytes());

        let table = &partition.lease.blocks_by_mc_seqno;
        let mut readopts = partition.read_options(table)?;
        readopts.set_iterate_lower_bound(range_from.as_slice());
        readopts.set_iterate_upper_bound(range_to.as_slice());

        let rocksdb = partition.lease.rocksdb();
        let mut iter = rocksdb.raw_iterator_cf_opt(&table.cf(), readopts);
        iter.seek(range_from.as_slice());

        Ok(Some(BlocksByMcSeqnoIter {
            mc_seqno,
            // SAFETY: Iterator was created from the same DB instance.
            inner: unsafe { weedb::OwnedRawIterator::new(rocksdb.clone(), iter) },
            partition,
        }))
    }

    pub fn get_brief_block_info(
        &self,
        block_id: &BlockIdShort,
        snapshot: Option<&RpcSnapshot>,
    ) -> Result<Option<(BlockId, u32, BriefBlockInfo)>> {
        let Ok(workchain) = i8::try_from(block_id.shard.workchain()) else {
            return Ok(None);
        };
        let snapshot = self.require_snapshot(snapshot)?;
        let Some(LocatedBlock {
            value,
            sealed_exact_lookup_permit,
            ..
        }) =
            self.block_partition(block_id, snapshot.clone())?
        else {
            return Ok(None);
        };
        let mc_seqno = u32::from_le_bytes(value[64..68].try_into().unwrap());

        let brief_info = BriefBlockInfo::load_from_bytes(workchain as i32, &value[68..])
            .context("invalid brief info")?;

        let block_id = BlockId {
            shard: block_id.shard,
            seqno: block_id.seqno,
            root_hash: HashBytes::from_slice(&value[0..32]),
            file_hash: HashBytes::from_slice(&value[32..64]),
        };

        let result = (block_id, mc_seqno, brief_info);
        drop(sealed_exact_lookup_permit);
        Ok(Some(result))
    }

    pub fn get_brief_shards_descr(
        &self,
        mc_seqno: u32,
        snapshot: Option<&RpcSnapshot>,
    ) -> Result<Option<Vec<BriefShardDescr>>> {
        let snapshot = self.require_snapshot(snapshot)?;
        if mc_seqno > snapshot.visible_frontier().seqno {
            return Ok(None);
        }
        let Some(partition_id) = snapshot
            .descriptor_for_mc_seqno(mc_seqno)
            .map(|descriptor| descriptor.id)
        else {
            return Ok(None);
        };
        let partition =
            acquire_partition_read(&self.partitions, snapshot, partition_id)?;
        let mut key = [0x00; tables::BlocksByMcSeqno::KEY_LEN];
        key[0..4].copy_from_slice(&mc_seqno.to_be_bytes());
        key[4] = -1i8 as u8;
        key[5..13].copy_from_slice(&ShardIdent::PREFIX_FULL.to_be_bytes());
        key[13..17].copy_from_slice(&mc_seqno.to_be_bytes());

        let table = &partition.lease.blocks_by_mc_seqno;
        let Some(value) = partition.get(table, key)? else {
            return Ok(None);
        };
        anyhow::ensure!(
            value.len() >= tables::BlocksByMcSeqno::DESCR_OFFSET,
            "invalid masterchain block description value"
        );

        let shard_count = u32::from_le_bytes(
            value[tables::BlocksByMcSeqno::VALUE_LEN..tables::BlocksByMcSeqno::VALUE_LEN + 4]
                .try_into()
                .unwrap(),
        ) as usize;
        let expected_len = tables::BlocksByMcSeqno::DESCR_OFFSET
            .checked_add(
                shard_count
                    .checked_mul(tables::BlocksByMcSeqno::DESCR_LEN)
                    .context("masterchain shard description length overflow")?,
            )
            .context("masterchain shard description length overflow")?;
        anyhow::ensure!(
            value.len() >= expected_len,
            "invalid masterchain shard description value length"
        );

        let mut result = Vec::with_capacity(shard_count);
        for i in 0..shard_count {
            let offset =
                tables::BlocksByMcSeqno::DESCR_OFFSET + i * tables::BlocksByMcSeqno::DESCR_LEN;
            let descr = &value[offset..offset + tables::BlocksByMcSeqno::DESCR_LEN];

            result.push(BriefShardDescr {
                shard_ident: ShardIdent::new(
                    descr[0] as i8 as i32,
                    u64::from_le_bytes(descr[1..9].try_into().unwrap()),
                )
                .context("invalid top shard ident")?,
                seqno: u32::from_le_bytes(descr[9..13].try_into().unwrap()),
                root_hash: HashBytes::from_slice(&descr[13..45]),
                file_hash: HashBytes::from_slice(&descr[45..77]),
                start_lt: u64::from_le_bytes(descr[77..85].try_into().unwrap()),
                end_lt: u64::from_le_bytes(descr[85..93].try_into().unwrap()),
            });
        }

        Ok(Some(result))
    }

    pub fn get_accounts_by_code_hash(
        &self,
        code_hash: &HashBytes,
        continuation: Option<&StdAddr>,
        snapshot: Option<RpcSnapshot>,
    ) -> Result<CodeHashesIter<'_>> {
        let mut key = [0u8; tables::CodeHashes::KEY_LEN];
        key[0..32].copy_from_slice(code_hash.as_ref());
        if let Some(continuation) = continuation {
            key[32] = continuation.workchain as u8;
            key[33..65].copy_from_slice(continuation.address.as_ref());
        }

        let mut upper_bound = Vec::with_capacity(tables::CodeHashes::KEY_LEN);
        upper_bound.extend_from_slice(&key[..32]);
        upper_bound.extend_from_slice(&[0xff; 33]);

        let mut readopts = self.current_state.code_hashes.new_read_config();
        // TODO: somehow make the range inclusive since
        // upper_bound is not included in the range
        readopts.set_iterate_upper_bound(upper_bound);

        let snapshot = self.require_snapshot(snapshot.as_ref())?;
        readopts.set_snapshot(snapshot.current_state());

        let rocksdb = self.current_state.rocksdb();
        let code_hashes_cf = self.current_state.code_hashes.cf();
        let mut iter = rocksdb.raw_iterator_cf_opt(&code_hashes_cf, readopts);

        iter.seek(key);
        if continuation.is_some() {
            iter.next();
        }

        Ok(CodeHashesIter {
            inner: iter,
            snapshot,
        })
    }

    pub fn get_block_transactions(
        &self,
        block_id: &BlockIdShort,
        reverse: bool,
        cursor: Option<&BlockTransactionsCursor>,
        snapshot: Option<RpcSnapshot>,
    ) -> Result<Option<BlockTransactionsIterBuilder>> {
        let snapshot = self.require_snapshot(snapshot.as_ref())?;
        let Some(ids) =
            self.get_block_transaction_ids(block_id, reverse, cursor, Some(snapshot))?
        else {
            return Ok(None);
        };

        Ok(Some(BlockTransactionsIterBuilder { ids }))
    }

    pub fn get_block_transaction_ids(
        &self,
        block_id: &BlockIdShort,
        reverse: bool,
        cursor: Option<&BlockTransactionsCursor>,
        snapshot: Option<RpcSnapshot>,
    ) -> Result<Option<BlockTransactionIdsIter>> {
        let snapshot = self.require_snapshot(snapshot.as_ref())?;
        let Some(LocatedBlock {
            partition,
            key,
            value,
            sealed_exact_lookup_permit,
        }) =
            self.block_partition(block_id, snapshot.clone())?
        else {
            return Ok(None);
        };

        let mut range_from = [0x00; tables::BlockTransactions::KEY_LEN];
        range_from[0..13].copy_from_slice(&key);

        if let Some(cursor) = cursor {
            range_from[13..45].copy_from_slice(cursor.hash.as_slice());
            range_from[45..53].copy_from_slice(&cursor.lt.to_be_bytes());
        }

        let ref_by_mc_seqno = u32::from_le_bytes(value[64..68].try_into().unwrap());
        let block_id = BlockId {
            shard: block_id.shard,
            seqno: block_id.seqno,
            root_hash: HashBytes::from_slice(&value[0..32]),
            file_hash: HashBytes::from_slice(&value[32..64]),
        };

        let mut range_to = [0xff; tables::BlockTransactions::KEY_LEN];
        range_to[0..13].copy_from_slice(&range_from[0..13]);

        let mut readopts = partition.read_options(&partition.lease.block_transactions)?;
        readopts.set_iterate_lower_bound(range_from.as_slice());
        readopts.set_iterate_upper_bound(range_to.as_slice());

        let rocksdb = partition.lease.rocksdb();
        let block_transactions_cf = partition.lease.block_transactions.cf();
        let mut iter = rocksdb.raw_iterator_cf_opt(&block_transactions_cf, readopts);

        if reverse {
            iter.seek_for_prev(range_to);
        } else {
            iter.seek(range_from);
        }

        if cursor.is_some()
            && let Some(key) = iter.key()
            && key == range_from.as_slice()
        {
            if reverse {
                iter.prev();
            } else {
                iter.next();
            }
        }

        Ok(Some(BlockTransactionIdsIter {
            block_id,
            ref_by_mc_seqno,
            is_reversed: reverse,
            // SAFETY: Iterator was created from the same DB instance.
            inner: unsafe { weedb::OwnedRawIterator::new(rocksdb.clone(), iter) },
            partition,
            _sealed_exact_lookup_permit: sealed_exact_lookup_permit,
        }))
    }

    pub fn get_transactions(
        &self,
        account: &StdAddr,
        start_lt: Option<u64>,
        end_lt: Option<u64>,
        reverse: bool,
        snapshot: Option<RpcSnapshot>,
    ) -> Result<TransactionsIterBuilder> {
        let mut start_lt = start_lt.unwrap_or_default();
        let mut end_lt = end_lt.unwrap_or(u64::MAX);
        if end_lt < start_lt {
            // Make empty iterator if `end_lt < start_lt`.
            start_lt = u64::MAX - 1;
            end_lt = u64::MAX;
        }

        let snapshot = self.require_snapshot(snapshot.as_ref())?;

        let mut range_from = [0u8; tables::Transactions::KEY_LEN];
        range_from[0] = account.workchain as u8;
        range_from[1..33].copy_from_slice(account.address.as_ref());
        range_from[33..41].copy_from_slice(&start_lt.to_be_bytes());
        let mut range_to = range_from;
        // NOTE: Compute upper bound as `end_lt + 1` since it will
        // not be included in the iteration result.
        range_to[33..41].copy_from_slice(&end_lt.saturating_add(1).to_be_bytes());

        let mut partition_ids = snapshot
            .0
            .descriptors
            .iter()
            .filter(|descriptor| {
                descriptor.first.block_id.is_some()
                    && descriptor.first.mc_seqno <= snapshot.visible_frontier().seqno
                    && descriptor.last.transaction_lt >= start_lt
                    && descriptor.first.transaction_lt <= end_lt
            })
            .map(|descriptor| descriptor.id)
            .collect::<Vec<_>>();
        if reverse {
            partition_ids.reverse();
        }

        Ok(TransactionsIterBuilder {
            is_reversed: reverse,
            visible_frontier_seqno: snapshot.visible_frontier().seqno,
            partitions: self.partitions.clone(),
            partition_ids,
            range_from,
            range_to,
            snapshot,
        })
    }

    fn get_routed_transaction<R, F>(
        &self,
        hash: &HashBytes,
        snapshot: RpcSnapshot,
        map: F,
    ) -> Result<Option<R>>
    where
        F: FnOnce(TransactionInfo, &[u8]) -> R,
    {
        let Some(LocatedTransaction {
            partition,
            info,
            key,
            sealed_exact_lookup_permit,
        }) =
            self.transaction_partition(hash, snapshot.clone())?
        else {
            return Ok(None);
        };
        let tx = partition
            .get_pinned(
                &partition.lease.transactions,
                &key,
            )?
            .context("transaction hash locator points to a missing local transaction")?;
        let transaction_mc_seqno = TransactionData::related_mc_seqno(tx.as_ref())?;
        anyhow::ensure!(
            transaction_mc_seqno == info.mc_seqno,
            "transaction hash locator and local transaction have different related masterchain seqnos"
        );
        anyhow::ensure!(
            TransactionData::read_tx_hash(tx.as_ref()) == *hash,
            "transaction hash locator points to a transaction with a different hash"
        );
        let result = map(info, tx.as_ref());
        drop(tx);
        drop(sealed_exact_lookup_permit);
        Ok(Some(result))
    }

    pub fn get_transaction(
        &self,
        hash: &HashBytes,
        snapshot: Option<&RpcSnapshot>,
    ) -> Result<Option<TransactionData<'_>>> {
        let snapshot = self.require_snapshot(snapshot)?;
        Ok(self
            .get_routed_transaction(hash, snapshot, |_, tx| {
                TransactionData::from_owned(tx.to_vec())
            })?)
    }

    pub fn get_transaction_ext<'db>(
        &'db self,
        hash: &HashBytes,
        snapshot: Option<&RpcSnapshot>,
    ) -> Result<Option<TransactionDataExt<'db>>> {
        let snapshot = self.require_snapshot(snapshot)?;
        Ok(self
            .get_routed_transaction(hash, snapshot, |info, tx| TransactionDataExt {
                info,
                data: TransactionData::from_owned(tx.to_vec()),
            })?)
    }

    pub fn get_transaction_info(
        &self,
        hash: &HashBytes,
        snapshot: Option<&RpcSnapshot>,
    ) -> Result<Option<TransactionInfo>> {
        let snapshot = self.require_snapshot(snapshot)?;
        self.get_routed_transaction(hash, snapshot, |info, _| info)
    }

    pub fn get_src_transaction<'db>(
        &'db self,
        account: &StdAddr,
        message_lt: u64,
        snapshot: Option<&RpcSnapshot>,
    ) -> Result<Option<TransactionData<'db>>> {
        let snapshot = self.require_snapshot(snapshot)?;
        let mut key = [0u8; tables::Transactions::KEY_LEN];
        key[0] = account.workchain as u8;
        key[1..33].copy_from_slice(account.address.as_slice());

        let lower_bound = key;
        key[33..41].copy_from_slice(&message_lt.to_be_bytes());

        let candidates = snapshot
            .0
            .descriptors
            .iter()
            .rev()
            .filter(|descriptor| {
                descriptor.first.block_id.is_some()
                    && descriptor.first.mc_seqno <= snapshot.visible_frontier().seqno
                    && descriptor.first.transaction_lt < message_lt
            })
            .map(|descriptor| descriptor.id)
            .collect::<Vec<_>>();
        for id in candidates {
            let partition =
                acquire_partition_read(&self.partitions, snapshot.clone(), id)?;
            let table = &partition.lease.transactions;
            let mut readopts = partition.read_options(table)?;
            readopts.set_iterate_lower_bound(lower_bound);
            readopts.set_iterate_upper_bound(key);
            let mut iter = partition
                .lease
                .rocksdb()
                .raw_iterator_cf_opt(&table.cf(), readopts);
            iter.seek_for_prev(key.as_slice());

            while let Some((tx_key, value)) = iter.item() {
                if tx_key[0..33] != key[0..33] {
                    break;
                }
                if TransactionData::related_mc_seqno(value)?
                    <= snapshot.visible_frontier().seqno
                {
                    return Ok(Some(TransactionData::from_owned(value.to_vec())));
                }
                iter.prev();
            }
            iter.status()?;
        }
        Ok(None)
    }

    pub fn get_dst_transaction<'db>(
        &'db self,
        in_msg_hash: &HashBytes,
        snapshot: Option<&RpcSnapshot>,
    ) -> Result<Option<TransactionData<'db>>> {
        let snapshot = self.require_snapshot(snapshot)?;
        let Some(LocatedInboundTransaction {
            partition,
            transaction,
            sealed_exact_lookup_permit,
        }) =
            self.inbound_message_partition(in_msg_hash, snapshot.clone())?
        else {
            return Ok(None);
        };
        let _partition = partition;
        drop(sealed_exact_lookup_permit);
        Ok(Some(transaction))
    }

    #[tracing::instrument(
        level = "info",
        name = "reset_accounts",
        skip_all,
        fields(shard = %shard_state.block_id().shard)
    )]
    pub async fn reset_accounts(
        &self,
        shard_state: ShardStateStuff,
        split_depth: u8,
    ) -> Result<()> {
        let shard_ident = shard_state.block_id().shard;
        let Ok(workchain) = i8::try_from(shard_ident.workchain()) else {
            return Ok(());
        };

        tracing::info!("clearing old code hash indices");
        let started_at = Instant::now();
        self.remove_code_hashes(&shard_ident).await?;
        tracing::info!(
            elapsed = %humantime::format_duration(started_at.elapsed()),
            "cleared old code hash indices"
        );

        // Split on virtual shards
        let split = {
            let guard = shard_state.ref_mc_state_handle().clone();

            let mut virtual_shards = FastHashMap::default();
            split_shard(
                &shard_ident,
                shard_state.state().load_accounts()?.dict(),
                split_depth,
                &mut virtual_shards,
            )
            .context("failed to split shard state into virtual shards")?;

            // NOTE: Ensure that the root cell is dropped.
            drop(shard_state);
            (guard, virtual_shards)
        };

        let cancelled = CancellationFlag::new();
        scopeguard::defer! {
            cancelled.cancel();
        }

        // Rebuild code hashes
        let db = self.current_state.clone();
        let mut cancelled = cancelled.debounce(10000);
        let span = tracing::Span::current();

        // NOTE: `spawn_blocking` is used here instead of `rayon_run` as it is IO-bound task.
        tokio::task::spawn_blocking(move || {
            let _span = span.enter();

            // NOTE: Ensure that guard is captured by the spawned thread.
            let (_state_guard, virtual_shards) = split;

            let guard = scopeguard::guard((), |_| {
                tracing::warn!("cancelled");
            });

            tracing::info!(split_depth, "started building new code hash indices");
            let started_at = Instant::now();

            let raw = db.rocksdb().as_ref();
            let code_hashes_cf = &db.code_hashes.cf();
            let code_hashes_by_address_cf = &db.code_hashes_by_address.cf();

            let mut non_empty_batch = false;
            let mut write_batch = rocksdb::WriteBatch::default();

            // Prepare buffer for code hashes ids
            let mut code_hashes_key = [0u8; { tables::CodeHashes::KEY_LEN }];
            code_hashes_key[32] = workchain as u8;

            let mut code_hashes_by_address_key = [0u8; { tables::CodeHashesByAddress::KEY_LEN }];
            code_hashes_by_address_key[0] = workchain as u8;

            // Iterate all accounts
            for (virtual_shard, accounts) in virtual_shards {
                tracing::info!(%virtual_shard, "started collecting code hashes");
                let started_at = Instant::now();

                for entry in accounts.iter() {
                    if cancelled.check() {
                        anyhow::bail!("accounts reset cancelled");
                    }

                    let (id, (_, account)) = entry?;

                    let code_hash = match extract_code_hash(&account)? {
                        ExtractedCodeHash::Exact(Some(code_hash)) => code_hash,
                        ExtractedCodeHash::Exact(None) => continue,
                        ExtractedCodeHash::Skip => anyhow::bail!("code in account state is pruned"),
                    };

                    non_empty_batch |= true;

                    // Fill account address in the key buffer
                    code_hashes_key[..32].copy_from_slice(code_hash.as_slice());
                    code_hashes_key[33..65].copy_from_slice(id.as_slice());

                    code_hashes_by_address_key[1..33].copy_from_slice(id.as_slice());

                    // Write tx data and indices
                    write_batch.put_cf(code_hashes_cf, code_hashes_key.as_slice(), []);
                    write_batch.put_cf(
                        code_hashes_by_address_cf,
                        code_hashes_by_address_key.as_slice(),
                        code_hash.as_slice(),
                    );
                }

                tracing::info!(
                    %virtual_shard,
                    elapsed = %humantime::format_duration(started_at.elapsed()),
                    "finished collecting code hashes",
                );
            }

            if non_empty_batch {
                raw.write_opt(write_batch, db.code_hashes.write_config())?;
            }

            tracing::info!(
                elapsed = %humantime::format_duration(started_at.elapsed()),
                "finished building new code hash indices"
            );

            // Flush indices after delete/insert
            tracing::info!("started flushing code hash indices");
            let started_at = Instant::now();

            let bound = Option::<[u8; 0]>::None;
            raw.compact_range_cf(code_hashes_cf, bound, bound);
            raw.compact_range_cf(code_hashes_by_address_cf, bound, bound);

            // Done
            scopeguard::ScopeGuard::into_inner(guard);
            tracing::info!(
                elapsed = %humantime::format_duration(started_at.elapsed()),
                "finished flushing code hash indices"
            );
            Ok(())
        })
        .await?
    }

    #[tracing::instrument(level = "info", name = "update", skip_all, fields(block_id = %block.id()))]
    pub async fn update(
        &self,
        mc_block_id: &BlockId,
        block: BlockStuff,
        rpc_blacklist: Option<&BlacklistedAccounts>,
        subscriptions: &super::subscriptions::RpcSubscriptions,
    ) -> Result<BlockWriteResult> {
        let Ok(workchain) = i8::try_from(block.id().shard.workchain()) else {
            return Ok(BlockWriteResult { partition_id: 0, stats: Default::default(), newly_committed: false });
        };

        let is_masterchain = block.id().is_masterchain();
        let mc_seqno = mc_block_id.seqno;

        let admission_mode = self.admit_block_set(mc_block_id)?;

        let shard_hashes = is_masterchain
            .then(|| {
                let custom = block.load_custom()?;
                Ok::<_, anyhow::Error>(custom.shards.clone())
            })
            .transpose()?;

        let span = tracing::Span::current();
        let (partition_id, mut partition_lease) = self.partitions.lock().lease_for_mc_seqno(mc_seqno)?;
        let commit_key = codec::partition_commit_key(mc_seqno, &block.id().as_short_id());
        let existing_commit = partition_lease.partition_commits.get(commit_key)?
            .map(|value| codec::decode_partition_commit(value.as_ref()))
            .transpose()?;
        if let Some(commit) = existing_commit {
            // replay validates the local batch, then repeats the current-state stage
            anyhow::ensure!(commit.block_id == *block.id() && commit.digest == block.id().root_hash, "partition commit marker identity mismatch for {}", block.id());
        } else if admission_mode != BlockSetMode::New {
            anyhow::bail!(
                "{} block-set replay is missing commit marker for {}",
                match admission_mode {
                    BlockSetMode::Replay => "RPC",
                    BlockSetMode::Same => "same-boundary",
                    BlockSetMode::New => unreachable!(),
                },
                block.id()
            );
        } else if partition_lease.lifecycle() == codec::ManifestLifecycle::Sealed {
            anyhow::bail!("sealed transaction partition {} is missing commit marker for {}", partition_id.0, block.id());
        } else {
            let (write_partition_id, write_lease) =
                self.partitions.lock().write_lease_for_mc_seqno(mc_seqno)?;
            anyhow::ensure!(
                write_partition_id == partition_id,
                "transaction partition selection changed while preparing {}",
                block.id()
            );
            partition_lease = write_lease;
        }
        let db = partition_lease.db().clone();
        let newly_committed = existing_commit.is_none();
        let current_state = self.current_state.clone();

        let rpc_blacklist = rpc_blacklist.map(|x| x.load());

        // NOTE: `spawn_blocking` is used here instead of `rayon_run` as it is IO-bound task.
        let (start_lt, updates, computed_stats) = tokio::task::spawn_blocking(move || {
            let _partition_lease = partition_lease;
            let prepare_batch_histogram =
                HistogramGuard::begin("tycho_storage_rpc_prepare_batch_time");

            let _span = span.enter();

            let info = block.load_info()?;
            let extra = block.load_extra()?;

            let mut updates = Some(FastHashMap::default());

            let account_blocks = extra.account_blocks.load()?;

            let accounts = if account_blocks.is_empty() {
                Dict::new()
            } else {
                let merkle_update = block.as_ref().state_update.load()?;

                // Accounts dict is stored in the second cell.
                let get_accounts = |cell: Cell| {
                    let mut cs = cell.as_slice()?;
                    cs.skip_first(0, 1)?;
                    cs.load_reference_cloned().map(Cell::virtualize)
                };

                let old_accounts = get_accounts(merkle_update.old)?;
                let new_accounts = get_accounts(merkle_update.new)?;

                if old_accounts.repr_hash() == new_accounts.repr_hash() {
                    Dict::new()
                } else {
                    let accounts = Lazy::<ShardAccounts>::from_raw(new_accounts)?.load()?;
                    let (accounts, _) = accounts.into_parts();
                    accounts
                }
            };

            let mut write_batch = rocksdb::WriteBatch::default();
            let mut stats = BlockWriteStats::default();
            let mut current_state_batch = rocksdb::WriteBatch::default();
            let tx_cf = &db.transactions.cf();
            let tx_by_hash_cf = &db.transactions_by_hash.cf();
            let tx_by_in_msg_cf = &db.transactions_by_in_msg.cf();
            let block_txs_cf = &db.block_transactions.cf();

            // Prepare buffer for full tx id
            let mut tx_info = [0u8; tables::TransactionsByHash::VALUE_FULL_LEN];
            tx_info[0] = workchain as u8;

            let block_id = block.id();
            tx_info[41] = block_id.shard.prefix_len() as u8;
            tx_info[42..46].copy_from_slice(&block_id.seqno.to_le_bytes());
            tx_info[46..78].copy_from_slice(block_id.root_hash.as_slice());
            tx_info[78..110].copy_from_slice(block_id.file_hash.as_slice());
            tx_info[110..114].copy_from_slice(&mc_seqno.to_le_bytes());

            let mut block_tx = [0u8; tables::BlockTransactions::KEY_LEN];
            block_tx[0] = workchain as u8;
            block_tx[1..9].copy_from_slice(&block_id.shard.prefix().to_be_bytes());
            block_tx[9..13].copy_from_slice(&block_id.seqno.to_be_bytes());

            // Prepare buffer.
            let mut buffer = Vec::with_capacity(64 + BriefBlockInfo::MIN_BYTE_LEN);

            // Write block info.
            {
                let mut key = [0u8; tables::BlocksByMcSeqno::KEY_LEN];
                key[0..4].copy_from_slice(&mc_seqno.to_be_bytes());
                key[4] = workchain as u8;
                key[5..13].copy_from_slice(&block_id.shard.prefix().to_be_bytes());
                key[13..17].copy_from_slice(&block_id.seqno.to_be_bytes());

                buffer.clear();
                buffer.extend_from_slice(block_id.root_hash.as_slice()); // 0..32
                buffer.extend_from_slice(block_id.file_hash.as_slice()); // 32..64
                buffer.extend_from_slice(&info.start_lt.to_le_bytes()); // 64..72
                buffer.extend_from_slice(&info.end_lt.to_le_bytes()); // 72..80
                if let Some(shard_hashes) = shard_hashes {
                    let shards = shard_hashes
                        .iter()
                        .filter_map(|item| {
                            let (shard_ident, descr) = match item {
                                Ok(item) => item,
                                Err(e) => return Some(Err(e)),
                            };
                            if i8::try_from(shard_ident.workchain()).is_err() {
                                return None;
                            }

                            Some(Ok(BriefShardDescr {
                                shard_ident,
                                seqno: descr.seqno,
                                root_hash: descr.root_hash,
                                file_hash: descr.file_hash,
                                start_lt: descr.start_lt,
                                end_lt: descr.end_lt,
                            }))
                        })
                        .collect::<Result<Vec<_>, _>>()?;

                    buffer.reserve(4 + tables::BlocksByMcSeqno::DESCR_LEN * shards.len());
                    buffer.extend_from_slice(&(shards.len() as u32).to_le_bytes());
                    for shard in shards {
                        buffer.push(shard.shard_ident.workchain() as i8 as u8);
                        buffer.extend_from_slice(&shard.shard_ident.prefix().to_le_bytes());
                        buffer.extend_from_slice(&shard.seqno.to_le_bytes());
                        buffer.extend_from_slice(shard.root_hash.as_slice());
                        buffer.extend_from_slice(shard.file_hash.as_slice());
                        buffer.extend_from_slice(&shard.start_lt.to_le_bytes());
                        buffer.extend_from_slice(&shard.end_lt.to_le_bytes());
                    }
                }

                write_batch.put_cf(&db.blocks_by_mc_seqno.cf(), key, buffer.as_slice());
            }

            let rpc_blacklist = rpc_blacklist.as_deref();

            // Iterate through all changed accounts in the block.
            let mut block_tx_count = 0usize;
            for item in account_blocks.iter() {
                let (account, _, account_block) = item?;

                // Fill account address in the key buffer
                tx_info[1..33].copy_from_slice(account.as_slice());
                block_tx[13..45].copy_from_slice(account.as_slice());

                // Flag to update code hash
                let mut has_special_actions = false;
                let mut was_active = false;
                let mut is_active = false;

                // Process account transactions
                let mut first_tx = true;
                for item in account_block.transactions.values() {
                    let (_, tx_cell) = item?;

                    // TODO: Should we increase this counter only for non-blacklisted accounts?
                    block_tx_count += 1;

                    let tx = tx_cell.load()?;

                    tx_info[33..41].copy_from_slice(&tx.lt.to_be_bytes());
                    block_tx[45..53].copy_from_slice(&tx.lt.to_be_bytes());

                    // Update flags
                    if first_tx {
                        // Remember the original status from the first transaction
                        was_active = tx.orig_status == AccountStatus::Active;
                        first_tx = false;
                    }
                    if was_active && tx.orig_status != AccountStatus::Active {
                        // Handle the case when an account (with some updated code) was deleted,
                        // and then deployed with the initial code (end status).
                        // Treat this situation as a special action.
                        has_special_actions = true;
                    }
                    is_active = tx.end_status == AccountStatus::Active;

                    if !has_special_actions {
                        // Search for special actions (might be code hash update)
                        let info = tx.load_info()?;
                        let action_phase = match &info {
                            TxInfo::Ordinary(info) => info.action_phase.as_ref(),
                            TxInfo::TickTock(info) => info.action_phase.as_ref(),
                        };
                        if let Some(action_phase) = action_phase {
                            has_special_actions |= action_phase.special_actions > 0;
                        }
                    }

                    // Don't write tx for account from blacklist
                    if let Some(blacklist) = &rpc_blacklist
                        && blacklist.contains(&tx_info[..33])
                    {
                        continue;
                    }

                    if let Some(ref mut map) = updates {
                        let entry = map.entry(account).or_insert(tx.lt);
                        if tx.lt > *entry {
                            *entry = tx.lt;
                        }
                    }

                    let tx_hash = tx_cell.inner().repr_hash();
                    let (tx_mask, msg_hash) = match &tx.in_msg {
                        Some(in_msg) => {
                            let hash = Some(in_msg.repr_hash());
                            let mask = TransactionMask::HAS_MSG_HASH;
                            (mask, hash)
                        }
                        None => (TransactionMask::empty(), None),
                    };

                    // Collect transaction payload without the D3 related-masterchain prefix.
                    buffer.clear();
                    buffer.push(tx_mask.bits());
                    buffer.extend_from_slice(tx_hash.as_slice());
                    if let Some(msg_hash) = msg_hash {
                        buffer.extend_from_slice(msg_hash.as_slice());
                    }
                    tycho_types::boc::ser::BocHeader::<ahash::RandomState>::with_root(
                        tx_cell.inner().as_ref(),
                    )
                    .encode(&mut buffer);
                    let tx_value = codec::encode_transaction_value(mc_seqno, &buffer)?;
                    stats.add_transaction(tx_value.len(), msg_hash.is_some())?;

                    // Write tx data and indices
                    write_batch.put_cf(tx_by_hash_cf, tx_hash.as_slice(), tx_info.as_slice());
                    write_batch.put_cf(block_txs_cf, block_tx.as_slice(), tx_hash.as_slice());
                    if let Some(msg_hash) = msg_hash {
                        write_batch.put_cf(
                            tx_by_in_msg_cf,
                            msg_hash,
                            &tx_info[..tables::Transactions::KEY_LEN],
                        );
                    }

                    write_batch.put_cf(tx_cf, &tx_info[..tables::Transactions::KEY_LEN], &tx_value);
                }

                // Update code hash
                let update = if is_active && (!was_active || has_special_actions) {
                    // Account is active after this block and this is either a new account,
                    // or it was an existing account which possibly changed its code.
                    // Update: just store the code hash.
                    Some(false)
                } else if was_active && !is_active {
                    // Account was active before this block and is not active after the block.
                    // Update: remove the code hash.
                    Some(true)
                } else {
                    // No update for other cases
                    None
                };

                // Apply the update if any
                if let Some(remove) = update {
                    Self::update_code_hash(
                        &current_state,
                        workchain,
                        &account,
                        &accounts,
                        remove,
                        &mut current_state_batch,
                    )?;
                }
            }

            // Write block info.
            let brief_block_info =
                BriefBlockInfo::new(block.as_ref(), info, extra, block_tx_count)?;
            buffer.clear();
            buffer.extend_from_slice(&tx_info[46..114]); // root_hash + file_hash + mc_seqno
            brief_block_info.write_to_bytes(&mut buffer); // everything else

            write_batch.put_cf(
                &db.known_blocks.cf(),
                &block_tx[0..tables::KnownBlocks::KEY_LEN],
                buffer.as_slice(),
            );

            drop(prepare_batch_histogram);

            let _execute_batch_histogram =
                HistogramGuard::begin("tycho_storage_rpc_execute_batch_time");

            if let Some(commit) = existing_commit {
                anyhow::ensure!(
                    commit.transaction_count == stats.transaction_count
                        && commit.index_record_count == stats.index_record_count
                        && commit.estimated_lsm_bytes == stats.estimated_lsm_bytes
                        && commit.estimated_blob_bytes == stats.estimated_blob_bytes
                        && commit.start_lt == info.start_lt
                        && commit.end_lt == info.end_lt
                        && commit.gen_utime == info.gen_utime,
                    "partition commit marker statistics mismatch for {block_id}"
                );
            }

            if newly_committed {
                let commit = codec::PartitionCommit {
                    block_id: *block_id,
                    digest: block_id.root_hash,
                    transaction_count: stats.transaction_count,
                    estimated_lsm_bytes: stats.estimated_lsm_bytes,
                    estimated_blob_bytes: stats.estimated_blob_bytes,
                    index_record_count: stats.index_record_count,
                    start_lt: info.start_lt,
                    end_lt: info.end_lt,
                    gen_utime: info.gen_utime,
                };
                write_batch.put_cf(&db.partition_commits.cf(), commit_key, codec::encode_partition_commit(&commit));
                let _stage = HistogramGuard::begin("tycho_storage_rpc_write_partition_time");
                db.rocksdb().write_opt(write_batch, db.transactions.write_config()).context("failed to write RPC partition stage")?;
            }
            let _stage = HistogramGuard::begin("tycho_storage_rpc_write_current_state_time");
            current_state
                .rocksdb()
                .write_opt(current_state_batch, current_state.code_hashes.write_config()).context("failed to write RPC current-state stage")?;
            drop(_stage);

            let updates = updates
                .map(|map| {
                    map.into_iter()
                        .map(|(address, max_lt)| super::subscriptions::AccountUpdate {
                            address: StdAddr::new(workchain, address),
                            max_lt,
                            gen_utime: info.gen_utime,
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();

            Ok::<_, anyhow::Error>((info.start_lt, updates, stats))
        })
        .await??;

        // Update the runtime value first, then repair the durable value even after a prior
        // failed write already lowered the atomic cache.
        self.min_tx_lt.fetch_min(start_lt, Ordering::Release);
        let _guard = self.min_tx_lt_guard.lock().await;
        let min_tx_lt = self.min_tx_lt.load(Ordering::Acquire);
        self.partitions.lock().persist_min_transaction_lt_decrease(min_tx_lt)?;

        if !updates.is_empty() {
            subscriptions.fanout_updates(updates).await;
        }

        let stats = existing_commit.map(|commit| BlockWriteStats { transaction_count: commit.transaction_count, index_record_count: commit.index_record_count, estimated_lsm_bytes: commit.estimated_lsm_bytes, estimated_blob_bytes: commit.estimated_blob_bytes }).unwrap_or(computed_stats);
        Ok(BlockWriteResult { partition_id: partition_id.0, stats, newly_committed })
    }

    fn update_code_hash(
        db: &RpcCurrentStateDb,
        workchain: i8,
        account: &HashBytes,
        accounts: &ShardAccountsDict,
        remove: bool,
        write_batch: &mut rocksdb::WriteBatch,
    ) -> Result<()> {
        // Find the new code hash
        let new_code_hash = 'code_hash: {
            if !remove && let Some((_, account)) = accounts.get(account)? {
                match extract_code_hash(&account)? {
                    ExtractedCodeHash::Exact(hash) => break 'code_hash hash,
                    ExtractedCodeHash::Skip => return Ok(()),
                }
            }
            None
        };

        // Prepare column families
        let code_hashes_cf = &db.code_hashes.cf();
        let code_hashes_by_address_cf = &db.code_hashes_by_address.cf();

        // Check the secondary index first
        let mut code_hashes_by_address_id = [0u8; tables::CodeHashesByAddress::KEY_LEN];
        code_hashes_by_address_id[0] = workchain as u8;
        code_hashes_by_address_id[1..33].copy_from_slice(account.as_slice());

        // Find the old code hash
        let old_code_hash = db
            .code_hashes_by_address
            .get(code_hashes_by_address_id.as_slice())?;

        if remove && old_code_hash.is_none()
            || matches!(
                (&old_code_hash, &new_code_hash),
                (Some(old), Some(new)) if old.as_ref() == new.as_slice()
            )
        {
            // Code hash should not be changed.
            return Ok(());
        }

        let mut code_hashes_id = [0u8; tables::CodeHashes::KEY_LEN];
        code_hashes_id[32] = workchain as u8;
        code_hashes_id[33..65].copy_from_slice(account.as_slice());

        // Remove entry from the primary index
        if let Some(old_code_hash) = old_code_hash {
            code_hashes_id[..32].copy_from_slice(&old_code_hash);
            write_batch.delete_cf(code_hashes_cf, code_hashes_id.as_slice());
        }

        match new_code_hash {
            Some(new_code_hash) => {
                // Update primary index
                code_hashes_id[..32].copy_from_slice(new_code_hash.as_slice());
                write_batch.put_cf(
                    code_hashes_cf,
                    code_hashes_id.as_slice(),
                    new_code_hash.as_slice(),
                );

                // Update secondary index
                write_batch.put_cf(
                    code_hashes_by_address_cf,
                    code_hashes_by_address_id.as_slice(),
                    new_code_hash.as_slice(),
                );
            }
            None => {
                // Remove entry from the secondary index
                write_batch.delete_cf(
                    code_hashes_by_address_cf,
                    code_hashes_by_address_id.as_slice(),
                );
            }
        }

        Ok(())
    }

    async fn remove_code_hashes(&self, shard: &ShardIdent) -> Result<()> {
        let workchain = shard.workchain() as u8;

        // Remove from the secondary index first
        {
            let mut from = [0u8; { tables::CodeHashesByAddress::KEY_LEN }];
            from[0] = workchain;

            {
                let [_, from @ ..] = &mut from;
                extend_account_prefix(shard, false, from);
            }

            let mut to = from;
            {
                let [_, to @ ..] = &mut to;
                extend_account_prefix(shard, true, to);
            }

            let raw = self.current_state.rocksdb();
            let cf = &self.current_state.code_hashes_by_address.cf();
            let writeopts = self.current_state.code_hashes_by_address.write_config();

            // Remove `[from; to)`
            raw.delete_range_cf_opt(cf, &from, &to, writeopts)?;
            // Remove `to`, (-1:ffff..ffff might be a valid existing address)
            raw.delete_cf_opt(cf, to, writeopts)?;
        }

        let cancelled = CancellationFlag::new();
        scopeguard::defer! {
            cancelled.cancel();
        }

        // Full scan the main code hashes index and remove all entires for the shard
        let db = self.current_state.clone();
        let mut cancelled = cancelled.debounce(1000);
        let shard = *shard;
        let span = tracing::Span::current();

        // NOTE: `spawn_blocking` is used here instead of `rayon_run` as it is IO-bound task.
        tokio::task::spawn_blocking(move || {
            let _span = span.enter();

            let guard = scopeguard::guard((), |_| {
                tracing::warn!("cancelled");
            });

            let cf = &db.code_hashes.cf();

            let raw = db.rocksdb().as_ref();
            let snapshot = raw.snapshot();
            let mut readopts = db.code_hashes.new_read_config();
            readopts.set_snapshot(&snapshot);

            let writeopts = db.code_hashes.write_config();

            let mut iter = raw.raw_iterator_cf_opt(cf, readopts);
            iter.seek_to_first();

            let mut prefix = shard.prefix();
            let tag = extract_tag(&shard);
            prefix -= tag; // Remove tag from the prefix

            // For the prefix 1010000 the mask is 1100000
            let prefix_mask = !(tag | (tag - 1));

            loop {
                let key = match iter.key() {
                    Some(key) => key,
                    None => break iter.status()?,
                };

                if cancelled.check() {
                    anyhow::bail!("remove_code_hashes cancelled");
                }

                if key.len() != tables::CodeHashes::KEY_LEN
                    || key[32] == workchain
                        && (shard.is_full() || {
                            // Filter only the keys with the same prefix
                            let key = u64::from_be_bytes(key[33..41].try_into().unwrap());
                            (key ^ prefix) & prefix_mask == 0
                        })
                {
                    raw.delete_cf_opt(cf, key, writeopts)?;
                }

                iter.next();
            }

            scopeguard::ScopeGuard::into_inner(guard);
            Ok(())
        })
        .await?
    }
}

trait TableExt {
    fn get_ext<'db, K: AsRef<[u8]>>(
        &'db self,
        key: K,
        snapshot: &weedb::OwnedSnapshot,
    ) -> Result<Option<rocksdb::DBPinnableSlice<'db>>>;
}

impl<T: weedb::ColumnFamily> TableExt for weedb::Table<T> {
    fn get_ext<'db, K: AsRef<[u8]>>(
        &'db self,
        key: K,
        snapshot: &weedb::OwnedSnapshot,
    ) -> Result<Option<rocksdb::DBPinnableSlice<'db>>> {
        anyhow::ensure!(
            Arc::ptr_eq(snapshot.db(), self.db()),
            "snapshot must be made for the same DB instance"
        );
        let mut readopts = self.new_read_config();
        readopts.set_snapshot(snapshot);
        self.db()
            .get_pinned_cf_opt(&self.cf(), key, &readopts)
            .map_err(Into::into)
    }
}

#[derive(Debug)]
pub struct BriefShardDescr {
    pub shard_ident: ShardIdent,
    pub seqno: u32,
    pub root_hash: HashBytes,
    pub file_hash: HashBytes,
    pub start_lt: u64,
    pub end_lt: u64,
}

#[derive(Debug, Clone)]
pub struct BriefBlockInfo {
    pub global_id: i32,
    pub version: u32,
    pub flags: u8,
    pub after_merge: bool,
    pub after_split: bool,
    pub before_split: bool,
    pub want_merge: bool,
    pub want_split: bool,
    pub validator_list_hash_short: u32,
    pub catchain_seqno: u32,
    pub min_ref_mc_seqno: u32,
    pub is_key_block: bool,
    pub prev_key_block_seqno: u32,
    pub start_lt: u64,
    pub end_lt: u64,
    pub gen_utime: u32,
    pub vert_seqno: u32,
    pub rand_seed: HashBytes,
    pub tx_count: u32,
    pub master_ref: Option<BlockId>,
    pub prev_blocks: Vec<BlockId>,
}

impl BriefBlockInfo {
    const VERSION: u8 = 0;
    const MIN_BYTE_LEN: usize = 256;

    fn new(
        block: &Block,
        info: &BlockInfo,
        extra: &BlockExtra,
        tx_count: usize,
    ) -> Result<Self, tycho_types::error::Error> {
        let shard_ident = info.shard;
        let prev_blocks = match info.load_prev_ref()? {
            PrevBlockRef::Single(block_ref) => vec![block_ref.as_block_id(shard_ident)],
            PrevBlockRef::AfterMerge { left, right } => vec![
                left.as_block_id(shard_ident),
                right.as_block_id(shard_ident),
            ],
        };

        Ok(Self {
            global_id: block.global_id,
            version: info.version,
            flags: info.flags,
            after_merge: info.after_merge,
            after_split: info.after_split,
            before_split: info.before_split,
            want_merge: info.want_merge,
            want_split: info.want_split,
            validator_list_hash_short: info.gen_validator_list_hash_short,
            catchain_seqno: info.gen_catchain_seqno,
            min_ref_mc_seqno: info.min_ref_mc_seqno,
            is_key_block: info.key_block,
            prev_key_block_seqno: info.prev_key_block_seqno,
            start_lt: info.start_lt,
            end_lt: info.end_lt,
            gen_utime: info.gen_utime,
            vert_seqno: info.vert_seqno,
            rand_seed: extra.rand_seed,
            tx_count: tx_count.try_into().unwrap_or(u32::MAX),
            master_ref: info
                .load_master_ref()?
                .map(|r| r.as_block_id(ShardIdent::MASTERCHAIN)),
            prev_blocks,
        })
    }

    fn write_to_bytes(&self, target: &mut Vec<u8>) {
        // NOTE: Bit 0 is reserved for future.
        let packed_flags = ((self.master_ref.is_some() as u8) << 7)
            | ((self.after_merge as u8) << 6)
            | ((self.before_split as u8) << 5)
            | ((self.after_split as u8) << 4)
            | ((self.want_split as u8) << 3)
            | ((self.want_merge as u8) << 2)
            | ((self.is_key_block as u8) << 1);

        target.reserve(Self::MIN_BYTE_LEN);
        target.push(Self::VERSION);
        target.extend_from_slice(&self.global_id.to_le_bytes());
        target.extend_from_slice(&self.version.to_le_bytes());
        target.push(self.flags);
        target.push(packed_flags);
        target.extend_from_slice(&self.validator_list_hash_short.to_le_bytes());
        target.extend_from_slice(&self.catchain_seqno.to_le_bytes());
        target.extend_from_slice(&self.min_ref_mc_seqno.to_le_bytes());
        target.extend_from_slice(&self.prev_key_block_seqno.to_le_bytes());
        target.extend_from_slice(&self.start_lt.to_le_bytes());
        target.extend_from_slice(&self.end_lt.to_le_bytes());
        target.extend_from_slice(&self.gen_utime.to_le_bytes());
        target.extend_from_slice(&self.vert_seqno.to_le_bytes());
        target.extend_from_slice(self.rand_seed.as_slice());
        target.extend_from_slice(&self.tx_count.to_le_bytes());
        if let Some(block_id) = &self.master_ref {
            target.extend_from_slice(&block_id.seqno.to_le_bytes());
            target.extend_from_slice(block_id.root_hash.as_slice());
            target.extend_from_slice(block_id.file_hash.as_slice());
        }
        target.push(self.prev_blocks.len() as u8);
        for block_id in &self.prev_blocks {
            target.extend_from_slice(&block_id.shard.prefix().to_le_bytes());
            target.extend_from_slice(&block_id.seqno.to_le_bytes());
            target.extend_from_slice(block_id.root_hash.as_slice());
            target.extend_from_slice(block_id.file_hash.as_slice());
        }
    }

    fn load_from_bytes(workchain: i32, mut bytes: &[u8]) -> Option<Self> {
        use bytes::Buf;

        if bytes.get_u8() != Self::VERSION {
            return None;
        }

        let global_id = bytes.get_i32_le();
        let version = bytes.get_u32_le();
        let [flags, packed_flags] = bytes.get_u16().to_be_bytes();
        let validator_list_hash_short = bytes.get_u32_le();
        let catchain_seqno = bytes.get_u32_le();
        let min_ref_mc_seqno = bytes.get_u32_le();
        let prev_key_block_seqno = bytes.get_u32_le();
        let start_lt = bytes.get_u64_le();
        let end_lt = bytes.get_u64_le();
        let gen_utime = bytes.get_u32_le();
        let vert_seqno = bytes.get_u32_le();
        let rand_seed = HashBytes::from_slice(&bytes[..32]);
        bytes = &bytes[32..];
        let tx_count = bytes.get_u32_le();

        let master_ref = if packed_flags & 0b10000000 != 0 {
            let seqno = bytes.get_u32_le();
            let root_hash = HashBytes::from_slice(&bytes[0..32]);
            let file_hash = HashBytes::from_slice(&bytes[32..64]);
            bytes = &bytes[64..];
            Some(BlockId {
                shard: ShardIdent::MASTERCHAIN,
                seqno,
                root_hash,
                file_hash,
            })
        } else {
            None
        };

        let prev_block_count = bytes.get_u8();
        let mut prev_blocks = Vec::with_capacity(prev_block_count as _);
        for _ in 0..prev_block_count {
            prev_blocks.push(BlockId {
                shard: ShardIdent::new(
                    workchain,
                    u64::from_le_bytes(bytes[0..8].try_into().unwrap()),
                )
                .unwrap(),
                seqno: u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
                root_hash: HashBytes::from_slice(&bytes[12..44]),
                file_hash: HashBytes::from_slice(&bytes[44..76]),
            });
            bytes = &bytes[76..];
        }

        Some(Self {
            global_id,
            version,
            flags,
            after_merge: packed_flags & 0b01000000 != 0,
            after_split: packed_flags & 0b00010000 != 0,
            before_split: packed_flags & 0b00100000 != 0,
            want_merge: packed_flags & 0b00000100 != 0,
            want_split: packed_flags & 0b00001000 != 0,
            validator_list_hash_short,
            catchain_seqno,
            min_ref_mc_seqno,
            is_key_block: packed_flags & 0b00000010 != 0,
            prev_key_block_seqno,
            start_lt,
            end_lt,
            gen_utime,
            vert_seqno,
            rand_seed,
            tx_count,
            master_ref,
            prev_blocks,
        })
    }
}

impl Drop for RpcStorage {
    fn drop(&mut self) {
        self.sealing_cancel.cancel();
        if let Some(task) = self.sealing_task.take() {
            task.abort();
        }
        self.filter_worker.shutdown();
    }
}

struct RpcTransactionPartitionSnapshot {
    descriptor: PartitionDescriptor,
    snapshot: weedb::OwnedSnapshot,
    lease: PartitionReadLease,
}

struct RpcSnapshotInner {
    visible_frontier: BlockId,
    manifest_epoch: u64,
    descriptors: Vec<PartitionDescriptor>,
    descriptor_indices: FastHashMap<PartitionId, usize>,
    current_state: weedb::OwnedSnapshot,
    writable_partitions: BTreeMap<PartitionId, RpcTransactionPartitionSnapshot>,
}

fn snapshot_filter_bundles(
    partitions: &PartitionManager,
    filter_registry: &FilterRegistry,
    descriptors: &[PartitionDescriptor],
) -> Result<BTreeMap<PartitionId, Option<Arc<ValidatedFilterBundle>>>> {
    let mut filter_bundles = BTreeMap::new();
    for descriptor in descriptors {
        let bundle = if descriptor.lifecycle == codec::ManifestLifecycle::Sealed {
            let manifest_digest = partitions.sealed_manifest_digest(descriptor.id)?;
            filter_registry
                .get(descriptor.id)
                .filter(|bundle| bundle.manifest_digest() == manifest_digest)
        } else {
            None
        };
        filter_bundles.insert(descriptor.id, bundle);
    }
    Ok(filter_bundles)
}

fn publish_filter_snapshot(
    partitions: &Mutex<PartitionManager>,
    snapshots: &SnapshotPublisher,
    filter_registry: &FilterRegistry,
    id: PartitionId,
    bundle: Arc<ValidatedFilterBundle>,
) -> Result<()> {
    let mut published = snapshots.current.write();
    let current = published
        .as_ref()
        .context("RPC filter publication requires a correctness-ready snapshot")?;
    let partitions = partitions.lock();
    anyhow::ensure!(
        partitions.manifest_epoch() == current.manifest_epoch(),
        "RPC partition manifest changed before filter-only snapshot publication"
    );
    let descriptor = current
        .descriptor(id)
        .with_context(|| format!("filtered partition {} is missing from the RPC snapshot", id.0))?;
    anyhow::ensure!(
        descriptor.lifecycle == codec::ManifestLifecycle::Sealed,
        "RPC filters can only be published for a sealed partition"
    );
    anyhow::ensure!(
        bundle.partition_id() == id.0
            && partitions.sealed_manifest_digest(id)? == bundle.manifest_digest(),
        "RPC filter bundle does not match the current sealed partition manifest"
    );
    let mut filter_bundles =
        snapshot_filter_bundles(&partitions, filter_registry, &current.0.descriptors)?;
    filter_bundles.insert(id, Some(bundle.clone()));
    // filter-only publication must retain the exact immutable correctness base
    let next = RpcSnapshot(current.0.clone(), Arc::new(filter_bundles));
    anyhow::ensure!(
        next.visible_frontier() == current.visible_frontier()
            && next.manifest_epoch() == current.manifest_epoch(),
        "filter-only RPC snapshot publication changed visibility"
    );
    filter_registry.install(id, bundle);
    *published = Some(next);
    Ok(())
}

fn build_composite_snapshot(
    partitions: &mut PartitionManager,
    filter_registry: &FilterRegistry,
    visible_frontier: BlockId,
) -> Result<RpcSnapshot> {
    let descriptors = partitions
        .descriptors()
        .into_iter()
        .filter(|descriptor| descriptor.lifecycle != codec::ManifestLifecycle::Creating)
        .collect::<Vec<_>>();
    let descriptor_indices = descriptors
        .iter()
        .enumerate()
        .map(|(index, descriptor)| (descriptor.id, index))
        .collect();
    let mut writable_partitions = BTreeMap::new();
    for descriptor in &descriptors {
        match descriptor.lifecycle {
            codec::ManifestLifecycle::Active | codec::ManifestLifecycle::Sealing => {
                let lease = partitions.snapshot_lease(descriptor.id)?;
                let snapshot = lease.owned_snapshot();
                writable_partitions.insert(
                    descriptor.id,
                    RpcTransactionPartitionSnapshot {
                        descriptor: descriptor.clone(),
                        snapshot,
                        lease,
                    },
                );
            }
            codec::ManifestLifecycle::Creating => unreachable!(),
            // sealed partitions are immutable and opened through the bounded cache on demand
            codec::ManifestLifecycle::Sealed => {}
        }
    }
    anyhow::ensure!(
        writable_partitions.contains_key(&partitions.active_id()),
        "active transaction partition is missing from RPC snapshot"
    );
    let filter_bundles = snapshot_filter_bundles(partitions, filter_registry, &descriptors)?;
    Ok(RpcSnapshot(
        Arc::new(RpcSnapshotInner {
            visible_frontier,
            manifest_epoch: partitions.manifest_epoch(),
            descriptors,
            descriptor_indices,
            current_state: partitions.current_state_db().owned_snapshot(),
            writable_partitions,
        }),
        Arc::new(filter_bundles),
    ))
}

#[derive(Clone)]
pub struct RpcSnapshot(
    Arc<RpcSnapshotInner>,
    // every visible partition has one frozen optional acceleration bundle
    Arc<BTreeMap<PartitionId, Option<Arc<ValidatedFilterBundle>>>>,
);

impl RpcSnapshot {
    pub fn visible_frontier(&self) -> &BlockId {
        &self.0.visible_frontier
    }

    pub fn manifest_epoch(&self) -> u64 {
        self.0.manifest_epoch
    }

    fn descriptor(&self, id: PartitionId) -> Option<&PartitionDescriptor> {
        self.0
            .descriptor_indices
            .get(&id)
            .map(|index| &self.0.descriptors[*index])
    }

    fn filter_bundle(&self, id: PartitionId) -> Option<&Arc<ValidatedFilterBundle>> {
        self.1.get(&id).and_then(Option::as_ref)
    }

    fn filter_might_contain(
        &self,
        id: PartitionId,
        namespace: FilterNamespace,
        key: &[u8],
    ) -> Result<bool> {
        match self.filter_bundle(id) {
            Some(bundle) => bundle.filter(namespace).contains(key),
            None => Ok(true),
        }
    }

    fn descriptor_for_mc_seqno(&self, mc_seqno: u32) -> Option<&PartitionDescriptor> {
        if mc_seqno > self.visible_frontier().seqno {
            return None;
        }
        // non-empty manifest ranges are ordered and precede any empty range
        let index = self.0.descriptors.partition_point(|descriptor| {
            descriptor.last.block_id.is_some() && descriptor.last.mc_seqno < mc_seqno
        });
        let descriptor = self.0.descriptors.get(index)?;
        (descriptor.first.block_id.is_some()
            && descriptor.last.block_id.is_some()
            && descriptor.first.mc_seqno <= mc_seqno
            && mc_seqno <= descriptor.last.mc_seqno)
            .then_some(descriptor)
    }

    #[cfg(test)]
    fn active_partition(&self) -> &RpcTransactionPartitionSnapshot {
        self.0
            .writable_partitions
            .values()
            .find(|partition| {
                partition.descriptor.lifecycle == codec::ManifestLifecycle::Active
            })
            .expect("validated RPC snapshot has an active transaction partition")
    }

    fn current_state(&self) -> &weedb::OwnedSnapshot {
        &self.0.current_state
    }
}

struct RpcTransactionPartitionRead {
    id: PartitionId,
    snapshot: RpcSnapshot,
    lease: PartitionReadLease,
}

impl RpcTransactionPartitionRead {
    fn db_snapshot(&self) -> Option<&weedb::OwnedSnapshot> {
        self.snapshot
            .0
            .writable_partitions
            .get(&self.id)
            .map(|partition| &partition.snapshot)
    }

    fn get<T, K>(&self, table: &weedb::Table<T>, key: K) -> Result<Option<Vec<u8>>>
    where
        T: weedb::ColumnFamily,
        K: AsRef<[u8]>,
    {
        self.get_pinned(table, key)
            .map(|value| value.map(|value| value.as_ref().to_vec()))
    }

    fn get_pinned<'a, T, K>(
        &'a self,
        table: &'a weedb::Table<T>,
        key: K,
    ) -> Result<Option<rocksdb::DBPinnableSlice<'a>>>
    where
        T: weedb::ColumnFamily,
        K: AsRef<[u8]>,
    {
        anyhow::ensure!(
            Arc::ptr_eq(table.db(), self.lease.rocksdb()),
            "partition table must belong to the selected DB instance"
        );
        match self.db_snapshot() {
            Some(snapshot) => table.get_ext(key, snapshot),
            None => table.get(key).map_err(Into::into),
        }
    }

    fn read_options<T: weedb::ColumnFamily>(
        &self,
        table: &weedb::Table<T>,
    ) -> Result<rocksdb::ReadOptions> {
        anyhow::ensure!(
            Arc::ptr_eq(table.db(), self.lease.rocksdb()),
            "partition table must belong to the selected DB instance"
        );
        let mut readopts = table.new_read_config();
        if let Some(snapshot) = self.db_snapshot() {
            readopts.set_snapshot(snapshot);
        }
        Ok(readopts)
    }
}

fn acquire_partition_read(
    partitions: &Arc<Mutex<PartitionManager>>,
    snapshot: RpcSnapshot,
    id: PartitionId,
) -> Result<RpcTransactionPartitionRead> {
    let descriptor = snapshot
        .descriptor(id)
        .with_context(|| format!("transaction partition {} is missing from the RPC snapshot", id.0))?;
    let lease = match descriptor.lifecycle {
        codec::ManifestLifecycle::Active | codec::ManifestLifecycle::Sealing => {
            let partition = snapshot
                .0
                .writable_partitions
                .get(&id)
                .with_context(|| {
                    format!(
                        "writable transaction partition {} is missing from the RPC snapshot",
                        id.0
                    )
                })?;
            anyhow::ensure!(
                partition.descriptor == *descriptor,
                "writable transaction partition descriptor changed within the RPC snapshot"
            );
            partition.lease.clone()
        }
        codec::ManifestLifecycle::Sealed => {
            let opener = partitions.lock().sealed_lease_opener(id)?;
            opener.open()?
        }
        codec::ManifestLifecycle::Creating => {
            anyhow::bail!("transaction partition {} is still creating", id.0);
        }
    };
    Ok(RpcTransactionPartitionRead {
        id,
        snapshot,
        lease,
    })
}

pub struct BlocksByMcSeqnoIter {
    mc_seqno: u32,
    inner: weedb::OwnedRawIterator,
    partition: RpcTransactionPartitionRead,
}

impl BlocksByMcSeqnoIter {
    pub fn mc_seqno(&self) -> u32 {
        self.mc_seqno
    }

    pub fn snapshot(&self) -> &RpcSnapshot {
        &self.partition.snapshot
    }
}

impl Iterator for BlocksByMcSeqnoIter {
    // TODO: Extend with LT range?
    type Item = BlockId;

    fn next(&mut self) -> Option<Self::Item> {
        let (key, value) = self.inner.item()?;
        let shard = ShardIdent::new(
            key[4] as i8 as i32,
            u64::from_be_bytes(key[5..13].try_into().unwrap()),
        )
        .expect("stored shard must have a valid prefix");
        let seqno = u32::from_be_bytes(key[13..17].try_into().unwrap());

        let block_id = BlockId {
            shard,
            seqno,
            root_hash: HashBytes::from_slice(&value[0..32]),
            file_hash: HashBytes::from_slice(&value[32..64]),
        };
        self.inner.next();

        Some(block_id)
    }
}

pub struct CodeHashesIter<'a> {
    inner: rocksdb::DBRawIterator<'a>,
    snapshot: RpcSnapshot,
}

impl<'a> CodeHashesIter<'a> {
    pub fn snapshot(&self) -> &RpcSnapshot {
        &self.snapshot
    }

    pub fn into_raw(self) -> RawCodeHashesIter<'a> {
        RawCodeHashesIter {
            inner: self.inner,
            snapshot: self.snapshot,
        }
    }
}

impl Iterator for CodeHashesIter<'_> {
    type Item = StdAddr;

    fn next(&mut self) -> Option<Self::Item> {
        let value = self.inner.key()?;
        debug_assert!(value.len() == tables::CodeHashes::KEY_LEN);

        let result = Some(StdAddr {
            anycast: None,
            workchain: value[32] as i8,
            address: HashBytes(value[33..65].try_into().unwrap()),
        });
        self.inner.next();
        result
    }
}

pub struct RawCodeHashesIter<'a> {
    inner: rocksdb::DBRawIterator<'a>,
    snapshot: RpcSnapshot,
}

impl RawCodeHashesIter<'_> {
    pub fn snapshot(&self) -> &RpcSnapshot {
        &self.snapshot
    }
}

impl Iterator for RawCodeHashesIter<'_> {
    type Item = [u8; 33];

    fn next(&mut self) -> Option<Self::Item> {
        let value = self.inner.key()?;
        debug_assert!(value.len() == tables::CodeHashes::KEY_LEN);

        let result = Some(value[32..65].try_into().unwrap());
        self.inner.next();
        result
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct BlockTransactionsCursor {
    pub hash: HashBytes,
    pub lt: u64,
}

pub struct BlockTransactionIdsIter {
    block_id: BlockId,
    ref_by_mc_seqno: u32,
    is_reversed: bool,
    inner: weedb::OwnedRawIterator,
    partition: RpcTransactionPartitionRead,
    _sealed_exact_lookup_permit: Option<TrackedSealedExactLookupPermit>,
}

impl BlockTransactionIdsIter {
    pub fn is_reversed(&self) -> bool {
        self.is_reversed
    }

    pub fn block_id(&self) -> &BlockId {
        &self.block_id
    }

    pub fn ref_by_mc_seqno(&self) -> u32 {
        self.ref_by_mc_seqno
    }

    pub fn snapshot(&self) -> &RpcSnapshot {
        &self.partition.snapshot
    }
}

impl Iterator for BlockTransactionIdsIter {
    type Item = FullTransactionId;

    fn next(&mut self) -> Option<Self::Item> {
        let (key, value) = self.inner.item()?;
        let res = Some(FullTransactionId {
            account: StdAddr::new(key[0] as i8, HashBytes::from_slice(&key[13..45])),
            lt: u64::from_be_bytes(key[45..53].try_into().unwrap()),
            hash: HashBytes::from_slice(&value[0..32]),
        });
        if self.is_reversed {
            self.inner.prev();
        } else {
            self.inner.next();
        }
        res
    }
}

pub struct BlockTransactionsIterBuilder {
    ids: BlockTransactionIdsIter,
}

impl BlockTransactionsIterBuilder {
    #[inline]
    pub fn is_reversed(&self) -> bool {
        self.ids.is_reversed()
    }

    #[inline]
    pub fn block_id(&self) -> &BlockId {
        self.ids.block_id()
    }

    #[inline]
    pub fn ref_by_mc_seqno(&self) -> u32 {
        self.ids.ref_by_mc_seqno()
    }

    #[inline]
    pub fn snapshot(&self) -> &RpcSnapshot {
        self.ids.snapshot()
    }

    #[inline]
    pub fn into_ids(self) -> BlockTransactionIdsIter {
        self.ids
    }

    pub fn map<F, R>(self, map: F) -> BlockTransactionsIter<F>
    where
        for<'a> F: FnMut(&'a StdAddr, u64, &'a [u8]) -> R,
    {
        BlockTransactionsIter {
            ids: self.ids,
            map,
        }
    }
}

pub struct BlockTransactionsIter<F> {
    ids: BlockTransactionIdsIter,
    map: F,
}

impl<F> BlockTransactionsIter<F> {
    #[inline]
    pub fn is_reversed(&self) -> bool {
        self.ids.is_reversed()
    }

    #[inline]
    pub fn block_id(&self) -> &BlockId {
        self.ids.block_id()
    }

    #[inline]
    pub fn snapshot(&self) -> &RpcSnapshot {
        self.ids.snapshot()
    }

    #[inline]
    pub fn into_ids(self) -> BlockTransactionIdsIter {
        self.ids
    }
}

impl<F, R> Iterator for BlockTransactionsIter<F>
where
    for<'a> F: FnMut(&'a StdAddr, u64, &'a [u8]) -> Option<R>,
{
    type Item = R;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let id = self.ids.next()?;

            let mut key = [0; tables::Transactions::KEY_LEN];
            key[0] = id.account.workchain as u8;
            key[1..33].copy_from_slice(id.account.address.as_slice());
            key[33..41].copy_from_slice(&id.lt.to_be_bytes());

            let value = match self.ids.partition.get_pinned(
                &self.ids.partition.lease.transactions,
                key,
            ) {
                Ok(Some(value)) => value,
                // TODO: Maybe return error here?
                Ok(None) => continue,
                // TODO: Maybe return error here?
                Err(_) => return None,
            };
            if !TransactionData::related_mc_seqno(value.as_ref())
                .is_ok_and(|mc_seqno| mc_seqno == self.ids.ref_by_mc_seqno)
                || TransactionData::read_tx_hash(value.as_ref()) != id.hash
            {
                continue;
            }
            break (self.map)(
                &id.account,
                id.lt,
                TransactionData::read_transaction(&value),
            );
        }
    }
}

#[derive(Debug, Clone)]
pub struct FullTransactionId {
    pub account: StdAddr,
    pub lt: u64,
    pub hash: HashBytes,
}

pub struct TransactionsIterBuilder {
    is_reversed: bool,
    visible_frontier_seqno: u32,
    partitions: Arc<Mutex<PartitionManager>>,
    partition_ids: Vec<PartitionId>,
    range_from: [u8; tables::Transactions::KEY_LEN],
    range_to: [u8; tables::Transactions::KEY_LEN],
    snapshot: RpcSnapshot,
}

impl TransactionsIterBuilder {
    #[inline]
    pub fn is_reversed(&self) -> bool {
        self.is_reversed
    }

    #[inline]
    pub fn snapshot(&self) -> &RpcSnapshot {
        &self.snapshot
    }

    pub fn map<F, R>(self, map: F) -> TransactionsIter<F, false>
    where
        for<'a> F: FnMut(&'a [u8]) -> R,
    {
        TransactionsIter {
            is_reversed: self.is_reversed,
            visible_frontier_seqno: self.visible_frontier_seqno,
            current: None,
            next_partition: 0,
            visited_partitions: 0,
            partitions: self.partitions,
            partition_ids: self.partition_ids,
            range_from: self.range_from,
            range_to: self.range_to,
            map,
            snapshot: self.snapshot,
        }
    }

    pub fn map_ext<F, R>(self, map: F) -> TransactionsIter<F, true>
    where
        for<'a> F: FnMut(u64, &'a HashBytes, &'a [u8]) -> R,
    {
        TransactionsIter {
            is_reversed: self.is_reversed,
            visible_frontier_seqno: self.visible_frontier_seqno,
            current: None,
            next_partition: 0,
            visited_partitions: 0,
            partitions: self.partitions,
            partition_ids: self.partition_ids,
            range_from: self.range_from,
            range_to: self.range_to,
            map,
            snapshot: self.snapshot,
        }
    }
}

struct PartitionTransactionsIter {
    inner: weedb::OwnedRawIterator,
    partition: RpcTransactionPartitionRead,
}

pub struct TransactionsIter<F, const EXT: bool> {
    is_reversed: bool,
    visible_frontier_seqno: u32,
    current: Option<PartitionTransactionsIter>,
    next_partition: usize,
    visited_partitions: u64,
    partitions: Arc<Mutex<PartitionManager>>,
    partition_ids: Vec<PartitionId>,
    range_from: [u8; tables::Transactions::KEY_LEN],
    range_to: [u8; tables::Transactions::KEY_LEN],
    map: F,
    snapshot: RpcSnapshot,
}

pub type TransactionsExtIter<F> = TransactionsIter<F, true>;

impl<F, const EXT: bool> TransactionsIter<F, EXT> {
    #[inline]
    pub fn is_reversed(&self) -> bool {
        self.is_reversed
    }

    #[inline]
    pub fn snapshot(&self) -> &RpcSnapshot {
        &self.snapshot
    }

    fn open_next_partition(&mut self) -> bool {
        let Some(id) = self.partition_ids.get(self.next_partition).copied() else {
            return false;
        };
        self.next_partition += 1;
        self.visited_partitions += 1;
        let partition = match acquire_partition_read(
            &self.partitions,
            self.snapshot.clone(),
            id,
        ) {
            Ok(partition) => partition,
            Err(e) => {
                tracing::error!(
                    partition_id = id.0,
                    "failed to open RPC transaction partition during account iteration: {e:#}"
                );
                self.next_partition = self.partition_ids.len();
                return false;
            }
        };
        let table = &partition.lease.transactions;
        let mut readopts = match partition.read_options(table) {
            Ok(readopts) => readopts,
            Err(e) => {
                tracing::error!(
                    partition_id = id.0,
                    "failed to configure RPC transaction partition iterator: {e:#}"
                );
                self.next_partition = self.partition_ids.len();
                return false;
            }
        };
        readopts.set_iterate_lower_bound(self.range_from);
        readopts.set_iterate_upper_bound(self.range_to);
        let rocksdb = partition.lease.rocksdb();
        let mut inner =
            rocksdb.raw_iterator_cf_opt(&table.cf(), readopts);
        if self.is_reversed {
            inner.seek_for_prev(self.range_to);
        } else {
            inner.seek(self.range_from);
        }
        if let Err(e) = inner.status() {
            tracing::error!(
                partition_id = id.0,
                "failed to seek RPC transaction partition iterator: {e}"
            );
            self.next_partition = self.partition_ids.len();
            return false;
        }
        self.current = Some(PartitionTransactionsIter {
            // SAFETY: Iterator was created from the same DB instance.
            inner: unsafe { weedb::OwnedRawIterator::new(rocksdb.clone(), inner) },
            partition,
        });
        true
    }

    fn finish_current_partition(&mut self) -> bool {
        if let Some(current) = self.current.as_mut()
            && let Err(e) = current.inner.status()
        {
            tracing::error!(
                partition_id = current.partition.id.0,
                "RPC transaction partition iterator failed: {e}"
            );
            self.next_partition = self.partition_ids.len();
        }
        self.current = None;
        self.open_next_partition()
    }
}

impl<F, const EXT: bool> Drop for TransactionsIter<F, EXT> {
    fn drop(&mut self) {
        metrics::histogram!("tycho_storage_rpc_account_query_partitions_visited")
            .record(self.visited_partitions as f64);
    }
}

impl<F, R> Iterator for TransactionsIter<F, false>
where
    for<'a> F: FnMut(&'a [u8]) -> Option<R>,
{
    type Item = R;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current.is_none() && !self.open_next_partition() {
                return None;
            }
            let Some(value) = self.current.as_mut().unwrap().inner.value() else {
                if self.finish_current_partition() {
                    continue;
                }
                return None;
            };
            let visible = TransactionData::related_mc_seqno(value)
                .expect("validated rpc transaction value")
                <= self.visible_frontier_seqno;
            let result = if visible {
                (self.map)(TransactionData::read_transaction(value))
            } else {
                None
            };
            if self.is_reversed {
                self.current.as_mut().unwrap().inner.prev();
            } else {
                self.current.as_mut().unwrap().inner.next();
            }
            if let Some(result) = result {
                return Some(result);
            }
        }
    }
}

impl<F, R> Iterator for TransactionsIter<F, true>
where
    for<'a> F: FnMut(u64, &'a HashBytes, &'a [u8]) -> Option<R>,
{
    type Item = R;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current.is_none() && !self.open_next_partition() {
                return None;
            }
            let Some((key, value)) = self.current.as_mut().unwrap().inner.item() else {
                if self.finish_current_partition() {
                    continue;
                }
                return None;
            };
            let visible = TransactionData::related_mc_seqno(value)
                .expect("validated rpc transaction value")
                <= self.visible_frontier_seqno;
            let result = if visible {
                (self.map)(
                    u64::from_be_bytes(key[33..41].try_into().unwrap()),
                    &TransactionData::read_tx_hash(value),
                    TransactionData::read_transaction(value),
                )
            } else {
                None
            };
            if self.is_reversed {
                self.current.as_mut().unwrap().inner.prev();
            } else {
                self.current.as_mut().unwrap().inner.next();
            }
            if let Some(result) = result {
                return Some(result);
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct TransactionInfo {
    pub account: StdAddr,
    pub lt: u64,
    pub block_id: BlockId,
    pub mc_seqno: u32,
}

impl TransactionInfo {
    fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < tables::TransactionsByHash::VALUE_FULL_LEN {
            return None;
        }

        let account = StdAddr::new(bytes[0] as i8, HashBytes::from_slice(&bytes[1..33]));
        let lt = u64::from_be_bytes(bytes[33..41].try_into().unwrap());
        let prefix_len = bytes[41];
        debug_assert!(prefix_len < 64);

        let tail_mask = 1u64 << (63 - prefix_len);

        // TODO: Move into types?
        let Some(shard) = ShardIdent::new(
            bytes[0] as i8 as i32,
            (account.prefix() | tail_mask) & !(tail_mask - 1),
        ) else {
            // TODO: unwrap?
            return None;
        };

        let block_id = BlockId {
            shard,
            seqno: u32::from_le_bytes(bytes[42..46].try_into().unwrap()),
            root_hash: HashBytes::from_slice(&bytes[46..78]),
            file_hash: HashBytes::from_slice(&bytes[78..110]),
        };
        let mc_seqno = u32::from_le_bytes(bytes[110..114].try_into().unwrap());

        Some(Self {
            account,
            lt,
            block_id,
            mc_seqno,
        })
    }
}

pub struct TransactionDataExt<'a> {
    pub info: TransactionInfo,
    pub data: TransactionData<'a>,
}

pub struct TransactionData<'a> {
    data: TransactionDataInner<'a>,
}

enum TransactionDataInner<'a> {
    Pinned(rocksdb::DBPinnableSlice<'a>),
    Owned(Vec<u8>),
}

impl AsRef<[u8]> for TransactionDataInner<'_> {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Pinned(data) => data.as_ref(),
            Self::Owned(data) => data.as_ref(),
        }
    }
}

impl<'a> TransactionData<'a> {
    pub fn new(data: rocksdb::DBPinnableSlice<'a>) -> Self {
        Self {
            data: TransactionDataInner::Pinned(data),
        }
    }

    fn from_owned(data: Vec<u8>) -> Self {
        Self {
            data: TransactionDataInner::Owned(data),
        }
    }

    pub fn tx_hash(&self) -> HashBytes {
        let value = codec::decode_transaction_value(self.data.as_ref())
            .expect("validated rpc transaction value")
            .payload();
        HashBytes::from_slice(&value[1..33])
    }

    pub fn in_msg_hash(&self) -> Option<HashBytes> {
        let value = codec::decode_transaction_value(self.data.as_ref())
            .expect("validated rpc transaction value")
            .payload();

        let mask = TransactionMask::from_bits_retain(value[0]);
        mask.has_msg_hash()
            .then(|| HashBytes::from_slice(&value[33..65]))
    }

    fn read_tx_hash(value: &[u8]) -> HashBytes {
        let value = codec::decode_transaction_value(value)
            .expect("validated rpc transaction value")
            .payload();
        HashBytes::from_slice(&value[1..33])
    }

    fn related_mc_seqno(value: &[u8]) -> Result<u32> {
        Ok(codec::decode_transaction_value(value)?.mc_seqno())
    }

    fn read_transaction<T: AsRef<[u8]> + ?Sized>(value: &T) -> &[u8] {
        let value = codec::decode_transaction_value(value.as_ref())
            .expect("validated rpc transaction value")
            .payload();

        let mask = TransactionMask::from_bits_retain(value[0]);
        let boc_start = if mask.has_msg_hash() { 65 } else { 33 }; // 1 + 32 + (32)

        assert!(boc_start < value.len());

        value[boc_start..].as_ref()
    }
}

impl AsRef<[u8]> for TransactionData<'_> {
    fn as_ref(&self) -> &[u8] {
        Self::read_transaction(&self.data)
    }
}

enum ExtractedCodeHash {
    Exact(Option<HashBytes>),
    Skip,
}

fn extract_code_hash(account: &ShardAccount) -> Result<ExtractedCodeHash> {
    if account.account.inner().descriptor().is_pruned_branch() {
        return Ok(ExtractedCodeHash::Skip);
    }

    if let Some(account) = account.load_account()?
        && let AccountState::Active(state_init) = &account.state
        && let Some(code) = &state_init.code
    {
        return Ok(ExtractedCodeHash::Exact(Some(*code.repr_hash())));
    }

    Ok(ExtractedCodeHash::Exact(None))
}

fn split_shard(
    shard: &ShardIdent,
    accounts: &ShardAccountsDict,
    depth: u8,
    shards: &mut FastHashMap<ShardIdent, ShardAccountsDict>,
) -> Result<()> {
    fn split_shard_impl(
        shard: &ShardIdent,
        accounts: &ShardAccountsDict,
        depth: u8,
        shards: &mut FastHashMap<ShardIdent, ShardAccountsDict>,
        builder: &mut CellBuilder,
    ) -> Result<()> {
        let (left_shard_ident, right_shard_ident) = 'split: {
            if depth > 0
                && let Some((left, right)) = shard.split()
            {
                break 'split (left, right);
            }
            shards.insert(*shard, accounts.clone());
            return Ok(());
        };

        let (left_accounts, right_accounts) = {
            builder.clear_bits();
            let prefix_len = shard.prefix_len();
            if prefix_len > 0 {
                builder.store_uint(shard.prefix() >> (64 - prefix_len), prefix_len)?;
            }
            accounts.split_by_prefix(&builder.as_data_slice())?
        };

        split_shard_impl(
            &left_shard_ident,
            &left_accounts,
            depth - 1,
            shards,
            builder,
        )?;
        split_shard_impl(
            &right_shard_ident,
            &right_accounts,
            depth - 1,
            shards,
            builder,
        )
    }

    split_shard_impl(shard, accounts, depth, shards, &mut CellBuilder::new())
}

type ShardAccountsDict = Dict<HashBytes, (DepthBalanceInfo, ShardAccount)>;

fn extend_account_prefix(shard: &ShardIdent, max: bool, target: &mut [u8; 32]) {
    let mut prefix = shard.prefix();
    if max {
        // Fill remaining bits after the trailing bit
        // 1010000:
        // 1010000 | (1010000 - 1) = 1010000 | 1001111 = 1011111
        prefix |= prefix - 1;
    } else {
        // Remove the trailing bit
        // 1010000:
        // (!1010000 + 1) = 0101111 + 1 = 0110000
        // 1010000 & 0110000 = 0010000 // only trailing bit
        prefix -= extract_tag(shard);
    };
    target[..8].copy_from_slice(&prefix.to_be_bytes());
    target[8..].fill(0xff * max as u8);
}

const fn extract_tag(shard: &ShardIdent) -> u64 {
    let prefix = shard.prefix();
    prefix & (!prefix).wrapping_add(1)
}

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct TransactionMask: u8 {
        const HAS_MSG_HASH = 1 << 0;
    }
}

impl TransactionMask {
    pub fn has_msg_hash(&self) -> bool {
        self.contains(TransactionMask::HAS_MSG_HASH)
    }
}

type AddressKey = [u8; 33];

const INSTANCE_ID: &[u8] = b"instance_id";

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::filter::{FilterBuilder, FilterCatalogStore, FilterFileIdentity};
    use super::super::partition::tests::{TestMetricsRecorder, persist_initial_creating};
    use std::sync::Barrier;

    fn test_partitions_config() -> RpcTransactionPartitionsConfig {
        RpcTransactionPartitionsConfig {
            target_lsm_bytes: 1,
            target_blob_bytes: 1,
            target_index_records: 1,
            max_open_sealed_partitions: 1,
            ..Default::default()
        }
    }

    fn assert_sealed_exact_lookup_error<T>(
        result: Result<T>,
        expected: SealedExactLookupAvailabilityError,
    ) {
        let error = match result {
            Ok(_) => panic!("sealed exact lookup must fail"),
            Err(error) => error,
        };
        assert_eq!(
            error.downcast_ref::<SealedExactLookupAvailabilityError>(),
            Some(&expected),
        );
    }

    fn masterchain_block(seqno: u32) -> BlockId {
        BlockId {
            shard: ShardIdent::MASTERCHAIN,
            seqno,
            root_hash: HashBytes([seqno as u8; 32]),
            file_hash: HashBytes([(seqno as u8).wrapping_add(1); 32]),
        }
    }

    fn basechain_block(seqno: u32) -> BlockId {
        BlockId {
            shard: ShardIdent::BASECHAIN,
            seqno,
            root_hash: HashBytes([(seqno as u8).wrapping_add(10); 32]),
            file_hash: HashBytes([(seqno as u8).wrapping_add(11); 32]),
        }
    }

    struct ReadTestTransaction {
        lt: u64,
        hash: HashBytes,
        in_msg_hash: HashBytes,
        boc_byte: u8,
    }

    fn known_block_key(block_id: &BlockId) -> [u8; tables::KnownBlocks::KEY_LEN] {
        let mut key = [0; tables::KnownBlocks::KEY_LEN];
        key[0] = i8::try_from(block_id.shard.workchain()).unwrap() as u8;
        key[1..9].copy_from_slice(&block_id.shard.prefix().to_be_bytes());
        key[9..13].copy_from_slice(&block_id.seqno.to_be_bytes());
        key
    }

    fn known_block_value(
        block_id: &BlockId,
        mc_block_id: &BlockId,
        start_lt: u64,
        end_lt: u64,
        tx_count: u32,
    ) -> Vec<u8> {
        let mut value = Vec::new();
        value.extend_from_slice(block_id.root_hash.as_slice());
        value.extend_from_slice(block_id.file_hash.as_slice());
        value.extend_from_slice(&mc_block_id.seqno.to_le_bytes());
        BriefBlockInfo {
            global_id: 0,
            version: 0,
            flags: 0,
            after_merge: false,
            after_split: false,
            before_split: false,
            want_merge: false,
            want_split: false,
            validator_list_hash_short: 0,
            catchain_seqno: 0,
            min_ref_mc_seqno: 0,
            is_key_block: false,
            prev_key_block_seqno: 0,
            start_lt,
            end_lt,
            gen_utime: mc_block_id.seqno,
            vert_seqno: 0,
            rand_seed: HashBytes::ZERO,
            tx_count,
            master_ref: (!block_id.is_masterchain()).then_some(*mc_block_id),
            prev_blocks: Vec::new(),
        }
        .write_to_bytes(&mut value);
        value
    }

    fn test_filter_bundle(
        partition_id: PartitionId,
        generation_id: u128,
        manifest_digest: HashBytes,
        transaction_keys: &[HashBytes],
        inbound_message_keys: &[HashBytes],
        block_keys: &[[u8; tables::KnownBlocks::KEY_LEN]],
    ) -> Arc<ValidatedFilterBundle> {
        fn build(
            namespace: FilterNamespace,
            partition_id: PartitionId,
            generation_id: u128,
            manifest_digest: HashBytes,
            keys: &[&[u8]],
        ) -> super::super::filter::ImmutableNamespaceFilter {
            let mut builder = FilterBuilder::new(
                FilterFileIdentity {
                    namespace,
                    partition_id: partition_id.0,
                    generation_id,
                    manifest_digest,
                },
                keys.len() as u64,
                1_000,
            )
            .unwrap();
            for key in keys {
                builder.insert(key).unwrap();
            }
            builder.finish().unwrap()
        }

        let transaction_keys = transaction_keys
            .iter()
            .map(HashBytes::as_slice)
            .collect::<Vec<_>>();
        let inbound_message_keys = inbound_message_keys
            .iter()
            .map(HashBytes::as_slice)
            .collect::<Vec<_>>();
        let block_keys = block_keys
            .iter()
            .map(|key| key.as_slice())
            .collect::<Vec<_>>();
        Arc::new(
            ValidatedFilterBundle::new(
                build(
                    FilterNamespace::Transactions,
                    partition_id,
                    generation_id,
                    manifest_digest,
                    &transaction_keys,
                ),
                build(
                    FilterNamespace::InboundMessages,
                    partition_id,
                    generation_id,
                    manifest_digest,
                    &inbound_message_keys,
                ),
                build(
                    FilterNamespace::Blocks,
                    partition_id,
                    generation_id,
                    manifest_digest,
                    &block_keys,
                ),
                u64::MAX,
            )
            .unwrap(),
        )
    }

    fn insert_read_test_block(
        storage: &RpcStorage,
        account: &StdAddr,
        mc_block_id: &BlockId,
        block_id: &BlockId,
        transactions: &[ReadTestTransaction],
        estimated_lsm_bytes: u64,
    ) -> PartitionId {
        let (partition_id, lease) = {
            let manager = storage.partitions.lock();
            (manager.active_id(), manager.active_lease())
        };
        let mut local_batch = rocksdb::WriteBatch::default();
        let start_lt = transactions.first().map_or(mc_block_id.seqno as u64 * 10, |tx| tx.lt);
        let end_lt = transactions.last().map_or(start_lt + 1, |tx| tx.lt + 1);
        for tx in transactions {
            let mut tx_key = [0; tables::Transactions::KEY_LEN];
            tx_key[0] = account.workchain as u8;
            tx_key[1..33].copy_from_slice(account.address.as_slice());
            tx_key[33..41].copy_from_slice(&tx.lt.to_be_bytes());

            let mut payload = Vec::with_capacity(66);
            payload.push(TransactionMask::HAS_MSG_HASH.bits());
            payload.extend_from_slice(tx.hash.as_slice());
            payload.extend_from_slice(tx.in_msg_hash.as_slice());
            payload.push(tx.boc_byte);
            local_batch.put_cf(
                &lease.transactions.cf(),
                tx_key,
                codec::encode_transaction_value(mc_block_id.seqno, &payload).unwrap(),
            );

            let mut tx_info = [0; tables::TransactionsByHash::VALUE_FULL_LEN];
            tx_info[0] = account.workchain as u8;
            tx_info[1..33].copy_from_slice(account.address.as_slice());
            tx_info[33..41].copy_from_slice(&tx.lt.to_be_bytes());
            tx_info[41] = block_id.shard.prefix_len() as u8;
            tx_info[42..46].copy_from_slice(&block_id.seqno.to_le_bytes());
            tx_info[46..78].copy_from_slice(block_id.root_hash.as_slice());
            tx_info[78..110].copy_from_slice(block_id.file_hash.as_slice());
            tx_info[110..114].copy_from_slice(&mc_block_id.seqno.to_le_bytes());
            local_batch.put_cf(&lease.transactions_by_hash.cf(), tx.hash, tx_info);
            local_batch.put_cf(&lease.transactions_by_in_msg.cf(), tx.in_msg_hash, tx_key);

            let mut block_tx_key = [0; tables::BlockTransactions::KEY_LEN];
            block_tx_key[0] = block_id.shard.workchain() as i8 as u8;
            block_tx_key[1..9].copy_from_slice(&block_id.shard.prefix().to_be_bytes());
            block_tx_key[9..13].copy_from_slice(&block_id.seqno.to_be_bytes());
            block_tx_key[13..45].copy_from_slice(account.address.as_slice());
            block_tx_key[45..53].copy_from_slice(&tx.lt.to_be_bytes());
            local_batch.put_cf(&lease.block_transactions.cf(), block_tx_key, tx.hash);

        }

        for current_block_id in [block_id, mc_block_id] {
            let tx_count = if current_block_id == block_id {
                transactions.len() as u32
            } else {
                0
            };
            local_batch.put_cf(
                &lease.known_blocks.cf(),
                known_block_key(current_block_id),
                known_block_value(
                    current_block_id,
                    mc_block_id,
                    start_lt,
                    end_lt,
                    tx_count,
                ),
            );
        }

        let mut block_value = Vec::new();
        block_value.extend_from_slice(block_id.root_hash.as_slice());
        block_value.extend_from_slice(block_id.file_hash.as_slice());
        block_value.extend_from_slice(&start_lt.to_le_bytes());
        block_value.extend_from_slice(&end_lt.to_le_bytes());
        let mut block_key = [0; tables::BlocksByMcSeqno::KEY_LEN];
        block_key[..4].copy_from_slice(&mc_block_id.seqno.to_be_bytes());
        block_key[4] = block_id.shard.workchain() as i8 as u8;
        block_key[5..13].copy_from_slice(&block_id.shard.prefix().to_be_bytes());
        block_key[13..17].copy_from_slice(&block_id.seqno.to_be_bytes());
        local_batch.put_cf(&lease.blocks_by_mc_seqno.cf(), block_key, &block_value);

        let mut mc_value = Vec::new();
        mc_value.extend_from_slice(mc_block_id.root_hash.as_slice());
        mc_value.extend_from_slice(mc_block_id.file_hash.as_slice());
        mc_value.extend_from_slice(&start_lt.to_le_bytes());
        mc_value.extend_from_slice(&end_lt.to_le_bytes());
        mc_value.extend_from_slice(&1u32.to_le_bytes());
        mc_value.push(block_id.shard.workchain() as i8 as u8);
        mc_value.extend_from_slice(&block_id.shard.prefix().to_le_bytes());
        mc_value.extend_from_slice(&block_id.seqno.to_le_bytes());
        mc_value.extend_from_slice(block_id.root_hash.as_slice());
        mc_value.extend_from_slice(block_id.file_hash.as_slice());
        mc_value.extend_from_slice(&start_lt.to_le_bytes());
        mc_value.extend_from_slice(&end_lt.to_le_bytes());
        let mut mc_key = [0; tables::BlocksByMcSeqno::KEY_LEN];
        mc_key[..4].copy_from_slice(&mc_block_id.seqno.to_be_bytes());
        mc_key[4] = mc_block_id.shard.workchain() as i8 as u8;
        mc_key[5..13].copy_from_slice(&mc_block_id.shard.prefix().to_be_bytes());
        mc_key[13..17].copy_from_slice(&mc_block_id.seqno.to_be_bytes());
        local_batch.put_cf(&lease.blocks_by_mc_seqno.cf(), mc_key, mc_value);

        for (current_block_id, transaction_count, lsm_bytes) in [
            (block_id, transactions.len() as u64, estimated_lsm_bytes),
            (mc_block_id, 0, 0),
        ] {
            let commit = codec::PartitionCommit {
                block_id: *current_block_id,
                digest: current_block_id.root_hash,
                transaction_count,
                estimated_lsm_bytes: lsm_bytes,
                estimated_blob_bytes: 0,
                index_record_count: transaction_count * 4,
                start_lt,
                end_lt,
                gen_utime: mc_block_id.seqno,
            };
            local_batch.put_cf(
                &lease.partition_commits.cf(),
                codec::partition_commit_key(
                    mc_block_id.seqno,
                    &current_block_id.as_short_id(),
                ),
                codec::encode_partition_commit(&commit),
            );
        }
        lease
            .rocksdb()
            .write_opt(local_batch, lease.transactions.write_config())
            .unwrap();
        partition_id
    }

    async fn wait_for_sealed_partition(storage: &RpcStorage, id: PartitionId) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if storage
                    .partitions
                    .lock()
                    .descriptors()
                    .iter()
                    .any(|descriptor| {
                        descriptor.id == id
                            && descriptor.lifecycle == codec::ManifestLifecycle::Sealed
                    })
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }

    fn insert_masterchain_commit(
        manager: &PartitionManager,
        block_id: &BlockId,
        estimated_lsm_bytes: u64,
    ) {
        let commit = codec::PartitionCommit {
            block_id: *block_id,
            digest: block_id.root_hash,
            transaction_count: 0,
            estimated_lsm_bytes,
            estimated_blob_bytes: 0,
            index_record_count: 0,
            start_lt: block_id.seqno as u64,
            end_lt: block_id.seqno as u64 + 1,
            gen_utime: block_id.seqno,
        };
        let db = manager.active_db();
        let mut batch = rocksdb::WriteBatch::default();
        batch.put_cf(
            &db.partition_commits.cf(),
            codec::partition_commit_key(block_id.seqno, &block_id.as_short_id()),
            codec::encode_partition_commit(&commit),
        );
        batch.put_cf(
            &db.known_blocks.cf(),
            known_block_key(block_id),
            known_block_value(
                block_id,
                block_id,
                block_id.seqno as u64,
                block_id.seqno as u64 + 1,
                0,
            ),
        );
        db.rocksdb()
            .write_opt(batch, db.partition_commits.write_config())
            .unwrap();
    }

    fn reconciliation_error(storage: &RpcStorage, core_frontier: &BlockId) -> anyhow::Error {
        match storage.reconcile_startup(core_frontier) {
            Ok(_) => panic!("startup reconciliation unexpectedly succeeded"),
            Err(error) => error,
        }
    }

    fn persist_visible_frontier(storage: &RpcStorage, block_id: &BlockId) {
        storage
            .partitions
            .lock()
            .control_db()
            .state
            .insert(
                codec::visible_frontier_key(),
                codec::encode_visible_frontier(block_id),
            )
            .unwrap();
    }

    fn publish_test_frontier(storage: &RpcStorage, block_id: &BlockId) {
        if block_id.seqno != 0 {
            let mut manager = storage.partitions.lock();
            insert_masterchain_commit(&manager, block_id, 0);
            manager.commit_masterchain_block_set(block_id).unwrap();
        }
        storage.publish_snapshot(block_id).unwrap();
    }

    #[test]
    fn shard_prefix() {
        let prefix_len = 10;

        let account_prefix = 0xabccdeadaaaaaaaa;
        let tail_mask = 1u64 << (63 - prefix_len);

        let shard = ShardIdent::new(0, (account_prefix | tail_mask) & !(tail_mask - 1)).unwrap();
        assert_eq!(shard, unsafe {
            ShardIdent::new_unchecked(0, 0xabe0000000000000)
        });
    }

    #[test]
    fn block_write_stats_counts_blob_threshold_and_indices() {
        let mut below = BlockWriteStats::default();
        below.add_transaction((DEFAULT_MIN_BLOB_SIZE - 1) as usize, false).unwrap();
        assert_eq!(below.transaction_count, 1);
        assert_eq!(below.index_record_count, 3);
        assert_eq!(below.estimated_blob_bytes, 0);
        assert_eq!(below.estimated_lsm_bytes, (tables::Transactions::KEY_LEN + (DEFAULT_MIN_BLOB_SIZE - 1) as usize + 32 + tables::TransactionsByHash::VALUE_FULL_LEN + tables::BlockTransactions::KEY_LEN + 32) as u64);
        let mut at = BlockWriteStats::default();
        at.add_transaction(DEFAULT_MIN_BLOB_SIZE as usize, true).unwrap();
        assert_eq!(at.transaction_count, 1);
        assert_eq!(at.index_record_count, 4);
        assert_eq!(at.estimated_blob_bytes, DEFAULT_MIN_BLOB_SIZE);
        assert_eq!(at.estimated_lsm_bytes, (tables::Transactions::KEY_LEN + 32 + tables::TransactionsByHash::VALUE_FULL_LEN + 32 + tables::Transactions::KEY_LEN + tables::BlockTransactions::KEY_LEN + 32) as u64);
    }

    #[test]
    fn block_write_stats_excludes_metadata() {
        let stats = BlockWriteStats::default();
        assert_eq!(stats.transaction_count, 0);
        assert_eq!(stats.index_record_count, 0);
        assert_eq!(stats.estimated_lsm_bytes, 0);
        assert_eq!(stats.estimated_blob_bytes, 0);
    }

    #[tokio::test]
    async fn open_constructs_filter_worker_without_opening_acceleration_storage() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let root = context.root_dir().path().to_path_buf();
        let _storage = RpcStorage::open(context, RpcTransactionPartitionsConfig::default()).unwrap();
        assert!(!root.join("rpc/filter-catalog").exists());
        assert!(!root.join("rpc/filters").exists());
    }

    #[tokio::test]
    async fn startup_reconciliation_accepts_initial_creating_before_lifecycle() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        persist_initial_creating(&context);
        let storage = RpcStorage::open(context, RpcTransactionPartitionsConfig::default()).unwrap();
        let zerostate = masterchain_block(0);
        let reconciliation = storage.reconcile_startup(&zerostate).unwrap();
        assert_eq!(reconciliation.effective_frontier, zerostate);
        assert!(!reconciliation.rebuild_current_state);
        assert!(storage.partitions.lock().has_creating_partition());
        storage.continue_lifecycle().unwrap();
        storage.publish_snapshot(&zerostate).unwrap();
        assert!(storage.load_snapshot().is_some());
    }

    #[tokio::test]
    async fn lifecycle_continuation_wakes_persisted_sealing_after_reconciliation() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let config = test_partitions_config();
        let storage = RpcStorage::open(context.clone(), config.clone()).unwrap();
        storage.sealing_cancel.cancel();
        let masterchain = masterchain_block(1);
        let sealing_id = insert_read_test_block(
            &storage,
            &StdAddr::new(0, HashBytes::ZERO),
            &masterchain,
            &basechain_block(1),
            &[ReadTestTransaction {
                lt: 10,
                hash: HashBytes([1; 32]),
                in_msg_hash: HashBytes([101; 32]),
                boc_byte: 1,
            }],
            1,
        );
        storage.commit_masterchain_block_set(&masterchain).unwrap();
        drop(storage);
        tokio::task::yield_now().await;

        let storage = RpcStorage::open(context, config).unwrap();
        let before = {
            let manager = storage.partitions.lock();
            (
                manager
                    .control_db()
                    .state
                    .get(codec::manifest_epoch_key())
                    .unwrap()
                    .unwrap()
                    .to_vec(),
                manager
                    .control_db()
                    .manifests
                    .get(codec::partition_manifest_key(sealing_id.0))
                    .unwrap()
                    .unwrap()
                    .to_vec(),
            )
        };
        assert_eq!(
            storage
                .partitions
                .lock()
                .descriptors()
                .into_iter()
                .find(|descriptor| descriptor.id == sealing_id)
                .unwrap()
                .lifecycle,
            codec::ManifestLifecycle::Sealing,
        );
        let reconciliation = storage.reconcile_startup(&masterchain).unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        let after_reconciliation = {
            let manager = storage.partitions.lock();
            (
                manager
                    .control_db()
                    .state
                    .get(codec::manifest_epoch_key())
                    .unwrap()
                    .unwrap()
                    .to_vec(),
                manager
                    .control_db()
                    .manifests
                    .get(codec::partition_manifest_key(sealing_id.0))
                    .unwrap()
                    .unwrap()
                    .to_vec(),
            )
        };
        assert_eq!(after_reconciliation, before);
        let held_sealing_lease = storage.partitions.lock().read_lease(sealing_id).unwrap();
        storage.continue_lifecycle().unwrap();
        storage
            .publish_snapshot(&reconciliation.effective_frontier)
            .unwrap();
        drop(held_sealing_lease);
        wait_for_sealed_partition(&storage, sealing_id).await;
    }

    #[tokio::test]
    async fn observability_records_startup_lifecycle_and_admission_results() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = RpcStorage::open(context, test_partitions_config()).unwrap();
        let zerostate = masterchain_block(0);
        let recorder = TestMetricsRecorder::default();

        metrics::with_local_recorder(&recorder, || {
            let reconciliation = storage.reconcile_startup(&zerostate).unwrap();
            storage.continue_lifecycle().unwrap();
            storage.publish_snapshot(&reconciliation.effective_frontier).unwrap();
            assert_eq!(
                storage.admit_block_set(&masterchain_block(1)).unwrap(),
                BlockSetMode::New
            );
            assert!(storage.admit_block_set(&masterchain_block(3)).is_err());
        });

        assert_eq!(
            recorder.histogram_len(
                "tycho_storage_rpc_startup_reconciliation_duration_seconds|result=success"
            ),
            1
        );
        assert_eq!(
            recorder.histogram_values(
                "tycho_storage_rpc_startup_synchronous_sealed_opens|result=success"
            ),
            vec![0.0]
        );
        assert_eq!(
            recorder.histogram_len(
                "tycho_storage_rpc_active_tail_probe_duration_seconds|result=empty"
            ),
            1
        );
        assert_eq!(
            recorder.histogram_len(
                "tycho_storage_rpc_deferred_lifecycle_duration_seconds|result=success"
            ),
            1
        );
        assert_eq!(
            recorder.histogram_len(
                "tycho_storage_rpc_initial_local_snapshot_duration_seconds|result=success"
            ),
            1
        );
        assert_eq!(
            recorder.counter(
                "tycho_storage_rpc_block_set_admission_checks_total|stage=new|result=success"
            ),
            1
        );
        assert_eq!(
            recorder.counter(
                "tycho_storage_rpc_block_set_admission_failures_total|stage=new|reason=token_conflict"
            ),
            1
        );
    }

    #[tokio::test]
    async fn sealing_worker_flushes_closes_and_publishes_read_only_partition() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let mut manager = PartitionManager::open(
            context,
            RpcTransactionPartitionsConfig {
                target_lsm_bytes: 1,
                target_blob_bytes: 1,
                target_index_records: 1,
                max_open_sealed_partitions: 1,
                ..Default::default()
            },
        )
        .unwrap();
        manager.request_rotation(super::super::partition::PartitionCounters {
            estimated_lsm_bytes: 1,
            ..Default::default()
        });
        let old = manager.rotate_if_requested().unwrap().unwrap().0;
        let partitions = Arc::new(Mutex::new(manager));

        seal_partition(
            partitions.clone(),
            Arc::new(SnapshotPublisher::default()),
            Arc::new(FilterRegistry::default()),
            old,
            CancellationFlag::new(),
            None,
        )
            .await
            .unwrap();

        let manager = partitions.lock();
        assert_eq!(manager.descriptors().into_iter().find(|entry| entry.id == old).unwrap().lifecycle, codec::ManifestLifecycle::Sealed);
        assert!(manager.sealed_lease(old).unwrap().db().partition_commits.insert([1], [1]).is_err());
    }

    #[tokio::test]
    async fn composite_snapshot_gate_releases_sealing_partition() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = Arc::new(RpcStorage::open(context, test_partitions_config()).unwrap());
        let block_id = masterchain_block(1);
        {
            let mut manager = storage.partitions.lock();
            insert_masterchain_commit(&manager, &block_id, 1);
            manager.commit_masterchain_block_set(&block_id).unwrap();
        }
        storage.publish_snapshot(&block_id).unwrap();
        storage.publish_snapshot(&masterchain_block(0)).unwrap();
        assert_eq!(
            storage.load_snapshot().unwrap().visible_frontier(),
            &block_id
        );
        let mut different = block_id;
        different.file_hash = HashBytes([0xff; 32]);
        assert!(storage.publish_snapshot(&different).is_err());
        let held_snapshot = storage.load_snapshot().unwrap();
        assert_eq!(held_snapshot.0.writable_partitions.len(), 2);

        storage.sealing_notify.notify_one();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(storage
            .partitions
            .lock()
            .descriptors()
            .iter()
            .any(|descriptor| descriptor.lifecycle == codec::ManifestLifecycle::Sealing));

        let publish_storage = storage.clone();
        tokio::time::timeout(
            Duration::from_secs(1),
            tokio::task::spawn_blocking(move || publish_storage.publish_snapshot(&block_id)),
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();
        let load_storage = storage.clone();
        let latest = tokio::time::timeout(
            Duration::from_secs(1),
            tokio::task::spawn_blocking(move || load_storage.load_snapshot().unwrap()),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(latest.visible_frontier(), &block_id);
        drop(latest);

        drop(held_snapshot);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if storage
                    .partitions
                    .lock()
                    .descriptors()
                    .iter()
                    .any(|descriptor| descriptor.lifecycle == codec::ManifestLifecycle::Sealed)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(storage.filter_worker.pending_sealed_contains(
            storage
                .partitions
                .lock()
                .descriptors()
                .iter()
                .find(|descriptor| descriptor.lifecycle == codec::ManifestLifecycle::Sealed)
                .unwrap()
                .id,
        ));

        let snapshot = storage.load_snapshot().unwrap();
        assert_eq!(snapshot.visible_frontier(), &block_id);
        assert_eq!(snapshot.0.writable_partitions.len(), 1);
        assert_eq!(
            snapshot.active_partition().descriptor.id,
            storage.partitions.lock().active_id()
        );
    }

    #[tokio::test]
    async fn composite_snapshot_restores_missing_sealing_handle() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let mut manager =
            PartitionManager::open(context, test_partitions_config()).unwrap();
        let block_id = masterchain_block(1);
        let old = manager.active_id();
        insert_masterchain_commit(&manager, &block_id, 1);
        manager.commit_masterchain_block_set(&block_id).unwrap();

        let worker = manager.begin_sealing(old).unwrap();
        drop(worker);
        let primary = manager.take_sealing_handle(old).unwrap();
        drop(primary);
        assert_eq!(manager.sealing_handle_strong_count(old), None);

        let snapshot =
            build_composite_snapshot(&mut manager, &FilterRegistry::default(), block_id).unwrap();
        assert!(snapshot.0.writable_partitions.contains_key(&old));
        assert_eq!(manager.sealing_handle_strong_count(old), Some(2));
    }

    #[tokio::test]
    async fn startup_reconciliation_validates_frontiers() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = RpcStorage::open(context, RpcTransactionPartitionsConfig::default()).unwrap();
        let zerostate = masterchain_block(0);
        let empty = storage.reconcile_startup(&zerostate).unwrap();
        assert_eq!(empty.effective_frontier, zerostate);
        assert!(!empty.rebuild_current_state);

        let block_id = masterchain_block(1);
        assert!(storage.reconcile_startup(&block_id).is_err());
        {
            let mut manager = storage.partitions.lock();
            insert_masterchain_commit(&manager, &block_id, 0);
            manager.commit_masterchain_block_set(&block_id).unwrap();
        }
        let current = storage.reconcile_startup(&block_id).unwrap();
        assert_eq!(current.effective_frontier, block_id);
        assert!(!current.rebuild_current_state);
        assert!(storage.reconcile_startup(&zerostate).unwrap().rebuild_current_state);

        let mut different = block_id;
        different.root_hash = HashBytes([0xff; 32]);
        assert!(storage.reconcile_startup(&different).is_err());
        assert!(storage.reconcile_startup(&masterchain_block(2)).is_err());

        let next = masterchain_block(2);
        {
            let mut manager = storage.partitions.lock();
            insert_masterchain_commit(&manager, &next, 0);
            manager.commit_masterchain_block_set(&next).unwrap();
        }
        let behind_control = storage.reconcile_startup(&block_id).unwrap();
        assert_eq!(behind_control.effective_frontier, block_id);
        assert!(behind_control.rebuild_current_state);
        storage.publish_snapshot(&behind_control.effective_frontier).unwrap();
        assert_eq!(storage.load_snapshot().unwrap().visible_frontier(), &block_id);
    }

    #[tokio::test]
    async fn zerostate_startup_accepts_only_matching_or_absent_frontier() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let zerostate = masterchain_block(0);
        let storage = RpcStorage::open(context.clone(), RpcTransactionPartitionsConfig::default()).unwrap();
        assert_eq!(storage.reconcile_startup(&zerostate).unwrap().effective_frontier, zerostate);
        persist_visible_frontier(&storage, &zerostate);
        drop(storage);
        tokio::task::yield_now().await;

        let storage = RpcStorage::open(context.clone(), RpcTransactionPartitionsConfig::default()).unwrap();
        assert_eq!(storage.reconcile_startup(&zerostate).unwrap().effective_frontier, zerostate);
        let mut different = zerostate;
        different.root_hash = HashBytes([0xff; 32]);
        persist_visible_frontier(&storage, &different);
        drop(storage);
        tokio::task::yield_now().await;

        let storage = RpcStorage::open(context, RpcTransactionPartitionsConfig::default()).unwrap();
        assert!(format!("{:#}", reconciliation_error(&storage, &zerostate))
            .contains("RPC and core frontiers have different full block ids"));
    }

    #[tokio::test]
    async fn startup_reconciliation_detects_unpublished_commits_after_reopen() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        {
            let manager =
                PartitionManager::open(context.clone(), RpcTransactionPartitionsConfig::default())
                    .unwrap();
            insert_masterchain_commit(&manager, &masterchain_block(1), 0);
        }
        let storage = RpcStorage::open(context, RpcTransactionPartitionsConfig::default()).unwrap();
        assert!(storage
            .reconcile_startup(&masterchain_block(0))
            .unwrap()
            .rebuild_current_state);
    }

    #[tokio::test]
    async fn same_boundary_retry_accepts_a_durable_replay_tail_ahead_of_visibility() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let first = masterchain_block(1);
        let second = masterchain_block(2);
        {
            let storage = RpcStorage::open(context.clone(), RpcTransactionPartitionsConfig::default()).unwrap();
            let mut manager = storage.partitions.lock();
            insert_masterchain_commit(&manager, &first, 0);
            manager.commit_masterchain_block_set(&first).unwrap();
            insert_masterchain_commit(&manager, &second, 0);
            manager.commit_masterchain_block_set(&second).unwrap();
        }
        tokio::task::yield_now().await;

        let storage = RpcStorage::open(context, RpcTransactionPartitionsConfig::default()).unwrap();
        let reconciliation = storage.reconcile_startup(&first).unwrap();
        assert_eq!(reconciliation.effective_frontier, first);
        assert!(reconciliation.rebuild_current_state);
        storage.continue_lifecycle().unwrap();
        storage
            .publish_snapshot(&reconciliation.effective_frontier)
            .unwrap();
        assert_eq!(storage.load_snapshot().unwrap().visible_frontier(), &first);
        assert_eq!(storage.admit_block_set(&first).unwrap(), BlockSetMode::Same);
        storage
            .commit_masterchain_block_set_with_predecessor(&first, &masterchain_block(0))
            .unwrap();
        assert_eq!(storage.load_snapshot().unwrap().visible_frontier(), &first);
        assert_eq!(
            storage.partitions.lock().visible_frontier(),
            Some(&second),
        );
    }

    #[tokio::test]
    async fn block_set_admission_enforces_new_replay_same_and_predecessor_identity() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = RpcStorage::open(context, RpcTransactionPartitionsConfig::default()).unwrap();
        let zerostate = masterchain_block(0);
        publish_test_frontier(&storage, &zerostate);

        let first = masterchain_block(1);
        assert_eq!(storage.admit_block_set(&first).unwrap(), BlockSetMode::New);
        assert_eq!(storage.admit_block_set(&first).unwrap(), BlockSetMode::New);
        {
            let manager = storage.partitions.lock();
            insert_masterchain_commit(&manager, &first, 0);
        }
        let mut wrong_predecessor = zerostate;
        wrong_predecessor.root_hash = HashBytes([0xff; 32]);
        assert!(storage
            .commit_masterchain_block_set_with_predecessor(&first, &wrong_predecessor)
            .is_err());
        storage
            .commit_masterchain_block_set_with_predecessor(&first, &zerostate)
            .unwrap();
        assert_eq!(storage.load_snapshot().unwrap().visible_frontier(), &first);

        assert_eq!(storage.admit_block_set(&first).unwrap(), BlockSetMode::Same);
        storage
            .commit_masterchain_block_set_with_predecessor(&first, &wrong_predecessor)
            .unwrap();
        assert!(storage.admit_block_set(&masterchain_block(3)).is_err());

        let second = masterchain_block(2);
        let third = masterchain_block(3);
        {
            let mut manager = storage.partitions.lock();
            insert_masterchain_commit(&manager, &second, 0);
            manager.commit_masterchain_block_set(&second).unwrap();
            insert_masterchain_commit(&manager, &third, 0);
            manager.commit_masterchain_block_set(&third).unwrap();
        }
        assert_eq!(storage.admit_block_set(&second).unwrap(), BlockSetMode::Replay);
        storage
            .commit_masterchain_block_set_with_predecessor(&second, &first)
            .unwrap();
        assert_eq!(storage.load_snapshot().unwrap().visible_frontier(), &second);
    }

    #[tokio::test]
    async fn block_set_admission_rejects_missing_predecessor_commit_before_local_write() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = RpcStorage::open(context, RpcTransactionPartitionsConfig::default()).unwrap();
        let first = masterchain_block(1);
        publish_test_frontier(&storage, &first);
        let lease = storage.partitions.lock().active_lease();
        let key = codec::partition_commit_key(first.seqno, &first.as_short_id());
        let mut batch = rocksdb::WriteBatch::default();
        batch.delete_cf(&lease.partition_commits.cf(), key);
        lease
            .rocksdb()
            .write_opt(batch, lease.partition_commits.write_config())
            .unwrap();
        assert!(storage.admit_block_set(&masterchain_block(2)).is_err());
    }

    #[tokio::test]
    async fn same_boundary_admission_rejects_missing_or_malformed_current_commit() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = RpcStorage::open(context, RpcTransactionPartitionsConfig::default()).unwrap();
        let first = masterchain_block(1);
        publish_test_frontier(&storage, &first);
        let lease = storage.partitions.lock().active_lease();
        let key = codec::partition_commit_key(first.seqno, &first.as_short_id());
        let valid = lease.partition_commits.get(key).unwrap().unwrap().to_vec();
        let mut batch = rocksdb::WriteBatch::default();
        batch.delete_cf(&lease.partition_commits.cf(), key);
        lease
            .rocksdb()
            .write_opt(batch, lease.partition_commits.write_config())
            .unwrap();
        assert!(storage.admit_block_set(&first).is_err());
        lease.partition_commits.insert(key, [0]).unwrap();
        assert!(storage.admit_block_set(&first).is_err());
        lease.partition_commits.insert(key, valid).unwrap();
    }

    #[tokio::test]
    async fn block_set_admission_reclassifies_durable_new_boundary_as_replay() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = RpcStorage::open(context, RpcTransactionPartitionsConfig::default()).unwrap();
        let zerostate = masterchain_block(0);
        let first = masterchain_block(1);
        publish_test_frontier(&storage, &zerostate);
        assert_eq!(storage.admit_block_set(&first).unwrap(), BlockSetMode::New);
        {
            let mut manager = storage.partitions.lock();
            insert_masterchain_commit(&manager, &first, 0);
            manager.commit_masterchain_block_set(&first).unwrap();
        }
        assert_eq!(storage.admit_block_set(&first).unwrap(), BlockSetMode::Replay);
        storage
            .commit_masterchain_block_set_with_predecessor(&first, &zerostate)
            .unwrap();
        assert_eq!(storage.load_snapshot().unwrap().visible_frontier(), &first);
    }

    #[tokio::test]
    async fn replay_boundary_requires_existing_current_commit() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = RpcStorage::open(context, RpcTransactionPartitionsConfig::default()).unwrap();
        let first = masterchain_block(1);
        let second = masterchain_block(2);
        let third = masterchain_block(3);
        publish_test_frontier(&storage, &first);
        {
            let mut manager = storage.partitions.lock();
            insert_masterchain_commit(&manager, &second, 0);
            manager.commit_masterchain_block_set(&second).unwrap();
            insert_masterchain_commit(&manager, &third, 0);
            manager.commit_masterchain_block_set(&third).unwrap();
        }
        let lease = storage.partitions.lock().active_lease();
        let key = codec::partition_commit_key(second.seqno, &second.as_short_id());
        let mut batch = rocksdb::WriteBatch::default();
        batch.delete_cf(&lease.partition_commits.cf(), key);
        lease
            .rocksdb()
            .write_opt(batch, lease.partition_commits.write_config())
            .unwrap();
        assert_eq!(storage.admit_block_set(&second).unwrap(), BlockSetMode::Replay);
        assert!(storage
            .commit_masterchain_block_set_with_predecessor(&second, &first)
            .is_err());
    }

    #[tokio::test]
    async fn durable_boundary_survives_creation_activation_failure_and_retries_before_write() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = RpcStorage::open(context, test_partitions_config()).unwrap();
        let zerostate = masterchain_block(0);
        let first = masterchain_block(1);
        publish_test_frontier(&storage, &zerostate);
        assert_eq!(storage.admit_block_set(&first).unwrap(), BlockSetMode::New);
        {
            let mut manager = storage.partitions.lock();
            insert_masterchain_commit(&manager, &first, 1);
            manager.fail_next_creation_activation();
        }
        storage
            .commit_masterchain_block_set_with_predecessor(&first, &zerostate)
            .unwrap();
        let snapshot = storage.load_snapshot().unwrap();
        assert_eq!(snapshot.visible_frontier(), &first);
        assert!(snapshot
            .0
            .descriptors
            .iter()
            .all(|descriptor| descriptor.lifecycle != codec::ManifestLifecycle::Creating));
        drop(snapshot);
        assert!(storage.partitions.lock().has_creating_partition());

        assert_eq!(
            storage.admit_block_set(&masterchain_block(2)).unwrap(),
            BlockSetMode::New
        );
        assert!(!storage.partitions.lock().has_creating_partition());
    }

    #[tokio::test]
    async fn partition_aware_reads_cross_sealed_and_active_partitions() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = RpcStorage::open(
            context,
            RpcTransactionPartitionsConfig {
                target_lsm_bytes: 1,
                target_blob_bytes: u64::MAX,
                target_index_records: u64::MAX,
                max_open_sealed_partitions: 1,
                ..Default::default()
            },
        )
        .unwrap();
        let account = StdAddr::new(0, HashBytes::ZERO);
        let mc1 = masterchain_block(1);
        let block1 = basechain_block(1);
        let hash10 = HashBytes([10; 32]);
        let hash11 = HashBytes([11; 32]);
        let msg10 = HashBytes([110; 32]);
        let msg11 = HashBytes([111; 32]);
        let first_transactions = [
            ReadTestTransaction {
                lt: 10,
                hash: hash10,
                in_msg_hash: msg10,
                boc_byte: 10,
            },
            ReadTestTransaction {
                lt: 11,
                hash: hash11,
                in_msg_hash: msg11,
                boc_byte: 11,
            },
        ];
        let held_first_lease = storage.partitions.lock().active_lease();
        let first_id = insert_read_test_block(
            &storage,
            &account,
            &mc1,
            &block1,
            &first_transactions,
            1,
        );
        storage.commit_masterchain_block_set(&mc1).unwrap();
        let sealing_snapshot = storage.load_snapshot().unwrap();
        assert_eq!(
            storage
                .get_transaction(&hash10, Some(&sealing_snapshot))
                .unwrap()
                .unwrap()
                .as_ref(),
            [10]
        );
        drop(sealing_snapshot);
        drop(held_first_lease);
        wait_for_sealed_partition(&storage, first_id).await;

        let mc2 = masterchain_block(2);
        let block2 = basechain_block(2);
        let hash20 = HashBytes([20; 32]);
        let msg20 = HashBytes([120; 32]);
        let second_transactions = [ReadTestTransaction {
            lt: 20,
            hash: hash20,
            in_msg_hash: msg20,
            boc_byte: 20,
        }];
        let second_id = insert_read_test_block(
            &storage,
            &account,
            &mc2,
            &block2,
            &second_transactions,
            1,
        );
        storage.commit_masterchain_block_set(&mc2).unwrap();
        wait_for_sealed_partition(&storage, second_id).await;

        let mc3 = masterchain_block(3);
        let block3 = basechain_block(3);
        let hash30 = HashBytes([30; 32]);
        let msg30 = HashBytes([130; 32]);
        let third_transactions = [ReadTestTransaction {
            lt: 30,
            hash: hash30,
            in_msg_hash: msg30,
            boc_byte: 30,
        }];
        let third_id = insert_read_test_block(
            &storage,
            &account,
            &mc3,
            &block3,
            &third_transactions,
            0,
        );
        storage.commit_masterchain_block_set(&mc3).unwrap();
        assert_ne!(third_id, first_id);
        assert_ne!(third_id, second_id);

        assert_eq!(storage.get_known_mc_blocks_range(None).unwrap(), Some((1, 3)));
        let forward = storage
            .get_transactions(&account, None, None, false, None)
            .unwrap()
            .map_ext(|lt, _, _| Some(lt))
            .collect::<Vec<_>>();
        assert_eq!(forward, [10, 11, 20, 30]);
        let reverse = storage
            .get_transactions(&account, Some(11), Some(20), true, None)
            .unwrap()
            .map_ext(|lt, _, _| Some(lt))
            .collect::<Vec<_>>();
        assert_eq!(reverse, [20, 11]);
        let full_reverse = storage
            .get_transactions(&account, None, None, true, None)
            .unwrap()
            .map_ext(|lt, _, _| Some(lt))
            .collect::<Vec<_>>();
        assert_eq!(full_reverse, [30, 20, 11, 10]);
        let bounded_forward = storage
            .get_transactions(&account, Some(11), Some(30), false, None)
            .unwrap()
            .map_ext(|lt, _, _| Some(lt))
            .collect::<Vec<_>>();
        assert_eq!(bounded_forward, [11, 20, 30]);
        let bounded_reverse = storage
            .get_transactions(&account, Some(11), Some(30), true, None)
            .unwrap()
            .map_ext(|lt, _, _| Some(lt))
            .collect::<Vec<_>>();
        assert_eq!(bounded_reverse, [30, 20, 11]);

        for (hash, msg_hash, boc_byte, block_id) in [
            (hash10, msg10, 10, block1),
            (hash20, msg20, 20, block2),
            (hash30, msg30, 30, block3),
        ] {
            assert_eq!(
                storage
                    .get_transaction(&hash, None)
                    .unwrap()
                    .unwrap()
                    .as_ref(),
                [boc_byte]
            );
            assert_eq!(
                storage
                    .get_dst_transaction(&msg_hash, None)
                    .unwrap()
                    .unwrap()
                    .as_ref(),
                [boc_byte]
            );
            assert_eq!(
                storage
                    .get_transaction_info(&hash, None)
                    .unwrap()
                    .unwrap()
                    .block_id,
                block_id
            );
        }
        assert_eq!(
            storage
                .get_src_transaction(&account, 25, None)
                .unwrap()
                .unwrap()
                .as_ref(),
            [20]
        );
        assert_eq!(
            storage
                .get_src_transaction(&account, 11, None)
                .unwrap()
                .unwrap()
                .as_ref(),
            [10]
        );
        assert_eq!(
            storage
                .get_src_transaction(&account, 35, None)
                .unwrap()
                .unwrap()
                .as_ref(),
            [30]
        );

        let blocks = storage
            .get_blocks_by_mc_seqno(1, None)
            .unwrap()
            .unwrap()
            .collect::<Vec<_>>();
        assert_eq!(blocks.len(), 2);
        assert!(blocks.contains(&block1));
        assert!(blocks.contains(&mc1));
        storage.reset_known_block_point_reads();
        let (brief_id, brief_mc_seqno, brief) = storage
            .get_brief_block_info(&block1.as_short_id(), None)
            .unwrap()
            .unwrap();
        assert_eq!(brief_id, block1);
        assert_eq!(brief_mc_seqno, 1);
        assert_eq!(brief.tx_count, 2);
        assert_eq!(storage.known_block_point_reads(), 3);
        storage.reset_known_block_point_reads();
        let (masterchain_brief_id, masterchain_mc_seqno, _) = storage
            .get_brief_block_info(&mc1.as_short_id(), None)
            .unwrap()
            .unwrap();
        assert_eq!(masterchain_brief_id, mc1);
        assert_eq!(masterchain_mc_seqno, mc1.seqno);
        assert_eq!(storage.known_block_point_reads(), 1);
        let shards = storage
            .get_brief_shards_descr(1, None)
            .unwrap()
            .unwrap();
        assert_eq!(shards.len(), 1);
        assert_eq!(shards[0].shard_ident, ShardIdent::BASECHAIN);
        assert_eq!(shards[0].seqno, block1.seqno);

        storage.reset_known_block_point_reads();
        let block_transactions = storage
            .get_block_transactions(&block1.as_short_id(), false, None, None)
            .unwrap()
            .unwrap()
            .map(|_, lt, boc| Some((lt, boc[0])))
            .collect::<Vec<_>>();
        assert_eq!(block_transactions, [(10, 10), (11, 11)]);
        assert_eq!(storage.known_block_point_reads(), 3);
        let reverse_block_transactions = storage
            .get_block_transaction_ids(&block1.as_short_id(), true, None, None)
            .unwrap()
            .unwrap()
            .map(|id| id.lt)
            .collect::<Vec<_>>();
        assert_eq!(reverse_block_transactions, [11, 10]);
        let cursor = BlockTransactionsCursor {
            hash: account.address,
            lt: 10,
        };
        let after_cursor = storage
            .get_block_transaction_ids(
                &block1.as_short_id(),
                false,
                Some(&cursor),
                None,
            )
            .unwrap()
            .unwrap()
            .map(|id| id.lt)
            .collect::<Vec<_>>();
        assert_eq!(after_cursor, [11]);
        for (mc_seqno, block_id, mc_block_id, transaction_lt) in [
            (2, block2, mc2, 20),
            (3, block3, mc3, 30),
        ] {
            let blocks = storage
                .get_blocks_by_mc_seqno(mc_seqno, None)
                .unwrap()
                .unwrap()
                .collect::<Vec<_>>();
            assert_eq!(blocks.len(), 2);
            assert!(blocks.contains(&block_id));
            assert!(blocks.contains(&mc_block_id));
            assert_eq!(
                storage
                    .get_brief_block_info(&block_id.as_short_id(), None)
                    .unwrap()
                    .unwrap()
                    .0,
                block_id
            );
            let transactions = storage
                .get_block_transaction_ids(
                    &block_id.as_short_id(),
                    false,
                    None,
                    None,
                )
                .unwrap()
                .unwrap()
                .map(|id| id.lt)
                .collect::<Vec<_>>();
            assert_eq!(transactions, [transaction_lt]);
        }

        let mut retained_first_partition = storage
            .get_transactions(&account, None, None, false, None)
            .unwrap()
            .map_ext(|lt, _, _| Some(lt));
        assert_eq!(retained_first_partition.visited_partitions, 0);
        assert_eq!(retained_first_partition.next(), Some(10));
        assert_eq!(retained_first_partition.visited_partitions, 1);
        assert!(storage.get_transaction(&hash20, None).unwrap().is_some());
        {
            let manager = storage.partitions.lock();
            manager.run_sealed_cache_pending_tasks();
            assert!(manager.sealed_cache_entry_count() <= 1);
        }
        assert_eq!(retained_first_partition.next(), Some(11));
        assert_eq!(
            retained_first_partition.by_ref().collect::<Vec<_>>(),
            [20, 30]
        );
        assert_eq!(retained_first_partition.visited_partitions, 3);

        let mc4 = masterchain_block(4);
        let block4 = basechain_block(4);
        let future_hash = HashBytes([40; 32]);
        let future_msg = HashBytes([140; 32]);
        insert_read_test_block(
            &storage,
            &account,
            &mc4,
            &block4,
            &[ReadTestTransaction {
                lt: 40,
                hash: future_hash,
                in_msg_hash: future_msg,
                boc_byte: 40,
            }, ReadTestTransaction {
                lt: 41,
                hash: hash10,
                in_msg_hash: msg10,
                boc_byte: 41,
            }],
            0,
        );
        let active_lease = storage.partitions.lock().active_lease();
        active_lease
            .known_blocks
            .insert(
                known_block_key(&block1),
                known_block_value(&block1, &mc4, 10, 12, 2),
            )
            .unwrap();
        storage.publish_snapshot(&mc3).unwrap();
        assert!(storage.get_transaction(&future_hash, None).unwrap().is_none());
        assert!(storage
            .get_dst_transaction(&future_msg, None)
            .unwrap()
            .is_none());
        assert!(storage
            .get_brief_block_info(&block4.as_short_id(), None)
            .unwrap()
            .is_none());
        assert!(storage
            .get_block_transaction_ids(&block4.as_short_id(), false, None, None)
            .unwrap()
            .is_none());
        assert_eq!(
            storage.get_transaction(&hash10, None).unwrap().unwrap().as_ref(),
            [10]
        );
        assert_eq!(
            storage.get_dst_transaction(&msg10, None).unwrap().unwrap().as_ref(),
            [10]
        );
        assert_eq!(
            storage
                .get_brief_block_info(&block1.as_short_id(), None)
                .unwrap()
                .unwrap()
                .1,
            mc1.seqno
        );
        assert!(storage
            .get_blocks_by_mc_seqno(4, None)
            .unwrap()
            .is_none());
        assert_eq!(
            storage
                .get_src_transaction(&account, 50, None)
                .unwrap()
                .unwrap()
                .as_ref(),
            [30]
        );
        let visible_lts = storage
            .get_transactions(&account, None, None, false, None)
            .unwrap()
            .map_ext(|lt, _, _| Some(lt))
            .collect::<Vec<_>>();
        assert_eq!(visible_lts, [10, 11, 20, 30]);

        let active_lease = storage.partitions.lock().active_lease();
        let mut tx_info = active_lease
            .transactions_by_hash
            .get(hash30)
            .unwrap()
            .unwrap()
            .as_ref()
            .to_vec();
        tx_info[110..114].copy_from_slice(&2u32.to_le_bytes());
        active_lease
            .transactions_by_hash
            .insert(hash30, tx_info)
            .unwrap();
        storage.publish_snapshot(&mc3).unwrap();
        assert!(storage.get_transaction(&hash30, None).is_err());
    }

    #[tokio::test]
    async fn v2_local_acceptance_fixture_builds_recovers_and_measures_filters() {
        let mut config = test_partitions_config();
        config.max_open_sealed_partitions = 3;
        let sealed_cache_capacity = config.max_open_sealed_partitions;
        let exact_lookup_limit = config.filters.max_concurrent_sealed_exact_lookups;
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let root = context.root_dir().path().to_path_buf();
        let storage = RpcStorage::open(context.clone(), config.clone()).unwrap();
        storage.sealing_cancel.cancel();
        let account = StdAddr::new(0, HashBytes::ZERO);
        let mut sealed = Vec::new();
        for seqno in 1..=3 {
            let masterchain = masterchain_block(seqno);
            let block = basechain_block(seqno);
            let transaction = HashBytes([seqno as u8; 32]);
            let inbound = HashBytes([(seqno as u8).wrapping_add(100); 32]);
            let id = insert_read_test_block(
                &storage,
                &account,
                &masterchain,
                &block,
                &[ReadTestTransaction {
                    lt: seqno as u64 * 10,
                    hash: transaction,
                    in_msg_hash: inbound,
                    boc_byte: seqno as u8,
                }],
                1,
            );
            storage.commit_masterchain_block_set(&masterchain).unwrap();
            seal_partition(
                storage.partitions.clone(),
                storage.snapshots.clone(),
                storage.filter_registry.clone(),
                id,
                CancellationFlag::new(),
                None,
            )
            .await
            .unwrap();
            sealed.push((id, masterchain, block, transaction, inbound));
        }
        let active_masterchain = masterchain_block(4);
        let active_block = basechain_block(4);
        let active_transaction = HashBytes([4; 32]);
        let active_inbound = HashBytes([104; 32]);
        insert_read_test_block(
            &storage,
            &account,
            &active_masterchain,
            &active_block,
            &[ReadTestTransaction {
                lt: 40,
                hash: active_transaction,
                in_msg_hash: active_inbound,
                boc_byte: 4,
            }],
            0,
        );
        storage
            .commit_masterchain_block_set(&active_masterchain)
            .unwrap();
        assert_eq!(
            storage
                .partitions
                .lock()
                .descriptors()
                .into_iter()
                .filter(|descriptor| descriptor.lifecycle == codec::ManifestLifecycle::Sealed)
                .count(),
            3,
        );
        assert_eq!(
            storage
                .partitions
                .lock()
                .descriptors()
                .into_iter()
                .filter(|descriptor| descriptor.lifecycle == codec::ManifestLifecycle::Active)
                .count(),
            1,
        );

        let store = FilterCatalogStore::open(&context, config.filters.clone()).unwrap();
        let cancelled = CancellationFlag::new();
        let mut persisted_bytes = 0;
        let mut resident_bytes = 0;
        let mut build_elapsed = Duration::ZERO;
        let mut validation_elapsed = Duration::ZERO;
        let mut generations = BTreeMap::new();
        for (id, ..) in &sealed {
            let started_at = Instant::now();
            let bundle = store
                .build_and_publish(&storage.partitions, *id, &cancelled)
                .unwrap();
            build_elapsed += started_at.elapsed();
            let started_at = Instant::now();
            let loaded = store
                .load_and_validate_sealed(&storage.partitions, *id, &cancelled)
                .unwrap()
                .unwrap();
            validation_elapsed += started_at.elapsed();
            assert_eq!(loaded.generation_id(), bundle.generation_id());
            persisted_bytes += bundle
                .bundle_bytes()
                .checked_sub(bundle.resident_bytes())
                .unwrap();
            resident_bytes += bundle.resident_bytes();
            generations.insert(*id, bundle.generation_id());
            publish_filter_snapshot(
                &storage.partitions,
                &storage.snapshots,
                &storage.filter_registry,
                *id,
                bundle,
            )
            .unwrap();
        }
        let filtered = storage.load_snapshot().unwrap();
        for (id, _, block, transaction, inbound) in &sealed {
            assert!(filtered.filter_bundle(*id).is_some());
            assert!(storage
                .get_transaction(transaction, Some(&filtered))
                .unwrap()
                .is_some());
            assert!(storage
                .get_dst_transaction(inbound, Some(&filtered))
                .unwrap()
                .is_some());
            assert!(storage
                .get_brief_block_info(&block.as_short_id(), Some(&filtered))
                .unwrap()
                .is_some());
        }
        assert!(storage
            .get_transaction(&active_transaction, Some(&filtered))
            .unwrap()
            .is_some());
        assert!(storage
            .get_dst_transaction(&active_inbound, Some(&filtered))
            .unwrap()
            .is_some());
        assert!(storage
            .get_brief_block_info(&active_block.as_short_id(), Some(&filtered))
            .unwrap()
            .is_some());

        let fully_filtered_absent = (0u64..)
            .map(|value| {
                let mut bytes = [0xa5; 32];
                bytes[..8].copy_from_slice(&value.to_le_bytes());
                HashBytes(bytes)
            })
            .find(|hash| {
                sealed.iter().all(|(id, ..)| {
                    !filtered
                        .filter_might_contain(*id, FilterNamespace::Transactions, hash.as_slice())
                        .unwrap()
                })
            })
            .unwrap();
        storage
            .sealed_exact_lookup_acquisitions
            .store(0, Ordering::Release);
        let started_at = Instant::now();
        assert!(storage
            .get_transaction(&fully_filtered_absent, Some(&filtered))
            .unwrap()
            .is_none());
        let filtered_first_elapsed = started_at.elapsed();
        let started_at = Instant::now();
        assert!(storage
            .get_transaction(&fully_filtered_absent, Some(&filtered))
            .unwrap()
            .is_none());
        let filtered_warm_elapsed = started_at.elapsed();
        assert_eq!(
            storage
                .sealed_exact_lookup_acquisitions
                .load(Ordering::Acquire),
            0,
        );

        storage
            .sealed_exact_lookup_acquisitions
            .store(0, Ordering::Release);
        let recorder = TestMetricsRecorder::default();
        let probes = 100_000u64;
        let started_at = Instant::now();
        metrics::with_local_recorder(&recorder, || {
            for value in 0..probes {
                let mut bytes = [0x5a; 32];
                bytes[..8].copy_from_slice(&value.to_le_bytes());
                let hash = HashBytes(bytes);
                assert!(storage
                    .get_transaction(&hash, Some(&filtered))
                    .unwrap()
                    .is_none());
            }
        });
        let filtered_probe_elapsed = started_at.elapsed();
        let filter_checks = recorder.counter(
            "tycho_storage_rpc_filters_checked_total|namespace=transactions",
        );
        let false_positives = recorder.counter(
            "tycho_storage_rpc_filter_false_positives_total|namespace=transactions",
        );
        let filter_positives = recorder.counter(
            "tycho_storage_rpc_filter_positives_total|namespace=transactions",
        );
        let sealed_positive_probes = recorder.counter(
            "tycho_storage_rpc_exact_lookup_probes_total|namespace=transactions|lifecycle=sealed|reason=filter_positive",
        );
        let partitions_traversed = recorder.histogram_values(
            "tycho_storage_rpc_exact_lookup_partitions_traversed|namespace=transactions",
        );
        assert_eq!(filter_positives, false_positives);
        assert_eq!(sealed_positive_probes, false_positives);
        assert_eq!(filter_checks, probes * sealed.len() as u64);
        assert_eq!(partitions_traversed.len(), probes as usize);
        assert!(partitions_traversed.iter().all(|value| *value == 4.0));
        let observed_rate = false_positives as f64 / filter_checks as f64;
        let target_rate = config.filters.transaction_false_positive_rate_ppm as f64 / 1_000_000.0;
        let confidence_margin = 2.575_829_303_548_900_4
            * (target_rate * (1.0 - target_rate) / filter_checks as f64).sqrt();
        assert!(observed_rate <= target_rate * 2.0 + confidence_margin);
        let filtered_sealed_opens = storage
            .sealed_exact_lookup_acquisitions
            .load(Ordering::Acquire);
        assert!(filtered_sealed_opens <= false_positives);

        let removed_id = sealed[0].0;
        store.remove_descriptor_for_test(removed_id).unwrap();
        assert!(store
            .load_and_validate_sealed(&storage.partitions, removed_id, &cancelled)
            .unwrap()
            .is_none());
        drop(store);
        let corrupt_id = sealed[1].0;
        let corrupt_generation = generations[&corrupt_id];
        let corrupt_path = root
            .join("rpc/filters")
            .join(codec::format_filter_generation_directory(
                corrupt_id.0,
                corrupt_generation,
            ))
            .join(FilterNamespace::Transactions.file_name());
        let mut corrupted = std::fs::read(&corrupt_path).unwrap();
        *corrupted.last_mut().unwrap() ^= 1;
        std::fs::write(&corrupt_path, corrupted).unwrap();
        drop(filtered);
        drop(storage);
        tokio::task::yield_now().await;

        let direct_storage = RpcStorage::open(context.clone(), config.clone()).unwrap();
        let reconciliation = direct_storage.reconcile_startup(&active_masterchain).unwrap();
        direct_storage.continue_lifecycle().unwrap();
        direct_storage
            .publish_snapshot(&reconciliation.effective_frontier)
            .unwrap();
        let direct = direct_storage.load_snapshot().unwrap();
        assert!(sealed
            .iter()
            .all(|(id, ..)| direct.filter_bundle(*id).is_none()));
        let started_at = Instant::now();
        assert!(direct_storage
            .get_transaction(&fully_filtered_absent, Some(&direct))
            .unwrap()
            .is_none());
        let direct_cold_elapsed = started_at.elapsed();
        let started_at = Instant::now();
        assert!(direct_storage
            .get_transaction(&fully_filtered_absent, Some(&direct))
            .unwrap()
            .is_none());
        let direct_warm_elapsed = started_at.elapsed();
        let direct_cache_entries = {
            let manager = direct_storage.partitions.lock();
            manager.run_sealed_cache_pending_tasks();
            manager.sealed_cache_entry_count()
        };
        assert_eq!(direct_cache_entries, sealed_cache_capacity as u64);
        for (_, _, block, transaction, inbound) in &sealed {
            assert!(direct_storage
                .get_transaction(transaction, Some(&direct))
                .unwrap()
                .is_some());
            assert!(direct_storage
                .get_dst_transaction(inbound, Some(&direct))
                .unwrap()
                .is_some());
            assert!(direct_storage
                .get_brief_block_info(&block.as_short_id(), Some(&direct))
                .unwrap()
                .is_some());
        }
        drop(direct);
        drop(direct_storage);
        tokio::task::yield_now().await;

        let storage = RpcStorage::open(context, config).unwrap();
        let reconciliation = storage.reconcile_startup(&active_masterchain).unwrap();
        storage.continue_lifecycle().unwrap();
        storage
            .publish_snapshot(&reconciliation.effective_frontier)
            .unwrap();
        storage.start_filter_worker();
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let snapshot = storage.load_snapshot().unwrap();
                if sealed
                    .iter()
                    .all(|(id, ..)| snapshot.filter_bundle(*id).is_some())
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let restarted = storage.load_snapshot().unwrap();
        assert_ne!(
            restarted.filter_bundle(removed_id).unwrap().generation_id(),
            generations[&removed_id],
        );
        assert_ne!(
            restarted.filter_bundle(corrupt_id).unwrap().generation_id(),
            corrupt_generation,
        );
        let valid_id = sealed[2].0;
        assert_eq!(
            restarted.filter_bundle(valid_id).unwrap().generation_id(),
            generations[&valid_id],
        );
        for (_, _, block, transaction, inbound) in &sealed {
            assert!(storage
                .get_transaction(transaction, Some(&restarted))
                .unwrap()
                .is_some());
            assert!(storage
                .get_dst_transaction(inbound, Some(&restarted))
                .unwrap()
                .is_some());
            assert!(storage
                .get_brief_block_info(&block.as_short_id(), Some(&restarted))
                .unwrap()
                .is_some());
        }
        eprintln!(
            "VT15 fixture: sealed=3 active=1 sealed_cache_capacity={sealed_cache_capacity} sealed_cache_entries={direct_cache_entries} exact_lookup_limit={} resident_bytes={resident_bytes} persisted_bytes={persisted_bytes} build_ms={} validation_ms={} filtered_first_us={} filtered_warm_us={} filtered_100k_ms={} filter_checks={filter_checks} partitions_traversed=4/request false_positives={false_positives} observed_rate={observed_rate:.9} confidence_margin={confidence_margin:.9} filtered_sealed_opens={filtered_sealed_opens} direct_cold_us={} direct_warm_us={}",
            exact_lookup_limit,
            build_elapsed.as_millis(),
            validation_elapsed.as_millis(),
            filtered_first_elapsed.as_micros(),
            filtered_warm_elapsed.as_micros(),
            filtered_probe_elapsed.as_millis(),
            direct_cold_elapsed.as_micros(),
            direct_warm_elapsed.as_micros(),
        );
    }

    #[tokio::test]
    async fn immutable_snapshots_use_optional_partition_filters_without_advancing_visibility() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = RpcStorage::open(
            context,
            RpcTransactionPartitionsConfig {
                target_lsm_bytes: 1,
                target_blob_bytes: u64::MAX,
                target_index_records: u64::MAX,
                max_open_sealed_partitions: 1,
                ..Default::default()
            },
        )
        .unwrap();
        let account = StdAddr::new(0, HashBytes::ZERO);
        let mc1 = masterchain_block(1);
        let block1 = basechain_block(1);
        let transaction_hash = HashBytes([10; 32]);
        let inbound_message_hash = HashBytes([110; 32]);
        let partition_id = insert_read_test_block(
            &storage,
            &account,
            &mc1,
            &block1,
            &[ReadTestTransaction {
                lt: 10,
                hash: transaction_hash,
                in_msg_hash: inbound_message_hash,
                boc_byte: 10,
            }],
            1,
        );
        storage.commit_masterchain_block_set(&mc1).unwrap();
        wait_for_sealed_partition(&storage, partition_id).await;

        let local_only = storage.load_snapshot().unwrap();
        assert!(local_only.filter_bundle(partition_id).is_none());
        let manifest_digest = storage
            .partitions
            .lock()
            .sealed_manifest_digest(partition_id)
            .unwrap();
        let transaction_positive_without_record = HashBytes([210; 32]);
        let inbound_positive_without_record = HashBytes([211; 32]);
        let block_positive_without_record = basechain_block(210);
        let mismatched_bundle = test_filter_bundle(
            partition_id,
            0,
            HashBytes::ZERO,
            &[],
            &[],
            &[],
        );
        assert!(publish_filter_snapshot(
            &storage.partitions,
            &storage.snapshots,
            &storage.filter_registry,
            partition_id,
            mismatched_bundle,
        )
        .is_err());
        let after_failed_publication = storage.load_snapshot().unwrap();
        assert!(Arc::ptr_eq(&local_only.0, &after_failed_publication.0));
        assert!(after_failed_publication.filter_bundle(partition_id).is_none());
        assert!(storage.filter_registry.get(partition_id).is_none());
        let first_bundle = test_filter_bundle(
            partition_id,
            1,
            manifest_digest,
            &[transaction_hash, transaction_positive_without_record],
            &[inbound_message_hash, inbound_positive_without_record],
            &[
                known_block_key(&block1),
                known_block_key(&block_positive_without_record),
            ],
        );
        publish_filter_snapshot(
            &storage.partitions,
            &storage.snapshots,
            &storage.filter_registry,
            partition_id,
            first_bundle.clone(),
        )
        .unwrap();
        let first_filtered = storage.load_snapshot().unwrap();
        assert!(Arc::ptr_eq(&local_only.0, &first_filtered.0));
        assert_eq!(first_filtered.visible_frontier(), local_only.visible_frontier());
        assert_eq!(first_filtered.manifest_epoch(), local_only.manifest_epoch());
        assert_eq!(
            first_filtered
                .filter_bundle(partition_id)
                .unwrap()
                .generation_id(),
            1
        );

        let transaction_negative = (1u8..=u8::MAX)
            .map(|byte| HashBytes([byte; 32]))
            .find(|hash| {
                !first_bundle
                    .filter(FilterNamespace::Transactions)
                    .contains(hash.as_slice())
                    .unwrap()
            })
            .unwrap();
        let inbound_negative = (1u8..=u8::MAX)
            .rev()
            .map(|byte| HashBytes([byte; 32]))
            .find(|hash| {
                !first_bundle
                    .filter(FilterNamespace::InboundMessages)
                    .contains(hash.as_slice())
                    .unwrap()
            })
            .unwrap();
        let block_negative = (220..u32::MAX)
            .map(basechain_block)
            .find(|block_id| {
                !first_bundle
                    .filter(FilterNamespace::Blocks)
                    .contains(&known_block_key(block_id))
                    .unwrap()
            })
            .unwrap();
        let sealed_cache_entries_before = {
            let manager = storage.partitions.lock();
            manager.run_sealed_cache_pending_tasks();
            manager.sealed_cache_entry_count()
        };
        assert!(storage
            .get_transaction(&transaction_negative, Some(&first_filtered))
            .unwrap()
            .is_none());
        assert!(storage
            .get_dst_transaction(&inbound_negative, Some(&first_filtered))
            .unwrap()
            .is_none());
        assert!(storage
            .get_brief_block_info(&block_negative.as_short_id(), Some(&first_filtered))
            .unwrap()
            .is_none());
        {
            let manager = storage.partitions.lock();
            manager.run_sealed_cache_pending_tasks();
            assert_eq!(manager.sealed_cache_entry_count(), sealed_cache_entries_before);
        }

        assert_eq!(
            storage
                .get_transaction(&transaction_hash, Some(&local_only))
                .unwrap()
                .unwrap()
                .as_ref(),
            [10]
        );
        assert_eq!(
            storage
                .get_dst_transaction(&inbound_message_hash, Some(&local_only))
                .unwrap()
                .unwrap()
                .as_ref(),
            [10]
        );
        assert!(storage
            .get_brief_block_info(&block1.as_short_id(), Some(&local_only))
            .unwrap()
            .is_some());
        assert_eq!(
            storage
                .get_transaction(&transaction_hash, Some(&first_filtered))
                .unwrap()
                .unwrap()
                .as_ref(),
            [10]
        );
        assert_eq!(
            storage
                .get_dst_transaction(&inbound_message_hash, Some(&first_filtered))
                .unwrap()
                .unwrap()
                .as_ref(),
            [10]
        );
        assert!(storage
            .get_brief_block_info(&block1.as_short_id(), Some(&first_filtered))
            .unwrap()
            .is_some());
        assert!(storage
            .get_transaction(
                &transaction_positive_without_record,
                Some(&first_filtered),
            )
            .unwrap()
            .is_none());
        assert!(storage
            .get_dst_transaction(
                &inbound_positive_without_record,
                Some(&first_filtered),
            )
            .unwrap()
            .is_none());
        assert!(storage
            .get_brief_block_info(
                &block_positive_without_record.as_short_id(),
                Some(&first_filtered),
            )
            .unwrap()
            .is_none());

        let future_hash = HashBytes([30; 32]);
        insert_read_test_block(
            &storage,
            &account,
            &masterchain_block(2),
            &basechain_block(2),
            &[ReadTestTransaction {
                lt: 20,
                hash: future_hash,
                in_msg_hash: HashBytes([130; 32]),
                boc_byte: 20,
            }],
            0,
        );
        let second_bundle = test_filter_bundle(
            partition_id,
            2,
            manifest_digest,
            &[transaction_hash],
            &[inbound_message_hash],
            &[known_block_key(&block1)],
        );
        publish_filter_snapshot(
            &storage.partitions,
            &storage.snapshots,
            &storage.filter_registry,
            partition_id,
            second_bundle,
        )
        .unwrap();
        let second_filtered = storage.load_snapshot().unwrap();
        assert!(Arc::ptr_eq(&first_filtered.0, &second_filtered.0));
        assert_eq!(second_filtered.visible_frontier(), &mc1);
        assert_eq!(second_filtered.manifest_epoch(), first_filtered.manifest_epoch());
        assert_eq!(
            first_filtered
                .filter_bundle(partition_id)
                .unwrap()
                .generation_id(),
            1
        );
        assert_eq!(
            second_filtered
                .filter_bundle(partition_id)
                .unwrap()
                .generation_id(),
            2
        );
        assert!(storage
            .get_transaction(&future_hash, Some(&second_filtered))
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn concurrent_filter_frontier_and_sealing_publications_complete_in_lock_order() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = Arc::new(RpcStorage::open(context, test_partitions_config()).unwrap());
        storage.sealing_cancel.cancel();
        let account = StdAddr::new(0, HashBytes::ZERO);
        let mc1 = masterchain_block(1);
        let block1 = basechain_block(1);
        let first_id = insert_read_test_block(
            &storage,
            &account,
            &mc1,
            &block1,
            &[ReadTestTransaction {
                lt: 10,
                hash: HashBytes([10; 32]),
                in_msg_hash: HashBytes([110; 32]),
                boc_byte: 10,
            }],
            1,
        );
        storage.commit_masterchain_block_set(&mc1).unwrap();
        seal_partition(
            storage.partitions.clone(),
            storage.snapshots.clone(),
            storage.filter_registry.clone(),
            first_id,
            CancellationFlag::new(),
            None,
        )
        .await
        .unwrap();
        let first_digest = storage
            .partitions
            .lock()
            .sealed_manifest_digest(first_id)
            .unwrap();

        let mc2 = masterchain_block(2);
        let block2 = basechain_block(2);
        let second_id = insert_read_test_block(
            &storage,
            &account,
            &mc2,
            &block2,
            &[ReadTestTransaction {
                lt: 20,
                hash: HashBytes([20; 32]),
                in_msg_hash: HashBytes([120; 32]),
                boc_byte: 20,
            }],
            1,
        );
        storage.commit_masterchain_block_set(&mc2).unwrap();
        let held_snapshot = storage.load_snapshot().unwrap();
        let barrier = Arc::new(Barrier::new(4));
        let filter_storage = storage.clone();
        let filter_barrier = barrier.clone();
        let filter_publication = tokio::task::spawn_blocking(move || {
            filter_barrier.wait();
            publish_filter_snapshot(
                &filter_storage.partitions,
                &filter_storage.snapshots,
                &filter_storage.filter_registry,
                first_id,
                test_filter_bundle(first_id, 1, first_digest, &[], &[], &[]),
            )
        });
        let frontier_storage = storage.clone();
        let frontier_barrier = barrier.clone();
        let frontier_publication = tokio::task::spawn_blocking(move || {
            frontier_barrier.wait();
            frontier_storage.publish_snapshot(&mc2)
        });
        let sealing_storage = storage.clone();
        let sealing_barrier = barrier.clone();
        let runtime = tokio::runtime::Handle::current();
        let sealing_publication = tokio::task::spawn_blocking(move || {
            sealing_barrier.wait();
            runtime.block_on(seal_partition(
                sealing_storage.partitions.clone(),
                sealing_storage.snapshots.clone(),
                sealing_storage.filter_registry.clone(),
                second_id,
                CancellationFlag::new(),
                None,
            ))
        });
        barrier.wait();
        tokio::time::timeout(Duration::from_secs(5), async {
            filter_publication.await.unwrap().unwrap();
            frontier_publication.await.unwrap().unwrap();
            drop(held_snapshot);
            sealing_publication.await.unwrap().unwrap();
        })
        .await
        .unwrap();
        let snapshot = storage.load_snapshot().unwrap();
        assert_eq!(snapshot.visible_frontier(), &mc2);
        assert_eq!(
            snapshot.filter_bundle(first_id).unwrap().generation_id(),
            1,
        );
        assert_eq!(
            storage
                .partitions
                .lock()
                .descriptors()
                .into_iter()
                .find(|descriptor| descriptor.id == second_id)
                .unwrap()
                .lifecycle,
            codec::ManifestLifecycle::Sealed,
        );
    }

    #[test]
    fn sealed_exact_lookup_context_acquires_once_and_reports_availability() {
        let semaphore = Arc::new(Semaphore::new(1));
        let acquisitions = Arc::new(AtomicU64::new(0));
        let mut first =
            SealedExactLookupContext::new(
                semaphore.clone(),
                Arc::new(SealedExactLookupMetrics::default()),
                FilterNamespace::Transactions,
                acquisitions.clone(),
            );
        first.acquire().unwrap();
        first.acquire().unwrap();
        assert_eq!(semaphore.available_permits(), 0);
        assert_eq!(acquisitions.load(Ordering::Acquire), 1);

        let mut second =
            SealedExactLookupContext::new(
                semaphore.clone(),
                Arc::new(SealedExactLookupMetrics::default()),
                FilterNamespace::Transactions,
                acquisitions.clone(),
            );
        assert_sealed_exact_lookup_error(
            second.acquire(),
            SealedExactLookupAvailabilityError::Overloaded,
        );
        drop(first);
        second.acquire().unwrap();
        assert_eq!(acquisitions.load(Ordering::Acquire), 2);
        drop(second);
        assert_eq!(semaphore.available_permits(), 1);

        semaphore.close();
        let mut closed = SealedExactLookupContext::new(
            semaphore,
            Arc::new(SealedExactLookupMetrics::default()),
            FilterNamespace::Transactions,
            acquisitions,
        );
        assert_sealed_exact_lookup_error(
            closed.acquire(),
            SealedExactLookupAvailabilityError::Closed,
        );
    }

    #[test]
    fn observability_records_request_traversal_permits_and_bounded_labels() {
        let recorder = TestMetricsRecorder::default();
        let semaphore = Arc::new(Semaphore::new(1));
        let state = Arc::new(SealedExactLookupMetrics::default());
        let acquisitions = Arc::new(AtomicU64::new(0));

        metrics::with_local_recorder(&recorder, || {
            let mut hit = SealedExactLookupContext::new(
                semaphore.clone(),
                state.clone(),
                FilterNamespace::Transactions,
                acquisitions.clone(),
            );
            hit.consider_partition();
            hit.record_filter(true);
            hit.record_exact_probe("sealed", "filter_positive");
            hit.acquire().unwrap();
            hit.record_hit("sealed");

            let mut overloaded = SealedExactLookupContext::new(
                semaphore.clone(),
                state.clone(),
                FilterNamespace::Transactions,
                acquisitions.clone(),
            );
            assert_sealed_exact_lookup_error(
                overloaded.acquire(),
                SealedExactLookupAvailabilityError::Overloaded,
            );
            drop(overloaded);

            let permit = hit.into_permit();
            assert_eq!(
                recorder.gauge(
                    "tycho_storage_rpc_sealed_exact_lookup_permits|result=current"
                ),
                1.0
            );
            drop(permit);

            let mut miss = SealedExactLookupContext::new(
                semaphore,
                state,
                FilterNamespace::Transactions,
                acquisitions,
            );
            miss.consider_partition();
            miss.record_filter(false);
            miss.consider_partition();
            miss.record_filter(true);
            miss.record_exact_probe("sealed", "filter_positive");
            miss.record_false_positive();
            miss.record_miss();
        });

        assert_eq!(
            recorder.gauge("tycho_storage_rpc_sealed_exact_lookup_permits|result=current"),
            0.0
        );
        assert_eq!(
            recorder.gauge("tycho_storage_rpc_sealed_exact_lookup_permits|result=peak"),
            1.0
        );
        assert_eq!(
            recorder.counter(
                "tycho_storage_rpc_sealed_exact_lookup_overloads_total|reason=exhausted"
            ),
            1
        );
        assert_eq!(
            recorder.counter(
                "tycho_storage_rpc_filter_negative_skips_total|namespace=transactions"
            ),
            1
        );
        assert_eq!(
            recorder.counter(
                "tycho_storage_rpc_filter_positives_total|namespace=transactions"
            ),
            2
        );
        assert_eq!(
            recorder.counter(
                "tycho_storage_rpc_filter_false_positives_total|namespace=transactions"
            ),
            1
        );
        assert_eq!(
            recorder.gauge("tycho_storage_rpc_filter_observed_error_ratio|namespace=transactions"),
            0.5
        );
        assert_eq!(
            recorder.counter(
                "tycho_storage_rpc_exact_lookup_hits_total|namespace=transactions|lifecycle=sealed"
            ),
            1
        );
        assert_eq!(
            recorder.histogram_len(
                "tycho_storage_rpc_exact_lookup_duration_seconds|namespace=transactions|result=miss"
            ),
            1
        );
        for key in recorder.keys() {
            let labels = key.split_once('|').map_or("", |(_, labels)| labels);
            for label in labels.split('|').filter(|label| !label.is_empty()) {
                let label_key = label.split_once('=').unwrap().0;
                assert!(matches!(
                    label_key,
                    "namespace" | "lifecycle" | "result" | "stage" | "reason"
                ));
            }
            assert!(!labels.contains("partition"));
            assert!(!labels.contains("generation"));
            assert!(!labels.contains("hash"));
            assert!(!labels.contains("path"));
            assert!(!labels.contains("seqno"));
            assert!(!key.contains("router"));
        }
    }

    #[tokio::test]
    async fn sealed_exact_lookup_backpressure_preserves_lookup_semantics() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let mut config = RpcTransactionPartitionsConfig {
            target_lsm_bytes: 1,
            target_blob_bytes: u64::MAX,
            target_index_records: u64::MAX,
            max_open_sealed_partitions: 1,
            ..Default::default()
        };
        config.filters.max_concurrent_sealed_exact_lookups = 1;
        let storage = RpcStorage::open(context, config).unwrap();
        assert_eq!(storage.sealed_exact_lookup_semaphore.available_permits(), 1);
        let account = StdAddr::new(0, HashBytes::ZERO);

        let mc1 = masterchain_block(1);
        let block1 = basechain_block(1);
        let hash10 = HashBytes([10; 32]);
        let msg10 = HashBytes([110; 32]);
        let held_first_lease = storage.partitions.lock().active_lease();
        let first_id = insert_read_test_block(
            &storage,
            &account,
            &mc1,
            &block1,
            &[ReadTestTransaction {
                lt: 10,
                hash: hash10,
                in_msg_hash: msg10,
                boc_byte: 10,
            }],
            1,
        );
        storage.commit_masterchain_block_set(&mc1).unwrap();
        let sealing_snapshot = storage.load_snapshot().unwrap();
        let saturated = storage
            .sealed_exact_lookup_semaphore
            .clone()
            .try_acquire_owned()
            .unwrap();
        assert!(storage
            .get_transaction(&hash10, Some(&sealing_snapshot))
            .unwrap()
            .is_some());
        assert!(storage
            .get_dst_transaction(&msg10, Some(&sealing_snapshot))
            .unwrap()
            .is_some());
        assert!(storage
            .get_brief_block_info(&block1.as_short_id(), Some(&sealing_snapshot))
            .unwrap()
            .is_some());
        drop(saturated);
        drop(sealing_snapshot);
        drop(held_first_lease);
        wait_for_sealed_partition(&storage, first_id).await;

        let mc2 = masterchain_block(2);
        let block2 = basechain_block(2);
        let hash20 = HashBytes([20; 32]);
        let msg20 = HashBytes([120; 32]);
        let second_id = insert_read_test_block(
            &storage,
            &account,
            &mc2,
            &block2,
            &[ReadTestTransaction {
                lt: 20,
                hash: hash20,
                in_msg_hash: msg20,
                boc_byte: 20,
            }],
            1,
        );
        storage.commit_masterchain_block_set(&mc2).unwrap();
        wait_for_sealed_partition(&storage, second_id).await;
        assert_ne!(first_id, second_id);

        let mc3 = masterchain_block(3);
        let block3 = basechain_block(3);
        let hash30 = HashBytes([30; 32]);
        let msg30 = HashBytes([130; 32]);
        insert_read_test_block(
            &storage,
            &account,
            &mc3,
            &block3,
            &[ReadTestTransaction {
                lt: 30,
                hash: hash30,
                in_msg_hash: msg30,
                boc_byte: 30,
            }],
            0,
        );
        storage.commit_masterchain_block_set(&mc3).unwrap();
        let local_only = storage.load_snapshot().unwrap();

        let acquisitions_before = storage
            .sealed_exact_lookup_acquisitions
            .load(Ordering::Acquire);
        assert!(storage
            .get_transaction(&hash10, Some(&local_only))
            .unwrap()
            .is_some());
        assert_eq!(
            storage
                .sealed_exact_lookup_acquisitions
                .load(Ordering::Acquire)
                - acquisitions_before,
            1,
        );

        let block_iter = storage
            .get_block_transaction_ids(&block1.as_short_id(), false, None, Some(local_only.clone()))
            .unwrap()
            .unwrap();
        assert_eq!(storage.sealed_exact_lookup_semaphore.available_permits(), 0);
        assert_sealed_exact_lookup_error(
            storage.get_transaction(&hash20, Some(&local_only)),
            SealedExactLookupAvailabilityError::Overloaded,
        );
        drop(block_iter);
        assert_eq!(storage.sealed_exact_lookup_semaphore.available_permits(), 1);

        let saturated = storage
            .sealed_exact_lookup_semaphore
            .clone()
            .try_acquire_owned()
            .unwrap();
        assert!(storage
            .get_transaction(&hash30, Some(&local_only))
            .unwrap()
            .is_some());
        assert!(storage
            .get_dst_transaction(&msg30, Some(&local_only))
            .unwrap()
            .is_some());
        assert!(storage
            .get_brief_block_info(&block3.as_short_id(), Some(&local_only))
            .unwrap()
            .is_some());
        assert_sealed_exact_lookup_error(
            storage.get_transaction(&hash10, Some(&local_only)),
            SealedExactLookupAvailabilityError::Overloaded,
        );
        assert_sealed_exact_lookup_error(
            storage.get_dst_transaction(&msg10, Some(&local_only)),
            SealedExactLookupAvailabilityError::Overloaded,
        );
        assert_sealed_exact_lookup_error(
            storage.get_brief_block_info(&block1.as_short_id(), Some(&local_only)),
            SealedExactLookupAvailabilityError::Overloaded,
        );

        for id in [first_id, second_id] {
            let manifest_digest = storage
                .partitions
                .lock()
                .sealed_manifest_digest(id)
                .unwrap();
            publish_filter_snapshot(
                &storage.partitions,
                &storage.snapshots,
                &storage.filter_registry,
                id,
                test_filter_bundle(id, 1, manifest_digest, &[], &[], &[]),
            )
            .unwrap();
        }
        let negative_filtered = storage.load_snapshot().unwrap();
        assert!(storage
            .get_transaction(&HashBytes([200; 32]), Some(&negative_filtered))
            .unwrap()
            .is_none());
        assert!(storage
            .get_dst_transaction(&HashBytes([201; 32]), Some(&negative_filtered))
            .unwrap()
            .is_none());
        assert!(storage
            .get_brief_block_info(
                &basechain_block(200).as_short_id(),
                Some(&negative_filtered),
            )
            .unwrap()
            .is_none());

        let first_manifest_digest = storage
            .partitions
            .lock()
            .sealed_manifest_digest(first_id)
            .unwrap();
        let positive_transaction_without_record = HashBytes([210; 32]);
        let positive_inbound_without_record = HashBytes([211; 32]);
        let positive_block_without_record = basechain_block(210);
        publish_filter_snapshot(
            &storage.partitions,
            &storage.snapshots,
            &storage.filter_registry,
            first_id,
            test_filter_bundle(
                first_id,
                2,
                first_manifest_digest,
                &[hash10, positive_transaction_without_record],
                &[msg10, positive_inbound_without_record],
                &[
                    known_block_key(&block1),
                    known_block_key(&positive_block_without_record),
                ],
            ),
        )
        .unwrap();
        let positive_filtered = storage.load_snapshot().unwrap();
        assert_sealed_exact_lookup_error(
            storage.get_transaction(&hash10, Some(&positive_filtered)),
            SealedExactLookupAvailabilityError::Overloaded,
        );
        assert_sealed_exact_lookup_error(
            storage.get_dst_transaction(&msg10, Some(&positive_filtered)),
            SealedExactLookupAvailabilityError::Overloaded,
        );
        assert_sealed_exact_lookup_error(
            storage.get_brief_block_info(&block1.as_short_id(), Some(&positive_filtered)),
            SealedExactLookupAvailabilityError::Overloaded,
        );
        assert_sealed_exact_lookup_error(
            storage.get_transaction(
                &positive_transaction_without_record,
                Some(&positive_filtered),
            ),
            SealedExactLookupAvailabilityError::Overloaded,
        );
        assert_sealed_exact_lookup_error(
            storage.get_dst_transaction(
                &positive_inbound_without_record,
                Some(&positive_filtered),
            ),
            SealedExactLookupAvailabilityError::Overloaded,
        );
        assert_sealed_exact_lookup_error(
            storage.get_brief_block_info(
                &positive_block_without_record.as_short_id(),
                Some(&positive_filtered),
            ),
            SealedExactLookupAvailabilityError::Overloaded,
        );
        drop(saturated);
        assert!(storage
            .get_transaction(&hash10, Some(&positive_filtered))
            .unwrap()
            .is_some());
        storage.reset_known_block_point_reads();
        assert!(storage
            .get_brief_block_info(&mc1.as_short_id(), Some(&positive_filtered))
            .unwrap()
            .is_some());
        assert_eq!(storage.known_block_point_reads(), 1);
    }

    #[tokio::test]
    async fn published_snapshot_hides_future_and_live_records() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = RpcStorage::open(context, RpcTransactionPartitionsConfig::default()).unwrap();
        let account = StdAddr::new(0, HashBytes::ZERO);
        assert!(storage
            .get_transactions(&account, None, None, false, None)
            .is_err());

        let tx_hash = HashBytes([1; 32]);
        let mut tx_key = [0; tables::Transactions::KEY_LEN];
        tx_key[33..41].copy_from_slice(&1u64.to_be_bytes());
        let mut payload = vec![0];
        payload.extend_from_slice(tx_hash.as_slice());
        payload.push(0);
        let value = codec::encode_transaction_value(1, &payload).unwrap();
        let mut tx_info = [0; tables::TransactionsByHash::VALUE_FULL_LEN];
        tx_info[33..41].copy_from_slice(&1u64.to_be_bytes());
        tx_info[110..114].copy_from_slice(&1u32.to_le_bytes());
        {
            let manager = storage.partitions.lock();
            manager.active_db().transactions.insert(tx_key, value).unwrap();
            manager
                .active_db()
                .transactions_by_hash
                .insert(tx_hash, tx_info)
                .unwrap();
        }

        let zerostate = masterchain_block(0);
        storage.publish_snapshot(&zerostate).unwrap();
        assert!(storage.get_transaction(&tx_hash, None).unwrap().is_none());
        assert!(storage
            .get_transactions(&account, None, None, false, None)
            .unwrap()
            .map(|_| Some(()))
            .next()
            .is_none());

        let code_hash = HashBytes([2; 32]);
        let mut code_hash_key = [0; tables::CodeHashes::KEY_LEN];
        code_hash_key[..32].copy_from_slice(code_hash.as_slice());
        storage
            .current_state
            .code_hashes
            .insert(code_hash_key, [])
            .unwrap();
        assert!(storage
            .get_accounts_by_code_hash(&code_hash, None, None)
            .unwrap()
            .next()
            .is_none());
    }
}
