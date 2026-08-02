use std::collections::BTreeMap;
use std::sync::{Arc, Weak};
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

use crate::config::{
    RpcTransactionMaintenanceConfig, RpcTransactionPartitionsConfig, TransactionsGcConfig,
};

use super::db::{RpcCurrentStateDb, RpcTransactionsDb};
use super::filter::{FilterNamespace, FilterRegistry, FilterWorker, ValidatedFilterBundle};
use super::gc::GcStateMachine;
#[cfg(test)]
use super::gc::GcDeletionFailureStage;
use super::maintenance::{MaintenanceCoordinator, MaintenancePermit, MaintenancePriority};
use super::partition::{PartitionDescriptor, PartitionId, PartitionManager, PartitionReadLease, SealedPartitionLeaseOpener};
use super::tail::{
    AccountTailDelta, AuthoritativeErrorKind, TailPromotedTransaction, TailRequestSnapshot,
    TailStore, TailTransactionRecord, classify_authoritative_error,
    conflicting_authoritative_error, malformed_authoritative_error,
    missing_authoritative_error,
};
use super::tables;
use super::codec::{self, AccountKey};

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
    tail: Arc<TailStore>,
    block_set_admission: Mutex<Option<BlockSetAdmission>>,
    current_state: RpcCurrentStateDb,
    min_tx_lt: AtomicU64,
    min_tx_lt_guard: tokio::sync::Mutex<()>,
    snapshots: Arc<SnapshotPublisher>,
    sealing_notify: Arc<Notify>,
    gc_notify: Arc<Notify>,
    gc: Arc<GcStateMachine>,
    sealing_cancel: CancellationFlag,
    sealing_task: Option<JoinHandle<()>>,
    maintenance: Arc<MaintenanceCoordinator>,
    maintenance_config: RpcTransactionMaintenanceConfig,
    gc_config: Option<TransactionsGcConfig>,
    gc_cursor_observation: Mutex<Option<GcCursorObservation>>,
    filter_registry: Arc<FilterRegistry>,
    filter_worker: Arc<FilterWorker>,
    sealed_exact_lookup_semaphore: Arc<Semaphore>,
    sealed_exact_lookup_metrics: Arc<SealedExactLookupMetrics>,
    initial_snapshot_recorded: AtomicBool,
    #[cfg(test)]
    known_block_point_reads: AtomicU64,
    #[cfg(test)]
    sealed_exact_lookup_acquisitions: Arc<AtomicU64>,
    #[cfg(test)]
    gc_evacuation_failure: Mutex<Option<GcEvacuationFailureStage>>,
}

#[derive(Clone, Copy)]
struct BlockSetAdmission {
    frontier: BlockId,
    block_set: BlockId,
    mode: BlockSetMode,
}

#[derive(Clone, Copy)]
struct GcCursorObservation {
    operation_id: u128,
    cursor: codec::TailProgressCursor,
    observed_at: Instant,
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
pub(super) struct SnapshotPublisher {
    /// Keeps publication and sealing lease removal in one lock order.
    current: RwLock<Option<RpcSnapshot>>,
    resync_required: AtomicBool,
    #[cfg(test)]
    resync_transitions: AtomicU64,
    #[cfg(test)]
    resync_transitioned: Notify,
    #[cfg(test)]
    rebuild_failures: AtomicU64,
    #[cfg(test)]
    rebuild_failure_notify: Notify,
    #[cfg(test)]
    rebuild_failure_gate: Mutex<Option<Arc<SealingRebuildFailureGate>>>,
    #[cfg(test)]
    sealing_drain_notify: Notify,
    #[cfg(test)]
    gc_cutover_failure: Mutex<Option<GcCutoverFailureStage>>,
}

impl SnapshotPublisher {
    fn lock_for_publication(
        &self,
    ) -> Result<parking_lot::RwLockWriteGuard<'_, Option<RpcSnapshot>>> {
        let published = self.current.write();
        ensure!(
            !self.resync_required.load(Ordering::Acquire),
            "RPC transaction storage requires resync",
        );
        Ok(published)
    }

    fn load(&self) -> Option<RpcSnapshot> {
        let published = self.current.read();
        if self.resync_required.load(Ordering::Acquire) {
            return None;
        }
        published.clone()
    }

    fn require(&self, snapshot: Option<&RpcSnapshot>) -> Result<RpcSnapshot> {
        let published = self.current.read();
        ensure!(
            !self.resync_required.load(Ordering::Acquire),
            "RPC transaction storage requires resync",
        );
        snapshot
            .cloned()
            .or_else(|| published.clone())
            .context("No RPC snapshot available")
    }

    pub(super) fn is_resync_required(&self) -> bool {
        self.resync_required.load(Ordering::Acquire)
    }

    pub(super) fn transition_to_resync_required(
        &self,
        kind: super::tail::AuthoritativeErrorKind,
        error: &anyhow::Error,
    ) -> bool {
        let published = self.current.write();
        self.transition_to_resync_required_with_guard(published, kind, error)
    }

    fn transition_to_resync_required_with_guard(
        &self,
        mut published: parking_lot::RwLockWriteGuard<'_, Option<RpcSnapshot>>,
        kind: super::tail::AuthoritativeErrorKind,
        error: &anyhow::Error,
    ) -> bool {
        let first_transition = self
            .resync_required
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok();
        published.take();
        drop(published);
        if !first_transition {
            return false;
        }
        let reason = match kind {
            super::tail::AuthoritativeErrorKind::MalformedCommittedData => {
                "malformed_committed_data"
            }
            super::tail::AuthoritativeErrorKind::MissingCommittedData => {
                "missing_committed_data"
            }
            super::tail::AuthoritativeErrorKind::ConflictingCommittedData => {
                "conflicting_committed_data"
            }
        };
        metrics::counter!("tycho_storage_rpc_resync_required_total", "reason" => reason)
            .increment(1);
        tracing::error!(reason, "RPC transaction storage requires resync: {error:#}");
        #[cfg(test)]
        {
            self.resync_transitions.fetch_add(1, Ordering::AcqRel);
            self.resync_transitioned.notify_one();
        }
        true
    }

    #[cfg(test)]
    pub(super) async fn wait_for_resync_required(&self) {
        loop {
            let transitioned = self.resync_transitioned.notified();
            if self.is_resync_required() && self.resync_transitions() > 0 {
                return;
            }
            transitioned.await;
        }
    }

    #[cfg(test)]
    pub(super) fn resync_transitions(&self) -> u64 {
        self.resync_transitions.load(Ordering::Acquire)
    }
}

#[cfg(test)]
#[derive(Default)]
struct SealingRebuildFailureGate {
    released: Mutex<bool>,
    wake: parking_lot::Condvar,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GcCutoverFailureStage {
    BeforeControl,
    AfterControl,
    PostBuildValidation,
    AfterPublication,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GcEvacuationFailureStage {
    AfterTerminalCommit,
}

#[cfg(test)]
impl SnapshotPublisher {
    fn fail_gc_cutover_at(&self, stage: GcCutoverFailureStage) -> Result<()> {
        let mut failure = self.gc_cutover_failure.lock();
        if *failure == Some(stage) {
            failure.take();
            anyhow::bail!("injected RPC transaction GC cutover failure at {stage:?}");
        }
        Ok(())
    }
}

#[cfg(test)]
impl SealingRebuildFailureGate {
    fn wait(&self) {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut released = self.released.lock();
        while !*released {
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(
                !remaining.is_zero(),
                "timed out waiting to release the injected sealing rebuild failure"
            );
            let timeout = self.wake.wait_for(&mut released, remaining);
            assert!(
                !timeout.timed_out() || *released,
                "timed out waiting to release the injected sealing rebuild failure"
            );
        }
    }

    fn release(&self) {
        *self.released.lock() = true;
        self.wake.notify_all();
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BlockWriteStats {
    pub transaction_count: u64,
    pub index_record_count: u64,
    pub estimated_lsm_bytes: u64,
    pub estimated_blob_bytes: u64,
}

#[derive(Default)]
struct BlockWriteAccounting {
    stats: BlockWriteStats,
    estimated_block_metadata_bytes: u64,
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
    is_sealed: bool,
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
    filter_negative_skips: [AtomicU64; 4],
    filter_false_positives: [AtomicU64; 4],
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
    fn add_transaction_record(&mut self, key_len: usize, value_len: usize, blob_value: bool) -> Result<()> {
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

    fn add_account_marker(&mut self) -> Result<()> {
        self.add_transaction_record(tables::Accounts::KEY_LEN, 0, false)
    }

    fn add_transaction(&mut self, tx_value_len: usize, has_in_msg: bool) -> Result<()> {
        self.transaction_count = self.transaction_count.checked_add(1).context("transaction count overflow")?;
        self.index_record_count = self.index_record_count.checked_add(if has_in_msg { 4 } else { 3 }).context("transaction index count overflow")?;
        let tx_value_len_u64 = u64::try_from(tx_value_len).context("transaction value length exceeds u64")?;
        self.add_transaction_record(tables::Transactions::KEY_LEN, tx_value_len, tx_value_len_u64 >= DEFAULT_MIN_BLOB_SIZE)?;
        self.add_transaction_record(32, tables::TransactionsByHash::VALUE_FULL_LEN, false)?;
        if has_in_msg { self.add_transaction_record(32, tables::Transactions::KEY_LEN, false)?; }
        self.add_transaction_record(tables::BlockTransactions::KEY_LEN, 32, false)
    }
}

impl BlockWriteAccounting {
    fn add_block_metadata_record(&mut self, key_len: usize, value_len: usize) -> Result<()> {
        let key_len = u64::try_from(key_len).context("block metadata key length exceeds u64")?;
        let value_len = u64::try_from(value_len).context("block metadata value length exceeds u64")?;
        self.estimated_block_metadata_bytes = self.estimated_block_metadata_bytes
            .checked_add(key_len)
            .and_then(|value| value.checked_add(value_len))
            .context("block metadata estimate overflow")?;
        Ok(())
    }
}

fn prepare_account_marker(
    db: &RpcTransactionsDb,
    write_batch: &mut rocksdb::WriteBatch,
    account_key: &[u8; tables::Accounts::KEY_LEN],
    has_indexed_transaction: bool,
    newly_committed: bool,
    stats: &mut BlockWriteStats,
) -> Result<()> {
    if !has_indexed_transaction {
        return Ok(());
    }
    stats.add_account_marker()?;
    if newly_committed {
        write_batch.put_cf(&db.accounts.cf(), account_key, []);
    } else {
        let marker = db.accounts
            .get(account_key)?
            .context("partition commit certifies a missing account marker")?;
        anyhow::ensure!(marker.is_empty(), "partition account marker must have an empty value");
    }
    Ok(())
}

fn spawn_sealing_worker(
    partitions: Arc<Mutex<PartitionManager>>,
    tail: Arc<TailStore>,
    snapshots: Arc<SnapshotPublisher>,
    filter_registry: Arc<FilterRegistry>,
    maintenance: Arc<MaintenanceCoordinator>,
    notify: Arc<Notify>,
    gc_notify: Arc<Notify>,
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
                    tail.clone(),
                    snapshots.clone(),
                    filter_registry.clone(),
                    maintenance.clone(),
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
                    gc_notify.notify_one();
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
    tail: Arc<TailStore>,
    snapshots: Arc<SnapshotPublisher>,
    filter_registry: Arc<FilterRegistry>,
    maintenance: Arc<MaintenanceCoordinator>,
    id: PartitionId,
    cancelled: CancellationFlag,
    filter_worker: Option<Arc<FilterWorker>>,
) -> Result<()> {
    let permit = acquire_sealing_permit(&maintenance, &cancelled).await?;
    let phase_partitions = partitions.clone();
    let phase_cancelled = cancelled.clone();
    let mut db = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        anyhow::ensure!(
            !phase_cancelled.check(),
            "RPC transaction partition sealing cancelled"
        );
        let db = phase_partitions.lock().begin_sealing(id)?;
        // reduce the final gated flush while old readers and pre-closing writers drain
        flush_transaction_partition(&db)?;
        Ok::<_, anyhow::Error>(db)
    })
    .await
    .context("RPC transaction partition preliminary sealing task failed")??;

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
        #[cfg(test)]
        if !ready {
            snapshots.sealing_drain_notify.notify_one();
        }
        if ready {
            let permit = acquire_sealing_permit(&maintenance, &cancelled).await?;
            let phase_partitions = partitions.clone();
            let phase_snapshots = snapshots.clone();
            let phase_filter_registry = filter_registry.clone();
            let phase_tail = tail.clone();
            let phase_cancelled = cancelled.clone();
            let phase = tokio::task::spawn_blocking(move || {
                let _permit = permit;
                anyhow::ensure!(
                    !phase_cancelled.check(),
                    "RPC transaction partition sealing cancelled"
                );
                let mut published = phase_snapshots.lock_for_publication()?;
                if !sealing_snapshot_can_be_withdrawn(
                    &phase_partitions,
                    published.as_ref(),
                    id,
                ) {
                    return Ok::<_, anyhow::Error>(SealingPhase::SnapshotDrainPending(db));
                }
                let previous = published.take();
                let frontier = previous.as_ref().map(|snapshot| *snapshot.visible_frontier());
                drop(previous);

                // a lease acquired before closing may have written after the preliminary flush
                let seal_result = flush_transaction_partition(&db)
                    .and_then(|()| finish_sealing_partition(&phase_partitions, id, db));
                let Some(frontier) = frontier else {
                    seal_result?;
                    return Ok(SealingPhase::Complete {
                        snapshot_installed: false,
                    });
                };
                match build_sealing_composite_snapshot(
                    &phase_partitions,
                    &phase_filter_registry,
                    &phase_tail,
                    &phase_snapshots,
                    frontier,
                ) {
                    Ok(snapshot) => {
                        *published = Some(snapshot);
                        seal_result?;
                        Ok(SealingPhase::Complete {
                            snapshot_installed: true,
                        })
                    }
                    Err(snapshot_error) => {
                        if let Some(kind) = classify_authoritative_error(&snapshot_error) {
                            phase_snapshots.transition_to_resync_required_with_guard(
                                published,
                                kind,
                                &snapshot_error,
                            );
                            return Err(combine_sealing_snapshot_errors(
                                seal_result.err(),
                                snapshot_error.context(
                                    "authoritative RPC composite snapshot rebuild failed after sealing",
                                ),
                            ));
                        }
                        if phase_cancelled.check() {
                            return Err(combine_sealing_snapshot_errors(
                                seal_result.err(),
                                snapshot_error.context(
                                    "RPC composite snapshot rebuild cancelled after sealing",
                                ),
                            ));
                        }
                        Ok(SealingPhase::SnapshotRebuildPending(
                            SealingSnapshotRebuild {
                                frontier,
                                seal_error: seal_result.err(),
                                snapshot_error,
                            },
                        ))
                    }
                }
            })
            .await
            .context("RPC transaction partition final sealing task failed")??;
            match phase {
                SealingPhase::SnapshotDrainPending(returned) => db = returned,
                SealingPhase::SnapshotRebuildPending(pending) => {
                    rebuild_sealing_snapshot_until_ready(
                        partitions.clone(),
                        tail.clone(),
                        snapshots.clone(),
                        filter_registry.clone(),
                        maintenance.clone(),
                        cancelled.clone(),
                        pending,
                    )
                    .await?;
                    if let Some(filter_worker) = &filter_worker {
                        filter_worker.notify_sealed(id);
                    }
                    return Ok(());
                }
                SealingPhase::Complete { snapshot_installed } => {
                    if snapshot_installed
                        && let Some(filter_worker) = &filter_worker
                    {
                        filter_worker.notify_sealed(id);
                    }
                    return Ok(());
                }
            }
        }
        tokio::time::sleep(wait_delay).await;
        wait_delay = wait_delay
            .saturating_mul(2)
            .min(Duration::from_millis(250));
    }
}

enum SealingPhase {
    SnapshotDrainPending(Arc<RpcTransactionsDb>),
    SnapshotRebuildPending(SealingSnapshotRebuild),
    Complete { snapshot_installed: bool },
}

struct SealingSnapshotRebuild {
    frontier: BlockId,
    seal_error: Option<anyhow::Error>,
    snapshot_error: anyhow::Error,
}

async fn acquire_sealing_permit(
    maintenance: &MaintenanceCoordinator,
    cancelled: &CancellationFlag,
) -> Result<MaintenancePermit> {
    anyhow::ensure!(
        !cancelled.check(),
        "RPC transaction partition sealing cancelled"
    );
    let permit = maintenance.acquire(MaintenancePriority::Sealing);
    tokio::pin!(permit);
    loop {
        tokio::select! {
            result = &mut permit => {
                return result.context("RPC maintenance coordinator closed while sealing");
            }
            _ = tokio::time::sleep(Duration::from_millis(10)) => {
                anyhow::ensure!(
                    !cancelled.check(),
                    "RPC transaction partition sealing cancelled"
                );
            }
        }
    }
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

async fn rebuild_sealing_snapshot_until_ready(
    partitions: Arc<Mutex<PartitionManager>>,
    tail: Arc<TailStore>,
    snapshots: Arc<SnapshotPublisher>,
    filter_registry: Arc<FilterRegistry>,
    maintenance: Arc<MaintenanceCoordinator>,
    cancelled: CancellationFlag,
    mut pending: SealingSnapshotRebuild,
) -> Result<()> {
    let mut retry_delay = Duration::from_millis(10);
    loop {
        if cancelled.check() {
            return Err(combine_sealing_snapshot_errors(
                pending.seal_error.take(),
                pending
                    .snapshot_error
                    .context("RPC composite snapshot rebuild cancelled after sealing"),
            ));
        }
        tracing::error!(
            retry_delay_ms = retry_delay.as_millis(),
            "failed to rebuild RPC composite snapshot after sealing: {:#}",
            pending.snapshot_error,
        );
        tokio::time::sleep(retry_delay).await;
        tokio::task::yield_now().await;
        retry_delay = retry_delay
            .saturating_mul(2)
            .min(Duration::from_secs(1));

        let permit = match acquire_sealing_permit(&maintenance, &cancelled).await {
            Ok(permit) => permit,
            Err(e) => {
                let retry_error = e.context(
                    "failed to reacquire sealing maintenance permit for RPC composite snapshot rebuild",
                );
                return Err(combine_sealing_snapshot_errors(
                    pending.seal_error.take(),
                    combine_snapshot_retry_errors(pending.snapshot_error, retry_error),
                ));
            }
        };
        let phase_partitions = partitions.clone();
        let phase_snapshots = snapshots.clone();
        let phase_filter_registry = filter_registry.clone();
        let phase_tail = tail.clone();
        let phase_cancelled = cancelled.clone();
        let phase = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            if phase_cancelled.check() {
                return Err(combine_sealing_snapshot_errors(
                    pending.seal_error,
                    pending
                        .snapshot_error
                        .context("RPC composite snapshot rebuild cancelled after sealing"),
                ));
            }
            let mut published = phase_snapshots.lock_for_publication()?;
            let frontier = match sealing_rebuild_frontier(
                published.as_ref().map(RpcSnapshot::visible_frontier),
                pending.frontier,
            ) {
                Ok(frontier) => frontier,
                Err(identity_error) => {
                    return Err(combine_sealing_snapshot_errors(
                        pending.seal_error,
                        combine_snapshot_retry_errors(
                            pending.snapshot_error,
                            identity_error,
                        ),
                    ));
                }
            };
            match build_sealing_composite_snapshot(
                &phase_partitions,
                &phase_filter_registry,
                &phase_tail,
                &phase_snapshots,
                frontier,
            ) {
                Ok(snapshot) => {
                    *published = Some(snapshot);
                    match pending.seal_error {
                        Some(e) => Err(e),
                        None => Ok(SealingPhase::Complete {
                            snapshot_installed: true,
                        }),
                    }
                }
                Err(snapshot_error) => {
                    if let Some(kind) = classify_authoritative_error(&snapshot_error) {
                        phase_snapshots.transition_to_resync_required_with_guard(
                            published,
                            kind,
                            &snapshot_error,
                        );
                        return Err(combine_sealing_snapshot_errors(
                            pending.seal_error,
                            snapshot_error.context(
                                "authoritative RPC composite snapshot rebuild failed after sealing",
                            ),
                        ));
                    }
                    if phase_cancelled.check() {
                        return Err(combine_sealing_snapshot_errors(
                            pending.seal_error,
                            snapshot_error.context(
                                "RPC composite snapshot rebuild cancelled after sealing",
                            ),
                        ));
                    }
                    Ok(SealingPhase::SnapshotRebuildPending(
                        SealingSnapshotRebuild {
                            frontier,
                            seal_error: pending.seal_error,
                            snapshot_error,
                        },
                    ))
                }
            }
        })
        .await
        .context("RPC transaction partition snapshot rebuild task failed")??;
        match phase {
            SealingPhase::SnapshotRebuildPending(next) => pending = next,
            SealingPhase::Complete {
                snapshot_installed: true,
            } => return Ok(()),
            SealingPhase::Complete {
                snapshot_installed: false,
            } => unreachable!(),
            SealingPhase::SnapshotDrainPending(_) => unreachable!(),
        }
    }
}

fn build_sealing_composite_snapshot(
    partitions: &Arc<Mutex<PartitionManager>>,
    filter_registry: &Arc<FilterRegistry>,
    tail: &TailStore,
    snapshots: &SnapshotPublisher,
    frontier: BlockId,
) -> Result<RpcSnapshot> {
    #[cfg(test)]
    if snapshots
        .rebuild_failures
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
            remaining.checked_sub(1)
        })
        .is_ok()
    {
        snapshots.rebuild_failure_notify.notify_one();
        if let Some(gate) = snapshots.rebuild_failure_gate.lock().clone() {
            gate.wait();
        }
        anyhow::bail!("injected RPC composite snapshot rebuild failure");
    }
    build_composite_snapshot(&mut partitions.lock(), filter_registry, tail, frontier)
}

fn sealing_rebuild_frontier(
    current: Option<&BlockId>,
    pending: BlockId,
) -> Result<BlockId> {
    match current {
        Some(current) if current.seqno > pending.seqno => Ok(*current),
        Some(current) if current.seqno == pending.seqno => {
            anyhow::ensure!(
                current == &pending,
                "cannot rebuild a different RPC snapshot frontier at masterchain seqno {}",
                pending.seqno,
            );
            Ok(pending)
        }
        _ => Ok(pending),
    }
}

fn combine_snapshot_retry_errors(
    snapshot_error: anyhow::Error,
    retry_error: anyhow::Error,
) -> anyhow::Error {
    anyhow::anyhow!(
        "previous composite snapshot rebuild failed: {snapshot_error:#}; subsequent rebuild coordination failed: {retry_error:#}"
    )
}

fn combine_sealing_snapshot_errors(
    seal_error: Option<anyhow::Error>,
    snapshot_error: anyhow::Error,
) -> anyhow::Error {
    match seal_error {
        Some(seal_error) => anyhow::anyhow!(
            "sealing failed: {seal_error:#}; composite snapshot rebuild failed: {snapshot_error:#}"
        ),
        None => snapshot_error,
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
    if let Err(e) = partitions.lock().complete_sealing(id, read_only) {
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
    raw.flush_cf_opt(&db.accounts.cf(), &options)?;
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
        Self::open_inner(context, config, None)
    }

    pub(crate) fn open_full(
        context: StorageContext,
        config: RpcTransactionPartitionsConfig,
        gc_config: Option<TransactionsGcConfig>,
    ) -> Result<Self> {
        Self::open_inner(context, config, gc_config)
    }

    fn open_inner(
        context: StorageContext,
        config: RpcTransactionPartitionsConfig,
        gc_config: Option<TransactionsGcConfig>,
    ) -> Result<Self> {
        config.validate().map_err(anyhow::Error::msg)?;
        let filter_context = context.clone();
        let tail_context = context.clone();
        let filter_config = config.filters.clone();
        let maintenance_config = config.maintenance.clone();
        let maintenance = Arc::new(MaintenanceCoordinator::new(
            config.maintenance.max_concurrent_tasks,
        ));
        let sealed_exact_lookup_semaphore = Arc::new(Semaphore::new(
            filter_config.max_concurrent_sealed_exact_lookups,
        ));
        let sealed_exact_lookup_metrics = Arc::new(SealedExactLookupMetrics::default());
        #[cfg(test)]
        let sealed_exact_lookup_acquisitions = Arc::new(AtomicU64::new(0));
        let manager = PartitionManager::open(context, config)?;
        let tail = Arc::new(TailStore::open(
            &tail_context,
            manager.tail_identity(),
            manager.tail_visible_generation(),
        )?);
        if let Some(intent) = manager.gc_intent()? {
            tail.validate_startup_gc_intent(intent)?;
        }
        let partitions = Arc::new(Mutex::new(manager));
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
            snapshots.clone(),
            maintenance.clone(),
        ));
        let sealing_notify = Arc::new(Notify::new());
        let gc_notify = Arc::new(Notify::new());
        let gc = Arc::new(GcStateMachine::new(
            partitions.clone(),
            filter_worker.clone(),
            tail.clone(),
            maintenance.clone(),
            gc_notify.clone(),
            maintenance_config.tail_sweep_records_per_batch,
            snapshots.clone(),
        ));
        let sealing_cancel = CancellationFlag::new();
        let sealing_task = Some(spawn_sealing_worker(
            partitions.clone(),
            tail.clone(),
            snapshots.clone(),
            filter_registry.clone(),
            maintenance.clone(),
            sealing_notify.clone(),
            gc_notify.clone(),
            sealing_cancel.clone(),
            filter_worker.clone(),
        ));
        let this = Self {
            partitions,
            tail,
            block_set_admission: Default::default(),
            current_state,
            min_tx_lt: AtomicU64::new(u64::MAX),
            min_tx_lt_guard: Default::default(),
            snapshots,
            sealing_notify,
            gc_notify,
            gc,
            sealing_cancel,
            sealing_task,
            maintenance,
            maintenance_config,
            gc_config,
            gc_cursor_observation: Default::default(),
            filter_registry,
            filter_worker,
            sealed_exact_lookup_semaphore,
            sealed_exact_lookup_metrics,
            initial_snapshot_recorded: AtomicBool::new(false),
            #[cfg(test)]
            known_block_point_reads: AtomicU64::new(0),
            #[cfg(test)]
            sealed_exact_lookup_acquisitions,
            #[cfg(test)]
            gc_evacuation_failure: Default::default(),
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

    #[cfg(test)]
    pub(super) fn gc_config(&self) -> Option<&TransactionsGcConfig> {
        self.gc_config.as_ref()
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
        let result = self.partitions.lock().continue_lifecycle();
        metrics::histogram!(
            "tycho_storage_rpc_deferred_lifecycle_duration_seconds",
            "result" => if result.is_ok() { "success" } else { "failure" },
        )
        .record(started_at.elapsed());
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
        let mut published = self.snapshots.lock_for_publication()?;
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
        let snapshot = match build_composite_snapshot(
            &mut self.partitions.lock(),
            &self.filter_registry,
            &self.tail,
            visible_frontier,
        ) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                if let Some(kind) = classify_authoritative_error(&error) {
                    self.snapshots.transition_to_resync_required_with_guard(
                        published,
                        kind,
                        &error,
                    );
                }
                return Err(error);
            }
        };
        *published = Some(snapshot);
        Ok(())
    }

    pub fn load_snapshot(&self) -> Option<RpcSnapshot> {
        self.snapshots.load()
    }

    pub(super) fn is_resync_required(&self) -> bool {
        self.snapshots.is_resync_required()
    }

    pub(super) fn begin_or_resume_gc(
        &self,
        config: Option<&TransactionsGcConfig>,
    ) -> Result<Option<codec::GcIntent>> {
        self.partitions.lock().begin_gc_intent(config)
    }

    pub(super) fn begin_or_resume_gc_at_frontier(
        &self,
        config: Option<&TransactionsGcConfig>,
        frontier: &BlockId,
    ) -> Result<Option<codec::GcIntent>> {
        self.partitions
            .lock()
            .begin_gc_intent_at_frontier(config, frontier)
    }

    pub(super) fn startup_sealing_pending(&self, expected: Option<PartitionId>) -> bool {
        expected.is_some_and(|expected| {
            self.partitions.lock().next_sealing_partition() == Some(expected)
        })
    }

    pub(super) fn evacuate_gc_chunk(
        &self,
        intent: codec::GcIntent,
    ) -> Result<GcEvacuationChunkResult> {
        let started_at = Instant::now();
        let result = self.evacuate_gc_chunk_inner(intent);
        metrics::histogram!(
            "tycho_storage_rpc_gc_chunk_duration_seconds",
            "result" => if result.is_ok() { "success" } else { "failure" },
        )
        .record(started_at.elapsed());
        result
    }

    fn evacuate_gc_chunk_inner(
        &self,
        intent: codec::GcIntent,
    ) -> Result<GcEvacuationChunkResult> {
        ensure!(intent.phase == codec::GcIntentPhase::Evacuating, "RPC transaction GC intent is not evacuating");
        let stored_progress = self.tail.generation_progress(intent.target_generation)?;
        let terminal_commit = self.tail.generation_commit(intent.target_generation)?;
        if let Some(commit) = terminal_commit {
            let progress = stored_progress.ok_or_else(|| {
                missing_authoritative_error(
                    "terminal RPC tail generation is missing EOF progress",
                )
            })?;
            ensure_gc_progress_matches_intent(progress, intent)?;
            if !progress.eof {
                return Err(conflicting_authoritative_error(
                    "terminal RPC tail generation progress is not EOF",
                ));
            }
            if commit != gc_generation_commit(intent, progress.counters) {
                return Err(conflicting_authoritative_error(
                    "terminal RPC tail generation commit conflicts with the GC intent",
                ));
            }
            self.complete_gc_progress_metrics(intent, progress);
            let prepared = self
                .partitions
                .lock()
                .transition_gc_intent_to_prepared(&intent)?;
            return Ok(GcEvacuationChunkResult::Prepared(prepared));
        }
        let previous_progress = match stored_progress {
            Some(progress) => {
                ensure_gc_progress_matches_intent(progress, intent)?;
                if progress.eof {
                    return Err(missing_authoritative_error(
                        "EOF RPC tail progress exists without its atomic terminal commit",
                    ));
                }
                progress
            }
            None => gc_generation_progress(
                intent,
                codec::TailProgressCursor::Start,
                false,
                codec::TailGenerationCounters::default(),
                codec::EMPTY_TAIL_CHUNK_DIGEST,
            ),
        };

        let (source_opener, source_descriptor) = {
            let mut partitions = self.partitions.lock();
            let current = partitions
                .begin_gc_intent(None)?
                .ok_or_else(|| missing_authoritative_error(
                    "RPC transaction GC intent disappeared during evacuation",
                ))?;
            if current != intent {
                return Err(conflicting_authoritative_error(
                    "RPC transaction GC intent changed during evacuation",
                ));
            }
            let source_id = PartitionId(intent.source_partition_id);
            let descriptor = partitions
                .descriptors()
                .into_iter()
                .find(|descriptor| descriptor.id == source_id)
                .ok_or_else(|| missing_authoritative_error(
                    "RPC transaction GC source descriptor is missing",
                ))?;
            (
                partitions.maintenance_sealed_opener(source_id)?,
                descriptor,
            )
        };
        let source = source_opener.open()?;
        let snapshot = self
            .load_snapshot()
            .context("RPC transaction GC requires a published RPC snapshot")?;
        ensure!(snapshot.tail_snapshot().visible_generation() == intent.previous_visible_generation,
            "RPC transaction GC snapshot has a different visible tail generation");
        ensure!(snapshot.descriptor(source_descriptor.id) == Some(&source_descriptor),
            "RPC transaction GC source descriptor changed in the published snapshot");
        let estimated_candidate_accounts = snapshot
            .filter_bundle(source_descriptor.id)
            .map(|bundle| {
                bundle
                    .filter(FilterNamespace::Accounts)
                    .metadata()
                    .source_key_count
            })
            .or_else(|| match source.rocksdb().property_int_value_cf(
                &source.accounts.cf(),
                rocksdb::properties::ESTIMATE_NUM_KEYS,
            ) {
                Ok(value) => value,
                Err(error) => {
                    tracing::warn!(
                        partition_id = source_descriptor.id.0,
                        "failed to estimate RPC transaction GC candidate accounts: {error:#}",
                    );
                    None
                }
            })
            .unwrap_or(previous_progress.counters.processed_accounts)
            .max(previous_progress.counters.processed_accounts);
        metrics::gauge!(
            "tycho_storage_rpc_gc_candidate_accounts",
            "state" => "total",
        )
        .set(estimated_candidate_accounts as f64);
        self.record_gc_progress_metrics(intent, previous_progress);

        let accounts = candidate_accounts_after(
            &source,
            previous_progress.cursor,
            self.maintenance_config.gc_accounts_per_chunk,
        )?;
        if accounts.is_empty() {
            let progress = gc_generation_progress(
                intent,
                previous_progress.cursor,
                true,
                previous_progress.counters,
                previous_progress.chunk_digest,
            );
            let commit = gc_generation_commit(intent, progress.counters);
            self.tail.finish_generation(progress, commit)?;
            #[cfg(test)]
            self.fail_gc_evacuation_at(GcEvacuationFailureStage::AfterTerminalCommit)?;
            self.complete_gc_progress_metrics(intent, progress);
            let prepared = self
                .partitions
                .lock()
                .transition_gc_intent_to_prepared(&intent)?;
            return Ok(GcEvacuationChunkResult::Prepared(prepared));
        }

        let mut deltas = Vec::with_capacity(accounts.len());
        let mut delta_counters = codec::TailGenerationCounters::default();
        let mut staged_bytes = 0u64;
        for account in accounts {
            let delta = build_gc_account_delta(&snapshot, &source, intent, account)?;
            let next_staged_bytes = staged_bytes
                .checked_add(delta.staged_bytes)
                .context("RPC transaction GC staged byte count overflow")?;
            if !deltas.is_empty()
                && next_staged_bytes > self.maintenance_config.gc_max_staged_bytes_per_batch
            {
                break;
            }
            staged_bytes = next_staged_bytes;
            delta_counters = checked_add_gc_counters(delta_counters, delta.counters)?;
            deltas.push(delta.delta);
        }
        ensure!(!deltas.is_empty(), "RPC transaction GC chunk did not retain its oversized first account");
        let counters = checked_add_gc_counters(previous_progress.counters, delta_counters)?;
        let cursor = codec::TailProgressCursor::Account(
            deltas.last().expect("non-empty GC chunk").account(),
        );
        let progress = gc_generation_progress(intent, cursor, false, counters, codec::EMPTY_TAIL_CHUNK_DIGEST);
        let progress = self.tail.append_chunk(&deltas, stored_progress, progress)?;
        metrics::histogram!("tycho_storage_rpc_gc_chunk_accounts")
            .record(delta_counters.processed_accounts as f64);
        metrics::histogram!("tycho_storage_rpc_gc_chunk_staged_bytes")
            .record(staged_bytes as f64);
        if delta_counters.processed_accounts == 1
            && staged_bytes > self.maintenance_config.gc_max_staged_bytes_per_batch
        {
            metrics::counter!("tycho_storage_rpc_gc_oversized_soft_limit_chunks_total")
                .increment(1);
        }
        self.record_gc_progress_metrics(intent, progress);
        Ok(GcEvacuationChunkResult::Appended)
    }

    #[cfg(test)]
    fn fail_gc_evacuation_at(&self, stage: GcEvacuationFailureStage) -> Result<()> {
        let mut failure = self.gc_evacuation_failure.lock();
        if *failure == Some(stage) {
            failure.take();
            anyhow::bail!("injected RPC transaction GC evacuation failure at {stage:?}");
        }
        Ok(())
    }

    fn record_gc_progress_metrics(
        &self,
        intent: codec::GcIntent,
        progress: codec::TailGenerationProgress,
    ) {
        let now = Instant::now();
        let mut observation = self.gc_cursor_observation.lock();
        let cursor_age = match *observation {
            Some(current)
                if current.operation_id == intent.operation_id
                    && current.cursor == progress.cursor =>
            {
                now.saturating_duration_since(current.observed_at)
            }
            _ => {
                *observation = Some(GcCursorObservation {
                    operation_id: intent.operation_id,
                    cursor: progress.cursor,
                    observed_at: now,
                });
                Duration::ZERO
            }
        };
        metrics::gauge!(
            "tycho_storage_rpc_gc_candidate_accounts",
            "state" => "processed",
        )
        .set(progress.counters.processed_accounts as f64);
        metrics::gauge!("tycho_storage_rpc_gc_cursor_age_seconds")
            .set(cursor_age.as_secs_f64());
        metrics::gauge!("tycho_storage_rpc_gc_keep_transactions_per_account")
            .set(intent.keep_tx_per_account as f64);
        metrics::gauge!("tycho_storage_rpc_gc_cutoff_unix_seconds")
            .set(intent.cutoff_utime as f64);
        metrics::gauge!(
            "tycho_storage_rpc_gc_intent_info",
            "policy" => "ttl_keep_n",
            "cutoff" => "fixed_frontier",
        )
        .set(1.0);
    }

    fn complete_gc_progress_metrics(
        &self,
        intent: codec::GcIntent,
        progress: codec::TailGenerationProgress,
    ) {
        self.record_gc_progress_metrics(intent, progress);
        let mut observation = self.gc_cursor_observation.lock();
        if observation
            .as_ref()
            .is_some_and(|current| current.operation_id == intent.operation_id)
        {
            observation.take();
        }
        metrics::gauge!("tycho_storage_rpc_gc_cursor_age_seconds").set(0.0);
        metrics::gauge!(
            "tycho_storage_rpc_gc_candidate_accounts",
            "state" => "total",
        )
        .set(progress.counters.processed_accounts as f64);
    }

    #[cfg(test)]
    fn evacuate_gc(
        &self,
        config: Option<&TransactionsGcConfig>,
    ) -> Result<Option<codec::GcIntent>> {
        let Some(intent) = self.begin_or_resume_gc(config)? else {
            return Ok(None);
        };
        match intent.phase {
            codec::GcIntentPhase::Evacuating => loop {
                if let GcEvacuationChunkResult::Prepared(prepared) =
                    self.evacuate_gc_chunk(intent)?
                {
                    return Ok(Some(prepared));
                }
            },
            codec::GcIntentPhase::Prepared => {
                let progress = self
                    .tail
                    .generation_progress(intent.target_generation)?
                    .context("prepared RPC transaction GC intent is missing tail progress")?;
                let commit = self
                    .tail
                    .generation_commit(intent.target_generation)?
                    .context("prepared RPC transaction GC intent is missing tail commit")?;
                ensure_gc_progress_matches_intent(progress, intent)?;
                ensure!(progress.eof && commit == gc_generation_commit(intent, progress.counters),
                    "prepared RPC transaction GC intent conflicts with terminal tail state");
                Ok(Some(intent))
            }
            codec::GcIntentPhase::CutoverCommitted | codec::GcIntentPhase::Deleting => {
                Ok(Some(intent))
            }
        }
    }

    pub(super) async fn cutover_prepared_gc(
        &self,
        intent: codec::GcIntent,
    ) -> Result<codec::GcIntent> {
        ensure!(intent.phase == codec::GcIntentPhase::Prepared,
            "RPC transaction GC intent is not prepared for cutover");
        // Keep the watermark guard outside the foreground admission -> snapshot -> manager order.
        // No await occurs after these locks are acquired, so cutover cannot invert publication.
        let _min_tx_lt_guard = self
            .min_tx_lt_guard
            .try_lock()
            .context("RPC transaction GC cutover is deferred by a concurrent history-watermark update")?;
        let admission = self.block_set_admission.lock();
        ensure!(admission.is_none(), "RPC transaction GC cutover is deferred by block-set admission");
        let mut published = self.snapshots.lock_for_publication()?;
        let mut partitions = self.partitions.lock();
        let current_intent = partitions
            .gc_intent()?
            .ok_or_else(|| missing_authoritative_error(
                "RPC transaction GC cutover intent is missing",
            ))?;
        let committed_intent = codec::GcIntent {
            phase: codec::GcIntentPhase::CutoverCommitted,
            ..intent
        };
        if current_intent != intent && current_intent != committed_intent {
            return Err(conflicting_authoritative_error(
                "RPC transaction GC cutover intent identity changed",
            ));
        }
        let control_committed = current_intent == committed_intent;
        let frontier = match partitions.visible_frontier().copied() {
            Some(frontier) => frontier,
            None if control_committed => {
                published.take();
                return Err(missing_authoritative_error(
                    "committed RPC transaction GC cutover is missing its visible frontier",
                ));
            }
            None => anyhow::bail!("RPC transaction GC cutover is missing its visible frontier"),
        };
        if control_committed {
            // a committed replay may lack a publisher, but an existing one must be authoritative
            if published.as_ref().is_some_and(|current| {
                current.tail_snapshot().visible_generation() != intent.target_generation
                    || current.descriptor(PartitionId(intent.source_partition_id)).is_some()
                    || current.manifest_epoch() != partitions.manifest_epoch()
                    || *current.visible_frontier() != frontier
            }) {
                published.take();
                return Err(conflicting_authoritative_error(
                    "published RPC transaction snapshot conflicts with committed GC cutover",
                ));
            }
        } else {
            let current = published
                .as_ref()
                .context("RPC transaction GC cutover requires a published snapshot")?;
            ensure!(current.tail_snapshot().visible_generation() == intent.previous_visible_generation,
                "RPC transaction GC cutover snapshot has a different visible tail generation");
            ensure!(current.descriptor(PartitionId(intent.source_partition_id))
                .is_some_and(|descriptor| descriptor.lifecycle == codec::ManifestLifecycle::Sealed),
                "RPC transaction GC cutover snapshot is missing its sealed source");
            ensure!(*current.visible_frontier() == frontier,
                "RPC transaction GC cutover snapshot frontier conflicts with control state");
        }
        #[cfg(test)]
        self.snapshots.fail_gc_cutover_at(GcCutoverFailureStage::BeforeControl)?;
        let progress = self
            .tail
            .generation_progress(intent.target_generation)?
            .ok_or_else(|| missing_authoritative_error(
                "prepared RPC transaction GC cutover is missing EOF tail progress",
            ))?;
        let commit = self
            .tail
            .generation_commit(intent.target_generation)?
            .ok_or_else(|| missing_authoritative_error(
                "prepared RPC transaction GC cutover is missing its terminal tail commit",
            ))?;
        ensure_gc_progress_matches_intent(progress, intent)?;
        if !progress.eof || commit != gc_generation_commit(intent, progress.counters) {
            return Err(conflicting_authoritative_error(
                "prepared RPC transaction GC cutover conflicts with terminal tail state",
            ));
        }
        let cutover = partitions.commit_gc_cutover(&intent, progress, commit)?;
        record_gc_generation_cutover_metrics(control_committed, progress.counters);
        self.min_tx_lt
            .store(cutover.smallest_known_lt, Ordering::Release);
        #[cfg(test)]
        if let Err(error) = self
            .snapshots
            .fail_gc_cutover_at(GcCutoverFailureStage::AfterControl)
        {
            published.take();
            return Err(error);
        }
        let next = match build_composite_snapshot(
            &mut partitions,
            &self.filter_registry,
            &self.tail,
            frontier,
        ) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                published.take();
                return Err(error).context(
                    "failed to install the committed RPC transaction GC cutover snapshot",
                );
            }
        };
        #[cfg(test)]
        let validation_error = self
            .snapshots
            .fail_gc_cutover_at(GcCutoverFailureStage::PostBuildValidation)
            .err();
        #[cfg(not(test))]
        let validation_error: Option<anyhow::Error> = None;
        let snapshot_matches = next.tail_snapshot().visible_generation() == cutover.visible_generation
            && next.descriptor(cutover.source_partition_id).is_none()
            && next.manifest_epoch() == cutover.manifest_epoch
            && *next.visible_frontier() == frontier;
        if validation_error.is_some() || !snapshot_matches {
            published.take();
            if let Some(error) = validation_error {
                return Err(error);
            }
            return Err(conflicting_authoritative_error(
                "RPC transaction GC cutover snapshot does not match committed control state",
            ));
        }
        *published = Some(next);
        #[cfg(test)]
        self.snapshots.fail_gc_cutover_at(GcCutoverFailureStage::AfterPublication)?;
        drop(partitions);
        drop(published);
        drop(admission);
        self.gc_notify.notify_one();
        Ok(cutover.intent)
    }

    pub(crate) fn start_filter_worker(&self) {
        self.filter_worker.start();
        if self.partitions.lock().next_sealing_partition().is_some() {
            self.sealing_notify.notify_one();
        }
        self.gc.start();
    }

    pub(crate) fn start_maintenance(self: &Arc<Self>, frontier: &BlockId) -> Result<()> {
        let pending_sealing = self.partitions.lock().next_sealing_partition();
        self.gc.configure_startup(
            Arc::downgrade(self),
            self.gc_config.clone(),
            *frontier,
            pending_sealing,
        )?;
        self.filter_worker.start();
        if self.partitions.lock().next_sealing_partition().is_some() {
            self.sealing_notify.notify_one();
        }
        self.gc.start();
        Ok(())
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
        let mut published = self.snapshots.lock_for_publication()?;
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
            let commit_result = partitions.commit_masterchain_block_set_after_admission(
                block_id,
                token.mode == BlockSetMode::Same,
            );
            if let Err(error) = commit_result {
                if let Some(kind) = classify_authoritative_error(&error) {
                    self.snapshots.transition_to_resync_required_with_guard(
                        published,
                        kind,
                        &error,
                    );
                }
                return Err(error);
            }
            if token.mode != BlockSetMode::Same {
                let snapshot = match build_composite_snapshot(
                    &mut partitions,
                    &self.filter_registry,
                    &self.tail,
                    *block_id,
                ) {
                    Ok(snapshot) => snapshot,
                    Err(error) => {
                        if let Some(kind) = classify_authoritative_error(&error) {
                            self.snapshots.transition_to_resync_required_with_guard(
                                published,
                                kind,
                                &error,
                            );
                        }
                        return Err(error);
                    }
                };
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
        let mut published = self.snapshots.lock_for_publication()?;
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
            let snapshot = match build_composite_snapshot(
                &mut partitions,
                &self.filter_registry,
                &self.tail,
                frontier,
            ) {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    if let Some(kind) = classify_authoritative_error(&error) {
                        self.snapshots.transition_to_resync_required_with_guard(
                            published,
                            kind,
                            &error,
                        );
                    }
                    return Err(error);
                }
            };
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
        self.snapshots.require(snapshot)
    }

    fn classify_authoritative_result<T>(&self, result: Result<T>) -> Result<T> {
        if let Err(error) = &result
            && let Some(kind) = classify_authoritative_error(error)
        {
            self.snapshots.transition_to_resync_required(kind, error);
        }
        result
    }

    #[cfg(test)]
    pub(super) fn transition_to_resync_required_for_test(
        &self,
        kind: AuthoritativeErrorKind,
    ) {
        let error = anyhow::anyhow!("injected committed RPC transaction storage failure");
        self.snapshots.transition_to_resync_required(kind, &error);
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

    fn tail_transaction_info(record: &TailTransactionRecord) -> Result<TransactionInfo> {
        let (account, lt) = codec::decode_tail_payload_key(&record.payload_key).map_err(|_| {
            malformed_authoritative_error("RPC tail transaction has an invalid payload key")
        })?;
        if record.hash_locator.payload_key != record.payload_key {
            return Err(conflicting_authoritative_error(
                "RPC tail transaction hash locator points to a different payload",
            ));
        }
        if record.hash_locator.mc_seqno != record.value.related_mc_seqno() {
            return Err(conflicting_authoritative_error(
                "RPC tail transaction hash locator and payload have different related masterchain seqnos",
            ));
        }
        Ok(TransactionInfo {
            account: StdAddr::new(account[0] as i8, HashBytes::from_slice(&account[1..])),
            lt,
            block_id: record.hash_locator.block_id,
            mc_seqno: record.hash_locator.mc_seqno,
        })
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
            let partition = self.classify_authoritative_result(acquire_partition_read(
                snapshot.clone(),
                descriptor.id,
            ))?;
            if let Some(value) = partition.get(&partition.lease.transactions_by_hash, hash)? {
                if value.len() != tables::TransactionsByHash::VALUE_FULL_LEN || value[41] >= 64 {
                    if descriptor.lifecycle == codec::ManifestLifecycle::Sealed {
                        return Err(malformed_authoritative_error(
                            "invalid committed sealed transaction hash locator",
                        ));
                    }
                    anyhow::bail!("invalid local transaction hash locator");
                }
                let info = match TransactionInfo::from_bytes(&value) {
                    Some(info) => info,
                    None if descriptor.lifecycle == codec::ManifestLifecycle::Sealed => {
                        return Err(malformed_authoritative_error(
                            "invalid committed sealed transaction hash locator",
                        ));
                    }
                    None => anyhow::bail!("invalid local transaction hash locator"),
                };
                if descriptor.lifecycle == codec::ManifestLifecycle::Sealed
                    && info.mc_seqno < descriptor.first.mc_seqno
                {
                    return Err(conflicting_authoritative_error(
                        "committed sealed transaction hash locator precedes the partition lower bound",
                    ));
                }
                if descriptor.lifecycle == codec::ManifestLifecycle::Sealed
                    && info.mc_seqno > descriptor.last.mc_seqno
                {
                    return Err(conflicting_authoritative_error(
                        "committed sealed transaction hash locator exceeds the partition upper bound",
                    ));
                }
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
                        is_sealed: descriptor.lifecycle == codec::ManifestLifecycle::Sealed,
                        sealed_exact_lookup_permit: exact_lookup.into_permit(),
                    }));
                }
                if descriptor.lifecycle == codec::ManifestLifecycle::Sealed
                    && descriptor.last.mc_seqno <= snapshot.visible_frontier().seqno
                {
                    return Err(conflicting_authoritative_error(
                        "committed sealed transaction hash locator is newer than the frozen frontier",
                    ));
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
            let partition = self.classify_authoritative_result(acquire_partition_read(
                snapshot.clone(),
                descriptor.id,
            ))?;
            if let Some(key) = partition.get(&partition.lease.transactions_by_in_msg, hash)? {
                if key.len() != tables::Transactions::KEY_LEN {
                    if descriptor.lifecycle == codec::ManifestLifecycle::Sealed {
                        return Err(malformed_authoritative_error(
                            "invalid committed sealed inbound-message locator length",
                        ));
                    }
                    anyhow::bail!("invalid local inbound-message locator length");
                }
                let tx = match partition.get(&partition.lease.transactions, &key)? {
                    Some(tx) => tx,
                    None if descriptor.lifecycle == codec::ManifestLifecycle::Sealed => {
                        return Err(missing_authoritative_error(
                            "committed sealed inbound-message locator points to a missing transaction",
                        ));
                    }
                    None => anyhow::bail!(
                        "inbound-message locator points to a missing local transaction"
                    ),
                };
                let related_mc_seqno = match TransactionData::related_mc_seqno(&tx) {
                    Ok(mc_seqno) => mc_seqno,
                    Err(_) if descriptor.lifecycle == codec::ManifestLifecycle::Sealed => {
                        return Err(malformed_authoritative_error(
                            "committed sealed inbound-message locator points to an invalid transaction",
                        ));
                    }
                    Err(error) => return Err(error),
                };
                if descriptor.lifecycle == codec::ManifestLifecycle::Sealed
                    && related_mc_seqno < descriptor.first.mc_seqno
                {
                    return Err(conflicting_authoritative_error(
                        "committed sealed inbound-message transaction precedes the partition lower bound",
                    ));
                }
                if descriptor.lifecycle == codec::ManifestLifecycle::Sealed
                    && related_mc_seqno > descriptor.last.mc_seqno
                {
                    return Err(conflicting_authoritative_error(
                        "committed sealed inbound-message transaction exceeds the partition upper bound",
                    ));
                }
                if related_mc_seqno <= snapshot.visible_frontier().seqno {
                    let transaction = TransactionData::from_owned(tx);
                    if transaction.in_msg_hash().as_ref() != Some(hash) {
                        if descriptor.lifecycle == codec::ManifestLifecycle::Sealed {
                            return Err(conflicting_authoritative_error(
                                "committed inbound-message locator points to a transaction with a different message hash",
                            ));
                        }
                        anyhow::bail!(
                            "inbound-message locator points to a transaction with a different message hash"
                        );
                    }
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
                if descriptor.lifecycle == codec::ManifestLifecycle::Sealed
                    && descriptor.last.mc_seqno <= snapshot.visible_frontier().seqno
                {
                    return Err(conflicting_authoritative_error(
                        "committed sealed inbound-message locator is newer than the frozen frontier",
                    ));
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
            let partition = self.classify_authoritative_result(acquire_partition_read(
                snapshot.clone(),
                descriptor.id,
            ))?;
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
            self.classify_authoritative_result(acquire_partition_read(snapshot, partition_id))?;
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
            self.classify_authoritative_result(acquire_partition_read(snapshot, partition_id))?;
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
        let start_lt = start_lt.unwrap_or_default();
        let end_lt = end_lt.unwrap_or(u64::MAX);
        let range_empty = end_lt < start_lt;

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
            partition_ids,
            range_empty,
            start_lt,
            end_lt,
            range_from,
            range_to,
            snapshot_publisher: Arc::downgrade(&self.snapshots),
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
        let local_result = self.transaction_partition(hash, snapshot.clone());
        if let Some(LocatedTransaction {
            partition,
            info,
            key,
            is_sealed,
            sealed_exact_lookup_permit,
        }) = self.classify_authoritative_result(local_result)? {
            let tx_result = partition
                .get_pinned(
                    &partition.lease.transactions,
                    &key,
                )
                .and_then(|tx| tx.ok_or_else(|| {
                    if is_sealed {
                        missing_authoritative_error(
                            "committed transaction hash locator points to a missing local transaction",
                        )
                    } else {
                        anyhow::anyhow!(
                            "transaction hash locator points to a missing local transaction"
                        )
                    }
                }));
            let tx = self.classify_authoritative_result(tx_result)?;
            let mc_seqno_result = TransactionData::related_mc_seqno(tx.as_ref()).map_err(|_| {
                if is_sealed {
                    malformed_authoritative_error(
                        "committed transaction hash locator points to an invalid local transaction",
                    )
                } else {
                    anyhow::anyhow!(
                        "transaction hash locator points to an invalid local transaction"
                    )
                }
            });
            let transaction_mc_seqno = self.classify_authoritative_result(mc_seqno_result)?;
            if transaction_mc_seqno != info.mc_seqno {
                let error = if is_sealed {
                    conflicting_authoritative_error(
                        "transaction hash locator and local transaction have different related masterchain seqnos",
                    )
                } else {
                    anyhow::anyhow!(
                        "transaction hash locator and local transaction have different related masterchain seqnos"
                    )
                };
                return self.classify_authoritative_result(Err(error));
            }
            if TransactionData::read_tx_hash(tx.as_ref()) != *hash {
                let error = if is_sealed {
                    conflicting_authoritative_error(
                        "transaction hash locator points to a transaction with a different hash",
                    )
                } else {
                    anyhow::anyhow!(
                        "transaction hash locator points to a transaction with a different hash"
                    )
                };
                return self.classify_authoritative_result(Err(error));
            }
            let result = map(info, tx.as_ref());
            drop(tx);
            drop(sealed_exact_lookup_permit);
            return Ok(Some(result));
        }
        let tail_result = snapshot.tail_snapshot().transaction_by_hash(
            hash,
            snapshot.visible_frontier().seqno,
        );
        let Some(record) = self.classify_authoritative_result(tail_result)? else {
            return Ok(None);
        };
        anyhow::ensure!(
            record.value.transaction_hash() == *hash,
            "RPC tail hash lookup returned a transaction with a different hash"
        );
        let info = self.classify_authoritative_result(Self::tail_transaction_info(&record))?;
        Ok(Some(map(info, record.value.as_bytes())))
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
            let partition = self.classify_authoritative_result(acquire_partition_read(
                snapshot.clone(),
                id,
            ))?;
            let is_sealed = partition.lease.lifecycle() == codec::ManifestLifecycle::Sealed;
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
                if tx_key.len() != tables::Transactions::KEY_LEN {
                    let error = if is_sealed {
                        malformed_authoritative_error(
                            "committed sealed source transaction has an invalid key",
                        )
                    } else {
                        anyhow::anyhow!("source transaction has an invalid local key")
                    };
                    return self.classify_authoritative_result(Err(error));
                }
                if tx_key[0..33] != key[0..33] {
                    break;
                }
                let related_mc_seqno = match TransactionData::related_mc_seqno(value) {
                    Ok(related_mc_seqno) => related_mc_seqno,
                    Err(_) if is_sealed => {
                        return self.classify_authoritative_result(Err(
                            malformed_authoritative_error(
                                "committed sealed source transaction has an invalid payload",
                            ),
                        ));
                    }
                    Err(error) => return Err(error),
                };
                if related_mc_seqno <= snapshot.visible_frontier().seqno {
                    return Ok(Some(TransactionData::from_owned(value.to_vec())));
                }
                iter.prev();
            }
            iter.status()?;
        }
        let account_key = key[..codec::ACCOUNT_KEY_LEN].try_into().unwrap();
        let tail_result = snapshot.tail_snapshot().source_transaction(
            account_key,
            message_lt,
            snapshot.visible_frontier().seqno,
        );
        let Some(record) = self.classify_authoritative_result(tail_result)? else {
            return Ok(None);
        };
        let info = self.classify_authoritative_result(Self::tail_transaction_info(&record))?;
        anyhow::ensure!(
            info.account == *account && info.lt < message_lt,
            "RPC tail source transaction does not match the requested account or message LT"
        );
        Ok(Some(TransactionData::from_owned(record.value.as_bytes().to_vec())))
    }

    pub fn get_dst_transaction<'db>(
        &'db self,
        in_msg_hash: &HashBytes,
        snapshot: Option<&RpcSnapshot>,
    ) -> Result<Option<TransactionData<'db>>> {
        let snapshot = self.require_snapshot(snapshot)?;
        let local_result = self.inbound_message_partition(in_msg_hash, snapshot.clone());
        if let Some(LocatedInboundTransaction {
            partition,
            transaction,
            sealed_exact_lookup_permit,
        }) = self.classify_authoritative_result(local_result)? {
            let _partition = partition;
            drop(sealed_exact_lookup_permit);
            return Ok(Some(transaction));
        }
        let tail_result = snapshot.tail_snapshot().transaction_by_in_msg(
            in_msg_hash,
            snapshot.visible_frontier().seqno,
        );
        let Some(record) = self.classify_authoritative_result(tail_result)? else {
            return Ok(None);
        };
        anyhow::ensure!(
            record.value.in_msg_hash().as_ref() == Some(in_msg_hash)
                && record.in_msg_locator.as_ref().map(|(hash, _)| hash) == Some(in_msg_hash),
            "RPC tail inbound-message lookup returned a transaction with a different inbound message"
        );
        self.classify_authoritative_result(Self::tail_transaction_info(&record))?;
        Ok(Some(TransactionData::from_owned(record.value.as_bytes().to_vec())))
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
        let partition_lease = self.partitions.lock().lease_for_mc_seqno(mc_seqno);
        let (partition_id, mut partition_lease) = self.classify_authoritative_result(partition_lease)?;
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
        let (start_lt, updates, computed_accounting) = tokio::task::spawn_blocking(move || {
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
            let mut accounting = BlockWriteAccounting::default();
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

                accounting.add_block_metadata_record(key.len(), buffer.len())?;
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
                let mut has_indexed_transaction = false;
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
                    has_indexed_transaction = true;

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
                    accounting.stats.add_transaction(tx_value.len(), msg_hash.is_some())?;

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
                let account_key = <&[u8; tables::Accounts::KEY_LEN]>::try_from(
                    &tx_info[..tables::Accounts::KEY_LEN],
                ).expect("account key has a fixed length");
                prepare_account_marker(
                    &db,
                    &mut write_batch,
                    account_key,
                    has_indexed_transaction,
                    newly_committed,
                    &mut accounting.stats,
                )?;

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

            accounting.add_block_metadata_record(tables::KnownBlocks::KEY_LEN, buffer.len())?;
            write_batch.put_cf(
                &db.known_blocks.cf(),
                &block_tx[0..tables::KnownBlocks::KEY_LEN],
                buffer.as_slice(),
            );

            drop(prepare_batch_histogram);

            let _execute_batch_histogram =
                HistogramGuard::begin("tycho_storage_rpc_execute_batch_time");

            accounting.add_block_metadata_record(
                codec::PARTITION_COMMIT_KEY_LEN,
                codec::PARTITION_COMMIT_VALUE_LEN,
            )?;

            if let Some(commit) = existing_commit {
                anyhow::ensure!(
                    commit.transaction_count == accounting.stats.transaction_count
                        && commit.transaction_index_record_count == accounting.stats.index_record_count
                        && commit.estimated_transaction_lsm_bytes == accounting.stats.estimated_lsm_bytes
                        && commit.estimated_transaction_blob_bytes == accounting.stats.estimated_blob_bytes
                        && commit.estimated_block_metadata_bytes == accounting.estimated_block_metadata_bytes
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
                    transaction_count: accounting.stats.transaction_count,
                    estimated_transaction_lsm_bytes: accounting.stats.estimated_lsm_bytes,
                    estimated_transaction_blob_bytes: accounting.stats.estimated_blob_bytes,
                    transaction_index_record_count: accounting.stats.index_record_count,
                    estimated_block_metadata_bytes: accounting.estimated_block_metadata_bytes,
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

            Ok::<_, anyhow::Error>((info.start_lt, updates, accounting))
        })
        .await??;

        let _guard = self.min_tx_lt_guard.lock().await;
        {
            let mut partitions = self.partitions.lock();
            let min_tx_lt = self.min_tx_lt.load(Ordering::Acquire).min(start_lt);
            partitions.persist_min_transaction_lt_decrease(min_tx_lt)?;
            self.min_tx_lt
                .store(partitions.min_transaction_lt(), Ordering::Release);
        }
        drop(_guard);

        if !updates.is_empty() {
            subscriptions.fanout_updates(updates).await;
        }

        let stats = existing_commit.map(|commit| BlockWriteStats { transaction_count: commit.transaction_count, index_record_count: commit.transaction_index_record_count, estimated_lsm_bytes: commit.estimated_transaction_lsm_bytes, estimated_blob_bytes: commit.estimated_transaction_blob_bytes }).unwrap_or(computed_accounting.stats);
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
        self.gc.shutdown();
        self.maintenance.close();
        self.sealing_cancel.cancel();
        if let Some(task) = self.sealing_task.take() {
            task.abort();
        }
        self.filter_worker.shutdown();
    }
}

pub(super) enum GcEvacuationChunkResult {
    Appended,
    Prepared(codec::GcIntent),
}

struct GcAccountDelta {
    delta: AccountTailDelta,
    counters: codec::TailGenerationCounters,
    staged_bytes: u64,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum GcAccountFilterResult {
    Negative,
    Positive,
    Unknown,
}

impl GcAccountFilterResult {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Negative => "negative",
            Self::Positive => "positive",
            Self::Unknown => "unknown",
        }
    }
}

fn record_gc_account_filter_probe(result: GcAccountFilterResult) {
    metrics::counter!(
        "tycho_storage_rpc_gc_account_filter_probes_total",
        "result" => result.as_str(),
    )
    .increment(1);
}

fn record_gc_generation_metrics(counters: codec::TailGenerationCounters) {
    metrics::histogram!(
        "tycho_storage_rpc_gc_generation_records",
        "action" => "promoted",
    )
    .record(counters.promoted_records as f64);
    metrics::histogram!(
        "tycho_storage_rpc_gc_generation_records",
        "action" => "retired",
    )
    .record(counters.retired_records as f64);
    metrics::histogram!(
        "tycho_storage_rpc_gc_generation_bytes",
        "action" => "promoted",
    )
    .record(counters.promoted_bytes as f64);
    metrics::histogram!(
        "tycho_storage_rpc_gc_generation_bytes",
        "action" => "retired",
    )
    .record(counters.retired_bytes as f64);
}

fn record_gc_generation_cutover_metrics(
    control_committed: bool,
    counters: codec::TailGenerationCounters,
) {
    if !control_committed {
        record_gc_generation_metrics(counters);
    }
}

fn gc_generation_progress(
    intent: codec::GcIntent,
    cursor: codec::TailProgressCursor,
    eof: bool,
    counters: codec::TailGenerationCounters,
    chunk_digest: HashBytes,
) -> codec::TailGenerationProgress {
    codec::TailGenerationProgress {
        target_generation: intent.target_generation,
        operation_id: intent.operation_id,
        source_partition_id: intent.source_partition_id,
        source_manifest_digest: intent.source_manifest_digest,
        retention_policy_digest: intent.retention_policy_digest,
        cursor,
        eof,
        counters,
        chunk_digest,
    }
}

fn gc_generation_commit(
    intent: codec::GcIntent,
    counters: codec::TailGenerationCounters,
) -> codec::TailGenerationCommit {
    codec::TailGenerationCommit {
        layout_version: codec::TailLayoutVersion::MonolithicV1,
        target_generation: intent.target_generation,
        operation_id: intent.operation_id,
        source_partition_id: intent.source_partition_id,
        source_manifest_digest: intent.source_manifest_digest,
        previous_visible_generation: intent.previous_visible_generation,
        cutoff_utime: intent.cutoff_utime,
        keep_tx_per_account: intent.keep_tx_per_account,
        retention_policy_digest: intent.retention_policy_digest,
        counters,
    }
}

fn ensure_gc_progress_matches_intent(
    progress: codec::TailGenerationProgress,
    intent: codec::GcIntent,
) -> Result<()> {
    // every persisted progress identity field must remain pinned to its owning intent
    if progress.target_generation != intent.target_generation
        || progress.operation_id != intent.operation_id
        || progress.source_partition_id != intent.source_partition_id
        || progress.source_manifest_digest != intent.source_manifest_digest
        || progress.retention_policy_digest != intent.retention_policy_digest
    {
        return Err(conflicting_authoritative_error(
            "RPC tail progress conflicts with the transaction GC intent",
        ));
    }
    Ok(())
}

fn checked_add_gc_counters(
    left: codec::TailGenerationCounters,
    right: codec::TailGenerationCounters,
) -> Result<codec::TailGenerationCounters> {
    Ok(codec::TailGenerationCounters {
        processed_accounts: left.processed_accounts
            .checked_add(right.processed_accounts)
            .context("RPC transaction GC processed-account count overflow")?,
        promoted_records: left.promoted_records
            .checked_add(right.promoted_records)
            .context("RPC transaction GC promoted-record count overflow")?,
        promoted_bytes: left.promoted_bytes
            .checked_add(right.promoted_bytes)
            .context("RPC transaction GC promoted-byte count overflow")?,
        retired_records: left.retired_records
            .checked_add(right.retired_records)
            .context("RPC transaction GC retired-record count overflow")?,
        retired_bytes: left.retired_bytes
            .checked_add(right.retired_bytes)
            .context("RPC transaction GC retired-byte count overflow")?,
    })
}

fn candidate_accounts_after(
    source: &PartitionReadLease,
    cursor: codec::TailProgressCursor,
    max_count: usize,
) -> Result<Vec<AccountKey>> {
    ensure!(max_count > 0, "RPC transaction GC account chunk limit must be positive");
    let table = &source.accounts;
    let read_options = table.new_read_config();
    let mut iterator = source
        .rocksdb()
        .raw_iterator_cf_opt(&table.cf(), read_options);
    match cursor {
        codec::TailProgressCursor::Start => iterator.seek_to_first(),
        codec::TailProgressCursor::Account(account) => {
            iterator.seek(account);
            let Some((key, value)) = iterator.item() else {
                iterator.status()?;
                return Err(missing_authoritative_error(
                    "RPC transaction GC progress account is missing from the candidate",
                ));
            };
            if key != account {
                return Err(missing_authoritative_error(
                    "RPC transaction GC progress account is missing from the candidate",
                ));
            }
            if !value.is_empty() {
                return Err(malformed_authoritative_error(
                    "RPC transaction GC progress account marker is invalid",
                ));
            }
            iterator.next();
        }
    }
    let mut result = Vec::with_capacity(max_count);
    while result.len() < max_count {
        let Some((key, value)) = iterator.item() else {
            break;
        };
        let account = <AccountKey>::try_from(key).map_err(|_| {
            malformed_authoritative_error(
                "RPC transaction GC candidate contains an invalid account key",
            )
        })?;
        if !value.is_empty() {
            return Err(malformed_authoritative_error(
                "RPC transaction GC candidate account marker must be empty",
            ));
        }
        if let codec::TailProgressCursor::Account(cursor) = cursor
            && account <= cursor
        {
            return Err(conflicting_authoritative_error(
                "RPC transaction GC candidate account scan did not advance its cursor",
            ));
        }
        result.push(account);
        iterator.next();
    }
    iterator.status()?;
    Ok(result)
}

fn build_gc_account_delta(
    snapshot: &RpcSnapshot,
    source: &PartitionReadLease,
    intent: codec::GcIntent,
    account: AccountKey,
) -> Result<GcAccountDelta> {
    let newer_count = count_newer_account_transactions(
        snapshot,
        PartitionId(intent.source_partition_id),
        account,
        intent.keep_tx_per_account,
    )?;
    let candidate_limit = intent.keep_tx_per_account
        .checked_sub(newer_count)
        .context("RPC transaction GC newer history exceeds its bounded retention count")?;
    let mut promoted = load_candidate_promotions(
        source,
        account,
        candidate_limit,
        snapshot.visible_frontier().seqno,
        intent.target_generation,
    )?;
    let promoted_count = u64::try_from(promoted.len())
        .context("RPC transaction GC promoted-record count overflow")?;
    let mut tail_to_keep = candidate_limit
        .checked_sub(promoted_count)
        .context("RPC transaction GC candidate history exceeds its bounded retention count")?;
    let promoted_bytes = promoted.iter().try_fold(0u64, |sum, transaction| {
        sum.checked_add(
            u64::try_from(transaction.value_len())
                .context("RPC transaction GC promoted-byte count overflow")?,
        )
        .context("RPC transaction GC promoted-byte count overflow")
    })?;
    let mut staged_bytes = promoted.iter().try_fold(0u64, |sum, transaction| {
        sum.checked_add(transaction.staged_write_bytes()?)
            .context("RPC transaction GC staged byte count overflow")
    })?;

    let mut retired = Vec::new();
    let mut retired_bytes = 0u64;
    let mut cursor_lt = None;
    loop {
        let records = snapshot.tail_snapshot().newest_account_transactions(
            account,
            cursor_lt,
            1024,
            snapshot.visible_frontier().seqno,
        )?;
        if records.is_empty() {
            break;
        }
        for record in &records {
            if tail_to_keep > 0 {
                tail_to_keep -= 1;
            } else {
                retired.push(record.payload_key);
                retired_bytes = retired_bytes
                    .checked_add(
                        u64::try_from(record.value.as_bytes().len())
                            .context("RPC transaction GC retired-byte count overflow")?,
                    )
                    .context("RPC transaction GC retired-byte count overflow")?;
                staged_bytes = staged_bytes
                    .checked_add(record.retirement_staged_write_bytes(intent.target_generation)?)
                    .context("RPC transaction GC staged byte count overflow")?;
            }
        }
        cursor_lt = Some(
            codec::decode_tail_payload_key(&records.last().unwrap().payload_key)
                .expect("validated RPC tail payload key")
                .1,
        );
    }
    promoted.sort_unstable_by_key(|transaction| transaction.payload_key());
    retired.sort_unstable();
    let counters = codec::TailGenerationCounters {
        processed_accounts: 1,
        promoted_records: promoted_count,
        promoted_bytes,
        retired_records: u64::try_from(retired.len())
            .context("RPC transaction GC retired-record count overflow")?,
        retired_bytes,
    };
    Ok(GcAccountDelta {
        delta: AccountTailDelta::new(account, promoted, retired)?,
        counters,
        staged_bytes,
    })
}

fn count_newer_account_transactions(
    snapshot: &RpcSnapshot,
    source_id: PartitionId,
    account: AccountKey,
    limit: u64,
) -> Result<u64> {
    if limit == 0 {
        metrics::histogram!("tycho_storage_rpc_gc_newer_partitions_probed").record(0.0);
        return Ok(0);
    }
    let source_index = snapshot
        .0
        .descriptor_indices
        .get(&source_id)
        .copied()
        .context("RPC transaction GC source is missing from the published snapshot")?;
    let mut count = 0u64;
    let mut probed_partitions = 0u64;
    for descriptor in snapshot.0.descriptors[source_index + 1..].iter().rev() {
        if descriptor.first.block_id.is_none()
            || descriptor.first.mc_seqno > snapshot.visible_frontier().seqno
        {
            continue;
        }
        probed_partitions += 1;
        let (filter_result, filtered_positive) = match snapshot.filter_bundle(descriptor.id) {
            Some(_) => match snapshot.filter_might_contain(
                descriptor.id,
                FilterNamespace::Accounts,
                &account,
            ) {
                Ok(false) => (GcAccountFilterResult::Negative, false),
                Ok(true) => (GcAccountFilterResult::Positive, true),
                Err(error) => {
                    tracing::warn!(partition_id = descriptor.id.0,
                        "RPC transaction GC account filter failed; using exact scan: {error:#}");
                    (GcAccountFilterResult::Unknown, false)
                }
            },
            None => (GcAccountFilterResult::Unknown, false),
        };
        record_gc_account_filter_probe(filter_result);
        if filter_result == GcAccountFilterResult::Negative {
            continue;
        }
        metrics::counter!("tycho_storage_rpc_gc_account_exact_seeks_total").increment(1);
        let remaining = limit
            .checked_sub(count)
            .expect("bounded RPC transaction GC account count");
        let found = count_partition_account_transaction_keys(
            snapshot,
            descriptor.id,
            account,
            remaining,
            descriptor.lifecycle == codec::ManifestLifecycle::Sealed,
        )?;
        if filtered_positive && found == 0 {
            metrics::counter!("tycho_storage_rpc_gc_account_filter_false_positives_total")
                .increment(1);
        }
        count = count
            .checked_add(found)
            .context("RPC transaction GC newer-record count overflow")?;
        if count == limit {
            break;
        }
    }
    metrics::histogram!("tycho_storage_rpc_gc_newer_partitions_probed")
        .record(probed_partitions as f64);
    Ok(count)
}

fn count_partition_account_transaction_keys(
    snapshot: &RpcSnapshot,
    id: PartitionId,
    account: AccountKey,
    limit: u64,
    is_sealed: bool,
) -> Result<u64> {
    if limit == 0 {
        return Ok(0);
    }
    let partition = acquire_partition_read(snapshot.clone(), id)?;
    let table = &partition.lease.transactions;
    let read_options = partition.read_options(table)?;
    let mut iterator = partition
        .lease
        .rocksdb()
        .raw_iterator_cf_opt(&table.cf(), read_options);
    iterator.seek_for_prev(codec::tail_payload_key(account, u64::MAX));
    let mut count = 0u64;
    while count < limit {
        let Some(key) = iterator.key() else {
            break;
        };
        let (key_account, _) = codec::decode_tail_payload_key(key).map_err(|_| {
            if is_sealed {
                malformed_authoritative_error(
                    "invalid committed RPC transaction key in a newer sealed partition",
                )
            } else {
                anyhow::anyhow!("invalid RPC transaction key in a newer local partition")
            }
        })?;
        if key_account != account {
            break;
        }
        count = count
            .checked_add(1)
            .context("RPC transaction GC newer-record count overflow")?;
        iterator.prev();
    }
    iterator.status()?;
    Ok(count)
}

fn load_candidate_promotions(
    source: &PartitionReadLease,
    account: AccountKey,
    limit: u64,
    max_mc_seqno: u32,
    target_generation: u64,
) -> Result<Vec<TailPromotedTransaction>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let table = &source.transactions;
    let read_options = table.new_read_config();
    let mut iterator = source
        .rocksdb()
        .raw_iterator_cf_opt(&table.cf(), read_options);
    iterator.seek_for_prev(codec::tail_payload_key(account, u64::MAX));
    let mut result = Vec::new();
    let mut count = 0u64;
    while count < limit {
        let Some((key, value)) = iterator.item() else {
            break;
        };
        let payload_key = <[u8; tables::Transactions::KEY_LEN]>::try_from(key).map_err(|_| {
            malformed_authoritative_error(
                "RPC transaction GC candidate contains an invalid transaction key",
            )
        })?;
        let (key_account, lt) = codec::decode_tail_payload_key(&payload_key).map_err(|_| {
            malformed_authoritative_error(
                "RPC transaction GC candidate contains an invalid transaction key",
            )
        })?;
        if key_account != account {
            break;
        }
        let transaction = codec::decode_transaction_value(value).map_err(|_| {
            malformed_authoritative_error(
                "RPC transaction GC candidate contains an invalid transaction value",
            )
        })?;
        if transaction.mc_seqno() > max_mc_seqno {
            return Err(conflicting_authoritative_error(
                "RPC transaction GC candidate transaction is newer than the frozen frontier",
            ));
        }
        let payload = transaction.payload();
        let mask = TransactionMask::from_bits(payload[0]).ok_or_else(|| {
            malformed_authoritative_error(
                "RPC transaction GC candidate contains an invalid transaction mask",
            )
        })?;
        let transaction_hash = HashBytes::from_slice(&payload[1..33]);
        let locator = source
            .transactions_by_hash
            .get(transaction_hash)?
            .ok_or_else(|| missing_authoritative_error(
                "RPC transaction GC candidate transaction is missing its hash locator",
            ))?;
        if locator.len() != tables::TransactionsByHash::VALUE_FULL_LEN {
            return Err(malformed_authoritative_error(
                "RPC transaction GC candidate hash locator has an invalid length",
            ));
        }
        if locator[41] >= 64 {
            return Err(malformed_authoritative_error(
                "RPC transaction GC candidate hash locator has an invalid shard prefix",
            ));
        }
        let info = TransactionInfo::from_bytes(locator.as_ref())
            .ok_or_else(|| malformed_authoritative_error(
                "RPC transaction GC candidate hash locator is invalid",
            ))?;
        let mut info_account = [0; codec::ACCOUNT_KEY_LEN];
        info_account[0] = info.account.workchain as u8;
        info_account[1..].copy_from_slice(info.account.address.as_slice());
        // every required hash locator field must identify the selected committed transaction
        if info_account != account
            || info.lt != lt
            || info.mc_seqno != transaction.mc_seqno()
        {
            return Err(conflicting_authoritative_error(
                "RPC transaction GC candidate transaction conflicts with its hash locator",
            ));
        }
        let expected_in_msg = mask.has_msg_hash()
            .then(|| HashBytes::from_slice(&payload[33..65]));
        if let Some(in_msg_hash) = expected_in_msg {
            let in_msg_locator = source
                .transactions_by_in_msg
                .get(in_msg_hash)?
                .ok_or_else(|| missing_authoritative_error(
                    "RPC transaction GC candidate transaction is missing its inbound-message locator",
                ))?;
            if in_msg_locator.len() != tables::Transactions::KEY_LEN {
                return Err(malformed_authoritative_error(
                    "RPC transaction GC candidate inbound-message locator has an invalid length",
                ));
            }
            if in_msg_locator.as_ref() != payload_key {
                return Err(conflicting_authoritative_error(
                    "RPC transaction GC candidate inbound-message locator points to a different transaction",
                ));
            }
        }
        let promoted = TailPromotedTransaction::new(
            payload_key,
            value.to_vec(),
            info.block_id,
            target_generation,
        )
        .map_err(|_| malformed_authoritative_error(
            "RPC transaction GC candidate promotion is invalid",
        ))?;
        if promoted.transaction_hash() != transaction_hash
            || promoted.in_msg_hash() != expected_in_msg
        {
            return Err(conflicting_authoritative_error(
                "RPC transaction GC candidate promotion changed transaction identity",
            ));
        }
        result.push(promoted);
        count = count
            .checked_add(1)
            .context("RPC transaction GC promoted-record count overflow")?;
        iterator.prev();
    }
    iterator.status()?;
    Ok(result)
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
    sealed_openers: BTreeMap<PartitionId, SealedPartitionLeaseOpener>,
    tail: TailRequestSnapshot,
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
    let mut published = snapshots.lock_for_publication()?;
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
    tail: &TailStore,
    visible_frontier: BlockId,
) -> Result<RpcSnapshot> {
    let descriptors = partitions
        .descriptors()
        .into_iter()
        .filter(|descriptor| matches!(
            descriptor.lifecycle,
            codec::ManifestLifecycle::Active
                | codec::ManifestLifecycle::Sealing
                | codec::ManifestLifecycle::Sealed
        ))
        .collect::<Vec<_>>();
    let descriptor_indices = descriptors
        .iter()
        .enumerate()
        .map(|(index, descriptor)| (descriptor.id, index))
        .collect();
    let mut writable_partitions = BTreeMap::new();
    let mut sealed_openers = BTreeMap::new();
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
            codec::ManifestLifecycle::Sealed => {
                sealed_openers.insert(
                    descriptor.id,
                    partitions.sealed_lease_opener(descriptor.id)?,
                );
            }
            codec::ManifestLifecycle::Creating
            | codec::ManifestLifecycle::Retired
            | codec::ManifestLifecycle::Deleting => unreachable!(),
        }
    }
    anyhow::ensure!(
        writable_partitions.contains_key(&partitions.active_id()),
        "active transaction partition is missing from RPC snapshot"
    );
    let filter_bundles = snapshot_filter_bundles(partitions, filter_registry, &descriptors)?;
    let tail = tail.request_snapshot(
        partitions.tail_layout_version(),
        partitions.tail_visible_generation(),
    )?;
    Ok(RpcSnapshot(
        Arc::new(RpcSnapshotInner {
            visible_frontier,
            manifest_epoch: partitions.manifest_epoch(),
            descriptors,
            descriptor_indices,
            current_state: partitions.current_state_db().owned_snapshot(),
            writable_partitions,
            sealed_openers,
            tail,
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

    pub(super) fn tail_snapshot(&self) -> &TailRequestSnapshot {
        &self.0.tail
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
            let opener = snapshot
                .0
                .sealed_openers
                .get(&id)
                .with_context(|| {
                    format!(
                        "sealed transaction partition {} opener is missing from the RPC snapshot",
                        id.0
                    )
                })?;
            opener.open()?
        }
        codec::ManifestLifecycle::Creating => {
            anyhow::bail!("transaction partition {} is still creating", id.0);
        }
        codec::ManifestLifecycle::Retired | codec::ManifestLifecycle::Deleting => {
            anyhow::bail!("transaction partition {} is not visible in new RPC snapshots", id.0);
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
    partition_ids: Vec<PartitionId>,
    range_empty: bool,
    start_lt: u64,
    end_lt: u64,
    range_from: [u8; tables::Transactions::KEY_LEN],
    range_to: [u8; tables::Transactions::KEY_LEN],
    snapshot_publisher: Weak<SnapshotPublisher>,
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
            partition_ids: self.partition_ids,
            range_empty: self.range_empty,
            start_lt: self.start_lt,
            end_lt: self.end_lt,
            range_from: self.range_from,
            range_to: self.range_to,
            tail_cursor_lt: None,
            tail_next: None,
            tail_exhausted: false,
            last_key: None,
            ordering_failed: false,
            map,
            snapshot_publisher: self.snapshot_publisher,
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
            partition_ids: self.partition_ids,
            range_empty: self.range_empty,
            start_lt: self.start_lt,
            end_lt: self.end_lt,
            range_from: self.range_from,
            range_to: self.range_to,
            tail_cursor_lt: None,
            tail_next: None,
            tail_exhausted: false,
            last_key: None,
            ordering_failed: false,
            map,
            snapshot_publisher: self.snapshot_publisher,
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
    partition_ids: Vec<PartitionId>,
    range_empty: bool,
    start_lt: u64,
    end_lt: u64,
    range_from: [u8; tables::Transactions::KEY_LEN],
    range_to: [u8; tables::Transactions::KEY_LEN],
    tail_cursor_lt: Option<u64>,
    tail_next: Option<TailTransactionRecord>,
    tail_exhausted: bool,
    last_key: Option<[u8; tables::Transactions::KEY_LEN]>,
    ordering_failed: bool,
    map: F,
    snapshot_publisher: Weak<SnapshotPublisher>,
    snapshot: RpcSnapshot,
}

pub type TransactionsExtIter<F> = TransactionsIter<F, true>;

enum AccountTransactionSource {
    Live,
    Tail,
    LiveAndTail,
}

impl<F, const EXT: bool> TransactionsIter<F, EXT> {
    #[inline]
    pub fn is_reversed(&self) -> bool {
        self.is_reversed
    }

    #[inline]
    pub fn snapshot(&self) -> &RpcSnapshot {
        &self.snapshot
    }

    fn next_tail_transaction(&mut self) -> Option<TailTransactionRecord> {
        if self.tail_exhausted || self.ordering_failed {
            return None;
        }
        let account = self.range_from[..codec::ACCOUNT_KEY_LEN]
            .try_into()
            .expect("transaction range contains a complete account key");
        match self.snapshot.tail_snapshot().next_account_transaction(
            account,
            self.start_lt,
            self.end_lt,
            self.is_reversed,
            self.tail_cursor_lt,
            self.visible_frontier_seqno,
        ) {
            Ok(Some(record)) => {
                self.tail_cursor_lt = Some(
                    codec::decode_tail_payload_key(&record.payload_key)
                        .expect("validated RPC tail payload key")
                        .1,
                );
                Some(record)
            }
            Ok(None) => {
                self.tail_exhausted = true;
                None
            }
            Err(e) => {
                if let Some(kind) = classify_authoritative_error(&e)
                    && let Some(snapshot_publisher) = self.snapshot_publisher.upgrade()
                {
                    snapshot_publisher.transition_to_resync_required(kind, &e);
                }
                tracing::error!("RPC tail account iterator failed: {e:#}");
                self.tail_exhausted = true;
                self.next_partition = self.partition_ids.len();
                self.current = None;
                self.ordering_failed = true;
                None
            }
        }
    }

    fn fill_tail_next(&mut self) {
        if self.tail_next.is_none() && !self.tail_exhausted {
            self.tail_next = self.next_tail_transaction();
        }
    }

    fn fail_live_integrity(&mut self, message: &'static str) {
        let error = if self.current.as_ref().is_some_and(|current| {
            current.partition.lease.lifecycle() == codec::ManifestLifecycle::Sealed
        }) {
            malformed_authoritative_error(message)
        } else {
            anyhow::anyhow!(message)
        };
        if let Some(kind) = classify_authoritative_error(&error)
            && let Some(snapshot_publisher) = self.snapshot_publisher.upgrade()
        {
            snapshot_publisher.transition_to_resync_required(kind, &error);
        }
        tracing::error!("RPC local account iterator failed: {error:#}");
        self.next_partition = self.partition_ids.len();
        self.current = None;
        self.ordering_failed = true;
    }

    fn accept_key(&mut self, key: &[u8]) -> bool {
        let key = match <[u8; tables::Transactions::KEY_LEN]>::try_from(key) {
            Ok(key) => key,
            Err(_) => {
                tracing::error!("RPC account iterator returned an invalid transaction key length");
                self.ordering_failed = true;
                return false;
            }
        };
        if let Some(previous) = self.last_key {
            match key.cmp(&previous) {
                std::cmp::Ordering::Equal => return false,
                std::cmp::Ordering::Less if self.is_reversed => {}
                std::cmp::Ordering::Greater if !self.is_reversed => {}
                _ => {
                    tracing::error!("RPC account iterator sources are not globally ordered");
                    self.ordering_failed = true;
                    return false;
                }
            }
        }
        self.last_key = Some(key);
        true
    }

    fn ensure_live_current(&mut self) -> bool {
        loop {
            if self.current.is_none() && !self.open_next_partition() {
                return false;
            }
            let Some(key) = self.current.as_ref().unwrap().inner.key() else {
                if self.finish_current_partition() {
                    continue;
                }
                return false;
            };
            let key = match <[u8; tables::Transactions::KEY_LEN]>::try_from(key) {
                Ok(key) => key,
                Err(_) => {
                    self.fail_live_integrity(
                        "RPC local account iterator returned an invalid transaction key",
                    );
                    return false;
                }
            };
            if key[..codec::ACCOUNT_KEY_LEN] != self.range_from[..codec::ACCOUNT_KEY_LEN] {
                if self.finish_current_partition() {
                    continue;
                }
                return false;
            }
            let lt = u64::from_be_bytes(key[codec::ACCOUNT_KEY_LEN..].try_into().unwrap());
            if lt < self.start_lt || lt > self.end_lt {
                if self.finish_current_partition() {
                    continue;
                }
                return false;
            }
            let value = self.current.as_ref().unwrap().inner.value().unwrap();
            let related_mc_seqno = match TransactionData::related_mc_seqno(value) {
                Ok(related_mc_seqno) => related_mc_seqno,
                Err(_) => {
                    self.fail_live_integrity(
                        "RPC local account iterator returned an invalid transaction value",
                    );
                    return false;
                }
            };
            if related_mc_seqno <= self.visible_frontier_seqno {
                return true;
            }
            self.advance_live();
        }
    }

    fn advance_live(&mut self) {
        if self.is_reversed {
            self.current.as_mut().unwrap().inner.prev();
        } else {
            self.current.as_mut().unwrap().inner.next();
        }
    }

    fn next_source(&mut self) -> Option<AccountTransactionSource> {
        if self.range_empty || self.ordering_failed {
            return None;
        }
        self.fill_tail_next();
        let has_live = self.ensure_live_current();
        if self.ordering_failed {
            return None;
        }
        let live_key = has_live.then(|| {
            <[u8; tables::Transactions::KEY_LEN]>::try_from(
                self.current.as_ref().unwrap().inner.key().expect("live iterator has a value"),
            )
            .expect("validated RPC transaction key length")
        });
        let tail_key = self.tail_next.as_ref().map(|record| record.payload_key);
        match (live_key, tail_key) {
            (None, None) => None,
            (Some(_), None) => Some(AccountTransactionSource::Live),
            (None, Some(_)) => Some(AccountTransactionSource::Tail),
            (Some(live), Some(tail)) => match live.cmp(&tail) {
                std::cmp::Ordering::Equal => Some(AccountTransactionSource::LiveAndTail),
                std::cmp::Ordering::Less if !self.is_reversed => {
                    Some(AccountTransactionSource::Live)
                }
                std::cmp::Ordering::Greater if self.is_reversed => {
                    Some(AccountTransactionSource::Live)
                }
                _ => Some(AccountTransactionSource::Tail),
            },
        }
    }

    fn open_next_partition(&mut self) -> bool {
        let Some(id) = self.partition_ids.get(self.next_partition).copied() else {
            return false;
        };
        self.next_partition += 1;
        self.visited_partitions += 1;
        let partition = match acquire_partition_read(self.snapshot.clone(), id) {
            Ok(partition) => partition,
            Err(e) => {
                if let Some(kind) = classify_authoritative_error(&e)
                    && let Some(snapshot_publisher) = self.snapshot_publisher.upgrade()
                {
                    snapshot_publisher.transition_to_resync_required(kind, &e);
                }
                tracing::error!(
                    partition_id = id.0,
                    "failed to open RPC transaction partition during account iteration: {e:#}"
                );
                self.next_partition = self.partition_ids.len();
                self.ordering_failed = true;
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
                self.ordering_failed = true;
                return false;
            }
        };
        readopts.set_iterate_lower_bound(self.range_from);
        if self.end_lt != u64::MAX {
            readopts.set_iterate_upper_bound(self.range_to);
        }
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
            self.ordering_failed = true;
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
            self.ordering_failed = true;
        }
        self.current = None;
        if self.ordering_failed {
            return false;
        }
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
            let source = self.next_source()?;
            if matches!(source, AccountTransactionSource::Tail) {
                let record = self.tail_next.take().unwrap();
                if !self.accept_key(&record.payload_key) {
                    if self.ordering_failed {
                        return None;
                    }
                    continue;
                }
                if let Some(result) = (self.map)(
                    TransactionData::read_transaction(record.value.as_bytes()),
                ) {
                    return Some(result);
                }
                continue;
            }
            if matches!(source, AccountTransactionSource::LiveAndTail) {
                self.tail_next = None;
            }
            let key = self.current.as_ref().unwrap().inner.key().unwrap().to_vec();
            if !self.accept_key(&key) {
                self.advance_live();
                if self.ordering_failed {
                    return None;
                }
                continue;
            }
            let result = (self.map)(TransactionData::read_transaction(
                self.current.as_mut().unwrap().inner.value().unwrap(),
            ));
            self.advance_live();
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
            let source = self.next_source()?;
            if matches!(source, AccountTransactionSource::Tail) {
                let record = self.tail_next.take().unwrap();
                if !self.accept_key(&record.payload_key) {
                    if self.ordering_failed {
                        return None;
                    }
                    continue;
                }
                let (_, lt) = codec::decode_tail_payload_key(&record.payload_key)
                    .expect("validated RPC tail payload key");
                let hash = record.value.transaction_hash();
                if let Some(result) = (self.map)(
                    lt,
                    &hash,
                    TransactionData::read_transaction(record.value.as_bytes()),
                ) {
                    return Some(result);
                }
                continue;
            }
            if matches!(source, AccountTransactionSource::LiveAndTail) {
                self.tail_next = None;
            }
            let key = self.current.as_ref().unwrap().inner.key().unwrap().to_vec();
            if !self.accept_key(&key) {
                self.advance_live();
                if self.ordering_failed {
                    return None;
                }
                continue;
            }
            let value = self.current.as_mut().unwrap().inner.value().unwrap();
            let result = (self.map)(
                u64::from_be_bytes(key[33..41].try_into().unwrap()),
                &TransactionData::read_tx_hash(value),
                TransactionData::read_transaction(value),
            );
            self.advance_live();
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
    use super::super::tail::{AccountTailDelta, TailPromotedTransaction};
    use std::str::FromStr;
    use std::sync::Barrier;
    use tycho_rpc_subscriptions::SubscriberManagerConfig;
    use tycho_types::boc::Boc;

    fn test_partitions_config() -> RpcTransactionPartitionsConfig {
        RpcTransactionPartitionsConfig {
            target_transaction_lsm_bytes: 1,
            target_transaction_blob_bytes: 1,
            target_transaction_index_records: 1,
            max_open_sealed_partitions: 1,
            ..Default::default()
        }
    }

    fn open_test_tail(
        context: &StorageContext,
        manager: &PartitionManager,
    ) -> Arc<TailStore> {
        Arc::new(TailStore::open(
            context,
            manager.tail_identity(),
            manager.tail_visible_generation(),
        ).unwrap())
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

    fn replay_test_block() -> BlockStuff {
        let block_data = include_bytes!("../../../core/tests/data/block.bin");
        let root = Boc::decode(block_data).unwrap();
        let block = root.parse::<Block>().unwrap();
        let block_id = BlockId::from_str(
            include_str!("../../../core/tests/data/block_id.txt").trim_end(),
        )
        .unwrap();
        BlockStuff::from_block_and_root(&block_id, block, root, block_data.len())
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
        test_filter_bundle_with_accounts(
            partition_id,
            generation_id,
            manifest_digest,
            transaction_keys,
            inbound_message_keys,
            block_keys,
            &[],
        )
    }

    fn test_filter_bundle_with_accounts(
        partition_id: PartitionId,
        generation_id: u128,
        manifest_digest: HashBytes,
        transaction_keys: &[HashBytes],
        inbound_message_keys: &[HashBytes],
        block_keys: &[[u8; tables::KnownBlocks::KEY_LEN]],
        account_keys: &[AccountKey],
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
        let account_keys = account_keys
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
                build(
                    FilterNamespace::Accounts,
                    partition_id,
                    generation_id,
                    manifest_digest,
                    &account_keys,
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
        estimated_transaction_lsm_bytes: u64,
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
        if !transactions.is_empty() {
            let mut account_key = [0; tables::Accounts::KEY_LEN];
            account_key[0] = account.workchain as u8;
            account_key[1..].copy_from_slice(account.address.as_slice());
            local_batch.put_cf(&lease.accounts.cf(), account_key, []);
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
            (block_id, transactions.len() as u64, estimated_transaction_lsm_bytes),
            (mc_block_id, 0, 0),
        ] {
            let commit = codec::PartitionCommit {
                block_id: *current_block_id,
                digest: current_block_id.root_hash,
                transaction_count,
                estimated_transaction_lsm_bytes: lsm_bytes,
                estimated_transaction_blob_bytes: 0,
                transaction_index_record_count: transaction_count * 4,
                estimated_block_metadata_bytes: 0,
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
        estimated_transaction_lsm_bytes: u64,
    ) {
        let commit = codec::PartitionCommit {
            block_id: *block_id,
            digest: block_id.root_hash,
            transaction_count: 0,
            estimated_transaction_lsm_bytes,
            estimated_transaction_blob_bytes: 0,
            transaction_index_record_count: 0,
            estimated_block_metadata_bytes: 0,
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

    fn persisted_partition_aggregate_and_control(
        storage: &RpcStorage,
        partition_id: PartitionId,
    ) -> [Vec<u8>; 4] {
        let manager = storage.partitions.lock();
        let control = manager.control_db();
        [
            control
                .manifests
                .get(codec::partition_manifest_key(partition_id.0))
                .unwrap()
                .unwrap()
                .to_vec(),
            control
                .state
                .get(codec::control_state_key())
                .unwrap()
                .unwrap()
                .to_vec(),
            control
                .state
                .get(codec::visible_frontier_key())
                .unwrap()
                .unwrap()
                .to_vec(),
            control
                .state
                .get(codec::manifest_epoch_key())
                .unwrap()
                .unwrap()
                .to_vec(),
        ]
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

    fn gc_test_config(
        accounts_per_chunk: usize,
        max_staged_bytes_per_batch: u64,
    ) -> RpcTransactionPartitionsConfig {
        RpcTransactionPartitionsConfig {
            target_transaction_lsm_bytes: 1,
            target_transaction_blob_bytes: u64::MAX,
            target_transaction_index_records: u64::MAX,
            target_block_metadata_bytes: u64::MAX,
            max_open_sealed_partitions: 8,
            maintenance: RpcTransactionMaintenanceConfig {
                gc_accounts_per_chunk: accounts_per_chunk,
                gc_max_staged_bytes_per_batch: max_staged_bytes_per_batch,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn gc_account(byte: u8) -> StdAddr {
        StdAddr::new(0, HashBytes([byte; 32]))
    }

    fn gc_account_key(account: &StdAddr) -> AccountKey {
        let mut key = [0; codec::ACCOUNT_KEY_LEN];
        key[0] = account.workchain as u8;
        key[1..].copy_from_slice(account.address.as_slice());
        key
    }

    fn gc_transaction(lt: u64, byte: u8) -> ReadTestTransaction {
        ReadTestTransaction {
            lt,
            hash: HashBytes([byte; 32]),
            in_msg_hash: HashBytes([byte.wrapping_add(0x40); 32]),
            boc_byte: byte,
        }
    }

    fn gc_tail_promotion(
        account: AccountKey,
        lt: u64,
        byte: u8,
        mc_seqno: u32,
        generation: u64,
    ) -> (TailPromotedTransaction, u64) {
        let transaction = gc_transaction(lt, byte);
        let mut payload = Vec::with_capacity(66);
        payload.push(TransactionMask::HAS_MSG_HASH.bits());
        payload.extend_from_slice(transaction.hash.as_slice());
        payload.extend_from_slice(transaction.in_msg_hash.as_slice());
        payload.push(transaction.boc_byte);
        let value = codec::encode_transaction_value(mc_seqno, &payload).unwrap();
        let value_len = value.len() as u64;
        (
            TailPromotedTransaction::new(
                codec::tail_payload_key(account, lt),
                value,
                basechain_block(mc_seqno),
                generation,
            )
            .unwrap(),
            value_len,
        )
    }

    async fn seal_gc_partition(storage: &RpcStorage, id: PartitionId) {
        seal_partition(
            storage.partitions.clone(),
            storage.tail.clone(),
            storage.snapshots.clone(),
            storage.filter_registry.clone(),
            storage.maintenance.clone(),
            id,
            CancellationFlag::new(),
            None,
        )
        .await
        .unwrap();
    }

    fn tail_account_lts(
        storage: &RpcStorage,
        generation: u64,
        account: AccountKey,
        max_mc_seqno: u32,
    ) -> Vec<u64> {
        storage
            .tail
            .request_snapshot(codec::TailLayoutVersion::MonolithicV1, generation)
            .unwrap()
            .newest_account_transactions(account, None, usize::MAX, max_mc_seqno)
            .unwrap()
            .into_iter()
            .map(|record| codec::decode_tail_payload_key(&record.payload_key).unwrap().1)
            .collect()
    }

    struct GcCutoverFixture {
        prepared: codec::GcIntent,
        pre_cutover: RpcSnapshot,
        source: PartitionId,
        source_block: BlockId,
        transaction_hash: HashBytes,
        expected_watermark: u64,
    }

    async fn prepare_gc_cutover_fixture_inner(
        storage: &RpcStorage,
        filter_context: Option<&StorageContext>,
    ) -> (GcCutoverFixture, Option<std::path::PathBuf>) {
        storage.sealing_cancel.cancel();
        if filter_context.is_none() {
            storage.filter_worker.shutdown();
        }
        let source_account = gc_account(0x91);
        let source_block = basechain_block(1);
        let source_transaction = gc_transaction(10, 0x92);
        let transaction_hash = source_transaction.hash;
        let candidate_mc = masterchain_block(1);
        let source = insert_read_test_block(
            storage,
            &source_account,
            &candidate_mc,
            &source_block,
            &[source_transaction],
            1,
        );
        storage.commit_masterchain_block_set(&candidate_mc).unwrap();
        seal_gc_partition(storage, source).await;
        let filter_generation_path = filter_context.map(|context| {
            let store = FilterCatalogStore::open(context, Default::default()).unwrap();
            let bundle = store
                .build_and_publish(
                    &storage.partitions,
                    source,
                    &CancellationFlag::new(),
                )
                .unwrap();
            let path = context.root_dir().path().join("rpc/filters").join(
                codec::format_filter_generation_directory(source.0, bundle.generation_id()),
            );
            storage.filter_registry.install(source, bundle);
            path
        });

        let newer_account = gc_account(0x93);
        let newer_mc = masterchain_block(2);
        insert_read_test_block(
            storage,
            &newer_account,
            &newer_mc,
            &basechain_block(2),
            &[gc_transaction(20, 0x94)],
            0,
        );
        storage.commit_masterchain_block_set(&newer_mc).unwrap();
        let frontier = masterchain_block(100);
        publish_test_frontier(storage, &frontier);
        let pre_cutover = storage.load_snapshot().unwrap();
        let prepared = storage
            .evacuate_gc(Some(&TransactionsGcConfig {
                tx_ttl: Duration::from_secs(10),
                keep_tx_per_account: 1,
            }))
            .unwrap()
            .unwrap();
        assert_eq!(prepared.phase, codec::GcIntentPhase::Prepared);

        (
            GcCutoverFixture {
                prepared,
                pre_cutover,
                source,
                source_block,
                transaction_hash,
                expected_watermark: 20,
            },
            filter_generation_path,
        )
    }

    async fn prepare_gc_cutover_fixture(storage: &RpcStorage) -> GcCutoverFixture {
        prepare_gc_cutover_fixture_inner(storage, None).await.0
    }

    async fn prepare_gc_deletion_fixture(
        storage: &RpcStorage,
        context: &StorageContext,
    ) -> (GcCutoverFixture, std::path::PathBuf) {
        let (fixture, filter_generation_path) =
            prepare_gc_cutover_fixture_inner(storage, Some(context)).await;
        (fixture, filter_generation_path.unwrap())
    }

    struct GcTailSweepFixture {
        storage: RpcStorage,
        old_tail: TailRequestSnapshot,
        retired_payload_key: codec::TailPayloadKey,
        committed: codec::GcIntent,
    }

    async fn prepare_gc_tail_sweep_fixture(context: &StorageContext) -> GcTailSweepFixture {
        let config = gc_test_config(128, 16 * 1024 * 1024);
        let storage = RpcStorage::open(context.clone(), config.clone()).unwrap();
        storage.sealing_cancel.cancel();
        storage.filter_worker.shutdown();
        let account = gc_account(0x41);
        let account_key = gc_account_key(&account);
        let (tail5, bytes5) = gc_tail_promotion(account_key, 5, 0x51, 1, 1);
        let retired_payload_key = tail5.payload_key();
        let (tail15, bytes15) = gc_tail_promotion(account_key, 15, 0x52, 1, 1);
        let counters = codec::TailGenerationCounters {
            processed_accounts: 1,
            promoted_records: 2,
            promoted_bytes: bytes5 + bytes15,
            ..Default::default()
        };
        let progress = codec::TailGenerationProgress {
            target_generation: 1,
            operation_id: 0x101,
            source_partition_id: 7,
            source_manifest_digest: HashBytes([0x61; 32]),
            retention_policy_digest: HashBytes([0x62; 32]),
            cursor: codec::TailProgressCursor::Account(account_key),
            eof: false,
            counters,
            chunk_digest: codec::EMPTY_TAIL_CHUNK_DIGEST,
        };
        let progress = storage
            .tail
            .append_chunk(
                &[AccountTailDelta::new(account_key, vec![tail5, tail15], vec![]).unwrap()],
                None,
                progress,
            )
            .unwrap();
        storage
            .tail
            .finish_generation(
                codec::TailGenerationProgress { eof: true, ..progress },
                codec::TailGenerationCommit {
                    layout_version: codec::TailLayoutVersion::MonolithicV1,
                    target_generation: 1,
                    operation_id: progress.operation_id,
                    source_partition_id: progress.source_partition_id,
                    source_manifest_digest: progress.source_manifest_digest,
                    previous_visible_generation: 0,
                    cutoff_utime: 1,
                    keep_tx_per_account: 2,
                    retention_policy_digest: progress.retention_policy_digest,
                    counters,
                },
            )
            .unwrap();
        {
            let manager = storage.partitions.lock();
            let mut control_state = codec::decode_control_state(
                manager
                    .control_db()
                    .state
                    .get(codec::control_state_key())
                    .unwrap()
                    .unwrap()
                    .as_ref(),
            )
            .unwrap();
            control_state.tail_visible_generation = 1;
            manager
                .control_db()
                .state
                .insert(
                    codec::control_state_key(),
                    codec::encode_control_state(control_state),
                )
                .unwrap();
        }
        drop(storage);
        tokio::task::yield_now().await;

        let storage = RpcStorage::open(context.clone(), config).unwrap();
        storage.sealing_cancel.cancel();
        let candidate_mc = masterchain_block(2);
        let source = insert_read_test_block(
            &storage,
            &account,
            &candidate_mc,
            &basechain_block(2),
            &[gc_transaction(20, 0x53)],
            1,
        );
        storage.commit_masterchain_block_set(&candidate_mc).unwrap();
        seal_gc_partition(&storage, source).await;
        let frontier = masterchain_block(100);
        publish_test_frontier(&storage, &frontier);
        let old_tail = storage
            .tail
            .request_snapshot(codec::TailLayoutVersion::MonolithicV1, 1)
            .unwrap();
        let prepared = storage
            .evacuate_gc(Some(&TransactionsGcConfig {
                tx_ttl: Duration::from_secs(10),
                keep_tx_per_account: 2,
            }))
            .unwrap()
            .unwrap();
        assert_eq!(prepared.target_generation, 2);
        let committed = storage.cutover_prepared_gc(prepared).await.unwrap();
        assert!(storage
            .tail
            .retirement_entry_exists(committed.target_generation, retired_payload_key)
            .unwrap());
        GcTailSweepFixture {
            storage,
            old_tail,
            retired_payload_key,
            committed,
        }
    }

    fn persisted_gc_control(storage: &RpcStorage) -> codec::ControlState {
        let manager = storage.partitions.lock();
        codec::decode_control_state(
            manager
                .control_db()
                .state
                .get(codec::control_state_key())
                .unwrap()
                .unwrap()
                .as_ref(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn gc_cutover_switches_snapshot_authority_and_persists_history_watermark() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = RpcStorage::open(context, gc_test_config(128, 16 * 1024 * 1024)).unwrap();
        let fixture = prepare_gc_cutover_fixture(&storage).await;
        let previous_epoch = fixture.pre_cutover.manifest_epoch();

        let committed = storage.cutover_prepared_gc(fixture.prepared).await.unwrap();
        assert_eq!(committed.phase, codec::GcIntentPhase::CutoverCommitted);
        let post_cutover = storage.load_snapshot().unwrap();
        assert_eq!(fixture.pre_cutover.tail_snapshot().visible_generation(), 0);
        assert_eq!(
            fixture.pre_cutover.descriptor(fixture.source).unwrap().lifecycle,
            codec::ManifestLifecycle::Sealed,
        );
        assert_eq!(
            post_cutover.tail_snapshot().visible_generation(),
            fixture.prepared.target_generation,
        );
        assert!(post_cutover.descriptor(fixture.source).is_none());
        assert_eq!(post_cutover.manifest_epoch(), previous_epoch + 1);

        for snapshot in [&fixture.pre_cutover, &post_cutover] {
            assert_eq!(
                storage
                    .get_transaction(&fixture.transaction_hash, Some(snapshot))
                    .unwrap()
                    .unwrap()
                    .as_ref(),
                [0x92],
            );
        }
        assert!(storage
            .get_brief_block_info(
                &fixture.source_block.as_short_id(),
                Some(&fixture.pre_cutover),
            )
            .unwrap()
            .is_some());
        assert!(storage
            .get_brief_block_info(&fixture.source_block.as_short_id(), Some(&post_cutover))
            .unwrap()
            .is_none());

        assert_eq!(storage.min_tx_lt(), fixture.expected_watermark);
        let manager = storage.partitions.lock();
        assert_eq!(manager.tail_visible_generation(), fixture.prepared.target_generation);
        assert_eq!(manager.min_transaction_lt(), fixture.expected_watermark);
        assert_eq!(manager.gc_intent().unwrap(), Some(committed));
        assert_eq!(
            manager
                .descriptors()
                .into_iter()
                .find(|descriptor| descriptor.id == fixture.source)
                .unwrap()
                .lifecycle,
            codec::ManifestLifecycle::Retired,
        );
        drop(manager);
        let persisted = persisted_gc_control(&storage);
        assert_eq!(persisted.tail_visible_generation, fixture.prepared.target_generation);
        assert_eq!(persisted.smallest_known_lt, fixture.expected_watermark);
    }

    #[tokio::test]
    async fn gc_cutover_failure_stages_have_deterministic_control_and_visibility() {
        for stage in [
            GcCutoverFailureStage::BeforeControl,
            GcCutoverFailureStage::AfterControl,
            GcCutoverFailureStage::PostBuildValidation,
            GcCutoverFailureStage::AfterPublication,
        ] {
            let (context, _tmp) = StorageContext::new_temp().await.unwrap();
            let storage = RpcStorage::open(context, gc_test_config(128, 16 * 1024 * 1024)).unwrap();
            let fixture = prepare_gc_cutover_fixture(&storage).await;
            *storage.snapshots.gc_cutover_failure.lock() = Some(stage);

            let error = storage.cutover_prepared_gc(fixture.prepared).await.unwrap_err();
            assert!(format!("{error:#}").contains("injected RPC transaction GC cutover failure"));
            assert_eq!(classify_authoritative_error(&error), None);
            assert!(!storage.is_resync_required());
            assert_eq!(storage.gc.resync_transitions(), 0);
            assert_eq!(fixture.pre_cutover.tail_snapshot().visible_generation(), 0);
            assert_eq!(
                fixture.pre_cutover.descriptor(fixture.source).unwrap().lifecycle,
                codec::ManifestLifecycle::Sealed,
            );
            assert_eq!(
                storage
                    .get_transaction(
                        &fixture.transaction_hash,
                        Some(&fixture.pre_cutover),
                    )
                    .unwrap()
                    .unwrap()
                    .as_ref(),
                [0x92],
            );

            let control_committed = stage != GcCutoverFailureStage::BeforeControl;
            let manager = storage.partitions.lock();
            assert_eq!(
                manager.tail_visible_generation(),
                if control_committed { fixture.prepared.target_generation } else { 0 },
            );
            assert_eq!(
                manager.min_transaction_lt(),
                if control_committed { fixture.expected_watermark } else { u64::MAX },
            );
            assert_eq!(
                manager.gc_intent().unwrap().unwrap().phase,
                if control_committed {
                    codec::GcIntentPhase::CutoverCommitted
                } else {
                    codec::GcIntentPhase::Prepared
                },
            );
            assert_eq!(
                manager
                    .descriptors()
                    .into_iter()
                    .find(|descriptor| descriptor.id == fixture.source)
                    .unwrap()
                    .lifecycle,
                if control_committed {
                    codec::ManifestLifecycle::Retired
                } else {
                    codec::ManifestLifecycle::Sealed
                },
            );
            drop(manager);
            let persisted = persisted_gc_control(&storage);
            assert_eq!(
                persisted.tail_visible_generation,
                if control_committed { fixture.prepared.target_generation } else { 0 },
            );
            assert_eq!(
                persisted.smallest_known_lt,
                if control_committed { fixture.expected_watermark } else { u64::MAX },
            );
            assert_eq!(storage.min_tx_lt(), persisted.smallest_known_lt);

            match stage {
                GcCutoverFailureStage::BeforeControl => {
                    let published = storage.load_snapshot().unwrap();
                    assert_eq!(published.tail_snapshot().visible_generation(), 0);
                    assert_eq!(
                        published.descriptor(fixture.source).unwrap().lifecycle,
                        codec::ManifestLifecycle::Sealed,
                    );
                }
                GcCutoverFailureStage::AfterControl => {
                    assert!(storage.load_snapshot().is_none());
                }
                GcCutoverFailureStage::PostBuildValidation => {
                    assert!(storage.load_snapshot().is_none());
                }
                GcCutoverFailureStage::AfterPublication => {
                    let published = storage.load_snapshot().unwrap();
                    assert_eq!(
                        published.tail_snapshot().visible_generation(),
                        fixture.prepared.target_generation,
                    );
                    assert!(published.descriptor(fixture.source).is_none());
                    assert_eq!(
                        storage
                            .get_transaction(&fixture.transaction_hash, Some(&published))
                            .unwrap()
                            .unwrap()
                            .as_ref(),
                        [0x92],
                    );
                }
            }

            let replayed = storage.cutover_prepared_gc(fixture.prepared).await.unwrap();
            assert_eq!(replayed.phase, codec::GcIntentPhase::CutoverCommitted);
            let published = storage.load_snapshot().unwrap();
            assert_eq!(
                published.tail_snapshot().visible_generation(),
                fixture.prepared.target_generation,
            );
            assert!(published.descriptor(fixture.source).is_none());
            assert_eq!(published.manifest_epoch(), fixture.pre_cutover.manifest_epoch() + 1);
            assert_eq!(published.visible_frontier(), fixture.pre_cutover.visible_frontier());
            assert_eq!(storage.min_tx_lt(), fixture.expected_watermark);
            assert_eq!(
                storage
                    .get_transaction(&fixture.transaction_hash, Some(&published))
                    .unwrap()
                    .unwrap()
                    .as_ref(),
                [0x92],
            );
        }
    }

    #[tokio::test]
    async fn gc_cutover_reopens_after_committed_control_before_publication() {
        for stage in [
            GcCutoverFailureStage::AfterControl,
            GcCutoverFailureStage::PostBuildValidation,
        ] {
            let (context, _tmp) = StorageContext::new_temp().await.unwrap();
            let config = gc_test_config(128, 16 * 1024 * 1024);
            let storage = RpcStorage::open(context.clone(), config.clone()).unwrap();
            let fixture = prepare_gc_cutover_fixture(&storage).await;
            let frontier = *fixture.pre_cutover.visible_frontier();
            *storage.snapshots.gc_cutover_failure.lock() = Some(stage);

            let error = storage.cutover_prepared_gc(fixture.prepared).await.unwrap_err();
            assert!(format!("{error:#}").contains("injected RPC transaction GC cutover failure"));
            assert_eq!(
                storage.partitions.lock().gc_intent().unwrap().unwrap().phase,
                codec::GcIntentPhase::CutoverCommitted,
            );
            assert!(storage.load_snapshot().is_none());
            drop(fixture.pre_cutover);
            drop(storage);
            tokio::task::yield_now().await;

            let storage = RpcStorage::open_full(context, config, None).unwrap();
            let reconciliation = storage.reconcile_startup(&frontier).unwrap();
            storage.continue_lifecycle().unwrap();
            storage.publish_snapshot(&reconciliation.effective_frontier).unwrap();
            let published = storage.load_snapshot().unwrap();
            assert_eq!(
                published.tail_snapshot().visible_generation(),
                fixture.prepared.target_generation,
            );
            assert!(published.descriptor(fixture.source).is_none());
            assert_eq!(
                storage
                    .get_transaction(&fixture.transaction_hash, Some(&published))
                    .unwrap()
                    .unwrap()
                    .as_ref(),
                [0x92],
            );
        }
    }

    #[tokio::test]
    async fn sealed_transaction_hash_locator_missing_payload_marks_resync() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = RpcStorage::open(context, gc_test_config(128, 16 * 1024 * 1024)).unwrap();
        storage.sealing_cancel.cancel();
        storage.filter_worker.shutdown();
        let account = gc_account(0x89);
        let transaction = gc_transaction(10, 0x8a);
        let frontier = masterchain_block(1);
        let source = insert_read_test_block(
            &storage,
            &account,
            &frontier,
            &basechain_block(1),
            std::slice::from_ref(&transaction),
            1,
        );
        let mut transaction_key = [0; tables::Transactions::KEY_LEN];
        transaction_key[..codec::ACCOUNT_KEY_LEN]
            .copy_from_slice(&gc_account_key(&account));
        transaction_key[codec::ACCOUNT_KEY_LEN..].copy_from_slice(&transaction.lt.to_be_bytes());
        storage
            .partitions
            .lock()
            .active_lease()
            .transactions
            .remove(transaction_key)
            .unwrap();
        storage.commit_masterchain_block_set(&frontier).unwrap();
        seal_gc_partition(&storage, source).await;
        let old_snapshot = storage.load_snapshot().unwrap();

        let error = match storage.get_transaction(&transaction.hash, None) {
            Ok(_) => panic!("sealed transaction with a missing payload must fail"),
            Err(error) => error,
        };
        assert_eq!(
            classify_authoritative_error(&error),
            Some(AuthoritativeErrorKind::MissingCommittedData),
        );
        assert!(storage.is_resync_required());
        assert!(storage.load_snapshot().is_none());
        assert!(storage
            .get_transaction(&transaction.hash, Some(&old_snapshot))
            .is_err());
        assert_eq!(storage.gc.resync_transitions(), 1);
    }

    #[tokio::test]
    async fn lazy_sealed_partition_missing_or_non_directory_marks_resync() {
        for replacement in ["missing", "file"] {
            let (context, _tmp) = StorageContext::new_temp().await.unwrap();
            let storage = RpcStorage::open(
                context.clone(),
                gc_test_config(128, 16 * 1024 * 1024),
            )
            .unwrap();
            storage.sealing_cancel.cancel();
            storage.filter_worker.shutdown();
            let account = gc_account(0xa1);
            let transaction = gc_transaction(10, 0xa2);
            let frontier = masterchain_block(1);
            let source = insert_read_test_block(
                &storage,
                &account,
                &frontier,
                &basechain_block(1),
                std::slice::from_ref(&transaction),
                1,
            );
            storage.commit_masterchain_block_set(&frontier).unwrap();
            seal_gc_partition(&storage, source).await;
            let old_snapshot = storage.load_snapshot().unwrap();
            let source_path = context
                .root_dir()
                .path()
                .join("rpc/transactions")
                .join(source.directory_name());
            storage
                .partitions
                .lock()
                .invalidate_sealed_cache_for_test(source);
            std::fs::remove_dir_all(&source_path).unwrap();
            if replacement == "file" {
                std::fs::write(&source_path, []).unwrap();
            }

            let error = match storage.get_transaction(&transaction.hash, None) {
                Ok(_) => panic!("missing committed sealed partition must fail"),
                Err(error) => error,
            };
            assert_eq!(
                classify_authoritative_error(&error),
                Some(AuthoritativeErrorKind::MissingCommittedData),
            );
            assert!(format!("{error:#}").contains(
                "committed RPC transaction partition directory is missing or not a directory"
            ));
            assert!(storage.is_resync_required());
            assert!(storage.load_snapshot().is_none());
            assert!(storage
                .get_transaction(&transaction.hash, Some(&old_snapshot))
                .is_err());
            assert_eq!(storage.gc.resync_transitions(), 1);
        }
    }

    #[tokio::test]
    async fn filter_maintenance_missing_sealed_source_marks_resync() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = RpcStorage::open(
            context.clone(),
            gc_test_config(128, 16 * 1024 * 1024),
        )
        .unwrap();
        storage.sealing_cancel.cancel();
        let account = gc_account(0xa3);
        let transaction = gc_transaction(10, 0xa4);
        let frontier = masterchain_block(1);
        let source = insert_read_test_block(
            &storage,
            &account,
            &frontier,
            &basechain_block(1),
            std::slice::from_ref(&transaction),
            1,
        );
        storage.commit_masterchain_block_set(&frontier).unwrap();
        seal_gc_partition(&storage, source).await;
        let old_snapshot = storage.load_snapshot().unwrap();
        let source_path = context
            .root_dir()
            .path()
            .join("rpc/transactions")
            .join(source.directory_name());
        storage
            .partitions
            .lock()
            .invalidate_sealed_cache_for_test(source);
        std::fs::remove_dir_all(source_path).unwrap();

        storage.filter_worker.start();
        tokio::time::timeout(
            Duration::from_secs(5),
            storage.snapshots.wait_for_resync_required(),
        )
        .await
        .unwrap();
        assert!(storage.is_resync_required());
        assert!(storage.load_snapshot().is_none());
        assert!(storage
            .get_transaction(&transaction.hash, Some(&old_snapshot))
            .is_err());
        assert_eq!(storage.gc.resync_transitions(), 1);
        storage.filter_worker.shutdown();
    }

    #[tokio::test]
    async fn active_transaction_hash_locator_missing_payload_remains_retryable() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = RpcStorage::open(context, gc_test_config(128, 16 * 1024 * 1024)).unwrap();
        storage.sealing_cancel.cancel();
        storage.filter_worker.shutdown();
        let account = gc_account(0x8b);
        let transaction = gc_transaction(10, 0x8c);
        let frontier = masterchain_block(1);
        insert_read_test_block(
            &storage,
            &account,
            &frontier,
            &basechain_block(1),
            std::slice::from_ref(&transaction),
            1,
        );
        let mut transaction_key = [0; tables::Transactions::KEY_LEN];
        transaction_key[..codec::ACCOUNT_KEY_LEN]
            .copy_from_slice(&gc_account_key(&account));
        transaction_key[codec::ACCOUNT_KEY_LEN..].copy_from_slice(&transaction.lt.to_be_bytes());
        storage
            .partitions
            .lock()
            .active_lease()
            .transactions
            .remove(transaction_key)
            .unwrap();
        storage.commit_masterchain_block_set(&frontier).unwrap();

        let error = match storage.get_transaction(&transaction.hash, None) {
            Ok(_) => panic!("active transaction with a missing payload must fail"),
            Err(error) => error,
        };
        assert_eq!(classify_authoritative_error(&error), None);
        assert!(!storage.is_resync_required());
        assert!(storage.load_snapshot().is_some());
        assert_eq!(storage.gc.resync_transitions(), 0);
    }

    #[tokio::test]
    async fn sealed_account_iterator_malformed_payload_marks_resync() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = RpcStorage::open(context, gc_test_config(128, 16 * 1024 * 1024)).unwrap();
        storage.sealing_cancel.cancel();
        storage.filter_worker.shutdown();
        let account = gc_account(0x8d);
        let transaction = gc_transaction(10, 0x8e);
        let frontier = masterchain_block(1);
        let source = insert_read_test_block(
            &storage,
            &account,
            &frontier,
            &basechain_block(1),
            std::slice::from_ref(&transaction),
            1,
        );
        let mut transaction_key = [0; tables::Transactions::KEY_LEN];
        transaction_key[..codec::ACCOUNT_KEY_LEN]
            .copy_from_slice(&gc_account_key(&account));
        transaction_key[codec::ACCOUNT_KEY_LEN..].copy_from_slice(&transaction.lt.to_be_bytes());
        storage
            .partitions
            .lock()
            .active_lease()
            .transactions
            .insert(transaction_key, [0])
            .unwrap();
        storage.commit_masterchain_block_set(&frontier).unwrap();
        seal_gc_partition(&storage, source).await;
        let mut transactions = storage
            .get_transactions(&account, None, None, false, None)
            .unwrap()
            .map_ext(|lt, _, _| Some(lt));

        assert_eq!(transactions.next(), None);
        assert!(storage.is_resync_required());
        assert!(storage.load_snapshot().is_none());
        assert_eq!(storage.gc.resync_transitions(), 1);
    }

    #[tokio::test]
    async fn active_account_iterator_and_source_read_corruption_remain_retryable() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = RpcStorage::open(context, gc_test_config(128, 16 * 1024 * 1024)).unwrap();
        storage.sealing_cancel.cancel();
        storage.filter_worker.shutdown();
        let account = gc_account(0x8f);
        let transaction = gc_transaction(10, 0x90);
        let frontier = masterchain_block(1);
        insert_read_test_block(
            &storage,
            &account,
            &frontier,
            &basechain_block(1),
            std::slice::from_ref(&transaction),
            0,
        );
        let mut transaction_key = [0; tables::Transactions::KEY_LEN];
        transaction_key[..codec::ACCOUNT_KEY_LEN]
            .copy_from_slice(&gc_account_key(&account));
        transaction_key[codec::ACCOUNT_KEY_LEN..].copy_from_slice(&transaction.lt.to_be_bytes());
        storage
            .partitions
            .lock()
            .active_lease()
            .transactions
            .insert(transaction_key, [0])
            .unwrap();
        storage.commit_masterchain_block_set(&frontier).unwrap();
        let mut transactions = storage
            .get_transactions(&account, None, None, false, None)
            .unwrap()
            .map_ext(|lt, _, _| Some(lt));

        assert_eq!(transactions.next(), None);
        assert!(!storage.is_resync_required());
        let error = match storage.get_src_transaction(&account, 11, None) {
            Ok(_) => panic!("active malformed source transaction must fail"),
            Err(error) => error,
        };
        assert_eq!(classify_authoritative_error(&error), None);
        assert!(!storage.is_resync_required());
        assert!(storage.load_snapshot().is_some());
        assert_eq!(storage.gc.resync_transitions(), 0);
    }

    #[tokio::test]
    async fn sealed_source_transaction_malformed_payload_marks_resync() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = RpcStorage::open(context, gc_test_config(128, 16 * 1024 * 1024)).unwrap();
        storage.sealing_cancel.cancel();
        storage.filter_worker.shutdown();
        let account = gc_account(0x97);
        let transaction = gc_transaction(10, 0x98);
        let frontier = masterchain_block(1);
        let source = insert_read_test_block(
            &storage,
            &account,
            &frontier,
            &basechain_block(1),
            std::slice::from_ref(&transaction),
            1,
        );
        let mut transaction_key = [0; tables::Transactions::KEY_LEN];
        transaction_key[..codec::ACCOUNT_KEY_LEN]
            .copy_from_slice(&gc_account_key(&account));
        transaction_key[codec::ACCOUNT_KEY_LEN..].copy_from_slice(&transaction.lt.to_be_bytes());
        storage
            .partitions
            .lock()
            .active_lease()
            .transactions
            .insert(transaction_key, [0])
            .unwrap();
        storage.commit_masterchain_block_set(&frontier).unwrap();
        seal_gc_partition(&storage, source).await;

        let error = match storage.get_src_transaction(&account, 11, None) {
            Ok(_) => panic!("sealed malformed source transaction must fail"),
            Err(error) => error,
        };
        assert_eq!(
            classify_authoritative_error(&error),
            Some(AuthoritativeErrorKind::MalformedCommittedData),
        );
        assert!(storage.is_resync_required());
        assert!(storage.load_snapshot().is_none());
        assert_eq!(storage.gc.resync_transitions(), 1);
    }

    #[tokio::test]
    async fn committed_tail_read_marks_resync_for_a_missing_payload() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = RpcStorage::open(
            context,
            gc_test_config(128, 16 * 1024 * 1024),
        )
        .unwrap();
        let fixture = prepare_gc_cutover_fixture(&storage).await;
        storage.cutover_prepared_gc(fixture.prepared).await.unwrap();
        let pre_corruption = storage.load_snapshot().unwrap();
        let frontier = *pre_corruption.visible_frontier();
        let payload_key = codec::tail_payload_key(gc_account_key(&gc_account(0x91)), 10);
        storage.tail.remove_transaction_payload(payload_key).unwrap();
        storage.publish_snapshot(&frontier).unwrap();

        let error = match storage.get_transaction(&fixture.transaction_hash, None) {
            Ok(_) => panic!("committed RPC tail read must fail after payload removal"),
            Err(error) => error,
        };
        assert_eq!(
            classify_authoritative_error(&error),
            Some(AuthoritativeErrorKind::MissingCommittedData),
        );
        assert!(storage.is_resync_required());
        assert!(storage.load_snapshot().is_none());
        assert!(storage
            .get_transaction(&fixture.transaction_hash, Some(&pre_corruption))
            .is_err());
        assert_eq!(storage.gc.resync_transitions(), 1);
    }

    #[tokio::test]
    async fn committed_tail_snapshot_rebuild_marks_resync_for_a_missing_commit() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = RpcStorage::open(
            context,
            gc_test_config(128, 16 * 1024 * 1024),
        )
        .unwrap();
        let fixture = prepare_gc_cutover_fixture(&storage).await;
        storage.cutover_prepared_gc(fixture.prepared).await.unwrap();
        let old_snapshot = storage.load_snapshot().unwrap();
        storage
            .tail
            .remove_generation_commit(fixture.prepared.target_generation)
            .unwrap();

        let error = storage
            .publish_snapshot(old_snapshot.visible_frontier())
            .unwrap_err();
        assert_eq!(
            classify_authoritative_error(&error),
            Some(AuthoritativeErrorKind::MissingCommittedData),
        );
        assert!(storage.is_resync_required());
        assert!(storage.load_snapshot().is_none());
        assert!(storage
            .get_known_mc_blocks_range(Some(&old_snapshot))
            .is_err());
        assert_eq!(storage.gc.resync_transitions(), 1);
        storage.transition_to_resync_required_for_test(
            AuthoritativeErrorKind::ConflictingCommittedData,
        );
        assert_eq!(storage.gc.resync_transitions(), 1);
    }

    #[tokio::test]
    async fn committed_tail_boundary_rebuild_marks_resync_for_a_missing_commit() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = RpcStorage::open(
            context,
            gc_test_config(128, 16 * 1024 * 1024),
        )
        .unwrap();
        let fixture = prepare_gc_cutover_fixture(&storage).await;
        storage.cutover_prepared_gc(fixture.prepared).await.unwrap();
        let old_snapshot = storage.load_snapshot().unwrap();
        let next = masterchain_block(old_snapshot.visible_frontier().seqno + 1);
        assert_eq!(storage.admit_block_set(&next).unwrap(), BlockSetMode::New);
        insert_masterchain_commit(&storage.partitions.lock(), &next, 0);
        storage
            .tail
            .remove_generation_commit(fixture.prepared.target_generation)
            .unwrap();

        let error = storage
            .commit_masterchain_block_set_with_predecessor(
                &next,
                old_snapshot.visible_frontier(),
            )
            .unwrap_err();
        assert_eq!(
            classify_authoritative_error(&error),
            Some(AuthoritativeErrorKind::MissingCommittedData),
        );
        assert!(storage.is_resync_required());
        assert!(storage.load_snapshot().is_none());
        assert!(storage
            .get_known_mc_blocks_range(Some(&old_snapshot))
            .is_err());
        assert_eq!(storage.gc.resync_transitions(), 1);
    }

    #[tokio::test]
    async fn committed_tail_sealing_rebuild_marks_resync_for_a_missing_commit() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = RpcStorage::open(
            context,
            gc_test_config(128, 16 * 1024 * 1024),
        )
        .unwrap();
        let fixture = prepare_gc_cutover_fixture(&storage).await;
        let target_generation = fixture.prepared.target_generation;
        storage.cutover_prepared_gc(fixture.prepared).await.unwrap();
        drop(fixture.pre_cutover);
        let next = masterchain_block(storage.load_snapshot().unwrap().visible_frontier().seqno + 1);
        let account = gc_account(0x95);
        let sealing = insert_read_test_block(
            &storage,
            &account,
            &next,
            &basechain_block(next.seqno),
            &[gc_transaction(30, 0x96)],
            1,
        );
        storage.commit_masterchain_block_set(&next).unwrap();
        let old_snapshot = storage.load_snapshot().unwrap();
        assert_eq!(
            old_snapshot.descriptor(sealing).unwrap().lifecycle,
            codec::ManifestLifecycle::Sealing,
        );
        drop(old_snapshot);
        storage
            .tail
            .remove_generation_commit(target_generation)
            .unwrap();

        let error = seal_partition(
            storage.partitions.clone(),
            storage.tail.clone(),
            storage.snapshots.clone(),
            storage.filter_registry.clone(),
            storage.maintenance.clone(),
            sealing,
            CancellationFlag::new(),
            None,
        )
        .await
        .unwrap_err();
        assert_eq!(
            classify_authoritative_error(&error),
            Some(AuthoritativeErrorKind::MissingCommittedData),
        );
        assert!(storage.is_resync_required());
        assert!(storage.load_snapshot().is_none());
        assert_eq!(storage.gc.resync_transitions(), 1);
    }

    #[tokio::test]
    async fn committed_tail_iterator_marks_resync_for_a_missing_payload() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = RpcStorage::open(
            context,
            gc_test_config(128, 16 * 1024 * 1024),
        )
        .unwrap();
        let fixture = prepare_gc_cutover_fixture(&storage).await;
        storage.cutover_prepared_gc(fixture.prepared).await.unwrap();
        let old_snapshot = storage.load_snapshot().unwrap();
        let payload_key = codec::tail_payload_key(gc_account_key(&gc_account(0x91)), 10);
        storage.tail.remove_transaction_payload(payload_key).unwrap();
        storage.publish_snapshot(old_snapshot.visible_frontier()).unwrap();
        let mut transactions = storage
            .get_transactions(&gc_account(0x91), None, None, false, None)
            .unwrap()
            .map_ext(|lt, _, _| Some(lt));

        assert_eq!(transactions.next(), None);
        assert!(storage.is_resync_required());
        assert!(storage.load_snapshot().is_none());
        assert!(storage
            .get_transactions(
                &gc_account(0x91),
                None,
                None,
                false,
                Some(old_snapshot),
            )
            .is_err());
        assert_eq!(storage.gc.resync_transitions(), 1);
        assert_eq!(transactions.next(), None);
        assert_eq!(storage.gc.resync_transitions(), 1);
    }

    #[tokio::test]
    async fn account_iterator_created_before_resync_remains_in_flight() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = RpcStorage::open(
            context,
            gc_test_config(128, 16 * 1024 * 1024),
        )
        .unwrap();
        let fixture = prepare_gc_cutover_fixture(&storage).await;
        storage.cutover_prepared_gc(fixture.prepared).await.unwrap();
        let snapshot = storage.load_snapshot().unwrap();
        let mut transactions = storage
            .get_transactions(&gc_account(0x91), None, None, false, None)
            .unwrap()
            .map_ext(|lt, _, _| Some(lt));

        storage.transition_to_resync_required_for_test(
            AuthoritativeErrorKind::MissingCommittedData,
        );
        assert!(storage
            .get_transactions(
                &gc_account(0x91),
                None,
                None,
                false,
                Some(snapshot),
            )
            .is_err());
        assert_eq!(transactions.next(), Some(10));
        assert_eq!(transactions.next(), None);
        assert_eq!(storage.gc.resync_transitions(), 1);
    }

    #[tokio::test]
    async fn gc_cutover_defers_while_a_block_set_is_admitted() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = RpcStorage::open(context, gc_test_config(128, 16 * 1024 * 1024)).unwrap();
        let fixture = prepare_gc_cutover_fixture(&storage).await;
        let frontier = *fixture.pre_cutover.visible_frontier();
        *storage.block_set_admission.lock() = Some(BlockSetAdmission {
            frontier,
            block_set: frontier,
            mode: BlockSetMode::Same,
        });

        let error = storage.cutover_prepared_gc(fixture.prepared).await.unwrap_err();
        assert!(format!("{error:#}").contains("cutover is deferred by block-set admission"));
        assert_eq!(classify_authoritative_error(&error), None);
        assert!(!storage.is_resync_required());
        assert_eq!(storage.gc.resync_transitions(), 0);
        assert_eq!(storage.partitions.lock().tail_visible_generation(), 0);
        assert_eq!(
            storage.partitions.lock().gc_intent().unwrap().unwrap().phase,
            codec::GcIntentPhase::Prepared,
        );
        let published = storage.load_snapshot().unwrap();
        assert_eq!(published.tail_snapshot().visible_generation(), 0);
        assert_eq!(
            published.descriptor(fixture.source).unwrap().lifecycle,
            codec::ManifestLifecycle::Sealed,
        );
    }

    #[tokio::test]
    async fn gc_source_deletion_waits_for_snapshot_filter_and_cache_references() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = RpcStorage::open(
            context.clone(),
            gc_test_config(128, 16 * 1024 * 1024),
        )
        .unwrap();
        let (fixture, filter_generation_path) =
            prepare_gc_deletion_fixture(&storage, &context).await;
        let source = fixture.source;
        let transaction_hash = fixture.transaction_hash;
        let source_path = context
            .root_dir()
            .path()
            .join("rpc/transactions")
            .join(source.directory_name());
        let old_read = acquire_partition_read(fixture.pre_cutover.clone(), source).unwrap();
        let committed = storage.cutover_prepared_gc(fixture.prepared).await.unwrap();
        assert!(storage.tail.generation_progress(committed.target_generation).unwrap().is_some());
        assert!(storage.tail.generation_commit(committed.target_generation).unwrap().is_some());
        storage.filter_worker.start();
        let gc = storage.gc.clone();
        let deletion = tokio::spawn(async move { gc.delete_source(committed).await });

        tokio::time::timeout(Duration::from_secs(5), async {
            while filter_generation_path.exists() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!deletion.is_finished());
        assert!(source_path.is_dir());
        assert!(!storage
            .partitions
            .lock()
            .deletion_references_drained(source)
            .unwrap());

        drop(old_read);
        drop(fixture.pre_cutover);
        let removed = tokio::time::timeout(Duration::from_secs(5), deletion)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(removed, source);
        assert!(!source_path.exists());
        assert!(!filter_generation_path.exists());
        let manager = storage.partitions.lock();
        assert_eq!(manager.removed_through_partition_id(), source.0);
        assert_eq!(manager.gc_intent().unwrap(), None);
        assert!(!manager.descriptors().iter().any(|descriptor| descriptor.id == source));
        drop(manager);
        assert!(storage.tail.generation_progress(committed.target_generation).unwrap().is_none());
        assert!(storage.tail.generation_commit(committed.target_generation).unwrap().is_some());
        assert!(!storage.is_resync_required());
        assert_eq!(storage.gc.resync_transitions(), 0);
        let snapshot = storage.load_snapshot().unwrap();
        assert_eq!(
            storage
                .get_transaction(&transaction_hash, Some(&snapshot))
                .unwrap()
                .unwrap()
                .as_ref(),
            [0x92],
        );
    }

    #[tokio::test]
    async fn gc_source_deletion_failure_stages_resume_without_ambiguous_control() {
        for stage in [
            GcDeletionFailureStage::BeforeDeletingControl,
            GcDeletionFailureStage::AfterDeletingControl,
            GcDeletionFailureStage::BeforeDirectoryRemoval,
            GcDeletionFailureStage::AfterDirectoryRemoval,
        ] {
            let (context, _tmp) = StorageContext::new_temp().await.unwrap();
            let storage = RpcStorage::open(
                context.clone(),
                gc_test_config(128, 16 * 1024 * 1024),
            )
            .unwrap();
            let (fixture, filter_generation_path) =
                prepare_gc_deletion_fixture(&storage, &context).await;
            let source = fixture.source;
            let source_path = context
                .root_dir()
                .path()
                .join("rpc/transactions")
                .join(source.directory_name());
            let committed = storage.cutover_prepared_gc(fixture.prepared).await.unwrap();
            drop(fixture.pre_cutover);
            storage.filter_worker.start();
            storage.gc.fail_deletion_at(stage);

            let error = storage.gc.delete_source(committed).await.unwrap_err();
            assert!(format!("{error:#}").contains("injected RPC transaction GC source deletion failure"));
            assert_eq!(classify_authoritative_error(&error), None);
            assert!(!storage.is_resync_required());
            assert_eq!(storage.gc.resync_transitions(), 0);
            assert!(storage.tail.generation_progress(committed.target_generation).unwrap().is_some());
            assert!(storage.tail.generation_commit(committed.target_generation).unwrap().is_some());
            assert!(!filter_generation_path.exists());
            let control_deleting = stage != GcDeletionFailureStage::BeforeDeletingControl;
            let directory_removed = stage == GcDeletionFailureStage::AfterDirectoryRemoval;
            let manager = storage.partitions.lock();
            assert_eq!(
                manager.gc_intent().unwrap().unwrap().phase,
                if control_deleting {
                    codec::GcIntentPhase::Deleting
                } else {
                    codec::GcIntentPhase::CutoverCommitted
                },
            );
            assert_eq!(
                manager
                    .descriptors()
                    .into_iter()
                    .find(|descriptor| descriptor.id == source)
                    .unwrap()
                    .lifecycle,
                if control_deleting {
                    codec::ManifestLifecycle::Deleting
                } else {
                    codec::ManifestLifecycle::Retired
                },
            );
            assert_eq!(manager.removed_through_partition_id(), 0);
            drop(manager);
            assert_eq!(source_path.exists(), !directory_removed);

            assert_eq!(storage.gc.delete_source(committed).await.unwrap(), source);
            assert!(!source_path.exists());
            let manager = storage.partitions.lock();
            assert_eq!(manager.gc_intent().unwrap(), None);
            assert_eq!(manager.removed_through_partition_id(), source.0);
            assert!(!manager.descriptors().iter().any(|descriptor| descriptor.id == source));
            drop(manager);
            assert!(storage.tail.generation_progress(committed.target_generation).unwrap().is_none());
            assert!(storage.tail.generation_commit(committed.target_generation).unwrap().is_some());
        }
    }

    #[tokio::test]
    async fn gc_restart_cleans_progress_finalized_before_tail_cleanup() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let config = gc_test_config(128, 16 * 1024 * 1024);
        let storage = RpcStorage::open(context.clone(), config.clone()).unwrap();
        let (fixture, _) = prepare_gc_deletion_fixture(&storage, &context).await;
        let committed = storage.cutover_prepared_gc(fixture.prepared).await.unwrap();
        drop(fixture.pre_cutover);
        storage.filter_worker.start();
        storage
            .gc
            .fail_deletion_at(GcDeletionFailureStage::AfterControlFinalization);

        let error = storage.gc.delete_source(committed).await.unwrap_err();
        assert!(format!("{error:#}").contains("injected RPC transaction GC source deletion failure"));
        assert_eq!(storage.partitions.lock().gc_intent().unwrap(), None);
        assert!(storage.tail.generation_progress(committed.target_generation).unwrap().is_some());
        assert!(storage.tail.generation_commit(committed.target_generation).unwrap().is_some());
        drop(storage);
        tokio::task::yield_now().await;

        let storage = RpcStorage::open(context, config).unwrap();
        storage.sealing_cancel.cancel();
        storage.filter_worker.shutdown();
        assert!(storage.tail.generation_progress(committed.target_generation).unwrap().is_some());
        storage.gc.run_once_for_test().await.unwrap();
        assert!(storage.tail.generation_progress(committed.target_generation).unwrap().is_none());
        assert!(storage.tail.generation_commit(committed.target_generation).unwrap().is_some());
    }

    #[tokio::test]
    async fn gc_tail_worker_waits_without_polling_and_retries_after_generation_release() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let GcTailSweepFixture {
            storage,
            old_tail,
            retired_payload_key,
            committed,
        } = prepare_gc_tail_sweep_fixture(&context).await;
        let first_attempt = storage.tail.sweep_attempts() + 1;
        storage.tail.inject_next_sweep_write_failure();
        storage.filter_worker.start();
        storage.gc.start();
        tokio::time::timeout(
            Duration::from_secs(5),
            storage.tail.wait_for_sweep_attempts(first_attempt),
        )
        .await
        .unwrap();

        assert!(storage
            .tail
            .retirement_entry_exists(committed.target_generation, retired_payload_key)
            .unwrap());
        assert!(storage.tail.generation_progress(committed.target_generation).unwrap().is_none());
        assert!(storage.tail.generation_commit(committed.target_generation).unwrap().is_some());
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
        assert_eq!(storage.tail.sweep_attempts(), first_attempt);
        assert!(!storage.is_resync_required());

        drop(old_tail);
        tokio::time::timeout(
            Duration::from_secs(5),
            storage.tail.wait_for_sweep_writes(1),
        )
        .await
        .unwrap();
        assert!(!storage
            .tail
            .retirement_entry_exists(committed.target_generation, retired_payload_key)
            .unwrap());
        assert!(!storage.is_resync_required());
        assert_eq!(storage.gc.resync_transitions(), 0);
    }

    #[tokio::test]
    async fn gc_tail_worker_marks_resync_once_for_missing_committed_payload() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let GcTailSweepFixture {
            storage,
            old_tail,
            retired_payload_key,
            committed,
        } = prepare_gc_tail_sweep_fixture(&context).await;
        let old_snapshot = storage.load_snapshot().unwrap();
        let frontier = *old_snapshot.visible_frontier();
        drop(old_tail);
        storage
            .tail
            .remove_transaction_payload(retired_payload_key)
            .unwrap();
        storage.filter_worker.start();
        storage.gc.start();
        tokio::time::timeout(
            Duration::from_secs(5),
            storage.gc.wait_for_resync_required(),
        )
        .await
        .unwrap();

        assert!(storage.is_resync_required());
        assert!(storage.load_snapshot().is_none());
        assert!(storage.get_known_mc_blocks_range(None).is_err());
        assert!(storage
            .get_known_mc_blocks_range(Some(&old_snapshot))
            .is_err());
        assert!(storage.publish_snapshot(&frontier).is_err());
        assert_eq!(storage.gc.resync_transitions(), 1);
        assert!(storage
            .tail
            .retirement_entry_exists(committed.target_generation, retired_payload_key)
            .unwrap());
        let attempts = storage.tail.sweep_attempts();
        storage.gc_notify.notify_one();
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
        assert_eq!(storage.tail.sweep_attempts(), attempts);
        storage.transition_to_resync_required_for_test(
            AuthoritativeErrorKind::ConflictingCommittedData,
        );
        assert!(storage.load_snapshot().is_none());
        assert_eq!(storage.gc.resync_transitions(), 1);
    }

    #[tokio::test]
    async fn gc_tail_worker_marks_resync_once_for_missing_committed_locator() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let GcTailSweepFixture {
            storage,
            old_tail,
            retired_payload_key,
            committed,
        } = prepare_gc_tail_sweep_fixture(&context).await;
        let old_snapshot = storage.load_snapshot().unwrap();
        drop(old_tail);
        storage
            .tail
            .remove_transaction_hash_locator(retired_payload_key)
            .unwrap();
        storage.filter_worker.start();
        storage.gc.start();
        tokio::time::timeout(
            Duration::from_secs(5),
            storage.gc.wait_for_resync_required(),
        )
        .await
        .unwrap();

        assert!(storage.is_resync_required());
        assert!(storage.load_snapshot().is_none());
        assert!(storage.get_known_mc_blocks_range(None).is_err());
        assert!(storage
            .get_known_mc_blocks_range(Some(&old_snapshot))
            .is_err());
        assert_eq!(storage.gc.resync_transitions(), 1);
        assert!(storage
            .tail
            .retirement_entry_exists(committed.target_generation, retired_payload_key)
            .unwrap());
        let attempts = storage.tail.sweep_attempts();
        storage.gc_notify.notify_one();
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
        assert_eq!(storage.tail.sweep_attempts(), attempts);
    }

    #[tokio::test]
    async fn startup_gc_marks_resync_for_a_missing_prepared_commit() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = Arc::new(RpcStorage::open_full(
            context,
            gc_test_config(128, 16 * 1024 * 1024),
            None,
        )
        .unwrap());
        let fixture = prepare_gc_cutover_fixture(&storage).await;
        let frontier = *fixture.pre_cutover.visible_frontier();
        storage
            .tail
            .remove_generation_commit(fixture.prepared.target_generation)
            .unwrap();
        storage.start_maintenance(&frontier).unwrap();
        tokio::time::timeout(
            Duration::from_secs(5),
            storage.gc.wait_for_resync_required(),
        )
        .await
        .unwrap();

        assert!(storage.is_resync_required());
        assert!(storage.load_snapshot().is_none());
        assert!(storage
            .get_known_mc_blocks_range(Some(&fixture.pre_cutover))
            .is_err());
        assert_eq!(storage.gc.resync_transitions(), 1);
        assert_eq!(
            storage.partitions.lock().gc_intent().unwrap().unwrap().phase,
            codec::GcIntentPhase::Prepared,
        );
    }

    #[tokio::test]
    async fn startup_gc_marks_resync_for_malformed_prepared_progress() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = Arc::new(RpcStorage::open_full(
            context,
            gc_test_config(128, 16 * 1024 * 1024),
            None,
        )
        .unwrap());
        let fixture = prepare_gc_cutover_fixture(&storage).await;
        let frontier = *fixture.pre_cutover.visible_frontier();
        storage
            .tail
            .replace_generation_progress(fixture.prepared.target_generation, &[0])
            .unwrap();
        storage.start_maintenance(&frontier).unwrap();
        tokio::time::timeout(
            Duration::from_secs(5),
            storage.gc.wait_for_resync_required(),
        )
        .await
        .unwrap();

        assert!(storage.is_resync_required());
        assert!(storage.load_snapshot().is_none());
        assert_eq!(storage.gc.resync_transitions(), 1);
        assert_eq!(
            storage.partitions.lock().gc_intent().unwrap().unwrap().phase,
            codec::GcIntentPhase::Prepared,
        );
    }

    #[test]
    fn resync_transition_emits_one_bounded_event() {
        let snapshots = SnapshotPublisher::default();
        let recorder = TestMetricsRecorder::default();
        let error = anyhow::anyhow!("injected committed RPC transaction storage failure");
        metrics::with_local_recorder(&recorder, || {
            assert!(snapshots.transition_to_resync_required(
                AuthoritativeErrorKind::MissingCommittedData,
                &error,
            ));
            assert!(!snapshots.transition_to_resync_required(
                AuthoritativeErrorKind::ConflictingCommittedData,
                &error,
            ));
        });

        assert_eq!(
            recorder.counter(
                "tycho_storage_rpc_resync_required_total|reason=missing_committed_data",
            ),
            1,
        );
        assert_eq!(
            recorder.counter(
                "tycho_storage_rpc_resync_required_total|reason=conflicting_committed_data",
            ),
            0,
        );
        assert_eq!(
            recorder
                .keys()
                .into_iter()
                .filter(|key| key.starts_with("tycho_storage_rpc_resync_required_total"))
                .collect::<Vec<_>>(),
            vec![
                "tycho_storage_rpc_resync_required_total|reason=missing_committed_data"
                    .to_owned(),
            ],
        );
        assert!(snapshots.lock_for_publication().is_err());
    }

    #[tokio::test]
    async fn resync_transition_withdraws_an_in_flight_publication() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = RpcStorage::open(context, RpcTransactionPartitionsConfig::default()).unwrap();
        let frontier = masterchain_block(0);
        publish_test_frontier(&storage, &frontier);
        let snapshot = storage.load_snapshot().unwrap();
        let snapshots = storage.snapshots.clone();
        let mut publication = snapshots.lock_for_publication().unwrap();
        let transition_snapshots = snapshots.clone();
        let transition = std::thread::spawn(move || {
            let error = anyhow::anyhow!("injected committed RPC transaction storage failure");
            transition_snapshots.transition_to_resync_required(
                AuthoritativeErrorKind::MissingCommittedData,
                &error,
            )
        });
        *publication = Some(snapshot);
        drop(publication);

        assert!(transition.join().unwrap());
        assert!(snapshots.load().is_none());
        assert!(snapshots.lock_for_publication().is_err());
    }

    #[tokio::test]
    async fn startup_maintenance_is_exact_once_and_budgeted_when_gc_disabled() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let mut config = gc_test_config(128, 16 * 1024 * 1024);
        config.maintenance.max_concurrent_tasks = 1;
        let storage = Arc::new(RpcStorage::open_full(context, config, None).unwrap());
        let startup_frontier = masterchain_block(0);
        publish_test_frontier(&storage, &startup_frontier);
        let blocker = storage
            .maintenance
            .acquire(MaintenancePriority::Background)
            .await
            .unwrap();
        storage.start_maintenance(&startup_frontier).unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while storage.maintenance.background_waiters() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(storage.gc.startup_passes_completed(), 0);
        assert_eq!(storage.partitions.lock().gc_intent().unwrap(), None);
        drop(blocker);
        tokio::time::timeout(
            Duration::from_secs(5),
            storage.gc.wait_for_startup_passes_completed(1),
        )
        .await
        .unwrap();
        assert_eq!(storage.gc.startup_passes_completed(), 1);
        storage.gc_notify.notify_one();
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
        assert_eq!(storage.gc.startup_passes_completed(), 1);
        assert_eq!(storage.partitions.lock().gc_intent().unwrap(), None);
    }

    #[tokio::test]
    async fn startup_maintenance_waiter_does_not_keep_rpc_storage_alive() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let mut config = gc_test_config(128, 16 * 1024 * 1024);
        config.maintenance.max_concurrent_tasks = 1;
        let storage = Arc::new(RpcStorage::open_full(context, config, None).unwrap());
        let startup_frontier = masterchain_block(0);
        publish_test_frontier(&storage, &startup_frontier);
        let blocker = storage
            .maintenance
            .acquire(MaintenancePriority::Background)
            .await
            .unwrap();
        storage.start_maintenance(&startup_frontier).unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while storage.maintenance.background_waiters() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let weak = Arc::downgrade(&storage);
        drop(storage);
        assert!(weak.upgrade().is_none());
        drop(blocker);
    }

    #[tokio::test]
    async fn startup_reopens_a_sealed_source_before_gc_intent_creation() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let config = gc_test_config(128, 16 * 1024 * 1024);
        let storage = RpcStorage::open_full(context.clone(), config.clone(), None).unwrap();
        storage.sealing_cancel.cancel();
        let account = gc_account(0x77);
        let transaction = gc_transaction(10, 0x78);
        let transaction_hash = transaction.hash;
        let candidate_mc = masterchain_block(1);
        let source = insert_read_test_block(
            &storage,
            &account,
            &candidate_mc,
            &basechain_block(1),
            &[transaction],
            1,
        );
        storage.commit_masterchain_block_set(&candidate_mc).unwrap();
        seal_gc_partition(&storage, source).await;
        let frontier = masterchain_block(100);
        publish_test_frontier(&storage, &frontier);
        assert_eq!(storage.partitions.lock().gc_intent().unwrap(), None);
        drop(storage);
        tokio::task::yield_now().await;

        let storage = RpcStorage::open_full(context, config, None).unwrap();
        let reconciliation = storage.reconcile_startup(&frontier).unwrap();
        storage.continue_lifecycle().unwrap();
        storage.publish_snapshot(&reconciliation.effective_frontier).unwrap();
        let published = storage.load_snapshot().unwrap();
        assert_eq!(published.tail_snapshot().visible_generation(), 0);
        assert!(published.descriptor(source).is_some());
        assert_eq!(storage.partitions.lock().gc_intent().unwrap(), None);
        assert_eq!(
            storage
                .get_transaction(&transaction_hash, Some(&published))
                .unwrap()
                .unwrap()
                .as_ref(),
            [0x78],
        );
    }

    #[tokio::test]
    async fn startup_maintenance_resumes_evacuating_without_progress_with_gc_disabled() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let config = gc_test_config(128, 16 * 1024 * 1024);
        let storage = RpcStorage::open_full(context.clone(), config.clone(), None).unwrap();
        storage.sealing_cancel.cancel();
        let account = gc_account(0x79);
        let transaction = gc_transaction(10, 0x7a);
        let transaction_hash = transaction.hash;
        let candidate_mc = masterchain_block(1);
        let source = insert_read_test_block(
            &storage,
            &account,
            &candidate_mc,
            &basechain_block(1),
            &[transaction],
            1,
        );
        storage.commit_masterchain_block_set(&candidate_mc).unwrap();
        seal_gc_partition(&storage, source).await;
        let frontier = masterchain_block(100);
        publish_test_frontier(&storage, &frontier);
        let intent = storage
            .begin_or_resume_gc(Some(&TransactionsGcConfig {
                tx_ttl: Duration::from_secs(10),
                keep_tx_per_account: 1,
            }))
            .unwrap()
            .unwrap();
        assert!(storage
            .tail
            .generation_progress(intent.target_generation)
            .unwrap()
            .is_none());
        assert!(storage
            .tail
            .generation_commit(intent.target_generation)
            .unwrap()
            .is_none());
        drop(storage);
        tokio::task::yield_now().await;

        let storage = Arc::new(RpcStorage::open_full(context, config, None).unwrap());
        let reconciliation = storage.reconcile_startup(&frontier).unwrap();
        storage.continue_lifecycle().unwrap();
        storage.publish_snapshot(&reconciliation.effective_frontier).unwrap();
        let initial = storage.load_snapshot().unwrap();
        assert_eq!(initial.tail_snapshot().visible_generation(), intent.previous_visible_generation);
        assert!(initial.descriptor(source).is_some());
        assert_eq!(
            storage
                .get_transaction(&transaction_hash, Some(&initial))
                .unwrap()
                .unwrap()
                .as_ref(),
            [0x7a],
        );
        drop(initial);
        storage
            .start_maintenance(&reconciliation.effective_frontier)
            .unwrap();
        tokio::time::timeout(
            Duration::from_secs(10),
            storage.gc.wait_for_startup_passes_completed(1),
        )
        .await
        .unwrap();

        assert_eq!(storage.partitions.lock().gc_intent().unwrap(), None);
        assert_eq!(storage.partitions.lock().removed_through_partition_id(), source.0);
        assert!(storage.tail.generation_progress(intent.target_generation).unwrap().is_none());
        assert!(storage.tail.generation_commit(intent.target_generation).unwrap().is_some());
        let published = storage.load_snapshot().unwrap();
        assert_eq!(published.tail_snapshot().visible_generation(), intent.target_generation);
        assert!(published.descriptor(source).is_none());
        assert_eq!(
            storage
                .get_transaction(&transaction_hash, Some(&published))
                .unwrap()
                .unwrap()
                .as_ref(),
            [0x7a],
        );
    }

    #[tokio::test]
    async fn startup_maintenance_retries_an_uncommitted_tail_chunk_after_reopen() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let config = gc_test_config(128, 16 * 1024 * 1024);
        let storage = RpcStorage::open_full(context.clone(), config.clone(), None).unwrap();
        storage.sealing_cancel.cancel();
        let account = gc_account(0x7d);
        let transaction = gc_transaction(10, 0x7e);
        let transaction_hash = transaction.hash;
        let candidate_mc = masterchain_block(1);
        let source = insert_read_test_block(
            &storage,
            &account,
            &candidate_mc,
            &basechain_block(1),
            &[transaction],
            1,
        );
        storage.commit_masterchain_block_set(&candidate_mc).unwrap();
        seal_gc_partition(&storage, source).await;
        let frontier = masterchain_block(100);
        publish_test_frontier(&storage, &frontier);
        let intent = storage
            .begin_or_resume_gc(Some(&TransactionsGcConfig {
                tx_ttl: Duration::from_secs(10),
                keep_tx_per_account: 1,
            }))
            .unwrap()
            .unwrap();
        storage.tail.inject_next_append_write_failure();
        let error = match storage.evacuate_gc_chunk(intent) {
            Ok(_) => panic!("tail chunk write failure was not injected"),
            Err(error) => error,
        };
        assert!(format!("{error:#}").contains("injected RPC tail chunk write failure"));
        assert_eq!(classify_authoritative_error(&error), None);
        assert!(storage
            .tail
            .generation_progress(intent.target_generation)
            .unwrap()
            .is_none());
        assert!(storage
            .tail
            .generation_commit(intent.target_generation)
            .unwrap()
            .is_none());
        drop(storage);
        tokio::task::yield_now().await;

        let storage = Arc::new(RpcStorage::open_full(context, config, None).unwrap());
        let reconciliation = storage.reconcile_startup(&frontier).unwrap();
        storage.continue_lifecycle().unwrap();
        storage.publish_snapshot(&reconciliation.effective_frontier).unwrap();
        storage
            .start_maintenance(&reconciliation.effective_frontier)
            .unwrap();
        tokio::time::timeout(
            Duration::from_secs(10),
            storage.gc.wait_for_startup_passes_completed(1),
        )
        .await
        .unwrap();

        assert_eq!(storage.partitions.lock().gc_intent().unwrap(), None);
        assert_eq!(storage.partitions.lock().removed_through_partition_id(), source.0);
        assert!(storage.tail.generation_progress(intent.target_generation).unwrap().is_none());
        assert!(storage.tail.generation_commit(intent.target_generation).unwrap().is_some());
        let published = storage.load_snapshot().unwrap();
        assert_eq!(published.tail_snapshot().visible_generation(), intent.target_generation);
        assert!(published.descriptor(source).is_none());
        assert_eq!(
            storage
                .get_transaction(&transaction_hash, Some(&published))
                .unwrap()
                .unwrap()
                .as_ref(),
            [0x7e],
        );
    }

    #[tokio::test]
    async fn startup_maintenance_resumes_terminal_commit_before_prepared() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let config = gc_test_config(128, 16 * 1024 * 1024);
        let storage = RpcStorage::open_full(context.clone(), config.clone(), None).unwrap();
        storage.sealing_cancel.cancel();
        let account = gc_account(0x7b);
        let transaction = gc_transaction(10, 0x7c);
        let transaction_hash = transaction.hash;
        let candidate_mc = masterchain_block(1);
        let source = insert_read_test_block(
            &storage,
            &account,
            &candidate_mc,
            &basechain_block(1),
            &[transaction],
            1,
        );
        storage.commit_masterchain_block_set(&candidate_mc).unwrap();
        seal_gc_partition(&storage, source).await;
        let frontier = masterchain_block(100);
        publish_test_frontier(&storage, &frontier);
        let intent = storage
            .begin_or_resume_gc(Some(&TransactionsGcConfig {
                tx_ttl: Duration::from_secs(10),
                keep_tx_per_account: 1,
            }))
            .unwrap()
            .unwrap();
        assert!(matches!(
            storage.evacuate_gc_chunk(intent).unwrap(),
            GcEvacuationChunkResult::Appended,
        ));
        *storage.gc_evacuation_failure.lock() =
            Some(GcEvacuationFailureStage::AfterTerminalCommit);
        let error = match storage.evacuate_gc_chunk(intent) {
            Ok(_) => panic!("terminal GC evacuation failure was not injected"),
            Err(error) => error,
        };
        assert!(format!("{error:#}").contains(
            "injected RPC transaction GC evacuation failure at AfterTerminalCommit"
        ));
        let progress = storage
            .tail
            .generation_progress(intent.target_generation)
            .unwrap()
            .unwrap();
        assert!(progress.eof);
        assert!(storage
            .tail
            .generation_commit(intent.target_generation)
            .unwrap()
            .is_some());
        assert_eq!(
            storage.partitions.lock().gc_intent().unwrap().unwrap().phase,
            codec::GcIntentPhase::Evacuating,
        );
        drop(storage);
        tokio::task::yield_now().await;

        let storage = Arc::new(RpcStorage::open_full(context, config, None).unwrap());
        let reconciliation = storage.reconcile_startup(&frontier).unwrap();
        storage.continue_lifecycle().unwrap();
        storage.publish_snapshot(&reconciliation.effective_frontier).unwrap();
        let initial = storage.load_snapshot().unwrap();
        assert_eq!(initial.tail_snapshot().visible_generation(), intent.previous_visible_generation);
        assert!(initial.descriptor(source).is_some());
        assert_eq!(
            storage
                .get_transaction(&transaction_hash, Some(&initial))
                .unwrap()
                .unwrap()
                .as_ref(),
            [0x7c],
        );
        drop(initial);
        storage
            .start_maintenance(&reconciliation.effective_frontier)
            .unwrap();
        tokio::time::timeout(
            Duration::from_secs(10),
            storage.gc.wait_for_startup_passes_completed(1),
        )
        .await
        .unwrap();

        assert_eq!(storage.partitions.lock().gc_intent().unwrap(), None);
        assert_eq!(storage.partitions.lock().removed_through_partition_id(), source.0);
        assert!(storage.tail.generation_progress(intent.target_generation).unwrap().is_none());
        assert!(storage.tail.generation_commit(intent.target_generation).unwrap().is_some());
        let published = storage.load_snapshot().unwrap();
        assert_eq!(published.tail_snapshot().visible_generation(), intent.target_generation);
        assert!(published.descriptor(source).is_none());
        assert_eq!(
            storage
                .get_transaction(&transaction_hash, Some(&published))
                .unwrap()
                .unwrap()
                .as_ref(),
            [0x7c],
        );
    }

    #[tokio::test]
    async fn startup_maintenance_resumes_partial_evacuating_with_gc_disabled() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let config = gc_test_config(1, 16 * 1024 * 1024);
        let storage = RpcStorage::open_full(context.clone(), config.clone(), None).unwrap();
        storage.sealing_cancel.cancel();
        let first_account = gc_account(0x81);
        let second_account = gc_account(0x82);
        let first_transaction = gc_transaction(10, 0x83);
        let first_transaction_hash = first_transaction.hash;
        let second_transaction = gc_transaction(20, 0x84);
        let second_transaction_hash = second_transaction.hash;
        let candidate_mc = masterchain_block(1);
        let source = insert_read_test_block(
            &storage,
            &first_account,
            &candidate_mc,
            &basechain_block(1),
            &[first_transaction],
            0,
        );
        assert_eq!(
            insert_read_test_block(
                &storage,
                &second_account,
                &candidate_mc,
                &basechain_block(1),
                &[second_transaction],
                1,
            ),
            source,
        );
        storage.commit_masterchain_block_set(&candidate_mc).unwrap();
        seal_gc_partition(&storage, source).await;
        let frontier = masterchain_block(100);
        publish_test_frontier(&storage, &frontier);
        let intent = storage
            .begin_or_resume_gc(Some(&TransactionsGcConfig {
                tx_ttl: Duration::from_secs(10),
                keep_tx_per_account: 1,
            }))
            .unwrap()
            .unwrap();
        assert!(matches!(
            storage.evacuate_gc_chunk(intent).unwrap(),
            GcEvacuationChunkResult::Appended,
        ));
        assert!(!storage
            .tail
            .generation_progress(intent.target_generation)
            .unwrap()
            .unwrap()
            .eof);
        drop(storage);
        tokio::task::yield_now().await;

        let storage = Arc::new(RpcStorage::open_full(context, config, None).unwrap());
        let reconciliation = storage.reconcile_startup(&frontier).unwrap();
        storage.continue_lifecycle().unwrap();
        storage.publish_snapshot(&reconciliation.effective_frontier).unwrap();
        let initial = storage.load_snapshot().unwrap();
        assert_eq!(initial.tail_snapshot().visible_generation(), intent.previous_visible_generation);
        assert!(initial.descriptor(source).is_some());
        for (hash, byte) in [(first_transaction_hash, 0x83), (second_transaction_hash, 0x84)] {
            assert_eq!(
                storage
                    .get_transaction(&hash, Some(&initial))
                    .unwrap()
                    .unwrap()
                    .as_ref(),
                [byte],
            );
        }
        drop(initial);
        storage
            .start_maintenance(&reconciliation.effective_frontier)
            .unwrap();
        tokio::time::timeout(
            Duration::from_secs(10),
            storage.gc.wait_for_startup_passes_completed(1),
        )
        .await
        .unwrap();

        assert_eq!(storage.partitions.lock().gc_intent().unwrap(), None);
        assert_eq!(storage.partitions.lock().removed_through_partition_id(), source.0);
        assert!(storage.tail.generation_progress(intent.target_generation).unwrap().is_none());
        assert!(storage.tail.generation_commit(intent.target_generation).unwrap().is_some());
        let published = storage.load_snapshot().unwrap();
        assert_eq!(published.tail_snapshot().visible_generation(), intent.target_generation);
        assert!(published.descriptor(source).is_none());
        for (hash, byte) in [(first_transaction_hash, 0x83), (second_transaction_hash, 0x84)] {
            assert_eq!(
                storage
                    .get_transaction(&hash, Some(&published))
                    .unwrap()
                    .unwrap()
                    .as_ref(),
                [byte],
            );
        }
    }

    #[tokio::test]
    async fn startup_maintenance_resumes_prepared_with_gc_disabled() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let config = gc_test_config(128, 16 * 1024 * 1024);
        let storage = Arc::new(RpcStorage::open_full(
            context.clone(),
            config.clone(),
            None,
        )
        .unwrap());
        let (fixture, filter_generation_path) =
            prepare_gc_deletion_fixture(&storage, &context).await;
        let source = fixture.source;
        let target_generation = fixture.prepared.target_generation;
        let transaction_hash = fixture.transaction_hash;
        let frontier = *fixture.pre_cutover.visible_frontier();
        let source_path = context
            .root_dir()
            .path()
            .join("rpc/transactions")
            .join(source.directory_name());
        drop(fixture.pre_cutover);
        drop(storage);
        tokio::task::yield_now().await;
        let storage = Arc::new(RpcStorage::open_full(context, config, None).unwrap());
        let reconciliation = storage.reconcile_startup(&frontier).unwrap();
        storage.continue_lifecycle().unwrap();
        storage.publish_snapshot(&reconciliation.effective_frontier).unwrap();
        let initial = storage.load_snapshot().unwrap();
        assert_eq!(
            initial.tail_snapshot().visible_generation(),
            fixture.prepared.previous_visible_generation,
        );
        assert!(initial.descriptor(source).is_some());
        assert_eq!(
            storage
                .get_transaction(&transaction_hash, Some(&initial))
                .unwrap()
                .unwrap()
                .as_ref(),
            [0x92],
        );
        drop(initial);
        storage
            .start_maintenance(&reconciliation.effective_frontier)
            .unwrap();
        tokio::time::timeout(
            Duration::from_secs(5),
            storage.gc.wait_for_startup_passes_completed(1),
        )
        .await
        .unwrap();

        assert_eq!(storage.gc.startup_passes_completed(), 1);
        assert_eq!(storage.partitions.lock().gc_intent().unwrap(), None);
        assert_eq!(storage.partitions.lock().removed_through_partition_id(), source.0);
        assert!(!source_path.exists());
        assert!(!filter_generation_path.exists());
        assert!(storage.tail.generation_progress(target_generation).unwrap().is_none());
        assert!(storage.tail.generation_commit(target_generation).unwrap().is_some());
        let published = storage.load_snapshot().unwrap();
        assert_eq!(published.tail_snapshot().visible_generation(), target_generation);
        assert!(published.descriptor(source).is_none());
        assert_eq!(
            storage
                .get_transaction(&transaction_hash, Some(&published))
                .unwrap()
                .unwrap()
                .as_ref(),
            [0x92],
        );
    }

    #[tokio::test]
    async fn startup_maintenance_resumes_post_cutover_with_source_present_or_absent() {
        for failure_stage in [
            None,
            Some(GcDeletionFailureStage::BeforeDirectoryRemoval),
            Some(GcDeletionFailureStage::AfterDirectoryRemoval),
        ] {
            let (context, _tmp) = StorageContext::new_temp().await.unwrap();
            let config = gc_test_config(128, 16 * 1024 * 1024);
            let storage = RpcStorage::open_full(context.clone(), config.clone(), None).unwrap();
            let (fixture, filter_generation_path) =
                prepare_gc_deletion_fixture(&storage, &context).await;
            let source = fixture.source;
            let transaction_hash = fixture.transaction_hash;
            let frontier = *fixture.pre_cutover.visible_frontier();
            let source_path = context
                .root_dir()
                .path()
                .join("rpc/transactions")
                .join(source.directory_name());
            let committed = storage.cutover_prepared_gc(fixture.prepared).await.unwrap();
            drop(fixture.pre_cutover);
            if let Some(failure_stage) = failure_stage {
                storage.filter_worker.start();
                storage.gc.fail_deletion_at(failure_stage);
                storage.gc.delete_source(committed).await.unwrap_err();
                assert_eq!(
                    storage.partitions.lock().gc_intent().unwrap().unwrap().phase,
                    codec::GcIntentPhase::Deleting,
                );
                assert_eq!(
                    source_path.exists(),
                    failure_stage == GcDeletionFailureStage::BeforeDirectoryRemoval,
                );
            } else {
                assert_eq!(
                    storage.partitions.lock().gc_intent().unwrap().unwrap().phase,
                    codec::GcIntentPhase::CutoverCommitted,
                );
                assert!(source_path.exists());
            }
            drop(storage);
            tokio::task::yield_now().await;

            let storage = Arc::new(RpcStorage::open_full(context, config, None).unwrap());
            let reconciliation = storage.reconcile_startup(&frontier).unwrap();
            storage.continue_lifecycle().unwrap();
            storage.publish_snapshot(&reconciliation.effective_frontier).unwrap();
            let initial = storage.load_snapshot().unwrap();
            assert_eq!(initial.tail_snapshot().visible_generation(), committed.target_generation);
            assert!(initial.descriptor(source).is_none());
            assert_eq!(
                storage
                    .get_transaction(&transaction_hash, Some(&initial))
                    .unwrap()
                    .unwrap()
                    .as_ref(),
                [0x92],
            );
            drop(initial);
            storage
                .start_maintenance(&reconciliation.effective_frontier)
                .unwrap();
            tokio::time::timeout(
                Duration::from_secs(10),
                storage.gc.wait_for_startup_passes_completed(1),
            )
            .await
            .unwrap();

            assert_eq!(storage.partitions.lock().gc_intent().unwrap(), None);
            assert_eq!(storage.partitions.lock().removed_through_partition_id(), source.0);
            assert!(!source_path.exists());
            assert!(!filter_generation_path.exists());
            assert!(storage.tail.generation_progress(committed.target_generation).unwrap().is_none());
            assert!(storage.tail.generation_commit(committed.target_generation).unwrap().is_some());
            let published = storage.load_snapshot().unwrap();
            assert_eq!(published.tail_snapshot().visible_generation(), committed.target_generation);
            assert!(published.descriptor(source).is_none());
            assert_eq!(
                storage
                    .get_transaction(&transaction_hash, Some(&published))
                    .unwrap()
                    .unwrap()
                    .as_ref(),
                [0x92],
            );
        }
    }

    #[tokio::test]
    async fn startup_finishes_partial_source_removal_without_touching_sibling_paths() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let config = gc_test_config(128, 16 * 1024 * 1024);
        let storage = RpcStorage::open_full(context.clone(), config.clone(), None).unwrap();
        let (fixture, filter_generation_path) =
            prepare_gc_deletion_fixture(&storage, &context).await;
        let source = fixture.source;
        let transaction_hash = fixture.transaction_hash;
        let frontier = *fixture.pre_cutover.visible_frontier();
        let source_path = context
            .root_dir()
            .path()
            .join("rpc/transactions")
            .join(source.directory_name());
        let sibling_partition = storage.partitions.lock().active_id();
        assert_ne!(sibling_partition, source);
        let sibling_transaction_hash = HashBytes([0x94; 32]);
        let sibling_partition_path = context
            .root_dir()
            .path()
            .join("rpc/transactions")
            .join(sibling_partition.directory_name());
        let filter_sibling_path = filter_generation_path.with_file_name(format!(
            "{}-decoy",
            filter_generation_path.file_name().unwrap().to_string_lossy(),
        ));
        std::fs::create_dir(&filter_sibling_path).unwrap();
        let filter_sibling_sentinel = filter_sibling_path.join("sentinel");
        std::fs::write(&filter_sibling_sentinel, []).unwrap();

        let committed = storage.cutover_prepared_gc(fixture.prepared).await.unwrap();
        drop(fixture.pre_cutover);
        storage.filter_worker.start();
        storage
            .gc
            .fail_deletion_at(GcDeletionFailureStage::BeforeDirectoryRemoval);
        storage.gc.delete_source(committed).await.unwrap_err();
        assert_eq!(
            storage.partitions.lock().gc_intent().unwrap().unwrap().phase,
            codec::GcIntentPhase::Deleting,
        );
        assert!(!filter_generation_path.exists());
        assert!(filter_sibling_sentinel.exists());
        let partial_file = std::fs::read_dir(&source_path)
            .unwrap()
            .map(|entry| entry.unwrap())
            .find(|entry| entry.file_type().unwrap().is_file())
            .unwrap()
            .path();
        std::fs::remove_file(partial_file).unwrap();
        assert!(source_path.is_dir());
        assert!(sibling_partition_path.is_dir());
        drop(storage);
        tokio::task::yield_now().await;

        let storage = Arc::new(RpcStorage::open_full(context, config, None).unwrap());
        let reconciliation = storage.reconcile_startup(&frontier).unwrap();
        storage.continue_lifecycle().unwrap();
        storage.publish_snapshot(&reconciliation.effective_frontier).unwrap();
        let initial = storage.load_snapshot().unwrap();
        assert_eq!(initial.tail_snapshot().visible_generation(), committed.target_generation);
        assert!(initial.descriptor(source).is_none());
        assert_eq!(
            storage
                .get_transaction(&transaction_hash, Some(&initial))
                .unwrap()
                .unwrap()
                .as_ref(),
            [0x92],
        );
        drop(initial);
        storage
            .start_maintenance(&reconciliation.effective_frontier)
            .unwrap();
        tokio::time::timeout(
            Duration::from_secs(10),
            storage.gc.wait_for_startup_passes_completed(1),
        )
        .await
        .unwrap();

        assert_eq!(storage.partitions.lock().gc_intent().unwrap(), None);
        assert_eq!(storage.partitions.lock().removed_through_partition_id(), source.0);
        assert!(!source_path.exists());
        assert!(!filter_generation_path.exists());
        assert!(sibling_partition_path.is_dir());
        assert!(filter_sibling_sentinel.exists());
        let published = storage.load_snapshot().unwrap();
        assert_eq!(
            storage
                .get_transaction(&sibling_transaction_hash, Some(&published))
                .unwrap()
                .unwrap()
                .as_ref(),
            [0x94],
        );
    }

    #[tokio::test]
    async fn startup_maintenance_drains_only_the_fixed_frontier_prefix_once() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let gc_config = TransactionsGcConfig {
            tx_ttl: Duration::from_secs(10),
            keep_tx_per_account: 1,
        };
        let storage = Arc::new(RpcStorage::open_full(
            context,
            gc_test_config(128, 16 * 1024 * 1024),
            Some(gc_config),
        )
        .unwrap());
        storage.sealing_cancel.cancel();
        let first_mc = masterchain_block(10);
        let first = insert_read_test_block(
            &storage,
            &gc_account(0x71),
            &first_mc,
            &basechain_block(10),
            &[gc_transaction(10, 0x72)],
            1,
        );
        storage.commit_masterchain_block_set(&first_mc).unwrap();
        seal_gc_partition(&storage, first).await;
        let second_mc = masterchain_block(20);
        let second = insert_read_test_block(
            &storage,
            &gc_account(0x73),
            &second_mc,
            &basechain_block(20),
            &[gc_transaction(20, 0x74)],
            1,
        );
        storage.commit_masterchain_block_set(&second_mc).unwrap();
        seal_gc_partition(&storage, second).await;
        let startup_frontier = masterchain_block(100);
        publish_test_frontier(&storage, &startup_frontier);
        storage.start_maintenance(&startup_frontier).unwrap();
        tokio::time::timeout(
            Duration::from_secs(10),
            storage.gc.wait_for_startup_passes_completed(1),
        )
        .await
        .unwrap();

        assert_eq!(storage.partitions.lock().removed_through_partition_id(), second.0);
        assert_eq!(storage.partitions.lock().gc_intent().unwrap(), None);
        assert_eq!(storage.gc.startup_passes_completed(), 1);
        let later_mc = masterchain_block(101);
        let later = insert_read_test_block(
            &storage,
            &gc_account(0x75),
            &later_mc,
            &basechain_block(101),
            &[gc_transaction(30, 0x76)],
            1,
        );
        storage.commit_masterchain_block_set(&later_mc).unwrap();
        seal_gc_partition(&storage, later).await;
        publish_test_frontier(&storage, &masterchain_block(200));
        storage.gc_notify.notify_one();
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
        assert_eq!(storage.gc.startup_passes_completed(), 1);
        assert_eq!(storage.partitions.lock().gc_intent().unwrap(), None);
        assert_eq!(
            storage
                .partitions
                .lock()
                .descriptors()
                .into_iter()
                .find(|descriptor| descriptor.id == later)
                .unwrap()
                .lifecycle,
            codec::ManifestLifecycle::Sealed,
        );
    }

    #[tokio::test]
    async fn startup_maintenance_keeps_the_reconciled_frontier_after_new_publication() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let gc_config = TransactionsGcConfig {
            tx_ttl: Duration::from_secs(10),
            keep_tx_per_account: 1,
        };
        let storage = Arc::new(RpcStorage::open_full(
            context,
            gc_test_config(128, 16 * 1024 * 1024),
            Some(gc_config),
        )
        .unwrap());
        storage.sealing_cancel.cancel();
        let source_mc = masterchain_block(20);
        let source = insert_read_test_block(
            &storage,
            &gc_account(0x77),
            &source_mc,
            &basechain_block(20),
            &[gc_transaction(20, 0x78)],
            1,
        );
        storage.commit_masterchain_block_set(&source_mc).unwrap();
        seal_gc_partition(&storage, source).await;
        let reconciled_frontier = masterchain_block(25);
        publish_test_frontier(&storage, &reconciled_frontier);
        let foreground_frontier = masterchain_block(100);
        publish_test_frontier(&storage, &foreground_frontier);
        assert_eq!(
            storage.load_snapshot().unwrap().visible_frontier(),
            &foreground_frontier,
        );

        storage.start_maintenance(&reconciled_frontier).unwrap();
        tokio::time::timeout(
            Duration::from_secs(5),
            storage.gc.wait_for_startup_passes_completed(1),
        )
        .await
        .unwrap();

        assert_eq!(storage.partitions.lock().removed_through_partition_id(), 0);
        assert_eq!(storage.partitions.lock().gc_intent().unwrap(), None);
        assert_eq!(
            storage
                .partitions
                .lock()
                .descriptors()
                .into_iter()
                .find(|descriptor| descriptor.id == source)
                .unwrap()
                .lifecycle,
            codec::ManifestLifecycle::Sealed,
        );
        assert_eq!(
            storage.load_snapshot().unwrap().visible_frontier(),
            &foreground_frontier,
        );
    }

    #[tokio::test]
    async fn startup_completion_follows_safe_tail_sweep() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let GcTailSweepFixture {
            storage,
            old_tail,
            retired_payload_key,
            committed,
        } = prepare_gc_tail_sweep_fixture(&context).await;
        drop(old_tail);
        let storage = Arc::new(storage);
        let startup_frontier = *storage.load_snapshot().unwrap().visible_frontier();
        storage.start_maintenance(&startup_frontier).unwrap();
        tokio::time::timeout(
            Duration::from_secs(5),
            storage.gc.wait_for_startup_passes_completed(1),
        )
        .await
        .unwrap();

        assert_eq!(storage.gc.startup_passes_completed(), 1);
        assert!(!storage
            .tail
            .retirement_entry_exists(committed.target_generation, retired_payload_key)
            .unwrap());
        assert!(storage.tail.generation_commit(committed.target_generation).unwrap().is_some());
    }

    #[tokio::test]
    async fn gc_source_directory_removal_retries_with_the_same_exact_guard() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = RpcStorage::open(
            context.clone(),
            gc_test_config(128, 16 * 1024 * 1024),
        )
        .unwrap();
        let (fixture, filter_generation_path) =
            prepare_gc_deletion_fixture(&storage, &context).await;
        let source = fixture.source;
        let source_path = context
            .root_dir()
            .path()
            .join("rpc/transactions")
            .join(source.directory_name());
        let committed = storage.cutover_prepared_gc(fixture.prepared).await.unwrap();
        drop(fixture.pre_cutover);
        storage.filter_worker.start();
        let attempts_before = storage.gc.remove_attempts();
        storage.gc.fail_next_directory_removals(1);

        assert_eq!(storage.gc.delete_source(committed).await.unwrap(), source);
        assert_eq!(storage.gc.remove_attempts() - attempts_before, 2);
        assert!(!source_path.exists());
        assert!(!filter_generation_path.exists());
        assert_eq!(storage.partitions.lock().removed_through_partition_id(), source.0);

        std::fs::create_dir_all(&source_path).unwrap();
        assert_eq!(
            storage
                .partitions
                .lock()
                .certified_removed_partition_directories()
                .unwrap(),
            vec![(source, source_path.clone())],
        );
        storage.gc.start();
        tokio::time::timeout(Duration::from_secs(5), async {
            while source_path.exists() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!storage.is_resync_required());
        assert_eq!(storage.gc.resync_transitions(), 0);
    }

    #[tokio::test]
    async fn concurrent_gc_source_deletion_replays_finalize_idempotently() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = RpcStorage::open(
            context.clone(),
            gc_test_config(128, 16 * 1024 * 1024),
        )
        .unwrap();
        let (fixture, filter_generation_path) =
            prepare_gc_deletion_fixture(&storage, &context).await;
        let source = fixture.source;
        let source_path = context
            .root_dir()
            .path()
            .join("rpc/transactions")
            .join(source.directory_name());
        let committed = storage.cutover_prepared_gc(fixture.prepared).await.unwrap();
        drop(fixture.pre_cutover);
        storage.filter_worker.start();
        storage
            .gc
            .synchronize_deletion_guard_acquisition(Arc::new(tokio::sync::Barrier::new(2)));

        let (first, second) = tokio::join!(
            storage.gc.delete_source(committed),
            storage.gc.delete_source(committed),
        );
        assert_eq!(first.unwrap(), source);
        assert_eq!(second.unwrap(), source);
        assert!(!source_path.exists());
        assert!(!filter_generation_path.exists());
        let manager = storage.partitions.lock();
        assert_eq!(manager.gc_intent().unwrap(), None);
        assert_eq!(manager.removed_through_partition_id(), source.0);
        assert!(!manager.descriptors().iter().any(|descriptor| descriptor.id == source));
    }

    #[tokio::test]
    async fn gc_source_deletion_post_error_recheck_observes_racing_finalization() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = RpcStorage::open(
            context.clone(),
            gc_test_config(128, 16 * 1024 * 1024),
        )
        .unwrap();
        let (fixture, filter_generation_path) =
            prepare_gc_deletion_fixture(&storage, &context).await;
        let source = fixture.source;
        let source_path = context
            .root_dir()
            .path()
            .join("rpc/transactions")
            .join(source.directory_name());
        let committed = storage.cutover_prepared_gc(fixture.prepared).await.unwrap();
        drop(fixture.pre_cutover);
        storage.filter_worker.start();
        storage.gc.pause_next_deletion_guard_after_precheck();
        let gc = storage.gc.clone();
        let paused = tokio::spawn(async move { gc.delete_source(committed).await });
        tokio::time::timeout(
            Duration::from_secs(5),
            storage.gc.wait_for_deletion_guard_precheck(),
        )
        .await
        .unwrap();

        assert_eq!(storage.gc.delete_source(committed).await.unwrap(), source);
        storage.gc.release_deletion_guard_after_precheck();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), paused)
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            source,
        );
        assert!(!source_path.exists());
        assert!(!filter_generation_path.exists());
        let manager = storage.partitions.lock();
        assert_eq!(manager.gc_intent().unwrap(), None);
        assert_eq!(manager.removed_through_partition_id(), source.0);
    }

    #[tokio::test]
    async fn gc_evacuation_retention_handles_zero_partial_exact_and_newer_history() {
        let cases = [
            (0usize, 0usize, Vec::new()),
            (5, 0, vec![30, 20, 10]),
            (2, 2, Vec::new()),
            (3, 1, vec![30, 20]),
        ];
        for (keep, newer_count, expected_tail_lts) in cases {
            let (context, _tmp) = StorageContext::new_temp().await.unwrap();
            let storage = RpcStorage::open(context, gc_test_config(128, 16 * 1024 * 1024)).unwrap();
            storage.sealing_cancel.cancel();
            storage.filter_worker.shutdown();
            let account = gc_account(0x11);
            let account_key = gc_account_key(&account);
            let candidate_mc = masterchain_block(1);
            let candidate = [
                gc_transaction(10, 0x10),
                gc_transaction(20, 0x20),
                gc_transaction(30, 0x30),
            ];
            let source = insert_read_test_block(
                &storage,
                &account,
                &candidate_mc,
                &basechain_block(1),
                &candidate,
                1,
            );
            storage.commit_masterchain_block_set(&candidate_mc).unwrap();
            seal_gc_partition(&storage, source).await;

            if newer_count > 0 {
                let newer_mc = masterchain_block(2);
                let newer = (0..newer_count)
                    .map(|index| {
                        gc_transaction(
                            40 + index as u64 * 10,
                            0x80u8.wrapping_add(index as u8),
                        )
                    })
                    .collect::<Vec<_>>();
                insert_read_test_block(
                    &storage,
                    &account,
                    &newer_mc,
                    &basechain_block(2),
                    &newer,
                    0,
                );
                storage.commit_masterchain_block_set(&newer_mc).unwrap();
            }
            let frontier = masterchain_block(100);
            publish_test_frontier(&storage, &frontier);

            let prepared = storage
                .evacuate_gc(Some(&TransactionsGcConfig {
                    tx_ttl: Duration::from_secs(10),
                    keep_tx_per_account: keep,
                }))
                .unwrap()
                .unwrap();
            assert_eq!(prepared.phase, codec::GcIntentPhase::Prepared);
            assert_eq!(prepared.keep_tx_per_account, keep as u64);
            assert_eq!(
                tail_account_lts(&storage, prepared.target_generation, account_key, frontier.seqno),
                expected_tail_lts,
            );
            let progress = storage
                .tail
                .generation_progress(prepared.target_generation)
                .unwrap()
                .unwrap();
            assert!(progress.eof);
            assert_eq!(progress.counters.processed_accounts, 1);
            assert_eq!(
                progress.counters.promoted_records,
                expected_tail_lts.len() as u64,
            );
            assert_eq!(progress.counters.retired_records, 0);
        }
    }

    #[tokio::test]
    async fn gc_evacuation_resumes_durable_cursor_and_keeps_original_policy() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let config = gc_test_config(1, 16 * 1024 * 1024);
        let storage = RpcStorage::open(context.clone(), config.clone()).unwrap();
        storage.sealing_cancel.cancel();
        storage.filter_worker.shutdown();
        let first_account = gc_account(0x21);
        let second_account = gc_account(0x22);
        let first_key = gc_account_key(&first_account);
        let second_key = gc_account_key(&second_account);
        let candidate_mc = masterchain_block(1);
        let source = insert_read_test_block(
            &storage,
            &first_account,
            &candidate_mc,
            &basechain_block(1),
            &[gc_transaction(10, 0x31)],
            0,
        );
        assert_eq!(
            insert_read_test_block(
                &storage,
                &second_account,
                &candidate_mc,
                &basechain_block(1),
                &[gc_transaction(20, 0x32)],
                1,
            ),
            source,
        );
        storage.commit_masterchain_block_set(&candidate_mc).unwrap();
        seal_gc_partition(&storage, source).await;
        let frontier = masterchain_block(100);
        publish_test_frontier(&storage, &frontier);
        let original_config = TransactionsGcConfig {
            tx_ttl: Duration::from_secs(10),
            keep_tx_per_account: 1,
        };
        let intent = storage
            .begin_or_resume_gc(Some(&original_config))
            .unwrap()
            .unwrap();
        assert!(matches!(
            storage.evacuate_gc_chunk(intent).unwrap(),
            GcEvacuationChunkResult::Appended,
        ));
        let first_progress = storage
            .tail
            .generation_progress(intent.target_generation)
            .unwrap()
            .unwrap();
        assert_eq!(first_progress.cursor, codec::TailProgressCursor::Account(first_key));
        assert_eq!(first_progress.counters.processed_accounts, 1);
        drop(storage);
        tokio::task::yield_now().await;

        let storage = RpcStorage::open(context, config).unwrap();
        storage.sealing_cancel.cancel();
        storage.filter_worker.shutdown();
        storage.publish_snapshot(&frontier).unwrap();
        let changed_config = TransactionsGcConfig {
            tx_ttl: Duration::from_secs(1),
            keep_tx_per_account: 0,
        };
        let resumed = storage
            .begin_or_resume_gc(Some(&changed_config))
            .unwrap()
            .unwrap();
        assert_eq!(resumed, intent);
        let prepared = storage.evacuate_gc(Some(&changed_config)).unwrap().unwrap();
        assert_eq!(prepared.phase, codec::GcIntentPhase::Prepared);
        assert_eq!(prepared.keep_tx_per_account, 1);
        assert_eq!(
            tail_account_lts(&storage, prepared.target_generation, first_key, frontier.seqno),
            [10],
        );
        assert_eq!(
            tail_account_lts(&storage, prepared.target_generation, second_key, frontier.seqno),
            [20],
        );
        let progress = storage
            .tail
            .generation_progress(prepared.target_generation)
            .unwrap()
            .unwrap();
        assert!(progress.eof);
        assert_eq!(progress.counters.processed_accounts, 2);
        assert_eq!(progress.counters.promoted_records, 2);
    }

    #[tokio::test]
    async fn gc_evacuation_refreshes_frozen_snapshot_between_chunks() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = RpcStorage::open(context, gc_test_config(1, 16 * 1024 * 1024)).unwrap();
        storage.sealing_cancel.cancel();
        storage.filter_worker.shutdown();
        let first_account = gc_account(0x31);
        let second_account = gc_account(0x32);
        let first_key = gc_account_key(&first_account);
        let second_key = gc_account_key(&second_account);
        let candidate_mc = masterchain_block(1);
        let source = insert_read_test_block(
            &storage,
            &first_account,
            &candidate_mc,
            &basechain_block(1),
            &[gc_transaction(10, 0x41)],
            0,
        );
        insert_read_test_block(
            &storage,
            &second_account,
            &candidate_mc,
            &basechain_block(1),
            &[gc_transaction(20, 0x42)],
            1,
        );
        storage.commit_masterchain_block_set(&candidate_mc).unwrap();
        seal_gc_partition(&storage, source).await;
        let initial_frontier = masterchain_block(100);
        publish_test_frontier(&storage, &initial_frontier);
        let gc_config = TransactionsGcConfig {
            tx_ttl: Duration::from_secs(10),
            keep_tx_per_account: 1,
        };
        let intent = storage
            .begin_or_resume_gc(Some(&gc_config))
            .unwrap()
            .unwrap();
        assert!(matches!(
            storage.evacuate_gc_chunk(intent).unwrap(),
            GcEvacuationChunkResult::Appended,
        ));
        let first_progress = storage
            .tail
            .generation_progress(intent.target_generation)
            .unwrap()
            .unwrap();
        assert_eq!(first_progress.cursor, codec::TailProgressCursor::Account(first_key));
        assert_eq!(first_progress.counters.promoted_records, 1);

        let newer_frontier = masterchain_block(101);
        let newer_hash = HashBytes([0x91; 32]);
        insert_read_test_block(
            &storage,
            &second_account,
            &newer_frontier,
            &basechain_block(101),
            &[ReadTestTransaction {
                lt: 200,
                hash: newer_hash,
                in_msg_hash: HashBytes([0x92; 32]),
                boc_byte: 0x93,
            }],
            0,
        );
        storage.commit_masterchain_block_set(&newer_frontier).unwrap();
        let prepared = storage.evacuate_gc(Some(&gc_config)).unwrap().unwrap();
        assert_eq!(prepared.phase, codec::GcIntentPhase::Prepared);
        assert_eq!(
            tail_account_lts(
                &storage,
                prepared.target_generation,
                first_key,
                newer_frontier.seqno,
            ),
            [10],
        );
        assert!(tail_account_lts(
            &storage,
            prepared.target_generation,
            second_key,
            newer_frontier.seqno,
        ).is_empty());
        assert_eq!(
            storage
                .get_transaction(&newer_hash, storage.load_snapshot().as_ref())
                .unwrap()
                .unwrap()
                .as_ref(),
            [0x93],
        );
        let progress = storage
            .tail
            .generation_progress(prepared.target_generation)
            .unwrap()
            .unwrap();
        assert_eq!(progress.counters.processed_accounts, 2);
        assert_eq!(progress.counters.promoted_records, 1);
    }

    #[tokio::test]
    async fn gc_evacuation_ignores_active_transaction_staged_after_frozen_snapshot() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = RpcStorage::open(context, gc_test_config(128, 16 * 1024 * 1024)).unwrap();
        storage.sealing_cancel.cancel();
        storage.filter_worker.shutdown();
        let account = gc_account(0x35);
        let account_key = gc_account_key(&account);
        let candidate_mc = masterchain_block(1);
        let source = insert_read_test_block(
            &storage,
            &account,
            &candidate_mc,
            &basechain_block(1),
            &[gc_transaction(10, 0x45)],
            1,
        );
        storage.commit_masterchain_block_set(&candidate_mc).unwrap();
        seal_gc_partition(&storage, source).await;
        let frozen_frontier = masterchain_block(100);
        publish_test_frontier(&storage, &frozen_frontier);

        let staged_mc = masterchain_block(101);
        insert_read_test_block(
            &storage,
            &account,
            &staged_mc,
            &basechain_block(101),
            &[gc_transaction(200, 0x46)],
            0,
        );
        let prepared = storage
            .evacuate_gc(Some(&TransactionsGcConfig {
                tx_ttl: Duration::from_secs(10),
                keep_tx_per_account: 1,
            }))
            .unwrap()
            .unwrap();
        assert_eq!(prepared.phase, codec::GcIntentPhase::Prepared);
        assert_eq!(
            tail_account_lts(
                &storage,
                prepared.target_generation,
                account_key,
                frozen_frontier.seqno,
            ),
            [10],
        );
        let progress = storage
            .tail
            .generation_progress(prepared.target_generation)
            .unwrap()
            .unwrap();
        assert_eq!(progress.counters.processed_accounts, 1);
        assert_eq!(progress.counters.promoted_records, 1);
    }

    #[tokio::test]
    async fn gc_evacuation_fills_from_visible_tail_and_retires_only_older_history() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let config = gc_test_config(128, 16 * 1024 * 1024);
        let storage = RpcStorage::open(context.clone(), config.clone()).unwrap();
        storage.sealing_cancel.cancel();
        storage.filter_worker.shutdown();
        let account = gc_account(0x41);
        let account_key = gc_account_key(&account);
        let (tail5, bytes5) = gc_tail_promotion(account_key, 5, 0x51, 1, 1);
        let (tail15, bytes15) = gc_tail_promotion(account_key, 15, 0x52, 1, 1);
        let counters = codec::TailGenerationCounters {
            processed_accounts: 1,
            promoted_records: 2,
            promoted_bytes: bytes5 + bytes15,
            ..Default::default()
        };
        let progress = codec::TailGenerationProgress {
            target_generation: 1,
            operation_id: 0x101,
            source_partition_id: 7,
            source_manifest_digest: HashBytes([0x61; 32]),
            retention_policy_digest: HashBytes([0x62; 32]),
            cursor: codec::TailProgressCursor::Account(account_key),
            eof: false,
            counters,
            chunk_digest: codec::EMPTY_TAIL_CHUNK_DIGEST,
        };
        let progress = storage
            .tail
            .append_chunk(
                &[AccountTailDelta::new(account_key, vec![tail5, tail15], vec![]).unwrap()],
                None,
                progress,
            )
            .unwrap();
        storage
            .tail
            .finish_generation(
                codec::TailGenerationProgress { eof: true, ..progress },
                codec::TailGenerationCommit {
                    layout_version: codec::TailLayoutVersion::MonolithicV1,
                    target_generation: 1,
                    operation_id: progress.operation_id,
                    source_partition_id: progress.source_partition_id,
                    source_manifest_digest: progress.source_manifest_digest,
                    previous_visible_generation: 0,
                    cutoff_utime: 1,
                    keep_tx_per_account: 2,
                    retention_policy_digest: progress.retention_policy_digest,
                    counters,
                },
            )
            .unwrap();
        {
            let manager = storage.partitions.lock();
            let mut control_state = codec::decode_control_state(
                manager
                    .control_db()
                    .state
                    .get(codec::control_state_key())
                    .unwrap()
                    .unwrap()
                    .as_ref(),
            )
            .unwrap();
            control_state.tail_visible_generation = 1;
            manager
                .control_db()
                .state
                .insert(
                    codec::control_state_key(),
                    codec::encode_control_state(control_state),
                )
                .unwrap();
        }
        drop(storage);
        tokio::task::yield_now().await;

        let storage = RpcStorage::open(context, config).unwrap();
        storage.sealing_cancel.cancel();
        storage.filter_worker.shutdown();
        let candidate_mc = masterchain_block(2);
        let source = insert_read_test_block(
            &storage,
            &account,
            &candidate_mc,
            &basechain_block(2),
            &[gc_transaction(20, 0x53)],
            1,
        );
        storage.commit_masterchain_block_set(&candidate_mc).unwrap();
        seal_gc_partition(&storage, source).await;
        let frontier = masterchain_block(100);
        publish_test_frontier(&storage, &frontier);
        let old_tail = storage
            .tail
            .request_snapshot(codec::TailLayoutVersion::MonolithicV1, 1)
            .unwrap();
        let prepared = storage
            .evacuate_gc(Some(&TransactionsGcConfig {
                tx_ttl: Duration::from_secs(10),
                keep_tx_per_account: 2,
            }))
            .unwrap()
            .unwrap();
        assert_eq!(prepared.target_generation, 2);
        assert_eq!(
            old_tail
                .newest_account_transactions(account_key, None, usize::MAX, frontier.seqno)
                .unwrap()
                .into_iter()
                .map(|record| codec::decode_tail_payload_key(&record.payload_key).unwrap().1)
                .collect::<Vec<_>>(),
            [15, 5],
        );
        assert_eq!(
            tail_account_lts(&storage, prepared.target_generation, account_key, frontier.seqno),
            [20, 15],
        );
        let progress = storage
            .tail
            .generation_progress(prepared.target_generation)
            .unwrap()
            .unwrap();
        assert_eq!(progress.counters.processed_accounts, 1);
        assert_eq!(progress.counters.promoted_records, 1);
        assert_eq!(progress.counters.retired_records, 1);
        assert_eq!(progress.counters.retired_bytes, bytes5);
    }

    #[tokio::test]
    async fn gc_evacuation_keeps_an_oversized_account_atomic_at_the_soft_limit() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = RpcStorage::open(context, gc_test_config(128, 1)).unwrap();
        storage.sealing_cancel.cancel();
        storage.filter_worker.shutdown();
        let first_account = gc_account(0x51);
        let second_account = gc_account(0x52);
        let first_key = gc_account_key(&first_account);
        let second_key = gc_account_key(&second_account);
        let candidate_mc = masterchain_block(1);
        let source = insert_read_test_block(
            &storage,
            &first_account,
            &candidate_mc,
            &basechain_block(1),
            &[gc_transaction(10, 0x61), gc_transaction(20, 0x62)],
            0,
        );
        insert_read_test_block(
            &storage,
            &second_account,
            &candidate_mc,
            &basechain_block(1),
            &[gc_transaction(30, 0x63)],
            1,
        );
        storage.commit_masterchain_block_set(&candidate_mc).unwrap();
        seal_gc_partition(&storage, source).await;
        let frontier = masterchain_block(100);
        publish_test_frontier(&storage, &frontier);
        let gc_config = TransactionsGcConfig {
            tx_ttl: Duration::from_secs(10),
            keep_tx_per_account: 2,
        };
        let intent = storage
            .begin_or_resume_gc(Some(&gc_config))
            .unwrap()
            .unwrap();
        let recorder = TestMetricsRecorder::default();

        assert!(matches!(metrics::with_local_recorder(&recorder, ||
            storage.evacuate_gc_chunk(intent).unwrap()),
            GcEvacuationChunkResult::Appended,
        ));
        let first_progress = storage
            .tail
            .generation_progress(intent.target_generation)
            .unwrap()
            .unwrap();
        assert_eq!(first_progress.cursor, codec::TailProgressCursor::Account(first_key));
        assert_eq!(first_progress.counters.processed_accounts, 1);
        assert_eq!(first_progress.counters.promoted_records, 2);
        assert!(first_progress.counters.promoted_bytes > 1);

        assert!(matches!(metrics::with_local_recorder(&recorder, ||
            storage.evacuate_gc_chunk(intent).unwrap()),
            GcEvacuationChunkResult::Appended,
        ));
        let second_progress = storage
            .tail
            .generation_progress(intent.target_generation)
            .unwrap()
            .unwrap();
        assert_eq!(second_progress.cursor, codec::TailProgressCursor::Account(second_key));
        assert_eq!(second_progress.counters.processed_accounts, 2);
        assert_eq!(second_progress.counters.promoted_records, 3);
        let prepared = match metrics::with_local_recorder(&recorder, ||
            storage.evacuate_gc_chunk(intent).unwrap()) {
            GcEvacuationChunkResult::Prepared(prepared) => prepared,
            GcEvacuationChunkResult::Appended => panic!("expected terminal GC evacuation chunk"),
        };
        assert_eq!(
            tail_account_lts(&storage, prepared.target_generation, first_key, frontier.seqno),
            [20, 10],
        );
        assert_eq!(
            tail_account_lts(&storage, prepared.target_generation, second_key, frontier.seqno),
            [30],
        );
        assert_eq!(
            recorder.histogram_values("tycho_storage_rpc_gc_chunk_accounts"),
            [1.0, 1.0],
        );
        assert!(recorder
            .histogram_values("tycho_storage_rpc_gc_chunk_staged_bytes")
            .into_iter()
            .all(|value| value > 1.0));
        assert_eq!(
            recorder.histogram_len(
                "tycho_storage_rpc_gc_chunk_duration_seconds|result=success"
            ),
            3,
        );
        assert_eq!(
            recorder.counter("tycho_storage_rpc_gc_oversized_soft_limit_chunks_total"),
            2,
        );
        assert_eq!(
            recorder.gauge("tycho_storage_rpc_gc_candidate_accounts|state=total"),
            2.0,
        );
        assert_eq!(
            recorder.gauge("tycho_storage_rpc_gc_candidate_accounts|state=processed"),
            2.0,
        );
        assert_eq!(
            recorder.gauge(
                "tycho_storage_rpc_gc_intent_info|policy=ttl_keep_n|cutoff=fixed_frontier"
            ),
            1.0,
        );
        assert_eq!(recorder.gauge("tycho_storage_rpc_gc_cursor_age_seconds"), 0.0);
    }

    #[test]
    fn gc_observability_uses_bounded_filter_and_generation_labels() {
        assert_eq!(
            [
                GcAccountFilterResult::Negative,
                GcAccountFilterResult::Positive,
                GcAccountFilterResult::Unknown,
            ]
            .map(GcAccountFilterResult::as_str),
            ["negative", "positive", "unknown"],
        );
        let recorder = TestMetricsRecorder::default();
        metrics::with_local_recorder(&recorder, || {
            for result in [
                GcAccountFilterResult::Negative,
                GcAccountFilterResult::Positive,
                GcAccountFilterResult::Unknown,
            ] {
                record_gc_account_filter_probe(result);
            }
            let counters = codec::TailGenerationCounters {
                processed_accounts: 2,
                promoted_records: 3,
                promoted_bytes: 30,
                retired_records: 4,
                retired_bytes: 40,
            };
            record_gc_generation_cutover_metrics(false, counters);
            record_gc_generation_cutover_metrics(true, counters);
        });
        for label in ["negative", "positive", "unknown"] {
            assert_eq!(
                recorder.counter(&format!(
                    "tycho_storage_rpc_gc_account_filter_probes_total|result={label}"
                )),
                1,
            );
        }
        assert_eq!(
            recorder.histogram_values(
                "tycho_storage_rpc_gc_generation_records|action=promoted"
            ),
            [3.0],
        );
        assert_eq!(
            recorder.histogram_values(
                "tycho_storage_rpc_gc_generation_records|action=retired"
            ),
            [4.0],
        );
        assert_eq!(
            recorder.histogram_values(
                "tycho_storage_rpc_gc_generation_bytes|action=promoted"
            ),
            [30.0],
        );
        assert_eq!(
            recorder.histogram_values(
                "tycho_storage_rpc_gc_generation_bytes|action=retired"
            ),
            [40.0],
        );
    }

    #[tokio::test]
    async fn gc_evacuation_advances_at_most_128_accounts_per_chunk() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = RpcStorage::open(context, gc_test_config(128, 16 * 1024 * 1024)).unwrap();
        storage.sealing_cancel.cancel();
        storage.filter_worker.shutdown();
        let (source, lease) = {
            let manager = storage.partitions.lock();
            (manager.active_id(), manager.active_lease())
        };
        let accounts = (1..=129)
            .map(|byte| gc_account_key(&gc_account(byte)))
            .collect::<Vec<_>>();
        let mut batch = rocksdb::WriteBatch::default();
        for account in &accounts {
            batch.put_cf(&lease.accounts.cf(), account, []);
        }
        lease
            .rocksdb()
            .write_opt(batch, lease.accounts.write_config())
            .unwrap();
        drop(lease);
        let candidate_mc = masterchain_block(1);
        insert_masterchain_commit(&storage.partitions.lock(), &candidate_mc, 1);
        storage.commit_masterchain_block_set(&candidate_mc).unwrap();
        seal_gc_partition(&storage, source).await;
        let frontier = masterchain_block(100);
        publish_test_frontier(&storage, &frontier);
        let gc_config = TransactionsGcConfig {
            tx_ttl: Duration::from_secs(10),
            keep_tx_per_account: 0,
        };
        let intent = storage
            .begin_or_resume_gc(Some(&gc_config))
            .unwrap()
            .unwrap();

        assert!(matches!(
            storage.evacuate_gc_chunk(intent).unwrap(),
            GcEvacuationChunkResult::Appended,
        ));
        let first_progress = storage
            .tail
            .generation_progress(intent.target_generation)
            .unwrap()
            .unwrap();
        assert_eq!(
            first_progress.cursor,
            codec::TailProgressCursor::Account(accounts[127]),
        );
        assert_eq!(first_progress.counters.processed_accounts, 128);

        assert!(matches!(
            storage.evacuate_gc_chunk(intent).unwrap(),
            GcEvacuationChunkResult::Appended,
        ));
        let second_progress = storage
            .tail
            .generation_progress(intent.target_generation)
            .unwrap()
            .unwrap();
        assert_eq!(
            second_progress.cursor,
            codec::TailProgressCursor::Account(accounts[128]),
        );
        assert_eq!(second_progress.counters.processed_accounts, 129);
        assert!(matches!(
            storage.evacuate_gc_chunk(intent).unwrap(),
            GcEvacuationChunkResult::Prepared(_),
        ));
    }

    #[tokio::test]
    async fn gc_candidate_cursor_requires_an_exact_well_formed_account_marker() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = RpcStorage::open(context, gc_test_config(128, 16 * 1024 * 1024)).unwrap();
        storage.sealing_cancel.cancel();
        storage.filter_worker.shutdown();
        let missing = gc_account_key(&gc_account(0x61));
        let present = gc_account_key(&gc_account(0x62));
        let lease = storage.partitions.lock().active_lease();
        lease.accounts.insert(present, []).unwrap();

        let error = candidate_accounts_after(
            &lease,
            codec::TailProgressCursor::Account(missing),
            1,
        )
        .unwrap_err();
        assert_eq!(
            classify_authoritative_error(&error),
            Some(AuthoritativeErrorKind::MissingCommittedData),
        );

        lease.accounts.insert(present, [1]).unwrap();
        let error = candidate_accounts_after(
            &lease,
            codec::TailProgressCursor::Account(present),
            1,
        )
        .unwrap_err();
        assert_eq!(
            classify_authoritative_error(&error),
            Some(AuthoritativeErrorKind::MalformedCommittedData),
        );
        assert!(!storage.is_resync_required());
        assert_eq!(storage.gc.resync_transitions(), 0);
    }

    #[tokio::test]
    async fn gc_evacuation_rejects_selected_candidate_with_missing_inbound_locator() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = Arc::new(
            RpcStorage::open(context, gc_test_config(128, 16 * 1024 * 1024)).unwrap(),
        );
        storage.sealing_cancel.cancel();
        storage.filter_worker.shutdown();
        let account = gc_account(0x71);
        let inbound_hash = HashBytes([0xb1; 32]);
        let candidate_mc = masterchain_block(1);
        let source = insert_read_test_block(
            &storage,
            &account,
            &candidate_mc,
            &basechain_block(1),
            &[ReadTestTransaction {
                lt: 10,
                hash: HashBytes([0x72; 32]),
                in_msg_hash: inbound_hash,
                boc_byte: 0x73,
            }],
            1,
        );
        {
            let lease = storage.partitions.lock().active_lease();
            let mut batch = rocksdb::WriteBatch::default();
            batch.delete_cf(&lease.transactions_by_in_msg.cf(), inbound_hash);
            lease
                .rocksdb()
                .write_opt(batch, lease.transactions_by_in_msg.write_config())
                .unwrap();
        }
        storage.commit_masterchain_block_set(&candidate_mc).unwrap();
        seal_gc_partition(&storage, source).await;
        let frontier = masterchain_block(100);
        publish_test_frontier(&storage, &frontier);
        let intent = storage
            .begin_or_resume_gc(Some(&TransactionsGcConfig {
                tx_ttl: Duration::from_secs(10),
                keep_tx_per_account: 1,
            }))
            .unwrap()
            .unwrap();

        let recorder = TestMetricsRecorder::default();
        let error = match metrics::with_local_recorder(&recorder, ||
            storage.evacuate_gc_chunk(intent)) {
            Ok(_) => panic!("candidate with a missing inbound locator must fail evacuation"),
            Err(error) => error,
        };
        assert_eq!(
            recorder.histogram_len(
                "tycho_storage_rpc_gc_chunk_duration_seconds|result=failure"
            ),
            1,
        );
        assert!(format!("{error:#}").contains(
            "RPC transaction GC candidate transaction is missing its inbound-message locator"
        ));
        assert_eq!(
            classify_authoritative_error(&error),
            Some(AuthoritativeErrorKind::MissingCommittedData),
        );
        assert!(storage
            .tail
            .generation_progress(intent.target_generation)
            .unwrap()
            .is_none());
        assert!(storage
            .tail
            .generation_commit(intent.target_generation)
            .unwrap()
            .is_none());
        storage.start_maintenance(&frontier).unwrap();
        tokio::time::timeout(
            Duration::from_secs(5),
            storage.gc.wait_for_resync_required(),
        )
        .await
        .unwrap();
        assert!(storage.is_resync_required());
        assert!(storage.load_snapshot().is_none());
        assert_eq!(storage.gc.resync_transitions(), 1);
    }

    #[tokio::test]
    async fn gc_evacuation_accounts_filter_false_positive_falls_back_to_exact_scan() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = RpcStorage::open(context, gc_test_config(128, 16 * 1024 * 1024)).unwrap();
        storage.sealing_cancel.cancel();
        storage.filter_worker.shutdown();
        let target_account = gc_account(0x81);
        let target_key = gc_account_key(&target_account);
        let candidate_mc = masterchain_block(1);
        let source = insert_read_test_block(
            &storage,
            &target_account,
            &candidate_mc,
            &basechain_block(1),
            &[gc_transaction(10, 0x82)],
            1,
        );
        storage.commit_masterchain_block_set(&candidate_mc).unwrap();
        seal_gc_partition(&storage, source).await;

        let negative_account = gc_account(0x85);
        let negative_key = gc_account_key(&negative_account);
        let negative_mc = masterchain_block(2);
        let negative_block = basechain_block(2);
        let negative_transaction = gc_transaction(20, 0x86);
        let negative = insert_read_test_block(
            &storage,
            &negative_account,
            &negative_mc,
            &negative_block,
            &[ReadTestTransaction {
                lt: negative_transaction.lt,
                hash: negative_transaction.hash,
                in_msg_hash: negative_transaction.in_msg_hash,
                boc_byte: negative_transaction.boc_byte,
            }],
            1,
        );
        storage.commit_masterchain_block_set(&negative_mc).unwrap();
        seal_gc_partition(&storage, negative).await;
        let newer_account = gc_account(0x83);
        let newer_key = gc_account_key(&newer_account);
        let newer_mc = masterchain_block(3);
        let newer_block = basechain_block(3);
        let newer_transaction = gc_transaction(30, 0x84);
        let newer = insert_read_test_block(
            &storage,
            &newer_account,
            &newer_mc,
            &newer_block,
            &[ReadTestTransaction {
                lt: newer_transaction.lt,
                hash: newer_transaction.hash,
                in_msg_hash: newer_transaction.in_msg_hash,
                boc_byte: newer_transaction.boc_byte,
            }],
            1,
        );
        storage.commit_masterchain_block_set(&newer_mc).unwrap();
        seal_gc_partition(&storage, newer).await;
        let frontier = masterchain_block(100);
        publish_test_frontier(&storage, &frontier);
        let negative_manifest_digest = storage
            .partitions
            .lock()
            .sealed_manifest_digest(negative)
            .unwrap();
        let negative_bundle = test_filter_bundle_with_accounts(
            negative,
            1,
            negative_manifest_digest,
            &[negative_transaction.hash],
            &[negative_transaction.in_msg_hash],
            &[known_block_key(&negative_block), known_block_key(&negative_mc)],
            &[negative_key],
        );
        publish_filter_snapshot(
            &storage.partitions,
            &storage.snapshots,
            &storage.filter_registry,
            negative,
            negative_bundle,
        )
        .unwrap();
        let manifest_digest = storage
            .partitions
            .lock()
            .sealed_manifest_digest(newer)
            .unwrap();
        let bundle = test_filter_bundle_with_accounts(
            newer,
            1,
            manifest_digest,
            &[newer_transaction.hash],
            &[newer_transaction.in_msg_hash],
            &[known_block_key(&newer_block), known_block_key(&newer_mc)],
            &[target_key, newer_key],
        );
        publish_filter_snapshot(
            &storage.partitions,
            &storage.snapshots,
            &storage.filter_registry,
            newer,
            bundle,
        )
        .unwrap();
        let snapshot = storage.load_snapshot().unwrap();
        assert!(!snapshot
            .filter_might_contain(negative, FilterNamespace::Accounts, &target_key)
            .unwrap());
        assert!(snapshot
            .filter_might_contain(newer, FilterNamespace::Accounts, &target_key)
            .unwrap());
        assert_eq!(
            count_partition_account_transaction_keys(&snapshot, newer, target_key, 1, true).unwrap(),
            0,
        );
        drop(snapshot);

        let recorder = TestMetricsRecorder::default();
        let prepared = metrics::with_local_recorder(&recorder, || storage
            .evacuate_gc(Some(&TransactionsGcConfig {
                tx_ttl: Duration::from_secs(10),
                keep_tx_per_account: 1,
            }))
            .unwrap()
            .unwrap());
        assert_eq!(prepared.phase, codec::GcIntentPhase::Prepared);
        assert_eq!(
            tail_account_lts(&storage, prepared.target_generation, target_key, frontier.seqno),
            [10],
        );
        let progress = storage
            .tail
            .generation_progress(prepared.target_generation)
            .unwrap()
            .unwrap();
        assert_eq!(progress.counters.promoted_records, 1);
        assert_eq!(
            recorder.counter(
                "tycho_storage_rpc_gc_account_filter_probes_total|result=positive"
            ),
            1,
        );
        assert_eq!(
            recorder.counter(
                "tycho_storage_rpc_gc_account_filter_probes_total|result=negative"
            ),
            1,
        );
        assert_eq!(
            recorder.counter(
                "tycho_storage_rpc_gc_account_filter_probes_total|result=unknown"
            ),
            1,
        );
        assert_eq!(
            recorder.counter("tycho_storage_rpc_gc_account_exact_seeks_total"),
            2,
        );
        assert_eq!(
            recorder.counter("tycho_storage_rpc_gc_account_filter_false_positives_total"),
            1,
        );
        assert_eq!(
            recorder.histogram_values("tycho_storage_rpc_gc_newer_partitions_probed"),
            [3.0],
        );
    }

    #[tokio::test]
    async fn gc_evacuation_enumerates_only_candidate_account_markers() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = RpcStorage::open(context, gc_test_config(128, 16 * 1024 * 1024)).unwrap();
        storage.sealing_cancel.cancel();
        storage.filter_worker.shutdown();
        let candidate_account = gc_account(0x87);
        let candidate_key = gc_account_key(&candidate_account);
        let candidate_mc = masterchain_block(1);
        let source = insert_read_test_block(
            &storage,
            &candidate_account,
            &candidate_mc,
            &basechain_block(1),
            &[gc_transaction(10, 0x88)],
            1,
        );
        storage.commit_masterchain_block_set(&candidate_mc).unwrap();
        seal_gc_partition(&storage, source).await;

        let unrelated_account = gc_account(0x89);
        let unrelated_key = gc_account_key(&unrelated_account);
        let newer_mc = masterchain_block(2);
        let newer = insert_read_test_block(
            &storage,
            &unrelated_account,
            &newer_mc,
            &basechain_block(2),
            &[gc_transaction(20, 0x8a)],
            1,
        );
        storage.commit_masterchain_block_set(&newer_mc).unwrap();
        {
            let lease = storage.partitions.lock().active_lease();
            lease.accounts.insert(unrelated_key, [1]).unwrap();
            lease
                .transactions
                .insert(vec![0xff; codec::ACCOUNT_KEY_LEN - 1], [0])
                .unwrap();
        }
        seal_gc_partition(&storage, newer).await;
        let frontier = masterchain_block(100);
        publish_test_frontier(&storage, &frontier);

        let recorder = TestMetricsRecorder::default();
        let prepared = metrics::with_local_recorder(&recorder, || storage
            .evacuate_gc(Some(&TransactionsGcConfig {
                tx_ttl: Duration::from_secs(10),
                keep_tx_per_account: 1,
            }))
            .unwrap()
            .unwrap());
        assert_eq!(prepared.source_partition_id, source.0);
        assert_eq!(
            tail_account_lts(
                &storage,
                prepared.target_generation,
                candidate_key,
                frontier.seqno,
            ),
            [10],
        );
        assert_eq!(
            recorder.counter("tycho_storage_rpc_gc_account_exact_seeks_total"),
            2,
        );
        assert_eq!(
            recorder.histogram_values("tycho_storage_rpc_gc_newer_partitions_probed"),
            [2.0],
        );
        assert!(!storage.is_resync_required());
    }

    #[tokio::test]
    async fn newer_partition_key_corruption_is_authoritative_only_when_sealed() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = RpcStorage::open(context, gc_test_config(128, 16 * 1024 * 1024)).unwrap();
        storage.sealing_cancel.cancel();
        storage.filter_worker.shutdown();
        let target = gc_account_key(&gc_account(0xa1));
        let first_frontier = masterchain_block(1);
        let source = insert_read_test_block(
            &storage,
            &gc_account(0xa2),
            &first_frontier,
            &basechain_block(1),
            &[gc_transaction(10, 0xa3)],
            0,
        );
        let mut malformed_key = target.to_vec();
        malformed_key.extend_from_slice(&[0xff; 4]);
        storage
            .partitions
            .lock()
            .active_lease()
            .transactions
            .insert(&malformed_key, [0])
            .unwrap();
        storage.commit_masterchain_block_set(&first_frontier).unwrap();
        let active_snapshot = storage.load_snapshot().unwrap();

        let error = count_partition_account_transaction_keys(
            &active_snapshot,
            source,
            target,
            1,
            false,
        )
        .unwrap_err();
        assert_eq!(classify_authoritative_error(&error), None);
        drop(active_snapshot);

        let second_frontier = masterchain_block(2);
        assert_eq!(
            insert_read_test_block(
                &storage,
                &gc_account(0xa4),
                &second_frontier,
                &basechain_block(2),
                &[gc_transaction(20, 0xa5)],
                1,
            ),
            source,
        );
        storage.commit_masterchain_block_set(&second_frontier).unwrap();
        seal_gc_partition(&storage, source).await;
        let sealed_snapshot = storage.load_snapshot().unwrap();
        let error = count_partition_account_transaction_keys(
            &sealed_snapshot,
            source,
            target,
            1,
            true,
        )
        .unwrap_err();
        assert_eq!(
            classify_authoritative_error(&error),
            Some(AuthoritativeErrorKind::MalformedCommittedData),
        );
        assert!(!storage.is_resync_required());
        assert_eq!(storage.gc.resync_transitions(), 0);
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
        let mut below = BlockWriteStats {
            transaction_count: 0,
            index_record_count: 0,
            estimated_lsm_bytes: 0,
            estimated_blob_bytes: 0,
        };
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
    fn block_write_stats_separates_account_and_block_metadata() {
        let mut accounting = BlockWriteAccounting::default();
        accounting.stats.add_account_marker().unwrap();
        accounting.add_block_metadata_record(17, 80).unwrap();
        accounting.add_block_metadata_record(
            codec::PARTITION_COMMIT_KEY_LEN,
            codec::PARTITION_COMMIT_VALUE_LEN,
        ).unwrap();
        assert_eq!(accounting.stats.transaction_count, 0);
        assert_eq!(accounting.stats.index_record_count, 0);
        assert_eq!(accounting.stats.estimated_lsm_bytes, tables::Accounts::KEY_LEN as u64);
        assert_eq!(accounting.stats.estimated_blob_bytes, 0);
        assert_eq!(accounting.estimated_block_metadata_bytes, (17 + 80 + codec::PARTITION_COMMIT_KEY_LEN + codec::PARTITION_COMMIT_VALUE_LEN) as u64);
    }

    #[tokio::test]
    async fn point_and_source_reads_fall_back_to_visible_tail_after_partition_miss() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = RpcStorage::open(context, test_partitions_config()).unwrap();
        let account = StdAddr::new(0, HashBytes([0x41; 32]));
        let mut account_key = [0; codec::ACCOUNT_KEY_LEN];
        account_key[0] = account.workchain as u8;
        account_key[1..].copy_from_slice(account.address.as_slice());
        let transaction_hash = HashBytes([0x51; 32]);
        let inbound_message_hash = HashBytes([0x61; 32]);
        let block_id = basechain_block(5);
        let mut payload = Vec::with_capacity(66);
        payload.push(TransactionMask::HAS_MSG_HASH.bits());
        payload.extend_from_slice(transaction_hash.as_slice());
        payload.extend_from_slice(inbound_message_hash.as_slice());
        payload.push(0xb5);
        let value = codec::encode_transaction_value(7, &payload).unwrap();
        let counters = codec::TailGenerationCounters {
            processed_accounts: 1,
            promoted_records: 1,
            promoted_bytes: value.len() as u64,
            ..Default::default()
        };
        let promoted = TailPromotedTransaction::new(
            codec::tail_payload_key(account_key, 10),
            value,
            block_id,
            1,
        ).unwrap();
        let progress = codec::TailGenerationProgress {
            target_generation: 1,
            operation_id: 10,
            source_partition_id: 1,
            source_manifest_digest: HashBytes([0x71; 32]),
            retention_policy_digest: HashBytes([0x81; 32]),
            cursor: codec::TailProgressCursor::Account(account_key),
            eof: false,
            counters,
            chunk_digest: codec::EMPTY_TAIL_CHUNK_DIGEST,
        };
        let progress = storage.tail.append_chunk(
            &[AccountTailDelta::new(account_key, vec![promoted], vec![]).unwrap()],
            None,
            progress,
        ).unwrap();
        let terminal = codec::TailGenerationProgress { eof: true, ..progress };
        storage.tail.finish_generation(
            terminal,
            codec::TailGenerationCommit {
                layout_version: codec::TailLayoutVersion::MonolithicV1,
                target_generation: 1,
                operation_id: progress.operation_id,
                source_partition_id: progress.source_partition_id,
                source_manifest_digest: progress.source_manifest_digest,
                previous_visible_generation: 0,
                cutoff_utime: 10,
                keep_tx_per_account: 2,
                retention_policy_digest: progress.retention_policy_digest,
                counters,
            },
        ).unwrap();

        let snapshot_at = |frontier| {
            let base = {
                let mut manager = storage.partitions.lock();
                build_composite_snapshot(
                    &mut manager,
                    &storage.filter_registry,
                    &storage.tail,
                    frontier,
                ).unwrap()
            };
            let RpcSnapshot(inner, filter_bundles) = base;
            let mut inner = Arc::try_unwrap(inner).ok().unwrap();
            inner.tail = storage.tail.request_snapshot(
                codec::TailLayoutVersion::MonolithicV1,
                1,
            ).unwrap();
            RpcSnapshot(Arc::new(inner), filter_bundles)
        };

        let future = snapshot_at(masterchain_block(6));
        assert!(storage.get_transaction(&transaction_hash, Some(&future)).unwrap().is_none());
        assert!(storage.get_transaction_info(&transaction_hash, Some(&future)).unwrap().is_none());
        assert!(storage.get_dst_transaction(&inbound_message_hash, Some(&future)).unwrap().is_none());
        assert!(storage.get_src_transaction(&account, 11, Some(&future)).unwrap().is_none());
        drop(future);

        let visible = snapshot_at(masterchain_block(7));
        assert_eq!(
            storage.get_transaction(&transaction_hash, Some(&visible)).unwrap().unwrap().as_ref(),
            [0xb5],
        );
        let ext = storage.get_transaction_ext(&transaction_hash, Some(&visible)).unwrap().unwrap();
        assert_eq!(ext.data.as_ref(), [0xb5]);
        assert_eq!(ext.info.account, account);
        assert_eq!(ext.info.lt, 10);
        assert_eq!(ext.info.block_id, block_id);
        assert_eq!(ext.info.mc_seqno, 7);
        let info = storage.get_transaction_info(&transaction_hash, Some(&visible)).unwrap().unwrap();
        assert_eq!(info.account, account);
        assert_eq!(info.lt, 10);
        assert_eq!(info.block_id, block_id);
        assert_eq!(info.mc_seqno, 7);
        assert_eq!(
            storage.get_dst_transaction(&inbound_message_hash, Some(&visible)).unwrap().unwrap().as_ref(),
            [0xb5],
        );
        assert_eq!(
            storage.get_src_transaction(&account, 11, Some(&visible)).unwrap().unwrap().as_ref(),
            [0xb5],
        );
        assert!(storage
            .get_brief_block_info(&block_id.as_short_id(), Some(&visible))
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn hidden_same_identity_tail_records_do_not_mask_a_visible_partition() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = RpcStorage::open(
            context,
            RpcTransactionPartitionsConfig {
                target_transaction_lsm_bytes: u64::MAX,
                target_transaction_blob_bytes: u64::MAX,
                target_transaction_index_records: u64::MAX,
                target_block_metadata_bytes: u64::MAX,
                ..Default::default()
            },
        )
        .unwrap();
        storage.sealing_cancel.cancel();
        storage.filter_worker.shutdown();
        let account = gc_account(0x4a);
        let account_key = gc_account_key(&account);
        let tail_transaction = gc_transaction(10, 0x4b);
        let transaction_hash = tail_transaction.hash;
        let inbound_message_hash = tail_transaction.in_msg_hash;
        let frontier = masterchain_block(5);
        insert_read_test_block(
            &storage,
            &account,
            &frontier,
            &basechain_block(5),
            &[ReadTestTransaction {
                lt: 10,
                hash: transaction_hash,
                in_msg_hash: inbound_message_hash,
                boc_byte: 0x4c,
            }],
            0,
        );
        storage.commit_masterchain_block_set(&frontier).unwrap();

        let (promoted, promoted_bytes) = gc_tail_promotion(account_key, 10, 0x4b, 5, 1);
        let payload_key = promoted.payload_key();
        let first_counters = codec::TailGenerationCounters {
            processed_accounts: 1,
            promoted_records: 1,
            promoted_bytes,
            ..Default::default()
        };
        let first_progress = codec::TailGenerationProgress {
            target_generation: 1,
            operation_id: 0x401,
            source_partition_id: 7,
            source_manifest_digest: HashBytes([0x4d; 32]),
            retention_policy_digest: HashBytes([0x4e; 32]),
            cursor: codec::TailProgressCursor::Account(account_key),
            eof: false,
            counters: first_counters,
            chunk_digest: codec::EMPTY_TAIL_CHUNK_DIGEST,
        };
        let first_progress = storage
            .tail
            .append_chunk(
                &[AccountTailDelta::new(account_key, vec![promoted], vec![]).unwrap()],
                None,
                first_progress,
            )
            .unwrap();
        storage
            .tail
            .finish_generation(
                codec::TailGenerationProgress { eof: true, ..first_progress },
                codec::TailGenerationCommit {
                    layout_version: codec::TailLayoutVersion::MonolithicV1,
                    target_generation: 1,
                    operation_id: first_progress.operation_id,
                    source_partition_id: first_progress.source_partition_id,
                    source_manifest_digest: first_progress.source_manifest_digest,
                    previous_visible_generation: 0,
                    cutoff_utime: 1,
                    keep_tx_per_account: 1,
                    retention_policy_digest: first_progress.retention_policy_digest,
                    counters: first_counters,
                },
            )
            .unwrap();
        let second_counters = codec::TailGenerationCounters {
            processed_accounts: 1,
            retired_records: 1,
            retired_bytes: promoted_bytes,
            ..Default::default()
        };
        let second_progress = codec::TailGenerationProgress {
            target_generation: 2,
            operation_id: 0x402,
            source_partition_id: 8,
            source_manifest_digest: HashBytes([0x4f; 32]),
            retention_policy_digest: HashBytes([0x50; 32]),
            cursor: codec::TailProgressCursor::Account(account_key),
            eof: false,
            counters: second_counters,
            chunk_digest: codec::EMPTY_TAIL_CHUNK_DIGEST,
        };
        let second_progress = storage
            .tail
            .append_chunk(
                &[AccountTailDelta::new(account_key, vec![], vec![payload_key]).unwrap()],
                None,
                second_progress,
            )
            .unwrap();
        storage
            .tail
            .finish_generation(
                codec::TailGenerationProgress { eof: true, ..second_progress },
                codec::TailGenerationCommit {
                    layout_version: codec::TailLayoutVersion::MonolithicV1,
                    target_generation: 2,
                    operation_id: second_progress.operation_id,
                    source_partition_id: second_progress.source_partition_id,
                    source_manifest_digest: second_progress.source_manifest_digest,
                    previous_visible_generation: 1,
                    cutoff_utime: 2,
                    keep_tx_per_account: 1,
                    retention_policy_digest: second_progress.retention_policy_digest,
                    counters: second_counters,
                },
            )
            .unwrap();

        let snapshot_at_generation = |generation| {
            let base = {
                let mut manager = storage.partitions.lock();
                build_composite_snapshot(
                    &mut manager,
                    &storage.filter_registry,
                    &storage.tail,
                    frontier,
                )
                .unwrap()
            };
            let RpcSnapshot(inner, filter_bundles) = base;
            let mut inner = Arc::try_unwrap(inner).ok().unwrap();
            inner.tail = storage
                .tail
                .request_snapshot(codec::TailLayoutVersion::MonolithicV1, generation)
                .unwrap();
            RpcSnapshot(Arc::new(inner), filter_bundles)
        };
        let future = snapshot_at_generation(0);
        let dead = snapshot_at_generation(2);

        for snapshot in [&future, &dead] {
            assert_eq!(
                storage
                    .get_transaction(&transaction_hash, Some(snapshot))
                    .unwrap()
                    .unwrap()
                    .as_ref(),
                [0x4c],
            );
            assert_eq!(
                storage
                    .get_dst_transaction(&inbound_message_hash, Some(snapshot))
                    .unwrap()
                    .unwrap()
                    .as_ref(),
                [0x4c],
            );
            assert_eq!(
                storage
                    .get_src_transaction(&account, 11, Some(snapshot))
                    .unwrap()
                    .unwrap()
                    .as_ref(),
                [0x4c],
            );
            assert_eq!(
                storage
                    .get_transactions(&account, None, None, false, Some((*snapshot).clone()))
                    .unwrap()
                    .map_ext(|lt, _, boc| Some((lt, boc[0])))
                    .collect::<Vec<_>>(),
                [(10, 0x4c)],
            );
        }
    }

    #[tokio::test]
    async fn account_history_merges_live_and_tail_with_global_order_and_bounds() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = RpcStorage::open(
            context,
            RpcTransactionPartitionsConfig {
                target_transaction_lsm_bytes: u64::MAX,
                target_transaction_blob_bytes: u64::MAX,
                target_transaction_index_records: u64::MAX,
                target_block_metadata_bytes: u64::MAX,
                ..Default::default()
            },
        ).unwrap();
        let account = StdAddr::new(0, HashBytes([0x42; 32]));
        let mut account_key = [0; codec::ACCOUNT_KEY_LEN];
        account_key[0] = account.workchain as u8;
        account_key[1..].copy_from_slice(account.address.as_slice());
        let promote = |lt, hash_byte, message_byte, boc_byte| {
            let mut payload = Vec::with_capacity(66);
            payload.push(TransactionMask::HAS_MSG_HASH.bits());
            payload.extend_from_slice(&[hash_byte; 32]);
            payload.extend_from_slice(&[message_byte; 32]);
            payload.push(boc_byte);
            let value = codec::encode_transaction_value(7, &payload).unwrap();
            let bytes = value.len() as u64;
            (
                TailPromotedTransaction::new(
                    codec::tail_payload_key(account_key, lt),
                    value,
                    basechain_block(7),
                    1,
                ).unwrap(),
                bytes,
            )
        };
        let (tail10, bytes10) = promote(10, 0x81, 0x91, 0xa1);
        let (tail30, bytes30) = promote(30, 0x82, 0x92, 0xa2);
        let (tail_max, bytes_max) = promote(u64::MAX, 0x83, 0x93, 0xaf);
        let counters = codec::TailGenerationCounters {
            processed_accounts: 1,
            promoted_records: 3,
            promoted_bytes: bytes10 + bytes30 + bytes_max,
            ..Default::default()
        };
        let progress = codec::TailGenerationProgress {
            target_generation: 1,
            operation_id: 11,
            source_partition_id: 1,
            source_manifest_digest: HashBytes([0x72; 32]),
            retention_policy_digest: HashBytes([0x82; 32]),
            cursor: codec::TailProgressCursor::Account(account_key),
            eof: false,
            counters,
            chunk_digest: codec::EMPTY_TAIL_CHUNK_DIGEST,
        };
        let progress = storage.tail.append_chunk(
            &[AccountTailDelta::new(
                account_key,
                vec![tail10, tail30, tail_max],
                vec![],
            ).unwrap()],
            None,
            progress,
        ).unwrap();
        let terminal = codec::TailGenerationProgress { eof: true, ..progress };
        storage.tail.finish_generation(
            terminal,
            codec::TailGenerationCommit {
                layout_version: codec::TailLayoutVersion::MonolithicV1,
                target_generation: 1,
                operation_id: progress.operation_id,
                source_partition_id: progress.source_partition_id,
                source_manifest_digest: progress.source_manifest_digest,
                previous_visible_generation: 0,
                cutoff_utime: 10,
                keep_tx_per_account: 3,
                retention_policy_digest: progress.retention_policy_digest,
                counters,
            },
        ).unwrap();

        let frontier = masterchain_block(7);
        insert_read_test_block(
            &storage,
            &account,
            &frontier,
            &basechain_block(7),
            &[
                ReadTestTransaction {
                    lt: 20,
                    hash: HashBytes([0x20; 32]),
                    in_msg_hash: HashBytes([0x70; 32]),
                    boc_byte: 0x20,
                },
                ReadTestTransaction {
                    lt: 30,
                    hash: HashBytes([0x30; 32]),
                    in_msg_hash: HashBytes([0x71; 32]),
                    boc_byte: 0x30,
                },
                ReadTestTransaction {
                    lt: 40,
                    hash: HashBytes([0x40; 32]),
                    in_msg_hash: HashBytes([0x72; 32]),
                    boc_byte: 0x40,
                },
            ],
            0,
        );
        storage.commit_masterchain_block_set(&frontier).unwrap();
        let base = {
            let mut manager = storage.partitions.lock();
            build_composite_snapshot(
                &mut manager,
                &storage.filter_registry,
                &storage.tail,
                frontier,
            ).unwrap()
        };
        let RpcSnapshot(inner, filter_bundles) = base;
        let mut inner = Arc::try_unwrap(inner).ok().unwrap();
        inner.tail = storage.tail.request_snapshot(
            codec::TailLayoutVersion::MonolithicV1,
            1,
        ).unwrap();
        let snapshot = RpcSnapshot(Arc::new(inner), filter_bundles);

        let collect = |reverse, start_lt, end_lt| {
            storage
                .get_transactions(
                    &account,
                    start_lt,
                    end_lt,
                    reverse,
                    Some(snapshot.clone()),
                )
                .unwrap()
                .map_ext(|lt, _, boc| Some((lt, boc[0])))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            collect(false, None, None),
            [(10, 0xa1), (20, 0x20), (30, 0x30), (40, 0x40), (u64::MAX, 0xaf)],
        );
        assert_eq!(
            collect(true, None, None),
            [(u64::MAX, 0xaf), (40, 0x40), (30, 0x30), (20, 0x20), (10, 0xa1)],
        );
        assert_eq!(
            collect(false, Some(20), Some(40)),
            [(20, 0x20), (30, 0x30), (40, 0x40)],
        );
        assert_eq!(
            collect(false, Some(u64::MAX), Some(u64::MAX)),
            [(u64::MAX, 0xaf)],
        );
        assert!(collect(false, Some(40), Some(20)).is_empty());
    }

    #[tokio::test]
    async fn request_snapshots_switch_transaction_authority_from_source_to_tail() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = RpcStorage::open(context, test_partitions_config()).unwrap();
        storage.sealing_cancel.cancel();
        storage.filter_worker.shutdown();
        let account = StdAddr::new(0, HashBytes([0x43; 32]));
        let frontier = masterchain_block(1);
        let block_id = basechain_block(1);
        let transaction_hash = HashBytes([0x53; 32]);
        let inbound_message_hash = HashBytes([0x63; 32]);
        let source = insert_read_test_block(
            &storage,
            &account,
            &frontier,
            &block_id,
            &[ReadTestTransaction {
                lt: 10,
                hash: transaction_hash,
                in_msg_hash: inbound_message_hash,
                boc_byte: 0xb1,
            }],
            1,
        );
        storage.commit_masterchain_block_set(&frontier).unwrap();
        seal_partition(
            storage.partitions.clone(),
            storage.tail.clone(),
            storage.snapshots.clone(),
            storage.filter_registry.clone(),
            storage.maintenance.clone(),
            source,
            CancellationFlag::new(),
            None,
        )
        .await
        .unwrap();
        let old_snapshot = storage.load_snapshot().unwrap();
        assert_eq!(old_snapshot.tail_snapshot().visible_generation(), 0);
        assert_eq!(
            old_snapshot.descriptor(source).unwrap().lifecycle,
            codec::ManifestLifecycle::Sealed,
        );

        let mut account_key = [0; codec::ACCOUNT_KEY_LEN];
        account_key[0] = account.workchain as u8;
        account_key[1..].copy_from_slice(account.address.as_slice());
        let mut payload = Vec::with_capacity(66);
        payload.push(TransactionMask::HAS_MSG_HASH.bits());
        payload.extend_from_slice(transaction_hash.as_slice());
        payload.extend_from_slice(inbound_message_hash.as_slice());
        payload.push(0xb1);
        let value = codec::encode_transaction_value(frontier.seqno, &payload).unwrap();
        let counters = codec::TailGenerationCounters {
            processed_accounts: 1,
            promoted_records: 1,
            promoted_bytes: value.len() as u64,
            ..Default::default()
        };
        let manifest_digest = storage
            .partitions
            .lock()
            .sealed_manifest_digest(source)
            .unwrap();
        let promoted = TailPromotedTransaction::new(
            codec::tail_payload_key(account_key, 10),
            value,
            block_id,
            1,
        ).unwrap();
        let progress = codec::TailGenerationProgress {
            target_generation: 1,
            operation_id: 12,
            source_partition_id: source.0,
            source_manifest_digest: manifest_digest,
            retention_policy_digest: HashBytes([0x83; 32]),
            cursor: codec::TailProgressCursor::Account(account_key),
            eof: false,
            counters,
            chunk_digest: codec::EMPTY_TAIL_CHUNK_DIGEST,
        };
        let progress = storage.tail.append_chunk(
            &[AccountTailDelta::new(account_key, vec![promoted], vec![]).unwrap()],
            None,
            progress,
        ).unwrap();
        let terminal = codec::TailGenerationProgress { eof: true, ..progress };
        storage.tail.finish_generation(
            terminal,
            codec::TailGenerationCommit {
                layout_version: codec::TailLayoutVersion::MonolithicV1,
                target_generation: 1,
                operation_id: progress.operation_id,
                source_partition_id: progress.source_partition_id,
                source_manifest_digest: progress.source_manifest_digest,
                previous_visible_generation: 0,
                cutoff_utime: 10,
                keep_tx_per_account: 1,
                retention_policy_digest: progress.retention_policy_digest,
                counters,
            },
        ).unwrap();
        storage
            .partitions
            .lock()
            .retire_partition_for_snapshot_test(source)
            .unwrap();
        let base = {
            let mut manager = storage.partitions.lock();
            build_composite_snapshot(
                &mut manager,
                &storage.filter_registry,
                &storage.tail,
                frontier,
            ).unwrap()
        };
        let RpcSnapshot(inner, filter_bundles) = base;
        let mut inner = Arc::try_unwrap(inner).ok().unwrap();
        inner.tail = storage.tail.request_snapshot(
            codec::TailLayoutVersion::MonolithicV1,
            1,
        ).unwrap();
        let new_snapshot = RpcSnapshot(Arc::new(inner), filter_bundles);
        assert!(new_snapshot.descriptor(source).is_none());

        for snapshot in [&old_snapshot, &new_snapshot] {
            assert_eq!(
                storage
                    .get_transaction(&transaction_hash, Some(snapshot))
                    .unwrap()
                    .unwrap()
                    .as_ref(),
                [0xb1],
            );
            assert_eq!(
                storage
                    .get_dst_transaction(&inbound_message_hash, Some(snapshot))
                    .unwrap()
                    .unwrap()
                    .as_ref(),
                [0xb1],
            );
            assert_eq!(
                storage
                    .get_transactions(&account, None, None, false, Some(snapshot.clone()))
                    .unwrap()
                    .map_ext(|lt, _, _| Some(lt))
                    .collect::<Vec<_>>(),
                [10],
            );
        }
        assert!(storage
            .get_brief_block_info(&block_id.as_short_id(), Some(&old_snapshot))
            .unwrap()
            .is_some());
        assert!(storage
            .get_brief_block_info(&block_id.as_short_id(), Some(&new_snapshot))
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn account_markers_are_atomic_deduplicated_and_required_on_replay() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let db: RpcTransactionsDb = context.open_preconfigured("account-marker-test").unwrap();
        let account_key = [0x11; tables::Accounts::KEY_LEN];

        let mut blacklisted_batch = rocksdb::WriteBatch::default();
        let mut blacklisted_stats = BlockWriteStats::default();
        prepare_account_marker(
            &db,
            &mut blacklisted_batch,
            &account_key,
            false,
            true,
            &mut blacklisted_stats,
        ).unwrap();
        db.rocksdb().write_opt(blacklisted_batch, db.transactions.write_config()).unwrap();
        assert!(db.accounts.get(account_key).unwrap().is_none());
        assert_eq!(blacklisted_stats, BlockWriteStats::default());

        let mut batch = rocksdb::WriteBatch::default();
        let mut stats = BlockWriteStats::default();
        prepare_account_marker(&db, &mut batch, &account_key, true, true, &mut stats).unwrap();
        assert!(db.accounts.get(account_key).unwrap().is_none());
        db.rocksdb().write_opt(batch, db.transactions.write_config()).unwrap();
        assert!(db.accounts.get(account_key).unwrap().unwrap().is_empty());
        assert_eq!(stats.transaction_count, 0);
        assert_eq!(stats.index_record_count, 0);
        assert_eq!(stats.estimated_lsm_bytes, tables::Accounts::KEY_LEN as u64);

        let mut replay_batch = rocksdb::WriteBatch::default();
        let mut replay_stats = BlockWriteStats::default();
        prepare_account_marker(
            &db,
            &mut replay_batch,
            &account_key,
            true,
            false,
            &mut replay_stats,
        ).unwrap();
        assert_eq!(replay_stats.estimated_lsm_bytes, tables::Accounts::KEY_LEN as u64);

        let mut delete_batch = rocksdb::WriteBatch::default();
        delete_batch.delete_cf(&db.accounts.cf(), account_key);
        db.rocksdb().write_opt(delete_batch, db.transactions.write_config()).unwrap();
        assert!(prepare_account_marker(
            &db,
            &mut replay_batch,
            &account_key,
            true,
            false,
            &mut BlockWriteStats::default(),
        ).is_err());
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
    async fn open_rejects_unsafe_capacities_before_constructing_runtime_consumers() {
        for case in ["semaphore", "gc-chunk"] {
            let (context, _tmp) = StorageContext::new_temp().await.unwrap();
            let mut config = RpcTransactionPartitionsConfig::default();
            let expected = match case {
                "semaphore" => {
                    config.filters.max_concurrent_sealed_exact_lookups =
                        tokio::sync::Semaphore::MAX_PERMITS + 1;
                    "exceeds Tokio semaphore limit"
                }
                "gc-chunk" => {
                    config.maintenance.gc_accounts_per_chunk = 129;
                    "gc_accounts_per_chunk must not exceed 128"
                }
                _ => unreachable!(),
            };
            let error = match RpcStorage::open(context, config) {
                Ok(_) => panic!("unsafe {case} capacity unexpectedly opened RPC storage"),
                Err(error) => error,
            };
            assert!(format!("{error:#}").contains(expected));
        }
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
    async fn lifecycle_continuation_defers_persisted_sealing_until_maintenance_start() {
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

        let storage = Arc::new(RpcStorage::open_full(context, config, None).unwrap());
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
        tokio::time::sleep(Duration::from_millis(50)).await;
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
        storage
            .start_maintenance(&reconciliation.effective_frontier)
            .unwrap();
        wait_for_sealed_partition(&storage, sealing_id).await;
        tokio::time::timeout(
            Duration::from_secs(5),
            storage.gc.wait_for_startup_passes_completed(1),
        )
        .await
        .unwrap();
        assert_eq!(storage.gc.startup_passes_completed(), 1);
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
            context.clone(),
            RpcTransactionPartitionsConfig {
                target_transaction_lsm_bytes: 1,
                target_transaction_blob_bytes: 1,
                target_transaction_index_records: 1,
                max_open_sealed_partitions: 1,
                ..Default::default()
            },
        )
        .unwrap();
        manager.request_rotation(super::super::partition::PartitionCounters {
            estimated_transaction_lsm_bytes: 1,
            ..Default::default()
        });
        let old = manager.rotate_if_requested().unwrap().unwrap().0;
        let tail = open_test_tail(&context, &manager);
        let partitions = Arc::new(Mutex::new(manager));

        seal_partition(
            partitions.clone(),
            tail,
            Arc::new(SnapshotPublisher::default()),
            Arc::new(FilterRegistry::default()),
            Arc::new(MaintenanceCoordinator::new(1)),
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

    #[test]
    fn sealing_rebuild_diagnostics_preserve_pending_errors() {
        let acquire_error = anyhow::anyhow!("maintenance coordinator is closed").context(
            "failed to reacquire sealing maintenance permit for RPC composite snapshot rebuild",
        );
        let error = combine_sealing_snapshot_errors(
            Some(anyhow::anyhow!("injected seal failure")),
            combine_snapshot_retry_errors(
                anyhow::anyhow!("injected snapshot failure"),
                acquire_error,
            ),
        );
        let message = format!("{error:#}");
        assert!(message.contains("injected seal failure"));
        assert!(message.contains("injected snapshot failure"));
        assert!(message.contains("failed to reacquire sealing maintenance permit"));
        assert!(message.contains("maintenance coordinator is closed"));

        let pending = masterchain_block(1);
        let mut different = pending;
        different.file_hash = HashBytes([0xff; 32]);
        let identity_error = sealing_rebuild_frontier(Some(&different), pending).unwrap_err();
        let error = combine_sealing_snapshot_errors(
            Some(anyhow::anyhow!("injected seal failure")),
            combine_snapshot_retry_errors(
                anyhow::anyhow!("injected snapshot failure"),
                identity_error,
            ),
        );
        let message = format!("{error:#}");
        assert!(message.contains("injected seal failure"));
        assert!(message.contains("injected snapshot failure"));
        assert!(message.contains("different RPC snapshot frontier"));
    }

    #[tokio::test]
    async fn sealing_rebuild_retry_releases_snapshot_guard_and_maintenance_permit() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let mut manager = PartitionManager::open(context.clone(), test_partitions_config()).unwrap();
        manager.request_rotation(super::super::partition::PartitionCounters {
            estimated_transaction_lsm_bytes: 1,
            ..Default::default()
        });
        let old = manager.rotate_if_requested().unwrap().unwrap().0;
        let filter_registry = Arc::new(FilterRegistry::default());
        let frontier = masterchain_block(1);
        let tail = open_test_tail(&context, &manager);
        let initial = build_composite_snapshot(
            &mut manager,
            &filter_registry,
            &tail,
            frontier,
        ).unwrap();
        let partitions = Arc::new(Mutex::new(manager));
        let snapshots = Arc::new(SnapshotPublisher::default());
        *snapshots.current.write() = Some(initial);
        snapshots.rebuild_failures.store(1, Ordering::Release);
        let failure_gate = Arc::new(SealingRebuildFailureGate::default());
        *snapshots.rebuild_failure_gate.lock() = Some(failure_gate.clone());
        let maintenance = Arc::new(MaintenanceCoordinator::new(1));
        let (publisher_queued, publisher_queued_rx) = tokio::sync::oneshot::channel();
        let (publisher_acquired, publisher_acquired_rx) = tokio::sync::oneshot::channel();
        let (release_publisher, release_publisher_rx) = std::sync::mpsc::channel();
        let publisher_maintenance = maintenance.clone();
        let publisher_snapshots = snapshots.clone();
        let publisher = tokio::spawn(async move {
            publisher_snapshots.rebuild_failure_notify.notified().await;
            let permit = publisher_maintenance.acquire(MaintenancePriority::Background);
            tokio::pin!(permit);
            tokio::select! {
                biased;
                result = &mut permit => {
                    drop(result);
                    panic!("background maintenance acquired before sealing released its permit");
                }
                _ = tokio::task::yield_now() => {}
            }
            publisher_queued.send(()).unwrap();
            let permit = permit.await.unwrap();
            tokio::task::spawn_blocking(move || {
                let _permit = permit;
                let _published = publisher_snapshots.current.write();
                publisher_acquired.send(()).unwrap();
                release_publisher_rx
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap();
            })
            .await
            .unwrap();
        });
        let sealing = tokio::spawn(seal_partition(
            partitions.clone(),
            tail,
            snapshots.clone(),
            filter_registry,
            maintenance.clone(),
            old,
            CancellationFlag::new(),
            None,
        ));

        tokio::time::timeout(Duration::from_secs(5), publisher_queued_rx)
            .await
            .unwrap()
            .unwrap();
        failure_gate.release();
        tokio::time::timeout(Duration::from_secs(1), publisher_acquired_rx)
            .await
            .unwrap()
            .unwrap();
        release_publisher.send(()).unwrap();
        publisher.await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), sealing)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        assert_eq!(snapshots.rebuild_failures.load(Ordering::Acquire), 0);
        let published = snapshots.current.read();
        let published = published.as_ref().unwrap();
        assert_eq!(
            published.descriptor(old).unwrap().lifecycle,
            codec::ManifestLifecycle::Sealed,
        );
        assert!(!published.0.writable_partitions.contains_key(&old));
    }

    #[tokio::test]
    async fn retired_partition_is_lazy_opened_only_by_pre_cutover_snapshot() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let mut manager = PartitionManager::open(context.clone(), test_partitions_config()).unwrap();
        manager.request_rotation(super::super::partition::PartitionCounters {
            estimated_transaction_lsm_bytes: 1,
            ..Default::default()
        });
        let source = manager.rotate_if_requested().unwrap().unwrap().0;
        let filter_registry = Arc::new(FilterRegistry::default());
        let frontier = masterchain_block(1);
        let tail = open_test_tail(&context, &manager);
        let initial = build_composite_snapshot(
            &mut manager,
            &filter_registry,
            &tail,
            frontier,
        ).unwrap();
        let partitions = Arc::new(Mutex::new(manager));
        let snapshots = Arc::new(SnapshotPublisher::default());
        *snapshots.current.write() = Some(initial);
        let maintenance = Arc::new(MaintenanceCoordinator::new(1));
        seal_partition(
            partitions.clone(),
            tail.clone(),
            snapshots.clone(),
            filter_registry.clone(),
            maintenance,
            source,
            CancellationFlag::new(),
            None,
        )
        .await
        .unwrap();

        let pre_cutover = snapshots.current.read().as_ref().unwrap().clone();
        assert_eq!(
            pre_cutover.descriptor(source).unwrap().lifecycle,
            codec::ManifestLifecycle::Sealed,
        );
        {
            let mut manager = partitions.lock();
            manager.retire_partition_for_snapshot_test(source).unwrap();
            assert!(manager.sealed_lease_opener(source).is_err());
            assert!(!manager.deletion_references_drained(source).unwrap());
        }

        let post_cutover = {
            let mut manager = partitions.lock();
            build_composite_snapshot(
                &mut manager,
                &filter_registry,
                &tail,
                frontier,
            ).unwrap()
        };
        *snapshots.current.write() = Some(post_cutover.clone());
        assert!(post_cutover.descriptor(source).is_none());

        let old_read = acquire_partition_read(pre_cutover.clone(), source).unwrap();
        assert_eq!(old_read.lease.lifecycle(), codec::ManifestLifecycle::Sealed);
        assert!(acquire_partition_read(post_cutover, source).is_err());
        drop(old_read);
        assert!(!partitions.lock().deletion_references_drained(source).unwrap());
        drop(pre_cutover);
        assert!(partitions.lock().deletion_references_drained(source).unwrap());
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

        let sealing_drain = storage.snapshots.sealing_drain_notify.notified();
        storage.sealing_notify.notify_one();
        tokio::time::timeout(Duration::from_secs(5), sealing_drain)
            .await
            .unwrap();
        assert!(storage
            .partitions
            .lock()
            .descriptors()
            .iter()
            .any(|descriptor| descriptor.lifecycle == codec::ManifestLifecycle::Sealing));
        let background_permit = tokio::time::timeout(
            Duration::from_secs(1),
            storage.maintenance.acquire(MaintenancePriority::Background),
        )
        .await
        .unwrap()
        .unwrap();
        drop(background_permit);

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
        let sealed = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(sealed) = storage
                    .partitions
                    .lock()
                    .descriptors()
                    .iter()
                    .find(|descriptor| descriptor.lifecycle == codec::ManifestLifecycle::Sealed)
                    .map(|descriptor| descriptor.id)
                    && storage.filter_worker.pending_sealed_contains(sealed)
                {
                    break sealed;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(storage.filter_worker.pending_sealed_contains(sealed));

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
            PartitionManager::open(context.clone(), test_partitions_config()).unwrap();
        let block_id = masterchain_block(1);
        let old = manager.active_id();
        insert_masterchain_commit(&manager, &block_id, 1);
        manager.commit_masterchain_block_set(&block_id).unwrap();

        let worker = manager.begin_sealing(old).unwrap();
        drop(worker);
        let primary = manager.take_sealing_handle(old).unwrap();
        drop(primary);
        assert_eq!(manager.sealing_handle_strong_count(old), None);

        let tail = open_test_tail(&context, &manager);
        let snapshot = build_composite_snapshot(
            &mut manager,
            &FilterRegistry::default(),
            &tail,
            block_id,
        ).unwrap();
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
    async fn sealed_rpc_ahead_point_reads_stay_invisible_until_replay() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let config = gc_test_config(128, 16 * 1024 * 1024);
        let account = gc_account(0xb1);
        let first_transaction = gc_transaction(10, 0xb2);
        let future_transaction = gc_transaction(20, 0xb3);
        let first = masterchain_block(1);
        let second = masterchain_block(2);
        let source = {
            let storage = RpcStorage::open(context.clone(), config.clone()).unwrap();
            storage.sealing_cancel.cancel();
            storage.filter_worker.shutdown();
            let source = insert_read_test_block(
                &storage,
                &account,
                &first,
                &basechain_block(1),
                std::slice::from_ref(&first_transaction),
                0,
            );
            storage.commit_masterchain_block_set(&first).unwrap();
            assert_eq!(
                insert_read_test_block(
                    &storage,
                    &account,
                    &second,
                    &basechain_block(2),
                    std::slice::from_ref(&future_transaction),
                    1,
                ),
                source,
            );
            storage.commit_masterchain_block_set(&second).unwrap();
            seal_gc_partition(&storage, source).await;
            let descriptor = storage
                .partitions
                .lock()
                .descriptors()
                .into_iter()
                .find(|descriptor| descriptor.id == source)
                .unwrap();
            assert_eq!(descriptor.lifecycle, codec::ManifestLifecycle::Sealed);
            assert_eq!(descriptor.first.mc_seqno, first.seqno);
            assert_eq!(descriptor.last.mc_seqno, second.seqno);
            source
        };
        tokio::task::yield_now().await;

        let storage = RpcStorage::open(context, config).unwrap();
        let reconciliation = storage.reconcile_startup(&first).unwrap();
        assert!(reconciliation.rebuild_current_state);
        storage.continue_lifecycle().unwrap();
        storage.publish_snapshot(&reconciliation.effective_frontier).unwrap();
        let initial = storage.load_snapshot().unwrap();
        assert_eq!(initial.visible_frontier(), &first);
        assert_eq!(
            initial.descriptor(source).unwrap().lifecycle,
            codec::ManifestLifecycle::Sealed,
        );
        assert_eq!(
            storage
                .get_transaction(&first_transaction.hash, Some(&initial))
                .unwrap()
                .unwrap()
                .as_ref(),
            [first_transaction.boc_byte],
        );
        assert!(storage
            .get_transaction(&future_transaction.hash, Some(&initial))
            .unwrap()
            .is_none());
        assert!(storage
            .get_dst_transaction(&future_transaction.in_msg_hash, Some(&initial))
            .unwrap()
            .is_none());
        assert!(!storage.is_resync_required());
        assert!(storage.load_snapshot().is_some());

        assert_eq!(storage.admit_block_set(&second).unwrap(), BlockSetMode::Replay);
        storage
            .commit_masterchain_block_set_with_predecessor(&second, &first)
            .unwrap();
        let replayed = storage.load_snapshot().unwrap();
        assert_eq!(replayed.visible_frontier(), &second);
        assert_eq!(
            storage
                .get_transaction(&future_transaction.hash, Some(&replayed))
                .unwrap()
                .unwrap()
                .as_ref(),
            [future_transaction.boc_byte],
        );
        assert_eq!(
            storage
                .get_dst_transaction(&future_transaction.in_msg_hash, Some(&replayed))
                .unwrap()
                .unwrap()
                .as_ref(),
            [future_transaction.boc_byte],
        );
        assert!(!storage.is_resync_required());
        assert_eq!(storage.gc.resync_transitions(), 0);
    }

    async fn prepare_sealed_out_of_range_point_read_fixture(
        context: &StorageContext,
        config: &RpcTransactionPartitionsConfig,
        corrupt: impl FnOnce(&PartitionReadLease, &StdAddr, &ReadTestTransaction),
    ) -> (ReadTestTransaction, BlockId, PartitionId) {
        let account = gc_account(0xb7);
        let first_transaction = gc_transaction(10, 0xb8);
        let future_transaction = gc_transaction(20, 0xb9);
        let first = masterchain_block(1);
        let second = masterchain_block(2);
        let storage = RpcStorage::open(context.clone(), config.clone()).unwrap();
        storage.sealing_cancel.cancel();
        storage.filter_worker.shutdown();
        let source = insert_read_test_block(
            &storage,
            &account,
            &first,
            &basechain_block(1),
            std::slice::from_ref(&first_transaction),
            0,
        );
        storage.commit_masterchain_block_set(&first).unwrap();
        assert_eq!(
            insert_read_test_block(
                &storage,
                &account,
                &second,
                &basechain_block(2),
                std::slice::from_ref(&future_transaction),
                1,
            ),
            source,
        );
        let lease = storage.partitions.lock().active_lease();
        corrupt(&lease, &account, &future_transaction);
        drop(lease);
        storage.commit_masterchain_block_set(&second).unwrap();
        seal_gc_partition(&storage, source).await;
        let descriptor = storage
            .partitions
            .lock()
            .descriptors()
            .into_iter()
            .find(|descriptor| descriptor.id == source)
            .unwrap();
        assert_eq!(descriptor.lifecycle, codec::ManifestLifecycle::Sealed);
        assert_eq!(descriptor.first.mc_seqno, first.seqno);
        assert_eq!(descriptor.last.mc_seqno, second.seqno);
        (future_transaction, first, source)
    }

    #[tokio::test]
    async fn sealed_hash_locator_beyond_descriptor_marks_resync_once() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let config = gc_test_config(128, 16 * 1024 * 1024);
        let (future_transaction, first, source) = prepare_sealed_out_of_range_point_read_fixture(
            &context,
            &config,
            |lease, _, transaction| {
                let mut locator = lease
                    .transactions_by_hash
                    .get(transaction.hash)
                    .unwrap()
                    .unwrap()
                    .as_ref()
                    .to_vec();
                locator[110..114].copy_from_slice(&3u32.to_le_bytes());
                lease
                    .transactions_by_hash
                    .insert(transaction.hash, locator)
                    .unwrap();
            },
        ).await;
        tokio::task::yield_now().await;

        let storage = RpcStorage::open(context, config).unwrap();
        let reconciliation = storage.reconcile_startup(&first).unwrap();
        assert!(reconciliation.rebuild_current_state);
        storage.continue_lifecycle().unwrap();
        storage.publish_snapshot(&reconciliation.effective_frontier).unwrap();
        let initial = storage.load_snapshot().unwrap();
        assert_eq!(initial.visible_frontier(), &first);
        assert_eq!(
            initial.descriptor(source).unwrap().lifecycle,
            codec::ManifestLifecycle::Sealed,
        );

        let error = match storage.get_transaction(&future_transaction.hash, Some(&initial)) {
            Ok(_) => panic!("out-of-range committed sealed hash locator must fail"),
            Err(error) => error,
        };
        assert_eq!(
            classify_authoritative_error(&error),
            Some(AuthoritativeErrorKind::ConflictingCommittedData),
        );
        assert!(storage.is_resync_required());
        assert!(storage.load_snapshot().is_none());
        assert_eq!(storage.gc.resync_transitions(), 1);
        assert!(storage
            .get_transaction(&future_transaction.hash, Some(&initial))
            .is_err());
        assert_eq!(storage.gc.resync_transitions(), 1);
    }

    #[tokio::test]
    async fn sealed_inbound_transaction_beyond_descriptor_marks_resync_once() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let config = gc_test_config(128, 16 * 1024 * 1024);
        let (future_transaction, first, source) = prepare_sealed_out_of_range_point_read_fixture(
            &context,
            &config,
            |lease, account, transaction| {
                let mut transaction_key = [0; tables::Transactions::KEY_LEN];
                transaction_key[..codec::ACCOUNT_KEY_LEN]
                    .copy_from_slice(&gc_account_key(account));
                transaction_key[codec::ACCOUNT_KEY_LEN..]
                    .copy_from_slice(&transaction.lt.to_be_bytes());
                let mut value = lease
                    .transactions
                    .get(transaction_key)
                    .unwrap()
                    .unwrap()
                    .as_ref()
                    .to_vec();
                value[..codec::TRANSACTION_VALUE_PREFIX_LEN]
                    .copy_from_slice(&3u32.to_be_bytes());
                lease.transactions.insert(transaction_key, value).unwrap();
            },
        ).await;
        tokio::task::yield_now().await;

        let storage = RpcStorage::open(context, config).unwrap();
        let reconciliation = storage.reconcile_startup(&first).unwrap();
        assert!(reconciliation.rebuild_current_state);
        storage.continue_lifecycle().unwrap();
        storage.publish_snapshot(&reconciliation.effective_frontier).unwrap();
        let initial = storage.load_snapshot().unwrap();
        assert_eq!(initial.visible_frontier(), &first);
        assert_eq!(
            initial.descriptor(source).unwrap().lifecycle,
            codec::ManifestLifecycle::Sealed,
        );

        let error = match storage.get_dst_transaction(&future_transaction.in_msg_hash, Some(&initial)) {
            Ok(_) => panic!("out-of-range committed sealed inbound transaction must fail"),
            Err(error) => error,
        };
        assert_eq!(
            classify_authoritative_error(&error),
            Some(AuthoritativeErrorKind::ConflictingCommittedData),
        );
        assert!(storage.is_resync_required());
        assert!(storage.load_snapshot().is_none());
        assert_eq!(storage.gc.resync_transitions(), 1);
        assert!(storage
            .get_dst_transaction(&future_transaction.in_msg_hash, Some(&initial))
            .is_err());
        assert_eq!(storage.gc.resync_transitions(), 1);
    }

    async fn prepare_sealed_under_range_point_read_fixture(
        context: &StorageContext,
        config: &RpcTransactionPartitionsConfig,
        corrupt: impl FnOnce(&PartitionReadLease, &StdAddr, &ReadTestTransaction),
    ) -> (ReadTestTransaction, BlockId, PartitionId) {
        let account = gc_account(0xba);
        let first_transaction = gc_transaction(20, 0xbb);
        let future_transaction = gc_transaction(30, 0xbc);
        let first = masterchain_block(2);
        let second = masterchain_block(3);
        let storage = RpcStorage::open(context.clone(), config.clone()).unwrap();
        storage.sealing_cancel.cancel();
        storage.filter_worker.shutdown();
        let source = insert_read_test_block(
            &storage,
            &account,
            &first,
            &basechain_block(2),
            std::slice::from_ref(&first_transaction),
            0,
        );
        storage.commit_masterchain_block_set(&first).unwrap();
        assert_eq!(
            insert_read_test_block(
                &storage,
                &account,
                &second,
                &basechain_block(3),
                std::slice::from_ref(&future_transaction),
                1,
            ),
            source,
        );
        let lease = storage.partitions.lock().active_lease();
        corrupt(&lease, &account, &future_transaction);
        drop(lease);
        storage.commit_masterchain_block_set(&second).unwrap();
        seal_gc_partition(&storage, source).await;
        let descriptor = storage
            .partitions
            .lock()
            .descriptors()
            .into_iter()
            .find(|descriptor| descriptor.id == source)
            .unwrap();
        assert_eq!(descriptor.lifecycle, codec::ManifestLifecycle::Sealed);
        assert_eq!(descriptor.first.mc_seqno, first.seqno);
        assert_eq!(descriptor.last.mc_seqno, second.seqno);
        (future_transaction, first, source)
    }

    #[tokio::test]
    async fn sealed_hash_locator_before_descriptor_marks_resync_once() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let config = gc_test_config(128, 16 * 1024 * 1024);
        let (future_transaction, first, source) = prepare_sealed_under_range_point_read_fixture(
            &context,
            &config,
            |lease, _, transaction| {
                let mut locator = lease
                    .transactions_by_hash
                    .get(transaction.hash)
                    .unwrap()
                    .unwrap()
                    .as_ref()
                    .to_vec();
                locator[110..114].copy_from_slice(&1u32.to_le_bytes());
                lease
                    .transactions_by_hash
                    .insert(transaction.hash, locator)
                    .unwrap();
            },
        ).await;
        tokio::task::yield_now().await;

        let storage = RpcStorage::open(context, config).unwrap();
        let reconciliation = storage.reconcile_startup(&first).unwrap();
        assert!(reconciliation.rebuild_current_state);
        storage.continue_lifecycle().unwrap();
        storage.publish_snapshot(&reconciliation.effective_frontier).unwrap();
        let initial = storage.load_snapshot().unwrap();
        assert_eq!(initial.visible_frontier(), &first);
        assert_eq!(
            initial.descriptor(source).unwrap().lifecycle,
            codec::ManifestLifecycle::Sealed,
        );

        let error = match storage.get_transaction(&future_transaction.hash, Some(&initial)) {
            Ok(_) => panic!("under-range committed sealed hash locator must fail"),
            Err(error) => error,
        };
        assert_eq!(
            classify_authoritative_error(&error),
            Some(AuthoritativeErrorKind::ConflictingCommittedData),
        );
        assert!(storage.is_resync_required());
        assert!(storage.load_snapshot().is_none());
        assert_eq!(storage.gc.resync_transitions(), 1);
        assert!(storage
            .get_transaction(&future_transaction.hash, Some(&initial))
            .is_err());
        assert_eq!(storage.gc.resync_transitions(), 1);
    }

    #[tokio::test]
    async fn sealed_inbound_transaction_before_descriptor_marks_resync_once() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let config = gc_test_config(128, 16 * 1024 * 1024);
        let (future_transaction, first, source) = prepare_sealed_under_range_point_read_fixture(
            &context,
            &config,
            |lease, account, transaction| {
                let mut transaction_key = [0; tables::Transactions::KEY_LEN];
                transaction_key[..codec::ACCOUNT_KEY_LEN]
                    .copy_from_slice(&gc_account_key(account));
                transaction_key[codec::ACCOUNT_KEY_LEN..]
                    .copy_from_slice(&transaction.lt.to_be_bytes());
                let mut value = lease
                    .transactions
                    .get(transaction_key)
                    .unwrap()
                    .unwrap()
                    .as_ref()
                    .to_vec();
                value[..codec::TRANSACTION_VALUE_PREFIX_LEN]
                    .copy_from_slice(&1u32.to_be_bytes());
                lease.transactions.insert(transaction_key, value).unwrap();
            },
        ).await;
        tokio::task::yield_now().await;

        let storage = RpcStorage::open(context, config).unwrap();
        let reconciliation = storage.reconcile_startup(&first).unwrap();
        assert!(reconciliation.rebuild_current_state);
        storage.continue_lifecycle().unwrap();
        storage.publish_snapshot(&reconciliation.effective_frontier).unwrap();
        let initial = storage.load_snapshot().unwrap();
        assert_eq!(initial.visible_frontier(), &first);
        assert_eq!(
            initial.descriptor(source).unwrap().lifecycle,
            codec::ManifestLifecycle::Sealed,
        );

        let error = match storage.get_dst_transaction(&future_transaction.in_msg_hash, Some(&initial)) {
            Ok(_) => panic!("under-range committed sealed inbound transaction must fail"),
            Err(error) => error,
        };
        assert_eq!(
            classify_authoritative_error(&error),
            Some(AuthoritativeErrorKind::ConflictingCommittedData),
        );
        assert!(storage.is_resync_required());
        assert!(storage.load_snapshot().is_none());
        assert_eq!(storage.gc.resync_transitions(), 1);
        assert!(storage
            .get_dst_transaction(&future_transaction.in_msg_hash, Some(&initial))
            .is_err());
        assert_eq!(storage.gc.resync_transitions(), 1);
    }

    #[tokio::test]
    async fn replay_boundary_missing_sealed_partition_marks_resync() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let config = gc_test_config(128, 16 * 1024 * 1024);
        let account = gc_account(0xb4);
        let first_transaction = gc_transaction(10, 0xb5);
        let future_transaction = gc_transaction(20, 0xb6);
        let first = masterchain_block(1);
        let second = masterchain_block(2);
        let source = {
            let storage = RpcStorage::open(context.clone(), config.clone()).unwrap();
            storage.sealing_cancel.cancel();
            storage.filter_worker.shutdown();
            let source = insert_read_test_block(
                &storage,
                &account,
                &first,
                &basechain_block(1),
                std::slice::from_ref(&first_transaction),
                0,
            );
            storage.commit_masterchain_block_set(&first).unwrap();
            insert_read_test_block(
                &storage,
                &account,
                &second,
                &basechain_block(2),
                std::slice::from_ref(&future_transaction),
                1,
            );
            storage.commit_masterchain_block_set(&second).unwrap();
            seal_gc_partition(&storage, source).await;
            source
        };
        tokio::task::yield_now().await;

        let storage = RpcStorage::open(context.clone(), config).unwrap();
        storage.sealing_cancel.cancel();
        storage.filter_worker.shutdown();
        let reconciliation = storage.reconcile_startup(&first).unwrap();
        storage.continue_lifecycle().unwrap();
        storage.publish_snapshot(&reconciliation.effective_frontier).unwrap();
        assert_eq!(storage.admit_block_set(&second).unwrap(), BlockSetMode::Replay);
        storage
            .partitions
            .lock()
            .invalidate_sealed_cache_for_test(source);
        let source_path = context
            .root_dir()
            .path()
            .join("rpc/transactions")
            .join(source.directory_name());
        std::fs::remove_dir_all(source_path).unwrap();

        let error = storage
            .commit_masterchain_block_set_with_predecessor(&second, &first)
            .unwrap_err();
        assert_eq!(
            classify_authoritative_error(&error),
            Some(AuthoritativeErrorKind::MissingCommittedData),
        );
        assert!(storage.is_resync_required());
        assert!(storage.load_snapshot().is_none());
        assert_eq!(storage.gc.resync_transitions(), 1);
    }

    #[tokio::test]
    async fn replay_update_missing_sealed_partition_marks_resync() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let config = gc_test_config(128, 16 * 1024 * 1024);
        let account = gc_account(0xb7);
        let first = masterchain_block(1);
        let second = masterchain_block(2);
        let source = {
            let storage = RpcStorage::open(context.clone(), config.clone()).unwrap();
            storage.sealing_cancel.cancel();
            storage.filter_worker.shutdown();
            let source = insert_read_test_block(
                &storage,
                &account,
                &first,
                &basechain_block(1),
                &[gc_transaction(10, 0xb8)],
                0,
            );
            storage.commit_masterchain_block_set(&first).unwrap();
            insert_read_test_block(
                &storage,
                &account,
                &second,
                &basechain_block(2),
                &[gc_transaction(20, 0xb9)],
                1,
            );
            storage.commit_masterchain_block_set(&second).unwrap();
            seal_gc_partition(&storage, source).await;
            source
        };
        tokio::task::yield_now().await;

        let storage = RpcStorage::open(context.clone(), config).unwrap();
        storage.sealing_cancel.cancel();
        storage.filter_worker.shutdown();
        let reconciliation = storage.reconcile_startup(&first).unwrap();
        storage.continue_lifecycle().unwrap();
        storage.publish_snapshot(&reconciliation.effective_frontier).unwrap();
        assert_eq!(storage.admit_block_set(&second).unwrap(), BlockSetMode::Replay);
        storage
            .partitions
            .lock()
            .invalidate_sealed_cache_for_test(source);
        let source_path = context
            .root_dir()
            .path()
            .join("rpc/transactions")
            .join(source.directory_name());
        std::fs::remove_dir_all(source_path).unwrap();
        let subscriptions = super::super::subscriptions::RpcSubscriptions::new(
            SubscriberManagerConfig::new(1, 1),
            1,
        );

        let error = storage
            .update(&second, replay_test_block(), None, &subscriptions)
            .await
            .unwrap_err();
        assert_eq!(
            classify_authoritative_error(&error),
            Some(AuthoritativeErrorKind::MissingCommittedData),
        );
        assert!(storage.is_resync_required());
        assert!(storage.load_snapshot().is_none());
        assert_eq!(storage.gc.resync_transitions(), 1);
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
    async fn replay_rejects_each_persisted_partition_counter_mismatch_without_changing_aggregate() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = RpcStorage::open(context, RpcTransactionPartitionsConfig::default()).unwrap();
        let zerostate = masterchain_block(0);
        let masterchain = masterchain_block(1);
        let block = replay_test_block();
        let block_id = *block.id();
        let subscriptions = super::super::subscriptions::RpcSubscriptions::new(
            SubscriberManagerConfig::new(1, 1),
            1,
        );
        publish_test_frontier(&storage, &zerostate);

        let initial = storage
            .update(&masterchain, block.clone(), None, &subscriptions)
            .await
            .unwrap();
        assert!(initial.newly_committed);
        let partition_id = PartitionId(initial.partition_id);
        insert_masterchain_commit(&storage.partitions.lock(), &masterchain, 0);
        storage
            .commit_masterchain_block_set_with_predecessor(&masterchain, &zerostate)
            .unwrap();
        let durable_state = persisted_partition_aggregate_and_control(&storage, partition_id);
        let lease = storage.partitions.lock().read_lease(partition_id).unwrap();
        let commit_key = codec::partition_commit_key(masterchain.seqno, &block_id.as_short_id());
        let original = codec::decode_partition_commit(
            lease
                .partition_commits
                .get(commit_key)
                .unwrap()
                .unwrap()
                .as_ref(),
        )
        .unwrap();

        for field in [
            "transaction_count",
            "transaction_index_record_count",
            "estimated_transaction_lsm_bytes",
            "estimated_transaction_blob_bytes",
            "estimated_block_metadata_bytes",
        ] {
            let mut corrupted = original;
            match field {
                "transaction_count" => corrupted.transaction_count += 1,
                "transaction_index_record_count" => {
                    corrupted.transaction_index_record_count += 1
                }
                "estimated_transaction_lsm_bytes" => {
                    corrupted.estimated_transaction_lsm_bytes += 1
                }
                "estimated_transaction_blob_bytes" => {
                    corrupted.estimated_transaction_blob_bytes += 1
                }
                "estimated_block_metadata_bytes" => {
                    corrupted.estimated_block_metadata_bytes += 1
                }
                _ => unreachable!(),
            }
            lease
                .partition_commits
                .insert(commit_key, codec::encode_partition_commit(&corrupted))
                .unwrap();

            let error = storage
                .update(&masterchain, block.clone(), None, &subscriptions)
                .await
                .unwrap_err();
            assert!(
                format!("{error:#}").contains("partition commit marker statistics mismatch"),
                "field {field} must be rejected by replay statistics validation",
            );
            assert_eq!(
                persisted_partition_aggregate_and_control(&storage, partition_id),
                durable_state,
                "field {field} must not change the durable partition aggregate or control state",
            );
        }

        lease
            .partition_commits
            .insert(commit_key, codec::encode_partition_commit(&original))
            .unwrap();
        let replay = storage
            .update(&masterchain, block, None, &subscriptions)
            .await
            .unwrap();
        assert!(!replay.newly_committed);
        assert_eq!(replay.partition_id, initial.partition_id);
        assert_eq!(replay.stats, initial.stats);
        assert_eq!(
            persisted_partition_aggregate_and_control(&storage, partition_id),
            durable_state,
        );
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
                target_transaction_lsm_bytes: 1,
                target_transaction_blob_bytes: u64::MAX,
                target_transaction_index_records: u64::MAX,
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
    async fn v3_partitioned_acceptance_fixture_builds_recovers_and_measures_filters() {
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
                storage.tail.clone(),
                storage.snapshots.clone(),
                storage.filter_registry.clone(),
                storage.maintenance.clone(),
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
            "VT17 V3 fixture: sealed=3 active=1 sealed_cache_capacity={sealed_cache_capacity} sealed_cache_entries={direct_cache_entries} exact_lookup_limit={} resident_bytes={resident_bytes} persisted_bytes={persisted_bytes} build_ms={} validation_ms={} filtered_first_us={} filtered_warm_us={} filtered_100k_ms={} filter_checks={filter_checks} partitions_traversed=4/request false_positives={false_positives} observed_rate={observed_rate:.9} confidence_margin={confidence_margin:.9} filtered_sealed_opens={filtered_sealed_opens} direct_cold_us={} direct_warm_us={}",
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
                target_transaction_lsm_bytes: 1,
                target_transaction_blob_bytes: u64::MAX,
                target_transaction_index_records: u64::MAX,
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
        assert!(!storage.is_resync_required());
        assert_eq!(storage.gc.resync_transitions(), 0);
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
            first_filtered.tail_snapshot().layout_version(),
            local_only.tail_snapshot().layout_version(),
        );
        assert_eq!(
            first_filtered.tail_snapshot().visible_generation(),
            local_only.tail_snapshot().visible_generation(),
        );
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
            storage.tail.clone(),
            storage.snapshots.clone(),
            storage.filter_registry.clone(),
            storage.maintenance.clone(),
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
                sealing_storage.tail.clone(),
                sealing_storage.snapshots.clone(),
                sealing_storage.filter_registry.clone(),
                sealing_storage.maintenance.clone(),
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

            let mut account = SealedExactLookupContext::new(
                Arc::new(Semaphore::new(1)),
                Arc::new(SealedExactLookupMetrics::default()),
                FilterNamespace::Accounts,
                Arc::new(AtomicU64::new(0)),
            );
            account.record_filter(false);
            account.record_filter(true);
            account.record_false_positive();
            account.record_miss();
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
            recorder.gauge("tycho_storage_rpc_filter_observed_error_ratio|namespace=accounts"),
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
            target_transaction_lsm_bytes: 1,
            target_transaction_blob_bytes: u64::MAX,
            target_transaction_index_records: u64::MAX,
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
