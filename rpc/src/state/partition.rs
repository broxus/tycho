use std::collections::{BTreeMap, BTreeSet};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use moka::sync::Cache;
use tycho_storage::StorageContext;
use tycho_storage::kv::{InstanceId, NamedTables};
use weedb::rocksdb;

use crate::config::RpcTransactionPartitionsConfig;

use super::codec::{
    self, ControlState, ManifestBound, ManifestLifecycle, ManifestTransition, PartitionManifest,
};
use super::db::{
    RpcControlDb, RpcCurrentStateDb, RpcRouterDb, RpcTransactionsDb, RpcTransactionsTables,
};

const RPC_ROOT: &str = "rpc";
const CONTROL_SUBDIR: &str = "rpc/control";
const ROUTER_SUBDIR: &str = "rpc/router";
const CURRENT_STATE_SUBDIR: &str = "rpc/current-state";
const TRANSACTIONS_SUBDIR: &str = "rpc/transactions";
const MAX_PARTITION_ID: u64 = 9_999_999_999_999_999;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PartitionId(pub u64);

impl PartitionId {
    pub const FIRST: Self = Self(1);

    pub fn directory_name(self) -> String {
        format!("{:016}", self.0)
    }

    pub fn parse_directory_name(name: &str) -> Result<Self> {
        ensure!(name.len() == 16 && name.bytes().all(|byte| byte.is_ascii_digit()), "invalid transaction partition directory name: {name}; expected a 16-digit decimal partition id");
        name.parse::<u64>()
            .map(Self)
            .map_err(|_| anyhow::anyhow!("invalid transaction partition directory name: {name}"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RotationReason {
    EstimatedLsmBytes,
    EstimatedBlobBytes,
    IndexRecordCount,
}

impl RotationReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::EstimatedLsmBytes => "estimated_lsm_bytes",
            Self::EstimatedBlobBytes => "estimated_blob_bytes",
            Self::IndexRecordCount => "index_record_count",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PartitionCounters {
    pub estimated_lsm_bytes: u64,
    pub estimated_blob_bytes: u64,
    pub transaction_count: u64,
    pub index_record_count: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct PartitionManifestMetrics {
    active_id: u64,
    creating_count: u64,
    active_count: u64,
    sealing_count: u64,
    sealed_count: u64,
    active_counters: PartitionCounters,
    manifest_epoch: u64,
    visible_mc_seqno: u32,
}

fn record_rotation_result(reason: RotationReason, elapsed: Duration, success: bool) {
    metrics::histogram!("tycho_storage_rpc_partition_rotation_time").record(elapsed);
    if success {
        metrics::counter!(
            "tycho_storage_rpc_partition_rotations_total",
            "reason" => reason.as_str(),
        )
        .increment(1);
    } else {
        metrics::counter!("tycho_storage_rpc_partition_rotation_failures_total").increment(1);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionDescriptor {
    pub id: PartitionId,
    pub lifecycle: ManifestLifecycle,
    pub first: ManifestBound,
    pub last: ManifestBound,
    pub counters: PartitionCounters,
    pub last_transition: ManifestTransition,
}

impl PartitionDescriptor {
    fn from_manifest(value: PartitionManifest) -> Self {
        Self {
            id: PartitionId(value.partition_id),
            lifecycle: value.lifecycle,
            first: value.first,
            last: value.last,
            counters: PartitionCounters {
                estimated_lsm_bytes: value.estimated_lsm_bytes,
                estimated_blob_bytes: value.estimated_blob_bytes,
                transaction_count: value.transaction_count,
                index_record_count: value.index_record_count,
            },
            last_transition: value.last_transition,
        }
    }

    fn to_manifest(&self) -> PartitionManifest {
        PartitionManifest {
            partition_id: self.id.0,
            lifecycle: self.lifecycle,
            first: self.first,
            last: self.last,
            transaction_count: self.counters.transaction_count,
            estimated_lsm_bytes: self.counters.estimated_lsm_bytes,
            estimated_blob_bytes: self.counters.estimated_blob_bytes,
            index_record_count: self.counters.index_record_count,
            last_transition: self.last_transition,
        }
    }

    fn contains_mc_seqno(&self, mc_seqno: u32) -> bool {
        match (self.first.block_id, self.last.block_id) {
            (Some(_), Some(_)) => self.first.mc_seqno <= mc_seqno && mc_seqno <= self.last.mc_seqno,
            _ => false,
        }
    }
}

/// Owns an `Arc` DB handle so an iterator remains valid after cache eviction or sealing starts.
#[derive(Clone)]
pub struct PartitionReadLease {
    db: Arc<RpcTransactionsDb>,
    lifecycle: ManifestLifecycle,
}

impl PartitionReadLease {
    pub fn db(&self) -> &RpcTransactionsDb {
        &self.db
    }

    pub fn lifecycle(&self) -> ManifestLifecycle {
        self.lifecycle
    }
}

impl Deref for PartitionReadLease {
    type Target = RpcTransactionsDb;

    fn deref(&self) -> &Self::Target {
        self.db()
    }
}

/// Opens an immutable partition after the manager lock has been released.
pub struct SealedPartitionLeaseOpener {
    id: PartitionId,
    context: StorageContext,
    subdir: PathBuf,
    cache: Cache<PartitionId, Arc<RpcTransactionsDb>>,
}

impl SealedPartitionLeaseOpener {
    pub fn open(self) -> Result<PartitionReadLease> {
        let db = match self.cache.get(&self.id) {
            Some(db) => {
                metrics::counter!("tycho_storage_rpc_partition_sealed_cache_hits_total")
                    .increment(1);
                db
            }
            None => {
                metrics::counter!("tycho_storage_rpc_partition_sealed_cache_misses_total")
                    .increment(1);
                let started_at = Instant::now();
                let result = self.context.open_read_only(self.subdir);
                metrics::histogram!("tycho_storage_rpc_partition_sealed_cache_open_time")
                    .record(started_at.elapsed());
                let db: Arc<RpcTransactionsDb> = Arc::new(result?);
                metrics::counter!("tycho_storage_rpc_partition_sealed_cache_opens_total")
                    .increment(1);
                self.cache.insert(self.id, db.clone());
                db
            }
        };
        Ok(PartitionReadLease {
            db,
            lifecycle: ManifestLifecycle::Sealed,
        })
    }
}

pub struct PartitionManager {
    context: StorageContext,
    config: RpcTransactionPartitionsConfig,
    root: PathBuf,
    control: RpcControlDb,
    router: RpcRouterDb,
    current_state: RpcCurrentStateDb,
    control_state: ControlState,
    visible_frontier: Option<tycho_types::models::BlockId>,
    manifest_epoch: u64,
    descriptors: BTreeMap<PartitionId, PartitionDescriptor>,
    active: Option<Arc<RpcTransactionsDb>>,
    active_id: PartitionId,
    sealing: BTreeMap<PartitionId, Arc<RpcTransactionsDb>>,
    closing: BTreeSet<PartitionId>,
    sealed_cache: Cache<PartitionId, Arc<RpcTransactionsDb>>,
    rotation_requested: Option<RotationReason>,
    sealer_busy: bool,
}

impl PartitionManager {
    pub fn open(context: StorageContext, config: RpcTransactionPartitionsConfig) -> Result<Self> {
        config.validate().map_err(anyhow::Error::msg)?;
        let root = context.root_dir().path().to_path_buf();
        let legacy_marker = root.join(RPC_ROOT).join("CURRENT");
        ensure!(
            !legacy_marker.exists(),
            "legacy RPC database detected at {}; clear the RPC DB before starting partitioned Full RPC storage",
            legacy_marker.display()
        );

        let control: RpcControlDb = context.open_preconfigured(CONTROL_SUBDIR)?;
        let router: RpcRouterDb = context.open_preconfigured(ROUTER_SUBDIR)?;
        let current_state: RpcCurrentStateDb = context.open_preconfigured(CURRENT_STATE_SUBDIR)?;
        let sealed_cache = Cache::builder()
            .max_capacity(config.max_open_sealed_partitions as u64)
            .build();

        let control_state = match control.state.get(codec::control_state_key())? {
            Some(value) => codec::decode_control_state(value.as_ref())?,
            None => ControlState {
                node_instance_id: rand::random::<InstanceId>(),
                next_partition_id: PartitionId::FIRST.0,
                min_transaction_lt: u64::MAX,
            },
        };

        let mut manager = Self {
            context,
            config,
            root,
            control,
            router,
            current_state,
            control_state,
            visible_frontier: None,
            manifest_epoch: 0,
            descriptors: BTreeMap::new(),
            active: None,
            active_id: PartitionId::FIRST,
            sealing: BTreeMap::new(),
            closing: BTreeSet::new(),
            sealed_cache,
            rotation_requested: None,
            sealer_busy: false,
        };
        manager.load_or_bootstrap()?;
        manager.refresh_lifecycle_metrics();
        Ok(manager)
    }

    #[cfg(test)]
    pub fn control_db(&self) -> &RpcControlDb {
        &self.control
    }

    pub fn router_db(&self) -> &RpcRouterDb {
        &self.router
    }

    pub fn current_state_db(&self) -> &RpcCurrentStateDb {
        &self.current_state
    }

    pub fn active_id(&self) -> PartitionId {
        self.active_id
    }

    #[cfg(test)]
    pub fn active_db(&self) -> &RpcTransactionsDb {
        self.active.as_deref().expect("partition manager is initialized with an active DB")
    }

    pub fn visible_frontier(&self) -> Option<&tycho_types::models::BlockId> {
        self.visible_frontier.as_ref()
    }

    pub fn has_commits_after(&self, mc_seqno: u32) -> bool {
        self.descriptors.values().any(|descriptor| {
            descriptor.last.block_id.is_some() && descriptor.last.mc_seqno > mc_seqno
        })
    }

    pub fn validate_masterchain_commit(
        &self,
        block_id: &tycho_types::models::BlockId,
    ) -> Result<()> {
        ensure!(block_id.is_masterchain(), "RPC frontier must be a masterchain block");
        let (id, lease) = self.lease_for_mc_seqno(block_id.seqno)?;
        let key = codec::partition_commit_key(block_id.seqno, &block_id.as_short_id());
        let value = lease
            .partition_commits
            .get(key)?
            .with_context(|| {
                format!(
                    "transaction partition {} is missing the RPC frontier commit for {}",
                    id.0, block_id
                )
            })?;
        let commit = codec::decode_partition_commit(value.as_ref())?;
        ensure!(
            commit.block_id == *block_id && commit.digest == block_id.root_hash,
            "RPC frontier commit identity mismatch for {block_id}"
        );
        Ok(())
    }

    pub fn manifest_epoch(&self) -> u64 {
        self.manifest_epoch
    }

    pub fn min_transaction_lt(&self) -> u64 {
        self.control_state.min_transaction_lt
    }

    pub fn persist_min_transaction_lt_decrease(&mut self, min_transaction_lt: u64) -> Result<bool> {
        if min_transaction_lt >= self.control_state.min_transaction_lt {
            return Ok(false);
        }
        let mut control_state = self.control_state;
        control_state.min_transaction_lt = min_transaction_lt;
        let mut batch = rocksdb::WriteBatch::default();
        batch.put_cf(&self.control.state.cf(), codec::control_state_key(), codec::encode_control_state(control_state));
        self.write_control(batch)?;
        self.control_state = control_state;
        Ok(true)
    }

    pub fn descriptors(&self) -> Vec<PartitionDescriptor> {
        self.descriptors.values().cloned().collect()
    }

    fn manifest_metrics(&self) -> PartitionManifestMetrics {
        let mut result = PartitionManifestMetrics {
            manifest_epoch: self.manifest_epoch,
            visible_mc_seqno: self
                .visible_frontier
                .map_or(0, |frontier| frontier.seqno),
            ..Default::default()
        };
        for descriptor in self.descriptors.values() {
            match descriptor.lifecycle {
                ManifestLifecycle::Creating => result.creating_count += 1,
                ManifestLifecycle::Active => {
                    result.active_id = descriptor.id.0;
                    result.active_count += 1;
                    result.active_counters = descriptor.counters;
                }
                ManifestLifecycle::Sealing => result.sealing_count += 1,
                ManifestLifecycle::Sealed => result.sealed_count += 1,
            }
        }
        result
    }

    fn refresh_lifecycle_metrics(&self) {
        let snapshot = self.manifest_metrics();
        metrics::gauge!(
            "tycho_storage_rpc_partition_count",
            "lifecycle" => "creating",
        )
        .set(snapshot.creating_count as f64);
        metrics::gauge!(
            "tycho_storage_rpc_partition_count",
            "lifecycle" => "active",
        )
        .set(snapshot.active_count as f64);
        metrics::gauge!(
            "tycho_storage_rpc_partition_count",
            "lifecycle" => "sealing",
        )
        .set(snapshot.sealing_count as f64);
        metrics::gauge!(
            "tycho_storage_rpc_partition_count",
            "lifecycle" => "sealed",
        )
        .set(snapshot.sealed_count as f64);
        metrics::gauge!("tycho_storage_rpc_partition_sealing_queue_depth")
            .set(snapshot.sealing_count as f64);
        Self::set_progress_metrics(snapshot);
    }

    fn refresh_progress_metrics(&self) {
        let active_counters = self
            .descriptors
            .get(&self.active_id)
            .filter(|descriptor| descriptor.lifecycle == ManifestLifecycle::Active)
            .map_or(PartitionCounters::default(), |descriptor| descriptor.counters);
        Self::set_progress_metrics(PartitionManifestMetrics {
            active_id: self.active_id.0,
            active_counters,
            manifest_epoch: self.manifest_epoch,
            visible_mc_seqno: self
                .visible_frontier
                .map_or(0, |frontier| frontier.seqno),
            ..Default::default()
        });
    }

    fn set_progress_metrics(snapshot: PartitionManifestMetrics) {
        metrics::gauge!("tycho_storage_rpc_partition_active_id").set(snapshot.active_id as f64);
        metrics::gauge!("tycho_storage_rpc_partition_active_estimated_lsm_bytes")
            .set(snapshot.active_counters.estimated_lsm_bytes as f64);
        metrics::gauge!("tycho_storage_rpc_partition_active_estimated_blob_bytes")
            .set(snapshot.active_counters.estimated_blob_bytes as f64);
        metrics::gauge!("tycho_storage_rpc_partition_active_transaction_count")
            .set(snapshot.active_counters.transaction_count as f64);
        metrics::gauge!("tycho_storage_rpc_partition_active_index_record_count")
            .set(snapshot.active_counters.index_record_count as f64);
        metrics::gauge!("tycho_storage_rpc_partition_manifest_epoch")
            .set(snapshot.manifest_epoch as f64);
        metrics::gauge!("tycho_storage_rpc_partition_visible_mc_seqno")
            .set(snapshot.visible_mc_seqno as f64);
    }

    fn log_lifecycle_transition(
        &self,
        descriptor: &PartitionDescriptor,
        threshold_reason: &'static str,
        elapsed: Duration,
    ) {
        let elapsed_micros = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
        tracing::info!(
            partition_id = descriptor.id.0,
            lifecycle = ?descriptor.lifecycle,
            manifest_epoch = self.manifest_epoch,
            frontier = ?self.visible_frontier,
            first_block_id = ?descriptor.first.block_id,
            first_mc_seqno = descriptor.first.mc_seqno,
            first_transaction_lt = descriptor.first.transaction_lt,
            first_gen_utime = descriptor.first.gen_utime,
            last_block_id = ?descriptor.last.block_id,
            last_mc_seqno = descriptor.last.mc_seqno,
            last_transaction_lt = descriptor.last.transaction_lt,
            last_gen_utime = descriptor.last.gen_utime,
            estimated_lsm_bytes = descriptor.counters.estimated_lsm_bytes,
            estimated_blob_bytes = descriptor.counters.estimated_blob_bytes,
            transaction_count = descriptor.counters.transaction_count,
            index_record_count = descriptor.counters.index_record_count,
            threshold_reason,
            elapsed_micros,
            "RPC transaction partition lifecycle transition",
        );
    }

    pub fn select_partition(&self, mc_seqno: u32) -> PartitionId {
        if self
            .visible_frontier
            .is_none_or(|frontier| mc_seqno > frontier.seqno)
        {
            return self.active_id;
        }
        self.descriptors
            .values()
            .find(|descriptor| descriptor.contains_mc_seqno(mc_seqno))
            .map_or(self.active_id, |descriptor| descriptor.id)
    }

    /// Acquires the selected partition handle without retaining the manager lock during I/O.
    pub fn lease_for_mc_seqno(&self, mc_seqno: u32) -> Result<(PartitionId, PartitionReadLease)> {
        let id = self.select_partition(mc_seqno);
        Ok((id, self.read_lease(id)?))
    }

    pub fn read_lease(&self, id: PartitionId) -> Result<PartitionReadLease> {
        let descriptor = self.descriptors.get(&id).context("selected transaction partition is missing")?;
        let lease = match descriptor.lifecycle {
            ManifestLifecycle::Active => self.active_lease(),
            ManifestLifecycle::Sealing => self.sealing_lease(id).context("sealing transaction partition handle is missing")?,
            ManifestLifecycle::Sealed => self.sealed_lease(id)?,
            ManifestLifecycle::Creating => bail!("selected transaction partition is still creating"),
        };
        Ok(lease)
    }

    /// Restores a missing sealing handle before publishing a complete composite snapshot.
    pub fn snapshot_lease(&mut self, id: PartitionId) -> Result<PartitionReadLease> {
        if self.descriptors.get(&id).map(|entry| entry.lifecycle)
            == Some(ManifestLifecycle::Sealing)
            && !self.sealing.contains_key(&id)
        {
            let db = self
                .reopen_sealing_writable(id)
                .with_context(|| {
                    format!(
                        "failed to restore sealing transaction partition {} for snapshot publication",
                        id.0
                    )
                })?;
            self.sealing.insert(id, db);
        }
        self.read_lease(id)
    }

    /// Acquires a writable lease and rejects a sealing partition once close has begun.
    pub fn write_lease_for_mc_seqno(&self, mc_seqno: u32) -> Result<(PartitionId, PartitionReadLease)> {
        let (id, lease) = self.lease_for_mc_seqno(mc_seqno)?;
        ensure!(!self.closing.contains(&id), "transaction partition {} is closing", id.0);
        ensure!(lease.lifecycle() != ManifestLifecycle::Sealed, "transaction partition {} is sealed", id.0);
        Ok((id, lease))
    }

    pub fn active_lease(&self) -> PartitionReadLease {
        PartitionReadLease {
            db: self.active.as_ref().expect("partition manager is initialized with an active DB").clone(),
            lifecycle: ManifestLifecycle::Active,
        }
    }

    pub fn sealing_lease(&self, id: PartitionId) -> Option<PartitionReadLease> {
        self.sealing
            .get(&id)
            .cloned()
            .map(|db| PartitionReadLease { db, lifecycle: ManifestLifecycle::Sealing })
    }

    pub fn sealed_lease(&self, id: PartitionId) -> Result<PartitionReadLease> {
        self.sealed_lease_opener(id)?.open()
    }

    pub fn sealed_lease_opener(&self, id: PartitionId) -> Result<SealedPartitionLeaseOpener> {
        ensure!(self.descriptors.get(&id).map(|entry| entry.lifecycle) == Some(ManifestLifecycle::Sealed), "partition {} is not sealed", id.0);
        Ok(SealedPartitionLeaseOpener {
            id,
            context: self.context.clone(),
            subdir: self.partition_subdir(id),
            cache: self.sealed_cache.clone(),
        })
    }

    #[cfg(test)]
    pub fn run_sealed_cache_pending_tasks(&self) {
        self.sealed_cache.run_pending_tasks();
    }

    #[cfg(test)]
    pub fn sealed_cache_entry_count(&self) -> u64 {
        self.sealed_cache.entry_count()
    }

    pub fn next_sealing_partition(&self) -> Option<PartitionId> {
        self.descriptors
            .values()
            .find(|descriptor| descriptor.lifecycle == ManifestLifecycle::Sealing)
            .map(|descriptor| descriptor.id)
    }

    pub fn begin_sealing(&mut self, id: PartitionId) -> Result<Arc<RpcTransactionsDb>> {
        ensure!(self.descriptors.get(&id).map(|entry| entry.lifecycle) == Some(ManifestLifecycle::Sealing), "partition {} is not sealing", id.0);
        self.closing.insert(id);
        if !self.sealing.contains_key(&id) {
            let db = self.reopen_sealing_writable(id)?;
            self.sealing.insert(id, db);
        }
        Ok(self.sealing.get(&id).unwrap().clone())
    }

    pub fn sealing_external_leases_drained(&self, id: PartitionId) -> bool {
        self.sealing.get(&id).is_some_and(|db| Arc::strong_count(db) <= 2)
    }

    /// Returns the primary handle count used by the snapshot publication gate.
    pub fn sealing_handle_strong_count(&self, id: PartitionId) -> Option<usize> {
        self.sealing.get(&id).map(Arc::strong_count)
    }

    pub fn take_sealing_handle(&mut self, id: PartitionId) -> Result<Arc<RpcTransactionsDb>> {
        ensure!(self.sealing_external_leases_drained(id), "transaction partition {} still has read leases", id.0);
        self.sealing.remove(&id).context("sealing transaction partition handle is missing")
    }

    pub fn open_sealed_read_only(&self, id: PartitionId) -> Result<Arc<RpcTransactionsDb>> {
        Ok(Arc::new(self.context.open_read_only(self.partition_subdir(id))?))
    }

    pub fn reopen_sealing_writable(&self, id: PartitionId) -> Result<Arc<RpcTransactionsDb>> {
        let db = Arc::new(self.context.open_preconfigured(self.partition_subdir(id))?);
        let validation = Self::validate_partition_db(&db, id);
        self.register_active_rocksdb_metrics();
        validation?;
        Ok(db)
    }

    pub fn restore_sealing_handle(&mut self, id: PartitionId, db: Arc<RpcTransactionsDb>) {
        self.sealing.insert(id, db);
    }

    fn register_active_rocksdb_metrics(&self) {
        if let Some(active) = &self.active {
            self.context
                .add_rocksdb_instance(RpcTransactionsTables::NAME, active.raw());
        }
    }

    pub fn complete_sealing(&mut self, id: PartitionId, db: Arc<RpcTransactionsDb>) -> Result<()> {
        let started_at = Instant::now();
        ensure!(self.descriptors.get(&id).map(|entry| entry.lifecycle) == Some(ManifestLifecycle::Sealing), "partition {} is not sealing", id.0);
        ensure!(!self.sealing.contains_key(&id), "sealing transaction partition {} handle was not closed", id.0);
        let manifest_epoch = self.manifest_epoch.checked_add(1).context("manifest epoch overflow")?;
        let mut descriptor = self.descriptors.get(&id).unwrap().clone();
        let threshold_reason = self
            .threshold_reason(descriptor.counters)
            .map(RotationReason::as_str)
            .unwrap_or("recovery");
        descriptor.lifecycle = ManifestLifecycle::Sealed;
        descriptor.last_transition = ManifestTransition {
            lifecycle: ManifestLifecycle::Sealed,
            epoch: manifest_epoch,
            at_unix_time: now_unix_time(),
        };
        let mut batch = rocksdb::WriteBatch::default();
        batch.put_cf(&self.control.manifests.cf(), codec::partition_manifest_key(id.0), codec::encode_manifest(&descriptor.to_manifest()));
        batch.put_cf(&self.control.state.cf(), codec::manifest_epoch_key(), codec::encode_manifest_epoch(manifest_epoch));
        self.write_control(batch)?;
        self.descriptors.insert(id, descriptor);
        self.manifest_epoch = manifest_epoch;
        self.closing.remove(&id);
        self.sealed_cache.insert(id, db);
        self.sealer_busy = false;
        self.refresh_lifecycle_metrics();
        self.log_lifecycle_transition(
            self.descriptors.get(&id).unwrap(),
            threshold_reason,
            started_at.elapsed(),
        );
        Ok(())
    }

    fn threshold_reason(&self, counters: PartitionCounters) -> Option<RotationReason> {
        if counters.estimated_lsm_bytes >= self.config.target_lsm_bytes {
            Some(RotationReason::EstimatedLsmBytes)
        } else if counters.estimated_blob_bytes >= self.config.target_blob_bytes {
            Some(RotationReason::EstimatedBlobBytes)
        } else if counters.index_record_count >= self.config.target_index_records {
            Some(RotationReason::IndexRecordCount)
        } else {
            None
        }
    }

    pub fn request_rotation(&mut self, counters: PartitionCounters) -> Option<RotationReason> {
        let reason = self.threshold_reason(counters);
        if self.rotation_requested.is_none() {
            self.rotation_requested = reason;
        }
        self.rotation_requested
    }

    #[cfg(test)]
    pub fn rotation_requested(&self) -> Option<RotationReason> {
        self.rotation_requested
    }

    pub fn begin_partition_creation(&mut self) -> Result<PartitionId> {
        let started_at = Instant::now();
        ensure!(!self.descriptors.values().any(|entry| entry.lifecycle == ManifestLifecycle::Creating), "a transaction partition is already creating");
        let has_active = self.descriptors.values().any(|entry| entry.lifecycle == ManifestLifecycle::Active);
        let threshold_reason = self
            .rotation_requested
            .map(RotationReason::as_str)
            .unwrap_or(if has_active { "manual" } else { "bootstrap" });
        ensure!(!has_active || (!self.sealer_busy && !self.descriptors.values().any(|entry| entry.lifecycle == ManifestLifecycle::Sealing)), "cannot create a transaction partition while another partition is sealing");
        let id = PartitionId(self.control_state.next_partition_id);
        ensure!(id.0 != 0, "next transaction partition id must be non-zero");
        ensure!(id.0 <= MAX_PARTITION_ID, "next transaction partition id exceeds the 16-digit decimal range");
        let mut control_state = self.control_state;
        control_state.next_partition_id = control_state.next_partition_id.checked_add(1).context("transaction partition id overflow")?;
        let manifest_epoch = self.manifest_epoch.checked_add(1).context("manifest epoch overflow")?;
        let descriptor = Self::empty_descriptor(id, ManifestLifecycle::Creating, manifest_epoch);
        let mut batch = rocksdb::WriteBatch::default();
        batch.put_cf(&self.control.state.cf(), codec::control_state_key(), codec::encode_control_state(control_state));
        batch.put_cf(&self.control.manifests.cf(), codec::partition_manifest_key(id.0), codec::encode_manifest(&descriptor.to_manifest()));
        batch.put_cf(&self.control.state.cf(), codec::manifest_epoch_key(), codec::encode_manifest_epoch(manifest_epoch));
        self.write_control(batch)?;
        self.control_state = control_state;
        self.manifest_epoch = manifest_epoch;
        self.descriptors.insert(id, descriptor);
        self.refresh_lifecycle_metrics();
        self.log_lifecycle_transition(
            self.descriptors.get(&id).unwrap(),
            threshold_reason,
            started_at.elapsed(),
        );
        Ok(id)
    }

    /// Completes the second, atomic half of the two-step creation protocol.
    pub fn complete_partition_creation(&mut self, id: PartitionId) -> Result<()> {
        let (old_descriptor, visible_frontier) = self.recover_creating_activation()?;
        self.activate_creating_partition(id, old_descriptor, visible_frontier.as_ref())
    }

    /// Rebuilds the data which must be atomically published when startup resumes `Creating`.
    fn recover_creating_activation(&self) -> Result<(Option<PartitionDescriptor>, Option<tycho_types::models::BlockId>)> {
        let Some(old_active) = self
            .descriptors
            .values()
            .find(|descriptor| descriptor.lifecycle == ManifestLifecycle::Active)
            .map(|descriptor| descriptor.id)
        else {
            return Ok((None, None));
        };
        let temporary_db = if self.active.is_none() {
            Some(self.context.open_preconfigured(self.partition_subdir(old_active))?)
        } else {
            None
        };
        let db = self
            .active
            .as_deref()
            .or(temporary_db.as_ref())
            .expect("partition manager is initialized with an active DB");
        Self::validate_partition_db(db, old_active)?;
        let descriptor = Self::reconstruct_descriptor(db, self.descriptors.get(&old_active).unwrap())?;
        let recovered_frontier = Self::latest_masterchain_commit(db)?;
        drop(temporary_db);
        let visible_frontier = match (self.visible_frontier, recovered_frontier) {
            (Some(persisted), Some(recovered)) => {
                ensure!(recovered.seqno >= persisted.seqno, "creating transaction partition recovery would regress visible frontier from {} to {}", persisted.seqno, recovered.seqno);
                if recovered.seqno == persisted.seqno {
                    ensure!(recovered == persisted, "creating transaction partition recovery found a different full block id at visible frontier seqno {}", persisted.seqno);
                }
                Some(recovered)
            }
            (Some(persisted), None) => Some(persisted),
            (None, recovered) => recovered,
        };
        Ok((Some(descriptor), visible_frontier))
    }

    /// Creates the DB for a persisted `Creating` entry and publishes the new active partition.
    ///
    /// The caller supplies the fully reconstructed old descriptor and the boundary frontier when
    /// activation is part of a block-set commit. This keeps all externally visible control state
    /// in the second write of the creation protocol.
    fn activate_creating_partition(
        &mut self,
        id: PartitionId,
        old_descriptor: Option<PartitionDescriptor>,
        visible_frontier: Option<&tycho_types::models::BlockId>,
    ) -> Result<()> {
        let started_at = Instant::now();
        let reason = self.rotation_requested;
        ensure!(self.descriptors.get(&id).map(|entry| entry.lifecycle) == Some(ManifestLifecycle::Creating), "partition {} is not creating", id.0);
        let db: Arc<RpcTransactionsDb> = Arc::new(self.context.open_preconfigured(self.partition_subdir(id))?);
        let registration_context = self.context.clone();
        let registration_active = self.active.clone();
        let registration_restore = scopeguard::guard((), move |()| {
            if let Some(active) = registration_active {
                registration_context
                    .add_rocksdb_instance(RpcTransactionsTables::NAME, active.raw());
            }
        });
        Self::validate_partition_db(&db, id)?;

        let manifest_epoch = self.manifest_epoch.checked_add(1).context("manifest epoch overflow")?;
        let old_active = self.active_id;
        let old_exists = self.descriptors.get(&old_active).map(|entry| entry.lifecycle == ManifestLifecycle::Active).unwrap_or(false);
        let threshold_reason = reason
            .map(RotationReason::as_str)
            .unwrap_or(if old_exists { "recovery" } else { "bootstrap" });
        let recovered_old_active = if old_exists && self.active.is_none() {
            let old: RpcTransactionsDb = self.context.open_preconfigured(self.partition_subdir(old_active))?;
            Self::validate_partition_db(&old, old_active)?;
            Some(Arc::new(old))
        } else {
            None
        };
        let old_descriptor = match old_descriptor {
            Some(descriptor) => Some(descriptor),
            None if old_exists => {
                let old_db = recovered_old_active
                    .as_ref()
                    .or(self.active.as_ref())
                    .expect("partition manager is initialized with an active DB");
                Some(Self::reconstruct_descriptor(
                    old_db,
                    self.descriptors.get(&old_active).unwrap(),
                )?)
            }
            None => None,
        };
        let mut descriptors = self.descriptors.clone();
        if old_exists {
            let old = descriptors.get_mut(&old_active).unwrap();
            if let Some(old_descriptor) = old_descriptor {
                ensure!(old_descriptor.id == old_active, "activation descriptor does not match old active partition");
                *old = old_descriptor;
            }
            old.lifecycle = ManifestLifecycle::Sealing;
            old.last_transition = ManifestTransition {
                lifecycle: ManifestLifecycle::Sealing,
                epoch: manifest_epoch,
                at_unix_time: now_unix_time(),
            };
        }
        let new = descriptors.get_mut(&id).unwrap();
        new.lifecycle = ManifestLifecycle::Active;
        new.last_transition = ManifestTransition {
            lifecycle: ManifestLifecycle::Active,
            epoch: manifest_epoch,
            at_unix_time: now_unix_time(),
        };
        let mut batch = rocksdb::WriteBatch::default();
        if old_exists {
            let old = descriptors.get(&old_active).unwrap();
            batch.put_cf(&self.control.manifests.cf(), codec::partition_manifest_key(old_active.0), codec::encode_manifest(&old.to_manifest()));
        }
        let new = descriptors.get(&id).unwrap();
        batch.put_cf(&self.control.manifests.cf(), codec::partition_manifest_key(id.0), codec::encode_manifest(&new.to_manifest()));
        batch.put_cf(&self.control.state.cf(), codec::active_partition_key(), codec::encode_active_partition(id.0));
        batch.put_cf(&self.control.state.cf(), codec::manifest_epoch_key(), codec::encode_manifest_epoch(manifest_epoch));
        if let Some(visible_frontier) = visible_frontier {
            batch.put_cf(&self.control.state.cf(), codec::visible_frontier_key(), codec::encode_visible_frontier(visible_frontier));
        }
        self.write_control(batch)?;

        if old_exists {
            let old = recovered_old_active.or_else(|| self.active.clone()).expect("partition manager is initialized with an active DB");
            self.sealing.insert(old_active, old);
            self.sealer_busy = true;
        }
        self.active = Some(db);
        self.active_id = id;
        self.manifest_epoch = manifest_epoch;
        self.descriptors = descriptors;
        if let Some(visible_frontier) = visible_frontier {
            self.visible_frontier = Some(*visible_frontier);
        }
        self.rotation_requested = None;
        self.register_active_rocksdb_metrics();
        let _ = scopeguard::ScopeGuard::into_inner(registration_restore);
        self.refresh_lifecycle_metrics();
        if old_exists {
            self.log_lifecycle_transition(
                self.descriptors.get(&old_active).unwrap(),
                threshold_reason,
                started_at.elapsed(),
            );
        }
        self.log_lifecycle_transition(
            self.descriptors.get(&id).unwrap(),
            threshold_reason,
            started_at.elapsed(),
        );
        Ok(())
    }

    /// AB15 calls this after a complete masterchain block set commits.
    #[cfg(test)]
    pub fn rotate_if_requested(&mut self) -> Result<Option<(PartitionId, RotationReason)>> {
        let Some(reason) = self.rotation_requested else {
            return Ok(None);
        };
        if self.sealer_busy {
            metrics::counter!("tycho_storage_rpc_partition_rotation_deferred_total").increment(1);
            return Ok(None);
        }
        let old = self.active_id;
        let started_at = Instant::now();
        let result = (|| {
            let id = self.begin_partition_creation()?;
            let visible_frontier = self.visible_frontier;
            self.activate_creating_partition(id, None, visible_frontier.as_ref())?;
            Ok((old, reason))
        })();
        record_rotation_result(reason, started_at.elapsed(), result.is_ok());
        result.map(Some)
    }

    /// Commits a complete masterchain block set after all related local writes succeeded.
    pub fn commit_masterchain_block_set(&mut self, block_id: &tycho_types::models::BlockId) -> Result<()> {
        ensure!(block_id.is_masterchain(), "block-set boundary must be a masterchain block");
        if let Some(visible_frontier) = self.visible_frontier
            && block_id.seqno == visible_frontier.seqno
        {
            ensure!(*block_id == visible_frontier, "masterchain block-set replay has a different full id at visible frontier seqno {}", block_id.seqno);
        }
        let id = self.select_partition(block_id.seqno);
        let lease = match self.descriptors.get(&id).context("selected partition is missing")?.lifecycle {
            ManifestLifecycle::Active => self.active_lease(),
            ManifestLifecycle::Sealing => self.sealing_lease(id).context("sealing partition handle is missing")?,
            ManifestLifecycle::Sealed => self.sealed_lease(id)?,
            ManifestLifecycle::Creating => bail!("selected partition is creating"),
        };
        let key = codec::partition_commit_key(block_id.seqno, &block_id.as_short_id());
        let value = lease.db().partition_commits.get(key)?.context("masterchain partition commit is missing")?;
        let commit = codec::decode_partition_commit(value.as_ref())?;
        ensure!(commit.block_id == *block_id && commit.digest == block_id.root_hash, "masterchain partition commit identity mismatch");
        let persisted_descriptor = self.descriptors.get(&id).unwrap().clone();

        if let Some(visible_frontier) = self.visible_frontier {
            // replays at or below the visible frontier only validate the durable local commit
            if block_id.seqno < visible_frontier.seqno {
                ensure!(persisted_descriptor.contains_mc_seqno(block_id.seqno), "older masterchain block-set replay is outside the persisted partition manifest");
                return Ok(());
            }
            if block_id.seqno == visible_frontier.seqno {
                ensure!(persisted_descriptor.contains_mc_seqno(block_id.seqno), "visible masterchain block-set replay is outside the persisted partition manifest");
                return Ok(());
            }
        }

        ensure!(id == self.active_id, "cannot publish a newer masterchain block set through non-active partition {}", id.0);

        // startup reconstruction can run ahead of the visible frontier and miss later same-seq writes
        let recovery_ahead = persisted_descriptor.last.block_id.is_some()
            && self
                .visible_frontier
                .is_none_or(|frontier| persisted_descriptor.last.mc_seqno > frontier.seqno);
        let descriptor = if recovery_ahead {
            Self::reconstruct_descriptor(lease.db(), &persisted_descriptor)?
        } else {
            Self::aggregate_commits_after_descriptor(
                lease.db(),
                &persisted_descriptor,
                block_id.seqno,
            )?
            .0
        };
        let rotation_reason = if descriptor.last.block_id.is_some()
            && descriptor.last.mc_seqno <= block_id.seqno
        {
            self.request_rotation(descriptor.counters)
        } else {
            None
        };
        if let Some(reason) = rotation_reason
            && !self.sealer_busy
        {
            let started_at = Instant::now();
            let result = (|| {
                let new_id = self.begin_partition_creation()?;
                self.activate_creating_partition(new_id, Some(descriptor), Some(block_id))
            })();
            record_rotation_result(reason, started_at.elapsed(), result.is_ok());
            result?;
            return Ok(());
        }
        if self.rotation_requested.is_some() {
            metrics::counter!("tycho_storage_rpc_partition_rotation_deferred_total").increment(1);
        }

        let epoch = self.manifest_epoch.checked_add(1).context("manifest epoch overflow")?;
        let mut batch = rocksdb::WriteBatch::default();
        batch.put_cf(&self.control.manifests.cf(), codec::partition_manifest_key(id.0), codec::encode_manifest(&descriptor.to_manifest()));
        batch.put_cf(&self.control.state.cf(), codec::visible_frontier_key(), codec::encode_visible_frontier(block_id));
        batch.put_cf(&self.control.state.cf(), codec::manifest_epoch_key(), codec::encode_manifest_epoch(epoch));
        self.write_control(batch)?;
        self.descriptors.insert(id, descriptor);
        self.visible_frontier = Some(*block_id);
        self.manifest_epoch = epoch;
        self.refresh_progress_metrics();
        Ok(())
    }

    fn load_or_bootstrap(&mut self) -> Result<()> {
        let state_exists = self.control.state.get(codec::control_state_key())?.is_some();
        if !state_exists {
            ensure!(self.read_descriptors()?.is_empty(), "partition manifests exist without control state");
            let id = self.begin_partition_creation()?;
            self.complete_partition_creation(id)?;
            self.validate_directories()?;
            return Ok(());
        }

        self.control_state = codec::decode_control_state(self.control.state.get(codec::control_state_key())?.unwrap().as_ref())?;
        ensure!(self.control_state.next_partition_id != 0, "control state next partition id must be non-zero");
        self.manifest_epoch = codec::decode_manifest_epoch(
            self.control.state.get(codec::manifest_epoch_key())?.context("missing manifest epoch")?.as_ref(),
        )?;
        self.visible_frontier = self.control.state.get(codec::visible_frontier_key())?
            .map(|value| codec::decode_visible_frontier(value.as_ref()))
            .transpose()?;
        self.descriptors = self.read_descriptors()?;
        self.validate_descriptors(true)?;
        self.validate_directories()?;
        self.resume_creating()?;
        self.validate_descriptors(false)?;
        self.validate_active_pointer()?;
        self.validate_directories()?;
        self.open_existing_partitions()?;
        self.reconstruct_partitions()?;
        Ok(())
    }

    fn resume_creating(&mut self) -> Result<()> {
        let creating = self.descriptors.values().filter(|entry| entry.lifecycle == ManifestLifecycle::Creating).map(|entry| entry.id).collect::<Vec<_>>();
        ensure!(creating.len() <= 1, "more than one transaction partition is creating");
        if let Some(id) = creating.first().copied() {
            self.complete_partition_creation(id)?;
        }
        Ok(())
    }

    fn open_existing_partitions(&mut self) -> Result<()> {
        let active = self.descriptors.values().find(|entry| entry.lifecycle == ManifestLifecycle::Active).context("active partition manifest is missing")?.id;
        if self.active.is_none() {
            self.active = Some(Arc::new(self.context.open_preconfigured(self.partition_subdir(active))?));
        }
        Self::validate_partition_db(self.active.as_ref().unwrap(), active)?;
        self.active_id = active;
        for descriptor in self.descriptors.values() {
            match descriptor.lifecycle {
                ManifestLifecycle::Sealing => {
                    if !self.sealing.contains_key(&descriptor.id) {
                        let db = self.reopen_sealing_writable(descriptor.id)?;
                        self.sealing.insert(descriptor.id, db);
                    }
                    self.sealer_busy = true;
                }
                ManifestLifecycle::Sealed => {
                    let db = self.context.open_read_only(self.partition_subdir(descriptor.id))?;
                    Self::validate_partition_db(&db, descriptor.id)?;
                }
                ManifestLifecycle::Creating | ManifestLifecycle::Active => {}
            }
        }
        Ok(())
    }

    fn reconstruct_partitions(&mut self) -> Result<()> {
        let ids = self.descriptors.keys().copied().collect::<Vec<_>>();
        for id in ids {
            let descriptor = self.descriptors.get(&id).unwrap().clone();
            if descriptor.lifecycle == ManifestLifecycle::Creating {
                continue;
            }
            let db = if id == self.active_id {
                self.active.as_ref().expect("partition manager is initialized with an active DB").clone()
            } else if let Some(db) = self.sealing.get(&id) {
                db.clone()
            } else {
                Arc::new(self.context.open_read_only(self.partition_subdir(id))?)
            };
            let reconstructed = Self::reconstruct_descriptor(&db, &descriptor)?;
            if reconstructed != descriptor {
                if id != self.active_id {
                    bail!("partition {} manifest counters or bounds do not match partition commits", id.0);
                }
                self.persist_active_reconstruction(reconstructed)?;
            }
        }
        self.validate_descriptors(false)
    }

    fn persist_active_reconstruction(&mut self, descriptor: PartitionDescriptor) -> Result<()> {
        let manifest_epoch = self.manifest_epoch.checked_add(1).context("manifest epoch overflow")?;
        let mut batch = rocksdb::WriteBatch::default();
        batch.put_cf(&self.control.manifests.cf(), codec::partition_manifest_key(descriptor.id.0), codec::encode_manifest(&descriptor.to_manifest()));
        batch.put_cf(&self.control.state.cf(), codec::manifest_epoch_key(), codec::encode_manifest_epoch(manifest_epoch));
        self.write_control(batch)?;
        self.manifest_epoch = manifest_epoch;
        self.descriptors.insert(descriptor.id, descriptor);
        self.refresh_progress_metrics();
        Ok(())
    }

    /// Aggregates only commits not yet represented by the persisted descriptor.
    fn aggregate_commits_after_descriptor(
        db: &RpcTransactionsDb,
        original: &PartitionDescriptor,
        through_mc_seqno: u32,
    ) -> Result<(PartitionDescriptor, usize)> {
        if original.last.block_id.is_some() && original.last.mc_seqno >= through_mc_seqno {
            return Ok((original.clone(), 0));
        }
        let first_mc_seqno = match original.last.block_id {
            Some(_) => original.last.mc_seqno.checked_add(1).context("partition descriptor masterchain bound overflow")?,
            None => 0,
        };
        let mut result = original.clone();
        let mut scanned_commits = 0usize;
        let mut iterator = db.rocksdb().raw_iterator_cf(&db.partition_commits.cf());
        iterator.seek(first_mc_seqno.to_be_bytes());
        while iterator.valid() {
            let key = iterator.key().context("partition commit iterator returned no key")?;
            ensure!(key.len() >= 4, "invalid partition commit key length: {}", key.len());
            let mc_seqno = u32::from_be_bytes(key[..4].try_into().unwrap());
            if mc_seqno > through_mc_seqno {
                break;
            }
            let value = iterator.value().context("partition commit iterator returned no value")?;
            let (mc_seqno, short_id) = codec::decode_partition_commit_key(key)?;
            let commit = codec::decode_partition_commit(value)?;
            ensure!(commit.block_id.as_short_id() == short_id, "partition commit key does not match its full block id");
            ensure!(commit.digest == commit.block_id.root_hash, "partition commit digest does not match block root hash");
            result.counters.transaction_count = result.counters.transaction_count.checked_add(commit.transaction_count).context("transaction count overflow while aggregating partition")?;
            result.counters.estimated_lsm_bytes = result.counters.estimated_lsm_bytes.checked_add(commit.estimated_lsm_bytes).context("LSM byte counter overflow while aggregating partition")?;
            result.counters.estimated_blob_bytes = result.counters.estimated_blob_bytes.checked_add(commit.estimated_blob_bytes).context("blob byte counter overflow while aggregating partition")?;
            result.counters.index_record_count = result.counters.index_record_count.checked_add(commit.index_record_count).context("index record counter overflow while aggregating partition")?;
            let block_key = (mc_seqno, commit.block_id);
            match (result.first.block_id, result.last.block_id) {
                (Some(first_block), Some(last_block)) => {
                    if block_key < (result.first.mc_seqno, first_block) {
                        result.first.block_id = Some(commit.block_id);
                    }
                    if block_key > (result.last.mc_seqno, last_block) {
                        result.last.block_id = Some(commit.block_id);
                    }
                    result.first.mc_seqno = result.first.mc_seqno.min(mc_seqno);
                    result.first.transaction_lt = result.first.transaction_lt.min(commit.start_lt);
                    result.first.gen_utime = result.first.gen_utime.min(commit.gen_utime);
                    result.last.mc_seqno = result.last.mc_seqno.max(mc_seqno);
                    result.last.transaction_lt = result.last.transaction_lt.max(commit.end_lt);
                    result.last.gen_utime = result.last.gen_utime.max(commit.gen_utime);
                }
                (None, None) => {
                    result.first = ManifestBound {
                        block_id: Some(commit.block_id),
                        mc_seqno,
                        transaction_lt: commit.start_lt,
                        gen_utime: commit.gen_utime,
                    };
                    result.last = ManifestBound {
                        block_id: Some(commit.block_id),
                        mc_seqno,
                        transaction_lt: commit.end_lt,
                        gen_utime: commit.gen_utime,
                    };
                }
                _ => bail!("partition descriptor has incomplete bounds"),
            }
            scanned_commits = scanned_commits.checked_add(1).context("scanned partition commit count overflow")?;
            iterator.next();
        }
        iterator.status()?;
        Ok((result, scanned_commits))
    }

    fn reconstruct_descriptor(db: &RpcTransactionsDb, original: &PartitionDescriptor) -> Result<PartitionDescriptor> {
        let mut commits = Vec::new();
        let mut iterator = db.rocksdb().raw_iterator_cf(&db.partition_commits.cf());
        iterator.seek_to_first();
        while iterator.valid() {
            let key = iterator.key().context("partition commit iterator returned no key")?;
            let value = iterator.value().context("partition commit iterator returned no value")?;
            let (mc_seqno, short_id) = codec::decode_partition_commit_key(key)?;
            let commit = codec::decode_partition_commit(value)?;
            ensure!(commit.block_id.as_short_id() == short_id, "partition commit key does not match its full block id");
            ensure!(commit.digest == commit.block_id.root_hash, "partition commit digest does not match block root hash");
            commits.push((mc_seqno, short_id, commit));
            iterator.next();
        }
        iterator.status()?;
        let mut result = original.clone();
        result.counters = PartitionCounters::default();
        result.first = empty_bound();
        result.last = empty_bound();
        let mut first_mc_seqno = u32::MAX;
        let mut first_transaction_lt = u64::MAX;
        let mut first_gen_utime = u32::MAX;
        let mut last_mc_seqno = 0;
        let mut last_transaction_lt = 0;
        let mut last_gen_utime = 0;
        let mut first_block = None;
        let mut last_block = None;
        for (mc_seqno, _, commit) in commits.iter() {
            result.counters.transaction_count = result.counters.transaction_count.checked_add(commit.transaction_count).context("transaction count overflow while reconstructing partition")?;
            result.counters.estimated_lsm_bytes = result.counters.estimated_lsm_bytes.checked_add(commit.estimated_lsm_bytes).context("LSM byte counter overflow while reconstructing partition")?;
            result.counters.estimated_blob_bytes = result.counters.estimated_blob_bytes.checked_add(commit.estimated_blob_bytes).context("blob byte counter overflow while reconstructing partition")?;
            result.counters.index_record_count = result.counters.index_record_count.checked_add(commit.index_record_count).context("index record counter overflow while reconstructing partition")?;
            first_mc_seqno = first_mc_seqno.min(*mc_seqno);
            first_transaction_lt = first_transaction_lt.min(commit.start_lt);
            first_gen_utime = first_gen_utime.min(commit.gen_utime);
            last_mc_seqno = last_mc_seqno.max(*mc_seqno);
            last_transaction_lt = last_transaction_lt.max(commit.end_lt);
            last_gen_utime = last_gen_utime.max(commit.gen_utime);
            let block_key = (*mc_seqno, commit.block_id);
            if first_block.as_ref().is_none_or(|key| block_key < *key) {
                first_block = Some(block_key);
            }
            if last_block.as_ref().is_none_or(|key| block_key > *key) {
                last_block = Some(block_key);
            }
        }
        if let (Some((_, first_block)), Some((_, last_block))) = (first_block, last_block) {
            result.first = ManifestBound {
                block_id: Some(first_block),
                mc_seqno: first_mc_seqno,
                transaction_lt: first_transaction_lt,
                gen_utime: first_gen_utime,
            };
            result.last = ManifestBound {
                block_id: Some(last_block),
                mc_seqno: last_mc_seqno,
                transaction_lt: last_transaction_lt,
                gen_utime: last_gen_utime,
            };
        }
        Ok(result)
    }

    fn latest_masterchain_commit(db: &RpcTransactionsDb) -> Result<Option<tycho_types::models::BlockId>> {
        let mut latest = None;
        let mut iterator = db.rocksdb().raw_iterator_cf(&db.partition_commits.cf());
        iterator.seek_to_first();
        while iterator.valid() {
            let key = iterator.key().context("partition commit iterator returned no key")?;
            let value = iterator.value().context("partition commit iterator returned no value")?;
            let (mc_seqno, short_id) = codec::decode_partition_commit_key(key)?;
            let commit = codec::decode_partition_commit(value)?;
            ensure!(commit.block_id.as_short_id() == short_id, "partition commit key does not match its full block id");
            ensure!(commit.digest == commit.block_id.root_hash, "partition commit digest does not match block root hash");
            if commit.block_id.is_masterchain() {
                ensure!(commit.block_id.seqno == mc_seqno, "masterchain partition commit seqno does not match related masterchain seqno");
                if latest.as_ref().is_none_or(|current: &tycho_types::models::BlockId| commit.block_id.seqno > current.seqno) {
                    latest = Some(commit.block_id);
                }
            }
            iterator.next();
        }
        iterator.status()?;
        Ok(latest)
    }

    fn read_descriptors(&self) -> Result<BTreeMap<PartitionId, PartitionDescriptor>> {
        let mut result = BTreeMap::new();
        let mut iterator = self.control.rocksdb().raw_iterator_cf(&self.control.manifests.cf());
        iterator.seek_to_first();
        while iterator.valid() {
            let key = iterator.key().context("partition manifest iterator returned no key")?;
            ensure!(key.len() == codec::PARTITION_ID_LEN, "invalid partition manifest key length: {}", key.len());
            let id = PartitionId(u64::from_be_bytes(key.try_into().unwrap()));
            let value = iterator.value().context("partition manifest iterator returned no value")?;
            let descriptor = PartitionDescriptor::from_manifest(codec::decode_manifest(value)?);
            ensure!(descriptor.id == id, "partition manifest key does not match record id");
            ensure!(result.insert(id, descriptor).is_none(), "duplicate partition manifest id: {}", id.0);
            iterator.next();
        }
        iterator.status()?;
        Ok(result)
    }

    fn validate_descriptors(&self, allow_initial_creating: bool) -> Result<()> {
        ensure!(!self.descriptors.is_empty(), "transaction partition manifest is empty");
        let active = self.descriptors.values().filter(|entry| entry.lifecycle == ManifestLifecycle::Active).collect::<Vec<_>>();
        let creating = self.descriptors.values().filter(|entry| entry.lifecycle == ManifestLifecycle::Creating).count();
        let sealing = self.descriptors.values().filter(|entry| entry.lifecycle == ManifestLifecycle::Sealing).count();
        ensure!(creating <= 1, "at most one transaction partition may be creating");
        ensure!(sealing <= 1, "at most one transaction partition may be sealing");
        ensure!(creating == 0 || sealing == 0, "creating and sealing transaction partitions cannot coexist");
        let initial_creating = allow_initial_creating && active.is_empty() && creating == 1 && self.descriptors.len() == 1 && self.descriptors.contains_key(&PartitionId::FIRST);
        ensure!(active.len() == 1 || initial_creating, "exactly one transaction partition must be active");
        ensure!(self.control_state.next_partition_id <= MAX_PARTITION_ID + 1, "next transaction partition id exceeds the 16-digit decimal range");
        let mut expected_id = PartitionId::FIRST.0;
        let mut previous_range_end = None;
        let mut found_empty_range = false;
        for descriptor in self.descriptors.values() {
            ensure!(descriptor.id.0 == expected_id, "transaction partition ids are not contiguous at {}", descriptor.id.0);
            expected_id = expected_id.checked_add(1).context("transaction partition id overflow")?;
            ensure!(descriptor.id.0 != 0 && descriptor.id.0 <= MAX_PARTITION_ID, "invalid transaction partition id: {}", descriptor.id.0);
            ensure!(descriptor.last_transition.lifecycle == descriptor.lifecycle, "partition {} transition lifecycle does not match manifest lifecycle", descriptor.id.0);
            ensure!(descriptor.last_transition.epoch <= self.manifest_epoch, "partition {} transition epoch exceeds manifest epoch", descriptor.id.0);
            let has_first = descriptor.first.block_id.is_some();
            let has_last = descriptor.last.block_id.is_some();
            ensure!(has_first == has_last, "partition {} has incomplete bounds", descriptor.id.0);
            if has_first {
                ensure!(!found_empty_range, "non-empty transaction partition {} follows an empty partition", descriptor.id.0);
                ensure!(descriptor.first.mc_seqno <= descriptor.last.mc_seqno, "partition {} has inverted masterchain bounds", descriptor.id.0);
                ensure!(descriptor.first.transaction_lt <= descriptor.last.transaction_lt, "partition {} has inverted transaction LT bounds", descriptor.id.0);
                ensure!(descriptor.first.gen_utime <= descriptor.last.gen_utime, "partition {} has inverted generation-time bounds", descriptor.id.0);
                if let Some(previous_range_end) = previous_range_end {
                    ensure!(previous_range_end < descriptor.first.mc_seqno, "transaction partition masterchain ranges are not ordered by partition id");
                }
                previous_range_end = Some(descriptor.last.mc_seqno);
            } else {
                ensure!(descriptor.first.mc_seqno == 0 && descriptor.first.transaction_lt == 0 && descriptor.first.gen_utime == 0 && descriptor.last.mc_seqno == 0 && descriptor.last.transaction_lt == 0 && descriptor.last.gen_utime == 0, "partition {} has non-zero empty bounds", descriptor.id.0);
                found_empty_range = true;
            }
        }
        ensure!(self.control_state.next_partition_id == expected_id, "next transaction partition id does not follow the manifest sequence");
        Ok(())
    }

    fn validate_active_pointer(&self) -> Result<()> {
        let value = self.control.state.get(codec::active_partition_key())?.context("missing active transaction partition pointer")?;
        let active = PartitionId(codec::decode_active_partition(value.as_ref())?);
        ensure!(active.0 != 0, "active transaction partition pointer must be non-zero");
        let manifest_active = self.descriptors.values().find(|entry| entry.lifecycle == ManifestLifecycle::Active).context("active partition manifest is missing")?.id;
        ensure!(active == manifest_active, "active transaction partition pointer does not match active manifest");
        Ok(())
    }

    fn validate_directories(&self) -> Result<()> {
        let root = self.root.join(TRANSACTIONS_SUBDIR);
        if !root.exists() {
            return Ok(());
        }
        for entry in std::fs::read_dir(&root).with_context(|| format!("failed to read transaction partitions at {}", root.display()))? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let name = entry.file_name();
            let name = name.to_str().context("non-utf8 transaction partition directory")?;
            let id = PartitionId::parse_directory_name(name)?;
            ensure!(self.descriptors.contains_key(&id), "unknown transaction partition directory: {name}");
        }
        Ok(())
    }

    fn partition_subdir(&self, id: PartitionId) -> PathBuf {
        Path::new(TRANSACTIONS_SUBDIR).join(id.directory_name())
    }

    fn write_control(&self, batch: rocksdb::WriteBatch) -> Result<()> {
        let started_at = Instant::now();
        let result = self.control
            .rocksdb()
            .write_opt(batch, self.control.state.write_config())
            .context("failed to persist RPC partition control state");
        metrics::histogram!("tycho_storage_rpc_write_control_time")
            .record(started_at.elapsed());
        result
    }

    fn validate_partition_db(db: &RpcTransactionsDb, id: PartitionId) -> Result<()> {
        let _ = db.partition_commits.cf();
        ensure!(id.0 != 0, "partition id must be non-zero");
        Ok(())
    }

    fn empty_descriptor(id: PartitionId, lifecycle: ManifestLifecycle, epoch: u64) -> PartitionDescriptor {
        PartitionDescriptor {
            id,
            lifecycle,
            first: empty_bound(),
            last: empty_bound(),
            counters: PartitionCounters::default(),
            last_transition: ManifestTransition {
                lifecycle,
                epoch,
                at_unix_time: now_unix_time(),
            },
        }
    }
}

fn empty_bound() -> ManifestBound {
    ManifestBound {
        block_id: None,
        mc_seqno: 0,
        transaction_lt: 0,
        gen_utime: 0,
    }
}

fn now_unix_time() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::codec::PartitionCommit;
    use super::super::db::{
        RpcControlTables, RpcCurrentStateTables, RpcRouterTables,
    };
    use metrics::{
        Counter, CounterFn, Gauge, GaugeFn, Histogram, HistogramFn, Key, KeyName, Metadata,
        Recorder, SharedString, Unit,
    };
    use std::sync::Mutex as StdMutex;
    use tycho_types::cell::HashBytes;
    use tycho_types::models::{BlockId, ShardIdent};

    #[derive(Default)]
    struct TestCounter(StdMutex<u64>);

    impl CounterFn for TestCounter {
        fn increment(&self, value: u64) {
            *self.0.lock().unwrap() += value;
        }

        fn absolute(&self, value: u64) {
            let mut current = self.0.lock().unwrap();
            *current = (*current).max(value);
        }
    }

    #[derive(Default)]
    struct TestGauge(StdMutex<f64>);

    impl GaugeFn for TestGauge {
        fn increment(&self, value: f64) {
            *self.0.lock().unwrap() += value;
        }

        fn decrement(&self, value: f64) {
            *self.0.lock().unwrap() -= value;
        }

        fn set(&self, value: f64) {
            *self.0.lock().unwrap() = value;
        }
    }

    #[derive(Default)]
    struct TestHistogram(StdMutex<Vec<f64>>);

    impl HistogramFn for TestHistogram {
        fn record(&self, value: f64) {
            self.0.lock().unwrap().push(value);
        }
    }

    #[derive(Default)]
    struct TestMetricsRecorder {
        counters: StdMutex<BTreeMap<String, Arc<TestCounter>>>,
        gauges: StdMutex<BTreeMap<String, Arc<TestGauge>>>,
        histograms: StdMutex<BTreeMap<String, Arc<TestHistogram>>>,
    }

    impl TestMetricsRecorder {
        fn key(key: &Key) -> String {
            let mut result = key.name().to_owned();
            for label in key.labels() {
                result.push('|');
                result.push_str(label.key());
                result.push('=');
                result.push_str(label.value());
            }
            result
        }

        fn counter(&self, key: &str) -> u64 {
            self.counters
                .lock()
                .unwrap()
                .get(key)
                .map(|value| *value.0.lock().unwrap())
                .unwrap_or_default()
        }

        fn gauge(&self, key: &str) -> f64 {
            self.gauges
                .lock()
                .unwrap()
                .get(key)
                .map(|value| *value.0.lock().unwrap())
                .unwrap_or_default()
        }

        fn histogram_len(&self, key: &str) -> usize {
            self.histograms
                .lock()
                .unwrap()
                .get(key)
                .map(|value| value.0.lock().unwrap().len())
                .unwrap_or_default()
        }

        fn keys(&self) -> Vec<String> {
            let mut result = self.counters.lock().unwrap().keys().cloned().collect::<Vec<_>>();
            result.extend(self.gauges.lock().unwrap().keys().cloned());
            result.extend(self.histograms.lock().unwrap().keys().cloned());
            result
        }
    }

    impl Recorder for TestMetricsRecorder {
        fn describe_counter(
            &self,
            _key: KeyName,
            _unit: Option<Unit>,
            _description: SharedString,
        ) {
        }

        fn describe_gauge(
            &self,
            _key: KeyName,
            _unit: Option<Unit>,
            _description: SharedString,
        ) {
        }

        fn describe_histogram(
            &self,
            _key: KeyName,
            _unit: Option<Unit>,
            _description: SharedString,
        ) {
        }

        fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
            let value = self
                .counters
                .lock()
                .unwrap()
                .entry(Self::key(key))
                .or_default()
                .clone();
            Counter::from_arc(value)
        }

        fn register_gauge(&self, key: &Key, _metadata: &Metadata<'_>) -> Gauge {
            let value = self
                .gauges
                .lock()
                .unwrap()
                .entry(Self::key(key))
                .or_default()
                .clone();
            Gauge::from_arc(value)
        }

        fn register_histogram(&self, key: &Key, _metadata: &Metadata<'_>) -> Histogram {
            let value = self
                .histograms
                .lock()
                .unwrap()
                .entry(Self::key(key))
                .or_default()
                .clone();
            Histogram::from_arc(value)
        }
    }

    fn config() -> RpcTransactionPartitionsConfig {
        RpcTransactionPartitionsConfig {
            target_lsm_bytes: 10,
            target_blob_bytes: 10,
            target_index_records: 10,
            max_open_sealed_partitions: 1,
        }
    }

    fn block_id(seqno: u32) -> BlockId {
        BlockId {
            shard: ShardIdent::BASECHAIN,
            seqno,
            root_hash: HashBytes([seqno as u8; 32]),
            file_hash: HashBytes([7; 32]),
        }
    }

    fn masterchain_block_id(seqno: u32) -> BlockId {
        BlockId {
            shard: ShardIdent::MASTERCHAIN,
            seqno,
            root_hash: HashBytes([seqno as u8; 32]),
            file_hash: HashBytes([9; 32]),
        }
    }

    fn write_commit(
        db: &RpcTransactionsDb,
        mc_seqno: u32,
        block_id: BlockId,
        estimated_lsm_bytes: u64,
        estimated_blob_bytes: u64,
        index_record_count: u64,
    ) {
        let commit = PartitionCommit {
            block_id,
            digest: block_id.root_hash,
            transaction_count: (estimated_lsm_bytes != 0 || estimated_blob_bytes != 0 || index_record_count != 0)
                as u64,
            estimated_lsm_bytes,
            estimated_blob_bytes,
            index_record_count,
            start_lt: mc_seqno as u64 * 10,
            end_lt: mc_seqno as u64 * 10 + 1,
            gen_utime: mc_seqno,
        };
        let mut batch = rocksdb::WriteBatch::default();
        batch.put_cf(
            &db.partition_commits.cf(),
            codec::partition_commit_key(mc_seqno, &block_id.as_short_id()),
            codec::encode_partition_commit(&commit),
        );
        db.rocksdb().write_opt(batch, db.partition_commits.write_config()).unwrap();
    }

    fn assert_writable_metrics_registration(
        context: &StorageContext,
        manager: &PartitionManager,
    ) {
        assert!(context.rocksdb_instance_is_registered(
            RpcControlTables::NAME,
            manager.control.raw(),
        ));
        assert!(context.rocksdb_instance_is_registered(
            RpcRouterTables::NAME,
            manager.router.raw(),
        ));
        assert!(context.rocksdb_instance_is_registered(
            RpcCurrentStateTables::NAME,
            manager.current_state.raw(),
        ));
        assert!(context.rocksdb_instance_is_registered(
            RpcTransactionsTables::NAME,
            manager.active_db().raw(),
        ));
    }

    #[tokio::test]
    async fn bootstrap_is_persisted_and_reopens() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let manager = PartitionManager::open(context.clone(), config()).unwrap();
        assert_eq!(manager.active_id(), PartitionId::FIRST);
        assert_eq!(manager.descriptors().len(), 1);
        assert_writable_metrics_registration(&context, &manager);
        drop(manager);
        let reopened = PartitionManager::open(context.clone(), config()).unwrap();
        assert_eq!(reopened.active_id(), PartitionId::FIRST);
        assert_eq!(reopened.descriptors().len(), 1);
        assert_writable_metrics_registration(&context, &reopened);
    }

    #[tokio::test]
    async fn min_transaction_lt_decrease_is_persisted_and_never_increases() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let mut manager = PartitionManager::open(context.clone(), config()).unwrap();
        assert!(manager.persist_min_transaction_lt_decrease(100).unwrap());
        assert!(!manager.persist_min_transaction_lt_decrease(200).unwrap());
        assert_eq!(manager.min_transaction_lt(), 100);
        drop(manager);
        let reopened = PartitionManager::open(context, config()).unwrap();
        assert_eq!(reopened.min_transaction_lt(), 100);
    }

    #[tokio::test]
    async fn initial_creating_without_directory_resumes_to_active() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let control: RpcControlDb = context.open_preconfigured(CONTROL_SUBDIR).unwrap();
        let state = ControlState {
            node_instance_id: rand::random::<InstanceId>(),
            next_partition_id: 2,
            min_transaction_lt: u64::MAX,
        };
        let descriptor = PartitionManager::empty_descriptor(PartitionId::FIRST, ManifestLifecycle::Creating, 1);
        let mut batch = rocksdb::WriteBatch::default();
        batch.put_cf(&control.state.cf(), codec::control_state_key(), codec::encode_control_state(state));
        batch.put_cf(&control.state.cf(), codec::manifest_epoch_key(), codec::encode_manifest_epoch(1));
        batch.put_cf(&control.manifests.cf(), codec::partition_manifest_key(1), codec::encode_manifest(&descriptor.to_manifest()));
        control.rocksdb().write_opt(batch, control.state.write_config()).unwrap();
        drop(control);
        let manager = PartitionManager::open(context, config()).unwrap();
        assert_eq!(manager.active_id(), PartitionId::FIRST);
        assert_eq!(manager.descriptors()[0].lifecycle, ManifestLifecycle::Active);
    }

    #[tokio::test]
    async fn rejects_missing_malformed_and_mismatched_active_pointer() {
        for pointer in [None, Some(vec![0]), Some(codec::encode_active_partition(2).to_vec())] {
            let (context, _tmp) = StorageContext::new_temp().await.unwrap();
            let manager = PartitionManager::open(context.clone(), config()).unwrap();
            let mut batch = rocksdb::WriteBatch::default();
            match pointer {
                Some(ref value) => batch.put_cf(&manager.control_db().state.cf(), codec::active_partition_key(), value),
                None => batch.delete_cf(&manager.control_db().state.cf(), codec::active_partition_key()),
            }
            manager.control_db().rocksdb().write_opt(batch, manager.control_db().state.write_config()).unwrap();
            drop(manager);
            assert!(PartitionManager::open(context, config()).is_err());
        }
    }

    #[tokio::test]
    async fn rejects_legacy_and_unknown_partition_directories() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let legacy = context.root_dir().path().join(RPC_ROOT);
        std::fs::create_dir_all(&legacy).unwrap();
        std::fs::write(legacy.join("CURRENT"), []).unwrap();
        assert!(PartitionManager::open(context.clone(), config()).is_err());
        std::fs::remove_file(legacy.join("CURRENT")).unwrap();
        let manager = PartitionManager::open(context.clone(), config()).unwrap();
        drop(manager);
        std::fs::create_dir_all(context.root_dir().path().join(TRANSACTIONS_SUBDIR).join("0000000000000002")).unwrap();
        assert!(PartitionManager::open(context, config()).is_err());
    }

    #[tokio::test]
    async fn rejects_malformed_manifest_records() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let manager = PartitionManager::open(context.clone(), config()).unwrap();
        let mut batch = rocksdb::WriteBatch::default();
        batch.put_cf(&manager.control_db().manifests.cf(), codec::partition_manifest_key(2), [0]);
        manager.control_db().rocksdb().write_opt(batch, manager.control_db().manifests.write_config()).unwrap();
        drop(manager);
        assert!(PartitionManager::open(context, config()).is_err());
    }

    #[tokio::test]
    async fn rejects_malformed_partition_commit_on_startup() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let manager = PartitionManager::open(context.clone(), config()).unwrap();
        manager
            .active_db()
            .partition_commits
            .insert(
                codec::partition_commit_key(1, &block_id(1).as_short_id()),
                [0],
            )
            .unwrap();
        drop(manager);

        let error = PartitionManager::open(context, config())
            .err()
            .expect("malformed partition commit must fail startup");
        assert!(format!("{error:#}").contains("partition commit"));
    }

    #[tokio::test]
    async fn rejects_manifest_invariant_violations() {
        for case in ["zero-id", "transition-epoch", "scalar-empty-bound", "overlap", "id-gap", "range-order", "lt-bounds", "time-bounds", "two-sealing", "creating-sealing"] {
            let (context, _tmp) = StorageContext::new_temp().await.unwrap();
            let manager = PartitionManager::open(context.clone(), config()).unwrap();
            let mut control_state = manager.control_state;
            let mut first = manager.descriptors.get(&PartitionId::FIRST).unwrap().clone();
            let mut records = Vec::new();
            match case {
                "zero-id" => records.push(PartitionManager::empty_descriptor(PartitionId(0), ManifestLifecycle::Sealed, manager.manifest_epoch)),
                "transition-epoch" => {
                    first.last_transition.epoch = manager.manifest_epoch + 1;
                    records.push(first);
                }
                "scalar-empty-bound" => {
                    first.first.transaction_lt = 1;
                    records.push(first);
                }
                "overlap" => {
                    let bound = ManifestBound {
                        block_id: Some(block_id(1)),
                        mc_seqno: 1,
                        transaction_lt: 1,
                        gen_utime: 1,
                    };
                    first.first = bound;
                    first.last = bound;
                    let mut second = PartitionManager::empty_descriptor(PartitionId(2), ManifestLifecycle::Sealed, manager.manifest_epoch);
                    second.first = bound;
                    second.last = bound;
                    control_state.next_partition_id = 3;
                    records.extend([first, second]);
                }
                "id-gap" => {
                    control_state.next_partition_id = 4;
                    records.push(PartitionManager::empty_descriptor(PartitionId(3), ManifestLifecycle::Sealed, manager.manifest_epoch));
                }
                "range-order" => {
                    let newer = ManifestBound {
                        block_id: Some(block_id(2)),
                        mc_seqno: 2,
                        transaction_lt: 2,
                        gen_utime: 2,
                    };
                    let older = ManifestBound {
                        block_id: Some(block_id(1)),
                        mc_seqno: 1,
                        transaction_lt: 1,
                        gen_utime: 1,
                    };
                    first.first = newer;
                    first.last = newer;
                    let mut second = PartitionManager::empty_descriptor(PartitionId(2), ManifestLifecycle::Sealed, manager.manifest_epoch);
                    second.first = older;
                    second.last = older;
                    control_state.next_partition_id = 3;
                    records.extend([first, second]);
                }
                "lt-bounds" | "time-bounds" => {
                    first.first = ManifestBound {
                        block_id: Some(block_id(1)),
                        mc_seqno: 1,
                        transaction_lt: 2,
                        gen_utime: 2,
                    };
                    first.last = ManifestBound {
                        block_id: Some(block_id(1)),
                        mc_seqno: 1,
                        transaction_lt: if case == "lt-bounds" { 1 } else { 2 },
                        gen_utime: if case == "time-bounds" { 1 } else { 2 },
                    };
                    records.push(first);
                }
                "two-sealing" => {
                    control_state.next_partition_id = 4;
                    records.extend([
                        PartitionManager::empty_descriptor(PartitionId(2), ManifestLifecycle::Sealing, manager.manifest_epoch),
                        PartitionManager::empty_descriptor(PartitionId(3), ManifestLifecycle::Sealing, manager.manifest_epoch),
                    ]);
                }
                "creating-sealing" => {
                    control_state.next_partition_id = 4;
                    records.extend([
                        PartitionManager::empty_descriptor(PartitionId(2), ManifestLifecycle::Creating, manager.manifest_epoch),
                        PartitionManager::empty_descriptor(PartitionId(3), ManifestLifecycle::Sealing, manager.manifest_epoch),
                    ]);
                }
                _ => unreachable!(),
            }
            let mut batch = rocksdb::WriteBatch::default();
            batch.put_cf(&manager.control_db().state.cf(), codec::control_state_key(), codec::encode_control_state(control_state));
            for descriptor in records {
                batch.put_cf(&manager.control_db().manifests.cf(), codec::partition_manifest_key(descriptor.id.0), codec::encode_manifest(&descriptor.to_manifest()));
            }
            manager.control_db().rocksdb().write_opt(batch, manager.control_db().state.write_config()).unwrap();
            drop(manager);
            assert!(PartitionManager::open(context, config()).is_err(), "case {case} must fail startup validation");
        }
    }

    #[tokio::test]
    async fn creating_partition_resumes_and_replay_selection_uses_existing_range() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let mut manager = PartitionManager::open(context.clone(), config()).unwrap();
        let masterchain = masterchain_block_id(1);
        write_commit(manager.active_db(), 1, block_id(1), 1, 1, 1);
        write_commit(manager.active_db(), 1, masterchain, 0, 0, 0);
        manager.commit_masterchain_block_set(&masterchain).unwrap();
        drop(manager);
        let mut manager = PartitionManager::open(context.clone(), config()).unwrap();
        let creating = manager.begin_partition_creation().unwrap();
        drop(manager);
        let reopened = PartitionManager::open(context, config()).unwrap();
        assert_eq!(reopened.active_id(), creating);
        assert_eq!(reopened.visible_frontier(), Some(&masterchain));
        assert_eq!(reopened.select_partition(1), PartitionId::FIRST);
    }

    #[tokio::test]
    async fn creating_recovery_publishes_reconstructed_boundary_frontier() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let manager = PartitionManager::open(context.clone(), config()).unwrap();
        let masterchain = masterchain_block_id(10);
        write_commit(manager.active_db(), 10, block_id(1), 6, 0, 3);
        write_commit(manager.active_db(), 10, block_id(2), 6, 0, 3);
        write_commit(manager.active_db(), 10, masterchain, 0, 0, 0);
        let creating = {
            let mut manager = manager;
            manager.begin_partition_creation().unwrap()
        };

        let reopened = PartitionManager::open(context, config()).unwrap();

        assert_eq!(reopened.active_id(), creating);
        assert_eq!(reopened.visible_frontier(), Some(&masterchain));
        assert_eq!(reopened.select_partition(10), PartitionId::FIRST);
        assert_eq!(reopened.select_partition(11), creating);
        let old = reopened.descriptors.get(&PartitionId::FIRST).unwrap();
        assert_eq!(old.lifecycle, ManifestLifecycle::Sealing);
        assert_eq!(old.counters.estimated_lsm_bytes, 12);
        assert_eq!(old.counters.index_record_count, 6);
        assert_eq!(old.first.mc_seqno, 10);
        assert_eq!(old.last.mc_seqno, 10);
        assert_eq!(
            codec::decode_active_partition(
                reopened.control_db().state.get(codec::active_partition_key()).unwrap().unwrap().as_ref(),
            )
            .unwrap(),
            creating.0,
        );
        assert_eq!(
            codec::decode_visible_frontier(
                reopened.control_db().state.get(codec::visible_frontier_key()).unwrap().unwrap().as_ref(),
            )
            .unwrap(),
            masterchain,
        );
    }

    #[tokio::test]
    async fn creating_completion_reuses_open_old_partition_handle() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let mut manager = PartitionManager::open(context, config()).unwrap();
        let masterchain = masterchain_block_id(10);
        write_commit(manager.active_db(), 10, masterchain, 0, 0, 0);
        let creating = manager.begin_partition_creation().unwrap();

        manager.complete_partition_creation(creating).unwrap();

        assert_eq!(manager.active_id(), creating);
        assert_eq!(manager.visible_frontier(), Some(&masterchain));
    }

    #[tokio::test]
    async fn reconstructs_active_counters_from_partition_commits() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let manager = PartitionManager::open(context.clone(), config()).unwrap();
        let block_id = block_id(1);
        let commit = PartitionCommit {
            block_id,
            digest: block_id.root_hash,
            transaction_count: 2,
            estimated_lsm_bytes: 3,
            estimated_blob_bytes: 4,
            index_record_count: 5,
            start_lt: 6,
            end_lt: 7,
            gen_utime: 8,
        };
        let mut batch = rocksdb::WriteBatch::default();
        batch.put_cf(&manager.active_db().partition_commits.cf(), codec::partition_commit_key(9, &block_id.as_short_id()), codec::encode_partition_commit(&commit));
        manager.active_db().rocksdb().write_opt(batch, manager.active_db().partition_commits.write_config()).unwrap();
        drop(manager);
        let reopened = PartitionManager::open(context, config()).unwrap();
        let descriptor = reopened.descriptors().pop().unwrap();
        assert_eq!(descriptor.counters.transaction_count, 2);
        assert_eq!(descriptor.first.mc_seqno, 9);
        assert_eq!(descriptor.last.transaction_lt, 7);
    }

    #[tokio::test]
    async fn reconstruction_uses_mc_lt_time_extrema_and_preserves_transition() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let manager = PartitionManager::open(context.clone(), config()).unwrap();
        let transition = manager.descriptors()[0].last_transition;
        let commits = [
            (2, block_id(1), 90, 91, 9),
            (1, block_id(9), 50, 300, 100),
            (3, block_id(2), 10, 11, 1),
            (3, block_id(8), 1, 200, 20),
        ];
        let mut batch = rocksdb::WriteBatch::default();
        for (mc_seqno, block_id, start_lt, end_lt, gen_utime) in commits {
            let commit = PartitionCommit {
                block_id,
                digest: block_id.root_hash,
                transaction_count: 1,
                estimated_lsm_bytes: 1,
                estimated_blob_bytes: 1,
                index_record_count: 1,
                start_lt,
                end_lt,
                gen_utime,
            };
            batch.put_cf(&manager.active_db().partition_commits.cf(), codec::partition_commit_key(mc_seqno, &block_id.as_short_id()), codec::encode_partition_commit(&commit));
        }
        manager.active_db().rocksdb().write_opt(batch, manager.active_db().partition_commits.write_config()).unwrap();
        drop(manager);
        let reopened = PartitionManager::open(context, config()).unwrap();
        let descriptor = reopened.descriptors().pop().unwrap();
        assert_eq!(descriptor.first.mc_seqno, 1);
        assert_eq!(descriptor.first.transaction_lt, 1);
        assert_eq!(descriptor.first.gen_utime, 1);
        assert_eq!(descriptor.first.block_id, Some(block_id(9)));
        assert_eq!(descriptor.last.mc_seqno, 3);
        assert_eq!(descriptor.last.transaction_lt, 300);
        assert_eq!(descriptor.last.gen_utime, 100);
        assert_eq!(descriptor.last.block_id, Some(block_id(8)));
        assert_eq!(descriptor.last_transition, transition);
    }

    #[tokio::test]
    async fn rotation_request_is_sticky_and_single_sealer_defers_switch() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let mut manager = PartitionManager::open(context, config()).unwrap();
        assert_eq!(manager.request_rotation(PartitionCounters { estimated_lsm_bytes: 10, ..Default::default() }), Some(RotationReason::EstimatedLsmBytes));
        assert_eq!(manager.rotate_if_requested().unwrap().unwrap().0, PartitionId::FIRST);
        assert!(manager.begin_partition_creation().is_err());
        assert_eq!(manager.request_rotation(PartitionCounters { estimated_blob_bytes: 10, ..Default::default() }), Some(RotationReason::EstimatedBlobBytes));
        assert!(manager.rotate_if_requested().unwrap().is_none());
    }

    #[tokio::test]
    async fn activation_failure_restores_active_metrics_registration() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let mut manager = PartitionManager::open(context.clone(), config()).unwrap();
        let id = manager.begin_partition_creation().unwrap();
        let mut invalid_old = manager
            .descriptors
            .get(&PartitionId::FIRST)
            .unwrap()
            .clone();
        invalid_old.id = PartitionId(99);

        assert!(
            manager
                .activate_creating_partition(id, Some(invalid_old), None)
                .is_err()
        );
        assert_eq!(manager.active_id(), PartitionId::FIRST);
        assert_writable_metrics_registration(&context, &manager);
    }

    #[tokio::test]
    async fn rotation_failure_records_metrics_and_keeps_active_registration() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let mut manager = PartitionManager::open(context.clone(), config()).unwrap();
        let next_path = context
            .root_dir()
            .path()
            .join(manager.partition_subdir(PartitionId(2)));
        std::fs::write(next_path, []).unwrap();
        manager.request_rotation(PartitionCounters {
            estimated_lsm_bytes: 10,
            ..Default::default()
        });
        let recorder = TestMetricsRecorder::default();

        metrics::with_local_recorder(&recorder, || {
            assert!(manager.rotate_if_requested().is_err());
        });

        assert_eq!(
            recorder.counter("tycho_storage_rpc_partition_rotation_failures_total"),
            1
        );
        assert_eq!(
            recorder.histogram_len("tycho_storage_rpc_partition_rotation_time"),
            1
        );
        assert_writable_metrics_registration(&context, &manager);
    }

    #[tokio::test]
    async fn sealing_reopens_read_only_only_after_leases_drain() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let mut manager = PartitionManager::open(context.clone(), config()).unwrap();
        let masterchain = masterchain_block_id(10);
        write_commit(manager.active_db(), 10, masterchain, 10, 0, 0);
        manager.commit_masterchain_block_set(&masterchain).unwrap();
        assert_writable_metrics_registration(&context, &manager);
        let old = PartitionId::FIRST;
        let reader = manager.sealing_lease(old).unwrap();
        let worker = manager.begin_sealing(old).unwrap();
        assert!(!manager.sealing_external_leases_drained(old));
        assert!(manager.write_lease_for_mc_seqno(10).is_err());
        drop(reader);
        assert!(manager.sealing_external_leases_drained(old));
        let closed = manager.take_sealing_handle(old).unwrap();
        drop(closed);
        drop(worker);
        assert_eq!(manager.next_sealing_partition(), Some(old));
        let recovered = manager.begin_sealing(old).unwrap();
        assert_writable_metrics_registration(&context, &manager);
        let closed = manager.take_sealing_handle(old).unwrap();
        drop(closed);
        drop(recovered);
        let read_only = manager.open_sealed_read_only(old).unwrap();
        assert_writable_metrics_registration(&context, &manager);
        manager.complete_sealing(old, read_only).unwrap();
        assert_eq!(manager.descriptors.get(&old).unwrap().lifecycle, ManifestLifecycle::Sealed);
        assert!(manager.sealed_lease(old).unwrap().db().partition_commits.insert([1], [1]).is_err());
        assert_writable_metrics_registration(&context, &manager);
        drop(manager);

        let reopened = PartitionManager::open(context.clone(), config()).unwrap();
        assert_eq!(
            reopened.descriptors.get(&old).unwrap().lifecycle,
            ManifestLifecycle::Sealed
        );
        assert!(reopened.sealed_lease(old).unwrap().db().partition_commits.insert([2], [2]).is_err());
        assert_writable_metrics_registration(&context, &reopened);
    }

    #[tokio::test]
    async fn startup_keeps_sealing_partition_for_worker_retry() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let mut manager = PartitionManager::open(context.clone(), config()).unwrap();
        manager.request_rotation(PartitionCounters { estimated_lsm_bytes: 10, ..Default::default() });
        manager.rotate_if_requested().unwrap();
        drop(manager);

        let reopened = PartitionManager::open(context.clone(), config()).unwrap();

        assert_eq!(reopened.next_sealing_partition(), Some(PartitionId::FIRST));
        assert_eq!(reopened.descriptors.get(&PartitionId::FIRST).unwrap().lifecycle, ManifestLifecycle::Sealing);
        assert_writable_metrics_registration(&context, &reopened);
    }

    #[tokio::test]
    async fn block_set_rotation_persists_absolute_old_manifest_and_selects_new_partition() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let mut manager = PartitionManager::open(context.clone(), config()).unwrap();
        let masterchain = masterchain_block_id(10);
        write_commit(manager.active_db(), 10, block_id(1), 6, 0, 3);
        write_commit(manager.active_db(), 10, block_id(2), 6, 0, 3);
        write_commit(manager.active_db(), 10, masterchain, 0, 0, 0);

        manager.commit_masterchain_block_set(&masterchain).unwrap();

        assert_eq!(manager.active_id(), PartitionId(2));
        assert_eq!(manager.select_partition(10), PartitionId::FIRST);
        assert_eq!(manager.select_partition(11), PartitionId(2));
        assert_eq!(manager.visible_frontier(), Some(&masterchain));
        let old = manager.descriptors.get(&PartitionId::FIRST).unwrap();
        assert_eq!(old.lifecycle, ManifestLifecycle::Sealing);
        assert_eq!(old.counters.estimated_lsm_bytes, 12);
        assert_eq!(old.counters.index_record_count, 6);
        assert_eq!(old.first.mc_seqno, 10);
        assert_eq!(old.last.mc_seqno, 10);
        let active = manager.descriptors.get(&PartitionId(2)).unwrap();
        assert_eq!(active.lifecycle, ManifestLifecycle::Active);
        assert_eq!(active.counters, PartitionCounters::default());
        assert_eq!(
            codec::decode_active_partition(
                manager.control_db().state.get(codec::active_partition_key()).unwrap().unwrap().as_ref(),
            )
            .unwrap(),
            2,
        );
        assert_eq!(
            codec::decode_visible_frontier(
                manager.control_db().state.get(codec::visible_frontier_key()).unwrap().unwrap().as_ref(),
            )
            .unwrap(),
            masterchain,
        );
        for block_id in [block_id(1), block_id(2), masterchain] {
            let key = codec::partition_commit_key(10, &block_id.as_short_id());
            assert!(manager.sealing_lease(PartitionId::FIRST).unwrap().partition_commits.get(key).unwrap().is_some());
            assert!(manager.active_db().partition_commits.get(key).unwrap().is_none());
        }
        let next = masterchain_block_id(11);
        let (next_id, next_lease) = manager.write_lease_for_mc_seqno(11).unwrap();
        assert_eq!(next_id, PartitionId(2));
        write_commit(next_lease.db(), 11, next, 0, 0, 0);
        assert!(next_lease.partition_commits.get(codec::partition_commit_key(11, &next.as_short_id())).unwrap().is_some());
        assert_writable_metrics_registration(&context, &manager);
    }

    #[tokio::test]
    async fn observability_tracks_rotation_fixed_sealed_counters_and_cache() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let mut manager = PartitionManager::open(context.clone(), config()).unwrap();
        let recorder = TestMetricsRecorder::default();

        metrics::with_local_recorder(&recorder, || {
            manager.refresh_lifecycle_metrics();
            assert_eq!(recorder.gauge("tycho_storage_rpc_partition_active_id"), 1.0);
            assert_eq!(
                recorder.gauge("tycho_storage_rpc_partition_count|lifecycle=active"),
                1.0
            );
            assert_eq!(
                recorder.gauge(
                    "tycho_storage_rpc_partition_active_estimated_lsm_bytes"
                ),
                0.0
            );

            let first = masterchain_block_id(10);
            write_commit(manager.active_db(), 10, first, 10, 0, 0);
            manager.commit_masterchain_block_set(&first).unwrap();
            let sealed_counters = manager
                .descriptors
                .get(&PartitionId::FIRST)
                .unwrap()
                .counters;

            assert_eq!(recorder.gauge("tycho_storage_rpc_partition_active_id"), 2.0);
            assert_eq!(
                recorder.gauge("tycho_storage_rpc_partition_count|lifecycle=sealing"),
                1.0
            );
            assert_eq!(
                recorder.gauge(
                    "tycho_storage_rpc_partition_active_estimated_lsm_bytes"
                ),
                0.0
            );
            assert_eq!(
                recorder.counter(
                    "tycho_storage_rpc_partition_rotations_total|reason=estimated_lsm_bytes"
                ),
                1
            );
            assert_eq!(
                recorder.histogram_len("tycho_storage_rpc_partition_rotation_time"),
                1
            );

            let second = masterchain_block_id(11);
            write_commit(manager.active_db(), 11, second, 10, 2, 3);
            manager.commit_masterchain_block_set(&second).unwrap();
            assert_eq!(
                recorder.counter(
                    "tycho_storage_rpc_partition_rotation_deferred_total"
                ),
                1
            );
            assert_eq!(
                recorder.gauge(
                    "tycho_storage_rpc_partition_active_estimated_lsm_bytes"
                ),
                10.0
            );
            assert_eq!(
                recorder.gauge(
                    "tycho_storage_rpc_partition_active_estimated_blob_bytes"
                ),
                2.0
            );
            assert_eq!(
                recorder.gauge(
                    "tycho_storage_rpc_partition_active_transaction_count"
                ),
                1.0
            );
            assert_eq!(
                recorder.gauge(
                    "tycho_storage_rpc_partition_active_index_record_count"
                ),
                3.0
            );
            assert_eq!(
                recorder.gauge("tycho_storage_rpc_partition_manifest_epoch"),
                manager.manifest_epoch as f64
            );
            assert_eq!(
                recorder.gauge("tycho_storage_rpc_partition_visible_mc_seqno"),
                11.0
            );
            assert_eq!(
                manager
                    .descriptors
                    .get(&PartitionId::FIRST)
                    .unwrap()
                    .counters,
                sealed_counters
            );

            let worker = manager.begin_sealing(PartitionId::FIRST).unwrap();
            let closed = manager.take_sealing_handle(PartitionId::FIRST).unwrap();
            drop(closed);
            drop(worker);
            let read_only = manager
                .open_sealed_read_only(PartitionId::FIRST)
                .unwrap();
            manager
                .complete_sealing(PartitionId::FIRST, read_only)
                .unwrap();
            assert_eq!(
                recorder.gauge("tycho_storage_rpc_partition_count|lifecycle=sealing"),
                0.0
            );
            assert_eq!(
                recorder.gauge("tycho_storage_rpc_partition_count|lifecycle=sealed"),
                1.0
            );
            assert_eq!(
                manager
                    .descriptors
                    .get(&PartitionId::FIRST)
                    .unwrap()
                    .counters,
                sealed_counters
            );

            manager.sealed_cache.invalidate(&PartitionId::FIRST);
            let miss = manager.sealed_lease(PartitionId::FIRST).unwrap();
            let hit = manager.sealed_lease(PartitionId::FIRST).unwrap();
            assert_eq!(
                recorder.counter(
                    "tycho_storage_rpc_partition_sealed_cache_misses_total"
                ),
                1
            );
            assert_eq!(
                recorder.counter(
                    "tycho_storage_rpc_partition_sealed_cache_opens_total"
                ),
                1
            );
            assert_eq!(
                recorder.counter(
                    "tycho_storage_rpc_partition_sealed_cache_hits_total"
                ),
                1
            );
            assert_eq!(
                recorder.histogram_len(
                    "tycho_storage_rpc_partition_sealed_cache_open_time"
                ),
                1
            );
            assert!(
                recorder.histogram_len("tycho_storage_rpc_write_control_time") > 0
            );
            assert!(
                recorder
                    .keys()
                    .iter()
                    .all(|key| !key.contains("|partition_id="))
            );
            drop(hit);
            drop(miss);
        });

        assert_writable_metrics_registration(&context, &manager);
    }

    #[tokio::test]
    async fn block_set_replay_does_not_double_count_or_repartition() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let mut manager = PartitionManager::open(context, config()).unwrap();
        let masterchain = masterchain_block_id(10);
        write_commit(manager.active_db(), 10, block_id(1), 10, 0, 0);
        write_commit(manager.active_db(), 10, masterchain, 0, 0, 0);
        manager.commit_masterchain_block_set(&masterchain).unwrap();
        let original = manager.descriptors.get(&PartitionId::FIRST).unwrap().clone();

        manager.commit_masterchain_block_set(&masterchain).unwrap();

        assert_eq!(manager.active_id(), PartitionId(2));
        assert_eq!(manager.select_partition(10), PartitionId::FIRST);
        assert_eq!(manager.descriptors.get(&PartitionId::FIRST).unwrap().counters, original.counters);
    }

    #[tokio::test]
    async fn newer_boundary_aggregation_scans_only_unpersisted_range() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let mut manager = PartitionManager::open(context, config()).unwrap();
        let first = masterchain_block_id(1);
        write_commit(manager.active_db(), 1, block_id(1), 3, 0, 3);
        write_commit(manager.active_db(), 1, first, 0, 0, 0);
        manager.commit_masterchain_block_set(&first).unwrap();
        let persisted = manager.descriptors.get(&PartitionId::FIRST).unwrap().clone();
        let transition = persisted.last_transition;
        let second = masterchain_block_id(2);
        write_commit(manager.active_db(), 2, block_id(2), 4, 2, 3);
        write_commit(manager.active_db(), 2, second, 0, 0, 0);
        let third = masterchain_block_id(3);
        write_commit(manager.active_db(), 3, block_id(3), 5, 0, 3);
        write_commit(manager.active_db(), 3, third, 0, 0, 0);

        let (unchanged, scanned) = PartitionManager::aggregate_commits_after_descriptor(
            manager.active_db(),
            &persisted,
            1,
        )
        .unwrap();
        assert_eq!(scanned, 0);
        assert_eq!(unchanged, persisted);
        let (expected, scanned) = PartitionManager::aggregate_commits_after_descriptor(
            manager.active_db(),
            &persisted,
            2,
        )
        .unwrap();
        assert_eq!(scanned, 2);
        assert_eq!(expected.counters.estimated_lsm_bytes, 7);
        assert_eq!(expected.counters.estimated_blob_bytes, 2);
        assert_eq!(expected.counters.transaction_count, 2);
        assert_eq!(expected.counters.index_record_count, 6);
        assert_eq!(expected.last.mc_seqno, 2);
        assert_eq!(expected.last_transition, transition);

        manager.commit_masterchain_block_set(&second).unwrap();

        assert_eq!(manager.active_id(), PartitionId::FIRST);
        assert_eq!(manager.descriptors.get(&PartitionId::FIRST).unwrap(), &expected);
        manager.commit_masterchain_block_set(&third).unwrap();
        assert_eq!(manager.active_id(), PartitionId(2));
        let sealed = manager.descriptors.get(&PartitionId::FIRST).unwrap();
        assert_eq!(sealed.counters.estimated_lsm_bytes, 12);
        assert_eq!(sealed.last.mc_seqno, 3);
    }

    #[tokio::test]
    async fn startup_recovery_reconstructs_same_seq_commits_added_after_open() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let manager = PartitionManager::open(context.clone(), config()).unwrap();
        write_commit(manager.active_db(), 2, block_id(1), 6, 0, 2);
        drop(manager);
        let mut manager = PartitionManager::open(context, config()).unwrap();
        assert_eq!(manager.descriptors.get(&PartitionId::FIRST).unwrap().counters.estimated_lsm_bytes, 6);
        assert_eq!(manager.visible_frontier(), None);
        assert_eq!(manager.select_partition(2), PartitionId::FIRST);
        let masterchain = masterchain_block_id(2);
        write_commit(manager.active_db(), 2, block_id(2), 4, 0, 2);
        write_commit(manager.active_db(), 2, masterchain, 0, 0, 0);

        manager.commit_masterchain_block_set(&masterchain).unwrap();

        assert_eq!(manager.active_id(), PartitionId(2));
        assert_eq!(manager.visible_frontier(), Some(&masterchain));
        assert_eq!(manager.select_partition(2), PartitionId::FIRST);
        assert_eq!(manager.select_partition(3), PartitionId(2));
        let old = manager.descriptors.get(&PartitionId::FIRST).unwrap();
        assert_eq!(old.lifecycle, ManifestLifecycle::Sealing);
        assert_eq!(old.counters.estimated_lsm_bytes, 10);
        assert_eq!(old.counters.transaction_count, 2);
        assert_eq!(old.last.mc_seqno, 2);
    }

    #[tokio::test]
    async fn startup_recovered_future_commits_defer_rotation_until_last_boundary() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let manager = PartitionManager::open(context.clone(), config()).unwrap();
        let first = masterchain_block_id(1);
        write_commit(manager.active_db(), 1, block_id(1), 6, 0, 2);
        write_commit(manager.active_db(), 1, first, 0, 0, 0);
        let second = masterchain_block_id(2);
        write_commit(manager.active_db(), 2, block_id(2), 6, 0, 2);
        write_commit(manager.active_db(), 2, second, 0, 0, 0);
        drop(manager);
        let mut manager = PartitionManager::open(context, config()).unwrap();
        assert_eq!(manager.descriptors.get(&PartitionId::FIRST).unwrap().last.mc_seqno, 2);
        assert_eq!(manager.descriptors.get(&PartitionId::FIRST).unwrap().counters.estimated_lsm_bytes, 12);

        manager.commit_masterchain_block_set(&first).unwrap();

        assert_eq!(manager.active_id(), PartitionId::FIRST);
        assert_eq!(manager.visible_frontier(), Some(&first));
        assert_eq!(manager.rotation_requested(), None);
        manager.commit_masterchain_block_set(&second).unwrap();
        assert_eq!(manager.active_id(), PartitionId(2));
        assert_eq!(manager.visible_frontier(), Some(&second));
        assert_eq!(manager.select_partition(2), PartitionId::FIRST);
        assert_eq!(manager.select_partition(3), PartitionId(2));
    }

    #[tokio::test]
    async fn older_replay_preserves_newer_frontier_and_equal_seqno_requires_full_id() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let mut manager = PartitionManager::open(context, config()).unwrap();
        let first = masterchain_block_id(10);
        write_commit(manager.active_db(), 10, first, 10, 0, 0);
        manager.commit_masterchain_block_set(&first).unwrap();
        let next = masterchain_block_id(11);
        write_commit(manager.active_db(), 11, next, 0, 0, 0);
        manager.commit_masterchain_block_set(&next).unwrap();
        let epoch = manager.manifest_epoch;

        manager.commit_masterchain_block_set(&first).unwrap();

        assert_eq!(manager.visible_frontier(), Some(&next));
        assert_eq!(manager.manifest_epoch, epoch);
        let mut different = next;
        different.root_hash = HashBytes([42; 32]);
        assert!(manager.commit_masterchain_block_set(&different).is_err());
        assert_eq!(manager.visible_frontier(), Some(&next));
    }

    #[tokio::test]
    async fn every_threshold_rotates_at_the_completed_block_set_boundary() {
        for (reason, lsm, blob, records) in [
            (RotationReason::EstimatedLsmBytes, 10, 0, 0),
            (RotationReason::EstimatedBlobBytes, 0, 10, 0),
            (RotationReason::IndexRecordCount, 0, 0, 10),
        ] {
            let (context, _tmp) = StorageContext::new_temp().await.unwrap();
            let mut manager = PartitionManager::open(context, config()).unwrap();
            let masterchain = masterchain_block_id(10);
            write_commit(manager.active_db(), 10, masterchain, lsm, blob, records);
            assert_eq!(
                manager.request_rotation(PartitionCounters::default()),
                None,
                "{reason:?} must not rotate before the boundary",
            );

            manager.commit_masterchain_block_set(&masterchain).unwrap();

            assert_eq!(manager.active_id(), PartitionId(2), "{reason:?}");
            assert_eq!(manager.select_partition(10), PartitionId::FIRST, "{reason:?}");
            assert_eq!(manager.select_partition(11), PartitionId(2), "{reason:?}");
        }
    }

    #[tokio::test]
    async fn threshold_or_semantics_rotate_after_one_block_set_overshoot() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let mut manager = PartitionManager::open(context, config()).unwrap();
        assert_eq!(
            manager.request_rotation(PartitionCounters {
                estimated_lsm_bytes: 9,
                estimated_blob_bytes: 10,
                index_record_count: 9,
                ..Default::default()
            }),
            Some(RotationReason::EstimatedBlobBytes)
        );
        manager.rotation_requested = None;
        let masterchain = masterchain_block_id(10);
        write_commit(manager.active_db(), 10, block_id(1), 6, 6, 6);
        write_commit(manager.active_db(), 10, block_id(2), 6, 6, 6);
        write_commit(manager.active_db(), 10, masterchain, 0, 0, 0);

        assert_eq!(manager.active_id(), PartitionId::FIRST);
        manager.commit_masterchain_block_set(&masterchain).unwrap();

        assert_eq!(manager.active_id(), PartitionId(2));
        let closed = manager.descriptors.get(&PartitionId::FIRST).unwrap();
        assert_eq!(closed.counters.estimated_lsm_bytes, 12);
        assert_eq!(closed.counters.estimated_blob_bytes, 12);
        assert_eq!(closed.counters.index_record_count, 12);
    }

    #[tokio::test]
    async fn empty_block_set_does_not_request_rotation_and_sealing_defers_next_switch() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let mut manager = PartitionManager::open(context, config()).unwrap();
        for seqno in 1..=3 {
            let empty = masterchain_block_id(seqno);
            write_commit(manager.active_db(), seqno, empty, 0, 0, 0);
            manager.commit_masterchain_block_set(&empty).unwrap();
            assert_eq!(manager.active_id(), PartitionId::FIRST);
            assert_eq!(manager.rotation_requested(), None);
            assert_eq!(manager.visible_frontier(), Some(&empty));
        }

        let first = masterchain_block_id(4);
        write_commit(manager.active_db(), 4, first, 10, 10, 10);
        manager.commit_masterchain_block_set(&first).unwrap();
        assert_eq!(manager.active_id(), PartitionId(2));
        let second = masterchain_block_id(5);
        write_commit(manager.active_db(), 5, second, 10, 0, 0);
        manager.commit_masterchain_block_set(&second).unwrap();
        assert_eq!(manager.active_id(), PartitionId(2));
        assert_eq!(manager.rotation_requested(), Some(RotationReason::EstimatedLsmBytes));
        assert_eq!(manager.visible_frontier(), Some(&second));
    }

    #[tokio::test]
    async fn cache_eviction_does_not_invalidate_lease() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let manager = PartitionManager::open(context.clone(), config()).unwrap();
        let second = PartitionId(2);
        let third = PartitionId(3);
        for id in [second, third] {
            let _: RpcTransactionsDb = context.open_preconfigured(manager.partition_subdir(id)).unwrap();
        }
        let mut control_state = manager.control_state;
        control_state.next_partition_id = 4;
        let second_descriptor = PartitionManager::empty_descriptor(second, ManifestLifecycle::Sealed, manager.manifest_epoch);
        let third_descriptor = PartitionManager::empty_descriptor(third, ManifestLifecycle::Sealed, manager.manifest_epoch);
        let mut batch = rocksdb::WriteBatch::default();
        batch.put_cf(&manager.control_db().state.cf(), codec::control_state_key(), codec::encode_control_state(control_state));
        batch.put_cf(&manager.control_db().manifests.cf(), codec::partition_manifest_key(second.0), codec::encode_manifest(&second_descriptor.to_manifest()));
        batch.put_cf(&manager.control_db().manifests.cf(), codec::partition_manifest_key(third.0), codec::encode_manifest(&third_descriptor.to_manifest()));
        manager.control_db().rocksdb().write_opt(batch, manager.control_db().state.write_config()).unwrap();
        drop(manager);
        let manager = PartitionManager::open(context, config()).unwrap();
        let lease = manager.sealed_lease(second).unwrap();
        let third_lease = manager.sealed_lease(third).unwrap();
        manager.sealed_cache.run_pending_tasks();
        assert!(manager.sealed_cache.entry_count() <= 1);
        assert!(lease.db().partition_commits.get([0]).is_ok());
        assert!(lease.db().partition_commits.insert([1], [1]).is_err());
        assert!(third_lease.db().partition_commits.get([0]).is_ok());
    }
}
