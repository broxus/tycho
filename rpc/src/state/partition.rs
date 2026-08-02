use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use moka::sync::Cache;
use tycho_storage::StorageContext;
use tycho_storage::kv::{InstanceId, NamedTables};
use tycho_types::prelude::HashBytes;
use weedb::rocksdb;

use crate::config::{RpcTransactionPartitionsConfig, TransactionsGcConfig};

use super::codec::{
    self, ControlState, GcIntent, GcIntentPhase, ManifestBound, ManifestLifecycle,
    ManifestTransition, PartitionManifest, TailIdentity, TailLayoutVersion,
};
use super::db::{
    RpcControlDb, RpcCurrentStateDb, RpcTransactionsDb,
    RpcTransactionsTables,
};
use super::tail::{
    conflicting_authoritative_error, malformed_authoritative_error,
    missing_authoritative_error,
};

const RPC_ROOT: &str = "rpc";
const CONTROL_SUBDIR: &str = "rpc/control";
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
    EstimatedTransactionLsmBytes,
    EstimatedTransactionBlobBytes,
    TransactionIndexRecordCount,
    EstimatedBlockMetadataBytes,
}

impl RotationReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::EstimatedTransactionLsmBytes => "estimated_transaction_lsm_bytes",
            Self::EstimatedTransactionBlobBytes => "estimated_transaction_blob_bytes",
            Self::TransactionIndexRecordCount => "transaction_index_record_count",
            Self::EstimatedBlockMetadataBytes => "estimated_block_metadata_bytes",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PartitionCounters {
    pub estimated_transaction_lsm_bytes: u64,
    pub estimated_transaction_blob_bytes: u64,
    pub transaction_count: u64,
    pub transaction_index_record_count: u64,
    pub estimated_block_metadata_bytes: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct PartitionManifestMetrics {
    active_id: u64,
    creating_count: u64,
    active_count: u64,
    sealing_count: u64,
    sealed_count: u64,
    retired_count: u64,
    deleting_count: u64,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct GcCutoverResult {
    pub(super) intent: GcIntent,
    pub(super) source_partition_id: PartitionId,
    pub(super) visible_generation: u64,
    pub(super) smallest_known_lt: u64,
    pub(super) manifest_epoch: u64,
}

impl PartitionDescriptor {
    fn from_manifest(value: PartitionManifest) -> Self {
        Self {
            id: PartitionId(value.partition_id),
            lifecycle: value.lifecycle,
            first: value.first,
            last: value.last,
            counters: PartitionCounters {
                estimated_transaction_lsm_bytes: value.estimated_transaction_lsm_bytes,
                estimated_transaction_blob_bytes: value.estimated_transaction_blob_bytes,
                transaction_count: value.transaction_count,
                transaction_index_record_count: value.transaction_index_record_count,
                estimated_block_metadata_bytes: value.estimated_block_metadata_bytes,
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
            estimated_transaction_lsm_bytes: self.counters.estimated_transaction_lsm_bytes,
            estimated_transaction_blob_bytes: self.counters.estimated_transaction_blob_bytes,
            transaction_index_record_count: self.counters.transaction_index_record_count,
            estimated_block_metadata_bytes: self.counters.estimated_block_metadata_bytes,
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

struct PartitionLifetimeInner {
    id: PartitionId,
}

struct PartitionLifetimeOwner(Arc<PartitionLifetimeInner>);

#[derive(Clone)]
pub(super) struct PartitionLifetimeToken(Arc<PartitionLifetimeInner>);

impl PartitionLifetimeOwner {
    fn new(id: PartitionId) -> Self {
        Self(Arc::new(PartitionLifetimeInner { id }))
    }

    fn token(&self) -> PartitionLifetimeToken {
        PartitionLifetimeToken(self.0.clone())
    }

    fn only_owner_remains(&self) -> bool {
        Arc::strong_count(&self.0) == 1
    }
}

impl PartitionLifetimeToken {
    fn id(&self) -> PartitionId {
        self.0.id
    }

    #[cfg(test)]
    pub(super) fn for_test(id: PartitionId) -> Self {
        PartitionLifetimeOwner::new(id).token()
    }
}

struct CachedSealedPartition {
    db: Arc<RpcTransactionsDb>,
    lifetime: PartitionLifetimeToken,
}

pub(super) struct PartitionDeletionGuard {
    id: PartitionId,
    path: PathBuf,
    _lifetime: PartitionLifetimeToken,
}

impl PartitionDeletionGuard {
    pub(super) fn id(&self) -> PartitionId {
        self.id
    }

    pub(super) fn path(&self) -> &Path {
        &self.path
    }
}

/// Owns an `Arc` DB handle so an iterator remains valid after cache eviction or sealing starts.
#[derive(Clone)]
pub struct PartitionReadLease {
    db: Arc<RpcTransactionsDb>,
    lifecycle: ManifestLifecycle,
    lifetime: PartitionLifetimeToken,
}

impl PartitionReadLease {
    pub fn db(&self) -> &RpcTransactionsDb {
        &self.db
    }

    pub fn lifecycle(&self) -> ManifestLifecycle {
        self.lifecycle
    }

    pub(super) fn lifetime_token(&self) -> PartitionLifetimeToken {
        self.lifetime.clone()
    }
}

impl Deref for PartitionReadLease {
    type Target = RpcTransactionsDb;

    fn deref(&self) -> &Self::Target {
        self.db()
    }
}

/// Opens an immutable partition after the manager lock has been released.
#[derive(Clone)]
pub struct SealedPartitionLeaseOpener {
    id: PartitionId,
    context: StorageContext,
    subdir: PathBuf,
    cache: Cache<PartitionId, Arc<CachedSealedPartition>>,
    lifetime: PartitionLifetimeToken,
    #[cfg(test)]
    post_open_hook: Option<Arc<dyn Fn(&Path) + Send + Sync>>,
}

/// Opens a sealed partition for one maintenance job without using the request cache.
pub(super) struct MaintenanceSealedPartitionOpener {
    id: PartitionId,
    context: StorageContext,
    subdir: PathBuf,
    lifetime: PartitionLifetimeToken,
    #[cfg(test)]
    post_open_hook: Option<Arc<dyn Fn(&Path) + Send + Sync>>,
}

pub(super) struct SealingReadOnlyHandle {
    id: PartitionId,
    db: Arc<RpcTransactionsDb>,
    lifetime: PartitionLifetimeToken,
}

fn validate_committed_partition_directory(path: &Path, id: PartitionId) -> Result<()> {
    let missing = || {
        missing_authoritative_error(
            "committed RPC transaction partition directory is missing or not a directory",
        )
        .context(format!("transaction partition {} at {}", id.0, path.display()))
    };
    match fs::metadata(path) {
        Ok(metadata) if metadata.is_dir() => Ok(()),
        Ok(_) => Err(missing()),
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
            ) =>
        {
            Err(missing())
        }
        Err(error) => Err(error).with_context(|| {
            format!(
                "failed to inspect committed RPC transaction partition {} directory {}",
                id.0,
                path.display(),
            )
        }),
    }
}

impl MaintenanceSealedPartitionOpener {
    pub(super) fn open(self) -> Result<PartitionReadLease> {
        let path = self.context.root_dir().path().join(&self.subdir);
        validate_committed_partition_directory(&path, self.id)?;
        let db = match self.context.open_read_only(self.subdir) {
            Ok(db) => db,
            Err(error) => {
                validate_committed_partition_directory(&path, self.id)?;
                return Err(error);
            }
        };
        #[cfg(test)]
        if let Some(hook) = &self.post_open_hook {
            hook(&path);
        }
        validate_committed_partition_directory(&path, self.id)?;
        Ok(PartitionReadLease {
            db: Arc::new(db),
            lifecycle: ManifestLifecycle::Sealed,
            lifetime: self.lifetime,
        })
    }
}

impl SealedPartitionLeaseOpener {
    pub fn open(&self) -> Result<PartitionReadLease> {
        let cached = match self.cache.get(&self.id) {
            Some(cached) => {
                metrics::counter!("tycho_storage_rpc_partition_sealed_cache_hits_total")
                    .increment(1);
                cached
            }
            None => {
                metrics::counter!("tycho_storage_rpc_partition_sealed_cache_misses_total")
                    .increment(1);
                let path = self.context.root_dir().path().join(&self.subdir);
                validate_committed_partition_directory(&path, self.id)?;
                let started_at = Instant::now();
                let result = self.context.open_read_only(&self.subdir);
                metrics::histogram!("tycho_storage_rpc_partition_sealed_cache_open_time")
                    .record(started_at.elapsed());
                let db: Arc<RpcTransactionsDb> = match result {
                    Ok(db) => Arc::new(db),
                    Err(error) => {
                        validate_committed_partition_directory(&path, self.id)?;
                        return Err(error);
                    }
                };
                #[cfg(test)]
                if let Some(hook) = &self.post_open_hook {
                    hook(&path);
                }
                validate_committed_partition_directory(&path, self.id)?;
                metrics::counter!("tycho_storage_rpc_partition_sealed_cache_opens_total")
                    .increment(1);
                let cached = Arc::new(CachedSealedPartition {
                    db,
                    lifetime: self.lifetime.clone(),
                });
                self.cache.insert(self.id, cached.clone());
                cached
            }
        };
        Ok(PartitionReadLease {
            db: cached.db.clone(),
            lifecycle: ManifestLifecycle::Sealed,
            lifetime: cached.lifetime.clone(),
        })
    }
}

pub struct PartitionManager {
    context: StorageContext,
    config: RpcTransactionPartitionsConfig,
    root: PathBuf,
    control: RpcControlDb,
    current_state: RpcCurrentStateDb,
    control_state: ControlState,
    visible_frontier: Option<tycho_types::models::BlockId>,
    manifest_epoch: u64,
    descriptors: BTreeMap<PartitionId, PartitionDescriptor>,
    active: Option<Arc<RpcTransactionsDb>>,
    active_id: PartitionId,
    sealing: BTreeMap<PartitionId, Arc<RpcTransactionsDb>>,
    closing: BTreeSet<PartitionId>,
    lifetime_owners: BTreeMap<PartitionId, PartitionLifetimeOwner>,
    sealed_cache: Cache<PartitionId, Arc<CachedSealedPartition>>,
    rotation_requested: Option<RotationReason>,
    sealer_busy: bool,
    #[cfg(test)]
    fail_next_creation_activation: bool,
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

        let rpc_root = root.join(RPC_ROOT);
        let root_was_nonempty = rpc_root
            .try_exists()?
            && fs::read_dir(&rpc_root)?.next().transpose()?.is_some();

        let control: RpcControlDb = context.open_preconfigured(CONTROL_SUBDIR)?;
        match control.state.get(codec::rpc_layout_marker_key())? {
            Some(value) => codec::decode_rpc_layout_marker(value.as_ref())?,
            None if root_was_nonempty => bail!(
                "unsupported pre-V2 RPC layout detected at {}; clear the RPC DB and perform a full reindex",
                rpc_root.display()
            ),
            None => {}
        }
        let current_state: RpcCurrentStateDb = context.open_preconfigured(CURRENT_STATE_SUBDIR)?;
        let sealed_cache = Cache::builder()
            .max_capacity(config.max_open_sealed_partitions as u64)
            .build();

        let control_state = match control.state.get(codec::control_state_key())? {
            Some(value) => codec::decode_control_state(value.as_ref())?,
            None => ControlState {
                node_instance_id: rand::random::<InstanceId>(),
                next_partition_id: PartitionId::FIRST.0,
                smallest_known_lt: u64::MAX,
                tail_visible_generation: 0,
                tail_layout_version: TailLayoutVersion::MonolithicV1,
                removed_through_partition_id: 0,
            },
        };

        let mut manager = Self {
            context,
            config,
            root,
            control,
            current_state,
            control_state,
            visible_frontier: None,
            manifest_epoch: 0,
            descriptors: BTreeMap::new(),
            active: None,
            active_id: PartitionId::FIRST,
            sealing: BTreeMap::new(),
            closing: BTreeSet::new(),
            lifetime_owners: BTreeMap::new(),
            sealed_cache,
            rotation_requested: None,
            sealer_busy: false,
            #[cfg(test)]
            fail_next_creation_activation: false,
        };
        manager.load_or_bootstrap()?;
        manager.refresh_lifecycle_metrics();
        Ok(manager)
    }

    #[cfg(test)]
    pub fn control_db(&self) -> &RpcControlDb {
        &self.control
    }

    pub fn current_state_db(&self) -> &RpcCurrentStateDb {
        &self.current_state
    }

    pub(super) fn tail_identity(&self) -> TailIdentity {
        TailIdentity {
            layout_version: self.control_state.tail_layout_version,
            node_instance_id: self.control_state.node_instance_id,
        }
    }

    pub(super) fn tail_visible_generation(&self) -> u64 {
        self.control_state.tail_visible_generation
    }

    pub(super) fn tail_layout_version(&self) -> TailLayoutVersion {
        self.control_state.tail_layout_version
    }

    pub(super) fn removed_through_partition_id(&self) -> u64 {
        self.control_state.removed_through_partition_id
    }

    pub(super) fn gc_intent(&self) -> Result<Option<GcIntent>> {
        self.control
            .state
            .get(codec::gc_intent_key())?
            .map(|bytes| {
                codec::decode_gc_intent(bytes.as_ref()).map_err(|_| {
                    malformed_authoritative_error("malformed committed RPC transaction GC intent")
                })
            })
            .transpose()
    }

    fn validate_pre_cutover_gc_intent(&self, intent: GcIntent) -> Result<()> {
        if !matches!(intent.phase, GcIntentPhase::Evacuating | GcIntentPhase::Prepared) {
            return Ok(());
        }
        if intent.previous_visible_generation != self.control_state.tail_visible_generation {
            return Err(conflicting_authoritative_error(
                "RPC transaction GC intent previous generation conflicts with control state",
            ));
        }
        let source = self
            .descriptors
            .get(&PartitionId(intent.source_partition_id))
            .ok_or_else(|| {
                missing_authoritative_error("committed RPC transaction GC source partition is missing")
            })?;
        if source.lifecycle != ManifestLifecycle::Sealed {
            return Err(conflicting_authoritative_error(
                "RPC transaction GC pre-cutover source is not sealed",
            ));
        }
        let oldest_sealed = self
            .descriptors
            .values()
            .find(|descriptor| descriptor.lifecycle == ManifestLifecycle::Sealed)
            .ok_or_else(|| {
                missing_authoritative_error("RPC transaction GC intent has no sealed source")
            })?;
        if oldest_sealed.id != source.id {
            return Err(conflicting_authoritative_error(
                "RPC transaction GC source is not the oldest sealed partition",
            ));
        }
        if source.last.gen_utime >= intent.cutoff_utime {
            return Err(conflicting_authoritative_error(
                "RPC transaction GC source is not strictly older than its fixed cutoff",
            ));
        }
        let source_manifest_digest = self.sealed_manifest_digest(source.id).map_err(|_| {
            malformed_authoritative_error("malformed committed RPC transaction GC source manifest")
        })?;
        if source_manifest_digest != intent.source_manifest_digest {
            return Err(conflicting_authoritative_error(
                "RPC transaction GC source manifest digest mismatch",
            ));
        }
        let source_path = self.root.join(Self::partition_subdir(source.id));
        match fs::metadata(&source_path) {
            Ok(metadata) if metadata.is_dir() => {}
            Ok(_) => {
                return Err(missing_authoritative_error(
                    "RPC transaction GC pre-cutover source directory is missing or not a directory",
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(missing_authoritative_error(
                    "RPC transaction GC pre-cutover source directory is missing or not a directory",
                ));
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "failed to inspect RPC transaction GC pre-cutover source directory {}",
                        source_path.display(),
                    )
                });
            }
        }
        Ok(())
    }

    pub(super) fn begin_gc_intent(
        &mut self,
        config: Option<&TransactionsGcConfig>,
    ) -> Result<Option<GcIntent>> {
        if let Some(intent) = self.gc_intent()? {
            self.validate_pre_cutover_gc_intent(intent)?;
            return Ok(Some(intent));
        }
        let Some(config) = config else {
            return Ok(None);
        };
        let Some(source_id) = self.gc_source_for_new_intent(config)? else {
            return Ok(None);
        };
        let frontier = self.visible_frontier.context("RPC transaction GC requires an effective masterchain frontier")?;
        self.begin_new_gc_intent_at_frontier(config, source_id, &frontier)
    }

    pub(super) fn begin_gc_intent_at_frontier(
        &mut self,
        config: Option<&TransactionsGcConfig>,
        fixed_frontier: &tycho_types::models::BlockId,
    ) -> Result<Option<GcIntent>> {
        if let Some(intent) = self.gc_intent()? {
            self.validate_pre_cutover_gc_intent(intent)?;
            return Ok(Some(intent));
        }
        let Some(config) = config else {
            return Ok(None);
        };
        let Some(source_id) = self.gc_source_for_new_intent(config)? else {
            return Ok(None);
        };
        self.begin_new_gc_intent_at_frontier(config, source_id, fixed_frontier)
    }

    fn gc_source_for_new_intent(
        &self,
        config: &TransactionsGcConfig,
    ) -> Result<Option<PartitionId>> {
        config.validate().map_err(anyhow::Error::msg)?;
        Ok(self
            .descriptors
            .values()
            .find(|descriptor| descriptor.lifecycle == ManifestLifecycle::Sealed)
            .map(|descriptor| descriptor.id))
    }

    fn begin_new_gc_intent_at_frontier(
        &mut self,
        config: &TransactionsGcConfig,
        source_id: PartitionId,
        fixed_frontier: &tycho_types::models::BlockId,
    ) -> Result<Option<GcIntent>> {
        let frontier_commit = self.load_masterchain_commit(fixed_frontier)?;
        let ttl_seconds = config.tx_ttl.as_secs();
        let cutoff_utime = u32::try_from(
            u64::from(frontier_commit.gen_utime).saturating_sub(ttl_seconds),
        )
        .expect("saturating transaction GC cutoff fits u32");
        let source = self.descriptors.get(&source_id).unwrap();
        if source.last.gen_utime >= cutoff_utime {
            return Ok(None);
        }
        let previous_visible_generation = self.control_state.tail_visible_generation;
        let target_generation = previous_visible_generation.checked_add(1).context("RPC tail generation overflow")?;
        let keep_tx_per_account = u64::try_from(config.keep_tx_per_account).context("RPC transaction GC keep count overflow")?;
        let operation_id = loop {
            let operation_id = rand::random::<u128>();
            if operation_id != 0 {
                break operation_id;
            }
        };
        let intent = GcIntent {
            phase: GcIntentPhase::Evacuating,
            operation_id,
            source_partition_id: source_id.0,
            source_manifest_digest: self.sealed_manifest_digest(source_id)?,
            target_generation,
            previous_visible_generation,
            cutoff_utime,
            keep_tx_per_account,
            retention_policy_digest: codec::retention_policy_digest(ttl_seconds, keep_tx_per_account),
        };
        let mut batch = rocksdb::WriteBatch::default();
        batch.put_cf(&self.control.state.cf(), codec::gc_intent_key(), codec::encode_gc_intent(&intent)?);
        self.write_control(batch)?;
        Ok(Some(intent))
    }

    pub(super) fn transition_gc_intent_to_prepared(
        &mut self,
        expected_evacuating: &GcIntent,
    ) -> Result<GcIntent> {
        ensure!(expected_evacuating.phase == GcIntentPhase::Evacuating, "expected RPC transaction GC intent is not evacuating");
        codec::encode_gc_intent(expected_evacuating)?;
        let current = self.gc_intent()?.ok_or_else(|| {
            missing_authoritative_error("committed RPC transaction GC intent is missing")
        })?;
        self.validate_pre_cutover_gc_intent(current)?;
        let prepared = GcIntent {
            phase: GcIntentPhase::Prepared,
            ..*expected_evacuating
        };
        if current == prepared {
            return Ok(current);
        }
        if current != *expected_evacuating {
            return Err(conflicting_authoritative_error(
                "RPC transaction GC intent identity changed before Prepared transition",
            ));
        }
        let mut batch = rocksdb::WriteBatch::default();
        batch.put_cf(&self.control.state.cf(), codec::gc_intent_key(), codec::encode_gc_intent(&prepared)?);
        self.write_control(batch)?;
        Ok(prepared)
    }

    pub(super) fn commit_gc_cutover(
        &mut self,
        expected_prepared: &GcIntent,
        terminal_progress: codec::TailGenerationProgress,
        terminal_commit: codec::TailGenerationCommit,
    ) -> Result<GcCutoverResult> {
        ensure!(expected_prepared.phase == GcIntentPhase::Prepared, "expected RPC transaction GC intent is not prepared");
        codec::encode_gc_intent(expected_prepared)?;
        self.validate_terminal_gc_generation(
            *expected_prepared,
            terminal_progress,
            terminal_commit,
        )?;
        let current = self.gc_intent()?.ok_or_else(|| {
            missing_authoritative_error("committed RPC transaction GC intent is missing")
        })?;
        let committed = GcIntent {
            phase: GcIntentPhase::CutoverCommitted,
            ..*expected_prepared
        };
        if current == committed {
            return self.validate_committed_gc_cutover(committed);
        }
        if current != *expected_prepared {
            return Err(conflicting_authoritative_error(
                "RPC transaction GC intent identity changed before cutover",
            ));
        }
        self.validate_pre_cutover_gc_intent(current)?;
        let source_id = PartitionId(current.source_partition_id);
        let source = self.descriptors.get(&source_id).unwrap();
        let source_manifest_digest = self.sealed_manifest_digest(source_id).map_err(|_| {
            malformed_authoritative_error("malformed committed RPC transaction GC source manifest")
        })?;
        if source_manifest_digest != current.source_manifest_digest {
            return Err(conflicting_authoritative_error(
                "RPC transaction GC source manifest digest changed before cutover",
            ));
        }
        let smallest_known_lt = self.gc_cutover_history_watermark(source);
        let manifest_epoch = self.manifest_epoch.checked_add(1).context("manifest epoch overflow")?;
        let mut retired = source.clone();
        retired.lifecycle = ManifestLifecycle::Retired;
        retired.last_transition = ManifestTransition {
            lifecycle: ManifestLifecycle::Retired,
            epoch: manifest_epoch,
            at_unix_time: now_unix_time(),
        };
        let mut control_state = self.control_state;
        control_state.tail_visible_generation = current.target_generation;
        control_state.smallest_known_lt = smallest_known_lt;
        let mut batch = rocksdb::WriteBatch::default();
        batch.put_cf(&self.control.manifests.cf(), codec::partition_manifest_key(source_id.0), codec::encode_manifest(&retired.to_manifest()));
        batch.put_cf(&self.control.state.cf(), codec::control_state_key(), codec::encode_control_state(control_state));
        batch.put_cf(&self.control.state.cf(), codec::manifest_epoch_key(), codec::encode_manifest_epoch(manifest_epoch));
        batch.put_cf(&self.control.state.cf(), codec::gc_intent_key(), codec::encode_gc_intent(&committed)?);
        self.write_control(batch)?;
        self.descriptors.insert(source_id, retired);
        self.control_state = control_state;
        self.manifest_epoch = manifest_epoch;
        self.refresh_lifecycle_metrics();
        Ok(GcCutoverResult {
            intent: committed,
            source_partition_id: source_id,
            visible_generation: current.target_generation,
            smallest_known_lt,
            manifest_epoch,
        })
    }

    fn validate_terminal_gc_generation(
        &self,
        intent: GcIntent,
        terminal_progress: codec::TailGenerationProgress,
        terminal_commit: codec::TailGenerationCommit,
    ) -> Result<()> {
        codec::encode_tail_generation_progress(&terminal_progress)?;
        codec::encode_tail_generation_commit(&terminal_commit)?;
        let expected_progress = codec::TailGenerationProgress {
            target_generation: intent.target_generation,
            operation_id: intent.operation_id,
            source_partition_id: intent.source_partition_id,
            source_manifest_digest: intent.source_manifest_digest,
            retention_policy_digest: intent.retention_policy_digest,
            cursor: terminal_progress.cursor,
            eof: true,
            counters: terminal_progress.counters,
            chunk_digest: terminal_progress.chunk_digest,
        };
        ensure!(terminal_progress == expected_progress, "terminal RPC tail generation progress conflicts with the GC intent");
        let expected_commit = codec::TailGenerationCommit {
            layout_version: self.control_state.tail_layout_version,
            target_generation: intent.target_generation,
            operation_id: intent.operation_id,
            source_partition_id: intent.source_partition_id,
            source_manifest_digest: intent.source_manifest_digest,
            previous_visible_generation: intent.previous_visible_generation,
            cutoff_utime: intent.cutoff_utime,
            keep_tx_per_account: intent.keep_tx_per_account,
            retention_policy_digest: intent.retention_policy_digest,
            counters: terminal_progress.counters,
        };
        ensure!(terminal_commit == expected_commit, "terminal RPC tail generation commit conflicts with the GC intent");
        Ok(())
    }

    fn validate_committed_gc_cutover(&self, committed: GcIntent) -> Result<GcCutoverResult> {
        if committed.phase != GcIntentPhase::CutoverCommitted {
            return Err(conflicting_authoritative_error(
                "RPC transaction GC intent is not cutover-committed",
            ));
        }
        let source_id = PartitionId(committed.source_partition_id);
        let source = self
            .descriptors
            .get(&source_id)
            .ok_or_else(|| {
                missing_authoritative_error(
                    "committed RPC transaction GC source partition is missing",
                )
            })?;
        if source.lifecycle != ManifestLifecycle::Retired {
            return Err(conflicting_authoritative_error(
                "committed RPC transaction GC source is not retired",
            ));
        }
        if source.last_transition.lifecycle != ManifestLifecycle::Retired {
            return Err(conflicting_authoritative_error(
                "committed RPC transaction GC source transition is not retired",
            ));
        }
        if source.last_transition.epoch > self.manifest_epoch {
            return Err(conflicting_authoritative_error(
                "committed RPC transaction GC source transition exceeds the manifest epoch",
            ));
        }
        let expected_source_id = self
            .control_state
            .removed_through_partition_id
            .checked_add(1)
            .ok_or_else(|| {
                conflicting_authoritative_error(
                    "committed RPC transaction GC removed-through partition id overflow",
                )
            })?;
        if source_id.0 != expected_source_id {
            return Err(conflicting_authoritative_error(
                "committed RPC transaction GC source does not follow the removed prefix",
            ));
        }
        if self.control_state.tail_visible_generation != committed.target_generation {
            return Err(conflicting_authoritative_error(
                "committed RPC transaction GC generation conflicts with control state",
            ));
        }
        let source_fallback = source.last.transaction_lt.saturating_add(1);
        if self.control_state.smallest_known_lt == u64::MAX && source_fallback != u64::MAX {
            return Err(conflicting_authoritative_error(
                "committed RPC transaction GC history watermark remains uninitialized",
            ));
        }
        if self.control_state.smallest_known_lt < source_fallback {
            return Err(conflicting_authoritative_error(
                "committed RPC transaction GC history watermark conflicts with the retired source",
            ));
        }
        Ok(GcCutoverResult {
            intent: committed,
            source_partition_id: source_id,
            visible_generation: committed.target_generation,
            smallest_known_lt: self.control_state.smallest_known_lt,
            manifest_epoch: self.manifest_epoch,
        })
    }

    pub(super) fn transition_gc_intent_to_deleting(
        &mut self,
        expected_committed: &GcIntent,
    ) -> Result<GcIntent> {
        ensure!(expected_committed.phase == GcIntentPhase::CutoverCommitted, "expected RPC transaction GC intent is not cutover-committed");
        codec::encode_gc_intent(expected_committed)?;
        let current = self.gc_intent()?.ok_or_else(|| {
            missing_authoritative_error("committed RPC transaction GC intent is missing")
        })?;
        let deleting = GcIntent {
            phase: GcIntentPhase::Deleting,
            ..*expected_committed
        };
        if current == deleting {
            self.validate_deleting_gc_intent(deleting)?;
            return Ok(current);
        }
        if current != *expected_committed {
            return Err(conflicting_authoritative_error(
                "RPC transaction GC intent identity changed before Deleting transition",
            ));
        }
        self.validate_committed_gc_cutover(current)?;
        let source_id = PartitionId(current.source_partition_id);
        ensure!(self.lifetime_references_drained(source_id)?, "RPC transaction GC source still has live references before Deleting transition");
        let mut descriptor = self.descriptors.get(&source_id).unwrap().clone();
        descriptor.lifecycle = ManifestLifecycle::Deleting;
        descriptor.last_transition = ManifestTransition {
            lifecycle: ManifestLifecycle::Deleting,
            epoch: self.manifest_epoch,
            at_unix_time: now_unix_time(),
        };
        let mut batch = rocksdb::WriteBatch::default();
        batch.put_cf(&self.control.manifests.cf(), codec::partition_manifest_key(source_id.0), codec::encode_manifest(&descriptor.to_manifest()));
        batch.put_cf(&self.control.state.cf(), codec::gc_intent_key(), codec::encode_gc_intent(&deleting)?);
        self.write_control(batch)?;
        self.descriptors.insert(source_id, descriptor);
        self.refresh_lifecycle_metrics();
        Ok(deleting)
    }

    fn validate_deleting_gc_intent(&self, deleting: GcIntent) -> Result<()> {
        if deleting.phase != GcIntentPhase::Deleting {
            return Err(conflicting_authoritative_error(
                "RPC transaction GC intent is not deleting",
            ));
        }
        let source_id = PartitionId(deleting.source_partition_id);
        let source = self
            .descriptors
            .get(&source_id)
            .ok_or_else(|| {
                missing_authoritative_error("deleting RPC transaction GC source partition is missing")
            })?;
        if source.lifecycle != ManifestLifecycle::Deleting {
            return Err(conflicting_authoritative_error(
                "deleting RPC transaction GC source manifest is not deleting",
            ));
        }
        if source.last_transition.lifecycle != ManifestLifecycle::Deleting {
            return Err(conflicting_authoritative_error(
                "deleting RPC transaction GC source transition is not deleting",
            ));
        }
        if source.last_transition.epoch > self.manifest_epoch {
            return Err(conflicting_authoritative_error(
                "deleting RPC transaction GC source transition exceeds the manifest epoch",
            ));
        }
        if self.control_state.tail_visible_generation != deleting.target_generation {
            return Err(conflicting_authoritative_error(
                "deleting RPC transaction GC generation conflicts with control state",
            ));
        }
        let expected_source_id = self
            .control_state
            .removed_through_partition_id
            .checked_add(1)
            .ok_or_else(|| {
                conflicting_authoritative_error(
                    "deleting RPC transaction GC removed-through partition id overflow",
                )
            })?;
        if source_id.0 != expected_source_id {
            return Err(conflicting_authoritative_error(
                "deleting RPC transaction GC source does not follow the removed prefix",
            ));
        }
        Ok(())
    }

    pub(super) fn finalize_gc_source_deletion(
        &mut self,
        expected_deleting: &GcIntent,
        guard: PartitionDeletionGuard,
    ) -> Result<PartitionId> {
        ensure!(expected_deleting.phase == GcIntentPhase::Deleting, "expected RPC transaction GC intent is not deleting");
        codec::encode_gc_intent(expected_deleting)?;
        let current = self.gc_intent()?.ok_or_else(|| {
            missing_authoritative_error("committed RPC transaction GC intent is missing")
        })?;
        if current != *expected_deleting {
            return Err(conflicting_authoritative_error(
                "RPC transaction GC intent identity changed before source deletion finalization",
            ));
        }
        self.validate_deleting_gc_intent(current)?;
        let source_id = PartitionId(current.source_partition_id);
        ensure!(guard.id == source_id, "RPC transaction GC deletion guard has a different source partition");
        ensure!(guard._lifetime.id() == source_id, "RPC transaction GC deletion guard has a different lifetime identity");
        let expected_path = self.root.join(Self::partition_subdir(source_id));
        ensure!(guard.path == expected_path, "RPC transaction GC deletion guard has a different source path");
        ensure!(!guard.path.try_exists().with_context(|| format!("failed to check deleted RPC transaction partition path {}", guard.path.display()))?, "RPC transaction GC source directory still exists at {}", guard.path.display());
        let owner = self
            .lifetime_owners
            .get(&source_id)
            .with_context(|| format!("transaction partition {} lifetime owner is missing", source_id.0))?;
        ensure!(Arc::strong_count(&owner.0) == 2, "RPC transaction GC source gained a live reference while its deletion guard was held");
        let mut control_state = self.control_state;
        control_state.removed_through_partition_id = source_id.0;
        let mut batch = rocksdb::WriteBatch::default();
        batch.delete_cf(&self.control.manifests.cf(), codec::partition_manifest_key(source_id.0));
        batch.delete_cf(&self.control.state.cf(), codec::gc_intent_key());
        batch.put_cf(&self.control.state.cf(), codec::control_state_key(), codec::encode_control_state(control_state));
        self.write_control(batch)?;
        self.descriptors.remove(&source_id).expect("validated deleting transaction partition is present");
        self.control_state = control_state;
        drop(guard);
        let owner = self.lifetime_owners.remove(&source_id).expect("validated transaction partition lifetime owner is present");
        debug_assert!(owner.only_owner_remains());
        self.refresh_lifecycle_metrics();
        Ok(source_id)
    }

    fn gc_cutover_history_watermark(&self, source: &PartitionDescriptor) -> u64 {
        let next_live_lt = self
            .descriptors
            .range((std::ops::Bound::Excluded(source.id), std::ops::Bound::Unbounded))
            .map(|(_, descriptor)| descriptor)
            .find(|descriptor| {
                descriptor.first.block_id.is_some()
                    && matches!(
                        descriptor.lifecycle,
                        ManifestLifecycle::Active
                            | ManifestLifecycle::Sealing
                            | ManifestLifecycle::Sealed
                    )
            })
            .map_or_else(
                || source.last.transaction_lt.saturating_add(1),
                |descriptor| descriptor.first.transaction_lt,
            );
        if self.control_state.smallest_known_lt == u64::MAX {
            next_live_lt
        } else {
            self.control_state.smallest_known_lt.max(next_live_lt)
        }
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

    pub fn has_creating_partition(&self) -> bool {
        self.descriptors
            .values()
            .any(|descriptor| descriptor.lifecycle == ManifestLifecycle::Creating)
    }

    #[cfg(test)]
    pub fn fail_next_creation_activation(&mut self) {
        self.fail_next_creation_activation = true;
    }

    pub fn active_has_commit_after(&self, mc_seqno: u32) -> Result<bool> {
        let started_at = Instant::now();
        let result = self.active_has_commit_after_inner(mc_seqno);
        metrics::histogram!(
            "tycho_storage_rpc_active_tail_probe_duration_seconds",
            "result" => match &result {
                Ok(true) => "found",
                Ok(false) => "empty",
                Err(_) => "failure",
            },
        )
        .record(started_at.elapsed());
        result
    }

    fn active_has_commit_after_inner(&self, mc_seqno: u32) -> Result<bool> {
        let Some(active) = self.active.as_ref() else {
            let initial = self.descriptors.get(&PartitionId::FIRST);
            // only the durable bootstrap state has no active database before lifecycle continuation
            ensure!(
                self.descriptors.len() == 1
                    && initial.is_some_and(|descriptor| {
                        descriptor.lifecycle == ManifestLifecycle::Creating
                            && descriptor.counters == PartitionCounters::default()
                            && descriptor.last_transition.epoch == self.manifest_epoch
                    })
                    && self.visible_frontier.is_none(),
                "partition manager is missing an active DB outside the initial creating state"
            );
            return Ok(false);
        };
        let Some(next_mc_seqno) = mc_seqno.checked_add(1) else {
            return Ok(false);
        };
        let mut iterator = active
            .rocksdb()
            .raw_iterator_cf(&active.partition_commits.cf());
        iterator.seek(next_mc_seqno.to_be_bytes());
        if !iterator.valid() {
            iterator.status()?;
            return Ok(false);
        }
        let key = iterator
            .key()
            .context("partition commit iterator returned no key")?;
        let (found_mc_seqno, _) = codec::decode_partition_commit_key(key)?;
        ensure!(found_mc_seqno >= next_mc_seqno, "partition commit lower-bound seek returned an earlier masterchain sequence number");
        iterator.status()?;
        Ok(true)
    }

    pub fn validate_masterchain_commit(
        &self,
        block_id: &tycho_types::models::BlockId,
    ) -> Result<()> {
        let started_at = Instant::now();
        let result = self.validate_masterchain_commit_inner(block_id);
        metrics::histogram!(
            "tycho_storage_rpc_predecessor_validation_duration_seconds",
            "stage" => "partition_commit",
            "result" => if result.is_ok() { "success" } else { "failure" },
        )
        .record(started_at.elapsed());
        if let Err(error) = &result {
            let message = format!("{error:#}");
            let reason = if message.contains("missing") {
                "commit_missing"
            } else if message.contains("digest") {
                "commit_digest_mismatch"
            } else if message.contains("identity") {
                "full_identity_mismatch"
            } else if message.contains("invalid") || message.contains("decode") {
                "commit_malformed"
            } else {
                "other"
            };
            metrics::counter!(
                "tycho_storage_rpc_predecessor_validation_failures_total",
                "reason" => reason,
            )
            .increment(1);
        }
        result
    }

    fn validate_masterchain_commit_inner(
        &self,
        block_id: &tycho_types::models::BlockId,
    ) -> Result<()> {
        self.load_masterchain_commit(block_id).map(drop)
    }

    fn load_masterchain_commit(
        &self,
        block_id: &tycho_types::models::BlockId,
    ) -> Result<codec::PartitionCommit> {
        if !block_id.is_masterchain() {
            return Err(conflicting_authoritative_error(
                "committed RPC frontier is not a masterchain block",
            ));
        }
        let (_, lease) = self.lease_for_mc_seqno(block_id.seqno)?;
        let key = codec::partition_commit_key(block_id.seqno, &block_id.as_short_id());
        let value = lease
            .partition_commits
            .get(key)?
            .ok_or_else(|| {
                missing_authoritative_error(
                    "transaction partition is missing the RPC frontier commit",
                )
            })?;
        let commit = codec::decode_partition_commit(value.as_ref()).map_err(|error| {
            if format!("{error:#}").contains("digest") {
                conflicting_authoritative_error("RPC frontier commit digest mismatch")
            } else {
                malformed_authoritative_error("invalid malformed committed RPC frontier commit")
            }
        })?;
        if commit.block_id != *block_id {
            return Err(conflicting_authoritative_error(
                "RPC frontier commit identity mismatch",
            ));
        }
        if commit.digest != block_id.root_hash {
            return Err(conflicting_authoritative_error(
                "RPC frontier commit digest mismatch",
            ));
        }
        Ok(commit)
    }

    pub fn manifest_epoch(&self) -> u64 {
        self.manifest_epoch
    }

    pub fn min_transaction_lt(&self) -> u64 {
        self.control_state.smallest_known_lt
    }

    pub fn persist_min_transaction_lt_decrease(&mut self, min_transaction_lt: u64) -> Result<bool> {
        if self.control_state.tail_visible_generation > 0
            || min_transaction_lt >= self.control_state.smallest_known_lt
        {
            return Ok(false);
        }
        let mut control_state = self.control_state;
        control_state.smallest_known_lt = min_transaction_lt;
        let mut batch = rocksdb::WriteBatch::default();
        batch.put_cf(&self.control.state.cf(), codec::control_state_key(), codec::encode_control_state(control_state));
        self.write_control(batch)?;
        self.control_state = control_state;
        Ok(true)
    }

    pub fn descriptors(&self) -> Vec<PartitionDescriptor> {
        self.descriptors.values().cloned().collect()
    }

    fn lifetime_token(&self, id: PartitionId) -> Result<PartitionLifetimeToken> {
        let token = self
            .lifetime_owners
            .get(&id)
            .with_context(|| format!("transaction partition {} lifetime owner is missing", id.0))?
            .token();
        ensure!(token.id() == id, "transaction partition lifetime owner id mismatch");
        Ok(token)
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
                ManifestLifecycle::Retired => result.retired_count += 1,
                ManifestLifecycle::Deleting => result.deleting_count += 1,
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
        metrics::gauge!(
            "tycho_storage_rpc_partition_count",
            "lifecycle" => "retired",
        )
        .set(snapshot.retired_count as f64);
        metrics::gauge!(
            "tycho_storage_rpc_partition_count",
            "lifecycle" => "deleting",
        )
        .set(snapshot.deleting_count as f64);
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
        metrics::gauge!("tycho_storage_rpc_partition_active_estimated_transaction_lsm_bytes")
            .set(snapshot.active_counters.estimated_transaction_lsm_bytes as f64);
        metrics::gauge!("tycho_storage_rpc_partition_active_estimated_transaction_blob_bytes")
            .set(snapshot.active_counters.estimated_transaction_blob_bytes as f64);
        metrics::gauge!("tycho_storage_rpc_partition_active_transaction_count")
            .set(snapshot.active_counters.transaction_count as f64);
        metrics::gauge!("tycho_storage_rpc_partition_active_transaction_index_record_count")
            .set(snapshot.active_counters.transaction_index_record_count as f64);
        metrics::gauge!("tycho_storage_rpc_partition_active_estimated_block_metadata_bytes")
            .set(snapshot.active_counters.estimated_block_metadata_bytes as f64);
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
            estimated_transaction_lsm_bytes = descriptor.counters.estimated_transaction_lsm_bytes,
            estimated_transaction_blob_bytes = descriptor.counters.estimated_transaction_blob_bytes,
            transaction_count = descriptor.counters.transaction_count,
            transaction_index_record_count = descriptor.counters.transaction_index_record_count,
            estimated_block_metadata_bytes = descriptor.counters.estimated_block_metadata_bytes,
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
            .filter(|descriptor| {
                matches!(
                    descriptor.lifecycle,
                    ManifestLifecycle::Active
                        | ManifestLifecycle::Sealing
                        | ManifestLifecycle::Sealed
                )
            })
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
            ManifestLifecycle::Retired | ManifestLifecycle::Deleting => {
                bail!("selected transaction partition {} is not readable", id.0)
            }
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
            lifetime: self.lifetime_token(self.active_id).expect("active transaction partition lifetime owner is missing"),
        }
    }

    pub fn sealing_lease(&self, id: PartitionId) -> Option<PartitionReadLease> {
        self.sealing
            .get(&id)
            .cloned()
            .map(|db| PartitionReadLease {
                db,
                lifecycle: ManifestLifecycle::Sealing,
                lifetime: self.lifetime_token(id).expect("sealing transaction partition lifetime owner is missing"),
            })
    }

    pub fn sealed_lease(&self, id: PartitionId) -> Result<PartitionReadLease> {
        self.sealed_lease_opener(id)?.open()
    }

    pub fn sealed_lease_opener(&self, id: PartitionId) -> Result<SealedPartitionLeaseOpener> {
        ensure!(self.descriptors.get(&id).map(|entry| entry.lifecycle) == Some(ManifestLifecycle::Sealed), "partition {} is not sealed", id.0);
        Ok(SealedPartitionLeaseOpener {
            id,
            context: self.context.clone(),
            subdir: Self::partition_subdir(id),
            cache: self.sealed_cache.clone(),
            lifetime: self.lifetime_token(id)?,
            #[cfg(test)]
            post_open_hook: None,
        })
    }

    pub(super) fn maintenance_sealed_opener(&self, id: PartitionId) -> Result<MaintenanceSealedPartitionOpener> {
        ensure!(self.descriptors.get(&id).map(|entry| entry.lifecycle) == Some(ManifestLifecycle::Sealed), "partition {} is not sealed", id.0);
        Ok(MaintenanceSealedPartitionOpener {
            id,
            context: self.context.clone(),
            subdir: Self::partition_subdir(id),
            lifetime: self.lifetime_token(id)?,
            #[cfg(test)]
            post_open_hook: None,
        })
    }

    pub(super) fn sealed_manifest_digest(&self, id: PartitionId) -> Result<HashBytes> {
        let descriptor = self.descriptors.get(&id).context("sealed transaction partition is missing")?;
        ensure!(descriptor.lifecycle == ManifestLifecycle::Sealed, "partition {} is not sealed", id.0);
        Ok(HashBytes::from_slice(blake3::hash(&codec::encode_manifest(&descriptor.to_manifest())).as_bytes()))
    }

    pub(super) fn sealed_manifest_and_lifetime(&self, id: PartitionId) -> Result<(HashBytes, PartitionLifetimeToken)> {
        Ok((self.sealed_manifest_digest(id)?, self.lifetime_token(id)?))
    }

    fn lifetime_references_drained(&self, id: PartitionId) -> Result<bool> {
        let descriptor = self
            .descriptors
            .get(&id)
            .context("transaction partition is missing")?;
        ensure!(
            matches!(
                descriptor.lifecycle,
                ManifestLifecycle::Retired | ManifestLifecycle::Deleting
            ),
            "partition {} is not retired or deleting",
            id.0
        );
        self.sealed_cache.invalidate(&id);
        self.sealed_cache.run_pending_tasks();
        Ok(self
            .lifetime_owners
            .get(&id)
            .with_context(|| format!("transaction partition {} lifetime owner is missing", id.0))?
            .only_owner_remains())
    }

    pub(super) fn deletion_references_drained(&self, id: PartitionId) -> Result<bool> {
        self.lifetime_references_drained(id)
    }

    pub(super) fn try_acquire_deletion_guard(&mut self, id: PartitionId) -> Result<Option<PartitionDeletionGuard>> {
        ensure!(
            self.descriptors.get(&id).map(|entry| entry.lifecycle)
                == Some(ManifestLifecycle::Deleting),
            "partition {} is not deleting",
            id.0
        );
        if !self.lifetime_references_drained(id)? {
            return Ok(None);
        }
        Ok(Some(PartitionDeletionGuard {
            id,
            path: self.root.join(Self::partition_subdir(id)),
            _lifetime: self.lifetime_token(id)?,
        }))
    }

    #[cfg(test)]
    pub(super) fn partition_lifetime_strong_count(&self, id: PartitionId) -> Option<usize> {
        self.lifetime_owners
            .get(&id)
            .map(|owner| Arc::strong_count(&owner.0))
    }

    #[cfg(test)]
    pub fn run_sealed_cache_pending_tasks(&self) {
        self.sealed_cache.run_pending_tasks();
    }

    #[cfg(test)]
    pub(super) fn invalidate_sealed_cache_for_test(&self, id: PartitionId) {
        self.sealed_cache.invalidate(&id);
        self.sealed_cache.run_pending_tasks();
    }

    pub(super) fn sealed_cache_entry_count(&self) -> u64 {
        self.sealed_cache.entry_count()
    }

    #[cfg(test)]
    pub(super) fn mutate_sealed_manifest_for_test(&mut self, id: PartitionId) -> Result<()> {
        let descriptor = self
            .descriptors
            .get_mut(&id)
            .context("sealed transaction partition is missing")?;
        ensure!(descriptor.lifecycle == ManifestLifecycle::Sealed, "partition {} is not sealed", id.0);
        descriptor.last_transition.epoch = descriptor
            .last_transition
            .epoch
            .checked_add(1)
            .context("sealed manifest test epoch overflow")?;
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn retire_partition_for_snapshot_test(&mut self, id: PartitionId) -> Result<()> {
        ensure!(
            self.descriptors.get(&id).map(|entry| entry.lifecycle)
                == Some(ManifestLifecycle::Sealed),
            "partition {} is not sealed",
            id.0
        );
        let manifest_epoch = self.manifest_epoch.checked_add(1).context("manifest epoch overflow")?;
        let mut descriptor = self.descriptors.get(&id).unwrap().clone();
        descriptor.lifecycle = ManifestLifecycle::Retired;
        descriptor.last_transition = ManifestTransition {
            lifecycle: ManifestLifecycle::Retired,
            epoch: manifest_epoch,
            at_unix_time: now_unix_time(),
        };
        let mut batch = rocksdb::WriteBatch::default();
        batch.put_cf(&self.control.manifests.cf(), codec::partition_manifest_key(id.0), codec::encode_manifest(&descriptor.to_manifest()));
        batch.put_cf(&self.control.state.cf(), codec::manifest_epoch_key(), codec::encode_manifest_epoch(manifest_epoch));
        self.write_control(batch)?;
        self.descriptors.insert(id, descriptor);
        self.manifest_epoch = manifest_epoch;
        self.refresh_lifecycle_metrics();
        Ok(())
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

    pub fn open_sealed_read_only(&self, id: PartitionId) -> Result<SealingReadOnlyHandle> {
        ensure!(self.descriptors.get(&id).map(|entry| entry.lifecycle) == Some(ManifestLifecycle::Sealing), "partition {} is not sealing", id.0);
        Ok(SealingReadOnlyHandle {
            id,
            db: Arc::new(self.context.open_read_only(Self::partition_subdir(id))?),
            lifetime: self.lifetime_token(id)?,
        })
    }

    pub fn reopen_sealing_writable(&self, id: PartitionId) -> Result<Arc<RpcTransactionsDb>> {
        let db = Arc::new(self.context.open_preconfigured(Self::partition_subdir(id))?);
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

    pub fn complete_sealing(&mut self, id: PartitionId, handle: SealingReadOnlyHandle) -> Result<()> {
        let started_at = Instant::now();
        ensure!(self.descriptors.get(&id).map(|entry| entry.lifecycle) == Some(ManifestLifecycle::Sealing), "partition {} is not sealing", id.0);
        ensure!(!self.sealing.contains_key(&id), "sealing transaction partition {} handle was not closed", id.0);
        ensure!(handle.id == id && handle.lifetime.id() == id, "sealing read-only handle belongs to a different transaction partition");
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
        let cached = Arc::new(CachedSealedPartition {
            db: handle.db,
            lifetime: handle.lifetime,
        });
        let mut batch = rocksdb::WriteBatch::default();
        batch.put_cf(&self.control.manifests.cf(), codec::partition_manifest_key(id.0), codec::encode_manifest(&descriptor.to_manifest()));
        batch.put_cf(&self.control.state.cf(), codec::manifest_epoch_key(), codec::encode_manifest_epoch(manifest_epoch));
        self.write_control(batch)?;
        self.descriptors.insert(id, descriptor);
        self.manifest_epoch = manifest_epoch;
        self.closing.remove(&id);
        self.sealed_cache.insert(id, cached);
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
        if counters.estimated_transaction_lsm_bytes >= self.config.target_transaction_lsm_bytes {
            Some(RotationReason::EstimatedTransactionLsmBytes)
        } else if counters.estimated_transaction_blob_bytes >= self.config.target_transaction_blob_bytes {
            Some(RotationReason::EstimatedTransactionBlobBytes)
        } else if counters.transaction_index_record_count >= self.config.target_transaction_index_records {
            Some(RotationReason::TransactionIndexRecordCount)
        } else if counters.estimated_block_metadata_bytes >= self.config.target_block_metadata_bytes {
            Some(RotationReason::EstimatedBlockMetadataBytes)
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
        ensure!(!self.lifetime_owners.contains_key(&id), "transaction partition {} lifetime owner already exists", id.0);
        let mut control_state = self.control_state;
        control_state.next_partition_id = control_state.next_partition_id.checked_add(1).context("transaction partition id overflow")?;
        let manifest_epoch = self.manifest_epoch.checked_add(1).context("manifest epoch overflow")?;
        let descriptor = Self::empty_descriptor(id, ManifestLifecycle::Creating, manifest_epoch);
        let mut batch = rocksdb::WriteBatch::default();
        if !has_active {
            batch.put_cf(
                &self.control.state.cf(),
                codec::rpc_layout_marker_key(),
                codec::rpc_layout_marker_value(),
            );
        }
        batch.put_cf(&self.control.state.cf(), codec::control_state_key(), codec::encode_control_state(control_state));
        batch.put_cf(&self.control.manifests.cf(), codec::partition_manifest_key(id.0), codec::encode_manifest(&descriptor.to_manifest()));
        batch.put_cf(&self.control.state.cf(), codec::manifest_epoch_key(), codec::encode_manifest_epoch(manifest_epoch));
        self.write_control(batch)?;
        self.control_state = control_state;
        self.manifest_epoch = manifest_epoch;
        self.descriptors.insert(id, descriptor);
        self.lifetime_owners.insert(id, PartitionLifetimeOwner::new(id));
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
        self.activate_creating_partition(id, None, None)
    }

    /// Creates the DB for a persisted `Creating` entry and publishes the new active partition.
    ///
    /// The old active descriptor and visible frontier are durable before creation starts.
    fn activate_creating_partition(
        &mut self,
        id: PartitionId,
        old_descriptor: Option<PartitionDescriptor>,
        visible_frontier: Option<&tycho_types::models::BlockId>,
    ) -> Result<()> {
        #[cfg(test)]
        if std::mem::take(&mut self.fail_next_creation_activation) {
            bail!("test transaction partition activation failure");
        }
        let started_at = Instant::now();
        let reason = self.rotation_requested;
        ensure!(self.descriptors.get(&id).map(|entry| entry.lifecycle) == Some(ManifestLifecycle::Creating), "partition {} is not creating", id.0);
        let db: Arc<RpcTransactionsDb> = Arc::new(self.context.open_preconfigured(Self::partition_subdir(id))?);
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
        let old_descriptor = match old_descriptor {
            Some(descriptor) => Some(descriptor),
            None if old_exists => Some(self.descriptors.get(&old_active).unwrap().clone()),
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
            let old = self.active.clone().expect("partition manager is initialized with an active DB");
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
    #[cfg(test)]
    pub fn commit_masterchain_block_set(&mut self, block_id: &tycho_types::models::BlockId) -> Result<()> {
        self.commit_masterchain_block_set_after_admission(block_id, false)
    }

    pub(crate) fn commit_masterchain_block_set_after_admission(
        &mut self,
        block_id: &tycho_types::models::BlockId,
        current_commit_prevalidated: bool,
    ) -> Result<()> {
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
            ManifestLifecycle::Retired | ManifestLifecycle::Deleting => {
                bail!("selected partition {} is not readable", id.0)
            }
        };
        if !current_commit_prevalidated {
            let key = codec::partition_commit_key(block_id.seqno, &block_id.as_short_id());
            let value = lease.db().partition_commits.get(key)?.context("masterchain partition commit is missing")?;
            let commit = codec::decode_partition_commit(value.as_ref())?;
            ensure!(commit.block_id == *block_id && commit.digest == block_id.root_hash, "masterchain partition commit identity mismatch");
        }
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

        let descriptor = Self::aggregate_commits_after_descriptor(
            lease.db(),
            &persisted_descriptor,
            block_id.seqno,
        )?
        .0;
        let rotation_reason = if descriptor.last.block_id.is_some()
            && descriptor.last.mc_seqno <= block_id.seqno
        {
            self.request_rotation(descriptor.counters)
        } else {
            None
        };
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
        if let Some(reason) = rotation_reason
            && !self.sealer_busy
        {
            let started_at = Instant::now();
            let result = (|| {
                let new_id = self.begin_partition_creation()?;
                self.activate_creating_partition(new_id, None, None)
            })();
            record_rotation_result(reason, started_at.elapsed(), result.is_ok());
            if let Err(e) = result {
                tracing::error!("failed to start RPC transaction partition rotation after durable boundary publication: {e:#}");
            }
        } else if self.rotation_requested.is_some() {
            metrics::counter!("tycho_storage_rpc_partition_rotation_deferred_total").increment(1);
        }
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
        self.lifetime_owners = self
            .descriptors
            .keys()
            .copied()
            .map(|id| (id, PartitionLifetimeOwner::new(id)))
            .collect();
        self.validate_descriptors(true)?;
        self.validate_gc_intent_and_cleanup_state()?;
        self.validate_directories()?;
        let initial_creating = self.descriptors.len() == 1
            && self.descriptors.contains_key(&PartitionId::FIRST)
            && self.descriptors[&PartitionId::FIRST].lifecycle == ManifestLifecycle::Creating;
        if initial_creating {
            ensure!(self.visible_frontier.is_none(), "initial creating transaction partition has a visible frontier");
            ensure!(self.descriptors[&PartitionId::FIRST].counters == PartitionCounters::default(), "initial creating transaction partition has non-empty counters");
            ensure!(self.descriptors[&PartitionId::FIRST].last_transition.epoch == self.manifest_epoch, "initial creating transaction partition has an inconsistent transition epoch");
            ensure!(self.control.state.get(codec::active_partition_key())?.is_none(), "initial creating transaction partition has an active pointer");
            return Ok(());
        }
        self.validate_active_pointer()?;
        self.validate_directories()?;
        self.open_existing_partitions()?;
        Ok(())
    }

    pub fn continue_lifecycle(&mut self) -> Result<()> {
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
            self.active = Some(Arc::new(self.context.open_preconfigured(Self::partition_subdir(active))?));
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
                ManifestLifecycle::Sealed => {}
                ManifestLifecycle::Creating
                | ManifestLifecycle::Active
                | ManifestLifecycle::Retired
                | ManifestLifecycle::Deleting => {}
            }
        }
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
            result.counters.estimated_transaction_lsm_bytes = result.counters.estimated_transaction_lsm_bytes.checked_add(commit.estimated_transaction_lsm_bytes).context("transaction LSM byte counter overflow while aggregating partition")?;
            result.counters.estimated_transaction_blob_bytes = result.counters.estimated_transaction_blob_bytes.checked_add(commit.estimated_transaction_blob_bytes).context("transaction blob byte counter overflow while aggregating partition")?;
            result.counters.transaction_index_record_count = result.counters.transaction_index_record_count.checked_add(commit.transaction_index_record_count).context("transaction index record counter overflow while aggregating partition")?;
            result.counters.estimated_block_metadata_bytes = result.counters.estimated_block_metadata_bytes.checked_add(commit.estimated_block_metadata_bytes).context("block metadata byte counter overflow while aggregating partition")?;
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
        let mut expected_id = self.control_state.removed_through_partition_id.checked_add(1).context("removed-through partition id overflow")?;
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

    fn validate_gc_intent_and_cleanup_state(&self) -> Result<()> {
        let cleanup = self
            .descriptors
            .values()
            .filter(|descriptor| matches!(descriptor.lifecycle, ManifestLifecycle::Retired | ManifestLifecycle::Deleting))
            .collect::<Vec<_>>();
        if cleanup.len() > 1 {
            return Err(conflicting_authoritative_error(
                "more than one RPC transaction partition is retired or deleting",
            ));
        }
        let Some(intent) = self.gc_intent()? else {
            if !cleanup.is_empty() {
                return Err(missing_authoritative_error(
                    "retired or deleting RPC transaction partition has no owning GC intent",
                ));
            }
            return Ok(());
        };
        match intent.phase {
            GcIntentPhase::Evacuating | GcIntentPhase::Prepared => {
                if !cleanup.is_empty() {
                    return Err(conflicting_authoritative_error(
                        "pre-cutover RPC transaction GC intent owns a retired or deleting source",
                    ));
                }
                self.validate_pre_cutover_gc_intent(intent)
            }
            GcIntentPhase::CutoverCommitted => {
                if cleanup.len() != 1 {
                    return Err(missing_authoritative_error(
                        "cutover-committed RPC transaction GC intent has no unique retired source",
                    ));
                }
                self.validate_committed_gc_cutover(intent).map(|_| ())
            }
            GcIntentPhase::Deleting => {
                if cleanup.len() != 1 {
                    return Err(missing_authoritative_error(
                        "deleting RPC transaction GC intent has no unique deleting source",
                    ));
                }
                self.validate_deleting_gc_intent(intent)
            }
        }
    }

    fn validate_active_pointer(&self) -> Result<()> {
        let value = self.control.state.get(codec::active_partition_key())?.context("missing active transaction partition pointer")?;
        let active = PartitionId(codec::decode_active_partition(value.as_ref())?);
        ensure!(active.0 != 0, "active transaction partition pointer must be non-zero");
        let manifest_active = self.descriptors.values().find(|entry| entry.lifecycle == ManifestLifecycle::Active).context("active partition manifest is missing")?.id;
        ensure!(active == manifest_active, "active transaction partition pointer does not match active manifest");
        Ok(())
    }

    pub(super) fn certified_removed_partition_directories(&self) -> Result<Vec<(PartitionId, PathBuf)>> {
        let root = self.root.join(TRANSACTIONS_SUBDIR);
        if !root.exists() {
            return Ok(Vec::new());
        }
        let mut certified = Vec::new();
        for entry in std::fs::read_dir(&root).with_context(|| format!("failed to read transaction partitions at {}", root.display()))? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let name = entry.file_name();
            let name = name.to_str().context("non-utf8 transaction partition directory")?;
            let id = PartitionId::parse_directory_name(name)?;
            ensure!(id.0 != 0, "invalid transaction partition directory id: {name}");
            if self.descriptors.contains_key(&id) {
                continue;
            }
            ensure!(id.0 <= self.control_state.removed_through_partition_id, "unknown transaction partition directory: {name}");
            certified.push((id, entry.path()));
        }
        certified.sort_unstable_by_key(|(id, _)| *id);
        Ok(certified)
    }

    fn validate_directories(&self) -> Result<()> {
        self.certified_removed_partition_directories()?;
        for descriptor in self.descriptors.values().filter(|descriptor| {
            matches!(
                descriptor.lifecycle,
                ManifestLifecycle::Active
                    | ManifestLifecycle::Sealing
                    | ManifestLifecycle::Sealed
            )
        }) {
            let path = self.root.join(Self::partition_subdir(descriptor.id));
            validate_committed_partition_directory(&path, descriptor.id)?;
        }
        Ok(())
    }

    fn partition_subdir(id: PartitionId) -> PathBuf {
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
        let _ = db.accounts.cf();
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
pub(super) mod tests {
    use super::*;
    use super::codec::PartitionCommit;
    use super::super::db::{RpcControlTables, RpcCurrentStateTables};
    use super::super::tail::{AuthoritativeErrorKind, classify_authoritative_error};
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
    pub(crate) struct TestMetricsRecorder {
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

        pub(crate) fn counter(&self, key: &str) -> u64 {
            self.counters
                .lock()
                .unwrap()
                .get(key)
                .map(|value| *value.0.lock().unwrap())
                .unwrap_or_default()
        }

        pub(crate) fn gauge(&self, key: &str) -> f64 {
            self.gauges
                .lock()
                .unwrap()
                .get(key)
                .map(|value| *value.0.lock().unwrap())
                .unwrap_or_default()
        }

        pub(crate) fn histogram_len(&self, key: &str) -> usize {
            self.histograms
                .lock()
                .unwrap()
                .get(key)
                .map(|value| value.0.lock().unwrap().len())
                .unwrap_or_default()
        }

        pub(crate) fn histogram_values(&self, key: &str) -> Vec<f64> {
            self.histograms
                .lock()
                .unwrap()
                .get(key)
                .map(|value| value.0.lock().unwrap().clone())
                .unwrap_or_default()
        }

        pub(crate) fn keys(&self) -> Vec<String> {
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
            target_transaction_lsm_bytes: 10,
            target_transaction_blob_bytes: 10,
            target_transaction_index_records: 10,
            target_block_metadata_bytes: 10,
            max_open_sealed_partitions: 1,
            ..Default::default()
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

    pub(crate) fn persist_initial_creating(context: &StorageContext) {
        let control: RpcControlDb = context.open_preconfigured(CONTROL_SUBDIR).unwrap();
        let state = ControlState {
            node_instance_id: rand::random::<InstanceId>(),
            next_partition_id: 2,
            smallest_known_lt: u64::MAX,
            tail_visible_generation: 0,
            tail_layout_version: TailLayoutVersion::MonolithicV1,
            removed_through_partition_id: 0,
        };
        let descriptor =
            PartitionManager::empty_descriptor(PartitionId::FIRST, ManifestLifecycle::Creating, 1);
        let mut batch = rocksdb::WriteBatch::default();
        batch.put_cf(
            &control.state.cf(),
            codec::rpc_layout_marker_key(),
            codec::rpc_layout_marker_value(),
        );
        batch.put_cf(
            &control.state.cf(),
            codec::control_state_key(),
            codec::encode_control_state(state),
        );
        batch.put_cf(
            &control.state.cf(),
            codec::manifest_epoch_key(),
            codec::encode_manifest_epoch(1),
        );
        batch.put_cf(
            &control.manifests.cf(),
            codec::partition_manifest_key(PartitionId::FIRST.0),
            codec::encode_manifest(&descriptor.to_manifest()),
        );
        control
            .rocksdb()
            .write_opt(batch, control.state.write_config())
            .unwrap();
    }

    fn write_commit(
        db: &RpcTransactionsDb,
        mc_seqno: u32,
        block_id: BlockId,
        estimated_transaction_lsm_bytes: u64,
        estimated_transaction_blob_bytes: u64,
        transaction_index_record_count: u64,
    ) {
        write_commit_with_block_metadata(
            db,
            mc_seqno,
            block_id,
            estimated_transaction_lsm_bytes,
            estimated_transaction_blob_bytes,
            transaction_index_record_count,
            0,
        );
    }

    fn write_commit_with_block_metadata(
        db: &RpcTransactionsDb,
        mc_seqno: u32,
        block_id: BlockId,
        estimated_transaction_lsm_bytes: u64,
        estimated_transaction_blob_bytes: u64,
        transaction_index_record_count: u64,
        estimated_block_metadata_bytes: u64,
    ) {
        let commit = PartitionCommit {
            block_id,
            digest: block_id.root_hash,
            transaction_count: (estimated_transaction_lsm_bytes != 0 || estimated_transaction_blob_bytes != 0 || transaction_index_record_count != 0)
                as u64,
            estimated_transaction_lsm_bytes,
            estimated_transaction_blob_bytes,
            transaction_index_record_count,
            estimated_block_metadata_bytes,
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

    fn gc_config(ttl_seconds: u64, keep_tx_per_account: usize) -> TransactionsGcConfig {
        TransactionsGcConfig {
            tx_ttl: Duration::from_secs(ttl_seconds),
            keep_tx_per_account,
            ..Default::default()
        }
    }

    fn rotate_and_seal_gc_source(manager: &mut PartitionManager, mc_seqno: u32) -> PartitionId {
        let source_id = manager.active_id();
        let frontier = masterchain_block_id(mc_seqno);
        write_commit(manager.active_db(), mc_seqno, frontier, 10, 0, 0);
        manager.commit_masterchain_block_set(&frontier).unwrap();
        assert_ne!(manager.active_id(), source_id);
        let worker = manager.begin_sealing(source_id).unwrap();
        let closed = manager.take_sealing_handle(source_id).unwrap();
        drop(closed);
        drop(worker);
        let read_only = manager.open_sealed_read_only(source_id).unwrap();
        manager.complete_sealing(source_id, read_only).unwrap();
        source_id
    }

    fn publish_gc_frontier(manager: &mut PartitionManager, mc_seqno: u32) -> BlockId {
        let frontier = masterchain_block_id(mc_seqno);
        write_commit(manager.active_db(), mc_seqno, frontier, 0, 0, 0);
        manager.commit_masterchain_block_set(&frontier).unwrap();
        frontier
    }

    fn terminal_gc_generation(
        intent: GcIntent,
    ) -> (codec::TailGenerationProgress, codec::TailGenerationCommit) {
        let progress = codec::TailGenerationProgress {
            target_generation: intent.target_generation,
            operation_id: intent.operation_id,
            source_partition_id: intent.source_partition_id,
            source_manifest_digest: intent.source_manifest_digest,
            retention_policy_digest: intent.retention_policy_digest,
            cursor: codec::TailProgressCursor::Start,
            eof: true,
            counters: codec::TailGenerationCounters::default(),
            chunk_digest: codec::EMPTY_TAIL_CHUNK_DIGEST,
        };
        let commit = codec::TailGenerationCommit {
            layout_version: TailLayoutVersion::MonolithicV1,
            target_generation: intent.target_generation,
            operation_id: intent.operation_id,
            source_partition_id: intent.source_partition_id,
            source_manifest_digest: intent.source_manifest_digest,
            previous_visible_generation: intent.previous_visible_generation,
            cutoff_utime: intent.cutoff_utime,
            keep_tx_per_account: intent.keep_tx_per_account,
            retention_policy_digest: intent.retention_policy_digest,
            counters: progress.counters,
        };
        (progress, commit)
    }

    fn commit_gc_cutover_for_deletion(
        manager: &mut PartitionManager,
    ) -> (PartitionId, GcIntent) {
        let source_id = rotate_and_seal_gc_source(manager, 10);
        publish_gc_frontier(manager, 100);
        let evacuating = manager.begin_gc_intent(Some(&gc_config(89, 3))).unwrap().unwrap();
        let prepared = manager.transition_gc_intent_to_prepared(&evacuating).unwrap();
        let (progress, commit) = terminal_gc_generation(prepared);
        let committed = manager.commit_gc_cutover(&prepared, progress, commit).unwrap().intent;
        (source_id, committed)
    }

    fn persist_prepared_gc_intent_for_test(
        manager: &PartitionManager,
        source_id: PartitionId,
    ) -> GcIntent {
        let source = manager.descriptors.get(&source_id).unwrap();
        let intent = GcIntent {
            phase: GcIntentPhase::Prepared,
            operation_id: 1,
            source_partition_id: source_id.0,
            source_manifest_digest: manager.sealed_manifest_digest(source_id).unwrap(),
            target_generation: manager.control_state.tail_visible_generation + 1,
            previous_visible_generation: manager.control_state.tail_visible_generation,
            cutoff_utime: source.last.gen_utime + 1,
            keep_tx_per_account: 0,
            retention_policy_digest: codec::retention_policy_digest(1, 0),
        };
        let mut batch = rocksdb::WriteBatch::default();
        batch.put_cf(
            &manager.control.state.cf(),
            codec::gc_intent_key(),
            codec::encode_gc_intent(&intent).unwrap(),
        );
        manager.write_control(batch).unwrap();
        intent
    }

    fn set_sealed_source_last_transaction_lt(
        manager: &mut PartitionManager,
        source_id: PartitionId,
        transaction_lt: u64,
    ) {
        let descriptor = manager.descriptors.get_mut(&source_id).unwrap();
        descriptor.last.transaction_lt = transaction_lt;
        manager
            .control
            .manifests
            .insert(
                codec::partition_manifest_key(source_id.0),
                codec::encode_manifest(&descriptor.to_manifest()),
            )
            .unwrap();
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
        assert_eq!(
            manager
                .control_db()
                .state
                .get(codec::rpc_layout_marker_key())
                .unwrap()
                .as_deref(),
            Some(codec::rpc_layout_marker_value())
        );
        assert_writable_metrics_registration(&context, &manager);
        drop(manager);
        let reopened = PartitionManager::open(context.clone(), config()).unwrap();
        assert_eq!(reopened.active_id(), PartitionId::FIRST);
        assert_eq!(reopened.descriptors().len(), 1);
        assert_writable_metrics_registration(&context, &reopened);
    }

    #[tokio::test]
    async fn nonempty_markerless_legacy_root_requires_reindex() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let control: RpcControlDb = context.open_preconfigured(CONTROL_SUBDIR).unwrap();
        control.state.insert(b"legacy", [1]).unwrap();
        drop(control);
        let error = PartitionManager::open(context, config()).err().unwrap();
        assert!(format!("{error:#}").contains("clear the RPC DB and perform a full reindex"));
    }

    #[tokio::test]
    async fn explicit_v2_layout_marker_requires_reindex_without_repair() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let v2_marker = b"tycho-rpc-layout-v2";
        let control: RpcControlDb = context.open_preconfigured(CONTROL_SUBDIR).unwrap();
        control.state.insert(codec::rpc_layout_marker_key(), v2_marker).unwrap();
        drop(control);

        let error = PartitionManager::open(context.clone(), config()).err().unwrap();
        assert!(format!("{error:#}").contains("unsupported RPC layout marker"));
        let control: RpcControlDb = context.open_preconfigured(CONTROL_SUBDIR).unwrap();
        assert_eq!(
            control.state.get(codec::rpc_layout_marker_key()).unwrap().as_deref(),
            Some(v2_marker.as_slice()),
        );
        assert!(control.state.get(codec::control_state_key()).unwrap().is_none());
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
            smallest_known_lt: u64::MAX,
            tail_visible_generation: 0,
            tail_layout_version: TailLayoutVersion::MonolithicV1,
            removed_through_partition_id: 0,
        };
        let descriptor = PartitionManager::empty_descriptor(PartitionId::FIRST, ManifestLifecycle::Creating, 1);
        let mut batch = rocksdb::WriteBatch::default();
        batch.put_cf(
            &control.state.cf(),
            codec::rpc_layout_marker_key(),
            codec::rpc_layout_marker_value(),
        );
        batch.put_cf(&control.state.cf(), codec::control_state_key(), codec::encode_control_state(state));
        batch.put_cf(&control.state.cf(), codec::manifest_epoch_key(), codec::encode_manifest_epoch(1));
        batch.put_cf(&control.manifests.cf(), codec::partition_manifest_key(1), codec::encode_manifest(&descriptor.to_manifest()));
        control.rocksdb().write_opt(batch, control.state.write_config()).unwrap();
        drop(control);
        let mut manager = PartitionManager::open(context, config()).unwrap();
        manager.continue_lifecycle().unwrap();
        assert_eq!(manager.active_id(), PartitionId::FIRST);
        assert_eq!(manager.descriptors()[0].lifecycle, ManifestLifecycle::Active);
    }

    #[tokio::test]
    async fn initial_creating_rejects_non_bootstrap_metadata() {
        for case in ["frontier", "counters", "epoch"] {
            let (context, _tmp) = StorageContext::new_temp().await.unwrap();
            let control: RpcControlDb = context.open_preconfigured(CONTROL_SUBDIR).unwrap();
            let state = ControlState {
                node_instance_id: rand::random::<InstanceId>(),
                next_partition_id: 2,
                smallest_known_lt: u64::MAX,
                tail_visible_generation: 0,
                tail_layout_version: TailLayoutVersion::MonolithicV1,
                removed_through_partition_id: 0,
            };
            let mut descriptor = PartitionManager::empty_descriptor(PartitionId::FIRST, ManifestLifecycle::Creating, 1);
            let mut batch = rocksdb::WriteBatch::default();
            batch.put_cf(&control.state.cf(), codec::rpc_layout_marker_key(), codec::rpc_layout_marker_value());
            batch.put_cf(&control.state.cf(), codec::control_state_key(), codec::encode_control_state(state));
            batch.put_cf(&control.state.cf(), codec::manifest_epoch_key(), codec::encode_manifest_epoch(1));
            match case {
                "frontier" => batch.put_cf(&control.state.cf(), codec::visible_frontier_key(), codec::encode_visible_frontier(&masterchain_block_id(1))),
                "counters" => descriptor.counters.transaction_count = 1,
                "epoch" => descriptor.last_transition.epoch = 0,
                _ => unreachable!(),
            }
            batch.put_cf(&control.manifests.cf(), codec::partition_manifest_key(1), codec::encode_manifest(&descriptor.to_manifest()));
            control.rocksdb().write_opt(batch, control.state.write_config()).unwrap();
            drop(control);
            assert!(PartitionManager::open(context, config()).is_err());
        }
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
    async fn startup_rejects_missing_or_non_directory_live_sealed_partition() {
        for replacement in ["missing", "file"] {
            let (context, _tmp) = StorageContext::new_temp().await.unwrap();
            let mut manager = PartitionManager::open(context.clone(), config()).unwrap();
            let sealed_id = rotate_and_seal_gc_source(&mut manager, 10);
            assert!(manager.gc_intent().unwrap().is_none());
            let sealed_path = manager.root.join(PartitionManager::partition_subdir(sealed_id));
            drop(manager);

            fs::remove_dir_all(&sealed_path).unwrap();
            if replacement == "file" {
                fs::write(&sealed_path, []).unwrap();
            }
            let error = PartitionManager::open(context, config()).err().unwrap();
            assert_eq!(
                classify_authoritative_error(&error),
                Some(AuthoritativeErrorKind::MissingCommittedData),
            );
            assert!(format!("{error:#}").contains(
                "committed RPC transaction partition directory is missing or not a directory"
            ));
        }
    }

    #[tokio::test]
    async fn maintenance_opener_classifies_missing_or_non_directory_sealed_partition() {
        for replacement in ["missing", "file"] {
            let (context, _tmp) = StorageContext::new_temp().await.unwrap();
            let mut manager = PartitionManager::open(context, config()).unwrap();
            let sealed_id = rotate_and_seal_gc_source(&mut manager, 10);
            let opener = manager.maintenance_sealed_opener(sealed_id).unwrap();
            let sealed_path = manager.root.join(PartitionManager::partition_subdir(sealed_id));
            drop(manager);

            fs::remove_dir_all(&sealed_path).unwrap();
            if replacement == "file" {
                fs::write(&sealed_path, []).unwrap();
            }
            let error = opener.open().err().unwrap();
            assert_eq!(
                classify_authoritative_error(&error),
                Some(AuthoritativeErrorKind::MissingCommittedData),
            );
            assert!(format!("{error:#}").contains(
                "committed RPC transaction partition directory is missing or not a directory"
            ));
        }
    }

    #[tokio::test]
    async fn request_opener_revalidates_directory_after_successful_uncached_open() {
        for replacement in ["missing", "file"] {
            let (context, _tmp) = StorageContext::new_temp().await.unwrap();
            let mut manager = PartitionManager::open(context, config()).unwrap();
            let sealed_id = rotate_and_seal_gc_source(&mut manager, 10);
            manager.invalidate_sealed_cache_for_test(sealed_id);
            let mut opener = manager.sealed_lease_opener(sealed_id).unwrap();
            let backup_path = manager
                .root
                .join(PartitionManager::partition_subdir(sealed_id))
                .with_extension("post-open");
            opener.post_open_hook = Some(Arc::new(move |path| {
                fs::rename(path, &backup_path).unwrap();
                if replacement == "file" {
                    fs::write(path, []).unwrap();
                }
            }));
            drop(manager);

            let error = opener.open().err().unwrap();
            assert_eq!(
                classify_authoritative_error(&error),
                Some(AuthoritativeErrorKind::MissingCommittedData),
            );
            assert!(format!("{error:#}").contains(
                "committed RPC transaction partition directory is missing or not a directory"
            ));
            assert!(opener.cache.get(&sealed_id).is_none());
        }
    }

    #[tokio::test]
    async fn maintenance_opener_revalidates_directory_after_successful_open() {
        for replacement in ["missing", "file"] {
            let (context, _tmp) = StorageContext::new_temp().await.unwrap();
            let mut manager = PartitionManager::open(context, config()).unwrap();
            let sealed_id = rotate_and_seal_gc_source(&mut manager, 10);
            let mut opener = manager.maintenance_sealed_opener(sealed_id).unwrap();
            let backup_path = manager
                .root
                .join(PartitionManager::partition_subdir(sealed_id))
                .with_extension("post-open");
            opener.post_open_hook = Some(Arc::new(move |path| {
                fs::rename(path, &backup_path).unwrap();
                if replacement == "file" {
                    fs::write(path, []).unwrap();
                }
            }));
            drop(manager);

            let error = opener.open().err().unwrap();
            assert_eq!(
                classify_authoritative_error(&error),
                Some(AuthoritativeErrorKind::MissingCommittedData),
            );
            assert!(format!("{error:#}").contains(
                "committed RPC transaction partition directory is missing or not a directory"
            ));
        }
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
    async fn startup_ignores_unselected_malformed_partition_commit() {
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

        assert!(PartitionManager::open(context, config()).is_ok());
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
    async fn creating_partition_requires_explicit_lifecycle_continuation() {
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
        let mut reopened = PartitionManager::open(context, config()).unwrap();
        assert_eq!(reopened.active_id(), PartitionId::FIRST);
        assert!(reopened.has_creating_partition());
        assert_eq!(reopened.visible_frontier(), Some(&masterchain));
        reopened.continue_lifecycle().unwrap();
        assert_eq!(reopened.active_id(), creating);
        assert_eq!(reopened.select_partition(1), PartitionId::FIRST);
    }

    #[tokio::test]
    async fn creating_recovery_keeps_unpublished_commits_out_of_persisted_state() {
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

        let mut reopened = PartitionManager::open(context, config()).unwrap();

        assert_eq!(reopened.active_id(), PartitionId::FIRST);
        assert!(reopened.has_creating_partition());
        assert_eq!(reopened.visible_frontier(), None);
        assert_eq!(reopened.descriptors.get(&PartitionId::FIRST).unwrap().counters, PartitionCounters::default());
        assert!(reopened.descriptors.get(&PartitionId::FIRST).unwrap().first.block_id.is_none());
        reopened.continue_lifecycle().unwrap();
        assert_eq!(reopened.active_id(), creating);
        let old = reopened.descriptors.get(&PartitionId::FIRST).unwrap();
        assert_eq!(old.lifecycle, ManifestLifecycle::Sealing);
        assert_eq!(old.counters, PartitionCounters::default());
        assert!(old.last.block_id.is_none());
    }

    #[tokio::test]
    async fn creating_completion_does_not_infer_frontier_from_commit_data() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let mut manager = PartitionManager::open(context, config()).unwrap();
        let masterchain = masterchain_block_id(10);
        write_commit(manager.active_db(), 10, masterchain, 0, 0, 0);
        let creating = manager.begin_partition_creation().unwrap();

        manager.complete_partition_creation(creating).unwrap();

        assert_eq!(manager.active_id(), creating);
        assert_eq!(manager.visible_frontier(), None);
    }

    #[tokio::test]
    async fn startup_keeps_active_descriptor_without_commit_reconstruction() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let manager = PartitionManager::open(context.clone(), config()).unwrap();
        let block_id = block_id(1);
        let commit = PartitionCommit {
            block_id,
            digest: block_id.root_hash,
            transaction_count: 2,
            estimated_transaction_lsm_bytes: 3,
            estimated_transaction_blob_bytes: 4,
            transaction_index_record_count: 5,
            estimated_block_metadata_bytes: 6,
            start_lt: 6,
            end_lt: 7,
            gen_utime: 8,
        };
        let mut batch = rocksdb::WriteBatch::default();
        batch.put_cf(&manager.active_db().partition_commits.cf(), codec::partition_commit_key(9, &block_id.as_short_id()), codec::encode_partition_commit(&commit));
        manager.active_db().rocksdb().write_opt(batch, manager.active_db().partition_commits.write_config()).unwrap();
        let persisted_manifest = manager
            .control_db()
            .manifests
            .get(codec::partition_manifest_key(PartitionId::FIRST.0))
            .unwrap()
            .unwrap()
            .to_vec();
        drop(manager);
        let reopened = PartitionManager::open(context, config()).unwrap();
        let descriptor = reopened.descriptors().pop().unwrap();
        assert_eq!(descriptor.counters, PartitionCounters::default());
        assert!(descriptor.first.block_id.is_none());
        assert_eq!(
            reopened
                .control_db()
                .manifests
                .get(codec::partition_manifest_key(PartitionId::FIRST.0))
                .unwrap()
                .unwrap()
                .as_ref(),
            persisted_manifest,
        );
    }

    #[tokio::test]
    async fn startup_ignores_unselected_historical_commit_extrema() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let manager = PartitionManager::open(context.clone(), config()).unwrap();
        let persisted_manifest = manager
            .control_db()
            .manifests
            .get(codec::partition_manifest_key(PartitionId::FIRST.0))
            .unwrap()
            .unwrap()
            .to_vec();
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
                estimated_transaction_lsm_bytes: 1,
                estimated_transaction_blob_bytes: 1,
                transaction_index_record_count: 1,
                estimated_block_metadata_bytes: 1,
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
        assert_eq!(descriptor.counters, PartitionCounters::default());
        assert!(descriptor.first.block_id.is_none());
        assert!(descriptor.last.block_id.is_none());
        assert_eq!(
            reopened
                .control_db()
                .manifests
                .get(codec::partition_manifest_key(PartitionId::FIRST.0))
                .unwrap()
                .unwrap()
                .as_ref(),
            persisted_manifest,
        );
    }

    #[tokio::test]
    async fn rotation_request_is_sticky_and_single_sealer_defers_switch() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let mut manager = PartitionManager::open(context, config()).unwrap();
        assert_eq!(manager.request_rotation(PartitionCounters { estimated_transaction_lsm_bytes: 10, ..Default::default() }), Some(RotationReason::EstimatedTransactionLsmBytes));
        assert_eq!(manager.rotate_if_requested().unwrap().unwrap().0, PartitionId::FIRST);
        assert!(manager.begin_partition_creation().is_err());
        assert_eq!(manager.request_rotation(PartitionCounters { estimated_transaction_blob_bytes: 10, ..Default::default() }), Some(RotationReason::EstimatedTransactionBlobBytes));
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
            .join(PartitionManager::partition_subdir(PartitionId(2)));
        std::fs::write(next_path, []).unwrap();
        manager.request_rotation(PartitionCounters {
            estimated_transaction_lsm_bytes: 10,
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
        assert!(manager.open_sealed_read_only(old).is_err());
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
        manager.request_rotation(PartitionCounters { estimated_transaction_lsm_bytes: 10, ..Default::default() });
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
        assert_eq!(old.counters.estimated_transaction_lsm_bytes, 12);
        assert_eq!(old.counters.transaction_index_record_count, 6);
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
                    "tycho_storage_rpc_partition_active_estimated_transaction_lsm_bytes"
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
                    "tycho_storage_rpc_partition_active_estimated_transaction_lsm_bytes"
                ),
                0.0
            );
            assert_eq!(
                recorder.counter(
                    "tycho_storage_rpc_partition_rotations_total|reason=estimated_transaction_lsm_bytes"
                ),
                1
            );
            assert_eq!(
                recorder.histogram_len("tycho_storage_rpc_partition_rotation_time"),
                1
            );

            let second = masterchain_block_id(11);
            write_commit_with_block_metadata(manager.active_db(), 11, second, 10, 2, 3, 4);
            manager.commit_masterchain_block_set(&second).unwrap();
            assert_eq!(
                recorder.counter(
                    "tycho_storage_rpc_partition_rotation_deferred_total"
                ),
                1
            );
            assert_eq!(
                recorder.gauge(
                    "tycho_storage_rpc_partition_active_estimated_transaction_lsm_bytes"
                ),
                10.0
            );
            assert_eq!(
                recorder.gauge(
                    "tycho_storage_rpc_partition_active_estimated_transaction_blob_bytes"
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
                    "tycho_storage_rpc_partition_active_transaction_index_record_count"
                ),
                3.0
            );
            assert_eq!(
                recorder.gauge(
                    "tycho_storage_rpc_partition_active_estimated_block_metadata_bytes"
                ),
                4.0
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
    async fn observability_uses_only_bounded_rotation_and_gc_lifecycle_labels() {
        let recorder = TestMetricsRecorder::default();
        metrics::with_local_recorder(&recorder, || {
            for reason in [
                RotationReason::EstimatedTransactionLsmBytes,
                RotationReason::EstimatedTransactionBlobBytes,
                RotationReason::TransactionIndexRecordCount,
                RotationReason::EstimatedBlockMetadataBytes,
            ] {
                record_rotation_result(reason, Duration::ZERO, true);
            }
        });

        let mut rotation_keys = recorder
            .keys()
            .into_iter()
            .filter(|key| key.starts_with("tycho_storage_rpc_partition_rotations_total"))
            .collect::<Vec<_>>();
        rotation_keys.sort_unstable();
        assert_eq!(
            rotation_keys,
            [
                "tycho_storage_rpc_partition_rotations_total|reason=estimated_block_metadata_bytes",
                "tycho_storage_rpc_partition_rotations_total|reason=estimated_transaction_blob_bytes",
                "tycho_storage_rpc_partition_rotations_total|reason=estimated_transaction_lsm_bytes",
                "tycho_storage_rpc_partition_rotations_total|reason=transaction_index_record_count",
            ]
            .map(str::to_owned),
        );
        assert!(rotation_keys
            .iter()
            .all(|key| recorder.counter(key) == 1));
        assert_eq!(
            recorder.histogram_len("tycho_storage_rpc_partition_rotation_time"),
            4,
        );

        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let mut manager = PartitionManager::open(context, config()).unwrap();
        let (_, committed) = commit_gc_cutover_for_deletion(&mut manager);
        metrics::with_local_recorder(&recorder, || {
            manager.refresh_lifecycle_metrics();
            assert_eq!(
                recorder.gauge("tycho_storage_rpc_partition_count|lifecycle=retired"),
                1.0,
            );
            assert_eq!(
                recorder.gauge("tycho_storage_rpc_partition_count|lifecycle=deleting"),
                0.0,
            );
            manager
                .transition_gc_intent_to_deleting(&committed)
                .unwrap();
            assert_eq!(
                recorder.gauge("tycho_storage_rpc_partition_count|lifecycle=retired"),
                0.0,
            );
            assert_eq!(
                recorder.gauge("tycho_storage_rpc_partition_count|lifecycle=deleting"),
                1.0,
            );
        });

        let mut lifecycle_keys = recorder
            .keys()
            .into_iter()
            .filter(|key| key.starts_with("tycho_storage_rpc_partition_count|lifecycle="))
            .collect::<Vec<_>>();
        lifecycle_keys.sort_unstable();
        assert_eq!(
            lifecycle_keys,
            [
                "tycho_storage_rpc_partition_count|lifecycle=active",
                "tycho_storage_rpc_partition_count|lifecycle=creating",
                "tycho_storage_rpc_partition_count|lifecycle=deleting",
                "tycho_storage_rpc_partition_count|lifecycle=retired",
                "tycho_storage_rpc_partition_count|lifecycle=sealed",
                "tycho_storage_rpc_partition_count|lifecycle=sealing",
            ]
            .map(str::to_owned),
        );
    }

    #[tokio::test]
    async fn observability_records_predecessor_commit_validation_results() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let manager = PartitionManager::open(context, config()).unwrap();
        let predecessor = masterchain_block_id(1);
        let recorder = TestMetricsRecorder::default();

        metrics::with_local_recorder(&recorder, || {
            assert!(manager.validate_masterchain_commit(&predecessor).is_err());
            write_commit(manager.active_db(), 1, predecessor, 0, 0, 0);
            let key = codec::partition_commit_key(1, &predecessor.as_short_id());
            let value = manager.active_db().partition_commits.get(key).unwrap().unwrap();
            let mut wrong_digest = codec::decode_partition_commit(value.as_ref()).unwrap();
            wrong_digest.digest = HashBytes([0xff; 32]);
            manager
                .active_db()
                .partition_commits
                .insert(key, codec::encode_partition_commit(&wrong_digest))
                .unwrap();
            assert!(manager.validate_masterchain_commit(&predecessor).is_err());
            write_commit(manager.active_db(), 1, predecessor, 0, 0, 0);
            manager.validate_masterchain_commit(&predecessor).unwrap();
        });

        assert_eq!(
            recorder.counter(
                "tycho_storage_rpc_predecessor_validation_failures_total|reason=commit_missing"
            ),
            1
        );
        assert_eq!(
            recorder.counter(
                "tycho_storage_rpc_predecessor_validation_failures_total|reason=commit_digest_mismatch"
            ),
            1
        );
        assert_eq!(
            recorder.counter(
                "tycho_storage_rpc_predecessor_validation_failures_total|reason=full_identity_mismatch"
            ),
            0
        );
        assert_eq!(
            recorder.histogram_len(
                "tycho_storage_rpc_predecessor_validation_duration_seconds|stage=partition_commit|result=failure"
            ),
            2
        );
        assert_eq!(
            recorder.histogram_len(
                "tycho_storage_rpc_predecessor_validation_duration_seconds|stage=partition_commit|result=success"
            ),
            1
        );
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
        write_commit_with_block_metadata(manager.active_db(), 2, block_id(2), 4, 2, 3, 5);
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
        assert_eq!(expected.counters.estimated_transaction_lsm_bytes, 7);
        assert_eq!(expected.counters.estimated_transaction_blob_bytes, 2);
        assert_eq!(expected.counters.transaction_count, 2);
        assert_eq!(expected.counters.transaction_index_record_count, 6);
        assert_eq!(expected.counters.estimated_block_metadata_bytes, 5);
        assert_eq!(expected.last.mc_seqno, 2);
        assert_eq!(expected.last_transition, transition);

        manager.commit_masterchain_block_set(&second).unwrap();

        assert_eq!(manager.active_id(), PartitionId::FIRST);
        assert_eq!(manager.descriptors.get(&PartitionId::FIRST).unwrap(), &expected);
        manager.commit_masterchain_block_set(&third).unwrap();
        assert_eq!(manager.active_id(), PartitionId(2));
        let sealed = manager.descriptors.get(&PartitionId::FIRST).unwrap();
        assert_eq!(sealed.counters.estimated_transaction_lsm_bytes, 12);
        assert_eq!(sealed.last.mc_seqno, 3);
    }

    #[tokio::test]
    async fn replay_boundary_aggregates_unpublished_active_tail_after_read_only_startup() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let manager = PartitionManager::open(context.clone(), config()).unwrap();
        write_commit(manager.active_db(), 2, block_id(1), 6, 0, 2);
        drop(manager);
        let mut manager = PartitionManager::open(context, config()).unwrap();
        assert_eq!(manager.descriptors.get(&PartitionId::FIRST).unwrap().counters, PartitionCounters::default());
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
        assert_eq!(old.counters.estimated_transaction_lsm_bytes, 10);
        assert_eq!(old.counters.transaction_count, 2);
        assert_eq!(old.last.mc_seqno, 2);
    }

    #[tokio::test]
    async fn replay_boundaries_advance_persisted_active_tail_without_startup_reconstruction() {
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
        assert!(manager.descriptors.get(&PartitionId::FIRST).unwrap().last.block_id.is_none());
        assert_eq!(manager.descriptors.get(&PartitionId::FIRST).unwrap().counters, PartitionCounters::default());

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
        for (reason, lsm, blob, records, block_metadata) in [
            (RotationReason::EstimatedTransactionLsmBytes, 10, 0, 0, 0),
            (RotationReason::EstimatedTransactionBlobBytes, 0, 10, 0, 0),
            (RotationReason::TransactionIndexRecordCount, 0, 0, 10, 0),
            (RotationReason::EstimatedBlockMetadataBytes, 0, 0, 0, 10),
        ] {
            let (context, _tmp) = StorageContext::new_temp().await.unwrap();
            let mut manager = PartitionManager::open(context, config()).unwrap();
            let masterchain = masterchain_block_id(10);
            write_commit_with_block_metadata(manager.active_db(), 10, masterchain, lsm, blob, records, block_metadata);
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
    async fn elapsed_time_below_thresholds_never_requests_rotation() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let mut manager = PartitionManager::open(context, config()).unwrap();
        let counters = PartitionCounters {
            estimated_transaction_lsm_bytes: 9,
            estimated_transaction_blob_bytes: 9,
            transaction_index_record_count: 9,
            estimated_block_metadata_bytes: 9,
            ..Default::default()
        };
        let active_id = manager.active_id();
        let manifest_epoch = manager.manifest_epoch;

        assert_eq!(manager.request_rotation(counters), None);
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::ZERO).await;
        assert_eq!(manager.request_rotation(counters), None);
        assert!(manager.rotate_if_requested().unwrap().is_none());
        assert_eq!(manager.rotation_requested(), None);
        assert_eq!(manager.active_id(), active_id);
        assert_eq!(manager.manifest_epoch, manifest_epoch);
    }

    #[tokio::test]
    async fn threshold_or_semantics_rotate_after_one_block_set_overshoot() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let mut manager = PartitionManager::open(context, config()).unwrap();
        assert_eq!(
            manager.threshold_reason(PartitionCounters {
                estimated_transaction_lsm_bytes: 10,
                estimated_transaction_blob_bytes: 10,
                transaction_index_record_count: 10,
                estimated_block_metadata_bytes: 10,
                ..Default::default()
            }),
            Some(RotationReason::EstimatedTransactionLsmBytes)
        );
        assert_eq!(
            manager.request_rotation(PartitionCounters {
                estimated_transaction_lsm_bytes: 9,
                estimated_transaction_blob_bytes: 10,
                transaction_index_record_count: 10,
                estimated_block_metadata_bytes: 10,
                ..Default::default()
            }),
            Some(RotationReason::EstimatedTransactionBlobBytes)
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
        assert_eq!(closed.counters.estimated_transaction_lsm_bytes, 12);
        assert_eq!(closed.counters.estimated_transaction_blob_bytes, 12);
        assert_eq!(closed.counters.transaction_index_record_count, 12);
    }

    #[tokio::test]
    async fn empty_block_metadata_rotates_and_sealing_defers_next_switch() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let mut manager = PartitionManager::open(context, config()).unwrap();
        for seqno in 1..=3 {
            let empty = masterchain_block_id(seqno);
            write_commit_with_block_metadata(manager.active_db(), seqno, empty, 0, 0, 0, 3);
            manager.commit_masterchain_block_set(&empty).unwrap();
            assert_eq!(manager.active_id(), PartitionId::FIRST);
            assert_eq!(manager.rotation_requested(), None);
            assert_eq!(manager.visible_frontier(), Some(&empty));
        }

        let first = masterchain_block_id(4);
        write_commit_with_block_metadata(manager.active_db(), 4, first, 0, 0, 0, 1);
        manager.commit_masterchain_block_set(&first).unwrap();
        assert_eq!(manager.active_id(), PartitionId(2));
        let second = masterchain_block_id(5);
        write_commit_with_block_metadata(manager.active_db(), 5, second, 0, 0, 0, 10);
        manager.commit_masterchain_block_set(&second).unwrap();
        assert_eq!(manager.active_id(), PartitionId(2));
        assert_eq!(manager.rotation_requested(), Some(RotationReason::EstimatedBlockMetadataBytes));
        assert_eq!(manager.visible_frontier(), Some(&second));
    }

    #[tokio::test]
    async fn gc_intent_is_not_created_when_disabled_or_without_a_sealed_candidate() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let mut manager = PartitionManager::open(context, config()).unwrap();
        assert!(manager.begin_gc_intent(None).unwrap().is_none());
        assert!(manager.begin_gc_intent(Some(&gc_config(1, 0))).unwrap().is_none());
        assert!(manager.gc_intent().unwrap().is_none());
    }

    #[tokio::test]
    async fn gc_intent_uses_exact_frontier_cutoff_and_strict_eligibility() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let mut manager = PartitionManager::open(context, config()).unwrap();
        let source_id = rotate_and_seal_gc_source(&mut manager, 10);
        publish_gc_frontier(&mut manager, 100);
        assert!(manager.begin_gc_intent(Some(&gc_config(90, 3))).unwrap().is_none());
        let intent = manager.begin_gc_intent(Some(&gc_config(89, 3))).unwrap().unwrap();
        assert_eq!(intent.phase, GcIntentPhase::Evacuating);
        assert_ne!(intent.operation_id, 0);
        assert_eq!(intent.source_partition_id, source_id.0);
        assert_eq!(intent.source_manifest_digest, manager.sealed_manifest_digest(source_id).unwrap());
        assert_eq!(intent.previous_visible_generation, 0);
        assert_eq!(intent.target_generation, 1);
        assert_eq!(intent.cutoff_utime, 11);
        assert_eq!(intent.keep_tx_per_account, 3);
        assert_eq!(intent.retention_policy_digest, codec::retention_policy_digest(89, 3));
        assert_eq!(manager.gc_intent().unwrap(), Some(intent));
    }

    #[tokio::test]
    async fn gc_intent_at_fixed_frontier_uses_exact_commit_and_resumes_without_config() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let mut manager = PartitionManager::open(context, config()).unwrap();
        let source_id = rotate_and_seal_gc_source(&mut manager, 10);
        let startup_frontier = publish_gc_frontier(&mut manager, 100);
        let later_frontier = publish_gc_frontier(&mut manager, 200);
        assert_eq!(manager.visible_frontier(), Some(&later_frontier));

        let intent = manager
            .begin_gc_intent_at_frontier(Some(&gc_config(89, 3)), &startup_frontier)
            .unwrap()
            .unwrap();
        assert_eq!(intent.source_partition_id, source_id.0);
        assert_eq!(intent.cutoff_utime, 11);

        let missing_frontier = masterchain_block_id(999);
        assert_eq!(manager.begin_gc_intent_at_frontier(None, &missing_frontier).unwrap(), Some(intent));
        assert_eq!(manager.begin_gc_intent_at_frontier(Some(&gc_config(0, 99)), &missing_frontier).unwrap(), Some(intent));
    }

    #[tokio::test]
    async fn gc_intent_selects_only_the_oldest_eligible_sealed_partition() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let mut manager = PartitionManager::open(context, config()).unwrap();
        let oldest = rotate_and_seal_gc_source(&mut manager, 10);
        let newer = rotate_and_seal_gc_source(&mut manager, 20);
        publish_gc_frontier(&mut manager, 100);
        let intent = manager.begin_gc_intent(Some(&gc_config(1, 1))).unwrap().unwrap();
        assert_eq!(intent.source_partition_id, oldest.0);
        assert_ne!(intent.source_partition_id, newer.0);
    }

    #[tokio::test]
    async fn gc_intent_rejects_tail_generation_overflow_without_persisting() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let mut manager = PartitionManager::open(context, config()).unwrap();
        rotate_and_seal_gc_source(&mut manager, 10);
        publish_gc_frontier(&mut manager, 100);
        manager.control_state.tail_visible_generation = u64::MAX;
        let error = manager.begin_gc_intent(Some(&gc_config(1, 1))).unwrap_err();
        assert!(format!("{error:#}").contains("generation overflow"));
        assert!(manager.gc_intent().unwrap().is_none());
    }

    #[tokio::test]
    async fn existing_gc_intent_is_authoritative_and_conflicts_are_rejected() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let mut manager = PartitionManager::open(context, config()).unwrap();
        let source_id = rotate_and_seal_gc_source(&mut manager, 10);
        publish_gc_frontier(&mut manager, 100);
        let intent = manager.begin_gc_intent(Some(&gc_config(89, 3))).unwrap().unwrap();
        assert_eq!(manager.begin_gc_intent(None).unwrap(), Some(intent));
        assert_eq!(manager.begin_gc_intent(Some(&gc_config(1, 99))).unwrap(), Some(intent));
        manager.mutate_sealed_manifest_for_test(source_id).unwrap();
        let error = manager.begin_gc_intent(None).unwrap_err();
        assert!(format!("{error:#}").contains("manifest digest mismatch"));
    }

    #[tokio::test]
    async fn matching_gc_intent_transitions_to_prepared_idempotently() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let mut manager = PartitionManager::open(context, config()).unwrap();
        rotate_and_seal_gc_source(&mut manager, 10);
        publish_gc_frontier(&mut manager, 100);
        let intent = manager.begin_gc_intent(Some(&gc_config(89, 3))).unwrap().unwrap();
        let prepared = manager.transition_gc_intent_to_prepared(&intent).unwrap();
        assert_eq!(prepared.phase, GcIntentPhase::Prepared);
        assert_eq!(manager.gc_intent().unwrap(), Some(prepared));
        assert_eq!(manager.transition_gc_intent_to_prepared(&intent).unwrap(), prepared);
        let conflicting = GcIntent {
            cutoff_utime: intent.cutoff_utime + 1,
            ..intent
        };
        assert!(manager.transition_gc_intent_to_prepared(&conflicting).is_err());
    }

    #[tokio::test]
    async fn gc_cutover_atomically_publishes_generation_retirement_and_next_live_watermark() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let mut manager = PartitionManager::open(context.clone(), config()).unwrap();
        let source_id = rotate_and_seal_gc_source(&mut manager, 10);
        publish_gc_frontier(&mut manager, 100);
        assert_eq!(manager.min_transaction_lt(), u64::MAX);
        let evacuating = manager.begin_gc_intent(Some(&gc_config(89, 3))).unwrap().unwrap();
        let prepared = manager.transition_gc_intent_to_prepared(&evacuating).unwrap();
        let (progress, commit) = terminal_gc_generation(prepared);
        let previous_epoch = manager.manifest_epoch;

        let result = manager.commit_gc_cutover(&prepared, progress, commit).unwrap();

        assert_eq!(result.intent.phase, GcIntentPhase::CutoverCommitted);
        assert_eq!(result.source_partition_id, source_id);
        assert_eq!(result.visible_generation, 1);
        assert_eq!(result.smallest_known_lt, 1000);
        assert_eq!(result.manifest_epoch, previous_epoch + 1);
        assert_eq!(manager.tail_visible_generation(), 1);
        assert_eq!(manager.min_transaction_lt(), 1000);
        assert_eq!(manager.gc_intent().unwrap(), Some(result.intent));
        assert_eq!(manager.descriptors.get(&source_id).unwrap().lifecycle, ManifestLifecycle::Retired);
        let persisted_control = codec::decode_control_state(
            manager.control.state.get(codec::control_state_key()).unwrap().unwrap().as_ref(),
        )
        .unwrap();
        let persisted_source = codec::decode_manifest(
            manager
                .control
                .manifests
                .get(codec::partition_manifest_key(source_id.0))
                .unwrap()
                .unwrap()
                .as_ref(),
        )
        .unwrap();
        assert_eq!(persisted_control.tail_visible_generation, 1);
        assert_eq!(persisted_control.smallest_known_lt, 1000);
        assert_eq!(persisted_source.lifecycle, ManifestLifecycle::Retired);

        drop(manager);
        let mut reopened = PartitionManager::open(context, config()).unwrap();
        assert_eq!(reopened.commit_gc_cutover(&prepared, progress, commit).unwrap(), result);
        assert_eq!(reopened.manifest_epoch, previous_epoch + 1);
        reopened.control_state.smallest_known_lt = u64::MAX;
        let error = reopened.commit_gc_cutover(&prepared, progress, commit).unwrap_err();
        assert_eq!(
            classify_authoritative_error(&error),
            Some(AuthoritativeErrorKind::ConflictingCommittedData),
        );
        reopened.control_state.smallest_known_lt = result.smallest_known_lt;
        assert!(!reopened.persist_min_transaction_lt_decrease(1).unwrap());
        assert_eq!(reopened.min_transaction_lt(), 1000);
    }

    #[tokio::test]
    async fn gc_cutover_uses_source_end_fallback_without_a_nonempty_live_partition() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let mut manager = PartitionManager::open(context.clone(), config()).unwrap();
        let source_id = rotate_and_seal_gc_source(&mut manager, 10);
        assert_eq!(manager.min_transaction_lt(), u64::MAX);
        let prepared = persist_prepared_gc_intent_for_test(&manager, source_id);
        let (progress, commit) = terminal_gc_generation(prepared);

        let result = manager.commit_gc_cutover(&prepared, progress, commit).unwrap();

        assert_eq!(result.smallest_known_lt, 102);
        assert_eq!(manager.min_transaction_lt(), 102);
        let later_frontier = masterchain_block_id(20);
        write_commit(manager.active_db(), 20, later_frontier, 0, 0, 0);
        manager.commit_masterchain_block_set(&later_frontier).unwrap();
        assert_eq!(manager.descriptors.get(&manager.active_id()).unwrap().first.transaction_lt, 200);
        assert_eq!(manager.min_transaction_lt(), 102);
        drop(manager);

        let mut reopened = PartitionManager::open(context, config()).unwrap();
        let replay = reopened.commit_gc_cutover(&prepared, progress, commit).unwrap();
        assert_eq!(replay.smallest_known_lt, 102);
        assert_eq!(reopened.min_transaction_lt(), 102);
    }

    #[tokio::test]
    async fn gc_cutover_source_end_fallback_saturates_at_maximum_lt() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let mut manager = PartitionManager::open(context.clone(), config()).unwrap();
        let source_id = rotate_and_seal_gc_source(&mut manager, 10);
        set_sealed_source_last_transaction_lt(&mut manager, source_id, u64::MAX);
        let prepared = persist_prepared_gc_intent_for_test(&manager, source_id);
        let (progress, commit) = terminal_gc_generation(prepared);

        let result = manager.commit_gc_cutover(&prepared, progress, commit).unwrap();
        assert_eq!(result.smallest_known_lt, u64::MAX);
        assert_eq!(manager.min_transaction_lt(), u64::MAX);
        drop(manager);

        let mut reopened = PartitionManager::open(context, config()).unwrap();
        let replay = reopened.commit_gc_cutover(&prepared, progress, commit).unwrap();
        assert_eq!(replay.smallest_known_lt, u64::MAX);
        assert_eq!(reopened.min_transaction_lt(), u64::MAX);
    }

    #[tokio::test]
    async fn gc_cutover_rejects_conflicting_terminal_state_and_changed_source() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let mut manager = PartitionManager::open(context, config()).unwrap();
        let source_id = rotate_and_seal_gc_source(&mut manager, 10);
        publish_gc_frontier(&mut manager, 100);
        assert!(manager.persist_min_transaction_lt_decrease(100).unwrap());
        let evacuating = manager.begin_gc_intent(Some(&gc_config(89, 3))).unwrap().unwrap();
        let prepared = manager.transition_gc_intent_to_prepared(&evacuating).unwrap();
        let (progress, commit) = terminal_gc_generation(prepared);
        let previous_epoch = manager.manifest_epoch;

        let conflicting_progress = codec::TailGenerationProgress {
            operation_id: progress.operation_id + 1,
            ..progress
        };
        assert!(manager.commit_gc_cutover(&prepared, conflicting_progress, commit).is_err());
        let conflicting_commit = codec::TailGenerationCommit {
            cutoff_utime: commit.cutoff_utime + 1,
            ..commit
        };
        assert!(manager.commit_gc_cutover(&prepared, progress, conflicting_commit).is_err());
        assert_eq!(manager.manifest_epoch, previous_epoch);
        assert_eq!(manager.tail_visible_generation(), 0);
        assert_eq!(manager.descriptors.get(&source_id).unwrap().lifecycle, ManifestLifecycle::Sealed);

        manager.mutate_sealed_manifest_for_test(source_id).unwrap();
        let error = manager.commit_gc_cutover(&prepared, progress, commit).unwrap_err();
        assert!(format!("{error:#}").contains("manifest digest mismatch"));
        assert_eq!(manager.manifest_epoch, previous_epoch);
        assert_eq!(manager.tail_visible_generation(), 0);
    }

    #[tokio::test]
    async fn gc_source_deletion_transition_waits_for_references_and_replays_exactly() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let mut manager = PartitionManager::open(context, config()).unwrap();
        let source_id = rotate_and_seal_gc_source(&mut manager, 10);
        let opener = manager.sealed_lease_opener(source_id).unwrap();
        publish_gc_frontier(&mut manager, 100);
        let evacuating = manager.begin_gc_intent(Some(&gc_config(89, 3))).unwrap().unwrap();
        let prepared = manager.transition_gc_intent_to_prepared(&evacuating).unwrap();
        let (progress, commit) = terminal_gc_generation(prepared);
        let committed = manager.commit_gc_cutover(&prepared, progress, commit).unwrap().intent;
        let manifest_epoch = manager.manifest_epoch;

        let error = manager.transition_gc_intent_to_deleting(&committed).unwrap_err();
        assert!(format!("{error:#}").contains("live references"));
        assert_eq!(manager.gc_intent().unwrap(), Some(committed));
        assert_eq!(manager.descriptors.get(&source_id).unwrap().lifecycle, ManifestLifecycle::Retired);
        drop(opener);

        let deleting = manager.transition_gc_intent_to_deleting(&committed).unwrap();
        assert_eq!(deleting.phase, GcIntentPhase::Deleting);
        assert_eq!(manager.gc_intent().unwrap(), Some(deleting));
        assert_eq!(manager.descriptors.get(&source_id).unwrap().lifecycle, ManifestLifecycle::Deleting);
        assert_eq!(manager.manifest_epoch, manifest_epoch);
        assert_eq!(manager.transition_gc_intent_to_deleting(&committed).unwrap(), deleting);
        assert_eq!(manager.manifest_epoch, manifest_epoch);
        let conflicting = GcIntent {
            operation_id: committed.operation_id + 1,
            ..committed
        };
        let error = manager.transition_gc_intent_to_deleting(&conflicting).unwrap_err();
        assert_eq!(
            classify_authoritative_error(&error),
            Some(AuthoritativeErrorKind::ConflictingCommittedData),
        );
    }

    #[tokio::test]
    async fn gc_source_deletion_finalization_requires_exact_identity_guard_and_absence() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let mut manager = PartitionManager::open(context, config()).unwrap();
        let (source_id, committed) = commit_gc_cutover_for_deletion(&mut manager);
        let prepared = GcIntent {
            phase: GcIntentPhase::Prepared,
            ..committed
        };
        let (progress, commit) = terminal_gc_generation(prepared);
        let deleting = manager.transition_gc_intent_to_deleting(&committed).unwrap();
        let manifest_epoch = manager.manifest_epoch;
        let smallest_known_lt = manager.min_transaction_lt();
        let source_path = manager.root.join(PartitionManager::partition_subdir(source_id));

        let guard = manager.try_acquire_deletion_guard(source_id).unwrap().unwrap();
        let error = manager.finalize_gc_source_deletion(&deleting, guard).unwrap_err();
        assert!(format!("{error:#}").contains("still exists"));
        assert_eq!(manager.gc_intent().unwrap(), Some(deleting));
        assert!(manager.descriptors.contains_key(&source_id));

        fs::remove_dir_all(&source_path).unwrap();
        let conflicting = GcIntent {
            operation_id: deleting.operation_id + 1,
            ..deleting
        };
        let guard = manager.try_acquire_deletion_guard(source_id).unwrap().unwrap();
        let error = manager.finalize_gc_source_deletion(&conflicting, guard).unwrap_err();
        assert!(format!("{error:#}").contains("identity changed"));
        assert_eq!(manager.removed_through_partition_id(), 0);

        let guard = manager.try_acquire_deletion_guard(source_id).unwrap().unwrap();
        assert_eq!(manager.finalize_gc_source_deletion(&deleting, guard).unwrap(), source_id);
        assert_eq!(manager.gc_intent().unwrap(), None);
        assert!(!manager.descriptors.contains_key(&source_id));
        assert_eq!(manager.removed_through_partition_id(), source_id.0);
        assert_eq!(manager.partition_lifetime_strong_count(source_id), None);
        assert_eq!(manager.manifest_epoch, manifest_epoch);
        let persisted = codec::decode_control_state(
            manager.control.state.get(codec::control_state_key()).unwrap().unwrap().as_ref(),
        )
        .unwrap();
        assert_eq!(persisted.removed_through_partition_id, source_id.0);
        assert!(manager.control.manifests.get(codec::partition_manifest_key(source_id.0)).unwrap().is_none());
        for _ in 0..2 {
            let error = manager
                .commit_gc_cutover(&prepared, progress, commit)
                .unwrap_err();
            assert_eq!(
                classify_authoritative_error(&error),
                Some(AuthoritativeErrorKind::MissingCommittedData),
            );
            assert_eq!(manager.min_transaction_lt(), smallest_known_lt);
            assert_eq!(manager.removed_through_partition_id(), source_id.0);
            assert_eq!(manager.gc_intent().unwrap(), None);
        }
    }

    #[tokio::test]
    async fn gc_source_deletion_restarts_at_each_durable_boundary_and_accepts_certified_orphan() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let mut manager = PartitionManager::open(context.clone(), config()).unwrap();
        let (source_id, committed) = commit_gc_cutover_for_deletion(&mut manager);
        let deleting = manager.transition_gc_intent_to_deleting(&committed).unwrap();
        let manifest_epoch = manager.manifest_epoch;
        drop(manager);

        let mut manager = PartitionManager::open(context.clone(), config()).unwrap();
        assert_eq!(manager.gc_intent().unwrap(), Some(deleting));
        assert_eq!(manager.descriptors.first_key_value().unwrap().0, &source_id);
        assert_eq!(manager.descriptors.get(&source_id).unwrap().lifecycle, ManifestLifecycle::Deleting);
        let guard = manager.try_acquire_deletion_guard(source_id).unwrap().unwrap();
        fs::remove_dir_all(guard.path()).unwrap();
        drop(guard);
        drop(manager);

        let mut manager = PartitionManager::open(context.clone(), config()).unwrap();
        assert_eq!(manager.gc_intent().unwrap(), Some(deleting));
        let guard = manager.try_acquire_deletion_guard(source_id).unwrap().unwrap();
        manager.finalize_gc_source_deletion(&deleting, guard).unwrap();
        drop(manager);

        let stale_path = context.root_dir().path().join(PartitionManager::partition_subdir(source_id));
        fs::create_dir_all(&stale_path).unwrap();
        let reopened = PartitionManager::open(context, config()).unwrap();
        assert_eq!(reopened.gc_intent().unwrap(), None);
        assert_eq!(reopened.removed_through_partition_id(), source_id.0);
        assert_eq!(reopened.descriptors.first_key_value().unwrap().0, &PartitionId(source_id.0 + 1));
        assert_eq!(reopened.manifest_epoch, manifest_epoch);
        assert_eq!(
            reopened.certified_removed_partition_directories().unwrap(),
            vec![(source_id, stale_path)],
        );
    }

    #[tokio::test]
    async fn gc_source_deletion_startup_rejects_unowned_cleanup_and_removed_prefix_gap() {
        for case in ["unowned", "removed-prefix"] {
            let (context, _tmp) = StorageContext::new_temp().await.unwrap();
            let mut manager = PartitionManager::open(context.clone(), config()).unwrap();
            let (source_id, committed) = commit_gc_cutover_for_deletion(&mut manager);
            let deleting = manager.transition_gc_intent_to_deleting(&committed).unwrap();
            let mut batch = rocksdb::WriteBatch::default();
            match case {
                "unowned" => batch.delete_cf(&manager.control.state.cf(), codec::gc_intent_key()),
                "removed-prefix" => {
                    let mut state = manager.control_state;
                    state.removed_through_partition_id = source_id.0;
                    batch.put_cf(&manager.control.state.cf(), codec::control_state_key(), codec::encode_control_state(state));
                }
                _ => unreachable!(),
            }
            manager.write_control(batch).unwrap();
            assert_eq!(manager.gc_intent().unwrap(), if case == "unowned" { None } else { Some(deleting) });
            drop(manager);
            assert!(PartitionManager::open(context, config()).is_err(), "case {case} must fail startup validation");
        }
    }

    #[tokio::test]
    async fn pre_cutover_gc_startup_requires_exact_source_directory() {
        for phase in [GcIntentPhase::Evacuating, GcIntentPhase::Prepared] {
            for replacement in ["missing", "file"] {
                let (context, _tmp) = StorageContext::new_temp().await.unwrap();
                let mut manager = PartitionManager::open(context.clone(), config()).unwrap();
                let source_id = rotate_and_seal_gc_source(&mut manager, 10);
                publish_gc_frontier(&mut manager, 100);
                let evacuating = manager.begin_gc_intent(Some(&gc_config(89, 3))).unwrap().unwrap();
                if phase == GcIntentPhase::Prepared {
                    manager.transition_gc_intent_to_prepared(&evacuating).unwrap();
                }
                let source_path = context.root_dir().path().join(PartitionManager::partition_subdir(source_id));
                drop(manager);

                fs::remove_dir_all(&source_path).unwrap();
                if replacement == "file" {
                    fs::write(&source_path, []).unwrap();
                }
                let error = PartitionManager::open(context, config()).err().unwrap();
                assert_eq!(
                    classify_authoritative_error(&error),
                    Some(AuthoritativeErrorKind::MissingCommittedData),
                );
                assert!(
                    format!("{error:#}").contains("pre-cutover source directory is missing or not a directory"),
                    "phase {phase:?} with {replacement} replacement must fail exact source directory validation",
                );
            }
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn pre_cutover_gc_source_metadata_io_remains_retryable() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let mut manager = PartitionManager::open(context.clone(), config()).unwrap();
        let source_id = rotate_and_seal_gc_source(&mut manager, 10);
        publish_gc_frontier(&mut manager, 100);
        let intent = manager.begin_gc_intent(Some(&gc_config(89, 3))).unwrap().unwrap();
        let source_path = context
            .root_dir()
            .path()
            .join(PartitionManager::partition_subdir(source_id));
        let saved_path = source_path.with_extension("metadata-test-backup");
        fs::rename(&source_path, &saved_path).unwrap();
        std::os::unix::fs::symlink(&source_path, &source_path).unwrap();

        let error = manager.validate_pre_cutover_gc_intent(intent).unwrap_err();
        assert_eq!(classify_authoritative_error(&error), None);
        assert!(format!("{error:#}").contains(
            "failed to inspect RPC transaction GC pre-cutover source directory"
        ));

        fs::remove_file(&source_path).unwrap();
        fs::rename(saved_path, source_path).unwrap();
    }

    #[tokio::test]
    async fn gc_intent_requires_the_exact_effective_frontier_commit() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let mut manager = PartitionManager::open(context, config()).unwrap();
        rotate_and_seal_gc_source(&mut manager, 10);
        let frontier = publish_gc_frontier(&mut manager, 100);
        manager
            .active_db()
            .partition_commits
            .remove(codec::partition_commit_key(frontier.seqno, &frontier.as_short_id()))
            .unwrap();
        let error = manager.begin_gc_intent(Some(&gc_config(1, 1))).unwrap_err();
        assert_eq!(
            classify_authoritative_error(&error),
            Some(AuthoritativeErrorKind::MissingCommittedData),
        );
        assert!(format!("{error:#}").contains("missing the RPC frontier commit"));
        assert!(manager.gc_intent().unwrap().is_none());
    }

    #[tokio::test]
    async fn partition_lifetime_blocks_deletion_until_all_references_drain() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let mut manager = PartitionManager::open(context.clone(), config()).unwrap();
        manager.request_rotation(PartitionCounters {
            estimated_transaction_lsm_bytes: 10,
            ..Default::default()
        });
        let sealed_id = manager.rotate_if_requested().unwrap().unwrap().0;
        let worker = manager.begin_sealing(sealed_id).unwrap();
        let closed = manager.take_sealing_handle(sealed_id).unwrap();
        drop(closed);
        drop(worker);
        let read_only = manager.open_sealed_read_only(sealed_id).unwrap();
        manager.complete_sealing(sealed_id, read_only).unwrap();
        manager.sealed_cache.invalidate(&sealed_id);
        manager.sealed_cache.run_pending_tasks();
        assert_eq!(manager.partition_lifetime_strong_count(sealed_id), Some(1));
        let opener = manager.sealed_lease_opener(sealed_id).unwrap();
        let unopened_clone = opener.clone();
        let maintenance_opener = manager.maintenance_sealed_opener(sealed_id).unwrap();
        assert_eq!(manager.partition_lifetime_strong_count(sealed_id), Some(4));
        manager.retire_partition_for_snapshot_test(sealed_id).unwrap();
        assert!(manager.read_lease(sealed_id).is_err());
        assert!(!manager.deletion_references_drained(sealed_id).unwrap());
        let maintenance_lease = maintenance_opener.open().unwrap();
        assert!(!manager.deletion_references_drained(sealed_id).unwrap());
        drop(maintenance_lease);
        drop(unopened_clone);
        assert!(!manager.deletion_references_drained(sealed_id).unwrap());
        // an opener captured before retirement may repopulate the cache after an earlier drain attempt
        let late_lease = opener.open().unwrap();
        drop(opener);
        assert!(!manager.deletion_references_drained(sealed_id).unwrap());
        drop(late_lease);
        assert!(manager.deletion_references_drained(sealed_id).unwrap());
        assert_eq!(manager.partition_lifetime_strong_count(sealed_id), Some(1));
        let descriptor = manager.descriptors.get_mut(&sealed_id).unwrap();
        descriptor.lifecycle = ManifestLifecycle::Deleting;
        descriptor.last_transition.lifecycle = ManifestLifecycle::Deleting;
        let expected_path = context
            .root_dir()
            .path()
            .join(PartitionManager::partition_subdir(sealed_id));
        let guard = manager.try_acquire_deletion_guard(sealed_id).unwrap().unwrap();
        assert_eq!(guard.id(), sealed_id);
        assert_eq!(guard.path(), expected_path.as_path());
        assert_eq!(manager.partition_lifetime_strong_count(sealed_id), Some(2));
        assert!(manager.try_acquire_deletion_guard(sealed_id).unwrap().is_none());
        drop(guard);
        assert!(manager.try_acquire_deletion_guard(sealed_id).unwrap().is_some());
    }

    #[tokio::test]
    async fn cache_eviction_does_not_invalidate_lease() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let manager = PartitionManager::open(context.clone(), config()).unwrap();
        let second = PartitionId(2);
        let third = PartitionId(3);
        for id in [second, third] {
            let _: RpcTransactionsDb = context.open_preconfigured(PartitionManager::partition_subdir(id)).unwrap();
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
