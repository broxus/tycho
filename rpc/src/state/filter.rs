use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::hash::{Hash, Hasher};
use std::io::{ErrorKind, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use bincode::Options;
use parking_lot::{Mutex, RwLock};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tycho_storage::StorageContext;
use tycho_util::sync::CancellationFlag;
use tycho_types::prelude::HashBytes;
use weedb::rocksdb;

use crate::config::RpcTransactionFiltersConfig;

use super::codec::{self, FilterCatalogDescriptor, FilterNamespaceDescriptor, ManifestLifecycle};
use super::db::RpcFilterCatalogDb;
use super::partition::{PartitionId, PartitionManager};

const FILTER_FILE_MAGIC: [u8; 8] = *b"TYCHQF02";
const FILTER_FILE_VERSION: u8 = 1;
const FILTER_FILE_HEADER_LEN: usize = 147;
const FILTER_FILE_DIGEST_LEN: usize = 32;
const FILTER_SERIALIZED_FIXED_LEN: u64 = 19;
const FILTERS_SUBDIR: &str = "rpc/filters";
const FILTER_CATALOG_SUBDIR: &str = "rpc/filter-catalog";

// Bump algorithm_id when membership algorithm or semantics change; hash_scheme_id when the
// hasher, seed, raw-key framing, or hash input changes; key_codec_id when canonical key encoding
// or required length changes; and FILTER_FILE_VERSION when the header, bincode options, or
// qfilter serde payload changes. Inspect fixed serialization and hash vectors for every qfilter
// update. Unknown identifiers are incompatible and must rebuild, never be interpreted as current.

/// The durable boundaries of a single immutable filter-bundle publication.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FilterPublicationStage {
    Vp1,
    Vp2,
    Vp3,
    Vp4,
    Vp5,
    Vp6,
    Vp7,
    Vp8,
    Vp9,
    Vp10,
}

#[cfg(test)]
thread_local! {
    static FILTER_PUBLICATION_STAGE_HOOK: std::cell::RefCell<Option<fn(FilterPublicationStage) -> Result<()>>> = const { std::cell::RefCell::new(None) };
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(super) enum FilterNamespace {
    Transactions = 1,
    InboundMessages = 2,
    Blocks = 3,
}

impl FilterNamespace {
    pub(super) const ALL: [Self; 3] = [Self::Transactions, Self::InboundMessages, Self::Blocks];

    fn from_wire(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::Transactions),
            2 => Ok(Self::InboundMessages),
            3 => Ok(Self::Blocks),
            _ => bail!("unsupported filter namespace id: {value}"),
        }
    }

    pub(super) const fn key_len(self) -> usize {
        match self {
            Self::Transactions | Self::InboundMessages => 32,
            Self::Blocks => 13,
        }
    }

    pub(super) const fn file_name(self) -> &'static str {
        match self {
            Self::Transactions => "transactions.qfilter",
            Self::InboundMessages => "inbound-messages.qfilter",
            Self::Blocks => "blocks.qfilter",
        }
    }

    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Transactions => "transactions",
            Self::InboundMessages => "inbound_messages",
            Self::Blocks => "blocks",
        }
    }
}

#[derive(Clone, Copy)]
enum FilterFailureReason {
    Malformed,
    ChecksumMismatch,
    IncompatibleVersion,
    ManifestMismatch,
    MissingFile,
    Io,
    Cancelled,
    Other,
}

#[derive(Clone, Copy, Debug)]
enum FilterIrreparableReason {
    Oversized,
    InvalidSource,
    GeneratedArtifactInvalid,
    ManifestInvariant,
}

impl FilterIrreparableReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Oversized => "oversized",
            Self::InvalidSource => "invalid_source",
            Self::GeneratedArtifactInvalid => "generated_artifact_invalid",
            Self::ManifestInvariant => "manifest_invariant",
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("irreparable RPC filter maintenance failure ({reason:?}): {source:#}")]
struct IrreparableFilterMaintenanceError {
    reason: FilterIrreparableReason,
    #[source]
    source: anyhow::Error,
}

fn irreparable_filter_error(reason: FilterIrreparableReason, source: anyhow::Error) -> anyhow::Error {
    anyhow::Error::new(IrreparableFilterMaintenanceError { reason, source })
}

fn invalid_source_filter_error(source: anyhow::Error) -> anyhow::Error {
    irreparable_filter_error(FilterIrreparableReason::InvalidSource, source)
}

fn generated_filter_artifact_error(source: anyhow::Error) -> anyhow::Error {
    if irreparable_filter_reason(&source).is_some() {
        source
    } else {
        irreparable_filter_error(FilterIrreparableReason::GeneratedArtifactInvalid, source)
    }
}

fn generated_filter_validation_error(source: anyhow::Error) -> anyhow::Error {
    if irreparable_filter_reason(&source).is_some()
        || source.chain().any(|cause| cause.downcast_ref::<std::io::Error>().is_some())
    {
        source
    } else {
        irreparable_filter_error(FilterIrreparableReason::GeneratedArtifactInvalid, source)
    }
}

fn irreparable_filter_reason(error: &anyhow::Error) -> Option<FilterIrreparableReason> {
    error
        .chain()
        .find_map(|cause| cause.downcast_ref::<IrreparableFilterMaintenanceError>())
        .map(|error| error.reason)
}

impl FilterFailureReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Malformed => "malformed",
            Self::ChecksumMismatch => "checksum_mismatch",
            Self::IncompatibleVersion => "incompatible_version",
            Self::ManifestMismatch => "manifest_mismatch",
            Self::MissingFile => "missing_file",
            Self::Io => "io",
            Self::Cancelled => "cancelled",
            Self::Other => "other",
        }
    }
}

fn classify_filter_failure(error: &anyhow::Error) -> FilterFailureReason {
    if error.chain().any(|cause| {
        cause.downcast_ref::<std::io::Error>().is_some_and(|error| error.kind() == ErrorKind::NotFound)
    }) {
        FilterFailureReason::MissingFile
    } else {
        let message = format!("{error:#}");
        if message.contains("checksum mismatch") {
            FilterFailureReason::ChecksumMismatch
        } else if message.contains("unsupported filter") || message.contains("filter file version") {
            FilterFailureReason::IncompatibleVersion
        } else if message.contains("manifest") {
            FilterFailureReason::ManifestMismatch
        } else if message.contains("cancelled") {
            FilterFailureReason::Cancelled
        } else if error.chain().any(|cause| cause.downcast_ref::<std::io::Error>().is_some()) {
            FilterFailureReason::Io
        } else if message.contains("invalid")
            || message.contains("malformed")
            || message.contains("mismatch")
            || message.contains("truncated")
            || message.contains("deserialize")
        {
            FilterFailureReason::Malformed
        } else {
            FilterFailureReason::Other
        }
    }
}

fn record_filter_event(reason: &'static str) {
    metrics::counter!("tycho_storage_rpc_filter_events_total", "reason" => reason).increment(1);
}

fn record_filter_operation<T>(stage: &'static str, started_at: Instant, result: &Result<T>) {
    let result_label = if result.is_ok() { "success" } else { "failure" };
    metrics::counter!(
        "tycho_storage_rpc_filter_operations_total",
        "stage" => stage,
        "result" => result_label,
    )
    .increment(1);
    metrics::histogram!(
        "tycho_storage_rpc_filter_operation_duration_seconds",
        "stage" => stage,
        "result" => result_label,
    )
    .record(started_at.elapsed());
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
enum FilterAlgorithm {
    QfilterRsqf025 = 1,
}

impl FilterAlgorithm {
    const fn wire(self) -> u8 {
        self as u8
    }

    fn from_wire(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::QfilterRsqf025),
            _ => bail!("unsupported filter algorithm id: {value}"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
enum FilterHashScheme {
    QfilterStableXxh3RawV1 = 1,
}

impl FilterHashScheme {
    const fn wire(self) -> u8 {
        self as u8
    }

    fn from_wire(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::QfilterStableXxh3RawV1),
            _ => bail!("unsupported filter hash scheme id: {value}"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
enum FilterKeyCodec {
    RawBytesV1 = 1,
}

impl FilterKeyCodec {
    const fn wire(self) -> u8 {
        self as u8
    }

    fn from_wire(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::RawBytesV1),
            _ => bail!("unsupported filter key codec id: {value}"),
        }
    }
}

#[derive(Clone, Copy)]
struct RawFilterKey<'a>(&'a [u8]);

impl Hash for RawFilterKey<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(self.0);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct FilterFileIdentity {
    pub(super) namespace: FilterNamespace,
    pub(super) partition_id: u64,
    pub(super) generation_id: u128,
    pub(super) manifest_digest: HashBytes,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct FilterFileMetadata {
    pub(super) identity: FilterFileIdentity,
    pub(super) false_positive_rate_ppm: u32,
    pub(super) source_key_count: u64,
    pub(super) fingerprint_count: u64,
    pub(super) capacity: u64,
    pub(super) fingerprint_size: u8,
    pub(super) resident_bytes: u64,
    pub(super) payload_len: u64,
    pub(super) payload_digest: HashBytes,
}

impl FilterFileMetadata {
    fn algorithm_id() -> u8 {
        FilterAlgorithm::QfilterRsqf025.wire()
    }

    fn hash_scheme_id() -> u8 {
        FilterHashScheme::QfilterStableXxh3RawV1.wire()
    }

    fn key_codec_id() -> u8 {
        FilterKeyCodec::RawBytesV1.wire()
    }

    pub(super) fn to_descriptor(self) -> FilterNamespaceDescriptor {
        FilterNamespaceDescriptor {
            algorithm_id: Self::algorithm_id(),
            hash_scheme_id: Self::hash_scheme_id(),
            key_codec_id: Self::key_codec_id(),
            key_len: self.identity.namespace.key_len() as u8,
            false_positive_rate_ppm: self.false_positive_rate_ppm,
            source_key_count: self.source_key_count,
            fingerprint_count: self.fingerprint_count,
            capacity: self.capacity,
            fingerprint_size: self.fingerprint_size,
            resident_bytes: self.resident_bytes,
            payload_len: self.payload_len,
            payload_digest: self.payload_digest,
        }
    }

    pub(super) fn matches_descriptor(&self, descriptor: FilterNamespaceDescriptor) -> bool {
        self.to_descriptor() == descriptor
    }
}

pub(super) struct EncodedFilterFile {
    pub(super) metadata: FilterFileMetadata,
    pub(super) bytes: Vec<u8>,
}

pub(super) struct DecodedFilterFile {
    pub(super) metadata: FilterFileMetadata,
    filter: qfilter::Filter,
}

impl DecodedFilterFile {
    pub(super) fn into_filter(self) -> ImmutableNamespaceFilter {
        ImmutableNamespaceFilter {
            metadata: self.metadata,
            filter: self.filter,
        }
    }
}

pub(super) struct ImmutableNamespaceFilter {
    metadata: FilterFileMetadata,
    filter: qfilter::Filter,
}

pub(super) struct ValidatedFilterBundle {
    partition_id: u64,
    generation_id: u128,
    manifest_digest: HashBytes,
    transactions: ImmutableNamespaceFilter,
    inbound_messages: ImmutableNamespaceFilter,
    blocks: ImmutableNamespaceFilter,
}

impl ValidatedFilterBundle {
    pub(super) fn new(
        transactions: ImmutableNamespaceFilter,
        inbound_messages: ImmutableNamespaceFilter,
        blocks: ImmutableNamespaceFilter,
        max_bundle_bytes: u64,
    ) -> Result<Self> {
        let transactions_metadata = transactions.metadata();
        let inbound_messages_metadata = inbound_messages.metadata();
        let blocks_metadata = blocks.metadata();
        let identity = transactions_metadata.identity;
        ensure!(identity.namespace == FilterNamespace::Transactions, "invalid transactions filter namespace");
        ensure!(inbound_messages_metadata.identity.namespace == FilterNamespace::InboundMessages, "invalid inbound-message filter namespace");
        ensure!(blocks_metadata.identity.namespace == FilterNamespace::Blocks, "invalid blocks filter namespace");
        for metadata in [inbound_messages_metadata, blocks_metadata] {
            ensure!(metadata.identity.partition_id == identity.partition_id, "filter bundle partition id mismatch");
            ensure!(metadata.identity.generation_id == identity.generation_id, "filter bundle generation id mismatch");
            ensure!(metadata.identity.manifest_digest == identity.manifest_digest, "filter bundle manifest digest mismatch");
        }
        checked_bundle_bytes(
            &[transactions_metadata, inbound_messages_metadata, blocks_metadata],
            max_bundle_bytes,
        )?;
        Ok(Self {
            partition_id: identity.partition_id,
            generation_id: identity.generation_id,
            manifest_digest: identity.manifest_digest,
            transactions,
            inbound_messages,
            blocks,
        })
    }

    pub(super) fn partition_id(&self) -> u64 {
        self.partition_id
    }

    pub(super) fn generation_id(&self) -> u128 {
        self.generation_id
    }

    pub(super) fn manifest_digest(&self) -> HashBytes {
        self.manifest_digest
    }

    #[cfg(test)]
    pub(super) fn resident_bytes(&self) -> u64 {
        self.transactions
            .metadata()
            .resident_bytes
            .checked_add(self.inbound_messages.metadata().resident_bytes)
            .and_then(|value| value.checked_add(self.blocks.metadata().resident_bytes))
            .expect("validated filter bundle resident size")
    }

    #[cfg(test)]
    pub(super) fn bundle_bytes(&self) -> u64 {
        checked_bundle_bytes(
            &[
                self.transactions.metadata(),
                self.inbound_messages.metadata(),
                self.blocks.metadata(),
            ],
            u64::MAX,
        )
        .expect("validated filter bundle size")
    }

    pub(super) fn filter(&self, namespace: FilterNamespace) -> &ImmutableNamespaceFilter {
        match namespace {
            FilterNamespace::Transactions => &self.transactions,
            FilterNamespace::InboundMessages => &self.inbound_messages,
            FilterNamespace::Blocks => &self.blocks,
        }
    }

    fn descriptor(&self) -> FilterCatalogDescriptor {
        FilterCatalogDescriptor {
            partition_id: self.partition_id,
            generation_id: self.generation_id,
            manifest_digest: self.manifest_digest,
            transactions: self.transactions.metadata().to_descriptor(),
            inbound_messages: self.inbound_messages.metadata().to_descriptor(),
            blocks: self.blocks.metadata().to_descriptor(),
        }
    }

    fn record_namespace_metrics(&self) {
        for namespace in FilterNamespace::ALL {
            let metadata = self.filter(namespace).metadata();
            let namespace = namespace.as_str();
            metrics::histogram!(
                "tycho_storage_rpc_filter_source_keys",
                "namespace" => namespace,
            )
            .record(metadata.source_key_count as f64);
            metrics::histogram!(
                "tycho_storage_rpc_filter_fingerprints",
                "namespace" => namespace,
            )
            .record(metadata.fingerprint_count as f64);
            metrics::histogram!(
                "tycho_storage_rpc_filter_capacity",
                "namespace" => namespace,
            )
            .record(metadata.capacity as f64);
            metrics::histogram!(
                "tycho_storage_rpc_filter_fingerprint_size",
                "namespace" => namespace,
            )
            .record(metadata.fingerprint_size as f64);
            metrics::histogram!(
                "tycho_storage_rpc_filter_current_error_ratio",
                "namespace" => namespace,
            )
            .record(self.filter(metadata.identity.namespace).filter.current_error_ratio());
            metrics::histogram!(
                "tycho_storage_rpc_filter_resident_bytes",
                "namespace" => namespace,
            )
            .record(metadata.resident_bytes as f64);
            metrics::histogram!(
                "tycho_storage_rpc_filter_persisted_bytes",
                "namespace" => namespace,
            )
            .record((FILTER_FILE_HEADER_LEN as u64 + metadata.payload_len) as f64);
        }
    }

    fn record_selected_rate_metrics(&self, selected_rates: &FilterSelectedRateMetrics) {
        for namespace in FilterNamespace::ALL {
            let metadata = self.filter(namespace).metadata();
            metrics::gauge!(
                "tycho_storage_rpc_filter_selected_error_ratio",
                "namespace" => namespace.as_str(),
            )
            .set(selected_rates.observe(namespace, metadata.false_positive_rate_ppm) as f64 / 1_000_000.0);
        }
    }
}

#[derive(Default)]
struct FilterSelectedRateMetrics {
    rates: [AtomicU32; 3],
}

impl FilterSelectedRateMetrics {
    fn observe(&self, namespace: FilterNamespace, rate: u32) -> u32 {
        self.rates[namespace as usize - 1].fetch_max(rate, Ordering::AcqRel).max(rate)
    }
}

fn record_configured_namespace_metrics(config: &RpcTransactionFiltersConfig) {
    for (namespace, rate) in [
        (FilterNamespace::Transactions, config.transaction_false_positive_rate_ppm),
        (FilterNamespace::InboundMessages, config.inbound_message_false_positive_rate_ppm),
        (FilterNamespace::Blocks, config.block_false_positive_rate_ppm),
    ] {
        metrics::gauge!(
            "tycho_storage_rpc_filter_configured_error_ratio",
            "namespace" => namespace.as_str(),
        )
        .set(rate as f64 / 1_000_000.0);
    }
}

#[derive(Default)]
pub(super) struct FilterRegistry {
    bundles: RwLock<BTreeMap<PartitionId, Arc<ValidatedFilterBundle>>>,
}

impl FilterRegistry {
    pub(super) fn install(&self, id: PartitionId, bundle: Arc<ValidatedFilterBundle>) {
        debug_assert_eq!(bundle.partition_id(), id.0);
        self.bundles.write().insert(id, bundle);
    }

    pub(super) fn remove(&self, id: PartitionId) {
        self.bundles.write().remove(&id);
    }

    pub(super) fn get(&self, id: PartitionId) -> Option<Arc<ValidatedFilterBundle>> {
        self.bundles.read().get(&id).cloned()
    }
}

pub(super) type FilterPublisher =
    Arc<dyn Fn(PartitionId, Arc<ValidatedFilterBundle>) -> Result<()> + Send + Sync>;

pub(super) struct FilterWorker {
    context: StorageContext,
    config: RpcTransactionFiltersConfig,
    partitions: Arc<Mutex<PartitionManager>>,
    registry: Arc<FilterRegistry>,
    publisher: FilterPublisher,
    pending_sealed: Arc<Mutex<BTreeSet<PartitionId>>>,
    notify: Arc<Notify>,
    cancelled: CancellationFlag,
    publication_gate: Arc<FilterPublicationGate>,
    selected_rate_metrics: Arc<FilterSelectedRateMetrics>,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl FilterWorker {
    pub(super) fn new(
        context: StorageContext,
        config: RpcTransactionFiltersConfig,
        partitions: Arc<Mutex<PartitionManager>>,
        registry: Arc<FilterRegistry>,
        publisher: FilterPublisher,
    ) -> Self {
        Self {
            context,
            config,
            partitions,
            registry,
            publisher,
            pending_sealed: Default::default(),
            notify: Arc::new(Notify::new()),
            cancelled: CancellationFlag::new(),
            publication_gate: Default::default(),
            selected_rate_metrics: Default::default(),
            task: Default::default(),
        }
    }

    pub(super) fn start(&self) {
        let mut task = self.task.lock();
        if task.is_some() {
            return;
        }
        *task = Some(tokio::spawn(run_filter_worker(
            self.context.clone(),
            self.config.clone(),
            self.partitions.clone(),
            self.registry.clone(),
            self.publisher.clone(),
            self.pending_sealed.clone(),
            self.notify.clone(),
            self.cancelled.clone(),
            self.publication_gate.clone(),
            self.selected_rate_metrics.clone(),
        )));
    }

    pub(super) fn notify_sealed(&self, id: PartitionId) {
        self.pending_sealed.lock().insert(id);
        self.notify.notify_one();
    }

    pub(super) fn shutdown(&self) {
        self.publication_gate.cancel(&self.cancelled);
        if let Some(task) = self.task.lock().take() {
            task.abort();
        }
    }

    #[cfg(test)]
    pub(super) fn pending_sealed_contains(&self, id: PartitionId) -> bool {
        self.pending_sealed.lock().contains(&id)
    }
}

#[derive(Default)]
struct FilterPublicationGate {
    lock: Mutex<()>,
}

impl FilterPublicationGate {
    fn publish<T>(&self, cancelled: &CancellationFlag, f: impl FnOnce() -> Result<T>) -> Result<T> {
        let _lock = self.lock.lock();
        check_cancelled(cancelled)?;
        f()
    }

    fn cancel(&self, cancelled: &CancellationFlag) {
        cancelled.cancel();
        let _lock = self.lock.lock();
    }
}

struct FilterWorkQueue {
    priority: BTreeSet<PartitionId>,
    pending: BTreeSet<PartitionId>,
    retry_deadlines: BTreeMap<PartitionId, Instant>,
    retry_delays: BTreeMap<PartitionId, Duration>,
    enqueued_at: BTreeMap<PartitionId, Instant>,
}

#[derive(Clone, Copy)]
enum FilterCatalogRecordStatus {
    PendingValidation,
    Validated,
    Unfiltered,
    Failed,
    Oversized,
    InvalidSource,
    GeneratedArtifactInvalid,
    ManifestInvariant,
}

impl FilterCatalogRecordStatus {
    const ALL: [Self; 8] = [
        Self::PendingValidation,
        Self::Validated,
        Self::Unfiltered,
        Self::Failed,
        Self::Oversized,
        Self::InvalidSource,
        Self::GeneratedArtifactInvalid,
        Self::ManifestInvariant,
    ];

    const fn as_str(self) -> &'static str {
        match self {
            Self::PendingValidation => "pending_validation",
            Self::Validated => "validated",
            Self::Unfiltered => "unfiltered",
            Self::Failed => "failed",
            Self::Oversized => "oversized",
            Self::InvalidSource => "invalid_source",
            Self::GeneratedArtifactInvalid => "generated_artifact_invalid",
            Self::ManifestInvariant => "manifest_invariant",
        }
    }
}

impl From<FilterIrreparableReason> for FilterCatalogRecordStatus {
    fn from(reason: FilterIrreparableReason) -> Self {
        match reason {
            FilterIrreparableReason::Oversized => Self::Oversized,
            FilterIrreparableReason::InvalidSource => Self::InvalidSource,
            FilterIrreparableReason::GeneratedArtifactInvalid => Self::GeneratedArtifactInvalid,
            FilterIrreparableReason::ManifestInvariant => Self::ManifestInvariant,
        }
    }
}

fn refresh_catalog_metrics(statuses: &BTreeMap<PartitionId, FilterCatalogRecordStatus>) {
    for status in FilterCatalogRecordStatus::ALL {
        let count = statuses
            .values()
            .filter(|current| current.as_str() == status.as_str())
            .count();
        metrics::gauge!(
            "tycho_storage_rpc_filter_catalog_records",
            "result" => status.as_str(),
        )
        .set(count as f64);
    }
}

fn complete_irreparable_filter_work(
    queue: &mut FilterWorkQueue,
    statuses: &mut BTreeMap<PartitionId, FilterCatalogRecordStatus>,
    irreparable: &mut BTreeSet<PartitionId>,
    id: PartitionId,
    reason: FilterIrreparableReason,
) {
    statuses.insert(id, reason.into());
    refresh_catalog_metrics(statuses);
    record_filter_event(reason.as_str());
    queue.complete(id);
    irreparable.insert(id);
    queue.refresh_metrics(Instant::now());
}

fn prioritize_filter_work(
    queue: &mut FilterWorkQueue,
    statuses: &mut BTreeMap<PartitionId, FilterCatalogRecordStatus>,
    irreparable: &BTreeSet<PartitionId>,
    partitions: impl IntoIterator<Item = PartitionId>,
) {
    let partitions = partitions
        .into_iter()
        .filter(|id| !irreparable.contains(id))
        .collect::<Vec<_>>();
    for id in &partitions {
        statuses.entry(*id).or_insert(FilterCatalogRecordStatus::Unfiltered);
    }
    queue.prioritize(partitions);
}

fn publisher_failure_has_manifest_invariant(
    partitions: &Mutex<PartitionManager>,
    id: PartitionId,
    bundle: &ValidatedFilterBundle,
) -> bool {
    !partitions
        .lock()
        .sealed_manifest_digest(id)
        .is_ok_and(|digest| digest == bundle.manifest_digest())
}

impl FilterWorkQueue {
    fn new(partitions: impl IntoIterator<Item = PartitionId>) -> Self {
        let now = Instant::now();
        let pending = partitions.into_iter().collect::<BTreeSet<_>>();
        Self {
            priority: Default::default(),
            enqueued_at: pending.iter().map(|id| (*id, now)).collect(),
            pending,
            retry_deadlines: Default::default(),
            retry_delays: Default::default(),
        }
    }

    fn prioritize(&mut self, partitions: impl IntoIterator<Item = PartitionId>) {
        for id in partitions {
            self.pending.remove(&id);
            self.retry_deadlines.remove(&id);
            self.enqueued_at.entry(id).or_insert_with(Instant::now);
            self.priority.insert(id);
        }
    }

    fn next_ready(&mut self, now: Instant) -> Option<PartitionId> {
        if let Some(id) = self.priority.pop_last() {
            return Some(id);
        }
        let retry = self
            .retry_deadlines
            .iter()
            .filter(|(_, deadline)| **deadline <= now)
            .map(|(id, _)| *id)
            .max();
        let id = self.pending.last().copied().max(retry)?;
        self.pending.remove(&id);
        self.retry_deadlines.remove(&id);
        Some(id)
    }

    fn retry(&mut self, id: PartitionId, now: Instant) -> Duration {
        let delay = self
            .retry_delays
            .entry(id)
            .or_insert(Duration::from_secs(1));
        let current = *delay;
        *delay = delay.saturating_mul(2).min(Duration::from_secs(30));
        self.retry_deadlines.insert(id, now + current);
        self.enqueued_at.entry(id).or_insert(now);
        metrics::counter!(
            "tycho_storage_rpc_filter_operations_total",
            "stage" => "retry",
            "result" => "scheduled",
        )
        .increment(1);
        metrics::histogram!(
            "tycho_storage_rpc_filter_operation_duration_seconds",
            "stage" => "retry",
            "result" => "scheduled",
        )
        .record(current);
        current
    }

    fn complete(&mut self, id: PartitionId) {
        self.retry_deadlines.remove(&id);
        self.retry_delays.remove(&id);
        self.enqueued_at.remove(&id);
    }

    fn next_deadline(&self) -> Option<Instant> {
        self.retry_deadlines.values().copied().min()
    }

    fn refresh_metrics(&self, now: Instant) {
        metrics::gauge!("tycho_storage_rpc_filter_queue_depth").set(self.enqueued_at.len() as f64);
        let oldest_age = self
            .enqueued_at
            .values()
            .map(|queued_at| now.saturating_duration_since(*queued_at))
            .max()
            .unwrap_or_default();
        metrics::gauge!("tycho_storage_rpc_filter_queue_oldest_age_seconds")
            .set(oldest_age.as_secs_f64());
    }
}

impl ImmutableNamespaceFilter {
    pub(super) fn metadata(&self) -> FilterFileMetadata {
        self.metadata
    }

    pub(super) fn contains(&self, key: &[u8]) -> Result<bool> {
        validate_key(self.metadata.identity.namespace, key)?;
        Ok(self.filter.contains(RawFilterKey(key)))
    }
}

pub(super) struct FilterBuilder {
    identity: FilterFileIdentity,
    false_positive_rate_ppm: u32,
    source_key_count: u64,
    inserted_key_count: u64,
    filter: qfilter::Filter,
}

impl FilterBuilder {
    pub(super) fn new(
        identity: FilterFileIdentity,
        source_key_count: u64,
        false_positive_rate_ppm: u32,
    ) -> Result<Self> {
        validate_false_positive_rate(false_positive_rate_ppm)?;
        let requested_capacity = source_key_count.max(1);
        let filter = qfilter::Filter::new(
            requested_capacity,
            false_positive_rate_ppm as f64 / 1_000_000.0,
        )
        .context("failed to construct qfilter")?;
        Ok(Self {
            identity,
            false_positive_rate_ppm,
            source_key_count,
            inserted_key_count: 0,
            filter,
        })
    }

    pub(super) fn insert(&mut self, key: &[u8]) -> Result<bool> {
        validate_key(self.identity.namespace, key).map_err(invalid_source_filter_error)?;
        self.inserted_key_count = self
            .inserted_key_count
            .checked_add(1)
            .context("filter source key count overflow")
            .map_err(invalid_source_filter_error)?;
        if self.inserted_key_count > self.source_key_count {
            return Err(invalid_source_filter_error(anyhow::anyhow!("filter received more keys than counted")));
        }
        self.filter
            .insert(RawFilterKey(key))
            .context("failed to insert filter key")
            .map_err(|error| irreparable_filter_error(FilterIrreparableReason::Oversized, error))
    }

    pub(super) fn finish(self) -> Result<ImmutableNamespaceFilter> {
        if self.inserted_key_count != self.source_key_count {
            return Err(invalid_source_filter_error(anyhow::anyhow!("filter source key count changed during build")));
        }
        let metadata = metadata_from_filter(
            self.identity,
            self.false_positive_rate_ppm,
            self.source_key_count,
            &self.filter,
            0,
            HashBytes::ZERO,
        )?;
        Ok(ImmutableNamespaceFilter {
            metadata,
            filter: self.filter,
        })
    }
}

impl ImmutableNamespaceFilter {
    pub(super) fn encode(self) -> Result<EncodedFilterFile> {
        let started_at = Instant::now();
        let result = (|| {
            let mut payload = bincode_options()
                .serialize(&self.filter)
                .context("failed to serialize qfilter")?;
            // qfilter omits its final None option field despite bincode's positional struct encoding
            payload.push(0);
            let payload_len = u64::try_from(payload.len()).context("filter payload length overflow")?;
            let payload_digest = HashBytes::from_slice(blake3::hash(&payload).as_bytes());
            let metadata = metadata_from_filter(
                self.metadata.identity,
                self.metadata.false_positive_rate_ppm,
                self.metadata.source_key_count,
                &self.filter,
                payload_len,
                payload_digest,
            )?;
            let mut bytes = Vec::with_capacity(
                FILTER_FILE_HEADER_LEN
                    .checked_add(payload.len())
                    .context("filter file length overflow")?,
            );
            encode_header(&metadata, &mut bytes);
            bytes.extend_from_slice(&payload);
            Ok(EncodedFilterFile { metadata, bytes })
        })();
        record_filter_operation("serialization", started_at, &result);
        result
    }
}

pub(super) fn decode_filter_file(
    bytes: &[u8],
    expected_identity: FilterFileIdentity,
    max_file_bytes: u64,
) -> Result<DecodedFilterFile> {
    let file_len = u64::try_from(bytes.len()).context("filter file length overflow")?;
    ensure!(file_len <= max_file_bytes, "filter file exceeds configured size limit");
    ensure!(bytes.len() >= FILTER_FILE_HEADER_LEN, "truncated filter file header");
    let metadata = decode_header(&bytes[..FILTER_FILE_HEADER_LEN])?;
    ensure!(metadata.identity == expected_identity, "filter file identity mismatch");
    let payload_len = usize::try_from(metadata.payload_len).context("filter payload length does not fit usize")?;
    let expected_len = FILTER_FILE_HEADER_LEN
        .checked_add(payload_len)
        .context("filter file length overflow")?;
    ensure!(bytes.len() == expected_len, "filter file length mismatch");
    let payload = &bytes[FILTER_FILE_HEADER_LEN..];
    ensure!(HashBytes::from_slice(blake3::hash(payload).as_bytes()) == metadata.payload_digest, "filter payload checksum mismatch");
    let filter: qfilter::Filter = bincode_options()
        .with_limit(metadata.payload_len)
        .deserialize(payload)
        .context("failed to deserialize qfilter")?;
    validate_runtime_metadata(&metadata, &filter)?;
    Ok(DecodedFilterFile { metadata, filter })
}

pub(super) fn checked_bundle_bytes(
    filters: &[FilterFileMetadata; 3],
    max_bundle_bytes: u64,
) -> Result<u64> {
    let total = filters.iter().try_fold(0u64, |total, metadata| {
        let file_bytes = u64::try_from(FILTER_FILE_HEADER_LEN)
            .context("filter header length overflow")?
            .checked_add(metadata.payload_len)
            .context("filter bundle file length overflow")?;
        total
            .checked_add(file_bytes)
            .and_then(|value| value.checked_add(metadata.resident_bytes))
            .context("filter bundle size overflow")
    })?;
    ensure!(total <= max_bundle_bytes, "filter bundle exceeds configured size limit");
    Ok(total)
}

fn checked_descriptor_bundle_bytes(
    filters: &[FilterNamespaceDescriptor; 3],
    max_bundle_bytes: u64,
) -> Result<u64> {
    let total = filters.iter().try_fold(0u64, |total, metadata| {
        let file_bytes = u64::try_from(FILTER_FILE_HEADER_LEN)
            .context("filter header length overflow")?
            .checked_add(metadata.payload_len)
            .context("filter bundle file length overflow")?;
        total
            .checked_add(file_bytes)
            .and_then(|value| value.checked_add(metadata.resident_bytes))
            .context("filter bundle size overflow")
    })?;
    ensure!(total <= max_bundle_bytes, "filter bundle exceeds configured size limit");
    Ok(total)
}

fn encode_header(metadata: &FilterFileMetadata, target: &mut Vec<u8>) {
    target.extend_from_slice(&FILTER_FILE_MAGIC);
    target.push(FILTER_FILE_VERSION);
    target.push(metadata.identity.namespace as u8);
    target.extend_from_slice(&metadata.identity.partition_id.to_le_bytes());
    target.extend_from_slice(&metadata.identity.generation_id.to_le_bytes());
    target.extend_from_slice(metadata.identity.manifest_digest.as_ref());
    target.push(FilterFileMetadata::algorithm_id());
    target.push(FilterFileMetadata::hash_scheme_id());
    target.push(FilterFileMetadata::key_codec_id());
    target.push(metadata.identity.namespace.key_len() as u8);
    target.extend_from_slice(&metadata.false_positive_rate_ppm.to_le_bytes());
    target.extend_from_slice(&metadata.source_key_count.to_le_bytes());
    target.extend_from_slice(&metadata.fingerprint_count.to_le_bytes());
    target.extend_from_slice(&metadata.capacity.to_le_bytes());
    target.push(metadata.fingerprint_size);
    target.extend_from_slice(&metadata.resident_bytes.to_le_bytes());
    target.extend_from_slice(&metadata.payload_len.to_le_bytes());
    target.extend_from_slice(metadata.payload_digest.as_ref());
    debug_assert_eq!(target.len(), FILTER_FILE_HEADER_LEN);
}

fn decode_header(bytes: &[u8]) -> Result<FilterFileMetadata> {
    ensure!(bytes.len() == FILTER_FILE_HEADER_LEN, "invalid filter file header length");
    ensure!(bytes[..8] == FILTER_FILE_MAGIC, "invalid filter file magic");
    ensure!(bytes[8] == FILTER_FILE_VERSION, "unsupported filter file version: {}", bytes[8]);
    let namespace = FilterNamespace::from_wire(bytes[9])?;
    let partition_id = u64::from_le_bytes(bytes[10..18].try_into().unwrap());
    let generation_id = u128::from_le_bytes(bytes[18..34].try_into().unwrap());
    let manifest_digest = HashBytes::from_slice(&bytes[34..66]);
    FilterAlgorithm::from_wire(bytes[66])?;
    FilterHashScheme::from_wire(bytes[67])?;
    FilterKeyCodec::from_wire(bytes[68])?;
    ensure!(bytes[69] == namespace.key_len() as u8, "invalid filter key length");
    let false_positive_rate_ppm = u32::from_le_bytes(bytes[70..74].try_into().unwrap());
    validate_false_positive_rate(false_positive_rate_ppm)?;
    let source_key_count = u64::from_le_bytes(bytes[74..82].try_into().unwrap());
    let fingerprint_count = u64::from_le_bytes(bytes[82..90].try_into().unwrap());
    let capacity = u64::from_le_bytes(bytes[90..98].try_into().unwrap());
    let fingerprint_size = bytes[98];
    let resident_bytes = u64::from_le_bytes(bytes[99..107].try_into().unwrap());
    let payload_len = u64::from_le_bytes(bytes[107..115].try_into().unwrap());
    let payload_digest = HashBytes::from_slice(&bytes[115..115 + FILTER_FILE_DIGEST_LEN]);
    validate_declared_metadata(
        source_key_count,
        fingerprint_count,
        capacity,
        fingerprint_size,
        resident_bytes,
    )?;
    ensure!(payload_len > 0, "invalid filter payload length");
    Ok(FilterFileMetadata {
        identity: FilterFileIdentity {
            namespace,
            partition_id,
            generation_id,
            manifest_digest,
        },
        false_positive_rate_ppm,
        source_key_count,
        fingerprint_count,
        capacity,
        fingerprint_size,
        resident_bytes,
        payload_len,
        payload_digest,
    })
}

fn metadata_from_filter(
    identity: FilterFileIdentity,
    false_positive_rate_ppm: u32,
    source_key_count: u64,
    filter: &qfilter::Filter,
    payload_len: u64,
    payload_digest: HashBytes,
) -> Result<FilterFileMetadata> {
    let metadata = FilterFileMetadata {
        identity,
        false_positive_rate_ppm,
        source_key_count,
        fingerprint_count: filter.len(),
        capacity: filter.capacity(),
        fingerprint_size: filter.fingerprint_size(),
        resident_bytes: u64::try_from(filter.memory_usage()).context("filter resident size overflow")?,
        payload_len,
        payload_digest,
    };
    validate_declared_metadata(
        metadata.source_key_count,
        metadata.fingerprint_count,
        metadata.capacity,
        metadata.fingerprint_size,
        metadata.resident_bytes,
    )?;
    Ok(metadata)
}

fn validate_runtime_metadata(metadata: &FilterFileMetadata, filter: &qfilter::Filter) -> Result<()> {
    ensure!(metadata.fingerprint_count == filter.len(), "filter fingerprint count mismatch");
    ensure!(metadata.capacity == filter.capacity(), "filter capacity mismatch");
    ensure!(metadata.fingerprint_size == filter.fingerprint_size(), "filter fingerprint size mismatch");
    ensure!(metadata.resident_bytes == u64::try_from(filter.memory_usage()).context("filter resident size overflow")?, "filter resident size mismatch");
    Ok(())
}

fn validate_declared_metadata(
    source_key_count: u64,
    fingerprint_count: u64,
    capacity: u64,
    fingerprint_size: u8,
    resident_bytes: u64,
) -> Result<()> {
    ensure!(capacity > 0, "invalid filter capacity");
    ensure!(fingerprint_count <= source_key_count, "filter fingerprint count exceeds source key count");
    ensure!(fingerprint_count <= capacity, "filter fingerprint count exceeds capacity");
    ensure!((7..=64).contains(&fingerprint_size), "invalid filter fingerprint size");
    ensure!(resident_bytes > 0, "invalid filter resident size");
    Ok(())
}

fn validate_false_positive_rate(value: u32) -> Result<()> {
    ensure!((1..=500_000).contains(&value), "invalid filter false-positive rate: {value}");
    Ok(())
}

fn validate_key(namespace: FilterNamespace, key: &[u8]) -> Result<()> {
    ensure!(key.len() == namespace.key_len(), "invalid {:?} filter key length: expected {}, got {}", namespace, namespace.key_len(), key.len());
    Ok(())
}

fn bincode_options() -> impl Options {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_little_endian()
        .reject_trailing_bytes()
}

/// Owns the optional filter catalog after RPC correctness readiness has completed.
pub(super) struct FilterCatalogStore {
    catalog: RpcFilterCatalogDb,
    filters_root: PathBuf,
    config: RpcTransactionFiltersConfig,
    publication_gate: RwLock<Option<Arc<FilterPublicationGate>>>,
}

struct EncodedFilterBundle {
    transactions: EncodedFilterFile,
    inbound_messages: EncodedFilterFile,
    blocks: EncodedFilterFile,
}

impl EncodedFilterBundle {
    fn metadata(&self) -> [FilterFileMetadata; 3] {
        [
            self.transactions.metadata,
            self.inbound_messages.metadata,
            self.blocks.metadata,
        ]
    }

    fn descriptor(&self) -> FilterCatalogDescriptor {
        let identity = self.transactions.metadata.identity;
        FilterCatalogDescriptor {
            partition_id: identity.partition_id,
            generation_id: identity.generation_id,
            manifest_digest: identity.manifest_digest,
            transactions: self.transactions.metadata.to_descriptor(),
            inbound_messages: self.inbound_messages.metadata.to_descriptor(),
            blocks: self.blocks.metadata.to_descriptor(),
        }
    }

    fn files(&self) -> [(FilterNamespace, &EncodedFilterFile); 3] {
        [
            (FilterNamespace::Transactions, &self.transactions),
            (FilterNamespace::InboundMessages, &self.inbound_messages),
            (FilterNamespace::Blocks, &self.blocks),
        ]
    }
}

impl FilterCatalogStore {
    /// Opens mutable acceleration metadata only after the local-only snapshot is ready.
    pub(super) fn open(
        context: &StorageContext,
        config: RpcTransactionFiltersConfig,
    ) -> Result<Self> {
        config.validate().map_err(anyhow::Error::msg)?;
        let filters_root = context.root_dir().path().join(FILTERS_SUBDIR);
        let filters_root_exists = filters_root.exists();
        fs::create_dir_all(&filters_root)
            .with_context(|| format!("failed to create RPC filter root at {}", filters_root.display()))?;
        if !filters_root_exists {
            sync_directory(filters_root.parent().context("RPC filter root has no parent directory")?)?;
        }
        Ok(Self {
            catalog: context.open_preconfigured(FILTER_CATALOG_SUBDIR)?,
            filters_root,
            config,
            publication_gate: Default::default(),
        })
    }

    fn set_publication_gate(&self, publication_gate: Arc<FilterPublicationGate>) {
        *self.publication_gate.write() = Some(publication_gate);
    }

    fn publish<T>(&self, cancelled: &CancellationFlag, f: impl FnOnce() -> Result<T>) -> Result<T> {
        match self.publication_gate.read().clone() {
            Some(publication_gate) => publication_gate.publish(cancelled, f),
            None => {
                check_cancelled(cancelled)?;
                f()
            }
        }
    }

    fn catalog_descriptors(
        &self,
        cancelled: &CancellationFlag,
    ) -> Result<BTreeMap<PartitionId, FilterCatalogDescriptor>> {
        let mut result = BTreeMap::new();
        let mut iterator = self
            .catalog
            .rocksdb()
            .raw_iterator_cf(&self.catalog.descriptors.cf());
        iterator.seek_to_first();
        while iterator.valid() {
            check_cancelled(cancelled)?;
            let key = iterator
                .key()
                .context("filter catalog iterator returned no key")?;
            let value = iterator
                .value()
                .context("filter catalog iterator returned no value")?;
            let id = match <[u8; 8]>::try_from(key) {
                Ok(key) => PartitionId(u64::from_be_bytes(key)),
                Err(_) => {
                    record_filter_event("malformed_catalog_key");
                    tracing::warn!(key_len = key.len(), "ignoring malformed RPC filter catalog key");
                    iterator.next();
                    continue;
                }
            };
            match codec::decode_filter_catalog_descriptor(value) {
                Ok(descriptor) if descriptor.partition_id == id.0 => {
                    result.insert(id, descriptor);
                }
                Ok(_) => {
                    record_filter_event("malformed_catalog_descriptor");
                    tracing::warn!(partition_id = id.0, "ignoring RPC filter catalog descriptor with a mismatched partition id");
                }
                Err(e) => {
                    record_filter_event("malformed_catalog_descriptor");
                    tracing::warn!(partition_id = id.0, "ignoring malformed RPC filter catalog descriptor: {e:#}");
                }
            }
            iterator.next();
        }
        iterator
            .status()
            .context("failed to iterate RPC filter catalog")?;
        Ok(result)
    }

    pub(super) fn build_and_publish(
        &self,
        partitions: &parking_lot::Mutex<PartitionManager>,
        id: PartitionId,
        cancelled: &CancellationFlag,
    ) -> Result<Arc<ValidatedFilterBundle>> {
        check_cancelled(cancelled)?;
        let (manifest_digest, opener) = {
            let partitions = partitions.lock();
            (
                partitions.sealed_manifest_digest(id)
                    .map_err(|error| irreparable_filter_error(FilterIrreparableReason::ManifestInvariant, error))?,
                partitions.maintenance_sealed_opener(id)?,
            )
        };
        publication_stage(FilterPublicationStage::Vp1, cancelled)?;

        let db = opener.open()?;
        let generation_id = self.next_generation_id(id)?;
        let identity = |namespace| FilterFileIdentity {
            namespace,
            partition_id: id.0,
            generation_id,
            manifest_digest,
        };
        let source_key_counts = [
            count_source_keys(&db, FilterNamespace::Transactions, cancelled)?,
            count_source_keys(&db, FilterNamespace::InboundMessages, cancelled)?,
            count_source_keys(&db, FilterNamespace::Blocks, cancelled)?,
        ];
        let selection = select_false_positive_rates(
            source_key_counts,
            [
                self.config.transaction_false_positive_rate_ppm,
                self.config.inbound_message_false_positive_rate_ppm,
                self.config.block_false_positive_rate_ppm,
            ],
            self.config.max_false_positive_rate_ppm,
            self.config.max_filter_bundle_bytes,
        )
        .map_err(|error| irreparable_filter_error(FilterIrreparableReason::Oversized, error))?;
        tracing::debug!(
            partition_id = id.0,
            preferred_false_positive_rates_ppm = ?[
                self.config.transaction_false_positive_rate_ppm,
                self.config.inbound_message_false_positive_rate_ppm,
                self.config.block_false_positive_rate_ppm,
            ],
            selected_false_positive_rates_ppm = ?selection.rates,
            preferred_bundle_bytes = selection.preferred_bundle_bytes,
            selected_bundle_bytes = selection.selected_bundle_bytes,
            reached_maximum = selection.reached_maximum,
            "selected RPC transaction filter false-positive rates"
        );
        let encoded = EncodedFilterBundle {
            transactions: build_filter_file(
                &db,
                identity(FilterNamespace::Transactions),
                source_key_counts[0],
                selection.rates[0],
                cancelled,
            )?,
            inbound_messages: build_filter_file(
                &db,
                identity(FilterNamespace::InboundMessages),
                source_key_counts[1],
                selection.rates[1],
                cancelled,
            )?,
            blocks: build_filter_file(
                &db,
                identity(FilterNamespace::Blocks),
                source_key_counts[2],
                selection.rates[2],
                cancelled,
            )?,
        };
        checked_bundle_bytes(&encoded.metadata(), self.config.max_filter_bundle_bytes)
            .map_err(|error| irreparable_filter_error(FilterIrreparableReason::Oversized, error))?;
        publication_stage(FilterPublicationStage::Vp2, cancelled)?;

        let descriptor = encoded.descriptor();
        let temporary_dir = self.temporary_generation_path(id, generation_id);
        let final_dir = self.final_generation_path(id, generation_id);
        let publication_started_at = Instant::now();
        let result = self.publish(cancelled, || {
            let partition_root = self.partition_root(id);
            let partition_root_exists = partition_root.exists();
            fs::create_dir_all(&partition_root)?;
            if !partition_root_exists {
                sync_directory(&self.filters_root)?;
            }
            fs::create_dir(&temporary_dir).with_context(|| {
                format!("failed to create temporary RPC filter generation at {}", temporary_dir.display())
            })?;
            let mut files = Vec::with_capacity(FilterNamespace::ALL.len());
            for (namespace, file) in encoded.files() {
                check_cancelled(cancelled)?;
                let path = temporary_dir.join(namespace.file_name());
                let mut output = OpenOptions::new().write(true).create_new(true).open(&path)
                    .with_context(|| format!("failed to create RPC filter file at {}", path.display()))?;
                output.write_all(&file.bytes)
                    .with_context(|| format!("failed to write RPC filter file at {}", path.display()))?;
                files.push((path, output));
            }
            publication_stage(FilterPublicationStage::Vp3, cancelled)?;
            for (path, mut output) in files {
                check_cancelled(cancelled)?;
                output.flush()
                    .with_context(|| format!("failed to flush RPC filter file at {}", path.display()))?;
                output.sync_all()
                    .with_context(|| format!("failed to sync RPC filter file at {}", path.display()))?;
            }
            publication_stage(FilterPublicationStage::Vp4, cancelled)?;
            sync_directory(&temporary_dir)?;
            publication_stage(FilterPublicationStage::Vp5, cancelled)?;

            let bundle = Self::read_and_validate_bundle(
                &temporary_dir,
                descriptor,
                manifest_digest,
                self.config.max_filter_bundle_bytes,
            )
            .map_err(generated_filter_validation_error)?;
            publication_stage(FilterPublicationStage::Vp6, cancelled)?;

            Self::ensure_current_sealed_manifest(partitions, id, manifest_digest)
                .map_err(|error| irreparable_filter_error(FilterIrreparableReason::ManifestInvariant, error))?;
            publication_stage(FilterPublicationStage::Vp7, cancelled)?;
            check_cancelled(cancelled)?;
            fs::rename(&temporary_dir, &final_dir).with_context(|| {
                format!(
                    "failed to publish RPC filter generation from {} to {}",
                    temporary_dir.display(),
                    final_dir.display(),
                )
            })?;
            publication_stage(FilterPublicationStage::Vp8, cancelled)?;
            sync_directory(self.partition_root(id))?;
            publication_stage(FilterPublicationStage::Vp9, cancelled)?;

            Self::ensure_current_sealed_manifest(partitions, id, manifest_digest)
                .map_err(|error| irreparable_filter_error(FilterIrreparableReason::ManifestInvariant, error))?;
            check_cancelled(cancelled)?;
            let mut batch = rocksdb::WriteBatch::default();
            batch.put_cf(
                &self.catalog.descriptors.cf(),
                id.0.to_be_bytes(),
                codec::encode_filter_catalog_descriptor(&descriptor),
            );
            self.catalog
                .rocksdb()
                .write_opt(batch, self.catalog.descriptors.write_config())
                .context("failed to publish RPC filter catalog descriptor")?;
            publication_stage(FilterPublicationStage::Vp10, cancelled)?;

            Self::ensure_current_sealed_manifest(partitions, id, manifest_digest)
                .map_err(|error| irreparable_filter_error(FilterIrreparableReason::ManifestInvariant, error))?;
            Ok(Arc::new(bundle))
        });
        if result.is_err() && !cancelled.check() && temporary_dir.exists() {
            self.publish(cancelled, || self.remove_owned_generation(id, generation_id, true))?;
        }
        record_filter_operation("publication", publication_started_at, &result);
        result
    }

    #[cfg(test)]
    pub(super) fn load_and_validate(
        &self,
        id: PartitionId,
        manifest_digest: HashBytes,
    ) -> Result<Option<Arc<ValidatedFilterBundle>>> {
        let Some(value) = self.catalog.descriptors.get(id.0.to_be_bytes())? else {
            return Ok(None);
        };
        let descriptor = codec::decode_filter_catalog_descriptor(value.as_ref())?;
        ensure!(descriptor.partition_id == id.0, "filter catalog key and descriptor partition id mismatch");
        ensure!(descriptor.manifest_digest == manifest_digest, "filter catalog manifest digest mismatch");
        let bundle = Self::read_and_validate_bundle(
            &self.final_generation_path(id, descriptor.generation_id),
            descriptor,
            manifest_digest,
            u64::MAX,
        )?;
        Ok(Some(Arc::new(bundle)))
    }

    #[cfg(test)]
    pub(super) fn remove_descriptor_for_test(&self, id: PartitionId) -> Result<()> {
        let mut batch = rocksdb::WriteBatch::default();
        batch.delete_cf(&self.catalog.descriptors.cf(), id.0.to_be_bytes());
        self.catalog
            .rocksdb()
            .write_opt(batch, self.catalog.descriptors.write_config())
            .context("failed to remove RPC filter catalog descriptor")
    }

    #[cfg(test)]
    pub(super) fn load_and_validate_sealed(
        &self,
        partitions: &parking_lot::Mutex<PartitionManager>,
        id: PartitionId,
        cancelled: &CancellationFlag,
    ) -> Result<Option<Arc<ValidatedFilterBundle>>> {
        check_cancelled(cancelled)?;
        let manifest_digest = {
            let partitions = partitions.lock();
            partitions.sealed_manifest_digest(id)?
        };
        let bundle = self.load_and_validate(id, manifest_digest)?;
        Self::ensure_current_sealed_manifest(partitions, id, manifest_digest)?;
        Ok(bundle)
    }

    fn load_and_validate_descriptor_sealed(
        &self,
        partitions: &parking_lot::Mutex<PartitionManager>,
        id: PartitionId,
        descriptor: FilterCatalogDescriptor,
        cancelled: &CancellationFlag,
    ) -> Result<Arc<ValidatedFilterBundle>> {
        check_cancelled(cancelled)?;
        let manifest_digest = {
            let partitions = partitions.lock();
            partitions.sealed_manifest_digest(id)?
        };
        ensure!(descriptor.partition_id == id.0, "filter catalog descriptor partition id mismatch");
        ensure!(descriptor.manifest_digest == manifest_digest, "filter catalog manifest digest mismatch");
        let bundle = Self::read_and_validate_bundle(
            &self.final_generation_path(id, descriptor.generation_id),
            descriptor,
            manifest_digest,
            u64::MAX,
        )?;
        Self::ensure_current_sealed_manifest(partitions, id, manifest_digest)?;
        Ok(Arc::new(bundle))
    }

    /// Deletes only generation names produced by the V2 formatter under this known partition.
    pub(super) fn cleanup_owned_generations(
        &self,
        id: PartitionId,
        catalog_generation: Option<u128>,
    ) -> Result<()> {
        let partition_root = self.partition_root(id);
        let entries = match fs::read_dir(&partition_root) {
            Ok(entries) => entries,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error).with_context(|| {
                format!("failed to enumerate RPC filter generations at {}", partition_root.display())
            }),
        };
        for entry in entries {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                record_filter_event("orphan_unrecognized");
                tracing::warn!(path = %entry.path().display(), "leaving unrecognized RPC filter entry untouched");
                continue;
            }
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                record_filter_event("orphan_unrecognized");
                tracing::warn!(path = %entry.path().display(), "leaving non-UTF8 RPC filter entry untouched");
                continue;
            };
            let parsed = codec::parse_filter_generation_directory(&format!("{}/{name}", id.directory_name()));
            let Ok((parsed_id, generation_id, temporary)) = parsed else {
                record_filter_event("orphan_unrecognized");
                tracing::warn!(path = %entry.path().display(), "leaving unrecognized RPC filter entry untouched");
                continue;
            };
            if parsed_id != id.0 || (!temporary && Some(generation_id) == catalog_generation) {
                continue;
            }
            self.remove_owned_generation(id, generation_id, temporary)?;
            record_filter_event("orphan_owned_removed");
        }
        Ok(())
    }

    fn read_and_validate_bundle(
        directory: &Path,
        descriptor: FilterCatalogDescriptor,
        manifest_digest: HashBytes,
        max_bundle_bytes: u64,
    ) -> Result<ValidatedFilterBundle> {
        let started_at = Instant::now();
        let result = (|| {
            ensure!(descriptor.manifest_digest == manifest_digest, "filter bundle manifest digest mismatch");
            let structural_bundle_bytes = checked_descriptor_bundle_bytes(
                &[
                    descriptor.transactions,
                    descriptor.inbound_messages,
                    descriptor.blocks,
                ],
                max_bundle_bytes,
            )?;
            let identity = |namespace| FilterFileIdentity {
                namespace,
                partition_id: descriptor.partition_id,
                generation_id: descriptor.generation_id,
                manifest_digest,
            };
            let transactions = Self::read_namespace_filter(
                directory,
                identity(FilterNamespace::Transactions),
                descriptor.transactions,
                structural_bundle_bytes,
            )?;
            let inbound_messages = Self::read_namespace_filter(
                directory,
                identity(FilterNamespace::InboundMessages),
                descriptor.inbound_messages,
                structural_bundle_bytes,
            )?;
            let blocks = Self::read_namespace_filter(
                directory,
                identity(FilterNamespace::Blocks),
                descriptor.blocks,
                structural_bundle_bytes,
            )?;
            let bundle = ValidatedFilterBundle::new(
                transactions,
                inbound_messages,
                blocks,
                structural_bundle_bytes,
            )?;
            Ok(bundle)
        })();
        record_filter_operation("validation", started_at, &result);
        if let Err(error) = &result {
            record_filter_event(classify_filter_failure(error).as_str());
        }
        result
    }

    fn read_namespace_filter(
        directory: &Path,
        identity: FilterFileIdentity,
        descriptor: FilterNamespaceDescriptor,
        max_file_bytes: u64,
    ) -> Result<ImmutableNamespaceFilter> {
        let path = directory.join(identity.namespace.file_name());
        let expected_len = u64::try_from(FILTER_FILE_HEADER_LEN)
            .context("filter header length overflow")?
            .checked_add(descriptor.payload_len)
            .context("filter file length overflow")?;
        ensure!(expected_len <= max_file_bytes, "filter file exceeds configured structural size limit");
        let file_len = fs::metadata(&path)
            .with_context(|| format!("failed to stat RPC filter file at {}", path.display()))?
            .len();
        ensure!(file_len == expected_len, "filter file length differs from catalog descriptor");
        let capacity = usize::try_from(file_len).context("filter file length does not fit usize")?;
        let mut bytes = Vec::with_capacity(capacity);
        let mut input = File::open(&path)
            .with_context(|| format!("failed to open RPC filter file at {}", path.display()))?
            .take(file_len.checked_add(1).context("filter file length overflow")?);
        input.read_to_end(&mut bytes)
            .with_context(|| format!("failed to read RPC filter file at {}", path.display()))?;
        ensure!(u64::try_from(bytes.len()).context("filter file length overflow")? == file_len, "filter file changed while it was being read");
        let decoded = decode_filter_file(&bytes, identity, max_file_bytes)?;
        ensure!(decoded.metadata.matches_descriptor(descriptor), "filter file metadata differs from catalog descriptor");
        Ok(decoded.into_filter())
    }

    fn ensure_current_sealed_manifest(
        partitions: &parking_lot::Mutex<PartitionManager>,
        id: PartitionId,
        manifest_digest: HashBytes,
    ) -> Result<()> {
        ensure!(partitions.lock().sealed_manifest_digest(id)? == manifest_digest, "sealed partition manifest changed during filter publication");
        Ok(())
    }

    fn next_generation_id(&self, id: PartitionId) -> Result<u128> {
        for _ in 0..16 {
            let generation_id = rand::random();
            if !self.temporary_generation_path(id, generation_id).exists()
                && !self.final_generation_path(id, generation_id).exists()
            {
                return Ok(generation_id);
            }
        }
        bail!("failed to allocate an unused RPC filter generation id")
    }

    fn partition_root(&self, id: PartitionId) -> PathBuf {
        self.filters_root.join(id.directory_name())
    }

    fn temporary_generation_path(&self, id: PartitionId, generation_id: u128) -> PathBuf {
        self.filters_root.join(codec::format_filter_temporary_generation_directory(id.0, generation_id))
    }

    fn final_generation_path(&self, id: PartitionId, generation_id: u128) -> PathBuf {
        self.filters_root.join(codec::format_filter_generation_directory(id.0, generation_id))
    }

    fn remove_owned_generation(&self, id: PartitionId, generation_id: u128, temporary: bool) -> Result<()> {
        let path = if temporary {
            self.temporary_generation_path(id, generation_id)
        } else {
            self.final_generation_path(id, generation_id)
        };
        let expected = if temporary {
            codec::format_filter_temporary_generation_directory(id.0, generation_id)
        } else {
            codec::format_filter_generation_directory(id.0, generation_id)
        };
        ensure!(codec::parse_filter_generation_directory(&expected)? == (id.0, generation_id, temporary), "invalid owned RPC filter generation path");
        if path.exists() {
            fs::remove_dir_all(&path)
                .with_context(|| format!("failed to remove owned RPC filter generation at {}", path.display()))?;
            sync_directory(self.partition_root(id))?;
        }
        Ok(())
    }
}

async fn run_filter_worker(
    context: StorageContext,
    config: RpcTransactionFiltersConfig,
    partitions: Arc<Mutex<PartitionManager>>,
    registry: Arc<FilterRegistry>,
    publisher: FilterPublisher,
    pending_sealed: Arc<Mutex<BTreeSet<PartitionId>>>,
    notify: Arc<Notify>,
    cancelled: CancellationFlag,
    publication_gate: Arc<FilterPublicationGate>,
    selected_rate_metrics: Arc<FilterSelectedRateMetrics>,
) {
    let warmup_started_at = Instant::now();
    if cancelled.check() {
        return;
    }
    let sealed = partitions
        .lock()
        .descriptors()
        .into_iter()
        .filter(|descriptor| descriptor.lifecycle == ManifestLifecycle::Sealed)
        .map(|descriptor| descriptor.id)
        .collect::<Vec<_>>();
    let mut queue = FilterWorkQueue::new(sealed.iter().copied());
    let catalog_cancelled = cancelled.clone();
    let catalog_publication_gate = publication_gate.clone();
    let catalog_config = config.clone();
    let catalog = tokio::task::spawn_blocking(move || {
        let store = Arc::new(catalog_publication_gate.publish(&catalog_cancelled, || {
            FilterCatalogStore::open(&context, catalog_config)
        })?);
        store.set_publication_gate(catalog_publication_gate);
        let descriptors = store.catalog_descriptors(&catalog_cancelled)?;
        Ok::<_, anyhow::Error>((store, descriptors))
    })
    .await;
    if cancelled.check() {
        return;
    }
    let (store, mut descriptors) = match catalog {
        Ok(Ok(catalog)) => catalog,
        Ok(Err(e)) => {
            metrics::histogram!(
                "tycho_storage_rpc_filter_warmup_duration_seconds",
                "result" => "failure",
            )
            .record(warmup_started_at.elapsed());
            disable_filter_worker(&e);
            return;
        }
        Err(e) => {
            metrics::histogram!(
                "tycho_storage_rpc_filter_warmup_duration_seconds",
                "result" => "failure",
            )
            .record(warmup_started_at.elapsed());
            disable_filter_worker(&anyhow::Error::new(e).context("RPC filter catalog maintenance task failed"));
            return;
        }
    };
    metrics::gauge!("tycho_storage_rpc_filter_worker_enabled").set(1.0);
    record_configured_namespace_metrics(&config);
    let mut statuses = sealed
        .into_iter()
        .map(|id| {
            let status = if descriptors.contains_key(&id) {
                FilterCatalogRecordStatus::PendingValidation
            } else {
                FilterCatalogRecordStatus::Unfiltered
            };
            (id, status)
        })
        .collect::<BTreeMap<_, _>>();
    refresh_catalog_metrics(&statuses);
    let mut irreparable = BTreeSet::new();
    let mut warmup_recorded = false;

    loop {
        if cancelled.check() {
            break;
        }
        let just_sealed = std::mem::take(&mut *pending_sealed.lock());
        prioritize_filter_work(&mut queue, &mut statuses, &irreparable, just_sealed);
        refresh_catalog_metrics(&statuses);
        let now = Instant::now();
        queue.refresh_metrics(now);
        let Some(id) = queue.next_ready(now) else {
            if !warmup_recorded && queue.enqueued_at.is_empty() {
                metrics::histogram!(
                    "tycho_storage_rpc_filter_warmup_duration_seconds",
                    "result" => "success",
                )
                .record(warmup_started_at.elapsed());
                warmup_recorded = true;
            }
            match queue.next_deadline() {
                Some(deadline) => {
                    tokio::select! {
                        _ = notify.notified() => {}
                        _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {}
                    }
                }
                None => notify.notified().await,
            }
            continue;
        };

        let descriptor = descriptors.get(&id).copied();
        let registry_matches_descriptor = registry.get(id).zip(descriptor).is_some_and(
            |(bundle, descriptor)| {
                bundle.generation_id() == descriptor.generation_id
                    && bundle.manifest_digest() == descriptor.manifest_digest
            },
        );
        if !registry_matches_descriptor {
            if publication_gate
                .publish(&cancelled, || {
                    registry.remove(id);
                    Ok(())
                })
                .is_err()
            {
                break;
            }
        }
        let worker_store = store.clone();
        let worker_partitions = partitions.clone();
        let worker_cancelled = cancelled.clone();
        let result = tokio::task::spawn_blocking(move || {
            maintain_filter_partition(
                &worker_store,
                &worker_partitions,
                id,
                descriptor,
                &worker_cancelled,
            )
        })
        .await;
        let bundle = match result {
            Ok(Ok(bundle)) => bundle,
            Ok(Err(e)) => {
                if cancelled.check() {
                    break;
                }
                if let Some(reason) = irreparable_filter_reason(&e) {
                    complete_irreparable_filter_work(&mut queue, &mut statuses, &mut irreparable, id, reason);
                    tracing::error!(
                        partition_id = id.0,
                        reason = reason.as_str(),
                        "irreparable RPC transaction filter maintenance failure: {e:#}"
                    );
                    continue;
                }
                statuses.insert(id, FilterCatalogRecordStatus::Failed);
                refresh_catalog_metrics(&statuses);
                record_filter_event(classify_filter_failure(&e).as_str());
                let retry_delay = queue.retry(id, Instant::now());
                tracing::error!(
                    partition_id = id.0,
                    retry_delay_secs = retry_delay.as_secs(),
                    "failed to maintain RPC transaction filters: {e:#}"
                );
                continue;
            }
            Err(e) => {
                if cancelled.check() {
                    break;
                }
                statuses.insert(id, FilterCatalogRecordStatus::Failed);
                refresh_catalog_metrics(&statuses);
                record_filter_event("maintenance_join_failure");
                let retry_delay = queue.retry(id, Instant::now());
                tracing::error!(
                    partition_id = id.0,
                    retry_delay_secs = retry_delay.as_secs(),
                    "RPC transaction filter maintenance task failed: {e}"
                );
                continue;
            }
        };
        bundle.record_namespace_metrics();
        retain_maintained_descriptor(&mut descriptors, id, &bundle);

        if cancelled.check() {
            break;
        }
        let manifest_matches = partitions
            .lock()
            .sealed_manifest_digest(id)
            .is_ok_and(|digest| digest == bundle.manifest_digest());
        if !manifest_matches {
            complete_irreparable_filter_work(
                &mut queue,
                &mut statuses,
                &mut irreparable,
                id,
                FilterIrreparableReason::ManifestInvariant,
            );
            tracing::error!(
                partition_id = id.0,
                "RPC transaction filter bundle no longer matches a sealed partition manifest before registry installation"
            );
            continue;
        }
        let publication_started_at = Instant::now();
        if let Err(e) = publication_gate.publish(&cancelled, || publisher(id, bundle.clone())) {
            let publication_result = Err::<(), _>(e);
            record_filter_operation("snapshot_publication", publication_started_at, &publication_result);
            if publisher_failure_has_manifest_invariant(&partitions, id, &bundle) {
                complete_irreparable_filter_work(
                    &mut queue,
                    &mut statuses,
                    &mut irreparable,
                    id,
                    FilterIrreparableReason::ManifestInvariant,
                );
                tracing::error!(
                    partition_id = id.0,
                    "RPC filter snapshot publication observed a sealed manifest invariant failure: {:#}",
                    publication_result.as_ref().unwrap_err()
                );
                continue;
            }
            statuses.insert(id, FilterCatalogRecordStatus::Failed);
            refresh_catalog_metrics(&statuses);
            let retry_delay = queue.retry(id, Instant::now());
            tracing::error!(
                partition_id = id.0,
                retry_delay_secs = retry_delay.as_secs(),
                "failed to publish RPC transaction filters into the request snapshot: {:#}",
                publication_result.as_ref().unwrap_err()
            );
            continue;
        }
        record_filter_operation("snapshot_publication", publication_started_at, &Ok::<_, anyhow::Error>(()));
        bundle.record_selected_rate_metrics(&selected_rate_metrics);
        statuses.insert(id, FilterCatalogRecordStatus::Validated);
        refresh_catalog_metrics(&statuses);
        if cancelled.check() {
            break;
        }
        let cleanup_store = store.clone();
        let cleanup_cancelled = cancelled.clone();
        let cleanup = tokio::task::spawn_blocking(move || {
            cleanup_store.publish(&cleanup_cancelled, || {
                cleanup_store.cleanup_owned_generations(id, Some(bundle.generation_id()))
            })
        })
        .await;
        if cancelled.check() {
            break;
        }
        match cleanup {
            Ok(Ok(())) => {
                queue.complete(id);
                queue.refresh_metrics(Instant::now());
            }
            Ok(Err(e)) => {
                let retry_delay = queue.retry(id, Instant::now());
                tracing::error!(
                    partition_id = id.0,
                    retry_delay_secs = retry_delay.as_secs(),
                    "failed to clean obsolete RPC filter generations: {e:#}"
                );
            }
            Err(e) => {
                let retry_delay = queue.retry(id, Instant::now());
                tracing::error!(
                    partition_id = id.0,
                    retry_delay_secs = retry_delay.as_secs(),
                    "RPC filter cleanup task failed: {e}"
                );
            }
        }
    }
    if !warmup_recorded {
        metrics::histogram!(
            "tycho_storage_rpc_filter_warmup_duration_seconds",
            "result" => "cancelled",
        )
        .record(warmup_started_at.elapsed());
    }
    metrics::gauge!("tycho_storage_rpc_filter_worker_enabled").set(0.0);
}

fn retain_maintained_descriptor(
    descriptors: &mut BTreeMap<PartitionId, FilterCatalogDescriptor>,
    id: PartitionId,
    bundle: &ValidatedFilterBundle,
) {
    descriptors.insert(id, bundle.descriptor());
}

fn maintain_filter_partition(
    store: &FilterCatalogStore,
    partitions: &Mutex<PartitionManager>,
    id: PartitionId,
    descriptor: Option<FilterCatalogDescriptor>,
    cancelled: &CancellationFlag,
) -> Result<Arc<ValidatedFilterBundle>> {
    check_cancelled(cancelled)?;
    store.publish(cancelled, || {
        store.cleanup_owned_generations(id, descriptor.map(|descriptor| descriptor.generation_id))
    })?;
    if let Some(descriptor) = descriptor {
        let started_at = Instant::now();
        match store.load_and_validate_descriptor_sealed(partitions, id, descriptor, cancelled) {
            Ok(bundle) => {
                let result = Ok::<_, anyhow::Error>(());
                record_filter_operation("catalog_validation", started_at, &result);
                return Ok(bundle);
            }
            Err(e) => {
                let result = Err::<(), _>(e);
                record_filter_operation("catalog_validation", started_at, &result);
                record_filter_event(classify_filter_failure(result.as_ref().unwrap_err()).as_str());
                tracing::warn!(
                    partition_id = id.0,
                    generation_id = descriptor.generation_id,
                    "rebuilding invalid RPC transaction filter bundle: {:#}",
                    result.as_ref().unwrap_err()
                );
            }
        }
    }
    let stage = if descriptor.is_some() { "rebuild" } else { "build" };
    let started_at = Instant::now();
    let result = store.build_and_publish(partitions, id, cancelled);
    record_filter_operation(stage, started_at, &result);
    result
}

fn disable_filter_worker(error: &anyhow::Error) {
    metrics::counter!(
        "tycho_storage_rpc_filter_catalog_failures_total",
        "stage" => "open_or_iterate",
        "result" => "failure",
    )
        .increment(1);
    metrics::gauge!("tycho_storage_rpc_filter_worker_enabled").set(0.0);
    tracing::error!(
        "RPC filter acceleration is disabled for this process because its catalog could not be opened or iterated: {error:#}; local transaction-partition routing remains available; clear only rpc/filter-catalog before restart to rebuild acceleration metadata"
    );
}

fn build_filter_file(
    db: &super::db::RpcTransactionsDb,
    identity: FilterFileIdentity,
    source_key_count: u64,
    false_positive_rate_ppm: u32,
    cancelled: &CancellationFlag,
) -> Result<EncodedFilterFile> {
    let mut builder = FilterBuilder::new(identity, source_key_count, false_positive_rate_ppm)
        .map_err(|error| irreparable_filter_error(FilterIrreparableReason::Oversized, error))?;
    scan_source_keys(db, identity.namespace, cancelled, |key| {
        let _ = builder.insert(key)?;
        Ok(())
    })?;
    builder
        .finish()
        .map_err(generated_filter_artifact_error)?
        .encode()
        .map_err(|error| irreparable_filter_error(FilterIrreparableReason::GeneratedArtifactInvalid, error))
}

fn count_source_keys(
    db: &super::db::RpcTransactionsDb,
    namespace: FilterNamespace,
    cancelled: &CancellationFlag,
) -> Result<u64> {
    let mut source_key_count = 0u64;
    scan_source_keys(db, namespace, cancelled, |_| {
        source_key_count = source_key_count
            .checked_add(1)
            .context("filter source key count overflow")
            .map_err(invalid_source_filter_error)?;
        Ok(())
    })?;
    Ok(source_key_count)
}

struct FalsePositiveRateSelection {
    rates: [u32; 3],
    preferred_bundle_bytes: u64,
    selected_bundle_bytes: u64,
    reached_maximum: bool,
}

fn select_false_positive_rates(
    source_key_counts: [u64; 3],
    preferred_rates_ppm: [u32; 3],
    max_false_positive_rate_ppm: u32,
    max_bundle_bytes: u64,
) -> Result<FalsePositiveRateSelection> {
    validate_false_positive_rate(max_false_positive_rate_ppm)?;
    ensure!(preferred_rates_ppm.into_iter().all(|rate| rate <= max_false_positive_rate_ppm), "filter preferred false-positive rate exceeds configured maximum");
    let bundle_bytes = |rates: [u32; 3]| {
        source_key_counts.into_iter().zip(rates).try_fold(0u64, |total, (source_key_count, false_positive_rate_ppm)| {
            total.checked_add(estimated_filter_bundle_bytes(source_key_count, false_positive_rate_ppm)?)
                .context("estimated filter bundle size overflow")
        })
    };
    let preferred_bundle_bytes = bundle_bytes(preferred_rates_ppm)?;
    if preferred_bundle_bytes <= max_bundle_bytes {
        return Ok(FalsePositiveRateSelection {
            rates: preferred_rates_ppm,
            preferred_bundle_bytes,
            selected_bundle_bytes: preferred_bundle_bytes,
            reached_maximum: preferred_rates_ppm.into_iter().any(|rate| rate == max_false_positive_rate_ppm),
        });
    }

    let mut order = [0usize, 1, 2];
    order.sort_by(|left, right| {
        estimated_filter_bundle_bytes(source_key_counts[*right], preferred_rates_ppm[*right])
            .expect("validated filter rate estimate")
            .cmp(&estimated_filter_bundle_bytes(source_key_counts[*left], preferred_rates_ppm[*left])
                .expect("validated filter rate estimate"))
            .then_with(|| left.cmp(right))
    });
    let mut rates = preferred_rates_ppm;
    let mut next = 0usize;
    let mut exhausted = [false; 3];
    loop {
        let index = order[next];
        next = (next + 1) % order.len();
        if !exhausted[index] {
            match next_false_positive_rate_step(rates[index], max_false_positive_rate_ppm) {
                Some(rate) => rates[index] = rate,
                None => exhausted[index] = true,
            }
            let selected_bundle_bytes = bundle_bytes(rates)?;
            if selected_bundle_bytes <= max_bundle_bytes {
                return Ok(FalsePositiveRateSelection {
                    rates,
                    preferred_bundle_bytes,
                    selected_bundle_bytes,
                    reached_maximum: rates.into_iter().any(|rate| rate == max_false_positive_rate_ppm),
                });
            }
        }
        if exhausted.into_iter().all(|value| value) {
            bail!("filter bundle exceeds configured size limit after maximum false-positive rate");
        }
    }
}

fn qfilter_precision_bits(false_positive_rate_ppm: u32) -> Result<u64> {
    validate_false_positive_rate(false_positive_rate_ppm)?;
    Ok((-(false_positive_rate_ppm as f64 / 1_000_000.0).log2())
        .round()
        .max(1.0) as u64)
}

fn next_false_positive_rate_step(current: u32, maximum: u32) -> Option<u32> {
    let current_precision = qfilter_precision_bits(current).ok()?;
    if current >= maximum || qfilter_precision_bits(maximum).ok()? >= current_precision {
        return None;
    }
    let mut lower = current.saturating_add(1);
    let mut upper = maximum;
    while lower < upper {
        let middle = lower + (upper - lower) / 2;
        if qfilter_precision_bits(middle).ok()? < current_precision {
            upper = middle;
        } else {
            lower = middle.saturating_add(1);
        }
    }
    Some(lower)
}

fn estimated_filter_bundle_bytes(source_key_count: u64, false_positive_rate_ppm: u32) -> Result<u64> {
    validate_false_positive_rate(false_positive_rate_ppm)?;
    let desired = source_key_count.max(1);
    let mut slots = desired
        .checked_next_power_of_two()
        .context("filter source key count exceeds qfilter capacity")?
        .max(64);
    while slots
        .checked_mul(19)
        .context("filter slot capacity overflow")?
        .div_ceil(20)
        < desired
    {
        slots = slots.checked_mul(2).context("filter slot capacity overflow")?;
    }
    let rbits = qfilter_precision_bits(false_positive_rate_ppm)?;
    let resident_bytes = slots
        .checked_div(64)
        .and_then(|blocks| blocks.checked_mul(17u64.checked_add(8 * rbits)?))
        .context("estimated qfilter resident size overflow")?;
    let payload_bytes = resident_bytes
        .checked_add(FILTER_SERIALIZED_FIXED_LEN)
        .context("estimated qfilter payload size overflow")?;
    u64::try_from(FILTER_FILE_HEADER_LEN)
        .context("filter header length overflow")?
        .checked_add(payload_bytes)
        .and_then(|value| value.checked_add(resident_bytes))
        .context("estimated filter file size overflow")
}

fn scan_source_keys(
    db: &super::db::RpcTransactionsDb,
    namespace: FilterNamespace,
    cancelled: &CancellationFlag,
    mut f: impl FnMut(&[u8]) -> Result<()>,
) -> Result<()> {
    macro_rules! scan_table {
        ($table:ident) => {{
            let mut iterator = db.rocksdb().raw_iterator_cf(&db.$table.cf());
            iterator.seek_to_first();
            while iterator.valid() {
                check_cancelled(cancelled)?;
                let key = iterator.key().context("filter source iterator returned no key")?;
                validate_key(namespace, key).map_err(invalid_source_filter_error)?;
                f(key)?;
                iterator.next();
            }
            iterator.status()?;
            Ok(())
        }};
    }
    match namespace {
        FilterNamespace::Transactions => scan_table!(transactions_by_hash),
        FilterNamespace::InboundMessages => scan_table!(transactions_by_in_msg),
        FilterNamespace::Blocks => scan_table!(known_blocks),
    }
}

fn sync_directory(path: impl AsRef<Path>) -> Result<()> {
    let path = path.as_ref();
    File::open(path)
        .with_context(|| format!("failed to open RPC filter directory at {}", path.display()))?
        .sync_all()
        .with_context(|| format!("failed to sync RPC filter directory at {}", path.display()))
}

fn check_cancelled(cancelled: &CancellationFlag) -> Result<()> {
    ensure!(!cancelled.check(), "RPC filter maintenance cancelled");
    Ok(())
}

#[cfg(test)]
fn publication_stage(stage: FilterPublicationStage, cancelled: &CancellationFlag) -> Result<()> {
    check_cancelled(cancelled)?;
    let hook = FILTER_PUBLICATION_STAGE_HOOK.with(|hook| *hook.borrow());
    if let Some(hook) = hook {
        hook(stage)?;
    }
    check_cancelled(cancelled)
}

#[cfg(not(test))]
fn publication_stage(_stage: FilterPublicationStage, cancelled: &CancellationFlag) -> Result<()> {
    check_cancelled(cancelled)
}

#[cfg(test)]
mod tests {
    use std::hash::Hasher;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::Arc;

    use super::*;
    use crate::config::RpcTransactionPartitionsConfig;
    use super::super::partition::PartitionCounters;
    use super::super::partition::tests::TestMetricsRecorder;

    thread_local! {
        static FAIL_PUBLICATION_STAGE: std::cell::Cell<Option<FilterPublicationStage>> = const { std::cell::Cell::new(None) };
        static MANIFEST_MUTATION_PARTITIONS: std::cell::RefCell<Option<Arc<parking_lot::Mutex<PartitionManager>>>> = const { std::cell::RefCell::new(None) };
    }

    fn fail_selected_publication_stage(stage: FilterPublicationStage) -> Result<()> {
        if FAIL_PUBLICATION_STAGE.with(|selected| selected.get()) == Some(stage) {
            bail!("injected filter publication failure after {stage:?}");
        }
        Ok(())
    }

    fn mutate_sealed_manifest_at_vp7(stage: FilterPublicationStage) -> Result<()> {
        if stage == FilterPublicationStage::Vp7 {
            MANIFEST_MUTATION_PARTITIONS.with(|partitions| {
                let partitions = partitions.borrow();
                let partitions = partitions.as_ref().context("missing test manifest mutation partitions")?;
                let id = partitions
                    .lock()
                    .descriptors()
                    .into_iter()
                    .find(|descriptor| descriptor.lifecycle == ManifestLifecycle::Sealed)
                    .context("missing test sealed partition")?
                    .id;
                partitions.lock().mutate_sealed_manifest_for_test(id)
            })?;
        }
        Ok(())
    }

    fn seal_partition(manager: &mut PartitionManager) -> PartitionId {
        let id = manager.active_id();
        manager.request_rotation(PartitionCounters {
            estimated_lsm_bytes: u64::MAX,
            ..Default::default()
        });
        manager.rotate_if_requested().unwrap();
        let worker = manager.begin_sealing(id).unwrap();
        let closed = manager.take_sealing_handle(id).unwrap();
        drop(closed);
        drop(worker);
        let read_only = manager.open_sealed_read_only(id).unwrap();
        manager.complete_sealing(id, read_only).unwrap();
        id
    }

    async fn sealed_filter_fixture() -> (
        StorageContext,
        Box<dyn std::any::Any>,
        Arc<parking_lot::Mutex<PartitionManager>>,
        FilterCatalogStore,
        PartitionId,
        [u8; 32],
        [u8; 32],
        [u8; 13],
    ) {
        let (context, temp) = StorageContext::new_temp().await.unwrap();
        let mut manager = PartitionManager::open(
            context.clone(),
            RpcTransactionPartitionsConfig::default(),
        )
        .unwrap();
        let transaction_key = [0x11; 32];
        let inbound_key = [0x22; 32];
        let block_key = [0x33; 13];
        manager.active_db().transactions.insert([0; 41], [0xff]).unwrap();
        manager.active_db().transactions_by_hash.insert(transaction_key, [1]).unwrap();
        manager.active_db().transactions_by_in_msg.insert(inbound_key, [2]).unwrap();
        manager.active_db().known_blocks.insert(block_key, [3]).unwrap();
        let id = seal_partition(&mut manager);
        let store = FilterCatalogStore::open(&context, Default::default()).unwrap();
        (
            context,
            Box::new(temp),
            Arc::new(parking_lot::Mutex::new(manager)),
            store,
            id,
            transaction_key,
            inbound_key,
            block_key,
        )
    }

    fn identity(namespace: FilterNamespace) -> FilterFileIdentity {
        FilterFileIdentity {
            namespace,
            partition_id: 0x0102_0304_0506_0708,
            generation_id: 0x1112_1314_1516_1718_2122_2324_2526_2728,
            manifest_digest: HashBytes::from_slice(&[0x33; 32]),
        }
    }

    fn make_file(namespace: FilterNamespace, keys: &[&[u8]]) -> EncodedFilterFile {
        let mut builder = FilterBuilder::new(identity(namespace), keys.len() as u64, 1_000).unwrap();
        for key in keys {
            builder.insert(key).unwrap();
        }
        builder.finish().unwrap().encode().unwrap()
    }

    #[test]
    fn raw_filter_key_writes_exactly_one_fixed_byte_vector() {
        #[derive(Default)]
        struct CountingHasher {
            writes: usize,
            bytes: Vec<u8>,
        }

        impl Hasher for CountingHasher {
            fn finish(&self) -> u64 {
                0
            }

            fn write(&mut self, bytes: &[u8]) {
                self.writes += 1;
                self.bytes.extend_from_slice(bytes);
            }
        }

        let key = [0x10, 0x20, 0x30, 0x40];
        let mut hasher = CountingHasher::default();
        RawFilterKey(&key).hash(&mut hasher);
        assert_eq!(hasher.writes, 1);
        assert_eq!(hasher.bytes, [0x10, 0x20, 0x30, 0x40]);
    }

    #[test]
    fn header_uses_the_fixed_little_endian_serialization_vector() {
        let metadata = FilterFileMetadata {
            identity: identity(FilterNamespace::Blocks),
            false_positive_rate_ppm: 1_000,
            source_key_count: 3,
            fingerprint_count: 2,
            capacity: 60,
            fingerprint_size: 17,
            resident_bytes: 123,
            payload_len: 456,
            payload_digest: HashBytes::from_slice(&[0x44; 32]),
        };
        let mut bytes = Vec::new();
        encode_header(&metadata, &mut bytes);
        let expected = [
            b'T', b'Y', b'C', b'H', b'Q', b'F', b'0', b'2', 1, 3, 8, 7, 6, 5, 4, 3, 2,
            1, 40, 39, 38, 37, 36, 35, 34, 33, 24, 23, 22, 21, 20, 19, 18, 17,
        ];
        assert_eq!(&bytes[..expected.len()], expected.as_slice());
        assert_eq!(&bytes[34..66], [0x33; 32]);
        assert_eq!(&bytes[66..70], [1, 1, 1, 13]);
        assert_eq!(&bytes[70..74], 1_000u32.to_le_bytes());
        assert_eq!(&bytes[74..82], 3u64.to_le_bytes());
        assert_eq!(&bytes[82..90], 2u64.to_le_bytes());
        assert_eq!(&bytes[90..98], 60u64.to_le_bytes());
        assert_eq!(bytes[98], 17);
        assert_eq!(&bytes[99..107], 123u64.to_le_bytes());
        assert_eq!(&bytes[107..115], 456u64.to_le_bytes());
        assert_eq!(&bytes[115..], [0x44; 32]);
    }

    #[test]
    fn empty_qfilter_payload_uses_the_fixed_serialization_digest_and_length_vector() {
        let encoded = make_file(FilterNamespace::Transactions, &[]);
        let mut expected_payload = Vec::with_capacity(115);
        expected_payload.extend_from_slice(&97u64.to_le_bytes());
        expected_payload.resize(8 + 97, 0);
        expected_payload.extend_from_slice(&0u64.to_le_bytes());
        expected_payload.extend_from_slice(&[6, 10]);
        expected_payload.push(0);
        assert_eq!(encoded.metadata.payload_len, 116);
        assert_eq!(&encoded.bytes[FILTER_FILE_HEADER_LEN..], expected_payload.as_slice());
        assert_eq!(
            encoded.metadata.payload_digest,
            HashBytes::from_slice(blake3::hash(&expected_payload).as_bytes()),
        );
    }

    #[test]
    fn inserted_empty_and_serialized_filters_never_report_false_negative() {
        let transaction_key = [0x11; 32];
        let inbound_message_key = [0x12; 32];
        let block_key = [0x22; 13];
        for (namespace, key) in [
            (FilterNamespace::Transactions, transaction_key.as_slice()),
            (FilterNamespace::InboundMessages, inbound_message_key.as_slice()),
            (FilterNamespace::Blocks, block_key.as_slice()),
        ] {
            let encoded = make_file(namespace, &[key]);
            let decoded = decode_filter_file(&encoded.bytes, identity(namespace), u64::MAX).unwrap();
            assert!(decoded.into_filter().contains(key).unwrap());
        }

        let empty = make_file(FilterNamespace::InboundMessages, &[]);
        let arbitrary_key = [0x99; 32];
        let decoded = decode_filter_file(
            &empty.bytes,
            identity(FilterNamespace::InboundMessages),
            u64::MAX,
        )
        .unwrap();
        assert!(!decoded.into_filter().contains(&arbitrary_key).unwrap());
    }

    #[test]
    fn colliding_fingerprint_entries_and_reloaded_metadata_remain_consistent() {
        let first_fingerprint = 0x0000_0000_0000_0042u64;
        let colliding_fingerprint = 0xffff_ffff_ffff_ff42u64;
        let mut collisions = qfilter::Filter::with_fingerprint_size(2, 8).unwrap();
        assert!(collisions.insert_fingerprint(false, first_fingerprint).unwrap());
        assert!(!collisions.insert_fingerprint(false, colliding_fingerprint).unwrap());
        assert!(collisions.contains_fingerprint(first_fingerprint));
        assert!(collisions.contains_fingerprint(colliding_fingerprint));

        let key = [0x51; 32];
        let mut builder = FilterBuilder::new(identity(FilterNamespace::Transactions), 1, 1_000).unwrap();
        builder.insert(&key).unwrap();
        let filter = builder.finish().unwrap();
        let current_error_ratio = filter.filter.current_error_ratio();
        let encoded = filter.encode().unwrap();
        let decoded = decode_filter_file(
            &encoded.bytes,
            identity(FilterNamespace::Transactions),
            u64::MAX,
        )
        .unwrap();
        assert_eq!(decoded.metadata.source_key_count, 1);
        assert_eq!(decoded.metadata.fingerprint_count, decoded.filter.len());
        assert_eq!(decoded.metadata.capacity, decoded.filter.capacity());
        assert_eq!(decoded.metadata.resident_bytes, decoded.filter.memory_usage() as u64);
        assert_eq!(decoded.filter.current_error_ratio(), current_error_ratio);
        assert!(decoded.into_filter().contains(&key).unwrap());
    }

    #[test]
    fn file_decoder_rejects_truncation_and_all_declared_integrity_failures() {
        let key = [0x11; 32];
        let encoded = make_file(FilterNamespace::Transactions, &[&key]);
        for boundary in [0, 8, 9, 10, 18, 34, 66, 70, 74, 82, 90, 98, 99, 107, 115, FILTER_FILE_HEADER_LEN] {
            assert!(decode_filter_file(&encoded.bytes[..boundary], identity(FilterNamespace::Transactions), u64::MAX).is_err());
        }
        let mut appended = encoded.bytes.clone();
        appended.push(0);
        assert!(decode_filter_file(&appended, identity(FilterNamespace::Transactions), u64::MAX).is_err());
        let mut checksum = encoded.bytes.clone();
        *checksum.last_mut().unwrap() ^= 1;
        assert!(decode_filter_file(&checksum, identity(FilterNamespace::Transactions), u64::MAX).is_err());
        let mut version = encoded.bytes.clone();
        version[8] = FILTER_FILE_VERSION + 1;
        assert!(decode_filter_file(&version, identity(FilterNamespace::Transactions), u64::MAX).is_err());
        let mut algorithm = encoded.bytes.clone();
        algorithm[66] = 0;
        assert!(decode_filter_file(&algorithm, identity(FilterNamespace::Transactions), u64::MAX).is_err());
        let mut metadata = encoded.bytes.clone();
        metadata[90] ^= 1;
        assert!(decode_filter_file(&metadata, identity(FilterNamespace::Transactions), u64::MAX).is_err());
        let mut key_len = encoded.bytes.clone();
        key_len[69] = 13;
        assert!(decode_filter_file(&key_len, identity(FilterNamespace::Transactions), u64::MAX).is_err());
        let mut trailing_payload = encoded.bytes.clone();
        trailing_payload.push(0);
        let payload_len = (trailing_payload.len() - FILTER_FILE_HEADER_LEN) as u64;
        trailing_payload[107..115].copy_from_slice(&payload_len.to_le_bytes());
        let digest = blake3::hash(&trailing_payload[FILTER_FILE_HEADER_LEN..]);
        trailing_payload[115..FILTER_FILE_HEADER_LEN].copy_from_slice(digest.as_bytes());
        assert!(decode_filter_file(&trailing_payload, identity(FilterNamespace::Transactions), u64::MAX).is_err());
        assert!(decode_filter_file(&encoded.bytes, identity(FilterNamespace::Transactions), 1).is_err());
    }

    #[test]
    fn bundle_limit_counts_headers_payloads_and_resident_filters() {
        let key = [0x11; 32];
        let encoded = make_file(FilterNamespace::Transactions, &[&key]);
        let mut metadata = encoded.metadata;
        metadata.identity.namespace = FilterNamespace::InboundMessages;
        let mut blocks = encoded.metadata;
        blocks.identity.namespace = FilterNamespace::Blocks;
        let total = checked_bundle_bytes(&[encoded.metadata, metadata, blocks], u64::MAX).unwrap();
        assert!(checked_bundle_bytes(&[encoded.metadata, metadata, blocks], total - 1).is_err());
    }

    #[test]
    fn validated_bundle_requires_one_complete_consistent_namespace_set() {
        let transaction_key = [0x11; 32];
        let inbound_message_key = [0x12; 32];
        let block_key = [0x13; 13];
        let transactions = make_file(FilterNamespace::Transactions, &[&transaction_key]);
        let inbound_messages = make_file(FilterNamespace::InboundMessages, &[&inbound_message_key]);
        let blocks = make_file(FilterNamespace::Blocks, &[&block_key]);
        let transactions = decode_filter_file(&transactions.bytes, identity(FilterNamespace::Transactions), u64::MAX).unwrap().into_filter();
        let inbound_messages = decode_filter_file(&inbound_messages.bytes, identity(FilterNamespace::InboundMessages), u64::MAX).unwrap().into_filter();
        let blocks = decode_filter_file(&blocks.bytes, identity(FilterNamespace::Blocks), u64::MAX).unwrap().into_filter();
        let bundle = ValidatedFilterBundle::new(transactions, inbound_messages, blocks, u64::MAX).unwrap();
        assert!(bundle.filter(FilterNamespace::Transactions).contains(&transaction_key).unwrap());
        assert!(bundle.filter(FilterNamespace::InboundMessages).contains(&inbound_message_key).unwrap());
        assert!(bundle.filter(FilterNamespace::Blocks).contains(&block_key).unwrap());
    }

    #[test]
    fn complete_bundle_accepts_an_empty_inbound_namespace() {
        let transaction_key = [0x11; 32];
        let block_key = [0x13; 13];
        let transactions = make_file(FilterNamespace::Transactions, &[&transaction_key]);
        let inbound_messages = make_file(FilterNamespace::InboundMessages, &[]);
        let blocks = make_file(FilterNamespace::Blocks, &[&block_key]);
        let transactions = decode_filter_file(&transactions.bytes, identity(FilterNamespace::Transactions), u64::MAX).unwrap().into_filter();
        let inbound_messages = decode_filter_file(&inbound_messages.bytes, identity(FilterNamespace::InboundMessages), u64::MAX).unwrap().into_filter();
        let blocks = decode_filter_file(&blocks.bytes, identity(FilterNamespace::Blocks), u64::MAX).unwrap().into_filter();
        let bundle = ValidatedFilterBundle::new(transactions, inbound_messages, blocks, u64::MAX).unwrap();
        assert_eq!(bundle.filter(FilterNamespace::InboundMessages).metadata().source_key_count, 0);
        assert!(!bundle.filter(FilterNamespace::InboundMessages).contains(&[0x22; 32]).unwrap());
    }

    #[test]
    fn observability_records_bounded_filter_worker_and_bundle_dimensions() {
        let transaction_key = [0x11; 32];
        let inbound_message_key = [0x12; 32];
        let block_key = [0x13; 13];
        let transactions = make_file(FilterNamespace::Transactions, &[&transaction_key]);
        let inbound_messages = make_file(FilterNamespace::InboundMessages, &[&inbound_message_key]);
        let blocks = make_file(FilterNamespace::Blocks, &[&block_key]);
        let transactions = decode_filter_file(&transactions.bytes, identity(FilterNamespace::Transactions), u64::MAX).unwrap().into_filter();
        let inbound_messages = decode_filter_file(&inbound_messages.bytes, identity(FilterNamespace::InboundMessages), u64::MAX).unwrap().into_filter();
        let blocks = decode_filter_file(&blocks.bytes, identity(FilterNamespace::Blocks), u64::MAX).unwrap().into_filter();
        let bundle = ValidatedFilterBundle::new(transactions, inbound_messages, blocks, u64::MAX).unwrap();
        let recorder = TestMetricsRecorder::default();
        let selected_rates = FilterSelectedRateMetrics::default();

        metrics::with_local_recorder(&recorder, || {
            bundle.record_namespace_metrics();
            bundle.record_selected_rate_metrics(&selected_rates);
            let mut queue = FilterWorkQueue::new([PartitionId(1), PartitionId(2)]);
            queue.refresh_metrics(Instant::now());
            queue.retry(PartitionId(2), Instant::now());
            let statuses = BTreeMap::from([
                (PartitionId(1), FilterCatalogRecordStatus::Validated),
                (PartitionId(2), FilterCatalogRecordStatus::Failed),
            ]);
            refresh_catalog_metrics(&statuses);
            record_filter_event("checksum_mismatch");
            record_filter_operation("validation", Instant::now(), &Ok::<_, anyhow::Error>(()));
            record_filter_operation("build", Instant::now(), &Ok::<_, anyhow::Error>(()));
        });

        assert_eq!(recorder.gauge("tycho_storage_rpc_filter_queue_depth"), 2.0);
        assert_eq!(
            recorder.gauge("tycho_storage_rpc_filter_catalog_records|result=validated"),
            1.0
        );
        assert_eq!(
            recorder.counter("tycho_storage_rpc_filter_events_total|reason=checksum_mismatch"),
            1
        );
        assert_eq!(
            recorder.counter(
                "tycho_storage_rpc_filter_operations_total|stage=retry|result=scheduled"
            ),
            1
        );
        assert_eq!(
            recorder.counter(
                "tycho_storage_rpc_filter_operations_total|stage=build|result=success"
            ),
            1
        );
        assert_eq!(
            recorder.gauge("tycho_storage_rpc_filter_selected_error_ratio|namespace=transactions"),
            0.001
        );
        assert_eq!(
            recorder.histogram_len(
                "tycho_storage_rpc_filter_source_keys|namespace=transactions"
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

    #[test]
    fn worker_queue_prioritizes_newly_sealed_and_caps_retry_backoff() {
        let now = Instant::now();
        let mut queue = FilterWorkQueue::new([PartitionId(1), PartitionId(2), PartitionId(3)]);
        assert_eq!(queue.next_ready(now), Some(PartitionId(3)));
        assert_eq!(queue.retry(PartitionId(3), now), Duration::from_secs(1));
        assert_eq!(queue.next_ready(now + Duration::from_secs(1)), Some(PartitionId(3)));

        let mut queue = FilterWorkQueue::new([PartitionId(1), PartitionId(2), PartitionId(3)]);
        assert_eq!(queue.next_ready(now), Some(PartitionId(3)));
        assert_eq!(queue.retry(PartitionId(3), now), Duration::from_secs(1));
        queue.prioritize([PartitionId(1), PartitionId(3), PartitionId(3)]);
        assert_eq!(queue.next_ready(now), Some(PartitionId(3)));
        assert_eq!(queue.next_ready(now), Some(PartitionId(1)));
        assert_eq!(queue.next_ready(now), Some(PartitionId(2)));
        assert_eq!(queue.next_ready(now), None);

        let mut delay = Duration::ZERO;
        for _ in 0..7 {
            delay = queue.retry(PartitionId(3), now);
        }
        assert_eq!(delay, Duration::from_secs(30));
        assert!(queue.retry_delays[&PartitionId(3)] <= Duration::from_secs(30));
    }

    #[test]
    fn irreparable_work_completes_without_retry_or_warmup_backlog() {
        let now = Instant::now();
        let id = PartitionId(1);
        let mut queue = FilterWorkQueue::new([id]);
        assert_eq!(queue.next_ready(now), Some(id));
        let mut statuses = BTreeMap::new();
        let mut irreparable = BTreeSet::new();
        complete_irreparable_filter_work(
            &mut queue,
            &mut statuses,
            &mut irreparable,
            id,
            FilterIrreparableReason::Oversized,
        );
        prioritize_filter_work(&mut queue, &mut statuses, &irreparable, [id]);
        assert_eq!(statuses[&id].as_str(), "oversized");
        assert!(queue.enqueued_at.is_empty());
        assert!(queue.retry_deadlines.is_empty());
        assert!(queue.next_deadline().is_none());
        assert_eq!(queue.next_ready(now), None);
    }

    #[test]
    fn transient_source_scan_failure_has_no_irreparable_disposition() {
        let error = anyhow::Error::new(std::io::Error::from(ErrorKind::Interrupted))
            .context("filter source iterator failed");
        assert!(irreparable_filter_reason(&error).is_none());
    }

    #[test]
    fn generated_validation_io_failure_remains_retryable() {
        let error = generated_filter_validation_error(
            anyhow::Error::new(std::io::Error::from(ErrorKind::Interrupted))
                .context("failed to read temporary filter generation"),
        );
        assert!(irreparable_filter_reason(&error).is_none());
    }

    #[tokio::test]
    async fn publisher_failure_with_a_changed_manifest_is_irreparable() {
        let (_context, _temp, partitions, store, id, ..) = sealed_filter_fixture().await;
        let bundle = store
            .build_and_publish(&partitions, id, &CancellationFlag::new())
            .unwrap();
        assert!(!publisher_failure_has_manifest_invariant(&partitions, id, &bundle));
        partitions.lock().mutate_sealed_manifest_for_test(id).unwrap();
        assert!(publisher_failure_has_manifest_invariant(&partitions, id, &bundle));
    }

    #[test]
    fn selected_rate_metric_keeps_the_process_local_maximum_after_lower_installation() {
        let key = [0x11; 32];
        let make_bundle = |rate| {
            let make_filter = |namespace, key: &[u8]| {
                let mut builder = FilterBuilder::new(identity(namespace), 1, rate).unwrap();
                builder.insert(key).unwrap();
                builder.finish().unwrap()
            };
            ValidatedFilterBundle::new(
                make_filter(FilterNamespace::Transactions, &key),
                make_filter(FilterNamespace::InboundMessages, &key),
                make_filter(FilterNamespace::Blocks, &key[..13]),
                u64::MAX,
            )
            .unwrap()
        };
        let recorder = TestMetricsRecorder::default();
        let selected_rates = FilterSelectedRateMetrics::default();
        metrics::with_local_recorder(&recorder, || {
            make_bundle(100_000).record_selected_rate_metrics(&selected_rates);
            make_bundle(1_000).record_selected_rate_metrics(&selected_rates);
        });
        assert_eq!(
            recorder.gauge("tycho_storage_rpc_filter_selected_error_ratio|namespace=transactions"),
            0.1
        );
    }

    #[tokio::test]
    async fn corrupt_persisted_bundle_is_rebuildable() {
        let (_context, _temp, partitions, store, id, ..) = sealed_filter_fixture().await;
        let cancelled = CancellationFlag::new();
        let bundle = store.build_and_publish(&partitions, id, &cancelled).unwrap();
        fs::write(
            store
                .final_generation_path(id, bundle.generation_id())
                .join(FilterNamespace::Transactions.file_name()),
            b"corrupt",
        )
        .unwrap();
        let rebuilt = maintain_filter_partition(
            &store,
            &partitions,
            id,
            Some(bundle.descriptor()),
            &cancelled,
        )
        .unwrap();
        assert_ne!(rebuilt.generation_id(), bundle.generation_id());
    }

    #[tokio::test]
    async fn builder_uses_only_authoritative_locator_keys_and_validates_every_serialized_key() {
        let (_context, _temp, partitions, store, id, transaction_key, inbound_key, block_key) =
            sealed_filter_fixture().await;
        let cancelled = CancellationFlag::new();
        let bundle = store.build_and_publish(&partitions, id, &cancelled).unwrap();
        assert!(bundle.filter(FilterNamespace::Transactions).contains(&transaction_key).unwrap());
        assert!(bundle.filter(FilterNamespace::InboundMessages).contains(&inbound_key).unwrap());
        assert!(bundle.filter(FilterNamespace::Blocks).contains(&block_key).unwrap());
        let loaded = store.load_and_validate_sealed(&partitions, id, &cancelled).unwrap().unwrap();
        assert_eq!(loaded.generation_id(), bundle.generation_id());
        assert_eq!(partitions.lock().sealed_cache_entry_count(), 0);
    }

    #[tokio::test]
    async fn structural_validation_reuses_a_complete_serialized_filter_without_source_rescan() {
        let (context, _temp, partitions, store, id, ..) =
            sealed_filter_fixture().await;
        let cancelled = CancellationFlag::new();
        let bundle = store.build_and_publish(&partitions, id, &cancelled).unwrap();
        let mut descriptor = bundle.descriptor();
        let empty_transactions = FilterBuilder::new(
            FilterFileIdentity {
                namespace: FilterNamespace::Transactions,
                partition_id: id.0,
                generation_id: bundle.generation_id(),
                manifest_digest: bundle.manifest_digest(),
            },
            0,
            descriptor.transactions.false_positive_rate_ppm,
        )
        .unwrap()
        .finish()
        .unwrap()
        .encode()
        .unwrap();
        descriptor.transactions = empty_transactions.metadata.to_descriptor();
        fs::write(
            store
                .final_generation_path(id, bundle.generation_id())
                .join(FilterNamespace::Transactions.file_name()),
            empty_transactions.bytes,
        )
        .unwrap();
        store
            .catalog
            .descriptors
            .insert(id.0.to_be_bytes(), codec::encode_filter_catalog_descriptor(&descriptor))
            .unwrap();
        drop(store);
        let mut config = RpcTransactionFiltersConfig::default();
        config.max_filter_bundle_bytes = 1;
        let store = FilterCatalogStore::open(&context, config).unwrap();
        let loaded = store.load_and_validate_sealed(&partitions, id, &cancelled).unwrap().unwrap();
        assert!(!loaded.filter(FilterNamespace::Transactions).contains(&[0x11; 32]).unwrap());
    }

    #[tokio::test]
    async fn manifest_change_before_catalog_publication_prevents_catalog_write() {
        let (_context, _temp, partitions, store, id, ..) = sealed_filter_fixture().await;
        MANIFEST_MUTATION_PARTITIONS.with(|target| *target.borrow_mut() = Some(partitions.clone()));
        FILTER_PUBLICATION_STAGE_HOOK.with(|hook| {
            *hook.borrow_mut() = Some(mutate_sealed_manifest_at_vp7);
        });
        assert!(store
            .build_and_publish(&partitions, id, &CancellationFlag::new())
            .is_err());
        FILTER_PUBLICATION_STAGE_HOOK.with(|hook| *hook.borrow_mut() = None);
        MANIFEST_MUTATION_PARTITIONS.with(|target| *target.borrow_mut() = None);
        assert!(store.catalog.descriptors.get(id.0.to_be_bytes()).unwrap().is_none());
    }

    #[tokio::test]
    async fn catalog_iteration_ignores_malformed_records_and_observes_cancellation() {
        let (context, _temp) = StorageContext::new_temp().await.unwrap();
        let store = FilterCatalogStore::open(&context, Default::default()).unwrap();
        store.catalog.descriptors.insert(PartitionId::FIRST.0.to_be_bytes(), [1, 2, 3]).unwrap();
        let cancelled = CancellationFlag::new();
        assert!(store.catalog_descriptors(&cancelled).unwrap().is_empty());
        cancelled.cancel();
        assert!(store.catalog_descriptors(&cancelled).is_err());
    }

    #[tokio::test]
    async fn builder_rejects_malformed_authoritative_locator_key_lengths() {
        let (context, _temp) = StorageContext::new_temp().await.unwrap();
        let db: super::super::db::RpcTransactionsDb = context.open_preconfigured("filter-source").unwrap();
        db.transactions_by_hash.insert([0; 31], [1]).unwrap();
        let error = match build_filter_file(
            &db,
            identity(FilterNamespace::Transactions),
            1,
            1_000,
            &CancellationFlag::new(),
        ) {
            Ok(_) => panic!("malformed locator key must reject filter build"),
            Err(error) => error,
        };
        assert!(format!("{error:#}").contains("filter key length"));
    }

    #[test]
    fn estimated_bundle_limit_matches_pinned_qfilter_allocation_before_filter_build() {
        for (source_key_count, false_positive_rate_ppm) in
            [(0, 1_000), (1, 1_000), (95, 1_000), (1_024, 500_000)]
        {
            let mut builder = FilterBuilder::new(
                identity(FilterNamespace::Transactions),
                source_key_count,
                false_positive_rate_ppm,
            )
            .unwrap();
            for index in 0..source_key_count {
                builder.insert(&[index as u8; 32]).unwrap();
            }
            let encoded = builder.finish().unwrap().encode().unwrap();
            let actual = u64::try_from(FILTER_FILE_HEADER_LEN)
                .unwrap()
                .checked_add(encoded.metadata.payload_len)
                .and_then(|value| value.checked_add(encoded.metadata.resident_bytes))
                .unwrap();
            assert_eq!(
                estimated_filter_bundle_bytes(source_key_count, false_positive_rate_ppm).unwrap(),
                actual
            );
        }
        let rates = [1_000; 3];
        let counts = [1; 3];
        let total = counts
            .into_iter()
            .zip(rates)
            .try_fold(0u64, |total, (count, rate)| {
                total.checked_add(estimated_filter_bundle_bytes(count, rate)?)
                    .context("test estimated filter bundle size overflow")
            })
            .unwrap();
        let selection = select_false_positive_rates(counts, rates, 500_000, total).unwrap();
        assert_eq!(selection.rates, rates);
        assert_eq!(selection.selected_bundle_bytes, total);
        let minimum_total = counts
            .into_iter()
            .zip([500_000; 3])
            .try_fold(0u64, |total, (count, rate)| {
                total.checked_add(estimated_filter_bundle_bytes(count, rate)?)
                    .context("test estimated filter bundle size overflow")
            })
            .unwrap();
        assert!(select_false_positive_rates(counts, rates, 500_000, minimum_total - 1).is_err());
    }

    #[test]
    fn adaptive_selection_advances_largest_namespaces_by_discrete_qfilter_precision() {
        fn total(counts: [u64; 3], rates: [u32; 3]) -> u64 {
            counts.into_iter().zip(rates).try_fold(0u64, |total, (count, rate)| {
                total.checked_add(estimated_filter_bundle_bytes(count, rate)?)
                    .context("test estimated filter bundle size overflow")
            })
            .unwrap()
        }

        let counts = [1_024, 128, 1];
        let preferred = [1_000; 3];
        let first_step = next_false_positive_rate_step(preferred[0], 100_000).unwrap();
        let second_largest_step = next_false_positive_rate_step(first_step, 100_000).unwrap();
        let expected = [
            [first_step, 1_000, 1_000],
            [first_step, first_step, 1_000],
            [first_step, first_step, first_step],
            [second_largest_step, first_step, first_step],
        ];
        for rates in expected {
            let selection = select_false_positive_rates(counts, preferred, 100_000, total(counts, rates)).unwrap();
            assert_eq!(selection.rates, rates);
        }
        assert_eq!(qfilter_precision_bits(first_step).unwrap() + 1, qfilter_precision_bits(1_000).unwrap());

        let equal_counts = [1; 3];
        for rates in [
            [first_step, 1_000, 1_000],
            [first_step, first_step, 1_000],
            [first_step, first_step, first_step],
        ] {
            let selection = select_false_positive_rates(equal_counts, preferred, 100_000, total(equal_counts, rates)).unwrap();
            assert_eq!(selection.rates, rates);
        }
    }

    #[test]
    fn publication_gate_prevents_in_flight_worker_publication_after_shutdown() {
        let gate = Arc::new(FilterPublicationGate::default());
        let cancelled = CancellationFlag::new();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let changes = Arc::new(AtomicU64::new(0));
        let worker_gate = gate.clone();
        let worker_cancelled = cancelled.clone();
        let worker_changes = changes.clone();
        let worker = std::thread::spawn(move || {
            worker_gate
                .publish(&worker_cancelled, || {
                    started_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                    worker_changes.fetch_add(1, Ordering::AcqRel);
                    Ok(())
                })
                .unwrap();
        });
        started_rx.recv().unwrap();
        let shutdown_gate = gate.clone();
        let shutdown_cancelled = cancelled.clone();
        let shutdown = std::thread::spawn(move || shutdown_gate.cancel(&shutdown_cancelled));
        while !cancelled.check() {
            std::thread::yield_now();
        }
        release_tx.send(()).unwrap();
        worker.join().unwrap();
        shutdown.join().unwrap();
        assert!(cancelled.check());
        assert!(gate
            .publish(&cancelled, || {
                changes.fetch_add(1, Ordering::AcqRel);
                Ok(())
            })
            .is_err());
        assert_eq!(changes.load(Ordering::Acquire), 1);
    }

    #[tokio::test]
    async fn publication_crash_matrix_never_creates_partial_catalog_bundle() {
        for stage in [
            FilterPublicationStage::Vp1,
            FilterPublicationStage::Vp2,
            FilterPublicationStage::Vp3,
            FilterPublicationStage::Vp4,
            FilterPublicationStage::Vp5,
            FilterPublicationStage::Vp6,
            FilterPublicationStage::Vp7,
            FilterPublicationStage::Vp8,
            FilterPublicationStage::Vp9,
            FilterPublicationStage::Vp10,
        ] {
            let (_context, _temp, partitions, store, id, ..) = sealed_filter_fixture().await;
            FILTER_PUBLICATION_STAGE_HOOK.with(|hook| {
                *hook.borrow_mut() = Some(fail_selected_publication_stage);
            });
            FAIL_PUBLICATION_STAGE.with(|selected| selected.set(Some(stage)));
            assert!(store.build_and_publish(&partitions, id, &CancellationFlag::new()).is_err());
            FAIL_PUBLICATION_STAGE.with(|selected| selected.set(None));
            FILTER_PUBLICATION_STAGE_HOOK.with(|hook| *hook.borrow_mut() = None);

            let descriptor = store.catalog.descriptors.get(id.0.to_be_bytes()).unwrap();
            let catalog_generation = if let Some(descriptor) = descriptor {
                let descriptor = codec::decode_filter_catalog_descriptor(descriptor.as_ref()).unwrap();
                let final_dir = store.final_generation_path(id, descriptor.generation_id);
                for namespace in FilterNamespace::ALL {
                    assert!(final_dir.join(namespace.file_name()).is_file());
                }
                Some(descriptor.generation_id)
            } else {
                None
            };
            store.cleanup_owned_generations(id, catalog_generation).unwrap();
        }
    }

    #[tokio::test]
    async fn vp11_failure_retry_retains_the_vp10_catalog_generation() {
        let (_context, _temp, partitions, store, id, transaction_key, ..) = sealed_filter_fixture().await;
        let cancelled = CancellationFlag::new();
        let old_bundle = store.build_and_publish(&partitions, id, &cancelled).unwrap();
        let new_bundle = store.build_and_publish(&partitions, id, &cancelled).unwrap();
        let mut descriptors = BTreeMap::from([(id, old_bundle.descriptor())]);
        retain_maintained_descriptor(&mut descriptors, id, &new_bundle);
        let publisher: FilterPublisher = Arc::new(|_, _| bail!("injected VP11 failure"));
        assert!(publisher(id, new_bundle.clone()).is_err());

        let retry_bundle = maintain_filter_partition(
            &store,
            &partitions,
            id,
            descriptors.get(&id).copied(),
            &cancelled,
        )
        .unwrap();
        assert_eq!(retry_bundle.generation_id(), new_bundle.generation_id());
        assert!(store
            .final_generation_path(id, new_bundle.generation_id())
            .is_dir());
        assert!(!store
            .final_generation_path(id, old_bundle.generation_id())
            .exists());
        assert!(old_bundle
            .filter(FilterNamespace::Transactions)
            .contains(&transaction_key)
            .unwrap());
    }

    #[tokio::test]
    async fn vp12_cleanup_removes_only_recognized_owned_generation_directories() {
        let (_context, _temp, partitions, store, id, ..) = sealed_filter_fixture().await;
        let bundle = store.build_and_publish(&partitions, id, &CancellationFlag::new()).unwrap();
        let unknown = store.partition_root(id).join("not-a-generation");
        fs::create_dir_all(&unknown).unwrap();
        store.cleanup_owned_generations(id, Some(bundle.generation_id())).unwrap();
        assert!(unknown.is_dir());
        assert!(store.final_generation_path(id, bundle.generation_id()).is_dir());
    }

    #[tokio::test]
    async fn cancelled_worker_prevents_catalog_registry_and_publisher_publication() {
        let (context, _temp, partitions, store, id, ..) = sealed_filter_fixture().await;
        let registry = Arc::new(FilterRegistry::default());
        let published = Arc::new(AtomicBool::new(false));
        let publisher: FilterPublisher = {
            let published = published.clone();
            Arc::new(move |_, _| {
                published.store(true, Ordering::Release);
                Ok(())
            })
        };
        let cancelled = CancellationFlag::new();
        cancelled.cancel();
        run_filter_worker(
            context,
            Default::default(),
            partitions,
            registry.clone(),
            publisher,
            Arc::new(Mutex::new(BTreeSet::from([id]))),
            Arc::new(Notify::new()),
            cancelled,
            Arc::new(FilterPublicationGate::default()),
            Arc::new(FilterSelectedRateMetrics::default()),
        )
        .await;
        assert!(store.catalog.descriptors.get(id.0.to_be_bytes()).unwrap().is_none());
        assert!(registry.get(id).is_none());
        assert!(!published.load(Ordering::Acquire));
    }
}
