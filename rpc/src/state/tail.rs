use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::fmt;
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
#[cfg(test)]
use std::sync::Barrier;

use anyhow::{Context, Result, ensure};
use parking_lot::Mutex;
use tokio::sync::Notify;
use tycho_storage::StorageContext;
use tycho_types::models::BlockId;
use tycho_types::prelude::HashBytes;
use weedb::{ColumnFamily, OwnedSnapshot, Table, rocksdb};

use super::TransactionMask;
use super::codec::{self, AccountKey, GcIntent, GcIntentPhase, TailAccountLocator, TailGenerationCommit, TailGenerationCounters, TailGenerationProgress, TailHashLocator, TailIdentity, TailInMsgLocator, TailLayoutVersion, TailPayloadKey, TailProgressCursor};
use super::db::{RpcTailDb, RpcTailTables};

const TAIL_SUBDIR: &str = "rpc/tail-directory";
const TAIL_SWEEP_MAX_RECORDS: usize = 1024;
const TAIL_CHUNK_DIGEST_DOMAIN: &[u8] = b"tycho-rpc-tail-chunk-digest";
const TAIL_CHUNK_DIGEST_VERSION: u8 = 1;

/// Owns the writable monolithic tail and serializes its read-check-write protocols.
pub(super) struct TailStore {
    db: Arc<RpcTailDb>,
    identity: TailIdentity,
    initial_visible_generation: u64,
    generation_tracker: Arc<TailGenerationTracker>,
    write_lock: Mutex<()>,
    #[cfg(test)]
    fail_next_append_write: AtomicBool,
    #[cfg(test)]
    fail_next_sweep_write: AtomicBool,
    #[cfg(test)]
    sweep_floor_gate: Mutex<Option<Arc<TailSweepFloorGate>>>,
    #[cfg(test)]
    sweep_attempts: AtomicU64,
    #[cfg(test)]
    sweep_attempted: Notify,
    #[cfg(test)]
    sweep_writes: AtomicU64,
    #[cfg(test)]
    sweep_written: Notify,
}

impl TailStore {
    pub(super) fn open(
        context: &StorageContext,
        expected_identity: TailIdentity,
        visible_generation: u64,
    ) -> Result<Self> {
        ensure!(expected_identity.layout_version == TailLayoutVersion::MonolithicV1, "unsupported writable RPC tail layout");
        let path = context.root_dir().path().join(TAIL_SUBDIR);
        let path_exists = path.try_exists()?;
        ensure!(visible_generation == 0 || path.is_dir(), "published RPC tail directory is missing");
        // only a missing or proven-empty generation-zero directory may create the tail schema
        let create = visible_generation == 0
            && (!path_exists || path.is_dir() && fs::read_dir(&path)?.next().transpose()?.is_none());
        let db = Arc::new(if create {
            context.open_preconfigured::<_, RpcTailTables>(TAIL_SUBDIR)?
        } else {
            ensure!(path.is_dir(), "existing RPC tail path is not a directory");
            context.open::<_, RpcTailTables, _>(TAIL_SUBDIR, |options| {
                context.apply_default_options(options);
                options.create_if_missing(false);
                options.create_missing_column_families(false);
            })?
        });
        let this = Self {
            db,
            identity: expected_identity,
            initial_visible_generation: visible_generation,
            generation_tracker: Arc::new(TailGenerationTracker::new(visible_generation)),
            write_lock: Default::default(),
            #[cfg(test)]
            fail_next_append_write: AtomicBool::new(false),
            #[cfg(test)]
            fail_next_sweep_write: AtomicBool::new(false),
            #[cfg(test)]
            sweep_floor_gate: Default::default(),
            #[cfg(test)]
            sweep_attempts: AtomicU64::new(0),
            #[cfg(test)]
            sweep_attempted: Notify::new(),
            #[cfg(test)]
            sweep_writes: AtomicU64::new(0),
            #[cfg(test)]
            sweep_written: Notify::new(),
        };
        this.initialize_or_validate_identity(expected_identity, visible_generation)?;
        if visible_generation > 0 {
            let commit = this
                .generation_commit(visible_generation)?
                .ok_or_else(|| missing_authoritative_error(
                    "published RPC tail generation commit is missing",
                ))?;
            if commit.layout_version != TailLayoutVersion::MonolithicV1 {
                return Err(conflicting_authoritative_error(
                    "published RPC tail generation uses an incompatible layout",
                ));
            }
        }
        metrics::gauge!(
            "tycho_storage_rpc_tail_layout_info",
            "layout" => "monolithic_v1",
        )
        .set(1.0);
        this.generation_tracker.refresh_metrics();
        Ok(this)
    }

    fn snapshot(&self) -> TailSnapshot {
        TailSnapshot {
            db: self.db.clone(),
            snapshot: self.db.owned_snapshot(),
        }
    }

    pub(super) fn request_snapshot(
        &self,
        layout_version: TailLayoutVersion,
        visible_generation: u64,
    ) -> Result<TailRequestSnapshot> {
        if layout_version != TailLayoutVersion::MonolithicV1 {
            return Err(conflicting_authoritative_error(
                "unsupported RPC tail request layout",
            ));
        }
        if layout_version != self.identity.layout_version {
            return Err(conflicting_authoritative_error(
                "RPC tail request layout does not match the store identity",
            ));
        }
        ensure!(visible_generation >= self.initial_visible_generation, "RPC tail request generation predates the store control generation");
        if visible_generation > 0 {
            let commit = self
                .generation_commit(visible_generation)?
                .ok_or_else(|| missing_authoritative_error(
                    "visible RPC tail request generation commit is missing",
                ))?;
            if commit.layout_version != layout_version {
                return Err(conflicting_authoritative_error(
                    "visible RPC tail request generation uses an incompatible layout",
                ));
            }
        }
        let generation_pin = self.generation_tracker.pin(visible_generation)?;
        Ok(TailRequestSnapshot {
            inner: Arc::new(TailRequestSnapshotInner {
                snapshot: self.snapshot(),
                layout_version,
                visible_generation,
                _generation_pin: generation_pin,
            }),
        })
    }

    pub(super) fn oldest_live_generation(&self) -> Option<u64> {
        self.generation_tracker.oldest_live_generation()
    }

    pub(super) fn can_sweep_generation(&self, dead_generation: u64) -> bool {
        self.generation_tracker.can_sweep_generation(dead_generation)
    }

    pub(super) fn generation_release_notify(&self) -> &Notify {
        &self.generation_tracker.released
    }

    #[cfg(test)]
    fn db(&self) -> &RpcTailDb {
        &self.db
    }

    fn initialize_or_validate_identity(
        &self,
        expected_identity: TailIdentity,
        visible_generation: u64,
    ) -> Result<()> {
        let _guard = self.write_lock.lock();
        match self.db.generation_commits.get(codec::tail_identity_key())? {
            Some(bytes) => {
                let identity = codec::decode_tail_identity(bytes.as_ref())
                    .context("invalid RPC tail identity")?;
                ensure!(identity == expected_identity, "RPC tail identity does not match control state");
            }
            None => {
                ensure!(visible_generation == 0, "published RPC tail identity is missing");
                self.ensure_empty_for_bootstrap()?;
                let mut batch = rocksdb::WriteBatch::default();
                batch.put_cf(
                    &self.db.generation_commits.cf(),
                    codec::tail_identity_key(),
                    codec::encode_tail_identity(expected_identity),
                );
                self.db
                    .rocksdb()
                    .write_opt(batch, self.db.generation_commits.write_config())
                    .context("failed to initialize RPC tail identity")?;
            }
        }
        Ok(())
    }

    fn ensure_empty_for_bootstrap(&self) -> Result<()> {
        macro_rules! ensure_empty {
            ($table:ident) => {{
                let mut iterator = self.db.rocksdb().raw_iterator_cf(&self.db.$table.cf());
                iterator.seek_to_first();
                ensure!(!iterator.valid(), "RPC tail without an identity contains committed data");
                iterator.status()?;
            }};
        }
        ensure_empty!(transactions);
        ensure_empty!(transactions_by_account);
        ensure_empty!(transactions_by_hash);
        ensure_empty!(transactions_by_in_msg);
        ensure_empty!(retired_by_generation);
        ensure_empty!(generation_progress);
        ensure_empty!(generation_commits);
        Ok(())
    }

    pub(super) fn generation_progress(&self, target_generation: u64) -> Result<Option<TailGenerationProgress>> {
        let key = codec::tail_generation_progress_key(target_generation)?;
        let progress = self.db
            .generation_progress
            .get(key)?
            .map(|bytes| codec::decode_tail_generation_progress(bytes.as_ref()))
            .transpose()
            .map_err(|_| malformed_authoritative_error(
                "invalid RPC tail generation progress",
            ))?;
        if let Some(progress) = progress
            && progress.target_generation != target_generation
        {
            return Err(conflicting_authoritative_error(
                "RPC tail progress key and value generations differ",
            ));
        }
        Ok(progress)
    }

    pub(super) fn generation_commit(&self, target_generation: u64) -> Result<Option<TailGenerationCommit>> {
        let key = codec::tail_generation_commit_key(target_generation)?;
        let commit = self.db
            .generation_commits
            .get(key)?
            .map(|bytes| codec::decode_tail_generation_commit(bytes.as_ref()))
            .transpose()
            .map_err(|_| malformed_authoritative_error(
                "invalid RPC tail generation commit",
            ))?;
        if let Some(commit) = commit
            && commit.target_generation != target_generation
        {
            return Err(conflicting_authoritative_error(
                "RPC tail commit key and value generations differ",
            ));
        }
        if let Some(commit) = commit
            && commit.layout_version != self.identity.layout_version
        {
            return Err(conflicting_authoritative_error(
                "RPC tail generation commit layout conflicts with the store identity",
            ));
        }
        Ok(commit)
    }

    pub(super) fn validate_startup_gc_intent(&self, intent: GcIntent) -> Result<()> {
        codec::encode_gc_intent(&intent)?;
        match intent.phase {
            GcIntentPhase::Evacuating | GcIntentPhase::Prepared => {
                ensure!(intent.previous_visible_generation == self.initial_visible_generation,
                    "pre-cutover RPC transaction GC intent conflicts with the published tail generation");
            }
            GcIntentPhase::CutoverCommitted | GcIntentPhase::Deleting => {
                ensure!(intent.target_generation == self.initial_visible_generation,
                    "post-cutover RPC transaction GC intent conflicts with the published tail generation");
            }
        }
        let snapshot = self.snapshot();
        let progress = snapshot.generation_progress(intent.target_generation)?;
        let commit = snapshot.generation_commit(intent.target_generation)?;
        if let Some(progress) = progress {
            // require the exact persisted operation identity without scanning staged data
            ensure!(progress.target_generation == intent.target_generation
                && progress.operation_id == intent.operation_id
                && progress.source_partition_id == intent.source_partition_id
                && progress.source_manifest_digest == intent.source_manifest_digest
                && progress.retention_policy_digest == intent.retention_policy_digest,
                "RPC tail generation progress conflicts with the persisted GC intent");
        }
        match (intent.phase, progress, commit) {
            (GcIntentPhase::Evacuating, None, None) => Ok(()),
            (GcIntentPhase::Evacuating, Some(progress), None) if !progress.eof => Ok(()),
            (_, Some(progress), Some(commit)) if progress.eof => {
                let expected_commit = TailGenerationCommit {
                    layout_version: self.identity.layout_version,
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
                ensure!(commit == expected_commit,
                    "RPC tail terminal generation commit conflicts with the persisted GC intent");
                Ok(())
            }
            _ => anyhow::bail!("RPC tail generation metadata is invalid for the persisted GC intent phase"),
        }
    }
}

struct TailGenerationTracker {
    state: Mutex<TailGenerationState>,
    released: Notify,
    #[cfg(test)]
    before_pin_gate: Mutex<Option<Arc<TailPinGate>>>,
}

struct TailGenerationState {
    minimum_request_generation: u64,
    counts: BTreeMap<u64, usize>,
}

#[cfg(test)]
struct TailSweepFloorGate {
    entered: Barrier,
    release: Barrier,
}

#[cfg(test)]
struct TailPinGate {
    before_lock: Barrier,
}

#[cfg(test)]
impl TailSweepFloorGate {
    fn new() -> Self {
        Self {
            entered: Barrier::new(2),
            release: Barrier::new(2),
        }
    }
}

#[cfg(test)]
impl TailPinGate {
    fn new() -> Self {
        Self {
            before_lock: Barrier::new(2),
        }
    }
}

impl TailGenerationTracker {
    fn new(minimum_request_generation: u64) -> Self {
        Self {
            state: Mutex::new(TailGenerationState {
                minimum_request_generation,
                counts: BTreeMap::new(),
            }),
            released: Notify::new(),
            #[cfg(test)]
            before_pin_gate: Default::default(),
        }
    }

    fn pin(self: &Arc<Self>, generation: u64) -> Result<TailGenerationPin> {
        #[cfg(test)]
        if let Some(gate) = self.before_pin_gate.lock().clone() {
            gate.before_lock.wait();
        }
        let mut state = self.state.lock();
        ensure!(generation >= state.minimum_request_generation,
            "RPC tail request generation predates the sweep safety floor");
        let count = state.counts.entry(generation).or_default();
        *count = count.checked_add(1).expect("RPC tail request generation pin count overflow");
        let metrics = Self::metrics(&state);
        Self::record_metrics(metrics);
        drop(state);
        Ok(TailGenerationPin {
            tracker: self.clone(),
            generation,
        })
    }

    fn oldest_live_generation(&self) -> Option<u64> {
        self.state
            .lock()
            .counts
            .first_key_value()
            .map(|(&generation, _)| generation)
    }

    fn can_sweep_generation(&self, dead_generation: u64) -> bool {
        self.state
            .lock()
            .counts
            .first_key_value()
            .is_none_or(|(&generation, _)| generation >= dead_generation)
    }

    fn unpin(&self, generation: u64) {
        let mut state = self.state.lock();
        let previous_oldest = state.counts.first_key_value().map(|(&generation, _)| generation);
        let count = state
            .counts
            .get_mut(&generation)
            .expect("RPC tail request generation pin is missing");
        if *count == 1 {
            state.counts.remove(&generation);
        } else {
            *count -= 1;
        }
        let next_oldest = state.counts.first_key_value().map(|(&generation, _)| generation);
        let metrics = Self::metrics(&state);
        Self::record_metrics(metrics);
        drop(state);
        if previous_oldest != next_oldest {
            self.released.notify_one();
        }
    }

    fn refresh_metrics(&self) {
        let state = self.state.lock();
        Self::record_metrics(Self::metrics(&state));
    }

    fn metrics(state: &TailGenerationState) -> (u64, u64) {
        let oldest_required_generation = state
            .counts
            .first_key_value()
            .map_or(state.minimum_request_generation, |(&generation, _)| generation);
        let live_request_snapshots = state
            .counts
            .values()
            .fold(0usize, |total, count| total.saturating_add(*count));
        (
            oldest_required_generation,
            u64::try_from(live_request_snapshots).unwrap_or(u64::MAX),
        )
    }

    fn record_metrics((oldest_required_generation, live_request_snapshots): (u64, u64)) {
        metrics::gauge!("tycho_storage_rpc_tail_oldest_required_generation")
            .set(oldest_required_generation as f64);
        metrics::gauge!("tycho_storage_rpc_tail_live_request_snapshots")
            .set(live_request_snapshots as f64);
    }
}

struct TailGenerationPin {
    tracker: Arc<TailGenerationTracker>,
    generation: u64,
}

impl Drop for TailGenerationPin {
    fn drop(&mut self) {
        self.tracker.unpin(self.generation);
    }
}

#[derive(Clone)]
pub(super) struct TailRequestSnapshot {
    inner: Arc<TailRequestSnapshotInner>,
}

pub(super) struct TailRequestSnapshotInner {
    snapshot: TailSnapshot,
    layout_version: TailLayoutVersion,
    visible_generation: u64,
    _generation_pin: TailGenerationPin,
}

impl std::ops::Deref for TailRequestSnapshot {
    type Target = TailRequestSnapshotInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl TailRequestSnapshot {
    pub(super) fn layout_version(&self) -> TailLayoutVersion {
        self.layout_version
    }

    pub(super) fn visible_generation(&self) -> u64 {
        self.visible_generation
    }

    #[cfg(test)]
    fn snapshot(&self) -> &TailSnapshot {
        &self.snapshot
    }

    pub(super) fn transaction_by_hash(
        &self,
        transaction_hash: &HashBytes,
        max_mc_seqno: u32,
    ) -> Result<Option<TailTransactionRecord>> {
        let Some(locator) = self.snapshot.hash_locator(transaction_hash)? else {
            return Ok(None);
        };
        if !self.generation_is_visible(locator.born_generation, locator.dead_generation)
            || locator.mc_seqno > max_mc_seqno
        {
            return Ok(None);
        }
        let record = self
            .snapshot
            .transaction_record(locator.payload_key)?
            .ok_or_else(|| missing_authoritative_error(
                "visible RPC tail hash locator points to a missing transaction",
            ))?;
        if record.account_locator.transaction_hash != *transaction_hash
            || record.hash_locator != locator
        {
            return Err(conflicting_authoritative_error(
                "visible RPC tail hash locator conflicts with its transaction",
            ));
        }
        Ok(Some(record))
    }

    pub(super) fn transaction_by_in_msg(
        &self,
        in_msg_hash: &HashBytes,
        max_mc_seqno: u32,
    ) -> Result<Option<TailTransactionRecord>> {
        let Some(locator) = self.snapshot.in_msg_locator(in_msg_hash)? else {
            return Ok(None);
        };
        if !self.generation_is_visible(locator.born_generation, locator.dead_generation) {
            return Ok(None);
        }
        let record = self
            .snapshot
            .transaction_record(locator.payload_key)?
            .ok_or_else(|| missing_authoritative_error(
                "visible RPC tail inbound-message locator points to a missing transaction",
            ))?;
        if record.in_msg_locator != Some((*in_msg_hash, locator)) {
            return Err(conflicting_authoritative_error(
                "visible RPC tail inbound-message locator conflicts with its transaction",
            ));
        }
        if record.value.related_mc_seqno() > max_mc_seqno {
            return Ok(None);
        }
        Ok(Some(record))
    }

    pub(super) fn next_account_transaction(
        &self,
        account: AccountKey,
        start_lt: u64,
        end_lt: u64,
        reverse: bool,
        cursor_lt: Option<u64>,
        max_mc_seqno: u32,
    ) -> Result<Option<TailTransactionRecord>> {
        if end_lt < start_lt {
            return Ok(None);
        }
        let seek_lt = if reverse {
            match cursor_lt {
                Some(cursor_lt) => match cursor_lt.checked_sub(1) {
                    Some(cursor_lt) if cursor_lt >= start_lt => cursor_lt.min(end_lt),
                    _ => return Ok(None),
                },
                None => end_lt,
            }
        } else {
            match cursor_lt {
                Some(cursor_lt) => match cursor_lt.checked_add(1) {
                    Some(cursor_lt) if cursor_lt <= end_lt => cursor_lt.max(start_lt),
                    _ => return Ok(None),
                },
                None => start_lt,
            }
        };
        let seek_key = codec::tail_payload_key(account, seek_lt);
        let table = &self.snapshot.db.transactions_by_account;
        let mut read_options = table.new_read_config();
        read_options.set_snapshot(&self.snapshot.snapshot);
        let mut iterator = self
            .snapshot
            .db
            .rocksdb()
            .raw_iterator_cf_opt(&table.cf(), read_options);
        if reverse {
            iterator.seek_for_prev(seek_key);
        } else {
            iterator.seek(seek_key);
        }
        while let Some((key, value)) = iterator.item() {
            let (key_account, lt) = codec::decode_tail_payload_key(key)
                .map_err(|_| malformed_authoritative_error(
                    "invalid RPC tail account transaction key",
                ))?;
            if key_account != account || lt < start_lt || lt > end_lt {
                break;
            }
            let locator = codec::decode_tail_account_locator(value)
                .map_err(|_| malformed_authoritative_error(
                    "invalid RPC tail account locator",
                ))?;
            if self.generation_is_visible(locator.born_generation, locator.dead_generation) {
                let payload_key = codec::tail_payload_key(account, lt);
                let record = self
                    .snapshot
                    .transaction_record(payload_key)?
                    .ok_or_else(|| missing_authoritative_error(
                        "visible RPC tail account locator points to a missing transaction",
                    ))?;
                if record.account_locator != locator {
                    return Err(conflicting_authoritative_error(
                        "visible RPC tail account locator conflicts with its transaction",
                    ));
                }
                if record.value.related_mc_seqno() <= max_mc_seqno {
                    iterator.status()?;
                    return Ok(Some(record));
                }
            }
            if reverse {
                iterator.prev();
            } else {
                iterator.next();
            }
        }
        iterator.status()?;
        Ok(None)
    }

    pub(super) fn source_transaction(
        &self,
        account: AccountKey,
        message_lt: u64,
        max_mc_seqno: u32,
    ) -> Result<Option<TailTransactionRecord>> {
        let Some(end_lt) = message_lt.checked_sub(1) else {
            return Ok(None);
        };
        self.next_account_transaction(account, 0, end_lt, true, None, max_mc_seqno)
    }

    /// Returns at most `max_count` visible transactions before the exclusive cursor, newest first.
    pub(super) fn newest_account_transactions(
        &self,
        account: AccountKey,
        cursor_lt: Option<u64>,
        max_count: usize,
        max_mc_seqno: u32,
    ) -> Result<Vec<TailTransactionRecord>> {
        if max_count == 0 {
            return Ok(Vec::new());
        }
        let seek_lt = match cursor_lt {
            Some(cursor_lt) => match cursor_lt.checked_sub(1) {
                Some(cursor_lt) => cursor_lt,
                None => return Ok(Vec::new()),
            },
            None => u64::MAX,
        };
        let seek_key = codec::tail_payload_key(account, seek_lt);
        let table = &self.snapshot.db.transactions_by_account;
        let mut read_options = table.new_read_config();
        read_options.set_snapshot(&self.snapshot.snapshot);
        let mut iterator = self
            .snapshot
            .db
            .rocksdb()
            .raw_iterator_cf_opt(&table.cf(), read_options);
        iterator.seek_for_prev(seek_key);
        let mut records = Vec::new();
        while records.len() < max_count {
            let Some((key, value)) = iterator.item() else {
                break;
            };
            let (key_account, _) = codec::decode_tail_payload_key(key)
                .map_err(|_| malformed_authoritative_error(
                    "invalid RPC tail account transaction key",
                ))?;
            if key_account != account {
                break;
            }
            let locator = codec::decode_tail_account_locator(value)
                .map_err(|_| malformed_authoritative_error(
                    "invalid RPC tail account locator",
                ))?;
            if self.generation_is_visible(locator.born_generation, locator.dead_generation) {
                let payload_key = key.try_into().expect("validated RPC tail payload key length");
                let record = self
                    .snapshot
                    .transaction_record(payload_key)?
                    .ok_or_else(|| missing_authoritative_error(
                        "visible RPC tail account locator points to a missing transaction",
                    ))?;
                if record.account_locator != locator {
                    return Err(conflicting_authoritative_error(
                        "visible RPC tail account locator conflicts with its transaction",
                    ));
                }
                if record.value.related_mc_seqno() <= max_mc_seqno {
                    records.push(record);
                }
            }
            iterator.prev();
        }
        iterator.status()?;
        Ok(records)
    }

    fn generation_is_visible(&self, born_generation: u64, dead_generation: Option<u64>) -> bool {
        born_generation <= self.visible_generation
            && dead_generation.is_none_or(|dead_generation| self.visible_generation < dead_generation)
    }
}

struct TailSnapshot {
    db: Arc<RpcTailDb>,
    snapshot: OwnedSnapshot,
}

impl TailSnapshot {
    pub(super) fn transaction(&self, payload_key: TailPayloadKey) -> Result<Option<TailTransactionValue>> {
        snapshot_get(&self.db, &self.snapshot, &self.db.transactions, &payload_key)?
            .map(TailTransactionValue::decode)
            .transpose()
            .map_err(|_| malformed_authoritative_error(
                "invalid RPC tail transaction payload",
            ))
    }

    pub(super) fn account_locator(&self, payload_key: TailPayloadKey) -> Result<Option<TailAccountLocator>> {
        snapshot_get(&self.db, &self.snapshot, &self.db.transactions_by_account, &payload_key)?
            .map(|bytes| codec::decode_tail_account_locator(&bytes))
            .transpose()
            .map_err(|_| malformed_authoritative_error(
                "invalid RPC tail account locator",
            ))
    }

    pub(super) fn hash_locator(&self, transaction_hash: &HashBytes) -> Result<Option<TailHashLocator>> {
        snapshot_get(&self.db, &self.snapshot, &self.db.transactions_by_hash, transaction_hash.as_slice())?
            .map(|bytes| codec::decode_tail_hash_locator(&bytes))
            .transpose()
            .map_err(|_| malformed_authoritative_error(
                "invalid RPC tail hash locator",
            ))
    }

    pub(super) fn in_msg_locator(&self, in_msg_hash: &HashBytes) -> Result<Option<TailInMsgLocator>> {
        snapshot_get(&self.db, &self.snapshot, &self.db.transactions_by_in_msg, in_msg_hash.as_slice())?
            .map(|bytes| codec::decode_tail_in_msg_locator(&bytes))
            .transpose()
            .map_err(|_| malformed_authoritative_error(
                "invalid RPC tail inbound-message locator",
            ))
    }

    pub(super) fn generation_progress(&self, target_generation: u64) -> Result<Option<TailGenerationProgress>> {
        let key = codec::tail_generation_progress_key(target_generation)?;
        let progress = snapshot_get(&self.db, &self.snapshot, &self.db.generation_progress, &key)?
            .map(|bytes| codec::decode_tail_generation_progress(&bytes))
            .transpose()
            .map_err(|_| malformed_authoritative_error(
                "invalid RPC tail generation progress",
            ))?;
        if let Some(progress) = progress
            && progress.target_generation != target_generation
        {
            return Err(conflicting_authoritative_error(
                "RPC tail progress key and value generations differ",
            ));
        }
        Ok(progress)
    }

    pub(super) fn generation_commit(&self, target_generation: u64) -> Result<Option<TailGenerationCommit>> {
        let key = codec::tail_generation_commit_key(target_generation)?;
        let commit = snapshot_get(&self.db, &self.snapshot, &self.db.generation_commits, &key)?
            .map(|bytes| codec::decode_tail_generation_commit(&bytes))
            .transpose()
            .map_err(|_| malformed_authoritative_error(
                "invalid RPC tail generation commit",
            ))?;
        if let Some(commit) = commit
            && commit.target_generation != target_generation
        {
            return Err(conflicting_authoritative_error(
                "RPC tail commit key and value generations differ",
            ));
        }
        Ok(commit)
    }

    pub(super) fn retirement_entry(
        &self,
        dead_generation: u64,
        payload_key: TailPayloadKey,
    ) -> Result<Option<TailRetirementEntry>> {
        let key = codec::retired_transaction_key(dead_generation, payload_key)?;
        let Some(value) = snapshot_get(&self.db, &self.snapshot, &self.db.retired_by_generation, &key)? else {
            return Ok(None);
        };
        if !value.is_empty() {
            return Err(malformed_authoritative_error(
                "RPC tail retirement queue value must be empty",
            ));
        }
        Ok(Some(TailRetirementEntry {
            dead_generation,
            payload_key,
        }))
    }

    pub(super) fn transaction_record(
        &self,
        payload_key: TailPayloadKey,
    ) -> Result<Option<TailTransactionRecord>> {
        let Some(value) = self.transaction(payload_key)? else {
            if self.account_locator(payload_key)?.is_some() {
                return Err(missing_authoritative_error(
                    "RPC tail account locator exists without its payload",
                ));
            }
            return Ok(None);
        };
        let account_locator = self
            .account_locator(payload_key)?
            .ok_or_else(|| missing_authoritative_error(
                "RPC tail transaction is missing its account locator",
            ))?;
        if account_locator.transaction_hash != value.transaction_hash() {
            return Err(conflicting_authoritative_error(
                "RPC tail account locator conflicts with its payload",
            ));
        }
        let hash_locator = self
            .hash_locator(&account_locator.transaction_hash)?
            .ok_or_else(|| missing_authoritative_error(
                "RPC tail transaction is missing its hash locator",
            ))?;
        // validate the complete cross-index identity before exposing a committed record
        if hash_locator.payload_key != payload_key
            || hash_locator.mc_seqno != value.related_mc_seqno()
            || hash_locator.born_generation != account_locator.born_generation
            || hash_locator.dead_generation != account_locator.dead_generation
        {
            return Err(conflicting_authoritative_error(
                "RPC tail hash locator conflicts with its account locator or payload",
            ));
        }
        let in_msg_locator = match value.in_msg_hash() {
            Some(in_msg_hash) => {
                let locator = self
                    .in_msg_locator(&in_msg_hash)?
                    .ok_or_else(|| missing_authoritative_error(
                        "RPC tail transaction is missing its inbound-message locator",
                    ))?;
                // validate the complete cross-index identity before exposing a committed record
                if locator.payload_key != payload_key
                    || locator.born_generation != account_locator.born_generation
                    || locator.dead_generation != account_locator.dead_generation
                {
                    return Err(conflicting_authoritative_error(
                        "RPC tail inbound-message locator conflicts with its account locator",
                    ));
                }
                Some((in_msg_hash, locator))
            }
            None => None,
        };
        Ok(Some(TailTransactionRecord {
            payload_key,
            value,
            account_locator,
            hash_locator,
            in_msg_locator,
        }))
    }
}

fn snapshot_get<T: ColumnFamily>(
    db: &RpcTailDb,
    snapshot: &OwnedSnapshot,
    table: &Table<T>,
    key: &[u8],
) -> Result<Option<Vec<u8>>> {
    ensure!(Arc::ptr_eq(snapshot.db(), table.db()) && Arc::ptr_eq(snapshot.db(), db.rocksdb()), "RPC tail snapshot and column family belong to different databases");
    let mut read_options = table.new_read_config();
    read_options.set_snapshot(snapshot);
    db.rocksdb()
        .get_cf_opt(&table.cf(), key, &read_options)
        .map_err(Into::into)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct TailTransactionValue {
    bytes: Vec<u8>,
    related_mc_seqno: u32,
    transaction_hash: HashBytes,
    in_msg_hash: Option<HashBytes>,
}

impl TailTransactionValue {
    fn decode(bytes: Vec<u8>) -> Result<Self> {
        let value = codec::decode_transaction_value(&bytes)?;
        let payload = value.payload();
        let mask = TransactionMask::from_bits(payload[0]).context("invalid RPC tail transaction mask")?;
        Ok(Self {
            related_mc_seqno: value.mc_seqno(),
            transaction_hash: HashBytes::from_slice(&payload[1..33]),
            in_msg_hash: mask.has_msg_hash().then(|| HashBytes::from_slice(&payload[33..65])),
            bytes,
        })
    }

    pub(super) fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(super) fn related_mc_seqno(&self) -> u32 {
        self.related_mc_seqno
    }

    pub(super) fn transaction_hash(&self) -> HashBytes {
        self.transaction_hash
    }

    pub(super) fn in_msg_hash(&self) -> Option<HashBytes> {
        self.in_msg_hash
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct TailRetirementEntry {
    pub(super) dead_generation: u64,
    pub(super) payload_key: TailPayloadKey,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum AuthoritativeErrorKind {
    MalformedCommittedData,
    MissingCommittedData,
    ConflictingCommittedData,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum TailSweepNext {
    Empty,
    Ready,
    AwaitPublishedGeneration {
        dead_generation: u64,
    },
    AwaitRequestGeneration {
        dead_generation: u64,
        oldest_live_generation: u64,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct TailSweepResult {
    pub(super) swept_records: usize,
    pub(super) oldest_swept_generation: Option<u64>,
    pub(super) newest_swept_generation: Option<u64>,
    pub(super) next: TailSweepNext,
}

#[derive(Debug)]
struct AuthoritativeError {
    kind: AuthoritativeErrorKind,
    message: &'static str,
}

impl fmt::Display for AuthoritativeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message)
    }
}

impl std::error::Error for AuthoritativeError {}

pub(super) fn classify_authoritative_error(
    error: &anyhow::Error,
) -> Option<AuthoritativeErrorKind> {
    error
        .downcast_ref::<AuthoritativeError>()
        .map(|error| error.kind)
}

fn authoritative_error(
    kind: AuthoritativeErrorKind,
    message: &'static str,
) -> anyhow::Error {
    AuthoritativeError { kind, message }.into()
}

pub(super) fn malformed_authoritative_error(message: &'static str) -> anyhow::Error {
    authoritative_error(
        AuthoritativeErrorKind::MalformedCommittedData,
        message,
    )
}

pub(super) fn missing_authoritative_error(message: &'static str) -> anyhow::Error {
    authoritative_error(
        AuthoritativeErrorKind::MissingCommittedData,
        message,
    )
}

pub(super) fn conflicting_authoritative_error(message: &'static str) -> anyhow::Error {
    authoritative_error(
        AuthoritativeErrorKind::ConflictingCommittedData,
        message,
    )
}

#[derive(Clone, Copy)]
struct TailSweepDelete {
    entry: TailRetirementEntry,
    transaction_hash: HashBytes,
    in_msg_hash: Option<HashBytes>,
}

#[derive(Clone, Debug)]
pub(super) struct TailTransactionRecord {
    pub(super) payload_key: TailPayloadKey,
    pub(super) value: TailTransactionValue,
    pub(super) account_locator: TailAccountLocator,
    pub(super) hash_locator: TailHashLocator,
    pub(super) in_msg_locator: Option<(HashBytes, TailInMsgLocator)>,
}

impl TailTransactionRecord {
    pub(super) fn retirement_staged_write_bytes(&self, dead_generation: u64) -> Result<u64> {
        let account = codec::encode_tail_account_locator(TailAccountLocator {
            dead_generation: Some(dead_generation),
            ..self.account_locator
        })?;
        let hash = codec::encode_tail_hash_locator(&TailHashLocator {
            dead_generation: Some(dead_generation),
            ..self.hash_locator
        })?;
        let retirement_key = codec::retired_transaction_key(dead_generation, self.payload_key)?;
        let mut bytes = checked_staged_write_bytes(&[
            self.payload_key.len(),
            account.len(),
            self.account_locator.transaction_hash.as_slice().len(),
            hash.len(),
            retirement_key.len(),
        ])?;
        if let Some((in_msg_hash, locator)) = self.in_msg_locator {
            let in_msg = codec::encode_tail_in_msg_locator(TailInMsgLocator {
                dead_generation: Some(dead_generation),
                ..locator
            })?;
            bytes = bytes
                .checked_add(checked_staged_write_bytes(&[
                    in_msg_hash.as_slice().len(),
                    in_msg.len(),
                ])?)
                .context("RPC tail staged write byte count overflow")?;
        }
        Ok(bytes)
    }
}

#[derive(Clone, Debug)]
pub(super) struct TailPromotedTransaction {
    payload_key: TailPayloadKey,
    value: TailTransactionValue,
    account_locator: TailAccountLocator,
    hash_locator: TailHashLocator,
    in_msg_locator: Option<(HashBytes, TailInMsgLocator)>,
}

impl TailPromotedTransaction {
    pub(super) fn new(
        payload_key: TailPayloadKey,
        value: Vec<u8>,
        block_id: BlockId,
        born_generation: u64,
    ) -> Result<Self> {
        codec::decode_tail_payload_key(&payload_key)?;
        let value = TailTransactionValue::decode(value)?;
        let account_locator = TailAccountLocator {
            transaction_hash: value.transaction_hash(),
            born_generation,
            dead_generation: None,
        };
        codec::encode_tail_account_locator(account_locator)?;
        let hash_locator = TailHashLocator {
            payload_key,
            block_id,
            mc_seqno: value.related_mc_seqno(),
            born_generation,
            dead_generation: None,
        };
        codec::encode_tail_hash_locator(&hash_locator)?;
        let in_msg_locator = value.in_msg_hash().map(|hash| {
            (
                hash,
                TailInMsgLocator {
                    payload_key,
                    born_generation,
                    dead_generation: None,
                },
            )
        });
        if let Some((_, locator)) = in_msg_locator {
            codec::encode_tail_in_msg_locator(locator)?;
        }
        Ok(Self {
            payload_key,
            value,
            account_locator,
            hash_locator,
            in_msg_locator,
        })
    }

    pub(super) fn payload_key(&self) -> TailPayloadKey {
        self.payload_key
    }

    pub(super) fn value_len(&self) -> usize {
        self.value.as_bytes().len()
    }

    pub(super) fn transaction_hash(&self) -> HashBytes {
        self.value.transaction_hash()
    }

    pub(super) fn in_msg_hash(&self) -> Option<HashBytes> {
        self.value.in_msg_hash()
    }

    pub(super) fn staged_write_bytes(&self) -> Result<u64> {
        let account = codec::encode_tail_account_locator(self.account_locator)?;
        let hash = codec::encode_tail_hash_locator(&self.hash_locator)?;
        let mut bytes = checked_staged_write_bytes(&[
            self.payload_key.len(),
            self.value.as_bytes().len(),
            self.payload_key.len(),
            account.len(),
            self.account_locator.transaction_hash.as_slice().len(),
            hash.len(),
        ])?;
        if let Some((in_msg_hash, locator)) = self.in_msg_locator {
            let in_msg = codec::encode_tail_in_msg_locator(locator)?;
            bytes = bytes
                .checked_add(checked_staged_write_bytes(&[
                    in_msg_hash.as_slice().len(),
                    in_msg.len(),
                ])?)
                .context("RPC tail staged write byte count overflow")?;
        }
        Ok(bytes)
    }
}

fn checked_staged_write_bytes(parts: &[usize]) -> Result<u64> {
    parts.iter().try_fold(0u64, |sum, part| {
        sum.checked_add(
            u64::try_from(*part).context("RPC tail staged write byte count overflow")?,
        )
        .context("RPC tail staged write byte count overflow")
    })
}

#[derive(Clone, Debug)]
pub(super) struct AccountTailDelta {
    account: AccountKey,
    promoted: Vec<TailPromotedTransaction>,
    retired: Vec<TailPayloadKey>,
}

impl AccountTailDelta {
    pub(super) fn new(
        account: AccountKey,
        promoted: Vec<TailPromotedTransaction>,
        retired: Vec<TailPayloadKey>,
    ) -> Result<Self> {
        ensure!(promoted.windows(2).all(|pair| pair[0].payload_key < pair[1].payload_key), "promoted RPC tail transactions must be strictly ordered");
        ensure!(retired.windows(2).all(|pair| pair[0] < pair[1]), "retired RPC tail transactions must be strictly ordered");
        let mut payload_keys = BTreeSet::new();
        for payload_key in promoted
            .iter()
            .map(|transaction| transaction.payload_key)
            .chain(retired.iter().copied())
        {
            let (payload_account, _) = codec::decode_tail_payload_key(&payload_key)?;
            ensure!(payload_account == account, "RPC tail delta contains a transaction for another account");
            ensure!(payload_keys.insert(payload_key), "RPC tail delta contains a duplicate transaction");
        }
        Ok(Self {
            account,
            promoted,
            retired,
        })
    }

    pub(super) fn account(&self) -> AccountKey {
        self.account
    }
}

fn derive_chunk_digest(
    expected_previous_progress: Option<TailGenerationProgress>,
    progress: TailGenerationProgress,
    deltas: &[AccountTailDelta],
) -> Result<HashBytes> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(TAIL_CHUNK_DIGEST_DOMAIN);
    hasher.update(&[TAIL_CHUNK_DIGEST_VERSION]);
    hasher.update(b"predecessor");
    match expected_previous_progress {
        Some(previous) => {
            hasher.update(&[1]);
            let encoded = codec::encode_tail_generation_progress(&previous)?;
            update_chunk_digest_len(&mut hasher, encoded.len())?;
            hasher.update(&encoded);
        }
        None => {
            hasher.update(&[0]);
            update_chunk_digest_len(&mut hasher, 0)?;
        }
    }
    hasher.update(b"chunk-header");
    let encoded = codec::encode_tail_generation_progress(&TailGenerationProgress {
        chunk_digest: codec::EMPTY_TAIL_CHUNK_DIGEST,
        ..progress
    })?;
    let header = &encoded[..encoded.len() - codec::EMPTY_TAIL_CHUNK_DIGEST.as_slice().len()];
    update_chunk_digest_len(&mut hasher, header.len())?;
    hasher.update(header);
    hasher.update(b"account-deltas");
    update_chunk_digest_len(&mut hasher, deltas.len())?;
    for delta in deltas {
        hasher.update(b"account");
        hasher.update(&delta.account);
        hasher.update(b"promoted");
        update_chunk_digest_len(&mut hasher, delta.promoted.len())?;
        for transaction in &delta.promoted {
            hasher.update(b"promotion");
            hasher.update(&transaction.payload_key);
            update_chunk_digest_len(&mut hasher, transaction.value.as_bytes().len())?;
            hasher.update(transaction.value.as_bytes());
            hasher.update(&codec::encode_tail_account_locator(transaction.account_locator)?);
            hasher.update(&codec::encode_tail_hash_locator(&transaction.hash_locator)?);
            match transaction.in_msg_locator {
                Some((hash, locator)) => {
                    hasher.update(&[1]);
                    hasher.update(hash.as_ref());
                    hasher.update(&codec::encode_tail_in_msg_locator(locator)?);
                }
                None => {
                    hasher.update(&[0]);
                }
            };
        }
        hasher.update(b"retired");
        update_chunk_digest_len(&mut hasher, delta.retired.len())?;
        for payload_key in &delta.retired {
            hasher.update(b"retirement");
            update_chunk_digest_len(&mut hasher, payload_key.len())?;
            hasher.update(payload_key);
        }
    }
    Ok(HashBytes::from_slice(hasher.finalize().as_bytes()))
}

fn update_chunk_digest_len(hasher: &mut blake3::Hasher, len: usize) -> Result<()> {
    hasher.update(&u64::try_from(len).context("RPC tail chunk digest length overflow")?.to_be_bytes());
    Ok(())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum StagedMutation {
    New,
    Existing,
}

impl TailStore {
    pub(super) fn append_chunk(
        &self,
        deltas: &[AccountTailDelta],
        expected_previous_progress: Option<TailGenerationProgress>,
        mut progress: TailGenerationProgress,
    ) -> Result<TailGenerationProgress> {
        ensure!(!progress.eof, "RPC tail chunk progress must not be EOF");
        ensure!(!deltas.is_empty(), "RPC tail chunk must contain at least one complete account");
        for pair in deltas.windows(2) {
            ensure!(pair[0].account < pair[1].account, "RPC tail chunk accounts must be strictly ordered");
        }
        ensure!(progress.cursor == TailProgressCursor::Account(deltas.last().unwrap().account), "RPC tail progress cursor does not match the last complete account");

        if let Some(previous) = expected_previous_progress {
            codec::encode_tail_generation_progress(&previous)?;
            ensure_progress_identity(previous, progress)?;
            ensure!(!previous.eof, "RPC tail generation is already complete");
            let TailProgressCursor::Account(previous_account) = previous.cursor else {
                anyhow::bail!("non-EOF RPC tail progress has an invalid Start cursor");
            };
            ensure!(previous_account < deltas[0].account, "RPC tail chunk does not advance the account cursor");
        }
        progress.chunk_digest = derive_chunk_digest(expected_previous_progress, progress, deltas)?;
        codec::encode_tail_generation_progress(&progress)?;

        let _guard = self.write_lock.lock();
        let snapshot = self.snapshot();
        ensure!(snapshot.generation_commit(progress.target_generation)?.is_none(), "RPC tail generation already has a terminal commit");
        let durable_progress = snapshot.generation_progress(progress.target_generation)?;
        let replay = durable_progress == Some(progress);
        // bind replay to the exact predecessor and full canonical chunk before inspecting staged data
        ensure!(replay || durable_progress == expected_previous_progress, "RPC tail durable progress differs from the expected chunk predecessor");

        let mut payload_keys = BTreeSet::new();
        let mut transaction_hashes = BTreeSet::new();
        let mut in_msg_hashes = BTreeSet::new();
        for delta in deltas {
            for transaction in &delta.promoted {
                ensure!(transaction.account_locator.born_generation == progress.target_generation, "promoted RPC tail transaction has the wrong born generation");
                ensure!(payload_keys.insert(transaction.payload_key), "RPC tail chunk contains a duplicate payload key");
                ensure!(transaction_hashes.insert(transaction.account_locator.transaction_hash), "RPC tail chunk contains a duplicate transaction hash");
                if let Some((hash, _)) = transaction.in_msg_locator {
                    ensure!(in_msg_hashes.insert(hash), "RPC tail chunk contains a duplicate inbound-message hash");
                }
            }
            for payload_key in &delta.retired {
                ensure!(payload_keys.insert(*payload_key), "RPC tail chunk promotes and retires the same payload key");
            }
        }

        let mut batch = rocksdb::WriteBatch::default();
        let mut delta_counters = TailGenerationCounters {
            processed_accounts: u64::try_from(deltas.len()).context("RPC tail account count overflow")?,
            ..Default::default()
        };
        for delta in deltas {
            for transaction in &delta.promoted {
                let state = self.stage_promotion(&snapshot, &mut batch, transaction)?;
                ensure!(state == if replay { StagedMutation::Existing } else { StagedMutation::New }, "RPC tail promotion conflicts with durable progress");
                delta_counters.promoted_records = delta_counters.promoted_records.checked_add(1).context("RPC tail promoted record count overflow")?;
                delta_counters.promoted_bytes = delta_counters.promoted_bytes
                    .checked_add(u64::try_from(transaction.value.as_bytes().len()).context("RPC tail promoted byte count overflow")?)
                    .context("RPC tail promoted byte count overflow")?;
            }
            for payload_key in &delta.retired {
                let (state, bytes) = self.stage_retirement(&snapshot, &mut batch, *payload_key, progress.target_generation)?;
                ensure!(state == if replay { StagedMutation::Existing } else { StagedMutation::New }, "RPC tail retirement conflicts with durable progress");
                delta_counters.retired_records = delta_counters.retired_records.checked_add(1).context("RPC tail retired record count overflow")?;
                delta_counters.retired_bytes = delta_counters.retired_bytes.checked_add(bytes).context("RPC tail retired byte count overflow")?;
            }
        }
        let expected_counters = checked_add_counters(
            expected_previous_progress.map(|progress| progress.counters).unwrap_or_default(),
            delta_counters,
        )?;
        ensure!(progress.counters == expected_counters, "RPC tail chunk counters do not match its complete account deltas");
        if replay {
            return Ok(progress);
        }
        batch.put_cf(
            &self.db.generation_progress.cf(),
            codec::tail_generation_progress_key(progress.target_generation)?,
            codec::encode_tail_generation_progress(&progress)?,
        );
        #[cfg(test)]
        if self.fail_next_append_write.swap(false, Ordering::AcqRel) {
            anyhow::bail!("injected RPC tail chunk write failure");
        }
        self.db
            .rocksdb()
            .write_opt(batch, self.db.transactions.write_config())
            .context("failed to atomically append RPC tail chunk")?;
        Ok(progress)
    }

    pub(super) fn finish_generation(
        &self,
        progress: TailGenerationProgress,
        commit: TailGenerationCommit,
    ) -> Result<()> {
        codec::encode_tail_generation_progress(&progress)?;
        codec::encode_tail_generation_commit(&commit)?;
        ensure!(progress.eof, "terminal RPC tail progress must be EOF");
        ensure!(commit.layout_version == TailLayoutVersion::MonolithicV1, "terminal RPC tail commit uses an incompatible layout");
        ensure!(progress.target_generation == commit.target_generation
            && progress.operation_id == commit.operation_id
            && progress.source_partition_id == commit.source_partition_id
            && progress.source_manifest_digest == commit.source_manifest_digest
            && progress.retention_policy_digest == commit.retention_policy_digest
            && progress.counters == commit.counters,
            "terminal RPC tail progress and commit identities differ");

        let _guard = self.write_lock.lock();
        let snapshot = self.snapshot();
        let previous_progress = snapshot.generation_progress(progress.target_generation)?;
        let previous_commit = snapshot.generation_commit(progress.target_generation)?;
        if let Some(previous_commit) = previous_commit {
            ensure!(previous_commit == commit && previous_progress == Some(progress), "conflicting terminal RPC tail generation state");
            return Ok(());
        }
        match previous_progress {
            Some(previous) => {
                ensure_progress_identity(previous, progress)?;
                ensure!(!previous.eof, "terminal RPC tail progress exists without its atomic commit");
                ensure!(previous.cursor == progress.cursor && previous.counters == progress.counters && previous.chunk_digest == progress.chunk_digest,
                    "terminal RPC tail progress does not follow the last committed chunk");
            }
            None => {
                ensure!(progress.cursor == TailProgressCursor::Start && progress.counters == TailGenerationCounters::default(), "non-empty RPC tail generation is missing authoritative progress");
            }
        }
        let mut batch = rocksdb::WriteBatch::default();
        batch.put_cf(
            &self.db.generation_progress.cf(),
            codec::tail_generation_progress_key(progress.target_generation)?,
            codec::encode_tail_generation_progress(&progress)?,
        );
        batch.put_cf(
            &self.db.generation_commits.cf(),
            codec::tail_generation_commit_key(commit.target_generation)?,
            codec::encode_tail_generation_commit(&commit)?,
        );
        self.db
            .rocksdb()
            .write_opt(batch, self.db.generation_commits.write_config())
            .context("failed to atomically commit RPC tail generation")
    }

    pub(super) fn sweep_retired(
        &self,
        published_visible_generation: u64,
        max_records: usize,
    ) -> Result<TailSweepResult> {
        ensure!(max_records > 0, "RPC tail sweep record limit must be positive");
        #[cfg(test)]
        {
            self.sweep_attempts.fetch_add(1, Ordering::AcqRel);
            self.sweep_attempted.notify_one();
        }
        let max_records = max_records.min(TAIL_SWEEP_MAX_RECORDS);
        let _guard = self.write_lock.lock();
        let mut generation_state = self.generation_tracker.state.lock();
        generation_state.minimum_request_generation = generation_state
            .minimum_request_generation
            .max(published_visible_generation);
        #[cfg(test)]
        if let Some(gate) = self.sweep_floor_gate.lock().clone() {
            gate.entered.wait();
            gate.release.wait();
        }
        let oldest_live_generation = generation_state
            .counts
            .first_key_value()
            .map(|(&generation, _)| generation);
        let generation_metrics = TailGenerationTracker::metrics(&generation_state);
        TailGenerationTracker::record_metrics(generation_metrics);
        drop(generation_state);
        let snapshot = self.snapshot();
        let table = &self.db.retired_by_generation;
        let mut read_options = table.new_read_config();
        read_options.set_snapshot(&snapshot.snapshot);
        let mut iterator = self
            .db
            .rocksdb()
            .raw_iterator_cf_opt(&table.cf(), read_options);
        iterator.seek_to_first();
        let mut certified_generations = BTreeSet::new();
        let mut deletes = Vec::with_capacity(max_records);
        let next = loop {
            let Some((key, value)) = iterator.item() else {
                break TailSweepNext::Empty;
            };
            let (dead_generation, payload_key) = codec::decode_retired_transaction_key(key)
                .map_err(|_| malformed_authoritative_error(
                    "malformed committed RPC tail retirement queue key",
                ))?;
            if !value.is_empty() {
                return Err(malformed_authoritative_error(
                    "malformed committed RPC tail retirement queue value",
                ));
            }
            if dead_generation > published_visible_generation {
                break TailSweepNext::AwaitPublishedGeneration { dead_generation };
            }
            if let Some(oldest_live_generation) = oldest_live_generation
                && oldest_live_generation < dead_generation
            {
                break TailSweepNext::AwaitRequestGeneration {
                    dead_generation,
                    oldest_live_generation,
                };
            }
            if deletes.len() == max_records {
                break TailSweepNext::Ready;
            }
            if certified_generations.insert(dead_generation) {
                self.load_sweep_generation_commit(&snapshot, dead_generation)?;
            }
            let entry = TailRetirementEntry {
                dead_generation,
                payload_key,
            };
            deletes.push(self.load_sweep_delete(&snapshot, entry)?);
            iterator.next();
        };
        iterator.status()?;
        if deletes.is_empty() {
            return Ok(TailSweepResult {
                swept_records: 0,
                oldest_swept_generation: None,
                newest_swept_generation: None,
                next,
            });
        }
        let oldest_swept_generation = deletes.first().map(|delete| delete.entry.dead_generation);
        let newest_swept_generation = deletes.last().map(|delete| delete.entry.dead_generation);
        let mut batch = rocksdb::WriteBatch::default();
        for delete in &deletes {
            batch.delete_cf(&self.db.transactions.cf(), delete.entry.payload_key);
            batch.delete_cf(&self.db.transactions_by_account.cf(), delete.entry.payload_key);
            batch.delete_cf(&self.db.transactions_by_hash.cf(), delete.transaction_hash);
            if let Some(in_msg_hash) = delete.in_msg_hash {
                batch.delete_cf(&self.db.transactions_by_in_msg.cf(), in_msg_hash);
            }
            batch.delete_cf(
                &self.db.retired_by_generation.cf(),
                codec::retired_transaction_key(
                    delete.entry.dead_generation,
                    delete.entry.payload_key,
                )?,
            );
        }
        #[cfg(test)]
        if self.fail_next_sweep_write.swap(false, Ordering::AcqRel) {
            anyhow::bail!("injected transient RPC tail sweep write failure");
        }
        self.db
            .rocksdb()
            .write_opt(batch, self.db.transactions.write_config())
            .context("failed to atomically sweep retired RPC tail transactions")?;
        #[cfg(test)]
        {
            self.sweep_writes.fetch_add(1, Ordering::AcqRel);
            self.sweep_written.notify_one();
        }
        metrics::counter!("tycho_storage_rpc_tail_sweep_records_total")
            .increment(deletes.len() as u64);
        metrics::counter!("tycho_storage_rpc_tail_sweep_batches_total").increment(1);
        for delete in &deletes {
            metrics::histogram!("tycho_storage_rpc_tail_sweep_generation_lag")
                .record(published_visible_generation.saturating_sub(delete.entry.dead_generation) as f64);
        }
        Ok(TailSweepResult {
            swept_records: deletes.len(),
            oldest_swept_generation,
            newest_swept_generation,
            next,
        })
    }

    pub(super) fn delete_obsolete_progress(
        &self,
        published_visible_generation: u64,
        target_generation: u64,
    ) -> Result<bool> {
        let progress_key = codec::tail_generation_progress_key(target_generation)?;
        ensure!(target_generation <= published_visible_generation,
            "RPC tail progress is not obsolete before its generation is published");
        let _guard = self.write_lock.lock();
        let snapshot = self.snapshot();
        self.load_sweep_generation_commit(&snapshot, target_generation)?;
        let Some(_) = snapshot_get(
            &self.db,
            &snapshot.snapshot,
            &self.db.generation_progress,
            &progress_key,
        )? else {
            return Ok(false);
        };
        let mut batch = rocksdb::WriteBatch::default();
        batch.delete_cf(&self.db.generation_progress.cf(), progress_key);
        self.db
            .rocksdb()
            .write_opt(batch, self.db.generation_progress.write_config())
            .context("failed to delete obsolete RPC tail generation progress")?;
        Ok(true)
    }

    fn load_sweep_generation_commit(
        &self,
        snapshot: &TailSnapshot,
        target_generation: u64,
    ) -> Result<TailGenerationCommit> {
        let key = codec::tail_generation_commit_key(target_generation)?;
        let bytes = snapshot_get(
            &self.db,
            &snapshot.snapshot,
            &self.db.generation_commits,
            &key,
        )?
        .ok_or_else(|| missing_authoritative_error(
            "published RPC tail generation is missing its terminal commit",
        ))?;
        let commit = codec::decode_tail_generation_commit(&bytes)
            .map_err(|_| malformed_authoritative_error(
                "malformed committed RPC tail generation commit",
            ))?;
        if commit.target_generation != target_generation {
            return Err(conflicting_authoritative_error(
                "RPC tail generation commit key and value generations differ",
            ));
        }
        Ok(commit)
    }

    fn load_sweep_delete(
        &self,
        snapshot: &TailSnapshot,
        entry: TailRetirementEntry,
    ) -> Result<TailSweepDelete> {
        let payload_bytes = snapshot_get(
            &self.db,
            &snapshot.snapshot,
            &self.db.transactions,
            &entry.payload_key,
        )?
        .ok_or_else(|| missing_authoritative_error(
            "retired RPC tail transaction payload is missing",
        ))?;
        let value = TailTransactionValue::decode(payload_bytes)
            .map_err(|_| malformed_authoritative_error(
                "retired RPC tail transaction payload is malformed",
            ))?;
        let account_bytes = snapshot_get(
            &self.db,
            &snapshot.snapshot,
            &self.db.transactions_by_account,
            &entry.payload_key,
        )?
        .ok_or_else(|| missing_authoritative_error(
            "retired RPC tail transaction account locator is missing",
        ))?;
        let account = codec::decode_tail_account_locator(&account_bytes)
            .map_err(|_| malformed_authoritative_error(
                "retired RPC tail transaction account locator is malformed",
            ))?;
        if account.transaction_hash != value.transaction_hash()
            || account.dead_generation != Some(entry.dead_generation)
        {
            return Err(conflicting_authoritative_error(
                "retired RPC tail transaction account locator conflicts with its payload or queue entry",
            ));
        }
        let hash_bytes = snapshot_get(
            &self.db,
            &snapshot.snapshot,
            &self.db.transactions_by_hash,
            account.transaction_hash.as_slice(),
        )?
        .ok_or_else(|| missing_authoritative_error(
            "retired RPC tail transaction hash locator is missing",
        ))?;
        let hash = codec::decode_tail_hash_locator(&hash_bytes)
            .map_err(|_| malformed_authoritative_error(
                "retired RPC tail transaction hash locator is malformed",
            ))?;
        // Every directory identity must match before one atomic delete batch is created.
        if hash.payload_key != entry.payload_key
            || hash.mc_seqno != value.related_mc_seqno()
            || hash.born_generation != account.born_generation
            || hash.dead_generation != account.dead_generation
        {
            return Err(conflicting_authoritative_error(
                "retired RPC tail transaction hash locator conflicts with its payload or account locator",
            ));
        }
        let in_msg_hash = match value.in_msg_hash() {
            Some(in_msg_hash) => {
                let locator_bytes = snapshot_get(
                    &self.db,
                    &snapshot.snapshot,
                    &self.db.transactions_by_in_msg,
                    in_msg_hash.as_slice(),
                )?
                .ok_or_else(|| missing_authoritative_error(
                    "retired RPC tail transaction inbound-message locator is missing",
                ))?;
                let locator = codec::decode_tail_in_msg_locator(&locator_bytes)
                    .map_err(|_| malformed_authoritative_error(
                        "retired RPC tail transaction inbound-message locator is malformed",
                    ))?;
                if locator.payload_key != entry.payload_key
                    || locator.born_generation != account.born_generation
                    || locator.dead_generation != account.dead_generation
                {
                    return Err(conflicting_authoritative_error(
                        "retired RPC tail transaction inbound-message locator conflicts with its payload or account locator",
                    ));
                }
                Some(in_msg_hash)
            }
            None => None,
        };
        Ok(TailSweepDelete {
            entry,
            transaction_hash: account.transaction_hash,
            in_msg_hash,
        })
    }

    #[cfg(test)]
    pub(super) fn inject_next_sweep_write_failure(&self) {
        self.fail_next_sweep_write.store(true, Ordering::Release);
    }

    #[cfg(test)]
    pub(super) fn inject_next_append_write_failure(&self) {
        self.fail_next_append_write.store(true, Ordering::Release);
    }

    #[cfg(test)]
    pub(super) fn sweep_attempts(&self) -> u64 {
        self.sweep_attempts.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(super) async fn wait_for_sweep_attempts(&self, expected: u64) {
        loop {
            let attempted = self.sweep_attempted.notified();
            if self.sweep_attempts() >= expected {
                return;
            }
            attempted.await;
        }
    }

    #[cfg(test)]
    pub(super) async fn wait_for_sweep_writes(&self, expected: u64) {
        loop {
            let written = self.sweep_written.notified();
            if self.sweep_writes.load(Ordering::Acquire) >= expected {
                return;
            }
            written.await;
        }
    }

    #[cfg(test)]
    pub(super) fn retirement_entry_exists(
        &self,
        dead_generation: u64,
        payload_key: TailPayloadKey,
    ) -> Result<bool> {
        let key = codec::retired_transaction_key(dead_generation, payload_key)?;
        Ok(self.db.retired_by_generation.get(key)?.is_some())
    }

    #[cfg(test)]
    pub(super) fn remove_transaction_payload(&self, payload_key: TailPayloadKey) -> Result<()> {
        self.db.transactions.remove(payload_key).map_err(Into::into)
    }

    #[cfg(test)]
    pub(super) fn remove_transaction_hash_locator(&self, payload_key: TailPayloadKey) -> Result<()> {
        let transaction_hash = self
            .snapshot()
            .transaction_record(payload_key)?
            .context("test RPC tail transaction is missing")?
            .account_locator
            .transaction_hash;
        self.db.transactions_by_hash.remove(transaction_hash).map_err(Into::into)
    }

    #[cfg(test)]
    pub(super) fn remove_generation_commit(&self, target_generation: u64) -> Result<()> {
        let key = codec::tail_generation_commit_key(target_generation)?;
        self.db.generation_commits.remove(key).map_err(Into::into)
    }

    #[cfg(test)]
    pub(super) fn replace_generation_progress(
        &self,
        target_generation: u64,
        value: &[u8],
    ) -> Result<()> {
        let key = codec::tail_generation_progress_key(target_generation)?;
        self.db.generation_progress.insert(key, value).map_err(Into::into)
    }

    #[cfg(test)]
    fn set_sweep_floor_gate(&self, gate: Option<Arc<TailSweepFloorGate>>) {
        *self.sweep_floor_gate.lock() = gate;
    }

    #[cfg(test)]
    fn set_before_pin_gate(&self, gate: Option<Arc<TailPinGate>>) {
        *self.generation_tracker.before_pin_gate.lock() = gate;
    }

    fn stage_promotion(
        &self,
        snapshot: &TailSnapshot,
        batch: &mut rocksdb::WriteBatch,
        transaction: &TailPromotedTransaction,
    ) -> Result<StagedMutation> {
        let account_value = codec::encode_tail_account_locator(transaction.account_locator)?;
        let hash_value = codec::encode_tail_hash_locator(&transaction.hash_locator)?;
        let payload = snapshot_get(&self.db, &snapshot.snapshot, &self.db.transactions, &transaction.payload_key)?;
        let account = snapshot_get(&self.db, &snapshot.snapshot, &self.db.transactions_by_account, &transaction.payload_key)?;
        let hash = snapshot_get(&self.db, &snapshot.snapshot, &self.db.transactions_by_hash, transaction.account_locator.transaction_hash.as_slice())?;
        let in_msg = match transaction.in_msg_locator {
            Some((in_msg_hash, locator)) => Some((
                in_msg_hash,
                codec::encode_tail_in_msg_locator(locator)?,
                snapshot_get(&self.db, &snapshot.snapshot, &self.db.transactions_by_in_msg, in_msg_hash.as_slice())?,
            )),
            None => None,
        };
        let all_missing = payload.is_none() && account.is_none() && hash.is_none()
            && in_msg.as_ref().is_none_or(|(_, _, value)| value.is_none());
        let all_identical = payload.as_deref() == Some(transaction.value.as_bytes())
            && account.as_deref() == Some(account_value.as_slice())
            && hash.as_deref() == Some(hash_value.as_slice())
            && in_msg.as_ref().is_none_or(|(_, expected, value)| value.as_deref() == Some(expected.as_slice()));
        ensure!(all_missing || all_identical, "conflicting or partial RPC tail promotion already exists");
        if all_identical {
            return Ok(StagedMutation::Existing);
        }
        batch.put_cf(&self.db.transactions.cf(), transaction.payload_key, transaction.value.as_bytes());
        batch.put_cf(&self.db.transactions_by_account.cf(), transaction.payload_key, account_value);
        batch.put_cf(&self.db.transactions_by_hash.cf(), transaction.account_locator.transaction_hash, hash_value);
        if let Some((in_msg_hash, value, _)) = in_msg {
            batch.put_cf(&self.db.transactions_by_in_msg.cf(), in_msg_hash, value);
        }
        Ok(StagedMutation::New)
    }

    fn stage_retirement(
        &self,
        snapshot: &TailSnapshot,
        batch: &mut rocksdb::WriteBatch,
        payload_key: TailPayloadKey,
        dead_generation: u64,
    ) -> Result<(StagedMutation, u64)> {
        let record = snapshot
            .transaction_record(payload_key)?
            .context("retired RPC tail transaction is missing")?;
        let payload = record.value;
        let account = record.account_locator;
        let hash = record.hash_locator;
        let in_msg = record.in_msg_locator;
        ensure!(dead_generation > account.born_generation, "RPC tail retirement generation must follow the born generation");
        let retirement_key = codec::retired_transaction_key(dead_generation, payload_key)?;
        let retirement_value = snapshot_get(&self.db, &snapshot.snapshot, &self.db.retired_by_generation, &retirement_key)?;
        if let Some(value) = &retirement_value {
            ensure!(value.is_empty(), "RPC tail retirement queue value must be empty");
        }
        let existing = account.dead_generation == Some(dead_generation)
            && hash.dead_generation == Some(dead_generation)
            && in_msg.as_ref().is_none_or(|(_, locator)| locator.dead_generation == Some(dead_generation))
            && retirement_value.is_some();
        let new = account.dead_generation.is_none()
            && hash.dead_generation.is_none()
            && in_msg.as_ref().is_none_or(|(_, locator)| locator.dead_generation.is_none())
            && retirement_value.is_none();
        ensure!(existing || new, "conflicting or partial RPC tail retirement already exists");
        let payload_len = u64::try_from(payload.as_bytes().len()).context("RPC tail retired byte count overflow")?;
        if existing {
            return Ok((StagedMutation::Existing, payload_len));
        }
        batch.put_cf(
            &self.db.transactions_by_account.cf(),
            payload_key,
            codec::encode_tail_account_locator(TailAccountLocator { dead_generation: Some(dead_generation), ..account })?,
        );
        batch.put_cf(
            &self.db.transactions_by_hash.cf(),
            account.transaction_hash,
            codec::encode_tail_hash_locator(&TailHashLocator { dead_generation: Some(dead_generation), ..hash })?,
        );
        if let Some((in_msg_hash, locator)) = in_msg {
            batch.put_cf(
                &self.db.transactions_by_in_msg.cf(),
                in_msg_hash,
                codec::encode_tail_in_msg_locator(TailInMsgLocator { dead_generation: Some(dead_generation), ..locator })?,
            );
        }
        batch.put_cf(&self.db.retired_by_generation.cf(), retirement_key, []);
        Ok((StagedMutation::New, payload_len))
    }
}

fn ensure_progress_identity(
    previous: TailGenerationProgress,
    next: TailGenerationProgress,
) -> Result<()> {
    ensure!(previous.target_generation == next.target_generation
        && previous.operation_id == next.operation_id
        && previous.source_partition_id == next.source_partition_id
        && previous.source_manifest_digest == next.source_manifest_digest
        && previous.retention_policy_digest == next.retention_policy_digest,
        "RPC tail progress identity changed within one generation");
    Ok(())
}

fn checked_add_counters(
    left: TailGenerationCounters,
    right: TailGenerationCounters,
) -> Result<TailGenerationCounters> {
    Ok(TailGenerationCounters {
        processed_accounts: left.processed_accounts.checked_add(right.processed_accounts).context("RPC tail processed-account count overflow")?,
        promoted_records: left.promoted_records.checked_add(right.promoted_records).context("RPC tail promoted record count overflow")?,
        promoted_bytes: left.promoted_bytes.checked_add(right.promoted_bytes).context("RPC tail promoted byte count overflow")?,
        retired_records: left.retired_records.checked_add(right.retired_records).context("RPC tail retired record count overflow")?,
        retired_bytes: left.retired_bytes.checked_add(right.retired_bytes).context("RPC tail retired byte count overflow")?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::partition::tests::TestMetricsRecorder;
    use super::super::tables;
    use tycho_storage::kv::{InstanceId, NamedTables, TableContext};
    use tycho_types::models::ShardIdent;

    weedb::tables! {
        struct IncompleteRpcTailTables<TableContext> {
            transactions_by_account: tables::TailTransactionsByAccount,
            transactions_by_hash: tables::TailTransactionsByHash,
            transactions_by_in_msg: tables::TailTransactionsByInMsg,
            retired_by_generation: tables::TailRetiredByGeneration,
            generation_progress: tables::TailGenerationProgress,
            generation_commits: tables::TailGenerationCommits,
        }
    }

    impl NamedTables for IncompleteRpcTailTables {
        const NAME: &'static str = "rpc-tail-incomplete-test";
    }

    fn identity(byte: u8) -> TailIdentity {
        TailIdentity {
            layout_version: TailLayoutVersion::MonolithicV1,
            node_instance_id: InstanceId([byte; 16]),
        }
    }

    fn account_key(byte: u8) -> AccountKey {
        let mut account = [byte; codec::ACCOUNT_KEY_LEN];
        account[0] = 0;
        account
    }

    fn promotion(
        account: AccountKey,
        lt: u64,
        born_generation: u64,
        byte: u8,
    ) -> TailPromotedTransaction {
        promotion_with_mc_seqno(account, lt, born_generation, byte, 7)
    }

    fn promotion_with_mc_seqno(
        account: AccountKey,
        lt: u64,
        born_generation: u64,
        byte: u8,
        mc_seqno: u32,
    ) -> TailPromotedTransaction {
        let mut payload = Vec::with_capacity(66);
        payload.push(TransactionMask::HAS_MSG_HASH.bits());
        payload.extend_from_slice(&[byte; 32]);
        payload.extend_from_slice(&[byte.wrapping_add(1); 32]);
        payload.push(0xb5);
        TailPromotedTransaction::new(
            codec::tail_payload_key(account, lt),
            codec::encode_transaction_value(mc_seqno, &payload).unwrap(),
            BlockId {
                shard: ShardIdent::BASECHAIN,
                seqno: 5,
                root_hash: HashBytes([3; 32]),
                file_hash: HashBytes([4; 32]),
            },
            born_generation,
        )
        .unwrap()
    }

    #[derive(Clone, Copy)]
    struct SweepRecordIdentity {
        payload_key: TailPayloadKey,
        transaction_hash: HashBytes,
        in_msg_hash: Option<HashBytes>,
    }

    fn sweep_promotion(
        account: AccountKey,
        lt: u64,
        with_in_msg: bool,
    ) -> TailPromotedTransaction {
        let mut transaction_hash = [0x91; 32];
        transaction_hash[24..].copy_from_slice(&lt.to_be_bytes());
        let mut payload = Vec::with_capacity(66);
        payload.push(if with_in_msg {
            TransactionMask::HAS_MSG_HASH.bits()
        } else {
            0
        });
        payload.extend_from_slice(&transaction_hash);
        if with_in_msg {
            let mut in_msg_hash = [0xa1; 32];
            in_msg_hash[24..].copy_from_slice(&lt.to_be_bytes());
            payload.extend_from_slice(&in_msg_hash);
        }
        payload.push(0xb5);
        TailPromotedTransaction::new(
            codec::tail_payload_key(account, lt),
            codec::encode_transaction_value(7, &payload).unwrap(),
            BlockId {
                shard: ShardIdent::BASECHAIN,
                seqno: 5,
                root_hash: HashBytes([3; 32]),
                file_hash: HashBytes([4; 32]),
            },
            1,
        )
        .unwrap()
    }

    fn stage_retired_transactions(
        store: &TailStore,
        count: usize,
        with_in_msg: impl Fn(usize) -> bool,
    ) -> Vec<SweepRecordIdentity> {
        assert!(count > 0);
        let account = account_key(9);
        let promoted = (1..=count)
            .map(|index| sweep_promotion(account, index as u64, with_in_msg(index)))
            .collect::<Vec<_>>();
        let identities = promoted
            .iter()
            .map(|transaction| SweepRecordIdentity {
                payload_key: transaction.payload_key,
                transaction_hash: transaction.account_locator.transaction_hash,
                in_msg_hash: transaction.in_msg_locator.map(|(hash, _)| hash),
            })
            .collect::<Vec<_>>();
        let promoted_bytes: u64 = promoted
            .iter()
            .map(|transaction| transaction.value.as_bytes().len() as u64)
            .sum();
        let promotion_progress = progress(
            1,
            80,
            TailProgressCursor::Account(account),
            false,
            TailGenerationCounters {
                processed_accounts: 1,
                promoted_records: count as u64,
                promoted_bytes,
                ..Default::default()
            },
        );
        store
            .append_chunk(
                &[AccountTailDelta::new(account, promoted, vec![]).unwrap()],
                None,
                promotion_progress,
            )
            .unwrap();
        finish_started_generation(store, promotion_progress);
        let retirement_progress = progress(
            2,
            81,
            TailProgressCursor::Account(account),
            false,
            TailGenerationCounters {
                processed_accounts: 1,
                retired_records: count as u64,
                retired_bytes: promoted_bytes,
                ..Default::default()
            },
        );
        store
            .append_chunk(
                &[AccountTailDelta::new(
                    account,
                    vec![],
                    identities.iter().map(|record| record.payload_key).collect(),
                )
                .unwrap()],
                None,
                retirement_progress,
            )
            .unwrap();
        finish_started_generation(store, retirement_progress);
        identities
    }

    fn progress(
        target_generation: u64,
        operation_id: u128,
        cursor: TailProgressCursor,
        eof: bool,
        counters: TailGenerationCounters,
    ) -> TailGenerationProgress {
        TailGenerationProgress {
            target_generation,
            operation_id,
            source_partition_id: 1,
            source_manifest_digest: HashBytes([5; 32]),
            retention_policy_digest: HashBytes([6; 32]),
            cursor,
            eof,
            counters,
            chunk_digest: codec::EMPTY_TAIL_CHUNK_DIGEST,
        }
    }

    fn commit(progress: TailGenerationProgress) -> TailGenerationCommit {
        TailGenerationCommit {
            layout_version: TailLayoutVersion::MonolithicV1,
            target_generation: progress.target_generation,
            operation_id: progress.operation_id,
            source_partition_id: progress.source_partition_id,
            source_manifest_digest: progress.source_manifest_digest,
            previous_visible_generation: progress.target_generation - 1,
            cutoff_utime: 10,
            keep_tx_per_account: 2,
            retention_policy_digest: progress.retention_policy_digest,
            counters: progress.counters,
        }
    }

    fn gc_intent(phase: GcIntentPhase, progress: TailGenerationProgress) -> GcIntent {
        GcIntent {
            phase,
            operation_id: progress.operation_id,
            source_partition_id: progress.source_partition_id,
            source_manifest_digest: progress.source_manifest_digest,
            target_generation: progress.target_generation,
            previous_visible_generation: progress.target_generation - 1,
            cutoff_utime: 10,
            keep_tx_per_account: 2,
            retention_policy_digest: progress.retention_policy_digest,
        }
    }

    fn finish_empty_generation(
        store: &TailStore,
        target_generation: u64,
        operation_id: u128,
    ) -> TailGenerationCommit {
        let progress = progress(
            target_generation,
            operation_id,
            TailProgressCursor::Start,
            true,
            TailGenerationCounters::default(),
        );
        let commit = commit(progress);
        store.finish_generation(progress, commit).unwrap();
        commit
    }

    fn finish_started_generation(
        store: &TailStore,
        progress: TailGenerationProgress,
    ) -> TailGenerationCommit {
        let progress = store.generation_progress(progress.target_generation).unwrap().unwrap();
        let progress = TailGenerationProgress { eof: true, ..progress };
        let commit = commit(progress);
        store.finish_generation(progress, commit).unwrap();
        commit
    }

    fn visible_account_lts(
        snapshot: &TailRequestSnapshot,
        account: AccountKey,
        start_lt: u64,
        end_lt: u64,
        reverse: bool,
        max_mc_seqno: u32,
    ) -> Vec<u64> {
        let mut result = Vec::new();
        let mut cursor = None;
        loop {
            let Some(record) = snapshot
                .next_account_transaction(
                    account,
                    start_lt,
                    end_lt,
                    reverse,
                    cursor,
                    max_mc_seqno,
                )
                .unwrap()
            else {
                break;
            };
            let (_, lt) = codec::decode_tail_payload_key(&record.payload_key).unwrap();
            result.push(lt);
            cursor = Some(lt);
        }
        result
    }

    fn record_lts(records: &[TailTransactionRecord]) -> Vec<u64> {
        records
            .iter()
            .map(|record| codec::decode_tail_payload_key(&record.payload_key).unwrap().1)
            .collect()
    }

    #[tokio::test]
    async fn bootstrap_is_strict_idempotent_and_registered_with_exact_schema() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let expected = identity(1);
        let store = TailStore::open(&context, expected, 0).unwrap();
        assert_eq!(
            store.db().raw().cf_names(),
            [
                "transactions",
                "transactions_by_account",
                "transactions_by_hash",
                "transactions_by_in_msg",
                "retired_by_generation",
                "generation_progress",
                "generation_commits",
            ]
        );
        // rpc-tail registration exposes fixed per-CF live records/bytes, tombstone, compaction, and BlobDB metrics
        assert!(context.rocksdb_instance_is_registered(RpcTailTables::NAME, store.db().raw()));
        assert!(codec::decode_tail_identity(
                store
                    .db()
                    .generation_commits
                    .get(codec::tail_identity_key())
                    .unwrap()
                    .unwrap()
                    .as_ref(),
            )
            .unwrap()
            == expected);
        drop(store);
        drop(TailStore::open(&context, expected, 0).unwrap());
        assert!(TailStore::open(&context, identity(2), 0).is_err());

        let (missing_context, _missing_tmp) = StorageContext::new_temp().await.unwrap();
        let missing_path = missing_context.root_dir().path().join(TAIL_SUBDIR);
        assert!(TailStore::open(&missing_context, expected, 1).is_err());
        assert!(!missing_path.exists());

        let (empty_context, _empty_tmp) = StorageContext::new_temp().await.unwrap();
        let empty: RpcTailDb = empty_context.open_preconfigured(TAIL_SUBDIR).unwrap();
        drop(empty);
        assert!(TailStore::open(&empty_context, expected, 1).is_err());

        let (dirty_context, _dirty_tmp) = StorageContext::new_temp().await.unwrap();
        let dirty: RpcTailDb = dirty_context.open_preconfigured(TAIL_SUBDIR).unwrap();
        dirty.transactions.insert([1; codec::TAIL_PAYLOAD_KEY_LEN], [1]).unwrap();
        drop(dirty);
        assert!(TailStore::open(&dirty_context, expected, 0).is_err());
        let dirty: RpcTailDb = dirty_context.open_preconfigured(TAIL_SUBDIR).unwrap();
        assert!(dirty.generation_commits.get(codec::tail_identity_key()).unwrap().is_none());
    }

    #[tokio::test]
    async fn generation_zero_request_snapshot_is_empty_without_a_commit() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let store = TailStore::open(&context, identity(18), 0).unwrap();
        let account = account_key(1);
        let promoted = promotion(account, 18, 1, 0x18);
        let transaction_hash = promoted.account_locator.transaction_hash;
        let in_msg_hash = promoted.in_msg_locator.unwrap().0;
        let promoted_bytes = promoted.value.as_bytes().len() as u64;
        store
            .append_chunk(
                &[AccountTailDelta::new(account, vec![promoted], vec![]).unwrap()],
                None,
                progress(
                    1,
                    18,
                    TailProgressCursor::Account(account),
                    false,
                    TailGenerationCounters {
                        processed_accounts: 1,
                        promoted_records: 1,
                        promoted_bytes,
                        ..Default::default()
                    },
                ),
            )
            .unwrap();
        assert!(store.generation_commit(1).unwrap().is_none());

        let snapshot = store
            .request_snapshot(TailLayoutVersion::MonolithicV1, 0)
            .unwrap();
        assert!(snapshot
            .transaction_by_hash(&transaction_hash, u32::MAX)
            .unwrap()
            .is_none());
        assert!(snapshot
            .transaction_by_in_msg(&in_msg_hash, u32::MAX)
            .unwrap()
            .is_none());
        assert!(snapshot
            .newest_account_transactions(account, None, 1, u32::MAX)
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn generation_zero_reopen_rejects_missing_column_family_with_durable_progress() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let expected = identity(2);
        let incomplete = context
            .open_preconfigured::<_, IncompleteRpcTailTables>(TAIL_SUBDIR)
            .unwrap();
        incomplete
            .generation_commits
            .insert(codec::tail_identity_key(), codec::encode_tail_identity(expected))
            .unwrap();
        let account = account_key(1);
        let durable_progress = progress(
            1,
            9,
            TailProgressCursor::Account(account),
            false,
            TailGenerationCounters {
                processed_accounts: 1,
                ..Default::default()
            },
        );
        incomplete
            .generation_progress
            .insert(
                codec::tail_generation_progress_key(1).unwrap(),
                codec::encode_tail_generation_progress(&durable_progress).unwrap(),
            )
            .unwrap();
        drop(incomplete);

        assert!(TailStore::open(&context, expected, 0).is_err());
    }

    #[tokio::test]
    async fn request_snapshots_pin_generations_until_the_last_owner_drops() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let expected = identity(7);
        let store = TailStore::open(&context, expected, 0).unwrap();
        let raw_snapshot = store.snapshot();
        assert_eq!(store.oldest_live_generation(), None);
        assert!(store.can_sweep_generation(1));
        assert!(store.request_snapshot(TailLayoutVersion::MonolithicV1, 1).is_err());
        assert_eq!(store.oldest_live_generation(), None);

        let first_commit = finish_empty_generation(&store, 1, 50);
        let second_commit = finish_empty_generation(&store, 2, 51);
        assert_eq!(store.oldest_live_generation(), None);

        let generation_two = store
            .request_snapshot(TailLayoutVersion::MonolithicV1, 2)
            .unwrap();
        assert_eq!(generation_two.layout_version(), TailLayoutVersion::MonolithicV1);
        assert_eq!(generation_two.visible_generation(), 2);
        assert_eq!(generation_two.snapshot().generation_commit(2).unwrap(), Some(second_commit));
        assert_eq!(store.oldest_live_generation(), Some(2));
        assert!(store.can_sweep_generation(2));
        assert!(!store.can_sweep_generation(3));

        let generation_one_first = store
            .request_snapshot(TailLayoutVersion::MonolithicV1, 1)
            .unwrap();
        let generation_one_second = generation_one_first.clone();
        assert_eq!(generation_one_first.snapshot().generation_commit(1).unwrap(), Some(first_commit));
        assert_eq!(store.oldest_live_generation(), Some(1));
        assert!(store.can_sweep_generation(1));
        assert!(!store.can_sweep_generation(2));

        drop(generation_one_first);
        assert_eq!(store.oldest_live_generation(), Some(1));
        let released = store.generation_release_notify().notified();
        drop(generation_one_second);
        tokio::time::timeout(std::time::Duration::from_secs(1), released)
            .await
            .unwrap();
        assert_eq!(store.oldest_live_generation(), Some(2));
        drop(generation_two);
        assert_eq!(store.oldest_live_generation(), None);
        assert!(store.can_sweep_generation(2));
        drop(raw_snapshot);
        drop(store);

        let reopened = TailStore::open(&context, expected, 2).unwrap();
        assert!(reopened.request_snapshot(TailLayoutVersion::MonolithicV1, 1).is_err());
        assert_eq!(reopened.oldest_live_generation(), None);
        drop(reopened
            .request_snapshot(TailLayoutVersion::MonolithicV1, 2)
            .unwrap());
        assert_eq!(reopened.oldest_live_generation(), None);
    }

    #[tokio::test]
    async fn generation_release_notifies_only_when_the_oldest_boundary_changes() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let store = TailStore::open(&context, identity(14), 0).unwrap();
        finish_empty_generation(&store, 1, 52);
        let first = store
            .request_snapshot(TailLayoutVersion::MonolithicV1, 1)
            .unwrap();
        let replacement = store
            .request_snapshot(TailLayoutVersion::MonolithicV1, 1)
            .unwrap();
        let unchanged = store.generation_release_notify().notified();
        drop(first);
        assert!(tokio::time::timeout(std::time::Duration::from_millis(25), unchanged)
            .await
            .is_err());
        assert_eq!(store.oldest_live_generation(), Some(1));

        let advanced = store.generation_release_notify().notified();
        drop(replacement);
        tokio::time::timeout(std::time::Duration::from_secs(1), advanced)
            .await
            .unwrap();
        assert_eq!(store.oldest_live_generation(), None);
    }

    #[tokio::test]
    async fn observability_records_bounded_layout_generation_and_sweep_metrics() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let recorder = TestMetricsRecorder::default();

        metrics::with_local_recorder(&recorder, || {
            let store = TailStore::open(&context, identity(16), 0).unwrap();
            assert_eq!(
                recorder.gauge("tycho_storage_rpc_tail_layout_info|layout=monolithic_v1"),
                1.0,
            );
            assert_eq!(
                recorder.gauge("tycho_storage_rpc_tail_oldest_required_generation"),
                0.0,
            );
            assert_eq!(
                recorder.gauge("tycho_storage_rpc_tail_live_request_snapshots"),
                0.0,
            );
            stage_retired_transactions(&store, 1, |_| true);
            let old = store
                .request_snapshot(TailLayoutVersion::MonolithicV1, 1)
                .unwrap();
            let old_clone = old.clone();
            let current = store
                .request_snapshot(TailLayoutVersion::MonolithicV1, 2)
                .unwrap();
            assert_eq!(
                recorder.gauge("tycho_storage_rpc_tail_oldest_required_generation"),
                1.0,
            );
            assert_eq!(
                recorder.gauge("tycho_storage_rpc_tail_live_request_snapshots"),
                2.0,
            );
            drop(old);
            assert_eq!(
                recorder.gauge("tycho_storage_rpc_tail_live_request_snapshots"),
                2.0,
            );
            drop(old_clone);
            assert_eq!(
                recorder.gauge("tycho_storage_rpc_tail_oldest_required_generation"),
                2.0,
            );
            assert_eq!(
                recorder.gauge("tycho_storage_rpc_tail_live_request_snapshots"),
                1.0,
            );
            let result = store.sweep_retired(2, 1).unwrap();
            assert_eq!(result.swept_records, 1);
            drop(current);
            assert_eq!(
                recorder.gauge("tycho_storage_rpc_tail_oldest_required_generation"),
                2.0,
            );
            assert_eq!(
                recorder.gauge("tycho_storage_rpc_tail_live_request_snapshots"),
                0.0,
            );
        });

        assert_eq!(
            recorder.counter("tycho_storage_rpc_tail_sweep_records_total"),
            1,
        );
        assert_eq!(
            recorder.counter("tycho_storage_rpc_tail_sweep_batches_total"),
            1,
        );
        assert_eq!(
            recorder.histogram_values("tycho_storage_rpc_tail_sweep_generation_lag"),
            vec![0.0],
        );
        assert!(recorder.keys().iter().all(|key| {
            !key.contains("generation=") && !key.contains("account=") && !key.contains("path=")
        }));
    }

    #[tokio::test]
    async fn generation_metadata_exact_reads_follow_atomic_writes() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let store = TailStore::open(&context, identity(10), 0).unwrap();
        assert_eq!(store.generation_progress(1).unwrap(), None);
        assert_eq!(store.generation_commit(1).unwrap(), None);

        let account = account_key(1);
        let promoted = promotion(account, 10, 1, 0xa1);
        let chunk_progress = progress(
            1,
            80,
            TailProgressCursor::Account(account),
            false,
            TailGenerationCounters {
                processed_accounts: 1,
                promoted_records: 1,
                promoted_bytes: promoted.value.as_bytes().len() as u64,
                ..Default::default()
            },
        );
        let chunk_progress = store
            .append_chunk(
                &[AccountTailDelta::new(account, vec![promoted], vec![]).unwrap()],
                None,
                chunk_progress,
            )
            .unwrap();
        assert_eq!(store.generation_progress(1).unwrap(), Some(chunk_progress));
        assert_eq!(store.generation_commit(1).unwrap(), None);

        let eof = TailGenerationProgress { eof: true, ..chunk_progress };
        let terminal = commit(eof);
        store.finish_generation(eof, terminal).unwrap();
        assert_eq!(store.generation_progress(1).unwrap(), Some(eof));
        assert_eq!(store.generation_commit(1).unwrap(), Some(terminal));
        assert_eq!(store.generation_progress(2).unwrap(), None);
        assert_eq!(store.generation_commit(2).unwrap(), None);
        assert!(store.generation_progress(0).is_err());
        assert!(store.generation_commit(0).is_err());

        let progress_key = codec::tail_generation_progress_key(1).unwrap();
        let commit_key = codec::tail_generation_commit_key(1).unwrap();
        store.db().generation_progress.insert(progress_key, [0]).unwrap();
        let error = store.generation_progress(1).unwrap_err();
        assert_eq!(
            classify_authoritative_error(&error),
            Some(AuthoritativeErrorKind::MalformedCommittedData),
        );
        let conflicting_progress = TailGenerationProgress {
            target_generation: 2,
            ..eof
        };
        store
            .db()
            .generation_progress
            .insert(progress_key, codec::encode_tail_generation_progress(&conflicting_progress).unwrap())
            .unwrap();
        let error = store.generation_progress(1).unwrap_err();
        assert_eq!(
            classify_authoritative_error(&error),
            Some(AuthoritativeErrorKind::ConflictingCommittedData),
        );
        store
            .db()
            .generation_progress
            .insert(progress_key, codec::encode_tail_generation_progress(&eof).unwrap())
            .unwrap();

        let mut malformed_layout = codec::encode_tail_generation_commit(&terminal).unwrap();
        malformed_layout[1] = 2;
        store.db().generation_commits.insert(commit_key, malformed_layout).unwrap();
        let error = store.generation_commit(1).unwrap_err();
        assert_eq!(
            classify_authoritative_error(&error),
            Some(AuthoritativeErrorKind::MalformedCommittedData),
        );
        let conflicting_commit = TailGenerationCommit {
            target_generation: 2,
            previous_visible_generation: 1,
            ..terminal
        };
        store
            .db()
            .generation_commits
            .insert(commit_key, codec::encode_tail_generation_commit(&conflicting_commit).unwrap())
            .unwrap();
        let error = store.generation_commit(1).unwrap_err();
        assert_eq!(
            classify_authoritative_error(&error),
            Some(AuthoritativeErrorKind::ConflictingCommittedData),
        );
        store
            .db()
            .generation_commits
            .insert(commit_key, codec::encode_tail_generation_commit(&terminal).unwrap())
            .unwrap();
    }

    #[tokio::test]
    async fn published_generation_commit_read_is_missing_or_conflicting_authoritative_data() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let expected = identity(17);
        let store = TailStore::open(&context, expected, 0).unwrap();
        let terminal = finish_empty_generation(&store, 1, 81);
        let commit_key = codec::tail_generation_commit_key(1).unwrap();

        store.db().generation_commits.remove(commit_key).unwrap();
        let error = store
            .request_snapshot(TailLayoutVersion::MonolithicV1, 1)
            .err()
            .unwrap();
        assert_eq!(
            classify_authoritative_error(&error),
            Some(AuthoritativeErrorKind::MissingCommittedData),
        );
        drop(store);
        let error = match TailStore::open(&context, expected, 1) {
            Ok(_) => panic!("published RPC tail generation must require its commit"),
            Err(error) => error,
        };
        assert_eq!(
            classify_authoritative_error(&error),
            Some(AuthoritativeErrorKind::MissingCommittedData),
        );
        let store = TailStore::open(&context, expected, 0).unwrap();

        let mut malformed_layout = codec::encode_tail_generation_commit(&terminal).unwrap();
        malformed_layout[1] = 2;
        store.db().generation_commits.insert(commit_key, malformed_layout).unwrap();
        let error = store
            .request_snapshot(TailLayoutVersion::MonolithicV1, 1)
            .err()
            .unwrap();
        assert_eq!(
            classify_authoritative_error(&error),
            Some(AuthoritativeErrorKind::MalformedCommittedData),
        );

        let conflicting_commit = TailGenerationCommit {
            target_generation: 2,
            previous_visible_generation: 1,
            ..terminal
        };
        store
            .db()
            .generation_commits
            .insert(commit_key, codec::encode_tail_generation_commit(&conflicting_commit).unwrap())
            .unwrap();
        let error = store
            .request_snapshot(TailLayoutVersion::MonolithicV1, 1)
            .err()
            .unwrap();
        assert_eq!(
            classify_authoritative_error(&error),
            Some(AuthoritativeErrorKind::ConflictingCommittedData),
        );
        drop(store);
        let error = match TailStore::open(&context, expected, 1) {
            Ok(_) => panic!("published RPC tail generation must reject a conflicting commit"),
            Err(error) => error,
        };
        assert_eq!(
            classify_authoritative_error(&error),
            Some(AuthoritativeErrorKind::ConflictingCommittedData),
        );
        let store = TailStore::open(&context, expected, 0).unwrap();

        store
            .db()
            .generation_commits
            .insert(commit_key, codec::encode_tail_generation_commit(&terminal).unwrap())
            .unwrap();
        assert!(store
            .request_snapshot(TailLayoutVersion::MonolithicV1, 1)
            .is_ok());
    }

    #[tokio::test]
    async fn startup_gc_intent_validation_accepts_only_the_phase_metadata_matrix() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let expected_identity = identity(15);
        let store = TailStore::open(&context, expected_identity, 0).unwrap();
        let partial = progress(
            1,
            90,
            TailProgressCursor::Account(account_key(1)),
            false,
            TailGenerationCounters {
                processed_accounts: 1,
                ..Default::default()
            },
        );
        let eof = TailGenerationProgress { eof: true, ..partial };
        let terminal = commit(eof);
        let progress_key = codec::tail_generation_progress_key(1).unwrap();
        let commit_key = codec::tail_generation_commit_key(1).unwrap();
        let evacuating = gc_intent(GcIntentPhase::Evacuating, partial);
        let prepared = gc_intent(GcIntentPhase::Prepared, partial);

        store.validate_startup_gc_intent(evacuating).unwrap();
        assert!(store.validate_startup_gc_intent(prepared).is_err());
        store
            .db()
            .generation_commits
            .insert(commit_key, codec::encode_tail_generation_commit(&terminal).unwrap())
            .unwrap();
        assert!(store.validate_startup_gc_intent(evacuating).is_err());
        assert!(store.validate_startup_gc_intent(prepared).is_err());
        store.db().generation_commits.remove(commit_key).unwrap();

        store
            .db()
            .generation_progress
            .insert(progress_key, codec::encode_tail_generation_progress(&partial).unwrap())
            .unwrap();
        store.validate_startup_gc_intent(evacuating).unwrap();
        assert!(store.validate_startup_gc_intent(prepared).is_err());
        store
            .db()
            .generation_commits
            .insert(commit_key, codec::encode_tail_generation_commit(&terminal).unwrap())
            .unwrap();
        assert!(store.validate_startup_gc_intent(evacuating).is_err());
        assert!(store.validate_startup_gc_intent(prepared).is_err());
        store.db().generation_commits.remove(commit_key).unwrap();

        store
            .db()
            .generation_progress
            .insert(progress_key, codec::encode_tail_generation_progress(&eof).unwrap())
            .unwrap();
        assert!(store.validate_startup_gc_intent(evacuating).is_err());
        assert!(store.validate_startup_gc_intent(prepared).is_err());
        store
            .db()
            .generation_commits
            .insert(commit_key, codec::encode_tail_generation_commit(&terminal).unwrap())
            .unwrap();
        store.validate_startup_gc_intent(evacuating).unwrap();
        store.validate_startup_gc_intent(prepared).unwrap();
        let committed = gc_intent(GcIntentPhase::CutoverCommitted, partial);
        let deleting = gc_intent(GcIntentPhase::Deleting, partial);
        assert!(store.validate_startup_gc_intent(committed).is_err());
        assert!(store.validate_startup_gc_intent(deleting).is_err());
        drop(store);

        let reopened = TailStore::open(&context, expected_identity, 1).unwrap();
        reopened.validate_startup_gc_intent(committed).unwrap();
        reopened.validate_startup_gc_intent(deleting).unwrap();
        assert!(reopened.validate_startup_gc_intent(evacuating).is_err());
        assert!(reopened.validate_startup_gc_intent(prepared).is_err());
    }

    #[tokio::test]
    async fn startup_gc_intent_validation_rejects_identity_and_layout_conflicts() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let store = TailStore::open(&context, identity(16), 0).unwrap();
        let partial = progress(
            1,
            91,
            TailProgressCursor::Account(account_key(1)),
            false,
            TailGenerationCounters {
                processed_accounts: 1,
                ..Default::default()
            },
        );
        let eof = TailGenerationProgress { eof: true, ..partial };
        let terminal = commit(eof);
        let progress_key = codec::tail_generation_progress_key(1).unwrap();
        let commit_key = codec::tail_generation_commit_key(1).unwrap();
        let evacuating = gc_intent(GcIntentPhase::Evacuating, partial);
        let prepared = gc_intent(GcIntentPhase::Prepared, partial);
        let conflicting_progress = TailGenerationProgress {
            operation_id: partial.operation_id + 1,
            ..partial
        };
        store
            .db()
            .generation_progress
            .insert(progress_key, codec::encode_tail_generation_progress(&conflicting_progress).unwrap())
            .unwrap();
        assert!(store.validate_startup_gc_intent(evacuating).is_err());

        store
            .db()
            .generation_progress
            .insert(progress_key, codec::encode_tail_generation_progress(&eof).unwrap())
            .unwrap();
        let conflicting_commit = TailGenerationCommit {
            cutoff_utime: terminal.cutoff_utime + 1,
            ..terminal
        };
        store
            .db()
            .generation_commits
            .insert(commit_key, codec::encode_tail_generation_commit(&conflicting_commit).unwrap())
            .unwrap();
        assert!(store.validate_startup_gc_intent(evacuating).is_err());
        assert!(store.validate_startup_gc_intent(prepared).is_err());

        let mut malformed_layout = codec::encode_tail_generation_commit(&terminal).unwrap();
        malformed_layout[1] = 2;
        store
            .db()
            .generation_commits
            .insert(commit_key, malformed_layout)
            .unwrap();
        assert!(store.validate_startup_gc_intent(evacuating).is_err());
        assert!(store.validate_startup_gc_intent(prepared).is_err());
    }

    #[tokio::test]
    async fn terminal_and_startup_metadata_reject_source_and_policy_mismatches() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let store = TailStore::open(&context, identity(19), 0).unwrap();
        let terminal_progress = progress(
            1,
            92,
            TailProgressCursor::Start,
            true,
            TailGenerationCounters::default(),
        );
        let terminal = commit(terminal_progress);
        for conflicting in [
            TailGenerationCommit {
                source_partition_id: terminal.source_partition_id + 1,
                ..terminal
            },
            TailGenerationCommit {
                source_manifest_digest: HashBytes([0x19; 32]),
                ..terminal
            },
            TailGenerationCommit {
                retention_policy_digest: HashBytes([0x1a; 32]),
                ..terminal
            },
        ] {
            assert!(store.finish_generation(terminal_progress, conflicting).is_err());
            assert!(store.generation_commit(1).unwrap().is_none());
        }

        store.finish_generation(terminal_progress, terminal).unwrap();
        let prepared = gc_intent(GcIntentPhase::Prepared, terminal_progress);
        store.validate_startup_gc_intent(prepared).unwrap();
        for conflicting in [
            GcIntent {
                source_partition_id: prepared.source_partition_id + 1,
                ..prepared
            },
            GcIntent {
                source_manifest_digest: HashBytes([0x1b; 32]),
                ..prepared
            },
            GcIntent {
                retention_policy_digest: HashBytes([0x1c; 32]),
                ..prepared
            },
        ] {
            assert!(store.validate_startup_gc_intent(conflicting).is_err());
        }
    }

    #[tokio::test]
    async fn newest_account_transactions_are_bounded_visible_and_pageable() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let store = TailStore::open(&context, identity(11), 0).unwrap();
        let account = account_key(1);
        let first = promotion(account, 10, 1, 0xb1);
        let future_frontier = promotion_with_mc_seqno(account, 20, 1, 0xb2, 8);
        let retired = promotion(account, 30, 1, 0xb3);
        let fourth = promotion(account, 40, 1, 0xb4);
        let retired_key = retired.payload_key;
        let retired_bytes = retired.value.as_bytes().len() as u64;
        let first_progress = progress(
            1,
            81,
            TailProgressCursor::Account(account),
            false,
            TailGenerationCounters {
                processed_accounts: 1,
                promoted_records: 4,
                promoted_bytes: [first.value.as_bytes(), future_frontier.value.as_bytes(), retired.value.as_bytes(), fourth.value.as_bytes()]
                    .into_iter()
                    .map(|value| value.len() as u64)
                    .sum(),
                ..Default::default()
            },
        );
        store
            .append_chunk(
                &[AccountTailDelta::new(
                    account,
                    vec![first, future_frontier, retired, fourth],
                    vec![],
                )
                .unwrap()],
                None,
                first_progress,
            )
            .unwrap();
        finish_started_generation(&store, first_progress);

        let fifth = promotion(account, 50, 2, 0xb5);
        let fifth_key = fifth.payload_key;
        let second_progress = progress(
            2,
            82,
            TailProgressCursor::Account(account),
            false,
            TailGenerationCounters {
                processed_accounts: 1,
                promoted_records: 1,
                promoted_bytes: fifth.value.as_bytes().len() as u64,
                retired_records: 1,
                retired_bytes,
            },
        );
        store
            .append_chunk(
                &[AccountTailDelta::new(account, vec![fifth], vec![retired_key]).unwrap()],
                None,
                second_progress,
            )
            .unwrap();
        finish_started_generation(&store, second_progress);

        let future_generation = promotion(account, 60, 3, 0xb6);
        let third_progress = progress(
            3,
            83,
            TailProgressCursor::Account(account),
            false,
            TailGenerationCounters {
                processed_accounts: 1,
                promoted_records: 1,
                promoted_bytes: future_generation.value.as_bytes().len() as u64,
                ..Default::default()
            },
        );
        store
            .append_chunk(
                &[AccountTailDelta::new(account, vec![future_generation], vec![]).unwrap()],
                None,
                third_progress,
            )
            .unwrap();

        let generation_one = store
            .request_snapshot(TailLayoutVersion::MonolithicV1, 1)
            .unwrap();
        assert_eq!(
            record_lts(&generation_one.newest_account_transactions(account, None, 10, 7).unwrap()),
            [40, 30, 10],
        );

        let generation_two = store
            .request_snapshot(TailLayoutVersion::MonolithicV1, 2)
            .unwrap();
        assert_eq!(
            record_lts(&generation_two.newest_account_transactions(account, None, 4, 7).unwrap()),
            [50, 40, 10],
        );
        assert_eq!(
            record_lts(&generation_two.newest_account_transactions(account, None, 3, 7).unwrap()),
            [50, 40, 10],
        );
        assert!(generation_two
            .newest_account_transactions(account, None, 0, 7)
            .unwrap()
            .is_empty());
        let first_page = generation_two
            .newest_account_transactions(account, None, 2, 7)
            .unwrap();
        assert_eq!(record_lts(&first_page), [50, 40]);
        assert_eq!(
            record_lts(&generation_two.newest_account_transactions(account, Some(40), 2, 7).unwrap()),
            [10],
        );
        assert!(generation_two
            .newest_account_transactions(account, Some(0), 2, 7)
            .unwrap()
            .is_empty());
        assert_eq!(
            record_lts(&generation_two.newest_account_transactions(account, None, 10, 8).unwrap()),
            [50, 40, 20, 10],
        );
        assert!(generation_two
            .newest_account_transactions(account_key(2), None, 10, 8)
            .unwrap()
            .is_empty());

        store
            .db()
            .transactions_by_account
            .insert(fifth_key, [0])
            .unwrap();
        let conflicting = store
            .request_snapshot(TailLayoutVersion::MonolithicV1, 2)
            .unwrap();
        assert!(conflicting
            .newest_account_transactions(account, None, 0, 7)
            .unwrap()
            .is_empty());
        assert!(conflicting.newest_account_transactions(account, None, 1, 7).is_err());
    }

    #[tokio::test]
    async fn request_reads_enforce_generation_frontier_and_account_bounds() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let store = TailStore::open(&context, identity(8), 0).unwrap();
        let account = account_key(1);
        let first = promotion(account, 10, 1, 0x81);
        let retired = promotion(account, 20, 1, 0x82);
        let third = promotion(account, 30, 1, 0x83);
        let first_hash = first.account_locator.transaction_hash;
        let first_in_msg = first.in_msg_locator.unwrap().0;
        let retired_key = retired.payload_key;
        let retired_hash = retired.account_locator.transaction_hash;
        let retired_in_msg = retired.in_msg_locator.unwrap().0;
        let retired_bytes = retired.value.as_bytes().len() as u64;
        let first_generation = progress(
            1,
            60,
            TailProgressCursor::Account(account),
            false,
            TailGenerationCounters {
                processed_accounts: 1,
                promoted_records: 3,
                promoted_bytes: [first.value.as_bytes(), retired.value.as_bytes(), third.value.as_bytes()]
                    .into_iter()
                    .map(|value| value.len() as u64)
                    .sum(),
                ..Default::default()
            },
        );
        store
            .append_chunk(
                &[AccountTailDelta::new(account, vec![first, retired, third], vec![]).unwrap()],
                None,
                first_generation,
            )
            .unwrap();
        finish_started_generation(&store, first_generation);

        let fourth = promotion(account, 40, 2, 0x84);
        let fourth_hash = fourth.account_locator.transaction_hash;
        let fourth_in_msg = fourth.in_msg_locator.unwrap().0;
        let second_generation = progress(
            2,
            61,
            TailProgressCursor::Account(account),
            false,
            TailGenerationCounters {
                processed_accounts: 1,
                promoted_records: 1,
                promoted_bytes: fourth.value.as_bytes().len() as u64,
                retired_records: 1,
                retired_bytes,
            },
        );
        store
            .append_chunk(
                &[AccountTailDelta::new(account, vec![fourth], vec![retired_key]).unwrap()],
                None,
                second_generation,
            )
            .unwrap();
        finish_started_generation(&store, second_generation);

        let future = promotion(account, 50, 3, 0x85);
        let future_hash = future.account_locator.transaction_hash;
        let third_generation = progress(
            3,
            62,
            TailProgressCursor::Account(account),
            false,
            TailGenerationCounters {
                processed_accounts: 1,
                promoted_records: 1,
                promoted_bytes: future.value.as_bytes().len() as u64,
                ..Default::default()
            },
        );
        store
            .append_chunk(
                &[AccountTailDelta::new(account, vec![future], vec![]).unwrap()],
                None,
                third_generation,
            )
            .unwrap();

        let generation_one = store
            .request_snapshot(TailLayoutVersion::MonolithicV1, 1)
            .unwrap();
        assert!(generation_one.transaction_by_hash(&retired_hash, 7).unwrap().is_some());
        assert!(generation_one.transaction_by_in_msg(&retired_in_msg, 7).unwrap().is_some());
        assert!(generation_one.transaction_by_hash(&fourth_hash, 7).unwrap().is_none());
        assert_eq!(visible_account_lts(&generation_one, account, 0, u64::MAX, false, 7), [10, 20, 30]);

        let generation_two = store
            .request_snapshot(TailLayoutVersion::MonolithicV1, 2)
            .unwrap();
        assert!(generation_two.transaction_by_hash(&first_hash, 7).unwrap().is_some());
        assert!(generation_two.transaction_by_in_msg(&first_in_msg, 7).unwrap().is_some());
        assert!(generation_two.transaction_by_hash(&retired_hash, 7).unwrap().is_none());
        assert!(generation_two.transaction_by_in_msg(&retired_in_msg, 7).unwrap().is_none());
        assert!(generation_two.transaction_by_hash(&fourth_hash, 7).unwrap().is_some());
        assert!(generation_two.transaction_by_in_msg(&fourth_in_msg, 7).unwrap().is_some());
        assert!(generation_two.transaction_by_hash(&future_hash, 7).unwrap().is_none());
        assert!(generation_two.transaction_by_hash(&first_hash, 6).unwrap().is_none());
        assert!(generation_two.transaction_by_in_msg(&first_in_msg, 6).unwrap().is_none());
        assert_eq!(visible_account_lts(&generation_two, account, 0, u64::MAX, false, 7), [10, 30, 40]);
        assert_eq!(visible_account_lts(&generation_two, account, 0, u64::MAX, true, 7), [40, 30, 10]);
        assert_eq!(visible_account_lts(&generation_two, account, 25, 40, false, 7), [30, 40]);
        assert!(visible_account_lts(&generation_two, account, 41, 39, false, 7).is_empty());
        assert!(visible_account_lts(&generation_two, account_key(2), 0, u64::MAX, false, 7).is_empty());
        assert!(visible_account_lts(&generation_two, account, 0, u64::MAX, false, 6).is_empty());
        assert_eq!(
            codec::decode_tail_payload_key(
                &generation_two.source_transaction(account, 35, 7).unwrap().unwrap().payload_key,
            )
            .unwrap()
            .1,
            30,
        );
        assert!(generation_two.source_transaction(account, 10, 7).unwrap().is_none());
        assert!(generation_two.source_transaction(account, 0, 7).unwrap().is_none());
    }

    #[tokio::test]
    async fn request_reads_classify_missing_payload_and_malformed_or_conflicting_locators() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let store = TailStore::open(&context, identity(9), 0).unwrap();
        let account = account_key(1);
        let first = promotion(account, 10, 1, 0x91);
        let second = promotion(account, 20, 1, 0x92);
        let first_key = first.payload_key;
        let first_hash = first.account_locator.transaction_hash;
        let first_payload = first.value.as_bytes().to_vec();
        let first_hash_locator = first.hash_locator;
        let (second_in_msg, second_in_msg_locator) = second.in_msg_locator.unwrap();
        let promoted_bytes = (first.value.as_bytes().len() + second.value.as_bytes().len()) as u64;
        let generation = progress(
            1,
            70,
            TailProgressCursor::Account(account),
            false,
            TailGenerationCounters {
                processed_accounts: 1,
                promoted_records: 2,
                promoted_bytes,
                ..Default::default()
            },
        );
        store
            .append_chunk(
                &[AccountTailDelta::new(account, vec![first, second], vec![]).unwrap()],
                None,
                generation,
            )
            .unwrap();
        finish_started_generation(&store, generation);

        store.db().transactions.remove(first_key).unwrap();
        let missing_payload = store
            .request_snapshot(TailLayoutVersion::MonolithicV1, 1)
            .unwrap();
        let error = missing_payload.transaction_by_hash(&first_hash, 7).unwrap_err();
        assert_eq!(
            classify_authoritative_error(&error),
            Some(AuthoritativeErrorKind::MissingCommittedData),
        );
        store.db().transactions.insert(first_key, first_payload).unwrap();

        store.db().transactions_by_hash.insert(first_hash, [0]).unwrap();
        let malformed = store
            .request_snapshot(TailLayoutVersion::MonolithicV1, 1)
            .unwrap();
        let error = malformed.transaction_by_hash(&first_hash, 7).unwrap_err();
        assert_eq!(
            classify_authoritative_error(&error),
            Some(AuthoritativeErrorKind::MalformedCommittedData),
        );
        store
            .db()
            .transactions_by_hash
            .insert(first_hash, codec::encode_tail_hash_locator(&first_hash_locator).unwrap())
            .unwrap();

        store
            .db()
            .transactions_by_in_msg
            .insert(
                second_in_msg,
                codec::encode_tail_in_msg_locator(TailInMsgLocator {
                    payload_key: first_key,
                    born_generation: 1,
                    dead_generation: None,
                })
                .unwrap(),
            )
            .unwrap();
        let conflicting = store
            .request_snapshot(TailLayoutVersion::MonolithicV1, 1)
            .unwrap();
        let error = conflicting.transaction_by_in_msg(&second_in_msg, 7).unwrap_err();
        assert_eq!(
            classify_authoritative_error(&error),
            Some(AuthoritativeErrorKind::ConflictingCommittedData),
        );
        store
            .db()
            .transactions_by_in_msg
            .insert(second_in_msg, codec::encode_tail_in_msg_locator(second_in_msg_locator).unwrap())
            .unwrap();

        store
            .db()
            .transactions_by_account
            .remove(first_key)
            .unwrap();
        let missing = store
            .request_snapshot(TailLayoutVersion::MonolithicV1, 1)
            .unwrap();
        let error = missing.transaction_by_hash(&first_hash, 7).unwrap_err();
        assert_eq!(
            classify_authoritative_error(&error),
            Some(AuthoritativeErrorKind::MissingCommittedData),
        );
        store
            .db()
            .transactions_by_hash
            .remove(first_hash)
            .unwrap();
        let absent = store
            .request_snapshot(TailLayoutVersion::MonolithicV1, 1)
            .unwrap();
        assert!(absent.transaction_by_hash(&first_hash, 7).unwrap().is_none());
    }

    #[tokio::test]
    async fn chunk_and_terminal_commit_are_atomic_idempotent_and_snapshot_typed() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let expected = identity(3);
        let store = TailStore::open(&context, expected, 0).unwrap();
        let account = account_key(1);
        let promoted = promotion(account, 11, 1, 0x11);
        let payload_key = promoted.payload_key;
        let transaction_hash = promoted.account_locator.transaction_hash;
        let in_msg_hash = promoted.in_msg_locator.unwrap().0;
        let promoted_bytes = promoted.value.as_bytes().len() as u64;
        let delta = AccountTailDelta::new(account, vec![promoted.clone()], vec![]).unwrap();
        let chunk_progress = progress(
            1,
            10,
            TailProgressCursor::Account(account),
            false,
            TailGenerationCounters {
                processed_accounts: 1,
                promoted_records: 1,
                promoted_bytes,
                ..Default::default()
            },
        );
        let before = store.snapshot();
        let chunk_progress = store.append_chunk(&[delta.clone()], None, chunk_progress).unwrap();
        assert!(before.transaction(payload_key).unwrap().is_none());
        assert!(before.generation_progress(1).unwrap().is_none());

        let snapshot = store.snapshot();
        let record = snapshot.transaction_record(payload_key).unwrap().unwrap();
        assert_eq!(record.payload_key, payload_key);
        assert_eq!(record.value.transaction_hash(), transaction_hash);
        assert_eq!(record.hash_locator.mc_seqno, record.value.related_mc_seqno());
        assert_eq!(record.in_msg_locator.unwrap().0, in_msg_hash);
        assert_eq!(snapshot.hash_locator(&transaction_hash).unwrap().unwrap().payload_key, payload_key);
        assert_eq!(snapshot.in_msg_locator(&in_msg_hash).unwrap().unwrap().payload_key, payload_key);
        assert_eq!(snapshot.generation_progress(1).unwrap(), Some(chunk_progress));
        store.append_chunk(&[delta], None, chunk_progress).unwrap();

        let conflicting = promotion(account, 11, 1, 0x21);
        let conflicting_delta = AccountTailDelta::new(account, vec![conflicting], vec![]).unwrap();
        assert!(store.append_chunk(&[conflicting_delta], None, chunk_progress).is_err());
        assert_eq!(store.snapshot().transaction(payload_key).unwrap().unwrap().transaction_hash(), transaction_hash);

        let second_account = account_key(2);
        let second = promotion(second_account, 12, 2, 0x31);
        let second_key = second.payload_key;
        let bad_progress = progress(
            2,
            11,
            TailProgressCursor::Account(second_account),
            false,
            TailGenerationCounters {
                processed_accounts: 1,
                promoted_records: 1,
                promoted_bytes: 0,
                ..Default::default()
            },
        );
        assert!(store
            .append_chunk(&[AccountTailDelta::new(second_account, vec![second], vec![]).unwrap()], None, bad_progress)
            .is_err());
        assert!(store.snapshot().transaction(second_key).unwrap().is_none());
        assert!(store.snapshot().generation_progress(2).unwrap().is_none());

        let eof = TailGenerationProgress { eof: true, ..chunk_progress };
        let terminal = commit(eof);
        store.finish_generation(eof, terminal).unwrap();
        store.finish_generation(eof, terminal).unwrap();
        assert_eq!(store.snapshot().generation_commit(1).unwrap(), Some(terminal));
        drop(snapshot);
        drop(before);
        drop(store);
        let reopened = TailStore::open(&context, expected, 1).unwrap();
        assert_eq!(reopened.snapshot().generation_commit(1).unwrap(), Some(terminal));
    }

    #[test]
    fn chunk_digest_canonical_vector_binds_empty_promoted_and_retired_deltas() {
        let first_account = account_key(1);
        let second_account = account_key(2);
        let third_account = account_key(3);
        let predecessor = TailGenerationProgress {
            chunk_digest: HashBytes([0x55; 32]),
            ..progress(
                2,
                12,
                TailProgressCursor::Account(first_account),
                false,
                TailGenerationCounters {
                    processed_accounts: 1,
                    ..Default::default()
                },
            )
        };
        let promoted = promotion(third_account, 13, 2, 0x73);
        let promoted_bytes = promoted.value.as_bytes().len() as u64;
        let target = progress(
            2,
            12,
            TailProgressCursor::Account(third_account),
            false,
            TailGenerationCounters {
                processed_accounts: 3,
                promoted_records: 1,
                promoted_bytes,
                retired_records: 1,
                retired_bytes: 99,
            },
        );
        let deltas = [
            AccountTailDelta::new(second_account, vec![], vec![]).unwrap(),
            AccountTailDelta::new(
                third_account,
                vec![promoted],
                vec![codec::tail_payload_key(third_account, 14)],
            )
            .unwrap(),
        ];

        let digest = derive_chunk_digest(Some(predecessor), target, &deltas).unwrap();
        assert_eq!(digest, HashBytes([
            0x25, 0x82, 0xf4, 0x84, 0x12, 0xcd, 0xcb, 0x78,
            0xa4, 0xb0, 0x0c, 0x24, 0xe2, 0xf8, 0xf4, 0x40,
            0x04, 0x83, 0x81, 0xcb, 0x8b, 0x8b, 0xf7, 0x29,
            0xbb, 0x74, 0x02, 0xed, 0x1a, 0xe8, 0xe6, 0x24,
        ]));
    }

    #[tokio::test]
    async fn multi_account_chunk_replay_rejects_suffix_and_forged_predecessor_across_restart() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let expected = identity(19);
        let store = TailStore::open(&context, expected, 0).unwrap();
        let first_account = account_key(1);
        let first = promotion(first_account, 11, 1, 0x71);
        let first_bytes = first.value.as_bytes().len() as u64;
        let first_progress = progress(
            1,
            12,
            TailProgressCursor::Account(first_account),
            false,
            TailGenerationCounters {
                processed_accounts: 1,
                promoted_records: 1,
                promoted_bytes: first_bytes,
                ..Default::default()
            },
        );
        let first_progress = store
            .append_chunk(
                &[AccountTailDelta::new(first_account, vec![first], vec![]).unwrap()],
                None,
                first_progress,
            )
            .unwrap();

        let second_account = account_key(2);
        let third_account = account_key(3);
        let third = promotion(third_account, 13, 1, 0x73);
        let third_bytes = third.value.as_bytes().len() as u64;
        let deltas = [
            AccountTailDelta::new(second_account, vec![], vec![]).unwrap(),
            AccountTailDelta::new(third_account, vec![third], vec![]).unwrap(),
        ];
        let chunk_progress = progress(
            1,
            12,
            TailProgressCursor::Account(third_account),
            false,
            TailGenerationCounters {
                processed_accounts: 3,
                promoted_records: 2,
                promoted_bytes: first_bytes + third_bytes,
                ..Default::default()
            },
        );
        let chunk_progress = store.append_chunk(&deltas, Some(first_progress), chunk_progress).unwrap();
        let forged_predecessor = progress(
            1,
            12,
            TailProgressCursor::Account(second_account),
            false,
            TailGenerationCounters {
                processed_accounts: 2,
                promoted_records: 1,
                promoted_bytes: first_bytes,
                ..Default::default()
            },
        );
        let forged_predecessor = TailGenerationProgress {
            chunk_digest: derive_chunk_digest(Some(first_progress), forged_predecessor, &deltas[..1]).unwrap(),
            ..forged_predecessor
        };
        drop(store);

        let reopened = TailStore::open(&context, expected, 0).unwrap();
        reopened.append_chunk(&deltas, Some(first_progress), chunk_progress).unwrap();
        assert!(reopened.append_chunk(&deltas[1..], Some(first_progress), chunk_progress).is_err());
        assert!(reopened.append_chunk(&deltas[1..], Some(forged_predecessor), chunk_progress).is_err());
        assert_eq!(reopened.generation_progress(1).unwrap(), Some(chunk_progress));
    }

    #[tokio::test]
    async fn append_rejects_terminal_commit_after_progress_cleanup_and_malformed_commit() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let store = TailStore::open(&context, identity(6), 0).unwrap();
        let first_account = account_key(1);
        let first = promotion(first_account, 11, 1, 0x61);
        let first_bytes = first.value.as_bytes().len() as u64;
        let first_progress = progress(
            1,
            40,
            TailProgressCursor::Account(first_account),
            false,
            TailGenerationCounters {
                processed_accounts: 1,
                promoted_records: 1,
                promoted_bytes: first_bytes,
                ..Default::default()
            },
        );
        let first_progress = store
            .append_chunk(
                &[AccountTailDelta::new(first_account, vec![first], vec![]).unwrap()],
                None,
                first_progress,
            )
            .unwrap();
        let eof = TailGenerationProgress { eof: true, ..first_progress };
        let terminal = commit(eof);
        store.finish_generation(eof, terminal).unwrap();
        store
            .db()
            .generation_progress
            .remove(codec::tail_generation_progress_key(1).unwrap())
            .unwrap();

        let second_account = account_key(2);
        let second = promotion(second_account, 12, 1, 0x62);
        let second_key = second.payload_key;
        let second_bytes = second.value.as_bytes().len() as u64;
        let second_progress = progress(
            1,
            40,
            TailProgressCursor::Account(second_account),
            false,
            TailGenerationCounters {
                processed_accounts: 1,
                promoted_records: 1,
                promoted_bytes: second_bytes,
                ..Default::default()
            },
        );
        assert!(store
            .append_chunk(
                &[AccountTailDelta::new(second_account, vec![second], vec![]).unwrap()],
                None,
                second_progress,
            )
            .is_err());
        assert!(store.snapshot().transaction(second_key).unwrap().is_none());
        assert!(store.snapshot().generation_progress(1).unwrap().is_none());
        assert_eq!(store.snapshot().generation_commit(1).unwrap(), Some(terminal));

        store
            .db()
            .generation_commits
            .insert(codec::tail_generation_commit_key(2).unwrap(), [0])
            .unwrap();
        let third_account = account_key(3);
        let third = promotion(third_account, 13, 2, 0x63);
        let third_key = third.payload_key;
        let third_progress = progress(
            2,
            41,
            TailProgressCursor::Account(third_account),
            false,
            TailGenerationCounters {
                processed_accounts: 1,
                promoted_records: 1,
                promoted_bytes: third.value.as_bytes().len() as u64,
                ..Default::default()
            },
        );
        assert!(store
            .append_chunk(
                &[AccountTailDelta::new(third_account, vec![third], vec![]).unwrap()],
                None,
                third_progress,
            )
            .is_err());
        assert!(store.snapshot().transaction(third_key).unwrap().is_none());
        assert!(store.snapshot().generation_progress(2).unwrap().is_none());
    }

    #[tokio::test]
    async fn logical_retirement_is_composite_atomic_and_replay_strict() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let store = TailStore::open(&context, identity(4), 0).unwrap();
        let account = account_key(3);
        let promoted = promotion(account, 15, 1, 0x41);
        let payload_key = promoted.payload_key;
        let transaction_hash = promoted.account_locator.transaction_hash;
        let in_msg_hash = promoted.in_msg_locator.unwrap().0;
        let promoted_bytes = promoted.value.as_bytes().len() as u64;
        store
            .append_chunk(
                &[AccountTailDelta::new(account, vec![promoted], vec![]).unwrap()],
                None,
                progress(
                    1,
                    20,
                    TailProgressCursor::Account(account),
                    false,
                    TailGenerationCounters {
                        processed_accounts: 1,
                        promoted_records: 1,
                        promoted_bytes,
                        ..Default::default()
                    },
                ),
            )
            .unwrap();
        let retirement_progress = progress(
            2,
            21,
            TailProgressCursor::Account(account),
            false,
            TailGenerationCounters {
                processed_accounts: 1,
                retired_records: 1,
                retired_bytes: promoted_bytes,
                ..Default::default()
            },
        );
        let retirement = AccountTailDelta::new(account, vec![], vec![payload_key]).unwrap();
        store.append_chunk(&[retirement.clone()], None, retirement_progress).unwrap();
        let snapshot = store.snapshot();
        let record = snapshot.transaction_record(payload_key).unwrap().unwrap();
        assert_eq!(record.account_locator.dead_generation, Some(2));
        assert_eq!(record.hash_locator.dead_generation, Some(2));
        assert_eq!(record.in_msg_locator.unwrap().1.dead_generation, Some(2));
        assert!(snapshot.retirement_entry(2, payload_key).unwrap().is_some());
        assert!(snapshot.transaction(payload_key).unwrap().is_some());
        store.append_chunk(&[retirement.clone()], None, retirement_progress).unwrap();

        store
            .db()
            .retired_by_generation
            .remove(codec::retired_transaction_key(2, payload_key).unwrap())
            .unwrap();
        assert!(store.append_chunk(&[retirement], None, retirement_progress).is_err());
        assert_eq!(store.snapshot().hash_locator(&transaction_hash).unwrap().unwrap().dead_generation, Some(2));
        assert_eq!(store.snapshot().in_msg_locator(&in_msg_hash).unwrap().unwrap().dead_generation, Some(2));
    }

    #[tokio::test]
    async fn delta_and_composite_reads_reject_noncanonical_or_conflicting_data() {
        let account = account_key(5);
        let first = promotion(account, 20, 1, 0x51);
        let second = promotion(account, 19, 1, 0x52);
        assert!(AccountTailDelta::new(account, vec![first.clone(), second], vec![]).is_err());
        assert!(AccountTailDelta::new(account, vec![], vec![first.payload_key, first.payload_key]).is_err());
        assert!(AccountTailDelta::new(account_key(6), vec![first.clone()], vec![]).is_err());

        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let store = TailStore::open(&context, identity(5), 0).unwrap();
        let promoted_bytes = first.value.as_bytes().len() as u64;
        let chunk_progress = progress(
            1,
            30,
            TailProgressCursor::Account(account),
            false,
            TailGenerationCounters {
                processed_accounts: 1,
                promoted_records: 1,
                promoted_bytes,
                ..Default::default()
            },
        );
        store
            .append_chunk(&[AccountTailDelta::new(account, vec![first.clone()], vec![]).unwrap()], None, chunk_progress)
            .unwrap();
        let mut conflicting_hash = first.hash_locator;
        conflicting_hash.mc_seqno += 1;
        store
            .db()
            .transactions_by_hash
            .insert(first.account_locator.transaction_hash, codec::encode_tail_hash_locator(&conflicting_hash).unwrap())
            .unwrap();
        assert!(store.snapshot().hash_locator(&first.account_locator.transaction_hash).unwrap().is_some());
        let error = store.snapshot().transaction_record(first.payload_key).unwrap_err();
        assert_eq!(
            classify_authoritative_error(&error),
            Some(AuthoritativeErrorKind::ConflictingCommittedData),
        );

        let mismatched_progress = progress(
            1,
            31,
            TailProgressCursor::Account(account),
            false,
            TailGenerationCounters {
                processed_accounts: 1,
                ..Default::default()
            },
        );
        store
            .db()
            .generation_progress
            .insert(codec::tail_generation_progress_key(2).unwrap(), codec::encode_tail_generation_progress(&mismatched_progress).unwrap())
            .unwrap();
        let error = store.snapshot().generation_progress(2).unwrap_err();
        assert_eq!(
            classify_authoritative_error(&error),
            Some(AuthoritativeErrorKind::ConflictingCommittedData),
        );
    }

    #[tokio::test]
    async fn tail_sweep_hard_caps_exact_batch_and_finishes_partial_batch() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let store = TailStore::open(&context, identity(8), 0).unwrap();
        let records = stage_retired_transactions(&store, TAIL_SWEEP_MAX_RECORDS + 1, |_| false);

        let first = store.sweep_retired(2, usize::MAX).unwrap();
        assert_eq!(first.swept_records, TAIL_SWEEP_MAX_RECORDS);
        assert_eq!(first.oldest_swept_generation, Some(2));
        assert_eq!(first.newest_swept_generation, Some(2));
        assert_eq!(first.next, TailSweepNext::Ready);
        assert!(store.db().transactions.get(records[0].payload_key).unwrap().is_none());
        assert!(store
            .snapshot()
            .retirement_entry(2, records[TAIL_SWEEP_MAX_RECORDS - 1].payload_key)
            .unwrap()
            .is_none());
        assert!(store.db().transactions.get(records[TAIL_SWEEP_MAX_RECORDS].payload_key).unwrap().is_some());

        let second = store.sweep_retired(2, TAIL_SWEEP_MAX_RECORDS).unwrap();
        assert_eq!(second.swept_records, 1);
        assert_eq!(second.next, TailSweepNext::Empty);
        assert!(store.db().transactions.get(records[TAIL_SWEEP_MAX_RECORDS].payload_key).unwrap().is_none());
        assert!(store.delete_obsolete_progress(1, 2).is_err());
        assert!(store.delete_obsolete_progress(2, 2).unwrap());
        assert!(store.generation_progress(2).unwrap().is_none());
        assert!(store.generation_commit(2).unwrap().is_some());
        assert!(!store.delete_obsolete_progress(2, 2).unwrap());
    }

    #[tokio::test]
    async fn obsolete_progress_requires_a_valid_commit_but_deletes_opaque_progress() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let store = TailStore::open(&context, identity(15), 0).unwrap();
        stage_retired_transactions(&store, 1, |_| false);
        let commit = store.generation_commit(2).unwrap().unwrap();
        let progress_key = codec::tail_generation_progress_key(2).unwrap();
        let commit_key = codec::tail_generation_commit_key(2).unwrap();
        store.db().generation_progress.insert(progress_key, [0]).unwrap();
        store.db().generation_commits.insert(commit_key, [0]).unwrap();

        let error = store.delete_obsolete_progress(2, 2).unwrap_err();
        assert_eq!(
            classify_authoritative_error(&error),
            Some(AuthoritativeErrorKind::MalformedCommittedData),
        );
        assert!(store.db().generation_progress.get(progress_key).unwrap().is_some());
        store
            .db()
            .generation_commits
            .insert(commit_key, codec::encode_tail_generation_commit(&commit).unwrap())
            .unwrap();

        assert!(store.delete_obsolete_progress(2, 2).unwrap());
        assert!(store.db().generation_progress.get(progress_key).unwrap().is_none());
        assert_eq!(store.generation_commit(2).unwrap(), Some(commit));
    }

    #[tokio::test]
    async fn tail_sweep_atomically_deletes_inbound_and_no_inbound_records() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let store = TailStore::open(&context, identity(9), 0).unwrap();
        let records = stage_retired_transactions(&store, 2, |index| index == 1);
        let inbound_hash = records[0].in_msg_hash.unwrap();
        assert!(records[1].in_msg_hash.is_none());
        assert!(store.db().transactions_by_in_msg.get(inbound_hash).unwrap().is_some());

        let result = store.sweep_retired(2, 2).unwrap();
        assert_eq!(result.swept_records, 2);
        assert_eq!(result.next, TailSweepNext::Empty);
        for record in records {
            assert!(store.db().transactions.get(record.payload_key).unwrap().is_none());
            assert!(store.db().transactions_by_account.get(record.payload_key).unwrap().is_none());
            assert!(store.db().transactions_by_hash.get(record.transaction_hash).unwrap().is_none());
            assert!(store.snapshot().retirement_entry(2, record.payload_key).unwrap().is_none());
        }
        assert!(store.db().transactions_by_in_msg.get(inbound_hash).unwrap().is_none());
    }

    #[tokio::test]
    async fn tail_sweep_waits_for_h_minus_one_but_not_h_snapshot() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let store = TailStore::open(&context, identity(10), 0).unwrap();
        stage_retired_transactions(&store, 1, |_| true);
        let old = store
            .request_snapshot(TailLayoutVersion::MonolithicV1, 1)
            .unwrap();
        let current = store
            .request_snapshot(TailLayoutVersion::MonolithicV1, 2)
            .unwrap();

        assert_eq!(
            store.sweep_retired(1, 1).unwrap().next,
            TailSweepNext::AwaitPublishedGeneration { dead_generation: 2 },
        );
        assert_eq!(
            store.sweep_retired(2, 1).unwrap().next,
            TailSweepNext::AwaitRequestGeneration {
                dead_generation: 2,
                oldest_live_generation: 1,
            },
        );
        assert!(store.request_snapshot(TailLayoutVersion::MonolithicV1, 1).is_err());
        drop(old);
        let result = store.sweep_retired(2, 1).unwrap();
        assert_eq!(result.swept_records, 1);
        assert_eq!(result.next, TailSweepNext::Empty);
        assert_eq!(current.visible_generation(), 2);
        drop(current);
    }

    #[tokio::test]
    async fn sweep_floor_serializes_a_late_old_request_with_physical_deletion() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let store = Arc::new(TailStore::open(&context, identity(11), 0).unwrap());
        stage_retired_transactions(&store, 1, |_| false);
        let gate = Arc::new(TailSweepFloorGate::new());
        let pin_gate = Arc::new(TailPinGate::new());
        store.set_sweep_floor_gate(Some(gate.clone()));
        store.set_before_pin_gate(Some(pin_gate.clone()));
        let sweep_store = store.clone();
        let sweep = std::thread::spawn(move || sweep_store.sweep_retired(2, 1));
        gate.entered.wait();
        let (finished_tx, finished_rx) = std::sync::mpsc::channel();
        let request_store = store.clone();
        let request = std::thread::spawn(move || {
            let rejected = request_store
                .request_snapshot(TailLayoutVersion::MonolithicV1, 1)
                .is_err();
            finished_tx.send(rejected).unwrap();
        });
        pin_gate.before_lock.wait();
        assert!(matches!(
            finished_rx.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty),
        ));
        gate.release.wait();

        let result = sweep.join().unwrap().unwrap();
        assert_eq!(result.swept_records, 1);
        assert!(finished_rx.recv().unwrap());
        request.join().unwrap();
        store.set_sweep_floor_gate(None);
        store.set_before_pin_gate(None);
    }

    #[tokio::test]
    async fn tail_sweep_restarts_and_retries_transient_write_idempotently() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let expected = identity(12);
        let store = TailStore::open(&context, expected, 0).unwrap();
        let record = stage_retired_transactions(&store, 1, |_| true)[0];
        store.inject_next_sweep_write_failure();
        let error = store.sweep_retired(2, 1).unwrap_err();
        assert_eq!(classify_authoritative_error(&error), None);
        assert!(store.db().transactions.get(record.payload_key).unwrap().is_some());
        assert!(store.snapshot().retirement_entry(2, record.payload_key).unwrap().is_some());
        drop(store);

        let reopened = TailStore::open(&context, expected, 2).unwrap();
        assert_eq!(reopened.sweep_retired(2, 1).unwrap().swept_records, 1);
        assert_eq!(reopened.sweep_retired(2, 1).unwrap().swept_records, 0);
        assert!(reopened.generation_commit(1).unwrap().is_some());
        assert!(reopened.generation_commit(2).unwrap().is_some());
    }

    #[tokio::test]
    async fn tail_sweep_classifies_missing_malformed_and_conflicting_committed_data() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let store = TailStore::open(&context, identity(13), 0).unwrap();
        let identity = stage_retired_transactions(&store, 1, |_| true)[0];
        let record = store
            .snapshot()
            .transaction_record(identity.payload_key)
            .unwrap()
            .unwrap();
        let retirement_key = codec::retired_transaction_key(2, identity.payload_key).unwrap();

        store.db().transactions.remove(identity.payload_key).unwrap();
        let error = store.sweep_retired(2, 1).unwrap_err();
        assert_eq!(
            classify_authoritative_error(&error),
            Some(AuthoritativeErrorKind::MissingCommittedData),
        );
        assert!(store.db().retired_by_generation.get(retirement_key).unwrap().is_some());
        store.db().transactions.insert(identity.payload_key, record.value.as_bytes()).unwrap();

        store.db().transactions_by_hash.remove(identity.transaction_hash).unwrap();
        let error = store.sweep_retired(2, 1).unwrap_err();
        assert_eq!(
            classify_authoritative_error(&error),
            Some(AuthoritativeErrorKind::MissingCommittedData),
        );
        store
            .db()
            .transactions_by_hash
            .insert(identity.transaction_hash, codec::encode_tail_hash_locator(&record.hash_locator).unwrap())
            .unwrap();

        store.db().transactions_by_account.insert(identity.payload_key, [0]).unwrap();
        let error = store.sweep_retired(2, 1).unwrap_err();
        assert_eq!(
            classify_authoritative_error(&error),
            Some(AuthoritativeErrorKind::MalformedCommittedData),
        );
        store
            .db()
            .transactions_by_account
            .insert(identity.payload_key, codec::encode_tail_account_locator(record.account_locator).unwrap())
            .unwrap();

        let mut conflicting_hash = record.hash_locator;
        conflicting_hash.mc_seqno += 1;
        store
            .db()
            .transactions_by_hash
            .insert(identity.transaction_hash, codec::encode_tail_hash_locator(&conflicting_hash).unwrap())
            .unwrap();
        let error = store.sweep_retired(2, 1).unwrap_err();
        assert_eq!(
            classify_authoritative_error(&error),
            Some(AuthoritativeErrorKind::ConflictingCommittedData),
        );
        store
            .db()
            .transactions_by_hash
            .insert(identity.transaction_hash, codec::encode_tail_hash_locator(&record.hash_locator).unwrap())
            .unwrap();

        let in_msg_hash = identity.in_msg_hash.unwrap();
        store.db().transactions_by_in_msg.remove(in_msg_hash).unwrap();
        let error = store.sweep_retired(2, 1).unwrap_err();
        assert_eq!(
            classify_authoritative_error(&error),
            Some(AuthoritativeErrorKind::MissingCommittedData),
        );
        store
            .db()
            .transactions_by_in_msg
            .insert(in_msg_hash, codec::encode_tail_in_msg_locator(record.in_msg_locator.unwrap().1).unwrap())
            .unwrap();

        assert_eq!(store.sweep_retired(2, 1).unwrap().swept_records, 1);
    }
}
