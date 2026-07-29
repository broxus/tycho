use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use arc_swap::ArcSwap;
use parking_lot::{Mutex, RwLock};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
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

use super::db::{RpcCurrentStateDb, RpcRouterDb, RpcTransactionsDb};
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
    router: RpcRouterDb,
    router_commit_guard: Mutex<()>,
    current_state: RpcCurrentStateDb,
    min_tx_lt: AtomicU64,
    min_tx_lt_guard: tokio::sync::Mutex<()>,
    snapshots: Arc<SnapshotPublisher>,
    sealing_notify: Arc<Notify>,
    sealing_cancel: CancellationFlag,
    sealing_task: Option<JoinHandle<()>>,
}

#[derive(Default)]
struct SnapshotPublisher {
    /// Keeps publication and sealing lease removal in one lock order.
    current: RwLock<Option<Arc<RpcSnapshotInner>>>,
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
    notify: Arc<Notify>,
    cancelled: CancellationFlag,
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
                    id,
                    cancelled.clone(),
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
    id: PartitionId,
    cancelled: CancellationFlag,
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
                        previous.as_ref().map(|snapshot| snapshot.visible_frontier);
                    drop(previous);

                    // a lease acquired before closing may have written after the preliminary flush
                    let seal_result = flush_transaction_partition(&db)
                        .and_then(|()| finish_sealing_partition(&partitions, id, db));
                    let snapshot_result = match frontier {
                        Some(frontier) => rebuild_composite_snapshot_until_ready(
                            &partitions,
                            frontier,
                            &cancelled,
                        )
                        .map(Some),
                        None => Ok(None),
                    };
                    let snapshot_error = match snapshot_result {
                        Ok(Some(snapshot)) => {
                            *published = Some(snapshot.0);
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
    published: Option<&Arc<RpcSnapshotInner>>,
    id: PartitionId,
) -> bool {
    let current_is_unshared = published.is_none_or(|snapshot| Arc::strong_count(snapshot) == 1);
    if !current_is_unshared {
        return false;
    }
    let current_has_lease = published
        .is_some_and(|snapshot| snapshot.writable_partitions.contains_key(&id));
    let expected_handles = 2 + usize::from(current_has_lease);
    partitions.lock().sealing_handle_strong_count(id) == Some(expected_handles)
}

fn rebuild_composite_snapshot_until_ready(
    partitions: &Arc<Mutex<PartitionManager>>,
    frontier: BlockId,
    cancelled: &CancellationFlag,
) -> Result<RpcSnapshot> {
    let mut retry_delay = Duration::from_millis(10);
    loop {
        match build_composite_snapshot(&mut partitions.lock(), frontier) {
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
        let partitions = Arc::new(Mutex::new(PartitionManager::open(context, config)?));
        let router = partitions.lock().router_db().clone();
        let current_state = partitions.lock().current_state_db().clone();
        let persisted_min_lt = partitions.lock().min_transaction_lt();
        let snapshots = Arc::new(SnapshotPublisher::default());
        let sealing_notify = Arc::new(Notify::new());
        let sealing_cancel = CancellationFlag::new();
        let sealing_task = Some(spawn_sealing_worker(
            partitions.clone(),
            snapshots.clone(),
            sealing_notify.clone(),
            sealing_cancel.clone(),
        ));
        let this = Self {
            partitions,
            router,
            router_commit_guard: Default::default(),
            current_state,
            min_tx_lt: AtomicU64::new(u64::MAX),
            min_tx_lt_guard: Default::default(),
            snapshots,
            sealing_notify,
            sealing_cancel,
            sealing_task,
        };

        let state = &this.current_state.state;
        if state.get(INSTANCE_ID)?.is_none() {
            state.insert(INSTANCE_ID, rand::random::<InstanceId>())?;
        }

        let min_lt = (persisted_min_lt != u64::MAX).then_some(persisted_min_lt);

        this.min_tx_lt
            .store(min_lt.unwrap_or(u64::MAX), Ordering::Release);

        tracing::debug!(?min_lt, "rpc storage initialized");

        if this.partitions.lock().next_sealing_partition().is_some() {
            this.sealing_notify.notify_one();
        }
        Ok(this)
    }

    pub fn min_tx_lt(&self) -> u64 {
        self.min_tx_lt.load(Ordering::Acquire)
    }

    pub(crate) fn reconcile_startup(
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

        let router_commit = self.load_router_commit()?;
        match control_frontier {
            None | Some(BlockId { seqno: 0, .. }) => match router_commit {
                None => {}
                Some(router_commit) if router_commit.seqno == 1 => {}
                Some(router_commit) => anyhow::bail!(
                    "router commit {} is more than one unpublished boundary ahead of zerostate; clear the RPC DB and reindex",
                    router_commit
                ),
            },
            Some(control_frontier) => {
                let router_commit = router_commit.context(
                    "RPC visible frontier has no router commit; clear the RPC DB and reindex",
                )?;
                if router_commit.seqno < control_frontier.seqno {
                    anyhow::bail!(
                        "router commit {} is behind RPC visible frontier {}; clear the RPC DB and reindex",
                        router_commit,
                        control_frontier
                    );
                }
                if router_commit.seqno == control_frontier.seqno {
                    anyhow::ensure!(
                        router_commit == control_frontier,
                        "router commit and RPC visible frontier have different full block ids at masterchain seqno {}",
                        control_frontier.seqno
                    );
                } else {
                    let next_seqno = control_frontier
                        .seqno
                        .checked_add(1)
                        .context("router commit is ahead of the maximum masterchain sequence number")?;
                    anyhow::ensure!(
                        router_commit.seqno == next_seqno,
                        "router commit {} is more than one unpublished boundary ahead of RPC visible frontier {}; clear the RPC DB and reindex",
                        router_commit,
                        control_frontier
                    );
                }
            }
        }

        if let Some(control_frontier) = control_frontier
            && control_frontier.seqno > 0
        {
            partitions
                .validate_masterchain_commit(&control_frontier)
                .context("invalid persisted RPC visible frontier")?;
        }

        Ok(StartupReconciliation {
            effective_frontier: *core_frontier,
            rebuild_current_state: partitions.has_commits_after(core_frontier.seqno),
        })
    }

    fn load_router_commit(&self) -> Result<Option<BlockId>> {
        self.router
            .state
            .get(codec::router_commit_key())?
            .map(|value| {
                let block_id = codec::decode_router_commit(value.as_ref())
                    .context("invalid persisted router commit")?;
                anyhow::ensure!(
                    block_id.is_masterchain() && block_id.seqno > 0,
                    "persisted router commit must be a non-zero masterchain block"
                );
                Ok(block_id)
            })
            .transpose()
    }

    fn commit_router_frontier(&self, block_id: &BlockId) -> Result<()> {
        anyhow::ensure!(
            block_id.is_masterchain() && block_id.seqno > 0,
            "router commit must be a non-zero masterchain block"
        );
        let _guard = self.router_commit_guard.lock();
        let should_write = match self.load_router_commit()? {
            None => true,
            Some(current) if current.seqno < block_id.seqno => true,
            Some(current) if current.seqno == block_id.seqno => {
                anyhow::ensure!(
                    current == *block_id,
                    "router commit has a different full block id at masterchain seqno {}",
                    block_id.seqno
                );
                false
            }
            Some(_) => false,
        };
        if should_write {
            let _stage = HistogramGuard::begin("tycho_storage_rpc_write_router_commit_time");
            self.router
                .state
                .insert(codec::router_commit_key(), codec::encode_router_commit(block_id))
                .context("failed to write RPC router commit")?;
        }
        Ok(())
    }

    pub fn publish_snapshot(&self, visible_frontier: &BlockId) -> Result<()> {
        let mut published = self.snapshots.current.write();
        // an older replay must not regress an already published effective frontier
        let visible_frontier = match published.as_ref() {
            Some(current) if current.visible_frontier.seqno > visible_frontier.seqno => {
                current.visible_frontier
            }
            Some(current) if current.visible_frontier.seqno == visible_frontier.seqno => {
                anyhow::ensure!(
                    current.visible_frontier == *visible_frontier,
                    "cannot publish a different RPC snapshot frontier at masterchain seqno {}",
                    visible_frontier.seqno
                );
                *visible_frontier
            }
            _ => *visible_frontier,
        };
        let snapshot =
            build_composite_snapshot(&mut self.partitions.lock(), visible_frontier)?;
        *published = Some(snapshot.0);
        Ok(())
    }

    /// Publishes a completed masterchain block set before exposing the following set to readers.
    pub fn commit_masterchain_block_set(&self, block_id: &BlockId) -> Result<()> {
        self.commit_router_frontier(block_id)?;
        self.partitions.lock().commit_masterchain_block_set(block_id)?;
        self.publish_snapshot(block_id)?;
        if self.partitions.lock().next_sealing_partition().is_some() {
            self.sealing_notify.notify_one();
        }
        Ok(())
    }

    pub fn load_snapshot(&self) -> Option<RpcSnapshot> {
        self.snapshots.current.read().clone().map(RpcSnapshot)
    }

    fn require_snapshot(&self, snapshot: Option<&RpcSnapshot>) -> Result<RpcSnapshot> {
        snapshot
            .cloned()
            .or_else(|| self.load_snapshot())
            .context("No RPC snapshot available")
    }

    fn resolve_router_location(
        &self,
        snapshot: RpcSnapshot,
        location: codec::RouterLocation,
    ) -> Result<Option<RpcTransactionPartitionRead>> {
        if location.mc_seqno > snapshot.visible_frontier().seqno {
            return Ok(None);
        }
        let id = PartitionId(location.partition_id);
        let descriptor = snapshot
            .descriptor(id)
            .with_context(|| {
                format!(
                    "router location references transaction partition {} outside the RPC snapshot",
                    id.0
                )
            })?;
        anyhow::ensure!(
            descriptor.first.block_id.is_some()
                && descriptor.last.block_id.is_some()
                && descriptor.first.mc_seqno <= location.mc_seqno
                && location.mc_seqno <= descriptor.last.mc_seqno,
            "router location masterchain seqno {} is outside transaction partition {} bounds",
            location.mc_seqno,
            id.0
        );
        acquire_partition_read(&self.partitions, snapshot, id).map(Some)
    }

    fn transaction_partition(
        &self,
        hash: &HashBytes,
        snapshot: RpcSnapshot,
    ) -> Result<Option<(codec::RouterLocation, RpcTransactionPartitionRead)>> {
        let Some(value) = self.router.transactions.get_ext(hash, snapshot.router())? else {
            return Ok(None);
        };
        let location = codec::decode_router_location(value.as_ref())
            .context("invalid transaction router location")?;
        drop(value);
        Ok(self
            .resolve_router_location(snapshot, location)?
            .map(|partition| (location, partition)))
    }

    fn inbound_message_partition(
        &self,
        hash: &HashBytes,
        snapshot: RpcSnapshot,
    ) -> Result<Option<(codec::RouterLocation, RpcTransactionPartitionRead)>> {
        let Some(value) = self
            .router
            .inbound_messages
            .get_ext(hash, snapshot.router())?
        else {
            return Ok(None);
        };
        let location = codec::decode_router_location(value.as_ref())
            .context("invalid inbound-message router location")?;
        drop(value);
        Ok(self
            .resolve_router_location(snapshot, location)?
            .map(|partition| (location, partition)))
    }

    fn block_partition(
        &self,
        block_id: &BlockIdShort,
        snapshot: RpcSnapshot,
    ) -> Result<Option<(codec::RouterLocation, RpcTransactionPartitionRead)>> {
        let key = codec::encode_short_block_id(block_id);
        let Some(value) = self.router.blocks.get_ext(key, snapshot.router())? else {
            return Ok(None);
        };
        let location =
            codec::decode_router_location(value.as_ref()).context("invalid block router location")?;
        drop(value);
        Ok(self
            .resolve_router_location(snapshot, location)?
            .map(|partition| (location, partition)))
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
        let mut key = [0; tables::KnownBlocks::KEY_LEN];
        key[0] = workchain as u8;
        key[1..9].copy_from_slice(&block_id.shard.prefix().to_be_bytes());
        key[9..13].copy_from_slice(&block_id.seqno.to_be_bytes());

        let snapshot = self.require_snapshot(snapshot)?;
        let Some((location, partition)) =
            self.block_partition(block_id, snapshot)?
        else {
            return Ok(None);
        };
        let table = &partition.lease.known_blocks;
        let value = partition
            .get(table, key)?
            .context("block router points to a missing local known-block record")?;
        anyhow::ensure!(value.len() >= 68, "invalid known block value");
        let mc_seqno = u32::from_le_bytes(value[64..68].try_into().unwrap());
        anyhow::ensure!(
            mc_seqno == location.mc_seqno,
            "block router and local known-block record have different related masterchain seqnos"
        );

        let brief_info = BriefBlockInfo::load_from_bytes(workchain as i32, &value[68..])
            .context("invalid brief info")?;

        let block_id = BlockId {
            shard: block_id.shard,
            seqno: block_id.seqno,
            root_hash: HashBytes::from_slice(&value[0..32]),
            file_hash: HashBytes::from_slice(&value[32..64]),
        };

        Ok(Some((block_id, mc_seqno, brief_info)))
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
        let Ok(workchain) = i8::try_from(block_id.shard.workchain()) else {
            return Ok(None);
        };

        let snapshot = self.require_snapshot(snapshot.as_ref())?;
        let Some((location, partition)) =
            self.block_partition(block_id, snapshot)?
        else {
            return Ok(None);
        };

        let mut range_from = [0x00; tables::BlockTransactions::KEY_LEN];
        range_from[0] = workchain as u8;
        range_from[1..9].copy_from_slice(&block_id.shard.prefix().to_be_bytes());
        range_from[9..13].copy_from_slice(&block_id.seqno.to_be_bytes());

        if let Some(cursor) = cursor {
            range_from[13..45].copy_from_slice(cursor.hash.as_slice());
            range_from[45..53].copy_from_slice(&cursor.lt.to_be_bytes());
        }

        let table = &partition.lease.known_blocks;
        let ref_by_mc_seqno;
        let block_id = match partition.get(table, &range_from[0..13])? {
            Some(value) => {
                anyhow::ensure!(value.len() >= 68, "invalid known block value");
                ref_by_mc_seqno = u32::from_le_bytes(value[64..68].try_into().unwrap());
                anyhow::ensure!(
                    ref_by_mc_seqno == location.mc_seqno,
                    "block router and local known-block record have different related masterchain seqnos"
                );
                BlockId {
                    shard: block_id.shard,
                    seqno: block_id.seqno,
                    root_hash: HashBytes::from_slice(&value[0..32]),
                    file_hash: HashBytes::from_slice(&value[32..64]),
                }
            }
            None => return Ok(None),
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
        let Some((location, partition)) =
            self.transaction_partition(hash, snapshot)?
        else {
            return Ok(None);
        };
        let tx_info = partition
            .get_pinned(&partition.lease.transactions_by_hash, hash)?
            .context("transaction router points to a missing local hash locator")?;
        let info = TransactionInfo::from_bytes(tx_info.as_ref())
            .context("transaction router points to an invalid local hash locator")?;
        anyhow::ensure!(
            info.mc_seqno == location.mc_seqno,
            "transaction router and local hash locator have different related masterchain seqnos"
        );
        let tx = partition
            .get_pinned(
                &partition.lease.transactions,
                &tx_info.as_ref()[..tables::Transactions::KEY_LEN],
            )?
            .context("transaction hash locator points to a missing local transaction")?;
        let transaction_mc_seqno = TransactionData::related_mc_seqno(tx.as_ref())?;
        anyhow::ensure!(
            transaction_mc_seqno == location.mc_seqno,
            "transaction router and local transaction have different related masterchain seqnos"
        );
        anyhow::ensure!(
            TransactionData::read_tx_hash(tx.as_ref()) == *hash,
            "transaction hash locator points to a transaction with a different hash"
        );
        Ok(Some(map(info, tx.as_ref())))
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
        let Some((location, partition)) =
            self.inbound_message_partition(in_msg_hash, snapshot)?
        else {
            return Ok(None);
        };
        let key = partition
            .get(&partition.lease.transactions_by_in_msg, in_msg_hash)?
            .context("inbound-message router points to a missing local message locator")?;
        anyhow::ensure!(
            key.len() == tables::Transactions::KEY_LEN,
            "invalid local inbound-message locator length"
        );
        let tx = partition
            .get(&partition.lease.transactions, &key)?
            .context("inbound-message locator points to a missing local transaction")?;
        anyhow::ensure!(
            TransactionData::related_mc_seqno(&tx)? == location.mc_seqno,
            "inbound-message router and local transaction have different related masterchain seqnos"
        );
        let tx = TransactionData::from_owned(tx);
        anyhow::ensure!(
            tx.in_msg_hash().as_ref() == Some(in_msg_hash),
            "inbound-message locator points to a transaction with a different message hash"
        );
        Ok(Some(tx))
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
            // replay validates the local batch, then repeats the global router and current-state stages
            anyhow::ensure!(commit.block_id == *block.id() && commit.digest == block.id().root_hash, "partition commit marker identity mismatch for {}", block.id());
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
        let router = self.router.clone();
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
            let mut router_batch = rocksdb::WriteBatch::default();
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
                    let location = codec::encode_router_location(codec::RouterLocation { partition_id: partition_id.0, mc_seqno });
                    router_batch.put_cf(&router.transactions.cf(), tx_hash.as_slice(), location);

                    if let Some(msg_hash) = msg_hash {
                        write_batch.put_cf(
                            tx_by_in_msg_cf,
                            msg_hash,
                            &tx_info[..tables::Transactions::KEY_LEN],
                        );
                        router_batch.put_cf(&router.inbound_messages.cf(), msg_hash, location);
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
            let location = codec::encode_router_location(codec::RouterLocation { partition_id: partition_id.0, mc_seqno });
            router_batch.put_cf(&router.blocks.cf(), codec::encode_short_block_id(&block_id.as_short_id()), location);

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
            let _stage = HistogramGuard::begin("tycho_storage_rpc_write_router_time");
            router
                .rocksdb()
                .write_opt(router_batch, router.transactions.write_config()).context("failed to write RPC router stage")?;
            drop(_stage);
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
    router: weedb::OwnedSnapshot,
    current_state: weedb::OwnedSnapshot,
    writable_partitions: BTreeMap<PartitionId, RpcTransactionPartitionSnapshot>,
}

fn build_composite_snapshot(
    partitions: &mut PartitionManager,
    visible_frontier: BlockId,
) -> Result<RpcSnapshot> {
    let descriptors = partitions.descriptors();
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
            codec::ManifestLifecycle::Creating => {
                anyhow::bail!(
                    "cannot publish RPC snapshot while transaction partition {} is creating",
                    descriptor.id.0
                );
            }
            // sealed partitions are immutable and opened through the bounded cache on demand
            codec::ManifestLifecycle::Sealed => {}
        }
    }
    anyhow::ensure!(
        writable_partitions.contains_key(&partitions.active_id()),
        "active transaction partition is missing from RPC snapshot"
    );
    Ok(RpcSnapshot(Arc::new(RpcSnapshotInner {
        visible_frontier,
        manifest_epoch: partitions.manifest_epoch(),
        descriptors,
        descriptor_indices,
        router: partitions.router_db().owned_snapshot(),
        current_state: partitions.current_state_db().owned_snapshot(),
        writable_partitions,
    })))
}

#[derive(Clone)]
#[repr(transparent)]
pub struct RpcSnapshot(Arc<RpcSnapshotInner>);

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

    fn descriptor_for_mc_seqno(&self, mc_seqno: u32) -> Option<&PartitionDescriptor> {
        if mc_seqno > self.visible_frontier().seqno {
            return None;
        }
        self.0.descriptors.iter().find(|descriptor| {
            descriptor.first.block_id.is_some()
                && descriptor.last.block_id.is_some()
                && descriptor.first.mc_seqno <= mc_seqno
                && mc_seqno <= descriptor.last.mc_seqno
        })
    }

    fn router(&self) -> &weedb::OwnedSnapshot {
        &self.0.router
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
    use std::str::FromStr;
    use tycho_rpc_subscriptions::SubscriberManagerConfig;
    use tycho_types::boc::Boc;

    fn test_partitions_config() -> RpcTransactionPartitionsConfig {
        RpcTransactionPartitionsConfig {
            target_lsm_bytes: 1,
            target_blob_bytes: 1,
            target_index_records: 1,
            max_open_sealed_partitions: 1,
        }
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

    fn indexed_block() -> BlockStuff {
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
        let mut router_batch = rocksdb::WriteBatch::default();
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

            let location = codec::encode_router_location(codec::RouterLocation {
                partition_id: partition_id.0,
                mc_seqno: mc_block_id.seqno,
            });
            router_batch.put_cf(&storage.router.transactions.cf(), tx.hash, location);
            router_batch.put_cf(
                &storage.router.inbound_messages.cf(),
                tx.in_msg_hash,
                location,
            );
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
            let location = codec::encode_router_location(codec::RouterLocation {
                partition_id: partition_id.0,
                mc_seqno: mc_block_id.seqno,
            });
            router_batch.put_cf(
                &storage.router.blocks.cf(),
                codec::encode_short_block_id(&current_block_id.as_short_id()),
                location,
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
        storage
            .router
            .rocksdb()
            .write_opt(router_batch, storage.router.transactions.write_config())
            .unwrap();
        partition_id
    }

    fn first_transaction_with_inbound_message(
        lease: &PartitionReadLease,
    ) -> (StdAddr, u64, HashBytes, HashBytes) {
        let mut iterator = lease
            .rocksdb()
            .raw_iterator_cf(&lease.transactions.cf());
        iterator.seek_to_first();
        while iterator.valid() {
            let key = iterator.key().unwrap();
            let value = iterator.value().unwrap();
            let payload = codec::decode_transaction_value(value).unwrap().payload();
            let mask = TransactionMask::from_bits_retain(payload[0]);
            if mask.has_msg_hash() {
                return (
                    StdAddr::new(key[0] as i8, HashBytes::from_slice(&key[1..33])),
                    u64::from_be_bytes(key[33..41].try_into().unwrap()),
                    HashBytes::from_slice(&payload[1..33]),
                    HashBytes::from_slice(&payload[33..65]),
                );
            }
            iterator.next();
        }
        panic!("indexed block fixture must contain an inbound message");
    }

    fn delete_router_records(
        storage: &RpcStorage,
        transaction_hash: &HashBytes,
        inbound_message_hash: &HashBytes,
        block_id: &BlockId,
    ) {
        let mut batch = rocksdb::WriteBatch::default();
        batch.delete_cf(&storage.router.transactions.cf(), transaction_hash);
        batch.delete_cf(
            &storage.router.inbound_messages.cf(),
            inbound_message_hash,
        );
        batch.delete_cf(
            &storage.router.blocks.cf(),
            codec::encode_short_block_id(&block_id.as_short_id()),
        );
        storage
            .router
            .rocksdb()
            .write_opt(batch, storage.router.transactions.write_config())
            .unwrap();
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

    fn persist_router_commit(storage: &RpcStorage, block_id: &BlockId) {
        storage
            .router
            .state
            .insert(codec::router_commit_key(), codec::encode_router_commit(block_id))
            .unwrap();
    }

    fn remove_router_commit(storage: &RpcStorage) {
        let mut batch = rocksdb::WriteBatch::default();
        batch.delete_cf(&storage.router.state.cf(), codec::router_commit_key());
        storage
            .router
            .rocksdb()
            .write_opt(batch, storage.router.state.write_config())
            .unwrap();
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
    async fn sealing_worker_flushes_closes_and_publishes_read_only_partition() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let mut manager = PartitionManager::open(
            context,
            RpcTransactionPartitionsConfig {
                target_lsm_bytes: 1,
                target_blob_bytes: 1,
                target_index_records: 1,
                max_open_sealed_partitions: 1,
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
            old,
            CancellationFlag::new(),
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

        let snapshot = build_composite_snapshot(&mut manager, block_id).unwrap();
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
        persist_router_commit(&storage, &block_id);

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
        persist_router_commit(&storage, &next);
        let behind_control = storage.reconcile_startup(&block_id).unwrap();
        assert_eq!(behind_control.effective_frontier, block_id);
        assert!(behind_control.rebuild_current_state);
        storage.publish_snapshot(&behind_control.effective_frontier).unwrap();
        assert_eq!(storage.load_snapshot().unwrap().visible_frontier(), &block_id);
    }

    #[tokio::test]
    async fn router_commit_is_monotonic_and_durable() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = RpcStorage::open(context.clone(), RpcTransactionPartitionsConfig::default()).unwrap();
        let first = masterchain_block(1);
        storage.commit_router_frontier(&first).unwrap();
        assert_eq!(storage.load_router_commit().unwrap(), Some(first));
        storage.commit_router_frontier(&first).unwrap();
        storage.commit_router_frontier(&masterchain_block(0)).unwrap_err();
        storage.commit_router_frontier(&basechain_block(2)).unwrap_err();

        let second = masterchain_block(2);
        storage.commit_router_frontier(&second).unwrap();
        storage.commit_router_frontier(&first).unwrap();
        assert_eq!(storage.load_router_commit().unwrap(), Some(second));
        let mut different = second;
        different.file_hash = HashBytes([0xff; 32]);
        assert!(storage.commit_router_frontier(&different).is_err());
        drop(storage);
        tokio::task::yield_now().await;

        let storage = RpcStorage::open(context, RpcTransactionPartitionsConfig::default()).unwrap();
        assert_eq!(storage.load_router_commit().unwrap(), Some(second));
        storage.router.state.insert(codec::router_commit_key(), [0]).unwrap();
        assert!(storage.load_router_commit().is_err());
    }

    #[tokio::test]
    async fn router_commit_replay_completes_unpublished_control_frontier() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = RpcStorage::open(context.clone(), RpcTransactionPartitionsConfig::default()).unwrap();
        let account = StdAddr::new(0, HashBytes::ZERO);
        let mc_block_id = masterchain_block(1);
        insert_read_test_block(
            &storage,
            &account,
            &mc_block_id,
            &basechain_block(1),
            &[ReadTestTransaction {
                lt: 1,
                hash: HashBytes([1; 32]),
                in_msg_hash: HashBytes([2; 32]),
                boc_byte: 3,
            }],
            0,
        );
        storage.commit_router_frontier(&mc_block_id).unwrap();
        drop(storage);
        tokio::task::yield_now().await;

        let storage = RpcStorage::open(context, RpcTransactionPartitionsConfig::default()).unwrap();
        let reconciliation = storage.reconcile_startup(&masterchain_block(0)).unwrap();
        assert_eq!(reconciliation.effective_frontier, masterchain_block(0));
        assert!(reconciliation.rebuild_current_state);

        storage.commit_masterchain_block_set(&mc_block_id).unwrap();
        assert_eq!(storage.load_router_commit().unwrap(), Some(mc_block_id));
        assert_eq!(
            storage
                .reconcile_startup(&mc_block_id)
                .unwrap()
                .effective_frontier,
            mc_block_id
        );
    }

    #[tokio::test]
    async fn startup_reconciliation_validates_router_commit_states() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = RpcStorage::open(context, RpcTransactionPartitionsConfig::default()).unwrap();
        let first = masterchain_block(1);
        let second = masterchain_block(2);
        {
            let manager = storage.partitions.lock();
            insert_masterchain_commit(&manager, &first, 0);
        }
        storage.commit_masterchain_block_set(&first).unwrap();
        {
            let manager = storage.partitions.lock();
            insert_masterchain_commit(&manager, &second, 0);
        }
        storage.commit_masterchain_block_set(&second).unwrap();
        let reconciliation = storage.reconcile_startup(&first).unwrap();
        assert_eq!(reconciliation.effective_frontier, first);
        assert!(reconciliation.rebuild_current_state);

        remove_router_commit(&storage);
        assert!(format!("{:#}", reconciliation_error(&storage, &second))
            .contains("clear the RPC DB and reindex"));
        persist_router_commit(&storage, &first);
        assert!(format!("{:#}", reconciliation_error(&storage, &second))
            .contains("is behind RPC visible frontier"));
        let mut different = second;
        different.root_hash = HashBytes([0xff; 32]);
        persist_router_commit(&storage, &different);
        assert!(format!("{:#}", reconciliation_error(&storage, &second))
            .contains("different full block ids"));
        persist_router_commit(&storage, &masterchain_block(3));
        assert_eq!(
            storage
                .reconcile_startup(&first)
                .unwrap()
                .effective_frontier,
            first
        );
        persist_router_commit(&storage, &masterchain_block(4));
        assert!(format!("{:#}", reconciliation_error(&storage, &second))
            .contains("more than one unpublished boundary ahead"));
        storage.router.state.insert(codec::router_commit_key(), [0]).unwrap();
        assert!(format!("{:#}", reconciliation_error(&storage, &second))
            .contains("invalid persisted router commit"));
        persist_router_commit(&storage, &masterchain_block(0));
        assert!(format!("{:#}", reconciliation_error(&storage, &second))
            .contains("non-zero masterchain block"));
        persist_router_commit(&storage, &basechain_block(2));
        assert!(format!("{:#}", reconciliation_error(&storage, &second))
            .contains("non-zero masterchain block"));
    }

    #[tokio::test]
    async fn router_commit_replay_completes_one_ahead_control_frontier() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = RpcStorage::open(context.clone(), RpcTransactionPartitionsConfig::default()).unwrap();
        let first = masterchain_block(1);
        let second = masterchain_block(2);
        let third = masterchain_block(3);
        {
            let manager = storage.partitions.lock();
            insert_masterchain_commit(&manager, &first, 0);
        }
        storage.commit_masterchain_block_set(&first).unwrap();
        {
            let manager = storage.partitions.lock();
            insert_masterchain_commit(&manager, &second, 0);
        }
        storage.commit_masterchain_block_set(&second).unwrap();
        {
            let manager = storage.partitions.lock();
            insert_masterchain_commit(&manager, &third, 0);
        }
        persist_router_commit(&storage, &third);
        drop(storage);
        tokio::task::yield_now().await;

        let storage = RpcStorage::open(context, RpcTransactionPartitionsConfig::default()).unwrap();
        let reconciliation = storage.reconcile_startup(&first).unwrap();
        assert_eq!(reconciliation.effective_frontier, first);
        assert!(reconciliation.rebuild_current_state);
        storage.publish_snapshot(&reconciliation.effective_frontier).unwrap();
        assert_eq!(storage.load_snapshot().unwrap().visible_frontier(), &first);

        storage.commit_masterchain_block_set(&third).unwrap();
        assert_eq!(storage.load_router_commit().unwrap(), Some(third));
        assert_eq!(storage.partitions.lock().visible_frontier(), Some(&third));
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
    async fn startup_frontier_uses_only_router_commit_witness() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = RpcStorage::open(
            context,
            RpcTransactionPartitionsConfig::default(),
        )
        .unwrap();
        let account = StdAddr::new(0, HashBytes::ZERO);
        let mc_block_id = masterchain_block(1);
        let block_id = basechain_block(1);
        let transaction_hash = HashBytes([1; 32]);
        insert_read_test_block(
            &storage,
            &account,
            &mc_block_id,
            &block_id,
            &[ReadTestTransaction {
                lt: 1,
                hash: transaction_hash,
                in_msg_hash: HashBytes([2; 32]),
                boc_byte: 3,
            }],
            0,
        );
        storage.commit_masterchain_block_set(&mc_block_id).unwrap();
        let witness_key = codec::encode_short_block_id(&mc_block_id.as_short_id());
        storage.router.transactions.insert(transaction_hash, [0]).unwrap();
        assert_eq!(
            storage
                .reconcile_startup(&mc_block_id)
                .unwrap()
                .effective_frontier,
            mc_block_id
        );

        storage.router.blocks.insert(witness_key, [0]).unwrap();
        assert_eq!(
            storage
                .reconcile_startup(&mc_block_id)
                .unwrap()
                .effective_frontier,
            mc_block_id
        );

        let mut batch = rocksdb::WriteBatch::default();
        batch.delete_cf(&storage.router.blocks.cf(), witness_key);
        storage
            .router
            .rocksdb()
            .write_opt(batch, storage.router.transactions.write_config())
            .unwrap();
        assert_eq!(
            storage
                .reconcile_startup(&mc_block_id)
                .unwrap()
                .effective_frontier,
            mc_block_id
        );
    }

    #[tokio::test]
    async fn startup_reconciliation_requires_router_commit_and_partition_commit_witnesses() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = RpcStorage::open(context, RpcTransactionPartitionsConfig::default()).unwrap();
        let account = StdAddr::new(0, HashBytes::ZERO);
        let mc_block_id = masterchain_block(1);
        let block_id = basechain_block(1);
        let transaction_hash = HashBytes([1; 32]);
        let inbound_message_hash = HashBytes([2; 32]);
        insert_read_test_block(
            &storage,
            &account,
            &mc_block_id,
            &block_id,
            &[ReadTestTransaction {
                lt: 1,
                hash: transaction_hash,
                in_msg_hash: inbound_message_hash,
                boc_byte: 3,
            }],
            0,
        );
        storage.commit_masterchain_block_set(&mc_block_id).unwrap();

        let lease = storage.partitions.lock().active_lease();
        let mut transaction_key = [0; tables::Transactions::KEY_LEN];
        transaction_key[0] = account.workchain as u8;
        transaction_key[1..33].copy_from_slice(account.address.as_slice());
        transaction_key[33..41].copy_from_slice(&1u64.to_be_bytes());
        let mut block_transaction_key = [0; tables::BlockTransactions::KEY_LEN];
        block_transaction_key[0] = block_id.shard.workchain() as i8 as u8;
        block_transaction_key[1..9].copy_from_slice(&block_id.shard.prefix().to_be_bytes());
        block_transaction_key[9..13].copy_from_slice(&block_id.seqno.to_be_bytes());
        block_transaction_key[13..45].copy_from_slice(account.address.as_slice());
        block_transaction_key[45..53].copy_from_slice(&1u64.to_be_bytes());
        let mut batch = rocksdb::WriteBatch::default();
        batch.delete_cf(&lease.known_blocks.cf(), known_block_key(&block_id));
        batch.delete_cf(&lease.block_transactions.cf(), block_transaction_key);
        batch.delete_cf(&lease.transactions_by_hash.cf(), transaction_hash);
        batch.delete_cf(&lease.transactions_by_in_msg.cf(), inbound_message_hash);
        batch.delete_cf(&lease.transactions.cf(), transaction_key);
        lease
            .rocksdb()
            .write_opt(batch, lease.transactions.write_config())
            .unwrap();
        drop(lease);

        let mut router_batch = rocksdb::WriteBatch::default();
        router_batch.delete_cf(&storage.router.transactions.cf(), transaction_hash);
        router_batch.delete_cf(&storage.router.inbound_messages.cf(), inbound_message_hash);
        storage
            .router
            .rocksdb()
            .write_opt(router_batch, storage.router.transactions.write_config())
            .unwrap();
        assert_eq!(
            storage
                .reconcile_startup(&mc_block_id)
                .unwrap()
                .effective_frontier,
            mc_block_id
        );

        let lease = storage.partitions.lock().active_lease();
        let commit_key = codec::partition_commit_key(mc_block_id.seqno, &mc_block_id.as_short_id());
        let valid_commit = lease.partition_commits.get(commit_key).unwrap().unwrap().to_vec();
        lease.partition_commits.insert(commit_key, [0]).unwrap();
        assert!(format!("{:#}", reconciliation_error(&storage, &mc_block_id))
            .contains("invalid persisted RPC visible frontier"));
        lease.partition_commits.insert(commit_key, &valid_commit).unwrap();
        let mut commit = codec::decode_partition_commit(&valid_commit).unwrap();
        commit.block_id.root_hash = HashBytes([0xff; 32]);
        commit.digest = commit.block_id.root_hash;
        lease
            .partition_commits
            .insert(commit_key, codec::encode_partition_commit(&commit))
            .unwrap();
        assert!(format!("{:#}", reconciliation_error(&storage, &mc_block_id))
            .contains("RPC frontier commit identity mismatch"));
        let mut batch = rocksdb::WriteBatch::default();
        batch.delete_cf(&lease.partition_commits.cf(), commit_key);
        lease
            .rocksdb()
            .write_opt(batch, lease.partition_commits.write_config())
            .unwrap();
        assert!(format!("{:#}", reconciliation_error(&storage, &mc_block_id))
            .contains("missing the RPC frontier commit"));
    }

    #[tokio::test]
    async fn replay_repairs_router_stages_before_and_after_partition_sealing() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let storage = RpcStorage::open(
            context,
            RpcTransactionPartitionsConfig {
                target_lsm_bytes: u64::MAX,
                target_blob_bytes: u64::MAX,
                target_index_records: 1,
                max_open_sealed_partitions: 1,
            },
        )
        .unwrap();
        let subscriptions = super::super::subscriptions::RpcSubscriptions::new(
            SubscriberManagerConfig::new(16, 4),
            4,
        );
        let zerostate = masterchain_block(0);
        let mc_block_id = masterchain_block(1);
        let block = indexed_block();
        let code_hash = HashBytes::from_str(
            "fc42205fe8c1c08846c1222c81eb416bdbf403253f6079691e04d52ce4400f8f",
        )
        .unwrap();
        let code_hash_account = StdAddr::new(
            0,
            HashBytes::from_str("b06c29df56964af1aeb3bbda73ea5685bc54f4131c1c8559ba2c6f971976cd2b")
                .unwrap(),
        );
        let mut code_hash_key = [0; tables::CodeHashes::KEY_LEN];
        code_hash_key[..32].copy_from_slice(code_hash.as_slice());
        code_hash_key[32] = code_hash_account.workchain as u8;
        code_hash_key[33..].copy_from_slice(code_hash_account.address.as_slice());
        let mut code_hash_by_address_key = [0; tables::CodeHashesByAddress::KEY_LEN];
        code_hash_by_address_key[0] = code_hash_account.workchain as u8;
        code_hash_by_address_key[1..].copy_from_slice(code_hash_account.address.as_slice());
        storage.publish_snapshot(&zerostate).unwrap();

        let first = storage
            .update(&mc_block_id, block.clone(), None, &subscriptions)
            .await
            .unwrap();
        assert!(first.newly_committed);
        let old_id = PartitionId(first.partition_id);
        let old_lease = storage.partitions.lock().active_lease();
        let (account, _lt, transaction_hash, inbound_message_hash) =
            first_transaction_with_inbound_message(&old_lease);
        assert!(old_lease
            .transactions_by_hash
            .get(transaction_hash)
            .unwrap()
            .is_some());
        drop(old_lease);
        {
            let manager = storage.partitions.lock();
            insert_masterchain_commit(&manager, &mc_block_id, 0);
        }
        persist_router_commit(&storage, &mc_block_id);
        assert_eq!(storage.load_router_commit().unwrap(), Some(mc_block_id));
        storage.commit_masterchain_block_set(&mc_block_id).unwrap();
        assert!(storage.current_state.code_hashes.get(code_hash_key).unwrap().is_some());
        assert_eq!(
            storage
                .current_state
                .code_hashes_by_address
                .get(code_hash_by_address_key)
                .unwrap()
                .as_deref(),
            Some(code_hash.as_slice())
        );

        {
            let reconciliation = storage.reconcile_startup(&zerostate).unwrap();
            assert_eq!(reconciliation.effective_frontier, zerostate);
            assert!(reconciliation.rebuild_current_state);
        }
        *storage.snapshots.current.write() = None;
        let mut current_state_batch = rocksdb::WriteBatch::default();
        current_state_batch.delete_cf(&storage.current_state.code_hashes.cf(), code_hash_key);
        current_state_batch.delete_cf(
            &storage.current_state.code_hashes_by_address.cf(),
            code_hash_by_address_key,
        );
        storage
            .current_state
            .rocksdb()
            .write_opt(
                current_state_batch,
                storage.current_state.code_hashes.write_config(),
            )
            .unwrap();
        storage.publish_snapshot(&zerostate).unwrap();
        assert_eq!(storage.load_snapshot().unwrap().visible_frontier(), &zerostate);
        assert!(storage
            .get_accounts_by_code_hash(&code_hash, None, None)
            .unwrap()
            .next()
            .is_none());

        delete_router_records(
            &storage,
            &transaction_hash,
            &inbound_message_hash,
            block.id(),
        );
        storage.publish_snapshot(&zerostate).unwrap();
        assert!(storage.get_transaction(&transaction_hash, None).unwrap().is_none());
        assert!(storage
            .get_dst_transaction(&inbound_message_hash, None)
            .unwrap()
            .is_none());
        assert!(storage
            .get_brief_block_info(&block.id().as_short_id(), None)
            .unwrap()
            .is_none());
        assert!(storage
            .get_transactions(&account, None, None, false, None)
            .unwrap()
            .map_ext(|lt, _, _| Some(lt))
            .next()
            .is_none());

        let active_replay = storage
            .update(&mc_block_id, block.clone(), None, &subscriptions)
            .await
            .unwrap();
        assert!(!active_replay.newly_committed);
        assert_eq!(active_replay.partition_id, old_id.0);
        assert_eq!(storage.load_router_commit().unwrap(), Some(mc_block_id));
        assert_eq!(storage.partitions.lock().visible_frontier(), Some(&mc_block_id));
        assert!(storage.current_state.code_hashes.get(code_hash_key).unwrap().is_some());
        assert_eq!(
            storage
                .current_state
                .code_hashes_by_address
                .get(code_hash_by_address_key)
                .unwrap()
                .as_deref(),
            Some(code_hash.as_slice())
        );
        assert!(storage
            .get_accounts_by_code_hash(&code_hash, None, None)
            .unwrap()
            .next()
            .is_none());
        assert!(storage.router.transactions.get(transaction_hash).unwrap().is_some());
        assert!(storage
            .router
            .inbound_messages
            .get(inbound_message_hash)
            .unwrap()
            .is_some());
        assert!(storage
            .router
            .blocks
            .get(codec::encode_short_block_id(&block.id().as_short_id()))
            .unwrap()
            .is_some());
        assert!(storage.get_transaction(&transaction_hash, None).unwrap().is_none());

        {
            let mut manager = storage.partitions.lock();
            insert_masterchain_commit(&manager, &mc_block_id, 0);
            manager.commit_masterchain_block_set(&mc_block_id).unwrap();
        }
        storage.publish_snapshot(&mc_block_id).unwrap();
        assert_eq!(storage.load_snapshot().unwrap().visible_frontier(), &mc_block_id);
        assert_eq!(
            storage
                .get_accounts_by_code_hash(&code_hash, None, None)
                .unwrap()
                .last()
                .unwrap(),
            code_hash_account
        );
        assert_eq!(storage.partitions.lock().active_id(), PartitionId(2));
        let expected_c1 = storage
            .get_transactions(&account, None, None, false, None)
            .unwrap()
            .map_ext(|lt, hash, _| Some((lt, *hash)))
            .collect::<Vec<_>>();
        assert!(!expected_c1.is_empty());
        assert_eq!(
            storage
                .get_transaction(&transaction_hash, None)
                .unwrap()
                .unwrap()
                .tx_hash(),
            transaction_hash
        );
        assert_eq!(
            storage
                .get_dst_transaction(&inbound_message_hash, None)
                .unwrap()
                .unwrap()
                .tx_hash(),
            transaction_hash
        );
        assert_eq!(
            storage
                .get_transaction_info(&transaction_hash, None)
                .unwrap()
                .unwrap()
                .block_id,
            *block.id()
        );
        assert_eq!(
            storage
                .get_brief_block_info(&block.id().as_short_id(), None)
                .unwrap()
                .unwrap()
                .0,
            *block.id()
        );

        delete_router_records(
            &storage,
            &transaction_hash,
            &inbound_message_hash,
            block.id(),
        );
        storage.publish_snapshot(&mc_block_id).unwrap();
        assert!(storage.get_transaction(&transaction_hash, None).unwrap().is_none());
        assert_eq!(
            storage
                .get_transactions(&account, None, None, false, None)
                .unwrap()
                .map_ext(|lt, hash, _| Some((lt, *hash)))
                .collect::<Vec<_>>(),
            expected_c1
        );
        let sealing_replay = storage
            .update(&mc_block_id, block.clone(), None, &subscriptions)
            .await
            .unwrap();
        assert!(!sealing_replay.newly_committed);
        assert_eq!(sealing_replay.partition_id, old_id.0);
        assert_eq!(storage.load_router_commit().unwrap(), Some(mc_block_id));
        storage.publish_snapshot(&mc_block_id).unwrap();
        assert!(storage.get_transaction(&transaction_hash, None).unwrap().is_some());

        storage.sealing_notify.notify_one();
        wait_for_sealed_partition(&storage, old_id).await;
        delete_router_records(
            &storage,
            &transaction_hash,
            &inbound_message_hash,
            block.id(),
        );
        storage.publish_snapshot(&mc_block_id).unwrap();
        assert!(storage.get_transaction(&transaction_hash, None).unwrap().is_none());
        let sealed_replay = storage
            .update(&mc_block_id, block.clone(), None, &subscriptions)
            .await
            .unwrap();
        assert!(!sealed_replay.newly_committed);
        assert_eq!(sealed_replay.partition_id, old_id.0);
        assert_eq!(storage.load_router_commit().unwrap(), Some(mc_block_id));
        storage.publish_snapshot(&mc_block_id).unwrap();

        assert_eq!(
            storage
                .get_transactions(&account, None, None, false, None)
                .unwrap()
                .map_ext(|lt, hash, _| Some((lt, *hash)))
                .collect::<Vec<_>>(),
            expected_c1
        );
        assert_eq!(
            storage
                .get_transaction(&transaction_hash, None)
                .unwrap()
                .unwrap()
                .tx_hash(),
            transaction_hash
        );
        assert_eq!(
            storage
                .get_dst_transaction(&inbound_message_hash, None)
                .unwrap()
                .unwrap()
                .tx_hash(),
            transaction_hash
        );
        assert_eq!(
            storage
                .get_transaction_info(&transaction_hash, None)
                .unwrap()
                .unwrap()
                .block_id,
            *block.id()
        );
        assert!(storage
            .get_brief_block_info(&block.id().as_short_id(), None)
            .unwrap()
            .is_some());
        for location in [
            storage.router.transactions.get(transaction_hash).unwrap().unwrap(),
            storage
                .router
                .inbound_messages
                .get(inbound_message_hash)
                .unwrap()
                .unwrap(),
            storage
                .router
                .blocks
                .get(codec::encode_short_block_id(&block.id().as_short_id()))
                .unwrap()
                .unwrap(),
        ] {
            assert_eq!(
                codec::decode_router_location(location.as_ref()).unwrap(),
                codec::RouterLocation {
                    partition_id: old_id.0,
                    mc_seqno: mc_block_id.seqno,
                }
            );
        }
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
        let (brief_id, brief_mc_seqno, brief) = storage
            .get_brief_block_info(&block1.as_short_id(), None)
            .unwrap()
            .unwrap();
        assert_eq!(brief_id, block1);
        assert_eq!(brief_mc_seqno, 1);
        assert_eq!(brief.tx_count, 2);
        let shards = storage
            .get_brief_shards_descr(1, None)
            .unwrap()
            .unwrap();
        assert_eq!(shards.len(), 1);
        assert_eq!(shards[0].shard_ident, ShardIdent::BASECHAIN);
        assert_eq!(shards[0].seqno, block1.seqno);

        let block_transactions = storage
            .get_block_transactions(&block1.as_short_id(), false, None, None)
            .unwrap()
            .unwrap()
            .map(|_, lt, boc| Some((lt, boc[0])))
            .collect::<Vec<_>>();
        assert_eq!(block_transactions, [(10, 10), (11, 11)]);
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
            }],
            0,
        );
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

        let stale_hash = HashBytes([90; 32]);
        storage
            .router
            .transactions
            .insert(
                stale_hash,
                codec::encode_router_location(codec::RouterLocation {
                    partition_id: second_id.0,
                    mc_seqno: 1,
                }),
            )
            .unwrap();
        storage.publish_snapshot(&mc3).unwrap();
        assert!(storage.get_transaction(&stale_hash, None).is_err());

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
