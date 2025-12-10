use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::num::NonZeroU64;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use bytes::{BufMut, BytesMut};
use bytesize::ByteSize;
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio::task::AbortHandle;
use tycho_block_util::archive::Archive;
use tycho_block_util::block::{BlockIdRelation, BlockStuffAug};
use tycho_types::models::BlockId;
use tycho_util::fs::TargetWriter;

use crate::block_strider::provider::{BlockProvider, CheckProof, OptionalBlockStuff, ProofChecker};
use crate::blockchain_rpc;
use crate::blockchain_rpc::BlockchainRpcClient;
use crate::overlay_client::{Neighbour, PunishReason};
#[cfg(feature = "s3")]
use crate::s3::S3Client;
use crate::storage::CoreStorage;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ArchiveBlockProviderConfig {
    pub max_archive_to_memory_size: ByteSize,

    /// Number of archives to prefetch ahead. None disables prefetching.
    pub num_prefetched_archives: Option<usize>,
}

impl Default for ArchiveBlockProviderConfig {
    fn default() -> Self {
        Self {
            max_archive_to_memory_size: ByteSize::mb(100),
            num_prefetched_archives: Some(10),
        }
    }
}

#[derive(Clone)]
#[repr(transparent)]
pub struct ArchiveBlockProvider {
    inner: Arc<Inner>,
}

impl ArchiveBlockProvider {
    pub fn new(
        client: impl IntoArchiveClient,
        storage: CoreStorage,
        config: ArchiveBlockProviderConfig,
    ) -> Self {
        let proof_checker = ProofChecker::new(storage.clone());

        let archives_manager = ArchivesManager::new();

        let sync_tracker = SyncTracker::new(Archive::MAX_MC_BLOCKS_PER_ARCHIVE as usize);

        Self {
            inner: Arc::new(Inner {
                client: client.into_archive_client(),
                archives: archives_manager,
                proof_checker,
                sync_tracker,
                storage,
                config,
            }),
        }
    }

    async fn get_next_block_impl(&self, block_id: &BlockId) -> OptionalBlockStuff {
        let this = &self.inner;

        let next_mc_seqno = block_id.seqno + 1;

        loop {
            let Some((archive_key, info)) = this.get_archive(next_mc_seqno).await else {
                tracing::warn!(prev_block_id = ?block_id, "archive not found");
                break None;
            };

            let Some(block_id) = info.archive.mc_block_ids.get(&next_mc_seqno) else {
                tracing::error!(
                    "received archive does not contain mc block with seqno {next_mc_seqno}"
                );
                this.remove_archive_if_same(archive_key, &info);
                if let Some(from) = &info.from {
                    from.punish(PunishReason::Malicious);
                }
                continue;
            };

            match self
                .checked_get_entry_by_id(&info.archive, block_id, block_id)
                .await
            {
                Ok(block) => {
                    let block_time = block.data.load_info().ok()?.gen_utime;
                    self.inner.sync_tracker.log(block.id().seqno, block_time);

                    return Some(Ok(block.clone()));
                }
                Err(e) => {
                    tracing::error!(archive_key, %block_id, "invalid archive entry: {e:?}");
                    this.remove_archive_if_same(archive_key, &info);
                    if let Some(from) = &info.from {
                        from.punish(PunishReason::Malicious);
                    }
                }
            }
        }
    }

    async fn get_block_impl(&self, block_id_relation: &BlockIdRelation) -> OptionalBlockStuff {
        let this = &self.inner;

        let block_id = block_id_relation.block_id;
        let mc_block_id = block_id_relation.mc_block_id;

        loop {
            let Some((archive_key, info)) = this.get_archive(mc_block_id.seqno).await else {
                tracing::warn!("shard block is too new for archives");

                // NOTE: This is a strange situation, but if we wait a bit it might go away.
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            };

            match self
                .checked_get_entry_by_id(&info.archive, &mc_block_id, &block_id)
                .await
            {
                Ok(block) => return Some(Ok(block.clone())),
                Err(e) => {
                    tracing::error!(archive_key, %block_id, %mc_block_id, "invalid archive entry: {e:?}");
                    this.remove_archive_if_same(archive_key, &info);
                    if let Some(from) = &info.from {
                        from.punish(PunishReason::Malicious);
                    }
                }
            }
        }
    }

    async fn checked_get_entry_by_id(
        &self,
        archive: &Arc<Archive>,
        mc_block_id: &BlockId,
        block_id: &BlockId,
    ) -> Result<BlockStuffAug> {
        let (block, ref proof, ref queue_diff) = match archive.get_entry_by_id(block_id).await {
            Ok(entry) => entry,
            Err(e) => anyhow::bail!("archive is corrupted: {e:?}"),
        };

        self.inner
            .proof_checker
            .check_proof(CheckProof {
                mc_block_id,
                block: &block,
                proof,
                queue_diff,
                store_on_success: true,
            })
            .await?;

        Ok(block)
    }
}

struct Inner {
    storage: CoreStorage,

    client: Arc<dyn ArchiveClient>,
    proof_checker: ProofChecker,

    archives: ArchivesManager,

    sync_tracker: SyncTracker,

    config: ArchiveBlockProviderConfig,
}

impl Inner {
    async fn get_archive(self: &Arc<Self>, mc_seqno: u32) -> Option<(u32, ArchiveInfo)> {
        loop {
            let mut pending = 'pending: {
                // Search for the downloaded archive or for and existing downloader task.
                for (archive_key, value) in self.archives.iter() {
                    match value {
                        ArchiveSlot::Downloaded(info) => {
                            if info.archive.mc_block_ids.contains_key(&mc_seqno) {
                                // Prefetch next archives if enabled
                                if let Some(num_prefetched_archives) =
                                    self.config.num_prefetched_archives
                                    && let Some((last_seqno, _)) =
                                        info.archive.mc_block_ids.last_key_value()
                                    && self.archives.try_prefetch()
                                {
                                    let this = self.clone();
                                    let next_archive_start_seqno = last_seqno.saturating_add(1);

                                    tokio::spawn(async move {
                                        this.try_prefetch_archives(
                                            next_archive_start_seqno,
                                            num_prefetched_archives,
                                        )
                                        .await;
                                    });
                                }
                                return Some((archive_key, info.clone()));
                            }
                        }
                        ArchiveSlot::Pending(task) => break 'pending task.clone(),
                    }
                }

                // Start downloading otherwise
                let task = self.make_downloader().spawn(mc_seqno);
                self.archives
                    .insert(mc_seqno, ArchiveSlot::Pending(task.clone()));

                task
            };

            // Wait until the pending task is finished or cancelled
            let mut res = None;
            let mut finished = false;
            loop {
                match &*pending.rx.borrow_and_update() {
                    ArchiveTaskState::None => {}
                    ArchiveTaskState::Finished(archive) => {
                        res = archive.clone();
                        finished = true;
                        break;
                    }
                    ArchiveTaskState::Cancelled => break,
                }
                if pending.rx.changed().await.is_err() {
                    break;
                }
            }

            // Replace pending with downloaded
            match self.archives.get(&pending.archive_key) {
                Some(ArchiveSlot::Pending(_)) => match &res {
                    None => {
                        // Task was either cancelled or received `TooNew` so no archive received.
                        self.archives.remove(&pending.archive_key);
                    }
                    Some(info) => {
                        // Task was finished with a non-empty result so store it.
                        self.archives
                            .insert(pending.archive_key, ArchiveSlot::Downloaded(info.clone()));
                    }
                },
                _ => {
                    // Do nothing if the entry was already removed or replaced.
                }
            }

            if finished {
                // Prefetch the next archive if enabled
                if let Some(num_prefetched_archives) = self.config.num_prefetched_archives
                    && let Some((last_seqno, _)) = res
                        .as_ref()
                        .and_then(|i| i.archive.mc_block_ids.last_key_value())
                    && self.archives.try_prefetch()
                {
                    let this = self.clone();
                    let next_archive_start_seqno = last_seqno.saturating_add(1);

                    tokio::spawn(async move {
                        this.try_prefetch_archives(
                            next_archive_start_seqno,
                            num_prefetched_archives,
                        )
                        .await;
                    });
                }
                return res.map(|info| (pending.archive_key, info));
            }

            tracing::warn!(mc_seqno, "archive task cancelled while in use");
            // Avoid spinloop just in case.
            tokio::task::yield_now().await;
        }
    }

    fn remove_archive_if_same(&self, archive_key: u32, prev: &ArchiveInfo) -> bool {
        match self.archives.get(&archive_key) {
            Some(ArchiveSlot::Downloaded(info)) if Arc::ptr_eq(&info.archive, &prev.archive) => {
                self.archives.remove(&archive_key);
                true
            }
            _ => false,
        }
    }

    /// Try to prefetch the archives starting from `prefetch_seqno` if not already present or pending.
    async fn try_prefetch_archives(self: &Arc<Self>, prefetch_seqno: u32, archives_limit: usize) {
        scopeguard::defer! {
            self.archives.release_prefetch();
        }

        let mut next_seqno = prefetch_seqno;

        while archives_limit > self.archives.len() {
            let mut task = match self.archives.get(&next_seqno) {
                Some(ArchiveSlot::Downloaded(info)) => {
                    if let Some((last_seqno, _)) = info.archive.mc_block_ids.last_key_value() {
                        next_seqno = last_seqno.saturating_add(1);
                        continue;
                    }
                    return;
                }
                Some(ArchiveSlot::Pending(_)) => {
                    return;
                }
                None => self.make_downloader().spawn(next_seqno),
            };

            // Abort a new task if one with the same archive_id already exists
            if let Some(ArchiveSlot::Pending(task)) = self
                .archives
                .try_insert(next_seqno, ArchiveSlot::Pending(task.clone()))
            {
                task.abort_handle.abort();
            }

            // Wait until the pending task is finished or cancelled
            let mut res = None;
            loop {
                match &*task.rx.borrow_and_update() {
                    ArchiveTaskState::None => {}
                    ArchiveTaskState::Finished(archive) => {
                        res = archive.clone();
                        break;
                    }
                    ArchiveTaskState::Cancelled => break,
                }
                if task.rx.changed().await.is_err() {
                    break;
                }
            }

            // Replace pending with downloaded
            match self.archives.get(&task.archive_key) {
                Some(ArchiveSlot::Pending(_)) => match &res {
                    None => {
                        // Task was either cancelled or received `TooNew` so no archive received.
                        self.archives.remove(&task.archive_key);
                        return;
                    }
                    Some(info) => {
                        // Task was finished with a non-empty result so store it.
                        self.archives
                            .insert(task.archive_key, ArchiveSlot::Downloaded(info.clone()));

                        if let Some((last_seqno, _)) = info.archive.mc_block_ids.last_key_value() {
                            next_seqno = last_seqno.saturating_add(1);
                            continue;
                        }

                        break;
                    }
                },
                _ => {
                    // Break the prefetch process if the entry was already removed or replaced.
                    break;
                }
            }
        }
    }

    fn make_downloader(&self) -> ArchiveDownloader {
        ArchiveDownloader {
            client: self.client.clone(),
            storage: self.storage.clone(),
            memory_threshold: self.config.max_archive_to_memory_size,
        }
    }

    fn clear_outdated_archives(&self, bound: u32) {
        let mut entries_remaining = 0usize;
        let mut entries_removed = 0usize;

        self.archives.retain(|_, archive| {
            let retain;
            match archive {
                ArchiveSlot::Downloaded(info) => match info.archive.mc_block_ids.last_key_value() {
                    None => retain = false,
                    Some((last_mc_seqno, _)) => retain = *last_mc_seqno >= bound,
                },
                ArchiveSlot::Pending(task) => {
                    retain = task
                        .archive_key
                        .saturating_add(Archive::MAX_MC_BLOCKS_PER_ARCHIVE)
                        >= bound;
                    if !retain {
                        task.abort_handle.abort();
                    }
                }
            };

            entries_remaining += retain as usize;
            entries_removed += !retain as usize;
            retain
        });

        tracing::debug!(
            entries_remaining,
            entries_removed,
            bound,
            "removed known archives"
        );
    }
}

type ArchivesMap = BTreeMap<u32, ArchiveSlot>;

struct ArchivesManager {
    map: parking_lot::Mutex<ArchivesMap>,
    prefetch_busy: AtomicBool,
}

impl ArchivesManager {
    fn new() -> Self {
        Self {
            map: parking_lot::Mutex::new(BTreeMap::new()),
            prefetch_busy: AtomicBool::new(false),
        }
    }

    fn len(&self) -> usize {
        self.map.lock().len()
    }

    fn insert(&self, key: u32, value: ArchiveSlot) -> Option<ArchiveSlot> {
        self.map.lock().insert(key, value)
    }

    // Try to insert a new entry.
    // Returns the new entry if it wasn't replaced.
    fn try_insert(&self, key: u32, value: ArchiveSlot) -> Option<ArchiveSlot> {
        match self.map.lock().entry(key) {
            Entry::Vacant(entry) => {
                entry.insert(value);
                None
            }
            Entry::Occupied(_) => Some(value),
        }
    }

    fn remove(&self, key: &u32) -> Option<ArchiveSlot> {
        self.map.lock().remove(key)
    }

    fn get(&self, key: &u32) -> Option<ArchiveSlot> {
        self.map.lock().get(key).cloned()
    }

    fn iter(&self) -> Vec<(u32, ArchiveSlot)> {
        self.map
            .lock()
            .iter()
            .map(|(k, v)| (*k, v.clone()))
            .collect()
    }

    fn retain<F>(&self, mut f: F)
    where
        F: FnMut(&u32, &mut ArchiveSlot) -> bool,
    {
        self.map.lock().retain(|k, v| f(k, v));
    }

    fn try_prefetch(&self) -> bool {
        self.prefetch_busy
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
    }

    fn release_prefetch(&self) {
        self.prefetch_busy.store(false, Ordering::Release);
    }
}

#[derive(Clone)]
enum ArchiveSlot {
    Downloaded(ArchiveInfo),
    Pending(ArchiveTask),
}

#[derive(Clone)]
struct ArchiveInfo {
    from: Option<Neighbour>, // None for S3
    archive: Arc<Archive>,
}

struct ArchiveDownloader {
    client: Arc<dyn ArchiveClient>,
    storage: CoreStorage,
    memory_threshold: ByteSize,
}

impl ArchiveDownloader {
    fn spawn(self, mc_seqno: u32) -> ArchiveTask {
        // TODO: Use a proper backoff here?
        const INTERVAL: Duration = Duration::from_secs(1);

        let (tx, rx) = watch::channel(ArchiveTaskState::None);

        let guard = scopeguard::guard(tx, move |tx| {
            tracing::warn!(mc_seqno, "cancelled preloading archive");
            tx.send_modify(|prev| {
                if !matches!(prev, ArchiveTaskState::Finished(..)) {
                    *prev = ArchiveTaskState::Cancelled;
                }
            });
        });

        // NOTE: Use a separate downloader to prevent reference cycles
        let handle = tokio::spawn(async move {
            tracing::debug!(mc_seqno, "started preloading archive");
            scopeguard::defer! {
                tracing::debug!(mc_seqno, "finished preloading archive");
            }

            loop {
                match self.try_download(mc_seqno).await {
                    Ok(res) => {
                        let tx = scopeguard::ScopeGuard::into_inner(guard);
                        tx.send_modify(move |prev| *prev = ArchiveTaskState::Finished(res));
                        break;
                    }
                    Err(e) => {
                        tracing::error!(mc_seqno, "failed to preload archive {e:?}");
                        tokio::time::sleep(INTERVAL).await;
                    }
                }
            }
        });

        ArchiveTask {
            archive_key: mc_seqno,
            rx,
            abort_handle: Arc::new(AbortOnDrop(handle.abort_handle())),
        }
    }

    async fn try_download(&self, mc_seqno: u32) -> Result<Option<ArchiveInfo>> {
        let ctx = ArchiveDownloadContext {
            storage: &self.storage,
            memory_threshold: self.memory_threshold,
        };
        let Some(found) = self.client.find_archive(mc_seqno, ctx).await? else {
            return Ok(None);
        };
        let res = (found.download)().await?;

        let span = tracing::Span::current();
        tokio::task::spawn_blocking(move || {
            let _span = span.enter();

            let bytes = res.writer.try_freeze()?;

            let archive = match Archive::new(bytes) {
                Ok(array) => array,
                Err(e) => {
                    if let Some(neighbour) = res.neighbour {
                        neighbour.punish(PunishReason::Malicious);
                    }
                    return Err(e);
                }
            };

            if let Err(e) = archive.check_mc_blocks_range() {
                // TODO: Punish a bit less for missing mc blocks?
                if let Some(neighbour) = res.neighbour {
                    neighbour.punish(PunishReason::Malicious);
                }
                return Err(e);
            }

            Ok(ArchiveInfo {
                archive: Arc::new(archive),
                from: res.neighbour,
            })
        })
        .await?
        .map(Some)
    }
}

#[derive(Clone)]
struct ArchiveTask {
    archive_key: u32,
    rx: watch::Receiver<ArchiveTaskState>,
    abort_handle: Arc<AbortOnDrop>,
}

#[repr(transparent)]
struct AbortOnDrop(AbortHandle);

impl std::ops::Deref for AbortOnDrop {
    type Target = AbortHandle;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[derive(Default)]
enum ArchiveTaskState {
    #[default]
    None,
    Finished(Option<ArchiveInfo>),
    Cancelled,
}

impl BlockProvider for ArchiveBlockProvider {
    type GetNextBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;
    type GetBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;
    type CleanupFut<'a> = futures_util::future::Ready<Result<()>>;

    fn get_next_block<'a>(&'a self, prev_block_id: &'a BlockId) -> Self::GetNextBlockFut<'a> {
        Box::pin(self.get_next_block_impl(prev_block_id))
    }

    fn get_block<'a>(&'a self, block_id_relation: &'a BlockIdRelation) -> Self::GetBlockFut<'a> {
        Box::pin(self.get_block_impl(block_id_relation))
    }

    fn cleanup_until(&self, mc_seqno: u32) -> Self::CleanupFut<'_> {
        self.inner.clear_outdated_archives(mc_seqno);
        futures_util::future::ready(Ok(()))
    }
}

pub struct ArchiveResponse {
    writer: TargetWriter,
    neighbour: Option<Neighbour>,
}

#[derive(Clone, Copy)]
pub struct ArchiveDownloadContext<'a> {
    pub storage: &'a CoreStorage,
    pub memory_threshold: ByteSize,
}

impl<'a> ArchiveDownloadContext<'a> {
    pub fn get_archive_writer(&self, size: NonZeroU64) -> Result<TargetWriter> {
        Ok(if size.get() > self.memory_threshold.as_u64() {
            let file = self.storage.context().temp_files().unnamed_file().open()?;
            TargetWriter::File(std::io::BufWriter::new(file))
        } else {
            TargetWriter::Bytes(BytesMut::new().writer())
        })
    }

    pub fn estimate_archive_id(&self, mc_seqno: u32) -> u32 {
        self.storage.block_storage().estimate_archive_id(mc_seqno)
    }
}

pub trait IntoArchiveClient {
    fn into_archive_client(self) -> Arc<dyn ArchiveClient>;
}

impl<T: ArchiveClient> IntoArchiveClient for (T,) {
    #[inline]
    fn into_archive_client(self) -> Arc<dyn ArchiveClient> {
        Arc::new(self.0)
    }
}

impl<T1: ArchiveClient, T2: ArchiveClient> IntoArchiveClient for (T1, Option<T2>) {
    fn into_archive_client(self) -> Arc<dyn ArchiveClient> {
        let (primary, secondary) = self;
        match secondary {
            None => Arc::new(primary),
            Some(secondary) => Arc::new(HybridArchiveClient::new(primary, secondary)),
        }
    }
}

pub struct FoundArchive<'a> {
    pub archive_id: u64,
    pub download: Box<dyn FnOnce() -> BoxFuture<'a, Result<ArchiveResponse>> + Send + 'a>,
}

#[async_trait]
pub trait ArchiveClient: Send + Sync + 'static {
    async fn find_archive<'a>(
        &'a self,
        mc_seqno: u32,
        ctx: ArchiveDownloadContext<'a>,
    ) -> Result<Option<FoundArchive<'a>>>;
}

#[async_trait]
impl ArchiveClient for BlockchainRpcClient {
    async fn find_archive<'a>(
        &'a self,
        mc_seqno: u32,
        ctx: ArchiveDownloadContext<'a>,
    ) -> Result<Option<FoundArchive<'a>>> {
        Ok(match self.find_archive(mc_seqno).await? {
            blockchain_rpc::PendingArchiveResponse::Found(found) => Some(FoundArchive {
                archive_id: found.id,
                download: Box::new(move || {
                    Box::pin(async move {
                        let neighbour = found.neighbour.clone();

                        let output = ctx.get_archive_writer(found.size)?;
                        let writer = self.download_archive(found, output).await?;

                        Ok(ArchiveResponse {
                            writer,
                            neighbour: Some(neighbour),
                        })
                    })
                }),
            }),
            blockchain_rpc::PendingArchiveResponse::TooNew => None,
        })
    }
}

impl IntoArchiveClient for BlockchainRpcClient {
    #[inline]
    fn into_archive_client(self) -> Arc<dyn ArchiveClient> {
        Arc::new(self)
    }
}

#[cfg(feature = "s3")]
#[async_trait]
impl ArchiveClient for S3Client {
    async fn find_archive<'a>(
        &'a self,
        mc_seqno: u32,
        ctx: ArchiveDownloadContext<'a>,
    ) -> Result<Option<FoundArchive<'a>>> {
        let archive_id = ctx.estimate_archive_id(mc_seqno);

        let Some(info) = self.get_archive_info(archive_id).await? else {
            return Ok(None);
        };

        Ok(Some(FoundArchive {
            archive_id: info.archive_id as u64,
            download: Box::new(move || {
                Box::pin(async move {
                    let output = ctx.get_archive_writer(info.size)?;
                    let writer = self.download_archive(info.archive_id, output).await?;

                    Ok(ArchiveResponse {
                        writer,
                        neighbour: None,
                    })
                })
            }),
        }))
    }
}

#[cfg(feature = "s3")]
impl IntoArchiveClient for S3Client {
    #[inline]
    fn into_archive_client(self) -> Arc<dyn ArchiveClient> {
        Arc::new(self)
    }
}

pub struct HybridArchiveClient<T1, T2> {
    primary: T1,
    secondary: T2,
    prefer: HybridArchiveClientState,
}

impl<T1, T2> HybridArchiveClient<T1, T2> {
    pub fn new(primary: T1, secondary: T2) -> Self {
        Self {
            primary,
            secondary,
            prefer: Default::default(),
        }
    }
}

#[async_trait]
impl<T1, T2> ArchiveClient for HybridArchiveClient<T1, T2>
where
    T1: ArchiveClient,
    T2: ArchiveClient,
{
    async fn find_archive<'a>(
        &'a self,
        mc_seqno: u32,
        ctx: ArchiveDownloadContext<'a>,
    ) -> Result<Option<FoundArchive<'a>>> {
        // FIXME: There should be a better way of writing this.

        if let Some(prefer) = self.prefer.get() {
            tracing::debug!(mc_seqno, ?prefer);
            match prefer {
                HybridArchiveClientPart::Primary => {
                    let res = self.primary.find_archive(mc_seqno, ctx).await;
                    if matches!(&res, Ok(Some(_))) {
                        return res;
                    }
                }
                HybridArchiveClientPart::Secondary => {
                    let res = self.secondary.find_archive(mc_seqno, ctx).await;
                    if matches!(&res, Ok(Some(_))) {
                        return res;
                    }
                }
            }
        }

        self.prefer.set(None);

        let primary = pin!(self.primary.find_archive(mc_seqno, ctx));
        let secondary = pin!(self.secondary.find_archive(mc_seqno, ctx));
        match futures_util::future::select(primary, secondary).await {
            futures_util::future::Either::Left((found, other)) => {
                match found {
                    Ok(Some(found)) => {
                        self.prefer.set(Some(HybridArchiveClientPart::Primary));
                        return Ok(Some(found));
                    }
                    Ok(None) => {}
                    Err(e) => tracing::warn!("primary archive client error: {e:?}"),
                }
                other.await.inspect(|res| {
                    if res.is_some() {
                        self.prefer.set(Some(HybridArchiveClientPart::Secondary));
                    }
                })
            }
            futures_util::future::Either::Right((found, other)) => {
                match found {
                    Ok(Some(found)) => {
                        self.prefer.set(Some(HybridArchiveClientPart::Secondary));
                        return Ok(Some(found));
                    }
                    Ok(None) => {}
                    Err(e) => tracing::warn!("secondary archive client error: {e:?}"),
                }
                other.await.inspect(|res| {
                    if res.is_some() {
                        self.prefer.set(Some(HybridArchiveClientPart::Primary));
                    }
                })
            }
        }
    }
}

#[derive(Default)]
struct HybridArchiveClientState(AtomicU8);

impl HybridArchiveClientState {
    fn get(&self) -> Option<HybridArchiveClientPart> {
        match self.0.load(Ordering::Acquire) {
            1 => Some(HybridArchiveClientPart::Primary),
            2 => Some(HybridArchiveClientPart::Secondary),
            _ => None,
        }
    }

    fn set(&self, value: Option<HybridArchiveClientPart>) {
        let value = match value {
            None => 0,
            Some(HybridArchiveClientPart::Primary) => 1,
            Some(HybridArchiveClientPart::Secondary) => 2,
        };
        self.0.store(value, Ordering::Release);
    }
}

#[derive(Debug, Clone, Copy)]
enum HybridArchiveClientPart {
    Primary,
    Secondary,
}

impl std::ops::Not for HybridArchiveClientPart {
    type Output = Self;

    #[inline]
    fn not(self) -> Self::Output {
        match self {
            Self::Primary => Self::Secondary,
            Self::Secondary => Self::Primary,
        }
    }
}

struct SyncTracker {
    inner: parking_lot::Mutex<SyncTrackerInner>,
}

struct SyncTrackerInner {
    window: std::collections::VecDeque<(u32, u32)>, // (block time, insert time)
    window_size: usize,
}

impl SyncTracker {
    fn new(window_size: usize) -> Self {
        Self {
            inner: parking_lot::Mutex::new(SyncTrackerInner {
                window: std::collections::VecDeque::with_capacity(window_size),
                window_size,
            }),
        }
    }

    fn log(&self, seqno: u32, block_time: u32) {
        let mut inner = self.inner.lock();

        let now = tycho_util::time::now_sec();

        inner.window.push_back((block_time, now));

        if inner.window.len() > inner.window_size {
            inner.window.pop_front();
        }

        let Some((first_block_time, first_insert_time)) = inner.window.front() else {
            return;
        };

        let Some((last_block_time, last_insert_time)) = inner.window.back() else {
            return;
        };

        if first_block_time >= last_block_time || first_insert_time >= last_insert_time {
            return;
        }

        let lag_secs = now.saturating_sub(*last_block_time);

        let sync_rate = (last_block_time - first_block_time) as f64
            / (last_insert_time - first_insert_time) as f64;

        let eta_secs = (lag_secs as f64 / sync_rate) as u64;

        fn format_duration(secs: u64) -> String {
            if secs < 60 {
                return format!("{}s", secs);
            }
            if secs < 3600 {
                return format!("{}m", secs / 60);
            }
            format!("{}h{}m", secs / 3600, (secs % 3600) / 60)
        }

        tracing::info!(
            seqno,
            lag = format_duration(lag_secs as u64),
            rate = format!("{:.2}x", sync_rate),
            eta = format_duration(eta_secs),
            "sync progress"
        );
    }
}
