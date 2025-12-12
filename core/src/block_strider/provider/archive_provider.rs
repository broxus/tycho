use std::collections::{BTreeMap, btree_map};
use std::num::NonZeroU64;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU32, Ordering};
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use bytesize::ByteSize;
use futures_util::future::BoxFuture;
use parking_lot::Mutex;
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
    /// Maximum memory allowed for archives.
    pub max_archive_to_memory_size: ByteSize,

    /// How many of archives to prefetch ahead.
    pub prefetch_archives: usize,

    /// At most how much of archives to prefetch ahead.
    ///
    /// Default: 4 GB
    pub prefetch_memory_soft_limit: ByteSize,
}

impl Default for ArchiveBlockProviderConfig {
    fn default() -> Self {
        Self {
            max_archive_to_memory_size: ByteSize::mb(100),
            prefetch_archives: 10,
            prefetch_memory_soft_limit: ByteSize::gb(4),
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
        let memory = Arc::new(ArchiveMemoryUsage::new(config.max_archive_to_memory_size));

        Self {
            inner: Arc::new(Inner {
                client: client.into_archive_client(),
                proof_checker,
                known_archives: Mutex::new(Default::default()),
                storage,
                memory,
                prefetch_archives: config.prefetch_archives,
                prefetch_memory_soft_limit: config.prefetch_memory_soft_limit.0,
                cleaned_until: AtomicU32::new(0),
            }),
        }
    }

    async fn get_next_block_impl(&self, block_id: &BlockId) -> OptionalBlockStuff {
        let this = self.inner.as_ref();

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
                Ok(block) => return Some(Ok(block.clone())),
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
        let this = self.inner.as_ref();

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
    known_archives: Mutex<ArchivesMap>,
    memory: Arc<ArchiveMemoryUsage>,
    prefetch_archives: usize,
    prefetch_memory_soft_limit: u64,
    cleaned_until: AtomicU32,
}

impl Inner {
    async fn get_archive(&self, mc_seqno: u32) -> Option<(u32, ArchiveInfo)> {
        loop {
            let mut pending = match self.find_downloaded_or_spawn(mc_seqno) {
                // Archive for this seqno was already downloaded.
                Ok(res) => return Some(res),
                // Otherwise wait for the task to complete.
                Err(pending) => pending,
            };

            // Wait until the pending task is finished or cancelled
            let (res, finished) = match pending.wait().await {
                ArchiveTaskState::Finished(state) => (state, true),
                ArchiveTaskState::Cancelled => (None, false),
            };

            // Replace pending with downloaded
            self.set_downloaded(pending.archive_key, res.as_ref());

            // Handle task result.
            match res {
                _ if !finished => {
                    tracing::warn!(mc_seqno, "archive task cancelled while in use");
                }
                Some(info) if !info.archive.mc_block_ids.contains_key(&mc_seqno) => {
                    tracing::warn!(mc_seqno, "waited on an unrelated task");
                }
                None if pending.archive_key > mc_seqno => {
                    tracing::warn!(mc_seqno, "waited on a prefetched block");
                }
                // Return result if finished on a proper task.
                res => return res.map(|info| (pending.archive_key, info)),
            }

            // Avoid spinloop just in case.
            tokio::task::yield_now().await;
        }
    }

    fn find_downloaded_or_spawn(&self, mc_seqno: u32) -> Result<(u32, ArchiveInfo), ArchiveTask> {
        let mut guard = self.known_archives.lock();

        let existing = 'existing: {
            let until_next_archive = mc_seqno.saturating_add(Archive::MAX_MC_BLOCKS_PER_ARCHIVE);

            // Check all downloaded archives that can potentially contain
            // the specified `mc_seqno`.
            for (archive_key, slot) in guard.range(..until_next_archive) {
                match slot {
                    ArchiveSlot::Downloaded(info) => {
                        // Use the first archive which contains the specified `mc_seqno`.
                        if info.archive.mc_block_ids.contains_key(&mc_seqno) {
                            break 'existing (*archive_key, info.clone());
                        }
                    }
                    // For "current" blocks we should download at most
                    // one archive at a time. This task could be related
                    // to the old entry, but we still need to wait it.
                    ArchiveSlot::Pending(task) => return Err(task.clone()),
                }
            }

            // If no pending or downloaded archives found, start
            // downloading a new one for the specified `mc_seqno`.
            let task = self.make_downloader().spawn(mc_seqno);
            guard.insert(mc_seqno, ArchiveSlot::Pending(task.clone()));

            // Wait this task.
            return Err(task);
        };

        // Start prefetching the next archive if needed.
        self.try_prefetch(guard);

        // Use the downloaded existing one.
        Ok(existing)
    }

    fn set_downloaded(&self, archive_key: u32, res: Option<&ArchiveInfo>) {
        let mut guard = self.known_archives.lock();

        match guard.entry(archive_key) {
            // Do nothing if the entry was already removed.
            btree_map::Entry::Vacant(_) => {}
            btree_map::Entry::Occupied(mut entry) => match res {
                // Task was either cancelled or received `TooNew` so no archive received.
                None => {
                    entry.remove();
                }
                // Task was finished with a non-empty result so store it.
                Some(info) => {
                    entry.insert(ArchiveSlot::Downloaded(info.clone()));
                }
            },
        }

        // Duplicate task on the first mc seqno of this archive.
        if let Some(info) = res
            && let Some((first_mc_seqno, _)) = info.archive.mc_block_ids.first_key_value()
            && let btree_map::Entry::Vacant(entry) = guard.entry(*first_mc_seqno)
        {
            entry.insert(ArchiveSlot::Downloaded(info.clone()));
        }

        // Start prefetching the next archive if needed.
        self.try_prefetch(guard);
    }

    fn try_prefetch(&self, _guard: parking_lot::MutexGuard<'_, ArchivesMap>) {
        let Some(preload_since) = self.cleaned_until.load(Ordering::Acquire).checked_add(1) else {
            return;
        };

        if self.memory.used.lock().total >= self.prefetch_memory_soft_limit {
            return;
        }

        let min_archive_key = preload_since.saturating_sub(Archive::MAX_MC_BLOCKS_PER_ARCHIVE);

        // let mut to_prefetch = None;
        // for (_, slot) in guard.range(min_archive_key..) {
        //     match slot {
        //         // Don't preload more than one archive
        //         ArchiveSlot::Pending(_) => return,
        //     }
        // }

        // TODO
    }

    fn remove_archive_if_same(&self, archive_key: u32, prev: &ArchiveInfo) -> bool {
        match self.known_archives.lock().entry(archive_key) {
            btree_map::Entry::Vacant(_) => false,
            btree_map::Entry::Occupied(entry) => {
                if matches!(
                    entry.get(),
                    ArchiveSlot::Downloaded(info)
                    if Arc::ptr_eq(&info.archive, &prev.archive)
                ) {
                    entry.remove();
                    true
                } else {
                    false
                }
            }
        }
    }

    fn make_downloader(&self) -> ArchiveDownloader {
        ArchiveDownloader {
            client: self.client.clone(),
            storage: self.storage.clone(),
            memory: self.memory.clone(),
        }
    }

    fn clear_outdated_archives(&self, bound: u32) {
        let mut entries_remaining = 0usize;
        let mut entries_removed = 0usize;

        let mut guard = self.known_archives.lock();
        guard.retain(|_, archive| {
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
        drop(guard);

        tracing::debug!(
            entries_remaining,
            entries_removed,
            bound,
            "removed known archives"
        );

        self.cleaned_until.fetch_max(bound, Ordering::Release);
    }
}

type ArchivesMap = BTreeMap<u32, ArchiveSlot>;

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
    memory: Arc<ArchiveMemoryUsage>,
}

impl ArchiveDownloader {
    fn spawn(self, mc_seqno: u32) -> ArchiveTask {
        // TODO: Use a proper backoff here?
        const INTERVAL: Duration = Duration::from_secs(1);

        let (tx, rx) = watch::channel(None);

        let guard = scopeguard::guard(tx, move |tx| {
            tracing::warn!(mc_seqno, "cancelled preloading archive");
            tx.send_modify(|prev| {
                if !matches!(prev, Some(ArchiveTaskState::Finished(..))) {
                    *prev = Some(ArchiveTaskState::Cancelled);
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
                        tx.send_modify(move |prev| *prev = Some(ArchiveTaskState::Finished(res)));
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
            memory: self.memory.clone(),
        };
        let Some(found) = self.client.find_archive(mc_seqno, ctx).await? else {
            return Ok(None);
        };
        let res = (found.download)().await?;

        let span = tracing::Span::current();
        tokio::task::spawn_blocking(move || {
            let _span = span.enter();

            let (bytes, neighbour) = res.finalize()?;

            let archive = match Archive::new(bytes) {
                Ok(array) => array,
                Err(e) => {
                    if let Some(neighbour) = neighbour {
                        neighbour.punish(PunishReason::Malicious);
                    }
                    return Err(e);
                }
            };

            if let Err(e) = archive.check_mc_blocks_range() {
                // TODO: Punish a bit less for missing mc blocks?
                if let Some(neighbour) = neighbour {
                    neighbour.punish(PunishReason::Malicious);
                }
                return Err(e);
            }

            Ok(ArchiveInfo {
                archive: Arc::new(archive),
                from: neighbour,
            })
        })
        .await?
        .map(Some)
    }
}

#[derive(Clone)]
struct ArchiveTask {
    archive_key: u32,
    rx: watch::Receiver<Option<ArchiveTaskState>>,
    abort_handle: Arc<AbortOnDrop>,
}

impl ArchiveTask {
    async fn wait(&mut self) -> ArchiveTaskState {
        loop {
            if let Some(state) = &*self.rx.borrow_and_update() {
                break state.clone();
            } else if self.rx.changed().await.is_err() {
                break ArchiveTaskState::Cancelled;
            }
        }
    }
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

#[derive(Clone)]
enum ArchiveTaskState {
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
    pub alloc: ArchiveMemoryAllocation,
    pub writer: TargetWriter,
    pub neighbour: Option<Neighbour>,
}

impl ArchiveResponse {
    pub fn finalize(self) -> Result<(Bytes, Option<Neighbour>)> {
        struct BytesWithAlloc {
            inner: Bytes,
            _alloc: ArchiveMemoryAllocation,
        }

        impl AsRef<[u8]> for BytesWithAlloc {
            #[inline]
            fn as_ref(&self) -> &[u8] {
                self.inner.as_ref()
            }
        }

        // NOTE: Archive will hold a reference to these bytes and
        // this allocation so we will be able to precisely track the memory usage.
        let bytes = Bytes::from_owner(BytesWithAlloc {
            _alloc: self.alloc,
            inner: self.writer.try_freeze()?,
        });
        Ok((bytes, self.neighbour))
    }
}

#[derive(Clone)]
pub struct ArchiveDownloadContext<'a> {
    pub storage: &'a CoreStorage,
    pub memory: Arc<ArchiveMemoryUsage>,
}

impl<'a> ArchiveDownloadContext<'a> {
    pub fn get_archive_writer(
        &self,
        size: NonZeroU64,
    ) -> Result<(ArchiveMemoryAllocation, TargetWriter)> {
        let alloc = self.memory.alloc(size);
        let writer = if alloc.use_disk() {
            let file = self.storage.context().temp_files().unnamed_file().open()?;
            TargetWriter::File(std::io::BufWriter::new(file))
        } else {
            TargetWriter::Bytes(BytesMut::new().writer())
        };
        Ok((alloc, writer))
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

                        let (alloc, output) = ctx.get_archive_writer(found.size)?;
                        let writer = self.download_archive(found, output).await?;

                        Ok(ArchiveResponse {
                            alloc,
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
                    let (alloc, output) = ctx.get_archive_writer(info.size)?;
                    let writer = self.download_archive(info.archive_id, output).await?;

                    Ok(ArchiveResponse {
                        alloc,
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
                    let res = self.primary.find_archive(mc_seqno, ctx.clone()).await;
                    if matches!(&res, Ok(Some(_))) {
                        return res;
                    }
                }
                HybridArchiveClientPart::Secondary => {
                    let res = self.secondary.find_archive(mc_seqno, ctx.clone()).await;
                    if matches!(&res, Ok(Some(_))) {
                        return res;
                    }
                }
            }
        }

        self.prefer.set(None);

        let primary = pin!(self.primary.find_archive(mc_seqno, ctx.clone()));
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

// === Memory management stuff ===

pub struct ArchiveMemoryUsage {
    memory_threshold: u64,
    used: Mutex<MemoryUsageState>,
}

impl ArchiveMemoryUsage {
    fn new(memory_threshold: ByteSize) -> Self {
        Self {
            memory_threshold: memory_threshold.0,
            used: Default::default(),
        }
    }

    pub fn alloc(self: &Arc<Self>, size: NonZeroU64) -> ArchiveMemoryAllocation {
        let mut guard = self.used.lock();
        let size = size.get();
        guard.total += size;
        let use_disk = if guard.memory + size > self.memory_threshold {
            true
        } else {
            guard.memory += size;
            false
        };
        drop(guard);

        ArchiveMemoryAllocation {
            size,
            use_disk,
            shared: self.clone(),
        }
    }
}

#[derive(Default)]
struct MemoryUsageState {
    total: u64,
    memory: u64,
}

pub struct ArchiveMemoryAllocation {
    size: u64,
    use_disk: bool,
    shared: Arc<ArchiveMemoryUsage>,
}

impl ArchiveMemoryAllocation {
    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn use_disk(&self) -> bool {
        self.use_disk
    }
}

impl Drop for ArchiveMemoryAllocation {
    fn drop(&mut self) {
        let mut guard = self.shared.used.lock();
        guard.total -= self.size;
        if !self.use_disk {
            guard.memory -= self.size;
        }
    }
}
