use std::collections::{BTreeMap, btree_map};
use std::io::Seek;
use std::num::NonZeroU64;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use bytesize::ByteSize;
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio::task::AbortHandle;
use tycho_block_util::archive::Archive;
use tycho_block_util::block::{BlockIdRelation, BlockStuffAug};
use tycho_storage::fs::MappedFile;
use tycho_types::models::BlockId;

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
}

impl Default for ArchiveBlockProviderConfig {
    fn default() -> Self {
        Self {
            max_archive_to_memory_size: ByteSize::mb(100),
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

        Self {
            inner: Arc::new(Inner {
                client: client.into_archive_client(),
                proof_checker,

                known_archives: parking_lot::Mutex::new(Default::default()),

                storage,
                config,
            }),
        }
    }

    async fn get_next_block_impl(&self, block_id: &BlockId) -> OptionalBlockStuff {
        let this = self.inner.as_ref();

        let next_mc_seqno = block_id.seqno + 1;

        loop {
            let Some((archive_key, info)) = this.get_archive(next_mc_seqno).await else {
                tracing::info!(mc_seqno = next_mc_seqno, "archive block provider finished");
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

    known_archives: parking_lot::Mutex<ArchivesMap>,

    config: ArchiveBlockProviderConfig,
}

impl Inner {
    async fn get_archive(&self, mc_seqno: u32) -> Option<(u32, ArchiveInfo)> {
        loop {
            let mut pending = 'pending: {
                let mut guard = self.known_archives.lock();

                // Search for the downloaded archive or for and existing downloader task.
                for (archive_key, value) in guard.iter() {
                    match value {
                        ArchiveSlot::Downloaded(info) => {
                            if info.archive.mc_block_ids.contains_key(&mc_seqno) {
                                return Some((*archive_key, info.clone()));
                            }
                        }
                        ArchiveSlot::Pending(task) => break 'pending task.clone(),
                    }
                }

                // Start downloading otherwise
                let task = self.make_downloader().spawn(mc_seqno);
                guard.insert(mc_seqno, ArchiveSlot::Pending(task.clone()));

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
            match self.known_archives.lock().entry(pending.archive_key) {
                btree_map::Entry::Vacant(_) => {
                    // Do nothing if the entry was already removed.
                }
                btree_map::Entry::Occupied(mut entry) => match &res {
                    None => {
                        // Task was either cancelled or received `TooNew` so no archive received.
                        entry.remove();
                    }
                    Some(info) => {
                        // Task was finished with a non-empty result so store it.
                        entry.insert(ArchiveSlot::Downloaded(info.clone()));
                    }
                },
            }

            if finished {
                return res.map(|info| (pending.archive_key, info));
            }

            tracing::warn!(mc_seqno, "archive task cancelled while in use");
            // Avoid spinloop just in case.
            tokio::task::yield_now().await;
        }
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
            memory_threshold: self.config.max_archive_to_memory_size,
        }
    }

    fn clear_outdated_archives(&self, bound: u32) {
        // TODO: Move into archive stuff
        const MAX_MC_PER_ARCHIVE: u32 = 100;

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
                    retain = task.archive_key.saturating_add(MAX_MC_PER_ARCHIVE) >= bound;
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

pub enum ArchiveWriter {
    File(std::io::BufWriter<std::fs::File>),
    Bytes(bytes::buf::Writer<BytesMut>),
}

impl ArchiveWriter {
    fn try_freeze(self) -> Result<Bytes, std::io::Error> {
        match self {
            Self::File(file) => match file.into_inner() {
                Ok(mut file) => {
                    file.seek(std::io::SeekFrom::Start(0))?;
                    MappedFile::from_existing_file(file).map(Bytes::from_owner)
                }
                Err(e) => Err(e.into_error()),
            },
            Self::Bytes(data) => Ok(data.into_inner().freeze()),
        }
    }
}

impl std::io::Write for ArchiveWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::File(writer) => writer.write(buf),
            Self::Bytes(writer) => writer.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::File(writer) => writer.flush(),
            Self::Bytes(writer) => writer.flush(),
        }
    }

    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        match self {
            Self::File(writer) => writer.write_all(buf),
            Self::Bytes(writer) => writer.write_all(buf),
        }
    }

    fn write_fmt(&mut self, fmt: std::fmt::Arguments<'_>) -> std::io::Result<()> {
        match self {
            Self::File(writer) => writer.write_fmt(fmt),
            Self::Bytes(writer) => writer.write_fmt(fmt),
        }
    }
}

pub struct ArchiveResponse {
    writer: ArchiveWriter,
    neighbour: Option<Neighbour>,
}

#[derive(Clone, Copy)]
pub struct ArchiveDownloadContext<'a> {
    pub storage: &'a CoreStorage,
    pub memory_threshold: ByteSize,
}

impl<'a> ArchiveDownloadContext<'a> {
    pub fn get_archive_writer(&self, size: NonZeroU64) -> Result<ArchiveWriter> {
        Ok(if size.get() > self.memory_threshold.as_u64() {
            let file = self.storage.context().temp_files().unnamed_file().open()?;
            ArchiveWriter::File(std::io::BufWriter::new(file))
        } else {
            ArchiveWriter::Bytes(BytesMut::new().writer())
        })
    }

    pub fn compute_archive_id(&self, mc_seqno: u32) -> Result<Option<u32>> {
        const BLOCKS_PER_ARCHIVE: u32 = Archive::MAX_MC_BLOCKS_PER_ARCHIVE;

        let storage = self.storage;

        // Next block should not be too far in the future.
        let last_mc_seqno = storage
            .node_state()
            .load_last_mc_block_id()
            .context("no blocks applied yet")?
            .seqno;
        if mc_seqno > last_mc_seqno.saturating_add(BLOCKS_PER_ARCHIVE) {
            return Ok(None);
        }

        // Archive id must be aligned to the latest key block id.
        let prev_key_block_seqno = storage
            .block_handle_storage()
            .find_prev_key_block(mc_seqno)
            .map(|handle| handle.id().seqno)
            .unwrap_or_default();

        let mut archive_id = mc_seqno;
        if prev_key_block_seqno < mc_seqno {
            // Example:
            // prev_key_block_seqno = 150
            // mc_seqno = 290
            // archive_id = 290 - (290 - 150) % 100 = 250
            archive_id -= (mc_seqno - prev_key_block_seqno) % BLOCKS_PER_ARCHIVE;
        }
        archive_id += 1;

        Ok(Some(archive_id))
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
        let Some(archive_id) = ctx.compute_archive_id(mc_seqno)? else {
            return Ok(None);
        };
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
