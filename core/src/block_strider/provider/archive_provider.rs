use std::collections::BTreeMap;
use std::io::Seek;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};
use bytesize::ByteSize;
use everscale_types::models::BlockId;
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio::task::AbortHandle;
use tycho_block_util::archive::Archive;
use tycho_block_util::block::{BlockIdRelation, BlockStuffAug};
use tycho_storage::{MappedFile, Storage};

use crate::block_strider::provider::{BlockProvider, CheckProof, OptionalBlockStuff, ProofChecker};
use crate::blockchain_rpc::{BlockchainRpcClient, PendingArchive, PendingArchiveResponse};
use crate::overlay_client::{Neighbour, PunishReason};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ArchiveBlockProviderConfig {
    pub max_archive_to_memory_size: ByteSize,
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
        client: BlockchainRpcClient,
        storage: Storage,
        config: ArchiveBlockProviderConfig,
    ) -> Self {
        let proof_checker = ProofChecker::new(storage.clone());

        let archives_manager = ArchivesManager::new();

        Self {
            inner: Arc::new(Inner {
                client,
                proof_checker,
                archives: archives_manager,
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
                tracing::info!(mc_seqno = next_mc_seqno, "archive block provider finished");
                break None;
            };

            let Some(block_id) = info.archive.mc_block_ids.get(&next_mc_seqno) else {
                tracing::error!(
                    "received archive does not contain mc block with seqno {next_mc_seqno}"
                );
                info.from.punish(PunishReason::Malicious);
                this.remove_archive_if_same(archive_key, &info);
                continue;
            };

            match self
                .checked_get_entry_by_id(&info.archive, block_id, block_id)
                .await
            {
                Ok(block) => return Some(Ok(block.clone())),
                Err(e) => {
                    tracing::error!(archive_key, %block_id, "invalid archive entry: {e}");
                    this.remove_archive_if_same(archive_key, &info);
                    info.from.punish(PunishReason::Malicious);
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
                    tracing::error!(archive_key, %block_id, %mc_block_id, "invalid archive entry: {e}");
                    this.remove_archive_if_same(archive_key, &info);
                    info.from.punish(PunishReason::Malicious);
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
    storage: Storage,

    client: BlockchainRpcClient,
    proof_checker: ProofChecker,

    archives: ArchivesManager,

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
                                // Prefetch next archive if enabled
                                if self.config.num_prefetched_archives.is_some() {
                                    if let Some((last_seqno, _)) =
                                        info.archive.mc_block_ids.last_key_value()
                                    {
                                        let next_archive_start_seqno = last_seqno.saturating_add(1);
                                        let this = self.clone();
                                        tokio::spawn(async move {
                                            this.try_prefetch_archive(next_archive_start_seqno)
                                                .await;
                                        });
                                    }
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
                // Prefetch next archive if enabled
                if self.config.num_prefetched_archives.is_some() {
                    if let Some(info) = res
                        .as_ref()
                        .and_then(|i| i.archive.mc_block_ids.last_key_value())
                    {
                        let (last_seqno, _) = info;
                        let next_archive_start_seqno = last_seqno.saturating_add(1);
                        let this = self.clone();
                        // Fire and forget prefetch
                        tokio::spawn(async move {
                            this.try_prefetch_archive(next_archive_start_seqno).await;
                        });
                    }
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

    /// Try to prefetch the archive containing `prefetch_seqno` if not already present or pending.
    async fn try_prefetch_archive(&self, prefetch_seqno: u32) {
        // Check if archive already present or pending
        for value in self.archives.values() {
            match value {
                ArchiveSlot::Downloaded(info) => {
                    if info.archive.mc_block_ids.contains_key(&prefetch_seqno) {
                        tracing::trace!(
                            prefetch_seqno,
                            "archive already downloaded, skipping prefetch"
                        );
                        return;
                    }
                }
                ArchiveSlot::Pending(task) => {
                    if task.archive_key == prefetch_seqno {
                        tracing::trace!(
                            prefetch_seqno,
                            "archive download already pending, skipping prefetch"
                        );
                        return;
                    }
                }
            }
        }

        let task = self.make_downloader().spawn(prefetch_seqno);
        self.archives
            .insert(prefetch_seqno, ArchiveSlot::Pending(task));
        tracing::debug!(prefetch_seqno, "starting archive prefetch");
    }

    fn make_downloader(&self) -> ArchiveDownloader {
        ArchiveDownloader {
            client: self.client.clone(),
            storage: self.storage.clone(),
            memory_threshold: self.config.max_archive_to_memory_size,
            max_downloaded_archives: self.config.num_prefetched_archives.unwrap_or(1), /* 0 will deadlock */
            map_len_at_spawn: self.archives.len(),
            archives_len_rx: self.archives.len_update_rx(),
        }
    }

    fn clear_outdated_archives(&self, bound: u32) {
        // TODO: Move into archive stuff
        const MAX_MC_PER_ARCHIVE: u32 = 100;

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
    len_tx: watch::Sender<usize>,
    known_archive_len: watch::Receiver<usize>,
}

impl ArchivesManager {
    fn new() -> Self {
        let (len_tx, len_rx) = watch::channel(0);
        Self {
            map: parking_lot::Mutex::new(BTreeMap::new()),
            len_tx,
            known_archive_len: len_rx,
        }
    }

    fn len_update_rx(&self) -> watch::Receiver<usize> {
        self.known_archive_len.clone()
    }

    fn len(&self) -> usize {
        self.map.lock().len()
    }

    fn insert(&self, key: u32, value: ArchiveSlot) -> Option<ArchiveSlot> {
        let mut map = self.map.lock();
        let res = map.insert(key, value);
        self.update_len(map.len());
        res
    }

    fn remove(&self, key: &u32) -> Option<ArchiveSlot> {
        let mut map = self.map.lock();
        let res = map.remove(key);

        self.update_len(map.len());

        res
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

    fn values(&self) -> Vec<ArchiveSlot> {
        self.map.lock().values().cloned().collect()
    }

    fn retain<F>(&self, mut f: F)
    where
        F: FnMut(&u32, &mut ArchiveSlot) -> bool,
    {
        let mut map = self.map.lock();
        map.retain(|k, v| f(k, v));
        self.update_len(map.len());
    }

    fn update_len(&self, len: usize) {
        let _ = self.len_tx.send_if_modified(|current| {
            if *current != len {
                *current = len;
                true
            } else {
                false
            }
        });
    }
}

#[derive(Clone)]
enum ArchiveSlot {
    Downloaded(ArchiveInfo),
    Pending(ArchiveTask),
}

#[derive(Clone)]
struct ArchiveInfo {
    from: Neighbour,
    archive: Arc<Archive>,
}

struct ArchiveDownloader {
    client: BlockchainRpcClient,
    storage: Storage,
    memory_threshold: ByteSize,
    max_downloaded_archives: usize,
    map_len_at_spawn: usize,
    archives_len_rx: watch::Receiver<usize>,
}

impl ArchiveDownloader {
    fn spawn(mut self, mc_seqno: u32) -> ArchiveTask {
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

            if self.map_len_at_spawn > self.max_downloaded_archives {
                tracing::debug!(mc_seqno, "too many archives already downloaded, waiting");
                match self
                    .archives_len_rx
                    .wait_for(|x| x < &self.max_downloaded_archives)
                    .await
                {
                    Err(e) => {
                        tracing::warn!(
                            mc_seqno,
                            "archive task cancelled while waiting for free space: {e}"
                        );
                        return;
                    }
                    Ok(v) => {
                        let v = *v;
                        tracing::debug!(mc_seqno, used_slots = v, "free space available: {v}");
                    }
                }
            }

            loop {
                match self.try_download(mc_seqno).await {
                    Ok(res) => {
                        let tx = scopeguard::ScopeGuard::into_inner(guard);
                        tx.send_modify(move |prev| *prev = ArchiveTaskState::Finished(res));
                        break;
                    }
                    Err(e) => {
                        tracing::error!(mc_seqno, "failed to preload archive {e}");
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

    async fn try_download(&self, seqno: u32) -> Result<Option<ArchiveInfo>> {
        let response = self.client.find_archive(seqno).await?;
        let pending = match response {
            PendingArchiveResponse::Found(pending) => pending,
            PendingArchiveResponse::TooNew => return Ok(None),
        };

        let neighbour = pending.neighbour.clone();

        let writer = self.get_archive_writer(&pending)?;
        let writer = self.client.download_archive(pending, writer).await?;

        let span = tracing::Span::current();
        tokio::task::spawn_blocking(move || {
            let _span = span.enter();

            let bytes = writer.try_freeze()?;

            let archive = match Archive::new(bytes) {
                Ok(array) => array,
                Err(e) => {
                    neighbour.punish(PunishReason::Malicious);
                    return Err(e);
                }
            };

            if let Err(e) = archive.check_mc_blocks_range() {
                // TODO: Punish a bit less for missing mc blocks?
                neighbour.punish(PunishReason::Malicious);
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

    fn get_archive_writer(&self, pending: &PendingArchive) -> Result<ArchiveWriter> {
        Ok(if pending.size.get() > self.memory_threshold.as_u64() {
            let file = self.storage.temp_file_storage().unnamed_file().open()?;
            ArchiveWriter::File(std::io::BufWriter::new(file))
        } else {
            ArchiveWriter::Bytes(BytesMut::new().writer())
        })
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

enum ArchiveWriter {
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
