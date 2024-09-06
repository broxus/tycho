use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use arc_swap::{ArcSwapAny, ArcSwapOption};
use bytes::{BufMut, Bytes, BytesMut};
use bytesize::ByteSize;
use everscale_types::models::BlockId;
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use tokio::sync::{oneshot, Mutex};
use tokio::task::AbortHandle;
use tycho_block_util::archive::{Archive, ArchiveError};
use tycho_block_util::block::BlockIdRelation;
use tycho_storage::{NewBlockMeta, Storage};
use tycho_util::time::now_sec;
use tycho_util::{DashMapEntry, FastDashMap};

use crate::block_strider::provider::{BlockProvider, OptionalBlockStuff, ProofChecker};
use crate::blockchain_rpc::{BlockchainRpcClient, PendingArchive};
use crate::overlay_client::Neighbour;

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
        client: BlockchainRpcClient,
        storage: Storage,
        config: ArchiveBlockProviderConfig,
    ) -> Self {
        let proof_checker = ProofChecker::new(storage.clone());

        Self {
            inner: Arc::new(Inner {
                client,
                proof_checker,
                last_known_archive: ArcSwapOption::empty(),
                prev_known_archive: ArcSwapOption::empty(),

                next_archive: tokio::sync::Mutex::new(None),

                storage,
                config,
            }),
        }
    }

    async fn get_next_block_impl(&self, block_id: &BlockId) -> OptionalBlockStuff {
        const MAX_OVERLAP_BLOCKS: u32 = 5;

        let this = self.inner.as_ref();

        let next_block_seqno = block_id.seqno + 1;

        // Clear the previous archive if the next block is too far ahead
        if let Some(prev) = &*this.prev_known_archive.load() {
            let mut clear_last = true;
            if let Some((prev_max_seqno, _)) = prev.archive.mc_block_ids.last_key_value() {
                clear_last &= next_block_seqno > *prev_max_seqno + MAX_OVERLAP_BLOCKS;
            }
            if clear_last {
                this.prev_known_archive.store(None);
            }
        }

        'main: loop {
            let block_id;
            let archive = 'download: loop {
                if let Some(archive) = this.last_known_archive.load_full() {
                    if let Some(mc_block_id) = archive.archive.mc_block_ids.get(&next_block_seqno) {
                        block_id = *mc_block_id;
                        tracing::debug!(%mc_block_id, "block found in the last known archive");
                        break 'download archive;
                    }
                }

                match self.get_next_archive(next_block_seqno).await {
                    Ok(archive) => {
                        // Duplicate the last known archive
                        if let Some(last) = this.last_known_archive.load_full() {
                            this.prev_known_archive.store(Some(last));
                        }

                        // Update the last known archive
                        this.last_known_archive.store(Some(Arc::new(archive)));
                    }
                    Err(e) => {
                        tracing::error!("failed to get next archive {e:?}");
                        self.inner.last_known_archive.store(None);
                        continue;
                    }
                }
            };

            let check_successful = 'checks: {
                match (
                    archive.archive.mc_block_ids.first_key_value(),
                    archive.archive.mc_block_ids.last_key_value(),
                ) {
                    (Some((first_seqno, _)), Some((last_seqno, _))) => {
                        if (*last_seqno - first_seqno) != archive.archive.mc_block_ids.len() as u32
                        {
                            tracing::error!("Archive does not contain some mc blocks");
                            break 'checks false;
                        }
                    }
                    _ => {
                        tracing::error!("Archive is empty");
                        break 'checks false;
                    }
                }

                let (block, proof, diff) = match archive.archive.get_entry_by_id(&block_id) {
                    Ok(entry) => entry,
                    Err(e) => {
                        tracing::error!(
                            "Archive is corrupted {e:?}. Retrying archive downloading."
                        );
                        break 'checks false;
                    }
                };

                match this
                    .proof_checker
                    .check_proof(&block, &proof, &diff, true)
                    .await
                {
                    // Stop using archives if the block is recent enough
                    Ok(meta) if is_block_recent(&meta) => {
                        tracing::info!(%block_id, "archive block provider finished");
                        break 'main None;
                    }
                    Ok(_) => {
                        break 'main Some(Ok(block.clone()));
                    }
                    Err(e) => {
                        tracing::error!("Failed to check block proof {e:?}");
                        break 'checks false;
                    }
                }

                true
            };

            if !check_successful {
                self.inner.last_known_archive.store(None);
                continue 'main;
            }
        }
    }

    async fn get_block_impl(&self, block_id_relation: &BlockIdRelation) -> OptionalBlockStuff {
        let this = self.inner.as_ref();
        let mc_seqno = block_id_relation.mc_block_id().seqno;

        // select where to search block {
        let last_opt_guard = self.inner.last_known_archive.load();
        let search_result = match &*last_opt_guard {
            Some(archive_info) => archive_info.archive.mc_block_ids.get(&mc_seqno),
            None => None,
        };

        let suitable_archive = match search_result {
            None => match &*self.inner.prev_known_archive.load() {
                Some(archive_info)
                    if archive_info.archive.mc_block_ids.get(&mc_seqno).is_some() =>
                {
                    SuitableArchive::Previous
                }
                _ => SuitableArchive::NotFound,
            },
            Some(_) => SuitableArchive::Last,
        };

        let archive = match suitable_archive {
            SuitableArchive::Last => this.last_known_archive.load_full(),
            SuitableArchive::Previous => this.prev_known_archive.load_full(),
            SuitableArchive::NotFound => {
                // TODO: reload both somehow?
                return Some(Err(anyhow::anyhow!("Both archives are empty. Looks sus")));
            }
        };

        'archive: loop {
            let (block, proof, diff) = match &archive {
                Some(mut a) => {
                    if a.is_valid {
                        match a.archive.get_entry_by_id(block_id_relation.block_id()) {
                            // Successfully found the block and proof
                            Ok(entry) => entry,
                            Err(e) => {
                                tracing::error!(
                                    "Failed to find block {} in archive {e:?}",
                                    block_id_relation.block_id()
                                );
                                a.downloaded_from.track_reliability(false);
                                a.is_valid = false;
                                continue 'archive;
                            }
                        }
                    } else {
                        let result_opt =
                            this.reload_archive(&block_id_relation.mc_block_id()).await;

                        match result_opt {
                            Ok((archive, neighbour)) => {
                                a.downloaded_from = neighbour;
                                a.is_valid = true;
                                a.archive = archive;
                            }
                            Err(e) => {
                                tracing::error!("Failed to reload archive {e:?}");
                            }
                        };
                        continue 'archive;
                    }
                }
                None => {
                    continue 'archive;
                }
            };

            if let Err(e) = this
                .proof_checker
                .check_proof(&block, &proof, &diff, true)
                .await
            {
                tracing::error!("Failed to check block proof {e:?}");
                continue 'archive;
            }
            // NOTE: Always return the block by id even if it's not recent
            Some(Ok(block.clone()))
        }
    }

    async fn get_next_archive(&self, next_block_seqno: u32) -> Result<Archive> {
        let mut guard = self.inner.next_archive.lock().await;

        loop {
            match &mut *guard {
                Some(next) => {
                    let archive_data = next.wait_for_archive().await?;

                    // Reset the guard
                    //
                    // FIXME: At this point if the future is cancelled, subsequent call will
                    // download the same archive again. We might want to reset the guard only
                    // after the archive is successfully constructed. But this will require
                    // the `archive_data` to be a `Shared` (clonable) future.
                    *guard = None;

                    let archive = match self.inner.construct_archive(archive_data).await {
                        Ok(archive) => archive,
                        Err(e) => {
                            // TODO: backoff
                            tracing::error!(
                                seqno = next_block_seqno,
                                "failed to construct archive {e:?}"
                            );
                            tokio::time::sleep(Duration::from_secs(1)).await;
                            continue;
                        }
                    };

                    if let Some((seqno, _)) = archive.mc_block_ids.last_key_value() {
                        *guard = Some(self.make_next_archive_task(*seqno + 1));
                    }

                    return Ok(archive);
                }
                None => *guard = Some(self.make_next_archive_task(next_block_seqno)),
            }
        }
    }

    fn make_next_archive_task(&self, seqno: u32) -> NextArchive {
        // TODO: Use a proper backoff here?
        const INTERVAL: Duration = Duration::from_secs(1);

        let (tx, rx) = oneshot::channel();

        // NOTE: Use a separate downloader to prevent reference cycles
        let downloader = self.inner.make_downloader();
        let this = self.inner.clone();

        let handle = tokio::spawn(async move {
            tracing::debug!(seqno, "started preloading archive");
            scopeguard::defer! {
                tracing::debug!(seqno, "finished preloading archive");
            }
            loop {
                match downloader.try_download(seqno).await {
                    Ok((archive, neighbour)) => {
                        tx.send(archive).ok();
                        break;
                    }
                    Err(e) => {
                        tracing::error!(seqno, "failed to preload archive {e:?}");
                        tokio::time::sleep(INTERVAL).await;
                    }
                }
            }
        });

        NextArchive {
            rx: Some(rx),
            abort_handle: handle.abort_handle(),
        }
    }
}

fn is_block_recent(meta: &NewBlockMeta) -> bool {
    meta.gen_utime + 600 > now_sec()
}

struct Inner {
    storage: Storage,

    client: BlockchainRpcClient,
    proof_checker: ProofChecker,
    last_known_archive: ArcSwapOption<DownloadedArchiveInfo>,
    prev_known_archive: ArcSwapOption<DownloadedArchiveInfo>,

    next_archive: tokio::sync::Mutex<Option<NextArchive>>,

    config: ArchiveBlockProviderConfig,
}

impl Inner {
    async fn reload_archive(&self, mc_block_id: &BlockId) -> Result<(Archive, Neighbour)> {
        let downloader = self.make_downloader();
        let (archive_data, neighbour) = downloader.try_download(mc_block_id.seqno).await?;
        let archive = self.construct_archive(archive_data).await?;
        Ok((archive, neighbour))
    }
    fn make_downloader(&self) -> ArchiveDownloader {
        ArchiveDownloader {
            client: self.client.clone(),
            storage: self.storage.clone(),
            memory_threshold: self.config.max_archive_to_memory_size,
        }
    }

    async fn construct_archive(&self, data: ArchiveData) -> Result<Archive> {
        let bytes = match data {
            ArchiveData::Bytes(bytes) => bytes,
            ArchiveData::File { id } => {
                let temp_archives = self.storage.temp_archive_storage();

                let data = temp_archives.read_archive_to_bytes(id).await?;
                if let Err(e) = temp_archives.remove_archive(id) {
                    tracing::warn!("failed to remove temp archive: {e:?}");
                }

                data
            }
        };

        Archive::new(bytes)
    }
}

struct ArchiveDownloader {
    client: BlockchainRpcClient,
    storage: Storage,
    memory_threshold: ByteSize,
}

impl ArchiveDownloader {
    async fn try_download(&self, seqno: u32) -> Result<(ArchiveData, Neighbour)> {
        let pending = self.client.find_archive(seqno).await?;
        let selected_neighbour = pending.neighbour.clone();
        let archive_id = pending.id;

        let writer = self.get_archive_writer(&pending)?;
        let writer = self.client.download_archive(pending, writer).await?;

        let archive_data = match writer {
            ArchiveWriter::File(_) => ArchiveData::File { id: archive_id },
            ArchiveWriter::Bytes(data) => ArchiveData::Bytes(data.into_inner().freeze()),
        };

        Ok((archive_data, selected_neighbour))
    }

    fn get_archive_writer(&self, pending: &PendingArchive) -> Result<ArchiveWriter> {
        Ok(if pending.size.get() > self.memory_threshold.as_u64() {
            let file = self
                .storage
                .temp_archive_storage()
                .create_archive_file(pending.id)?;
            ArchiveWriter::File(std::io::BufWriter::new(file))
        } else {
            ArchiveWriter::Bytes(BytesMut::new().writer())
        })
    }
}

struct NextArchive {
    rx: Option<oneshot::Receiver<ArchiveData>>,
    abort_handle: AbortHandle,
}

impl NextArchive {
    pub async fn wait_for_archive(&mut self) -> Result<ArchiveData, oneshot::error::RecvError> {
        let result = self.rx.as_mut().expect("should not wait twice").await;
        self.rx = None;
        result
    }
}

impl Drop for NextArchive {
    fn drop(&mut self) {
        if self.rx.is_some() {
            self.abort_handle.abort();
        }
    }
}

#[derive(Clone)]
enum ArchiveData {
    Bytes(Bytes),
    File { id: u64 },
}

impl BlockProvider for ArchiveBlockProvider {
    type GetNextBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;
    type GetBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;

    fn get_next_block<'a>(&'a self, prev_block_id: &'a BlockId) -> Self::GetNextBlockFut<'a> {
        Box::pin(self.get_next_block_impl(prev_block_id))
    }

    fn get_block<'a>(&'a self, block_id_relation: &'a BlockIdRelation) -> Self::GetBlockFut<'a> {
        Box::pin(self.get_block_impl(block_id_relation))
    }
}

enum ArchiveWriter {
    File(std::io::BufWriter<std::fs::File>),
    Bytes(bytes::buf::Writer<BytesMut>),
}

impl std::io::Write for ArchiveWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::File(writer) => writer.write(buf),
            Self::Bytes(writer) => writer.write(buf),
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

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::File(writer) => writer.flush(),
            Self::Bytes(writer) => writer.flush(),
        }
    }
}

pub enum SuitableArchive {
    Last,
    Previous,
    NotFound,
}

pub struct DownloadedArchiveInfo {
    pub archive: Archive,
    pub downloaded_from: Neighbour,
    pub is_valid: bool,
}
