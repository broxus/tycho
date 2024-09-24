use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use arc_swap::{ArcSwapAny, ArcSwapOption};
use bytes::{BufMut, Bytes, BytesMut};
use bytesize::ByteSize;
use everscale_types::models::BlockId;
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use tokio::task::AbortHandle;
use tycho_block_util::archive::{Archive, ArchiveError};
use tycho_block_util::block::BlockIdRelation;
use tycho_storage::Storage;

use crate::block_strider::provider::{BlockProvider, OptionalBlockStuff, ProofChecker};
use crate::blockchain_rpc::{BlockchainRpcClient, PendingArchive};

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
            if let Some((prev_max_seqno, _)) = prev.mc_block_ids.last_key_value() {
                clear_last &= next_block_seqno > *prev_max_seqno + MAX_OVERLAP_BLOCKS;
            }
            if clear_last {
                this.prev_known_archive.store(None);
            }
        }

        let block_id;
        let archive = loop {
            if let Some(archive) = this.last_known_archive.load_full() {
                if let Some(mc_block_id) = archive.mc_block_ids.get(&next_block_seqno) {
                    block_id = *mc_block_id;
                    tracing::debug!(%mc_block_id, "block found in the last known archive");
                    break archive;
                }
            }

            match self.get_next_archive(next_block_seqno).await {
                Ok(Some(archive)) => {
                    // Duplicate the last known archive
                    if let Some(last) = this.last_known_archive.load_full() {
                        this.prev_known_archive.store(Some(last));
                    }

                    // Update the last known archive
                    this.last_known_archive.store(Some(Arc::new(archive)));
                }
                Ok(None) => {
                    tracing::info!("archive block provider finished");
                    return None;
                }
                Err(e) => return Some(Err(e)),
            }
        };

        let (block, proof, diff) = match archive.get_entry_by_id(&block_id) {
            Ok(entry) => entry,
            Err(e) => return Some(Err(e.into())),
        };

        match this
            .proof_checker
            .check_proof(&block, &proof, &diff, true)
            .await
        {
            Ok(_) => Some(Ok(block.clone())),
            Err(e) => Some(Err(e)),
        }
    }

    async fn get_block_impl(&self, block_id_relation: &BlockIdRelation) -> OptionalBlockStuff {
        let this = self.inner.as_ref();

        let mut archive = this.last_known_archive.load_full();
        let (block, proof, diff) = 'found: {
            let mut fallback = Some(&this.prev_known_archive);

            while let Some(a) = &archive {
                match a.get_entry_by_id(&block_id_relation.block_id) {
                    // Successfully found the block and proof
                    Ok(entry) => break 'found entry,
                    // Block not found in the archive so try the fallback archive
                    Err(ArchiveError::OutOfRange) => {
                        archive = fallback.take().and_then(ArcSwapAny::load_full);
                        continue;
                    }
                    // Treat other errors as terminal
                    Err(e) => return Some(Err(e.into())),
                }
            }

            // Block not found in any archive
            return Some(Err(ArchiveError::OutOfRange.into()));
        };

        if let Err(e) = this
            .proof_checker
            .check_proof(&block, &proof, &diff, true)
            .await
        {
            return Some(Err(e));
        }

        // NOTE: Always return the block by id even if it's not recent
        Some(Ok(block.clone()))
    }

    async fn get_next_archive(&self, next_block_seqno: u32) -> Result<Option<Archive>> {
        let mut guard = self.inner.next_archive.lock().await;

        loop {
            match &mut *guard {
                Some(next) => {
                    let archive_data = match next.wait_for_archive().await? {
                        Some(archive_data) => archive_data,
                        None => return Ok(None),
                    };

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

                    return Ok(Some(archive));
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
        let handle = tokio::spawn(async move {
            tracing::debug!(seqno, "started preloading archive");
            scopeguard::defer! {
                tracing::debug!(seqno, "finished preloading archive");
            }

            loop {
                match downloader.try_download(seqno).await {
                    Ok(archive) => {
                        tx.send(archive).ok();
                        break;
                    }
                    Err(e) => {
                        tracing::error!(seqno, "failed to preload archive {e}");
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

struct Inner {
    storage: Storage,

    client: BlockchainRpcClient,
    proof_checker: ProofChecker,
    last_known_archive: ArcSwapOption<Archive>,
    prev_known_archive: ArcSwapOption<Archive>,

    next_archive: tokio::sync::Mutex<Option<NextArchive>>,

    config: ArchiveBlockProviderConfig,
}

impl Inner {
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
    async fn try_download(&self, seqno: u32) -> Result<Option<ArchiveData>> {
        let Some(pending) = self.client.find_archive(seqno).await? else {
            return Ok(None);
        };

        let archive_id = pending.id;

        let writer = self.get_archive_writer(&pending)?;
        let writer = self.client.download_archive(pending, writer).await?;

        let archive_data = match writer {
            ArchiveWriter::File(_) => ArchiveData::File { id: archive_id },
            ArchiveWriter::Bytes(data) => ArchiveData::Bytes(data.into_inner().freeze()),
        };

        Ok(Some(archive_data))
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
    rx: Option<oneshot::Receiver<Option<ArchiveData>>>,
    abort_handle: AbortHandle,
}

impl NextArchive {
    pub async fn wait_for_archive(
        &mut self,
    ) -> Result<Option<ArchiveData>, oneshot::error::RecvError> {
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
