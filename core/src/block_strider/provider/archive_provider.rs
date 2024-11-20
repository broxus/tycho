use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::io::{Read, Seek};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};
use bytesize::ByteSize;
use everscale_types::models::BlockId;
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use tokio::sync::{oneshot, Mutex};
use tokio::task::AbortHandle;
use tycho_block_util::archive::{Archive, ArchiveError};
use tycho_block_util::block::BlockIdRelation;
use tycho_storage::Storage;

use crate::block_strider::provider::{
    BlockProvider, CheckProof, OptionalBlockStuff, ProofChecker, RetryConfig,
};
use crate::blockchain_rpc::{BlockchainRpcClient, PendingArchive, PendingArchiveResponse};
use crate::overlay_client::{Neighbour, PunishReason};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ArchiveBlockProviderConfig {
    pub max_archive_to_memory_size: ByteSize,
    pub retry_config: RetryConfig,
}

impl Default for ArchiveBlockProviderConfig {
    fn default() -> Self {
        Self {
            max_archive_to_memory_size: ByteSize::mb(100),
            retry_config: RetryConfig::default(),
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

                known_archives: Mutex::new(Default::default()),

                storage,
                config,
            }),
        }
    }

    async fn get_next_block_impl(&self, block_id: &BlockId) -> OptionalBlockStuff {
        const MAX_OVERLAP_BLOCKS: u32 = 5;

        let this = self.inner.as_ref();

        let next_block_seqno = block_id.seqno + 1;

        if let Some((archive_key, info)) = this.look_for_archive(block_id.seqno).await {
            let mut should_clear_outdated = true;

            if let Some((first_seqno, _)) = info.archive.mc_block_ids.first_key_value() {
                should_clear_outdated &= next_block_seqno > *first_seqno + MAX_OVERLAP_BLOCKS;
            }

            if should_clear_outdated {
                this.clear_outdated_archives(archive_key).await;
            }
        }

        'main: loop {
            let (block_id, archive_key, archive_info) = 'download: loop {
                // Looking for block in the archives cache
                if let Some((key, info)) = this.look_for_archive(next_block_seqno).await {
                    if let Some(mc_block_id) = info.archive.mc_block_ids.get(&next_block_seqno) {
                        break 'download (*mc_block_id, key, info);
                    }
                }

                match self.download_archive(next_block_seqno).await {
                    Ok(Some(archive_info)) => {
                        if let Some(mc_block_id) =
                            archive_info.archive.mc_block_ids.get(&next_block_seqno)
                        {
                            tracing::debug!(%mc_block_id, "block found in the last known archive");
                            break 'download (*mc_block_id, next_block_seqno, archive_info);
                        }
                    }
                    Ok(None) => {
                        tracing::info!("archive block provider finished");

                        // Clear archives cache
                        this.clear_known_archives().await;

                        break 'main None;
                    }
                    Err(e) => {
                        tracing::error!("failed to get next archive {e:?}");
                        continue;
                    }
                }
            };

            let archive = &archive_info.archive;

            'checks: {
                {
                    match (
                        archive.mc_block_ids.first_key_value(),
                        archive.mc_block_ids.last_key_value(),
                    ) {
                        (Some((first_seqno, _)), Some((last_seqno, _))) => {
                            if (last_seqno - first_seqno + 1) != archive.mc_block_ids.len() as u32 {
                                tracing::error!("Archive does not contain some mc blocks");
                                break 'checks;
                            }
                        }
                        _ => {
                            tracing::error!("Archive is empty");
                            break 'checks;
                        }
                    }
                }

                let (ref block, ref proof, ref queue_diff) =
                    match archive.get_entry_by_id(&block_id) {
                        Ok(entry) => entry,
                        Err(e) => {
                            tracing::error!(
                                "Archive is corrupted {e:?}. Retrying archive downloading."
                            );
                            break 'checks;
                        }
                    };

                match this
                    .proof_checker
                    .check_proof(CheckProof {
                        mc_block_id: &block_id,
                        block,
                        proof,
                        queue_diff,
                        store_on_success: true,
                    })
                    .await
                {
                    Ok(_) => {
                        break 'main Some(Ok(block.clone()));
                    }
                    Err(e) => {
                        tracing::error!("Failed to check block proof {e:?}");
                        break 'checks;
                    }
                };
            };

            this.remove_archive(archive_key).await;
            archive_info.from.punish(PunishReason::Malicious);
        }
    }

    async fn get_block_impl(&self, block_id_relation: &BlockIdRelation) -> OptionalBlockStuff {
        let this = self.inner.as_ref();

        let block_id = block_id_relation.block_id;
        let mc_block_id = block_id_relation.mc_block_id;

        let mut entry = None;

        let guard = this.known_archives.lock().await;
        for (_, slot) in guard.iter() {
            if let ArchiveSlot::Downloaded(info) = slot {
                if let Ok(res) = info.archive.get_entry_by_id(&block_id) {
                    entry = Some(res);
                }
            }
        }
        drop(guard);

        let (ref block, ref proof, ref queue_diff) = match entry {
            Some(entry) => entry,
            None => {
                tracing::error!("archive out of range");
                return Some(Err(ArchiveError::OutOfRange.into()));
            }
        };

        if let Err(e) = this
            .proof_checker
            .check_proof(CheckProof {
                mc_block_id: &mc_block_id,
                block,
                proof,
                queue_diff,
                store_on_success: true,
            })
            .await
        {
            return Some(Err(e));
        }

        // NOTE: Always return the block by id even if it's not recent
        Some(Ok(block.clone()))
    }

    async fn download_archive(&self, archive_key: u32) -> Result<Option<ArchiveInfo>> {
        let this = self.inner.as_ref();

        loop {
            let mut guard = this.known_archives.lock().await;
            let pending = match guard.get_mut(&archive_key) {
                Some(archive_slot) => match archive_slot {
                    ArchiveSlot::Pending(ref mut next) => {
                        // Waiting for pending archive
                        let pr_archive_info = match next.wait_for_archive().await? {
                            Some(archive_info) => archive_info,
                            None => return Ok(None), // TooNew Archive
                        };
                        Some(pr_archive_info)
                    }
                    ArchiveSlot::Downloaded(info) => {
                        return Ok(Some(info.clone()));
                    }
                },
                None => None,
            };
            drop(guard);

            match pending {
                Some(pr_archive_info) => {
                    let archive = match this.construct_archive(pr_archive_info.data).await {
                        Ok(archive) => Arc::new(archive),
                        Err(e) => {
                            tracing::error!(
                                seqno = archive_key,
                                "failed to construct archive {e:?}"
                            );

                            pr_archive_info.neighbour.punish(PunishReason::Malicious);
                            return Err(e);
                        }
                    };

                    this.update_pending_archive(
                        archive_key,
                        archive.clone(),
                        pr_archive_info.neighbour.clone(),
                    )
                    .await;

                    if let Some((seqno, _)) = archive.mc_block_ids.last_key_value() {
                        let next_seqno = seqno + 1;

                        // Start downloading next archive if not started yet
                        if !this.is_archive_exist(next_seqno).await {
                            let next = self.make_next_archive_task(next_seqno);
                            this.add_pending_archive(next_seqno, next).await?;
                        }
                    }

                    let archive_info = ArchiveInfo {
                        archive,
                        from: pr_archive_info.neighbour,
                    };

                    return Ok(Some(archive_info));
                }
                None => {
                    let next = self.make_next_archive_task(archive_key);
                    this.add_pending_archive(archive_key, next).await?;
                }
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
                    Ok(res) => {
                        let archive =
                            res.map(|(data, neighbour)| PreloadedArchiveInfo { data, neighbour });
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

    known_archives: Mutex<ArchivesMap>,

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

            // NOTE: We are using an existing file descriptor here, so we cannot use
            //       the suggested `std::fs::read`.
            #[allow(clippy::verbose_file_reads)]
            ArchiveData::File(mut file) => {
                tokio::task::spawn_blocking(move || {
                    file.seek(std::io::SeekFrom::Start(0))?;
                    let size = file.metadata().map(|m| m.len() as usize).ok();
                    let mut bytes = Vec::new();
                    bytes.reserve_exact(size.unwrap_or(0));
                    file.read_to_end(&mut bytes)?;
                    Ok::<_, std::io::Error>(Bytes::from(bytes))
                })
                .await??
            }
        };

        Archive::new(bytes)
    }

    async fn look_for_archive(&self, mc_block_seqno: u32) -> Option<(u32, ArchiveInfo)> {
        let guard = self.known_archives.lock().await;
        for (archive_key, value) in guard.iter() {
            match value {
                ArchiveSlot::Downloaded(info) => {
                    if info.archive.mc_block_ids.contains_key(&mc_block_seqno) {
                        return Some((*archive_key, info.clone()));
                    }
                }
                ArchiveSlot::Pending { .. } => (),
            }
        }

        None
    }

    async fn add_pending_archive(&self, key: u32, next: NextArchive) -> Result<()> {
        let mut guard = self.known_archives.lock().await;
        match guard.entry(key) {
            Entry::Vacant(vacant) => {
                vacant.insert(ArchiveSlot::Pending(next));
            }
            Entry::Occupied(_) => {
                tracing::warn!("Archive already exist in archives cache for {key}");
            }
        }

        Ok(())
    }

    async fn is_archive_exist(&self, key: u32) -> bool {
        let guard = self.known_archives.lock().await;
        guard.get(&key).is_some()
    }

    async fn update_pending_archive(&self, key: u32, archive: Arc<Archive>, source: Neighbour) {
        let mut guard = self.known_archives.lock().await;
        match guard.entry(key) {
            Entry::Occupied(mut occupied) => {
                let new_value = ArchiveSlot::Downloaded(ArchiveInfo {
                    from: source,
                    archive,
                });
                occupied.insert(new_value);
            }
            Entry::Vacant(_) => {
                tracing::warn!("Nothing to update in archives cache with key {key}");
            }
        }
    }

    async fn remove_archive(&self, key: u32) {
        let mut guard = self.known_archives.lock().await;
        guard.remove(&key);
    }

    async fn clear_outdated_archives(&self, bound: u32) {
        let mut guard = self.known_archives.lock().await;
        guard.retain(|key, _| *key >= bound);
    }

    async fn clear_known_archives(&self) {
        let mut guard = self.known_archives.lock().await;
        guard.clear();
    }
}

struct ArchiveDownloader {
    client: BlockchainRpcClient,
    storage: Storage,
    memory_threshold: ByteSize,
}

impl ArchiveDownloader {
    async fn try_download(&self, seqno: u32) -> Result<Option<(ArchiveData, Neighbour)>> {
        let response = self.client.find_archive(seqno).await?;
        let pending = match response {
            PendingArchiveResponse::Found(pending) => pending,
            PendingArchiveResponse::TooNew => return Ok(None),
        };

        let neighbour = pending.neighbour.clone();

        let writer = self.get_archive_writer(&pending)?;
        let writer = self.client.download_archive(pending, writer).await?;

        let archive_data = match writer {
            ArchiveWriter::File(file) => match file.into_inner() {
                Ok(file) => ArchiveData::File(file),
                Err(e) => return Err(e.into_error().into()),
            },
            ArchiveWriter::Bytes(data) => ArchiveData::Bytes(data.into_inner().freeze()),
        };

        Ok(Some((archive_data, neighbour)))
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

struct NextArchive {
    rx: Option<oneshot::Receiver<Option<PreloadedArchiveInfo>>>,
    abort_handle: AbortHandle,
}

impl NextArchive {
    pub async fn wait_for_archive(
        &mut self,
    ) -> Result<Option<PreloadedArchiveInfo>, oneshot::error::RecvError> {
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

enum ArchiveData {
    Bytes(Bytes),
    File(std::fs::File),
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

type ArchivesMap = BTreeMap<u32, ArchiveSlot>;

enum ArchiveSlot {
    Downloaded(ArchiveInfo),
    Pending(NextArchive),
}

#[derive(Clone)]
pub struct ArchiveInfo {
    pub from: Neighbour,
    pub archive: Arc<Archive>,
}

struct PreloadedArchiveInfo {
    pub data: ArchiveData,
    pub neighbour: Neighbour,
}
