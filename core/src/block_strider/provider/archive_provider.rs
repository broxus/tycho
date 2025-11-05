use std::collections::{BTreeMap, btree_map};
use std::io::Seek;
use std::sync::Arc;
use std::time::Duration;
use anyhow::Result;
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
use crate::block_strider::provider::{
    BlockProvider, CheckProof, OptionalBlockStuff, ProofChecker,
};
use crate::blockchain_rpc::{BlockchainRpcClient, PendingArchive, PendingArchiveResponse};
use crate::overlay_client::{Neighbour, PunishReason};
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
        client: BlockchainRpcClient,
        storage: CoreStorage,
        config: ArchiveBlockProviderConfig,
    ) -> Self {
        let proof_checker = ProofChecker::new(storage.clone());
        Self {
            inner: Arc::new(Inner {
                client,
                proof_checker,
                known_archives: parking_lot::Mutex::new(Default::default()),
                storage,
                config,
            }),
        }
    }
    async fn get_next_block_impl(&self, block_id: &BlockId) -> OptionalBlockStuff {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(get_next_block_impl)),
            file!(),
            64u32,
        );
        let block_id = block_id;
        let this = self.inner.as_ref();
        let next_mc_seqno = block_id.seqno + 1;
        loop {
            __guard.checkpoint(69u32);
            let Some((archive_key, info)) = ({
                __guard.end_section(70u32);
                let __result = this.get_archive(next_mc_seqno).await;
                __guard.start_section(70u32);
                __result
            }) else {
                tracing::info!(
                    mc_seqno = next_mc_seqno, "archive block provider finished"
                );
                {
                    __guard.end_section(72u32);
                    __guard.start_section(72u32);
                    break None;
                };
            };
            let Some(block_id) = info.archive.mc_block_ids.get(&next_mc_seqno) else {
                tracing::error!(
                    "received archive does not contain mc block with seqno {next_mc_seqno}"
                );
                info.from.punish(PunishReason::Malicious);
                this.remove_archive_if_same(archive_key, &info);
                {
                    __guard.end_section(81u32);
                    __guard.start_section(81u32);
                    continue;
                };
            };
            match {
                __guard.end_section(86u32);
                let __result = self
                    .checked_get_entry_by_id(&info.archive, block_id, block_id)
                    .await;
                __guard.start_section(86u32);
                __result
            } {
                Ok(block) => {
                    __guard.end_section(88u32);
                    return Some(Ok(block.clone()));
                }
                Err(e) => {
                    tracing::error!(
                        archive_key, % block_id, "invalid archive entry: {e:?}"
                    );
                    this.remove_archive_if_same(archive_key, &info);
                    info.from.punish(PunishReason::Malicious);
                }
            }
        }
    }
    async fn get_block_impl(
        &self,
        block_id_relation: &BlockIdRelation,
    ) -> OptionalBlockStuff {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(get_block_impl)),
            file!(),
            98u32,
        );
        let block_id_relation = block_id_relation;
        let this = self.inner.as_ref();
        let block_id = block_id_relation.block_id;
        let mc_block_id = block_id_relation.mc_block_id;
        loop {
            __guard.checkpoint(104u32);
            let Some((archive_key, info)) = ({
                __guard.end_section(105u32);
                let __result = this.get_archive(mc_block_id.seqno).await;
                __guard.start_section(105u32);
                __result
            }) else {
                tracing::warn!("shard block is too new for archives");
                {
                    __guard.end_section(109u32);
                    let __result = tokio::time::sleep(Duration::from_secs(1)).await;
                    __guard.start_section(109u32);
                    __result
                };
                {
                    __guard.end_section(110u32);
                    __guard.start_section(110u32);
                    continue;
                };
            };
            match {
                __guard.end_section(115u32);
                let __result = self
                    .checked_get_entry_by_id(&info.archive, &mc_block_id, &block_id)
                    .await;
                __guard.start_section(115u32);
                __result
            } {
                Ok(block) => {
                    __guard.end_section(117u32);
                    return Some(Ok(block.clone()));
                }
                Err(e) => {
                    tracing::error!(
                        archive_key, % block_id, % mc_block_id,
                        "invalid archive entry: {e:?}"
                    );
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
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(checked_get_entry_by_id)),
            file!(),
            132u32,
        );
        let archive = archive;
        let mc_block_id = mc_block_id;
        let block_id = block_id;
        let (block, ref proof, ref queue_diff) = match {
            __guard.end_section(133u32);
            let __result = archive.get_entry_by_id(block_id).await;
            __guard.start_section(133u32);
            __result
        } {
            Ok(entry) => entry,
            Err(e) => anyhow::bail!("archive is corrupted: {e:?}"),
        };
        {
            __guard.end_section(147u32);
            let __result = self
                .inner
                .proof_checker
                .check_proof(CheckProof {
                    mc_block_id,
                    block: &block,
                    proof,
                    queue_diff,
                    store_on_success: true,
                })
                .await;
            __guard.start_section(147u32);
            __result
        }?;
        Ok(block)
    }
}
struct Inner {
    storage: CoreStorage,
    client: BlockchainRpcClient,
    proof_checker: ProofChecker,
    known_archives: parking_lot::Mutex<ArchivesMap>,
    config: ArchiveBlockProviderConfig,
}
impl Inner {
    async fn get_archive(&self, mc_seqno: u32) -> Option<(u32, ArchiveInfo)> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(get_archive)),
            file!(),
            165u32,
        );
        let mc_seqno = mc_seqno;
        loop {
            __guard.checkpoint(166u32);
            let mut pending = 'pending: {
                let mut guard = self.known_archives.lock();
                for (archive_key, value) in guard.iter() {
                    __guard.checkpoint(171u32);
                    match value {
                        ArchiveSlot::Downloaded(info) => {
                            if info.archive.mc_block_ids.contains_key(&mc_seqno) {
                                {
                                    __guard.end_section(175u32);
                                    return Some((*archive_key, info.clone()));
                                };
                            }
                        }
                        ArchiveSlot::Pending(task) => {
                            __guard.end_section(178u32);
                            __guard.start_section(178u32);
                            break 'pending task.clone();
                        }
                    }
                }
                let task = self.make_downloader().spawn(mc_seqno);
                guard.insert(mc_seqno, ArchiveSlot::Pending(task.clone()));
                task
            };
            let mut res = None;
            let mut finished = false;
            loop {
                __guard.checkpoint(192u32);
                match &*pending.rx.borrow_and_update() {
                    ArchiveTaskState::None => {}
                    ArchiveTaskState::Finished(archive) => {
                        res = archive.clone();
                        finished = true;
                        {
                            __guard.end_section(198u32);
                            __guard.start_section(198u32);
                            break;
                        };
                    }
                    ArchiveTaskState::Cancelled => {
                        __guard.end_section(200u32);
                        __guard.start_section(200u32);
                        break;
                    }
                }
                if {
                    __guard.end_section(202u32);
                    let __result = pending.rx.changed().await;
                    __guard.start_section(202u32);
                    __result
                }
                    .is_err()
                {
                    {
                        __guard.end_section(203u32);
                        __guard.start_section(203u32);
                        break;
                    };
                }
            }
            match self.known_archives.lock().entry(pending.archive_key) {
                btree_map::Entry::Vacant(_) => {}
                btree_map::Entry::Occupied(mut entry) => {
                    match &res {
                        None => {
                            entry.remove();
                        }
                        Some(info) => {
                            entry.insert(ArchiveSlot::Downloaded(info.clone()));
                        }
                    }
                }
            }
            if finished {
                {
                    __guard.end_section(225u32);
                    return res.map(|info| (pending.archive_key, info));
                };
            }
            tracing::warn!(mc_seqno, "archive task cancelled while in use");
            {
                __guard.end_section(230u32);
                let __result = tokio::task::yield_now().await;
                __guard.start_section(230u32);
                __result
            };
        }
    }
    fn remove_archive_if_same(&self, archive_key: u32, prev: &ArchiveInfo) -> bool {
        match self.known_archives.lock().entry(archive_key) {
            btree_map::Entry::Vacant(_) => false,
            btree_map::Entry::Occupied(entry) => {
                if matches!(
                    entry.get(), ArchiveSlot::Downloaded(info) if Arc::ptr_eq(& info
                    .archive, & prev.archive)
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
        const MAX_MC_PER_ARCHIVE: u32 = 100;
        let mut entries_remaining = 0usize;
        let mut entries_removed = 0usize;
        let mut guard = self.known_archives.lock();
        guard
            .retain(|_, archive| {
                let retain;
                match archive {
                    ArchiveSlot::Downloaded(info) => {
                        match info.archive.mc_block_ids.last_key_value() {
                            None => retain = false,
                            Some((last_mc_seqno, _)) => retain = *last_mc_seqno >= bound,
                        }
                    }
                    ArchiveSlot::Pending(task) => {
                        retain = task.archive_key.saturating_add(MAX_MC_PER_ARCHIVE)
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
            entries_remaining, entries_removed, bound, "removed known archives"
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
    from: Neighbour,
    archive: Arc<Archive>,
}
struct ArchiveDownloader {
    client: BlockchainRpcClient,
    storage: CoreStorage,
    memory_threshold: ByteSize,
}
impl ArchiveDownloader {
    fn spawn(self, mc_seqno: u32) -> ArchiveTask {
        const INTERVAL: Duration = Duration::from_secs(1);
        let (tx, rx) = watch::channel(ArchiveTaskState::None);
        let guard = scopeguard::guard(
            tx,
            move |tx| {
                tracing::warn!(mc_seqno, "cancelled preloading archive");
                tx.send_modify(|prev| {
                    if !matches!(prev, ArchiveTaskState::Finished(..)) {
                        *prev = ArchiveTaskState::Cancelled;
                    }
                });
            },
        );
        let handle = tokio::spawn(async move {
            let mut __guard = crate::__async_profile_guard__::Guard::new(
                concat!(module_path!(), "::async_block"),
                file!(),
                334u32,
            );
            tracing::debug!(mc_seqno, "started preloading archive");
            scopeguard::defer! {
                tracing::debug!(mc_seqno, "finished preloading archive");
            }
            loop {
                __guard.checkpoint(340u32);
                match {
                    __guard.end_section(341u32);
                    let __result = self.try_download(mc_seqno).await;
                    __guard.start_section(341u32);
                    __result
                } {
                    Ok(res) => {
                        let tx = scopeguard::ScopeGuard::into_inner(guard);
                        tx.send_modify(move |prev| {
                            *prev = ArchiveTaskState::Finished(res);
                        });
                        {
                            __guard.end_section(345u32);
                            __guard.start_section(345u32);
                            break;
                        };
                    }
                    Err(e) => {
                        tracing::error!(mc_seqno, "failed to preload archive {e:?}");
                        {
                            __guard.end_section(349u32);
                            let __result = tokio::time::sleep(INTERVAL).await;
                            __guard.start_section(349u32);
                            __result
                        };
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
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(try_download)),
            file!(),
            362u32,
        );
        let seqno = seqno;
        let response = {
            __guard.end_section(363u32);
            let __result = self.client.find_archive(seqno).await;
            __guard.start_section(363u32);
            __result
        }?;
        let pending = match response {
            PendingArchiveResponse::Found(pending) => pending,
            PendingArchiveResponse::TooNew => {
                __guard.end_section(366u32);
                return Ok(None);
            }
        };
        let neighbour = pending.neighbour.clone();
        let writer = self.get_archive_writer(&pending)?;
        let writer = {
            __guard.end_section(372u32);
            let __result = self.client.download_archive(pending, writer).await;
            __guard.start_section(372u32);
            __result
        }?;
        let span = tracing::Span::current();
        {
            __guard.end_section(399u32);
            let __result = tokio::task::spawn_blocking(move || {
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
                        neighbour.punish(PunishReason::Malicious);
                        return Err(e);
                    }
                    Ok(ArchiveInfo {
                        archive: Arc::new(archive),
                        from: neighbour,
                    })
                })
                .await;
            __guard.start_section(399u32);
            __result
        }?
            .map(Some)
    }
    fn get_archive_writer(&self, pending: &PendingArchive) -> Result<ArchiveWriter> {
        Ok(
            if pending.size.get() > self.memory_threshold.as_u64() {
                let file = self.storage.context().temp_files().unnamed_file().open()?;
                ArchiveWriter::File(std::io::BufWriter::new(file))
            } else {
                ArchiveWriter::Bytes(BytesMut::new().writer())
            },
        )
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
    fn get_next_block<'a>(
        &'a self,
        prev_block_id: &'a BlockId,
    ) -> Self::GetNextBlockFut<'a> {
        Box::pin(self.get_next_block_impl(prev_block_id))
    }
    fn get_block<'a>(
        &'a self,
        block_id_relation: &'a BlockIdRelation,
    ) -> Self::GetBlockFut<'a> {
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
            Self::File(file) => {
                match file.into_inner() {
                    Ok(mut file) => {
                        file.seek(std::io::SeekFrom::Start(0))?;
                        MappedFile::from_existing_file(file).map(Bytes::from_owner)
                    }
                    Err(e) => Err(e.into_error()),
                }
            }
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
