use std::fs::File;
use std::io::BufReader;
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::sync::Arc;
use anyhow::{Context, Result};
use bytes::Bytes;
use tokio::sync::broadcast;
use tycho_block_util::archive::ArchiveData;
use tycho_block_util::block::{
    BlockProofStuff, BlockProofStuffAug, BlockStuff, BlockStuffAug,
};
use tycho_block_util::queue::{QueueDiffStuff, QueueDiffStuffAug};
use tycho_types::models::*;
use tycho_util::FastHasherState;
use tycho_util::metrics::HistogramGuard;
use tycho_util::sync::{CancellationFlag, rayon_run};
pub use self::package_entry::{PackageEntryKey, PartialBlockId};
use super::util::SlotSubscriptions;
use super::{
    BlockConnectionStorage, BlockFlags, BlockHandle, BlockHandleStorage,
    BlocksCacheConfig, CoreDb, HandleCreationStatus, NewBlockMeta,
};
pub(crate) mod blobs;
mod package_entry;
pub use blobs::{ArchiveId, BlockGcStats, OpenStats};
use crate::storage::block::blobs::DEFAULT_CHUNK_SIZE;
use crate::storage::config::BlobDbConfig;
const METRIC_LOAD_BLOCK_TOTAL: &str = "tycho_storage_load_block_total";
const METRIC_BLOCK_CACHE_HIT_TOTAL: &str = "tycho_storage_block_cache_hit_total";
pub struct BlockStorage {
    blocks_cache: BlocksCache,
    block_handle_storage: Arc<BlockHandleStorage>,
    block_connection_storage: Arc<BlockConnectionStorage>,
    block_subscriptions: SlotSubscriptions<BlockId, BlockStuff>,
    store_block_data: tokio::sync::RwLock<()>,
    pub(crate) blob_storage: Arc<blobs::BlobStorage>,
}
impl BlockStorage {
    pub const DEFAULT_BLOB_CHUNK_SIZE: NonZeroU32 = NonZeroU32::new(
            DEFAULT_CHUNK_SIZE as u32,
        )
        .unwrap();
    pub async fn new(
        db: CoreDb,
        config: BlockStorageConfig,
        block_handle_storage: Arc<BlockHandleStorage>,
        block_connection_storage: Arc<BlockConnectionStorage>,
    ) -> Result<Self> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(new)),
            file!(),
            55u32,
        );
        let db = db;
        let config = config;
        let block_handle_storage = block_handle_storage;
        let block_connection_storage = block_connection_storage;
        fn weigher(_key: &BlockId, value: &BlockStuff) -> u32 {
            const BLOCK_STUFF_OVERHEAD: u32 = 1024;
            size_of::<BlockId>() as u32 + BLOCK_STUFF_OVERHEAD
                + value.data_size().try_into().unwrap_or(u32::MAX)
        }
        let blocks_cache = moka::sync::Cache::builder()
            .time_to_live(config.blocks_cache.ttl)
            .max_capacity(config.blocks_cache.size.as_u64())
            .weigher(weigher)
            .build_with_hasher(Default::default());
        let blob_storage = {
            __guard.end_section(76u32);
            let __result = blobs::BlobStorage::new(
                    db,
                    block_handle_storage.clone(),
                    &config.blobs_root,
                    config.blob_db_config.pre_create_cas_tree,
                )
                .await;
            __guard.start_section(76u32);
            __result
        }
            .map(Arc::new)?;
        Ok(Self {
            blocks_cache,
            block_handle_storage,
            block_connection_storage,
            block_subscriptions: Default::default(),
            store_block_data: Default::default(),
            blob_storage,
        })
    }
    pub fn open_stats(&self) -> &OpenStats {
        self.blob_storage.open_stats()
    }
    pub async fn wait_for_block(&self, block_id: &BlockId) -> Result<BlockStuffAug> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(wait_for_block)),
            file!(),
            95u32,
        );
        let block_id = block_id;
        let block_handle_storage = &self.block_handle_storage;
        let guard = {
            __guard.end_section(99u32);
            let __result = self.store_block_data.write().await;
            __guard.start_section(99u32);
            __result
        };
        if let Some(handle) = block_handle_storage.load_handle(block_id)
            && handle.has_data()
        {
            drop(guard);
            let block = {
                __guard.end_section(106u32);
                let __result = self.load_block_data(&handle).await;
                __guard.start_section(106u32);
                __result
            }?;
            {
                __guard.end_section(107u32);
                return Ok(BlockStuffAug::loaded(block));
            };
        }
        let rx = self.block_subscriptions.subscribe(block_id);
        drop(guard);
        let block = {
            __guard.end_section(116u32);
            let __result = rx.await;
            __guard.start_section(116u32);
            __result
        };
        Ok(BlockStuffAug::loaded(block))
    }
    pub async fn wait_for_next_block(
        &self,
        prev_block_id: &BlockId,
    ) -> Result<BlockStuffAug> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(wait_for_next_block)),
            file!(),
            120u32,
        );
        let prev_block_id = prev_block_id;
        let block_id = {
            __guard.end_section(124u32);
            let __result = self
                .block_connection_storage
                .wait_for_next1(prev_block_id)
                .await;
            __guard.start_section(124u32);
            __result
        };
        {
            __guard.end_section(126u32);
            let __result = self.wait_for_block(&block_id).await;
            __guard.start_section(126u32);
            __result
        }
    }
    pub async fn store_block_data(
        &self,
        block: &BlockStuff,
        archive_data: &ArchiveData,
        meta_data: NewBlockMeta,
    ) -> Result<StoreBlockResult> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(store_block_data)),
            file!(),
            136u32,
        );
        let block = block;
        let archive_data = archive_data;
        let meta_data = meta_data;
        let guard = {
            __guard.end_section(140u32);
            let __result = self.store_block_data.read().await;
            __guard.start_section(140u32);
            __result
        };
        let block_id = block.id();
        let (handle, status) = self
            .block_handle_storage
            .create_or_load_handle(block_id, meta_data);
        let archive_id = PackageEntryKey::block(block_id);
        let mut updated = false;
        if !handle.has_data() {
            let data = archive_data.clone_new_archive_data()?;
            metrics::histogram!("tycho_storage_store_block_data_size")
                .record(data.len() as f64);
            let lock = {
                __guard.end_section(153u32);
                let __result = handle.block_data_lock().write().await;
                __guard.start_section(153u32);
                __result
            };
            if !handle.has_data() {
                {
                    __guard.end_section(155u32);
                    let __result = self
                        .blob_storage
                        .add_data(&archive_id, data, &lock)
                        .await;
                    __guard.start_section(155u32);
                    __result
                }?;
                if handle.meta().add_flags(BlockFlags::HAS_DATA) {
                    self.block_handle_storage.store_handle(&handle, false);
                    updated = true;
                }
            }
        }
        self.block_subscriptions.notify(block_id, block);
        drop(guard);
        self.blocks_cache.insert(*block_id, block.clone());
        Ok(StoreBlockResult {
            handle,
            updated,
            new: status == HandleCreationStatus::Created,
        })
    }
    pub async fn load_block_data(&self, handle: &BlockHandle) -> Result<BlockStuff> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(load_block_data)),
            file!(),
            178u32,
        );
        let handle = handle;
        metrics::counter!(METRIC_LOAD_BLOCK_TOTAL).increment(1);
        const BIG_DATA_THRESHOLD: usize = 1 << 20;
        let _histogram = HistogramGuard::begin("tycho_storage_load_block_data_time");
        anyhow::ensure!(
            handle.has_data(), BlockStorageError::BlockDataNotFound(handle.id()
            .as_short_id())
        );
        if let Some(block) = self.blocks_cache.get(handle.id()) {
            metrics::counter!(METRIC_BLOCK_CACHE_HIT_TOTAL).increment(1);
            {
                __guard.end_section(193u32);
                return Ok(block.clone());
            };
        }
        let data = {
            __guard.end_section(199u32);
            let __result = self
                .blob_storage
                .get_block_data_decompressed(
                    handle,
                    &PackageEntryKey::block(handle.id()),
                )
                .await;
            __guard.start_section(199u32);
            __result
        }?;
        if data.len() < BIG_DATA_THRESHOLD {
            BlockStuff::deserialize(handle.id(), data.as_ref())
        } else {
            let handle = handle.clone();
            {
                __guard.end_section(205u32);
                let __result = rayon_run(move || BlockStuff::deserialize(
                        handle.id(),
                        data.as_ref(),
                    ))
                    .await;
                __guard.start_section(205u32);
                __result
            }
        }
    }
    pub async fn load_block_data_decompressed(
        &self,
        handle: &BlockHandle,
    ) -> Result<Bytes> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(load_block_data_decompressed)),
            file!(),
            209u32,
        );
        let handle = handle;
        anyhow::ensure!(
            handle.has_data(), BlockStorageError::BlockDataNotFound(handle.id()
            .as_short_id())
        );
        {
            __guard.end_section(217u32);
            let __result = self
                .blob_storage
                .get_block_data_decompressed(
                    handle,
                    &PackageEntryKey::block(handle.id()),
                )
                .await;
            __guard.start_section(217u32);
            __result
        }
    }
    pub async fn list_blocks(
        &self,
        continuation: Option<BlockIdShort>,
    ) -> Result<(Vec<BlockId>, Option<BlockIdShort>)> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(list_blocks)),
            file!(),
            223u32,
        );
        let continuation = continuation;
        {
            __guard.end_section(224u32);
            let __result = self.blob_storage.list_blocks(continuation).await;
            __guard.start_section(224u32);
            __result
        }
    }
    pub fn list_archive_ids(&self) -> Vec<u32> {
        self.blob_storage.list_archive_ids()
    }
    pub async fn load_block_data_range(
        &self,
        handle: &BlockHandle,
        offset: u64,
        length: u64,
    ) -> Result<Option<Bytes>> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(load_block_data_range)),
            file!(),
            236u32,
        );
        let handle = handle;
        let offset = offset;
        let length = length;
        anyhow::ensure!(
            handle.has_data(), BlockStorageError::BlockDataNotFound(handle.id()
            .as_short_id())
        );
        {
            __guard.end_section(243u32);
            let __result = self
                .blob_storage
                .get_block_data_range(handle, offset, length)
                .await;
            __guard.start_section(243u32);
            __result
        }
    }
    pub fn get_compressed_block_data_size(
        &self,
        handle: &BlockHandle,
    ) -> Result<Option<u64>> {
        let key = PackageEntryKey::block(handle.id());
        Ok(self.blob_storage.blocks().get_size(&key)?)
    }
    pub async fn find_mc_block_data(&self, mc_seqno: u32) -> Result<Option<Block>> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(find_mc_block_data)),
            file!(),
            251u32,
        );
        let mc_seqno = mc_seqno;
        {
            __guard.end_section(252u32);
            let __result = self.blob_storage.find_mc_block_data(mc_seqno).await;
            __guard.start_section(252u32);
            __result
        }
    }
    pub async fn store_block_proof(
        &self,
        proof: &BlockProofStuffAug,
        handle: MaybeExistingHandle,
    ) -> Result<StoreBlockResult> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(store_block_proof)),
            file!(),
            261u32,
        );
        let proof = proof;
        let handle = handle;
        let block_id = proof.id();
        if let MaybeExistingHandle::Existing(handle) = &handle && handle.id() != block_id
        {
            anyhow::bail!(
                BlockStorageError::BlockHandleIdMismatch { expected : block_id
                .as_short_id(), actual : handle.id().as_short_id(), }
            );
        }
        let (handle, status) = match handle {
            MaybeExistingHandle::Existing(handle) => {
                (handle, HandleCreationStatus::Fetched)
            }
            MaybeExistingHandle::New(meta_data) => {
                self.block_handle_storage.create_or_load_handle(block_id, meta_data)
            }
        };
        let mut updated = false;
        let archive_id = PackageEntryKey::proof(block_id);
        if !handle.has_proof() {
            let data = proof.clone_new_archive_data()?;
            let lock = {
                __guard.end_section(284u32);
                let __result = handle.proof_data_lock().write().await;
                __guard.start_section(284u32);
                __result
            };
            if !handle.has_proof() {
                {
                    __guard.end_section(286u32);
                    let __result = self
                        .blob_storage
                        .add_data(&archive_id, data, &lock)
                        .await;
                    __guard.start_section(286u32);
                    __result
                }?;
                if handle.meta().add_flags(BlockFlags::HAS_PROOF) {
                    self.block_handle_storage.store_handle(&handle, false);
                    updated = true;
                }
            }
        }
        Ok(StoreBlockResult {
            handle,
            updated,
            new: status == HandleCreationStatus::Created,
        })
    }
    pub async fn load_block_proof(
        &self,
        handle: &BlockHandle,
    ) -> Result<BlockProofStuff> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(load_block_proof)),
            file!(),
            301u32,
        );
        let handle = handle;
        let raw_proof = {
            __guard.end_section(302u32);
            let __result = self.load_block_proof_raw(handle).await;
            __guard.start_section(302u32);
            __result
        }?;
        BlockProofStuff::deserialize(handle.id(), &raw_proof)
    }
    pub async fn load_block_proof_raw(&self, handle: &BlockHandle) -> Result<Bytes> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(load_block_proof_raw)),
            file!(),
            306u32,
        );
        let handle = handle;
        anyhow::ensure!(
            handle.has_proof(), BlockStorageError::BlockProofNotFound(handle.id()
            .as_short_id())
        );
        {
            __guard.end_section(314u32);
            let __result = self
                .blob_storage
                .get_block_data_decompressed(
                    handle,
                    &PackageEntryKey::proof(handle.id()),
                )
                .await;
            __guard.start_section(314u32);
            __result
        }
    }
    pub async fn store_queue_diff(
        &self,
        queue_diff: &QueueDiffStuffAug,
        handle: MaybeExistingHandle,
    ) -> Result<StoreBlockResult> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(store_queue_diff)),
            file!(),
            323u32,
        );
        let queue_diff = queue_diff;
        let handle = handle;
        let block_id = queue_diff.block_id();
        if let MaybeExistingHandle::Existing(handle) = &handle && handle.id() != block_id
        {
            anyhow::bail!(
                BlockStorageError::BlockHandleIdMismatch { expected : block_id
                .as_short_id(), actual : handle.id().as_short_id(), }
            );
        }
        let (handle, status) = match handle {
            MaybeExistingHandle::Existing(handle) => {
                (handle, HandleCreationStatus::Fetched)
            }
            MaybeExistingHandle::New(meta_data) => {
                self.block_handle_storage.create_or_load_handle(block_id, meta_data)
            }
        };
        let mut updated = false;
        let archive_id = PackageEntryKey::queue_diff(block_id);
        if !handle.has_queue_diff() {
            let data = queue_diff.clone_new_archive_data()?;
            let lock = {
                __guard.end_section(346u32);
                let __result = handle.queue_diff_data_lock().write().await;
                __guard.start_section(346u32);
                __result
            };
            if !handle.has_queue_diff() {
                {
                    __guard.end_section(348u32);
                    let __result = self
                        .blob_storage
                        .add_data(&archive_id, data, &lock)
                        .await;
                    __guard.start_section(348u32);
                    __result
                }?;
                if handle.meta().add_flags(BlockFlags::HAS_QUEUE_DIFF) {
                    self.block_handle_storage.store_handle(&handle, false);
                    updated = true;
                }
            }
        }
        Ok(StoreBlockResult {
            handle,
            updated,
            new: status == HandleCreationStatus::Created,
        })
    }
    pub async fn load_queue_diff(&self, handle: &BlockHandle) -> Result<QueueDiffStuff> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(load_queue_diff)),
            file!(),
            363u32,
        );
        let handle = handle;
        const BIG_DATA_THRESHOLD: usize = 100 << 10;
        let data = {
            __guard.end_section(366u32);
            let __result = self.load_queue_diff_raw(handle).await;
            __guard.start_section(366u32);
            __result
        }?;
        if data.len() < BIG_DATA_THRESHOLD {
            QueueDiffStuff::deserialize(handle.id(), &data)
        } else {
            let handle = handle.clone();
            {
                __guard.end_section(372u32);
                let __result = rayon_run(move || QueueDiffStuff::deserialize(
                        handle.id(),
                        data.as_ref(),
                    ))
                    .await;
                __guard.start_section(372u32);
                __result
            }
        }
    }
    pub async fn load_queue_diff_raw(&self, handle: &BlockHandle) -> Result<Bytes> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(load_queue_diff_raw)),
            file!(),
            376u32,
        );
        let handle = handle;
        anyhow::ensure!(
            handle.has_queue_diff(), BlockStorageError::QueueDiffNotFound(handle.id()
            .as_short_id())
        );
        {
            __guard.end_section(384u32);
            let __result = self
                .blob_storage
                .get_block_data_decompressed(
                    handle,
                    &PackageEntryKey::queue_diff(handle.id()),
                )
                .await;
            __guard.start_section(384u32);
            __result
        }
    }
    pub async fn move_into_archive(
        &self,
        handle: &BlockHandle,
        mc_is_key_block: bool,
    ) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(move_into_archive)),
            file!(),
            393u32,
        );
        let handle = handle;
        let mc_is_key_block = mc_is_key_block;
        {
            __guard.end_section(396u32);
            let __result = self
                .blob_storage
                .move_into_archive(handle, mc_is_key_block)
                .await;
            __guard.start_section(396u32);
            __result
        }
    }
    pub async fn wait_for_archive_commit(&self) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(wait_for_archive_commit)),
            file!(),
            399u32,
        );
        {
            __guard.end_section(400u32);
            let __result = self.blob_storage.wait_for_archive_commit().await;
            __guard.start_section(400u32);
            __result
        }
    }
    /// Returns a corresponding archive id for the specified masterchain seqno.
    pub fn get_archive_id(&self, mc_seqno: u32) -> ArchiveId {
        self.blob_storage.get_archive_id(mc_seqno)
    }
    pub fn get_archive_size(&self, id: u32) -> Result<Option<usize>> {
        self.blob_storage.get_archive_size(id)
    }
    pub fn get_archive_reader(&self, id: u32) -> Result<Option<BufReader<File>>> {
        self.blob_storage.get_archive_reader(id)
    }
    /// Get the complete archive (compressed).
    ///
    /// Must not be used for anything other than tests.
    #[cfg(any(test, feature = "test"))]
    pub async fn get_archive_compressed_full(&self, id: u32) -> Result<Option<Bytes>> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(get_archive_compressed_full)),
            file!(),
            420u32,
        );
        let id = id;
        {
            __guard.end_section(421u32);
            let __result = self.blob_storage.get_archive_full(id).await;
            __guard.start_section(421u32);
            __result
        }
    }
    /// Get a chunk of the archive at the specified offset.
    pub async fn get_archive_chunk(&self, id: u32, offset: u64) -> Result<Bytes> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(get_archive_chunk)),
            file!(),
            425u32,
        );
        let id = id;
        let offset = offset;
        {
            __guard.end_section(426u32);
            let __result = self.blob_storage.get_archive_chunk(id, offset).await;
            __guard.start_section(426u32);
            __result
        }
    }
    pub fn subscribe_to_archive_ids(&self) -> broadcast::Receiver<u32> {
        self.blob_storage.subscribe_to_archive_ids()
    }
    pub async fn remove_outdated_archives(&self, until_id: u32) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(remove_outdated_archives)),
            file!(),
            433u32,
        );
        let until_id = until_id;
        {
            __guard.end_section(434u32);
            let __result = self.blob_storage.remove_outdated_archives(until_id).await;
            __guard.start_section(434u32);
            __result
        }
    }
    #[tracing::instrument(skip(self, max_blocks_per_batch))]
    pub async fn remove_outdated_blocks(
        &self,
        mc_seqno: u32,
        max_blocks_per_batch: Option<usize>,
    ) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(remove_outdated_blocks)),
            file!(),
            444u32,
        );
        let mc_seqno = mc_seqno;
        let max_blocks_per_batch = max_blocks_per_batch;
        if mc_seqno == 0 {
            {
                __guard.end_section(446u32);
                return Ok(());
            };
        }
        tracing::info!("started blocks GC for mc_block {mc_seqno}");
        let block = {
            __guard.end_section(454u32);
            let __result = self.blob_storage.find_mc_block_data(mc_seqno).await;
            __guard.start_section(454u32);
            __result
        }
            .context("failed to load target block data")?
            .context("target block not found")?;
        let shard_heights = {
            let extra = block.extra.load()?;
            let custom = extra.custom.context("mc block extra not found")?.load()?;
            custom
                .shards
                .latest_blocks()
                .map(|id| id.map(|id| (id.shard, id.seqno)))
                .collect::<Result<_, tycho_types::error::Error>>()?
        };
        let total_cached_handles_removed = self
            .block_handle_storage
            .gc_handles_cache(mc_seqno, &shard_heights);
        let cancelled = CancellationFlag::new();
        scopeguard::defer! {
            cancelled.cancel();
        }
        let BlockGcStats { mc_blocks_removed, total_blocks_removed } = {
            __guard.end_section(496u32);
            let __result = tokio::task::spawn_blocking({
                    let blob_storage = self.blob_storage.clone();
                    let cancelled = cancelled.clone();
                    move || {
                        blobs::remove_blocks(
                            &blob_storage,
                            max_blocks_per_batch,
                            mc_seqno,
                            shard_heights,
                            Some(&cancelled),
                        )
                    }
                })
                .await;
            __guard.start_section(496u32);
            __result
        }??;
        tracing::info!(
            total_cached_handles_removed, mc_blocks_removed, total_blocks_removed,
            "finished blocks GC"
        );
        Ok(())
    }
    #[cfg(any(test, feature = "test"))]
    pub fn blob_storage(&self) -> &blobs::BlobStorage {
        &self.blob_storage
    }
}
#[derive(Clone)]
pub enum MaybeExistingHandle {
    Existing(BlockHandle),
    New(NewBlockMeta),
}
impl From<BlockHandle> for MaybeExistingHandle {
    fn from(handle: BlockHandle) -> Self {
        Self::Existing(handle)
    }
}
impl From<NewBlockMeta> for MaybeExistingHandle {
    fn from(meta_data: NewBlockMeta) -> Self {
        Self::New(meta_data)
    }
}
pub struct StoreBlockResult {
    pub handle: BlockHandle,
    pub updated: bool,
    pub new: bool,
}
pub struct BlockStorageConfig {
    pub blocks_cache: BlocksCacheConfig,
    pub blobs_root: PathBuf,
    pub blob_db_config: BlobDbConfig,
}
type BlocksCache = moka::sync::Cache<BlockId, BlockStuff, FastHasherState>;
#[derive(thiserror::Error, Debug)]
enum BlockStorageError {
    #[error("Block data not found for block: {0}")]
    BlockDataNotFound(BlockIdShort),
    #[error("Block proof not found for block: {0}")]
    BlockProofNotFound(BlockIdShort),
    #[error("Queue diff not found for block: {0}")]
    QueueDiffNotFound(BlockIdShort),
    #[error("Block handle id mismatch: expected {expected}, got {actual}")]
    BlockHandleIdMismatch { expected: BlockIdShort, actual: BlockIdShort },
}
#[cfg(test)]
mod tests {
    use std::pin::pin;
    use blobs::*;
    use tycho_block_util::archive::{ArchiveEntryType, WithArchiveData};
    use tycho_storage::StorageContext;
    use tycho_types::prelude::*;
    use tycho_util::FastHashMap;
    use tycho_util::futures::JoinTask;
    use super::*;
    use crate::storage::{BlockConnection, CoreStorage, CoreStorageConfig};
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn parallel_store_data() -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(parallel_store_data)),
            file!(),
            577u32,
        );
        let (ctx, _tmp_dir) = {
            __guard.end_section(578u32);
            let __result = StorageContext::new_temp().await;
            __guard.start_section(578u32);
            __result
        }?;
        let storage = {
            __guard.end_section(579u32);
            let __result = CoreStorage::open(ctx, CoreStorageConfig::new_potato()).await;
            __guard.start_section(579u32);
            __result
        }?;
        let shard = ShardIdent::MASTERCHAIN;
        for seqno in 0..1000 {
            __guard.checkpoint(582u32);
            let block = BlockStuff::new_empty(shard, seqno);
            let block = {
                let data = BocRepr::encode_rayon(block.as_ref()).unwrap();
                WithArchiveData::new(block, data)
            };
            let block_id = block.id();
            let proof = BlockProofStuff::new_empty(block_id);
            let proof = {
                let data = BocRepr::encode_rayon(proof.as_ref()).unwrap();
                WithArchiveData::new(proof, data)
            };
            let queue_diff = QueueDiffStuff::builder(shard, seqno, &HashBytes::ZERO)
                .serialize()
                .build(block_id);
            let block_meta = NewBlockMeta {
                is_key_block: shard.is_masterchain() && seqno == 0,
                gen_utime: 0,
                ref_by_mc_seqno: seqno,
            };
            let store_block_data = || {
                let storage = storage.clone();
                JoinTask::new(async move {
                    let mut __guard = crate::__async_profile_guard__::Guard::new(
                        concat!(module_path!(), "::async_block"),
                        file!(),
                        608u32,
                    );
                    let res = {
                        __guard.end_section(612u32);
                        let __result = storage
                            .block_storage()
                            .store_block_data(&block, &block.archive_data, block_meta)
                            .await;
                        __guard.start_section(612u32);
                        __result
                    }?;
                    Ok::<_, anyhow::Error>(res.handle)
                })
            };
            let store_proof_and_queue = || {
                let storage = storage.clone();
                let proof = proof.clone();
                let queue_diff = queue_diff.clone();
                JoinTask::new(async move {
                    let mut __guard = crate::__async_profile_guard__::Guard::new(
                        concat!(module_path!(), "::async_block"),
                        file!(),
                        622u32,
                    );
                    if rand::random::<bool>() {
                        {
                            __guard.end_section(624u32);
                            let __result = tokio::task::yield_now().await;
                            __guard.start_section(624u32);
                            __result
                        };
                    }
                    let res = {
                        __guard.end_section(630u32);
                        let __result = storage
                            .block_storage()
                            .store_block_proof(
                                &proof,
                                MaybeExistingHandle::New(block_meta),
                            )
                            .await;
                        __guard.start_section(630u32);
                        __result
                    }?;
                    if rand::random::<bool>() {
                        {
                            __guard.end_section(633u32);
                            let __result = tokio::task::yield_now().await;
                            __guard.start_section(633u32);
                            __result
                        };
                    }
                    let res = {
                        __guard.end_section(639u32);
                        let __result = storage
                            .block_storage()
                            .store_queue_diff(&queue_diff, res.handle.into())
                            .await;
                        __guard.start_section(639u32);
                        __result
                    }?;
                    if rand::random::<bool>() {
                        {
                            __guard.end_section(642u32);
                            let __result = tokio::task::yield_now().await;
                            __guard.start_section(642u32);
                            __result
                        };
                    }
                    Ok::<_, anyhow::Error>(res.handle)
                })
            };
            let (data_res, proof_and_queue_res) = {
                __guard.end_section(662u32);
                let __result = async move {
                    let mut __guard = crate::__async_profile_guard__::Guard::new(
                        concat!(module_path!(), "::async_block"),
                        file!(),
                        649u32,
                    );
                    let data_fut = pin!(store_block_data());
                    let proof_and_queue_fut = pin!(
                        async { tokio::select! { left = store_proof_and_queue() => left,
                        right = store_proof_and_queue() => right, } }
                    );
                    let (data, other) = {
                        __guard.end_section(658u32);
                        let __result = futures_util::future::join(
                                data_fut,
                                proof_and_queue_fut,
                            )
                            .await;
                        __guard.start_section(658u32);
                        __result
                    };
                    Ok::<_, anyhow::Error>((data?, other?))
                }
                    .await;
                __guard.start_section(662u32);
                __result
            }?;
            assert!(
                std::ptr::addr_eq(arc_swap::RefCnt::as_ptr(& data_res),
                arc_swap::RefCnt::as_ptr(& proof_and_queue_res))
            );
            assert!(data_res.has_all_block_parts());
        }
        Ok(())
    }
    #[tokio::test]
    async fn blocks_gc() -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(blocks_gc)),
            file!(),
            675u32,
        );
        const GARBAGE: Bytes = Bytes::from_static(b"garbage");
        const ENTRY_TYPES: [ArchiveEntryType; 3] = [
            ArchiveEntryType::Block,
            ArchiveEntryType::Proof,
            ArchiveEntryType::QueueDiff,
        ];
        const CONNECTION_TYPES: [BlockConnection; 2] = [
            BlockConnection::Prev1,
            BlockConnection::Next1,
        ];
        let (ctx, _tmp_dir) = {
            __guard.end_section(685u32);
            let __result = StorageContext::new_temp().await;
            __guard.start_section(685u32);
            __result
        }?;
        let storage = {
            __guard.end_section(686u32);
            let __result = CoreStorage::open(ctx, CoreStorageConfig::new_potato()).await;
            __guard.start_section(686u32);
            __result
        }?;
        let blocks = storage.block_storage();
        let block_handles = storage.block_handle_storage();
        let block_connections = storage.block_connection_storage();
        let mut shard_block_ids = FastHashMap::<ShardIdent, Vec<BlockId>>::default();
        for shard in [ShardIdent::MASTERCHAIN, ShardIdent::BASECHAIN] {
            __guard.checkpoint(694u32);
            let entry = shard_block_ids.entry(shard).or_default();
            for seqno in 0..100 {
                __guard.checkpoint(697u32);
                let block_id = BlockId {
                    shard,
                    seqno,
                    root_hash: HashBytes(rand::random()),
                    file_hash: HashBytes(rand::random()),
                };
                entry.push(block_id);
                let (handle, _) = block_handles
                    .create_or_load_handle(
                        &block_id,
                        NewBlockMeta {
                            is_key_block: shard.is_masterchain() && seqno == 0,
                            gen_utime: 0,
                            ref_by_mc_seqno: seqno,
                        },
                    );
                let lock = {
                    __guard.end_section(711u32);
                    let __result = handle.block_data_lock().write().await;
                    __guard.start_section(711u32);
                    __result
                };
                for ty in ENTRY_TYPES {
                    __guard.checkpoint(713u32);
                    {
                        __guard.end_section(717u32);
                        let __result = blocks
                            .blob_storage
                            .add_data(&(block_id, ty).into(), GARBAGE, &lock)
                            .await;
                        __guard.start_section(717u32);
                        __result
                    }?;
                }
                for direction in CONNECTION_TYPES {
                    __guard.checkpoint(719u32);
                    block_connections.store_connection(&handle, direction, &block_id);
                }
                handle.meta().add_flags(BlockFlags::HAS_ALL_BLOCK_PARTS);
                block_handles.store_handle(&handle, false);
            }
        }
        let stats = blobs::remove_blocks(
            &blocks.blob_storage,
            None,
            70,
            [(ShardIdent::BASECHAIN, 50)].into(),
            None,
        )?;
        assert_eq!(
            stats, BlockGcStats { mc_blocks_removed : 69, total_blocks_removed : 69 + 49,
            }
        );
        let removed_ranges = FastHashMap::from_iter([
            (ShardIdent::MASTERCHAIN, vec![1..= 69]),
            (ShardIdent::BASECHAIN, vec![1..= 49]),
        ]);
        for (shard, block_ids) in shard_block_ids {
            __guard.checkpoint(745u32);
            let removed_ranges = removed_ranges.get(&shard).unwrap();
            for block_id in block_ids {
                __guard.checkpoint(748u32);
                let must_be_removed = 'removed: {
                    for range in removed_ranges {
                        __guard.checkpoint(750u32);
                        if range.contains(&block_id.seqno) {
                            {
                                __guard.end_section(752u32);
                                __guard.start_section(752u32);
                                break 'removed true;
                            };
                        }
                    }
                    false
                };
                let handle = block_handles.load_handle(&block_id);
                assert_eq!(handle.is_none(), must_be_removed);
                for ty in ENTRY_TYPES {
                    __guard.checkpoint(761u32);
                    let key = PackageEntryKey::from((block_id, ty));
                    let exists_in_cas = blocks
                        .blob_storage()
                        .blocks()
                        .read_index_state()
                        .contains_key(&key);
                    assert_eq!(! exists_in_cas, must_be_removed);
                }
                for direction in CONNECTION_TYPES {
                    __guard.checkpoint(772u32);
                    let connection = block_connections
                        .load_connection(&block_id, direction);
                    assert_eq!(connection.is_none(), must_be_removed);
                }
            }
        }
        let stats = blobs::remove_blocks(
            &blocks.blob_storage,
            None,
            71,
            [(ShardIdent::BASECHAIN, 51)].into(),
            None,
        )?;
        assert_eq!(
            stats, BlockGcStats { mc_blocks_removed : 1, total_blocks_removed : 2, }
        );
        let stats = blobs::remove_blocks(
            &blocks.blob_storage,
            None,
            71,
            [(ShardIdent::BASECHAIN, 51)].into(),
            None,
        )?;
        assert_eq!(
            stats, BlockGcStats { mc_blocks_removed : 0, total_blocks_removed : 0, }
        );
        Ok(())
    }
}
