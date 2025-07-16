use std::num::NonZeroU32;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;
use tokio::sync::broadcast;
use tycho_block_util::archive::ArchiveData;
use tycho_block_util::block::{BlockProofStuff, BlockProofStuffAug, BlockStuff, BlockStuffAug};
use tycho_block_util::queue::{QueueDiffStuff, QueueDiffStuffAug};
use tycho_types::models::*;
use tycho_util::FastHasherState;
use tycho_util::metrics::HistogramGuard;
use tycho_util::sync::{CancellationFlag, rayon_run};

pub use self::package_entry::{PackageEntryKey, PartialBlockId};
use super::util::SlotSubscriptions;
use super::{
    BlockConnectionStorage, BlockFlags, BlockHandle, BlockHandleStorage, BlocksCacheConfig, CoreDb,
    HandleCreationStatus, NewBlockMeta,
};

pub(crate) mod blobs;
mod package_entry;

pub use blobs::{ArchiveId, BlockGcStats};

use crate::storage::config::BlobDbConfig;

const METRIC_LOAD_BLOCK_TOTAL: &str = "tycho_storage_load_block_total";
const METRIC_BLOCK_CACHE_HIT_TOTAL: &str = "tycho_storage_block_cache_hit_total";

pub struct BlockStorage {
    blocks_cache: BlocksCache,
    block_handle_storage: Arc<BlockHandleStorage>,
    block_connection_storage: Arc<BlockConnectionStorage>,
    block_subscriptions: SlotSubscriptions<BlockId, BlockStuff>,
    store_block_data: tokio::sync::RwLock<()>,
    blob_storage: blobs::BlobStorage,
}

impl BlockStorage {
    // === Init stuff ===

    pub async fn new(
        db: CoreDb,
        config: BlockStorageConfig,
        block_handle_storage: Arc<BlockHandleStorage>,
        block_connection_storage: Arc<BlockConnectionStorage>,
    ) -> Result<Self> {
        fn weigher(_key: &BlockId, value: &BlockStuff) -> u32 {
            const BLOCK_STUFF_OVERHEAD: u32 = 1024; // 1 KB

            std::mem::size_of::<BlockId>() as u32
                + BLOCK_STUFF_OVERHEAD
                + value.data_size().try_into().unwrap_or(u32::MAX)
        }

        let blocks_cache = moka::sync::Cache::builder()
            .time_to_live(config.blocks_cache.ttl)
            .max_capacity(config.blocks_cache.size.as_u64())
            .weigher(weigher)
            .build_with_hasher(Default::default());

        let blob_storage = blobs::BlobStorage::new(
            db,
            block_handle_storage.clone(),
            &config.blobs_root,
            config.blob_db_config.pre_create_cas_tree,
        )?;

        blob_storage.preload_archive_ids().await?;

        Ok(Self {
            blocks_cache,
            block_handle_storage,
            block_connection_storage,
            block_subscriptions: Default::default(),
            store_block_data: Default::default(),
            blob_storage,
        })
    }

    pub fn archive_chunk_size(&self) -> NonZeroU32 {
        blobs::BlobStorage::archive_chunk_size()
    }

    /// Iterates over all archives and preloads their ids into memory.
    pub async fn preload_archive_ids(&self) -> Result<()> {
        self.blob_storage.preload_archive_ids().await
    }

    // === Subscription stuff ===

    pub async fn wait_for_block(&self, block_id: &BlockId) -> Result<BlockStuffAug> {
        let block_handle_storage = &self.block_handle_storage;

        // Take an exclusive lock to prevent any block data from being stored
        let guard = self.store_block_data.write().await;

        // Try to load the block data
        if let Some(handle) = block_handle_storage.load_handle(block_id)
            && handle.has_data()
        {
            drop(guard);
            let block = self.load_block_data(&handle).await?;
            return Ok(BlockStuffAug::loaded(block));
        }

        // Add subscription for the block and drop the lock
        let rx = self.block_subscriptions.subscribe(block_id);

        // NOTE: Guard must be dropped before awaiting
        drop(guard);

        let block = rx.await;
        Ok(BlockStuffAug::loaded(block))
    }

    pub async fn wait_for_next_block(&self, prev_block_id: &BlockId) -> Result<BlockStuffAug> {
        let block_id = self
            .block_connection_storage
            .wait_for_next1(prev_block_id)
            .await;

        self.wait_for_block(&block_id).await
    }

    // === Block data ===

    pub async fn store_block_data(
        &self,
        block: &BlockStuff,
        archive_data: &ArchiveData,
        meta_data: NewBlockMeta,
    ) -> Result<StoreBlockResult> {
        // NOTE: Any amount of blocks can be stored concurrently,
        // but the subscription lock can be acquired only while
        // no block data is being stored.
        let guard = self.store_block_data.read().await;

        let block_id = block.id();
        let (handle, status) = self
            .block_handle_storage
            .create_or_load_handle(block_id, meta_data);

        let archive_id = PackageEntryKey::block(block_id);
        let mut updated = false;
        if !handle.has_data() {
            let data = archive_data.as_new_archive_data()?;
            metrics::histogram!("tycho_storage_store_block_data_size").record(data.len() as f64);

            let lock = handle.block_data_lock().write().await;
            if !handle.has_data() {
                self.blob_storage.add_data(&archive_id, data, &lock).await?;
                if handle.meta().add_flags(BlockFlags::HAS_DATA) {
                    self.block_handle_storage.store_handle(&handle, false);
                    updated = true;
                }
            }
        }

        // TODO: only notify subscribers if `updated`?
        self.block_subscriptions.notify(block_id, block);

        drop(guard);

        // Update block cache
        self.blocks_cache.insert(*block_id, block.clone());

        Ok(StoreBlockResult {
            handle,
            updated,
            new: status == HandleCreationStatus::Created,
        })
    }

    pub async fn load_block_data(&self, handle: &BlockHandle) -> Result<BlockStuff> {
        metrics::counter!(METRIC_LOAD_BLOCK_TOTAL).increment(1);

        const BIG_DATA_THRESHOLD: usize = 1 << 20; // 1 MB

        let _histogram = HistogramGuard::begin("tycho_storage_load_block_data_time");

        if !handle.has_data() {
            return Err(BlockStorageError::BlockDataNotFound.into());
        }

        // Fast path - lookup in cache
        if let Some(block) = self.blocks_cache.get(handle.id()) {
            metrics::counter!(METRIC_BLOCK_CACHE_HIT_TOTAL).increment(1);
            return Ok(block.clone());
        }

        let data = self
            .blob_storage
            .get_block_data_decompressed(handle, &PackageEntryKey::block(handle.id()))
            .await?;

        if data.len() > BIG_DATA_THRESHOLD {
            BlockStuff::deserialize(handle.id(), data.as_ref())
        } else {
            let handle = handle.clone();
            rayon_run(move || BlockStuff::deserialize(handle.id(), data.as_ref())).await
        }
    }

    pub async fn load_block_data_decompressed(&self, handle: &BlockHandle) -> Result<Bytes> {
        if !handle.has_data() {
            return Err(BlockStorageError::BlockDataNotFound.into());
        }

        self.blob_storage
            .get_block_data_decompressed(handle, &PackageEntryKey::block(handle.id()))
            .await
    }

    pub async fn list_blocks(
        &self,
        continuation: Option<BlockIdShort>,
    ) -> Result<(Vec<BlockId>, Option<BlockIdShort>)> {
        self.blob_storage.list_blocks(continuation).await
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
        if !handle.has_data() {
            return Err(BlockStorageError::BlockDataNotFound.into());
        }
        self.blob_storage
            .get_block_data_range(handle, offset, length)
            .await
    }

    pub fn get_compressed_block_data_size(&self, handle: &BlockHandle) -> Result<Option<u64>> {
        let key = PackageEntryKey::block(handle.id());
        Ok(self.blob_storage.blocks().size(&key)?)
    }

    pub fn find_mc_block_data(&self, mc_seqno: u32) -> Result<Option<Block>> {
        self.blob_storage.find_mc_block_data(mc_seqno)
    }

    // === Block proof ===

    pub async fn store_block_proof(
        &self,
        proof: &BlockProofStuffAug,
        handle: MaybeExistingHandle,
    ) -> Result<StoreBlockResult> {
        let block_id = proof.id();
        if matches!(&handle, MaybeExistingHandle::Existing(handle) if handle.id() != block_id) {
            return Err(BlockStorageError::BlockHandleIdMismatch.into());
        }

        let (handle, status) = match handle {
            MaybeExistingHandle::Existing(handle) => (handle, HandleCreationStatus::Fetched),
            MaybeExistingHandle::New(meta_data) => self
                .block_handle_storage
                .create_or_load_handle(block_id, meta_data),
        };

        let mut updated = false;
        let archive_id = PackageEntryKey::proof(block_id);
        if !handle.has_proof() {
            let data = proof.as_new_archive_data()?;

            let lock = handle.proof_data_lock().write().await;
            if !handle.has_proof() {
                self.blob_storage.add_data(&archive_id, data, &lock).await?;
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

    pub async fn load_block_proof(&self, handle: &BlockHandle) -> Result<BlockProofStuff> {
        let raw_proof = self.load_block_proof_raw(handle).await?;
        BlockProofStuff::deserialize(handle.id(), &raw_proof)
    }

    pub async fn load_block_proof_raw(&self, handle: &BlockHandle) -> Result<Bytes> {
        if !handle.has_proof() {
            return Err(BlockStorageError::BlockProofNotFound.into());
        }

        self.blob_storage
            .get_block_data_decompressed(handle, &PackageEntryKey::proof(handle.id()))
            .await
    }

    // === Queue diff ===

    pub async fn store_queue_diff(
        &self,
        queue_diff: &QueueDiffStuffAug,
        handle: MaybeExistingHandle,
    ) -> Result<StoreBlockResult> {
        let block_id = queue_diff.block_id();
        if matches!(&handle, MaybeExistingHandle::Existing(handle) if handle.id() != block_id) {
            return Err(BlockStorageError::BlockHandleIdMismatch.into());
        }

        let (handle, status) = match handle {
            MaybeExistingHandle::Existing(handle) => (handle, HandleCreationStatus::Fetched),
            MaybeExistingHandle::New(meta_data) => self
                .block_handle_storage
                .create_or_load_handle(block_id, meta_data),
        };

        let mut updated = false;
        let archive_id = PackageEntryKey::queue_diff(block_id);
        if !handle.has_queue_diff() {
            let data = queue_diff.as_new_archive_data()?;

            let lock = handle.queue_diff_data_lock().write().await;
            if !handle.has_queue_diff() {
                self.blob_storage.add_data(&archive_id, data, &lock).await?;
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
        let raw_diff = self.load_queue_diff_raw(handle).await?;
        QueueDiffStuff::deserialize(handle.id(), &raw_diff)
    }

    pub async fn load_queue_diff_raw(&self, handle: &BlockHandle) -> Result<Bytes> {
        if !handle.has_queue_diff() {
            return Err(BlockStorageError::QueueDiffNotFound.into());
        }

        self.blob_storage
            .get_block_data_decompressed(handle, &PackageEntryKey::queue_diff(handle.id()))
            .await
    }

    // === Archive stuff ===

    pub async fn move_into_archive(
        &self,
        handle: &BlockHandle,
        mc_is_key_block: bool,
    ) -> Result<()> {
        self.blob_storage
            .move_into_archive(handle, mc_is_key_block)
            .await
    }

    pub async fn wait_for_archive_commit(&self) -> Result<()> {
        self.blob_storage.wait_for_archive_commit().await
    }

    /// Returns a corresponding archive id for the specified masterchain seqno.
    pub fn get_archive_id(&self, mc_seqno: u32) -> ArchiveId {
        self.blob_storage.get_archive_id(mc_seqno)
    }

    pub fn get_archive_size(&self, id: u32) -> Result<Option<usize>> {
        self.blob_storage.get_archive_size(id)
    }

    pub async fn get_archive_compressed(&self, id: u32) -> Result<Option<Bytes>> {
        self.blob_storage.get_archive(id).await
    }

    /// Get a chunk of the archive at the specified offset.
    pub async fn get_archive_chunk(&self, id: u32, offset: u64) -> Result<Bytes> {
        self.blob_storage.get_archive_chunk(id, offset).await
    }

    pub fn subscribe_to_archive_ids(&self) -> broadcast::Receiver<u32> {
        self.blob_storage.subscribe_to_archive_ids()
    }

    pub fn remove_outdated_archives(&self, until_id: u32) -> Result<()> {
        self.blob_storage.remove_outdated_archives(until_id)
    }

    // === GC stuff ===

    #[tracing::instrument(skip(self, max_blocks_per_batch))]
    pub async fn remove_outdated_blocks(
        &self,
        mc_seqno: u32,
        max_blocks_per_batch: Option<usize>,
    ) -> Result<()> {
        if mc_seqno == 0 {
            return Ok(());
        }

        tracing::info!("started blocks GC for mc_block {mc_seqno}");

        let block = self
            .blob_storage
            .find_mc_block_data(mc_seqno)
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

        // Remove all expired entries
        let total_cached_handles_removed = self
            .block_handle_storage
            .gc_handles_cache(mc_seqno, &shard_heights);

        let cancelled = CancellationFlag::new();
        scopeguard::defer! {
            cancelled.cancel();
        }

        // Since remove_blocks needs blob_storage but we can't move self into the closure,
        // we'll call it directly without rayon_run for now
        let BlockGcStats {
            mc_blocks_removed,
            total_blocks_removed,
        } = blobs::remove_blocks(
            self.blob_storage.db().clone(),
            &self.blob_storage,
            max_blocks_per_batch,
            mc_seqno,
            shard_heights,
            Some(&cancelled),
        )?;

        tracing::info!(
            total_cached_handles_removed,
            mc_blocks_removed,
            total_blocks_removed,
            "finished blocks GC"
        );
        Ok(())
    }

    // === Internal ===

    #[cfg(test)]
    pub fn db(&self) -> &CoreDb {
        self.blob_storage.db()
    }

    #[cfg(test)]
    pub(crate) fn blob_storage(&self) -> &blobs::BlobStorage {
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
    #[error("Block data not found")]
    BlockDataNotFound,
    #[error("Block proof not found")]
    BlockProofNotFound,
    #[error("Queue diff not found")]
    QueueDiffNotFound,
    #[error("Block handle id mismatch")]
    BlockHandleIdMismatch,
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
        let (ctx, _tmp_dir) = StorageContext::new_temp().await?;
        let storage = CoreStorage::open(ctx, CoreStorageConfig::new_potato()).await?;

        let shard = ShardIdent::MASTERCHAIN;
        for seqno in 0..1000 {
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
                    let res = storage
                        .block_storage()
                        .store_block_data(&block, &block.archive_data, block_meta)
                        .await?;

                    Ok::<_, anyhow::Error>(res.handle)
                })
            };

            let store_proof_and_queue = || {
                let storage = storage.clone();
                let proof = proof.clone();
                let queue_diff = queue_diff.clone();
                JoinTask::new(async move {
                    if rand::random::<bool>() {
                        tokio::task::yield_now().await;
                    }

                    let res = storage
                        .block_storage()
                        .store_block_proof(&proof, MaybeExistingHandle::New(block_meta))
                        .await?;

                    if rand::random::<bool>() {
                        tokio::task::yield_now().await;
                    }

                    let res = storage
                        .block_storage()
                        .store_queue_diff(&queue_diff, res.handle.into())
                        .await?;

                    if rand::random::<bool>() {
                        tokio::task::yield_now().await;
                    }

                    Ok::<_, anyhow::Error>(res.handle)
                })
            };

            let (data_res, proof_and_queue_res) = async move {
                let data_fut = pin!(store_block_data());
                let proof_and_queue_fut = pin!(async {
                    tokio::select! {
                        left = store_proof_and_queue() => left,
                        right = store_proof_and_queue() => right,
                    }
                });

                let (data, other) = futures_util::future::join(data_fut, proof_and_queue_fut).await;

                Ok::<_, anyhow::Error>((data?, other?))
            }
            .await?;

            assert!(std::ptr::addr_eq(
                arc_swap::RefCnt::as_ptr(&data_res),
                arc_swap::RefCnt::as_ptr(&proof_and_queue_res)
            ));
            assert!(data_res.has_all_block_parts());
        }

        Ok(())
    }

    #[tokio::test]
    async fn blocks_gc() -> Result<()> {
        const GARBAGE: &[u8] = b"garbage";
        const ENTRY_TYPES: [ArchiveEntryType; 3] = [
            ArchiveEntryType::Block,
            ArchiveEntryType::Proof,
            ArchiveEntryType::QueueDiff,
        ];
        const CONNECTION_TYPES: [BlockConnection; 2] =
            [BlockConnection::Prev1, BlockConnection::Next1];

        let (ctx, _tmp_dir) = StorageContext::new_temp().await?;
        let storage = CoreStorage::open(ctx, CoreStorageConfig::new_potato()).await?;

        let blocks = storage.block_storage();
        let block_handles = storage.block_handle_storage();
        let block_connections = storage.block_connection_storage();

        let mut shard_block_ids = FastHashMap::<ShardIdent, Vec<BlockId>>::default();

        for shard in [ShardIdent::MASTERCHAIN, ShardIdent::BASECHAIN] {
            let entry = shard_block_ids.entry(shard).or_default();

            for seqno in 0..100 {
                let block_id = BlockId {
                    shard,
                    seqno,
                    root_hash: HashBytes(rand::random()),
                    file_hash: HashBytes(rand::random()),
                };
                entry.push(block_id);

                let (handle, _) = block_handles.create_or_load_handle(&block_id, NewBlockMeta {
                    is_key_block: shard.is_masterchain() && seqno == 0,
                    gen_utime: 0,
                    ref_by_mc_seqno: seqno,
                });
                let lock = handle.block_data_lock().write().await;

                for ty in ENTRY_TYPES {
                    blocks
                        .blob_storage
                        .add_data(&(block_id, ty).into(), GARBAGE, &lock)
                        .await?;
                }
                for direction in CONNECTION_TYPES {
                    block_connections.store_connection(&handle, direction, &block_id);
                }

                handle.meta().add_flags(BlockFlags::HAS_ALL_BLOCK_PARTS);
                block_handles.store_handle(&handle, false);
            }
        }

        // Remove some blocks
        let stats = blobs::remove_blocks(
            blocks.db().clone(),
            &blocks.blob_storage,
            None,
            70,
            [(ShardIdent::BASECHAIN, 50)].into(),
            None,
        )?;
        assert_eq!(stats, BlockGcStats {
            mc_blocks_removed: 69,
            total_blocks_removed: 69 + 49,
        });

        let removed_ranges = FastHashMap::from_iter([
            (ShardIdent::MASTERCHAIN, vec![1..=69]),
            (ShardIdent::BASECHAIN, vec![1..=49]),
        ]);
        for (shard, block_ids) in shard_block_ids {
            let removed_ranges = removed_ranges.get(&shard).unwrap();

            for block_id in block_ids {
                let must_be_removed = 'removed: {
                    for range in removed_ranges {
                        if range.contains(&block_id.seqno) {
                            break 'removed true;
                        }
                    }
                    false
                };

                let handle = block_handles.load_handle(&block_id);
                assert_eq!(handle.is_none(), must_be_removed);

                for ty in ENTRY_TYPES {
                    let key = PackageEntryKey::from((block_id, ty));
                    // Check if the entry exists in Cassadilia
                    let exists_in_cas = blocks.blob_storage().blocks().contains_key(&key);
                    assert_eq!(!exists_in_cas, must_be_removed);
                }

                for direction in CONNECTION_TYPES {
                    let connection = block_connections.load_connection(&block_id, direction);
                    assert_eq!(connection.is_none(), must_be_removed);
                }
            }
        }

        // Remove single block
        let stats = blobs::remove_blocks(
            blocks.db().clone(),
            &blocks.blob_storage,
            None,
            71,
            [(ShardIdent::BASECHAIN, 51)].into(),
            None,
        )?;
        assert_eq!(stats, BlockGcStats {
            mc_blocks_removed: 1,
            total_blocks_removed: 2,
        });

        // Remove no blocks
        let stats = blobs::remove_blocks(
            blocks.db().clone(),
            &blocks.blob_storage,
            None,
            71,
            [(ShardIdent::BASECHAIN, 51)].into(),
            None,
        )?;
        assert_eq!(stats, BlockGcStats {
            mc_blocks_removed: 0,
            total_blocks_removed: 0,
        });

        Ok(())
    }
}
