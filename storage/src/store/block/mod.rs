use std::collections::BTreeSet;
use std::fs::File;
use std::io::BufReader;
use std::num::NonZeroU32;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use bytes::{Buf, Bytes};
use bytesize::ByteSize;
use cassadilia::Bubs;
use everscale_types::boc::{Boc, BocRepr};
use everscale_types::cell::HashBytes;
use everscale_types::models::*;
use parking_lot::RwLock;
use tl_proto::TlWrite;
use tokio::sync::{broadcast, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tycho_block_util::archive::{
    ArchiveData, ArchiveEntryHeader, ArchiveEntryType, ARCHIVE_ENTRY_HEADER_LEN, ARCHIVE_PREFIX,
};
use tycho_block_util::block::{
    BlockProofStuff, BlockProofStuffAug, BlockStuff, BlockStuffAug, ShardHeights,
};
use tycho_block_util::queue::{QueueDiffStuff, QueueDiffStuffAug};
use tycho_util::compression::ZstdCompressStream;
use tycho_util::metrics::HistogramGuard;
use tycho_util::sync::{rayon_run, CancellationFlag};
use tycho_util::{FastHashSet, FastHasherState};
use weedb::{rocksdb, ColumnFamily, OwnedPinnableSlice};

pub use self::package_entry::{BlockDataEntryKey, PackageEntryKey, PartialBlockId};
use crate::db::*;
use crate::util::*;
use crate::{
    BlockConnectionStorage, BlockDataGuard, BlockFlags, BlockHandle, BlockHandleStorage,
    BlocksCacheConfig, HandleCreationStatus, NewBlockMeta, NodeStateStorage, StorageConfig,
};

mod package_entry;

const METRIC_LOAD_BLOCK_TOTAL: &str = "tycho_storage_load_block_total";
const METRIC_BLOCK_CACHE_HIT_TOTAL: &str = "tycho_storage_block_cache_hit_total";

pub struct BlockStorage {
    db: BaseDb,
    blob_dbs: BlobDbs,
    blocks_cache: BlocksCache,
    block_handle_storage: Arc<BlockHandleStorage>,
    block_connection_storage: Arc<BlockConnectionStorage>,

    node_state_storage: NodeStateStorage,

    in_progress_archive_ids: RwLock<BTreeSet<u32>>,
    block_subscriptions: SlotSubscriptions<BlockId, BlockStuff>,
    store_block_data: tokio::sync::RwLock<()>,
    prev_archive_commit: tokio::sync::Mutex<Option<CommitArchiveTask>>,
    archive_ids_tx: ArchiveIdsTx,
    archive_chunk_size: NonZeroU32,

    split_block_semaphore: Arc<Semaphore>,

    in_progress_archives: FileDb,
}

pub struct BlobDbs {
    pub archives: cassadilia::Bubs<u32>,
    pub package_entries: cassadilia::Bubs<String>,
}

impl BlobDbs {
    fn new(db_root: &Path) -> BlobDbs {
        struct U32Encoder;

        impl cassadilia::KeyEncoder<u32> for U32Encoder {
            fn encode(&self, key: &u32) -> Result<Vec<u8>> {
                let mut buf = Vec::with_capacity(4);
                buf.extend_from_slice(&key.to_be_bytes());
                Ok(buf)
            }

            fn decode(&self, data: &[u8]) -> Result<u32> {
                if data.len() != 4 {
                    return Err(anyhow::anyhow!("Invalid length for u32 key"));
                }
                let mut buf = [0u8; 4];
                buf.copy_from_slice(data);
                Ok(u32::from_be_bytes(buf))
            }
        }

        struct NOOPEncoder;

        impl cassadilia::KeyEncoder<String> for NOOPEncoder {
            fn encode(&self, key: &String) -> Result<Vec<u8>> {
                todo!()
            }

            fn decode(&self, data: &[u8]) -> Result<String> {
                todo!()
            }
        }

        let archives = cassadilia::Bubs::open(db_root.join("archives"), U32Encoder).unwrap();
        let package_entries =
            cassadilia::Bubs::open(db_root.join("package_entries"), NOOPEncoder).unwrap();

        BlobDbs {
            archives,
            package_entries,
        }
    }
}

impl BlockStorage {
    // === Init stuff ===

    // NOTE: This is intentionally a method, not a constant because
    // it might be useful to allow configure it during the first run.
    pub fn archive_chunk_size(&self) -> NonZeroU32 {
        self.archive_chunk_size
    }

    pub async fn new(
        db: BaseDb,
        config: &StorageConfig,
        file_db: &FileDb,
        block_handle_storage: Arc<BlockHandleStorage>,
        block_connection_storage: Arc<BlockConnectionStorage>,
        archive_chunk_size: ByteSize,
        node_state_storage: NodeStateStorage,
    ) -> Result<Self> {
        fn weigher(_key: &BlockId, value: &BlockStuff) -> u32 {
            const BLOCK_STUFF_OVERHEAD: u32 = 1024; // 1 KB

            size_of::<BlockId>() as u32
                + BLOCK_STUFF_OVERHEAD
                + value.data_size().try_into().unwrap_or(u32::MAX)
        }

        let blocks_cache = moka::sync::Cache::builder()
            .time_to_live(config.blocks_cache.ttl)
            .max_capacity(config.blocks_cache.size.as_u64())
            .weigher(weigher)
            .build_with_hasher(Default::default());

        let in_progress_archives = file_db.create_subdir("archive_ids")?;

        let (archive_ids_tx, _) = broadcast::channel(4);

        let archive_chunk_size =
            NonZeroU32::new(archive_chunk_size.as_u64().clamp(1, u32::MAX as _) as _).unwrap();

        let split_block_semaphore = Arc::new(Semaphore::new(config.split_block_tasks));
        let this = Self {
            db,
            blocks_cache,
            block_handle_storage,
            block_connection_storage,
            archive_ids_tx,
            archive_chunk_size,
            split_block_semaphore,
            block_subscriptions: Default::default(),
            store_block_data: Default::default(),
            prev_archive_commit: Default::default(),
            blob_dbs: BlobDbs::new(&config.root_dir.join("blob")),
            in_progress_archive_ids: Default::default(),

            in_progress_archives,

            node_state_storage,
        };

        this.resume_archives_commit().await?;

        Ok(this)
    }

    async fn resume_archives_commit(&self) -> Result<()> {
        let dir_path = self.in_progress_archives.path();

        let unfinished_results: Vec<Result<u32>> = std::fs::read_dir(dir_path)
            .with_context(|| format!("Failed to read in-progress directory: {:?}", dir_path))?
            .map(|entry_res| {
                entry_res
                    .map_err(anyhow::Error::from)
                    .and_then(|entry| {
                        let file_name = entry.file_name();
                        let path = entry.path();
                        let file_name_str = file_name
                            .to_str()
                            .context(format!("Filename at path {:?} is not valid UTF-8", path))?;
                        u32::from_str(file_name_str)
                            .map_err(anyhow::Error::from)
                            .with_context(|| {
                                format!(
                                    "Invalid archive ID format in filename '{}' at path {:?}",
                                    file_name_str, path
                                )
                            })
                    })
                    .with_context(|| format!("Failed processing entry in directory {:?}", dir_path))
            })
            .collect();

        let unfinished_ids = unfinished_results
            .into_iter()
            .collect::<Result<Vec<u32>>>()
            .with_context(|| {
                format!(
                    "Error occurred while processing uncommited archives in directory {:?}",
                    dir_path
                )
            })?;

        let mut tasks = Vec::new();
        for archive_id in unfinished_ids {
            tracing::info!("found unfinished archive commit {archive_id}");
            tasks.push((archive_id, self.spawn_commit_archive(archive_id)));
        }

        for (archive_id, mut task) in tasks {
            task.finish()
                .await
                .with_context(|| format!("Failed to commit unfinished archive {}", archive_id))?;
        }

        Ok(())
    }

    pub fn block_data_chunk_size(&self) -> NonZeroU32 {
        NonZeroU32::new(BLOCK_DATA_CHUNK_SIZE).unwrap()
    }

    pub async fn finish_block_data(&self) -> Result<()> {
        let started_at = Instant::now();

        tracing::info!("started finishing compressed block data");

        let mut iter = self.db.block_data_entries.raw_iterator();
        iter.seek_to_first();

        let mut blocks_to_finish = Vec::new();

        loop {
            let Some(key) = iter.key() else {
                if let Err(e) = iter.status() {
                    tracing::error!("failed to iterate through compressed block data: {e:?}");
                }
                break;
            };

            let key = BlockDataEntryKey::from_slice(key);

            const _: () = const {
                // Rely on the specific order of these constants
                assert!(BLOCK_DATA_STARTED_MAGIC < BLOCK_DATA_SIZE_MAGIC);
            };

            // Chunk keys are sorted by offset.
            match key.chunk_index {
                // "Started" magic comes first, and indicates that the block exists.
                BLOCK_DATA_STARTED_MAGIC => {
                    blocks_to_finish.push(key.block_id);
                }
                // "Size" magic comes last, and indicates that the block data is finished.
                BLOCK_DATA_SIZE_MAGIC => {
                    // Last block id is already finished
                    let last = blocks_to_finish.pop();
                    anyhow::ensure!(last == Some(key.block_id), "invalid block data SIZE entry");
                }
                _ => {}
            }

            iter.next();
        }

        drop(iter);

        for block_id in blocks_to_finish {
            tracing::info!(?block_id, "found unfinished block");

            let key = PackageEntryKey {
                block_id,
                ty: ArchiveEntryType::Block,
            };

            let data = match self.db.package_entries.get(key.to_vec())? {
                Some(data) => data,
                None => return Err(BlockStorageError::BlockDataNotFound.into()),
            };

            let permit = self.split_block_semaphore.clone().acquire_owned().await?;
            self.spawn_split_block_data(&block_id, &data, permit)
                .await??;
        }

        tracing::info!(
            elapsed = %humantime::format_duration(started_at.elapsed()),
            "finished handling unfinished blocks"
        );

        Ok(())
    }

    // === Subscription stuff ===

    pub async fn wait_for_block(&self, block_id: &BlockId) -> Result<BlockStuffAug> {
        let block_handle_storage = &self.block_handle_storage;

        // Take an exclusive lock to prevent any block data from being stored
        let guard = self.store_block_data.write().await;

        // Try to load the block data
        if let Some(handle) = block_handle_storage.load_handle(block_id) {
            if handle.has_data() {
                drop(guard);
                let block = self.load_block_data(&handle).await?;
                return Ok(BlockStuffAug::loaded(block));
            }
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

            let _lock = handle.block_data_lock().write().await;
            if !handle.has_data() {
                self.add_block_data_and_split(&archive_id, data).await?;
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

        let FullBlockDataGuard { _lock, data } = self
            .get_data_ref(handle, &PackageEntryKey::block(handle.id()))
            .await?;

        if data.len() > BIG_DATA_THRESHOLD {
            BlockStuff::deserialize(handle.id(), data.as_ref())
        } else {
            let handle = handle.clone();

            // SAFETY: `data` was created by the `self.db` RocksDB instance.
            let owned_data =
                unsafe { weedb::OwnedPinnableSlice::new(self.db.rocksdb().clone(), data) };
            rayon_run(move || BlockStuff::deserialize(handle.id(), owned_data.as_ref())).await
        }
    }

    pub async fn load_block_data_raw(&self, handle: &BlockHandle) -> Result<OwnedPinnableSlice> {
        if !handle.has_data() {
            return Err(BlockStorageError::BlockDataNotFound.into());
        }
        self.get_data(handle, &PackageEntryKey::block(handle.id()))
            .await
    }

    pub async fn list_blocks(
        &self,
        continuation: Option<BlockIdShort>,
    ) -> Result<(Vec<BlockId>, Option<BlockIdShort>)> {
        const LIMIT: usize = 1000; // Max blocks per response
        const MAX_BYTES: usize = 1 << 20; // 1 MB processed per response

        let continuation = continuation.map(|block_id| {
            PackageEntryKey::block(&BlockId {
                shard: block_id.shard,
                seqno: block_id.seqno,
                root_hash: HashBytes::ZERO,
                file_hash: HashBytes::ZERO,
            })
            .to_vec()
        });

        let mut iter = {
            let mut readopts = rocksdb::ReadOptions::default();
            tables::PackageEntries::read_options(&mut readopts);
            if let Some(key) = &continuation {
                readopts.set_iterate_lower_bound(key.as_slice());
            }
            self.db
                .rocksdb()
                .raw_iterator_cf_opt(&self.db.package_entries.cf(), readopts)
        };

        // NOTE: Despite setting the lower bound we must still seek to the exact key.
        match continuation {
            None => iter.seek_to_first(),
            Some(key) => iter.seek(key),
        }

        let mut bytes = 0;
        let mut blocks = Vec::new();

        let continuation = loop {
            let (key, value) = match iter.item() {
                Some(item) => item,
                None => {
                    iter.status()?;
                    break None;
                }
            };

            let id = PackageEntryKey::from_slice(key);
            if id.ty != ArchiveEntryType::Block {
                // Ignore non-block entries
                iter.next();
                continue;
            }

            if blocks.len() >= LIMIT || bytes >= MAX_BYTES {
                break Some(id.block_id.as_short_id());
            }

            let file_hash = Boc::file_hash_blake(value);
            let block_id = id.block_id.make_full(file_hash);

            bytes += value.len();
            blocks.push(block_id);

            iter.next();
        };

        Ok((blocks, continuation))
    }

    pub fn list_archive_ids(&self) -> Vec<u32> {
        let mut keys = self.blob_dbs.archives.known_keys();
        keys.sort_unstable();
        keys
    }

    pub async fn load_block_data_raw_ref<'a>(
        &'a self,
        handle: &'a BlockHandle,
    ) -> Result<impl AsRef<[u8]> + 'a> {
        if !handle.has_data() {
            return Err(BlockStorageError::BlockDataNotFound.into());
        }
        self.get_data_ref(handle, &PackageEntryKey::block(handle.id()))
            .await
    }

    pub fn find_mc_block_data(&self, mc_seqno: u32) -> Result<Option<Block>> {
        let package_entries = &self.db.package_entries;

        let bound = BlockId {
            shard: ShardIdent::MASTERCHAIN,
            seqno: mc_seqno,
            root_hash: HashBytes::ZERO,
            file_hash: HashBytes::ZERO,
        };

        let mut bound = PackageEntryKey::block(&bound);

        let mut readopts = package_entries.new_read_config();
        readopts.set_iterate_lower_bound(bound.to_vec().into_vec());
        bound.block_id.seqno += 1;
        readopts.set_iterate_upper_bound(bound.to_vec().into_vec());

        let mut iter = self
            .db
            .rocksdb()
            .raw_iterator_cf_opt(&package_entries.cf(), readopts);

        iter.seek_to_first();
        loop {
            let Some((key, value)) = iter.item() else {
                iter.status()?;
                return Ok(None);
            };

            let Some(ArchiveEntryType::Block) = extract_entry_type(key) else {
                continue;
            };

            return Ok(Some(BocRepr::decode::<Block, _>(value)?));
        }
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

            let _lock = handle.proof_data_lock().write().await;
            if !handle.has_proof() {
                self.add_data(&archive_id, data)?;
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
        let raw_proof = self.load_block_proof_raw_ref(handle).await?;
        BlockProofStuff::deserialize(handle.id(), raw_proof.as_ref())
    }

    pub async fn load_block_proof_raw(&self, handle: &BlockHandle) -> Result<OwnedPinnableSlice> {
        if !handle.has_proof() {
            return Err(BlockStorageError::BlockProofNotFound.into());
        }

        self.get_data(handle, &PackageEntryKey::proof(handle.id()))
            .await
    }

    pub async fn load_block_proof_raw_ref<'a>(
        &'a self,
        handle: &'a BlockHandle,
    ) -> Result<impl AsRef<[u8]> + 'a> {
        if !handle.has_proof() {
            return Err(BlockStorageError::BlockProofNotFound.into());
        }

        self.get_data_ref(handle, &PackageEntryKey::proof(handle.id()))
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

            let _lock = handle.queue_diff_data_lock().write().await;
            if !handle.has_queue_diff() {
                self.add_data(&archive_id, data)?;
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
        let raw_diff = self.load_queue_diff_raw_ref(handle).await?;
        QueueDiffStuff::deserialize(handle.id(), raw_diff.as_ref())
    }

    pub async fn load_queue_diff_raw(&self, handle: &BlockHandle) -> Result<OwnedPinnableSlice> {
        if !handle.has_queue_diff() {
            return Err(BlockStorageError::QueueDiffNotFound.into());
        }

        self.get_data(handle, &PackageEntryKey::queue_diff(handle.id()))
            .await
    }

    pub async fn load_queue_diff_raw_ref<'a>(
        &'a self,
        handle: &'a BlockHandle,
    ) -> Result<impl AsRef<[u8]> + 'a> {
        if !handle.has_queue_diff() {
            return Err(BlockStorageError::QueueDiffNotFound.into());
        }
        self.get_data_ref(handle, &PackageEntryKey::queue_diff(handle.id()))
            .await
    }

    // === Archive stuff ===

    /// Loads data and proof for the block and appends them to the corresponding archive.
    pub async fn move_into_archive(
        &self,
        handle: &BlockHandle,
        mc_is_key_block: bool,
    ) -> Result<()> {
        let _histogram = HistogramGuard::begin("tycho_storage_move_into_archive_time");

        // Prepare data
        let block_id_bytes = handle.id().to_vec();

        // Prepare cf
        let archive_block_ids_cf = self.db.archive_block_ids.cf();

        // Prepare archive
        let archive_id = self.prepare_archive_id(
            handle.ref_by_mc_seqno(),
            mc_is_key_block || handle.is_key_block(),
        );
        let archive_id_bytes = archive_id.id.to_be_bytes();

        // 0. Create transaction
        let mut batch = rocksdb::WriteBatch::default();

        // 1. Append archive block id
        batch.merge_cf(&archive_block_ids_cf, archive_id_bytes, &block_id_bytes);

        self.db.rocksdb().write(batch)?;

        tracing::debug!(block_id = %handle.id(), ?archive_id,  "saved block id into archive");
        // Block will be removed after blocks gc

        if let Some(to_commit) = archive_id.to_commit {
            let mut should_start_commit = true;
            // Commit previous archive
            let mut prev_archive_commit = self.prev_archive_commit.lock().await;

            // NOTE: Wait on reference to make sure that the task is cancel safe
            if let Some(task) = &mut *prev_archive_commit {
                // Wait commit archive
                task.finish().await?;

                // we can have master id 100 from master and any shard block, so it will trigger
                // second commit which is not needed
                if task.archive_id == to_commit {
                    should_start_commit = false;
                }

                // Notify archive subscribers
                self.archive_ids_tx.send(task.archive_id).ok();
            }

            if should_start_commit {
                *prev_archive_commit = Some(self.spawn_commit_archive(to_commit));
            }
        }

        // Done
        Ok(())
    }

    pub async fn wait_for_archive_commit(&self) -> Result<()> {
        let mut prev_archive_commit = self.prev_archive_commit.lock().await;
        if let Some(task) = &mut *prev_archive_commit {
            task.finish().await?;
            *prev_archive_commit = None;
        }
        Ok(())
    }

    /// Returns a corresponding archive id for the specified masterchain seqno.
    pub fn get_archive_meta(&self, mc_seqno: u32) -> ArchiveMeta {
        let Some(last_mc_block) = self.node_state_storage.load_last_mc_block_id() else {
            return ArchiveMeta::TooNew; // Node has no known masterchain blocks yet
        };

        // If mc_seqno is too far ahead of the latest known block, it's too new.
        // Example: last_mc_block.seqno = 500, ARCHIVE_PACKAGE_SIZE = 100.
        // If mc_seqno = 300, 500 - 300 = 200. 200 - 100 = 100. is_some(). OK.
        // If mc_seqno = 450, 500 - 450 = 50. 50 - 100 = None. TooNew.
        // If mc_seqno = 600, 500 - 600 = None. TooNew.
        if last_mc_block
            .seqno
            .checked_sub(mc_seqno)
            .and_then(|diff| diff.checked_sub(ARCHIVE_PACKAGE_SIZE))
            .is_none()
        {
            return ArchiveMeta::TooNew;
        }

        // Assuming self.blob_dbs.archives.known_keys() now returns a BTreeSet<u32>
        let archive_ids: BTreeSet<u32> = self.blob_dbs.archives.known_keys();

        if archive_ids.is_empty() {
            return ArchiveMeta::NotFound; // No archives at all
        }

        // Find the largest archive ID that is less than or equal to mc_seqno.
        // This is our candidate archive, as mc_seqno could potentially fall into its range.
        // BTreeSet::range(..=value) gives an iterator to elements <= value.
        // .next_back() gives the largest of these.
        let Some(&candidate_id) = archive_ids.range(..=mc_seqno).next_back() else {
            // All archive IDs in the set are > mc_seqno.
            // This means mc_seqno is smaller than the ID of the very first archive.
            // Example: mc_seqno = 5, archive_ids = {10, 20} -> range(..=5) is empty.
            return ArchiveMeta::NotFound;
        };

        // Now `candidate_id` is the largest archive ID <= `mc_seqno`.
        // Example: mc_seqno = 15, archive_ids = {10, 20} -> candidate_id = 10.
        // Example: mc_seqno = 10, archive_ids = {10, 20} -> candidate_id = 10.
        // Example: mc_seqno = 25, archive_ids = {10, 20} -> candidate_id = 20.

        // Check if mc_seqno is within the range covered by this candidate archive package.
        // The range is [candidate_id, candidate_id + ARCHIVE_PACKAGE_SIZE).
        let archive_end_exclusive = candidate_id.saturating_add(ARCHIVE_PACKAGE_SIZE);

        if mc_seqno < archive_end_exclusive {
            // mc_seqno falls within the candidate package's range.
            match self.blob_dbs.archives.size(&candidate_id) {
                Ok(Some(len)) => ArchiveMeta::Found {
                    mc_block_id: candidate_id,
                    len,
                },
                Ok(None) => {
                    // Archive ID existed in keys, but size() returned None (deleted by GC?)
                    tracing::warn!(
                        archive_id = candidate_id,
                        target_mc_seqno = mc_seqno,
                        "archive was present in known_keys but size not found (deleted by gc or index corrupted?)"
                    );
                    ArchiveMeta::NotFound // Treat as not found if data is gone
                }
                Err(e) => {
                    tracing::error!(
                        archive_id = candidate_id,
                        target_mc_seqno = mc_seqno,
                        error = %e,
                        "Failed to get archive size"
                    );
                    ArchiveMeta::NotFound // Or introduce an ArchiveMeta::Error variant
                }
            }
        } else {
            // mc_seqno is >= candidate_id + ARCHIVE_PACKAGE_SIZE.
            // It falls *after* the range covered by the candidate package.
            // Now we need to distinguish: is it in a gap, or truly too new (newer than the latest archive)?

            // Check if the candidate we found was the *last* known archive ID in the set.
            // BTreeSet::last() returns an Option<&T> to the largest element.
            // We know archive_ids is not empty here, so .last() will be Some.
            if Some(&candidate_id) == archive_ids.last() {
                // Yes, `candidate_id` is the ID of the last archive package.
                // Since `mc_seqno` is outside its range (and it's the last archive),
                // it's newer than any known archive package.
                // Example: mc_seqno = 1025, archive_ids = {10, 20}, ARCHIVE_PACKAGE_SIZE = 1000.
                // candidate_id = 20. archive_end_exclusive = 1020. 1025 < 1020 is false.
                // archive_ids.last() = Some(20). Some(&20) == Some(&20). -> TooNew.
                ArchiveMeta::TooNew
            } else {
                // No, `candidate_id` is not the last archive ID in the set.
                // This means there's another archive ID after `candidate_id`
                // (e.g., *archive_ids.range((candidate_id+1)..).next() would yield a value).
                // `mc_seqno` falls into a gap between the package starting
                // at `candidate_id` and the next package.
                // Example: mc_seqno=1015, archive_ids={10, 1020}, ARCHIVE_PACKAGE_SIZE=1000.
                // candidate_id = 10. archive_end_exclusive = 1010. 1015 < 1010 is false.
                // archive_ids.last() = Some(1020). Some(&10) != Some(&1020). -> NotFound (gap).
                ArchiveMeta::NotFound
            }
        }
    }

    /// Loads an archive chunk.
    pub async fn get_archive_chunk(&self, id: u32, offset: u64) -> Result<Bytes> {
        Ok(self
            .blob_dbs
            .archives
            .get_range(&id, offset, offset + self.archive_chunk_size.get() as u64)?
            .ok_or(BlockStorageError::ArchiveNotFound)?)
    }

    pub fn get_block_data_size(&self, block_id: &BlockId) -> Result<Option<u32>> {
        let key = BlockDataEntryKey {
            block_id: block_id.into(),
            chunk_index: BLOCK_DATA_SIZE_MAGIC,
        };
        let size = self
            .db
            .block_data_entries
            .get(key.to_vec())?
            .map(|slice| u32::from_le_bytes(slice.as_ref().try_into().unwrap()));

        Ok(size)
    }

    pub fn get_block_data_chunk(
        &self,
        block_id: &BlockId,
        offset: u32,
    ) -> Result<Option<OwnedPinnableSlice>> {
        let chunk_size = self.block_data_chunk_size().get();
        if offset % chunk_size != 0 {
            return Err(BlockStorageError::InvalidOffset.into());
        }

        let key = BlockDataEntryKey {
            block_id: block_id.into(),
            chunk_index: offset / chunk_size,
        };

        Ok(self.db.block_data_entries.get(key.to_vec())?.map(|value| {
            // SAFETY: A value was received from the same RocksDB instance.
            unsafe { OwnedPinnableSlice::new(self.db.rocksdb().clone(), value) }
        }))
    }

    pub fn subscribe_to_archive_ids(&self) -> broadcast::Receiver<u32> {
        self.archive_ids_tx.subscribe()
    }

    pub fn archive_chunks_iterator(&self, archive_id: u32) -> Result<BufReader<File>> {
        self.blob_dbs.archives.raw_bufreader(&archive_id)
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
                .collect::<Result<_, everscale_types::error::Error>>()?
        };

        // Remove all expired entries
        let total_cached_handles_removed = self
            .block_handle_storage
            .gc_handles_cache(mc_seqno, &shard_heights);

        let cancelled = CancellationFlag::new();
        scopeguard::defer! {
            cancelled.cancel();
        }

        let span = tracing::Span::current();
        let cancelled = cancelled.clone();
        let db = self.db.clone();

        let BlockGcStats {
            mc_blocks_removed,
            total_blocks_removed,
        } = rayon_run(move || {
            let _span = span.enter();

            let guard = scopeguard::guard((), |_| {
                tracing::warn!("cancelled");
            });

            let stats = remove_blocks(
                db,
                max_blocks_per_batch,
                mc_seqno,
                shard_heights,
                Some(&cancelled),
            )?;

            scopeguard::ScopeGuard::into_inner(guard);
            Ok::<_, anyhow::Error>(stats)
        })
        .await?;

        tracing::info!(
            total_cached_handles_removed,
            mc_blocks_removed,
            total_blocks_removed,
            "finished blocks GC"
        );
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub fn remove_outdated_archives(&self, until_id: u32) -> Result<()> {
        tracing::trace!("started archives GC");

        let mut archive_ids = self.blob_dbs.archives.known_keys();
        archive_ids.sort_unstable();

        // Find the index up to which we need to remove IDs (exclusive of until_id)
        let partition_idx = archive_ids.partition_point(|&id| id < until_id);

        // Collect IDs to remove
        let removed_ids: Vec<u32> = archive_ids.drain(..partition_idx).collect();

        if removed_ids.is_empty() {
            tracing::info!("nothing to remove, until_id: {}", until_id);
            return Ok(());
        }

        let first = *removed_ids.first().unwrap(); // Safe due to is_empty check
        let last = *removed_ids.last().unwrap(); // Safe due to is_empty check
        let len = removed_ids.len();

        for id_to_remove in &removed_ids {
            self.blob_dbs.archives.remove(id_to_remove)?;
        }

        tracing::info!(
            archive_count = len,
            first,
            last,
            until_id,
            "finished archives GC"
        );
        Ok(())
    }

    // === Internal ===

    fn add_data(&self, id: &PackageEntryKey, data: &[u8]) -> Result<(), rocksdb::Error> {
        self.db.package_entries.insert(id.to_vec(), data)
    }

    async fn add_block_data_and_split(&self, id: &PackageEntryKey, data: &[u8]) -> Result<()> {
        let mut batch = rocksdb::WriteBatch::default();

        batch.put_cf(&self.db.package_entries.cf(), id.to_vec(), data);

        // Store info that new block was started
        let key = BlockDataEntryKey {
            block_id: id.block_id,
            chunk_index: BLOCK_DATA_STARTED_MAGIC,
        };
        batch.put_cf(&self.db.block_data_entries.cf(), key.to_vec(), []);

        self.db.rocksdb().write(batch)?;

        // Start splitting block data
        let permit = self.split_block_semaphore.clone().acquire_owned().await?;
        let _handle = self.spawn_split_block_data(&id.block_id, data, permit);

        Ok(())
    }

    async fn get_data(
        &self,
        handle: &BlockHandle,
        id: &PackageEntryKey,
    ) -> Result<OwnedPinnableSlice> {
        let _lock = match id.ty {
            ArchiveEntryType::Block => handle.block_data_lock(),
            ArchiveEntryType::Proof => handle.proof_data_lock(),
            ArchiveEntryType::QueueDiff => handle.queue_diff_data_lock(),
        }
        .read()
        .await;

        match self.db.package_entries.get(id.to_vec())? {
            // SAFETY: A value was received from the same RocksDB instance.
            Some(value) => Ok(unsafe { OwnedPinnableSlice::new(self.db.rocksdb().clone(), value) }),
            None => Err(BlockStorageError::PackageEntryNotFound.into()),
        }
    }

    async fn get_data_ref<'a, 'b: 'a>(
        &'a self,
        handle: &'b BlockHandle,
        id: &PackageEntryKey,
    ) -> Result<FullBlockDataGuard<'a>> {
        let lock = match id.ty {
            ArchiveEntryType::Block => handle.block_data_lock(),
            ArchiveEntryType::Proof => handle.proof_data_lock(),
            ArchiveEntryType::QueueDiff => handle.queue_diff_data_lock(),
        }
        .read()
        .await;

        match self.db.package_entries.get(id.to_vec())? {
            Some(data) => Ok(FullBlockDataGuard { _lock: lock, data }),
            None => Err(BlockStorageError::PackageEntryNotFound.into()),
        }
    }

    fn prepare_archive_id(&self, mc_seqno: u32, force_split_archive: bool) -> PreparedArchiveId {
        let mut archive_ids = self.in_progress_archive_ids.write();

        // Get the closest archive id
        let prev_id = archive_ids.range(..=mc_seqno).next_back().cloned();

        let mut archive_id = PreparedArchiveId {
            id: prev_id.unwrap_or_default(),
            to_commit: None,
        };

        let is_first_archive = prev_id.is_none();
        if is_first_archive
            || mc_seqno.saturating_sub(archive_id.id) >= ARCHIVE_PACKAGE_SIZE
            || force_split_archive
        {
            let is_new = archive_ids.insert(mc_seqno);
            archive_id = PreparedArchiveId {
                id: mc_seqno,
                to_commit: if is_new { prev_id } else { None },
            };
        }

        // NOTE: subtraction is intentional to panic if archive_id > mc_seqno
        debug_assert!(mc_seqno - archive_id.id <= ARCHIVE_PACKAGE_SIZE);

        archive_id
    }

    #[tracing::instrument(skip(self))]
    fn spawn_commit_archive(&self, archive_id: u32) -> CommitArchiveTask {
        let blob_db = self.blob_dbs.archives.clone();
        let db = self.db.clone();
        let block_handle_storage = self.block_handle_storage.clone();
        let chunk_size = self.archive_chunk_size().get() as u64;

        let file_db = self.in_progress_archives.clone();

        let span = tracing::Span::current();
        let cancelled = CancellationFlag::new();

        tracing::info!("started committing archive");

        let handle = tokio::task::spawn_blocking({
            let cancelled = cancelled.clone();

            move || {
                let _span = span.enter();

                let histogram = HistogramGuard::begin("tycho_storage_commit_archive_time");

                tracing::info!("started");

                let archive_commited = archive_id.to_string();
                let file = file_db
                    .file(&archive_commited)
                    .create(true)
                    .write(true)
                    .open()
                    .expect("failed to open an archive state file");

                file.sync_all().expect("failed to sync archive state file");

                let guard = scopeguard::guard((), |_| {
                    tracing::warn!("cancelled");
                });

                let raw_block_ids = db
                    .archive_block_ids
                    .get(archive_id.to_be_bytes())?
                    .ok_or(BlockStorageError::ArchiveNotFound)?;
                assert_eq!(raw_block_ids.len() % BlockId::SIZE_HINT, 0);

                let mut writer = ArchiveWriter::new(&blob_db, &db, archive_id, chunk_size)?;
                let mut header_buffer = Vec::with_capacity(ARCHIVE_ENTRY_HEADER_LEN);

                // Write archive prefix
                writer.write(&ARCHIVE_PREFIX)?;

                // Write all entries. We group them by type to achieve better compression.
                let mut unique_ids = FastHashSet::default();
                for ty in [
                    ArchiveEntryType::Block,
                    ArchiveEntryType::Proof,
                    ArchiveEntryType::QueueDiff,
                ] {
                    for raw_block_id in raw_block_ids.chunks_exact(BlockId::SIZE_HINT) {
                        anyhow::ensure!(!cancelled.check(), "task aborted");

                        let block_id = BlockId::from_slice(raw_block_id);
                        if !unique_ids.insert(block_id) {
                            tracing::warn!(%block_id, "skipped duplicate block id");
                            continue;
                        }

                        // Check handle flags (only for the first type).
                        if ty == ArchiveEntryType::Block {
                            let handle = block_handle_storage
                                .load_handle(&block_id)
                                .ok_or(BlockStorageError::BlockHandleNotFound)?;

                            let flags = handle.meta().flags();
                            anyhow::ensure!(
                                flags.contains(BlockFlags::HAS_ALL_BLOCK_PARTS),
                                "block does not have all parts: {block_id}, \
                                has_data={}, has_proof={}, queue_diff={}",
                                flags.contains(BlockFlags::HAS_DATA),
                                flags.contains(BlockFlags::HAS_PROOF),
                                flags.contains(BlockFlags::HAS_QUEUE_DIFF)
                            );
                        }

                        let key = PackageEntryKey::from((block_id, ty));
                        let Some(data) = db.package_entries.get(key.to_vec()).unwrap() else {
                            return Err(BlockStorageError::BlockDataNotFound.into());
                        };

                        // Serialize entry header
                        header_buffer.clear();
                        ArchiveEntryHeader {
                            block_id,
                            ty,
                            data_len: data.len() as u32,
                        }
                        .write_to(&mut header_buffer);

                        // Write entry header and data
                        writer.write(&header_buffer)?;
                        writer.write(data.as_ref())?;
                    }

                    unique_ids.clear();
                }

                // Drop ids entry just in case (before removing it)
                drop(raw_block_ids);

                // Finalize the archive
                writer.finalize()?;

                // remove archive lock
                file_db
                    .remove_file(archive_commited)
                    .expect("failed to remove archive lock");

                // Done
                scopeguard::ScopeGuard::into_inner(guard);
                tracing::info!(
                    elapsed = %humantime::format_duration(histogram.finish()),
                    "finished"
                );

                Ok(())
            }
        });

        CommitArchiveTask {
            archive_id,
            cancelled,
            handle: Some(handle),
        }
    }

    #[tracing::instrument(skip(self, data))]
    fn spawn_split_block_data(
        &self,
        block_id: &PartialBlockId,
        data: &[u8],
        permit: OwnedSemaphorePermit,
    ) -> JoinHandle<Result<()>> {
        let db = self.db.clone();
        let chunk_size = self.block_data_chunk_size().get() as usize;

        let span = tracing::Span::current();
        let handle = tokio::task::spawn_blocking({
            let block_id = *block_id;
            let data = data.to_vec();

            move || {
                let _span = span.enter();

                let _histogram = HistogramGuard::begin("tycho_storage_split_block_data_time");

                let mut compressed = Vec::new();
                tycho_util::compression::zstd_compress(&data, &mut compressed, 3);

                let chunks = compressed.chunks(chunk_size);
                for (index, chunk) in chunks.enumerate() {
                    let key = BlockDataEntryKey {
                        block_id,
                        chunk_index: index as u32,
                    };

                    db.block_data_entries.insert(key.to_vec(), chunk)?;
                }

                let key = BlockDataEntryKey {
                    block_id,
                    chunk_index: BLOCK_DATA_SIZE_MAGIC,
                };
                db.block_data_entries
                    .insert(key.to_vec(), (compressed.len() as u32).to_le_bytes())?;

                drop(permit);

                Ok(())
            }
        });

        handle
    }
}

struct CommitArchiveTask {
    archive_id: u32,
    cancelled: CancellationFlag,
    handle: Option<JoinHandle<Result<()>>>,
}

impl CommitArchiveTask {
    async fn finish(&mut self) -> Result<()> {
        // NOTE: Await on reference to make sure that the task is cancel safe
        if let Some(handle) = &mut self.handle {
            if let Err(e) = handle
                .await
                .map_err(|e| {
                    if e.is_panic() {
                        std::panic::resume_unwind(e.into_panic());
                    }
                    anyhow::Error::from(e)
                })
                .and_then(std::convert::identity)
            {
                tracing::error!(
                    archive_id = self.archive_id,
                    "failed to commit archive: {e:?}"
                );
            }

            self.handle = None;
        }

        Ok(())
    }
}

impl Drop for CommitArchiveTask {
    fn drop(&mut self) {
        self.cancelled.cancel();
        if let Some(handle) = &self.handle {
            handle.abort();
        }
    }
}

struct ArchiveWriter<'a> {
    transaction: cassadilia::Transaction<'a, u32>,
    rocksdb: &'a BaseDb,
    chunks_buffer: Vec<u8>,
    archive_id: u32,
    zstd_compressor: ZstdCompressStream<'a>,
}

impl<'a> ArchiveWriter<'a> {
    fn new(
        db: &'a Bubs<u32>,
        rocksdb: &'a BaseDb,
        archive_id: u32,
        chunk_len: u64,
    ) -> Result<Self> {
        let chunk_len = chunk_len as usize;

        let mut zstd_compressor = ZstdCompressStream::new(9, chunk_len)?;

        let workers = (std::thread::available_parallelism()?.get() / 4) as u8;
        zstd_compressor.multithreaded(workers)?;

        let transaction = db.put(archive_id)?;

        Ok(Self {
            transaction,
            rocksdb,
            chunks_buffer: Vec::with_capacity(chunk_len),
            archive_id,
            zstd_compressor,
        })
    }

    fn write(&mut self, data: &[u8]) -> Result<()> {
        self.zstd_compressor.write(data, &mut self.chunks_buffer)?;
        self.transaction.write(&self.chunks_buffer)?;
        self.chunks_buffer.clear();
        Ok(())
    }

    fn finalize(mut self) -> Result<()> {
        self.zstd_compressor.finish(&mut self.chunks_buffer)?;
        self.transaction.write(&self.chunks_buffer)?;

        self.transaction.finish()?;

        // Remove related block ids
        self.rocksdb
            .archive_block_ids
            .remove(self.archive_id.to_be_bytes())?;
        Ok(())
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

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ArchiveMeta {
    Found { mc_block_id: u32, len: u64 },
    TooNew,
    NotFound,
}

impl ArchiveMeta {
    pub fn size(&self) -> Option<u64> {
        match self {
            ArchiveMeta::Found { len, .. } => Some(*len),
            ArchiveMeta::TooNew | ArchiveMeta::NotFound => None,
        }
    }
}

fn remove_blocks(
    db: BaseDb,
    max_blocks_per_batch: Option<usize>,
    mc_seqno: u32,
    shard_heights: ShardHeights,
    cancelled: Option<&CancellationFlag>,
) -> Result<BlockGcStats> {
    let mut stats = BlockGcStats::default();

    let raw = db.rocksdb().as_ref();
    let full_block_ids_cf = db.full_block_ids.cf();
    let block_connections_cf = db.block_connections.cf();
    let package_entries_cf = db.package_entries.cf();
    let block_data_entries_cf = db.block_data_entries.cf();
    let block_handles_cf = db.block_handles.cf();

    // Create batch
    let mut batch = rocksdb::WriteBatch::default();
    let mut batch_len = 0;

    // Iterate all entries and find expired items
    let mut blocks_iter =
        raw.raw_iterator_cf_opt(&full_block_ids_cf, db.full_block_ids.new_read_config());
    blocks_iter.seek_to_first();

    let block_handles_readopts = db.block_handles.new_read_config();
    let is_persistent = |root_hash: &[u8; 32]| -> Result<bool> {
        const FLAGS: u64 =
            ((BlockFlags::IS_KEY_BLOCK.bits() | BlockFlags::IS_PERSISTENT.bits()) as u64) << 32;

        let Some(value) =
            raw.get_pinned_cf_opt(&block_handles_cf, root_hash, &block_handles_readopts)?
        else {
            return Ok(false);
        };
        Ok(value.as_ref().get_u64_le() & FLAGS != 0)
    };

    let mut key_buffer = [0u8; tables::PackageEntries::KEY_LEN];
    let mut delete_range =
        |batch: &mut rocksdb::WriteBatch, from: &BlockIdShort, to: &BlockIdShort| {
            debug_assert_eq!(from.shard, to.shard);
            debug_assert!(from.seqno <= to.seqno);

            let range_from = &mut key_buffer;
            range_from[..4].copy_from_slice(&from.shard.workchain().to_be_bytes());
            range_from[4..12].copy_from_slice(&from.shard.prefix().to_be_bytes());
            range_from[12..16].copy_from_slice(&from.seqno.to_be_bytes());

            let mut range_to = *range_from;
            range_to[12..16].copy_from_slice(&to.seqno.saturating_add(1).to_be_bytes());

            // At this point we have two keys:
            // [workchain, shard, from_seqno, 0...]
            // [workchain, shard, to_seqno + 1, 0...]
            //
            // It will delete all entries in range [from_seqno, to_seqno) for this shard.
            // Note that package entry keys are the same as block connection keys.
            batch.delete_range_cf(&full_block_ids_cf, &*range_from, &range_to);
            batch.delete_range_cf(&package_entries_cf, &*range_from, &range_to);
            batch.delete_range_cf(&block_data_entries_cf, &*range_from, &range_to);
            batch.delete_range_cf(&block_connections_cf, &*range_from, &range_to);

            tracing::debug!(%from, %to, "delete_range");
        };

    let mut cancelled = cancelled.map(|c| c.debounce(100));
    let mut current_range = None::<(BlockIdShort, BlockIdShort)>;
    loop {
        let key = match blocks_iter.key() {
            Some(key) => key,
            None => break blocks_iter.status()?,
        };

        if let Some(cancelled) = &mut cancelled {
            if cancelled.check() {
                anyhow::bail!("blocks GC cancelled");
            }
        }

        // Key structure:
        // [workchain id, 4 bytes]  |
        // [shard id, 8 bytes]      | BlockIdShort
        // [seqno, 4 bytes]         |
        // [root hash, 32 bytes] <-
        // ..
        let block_id = BlockIdShort::from_slice(key);
        let root_hash: &[u8; 32] = key[16..48].try_into().unwrap();
        let is_masterchain = block_id.shard.is_masterchain();

        // Don't gc latest blocks, key blocks or persistent blocks
        if block_id.seqno == 0
            || is_masterchain && block_id.seqno >= mc_seqno
            || !is_masterchain
                && shard_heights.contains_shard_seqno(&block_id.shard, block_id.seqno)
            || is_persistent(root_hash)?
        {
            // Remove the current range
            if let Some((from, to)) = current_range.take() {
                delete_range(&mut batch, &from, &to);
                batch_len += 1; // Ensure that we flush the batch
            }
            blocks_iter.next();
            continue;
        }

        match &mut current_range {
            // Delete the previous range and start a new one
            Some((from, to)) if from.shard != block_id.shard => {
                delete_range(&mut batch, from, to);
                *from = block_id;
                *to = block_id;
            }
            // Update the current range
            Some((_, to)) => *to = block_id,
            // Start a new range
            None => current_range = Some((block_id, block_id)),
        }

        // Count entry
        stats.total_blocks_removed += 1;
        if is_masterchain {
            stats.mc_blocks_removed += 1;
        }

        batch.delete_cf(&block_handles_cf, root_hash);

        batch_len += 1;
        if matches!(
            max_blocks_per_batch,
            Some(max_blocks_per_batch) if batch_len >= max_blocks_per_batch
        ) {
            tracing::info!(
                total_blocks_removed = stats.total_blocks_removed,
                "applying intermediate batch",
            );
            let batch = std::mem::take(&mut batch);
            raw.write(batch)?;
            batch_len = 0;
        }

        blocks_iter.next();
    }

    if let Some((from, to)) = current_range.take() {
        delete_range(&mut batch, &from, &to);
        batch_len += 1; // Ensure that we flush the batch
    }

    if batch_len > 0 {
        tracing::info!("applying final batch");
        raw.write(batch)?;
    }

    // Done
    Ok(stats)
}

pub struct BlockStorageConfig {
    pub archive_chunk_size: ByteSize,
    pub blocks_cache: BlocksCacheConfig,
    pub split_block_tasks: usize,
}

#[derive(Debug, Copy, Clone, Default, PartialEq, Eq)]
pub struct BlockGcStats {
    pub mc_blocks_removed: usize,
    pub total_blocks_removed: usize,
}

struct FullBlockDataGuard<'a> {
    _lock: BlockDataGuard<'a>,
    data: rocksdb::DBPinnableSlice<'a>,
}

impl AsRef<[u8]> for FullBlockDataGuard<'_> {
    fn as_ref(&self) -> &[u8] {
        self.data.as_ref()
    }
}

fn extract_entry_type(key: &[u8]) -> Option<ArchiveEntryType> {
    key.get(48).copied().and_then(ArchiveEntryType::from_byte)
}

const ARCHIVE_PACKAGE_SIZE: u32 = 100;
const BLOCK_DATA_CHUNK_SIZE: u32 = 1024 * 1024; // 1MB

// Reserved key in which the compressed block size is stored
const BLOCK_DATA_SIZE_MAGIC: u32 = u32::MAX;
// Reserved key in which we store the fact that compressed block was started
const BLOCK_DATA_STARTED_MAGIC: u32 = u32::MAX - 2;

#[derive(Default, Debug)]
pub struct PreparedArchiveId {
    /// ID of the archive this block belongs to
    id: u32,
    /// Which archive ID (if any) should be committed now?
    to_commit: Option<u32>,
}

type ArchiveIdsTx = broadcast::Sender<u32>;
type BlocksCache = moka::sync::Cache<BlockId, BlockStuff, FastHasherState>;

#[derive(thiserror::Error, Debug)]
enum BlockStorageError {
    #[error("Archive not found")]
    ArchiveNotFound,
    #[error("Block data not found")]
    BlockDataNotFound,
    #[error("Block proof not found")]
    BlockProofNotFound,
    #[error("Queue diff not found")]
    QueueDiffNotFound,
    #[error("Block handle id mismatch")]
    BlockHandleIdMismatch,
    #[error("Block handle not found")]
    BlockHandleNotFound,
    #[error("Package entry not found")]
    PackageEntryNotFound,
    #[error("Offset is outside of the archive slice")]
    InvalidOffset,
}

#[cfg(test)]
mod tests {
    use std::pin::pin;

    use ahash::HashMap;
    use tycho_block_util::archive::WithArchiveData;
    use tycho_util::futures::JoinTask;

    use super::*;
    use crate::{BlockConnection, Storage};

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn parallel_store_data() -> Result<()> {
        let (storage, _tmp_dir) = Storage::new_temp().await?;

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

        let (storage, _tmp_dir) = Storage::new_temp().await?;

        let blocks = storage.block_storage();
        let block_handles = storage.block_handle_storage();
        let block_connections = storage.block_connection_storage();

        let mut shard_block_ids: std::collections::HashMap<
            ShardIdent,
            Vec<BlockId>,
            ahash::RandomState,
        > = HashMap::<ShardIdent, Vec<BlockId>>::default();

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

                for ty in ENTRY_TYPES {
                    blocks.add_data(&(block_id, ty).into(), GARBAGE)?;
                }
                for direction in CONNECTION_TYPES {
                    block_connections.store_connection(&handle, direction, &block_id);
                }

                handle.meta().add_flags(BlockFlags::HAS_ALL_BLOCK_PARTS);
                block_handles.store_handle(&handle, false);
            }
        }

        // Remove some blocks
        let stats = remove_blocks(
            blocks.db.clone(),
            None,
            70,
            [(ShardIdent::BASECHAIN, 50)].into(),
            None,
        )?;
        assert_eq!(stats, BlockGcStats {
            mc_blocks_removed: 69,
            total_blocks_removed: 69 + 49,
        });

        let removed_ranges = HashMap::from_iter([
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
                    let stored = blocks.db.package_entries.get(key.to_vec())?;
                    assert_eq!(stored.is_none(), must_be_removed);
                }

                for direction in CONNECTION_TYPES {
                    let connection = block_connections.load_connection(&block_id, direction);
                    assert_eq!(connection.is_none(), must_be_removed);
                }
            }
        }

        // Remove single block
        let stats = remove_blocks(
            blocks.db.clone(),
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
        let stats = remove_blocks(
            blocks.db.clone(),
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
