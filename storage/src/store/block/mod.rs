use std::collections::BTreeSet;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use everscale_types::boc::{Boc, BocRepr};
use everscale_types::cell::HashBytes;
use everscale_types::models::*;
use parking_lot::RwLock;
use tl_proto::TlWrite;
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
use tycho_util::sync::rayon_run;
use tycho_util::FastHashSet;
use weedb::{rocksdb, ColumnFamily, OwnedPinnableSlice};

pub use self::package_entry::{BlockDataEntryKey, PackageEntryKey, PartialBlockId};
use crate::db::*;
use crate::util::*;
use crate::{
    BlockConnectionStorage, BlockDataGuard, BlockFlags, BlockHandle, BlockHandleStorage,
    HandleCreationStatus, NewBlockMeta,
};

mod package_entry;

pub struct BlockStorage {
    db: BaseDb,
    block_handle_storage: Arc<BlockHandleStorage>,
    block_connection_storage: Arc<BlockConnectionStorage>,
    archive_ids: RwLock<BTreeSet<u32>>,
    block_subscriptions: SlotSubscriptions<BlockId, BlockStuff>,
    store_block_data: tokio::sync::RwLock<()>,
    prev_archive_commit: tokio::sync::Mutex<Option<CommitArchiveTask>>,
}

impl BlockStorage {
    // === Init stuff ===

    pub fn new(
        db: BaseDb,
        block_handle_storage: Arc<BlockHandleStorage>,
        block_connection_storage: Arc<BlockConnectionStorage>,
    ) -> Self {
        Self {
            db,
            block_handle_storage,
            block_connection_storage,
            archive_ids: Default::default(),
            block_subscriptions: Default::default(),
            store_block_data: Default::default(),
            prev_archive_commit: Default::default(),
        }
    }

    // NOTE: This is intentionally a method, not a constant because
    // it might be useful to allow configure it during the first run.
    pub fn archive_chunk_size(&self) -> NonZeroU32 {
        NonZeroU32::new(ARCHIVE_CHUNK_SIZE as _).unwrap()
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
                Some(a) => a,
                None => return Err(BlockStorageError::BlockDataNotFound.into()),
            };

            self.spawn_split_block_data(&block_id, &data).await??;
        }

        tracing::info!(
            elapsed = %humantime::format_duration(started_at.elapsed()),
            "finished handling unfinished blocks"
        );

        Ok(())
    }

    /// Iterates over all archives and preloads their ids into memory.
    pub async fn preload_archive_ids(&self) -> Result<()> {
        let started_at = Instant::now();

        tracing::info!("started preloading archive ids");

        let mut iter = self.db.archives.raw_iterator();
        iter.seek_to_first();

        let mut archive_ids = BTreeSet::new();
        let mut archives_to_commit = Vec::new();
        loop {
            let Some(key) = iter.key() else {
                if let Err(e) = iter.status() {
                    tracing::error!("failed to iterate through archives: {e:?}");
                }
                break;
            };

            let archive_id = u32::from_be_bytes(key[..4].try_into().unwrap());
            let chunk_index = u64::from_be_bytes(key[4..].try_into().unwrap());

            const _: () = const {
                // Rely on the specific order of these constants
                assert!(
                    ARCHIVE_STARTED_MAGIC < ARCHIVE_TO_COMMIT_MAGIC
                        && ARCHIVE_TO_COMMIT_MAGIC < ARCHIVE_SIZE_MAGIC
                );
            };

            // Chunk keys are sorted by offset.
            match chunk_index {
                // "Started" magic comes first, and indicates that the archive exists.
                ARCHIVE_STARTED_MAGIC => {
                    archive_ids.insert(archive_id);
                }
                // "To commit" magic comes next, commit should have been started.
                ARCHIVE_TO_COMMIT_MAGIC => {
                    anyhow::ensure!(
                        archive_ids.contains(&archive_id),
                        "invalid archive TO_COMMIT entry"
                    );
                    archives_to_commit.push(archive_id);
                }
                // "Size" magic comes last, and indicates that the archive is fully committed.
                ARCHIVE_SIZE_MAGIC => {
                    // Last archive is already committed
                    let last = archives_to_commit.pop();
                    anyhow::ensure!(last == Some(archive_id), "invalid archive SIZE entry");

                    // Require only contiguous uncommited archives list
                    anyhow::ensure!(archives_to_commit.is_empty(), "skipped archive commit");
                }
                _ => {}
            }

            iter.next();
        }

        drop(iter);

        self.archive_ids.write().extend(archive_ids);

        tracing::info!(
            elapsed = %humantime::format_duration(started_at.elapsed()),
            "finished preloading archive ids"
        );

        for archive_id in archives_to_commit {
            tracing::info!(archive_id, "found partially committed archive");
            let mut task = self.spawn_commit_archive(archive_id);
            task.finish().await?;
        }

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
        let _guard = self.store_block_data.read().await;

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
                self.add_data_ext(&archive_id, data)?;
                if handle.meta().add_flags(BlockFlags::HAS_DATA) {
                    self.block_handle_storage.store_handle(&handle);
                    updated = true;
                }
            }
        }

        // TODO: only notify subscribers if `updated`?
        self.block_subscriptions.notify(block_id, block);

        Ok(StoreBlockResult {
            handle,
            updated,
            new: status == HandleCreationStatus::Created,
        })
    }

    pub async fn load_block_data(&self, handle: &BlockHandle) -> Result<BlockStuff> {
        const BIG_DATA_THRESHOLD: usize = 1 << 20; // 1 MB

        let _histogram = HistogramGuard::begin("tycho_storage_load_block_data_time");

        if !handle.has_data() {
            return Err(BlockStorageError::BlockDataNotFound.into());
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
        self.archive_ids.read().iter().cloned().collect()
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
                    self.block_handle_storage.store_handle(&handle);
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
                    self.block_handle_storage.store_handle(&handle);
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
        let raw_diff = self.load_queue_raw_ref(handle).await?;
        QueueDiffStuff::deserialize(handle.id(), raw_diff.as_ref())
    }

    pub async fn load_queue_diff_raw(&self, handle: &BlockHandle) -> Result<OwnedPinnableSlice> {
        if !handle.has_queue_diff() {
            return Err(BlockStorageError::QueueDiffNotFound.into());
        }

        self.get_data(handle, &PackageEntryKey::queue_diff(handle.id()))
            .await
    }

    pub async fn load_queue_raw_ref<'a>(
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
    pub async fn move_into_archive(&self, handle: &BlockHandle) -> Result<()> {
        let _histogram = HistogramGuard::begin("tycho_storage_move_into_archive_time");

        // Prepare data
        let block_id_bytes = handle.id().to_vec();

        // Prepare cf
        let archive_block_ids_cf = self.db.archive_block_ids.cf();
        let chunks_cf = self.db.archives.cf();

        // Prepare archive
        let archive_id = self.prepare_archive_id(handle);
        let archive_id_bytes = archive_id.id.to_be_bytes();

        // 0. Create transaction
        let mut batch = rocksdb::WriteBatch::default();

        // 1. Append archive block id
        batch.merge_cf(&archive_block_ids_cf, archive_id_bytes, &block_id_bytes);

        // 2. Store info that new archive was started
        if archive_id.is_new {
            let mut key = [0u8; tables::Archives::KEY_LEN];
            key[..4].copy_from_slice(&archive_id_bytes);
            key[4..].copy_from_slice(&ARCHIVE_STARTED_MAGIC.to_be_bytes());
            batch.put_cf(&chunks_cf, key, []);
        }
        // 3. Store info that archive commit is in progress
        if let Some(to_commit) = archive_id.to_commit {
            let mut key = [0u8; tables::Archives::KEY_LEN];
            key[..4].copy_from_slice(&to_commit.to_be_bytes());
            key[4..].copy_from_slice(&ARCHIVE_TO_COMMIT_MAGIC.to_be_bytes());
            batch.put_cf(&chunks_cf, key, []);
        }
        // 4. Execute transaction
        self.db.rocksdb().write(batch)?;

        tracing::debug!(block_id = %handle.id(), "saved block id into archive");
        // Block will be removed after blocks gc

        if let Some(to_commit) = archive_id.to_commit {
            // Commit previous archive
            let mut prev_archive_commit = self.prev_archive_commit.lock().await;

            // NOTE: Wait on reference to make sure that the task is cancel safe
            if let Some(task) = &mut *prev_archive_commit {
                task.finish().await?;
            }
            *prev_archive_commit = Some(self.spawn_commit_archive(to_commit));
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
    pub fn get_archive_id(&self, mc_seqno: u32) -> ArchiveId {
        let archive_ids = self.archive_ids.read();

        if !matches!(archive_ids.last(), Some(id) if mc_seqno < *id) {
            // Return `TooNew` if there are no archives yet, or the requested
            // seqno is greater than the beginning of the last archive. beg
            return ArchiveId::TooNew;
        }

        match archive_ids.range(..=mc_seqno).next_back() {
            // NOTE: handles case when mc_seqno is far in the future.
            // However if there is a key block between `id` and `mc_seqno`,
            // this will return an archive without that specified block.
            Some(id) if mc_seqno < id + ARCHIVE_PACKAGE_SIZE => ArchiveId::Found(*id),
            _ => ArchiveId::NotFound,
        }
    }

    pub fn get_archive_size(&self, id: u32) -> Result<Option<usize>> {
        let mut key = [0u8; tables::Archives::KEY_LEN];
        key[..4].copy_from_slice(&id.to_be_bytes());
        key[4..].copy_from_slice(&ARCHIVE_SIZE_MAGIC.to_be_bytes());

        match self.db.archives.get(key.as_slice())? {
            Some(slice) => Ok(Some(
                u64::from_le_bytes(slice.as_ref().try_into().unwrap()) as usize
            )),
            None => Ok(None),
        }
    }

    /// Loads an archive chunk.
    pub async fn get_archive_chunk(&self, id: u32, offset: u64) -> Result<OwnedPinnableSlice> {
        let chunk_size = self.archive_chunk_size().get() as u64;
        if offset % chunk_size != 0 {
            return Err(BlockStorageError::InvalidOffset.into());
        }

        let chunk_index = offset / chunk_size;

        let mut key = [0u8; tables::Archives::KEY_LEN];
        key[..4].copy_from_slice(&id.to_be_bytes());
        key[4..].copy_from_slice(&chunk_index.to_be_bytes());

        let chunk = self
            .db
            .archives
            .get(key.as_slice())?
            .ok_or(BlockStorageError::ArchiveNotFound)?;

        // SAFETY: A value was received from the same RocksDB instance.
        Ok(unsafe { OwnedPinnableSlice::new(self.db.rocksdb().clone(), chunk) })
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

        let span = tracing::Span::current();
        let db = self.db.clone();
        let BlockGcStats {
            mc_entries_removed,
            total_entries_removed,
        } = rayon_run(move || {
            let _span = span.enter();
            remove_blocks(db, max_blocks_per_batch, mc_seqno, shard_heights)
        })
        .await?;

        tracing::info!(
            total_cached_handles_removed,
            mc_entries_removed,
            total_entries_removed,
            "finished blocks GC"
        );
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn remove_outdated_archives(&self, until_id: u32) -> Result<()> {
        tracing::info!("started archives GC");

        let mut archive_ids = self.archive_ids.write();

        let retained_ids = match archive_ids.iter().rev().find(|&id| *id < until_id).cloned() {
            // Splits `archive_ids` into two parts - [..until_id] and [until_id..]
            // `archive_ids` will now contain [..until_id]
            Some(until_id) => archive_ids.split_off(&until_id),
            None => {
                tracing::info!("nothing to remove");
                return Ok(());
            }
        };
        // so we must swap maps to retain [until_id..] and get ids to remove
        let removed_ids = std::mem::replace(&mut *archive_ids, retained_ids);

        // Print removed range bounds and compute real `until_id`
        let (Some(first), Some(last)) = (removed_ids.first(), removed_ids.last()) else {
            tracing::info!("nothing to remove");
            return Ok(());
        };

        let len = removed_ids.len();
        let until_id = match archive_ids.first() {
            Some(until_id) => *until_id,
            None => *last + 1,
        };

        drop(archive_ids);

        // Remove all archives in range `[0, until_id)`
        let archives_cf = self.db.archives.cf();
        let write_options = self.db.archives.write_config();

        let start_key = [0u8; tables::Archives::KEY_LEN];

        // NOTE: End key points to the first entry of the `until_id` archive,
        // because `delete_range` removes all entries in range ["from", "to").
        let mut end_key = [0u8; tables::Archives::KEY_LEN];
        end_key[..4].copy_from_slice(&until_id.to_be_bytes());
        end_key[4..].copy_from_slice(&[0; 8]);

        self.db
            .rocksdb()
            .delete_range_cf_opt(&archives_cf, start_key, end_key, write_options)?;

        tracing::info!(archive_count = len, first, last, "finished archives GC");
        Ok(())
    }

    // === Internal ===

    fn add_data(&self, id: &PackageEntryKey, data: &[u8]) -> Result<(), rocksdb::Error> {
        self.db.package_entries.insert(id.to_vec(), data)
    }

    fn add_data_ext(&self, id: &PackageEntryKey, data: &[u8]) -> Result<(), rocksdb::Error> {
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
        let _handle = self.spawn_split_block_data(&id.block_id, data);

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

    fn prepare_archive_id(&self, handle: &BlockHandle) -> PreparedArchiveId {
        let mc_seqno = handle.mc_ref_seqno();

        let mut archive_ids = self.archive_ids.write();

        // Get the closest archive id
        let prev_id = archive_ids.range(..=mc_seqno).next_back().cloned();

        if handle.is_key_block() {
            let is_new = archive_ids.insert(mc_seqno);
            return PreparedArchiveId {
                id: mc_seqno,
                is_new,
                to_commit: if is_new { prev_id } else { None },
            };
        }

        let mut archive_id = PreparedArchiveId {
            id: prev_id.unwrap_or_default(),
            ..Default::default()
        };

        let is_first_archive = prev_id.is_none();
        if is_first_archive || mc_seqno.saturating_sub(archive_id.id) >= ARCHIVE_PACKAGE_SIZE {
            let is_new = archive_ids.insert(mc_seqno);
            archive_id = PreparedArchiveId {
                id: mc_seqno,
                is_new,
                to_commit: if is_new { prev_id } else { None },
            };
        }

        // NOTE: subtraction is intentional to panic if archive_id > mc_seqno
        debug_assert!(mc_seqno - archive_id.id <= ARCHIVE_PACKAGE_SIZE);

        archive_id
    }

    #[tracing::instrument(skip(self))]
    fn spawn_commit_archive(&self, archive_id: u32) -> CommitArchiveTask {
        let db = self.db.clone();
        let block_handle_storage = self.block_handle_storage.clone();
        let chunk_size = self.archive_chunk_size().get() as u64;

        let span = tracing::Span::current();
        let is_running = Arc::new(AtomicBool::new(true));
        let handle = tokio::task::spawn_blocking({
            let is_running = is_running.clone();
            move || {
                let _span = span.enter();

                let histogram = HistogramGuard::begin("tycho_storage_commit_archive_time");

                tracing::info!("started");
                let guard = scopeguard::guard((), |_| {
                    tracing::warn!("cancelled");
                });

                let raw_block_ids = db
                    .archive_block_ids
                    .get(archive_id.to_be_bytes())?
                    .ok_or(BlockStorageError::ArchiveNotFound)?;
                assert_eq!(raw_block_ids.len() % BlockId::SIZE_HINT, 0);

                let mut writer = ArchiveWriter::new(&db, archive_id, chunk_size)?;
                let mut header_buffer = Vec::with_capacity(ARCHIVE_ENTRY_HEADER_LEN);

                // Write archive prefix
                writer.write(&ARCHIVE_PREFIX)?;

                // Write all entries
                let mut unique_ids = FastHashSet::default();
                for raw_block_id in raw_block_ids.chunks_exact(BlockId::SIZE_HINT) {
                    anyhow::ensure!(is_running.load(Ordering::Relaxed), "task aborted");

                    let block_id = BlockId::from_slice(raw_block_id);
                    if !unique_ids.insert(block_id) {
                        tracing::warn!(%block_id, "skipped duplicate block id");
                        continue;
                    }

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

                    for ty in [
                        ArchiveEntryType::Block,
                        ArchiveEntryType::Proof,
                        ArchiveEntryType::QueueDiff,
                    ] {
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
                }

                // Drop ids entry just in case (before removing it)
                drop(raw_block_ids);

                // Finalize the archive
                writer.finalize()?;

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
            is_running,
            handle: Some(handle),
        }
    }

    #[tracing::instrument(skip(self, data))]
    fn spawn_split_block_data(
        &self,
        block_id: &PartialBlockId,
        data: &[u8],
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

                Ok(())
            }
        });

        handle
    }
}

struct CommitArchiveTask {
    archive_id: u32,
    is_running: Arc<AtomicBool>,
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
        self.is_running.store(false, Ordering::Relaxed);
        if let Some(handle) = &self.handle {
            handle.abort();
        }
    }
}

struct ArchiveWriter<'a> {
    db: &'a BaseDb,
    archive_id: u32,
    chunk_len: usize,
    total_len: u64,
    chunk_index: u64,
    chunks_buffer: Vec<u8>,
    zstd_compressor: ZstdCompressStream<'a>,
}

impl<'a> ArchiveWriter<'a> {
    fn new(db: &'a BaseDb, archive_id: u32, chunk_len: u64) -> Result<Self> {
        let chunk_len = chunk_len as usize;
        let zstd_compressor = ZstdCompressStream::new(9, chunk_len)?;

        Ok(Self {
            db,
            archive_id,
            chunk_len,
            total_len: 0,
            chunk_index: 0,
            chunks_buffer: Vec::with_capacity(chunk_len),
            zstd_compressor,
        })
    }

    fn write(&mut self, data: &[u8]) -> Result<()> {
        self.zstd_compressor.write(data, &mut self.chunks_buffer)?;
        self.flush(false)
    }

    fn finalize(mut self) -> Result<()> {
        self.zstd_compressor.finish(&mut self.chunks_buffer)?;

        // Write the last chunk
        self.flush(true)?;
        debug_assert!(self.chunks_buffer.is_empty());

        // Write archive size and remove archive block ids atomically
        let archives_cf = self.db.archives.cf();
        let block_ids_cf = self.db.archive_block_ids.cf();

        let mut batch = rocksdb::WriteBatch::default();

        // Write a special entry with the total size of the archive
        let mut key = [0u8; tables::Archives::KEY_LEN];
        key[..4].copy_from_slice(&self.archive_id.to_be_bytes());
        key[4..].copy_from_slice(&ARCHIVE_SIZE_MAGIC.to_be_bytes());
        batch.put_cf(&archives_cf, key.as_slice(), self.total_len.to_le_bytes());

        // Remove related block ids
        batch.delete_cf(&block_ids_cf, self.archive_id.to_be_bytes());

        self.db.rocksdb().write(batch)?;
        Ok(())
    }

    fn flush(&mut self, finalize: bool) -> Result<()> {
        let buffer_len = self.chunks_buffer.len();
        if buffer_len == 0 {
            return Ok(());
        }

        let mut key = [0u8; tables::Archives::KEY_LEN];
        key[..4].copy_from_slice(&self.archive_id.to_be_bytes());

        let mut do_flush = |data: &[u8]| {
            key[4..].copy_from_slice(&self.chunk_index.to_be_bytes());

            self.total_len += data.len() as u64;
            self.chunk_index += 1;

            self.db.archives.insert(key, data)
        };

        // Write all full chunks
        let mut buffer_offset = 0;
        while buffer_offset + self.chunk_len <= buffer_len {
            do_flush(&self.chunks_buffer[buffer_offset..buffer_offset + self.chunk_len])?;
            buffer_offset += self.chunk_len;
        }

        if finalize {
            // Just write the remaining data on finalize
            do_flush(&self.chunks_buffer[buffer_offset..])?;
            self.chunks_buffer.clear();
        } else {
            // Shift the remaining data to the beginning of the buffer and clear the rest
            let rem = buffer_len % self.chunk_len;
            if rem == 0 {
                self.chunks_buffer.clear();
            } else if buffer_offset > 0 {
                // TODO: Use memmove since we are copying non-overlapping regions
                self.chunks_buffer.copy_within(buffer_offset.., 0);
                self.chunks_buffer.truncate(rem);
            }
        }

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
pub enum ArchiveId {
    Found(u32),
    TooNew,
    NotFound,
}

fn remove_blocks(
    db: BaseDb,
    max_blocks_per_batch: Option<usize>,
    mc_seqno: u32,
    shard_heights: ShardHeights,
) -> Result<BlockGcStats> {
    let mut stats = BlockGcStats::default();

    let raw = db.rocksdb().as_ref();
    let block_connections_cf = db.block_connections.cf();
    let package_entries_cf = db.package_entries.cf();
    let block_data_entries_cf = db.block_data_entries.cf();
    let block_handles_cf = db.block_handles.cf();
    let key_blocks_cf = db.key_blocks.cf();

    // Create batch
    let mut batch = rocksdb::WriteBatch::default();
    let mut batch_len = 0;

    let package_entries_readopts = db.package_entries.new_read_config();
    let key_blocks_readopts = db.key_blocks.new_read_config();

    // Iterate all entries and find expired items
    let mut blocks_iter = raw.raw_iterator_cf_opt(&package_entries_cf, package_entries_readopts);
    blocks_iter.seek_to_first();

    let is_key_block = |seqno: u32| {
        raw.get_pinned_cf_opt(&key_blocks_cf, seqno.to_be_bytes(), &key_blocks_readopts)
            .map(|value| value.is_some())
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
            batch.delete_range_cf(&package_entries_cf, &*range_from, &range_to);
            batch.delete_range_cf(&block_data_entries_cf, &*range_from, &range_to);
            batch.delete_range_cf(&block_connections_cf, &*range_from, &range_to);

            tracing::debug!(%from, %to, "delete_range");
        };

    let mut current_range = None::<(BlockIdShort, BlockIdShort)>;
    loop {
        let key = match blocks_iter.key() {
            Some(key) => key,
            None => break blocks_iter.status()?,
        };

        // Read only prefix with shard ident and seqno
        let block_id = BlockIdShort::from_slice(key);
        let is_masterchain = block_id.shard.is_masterchain();

        // Don't gc latest blocks or key blocks
        if block_id.seqno == 0
            || is_masterchain && (block_id.seqno >= mc_seqno || is_key_block(block_id.seqno)?)
            || !is_masterchain
                && shard_heights.contains_shard_seqno(&block_id.shard, block_id.seqno)
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
        stats.total_entries_removed += 1;
        if is_masterchain {
            stats.mc_entries_removed += 1;
        }

        // Key structure:
        // [workchain id, 4 bytes]
        // [shard id, 8 bytes]
        // [seqno, 4 bytes]
        // [root hash, 32 bytes] <-
        // ..
        batch.delete_cf(&block_handles_cf, &key[16..48]);

        batch_len += 1;
        if matches!(
            max_blocks_per_batch,
            Some(max_blocks_per_batch) if batch_len >= max_blocks_per_batch
        ) {
            tracing::info!(
                total_package_entries_removed = stats.total_entries_removed,
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

#[derive(Debug, Copy, Clone, Default, PartialEq, Eq)]
pub struct BlockGcStats {
    pub mc_entries_removed: usize,
    pub total_entries_removed: usize,
}

struct FullBlockDataGuard<'a> {
    _lock: BlockDataGuard<'a>,
    data: rocksdb::DBPinnableSlice<'a>,
}

impl<'a> AsRef<[u8]> for FullBlockDataGuard<'a> {
    fn as_ref(&self) -> &[u8] {
        self.data.as_ref()
    }
}

fn extract_entry_type(key: &[u8]) -> Option<ArchiveEntryType> {
    key.get(48).copied().and_then(ArchiveEntryType::from_byte)
}

const ARCHIVE_PACKAGE_SIZE: u32 = 100;
const ARCHIVE_CHUNK_SIZE: u64 = 1024 * 1024; // 1MB

// Reserved key in which the archive size is stored
const ARCHIVE_SIZE_MAGIC: u64 = u64::MAX;
// Reserved key in which we store the fact that the archive must be committed
const ARCHIVE_TO_COMMIT_MAGIC: u64 = u64::MAX - 1;
// Reserved key in which we store the fact that archive was started
const ARCHIVE_STARTED_MAGIC: u64 = u64::MAX - 2;

const BLOCK_DATA_CHUNK_SIZE: u32 = 1024 * 1024; // 1MB

// Reserved key in which the compressed block size is stored
const BLOCK_DATA_SIZE_MAGIC: u32 = u32::MAX;
// Reserved key in which we store the fact that compressed block was started
const BLOCK_DATA_STARTED_MAGIC: u32 = u32::MAX - 2;

#[derive(Default)]
struct PreparedArchiveId {
    id: u32,
    is_new: bool,
    to_commit: Option<u32>,
}

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
                block_handles.store_handle(&handle);
            }
        }

        // Remove some blocks
        let stats = remove_blocks(
            blocks.db.clone(),
            None,
            70,
            [(ShardIdent::BASECHAIN, 50)].into(),
        )?;
        assert_eq!(stats, BlockGcStats {
            mc_entries_removed: 69 * ENTRY_TYPES.len(),
            total_entries_removed: (69 + 49) * ENTRY_TYPES.len(),
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
        )?;
        assert_eq!(stats, BlockGcStats {
            mc_entries_removed: ENTRY_TYPES.len(),
            total_entries_removed: 2 * ENTRY_TYPES.len(),
        });

        // Remove no blocks
        let stats = remove_blocks(
            blocks.db.clone(),
            None,
            71,
            [(ShardIdent::BASECHAIN, 51)].into(),
        )?;
        assert_eq!(stats, BlockGcStats {
            mc_entries_removed: 0,
            total_entries_removed: 0,
        });

        Ok(())
    }
}
