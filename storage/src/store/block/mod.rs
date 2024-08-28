use std::borrow::Borrow;
use std::collections::BTreeSet;
use std::hash::Hash;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use everscale_types::boc::BocRepr;
use everscale_types::cell::HashBytes;
use everscale_types::models::*;
use parking_lot::RwLock;
use tl_proto::TlWrite;
use tokio::task::JoinHandle;
use tycho_block_util::archive::{
    ArchiveData, ArchiveEntryHeader, ArchiveEntryId, ArchiveEntryType, ArchiveReaderError,
    ArchiveVerifier, ARCHIVE_ENTRY_HEADER_LEN, ARCHIVE_PREFIX,
};
use tycho_block_util::block::{
    BlockProofStuff, BlockProofStuffAug, BlockStuff, BlockStuffAug, ShardHeights,
};
use tycho_block_util::queue::{QueueDiffStuff, QueueDiffStuffAug};
use tycho_util::metrics::HistogramGuard;
use tycho_util::sync::rayon_run;
use weedb::rocksdb;

use crate::db::*;
use crate::models::*;
use crate::util::*;
use crate::{BlockConnectionStorage, BlockHandleStorage, HandleCreationStatus};

pub struct BlockStorage {
    db: BaseDb,
    block_handle_storage: Arc<BlockHandleStorage>,
    block_connection_storage: Arc<BlockConnectionStorage>,
    archive_ids: RwLock<BTreeSet<u32>>,
    block_subscriptions: SlotSubscriptions<BlockId, BlockStuff>,
    store_block_data: tokio::sync::RwLock<()>,
    prev_archive_commit: tokio::sync::Mutex<Option<JoinHandle<Result<()>>>>,
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

    pub fn archive_chunk_size(&self) -> NonZeroU32 {
        NonZeroU32::new(ARCHIVE_CHUNK_SIZE as _).unwrap()
    }

    /// Iterates over all archives and preloads their ids into memory.
    pub fn preload_archive_ids(&self) -> Result<()> {
        fn check_archive(value: &[u8]) -> Result<(), ArchiveReaderError> {
            let mut verifier = ArchiveVerifier::default();
            verifier.write_verify(value)?;
            verifier.final_check()
        }

        let started_at = Instant::now();

        tracing::info!("started preloading archive ids");

        let mut iter = self.db.archives.raw_iterator();
        iter.seek_to_first();

        let mut new_archive_ids = BTreeSet::new();

        let mut current_archive_data = Vec::new();
        loop {
            let (key, value) = match iter.item() {
                Some(item) => item,
                None => {
                    if let Err(e) = iter.status() {
                        tracing::error!("failed to iterate through archives: {e:?}");
                    }
                    break;
                }
            };

            let archive_id = u32::from_be_bytes(key[..4].try_into().unwrap());
            let chunk_index = u64::from_be_bytes(key[4..].try_into().unwrap());

            if chunk_index == ARCHIVE_SIZE_MAGIC {
                check_archive(&current_archive_data)?;
                new_archive_ids.insert(archive_id);
                current_archive_data.clear();
            } else {
                current_archive_data.extend_from_slice(value);
            }

            iter.next();
        }

        self.archive_ids.write().extend(new_archive_ids);

        tracing::info!(
            elapsed = %humantime::format_duration(started_at.elapsed()),
            "finished preloading archive ids"
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
        let _guard = self.store_block_data.read().await;

        let block_id = block.id();
        let (handle, status) = self
            .block_handle_storage
            .create_or_load_handle(block_id, meta_data);

        let archive_id = ArchiveEntryId::block(block_id);
        let mut updated = false;
        if !handle.has_data() {
            let data = archive_data.as_new_archive_data()?;
            metrics::histogram!("tycho_storage_store_block_data_size").record(data.len() as f64);

            let _lock = handle.block_data_lock().write().await;
            if !handle.has_data() {
                self.add_data(&archive_id, data)?;
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
        let raw_block = self.load_block_data_raw_ref(handle).await?;
        BlockStuff::deserialize(handle.id(), raw_block.as_ref())
    }

    pub async fn load_block_data_raw(&self, handle: &BlockHandle) -> Result<Vec<u8>> {
        if !handle.has_data() {
            return Err(BlockStorageError::BlockDataNotFound.into());
        }
        self.get_data(handle, &ArchiveEntryId::block(handle.id()))
            .await
    }

    pub async fn load_block_data_raw_ref<'a>(
        &'a self,
        handle: &'a BlockHandle,
    ) -> Result<impl AsRef<[u8]> + 'a> {
        if !handle.has_data() {
            return Err(BlockStorageError::BlockDataNotFound.into());
        }
        self.get_data_ref(handle, &ArchiveEntryId::block(handle.id()))
            .await
    }

    pub fn find_mc_block_data(&self, mc_seqno: u32) -> Result<Option<Block>> {
        let package_entries = &self.db.package_entries;

        let mut bound = BlockId {
            shard: ShardIdent::MASTERCHAIN,
            seqno: mc_seqno,
            root_hash: HashBytes::ZERO,
            file_hash: HashBytes::ZERO,
        };

        let mut readopts = package_entries.new_read_config();
        readopts.set_iterate_lower_bound(entry_key(&bound, ArchiveEntryType::Block));
        bound.seqno += 1;
        readopts.set_iterate_upper_bound(entry_key(&bound, ArchiveEntryType::Block));

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
        let archive_id = ArchiveEntryId::proof(block_id);
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

    pub async fn load_block_proof_raw(&self, handle: &BlockHandle) -> Result<Vec<u8>> {
        if !handle.has_proof() {
            return Err(BlockStorageError::BlockProofNotFound.into());
        }

        self.get_data(handle, &ArchiveEntryId::proof(handle.id()))
            .await
    }

    pub async fn load_block_proof_raw_ref<'a>(
        &'a self,
        handle: &'a BlockHandle,
    ) -> Result<impl AsRef<[u8]> + 'a> {
        if !handle.has_proof() {
            return Err(BlockStorageError::BlockProofNotFound.into());
        }

        self.get_data_ref(handle, &ArchiveEntryId::proof(handle.id()))
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
        let archive_id = ArchiveEntryId::queue_diff(block_id);
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

    pub async fn load_queue_diff_raw(&self, handle: &BlockHandle) -> Result<Vec<u8>> {
        if !handle.has_queue_diff() {
            return Err(BlockStorageError::QueueDiffNotFound.into());
        }

        self.get_data(handle, &ArchiveEntryId::queue_diff(handle.id()))
            .await
    }

    pub async fn load_queue_raw_ref<'a>(
        &'a self,
        handle: &'a BlockHandle,
    ) -> Result<impl AsRef<[u8]> + 'a> {
        if !handle.has_queue_diff() {
            return Err(BlockStorageError::QueueDiffNotFound.into());
        }
        self.get_data_ref(handle, &ArchiveEntryId::queue_diff(handle.id()))
            .await
    }

    // === Archive stuff ===

    /// Loads data and proof for the block and appends them to the corresponding archive.
    pub async fn move_into_archive(&self, handle: &BlockHandle) -> Result<()> {
        let _histogram = HistogramGuard::begin("tycho_storage_move_into_archive_time");

        // Prepare data
        let block_id = handle.id();
        let block_id_bytes = handle.id().to_vec();

        // Prepare cf
        let storage_cf = self.db.archive_block_ids.cf();
        let handle_cf = self.db.block_handles.cf();

        // Prepare archive
        let archive_id = self.compute_archive_id(handle);
        let archive_id_bytes = archive_id.id.to_be_bytes();

        // 0. Create transaction
        let mut batch = rocksdb::WriteBatch::default();
        // 1. Append archive block id
        batch.merge_cf(&storage_cf, archive_id_bytes, &block_id_bytes);
        // 2. Update block handle meta
        if handle.meta().add_flags(BlockFlags::IS_ARCHIVED) {
            batch.put_cf(
                &handle_cf,
                block_id.root_hash.as_slice(),
                handle.meta().to_vec(),
            );
        }
        // 3. Execute transaction
        self.db.rocksdb().write(batch)?;

        tracing::trace!(block_id = %handle.id(), "saved block id into archive");
        // Block will be removed after blocks gc

        if let (Some(prev_id), true) = (archive_id.prev_id, archive_id.is_new) {
            // commit previous archive
            let mut prev_archive_commit = self.prev_archive_commit.lock().await;
            if let Some(handle) = prev_archive_commit.take() {
                handle.await??;
            }
            *prev_archive_commit = Some(self.spawn_commit_archive(prev_id).await);
        }

        // Done
        Ok(())
    }

    /// Returns a corresponding archive id for the specified masterchain seqno.
    pub fn get_archive_id(&self, mc_seqno: u32) -> Option<u32> {
        match self.archive_ids.read().range(..=mc_seqno).next_back() {
            // NOTE: handles case when mc_seqno is far in the future.
            // However if there is a key block between `id` and `mc_seqno`,
            // this will return an archive without that specified block.
            Some(id) if mc_seqno < id + ARCHIVE_PACKAGE_SIZE => Some(*id),
            _ => None,
        }
    }

    pub fn get_archive_size(&self, id: u32) -> Result<Option<usize>> {
        let mut key = [0u8; tables::Archives::KEY_LEN];
        key[..4].copy_from_slice(&id.to_be_bytes());
        key[4..].copy_from_slice(&ARCHIVE_SIZE_MAGIC.to_be_bytes());

        match self.db.archives.get(key.as_slice())? {
            Some(slice) => Ok(Some(usize::from_be_bytes(
                slice.as_ref().try_into().unwrap(),
            ))),
            None => Ok(None),
        }
    }

    /// Loads an archive chunk.
    pub async fn get_archive_chunk(&self, id: u32, offset: u64) -> Result<Vec<u8>> {
        let archive_size = self
            .get_archive_size(id)?
            .ok_or(BlockStorageError::ArchiveNotFound)? as u64;

        if offset % ARCHIVE_CHUNK_SIZE != 0 || offset >= archive_size {
            return Err(BlockStorageError::InvalidOffset.into());
        }

        let chunk_index = offset / ARCHIVE_CHUNK_SIZE;

        let mut key = [0u8; tables::Archives::KEY_LEN];
        key[..4].copy_from_slice(&id.to_be_bytes());
        key[4..].copy_from_slice(&chunk_index.to_be_bytes());

        let chunk = self
            .db
            .archives
            .get(key.as_slice())?
            .ok_or(BlockStorageError::ArchiveNotFound)?;

        Ok(chunk.to_vec())
    }

    // === GC stuff ===

    #[tracing::instrument(skip_all, fields(mc_seqno))]
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

    #[tracing::instrument(skip_all, fields(until_id))]
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

    fn add_data<I>(&self, id: &ArchiveEntryId<I>, data: &[u8]) -> Result<(), rocksdb::Error>
    where
        I: Borrow<BlockId>,
    {
        let key = entry_key(id.block_id.borrow(), id.ty);
        self.db.package_entries.insert(key, data)
    }

    async fn get_data<I>(&self, handle: &BlockHandle, id: &ArchiveEntryId<I>) -> Result<Vec<u8>>
    where
        I: Borrow<BlockId>,
    {
        let _lock = match id.ty {
            ArchiveEntryType::Block => handle.block_data_lock(),
            ArchiveEntryType::Proof => handle.proof_data_lock(),
            ArchiveEntryType::QueueDiff => handle.queue_diff_data_lock(),
        }
        .read()
        .await;

        let key = entry_key(id.block_id.borrow(), id.ty);
        match self.db.package_entries.get(key)? {
            Some(a) => Ok(a.to_vec()),
            None => Err(BlockStorageError::InvalidBlockData.into()),
        }
    }

    async fn get_data_ref<'a, I>(
        &'a self,
        handle: &'a BlockHandle,
        id: &ArchiveEntryId<I>,
    ) -> Result<impl AsRef<[u8]> + 'a>
    where
        I: Borrow<BlockId> + Hash,
    {
        let lock = match id.ty {
            ArchiveEntryType::Block => handle.block_data_lock(),
            ArchiveEntryType::Proof => handle.proof_data_lock(),
            ArchiveEntryType::QueueDiff => handle.queue_diff_data_lock(),
        }
        .read()
        .await;

        let key = entry_key(id.block_id.borrow(), id.ty);
        match self.db.package_entries.get(key)? {
            Some(data) => Ok(BlockContentsLock { _lock: lock, data }),
            None => Err(BlockStorageError::InvalidBlockData.into()),
        }
    }

    fn compute_archive_id(&self, handle: &BlockHandle) -> ArchiveId {
        let mc_seqno = handle.mc_ref_seqno();

        // Get the closest archive id
        let prev_id = {
            let latest_archives = self.archive_ids.read();
            latest_archives.range(..=mc_seqno).next_back().cloned()
        };

        if handle.is_key_block() {
            self.archive_ids.write().insert(mc_seqno);
            return ArchiveId {
                id: mc_seqno,
                is_new: true,
                prev_id,
            };
        }

        let mut archive_id = ArchiveId {
            id: prev_id.unwrap_or_default(),
            ..Default::default()
        };

        let is_first_archive = prev_id.is_none();
        if is_first_archive || mc_seqno.saturating_sub(archive_id.id) >= ARCHIVE_PACKAGE_SIZE {
            self.archive_ids.write().insert(mc_seqno);
            archive_id = ArchiveId {
                id: mc_seqno,
                is_new: true,
                prev_id,
            };
        }

        // NOTE: subtraction is intentional to panic if archive_id > mc_seqno
        debug_assert!(mc_seqno - archive_id.id <= ARCHIVE_PACKAGE_SIZE);

        archive_id
    }

    async fn spawn_commit_archive(&self, archive_id: u32) -> JoinHandle<Result<()>> {
        let db = self.db.clone();
        let block_handle_storage = self.block_handle_storage.clone();

        tokio::task::spawn_blocking(move || {
            let _histogram = HistogramGuard::begin("tycho_storage_commit_archive_time");

            let raw_block_ids = db
                .archive_block_ids
                .get(archive_id.to_be_bytes())?
                .ok_or(BlockStorageError::ArchiveNotFound)?;
            assert_eq!(raw_block_ids.len() % BlockId::SIZE_HINT, 0);

            let mut writer = ArchiveWriter::new(&db, archive_id, ARCHIVE_CHUNK_SIZE);
            let mut header_buffer = Vec::with_capacity(ARCHIVE_ENTRY_HEADER_LEN);

            // Write archive prefix
            writer.write(&ARCHIVE_PREFIX)?;

            // Write all entries
            for raw_block_id in raw_block_ids.chunks_exact(BlockId::SIZE_HINT) {
                let block_id = BlockId::from_slice(raw_block_id);
                let handle = block_handle_storage
                    .load_handle(&block_id)
                    .ok_or(BlockStorageError::BlockHandleNotFound)?;

                let flags = handle.meta().flags();
                anyhow::ensure!(
                    flags.contains(BlockFlags::HAS_ALL_BLOCK_PARTS),
                    "block not full"
                );

                for ty in [
                    ArchiveEntryType::Block,
                    ArchiveEntryType::Proof,
                    ArchiveEntryType::QueueDiff,
                ] {
                    let key = entry_key(&block_id, ty);
                    let Some(data) = db.package_entries.get(key).unwrap() else {
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

            // Write the remaining data in the buffer if any
            writer.flush()?;

            // Finalize the archive
            writer.finalize()
        })
    }
}

struct ArchiveWriter<'a> {
    db: &'a BaseDb,
    archive_id: u32,
    chunk_len: u64,
    total_len: u64,
    chunk_index: u64,
    chunk_buffer: Vec<u8>,
}

impl<'a> ArchiveWriter<'a> {
    fn new(db: &'a BaseDb, archive_id: u32, chunk_len: u64) -> Self {
        Self {
            db,
            archive_id,
            chunk_len,
            total_len: 0,
            chunk_index: 0,
            chunk_buffer: Vec::with_capacity(chunk_len as usize),
        }
    }

    fn write(&mut self, mut data: &[u8]) -> Result<()> {
        loop {
            let data_len = data.len() as u64;
            let current_chunk_len = self.chunk_buffer.len() as u64;
            if current_chunk_len + data_len < self.chunk_len {
                self.chunk_buffer.extend_from_slice(data);
                return Ok(());
            }

            let (prefix, rem) = data.split_at((self.chunk_len - current_chunk_len) as usize);
            self.chunk_buffer.extend_from_slice(prefix);
            debug_assert_eq!(self.chunk_buffer.len() as u64, self.chunk_len);

            self.flush()?;
            data = rem;
        }
    }

    fn flush(&mut self) -> Result<()> {
        if self.chunk_buffer.is_empty() {
            return Ok(());
        }

        let mut key = [0u8; tables::Archives::KEY_LEN];
        key[..4].copy_from_slice(&self.archive_id.to_be_bytes());
        key[4..].copy_from_slice(&self.chunk_index.to_be_bytes());

        self.total_len += self.chunk_buffer.len() as u64;
        self.chunk_index += 1;

        self.db.archives.insert(key, self.chunk_buffer.as_slice())?;
        self.chunk_buffer.clear();
        Ok(())
    }

    fn finalize(self) -> Result<()> {
        debug_assert!(self.chunk_buffer.is_empty());

        // Write archive size and remove archive block ids atomically
        let archives_cf = self.db.archives.cf();
        let block_ids_cf = self.db.archive_block_ids.cf();

        let mut batch = rocksdb::WriteBatch::default();

        // Write a special entry with the total size of the archive
        let mut key = [0u8; tables::Archives::KEY_LEN];
        key[..4].copy_from_slice(&self.archive_id.to_be_bytes());
        key[4..].copy_from_slice(&ARCHIVE_SIZE_MAGIC.to_be_bytes());
        batch.put_cf(&archives_cf, key.as_slice(), self.total_len.to_be_bytes());

        // Remove related block ids
        batch.delete_cf(&block_ids_cf, self.archive_id.to_be_bytes());

        self.db.rocksdb().write(batch)?;
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

struct BlockContentsLock<'a> {
    _lock: tokio::sync::RwLockReadGuard<'a, ()>,
    data: rocksdb::DBPinnableSlice<'a>,
}

impl<'a> AsRef<[u8]> for BlockContentsLock<'a> {
    fn as_ref(&self) -> &[u8] {
        self.data.as_ref()
    }
}

fn entry_key(block_id: &BlockId, ty: ArchiveEntryType) -> [u8; tables::PackageEntries::KEY_LEN] {
    let mut result = [0; tables::PackageEntries::KEY_LEN];
    result[..4].copy_from_slice(&block_id.shard.workchain().to_be_bytes());
    result[4..12].copy_from_slice(&block_id.shard.prefix().to_be_bytes());
    result[12..16].copy_from_slice(&block_id.seqno.to_be_bytes());
    result[16..48].copy_from_slice(block_id.root_hash.as_slice());
    result[48] = ty as u8;
    result
}

fn extract_entry_type(key: &[u8]) -> Option<ArchiveEntryType> {
    key.get(48).copied().and_then(ArchiveEntryType::from_byte)
}

const ARCHIVE_PACKAGE_SIZE: u32 = 100;
const ARCHIVE_SIZE_MAGIC: u64 = u64::MAX;
const ARCHIVE_CHUNK_SIZE: u64 = 1024 * 1024; // 1MB

#[derive(Default)]
struct ArchiveId {
    id: u32,
    is_new: bool,
    prev_id: Option<u32>,
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
    #[error("Invalid block data")]
    InvalidBlockData,
    #[error("Offset is outside of the archive slice")]
    InvalidOffset,
}

#[cfg(test)]
mod tests {
    use ahash::HashMap;

    use super::*;
    use crate::{BlockConnection, Storage};

    #[test]
    fn blocks_gc() -> Result<()> {
        const GARBAGE: &[u8] = b"garbage";
        const ENTRY_TYPES: [ArchiveEntryType; 3] = [
            ArchiveEntryType::Block,
            ArchiveEntryType::Proof,
            ArchiveEntryType::QueueDiff,
        ];
        const CONNECTION_TYPES: [BlockConnection; 2] =
            [BlockConnection::Prev1, BlockConnection::Next1];

        let (storage, _tmp_dir) = Storage::new_temp()?;

        let blocks = storage.block_storage();
        let block_handles = storage.block_handle_storage();
        let block_connections = storage.block_connection_storage();

        let mut shard_block_ids = HashMap::<ShardIdent, Vec<BlockId>>::default();

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
                    is_key_block: seqno == 0,
                    gen_utime: 0,
                    mc_ref_seqno: None,
                });

                for ty in ENTRY_TYPES {
                    blocks.add_data(&ArchiveEntryId { block_id, ty }, GARBAGE)?;
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
                    let key = entry_key(block_id.borrow(), ty);
                    let stored = blocks.db.package_entries.get(key)?;
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
