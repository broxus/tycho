use std::borrow::Borrow;
use std::collections::BTreeSet;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use everscale_types::boc::BocRepr;
use everscale_types::cell::HashBytes;
use everscale_types::models::*;
use parking_lot::RwLock;
use tycho_block_util::archive::{
    make_archive_entry, ArchiveData, ArchiveEntryId, ArchiveEntryIdKind, ArchiveReaderError,
    ArchiveVerifier, GetFileName,
};
use tycho_block_util::block::{
    BlockProofStuff, BlockProofStuffAug, BlockStuff, BlockStuffAug, ShardHeights,
};
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
}

impl BlockStorage {
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
        }
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

    pub async fn wait_for_block(&self, block_id: &BlockId) -> Result<BlockStuffAug> {
        let block_handle_storage = &self.block_handle_storage;

        // Take an exclusive lock to prevent any block data from being stored
        let guard = self.store_block_data.write().await;

        // Try to load the block data
        if let Some(handle) = block_handle_storage.load_handle(block_id) {
            if handle.meta().has_data() {
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

    pub async fn store_block_data(
        &self,
        block: &BlockStuff,
        archive_data: &ArchiveData,
        meta_data: BlockMetaData,
    ) -> Result<StoreBlockResult> {
        // NOTE: Any amount of blocks can be stored concurrently,
        // but the subscription lock can be acquired only while
        // no block data is being stored.
        let _guard = self.store_block_data.read().await;

        let block_id = block.id();
        let (handle, status) = self
            .block_handle_storage
            .create_or_load_handle(block_id, meta_data);

        let archive_id = ArchiveEntryId::Block(block_id);
        let mut updated = false;
        if !handle.meta().has_data() {
            let data = archive_data.as_new_archive_data()?;
            metrics::histogram!("tycho_storage_store_block_data_size").record(data.len() as f64);

            let _lock = handle.block_data_lock().write().await;
            if !handle.meta().has_data() {
                self.add_data(&archive_id, data)?;
                if handle.meta().set_has_data() {
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
        if !handle.meta().has_data() {
            return Err(BlockStorageError::BlockDataNotFound.into());
        }
        self.get_data(handle, &ArchiveEntryId::Block(handle.id()))
            .await
    }

    pub async fn load_block_data_raw_ref<'a>(
        &'a self,
        handle: &'a BlockHandle,
    ) -> Result<impl AsRef<[u8]> + 'a> {
        if !handle.meta().has_data() {
            return Err(BlockStorageError::BlockDataNotFound.into());
        }
        self.get_data_ref(handle, &ArchiveEntryId::Block(handle.id()))
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
        readopts.set_iterate_lower_bound(ArchiveEntryId::Block(bound).to_vec().as_slice());
        bound.seqno += 1;
        readopts.set_iterate_upper_bound(ArchiveEntryId::Block(bound).to_vec().as_slice());

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

            let Some(ArchiveEntryIdKind::Block) = ArchiveEntryId::<()>::extract_kind(key) else {
                continue;
            };

            return Ok(Some(BocRepr::decode::<Block, _>(value)?));
        }
    }

    pub async fn store_block_proof(
        &self,
        proof: &BlockProofStuffAug,
        handle: BlockProofHandle,
    ) -> Result<StoreBlockResult> {
        let block_id = proof.id();
        if matches!(&handle, BlockProofHandle::Existing(handle) if handle.id() != block_id) {
            return Err(BlockStorageError::BlockHandleIdMismatch.into());
        }

        let (handle, status) = match handle {
            BlockProofHandle::Existing(handle) => (handle, HandleCreationStatus::Fetched),
            BlockProofHandle::New(meta_data) => self
                .block_handle_storage
                .create_or_load_handle(block_id, meta_data),
        };

        let mut updated = false;
        if proof.is_link() {
            let archive_id = ArchiveEntryId::ProofLink(block_id);
            if !handle.meta().has_proof_link() {
                let data = proof.as_new_archive_data()?;

                let _lock = handle.proof_data_lock().write().await;
                if !handle.meta().has_proof_link() {
                    self.add_data(&archive_id, data)?;
                    if handle.meta().set_has_proof_link() {
                        self.block_handle_storage.store_handle(&handle);
                        updated = true;
                    }
                }
            }
        } else {
            let archive_id = ArchiveEntryId::Proof(block_id);
            if !handle.meta().has_proof() {
                let data = proof.as_new_archive_data()?;

                let _lock = handle.proof_data_lock().write().await;
                if !handle.meta().has_proof() {
                    self.add_data(&archive_id, data)?;
                    if handle.meta().set_has_proof() {
                        self.block_handle_storage.store_handle(&handle);
                        updated = true;
                    }
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
        is_link: bool,
    ) -> Result<BlockProofStuff> {
        let raw_proof = self.load_block_proof_raw_ref(handle, is_link).await?;
        BlockProofStuff::deserialize(handle.id(), raw_proof.as_ref(), is_link)
    }

    pub async fn load_block_proof_raw(
        &self,
        handle: &BlockHandle,
        is_link: bool,
    ) -> Result<Vec<u8>> {
        let (archive_id, exists) = if is_link {
            (
                ArchiveEntryId::ProofLink(handle.id()),
                handle.meta().has_proof_link(),
            )
        } else {
            (
                ArchiveEntryId::Proof(handle.id()),
                handle.meta().has_proof(),
            )
        };

        if !exists {
            return Err(BlockStorageError::BlockProofNotFound.into());
        }

        self.get_data(handle, &archive_id).await
    }

    pub async fn load_block_proof_raw_ref<'a>(
        &'a self,
        handle: &'a BlockHandle,
        is_link: bool,
    ) -> Result<impl AsRef<[u8]> + 'a> {
        let (archive_id, exists) = if is_link {
            (
                ArchiveEntryId::ProofLink(handle.id()),
                handle.meta().has_proof_link(),
            )
        } else {
            (
                ArchiveEntryId::Proof(handle.id()),
                handle.meta().has_proof(),
            )
        };

        if !exists {
            return Err(BlockStorageError::BlockProofNotFound.into());
        }

        self.get_data_ref(handle, &archive_id).await
    }

    pub async fn commit_archive(&self, archive_id: u32) -> Result<()> {
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || {
            let data = db
                .intermediate_archives
                .get(archive_id.to_be_bytes())?
                .ok_or(BlockStorageError::ArchiveNotFound)?;

            let storage_cf = db.archives.cf();
            let intermediate_storage_cf = db.intermediate_archives.cf();

            let data_len = data.len() as u64;
            let num_chunks = (data_len + ARCHIVE_CHUNK_SIZE - 1) / ARCHIVE_CHUNK_SIZE; // Round up to get the number of chunks

            // Create transaction
            let mut batch = rocksdb::WriteBatch::default();

            // Write archive chunks
            for i in 0..num_chunks {
                let start = i * ARCHIVE_CHUNK_SIZE;
                let end = std::cmp::min(start + ARCHIVE_CHUNK_SIZE, data_len);
                let chunk = &data[start as usize..end as usize];

                let mut key = [0u8; tables::Archives::KEY_LEN];
                key[..4].copy_from_slice(&archive_id.to_be_bytes());
                key[4..].copy_from_slice(&i.to_be_bytes());

                batch.put_cf(&storage_cf, key.as_slice(), chunk);
            }

            // Write archive size
            let mut key = [0u8; tables::Archives::KEY_LEN];
            key[..4].copy_from_slice(&archive_id.to_be_bytes());
            key[4..].copy_from_slice(&ARCHIVE_SIZE_MAGIC.to_be_bytes());

            batch.put_cf(&storage_cf, key.as_slice(), data.len().to_be_bytes());

            // Remove intermediate archive
            batch.delete_cf(&intermediate_storage_cf, key);

            // Execute transaction
            db.rocksdb().write(batch)?;

            Ok(())
        })
        .await?
    }

    /// Loads data and proof for the block and appends them to the corresponding archive.
    pub async fn move_into_archive(&self, handle: &BlockHandle) -> Result<()> {
        let _histogram = HistogramGuard::begin("tycho_storage_move_into_archive_time");

        if handle.meta().is_archived() {
            return Ok(());
        }
        if !handle.meta().set_is_moving_to_archive() {
            return Ok(());
        }

        // Prepare data
        let block_id = handle.id();

        let has_data = handle.meta().has_data();
        let mut is_link = false;
        let has_proof = handle.has_proof_or_link(&mut is_link);

        let block_data = if has_data {
            let lock = handle.block_data_lock().write().await;

            let entry_id = ArchiveEntryId::Block(block_id);
            let data = self.make_archive_segment(&entry_id)?;

            Some((lock, data))
        } else {
            None
        };

        let block_proof_data = if has_proof {
            let lock = handle.proof_data_lock().write().await;

            let entry_id = if is_link {
                ArchiveEntryId::ProofLink(block_id)
            } else {
                ArchiveEntryId::Proof(block_id)
            };
            let data = self.make_archive_segment(&entry_id)?;

            Some((lock, data))
        } else {
            None
        };

        // Prepare cf
        let storage_cf = self.db.intermediate_archives.cf();
        let handle_cf = self.db.block_handles.cf();

        // Prepare archive
        let archive_id = self.compute_archive_id(handle);
        let archive_id_bytes = archive_id.id.to_be_bytes();

        // 0. Create transaction
        let mut batch = rocksdb::WriteBatch::default();
        // 1. Append archive segment with block data
        if let Some((_, data)) = &block_data {
            batch.merge_cf(&storage_cf, archive_id_bytes, data);
        }
        // 2. Append archive segment with block proof data
        if let Some((_, data)) = &block_proof_data {
            batch.merge_cf(&storage_cf, archive_id_bytes, data);
        }
        // 3. Update block handle meta
        if handle.meta().set_is_archived() {
            batch.put_cf(
                &handle_cf,
                block_id.root_hash.as_slice(),
                handle.meta().to_vec(),
            );
        }
        // 5. Execute transaction
        self.db.rocksdb().write(batch)?;

        tracing::trace!(block_id = %handle.id(), "saved block into archive");
        // Block will be removed after blocks gc

        if let (Some(prev_id), true) = (archive_id.prev_id, archive_id.is_new) {
            // commit previous archive
            self.commit_archive(prev_id).await?;
        }

        // Done
        Ok(())
    }

    /// Appends block data and proof to the corresponding archive.
    pub fn move_into_archive_with_data(
        &self,
        handle: &BlockHandle,
        is_link: bool,
        block_data: &[u8],
        block_proof_data: &[u8],
    ) -> Result<()> {
        if handle.meta().is_archived() {
            return Ok(());
        }
        if !handle.meta().set_is_moving_to_archive() {
            return Ok(());
        }

        let block_id = handle.id();

        // Prepare cf
        let archives_cf = self.db.intermediate_archives.cf();
        let block_handles_cf = self.db.block_handles.cf();

        // Prepare archive
        let archive_id = self.compute_archive_id(handle);
        let archive_id_bytes = archive_id.id.to_be_bytes();

        let mut batch = rocksdb::WriteBatch::default();

        batch.merge_cf(
            &archives_cf,
            archive_id_bytes,
            make_archive_entry(&ArchiveEntryId::Block(handle.id()).filename(), block_data),
        );

        batch.merge_cf(
            &archives_cf,
            archive_id_bytes,
            make_archive_entry(
                &if is_link {
                    ArchiveEntryId::ProofLink(block_id)
                } else {
                    ArchiveEntryId::Proof(block_id)
                }
                .filename(),
                block_proof_data,
            ),
        );

        if handle.meta().set_is_archived() {
            batch.put_cf(
                &block_handles_cf,
                block_id.root_hash.as_slice(),
                handle.meta().to_vec(),
            );
        }

        self.db.rocksdb().write(batch)?;

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

    /// Loads an archive slice (chunk).
    pub async fn get_archive_slice(&self, id: u32, offset: u64) -> Result<Vec<u8>> {
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
            mc_package_entries_removed,
            total_package_entries_removed,
            total_handles_removed,
        } = rayon_run(move || {
            let _span = span.enter();
            remove_blocks(db, max_blocks_per_batch, mc_seqno, shard_heights)
        })
        .await?;

        tracing::info!(
            total_cached_handles_removed,
            mc_package_entries_removed,
            total_package_entries_removed,
            total_handles_removed,
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

        // Remove archives
        let archives_cf = self.db.intermediate_archives.cf();
        let write_options = self.db.intermediate_archives.write_config();

        let start_key = [0u8; tables::Archives::KEY_LEN];

        let mut end_key = [0u8; tables::Archives::KEY_LEN];
        end_key[..4].copy_from_slice(&until_id.to_be_bytes());
        end_key[4..].copy_from_slice(&ARCHIVE_SIZE_MAGIC.to_be_bytes());

        self.db
            .rocksdb()
            .delete_range_cf_opt(&archives_cf, start_key, end_key, write_options)?;

        tracing::info!(archive_count = len, first, last, "finished archives GC");
        Ok(())
    }

    fn add_data<I>(&self, id: &ArchiveEntryId<I>, data: &[u8]) -> Result<(), rocksdb::Error>
    where
        I: Borrow<BlockId> + Hash,
    {
        self.db.package_entries.insert(id.to_vec(), data)
    }

    #[allow(dead_code)]
    fn has_data<I>(&self, id: &ArchiveEntryId<I>) -> Result<bool, rocksdb::Error>
    where
        I: Borrow<BlockId> + Hash,
    {
        self.db.package_entries.contains_key(id.to_vec())
    }

    async fn get_data<I>(&self, handle: &BlockHandle, id: &ArchiveEntryId<I>) -> Result<Vec<u8>>
    where
        I: Borrow<BlockId> + Hash,
    {
        let _lock = match &id {
            ArchiveEntryId::Block(_) => handle.block_data_lock().read().await,
            ArchiveEntryId::Proof(_) | ArchiveEntryId::ProofLink(_) => {
                handle.proof_data_lock().read().await
            }
        };

        match self.db.package_entries.get(id.to_vec())? {
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
        let lock = match id {
            ArchiveEntryId::Block(_) => handle.block_data_lock().read().await,
            ArchiveEntryId::Proof(_) | ArchiveEntryId::ProofLink(_) => {
                handle.proof_data_lock().read().await
            }
        };

        match self.db.package_entries.get(id.to_vec())? {
            Some(data) => Ok(BlockContentsLock { _lock: lock, data }),
            None => Err(BlockStorageError::InvalidBlockData.into()),
        }
    }

    fn prev_id(&self, mc_seqno: u32) -> Option<u32> {
        let prev_id = {
            let latest_archives = self.archive_ids.read();
            latest_archives.range(..=mc_seqno).next_back().cloned()
        };

        prev_id
    }

    fn compute_archive_id(&self, handle: &BlockHandle) -> ArchiveId {
        let mc_seqno = handle.mc_ref_seqno();

        if handle.meta().is_key_block() {
            let prev_id = self.prev_id(mc_seqno);
            self.archive_ids.write().insert(mc_seqno);
            return ArchiveId {
                id: mc_seqno,
                is_new: true,
                prev_id,
            };
        }

        let prev_id = self.prev_id(mc_seqno);

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

    fn make_archive_segment<I>(&self, entry_id: &ArchiveEntryId<I>) -> Result<Vec<u8>>
    where
        I: Borrow<BlockId> + Hash,
    {
        match self.db.package_entries.get(entry_id.to_vec())? {
            Some(data) => Ok(make_archive_entry(&entry_id.filename(), &data)),
            None => Err(BlockStorageError::InvalidBlockData.into()),
        }
    }
}

#[derive(Clone)]
pub enum BlockProofHandle {
    Existing(BlockHandle),
    New(BlockMetaData),
}

impl From<BlockHandle> for BlockProofHandle {
    fn from(handle: BlockHandle) -> Self {
        Self::Existing(handle)
    }
}

impl From<BlockMetaData> for BlockProofHandle {
    fn from(meta_data: BlockMetaData) -> Self {
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

    loop {
        let key = match blocks_iter.key() {
            Some(key) => key,
            None => break blocks_iter.status()?,
        };

        // Read only prefix with shard ident and seqno
        let BlockIdShort { shard, seqno } = BlockIdShort::from_slice(key);
        let is_masterchain = shard.is_masterchain();

        // Don't gc latest blocks
        if is_masterchain && seqno >= mc_seqno
            || !is_masterchain && shard_heights.contains_shard_seqno(&shard, seqno)
        {
            blocks_iter.next();
            continue;
        }

        // Additionally check whether this item is a key block
        if seqno == 0
            || is_masterchain
                && raw
                    .get_pinned_cf_opt(&key_blocks_cf, seqno.to_be_bytes(), &key_blocks_readopts)?
                    .is_some()
        {
            // Don't remove key blocks
            blocks_iter.next();
            continue;
        }

        // Add item to the batch
        batch.delete_cf(&package_entries_cf, key);
        stats.total_package_entries_removed += 1;
        if shard.is_masterchain() {
            stats.mc_package_entries_removed += 1;
        }

        // Key structure:
        // [workchain id, 4 bytes]
        // [shard id, 8 bytes]
        // [seqno, 4 bytes]
        // [root hash, 32 bytes] <-
        // ..
        if key.len() >= 48 {
            batch.delete_cf(&block_handles_cf, &key[16..48]);
            stats.total_handles_removed += 1;
        }

        batch_len += 1;
        if matches!(
            max_blocks_per_batch,
            Some(max_blocks_per_batch) if batch_len >= max_blocks_per_batch
        ) {
            tracing::info!(
                total_package_entries_removed = stats.total_package_entries_removed,
                "applying intermediate batch",
            );
            let batch = std::mem::take(&mut batch);
            raw.write(batch)?;
            batch_len = 0;
        }

        blocks_iter.next();
    }

    if batch_len > 0 {
        tracing::info!("applying final batch");
        raw.write(batch)?;
    }

    // Done
    Ok(stats)
}

#[derive(Debug, Copy, Clone, Default)]
pub struct BlockGcStats {
    pub mc_package_entries_removed: usize,
    pub total_package_entries_removed: usize,
    pub total_handles_removed: usize,
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

pub const ARCHIVE_PACKAGE_SIZE: u32 = 100;
pub const ARCHIVE_SIZE_MAGIC: u64 = u64::MAX;
pub const ARCHIVE_CHUNK_SIZE: u64 = 1024 * 1024; // 1MB

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
    #[error("Block handle id mismatch")]
    BlockHandleIdMismatch,
    #[error("Invalid block data")]
    InvalidBlockData,
    #[error("Offset is outside of the archive slice")]
    InvalidOffset,
}
