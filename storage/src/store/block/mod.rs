use std::borrow::Borrow;
use std::collections::{hash_map, BTreeSet};
use std::convert::TryInto;
use std::hash::Hash;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use anyhow::{Context, Result};
use everscale_types::models::*;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use tycho_block_util::archive::{
    make_archive_entry, ArchiveData, ArchiveEntryId, ArchiveReaderError, ArchiveVerifier,
    GetFileName, WithArchiveData,
};
use tycho_block_util::block::{
    BlockProofStuff, BlockProofStuffAug, BlockStuff, BlockStuffAug, TopBlocks,
};
use tycho_util::FastHashMap;

use crate::db::*;
use crate::util::*;
use crate::{
    models::*, BlockConnection, BlockConnectionStorage, BlockHandleStorage, HandleCreationStatus,
};

pub struct BlockStorage {
    db: Arc<Db>,
    block_handle_storage: Arc<BlockHandleStorage>,
    block_connection_storage: Arc<BlockConnectionStorage>,
    archive_ids: RwLock<BTreeSet<u32>>,
    block_subscriptions: Mutex<FastHashMap<BlockId, Vec<BlockTx>>>,
    block_subscriptions_lock: tokio::sync::Mutex<()>,
}

impl BlockStorage {
    pub fn new(
        db: Arc<Db>,
        block_handle_storage: Arc<BlockHandleStorage>,
        block_connection_storage: Arc<BlockConnectionStorage>,
    ) -> Result<Self> {
        let manager = Self {
            db,
            block_handle_storage,
            block_connection_storage,
            archive_ids: Default::default(),
            block_subscriptions: Default::default(),
            block_subscriptions_lock: Default::default(),
        };

        manager.preload()?;

        Ok(manager)
    }

    fn preload(&self) -> Result<()> {
        fn check_archive(value: &[u8]) -> Result<(), ArchiveReaderError> {
            let mut verifier = ArchiveVerifier::default();
            verifier.write_verify(value)?;
            verifier.final_check()
        }

        let mut iter = self.db.archives.raw_iterator();
        iter.seek_to_first();

        let mut archive_ids = self.archive_ids.write();

        while let (Some(key), value) = (iter.key(), iter.value()) {
            let archive_id = u32::from_be_bytes(
                key.try_into()
                    .with_context(|| format!("Invalid archive key: {}", hex::encode(key)))?,
            );

            if let Some(Err(e)) = value.map(check_archive) {
                tracing::error!(archive_id, "failed to read archive: {e:?}");
            }

            archive_ids.insert(archive_id);
            iter.next();
        }

        tracing::info!("selfcheck complete");
        Ok(())
    }

    pub async fn store_block_data(
        &self,
        block: &BlockStuff,
        archive_data: &ArchiveData,
        meta_data: BlockMetaData,
    ) -> Result<StoreBlockResult> {
        let _lock = self.block_subscriptions_lock.lock().await;

        let block_id = block.id();
        let (handle, status) = self
            .block_handle_storage
            .create_or_load_handle(block_id, meta_data)?;

        let archive_id = ArchiveEntryId::Block(block_id);
        let mut updated = false;
        if !handle.meta().has_data() {
            let data = archive_data.as_new_archive_data()?;

            let _lock = handle.block_data_lock().write().await;
            if !handle.meta().has_data() {
                self.add_data(&archive_id, data)?;
                if handle.meta().set_has_data() {
                    self.block_handle_storage.store_handle(&handle)?;
                    updated = true;
                }
            }
        }

        self.block_subscriptions
            .lock()
            .retain(|block_id, subscribers| {
                if block.id() == block_id {
                    while let Some(tx) = subscribers.pop() {
                        tx.send(WithArchiveData {
                            data: block.clone(),
                            archive_data: archive_data.clone(),
                        })
                        .ok();
                    }
                    false
                } else {
                    true
                }
            });

        Ok(StoreBlockResult {
            handle,
            updated,
            new: status == HandleCreationStatus::Created,
        })
    }

    pub async fn load_block_data(&self, handle: &BlockHandle) -> Result<BlockStuff> {
        let raw_block = self.load_block_data_raw_ref(handle).await?;
        BlockStuff::deserialize(*handle.id(), raw_block.as_ref())
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
                .create_or_load_handle(block_id, meta_data)?,
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
                        self.block_handle_storage.store_handle(&handle)?;
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
                        self.block_handle_storage.store_handle(&handle)?;
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
        BlockProofStuff::deserialize(*handle.id(), raw_proof.as_ref(), is_link)
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

    pub async fn move_into_archive(&self, handle: &BlockHandle) -> Result<()> {
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
        let storage_cf = self.db.archives.cf();
        let handle_cf = self.db.block_handles.cf();

        // Prepare archive
        let archive_id = self.compute_archive_id(handle);
        let archive_id_bytes = archive_id.to_be_bytes();

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
        self.db.raw().write(batch)?;

        // Block will be removed after blocks gc

        // Done
        Ok(())
    }

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
        let archives_cf = self.db.archives.cf();
        let block_handles_cf = self.db.block_handles.cf();

        // Prepare archive
        let archive_id = self.compute_archive_id(handle);
        let archive_id_bytes = archive_id.to_be_bytes();

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

        self.db.raw().write(batch)?;

        Ok(())
    }

    pub fn get_archive_id(&self, mc_seqno: u32) -> Option<u32> {
        match self.archive_ids.read().range(..=mc_seqno).next_back() {
            // NOTE: handles case when mc_seqno is far in the future.
            // However if there is a key block between `id` and `mc_seqno`,
            // this will return an archive without that specified block.
            Some(id) if mc_seqno < id + ARCHIVE_PACKAGE_SIZE => Some(*id),
            _ => None,
        }
    }

    #[allow(unused)]
    pub fn get_archives(
        &self,
        range: impl RangeBounds<u32> + 'static,
    ) -> impl Iterator<Item = (u32, Vec<u8>)> + '_ {
        struct ArchivesIterator<'a> {
            first: bool,
            ids: (Bound<u32>, Bound<u32>),
            iter: rocksdb::DBRawIterator<'a>,
        }

        impl<'a> Iterator for ArchivesIterator<'a> {
            type Item = (u32, Vec<u8>);

            fn next(&mut self) -> Option<Self::Item> {
                if self.first {
                    match self.ids.0 {
                        Bound::Included(id) => {
                            self.iter.seek(id.to_be_bytes());
                        }
                        Bound::Excluded(id) => {
                            self.iter.seek((id + 1).to_be_bytes());
                        }
                        Bound::Unbounded => {
                            self.iter.seek_to_first();
                        }
                    }
                    self.first = false;
                } else {
                    self.iter.next();
                }

                match (self.iter.key(), self.iter.value()) {
                    (Some(key), Some(value)) => {
                        let id = u32::from_be_bytes(key.try_into().unwrap_or_default());
                        match self.ids.1 {
                            Bound::Included(bound_id) if id > bound_id => None,
                            Bound::Excluded(bound_id) if id >= bound_id => None,
                            _ => Some((id, value.to_vec())),
                        }
                    }
                    _ => None,
                }
            }
        }

        ArchivesIterator {
            first: true,
            ids: (range.start_bound().cloned(), range.end_bound().cloned()),
            iter: self.db.archives.raw_iterator(),
        }
    }

    pub fn get_archive_slice(
        &self,
        id: u32,
        offset: usize,
        limit: usize,
    ) -> Result<Option<Vec<u8>>> {
        match self.db.archives.get(id.to_be_bytes())? {
            Some(slice) if offset < slice.len() => {
                let end = std::cmp::min(offset.saturating_add(limit), slice.len());
                Ok(Some(slice[offset..end].to_vec()))
            }
            Some(_) => Err(BlockStorageError::InvalidOffset.into()),
            None => Ok(None),
        }
    }

    pub async fn remove_outdated_blocks(
        &self,
        key_block_id: &BlockId,
        max_blocks_per_batch: Option<usize>,
        gc_type: BlocksGcKind,
    ) -> Result<()> {
        let _compaction_guard = self.db.delay_compaction().await;

        // Find target block
        let target_block = match gc_type {
            BlocksGcKind::BeforePreviousKeyBlock => self
                .block_handle_storage
                .find_prev_key_block(key_block_id.seqno)?,
            BlocksGcKind::BeforePreviousPersistentState => self
                .block_handle_storage
                .find_prev_persistent_key_block(key_block_id.seqno)?,
        };

        // Load target block data
        let top_blocks = match target_block {
            Some(handle) if handle.meta().has_data() => {
                tracing::info!(
                    %key_block_id,
                    target_block_id = %handle.id(),
                    "starting blocks GC",
                );
                self.load_block_data(&handle)
                    .await
                    .context("Failed to load target key block data")
                    .and_then(|block_data| TopBlocks::from_mc_block(&block_data))
                    .context("Failed to compute top blocks for target block")?
            }
            _ => {
                tracing::info!(%key_block_id, "blocks GC skipped");
                return Ok(());
            }
        };

        // Remove all expired entries
        let total_cached_handles_removed = self.block_handle_storage.gc_handles_cache(&top_blocks);

        let db = self.db.clone();
        let BlockGcStats {
            mc_package_entries_removed,
            total_package_entries_removed,
            total_handles_removed,
        } = tokio::task::spawn_blocking(move || {
            remove_blocks(db, max_blocks_per_batch, &top_blocks)
        })
        .await??;

        tracing::info!(
            %key_block_id,
            total_cached_handles_removed,
            mc_package_entries_removed,
            total_package_entries_removed,
            total_handles_removed,
            "finished blocks GC"
        );

        // Done
        Ok(())
    }

    pub async fn remove_outdated_archives(&self, until_id: u32) -> Result<()> {
        let _compaction_guard = self.db.delay_compaction().await;

        let mut archive_ids = self.archive_ids.write();

        let retained_ids = match archive_ids.iter().rev().find(|&id| *id < until_id).cloned() {
            // Splits `archive_ids` into two parts - [..until_id] and [until_id..]
            // `archive_ids` will now contain [..until_id]
            Some(until_id) => archive_ids.split_off(&until_id),
            None => {
                tracing::info!("archives GC: nothing to remove");
                return Ok(());
            }
        };
        // so we must swap maps to retain [until_id..] and get ids to remove
        let removed_ids = std::mem::replace(&mut *archive_ids, retained_ids);

        // Print removed range bounds and compute real `until_id`
        let until_id = match (removed_ids.first(), removed_ids.last()) {
            (Some(first), Some(last)) => {
                let len = removed_ids.len();
                tracing::info!(
                    archive_count = len,
                    first,
                    last,
                    "archives GC: removing archives"
                );

                match archive_ids.first() {
                    Some(until_id) => *until_id,
                    None => *last + 1,
                }
            }
            _ => {
                tracing::info!("archives GC: nothing to remove");
                return Ok(());
            }
        };

        // Remove archives
        let archives_cf = self.db.archives.cf();
        let write_options = self.db.archives.write_config();

        self.db.raw().delete_range_cf_opt(
            &archives_cf,
            [0; 4],
            until_id.to_be_bytes(),
            write_options,
        )?;

        tracing::info!("archives GC: done");
        Ok(())
    }

    pub async fn subscribe_to_block(&self, block_id: BlockId) -> Result<BlockRx> {
        let block_handle_storage = &self.block_handle_storage;

        let (tx, rx) = tokio::sync::oneshot::channel();

        let lock = self.block_subscriptions_lock.lock().await;
        match block_handle_storage.load_handle(&block_id)? {
            Some(handle) if handle.meta().has_data() => {
                drop(lock);

                let block = self.load_block_data(&handle).await?;
                tx.send(BlockStuffAug::loaded(block)).ok();
            }
            _ => {
                self.add_block_subscription(block_id, tx);
            }
        }

        Ok(rx)
    }

    pub async fn subscribe_to_next_block(&self, prev_block_id: BlockId) -> Result<BlockRx> {
        let block_handle_storage = &self.block_handle_storage;
        let block_connection_storage = &self.block_connection_storage;

        let (tx, rx) = tokio::sync::oneshot::channel();

        let lock = self.block_subscriptions_lock.lock().await;
        let next_block_id = match block_handle_storage.load_handle(&prev_block_id)? {
            Some(handle) if handle.meta().has_next1() => {
                block_connection_storage.load_connection(&prev_block_id, BlockConnection::Next1)?
            }
            _ => {
                self.add_block_subscription(prev_block_id, tx);
                return Ok(rx);
            }
        };

        match block_handle_storage.load_handle(&next_block_id)? {
            Some(handle) if handle.meta().has_data() => {
                drop(lock);

                let block = self.load_block_data(&handle).await?;
                tx.send(BlockStuffAug::loaded(block)).ok();
            }
            _ => {
                self.add_block_subscription(prev_block_id, tx);
            }
        }

        Ok(rx)
    }

    fn add_block_subscription(&self, block_id: BlockId, tx: BlockTx) {
        let mut block_subscriptions = self.block_subscriptions.lock();
        match block_subscriptions.entry(block_id) {
            hash_map::Entry::Occupied(mut entry) => {
                let sibscribers = entry.get_mut();
                sibscribers.push(tx);
            }
            hash_map::Entry::Vacant(entry) => {
                entry.insert(vec![tx]);
            }
        }
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

    fn compute_archive_id(&self, handle: &BlockHandle) -> u32 {
        let mc_seqno = handle.masterchain_ref_seqno();

        if handle.meta().is_key_block() {
            self.archive_ids.write().insert(mc_seqno);
            return mc_seqno;
        }

        let mut archive_id = mc_seqno - mc_seqno % ARCHIVE_SLICE_SIZE;

        let prev_id = {
            let latest_archives = self.archive_ids.read();
            latest_archives.range(..=mc_seqno).next_back().cloned()
        };

        if let Some(prev_id) = prev_id {
            if archive_id < prev_id {
                archive_id = prev_id;
            }
        }

        if mc_seqno.saturating_sub(archive_id) >= ARCHIVE_PACKAGE_SIZE {
            self.archive_ids.write().insert(mc_seqno);
            archive_id = mc_seqno;
        }

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

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BlocksGcKind {
    BeforePreviousKeyBlock,
    BeforePreviousPersistentState,
}

#[derive(Clone)]
pub enum BlockProofHandle {
    Existing(Arc<BlockHandle>),
    New(BlockMetaData),
}

impl From<Arc<BlockHandle>> for BlockProofHandle {
    fn from(handle: Arc<BlockHandle>) -> Self {
        Self::Existing(handle)
    }
}

impl From<BlockMetaData> for BlockProofHandle {
    fn from(meta_data: BlockMetaData) -> Self {
        Self::New(meta_data)
    }
}

pub struct StoreBlockResult {
    pub handle: Arc<BlockHandle>,
    pub updated: bool,
    pub new: bool,
}

fn remove_blocks(
    db: Arc<Db>,
    max_blocks_per_batch: Option<usize>,
    top_blocks: &TopBlocks,
) -> Result<BlockGcStats> {
    let mut stats = BlockGcStats::default();

    let raw = db.raw().as_ref();
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
        let BlockIdShort { shard, seqno } =
            <BlockIdShort as StoredValue>::deserialize(&mut std::convert::identity(key))?;

        // Don't gc latest blocks
        if top_blocks.contains_shard_seqno(&shard, seqno) {
            blocks_iter.next();
            continue;
        }

        // Additionally check whether this item is a key block
        if seqno == 0
            || shard.is_masterchain()
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
pub const ARCHIVE_SLICE_SIZE: u32 = 20_000;

type BlockRx = tokio::sync::oneshot::Receiver<BlockStuffAug>;
type BlockTx = tokio::sync::oneshot::Sender<BlockStuffAug>;

#[derive(thiserror::Error, Debug)]
enum BlockStorageError {
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
