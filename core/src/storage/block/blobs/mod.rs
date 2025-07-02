mod task;
mod types;
mod util;
mod writer;

use std::collections::BTreeSet;
use std::num::NonZeroU32;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use cassadilia::{Cas, KeyEncoderError};
use everscale_types::boc::{Boc, BocRepr};
use everscale_types::cell::HashBytes;
use everscale_types::models::*;
use parking_lot::RwLock;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, broadcast};
use tokio::task::JoinHandle;
use tycho_block_util::archive::ArchiveEntryType;
use tycho_storage::kv::StoredValue;
use tycho_util::metrics::HistogramGuard;
pub use types::{ArchiveId, BlockGcStats, BlockStorageError, FullBlockDataGuard};
pub use util::remove_blocks;
use weedb::{ColumnFamily, OwnedPinnableSlice, rocksdb};

use self::task::CommitArchiveTask;
use super::package_entry::{
    BlockDataEntryKey, PackageEntryKey, PackageEntryKeyEncoder, PartialBlockId,
};
use crate::storage::{BlockHandle, BlockHandleStorage, CoreDb, tables};

const ARCHIVE_PACKAGE_SIZE: u32 = 100;
// Reserved key in which the archive size is stored
const ARCHIVE_SIZE_MAGIC: u64 = u64::MAX;
// Reserved key in which we store the fact that the archive must be committed
const ARCHIVE_TO_COMMIT_MAGIC: u64 = u64::MAX - 1;
// Reserved key in which we store the next archive id to override.
const ARCHIVE_OVERRIDE_NEXT_MAGIC: u64 = u64::MAX - 2;
// Reserved key in which we store the fact that archive was started
const ARCHIVE_STARTED_MAGIC: u64 = u64::MAX - 3;

const ARCHIVE_MAGIC_MIN: u64 = u64::MAX & !0xff;

const BLOCK_DATA_CHUNK_SIZE: u32 = 1024 * 1024; // 1MB

// Reserved key in which the compressed block size is stored
const BLOCK_DATA_SIZE_MAGIC: u32 = u32::MAX;
// Reserved key in which we store the fact that compressed block was started
const BLOCK_DATA_STARTED_MAGIC: u32 = u32::MAX - 2;

pub struct BlobStorage {
    db: CoreDb,
    block_handle_storage: Arc<BlockHandleStorage>,
    archive_ids: RwLock<ArchiveIds>,
    prev_archive_commit: tokio::sync::Mutex<Option<CommitArchiveTask>>,
    archive_ids_tx: ArchiveIdsTx,
    archive_chunk_size: NonZeroU32,
    split_block_semaphore: Arc<Semaphore>,

    blocks: Cas<PackageEntryKey>,
    archives: Cas<u32>,
}

impl BlobStorage {
    pub fn new(
        db: CoreDb,
        block_handle_storage: Arc<BlockHandleStorage>,
        archive_chunk_size: NonZeroU32,
        split_block_tasks: usize,
        blobdb_path: &Path,
    ) -> Result<Self> {
        let (archive_ids_tx, _) = broadcast::channel(4);
        let split_block_semaphore = Arc::new(Semaphore::new(split_block_tasks));
        let blocks = Cas::open(
            blobdb_path.join("packages"),
            PackageEntryKeyEncoder,
            cassadilia::Config::default(),
        )?;
        let archives = Cas::open(
            blobdb_path.join("archives"),
            U32Encoder,
            cassadilia::Config::default(),
        )?;

        Ok(Self {
            db,
            block_handle_storage,
            archive_ids_tx,
            archive_chunk_size,
            split_block_semaphore,
            archive_ids: Default::default(),
            prev_archive_commit: Default::default(),

            blocks,
            archives,
        })
    }

    pub fn archive_chunk_size(&self) -> NonZeroU32 {
        self.archive_chunk_size
    }

    #[expect(clippy::unused_self)]
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

    pub async fn preload_archive_ids(&self) -> Result<()> {
        let started_at = Instant::now();

        tracing::info!("started preloading archive ids");

        let db = self.db.clone();

        let (archive_ids, override_next_id, to_commit) = tokio::task::spawn_blocking(move || {
            let mut iter = db.archives.raw_iterator();
            iter.seek_to_first();

            let mut archive_ids = BTreeSet::new();
            let mut archives_to_commit = Vec::new();
            let mut override_next_id = None;
            loop {
                let Some((key, value)) = iter.item() else {
                    if let Err(e) = iter.status() {
                        tracing::error!("failed to iterate through archives: {e:?}");
                    }
                    break;
                };

                let archive_id = u32::from_be_bytes(key[..4].try_into().unwrap());
                let chunk_index = u64::from_be_bytes(key[4..].try_into().unwrap());

                const _: () = const {
                    // Rely on the specific order of these constants
                    assert!(ARCHIVE_STARTED_MAGIC < ARCHIVE_OVERRIDE_NEXT_MAGIC);
                    assert!(ARCHIVE_OVERRIDE_NEXT_MAGIC < ARCHIVE_TO_COMMIT_MAGIC);
                    assert!(ARCHIVE_TO_COMMIT_MAGIC < ARCHIVE_SIZE_MAGIC);
                };

                let mut skip = None;

                if let Some(next_id) = override_next_id {
                    // Reset override when it is not needed.
                    if archive_id > next_id {
                        override_next_id = None;
                    }
                }

                // Chunk keys are sorted by offset.
                match chunk_index {
                    // "Started" magic comes first, and indicates that the archive exists.
                    ARCHIVE_STARTED_MAGIC => {
                        archive_ids.insert(archive_id);
                    }
                    // "Override" marig comes next, and sets the next archive id if was finished earlier.
                    ARCHIVE_OVERRIDE_NEXT_MAGIC => {
                        override_next_id = Some(u32::from_le_bytes(value[..4].try_into().unwrap()));
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
                    _ => {
                        // Skip all chunks until the magic
                        if chunk_index < ARCHIVE_STARTED_MAGIC {
                            let mut next_key = [0; tables::Archives::KEY_LEN];
                            next_key[..4].copy_from_slice(&archive_id.to_be_bytes());
                            next_key[4..].copy_from_slice(&ARCHIVE_STARTED_MAGIC.to_be_bytes());
                            skip = Some(next_key);
                        }
                    }
                }

                match skip {
                    None => iter.next(),
                    Some(key) => iter.seek(key),
                }
            }

            Ok::<_, anyhow::Error>((archive_ids, override_next_id, archives_to_commit))
        })
        .await??;

        {
            let mut ids = self.archive_ids.write();
            ids.items.extend(archive_ids);
            ids.override_next_id = override_next_id;
        }

        tracing::info!(
            elapsed = %humantime::format_duration(started_at.elapsed()),
            ?override_next_id,
            "finished preloading archive ids"
        );

        for archive_id in to_commit {
            tracing::info!(archive_id, "clear partially committed archive");
            // Solves the problem of non-deterministic compression when commit archive
            // was interrupted and should be rewritten
            self.clear_archive(archive_id)?;

            tracing::info!(archive_id, "rewrite partially committed archive");
            let mut task = self.spawn_commit_archive(archive_id);
            task.finish().await?;

            // Notify archive subscribers
            self.archive_ids_tx.send(task.archive_id).ok();
        }

        Ok(())
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
        self.archive_ids.read().items.iter().cloned().collect()
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

            let Some(ArchiveEntryType::Block) = util::extract_entry_type(key) else {
                continue;
            };

            return Ok(Some(BocRepr::decode::<Block, _>(value)?));
        }
    }

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
        let chunks_cf = self.db.archives.cf();

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

        // 2. Store info that new archive was started
        if archive_id.is_new {
            let mut key = [0u8; tables::Archives::KEY_LEN];
            key[..4].copy_from_slice(&archive_id_bytes);
            key[4..].copy_from_slice(&ARCHIVE_STARTED_MAGIC.to_be_bytes());
            batch.put_cf(&chunks_cf, key, []);
        }
        // 3. Store info about overriding next archive id
        if let Some(next_id) = archive_id.override_next_id {
            let mut key = [0u8; tables::Archives::KEY_LEN];
            key[..4].copy_from_slice(&archive_id_bytes);
            key[4..].copy_from_slice(&ARCHIVE_OVERRIDE_NEXT_MAGIC.to_be_bytes());
            batch.put_cf(&chunks_cf, key, next_id.to_le_bytes());
        }
        // 4. Store info that archive commit is in progress
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
                // Wait commit archive
                task.finish().await?;

                // Notify archive subscribers
                self.archive_ids_tx.send(task.archive_id).ok();
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

    pub fn get_archive_id(&self, mc_seqno: u32) -> ArchiveId {
        let archive_ids = self.archive_ids.read();

        if !matches!(archive_ids.items.last(), Some(id) if mc_seqno < *id) {
            // Return `TooNew` if there are no archives yet, or the requested
            // seqno is greater than the beginning of the last archive. beg
            return ArchiveId::TooNew;
        }

        match archive_ids.items.range(..=mc_seqno).next_back() {
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

    pub fn subscribe_to_archive_ids(&self) -> broadcast::Receiver<u32> {
        self.archive_ids_tx.subscribe()
    }

    pub fn archive_chunks_iterator(&self, archive_id: u32) -> rocksdb::DBRawIterator<'_> {
        let mut from = [0u8; tables::Archives::KEY_LEN];
        from[..4].copy_from_slice(&archive_id.to_be_bytes());

        let mut to = [0u8; tables::Archives::KEY_LEN];
        to[..4].copy_from_slice(&archive_id.to_be_bytes());
        to[4..].copy_from_slice(&ARCHIVE_MAGIC_MIN.to_be_bytes());

        let mut read_opts = self.db.archives.new_read_config();
        read_opts.set_iterate_upper_bound(to.as_slice());

        let rocksdb = self.db.rocksdb();
        let archives_cf = self.db.archives.cf();

        let mut raw_iterator = rocksdb.raw_iterator_cf_opt(&archives_cf, read_opts);
        raw_iterator.seek(from);

        raw_iterator
    }

    pub fn remove_outdated_archives(&self, until_id: u32) -> Result<()> {
        tracing::trace!("started archives GC");

        let mut archive_ids = self.archive_ids.write();

        let retained_ids = match archive_ids
            .items
            .iter()
            .rev()
            .find(|&id| *id < until_id)
            .cloned()
        {
            // Splits `archive_ids` into two parts - [..until_id] and [until_id..]
            // `archive_ids` will now contain [..until_id]
            Some(until_id) => archive_ids.items.split_off(&until_id),
            None => {
                tracing::trace!("nothing to remove");
                return Ok(());
            }
        };
        // so we must swap maps to retain [until_id..] and get ids to remove
        let removed_ids = std::mem::replace(&mut archive_ids.items, retained_ids);

        // Print removed range bounds and compute real `until_id`
        let (Some(first), Some(last)) = (removed_ids.first(), removed_ids.last()) else {
            tracing::info!("nothing to remove");
            return Ok(());
        };

        let len = removed_ids.len();
        let until_id = match archive_ids.items.first() {
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

    pub fn add_data(&self, id: &PackageEntryKey, data: &[u8]) -> Result<(), rocksdb::Error> {
        self.db.package_entries.insert(id.to_vec(), data)
    }

    pub async fn add_block_data_and_split(&self, id: &PackageEntryKey, data: &[u8]) -> Result<()> {
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

    pub async fn get_data(
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

    pub async fn get_data_ref<'a, 'b: 'a>(
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
        let mut archive_ids = self.archive_ids.write();

        // Get the closest archive id
        let prev_id = archive_ids.items.range(..=mc_seqno).next_back().cloned();

        if force_split_archive {
            archive_ids.override_next_id = Some(mc_seqno + 1);
        } else if let Some(next_id) = archive_ids.override_next_id {
            match mc_seqno.cmp(&next_id) {
                std::cmp::Ordering::Less => {}
                std::cmp::Ordering::Equal => {
                    let is_new = archive_ids.items.insert(mc_seqno);
                    return PreparedArchiveId {
                        id: mc_seqno,
                        is_new,
                        override_next_id: None,
                        to_commit: if is_new { prev_id } else { None },
                    };
                }
                std::cmp::Ordering::Greater => {
                    archive_ids.override_next_id = None;
                }
            }
        }

        let mut archive_id = PreparedArchiveId {
            id: prev_id.unwrap_or_default(),
            override_next_id: archive_ids.override_next_id,
            ..Default::default()
        };

        let is_first_archive = prev_id.is_none();
        if is_first_archive || mc_seqno.saturating_sub(archive_id.id) >= ARCHIVE_PACKAGE_SIZE {
            let is_new = archive_ids.items.insert(mc_seqno);
            archive_id = PreparedArchiveId {
                id: mc_seqno,
                is_new,
                override_next_id: None,
                to_commit: if is_new { prev_id } else { None },
            };
        }

        // NOTE: subtraction is intentional to panic if archive_id > mc_seqno
        debug_assert!(mc_seqno - archive_id.id <= ARCHIVE_PACKAGE_SIZE);

        archive_id
    }

    fn clear_archive(&self, archive_id: u32) -> Result<()> {
        let archives_cf = self.db.archives.cf();
        let write_options = self.db.archives.write_config();

        let mut start_key = [0u8; tables::Archives::KEY_LEN];
        start_key[..4].copy_from_slice(&archive_id.to_be_bytes());
        start_key[4..].fill(0x00);

        let mut end_key = [0u8; tables::Archives::KEY_LEN];
        end_key[..4].copy_from_slice(&archive_id.to_be_bytes());
        end_key[4..].fill(0xFF);

        self.db
            .rocksdb()
            .delete_range_cf_opt(&archives_cf, start_key, end_key, write_options)?;

        Ok(())
    }

    fn spawn_commit_archive(&self, archive_id: u32) -> CommitArchiveTask {
        let db = self.db.clone();
        let block_handle_storage = self.block_handle_storage.clone();
        let chunk_size = self.archive_chunk_size().get() as u64;

        CommitArchiveTask::new(db, block_handle_storage, archive_id, chunk_size)
    }

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

    pub fn db(&self) -> &CoreDb {
        &self.db
    }
}

#[derive(Default)]
struct PreparedArchiveId {
    id: u32,
    is_new: bool,
    override_next_id: Option<u32>,
    to_commit: Option<u32>,
}

#[derive(Default)]
struct ArchiveIds {
    items: BTreeSet<u32>,
    override_next_id: Option<u32>,
}

type ArchiveIdsTx = broadcast::Sender<u32>;

struct U32Encoder;

impl cassadilia::KeyEncoder<u32> for U32Encoder {
    fn encode(&self, key: &u32) -> std::result::Result<Vec<u8>, KeyEncoderError> {
        Ok(key.to_be_bytes().to_vec())
    }

    fn decode(&self, data: &[u8]) -> std::result::Result<u32, KeyEncoderError> {
        if data.len() != 4 {
            return Err(KeyEncoderError::DecodeError);
        }

        Ok(u32::from_be_bytes(data.try_into().unwrap()))
    }
}
