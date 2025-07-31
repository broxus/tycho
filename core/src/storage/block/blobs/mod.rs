//! # Archive Storage System
//!
//! This module implements a hybrid storage system for archives:
//!
//! ## Storage Architecture
//!
//! 1. **Cassadilia Storage** (Primary):
//!    - `blocks`: Individual block components (block data, proofs, queue diffs)
//!    - `archives`: Complete compressed archive packages
//!
//! 2. **`RocksDB` Metadata** (Temporary):
//!    - `archive_block_ids`: Accumulates block IDs during archive creation
//!    - Cleaned up after archive commit
//!
//! ## Archive Lifecycle
//!
//! ```
//! [Building]    -> [Committing] -> [Committed] -> [GC Eligible]
//!     |                  |              |              |
//!     v                  v              v              v
//! archive_block_ids -> Cassadilia -> ArchiveIds -> Removed
//! ```
//!
//! ## State Recovery
//!
//! On startup, the system recovers archive state by:
//! 1. Scanning Cassadilia index for committed archives
//! 2. Scanning `RocksDB` for building archives
//! 3. Resuming archive build. Incomplete archives can't be commited.
//!
//! ## Integration Points
//!
//! - **GC Subscriber**: Coordinates archive cleanup based on persistent state
//! - **Block Strider**: Calls `move_into_archive()` for each processed block
//! - **RPC Layer**: Serves archive data through `get_archive()` and related APIs

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
use bytes::Bytes;
use cassadilia::{Cas, KeyEncoderError};
use parking_lot::RwLock;
use tokio::sync::broadcast;
use tycho_block_util::archive::ArchiveEntryType;
use tycho_storage::kv::StoredValue;
use tycho_types::boc::{Boc, BocRepr};
use tycho_types::cell::HashBytes;
use tycho_types::models::*;
use tycho_util::compression::ZstdDecompress;
use tycho_util::metrics::HistogramGuard;
use weedb::rocksdb;

use self::task::CommitArchiveTask;
pub use self::types::{ArchiveId, ArchiveState, BlockGcStats, BlockStorageError};
pub use self::util::remove_blocks;
use super::package_entry::{PackageEntryKey, PackageEntryKeyEncoder, PartialBlockId};
use crate::storage::block_handle::BlockDataGuard;
use crate::storage::{BlockHandle, BlockHandleStorage, CoreDb};

const ARCHIVE_PACKAGE_SIZE: u32 = 100;

// Default archive chunk size (no longer used for actual chunking, kept for protocol compatibility)
const DEFAULT_ARCHIVE_CHUNK_SIZE: u32 = 1024 * 1024; // 1 MB

pub struct BlobStorage {
    db: CoreDb,
    block_handle_storage: Arc<BlockHandleStorage>,
    archive_ids: RwLock<ArchiveIds>,
    prev_archive_commit: tokio::sync::Mutex<Option<CommitArchiveTask>>,
    archive_ids_tx: ArchiveIdsTx,

    blocks: Cas<PackageEntryKey>,
    archives: Cas<u32>,
}

impl BlobStorage {
    pub fn new(
        db: CoreDb,
        block_handle_storage: Arc<BlockHandleStorage>,
        blobdb_path: &Path,
        pre_create_cas_dirs: bool,
    ) -> Result<Self> {
        let (archive_ids_tx, _) = broadcast::channel(4);
        let config = cassadilia::Config {
            sync_mode: cassadilia::SyncMode::Sync,
            num_ops_per_wal: 100_000,
            pre_create_cas_dirs,
            scan_orphans_on_startup: true,   // Clean-up your deads
            verify_blob_integrity: true,     // Better safe than sorry
            fail_on_integrity_errors: false, // But not to safe
        };
        let blocks = Cas::open(
            blobdb_path.join("packages"),
            PackageEntryKeyEncoder,
            config.clone(),
        )?;
        let archives = Cas::open(blobdb_path.join("archives"), ArchiveKeyEncoder, config)?;

        Ok(Self {
            db,
            block_handle_storage,
            archive_ids_tx,
            archive_ids: Default::default(),
            prev_archive_commit: Default::default(),

            blocks,
            archives,
        })
    }

    pub fn archive_chunk_size() -> NonZeroU32 {
        NonZeroU32::new(DEFAULT_ARCHIVE_CHUNK_SIZE).unwrap()
    }

    /// Recover archive state from Cassadilia index and `archive_block_ids`
    fn recover_archive_state(&self) -> Result<ArchiveState> {
        let committed_archives = self
            .archives
            .read_index_state()
            .iter()
            .map(|(id, _)| *id)
            .collect::<BTreeSet<u32>>();

        let mut building_archives = Vec::new();
        let mut iter = self.db.archive_block_ids.raw_iterator();
        iter.seek_to_first();

        while let Some((key, _)) = iter.item() {
            if key.len() >= 4 {
                let archive_id = u32::from_be_bytes(key[..4].try_into()?);
                if !committed_archives.contains(&archive_id) {
                    building_archives.push(archive_id);
                }
            }
            iter.next();
        }

        if let Err(e) = iter.status() {
            return Err(anyhow::anyhow!(
                "Failed to iterate archive_block_ids: {e:?}"
            ));
        }

        building_archives.sort();
        building_archives.dedup();

        let current_archive_id = building_archives.last().copied();
        let last_committed_id = committed_archives.iter().max().copied().unwrap_or(0);

        Ok(ArchiveState {
            committed_archives,
            building_archives,
            current_archive_id,
            last_committed_id,
        })
    }

    /// Preloads archive IDs from Cassadilia and handles incomplete archives.
    ///
    /// This method uses the new recovery function to:
    /// 1. Load all committed archives from Cassadilia
    /// 2. Detect incomplete archives in `archive_block_ids`
    /// 3. Resume or cleanup incomplete archives
    pub async fn preload_archive_ids(&self) -> Result<()> {
        let started_at = Instant::now();

        tracing::info!("started preloading archive ids");

        let state = self.recover_archive_state()?;
        let archive_count = state.committed_archives.len();

        // Update in-memory structures
        {
            let mut ids = self.archive_ids.write();
            ids.items = state.committed_archives.clone();
        }

        tracing::info!(
            elapsed = %humantime::format_duration(started_at.elapsed()),
            archive_count,
            building_count = state.building_archives.len(),
            "finished preloading archive ids"
        );

        // Handle any incomplete archives
        for archive_id in state.building_archives {
            if self.should_resume_archive(archive_id)? {
                tracing::info!(archive_id, "resuming incomplete archive");
                let mut task = self.spawn_commit_archive(archive_id);
                task.finish().await?;
                self.archive_ids_tx.send(task.archive_id).ok();
            } else {
                tracing::info!(archive_id, "cleaning up stale building archive");
                self.cleanup_archive_block_ids(archive_id)?;
            }
        }

        Ok(())
    }

    /// Determine if an archive should be resumed based on its state
    fn should_resume_archive(&self, archive_id: u32) -> Result<bool> {
        // Check if there are actually block IDs for this archive
        match self.db.archive_block_ids.get(archive_id.to_be_bytes())? {
            Some(block_ids) => Ok(!block_ids.is_empty()),
            None => Ok(false),
        }
    }

    /// Clean up `archive_block_ids` for a specific archive
    fn cleanup_archive_block_ids(&self, archive_id: u32) -> Result<()> {
        let mut batch = rocksdb::WriteBatch::default();
        let archive_block_ids_cf = self.db.archive_block_ids.cf();
        batch.delete_cf(&archive_block_ids_cf, archive_id.to_be_bytes());
        self.db.rocksdb().write(batch)?;
        Ok(())
    }

    pub async fn list_blocks(
        &self,
        continuation: Option<BlockIdShort>,
    ) -> Result<(Vec<BlockId>, Option<BlockIdShort>)> {
        const LIMIT: usize = 1000; // Max blocks per response

        let continuation_key = continuation.map(|block_id| {
            PackageEntryKey::block(&BlockId {
                shard: block_id.shard,
                seqno: block_id.seqno,
                root_hash: HashBytes::ZERO,
                file_hash: HashBytes::ZERO,
            })
        });

        let blocks = self.blocks.clone();
        tokio::task::spawn_blocking(move || {
            let index = blocks.read_index_state().keys_snapshot();

            let iter = match &continuation_key {
                Some(key) => index.range(key..),
                None => index.range(..),
            };

            let mut result = Vec::new();
            let mut last_key = None;

            for (key, _) in iter {
                if key.ty != ArchiveEntryType::Block {
                    // Ignore non-block entries
                    continue;
                }

                if result.len() >= LIMIT {
                    last_key = Some(key.block_id.as_short_id());
                    break;
                }

                // Get the compressed block data to calculate the actual file hash
                if let Some(compressed_data) = blocks.get(key)? {
                    let mut decompressed = Vec::new();
                    tycho_util::compression::ZstdDecompress::begin(&compressed_data)?
                        .decompress(&mut decompressed)?;

                    // Calculate hash of the decompressed BOC data
                    let file_hash = Boc::file_hash(&decompressed);
                    let block_id = key.block_id.make_full(file_hash);

                    result.push(block_id);
                }
            }

            Ok((result, last_key))
        })
        .await?
    }

    pub fn list_archive_ids(&self) -> Vec<u32> {
        self.archive_ids.read().items.iter().cloned().collect()
    }

    pub async fn find_mc_block_data(&self, mc_seqno: u32) -> Result<Option<Block>> {
        let lower_bound = BlockId {
            shard: ShardIdent::MASTERCHAIN,
            seqno: mc_seqno,
            root_hash: HashBytes::ZERO,
            file_hash: HashBytes::ZERO,
        };

        let upper_bound = BlockId {
            shard: ShardIdent::MASTERCHAIN,
            seqno: mc_seqno + 1,
            root_hash: HashBytes::ZERO,
            file_hash: HashBytes::ZERO,
        };

        let lower_key = PackageEntryKey::block(&lower_bound);
        let upper_key = PackageEntryKey::block(&upper_bound);

        let blocks = self.blocks.clone();
        tokio::task::spawn_blocking(move || {
            let index = blocks
                .read_index_state()
                .range(lower_key..upper_key)
                .filter_map(|(key, _)| (key.ty == ArchiveEntryType::Block).then_some(*key))
                .collect::<Vec<_>>();

            for key in index {
                if key.ty != ArchiveEntryType::Block {
                    continue;
                }

                // Found a masterchain block with the requested seqno
                // Now we need to fetch the actual block data
                if let Some(compressed_data) = blocks.get(&key)? {
                    let mut decompressed = Vec::new();
                    tycho_util::compression::ZstdDecompress::begin(&compressed_data)?
                        .decompress(&mut decompressed)?;
                    return Ok(Some(BocRepr::decode::<Block, _>(&decompressed)?));
                }
            }

            Ok(None)
        })
        .await?
    }

    /// This function is the primary way we add a block to a long-term storage archive. It's a three-step process:
    ///
    /// **Step 1: Decide which archive to use**
    /// First, we determine the correct archive for this block. This logic is based on the block's sequence number
    /// and whether it's a key block, which forces a new archive to start.
    ///
    /// **Step 2: Add the block's ID to the archive list**
    /// We record that this block belongs to its designated archive. This is done efficiently in `RocksDB`.
    /// The presence of this record marks the archive as "in progress."
    ///
    /// **Step 3: Finalize the *previous* archive (if it's ready)**
    /// When a new archive is started, it signals that the previous one is full and ready to be finalized.
    /// We then kick off an asynchronous background task to build and commit that completed archive.
    ///
    /// ## Important Guarantees
    ///
    /// **Sequential Calls are Required:** You *must* call this function sequentially. The calculation for archive IDs
    /// is stateful and relies on the previous call's state. This is currently handled by `BlockStrider`,
    /// so be cautious if calling this from anywhere else.
    ///
    /// **Atomic & Safe:**
    /// - A block's ID is safely stored in `RocksDB` *before* we attempt to build the archive file.
    /// - The final archive file is committed atomically (it's all-or-nothing).
    /// - If a commit fails, the block IDs remain in `RocksDB`, allowing us to recover and retry later.
    ///
    /// ## Error Recovery
    ///
    /// If the node crashes or something goes wrong, the block IDs will still be in `RocksDB`.
    /// On restart, a recovery process will find these incomplete archives and automatically resume
    /// the commit process.
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

        // Create transaction
        let mut batch = rocksdb::WriteBatch::default();

        // Append archive block id (this implicitly marks archive as "building")
        batch.merge_cf(&archive_block_ids_cf, archive_id_bytes, &block_id_bytes);

        // Execute transaction
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
                tracing::debug!(archive_id = task.archive_id, "committed archive");
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
        if let Some(size) = self.archives.get_size(&id)? {
            return Ok(Some(size as usize));
        }

        Ok(None)
    }

    /// Get the complete archive (compressed).
    ///
    /// Must not be used for anything other than tests.
    #[cfg(any(test, feature = "test"))]
    pub async fn get_archive_full(&self, id: u32) -> Result<Option<Bytes>> {
        let archives = self.archives.clone();
        tokio::task::spawn_blocking(move || {
            if let Some(compressed_data) = archives.get(&id)? {
                return Ok(Some(compressed_data));
            }
            Ok(None)
        })
        .await?
    }

    /// Get a chunk of the archive at the specified offset.
    pub async fn get_archive_chunk(&self, id: u32, offset: u64) -> Result<Bytes> {
        let chunk_size = Self::archive_chunk_size().get() as u64;
        if offset % chunk_size != 0 {
            return Err(BlockStorageError::InvalidOffset.into());
        }

        let archives = self.archives.clone();
        tokio::task::spawn_blocking(move || {
            archives
                .get_range(&id, offset, offset + chunk_size)?
                .ok_or(BlockStorageError::ArchiveNotFound.into())
        })
        .await?
    }

    pub fn subscribe_to_archive_ids(&self) -> broadcast::Receiver<u32> {
        self.archive_ids_tx.subscribe()
    }

    pub async fn remove_outdated_archives(&self, mut until_id: u32) -> Result<()> {
        tracing::trace!("started archives GC");

        let (len, first, last) = {
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
            let (Some(&first), Some(&last)) = (removed_ids.first(), removed_ids.last()) else {
                tracing::info!("nothing to remove");
                return Ok(());
            };

            let len = removed_ids.len();
            until_id = match archive_ids.items.first() {
                Some(until_id) => *until_id,
                None => last + 1,
            };

            (len, first, last)
        };

        let archives = self.archives.clone();
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || {
            archives.remove_range(0..until_id)?;

            // Clean up archive_block_ids entries for deleted archives
            let archive_block_ids_cf = db.archive_block_ids.cf();
            let write_options = db.archive_block_ids.write_config();

            // Remove all archive_block_ids entries for archives in range [0, until_id)
            let start_key = 0u32.to_be_bytes();
            let end_key = until_id.to_be_bytes();

            db.rocksdb().delete_range_cf_opt(
                &archive_block_ids_cf,
                start_key,
                end_key,
                write_options,
            )?;

            tracing::info!(archive_count = len, first, last, "finished archives GC");
            Ok(())
        })
        .await?
    }

    pub async fn add_data(
        &self,
        id: &PackageEntryKey,
        data: Bytes,
        // guard is used to ensure that the caller holds a lock on the block. It's passed so we don't
        //  accidentally take recursive locks on the same block.
        _guard: &BlockDataGuard<'_>,
    ) -> Result<()> {
        let id = *id;
        let blocks = self.blocks.clone();

        tokio::task::spawn_blocking(move || {
            let mut compressed = vec![];
            tycho_util::compression::zstd_compress(&data, &mut compressed, 3);
            drop(data);

            let mut tx = blocks.put(id)?;
            tx.write(&compressed)?;
            tx.finish()?;
            Ok(())
        })
        .await?
    }

    pub async fn get_block_data_decompressed(
        &self,
        handle: &BlockHandle,
        id: &PackageEntryKey,
    ) -> Result<Bytes> {
        let _lock = lock_block_handle(handle, id.ty).await;

        let id = *id;
        let blocks = self.blocks.clone();
        tokio::task::spawn_blocking(move || match blocks.get(&id)? {
            Some(compressed_data) => {
                let mut output = Vec::new();
                ZstdDecompress::begin(&compressed_data)?.decompress(&mut output)?;
                Ok(Bytes::from(output))
            }
            None => Err(BlockStorageError::PackageEntryNotFound.into()),
        })
        .await?
    }

    pub async fn get_block_data_range(
        &self,
        handle: &BlockHandle,
        offset: u64,
        length: u64,
    ) -> Result<Option<Bytes>> {
        let id = PackageEntryKey {
            block_id: PartialBlockId::from(handle.id()),
            ty: ArchiveEntryType::Block,
        };

        let blocks = self.blocks.clone();
        tokio::task::spawn_blocking(move || {
            match blocks.get_range(&id, offset, offset + length)? {
                Some(data) => Ok(Some(data)),
                None => Ok(None),
            }
        })
        .await?
    }

    pub(super) fn blocks(&self) -> &Cas<PackageEntryKey> {
        &self.blocks
    }

    fn prepare_archive_id(&self, mc_seqno: u32, force_split_archive: bool) -> PreparedArchiveId {
        let mut archive_ids = self.archive_ids.write();

        // Handle force split by setting override for next block
        if force_split_archive {
            archive_ids.override_next_id = Some(mc_seqno + 1);
        } else if let Some(next_id) = archive_ids.override_next_id {
            // Check if we've reached the override point
            match mc_seqno.cmp(&next_id) {
                std::cmp::Ordering::Less => {
                    // Not yet at override point, continue with current logic
                }
                std::cmp::Ordering::Equal => {
                    // Start new archive at override point
                    let is_new = archive_ids.items.insert(mc_seqno);
                    archive_ids.override_next_id = None;
                    let prev_id = archive_ids.items.range(..mc_seqno).next_back().cloned();
                    return PreparedArchiveId {
                        id: mc_seqno,
                        to_commit: if is_new { prev_id } else { None },
                    };
                }
                std::cmp::Ordering::Greater => {
                    // Passed override point, clear it
                    archive_ids.override_next_id = None;
                }
            }
        }

        // Get the closest archive id
        let prev_id = archive_ids.items.range(..=mc_seqno).next_back().cloned();

        let mut archive_id = PreparedArchiveId {
            id: prev_id.unwrap_or_default(),
            to_commit: None,
        };

        let is_first_archive = prev_id.is_none();
        if is_first_archive || mc_seqno.saturating_sub(archive_id.id) >= ARCHIVE_PACKAGE_SIZE {
            let is_new = archive_ids.items.insert(mc_seqno);
            archive_id = PreparedArchiveId {
                id: mc_seqno,
                to_commit: if is_new { prev_id } else { None },
            };
        }

        // NOTE: subtraction is intentional to panic if archive_id > mc_seqno
        debug_assert!(mc_seqno - archive_id.id <= ARCHIVE_PACKAGE_SIZE);

        archive_id
    }

    fn spawn_commit_archive(&self, archive_id: u32) -> CommitArchiveTask {
        let db = self.db.clone();
        let block_handle_storage = self.block_handle_storage.clone();
        let blocks = self.blocks.clone();
        let archives = self.archives.clone();

        CommitArchiveTask::new(db, block_handle_storage, archive_id, blocks, archives)
    }

    pub fn db(&self) -> &CoreDb {
        &self.db
    }
}

async fn lock_block_handle(handle: &BlockHandle, ty: ArchiveEntryType) -> BlockDataGuard<'_> {
    match ty {
        ArchiveEntryType::Block => handle.block_data_lock(),
        ArchiveEntryType::Proof => handle.proof_data_lock(),
        ArchiveEntryType::QueueDiff => handle.queue_diff_data_lock(),
    }
    .read()
    .await
}

#[derive(Default)]
struct PreparedArchiveId {
    id: u32,
    to_commit: Option<u32>,
}

#[derive(Default)]
struct ArchiveIds {
    items: BTreeSet<u32>,
    override_next_id: Option<u32>,
}

type ArchiveIdsTx = broadcast::Sender<u32>;

struct ArchiveKeyEncoder;

impl cassadilia::KeyEncoder<u32> for ArchiveKeyEncoder {
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

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use tempfile::TempDir;
    use tycho_block_util::archive::ArchiveReader;
    use tycho_types::models::ShardIdent;
    use tycho_types::prelude::HashBytes;

    use super::*;
    use crate::storage::block::package_entry::PackageEntryKeyEncoder;
    use crate::storage::{BlockFlags, BlockHandle, NewBlockMeta};

    const TEST_BLOCK_DATA: &[u8] = b"test block data";
    const TEST_PROOF_DATA: &[u8] = b"test proof data";
    const TEST_QUEUE_DIFF_DATA: &[u8] = b"test queue diff data";

    pub fn create_test_block_id(seqno: u32) -> BlockId {
        BlockId {
            shard: ShardIdent::MASTERCHAIN,
            seqno,
            root_hash: HashBytes::ZERO,
            file_hash: HashBytes::ZERO,
        }
    }

    async fn create_temp_db_and_handles() -> Result<(CoreDb, Arc<BlockHandleStorage>, TempDir)> {
        use tycho_storage::StorageContext;

        let (ctx, temp_dir) = StorageContext::new_temp().await?;
        let db: CoreDb = ctx.open_preconfigured("db")?;
        let handles = Arc::new(BlockHandleStorage::new(db.clone()));
        Ok((db, handles, temp_dir))
    }

    pub async fn create_test_storage() -> Result<(BlobStorage, TempDir)> {
        let (db, handles, temp_dir) = create_temp_db_and_handles().await?;
        let storage = BlobStorage::new(db, handles, temp_dir.path(), false)?;
        Ok((storage, temp_dir))
    }

    pub async fn create_test_storage_components() -> Result<(
        CoreDb,
        Arc<BlockHandleStorage>,
        Cas<PackageEntryKey>,
        Cas<u32>,
        TempDir,
    )> {
        let (db, handles, temp_dir) = create_temp_db_and_handles().await?;
        let blocks = Cas::<PackageEntryKey>::open(
            temp_dir.path().join("packages"),
            PackageEntryKeyEncoder,
            cassadilia::Config::default(),
        )?;
        let archives = Cas::<u32>::open(
            temp_dir.path().join("archives"),
            ArchiveKeyEncoder,
            cassadilia::Config::default(),
        )?;
        Ok((db, handles, blocks, archives, temp_dir))
    }

    pub async fn store_block_data(
        blocks: &Cas<PackageEntryKey>,
        key: PackageEntryKey,
        data: &[u8],
    ) -> Result<()> {
        let compressed = tycho_util::compression::zstd_compress_simple(data);
        let mut tx = blocks.put(key)?;
        tx.write(&compressed)?;
        tx.finish()?;
        Ok(())
    }

    pub fn create_handle_with_flags(
        block_id: BlockId,
        flags: BlockFlags,
        handles: &BlockHandleStorage,
    ) -> BlockHandle {
        let (handle, _) = handles.create_or_load_handle(&block_id, NewBlockMeta {
            is_key_block: false,
            gen_utime: 1000000,
            ref_by_mc_seqno: block_id.seqno,
        });
        handle.meta().add_flags(flags);
        handle
    }

    async fn store_all_block_parts(storage: &BlobStorage, block_id: &BlockId) -> Result<()> {
        for (entry_type, data) in [
            (ArchiveEntryType::Block, TEST_BLOCK_DATA),
            (ArchiveEntryType::Proof, TEST_PROOF_DATA),
            (ArchiveEntryType::QueueDiff, TEST_QUEUE_DIFF_DATA),
        ] {
            store_block_data(
                &storage.blocks,
                PackageEntryKey::from((*block_id, entry_type)),
                data,
            )
            .await?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_archive_override_mechanism() -> Result<()> {
        let (storage, _temp_dir) = create_test_storage().await?;

        let check = |id: u32, force_split: bool, expected_id: u32, expected_commit: Option<u32>| {
            let result = storage.prepare_archive_id(id, force_split);
            assert_eq!(
                (result.id, result.to_commit),
                (expected_id, expected_commit)
            );
        };

        check(10, true, 10, None);
        check(5, false, 5, None);
        check(8, false, 5, None);
        check(10, false, 10, None);
        check(11, false, 11, Some(10));
        check(12, false, 11, None);

        Ok(())
    }

    #[tokio::test]
    async fn test_should_resume() -> Result<()> {
        let (storage, _temp_dir) = create_test_storage().await?;

        storage
            .db
            .archive_block_ids
            .insert(1u32.to_be_bytes(), create_test_block_id(100).to_vec())?;
        storage
            .db
            .archive_block_ids
            .insert(2u32.to_be_bytes(), vec![])?;

        assert!(storage.should_resume_archive(1)?);
        assert!(!storage.should_resume_archive(2)?);
        assert!(!storage.should_resume_archive(999)?);

        Ok(())
    }

    #[tokio::test]
    async fn out_of_order() -> Result<()> {
        let (storage, _temp_dir) = create_test_storage().await?;
        tycho_util::test::init_logger("ordered_archive_saving", "debug,cassadilia=info");
        let storage = Arc::new(storage);

        let mut ids = HashSet::new();

        #[derive(Debug, Clone, Copy)]
        enum BlockDestination {
            Archive1,
            Archive2,
            TriggerSplit, // This block goes to Archive1 but triggers creation of Archive2
        }

        async fn save_block(
            id: u32,
            storage: &BlobStorage,
            set: &mut HashSet<(ArchiveEntryType, BlockId)>,
            destination: BlockDestination,
        ) -> Result<()> {
            let block_id = create_test_block_id(id);
            let handle = create_handle_with_flags(
                block_id,
                BlockFlags::empty(),
                &storage.block_handle_storage,
            );

            // Store all parts for block
            store_all_block_parts(storage, &block_id).await?;

            handle.meta().add_flags(BlockFlags::HAS_ALL_BLOCK_PARTS);
            storage.block_handle_storage.store_handle(&handle, true);

            let force_split = matches!(destination, BlockDestination::TriggerSplit);
            storage.move_into_archive(&handle, force_split).await?;

            // Add to expected set based on which archive this block actually goes into
            if matches!(
                destination,
                BlockDestination::Archive1 | BlockDestination::TriggerSplit
            ) {
                set.insert((ArchiveEntryType::Block, block_id));
                set.insert((ArchiveEntryType::Proof, block_id));
                set.insert((ArchiveEntryType::QueueDiff, block_id));
            }

            Ok(())
        }

        save_block(1, &storage, &mut ids, BlockDestination::Archive1).await?;

        // --- archive blocks 9, 8, 7, ..., 2
        for i in (2..10).rev() {
            save_block(i, &storage, &mut ids, BlockDestination::Archive1).await?;
        }

        save_block(11, &storage, &mut ids, BlockDestination::TriggerSplit).await?;

        // Block 11 sets up the next block to start a new archive
        // So we need block 12 to actually trigger the commit of archive 1
        save_block(12, &storage, &mut ids, BlockDestination::Archive2).await?;

        storage.wait_for_archive_commit().await?;

        let r = storage.db.archive_block_ids.get(1u32.to_be_bytes())?;
        assert!(r.is_none(), "Should be removed after archive commit");

        let archive = storage
            .get_archive_full(1)
            .await?
            .expect("Archive 1 should exist");

        let decompressed = tycho_util::compression::zstd_decompress_simple(&archive)?;
        let archive = ArchiveReader::new(&decompressed)?;

        for entry in archive {
            let entry = entry?;
            assert!(
                ids.remove(&(entry.ty, entry.block_id)),
                "duplicate or something"
            );
        }

        assert!(ids.is_empty(), "Not all entries were found in the archive");

        // Check that archive 2 was started
        storage
            .db
            .archive_block_ids
            .get(12u32.to_be_bytes())?
            .expect("Ids should exist");

        Ok(())
    }
}
