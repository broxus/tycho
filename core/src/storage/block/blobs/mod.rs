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
use tycho_types::boc::BocRepr;
use tycho_types::cell::HashBytes;
use tycho_types::models::*;
use tycho_util::metrics::HistogramGuard;
pub use types::{ArchiveId, ArchiveState, BlockGcStats, BlockStorageError};
pub use util::remove_blocks;
use weedb::rocksdb;

use self::task::CommitArchiveTask;
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
    ) -> Result<Self> {
        let (archive_ids_tx, _) = broadcast::channel(4);
        let config = cassadilia::Config {
            sync_mode: cassadilia::SyncMode::Sync,
            num_ops_per_wal: 100_000,
            pre_create_cas_dirs: true, // we can pay 300ms on the first start :)
            scan_orphans_on_startup: true, // Clean-up your deads
            verify_blob_integrity: true, // Better safe than sorry
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
            .index_snapshot()
            .keys()
            .copied()
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

        let index = self.blocks.index_snapshot();

        let iter = match &continuation_key {
            Some(key) => index.range(key..),
            None => index.range(..),
        };

        let mut blocks = Vec::new();
        let mut last_key = None;

        for (key, blob_hash) in iter {
            if key.ty != ArchiveEntryType::Block {
                // Ignore non-block entries
                continue;
            }

            if blocks.len() >= LIMIT {
                last_key = Some(key.block_id.as_short_id());
                break;
            }

            // Cassadilia hash is also blake3, so we can use it directly
            let file_hash = HashBytes::from_slice(blob_hash.as_ref());
            let block_id = key.block_id.make_full(file_hash);

            blocks.push(block_id);
        }

        Ok((blocks, last_key))
    }

    pub fn list_archive_ids(&self) -> Vec<u32> {
        self.archive_ids.read().items.iter().cloned().collect()
    }

    pub fn find_mc_block_data(&self, mc_seqno: u32) -> Result<Option<Block>> {
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

        let index = self.blocks.index_snapshot();

        for (key, _blob_hash) in index.range(lower_key..upper_key) {
            if key.ty != ArchiveEntryType::Block {
                continue;
            }

            // Found a masterchain block with the requested seqno
            // Now we need to fetch the actual block data
            if let Some(compressed_data) = self.blocks.get(key)? {
                let mut decompressed = Vec::new();
                tycho_util::compression::zstd_decompress(&compressed_data, &mut decompressed)?;
                return Ok(Some(BocRepr::decode::<Block, _>(&decompressed)?));
            }
        }

        Ok(None)
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
        if let Some(size) = self.archives.size(&id)? {
            return Ok(Some(size as usize));
        }

        Ok(None)
    }

    /// Get the complete archive (compressed).
    pub async fn get_archive(&self, id: u32) -> Result<Option<Bytes>> {
        if let Some(compressed_data) = self.archives.get(&id)? {
            return Ok(Some(compressed_data));
        }
        Ok(None)
    }

    /// Get a chunk of the archive at the specified offset.
    pub async fn get_archive_chunk(&self, id: u32, offset: u64) -> Result<Bytes> {
        let chunk_size = Self::archive_chunk_size().get() as u64;
        if offset % chunk_size != 0 {
            return Err(BlockStorageError::InvalidOffset.into());
        }

        self.archives
            .get_range(&id, offset, offset + chunk_size)?
            .ok_or(BlockStorageError::ArchiveNotFound.into())
    }

    pub fn subscribe_to_archive_ids(&self) -> broadcast::Receiver<u32> {
        self.archive_ids_tx.subscribe()
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

        self.archives.remove_range(0..until_id)?;

        // Clean up archive_block_ids entries for deleted archives
        let archive_block_ids_cf = self.db.archive_block_ids.cf();
        let write_options = self.db.archive_block_ids.write_config();

        // Remove all archive_block_ids entries for archives in range [0, until_id)
        let start_key = 0u32.to_be_bytes();
        let end_key = until_id.to_be_bytes();

        self.db.rocksdb().delete_range_cf_opt(
            &archive_block_ids_cf,
            start_key,
            end_key,
            write_options,
        )?;

        tracing::info!(archive_count = len, first, last, "finished archives GC");
        Ok(())
    }

    pub async fn add_data(
        &self,
        id: &PackageEntryKey,
        data: &[u8],
        // guard is used to ensure that the caller holds a lock on the block. It's passed so we don't
        //  accidentally take recursive locks on the same block.
        _guard: &BlockDataGuard<'_>,
    ) -> Result<()> {
        let mut compressed = vec![];
        tycho_util::compression::zstd_compress(data, &mut compressed, 3);

        let mut tx = self.blocks.put(id.clone())?;
        tx.write(&compressed)?;
        tx.finish()?;
        Ok(())
    }

    pub async fn get_block_data_decompressed(
        &self,
        handle: &BlockHandle,
        id: &PackageEntryKey,
    ) -> Result<Bytes> {
        let compressed = self.get_block_data(handle, id).await?;

        let mut decompressed = Vec::new();
        tycho_util::compression::zstd_decompress(&compressed, &mut decompressed)?;

        Ok(Bytes::from(decompressed))
    }

    pub async fn get_block_data(
        &self,
        handle: &BlockHandle,
        id: &PackageEntryKey,
    ) -> Result<Bytes> {
        let _lock = match id.ty {
            ArchiveEntryType::Block => handle.block_data_lock(),
            ArchiveEntryType::Proof => handle.proof_data_lock(),
            ArchiveEntryType::QueueDiff => handle.queue_diff_data_lock(),
        }
        .read()
        .await;

        match self.blocks.get(id)? {
            Some(compressed_data) => Ok(compressed_data),
            None => Err(BlockStorageError::PackageEntryNotFound.into()),
        }
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

        match self.blocks.get_range(&id, offset, offset + length)? {
            Some(data) => Ok(Some(data)),
            None => Ok(None),
        }
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
