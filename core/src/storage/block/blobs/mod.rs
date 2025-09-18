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
//! ```text
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
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use bytes::Bytes;
use cassadilia::Cas;
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
pub use self::types::{ArchiveId, BlockGcStats, BlockStorageError, OpenStats};
pub use self::util::remove_blocks;
use super::package_entry::{PackageEntryKey, PartialBlockId};
use crate::storage::block_handle::BlockDataGuard;
use crate::storage::{BlockFlags, BlockHandle, BlockHandleStorage, BlockMeta, CoreDb, tables};

const ARCHIVE_PACKAGE_SIZE: u32 = 100;

// Default archive chunk size (no longer used for actual chunking, kept for protocol compatibility)
pub(super) const DEFAULT_CHUNK_SIZE: u64 = 1024 * 1024; // 1 MB

pub struct BlobStorage {
    db: CoreDb,
    block_handle_storage: Arc<BlockHandleStorage>,
    archive_ids: RwLock<ArchiveIds>,
    prev_archive_commit: tokio::sync::Mutex<Option<CommitArchiveTask>>,
    archive_ids_tx: ArchiveIdsTx,
    open_stats: OpenStats,

    blocks: Cas<PackageEntryKey>,
    archives: Cas<u32>,
}

impl BlobStorage {
    pub async fn new(
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
        let blocks = Cas::open(blobdb_path.join("packages"), config.clone())?;
        let archives = Cas::open(blobdb_path.join("archives"), config)?;

        let storage = Self {
            db,
            block_handle_storage,
            archive_ids_tx,
            archive_ids: Default::default(),
            prev_archive_commit: Default::default(),
            open_stats: Default::default(),

            blocks,
            archives,
        };

        let storage = storage
            .sync_state_after_init()
            .await
            .context("failed to sync blob storage state on init")?;

        Ok(storage)
    }

    /// Ensures consistency between data stored in `RocksDB` and `CAS`.
    ///
    /// For archives we sync events and check `CAS` to contain all committed data:
    /// - Add all "started" archives to the runtime map.
    /// - Compute the latest "override next" id.
    /// - Find all archives that are needed to commit.
    /// - Ensure that `CAS` stored these committed archives.
    ///
    /// For block metadata handles both cases:
    /// - Blob is absent but the flag is still set (removes flag). In case of interrupted gc.
    /// - Blob exists but the flag is not set (restores flag). In case of interrupted block save.
    async fn sync_state_after_init(mut self) -> Result<Self> {
        let started_at = Instant::now();
        tracing::info!("started blob storage sync");

        let (this, to_commit) = tokio::task::spawn_blocking(move || {
            // Collect committed archive ids.
            let committed_archives = self
                .archives
                .read_index_state()
                .iter()
                .map(|(id, _)| *id)
                .collect::<BTreeSet<u32>>();

            // Process archive events.
            let mut iter = self.db.archive_events.raw_iterator();
            iter.seek_to_first();

            let mut archive_ids = BTreeSet::new();
            let mut archives_to_commit = Vec::new();
            let mut override_next_id = None;
            while let Some((key, value)) = iter.item() {
                let archive_id = u32::from_be_bytes(key[..4].try_into().unwrap());
                let event = u32::from_be_bytes(key[4..].try_into().unwrap());

                const _: () = const {
                    // Rely on the specific order of these constants
                    assert!(ARCHIVE_EVENT_STARTED < ARCHIVE_EVENT_OVERRIDE_NEXT);
                    assert!(ARCHIVE_EVENT_OVERRIDE_NEXT < ARCHIVE_EVENT_TO_COMMIT);
                };

                if let Some(next_id) = override_next_id {
                    // Reset override when it is not needed.
                    if archive_id > next_id {
                        override_next_id = None;
                    }
                }

                // Chunk keys are sorted by offset.
                match event {
                    // "Started" event comes first, and indicates that the archive exists.
                    ARCHIVE_EVENT_STARTED => {
                        archive_ids.insert(archive_id);
                    }
                    // "Override" event comes next, and sets the next archive id if was finished earlier.
                    ARCHIVE_EVENT_OVERRIDE_NEXT => {
                        override_next_id = Some(u32::from_le_bytes(value[..4].try_into().unwrap()));
                    }
                    // "To commit" event comes next, commit should have been started.
                    ARCHIVE_EVENT_TO_COMMIT => {
                        anyhow::ensure!(
                            archive_ids.contains(&archive_id),
                            "invalid archive TO_COMMIT entry"
                        );
                        archives_to_commit.push(archive_id);
                    }
                    // "Committed" event indicates that the archive must be stored in CAS.
                    ARCHIVE_EVENT_COMMITTED => {
                        // Last archive is already committed
                        let last = archives_to_commit.pop();
                        anyhow::ensure!(
                            last == Some(archive_id),
                            "invalid archive COMMITTED entry"
                        );

                        // Require only contiguous uncommited archives list
                        anyhow::ensure!(archives_to_commit.is_empty(), "skipped archive commit");

                        // Require CAS to contain the committed archive.
                        anyhow::ensure!(
                            committed_archives.contains(&archive_id),
                            "archive has COMMITTED event but is not present in CAS, \
                            archive_id={archive_id}"
                        );
                    }
                    // Log unknown events.
                    _ => tracing::warn!(archive_id, event, "skipping unknown archive event"),
                }

                iter.next();
            }
            iter.status()?;
            drop(iter);

            let archive_count = archive_ids.len();
            let archive_min_id = archive_ids.first().copied();
            let archive_max_id = archive_ids.last().copied();
            {
                let mut ids = self.archive_ids.write();
                anyhow::ensure!(ids.items.is_empty(), "invalid initial blob storage state");
                ids.items.extend(archive_ids);
                ids.override_next_id = override_next_id;
            }

            tracing::info!(
                archive_count,
                archive_min_id,
                archive_max_id,
                ?override_next_id,
                ?archives_to_commit,
                elapsed = %humantime::format_duration(started_at.elapsed()),
                "preloaded archive ids"
            );

            // Sync block ids.
            let started_at = Instant::now();
            let blocks_index = self.blocks.read_index_state();
            let package_entries_count = blocks_index.len();

            let mut batch = rocksdb::WriteBatch::default();
            let mut orphaned_flags_count = 0u32;
            let mut restored_flags_count = 0u32;

            let raw = self.db.rocksdb().as_ref();
            let block_handles_cf = self.db.block_handles.cf();
            let mut iter = raw.raw_iterator_cf_opt(
                &self.db.full_block_ids.cf(),
                self.db.full_block_ids.new_read_config(),
            );
            iter.seek_to_first();

            while let Some((key, _)) = iter.item() {
                let block_id = PartialBlockId::from_slice(key);
                let root_hash = *block_id.root_hash.as_array();

                // Get metadata
                let meta_bytes = self
                    .db
                    .block_handles
                    .get(root_hash)?
                    .expect("all 3 tables are written in 1 transaction");

                let calculated = BlockMeta::deserialize(&mut meta_bytes.as_ref());
                let before_flags = calculated.flags();

                // Always recompute flags from actual stored data
                for (ty, flag) in [
                    (ArchiveEntryType::Block, BlockFlags::HAS_DATA),
                    (ArchiveEntryType::Proof, BlockFlags::HAS_PROOF),
                    (ArchiveEntryType::QueueDiff, BlockFlags::HAS_QUEUE_DIFF),
                ] {
                    let key = PackageEntryKey { block_id, ty };

                    // in cas, not in flags
                    if blocks_index.contains_key(&key) {
                        calculated.add_flags(flag);
                    } else {
                        calculated.remove_flags(flag);
                    }
                }

                // If changed, save and update counters
                if calculated.flags() != before_flags {
                    batch.put_cf(&block_handles_cf, root_hash, calculated.to_vec());
                    let removed_flags = before_flags - calculated.flags();
                    let added_flags = calculated.flags() - before_flags;

                    tracing::debug!(
                        ?block_id,
                        ?added_flags,
                        ?removed_flags,
                        "Correcting block metadata inconsistency"
                    );

                    if removed_flags.intersects(BlockFlags::HAS_ALL_BLOCK_PARTS) {
                        orphaned_flags_count += 1;
                    }
                    if added_flags.intersects(BlockFlags::HAS_ALL_BLOCK_PARTS) {
                        restored_flags_count += 1;
                    }
                }

                iter.next();
            }
            iter.status()?;
            drop(iter);

            raw.write(batch)?;

            drop(blocks_index);

            tracing::info!(
                package_entries_count,
                orphaned_flags_count,
                restored_flags_count,
                elapsed = %humantime::format_duration(started_at.elapsed()),
                "updated block metadata flags"
            );

            self.open_stats = OpenStats {
                orphaned_flags_count,
                restored_flags_count,
                archive_count,
                archive_min_id,
                archive_max_id,
                package_entries_count,
            };

            Ok::<_, anyhow::Error>((self, archives_to_commit))
        })
        .await??;

        for archive_id in to_commit {
            tracing::info!(archive_id, "commit archive");
            let mut task = this.spawn_commit_archive(archive_id);
            task.finish().await?;

            // Notify archive subscribers
            this.archive_ids_tx.send(task.archive_id).ok();
        }

        tracing::info!(
            elapsed = %humantime::format_duration(started_at.elapsed()),
            "blob storage sync finished"
        );

        Ok(this)
    }

    pub fn open_stats(&self) -> &OpenStats {
        &self.open_stats
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
    #[tracing::instrument(
        skip(self, handle),
        fields(
            block_id = %handle.id(),
            mc_seqno = handle.ref_by_mc_seqno(),
            mc_is_key_block = mc_is_key_block,
        )
    )]
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
        let archive_events_cf = self.db.archive_events.cf();

        // Prepare archive
        let archive_id = self.prepare_archive_id(
            handle.ref_by_mc_seqno(),
            mc_is_key_block || handle.is_key_block(),
        );
        tracing::debug!(
            "archive_id for {} is {:?}",
            handle.id().as_short_id(),
            &archive_id
        );
        let archive_id_bytes = archive_id.id.to_be_bytes();

        // 0. Create transaction
        let mut batch = rocksdb::WriteBatch::default();
        // 1. Append archive block id.
        batch.merge_cf(&archive_block_ids_cf, archive_id_bytes, &block_id_bytes);
        // 2. Store info that new archive was started
        if archive_id.is_new {
            let mut key = [0u8; tables::ArchiveEvents::KEY_LEN];
            key[..4].copy_from_slice(&archive_id_bytes);
            key[4..].copy_from_slice(&ARCHIVE_EVENT_STARTED.to_be_bytes());
            batch.put_cf(&archive_events_cf, key, []);
        }
        // 3. Store info about overriding next archive id
        if let Some(next_id) = archive_id.override_next_id {
            let mut key = [0u8; tables::ArchiveEvents::KEY_LEN];
            key[..4].copy_from_slice(&archive_id_bytes);
            key[4..].copy_from_slice(&ARCHIVE_EVENT_OVERRIDE_NEXT.to_be_bytes());
            batch.put_cf(&archive_events_cf, key, next_id.to_le_bytes());
        }
        // 4. Store info that we should start committing PREVIOUS archive.
        if let Some(to_commit) = archive_id.to_commit {
            let mut key = [0u8; tables::ArchiveEvents::KEY_LEN];
            key[..4].copy_from_slice(&to_commit.to_be_bytes());
            key[4..].copy_from_slice(&ARCHIVE_EVENT_TO_COMMIT.to_be_bytes());
            batch.put_cf(&archive_events_cf, key, []);
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
        anyhow::ensure!(
            offset.is_multiple_of(DEFAULT_CHUNK_SIZE),
            BlockStorageError::InvalidOffset {
                offset,
                chunk_size: DEFAULT_CHUNK_SIZE,
            }
        );

        let archives = self.archives.clone();
        tokio::task::spawn_blocking(move || {
            archives
                .get_range(&id, offset, offset + DEFAULT_CHUNK_SIZE)?
                .ok_or_else(|| BlockStorageError::ArchiveNotFound(id).into())
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

            let retained_ids = match archive_ids.items.range(..until_id).next_back().cloned() {
                // Splits `archive_ids` into two parts, `archive_ids` will now contain `..until_id`.
                Some(until_id) => archive_ids.items.split_off(&until_id),
                None => {
                    tracing::trace!("nothing to remove");
                    return Ok(());
                }
            };
            // so we must swap maps to retain [until_id..) and get ids to remove
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
            // Clean up events first.
            let archive_events_cf = db.archive_events.cf();
            let write_options = db.archive_block_ids.write_config();

            let start_key = [0u8; tables::ArchiveEvents::KEY_LEN];
            // NOTE: End key points to the first entry of the `until_id` archive,
            // because `delete_range` removes all entries in range ["from", "to").
            let mut end_key = [0u8; tables::ArchiveEvents::KEY_LEN];
            end_key[..4].copy_from_slice(&until_id.to_be_bytes());

            db.rocksdb().delete_range_cf_opt(
                &archive_events_cf,
                start_key,
                end_key,
                write_options,
            )?;

            // Only after removing events, remove committed archives.
            archives.remove_range(0..until_id)?;

            tracing::info!(archive_count = len, first, last, "finished archives GC");
            Ok(())
        })
        .await?
    }

    pub(crate) async fn add_data(
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
        // NOTE: Lock here can be dropped if the future is cancelled.
        //       There is no need for exclusive read.
        let _lock = lock_block_handle(handle, id.ty).await;

        let id = *id;
        let blocks = self.blocks.clone();
        tokio::task::spawn_blocking(move || match blocks.get(&id)? {
            Some(compressed_data) => {
                let mut output = Vec::new();
                ZstdDecompress::begin(&compressed_data)?.decompress(&mut output)?;
                Ok(Bytes::from(output))
            }
            None => Err(
                BlockStorageError::PackageEntryNotFound(id.block_id.as_short_id(), id.ty).into(),
            ),
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

    // yay integration tests
    #[cfg(any(test, feature = "test"))]
    pub fn blocks_for_test(&self) -> &Cas<PackageEntryKey> {
        &self.blocks
    }

    fn prepare_archive_id(&self, mc_seqno: u32, force_split_archive: bool) -> PreparedArchiveId {
        let mut archive_ids = self.archive_ids.write();

        // Get the closest archive id
        let prev_id = archive_ids.items.range(..=mc_seqno).next_back().cloned();

        if force_split_archive {
            archive_ids.override_next_id = Some(mc_seqno + 1);
        } else if let Some(next_id) = archive_ids.override_next_id {
            // Check if we've reached the override point
            match mc_seqno.cmp(&next_id) {
                std::cmp::Ordering::Less => {}
                std::cmp::Ordering::Equal => {
                    // Start new archive at override point
                    let is_new = archive_ids.items.insert(mc_seqno);
                    return PreparedArchiveId {
                        id: mc_seqno,
                        is_new,
                        override_next_id: None,
                        to_commit: if is_new { prev_id } else { None },
                    };
                }
                std::cmp::Ordering::Greater => {
                    // Passed override point, clear it
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

#[derive(Default, Debug)]
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

/// New archive was started (does not imply that is was committed yet).
const ARCHIVE_EVENT_STARTED: u32 = 100;
/// Force split current archive.
///
/// Next archive id must be stored as event data (u32 LE).
const ARCHIVE_EVENT_OVERRIDE_NEXT: u32 = 200;
/// Archive commit should have started.
const ARCHIVE_EVENT_TO_COMMIT: u32 = 300;
/// Archive was committed to CAS.
const ARCHIVE_EVENT_COMMITTED: u32 = 400;

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use tempfile::TempDir;
    use tycho_block_util::archive::ArchiveReader;
    use tycho_types::models::ShardIdent;
    use tycho_types::prelude::HashBytes;

    use super::*;
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
        let storage = BlobStorage::new(db, handles, temp_dir.path(), false).await?;
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
            cassadilia::Config::default(),
        )?;
        let archives = Cas::<u32>::open(
            temp_dir.path().join("archives"),
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
