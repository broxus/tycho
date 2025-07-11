use std::sync::Arc;

use anyhow::Result;
use cassadilia::Cas;
use tl_proto::TlWrite;
use tokio::task::JoinHandle;
use tycho_block_util::archive::{
    ARCHIVE_ENTRY_HEADER_LEN, ARCHIVE_PREFIX, ArchiveEntryHeader, ArchiveEntryType,
};
use tycho_storage::kv::StoredValue;
use tycho_types::models::*;
use tycho_util::FastHashSet;
use tycho_util::metrics::HistogramGuard;
use tycho_util::sync::CancellationFlag;

use super::super::package_entry::PackageEntryKey;
use super::types::BlockStorageError;
use super::writer::ArchiveWriter;
use crate::storage::{BlockFlags, BlockHandleStorage, CoreDb};

pub(super) struct CommitArchiveTask {
    pub(super) archive_id: u32,
    cancelled: CancellationFlag,
    handle: Option<JoinHandle<Result<()>>>,
}

impl CommitArchiveTask {
    pub(super) fn new(
        db: CoreDb,
        block_handle_storage: Arc<BlockHandleStorage>,
        archive_id: u32,
        blocks: Cas<PackageEntryKey>,
        archives: Cas<u32>,
    ) -> Self {
        let span = tracing::Span::current();
        let cancelled = CancellationFlag::new();

        let handle = tokio::task::spawn_blocking({
            let cancelled = cancelled.clone();
            let blocks = blocks.clone();

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

                if raw_block_ids.is_empty() {
                    tracing::info!("no blocks to commit for archive {}", archive_id);
                    return Ok(());
                }

                assert_eq!(raw_block_ids.len() % BlockId::SIZE_HINT, 0);

                let mut writer = ArchiveWriter::new(&db, &archives, archive_id)?;
                let mut header_buffer = Vec::with_capacity(ARCHIVE_ENTRY_HEADER_LEN);
                let mut decompressed_buffer = vec![0; 1024 * 1024 * 100]; // Max block size is around 100MB

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
                        let Some(data) = blocks.get(&key).unwrap() else {
                            return Err(BlockStorageError::BlockDataNotFound.into());
                        };

                        tycho_util::compression::zstd_decompress(&data, &mut decompressed_buffer)?;
                        let data = &decompressed_buffer;

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

                // Done
                scopeguard::ScopeGuard::into_inner(guard);
                tracing::info!(
                    archive_id,
                    blocks = unique_ids.len(),
                    elapsed = %humantime::format_duration(histogram.finish()),
                    "Archive committed"
                );

                Ok(())
            }
        });

        Self {
            archive_id,
            cancelled,
            handle: Some(handle),
        }
    }

    pub(super) async fn finish(&mut self) -> Result<()> {
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
