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
    #[tracing::instrument(skip_all, name = "commit_archive", fields(archive_id = %archive_id))]
    pub(super) fn new(
        db: CoreDb,
        block_handle_storage: Arc<BlockHandleStorage>,
        archive_id: u32,
        blocks: Cas<PackageEntryKey>,
        archives: Cas<u32>,
    ) -> Self {
        let cancelled = CancellationFlag::new();
        let span = tracing::Span::current();

        if archives.read_index_state().contains_key(&archive_id) {
            tracing::warn!("attempted to commit an already committed archive, skipping");
            return Self {
                archive_id,
                cancelled,
                handle: None,
            };
        }

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
                anyhow::ensure!(
                    !raw_block_ids.is_empty(),
                    "cannot commit empty archive, archive_id={archive_id}"
                );

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

                        tycho_util::compression::ZstdDecompress::begin(&data)?
                            .decompress(&mut decompressed_buffer)?;
                        let data = decompressed_buffer.as_slice();

                        anyhow::ensure!(!cancelled.check(), "task aborted");

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
                        writer.write(data)?;
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
            let result = handle
                .await
                .map_err(|e| {
                    if e.is_panic() {
                        std::panic::resume_unwind(e.into_panic());
                    }
                    anyhow::Error::from(e)
                })
                .and_then(std::convert::identity);

            self.handle = None;

            if let Err(e) = &result {
                tracing::error!(
                    archive_id = self.archive_id,
                    "failed to commit archive: {e:?}"
                );
            }

            return result;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::block::blobs::test::{
        create_handle_with_flags, create_test_block_id, create_test_storage_components,
        store_block_data,
    };

    #[tokio::test]
    async fn test_commit_task_block_completeness_validation() -> Result<()> {
        let (db, handles, blocks, archives, _temp_dir) = create_test_storage_components().await?;
        let archive_id = 1u32;
        let block_id = create_test_block_id(100);

        handles.store_handle(
            &create_handle_with_flags(
                block_id,
                BlockFlags::HAS_DATA | BlockFlags::HAS_QUEUE_DIFF, // missing HAS_PROOF
                &handles,
            ),
            true,
        );

        db.archive_block_ids
            .insert(archive_id.to_be_bytes(), block_id.to_vec())?;
        store_block_data(
            &blocks,
            PackageEntryKey::from((block_id, ArchiveEntryType::Block)),
            b"test",
        )
        .await?;

        let mut task = CommitArchiveTask::new(db, handles, archive_id, blocks, archives);
        let err = task.finish().await.unwrap_err().to_string();

        assert!(err.contains("does not have all parts"));
        assert!(err.contains("has_proof=false"));

        Ok(())
    }

    #[tokio::test]
    async fn cannot_commit_empty_archive() -> Result<()> {
        let (db, handles, blocks, archives, _temp_dir) = create_test_storage_components().await?;
        let archive_id = 3u32;

        db.archive_block_ids
            .insert(archive_id.to_be_bytes(), vec![])?;
        CommitArchiveTask::new(db, handles, archive_id, blocks, archives)
            .finish()
            .await
            .unwrap_err();

        Ok(())
    }

    #[tokio::test]
    async fn test_commit_task_missing_archive() -> Result<()> {
        let (db, handles, blocks, archives, _temp_dir) = create_test_storage_components().await?;

        let err = CommitArchiveTask::new(db, handles, 4, blocks, archives)
            .finish()
            .await
            .unwrap_err();

        assert!(
            err.downcast_ref::<BlockStorageError>()
                .is_some_and(|e| matches!(e, BlockStorageError::ArchiveNotFound))
        );
        Ok(())
    }
}
