use anyhow::Result;
use cassadilia::{Cas, Transaction};
use tycho_util::compression::ZstdCompressStream;
use weedb::rocksdb;

use super::ARCHIVE_EVENT_COMMITTED;
use crate::storage::{CoreDb, tables};

pub(super) struct ArchiveWriter<'a> {
    db: &'a CoreDb,
    archive_id: u32,
    transaction: Transaction<'a, u32>,
    zstd_compressor: ZstdCompressStream<'a>,
    compress_buffer: Vec<u8>,
}

impl<'a> ArchiveWriter<'a> {
    pub(super) fn new(db: &'a CoreDb, archives_cas: &'a Cas<u32>, archive_id: u32) -> Result<Self> {
        let transaction = archives_cas.put(archive_id)?;

        let mut zstd_compressor = ZstdCompressStream::new(9, 64 * 1024)?;

        // Set up multithreaded compression
        let workers = (std::thread::available_parallelism()?.get() / 4) as u8;
        zstd_compressor.multithreaded(workers)?;

        Ok(Self {
            db,
            archive_id,
            transaction,
            zstd_compressor,
            compress_buffer: Vec::with_capacity(64 * 1024),
        })
    }

    pub(super) fn write(&mut self, data: &[u8]) -> Result<()> {
        self.zstd_compressor
            .write(data, &mut self.compress_buffer)?;

        if !self.compress_buffer.is_empty() {
            self.transaction.write(&self.compress_buffer)?;
            self.compress_buffer.clear();
        }

        Ok(())
    }

    pub(super) fn finalize(mut self) -> Result<()> {
        // Finish compression stream to flush any remaining data
        self.zstd_compressor.finish(&mut self.compress_buffer)?;
        if !self.compress_buffer.is_empty() {
            self.transaction.write(&self.compress_buffer)?;
        }

        // Store the committed data to CAS first.
        self.transaction.finish()?;

        // Only after that we remove the entry and
        // at the same time adding a "COMMITTED" event.
        let mut batch = rocksdb::WriteBatch::default();

        // Write a special entry with the total size of the archive
        let mut key = [0u8; tables::ArchiveEvents::KEY_LEN];
        key[..4].copy_from_slice(&self.archive_id.to_be_bytes());
        key[4..].copy_from_slice(&ARCHIVE_EVENT_COMMITTED.to_be_bytes());
        batch.put_cf(&self.db.archive_events.cf(), key.as_slice(), []);

        // Remove related block ids.
        batch.delete_cf(
            &self.db.archive_block_ids.cf(),
            self.archive_id.to_be_bytes(),
        );

        // Done
        self.db.rocksdb().write(batch)?;
        Ok(())
    }
}
