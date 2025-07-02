use anyhow::Result;
use tycho_util::compression::ZstdCompressStream;
use weedb::rocksdb;

use crate::storage::{CoreDb, tables};

const ARCHIVE_SIZE_MAGIC: u64 = u64::MAX;

pub(super) struct ArchiveWriter<'a> {
    db: &'a CoreDb,
    archive_id: u32,
    chunk_len: usize,
    total_len: u64,
    chunk_index: u64,
    chunks_buffer: Vec<u8>,
    zstd_compressor: ZstdCompressStream<'a>,
}

impl<'a> ArchiveWriter<'a> {
    pub(super) fn new(db: &'a CoreDb, archive_id: u32, chunk_len: u64) -> Result<Self> {
        let chunk_len = chunk_len as usize;

        let mut zstd_compressor = ZstdCompressStream::new(9, chunk_len)?;

        let workers = (std::thread::available_parallelism()?.get() / 4) as u8;
        zstd_compressor.multithreaded(workers)?;

        Ok(Self {
            db,
            archive_id,
            chunk_len,
            total_len: 0,
            chunk_index: 0,
            chunks_buffer: Vec::with_capacity(chunk_len),
            zstd_compressor,
        })
    }

    pub(super) fn write(&mut self, data: &[u8]) -> Result<()> {
        self.zstd_compressor.write(data, &mut self.chunks_buffer)?;
        self.flush(false)
    }

    pub(super) fn finalize(mut self) -> Result<()> {
        self.zstd_compressor.finish(&mut self.chunks_buffer)?;

        // Write the last chunk
        self.flush(true)?;
        debug_assert!(self.chunks_buffer.is_empty());

        // Write archive size and remove archive block ids atomically
        let archives_cf = self.db.archives.cf();
        let block_ids_cf = self.db.archive_block_ids.cf();

        let mut batch = rocksdb::WriteBatch::default();

        // Write a special entry with the total size of the archive
        let mut key = [0u8; tables::Archives::KEY_LEN];
        key[..4].copy_from_slice(&self.archive_id.to_be_bytes());
        key[4..].copy_from_slice(&ARCHIVE_SIZE_MAGIC.to_be_bytes());
        batch.put_cf(&archives_cf, key.as_slice(), self.total_len.to_le_bytes());

        // Remove related block ids
        batch.delete_cf(&block_ids_cf, self.archive_id.to_be_bytes());

        self.db.rocksdb().write(batch)?;
        Ok(())
    }

    fn flush(&mut self, finalize: bool) -> Result<()> {
        let buffer_len = self.chunks_buffer.len();
        if buffer_len == 0 {
            return Ok(());
        }

        let mut key = [0u8; tables::Archives::KEY_LEN];
        key[..4].copy_from_slice(&self.archive_id.to_be_bytes());

        let mut do_flush = |data: &[u8]| {
            key[4..].copy_from_slice(&self.chunk_index.to_be_bytes());

            self.total_len += data.len() as u64;
            self.chunk_index += 1;

            self.db.archives.insert(key, data)
        };

        // Write all full chunks
        let mut buffer_offset = 0;
        while buffer_offset + self.chunk_len <= buffer_len {
            do_flush(&self.chunks_buffer[buffer_offset..buffer_offset + self.chunk_len])?;
            buffer_offset += self.chunk_len;
        }

        if finalize {
            // Just write the remaining data on finalize
            do_flush(&self.chunks_buffer[buffer_offset..])?;
            self.chunks_buffer.clear();
        } else {
            // Shift the remaining data to the beginning of the buffer and clear the rest
            let rem = buffer_len % self.chunk_len;
            if rem == 0 {
                self.chunks_buffer.clear();
            } else if buffer_offset > 0 {
                // TODO: Use memmove since we are copying non-overlapping regions
                self.chunks_buffer.copy_within(buffer_offset.., 0);
                self.chunks_buffer.truncate(rem);
            }
        }

        Ok(())
    }
}
