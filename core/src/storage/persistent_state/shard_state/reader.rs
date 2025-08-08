use std::io::{BufReader, Read};

use crc32c::crc32c_append;
use tycho_types::boc::BocTag;
use tycho_types::cell::CellDescriptor;
use tycho_util::io::ByteOrderRead;

pub struct ShardStateReader<R> {
    header: BriefBocHeader,
    reader: BufReaderWithCrc<R>,
}

impl<R: Read> ShardStateReader<R> {
    pub fn begin(mut reader: R) -> std::io::Result<Self> {
        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;
        let mut total_size = 4u64;

        let first_byte = reader.read_byte()?;
        total_size += 1;

        let index_included;
        let mut has_root_index = false;
        let mut has_crc = false;
        let ref_size;

        match BocTag::from_bytes(magic) {
            Some(BocTag::Indexed) => {
                ref_size = first_byte as usize;
                index_included = true;
            }
            Some(BocTag::IndexedCrc32) => {
                ref_size = first_byte as usize;
                index_included = true;
                has_crc = true;
            }
            Some(BocTag::Generic) => {
                has_root_index = true;
                index_included = first_byte & 0b1000_0000 != 0;
                has_crc = first_byte & 0b0100_0000 != 0;
                ref_size = (first_byte & 0b0000_0111) as usize;
            }
            _ => return Err(parser_error("unknown BOC tag")),
        }

        let mut reader = CrcOptReader {
            checksum: has_crc.then_some(0),
            checksum_until: u64::MAX,
            read: total_size,
            inner: reader,
        };

        if ref_size == 0 || ref_size > 4 {
            return Err(parser_error("ref size must be in range [1;4]"));
        }

        let offset_size = reader.read_byte()? as u64;
        total_size += 1;
        if offset_size == 0 || offset_size > 8 {
            return Err(parser_error("offset size must be in range [1;8]"));
        }

        let cell_count = reader.read_be_uint(ref_size)?;
        total_size += ref_size as u64;

        let root_count = reader.read_be_uint(ref_size)?;
        total_size += ref_size as u64;

        reader.read_be_uint(ref_size)?; // skip absent
        total_size += ref_size as u64;

        if root_count != 1 {
            return Err(parser_error("expected exactly one root cell"));
        }
        if root_count > cell_count {
            return Err(parser_error("root count is greater than cell count"));
        }

        const TOTAL_CELLS_LIMIT: u64 = 100 << 30; // 100 GB
        let total_cells_size = reader.read_be_uint(offset_size as usize)?;
        total_size += offset_size;

        if total_cells_size > TOTAL_CELLS_LIMIT {
            return Err(parser_error("total cell size is too big"));
        }
        total_size += total_cells_size;

        let root_index = if has_root_index {
            let root_index = reader.read_be_uint(ref_size)?;
            total_size += ref_size as u64;
            root_index
        } else {
            0
        };

        let index_size;
        if index_included {
            const INDEX_SIZE_LIMIT: u64 = 10 << 30; // 10 GB
            index_size = cell_count.saturating_mul(offset_size);
            if index_size > INDEX_SIZE_LIMIT {
                return Err(parser_error("index size is too big"));
            }
            total_size += index_size;
        } else {
            index_size = 0;
        }

        // NOTE: At this point only the read bytes were included in the checksum.
        // However, `BufReader` will read by chunks and might read a bit more than
        // we need. That's why after we know the exact amount of bytes required for
        // the checksum, we can set the `checksum_until` limit and safely wrap the
        // reader into `BufReader`.
        debug_assert!(reader.read <= reader.checksum_until);
        reader.checksum_until = total_size;
        let mut reader = BufReader::new(reader);

        if index_included {
            // NOTE: We must forward all bytes to the CRC reader to get the correct checksum.
            //       AND we are doing it using the buf reader so it might not be that slow.
            std::io::copy(&mut reader.by_ref().take(index_size), &mut std::io::sink())?;
        }

        if has_crc {
            total_size += 4;
        }

        let header = BriefBocHeader {
            root_index,
            index_included,
            has_crc,
            ref_size,
            offset_size,
            cell_count,
            total_size,
        };

        Ok(Self { header, reader })
    }

    pub fn header(&self) -> &BriefBocHeader {
        &self.header
    }

    pub fn read_next_cell(&mut self, buffer: &mut [u8; 256]) -> std::io::Result<usize> {
        let descriptor = {
            let mut bytes = [0u8; 2];
            self.reader.read_exact(&mut bytes)?;
            CellDescriptor::new(bytes)
        };

        if descriptor.is_absent() {
            return Err(parser_error("absent cell are not supported"));
        }

        let refs = descriptor.reference_count() as usize;
        if refs > 4 {
            return Err(parser_error("invalid reference count"));
        }

        let hash_count = descriptor.hash_count();
        if descriptor.store_hashes() {
            // NOTE: We must forward all skipped bytes to the CRC reader to get the correct checksum
            std::io::copy(
                &mut self.reader.by_ref().take(hash_count as u64 * (32 + 2)),
                &mut std::io::sink(),
            )?;
        }

        let byte_len = descriptor.byte_len() as usize;
        let total_len = 2 + byte_len + refs * self.header.ref_size;

        buffer[0] = descriptor.d1;
        buffer[1] = descriptor.d2;
        self.reader.read_exact(&mut buffer[2..total_len])?;

        // TODO: Add full cell validation here? But what to do with indices

        Ok(total_len)
    }

    pub fn finish(mut self) -> std::io::Result<R> {
        debug_assert_eq!(
            self.header.has_crc,
            self.reader.get_ref().checksum.is_some()
        );

        if let Some(checksum) = self.reader.get_ref().checksum {
            let downloaded_crc = self.reader.read_le_u32()?;
            if checksum != downloaded_crc {
                return Err(parser_error("checksum mismatch"));
            }
        }

        Ok(self.reader.into_inner().inner)
    }
}

#[derive(Debug, Clone, Copy)]
#[allow(unused)]
pub struct BriefBocHeader {
    pub root_index: u64,
    pub index_included: bool,
    pub has_crc: bool,
    pub ref_size: usize,
    pub offset_size: u64,
    pub cell_count: u64,
    pub total_size: u64,
}

type BufReaderWithCrc<R> = BufReader<CrcOptReader<R>>;

struct CrcOptReader<R> {
    checksum: Option<u32>,
    checksum_until: u64,
    read: u64,
    inner: R,
}

impl<R: Read> Read for CrcOptReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let out = self.inner.read(buf)?;
        if let Some(checksum) = &mut self.checksum
            && let Some(remaining) = self.checksum_until.checked_sub(self.read)
        {
            let to_crc = std::cmp::min(out, remaining as usize);
            *checksum = crc32c_append(*checksum, &buf[..to_crc]);
        }
        Ok(out)
    }
}

fn parser_error<E>(error: E) -> std::io::Error
where
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    std::io::Error::other(error)
}
