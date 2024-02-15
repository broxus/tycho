use std::io::Read;

use anyhow::{Context, Result};
use crc::{Crc, CRC_32_ISCSI};
use everscale_types::cell::{CellDescriptor, LevelMask};
use smallvec::SmallVec;

macro_rules! try_read {
    ($expr:expr) => {
        match $expr {
            Ok(data) => data,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        }
    };
}

pub struct ShardStatePacketReader {
    hasher: crc::Digest<'static, u32>,
    has_crc: bool,
    offset: usize,
    current_packet: Vec<u8>,
    next_packet: Vec<u8>,
    bytes_to_skip: usize,
}

impl ShardStatePacketReader {
    pub fn new() -> Self {
        Self {
            hasher: CRC.digest(),
            has_crc: true,
            offset: 0,
            current_packet: Default::default(),
            next_packet: Default::default(),
            bytes_to_skip: 0,
        }
    }

    pub fn read_header(&mut self) -> Result<Option<BocHeader>> {
        const BOC_INDEXED_TAG: u32 = 0x68ff65f3;
        const BOC_INDEXED_CRC32_TAG: u32 = 0xacc3a728;
        const BOC_GENERIC_TAG: u32 = 0xb5ee9c72;

        if self.process_skip() == ReaderAction::Incomplete {
            return Ok(None);
        }

        let mut src = self.begin();
        let mut total_size = 0u64;

        let magic = try_read!(src.read_be_u32());
        total_size += 4;

        let first_byte = try_read!(src.read_byte());
        total_size += 1;

        let index_included;
        let mut has_crc = false;
        let ref_size;

        match magic {
            BOC_INDEXED_TAG => {
                ref_size = first_byte as usize;
                index_included = true;
            }
            BOC_INDEXED_CRC32_TAG => {
                ref_size = first_byte as usize;
                index_included = true;
                has_crc = true;
            }
            BOC_GENERIC_TAG => {
                index_included = first_byte & 0b1000_0000 != 0;
                has_crc = first_byte & 0b0100_0000 != 0;
                ref_size = (first_byte & 0b0000_0111) as usize;
            }
            _ => {
                return Err(ShardStateParserError::InvalidShardStateHeader).context("Invalid flags")
            }
        }

        src.reader.has_crc = has_crc;

        if ref_size == 0 || ref_size > 4 {
            return Err(ShardStateParserError::InvalidShardStateHeader)
                .context("Ref size must be in range [1;4]");
        }

        let offset_size = try_read!(src.read_byte()) as u64;
        total_size += 1;
        if offset_size == 0 || offset_size > 8 {
            return Err(ShardStateParserError::InvalidShardStateHeader)
                .context("Offset size must be in range [1;8]");
        }

        let cell_count = try_read!(src.read_be_uint(ref_size));
        total_size += ref_size as u64;

        let root_count = try_read!(src.read_be_uint(ref_size));
        total_size += ref_size as u64;

        try_read!(src.read_be_uint(ref_size)); // skip absent
        total_size += ref_size as u64;

        if root_count != 1 {
            return Err(ShardStateParserError::InvalidShardStateHeader)
                .context("Expected one root cell");
        }
        if root_count > cell_count {
            return Err(ShardStateParserError::InvalidShardStateHeader)
                .context("Root count is greater then cell count");
        }

        total_size += try_read!(src.read_be_uint(offset_size as usize)); // total cells size
        total_size += offset_size;

        let root_index = if magic == BOC_GENERIC_TAG {
            let root_index = try_read!(src.read_be_uint(ref_size));
            total_size += ref_size as u64;
            root_index
        } else {
            0
        };

        src.end();

        if index_included {
            let index_size = cell_count * offset_size;
            total_size += index_size;
            self.set_skip(index_size as usize);
        }

        if has_crc {
            total_size += 4;
        }

        Ok(Some(BocHeader {
            root_index,
            index_included,
            has_crc,
            ref_size,
            offset_size,
            cell_count,
            total_size,
        }))
    }

    pub fn read_cell(&mut self, ref_size: usize, buffer: &mut [u8]) -> Result<Option<usize>> {
        if self.process_skip() == ReaderAction::Incomplete {
            return Ok(None);
        }

        let mut src = self.begin();

        let d1 = try_read!(src.read_byte());
        let l = d1 >> 5;
        let h = (d1 & 0b0001_0000) != 0;
        let r = (d1 & 0b0000_0111) as usize;
        let absent = r == 0b111 && h;

        buffer[0] = d1;

        let size = if absent {
            let data_size = 32 * ((LevelMask::new(l).level() + 1) as usize);
            try_read!(src.read_exact(&mut buffer[1..1 + data_size]));

            tracing::info!("ABSENT");

            // 1 byte of d1 + fixed data size of absent cell
            1 + data_size
        } else {
            if r > 4 {
                tracing::error!("CELLS: {r}");
                return Err(ShardStateParserError::InvalidShardStateCell)
                    .context("Cell must contain at most 4 references");
            }

            let d2 = try_read!(src.read_byte());
            buffer[1] = d2;

            // Skip optional precalculated hashes
            let hash_count = LevelMask::new(l).level() as usize + 1;
            if h && !src.skip(hash_count * (32 + 2)) {
                return Ok(None);
            }

            let data_size = ((d2 >> 1) + u8::from(d2 & 1 != 0)) as usize;
            try_read!(src.read_exact(&mut buffer[2..2 + data_size + r * ref_size]));

            // 2 bytes for d1 and d2 + data size + total references size
            2 + data_size + r * ref_size
        };

        src.end();

        Ok(Some(size))
    }

    pub fn read_crc(&mut self) -> Result<Option<()>> {
        if self.process_skip() == ReaderAction::Incomplete {
            return Ok(None);
        }

        let current_crc = std::mem::replace(&mut self.hasher, CRC.digest()).finalize();

        let mut src = self.begin();
        let target_crc = try_read!(src.read_le_u32());
        src.end();

        if current_crc == target_crc {
            Ok(Some(()))
        } else {
            Err(ShardStateParserError::CrcMismatch.into())
        }
    }

    pub fn set_next_packet(&mut self, packet: Vec<u8>) {
        self.next_packet = packet;
    }

    fn begin(&'_ mut self) -> ShardStatePacketReaderTransaction<'_> {
        let offset = self.offset;
        ShardStatePacketReaderTransaction {
            reader: self,
            reading_next_packet: false,
            offset,
        }
    }

    fn set_skip(&mut self, n: usize) {
        self.bytes_to_skip = n;
    }

    fn process_skip(&mut self) -> ReaderAction {
        if self.bytes_to_skip == 0 {
            return ReaderAction::Complete;
        }

        let mut n = std::mem::take(&mut self.bytes_to_skip);

        let remaining = self.current_packet.len() - self.offset;
        match n.cmp(&remaining) {
            std::cmp::Ordering::Less => {
                self.hasher
                    .update(&self.current_packet[self.offset..self.offset + n]);
                self.offset += n;
                ReaderAction::Complete
            }
            std::cmp::Ordering::Equal => {
                self.hasher.update(&self.current_packet[self.offset..]);
                self.offset = 0;
                self.current_packet = std::mem::take(&mut self.next_packet);
                ReaderAction::Complete
            }
            std::cmp::Ordering::Greater => {
                n -= remaining;
                self.hasher.update(&self.current_packet[self.offset..]);
                self.offset = 0;
                self.current_packet = std::mem::take(&mut self.next_packet);

                if n > self.current_packet.len() {
                    n -= self.current_packet.len();
                    self.hasher.update(&self.current_packet);
                    self.current_packet = Vec::new();
                    self.bytes_to_skip = n;
                    ReaderAction::Incomplete
                } else {
                    self.offset = n;
                    self.hasher.update(&self.current_packet[..self.offset]);
                    ReaderAction::Complete
                }
            }
        }
    }
}

static CRC: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

#[derive(Debug)]
pub struct BocHeader {
    pub root_index: u64,
    pub index_included: bool,
    pub has_crc: bool,
    pub ref_size: usize,
    pub offset_size: u64,
    pub cell_count: u64,
    pub total_size: u64,
}

pub struct RawCell<'a> {
    pub descriptor: CellDescriptor,
    pub data: &'a [u8],
    pub bit_len: u16,
    pub reference_indices: SmallVec<[u32; 4]>,
}

impl<'a> RawCell<'a> {
    pub fn from_stored_data<R>(
        src: &mut R,
        ref_size: usize,
        cell_count: usize,
        cell_index: usize,
        data_buffer: &'a mut [u8],
    ) -> Result<Self>
    where
        R: Read,
    {
        let mut descriptor = [0u8; 2];
        src.read_exact(&mut descriptor)?;
        let descriptor = CellDescriptor::new(descriptor);
        let byte_len = descriptor.byte_len() as usize;
        let ref_count = descriptor.reference_count() as usize;

        anyhow::ensure!(!descriptor.is_absent(), "Absent cells are not supported");

        let data = &mut data_buffer[0..byte_len];
        src.read_exact(&mut data[..byte_len])?;

        let mut reference_indices = SmallVec::with_capacity(ref_count);
        for _ in 0..ref_count {
            let index = src.read_be_uint(ref_size)? as usize;
            if index > cell_count || index <= cell_index {
                return Err(ShardStateParserError::InvalidShardStateCell)
                    .context("Reference index out of range");
            } else {
                reference_indices.push(index as u32);
            }
        }

        let bit_len = if descriptor.is_aligned() {
            (byte_len * 8) as u16
        } else if let Some(data) = data.last() {
            byte_len as u16 * 8 - data.trailing_zeros() as u16 - 1
        } else {
            0
        };

        Ok(RawCell {
            descriptor,
            data,
            bit_len,
            reference_indices,
        })
    }
}

#[derive(Copy, Clone, Eq, PartialEq)]
pub enum ReaderAction {
    Incomplete,
    Complete,
}

pub struct ShardStatePacketReaderTransaction<'a> {
    reader: &'a mut ShardStatePacketReader,
    reading_next_packet: bool,
    offset: usize,
}

impl<'a> ShardStatePacketReaderTransaction<'a> {
    pub fn skip(&mut self, mut n: usize) -> bool {
        loop {
            let current_packet = match self.reading_next_packet {
                // Reading non-empty current packet
                false if self.offset < self.reader.current_packet.len() => {
                    &self.reader.current_packet
                }

                // Current packet is empty - retry and switch to next
                false => {
                    self.reading_next_packet = true;
                    self.offset = 0;
                    continue;
                }

                // Reading non-empty next packet
                true if self.offset < self.reader.next_packet.len() => &self.reader.next_packet,

                // Reading next packet which is empty
                true => return false,
            };

            let skipped = std::cmp::min(current_packet.len() - self.offset, n);
            n -= skipped;
            self.offset += skipped;

            if n == 0 {
                return true;
            }
        }
    }

    pub fn end(self) {
        if self.reading_next_packet {
            if self.reader.has_crc {
                // Write to the hasher until the end of current packet
                self.reader
                    .hasher
                    .update(&self.reader.current_packet[self.reader.offset..]);

                // Write to the hasher current bytes
                self.reader
                    .hasher
                    .update(&self.reader.next_packet[..self.offset]);
            }

            // Replace current packet
            self.reader.current_packet = std::mem::take(&mut self.reader.next_packet);
        } else if self.reader.has_crc {
            // Write to the hasher current bytes
            self.reader
                .hasher
                .update(&self.reader.current_packet[self.reader.offset..self.offset]);
        }

        // Bump offset
        self.reader.offset = self.offset;
    }
}

impl<'a> Read for ShardStatePacketReaderTransaction<'a> {
    fn read(&mut self, mut buf: &mut [u8]) -> std::io::Result<usize> {
        let mut result = 0;

        loop {
            let current_packet = match self.reading_next_packet {
                // Reading non-empty current packet
                false if self.offset < self.reader.current_packet.len() => {
                    &self.reader.current_packet
                }

                // Current packet is empty - retry and switch to next
                false => {
                    self.reading_next_packet = true;
                    self.offset = 0;
                    continue;
                }

                // Reading non-empty next packet
                true if self.offset < self.reader.next_packet.len() => &self.reader.next_packet,

                // Reading next packet which is empty
                true => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "packet buffer underflow",
                    ))
                }
            };

            let n = std::cmp::min(current_packet.len() - self.offset, buf.len());
            for i in 0..n {
                buf[i] = current_packet[self.offset + i];
            }

            result += n;
            self.offset += n;

            let tmp = buf;
            buf = &mut tmp[n..];

            if buf.is_empty() {
                return Ok(result);
            }
        }
    }
}

#[derive(thiserror::Error, Debug)]
enum ShardStateParserError {
    #[error("Invalid shard state header")]
    InvalidShardStateHeader,
    #[error("Invalid shard state cell")]
    InvalidShardStateCell,
    #[error("Crc mismatch")]
    CrcMismatch,
}

pub trait ByteOrderRead {
    fn read_be_uint(&mut self, bytes: usize) -> std::io::Result<u64>;
    fn read_byte(&mut self) -> std::io::Result<u8>;
    fn read_be_u32(&mut self) -> std::io::Result<u32>;
    fn read_le_u32(&mut self) -> std::io::Result<u32>;
}

impl<T: std::io::Read> ByteOrderRead for T {
    fn read_be_uint(&mut self, bytes: usize) -> std::io::Result<u64> {
        match bytes {
            1 => {
                let mut buf = [0];
                self.read_exact(&mut buf)?;
                Ok(buf[0] as u64)
            }
            2 => {
                let mut buf = [0; 2];
                self.read_exact(&mut buf)?;
                Ok(u16::from_be_bytes(buf) as u64)
            }
            3..=4 => {
                let mut buf = [0; 4];
                self.read_exact(&mut buf[4 - bytes..])?;
                Ok(u32::from_be_bytes(buf) as u64)
            }
            5..=8 => {
                let mut buf = [0; 8];
                self.read_exact(&mut buf[8 - bytes..])?;
                Ok(u64::from_be_bytes(buf))
            }
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "too many bytes to read in u64",
            )),
        }
    }

    fn read_byte(&mut self) -> std::io::Result<u8> {
        self.read_be_uint(1).map(|value| value as u8)
    }

    fn read_be_u32(&mut self) -> std::io::Result<u32> {
        self.read_be_uint(4).map(|value| value as u32)
    }

    fn read_le_u32(&mut self) -> std::io::Result<u32> {
        let mut buf = [0; 4];
        self.read_exact(&mut buf)?;
        Ok(u32::from_le_bytes(buf))
    }
}
