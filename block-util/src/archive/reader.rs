use everscale_types::models::BlockId;
use tl_proto::TlRead;

use super::ArchiveEntryType;
use crate::archive::proto::{ArchiveEntryHeader, ARCHIVE_ENTRY_HEADER_LEN, ARCHIVE_PREFIX};

/// Stateful archive package reader.
pub struct ArchiveReader<'a> {
    data: &'a [u8],
}

impl<'a> ArchiveReader<'a> {
    /// Starts reading archive package
    pub fn new(mut data: &'a [u8]) -> Result<Self, ArchiveReaderError> {
        read_archive_prefix(&mut data)?;
        Ok(Self { data })
    }
}

impl<'a> Iterator for ArchiveReader<'a> {
    type Item = Result<ArchiveEntry<'a>, ArchiveReaderError>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        read_next_entry(&mut self.data)
    }
}

fn read_next_entry<'a>(
    data: &mut &'a [u8],
) -> Option<Result<ArchiveEntry<'a>, ArchiveReaderError>> {
    if data.len() < 8 {
        return None;
    }

    Some('item: {
        // Read archive entry header
        let Ok(header) = ArchiveEntryHeader::read_from(data) else {
            break 'item Err(ArchiveReaderError::InvalidArchiveEntryHeader);
        };
        let data_len = header.data_len as usize;

        // Read data
        let Some((head, tail)) = data.split_at_checked(data_len) else {
            break 'item Err(ArchiveReaderError::UnexpectedEntryEof);
        };

        *data = tail;

        // Done
        Ok(ArchiveEntry {
            block_id: header.block_id,
            ty: header.ty,
            data: head,
        })
    })
}

/// Parsed archive entry
pub struct ArchiveEntry<'a> {
    pub block_id: BlockId,
    pub ty: ArchiveEntryType,
    pub data: &'a [u8],
}

/// Archive data stream verifier.
#[derive(Default)]
pub enum ArchiveVerifier {
    #[default]
    Start,
    EntryHeader {
        buffer: [u8; ARCHIVE_ENTRY_HEADER_LEN],
        filled: usize,
    },
    EntryData {
        data_len: usize,
    },
}

impl ArchiveVerifier {
    /// Verifies next archive chunk.
    pub fn write_verify(&mut self, mut part: &[u8]) -> Result<(), ArchiveReaderError> {
        loop {
            let part_len = part.len();
            if part_len == 0 {
                return Ok(());
            }

            match self {
                Self::Start if part_len >= 4 => {
                    read_archive_prefix(&mut part)?;
                    *self = Self::EntryHeader {
                        buffer: [0; ARCHIVE_ENTRY_HEADER_LEN],
                        filled: 0,
                    }
                }
                Self::Start => return Err(ArchiveReaderError::TooSmallInitialBatch),
                Self::EntryHeader { buffer, filled } => {
                    let remaining = std::cmp::min(part_len, ARCHIVE_ENTRY_HEADER_LEN - *filled);

                    // SAFETY:
                    // - `offset < part.len()`
                    // - `filled < buffer.len()`
                    // - `offset + remaining < part.len() && `
                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            part.as_ptr(),
                            buffer.as_mut_ptr().add(*filled),
                            remaining,
                        );
                    };

                    part = part.split_at(remaining).1;
                    *filled += remaining;

                    if *filled == ARCHIVE_ENTRY_HEADER_LEN {
                        let Ok(header) = ArchiveEntryHeader::read_from(&mut buffer.as_slice())
                        else {
                            return Err(ArchiveReaderError::InvalidArchiveEntryHeader);
                        };
                        *self = Self::EntryData {
                            data_len: header.data_len as usize,
                        };
                    }
                }
                Self::EntryData { data_len } => {
                    let remaining = std::cmp::min(part_len, *data_len);
                    *data_len -= remaining;
                    part = part.split_at(remaining).1;

                    if *data_len == 0 {
                        *self = Self::EntryHeader {
                            buffer: [0; ARCHIVE_ENTRY_HEADER_LEN],
                            filled: 0,
                        }
                    }
                }
            }
        }
    }

    /// Ensures that the verifier is in the correct state.
    pub fn final_check(&self) -> Result<(), ArchiveReaderError> {
        if matches!(self, Self::EntryHeader { filled: 0, .. }) {
            Ok(())
        } else {
            Err(ArchiveReaderError::UnexpectedArchiveEof)
        }
    }
}

fn read_archive_prefix(buf: &mut &[u8]) -> Result<(), ArchiveReaderError> {
    match buf.split_first_chunk() {
        Some((header, tail)) if header == &ARCHIVE_PREFIX => {
            *buf = tail;
            Ok(())
        }
        _ => Err(ArchiveReaderError::InvalidArchiveHeader),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ArchiveReaderError {
    #[error("invalid archive header")]
    InvalidArchiveHeader,
    #[error("unexpected archive eof")]
    UnexpectedArchiveEof,
    #[error("invalid archive entry header")]
    InvalidArchiveEntryHeader,
    #[error("invalid archive entry name")]
    InvalidArchiveEntryName,
    #[error("unexpected entry eof")]
    UnexpectedEntryEof,
    #[error("too small initial batch")]
    TooSmallInitialBatch,
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}
