use tl_proto::TlRead;

use super::ArchiveEntryId;
use crate::archive::proto::{ArchiveEntryHeader, ARCHIVE_ENTRY_HEADER_LEN, ARCHIVE_PREFIX};

/// Stateful archive package reader.
pub struct ArchiveReader<'a> {
    data: &'a [u8],
    offset: usize,
}

impl<'a> ArchiveReader<'a> {
    /// Starts reading archive package
    pub fn new(data: &'a [u8]) -> Result<Self, ArchiveReaderError> {
        let mut offset = 0;
        read_archive_prefix(data, &mut offset)?;
        Ok(Self { data, offset })
    }

    #[inline]
    pub fn offset(&self) -> usize {
        self.offset
    }
}

impl<'a> Iterator for ArchiveReader<'a> {
    type Item = Result<ArchiveEntry<'a>, ArchiveReaderError>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        read_next_entry(self.data, &mut self.offset)
    }
}

fn read_next_entry<'a>(
    data: &'a [u8],
    offset: &mut usize,
) -> Option<Result<ArchiveEntry<'a>, ArchiveReaderError>> {
    if data.len() < *offset + 8 {
        return None;
    }

    Some('item: {
        // Read archive entry header
        let Ok(header) = ArchiveEntryHeader::read_from(data, offset) else {
            break 'item Err(ArchiveReaderError::InvalidArchiveEntryHeader);
        };
        let data_len = header.data_len as usize;

        // Check if data has enough space for the entry
        let Some(target_size) = offset.checked_add(data_len) else {
            // Handle overflow
            break 'item Err(ArchiveReaderError::UnexpectedEntryEof);
        };

        if data.len() < target_size {
            break 'item Err(ArchiveReaderError::UnexpectedEntryEof);
        }

        // Read data
        let data = &data[*offset..*offset + data_len];
        *offset += data_len;

        // Done
        Ok(ArchiveEntry {
            id: ArchiveEntryId {
                block_id: header.block_id,
                ty: header.ty,
            },
            data,
        })
    })
}

/// Parsed archive entry
pub struct ArchiveEntry<'a> {
    pub id: ArchiveEntryId,
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
    pub fn write_verify(&mut self, part: &[u8]) -> Result<(), ArchiveReaderError> {
        let mut offset = 0;

        let part_len = part.len();

        while offset < part_len {
            let remaining = part_len - offset;

            match self {
                Self::Start if part_len >= 4 => {
                    read_archive_prefix(part, &mut offset)?;
                    *self = Self::EntryHeader {
                        buffer: [0; ARCHIVE_ENTRY_HEADER_LEN],
                        filled: 0,
                    }
                }
                Self::Start => return Err(ArchiveReaderError::TooSmallInitialBatch),
                Self::EntryHeader { buffer, filled } => {
                    let remaining = std::cmp::min(remaining, ARCHIVE_ENTRY_HEADER_LEN - *filled);

                    // SAFETY:
                    // - `offset < part.len()`
                    // - `filled < buffer.len()`
                    // - `offset + remaining < part.len() && `
                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            part.as_ptr().add(offset),
                            buffer.as_mut_ptr().add(*filled),
                            remaining,
                        );
                    };

                    offset += remaining;
                    *filled += remaining;

                    if *filled == ARCHIVE_ENTRY_HEADER_LEN {
                        let Ok(header) = ArchiveEntryHeader::read_from(buffer, &mut 0) else {
                            return Err(ArchiveReaderError::InvalidArchiveEntryHeader);
                        };
                        *self = Self::EntryData {
                            data_len: header.data_len as usize,
                        };
                    }
                }
                Self::EntryData { data_len } => {
                    let remaining = std::cmp::min(remaining, *data_len);
                    *data_len -= remaining;
                    offset += remaining;

                    if *data_len == 0 {
                        *self = Self::EntryHeader {
                            buffer: [0; ARCHIVE_ENTRY_HEADER_LEN],
                            filled: 0,
                        }
                    }
                }
            }
        }

        Ok(())
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

fn read_archive_prefix(buf: &[u8], offset: &mut usize) -> Result<(), ArchiveReaderError> {
    let end = *offset;

    // NOTE: `end > end + 4` is needed here because it eliminates useless
    // bounds check with panic. It is not even included into result assembly
    if buf.len() < end + 4 || end > end + 4 {
        return Err(ArchiveReaderError::UnexpectedArchiveEof);
    }

    if buf[end..end + 4] == ARCHIVE_PREFIX {
        *offset += 4;
        Ok(())
    } else {
        Err(ArchiveReaderError::InvalidArchiveHeader)
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
