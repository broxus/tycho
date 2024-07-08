use crate::archive::{ARCHIVE_ENTRY_HEADER_LEN, ARCHIVE_ENTRY_PREFIX, ARCHIVE_PREFIX};

/// Stateful archive package reader.
pub struct ArchiveReader<'a> {
    data: &'a [u8],
    offset: usize,
}

impl<'a> ArchiveReader<'a> {
    /// Starts reading archive package
    pub fn new(data: &'a [u8]) -> Result<Self, ArchiveReaderError> {
        let mut offset = 0;
        read_package_header(data, &mut offset)?;
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
        read_next_package_entry(self.data, &mut self.offset)
    }
}

fn read_next_package_entry<'a>(
    data: &'a [u8],
    offset: &mut usize,
) -> Option<Result<ArchiveEntry<'a>, ArchiveReaderError>> {
    if data.len() < *offset + 8 {
        return None;
    }

    Some('item: {
        // Read archive entry prefix
        if data[*offset..*offset + 2] != ARCHIVE_ENTRY_PREFIX {
            break 'item Err(ArchiveReaderError::InvalidArchiveEntryHeader);
        }
        *offset += 2;

        // Read filename size
        let filename_size = u16::from_le_bytes([data[*offset], data[*offset + 1]]) as usize;
        *offset += 2;

        // Read data size
        let data_size = u32::from_le_bytes([
            data[*offset],
            data[*offset + 1],
            data[*offset + 2],
            data[*offset + 3],
        ]) as usize;
        *offset += 4;

        // Check if data has enough space for the entry
        let Some(target_size) = filename_size
            .checked_add(data_size)
            .and_then(|entry_size| offset.checked_add(entry_size))
        else {
            // Handle overflow
            break 'item Err(ArchiveReaderError::UnexpectedEntryEof);
        };

        if data.len() < target_size {
            break 'item Err(ArchiveReaderError::UnexpectedEntryEof);
        }

        // Read filename
        let Ok(name) = std::str::from_utf8(&data[*offset..*offset + filename_size]) else {
            break 'item Err(ArchiveReaderError::InvalidArchiveEntryName);
        };
        *offset += filename_size;

        // Read data
        let data = &data[*offset..*offset + data_size];
        *offset += data_size;

        // Done
        Ok(ArchiveEntry { name, data })
    })
}

/// Parsed archive package entry
pub struct ArchiveEntry<'a> {
    pub name: &'a str,
    pub data: &'a [u8],
}

/// Archive data stream verifier.
#[derive(Default)]
pub enum ArchiveVerifier {
    #[default]
    Start,
    PackageEntryHeader {
        buffer: [u8; ARCHIVE_ENTRY_HEADER_LEN],
        filled: usize,
    },
    PackageFileName {
        filename_len: usize,
        data_len: usize,
    },
    PackageData {
        data_len: usize,
    },
}

impl ArchiveVerifier {
    /// Verifies next archive package segment.
    pub fn write_verify(&mut self, part: &[u8]) -> Result<(), ArchiveReaderError> {
        let mut offset = 0;

        let part_len = part.len();

        while offset < part_len {
            let remaining = part_len - offset;

            match self {
                Self::Start if part_len >= 4 => {
                    read_package_header(part, &mut offset)?;
                    *self = Self::PackageEntryHeader {
                        buffer: Default::default(),
                        filled: 0,
                    }
                }
                Self::Start => return Err(ArchiveReaderError::TooSmallInitialBatch),
                Self::PackageEntryHeader { buffer, filled } => {
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
                        if buffer[..2] != ARCHIVE_ENTRY_PREFIX {
                            return Err(ArchiveReaderError::InvalidArchiveEntryHeader);
                        }

                        *self = Self::PackageFileName {
                            filename_len: u16::from_le_bytes([buffer[2], buffer[3]]) as usize,
                            data_len: u32::from_le_bytes([
                                buffer[4], buffer[5], buffer[6], buffer[7],
                            ]) as usize,
                        }
                    }
                }
                Self::PackageFileName {
                    filename_len,
                    data_len,
                } => {
                    let remaining = std::cmp::min(remaining, *filename_len);
                    *filename_len -= remaining;
                    offset += remaining;

                    if *filename_len == 0 {
                        *self = Self::PackageData {
                            data_len: *data_len,
                        }
                    }
                }
                Self::PackageData { data_len } => {
                    let remaining = std::cmp::min(remaining, *data_len);
                    *data_len -= remaining;
                    offset += remaining;

                    if *data_len == 0 {
                        *self = Self::PackageEntryHeader {
                            buffer: Default::default(),
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
        if matches!(self, Self::PackageEntryHeader { filled: 0, .. }) {
            Ok(())
        } else {
            Err(ArchiveReaderError::UnexpectedArchiveEof)
        }
    }
}

fn read_package_header(buf: &[u8], offset: &mut usize) -> Result<(), ArchiveReaderError> {
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
}
