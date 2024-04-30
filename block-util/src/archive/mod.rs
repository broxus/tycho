use bytes::Bytes;

pub use self::entry_id::{ArchiveEntryId, GetFileName};
pub use self::reader::{ArchiveEntry, ArchiveReader, ArchiveReaderError, ArchiveVerifier};

mod entry_id;
mod reader;

pub const ARCHIVE_PREFIX: [u8; 4] = u32::to_le_bytes(0xae8fdd01);
pub const ARCHIVE_ENTRY_PREFIX: [u8; 2] = u16::to_le_bytes(0x1e8b);
pub const ARCHIVE_ENTRY_HEADER_LEN: usize = ARCHIVE_ENTRY_PREFIX.len() + 2 + 4; // magic + filename len + data len

#[derive(Clone)]
pub enum ArchiveData {
    /// The raw data is known.
    New(Bytes),
    /// Raw data is not known (due to nondeterministic serialization).
    Existing,
}

impl ArchiveData {
    /// Assumes that the object is constructed with known raw data.
    pub fn as_new_archive_data(&self) -> Result<&[u8], WithArchiveDataError> {
        match self {
            ArchiveData::New(data) => Ok(data),
            ArchiveData::Existing => Err(WithArchiveDataError),
        }
    }
}

/// Parsed data wrapper, augmented with the optional raw data.
///
/// Stores the raw data only in the context of the archive parser, or received block.
///
/// NOTE: Can be safely cloned, all raw bytes are shared (see [`Bytes`])
///
/// See: [`ArchiveData`]
#[derive(Clone)]
pub struct WithArchiveData<T> {
    pub data: T,
    pub archive_data: ArchiveData,
}

impl<T> WithArchiveData<T> {
    /// Constructs a new object from the context with known raw data.
    pub fn new<A>(data: T, archive_data: A) -> Self
    where
        Bytes: From<A>,
    {
        Self {
            data,
            archive_data: ArchiveData::New(Bytes::from(archive_data)),
        }
    }

    /// Constructs a new object from the context without known raw data.
    pub fn loaded(data: T) -> Self {
        Self {
            data,
            archive_data: ArchiveData::Existing,
        }
    }

    /// Assumes that the object is constructed with known raw data.
    pub fn as_new_archive_data(&self) -> Result<&[u8], WithArchiveDataError> {
        self.archive_data.as_new_archive_data()
    }
}

impl<T> std::ops::Deref for WithArchiveData<T> {
    type Target = T;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

#[derive(Debug, Copy, Clone, thiserror::Error)]
#[error("archive data not loaded")]
pub struct WithArchiveDataError;

/// Encodes archive package segment.
pub fn make_archive_entry(filename: &str, data: &[u8]) -> Vec<u8> {
    let mut vec = Vec::with_capacity(2 + 2 + 4 + filename.len() + data.len());
    vec.extend_from_slice(&ARCHIVE_ENTRY_PREFIX);
    vec.extend_from_slice(&(filename.len() as u16).to_le_bytes());
    vec.extend_from_slice(&(data.len() as u32).to_le_bytes());
    vec.extend_from_slice(filename.as_bytes());
    vec.extend_from_slice(data);
    vec
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn correct_context() {
        const DATA: &[u8] = &[1, 2, 3];

        assert_eq!(
            WithArchiveData::new((), DATA.to_vec())
                .as_new_archive_data()
                .unwrap(),
            DATA
        );
        assert!(WithArchiveData::loaded(()).as_new_archive_data().is_err());
    }
}
