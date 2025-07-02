use weedb::rocksdb;

use crate::storage::BlockDataGuard;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ArchiveId {
    Found(u32),
    TooNew,
    NotFound,
}

#[derive(thiserror::Error, Debug)]
pub enum BlockStorageError {
    #[error("Archive not found")]
    ArchiveNotFound,
    #[error("Block data not found")]
    BlockDataNotFound,
    #[error("Block handle not found")]
    BlockHandleNotFound,
    #[error("Package entry not found")]
    PackageEntryNotFound,
    #[error("Offset is outside of the archive slice")]
    InvalidOffset,
}

pub struct FullBlockDataGuard<'a> {
    pub _lock: BlockDataGuard<'a>,
    pub data: rocksdb::DBPinnableSlice<'a>,
}

impl AsRef<[u8]> for FullBlockDataGuard<'_> {
    fn as_ref(&self) -> &[u8] {
        self.data.as_ref()
    }
}

#[derive(Debug, Copy, Clone, Default, PartialEq, Eq)]
pub struct BlockGcStats {
    pub mc_blocks_removed: usize,
    pub total_blocks_removed: usize,
}
