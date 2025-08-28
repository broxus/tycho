use tycho_block_util::archive::ArchiveEntryType;
use tycho_types::models::BlockIdShort;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ArchiveId {
    Found(u32),
    TooNew,
    NotFound,
}

#[derive(thiserror::Error, Debug)]
pub enum BlockStorageError {
    #[error("Archive not found: id={0}")]
    ArchiveNotFound(u32),
    #[error("Block data not found: {0:?}, {1:?}")]
    BlockDataNotFound(BlockIdShort, ArchiveEntryType),
    #[error("Block handle not found: {0}")]
    BlockHandleNotFound(BlockIdShort),
    #[error("Package entry not found: {0:?}, {1:?}")]
    PackageEntryNotFound(BlockIdShort, ArchiveEntryType),
    #[error(
        "Offset {offset} is not aligned to chunk size {chunk_size} (must be multiple of {chunk_size})"
    )]
    InvalidOffset { offset: u64, chunk_size: u64 },
}

#[derive(Debug, Copy, Clone, Default, PartialEq, Eq)]
pub struct BlockGcStats {
    pub mc_blocks_removed: usize,
    pub total_blocks_removed: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OpenStats {
    pub orphaned_flags_count: u32,
    pub restored_flags_count: u32,
    pub archive_count: usize,
    pub archive_min_id: Option<u32>,
    pub archive_max_id: Option<u32>,
    pub package_entries_count: usize,
}
