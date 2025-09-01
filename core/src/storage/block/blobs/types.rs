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
