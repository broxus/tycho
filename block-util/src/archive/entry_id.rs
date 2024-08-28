use std::hash::Hash;

use everscale_types::models::*;

use crate::archive::proto::ArchiveEntryType;

/// Package entry id.
#[derive(Debug, Hash, Eq, PartialEq)]
pub struct ArchiveEntryId<T = BlockId> {
    pub block_id: T,
    pub ty: ArchiveEntryType,
}

impl<T> ArchiveEntryId<T> {
    pub fn block(block_id: T) -> Self {
        Self {
            block_id,
            ty: ArchiveEntryType::Block,
        }
    }

    pub fn proof(block_id: T) -> Self {
        Self {
            block_id,
            ty: ArchiveEntryType::Proof,
        }
    }

    pub fn queue_diff(block_id: T) -> Self {
        Self {
            block_id,
            ty: ArchiveEntryType::QueueDiff,
        }
    }
}
