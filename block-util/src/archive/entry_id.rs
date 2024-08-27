use std::borrow::Borrow;
use std::hash::Hash;

use everscale_types::models::*;
use smallvec::SmallVec;

use crate::archive::proto::ArchiveEntryType;

/// Package entry id.
#[derive(Debug, Hash, Eq, PartialEq)]
pub struct ArchiveEntryId<T = BlockId> {
    pub block_id: T,
    pub ty: ArchiveEntryType,
}

impl<T> ArchiveEntryId<T> {
    pub const FILENAME_LEN: usize = 4 + 8 + 4 + 32 + 32 + 4;

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

    pub fn extract_type(data: &[u8]) -> Option<ArchiveEntryType> {
        if data.len() < SERIALIZED_LEN {
            return None;
        }

        ArchiveEntryType::from_byte(data[SERIALIZED_LEN - 1])
    }
}

impl<I> ArchiveEntryId<I>
where
    I: Borrow<BlockId>,
{
    /// Constructs on-stack buffer with the serialized object
    pub fn to_vec(&self) -> SmallVec<[u8; SERIALIZED_LEN]> {
        let mut result = SmallVec::with_capacity(SERIALIZED_LEN);
        let block_id: &BlockId = self.block_id.borrow();
        let ty = self.ty as u8;

        result.extend_from_slice(&block_id.shard.workchain().to_be_bytes());
        result.extend_from_slice(&block_id.shard.prefix().to_be_bytes());
        result.extend_from_slice(&block_id.seqno.to_be_bytes());
        result.extend_from_slice(block_id.root_hash.as_slice());
        result.push(ty);

        result
    }
}

const SERIALIZED_LEN: usize = 4 + 8 + 4 + 32 + 1;
