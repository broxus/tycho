use tl_proto::{TlRead, TlWrite};
use tycho_types::models::BlockId;

use crate::tl;

const ARCHIVE_PREFIX_ID: u32 = tl_proto::id!("archive.prefix", scheme = "proto.tl");
pub const ARCHIVE_PREFIX: [u8; 4] = u32::to_le_bytes(ARCHIVE_PREFIX_ID);

#[derive(Debug, Clone, TlRead, TlWrite)]
#[tl(boxed, id = "archive.entryHeader", scheme = "proto.tl")]
pub struct ArchiveEntryHeader {
    #[tl(with = "tl::block_id")]
    pub block_id: BlockId,
    pub ty: ArchiveEntryType,
    pub data_len: u32,
}

pub const ARCHIVE_ENTRY_HEADER_LEN: usize = 4 + 4 + 8 + 4 + 32 + 32 + 4 + 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, TlRead, TlWrite)]
#[tl(boxed, scheme = "proto.tl")]
#[repr(u8)]
pub enum ArchiveEntryType {
    #[tl(id = "archive.entryType.block")]
    Block = 0,
    #[tl(id = "archive.entryType.proof")]
    Proof = 1,
    #[tl(id = "archive.entryType.queueDiff")]
    QueueDiff = 2,
}

impl ArchiveEntryType {
    pub const fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0 => Some(Self::Block),
            1 => Some(Self::Proof),
            2 => Some(Self::QueueDiff),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entry_header_len_consistent() {
        let header = ArchiveEntryHeader {
            block_id: BlockId::default(),
            ty: ArchiveEntryType::Block,
            data_len: 123123,
        };

        let serialized = tl_proto::serialize(header);
        assert_eq!(serialized.len(), ARCHIVE_ENTRY_HEADER_LEN);
    }
}
