use std::hash::Hash;

use everscale_types::cell::HashBytes;
use everscale_types::models::*;
use tycho_block_util::archive::ArchiveEntryType;
use tycho_storage::kv::{StoredValue, StoredValueBuffer};

#[derive(Debug, Hash, Eq, PartialEq, Clone, Copy)]
pub struct PartialBlockId {
    pub shard: ShardIdent,
    pub seqno: u32,
    pub root_hash: HashBytes,
}

impl PartialBlockId {
    pub fn as_short_id(&self) -> BlockIdShort {
        BlockIdShort {
            shard: self.shard,
            seqno: self.seqno,
        }
    }

    pub fn make_full(&self, file_hash: HashBytes) -> BlockId {
        BlockId {
            shard: self.shard,
            seqno: self.seqno,
            root_hash: self.root_hash,
            file_hash,
        }
    }
}

impl From<BlockId> for PartialBlockId {
    fn from(value: BlockId) -> Self {
        Self {
            shard: value.shard,
            seqno: value.seqno,
            root_hash: value.root_hash,
        }
    }
}

impl From<&BlockId> for PartialBlockId {
    fn from(value: &BlockId) -> Self {
        Self {
            shard: value.shard,
            seqno: value.seqno,
            root_hash: value.root_hash,
        }
    }
}

impl StoredValue for PartialBlockId {
    const SIZE_HINT: usize = 4 + 8 + 4 + 32;

    type OnStackSlice = [u8; Self::SIZE_HINT];

    fn serialize<T: StoredValueBuffer>(&self, buffer: &mut T) {
        let mut result = [0; Self::SIZE_HINT];
        result[..4].copy_from_slice(&self.shard.workchain().to_be_bytes());
        result[4..12].copy_from_slice(&self.shard.prefix().to_be_bytes());
        result[12..16].copy_from_slice(&self.seqno.to_be_bytes());
        result[16..48].copy_from_slice(self.root_hash.as_slice());

        buffer.write_raw_slice(&result);
    }

    fn deserialize(reader: &mut &[u8]) -> Self
    where
        Self: Sized,
    {
        assert_eq!(reader.len(), Self::SIZE_HINT, "invalid partial id");

        let workchain = i32::from_be_bytes(reader[..4].try_into().unwrap());
        let prefix = u64::from_be_bytes(reader[4..12].try_into().unwrap());
        let seqno = u32::from_be_bytes(reader[12..16].try_into().unwrap());
        let root_hash = HashBytes::from_slice(&reader[16..48]);

        *reader = &reader[Self::SIZE_HINT..];

        Self {
            shard: ShardIdent::new(workchain, prefix).expect("invalid shard ident"),
            seqno,
            root_hash,
        }
    }
}

/// Package entry id.
#[derive(Debug, Hash, Eq, PartialEq)]
pub struct PackageEntryKey {
    pub block_id: PartialBlockId,
    pub ty: ArchiveEntryType,
}

impl PackageEntryKey {
    pub fn block(block_id: &BlockId) -> Self {
        Self {
            block_id: block_id.into(),
            ty: ArchiveEntryType::Block,
        }
    }

    pub fn proof(block_id: &BlockId) -> Self {
        Self {
            block_id: block_id.into(),
            ty: ArchiveEntryType::Proof,
        }
    }

    pub fn queue_diff(block_id: &BlockId) -> Self {
        Self {
            block_id: block_id.into(),
            ty: ArchiveEntryType::QueueDiff,
        }
    }
}

impl From<(BlockId, ArchiveEntryType)> for PackageEntryKey {
    fn from((block_id, ty): (BlockId, ArchiveEntryType)) -> Self {
        Self {
            block_id: block_id.into(),
            ty,
        }
    }
}

impl StoredValue for PackageEntryKey {
    const SIZE_HINT: usize = 4 + 8 + 4 + 32 + 1;

    type OnStackSlice = [u8; Self::SIZE_HINT];

    fn serialize<T: StoredValueBuffer>(&self, buffer: &mut T) {
        let mut result = [0; Self::SIZE_HINT];
        result[..4].copy_from_slice(&self.block_id.shard.workchain().to_be_bytes());
        result[4..12].copy_from_slice(&self.block_id.shard.prefix().to_be_bytes());
        result[12..16].copy_from_slice(&self.block_id.seqno.to_be_bytes());
        result[16..48].copy_from_slice(self.block_id.root_hash.as_slice());
        result[48] = self.ty as u8;
        buffer.write_raw_slice(&result);
    }

    fn deserialize(reader: &mut &[u8]) -> Self
    where
        Self: Sized,
    {
        assert_eq!(reader.len(), Self::SIZE_HINT, "invalid package entry");

        let workchain = i32::from_be_bytes(reader[..4].try_into().unwrap());
        let prefix = u64::from_be_bytes(reader[4..12].try_into().unwrap());
        let seqno = u32::from_be_bytes(reader[12..16].try_into().unwrap());
        let root_hash = HashBytes::from_slice(&reader[16..48]);
        let ty = ArchiveEntryType::from_byte(reader[48]).expect("invalid entry type");

        *reader = &reader[Self::SIZE_HINT..];

        Self {
            block_id: PartialBlockId {
                shard: ShardIdent::new(workchain, prefix).expect("invalid shard ident"),
                seqno,
                root_hash,
            },
            ty,
        }
    }
}

/// Block data entry id.
#[derive(Debug, Hash, Eq, PartialEq)]
pub struct BlockDataEntryKey {
    pub block_id: PartialBlockId,
    pub chunk_index: u32,
}

impl StoredValue for BlockDataEntryKey {
    const SIZE_HINT: usize = 4 + 8 + 4 + 32 + 4;

    type OnStackSlice = [u8; Self::SIZE_HINT];

    fn serialize<T: StoredValueBuffer>(&self, buffer: &mut T) {
        let mut result = [0; Self::SIZE_HINT];
        result[..4].copy_from_slice(&self.block_id.shard.workchain().to_be_bytes());
        result[4..12].copy_from_slice(&self.block_id.shard.prefix().to_be_bytes());
        result[12..16].copy_from_slice(&self.block_id.seqno.to_be_bytes());
        result[16..48].copy_from_slice(self.block_id.root_hash.as_slice());
        result[48..52].copy_from_slice(&self.chunk_index.to_be_bytes());
        buffer.write_raw_slice(&result);
    }

    fn deserialize(reader: &mut &[u8]) -> Self
    where
        Self: Sized,
    {
        assert_eq!(reader.len(), Self::SIZE_HINT, "invalid block data entry");

        let workchain = i32::from_be_bytes(reader[..4].try_into().unwrap());
        let prefix = u64::from_be_bytes(reader[4..12].try_into().unwrap());
        let seqno = u32::from_be_bytes(reader[12..16].try_into().unwrap());
        let root_hash = HashBytes::from_slice(&reader[16..48]);
        let chunk_index = u32::from_be_bytes(reader[48..52].try_into().unwrap());

        *reader = &reader[Self::SIZE_HINT..];

        Self {
            block_id: PartialBlockId {
                shard: ShardIdent::new(workchain, prefix).expect("invalid shard ident"),
                seqno,
                root_hash,
            },
            chunk_index,
        }
    }
}
