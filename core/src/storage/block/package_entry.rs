use std::cmp::Ordering;
use std::hash::Hash;

use cassadilia::KeyEncoderError;
use tycho_block_util::archive::ArchiveEntryType;
use tycho_storage::kv::{StoredValue, StoredValueBuffer};
use tycho_types::cell::HashBytes;
use tycho_types::models::*;

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
#[derive(Debug, Hash, Eq, PartialEq, Clone)]
pub struct PackageEntryKey {
    pub block_id: PartialBlockId,
    pub ty: ArchiveEntryType,
}

impl Ord for PackageEntryKey {
    fn cmp(&self, other: &Self) -> Ordering {
        // NOTE: Can't just derive Ord here.
        // RocksDB does a lexicographical byte compare, but the default Ord for
        // `workchain: i32` is wrong. For RocksDB, the bytes for -1 = u32::MAX, and they are > bytes for 0.
        // Casting to u32 mimics this byte-level compare.
        let self_wc_ordered = self.block_id.shard.workchain() as u32;
        let other_wc_ordered = other.block_id.shard.workchain() as u32;

        (
            self_wc_ordered,
            self.block_id.shard.prefix(),
            self.block_id.seqno,
            self.block_id.root_hash,
            self.ty as u8,
        )
            .cmp(&(
                other_wc_ordered,
                other.block_id.shard.prefix(),
                other.block_id.seqno,
                other.block_id.root_hash,
                other.ty as u8,
            ))
    }
}

impl PartialOrd for PackageEntryKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub struct PackageEntryKeyEncoder;
impl cassadilia::KeyEncoder<PackageEntryKey> for PackageEntryKeyEncoder {
    fn encode(&self, key: &PackageEntryKey) -> Result<Vec<u8>, KeyEncoderError> {
        let mut buffer = Vec::with_capacity(PackageEntryKey::SIZE_HINT);
        key.serialize(&mut buffer);

        Ok(buffer)
    }

    fn decode(&self, data: &[u8]) -> Result<PackageEntryKey, KeyEncoderError> {
        let mut reader = data;
        let key = PackageEntryKey::deserialize(&mut reader);
        if reader.is_empty() {
            Ok(key)
        } else {
            Err(KeyEncoderError::DecodeError)
        }
    }
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

    pub fn from_slice(data: &[u8]) -> Self {
        let mut reader = data;
        Self::deserialize(&mut reader)
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

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use super::*;

    #[test]
    fn ord_is_sane_and_matches_bytes() {
        // copied from remove_blocks
        let to_bytes = |k: &PackageEntryKey| -> Vec<u8> {
            let mut bytes = Vec::with_capacity(97);
            bytes.extend_from_slice(&k.block_id.shard.workchain().to_be_bytes());
            bytes.extend_from_slice(&k.block_id.shard.prefix().to_be_bytes());
            bytes.extend_from_slice(&k.block_id.seqno.to_be_bytes());
            bytes.extend_from_slice(&k.block_id.root_hash.0);
            bytes.push(k.ty as u8);
            bytes
        };

        let base = PackageEntryKey {
            block_id: PartialBlockId {
                shard: ShardIdent::BASECHAIN, // wc 0, prefix 0x8000...
                seqno: 100,
                root_hash: HashBytes([1; 32]),
            },
            ty: ArchiveEntryType::Block,
        };

        let check = |a: PackageEntryKey, b: PackageEntryKey, expected: Ordering| {
            let rust_ord = a.cmp(&b);
            let byte_ord = to_bytes(&a).cmp(&to_bytes(&b));
            // if these don't match, our Ord is fucked
            assert_eq!(rust_ord, byte_ord, "Rust Ord vs byte Ord mismatch");
            // check the actual ordering logic
            assert_eq!(rust_ord, expected, "Unexpected ordering");
        };

        // --- test cases ---

        // masterchain (-1) > basechain (0)
        check(
            PackageEntryKey {
                block_id: PartialBlockId {
                    shard: ShardIdent::MASTERCHAIN,
                    ..base.block_id
                },
                ..base
            },
            base.clone(),
            Ordering::Greater,
        );

        // shard prefix split: left < right
        let (left_shard, right_shard) = base.block_id.shard.split().unwrap();
        check(
            PackageEntryKey {
                block_id: PartialBlockId {
                    shard: left_shard,
                    ..base.block_id
                },
                ..base
            },
            PackageEntryKey {
                block_id: PartialBlockId {
                    shard: right_shard,
                    ..base.block_id
                },
                ..base
            },
            Ordering::Less,
        );

        // seqno
        check(
            base.clone(),
            PackageEntryKey {
                block_id: PartialBlockId {
                    seqno: base.block_id.seqno + 1,
                    ..base.block_id
                },
                ..base
            },
            Ordering::Less,
        );

        // root hash
        let mut bigger_hash = base.block_id.root_hash;
        bigger_hash.0[31] = 2; // just need a lexicographically larger hash
        check(
            base.clone(),
            PackageEntryKey {
                block_id: PartialBlockId {
                    root_hash: bigger_hash,
                    ..base.block_id
                },
                ..base
            },
            Ordering::Less,
        );

        // entry type
        check(
            base.clone(),
            PackageEntryKey {
                ty: ArchiveEntryType::Proof,
                ..base
            },
            Ordering::Less,
        );
    }
}
