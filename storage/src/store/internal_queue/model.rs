use everscale_types::cell::HashBytes;
use everscale_types::models::ShardIdent;

use crate::util::{StoredValue, StoredValueBuffer};

pub struct StorageInternalMessageKey {
    pub lt: u64,
    pub hash: HashBytes,
    pub shard_ident: ShardIdent,
}

impl From<&[u8]> for StorageInternalMessageKey {
    fn from(bytes: &[u8]) -> Self {
        let mut reader = bytes;
        Self::deserialize(&mut reader)
    }
}

impl StoredValue for StorageInternalMessageKey {
    const SIZE_HINT: usize = 40 + ShardIdent::SIZE_HINT;
    type OnStackSlice = [u8; Self::SIZE_HINT];

    fn serialize<T: StoredValueBuffer>(&self, buffer: &mut T) {
        buffer.write_raw_slice(&self.lt.to_be_bytes()); // Use big-endian for proper ordering
        buffer.write_raw_slice(&self.hash.0); // Directly write the byte array
        self.shard_ident.serialize(buffer);
    }

    fn deserialize(reader: &mut &[u8]) -> Self
    where
        Self: Sized,
    {
        assert!(
            reader.len() >= Self::SIZE_HINT,
            "Insufficient data for deserialization"
        );

        let mut lt_bytes = [0u8; 8];
        lt_bytes.copy_from_slice(&reader[..8]);
        let lt = u64::from_be_bytes(lt_bytes); // Use big-endian for proper ordering

        let mut hash_bytes = [0u8; 32];
        hash_bytes.copy_from_slice(&reader[8..40]);
        let hash = HashBytes(hash_bytes);

        *reader = &reader[40..];

        let shard_ident = ShardIdent::deserialize(reader);

        Self {
            lt,
            hash,
            shard_ident,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ShardsInternalMessagesKey {
    pub shard_ident: ShardIdent,
    pub lt: u64,
    pub hash: HashBytes,
}

impl From<&[u8]> for ShardsInternalMessagesKey {
    fn from(bytes: &[u8]) -> Self {
        let mut reader = bytes;
        Self::deserialize(&mut reader)
    }
}

impl StoredValue for ShardsInternalMessagesKey {
    const SIZE_HINT: usize = ShardIdent::SIZE_HINT + 8 + 32; // 32 bytes for hash
    type OnStackSlice = [u8; Self::SIZE_HINT];

    fn serialize<T: StoredValueBuffer>(&self, buffer: &mut T) {
        self.shard_ident.serialize(buffer);
        buffer.write_raw_slice(&self.lt.to_be_bytes()); // Use big-endian for proper ordering
        buffer.write_raw_slice(&self.hash.0); // Directly write the byte array
    }

    fn deserialize(reader: &mut &[u8]) -> Self
    where
        Self: Sized,
    {
        assert!(
            reader.len() >= Self::SIZE_HINT,
            "Insufficient data for deserialization"
        );

        let shard_ident = ShardIdent::deserialize(reader);

        let mut lt_bytes = [0u8; 8];
        lt_bytes.copy_from_slice(&reader[..8]);
        let lt = u64::from_be_bytes(lt_bytes); // Use big-endian for proper ordering

        *reader = &reader[8..];

        let mut hash_bytes = [0u8; 32];
        hash_bytes.copy_from_slice(&reader[..32]);
        let hash = HashBytes(hash_bytes);

        *reader = &reader[32..];

        Self {
            shard_ident,
            lt,
            hash,
        }
    }
}
