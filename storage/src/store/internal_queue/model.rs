use everscale_types::cell::HashBytes;
use everscale_types::models::ShardIdent;

use crate::util::{StoredValue, StoredValueBuffer};

pub struct InternalMessageKey {
    pub lt: u64,
    pub hash: HashBytes,
    pub shard_ident: ShardIdent,
}

impl From<&[u8]> for InternalMessageKey {
    fn from(bytes: &[u8]) -> Self {
        let mut reader = bytes;
        Self::deserialize(&mut reader)
    }
}

impl StoredValue for InternalMessageKey {
    const SIZE_HINT: usize = 40 + ShardIdent::SIZE_HINT;
    type OnStackSlice = [u8; Self::SIZE_HINT];

    fn serialize<T: StoredValueBuffer>(&self, buffer: &mut T) {
        buffer.write_raw_slice(&self.lt.to_le_bytes());
        buffer.write_raw_slice(&self.hash.0);
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
        let lt = u64::from_le_bytes(lt_bytes);

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

pub struct ShardsInternalMessagesKey {
    pub shard_ident: ShardIdent,
    pub lt: u64,
}

impl From<&[u8]> for ShardsInternalMessagesKey {
    fn from(bytes: &[u8]) -> Self {
        let mut reader = bytes;
        Self::deserialize(&mut reader)
    }
}

impl StoredValue for ShardsInternalMessagesKey {
    const SIZE_HINT: usize = ShardIdent::SIZE_HINT + 8;
    type OnStackSlice = [u8; Self::SIZE_HINT];

    fn serialize<T: StoredValueBuffer>(&self, buffer: &mut T) {
        self.shard_ident.serialize(buffer);
        buffer.write_raw_slice(&self.lt.to_le_bytes());
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
        let lt = u64::from_le_bytes(lt_bytes);

        *reader = &reader[8..];

        Self { shard_ident, lt }
    }
}
