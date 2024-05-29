use everscale_types::cell::HashBytes;
use everscale_types::models::ShardIdent;

use crate::util::{StoredValue, StoredValueBuffer};

pub struct InternalMessagesKey {
    pub lt: u64,
    pub hash: HashBytes,
}
impl StoredValue for InternalMessagesKey {
    const SIZE_HINT: usize = 40;
    type OnStackSlice = [u8; Self::SIZE_HINT];

    fn serialize<T: StoredValueBuffer>(&self, buffer: &mut T) {
        buffer.write_raw_slice(&self.lt.to_le_bytes());
        buffer.write_raw_slice(&self.hash.0);
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

        Self { lt, hash }
    }
}

pub struct ShardsInternalMessagesKey {
    pub shard_ident: ShardIdent,
    pub lt: u64,
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

        *reader = &reader[8..]; // Advance the reader

        ShardsInternalMessagesKey { shard_ident, lt }
    }
}
