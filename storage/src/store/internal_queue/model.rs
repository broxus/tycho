use everscale_types::cell::HashBytes;
use everscale_types::models::ShardIdent;

use crate::util::{StoredValue, StoredValueBuffer};

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ShardsInternalMessagesKey {
    pub shard_ident: ShardIdent,
    pub internal_message_key: InternalMessageKey,
}

impl ShardsInternalMessagesKey {
    pub fn new(shard_ident: ShardIdent, internal_message_key: InternalMessageKey) -> Self {
        Self {
            shard_ident,
            internal_message_key,
        }
    }
}

impl From<&[u8]> for ShardsInternalMessagesKey {
    fn from(bytes: &[u8]) -> Self {
        let mut reader = bytes;
        Self::deserialize(&mut reader)
    }
}

impl StoredValue for ShardsInternalMessagesKey {
    const SIZE_HINT: usize = ShardIdent::SIZE_HINT + InternalMessageKey::SIZE_HINT;

    type OnStackSlice = [u8; Self::SIZE_HINT];

    fn serialize<T: StoredValueBuffer>(&self, buffer: &mut T) {
        self.shard_ident.serialize(buffer);
        self.internal_message_key.serialize(buffer);
    }

    fn deserialize(reader: &mut &[u8]) -> Self {
        if reader.len() < Self::SIZE_HINT {
            panic!("Insufficient data for deserialization")
        }

        let shard_ident = ShardIdent::deserialize(reader);
        let internal_message_key = InternalMessageKey::deserialize(reader);

        Self {
            shard_ident,
            internal_message_key,
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct InternalMessageKey {
    pub lt: u64,
    pub hash: HashBytes,
}

impl InternalMessageKey {
    const SIZE_HINT: usize = 8 + 32;
}

impl StoredValue for InternalMessageKey {
    const SIZE_HINT: usize = Self::SIZE_HINT;

    type OnStackSlice = [u8; Self::SIZE_HINT];

    fn serialize<T: StoredValueBuffer>(&self, buffer: &mut T) {
        buffer.write_raw_slice(&self.lt.to_be_bytes());
        buffer.write_raw_slice(&self.hash.0);
    }

    fn deserialize(reader: &mut &[u8]) -> Self {
        if reader.len() < Self::SIZE_HINT {
            panic!("Insufficient data for deserialization")
        }

        let mut lt_bytes = [0u8; 8];
        lt_bytes.copy_from_slice(&reader[..8]);
        let lt = u64::from_be_bytes(lt_bytes);
        *reader = &reader[8..];

        let mut hash_bytes = [0u8; 32];
        hash_bytes.copy_from_slice(&reader[..32]);
        let hash = HashBytes(hash_bytes);
        *reader = &reader[32..];

        Self { lt, hash }
    }
}
