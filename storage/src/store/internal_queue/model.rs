use everscale_types::cell::HashBytes;
use everscale_types::models::ShardIdent;
use tycho_block_util::queue::{QueueKey, QueuePartition};

use crate::util::{StoredValue, StoredValueBuffer};
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ShardsInternalMessagesKey {
    pub shard_ident: ShardIdent,
    pub partition: QueuePartition,
    pub internal_message_key: QueueKey,
}

impl ShardsInternalMessagesKey {
    pub fn new(
        partition: QueuePartition,
        shard_ident: ShardIdent,
        internal_message_key: QueueKey,
    ) -> Self {
        Self {
            partition,
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
    const SIZE_HINT: usize = ShardIdent::SIZE_HINT + QueueKey::SIZE_HINT;

    type OnStackSlice = [u8; Self::SIZE_HINT];

    fn serialize<T: StoredValueBuffer>(&self, buffer: &mut T) {
        self.partition.serialize(buffer);
        self.shard_ident.serialize(buffer);
        self.internal_message_key.serialize(buffer);
    }

    fn deserialize(reader: &mut &[u8]) -> Self {
        if reader.len() < Self::SIZE_HINT {
            panic!("Insufficient data for deserialization")
        }

        let partition = QueuePartition::deserialize(reader);
        let shard_ident = ShardIdent::deserialize(reader);
        let internal_message_key = QueueKey::deserialize(reader);

        Self {
            partition,
            shard_ident,
            internal_message_key,
        }
    }
}

impl StoredValue for QueuePartition {
    const SIZE_HINT: usize = 1;

    type OnStackSlice = [u8; Self::SIZE_HINT];

    fn serialize<T: StoredValueBuffer>(&self, buffer: &mut T) {
        let value = match self {
            QueuePartition::NormalPriority => 0u8,
            QueuePartition::LowPriority => 1u8,
        };
        buffer.write_raw_slice(&[value]);
    }

    fn deserialize(reader: &mut &[u8]) -> Self
    where
        Self: Sized,
    {
        if reader.len() < Self::SIZE_HINT {
            panic!("Insufficient data for deserialization");
        }

        let value = reader[0];
        *reader = &reader[1..];

        match value {
            0 => QueuePartition::NormalPriority,
            1 => QueuePartition::LowPriority,
            _ => panic!("Unknown value for QueuePartition: {}", value),
        }
    }
}

impl StoredValue for QueueKey {
    const SIZE_HINT: usize = 8 + 32;

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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct StatKey {
    pub shard_ident: ShardIdent,
    pub partition: QueuePartition,
    pub min_message: QueueKey,
    pub max_message: QueueKey,
    pub index: u64,
}

impl StatKey {
    pub fn new(
        shard_ident: ShardIdent,
        partition: QueuePartition,
        min_message: QueueKey,
        max_message: QueueKey,
        index: u64,
    ) -> Self {
        Self {
            shard_ident,
            partition,
            min_message,
            max_message,
            index,
        }
    }
}

impl StoredValue for StatKey {
    const SIZE_HINT: usize = ShardIdent::SIZE_HINT
        + QueuePartition::SIZE_HINT
        + QueueKey::SIZE_HINT * 2
        + std::mem::size_of::<u64>();

    type OnStackSlice = [u8; ShardIdent::SIZE_HINT
        + QueuePartition::SIZE_HINT
        + QueueKey::SIZE_HINT * 2
        + std::mem::size_of::<u64>()];

    fn serialize<T: StoredValueBuffer>(&self, buffer: &mut T) {
        self.shard_ident.serialize(buffer);
        self.partition.serialize(buffer);
        self.min_message.serialize(buffer);
        self.max_message.serialize(buffer);
        buffer.write_raw_slice(&self.index.to_be_bytes());
    }

    fn deserialize(reader: &mut &[u8]) -> Self {
        if reader.len() < Self::SIZE_HINT {
            panic!("Insufficient data for deserialization");
        }

        let shard_ident = ShardIdent::deserialize(reader);
        let partition = QueuePartition::deserialize(reader);
        let min_message = QueueKey::deserialize(reader);
        let max_message = QueueKey::deserialize(reader);

        let mut index_bytes = [0u8; std::mem::size_of::<u64>()];
        index_bytes.copy_from_slice(&reader[..std::mem::size_of::<u64>()]);
        let index = u64::from_be_bytes(index_bytes);
        *reader = &reader[std::mem::size_of::<u64>()..];

        Self {
            shard_ident,
            partition,
            min_message,
            max_message,
            index,
        }
    }
}

pub struct QueueRange {
    pub shard_ident: ShardIdent,
    pub partition: QueuePartition,
    pub from: QueueKey,
    pub to: QueueKey,
}
