use everscale_types::cell::HashBytes;
use everscale_types::models::ShardIdent;
use tycho_block_util::queue::{QueueKey, QueuePartitionIdx, RouterAddr};

use crate::util::{StoredValue, StoredValueBuffer};
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ShardsInternalMessagesKey {
    pub shard_ident: ShardIdent,
    pub partition: QueuePartitionIdx,
    pub internal_message_key: QueueKey,
}

impl ShardsInternalMessagesKey {
    pub fn new(
        partition: QueuePartitionIdx,
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

        let partition = QueuePartitionIdx::deserialize(reader);
        let shard_ident = ShardIdent::deserialize(reader);
        let internal_message_key = QueueKey::deserialize(reader);

        Self {
            partition,
            shard_ident,
            internal_message_key,
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

impl StoredValue for RouterAddr {
    const SIZE_HINT: usize = 1 + 32;
    type OnStackSlice = [u8; Self::SIZE_HINT];

    fn serialize<T: StoredValueBuffer>(&self, buffer: &mut T) {
        buffer.write_raw_slice(&self.workchain.to_be_bytes());
        buffer.write_raw_slice(&self.account.0);
    }

    fn deserialize(reader: &mut &[u8]) -> Self
    where
        Self: Sized,
    {
        if reader.len() < Self::SIZE_HINT {
            panic!("Insufficient data for deserialization");
        }

        let workchain = reader[0] as i8;
        *reader = &reader[1..];

        let mut account_bytes = [0u8; 32];
        account_bytes.copy_from_slice(&reader[..32]);
        let account = HashBytes(account_bytes);
        *reader = &reader[32..];

        Self { workchain, account }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct StatKey {
    pub shard_ident: ShardIdent,
    pub partition: QueuePartitionIdx,
    pub min_message: QueueKey,
    pub max_message: QueueKey,
    pub dest: RouterAddr,
}

impl StatKey {
    pub fn new(
        shard_ident: ShardIdent,
        partition: QueuePartitionIdx,
        min_message: QueueKey,
        max_message: QueueKey,
        dest: RouterAddr,
    ) -> Self {
        Self {
            shard_ident,
            partition,
            min_message,
            max_message,
            dest,
        }
    }
}

impl StoredValue for StatKey {
    const SIZE_HINT: usize = ShardIdent::SIZE_HINT
        + QueuePartitionIdx::SIZE_HINT
        + QueueKey::SIZE_HINT * 2
        + RouterAddr::SIZE_HINT;

    type OnStackSlice = [u8; ShardIdent::SIZE_HINT
        + QueuePartitionIdx::SIZE_HINT
        + QueueKey::SIZE_HINT * 2
        + RouterAddr::SIZE_HINT];

    fn serialize<T: StoredValueBuffer>(&self, buffer: &mut T) {
        self.shard_ident.serialize(buffer);
        self.partition.serialize(buffer);
        self.min_message.serialize(buffer);
        self.max_message.serialize(buffer);
        self.dest.serialize(buffer);
    }

    fn deserialize(reader: &mut &[u8]) -> Self {
        if reader.len() < Self::SIZE_HINT {
            panic!("Insufficient data for deserialization");
        }

        let shard_ident = ShardIdent::deserialize(reader);
        let partition = QueuePartitionIdx::deserialize(reader);
        let min_message = QueueKey::deserialize(reader);
        let max_message = QueueKey::deserialize(reader);
        let dest = RouterAddr::deserialize(reader);

        Self {
            shard_ident,
            partition,
            min_message,
            max_message,
            dest,
        }
    }
}

pub struct QueueRange {
    pub shard_ident: ShardIdent,
    pub partition: QueuePartitionIdx,
    pub from: QueueKey,
    pub to: QueueKey,
}

impl StoredValue for QueuePartitionIdx {
    const SIZE_HINT: usize = std::mem::size_of::<QueuePartitionIdx>();

    type OnStackSlice = [u8; Self::SIZE_HINT];

    fn serialize<T: StoredValueBuffer>(&self, buffer: &mut T) {
        buffer.write_raw_slice(&self.to_be_bytes());
    }

    fn deserialize(reader: &mut &[u8]) -> Self {
        if reader.len() < Self::SIZE_HINT {
            panic!("Insufficient data for deserialization");
        }

        let mut partition_bytes = [0u8; 2];
        partition_bytes.copy_from_slice(&reader[..2]);
        let partition = u16::from_be_bytes(partition_bytes);
        *reader = &reader[2..];

        partition
    }
}
