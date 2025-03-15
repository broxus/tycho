use std::collections::BTreeMap;

use everscale_types::cell::HashBytes;
use everscale_types::models::ShardIdent;
use tl_proto::{TlError, TlPacket, TlRead, TlResult, TlWrite};
use tycho_block_util::queue::{
    processed_to_map, router_partitions_map, QueueKey, QueuePartitionIdx, RouterAddr,
    RouterPartitions,
};
use tycho_block_util::tl;
use tycho_util::FastHashMap;

use crate::util::{StoredValue, StoredValueBuffer};

pub struct InternalQueueMessage<'a> {
    pub key: ShardsInternalMessagesKey,
    pub workchain: i8,
    pub prefix: u64,
    pub message_boc: &'a [u8],
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ShardsInternalMessagesKey {
    pub partition: QueuePartitionIdx,
    pub shard_ident: ShardIdent,
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
        buffer.write_raw_slice(&[self.workchain as u8]);
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct DiffTailKey {
    pub shard_ident: ShardIdent,
    pub max_message: QueueKey,
}

impl DiffTailKey {
    pub fn new(shard_ident: ShardIdent, max_message: QueueKey) -> Self {
        Self {
            shard_ident,
            max_message,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct DiffInfoKey {
    pub shard_ident: ShardIdent,
    pub seqno: u32,
}

impl DiffInfoKey {
    pub fn new(shard_ident: ShardIdent, seqno: u32) -> Self {
        Self { shard_ident, seqno }
    }
}

impl StoredValue for StatKey {
    const SIZE_HINT: usize = ShardIdent::SIZE_HINT
        + QueuePartitionIdx::SIZE_HINT
        + QueueKey::SIZE_HINT * 2
        + RouterAddr::SIZE_HINT;

    type OnStackSlice = [u8; Self::SIZE_HINT];

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

impl StoredValue for DiffTailKey {
    const SIZE_HINT: usize = ShardIdent::SIZE_HINT + QueueKey::SIZE_HINT;
    type OnStackSlice = [u8; Self::SIZE_HINT];

    fn serialize<T: StoredValueBuffer>(&self, buffer: &mut T) {
        self.shard_ident.serialize(buffer);
        self.max_message.serialize(buffer);
    }

    fn deserialize(reader: &mut &[u8]) -> Self
    where
        Self: Sized,
    {
        if reader.len() < Self::SIZE_HINT {
            panic!("Insufficient data for deserialization");
        }

        let shard_ident = ShardIdent::deserialize(reader);
        let max_message = QueueKey::deserialize(reader);

        Self {
            shard_ident,
            max_message,
        }
    }
}

impl StoredValue for DiffInfoKey {
    const SIZE_HINT: usize = ShardIdent::SIZE_HINT + 4;
    type OnStackSlice = [u8; Self::SIZE_HINT];

    fn serialize<T: StoredValueBuffer>(&self, buffer: &mut T) {
        self.shard_ident.serialize(buffer);
        buffer.write_raw_slice(&self.seqno.to_be_bytes());
    }

    fn deserialize(reader: &mut &[u8]) -> Self
    where
        Self: Sized,
    {
        if reader.len() < Self::SIZE_HINT {
            panic!("Insufficient data for deserialization");
        }

        let shard_ident = ShardIdent::deserialize(reader);
        let mut seqno_bytes = [0u8; 4];
        seqno_bytes.copy_from_slice(&reader[..4]);
        let seqno = u32::from_be_bytes(seqno_bytes);
        *reader = &reader[4..];

        Self { shard_ident, seqno }
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

#[test]
fn diff_info_key_serialization() {
    let key = DiffInfoKey::new(ShardIdent::MASTERCHAIN, 10);
    let mut buffer = Vec::with_capacity(DiffInfoKey::SIZE_HINT);
    key.serialize(&mut buffer);
    let key2 = DiffInfoKey::deserialize(&mut buffer.as_slice());
    assert_eq!(key, key2);
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiffInfo {
    pub min_message: QueueKey,
    pub max_message: QueueKey,
    pub shards_messages_count: FastHashMap<ShardIdent, u64>,
    pub hash: HashBytes,
    pub processed_to: BTreeMap<ShardIdent, QueueKey>,
    pub router_partitions_src: RouterPartitions,
    pub router_partitions_dst: RouterPartitions,
    pub seqno: u32,
}

impl DiffInfo {
    pub fn get_messages_count_by_shard(&self, shard_ident: &ShardIdent) -> u64 {
        self.shards_messages_count
            .get(shard_ident)
            .copied()
            .unwrap_or_default()
    }
}

impl TlWrite for DiffInfo {
    type Repr = tl_proto::Boxed;

    fn max_size_hint(&self) -> usize {
        QueueKey::SIZE_HINT
            + QueueKey::SIZE_HINT
            + QueueKey::SIZE_HINT
            + 4
            + self.shards_messages_count.len() * (tl::shard_ident::SIZE_HINT + 8)
            + tl::hash_bytes::SIZE_HINT
    }

    fn write_to<P: TlPacket>(&self, packet: &mut P) {
        self.min_message.write_to(packet);
        self.max_message.write_to(packet);
        packet.write_u32(self.shards_messages_count.len() as u32);

        for (shard_ident, count) in &self.shards_messages_count {
            tl::shard_ident::write(shard_ident, packet);
            packet.write_u64(*count);
        }

        tl::hash_bytes::write(&self.hash, packet);

        processed_to_map::write(&self.processed_to, packet);
        router_partitions_map::write(&self.router_partitions_src, packet);
        router_partitions_map::write(&self.router_partitions_dst, packet);
        packet.write_u32(self.seqno);
    }
}

impl<'tl> TlRead<'tl> for DiffInfo {
    type Repr = tl_proto::Boxed;

    fn read_from(data: &mut &'tl [u8]) -> TlResult<Self> {
        let min_message = QueueKey::read_from(data)?;
        let max_message = QueueKey::read_from(data)?;

        let len = u32::read_from(data)? as usize;
        if len > 10_000_000 {
            return Err(TlError::InvalidData);
        }

        let mut shards_messages_count =
            FastHashMap::with_capacity_and_hasher(len, Default::default());

        for _ in 0..len {
            let shard_ident = tl::shard_ident::read(data)?;
            let count = u64::read_from(data)?;
            shards_messages_count.insert(shard_ident, count);
        }

        let hash = tl::hash_bytes::read(data)?;

        let processed_to = processed_to_map::read(data)?;
        let router_partitions_src = router_partitions_map::read(data)?;
        let router_partitions_dst = router_partitions_map::read(data)?;
        let seqno = u32::read_from(data)?;

        Ok(DiffInfo {
            min_message,
            max_message,
            shards_messages_count,
            hash,
            processed_to,
            router_partitions_src,
            router_partitions_dst,
            seqno,
        })
    }
}

#[derive(Debug, Clone)]
pub struct CommitPointerKey {
    pub shard_ident: ShardIdent,
}

#[derive(Debug, Clone, Default)]

pub struct CommitPointerValue {
    pub queue_key: QueueKey,
    pub seqno: u32,
}

impl StoredValue for CommitPointerKey {
    const SIZE_HINT: usize = ShardIdent::SIZE_HINT;

    type OnStackSlice = [u8; Self::SIZE_HINT];

    fn serialize<T: StoredValueBuffer>(&self, buffer: &mut T) {
        self.shard_ident.serialize(buffer);
    }

    fn deserialize(reader: &mut &[u8]) -> Self {
        if reader.len() < Self::SIZE_HINT {
            panic!("Insufficient data for deserialization");
        }

        let shard_ident = ShardIdent::deserialize(reader);

        Self { shard_ident }
    }
}

impl StoredValue for CommitPointerValue {
    const SIZE_HINT: usize = QueueKey::SIZE_HINT + 4;

    type OnStackSlice = [u8; Self::SIZE_HINT];

    fn serialize<T: StoredValueBuffer>(&self, buffer: &mut T) {
        self.queue_key.serialize(buffer);
        buffer.write_raw_slice(&self.seqno.to_be_bytes());
    }

    fn deserialize(reader: &mut &[u8]) -> Self {
        if reader.len() < Self::SIZE_HINT {
            panic!("Insufficient data for deserialization");
        }

        let queue_key = QueueKey::deserialize(reader);
        let mut seqno_bytes = [0u8; 4];
        seqno_bytes.copy_from_slice(&reader[..4]);
        let seqno = u32::from_be_bytes(seqno_bytes);

        Self { queue_key, seqno }
    }
}
