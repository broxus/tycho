use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;

use bytes::Bytes;
use tl_proto::{TlError, TlRead, TlWrite};
use tycho_types::models::*;
use tycho_types::prelude::*;

use crate::tl;

/// Representation of an internal messages queue diff.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueDiff {
    /// Computed hash of this diff.
    ///
    /// NOTE: This field is not serialized and can be [`HashBytes::ZERO`] for serialization.
    pub hash: HashBytes,

    /// Hash of the TL repr of the previous queue diff.
    pub prev_hash: HashBytes,
    /// Shard identifier of the corresponding block
    pub shard_ident: ShardIdent,
    /// Seqno of the corresponding block.
    pub seqno: u32,
    /// collator boundaries.
    pub processed_to: BTreeMap<ShardIdent, QueueKey>,
    /// Min message queue key.
    pub min_message: QueueKey,
    /// Max message queue key.
    pub max_message: QueueKey,
    /// List of message hashes (sorted ASC).
    pub messages: Vec<HashBytes>,
    /// Inbound router partitions.
    pub router_partitions_src: RouterPartitions,
    /// Outbound router partitions.
    pub router_partitions_dst: RouterPartitions,
}

impl QueueDiff {
    pub const TL_ID: u32 = tl_proto::id!("block.queueDiff", scheme = "proto.tl");

    /// Recomputes the hash of the diff.
    ///
    /// NOTE: Since the hash is not serialized, it is NOT mandatory to call this method
    /// if it will not be used after this.
    pub fn recompute_hash(&mut self) {
        self.hash = Self::compute_hash(&tl_proto::serialize(&*self));
    }

    /// Computes the hash of the serialized diff.
    pub fn compute_hash(data: &[u8]) -> HashBytes {
        Boc::file_hash_blake(data)
    }
}

impl TlWrite for QueueDiff {
    type Repr = tl_proto::Boxed;

    fn max_size_hint(&self) -> usize {
        4 + tl::hash_bytes::SIZE_HINT
            + tl::shard_ident::SIZE_HINT
            + 4
            + processed_to_map::size_hint(&self.processed_to)
            + 2 * QueueKey::SIZE_HINT
            + messages_list::size_hint(&self.messages)
            + router_partitions_map::size_hint(&self.router_partitions_src)
            + router_partitions_map::size_hint(&self.router_partitions_dst)
    }

    fn write_to<P>(&self, packet: &mut P)
    where
        P: tl_proto::TlPacket,
    {
        packet.write_u32(Self::TL_ID);
        tl::hash_bytes::write(&self.prev_hash, packet);
        tl::shard_ident::write(&self.shard_ident, packet);
        packet.write_u32(self.seqno);
        processed_to_map::write(&self.processed_to, packet);
        self.min_message.write_to(packet);
        self.max_message.write_to(packet);
        messages_list::write(&self.messages, packet);
        router_partitions_map::write(&self.router_partitions_src, packet);
        router_partitions_map::write(&self.router_partitions_dst, packet);
    }
}

impl<'tl> TlRead<'tl> for QueueDiff {
    type Repr = tl_proto::Boxed;

    fn read_from(data: &mut &'tl [u8]) -> tl_proto::TlResult<Self> {
        let data_before = *data;
        if u32::read_from(data)? != Self::TL_ID {
            return Err(tl_proto::TlError::UnknownConstructor);
        }

        let mut result = Self {
            hash: HashBytes::ZERO,
            prev_hash: tl::hash_bytes::read(data)?,
            shard_ident: tl::shard_ident::read(data)?,
            seqno: u32::read_from(data)?,
            processed_to: processed_to_map::read(data)?,
            min_message: QueueKey::read_from(data)?,
            max_message: QueueKey::read_from(data)?,
            messages: messages_list::read(data)?,
            router_partitions_src: router_partitions_map::read(data)?,
            router_partitions_dst: router_partitions_map::read(data)?,
        };

        if result.max_message < result.min_message {
            return Err(tl_proto::TlError::InvalidData);
        }

        let result_bytes = data_before.len() - data.len();

        // Compute the hash of the diff
        result.hash = Self::compute_hash(&data_before[..result_bytes]);

        Ok(result)
    }
}

/// Persistent internal messages queue state.
#[derive(Clone, PartialEq, Eq, TlWrite, TlRead)]
#[tl(boxed, id = "block.queueState", scheme = "proto.tl")]
pub struct QueueState {
    pub header: QueueStateHeader,

    /// Chunks of messages in the same order as messages in `header.queue_diffs.messages`.
    /// Only the order is guaranteed, but not the chunk sizes.
    #[tl(with = "state_messages_list")]
    pub messages: Vec<Bytes>,
}

/// Persistent internal messages queue state.
///
/// A non-owned version of [`QueueState`].
#[derive(Clone, PartialEq, Eq, TlWrite, TlRead)]
#[tl(boxed, id = "block.queueState", scheme = "proto.tl")]
pub struct QueueStateRef<'tl> {
    pub header: QueueStateHeader,

    /// Chunks of messages in the same order as messages in `header.queue_diffs.messages`.
    /// Only the order is guaranteed, but not the chunk sizes.
    #[tl(with = "state_messages_list_ref")]
    pub messages: Vec<&'tl [u8]>,
}

/// A header for a persistent internal messages queue state.
#[derive(Debug, Clone, PartialEq, Eq, TlWrite, TlRead)]
#[tl(boxed, id = "block.queueStateHeader", scheme = "proto.tl")]
pub struct QueueStateHeader {
    #[tl(with = "tl::shard_ident")]
    pub shard_ident: ShardIdent,
    pub seqno: u32,
    #[tl(with = "queue_diffs_list")]
    pub queue_diffs: Vec<QueueDiff>,
}

/// Queue key.
#[derive(Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, TlWrite, TlRead)]
pub struct QueueKey {
    pub lt: u64,
    #[tl(with = "tl::hash_bytes")]
    pub hash: HashBytes,
}

impl QueueKey {
    pub const SIZE_HINT: usize = 8 + 32;

    pub const MIN: Self = Self {
        lt: 0,
        hash: HashBytes::ZERO,
    };

    pub const MAX: Self = Self {
        lt: u64::MAX,
        hash: HashBytes([0xff; 32]),
    };

    pub const fn min_for_lt(lt: u64) -> Self {
        Self {
            lt,
            hash: HashBytes::ZERO,
        }
    }

    pub const fn max_for_lt(lt: u64) -> Self {
        Self {
            lt,
            hash: HashBytes([0xff; 32]),
        }
    }

    // add min step to the key
    pub fn next_value(&self) -> Self {
        let mut new_lt = self.lt;
        let mut new_hash = self.hash;

        if new_hash.0 == [0xff; 32] {
            // check if lt is already max then do nothing
            if new_lt == u64::MAX {
                return Self {
                    lt: u64::MAX,
                    hash: HashBytes([0xff; 32]),
                };
            } else {
                new_lt += 1;
                new_hash = HashBytes::ZERO;
            }
        } else {
            let carry = 1u8;
            for byte in new_hash.0.iter_mut().rev() {
                let (res, overflow) = byte.overflowing_add(carry);
                *byte = res;
                if !overflow {
                    break;
                }
            }
        }

        Self {
            lt: new_lt,
            hash: new_hash,
        }
    }

    #[inline]
    pub const fn split(self) -> (u64, HashBytes) {
        (self.lt, self.hash)
    }
}

impl From<(u64, HashBytes)> for QueueKey {
    #[inline]
    fn from((lt, hash): (u64, HashBytes)) -> Self {
        Self { lt, hash }
    }
}

impl From<QueueKey> for (u64, HashBytes) {
    #[inline]
    fn from(key: QueueKey) -> Self {
        key.split()
    }
}

impl std::fmt::Debug for QueueKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl std::fmt::Display for QueueKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let short_hash = get_short_hash_string(&self.hash);

        write!(f, "LT_HASH({}_{short_hash})", self.lt)
    }
}

#[cfg(feature = "storage")]
impl tycho_storage_traits::StoredValue for QueueKey {
    const SIZE_HINT: usize = 8 + 32;

    type OnStackSlice = [u8; Self::SIZE_HINT];

    fn serialize<T: tycho_storage_traits::StoredValueBuffer>(&self, buffer: &mut T) {
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

pub fn get_short_hash_string(hash: &HashBytes) -> String {
    let mut short_hash = [0u8; 8];
    hex::encode_to_slice(&hash.as_array()[..4], &mut short_hash).ok();

    // SAFETY: output is guaranteed to contain only [0-9a-f]
    let res = unsafe { std::str::from_utf8_unchecked(&short_hash) };

    res.to_owned()
}

pub fn get_short_addr_string(addr: &IntAddr) -> String {
    match addr {
        IntAddr::Std(addr) => {
            let addr_hash_short = get_short_hash_string(&addr.address);
            format!("{}:{}", addr.workchain, addr_hash_short)
        }
        IntAddr::Var(_) => unreachable!(),
    }
}

/// Std dest addr
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RouterAddr {
    pub workchain: i8,
    pub account: HashBytes,
}

impl RouterAddr {
    pub const MIN: Self = Self {
        workchain: i8::MIN,
        account: HashBytes::ZERO,
    };

    pub const MAX: Self = Self {
        workchain: i8::MAX,
        account: HashBytes([0xff; 32]),
    };

    const TL_SIZE_HINT: usize = 4 + 32;

    pub fn to_int_addr(&self) -> IntAddr {
        IntAddr::Std(StdAddr {
            anycast: None,
            workchain: self.workchain,
            address: self.account,
        })
    }

    pub fn from_int_addr(addr: &IntAddr) -> Option<Self> {
        match addr {
            IntAddr::Std(addr) => Some(Self::from(addr)),
            IntAddr::Var(_) => None,
        }
    }
}

impl From<&StdAddr> for RouterAddr {
    fn from(value: &StdAddr) -> Self {
        Self {
            workchain: value.workchain,
            account: value.address,
        }
    }
}

impl From<StdAddr> for RouterAddr {
    fn from(value: StdAddr) -> Self {
        Self::from(&value)
    }
}

impl TlWrite for RouterAddr {
    type Repr = tl_proto::Boxed;

    fn max_size_hint(&self) -> usize {
        Self::TL_SIZE_HINT
    }

    fn write_to<P>(&self, packet: &mut P)
    where
        P: tl_proto::TlPacket,
    {
        packet.write_i32(self.workchain as i32);
        tl::hash_bytes::write(&self.account, packet);
    }
}

impl<'tl> TlRead<'tl> for RouterAddr {
    type Repr = tl_proto::Boxed;

    fn read_from(data: &mut &'tl [u8]) -> tl_proto::TlResult<Self> {
        let Ok(workchain) = i32::read_from(data)?.try_into() else {
            return Err(TlError::InvalidData);
        };
        let account = tl::hash_bytes::read(data)?;

        Ok(Self { workchain, account })
    }
}

#[cfg(feature = "storage")]
impl tycho_storage_traits::StoredValue for RouterAddr {
    const SIZE_HINT: usize = 1 + 32;
    type OnStackSlice = [u8; Self::SIZE_HINT];

    fn serialize<T: tycho_storage_traits::StoredValueBuffer>(&self, buffer: &mut T) {
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

#[derive(Default, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct QueuePartitionIdx(pub u16);

impl QueuePartitionIdx {
    pub const ZERO: Self = Self(0);
    pub const MIN: Self = Self(u16::MIN);
    pub const MAX: Self = Self(u16::MAX);

    pub const fn is_zero(self) -> bool {
        self.0 == 0
    }
}

impl PartialEq<QueuePartitionIdx> for u16 {
    #[inline]
    fn eq(&self, other: &QueuePartitionIdx) -> bool {
        *self == other.0
    }
}

impl PartialEq<&QueuePartitionIdx> for u16 {
    #[inline]
    fn eq(&self, other: &&QueuePartitionIdx) -> bool {
        *self == other.0
    }
}

impl PartialEq<u16> for QueuePartitionIdx {
    #[inline]
    fn eq(&self, other: &u16) -> bool {
        self.0 == *other
    }
}

impl PartialEq<&u16> for QueuePartitionIdx {
    #[inline]
    fn eq(&self, other: &&u16) -> bool {
        self.0 == **other
    }
}

impl std::fmt::Display for QueuePartitionIdx {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl std::fmt::Debug for QueuePartitionIdx {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl From<u16> for QueuePartitionIdx {
    #[inline]
    fn from(value: u16) -> Self {
        Self(value)
    }
}

impl From<QueuePartitionIdx> for u16 {
    #[inline]
    fn from(value: QueuePartitionIdx) -> Self {
        value.0
    }
}

#[cfg(feature = "storage")]
impl tycho_storage_traits::StoredValue for QueuePartitionIdx {
    const SIZE_HINT: usize = std::mem::size_of::<QueuePartitionIdx>();

    type OnStackSlice = [u8; Self::SIZE_HINT];

    fn serialize<T: tycho_storage_traits::StoredValueBuffer>(&self, buffer: &mut T) {
        buffer.write_raw_slice(&self.0.to_be_bytes());
    }

    fn deserialize(reader: &mut &[u8]) -> Self {
        if reader.len() < Self::SIZE_HINT {
            panic!("Insufficient data for deserialization");
        }

        let mut partition_bytes = [0u8; 2];
        partition_bytes.copy_from_slice(&reader[..2]);
        let partition = u16::from_be_bytes(partition_bytes);
        *reader = &reader[2..];

        Self(partition)
    }
}

pub type RouterPartitions = BTreeMap<QueuePartitionIdx, BTreeSet<RouterAddr>>;

pub mod processed_to_map {
    use tl_proto::{TlPacket, TlResult};

    use super::*;

    /// We assume that the number of processed shards is limited.
    const MAX_SIZE: usize = 1000;

    pub fn size_hint(items: &BTreeMap<ShardIdent, QueueKey>) -> usize {
        const PER_ITEM: usize = tl::shard_ident::SIZE_HINT + QueueKey::SIZE_HINT;

        4 + items.len() * PER_ITEM
    }

    pub fn write<P: TlPacket>(items: &BTreeMap<ShardIdent, QueueKey>, packet: &mut P) {
        packet.write_u32(items.len() as u32);

        for (shard_ident, processed_to) in items {
            tl::shard_ident::write(shard_ident, packet);
            processed_to.write_to(packet);
        }
    }

    pub fn read(data: &mut &[u8]) -> TlResult<BTreeMap<ShardIdent, QueueKey>> {
        let len = u32::read_from(data)? as usize;
        if len > MAX_SIZE {
            return Err(tl_proto::TlError::InvalidData);
        }

        let mut items = BTreeMap::new();
        let mut prev_shard = None;
        for _ in 0..len {
            let shard_ident = tl::shard_ident::read(data)?;

            // Require that shards are sorted in ascending order.
            if let Some(prev_shard) = prev_shard
                && shard_ident <= prev_shard
            {
                return Err(tl_proto::TlError::InvalidData);
            }
            prev_shard = Some(shard_ident);

            // Read the rest of the entry.
            items.insert(shard_ident, QueueKey::read_from(data)?);
        }

        Ok(items)
    }
}

pub mod router_partitions_map {
    use tl_proto::{TlPacket, TlResult};

    use super::*;

    const MAX_ADDRS_PER_PARTITION: u32 = 10_000_000;

    pub fn size_hint(items: &BTreeMap<QueuePartitionIdx, BTreeSet<RouterAddr>>) -> usize {
        let mut size = 4;
        for accounts in items.values() {
            size += 4 + 4 + accounts.len() * RouterAddr::TL_SIZE_HINT;
        }
        size
    }

    pub fn write<P>(items: &BTreeMap<QueuePartitionIdx, BTreeSet<RouterAddr>>, packet: &mut P)
    where
        P: TlPacket,
    {
        packet.write_u32(items.len() as u32);
        for (partition, accounts) in items {
            packet.write_u32(partition.0 as u32);
            packet.write_u32(accounts.len() as u32);
            for account in accounts {
                account.write_to(packet);
            }
        }
    }

    pub fn read(data: &mut &[u8]) -> TlResult<BTreeMap<QueuePartitionIdx, BTreeSet<RouterAddr>>> {
        let partition_count = u32::read_from(data)?;
        if partition_count > u16::MAX as u32 + 1 {
            return Err(TlError::InvalidData);
        }

        let mut partitions = BTreeMap::new();

        let mut prev_index = None;
        for _ in 0..partition_count {
            let Ok::<u16, _>(partition_index) = u32::read_from(data)?.try_into() else {
                return Err(TlError::InvalidData);
            };

            if let Some(prev_index) = prev_index
                && partition_index <= prev_index
            {
                return Err(TlError::InvalidData);
            }
            prev_index = Some(partition_index);

            let account_count = u32::read_from(data)?;
            if account_count > MAX_ADDRS_PER_PARTITION {
                return Err(TlError::InvalidData);
            }

            let mut accounts = BTreeSet::new();
            let mut prev_account = None;
            for _ in 0..account_count {
                let account = RouterAddr::read_from(data)?;

                if let Some(prev_account) = &prev_account
                    && &account <= prev_account
                {
                    return Err(TlError::InvalidData);
                }
                prev_account = Some(account);

                let is_unique = accounts.insert(account);
                debug_assert!(is_unique);
            }

            partitions.insert(partition_index.into(), accounts);
        }

        Ok(partitions)
    }
}

mod queue_diffs_list {
    use tl_proto::{TlPacket, TlResult};

    use super::*;

    /// We assume that the number of queue diffs is limited.
    const MAX_SIZE: usize = 1000;

    pub fn size_hint(items: &[QueueDiff]) -> usize {
        4 + items.iter().map(TlWrite::max_size_hint).sum::<usize>()
    }

    pub fn write<P: TlPacket>(items: &[QueueDiff], packet: &mut P) {
        packet.write_u32(items.len() as u32);

        let mut iter = items.iter();
        let mut latest_diff = iter.next().expect("diffs list must not be empty");
        latest_diff.write_to(packet);

        for diff in iter {
            // Require that diffs are sorted by descending seqno.
            debug_assert!(latest_diff.seqno > diff.seqno);
            debug_assert_eq!(latest_diff.prev_hash, diff.hash);
            latest_diff = diff;

            diff.write_to(packet);
        }
    }

    pub fn read(data: &mut &[u8]) -> TlResult<Vec<QueueDiff>> {
        let len = u32::read_from(data)? as usize;
        if len > MAX_SIZE || len == 0 {
            return Err(tl_proto::TlError::InvalidData);
        }

        let mut items = Vec::with_capacity(len);
        items.push(QueueDiff::read_from(data)?);

        let (mut latest_seqno, mut prev_hash) = {
            let latest = items.first().expect("always not empty");
            (latest.seqno, latest.prev_hash)
        };

        for _ in 1..len {
            let diff = QueueDiff::read_from(data)?;

            // Require that diffs are sorted by descending seqno.
            if latest_seqno <= diff.seqno || prev_hash != diff.hash {
                return Err(tl_proto::TlError::InvalidData);
            }
            latest_seqno = diff.seqno;
            prev_hash = diff.prev_hash;

            items.push(diff);
        }

        Ok(items)
    }
}

mod messages_list {
    use super::*;

    /// We assume that the number of messages is limited.
    const MAX_SIZE: usize = 10_000_000;

    pub fn size_hint(items: &[HashBytes]) -> usize {
        4 + items.len() * tl::hash_bytes::SIZE_HINT
    }

    pub fn write<P: tl_proto::TlPacket>(items: &[HashBytes], packet: &mut P) {
        static ZERO_HASH: HashBytes = HashBytes::ZERO;

        packet.write_u32(items.len() as u32);

        let mut prev_hash = &ZERO_HASH;
        for item in items {
            // Require that messages are sorted by ascending hash.
            debug_assert!(prev_hash < item);
            prev_hash = item;

            tl::hash_bytes::write(item, packet);
        }
    }

    pub fn read(data: &mut &[u8]) -> tl_proto::TlResult<Vec<HashBytes>> {
        let len = u32::read_from(data)? as usize;
        if len > MAX_SIZE {
            return Err(tl_proto::TlError::InvalidData);
        }

        let mut items = Vec::with_capacity(len);

        let mut prev_hash = HashBytes::ZERO;
        for _ in 0..len {
            // NOTE: Assume that there are no messages with zero hash.
            let hash = tl::hash_bytes::read(data)?;

            // Require that messages are sorted by ascending hash.
            if hash <= prev_hash {
                return Err(tl_proto::TlError::InvalidData);
            }
            prev_hash = hash;

            items.push(hash);
        }

        Ok(items)
    }
}

mod state_messages_list {
    use super::*;

    /// We assume that the number of chunks is limited.
    pub const MAX_CHUNKS: usize = 10_000_000;

    pub const MAX_CHUNK_SIZE: usize = 100 << 20; // 100 MB

    pub type BigBytes = tycho_util::tl::BigBytes<MAX_CHUNK_SIZE>;

    pub fn size_hint(items: &[Bytes]) -> usize {
        4 + items.iter().map(BigBytes::size_hint).sum::<usize>()
    }

    pub fn write<P: tl_proto::TlPacket>(items: &[Bytes], packet: &mut P) {
        packet.write_u32(items.len() as u32);
        for item in items {
            BigBytes::write(item, packet);
        }
    }

    pub fn read(data: &mut &[u8]) -> tl_proto::TlResult<Vec<Bytes>> {
        let len = u32::read_from(data)? as usize;
        if len > MAX_CHUNKS {
            return Err(tl_proto::TlError::InvalidData);
        }

        let mut items = Vec::with_capacity(len);
        for _ in 0..len {
            items.push(BigBytes::read(data)?);
        }

        Ok(items)
    }
}

mod state_messages_list_ref {
    use super::state_messages_list::{MAX_CHUNK_SIZE, MAX_CHUNKS};
    use super::*;

    type BigBytesRef = tycho_util::tl::BigBytesRef<MAX_CHUNK_SIZE>;

    pub fn size_hint(items: &[&[u8]]) -> usize {
        4 + items.iter().map(BigBytesRef::size_hint).sum::<usize>()
    }

    pub fn write<P: tl_proto::TlPacket>(items: &[&[u8]], packet: &mut P) {
        packet.write_u32(items.len() as u32);
        for item in items {
            BigBytesRef::write(item, packet);
        }
    }

    pub fn read<'tl>(data: &mut &'tl [u8]) -> tl_proto::TlResult<Vec<&'tl [u8]>> {
        let len = u32::read_from(data)? as usize;
        if len > MAX_CHUNKS {
            return Err(tl_proto::TlError::InvalidData);
        }

        let mut items = Vec::with_capacity(len);
        for _ in 0..len {
            items.push(BigBytesRef::read(data)?);
        }

        Ok(items)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_diff_binary_repr() {
        let addr1 = RouterAddr {
            workchain: 0,
            account: HashBytes::from([0x01; 32]),
        };

        let addr2 = RouterAddr {
            workchain: 1,
            account: HashBytes::from([0x02; 32]),
        };

        let mut diff = QueueDiff {
            hash: HashBytes::ZERO, // NOTE: Uninitialized
            prev_hash: HashBytes::from([0x33; 32]),
            shard_ident: ShardIdent::MASTERCHAIN,
            seqno: 123,
            processed_to: BTreeMap::from([
                (ShardIdent::MASTERCHAIN, QueueKey {
                    lt: 1,
                    hash: HashBytes::from([0x11; 32]),
                }),
                (ShardIdent::BASECHAIN, QueueKey {
                    lt: 123123,
                    hash: HashBytes::from([0x22; 32]),
                }),
            ]),
            min_message: QueueKey {
                lt: 1,
                hash: HashBytes::from([0x11; 32]),
            },
            max_message: QueueKey {
                lt: 123,
                hash: HashBytes::from([0x22; 32]),
            },
            messages: vec![
                HashBytes::from([0x01; 32]),
                HashBytes::from([0x02; 32]),
                HashBytes::from([0x03; 32]),
            ],
            router_partitions_src: Default::default(),
            router_partitions_dst: BTreeMap::from([(1.into(), BTreeSet::from([addr1, addr2]))]),
        };

        let bytes = tl_proto::serialize(&diff);
        assert_eq!(bytes.len(), diff.max_size_hint());

        let parsed = tl_proto::deserialize::<QueueDiff>(&bytes).unwrap();
        assert_eq!(parsed.hash, QueueDiff::compute_hash(&bytes));

        diff.hash = parsed.hash;
        assert_eq!(diff, parsed);
    }

    #[test]
    fn queue_state_binary_repr() {
        let addr1 = RouterAddr {
            workchain: 0,
            account: HashBytes::from([0x01; 32]),
        };

        let addr2 = RouterAddr {
            workchain: 1,
            account: HashBytes::from([0x02; 32]),
        };

        let mut queue_diffs = Vec::<QueueDiff>::new();
        for seqno in 1..=10 {
            let prev_hash = queue_diffs.last().map(|diff| diff.hash).unwrap_or_default();

            let mut diff = QueueDiff {
                hash: HashBytes::ZERO, // NOTE: Uninitialized
                prev_hash,
                shard_ident: ShardIdent::MASTERCHAIN,
                seqno,
                processed_to: BTreeMap::from([
                    (ShardIdent::MASTERCHAIN, QueueKey {
                        lt: 10 * seqno as u64,
                        hash: HashBytes::from([seqno as u8; 32]),
                    }),
                    (ShardIdent::BASECHAIN, QueueKey {
                        lt: 123123 * seqno as u64,
                        hash: HashBytes::from([seqno as u8 * 2; 32]),
                    }),
                ]),
                min_message: QueueKey {
                    lt: 1,
                    hash: HashBytes::from([0x11; 32]),
                },
                max_message: QueueKey {
                    lt: 123,
                    hash: HashBytes::from([0x22; 32]),
                },
                messages: vec![
                    HashBytes::from([0x01; 32]),
                    HashBytes::from([0x02; 32]),
                    HashBytes::from([0x03; 32]),
                ],
                router_partitions_src: Default::default(),
                router_partitions_dst: BTreeMap::from([(1.into(), BTreeSet::from([addr1, addr2]))]),
            };

            // NOTE: We need this for the hash computation.
            diff.recompute_hash();

            queue_diffs.push(diff);
        }

        // We store diffs in descending order.
        queue_diffs.reverse();

        let state = QueueStateHeader {
            shard_ident: ShardIdent::MASTERCHAIN,
            seqno: 10,
            queue_diffs,
        };

        let bytes = tl_proto::serialize(&state);
        assert_eq!(bytes.len(), state.max_size_hint());

        let parsed = tl_proto::deserialize::<QueueStateHeader>(&bytes).unwrap();
        assert_eq!(state, parsed);
    }

    #[test]
    fn test_next_value() {
        // 1) Check increment when hash is all zeros
        let key_zero = QueueKey {
            lt: 5,
            hash: HashBytes::ZERO,
        };
        let next_zero = key_zero.next_value();
        // Expect that lt remains unchanged and hash is [0..0, 1]
        assert_eq!(
            next_zero.lt, 5,
            "LT must remain the same if hash is not full 0xFF"
        );
        let mut expected_hash_zero = [0u8; 32];
        expected_hash_zero[31] = 1;
        assert_eq!(
            next_zero.hash.0, expected_hash_zero,
            "Hash should increment by 1"
        );

        // 2) Check increment when hash has partial 0xFF at the end
        //    e.g., last two bytes are 0xFF, but not the whole array
        let mut partial_ff = [0u8; 32];
        partial_ff[30] = 0xFF;
        partial_ff[31] = 0xFF;
        let key_partial_ff = QueueKey {
            lt: 10,
            hash: HashBytes(partial_ff),
        };
        let next_partial_ff = key_partial_ff.next_value();
        // Expected result: carry rolls over the last two 0xFF bytes
        // and increments the next byte
        let mut expected_hash_partial = [0u8; 32];
        expected_hash_partial[29] = 0x01; // incremented by carry
        // bytes 30, 31 become 0x00
        assert_eq!(
            next_partial_ff.lt, 10,
            "LT must remain the same with partial 0xFF"
        );
        assert_eq!(
            next_partial_ff.hash.0, expected_hash_partial,
            "Hash should be incremented correctly with carry"
        );

        // 3) Check increment when hash is fully 0xFF
        let key_full_ff = QueueKey {
            lt: 999,
            hash: HashBytes([0xFF; 32]),
        };
        let next_full_ff = key_full_ff.next_value();
        // Expect that hash resets to zero and LT increments by 1
        assert_eq!(
            next_full_ff.lt, 1000,
            "LT must increment if hash was all 0xFF"
        );
        assert_eq!(next_full_ff.hash.0, [0u8; 32], "Hash should reset to zero");

        // 4) A quick check of mid-range increment with carry:
        //    Example: [.., 0x01, 0xFF, 0xFF]
        let mut mid_hash = [0u8; 32];
        mid_hash[29] = 0x01;
        mid_hash[30] = 0xFF;
        mid_hash[31] = 0xFF;
        let key_mid = QueueKey {
            lt: 50,
            hash: HashBytes(mid_hash),
        };
        let next_mid = key_mid.next_value();
        // We expect that byte 29 increments to 0x02 and the last two bytes become 0x00
        let mut expected_mid_hash = [0u8; 32];
        expected_mid_hash[29] = 0x02;
        assert_eq!(
            next_mid.lt, 50,
            "LT should remain the same for a mid-range carry"
        );
        assert_eq!(
            next_mid.hash.0, expected_mid_hash,
            "Hash should increment the correct byte after partial 0xFF"
        );
    }
}
