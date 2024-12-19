use std::collections::BTreeMap;

use bytes::Bytes;
use everscale_types::models::*;
use everscale_types::prelude::*;
use tl_proto::{TlRead, TlWrite};
use tycho_util::FastHashMap;

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
    // TODO: should rename field in `proto.tl` on network reset
    pub processed_to: BTreeMap<ShardIdent, QueueKey>,
    /// Min message queue key.
    pub min_message: QueueKey,
    /// Max message queue key.
    pub max_message: QueueKey,
    /// List of message hashes (sorted ASC).
    pub messages: Vec<HashBytes>,
    /// Partition router
    pub partition_router: FastHashMap<IntAddr, QueuePartition>,
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
    }
}

impl<'tl> TlRead<'tl> for QueueDiff {
    type Repr = tl_proto::Boxed;

    fn read_from(data: &'tl [u8], offset: &mut usize) -> tl_proto::TlResult<Self> {
        let offset_before = *offset;

        if u32::read_from(data, offset)? != Self::TL_ID {
            return Err(tl_proto::TlError::UnknownConstructor);
        }

        let mut result = Self {
            hash: HashBytes::ZERO,
            prev_hash: tl::hash_bytes::read(data, offset)?,
            shard_ident: tl::shard_ident::read(data, offset)?,
            seqno: u32::read_from(data, offset)?,
            processed_to: processed_to_map::read(data, offset)?,
            min_message: QueueKey::read_from(data, offset)?,
            max_message: QueueKey::read_from(data, offset)?,
            messages: messages_list::read(data, offset)?,
            // TODO !!! add read for partition_router
            partition_router: Default::default(),
        };

        if result.max_message < result.min_message {
            return Err(tl_proto::TlError::InvalidData);
        }

        // Compute the hash of the diff
        result.hash = Self::compute_hash(&data[offset_before..*offset]);

        Ok(result)
    }
}

/// Persistent internal messages queue state.
#[derive(Debug, Clone, PartialEq, Eq, TlWrite, TlRead)]
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
#[derive(Debug, Clone, PartialEq, Eq, TlWrite, TlRead)]
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

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, TlWrite, TlRead)]
#[tl(boxed)]
pub enum QueuePartition {
    #[tl(id = 0)]
    NormalPriority = 0,
    #[tl(id = 1)]
    LowPriority = 1,
}

impl Default for QueuePartition {
    fn default() -> Self {
        Self::NormalPriority
    }
}

impl QueueKey {
    const SIZE_HINT: usize = 8 + 32;

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
        write!(f, "{}", self)
    }
}

impl std::fmt::Display for QueueKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LT_HASH({}_{})", self.lt, self.hash)
    }
}

mod processed_to_map {
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

    pub fn read(data: &[u8], offset: &mut usize) -> TlResult<BTreeMap<ShardIdent, QueueKey>> {
        let len = u32::read_from(data, offset)? as usize;
        if len > MAX_SIZE {
            return Err(tl_proto::TlError::InvalidData);
        }

        let mut items = BTreeMap::new();
        let mut prev_shard = None;
        for _ in 0..len {
            let shard_ident = tl::shard_ident::read(data, offset)?;

            // Require that shards are sorted in ascending order.
            if let Some(prev_shard) = prev_shard {
                if shard_ident <= prev_shard {
                    return Err(tl_proto::TlError::InvalidData);
                }
            }
            prev_shard = Some(shard_ident);

            // Read the rest of the entry.
            items.insert(shard_ident, QueueKey::read_from(data, offset)?);
        }

        Ok(items)
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

    pub fn read(data: &[u8], offset: &mut usize) -> TlResult<Vec<QueueDiff>> {
        let len = u32::read_from(data, offset)? as usize;
        if len > MAX_SIZE || len == 0 {
            return Err(tl_proto::TlError::InvalidData);
        }

        let mut items = Vec::with_capacity(len);
        items.push(QueueDiff::read_from(data, offset)?);

        let (mut latest_seqno, mut prev_hash) = {
            let latest = items.first().expect("always not empty");
            (latest.seqno, latest.prev_hash)
        };

        for _ in 1..len {
            let diff = QueueDiff::read_from(data, offset)?;

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
    const MAX_SIZE: usize = 100_000;

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

    pub fn read(data: &[u8], offset: &mut usize) -> tl_proto::TlResult<Vec<HashBytes>> {
        let len = u32::read_from(data, offset)? as usize;
        if len > MAX_SIZE {
            return Err(tl_proto::TlError::InvalidData);
        }

        let mut items = Vec::with_capacity(len);

        let mut prev_hash = HashBytes::ZERO;
        for _ in 0..len {
            // NOTE: Assume that there are no messages with zero hash.
            let hash = tl::hash_bytes::read(data, offset)?;

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

    pub fn read(data: &[u8], offset: &mut usize) -> tl_proto::TlResult<Vec<Bytes>> {
        let len = u32::read_from(data, offset)? as usize;
        if len > MAX_CHUNKS {
            return Err(tl_proto::TlError::InvalidData);
        }

        let mut items = Vec::with_capacity(len);
        for _ in 0..len {
            items.push(BigBytes::read(data, offset)?);
        }

        Ok(items)
    }
}

mod state_messages_list_ref {
    use super::state_messages_list::{MAX_CHUNKS, MAX_CHUNK_SIZE};
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

    pub fn read<'tl>(data: &'tl [u8], offset: &mut usize) -> tl_proto::TlResult<Vec<&'tl [u8]>> {
        let len = u32::read_from(data, offset)? as usize;
        if len > MAX_CHUNKS {
            return Err(tl_proto::TlError::InvalidData);
        }

        let mut items = Vec::with_capacity(len);
        for _ in 0..len {
            items.push(BigBytesRef::read(data, offset)?);
        }

        Ok(items)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_diff_binary_repr() {
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
            partition_router: Default::default(),
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
                partition_router: Default::default(),
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
}
