use std::collections::BTreeMap;

use everscale_types::models::*;
use everscale_types::prelude::*;
use tl_proto::{TlRead, TlWrite};

use crate::tl;

/// Representation of an internal messages queue diff.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueDiff {
    /// Computed hash of this diff.
    ///
    /// NOTE: This field is not serialized and can be [`HashBytes::ZERO`] for serialization.
    pub hash: HashBytes,

    pub prev_hash: HashBytes,
    pub shard_ident: ShardIdent,
    pub seqno: u32,
    pub processed_upto: BTreeMap<ShardIdent, ShardProcessedUpto>,
    pub messages: Vec<HashBytes>,
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
            + processed_upto_map::size_hint(&self.processed_upto)
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
        processed_upto_map::write(&self.processed_upto, packet);
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
            processed_upto: processed_upto_map::read(data, offset)?,
            messages: messages_list::read(data, offset)?,
        };

        // Compute the hash of the diff
        result.hash = Self::compute_hash(&data[offset_before..*offset]);

        Ok(result)
    }
}

/// Representation of a persistent internal messages queue state.
#[derive(Debug, Clone, PartialEq, Eq, TlWrite, TlRead)]
#[tl(boxed, id = "block.queueState", scheme = "proto.tl")]
pub struct QueueState {
    #[tl(with = "tl::shard_ident")]
    pub shard_ident: ShardIdent,
    pub seqno: u32,
    #[tl(with = "queue_diffs_list")]
    pub queue_diffs: Vec<QueueDiff>,
}

/// Lower bound of work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, TlWrite, TlRead)]
#[tl(boxed, id = "block.shardProcessedUpto", scheme = "proto.tl")]
pub struct ShardProcessedUpto {
    pub lt: u64,
    #[tl(with = "tl::hash_bytes")]
    pub hash: HashBytes,
}

mod processed_upto_map {
    use tl_proto::{TlPacket, TlResult};

    use super::*;

    /// We assume that the number of processed shards is limited.
    const MAX_SIZE: usize = 1000;

    pub fn size_hint(items: &BTreeMap<ShardIdent, ShardProcessedUpto>) -> usize {
        const PER_ITEM: usize = tl::shard_ident::SIZE_HINT + (4 + 8 + 32);

        4 + items.len() * PER_ITEM
    }

    pub fn write<P: TlPacket>(items: &BTreeMap<ShardIdent, ShardProcessedUpto>, packet: &mut P) {
        packet.write_u32(items.len() as u32);

        for (shard_ident, processed_upto) in items {
            tl::shard_ident::write(shard_ident, packet);
            processed_upto.write_to(packet);
        }
    }

    pub fn read(
        data: &[u8],
        offset: &mut usize,
    ) -> TlResult<BTreeMap<ShardIdent, ShardProcessedUpto>> {
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
            items.insert(shard_ident, ShardProcessedUpto::read_from(data, offset)?);
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
            let latest = items.get(0).expect("always not empty");
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
            processed_upto: BTreeMap::from([
                (ShardIdent::MASTERCHAIN, ShardProcessedUpto {
                    lt: 1,
                    hash: HashBytes::from([0x11; 32]),
                }),
                (ShardIdent::BASECHAIN, ShardProcessedUpto {
                    lt: 123123,
                    hash: HashBytes::from([0x22; 32]),
                }),
            ]),
            messages: vec![
                HashBytes::from([0x01; 32]),
                HashBytes::from([0x02; 32]),
                HashBytes::from([0x03; 32]),
            ],
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
                processed_upto: BTreeMap::from([
                    (ShardIdent::MASTERCHAIN, ShardProcessedUpto {
                        lt: 10 * seqno as u64,
                        hash: HashBytes::from([seqno as u8; 32]),
                    }),
                    (ShardIdent::BASECHAIN, ShardProcessedUpto {
                        lt: 123123 * seqno as u64,
                        hash: HashBytes::from([seqno as u8 * 2; 32]),
                    }),
                ]),
                messages: vec![
                    HashBytes::from([0x01; 32]),
                    HashBytes::from([0x02; 32]),
                    HashBytes::from([0x03; 32]),
                ],
            };

            // NOTE: We need this for the hash computation.
            diff.recompute_hash();

            queue_diffs.push(diff);
        }

        // We store diffs in descending order.
        queue_diffs.reverse();

        let state = QueueState {
            shard_ident: ShardIdent::MASTERCHAIN,
            seqno: 10,
            queue_diffs,
        };

        let bytes = tl_proto::serialize(&state);
        assert_eq!(bytes.len(), state.max_size_hint());

        let parsed = tl_proto::deserialize::<QueueState>(&bytes).unwrap();
        assert_eq!(state, parsed);
    }
}
