use tl_proto::{TlError, TlPacket, TlRead, TlResult, TlWrite};
use tycho_util::FastHashSet;

use crate::util::BitSet;
use crate::{BlocksBatch, SignatureHistory};

#[repr(transparent)]
pub struct StoredBlocksBatch(pub BlocksBatch);

impl StoredBlocksBatch {
    pub const TL_ID: u32 = tl_proto::id!("slasher.blocksBatch", scheme = "proto.tl");

    const MAX_SAFE_COMMITTED_BLOCKS: usize = 500;
    const MAX_SAFE_HISTORY_COUNT: usize = 1000;

    #[inline]
    pub const fn wrap(inner: &BlocksBatch) -> &Self {
        // SAFETY: `StoredBlocksBatch` has the same layout as `BlocksBatch`.
        unsafe { &*(inner as *const BlocksBatch).cast::<Self>() }
    }
}

impl TlWrite for StoredBlocksBatch {
    type Repr = tl_proto::Boxed;

    // TODO: Simplify becase all signature histories are equal in size.
    fn max_size_hint(&self) -> usize {
        4 + 4
            + self.0.committed_blocks.max_size_hint()
            + 4
            + self
                .0
                .signatures_history
                .iter()
                .map(|item| 4 + item.bits.max_size_hint())
                .sum::<usize>()
    }

    fn write_to<P: TlPacket>(&self, packet: &mut P) {
        packet.write_u32(Self::TL_ID);
        packet.write_u32(self.0.start_seqno);
        self.0.committed_blocks.write_to(packet);
        packet.write_u32(self.0.signatures_history.len() as u32);
        for item in &self.0.signatures_history {
            packet.write_u32(item.validator_idx as u32);
            item.bits.write_to(packet);
        }
    }
}

impl<'tl> TlRead<'tl> for StoredBlocksBatch {
    type Repr = tl_proto::Boxed;

    fn read_from(packet: &mut &'tl [u8]) -> TlResult<Self> {
        if u32::read_from(packet)? != Self::TL_ID {
            return Err(TlError::UnknownConstructor);
        }

        let start_seqno = u32::read_from(packet)?;
        let committed_blocks = BitSet::read_from(packet)?;
        let block_count = committed_blocks.len();
        if start_seqno.checked_add(block_count as u32).is_none() {
            return Err(TlError::InvalidData);
        }

        let history_count = u32::read_from(packet)? as usize;
        if history_count > Self::MAX_SAFE_HISTORY_COUNT
            || block_count > Self::MAX_SAFE_COMMITTED_BLOCKS
        {
            return Err(TlError::InvalidData);
        }

        let mut signatures_history = Vec::with_capacity(history_count);
        let mut unique_indices =
            FastHashSet::with_capacity_and_hasher(history_count, Default::default());
        for _ in 0..history_count {
            let Ok(validator_idx) = u16::try_from(u32::read_from(packet)?) else {
                return Err(TlError::InvalidData);
            };
            if !unique_indices.insert(validator_idx) {
                return Err(TlError::InvalidData);
            }
            let bits = BitSet::read_from(packet)?;
            if bits.len() != block_count * 2 {
                return Err(TlError::InvalidData);
            }
            signatures_history.push(SignatureHistory {
                validator_idx,
                bits,
            });
        }

        Ok(Self(BlocksBatch {
            start_seqno,
            committed_blocks,
            signatures_history: signatures_history.into_boxed_slice(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use tycho_slasher_traits::ReceivedSignature;

    use super::*;

    #[test]
    fn blocks_batch_tl_repr() {
        let mut batch = BlocksBatch::new(230, NonZeroU32::new(100).unwrap(), &[5, 10, 12, 3]);

        for (seqno, signatures) in [
            (230, [
                ReceivedSignature(ReceivedSignature::VALID_SIGNATURE_BIT),
                ReceivedSignature(ReceivedSignature::VALID_SIGNATURE_BIT),
                ReceivedSignature(0),
                ReceivedSignature(ReceivedSignature::INVALID_SIGNATURE_BIT),
            ]),
            (250, [
                ReceivedSignature(0),
                ReceivedSignature(ReceivedSignature::VALID_SIGNATURE_BIT),
                ReceivedSignature(ReceivedSignature::INVALID_SIGNATURE_BIT),
                ReceivedSignature(ReceivedSignature::VALID_SIGNATURE_BIT),
            ]),
            (251, [
                ReceivedSignature(ReceivedSignature::VALID_SIGNATURE_BIT),
                ReceivedSignature(ReceivedSignature::VALID_SIGNATURE_BIT),
                ReceivedSignature(ReceivedSignature::VALID_SIGNATURE_BIT),
                ReceivedSignature(ReceivedSignature::VALID_SIGNATURE_BIT),
            ]),
            (300, [
                ReceivedSignature(ReceivedSignature::VALID_SIGNATURE_BIT),
                ReceivedSignature(ReceivedSignature::VALID_SIGNATURE_BIT),
                ReceivedSignature(ReceivedSignature::VALID_SIGNATURE_BIT),
                ReceivedSignature(ReceivedSignature::VALID_SIGNATURE_BIT),
            ]),
            (329, [
                ReceivedSignature(ReceivedSignature::VALID_SIGNATURE_BIT),
                ReceivedSignature(ReceivedSignature::VALID_SIGNATURE_BIT),
                ReceivedSignature(ReceivedSignature::VALID_SIGNATURE_BIT),
                ReceivedSignature(ReceivedSignature::VALID_SIGNATURE_BIT),
            ]),
        ] {
            let committed = batch.commit_signatures(seqno, &signatures);
            assert!(committed);
        }

        let stored = tl_proto::serialize(StoredBlocksBatch::wrap(&batch));
        let loaded = tl_proto::deserialize::<StoredBlocksBatch>(&stored).unwrap();
        assert_eq!(batch, loaded.0);
    }
}
