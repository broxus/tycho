use tl_proto::{TlError, TlPacket, TlRead, TlResult, TlWrite};
use tycho_slasher_traits::ValidationSessionId;
use tycho_util::FastHashSet;

use crate::BlocksBatch;
use crate::bc::ObservedHistory;
use crate::util::BitSet;
// === Vset State Stuff ===

#[derive(Debug, Clone, Copy, TlRead, TlWrite)]
#[tl(boxed, id = "slasher.storedVsetInfo", scheme = "proto.tl")]
pub struct StoredVsetInfo {
    pub prev_vset_hash: [u8; 32],
    #[tl(with = "tl_session_id")]
    pub first_session_id: ValidationSessionId,
    pub start_seqno: u32,
}

#[derive(Debug, Clone, TlRead, TlWrite)]
#[tl(boxed, id = "slasher.storedVsetReport", scheme = "proto.tl")]
pub struct StoredVsetReport {
    pub public_key: [u8; 32],
    #[tl(with = "tl_accusations")]
    pub accusations: Vec<u16>,
}

// === StoredBlocksBatch ===

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

    fn max_size_hint(&self) -> usize {
        4 + 4
            + (4 + 4)
            + self.0.committed_blocks.max_size_hint()
            + 4
            + (self.0.observed)
                .iter()
                .map(|item| 4 + 4 + item.bits.max_size_hint())
                .sum::<usize>()
    }

    fn write_to<P: TlPacket>(&self, packet: &mut P) {
        packet.write_u32(Self::TL_ID);
        packet.write_u32(self.0.start_seqno);
        if let Some(anchor_range) = self.0.anchor_range.as_ref() {
            packet.write_u32(*anchor_range.start());
            packet.write_u32(*anchor_range.end());
        } else {
            packet.write_u32(0);
            packet.write_u32(0);
        };

        self.0.committed_blocks.write_to(packet);
        packet.write_u32(self.0.observed.len() as u32);
        for item in &self.0.observed {
            packet.write_u32(item.validator_idx as u32);
            packet.write_u32(item.points_proven as u32);
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
        let anchor_range = Some(u32::read_from(packet)?..=u32::read_from(packet)?)
            .filter(|range| *range.end() > 0);

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

        let mut observed = Vec::with_capacity(history_count);
        let mut unique_indices =
            FastHashSet::with_capacity_and_hasher(history_count, Default::default());
        for _ in 0..history_count {
            let Ok(validator_idx) = u16::try_from(u32::read_from(packet)?) else {
                return Err(TlError::InvalidData);
            };
            if !unique_indices.insert(validator_idx) {
                return Err(TlError::InvalidData);
            }
            let points_proven =
                u16::try_from(u32::read_from(packet)?).map_err(|_e| TlError::InvalidData)?;
            let bits = BitSet::read_from(packet)?;
            if bits.len() != block_count * 2 {
                return Err(TlError::InvalidData);
            }
            observed.push(ObservedHistory {
                validator_idx,
                points_proven,
                bits,
            });
        }

        Ok(Self(BlocksBatch {
            start_seqno,
            anchor_range,
            committed_blocks,
            observed: observed.into_boxed_slice(),
        }))
    }
}

// === Stuff ===

mod tl_session_id {
    use super::*;

    pub fn size_hint(_: &ValidationSessionId) -> usize {
        4 + 4
    }

    pub fn write<T: TlPacket>(value: &ValidationSessionId, packet: &mut T) {
        packet.write_u32(value.catchain_seqno);
        packet.write_u32(value.vset_switch_round);
    }

    pub fn read(packet: &mut &[u8]) -> TlResult<ValidationSessionId> {
        Ok(ValidationSessionId {
            catchain_seqno: u32::read_from(packet)?,
            vset_switch_round: u32::read_from(packet)?,
        })
    }
}

mod tl_accusations {
    use super::*;

    pub fn size_hint(value: &[u16]) -> usize {
        4 + 4 * value.len()
    }

    pub fn write<T: TlPacket>(value: &[u16], packet: &mut T) {
        packet.write_u32(value.len() as u32);
        for item in value {
            packet.write_u32(*item as u32);
        }
    }

    pub fn read(packet: &mut &[u8]) -> TlResult<Vec<u16>> {
        let len = u32::read_from(packet)? as usize;
        let mut result = Vec::with_capacity(len);
        for _ in 0..len {
            let idx = u32::read_from(packet)?;
            if idx > u16::MAX as u32 {
                return Err(TlError::InvalidData);
            }
            result.push(idx as u16);
        }
        Ok(result)
    }
}
