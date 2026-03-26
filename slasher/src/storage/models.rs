use tl_proto::{TlError, TlPacket, TlRead, TlResult, TlWrite};
use tycho_slasher_traits::ValidationSessionId;
use tycho_types::cell::HashBytes;
use tycho_util::FastHashSet;

use crate::analyzer::{
    SessionMeta, SessionPenaltyReport, SessionValidatorScore, VsetEpoch, VsetPenaltyReport,
    VsetValidatorPenalty,
};
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

#[repr(transparent)]
pub struct StoredVsetEpoch(pub VsetEpoch);

impl StoredVsetEpoch {
    pub const TL_ID: u32 = tl_proto::id!("slasher.vsetEpoch", scheme = "proto.tl");

    #[inline]
    pub const fn wrap(inner: &VsetEpoch) -> &Self {
        // SAFETY: `StoredVsetEpoch` has the same layout as `VsetEpoch`.
        unsafe { &*(inner as *const VsetEpoch).cast::<Self>() }
    }
}

impl TlWrite for StoredVsetEpoch {
    type Repr = tl_proto::Boxed;

    fn max_size_hint(&self) -> usize {
        4 + 32 + 4 + 4 + 4
    }

    fn write_to<P: TlPacket>(&self, packet: &mut P) {
        packet.write_u32(Self::TL_ID);
        packet.write_raw_slice(&self.0.vset_hash.0);
        packet.write_u32(u32::from(self.0.next_epoch_start_session_id.is_some()));
        let next_session_id = self
            .0
            .next_epoch_start_session_id
            .unwrap_or(ValidationSessionId {
                catchain_seqno: 0,
                vset_switch_round: 0,
            });
        packet.write_u32(next_session_id.catchain_seqno);
        packet.write_u32(next_session_id.vset_switch_round);
    }
}

impl<'tl> TlRead<'tl> for StoredVsetEpoch {
    type Repr = tl_proto::Boxed;

    fn read_from(packet: &mut &'tl [u8]) -> TlResult<Self> {
        if u32::read_from(packet)? != Self::TL_ID {
            return Err(TlError::UnknownConstructor);
        }

        let vset_hash = read_hash_bytes(packet)?;
        let has_next_epoch = u32::read_from(packet)? != 0;
        let next_session_id = ValidationSessionId {
            catchain_seqno: u32::read_from(packet)?,
            vset_switch_round: u32::read_from(packet)?,
        };

        Ok(Self(VsetEpoch {
            start_session_id: ValidationSessionId {
                catchain_seqno: 0,
                vset_switch_round: 0,
            },
            vset_hash,
            next_epoch_start_session_id: has_next_epoch.then_some(next_session_id),
        }))
    }
}

#[repr(transparent)]
pub struct StoredSessionMeta(pub SessionMeta);

impl StoredSessionMeta {
    pub const TL_ID: u32 = tl_proto::id!("slasher.sessionMeta", scheme = "proto.tl");

    #[inline]
    pub const fn wrap(inner: &SessionMeta) -> &Self {
        // SAFETY: `StoredSessionMeta` has the same layout as `SessionMeta`.
        unsafe { &*(inner as *const SessionMeta).cast::<Self>() }
    }
}

impl TlWrite for StoredSessionMeta {
    type Repr = tl_proto::Boxed;

    fn max_size_hint(&self) -> usize {
        4 + 4 + self.0.validator_indices.len() * 4
    }

    fn write_to<P: TlPacket>(&self, packet: &mut P) {
        packet.write_u32(Self::TL_ID);
        packet.write_u32(self.0.validator_indices.len() as u32);
        for validator_idx in &self.0.validator_indices {
            packet.write_u32(u32::from(*validator_idx));
        }
    }
}

impl<'tl> TlRead<'tl> for StoredSessionMeta {
    type Repr = tl_proto::Boxed;

    fn read_from(packet: &mut &'tl [u8]) -> TlResult<Self> {
        if u32::read_from(packet)? != Self::TL_ID {
            return Err(TlError::UnknownConstructor);
        }

        let validator_count = u32::read_from(packet)? as usize;
        let mut validator_indices = Vec::with_capacity(validator_count);
        let mut unique_indices =
            FastHashSet::with_capacity_and_hasher(validator_count, Default::default());
        for _ in 0..validator_count {
            let Ok(validator_idx) = u16::try_from(u32::read_from(packet)?) else {
                return Err(TlError::InvalidData);
            };
            if !unique_indices.insert(validator_idx) {
                return Err(TlError::InvalidData);
            }
            validator_indices.push(validator_idx);
        }

        Ok(Self(SessionMeta {
            session_id: ValidationSessionId {
                catchain_seqno: 0,
                vset_switch_round: 0,
            },
            epoch_start_session_id: ValidationSessionId {
                catchain_seqno: 0,
                vset_switch_round: 0,
            },
            validator_indices,
        }))
    }
}

#[repr(transparent)]
pub struct StoredSessionPenaltyReport(pub SessionPenaltyReport);

impl StoredSessionPenaltyReport {
    pub const TL_ID: u32 = tl_proto::id!("slasher.sessionPenaltyReport", scheme = "proto.tl");

    #[inline]
    pub const fn wrap(inner: &SessionPenaltyReport) -> &Self {
        // SAFETY: `StoredSessionPenaltyReport` has the same layout as `SessionPenaltyReport`.
        unsafe { &*(inner as *const SessionPenaltyReport).cast::<Self>() }
    }
}

impl TlWrite for StoredSessionPenaltyReport {
    type Repr = tl_proto::Boxed;

    fn max_size_hint(&self) -> usize {
        4 + 4 + 4 + 4 + 4 + 4 + self.0.validators.len() * (4 + 8 + 8 + 4)
    }

    fn write_to<P: TlPacket>(&self, packet: &mut P) {
        packet.write_u32(Self::TL_ID);
        packet.write_u32(self.0.session_id.catchain_seqno);
        packet.write_u32(self.0.session_id.vset_switch_round);
        packet.write_u32(self.0.epoch_start_session_id.catchain_seqno);
        packet.write_u32(self.0.epoch_start_session_id.vset_switch_round);
        packet.write_u32(self.0.session_weight);
        packet.write_u32(self.0.validators.len() as u32);
        for item in &self.0.validators {
            packet.write_u32(u32::from(item.validator_idx));
            packet.write_u64(item.earned_points);
            packet.write_u64(item.max_points);
            packet.write_u32(u32::from(item.is_bad));
        }
    }
}

impl<'tl> TlRead<'tl> for StoredSessionPenaltyReport {
    type Repr = tl_proto::Boxed;

    fn read_from(packet: &mut &'tl [u8]) -> TlResult<Self> {
        if u32::read_from(packet)? != Self::TL_ID {
            return Err(TlError::UnknownConstructor);
        }

        let session_id = ValidationSessionId {
            catchain_seqno: u32::read_from(packet)?,
            vset_switch_round: u32::read_from(packet)?,
        };
        let epoch_start_session_id = ValidationSessionId {
            catchain_seqno: u32::read_from(packet)?,
            vset_switch_round: u32::read_from(packet)?,
        };
        let session_weight = u32::read_from(packet)?;
        let validator_count = u32::read_from(packet)? as usize;

        let mut validators = Vec::with_capacity(validator_count);
        let mut unique_indices =
            FastHashSet::with_capacity_and_hasher(validator_count, Default::default());
        for _ in 0..validator_count {
            let Ok(validator_idx) = u16::try_from(u32::read_from(packet)?) else {
                return Err(TlError::InvalidData);
            };
            if !unique_indices.insert(validator_idx) {
                return Err(TlError::InvalidData);
            }

            validators.push(SessionValidatorScore {
                validator_idx,
                earned_points: u64::read_from(packet)?,
                max_points: u64::read_from(packet)?,
                is_bad: u32::read_from(packet)? != 0,
            });
        }

        Ok(Self(SessionPenaltyReport {
            session_id,
            epoch_start_session_id,
            session_weight,
            validators: validators.into_boxed_slice(),
        }))
    }
}

#[repr(transparent)]
pub struct StoredVsetPenaltyReport(pub VsetPenaltyReport);

impl StoredVsetPenaltyReport {
    pub const TL_ID: u32 = tl_proto::id!("slasher.vsetPenaltyReport", scheme = "proto.tl");

    #[inline]
    pub const fn wrap(inner: &VsetPenaltyReport) -> &Self {
        // SAFETY: `StoredVsetPenaltyReport` has the same layout as `VsetPenaltyReport`.
        unsafe { &*(inner as *const VsetPenaltyReport).cast::<Self>() }
    }
}

impl TlWrite for StoredVsetPenaltyReport {
    type Repr = tl_proto::Boxed;

    fn max_size_hint(&self) -> usize {
        4 + 4 + 4 + 32 + 4 + self.0.validators.len() * (4 + 8 + 8 + 4)
    }

    fn write_to<P: TlPacket>(&self, packet: &mut P) {
        packet.write_u32(Self::TL_ID);
        packet.write_u32(self.0.epoch_start_session_id.catchain_seqno);
        packet.write_u32(self.0.epoch_start_session_id.vset_switch_round);
        packet.write_raw_slice(&self.0.vset_hash.0);
        packet.write_u32(self.0.validators.len() as u32);
        for item in &self.0.validators {
            packet.write_u32(u32::from(item.validator_idx));
            packet.write_u64(item.bad_sessions_weight);
            packet.write_u64(item.total_sessions_weight);
            packet.write_u32(u32::from(item.is_bad));
        }
    }
}

impl<'tl> TlRead<'tl> for StoredVsetPenaltyReport {
    type Repr = tl_proto::Boxed;

    fn read_from(packet: &mut &'tl [u8]) -> TlResult<Self> {
        if u32::read_from(packet)? != Self::TL_ID {
            return Err(TlError::UnknownConstructor);
        }

        let epoch_start_session_id = ValidationSessionId {
            catchain_seqno: u32::read_from(packet)?,
            vset_switch_round: u32::read_from(packet)?,
        };
        let vset_hash = read_hash_bytes(packet)?;
        let validator_count = u32::read_from(packet)? as usize;

        let mut validators = Vec::with_capacity(validator_count);
        let mut unique_indices =
            FastHashSet::with_capacity_and_hasher(validator_count, Default::default());
        for _ in 0..validator_count {
            let Ok(validator_idx) = u16::try_from(u32::read_from(packet)?) else {
                return Err(TlError::InvalidData);
            };
            if !unique_indices.insert(validator_idx) {
                return Err(TlError::InvalidData);
            }

            validators.push(VsetValidatorPenalty {
                validator_idx,
                bad_sessions_weight: u64::read_from(packet)?,
                total_sessions_weight: u64::read_from(packet)?,
                is_bad: u32::read_from(packet)? != 0,
            });
        }

        Ok(Self(VsetPenaltyReport {
            epoch_start_session_id,
            vset_hash,
            validators: validators.into_boxed_slice(),
        }))
    }
}

fn read_hash_bytes(packet: &mut &[u8]) -> TlResult<HashBytes> {
    if packet.len() < size_of::<HashBytes>() {
        return Err(TlError::UnexpectedEof);
    }

    let (hash, tail) = packet.split_at(size_of::<HashBytes>());
    *packet = tail;
    let mut bytes = [0; size_of::<HashBytes>()];
    bytes.copy_from_slice(hash);
    Ok(HashBytes(bytes))
}
