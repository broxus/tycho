use std::fmt::{Display, Formatter};

use tl_proto::{TlRead, TlWrite};

use super::{
    AnchorFlags, CommitHistoryPart, PointStatus, PointStatusCommittable, PointStatusStore,
    StatusFlags,
};
use crate::effects::AltFormat;
use crate::models::IndirectLink;

/// Must not implement neither Copy nor Clone to prevent coding errors.
#[cfg_attr(any(test, feature = "test"), derive(PartialEq))]
pub struct PointStatusTransInvalid {
    pub is_first_resolved: bool,

    pub has_proof: bool,
    pub has_dag_round: bool,

    pub anchor_flags: AnchorFlags,
    pub committed: Option<CommitHistoryPart>,
    pub root_cause: IndirectLink,
}

impl PointStatus for PointStatusTransInvalid {
    fn set_first_resolved(&mut self) {
        self.is_first_resolved = true;
    }
    fn is_first_resolved(&self) -> bool {
        self.is_first_resolved
    }
}

impl PointStatusStore for PointStatusTransInvalid {
    const BYTE_SIZE: usize = PointStatusCommittable::BYTE_SIZE + IndirectLink::MAX_TL_BYTES;

    const DEFAULT_FLAGS: StatusFlags = StatusFlags::TransInvalid;

    fn status_flags(&self) -> StatusFlags {
        let mut flags = Self::DEFAULT_FLAGS;

        flags.set(StatusFlags::HasProof, self.has_proof);
        flags.set(StatusFlags::InvalidHasDagRound, self.has_dag_round);

        flags
    }

    fn read(flags: StatusFlags, stored: &[u8]) -> anyhow::Result<Self> {
        anyhow::ensure!(flags.contains(Self::DEFAULT_FLAGS));
        anyhow::ensure!(stored.len() == Self::BYTE_SIZE);

        Ok(Self {
            is_first_resolved: false,

            has_proof: flags.contains(StatusFlags::HasProof),
            has_dag_round: flags.contains(StatusFlags::InvalidHasDagRound),

            anchor_flags: AnchorFlags::from_bits_retain(stored[2]),
            committed: CommitHistoryPart::read(&stored[CommitHistoryPart::RANGE])?,
            root_cause: IndirectLink::read_from(&mut &stored[Self::ROOT_CAUSE_RANGE])?,
        })
    }

    fn write_to(&self, buffer: &mut Vec<u8>) {
        let flags = self.status_flags();

        buffer.extend_from_slice(&flags.bits().to_be_bytes());
        buffer.push(self.anchor_flags.bits());

        CommitHistoryPart::write(self.committed.as_ref(), buffer);
        self.root_cause.write_to(buffer);
    }

    fn fill(&self, buf: &mut [u8]) -> anyhow::Result<()> {
        let len = buf.len();
        anyhow::ensure!(len == Self::BYTE_SIZE, "buf len {len}");
        let flags = self.status_flags();

        buf[..2].copy_from_slice(&flags.bits().to_be_bytes());
        buf[2] = self.anchor_flags.bits();

        CommitHistoryPart::fill(self.committed.as_ref(), &mut buf[CommitHistoryPart::RANGE])?;
        self.root_cause.fill(&mut buf[Self::ROOT_CAUSE_RANGE])?;

        Ok(())
    }
}

impl PointStatusTransInvalid {
    const ROOT_CAUSE_RANGE: std::ops::Range<usize> =
        CommitHistoryPart::RANGE.end..CommitHistoryPart::RANGE.end + IndirectLink::MAX_TL_BYTES;
}

impl Display for PointStatusTransInvalid {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut tuple = f.debug_tuple("TransInvalid");
        if self.is_first_resolved {
            tuple.field(&"first resolved");
        }
        if self.has_proof {
            tuple.field(&"has proof");
        }
        if !self.has_dag_round {
            tuple.field(&"root cause no dag round");
        }
        if !self.anchor_flags.is_empty() {
            tuple.field(&self.anchor_flags);
        }
        if let Some(committed) = &self.committed {
            tuple.field(committed);
        }
        tuple.field(&self.root_cause.alt());
        tuple.finish()
    }
}

#[cfg(any(test, feature = "test"))]
impl super::PointStatusStoreRandom for PointStatusTransInvalid {
    fn random() -> Self {
        Self {
            is_first_resolved: false,
            has_proof: rand::random(),
            has_dag_round: rand::random(),
            anchor_flags: AnchorFlags::from_bits_truncate(rand::random()),
            committed: CommitHistoryPart::random_opt(),
            root_cause: IndirectLink::random(),
        }
    }
}
