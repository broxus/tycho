use std::fmt::{Display, Formatter};

use super::{
    AnchorFlags, CommitHistoryPart, PointStatus, PointStatusCommittable, PointStatusStore,
    StatusFlags,
};

/// Must not implement neither Copy nor Clone to prevent coding errors.
#[derive(Default)]
#[cfg_attr(any(test, feature = "test"), derive(PartialEq))]
pub struct PointStatusValid {
    pub is_first_resolved: bool,
    pub is_first_valid: bool,

    pub has_proof: bool,

    pub anchor_flags: AnchorFlags,
    pub committed: Option<CommitHistoryPart>,
}

impl PointStatus for PointStatusValid {
    fn set_first_resolved(&mut self) {
        self.is_first_resolved = true;
    }
    fn is_first_resolved(&self) -> bool {
        self.is_first_resolved
    }
    fn is_valid() -> bool {
        true
    }
    fn set_first_valid(&mut self) {
        self.is_first_valid = true;
    }
    fn is_first_valid(&self) -> bool {
        self.is_first_valid
    }
}

impl PointStatusStore for PointStatusValid {
    const BYTE_SIZE: usize = PointStatusCommittable::BYTE_SIZE;

    const DEFAULT_FLAGS: StatusFlags = StatusFlags::Valid;

    fn status_flags(&self) -> StatusFlags {
        let mut flags = Self::DEFAULT_FLAGS;

        flags.set(StatusFlags::FirstValid, self.is_first_valid);

        flags.set(StatusFlags::HasProof, self.has_proof);

        flags
    }

    fn read(flags: StatusFlags, stored: &[u8]) -> anyhow::Result<Self> {
        anyhow::ensure!(flags.contains(Self::DEFAULT_FLAGS));
        anyhow::ensure!(stored.len() == Self::BYTE_SIZE);

        Ok(Self {
            is_first_resolved: false,

            is_first_valid: flags.contains(StatusFlags::FirstValid),

            has_proof: flags.contains(StatusFlags::HasProof),

            anchor_flags: AnchorFlags::from_bits_retain(stored[2]),
            committed: CommitHistoryPart::read(&stored[CommitHistoryPart::RANGE])?,
        })
    }

    fn write_to(&self, buffer: &mut Vec<u8>) {
        let flags = self.status_flags();

        buffer.extend_from_slice(&flags.bits().to_be_bytes());
        buffer.push(self.anchor_flags.bits());

        CommitHistoryPart::write(self.committed.as_ref(), buffer);
    }

    fn fill(&self, buf: &mut [u8]) -> anyhow::Result<()> {
        let len = buf.len();
        anyhow::ensure!(len == Self::BYTE_SIZE, "buf len {len}");
        let flags = self.status_flags();

        buf[..2].copy_from_slice(&flags.bits().to_be_bytes());
        buf[2] = self.anchor_flags.bits();

        CommitHistoryPart::fill(self.committed.as_ref(), &mut buf[CommitHistoryPart::RANGE])?;

        Ok(())
    }
}

impl Display for PointStatusValid {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut tuple = f.debug_tuple("Valid");
        if self.is_first_valid {
            tuple.field(&"first valid");
        }
        if self.is_first_resolved {
            tuple.field(&"first resolved");
        }
        if self.has_proof {
            tuple.field(&"has proof");
        }
        if !self.anchor_flags.is_empty() {
            tuple.field(&self.anchor_flags);
        }
        if let Some(committed) = &self.committed {
            tuple.field(committed);
        }
        tuple.finish()
    }
}

#[cfg(any(test, feature = "test"))]
impl super::PointStatusStoreRandom for PointStatusValid {
    fn random() -> Self {
        Self {
            is_first_resolved: false,
            is_first_valid: rand::random(),
            has_proof: rand::random(),
            anchor_flags: AnchorFlags::from_bits_truncate(rand::random()),
            committed: CommitHistoryPart::random_opt(),
        }
    }
}
