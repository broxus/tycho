use std::fmt::{Display, Formatter};

use super::{PointStatus, PointStatusStore, StatusFlags};

/// Must not implement neither Copy nor Clone to prevent coding errors.
#[cfg_attr(any(test, feature = "test"), derive(PartialEq))]
pub struct PointStatusIllFormed {
    pub is_first_resolved: bool,

    pub has_proof: bool,
    pub is_reason_final: bool,
}

impl PointStatus for PointStatusIllFormed {
    fn set_first_resolved(&mut self) {
        self.is_first_resolved = true;
    }
    fn is_first_resolved(&self) -> bool {
        self.is_first_resolved
    }
}

impl PointStatusStore for PointStatusIllFormed {
    const DEFAULT_FLAGS: StatusFlags = StatusFlags::IllFormed;

    fn status_flags(&self) -> StatusFlags {
        let mut flags = Self::DEFAULT_FLAGS;

        flags.set(StatusFlags::HasProof, self.has_proof);
        flags.set(StatusFlags::IllFormedReasonFinal, self.is_reason_final);

        flags
    }

    fn read(flags: StatusFlags, stored: &[u8]) -> anyhow::Result<Self> {
        anyhow::ensure!(flags.contains(Self::DEFAULT_FLAGS));
        anyhow::ensure!(stored.len() == Self::BYTE_SIZE);

        Ok(Self {
            is_first_resolved: false,
            has_proof: flags.contains(StatusFlags::HasProof),
            is_reason_final: flags.contains(StatusFlags::IllFormedReasonFinal),
        })
    }
}

impl Display for PointStatusIllFormed {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut tuple = f.debug_tuple("IllFormed");
        if self.is_first_resolved {
            tuple.field(&"first resolved");
        }
        if self.has_proof {
            tuple.field(&"has proof");
        }
        if self.is_reason_final {
            tuple.field(&"reason is final");
        }
        tuple.finish()
    }
}

#[cfg(any(test, feature = "test"))]
impl super::PointStatusStoreRandom for PointStatusIllFormed {
    fn random() -> Self {
        Self {
            is_first_resolved: false,
            has_proof: rand::random(),
            is_reason_final: rand::random(),
        }
    }
}
