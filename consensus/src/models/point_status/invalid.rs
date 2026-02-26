use std::fmt::{Display, Formatter};

use super::{PointStatus, PointStatusStore, StatusFlags};

/// Must not implement neither Copy nor Clone to prevent coding errors.
#[cfg_attr(any(test, feature = "test"), derive(PartialEq))]
pub struct PointStatusInvalid {
    pub is_first_resolved: bool,

    pub has_proof: bool,
    pub has_dag_round: bool,
}

impl PointStatus for PointStatusInvalid {
    fn set_first_resolved(&mut self) {
        self.is_first_resolved = true;
    }
    fn is_first_resolved(&self) -> bool {
        self.is_first_resolved
    }
}

impl PointStatusStore for PointStatusInvalid {
    const DEFAULT_FLAGS: StatusFlags = StatusFlags::Invalid;

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
        })
    }
}

impl Display for PointStatusInvalid {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut tuple = f.debug_tuple("Invalid");
        if self.is_first_resolved {
            tuple.field(&"first resolved");
        }
        if self.has_proof {
            tuple.field(&"has proof");
        }
        if !self.has_dag_round {
            tuple.field(&"no dag round");
        }
        tuple.finish()
    }
}

#[cfg(any(test, feature = "test"))]
impl super::PointStatusStoreRandom for PointStatusInvalid {
    fn random() -> Self {
        Self {
            is_first_resolved: false,
            has_proof: rand::random(),
            has_dag_round: rand::random(),
        }
    }
}
