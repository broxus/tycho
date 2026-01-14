use std::fmt::{Display, Formatter};

use super::{PointStatusStore, StatusFlags};

/// Must not implement neither Copy nor Clone to prevent coding errors.
#[cfg_attr(any(test, feature = "test"), derive(PartialEq))]
pub struct PointStatusProven;

impl PointStatusStore for PointStatusProven {
    const DEFAULT_FLAGS: StatusFlags = StatusFlags::HasProof;

    fn status_flags(&self) -> StatusFlags {
        Self::DEFAULT_FLAGS
    }

    fn read(flags: StatusFlags, stored: &[u8]) -> anyhow::Result<Self> {
        const FORBIDDEN_FLAGS: StatusFlags = StatusFlags::Found
            .union(StatusFlags::Resolved)
            .union(StatusFlags::WellFormed)
            .union(StatusFlags::Committable)
            .union(StatusFlags::Valid);

        anyhow::ensure!(flags.contains(Self::DEFAULT_FLAGS));
        anyhow::ensure!(!flags.contains(FORBIDDEN_FLAGS));
        anyhow::ensure!(stored.len() == Self::BYTE_SIZE);

        Ok(Self)
    }
}

impl Display for PointStatusProven {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Proven").finish()
    }
}

#[cfg(any(test, feature = "test"))]
impl super::PointStatusStoreRandom for PointStatusProven {
    fn random() -> Self {
        Self
    }
}
