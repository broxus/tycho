use std::fmt::{Display, Formatter};

use super::{PointStatusStore, StatusFlags};

/// Must not implement neither Copy nor Clone to prevent coding errors.
#[derive(Default)]
#[cfg_attr(any(test, feature = "test"), derive(PartialEq))]
pub struct PointStatusFound {
    pub has_proof: bool,
}

impl PointStatusStore for PointStatusFound {
    const DEFAULT_FLAGS: StatusFlags = StatusFlags::Found;

    fn status_flags(&self) -> StatusFlags {
        let mut flags = Self::DEFAULT_FLAGS;

        flags.set(StatusFlags::HasProof, self.has_proof);

        flags
    }

    fn read(flags: StatusFlags, stored: &[u8]) -> anyhow::Result<Self> {
        anyhow::ensure!(flags.contains(Self::DEFAULT_FLAGS));
        anyhow::ensure!(stored.len() == Self::BYTE_SIZE);

        Ok(Self {
            has_proof: flags.contains(StatusFlags::HasProof),
        })
    }
}

impl Display for PointStatusFound {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut tuple = f.debug_tuple("Found");
        if self.has_proof {
            tuple.field(&"has proof");
        }
        tuple.finish()
    }
}

#[cfg(any(test, feature = "test"))]
impl super::PointStatusStoreRandom for PointStatusFound {
    fn random() -> Self {
        Self {
            has_proof: rand::random(),
        }
    }
}
