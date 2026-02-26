use std::fmt::{Display, Formatter};

use super::{PointStatusStore, StatusFlags};

/// Must not implement neither Copy nor Clone to prevent coding errors.
#[cfg_attr(any(test, feature = "test"), derive(PartialEq))]
pub struct PointStatusProven {
    pub has_proof: bool,
}

impl PointStatusStore for PointStatusProven {
    const DEFAULT_FLAGS: StatusFlags = StatusFlags::empty();

    fn status_flags(&self) -> StatusFlags {
        let mut flags = Self::DEFAULT_FLAGS;

        flags.set(StatusFlags::HasProof, self.has_proof);

        flags
    }

    fn read(flags: StatusFlags, stored: &[u8]) -> anyhow::Result<Self> {
        anyhow::ensure!(stored.len() == Self::BYTE_SIZE);
        anyhow::ensure!(stored[0] == 0);

        Ok(Self {
            has_proof: flags.contains(StatusFlags::HasProof),
        })
    }
}

impl Display for PointStatusProven {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut tuple = f.debug_tuple("Proven");
        if self.has_proof {
            tuple.field(&"has proof");
        } else {
            tuple.field(&"no proof");
        }
        tuple.finish()
    }
}

#[cfg(any(test, feature = "test"))]
impl super::PointStatusStoreRandom for PointStatusProven {
    fn random() -> Self {
        Self {
            has_proof: rand::random(),
        }
    }
}
