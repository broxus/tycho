use std::fmt::{Display, Formatter};

use tl_proto::{TlRead, TlWrite};
use tycho_network::PeerId;

use super::{PointStatus, PointStatusStore, StatusFlags};
use crate::effects::AltFormat;

/// Must not implement neither Copy nor Clone to prevent coding errors.
#[cfg_attr(any(test, feature = "test"), derive(PartialEq))]
pub struct PointStatusNotFound {
    pub is_first_resolved: bool,
    pub has_proof: bool,
    pub author: PeerId,
}

impl PointStatus for PointStatusNotFound {
    fn set_first_resolved(&mut self) {
        self.is_first_resolved = true;
    }
    fn is_first_resolved(&self) -> bool {
        self.is_first_resolved
    }
}
impl PointStatusStore for PointStatusNotFound {
    const BYTE_SIZE: usize = 2 + PeerId::MAX_TL_BYTES;

    const DEFAULT_FLAGS: StatusFlags = StatusFlags::Resolved;

    fn status_flags(&self) -> StatusFlags {
        let mut flags = Self::DEFAULT_FLAGS;

        flags.set(StatusFlags::HasProof, self.has_proof);

        flags
    }

    fn read(flags: StatusFlags, stored: &[u8]) -> anyhow::Result<Self> {
        const FORBIDDEN_FLAGS: StatusFlags = StatusFlags::Found
            .union(StatusFlags::WellFormed)
            .union(StatusFlags::Committable)
            .union(StatusFlags::Valid);

        anyhow::ensure!(flags.contains(Self::DEFAULT_FLAGS));
        anyhow::ensure!(!flags.contains(FORBIDDEN_FLAGS));
        anyhow::ensure!(stored.len() == Self::BYTE_SIZE);

        Ok(Self {
            is_first_resolved: false,

            has_proof: flags.contains(StatusFlags::HasProof),

            author: PeerId::read_from(&mut &stored[2..])?,
        })
    }

    fn write_to(&self, buffer: &mut Vec<u8>) {
        let flags = self.status_flags();

        buffer.extend_from_slice(&flags.bits().to_be_bytes());
        self.author.write_to(buffer);
    }

    fn fill(&self, buf: &mut [u8]) -> anyhow::Result<()> {
        let len = buf.len();
        anyhow::ensure!(len == Self::BYTE_SIZE, "buf len {len}");
        let flags = self.status_flags();

        buf[..2].copy_from_slice(&flags.bits().to_be_bytes());
        buf[2..2 + 4].copy_from_slice(&PeerId::TL_ID.to_le_bytes());
        buf[2 + 4..].copy_from_slice(&self.author.0);

        Ok(())
    }
}

impl Display for PointStatusNotFound {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut tuple = f.debug_tuple("NotFound");
        if self.is_first_resolved {
            tuple.field(&"first resolved");
        }
        if self.has_proof {
            tuple.field(&"has proof");
        }
        tuple.field(&format!("author: {}", self.author.alt()));
        tuple.finish()
    }
}

#[cfg(any(test, feature = "test"))]
impl super::PointStatusStoreRandom for PointStatusNotFound {
    fn random() -> Self {
        Self {
            is_first_resolved: false,
            has_proof: rand::random(),
            author: PeerId(rand::random()),
        }
    }
}
