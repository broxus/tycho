use std::fmt::{Display, Formatter};
use std::num::NonZeroU32;

use super::{AnchorFlags, PointStatusStore, StatusFlags};

/// Must not implement neither Copy nor Clone to prevent coding errors.
#[derive(Default)]
#[cfg_attr(any(test, feature = "test"), derive(PartialEq))]
pub struct PointStatusCommittable {
    pub anchor_flags: AnchorFlags,
    pub committed: Option<CommitHistoryPart>,
}

/// Not committed are stored with impossible zero round
#[derive(Debug)]
#[cfg_attr(any(test, feature = "test"), derive(PartialEq))]
pub struct CommitHistoryPart {
    pub anchor_round: NonZeroU32,
    pub seq_no: u32,
}

impl PointStatusStore for PointStatusCommittable {
    const BYTE_SIZE: usize = 2 + 1 + CommitHistoryPart::BYTE_SIZE;

    const DEFAULT_FLAGS: StatusFlags = StatusFlags::Committable;

    fn status_flags(&self) -> StatusFlags {
        Self::DEFAULT_FLAGS
    }

    fn read(flags: StatusFlags, stored: &[u8]) -> anyhow::Result<Self> {
        const FORBIDDEN_FLAGS: StatusFlags = StatusFlags::Found
            .union(StatusFlags::Resolved)
            .union(StatusFlags::WellFormed)
            .union(StatusFlags::Valid);

        anyhow::ensure!(flags.contains(Self::DEFAULT_FLAGS));
        anyhow::ensure!(!flags.contains(FORBIDDEN_FLAGS));
        anyhow::ensure!(stored.len() == Self::BYTE_SIZE);

        Ok(Self {
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

impl CommitHistoryPart {
    pub const BYTE_SIZE: usize = 2 * size_of::<u32>();
    /// stored at the same offset
    pub const RANGE: std::ops::Range<usize> = 3..3 + Self::BYTE_SIZE;

    pub fn write(this: Option<&Self>, dest: &mut Vec<u8>) {
        let anchor_round = this.map(|c| c.anchor_round.get()).unwrap_or_default();
        let seq_no = this.map(|c| c.seq_no).unwrap_or_default();

        dest.extend_from_slice(&anchor_round.to_be_bytes());
        dest.extend_from_slice(&seq_no.to_be_bytes());
    }

    pub fn fill(this: Option<&Self>, dest: &mut [u8]) -> anyhow::Result<()> {
        let len = dest.len();
        anyhow::ensure!(len == Self::BYTE_SIZE, "fill buf len {len}");

        let anchor_round = this.map(|c| c.anchor_round.get()).unwrap_or_default();
        let seq_no = this.map(|c| c.seq_no).unwrap_or_default();

        dest[..4].copy_from_slice(&anchor_round.to_be_bytes());
        dest[4..].copy_from_slice(&seq_no.to_be_bytes());
        Ok(())
    }

    pub fn read(slice: &[u8]) -> anyhow::Result<Option<Self>> {
        let len = slice.len();
        anyhow::ensure!(len == Self::BYTE_SIZE, "read slice len {len}");

        let mut u32_buf = [0; 4];

        u32_buf.copy_from_slice(&slice[..4]);
        let Some(anchor_round) = NonZeroU32::new(u32::from_be_bytes(u32_buf)) else {
            return Ok(None);
        };

        u32_buf.copy_from_slice(&slice[4..]);
        let seq_no = u32::from_be_bytes(u32_buf);

        Ok(Some(Self {
            anchor_round,
            seq_no,
        }))
    }
}

impl Display for PointStatusCommittable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut tuple = f.debug_tuple("Committable");
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
impl super::PointStatusStoreRandom for PointStatusCommittable {
    fn random() -> Self {
        Self {
            anchor_flags: AnchorFlags::from_bits_truncate(rand::random()),
            committed: CommitHistoryPart::random_opt(),
        }
    }
}

#[cfg(any(test, feature = "test"))]
impl CommitHistoryPart {
    pub fn random() -> Self {
        Self {
            anchor_round: rand::random(),
            seq_no: rand::random(),
        }
    }
    pub fn random_opt() -> Option<Self> {
        rand::random_bool(0.8).then_some(Self::random())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn commit_history_part_read_write() -> anyhow::Result<()> {
        let cp = CommitHistoryPart::random();

        let mut buf: [u8; CommitHistoryPart::BYTE_SIZE] = [0; _];

        CommitHistoryPart::fill(Some(&cp), &mut buf)?;

        let Some(cp_2) = CommitHistoryPart::read(&buf)? else {
            anyhow::bail!("cannot read");
        };

        anyhow::ensure!(cp_2 == cp);

        let mut vec = Vec::new();

        CommitHistoryPart::write(Some(&cp), &mut vec);

        anyhow::ensure!(&vec[..] == &buf[..]);

        vec.clear();
        CommitHistoryPart::write(None, &mut vec);
        CommitHistoryPart::fill(None, &mut buf)?;

        anyhow::ensure!(&vec[..] == &buf[..]);

        anyhow::ensure!(CommitHistoryPart::read(&buf)?.is_none());

        Ok(())
    }
}
