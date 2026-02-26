pub use all_stored::*;
pub use committable::*;
pub use found::*;
pub use ill_formed::*;
pub use invalid::*;
pub use not_found::*;
pub use proven::*;
pub use trans_invalid::*;
pub use valid::*;

mod all_stored;
mod committable;
mod found;
mod ill_formed;
mod invalid;
mod not_found;
mod proven;
mod trans_invalid;
mod valid;

// mandatory first 2 bytes for every stored record
bitflags::bitflags! {
    #[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
    pub struct StatusFlags : u16 {
        // first byte is merged with `max`, unique, order reflects priority

        const Valid = 0b_1 << 15;
        const TransInvalid = 0b_1 << 14;
        const Committable = 0b_1 << 13;

        const Invalid = 0b_1 << 11;
        const IllFormed = 0b_1 << 10;
        const Found = 0b_1 << 9;
        const NotFound = 0b_1 << 8; // lowest priority: no signs of a point
        // and also a `Proven` status with empty first byte

        // next byte is merged with `byte_or`

        const FirstValid = 0b_1 << 7; // highest priority among others in same location
        const HasProof = 0b_1 << 5; // may be the only flag (for merge), even without stored point
        const IllFormedReasonFinal = 0b_1 << 4;
        const InvalidHasDagRound = 0b_1 << 2;

        const AllWithPoint =
            Self::FirstValid.bits() |
            Self::Valid.bits() |
            Self::TransInvalid.bits() |
            Self::Invalid.bits() |
            Self::IllFormed.bits() |
            Self::Found.bits();

        const AllCommittable =
            Self::Valid.bits() |
            Self::TransInvalid.bits() |
            Self::Committable.bits();
    }
}

impl StatusFlags {
    /// Preserve `NotFound` status, final `IllFormed`, `Found` and `Proven` statuses for:
    /// * either `Invalid` -> `Valid` transitions based on certification re-read
    /// * or `IllFormed` -> `Valid` transitions based on updated `PeerSchedule`
    ///
    /// Also keep `FirstValid` points for stable decision on equivocation.
    /// May reset commit attributes as `HistoryConflict` is raised before they are created.
    pub fn keep_on_history_conflict(self) -> bool {
        self.intersects(Self::Found.union(Self::NotFound))
            || self.contains(Self::Valid.union(Self::FirstValid))
            || self.contains(Self::IllFormed.union(Self::IllFormedReasonFinal))
            || self.bits().to_be_bytes()[0] == 0 // `Proven` status
    }
}

// only for `Committable`, `Valid` and `TransInvalid` records; merged with `byte_or`
bitflags::bitflags! {
    #[derive(Copy, Clone, Default, Debug)]
    #[cfg_attr(any(test, feature = "test"), derive(PartialEq))]
    pub struct AnchorFlags : u8 {
        const Used = 0b_1 << 7;

        const Anchor = 0b_1 << 6;
        const Trigger = 0b_1 << 5;
        const Proof = 0b_1 << 4;
    }
}

pub trait PointStatus: std::fmt::Display {
    fn set_first_resolved(&mut self);
    /// Not stored in DB: used inflight to determine first valid
    fn is_first_resolved(&self) -> bool;
    fn is_valid() -> bool {
        false
    }
    fn set_first_valid(&mut self) {}
    fn is_first_valid(&self) -> bool {
        false
    }
}

/// Default impl is suitable for struct that stores only [`StatusFlags`]
pub trait PointStatusStore: Sized {
    const BYTE_SIZE: usize = 2;

    const DEFAULT_FLAGS: StatusFlags;

    /// Also a canary for all default flags to be unique, because it is used in a match
    const TYPE: u8 = Self::DEFAULT_FLAGS.bits().to_be_bytes()[0];

    fn status_flags(&self) -> StatusFlags;

    fn read(flags: StatusFlags, stored: &[u8]) -> anyhow::Result<Self>;

    fn write_to(&self, buffer: &mut Vec<u8>) {
        let flags = self.status_flags();

        buffer.extend_from_slice(&flags.bits().to_be_bytes());
    }

    fn fill(&self, buf: &mut [u8]) -> anyhow::Result<()> {
        let len = buf.len();
        anyhow::ensure!(len == Self::BYTE_SIZE, "buf len {len}");
        let flags = self.status_flags();

        buf.copy_from_slice(&flags.bits().to_be_bytes());

        Ok(())
    }
}

#[allow(unused, reason = "false positive")]
#[cfg(any(test, feature = "test"))]
pub trait PointStatusStoreRandom: PointStatusStore {
    fn random() -> Self;

    fn bytes(&self) -> Vec<u8> {
        let mut vec = Vec::with_capacity(Self::BYTE_SIZE);
        self.write_to(&mut vec);
        vec
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn check_valid() -> anyhow::Result<()> {
        for _ in 0..50 {
            self_check::<PointStatusValid, { PointStatusValid::BYTE_SIZE }>()?;
        }
        Ok(())
    }

    #[test]
    fn check_trans_invalid() -> anyhow::Result<()> {
        for _ in 0..100 {
            self_check::<PointStatusTransInvalid, { PointStatusTransInvalid::BYTE_SIZE }>()?;
        }
        Ok(())
    }

    #[test]
    fn check_committable() -> anyhow::Result<()> {
        for _ in 0..10 {
            self_check::<PointStatusCommittable, { PointStatusCommittable::BYTE_SIZE }>()?;
        }
        Ok(())
    }

    #[test]
    fn check_invalid() -> anyhow::Result<()> {
        for _ in 0..25 {
            self_check::<PointStatusInvalid, { PointStatusInvalid::BYTE_SIZE }>()?;
        }
        Ok(())
    }

    #[test]
    fn check_ill_formed() -> anyhow::Result<()> {
        for _ in 0..10 {
            self_check::<PointStatusIllFormed, { PointStatusIllFormed::BYTE_SIZE }>()?;
        }
        Ok(())
    }

    #[test]
    fn check_found() -> anyhow::Result<()> {
        for _ in 0..5 {
            self_check::<PointStatusFound, { PointStatusFound::BYTE_SIZE }>()?;
        }
        Ok(())
    }

    #[test]
    fn check_not_found() -> anyhow::Result<()> {
        for _ in 0..20 {
            self_check::<PointStatusNotFound, { PointStatusNotFound::BYTE_SIZE }>()?;
        }
        Ok(())
    }

    #[test]
    fn check_proven() -> anyhow::Result<()> {
        self_check::<PointStatusProven, { PointStatusProven::BYTE_SIZE }>()?;
        Ok(())
    }

    fn self_check<T: PointStatusStoreRandom + PartialEq, const SIZE: usize>() -> anyhow::Result<()>
    {
        const {
            assert!(SIZE == T::BYTE_SIZE, "wrong size const");
        }

        let s = T::random();

        let mut vec = Vec::new();
        s.write_to(&mut vec);

        let mut buf: [u8; SIZE] = [0; _];
        s.fill(&mut buf)?;

        anyhow::ensure!(&vec[..] == &buf[..]);

        let flags = PointStatusStored::read_flags(&buf)?;

        let s_2 = T::read(flags, &buf)?;

        anyhow::ensure!(s == s_2);

        Ok(())
    }
}
