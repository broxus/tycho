use std::fmt::{Debug, Display, Formatter};

use tycho_network::PeerId;
use tycho_storage::point_status::{AnchorFlags, StatusFlags};

use crate::effects::{AltFmt, AltFormat};
use crate::models::{Digest, PointId, PointInfo, PrevPointProof, Round};

pub enum PointRestore {
    /// non-terminal status can be found in DB if point task was aborted before validation;
    /// Note: must repeat `verify()`, as this also represents an error during merge of DB statuses
    Exists(PointInfo, Option<PrevPointProof>),
    Valid(PointInfo, PointStatusValid),
    Invalid(PointInfo, PointStatusInvalid),
    IllFormed(PointId, PointStatusIllFormed),
    NotFound(Round, Digest, PointStatusNotFound),
}
impl PointRestore {
    /// required partial order: resolved + valid, resolved, valid, no flags, no status
    pub fn restore_order_asc(&self) -> u8 {
        /// greater value for greater priority
        fn order_desc<T: PointStatus>(status: &T) -> u8 {
            let mut priority = 1;
            priority |= (status.is_first_resolved() as u8) << 7;
            priority |= (status.is_first_valid() as u8) << 6;
            priority |= (T::is_valid() as u8) << 5;
            priority
        }
        let desc = match self {
            PointRestore::Exists(_, _) => 0,
            PointRestore::Valid(_, status) => order_desc(status),
            PointRestore::Invalid(_, status) => order_desc(status),
            PointRestore::IllFormed(_, status) => order_desc(status),
            PointRestore::NotFound(_, _, status) => order_desc(status),
        };
        !desc // invert priority for ascending order
    }
    pub fn round(&self) -> Round {
        match self {
            Self::Exists(info, _) | Self::Valid(info, _) | Self::Invalid(info, _) => info.round(),
            Self::IllFormed(id, _) => id.round,
            Self::NotFound(round, _, _) => *round,
        }
    }
    pub fn author(&self) -> &PeerId {
        match self {
            Self::Exists(info, _) | Self::Valid(info, _) | Self::Invalid(info, _) => {
                &info.data().author
            }
            Self::IllFormed(id, _) => &id.author,
            Self::NotFound(_, _, status) => &status.author,
        }
    }
    pub fn digest(&self) -> &Digest {
        match self {
            Self::Exists(info, _) | Self::Valid(info, _) | Self::Invalid(info, _) => info.digest(),
            Self::IllFormed(id, _) => &id.digest,
            Self::NotFound(_, digest, _) => digest,
        }
    }
    pub fn id(&self) -> PointId {
        PointId {
            author: *self.author(),
            round: self.round(),
            digest: *self.digest(),
        }
    }
}

impl AltFormat for PointRestore {}
impl Debug for AltFmt<'_, PointRestore> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let inner = AltFormat::unpack(self);
        write!(f, "Restore {{ {:?} ", inner.id().alt())?;
        match inner {
            PointRestore::Exists(_, Some(prev)) => {
                write!(f, "Exists prev # {}", prev.digest.alt())?;
            }
            PointRestore::Exists(_, None) => {
                f.write_str("Exists prev # None")?;
            }
            PointRestore::Valid(_, status) => {
                write!(f, "{status}")?;
            }
            PointRestore::Invalid(_, status) => {
                write!(f, "{status}")?;
            }
            PointRestore::IllFormed(_, status) => {
                write!(f, "{status}")?;
            }
            PointRestore::NotFound(_, _, status) => {
                let mut tuple = f.debug_tuple("NotFound");
                if status.is_first_resolved {
                    tuple.field(&"first resolved");
                }
                if status.is_certified {
                    tuple.field(&"certified");
                }
                // author is shown in point id
                tuple.finish()?;
            }
        }
        f.write_str(" }")
    }
}

#[derive(Debug)]
pub enum PointStatusStored {
    Exists,
    Valid(PointStatusValid),
    Invalid(PointStatusInvalid),
    IllFormed(PointStatusIllFormed),
    NotFound(PointStatusNotFound),
}
impl Display for PointStatusStored {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.as_ref(), f)
    }
}
pub enum PointStatusStoredRef<'a> {
    Exists,
    Valid(&'a PointStatusValid),
    Invalid(&'a PointStatusInvalid),
    IllFormed(&'a PointStatusIllFormed),
    NotFound(&'a PointStatusNotFound),
}
impl Display for PointStatusStoredRef<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Exists => f.write_str("Exists"),
            Self::Valid(resolved) => Display::fmt(resolved, f),
            Self::Invalid(resolved) => Display::fmt(resolved, f),
            Self::IllFormed(resolved) => Display::fmt(resolved, f),
            Self::NotFound(resolved) => Display::fmt(resolved, f),
        }
    }
}

pub trait PointStatus: Display {
    fn is_valid() -> bool {
        false
    }
    fn set_first_valid(&mut self) {}
    fn is_first_valid(&self) -> bool {
        false
    }
    fn set_first_resolved(&mut self);
    fn is_first_resolved(&self) -> bool;
    fn size_hint() -> usize;
    fn write_to(&self, buffer: &mut Vec<u8>);

    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(Self::size_hint());
        self.write_to(&mut buf);
        buf
    }
}

#[derive(Debug, Default)]
pub struct PointStatusValid {
    pub is_first_valid: bool,
    pub is_first_resolved: bool,
    pub is_certified: bool,
    anchor_flags: AnchorFlags, // TODO make public and fill, only zeros now
    pub committed_at_round: Option<u32>, // not committed are stored with impossible zero round
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
        if self.is_certified {
            tuple.field(&"certified");
        }
        tuple.finish_non_exhaustive()
    }
}
impl PointStatus for PointStatusValid {
    fn is_valid() -> bool {
        true
    }
    fn set_first_valid(&mut self) {
        self.is_first_valid = true;
    }
    fn is_first_valid(&self) -> bool {
        self.is_first_valid
    }
    fn set_first_resolved(&mut self) {
        self.is_first_resolved = true;
    }
    fn is_first_resolved(&self) -> bool {
        self.is_first_resolved
    }
    fn size_hint() -> usize {
        StatusFlags::VALID_BYTES
    }
    fn write_to(&self, buffer: &mut Vec<u8>) {
        let mut flags = StatusFlags::empty();
        flags.insert(StatusFlags::Found);
        flags.insert(StatusFlags::WellFormed);
        flags.insert(StatusFlags::Valid);
        flags.set(StatusFlags::FirstValid, self.is_first_valid);
        flags.set(StatusFlags::FirstResolved, self.is_first_resolved);
        flags.set(StatusFlags::Certified, self.is_certified);

        buffer.push(flags.bits());
        buffer.push(self.anchor_flags.bits());
        let at = self.committed_at_round.unwrap_or_default();
        buffer.extend_from_slice(&at.to_be_bytes());
    }
}

#[derive(Debug)]
pub struct PointStatusInvalid {
    pub is_first_resolved: bool,
    pub is_certified: bool,
}
impl Display for PointStatusInvalid {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut tuple = f.debug_tuple("Invalid");
        if self.is_first_resolved {
            tuple.field(&"first resolved");
        }
        if self.is_certified {
            tuple.field(&"certified");
        }
        tuple.finish()
    }
}
impl PointStatus for PointStatusInvalid {
    fn set_first_resolved(&mut self) {
        self.is_first_resolved = true;
    }
    fn is_first_resolved(&self) -> bool {
        self.is_first_resolved
    }
    fn size_hint() -> usize {
        StatusFlags::INVALID_BYTES
    }
    fn write_to(&self, buffer: &mut Vec<u8>) {
        let mut flags = StatusFlags::empty();
        flags.insert(StatusFlags::Found);
        flags.insert(StatusFlags::WellFormed);
        flags.set(StatusFlags::FirstResolved, self.is_first_resolved);
        flags.set(StatusFlags::Certified, self.is_certified);

        buffer.push(flags.bits());
    }
}

#[derive(Debug, Default)]
pub struct PointStatusIllFormed {
    pub is_first_resolved: bool,
}
impl Display for PointStatusIllFormed {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut tuple = f.debug_tuple("IllFormed");
        if self.is_first_resolved {
            tuple.field(&"first resolved");
        }
        tuple.finish()
    }
}
impl PointStatus for PointStatusIllFormed {
    fn set_first_resolved(&mut self) {
        self.is_first_resolved = true;
    }
    fn is_first_resolved(&self) -> bool {
        self.is_first_resolved
    }
    fn size_hint() -> usize {
        StatusFlags::ILL_FORMED_BYTES
    }
    fn write_to(&self, buffer: &mut Vec<u8>) {
        let mut flags = StatusFlags::empty();
        flags.insert(StatusFlags::Found);
        flags.set(StatusFlags::FirstResolved, self.is_first_resolved);

        buffer.push(flags.bits());
    }
}

#[derive(Debug)]
pub struct PointStatusNotFound {
    pub is_first_resolved: bool,
    pub is_certified: bool,
    pub author: PeerId,
}
impl Display for PointStatusNotFound {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut tuple = f.debug_tuple("NotFound");
        if self.is_first_resolved {
            tuple.field(&"first resolved");
        }
        if self.is_certified {
            tuple.field(&"certified");
        }
        tuple.field(&format!("author: {}", self.author.alt()));
        tuple.finish()
    }
}

impl PointStatus for PointStatusNotFound {
    fn set_first_resolved(&mut self) {
        self.is_first_resolved = true;
    }
    fn is_first_resolved(&self) -> bool {
        self.is_first_resolved
    }
    fn size_hint() -> usize {
        StatusFlags::NOT_FOUND_BYTES
    }
    fn write_to(&self, buffer: &mut Vec<u8>) {
        let mut flags = StatusFlags::empty();
        flags.set(StatusFlags::FirstResolved, self.is_first_resolved);
        flags.set(StatusFlags::Certified, self.is_certified);

        buffer.push(flags.bits());
        buffer.extend_from_slice(&self.author.0);
    }
}

impl PointStatusStoredRef<'_> {
    pub fn encode(&self) -> Vec<u8> {
        match self {
            Self::Exists => Vec::new(), // no data
            Self::Valid(resolved) => resolved.encode(),
            Self::Invalid(resolved) => resolved.encode(),
            Self::IllFormed(resolved) => resolved.encode(),
            Self::NotFound(resolved) => resolved.encode(),
        }
    }

    pub fn write_to(&self, buffer: &mut Vec<u8>) {
        match self {
            Self::Exists => {} // no data
            Self::Valid(resolved) => resolved.write_to(buffer),
            Self::Invalid(resolved) => resolved.write_to(buffer),
            Self::IllFormed(resolved) => resolved.write_to(buffer),
            Self::NotFound(resolved) => resolved.write_to(buffer),
        }
    }
}

impl PointStatusStored {
    pub fn as_ref(&self) -> PointStatusStoredRef<'_> {
        match self {
            Self::Exists => PointStatusStoredRef::Exists,
            Self::Valid(resolved) => PointStatusStoredRef::Valid(resolved),
            Self::Invalid(resolved) => PointStatusStoredRef::Invalid(resolved),
            Self::IllFormed(resolved) => PointStatusStoredRef::IllFormed(resolved),
            Self::NotFound(resolved) => PointStatusStoredRef::NotFound(resolved),
        }
    }
    pub fn decode(stored: &[u8]) -> anyhow::Result<Self> {
        let Some(flags) = StatusFlags::try_from_stored(stored).map_err(anyhow::Error::msg)? else {
            return Ok(Self::Exists);
        };
        let resolved = if flags.contains(StatusFlags::Found) {
            if flags.contains(StatusFlags::WellFormed) {
                if flags.contains(StatusFlags::Valid) {
                    let mut committed_at = [0_u8; 4];
                    committed_at.copy_from_slice(&stored[2..]);
                    let committed_at = u32::from_be_bytes(committed_at);
                    let committed_at_round = if committed_at == 0 {
                        None
                    } else {
                        Some(committed_at)
                    };
                    Self::Valid(PointStatusValid {
                        is_first_valid: flags.contains(StatusFlags::FirstValid),
                        is_first_resolved: flags.contains(StatusFlags::FirstResolved),
                        is_certified: flags.contains(StatusFlags::Certified),
                        anchor_flags: AnchorFlags::from_bits_retain(stored[1]),
                        committed_at_round,
                    })
                } else {
                    Self::Invalid(PointStatusInvalid {
                        is_first_resolved: flags.contains(StatusFlags::FirstResolved),
                        is_certified: flags.contains(StatusFlags::Certified),
                    })
                }
            } else {
                Self::IllFormed(PointStatusIllFormed {
                    is_first_resolved: flags.contains(StatusFlags::FirstResolved),
                })
            }
        } else {
            let mut author = [0_u8; 32];
            author.copy_from_slice(&stored[1..]);
            Self::NotFound(PointStatusNotFound {
                author: PeerId(author),
                is_first_resolved: flags.contains(StatusFlags::FirstResolved),
                is_certified: flags.contains(StatusFlags::Certified),
            })
        };
        Ok(resolved)
    }
}
